// File format:
// <header>
//  [<filetype present>]*
//  [<offsets>]*
//

use std::{
    io,
    ops::Range,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::Poll,
};

use async_trait::async_trait;
use bytes::{buf, Buf, BufMut, Bytes, BytesMut};
use std::io::Write;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWrite},
};

use crate::structure::smallbitarray::SmallBitArray;

use super::{consts::LayerFileEnum, FileLoad, FileStore, PersistentLayerStore, SyncableFile};

#[async_trait]
pub trait ArchiveLayerStoreBackend: Clone + Send + Sync {
    type File: FileLoad + FileStore + Clone;
    async fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>>;
    async fn get_layer_file(&self, id: [u32; 5]) -> io::Result<Self::File>;
    async fn store_layer_file<B: Buf>(&self, id: [u32; 5], bytes: B) -> io::Result<()>;

    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()>;
}

pub enum ConstructionFileState {
    UnderConstruction(BytesMut),
    Finalizing,
    Finalized(Bytes),
}

#[derive(Clone)]
pub struct ConstructionFile(Arc<RwLock<ConstructionFileState>>);

#[async_trait]
impl FileStore for ConstructionFile {
    type Write = Self;
    async fn open_write(&self) -> io::Result<Self::Write> {
        Ok(self.clone())
    }
}

impl AsyncWrite for ConstructionFile {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut guard = self.0.write().unwrap();
        match &mut *guard {
            ConstructionFileState::UnderConstruction(x) => {
                x.put_slice(buf);

                Poll::Ready(Ok(buf.len()))
            }
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "file already written",
            ))),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // noop
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // noop
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl SyncableFile for ConstructionFile {
    async fn sync_all(self) -> io::Result<()> {
        let mut guard = self.0.write().unwrap();
        let mut state = ConstructionFileState::Finalizing;
        std::mem::swap(&mut state, &mut *guard);

        match state {
            ConstructionFileState::UnderConstruction(x) => {
                let buf = x.freeze();
                *guard = ConstructionFileState::Finalized(buf);

                Ok(())
            }
            _ => {
                *guard = state;
                Err(io::Error::new(io::ErrorKind::Other, "file already written"))
            }
        }
    }
}

impl AsyncRead for ConstructionFile {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(x) => {
                buf.put_slice(x.as_ref());

                Poll::Ready(Ok(()))
            }
            _ => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "file not yet written",
            ))),
        }
    }
}

#[async_trait]
impl FileLoad for ConstructionFile {
    type Read = Self;

    async fn exists(&self) -> io::Result<bool> {
        let guard = self.0.read().unwrap();
        Ok(match &*guard {
            ConstructionFileState::Finalized(x) => true,
            _ => false,
        })
    }
    async fn size(&self) -> io::Result<usize> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(x) => Ok(x.len()),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not finalized",
            )),
        }
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(_) => Ok(self.clone()),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not finalized",
            )),
        }
    }

    async fn map(&self) -> io::Result<Bytes> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(x) => Ok(x.clone()),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not finalized",
            )),
        }
    }
}

pub struct ArchiveHeader {
    present_files: SmallBitArray,
}

impl ArchiveHeader {
    pub fn new(val: u64) -> Self {
        Self {
            present_files: SmallBitArray::new(val),
        }
    }

    pub fn from_present<I: Iterator<Item = LayerFileEnum>>(present_files: I) -> Self {
        let mut val = 0;

        for file in present_files {
            val |= 1 << (u64::BITS - file as u32 - 1);
        }

        Self::new(val)
    }

    pub fn is_present(&self, file: LayerFileEnum) -> bool {
        self.present_files.get(file as usize)
    }

    pub fn inner(&self) -> u64 {
        self.present_files.inner()
    }
}

#[derive(Clone)]
pub struct PersistentFileSlice {
    path: PathBuf,
    file_type: LayerFileEnum,
}

#[async_trait]
impl FileLoad for PersistentFileSlice {
    type Read = File;

    async fn exists(&self) -> io::Result<bool> {
        let metadata = tokio::fs::metadata(&self.path).await;
        if metadata.is_err() && metadata.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
            // layer itself not found
            return Ok(false);
        }
        // propagate error if it was anything but NotFound
        metadata?;

        // read header!
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).await?;
        let header = ArchiveHeader::new(file.read_u64().await?);

        Ok(header.is_present(self.file_type))
    }
    async fn size(&self) -> io::Result<usize> {
        todo!();
    }
    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        todo!();
    }
    async fn map(&self) -> io::Result<Bytes> {
        todo!();
    }
}

/*

#[derive(Clone)]
pub struct ArchiveLayerStore<T:ArchiveLayerStoreBackend> {
    backend: T,
    construction: Arc<RwLock<HashMap<[u32;5], HashMap<LayerFileEnum, ConstructionFile>
}

#[async_trait]
impl<T:ArchiveLayerStoreBackend+'static> PersistentLayerStore for ArchiveLayerStore<T> {
    type File = T::File;

    async fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        self.backend.get_layer_names().await
    }

    async fn create_named_directory(&self, id: [u32; 5]) -> io::Result<()> {

    }
}

*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_use_header() {
        let files_present = vec![
            LayerFileEnum::PredicateDictionaryBlocks,
            LayerFileEnum::NegObjects,
            LayerFileEnum::NodeValueIdMapBits,
            LayerFileEnum::Parent,
            LayerFileEnum::Rollup,
            LayerFileEnum::NodeDictionaryBlocks,
        ];

        let header = ArchiveHeader::from_present(files_present.iter().cloned());

        for file in files_present {
            assert!(header.is_present(file));
        }

        assert!(!header.is_present(LayerFileEnum::NodeDictionaryOffsets));
    }
}
