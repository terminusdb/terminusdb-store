// File format:
// <header>
//  [<filetype present>]*
//  [<offsets>]*
//

use std::{
    io,
    ops::{Range, RangeBounds},
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

use crate::structure::{
    logarray_length_from_control_word, smallbitarray::SmallBitArray, LogArray, MonotonicLogArray,
};

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
        let mut guard = self.0.write().unwrap();
        match &mut *guard {
            ConstructionFileState::Finalized(x) => {
                let slice = if buf.remaining() > x.len() {
                    x.split_to(x.len())
                } else {
                    x.split_to(buf.remaining())
                };
                buf.put_slice(slice.as_ref());

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

pub struct ArchiveFilePresenceHeader {
    present_files: SmallBitArray,
}

impl ArchiveFilePresenceHeader {
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

    pub fn file_index(&self, file: LayerFileEnum) -> Option<usize> {
        if !self.is_present(file) {
            return None;
        }

        Some(self.present_files.rank1(file as usize) - 1)
    }
}

pub struct ArchiveHeader {
    file_presence: ArchiveFilePresenceHeader,
    file_offsets: MonotonicLogArray,
}

impl ArchiveHeader {
    pub fn parse(mut bytes: Bytes) -> (Self, Bytes) {
        let file_presence = ArchiveFilePresenceHeader::new(bytes.get_u64());
        let (file_offsets, remainder) =
            MonotonicLogArray::parse_header_first(bytes).expect("whatever");

        (
            Self {
                file_presence,
                file_offsets,
            },
            remainder,
        )
    }

    pub async fn parse_from_reader<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let file_presence = ArchiveFilePresenceHeader::new(reader.read_u64().await?);
        let mut logarray_bytes = BytesMut::new();
        logarray_bytes.resize(8, 0);
        reader.read_exact(&mut logarray_bytes[0..8]).await?;
        let len = logarray_length_from_control_word(&logarray_bytes[0..8]);
        unsafe {
            logarray_bytes.set_len(8 + len);
        }
        reader.read_exact(&mut logarray_bytes[8..]).await?;

        let file_offsets =
            MonotonicLogArray::parse(logarray_bytes.freeze()).expect("what the heck");

        Ok(Self {
            file_presence,
            file_offsets,
        })
    }

    pub fn range_for(&self, file: LayerFileEnum) -> Option<Range<usize>> {
        if let Some(file_index) = self.file_presence.file_index(file) {
            let start: usize = if file_index == 0 {
                0
            } else {
                self.file_offsets.entry(file_index - 1) as usize
            };

            let end: usize = self.file_offsets.entry(file_index) as usize;

            Some(start..end)
        } else {
            None
        }
    }

    pub fn size_of(&self, file: LayerFileEnum) -> Option<usize> {
        if let Some(range) = self.range_for(file) {
            Some(range.end - range.start)
        } else {
            None
        }
    }
}

pub struct Archive {
    header: ArchiveHeader,
    data: Bytes,
}

impl Archive {
    pub fn parse(bytes: Bytes) -> Self {
        let (header, data) = ArchiveHeader::parse(bytes);

        Self { header, data }
    }

    pub async fn parse_from_reader<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Self> {
        let header = ArchiveHeader::parse_from_reader(reader).await?;
        let data_len = header.file_offsets.entry(header.file_offsets.len() - 1) as usize;
        let mut data = BytesMut::with_capacity(data_len);
        unsafe { data.set_len(data_len) };
        reader.read_exact(&mut data[..]).await?;

        Ok(Self {
            header,
            data: data.freeze(),
        })
    }

    pub fn slice_for(&self, file: LayerFileEnum) -> Option<Bytes> {
        if let Some(range) = self.header.range_for(file) {
            Some(self.data.slice(range))
        } else {
            None
        }
    }

    pub fn size_of(&self, file: LayerFileEnum) -> Option<usize> {
        self.header.size_of(file)
    }
}

#[derive(Clone)]
pub struct PersistentFileSlice {
    path: PathBuf,
    file_type: LayerFileEnum,
}

pub struct ArchiveSliceReader {
    file: File,
    remaining: usize,
}

impl AsyncRead for ArchiveSliceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let read = AsyncRead::poll_read(Pin::new(&mut (*self).file), cx, buf);
        match read {
            Poll::Pending => return Poll::Pending,
            _ => {}
        }

        if buf.filled().len() > self.remaining {
            buf.set_filled(self.remaining);
        }

        self.remaining -= buf.filled().len();

        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl FileLoad for PersistentFileSlice {
    type Read = ArchiveSliceReader;

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
        let header = ArchiveFilePresenceHeader::new(file.read_u64().await?);

        Ok(header.is_present(self.file_type))
    }
    async fn size(&self) -> io::Result<usize> {
        // read header!
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).await?;
        let header = ArchiveHeader::parse_from_reader(&mut file).await?;

        header
            .size_of(self.file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }
    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).await?;
        let header = ArchiveHeader::parse_from_reader(&mut file).await?;
        let remaining = header
            .size_of(self.file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))?;

        Ok(ArchiveSliceReader { file, remaining })
    }

    async fn map(&self) -> io::Result<Bytes> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).await?;
        let archive = Archive::parse_from_reader(&mut file).await?;
        archive
            .slice_for(self.file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }
}

#[derive(Clone)]
pub enum ArchiveLayerHandle {
    Construction(ConstructionFile),
    Persistent(PersistentFileSlice),
}

#[async_trait]
impl FileStore for ArchiveLayerHandle {
    type Write = ConstructionFile;
    async fn open_write(&self) -> io::Result<Self::Write> {
        match self {
            Self::Construction(c) => c.open_write().await,
            _ => panic!("cannot write to a persistent file slice"),
        }
    }
}

#[async_trait]
impl FileLoad for ArchiveLayerHandle {
    type Read = ArchiveLayerHandleReader;

    async fn exists(&self) -> io::Result<bool> {
        match self {
            Self::Construction(c) => c.exists().await,
            Self::Persistent(p) => p.exists().await,
        }
    }
    async fn size(&self) -> io::Result<usize> {
        match self {
            Self::Construction(c) => c.size().await,
            Self::Persistent(p) => p.size().await,
        }
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        Ok(match self {
            Self::Construction(c) => {
                ArchiveLayerHandleReader::Construction(c.open_read_from(offset).await?)
            }
            Self::Persistent(p) => {
                ArchiveLayerHandleReader::Persistent(p.open_read_from(offset).await?)
            }
        })
    }

    async fn map(&self) -> io::Result<Bytes> {
        match self {
            Self::Construction(c) => c.map().await,
            Self::Persistent(p) => p.map().await,
        }
    }
}

pub enum ArchiveLayerHandleReader {
    Construction(ConstructionFile),
    Persistent(ArchiveSliceReader),
}

impl AsyncRead for ArchiveLayerHandleReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Construction(c) => AsyncRead::poll_read(Pin::new(c), cx, buf),
            Self::Persistent(p) => AsyncRead::poll_read(Pin::new(p), cx, buf),
        }
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

        let header = ArchiveFilePresenceHeader::from_present(files_present.iter().cloned());

        for file in files_present {
            assert!(header.is_present(file));
        }

        assert!(!header.is_present(LayerFileEnum::NodeDictionaryOffsets));
    }
}
