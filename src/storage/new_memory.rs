use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use futures::task::{Context, Poll};
use futures::{future, Future};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::file::*;
use super::layer::*;

use bytes::{Bytes, BytesMut};
enum MemoryBackedStoreContents {
    Nonexistent,
    Existent(Bytes),
}

#[derive(Clone)]
pub struct NewMemoryBackedStore {
    contents: Arc<RwLock<MemoryBackedStoreContents>>,
}

impl NewMemoryBackedStore {
    pub fn new() -> Self {
        Self {
            contents: Arc::new(RwLock::new(MemoryBackedStoreContents::Nonexistent)),
        }
    }
}

pub struct NewMemoryBackedStoreWriter {
    file: NewMemoryBackedStore,
    bytes: BytesMut,
}

impl SyncableFile for NewMemoryBackedStoreWriter {
    fn sync_all(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let mut contents = self.file.contents.write().unwrap();
        *contents = MemoryBackedStoreContents::Existent(self.bytes.freeze());

        Box::pin(future::ok(()))
    }
}

impl std::io::Write for NewMemoryBackedStoreWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.bytes.extend_from_slice(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl AsyncWrite for NewMemoryBackedStoreWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(std::io::Write::write(self.get_mut(), buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
        Poll::Ready(std::io::Write::flush(self.get_mut()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.poll_flush(cx)
    }
}

impl FileStore for NewMemoryBackedStore {
    type Write = NewMemoryBackedStoreWriter;

    fn open_write(&self) -> Self::Write {
        NewMemoryBackedStoreWriter {
            file: self.clone(),
            bytes: BytesMut::new(),
        }
    }
}

pub struct NewMemoryBackedStoreReader {
    bytes: Bytes,
    pos: usize,
}

impl std::io::Read for NewMemoryBackedStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        if self.bytes.len() == self.pos {
            // end of file
            Ok(0)
        } else if self.bytes.len() < self.pos + buf.len() {
            // read up to end
            let len = self.bytes.len() - self.pos;
            buf[..len].copy_from_slice(&self.bytes[self.pos..]);

            self.pos += len;

            Ok(len)
        } else {
            // read full buf
            buf.copy_from_slice(&self.bytes[self.pos..self.pos + buf.len()]);

            self.pos += buf.len();

            Ok(buf.len())
        }
    }
}

impl AsyncRead for NewMemoryBackedStoreReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        let slice = buf.initialize_unfilled();
        let count = std::io::Read::read(self.get_mut(), slice);
        if count.is_ok() {
            buf.advance(*count.as_ref().unwrap());
        }

        Poll::Ready(count.map(|_| ()))
    }
}

impl FileLoad for NewMemoryBackedStore {
    type Read = NewMemoryBackedStoreReader;

    fn exists(&self) -> bool {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => false,
            _ => true,
        }
    }

    fn size(&self) -> usize {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to retrieve size of nonexistent memory file")
            }
            MemoryBackedStoreContents::Existent(bytes) => bytes.len(),
        }
    }

    fn open_read_from(&self, offset: usize) -> NewMemoryBackedStoreReader {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to open nonexistent memory file for reading")
            }
            MemoryBackedStoreContents::Existent(bytes) => NewMemoryBackedStoreReader {
                bytes: bytes.clone(),
                pos: offset,
            },
        }
    }

    fn map(&self) -> Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to open nonexistent memory file for reading")
            }
            MemoryBackedStoreContents::Existent(bytes) => Box::pin(future::ok(bytes.clone())),
        }
    }
}

#[derive(Clone, Default)]
pub struct NewMemoryLayerStore {
    layers: futures_locks::RwLock<HashMap<[u32; 5], HashMap<String, NewMemoryBackedStore>>>,
}

impl PersistentLayerStore for NewMemoryLayerStore {
    type File = NewMemoryBackedStore;

    fn directories(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move { Ok(guard.await.keys().cloned().collect()) })
    }

    fn create_directory(&self) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let guard = self.layers.write();
        Box::pin(async move {
            let name: [u32; 5] = rand::random();
            guard.await.insert(name, HashMap::new());

            Ok(name)
        })
    }

    fn directory_exists(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move { Ok(guard.await.contains_key(&name)) })
    }

    fn file_exists(
        &self,
        directory: [u32; 5],
        file: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let guard = self.layers.read();
        let file = file.to_owned();
        Box::pin(async move {
            if let Some(files) = guard.await.get(&directory) {
                Ok(files.contains_key(&file))
            } else {
                Ok(false)
            }
        })
    }

    fn get_file(
        &self,
        directory: [u32; 5],
        name: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::File>> + Send>> {
        let guard = self.layers.read();
        let name = name.to_owned();
        Box::pin(async move {
            if let Some(files) = guard.await.get(&directory) {
                if let Some(file) = files.get(&name) {
                    Ok(file.clone())
                } else {
                    Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
                }
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        todo!();
    }

    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        todo!();
    }
}
