//! storage traits that the builders and loaders can rely on

use tokio::prelude::*;
use std::sync::{Arc,RwLock};
use std::io;

pub trait FileStore {
    type Write: AsyncWrite;
    fn open_write(&self) -> Self::Write;
}

pub trait FileLoad {
    type Read: AsyncRead;
    type Map: AsRef<[u8]>;
    
    fn size(&self) -> usize;
    fn open_read(&self) -> Self::Read;
    fn open_read_from(&self, offset: usize) -> Self::Read;
    fn map(&self) -> Self::Map;
}

pub struct MemoryBackedStoreWriter {
    vec: Arc<RwLock<Vec<u8>>>
}

impl Write for MemoryBackedStoreWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let mut v = self.vec.write().unwrap();
        v.extend_from_slice(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl AsyncWrite for MemoryBackedStoreWriter {
    fn shutdown(&mut self) -> Result<Async<()>, io::Error> {
        Ok(Async::Ready(()))
    }
}

pub struct MemoryBackedStoreReader {
    vec: Arc<RwLock<Vec<u8>>>,
    pos: usize
}

impl Read for MemoryBackedStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let v = self.vec.read().unwrap();

        if self.pos >= v.len() {
            return Ok(0);
        }

        let slice = &v[self.pos..];
        if slice.len() >= buf.len() {
            buf.copy_from_slice(&slice[..buf.len()]);
            self.pos += buf.len();

            Ok(buf.len())
        }
        else {
            buf[..slice.len()].copy_from_slice(slice);
            self.pos += slice.len();

            Ok(slice.len())
        }
    }
}

impl AsyncRead for MemoryBackedStoreReader {
}

#[derive(Clone)]
pub struct MemoryBackedStore {
    vec: Arc<RwLock<Vec<u8>>>
}

impl MemoryBackedStore {
    pub fn new() -> MemoryBackedStore {
        MemoryBackedStore { vec: Default::default() }
    }
}

impl FileStore for MemoryBackedStore {
    type Write = MemoryBackedStoreWriter;

    fn open_write(&self) -> MemoryBackedStoreWriter {
        let mut v = self.vec.write().unwrap();
        v.truncate(0);

        MemoryBackedStoreWriter { vec: self.vec.clone() }
    }
}

impl FileLoad for MemoryBackedStore {
    type Read = MemoryBackedStoreReader;
    type Map = Vec<u8>;

    fn size(&self) -> usize {
        self.vec.read().unwrap().len()
    }

    fn open_read(&self) -> MemoryBackedStoreReader {
        MemoryBackedStoreReader { vec: self.vec.clone(), pos: 0 }
    }

    fn open_read_from(&self, offset: usize) -> MemoryBackedStoreReader {
        MemoryBackedStoreReader { vec: self.vec.clone(), pos: offset }
    }

    fn map(&self) -> Vec<u8> {
        self.vec.read().unwrap().clone()
    }
}
