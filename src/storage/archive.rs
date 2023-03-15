// File format:
// <header>
//  [<filetype present>]*
//  [<offsets>]*
//

use std::{
    collections::HashMap,
    io::{self, ErrorKind, SeekFrom},
    ops::Range,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
    task::Poll,
};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lru::LruCache;
use tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
};

use crate::structure::{
    logarray_length_from_control_word, smallbitarray::SmallBitArray, LateLogArrayBufBuilder,
    MonotonicLogArray,
};

use super::{
    consts::{LayerFileEnum, FILENAME_ENUM_MAP},
    name_to_string, string_to_name, FileLoad, FileStore, PersistentLayerStore, SyncableFile,
};

#[async_trait]
pub trait ArchiveBackend: Clone + Send + Sync {
    type Read: AsyncRead + Unpin + Send;
    async fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes>;
    async fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Bytes>;
    async fn store_layer_file<B: Buf + Send>(&self, id: [u32; 5], bytes: B) -> io::Result<()>;
    async fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read>;
}

#[async_trait]
pub trait ArchiveMetadataBackend: Clone + Send + Sync {
    async fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>>;
    async fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool>;
    async fn layer_size(&self, id: [u32; 5]) -> io::Result<u64>;
    async fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool>;
    async fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize>;
    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()>;
}

pub struct BytesAsyncReader(Bytes);

impl AsyncRead for BytesAsyncReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let bytes = &mut self.get_mut().0;
        let consumed = if buf.remaining() > bytes.len() {
            bytes.split_to(bytes.len())
        } else {
            bytes.split_to(buf.remaining())
        };

        buf.put_slice(consumed.as_ref());

        Poll::Ready(Ok(()))
    }
}

#[derive(Clone)]
pub struct DirectoryArchiveBackend {
    path: PathBuf,
}

impl DirectoryArchiveBackend {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
    fn path_for_layer(&self, name: [u32; 5]) -> PathBuf {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(&format!("{}.larch", name_str));

        p
    }

    fn path_for_rollup(&self, name: [u32; 5]) -> PathBuf {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(&format!("{}.rollup.hex", name_str));

        p
    }
}

#[async_trait]
impl ArchiveBackend for DirectoryArchiveBackend {
    type Read = ArchiveSliceReader;
    async fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes> {
        let path = self.path_for_layer(id);
        let mut options = fs::OpenOptions::new();
        options.read(true);
        options.create(false);
        let mut result = options.open(path).await?;
        let mut buf = Vec::new();
        result.read_to_end(&mut buf).await?;

        Ok(buf.into())
    }

    async fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Bytes> {
        let path = self.path_for_layer(id);
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path).await?;
        let archive = Archive::parse_from_reader(&mut file).await?;
        archive
            .slice_for(file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }

    async fn store_layer_file<B: Buf + Send>(&self, id: [u32; 5], mut bytes: B) -> io::Result<()> {
        let path = self.path_for_layer(id);
        let mut directory_path = path.clone();
        directory_path.pop();
        fs::create_dir_all(directory_path).await?;

        let mut options = tokio::fs::OpenOptions::new();
        options.create(true);
        options.write(true);
        let mut file = options.open(path).await?;
        while bytes.remaining() > 0 {
            let chunk = bytes.chunk();
            let written = file.write(chunk).await?;
            bytes.advance(written);
        }

        file.flush().await?;
        file.sync_data().await?;

        Ok(())
    }

    async fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read> {
        let path = self.path_for_layer(id);
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path).await?;
        let header = ArchiveHeader::parse_from_reader(&mut file).await?;

        let range = header
            .range_for(file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))?;

        let remaining = range.len() - read_from;
        file.seek(SeekFrom::Current((range.start + read_from) as i64))
            .await?;

        Ok(ArchiveSliceReader { file, remaining })
    }
}

#[async_trait]
impl ArchiveMetadataBackend for DirectoryArchiveBackend {
    async fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut stream = fs::read_dir(&self.path).await?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next_entry().await? {
            let os_name = direntry.file_name();
            let name = os_name.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected non-utf8 directory name",
                )
            })?;
            if name.ends_with(".larch") && direntry.file_type().await?.is_file() {
                let name_component = &name[..name.len() - 6];
                result.push(string_to_name(name_component)?);
            }
        }

        Ok(result)
    }

    async fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool> {
        let path = self.path_for_layer(id);
        let metadata = tokio::fs::metadata(path).await;
        if metadata.is_err() && metadata.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
            // layer itself not found
            return Ok(false);
        }
        // propagate error if it was anything but NotFound
        metadata?;

        // if we got here it means the layer exists
        Ok(true)
    }

    async fn layer_size(&self, id: [u32; 5]) -> io::Result<u64> {
        let path = self.path_for_layer(id);
        let metadata = tokio::fs::metadata(path).await?;
        Ok(metadata.len())
    }

    async fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool> {
        let path = self.path_for_layer(id);
        let metadata = tokio::fs::metadata(&path).await;
        if metadata.is_err() && metadata.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
            // layer itself not found
            return Ok(false);
        }
        // propagate error if it was anything but NotFound
        metadata?;

        // read header!
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path).await?;
        let header = ArchiveFilePresenceHeader::new(file.read_u64().await?);

        Ok(header.is_present(file_type))
    }

    async fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize> {
        let path = self.path_for_layer(id);
        // read header!
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path).await?;
        let header = ArchiveHeader::parse_from_reader(&mut file).await?;

        header
            .size_of(file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }

    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        let path = self.path_for_rollup(id);
        let result = fs::read_to_string(path).await;

        if result.is_err() && result.as_ref().err().unwrap().kind() == ErrorKind::NotFound {
            return Ok(None);
        }
        let data = result?;
        let name = data.lines().skip(1).next().unwrap();
        Ok(Some(string_to_name(&name)?))
    }

    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        let path = self.path_for_rollup(id);
        let mut data = Vec::with_capacity(43);
        data.extend_from_slice(b"1\n");
        data.extend_from_slice(name_to_string(rollup).as_bytes());
        data.extend_from_slice(b"\n");
        fs::write(path, data).await
    }
}

#[derive(Clone)]
pub struct LruArchiveBackend<T> {
    cache: Arc<tokio::sync::Mutex<LruCache<[u32; 5], CacheEntry>>>,
    limit: usize,
    current: usize,
    origin: T,
}

#[derive(Clone)]
enum CacheEntry {
    Resolving(Arc<tokio::sync::RwLock<Option<Result<Bytes, io::ErrorKind>>>>),
    Resolved(Bytes),
}

impl CacheEntry {
    fn is_resolving(&self) -> bool {
        if let Self::Resolving(_) = self {
            true
        } else {
            false
        }
    }
}

impl<T> LruArchiveBackend<T> {
    pub fn new(origin: T, limit: usize) -> Self {
        let cache = Arc::new(tokio::sync::Mutex::new(LruCache::unbounded()));

        Self {
            cache,
            limit,
            current: 0,
            origin,
        }
    }

    fn limit_bytes(&self) -> usize {
        self.limit * 1024 * 1024
    }
}

fn ensure_additional_cache_space(cache: &mut LruCache<[u32; 5], CacheEntry>, mut required: usize) {
    if required == 0 {
        return;
    }

    loop {
        let peek = cache
            .peek_lru()
            .expect("cache is empty but stored entries were expected");
        if peek.1.is_resolving() {
            // this is a resolving entry, we don't want to pop it.
            let id = peek.0.clone();
            std::mem::drop(peek);
            cache.promote(&id);
            continue;
        }
        // at this point the lru item is not resolving
        let entry = cache
            .pop_lru()
            .expect("cache is empty but stored entries were expected")
            .1;
        if let CacheEntry::Resolved(entry) = entry {
            if entry.len() >= required {
                // done!
                return;
            }

            // more needs to be popped
            required -= entry.len();
        } else {
            panic!("expected resolved entry but got a resolving");
        }
    }
}

fn ensure_enough_cache_space(
    cache: &mut LruCache<[u32; 5], CacheEntry>,
    limit: usize,
    current: usize,
    required: usize,
) -> bool {
    if required > limit {
        // this entry is too big for the cache
        return false;
    }

    let remaining = limit - current;
    if remaining < required {
        // we need to clean up some cache spacew to fit this entry
        ensure_additional_cache_space(cache, required - remaining);
    }

    true
}

fn drop_from_cache(cache: &mut LruCache<[u32; 5], CacheEntry>, id: [u32; 5]) {
    assert!(cache.contains(&id));
    cache.demote(&id);
    cache.pop_lru();
}

#[async_trait]
impl<T: ArchiveBackend> ArchiveBackend for LruArchiveBackend<T> {
    type Read = BytesAsyncReader;
    async fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes> {
        let mut cache = self.cache.lock().await;
        let cached = cache.get(&id).cloned();

        match cached {
            Some(CacheEntry::Resolved(bytes)) => Ok(bytes),
            Some(CacheEntry::Resolving(barrier)) => {
                // someone is already looking up this layer. we'll wait for them to be done.
                std::mem::drop(cache);
                let guard = barrier.read().await;
                match guard.as_ref().unwrap() {
                    Ok(bytes) => Ok(bytes.clone()),
                    Err(kind) => Err(io::Error::new(*kind, "layer resolve failed")),
                }
            }
            None => {
                // nobody is looking this up yet, it is up to us.
                let barrier = Arc::new(tokio::sync::RwLock::new(None));
                let mut result = barrier.write().await;
                cache.get_or_insert(id, || CacheEntry::Resolving(barrier.clone()));

                // drop the cache while doing the lookup
                std::mem::drop(cache);
                let lookup = self.origin.get_layer_bytes(id).await;

                *result = Some(lookup.as_ref().map_err(|e| e.kind()).cloned());

                // reacquire cache
                let mut cache = self.cache.lock().await;
                match lookup {
                    Ok(bytes) => {
                        if ensure_enough_cache_space(
                            &mut *cache,
                            self.limit_bytes(),
                            self.current,
                            bytes.len(),
                        ) {
                            let cached = cache
                                .get_mut(&id)
                                .expect("layer resolving entry not found in cache");
                            *cached = CacheEntry::Resolved(bytes.clone());
                        } else {
                            // this entry is uncachable. Just remove the resolving entry
                            drop_from_cache(&mut *cache, id);
                        }
                        Ok(bytes)
                    }
                    Err(e) => {
                        drop_from_cache(&mut *cache, id);

                        Err(e)
                    }
                }
            }
        }
    }
    async fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Bytes> {
        let bytes = self.get_layer_bytes(id).await?;
        let archive = Archive::parse(bytes);
        archive
            .slice_for(file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }
    async fn store_layer_file<B: Buf + Send>(&self, id: [u32; 5], bytes: B) -> io::Result<()> {
        // TODO immediately cache ?
        self.origin.store_layer_file(id, bytes).await
    }
    async fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read> {
        let mut bytes = self.get_layer_structure_bytes(id, file_type).await?;
        bytes.advance(read_from);

        Ok(BytesAsyncReader(bytes))
    }
}

#[derive(Clone)]
pub struct LruMetadataArchiveBackend<M, D> {
    metadata_backend: M,
    data_backend: LruArchiveBackend<D>,
}

impl<M, D> LruMetadataArchiveBackend<M, D> {
    pub fn new(metadata_backend: M, data_backend: LruArchiveBackend<D>) -> Self {
        Self {
            metadata_backend,
            data_backend,
        }
    }
}

#[async_trait]
impl<M: ArchiveMetadataBackend, D: ArchiveBackend> ArchiveMetadataBackend
    for LruMetadataArchiveBackend<M, D>
{
    async fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>> {
        self.metadata_backend.get_layer_names().await
    }
    async fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool> {
        if let Some(CacheEntry::Resolved(_)) = self.data_backend.cache.lock().await.peek(&id) {
            Ok(true)
        } else {
            self.metadata_backend.layer_exists(id).await
        }
    }
    async fn layer_size(&self, id: [u32; 5]) -> io::Result<u64> {
        if let Some(CacheEntry::Resolved(bytes)) = self.data_backend.cache.lock().await.peek(&id) {
            Ok(bytes.len() as u64)
        } else {
            self.metadata_backend.layer_size(id).await
        }
    }
    async fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool> {
        if let Some(CacheEntry::Resolved(bytes)) = self.data_backend.cache.lock().await.peek(&id) {
            let header = ArchiveFilePresenceHeader::new(bytes.clone().get_u64());
            Ok(header.is_present(file_type))
        } else {
            self.metadata_backend.layer_file_exists(id, file_type).await
        }
    }
    async fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize> {
        if let Some(CacheEntry::Resolved(bytes)) = self.data_backend.cache.lock().await.peek(&id) {
            let (header, _) = ArchiveHeader::parse(bytes.clone());

            if let Some(size) = header.size_of(file_type) {
                Ok(size)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "structure {file_type:?} not found in layer {}",
                        name_to_string(id)
                    ),
                ))
            }
        } else {
            self.metadata_backend
                .get_layer_structure_size(id, file_type)
                .await
        }
    }
    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        self.metadata_backend.get_rollup(id).await
    }
    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        self.metadata_backend.set_rollup(id, rollup).await
    }
}

pub enum ConstructionFileState {
    UnderConstruction(BytesMut),
    Finalizing,
    Finalized(Bytes),
}

#[derive(Clone)]
pub struct ConstructionFile(Arc<RwLock<ConstructionFileState>>);

impl ConstructionFile {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(
            ConstructionFileState::UnderConstruction(BytesMut::new()),
        )))
    }

    fn new_finalized(bytes: Bytes) -> Self {
        Self(Arc::new(RwLock::new(ConstructionFileState::Finalized(
            bytes,
        ))))
    }

    fn is_finalized(&self) -> bool {
        let guard = self.0.read().unwrap();
        if let ConstructionFileState::Finalized(_) = &*guard {
            true
        } else {
            false
        }
    }

    fn finalized_buf(self) -> Bytes {
        let guard = self.0.read().unwrap();
        if let ConstructionFileState::Finalized(bytes) = &*guard {
            bytes.clone()
        } else {
            panic!("tried to get the finalized buf from an unfinalized ConstructionFile");
        }
    }
}

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
        Ok(matches!(&*guard, ConstructionFileState::Finalized(_)))
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
            ConstructionFileState::Finalized(data) => {
                let mut data = data.clone();
                if data.len() < offset {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "offset is beyond end of file",
                    ))
                } else {
                    data.advance(offset);
                    // this is suspicious, why would we need a lock here? Maybe we should have a different reader type from the file type
                    Ok(ConstructionFile(Arc::new(RwLock::new(
                        ConstructionFileState::Finalized(data),
                    ))))
                }
            }
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ArchiveHeader {
    file_presence: ArchiveFilePresenceHeader,
    file_offsets: MonotonicLogArray,
}

impl ArchiveHeader {
    pub fn parse(mut bytes: Bytes) -> (Self, Bytes) {
        let file_presence = ArchiveFilePresenceHeader::new(bytes.get_u64());
        let (file_offsets, remainder) = MonotonicLogArray::parse_header_first(bytes)
            .expect("unable to parse structure offsets");

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
        logarray_bytes.reserve(len);
        unsafe {
            logarray_bytes.set_len(8 + len);
        }
        reader.read_exact(&mut logarray_bytes[8..]).await?;

        let (file_offsets, _) =
            MonotonicLogArray::parse_header_first(logarray_bytes.freeze()).expect("what the heck");

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
        self.range_for(file).map(|range| range.end - range.start)
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
        data.reserve(data_len);
        unsafe { data.set_len(data_len) };
        reader.read_exact(&mut data[..]).await?;

        Ok(Self {
            header,
            data: data.freeze(),
        })
    }

    pub fn slice_for(&self, file: LayerFileEnum) -> Option<Bytes> {
        self.header
            .range_for(file)
            .map(|range| self.data.slice(range))
    }

    pub fn size_of(&self, file: LayerFileEnum) -> Option<usize> {
        self.header.size_of(file)
    }
}

#[derive(Clone)]
pub struct PersistentFileSlice<M, D> {
    metadata_backend: M,
    data_backend: D,
    layer_id: [u32; 5],
    file_type: LayerFileEnum,
}

impl<M, D> PersistentFileSlice<M, D> {
    fn new(
        metadata_backend: M,
        data_backend: D,
        layer_id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> Self {
        Self {
            metadata_backend,
            data_backend,
            layer_id,
            file_type,
        }
    }
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

        let read = AsyncRead::poll_read(Pin::new(&mut self.file), cx, buf);
        if let Poll::Pending = read {
            return Poll::Pending;
        }

        if buf.filled().len() > self.remaining {
            buf.set_filled(self.remaining);
        }

        self.remaining -= buf.filled().len();

        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl<M: ArchiveMetadataBackend, D: ArchiveBackend> FileLoad for PersistentFileSlice<M, D> {
    type Read = D::Read;

    async fn exists(&self) -> io::Result<bool> {
        self.metadata_backend
            .layer_file_exists(self.layer_id, self.file_type)
            .await
    }

    async fn size(&self) -> io::Result<usize> {
        self.metadata_backend
            .get_layer_structure_size(self.layer_id, self.file_type)
            .await
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        self.data_backend
            .read_layer_structure_bytes_from(self.layer_id, self.file_type, offset)
            .await
    }

    async fn map(&self) -> io::Result<Bytes> {
        self.data_backend
            .get_layer_structure_bytes(self.layer_id, self.file_type)
            .await
    }
}

// This is some pretty ridiculous contrived logic but it saves having to refactor some other places which should just take a rollup id in the first place.
#[derive(Clone)]
pub struct ArchiveRollupFile<M> {
    layer_id: [u32; 5],
    metadata_backend: M,
}

#[async_trait]
impl<M: ArchiveMetadataBackend> FileLoad for ArchiveRollupFile<M> {
    type Read = BytesAsyncReader;

    async fn exists(&self) -> io::Result<bool> {
        Ok(self
            .metadata_backend
            .get_rollup(self.layer_id)
            .await?
            .is_some())
    }

    async fn size(&self) -> io::Result<usize> {
        if self
            .metadata_backend
            .get_rollup(self.layer_id)
            .await?
            .is_some()
        {
            Ok(std::mem::size_of::<[u32; 5]>() + 2)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "layer has no rollup",
            ))
        }
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        let mut bytes = self.map().await?;
        bytes.advance(offset);
        Ok(BytesAsyncReader(bytes))
    }

    async fn map(&self) -> io::Result<Bytes> {
        let id = self.metadata_backend.get_rollup(self.layer_id).await?;
        if let Some(id) = id {
            let mut bytes = Vec::with_capacity(42);
            bytes.extend_from_slice(b"1\n");
            bytes.extend_from_slice(name_to_string(id).as_bytes());
            Ok(bytes.into())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "layer has no rollup",
            ))
        }
    }
}

#[async_trait]
impl<M: ArchiveMetadataBackend + Unpin> FileStore for ArchiveRollupFile<M> {
    type Write = ArchiveRollupFileWriter<M>;
    async fn open_write(&self) -> io::Result<Self::Write> {
        Ok(ArchiveRollupFileWriter {
            layer_id: self.layer_id,
            data: BytesMut::new(),
            metadata_backend: self.metadata_backend.clone(),
        })
    }
}

pub struct ArchiveRollupFileWriter<M> {
    layer_id: [u32; 5],
    data: BytesMut,
    metadata_backend: M,
}

impl<M: ArchiveMetadataBackend + Unpin> AsyncWrite for ArchiveRollupFileWriter<M> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.get_mut().data.extend_from_slice(buf);

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
impl<M: ArchiveMetadataBackend + Unpin> SyncableFile for ArchiveRollupFileWriter<M> {
    async fn sync_all(self) -> io::Result<()> {
        let rollup_string =
            String::from_utf8(self.data.to_vec()).expect("rollup id was not a string");
        // first line of this string is going to be a version number. it should be discarded.
        let line = rollup_string.lines().skip(1).next().unwrap();
        let rollup_id = string_to_name(&line)?;

        self.metadata_backend
            .set_rollup(self.layer_id, rollup_id)
            .await
    }
}

#[derive(Clone)]
pub enum ArchiveLayerHandle<M, D> {
    Construction(ConstructionFile),
    Persistent(PersistentFileSlice<M, D>),
    Rollup(ArchiveRollupFile<M>),
}

#[async_trait]
impl<M: ArchiveMetadataBackend + Unpin, D: ArchiveBackend> FileStore for ArchiveLayerHandle<M, D> {
    type Write = ArchiveLayerHandleWriter<M>;
    async fn open_write(&self) -> io::Result<Self::Write> {
        Ok(match self {
            Self::Construction(c) => ArchiveLayerHandleWriter::Construction(c.open_write().await?),
            Self::Rollup(r) => ArchiveLayerHandleWriter::Rollup(r.open_write().await?),
            _ => panic!("cannot write to a persistent file slice"),
        })
    }
}

#[async_trait]
impl<M: ArchiveMetadataBackend, D: ArchiveBackend> FileLoad for ArchiveLayerHandle<M, D> {
    type Read = ArchiveLayerHandleReader<D::Read, BytesAsyncReader>;

    async fn exists(&self) -> io::Result<bool> {
        match self {
            Self::Construction(c) => c.exists().await,
            Self::Persistent(p) => p.exists().await,
            Self::Rollup(r) => r.exists().await,
        }
    }
    async fn size(&self) -> io::Result<usize> {
        match self {
            Self::Construction(c) => c.size().await,
            Self::Persistent(p) => p.size().await,
            Self::Rollup(r) => r.size().await,
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
            Self::Rollup(r) => ArchiveLayerHandleReader::Rollup(r.open_read_from(offset).await?),
        })
    }

    async fn map(&self) -> io::Result<Bytes> {
        match self {
            Self::Construction(c) => c.map().await,
            Self::Persistent(p) => p.map().await,
            Self::Rollup(r) => r.map().await,
        }
    }
}

pub enum ArchiveLayerHandleReader<P, R> {
    Construction(ConstructionFile),
    Persistent(P),
    Rollup(R),
}

impl<P: AsyncRead + Unpin, R: AsyncRead + Unpin> AsyncRead for ArchiveLayerHandleReader<P, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Construction(c) => AsyncRead::poll_read(Pin::new(c), cx, buf),
            Self::Persistent(p) => AsyncRead::poll_read(Pin::new(p), cx, buf),
            Self::Rollup(r) => AsyncRead::poll_read(Pin::new(r), cx, buf),
        }
    }
}

pub enum ArchiveLayerHandleWriter<M> {
    Construction(ConstructionFile),
    Rollup(ArchiveRollupFileWriter<M>),
}

impl<M: ArchiveMetadataBackend + Unpin> AsyncWrite for ArchiveLayerHandleWriter<M> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match &mut *self {
            Self::Construction(c) => AsyncWrite::poll_write(Pin::new(c), cx, buf),
            Self::Rollup(r) => AsyncWrite::poll_write(Pin::new(r), cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match &mut *self {
            Self::Construction(c) => AsyncWrite::poll_flush(Pin::new(c), cx),
            Self::Rollup(r) => AsyncWrite::poll_flush(Pin::new(r), cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match &mut *self {
            Self::Construction(c) => AsyncWrite::poll_shutdown(Pin::new(c), cx),
            Self::Rollup(r) => AsyncWrite::poll_shutdown(Pin::new(r), cx),
        }
    }
}

#[async_trait]
impl<M: ArchiveMetadataBackend + Unpin> SyncableFile for ArchiveLayerHandleWriter<M> {
    async fn sync_all(self) -> io::Result<()> {
        match self {
            Self::Construction(c) => c.sync_all().await,
            Self::Rollup(r) => r.sync_all().await,
        }
    }
}

type ArchiveLayerConstructionMap =
    Arc<RwLock<HashMap<[u32; 5], HashMap<LayerFileEnum, ConstructionFile>>>>;

#[derive(Clone)]
pub struct ArchiveLayerStore<M, D> {
    metadata_backend: M,
    data_backend: D,
    construction: ArchiveLayerConstructionMap,
}

impl<M, D> ArchiveLayerStore<M, D> {
    pub fn new(metadata_backend: M, data_backend: D) -> ArchiveLayerStore<M, D> {
        ArchiveLayerStore {
            metadata_backend,
            data_backend,
            construction: Default::default(),
        }
    }

    #[doc(hidden)]
    pub fn write_bytes(&self, name: [u32; 5], file: LayerFileEnum, bytes: Bytes) {
        let mut guard = self.construction.write().unwrap();
        if let Some(map) = guard.get_mut(&name) {
            if map.contains_key(&file) {
                panic!("tried to write bytes to an archive, but file is already open");
            }

            map.insert(file, ConstructionFile::new_finalized(bytes));
        } else {
            panic!("tried to write bytes to an archive, but layer is not under construction");
        }
    }
}

const PREFIX_DIR_SIZE: usize = 3;

#[async_trait]
impl<M: ArchiveMetadataBackend + Unpin + 'static, D: ArchiveBackend + 'static> PersistentLayerStore
    for ArchiveLayerStore<M, D>
{
    type File = ArchiveLayerHandle<M, D>;

    async fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut result = self.metadata_backend.get_layer_names().await?;

        {
            let guard = self.construction.read().unwrap();

            for name in guard.keys() {
                result.push(*name);
            }
        }

        result.sort();
        result.dedup();

        Ok(result)
    }

    async fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        if !self.metadata_backend.layer_exists(name).await? {
            // layer does not exist yet on disk, good.
            let mut guard = self.construction.write().unwrap();
            if guard.contains_key(&name) {
                // whoops! Looks like layer is already under construction!
                panic!("tried to create a new layer which is already under construction");
            }

            // layer is neither on disk nor in the construction map. Let's create it.
            guard.insert(name, HashMap::new());
            return Ok(name);
        } else {
            // still here? That means the file existed, even though it shouldn't!
            panic!("tried to create a new layer which already exists");
        }
    }

    async fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        {
            let guard = self.construction.read().unwrap();
            if guard.contains_key(&name) {
                return Ok(true);
            }
        }

        self.metadata_backend.layer_exists(name).await
    }

    async fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let file_type = FILENAME_ENUM_MAP[name];
        if file_type == LayerFileEnum::Rollup {
            // special case! This is always coming from disk, in its own file
            return Ok(ArchiveLayerHandle::Rollup(ArchiveRollupFile {
                layer_id: directory,
                metadata_backend: self.metadata_backend.clone(),
            }));
        }

        {
            let guard = self.construction.read().unwrap();
            if let Some(map) = guard.get(&directory) {
                if let Some(file) = map.get(&file_type) {
                    return Ok(ArchiveLayerHandle::Construction(file.clone()));
                }

                // the directory is there but the file is not. We'll have to construct it.
                std::mem::drop(guard);
                let mut guard = self.construction.write().unwrap();
                let map = guard.get_mut(&directory).unwrap();
                let file = ConstructionFile::new();
                map.insert(file_type, file.clone());

                Ok(ArchiveLayerHandle::Construction(file))
            } else {
                // layer does not appear to be under construction so it has to be in persistent storage
                Ok(ArchiveLayerHandle::Persistent(PersistentFileSlice::new(
                    self.metadata_backend.clone(),
                    self.data_backend.clone(),
                    directory,
                    file_type,
                )))
            }
        }
    }

    async fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let file_type = FILENAME_ENUM_MAP[file];
        if file_type == LayerFileEnum::Rollup {
            // special case! This is always coming out of the persistent metadata
            return Ok(self.metadata_backend.get_rollup(directory).await?.is_some());
        }

        {
            let guard = self.construction.read().unwrap();
            if let Some(map) = guard.get(&directory) {
                return Ok(map.contains_key(&file_type));
            }
        }

        self.metadata_backend
            .layer_file_exists(directory, file_type)
            .await
    }

    async fn finalize(&self, directory: [u32; 5]) -> io::Result<()> {
        let files = {
            let mut guard = self.construction.write().unwrap();
            guard
                .remove(&directory)
                .expect("layer to be finalized was not found in construction map")
        };

        let mut files: Vec<(_, _)> = files
            .into_iter()
            .filter(|(_file_type, file)| file.is_finalized())
            .map(|(file_type, file)| (file_type, file.finalized_buf()))
            .collect();
        files.sort();
        let presence_header =
            ArchiveFilePresenceHeader::from_present(files.iter().map(|(t, _)| t).cloned());

        let mut offsets = LateLogArrayBufBuilder::new(BytesMut::new());
        let mut tally = 0;
        for (_file_type, data) in files.iter() {
            tally += data.len();
            offsets.push(tally as u64);
        }

        let offsets_buf = offsets.finalize_header_first();

        let mut data_buf = BytesMut::new();
        data_buf.put_u64(presence_header.inner());
        data_buf.extend(offsets_buf);
        for (_file_type, data) in files {
            data_buf.extend(data);
        }

        self.data_backend
            .store_layer_file(directory, data_buf)
            .await
    }
}

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
