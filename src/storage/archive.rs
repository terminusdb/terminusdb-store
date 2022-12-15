// File format:
// <header>
//  [<filetype present>]*
//  [<offsets>]*
//

use std::{
    collections::HashMap,
    io::{self, SeekFrom},
    ops::Range,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
    task::Poll,
};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufWriter},
};

use crate::structure::{
    logarray_length_from_control_word, smallbitarray::SmallBitArray, LateLogArrayBufBuilder,
    MonotonicLogArray,
};

use super::{
    consts::{LayerFileEnum, FILENAME_ENUM_MAP},
    directory::FileBackedStore,
    name_to_string, string_to_name, FileLoad, FileStore, PersistentLayerStore, SyncableFile,
};

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

impl ConstructionFile {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(
            ConstructionFileState::UnderConstruction(BytesMut::new()),
        )))
    }

    fn new_finalized(bytes: Bytes) -> Self {
        Self(Arc::new(RwLock::new(
            ConstructionFileState::Finalized(bytes),
        )))
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
        Ok(match &*guard {
            ConstructionFileState::Finalized(_) => true,
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

#[derive(Debug)]
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

#[derive(Debug)]
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
        data.reserve(data_len);
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

impl PersistentFileSlice {
    fn new<P: Into<PathBuf>>(path: P, file_type: LayerFileEnum) -> Self {
        Self {
            path: path.into(),
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
        let mut remaining = header
            .size_of(self.file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))?;

        if remaining < offset {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset is past end of file slice",
            ))
        } else {
            let total_offset = header.range_for(self.file_type).unwrap().start + offset;
            file.seek(SeekFrom::Current(total_offset as i64)).await?;

            remaining -= offset;
            Ok(ArchiveSliceReader { file, remaining })
        }
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
    Rollup(FileBackedStore),
}

#[async_trait]
impl FileStore for ArchiveLayerHandle {
    type Write = ArchiveLayerHandleWriter;
    async fn open_write(&self) -> io::Result<Self::Write> {
        Ok(match self {
            Self::Construction(c) => ArchiveLayerHandleWriter::Construction(c.open_write().await?),
            Self::Rollup(r) => ArchiveLayerHandleWriter::Rollup(r.open_write().await?),
            _ => panic!("cannot write to a persistent file slice"),
        })
    }
}

#[async_trait]
impl FileLoad for ArchiveLayerHandle {
    type Read = ArchiveLayerHandleReader;

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

pub enum ArchiveLayerHandleReader {
    Construction(ConstructionFile),
    Persistent(ArchiveSliceReader),
    Rollup(File),
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
            Self::Rollup(r) => AsyncRead::poll_read(Pin::new(r), cx, buf),
        }
    }
}

pub enum ArchiveLayerHandleWriter {
    Construction(ConstructionFile),
    Rollup(BufWriter<File>),
}

impl AsyncWrite for ArchiveLayerHandleWriter {
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
impl SyncableFile for ArchiveLayerHandleWriter {
    async fn sync_all(self) -> io::Result<()> {
        match self {
            Self::Construction(c) => c.sync_all().await,
            Self::Rollup(r) => r.sync_all().await,
        }
    }
}

#[derive(Clone)]
pub struct ArchiveLayerStore {
    path: PathBuf,
    construction: Arc<RwLock<HashMap<[u32; 5], HashMap<LayerFileEnum, ConstructionFile>>>>,
}

impl ArchiveLayerStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> ArchiveLayerStore {
        ArchiveLayerStore {
            path: path.into(),
            construction: Default::default(),
        }
    }

    #[doc(hidden)]
    pub fn write_bytes(&self, name: [u32;5], file: LayerFileEnum, bytes: Bytes) {
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

const PREFIX_DIR_SIZE: usize = 2;

#[async_trait]
impl PersistentLayerStore for ArchiveLayerStore {
    type File = ArchiveLayerHandle;

    async fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
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
        let p = self.path_for_layer(name);
        let meta = fs::metadata(p).await;
        if meta.is_err() && meta.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
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
            // return error if it is there
            meta?;

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

        let p = self.path_for_layer(name);
        let meta = fs::metadata(p).await;

        if meta.is_err() && meta.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
            return Ok(false);
        }
        let meta = meta?;

        if !meta.is_file() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "layer was not a file",
            ))
        } else {
            Ok(true)
        }
    }

    async fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let file_type = FILENAME_ENUM_MAP[name];
        if file_type == LayerFileEnum::Rollup {
            // special case! This is always coming from disk, in its own file
            let p = self.path_for_rollup(directory);

            return Ok(ArchiveLayerHandle::Rollup(FileBackedStore::new(p)));
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
                // layer does not appear to be under construction so it has to be on disk
                let p = self.path_for_layer(directory);

                Ok(ArchiveLayerHandle::Persistent(PersistentFileSlice::new(
                    p, file_type,
                )))
            }
        }
    }

    async fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let file_type = FILENAME_ENUM_MAP[file];
        if file_type == LayerFileEnum::Rollup {
            // special case! This is always coming from disk, in its own file
            let p = self.path_for_rollup(directory);

            let mut options = tokio::fs::OpenOptions::new();
            options.read(true);
            let file = options.open(p).await;
            if file.is_err() && file.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
                return Ok(false);
            }
            file?;

            return Ok(true);
        }

        {
            let guard = self.construction.read().unwrap();
            if let Some(map) = guard.get(&directory) {
                return Ok(map.contains_key(&file_type));
            }
        }
        let p = self.path_for_layer(directory);

        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let file = options.open(p).await;
        if file.is_err() && file.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
            return Ok(false);
        }
        let mut file = file?;
        let header = ArchiveFilePresenceHeader::new(file.read_u64().await?);

        Ok(header.is_present(file_type))
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

        let path = self.path_for_layer(directory);
        let mut directory_path = path.clone();
        directory_path.pop();
        fs::create_dir_all(directory_path).await?;
        let mut options = tokio::fs::OpenOptions::new();
        options.create_new(true).write(true);
        let mut file = options.open(path).await?;

        while data_buf.has_remaining() {
            file.write_buf(&mut data_buf).await?;
        }

        file.flush().await?;
        file.sync_all().await
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
