//! Directory-based implementation of storage traits.

use bytes::{Bytes, BytesMut};
use dyn_clone::{clone_trait_object, DynClone};
use locking::*;
use std::collections::HashMap;
use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, *};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::RwLock;
use urlencoding::{decode, encode};

use async_trait::async_trait;

use super::*;

const PREFIX_DIR_SIZE: usize = 3;

#[derive(Clone, Debug)]
pub struct FileBackedStore {
    path: PathBuf,
}

#[async_trait]
impl SyncableFile for File {
    async fn sync_all(self) -> io::Result<()> {
        File::sync_all(&self).await
    }
}

#[async_trait]
impl SyncableFile for BufWriter<File> {
    async fn sync_all(self) -> io::Result<()> {
        let inner = self.into_inner();

        File::sync_all(&inner).await
    }
}

impl FileBackedStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> FileBackedStore {
        FileBackedStore { path: path.into() }
    }
}

#[async_trait]
impl FileLoad for FileBackedStore {
    type Read = File;

    async fn exists(&self) -> io::Result<bool> {
        let metadata = tokio::fs::metadata(&self.path).await;
        Ok(!(metadata.is_err() && metadata.err().unwrap().kind() == io::ErrorKind::NotFound))
    }

    async fn size(&self) -> io::Result<usize> {
        let m = tokio::fs::metadata(&self.path).await?;
        Ok(m.len() as usize)
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<File> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).await?;

        file.seek(SeekFrom::Start(offset as u64)).await?;

        Ok(file)
    }

    async fn map(&self) -> io::Result<Bytes> {
        let size = self.size().await?;
        if size == 0 {
            Ok(Bytes::new())
        } else {
            let mut f = self.open_read().await?;
            let mut b = BytesMut::with_capacity(size);

            // unsafe justification: We are immediately
            // overwriting the data in this BytesMut with the file
            // contents, so it doesn't matter that it is
            // uninitialized.
            // Should file reading fail, an error will be
            // returned, and the BytesMut will be freed, ensuring
            // nobody ever looks at the initialized data.
            unsafe { b.set_len(size) };
            f.read_exact(&mut b[..]).await?;
            Ok(b.freeze())
        }
    }
}

#[async_trait]
impl FileStore for FileBackedStore {
    type Write = BufWriter<File>;

    async fn open_write(&self) -> io::Result<BufWriter<File>> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = options.open(&self.path).await?;

        Ok(BufWriter::new(file))
    }
}

#[derive(Clone)]
pub struct DirectoryLayerStore {
    path: PathBuf,
}

impl DirectoryLayerStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> DirectoryLayerStore {
        DirectoryLayerStore { path: path.into() }
    }
}

#[async_trait]
impl PersistentLayerStore for DirectoryLayerStore {
    type File = FileBackedStore;
    async fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut stream = fs::read_dir(&self.path).await?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next_entry().await? {
            if direntry.file_type().await?.is_dir() {
                let os_name = direntry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected non-utf8 directory name",
                    )
                })?;
                result.push(string_to_name(name)?);
            }
        }

        Ok(result)
    }

    async fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(name_str);

        fs::create_dir_all(p).await?;

        Ok(name)
    }

    async fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        let mut p = self.path.clone();
        let name = name_to_string(name);
        p.push(&name[0..PREFIX_DIR_SIZE]);
        p.push(name);

        match fs::metadata(p).await {
            Ok(m) => Ok(m.is_dir()),
            Err(_) => Ok(false),
        }
    }

    async fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(name);
        Ok(FileBackedStore::new(p))
    }

    async fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(file);

        match fs::metadata(p).await {
            Ok(m) => Ok(m.is_file()),
            Err(_) => Ok(false),
        }
    }
}

pub trait FilenameEncoding: DynClone + Send + Sync {
    fn encode(&self, str: String) -> String;
    fn decode(&self, str: String) -> String;
}

clone_trait_object!(FilenameEncoding);

#[derive(Clone)]
pub struct NoFilenameEncoding {}

impl FilenameEncoding for NoFilenameEncoding {
    fn encode(&self, str: String) -> String {
        str
    }
    fn decode(&self, str: String) -> String {
        str
    }
}

#[derive(Clone)]
pub struct URLFilenameEncoding {}

impl FilenameEncoding for URLFilenameEncoding {
    fn encode(&self, str: String) -> String {
        return encode(str.as_str()).to_string();
    }
    fn decode(&self, str: String) -> String {
        return decode(str.as_str()).unwrap().to_string();
    }
}

#[derive(Clone)]
pub struct DirectoryLabelStore {
    path: PathBuf,
    filename_encoding: Box<dyn FilenameEncoding>,
}

impl DirectoryLabelStore {
    pub fn new<P: Into<PathBuf>>(
        path: P,
        filename_encoding: impl FilenameEncoding + 'static,
    ) -> DirectoryLabelStore {
        DirectoryLabelStore {
            path: path.into(),
            filename_encoding: Box::new(filename_encoding),
        }
    }
}

fn get_label_from_data(name: String, data: &[u8]) -> io::Result<Label> {
    let s = String::from_utf8_lossy(&data);
    let lines: Vec<&str> = s.lines().collect();
    if lines.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected label file to have two lines. contents were ({:?})",
                lines
            ),
        ));
    }

    let version_str = &lines[0];
    let layer_str = &lines[1];

    let version = u64::from_str_radix(version_str, 10);
    if version.is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected first line of label file to be a number but it was {}",
                version_str
            ),
        ));
    }

    if layer_str.is_empty() {
        Ok(Label {
            name,
            layer: None,
            version: version.unwrap(),
        })
    } else {
        let layer = layer::string_to_name(layer_str)?;
        Ok(Label {
            name,
            layer: Some(layer),
            version: version.unwrap(),
        })
    }
}

async fn get_label_from_file<P: Into<PathBuf>>(path: P, name: String) -> io::Result<Label> {
    let path: PathBuf = path.into();

    let mut file = LockedFile::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    get_label_from_data(name, &data)
}

async fn get_label_from_exclusive_locked_file<P: Into<PathBuf>>(
    path: P,
    name: String,
) -> io::Result<(Label, ExclusiveLockedFile)> {
    let path: PathBuf = path.into();

    let mut file = ExclusiveLockedFile::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    let label = get_label_from_data(name, &data)?;
    file.seek(SeekFrom::Start(0)).await?;

    Ok((label, file))
}

#[async_trait]
impl LabelStore for DirectoryLabelStore {
    async fn labels(&self) -> io::Result<Vec<Label>> {
        let mut stream = fs::read_dir(self.path.clone()).await?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next_entry().await? {
            if direntry.file_type().await?.is_file() {
                let os_name = direntry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected non-utf8 directory name",
                    )
                })?;
                if name.ends_with(".label") {
                    let label = get_label_from_file(
                        direntry.path(),
                        self.filename_encoding.decode(
                            direntry
                                .path()
                                .file_stem()
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned(),
                        ),
                    )
                    .await?;
                    result.push(label);
                }
            }
        }

        Ok(result)
    }

    async fn create_label(&self, label: &str) -> io::Result<Label> {
        let mut p = self.path.clone();
        p.push(self.filename_encoding.encode(format!("{}.label", label)));
        let contents = "0\n\n".to_string().into_bytes();
        match fs::metadata(&p).await {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database already exists",
            )),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    let mut file = ExclusiveLockedFile::create_and_open(p).await?;
                    file.write_all(&contents).await?;
                    file.flush().await?;
                    file.sync_all().await?;

                    Ok(Label::new_empty(label))
                }
                _ => Err(e),
            },
        }
    }

    async fn get_label(&self, label: &str) -> io::Result<Option<Label>> {
        let mut p = self.path.clone();
        p.push(self.filename_encoding.encode(format!("{}.label", label)));

        match get_label_from_file(p, label.to_owned()).await {
            Ok(label) => Ok(Some(label)),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut p = self.path.clone();
        p.push(
            self.filename_encoding
                .encode(format!("{}.label", label.name)),
        );
        let (retrieved_label, mut file) =
            get_label_from_exclusive_locked_file(p, label.name.clone()).await?;
        if retrieved_label == *label {
            // all good, let's a go
            file.truncate().await?;
            file.write_all(&contents).await?;
            file.flush().await?;
            file.sync_all().await?;
            Ok(Some(new_label))
        } else {
            Ok(None)
        }
    }

    async fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut p = self.path.clone();
        p.push(self.filename_encoding.encode(format!("{}.label", name)));

        // We're not locking here to remove the file. The assumption
        // is that any concurrent operation that is done on the label
        // file will not matter. If it is a label read, a concurrent
        // operation will simply get the label contents, which
        // immediately afterwards become invalid. Similarly if it is
        // for a write, the write will appear to be succesful even
        // though the file will be gone afterwards. This is
        // indistinguishable from the case where the read/write and
        // the remove happened in reverse order.
        match tokio::fs::remove_file(p).await {
            Ok(()) => Ok(true),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(false),
                _ => Err(e),
            },
        }
    }
}

/// A version of the directory label store that keeps all labels in
/// memory and doesn't lock.
///
/// This is useful for situations where we can be sure that only one
/// process is working with a set of label files. In that case, we can
/// keep all label files cached in memory in order to process reads as
/// quickly as possible, and we can perform writes without any sort of
/// file system locking.
pub struct CachedDirectoryLabelStore {
    path: PathBuf,
    labels: Arc<RwLock<HashMap<String, Label>>>,
    filename_encoding: Box<dyn FilenameEncoding>,
}

impl CachedDirectoryLabelStore {
    /// Open a new label store.
    ///
    /// This will read in all label files on startup, which is why
    /// this is an async operation.
    pub async fn open<P: Into<PathBuf>>(
        path: P,
        filename_encoding: impl FilenameEncoding + 'static,
    ) -> io::Result<Self> {
        let path: PathBuf = path.into();
        let mut labels = HashMap::new();
        let mut entries = tokio::fs::read_dir(&path).await?;

        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }
            if let Ok(file_name) = entry.file_name().into_string() {
                if !file_name.ends_with(".label") {
                    continue;
                }

                let label_name = file_name[..file_name.len() - 6].to_string();
                let label =
                    get_label_from_file(entry.path(), filename_encoding.decode(label_name.clone()))
                        .await?;

                labels.insert(label_name, label);
            }
        }

        Ok(Self {
            path,
            labels: Arc::new(RwLock::new(labels)),
            filename_encoding: Box::new(filename_encoding),
        })
    }
}

#[async_trait]
impl LabelStore for CachedDirectoryLabelStore {
    async fn labels(&self) -> io::Result<Vec<Label>> {
        let labels = self.labels.read().await;
        Ok(labels.values().cloned().collect())
    }

    async fn create_label(&self, label: &str) -> io::Result<Label> {
        let mut labels = self.labels.write().await;
        if labels.contains_key(label) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database already exists",
            ));
        }

        let mut p = self.path.clone();
        p.push(format!("{}.label", label));
        let contents = b"0\n\n";
        match fs::metadata(&p).await {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::Other,
                "label was not in cached map but was found on disk",
            )),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    let mut options = fs::OpenOptions::new();
                    options.create_new(true);
                    options.write(true);
                    let mut file = options.open(p).await?;
                    file.write_all(contents).await?;
                    file.flush().await?;
                    file.sync_all().await?;

                    let l = Label::new_empty(label);
                    labels.insert(label.to_string(), l.clone());

                    Ok(l)
                }
                _ => Err(e),
            },
        }
    }
    async fn get_label(&self, label: &str) -> io::Result<Option<Label>> {
        let labels = self.labels.read().await;
        Ok(labels.get(label).cloned())
    }
    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut labels = self.labels.write().await;
        if let Some(retrieved_label) = labels.get(&label.name) {
            if retrieved_label == label {
                // all good, let's a go
                let mut p = self.path.clone();
                p.push(
                    self.filename_encoding
                        .encode(format!("{}.label", label.name)),
                );
                let mut options = fs::OpenOptions::new();
                options.create(false);
                options.write(true);
                let mut file = options.open(p).await?;
                file.write_all(&contents).await?;
                file.flush().await?;
                file.sync_data().await?;

                labels.insert(label.name.clone(), new_label.clone());
                Ok(Some(new_label))
            } else {
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "label not found"))
        }
    }

    async fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut labels = self.labels.write().await;
        if labels.remove(name).is_some() {
            let mut p = self.path.clone();
            p.push(self.filename_encoding.encode(format!("{}.label", name)));
            tokio::fs::remove_file(p).await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn write_and_read_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().await.unwrap();
        w.write_all(&[1, 2, 3]).await.unwrap();
        w.flush().await.unwrap();
        let mut buf = Vec::new();
        file.open_read()
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[tokio::test]
    async fn write_and_map_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().await.unwrap();
        w.write_all(&[1, 2, 3]).await.unwrap();
        w.flush().await.unwrap();

        let map = file.map().await.unwrap();

        assert_eq!(&vec![1, 2, 3][..], &map.as_ref()[..]);
    }

    #[tokio::test]
    async fn write_and_map_large_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().await.unwrap();
        let mut contents = vec![0u8; 4096 << 4];
        for i in 0..contents.capacity() {
            contents[i] = (i as usize % 256) as u8;
        }

        w.write_all(&contents).await.unwrap();
        w.flush().await.unwrap();

        let map = file.map().await.unwrap();

        assert_eq!(contents, map.as_ref());
    }

    #[tokio::test]
    async fn create_layers_from_directory_store() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());

        let layer = async {
            let mut builder = store.create_base_layer().await?;
            let base_name = builder.name();

            builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
            builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
            builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

            builder.commit_boxed().await?;

            let mut builder = store.create_child_layer(base_name).await?;
            let child_name = builder.name();

            builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
            builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

            builder.commit_boxed().await?;

            store.get_layer(child_name).await
        }
        .await
        .unwrap()
        .unwrap();

        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn directory_create_and_retrieve_equal_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        let (stored, retrieved) = async {
            let stored = store.create_label("foo").await?;
            let retrieved = store.get_label("foo").await?;

            Ok::<_, io::Error>((stored, retrieved))
        }
        .await
        .unwrap();

        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[tokio::test]
    async fn directory_create_and_retrieve_equal_url_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), URLFilenameEncoding {});

        let (stored, retrieved) = async {
            let stored = store
                .create_label("https://terminusdb.com/?s=does+work")
                .await?;
            let retrieved = store
                .get_label("https://terminusdb.com/?s=does+work")
                .await?;

            Ok::<_, io::Error>((stored, retrieved))
        }
        .await
        .unwrap();

        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[tokio::test]
    async fn directory_update_label_succeeds() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        let retrieved = async {
            let stored = store.create_label("foo").await?;
            store.set_label(&stored, [6, 7, 8, 9, 10]).await?;

            store.get_label("foo").await
        }
        .await
        .unwrap()
        .unwrap();

        assert_eq!(Some([6, 7, 8, 9, 10]), retrieved.layer);
    }

    #[tokio::test]
    async fn directory_update_label_twice_from_same_label_object_fails() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        let (stored2, stored3) = async {
            let stored1 = store.create_label("foo").await?;

            let stored2 = store.set_label(&stored1, [6, 7, 8, 9, 10]).await?;
            let stored3 = store.set_label(&stored1, [10, 9, 8, 7, 6]).await?;

            Ok::<_, io::Error>((stored2, stored3))
        }
        .await
        .unwrap();

        assert!(stored2.is_some());
        assert!(stored3.is_none());
    }

    #[tokio::test]
    async fn directory_create_label_twice_errors() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        store.create_label("foo").await.unwrap();
        let result = store.create_label("foo").await;

        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(io::ErrorKind::InvalidInput, error.kind());
    }

    #[tokio::test]
    async fn nonexistent_file_is_nonexistent() {
        let file = FileBackedStore::new("asdfasfopivbuzxcvopiuvpoawehkafpouzvxv");
        assert!(!file.exists().await.unwrap());
    }

    #[tokio::test]
    async fn rollup_and_retrieve_base() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store.clone().rollup(unrolled_layer).await.unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn rollup_and_retrieve_child() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(child_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("cow", "likes", "pig"));
        builder.add_value_triple(ValueTriple::new_node("cow", "hates", "pig"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store
            .clone()
            .rollup_upto(unrolled_layer, base_name)
            .await
            .unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "hates", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn create_and_delete_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        store.create_label("foo").await.unwrap();
        assert!(store.get_label("foo").await.unwrap().is_some());
        assert!(store.delete_label("foo").await.unwrap());
        assert!(store.get_label("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        assert!(!store.delete_label("foo").await.unwrap());
    }

    #[tokio::test]
    async fn delete_shared_locked_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        store.create_label("foo").await.unwrap();
        let label_path = dir.path().join("foo.label");
        let _f = LockedFile::open(label_path).await.unwrap();

        assert!(store.delete_label("foo").await.unwrap());
    }

    #[tokio::test]
    async fn delete_exclusive_locked_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path(), NoFilenameEncoding {});

        store.create_label("foo").await.unwrap();
        let label_path = dir.path().join("foo.label");
        let _f = ExclusiveLockedFile::open(label_path).await.unwrap();

        assert!(store.delete_label("foo").await.unwrap());
    }
}
