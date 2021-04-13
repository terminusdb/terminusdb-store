//! Directory-based implementation of storage traits.

use bytes::{Bytes, BytesMut};
use futures::{future, Future};
use locking::*;
use std::io::{self, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::{self, *};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use super::*;

const PREFIX_DIR_SIZE: usize = 3;

#[derive(Clone)]
pub struct FileBackedStore {
    path: PathBuf,
}

impl SyncableFile for File {
    fn sync_all(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        Box::pin(async move { File::sync_all(&self).await })
    }
}

impl SyncableFile for BufWriter<File> {
    fn sync_all(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        Box::pin(async move {
            let inner = self.into_inner();

            File::sync_all(&inner).await
        })
    }
}

impl FileBackedStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> FileBackedStore {
        FileBackedStore { path: path.into() }
    }

    fn open_read_from_std(&self, offset: usize) -> std::fs::File {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(&self.path).unwrap();

        file.seek(SeekFrom::Start(offset as u64)).unwrap();

        file
    }
}

impl FileLoad for FileBackedStore {
    type Read = File;

    fn exists(&self) -> bool {
        let metadata = std::fs::metadata(&self.path);
        !(metadata.is_err() && metadata.err().unwrap().kind() == io::ErrorKind::NotFound)
    }

    fn size(&self) -> usize {
        let m = std::fs::metadata(&self.path).unwrap();
        m.len() as usize
    }

    fn open_read_from(&self, offset: usize) -> File {
        let f = self.open_read_from_std(offset);

        File::from_std(f)
    }

    fn map(&self) -> Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>> {
        let file = self.clone();
        Box::pin(async move {
            let size = file.size();
            if size == 0 {
                Ok(Bytes::new())
            } else {
                let mut f = file.open_read();
                let mut b = BytesMut::with_capacity(file.size());

                // unsafe justification: We are immediately
                // overwriting the data in this BytesMut with the file
                // contents, so it doesn't matter that it is
                // uninitialized.
                // Should file reading fail, an error will be
                // returned, and the BytesMut will be freed, ensuring
                // nobody ever looks at the initialized data.
                unsafe { b.set_len(file.size()) };
                f.read_exact(&mut b[..]).await?;
                Ok(b.freeze())
            }
        })
    }
}

impl FileStore for FileBackedStore {
    type Write = BufWriter<File>;

    fn open_write(&self) -> BufWriter<File> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = options.open(&self.path).unwrap();

        BufWriter::new(File::from_std(file))
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

impl PersistentLayerStore for DirectoryLayerStore {
    type File = FileBackedStore;
    fn directories(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let path = self.path.clone();
        Box::pin(async move {
            let mut stream = fs::read_dir(path).await?;
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
        })
    }

    fn create_named_directory(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(name_str);

        Box::pin(async move {
            fs::create_dir_all(p).await?;

            Ok(name)
        })
    }

    fn directory_exists(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let mut p = self.path.clone();
        let name = name_to_string(name);
        p.push(&name[0..PREFIX_DIR_SIZE]);
        p.push(name);

        Box::pin(async move {
            match fs::metadata(p).await {
                Ok(m) => Ok(m.is_dir()),
                Err(_) => Ok(false),
            }
        })
    }

    fn get_file(
        &self,
        directory: [u32; 5],
        name: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::File>> + Send>> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(name);
        Box::pin(future::ok(FileBackedStore::new(p)))
    }

    fn file_exists(
        &self,
        directory: [u32; 5],
        file: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(file);

        Box::pin(async move {
            match fs::metadata(p).await {
                Ok(m) => Ok(m.is_file()),
                Err(_) => Ok(false),
            }
        })
    }
}

#[derive(Clone)]
pub struct DirectoryLabelStore {
    path: PathBuf,
}

impl DirectoryLabelStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> DirectoryLabelStore {
        DirectoryLabelStore { path: path.into() }
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

async fn get_label_from_file<P: Into<PathBuf>>(path: P) -> io::Result<Label> {
    let path: PathBuf = path.into();
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    let mut file = LockedFile::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    get_label_from_data(label, &data)
}

async fn get_label_from_exclusive_locked_file<P: Into<PathBuf>>(
    path: P,
) -> io::Result<(Label, ExclusiveLockedFile)> {
    let path: PathBuf = path.into();
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    let mut file = ExclusiveLockedFile::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    let label = get_label_from_data(label, &data)?;
    file.seek(SeekFrom::Start(0)).await?;

    Ok((label, file))
}

impl LabelStore for DirectoryLabelStore {
    fn labels(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<Label>>> + Send>> {
        let path = self.path.clone();
        Box::pin(async move {
            let mut stream = fs::read_dir(path).await?;
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
                        let label = get_label_from_file(name).await?;
                        result.push(label);
                    }
                }
            }

            Ok(result)
        })
    }

    fn create_label(&self, label: &str) -> Pin<Box<dyn Future<Output = io::Result<Label>> + Send>> {
        let mut p = self.path.clone();
        let label = label.to_owned();
        p.push(format!("{}.label", label));
        let contents = "0\n\n".to_string().into_bytes();
        Box::pin(async move {
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

                        Ok(Label::new_empty(&label))
                    }
                    _ => Err(e),
                },
            }
        })
    }

    fn get_label(
        &self,
        label: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>> {
        let label = label.to_owned();
        let mut p = self.path.clone();
        p.push(format!("{}.label", label));

        Box::pin(async move {
            match get_label_from_file(p).await {
                Ok(label) => Ok(Some(label)),
                Err(e) => match e.kind() {
                    io::ErrorKind::NotFound => Ok(None),
                    _ => Err(e),
                },
            }
        })
    }

    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>> {
        let old_label = label.clone();
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut p = self.path.clone();
        p.push(format!("{}.label", label.name));
        let fut = get_label_from_exclusive_locked_file(p);
        Box::pin(async move {
            let (retrieved_label, mut file) = fut.await?;
            if retrieved_label == old_label {
                // all good, let's a go
                file.truncate().await?;
                file.write_all(&contents).await?;
                file.flush().await?;
                file.sync_all().await?;
                Ok(Some(new_label))
            } else {
                Ok(None)
            }
        })
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

        let mut w = file.open_write();
        let buf = async {
            w.write_all(&[1, 2, 3]).await?;
            w.flush().await?;
            let mut result = Vec::new();
            file.open_read().read_to_end(&mut result).await?;

            Ok::<_, io::Error>(result)
        }
        .await
        .unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[tokio::test]
    async fn write_and_map_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write();
        let map = async {
            w.write_all(&[1, 2, 3]).await?;
            w.flush().await?;

            file.map().await
        }
        .await
        .unwrap();

        assert_eq!(&vec![1, 2, 3][..], &map.as_ref()[..]);
    }

    #[tokio::test]
    async fn write_and_map_large_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write();
        let mut contents = vec![0u8; 4096 << 4];
        for i in 0..contents.capacity() {
            contents[i] = (i as usize % 256) as u8;
        }
        let map = async {
            w.write_all(&contents).await?;
            w.flush().await?;

            file.map().await
        }
        .await
        .unwrap();

        assert_eq!(contents, map.as_ref());
    }

    #[tokio::test]
    async fn create_layers_from_directory_store() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());

        let layer = async {
            let mut builder = store.create_base_layer().await?;
            let base_name = builder.name();

            builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
            builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
            builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

            builder.commit_boxed().await?;

            let mut builder = store.create_child_layer(base_name).await?;
            let child_name = builder.name();

            builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
            builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

            builder.commit_boxed().await?;

            store.get_layer(child_name).await
        }
        .await
        .unwrap()
        .unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn directory_create_and_retrieve_equal_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

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
    async fn directory_update_label_succeeds() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

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
        let store = DirectoryLabelStore::new(dir.path());

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
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").await.unwrap();
        let result = store.create_label("foo").await;

        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(io::ErrorKind::InvalidInput, error.kind());
    }

    #[test]
    fn nonexistent_file_is_nonexistent() {
        let file = FileBackedStore::new("asdfasfopivbuzxcvopiuvpoawehkafpouzvxv");
        assert!(!file.exists());
    }

    #[tokio::test]
    async fn rollup_and_retrieve_base() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store.clone().rollup(unrolled_layer).await.unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(rolled_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(rolled_layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(rolled_layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(
            !rolled_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack"))
        );
    }

    #[tokio::test]
    async fn rollup_and_retrieve_child() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(child_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("cow", "likes", "pig"));
        builder.add_string_triple(StringTriple::new_node("cow", "hates", "pig"));

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

        assert!(rolled_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(rolled_layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(rolled_layer.string_triple_exists(&StringTriple::new_node("cow", "hates", "pig")));
        assert!(!rolled_layer.string_triple_exists(&StringTriple::new_value("cow", "likes", "pig")));
        assert!(
            !rolled_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack"))
        );
    }
}
