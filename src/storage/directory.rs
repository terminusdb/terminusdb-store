//! Directory-based implementation of storage traits.

use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::{future, Future};
use locking::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use tar::{self, Archive};
use tokio::fs::{self, *};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use super::consts::*;
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
                let mut v = Vec::with_capacity(file.size());
                f.read_to_end(&mut v).await?;
                Ok(Bytes::from(v))
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

    fn create_directory(&self) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let name = rand::random();
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

    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        let path = &self.path;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            for id in layer_ids {
                let id_string = name_to_string(id);
                let mut layer_path: PathBuf = path.into();
                let layer_id_prefix_dir = &id_string[0..PREFIX_DIR_SIZE];
                layer_path.push(layer_id_prefix_dir);
                layer_path.push(&id_string);

                let mut tar_path = PathBuf::new();
                tar_path.push(&id_string);
                tar_append_layer(&mut tar, &tar_path, &layer_path).unwrap();
            }
            tar.finish().unwrap();
        }
        // TODO: Proper error handling
        enc.finish().unwrap()
    }
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        let cursor = io::Cursor::new(pack);
        let tar = GzDecoder::new(cursor);
        let mut archive = Archive::new(tar);

        // collect layer ids into a set
        let layer_id_set: HashSet<String> = layer_ids.map(name_to_string).collect();

        // TODO we actually need to validate that these layers, when extracted, will make for a valid store.
        // In terminus-server we are currently already doing this validation. Due to time constraints, we're not implementing it here.
        //
        // This should definitely be done in the future though, to make this part of the library independently usable in a safe manner.
        for e in archive.entries()? {
            let mut entry = e?;
            let path = entry.path()?;

            // check if entry is prefixed with a layer id we are interested in
            let layer_id = path.iter().next().and_then(|p| p.to_str()).unwrap_or("");
            if layer_id_set.contains(layer_id) {
                let mut path: PathBuf = (&self.path).into();
                let prefix = &layer_id[0..PREFIX_DIR_SIZE];
                path.push(prefix);

                // extract!
                entry.unpack_in(path)?;
            }
        }

        Ok(())
    }
}

fn tar_append_file<W: io::Write>(
    tar: &mut tar::Builder<W>,
    destination: &PathBuf,
    origin: &PathBuf,
    file: &str,
) -> io::Result<()> {
    let file_path = origin.join(file);
    tar.append_path_with_name(file_path, destination.join(file))
}

fn tar_append_file_if_exists<W: io::Write>(
    tar: &mut tar::Builder<W>,
    destination: &PathBuf,
    origin: &PathBuf,
    file: &str,
) -> io::Result<()> {
    let file_path = origin.join(file);
    if file_path.exists() {
        tar.append_path_with_name(file_path, destination.join(file))
    } else {
        Ok(())
    }
}

fn tar_append_layer<W: io::Write>(
    tar: &mut tar::Builder<W>,
    tar_path: &PathBuf,
    layer_path: &PathBuf,
) -> io::Result<()> {
    // this appends known layer files, excluding the rollup file.
    tar.append_dir(tar_path, layer_path)?;

    for f in &SHARED_REQUIRED_FILES {
        tar_append_file(tar, tar_path, layer_path, f)?;
    }
    for f in &SHARED_OPTIONAL_FILES {
        if f == &FILENAMES.rollup {
            // skip the rollup file. It will not be resolvable remotely.
            continue;
        }
        tar_append_file_if_exists(tar, tar_path, layer_path, f)?;
    }
    if layer_path.join(FILENAMES.parent).exists() {
        // this is a child layer
        for f in &CHILD_LAYER_REQUIRED_FILES {
            tar_append_file(tar, tar_path, layer_path, f)?;
        }
        for f in &CHILD_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(tar, tar_path, layer_path, f)?;
        }
    } else {
        // this is a base layer
        for f in &BASE_LAYER_REQUIRED_FILES {
            tar_append_file(tar, tar_path, layer_path, f)?;
        }
        for f in &BASE_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(tar, tar_path, layer_path, f)?;
        }
    }

    Ok(())
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

async fn get_label_from_file<P: Into<PathBuf>>(path: P) -> io::Result<Label> {
    let path: PathBuf = path.into();
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    let mut file = LockedFile::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

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
            name: label,
            layer: None,
            version: version.unwrap(),
        })
    } else {
        let layer = layer::string_to_name(layer_str)?;
        Ok(Label {
            name: label,
            layer: Some(layer),
            version: version.unwrap(),
        })
    }
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
        let mut p = self.path.clone();
        p.push(format!("{}.label", label.name));

        let old_label = label.clone();
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let get_label = self.get_label(&label.name);
        Box::pin(async move {
            let retrieved_label = get_label.await?;
            if retrieved_label == Some(old_label) {
                // all good, let's a go
                let mut file = ExclusiveLockedFile::open(p).await?;
                file.write_all(&contents).await?;
                file.flush().await?;
                Ok(Some(new_label))
            } else {
                Ok(None)
            }
        })
    }
}

#[derive(Debug)]
pub enum PackError {
    LayerNotFound,
    Io(io::Error),
    Utf8Error(std::str::Utf8Error),
}

impl Display for PackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{:?}", self)
    }
}

impl From<io::Error> for PackError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
impl From<std::str::Utf8Error> for PackError {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Utf8Error(err)
    }
}

pub fn pack_layer_parents<R: io::Read>(
    readable: R,
) -> Result<HashMap<[u32; 5], Option<[u32; 5]>>, PackError> {
    let tar = GzDecoder::new(readable);
    let mut archive = Archive::new(tar);

    // build a set out of the layer ids for easy retrieval
    let mut result_map = HashMap::new();

    for e in archive.entries()? {
        let mut entry = e?;
        let path = entry.path()?;

        let id = string_to_name(
            path.iter()
                .next()
                .expect("expected path to have at least one component")
                .to_str()
                .expect("expected proper unicode path"),
        )?;

        if path.file_name().expect("expected path to have a filename") == "parent.hex" {
            // this is an element we want to know the parent of
            // lets read it
            let mut parent_id_bytes = [0u8; 40];
            entry.read_exact(&mut parent_id_bytes)?;
            let parent_id_str = std::str::from_utf8(&parent_id_bytes)?;
            let parent_id = string_to_name(parent_id_str)?;

            result_map.insert(id, Some(parent_id));
        } else if !result_map.contains_key(&id) {
            // Ensure that an entry for this layer exists
            // If we encounter the parent file later on, this'll be overwritten with the parent id.
            // If not, it can be assumed to not have a parent.
            result_map.insert(id, None);
        }
    }

    Ok(result_map)
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

    #[tokio::test]
    async fn export_import_layer_with_rollup() {
        let dir1 = tempdir().unwrap();
        let store1 = Arc::new(DirectoryLayerStore::new(dir1.path()));
        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));

        let mut builder = store1.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store1.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store1.get_layer(child_name).await.unwrap().unwrap();

        store1.clone().rollup(unrolled_layer).await.unwrap();

        let export =
            LayerStore::export_layers(&*store1, Box::new(vec![base_name, child_name].into_iter()));

        LayerStore::import_layers(
            &*store2,
            &export,
            Box::new(vec![base_name, child_name].into_iter()),
        )
        .unwrap();

        let imported_layer = store2.get_layer(child_name).await.unwrap().unwrap();
        let triples: Vec<_> = imported_layer
            .triples()
            .map(|t| imported_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                StringTriple::new_node("cow", "likes", "duck"),
                StringTriple::new_node("duck", "likes", "cow")
            ],
            triples
        );
    }
}
