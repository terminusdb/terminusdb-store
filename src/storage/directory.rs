//! Directory-based implementation of storage traits.
use futures::prelude::*;
use tokio::prelude::*;
use tokio::fs::{self,*};
use std::io::{self,Seek, SeekFrom};
use std::sync::Arc;
use std::path::PathBuf;
use memmap::*;

use super::*;

#[derive(Clone)]
pub struct FileBackedStore {
    path: PathBuf
}

impl FileBackedStore {
    pub fn new<P:Into<PathBuf>>(path: P) -> FileBackedStore {
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

#[derive(Clone)]
pub struct SharedMmap(Option<Arc<Mmap>>);

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            None => &[],
            Some(map) => &*map
        }
    }
}


impl FileLoad for FileBackedStore {
    type Read = File;
    type Map = SharedMmap;

    fn size(&self) -> usize {
        let m = std::fs::metadata(&self.path).unwrap();
        m.len() as usize
    }

    fn open_read_from(&self, offset: usize) -> File {
        let f = self.open_read_from_std(offset);

        File::from_std(f)
    }

    fn map(&self) -> Box<dyn Future<Item=SharedMmap,Error=std::io::Error>+Send> {
        let file = self.clone();
        Box::new(future::lazy(move || {
            if file.size() == 0 {
                future::ok(SharedMmap(None))
            }
            else {
                let f = file.open_read_from_std(0);
                // unsafe justification: we opened this file specifically to do memory mapping, and will do nothing else with it.
                future::ok(SharedMmap(Some(Arc::new(unsafe { Mmap::map(&f) }.unwrap()))))
            }
        }))
    }
}

impl FileStore for FileBackedStore {
    type Write = File;

    fn open_write_from(&self, offset: usize) -> File {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        let mut file = options.open(&self.path).unwrap();

        file.seek(SeekFrom::Start(offset as u64)).unwrap();

        File::from_std(file)
    }
}

#[derive(Clone)]
pub struct DirectoryLayerStore {
    path: PathBuf
}

impl DirectoryLayerStore {
    pub fn new<P:Into<PathBuf>>(path: P) -> DirectoryLayerStore {
        DirectoryLayerStore {
            path: path.into()
        }
    }
}

impl PersistentLayerStore for DirectoryLayerStore {
    type File = FileBackedStore;
    fn directories(&self) -> Box<dyn Future<Item=Vec<[u32; 5]>, Error=std::io::Error>+Send> {
        Box::new(fs::read_dir(self.path.clone()).flatten_stream()
                 .map(|direntry| (direntry.file_name(), direntry))
                 .and_then(|(dir_name, direntry)| future::poll_fn(move || direntry.poll_file_type())
                           .map(move |ft| (dir_name, ft.is_dir())))
                 .filter_map(|(dir_name, is_dir)| match is_dir {
                     true => Some(dir_name),
                     false => None
                 })
                 .and_then(|dir_name| dir_name.to_str().ok_or(io::Error::new(io::ErrorKind::InvalidData, "unexpected non-utf8 directory name")).map(|s|s.to_owned()))
                 .and_then(|s| string_to_name(&s))
                 .collect())
    }

    fn create_directory(&self) -> Box<dyn Future<Item=[u32;5], Error=io::Error>+Send> {
        let name = rand::random();
        let mut p = self.path.clone();
        p.push(name_to_string(name));

        Box::new(fs::create_dir(p)
                 .map(move |_| name))
    }

    fn directory_exists(&self, name: [u32; 5]) -> Box<dyn Future<Item=bool,Error=io::Error>+Send> {
        let mut p = self.path.clone();
        p.push(name_to_string(name));

        Box::new(fs::metadata(p)
                 .then(|result| match result {
                     Ok(f) => Ok(f.is_dir()),
                     Err(_) => Ok(false)
                 }))
    }

    fn get_file(&self, directory: [u32;5], name: &str) -> Box<dyn Future<Item=Self::File, Error=io::Error>+Send> {
        let mut p = self.path.clone();
        p.push(name_to_string(directory));
        p.push(name);
        Box::new(future::ok(FileBackedStore::new(p)))
    }
    
    fn file_exists(&self, directory: [u32;5], file: &str) -> Box<dyn Future<Item=bool,Error=io::Error>+Send> {
        let mut p = self.path.clone();
        p.push(name_to_string(directory));
        p.push(file);
        Box::new(fs::metadata(p)
                 .then(|result| match result {
                     Ok(f) => Ok(f.is_file()),
                     Err(_) => Ok(false)
                 }))
    }
}

#[derive(Clone)]
pub struct DirectoryLabelStore {
    path: PathBuf
}

impl DirectoryLabelStore {
    pub fn new<P:Into<PathBuf>>(path: P) -> DirectoryLabelStore {
        DirectoryLabelStore {
            path: path.into()
        }
    }
}

fn get_label_from_file(path: PathBuf) -> impl Future<Item=Label,Error=std::io::Error>+Send {
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    fs::read(path)
        .and_then(move |data| {
            let s = String::from_utf8_lossy(&data);
            let lines: Vec<&str> = s.lines().collect();
            if lines.len() != 2 {
                let result: Box<dyn Future<Item=_,Error=_>+Send> = 
                    Box::new(future::err(io::Error::new(io::ErrorKind::InvalidData, format!("expected label file to have two lines. contents were ({:?})",lines))));
                return result;
            }
            let version_str = &lines[0];
            let layer_str = &lines[1];

            let version = u64::from_str_radix(version_str,10);
            if version.is_err() {
                return Box::new(future::err(io::Error::new(io::ErrorKind::InvalidData, format!("expected first line of label file to be a number but it was {}", version_str))));
            }

            if layer_str.len() == 0 {
                Box::new(future::ok(Label {
                    name: label,
                    layer: None,
                    version: version.unwrap()
                }))
            }
            else {
                let layer = layer::string_to_name(layer_str);
                Box::new(layer.into_future()
                         .map(|layer| Label {
                             name: label,
                             layer: Some(layer),
                             version: version.unwrap()
                         }))
            }

        })
}

impl LabelStore for DirectoryLabelStore {
    fn labels(&self) -> Box<dyn Future<Item=Vec<Label>,Error=std::io::Error>+Send> {
        Box::new(fs::read_dir(self.path.clone()).flatten_stream()
                 .map(|direntry| (direntry.file_name(), direntry))
                 .and_then(|(dir_name, direntry)| future::poll_fn(move || direntry.poll_file_type())
                           .map(move |ft| (dir_name, ft.is_file())))
                 .filter(|(file_name, is_file)|file_name.to_str().unwrap().ends_with(".label") && *is_file)
                 .and_then(|(file_name, _)| get_label_from_file(file_name.into()))
                 .collect())
    }

    fn create_label(&self, label: &str) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send> {
        let mut p = self.path.clone();
        let label = label.to_owned();
        p.push(format!("{}.label", label));
        let contents = format!("0\n\n").into_bytes();
        Box::new(fs::metadata(p.clone())
                 .then(move |metadata| match metadata {
                     Ok(_) => future::err(io::Error::new(io::ErrorKind::InvalidInput, "database already exists")),
                     Err(e) => match e.kind() {
                         io::ErrorKind::NotFound => future::ok(p),
                         _ => future::err(e)
                     }
                 })
                 .and_then(|p| fs::write(p, contents)
                           .map(move |_| Label::new_empty(&label))))
    }

    fn get_label(&self, label: &str) -> Box<dyn Future<Item=Option<Label>,Error=std::io::Error>+Send> {
        let label = label.to_owned();
        let mut p = self.path.clone();
        p.push(format!("{}.label", label));

        Box::new(get_label_from_file(p)
                 .map(|label| Some(label))
                 .or_else(move |e| {
                     if e.kind() == io::ErrorKind::NotFound {
                         Ok(None)
                     }
                     else {
                         Err(e)
                     }
                 }))
    }

    fn set_label_option(&self, label: &Label, layer: Option<[u32;5]>) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", label.name));

        let old_label = label.clone();
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
        };

        Box::new(self.get_label(&label.name)
                 .and_then(move |l| if l == Some(old_label) {
                     // all good, let's a go
                     let result: Box<dyn Future<Item=_,Error=_>+Send> = Box::new(
                         fs::write(p, contents)
                             .map(|_| Some(new_label)));
                     result
                 } else {
                     Box::new(future::ok(None))
                 }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use tempfile::tempdir;
    use futures::sync::oneshot;
    use tokio::runtime::Runtime;

    #[test]
    fn write_and_read_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);
        let runtime = Runtime::new().unwrap();

        let w = file.open_write();
        let task = tokio::io::write_all(w,[1,2,3])
            .and_then(move |_| tokio::io::read_to_end(file.open_read(), Vec::new()))
            .map(move |(_,buf)| buf);


        let buf = oneshot::spawn(task, &runtime.executor()).wait().unwrap();
        runtime.shutdown_now();

        assert_eq!(vec![1,2,3], buf);
    }

    #[test]
    fn write_and_map_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);
        let runtime = Runtime::new().unwrap();

        let w = file.open_write();
        let task = tokio::io::write_all(w,[1,2,3])
            .and_then(move |_| file.map());

        let map = oneshot::spawn(task, &runtime.executor()).wait().unwrap();
        runtime.shutdown_now();

        assert_eq!(&vec![1,2,3][..], &map.as_ref()[..]);
    }

    #[test]
    fn create_layers_from_directory_store() {
        let runtime = Runtime::new().unwrap();
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        let task = store.create_base_layer()
            .and_then(|mut builder| {
                let base_name = builder.name();

                builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
                builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
                builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

                builder.commit_boxed()
                    .map(move |_| base_name)
            })
            .and_then(move |base_name| store.create_child_layer(base_name)
                      .and_then(|mut builder| {
                          let child_name = builder.name();

                          builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
                          builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

                          builder.commit_boxed()
                              .map(move |_| child_name)
                      })
                      .and_then(move |child_name| store.get_layer(child_name)));

        let layer = oneshot::spawn(task, &runtime.executor()).wait().unwrap().unwrap();
        runtime.shutdown_now();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[test]
    fn directory_create_and_retrieve_equal_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());
        let runtime = Runtime::new().unwrap();

        let task = store.create_label("foo")
            .and_then(move |stored| store.get_label("foo")
                      .map(|retrieved| (stored, retrieved)));

        let (stored,retrieved) = oneshot::spawn(task, &runtime.executor()).wait().unwrap();
        runtime.shutdown_now();

        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[test]
    fn directory_update_label_succeeds() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());
        let runtime = Runtime::new().unwrap();

        let task = store.create_label("foo")
            .and_then(move |stored| store.set_label(&stored, [6,7,8,9,10])
                      .and_then(move |_| store.get_label("foo")));

        let retrieved = oneshot::spawn(task, &runtime.executor()).wait().unwrap().unwrap();
        runtime.shutdown_now();
        assert_eq!(Some([6,7,8,9,10]),retrieved.layer);
    }

    #[test]
    fn directory_update_label_twice_from_same_label_object_fails() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());
        let runtime = Runtime::new().unwrap();

        let task = store.create_label("foo")
            .and_then(move |stored1| store.set_label(&stored1, [6,7,8,9,10])
                      .and_then(move |stored2| store.set_label(&stored1, [10,9,8,7,6])
                                .map(|stored3| (stored2, stored3))));

        let (stored2, stored3) = oneshot::spawn(task, &runtime.executor()).wait().unwrap();
        runtime.shutdown_now();

        assert!(stored2.is_some());
        assert!(stored3.is_none());
    }

    #[test]
    fn directory_create_label_twice_errors() {
        let runtime = Runtime::new().unwrap();
        let executor = runtime.executor();

        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        oneshot::spawn(store.create_label("foo"), &executor).wait().unwrap();
        let result = oneshot::spawn(store.create_label("foo"), &executor).wait();
        runtime.shutdown_now();

        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(io::ErrorKind::InvalidInput, error.kind());
    }
}
