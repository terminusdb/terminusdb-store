use futures::prelude::*;
use std::collections::HashMap;
use futures_locks::RwLock;
use std::path::PathBuf;
use tokio::fs;
use futures::future;
use std::io;
use super::layer;

#[derive(Clone,PartialEq,Eq,Debug)]
pub struct Label {
    pub name: String,
    pub layer: Option<[u32;5]>,
    pub version: u64
}

impl Label {
    pub fn new_empty(name: &str) -> Label {
        Label {
            name: name.to_owned(),
            layer: None,
            version: 0
        }
    }
    pub fn new(name: &str, layer: [u32;5]) -> Label {
        Label {
            name: name.to_owned(),
            layer: Some(layer),
            version: 0
        }
    }

    pub fn with_updated_layer(&self, layer: Option<[u32;5]>) -> Label {
        Label {
            name: self.name.clone(),
            layer,
            version: self.version+1
        }
    }
}

pub trait LabelStore {
    fn labels(&self) -> Box<dyn Future<Item=Vec<Label>,Error=std::io::Error>+Send+Sync>;
    fn create_label(&self, name: &str) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send+Sync>;
    fn get_label(&self, name: &str) -> Box<dyn Future<Item=Option<Label>,Error=std::io::Error>+Send+Sync>;
    fn set_label_option(&self, label: &Label, layer: Option<[u32;5]>) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync>;

    fn set_label(&self, label: &Label, layer: [u32;5]) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync> {
        self.set_label_option(label, Some(layer))
    }

    fn clear_label(&self, label: &Label) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync> {
        self.set_label_option(label, None)
    }
}

#[derive(Clone)]
pub struct MemoryLabelStore {
    labels: RwLock<HashMap<String, Label>>
}

impl MemoryLabelStore {
    pub fn new() -> MemoryLabelStore {
        MemoryLabelStore {
            labels: RwLock::new(HashMap::new())
        }
    }
}

impl LabelStore for MemoryLabelStore {
    fn labels(&self) -> Box<dyn Future<Item=Vec<Label>,Error=std::io::Error>+Send+Sync> {
        Box::new(self.labels.read()
                 .then(|l| Ok(l.expect("rwlock read should always succeed")
                              .values().map(|v|v.clone()).collect())))
    }

    fn create_label(&self, name: &str) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send+Sync> {
        let label = Label::new_empty(name);

        Box::new(self.labels.write()
                 .then(move |l| {
                     let mut labels = l.expect("rwlock write should always succeed");
                     if labels.get(&label.name).is_some() {
                         Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "label already exists"))
                     }
                     else {
                         labels.insert(label.name.clone(), label.clone());
                         Ok(label)
                     }
                 }))
    }

    fn get_label(&self, name: &str) -> Box<dyn Future<Item=Option<Label>,Error=std::io::Error>+Send+Sync> {
        let name = name.to_owned();
        Box::new(self.labels.read()
                 .then(move |l| Ok(l.expect("rwlock read should always succeed")
                                   .get(&name).map(|label|label.clone()))))
    }

    fn set_label_option(&self, label: &Label, layer: Option<[u32;5]>) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync> {
        let new_label = label.with_updated_layer(layer);

        Box::new(self.labels.write()
                 .then(move |l| {
                     let mut labels = l.expect("rwlock write should always succeed");

                     match labels.get(&new_label.name) {
                         None => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "label does not exist")),
                         Some(old_label) => {
                             if old_label.version+1 != new_label.version {
                                 Ok(None)
                             }
                             else {
                                 labels.insert(new_label.name.clone(), new_label.clone());

                                 Ok(Some(new_label))
                             }
                         }
                     }
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

fn get_label_from_file(path: PathBuf) -> impl Future<Item=Label,Error=std::io::Error>+Send+Sync {
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    fs::read(path)
        .and_then(move |data| {
            let s = String::from_utf8_lossy(&data);
            let lines: Vec<&str> = s.lines().collect();
            if lines.len() != 2 {
                let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> = 
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
    fn labels(&self) -> Box<dyn Future<Item=Vec<Label>,Error=std::io::Error>+Send+Sync> {
        Box::new(fs::read_dir(self.path.clone()).flatten_stream()
                 .map(|direntry| (direntry.file_name(), direntry))
                 .and_then(|(dir_name, direntry)| future::poll_fn(move || direntry.poll_file_type())
                           .map(move |ft| (dir_name, ft.is_file())))
                 .filter(|(file_name, is_file)|file_name.to_str().unwrap().ends_with(".label") && *is_file)
                 .and_then(|(file_name, _)| get_label_from_file(file_name.into()))
                 .collect())
    }

    fn create_label(&self, label: &str) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send+Sync> {
        let mut p = self.path.clone();
        let label = label.to_owned();
        p.push(format!("{}.label", label));
        let contents = format!("0\n\n").into_bytes();
        Box::new(fs::write(p, contents)
                 .map(move |_| Label::new_empty(&label)))
    }

    fn get_label(&self, label: &str) -> Box<dyn Future<Item=Option<Label>,Error=std::io::Error>+Send+Sync> {
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

    fn set_label_option(&self, label: &Label, layer: Option<[u32;5]>) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", label.name));

        let old_label = label.clone();
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", label.version).into_bytes(),
            Some(layer) => format!("{}\n{}\n", label.version, layer::name_to_string(layer)).into_bytes()
        };

        Box::new(self.get_label(&label.name)
                 .and_then(move |l| if l == Some(old_label) {
                     // all good, let's a go
                     let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> = Box::new(
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
    use tempfile::tempdir;
    use futures::sync::oneshot::channel;

    #[test]
    fn memory_create_and_retrieve_equal_label() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();
        assert_eq!(foo, store.get_label("foo").wait().unwrap().unwrap());
    }

    #[test]
    fn memory_update_label_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();

        assert_eq!(1, store.set_label(&foo, [6,7,8,9,10]).wait().unwrap().unwrap().version);

        assert_eq!(1, store.get_label("foo").wait().unwrap().unwrap().version);
    }

    #[test]
    fn memory_update_label_twice_from_same_label_object_fails() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();

        assert!(store.set_label(&foo, [6,7,8,9,10]).wait().unwrap().is_some());
        assert!(store.set_label(&foo, [1,1,1,1,1]).wait().unwrap().is_none());
    }

    #[test]
    fn memory_update_label_twice_from_updated_label_object_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();

        let foo2 = store.set_label(&foo, [6,7,8,9,10]).wait().unwrap().unwrap();
        assert!(store.set_label(&foo2, [1,1,1,1,1]).wait().unwrap().is_some());
    }

    #[test]
    fn directory_create_and_retrieve_equal_label() {
        let (tx,rx) = channel::<Result<(Label,Option<Label>), std::io::Error>>();
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let task = store.create_label("foo")
            .and_then(move |stored| store.get_label("foo")
                      .map(|retrieved| (stored, retrieved)))
            .then(|result| tx.send(result))
            .map(|_|())
            .map_err(|_|());

        tokio::run(task);
        let (stored,retrieved) = rx.wait().unwrap().unwrap();
        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[test]
    fn directory_update_label_succeeds() {
        let (tx,rx) = channel::<Result<Option<Label>, std::io::Error>>();
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let task = store.create_label("foo")
            .and_then(move |stored| store.set_label(&stored, [6,7,8,9,10])
                      .and_then(move |_| store.get_label("foo")))
            .then(|result| tx.send(result))
            .map(|_|())
            .map_err(|_|());

        tokio::run(task);
        let retrieved = rx.wait().unwrap().unwrap().unwrap();
        assert_eq!(Some([6,7,8,9,10]),retrieved.layer);
    }

    #[test]
    fn directory_update_label_twice_from_same_label_object_fails() {
        let (tx,rx) = channel::<Result<(Option<Label>,Option<Label>), std::io::Error>>();
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let task = store.create_label("foo")
            .and_then(move |stored1| store.set_label(&stored1, [6,7,8,9,10])
                      .and_then(move |stored2| store.set_label(&stored1, [10,9,8,7,6])
                                .map(|stored3| (stored2, stored3))))
            .then(|result| tx.send(result))
            .map(|_|())
            .map_err(|_|());

        tokio::run(task);
        let (stored2, stored3) = rx.wait().unwrap().unwrap();

        assert!(stored2.is_some());
        assert!(stored3.is_none());
    }
}
