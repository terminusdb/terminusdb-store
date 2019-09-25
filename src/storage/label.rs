use futures::prelude::*;
use std::collections::HashMap;
use futures_locks::RwLock;

#[derive(Clone,PartialEq,Eq,Debug)]
pub struct Label {
    pub name: String,
    pub layer: [u32;5],
    pub version: u64
}

impl Label {
    pub fn new(name: &str, layer: [u32;5]) -> Label {
        Label {
            name: name.to_owned(),
            layer: layer,
            version: 0
        }
    }

    pub fn with_updated_layer(&self, layer: [u32;5]) -> Label {
        Label {
            name: self.name.clone(),
            layer,
            version: self.version+1
        }
    }
}

pub trait LabelStore {
    fn labels(&self) -> Box<dyn Future<Item=Vec<Label>,Error=std::io::Error>+Send+Sync>;
    fn create_label(&self, name: &str, layer: [u32;5]) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send+Sync>;
    fn get_label(&self, name: &str) -> Box<dyn Future<Item=Option<Label>,Error=std::io::Error>+Send+Sync>;
    fn set_label(&self, label: &Label, layer: [u32;5]) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync>;
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

    fn create_label(&self, name: &str, layer: [u32;5]) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send+Sync> {
        let label = Label::new(name, layer);

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

    fn set_label(&self, label: &Label, layer: [u32;5]) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_retrieve_equal_label() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo", [1,2,3,4,5]).wait().unwrap();
        assert_eq!(foo, store.get_label("foo").wait().unwrap().unwrap());
    }

    #[test]
    fn update_label_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo", [1,2,3,4,5]).wait().unwrap();

        assert_eq!(1, store.set_label(&foo, [6,7,8,9,10]).wait().unwrap().unwrap().version);

        assert_eq!(1, store.get_label("foo").wait().unwrap().unwrap().version);
    }

    #[test]
    fn update_label_twice_from_same_label_object_fails() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo", [1,2,3,4,5]).wait().unwrap();

        assert!(store.set_label(&foo, [6,7,8,9,10]).wait().unwrap().is_some());
        assert!(store.set_label(&foo, [1,1,1,1,1]).wait().unwrap().is_none());
    }

    #[test]
    fn update_label_twice_from_updated_label_object_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo", [1,2,3,4,5]).wait().unwrap();

        let foo2 = store.set_label(&foo, [6,7,8,9,10]).wait().unwrap().unwrap();
        assert!(store.set_label(&foo2, [1,1,1,1,1]).wait().unwrap().is_some());
    }
}
