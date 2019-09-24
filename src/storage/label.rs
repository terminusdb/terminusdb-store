use tokio::sync::lock::*;
use futures::prelude::*;
use std::collections::HashMap;

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

pub struct MemoryLabelsFuture {
    labels: Lock<HashMap<String, Label>>
}

impl Future for MemoryLabelsFuture {
    type Item = Vec<Label>;
    type Error = std::io::Error;

    fn poll(&mut self) -> Result<Async<Vec<Label>>, std::io::Error> {
        match self.labels.poll_lock() {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(guard) => {
                Ok(Async::Ready(guard.values().map(|l|l.clone()).collect()))
            }
        }
    }
}

pub struct MemoryGetLabelFuture {
    labels: Lock<HashMap<String, Label>>,
    name: String
}

impl Future for MemoryGetLabelFuture {
    type Item = Option<Label>;
    type Error = std::io::Error;

    fn poll(&mut self) -> Result<Async<Option<Label>>, std::io::Error> {
        match self.labels.poll_lock() {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(guard) => Ok(Async::Ready(guard.get(&self.name).map(|l|l.clone())))
        }
    }
}

pub struct MemorySetLabelFuture {
    labels: Lock<HashMap<String, Label>>,
    label: Label
}

impl Future for MemorySetLabelFuture {
    type Item = bool;
    type Error = std::io::Error;

    fn poll(&mut self) -> Result<Async<bool>, std::io::Error> {
        match self.labels.poll_lock() {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(mut guard) => {
                let previous = guard.insert(self.label.name.clone(), self.label.clone());
                if previous.is_some() {
                    let previous = previous.unwrap();
                    if previous.version + 1 != self.label.version {
                        guard.insert(previous.name.clone(), previous);
                        Ok(Async::Ready(false))
                    }
                    else {
                        Ok(Async::Ready(true))
                    }
                }
                else {
                    if self.label.version != 0 {
                        guard.remove(&self.label.name);
                        Ok(Async::Ready(false))
                    }
                    else {
                        Ok(Async::Ready(true))
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct MemoryLabelStore {
    labels: Lock<HashMap<String, Label>>
}

impl MemoryLabelStore {
    pub fn new() -> MemoryLabelStore {
        MemoryLabelStore {
            labels: Lock::new(HashMap::new())
        }
    }
}

impl LabelStore for MemoryLabelStore {
    fn labels(&self) -> Box<dyn Future<Item=Vec<Label>,Error=std::io::Error>+Send+Sync> {
        Box::new(MemoryLabelsFuture { labels: self.labels.clone() })
    }

    fn create_label(&self, name: &str, layer: [u32;5]) -> Box<dyn Future<Item=Label, Error=std::io::Error>+Send+Sync> {
        let labels = self.labels.clone();
        let label = Label::new(name, layer);

        Box::new(MemorySetLabelFuture { labels: labels, label: label.clone() }
                 .and_then(move |b| match b {
                     true => Ok(label),
                     false => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "layer already exists"))
                 }))
    }

    fn get_label(&self, name: &str) -> Box<dyn Future<Item=Option<Label>,Error=std::io::Error>+Send+Sync> {
        Box::new(MemoryGetLabelFuture { labels: self.labels.clone(), name: name.to_owned() })
    }

    fn set_label(&self, label: &Label, layer: [u32;5]) -> Box<dyn Future<Item=Option<Label>, Error=std::io::Error>+Send+Sync> {
        let labels = self.labels.clone();
        let new_label = label.with_updated_layer(layer);

        Box::new(MemorySetLabelFuture { labels: labels, label: new_label.clone() }
                 .map(move |b| match b {
                     true => Some(new_label),
                     false => None
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
