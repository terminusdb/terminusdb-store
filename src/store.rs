use futures::prelude::*;
use futures::future;
use std::sync::Arc;

use crate::storage::label::LabelStore;
use crate::storage::layer::LayerStore;
use crate::layer::{Layer,GenericLayer,SimpleLayerBuilder,ObjectType,StringTriple,IdTriple};

use std::io;

pub struct DatabaseLayerBuilder<Layers:'static+LayerStore> {
    builder: SimpleLayerBuilder<Layers::File>,
    store: Layers
}

impl<Layers:'static+LayerStore> DatabaseLayerBuilder<Layers> {
    fn new(store: Layers) -> impl Future<Item=Self,Error=io::Error>+Send+Sync {
        store.create_base_layer()
            .map(|builder|
                 Self {
                     builder,
                     store 
                 })
    }

    fn wrap(builder: SimpleLayerBuilder<Layers::File>, store: Layers) -> Self {
        DatabaseLayerBuilder {
            builder,
            store
        }
    }

    pub fn name(&self) -> [u32;5] {
        self.builder.name()
    }

    pub fn add_string_triple(&mut self, triple: &StringTriple) {
        self.builder.add_string_triple(triple)
    }

    pub fn add_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        self.builder.add_id_triple(triple)
    }

    pub fn remove_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        self.builder.remove_id_triple(triple)
    }

    pub fn remove_string_triple(&mut self, triple: &StringTriple) -> Option<()> {
        self.builder.remove_string_triple(triple)
    }

    pub fn commit(self) -> impl Future<Item=DatabaseLayer<Layers>, Error=std::io::Error>+Send+Sync {
        let store = self.store.clone();
        let name = self.builder.name();

        self.builder.commit()
            .and_then(move |_| store.get_layer(name)
                      .map(move |layer| DatabaseLayer::wrap(layer.expect("layer that was just created was not found in store"), store)))
    }
}

/// A layer that keeps track of the store it came out of, allowing the creation of a layer builder on top of this layer.
pub struct DatabaseLayer<Layers:'static+LayerStore> {
    layer: Arc<GenericLayer<Layers::Map>>,
    store: Layers
}

impl<Layers:'static+LayerStore> DatabaseLayer<Layers> {
    fn wrap(layer: Arc<GenericLayer<Layers::Map>>, store: Layers) -> Self {
        DatabaseLayer {
            layer, store
        }
    }
    pub fn open_write(&self) -> impl Future<Item=DatabaseLayerBuilder<Layers>,Error=io::Error>+Send+Sync {
        let store = self.store.clone();
        self.store.create_child_layer(self.layer.name())
            .map(move |layer|DatabaseLayerBuilder::wrap(layer, store))
    }

    pub fn is_ancestor_of(&self, other: &DatabaseLayer<Layers>) -> bool {
        self.layer.is_ancestor_of(&other.layer)
    }
}

impl<Layers:'static+LayerStore> Layer for DatabaseLayer<Layers> {
    type PredicateObjectPairsForSubject = <GenericLayer<Layers::Map> as Layer>::PredicateObjectPairsForSubject;
    type SubjectIterator = <GenericLayer<Layers::Map> as Layer>::SubjectIterator;
    fn name(&self) -> [u32;5] {
        self.layer.name()
    }

    fn node_and_value_count(&self) -> usize {
        self.layer.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.layer.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.layer.subject_id(subject)
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.layer.predicate_id(predicate)
    }

    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.layer.object_node_id(object)
    }

    fn object_value_id(&self, object: &str) -> Option<u64> {
        self.layer.object_value_id(object)
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        self.layer.id_subject(id)
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        self.layer.id_predicate(id)
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        self.layer.id_object(id)
    }

    fn subjects(&self) -> Self::SubjectIterator {
        self.layer.subjects()
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<Self::PredicateObjectPairsForSubject> {
        self.layer.predicate_object_pairs_for_subject(subject)
    }
}


pub struct Database<Labels:'static+LabelStore, Layers:'static+LayerStore> {
    label: String,
    label_store: Labels,
    layer_store: Layers,
}

impl<Labels:'static+LabelStore, Layers:'static+LayerStore> Database<Labels, Layers> {
    fn new(label: String, label_store: Labels, layer_store: Layers) -> Self {
        Database {
            label,
            label_store,
            layer_store
        }
    }

    pub fn head(&self) -> impl Future<Item=Option<DatabaseLayer<Layers>>,Error=io::Error>+Send+Sync {
        let layer_store = self.layer_store.clone();
        let label_store = self.label_store.clone();
        label_store.get_label(&self.label)
            .and_then(move |new_label| {
                match new_label {
                    None => Box::new(future::err(io::Error::new(io::ErrorKind::NotFound, "database not found"))),
                    Some(new_label) => {
                        let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> =
                            match new_label.layer {
                                None => Box::new(future::ok(None)),
                                Some(layer) => Box::new(layer_store.get_layer(layer)
                                                        .map(move |layer| layer.map(move |layer|DatabaseLayer::wrap(layer, layer_store))))
                            };
                        result
                    }
                }
            })
    }
    
    pub fn set_head(&self, layer: &DatabaseLayer<Layers>) -> impl Future<Item=bool,Error=io::Error>+Send+Sync {
        let layer_store = self.layer_store.clone();
        let label_store = self.label_store.clone();
        let layer_name = layer.name();
        let cloned_layer = layer.layer.clone();
        label_store.get_label(&self.label)
            .and_then(move |label| {
                let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> = 
                    match label {
                        None => Box::new(future::err(io::Error::new(io::ErrorKind::NotFound, "label not found"))),
                        Some(label) => Box::new({
                            let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> =
                                match label.layer {
                                    None => Box::new(future::ok(true)),
                                    Some(layer_name) => Box::new(layer_store.get_layer(layer_name)
                                                                 .map(move |l|l.map(|l|l.is_ancestor_of(&cloned_layer)).unwrap_or(false)))
                                };

                            result
                        }.and_then(move |b| {
                            let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> =
                                if b {
                                    Box::new(label_store.set_label(&label, layer_name).map(|_|true))
                                } else {
                                    Box::new(future::ok(false))
                                };

                            result
                        }))
                    };
                result
            })

    }
}

pub struct Store<Labels:'static+LabelStore, Layers:'static+LayerStore> {
    label_store: Labels,
    layer_store: Layers,
}

impl<Labels:'static+LabelStore, Layers:'static+LayerStore> Store<Labels, Layers> {
    pub fn new(label_store: Labels, layer_store: Layers) -> Store<Labels, Layers> {
        Store {
            label_store,
            layer_store: layer_store,
        }
    }

    pub fn label_store(&self) -> &Labels {
        &self.label_store
    }

    pub fn layer_store(&self) -> &Layers {
        &self.layer_store
    }

    pub fn create(&self, label: &str) -> impl Future<Item=Database<Labels, Layers>,Error=std::io::Error>+Send+Sync {
        let label_store = self.label_store.clone();
        let layer_store = self.layer_store.clone();
        self.label_store.create_label(label)
            .map(move |label| Database::new(label.name, label_store, layer_store))
    }

    pub fn open(&self, label: &str) -> impl Future<Item=Option<Database<Labels, Layers>>,Error=std::io::Error> {
        let label_store = self.label_store.clone();
        let layer_store = self.layer_store.clone();
        self.label_store.get_label(label)
            .map(move |label| label.map(|label|Database::new(label.name, label_store, layer_store)))
    }

    pub fn create_base_layer(&self) -> impl Future<Item=DatabaseLayerBuilder<Layers>,Error=io::Error>+Send+Sync {
        DatabaseLayerBuilder::new(self.layer_store.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use futures::sync::oneshot;
    use crate::storage::{MemoryLabelStore,MemoryLayerStore, DirectoryLabelStore, DirectoryLayerStore};
    use tempfile::tempdir;

    #[test]
    fn create_and_manipulate_memory_database() {
        let runtime = Runtime::new().unwrap();

        let store = Store::new(MemoryLabelStore::new(), MemoryLayerStore::new());
        let database = oneshot::spawn(store.create("foodb"), &runtime.executor()).wait().unwrap();

        let head = oneshot::spawn(database.head(), &runtime.executor()).wait().unwrap();
        assert!(head.is_none());

        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor()).wait().unwrap();
        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));

        let layer = oneshot::spawn(builder.commit(), &runtime.executor()).wait().unwrap();
        assert!(oneshot::spawn(database.set_head(&layer), &runtime.executor()).wait().unwrap());

        builder = oneshot::spawn(layer.open_write(), &runtime.executor()).wait().unwrap();
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));

        let layer2 = oneshot::spawn(builder.commit(), &runtime.executor()).wait().unwrap();
        assert!(oneshot::spawn(database.set_head(&layer2), &runtime.executor()).wait().unwrap());
        let layer2_name = layer2.name();

        let layer = oneshot::spawn(database.head(), &runtime.executor()).wait().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow","says","moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig","says","oink")));
    }

    #[test]
    fn create_and_manipulate_directory_database() {
        let runtime = Runtime::new().unwrap();
        let dir = tempdir().unwrap();

        let store = Store::new(DirectoryLabelStore::new(dir.path()), DirectoryLayerStore::new(dir.path()));
        let database = oneshot::spawn(store.create("foodb"), &runtime.executor()).wait().unwrap();

        let head = oneshot::spawn(database.head(), &runtime.executor()).wait().unwrap();
        assert!(head.is_none());

        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor()).wait().unwrap();
        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));

        let layer = oneshot::spawn(builder.commit(), &runtime.executor()).wait().unwrap();
        assert!(oneshot::spawn(database.set_head(&layer), &runtime.executor()).wait().unwrap());

        builder = oneshot::spawn(layer.open_write(), &runtime.executor()).wait().unwrap();
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));

        let layer2 = oneshot::spawn(builder.commit(), &runtime.executor()).wait().unwrap();
        assert!(oneshot::spawn(database.set_head(&layer2), &runtime.executor()).wait().unwrap());
        let layer2_name = layer2.name();

        let layer = oneshot::spawn(database.head(), &runtime.executor()).wait().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow","says","moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig","says","oink")));
    }
}
