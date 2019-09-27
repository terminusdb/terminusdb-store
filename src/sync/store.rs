use tokio::runtime::{Runtime,TaskExecutor};
use futures::prelude::*;
use futures::sync::oneshot;

use std::io;
use std::path::PathBuf;

use crate::storage::{LabelStore, LayerStore, MemoryLabelStore, MemoryLayerStore, DirectoryLabelStore, DirectoryLayerStore};
use crate::layer::{Layer,GenericLayer,ObjectType,StringTriple,IdTriple};
use crate::store::{Store, Database, DatabaseLayer, DatabaseLayerBuilder, open_memory_store, open_directory_store};

pub struct SyncDatabaseLayerBuilder<Layers:'static+LayerStore> {
    inner: DatabaseLayerBuilder<Layers>,
    executor: TaskExecutor
}

impl<Layers:'static+LayerStore> SyncDatabaseLayerBuilder<Layers> {
    pub fn wrap(inner: DatabaseLayerBuilder<Layers>, executor: TaskExecutor) -> Self {
        SyncDatabaseLayerBuilder {
            inner, executor
        }
    }

    pub fn name(&self) -> [u32;5] {
        self.inner.name()
    }

    pub fn add_string_triple(&mut self, triple: &StringTriple) {
        self.inner.add_string_triple(triple)
    }

    pub fn add_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        self.inner.add_id_triple(triple)
    }

    pub fn remove_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        self.inner.remove_id_triple(triple)
    }

    pub fn remove_string_triple(&mut self, triple: &StringTriple) -> Option<()> {
        self.inner.remove_string_triple(triple)
    }

    pub fn commit(self) -> Result<SyncDatabaseLayer<Layers>,io::Error> {
        let executor = self.executor.clone();
        let inner = oneshot::spawn(self.inner.commit(), &self.executor).wait();

        inner.map(|i|SyncDatabaseLayer::wrap(i, executor))
    }
}

pub struct SyncDatabaseLayer<Layers:'static+LayerStore> {
    inner: DatabaseLayer<Layers>,
    executor: TaskExecutor
}

impl<Layers:'static+LayerStore> SyncDatabaseLayer<Layers> {
    pub fn wrap(inner: DatabaseLayer<Layers>, executor: TaskExecutor) -> Self {
        Self {
            inner, executor
        }
    }

    pub fn open_write(&self) -> Result<SyncDatabaseLayerBuilder<Layers>,io::Error> {
        let inner = oneshot::spawn(self.inner.open_write(), &self.executor).wait();

        inner.map(|i|SyncDatabaseLayerBuilder::wrap(i, self.executor.clone()))
    }
}

impl<Layers:'static+LayerStore> Layer for SyncDatabaseLayer<Layers> {
    type PredicateObjectPairsForSubject = <GenericLayer<Layers::Map> as Layer>::PredicateObjectPairsForSubject;
    type SubjectIterator = <GenericLayer<Layers::Map> as Layer>::SubjectIterator;
    fn name(&self) -> [u32;5] {
        self.inner.name()
    }

    fn node_and_value_count(&self) -> usize {
        self.inner.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.inner.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.inner.subject_id(subject)
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.inner.predicate_id(predicate)
    }

    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.inner.object_node_id(object)
    }

    fn object_value_id(&self, object: &str) -> Option<u64> {
        self.inner.object_value_id(object)
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        self.inner.id_subject(id)
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        self.inner.id_predicate(id)
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        self.inner.id_object(id)
    }

    fn subjects(&self) -> Self::SubjectIterator {
        self.inner.subjects()
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<Self::PredicateObjectPairsForSubject> {
        self.inner.predicate_object_pairs_for_subject(subject)
    }
}

pub struct SyncDatabase<Labels:'static+LabelStore, Layers:'static+LayerStore> {
    inner: Database<Labels, Layers>,
    executor: TaskExecutor
}

impl<Labels:'static+LabelStore, Layers:'static+LayerStore> SyncDatabase<Labels, Layers> {
    pub fn wrap(inner: Database<Labels, Layers>, executor: TaskExecutor) -> Self {
        Self {
            inner, executor
        }
    }

    pub fn head(&self) -> Result<Option<SyncDatabaseLayer<Layers>>,io::Error> {
        let inner = oneshot::spawn(self.inner.head(), &self.executor).wait();

        inner.map(|i|i.map(|i|SyncDatabaseLayer::wrap(i, self.executor.clone())))
    }

    pub fn set_head(&self, layer: &SyncDatabaseLayer<Layers>) -> Result<bool, io::Error> {
        oneshot::spawn(self.inner.set_head(&layer.inner), &self.executor).wait()
    }
}

pub struct SyncStore<Labels:'static+LabelStore, Layers:'static+LayerStore> {
    inner: Store<Labels, Layers>,
    runtime: Runtime
}

impl<Labels:'static+LabelStore, Layers:'static+LayerStore> SyncStore<Labels, Layers> {
    pub fn wrap(inner: Store<Labels, Layers>, runtime: Runtime) -> Self {
        Self {
            inner, runtime
        }
    }

    pub fn create(&self, label: &str) -> Result<SyncDatabase<Labels,Layers>,io::Error> {
        let inner = oneshot::spawn(self.inner.create(label), &self.runtime.executor()).wait();

        inner.map(|i| SyncDatabase::wrap(i, self.runtime.executor()))
    }

    pub fn open(&self, label: &str) -> Result<Option<SyncDatabase<Labels,Layers>>,io::Error> {
        let inner = oneshot::spawn(self.inner.open(label), &self.runtime.executor()).wait();

        inner.map(|i| i.map(|i|SyncDatabase::wrap(i, self.runtime.executor())))
    }

    pub fn create_base_layer(&self) -> Result<SyncDatabaseLayerBuilder<Layers>,io::Error> {
        let inner = oneshot::spawn(self.inner.create_base_layer(), &self.runtime.executor()).wait();

        inner.map(|i| SyncDatabaseLayerBuilder::wrap(i, self.runtime.executor()))
    }
}

pub fn open_sync_memory_store() -> SyncStore<MemoryLabelStore, MemoryLayerStore> {
    SyncStore::wrap(open_memory_store(), Runtime::new().unwrap())
}

pub fn open_sync_directory_store<P:Into<PathBuf>>(path: P) -> SyncStore<DirectoryLabelStore, DirectoryLayerStore> {
    SyncStore::wrap(open_directory_store(path), Runtime::new().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_manipulate_sync_memory_database() {
        let store = open_sync_memory_store();
        let database = store.create("foodb").unwrap();

        let head = database.head().unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().unwrap();
        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));

        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));

        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow","says","moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig","says","oink")));
    }

    #[test]
    fn create_and_manipulate_sync_directory_database() {
        let dir = tempdir().unwrap();
        let store = open_sync_directory_store(dir.path());
        let database = store.create("foodb").unwrap();

        let head = database.head().unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().unwrap();
        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));

        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));

        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow","says","moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig","says","oink")));
    }
}
