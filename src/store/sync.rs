//! a synchronous version of the store API
//!
//! Since not everyone likes tokio, or dealing with async code, this
//! module exposes the same API as the asynchronous store API, only
//! without any futures.
use tokio::runtime::{Runtime,TaskExecutor};
use futures::prelude::*;
use futures::sync::oneshot;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::layer::{Layer,ObjectType,StringTriple,IdTriple,SubjectLookup,ObjectLookup};
use crate::store::{Store, Database, DatabaseLayer, DatabaseLayerBuilder, open_memory_store, open_directory_store};

/// A wrapper over a SimpleLayerBuilder, providing a thread-safe sharable interface
///
/// The SimpleLayerBuilder requires one to have a mutable reference to
/// it, and on commit it will be consumed. This builder only requires
/// an immutable reference, and uses a futures-aware read-write lock
/// to synchronize access to it between threads. Also, rather than
/// consuming itself on commit, this wrapper will simply mark itself
/// as having committed, returning errors on further calls.
pub struct SyncDatabaseLayerBuilder {
    inner: DatabaseLayerBuilder,
    executor: TaskExecutor
}

impl SyncDatabaseLayerBuilder {
    fn wrap(inner: DatabaseLayerBuilder, executor: TaskExecutor) -> Self {
        SyncDatabaseLayerBuilder {
            inner, executor
        }
    }

    /// Returns the name of the layer being built
    pub fn name(&self) -> [u32;5] {
        self.inner.name()
    }

    /// Add a string triple
    pub fn add_string_triple(&self, triple: &StringTriple) -> Result<(), io::Error> {
        oneshot::spawn(self.inner.add_string_triple(triple), &self.executor).wait()
    }

    /// Add an id triple
    pub fn add_id_triple(&self, triple: IdTriple) -> Result<bool, io::Error> {
        oneshot::spawn(self.inner.add_id_triple(triple), &self.executor).wait()
    }

    /// Remove a string triple
    pub fn remove_string_triple(&self, triple: &StringTriple) -> Result<bool, io::Error> {
        oneshot::spawn(self.inner.remove_string_triple(triple), &self.executor).wait()
    }

    /// Remove an id triple
    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<bool, io::Error> {
        oneshot::spawn(self.inner.remove_id_triple(triple), &self.executor).wait()
    }

    /// Commit the layer to storage
    pub fn commit(&self) -> Result<SyncDatabaseLayer,io::Error> {
        let executor = self.executor.clone();
        let inner = oneshot::spawn(self.inner.commit(), &self.executor).wait();

        inner.map(|i|SyncDatabaseLayer::wrap(i, executor))
    }
}

/// A layer that keeps track of the store it came out of, allowing the creation of a layer builder on top of this layer
pub struct SyncDatabaseLayer {
    inner: DatabaseLayer,
    executor: TaskExecutor
}

impl SyncDatabaseLayer {
    fn wrap(inner: DatabaseLayer, executor: TaskExecutor) -> Self {
        Self {
            inner, executor
        }
    }

    /// Create a layer builder based on this layer
    pub fn open_write(&self) -> Result<SyncDatabaseLayerBuilder,io::Error> {
        let inner = oneshot::spawn(self.inner.open_write(), &self.executor).wait();

        inner.map(|i|SyncDatabaseLayerBuilder::wrap(i, self.executor.clone()))
    }
}

impl Layer for SyncDatabaseLayer {
    fn name(&self) -> [u32;5] {
        self.inner.name()
    }

    fn parent(&self) -> Option<Arc<dyn Layer>> {
        self.inner.parent()
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

    fn subjects(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        self.inner.subjects()
    }

    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        self.inner.lookup_subject(subject)
    }

    fn objects(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        self.inner.objects()
    }

    fn lookup_object(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.inner.lookup_object(object)
    }
}

/// A database in terminus-store
///
/// Databases in terminus-store are basically just a label pointing to
/// a layer. Opening a read transaction to a database is just getting
/// hold of the layer it points at, as layers are read-only. Writing
/// to a database is just making it point to a new layer.
pub struct SyncDatabase {
    inner: Database,
    executor: TaskExecutor
}

impl SyncDatabase {
    fn wrap(inner: Database, executor: TaskExecutor) -> Self {
        Self {
            inner, executor
        }
    }

    /// Returns the layer this database points at
    pub fn head(&self) -> Result<Option<SyncDatabaseLayer>,io::Error> {
        let inner = oneshot::spawn(self.inner.head(), &self.executor).wait();

        inner.map(|i|i.map(|i|SyncDatabaseLayer::wrap(i, self.executor.clone())))
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise
    pub fn set_head(&self, layer: &SyncDatabaseLayer) -> Result<bool, io::Error> {
        oneshot::spawn(self.inner.set_head(&layer.inner), &self.executor).wait()
    }
}

/// A store, storing a set of layers and database labels pointing to these layers
pub struct SyncStore {
    inner: Store,
    runtime: Runtime
}

impl SyncStore {
    /// wrap an asynchronous `Store`, running all futures on the given tokio runtime
    pub fn wrap(inner: Store, runtime: Runtime) -> Self {
        Self {
            inner, runtime
        }
    }

    /// Create a new database with the given name
    ///
    /// If the database already exists, this will return an error
    pub fn create(&self, label: &str) -> Result<SyncDatabase,io::Error> {
        let inner = oneshot::spawn(self.inner.create(label), &self.runtime.executor()).wait();

        inner.map(|i| SyncDatabase::wrap(i, self.runtime.executor()))
    }

    /// Open an existing database with the given name, or None if it does not exist
    pub fn open(&self, label: &str) -> Result<Option<SyncDatabase>,io::Error> {
        let inner = oneshot::spawn(self.inner.open(label), &self.runtime.executor()).wait();

        inner.map(|i| i.map(|i|SyncDatabase::wrap(i, self.runtime.executor())))
    }

    /// Create a base layer builder, unattached to any database label
    ///
    /// After having committed it, use `set_head` on a `Database` to attach it.
    pub fn create_base_layer(&self) -> Result<SyncDatabaseLayerBuilder,io::Error> {
        let inner = oneshot::spawn(self.inner.create_base_layer(), &self.runtime.executor()).wait();

        inner.map(|i| SyncDatabaseLayerBuilder::wrap(i, self.runtime.executor()))
    }
}

/// Open a store that is entirely in memory
///
/// This is useful for testing purposes, or if the database is only going to be used for caching purposes
pub fn open_sync_memory_store() -> SyncStore {
    SyncStore::wrap(open_memory_store(), Runtime::new().unwrap())
}

/// Open a store that stores its data in the given directory
pub fn open_sync_directory_store<P:Into<PathBuf>>(path: P) -> SyncStore {
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
        builder.add_string_triple(&StringTriple::new_value("cow","says","moo")).unwrap();

        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink")).unwrap();

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
        builder.add_string_triple(&StringTriple::new_value("cow","says","moo")).unwrap();

        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink")).unwrap();

        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow","says","moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig","says","oink")));
    }
}
