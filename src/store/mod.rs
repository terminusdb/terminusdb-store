//! High-level API for working with terminus-store.
//!
//! It is expected that most users of this library will work exclusively with the types contained in this module.
pub mod sync;

use futures::future;
use futures::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::layer::{
    IdTriple, Layer, LayerBuilder, LayerCounts, LayerObjectLookup, LayerPredicateLookup,
    LayerSubjectLookup, ObjectLookup, ObjectType, PredicateLookup, StringTriple, SubjectLookup,
};
use crate::storage::directory::{DirectoryLabelStore, DirectoryLayerStore};
use crate::storage::memory::{MemoryLabelStore, MemoryLayerStore};
use crate::storage::{CachedLayerStore, LabelStore, LayerStore, LockingHashMapLayerCache};

use std::io;

/// A wrapper over a SimpleLayerBuilder, providing a thread-safe sharable interface
///
/// The SimpleLayerBuilder requires one to have a mutable reference to
/// it, and on commit it will be consumed. This builder only requires
/// an immutable reference, and uses a futures-aware read-write lock
/// to synchronize access to it between threads. Also, rather than
/// consuming itself on commit, this wrapper will simply mark itself
/// as having committed, returning errors on further calls.
pub struct StoreLayerBuilder {
    builder: RwLock<Option<Box<dyn LayerBuilder>>>,
    name: [u32; 5],
    store: Store,
}

impl StoreLayerBuilder {
    fn new(store: Store) -> impl Future<Item = Self, Error = io::Error> + Send {
        store.layer_store.create_base_layer().map(|builder| Self {
            name: builder.name(),
            builder: RwLock::new(Some(builder)),
            store,
        })
    }

    fn wrap(builder: Box<dyn LayerBuilder>, store: Store) -> Self {
        StoreLayerBuilder {
            name: builder.name(),
            builder: RwLock::new(Some(builder)),
            store,
        }
    }

    fn with_builder<R, F: FnOnce(&mut Box<dyn LayerBuilder>) -> R>(
        &self,
        f: F,
    ) -> Result<R, io::Error> {
        let mut builder = self
            .builder
            .write()
            .expect("rwlock write should always succeed");
        match (*builder).as_mut() {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "builder has already been committed",
            )),
            Some(builder) => Ok(f(builder)),
        }
    }

    /// Returns the name of the layer being built
    pub fn name(&self) -> [u32; 5] {
        self.name
    }

    /// Add a string triple
    pub fn add_string_triple(&self, triple: &StringTriple) -> Result<(), io::Error> {
        let triple = triple.clone();
        self.with_builder(move |b| b.add_string_triple(&triple))
    }

    /// Add an id triple
    pub fn add_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_id_triple(triple))
    }

    /// Remove a string triple
    pub fn remove_string_triple(&self, triple: &StringTriple) -> Result<(), io::Error> {
        let triple = triple.clone();
        self.with_builder(move |b| b.remove_string_triple(&triple))
    }

    /// Remove an id triple
    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_id_triple(triple))
    }

    /// Returns a Future which will yield true if this layer has been committed, and false otherwise.
    pub fn committed(&self) -> bool {
        self.builder
            .read()
            .expect("rwlock write should always succeed")
            .is_none()
    }

    /// Commit the layer to storage
    pub fn commit(&self) -> impl Future<Item = StoreLayer, Error = std::io::Error> + Send {
        let store = self.store.clone();
        let name = self.name;
        let mut guard = self
            .builder
            .write()
            .expect("rwlock write should always succeed");
        let mut builder = None;

        // Setting the builder to None ensures that committed() detects we already committed (or tried to do so anyway)
        std::mem::swap(&mut builder, &mut guard);

        let result: Box<dyn Future<Item = _, Error = _> + Send> = match builder {
            None => Box::new(future::err(io::Error::new(
                io::ErrorKind::InvalidData,
                "builder has already been committed",
            ))),
            Some(builder) => Box::new(builder.commit_boxed().and_then(move |_| {
                store.layer_store.get_layer(name).map(move |layer| {
                    StoreLayer::wrap(
                        layer.expect("layer that was just created was not found in store"),
                        store,
                    )
                })
            })),
        };

        result
    }

    pub fn apply_delta(&self, delta: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        delta
            .triple_additions()
            .map(|t| {
                delta
                    .id_triple_to_string(&t)
                    .map(|st| self.add_string_triple(&st))
            })
            .for_each(|_| ());

        delta
            .triple_removals()
            .map(|t| {
                delta
                    .id_triple_to_string(&t)
                    .map(|st| self.remove_string_triple(&st))
            })
            .for_each(|_| ());

        Ok(())
    }
}

/// A layer that keeps track of the store it came out of, allowing the creation of a layer builder on top of this layer
#[derive(Clone)]
pub struct StoreLayer {
    // TODO this Arc here is not great
    layer: Arc<dyn Layer>,
    store: Store,
}

impl StoreLayer {
    fn wrap(layer: Arc<dyn Layer>, store: Store) -> Self {
        StoreLayer { layer, store }
    }

    /// Create a layer builder based on this layer
    pub fn open_write(&self) -> impl Future<Item = StoreLayerBuilder, Error = io::Error> + Send {
        let store = self.store.clone();
        self.store
            .layer_store
            .create_child_layer(self.layer.name())
            .map(move |layer| StoreLayerBuilder::wrap(layer, store))
    }

    pub fn parent(&self) -> Box<dyn Future<Item = Option<StoreLayer>, Error = io::Error> + Send> {
        let parent_name = self.layer.parent_name();

        let store = self.store.clone();

        Box::new(match parent_name {
            None => future::Either::A(future::ok(None)),
            Some(parent_name) => future::Either::B(
                self.store
                    .layer_store
                    .get_layer(parent_name)
                    .map(move |l| l.map(|layer| StoreLayer::wrap(layer, store))),
            ),
        })
    }

    pub fn squash(&self) -> impl Future<Item = StoreLayer, Error = io::Error> + Send {
        let self_clone = self.clone();

        // TODO check if we already committed
        self.store
            .create_base_layer()
            .and_then(move |new_builder: StoreLayerBuilder| {
                let iter = self_clone
                    .triples()
                    .map(|t| self_clone.id_triple_to_string(&t).unwrap());

                for st in iter {
                    new_builder.add_string_triple(&st).unwrap()
                }

                new_builder.commit()
            })
    }
}

impl Layer for StoreLayer {
    fn name(&self) -> [u32; 5] {
        self.layer.name()
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.layer.parent_name()
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

    fn subjects(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectLookup>>> {
        self.layer.subjects()
    }

    fn subject_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>> {
        self.layer.subject_additions()
    }

    fn subject_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>> {
        self.layer.subject_removals()
    }

    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        self.layer.lookup_subject(subject)
    }

    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        self.layer.lookup_subject_addition(subject)
    }

    fn lookup_subject_removal(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        self.layer.lookup_subject_removal(subject)
    }

    fn objects(&self) -> Box<dyn Iterator<Item = Box<dyn ObjectLookup>>> {
        self.layer.objects()
    }

    fn object_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>> {
        self.layer.object_additions()
    }

    fn object_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>> {
        self.layer.object_removals()
    }

    fn lookup_object(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.layer.lookup_object(object)
    }

    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        self.layer.lookup_object_addition(object)
    }

    fn lookup_object_removal(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        self.layer.lookup_object_removal(object)
    }

    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn PredicateLookup>>> {
        self.layer.predicates()
    }

    fn predicate_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerPredicateLookup>>> {
        self.layer.predicate_additions()
    }

    fn predicate_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerPredicateLookup>>> {
        self.layer.predicate_removals()
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        self.layer.lookup_predicate(predicate)
    }

    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        self.layer.lookup_predicate_addition(predicate)
    }

    fn lookup_predicate_removal(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        self.layer.lookup_predicate_removal(predicate)
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        self.layer.triples()
    }

    fn triple_additions(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        self.layer.triple_additions()
    }

    fn triple_removals(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        self.layer.triple_removals()
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        self.layer.triples_s(subject)
    }

    fn triple_additions_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        self.layer.triple_additions_s(subject)
    }

    fn triple_removals_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        self.layer.triple_removals_s(subject)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_layer_addition_count(&self) -> usize {
        self.layer.triple_layer_addition_count()
    }

    fn triple_layer_removal_count(&self) -> usize {
        self.layer.triple_layer_removal_count()
    }

    fn triple_addition_count(&self) -> usize {
        self.layer.triple_addition_count()
    }

    fn triple_removal_count(&self) -> usize {
        self.layer.triple_removal_count()
    }

    fn all_counts(&self) -> LayerCounts {
        self.layer.all_counts()
    }
}

/// A named graph in terminus-store.
///
/// Named graphs in terminus-store are basically just a label pointing
/// to a layer. Opening a read transaction to a named graph is just
/// getting hold of the layer it points at, as layers are
/// read-only. Writing to a named graph is just making it point to a
/// new layer.
pub struct NamedGraph {
    label: String,
    store: Store,
}

impl NamedGraph {
    fn new(label: String, store: Store) -> Self {
        NamedGraph { label, store }
    }

    pub fn name(&self) -> &str {
        &self.label
    }

    /// Returns the layer this database points at
    pub fn head(&self) -> impl Future<Item = Option<StoreLayer>, Error = io::Error> + Send {
        let store = self.store.clone();
        store
            .label_store
            .get_label(&self.label)
            .and_then(move |new_label| match new_label {
                None => Box::new(future::err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "database not found",
                ))),
                Some(new_label) => {
                    let result: Box<dyn Future<Item = _, Error = _> + Send> = match new_label.layer
                    {
                        None => Box::new(future::ok(None)),
                        Some(layer) => {
                            Box::new(store.layer_store.get_layer(layer).map(move |layer| {
                                layer.map(move |layer| StoreLayer::wrap(layer, store))
                            }))
                        }
                    };
                    result
                }
            })
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise
    pub fn set_head(
        &self,
        layer: &StoreLayer,
    ) -> impl Future<Item = bool, Error = io::Error> + Send {
        let store = self.store.clone();
        let store2 = self.store.clone();
        let layer_name = layer.name();
        let cloned_layer = layer.layer.clone();
        store
            .label_store
            .get_label(&self.label)
            .and_then(move |label| {
                let result: Box<dyn Future<Item = _, Error = _> + Send> = match label {
                    None => Box::new(future::err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "label not found",
                    ))),
                    Some(label) => Box::new(
                        {
                            let result: Box<dyn Future<Item = _, Error = _> + Send> = match label
                                .layer
                            {
                                None => Box::new(future::ok(true)),
                                Some(layer_name) => Box::new(
                                    store.layer_store.get_layer(layer_name).and_then(move |l| {
                                        l.map(|l| {
                                            future::Either::A(
                                                store.layer_store.layer_is_ancestor_of(
                                                    cloned_layer.name(),
                                                    l.name(),
                                                ),
                                            )
                                        })
                                        .unwrap_or(future::Either::B(future::ok(false)))
                                    }),
                                ),
                            };

                            result
                        }
                        .and_then(move |b| {
                            let result: Box<dyn Future<Item = _, Error = _> + Send> = if b {
                                Box::new(
                                    store2
                                        .label_store
                                        .set_label(&label, layer_name)
                                        .map(|_| true),
                                )
                            } else {
                                Box::new(future::ok(false))
                            };

                            result
                        }),
                    ),
                };
                result
            })
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise
    pub fn force_set_head(
        &self,
        layer: &StoreLayer,
    ) -> impl Future<Item = bool, Error = io::Error> + Send {
        let store = self.store.clone();
        let layer_name = layer.name();
        store
            .label_store
            .get_label(&self.label)
            .and_then(move |label| {
                let result: Box<dyn Future<Item = _, Error = _> + Send> = match label {
                    None => Box::new(future::err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "label not found",
                    ))),
                    Some(label) => Box::new({
                        store
                            .label_store
                            .set_label(&label, layer_name)
                            .map(|_| true)
                    }),
                };
                result
            })
    }
}

/// A store, storing a set of layers and database labels pointing to these layers
#[derive(Clone)]
pub struct Store {
    label_store: Arc<dyn LabelStore>,
    layer_store: Arc<dyn LayerStore>,
}

impl Store {
    /// Create a new store from the given label and layer store
    pub fn new<Labels: 'static + LabelStore, Layers: 'static + LayerStore>(
        label_store: Labels,
        layer_store: Layers,
    ) -> Store {
        Store {
            label_store: Arc::new(label_store),
            layer_store: Arc::new(layer_store),
        }
    }

    /// Create a new database with the given name
    ///
    /// If the database already exists, this will return an error
    pub fn create(
        &self,
        label: &str,
    ) -> impl Future<Item = NamedGraph, Error = std::io::Error> + Send {
        let store = self.clone();
        self.label_store
            .create_label(label)
            .map(move |label| NamedGraph::new(label.name, store))
    }

    /// Open an existing database with the given name, or None if it does not exist
    pub fn open(
        &self,
        label: &str,
    ) -> impl Future<Item = Option<NamedGraph>, Error = std::io::Error> {
        let store = self.clone();
        self.label_store
            .get_label(label)
            .map(move |label| label.map(|label| NamedGraph::new(label.name, store)))
    }

    pub fn get_layer_from_id(
        &self,
        layer: [u32; 5],
    ) -> impl Future<Item = Option<StoreLayer>, Error = std::io::Error> {
        let store = self.clone();
        self.layer_store
            .get_layer(layer)
            .map(move |layer| layer.map(move |l| StoreLayer::wrap(l, store)))
    }

    /// Create a base layer builder, unattached to any database label
    ///
    /// After having committed it, use `set_head` on a `NamedGraph` to attach it.
    pub fn create_base_layer(
        &self,
    ) -> impl Future<Item = StoreLayerBuilder, Error = io::Error> + Send {
        StoreLayerBuilder::new(self.clone())
    }

    pub fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        self.layer_store.export_layers(layer_ids)
    }
    pub fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        self.layer_store.import_layers(pack, layer_ids)
    }
}

/// Open a store that is entirely in memory
///
/// This is useful for testing purposes, or if the database is only going to be used for caching purposes
pub fn open_memory_store() -> Store {
    Store::new(
        MemoryLabelStore::new(),
        CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new()),
    )
}

/// Open a store that stores its data in the given directory
pub fn open_directory_store<P: Into<PathBuf>>(path: P) -> Store {
    let p = path.into();
    Store::new(
        DirectoryLabelStore::new(p.clone()),
        CachedLayerStore::new(DirectoryLayerStore::new(p), LockingHashMapLayerCache::new()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::sync::oneshot;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    #[test]
    fn create_and_manipulate_memory_database() {
        let runtime = Runtime::new().unwrap();

        let store = open_memory_store();
        let database = oneshot::spawn(store.create("foodb"), &runtime.executor())
            .wait()
            .unwrap();

        let head = oneshot::spawn(database.head(), &runtime.executor())
            .wait()
            .unwrap();
        assert!(head.is_none());

        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();
        assert!(
            oneshot::spawn(database.set_head(&layer), &runtime.executor())
                .wait()
                .unwrap()
        );

        builder = oneshot::spawn(layer.open_write(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("pig", "says", "oink"))
            .unwrap();

        let layer2 = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();
        assert!(
            oneshot::spawn(database.set_head(&layer2), &runtime.executor())
                .wait()
                .unwrap()
        );
        let layer2_name = layer2.name();

        let layer = oneshot::spawn(database.head(), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
    }

    #[test]
    fn create_and_manipulate_directory_database() {
        let runtime = Runtime::new().unwrap();
        let dir = tempdir().unwrap();

        let store = open_directory_store(dir.path());
        let database = oneshot::spawn(store.create("foodb"), &runtime.executor())
            .wait()
            .unwrap();

        let head = oneshot::spawn(database.head(), &runtime.executor())
            .wait()
            .unwrap();
        assert!(head.is_none());

        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();
        assert!(
            oneshot::spawn(database.set_head(&layer), &runtime.executor())
                .wait()
                .unwrap()
        );

        builder = oneshot::spawn(layer.open_write(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("pig", "says", "oink"))
            .unwrap();

        let layer2 = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();
        assert!(
            oneshot::spawn(database.set_head(&layer2), &runtime.executor())
                .wait()
                .unwrap()
        );
        let layer2_name = layer2.name();

        let layer = oneshot::spawn(database.head(), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
    }

    #[test]
    fn create_layer_and_retrieve_it_by_id() {
        let runtime = Runtime::new().unwrap();

        let store = open_memory_store();
        let builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let id = layer.name();

        let layer2 = oneshot::spawn(store.get_layer_from_id(id), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();
        assert!(layer2.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[test]
    fn commit_builder_makes_builder_committed() {
        let runtime = Runtime::new().unwrap();

        let store = open_memory_store();
        let builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        assert!(!builder.committed());

        let _layer = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        assert!(builder.committed());
    }

    #[test]
    fn hard_reset() {
        let runtime = Runtime::new().unwrap();

        let store = open_memory_store();
        let database = oneshot::spawn(store.create("foodb"), &runtime.executor())
            .wait()
            .unwrap();

        let builder1 = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder1
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer1 = oneshot::spawn(builder1.commit(), &runtime.executor())
            .wait()
            .unwrap();

        assert!(
            oneshot::spawn(database.set_head(&layer1), &runtime.executor())
                .wait()
                .unwrap()
        );

        let builder2 = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder2
            .add_string_triple(&StringTriple::new_value("duck", "says", "quack"))
            .unwrap();

        let layer2 = oneshot::spawn(builder2.commit(), &runtime.executor())
            .wait()
            .unwrap();

        assert!(
            oneshot::spawn(database.force_set_head(&layer2), &runtime.executor())
                .wait()
                .unwrap()
        );

        let new_layer = oneshot::spawn(database.head(), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();

        assert!(new_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
        assert!(!new_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[test]
    fn create_two_layers_and_squash() {
        let runtime = Runtime::new().unwrap();

        let store = open_memory_store();
        let builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        builder
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let builder2 = oneshot::spawn(layer.open_write(), &runtime.executor())
            .wait()
            .unwrap();

        builder2
            .add_string_triple(&StringTriple::new_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = oneshot::spawn(builder2.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let new = layer2.squash().wait().unwrap();

        assert!(new.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(new.string_triple_exists(&StringTriple::new_value("dog", "says", "woof")));
        assert!(oneshot::spawn(new.parent(), &runtime.executor())
            .wait()
            .unwrap()
            .is_none());
    }

    #[test]
    fn apply_a_base_delta() {
        let runtime = Runtime::new().unwrap();

        let store = open_memory_store();
        let builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();

        builder
            .add_string_triple(&StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let builder2 = oneshot::spawn(layer.open_write(), &runtime.executor())
            .wait()
            .unwrap();

        builder2
            .add_string_triple(&StringTriple::new_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = oneshot::spawn(builder2.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let delta_builder_1 = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();

        delta_builder_1
            .add_string_triple(&StringTriple::new_value("dog", "says", "woof"))
            .unwrap();
        delta_builder_1
            .add_string_triple(&StringTriple::new_value("cat", "says", "meow"))
            .unwrap();

        let delta_1 = oneshot::spawn(delta_builder_1.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let delta_builder_2 = oneshot::spawn(delta_1.open_write(), &runtime.executor())
            .wait()
            .unwrap();

        delta_builder_2
            .add_string_triple(&StringTriple::new_value("crow", "says", "caw"))
            .unwrap();
        delta_builder_2
            .remove_string_triple(&StringTriple::new_value("cat", "says", "meow"))
            .unwrap();

        let delta = oneshot::spawn(delta_builder_2.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let rebase_builder = oneshot::spawn(layer2.open_write(), &runtime.executor())
            .wait()
            .unwrap();

        let _ = rebase_builder.apply_delta(&delta).unwrap();

        let rebase_layer = oneshot::spawn(rebase_builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        assert!(rebase_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(rebase_layer.string_triple_exists(&StringTriple::new_value("crow", "says", "caw")));
        assert!(rebase_layer.string_triple_exists(&StringTriple::new_value("dog", "says", "woof")));
        assert!(!rebase_layer.string_triple_exists(&StringTriple::new_value("cat", "says", "meow")));
    }
}
