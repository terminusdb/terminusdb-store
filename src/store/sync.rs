//! a synchronous version of the store API
//!
//! Since not everyone likes tokio, or dealing with async code, this
//! module exposes the same API as the asynchronous store API, only
//! without any futures. This is done by wrapping all the async calls
//! in a sync wrapper that runs on a tokio runtime managed by this
//! module.
use futures::Future;
use tokio::runtime::Runtime;

use std::io;
use std::path::PathBuf;

use crate::layer::{IdTriple, Layer, LayerCounts, ObjectType, StringTriple};
use crate::store::{
    open_directory_store, open_memory_store, NamedGraph, Store, StoreLayer, StoreLayerBuilder,
};

lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

/// Trampoline function for calling the async api in a sync way.
///
/// This convoluted mess was implemented because doing oneshot::spawn
/// directly on the async api functions resulted in a memory leak in
/// tokio_threadpool. Spawning the future indirectly appears to work
/// without memory leak.
pub fn task_sync<T: Send, F: Future<Output = T> + Send>(future: F) -> T {
    RUNTIME.block_on(future)
}

/// A wrapper over a SimpleLayerBuilder, providing a thread-safe sharable interface.
///
/// The SimpleLayerBuilder requires one to have a mutable reference to
/// it, and on commit it will be consumed. This builder only requires
/// an immutable reference, and uses a futures-aware read-write lock
/// to synchronize access to it between threads. Also, rather than
/// consuming itself on commit, this wrapper will simply mark itself
/// as having committed, returning errors on further calls.
#[derive(Clone)]
pub struct SyncStoreLayerBuilder {
    inner: StoreLayerBuilder,
}

impl SyncStoreLayerBuilder {
    fn wrap(inner: StoreLayerBuilder) -> Self {
        SyncStoreLayerBuilder { inner }
    }

    /// Returns the name of the layer being built.
    pub fn name(&self) -> [u32; 5] {
        self.inner.name()
    }

    /// Add a string triple.
    pub fn add_string_triple(&self, triple: StringTriple) -> Result<(), io::Error> {
        self.inner.add_string_triple(triple)
    }

    /// Add an id triple.
    pub fn add_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.inner.add_id_triple(triple)
    }

    /// Remove a string triple.
    pub fn remove_string_triple(&self, triple: StringTriple) -> Result<(), io::Error> {
        self.inner.remove_string_triple(triple)
    }

    /// Remove an id triple.
    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.inner.remove_id_triple(triple)
    }

    /// Returns a boolean result which is true if this builder has been committed, and false otherwise.
    pub fn committed(&self) -> bool {
        self.inner.committed()
    }

    /// Commit the layer to storage without loading the resulting layer.
    pub fn commit_no_load(&self) -> Result<(), io::Error> {
        task_sync(self.inner.commit_no_load())
    }

    /// Commit the layer to storage.
    pub fn commit(&self) -> Result<SyncStoreLayer, io::Error> {
        let inner = task_sync(self.inner.commit());

        inner.map(SyncStoreLayer::wrap)
    }

    /// Apply all triples added and removed by a layer to this builder.
    ///
    /// This is a way to 'cherry-pick' a layer on top of another
    /// layer, without caring about its history.
    pub fn apply_delta(&self, delta: &SyncStoreLayer) -> Result<(), io::Error> {
        task_sync(self.inner.apply_delta(&delta.inner))
    }

    /// Apply the changes required to change our parent layer into the given layer.
    pub fn apply_diff(&self, other: &SyncStoreLayer) -> Result<(), io::Error> {
        self.inner.apply_diff(&other.inner)
    }
}

/// A layer that keeps track of the store it came out of, allowing the creation of a layer builder on top of this layer.
///
/// This type of layer supports querying what was added and what was
/// removed in this layer. This can not be done in general, because
/// the layer that has been loaded may not be the layer that was
/// originally built. This happens whenever a rollup is done. A rollup
/// will create a new layer that bundles the changes of various
/// layers. It allows for more efficient querying, but loses the
/// ability to do these delta queries directly. In order to support
/// them anyway, the SyncStoreLayer will dynamically load in the
/// relevant files to perform the requested addition or removal query
/// method.
#[derive(Clone)]
pub struct SyncStoreLayer {
    inner: StoreLayer,
}

impl SyncStoreLayer {
    fn wrap(inner: StoreLayer) -> Self {
        Self { inner }
    }

    /// Create a layer builder based on this layer.
    pub fn open_write(&self) -> Result<SyncStoreLayerBuilder, io::Error> {
        let inner = task_sync(self.inner.open_write());

        inner.map(SyncStoreLayerBuilder::wrap)
    }

    /// Returns the parent of this layer, if any, or None if this layer has no parent.
    pub fn parent(&self) -> Result<Option<SyncStoreLayer>, io::Error> {
        let inner = task_sync(self.inner.parent());
        inner.map(|p| p.map(|p| SyncStoreLayer { inner: p }))
    }

    /// Create a new base layer consisting of all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Squash is one approach of
    /// accomplishing this. Rollup is another. Squash is the better
    /// option if you do not care for history, as it throws away all
    /// data that you no longer need.
    pub fn squash(&self) -> Result<SyncStoreLayer, io::Error> {
        let inner = task_sync(self.inner.clone().squash());

        inner.map(SyncStoreLayer::wrap)
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub fn rollup(&self) -> Result<(), io::Error> {
        task_sync(self.inner.clone().rollup())
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all ancestors up to (but not including) the given ancestor.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub fn rollup_upto(&self, upto: &SyncStoreLayer) -> Result<(), io::Error> {
        task_sync(self.inner.clone().rollup_upto(&upto.inner))
    }

    /// Like rollup_upto, rolls up upto the given layer. However, if
    /// this layer is a rollup layer, this will roll up upto that
    /// rollup.
    pub fn imprecise_rollup_upto(&self, upto: &SyncStoreLayer) -> Result<(), io::Error> {
        task_sync(self.inner.clone().imprecise_rollup_upto(&upto.inner))
    }

    /// Returns true if this triple has been added in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        task_sync(
            self.inner
                .triple_addition_exists(subject, predicate, object),
        )
    }

    /// Returns true if this triple has been removed in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        task_sync(self.inner.triple_removal_exists(subject, predicate, object))
    }

    /// Returns an iterator over all layer additions.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_additions())
    }

    /// Returns an iterator over all layer removals.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_removals())
    }

    /// Returns an iterator over all layer additions that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_additions_s(subject))
    }

    /// Returns an iterator over all layer removals that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_removals_s(subject))
    }

    /// Returns an iterator over all layer additions that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_additions_sp(subject, predicate))
    }

    /// Returns an iterator over all layer removals that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_removals_sp(subject, predicate))
    }

    /// Returns an iterator over all layer additions that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_additions_p(predicate))
    }

    /// Returns an iterator over all layer removals that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_removals_p(predicate))
    }

    /// Returns an iterator over all layer additions that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_additions_o(object))
    }

    /// Returns an iterator over all layer removals that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        task_sync(self.inner.triple_removals_o(object))
    }

    /// Returns the amount of triples that this layer adds.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_layer_addition_count(&self) -> io::Result<usize> {
        task_sync(self.inner.triple_layer_addition_count())
    }

    /// Returns the amount of triples that this layer removes.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_layer_removal_count(&self) -> io::Result<usize> {
        task_sync(self.inner.triple_layer_removal_count())
    }

    /// Returns a vector of layer stack names describing the history of this layer, starting from the base layer up to and including the name of this layer itself.
    pub fn retrieve_layer_stack_names(&self) -> io::Result<Vec<[u32; 5]>> {
        task_sync(self.inner.retrieve_layer_stack_names())
    }
}

impl Layer for SyncStoreLayer {
    fn name(&self) -> [u32; 5] {
        self.inner.name()
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.inner.parent_name()
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

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.inner.triple_exists(subject, predicate, object)
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples()
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_s(subject)
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_sp(subject, predicate)
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_p(predicate)
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_o(object)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_addition_count(&self) -> usize {
        self.inner.triple_addition_count()
    }

    fn triple_removal_count(&self) -> usize {
        self.inner.triple_removal_count()
    }

    fn all_counts(&self) -> LayerCounts {
        self.inner.all_counts()
    }
}

/// A named graph in terminus-store.
///
/// Named graphs in terminus-store are basically just a label pointing
/// to a layer. Opening a read transaction to a named graph is just
/// getting hold of the layer it points at, as layers are
/// read-only. Writing to a named graph is just making it point to a
/// new layer.
#[derive(Clone)]
pub struct SyncNamedGraph {
    inner: NamedGraph,
}

impl SyncNamedGraph {
    fn wrap(inner: NamedGraph) -> Self {
        Self { inner }
    }

    /// Returns the label name itself.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns the layer this database points at, as well as the label version.
    pub fn head_version(&self) -> io::Result<(Option<SyncStoreLayer>, u64)> {
        let inner = task_sync(self.inner.head_version());

        inner.map(|(layer, version)| (layer.map(SyncStoreLayer::wrap), version))
    }

    /// Returns the layer this database points at.
    pub fn head(&self) -> io::Result<Option<SyncStoreLayer>> {
        let inner = task_sync(self.inner.head());

        inner.map(|i| i.map(SyncStoreLayer::wrap))
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise.
    pub fn set_head(&self, layer: &SyncStoreLayer) -> Result<bool, io::Error> {
        task_sync(self.inner.set_head(&layer.inner))
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor.
    pub fn force_set_head(&self, layer: &SyncStoreLayer) -> Result<(), io::Error> {
        task_sync(self.inner.force_set_head(&layer.inner))
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor. Also checks given version, and if it doesn't match, the update won't happen and false will be returned.
    pub fn force_set_head_version(&self, layer: &SyncStoreLayer, version: u64) -> io::Result<bool> {
        task_sync(self.inner.force_set_head_version(&layer.inner, version))
    }

    pub fn delete(&self) -> io::Result<()> {
        task_sync(self.inner.delete())
    }
}

/// A store, storing a set of layers and database labels pointing to these layers.
#[derive(Clone)]
pub struct SyncStore {
    inner: Store,
}

impl SyncStore {
    /// wrap an asynchronous `Store`, running all futures on a lazily-constructed tokio runtime.
    pub fn wrap(inner: Store) -> Self {
        Self { inner }
    }

    /// Create a new database with the given name.
    ///
    /// If the database already exists, this will return an error.
    pub fn create(&self, label: &str) -> Result<SyncNamedGraph, io::Error> {
        let inner = task_sync(self.inner.create(label));

        inner.map(SyncNamedGraph::wrap)
    }

    /// Open an existing database with the given name, or None if it does not exist.
    pub fn open(&self, label: &str) -> Result<Option<SyncNamedGraph>, io::Error> {
        let inner = task_sync(self.inner.open(label));

        inner.map(|i| i.map(SyncNamedGraph::wrap))
    }

    /// Delete an existing database with the given name. Returns true if this database was deleted
    /// and false otherwise.
    pub fn delete(&self, label: &str) -> io::Result<bool> {
        task_sync(self.inner.delete(label))
    }

    /// Retrieve a layer with the given name from the layer store this Store was initialized with.
    pub fn get_layer_from_id(
        &self,
        layer: [u32; 5],
    ) -> Result<Option<SyncStoreLayer>, std::io::Error> {
        let inner = task_sync(self.inner.get_layer_from_id(layer));

        inner.map(|layer| layer.map(SyncStoreLayer::wrap))
    }

    /// Create a base layer builder, unattached to any database label.
    ///
    /// After having committed it, use `set_head` on a `NamedGraph` to attach it.
    pub fn create_base_layer(&self) -> Result<SyncStoreLayerBuilder, io::Error> {
        let inner = task_sync(self.inner.create_base_layer());

        inner.map(SyncStoreLayerBuilder::wrap)
    }

    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    pub fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        task_sync(self.inner.layer_store.export_layers(layer_ids))
    }

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    pub fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        task_sync(self.inner.layer_store.import_layers(pack, layer_ids))
    }
}

/// Open a store that is entirely in memory.
///
/// This is useful for testing purposes, or if the database is only going to be used for caching purposes.
pub fn open_sync_memory_store() -> SyncStore {
    SyncStore::wrap(open_memory_store())
}

/// Open a store that stores its data in the given directory.
pub fn open_sync_directory_store<P: Into<PathBuf>>(path: P) -> io::Result<SyncStore> {
    Ok(SyncStore::wrap(open_directory_store(path)?))
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
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder
            .add_string_triple(StringTriple::new_value("pig", "says", "oink"))
            .unwrap();

        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
    }

    #[test]
    fn create_and_manipulate_sync_directory_database() {
        let dir = tempdir().unwrap();
        let store = open_sync_directory_store(dir.path()).unwrap();
        let database = store.create("foodb").unwrap();

        let head = database.head().unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().unwrap();
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder
            .add_string_triple(StringTriple::new_value("pig", "says", "oink"))
            .unwrap();

        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
    }

    #[test]
    fn create_sync_layer_and_retrieve_it_by_id() {
        let store = open_sync_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().unwrap();

        let id = layer.name();

        let layer2 = store.get_layer_from_id(id).unwrap().unwrap();
        assert!(layer2.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[test]
    fn commit_builder_makes_builder_committed() {
        let store = open_sync_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        assert!(!builder.committed());

        let _layer = builder.commit().unwrap();

        assert!(builder.committed());
    }

    use crate::storage::pack::pack_layer_parents;
    #[test]
    fn export_and_import_pack() {
        let dir1 = tempdir().unwrap();
        let store1 = open_sync_directory_store(dir1.path()).unwrap();

        let dir2 = tempdir().unwrap();
        let store2 = open_sync_directory_store(dir2.path()).unwrap();

        let builder1 = store1.create_base_layer().unwrap();
        builder1
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();
        let layer1 = builder1.commit().unwrap();

        let builder2 = store1.create_base_layer().unwrap();
        builder2
            .add_string_triple(StringTriple::new_value("duck", "says", "quack"))
            .unwrap();
        let layer2 = builder2.commit().unwrap();

        let builder3 = layer2.open_write().unwrap();
        builder3
            .add_string_triple(StringTriple::new_value("horse", "says", "neigh"))
            .unwrap();
        let layer3 = builder3.commit().unwrap();

        let ids = vec![layer1.name(), layer2.name(), layer3.name()];
        let pack = store1
            .export_layers(Box::new(ids.clone().into_iter()))
            .unwrap();

        let parents_map = pack_layer_parents(io::Cursor::new(&pack)).unwrap();

        assert_eq!(3, parents_map.len());
        assert_eq!(None, parents_map[&layer1.name()]);
        assert_eq!(None, parents_map[&layer2.name()]);
        assert_eq!(Some(layer2.name()), parents_map[&layer3.name()]);

        store2
            .import_layers(&pack, Box::new(ids.into_iter()))
            .unwrap();

        let result_layer = store2.get_layer_from_id(layer3.name()).unwrap().unwrap();
        assert!(
            result_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack"))
        );
        assert!(
            result_layer.string_triple_exists(&StringTriple::new_value("horse", "says", "neigh"))
        );
    }
}
