//! High-level API for working with terminus-store.
//!
//! It is expected that most users of this library will work exclusively with the types contained in this module.
pub mod sync;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::layer::{IdTriple, Layer, LayerBuilder, LayerCounts, ObjectType, StringTriple};
use crate::storage::directory::{DirectoryLabelStore, DirectoryLayerStore};
use crate::storage::memory::{MemoryLabelStore, MemoryLayerStore};
use crate::storage::{CachedLayerStore, LabelStore, LayerStore, LockingHashMapLayerCache};

use std::io;
use std::pin::Pin;

use rayon::prelude::*;

use futures::future::Future;

/// A store, storing a set of layers and database labels pointing to these layers.
#[derive(Clone)]
pub struct Store {
    label_store: Arc<dyn LabelStore>,
    layer_store: Arc<dyn LayerStore>,
}

/// A wrapper over a SimpleLayerBuilder, providing a thread-safe sharable interface.
///
/// The SimpleLayerBuilder requires one to have a mutable reference to
/// the underlying LayerBuilder, and on commit it will be
/// consumed. This builder only requires an immutable reference, and
/// uses a futures-aware read-write lock to synchronize access to it
/// between threads. Also, rather than consuming itself on commit,
/// this wrapper will simply mark itself as having committed,
/// returning errors on further calls.
#[derive(Clone)]
pub struct StoreLayerBuilder {
    parent: Option<Arc<dyn Layer>>,
    builder: Arc<RwLock<Option<Box<dyn LayerBuilder>>>>,
    name: [u32; 5],
    store: Store,
}

impl StoreLayerBuilder {
    async fn new(store: Store) -> io::Result<Self> {
        let builder = store.layer_store.create_base_layer().await?;

        Ok(Self {
            parent: builder.parent(),
            name: builder.name(),
            builder: Arc::new(RwLock::new(Some(builder))),
            store,
        })
    }

    fn wrap(builder: Box<dyn LayerBuilder>, store: Store) -> Self {
        StoreLayerBuilder {
            parent: builder.parent(),
            name: builder.name(),
            builder: Arc::new(RwLock::new(Some(builder))),
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

    /// Returns the name of the layer being built.
    pub fn name(&self) -> [u32; 5] {
        self.name
    }

    /// Returns the parent layer this builder is building on top of, if any.
    ///
    /// If there's no parent, this returns None.
    pub fn parent(&self) -> Option<Arc<dyn Layer>> {
        self.parent.clone()
    }

    /// Add a string triple.
    pub fn add_string_triple(&self, triple: StringTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_string_triple(triple))
    }

    /// Add an id triple.
    pub fn add_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_id_triple(triple))
    }

    /// Remove a string triple.
    pub fn remove_string_triple(&self, triple: StringTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_string_triple(triple))
    }

    /// Remove an id triple.
    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_id_triple(triple))
    }

    /// Returns true if this layer has been committed, and false otherwise.
    pub fn committed(&self) -> bool {
        self.builder
            .read()
            .expect("rwlock write should always succeed")
            .is_none()
    }

    /// Commit the layer to storage without loading the resulting layer.
    pub async fn commit_no_load(&self) -> io::Result<()> {
        let mut builder = None;
        {
            let mut guard = self
                .builder
                .write()
                .expect("rwlock write should always succeed");

            // Setting the builder to None ensures that committed() detects we already committed (or tried to do so anyway)
            std::mem::swap(&mut builder, &mut guard);
        }

        match builder {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "builder has already been committed",
            )),
            Some(builder) => builder.commit_boxed().await,
        }
    }

    /// Commit the layer to storage.
    pub async fn commit(&self) -> io::Result<StoreLayer> {
        let name = self.name;
        self.commit_no_load().await?;

        let layer = self.store.layer_store.get_layer(name).await?;
        Ok(StoreLayer::wrap(
            layer.expect("layer that was just created was not found in store"),
            self.store.clone(),
        ))
    }

    /// Apply all triples added and removed by a layer to this builder.
    ///
    /// This is a way to 'cherry-pick' a layer on top of another
    /// layer, without caring about its history.
    pub async fn apply_delta(&self, delta: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        let triple_additions = delta.triple_additions().await?;
        let triple_removals = delta.triple_removals().await?;
        rayon::join(
            move || {
                triple_additions.par_bridge().for_each(|t| {
                    delta
                        .id_triple_to_string(&t)
                        .map(|st| self.add_string_triple(st));
                });
            },
            move || {
                triple_removals.par_bridge().for_each(|t| {
                    delta
                        .id_triple_to_string(&t)
                        .map(|st| self.remove_string_triple(st));
                })
            },
        );

        Ok(())
    }

    /// Apply the changes required to change our parent layer into the given layer.
    pub fn apply_diff(&self, other: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        rayon::join(
            || {
                if let Some(this) = self.parent() {
                    this.triples().par_bridge().for_each(|t| {
                        if let Some(st) = this.id_triple_to_string(&t) {
                            if !other.string_triple_exists(&st) {
                                self.remove_string_triple(st).unwrap()
                            }
                        }
                    })
                };
            },
            || {
                other.triples().par_bridge().for_each(|t| {
                    if let Some(st) = other.id_triple_to_string(&t) {
                        if let Some(this) = self.parent() {
                            if !this.string_triple_exists(&st) {
                                self.add_string_triple(st).unwrap()
                            }
                        } else {
                            self.add_string_triple(st).unwrap()
                        };
                    }
                })
            },
        );

        Ok(())
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
/// them anyway, the StoreLayer will dynamically load in the relevant
/// files to perform the requested addition or removal query method.
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

    /// Create a layer builder based on this layer.
    pub async fn open_write(&self) -> io::Result<StoreLayerBuilder> {
        let layer = self
            .store
            .layer_store
            .create_child_layer(self.layer.name())
            .await?;

        Ok(StoreLayerBuilder::wrap(layer, self.store.clone()))
    }

    /// Returns the parent of this layer, if any, or None if this layer has no parent.
    pub async fn parent(&self) -> io::Result<Option<StoreLayer>> {
        let parent_name = self.layer.parent_name();

        match parent_name {
            None => Ok(None),
            Some(parent_name) => match self.store.layer_store.get_layer(parent_name).await? {
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found even though it should exist",
                )),
                Some(layer) => Ok(Some(StoreLayer::wrap(layer, self.store.clone()))),
            },
        }
    }

    /// Create a new base layer consisting of all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Squash is one approach of
    /// accomplishing this. Rollup is another. Squash is the better
    /// option if you do not care for history, as it throws away all
    /// data that you no longer need.
    pub async fn squash(&self) -> io::Result<StoreLayer> {
        // TODO check if we already committed
        let new_builder = self.store.create_base_layer().await?;
        self.triples().par_bridge().for_each(|t| {
            let st = self.id_triple_to_string(&t).unwrap();
            new_builder.add_string_triple(st).unwrap()
        });

        new_builder.commit().await
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub async fn rollup(&self) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.rollup(layer).await?;
        Ok(())
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all ancestors up to (but not including) the given ancestor.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub async fn rollup_upto(&self, upto: &StoreLayer) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.rollup_upto(layer, upto.name()).await?;
        Ok(())
    }

    /// Like rollup_upto, rolls up upto the given layer. However, if
    /// this layer is a rollup layer, this will roll up upto that
    /// rollup.
    pub async fn imprecise_rollup_upto(&self, upto: &StoreLayer) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name()).await?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.imprecise_rollup_upto(layer, upto.name()).await?;
        Ok(())
    }

    /// Returns a future that yields true if this triple has been added in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        self.store
            .layer_store
            .triple_addition_exists(self.layer.name(), subject, predicate, object)
    }

    /// Returns a future that yields true if this triple has been removed in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        self.store
            .layer_store
            .triple_removal_exists(self.layer.name(), subject, predicate, object)
    }

    /// Returns a future that yields an iterator over all layer additions.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let fut = self.store.layer_store.triple_additions(self.layer.name());
        Box::pin(async move {
            let result = fut.await?;

            Ok(Box::new(result) as Box<dyn Iterator<Item = _> + Send>)
        })
    }

    /// Returns a future that yields an iterator over all layer removals.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let fut = self.store.layer_store.triple_removals(self.layer.name());
        Box::pin(async move {
            let result = fut.await?;

            Ok(Box::new(result) as Box<dyn Iterator<Item = _> + Send>)
        })
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_s(
        &self,
        subject: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_additions_s(self.layer.name(), subject)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_s(
        &self,
        subject: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_removals_s(self.layer.name(), subject)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_additions_sp(self.layer.name(), subject, predicate)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_removals_sp(self.layer.name(), subject, predicate)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_p(
        &self,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_additions_p(self.layer.name(), predicate)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_p(
        &self,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_removals_p(self.layer.name(), predicate)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_o(
        &self,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_additions_o(self.layer.name(), object)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_o(
        &self,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        self.store
            .layer_store
            .triple_removals_o(self.layer.name(), object)
    }

    /// Returns a future that yields the amount of triples that this layer adds.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_layer_addition_count(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send>> {
        self.store
            .layer_store
            .triple_layer_addition_count(self.layer.name())
    }

    /// Returns a future that yields the amount of triples that this layer removes.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_layer_removal_count(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send>> {
        self.store
            .layer_store
            .triple_layer_removal_count(self.layer.name())
    }

    /// Returns a future that yields a vector of layer stack names describing the history of this layer, starting from the base layer up to and including the name of this layer itself.
    pub fn retrieve_layer_stack_names(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let name = self.name();
        self.store.layer_store.retrieve_layer_stack_names(name)
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

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.layer.triple_exists(subject, predicate, object)
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples()
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_s(subject)
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_sp(subject, predicate)
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_p(predicate)
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_o(object)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
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
#[derive(Clone)]
pub struct NamedGraph {
    label: String,
    store: Store,
}

impl NamedGraph {
    fn new(label: String, store: Store) -> Self {
        NamedGraph { label, store }
    }

    /// Returns the label name itself.
    pub fn name(&self) -> &str {
        &self.label
    }

    /// Returns the layer this database points at, as well as the label version.
    pub async fn head_version(&self) -> io::Result<(Option<StoreLayer>, u64)> {
        let new_label = self.store.label_store.get_label(&self.label).await?;

        match new_label {
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "database not found",
            )),
            Some(new_label) => {
                let layer = match new_label.layer {
                    None => None,
                    Some(layer) => {
                        let layer = self.store.layer_store.get_layer(layer).await?;
                        match layer {
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "layer not found even though it is pointed at by a label",
                                ))
                            }
                            Some(layer) => Some(StoreLayer::wrap(layer, self.store.clone())),
                        }
                    }
                };
                Ok((layer, new_label.version))
            }
        }
    }

    /// Returns the layer this database points at.
    pub async fn head(&self) -> io::Result<Option<StoreLayer>> {
        Ok(self.head_version().await?.0)
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise.
    pub async fn set_head(&self, layer: &StoreLayer) -> io::Result<bool> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label).await?;
        if label.is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "label not found"));
        }
        let label = label.unwrap();

        let set_is_ok = match label.layer {
            None => true,
            Some(retrieved_layer_name) => {
                self.store
                    .layer_store
                    .layer_is_ancestor_of(layer_name, retrieved_layer_name)
                    .await?
            }
        };

        if set_is_ok {
            self.store.label_store.set_label(&label, layer_name).await?;
        }

        Ok(set_is_ok)
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor.
    pub async fn force_set_head(&self, layer: &StoreLayer) -> io::Result<()> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label).await?;
        match label {
            None => Err(io::Error::new(io::ErrorKind::NotFound, "label not found")),
            Some(label) => {
                self.store.label_store.set_label(&label, layer_name).await?;

                Ok(())
            }
        }
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor. Also checks given version, and if it doesn't match, the update won't happen and false will be returned.
    pub async fn force_set_head_version(
        &self,
        layer: &StoreLayer,
        version: u64,
    ) -> io::Result<bool> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label).await?;
        match label {
            None => Err(io::Error::new(io::ErrorKind::NotFound, "label not found")),
            Some(label) => {
                if label.version != version {
                    Ok(false)
                } else {
                    self.store.label_store.set_label(&label, layer_name).await?;

                    Ok(true)
                }
            }
        }
    }

    pub async fn delete(&self) -> io::Result<()> {
        self.store.delete(&self.label).await.map(|_| ())
    }
}

impl Store {
    /// Create a new store from the given label and layer store.
    pub fn new<Labels: 'static + LabelStore, Layers: 'static + LayerStore>(
        label_store: Labels,
        layer_store: Layers,
    ) -> Store {
        Store {
            label_store: Arc::new(label_store),
            layer_store: Arc::new(layer_store),
        }
    }

    /// Create a new database with the given name.
    ///
    /// If the database already exists, this will return an error.
    pub async fn create(&self, label: &str) -> io::Result<NamedGraph> {
        let label = self.label_store.create_label(label).await?;
        Ok(NamedGraph::new(label.name, self.clone()))
    }

    /// Open an existing database with the given name, or None if it does not exist.
    pub async fn open(&self, label: &str) -> io::Result<Option<NamedGraph>> {
        let label = self.label_store.get_label(label).await?;
        Ok(label.map(|label| NamedGraph::new(label.name, self.clone())))
    }

    /// Delete an existing database with the given name. Returns true if this database was deleted
    /// and false otherwise.
    pub async fn delete(&self, label: &str) -> io::Result<bool> {
        self.label_store.delete_label(label).await
    }

    /// Retrieve a layer with the given name from the layer store this Store was initialized with.
    pub async fn get_layer_from_id(&self, layer: [u32; 5]) -> io::Result<Option<StoreLayer>> {
        let layer = self.layer_store.get_layer(layer).await?;
        Ok(layer.map(|layer| StoreLayer::wrap(layer, self.clone())))
    }

    /// Create a base layer builder, unattached to any database label.
    ///
    /// After having committed it, use `set_head` on a `NamedGraph` to attach it.
    pub async fn create_base_layer(&self) -> io::Result<StoreLayerBuilder> {
        StoreLayerBuilder::new(self.clone()).await
    }

    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    pub async fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        self.layer_store.export_layers(layer_ids).await
    }

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    pub async fn import_layers<'a>(
        &'a self,
        pack: &'a [u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        self.layer_store.import_layers(pack, layer_ids).await
    }
}

/// Open a store that is entirely in memory.
///
/// This is useful for testing purposes, or if the database is only going to be used for caching purposes.
pub fn open_memory_store() -> Store {
    Store::new(
        MemoryLabelStore::new(),
        CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new()),
    )
}

/// Open a store that stores its data in the given directory.
pub fn open_directory_store<P: Into<PathBuf>>(path: P) -> io::Result<Store> {
    let path = path.into();

    // Return an error if the path cannot be used. `std::fs::metadata` checks for access permission
    // and existence of the path. Then, we also confirm that it is a directory.
    if !std::fs::metadata(&path)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path '{:?}' is not a directory", path),
        ));
    }

    Ok(Store::new(
        DirectoryLabelStore::new(path.clone()),
        CachedLayerStore::new(
            DirectoryLayerStore::new(path),
            LockingHashMapLayerCache::new(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, NamedTempFile};

    async fn create_and_manipulate_database(store: Store) {
        let database = store.create("foodb").await.unwrap();

        let head = database.head().await.unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().await.unwrap();
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();
        assert!(database.set_head(&layer).await.unwrap());

        builder = layer.open_write().await.unwrap();
        builder
            .add_string_triple(StringTriple::new_value("pig", "says", "oink"))
            .unwrap();

        let layer2 = builder.commit().await.unwrap();
        assert!(database.set_head(&layer2).await.unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().await.unwrap().unwrap();

        assert_eq!(layer2_name, layer.name());
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
    }

    #[tokio::test]
    async fn create_and_manipulate_memory_database() {
        let store = open_memory_store();

        create_and_manipulate_database(store).await;
    }

    #[tokio::test]
    async fn open_directory_store_dir_not_found() {
        let dir = tempdir().unwrap();
        let path = dir.path().clone();
        std::fs::remove_dir(dir.path()).unwrap();
        let err = open_directory_store(path.clone()).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn open_directory_store_dir_is_file() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();
        let err = open_directory_store(path).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            format!("path '{:?}' is not a directory", path)
        );
    }

    #[tokio::test]
    async fn create_and_manipulate_directory_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();

        create_and_manipulate_database(store).await;
    }

    #[tokio::test]
    async fn create_layer_and_retrieve_it_by_id() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();

        let id = layer.name();

        let layer2 = store.get_layer_from_id(id).await.unwrap().unwrap();

        assert!(layer2.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn commit_builder_makes_builder_committed() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();

        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        assert!(!builder.committed());

        builder.commit_no_load().await.unwrap();

        assert!(builder.committed());
    }

    #[tokio::test]
    async fn hard_reset() {
        let store = open_memory_store();
        let database = store.create("foodb").await.unwrap();

        let builder1 = store.create_base_layer().await.unwrap();
        builder1
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer1 = builder1.commit().await.unwrap();

        assert!(database.set_head(&layer1).await.unwrap());

        let builder2 = store.create_base_layer().await.unwrap();
        builder2
            .add_string_triple(StringTriple::new_value("duck", "says", "quack"))
            .unwrap();

        let layer2 = builder2.commit().await.unwrap();

        database.force_set_head(&layer2).await.unwrap();

        let new_layer = database.head().await.unwrap().unwrap();

        assert!(new_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
        assert!(!new_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn create_two_layers_and_squash() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();

        let builder2 = layer.open_write().await.unwrap();

        builder2
            .add_string_triple(StringTriple::new_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = builder2.commit().await.unwrap();

        let new = layer2.squash().await.unwrap();

        assert!(new.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(new.string_triple_exists(&StringTriple::new_value("dog", "says", "woof")));
        assert!(new.parent().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn apply_a_base_delta() {
        let store = open_memory_store();
        let builder = store.create_base_layer().await.unwrap();

        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().await.unwrap();

        let builder2 = layer.open_write().await.unwrap();

        builder2
            .add_string_triple(StringTriple::new_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = builder2.commit().await.unwrap();

        let delta_builder_1 = store.create_base_layer().await.unwrap();

        delta_builder_1
            .add_string_triple(StringTriple::new_value("dog", "says", "woof"))
            .unwrap();
        delta_builder_1
            .add_string_triple(StringTriple::new_value("cat", "says", "meow"))
            .unwrap();

        let delta_1 = delta_builder_1.commit().await.unwrap();

        let delta_builder_2 = delta_1.open_write().await.unwrap();

        delta_builder_2
            .add_string_triple(StringTriple::new_value("crow", "says", "caw"))
            .unwrap();
        delta_builder_2
            .remove_string_triple(StringTriple::new_value("cat", "says", "meow"))
            .unwrap();

        let delta = delta_builder_2.commit().await.unwrap();

        let rebase_builder = layer2.open_write().await.unwrap();

        let _ = rebase_builder.apply_delta(&delta).await.unwrap();

        let rebase_layer = rebase_builder.commit().await.unwrap();

        assert!(rebase_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(rebase_layer.string_triple_exists(&StringTriple::new_value("crow", "says", "caw")));
        assert!(rebase_layer.string_triple_exists(&StringTriple::new_value("dog", "says", "woof")));
        assert!(!rebase_layer.string_triple_exists(&StringTriple::new_value("cat", "says", "meow")));
    }

    async fn cached_layer_name_does_not_change_after_rollup(store: Store) {
        let builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();
        let x = builder.commit().await.unwrap();
        let builder = x.open_write().await.unwrap();
        let child_name = builder.name();
        builder.commit().await.unwrap();

        let unrolled_layer = store.get_layer_from_id(child_name).await.unwrap().unwrap();
        let unrolled_name = unrolled_layer.name();
        let unrolled_parent_name = unrolled_layer.parent_name().unwrap();
        assert_eq!(child_name, unrolled_name);
        assert_eq!(base_name, unrolled_parent_name);

        unrolled_layer.rollup().await.unwrap();
        let rolled_layer = store.get_layer_from_id(child_name).await.unwrap().unwrap();
        let rolled_name = rolled_layer.name();
        let rolled_parent_name = rolled_layer.parent_name().unwrap();
        assert_eq!(child_name, rolled_name);
        assert_eq!(base_name, rolled_parent_name);

        rolled_layer.rollup().await.unwrap();
        let rolled_layer2 = store.get_layer_from_id(child_name).await.unwrap().unwrap();
        let rolled_name2 = rolled_layer2.name();
        let rolled_parent_name2 = rolled_layer2.parent_name().unwrap();
        assert_eq!(child_name, rolled_name2);
        assert_eq!(base_name, rolled_parent_name2);
    }

    #[tokio::test]
    async fn mem_cached_layer_name_does_not_change_after_rollup() {
        let store = open_memory_store();

        cached_layer_name_does_not_change_after_rollup(store).await
    }

    #[tokio::test]
    async fn dir_cached_layer_name_does_not_change_after_rollup() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();

        cached_layer_name_does_not_change_after_rollup(store).await
    }

    async fn cached_layer_name_does_not_change_after_rollup_upto(store: Store) {
        let builder = store.create_base_layer().await.unwrap();
        let _base_name = builder.name();
        let base_layer = builder.commit().await.unwrap();
        let builder = base_layer.open_write().await.unwrap();
        let child_name = builder.name();
        let x = builder.commit().await.unwrap();
        let builder = x.open_write().await.unwrap();
        let child_name2 = builder.name();
        builder.commit().await.unwrap();

        let unrolled_layer = store.get_layer_from_id(child_name2).await.unwrap().unwrap();
        let unrolled_name = unrolled_layer.name();
        let unrolled_parent_name = unrolled_layer.parent_name().unwrap();
        assert_eq!(child_name2, unrolled_name);
        assert_eq!(child_name, unrolled_parent_name);

        unrolled_layer.rollup_upto(&base_layer).await.unwrap();
        let rolled_layer = store.get_layer_from_id(child_name2).await.unwrap().unwrap();
        let rolled_name = rolled_layer.name();
        let rolled_parent_name = rolled_layer.parent_name().unwrap();
        assert_eq!(child_name2, rolled_name);
        assert_eq!(child_name, rolled_parent_name);

        rolled_layer.rollup_upto(&base_layer).await.unwrap();
        let rolled_layer2 = store.get_layer_from_id(child_name2).await.unwrap().unwrap();
        let rolled_name2 = rolled_layer2.name();
        let rolled_parent_name2 = rolled_layer2.parent_name().unwrap();
        assert_eq!(child_name2, rolled_name2);
        assert_eq!(child_name, rolled_parent_name2);
    }

    #[tokio::test]
    async fn mem_cached_layer_name_does_not_change_after_rollup_upto() {
        let store = open_memory_store();
        cached_layer_name_does_not_change_after_rollup_upto(store).await
    }

    #[tokio::test]
    async fn dir_cached_layer_name_does_not_change_after_rollup_upto() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        cached_layer_name_does_not_change_after_rollup_upto(store).await
    }

    #[tokio::test]
    async fn force_update_with_matching_0_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let graph = store.create("foo").await.unwrap();
        let (layer, version) = graph.head_version().await.unwrap();
        assert!(layer.is_none());
        assert_eq!(0, version);

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();

        assert!(graph.force_set_head_version(&layer, 0).await.unwrap());
    }

    #[tokio::test]
    async fn force_update_with_mismatching_0_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let graph = store.create("foo").await.unwrap();
        let (layer, version) = graph.head_version().await.unwrap();
        assert!(layer.is_none());
        assert_eq!(0, version);

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();

        assert!(!graph.force_set_head_version(&layer, 3).await.unwrap());
    }

    #[tokio::test]
    async fn force_update_with_matching_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let graph = store.create("foo").await.unwrap();

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();
        assert!(graph.set_head(&layer).await.unwrap());

        let (_, version) = graph.head_version().await.unwrap();
        assert_eq!(1, version);

        let builder2 = store.create_base_layer().await.unwrap();
        let layer2 = builder2.commit().await.unwrap();

        assert!(graph.force_set_head_version(&layer2, 1).await.unwrap());
    }

    #[tokio::test]
    async fn force_update_with_mismatched_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let graph = store.create("foo").await.unwrap();

        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();
        assert!(graph.set_head(&layer).await.unwrap());

        let (_, version) = graph.head_version().await.unwrap();
        assert_eq!(1, version);

        let builder2 = store.create_base_layer().await.unwrap();
        let layer2 = builder2.commit().await.unwrap();

        assert!(!graph.force_set_head_version(&layer2, 0).await.unwrap());
    }

    #[tokio::test]
    async fn delete_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let _ = store.create("foo").await.unwrap();
        assert!(store.delete("foo").await.unwrap());
        assert!(store.open("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        assert!(!store.delete("foo").await.unwrap());
    }

    #[tokio::test]
    async fn delete_graph() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let graph = store.create("foo").await.unwrap();
        assert!(store.open("foo").await.unwrap().is_some());
        graph.delete().await.unwrap();
        assert!(store.open("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recreate_graph() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path()).unwrap();
        let graph = store.create("foo").await.unwrap();
        let builder = store.create_base_layer().await.unwrap();
        let layer = builder.commit().await.unwrap();
        graph.set_head(&layer).await.unwrap();
        assert!(graph.head().await.unwrap().is_some());
        graph.delete().await.unwrap();
        store.create("foo").await.unwrap();
        assert!(graph.head().await.unwrap().is_none());
    }
}
