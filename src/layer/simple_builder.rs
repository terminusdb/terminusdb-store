//! builder frontend for constructing new layers
//!
//! `base` and `child` contain their own layer builders, but these are
//! not very easy to use. They require one to first insert all new
//! dictionary entries in sorted order, and then all numerical triple
//! additions/removals in sorted order
//!
//! The layer builder implemented here will instead take triples in
//! any format (numerical, string, or a mixture), store them in
//! memory, then does the required sorting and id conversion on
//! commit.
use super::internal::*;
use super::layer::*;
use crate::storage::*;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::Future;

use bitvec::prelude::*;

/// A layer builder trait with no generic typing.
///
/// Lack of generic types allows layer builders with different storage
/// backends to be handled by trait objects of this type.
pub trait LayerBuilder: Send + Sync {
    /// Returns the name of the layer being built
    fn name(&self) -> [u32; 5];
    /// Return the parent if it exists
    fn parent(&self) -> Option<Arc<dyn Layer>>;
    /// Add a string triple
    fn add_value_triple(&mut self, triple: ValueTriple);
    /// Add an id triple
    fn add_id_triple(&mut self, triple: IdTriple);
    /// Remove a string triple
    fn remove_value_triple(&mut self, triple: ValueTriple);
    /// Remove an id triple
    fn remove_id_triple(&mut self, triple: IdTriple);
    /// Commit the layer to storage
    fn commit(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
    /// Commit a boxed layer to storage
    fn commit_boxed(self: Box<Self>) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
}

/// A layer builder
///
/// `SimpleLayerBuilder` provides methods for adding and removing
/// triples, and for committing the layer builder to storage.
#[derive(Clone)]
pub struct SimpleLayerBuilder<F: 'static + FileLoad + FileStore + Clone> {
    name: [u32; 5],
    parent: Option<Arc<dyn Layer>>,
    files: LayerFiles<F>,
    id_additions: Vec<IdTriple>,
    id_removals: Vec<IdTriple>,

    nodes_values_map: HashMap<ObjectType, u64>,
    predicates_map: HashMap<String, u64>,
    nodes_values_map_count: usize,
    predicates_map_count: usize,
    node_count: usize,
    pred_count: usize,
    val_count: usize,
}

impl<F: 'static + FileLoad + FileStore + Clone> SimpleLayerBuilder<F> {
    /// Construct a layer builder for a base layer
    pub fn new(name: [u32; 5], files: BaseLayerFiles<F>) -> Self {
        Self {
            name,
            parent: None,
            files: LayerFiles::Base(files),
            id_additions: Vec::with_capacity(0),
            id_removals: Vec::with_capacity(0),

            nodes_values_map: HashMap::new(),
            predicates_map: HashMap::new(),
            nodes_values_map_count: 0,
            predicates_map_count: 0,
            node_count: 0,
            pred_count: 0,
            val_count: 0,
        }
    }

    /// Construct a layer builder for a child layer
    pub fn from_parent(name: [u32; 5], parent: Arc<dyn Layer>, files: ChildLayerFiles<F>) -> Self {
        let nodes_values_map_count = parent.node_and_value_count();
        let predicates_map_count = parent.predicate_count();
        Self {
            name,
            parent: Some(parent),
            files: LayerFiles::Child(files),
            id_additions: Vec::new(),
            id_removals: Vec::new(),

            nodes_values_map: HashMap::new(),
            predicates_map: HashMap::new(),
            nodes_values_map_count,
            predicates_map_count,
            node_count: 0,
            pred_count: 0,
            val_count: 0,
        }
    }

    fn calculate_triple(&mut self, triple: ValueTriple) -> IdTriple {
        let subject = ObjectType::Node(triple.subject);
        let predicate = triple.predicate;
        let object = triple.object;
        let subject_id = if let Some(n) = self.nodes_values_map.get(&subject) {
            *n
        } else {
            let node_id = if let Some(node_id) = self
                .parent
                .as_ref()
                .and_then(|p| p.subject_id(subject.node_ref().unwrap()))
            {
                node_id
            } else {
                self.nodes_values_map_count += 1;
                self.node_count += 1;
                self.nodes_values_map_count as u64
            };
            self.nodes_values_map.insert(subject, node_id);

            node_id
        };

        let predicate_id = if let Some(p) = self.predicates_map.get(&predicate) {
            *p
        } else {
            let predicate_id = if let Some(predicate_id) = self
                .parent
                .as_ref()
                .and_then(|p| p.predicate_id(&predicate))
            {
                predicate_id
            } else {
                self.predicates_map_count += 1;
                self.pred_count += 1;
                self.predicates_map_count as u64
            };
            self.predicates_map.insert(predicate, predicate_id);

            predicate_id
        };
        let object_id = if let Some(o) = self.nodes_values_map.get(&object) {
            *o
        } else {
            match object {
                ObjectType::Node(n) => {
                    let node_id = if let Some(node_id) = self
                        .parent
                        .as_ref()
                        .and_then(|p| p.object_node_id(n.as_str()))
                    {
                        node_id
                    } else {
                        self.nodes_values_map_count += 1;
                        self.node_count += 1;
                        self.nodes_values_map_count as u64
                    };
                    self.nodes_values_map.insert(ObjectType::Node(n), node_id);

                    node_id
                }
                ObjectType::Value(v) => {
                    let value_id = if let Some(value_id) =
                        self.parent.as_ref().and_then(|p| p.object_value_id(&v))
                    {
                        value_id
                    } else {
                        self.nodes_values_map_count += 1;
                        self.val_count += 1;
                        self.nodes_values_map_count as u64
                    };
                    self.nodes_values_map.insert(ObjectType::Value(v), value_id);

                    value_id
                }
            }
        };

        IdTriple::new(subject_id, predicate_id, object_id)
    }
}

impl<F: 'static + FileLoad + FileStore + Clone> LayerBuilder for SimpleLayerBuilder<F> {
    fn name(&self) -> [u32; 5] {
        self.name
    }

    fn parent(&self) -> Option<Arc<dyn Layer>> {
        self.parent.clone()
    }

    fn add_value_triple(&mut self, addition: ValueTriple) {
        let triple = self.calculate_triple(addition);
        self.id_additions.push(triple);
    }

    fn add_id_triple(&mut self, triple: IdTriple) {
        self.id_additions.push(triple);
    }

    fn remove_value_triple(&mut self, removal: ValueTriple) {
        let triple = self.calculate_triple(removal);
        self.id_removals.push(triple);
    }

    fn remove_id_triple(&mut self, triple: IdTriple) {
        self.id_removals.push(triple);
    }

    fn commit(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let SimpleLayerBuilder {
            name: _,
            parent,
            files,
            mut id_additions,
            mut id_removals,

            nodes_values_map,
            predicates_map,
            nodes_values_map_count: _,
            predicates_map_count: _,
            node_count,
            pred_count,
            val_count,
        } = self;
        let parent_node_value_offset = parent
            .as_ref()
            .map(|p| p.node_and_value_count())
            .unwrap_or(0);
        let parent_predicate_offset = parent.as_ref().map(|p| p.predicate_count()).unwrap_or(0);
        // time to deduplicate!

        id_additions.sort();
        id_additions.dedup();
        id_additions.shrink_to_fit();
        id_removals.sort();
        id_removals.dedup();
        id_removals.shrink_to_fit();

        // we now need to figure out noops.
        let mut additions_it = id_additions.iter_mut().peekable();
        let mut removals_it = id_removals.iter_mut().peekable();
        loop {
            let addition = additions_it.peek();
            let removal = removals_it.peek();

            // advance those iterators in order until we reach the end
            if removal.is_none() {
                break;
            }
            if addition.is_none() {
                // loop over the remaining removals to nullify everything that should be a noop due to being out of range
                while let Some(removal) = removals_it.next() {
                    if removal.subject > parent_node_value_offset as u64
                        || removal.predicate > parent_predicate_offset as u64
                        || removal.object > parent_node_value_offset as u64
                    {
                        *removal = IdTriple::new(0, 0, 0);
                    }
                }
                break;
            }

            if addition < removal {
                additions_it.next();
            } else if addition > removal {
                let removal = removals_it.next().unwrap();
                // we need to clear a potential noop
                if removal.subject > parent_node_value_offset as u64
                    || removal.predicate > parent_predicate_offset as u64
                    || removal.object > parent_node_value_offset as u64
                {
                    *removal = IdTriple::new(0, 0, 0);
                }
            } else {
                // same triple! make it zeroes to express a no-op without having to shift around triples
                let addition = additions_it.next().unwrap();
                let removal = removals_it.next().unwrap();
                *addition = IdTriple::new(0, 0, 0);
                *removal = IdTriple::new(0, 0, 0);
            }
        }

        // some dict entries might now be unused. We need to do an existence check.
        let mut node_value_existences = bitvec![0;node_count + val_count];
        let mut predicate_existences = bitvec![0;pred_count];
        for triple in id_additions.iter().chain(id_removals.iter()) {
            if triple.subject > parent_node_value_offset as u64 {
                node_value_existences
                    .set(triple.subject as usize - parent_node_value_offset - 1, true);
            }
            if triple.predicate > parent_predicate_offset as u64 {
                predicate_existences.set(
                    triple.predicate as usize - parent_predicate_offset - 1,
                    true,
                );
            }
            if triple.object > parent_node_value_offset as u64 {
                node_value_existences
                    .set(triple.object as usize - parent_node_value_offset - 1, true);
            }
        }

        // time to collect our dictionaries.
        let mut nodes = Vec::with_capacity(node_count);
        let mut predicates = Vec::with_capacity(pred_count);
        let mut values = Vec::with_capacity(val_count);

        for (entry, id) in nodes_values_map.into_iter() {
            if id <= parent_node_value_offset as u64 {
                // we don't care about these ids. they are already correct in the triples
                continue;
            }
            if !node_value_existences[id as usize - parent_node_value_offset - 1] {
                // while originally collected, in the end this entry was unused
                continue;
            }
            match entry {
                ObjectType::Node(n) => nodes.push((n, id)),
                ObjectType::Value(v) => values.push((v, id)),
            }
        }
        for (entry, id) in predicates_map.into_iter() {
            if id <= parent_predicate_offset as u64 {
                // we don't care about these ids. they are already correct in the triples
                continue;
            }
            if !predicate_existences[id as usize - parent_predicate_offset - 1] {
                // while originally collected, in the end this entry was unused
                continue;
            }

            predicates.push((entry, id));
        }

        nodes.sort();
        predicates.sort();
        values.sort();

        // build up conversion maps for converting the id triples to their final id
        let mut node_value_id_map = vec![0_u64; node_count + val_count];
        let mut predicate_id_map = vec![0_u64; pred_count];
        for (new_id, (_, old_id)) in nodes.iter().enumerate() {
            let mapped_old_id = *old_id as usize - parent_node_value_offset - 1;
            node_value_id_map[mapped_old_id] = (new_id + parent_node_value_offset + 1) as u64;
        }
        for (new_id, (_, old_id)) in values.iter().enumerate() {
            let mapped_old_id = *old_id as usize - parent_node_value_offset - 1;
            node_value_id_map[mapped_old_id] =
                (new_id + parent_node_value_offset + nodes.len() + 1) as u64;
        }
        for (new_id, (_, old_id)) in predicates.iter().enumerate() {
            let mapped_old_id = *old_id as usize - parent_predicate_offset - 1;
            predicate_id_map[mapped_old_id] = (new_id + parent_predicate_offset + 1) as u64;
        }

        // now we have to map all the additions and removals
        for triple in id_additions.iter_mut().chain(id_removals.iter_mut()) {
            if triple.subject > parent_node_value_offset as u64 {
                let mapped_id = triple.subject as usize - parent_node_value_offset - 1;
                triple.subject = node_value_id_map[mapped_id];
            }
            if triple.predicate > parent_predicate_offset as u64 {
                let mapped_id = triple.predicate as usize - parent_predicate_offset - 1;
                triple.predicate = predicate_id_map[mapped_id];
            }
            if triple.object > parent_node_value_offset as u64 {
                let mapped_id = triple.object as usize - parent_node_value_offset - 1;
                triple.object = node_value_id_map[mapped_id];
            }
        }
        // and resort them
        id_additions.sort();
        id_removals.sort();

        // great! everything is now in order. Let's stuff it into an actual builder
        Box::pin(async {
            match parent {
                Some(parent) => {
                    let files = files.into_child();
                    let mut builder =
                        ChildLayerFileBuilder::from_files(parent.clone(), &files).await?;

                    builder.add_nodes(nodes.into_iter().map(|x| x.0));
                    builder.add_predicates(predicates.into_iter().map(|x| x.0));
                    builder.add_values(values.into_iter().map(|x| x.0));

                    let mut builder = builder.into_phase2().await?;

                    builder.add_id_triples(id_additions).await?;
                    builder.remove_id_triples(id_removals).await?;

                    builder.finalize().await
                }
                None => {
                    // TODO almost same as above, should be more generic
                    let files = files.into_base();
                    let mut builder = BaseLayerFileBuilder::from_files(&files).await?;

                    builder.add_nodes(nodes.into_iter().map(|x| x.0));
                    builder.add_predicates(predicates.into_iter().map(|x| x.0));
                    builder.add_values(values.into_iter().map(|x| x.0));

                    let mut builder = builder.into_phase2().await?;

                    builder.add_id_triples(id_additions).await?;

                    builder.finalize().await
                }
            }
        })
    }

    fn commit_boxed(self: Box<Self>) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let builder = *self;
        builder.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::internal::InternalLayer;
    use crate::storage::memory::*;
    use crate::structure::TdbDataType;

    fn new_base_files() -> BaseLayerFiles<MemoryBackedStore> {
        // TODO inline
        base_layer_memory_files()
    }

    fn new_child_files() -> ChildLayerFiles<MemoryBackedStore> {
        // TODO inline
        child_layer_memory_files()
    }

    async fn example_base_layer() -> Arc<InternalLayer> {
        let name = [1, 2, 3, 4, 5];
        let files = new_base_files();
        let mut builder = SimpleLayerBuilder::new(name, files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit().await.unwrap();

        let layer = BaseLayer::load_from_files(name, &files).await.unwrap();
        Arc::new(layer.into())
    }

    #[tokio::test]
    async fn simple_base_layer_construction() {
        let layer = example_base_layer().await;

        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn simple_child_layer_construction() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("horse", "says", "neigh"));
        builder.add_value_triple(ValueTriple::new_node("horse", "likes", "cow"));
        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        let child_layer = Arc::new(
            async {
                builder.commit().await?;

                ChildLayer::load_from_files(name, base_layer, &files).await
            }
            .await
            .unwrap(),
        );

        assert!(child_layer
            .value_triple_exists(&ValueTriple::new_string_value("horse", "says", "neigh")));
        assert!(child_layer.value_triple_exists(&ValueTriple::new_node("horse", "likes", "cow")));
        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(!child_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn multi_level_layers() {
        let base_layer = example_base_layer().await;
        let name2 = [0, 0, 0, 0, 0];
        let files2 = new_child_files();
        let mut builder =
            SimpleLayerBuilder::from_parent(name2, base_layer.clone(), files2.clone());

        builder.add_value_triple(ValueTriple::new_string_value("horse", "says", "neigh"));
        builder.add_value_triple(ValueTriple::new_node("horse", "likes", "cow"));
        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit().await.unwrap();
        let layer2: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name2, base_layer, &files2)
                .await
                .unwrap()
                .into(),
        );

        let name3 = [0, 0, 0, 0, 1];
        let files3 = new_child_files();
        builder = SimpleLayerBuilder::from_parent(name3, layer2.clone(), files3.clone());
        builder.remove_value_triple(ValueTriple::new_node("horse", "likes", "cow"));
        builder.add_value_triple(ValueTriple::new_node("horse", "likes", "pig"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit().await.unwrap();
        let layer3: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name3, layer2, &files3)
                .await
                .unwrap()
                .into(),
        );

        let name4 = [0, 0, 0, 0, 1];
        let files4 = new_child_files();
        builder = SimpleLayerBuilder::from_parent(name4, layer3.clone(), files4.clone());
        builder.remove_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "horse"));
        builder.commit().await.unwrap();
        let layer4: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name4, layer3, &files4)
                .await
                .unwrap()
                .into(),
        );

        assert!(layer4.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer4.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
        assert!(
            layer4.value_triple_exists(&ValueTriple::new_string_value("horse", "says", "neigh"))
        );
        assert!(layer4.value_triple_exists(&ValueTriple::new_node("horse", "likes", "pig")));
        assert!(layer4.value_triple_exists(&ValueTriple::new_node("cow", "likes", "horse")));

        assert!(!layer4.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(!layer4.value_triple_exists(&ValueTriple::new_node("horse", "likes", "cow")));
    }

    #[tokio::test]
    async fn remove_and_add_same_triple_on_base_layer_is_noop() {
        let files = new_base_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::new(name, files.clone());

        builder.remove_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));
        builder.add_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let base_layer: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files(name, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            !base_layer.value_triple_exists(&ValueTriple::new_string_value("crow", "says", "caw"))
        );
    }

    #[tokio::test]
    async fn add_and_remove_same_triple_on_base_layer_is_noop() {
        let files = new_base_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::new(name, files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));
        builder.remove_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let base_layer: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files(name, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            !base_layer.value_triple_exists(&ValueTriple::new_string_value("crow", "says", "caw"))
        );
    }

    #[tokio::test]
    async fn remove_and_add_same_existing_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[tokio::test]
    async fn add_and_remove_same_existing_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[tokio::test]
    async fn remove_and_add_same_nonexisting_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));
        builder.add_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            !child_layer.value_triple_exists(&ValueTriple::new_string_value("crow", "says", "caw"))
        );
    }

    #[tokio::test]
    async fn add_and_remove_same_nonexisting_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));
        builder.remove_value_triple(ValueTriple::new_string_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            !child_layer.value_triple_exists(&ValueTriple::new_string_value("crow", "says", "caw"))
        );
    }

    #[tokio::test]
    async fn remove_and_add_same_triple_by_id_and_string_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer
            .object_value_id(&String::make_entry(&"moo"))
            .unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_id_triple(IdTriple::new(node_id, predicate_id, value_id));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[tokio::test]
    async fn remove_and_add_same_triple_by_string_and_id_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer
            .object_value_id(&String::make_entry(&"moo"))
            .unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_id_triple(IdTriple::new(node_id, predicate_id, value_id));
        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[tokio::test]
    async fn add_and_remove_same_triple_by_id_and_string_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer
            .object_value_id(&String::make_entry(&"moo"))
            .unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.remove_id_triple(IdTriple::new(node_id, predicate_id, value_id));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[tokio::test]
    async fn add_and_remove_same_triple_by_string_and_id_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer
            .object_value_id(&String::make_entry(&"moo"))
            .unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_id_triple(IdTriple::new(node_id, predicate_id, value_id));
        builder.remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(
            child_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }
}
