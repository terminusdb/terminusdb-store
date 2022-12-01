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
use std::collections::{HashMap, HashSet};
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::Future;

use rayon::prelude::*;

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
    fn add_string_triple(&mut self, triple: StringTriple);
    /// Add an id triple
    fn add_id_triple(&mut self, triple: IdTriple);
    /// Remove a string triple
    fn remove_string_triple(&mut self, triple: StringTriple);
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
    additions: Vec<StringTriple>,
    id_additions: Vec<IdTriple>,
    removals: Vec<StringTriple>,
    id_removals: Vec<IdTriple>,
}

impl<F: 'static + FileLoad + FileStore + Clone> SimpleLayerBuilder<F> {
    /// Construct a layer builder for a base layer
    pub fn new(name: [u32; 5], files: BaseLayerFiles<F>) -> Self {
        eprintln!("Trying to make a new layer file");
        Self {
            name,
            parent: None,
            files: LayerFiles::Base(files),
            additions: Vec::new(),
            id_additions: Vec::with_capacity(0),
            removals: Vec::new(),
            id_removals: Vec::with_capacity(0),
        }
    }

    /// Construct a layer builder for a child layer
    pub fn from_parent(name: [u32; 5], parent: Arc<dyn Layer>, files: ChildLayerFiles<F>) -> Self {
        Self {
            name,
            parent: Some(parent),
            files: LayerFiles::Child(files),
            additions: Vec::new(),
            id_additions: Vec::new(),
            removals: Vec::new(),
            id_removals: Vec::new(),
        }
    }
}

impl<F: 'static + FileLoad + FileStore + Clone> LayerBuilder for SimpleLayerBuilder<F> {
    fn name(&self) -> [u32; 5] {
        self.name
    }

    fn parent(&self) -> Option<Arc<dyn Layer>> {
        self.parent.clone()
    }

    fn add_string_triple(&mut self, triple: StringTriple) {
        self.additions.push(triple);
    }

    fn add_id_triple(&mut self, triple: IdTriple) {
        self.id_additions.push(triple);
    }

    fn remove_string_triple(&mut self, triple: StringTriple) {
        self.removals.push(triple);
    }

    fn remove_id_triple(&mut self, triple: IdTriple) {
        self.id_removals.push(triple);
    }

    fn commit(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let SimpleLayerBuilder {
            name: _,
            parent,
            files,
            additions,
            id_additions,
            removals,
            id_removals,
        } = self;

        let (mut additions, mut removals) = rayon::join(
            || {
                let mut additions: Vec<_> = match parent.as_ref() {
                    None => additions
                        .into_iter()
                        .map(|triple| triple.to_unresolved())
                        .collect(),
                    Some(parent) => additions
                        .into_par_iter()
                        .map(move |triple| parent.string_triple_to_partially_resolved(triple))
                        .collect(),
                };

                additions.extend(id_additions.into_iter().map(|triple| triple.to_resolved()));
                additions.par_sort_unstable();
                additions.dedup();

                additions
            },
            || {
                let mut removals: Vec<_> = match parent.as_ref() {
                    None => removals
                        .into_iter()
                        .map(|triple| triple.to_unresolved())
                        .collect(),
                    Some(parent) => removals
                        .into_par_iter()
                        .map(move |triple| parent.string_triple_to_partially_resolved(triple))
                        .collect(),
                };

                removals.extend(id_removals.into_iter().map(|triple| triple.to_resolved()));
                removals.par_sort_unstable();
                removals.dedup();

                removals
            },
        );

        // there's now a sorted list of additions and a sorted list of
        // removals, all as resolved as they can possibly be at this
        // point.  In order to support no-ops (where you add and
        // remove the same triple in the same builder), we need to
        // cross off the instances that appear in both lists.
        // 'crossing off' is accomplished by setting the particular
        // triple to (0,0,0), which is understood by the rest of the
        // code to mean a no-op.

        zero_equivalents(&mut additions, &mut removals);

        // in addition, all removals that aren't resolved at this
        // point are actually no-ops.
        if parent.is_some() {
            removals
                .par_iter_mut()
                .for_each(|triple| triple.make_resolved_or_zero())
        }

        // collect all strings we don't yet know about
        let (unresolved_nodes, unresolved_predicates, unresolved_values) =
            collect_unresolved_strings(&additions);

        // time to build things
        Box::pin(async {
            match parent {
                Some(parent) => {
                    let files = files.into_child();
                    let mut builder =
                        ChildLayerFileBuilder::from_files(parent.clone(), &files).await?;

                    let node_ids = builder.add_nodes(unresolved_nodes.clone());
                    let predicate_ids = builder.add_predicates(unresolved_predicates.clone());
                    let value_ids = builder.add_values(unresolved_values.clone());

                    let mut builder = builder.into_phase2().await?;

                    let counts = parent.all_counts();
                    let parent_node_offset = counts.node_count as u64 + counts.value_count as u64;
                    let parent_predicate_offset = counts.predicate_count as u64;
                    let mut node_map = HashMap::new();
                    for (node, id) in unresolved_nodes.into_iter().zip(node_ids) {
                        node_map.insert(node, id + parent_node_offset);
                    }
                    let mut predicate_map = HashMap::new();
                    for (predicate, id) in unresolved_predicates.into_iter().zip(predicate_ids) {
                        predicate_map.insert(predicate, id + parent_predicate_offset);
                    }
                    let mut value_map = HashMap::new();
                    for (value, id) in unresolved_values.into_iter().zip(value_ids) {
                        value_map.insert(value, id + parent_node_offset + node_map.len() as u64);
                    }

                    let mut add_triples: Vec<_> = additions
                        .into_iter()
                        .map(|t| {
                            t.resolve_with(&node_map, &predicate_map, &value_map)
                                .expect("triple should have been resolvable")
                        })
                        .collect();
                    add_triples.par_sort_unstable();
                    let remove_triples: Vec<_> = removals
                        .into_iter()
                        .filter_map(|r| r.as_resolved())
                        .collect();

                    // TODO this should be in parallel
                    builder.add_id_triples(add_triples).await?;
                    builder.remove_id_triples(remove_triples).await?;
                    builder.finalize().await
                }
                None => {
                    // TODO almost same as above, should be more generic
                    let files = files.into_base();
                    let mut builder = BaseLayerFileBuilder::from_files(&files).await?;

                    let node_ids = builder.add_nodes(unresolved_nodes.clone());
                    let predicate_ids = builder.add_predicates(unresolved_predicates.clone());
                    let value_ids = builder.add_values(unresolved_values.clone());

                    let mut builder = builder.into_phase2().await?;

                    let mut node_map = HashMap::new();
                    for (node, id) in unresolved_nodes.into_iter().zip(node_ids) {
                        node_map.insert(node, id);
                    }
                    let mut predicate_map = HashMap::new();
                    for (predicate, id) in unresolved_predicates.into_iter().zip(predicate_ids) {
                        predicate_map.insert(predicate, id);
                    }
                    let mut value_map = HashMap::new();
                    for (value, id) in unresolved_values.into_iter().zip(value_ids) {
                        value_map.insert(value, id + node_map.len() as u64);
                    }

                    let mut add_triples: Vec<_> = additions
                        .into_iter()
                        .map(|t| {
                            t.resolve_with(&node_map, &predicate_map, &value_map)
                                .expect("triple should have been resolvable")
                        })
                        .collect();
                    add_triples.par_sort_unstable();

                    builder.add_id_triples(add_triples).await?;
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

fn zero_equivalents(
    additions: &mut [PartiallyResolvedTriple],
    removals: &mut [PartiallyResolvedTriple],
) {
    let mut removals_iter = removals.iter_mut().peekable();
    'outer: for mut addition in additions {
        let mut next = removals_iter.peek();
        if next == None {
            break;
        }

        if next < Some(&addition) {
            loop {
                removals_iter.next().unwrap();
                next = removals_iter.peek();

                if next == None {
                    break 'outer;
                } else if next >= Some(&addition) {
                    break;
                }
            }
        }

        if next == Some(&addition) {
            let mut removal = removals_iter.next().unwrap();
            addition.subject = PossiblyResolved::Resolved(0);
            addition.predicate = PossiblyResolved::Resolved(0);
            addition.object = PossiblyResolved::Resolved(0);

            removal.subject = PossiblyResolved::Resolved(0);
            removal.predicate = PossiblyResolved::Resolved(0);
            removal.object = PossiblyResolved::Resolved(0);
        }
    }
}

fn collect_unresolved_strings(
    triples: &[PartiallyResolvedTriple],
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let (unresolved_nodes, (unresolved_predicates, unresolved_values)) = rayon::join(
        || {
            let unresolved_nodes_set: HashSet<_> = triples
                .par_iter()
                .filter_map(|triple| {
                    let subject = match triple.subject.is_resolved() {
                        true => None,
                        false => Some(triple.subject.as_ref().unwrap_unresolved().to_owned()),
                    };
                    let object = match triple.object.is_resolved() {
                        true => None,
                        false => match triple.object.as_ref().unwrap_unresolved() {
                            ObjectType::Node(node) => Some(node.to_owned()),
                            _ => None,
                        },
                    };

                    match (subject, object) {
                        (Some(subject), Some(object)) => Some(vec![subject, object]),
                        (Some(subject), _) => Some(vec![subject]),
                        (_, Some(object)) => Some(vec![object]),
                        _ => None,
                    }
                })
                .flatten()
                .collect();

            let mut unresolved_nodes: Vec<_> = unresolved_nodes_set.into_iter().collect();
            unresolved_nodes.par_sort_unstable();

            unresolved_nodes
        },
        || {
            rayon::join(
                || {
                    let unresolved_predicates_set: HashSet<_> = triples
                        .par_iter()
                        .filter_map(|triple| match triple.predicate.is_resolved() {
                            true => None,
                            false => Some(triple.predicate.as_ref().unwrap_unresolved().to_owned()),
                        })
                        .collect();
                    let mut unresolved_predicates: Vec<_> =
                        unresolved_predicates_set.into_iter().collect();
                    unresolved_predicates.par_sort_unstable();

                    unresolved_predicates
                },
                || {
                    let unresolved_values_set: HashSet<_> = triples
                        .par_iter()
                        .filter_map(|triple| match triple.object.is_resolved() {
                            true => None,
                            false => match triple.object.as_ref().unwrap_unresolved() {
                                ObjectType::Value(value) => Some(value.to_owned()),
                                _ => None,
                            },
                        })
                        .collect();
                    let mut unresolved_values: Vec<_> = unresolved_values_set.into_iter().collect();
                    unresolved_values.par_sort_unstable();
                    unresolved_values
                },
            )
        },
    );

    (unresolved_nodes, unresolved_predicates, unresolved_values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::internal::InternalLayer;
    use crate::storage::memory::*;

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

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit().await.unwrap();

        let layer = BaseLayer::load_from_files(name, &files).await.unwrap();
        Arc::new(layer.into())
    }

    #[tokio::test]
    async fn simple_base_layer_construction() {
        let layer = example_base_layer().await;

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn simple_child_layer_construction() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "cow"));
        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));

        let child_layer = Arc::new(
            async {
                builder.commit().await?;

                ChildLayer::load_from_files(name, base_layer, &files).await
            }
            .await
            .unwrap(),
        );

        assert!(
            child_layer.string_triple_exists(&StringTriple::new_value("horse", "says", "neigh"))
        );
        assert!(child_layer.string_triple_exists(&StringTriple::new_node("horse", "likes", "cow")));
        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(child_layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(
            !child_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack"))
        );
    }

    #[tokio::test]
    async fn multi_level_layers() {
        let base_layer = example_base_layer().await;
        let name2 = [0, 0, 0, 0, 0];
        let files2 = new_child_files();
        let mut builder =
            SimpleLayerBuilder::from_parent(name2, base_layer.clone(), files2.clone());

        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "cow"));
        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));

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
        builder.remove_string_triple(StringTriple::new_node("horse", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "pig"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

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
        builder.remove_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "horse"));
        builder.commit().await.unwrap();
        let layer4: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name4, layer3, &files4)
                .await
                .unwrap()
                .into(),
        );

        assert!(layer4.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer4.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
        assert!(layer4.string_triple_exists(&StringTriple::new_value("horse", "says", "neigh")));
        assert!(layer4.string_triple_exists(&StringTriple::new_node("horse", "likes", "pig")));
        assert!(layer4.string_triple_exists(&StringTriple::new_node("cow", "likes", "horse")));

        assert!(!layer4.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(!layer4.string_triple_exists(&StringTriple::new_node("horse", "likes", "cow")));
    }

    #[tokio::test]
    async fn remove_and_add_same_triple_on_base_layer_is_noop() {
        let files = new_base_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::new(name, files.clone());

        builder.remove_string_triple(StringTriple::new_value("crow", "says", "caw"));
        builder.add_string_triple(StringTriple::new_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let base_layer: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files(name, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(!base_layer.string_triple_exists(&StringTriple::new_value("crow", "says", "caw")));
    }

    #[tokio::test]
    async fn add_and_remove_same_triple_on_base_layer_is_noop() {
        let files = new_base_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::new(name, files.clone());

        builder.add_string_triple(StringTriple::new_value("crow", "says", "caw"));
        builder.remove_string_triple(StringTriple::new_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let base_layer: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files(name, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(!base_layer.string_triple_exists(&StringTriple::new_value("crow", "says", "caw")));
    }

    #[tokio::test]
    async fn remove_and_add_same_existing_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn add_and_remove_same_existing_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn remove_and_add_same_nonexisting_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_string_triple(StringTriple::new_value("crow", "says", "caw"));
        builder.add_string_triple(StringTriple::new_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(!child_layer.string_triple_exists(&StringTriple::new_value("crow", "says", "caw")));
    }

    #[tokio::test]
    async fn add_and_remove_same_nonexisting_triple_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_string_triple(StringTriple::new_value("crow", "says", "caw"));
        builder.remove_string_triple(StringTriple::new_value("crow", "says", "caw"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(!child_layer.string_triple_exists(&StringTriple::new_value("crow", "says", "caw")));
    }

    #[tokio::test]
    async fn remove_and_add_same_triple_by_id_and_string_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer.object_value_id("moo").unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_id_triple(IdTriple::new(node_id, predicate_id, value_id));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn remove_and_add_same_triple_by_string_and_id_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer.object_value_id("moo").unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.remove_id_triple(IdTriple::new(node_id, predicate_id, value_id));
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn add_and_remove_same_triple_by_id_and_string_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer.object_value_id("moo").unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.remove_id_triple(IdTriple::new(node_id, predicate_id, value_id));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }

    #[tokio::test]
    async fn add_and_remove_same_triple_by_string_and_id_on_child_layer_is_noop() {
        let base_layer = example_base_layer().await;
        let files = new_child_files();
        let name = [0, 0, 0, 0, 0];
        let node_id = base_layer.subject_id("cow").unwrap();
        let predicate_id = base_layer.predicate_id("says").unwrap();
        let value_id = base_layer.object_value_id("moo").unwrap();
        let mut builder = SimpleLayerBuilder::from_parent(name, base_layer.clone(), files.clone());

        builder.add_id_triple(IdTriple::new(node_id, predicate_id, value_id));
        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));

        builder.commit().await.unwrap();
        let child_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files(name, base_layer, &files)
                .await
                .unwrap()
                .into(),
        );

        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
    }
}
