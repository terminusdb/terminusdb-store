//! Child layer implementation
//!
//! A child layer stores a reference to a base layer, as well as
//! triple additions and removals, and any new dictionary entries that
//! this layer needs for its additions.
use super::layer::*;
use super::internal::*;
use super::builder::*;
use crate::storage::*;
use crate::structure::*;
use futures::future;
use futures::prelude::*;
use futures::stream;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

/// A child layer.
///
/// This layer type has a parent. It stores triple additions and removals.
#[derive(Clone)]
pub struct ChildLayer {
    name: [u32; 5],
    parent: Arc<InternalLayer>,

    node_dictionary: PfcDict,
    predicate_dictionary: PfcDict,
    value_dictionary: PfcDict,

    pos_subjects: MonotonicLogArray,
    pos_objects: MonotonicLogArray,
    pos_s_p_adjacency_list: AdjacencyList,
    pos_sp_o_adjacency_list: AdjacencyList,
    pos_o_ps_adjacency_list: AdjacencyList,

    neg_subjects: MonotonicLogArray,
    neg_objects: MonotonicLogArray,
    neg_s_p_adjacency_list: AdjacencyList,
    neg_sp_o_adjacency_list: AdjacencyList,
    neg_o_ps_adjacency_list: AdjacencyList,

    pos_predicate_wavelet_tree: WaveletTree,
    neg_predicate_wavelet_tree: WaveletTree,
}

impl ChildLayer {
    pub fn load_from_files<F: FileLoad + FileStore + Clone>(
        name: [u32; 5],
        parent: Arc<InternalLayer>,
        files: &ChildLayerFiles<F>,
    ) -> impl Future<Item = Self, Error = std::io::Error> {
        files
            .map_all()
            .map(move |maps| Self::load(name, parent, maps))
    }

    pub fn load(name: [u32; 5], parent: Arc<InternalLayer>, maps: ChildLayerMaps) -> ChildLayer {
        let node_dictionary = PfcDict::parse(
            maps.node_dictionary_maps.blocks_map,
            maps.node_dictionary_maps.offsets_map,
        )
        .unwrap();
        let predicate_dictionary = PfcDict::parse(
            maps.predicate_dictionary_maps.blocks_map,
            maps.predicate_dictionary_maps.offsets_map,
        )
        .unwrap();
        let value_dictionary = PfcDict::parse(
            maps.value_dictionary_maps.blocks_map,
            maps.value_dictionary_maps.offsets_map,
        )
        .unwrap();

        let pos_subjects =
            MonotonicLogArray::from_logarray(LogArray::parse(maps.pos_subjects_map).unwrap());
        let pos_objects =
            MonotonicLogArray::from_logarray(LogArray::parse(maps.pos_objects_map).unwrap());
        let neg_subjects =
            MonotonicLogArray::from_logarray(LogArray::parse(maps.neg_subjects_map).unwrap());
        let neg_objects =
            MonotonicLogArray::from_logarray(LogArray::parse(maps.neg_objects_map).unwrap());

        let pos_s_p_adjacency_list = AdjacencyList::parse(
            maps.pos_s_p_adjacency_list_maps.nums_map,
            maps.pos_s_p_adjacency_list_maps.bitindex_maps.bits_map,
            maps.pos_s_p_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.pos_s_p_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let pos_sp_o_adjacency_list = AdjacencyList::parse(
            maps.pos_sp_o_adjacency_list_maps.nums_map,
            maps.pos_sp_o_adjacency_list_maps.bitindex_maps.bits_map,
            maps.pos_sp_o_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.pos_sp_o_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let pos_o_ps_adjacency_list = AdjacencyList::parse(
            maps.pos_o_ps_adjacency_list_maps.nums_map,
            maps.pos_o_ps_adjacency_list_maps.bitindex_maps.bits_map,
            maps.pos_o_ps_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.pos_o_ps_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let neg_s_p_adjacency_list = AdjacencyList::parse(
            maps.neg_s_p_adjacency_list_maps.nums_map,
            maps.neg_s_p_adjacency_list_maps.bitindex_maps.bits_map,
            maps.neg_s_p_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.neg_s_p_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let neg_sp_o_adjacency_list = AdjacencyList::parse(
            maps.neg_sp_o_adjacency_list_maps.nums_map,
            maps.neg_sp_o_adjacency_list_maps.bitindex_maps.bits_map,
            maps.neg_sp_o_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.neg_sp_o_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let neg_o_ps_adjacency_list = AdjacencyList::parse(
            maps.neg_o_ps_adjacency_list_maps.nums_map,
            maps.neg_o_ps_adjacency_list_maps.bitindex_maps.bits_map,
            maps.neg_o_ps_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.neg_o_ps_adjacency_list_maps.bitindex_maps.sblocks_map,
        );

        let pos_predicate_wavelet_tree_width = pos_s_p_adjacency_list.nums().width();
        let pos_predicate_wavelet_tree = WaveletTree::from_parts(
            BitIndex::from_maps(
                maps.pos_predicate_wavelet_tree_maps.bits_map,
                maps.pos_predicate_wavelet_tree_maps.blocks_map,
                maps.pos_predicate_wavelet_tree_maps.sblocks_map,
            ),
            pos_predicate_wavelet_tree_width,
        );

        let neg_predicate_wavelet_tree_width = neg_s_p_adjacency_list.nums().width();
        let neg_predicate_wavelet_tree = WaveletTree::from_parts(
            BitIndex::from_maps(
                maps.neg_predicate_wavelet_tree_maps.bits_map,
                maps.neg_predicate_wavelet_tree_maps.blocks_map,
                maps.neg_predicate_wavelet_tree_maps.sblocks_map,
            ),
            neg_predicate_wavelet_tree_width,
        );

        ChildLayer {
            name,
            parent: parent,

            node_dictionary: node_dictionary,
            predicate_dictionary: predicate_dictionary,
            value_dictionary: value_dictionary,

            pos_subjects,
            pos_objects,
            neg_subjects,
            neg_objects,

            pos_s_p_adjacency_list,
            pos_sp_o_adjacency_list,
            pos_o_ps_adjacency_list,

            neg_s_p_adjacency_list,
            neg_sp_o_adjacency_list,
            neg_o_ps_adjacency_list,

            pos_predicate_wavelet_tree,
            neg_predicate_wavelet_tree,
        }
    }
}

impl InternalLayerImpl for ChildLayer {
    fn name(&self) -> [u32;5] {
        self.name
    }

    fn layer_type(&self) -> LayerType {
        LayerType::Base
    }

    fn parent_name(&self) -> Option<[u32;5]> {
        Some(InternalLayerImpl::name(&*self.parent))
    }

    fn immediate_parent(&self) -> Option<&InternalLayer> {
        Some(&*self.parent)
    }

    fn node_dictionary(&self) -> &PfcDict {
        &self.node_dictionary
    }

    fn predicate_dictionary(&self) -> &PfcDict {
        &self.predicate_dictionary
    }

    fn value_dictionary(&self) -> &PfcDict {
        &self.value_dictionary
    }

    fn pos_s_p_adjacency_list(&self) -> &AdjacencyList {
        &self.pos_s_p_adjacency_list
    }

    fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList {
        &self.pos_sp_o_adjacency_list
    }

    fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList {
        &self.pos_o_ps_adjacency_list
    }

    fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList> {
        Some(&self.neg_s_p_adjacency_list)
    }

    fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList> {
        Some(&self.neg_sp_o_adjacency_list)
    }

    fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList> {
        Some(&self.neg_o_ps_adjacency_list)
    }

    fn pos_predicate_wavelet_tree(&self) -> &WaveletTree {
        &self.pos_predicate_wavelet_tree
    }

    fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree> {
        Some(&self.neg_predicate_wavelet_tree)
    }

    fn pos_subjects(&self) -> Option<&MonotonicLogArray> {
        Some(&self.pos_subjects)
    }

    fn pos_objects(&self) -> Option<&MonotonicLogArray> {
        Some(&self.pos_objects)
    }

    fn neg_subjects(&self) -> Option<&MonotonicLogArray> {
        Some(&self.neg_subjects)
    }

    fn neg_objects(&self) -> Option<&MonotonicLogArray> {
        Some(&self.neg_objects)
    }
}

/// A builder for a child layer.
///
/// This builder takes node, predicate and value strings in lexical
/// order through the corresponding `add_<thing>` methods. When
/// they're all added, `into_phase2()` is to be called to turn this
/// builder into a second builder that takes triple data.
pub struct ChildLayerFileBuilder<F: 'static + FileLoad + FileStore + Clone + Send + Sync> {
    parent: Arc<dyn Layer>,
    files: ChildLayerFiles<F>,
    builder: DictionarySetFileBuilder<F>
}

impl<F: 'static + FileLoad + FileStore + Clone + Send + Sync> ChildLayerFileBuilder<F> {
    /// Create the builder from the given files.
    pub fn from_files(parent: Arc<dyn Layer>, files: &ChildLayerFiles<F>) -> Self {
        let builder = DictionarySetFileBuilder::from_files(
            files.node_dictionary_files.clone(),
            files.predicate_dictionary_files.clone(),
            files.value_dictionary_files.clone()
        );

        Self {
            parent,
            files: files.clone(),
            builder,
        }
    }

    /// Add a node string.
    ///
    /// Does nothing if the node already exists in the paretn, and
    /// panics if the given node string is not a lexical successor of
    /// the previous node string.
    pub fn add_node(
        self,
        node: &str,
    ) -> impl Future<Item = (u64, Self), Error = std::io::Error> + Send {
        match self.parent.subject_id(node) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    files,
                    builder
                } = self;
                future::Either::A(builder.add_node(node).map(
                    move |(result, builder)| {
                        (
                            result,
                            ChildLayerFileBuilder {
                                parent,
                                files,
                                builder
                            },
                        )
                    },
                ))
            }
            Some(id) => future::Either::B(future::ok((id, self))),
        }
    }

    /// Add a predicate string.
    ///
    /// Does nothing if the predicate already exists in the paretn, and
    /// panics if the given predicate string is not a lexical successor of
    /// the previous predicate string.
    pub fn add_predicate(
        self,
        predicate: &str,
    ) -> impl Future<Item = (u64, Self), Error = std::io::Error> + Send {
        match self.parent.predicate_id(predicate) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    files,
                    builder
                } = self;

                future::Either::A(builder.add_predicate(predicate).map(
                    move |(result, builder)| {
                        (
                            result,
                            ChildLayerFileBuilder {
                                parent,
                                files,
                                builder
                            },
                        )
                    },
                ))
            }
            Some(id) => future::Either::B(future::ok((id, self))),
        }
    }

    /// Add a value string.
    ///
    /// Does nothing if the value already exists in the paretn, and
    /// panics if the given value string is not a lexical successor of
    /// the previous value string.
    pub fn add_value(
        self,
        value: &str,
    ) -> impl Future<Item = (u64, Self), Error = std::io::Error> + Send {
        match self.parent.object_value_id(value) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    files,
                    builder
                } = self;
                future::Either::A(builder.add_value(value).map(
                    move |(result, builder)| {
                        (
                            result,
                            ChildLayerFileBuilder {
                                parent,
                                files,
                                builder
                            },
                        )
                    },
                ))
            }
            Some(id) => future::Either::B(future::ok((id, self))),
        }
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous
    /// added nodes are a lexical succesor of any of these
    /// nodes. Skips any nodes that are already part of the base
    /// layer.
    pub fn add_nodes<I: 'static + IntoIterator<Item = String>+Send>(
        self,
        nodes: I,
    ) -> impl Future<Item = (Vec<u64>, Self), Error = std::io::Error>+Send
    where
        <I as std::iter::IntoIterator>::IntoIter: Send,
    {
        stream::iter_ok(nodes.into_iter()).fold(
            (Vec::new(), self),
            |(mut result, builder), node| {
                builder.add_node(&node).map(|(id, builder)| {
                    result.push(id);

                    (result, builder)
                })
            },
        )
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if
    /// previous added predicates are a lexical succesor of any of
    /// these predicates. Skips any predicates that are already part
    /// of the base layer.
    pub fn add_predicates<I: 'static + IntoIterator<Item = String>+Send>(
        self,
        predicates: I,
    ) -> impl Future<Item = (Vec<u64>, Self), Error = std::io::Error>+Send
    where
        <I as std::iter::IntoIterator>::IntoIter: Send,
    {
        stream::iter_ok(predicates.into_iter()).fold(
            (Vec::new(), self),
            |(mut result, builder), predicate| {
                builder.add_predicate(&predicate).map(|(id, builder)| {
                    result.push(id);

                    (result, builder)
                })
            },
        )
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous
    /// added values are a lexical succesor of any of these
    /// values. Skips any nodes that are already part of the base
    /// layer.
    pub fn add_values<I: 'static + IntoIterator<Item = String>+Send>(
        self,
        values: I,
    ) -> impl Future<Item = (Vec<u64>, Self), Error = std::io::Error>+Send 
    where
        <I as std::iter::IntoIterator>::IntoIter: Send,
    {
        stream::iter_ok(values.into_iter()).fold(
            (Vec::new(), self),
            |(mut result, builder), value| {
                builder.add_value(&value).map(|(id, builder)| {
                    result.push(id);

                    (result, builder)
                })
            },
        )
    }

    /// Turn this builder into a phase 2 builder that will take triple data.
    pub fn into_phase2(
        self,
    ) -> impl Future<Item = ChildLayerFileBuilderPhase2<F>, Error = std::io::Error> {
        let ChildLayerFileBuilder {
            parent,
            files,
            builder
        } = self;

        let dict_maps_fut = vec![
            files.node_dictionary_files.blocks_file.map(),
            files.node_dictionary_files.offsets_file.map(),
            files.predicate_dictionary_files.blocks_file.map(),
            files.predicate_dictionary_files.offsets_file.map(),
            files.value_dictionary_files.blocks_file.map(),
            files.value_dictionary_files.offsets_file.map(),
        ];

        builder.finalize()
            .and_then(move |_| future::join_all(dict_maps_fut))
            .and_then(move |dict_maps| {
                let node_dict_r = PfcDict::parse(dict_maps[0].clone(), dict_maps[1].clone());
                if node_dict_r.is_err() {
                    return future::err(node_dict_r.err().unwrap().into());
                }
                let node_dict = node_dict_r.unwrap();

                let pred_dict_r = PfcDict::parse(dict_maps[2].clone(), dict_maps[3].clone());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(dict_maps[4].clone(), dict_maps[5].clone());
                if val_dict_r.is_err() {
                    return future::err(val_dict_r.err().unwrap().into());
                }
                let val_dict = val_dict_r.unwrap();

                let num_nodes = node_dict.len();
                let num_predicates = pred_dict.len();
                let num_values = val_dict.len();

                future::ok(ChildLayerFileBuilderPhase2::new(
                    parent,
                    files,
                    num_nodes,
                    num_predicates,
                    num_values,
                ))
            })
    }
}

/// Second phase of child layer building.
///
/// This builder takes ordered triple additions and removals. When all
/// data has been added, `finalize()` will build a layer.
pub struct ChildLayerFileBuilderPhase2<F: 'static + FileLoad + FileStore + Clone + Send + Sync> {
    parent: Arc<dyn Layer>,

    files: ChildLayerFiles<F>,

    pos_builder: TripleFileBuilder<F>,
    neg_builder: TripleFileBuilder<F>,
}

impl<F: 'static + FileLoad + FileStore + Clone + Send + Sync> ChildLayerFileBuilderPhase2<F> {
    fn new(
        parent: Arc<dyn Layer>,
        files: ChildLayerFiles<F>,

        num_nodes: usize,
        num_predicates: usize,
        num_values: usize,
    ) -> Self {
        let parent_counts = parent.all_counts();
        let pos_builder = TripleFileBuilder::new(
            files.pos_s_p_adjacency_list_files.clone(),
            files.pos_sp_o_adjacency_list_files.clone(),
            num_nodes + parent_counts.node_count,
            num_predicates + parent_counts.predicate_count,
            num_values + parent_counts.value_count,
            Some(files.pos_subjects_file.clone())
        );

        let neg_builder = TripleFileBuilder::new(
            files.neg_s_p_adjacency_list_files.clone(),
            files.neg_sp_o_adjacency_list_files.clone(),
            num_nodes + parent_counts.node_count,
            num_predicates + parent_counts.predicate_count,
            num_values + parent_counts.value_count,
            Some(files.neg_subjects_file.clone())
        );

        ChildLayerFileBuilderPhase2 {
            parent,
            files,

            pos_builder,
            neg_builder
        }
    }

    /// Add the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been added,
    /// and do nothing if the triple is already part of the parent.
    pub fn add_triple(
        self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> impl Future<Item = Self, Error = std::io::Error> + Send {
        if self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            return future::Either::A(future::ok(self))
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            files,

            pos_builder,
            neg_builder,
        } = self;

        future::Either::B(
            pos_builder.add_triple(subject, predicate, object).
                map(move |pos_builder| ChildLayerFileBuilderPhase2 {
                    parent,
                    files,

                    pos_builder,
                    neg_builder,
                })
        )
    }

    /// Remove the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been removed,
    /// and do nothing if the parent doesn't know aobut this triple.
    pub fn remove_triple(
        self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> impl Future<Item = Self, Error = std::io::Error> + Send {
        if !self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            return future::Either::A(future::ok(self))
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            files,

            pos_builder,
            neg_builder,
        } = self;

        future::Either::B(
            neg_builder.add_triple(subject, predicate, object).
                map(move |neg_builder| ChildLayerFileBuilderPhase2 {
                    parent,
                    files,

                    pos_builder,
                    neg_builder,
                })
        )
    }

    /// Add the given triple.
    ///
    /// This will panic if a greater triple has already been added,
    /// and do nothing if the parent already contains this triple.
    pub fn add_id_triples<I: 'static + IntoIterator<Item = IdTriple>>(
        self,
        triples: I,
    ) -> impl Future<Item = Self, Error = std::io::Error>+Send
        where <I as std::iter::IntoIterator>::IntoIter: Send {
        stream::iter_ok(triples).fold(self, |b, triple| {
            b.add_triple(triple.subject, triple.predicate, triple.object)
        })
    }

    /// Remove the given triple.
    ///
    /// This will panic if a greater triple has already been removed,
    /// and do nothing if the parent doesn't know aobut this triple.
    pub fn remove_id_triples<I: 'static + IntoIterator<Item = IdTriple>>(
        self,
        triples: I,
    ) -> impl Future<Item = Self, Error = std::io::Error>+Send
        where <I as std::iter::IntoIterator>::IntoIter: Send {
        stream::iter_ok(triples).fold(self, |b, triple| {
            b.remove_triple(triple.subject, triple.predicate, triple.object)
        })
    }

    /// Write the layer data to storage.
    pub fn finalize(self) -> impl Future<Item = (), Error = std::io::Error>+Send {
        let builder_futs = vec![
            self.pos_builder.finalize(),
            self.neg_builder.finalize(),
        ];

        let pos_s_p_files = self.files.pos_s_p_adjacency_list_files;
        let pos_sp_o_files = self.files.pos_sp_o_adjacency_list_files;
        let pos_o_ps_files = self.files.pos_o_ps_adjacency_list_files;
        let pos_objects_file = self.files.pos_objects_file;
        let neg_s_p_files = self.files.neg_s_p_adjacency_list_files;
        let neg_sp_o_files = self.files.neg_sp_o_adjacency_list_files;
        let neg_o_ps_files = self.files.neg_o_ps_adjacency_list_files;
        let neg_objects_file = self.files.neg_objects_file;

        let pos_predicate_wavelet_tree_files = self.files.pos_predicate_wavelet_tree_files;
        let neg_predicate_wavelet_tree_files = self.files.neg_predicate_wavelet_tree_files;

        future::join_all(builder_futs)
        .and_then(|_| {
            build_object_index(pos_sp_o_files, pos_o_ps_files, pos_objects_file)
                .join(build_object_index(
                    neg_sp_o_files,
                    neg_o_ps_files,
                    neg_objects_file,
                ))
                .join(build_wavelet_tree_from_logarray(
                    pos_s_p_files.nums_file,
                    pos_predicate_wavelet_tree_files.bits_file,
                    pos_predicate_wavelet_tree_files.blocks_file,
                    pos_predicate_wavelet_tree_files.sblocks_file,
                ))
                .join(build_wavelet_tree_from_logarray(
                    neg_s_p_files.nums_file,
                    neg_predicate_wavelet_tree_files.bits_file,
                    neg_predicate_wavelet_tree_files.blocks_file,
                    neg_predicate_wavelet_tree_files.sblocks_file,
                ))
        })
        .map(|_| ())
    }
}

fn build_object_index<F: 'static + FileLoad + FileStore>(
    sp_o_files: AdjacencyListFiles<F>,
    o_ps_files: AdjacencyListFiles<F>,
    objects_file: F,
) -> impl Future<Item = (), Error = std::io::Error> {
    adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file)
        .map(|(left, right)| (right, left))
        .fold(
            (BTreeSet::new(), BTreeSet::new(), 0),
            |(mut pairs_set, mut objects_set, _), (left, right)| {
                pairs_set.insert((left, right));
                objects_set.insert(left);
                future::ok::<_, std::io::Error>((pairs_set, objects_set, right))
            },
        )
        .and_then(move |(pairs, objects, greatest_sp)| {
            let greatest_object = objects.iter().next_back().unwrap_or(&0);
            let objects_width = ((*greatest_object + 1) as f32).log2().ceil() as u8;
            let aj_width = ((greatest_sp + 1) as f32).log2().ceil() as u8;

            let o_ps_adjacency_list_builder = AdjacencyListBuilder::new(
                o_ps_files.bitindex_files.bits_file,
                o_ps_files.bitindex_files.blocks_file.open_write(),
                o_ps_files.bitindex_files.sblocks_file.open_write(),
                o_ps_files.nums_file.open_write(),
                aj_width,
            );
            let objects_builder =
                LogArrayFileBuilder::new(objects_file.open_write(), objects_width);

            let compressed_pairs = pairs
                .into_iter()
                .scan((0, 0), |(compressed, last), (left, right)| {
                    if left > *last {
                        *compressed += 1;
                    }

                    *last = left;

                    Some((*compressed, right))
                })
                .collect::<Vec<_>>();

            let build_o_ps_task = o_ps_adjacency_list_builder
                .push_all(stream::iter_ok(compressed_pairs))
                .and_then(|builder| builder.finalize());

            let build_objects_task = objects_builder
                .push_all(stream::iter_ok(objects))
                .and_then(|builder| builder.finalize());

            build_o_ps_task.join(build_objects_task)
        })
        .map(|_| ())
}

pub struct ChildTripleStream<
    S1: Stream<Item = u64, Error = io::Error>,
    S2: Stream<Item = (u64, u64), Error = io::Error> + Send,
> {
    subjects_stream: stream::Peekable<S1>,
    s_p_stream: stream::Peekable<S2>,
    sp_o_stream: stream::Peekable<S2>,
    last_mapped_s: u64,
    last_s_p: (u64, u64),
    last_sp: u64,
}

impl<
        S1: Stream<Item = u64, Error = io::Error>,
        S2: Stream<Item = (u64, u64), Error = io::Error> + Send,
    > ChildTripleStream<S1, S2>
{
    fn new(subjects_stream: S1, s_p_stream: S2, sp_o_stream: S2) -> ChildTripleStream<S1, S2> {
        ChildTripleStream {
            subjects_stream: subjects_stream.peekable(),
            s_p_stream: s_p_stream.peekable(),
            sp_o_stream: sp_o_stream.peekable(),
            last_mapped_s: 0,
            last_s_p: (0, 0),
            last_sp: 0,
        }
    }
}

impl<
        S1: Stream<Item = u64, Error = io::Error>,
        S2: Stream<Item = (u64, u64), Error = io::Error> + Send,
    > Stream for ChildTripleStream<S1, S2>
{
    type Item = (u64, u64, u64);
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<(u64, u64, u64)>>, io::Error> {
        let sp_o = self.sp_o_stream.peek().map(|x| x.map(|x| x.map(|x| *x)));
        match sp_o {
            Err(e) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(Some((sp, o)))) => {
                if sp > self.last_sp {
                    let s_p = self.s_p_stream.peek().map(|x| x.map(|x| x.map(|x| *x)));
                    match s_p {
                        Err(e) => Err(e),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Ok(Async::Ready(None)) => Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected end of s_p_stream",
                        )),
                        Ok(Async::Ready(Some((s, p)))) => {
                            if s > self.last_s_p.0 {
                                let mapped_s = self
                                    .subjects_stream
                                    .peek()
                                    .map(|x| x.map(|x| x.map(|x| *x)));
                                match mapped_s {
                                    Err(e) => Err(e),
                                    Ok(Async::NotReady) => Ok(Async::NotReady),
                                    Ok(Async::Ready(None)) => Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "unexpected end of subjects_stream",
                                    )),
                                    Ok(Async::Ready(Some(mapped_s))) => {
                                        self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");
                                        self.s_p_stream.poll().expect("peeked stream s_p_stream with confirmed result did not have result on poll");
                                        self.subjects_stream.poll().expect("peeked stream subjects_stream with confirmed result did not have result on poll");
                                        self.last_mapped_s = mapped_s;
                                        self.last_s_p = (s, p);
                                        self.last_sp = sp;

                                        Ok(Async::Ready(Some((mapped_s, p, o))))
                                    }
                                }
                            } else {
                                self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");
                                self.s_p_stream.poll().expect("peeked stream s_p_stream with confirmed result did not have result on poll");
                                self.last_s_p = (s, p);
                                self.last_sp = sp;

                                Ok(Async::Ready(Some((self.last_mapped_s, p, o))))
                            }
                        }
                    }
                } else {
                    self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");

                    Ok(Async::Ready(Some((self.last_mapped_s, self.last_s_p.1, o))))
                }
            }
        }
    }
}

pub fn open_child_triple_stream<F: 'static + FileLoad + FileStore>(
    subjects_file: F,
    s_p_files: AdjacencyListFiles<F>,
    sp_o_files: AdjacencyListFiles<F>,
) -> impl Stream<Item = (u64, u64, u64), Error = io::Error> + Send {
    let subjects_stream = logarray_stream_entries(subjects_file);
    let s_p_stream =
        adjacency_list_stream_pairs(s_p_files.bitindex_files.bits_file, s_p_files.nums_file);
    let sp_o_stream =
        adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file);

    ChildTripleStream::new(subjects_stream, s_p_stream, sp_o_stream)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::layer::base::tests::*;
    use crate::storage::memory::*;
    pub fn child_layer_files() -> ChildLayerFiles<MemoryBackedStore> {
        ChildLayerFiles {
            node_dictionary_files: DictionaryFiles {
                blocks_file: MemoryBackedStore::new(),
                offsets_file: MemoryBackedStore::new(),
            },
            predicate_dictionary_files: DictionaryFiles {
                blocks_file: MemoryBackedStore::new(),
                offsets_file: MemoryBackedStore::new(),
            },
            value_dictionary_files: DictionaryFiles {
                blocks_file: MemoryBackedStore::new(),
                offsets_file: MemoryBackedStore::new(),
            },

            pos_subjects_file: MemoryBackedStore::new(),
            pos_objects_file: MemoryBackedStore::new(),
            neg_subjects_file: MemoryBackedStore::new(),
            neg_objects_file: MemoryBackedStore::new(),

            pos_s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            pos_sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            pos_o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            neg_s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            neg_sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            neg_o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            pos_predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            neg_predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
        }
    }

    #[test]
    fn empty_child_layer_equivalent_to_parent() {
        let base_layer = example_base_layer();

        let parent:Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert!(child_layer.triple_exists(1, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 3));
        assert!(child_layer.triple_exists(2, 3, 6));
        assert!(child_layer.triple_exists(3, 2, 5));
        assert!(child_layer.triple_exists(3, 3, 6));
        assert!(child_layer.triple_exists(4, 3, 6));

        assert!(!child_layer.triple_exists(2, 2, 0));
    }

    #[test]
    fn child_layer_can_have_inserts() {
        let base_layer = example_base_layer();

        let parent:Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(2, 1, 2))
            .and_then(|b| b.add_triple(3, 3, 3))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert!(child_layer.triple_exists(1, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 2));
        assert!(child_layer.triple_exists(2, 1, 3));
        assert!(child_layer.triple_exists(2, 3, 6));
        assert!(child_layer.triple_exists(3, 2, 5));
        assert!(child_layer.triple_exists(3, 3, 3));
        assert!(child_layer.triple_exists(3, 3, 6));
        assert!(child_layer.triple_exists(4, 3, 6));

        assert!(!child_layer.triple_exists(2, 2, 0));
    }

    #[test]
    fn child_layer_can_have_deletes() {
        let base_layer = example_base_layer();

        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert!(child_layer.triple_exists(1, 1, 1));
        assert!(!child_layer.triple_exists(2, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 3));
        assert!(child_layer.triple_exists(2, 3, 6));
        assert!(!child_layer.triple_exists(3, 2, 5));
        assert!(child_layer.triple_exists(3, 3, 6));
        assert!(child_layer.triple_exists(4, 3, 6));

        assert!(!child_layer.triple_exists(2, 2, 0));
    }

    #[test]
    fn child_layer_can_have_inserts_and_deletes() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 3))
            .and_then(|b| b.add_triple(2, 3, 4))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert!(child_layer.triple_exists(1, 1, 1));
        assert!(child_layer.triple_exists(1, 2, 3));
        assert!(child_layer.triple_exists(2, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 3));
        assert!(child_layer.triple_exists(2, 3, 4));
        assert!(child_layer.triple_exists(2, 3, 6));
        assert!(!child_layer.triple_exists(3, 2, 5));
        assert!(child_layer.triple_exists(3, 3, 6));
        assert!(child_layer.triple_exists(4, 3, 6));

        assert!(!child_layer.triple_exists(2, 2, 0));
    }

    #[test]
    fn iterate_child_layer_triples() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 3))
            .and_then(|b| b.add_triple(2, 3, 4))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let subjects: Vec<_> = child_layer
            .triples()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(
            vec![
                (1, 1, 1),
                (1, 2, 3),
                (2, 1, 1),
                (2, 1, 3),
                (2, 3, 4),
                (2, 3, 6),
                (3, 3, 6),
                (4, 3, 6)
            ],
            subjects
        );
    }

    #[test]
    fn iterate_child_layer_triples_by_object() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 3))
            .and_then(|b| b.add_triple(2, 3, 4))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let triples: Vec<_> = child_layer
            .objects()
            .map(|o| o.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(
            vec![
                (1, 1, 1),
                (2, 1, 1),
                (1, 2, 3),
                (2, 1, 3),
                (2, 3, 4),
                (2, 3, 6),
                (3, 3, 6),
                (4, 3, 6)
            ],
            triples
        );
    }

    #[test]
    fn iterate_child_layer_triples_by_objects_with_equal_predicates() {
        let base_layer = empty_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .add_node("a")
            .and_then(|(_, b)| b.add_predicate("b"))
            .and_then(|(_, b)| b.add_predicate("c"))
            .and_then(|(_, b)| b.add_value("d"))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(1, 1, 2))
            .and_then(|b| b.add_triple(1, 2, 1))
            .and_then(|b| b.add_triple(2, 1, 1))
            .and_then(|b| b.add_triple(2, 2, 1))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let triples: Vec<_> = child_layer
            .objects()
            .map(|o| o.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(
            vec![(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1), (1, 1, 2)],
            triples
        );
    }

    #[test]
    fn lookup_child_layer_triples_by_predicate() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 3))
            .and_then(|b| b.add_triple(2, 3, 4))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let lookup = child_layer.lookup_predicate(1).unwrap();
        let pairs: Vec<_> = lookup
            .subject_predicate_pairs()
            .map(|sp| sp.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 1, 1), (2, 1, 1), (2, 1, 3)], pairs);

        let lookup = child_layer.lookup_predicate(2).unwrap();
        let pairs: Vec<_> = lookup
            .subject_predicate_pairs()
            .map(|sp| sp.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 2, 3)], pairs);

        let lookup = child_layer.lookup_predicate(3).unwrap();
        let pairs: Vec<_> = lookup
            .subject_predicate_pairs()
            .map(|sp| sp.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 3, 4), (2, 3, 6), (3, 3, 6), (4, 3, 6)], pairs);

        let lookup = child_layer.lookup_predicate(4);

        assert!(lookup.is_none());
    }

    #[test]
    fn adding_new_nodes_predicates_and_values_in_child() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(11, 2, 3))
            .and_then(|b| b.add_triple(12, 3, 4))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert!(child_layer.triple_exists(11, 2, 3));
        assert!(child_layer.triple_exists(12, 3, 4));
    }

    #[test]
    fn old_dictionary_entries_in_child() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .add_node("foo")
            .and_then(|(_, b)| b.add_predicate("bar"))
            .and_then(|(_, b)| b.add_value("baz"))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert_eq!(3, child_layer.subject_id("bbbbb").unwrap());
        assert_eq!(2, child_layer.predicate_id("fghij").unwrap());
        assert_eq!(1, child_layer.object_node_id("aaaaa").unwrap());
        assert_eq!(6, child_layer.object_value_id("chicken").unwrap());

        assert_eq!("bbbbb", child_layer.id_subject(3).unwrap());
        assert_eq!("fghij", child_layer.id_predicate(2).unwrap());
        assert_eq!(
            ObjectType::Node("aaaaa".to_string()),
            child_layer.id_object(1).unwrap()
        );
        assert_eq!(
            ObjectType::Value("chicken".to_string()),
            child_layer.id_object(6).unwrap()
        );
    }

    #[test]
    fn new_dictionary_entries_in_child() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .add_node("foo")
            .and_then(|(_, b)| b.add_predicate("bar"))
            .and_then(|(_, b)| b.add_value("baz"))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert_eq!(11, child_layer.subject_id("foo").unwrap());
        assert_eq!(5, child_layer.predicate_id("bar").unwrap());
        assert_eq!(11, child_layer.object_node_id("foo").unwrap());
        assert_eq!(12, child_layer.object_value_id("baz").unwrap());

        assert_eq!("foo", child_layer.id_subject(11).unwrap());
        assert_eq!("bar", child_layer.id_predicate(5).unwrap());
        assert_eq!(
            ObjectType::Node("foo".to_string()),
            child_layer.id_object(11).unwrap()
        );
        assert_eq!(
            ObjectType::Value("baz".to_string()),
            child_layer.id_object(12).unwrap()
        );
    }

    #[test]
    fn lookup_additions_by_subject() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.add_triple(2, 2, 2))
            .and_then(|b| b.add_triple(3, 4, 5))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let result: Vec<_> = child_layer
            .subject_additions()
            .map(|s| s.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 3, 4), (2, 2, 2), (3, 4, 5)], result);
    }

    #[test]
    fn lookup_additions_by_predicate() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.add_triple(2, 2, 2))
            .and_then(|b| b.add_triple(3, 4, 5))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let result: Vec<_> = child_layer
            .predicate_additions()
            .map(|s| s.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 2, 2), (1, 3, 4), (3, 4, 5)], result);
    }

    #[test]
    fn lookup_additions_by_object() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.add_triple(2, 2, 2))
            .and_then(|b| b.add_triple(3, 4, 5))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let result: Vec<_> = child_layer
            .object_additions()
            .map(|s| s.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 2, 2), (1, 3, 4), (3, 4, 5)], result);
    }

    #[test]
    fn lookup_removals_by_subject() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.remove_triple(4, 3, 6))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let result: Vec<_> = child_layer
            .subject_removals()
            .map(|s| s.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 1, 1), (3, 2, 5), (4, 3, 6)], result);
    }

    #[test]
    fn lookup_removals_by_predicate() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(2, 3, 6))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let result: Vec<_> = child_layer
            .predicate_removals()
            .map(|s| s.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 1, 1), (3, 2, 5), (2, 3, 6)], result);
    }

    #[test]
    fn lookup_removals_by_object() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(2, 3, 6))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        let result: Vec<_> = child_layer
            .object_removals()
            .map(|s| s.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 1, 1), (3, 2, 5), (2, 3, 6)], result);
    }

    #[test]
    fn create_empty_child_layer() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 3, 4))
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(2, 3, 6))
            .and_then(|b| b.remove_triple(3, 2, 5))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let child_layer =
            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent.clone(), &child_files)
                .wait()
                .unwrap();

        assert_eq!(
            parent.node_and_value_count(),
            child_layer.node_and_value_count()
        );
        assert_eq!(parent.predicate_count(), child_layer.predicate_count());
    }

    #[test]
    fn child_layer_with_multiple_pairs_pointing_at_same_object_lookup_by_objects() {
        let base_layer = empty_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let future = builder
            .add_nodes(vec!["a", "b"].into_iter().map(|x| x.to_string()))
            .and_then(|(_, b)| b.add_predicates(vec!["c", "d"].into_iter().map(|x| x.to_string())))
            .and_then(|(_, b)| b.add_values(vec!["e"].into_iter().map(|x| x.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(1, 2, 1))
            .and_then(|b| b.add_triple(2, 1, 1))
            .and_then(|b| b.add_triple(2, 2, 1))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        let child_layer =
            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent.clone(), &child_files)
                .wait()
                .unwrap();

        let triples_by_object: Vec<_> = child_layer
            .objects()
            .map(|o| {
                o.subject_predicate_pairs()
                    .map(move |(s, p)| (s, p, o.object()))
            })
            .flatten()
            .collect();

        assert_eq!(
            vec![(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)],
            triples_by_object
        );
    }

    #[test]
    fn stream_child_triples() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let future = builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 1))
            .and_then(|b| b.add_triple(3, 1, 5))
            .and_then(|b| b.add_triple(5, 2, 3))
            .and_then(|b| b.add_triple(5, 2, 4))
            .and_then(|b| b.add_triple(5, 2, 5))
            .and_then(|b| b.add_triple(5, 3, 1))
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(2, 3, 6))
            .and_then(|b| b.remove_triple(4, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        let addition_stream = open_child_triple_stream(
            child_files.pos_subjects_file,
            child_files.pos_s_p_adjacency_list_files,
            child_files.pos_sp_o_adjacency_list_files,
        );
        let removal_stream = open_child_triple_stream(
            child_files.neg_subjects_file,
            child_files.neg_s_p_adjacency_list_files,
            child_files.neg_sp_o_adjacency_list_files,
        );

        let addition_triples: Vec<_> = addition_stream.collect().wait().unwrap();
        let removal_triples: Vec<_> = removal_stream.collect().wait().unwrap();

        assert_eq!(
            vec![
                (1, 2, 1),
                (3, 1, 5),
                (5, 2, 3),
                (5, 2, 4),
                (5, 2, 5),
                (5, 3, 1)
            ],
            addition_triples
        );

        assert_eq!(vec![(2, 1, 1), (2, 3, 6), (4, 3, 6)], removal_triples);
    }

    #[test]
    fn count_triples() {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let future = builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 1))
            .and_then(|b| b.add_triple(3, 1, 5))
            .and_then(|b| b.add_triple(5, 2, 3))
            .and_then(|b| b.add_triple(5, 2, 4))
            .and_then(|b| b.add_triple(5, 2, 5))
            .and_then(|b| b.add_triple(5, 3, 1))
            .and_then(|b| b.remove_triple(2, 1, 1))
            .and_then(|b| b.remove_triple(2, 3, 6))
            .and_then(|b| b.remove_triple(4, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();
        let child_layer = ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap();

        assert_eq!(6, child_layer.triple_layer_addition_count());
        assert_eq!(3, child_layer.triple_layer_removal_count());
        assert_eq!(13, child_layer.triple_addition_count());
        assert_eq!(3, child_layer.triple_removal_count());
        assert_eq!(10, child_layer.triple_count());
    }
}
