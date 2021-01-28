//! Child layer implementation
//!
//! A child layer stores a reference to a base layer, as well as
//! triple additions and removals, and any new dictionary entries that
//! this layer needs for its additions.
use super::super::builder::*;
use super::super::id_map::*;
use super::super::layer::*;
use super::*;
use crate::storage::*;
use crate::structure::*;
use rayon::prelude::*;

use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};
use futures::task::{Context, Poll};

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

    node_value_idmap: IdMap,
    predicate_idmap: IdMap,

    parent_node_value_count: usize,
    parent_predicate_count: usize,

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
    pub async fn load_from_files<F: FileLoad + FileStore + Clone>(
        name: [u32; 5],
        parent: Arc<InternalLayer>,
        files: &ChildLayerFiles<F>,
    ) -> io::Result<Self> {
        let maps = files.map_all().await?;
        Ok(Self::load(name, parent, maps))
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

        let parent_node_value_count = parent.node_and_value_count();
        let parent_predicate_count = parent.predicate_count();

        let node_value_idmap = match maps.id_map_maps.node_value_idmap_maps {
            None => IdMap::default(),
            Some(maps) => IdMap::from_maps(
                maps,
                util::calculate_width((node_dictionary.len() + value_dictionary.len()) as u64),
            ),
        };

        let predicate_idmap = match maps.id_map_maps.predicate_idmap_maps {
            None => IdMap::default(),
            Some(map) => IdMap::from_maps(
                map,
                util::calculate_width(predicate_dictionary.len() as u64),
            ),
        };

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
            parent,

            node_dictionary,
            predicate_dictionary,
            value_dictionary,

            node_value_idmap,
            predicate_idmap,

            parent_node_value_count,
            parent_predicate_count,

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
    fn name(&self) -> [u32; 5] {
        self.name
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
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

    fn node_value_id_map(&self) -> &IdMap {
        &self.node_value_idmap
    }

    fn predicate_id_map(&self) -> &IdMap {
        &self.predicate_idmap
    }

    fn parent_node_value_count(&self) -> usize {
        self.parent_node_value_count
    }

    fn parent_predicate_count(&self) -> usize {
        self.parent_predicate_count
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
    builder: DictionarySetFileBuilder<F>,
}

impl<F: 'static + FileLoad + FileStore + Clone + Send + Sync> ChildLayerFileBuilder<F> {
    /// Create the builder from the given files.
    pub fn from_files(parent: Arc<dyn Layer>, files: &ChildLayerFiles<F>) -> Self {
        let builder = DictionarySetFileBuilder::from_files(
            files.node_dictionary_files.clone(),
            files.predicate_dictionary_files.clone(),
            files.value_dictionary_files.clone(),
        );

        Self {
            parent,
            files: files.clone(),
            builder,
        }
    }

    /// Add a node string.
    ///
    /// Does nothing if the node already exists in the parent, and
    /// panics if the given node string is not a lexical successor of
    /// the previous node string.
    pub async fn add_node(&mut self, node: &str) -> io::Result<u64> {
        match self.parent.subject_id(node) {
            None => self.builder.add_node(node).await,
            Some(id) => Ok(id),
        }
    }

    /// Add a predicate string.
    ///
    /// Does nothing if the predicate already exists in the paretn, and
    /// panics if the given predicate string is not a lexical successor of
    /// the previous predicate string.
    pub async fn add_predicate(&mut self, predicate: &str) -> io::Result<u64> {
        match self.parent.predicate_id(predicate) {
            None => self.builder.add_predicate(predicate).await,
            Some(id) => Ok(id),
        }
    }

    /// Add a value string.
    ///
    /// Does nothing if the value already exists in the paretn, and
    /// panics if the given value string is not a lexical successor of
    /// the previous value string.
    pub async fn add_value(&mut self, value: &str) -> io::Result<u64> {
        match self.parent.object_value_id(value) {
            None => self.builder.add_value(value).await,
            Some(id) => Ok(id),
        }
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous
    /// added nodes are a lexical succesor of any of these
    /// nodes. Skips any nodes that are already part of the base
    /// layer.
    pub async fn add_nodes<I: 'static + IntoIterator<Item = String> + Send>(
        &mut self,
        nodes: I,
    ) -> io::Result<Vec<u64>>
    where
        <I as std::iter::IntoIterator>::IntoIter: Send,
    {
        // TODO bulk check node existence
        let mut result = Vec::new();
        for node in nodes {
            let id = self.add_node(&node).await?;
            result.push(id);
        }

        Ok(result)
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if
    /// previous added predicates are a lexical succesor of any of
    /// these predicates. Skips any predicates that are already part
    /// of the base layer.
    pub async fn add_predicates<I: 'static + IntoIterator<Item = String> + Send>(
        &mut self,
        predicates: I,
    ) -> io::Result<Vec<u64>>
    where
        <I as std::iter::IntoIterator>::IntoIter: Send,
    {
        // TODO bulk check predicate existence
        let mut result = Vec::new();
        for predicate in predicates {
            let id = self.add_predicate(&predicate).await?;
            result.push(id);
        }

        Ok(result)
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous
    /// added values are a lexical succesor of any of these
    /// values. Skips any nodes that are already part of the base
    /// layer.
    pub async fn add_values<I: 'static + IntoIterator<Item = String> + Send>(
        &mut self,
        values: I,
    ) -> io::Result<Vec<u64>>
    where
        <I as std::iter::IntoIterator>::IntoIter: Send,
    {
        // TODO bulk check predicate existence
        let mut result = Vec::new();
        for value in values {
            let id = self.add_value(&value).await?;
            result.push(id);
        }

        Ok(result)
    }

    /// Turn this builder into a phase 2 builder that will take triple data.
    pub async fn into_phase2(self) -> io::Result<ChildLayerFileBuilderPhase2<F>> {
        let ChildLayerFileBuilder {
            parent,
            files,
            builder,
        } = self;

        builder.finalize().await?;

        let node_dict_blocks_map = files.node_dictionary_files.blocks_file.map().await?;
        let node_dict_offsets_map = files.node_dictionary_files.offsets_file.map().await?;
        let predicate_dict_blocks_map = files.predicate_dictionary_files.blocks_file.map().await?;
        let predicate_dict_offsets_map =
            files.predicate_dictionary_files.offsets_file.map().await?;
        let value_dict_blocks_map = files.value_dictionary_files.blocks_file.map().await?;
        let value_dict_offsets_map = files.value_dictionary_files.offsets_file.map().await?;

        let node_dict = PfcDict::parse(node_dict_blocks_map, node_dict_offsets_map)?;
        let pred_dict = PfcDict::parse(predicate_dict_blocks_map, predicate_dict_offsets_map)?;
        let val_dict = PfcDict::parse(value_dict_blocks_map, value_dict_offsets_map)?;

        // TODO: it is a bit silly to parse the dictionaries just for this. surely we can get the counts in an easier way?
        let num_nodes = node_dict.len();
        let num_predicates = pred_dict.len();
        let num_values = val_dict.len();

        Ok(ChildLayerFileBuilderPhase2::new(
            parent,
            files,
            num_nodes,
            num_predicates,
            num_values,
        ))
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
            Some(files.pos_subjects_file.clone()),
        );

        let neg_builder = TripleFileBuilder::new(
            files.neg_s_p_adjacency_list_files.clone(),
            files.neg_sp_o_adjacency_list_files.clone(),
            num_nodes + parent_counts.node_count,
            num_predicates + parent_counts.predicate_count,
            num_values + parent_counts.value_count,
            Some(files.neg_subjects_file.clone()),
        );

        ChildLayerFileBuilderPhase2 {
            parent,
            files,

            pos_builder,
            neg_builder,
        }
    }

    async fn add_triple_unchecked(
        &mut self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<()> {
        self.pos_builder
            .add_triple(subject, predicate, object)
            .await
    }

    /// Add the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been added,
    /// and do nothing if the triple is already part of the parent.
    pub async fn add_triple(
        &mut self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<()> {
        if !self.parent.triple_exists(subject, predicate, object) {
            self.add_triple_unchecked(subject, predicate, object).await
        } else {
            Ok(())
        }
    }

    async fn remove_triple_unchecked(
        &mut self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<()> {
        self.neg_builder
            .add_triple(subject, predicate, object)
            .await
    }

    /// Remove the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been removed,
    /// and do nothing if the parent doesn't know aobut this triple.
    pub async fn remove_triple(
        &mut self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<()> {
        if self.parent.triple_exists(subject, predicate, object) {
            self.remove_triple_unchecked(subject, predicate, object)
                .await
        } else {
            Ok(())
        }
    }

    /// Add the given triple.
    ///
    /// This will panic if a greater triple has already been added,
    /// and do nothing if the parent already contains this triple.
    pub async fn add_id_triples(&mut self, triples: Vec<IdTriple>) -> io::Result<()> {
        let parent = self.parent.clone();
        let filtered: Vec<_> = triples
            .into_par_iter()
            .filter(move |triple| {
                !parent.triple_exists(triple.subject, triple.predicate, triple.object)
            })
            .collect();

        for triple in filtered {
            self.add_triple_unchecked(triple.subject, triple.predicate, triple.object)
                .await?;
        }

        Ok(())
    }

    /// Remove the given triple.
    ///
    /// This will panic if a greater triple has already been removed,
    /// and do nothing if the parent doesn't know aobut this triple.
    pub async fn remove_id_triples(&mut self, triples: Vec<IdTriple>) -> io::Result<()> {
        let parent = self.parent.clone();
        let filtered: Vec<_> = triples
            .into_par_iter()
            .filter(move |triple| {
                parent.triple_exists(triple.subject, triple.predicate, triple.object)
            })
            .collect();

        for triple in filtered {
            self.remove_triple_unchecked(triple.subject, triple.predicate, triple.object)
                .await?;
        }

        Ok(())
    }

    /// Write the layer data to storage.
    pub async fn finalize(self) -> io::Result<()> {
        let pos_task = tokio::spawn(self.pos_builder.finalize());
        let neg_task = tokio::spawn(self.neg_builder.finalize());

        pos_task.await??;
        neg_task.await??;

        let pos_indexes_task = tokio::spawn(build_indexes(
            self.files.pos_s_p_adjacency_list_files,
            self.files.pos_sp_o_adjacency_list_files,
            self.files.pos_o_ps_adjacency_list_files,
            Some(self.files.pos_objects_file),
            self.files.pos_predicate_wavelet_tree_files,
        ));
        let neg_indexes_task = tokio::spawn(build_indexes(
            self.files.neg_s_p_adjacency_list_files,
            self.files.neg_sp_o_adjacency_list_files,
            self.files.neg_o_ps_adjacency_list_files,
            Some(self.files.neg_objects_file),
            self.files.neg_predicate_wavelet_tree_files,
        ));

        pos_indexes_task.await??;
        neg_indexes_task.await??;

        Ok(())
    }
}

pub struct ChildTripleStream<
    S1: Stream<Item = io::Result<u64>> + Unpin + Send,
    S2: Stream<Item = io::Result<(u64, u64)>> + Unpin + Send,
> {
    subjects_stream: stream::Peekable<S1>,
    s_p_stream: stream::Peekable<S2>,
    sp_o_stream: stream::Peekable<S2>,
    last_mapped_s: u64,
    last_s_p: (u64, u64),
    last_sp: u64,
}

impl<
        S1: Stream<Item = io::Result<u64>> + Unpin + Send,
        S2: Stream<Item = io::Result<(u64, u64)>> + Unpin + Send,
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
        S1: Stream<Item = io::Result<u64>> + Unpin + Send,
        S2: Stream<Item = io::Result<(u64, u64)>> + Unpin + Send,
    > Stream for ChildTripleStream<S1, S2>
{
    type Item = io::Result<(u64, u64, u64)>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<io::Result<(u64, u64, u64)>>> {
        let sp_o = Pin::new(&mut self.sp_o_stream).poll_peek(cx);
        match sp_o {
            Poll::Ready(Some(Ok((sp, o)))) => {
                let sp = *sp;
                let o = *o;
                if sp > self.last_sp {
                    let s_p = Pin::new(&mut self.s_p_stream).poll_peek(cx);
                    match s_p {
                        Poll::Ready(None) => Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected end of s_p_stream",
                        )))),
                        Poll::Ready(Some(Ok((s, p)))) => {
                            let s = *s;
                            let p = *p;
                            if s > self.last_s_p.0 {
                                let mapped_s = Pin::new(&mut self.subjects_stream).poll_peek(cx);
                                match mapped_s {
                                    Poll::Ready(None) => Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "unexpected end of subjects_stream",
                                    )))),
                                    Poll::Ready(Some(Ok(mapped_s))) => {
                                        let mapped_s = *mapped_s;
                                        util::assert_poll_next(
                                            Pin::new(&mut self.subjects_stream),
                                            cx,
                                        )
                                        .unwrap();
                                        util::assert_poll_next(Pin::new(&mut self.s_p_stream), cx)
                                            .unwrap();
                                        util::assert_poll_next(Pin::new(&mut self.sp_o_stream), cx)
                                            .unwrap();
                                        self.last_mapped_s = mapped_s;
                                        self.last_s_p = (s, p);
                                        self.last_sp = sp;

                                        Poll::Ready(Some(Ok((mapped_s, p, o))))
                                    }
                                    Poll::Ready(Some(Err(_))) => {
                                        Poll::Ready(Some(Err(util::assert_poll_next(
                                            Pin::new(&mut self.subjects_stream),
                                            cx,
                                        )
                                        .err()
                                        .unwrap())))
                                    }
                                    Poll::Pending => Poll::Pending,
                                }
                            } else {
                                util::assert_poll_next(Pin::new(&mut self.s_p_stream), cx).unwrap();
                                util::assert_poll_next(Pin::new(&mut self.sp_o_stream), cx)
                                    .unwrap();
                                self.last_s_p = (s, p);
                                self.last_sp = sp;

                                Poll::Ready(Some(Ok((self.last_mapped_s, p, o))))
                            }
                        }
                        Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(
                            util::assert_poll_next(Pin::new(&mut self.s_p_stream), cx)
                                .err()
                                .unwrap(),
                        ))),
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    util::assert_poll_next(Pin::new(&mut self.sp_o_stream), cx).unwrap();
                    Poll::Ready(Some(Ok((self.last_mapped_s, self.last_s_p.1, o))))
                }
            }
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(util::assert_poll_next(
                Pin::new(&mut self.sp_o_stream),
                cx,
            )
            .err()
            .unwrap()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub fn open_child_triple_stream<F: 'static + FileLoad + FileStore>(
    subjects_file: F,
    s_p_files: AdjacencyListFiles<F>,
    sp_o_files: AdjacencyListFiles<F>,
) -> impl Stream<Item = io::Result<(u64, u64, u64)>> + Unpin + Send {
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
    use futures::stream::TryStreamExt;

    pub fn child_layer_files() -> ChildLayerFiles<MemoryBackedStore> {
        // TODO inline
        child_layer_memory_files()
    }

    #[tokio::test]
    async fn empty_child_layer_equivalent_to_parent() {
        let base_layer = example_base_layer().await;

        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let builder = child_builder.into_phase2().await?;
            builder.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        assert!(child_layer.triple_exists(1, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 3));
        assert!(child_layer.triple_exists(2, 3, 6));
        assert!(child_layer.triple_exists(3, 2, 5));
        assert!(child_layer.triple_exists(3, 3, 6));
        assert!(child_layer.triple_exists(4, 3, 6));

        assert!(!child_layer.triple_exists(2, 2, 0));
    }

    #[tokio::test]
    async fn child_layer_can_have_inserts() {
        let base_layer = example_base_layer().await;

        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(2, 1, 2).await?;
            b.add_triple(3, 3, 3).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

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

    #[tokio::test]
    async fn child_layer_can_have_deletes() {
        let base_layer = example_base_layer().await;

        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.remove_triple(2, 1, 1).await?;
            b.remove_triple(3, 2, 5).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        assert!(child_layer.triple_exists(1, 1, 1));
        assert!(!child_layer.triple_exists(2, 1, 1));
        assert!(child_layer.triple_exists(2, 1, 3));
        assert!(child_layer.triple_exists(2, 3, 6));
        assert!(!child_layer.triple_exists(3, 2, 5));
        assert!(child_layer.triple_exists(3, 3, 6));
        assert!(child_layer.triple_exists(4, 3, 6));

        assert!(!child_layer.triple_exists(2, 2, 0));
    }

    #[tokio::test]
    async fn child_layer_can_have_inserts_and_deletes() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(1, 2, 3).await?;
            b.add_triple(2, 3, 4).await?;
            b.remove_triple(3, 2, 5).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

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

    #[tokio::test]
    async fn iterate_child_layer_triples() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(1, 2, 3).await?;
            b.add_triple(2, 3, 4).await?;
            b.remove_triple(3, 2, 5).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

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

    #[tokio::test]
    async fn lookup_child_layer_triples_by_predicate() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(1, 2, 3).await?;
            b.add_triple(2, 3, 4).await?;
            b.remove_triple(3, 2, 5).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        let pairs: Vec<_> = child_layer
            .triples_p(1)
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 1, 1), (2, 1, 1), (2, 1, 3)], pairs);

        let pairs: Vec<_> = child_layer
            .triples_p(2)
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 2, 3)], pairs);

        let pairs: Vec<_> = child_layer
            .triples_p(3)
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 3, 4), (2, 3, 6), (3, 3, 6), (4, 3, 6)], pairs);

        assert!(child_layer.triples_p(4).next().is_none());
    }

    #[tokio::test]
    async fn adding_new_nodes_predicates_and_values_in_child() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(11, 2, 3).await?;
            b.add_triple(12, 3, 4).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        assert!(child_layer.triple_exists(11, 2, 3));
        assert!(child_layer.triple_exists(12, 3, 4));
    }

    #[tokio::test]
    async fn old_dictionary_entries_in_child() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let mut b = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            b.add_node("foo").await?;
            b.add_predicate("bar").await?;
            b.add_value("baz").await?;

            let b = b.into_phase2().await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

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

    #[tokio::test]
    async fn new_dictionary_entries_in_child() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let mut b = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            b.add_node("foo").await?;
            b.add_predicate("bar").await?;
            b.add_value("baz").await?;
            let b = b.into_phase2().await?;

            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

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

    #[tokio::test]
    async fn lookup_additions_by_subject() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(1, 3, 4).await?;
            b.add_triple(2, 2, 2).await?;
            b.add_triple(3, 4, 5).await?;
            b.remove_triple(3, 2, 5).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        let result: Vec<_> = child_layer
            .internal_triple_additions()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 3, 4), (2, 2, 2), (3, 4, 5)], result);
    }

    #[tokio::test]
    async fn lookup_removals_by_subject() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(1, 3, 4).await?;
            b.remove_triple(2, 1, 1).await?;
            b.remove_triple(3, 2, 5).await?;
            b.remove_triple(4, 3, 6).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        let result: Vec<_> = child_layer
            .internal_triple_removals()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 1, 1), (3, 2, 5), (4, 3, 6)], result);
    }

    #[tokio::test]
    async fn create_empty_child_layer() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut b = child_builder.into_phase2().await?;
            b.add_triple(1, 3, 4).await?;
            b.remove_triple(2, 1, 1).await?;
            b.remove_triple(2, 3, 6).await?;
            b.remove_triple(3, 2, 5).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent.clone(), &child_files).await
        };

        let child_layer = fut.await.unwrap();

        assert_eq!(
            parent.node_and_value_count(),
            child_layer.node_and_value_count()
        );
        assert_eq!(parent.predicate_count(), child_layer.predicate_count());
    }

    #[tokio::test]
    async fn stream_child_triples() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let fut = async {
            let mut b = builder.into_phase2().await?;
            b.add_triple(1, 2, 1).await?;
            b.add_triple(3, 1, 5).await?;
            b.add_triple(5, 2, 3).await?;
            b.add_triple(5, 2, 4).await?;
            b.add_triple(5, 2, 5).await?;
            b.add_triple(5, 3, 1).await?;
            b.remove_triple(2, 1, 1).await?;
            b.remove_triple(2, 3, 6).await?;
            b.remove_triple(4, 3, 6).await?;
            b.finalize().await
        };

        fut.await.unwrap();

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

        let addition_triples: Vec<_> = addition_stream.try_collect().await.unwrap();
        let removal_triples: Vec<_> = removal_stream.try_collect().await.unwrap();

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

    #[tokio::test]
    async fn count_triples() {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let fut = async {
            let mut b = builder.into_phase2().await?;
            b.add_triple(1, 2, 1).await?;
            b.add_triple(3, 1, 5).await?;
            b.add_triple(5, 2, 3).await?;
            b.add_triple(5, 2, 4).await?;
            b.add_triple(5, 2, 5).await?;
            b.add_triple(5, 3, 1).await?;
            b.remove_triple(2, 1, 1).await?;
            b.remove_triple(2, 3, 6).await?;
            b.remove_triple(4, 3, 6).await?;
            b.finalize().await?;

            ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files).await
        };

        let child_layer = fut.await.unwrap();

        assert_eq!(6, child_layer.internal_triple_layer_addition_count());
        assert_eq!(3, child_layer.internal_triple_layer_removal_count());
        assert_eq!(13, child_layer.triple_addition_count());
        assert_eq!(3, child_layer.triple_removal_count());
        assert_eq!(10, child_layer.triple_count());
    }
}
