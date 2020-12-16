use super::*;
use std::sync::Arc;

use crate::structure::*;

#[derive(Clone)]
pub struct RollupLayer {
    internal: Arc<InternalLayer>,
    original: [u32; 5],
    original_parent: Option<[u32; 5]>, // TODO something with a light delta structure for answering delta queries?
}

impl InternalLayerImpl for RollupLayer {
    fn name(&self) -> [u32; 5] {
        self.original
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.original_parent
    }

    fn immediate_parent(&self) -> Option<&InternalLayer> {
        self.internal.immediate_parent()
    }

    fn node_dictionary(&self) -> &PfcDict {
        self.internal.node_dictionary()
    }
    fn predicate_dictionary(&self) -> &PfcDict {
        self.internal.predicate_dictionary()
    }
    fn value_dictionary(&self) -> &PfcDict {
        self.internal.value_dictionary()
    }

    fn node_value_id_map(&self) -> &IdMap {
        self.internal.node_value_id_map()
    }

    fn predicate_id_map(&self) -> &IdMap {
        self.internal.predicate_id_map()
    }

    fn parent_node_value_count(&self) -> usize {
        self.internal.parent_node_value_count()
    }

    fn parent_predicate_count(&self) -> usize {
        self.internal.parent_predicate_count()
    }

    fn pos_s_p_adjacency_list(&self) -> &AdjacencyList {
        self.internal.pos_s_p_adjacency_list()
    }

    fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList {
        self.internal.pos_sp_o_adjacency_list()
    }

    fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList {
        self.internal.pos_o_ps_adjacency_list()
    }

    fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList> {
        self.internal.neg_s_p_adjacency_list()
    }

    fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList> {
        self.internal.neg_sp_o_adjacency_list()
    }

    fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList> {
        self.internal.neg_o_ps_adjacency_list()
    }

    fn pos_predicate_wavelet_tree(&self) -> &WaveletTree {
        self.internal.pos_predicate_wavelet_tree()
    }

    fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree> {
        self.internal.neg_predicate_wavelet_tree()
    }

    fn pos_subjects(&self) -> Option<&MonotonicLogArray> {
        self.internal.pos_subjects()
    }

    fn pos_objects(&self) -> Option<&MonotonicLogArray> {
        self.internal.pos_objects()
    }

    fn neg_subjects(&self) -> Option<&MonotonicLogArray> {
        self.internal.neg_subjects()
    }

    fn neg_objects(&self) -> Option<&MonotonicLogArray> {
        self.internal.neg_objects()
    }
}
