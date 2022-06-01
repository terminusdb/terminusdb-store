pub mod base;
pub mod child;
mod object_iterator;
mod predicate_iterator;
pub mod rollup;
mod subject_iterator;

use super::id_map::*;
use super::layer::*;
use crate::structure::*;

use std::collections::HashSet;
use std::convert::TryInto;

pub use base::*;
pub use child::*;
pub use object_iterator::*;
pub use predicate_iterator::*;
pub use rollup::*;
pub use subject_iterator::*;

#[derive(Clone)]
pub enum InternalLayer {
    Base(BaseLayer),
    Child(ChildLayer),
    Rollup(RollupLayer),
}

use InternalLayer::*;

impl InternalLayer {
    pub fn name(&self) -> [u32; 5] {
        match self {
            Base(base) => base.name,
            Child(child) => child.name,
            Rollup(rollup) => rollup.original,
        }
    }

    pub fn parent_name(&self) -> Option<[u32; 5]> {
        match self {
            Base(_) => None,
            Child(child) => Some(child.parent.name()),
            Rollup(rollup) => rollup.original_parent,
        }
    }

    pub fn immediate_parent(&self) -> Option<&InternalLayer> {
        match self {
            Base(_) => None,
            Child(child) => Some(&*child.parent),
            Rollup(rollup) => rollup.internal.immediate_parent(),
        }
    }

    pub fn layer_stack_size(&self) -> usize {
        let mut count = 1;
        let mut l = self;
        while let Some(p) = l.immediate_parent() {
            l = p;
            count += 1;
        }

        count
    }

    pub fn node_dictionary(&self) -> &PfcDict {
        match self {
            Base(base) => &base.node_dictionary,
            Child(child) => &child.node_dictionary,
            Rollup(rollup) => rollup.internal.node_dictionary(),
        }
    }

    pub fn predicate_dictionary(&self) -> &PfcDict {
        match self {
            Base(base) => &base.predicate_dictionary,
            Child(child) => &child.predicate_dictionary,
            Rollup(rollup) => rollup.internal.predicate_dictionary(),
        }
    }

    pub fn value_dictionary(&self) -> &PfcDict {
        match self {
            Base(base) => &base.value_dictionary,
            Child(child) => &child.value_dictionary,
            Rollup(rollup) => rollup.internal.value_dictionary(),
        }
    }

    pub fn node_value_id_map(&self) -> &IdMap {
        match self {
            Base(base) => &base.node_value_idmap,
            Child(child) => &child.node_value_idmap,
            Rollup(rollup) => rollup.internal.node_value_id_map(),
        }
    }

    pub fn predicate_id_map(&self) -> &IdMap {
        match self {
            Base(base) => &base.predicate_idmap,
            Child(child) => &child.predicate_idmap,
            Rollup(rollup) => rollup.internal.predicate_id_map(),
        }
    }

    pub fn parent_node_value_count(&self) -> usize {
        match self {
            Base(_) => 0,
            Child(child) => child.parent_node_value_count,
            Rollup(rollup) => rollup.internal.parent_node_value_count(),
        }
    }

    pub fn parent_predicate_count(&self) -> usize {
        match self {
            Base(_) => 0,
            Child(child) => child.parent_predicate_count,
            Rollup(rollup) => rollup.internal.parent_predicate_count(),
        }
    }

    pub fn pos_s_p_adjacency_list(&self) -> &AdjacencyList {
        match self {
            Base(base) => &base.s_p_adjacency_list,
            Child(child) => &child.pos_s_p_adjacency_list,
            Rollup(rollup) => rollup.internal.pos_s_p_adjacency_list(),
        }
    }

    pub fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList {
        match self {
            Base(base) => &base.sp_o_adjacency_list,
            Child(child) => &child.pos_sp_o_adjacency_list,
            Rollup(rollup) => rollup.internal.pos_sp_o_adjacency_list(),
        }
    }

    pub fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList {
        match self {
            Base(base) => &base.o_ps_adjacency_list,
            Child(child) => &child.pos_o_ps_adjacency_list,
            Rollup(rollup) => rollup.internal.pos_o_ps_adjacency_list(),
        }
    }

    pub fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_s_p_adjacency_list),
            Rollup(rollup) => rollup.internal.neg_s_p_adjacency_list(),
        }
    }

    pub fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_sp_o_adjacency_list),
            Rollup(rollup) => rollup.internal.neg_sp_o_adjacency_list(),
        }
    }

    pub fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_o_ps_adjacency_list),
            Rollup(rollup) => rollup.internal.neg_o_ps_adjacency_list(),
        }
    }

    pub fn pos_predicate_wavelet_tree(&self) -> &WaveletTree {
        match self {
            Base(base) => &base.predicate_wavelet_tree,
            Child(child) => &child.pos_predicate_wavelet_tree,
            Rollup(rollup) => rollup.internal.pos_predicate_wavelet_tree(),
        }
    }

    pub fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_predicate_wavelet_tree),
            Rollup(rollup) => rollup.internal.neg_predicate_wavelet_tree(),
        }
    }

    pub fn pos_subjects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(base) => base.subjects.as_ref(),
            Child(child) => Some(&child.pos_subjects),
            Rollup(rollup) => rollup.internal.pos_subjects(),
        }
    }

    pub fn pos_objects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(base) => base.objects.as_ref(),
            Child(child) => Some(&child.pos_objects),
            Rollup(rollup) => rollup.internal.pos_objects(),
        }
    }

    pub fn neg_subjects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_subjects),
            Rollup(rollup) => rollup.internal.neg_subjects(),
        }
    }

    pub fn neg_objects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_objects),
            Rollup(rollup) => rollup.internal.neg_objects(),
        }
    }

    pub fn predicate_dict_get(&self, id: usize) -> Option<String> {
        self.predicate_dictionary().get(id)
    }

    pub fn predicate_dict_len(&self) -> usize {
        self.predicate_dictionary().len()
    }

    pub fn predicate_dict_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary().id(predicate)
    }

    pub fn node_dict_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary().id(subject)
    }

    pub fn node_dict_get(&self, id: usize) -> Option<String> {
        self.node_dictionary().get(id)
    }

    pub fn node_dict_len(&self) -> usize {
        self.node_dictionary().len()
    }

    pub fn value_dict_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary().id(value)
    }

    pub fn value_dict_len(&self) -> usize {
        self.value_dictionary().len()
    }

    pub fn value_dict_get(&self, id: usize) -> Option<String> {
        self.value_dictionary().get(id)
    }

    pub fn internal_triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> bool {
        layer_triple_exists(
            self.pos_subjects(),
            self.pos_s_p_adjacency_list(),
            self.pos_sp_o_adjacency_list(),
            subject,
            predicate,
            object,
        )
    }

    pub fn internal_triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> bool {
        match (
            self.neg_subjects(),
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (neg_subject, Some(neg_s_p_adjacency_list), Some(neg_sp_o_adjacency_list)) => {
                layer_triple_exists(
                    neg_subject,
                    neg_s_p_adjacency_list,
                    neg_sp_o_adjacency_list,
                    subject,
                    predicate,
                    object,
                )
            }
            _ => false,
        }
    }

    pub fn internal_triple_additions(&self) -> OptInternalLayerTripleSubjectIterator {
        OptInternalLayerTripleSubjectIterator(Some(InternalLayerTripleSubjectIterator::new(
            self.pos_subjects().cloned(),
            self.pos_s_p_adjacency_list().clone(),
            self.pos_sp_o_adjacency_list().clone(),
        )))
    }

    pub fn internal_triple_removals(&self) -> OptInternalLayerTripleSubjectIterator {
        OptInternalLayerTripleSubjectIterator(
            match (
                self.neg_subjects(),
                self.neg_s_p_adjacency_list(),
                self.neg_sp_o_adjacency_list(),
            ) {
                (neg_subjects, Some(neg_s_p_adjacency_list), Some(neg_sp_o_adjacency_list)) => {
                    Some(InternalLayerTripleSubjectIterator::new(
                        neg_subjects.cloned(),
                        neg_s_p_adjacency_list.clone(),
                        neg_sp_o_adjacency_list.clone(),
                    ))
                }
                _ => None,
            },
        )
    }

    pub fn internal_triple_additions_s(
        &self,
        subject: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    pub fn internal_triple_removals_s(
        &self,
        subject: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    pub fn internal_triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.predicate == predicate && t.subject == subject),
        )
    }

    pub fn internal_triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.predicate == predicate && t.subject == subject),
        )
    }

    pub fn internal_triple_additions_p(
        &self,
        predicate: u64,
    ) -> OptInternalLayerTriplePredicateIterator {
        match self.pos_predicate_wavelet_tree().lookup(predicate) {
            Some(lookup) => OptInternalLayerTriplePredicateIterator(Some(
                InternalLayerTriplePredicateIterator::new(
                    lookup,
                    self.pos_subjects().cloned(),
                    self.pos_s_p_adjacency_list().clone(),
                    self.pos_sp_o_adjacency_list().clone(),
                ),
            )),
            None => OptInternalLayerTriplePredicateIterator(None),
        }
    }

    pub fn internal_triple_removals_p(
        &self,
        predicate: u64,
    ) -> OptInternalLayerTriplePredicateIterator {
        match (
            self.neg_predicate_wavelet_tree()
                .and_then(|t| t.lookup(predicate)),
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (Some(lookup), Some(s_p_adjacency_list), Some(sp_o_adjacency_list)) => {
                OptInternalLayerTriplePredicateIterator(Some(
                    InternalLayerTriplePredicateIterator::new(
                        lookup,
                        self.neg_subjects().cloned(),
                        s_p_adjacency_list.clone(),
                        sp_o_adjacency_list.clone(),
                    ),
                ))
            }
            _ => OptInternalLayerTriplePredicateIterator(None),
        }
    }

    pub fn internal_triple_additions_o(
        &self,
        object: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions_by_object()
                .seek_object(object)
                .take_while(move |t| t.object == object),
        )
    }

    pub fn internal_triple_additions_by_object(&self) -> OptInternalLayerTripleObjectIterator {
        OptInternalLayerTripleObjectIterator(Some(InternalLayerTripleObjectIterator::new(
            self.pos_subjects().cloned(),
            self.pos_objects().cloned(),
            self.pos_o_ps_adjacency_list().clone(),
            self.pos_s_p_adjacency_list().clone(),
        )))
    }

    pub fn internal_triple_removals_o(
        &self,
        object: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals_by_object()
                .seek_object(object)
                .take_while(move |t| t.object == object),
        )
    }

    pub fn internal_triple_removals_by_object(&self) -> OptInternalLayerTripleObjectIterator {
        OptInternalLayerTripleObjectIterator(
            match (
                self.neg_subjects(),
                self.neg_objects(),
                self.neg_o_ps_adjacency_list(),
                self.neg_s_p_adjacency_list(),
            ) {
                (
                    neg_subjects,
                    neg_objects,
                    Some(neg_o_ps_adjacency_list),
                    Some(neg_s_p_adjacency_list),
                ) => Some(InternalLayerTripleObjectIterator::new(
                    neg_subjects.cloned(),
                    neg_objects.cloned(),
                    neg_o_ps_adjacency_list.clone(),
                    neg_s_p_adjacency_list.clone(),
                )),
                _ => None,
            },
        )
    }

    pub fn internal_triple_layer_addition_count(&self) -> usize {
        self.pos_sp_o_adjacency_list().right_count()
            - self
                .pos_predicate_wavelet_tree()
                .lookup(0)
                .map(|l| l.len())
                .unwrap_or(0)
    }

    pub fn internal_triple_layer_removal_count(&self) -> usize {
        match self.neg_sp_o_adjacency_list() {
            None => 0,
            Some(adjacency_list) => adjacency_list.right_count()
                - self.neg_predicate_wavelet_tree().expect("negative wavelet tree should exist when negative sp_o adjacency list exists")
                .lookup(0).map(|l|l.len()).unwrap_or(0)
        }
    }

    pub fn immediate_layers(&self) -> Vec<&InternalLayer> {
        let mut layer = Some(self);
        let mut result = Vec::new();

        while let Some(l) = layer {
            result.push(l);

            layer = l.immediate_parent();
        }

        result.reverse();

        result
    }

    pub fn immediate_layers_upto(&self, upto_layer_id: [u32; 5]) -> Vec<&InternalLayer> {
        if self.name() == upto_layer_id {
            panic!("tried to retrieve layers up to a boundary, but boundary was the top layer");
        }

        let mut layer = Some(self);
        let mut result = Vec::new();

        while let Some(l) = layer {
            if l.name() == upto_layer_id {
                break;
            }
            result.push(l);

            layer = l.immediate_parent();
        }

        if layer.is_none() {
            // we went through the whole stack and we did not find the boundary.
            panic!("tried to find all layers up to a boundary, but boundary was not found");
        }

        result.reverse();

        result
    }

    pub fn is_rollup(&self) -> bool {
        match self {
            Rollup(_) => true,
            _ => false,
        }
    }
}

impl Layer for InternalLayer {
    fn name(&self) -> [u32; 5] {
        self.name()
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.parent_name()
    }

    fn node_and_value_count(&self) -> usize {
        self.parent_node_value_count()
            + self.node_dictionary().len()
            + self.value_dictionary().len()
    }

    fn predicate_count(&self) -> usize {
        self.parent_predicate_count() + self.predicate_dictionary().len()
    }

    fn subject_id<'a>(&'a self, subject: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer
                    .node_dict_id(subject)
                    .map(|id| layer.node_value_id_map().inner_to_outer(id)),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn predicate_id<'a>(&'a self, predicate: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer
                    .predicate_dict_id(predicate)
                    .map(|id| layer.predicate_id_map().inner_to_outer(id)),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.predicate_count() as u64))
    }

    fn object_node_id<'a>(&'a self, object: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer
                    .node_dict_id(object)
                    .map(|id| layer.node_value_id_map().inner_to_outer(id)),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn object_value_id<'a>(&'a self, object: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer.value_dict_id(object).map(|i| {
                    layer
                        .node_value_id_map()
                        .inner_to_outer(i + layer.node_dict_len() as u64)
                }),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;
                if corrected_id >= parent_count as u64 {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            return current_layer.node_dict_get(
                current_layer
                    .node_value_id_map()
                    .outer_to_inner(corrected_id)
                    .try_into()
                    .unwrap(),
            );
        }

        None
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.predicate_count() as u64;
        while let Some(current_layer) = current_option {
            let mut corrected_id = id - 1;
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count -= current_layer.predicate_dict_len() as u64;
                if corrected_id >= parent_count as u64 {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            return current_layer.predicate_dict_get(
                current_layer
                    .predicate_id_map()
                    .outer_to_inner(corrected_id)
                    .try_into()
                    .unwrap(),
            );
        }

        None
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;

                if corrected_id >= parent_count {
                    // object, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            corrected_id = current_layer
                .node_value_id_map()
                .outer_to_inner(corrected_id);

            if corrected_id >= current_layer.node_dict_len() as u64 {
                // object, if it exists, must be a value
                corrected_id -= current_layer.node_dict_len() as u64;
                return current_layer
                    .value_dict_get(corrected_id.try_into().unwrap())
                    .map(ObjectType::Value);
            } else {
                return current_layer
                    .node_dict_get(corrected_id.try_into().unwrap())
                    .map(ObjectType::Node);
            }
        }

        None
    }

    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        if id == 0 {
            return None;
        }

        let mut corrected_id = id - 1;
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;

                if corrected_id >= parent_count {
                    // object, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            corrected_id = current_layer
                .node_value_id_map()
                .outer_to_inner(corrected_id);

            return Some(corrected_id < current_layer.node_dict_len() as u64);
        }

        None
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_addition_count(&self) -> usize {
        let mut additions = self.internal_triple_layer_addition_count();

        let mut parent = self.immediate_parent();
        while parent.is_some() {
            additions += parent.unwrap().internal_triple_layer_addition_count();

            parent = parent.unwrap().immediate_parent();
        }

        additions
    }

    fn triple_removal_count(&self) -> usize {
        let mut removals = self.internal_triple_layer_removal_count();

        let mut parent = self.immediate_parent();
        while parent.is_some() {
            removals += parent.unwrap().internal_triple_layer_removal_count();

            parent = parent.unwrap().immediate_parent();
        }

        removals
    }

    fn all_counts(&self) -> LayerCounts {
        let mut node_count = self.node_dict_len();
        let mut predicate_count = self.predicate_dict_len();
        let mut value_count = self.value_dict_len();
        let mut parent_option = self.immediate_parent();
        while let Some(parent) = parent_option {
            node_count += parent.node_dict_len();
            predicate_count += parent.predicate_dict_len();
            value_count += parent.value_dict_len();
            parent_option = parent.immediate_parent();
        }
        LayerCounts {
            node_count,
            predicate_count,
            value_count,
        }
    }

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        if subject == 0 || predicate == 0 || object == 0 {
            return false;
        }

        if self.internal_triple_addition_exists(subject, predicate, object) {
            true
        } else if self.internal_triple_removal_exists(subject, predicate, object) {
            false
        } else {
            let mut parent_opt = self.immediate_parent();
            while parent_opt.is_some() {
                let parent = parent_opt.unwrap();
                if parent.internal_triple_addition_exists(subject, predicate, object) {
                    return true;
                } else if parent.internal_triple_removal_exists(subject, predicate, object) {
                    return false;
                }

                parent_opt = parent.immediate_parent();
            }

            false
        }
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(InternalTripleSubjectIterator::from_layer(self))
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            InternalTripleSubjectIterator::from_layer(self)
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            InternalTripleSubjectIterator::from_layer(self)
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.subject == subject && t.predicate == predicate),
        )
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(InternalTriplePredicateIterator::from_layer(self, predicate))
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            InternalTripleObjectIterator::from_layer(self)
                .seek_object(object)
                .take_while(move |t| t.object == object),
        )
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        // is subject/predicate in the positives? we're in luck
        if let Some(pos) = sp_o_position(
            self.pos_subjects(),
            self.pos_s_p_adjacency_list(),
            self.pos_sp_o_adjacency_list(),
            subject,
            predicate,
        ) {
            return Some(IdTriple {
                subject,
                predicate,
                object: self.pos_sp_o_adjacency_list().num_at_pos(pos),
            });
        }

        // alas, it's not in there.
        let mut exclude = HashSet::new();
        let mut l = self;
        loop {
            if let Some(parent) = l.immediate_parent() {
                if let Some(neg_sp_o_adjacency_list) = l.neg_sp_o_adjacency_list() {
                    if let Some(mut pos) = sp_o_position(
                        l.neg_subjects(),
                        l.neg_s_p_adjacency_list().unwrap(),
                        neg_sp_o_adjacency_list,
                        subject,
                        predicate,
                    ) {
                        loop {
                            exclude.insert(neg_sp_o_adjacency_list.num_at_pos(pos));
                            if neg_sp_o_adjacency_list.bit_at_pos(pos) {
                                break;
                            }
                            pos += 1;
                        }
                    }
                }

                let pos_sp_o_adjacency_list = parent.pos_sp_o_adjacency_list();
                if let Some(mut pos) = sp_o_position(
                    parent.pos_subjects(),
                    parent.pos_s_p_adjacency_list(),
                    pos_sp_o_adjacency_list,
                    subject,
                    predicate,
                ) {
                    // we need to iterate through the positives until we find an element that is not excluded
                    loop {
                        let num = pos_sp_o_adjacency_list.num_at_pos(pos);
                        if !exclude.contains(&num) {
                            return Some(IdTriple {
                                subject,
                                predicate,
                                object: num,
                            });
                        }

                        if pos_sp_o_adjacency_list.bit_at_pos(pos) {
                            break;
                        }
                        pos += 1;
                    }
                }

                l = parent;
            } else {
                return None;
            }
        }
    }
}

impl From<BaseLayer> for InternalLayer {
    fn from(layer: BaseLayer) -> InternalLayer {
        InternalLayer::Base(layer)
    }
}

impl From<ChildLayer> for InternalLayer {
    fn from(layer: ChildLayer) -> InternalLayer {
        InternalLayer::Child(layer)
    }
}

impl From<RollupLayer> for InternalLayer {
    fn from(layer: RollupLayer) -> InternalLayer {
        InternalLayer::Rollup(layer)
    }
}

fn sp_o_position(
    subjects: Option<&MonotonicLogArray>,
    s_p_adjacency_list: &AdjacencyList,
    sp_o_adjacency_list: &AdjacencyList,
    subject: u64,
    predicate: u64,
) -> Option<u64> {
    if subject == 0 || predicate == 0 {
        return None;
    }

    let s_position = match subjects {
        None => {
            if subject > s_p_adjacency_list.left_count() as u64 {
                return None;
            } else {
                subject - 1
            }
        }
        Some(subjects) => match subjects.index_of(subject) {
            Some(pos) => pos as u64,
            None => return None,
        },
    };

    let mut s_p_position = s_p_adjacency_list.offset_for(s_position + 1);
    loop {
        let bit = s_p_adjacency_list.bit_at_pos(s_p_position);
        if s_p_adjacency_list.num_at_pos(s_p_position) == predicate {
            break;
        }

        if bit {
            // moved past the end for this subject. triple isn't here.
            return None;
        }

        s_p_position += 1;
    }

    Some(sp_o_adjacency_list.offset_for(s_p_position + 1))
}

pub(crate) fn layer_triple_exists(
    subjects: Option<&MonotonicLogArray>,
    s_p_adjacency_list: &AdjacencyList,
    sp_o_adjacency_list: &AdjacencyList,
    subject: u64,
    predicate: u64,
    object: u64,
) -> bool {
    if object == 0 {
        return false;
    }

    let mut sp_o_position = match sp_o_position(
        subjects,
        s_p_adjacency_list,
        sp_o_adjacency_list,
        subject,
        predicate,
    ) {
        Some(p) => p,
        None => return false,
    };

    loop {
        let bit = sp_o_adjacency_list.bit_at_pos(sp_o_position);
        if sp_o_adjacency_list.num_at_pos(sp_o_position) == object {
            // yay we found it
            return true;
        }

        if bit {
            // moved past the end for this subject-predicate pair. triple isn't here.
            break;
        }

        sp_o_position += 1;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_sync_memory_store;
    use crate::store::sync::*;

    fn create_base_layer(store: &SyncStore) -> SyncStoreLayer {
        let builder = store.create_base_layer().unwrap();

        builder
            .add_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_string_triple(StringTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_string_triple(StringTriple::new_value("duck", "says", "quack"))
            .unwrap();

        builder.commit().unwrap()
    }

    #[test]
    fn base_layer_addition_count() {
        let store = open_sync_memory_store();

        let layer = create_base_layer(&store);

        assert_eq!(3, layer.triple_layer_addition_count().unwrap());
    }

    #[test]
    fn child_layer_addition_removal_count() {
        let store = open_sync_memory_store();
        let base_layer = create_base_layer(&store);
        let builder = base_layer.open_write().unwrap();

        builder
            .remove_string_triple(StringTriple::new_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_string_triple(StringTriple::new_value("horse", "says", "neigh"))
            .unwrap();

        let layer = builder.commit().unwrap();

        assert_eq!(1, layer.triple_layer_addition_count().unwrap());
        assert_eq!(1, layer.triple_layer_removal_count().unwrap());
    }

    use crate::layer::base::tests::*;
    #[tokio::test]
    async fn base_layer_with_gaps_addition_count() {
        let files = base_layer_files();

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let mut builder = BaseLayerFileBuilder::from_files(&files).await.unwrap();
        builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .await
            .unwrap();
        builder
            .add_predicates(predicates.into_iter().map(|s| s.to_string()))
            .await
            .unwrap();
        builder
            .add_values(values.into_iter().map(|s| s.to_string()))
            .await
            .unwrap();
        let mut builder = builder.into_phase2().await.unwrap();
        builder.add_triple(3, 3, 3).await.unwrap();
        builder.finalize().await.unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .await
            .unwrap();

        assert_eq!(1, layer.internal_triple_layer_addition_count());
    }
}
