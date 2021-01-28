pub mod base;
pub mod child;
mod object_iterator;
mod predicate_iterator;
pub mod rollup;
mod subject_iterator;

use super::id_map::*;
use super::layer::*;
use crate::structure::*;
use std::convert::TryInto;
use std::ops::Deref;

pub use base::*;
pub use child::*;
pub use object_iterator::*;
pub use predicate_iterator::*;
pub use rollup::*;
pub use subject_iterator::*;

/*
fn external_id_to_internal(array_option: Option<&MonotonicLogArray>, id: u64) -> Option<u64> {
    if id == 0 {
        return None;
    }

    match array_option {
        Some(array) => array.index_of(id).map(|mapped| mapped as u64 + 1),
        None => Some(id),
    }
}

fn internal_id_to_external(array_option: Option<&MonotonicLogArray>, id: u64) -> u64 {
    match array_option {
        Some(array) => array.entry((id - 1).try_into().unwrap()),
        None => id,
    }
}

fn id_iter(
    array_option: Option<&MonotonicLogArray>,
    adjacency_list_option: Option<&AdjacencyList>,
) -> Box<dyn Iterator<Item = u64>> {
    match (array_option, adjacency_list_option) {
        (Some(array), _) => Box::new(array.iter()),
        (_, Some(adjacency_list)) => Box::new(1..(adjacency_list.left_count() as u64 + 1)),
        _ => Box::new(std::iter::empty()),
    }
}
*/

pub trait InternalLayerImpl {
    fn name(&self) -> [u32; 5];
    fn parent_name(&self) -> Option<[u32; 5]>;
    fn immediate_parent(&self) -> Option<&InternalLayer>;

    fn node_dictionary(&self) -> &PfcDict;
    fn predicate_dictionary(&self) -> &PfcDict;
    fn value_dictionary(&self) -> &PfcDict;

    fn node_value_id_map(&self) -> &IdMap;
    fn predicate_id_map(&self) -> &IdMap;

    fn parent_node_value_count(&self) -> usize;
    fn parent_predicate_count(&self) -> usize;

    fn pos_s_p_adjacency_list(&self) -> &AdjacencyList;
    fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList;
    fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList;

    fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList>;
    fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList>;
    fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList>;

    fn pos_predicate_wavelet_tree(&self) -> &WaveletTree;
    fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree>;

    fn pos_subjects(&self) -> Option<&MonotonicLogArray>;
    fn pos_objects(&self) -> Option<&MonotonicLogArray>;
    fn neg_subjects(&self) -> Option<&MonotonicLogArray>;
    fn neg_objects(&self) -> Option<&MonotonicLogArray>;

    fn predicate_dict_get(&self, id: usize) -> Option<String> {
        self.predicate_dictionary().get(id)
    }

    fn predicate_dict_len(&self) -> usize {
        self.predicate_dictionary().len()
    }

    fn predicate_dict_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary().id(predicate)
    }

    fn node_dict_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary().id(subject)
    }

    fn node_dict_get(&self, id: usize) -> Option<String> {
        self.node_dictionary().get(id)
    }

    fn node_dict_len(&self) -> usize {
        self.node_dictionary().len()
    }

    fn value_dict_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary().id(value)
    }

    fn value_dict_len(&self) -> usize {
        self.value_dictionary().len()
    }

    fn value_dict_get(&self, id: usize) -> Option<String> {
        self.value_dictionary().get(id)
    }

    fn node_dict_entries_zero_index(&self) -> Box<dyn Iterator<Item = (u64, PfcDictEntry)> + Send> {
        let parent_node_value_count = self.parent_node_value_count();
        let node_value_id_map = self.node_value_id_map().clone();
        Box::new(
            self.node_dictionary()
                .entries()
                .enumerate()
                .map(move |(i, e)| {
                    (
                        node_value_id_map.inner_to_outer(i as u64) + parent_node_value_count as u64,
                        e,
                    )
                }),
        )
    }

    fn value_dict_entries_zero_index(
        &self,
    ) -> Box<dyn Iterator<Item = (u64, PfcDictEntry)> + Send> {
        let parent_node_value_count = self.parent_node_value_count();
        let node_count = self.node_dict_len();
        let node_value_id_map = self.node_value_id_map().clone();
        Box::new(
            self.value_dictionary()
                .entries()
                .enumerate()
                .map(move |(i, e)| {
                    (
                        node_value_id_map.inner_to_outer((i + node_count) as u64)
                            + parent_node_value_count as u64,
                        e,
                    )
                }),
        )
    }

    fn predicate_dict_entries_zero_index(
        &self,
    ) -> Box<dyn Iterator<Item = (u64, PfcDictEntry)> + Send> {
        let parent_predicate_count = self.parent_predicate_count();
        let predicate_id_map = self.predicate_id_map().clone();
        Box::new(
            self.predicate_dictionary()
                .entries()
                .enumerate()
                .map(move |(i, e)| {
                    (
                        predicate_id_map.inner_to_outer(i as u64) + parent_predicate_count as u64,
                        e,
                    )
                }),
        )
    }

    fn internal_triple_addition_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        layer_triple_exists(
            self.pos_subjects(),
            self.pos_s_p_adjacency_list(),
            self.pos_sp_o_adjacency_list(),
            subject,
            predicate,
            object,
        )
    }

    fn internal_triple_removal_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
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

    fn internal_triple_additions(&self) -> OptInternalLayerTripleSubjectIterator {
        OptInternalLayerTripleSubjectIterator(Some(InternalLayerTripleSubjectIterator::new(
            self.pos_subjects().cloned(),
            self.pos_s_p_adjacency_list().clone(),
            self.pos_sp_o_adjacency_list().clone(),
        )))
    }

    fn internal_triple_removals(&self) -> OptInternalLayerTripleSubjectIterator {
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

    fn internal_triple_additions_s(
        &self,
        subject: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn internal_triple_removals_s(
        &self,
        subject: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn internal_triple_additions_sp(
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

    fn internal_triple_removals_sp(
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

    fn internal_triple_additions_p(
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

    fn internal_triple_removals_p(
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

    fn internal_triple_additions_o(
        &self,
        object: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions_by_object()
                .seek_object(object)
                .take_while(move |t| t.object == object),
        )
    }

    fn internal_triple_additions_by_object(&self) -> OptInternalLayerTripleObjectIterator {
        OptInternalLayerTripleObjectIterator(Some(InternalLayerTripleObjectIterator::new(
            self.pos_subjects().cloned(),
            self.pos_objects().cloned(),
            self.pos_o_ps_adjacency_list().clone(),
            self.pos_s_p_adjacency_list().clone(),
        )))
    }

    fn internal_triple_removals_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals_by_object()
                .seek_object(object)
                .take_while(move |t| t.object == object),
        )
    }

    fn internal_triple_removals_by_object(&self) -> OptInternalLayerTripleObjectIterator {
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

    fn internal_triple_layer_addition_count(&self) -> usize {
        self.pos_sp_o_adjacency_list().right_count()
            - self
                .pos_predicate_wavelet_tree()
                .lookup(0)
                .map(|l| l.len())
                .unwrap_or(0)
    }

    fn internal_triple_layer_removal_count(&self) -> usize {
        match self.neg_sp_o_adjacency_list() {
            None => 0,
            Some(adjacency_list) => adjacency_list.right_count()
                - self.neg_predicate_wavelet_tree().expect("negative wavelet tree should exist when negative sp_o adjacency list exists")
                .lookup(0).map(|l|l.len()).unwrap_or(0)
        }
    }
}

impl<T: 'static + InternalLayerImpl + Send + Sync + Clone> Layer for T {
    fn name(&self) -> [u32; 5] {
        Self::name(self)
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        Self::parent_name(self)
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
        let to_result = |layer: &'a dyn InternalLayerImpl| {
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
        let to_result = |layer: &'a dyn InternalLayerImpl| {
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
        let to_result = |layer: &'a dyn InternalLayerImpl| {
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
        let to_result = |layer: &'a dyn InternalLayerImpl| {
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
        let mut current_option: Option<&dyn InternalLayerImpl> = Some(self);
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
        let mut current_option: Option<&dyn InternalLayerImpl> = Some(self);
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
        let mut current_option: Option<&dyn InternalLayerImpl> = Some(self);
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
}

#[derive(Clone)]
pub enum InternalLayer {
    Base(BaseLayer),
    Child(ChildLayer),
    Rollup(RollupLayer),
}

impl InternalLayer {
    pub fn as_layer(&self) -> &dyn Layer {
        match self {
            Self::Base(base) => base as &dyn Layer,
            Self::Child(child) => child as &dyn Layer,
            Self::Rollup(rollup) => rollup as &dyn Layer,
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
        if InternalLayerImpl::name(self) == upto_layer_id {
            panic!("tried to retrieve layers up to a boundary, but boundary was the top layer");
        }

        let mut layer = Some(self);
        let mut result = Vec::new();

        while let Some(l) = layer {
            if InternalLayerImpl::name(l) == upto_layer_id {
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
            Self::Rollup(_) => true,
            _ => false,
        }
    }
}

impl Deref for InternalLayer {
    type Target = dyn InternalLayerImpl + Send + Sync;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Base(base) => base as &Self::Target,
            Self::Child(child) => child as &Self::Target,
            Self::Rollup(rollup) => rollup as &Self::Target,
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

impl InternalLayerImpl for InternalLayer {
    fn name(&self) -> [u32; 5] {
        InternalLayerImpl::name(&**self)
    }
    fn parent_name(&self) -> Option<[u32; 5]> {
        (&**self).parent_name()
    }
    fn immediate_parent(&self) -> Option<&InternalLayer> {
        (&**self).immediate_parent()
    }

    fn node_dictionary(&self) -> &PfcDict {
        (&**self).node_dictionary()
    }
    fn predicate_dictionary(&self) -> &PfcDict {
        (&**self).predicate_dictionary()
    }
    fn value_dictionary(&self) -> &PfcDict {
        (&**self).value_dictionary()
    }

    fn node_value_id_map(&self) -> &IdMap {
        (&**self).node_value_id_map()
    }

    fn predicate_id_map(&self) -> &IdMap {
        (&**self).predicate_id_map()
    }

    fn parent_node_value_count(&self) -> usize {
        (&**self).parent_node_value_count()
    }

    fn parent_predicate_count(&self) -> usize {
        (&**self).parent_predicate_count()
    }

    fn pos_s_p_adjacency_list(&self) -> &AdjacencyList {
        (&**self).pos_s_p_adjacency_list()
    }
    fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList {
        (&**self).pos_sp_o_adjacency_list()
    }
    fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList {
        (&**self).pos_o_ps_adjacency_list()
    }

    fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList> {
        (&**self).neg_s_p_adjacency_list()
    }
    fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList> {
        (&**self).neg_sp_o_adjacency_list()
    }
    fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList> {
        (&**self).neg_o_ps_adjacency_list()
    }

    fn pos_predicate_wavelet_tree(&self) -> &WaveletTree {
        (&**self).pos_predicate_wavelet_tree()
    }
    fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree> {
        (&**self).neg_predicate_wavelet_tree()
    }

    fn pos_subjects(&self) -> Option<&MonotonicLogArray> {
        (&**self).pos_subjects()
    }
    fn pos_objects(&self) -> Option<&MonotonicLogArray> {
        (&**self).pos_objects()
    }
    fn neg_subjects(&self) -> Option<&MonotonicLogArray> {
        (&**self).neg_subjects()
    }
    fn neg_objects(&self) -> Option<&MonotonicLogArray> {
        (&**self).neg_objects()
    }
}

pub(crate) fn layer_triple_exists(
    subjects: Option<&MonotonicLogArray>,
    s_p_adjacency_list: &AdjacencyList,
    sp_o_adjacency_list: &AdjacencyList,
    subject: u64,
    predicate: u64,
    object: u64,
) -> bool {
    if subject == 0 || predicate == 0 || object == 0 {
        return false;
    }

    let s_position = match subjects.as_ref() {
        None => {
            if subject > s_p_adjacency_list.left_count() as u64 {
                return false;
            }

            subject - 1
        }
        Some(subjects) => match subjects.index_of(subject) {
            Some(pos) => pos as u64,
            None => return false,
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
            return false;
        }

        s_p_position += 1;
    }

    let mut sp_o_position = sp_o_adjacency_list.offset_for(s_p_position + 1);
    loop {
        let bit = sp_o_adjacency_list.bit_at_pos(sp_o_position);
        if sp_o_adjacency_list.num_at_pos(sp_o_position) == object {
            // yay we found it
            return true;
        }

        if bit {
            // moved past the end for this subject. triple isn't here.
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

        let mut builder = BaseLayerFileBuilder::from_files(&files);
        let future = async {
            builder
                .add_nodes(nodes.into_iter().map(|s| s.to_string()))
                .await?;
            builder
                .add_predicates(predicates.into_iter().map(|s| s.to_string()))
                .await?;
            builder
                .add_values(values.into_iter().map(|s| s.to_string()))
                .await?;
            let mut builder = builder.into_phase2().await?;
            builder.add_triple(3, 3, 3).await?;
            builder.finalize().await
        };

        future.await.unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .await
            .unwrap();

        assert_eq!(1, layer.internal_triple_layer_addition_count());
    }
}
