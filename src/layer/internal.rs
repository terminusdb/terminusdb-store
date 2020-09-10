use super::base::*;
use super::child::*;
use super::layer::*;
use crate::structure::*;
use std::convert::TryInto;
use std::ops::Deref;

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
        Some(array) => array.entry((id - 1) as usize),
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

pub trait InternalLayerImpl {
    fn name(&self) -> [u32; 5];
    fn parent_name(&self) -> Option<[u32; 5]>;
    fn layer_type(&self) -> LayerType;
    fn immediate_parent(&self) -> Option<&InternalLayer>;

    fn node_dictionary(&self) -> &PfcDict;
    fn predicate_dictionary(&self) -> &PfcDict;
    fn value_dictionary(&self) -> &PfcDict;

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

    fn internal_triple_additions(&self) -> OptInternalLayerTripleIterator {
        OptInternalLayerTripleIterator(Some(InternalLayerTripleIterator::new(
            self.pos_subjects(),
            self.pos_s_p_adjacency_list(),
            self.pos_sp_o_adjacency_list(),
        )))
    }

    fn internal_triple_removals(&self) -> OptInternalLayerTripleIterator {
        OptInternalLayerTripleIterator(
            match (
                self.neg_subjects(),
                self.neg_s_p_adjacency_list(),
                self.neg_sp_o_adjacency_list(),
            ) {
                (neg_subjects, Some(neg_s_p_adjacency_list), Some(neg_sp_o_adjacency_list)) => {
                    Some(InternalLayerTripleIterator::new(
                        neg_subjects,
                        neg_s_p_adjacency_list,
                        neg_sp_o_adjacency_list,
                    ))
                }
                _ => None,
            },
        )
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
        let mut parent_option = self.immediate_parent();
        let mut count = self.node_dictionary().len() + self.value_dictionary().len();
        while let Some(parent) = parent_option {
            count += parent.node_dict_len() + parent.value_dict_len();
            parent_option = parent.immediate_parent();
        }
        count
    }

    fn predicate_count(&self) -> usize {
        let mut parent_option = self.immediate_parent();
        let mut count = self.predicate_dictionary().len();
        while let Some(parent) = parent_option {
            count += parent.predicate_dict_len();
            parent_option = parent.immediate_parent();
        }
        count
    }

    fn subject_id<'a>(&'a self, subject: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn InternalLayerImpl| {
            (layer.node_dict_id(subject), layer.immediate_parent())
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
            (layer.predicate_dict_id(predicate), layer.immediate_parent())
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
            (layer.node_dict_id(object), layer.immediate_parent())
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
                layer
                    .value_dict_id(object)
                    .map(|i| i + layer.node_dict_len() as u64),
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

            return current_layer.node_dict_get(corrected_id as usize);
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
                parent_count = parent_count - current_layer.predicate_dict_len() as u64;
                if corrected_id >= parent_count as u64 {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            return current_layer.predicate_dict_get(corrected_id as usize);
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

            if corrected_id >= current_layer.node_dict_len() as u64 {
                // object, if it exists, must be a value
                corrected_id -= current_layer.node_dict_len() as u64;
                return current_layer
                    .value_dict_get(corrected_id as usize)
                    .map(ObjectType::Value);
            } else {
                return current_layer
                    .node_dict_get(corrected_id as usize)
                    .map(ObjectType::Node);
            }
        }

        None
    }

    fn subjects(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectLookup>>> {
        let mut layers = Vec::new();
        layers.push((
            self.subject_additions().peekable(),
            self.subject_removals().peekable(),
        ));
        let mut cur = self.immediate_parent();

        while cur.is_some() {
            layers.push((
                cur.unwrap().subject_additions().peekable(),
                cur.unwrap().subject_removals().peekable(),
            ));
            cur = cur.unwrap().immediate_parent();
        }

        let it = GenericSubjectIterator { layers };

        Box::new(it.map(|s| Box::new(s) as Box<dyn SubjectLookup>))
    }

    fn subject_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>> {
        let s_p_adjacency_list = self.pos_s_p_adjacency_list().clone();
        let sp_o_adjacency_list = self.pos_sp_o_adjacency_list().clone();

        Box::new(
            id_iter(self.pos_subjects(), Some(&s_p_adjacency_list))
                .enumerate()
                .filter_map(move |(c, s)| {
                    let predicates = s_p_adjacency_list.get((c as u64) + 1);
                    if predicates.len() == 1 && predicates.entry(0) == 0 {
                        None
                    } else {
                        Some(Box::new(InternalLayerSubjectLookup {
                            subject: s,
                            adjacencies: AdjacencyStuff {
                                predicates: s_p_adjacency_list.get((c as u64) + 1),
                                sp_offset: s_p_adjacency_list.offset_for((c as u64) + 1),
                                sp_o_adjacency_list: sp_o_adjacency_list.clone(),
                            },
                        }) as Box<dyn LayerSubjectLookup>)
                    }
                }),
        )
    }

    fn subject_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>> {
        let s_p_adjacency_list: AdjacencyList;
        let sp_o_adjacency_list: AdjacencyList;
        match (
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (Some(s_p), Some(sp_o)) => {
                s_p_adjacency_list = s_p.clone();
                sp_o_adjacency_list = sp_o.clone();
            }
            _ => return Box::new(std::iter::empty()),
        }

        Box::new(
            id_iter(self.neg_subjects(), Some(&s_p_adjacency_list))
                .enumerate()
                .filter_map(move |(c, s)| {
                    let predicates = s_p_adjacency_list.get((c as u64) + 1);
                    if predicates.len() == 1 && predicates.entry(0) == 0 {
                        None
                    } else {
                        Some(Box::new(InternalLayerSubjectLookup {
                            subject: s,
                            adjacencies: AdjacencyStuff {
                                predicates: s_p_adjacency_list.get((c as u64) + 1),
                                sp_offset: s_p_adjacency_list.offset_for((c as u64) + 1),
                                sp_o_adjacency_list: sp_o_adjacency_list.clone(),
                            },
                        }) as Box<dyn LayerSubjectLookup>)
                    }
                }),
        )
    }

    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        let mut lookups = Vec::new();

        let addition = self.lookup_subject_addition(subject);
        let removal = self.lookup_subject_removal(subject);
        if addition.is_some() || removal.is_some() {
            lookups.push((addition, removal));
        }

        let mut cur = self.immediate_parent();
        while cur.is_some() {
            let addition = cur.unwrap().lookup_subject_addition(subject);
            let removal = cur.unwrap().lookup_subject_removal(subject);

            if addition.is_some() || removal.is_some() {
                lookups.push((addition, removal));
            }

            cur = cur.unwrap().immediate_parent();
        }

        if lookups.iter().any(|(pos, _neg)| pos.is_some()) {
            Some(Box::new(GenericSubjectLookup {
                subject: subject,
                lookups: lookups,
            }) as Box<dyn SubjectLookup>)
        } else {
            None
        }
    }

    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        if subject == 0 {
            return None;
        }

        let mapped_subject: u64;
        match external_id_to_internal(self.pos_subjects(), subject) {
            Some(ms) => mapped_subject = ms,
            None => return None,
        }

        let pos_s_p_adjacency_list = self.pos_s_p_adjacency_list();
        if mapped_subject <= pos_s_p_adjacency_list.left_count() as u64 {
            let predicates = pos_s_p_adjacency_list.get(mapped_subject);
            if predicates.len() == 1 && predicates.entry(0) == 0 {
                None
            } else {
                let sp_offset = pos_s_p_adjacency_list.offset_for(mapped_subject);
                Some(Box::new(InternalLayerSubjectLookup {
                    subject,
                    adjacencies: AdjacencyStuff {
                        predicates,
                        sp_offset,
                        sp_o_adjacency_list: self.pos_sp_o_adjacency_list().clone(),
                    },
                }))
            }
        } else {
            None
        }
    }

    fn lookup_subject_removal(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        if subject == 0 {
            return None;
        }

        let neg_s_p_adjacency_list: &AdjacencyList;
        let neg_sp_o_adjacency_list: &AdjacencyList;
        match (
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (Some(s_p), Some(sp_o)) => {
                neg_s_p_adjacency_list = s_p;
                neg_sp_o_adjacency_list = sp_o;
            }
            _ => return None,
        }

        let mapped_subject: u64;
        match external_id_to_internal(self.neg_subjects(), subject) {
            Some(ms) => mapped_subject = ms,
            None => return None,
        }

        if mapped_subject <= neg_s_p_adjacency_list.left_count() as u64 {
            let predicates = neg_s_p_adjacency_list.get(mapped_subject);
            if predicates.len() == 1 && predicates.entry(0) == 0 {
                None
            } else {
                let sp_offset = neg_s_p_adjacency_list.offset_for(mapped_subject);
                Some(Box::new(InternalLayerSubjectLookup {
                    subject,
                    adjacencies: AdjacencyStuff {
                        predicates,
                        sp_offset,
                        sp_o_adjacency_list: neg_sp_o_adjacency_list.clone(),
                    },
                }))
            }
        } else {
            None
        }
    }

    fn objects(&self) -> Box<dyn Iterator<Item = Box<dyn ObjectLookup>>> {
        let mut layers = Vec::new();
        layers.push((
            self.object_additions().peekable(),
            self.object_removals().peekable(),
        ));
        let mut cur = self.immediate_parent();

        while cur.is_some() {
            layers.push((
                cur.unwrap().object_additions().peekable(),
                cur.unwrap().object_removals().peekable(),
            ));
            cur = cur.unwrap().immediate_parent();
        }

        let it = GenericObjectIterator { layers };

        Box::new(it.map(|s| Box::new(s) as Box<dyn ObjectLookup>))
    }

    fn object_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>> {
        // TODO make more efficient
        let cloned = self.clone_boxed();
        Box::new(
            id_iter(self.pos_objects(), Some(self.pos_o_ps_adjacency_list()))
                .filter_map(move |object| cloned.lookup_object_addition(object)),
        )
    }

    fn object_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>> {
        // TODO make more efficient
        let cloned = self.clone_boxed();
        Box::new(
            id_iter(self.neg_objects(), self.neg_o_ps_adjacency_list())
                .filter_map(move |object| cloned.lookup_object_removal(object)),
        )
    }

    fn lookup_object(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        let mut lookups = Vec::new();

        let addition = self.lookup_object_addition(object);
        let removal = self.lookup_object_removal(object);
        if addition.is_some() || removal.is_some() {
            lookups.push((addition, removal));
        }

        let mut cur = self.immediate_parent();
        while cur.is_some() {
            let addition = cur.unwrap().lookup_object_addition(object);
            let removal = cur.unwrap().lookup_object_removal(object);

            if addition.is_some() || removal.is_some() {
                lookups.push((addition, removal));
            }

            cur = cur.unwrap().immediate_parent();
        }

        if lookups.iter().any(|(pos, _neg)| pos.is_some()) {
            Some(Box::new(GenericObjectLookup { object, lookups }))
        } else {
            None
        }
    }

    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        let mapped_object = external_id_to_internal(self.pos_objects(), object);

        mapped_object.and_then(|o| {
            if o > self.pos_o_ps_adjacency_list().left_count() as u64 {
                return None;
            }

            let sp_slice = self.pos_o_ps_adjacency_list().get(o);
            if sp_slice.len() == 1 && sp_slice.entry(0) == 0 {
                // this is a stub
                None
            } else {
                let subjects = self.pos_subjects().map(|s| s.clone());
                let s_p_adjacency_list = self.pos_s_p_adjacency_list().clone();

                Some(Box::new(InternalLayerObjectLookup {
                    object,
                    sp_slice,
                    s_p_adjacency_list,
                    subjects,
                }) as Box<dyn LayerObjectLookup>)
            }
        })
    }

    fn lookup_object_removal(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        let mapped_object = external_id_to_internal(self.neg_objects(), object);

        mapped_object.and_then(|o| {
            match (
                self.neg_o_ps_adjacency_list(),
                self.neg_subjects(),
                self.neg_s_p_adjacency_list(),
            ) {
                (Some(neg_o_ps_adjacency_list), neg_subjects, Some(neg_s_p_adjacency_list)) => {
                    if o > neg_o_ps_adjacency_list.left_count() as u64 {
                        return None;
                    }
                    let sp_slice = neg_o_ps_adjacency_list.get(o);
                    if sp_slice.len() == 1 && sp_slice.entry(0) == 0 {
                        // this is a stub
                        None
                    } else {
                        let subjects = neg_subjects.map(|s| s.clone());
                        let s_p_adjacency_list = neg_s_p_adjacency_list.clone();

                        Some(Box::new(InternalLayerObjectLookup {
                            object,
                            sp_slice,
                            s_p_adjacency_list,
                            subjects,
                        }) as Box<dyn LayerObjectLookup>)
                    }
                }
                _ => None,
            }
        })
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        let mut lookups = Vec::new();

        let addition = self.lookup_predicate_addition(predicate);
        let removal = self.lookup_predicate_removal(predicate);
        if addition.is_some() || removal.is_some() {
            lookups.push((addition, removal));
        }

        let mut cur = self.immediate_parent();
        while cur.is_some() {
            let addition = cur.unwrap().lookup_predicate_addition(predicate);
            let removal = cur.unwrap().lookup_predicate_removal(predicate);

            if addition.is_some() || removal.is_some() {
                lookups.push((addition, removal));
            }

            cur = cur.unwrap().immediate_parent();
        }

        if lookups.iter().any(|(pos, _neg)| pos.is_some()) {
            Some(Box::new(GenericPredicateLookup {
                predicate: predicate,
                lookups: lookups,
            }) as Box<dyn PredicateLookup>)
        } else {
            None
        }
    }

    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        self.pos_predicate_wavelet_tree()
            .lookup(predicate)
            .map(|lookup| {
                Box::new(InternalLayerPredicateLookup {
                    predicate,
                    lookup,
                    subjects: self.pos_subjects().map(|s| s.clone()),
                    s_p_adjacency_list: self.pos_s_p_adjacency_list().clone(),
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list().clone(),
                }) as Box<dyn LayerPredicateLookup>
            })
    }

    fn lookup_predicate_removal(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        match (
            self.neg_subjects(),
            self.neg_predicate_wavelet_tree(),
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (
                neg_subjects,
                Some(neg_predicate_wavelet_tree),
                Some(neg_s_p_adjacency_list),
                Some(neg_sp_o_adjacency_list),
            ) => neg_predicate_wavelet_tree.lookup(predicate).map(|lookup| {
                Box::new(InternalLayerPredicateLookup {
                    predicate,
                    lookup,
                    subjects: neg_subjects.map(|s| s.clone()),
                    s_p_adjacency_list: neg_s_p_adjacency_list.clone(),
                    sp_o_adjacency_list: neg_sp_o_adjacency_list.clone(),
                }) as Box<dyn LayerPredicateLookup>
            }),
            _ => None,
        }
    }

    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn PredicateLookup>>> {
        let cloned = self.clone_boxed();
        Box::new(
            (1..=self.predicate_count())
                .map(move |p| cloned.lookup_predicate(p as u64))
                .flatten(),
        )
    }

    fn predicate_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerPredicateLookup>>> {
        let cloned = self.clone_boxed();
        Box::new(
            (1..=self.predicate_count())
                .map(move |p| cloned.lookup_predicate_addition(p as u64))
                .flatten(),
        )
    }

    fn predicate_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerPredicateLookup>>> {
        let cloned = self.clone_boxed();
        Box::new(
            (1..=self.predicate_count())
                .map(move |p| cloned.lookup_predicate_removal(p as u64))
                .flatten(),
        )
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_layer_addition_count(&self) -> usize {
        self.pos_sp_o_adjacency_list().right_count()
    }

    fn triple_layer_removal_count(&self) -> usize {
        self.neg_sp_o_adjacency_list()
            .map(|aj| aj.right_count())
            .unwrap_or(0)
    }

    fn triple_addition_count(&self) -> usize {
        let mut additions = self.triple_layer_addition_count();

        let mut parent = self.immediate_parent();
        while parent.is_some() {
            additions += parent.unwrap().triple_layer_addition_count();

            parent = parent.unwrap().immediate_parent();
        }

        additions
    }

    fn triple_removal_count(&self) -> usize {
        let mut removals = self.triple_layer_removal_count();

        let mut parent = self.immediate_parent();
        while parent.is_some() {
            removals += parent.unwrap().triple_layer_removal_count();

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

    fn triple_additions(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(self.internal_triple_additions())
    }

    fn triple_removals(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(self.internal_triple_removals())
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(InternalTripleIterator::from_layer(self))
    }

    fn triple_additions_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn triple_removals_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            InternalTripleIterator::from_layer(self)
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.subject == subject && t.predicate == predicate),
        )
    }

    fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.subject == subject && t.predicate == predicate),
        )
    }

    fn triples_sp(&self, subject: u64, predicate: u64) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            InternalTripleIterator::from_layer(self)
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.subject == subject && t.predicate == predicate),
        )
    }
}

#[derive(Clone)]
pub enum InternalLayer {
    Base(BaseLayer),
    Child(ChildLayer),
}

impl InternalLayer {
    pub fn as_layer(&self) -> &dyn Layer {
        match self {
            Self::Base(base) => base as &dyn Layer,
            Self::Child(child) => child as &dyn Layer,
        }
    }
}

impl Deref for InternalLayer {
    type Target = dyn InternalLayerImpl + Send + Sync;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Base(base) => base as &Self::Target,
            Self::Child(child) => child as &Self::Target,
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

impl InternalLayerImpl for InternalLayer {
    fn name(&self) -> [u32; 5] {
        InternalLayerImpl::name(&**self)
    }
    fn layer_type(&self) -> LayerType {
        (&**self).layer_type()
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

#[derive(Clone)]
struct AdjacencyStuff {
    predicates: LogArray,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList,
}

struct InternalLayerSubjectLookup {
    subject: u64,

    adjacencies: AdjacencyStuff,
}

impl LayerSubjectLookup for InternalLayerSubjectLookup {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>> {
        let subject = self.subject;
        let offset = self.adjacencies.sp_offset;
        let aj = self.adjacencies.sp_o_adjacency_list.clone();
        Box::new(
            self.adjacencies
                .predicates
                .iter()
                .enumerate()
                .map(move |(c, p)| {
                    Box::new(InternalLayerSubjectPredicateLookup {
                        subject: subject,
                        predicate: p,
                        objects: aj.get(offset + (c as u64) + 1),
                    }) as Box<dyn LayerSubjectPredicateLookup>
                }),
        )
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn LayerSubjectPredicateLookup>> {
        self.adjacencies
            .predicates
            .iter()
            .position(|p| p == predicate)
            .map(|pos| {
                self.adjacencies
                    .sp_o_adjacency_list
                    .get(self.adjacencies.sp_offset + (pos as u64) + 1)
            })
            .map(|objects| {
                Box::new(InternalLayerSubjectPredicateLookup {
                    subject: self.subject,
                    predicate: predicate,
                    objects: objects,
                }) as Box<dyn LayerSubjectPredicateLookup>
            })
    }
}

struct InternalLayerSubjectPredicateLookup {
    subject: u64,
    predicate: u64,
    objects: LogArray,
}

impl LayerSubjectPredicateLookup for InternalLayerSubjectPredicateLookup {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn objects(&self) -> Box<dyn Iterator<Item = u64>> {
        Box::new(self.objects.iter())
    }

    fn has_object(&self, object: u64) -> bool {
        self.objects.iter().find(|&x| x == object).is_some()
    }
}

struct InternalLayerObjectLookup {
    object: u64,
    sp_slice: LogArray,
    s_p_adjacency_list: AdjacencyList,
    subjects: Option<MonotonicLogArray>,
}

impl LayerObjectLookup for InternalLayerObjectLookup {
    fn object(&self) -> u64 {
        self.object
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item = (u64, u64)>> {
        let sp_slice = self.sp_slice.clone();
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let subjects = self.subjects.clone();
        Box::new(
            sp_slice
                .iter()
                .map(move |index| s_p_adjacency_list.pair_at_pos(index - 1))
                .map(move |(mapped_subject, predicate)| {
                    (
                        internal_id_to_external(subjects.as_ref(), mapped_subject),
                        predicate,
                    )
                }),
        )
    }
}

struct InternalLayerPredicateLookup {
    predicate: u64,
    lookup: WaveletLookup,
    subjects: Option<MonotonicLogArray>,
    s_p_adjacency_list: AdjacencyList,
    sp_o_adjacency_list: AdjacencyList,
}

impl LayerPredicateLookup for InternalLayerPredicateLookup {
    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn subject_predicate_pairs(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>> {
        let predicate = self.predicate;
        let subjects = self.subjects.clone();
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let sp_o_adjacency_list = self.sp_o_adjacency_list.clone();

        Box::new(self.lookup.iter().map(move |pos| {
            let (mapped_subject, _predicate) = s_p_adjacency_list.pair_at_pos(pos);
            Box::new(InternalLayerSubjectPredicateLookup {
                subject: internal_id_to_external(subjects.as_ref(), mapped_subject),
                predicate: predicate,
                objects: sp_o_adjacency_list.get(pos + 1),
            }) as Box<dyn LayerSubjectPredicateLookup>
        }))
    }
}

#[derive(Clone)]
pub struct InternalLayerTripleIterator {
    subjects: Option<MonotonicLogArray>,
    s_p_adjacency_list: AdjacencyList,
    sp_o_adjacency_list: AdjacencyList,
    s_position: u64,
    s_p_position: u64,
    sp_o_position: u64,
    peeked: Option<IdTriple>,
}

impl InternalLayerTripleIterator {
    fn new(
        subjects: Option<&MonotonicLogArray>,
        s_p_adjacency_list: &AdjacencyList,
        sp_o_adjacency_list: &AdjacencyList,
    ) -> Self {
        Self {
            subjects: subjects.map(|s| s.clone()),
            s_p_adjacency_list: s_p_adjacency_list.clone(),
            sp_o_adjacency_list: sp_o_adjacency_list.clone(),
            s_position: 0,
            s_p_position: 0,
            sp_o_position: 0,
            peeked: None,
        }
    }

    pub fn seek_subject(mut self, subject: u64) -> Self {
        self.seek_subject_ref(subject);

        self
    }

    pub fn seek_subject_ref(&mut self, subject: u64) {
        if subject == 0 {
            self.s_position = 0;
            self.s_p_position = 0;
            self.sp_o_position = 0;

            return;
        }

        self.s_position = match self.subjects.as_ref() {
            None => subject - 1,
            Some(subjects) => subjects.nearest_index_of(subject) as u64,
        };

        if self.s_position >= self.s_p_adjacency_list.left_count() as u64 {
            self.s_p_position = self.s_p_adjacency_list.right_count() as u64;
            self.sp_o_position = self.sp_o_adjacency_list.right_count() as u64;
        } else {
            self.s_p_position = self.s_p_adjacency_list.offset_for(self.s_position + 1);
            self.sp_o_position = self.sp_o_adjacency_list.offset_for(self.s_p_position + 1);
        }
    }

    pub fn seek_subject_predicate(mut self, subject: u64, predicate: u64) -> Self {
        self.seek_subject_predicate_ref(subject, predicate);

        self
    }

    pub fn seek_subject_predicate_ref(&mut self, subject: u64, predicate: u64) {
        if predicate == 0 {
            // equivalent to seeking subject
            self.seek_subject_ref(subject);
            return;
        }

        if subject == 0 {
            self.s_position = 0;
            self.s_p_position = 0;
            self.sp_o_position = 0;

            return;
        }

        self.s_position = match self.subjects.as_ref() {
            None => subject - 1,
            Some(subjects) => subjects.nearest_index_of(subject) as u64,
        };

        if self.s_position >= self.s_p_adjacency_list.left_count() as u64 {
            self.s_p_position = self.s_p_adjacency_list.right_count() as u64;
            self.sp_o_position = self.sp_o_adjacency_list.right_count() as u64;
        } else {
            let mut s_p_position = self.s_p_adjacency_list.offset_for(self.s_position + 1);
            while self.s_p_adjacency_list.num_at_pos(s_p_position) < predicate {
                s_p_position += 1;

                if self.s_p_adjacency_list.bit_at_pos(s_p_position - 1) {
                    // we just moved past the end for this subject, without finding the predicate.
                    // so this is where we have to stop
                    self.s_position += 1;
                    break;
                }
            }
            self.s_p_position = s_p_position;
            self.sp_o_position = self.sp_o_adjacency_list.offset_for(self.s_p_position + 1);
        }
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.peeked = self.next();

        self.peeked.as_ref()
    }
}

impl Iterator for InternalLayerTripleIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        if self.peeked.is_some() {
            let peeked = self.peeked;
            self.peeked = None;

            return peeked;
        }
        loop {
            if self.sp_o_position >= self.sp_o_adjacency_list.right_count() as u64 {
                return None;
            } else {
                let subject = match self.subjects.as_ref() {
                    Some(subjects) => subjects.entry(self.s_position.try_into().unwrap()),
                    None => self.s_position + 1,
                };

                let s_p_bit = self.s_p_adjacency_list.bit_at_pos(self.s_p_position);
                let predicate = self.s_p_adjacency_list.num_at_pos(self.s_p_position);
                if predicate == 0 {
                    self.s_position += 1;
                    self.s_p_position += 1;
                    self.sp_o_position += 1;
                    continue;
                }

                let sp_o_bit = self.sp_o_adjacency_list.bit_at_pos(self.sp_o_position);
                let object = self.sp_o_adjacency_list.num_at_pos(self.sp_o_position);
                if sp_o_bit {
                    self.s_p_position += 1;
                    if s_p_bit {
                        self.s_position += 1;
                    }
                }
                self.sp_o_position += 1;

                if object == 0 {
                    continue;
                }

                return Some(IdTriple::new(subject, predicate, object));
            }
        }
    }
}

pub struct OptInternalLayerTripleIterator(Option<InternalLayerTripleIterator>);

impl OptInternalLayerTripleIterator {
    pub fn seek_subject_ref(&mut self, subject: u64) {
        self.0.as_mut().map(|i| i.seek_subject_ref(subject));
    }

    pub fn seek_subject(self, subject: u64) -> Self {
        OptInternalLayerTripleIterator(self.0.map(|i| i.seek_subject(subject)))
    }

    pub fn seek_subject_predicate_ref(&mut self, subject: u64, predicate: u64) {
        self.0
            .as_mut()
            .map(|i| i.seek_subject_predicate_ref(subject, predicate));
    }

    pub fn seek_subject_predicate(self, subject: u64, predicate: u64) -> Self {
        OptInternalLayerTripleIterator(self.0.map(|i| i.seek_subject_predicate(subject, predicate)))
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.0.as_mut().and_then(|i| i.peek())
    }
}

pub struct InternalTripleIterator {
    positives: Vec<OptInternalLayerTripleIterator>,
    negatives: Vec<OptInternalLayerTripleIterator>,
}

impl InternalTripleIterator {
    fn from_layer<T: 'static + InternalLayerImpl>(layer: &T) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        positives.push(layer.internal_triple_additions());
        negatives.push(layer.internal_triple_removals());

        let mut layer_opt = layer.immediate_parent();

        while layer_opt.is_some() {
            positives.push(layer_opt.unwrap().internal_triple_additions());
            negatives.push(layer_opt.unwrap().internal_triple_removals());

            layer_opt = layer_opt.unwrap().immediate_parent();
        }

        Self {
            positives,
            negatives,
        }
    }

    pub fn seek_subject(mut self, subject: u64) -> Self {
        for p in self.positives.iter_mut() {
            p.seek_subject_ref(subject);
        }

        for n in self.negatives.iter_mut() {
            n.seek_subject_ref(subject);
        }

        self
    }

    pub fn seek_subject_predicate(mut self, subject: u64, predicate: u64) -> Self {
        for p in self.positives.iter_mut() {
            p.seek_subject_predicate_ref(subject, predicate);
        }

        for n in self.negatives.iter_mut() {
            n.seek_subject_predicate_ref(subject, predicate);
        }

        self
    }
}

impl Iterator for OptInternalLayerTripleIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        self.0.as_mut().and_then(|i| i.next())
    }
}

impl Iterator for InternalTripleIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        'outer: loop {
            // find the lowest triple.
            // if that triple appears multiple times, we want the most recent one, which should be the one appearing the earliest in the positives list.
            let lowest_index = self
                .positives
                .iter_mut()
                .map(|p| p.peek())
                .enumerate()
                .filter(|(_, elt)| elt.is_some())
                .min_by_key(|(_, elt)| elt.unwrap())
                .map(|(index, _)| index);

            match lowest_index {
                None => return None,
                Some(lowest_index) => {
                    let lowest = self.positives[lowest_index].next().unwrap();
                    // check all negative layers below the lowest_index for a removal
                    // if there's a removal, we continue after advancing. if not, it is the result.
                    // we can be sure that there's only one removal, or we'd have found another addition.
                    for iter in self.negatives[0..lowest_index].iter_mut() {
                        if iter.peek() == Some(&lowest) {
                            iter.next().unwrap();
                            continue 'outer;
                        }
                    }

                    return Some(lowest);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::layer::base::tests::*;
    use crate::layer::child::tests::*;
    use crate::layer::*;

    use futures::prelude::*;
    use std::sync::Arc;

    #[test]
    fn base_triple_iterator() {
        let base_layer: InternalLayer = example_base_layer().into();

        let triples: Vec<_> = base_layer.triple_additions().collect();
        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(2, 1, 1),
            IdTriple::new(2, 1, 3),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_removal_iterator() {
        let base_layer: InternalLayer = example_base_layer().into();

        let triples: Vec<_> = base_layer.triple_removals().collect();
        assert!(triples.is_empty());
    }

    #[test]
    fn base_stubs_triple_iterator() {
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap();

        let triples: Vec<_> = layer.triple_additions().collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(3, 2, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    fn layer_for_seek_tests() -> BaseLayer {
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 3, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap()
    }

    #[test]
    fn base_triple_iterator_seek_to_subject() {
        let layer = layer_for_seek_tests();

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(3).collect();

        let expected = vec![
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_nonexistent() {
        let layer = layer_for_seek_tests();

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(4).collect();

        let expected = vec![IdTriple::new(5, 3, 6)];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_past_end() {
        let layer = layer_for_seek_tests();

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(7).collect();

        assert!(triples.is_empty());
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_0() {
        let layer = layer_for_seek_tests();

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(0).collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_before_begin() {
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 3, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap();

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(2).collect();

        let expected = vec![
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    fn layer_for_seek_sp_tests() -> BaseLayer {
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll", "xyz", "yyy"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(3, 2, 4))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 4, 2))
            .and_then(|b| b.add_triple(3, 4, 3))
            .and_then(|b| b.add_triple(3, 4, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap()
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 4)
            .collect();

        let expected = vec![
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_nonexistent() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 3)
            .collect();

        let expected = vec![
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred0() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 0)
            .collect();

        let expected = vec![
            IdTriple::new(3, 2, 4),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_sub0() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(0, 2)
            .collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(3, 2, 4),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred_before() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 1)
            .collect();

        let expected = vec![
            IdTriple::new(3, 2, 4),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred_past_end_of_subject() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 6)
            .collect();

        let expected = vec![IdTriple::new(5, 3, 6)];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred_past_end() {
        let layer = layer_for_seek_sp_tests();

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(5, 4)
            .collect();

        assert!(triples.is_empty());
    }

    #[test]
    fn base_triple_iterator_additions_for_subject() {
        let layer = layer_for_seek_tests();

        let triples: Vec<_> = layer.triple_additions_s(3).collect();

        let expected = vec![IdTriple::new(3, 2, 5), IdTriple::new(3, 3, 5)];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_additions_for_subject_predicate() {
        let layer = layer_for_seek_sp_tests();

        let expected = vec![
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
        ];

        let triples: Vec<_> = layer.triple_additions_sp(3, 4).collect();

        assert_eq!(expected, triples);
    }

    fn child_layer() -> InternalLayer {
        let base_layer = example_base_layer();
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 3))
            .and_then(|b| b.add_triple(3, 3, 4))
            .and_then(|b| b.add_triple(3, 5, 6))
            .and_then(|b| b.remove_triple(1, 1, 1))
            .and_then(|b| b.remove_triple(2, 1, 3))
            .and_then(|b| b.remove_triple(4, 3, 6))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap()
            .into()
    }

    #[test]
    fn child_triple_addition_iterator() {
        let layer = child_layer();

        let triples: Vec<_> = layer.triple_additions().collect();

        let expected = vec![
            IdTriple::new(1, 2, 3),
            IdTriple::new(3, 3, 4),
            IdTriple::new(3, 5, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn child_triple_removal_iterator() {
        let layer = child_layer();

        let triples: Vec<_> = layer.triple_removals().collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(2, 1, 3),
            IdTriple::new(4, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    use crate::storage::memory::*;
    use crate::storage::LayerStore;
    #[test]
    fn combined_iterator_for_subject() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(&StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(&StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child1_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(&StringTriple::new_node("horse", "likes", "horse"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(child1_name).wait().unwrap();
        let child2_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "cow"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(child2_name).wait().unwrap();
        let child3_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(child3_name).wait().unwrap();
        let child4_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "cow"));
        builder.commit_boxed().wait().unwrap();

        let layer = store.get_layer(child4_name).wait().unwrap().unwrap();

        let subject_id = layer.subject_id("duck").unwrap();
        let triples: Vec<_> = layer
            .triples_s(subject_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_value("duck", "says", "quack"),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn combined_iterator_for_subject_predicate() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(&StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(&StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child1_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(&StringTriple::new_node("horse", "likes", "horse"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(child1_name).wait().unwrap();
        let child2_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "horse"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(child2_name).wait().unwrap();
        let child3_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "pig"));
        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(child3_name).wait().unwrap();
        let child4_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_node("duck", "hates", "cow"));
        builder.remove_string_triple(&StringTriple::new_node("duck", "likes", "horse"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(&StringTriple::new_node("duck", "likes", "rabbit"));
        builder.commit_boxed().wait().unwrap();

        let layer = store.get_layer(child4_name).wait().unwrap().unwrap();

        let subject_id = layer.subject_id("duck").unwrap();
        let predicate_id = layer.predicate_id("likes").unwrap();
        let triples: Vec<_> = layer
            .triples_sp(subject_id, predicate_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_node("duck", "likes", "pig"),
            StringTriple::new_node("duck", "likes", "rabbit"),
        ];

        assert_eq!(expected, triples);
    }
}
