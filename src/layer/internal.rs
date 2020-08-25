use crate::structure::*;
use super::layer::*;
use super::base::*;
use super::child::*;
use std::ops::Deref;

fn external_id_to_internal(array_option: Option<&MonotonicLogArray>, id: u64) -> Option<u64> {
    if id == 0 {
        return None;
    }

    match array_option {
        Some(array) => array.index_of(id).map(|mapped| mapped as u64 + 1),
        None => Some(id)
    }
}

fn internal_id_to_external(array_option: Option<&MonotonicLogArray>, id: u64) -> u64 {
    match array_option {
        Some(array) => array.entry((id - 1) as usize),
        None => id
    }
}

fn id_iter(array_option: Option<&MonotonicLogArray>, adjacency_list_option: Option<&AdjacencyList>) -> Box<dyn Iterator<Item=u64>> {
    match (array_option, adjacency_list_option) {
        (Some(array), _) => Box::new(array.iter()),
        (_, Some(adjacency_list)) => Box::new(1..(adjacency_list.left_count() as u64 +1)),
        _ => Box::new(std::iter::empty())
    }
}

pub trait InternalLayerImpl {
    fn name(&self) -> [u32;5];
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
}

impl<T:'static+InternalLayerImpl+Send+Sync+Clone> Layer for T {
    fn name(&self) -> [u32; 5] {
        Self::name(self)
    }

    fn names(&self) -> Vec<[u32; 5]> {
        let mut result = Vec::new();
        result.push(self.name());

        let mut parent_option = self.parent();
        while let Some(parent) = parent_option {
            result.push(parent.name());
            parent_option = parent.parent();
        }

        result.reverse();

        result
    }

    fn parent(&self) -> Option<&dyn Layer> {
        self.immediate_parent()
            .map(|layer| layer.as_layer())
    }

    fn node_and_value_count(&self) -> usize {
        let mut parent_option = self.parent();
        let mut count = self.node_dictionary().len() + self.value_dictionary().len();
        while let Some(parent) = parent_option {
            count += parent.node_dict_len() + parent.value_dict_len();
            parent_option = parent.parent();
        }
        count
    }

    fn predicate_count(&self) -> usize {
        let mut parent_option = self.parent();
        let mut count = self.predicate_dictionary().len();
        while let Some(parent) = parent_option {
            count += parent.predicate_dict_len();
            parent_option = parent.parent();
        }
        count
    }

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

    fn subject_id<'a>(&'a self, subject: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn Layer| (layer.node_dict_id(subject), layer.parent());
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn predicate_id<'a>(&'a self, predicate: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn Layer| (layer.predicate_dict_id(predicate), layer.parent());
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.predicate_count() as u64))
    }

    fn object_node_id<'a>(&'a self, object: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn Layer| (layer.node_dict_id(object), layer.parent());
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn object_value_id<'a>(&'a self, object: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn Layer| {
            (
                layer
                    .value_dict_id(object)
                    .map(|i| i + layer.node_dict_len() as u64),
                layer.parent(),
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
        let mut current_option: Option<&dyn Layer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;
                if corrected_id >= parent_count as u64 {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                }
                else {
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
        let mut current_option: Option<&dyn Layer> = Some(self);
        let mut parent_count = self.predicate_count() as u64;
        while let Some(current_layer) = current_option {
            let mut corrected_id = id - 1;
            if let Some(parent) = current_layer.parent() {
                parent_count = parent_count - current_layer.predicate_dict_len() as u64;
                if corrected_id >= parent_count as u64 {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                }
                else {
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
        let mut current_option: Option<&dyn Layer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;

                if corrected_id >= parent_count {
                    // object, if it exists, is in this layer
                    corrected_id -= parent_count;
                }
                else {
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

    fn subject_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>> {
        let s_p_adjacency_list = self.pos_s_p_adjacency_list().clone();
        let sp_o_adjacency_list = self.pos_sp_o_adjacency_list().clone();

        Box::new(id_iter(self.pos_subjects(), Some(&s_p_adjacency_list)).enumerate().map(move |(c, s)| {
            Box::new(InternalLayerSubjectLookup {
                subject: s,
                adjacencies: AdjacencyStuff {
                    predicates: s_p_adjacency_list.get((c as u64) + 1),
                    sp_offset: s_p_adjacency_list.offset_for((c as u64) + 1),
                    sp_o_adjacency_list: sp_o_adjacency_list.clone(),
                },
            }) as Box<dyn LayerSubjectLookup>
        }))
    }

    fn subject_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>> {
        let s_p_adjacency_list: AdjacencyList;
        let sp_o_adjacency_list: AdjacencyList;
        match (self.neg_s_p_adjacency_list(), self.neg_sp_o_adjacency_list()) {
            (Some(s_p), Some(sp_o)) => {
                s_p_adjacency_list = s_p.clone();
                sp_o_adjacency_list = sp_o.clone();
            },
            _ => return Box::new(std::iter::empty())
        }

        Box::new(id_iter(self.neg_subjects(), Some(&s_p_adjacency_list)).enumerate().map(move |(c, s)| {
            Box::new(InternalLayerSubjectLookup {
                subject: s,
                adjacencies: AdjacencyStuff {
                    predicates: s_p_adjacency_list.get((c as u64) + 1),
                    sp_offset: s_p_adjacency_list.offset_for((c as u64) + 1),
                    sp_o_adjacency_list: sp_o_adjacency_list.clone(),
                },
            }) as Box<dyn LayerSubjectLookup>
        }))
    }

    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        if subject == 0 {
            return None;
        }

        let mapped_subject: u64;
        match external_id_to_internal(self.pos_subjects(),subject) {
            Some(ms) => mapped_subject = ms,
            None => return None
        }

        let pos_s_p_adjacency_list = self.pos_s_p_adjacency_list();
        if mapped_subject <= pos_s_p_adjacency_list.left_count() as u64 {
            let predicates = pos_s_p_adjacency_list.get(mapped_subject);
            let sp_offset = pos_s_p_adjacency_list.offset_for(mapped_subject);
            Some(Box::new(InternalLayerSubjectLookup {
                subject,
                adjacencies: AdjacencyStuff {
                    predicates,
                    sp_offset,
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list().clone(),
                },
            }))
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
        match (self.neg_s_p_adjacency_list(), self.neg_sp_o_adjacency_list()) {
            (Some(s_p), Some(sp_o)) => {
                neg_s_p_adjacency_list = s_p;
                neg_sp_o_adjacency_list = sp_o;
            },
            _ => return None
        }

        let mapped_subject:u64;
        match external_id_to_internal(self.neg_subjects(), subject) {
            Some(ms) => mapped_subject = ms,
            None => return None
        }

        if mapped_subject <= neg_s_p_adjacency_list.left_count() as u64 {
            let predicates = neg_s_p_adjacency_list.get(mapped_subject);
            let sp_offset = neg_s_p_adjacency_list.offset_for(mapped_subject);
            Some(Box::new(InternalLayerSubjectLookup {
                subject,
                adjacencies: AdjacencyStuff {
                    predicates,
                    sp_offset,
                    sp_o_adjacency_list: neg_sp_o_adjacency_list.clone(),
                },
            }))
        } else {
            None
        }
    }

    fn object_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>> {
        // TODO make more efficient
        let cloned = self.clone_boxed();
        Box::new(id_iter(self.pos_objects(), Some(self.pos_o_ps_adjacency_list()))
                 .filter_map(move |object| cloned.lookup_object_addition(object)))
    }

    fn object_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>> {
        // TODO make more efficient
        let cloned = self.clone_boxed();
        Box::new(id_iter(self.neg_objects(), self.neg_o_ps_adjacency_list())
                 .filter_map(move |object| cloned.lookup_object_removal(object)))
    }

    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        let mapped_object = external_id_to_internal(self.pos_objects(), object);

        mapped_object.and_then(|o| {
            let sp_slice = self.pos_o_ps_adjacency_list().get(o);
            if sp_slice.len() == 1 && sp_slice.entry(0) == 0 {
                // this is a stub
                None
            }
            else {
                let subjects = self.pos_subjects().map(|s|s.clone());
                let s_p_adjacency_list = self.pos_s_p_adjacency_list().clone();

                Some(Box::new(InternalLayerObjectLookup {
                    object,
                    sp_slice,
                    s_p_adjacency_list,
                    subjects
                }) as Box<dyn LayerObjectLookup>)
            }
        })
    }

    fn lookup_object_removal(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        let mapped_object = external_id_to_internal(self.neg_objects(), object);

        mapped_object.and_then(|o| {
            match (self.neg_o_ps_adjacency_list(),
                   self.neg_subjects(),
                   self.neg_s_p_adjacency_list()) {
                (Some(neg_o_ps_adjacency_list),
                 neg_subjects,
                 Some(neg_s_p_adjacency_list)) => {
                    let sp_slice = neg_o_ps_adjacency_list.get(o);
                    let subjects = neg_subjects.map(|s|s.clone());
                    let s_p_adjacency_list = neg_s_p_adjacency_list.clone();

                    Some(Box::new(InternalLayerObjectLookup {
                        object,
                        sp_slice,
                        s_p_adjacency_list,
                        subjects
                    }) as Box<dyn LayerObjectLookup>)
                },
                _ => None
            }

        })
    }

    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        self.pos_predicate_wavelet_tree()
            .lookup(predicate)
            .map(|lookup| {
                Box::new(InternalLayerPredicateLookup {
                    predicate,
                    lookup,
                    subjects: self.pos_subjects().map(|s|s.clone()),
                    s_p_adjacency_list: self.pos_s_p_adjacency_list().clone(),
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list().clone(),
                }) as Box<dyn LayerPredicateLookup>
            })
    }

    fn lookup_predicate_removal(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        match (self.neg_subjects(),
               self.neg_predicate_wavelet_tree(),
               self.neg_s_p_adjacency_list(),
               self.neg_sp_o_adjacency_list()) {
            (neg_subjects,
             Some(neg_predicate_wavelet_tree),
             Some(neg_s_p_adjacency_list),
             Some(neg_sp_o_adjacency_list)) => {
                neg_predicate_wavelet_tree
                    .lookup(predicate)
                    .map(|lookup| {
                        Box::new(InternalLayerPredicateLookup {
                            predicate,
                            lookup,
                            subjects: neg_subjects.map(|s|s.clone()),
                            s_p_adjacency_list: neg_s_p_adjacency_list.clone(),
                            sp_o_adjacency_list: neg_sp_o_adjacency_list.clone(),
                        }) as Box<dyn LayerPredicateLookup>
                    })
            },
            _ => None
        }
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_layer_addition_count(&self) -> usize {
        self.pos_sp_o_adjacency_list().right_count()
    }

    fn triple_layer_removal_count(&self) -> usize {
        self.neg_sp_o_adjacency_list().map(|aj|aj.right_count())
            .unwrap_or(0)
    }


}

#[derive(Clone)]
pub enum InternalLayer {
    Base(BaseLayer),
    Child(ChildLayer)
}

impl InternalLayer {
    pub fn as_layer(&self) -> &dyn Layer {
        match self {
            Self::Base(base) => base as &dyn Layer,
            Self::Child(child) => child as &dyn Layer
        }
    }
}

impl Deref for InternalLayer {
    type Target = dyn InternalLayerImpl+Send+Sync;
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
    fn name(&self) -> [u32;5] {
        InternalLayerImpl::name(&**self)
    }
    fn layer_type(&self) -> LayerType {
        (&**self).layer_type()
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
                    (internal_id_to_external(subjects.as_ref(), mapped_subject), predicate)
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
