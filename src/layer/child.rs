//! Child layer implementation
//!
//! A child layer stores a reference to a base layer, as well as
//! triple additions and removals, and any new dictionary entries that
//! this layer needs for its additions.
use super::layer::*;
use crate::structure::*;
use crate::storage::*;
use futures::prelude::*;
use futures::future;
use futures::stream;

use std::cmp::Ordering;
use std::sync::Arc;
use std::collections::BTreeSet;
use std::io;

/// A child layer.
///
/// This layer type has a parent. It stores triple additions and removals.
#[derive(Clone)]
pub struct ChildLayer<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    name: [u32;5],
    parent: Arc<dyn Layer>,

    node_dictionary: PfcDict<M>,
    predicate_dictionary: PfcDict<M>,
    value_dictionary: PfcDict<M>,

    pos_subjects: MonotonicLogArray<M>,
    pos_objects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,
    pos_o_ps_adjacency_list: AdjacencyList<M>,

    neg_subjects: MonotonicLogArray<M>,
    neg_objects: MonotonicLogArray<M>,
    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>,
    neg_o_ps_adjacency_list: AdjacencyList<M>,

    pos_predicate_wavelet_tree: WaveletTree<M>,
    neg_predicate_wavelet_tree: WaveletTree<M>,
}

impl<M:'static+AsRef<[u8]>+Clone+Send+Sync> ChildLayer<M> {
    pub fn load_from_files<F:FileLoad<Map=M>+FileStore+Clone>(name: [u32;5], parent: Arc<dyn Layer>, files: &ChildLayerFiles<F>) -> impl Future<Item=Self,Error=std::io::Error> {
        files.map_all()
            .map(move |maps| Self::load(name, parent, maps))
    }

    pub fn load(name: [u32;5],
                parent: Arc<dyn Layer>,
                maps: ChildLayerMaps<M>) -> ChildLayer<M> {
        let node_dictionary = PfcDict::parse(maps.node_dictionary_maps.blocks_map, maps.node_dictionary_maps.offsets_map).unwrap();
        let predicate_dictionary = PfcDict::parse(maps.predicate_dictionary_maps.blocks_map, maps.predicate_dictionary_maps.offsets_map).unwrap();
        let value_dictionary = PfcDict::parse(maps.value_dictionary_maps.blocks_map, maps.value_dictionary_maps.offsets_map).unwrap();

        let pos_subjects = MonotonicLogArray::from_logarray(LogArray::parse(maps.pos_subjects_map).unwrap());
        let pos_objects = MonotonicLogArray::from_logarray(LogArray::parse(maps.pos_objects_map).unwrap());
        let neg_subjects = MonotonicLogArray::from_logarray(LogArray::parse(maps.neg_subjects_map).unwrap());
        let neg_objects = MonotonicLogArray::from_logarray(LogArray::parse(maps.neg_objects_map).unwrap());

        let pos_s_p_adjacency_list = AdjacencyList::parse(maps.pos_s_p_adjacency_list_maps.nums_map,
                                                          maps.pos_s_p_adjacency_list_maps.bitindex_maps.bits_map,
                                                          maps.pos_s_p_adjacency_list_maps.bitindex_maps.blocks_map,
                                                          maps.pos_s_p_adjacency_list_maps.bitindex_maps.sblocks_map);
        let pos_sp_o_adjacency_list = AdjacencyList::parse(maps.pos_sp_o_adjacency_list_maps.nums_map,
                                                          maps.pos_sp_o_adjacency_list_maps.bitindex_maps.bits_map,
                                                          maps.pos_sp_o_adjacency_list_maps.bitindex_maps.blocks_map,
                                                          maps.pos_sp_o_adjacency_list_maps.bitindex_maps.sblocks_map);
        let pos_o_ps_adjacency_list = AdjacencyList::parse(maps.pos_o_ps_adjacency_list_maps.nums_map,
                                                          maps.pos_o_ps_adjacency_list_maps.bitindex_maps.bits_map,
                                                          maps.pos_o_ps_adjacency_list_maps.bitindex_maps.blocks_map,
                                                          maps.pos_o_ps_adjacency_list_maps.bitindex_maps.sblocks_map);
        let neg_s_p_adjacency_list = AdjacencyList::parse(maps.neg_s_p_adjacency_list_maps.nums_map,
                                                          maps.neg_s_p_adjacency_list_maps.bitindex_maps.bits_map,
                                                          maps.neg_s_p_adjacency_list_maps.bitindex_maps.blocks_map,
                                                          maps.neg_s_p_adjacency_list_maps.bitindex_maps.sblocks_map);
        let neg_sp_o_adjacency_list = AdjacencyList::parse(maps.neg_sp_o_adjacency_list_maps.nums_map,
                                                          maps.neg_sp_o_adjacency_list_maps.bitindex_maps.bits_map,
                                                          maps.neg_sp_o_adjacency_list_maps.bitindex_maps.blocks_map,
                                                          maps.neg_sp_o_adjacency_list_maps.bitindex_maps.sblocks_map);
        let neg_o_ps_adjacency_list = AdjacencyList::parse(maps.neg_o_ps_adjacency_list_maps.nums_map,
                                                          maps.neg_o_ps_adjacency_list_maps.bitindex_maps.bits_map,
                                                          maps.neg_o_ps_adjacency_list_maps.bitindex_maps.blocks_map,
                                                          maps.neg_o_ps_adjacency_list_maps.bitindex_maps.sblocks_map);

        let pos_predicate_wavelet_tree_width = pos_s_p_adjacency_list.nums().width();
        let pos_predicate_wavelet_tree = WaveletTree::from_parts(BitIndex::from_maps(maps.pos_predicate_wavelet_tree_maps.bits_map,
                                                                                     maps.pos_predicate_wavelet_tree_maps.blocks_map,
                                                                                     maps.pos_predicate_wavelet_tree_maps.sblocks_map),
                                                                 pos_predicate_wavelet_tree_width);

        let neg_predicate_wavelet_tree_width = neg_s_p_adjacency_list.nums().width();
        let neg_predicate_wavelet_tree = WaveletTree::from_parts(BitIndex::from_maps(maps.neg_predicate_wavelet_tree_maps.bits_map,
                                                                                     maps.neg_predicate_wavelet_tree_maps.blocks_map,
                                                                                     maps.neg_predicate_wavelet_tree_maps.sblocks_map),
                                                                 neg_predicate_wavelet_tree_width);

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

    fn lookup_layer_object_addition_mapped(&self, mapped_object: u64) -> impl LayerObjectLookup {
        if mapped_object == 0 || mapped_object as usize > self.pos_objects.len() {
            panic!("unknown mapped object requested");
        }
        let object = self.pos_objects.entry((mapped_object-1) as usize);

        let sp_slice = self.pos_o_ps_adjacency_list.get(mapped_object);
        let subjects = self.pos_subjects.clone();
        let s_p_adjacency_list = self.pos_s_p_adjacency_list.clone();

        ChildLayerObjectLookup {
            object,
            sp_slice,
            s_p_adjacency_list,
            subjects
        }
    }

    fn lookup_layer_object_removal_mapped(&self, mapped_object: u64) -> impl LayerObjectLookup {
        if mapped_object == 0 || mapped_object as usize > self.neg_objects.len() {
            panic!("unknown mapped object requested");
        }
        let object = self.neg_objects.entry((mapped_object-1) as usize);

        let sp_slice = self.neg_o_ps_adjacency_list.get(mapped_object);
        let subjects = self.neg_subjects.clone();
        let s_p_adjacency_list = self.neg_s_p_adjacency_list.clone();

        ChildLayerObjectLookup {
            object,
            sp_slice,
            s_p_adjacency_list,
            subjects
        }
    }
}

impl<M:'static+AsRef<[u8]>+Clone+Send+Sync> Layer for ChildLayer<M> {
    fn name(&self) -> [u32;5] {
        self.name
    }

    fn parent(&self) -> Option<&dyn Layer> {
        Some(&*self.parent)
    }

    fn node_dict_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary.id(subject)
    }

    fn node_dict_len(&self) -> usize {
        self.node_dictionary.len()
    }

    fn node_dict_get(&self, id: usize) -> Option<String> {
        self.node_dictionary.get(id)
    }


    fn value_dict_get(&self, id: usize) -> Option<String> {
        self.value_dictionary.get(id)
    }

    fn value_dict_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary.id(value)
    }

    fn value_dict_len(&self) -> usize {
        self.value_dictionary.len()
    }

    fn node_and_value_count(&self) -> usize {
        let mut parent_option = self.parent();
        let mut count = self.node_dictionary.len() + self.value_dictionary.len();
        while let Some(parent) = parent_option {
            count += parent.node_dict_len() + parent.value_dict_len();
            parent_option = parent.parent();
        }
        count
    }

    fn predicate_dict_len(&self) -> usize {
        self.predicate_dictionary.len()
    }

    fn predicate_dict_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary.id(predicate)
    }

    fn predicate_dict_get(&self, id: usize) -> Option<String> {
        self.predicate_dictionary.get(id)
    }

    fn predicate_count(&self) -> usize {
        let mut parent_option = self.parent();
        let mut count = self.predicate_dictionary.len();
        while let Some(parent) = parent_option {
            count += parent.predicate_dict_len();
            parent_option = parent.parent();
        }
        count
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

    fn object_node_id<'a>(&'a self, subject: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn Layer| (layer.node_dict_id(subject), layer.parent());
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| 1 + id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn object_value_id<'a>(&'a self, value: &str) -> Option<u64> {
        let to_result = |layer: &'a dyn Layer| {
            (
                layer
                    .value_dict_id(value)
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
            parent_count = parent_count - current_layer.node_dict_len() as u64 - current_layer.value_dict_len() as u64;
            if corrected_id >= parent_count as u64 {
                // subject, if it exists, is in this layer
                corrected_id -= parent_count;
                return current_layer.node_dict_get(corrected_id as usize);
            }
            else {
                current_option = current_layer.parent();
            }
        }
        return None;
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let mut current_option: Option<&dyn Layer> = Some(self);
        let mut parent_count = self.predicate_count() as u64;
        while let Some(current_layer) = current_option {
            let parent = current_layer.parent();
            if parent.is_none() {
                return current_layer.id_predicate(id);
            }
            parent_count = parent_count - current_layer.predicate_dict_len() as u64;
            if corrected_id >= parent_count as u64 {
                // subject, if it exists, is in this layer
                corrected_id -= parent_count;
                return current_layer.predicate_dict_get(corrected_id as usize);
            }
            else {
                current_option = current_layer.parent();
            }
        }
        return None;
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let mut current_option: Option<&dyn Layer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            let parent = current_layer.parent();
            if parent.is_none() {
                return current_layer.id_object(id);
            }
            parent_count = parent_count - current_layer.node_dict_len() as u64 - current_layer.value_dict_len() as u64;

            if corrected_id >= parent_count {
                // object, if it exists, is in this layer
                corrected_id -= parent_count;
                if corrected_id >= current_layer.node_dict_len() as u64 {
                    // object, if it exists, must be a value
                    corrected_id -= current_layer.node_dict_len() as u64;
                    return current_layer.value_dict_get(corrected_id as usize).map(ObjectType::Value);
                }
                else {
                    return current_layer.node_dict_get(corrected_id as usize).map(ObjectType::Node);
                }
            }
            else {
                current_option = current_layer.parent();
            }
        }
        return None;
    }

    fn subject_additions(&self) -> Box<dyn Iterator<Item=Box<dyn LayerSubjectLookup>>> {
        let s_p_adjacency_list = self.pos_s_p_adjacency_list.clone();
        let sp_o_adjacency_list = self.pos_sp_o_adjacency_list.clone();
        Box::new(self.pos_subjects.clone().into_iter().enumerate()
                 .map(move |(c,s)| Box::new(ChildLayerSubjectLookup {
                     subject: s,
                     adjacencies: AdjacencyStuff {
                         predicates: s_p_adjacency_list.get((c as u64)+1),
                         sp_offset: s_p_adjacency_list.offset_for((c as u64)+1),
                         sp_o_adjacency_list: sp_o_adjacency_list.clone()
                     }
                 }) as Box<dyn LayerSubjectLookup>))
    }

    fn subject_removals(&self) -> Box<dyn Iterator<Item=Box<dyn LayerSubjectLookup>>> {
        let s_p_adjacency_list = self.neg_s_p_adjacency_list.clone();
        let sp_o_adjacency_list = self.neg_sp_o_adjacency_list.clone();
        Box::new(self.neg_subjects.clone().into_iter().enumerate()
                 .map(move |(c,s)| Box::new(ChildLayerSubjectLookup {
                     subject: s,
                     adjacencies: AdjacencyStuff {
                         predicates: s_p_adjacency_list.get((c as u64)+1),
                         sp_offset: s_p_adjacency_list.offset_for((c as u64)+1),
                         sp_o_adjacency_list: sp_o_adjacency_list.clone()
                     }
                 }) as Box<dyn LayerSubjectLookup>))
    }

    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        if subject == 0 {
            return None;
        }

        let index = self.pos_subjects.index_of(subject);
        if index.is_none() {
            return None;
        }

        let mapped_subject = index.unwrap() as u64 + 1;
        if mapped_subject <= self.pos_s_p_adjacency_list.left_count() as u64 {
            let predicates = self.pos_s_p_adjacency_list.get(mapped_subject);
            let sp_offset = self.pos_s_p_adjacency_list.offset_for(mapped_subject);
            Some(Box::new(ChildLayerSubjectLookup {
                subject,
                adjacencies: AdjacencyStuff {
                    predicates,
                    sp_offset,
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
                }
            }))
        }
        else {
            None
        }
    }

    fn lookup_subject_removal(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>> {
        if subject == 0 {
            return None;
        }

        let index = self.neg_subjects.index_of(subject);
        if index.is_none() {
            return None;
        }

        let mapped_subject = index.unwrap() as u64 + 1;
        if mapped_subject <= self.neg_s_p_adjacency_list.left_count() as u64 {
            let predicates = self.neg_s_p_adjacency_list.get(mapped_subject);
            let sp_offset = self.neg_s_p_adjacency_list.offset_for(mapped_subject);
            Some(Box::new(ChildLayerSubjectLookup {
                subject,
                adjacencies: AdjacencyStuff {
                    predicates,
                    sp_offset,
                    sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
                }
            }))
        }
        else {
            None
        }
    }

    fn object_additions(&self) -> Box<dyn Iterator<Item=Box<dyn LayerObjectLookup>>> {
        let cloned = self.clone();
        Box::new((0..self.pos_objects.len())
                 .map(move |mapped_object| Box::new(cloned.lookup_layer_object_addition_mapped((mapped_object+1) as u64)) as Box<dyn LayerObjectLookup>))
    }

    fn object_removals(&self) -> Box<dyn Iterator<Item=Box<dyn LayerObjectLookup>>> {
        let cloned = self.clone();
        Box::new((0..self.neg_objects.len())
                 .map(move |mapped_object| Box::new(cloned.lookup_layer_object_removal_mapped((mapped_object+1) as u64)) as Box<dyn LayerObjectLookup>))
    }

    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        self.pos_objects.index_of(object)
            .map(|index| Box::new(self.lookup_layer_object_addition_mapped((index as u64)+1)) as Box<dyn LayerObjectLookup>)
    }

    fn lookup_object_removal(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>> {
        self.neg_objects.index_of(object)
            .map(|index| Box::new(self.lookup_layer_object_removal_mapped((index as u64)+1)) as Box<dyn LayerObjectLookup>)
    }


    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        self.pos_predicate_wavelet_tree.lookup(predicate)
            .map(|lookup| Box::new(ChildLayerPredicateLookup {
                predicate,
                lookup,
                subjects: self.pos_subjects.clone(),
                s_p_adjacency_list: self.pos_s_p_adjacency_list.clone(),
                sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
            }) as Box<dyn LayerPredicateLookup>)
    }

    fn lookup_predicate_removal(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>> {
        self.neg_predicate_wavelet_tree.lookup(predicate)
            .map(|lookup| Box::new(ChildLayerPredicateLookup {
                predicate,
                lookup,
                subjects: self.neg_subjects.clone(),
                s_p_adjacency_list: self.neg_s_p_adjacency_list.clone(),
                sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
            }) as Box<dyn LayerPredicateLookup>)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct AdjacencyStuff<M:'static+AsRef<[u8]>+Clone> {
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}

struct ChildLayerSubjectLookup<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,

    adjacencies: AdjacencyStuff<M>,
}

impl<M:AsRef<[u8]>+Clone> LayerSubjectLookup for ChildLayerSubjectLookup<M> {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicates(&self) -> Box<dyn Iterator<Item=Box<dyn LayerSubjectPredicateLookup>>> {
        let subject = self.subject;
        let offset = self.adjacencies.sp_offset;
        let aj = self.adjacencies.sp_o_adjacency_list.clone();
        Box::new(self.adjacencies.predicates.clone().into_iter().enumerate()
                 .map(move |(c,p)| Box::new(ChildLayerSubjectPredicateLookup {
                     subject: subject,
                     predicate: p,
                     objects: aj.get(offset+(c as u64)+1)
                 }) as Box<dyn LayerSubjectPredicateLookup>))
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn LayerSubjectPredicateLookup>> {
        self.adjacencies.predicates.iter().position(|p| p == predicate)
            .map(|pos| self.adjacencies.sp_o_adjacency_list.get(self.adjacencies.sp_offset+(pos as u64)+1))
            .map(|objects| Box::new(ChildLayerSubjectPredicateLookup {
                subject: self.subject,
                predicate: predicate,
                objects: objects
            }) as Box<dyn LayerSubjectPredicateLookup>)
    }
}


struct ChildLayerSubjectPredicateLookup<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    predicate: u64,
    objects: LogArraySlice<M>,
}

impl<M:'static+AsRef<[u8]>+Clone> LayerSubjectPredicateLookup for ChildLayerSubjectPredicateLookup<M> {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn objects(&self) -> Box<dyn Iterator<Item=u64>> {
        Box::new(self.objects.clone().into_iter())
    }

    fn has_object(&self, object: u64) -> bool {
        self.objects.iter().find(|&x|x==object).is_some()
    }
}

struct ChildLayerObjectLookup<M:'static+AsRef<[u8]>+Clone> {
    object: u64,
    sp_slice: LogArraySlice<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    subjects: MonotonicLogArray<M>,
}

impl<M:'static+AsRef<[u8]>+Clone> LayerObjectLookup for ChildLayerObjectLookup<M> {
    fn object(&self) -> u64 {
        self.object
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item=(u64, u64)>> {
        let sp_slice = self.sp_slice.clone();
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let subjects = self.subjects.clone();
        Box::new(sp_slice.into_iter()
            .map(move |index| s_p_adjacency_list.pair_at_pos(index-1))
            .map(move |(mapped_subject, predicate)| (subjects.entry((mapped_subject as usize)-1), predicate)))
    }
}

struct ChildLayerPredicateLookup<M:'static+AsRef<[u8]>+Clone> {
    predicate: u64,
    lookup: WaveletLookup<M>,
    subjects: MonotonicLogArray<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>,
}

impl<M:'static+AsRef<[u8]>+Clone> LayerPredicateLookup for ChildLayerPredicateLookup<M> {
    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item=Box<dyn LayerSubjectPredicateLookup>>> {
        let predicate = self.predicate;
        let subjects = self.subjects.clone();
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let sp_o_adjacency_list = self.sp_o_adjacency_list.clone();
        
        Box::new(self.lookup.iter()
                 .map(move |pos| {
                     let (mapped_subject, _predicate) = s_p_adjacency_list.pair_at_pos(pos);
                      Box::new(ChildLayerSubjectPredicateLookup {
                          subject: subjects.entry((mapped_subject as usize)-1),
                          predicate: predicate,
                          objects: sp_o_adjacency_list.get(pos+1)
                      }) as Box<dyn LayerSubjectPredicateLookup>
                 }))
    }
}

/// A builder for a child layer.
///
/// This builder takes node, predicate and value strings in lexical
/// order through the corresponding `add_<thing>` methods. When
/// they're all added, `into_phase2()` is to be called to turn this
/// builder into a second builder that takes triple data.
pub struct ChildLayerFileBuilder<F:'static+FileLoad+FileStore+Clone+Send+Sync> {
    parent: Arc<dyn Layer>,
    files: ChildLayerFiles<F>,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F:'static+FileLoad+FileStore+Clone+Send+Sync> ChildLayerFileBuilder<F> {
    /// Create the builder from the given files.
    pub fn from_files(parent: Arc<dyn Layer>, files: &ChildLayerFiles<F>) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(files.node_dictionary_files.blocks_file.open_write(), files.node_dictionary_files.offsets_file.open_write());
        let predicate_dictionary_builder = PfcDictFileBuilder::new(files.predicate_dictionary_files.blocks_file.open_write(), files.predicate_dictionary_files.offsets_file.open_write());
        let value_dictionary_builder = PfcDictFileBuilder::new(files.value_dictionary_files.blocks_file.open_write(), files.value_dictionary_files.offsets_file.open_write());


        Self {
            parent,
            files: files.clone(),

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        }
    }

    /// Add a node string.
    ///
    /// Does nothing if the node already exists in the paretn, and
    /// panics if the given node string is not a lexical successor of
    /// the previous node string.
    pub fn add_node(self, node: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>+Send> {
        match self.parent.subject_id(node) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;
                Box::new(node_dictionary_builder.add(node)
                         .map(move|(result, node_dictionary_builder)| (result, ChildLayerFileBuilder {
                             parent,
                             files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    /// Add a predicate string.
    ///
    /// Does nothing if the predicate already exists in the paretn, and
    /// panics if the given predicate string is not a lexical successor of
    /// the previous predicate string.
    pub fn add_predicate(self, predicate: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>+Send> {
        match self.parent.predicate_id(predicate) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;

                Box::new(predicate_dictionary_builder.add(predicate)
                         .map(move|(result, predicate_dictionary_builder)| (result, ChildLayerFileBuilder {
                             parent,
                             files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    /// Add a value string.
    ///
    /// Does nothing if the value already exists in the paretn, and
    /// panics if the given value string is not a lexical successor of
    /// the previous value string.
    pub fn add_value(self, value: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>+Send> {
        match self.parent.object_value_id(value) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;
                Box::new(value_dictionary_builder.add(value)
                         .map(move|(result, value_dictionary_builder)| (result, ChildLayerFileBuilder {
                             parent,
                             files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous
    /// added nodes are a lexical succesor of any of these
    /// nodes. Skips any nodes that are already part of the base
    /// layer.
    pub fn add_nodes<I:'static+IntoIterator<Item=String>>(self, nodes: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(nodes.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), node|
                  builder.add_node(&node)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if
    /// previous added predicates are a lexical succesor of any of
    /// these predicates. Skips any predicates that are already part
    /// of the base layer.
    pub fn add_predicates<I:'static+IntoIterator<Item=String>>(self, predicates: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(predicates.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), predicate|
                  builder.add_predicate(&predicate)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous
    /// added values are a lexical succesor of any of these
    /// values. Skips any nodes that are already part of the base
    /// layer.
    pub fn add_values<I:'static+IntoIterator<Item=String>>(self, values: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(values.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), value|
                  builder.add_value(&value)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    /// Turn this builder into a phase 2 builder that will take triple data.
    pub fn into_phase2(self) -> impl Future<Item=ChildLayerFileBuilderPhase2<F>,Error=std::io::Error> {
        let ChildLayerFileBuilder {
            parent,
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        let finalize_nodedict: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(node_dictionary_builder.finalize());
        let finalize_preddict: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(predicate_dictionary_builder.finalize());
        let finalize_valdict: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(value_dictionary_builder.finalize());

        let dict_maps_fut = vec![files.node_dictionary_files.blocks_file.map(),
                                 files.node_dictionary_files.offsets_file.map(),
                                 files.predicate_dictionary_files.blocks_file.map(),
                                 files.predicate_dictionary_files.offsets_file.map(),
                                 files.value_dictionary_files.blocks_file.map(),
                                 files.value_dictionary_files.offsets_file.map()];

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .and_then(move |_| future::join_all(dict_maps_fut))
            .and_then(move |dict_maps| {
                let node_dict_r = PfcDict::parse(dict_maps[0].clone(),
                                                 dict_maps[1].clone());
                if node_dict_r.is_err() {
                    return future::err(node_dict_r.err().unwrap().into());
                }
                let node_dict = node_dict_r.unwrap();

                let pred_dict_r = PfcDict::parse(dict_maps[2].clone(),
                                                 dict_maps[3].clone());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(dict_maps[4].clone(),
                                                dict_maps[5].clone());
                if val_dict_r.is_err() {
                    return future::err(val_dict_r.err().unwrap().into());
                }
                let val_dict = val_dict_r.unwrap();

                let num_nodes = node_dict.len();
                let num_predicates = pred_dict.len();
                let num_values = val_dict.len();

                future::ok(ChildLayerFileBuilderPhase2::new(parent,
                                                            files,

                                                            num_nodes,
                                                            num_predicates,
                                                            num_values))
            })
    }
}

/// Second phase of child layer building.
///
/// This builder takes ordered triple additions and removals. When all
/// data has been added, `finalize()` will build a layer.
pub struct ChildLayerFileBuilderPhase2<F:'static+FileLoad+FileStore+Clone+Send+Sync> {
    parent: Arc<dyn Layer>,

    files: ChildLayerFiles<F>,
    pos_subjects: Vec<u64>,
    neg_subjects: Vec<u64>,

    pos_s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    pos_sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    pos_last_subject: u64,
    pos_last_predicate: u64,

    neg_s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    neg_sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    neg_last_subject: u64,
    neg_last_predicate: u64,
}

impl<F:'static+FileLoad+FileStore+Clone+Send+Sync> ChildLayerFileBuilderPhase2<F> {
    fn new(parent: Arc<dyn Layer>,
           files: ChildLayerFiles<F>,

           num_nodes: usize,
           num_predicates: usize,
           num_values: usize
    ) -> Self {
        let pos_subjects = Vec::new();
        let neg_subjects = Vec::new();
        let parent_counts = parent.all_counts();
        let s_p_width = ((parent_counts.predicate_count + num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((parent_counts.node_count + parent_counts.value_count + num_nodes + num_values + 1) as f32).log2().ceil() as u8;

        let f = files.clone();

        let pos_s_p_adjacency_list_builder = AdjacencyListBuilder::new(files.pos_s_p_adjacency_list_files.bitindex_files.bits_file,
                                                                       files.pos_s_p_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                       files.pos_s_p_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                       files.pos_s_p_adjacency_list_files.nums_file.open_write(),
                                                                       s_p_width);

        let pos_sp_o_adjacency_list_builder = AdjacencyListBuilder::new(files.pos_sp_o_adjacency_list_files.bitindex_files.bits_file,
                                                                        files.pos_sp_o_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                        files.pos_sp_o_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                        files.pos_sp_o_adjacency_list_files.nums_file.open_write(),
                                                                        sp_o_width);

        let neg_s_p_adjacency_list_builder = AdjacencyListBuilder::new(files.neg_s_p_adjacency_list_files.bitindex_files.bits_file,
                                                                       files.neg_s_p_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                       files.neg_s_p_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                       files.neg_s_p_adjacency_list_files.nums_file.open_write(),
                                                                       s_p_width);

        let neg_sp_o_adjacency_list_builder = AdjacencyListBuilder::new(files.neg_sp_o_adjacency_list_files.bitindex_files.bits_file,
                                                                        files.neg_sp_o_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                        files.neg_sp_o_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                        files.neg_sp_o_adjacency_list_files.nums_file.open_write(),
                                                                        sp_o_width);

        ChildLayerFileBuilderPhase2 {
            parent,
            files: f,
            pos_subjects,
            neg_subjects,

            pos_s_p_adjacency_list_builder,
            pos_sp_o_adjacency_list_builder,
            pos_last_subject: 0,
            pos_last_predicate: 0,

            neg_s_p_adjacency_list_builder,
            neg_sp_o_adjacency_list_builder,
            neg_last_subject: 0,
            neg_last_predicate: 0,
        }
    }

    /// Add the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been added,
    /// and do nothing if the triple is already part of the parent.
    pub fn add_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>+Send> {
        if self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            // TODO maybe return an error instead?
            return Box::new(future::ok(self));
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            files,
            mut pos_subjects,
            neg_subjects,

            pos_s_p_adjacency_list_builder,
            pos_sp_o_adjacency_list_builder,
            pos_last_subject,
            pos_last_predicate,

            neg_s_p_adjacency_list_builder,
            neg_sp_o_adjacency_list_builder,
            neg_last_subject,
            neg_last_predicate,
        } = self;

        // TODO make this a proper error, rather than a panic
        match subject.cmp(&pos_last_subject) {
            Ordering::Less => panic!("layer builder got addition in wrong order (subject is {} while previously {} was pushed)", subject, pos_last_subject),
            Ordering::Equal => {},
            Ordering::Greater => pos_subjects.push(subject)
        };

        if pos_last_subject == subject && pos_last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = pos_s_p_adjacency_list_builder.count() + 1;
            Box::new(pos_sp_o_adjacency_list_builder.push(count, object)
                     .map(move |pos_sp_o_adjacency_list_builder| {
                         ChildLayerFileBuilderPhase2 {
                             parent,
                             files,
                             pos_subjects,
                             neg_subjects,

                             pos_s_p_adjacency_list_builder,
                             pos_sp_o_adjacency_list_builder,
                             pos_last_subject: subject,
                             pos_last_predicate: predicate,

                             neg_s_p_adjacency_list_builder,
                             neg_sp_o_adjacency_list_builder,
                             neg_last_subject,
                             neg_last_predicate,
                         }
                     }))
        }
        else {
            // both list have to be pushed to
            let mapped_subject = pos_subjects.len() as u64;

            Box::new(
                pos_s_p_adjacency_list_builder.push(mapped_subject, predicate)
                    .and_then(move |pos_s_p_adjacency_list_builder| {
                        let count = pos_s_p_adjacency_list_builder.count() + 1;
                        pos_sp_o_adjacency_list_builder.push(count, object)
                            .map(move |pos_sp_o_adjacency_list_builder| {
                                ChildLayerFileBuilderPhase2 {
                                    parent,
                                    files,
                                    pos_subjects,
                                    neg_subjects,

                                    pos_s_p_adjacency_list_builder,
                                    pos_sp_o_adjacency_list_builder,
                                    pos_last_subject: subject,
                                    pos_last_predicate: predicate,

                                    neg_s_p_adjacency_list_builder,
                                    neg_sp_o_adjacency_list_builder,
                                    neg_last_subject,
                                    neg_last_predicate,
                                }
                            })
                    }))
        }
    }

    /// Remove the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been removed,
    /// and do nothing if the parent doesn't know aobut this triple.
    pub fn remove_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>+Send> {
        if !self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            // TODO maybe return an error instead?
            return Box::new(future::ok(self));
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            files,
            pos_subjects,
            mut neg_subjects,

            pos_s_p_adjacency_list_builder,
            pos_sp_o_adjacency_list_builder,
            pos_last_subject,
            pos_last_predicate,

            neg_s_p_adjacency_list_builder,
            neg_sp_o_adjacency_list_builder,
            neg_last_subject,
            neg_last_predicate,
        } = self;

        // TODO make this a proper error, rather than a panic
        match subject.cmp(&neg_last_subject) {
            Ordering::Less => panic!("layer builder got removal in wrong order (subject is {} while previously {} was pushed)", subject, neg_last_subject),
            Ordering::Equal => {},
            Ordering::Greater => neg_subjects.push(subject)
        }

        if neg_last_subject == subject && neg_last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = neg_s_p_adjacency_list_builder.count() + 1;
            Box::new(neg_sp_o_adjacency_list_builder.push(count, object)
                     .map(move |neg_sp_o_adjacency_list_builder| {
                         ChildLayerFileBuilderPhase2 {
                             parent,
                             files,
                             pos_subjects,
                             neg_subjects,

                             pos_s_p_adjacency_list_builder,
                             pos_sp_o_adjacency_list_builder,
                             pos_last_subject,
                             pos_last_predicate,

                             neg_s_p_adjacency_list_builder,
                             neg_sp_o_adjacency_list_builder,
                             neg_last_subject: subject,
                             neg_last_predicate: predicate,
                         }
                     }))
        }
        else {
            // both list have to be pushed to
            let mapped_subject = neg_subjects.len() as u64;

            Box::new(
                neg_s_p_adjacency_list_builder.push(mapped_subject, predicate)
                    .and_then(move |neg_s_p_adjacency_list_builder| {
                        let count = neg_s_p_adjacency_list_builder.count() + 1;
                        neg_sp_o_adjacency_list_builder.push(count, object)
                            .map(move |neg_sp_o_adjacency_list_builder| {
                                ChildLayerFileBuilderPhase2 {
                                    parent,
                                    files,
                                   pos_subjects,
                                    neg_subjects,

                                    pos_s_p_adjacency_list_builder,
                                    pos_sp_o_adjacency_list_builder,
                                    pos_last_subject,
                                    pos_last_predicate,

                                    neg_s_p_adjacency_list_builder,
                                    neg_sp_o_adjacency_list_builder,
                                    neg_last_subject: subject,
                                    neg_last_predicate: predicate,
                                }
                            })
                    }))
        }
    }

    /// Add the given triple.
    ///
    /// This will panic if a greater triple has already been added,
    /// and do nothing if the parent already contains this triple.
    pub fn add_id_triples<I:'static+IntoIterator<Item=IdTriple>>(self, triples: I) -> impl Future<Item=Self, Error=std::io::Error> {
        stream::iter_ok(triples)
                 .fold(self, |b, triple| b.add_triple(triple.subject, triple.predicate, triple.object))
    }

    /// Remove the given triple.
    ///
    /// This will panic if a greater triple has already been removed,
    /// and do nothing if the parent doesn't know aobut this triple.
    pub fn remove_id_triples<I:'static+IntoIterator<Item=IdTriple>>(self, triples: I) -> impl Future<Item=Self, Error=std::io::Error> {
        stream::iter_ok(triples)
                 .fold(self, |b, triple| b.remove_triple(triple.subject, triple.predicate, triple.object))
    }

    /// Write the layer data to storage.
    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        let max_pos_subject = if self.pos_subjects.len() == 0 { 0 } else { self.pos_subjects[self.pos_subjects.len() - 1] };
        let max_neg_subject = if self.neg_subjects.len() == 0 { 0 } else { self.neg_subjects[self.neg_subjects.len() - 1] };
        let pos_subjects_width = 1+(max_pos_subject as f32).log2().ceil() as u8;
        let neg_subjects_width = 1+(max_neg_subject as f32).log2().ceil() as u8;
        let pos_subjects_logarray_builder = LogArrayFileBuilder::new(self.files.pos_subjects_file.open_write(), pos_subjects_width);
        let neg_subjects_logarray_builder = LogArrayFileBuilder::new(self.files.neg_subjects_file.open_write(), neg_subjects_width);

        let build_pos_s_p_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(self.pos_s_p_adjacency_list_builder.finalize());
        let build_pos_sp_o_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(self.pos_sp_o_adjacency_list_builder.finalize());
        let build_neg_s_p_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(self.neg_s_p_adjacency_list_builder.finalize());
        let build_neg_sp_o_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(self.neg_sp_o_adjacency_list_builder.finalize());

        let build_pos_subjects: Box<dyn Future<Item=(),Error=std::io::Error>+Send> = Box::new(pos_subjects_logarray_builder.push_all(stream::iter_ok(self.pos_subjects))
                                                                                         .and_then(|b|b.finalize())
                                                                                         .map(|_|()));
        let build_neg_subjects: Box<dyn Future<Item=(), Error=std::io::Error>+Send> = Box::new(neg_subjects_logarray_builder.push_all(stream::iter_ok(self.neg_subjects))
                                                                                          .and_then(|b|b.finalize())
                                                                                          .map(|_|()));

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

        future::join_all(vec![build_pos_s_p_adjacency_list,
                              build_pos_sp_o_adjacency_list,
                              build_neg_s_p_adjacency_list,
                              build_neg_sp_o_adjacency_list,
                              build_pos_subjects,
                              build_neg_subjects])
            .and_then(|_| build_object_index(pos_sp_o_files, pos_o_ps_files, pos_objects_file)
                      .join(build_object_index(neg_sp_o_files, neg_o_ps_files, neg_objects_file))
                      .join(build_wavelet_tree_from_logarray(pos_s_p_files.nums_file,
                                                             pos_predicate_wavelet_tree_files.bits_file,
                                                             pos_predicate_wavelet_tree_files.blocks_file,
                                                             pos_predicate_wavelet_tree_files.sblocks_file))
                      .join(build_wavelet_tree_from_logarray(neg_s_p_files.nums_file,
                                                             neg_predicate_wavelet_tree_files.bits_file,
                                                             neg_predicate_wavelet_tree_files.blocks_file,
                                                             neg_predicate_wavelet_tree_files.sblocks_file)))
            .map(|_|())
    }
}

fn build_object_index<F:'static+FileLoad+FileStore>(sp_o_files: AdjacencyListFiles<F>, o_ps_files: AdjacencyListFiles<F>, objects_file: F) -> impl Future<Item=(),Error=std::io::Error> {
    adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file)
        .map(|(left, right)| (right, left))
        .fold((BTreeSet::new(),BTreeSet::new(), 0), |(mut pairs_set, mut objects_set, _), (left, right)| {
            pairs_set.insert((left, right));
            objects_set.insert(left);
            future::ok::<_,std::io::Error>((pairs_set, objects_set, right))
        })
        .and_then(move |(pairs, objects, greatest_sp)| {
            let greatest_object = objects.iter().next_back().unwrap_or(&0);
            let objects_width = ((*greatest_object+1) as f32).log2().ceil() as u8;
            let aj_width = ((greatest_sp+1) as f32).log2().ceil() as u8;

            let o_ps_adjacency_list_builder = AdjacencyListBuilder::new(o_ps_files.bitindex_files.bits_file,
                                                                        o_ps_files.bitindex_files.blocks_file.open_write(),
                                                                        o_ps_files.bitindex_files.sblocks_file.open_write(),
                                                                        o_ps_files.nums_file.open_write(),
                                                                        aj_width);
            let objects_builder = LogArrayFileBuilder::new(objects_file.open_write(), objects_width);

            let compressed_pairs = pairs.into_iter()
                .scan((0,0), |(compressed, last), (left, right)| {
                    if left > *last {
                        *compressed += 1;
                    }

                    *last = left;

                    Some((*compressed, right))
                }).collect::<Vec<_>>();

            let build_o_ps_task = o_ps_adjacency_list_builder.push_all(stream::iter_ok(compressed_pairs))
                .and_then(|builder| builder.finalize());

            let build_objects_task = objects_builder.push_all(stream::iter_ok(objects))
                .and_then(|builder| builder.finalize());

            build_o_ps_task.join(build_objects_task)
        })
        .map(|_|())
}

pub struct ChildTripleStream<S1: Stream<Item=u64,Error=io::Error>, S2: Stream<Item=(u64,u64),Error=io::Error>+Send> {
    subjects_stream: stream::Peekable<S1>,
    s_p_stream: stream::Peekable<S2>,
    sp_o_stream: stream::Peekable<S2>,
    last_mapped_s: u64,
    last_s_p: (u64, u64),
    last_sp: u64,
}

impl<S1: Stream<Item=u64,Error=io::Error>, S2: Stream<Item=(u64,u64),Error=io::Error>+Send> ChildTripleStream<S1,S2> {
    fn new(subjects_stream: S1, s_p_stream: S2, sp_o_stream: S2) -> ChildTripleStream<S1,S2> {
        ChildTripleStream {
            subjects_stream: subjects_stream.peekable(),
            s_p_stream: s_p_stream.peekable(),
            sp_o_stream: sp_o_stream.peekable(),
            last_mapped_s: 0,
            last_s_p: (0,0),
            last_sp: 0
        }
    }
}

impl<S1: Stream<Item=u64,Error=io::Error>, S2: Stream<Item=(u64,u64),Error=io::Error>+Send> Stream for ChildTripleStream<S1,S2> {
    type Item = (u64,u64,u64);
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<(u64,u64,u64)>>,io::Error> {
        let sp_o = self.sp_o_stream.peek().map(|x|x.map(|x|x.map(|x|*x)));
        match sp_o {
            Err(e) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(Some((sp,o)))) => {
                if sp > self.last_sp {
                    let s_p = self.s_p_stream.peek().map(|x|x.map(|x|x.map(|x|*x)));
                    match s_p {
                        Err(e) => Err(e),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Ok(Async::Ready(None)) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of s_p_stream")),
                        Ok(Async::Ready(Some((s,p)))) => {
                            if s > self.last_s_p.0 {
                                let mapped_s = self.subjects_stream.peek().map(|x|x.map(|x|x.map(|x|*x)));
                                match mapped_s {
                                    Err(e) => Err(e),
                                    Ok(Async::NotReady) => Ok(Async::NotReady),
                                    Ok(Async::Ready(None)) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of subjects_stream")),
                                    Ok(Async::Ready(Some(mapped_s))) => {
                                        self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");
                                        self.s_p_stream.poll().expect("peeked stream s_p_stream with confirmed result did not have result on poll");
                                        self.subjects_stream.poll().expect("peeked stream subjects_stream with confirmed result did not have result on poll");
                                        self.last_mapped_s = mapped_s;
                                        self.last_s_p = (s,p);
                                        self.last_sp = sp;

                                        Ok(Async::Ready(Some((mapped_s,p,o))))
                                    }
                                }
                            }
                            else {
                                self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");
                                self.s_p_stream.poll().expect("peeked stream s_p_stream with confirmed result did not have result on poll");
                                self.last_s_p = (s,p);
                                self.last_sp = sp;

                                Ok(Async::Ready(Some((self.last_mapped_s,p,o))))
                            }
                        }
                    }
                }
                else {
                    self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");

                    Ok(Async::Ready(Some((self.last_mapped_s, self.last_s_p.1, o))))
                }
            }
        }
    }
}

pub fn open_child_triple_stream<F:'static+FileLoad+FileStore>(subjects_file: F, s_p_files: AdjacencyListFiles<F>, sp_o_files: AdjacencyListFiles<F>) -> impl Stream<Item=(u64,u64,u64),Error=io::Error>+Send {
    let subjects_stream = logarray_stream_entries(subjects_file);
    let s_p_stream = adjacency_list_stream_pairs(s_p_files.bitindex_files.bits_file, s_p_files.nums_file);
    let sp_o_stream = adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file);

    ChildTripleStream::new(subjects_stream, s_p_stream, sp_o_stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use crate::layer::base::tests::*;
    fn child_layer_files() -> ChildLayerFiles<MemoryBackedStore> {
        let files: Vec<_> = (0..40).map(|_| MemoryBackedStore::new()).collect();

        ChildLayerFiles {
            node_dictionary_files: DictionaryFiles {
                blocks_file: files[0].clone(),
                offsets_file: files[1].clone()
            },
            predicate_dictionary_files: DictionaryFiles {
                blocks_file: files[2].clone(),
                offsets_file: files[3].clone()
            },
            value_dictionary_files: DictionaryFiles {
                blocks_file: files[4].clone(),
                offsets_file: files[5].clone()
            },

            pos_subjects_file: files[6].clone(),
            pos_objects_file: files[7].clone(),
            neg_subjects_file: files[8].clone(),
            neg_objects_file: files[9].clone(),

            pos_s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[10].clone(),
                    blocks_file: files[11].clone(),
                    sblocks_file: files[12].clone(),
                },
                nums_file: files[13].clone()
            },
            pos_sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[14].clone(),
                    blocks_file: files[15].clone(),
                    sblocks_file: files[16].clone(),
                },
                nums_file: files[17].clone()
            },
            pos_o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[18].clone(),
                    blocks_file: files[19].clone(),
                    sblocks_file: files[20].clone(),
                },
                nums_file: files[21].clone()
            },
            neg_s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[22].clone(),
                    blocks_file: files[23].clone(),
                    sblocks_file: files[24].clone(),
                },
                nums_file: files[25].clone()
            },
            neg_sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[26].clone(),
                    blocks_file: files[27].clone(),
                    sblocks_file: files[28].clone(),
                },
                nums_file: files[29].clone()
            },
            neg_o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[30].clone(),
                    blocks_file: files[31].clone(),
                    sblocks_file: files[32].clone(),
                },
                nums_file: files[33].clone()
            },
            pos_predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[34].clone(),
                blocks_file: files[35].clone(),
                sblocks_file: files[36].clone()
            },
            neg_predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[37].clone(),
                blocks_file: files[38].clone(),
                sblocks_file: files[39].clone()
            },
        }
    }

    #[test]
    fn empty_child_layer_equivalent_to_parent() {
        let base_layer = example_base_layer();

        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert!(child_layer.triple_exists(1,1,1));
        assert!(child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }

    #[test]
    fn child_layer_can_have_inserts() {
        let base_layer = example_base_layer();

        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(2,1,2))
            .and_then(|b| b.add_triple(3,3,3))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert!(child_layer.triple_exists(1,1,1));
        assert!(child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,2));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,3));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }

    #[test]
    fn child_layer_can_have_deletes() {
        let base_layer = example_base_layer();

        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert!(child_layer.triple_exists(1,1,1));
        assert!(!child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(!child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }

    #[test]
    fn child_layer_can_have_inserts_and_deletes() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert!(child_layer.triple_exists(1,1,1));
        assert!(child_layer.triple_exists(1,2,3));
        assert!(child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,4));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(!child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }

    #[test]
    fn iterate_child_layer_triples() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let subjects: Vec<_> = child_layer.triples()
            .map(|t|(t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1,1,1),
                        (1,2,3),
                        (2,1,1),
                        (2,1,3),
                        (2,3,4),
                        (2,3,6),
                        (3,3,6),
                        (4,3,6)], subjects);
    }

    #[test]
    fn iterate_child_layer_triples_by_object() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let triples: Vec<_> = child_layer.objects()
            .map(|o|o.triples())
            .flatten()
            .map(|t|(t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1,1,1),
                        (2,1,1),
                        (1,2,3),
                        (2,1,3),
                        (2,3,4),
                        (2,3,6),
                        (3,3,6),
                        (4,3,6)], triples);
    }

    #[test]
    fn iterate_child_layer_triples_by_objects_with_equal_predicates() {
        let base_layer = empty_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .add_node("a")
            .and_then(|(_,b)|b.add_predicate("b"))
            .and_then(|(_,b)|b.add_predicate("c"))
            .and_then(|(_,b)|b.add_value("d"))

            .and_then(|(_,b)|b.into_phase2())

            .and_then(|b| b.add_triple(1,1,1))
            .and_then(|b| b.add_triple(1,1,2))
            .and_then(|b| b.add_triple(1,2,1))
            .and_then(|b| b.add_triple(2,1,1))
            .and_then(|b| b.add_triple(2,2,1))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let triples: Vec<_> = child_layer.objects()
            .map(|o|o.triples())
            .flatten()
            .map(|t|(t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1,1,1),
                        (1,2,1),
                        (2,1,1),
                        (2,2,1),
                        (1,1,2)], triples);
    }

    #[test]
    fn lookup_child_layer_triples_by_predicate() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let lookup = child_layer.lookup_predicate(1).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs()
            .map(|sp|sp.triples())
            .flatten()
            .map(|t|(t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(1,1,1), (2,1,1), (2,1,3)], pairs);

        let lookup = child_layer.lookup_predicate(2).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs()
            .map(|sp|sp.triples())
            .flatten()
            .map(|t|(t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(1,2,3)], pairs);

        let lookup = child_layer.lookup_predicate(3).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs()
            .map(|sp|sp.triples())
            .flatten()
            .map(|t|(t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,3,4),(2,3,6),(3,3,6),(4,3,6)], pairs);

        let lookup = child_layer.lookup_predicate(4);

        assert!(lookup.is_none());
    }

    #[test]
    fn adding_new_nodes_predicates_and_values_in_child() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(11,2,3))
            .and_then(|b| b.add_triple(12,3,4))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert!(child_layer.triple_exists(11,2,3));
        assert!(child_layer.triple_exists(12,3,4));
    }

    #[test]
    fn old_dictionary_entries_in_child() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .add_node("foo")
            .and_then(|(_,b)|b.add_predicate("bar"))
            .and_then(|(_,b)|b.add_value("baz"))
            .and_then(|(_,b)|b.into_phase2())
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert_eq!(3, child_layer.subject_id("bbbbb").unwrap());
        assert_eq!(2, child_layer.predicate_id("fghij").unwrap());
        assert_eq!(1, child_layer.object_node_id("aaaaa").unwrap());
        assert_eq!(6, child_layer.object_value_id("chicken").unwrap());

        assert_eq!("bbbbb", child_layer.id_subject(3).unwrap());
        assert_eq!("fghij", child_layer.id_predicate(2).unwrap());
        assert_eq!(ObjectType::Node("aaaaa".to_string()), child_layer.id_object(1).unwrap());
        assert_eq!(ObjectType::Value("chicken".to_string()), child_layer.id_object(6).unwrap());
    }

    #[test]
    fn new_dictionary_entries_in_child() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder
            .add_node("foo")
            .and_then(|(_,b)|b.add_predicate("bar"))
            .and_then(|(_,b)|b.add_value("baz"))
            .and_then(|(_,b)|b.into_phase2())
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        assert_eq!(11, child_layer.subject_id("foo").unwrap());
        assert_eq!(5, child_layer.predicate_id("bar").unwrap());
        assert_eq!(11, child_layer.object_node_id("foo").unwrap());
        assert_eq!(12, child_layer.object_value_id("baz").unwrap());

        assert_eq!("foo", child_layer.id_subject(11).unwrap());
        assert_eq!("bar", child_layer.id_predicate(5).unwrap());
        assert_eq!(ObjectType::Node("foo".to_string()), child_layer.id_object(11).unwrap());
        assert_eq!(ObjectType::Value("baz".to_string()), child_layer.id_object(12).unwrap());
    }

    #[test]
    fn lookup_additions_by_subject() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.add_triple(2,2,2))
            .and_then(|b| b.add_triple(3,4,5))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let result: Vec<_> = child_layer.subject_additions()
            .map(|s|s.triples())
            .flatten()
            .map(|t| (t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(1,3,4),
                        (2,2,2),
                        (3,4,5)],
                   result);
    }

    #[test]
    fn lookup_additions_by_predicate() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.add_triple(2,2,2))
            .and_then(|b| b.add_triple(3,4,5))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let result: Vec<_> = child_layer.predicate_additions()
            .map(|s|s.triples())
            .flatten()
            .map(|t| (t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,2,2),
                        (1,3,4),
                        (3,4,5)],
                   result);
    }

    #[test]
    fn lookup_additions_by_object() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.add_triple(2,2,2))
            .and_then(|b| b.add_triple(3,4,5))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let result: Vec<_> = child_layer.object_additions()
            .map(|s|s.triples())
            .flatten()
            .map(|t| (t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,2,2),
                        (1,3,4),
                        (3,4,5)],
                   result);
    }

    #[test]
    fn lookup_removals_by_subject() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b| b.remove_triple(4,3,6))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let result: Vec<_> = child_layer.subject_removals()
            .map(|s|s.triples())
            .flatten()
            .map(|t| (t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,1,1),
                        (3,2,5),
                        (4,3,6)],
                   result);
    }

    #[test]
    fn lookup_removals_by_predicate() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(2,3,6))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let result: Vec<_> = child_layer.predicate_removals()
            .map(|s|s.triples())
            .flatten()
            .map(|t| (t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,1,1),
                        (3,2,5),
                        (2,3,6)],
                   result);
    }

    #[test]
    fn lookup_removals_by_object() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(2,3,6))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent, &child_files).wait().unwrap();

        let result: Vec<_> = child_layer.object_removals()
            .map(|s|s.triples())
            .flatten()
            .map(|t| (t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,1,1),
                        (3,2,5),
                        (2,3,6)],
                   result);
    }

    #[test]
    fn create_empty_child_layer() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,3,4))
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(2,3,6))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent.clone(), &child_files).wait().unwrap();

        assert_eq!(parent.node_and_value_count(), child_layer.node_and_value_count());
        assert_eq!(parent.predicate_count(), child_layer.predicate_count());
    }

    #[test]
    fn child_layer_with_multiple_pairs_pointing_at_same_object_lookup_by_objects() {
        let base_layer = empty_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let future = builder.add_nodes(vec!["a","b"].into_iter().map(|x|x.to_string()))
            .and_then(|(_,b)| b.add_predicates(vec!["c","d"].into_iter().map(|x|x.to_string())))
            .and_then(|(_,b)| b.add_values(vec!["e"].into_iter().map(|x|x.to_string())))
            .and_then(|(_,b)| b.into_phase2())
            .and_then(|b| b.add_triple(1,1,1))
            .and_then(|b| b.add_triple(1,2,1))
            .and_then(|b| b.add_triple(2,1,1))
            .and_then(|b| b.add_triple(2,2,1))
            .and_then(|b| b.finalize());


        future.wait().unwrap();

        let child_layer = ChildLayer::load_from_files([5,4,3,2,1], parent.clone(), &child_files).wait().unwrap();

        let triples_by_object: Vec<_> = child_layer.objects()
            .map(|o|o.subject_predicate_pairs()
                 .map(move |(s,p)|(s,p,o.object())))
            .flatten()
            .collect();

        assert_eq!(vec![(1,1,1),
                        (1,2,1),
                        (2,1,1),
                        (2,2,1)],
                   triples_by_object);
    }

    #[test]
    fn stream_child_triples() {
        let base_layer = example_base_layer();
        let parent = Arc::new(base_layer);

        let child_files = child_layer_files();
        let builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);

        let future = builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,1))
            .and_then(|b| b.add_triple(3,1,5))
            .and_then(|b| b.add_triple(5,2,3))
            .and_then(|b| b.add_triple(5,2,4))
            .and_then(|b| b.add_triple(5,2,5))
            .and_then(|b| b.add_triple(5,3,1))

            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(2,3,6))
            .and_then(|b| b.remove_triple(4,3,6))
            .and_then(|b| b.finalize());


        future.wait().unwrap();

        let addition_stream = open_child_triple_stream(child_files.pos_subjects_file, child_files.pos_s_p_adjacency_list_files, child_files.pos_sp_o_adjacency_list_files);
        let removal_stream = open_child_triple_stream(child_files.neg_subjects_file, child_files.neg_s_p_adjacency_list_files, child_files.neg_sp_o_adjacency_list_files);

        let addition_triples: Vec<_> = addition_stream.collect().wait().unwrap();
        let removal_triples: Vec<_> = removal_stream.collect().wait().unwrap();

        assert_eq!(vec![(1,2,1),
                        (3,1,5),
                        (5,2,3),
                        (5,2,4),
                        (5,2,5),
                        (5,3,1)], addition_triples);

        assert_eq!(vec![(2,1,1),
                        (2,3,6),
                        (4,3,6)], removal_triples);
    }
}
