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
use std::iter::Peekable;

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

    fn lookup_object_addition_mapped(&self, mapped_object: u64) -> impl ObjectLookup {
        if mapped_object == 0 || mapped_object as usize > self.pos_objects.len() {
            panic!("unknown mapped object requested");
        }
        let object = self.pos_objects.entry((mapped_object-1) as usize);

        let pos_sp_slice = self.pos_o_ps_adjacency_list.get(mapped_object);
        let pos = ChildObjectLookupAdjacency {
            subjects: self.pos_subjects.clone(),
            sp_slice: pos_sp_slice,
            s_p_adjacency_list: self.pos_s_p_adjacency_list.clone()
        };


        ChildObjectLookup::new(object, None, Some(pos), None)
    }

    fn lookup_object_removal_mapped(&self, mapped_object: u64) -> impl ObjectLookup {
        if mapped_object == 0 || mapped_object as usize > self.neg_objects.len() {
            panic!("unknown mapped object requested");
        }
        let object = self.neg_objects.entry((mapped_object-1) as usize);

        let neg_sp_slice = self.neg_o_ps_adjacency_list.get(mapped_object);
        // we pretend it's actually the pos part and reuse subject mapping in the child object lookup
        let pos = ChildObjectLookupAdjacency {
            subjects: self.neg_subjects.clone(),
            sp_slice: neg_sp_slice,
            s_p_adjacency_list: self.neg_s_p_adjacency_list.clone()
        };

        ChildObjectLookup::new(object, None, Some(pos), None)
    }

    fn parents(&self) -> Vec<Arc<dyn Layer>> {
        let mut parents: Vec<Arc<dyn Layer>> = Vec::new();
        let mut parent_option = self.parent();
        while let Some(parent) = parent_option {
            parent_option = parent.parent();
            parents.push(parent);
        }
        parents
    }

}

impl<M:'static+AsRef<[u8]>+Clone+Send+Sync> Layer for ChildLayer<M> {
    fn name(&self) -> [u32;5] {
        self.name
    }

    fn parent(&self) -> Option<Arc<dyn Layer>> {
        Some(self.parent.clone())
    }

    fn node_dict_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary.id(subject)
    }

    fn node_dict_len(&self) -> usize {
        self.node_dictionary.len()
    }

    fn node_dict_get(&self, id: usize) -> String {
        self.node_dictionary.get(id)
    }


    fn value_dict_get(&self, id: usize) -> String {
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

    fn predicate_dict_get(&self, id: usize) -> String {
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

    fn subject_id(&self, subject: &str) -> Option<u64> {
        let mut result: Option<u64> = self.node_dict_id(subject);
        let mut parent_option = self.parent();
        while let Some(parent) = parent_option {
            let next_parent = parent.parent();
            if result.is_some() {
                return Some(parent.node_and_value_count() as u64 + result.unwrap() + 1);
            }
            result = parent.node_dict_id(subject);
            if next_parent.is_none() {
                return parent.subject_id(subject);
            }
            parent_option = next_parent;
        }
        None
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        let mut result: Option<u64> = self.predicate_dict_id(predicate);
        let mut parent_option = self.parent();
        while let Some(parent) = parent_option {
            let next_parent = parent.parent();
            if result.is_some() {
                return Some(parent.predicate_count() as u64 + result.unwrap() + 1);
            }
            result = parent.predicate_dict_id(predicate);
            if next_parent.is_none() {
                return parent.predicate_id(predicate);
            }
            parent_option = next_parent;
        }
        None
    }

    fn object_node_id(&self, node: &str) -> Option<u64> {
        let mut result: Option<u64> = self.node_dict_id(node);
        let mut parent_option = self.parent();
        while let Some(parent) = parent_option {
            let next_parent = parent.parent();
            if result.is_some() {
                return Some(parent.node_and_value_count() as u64 + result.unwrap() + 1);
            }
            result = parent.node_dict_id(node);
            if next_parent.is_none() {
                return parent.object_node_id(node);
            }
            parent_option = next_parent;
        }
        None
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        let mut result: Option<u64> = self.value_dict_id(value);
        if result.is_some() {
            return Some(self.parent.node_and_value_count() as u64 + self.node_dict_len() as u64 + result.unwrap() + 1);
        }
        let mut parent_option = self.parent();
        while let Some(parent) = parent_option {
            let next_parent = parent.parent();
            if next_parent.is_none() {
                return parent.object_value_id(value);
            }
            result = parent.value_dict_id(value);
            if result.is_some() {
                return Some(next_parent.unwrap().node_and_value_count() as u64 + parent.node_dict_len() as u64 + result.unwrap() + 1);
            }
            parent_option = next_parent;
        }
        None
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        // TODO: Can we do this without cloning self? It is not that efficient
        let mut current_option: Option<Arc<dyn Layer>> = Some(Arc::new(self.clone()));
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            parent_count = parent_count - current_layer.node_dict_len() as u64 - current_layer.value_dict_len() as u64;
            if corrected_id >= parent_count as u64 {
                // subject, if it exists, is in this layer
                corrected_id -= parent_count;
                if corrected_id >= current_layer.node_dict_len() as u64 {
                    return None
                }
                else {
                    return Some(current_layer.node_dict_get(corrected_id as usize))
                }
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
        // TODO: Can we do this without cloning self? It is not that efficient
        let mut current_option: Option<Arc<dyn Layer>> = Some(Arc::new(self.clone()));
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
                if corrected_id >= current_layer.predicate_dict_len() as u64 {
                    return None
                }
                else {
                    return Some(current_layer.predicate_dict_get(corrected_id as usize))
                }
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
        // TODO: Can we do this without cloning self? It is not that efficient
        let mut current_option: Option<Arc<dyn Layer>> = Some(Arc::new(self.clone()));
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
                    if corrected_id >= current_layer.value_dict_len() as u64 {
                        return None;
                    }
                    else {
                        return Some(ObjectType::Value(current_layer.value_dict_get(corrected_id as usize)));
                    }
                }
                else {
                    return Some(ObjectType::Node(current_layer.node_dict_get(corrected_id as usize)));
                }
            }
            else {
                current_option = current_layer.parent();
            }
        }
        return None;
    }

    fn subjects(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        Box::new(ChildSubjectIterator {
            parent: Some(Box::new(self.parent.subjects())),
            next_parent_subject: None,

            pos: Some(ChildSubjectIteratorPart {
                subjects: self.pos_subjects.clone(),
                s_p_adjacency_list: self.pos_s_p_adjacency_list.clone(),
                sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone(),
                pos: 0
            }),

            neg: Some(ChildSubjectIteratorPart {
                subjects: self.neg_subjects.clone(),
                s_p_adjacency_list: self.neg_s_p_adjacency_list.clone(),
                sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone(),
                pos: 0
            }),
        })
    }

    fn subject_additions(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        Box::new(ChildSubjectIterator {
            parent: None,
            next_parent_subject: None,

            pos: Some(ChildSubjectIteratorPart {
                subjects: self.pos_subjects.clone(),
                s_p_adjacency_list: self.pos_s_p_adjacency_list.clone(),
                sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone(),
                pos: 0
            }),

            neg: None
        })
    }

    fn subject_removals(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        Box::new(ChildSubjectIterator {
            parent: None,
            next_parent_subject: None,

            pos: Some(ChildSubjectIteratorPart {
                subjects: self.neg_subjects.clone(),
                s_p_adjacency_list: self.neg_s_p_adjacency_list.clone(),
                sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone(),
                pos: 0
            }),

            neg: None
        })
    }

    fn lookup_subject_current_layer(&self, subject: u64, parent: Option<Box<dyn SubjectLookup>>) -> Option<Box<dyn SubjectLookup>> {
        let mut pos: Option<AdjacencyStuff<M>> = None;
        let mut neg: Option<AdjacencyStuff<M>> = None;
        let pos_index = self.pos_subjects.index_of(subject);
        let neg_index = self.neg_subjects.index_of(subject);

        if pos_index.is_some() {
            // subject is mentioned in this layer (as an insert), and might be in the parent layer as well
            let pos_mapped_subject = pos_index.unwrap() as u64 + 1;
            if pos_mapped_subject <= self.pos_s_p_adjacency_list.left_count() as u64 {
                let pos_predicates = self.pos_s_p_adjacency_list.get(pos_mapped_subject);
                let pos_sp_offset = self.pos_s_p_adjacency_list.offset_for(pos_mapped_subject);
                pos = Some(AdjacencyStuff {
                    predicates: pos_predicates,
                    sp_offset: pos_sp_offset,
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
                });
            }
        }
        if neg_index.is_some() {
            let neg_mapped_subject = neg_index.unwrap() as u64 + 1;
            if neg_mapped_subject <= self.neg_s_p_adjacency_list.left_count() as u64 {
                let neg_predicates = self.neg_s_p_adjacency_list.get(neg_mapped_subject);
                let neg_sp_offset = self.neg_s_p_adjacency_list.offset_for(neg_mapped_subject);

                neg = Some(AdjacencyStuff {
                    predicates: neg_predicates,
                    sp_offset: neg_sp_offset,
                    sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
                });
            }
        }
        return Some(Box::new(ChildSubjectLookup {
            parent,
            subject,
            pos,
            neg
        }));
    }

    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        if subject == 0 {
            return None;
        }
        let parents = self.parents();
        let latest_idx = parents.len();
        let mut latest_lookup: Option<Box<dyn SubjectLookup>> = None;
        for index in 0..latest_idx {
            let real_idx = latest_idx - index - 1;
            latest_lookup = parents[real_idx].lookup_subject_current_layer(subject, latest_lookup);
        }
        return self.lookup_subject_current_layer(subject, latest_lookup);
    }


    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
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
            Some(Box::new(ChildSubjectLookup {
                parent: None,
                subject,
                pos: Some(AdjacencyStuff {
                    predicates,
                    sp_offset,
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
                }),
                neg: None
            }))
        }
        else {
            None
        }
    }

    fn lookup_subject_removal(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
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
            Some(Box::new(ChildSubjectLookup {
                parent: None,
                subject,
                pos: Some(AdjacencyStuff {
                    predicates,
                    sp_offset,
                    sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
                }),
                neg: None
            }))
        }
        else {
            None
        }
    }

    fn objects(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        // todo: there might be a more efficient method than doing
        // this lookup over and over, due to sequentiality of the
        // underlying data structures
        let cloned = self.clone();
        Box::new((0..self.node_and_value_count())
                 .map(move |object| cloned.lookup_object((object+1) as u64))
                 .flatten())
    }

    fn object_additions(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        let cloned = self.clone();
        Box::new((0..self.pos_objects.len())
                 .map(move |mapped_object| Box::new(cloned.lookup_object_addition_mapped((mapped_object+1) as u64)) as Box<dyn ObjectLookup>))
    }

    fn object_removals(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        let cloned = self.clone();
        Box::new((0..self.neg_objects.len())
                 .map(move |mapped_object| Box::new(cloned.lookup_object_removal_mapped((mapped_object+1) as u64)) as Box<dyn ObjectLookup>))
    }

    fn lookup_object(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        let pos = self.pos_objects.index_of(object)
            .map(|index| self.pos_o_ps_adjacency_list.get((index as u64)+1))
            .map(|pos_sp_slice| ChildObjectLookupAdjacency {
                subjects: self.pos_subjects.clone(),
                sp_slice: pos_sp_slice,
                s_p_adjacency_list: self.pos_s_p_adjacency_list.clone()
            });
        // TODO: Eliminate recursion
        let parent = self.parent.lookup_object(object);
        if pos.is_none() && parent.is_none() {
            return None;
        }

        let neg = self.neg_objects.index_of(object)
            .map(|index| self.neg_o_ps_adjacency_list.get((index as u64)+1))
            .map(|neg_sp_slice| ChildObjectLookupAdjacency {
                subjects: self.neg_subjects.clone(),
                sp_slice: neg_sp_slice,
                s_p_adjacency_list: self.neg_s_p_adjacency_list.clone()
            });

        Some(Box::new(ChildObjectLookup::new(object, parent, pos, neg)))
    }

    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.pos_objects.index_of(object)
            .map(|index| Box::new(self.lookup_object_addition_mapped((index as u64)+1)) as Box<dyn ObjectLookup>)
    }

    fn lookup_object_removal(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.pos_objects.index_of(object)
            .map(|index| Box::new(self.lookup_object_removal_mapped((index as u64)+1)) as Box<dyn ObjectLookup>)
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        let parent = self.parent.lookup_predicate(predicate)
            .map(|parent| ChildPredicateLookupParentData {
                parent,
                neg_subjects: self.neg_subjects.clone(),
                neg_s_p_adjacency_list: self.neg_s_p_adjacency_list.clone(),
                neg_sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
            });
        let child = self.pos_predicate_wavelet_tree.lookup(predicate)
            .map(|lookup| ChildPredicateLookupChildData {
                lookup,
                pos_subjects: self.pos_subjects.clone(),
                pos_s_p_adjacency_list: self.pos_s_p_adjacency_list.clone(),
                pos_sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
            });

        if parent.is_none() && child.is_none() {
            None
        }
        else {
            Some(Box::new(ChildPredicateLookup {
                predicate,
                parent,
                child
            }))
        }
    }

    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        self.pos_predicate_wavelet_tree.lookup(predicate)
            .map(|lookup| Box::new(ChildPredicateLookup {
                predicate,
                parent: None,
                child: Some(ChildPredicateLookupChildData {
                    lookup,
                    pos_subjects: self.pos_subjects.clone(),
                    pos_s_p_adjacency_list: self.pos_s_p_adjacency_list.clone(),
                    pos_sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
                })
            }) as Box<dyn PredicateLookup>)
    }

    fn lookup_predicate_removal(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        self.neg_predicate_wavelet_tree.lookup(predicate)
            .map(|lookup| Box::new(ChildPredicateLookup {
                predicate,
                parent: None,
                child: Some(ChildPredicateLookupChildData {
                    lookup,
                    pos_subjects: self.neg_subjects.clone(),
                    pos_s_p_adjacency_list: self.neg_s_p_adjacency_list.clone(),
                    pos_sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
                })
            }) as Box<dyn PredicateLookup>)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }
}

struct ChildSubjectIteratorPart<M:'static+AsRef<[u8]>+Clone> {
    subjects: MonotonicLogArray<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>,
    pos: usize
}

struct ChildSubjectIterator<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<dyn Iterator<Item=Box<dyn SubjectLookup>>>>,
    next_parent_subject: Option<Box<dyn SubjectLookup>>,

    pos: Option<ChildSubjectIteratorPart<M>>,
    neg: Option<ChildSubjectIteratorPart<M>>,
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildSubjectIterator<M> {
    type Item = Box<dyn SubjectLookup>;

    fn next(&mut self) -> Option<Box<dyn SubjectLookup>> {
        if self.parent.is_some() && self.next_parent_subject.is_none() {
            self.next_parent_subject = self.parent.as_mut().unwrap().next();
            if self.next_parent_subject.is_none() {
                self.parent = None;
            }
        }

        let mut next_parent = None;
        std::mem::swap(&mut next_parent, &mut self.next_parent_subject);
        let next_parent_subject = next_parent.as_ref().map(|p|p.subject()).unwrap_or(0);

        let next_pos_subject = self.pos.as_ref().map(|pos| if pos.pos < pos.subjects.len() { pos.subjects.entry(pos.pos) } else { 0 }).unwrap_or(0);
        let next_neg_subject = self.neg.as_ref().map(|neg| if neg.pos < neg.subjects.len() { neg.subjects.entry(neg.pos) } else { 0 }).unwrap_or(0);

        let mut pos: Option<AdjacencyStuff<M>> = None;
        let mut neg: Option<AdjacencyStuff<M>> = None;

        let mut subject = next_parent_subject;

        if next_pos_subject != 0 && (next_parent_subject == 0 || next_pos_subject <= next_parent_subject) {
            subject = next_pos_subject;
            let selfpos = self.pos.as_mut().unwrap();
            pos = Some(AdjacencyStuff {
                predicates: selfpos.s_p_adjacency_list.get(1+selfpos.pos as u64),
                sp_offset: selfpos.s_p_adjacency_list.offset_for(1+selfpos.pos as u64),
                sp_o_adjacency_list: selfpos.sp_o_adjacency_list.clone()
            });

            selfpos.pos += 1;
        }

        if next_neg_subject != 0 && (next_pos_subject == 0 || next_parent_subject <= next_pos_subject) && next_parent_subject == next_neg_subject {
            let selfneg = self.neg.as_mut().unwrap();
            neg = Some(AdjacencyStuff {
                predicates: selfneg.s_p_adjacency_list.get(1+selfneg.pos as u64),
                sp_offset: selfneg.s_p_adjacency_list.offset_for(1+selfneg.pos as u64),
                sp_o_adjacency_list: selfneg.sp_o_adjacency_list.clone()
            });

            selfneg.pos +=1;
        }

        if !(next_pos_subject == 0 || next_parent_subject <= next_pos_subject) {
            std::mem::swap(&mut next_parent, &mut self.next_parent_subject);
        }

        if next_parent_subject != 0 || next_pos_subject != 0 {
            Some(Box::new(ChildSubjectLookup {
                parent: next_parent,
                subject: subject,
                pos,
                neg
            }))
        }
        else {
            None
        }
    }
}

#[derive(Clone)]
struct AdjacencyStuff<M:'static+AsRef<[u8]>+Clone> {
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}


struct ChildSubjectLookup<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<dyn SubjectLookup>>,
    subject: u64,

    pos: Option<AdjacencyStuff<M>>,
    neg: Option<AdjacencyStuff<M>>,
}

impl<M:'static+AsRef<[u8]>+Clone> SubjectLookup for ChildSubjectLookup<M> {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicates(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>> {
        Box::new(ChildPredicateIterator {
            parent: self.parent.as_ref().map(|parent|parent.predicates()),
            subject: self.subject,
            pos_adjacencies: self.pos.clone(),
            neg_adjacencies: self.neg.clone(),
            next_parent_predicate: None,
            pos_pos: 0,
            neg_pos: 0
        })
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn SubjectPredicateLookup>> {
        if predicate == 0 {
            return None;
        }
        // TODO: Make non-recursive
        let parent_objects = self.parent.as_ref().and_then(|parent|parent.lookup_predicate(predicate));

        if self.pos.is_none() && parent_objects.is_none() {
            None
        }
        else {
            let pos_objects = self.pos.as_ref()
                .and_then(|pos| pos.predicates.iter().position(|p|p==predicate)
                          .map(|position_in_pos_predicates|
                               pos.sp_o_adjacency_list.get(pos.sp_offset+(position_in_pos_predicates as u64)+1)));
            let neg_objects = self.neg.as_ref()
                .and_then(|neg| neg.predicates.iter().position(|p|p==predicate)
                          .map(|position_in_neg_predicates|
                               neg.sp_o_adjacency_list.get(neg.sp_offset+(position_in_neg_predicates as u64)+1)));

            Some(Box::new(ChildSubjectPredicateLookup {
                parent: parent_objects,
                subject: self.subject,
                predicate,
                pos_objects,
                neg_objects
            }))
        }
    }
}

struct ChildPredicateIterator<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>>>,
    subject: u64,
    pos_adjacencies: Option<AdjacencyStuff<M>>,
    neg_adjacencies: Option<AdjacencyStuff<M>>,
    next_parent_predicate: Option<Box<dyn SubjectPredicateLookup>>,
    pos_pos: usize,
    neg_pos: usize
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildPredicateIterator<M> {
    type Item=Box<dyn SubjectPredicateLookup>;

    fn next(&mut self) -> Option<Box<dyn SubjectPredicateLookup>> {
        if self.parent.is_some() && self.next_parent_predicate.is_none() {
            match self.parent.as_mut().unwrap().next() {
                Some(predicate) => self.next_parent_predicate = Some(predicate),
                None => self.parent = None
            };
        }
        let pos_predicate = if self.pos_pos < self.pos_adjacencies.as_ref().map(|pa|pa.predicates.len()).unwrap_or(0) { Some(self.pos_adjacencies.as_ref().unwrap().predicates.entry(self.pos_pos)) } else { None };
        let neg_predicate = if self.neg_pos < self.neg_adjacencies.as_ref().map(|no|no.predicates.len()).unwrap_or(0) { Some(self.neg_adjacencies.as_ref().unwrap().predicates.entry(self.neg_pos)) } else { None };

        let mut next_parent_predicate = None;
        std::mem::swap(&mut next_parent_predicate, &mut self.next_parent_predicate);
        match (next_parent_predicate, pos_predicate, neg_predicate) {
            (Some(parent), Some(pos), neg) => {
                let neg_objects = if parent.predicate() == neg.unwrap_or(0) && parent.predicate() <= pos {
                    let neg_adjacencies = self.neg_adjacencies.as_ref().unwrap();
                    let result = neg_adjacencies.sp_o_adjacency_list.get(neg_adjacencies.sp_offset+(self.neg_pos as u64)+1);
                    self.neg_pos += 1;

                    Some(result)
                } else {
                    None
                };

                let pos_objects = if parent.predicate() >= pos {
                    let pos_adjacencies = self.pos_adjacencies.as_ref().unwrap();
                    let result = pos_adjacencies.sp_o_adjacency_list.get(pos_adjacencies.sp_offset+(self.pos_pos as u64)+1);
                    self.pos_pos += 1;

                    Some(result)
                } else {
                    None
                };

                let parent_predicate = parent.predicate();
                let predicate = if parent_predicate <= pos { parent_predicate } else { pos };

                let parent_option = if parent.predicate() <= pos {
                    Some(parent)
                } else {
                    std::mem::swap(&mut Some(parent), &mut self.next_parent_predicate);
                    None
                };


                Some(ChildSubjectPredicateLookup {
                    parent: parent_option,
                    subject: self.subject,
                    predicate,
                    pos_objects,
                    neg_objects
                })
            },
            (Some(parent), None, neg) => {
                let neg_objects = if parent.predicate() == neg.unwrap_or(0) {
                    let neg_adjacencies = self.neg_adjacencies.as_ref().unwrap();
                    let result = neg_adjacencies.sp_o_adjacency_list.get(neg_adjacencies.sp_offset+(self.neg_pos as u64)+1);
                    self.neg_pos += 1;
                    Some(result)
                } else {
                    None
                };

                let predicate = parent.predicate();

                Some(ChildSubjectPredicateLookup {
                    parent: Some(parent),
                    subject: self.subject,
                    predicate,
                    pos_objects: None,
                    neg_objects
                })
            },
            (None, Some(pos), _) => {
                let pos_adjacencies = self.pos_adjacencies.as_ref().unwrap();
                let pos_objects = Some(pos_adjacencies.sp_o_adjacency_list.get(pos_adjacencies.sp_offset+(self.pos_pos as u64)+1));
                self.pos_pos += 1;

                Some(ChildSubjectPredicateLookup {
                    parent: None,
                    subject: self.subject,
                    predicate: pos,
                    pos_objects,
                    neg_objects: None
                })
            },
            (None, None, _) => None
        }
        .map(|x|{
            let result: Box<dyn SubjectPredicateLookup> = Box::new(x);
            result
        })
    }
}

struct ChildSubjectPredicateLookup<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<dyn SubjectPredicateLookup>>,
    subject: u64,
    predicate: u64,
    pos_objects: Option<LogArraySlice<M>>,
    neg_objects: Option<LogArraySlice<M>>
}

impl<M:'static+AsRef<[u8]>+Clone> SubjectPredicateLookup for ChildSubjectPredicateLookup<M> {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn objects(&self) -> Box<dyn Iterator<Item=u64>> {
        Box::new(ChildObjectIterator {
            parent: self.parent.as_ref().map(|p|p.triples()),
            pos_objects: self.pos_objects.clone(),
            neg_objects: self.neg_objects.clone(),

            next_parent_object: None,
            pos_pos: 0,
            neg_pos: 0
        })
    }

    fn has_object(&self, object: u64) -> bool {
        if object == 0 {
            return false;
        }
        // this should check pos (if it is there), then neg (if it is there), and finally parent.
        // if it is in neg, return None. otherwise return the triple (if in pos) or whatever comes out of parent.
        self.pos_objects.as_ref()
            .and_then(|po|po.iter().position(|o|o == object).map(|_|true))
            .or_else(|| {
                if self.neg_objects.as_ref().and_then(|no|no.iter().position(|o|o == object)).is_some() {
                    Some(false)
                }
                else {
                    self.parent.as_ref().map(|p|p.has_object(object))
                }
            })
            .unwrap_or(false)
    }
}

struct ChildObjectIterator<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<dyn Iterator<Item=IdTriple>>>,
    next_parent_object: Option<u64>,
    pos_objects: Option<LogArraySlice<M>>,
    neg_objects: Option<LogArraySlice<M>>,
    pos_pos: usize,
    neg_pos: usize,
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildObjectIterator<M> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        // first iterate through all the pos objects
        // then, iterate through the parent, and filter out the next objects
        if self.parent.is_some() && self.next_parent_object.is_none() {
            match self.parent.as_mut().unwrap().next() {
                Some(triple) => self.next_parent_object = Some(triple.object),
                None => self.parent = None
            };
        }

        let next_pos_object: Option<u64> = if self.pos_pos < self.pos_objects.as_ref().map(|po|po.len()).unwrap_or(0) { Some(self.pos_objects.as_ref().unwrap().entry(self.pos_pos)) } else { None };
        let next_neg_object: Option<u64> = if self.neg_pos < self.neg_objects.as_ref().map(|no|no.len()).unwrap_or(0) { Some(self.neg_objects.as_ref().unwrap().entry(self.neg_pos)) } else { None };

        match (self.next_parent_object, next_pos_object, next_neg_object) {
            (Some(parent_object), Some(pos_object), neg_object) => {
                if parent_object < pos_object {
                    self.next_parent_object = None;
                    if parent_object == neg_object.unwrap_or(0) {
                        // skip this one, since it has been removed
                        self.neg_pos += 1;
                        self.next()
                    }
                    else {
                        Some(parent_object)
                    }
                }
                else {
                    self.pos_pos += 1;
                    Some(pos_object)
                }
            },
            (Some(parent_object), None, neg_object) => {
                self.next_parent_object = None;
                if parent_object == neg_object.unwrap_or(0) {
                    // skip this one, since it has been removed
                    self.neg_pos += 1;
                    self.next()
                }
                else {
                    Some(parent_object)
                }
            },
            (None, Some(own_object), _) => {
                self.pos_pos += 1;
                Some(own_object)
            },
            (None, None, _) => None
        }
    }
}

#[derive(Clone)]
struct ChildObjectLookupAdjacency<M:AsRef<[u8]>+Clone> {
    sp_slice: LogArraySlice<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    subjects: MonotonicLogArray<M>,
}

impl<M:'static+AsRef<[u8]>+Clone> ChildObjectLookupAdjacency<M> {
    fn iter(&self) -> impl Iterator<Item=(u64,u64)> {
        let sp_slice = self.sp_slice.clone();
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let subjects = self.subjects.clone();
        sp_slice.into_iter()
            .map(move |index| s_p_adjacency_list.pair_at_pos(index-1))
            .map(move |(mapped_subject, predicate)| (subjects.entry((mapped_subject as usize)-1), predicate))
    }
}

//#[derive(Clone)]
struct ChildObjectLookup<M:AsRef<[u8]>+Clone> {
    object: u64,
    parent: Option<Box<dyn ObjectLookup>>,

    pos: Option<ChildObjectLookupAdjacency<M>>,
    neg: Option<ChildObjectLookupAdjacency<M>>,
}

impl<M:AsRef<[u8]>+Clone> ChildObjectLookup<M> {
    fn new(object: u64,
           parent: Option<Box<dyn ObjectLookup>>,
           pos: Option<ChildObjectLookupAdjacency<M>>,
           neg: Option<ChildObjectLookupAdjacency<M>>) -> Self {
        Self {
            object,
            parent,
            pos,
            neg,
        }
    }
}

impl<M:AsRef<[u8]>+Clone> Clone for ChildObjectLookup<M> {
    fn clone(&self) -> Self {
        ChildObjectLookup {
            object: self.object,
            parent: self.parent.as_ref().map(|p|p.clone_box()),
            pos: self.pos.clone(),
            neg: self.neg.clone()
        }
    }
}

impl<M:'static+AsRef<[u8]>+Clone> ObjectLookup for ChildObjectLookup<M> {
    fn object(&self) -> u64 {
        self.object
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item=(u64, u64)>> {
        Box::new(ChildSubjectPredicatePairsIterator::new(self.parent.as_ref().map(|p|p.clone_box()), self.pos.clone(), self.neg.clone()))
    }

    fn clone_box(&self) -> Box<dyn ObjectLookup> {
        Box::new(self.clone())
    }
}

struct ChildSubjectPredicatePairsIterator {
    parent: Option<Peekable<Box<dyn Iterator<Item=(u64,u64)>>>>,

    pos: Option<Peekable<Box<dyn Iterator<Item=(u64,u64)>>>>,
    neg: Option<Peekable<Box<dyn Iterator<Item=(u64, u64)>>>>
}

impl ChildSubjectPredicatePairsIterator {
    fn new<M:'static+AsRef<[u8]>+Clone>(parent: Option<Box<dyn ObjectLookup>>,
                                        pos: Option<ChildObjectLookupAdjacency<M>>,
                                        neg: Option<ChildObjectLookupAdjacency<M>>) -> Self {
        Self {
            parent: parent.map(|p|p.subject_predicate_pairs().peekable()),
            pos: pos.map(|p| (Box::new(p.iter()) as Box<dyn Iterator<Item=(u64,u64)>>).peekable()),
            neg: neg.map(|n| (Box::new(n.iter()) as Box<dyn Iterator<Item=(u64,u64)>>).peekable()),
        }
    }
}

impl Iterator for ChildSubjectPredicatePairsIterator {
    type Item = (u64,u64);

    fn next(&mut self) -> Option<(u64,u64)> {
        let parent = self.parent.as_mut().and_then(|p|p.peek()).map(|p|*p);
        let pos = self.pos.as_mut().and_then(|p|p.peek()).map(|p|*p);
        let neg = self.neg.as_mut().and_then(|n|n.peek()).map(|n|*n);
        if parent.is_none() {
            self.parent = None;
        }
        if pos.is_none() {
            self.pos = None;
        }
        if neg.is_none() {
            self.neg = None;
        }

        if parent.is_some() {
            let parent = parent.unwrap();
            if pos.is_none() || parent < pos.unwrap() {
                // pick parent result (after checking neg)
                let read_parent = self.parent.as_mut().unwrap().next().unwrap();
                let mut read_neg: Option<(u64,u64)> = neg;
                while self.neg.as_mut().and_then(|n|n.peek().map(|n|*n <= parent)).unwrap_or(false) {
                    // next result on neg stream is less than or equal to parent, so we need to read it.
                    read_neg = self.neg.as_mut().unwrap().next();
                }

                if read_neg.is_some() && read_neg.unwrap() == read_parent {
                    // parent entry was found in neg, so skip to next entry
                    return self.next();
                }
                else {
                    return Some(read_parent);
                }
            }
            else if parent == pos.unwrap() {
                // this should not happen, as pos layer should not
                // contain anything that is also in the parent
                // layer. Panic as something must have gone terribly wrong.
                panic!("unexpectedly found equal value in parent and child layers");
            }
        }

        // no parent, or earlier pos
        if pos.is_some() {
            // just child
            Some(self.pos.as_mut().unwrap().next().unwrap())
        }
        else {
            None
        }
    }
}

struct ChildPredicateLookupParentData<M:AsRef<[u8]>+Clone> {
    parent: Box<dyn PredicateLookup>,
    neg_subjects: MonotonicLogArray<M>,
    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>,
}

struct ChildPredicateLookupChildData<M:AsRef<[u8]>+Clone> {
    lookup: WaveletLookup<M>,
    pos_subjects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,
}

struct ChildPredicateLookup<M:AsRef<[u8]>+Clone> {
    predicate: u64,
    parent: Option<ChildPredicateLookupParentData<M>>,
    child: Option<ChildPredicateLookupChildData<M>>
}

impl<M:'static+AsRef<[u8]>+Clone> PredicateLookup for ChildPredicateLookup<M> {
    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>> {
        Box::new(ChildPredicateLookupSubjectPredicatePairsIterator {
            predicate: self.predicate,
            parent: self.parent.as_ref().map(|p|ChildPredicateLookupSubjectPredicatePairsIteratorParentData {
                parent_pairs: p.parent.subject_predicate_pairs().peekable(),
                neg_subjects: p.neg_subjects.clone(),
                neg_s_p_adjacency_list: p.neg_s_p_adjacency_list.clone(),
                neg_sp_o_adjacency_list: p.neg_sp_o_adjacency_list.clone()
            }),
            child: self.child.as_ref().map(|child| ChildPredicateLookupSubjectPredicatePairsIteratorChildData {
                lookup_positions: (Box::new(child.lookup.iter()) as Box<dyn Iterator<Item=u64>>).peekable(),
                pos_subjects: child.pos_subjects.clone(),
                pos_s_p_adjacency_list: child.pos_s_p_adjacency_list.clone(),
                pos_sp_o_adjacency_list: child.pos_sp_o_adjacency_list.clone(),
            })
        })
    }
}

struct ChildPredicateLookupSubjectPredicatePairsIteratorParentData<M:AsRef<[u8]>+Clone> {
    parent_pairs: Peekable<Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>>>,

    neg_subjects: MonotonicLogArray<M>,
    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>,
}

struct ChildPredicateLookupSubjectPredicatePairsIteratorChildData<M:AsRef<[u8]>+Clone> {
    lookup_positions: Peekable<Box<dyn Iterator<Item=u64>>>,

    pos_subjects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,
}

impl<M:AsRef<[u8]>+Clone> ChildPredicateLookupSubjectPredicatePairsIteratorParentData<M> {
    fn peek_next_subject(&mut self) -> Option<u64> {
        self.parent_pairs.peek().map(|sp| sp.subject())
    }

    fn next(&mut self) -> (Option<Box<dyn SubjectPredicateLookup>>, Option<LogArraySlice<M>>) {
        let pair = self.parent_pairs.next().unwrap();
        let subject = pair.subject();

        let mapped_subject = self.neg_subjects.index_of(subject).map(|s|s + 1);
        let entry_in_sp_o = mapped_subject.and_then(|mapped_subject| {
            let offset = self.neg_s_p_adjacency_list.offset_for(mapped_subject as u64);
            let s_p_slice = self.neg_s_p_adjacency_list.get(mapped_subject as u64);
            s_p_slice.iter().position(|p|p == pair.predicate())
                .map(|pos_in_slice| offset + (pos_in_slice as u64) + 1)
        });
        let slice = entry_in_sp_o.map(|entry_in_sp_o|self.neg_sp_o_adjacency_list.get(entry_in_sp_o));

        (Some(pair), slice)
    }
}

impl<M:AsRef<[u8]>+Clone> ChildPredicateLookupSubjectPredicatePairsIteratorChildData<M> {
    fn peek_next_subject(&mut self) -> Option<u64> {
        self.lookup_positions.peek()
            .map(|&pos| pos)
            .map(|pos| {
                let mapped_subject = self.pos_s_p_adjacency_list.pair_at_pos(pos).0 as usize;
                self.pos_subjects.entry(mapped_subject-1)
            })
    }

    fn next(&mut self) -> Option<LogArraySlice<M>> {
        let pos = self.lookup_positions.next().unwrap();
        let slice = self.pos_sp_o_adjacency_list.get(pos+1);

        Some(slice)
    }
}


struct ChildPredicateLookupSubjectPredicatePairsIterator<M:AsRef<[u8]>+Clone> {
    predicate: u64,
    parent: Option<ChildPredicateLookupSubjectPredicatePairsIteratorParentData<M>>,
    child: Option<ChildPredicateLookupSubjectPredicatePairsIteratorChildData<M>>
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildPredicateLookupSubjectPredicatePairsIterator<M> {
    type Item = Box<dyn SubjectPredicateLookup>;

    fn next(&mut self) -> Option<Box<dyn SubjectPredicateLookup>> {
        let next_parent_subject = self.parent.as_mut().and_then(|p|p.peek_next_subject());
        let next_child_subject = self.child.as_mut().and_then(|c|c.peek_next_subject());

        let mut parent: Option<Box<dyn SubjectPredicateLookup>> = None;
        let subject: u64;
        let predicate = self.predicate;
        let mut pos_objects: Option<LogArraySlice<M>> = None;
        let mut neg_objects: Option<LogArraySlice<M>> = None;

        match (next_parent_subject, next_child_subject) {
            (Some(next_parent_subject), Some(next_child_subject)) => {
                if next_parent_subject == next_child_subject {
                    let result = self.parent.as_mut().unwrap().next();
                    parent = result.0;
                    neg_objects = result.1;
                    subject = next_parent_subject;
                    pos_objects = self.child.as_mut().unwrap().next();
                }
                else if next_parent_subject < next_child_subject {
                    let result = self.parent.as_mut().unwrap().next();
                    parent = result.0;
                    neg_objects = result.1;
                    subject = next_parent_subject;
                }
                else {
                    pos_objects = self.child.as_mut().unwrap().next();
                    subject = next_child_subject;
                }
            },
            (Some(next_parent_subject), None) => {
                let result = self.parent.as_mut().unwrap().next();
                parent = result.0;
                neg_objects = result.1;
                subject = next_parent_subject;
            },
            (None, Some(next_child_subject)) => {
                pos_objects = self.child.as_mut().unwrap().next();
                subject = next_child_subject;
            },
            (None, None) => return None
        }

        Some(Box::new(ChildSubjectPredicateLookup {
            parent,
            subject,
            predicate,
            pos_objects,
            neg_objects
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

#[cfg(test)]
mod tests {
    use crate::layer::base::*;
    use super::*;
    use crate::storage::memory::*;
    fn base_layer_files() -> BaseLayerFiles<MemoryBackedStore> {
        let files: Vec<_> = (0..21).map(|_| MemoryBackedStore::new()).collect();
        BaseLayerFiles {
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
            s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[6].clone(),
                    blocks_file: files[7].clone(),
                    sblocks_file: files[8].clone(),
                },
                nums_file: files[9].clone()
            },
            sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[10].clone(),
                    blocks_file: files[11].clone(),
                    sblocks_file: files[12].clone(),
                },
                nums_file: files[13].clone()
            },
            o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[14].clone(),
                    blocks_file: files[15].clone(),
                    sblocks_file: files[16].clone(),
                },
                nums_file: files[17].clone()
            },
            predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[18].clone(),
                blocks_file: files[19].clone(),
                sblocks_file: files[20].clone(),
            },
        }
    }

    fn empty_base_layer() -> BaseLayer<SharedVec> {
        let files = base_layer_files();
        let base_builder = BaseLayerFileBuilder::from_files(&files);
        base_builder.into_phase2().and_then(|b|b.finalize()).wait().unwrap();

        BaseLayer::load_from_files([1,2,3,4,5], &files).wait().unwrap()
    }

    fn example_base_layer() -> BaseLayer<SharedVec> {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];


        let files = base_layer_files();
        let base_builder = BaseLayerFileBuilder::from_files(&files);

        let future = base_builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
            .and_then(move |(_,b)| b.add_predicates(predicates.into_iter().map(|s|s.to_string())))
            .and_then(move |(_,b)| b.add_values(values.into_iter().map(|s|s.to_string())))
            .and_then(|(_,b)| b.into_phase2())

            .and_then(|b| b.add_triple(1,1,1))
            .and_then(|b| b.add_triple(2,1,1))
            .and_then(|b| b.add_triple(2,1,3))
            .and_then(|b| b.add_triple(2,3,6))
            .and_then(|b| b.add_triple(3,2,5))
            .and_then(|b| b.add_triple(3,3,6))
            .and_then(|b| b.add_triple(4,3,6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        let base_layer = BaseLayer::load_from_files([1,2,3,4,5], &files).wait().unwrap();

        base_layer
    }

    fn example_child_files() -> ChildLayerFiles<MemoryBackedStore> {
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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();

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

        let child_files = example_child_files();
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
}
