use super::layer::*;
use crate::structure::*;
use crate::storage::file::*;
use futures::prelude::*;
use futures::future;
use futures::stream;

use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub struct ChildLayerFiles<F:FileLoad+FileStore+Clone> {
    pub node_dictionary_blocks_file: F,
    pub node_dictionary_offsets_file: F,

    pub predicate_dictionary_blocks_file: F,
    pub predicate_dictionary_offsets_file: F,

    pub value_dictionary_blocks_file: F,
    pub value_dictionary_offsets_file: F,

    pub pos_subjects_file: F,
    pub neg_subjects_file: F,

    pub pos_s_p_adjacency_list_bits_file: F,
    pub pos_s_p_adjacency_list_blocks_file: F,
    pub pos_s_p_adjacency_list_sblocks_file: F,
    pub pos_s_p_adjacency_list_nums_file: F,

    pub pos_sp_o_adjacency_list_bits_file: F,
    pub pos_sp_o_adjacency_list_blocks_file: F,
    pub pos_sp_o_adjacency_list_sblocks_file: F,
    pub pos_sp_o_adjacency_list_nums_file: F,

    pub neg_s_p_adjacency_list_bits_file: F,
    pub neg_s_p_adjacency_list_blocks_file: F,
    pub neg_s_p_adjacency_list_sblocks_file: F,
    pub neg_s_p_adjacency_list_nums_file: F,

    pub neg_sp_o_adjacency_list_bits_file: F,
    pub neg_sp_o_adjacency_list_blocks_file: F,
    pub neg_sp_o_adjacency_list_sblocks_file: F,
    pub neg_sp_o_adjacency_list_nums_file: F,
}

#[derive(Clone)]
pub struct ChildLayer<M:'static+AsRef<[u8]>+Clone> {
    parent: Arc<GenericLayer<M>>,

    node_dictionary: PfcDict<M>,
    predicate_dictionary: PfcDict<M>,
    value_dictionary: PfcDict<M>,

    pos_subjects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,

    neg_subjects: MonotonicLogArray<M>,
    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> ChildLayer<M> {
    pub fn load_from_files<F:FileLoad<Map=M>+FileStore+Clone>(parent: GenericLayer<M>, files: &ChildLayerFiles<F>) -> Self {
        Self::load(parent,
                   files.node_dictionary_blocks_file.map(),
                   files.node_dictionary_offsets_file.map(),

                   files.predicate_dictionary_blocks_file.map(),
                   files.predicate_dictionary_offsets_file.map(),

                   files.value_dictionary_blocks_file.map(),
                   files.value_dictionary_offsets_file.map(),

                   files.pos_subjects_file.map(),
                   files.neg_subjects_file.map(),

                   files.pos_s_p_adjacency_list_bits_file.map(),
                   files.pos_s_p_adjacency_list_blocks_file.map(),
                   files.pos_s_p_adjacency_list_sblocks_file.map(),
                   files.pos_s_p_adjacency_list_nums_file.map(),

                   files.pos_sp_o_adjacency_list_bits_file.map(),
                   files.pos_sp_o_adjacency_list_blocks_file.map(),
                   files.pos_sp_o_adjacency_list_sblocks_file.map(),
                   files.pos_sp_o_adjacency_list_nums_file.map(),

                   files.neg_s_p_adjacency_list_bits_file.map(),
                   files.neg_s_p_adjacency_list_blocks_file.map(),
                   files.neg_s_p_adjacency_list_sblocks_file.map(),
                   files.neg_s_p_adjacency_list_nums_file.map(),

                   files.neg_sp_o_adjacency_list_bits_file.map(),
                   files.neg_sp_o_adjacency_list_blocks_file.map(),
                   files.neg_sp_o_adjacency_list_sblocks_file.map(),
                   files.neg_sp_o_adjacency_list_nums_file.map())
    }

    pub fn load(parent: GenericLayer<M>,
                node_dictionary_blocks_file: M,
                node_dictionary_offsets_file: M,

                predicate_dictionary_blocks_file: M,
                predicate_dictionary_offsets_file: M,

                value_dictionary_blocks_file: M,
                value_dictionary_offsets_file: M,

                pos_subjects_file: M,
                neg_subjects_file: M,

                pos_s_p_adjacency_list_bits_file: M,
                pos_s_p_adjacency_list_blocks_file: M,
                pos_s_p_adjacency_list_sblocks_file: M,
                pos_s_p_adjacency_list_nums_file: M,

                pos_sp_o_adjacency_list_bits_file: M,
                pos_sp_o_adjacency_list_blocks_file: M,
                pos_sp_o_adjacency_list_sblocks_file: M,
                pos_sp_o_adjacency_list_nums_file: M,

                neg_s_p_adjacency_list_bits_file: M,
                neg_s_p_adjacency_list_blocks_file: M,
                neg_s_p_adjacency_list_sblocks_file: M,
                neg_s_p_adjacency_list_nums_file: M,

                neg_sp_o_adjacency_list_bits_file: M,
                neg_sp_o_adjacency_list_blocks_file: M,
                neg_sp_o_adjacency_list_sblocks_file: M,
                neg_sp_o_adjacency_list_nums_file: M,
    ) -> ChildLayer<M> {
        let node_dictionary = PfcDict::parse(node_dictionary_blocks_file, node_dictionary_offsets_file).unwrap();
        let predicate_dictionary = PfcDict::parse(predicate_dictionary_blocks_file, predicate_dictionary_offsets_file).unwrap();
        let value_dictionary = PfcDict::parse(value_dictionary_blocks_file, value_dictionary_offsets_file).unwrap();

        let pos_subjects = MonotonicLogArray::from_logarray(LogArray::parse(pos_subjects_file).unwrap());
        let neg_subjects = MonotonicLogArray::from_logarray(LogArray::parse(neg_subjects_file).unwrap());

        let pos_s_p_adjacency_list = AdjacencyList::parse(pos_s_p_adjacency_list_nums_file, pos_s_p_adjacency_list_bits_file, pos_s_p_adjacency_list_blocks_file, pos_s_p_adjacency_list_sblocks_file);
        let pos_sp_o_adjacency_list = AdjacencyList::parse(pos_sp_o_adjacency_list_nums_file, pos_sp_o_adjacency_list_bits_file, pos_sp_o_adjacency_list_blocks_file, pos_sp_o_adjacency_list_sblocks_file);

        let neg_s_p_adjacency_list = AdjacencyList::parse(neg_s_p_adjacency_list_nums_file, neg_s_p_adjacency_list_bits_file, neg_s_p_adjacency_list_blocks_file, neg_s_p_adjacency_list_sblocks_file);
        let neg_sp_o_adjacency_list = AdjacencyList::parse(neg_sp_o_adjacency_list_nums_file, neg_sp_o_adjacency_list_bits_file, neg_sp_o_adjacency_list_blocks_file, neg_sp_o_adjacency_list_sblocks_file);

        ChildLayer {
            parent: Arc::new(parent),
            
            node_dictionary: node_dictionary,
            predicate_dictionary: predicate_dictionary,
            value_dictionary: value_dictionary,

            pos_subjects,
            neg_subjects,

            pos_s_p_adjacency_list,
            pos_sp_o_adjacency_list,

            neg_s_p_adjacency_list,
            neg_sp_o_adjacency_list,
        }
    }
}

impl<M:'static+AsRef<[u8]>+Clone> Layer for ChildLayer<M> {
    type PredicateObjectPairsForSubject = ChildPredicateObjectPairsForSubject<M>;
    type SubjectIterator = ChildSubjectIterator<M>;

    fn node_and_value_count(&self) -> usize {
        self.node_dictionary.len() + self.value_dictionary.len() + self.parent.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.predicate_dictionary.len() + self.parent.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self.node_dictionary.id(subject) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.subject_id(subject)
        }
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self.predicate_dictionary.id(predicate) {
            Some(id) => Some(self.parent.predicate_count() as u64 + id + 1),
            None => self.parent.predicate_id(predicate)
        }
    }

    fn object_node_id(&self, node: &str) -> Option<u64> {
        match self.node_dictionary.id(node) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.object_node_id(node)
        }
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        match self.value_dictionary.id(value) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + self.node_dictionary.len() as u64 + id + 1),
            None => self.parent.object_value_id(value)
        }
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let parent_count = self.parent.node_and_value_count() as u64;

        if corrected_id >= parent_count as u64 {
            // subject, if it exists, is in this layer
            corrected_id -= parent_count;
            if corrected_id >= self.node_dictionary.len() as u64 {
                None
            }
            else {
                Some(self.node_dictionary.get(corrected_id as usize))
            }
        }
        else {
            // subject, if it exists, is in a parent layer
            self.parent.id_subject(id)
        }
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let parent_count = self.parent.predicate_count() as u64;

        if corrected_id >= parent_count {
            // predicate, if it exists, is in this layer
            corrected_id -= parent_count;
            if corrected_id >= self.predicate_dictionary.len() as u64 {
                None
            }
            else {
                Some(self.predicate_dictionary.get(corrected_id as usize))
            }
        }
        else {
            self.parent.id_predicate(id)
        }
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id - 1;
        let parent_count = self.parent.node_and_value_count() as u64;

        if corrected_id >= parent_count {
            // object, if it exists, is in this layer
            corrected_id -= parent_count;
            if corrected_id >= self.node_dictionary.len() as u64 {
                // object, if it exists, must be a value
                corrected_id -= self.node_dictionary.len() as u64;
                if corrected_id >= self.value_dictionary.len() as u64 {
                    None
                }
                else {
                    Some(ObjectType::Value(self.value_dictionary.get(corrected_id as usize)))
                }
            }
            else {
                Some(ObjectType::Node(self.node_dictionary.get(corrected_id as usize)))
            }
        }
        else {
            self.parent.id_object(id)
        }
    }

    fn subjects(&self) -> ChildSubjectIterator<M> {
        ChildSubjectIterator {
            parent: Some(Box::new(self.parent.subjects())),

            pos_subjects: self.pos_subjects.clone(),
            pos_s_p_adjacency_list: self.pos_s_p_adjacency_list.clone(),
            pos_sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone(),

            neg_subjects: self.neg_subjects.clone(),
            neg_s_p_adjacency_list: self.neg_s_p_adjacency_list.clone(),
            neg_sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone(),

            next_parent_subject: None,
            pos_pos: 0,
            neg_pos: 0
        }
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<ChildPredicateObjectPairsForSubject<M>> {
        if subject == 0 {
            return None;
        }

        let mut pos: Option<AdjacencyStuff<M>> = None;
        let mut neg: Option<AdjacencyStuff<M>> = None;
        
        // first determine where we should be looking.
        let pos_index = self.pos_subjects.index_of(subject);
        let neg_index = self.neg_subjects.index_of(subject);
        let parent = self.parent.predicate_object_pairs_for_subject(subject).map(|p|Box::new(p));
        if pos_index.is_none() && parent.is_none() {
            return None;
        }

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

        Some(ChildPredicateObjectPairsForSubject {
            parent,
            subject,
            pos,
            neg
        })
    }
}

#[derive(Clone)]
pub struct ChildSubjectIterator<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<GenericSubjectIterator<M>>>,
    pos_subjects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,

    neg_subjects: MonotonicLogArray<M>,
    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>,

    next_parent_subject: Option<GenericPredicateObjectPairsForSubject<M>>,
    pos_pos: usize,
    neg_pos: usize
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildSubjectIterator<M> {
    type Item = ChildPredicateObjectPairsForSubject<M>;

    fn next(&mut self) -> Option<ChildPredicateObjectPairsForSubject<M>> {
        if self.parent.is_some() && self.next_parent_subject.is_none() {
            self.next_parent_subject = self.parent.as_mut().unwrap().next();
            if self.next_parent_subject.is_none() {
                self.parent = None;
            }
        }

        let next_parent = self.next_parent_subject.clone().map(|parent|Box::new(parent));
        let next_parent_subject = next_parent.as_ref().map(|p|p.subject()).unwrap_or(0);

        let next_pos_subject = if self.pos_pos < self.pos_subjects.len() { self.pos_subjects.entry(self.pos_pos) } else { 0 };
        let next_neg_subject = if self.neg_pos < self.neg_subjects.len() { self.neg_subjects.entry(self.neg_pos) } else { 0 };

        let mut pos: Option<AdjacencyStuff<M>> = None;
        let mut neg: Option<AdjacencyStuff<M>> = None;

        let mut subject = next_parent_subject;

        if next_pos_subject != 0 && (next_parent_subject == 0 || next_pos_subject <= next_parent_subject) {
            subject = next_pos_subject;
            pos = Some(AdjacencyStuff {
                predicates: self.pos_s_p_adjacency_list.get(1+self.pos_pos as u64),
                sp_offset: self.pos_s_p_adjacency_list.offset_for(1+self.pos_pos as u64),
                sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
            });

            self.pos_pos += 1;
        }

        if next_neg_subject != 0 && (next_pos_subject == 0 || next_parent_subject <= next_pos_subject) && next_parent_subject == next_neg_subject {
            neg = Some(AdjacencyStuff {
                predicates: self.neg_s_p_adjacency_list.get(1+self.neg_pos as u64),
                sp_offset: self.neg_s_p_adjacency_list.offset_for(1+self.neg_pos as u64),
                sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
            });

            self.neg_pos +=1;
        }

        if next_pos_subject == 0 || next_parent_subject <= next_pos_subject {
            self.next_parent_subject = None;
        }

        if next_parent_subject != 0 || next_pos_subject != 0 {
            Some(ChildPredicateObjectPairsForSubject {
                parent: next_parent,
                subject: subject,
                pos,
                neg
            })
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


#[derive(Clone)]
pub struct ChildPredicateObjectPairsForSubject<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<GenericPredicateObjectPairsForSubject<M>>>,
    subject: u64,

    pos: Option<AdjacencyStuff<M>>,
    neg: Option<AdjacencyStuff<M>>,
}

impl<M:'static+AsRef<[u8]>+Clone> PredicateObjectPairsForSubject for ChildPredicateObjectPairsForSubject<M> {
    type Objects = ChildObjectsForSubjectPredicatePair<M>;
    type PredicateIterator = ChildPredicateIterator<M>;

    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicates(&self) -> ChildPredicateIterator<M> {
        ChildPredicateIterator {
            parent: self.parent.as_ref().map(|parent|Box::new(parent.predicates())),
            subject: self.subject,
            pos_adjacencies: self.pos.clone(),
            neg_adjacencies: self.neg.clone(),
            next_parent_predicate: None,
            pos_pos: 0,
            neg_pos: 0
        }
    }

    fn objects_for_predicate(&self, predicate: u64) -> Option<ChildObjectsForSubjectPredicatePair<M>> {
        if predicate == 0 {
            return None;
        }

        let parent_objects = self.parent.as_ref().and_then(|parent|parent.objects_for_predicate(predicate).map(|ofp|Box::new(ofp)));

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

            Some(ChildObjectsForSubjectPredicatePair {
                parent: parent_objects,
                subject: self.subject,
                predicate,
                pos_objects,
                neg_objects
            })
        }
    }
}

#[derive(Clone)]
pub struct ChildPredicateIterator<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<GenericPredicateIterator<M>>>,
    subject: u64,
    pos_adjacencies: Option<AdjacencyStuff<M>>,
    neg_adjacencies: Option<AdjacencyStuff<M>>,
    next_parent_predicate: Option<GenericObjectsForSubjectPredicatePair<M>>,
    pos_pos: usize,
    neg_pos: usize
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildPredicateIterator<M> {
    type Item=ChildObjectsForSubjectPredicatePair<M>;

    fn next(&mut self) -> Option<ChildObjectsForSubjectPredicatePair<M>> {
        if self.parent.is_some() && self.next_parent_predicate.is_none() {
            match self.parent.as_mut().unwrap().next() {
                Some(predicate) => self.next_parent_predicate = Some(predicate),
                None => self.parent = None
            };
        }
        let pos_predicate = if self.pos_pos < self.pos_adjacencies.as_ref().map(|pa|pa.predicates.len()).unwrap_or(0) { Some(self.pos_adjacencies.as_ref().unwrap().predicates.entry(self.pos_pos)) } else { None };
        let neg_predicate = if self.neg_pos < self.neg_adjacencies.as_ref().map(|no|no.predicates.len()).unwrap_or(0) { Some(self.neg_adjacencies.as_ref().unwrap().predicates.entry(self.neg_pos)) } else { None };

        match (self.next_parent_predicate.clone(), pos_predicate, neg_predicate) {
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
                    self.next_parent_predicate = None;
                    Some(Box::new(parent))
                } else {
                    None
                };

                Some(ChildObjectsForSubjectPredicatePair {
                    parent: parent_option,
                    subject: self.subject,
                    predicate,
                    pos_objects,
                    neg_objects
                })
            },
            (Some(parent), None, neg) => {
                self.next_parent_predicate = None;
                let neg_objects = if parent.predicate() == neg.unwrap_or(0) {
                    let neg_adjacencies = self.neg_adjacencies.as_ref().unwrap();
                    let result = neg_adjacencies.sp_o_adjacency_list.get(neg_adjacencies.sp_offset+(self.neg_pos as u64)+1);
                    self.neg_pos += 1;
                    Some(result)
                } else {
                    None
                };

                let predicate = parent.predicate();
                
                Some(ChildObjectsForSubjectPredicatePair {
                    parent: Some(Box::new(parent)),
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

                Some(ChildObjectsForSubjectPredicatePair {
                    parent: None,
                    subject: self.subject,
                    predicate: pos,
                    pos_objects,
                    neg_objects: None
                })
            },
            (None, None, _) => None
        }
    }
}

#[derive(Clone)]
pub struct ChildObjectsForSubjectPredicatePair<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<GenericObjectsForSubjectPredicatePair<M>>>,
    subject: u64,
    predicate: u64,
    pos_objects: Option<LogArraySlice<M>>,
    neg_objects: Option<LogArraySlice<M>>
}

impl<M:'static+AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair for ChildObjectsForSubjectPredicatePair<M> {
    type ObjectIterator = ChildObjectIterator<M>;

    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }
    
    fn triples(&self) -> ChildObjectIterator<M> {
        ChildObjectIterator {
            parent: self.parent.as_ref().map(|p|Box::new(p.triples())),
            subject: self.subject,
            predicate: self.predicate,
            pos_objects: self.pos_objects.clone(),
            neg_objects: self.neg_objects.clone(),

            next_parent_object: None,
            pos_pos: 0,
            neg_pos: 0
        }
    }

    fn triple(&self, object: u64) -> Option<IdTriple> {
        if object == 0 {
            return None;
        }
        // this should check pos (if it is there), then neg (if it is there), and finally parent.
        // if it is in neg, return None. otherwise return the triple (if in pos) or whatever comes out of parent.
        self.pos_objects.as_ref()
            .and_then(|po|po.iter().position(|o|o == object))
            .map(|_| IdTriple { subject: self.subject, predicate: self.predicate, object: object })
            .or_else(|| {
                if self.neg_objects.as_ref().and_then(|no|no.iter().position(|o|o == object)).is_some() {
                    None
                }
                else {
                    self.parent.as_ref().and_then(|p|p.triple(object))
                }
            })
    }
}

#[derive(Clone)]
pub struct ChildObjectIterator<M:'static+AsRef<[u8]>+Clone> {
    parent: Option<Box<GenericObjectIterator<M>>>,
    next_parent_object: Option<u64>,
    subject: u64,
    predicate: u64,
    pos_objects: Option<LogArraySlice<M>>,
    neg_objects: Option<LogArraySlice<M>>,
    pos_pos: usize,
    neg_pos: usize,
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for ChildObjectIterator<M> {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
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
                        self.next().map(|triple| triple.object)
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
                    self.next().map(|triple| triple.object)
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
        }.map(|object| IdTriple {
            subject: self.subject,
            predicate: self.predicate,
            object
        })
    }
}

pub struct ChildLayerFileBuilder<F:'static+FileLoad+FileStore> {
    parent: GenericLayer<F::Map>,
    node_dictionary_files: DictionaryFiles<F>,
    predicate_dictionary_files: DictionaryFiles<F>,
    value_dictionary_files: DictionaryFiles<F>,

    pos_subjects_file: F,
    neg_subjects_file: F,

    pos_s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pos_sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    neg_s_p_adjacency_list_files: AdjacencyListFiles<F>,
    neg_sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F:'static+FileLoad+FileStore+Clone> ChildLayerFileBuilder<F> {
    pub fn from_files(parent: GenericLayer<F::Map>, files: &ChildLayerFiles<F>) -> Self {
        Self::new(parent,
                  files.node_dictionary_blocks_file.clone(),
                  files.node_dictionary_offsets_file.clone(),

                  files.predicate_dictionary_blocks_file.clone(),
                  files.predicate_dictionary_offsets_file.clone(),

                  files.value_dictionary_blocks_file.clone(),
                  files.value_dictionary_offsets_file.clone(),

                  files.pos_subjects_file.clone(),
                  files.neg_subjects_file.clone(),

                  files.pos_s_p_adjacency_list_bits_file.clone(),
                  files.pos_s_p_adjacency_list_blocks_file.clone(),
                  files.pos_s_p_adjacency_list_sblocks_file.clone(),
                  files.pos_s_p_adjacency_list_nums_file.clone(),

                  files.pos_sp_o_adjacency_list_bits_file.clone(),
                  files.pos_sp_o_adjacency_list_blocks_file.clone(),
                  files.pos_sp_o_adjacency_list_sblocks_file.clone(),
                  files.pos_sp_o_adjacency_list_nums_file.clone(),

                  files.neg_s_p_adjacency_list_bits_file.clone(),
                  files.neg_s_p_adjacency_list_blocks_file.clone(),
                  files.neg_s_p_adjacency_list_sblocks_file.clone(),
                  files.neg_s_p_adjacency_list_nums_file.clone(),

                  files.neg_sp_o_adjacency_list_bits_file.clone(),
                  files.neg_sp_o_adjacency_list_blocks_file.clone(),
                  files.neg_sp_o_adjacency_list_sblocks_file.clone(),
                  files.neg_sp_o_adjacency_list_nums_file.clone())
    }

    pub fn new(parent: GenericLayer<F::Map>,
               node_dictionary_blocks_file: F,
               node_dictionary_offsets_file: F,

               predicate_dictionary_blocks_file: F,
               predicate_dictionary_offsets_file: F,

               value_dictionary_blocks_file: F,
               value_dictionary_offsets_file: F,

               pos_subjects_file: F,
               neg_subjects_file: F,

               pos_s_p_adjacency_list_bits_file: F,
               pos_s_p_adjacency_list_blocks_file: F,
               pos_s_p_adjacency_list_sblocks_file: F,
               pos_s_p_adjacency_list_nums_file: F,

               pos_sp_o_adjacency_list_bits_file: F,
               pos_sp_o_adjacency_list_blocks_file: F,
               pos_sp_o_adjacency_list_sblocks_file: F,
               pos_sp_o_adjacency_list_nums_file: F,

               neg_s_p_adjacency_list_bits_file: F,
               neg_s_p_adjacency_list_blocks_file: F,
               neg_s_p_adjacency_list_sblocks_file: F,
               neg_s_p_adjacency_list_nums_file: F,

               neg_sp_o_adjacency_list_bits_file: F,
               neg_sp_o_adjacency_list_blocks_file: F,
               neg_sp_o_adjacency_list_sblocks_file: F,
               neg_sp_o_adjacency_list_nums_file: F,
    ) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(node_dictionary_blocks_file.open_write(), node_dictionary_offsets_file.open_write());
        let predicate_dictionary_builder = PfcDictFileBuilder::new(predicate_dictionary_blocks_file.open_write(), predicate_dictionary_offsets_file.open_write());
        let value_dictionary_builder = PfcDictFileBuilder::new(value_dictionary_blocks_file.open_write(), value_dictionary_offsets_file.open_write());

        let node_dictionary_files = DictionaryFiles { 
            blocks_file: node_dictionary_blocks_file,
            offsets_file: node_dictionary_offsets_file,
        };

        let predicate_dictionary_files = DictionaryFiles { 
            blocks_file: predicate_dictionary_blocks_file,
            offsets_file: predicate_dictionary_offsets_file,
        };

        let value_dictionary_files = DictionaryFiles { 
            blocks_file: value_dictionary_blocks_file,
            offsets_file: value_dictionary_offsets_file,
        };

        let pos_s_p_adjacency_list_files = AdjacencyListFiles {
            bits_file: pos_s_p_adjacency_list_bits_file,
            blocks_file: pos_s_p_adjacency_list_blocks_file,
            sblocks_file: pos_s_p_adjacency_list_sblocks_file,
            nums_file: pos_s_p_adjacency_list_nums_file,
        };

        let pos_sp_o_adjacency_list_files = AdjacencyListFiles {
            bits_file: pos_sp_o_adjacency_list_bits_file,
            blocks_file: pos_sp_o_adjacency_list_blocks_file,
            sblocks_file: pos_sp_o_adjacency_list_sblocks_file,
            nums_file: pos_sp_o_adjacency_list_nums_file,
        };

        let neg_s_p_adjacency_list_files = AdjacencyListFiles {
            bits_file: neg_s_p_adjacency_list_bits_file,
            blocks_file: neg_s_p_adjacency_list_blocks_file,
            sblocks_file: neg_s_p_adjacency_list_sblocks_file,
            nums_file: neg_s_p_adjacency_list_nums_file,
        };

        let neg_sp_o_adjacency_list_files = AdjacencyListFiles {
            bits_file: neg_sp_o_adjacency_list_bits_file,
            blocks_file: neg_sp_o_adjacency_list_blocks_file,
            sblocks_file: neg_sp_o_adjacency_list_sblocks_file,
            nums_file: neg_sp_o_adjacency_list_nums_file,
        };

        ChildLayerFileBuilder {
            parent,
            
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            pos_subjects_file,
            neg_subjects_file,

            pos_s_p_adjacency_list_files,
            pos_sp_o_adjacency_list_files,
            neg_s_p_adjacency_list_files,
            neg_sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        }
    }

    pub fn add_node(self, node: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>> {
        match self.parent.subject_id(node) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    
                    node_dictionary_files,
                    predicate_dictionary_files,
                    value_dictionary_files,

                    pos_subjects_file,
                    neg_subjects_file,

                    pos_s_p_adjacency_list_files,
                    pos_sp_o_adjacency_list_files,
                    neg_s_p_adjacency_list_files,
                    neg_sp_o_adjacency_list_files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;
                Box::new(node_dictionary_builder.add(node)
                         .map(move|(result, node_dictionary_builder)| (result, ChildLayerFileBuilder {
                             parent,

                             node_dictionary_files,
                             predicate_dictionary_files,
                             value_dictionary_files,

                             pos_subjects_file,
                             neg_subjects_file,

                             pos_s_p_adjacency_list_files,
                             pos_sp_o_adjacency_list_files,
                             neg_s_p_adjacency_list_files,
                             neg_sp_o_adjacency_list_files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }
    
    pub fn add_predicate(self, predicate: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>> {
        match self.parent.predicate_id(predicate) {
            None => {
                let ChildLayerFileBuilder {
                    parent,

                    node_dictionary_files,
                    predicate_dictionary_files,
                    value_dictionary_files,

                    pos_subjects_file,
                    neg_subjects_file,

                    pos_s_p_adjacency_list_files,
                    pos_sp_o_adjacency_list_files,
                    neg_s_p_adjacency_list_files,
                    neg_sp_o_adjacency_list_files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;

                Box::new(predicate_dictionary_builder.add(predicate)
                         .map(move|(result, predicate_dictionary_builder)| (result, ChildLayerFileBuilder {
                             parent,

                             node_dictionary_files,
                             predicate_dictionary_files,
                             value_dictionary_files,

                             pos_subjects_file,
                             neg_subjects_file,

                             pos_s_p_adjacency_list_files,
                             pos_sp_o_adjacency_list_files,
                             neg_s_p_adjacency_list_files,
                             neg_sp_o_adjacency_list_files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    pub fn add_value(self, value: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>> {
        match self.parent.object_value_id(value) {
            None => {
                let ChildLayerFileBuilder {
                    parent,

                    node_dictionary_files,
                    predicate_dictionary_files,
                    value_dictionary_files,

                    pos_subjects_file,
                    neg_subjects_file,

                    pos_s_p_adjacency_list_files,
                    pos_sp_o_adjacency_list_files,
                    neg_s_p_adjacency_list_files,
                    neg_sp_o_adjacency_list_files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;
                Box::new(value_dictionary_builder.add(value)
                         .map(move|(result, value_dictionary_builder)| (result, ChildLayerFileBuilder {
                             parent,

                             node_dictionary_files,
                             predicate_dictionary_files,
                             value_dictionary_files,

                             pos_subjects_file,
                             neg_subjects_file,

                             pos_s_p_adjacency_list_files,
                             pos_sp_o_adjacency_list_files,
                             neg_s_p_adjacency_list_files,
                             neg_sp_o_adjacency_list_files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    pub fn add_nodes<I:'static+IntoIterator<Item=String>>(self, nodes: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(nodes.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), node|
                  builder.add_node(&node)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    pub fn add_predicates<I:'static+IntoIterator<Item=String>>(self, predicates: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(predicates.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), predicate|
                  builder.add_predicate(&predicate)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    pub fn add_values<I:'static+IntoIterator<Item=String>>(self, values: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(values.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), value|
                  builder.add_value(&value)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    pub fn into_phase2(self) -> impl Future<Item=ChildLayerFileBuilderPhase2<F>,Error=std::io::Error> {
        let ChildLayerFileBuilder {
            parent,

            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            pos_subjects_file,
            neg_subjects_file,

            pos_s_p_adjacency_list_files,
            pos_sp_o_adjacency_list_files,
            neg_s_p_adjacency_list_files,
            neg_sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        let finalize_nodedict: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(node_dictionary_builder.finalize());
        let finalize_preddict: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(predicate_dictionary_builder.finalize());
        let finalize_valdict: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(value_dictionary_builder.finalize());

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .and_then(move |_| {
                let node_dict_r = PfcDict::parse(node_dictionary_files.blocks_file.map(),
                                                 node_dictionary_files.offsets_file.map());
                if node_dict_r.is_err() {
                    return future::err(node_dict_r.err().unwrap().into());
                }
                let node_dict = node_dict_r.unwrap();

                let pred_dict_r = PfcDict::parse(predicate_dictionary_files.blocks_file.map(),
                                                 predicate_dictionary_files.offsets_file.map());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(value_dictionary_files.blocks_file.map(),
                                                value_dictionary_files.offsets_file.map());
                if val_dict_r.is_err() {
                    return future::err(val_dict_r.err().unwrap().into());
                }
                let val_dict = val_dict_r.unwrap();

                let num_nodes = node_dict.len();
                let num_predicates = pred_dict.len();
                let num_values = val_dict.len();

                future::ok(ChildLayerFileBuilderPhase2::new(parent,
                                                            pos_subjects_file,
                                                            neg_subjects_file,
                                                            
                                                            pos_s_p_adjacency_list_files.bits_file,
                                                            pos_s_p_adjacency_list_files.blocks_file,
                                                            pos_s_p_adjacency_list_files.sblocks_file,
                                                            pos_s_p_adjacency_list_files.nums_file,

                                                            pos_sp_o_adjacency_list_files.bits_file,
                                                            pos_sp_o_adjacency_list_files.blocks_file,
                                                            pos_sp_o_adjacency_list_files.sblocks_file,
                                                            pos_sp_o_adjacency_list_files.nums_file,

                                                            neg_s_p_adjacency_list_files.bits_file,
                                                            neg_s_p_adjacency_list_files.blocks_file,
                                                            neg_s_p_adjacency_list_files.sblocks_file,
                                                            neg_s_p_adjacency_list_files.nums_file,

                                                            neg_sp_o_adjacency_list_files.bits_file,
                                                            neg_sp_o_adjacency_list_files.blocks_file,
                                                            neg_sp_o_adjacency_list_files.sblocks_file,
                                                            neg_sp_o_adjacency_list_files.nums_file,

                                                            num_nodes,
                                                            num_predicates,
                                                            num_values))
            })
    }
}

pub struct ChildLayerFileBuilderPhase2<F:'static+FileLoad+FileStore> {
    parent: GenericLayer<F::Map>,

    pos_subjects_file: F,
    neg_subjects_file: F,
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

impl<F:'static+FileLoad+FileStore> ChildLayerFileBuilderPhase2<F> {
    fn new(parent: GenericLayer<F::Map>,
           pos_subjects_file: F,
           neg_subjects_file: F,

           pos_s_p_adjacency_list_bits_file: F,
           pos_s_p_adjacency_list_blocks_file: F,
           pos_s_p_adjacency_list_sblocks_file: F,
           pos_s_p_adjacency_list_nums_file: F,
           
           pos_sp_o_adjacency_list_bits_file: F,
           pos_sp_o_adjacency_list_blocks_file: F,
           pos_sp_o_adjacency_list_sblocks_file: F,
           pos_sp_o_adjacency_list_nums_file: F,

           neg_s_p_adjacency_list_bits_file: F,
           neg_s_p_adjacency_list_blocks_file: F,
           neg_s_p_adjacency_list_sblocks_file: F,
           neg_s_p_adjacency_list_nums_file: F,
           
           neg_sp_o_adjacency_list_bits_file: F,
           neg_sp_o_adjacency_list_blocks_file: F,
           neg_sp_o_adjacency_list_sblocks_file: F,
           neg_sp_o_adjacency_list_nums_file: F,

           num_nodes: usize,
           num_predicates: usize,
           num_values: usize
    ) -> Self {
        let pos_subjects = Vec::new();
        let neg_subjects = Vec::new();
        let s_p_width = ((parent.predicate_count() + num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((parent.node_and_value_count() + num_nodes + num_values + 1) as f32).log2().ceil() as u8;
        let pos_s_p_adjacency_list_builder = AdjacencyListBuilder::new(pos_s_p_adjacency_list_bits_file,
                                                                       pos_s_p_adjacency_list_blocks_file.open_write(),
                                                                       pos_s_p_adjacency_list_sblocks_file.open_write(),
                                                                       pos_s_p_adjacency_list_nums_file.open_write(),
                                                                       s_p_width);

        let pos_sp_o_adjacency_list_builder = AdjacencyListBuilder::new(pos_sp_o_adjacency_list_bits_file,
                                                                        pos_sp_o_adjacency_list_blocks_file.open_write(),
                                                                        pos_sp_o_adjacency_list_sblocks_file.open_write(),
                                                                        pos_sp_o_adjacency_list_nums_file.open_write(),
                                                                        sp_o_width);

        let neg_s_p_adjacency_list_builder = AdjacencyListBuilder::new(neg_s_p_adjacency_list_bits_file,
                                                                       neg_s_p_adjacency_list_blocks_file.open_write(),
                                                                       neg_s_p_adjacency_list_sblocks_file.open_write(),
                                                                       neg_s_p_adjacency_list_nums_file.open_write(),
                                                                       s_p_width);

        let neg_sp_o_adjacency_list_builder = AdjacencyListBuilder::new(neg_sp_o_adjacency_list_bits_file,
                                                                        neg_sp_o_adjacency_list_blocks_file.open_write(),
                                                                        neg_sp_o_adjacency_list_sblocks_file.open_write(),
                                                                        neg_sp_o_adjacency_list_nums_file.open_write(),
                                                                        sp_o_width);

        ChildLayerFileBuilderPhase2 {
            parent,
            pos_subjects_file,
            neg_subjects_file,
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

    pub fn add_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>> {
        if self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            // TODO maybe return an error instead?
            return Box::new(future::ok(self));
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            pos_subjects_file,
            neg_subjects_file,
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

        let mapped_subject = pos_subjects.len() as u64;
        
        if pos_last_subject == subject && pos_last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = pos_s_p_adjacency_list_builder.count() + 1;
            Box::new(pos_sp_o_adjacency_list_builder.push(count, object)
                     .map(move |pos_sp_o_adjacency_list_builder| {
                         ChildLayerFileBuilderPhase2 {
                             parent,
                             pos_subjects_file,
                             neg_subjects_file,
                             pos_subjects,
                             neg_subjects,
                             
                             pos_s_p_adjacency_list_builder,
                             pos_sp_o_adjacency_list_builder,
                             pos_last_subject: mapped_subject,
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
            Box::new(
                pos_s_p_adjacency_list_builder.push(mapped_subject, predicate)
                    .and_then(move |pos_s_p_adjacency_list_builder| {
                        let count = pos_s_p_adjacency_list_builder.count() + 1;
                        pos_sp_o_adjacency_list_builder.push(count, object)
                            .map(move |pos_sp_o_adjacency_list_builder| {
                                ChildLayerFileBuilderPhase2 {
                                    parent,
                                    pos_subjects_file,
                                    neg_subjects_file,
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

    pub fn remove_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>> {
        if !self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            // TODO maybe return an error instead?
            return Box::new(future::ok(self));
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            pos_subjects_file,
            neg_subjects_file,
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

        let mapped_subject = neg_subjects.len() as u64;
        
        if neg_last_subject == subject && neg_last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = neg_s_p_adjacency_list_builder.count() + 1;
            Box::new(neg_sp_o_adjacency_list_builder.push(count, object)
                     .map(move |neg_sp_o_adjacency_list_builder| {
                         ChildLayerFileBuilderPhase2 {
                             parent,
                             pos_subjects_file,
                             neg_subjects_file,
                             pos_subjects,
                             neg_subjects,
                             
                             pos_s_p_adjacency_list_builder,
                             pos_sp_o_adjacency_list_builder,
                             pos_last_subject,
                             pos_last_predicate,

                             neg_s_p_adjacency_list_builder,
                             neg_sp_o_adjacency_list_builder,
                             neg_last_subject: mapped_subject,
                             neg_last_predicate: predicate,
                         }
                     }))
        }
        else {
            // both list have to be pushed to
            Box::new(
                neg_s_p_adjacency_list_builder.push(mapped_subject, predicate)
                    .and_then(move |neg_s_p_adjacency_list_builder| {
                        let count = neg_s_p_adjacency_list_builder.count() + 1;
                        neg_sp_o_adjacency_list_builder.push(count, object)
                            .map(move |neg_sp_o_adjacency_list_builder| {
                                ChildLayerFileBuilderPhase2 {
                                    parent,
                                    pos_subjects_file,
                                    neg_subjects_file,
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

    pub fn add_id_triples<I:'static+IntoIterator<Item=IdTriple>>(self, triples: I) -> impl Future<Item=Self, Error=std::io::Error> {
        stream::iter_ok(triples)
                 .fold(self, |b, triple| b.add_triple(triple.subject, triple.predicate, triple.object))
    }

    pub fn remove_id_triples<I:'static+IntoIterator<Item=IdTriple>>(self, triples: I) -> impl Future<Item=Self, Error=std::io::Error> {
        stream::iter_ok(triples)
                 .fold(self, |b, triple| b.remove_triple(triple.subject, triple.predicate, triple.object))
    }


    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        let max_pos_subject = if self.pos_subjects.len() == 0 { 0 } else { self.pos_subjects[self.pos_subjects.len() - 1] };
        let max_neg_subject = if self.neg_subjects.len() == 0 { 0 } else { self.neg_subjects[self.neg_subjects.len() - 1] };
        let pos_subjects_width = 1+(max_pos_subject as f32).log2().ceil() as u8;
        let neg_subjects_width = 1+(max_neg_subject as f32).log2().ceil() as u8;
        let pos_subjects_logarray_builder = LogArrayFileBuilder::new(self.pos_subjects_file.open_write(), pos_subjects_width);
        let neg_subjects_logarray_builder = LogArrayFileBuilder::new(self.neg_subjects_file.open_write(), neg_subjects_width);

        let build_pos_s_p_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(self.pos_s_p_adjacency_list_builder.finalize());
        let build_pos_sp_o_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(self.pos_sp_o_adjacency_list_builder.finalize());
        let build_neg_s_p_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(self.neg_s_p_adjacency_list_builder.finalize());
        let build_neg_sp_o_adjacency_list: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(self.neg_sp_o_adjacency_list_builder.finalize());

        let build_pos_subjects: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(pos_subjects_logarray_builder.push_all(stream::iter_ok(self.pos_subjects))
                                                                                         .and_then(|b|b.finalize())
                                                                                         .map(|_|()));
        let build_neg_subjects: Box<dyn Future<Item=(), Error=std::io::Error>> = Box::new(neg_subjects_logarray_builder.push_all(stream::iter_ok(self.neg_subjects))
                                                                                          .and_then(|b|b.finalize())
                                                                                          .map(|_|()));

        future::join_all(vec![build_pos_s_p_adjacency_list,
                              build_pos_sp_o_adjacency_list,
                              build_neg_s_p_adjacency_list,
                              build_neg_sp_o_adjacency_list,
                              build_pos_subjects,
                              build_neg_subjects])
            .map(|_|())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file::*;
    use crate::layer::base::*;
    use super::*;

    fn example_base_layer() -> BaseLayer<Vec<u8>> {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_builder = BaseLayerFileBuilder::new(base_files[0].clone(), base_files[1].clone(), base_files[2].clone(), base_files[3].clone(), base_files[4].clone(), base_files[5].clone(), base_files[6].clone(), base_files[7].clone(), base_files[8].clone(), base_files[9].clone(), base_files[10].clone(), base_files[11].clone(), base_files[12].clone(), base_files[13].clone());

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

        let base_layer = BaseLayer::load(base_files[0].clone().map(), base_files[1].clone().map(), base_files[2].clone().map(), base_files[3].clone().map(), base_files[4].clone().map(), base_files[5].clone().map(), base_files[6].clone().map(), base_files[7].clone().map(), base_files[8].clone().map(), base_files[9].clone().map(), base_files[10].clone().map(), base_files[11].clone().map(), base_files[12].clone().map(), base_files[13].clone().map());

        base_layer
    }
    
    #[test]
    fn empty_child_layer_equivalent_to_parent() {
        let base_layer = example_base_layer();

        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder.into_phase2()
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

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

        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(2,1,2))
            .and_then(|b| b.add_triple(3,3,3))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

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

        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder.into_phase2()
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

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
        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

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
        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

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
    fn adding_new_nodes_predicates_and_values_in_child() {
        let base_layer = example_base_layer();
        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder.into_phase2()
            .and_then(|b| b.add_triple(11,2,3))
            .and_then(|b| b.add_triple(12,3,4))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

        assert!(child_layer.triple_exists(11,2,3));
        assert!(child_layer.triple_exists(12,3,4));
    }

    #[test]
    fn old_dictionary_entries_in_child() {
        let base_layer = example_base_layer();
        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder
            .add_node("foo")
            .and_then(|(_,b)|b.add_predicate("bar"))
            .and_then(|(_,b)|b.add_value("baz"))
            .and_then(|(_,b)|b.into_phase2())
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

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
        let parent = GenericLayer::Base(base_layer);

        let child_files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone(), child_files[23].clone());
        child_builder
            .add_node("foo")
            .and_then(|(_,b)|b.add_predicate("bar"))
            .and_then(|(_,b)|b.add_value("baz"))
            .and_then(|(_,b)|b.into_phase2())
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map(), child_files[23].clone().map());

        assert_eq!(11, child_layer.subject_id("foo").unwrap());
        assert_eq!(5, child_layer.predicate_id("bar").unwrap());
        assert_eq!(11, child_layer.object_node_id("foo").unwrap());
        assert_eq!(12, child_layer.object_value_id("baz").unwrap());

        assert_eq!("foo", child_layer.id_subject(11).unwrap());
        assert_eq!("bar", child_layer.id_predicate(5).unwrap());
        assert_eq!(ObjectType::Node("foo".to_string()), child_layer.id_object(11).unwrap());
        assert_eq!(ObjectType::Value("baz".to_string()), child_layer.id_object(12).unwrap());
    }
}
