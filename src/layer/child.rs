use super::layer::*;
use crate::structure::*;

#[derive(Clone)]
pub struct ChildLayer<M:AsRef<[u8]>+Clone> {
    parent: Box<ParentLayer<M>>,

    node_dictionary: Option<PfcDict<M>>,
    predicate_dictionary: Option<PfcDict<M>>,
    value_dictionary: Option<PfcDict<M>>,

    subjects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,

    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:AsRef<[u8]>+Clone> Layer for ChildLayer<M> {
    type PredicateObjectPairsForSubject = ChildPredicateObjectPairsForSubject<M>;

    fn node_and_value_count(&self) -> usize {
        self.node_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.value_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.predicate_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self.node_dictionary.as_ref().and_then(|dict| dict.id(subject)) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.subject_id(subject)
        }
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self.predicate_dictionary.as_ref().and_then(|dict| dict.id(predicate)) {
            Some(id) => Some(self.parent.predicate_count() as u64 + id + 1),
            None => self.parent.predicate_id(predicate)
        }
    }

    fn object_node_id(&self, node: &str) -> Option<u64> {
        match self.node_dictionary.as_ref().and_then(|dict| dict.id(node)) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.object_node_id(node)
        }
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        match self.value_dictionary.as_ref().and_then(|dict| dict.id(value)) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.object_value_id(value)
        }
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<ChildPredicateObjectPairsForSubject<M>> {
        if subject == 0 {
            return None;
        }

        let mut pos: Option<AdjacencyStuff<M>> = None;
        let mut neg: Option<AdjacencyStuff<M>> = None;
        
        // first determine where we should be looking.
        let index = self.subjects.index_of(subject);
        let parent = self.parent.predicate_object_pairs_for_subject(subject).map(|p|Box::new(p));
        if index.is_none() && parent.is_none() {
            return None;
        }

        if index.is_some() {
            // subject is mentioned in this layer (as an insert or delete), and might be in the parent layer as well
            let mapped_subject = index.unwrap() as u64 + 1;
            let pos_predicates = self.pos_s_p_adjacency_list.get(mapped_subject);
            let pos_sp_offset = self.pos_s_p_adjacency_list.offset_for(mapped_subject);

            let neg_predicates = self.neg_s_p_adjacency_list.get(mapped_subject);
            let neg_sp_offset = self.neg_s_p_adjacency_list.offset_for(mapped_subject);

            pos = Some(AdjacencyStuff {
                predicates: pos_predicates,
                sp_offset: pos_sp_offset,
                sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
            });

            neg = Some(AdjacencyStuff {
                predicates: neg_predicates,
                sp_offset: neg_sp_offset,
                sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
            });
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
struct AdjacencyStuff<M:AsRef<[u8]>+Clone> {
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}


#[derive(Clone)]
pub struct ChildPredicateObjectPairsForSubject<M:AsRef<[u8]>+Clone> {
    parent: Option<Box<ParentPredicateObjectPairsForSubject<M>>>,
    subject: u64,

    pos: Option<AdjacencyStuff<M>>,
    neg: Option<AdjacencyStuff<M>>,
}

impl<M:AsRef<[u8]>+Clone> PredicateObjectPairsForSubject for ChildPredicateObjectPairsForSubject<M> {
    type Objects = ChildObjectsForSubjectPredicatePair<M>;
    fn objects_for_predicate(&self, predicate: u64) -> Option<ChildObjectsForSubjectPredicatePair<M>> {
        if predicate == 0 {
            return None;
        }
        // returns None if
        // - the pos dictionary does not contain anything for this predicate, AND
        // - parent returns None
        // otherwise look up pos and neg and create the next object
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
pub struct ChildObjectsForSubjectPredicatePair<M:AsRef<[u8]>+Clone> {
    parent: Option<Box<ParentObjectsForSubjectPredicatePair<M>>>,
    subject: u64,
    predicate: u64,
    pos_objects: Option<LogArraySlice<M>>,
    neg_objects: Option<LogArraySlice<M>>
}

impl<M:AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair for ChildObjectsForSubjectPredicatePair<M> {
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
