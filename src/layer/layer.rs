use super::base::*;
use super::child::*;

pub trait Layer {
    type PredicateObjectPairsForSubject: PredicateObjectPairsForSubject;

    fn node_and_value_count(&self) -> usize;
    fn predicate_count(&self) -> usize;

    fn subject_id(&self, subject: &str) -> Option<u64>;
    fn predicate_id(&self, predicate: &str) -> Option<u64>;
    fn object_node_id(&self, object: &str) -> Option<u64>;
    fn object_value_id(&self, object: &str) -> Option<u64>;
    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<Self::PredicateObjectPairsForSubject>;
}

#[derive(Clone)]
pub enum ParentLayer<M:AsRef<[u8]>+Clone> {
    Base(BaseLayer<M>),
    Child(ChildLayer<M>)
}

impl<M:AsRef<[u8]>+Clone> Layer for ParentLayer<M> {
    type PredicateObjectPairsForSubject = ParentPredicateObjectPairsForSubject<M>;

    fn node_and_value_count(&self) -> usize {
        match self {
            Self::Base(b) => b.node_and_value_count(),
            Self::Child(c) => c.node_and_value_count()
        }
    }

    fn predicate_count(&self) -> usize {
        match self {
            Self::Base(b) => b.predicate_count(),
            Self::Child(c) => c.predicate_count()
        }
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.subject_id(subject),
            Self::Child(c) => c.subject_id(subject)
        }
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.predicate_id(predicate),
            Self::Child(c) => c.predicate_id(predicate)
        }
    }

    fn object_node_id(&self, node: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.object_node_id(node),
            Self::Child(c) => c.object_node_id(node)
        }
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.object_value_id(value),
            Self::Child(c) => c.object_value_id(value)
        }
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<ParentPredicateObjectPairsForSubject<M>> {
        match self {
            Self::Base(b) => b.predicate_object_pairs_for_subject(subject).map(|b| ParentPredicateObjectPairsForSubject::Base(b)),
            Self::Child(c) => c.predicate_object_pairs_for_subject(subject).map(|c| ParentPredicateObjectPairsForSubject::Child(c)),
        }
    }
}

pub trait PredicateObjectPairsForSubject {
    type Objects: ObjectsForSubjectPredicatePair;
    fn objects_for_predicate(&self, predicate: u64) -> Option<Self::Objects>;
}

pub trait ObjectsForSubjectPredicatePair {
    fn triple(&self, object: u64) -> Option<IdTriple>;
}

#[derive(Clone,Copy)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64
}

#[derive(Clone)]
pub enum ParentPredicateObjectPairsForSubject<M:AsRef<[u8]>+Clone> {
    Base(BasePredicateObjectPairsForSubject<M>),
    Child(ChildPredicateObjectPairsForSubject<M>)
}

impl<M:AsRef<[u8]>+Clone> PredicateObjectPairsForSubject for ParentPredicateObjectPairsForSubject<M> {
    type Objects = ParentObjectsForSubjectPredicatePair<M>;

    fn objects_for_predicate(&self, predicate: u64) -> Option<ParentObjectsForSubjectPredicatePair<M>> {
        match self {
            Self::Base(b) => b.objects_for_predicate(predicate).map(|b| ParentObjectsForSubjectPredicatePair::Base(b)),
            Self::Child(c) => c.objects_for_predicate(predicate).map(|c| ParentObjectsForSubjectPredicatePair::Child(c)),
        }
    }
}

#[derive(Clone)]
pub enum ParentObjectsForSubjectPredicatePair<M:AsRef<[u8]>+Clone> {
    Base(BaseObjectsForSubjectPredicatePair<M>),
    Child(ChildObjectsForSubjectPredicatePair<M>)
}

impl<M:AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair for ParentObjectsForSubjectPredicatePair<M> {
    fn triple(&self, object: u64) -> Option<IdTriple> {
        match self {
            Self::Base(b) => b.triple(object),
            Self::Child(c) => c.triple(object)
        }
    }
}
