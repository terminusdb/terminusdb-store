use super::base::*;
use super::child::*;
use crate::storage::file::*;
use std::hash::Hash;
use std::collections::HashMap;

pub trait Layer {
    type PredicateObjectPairsForSubject: PredicateObjectPairsForSubject;
    type SubjectIterator: 'static+Iterator<Item=Self::PredicateObjectPairsForSubject>;

    fn node_and_value_count(&self) -> usize;
    fn predicate_count(&self) -> usize;

    fn subject_id(&self, subject: &str) -> Option<u64>;
    fn predicate_id(&self, predicate: &str) -> Option<u64>;
    fn object_node_id(&self, object: &str) -> Option<u64>;
    fn object_value_id(&self, object: &str) -> Option<u64>;
    fn id_subject(&self, id: u64) -> Option<String>;
    fn id_predicate(&self, id: u64) -> Option<String>;
    fn id_object(&self, id: u64) -> Option<ObjectType>;

    fn subjects(&self) -> Self::SubjectIterator;
    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<Self::PredicateObjectPairsForSubject>;
    
    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.predicate_object_pairs_for_subject(subject)
            .and_then(|pairs| pairs.objects_for_predicate(predicate))
            .and_then(|objects| objects.triple(object))
            .is_some()
    }

    fn id_triple_exists(&self, triple: IdTriple) -> bool {
        self.triple_exists(triple.subject, triple.predicate, triple.object)
    }

    fn string_triple_exists(&self, triple: &StringTriple) -> bool {
        self.string_triple_to_id(triple)
            .map(|t| self.id_triple_exists(t))
            .unwrap_or(false)
    }

    fn triples(&self) -> Box<dyn Iterator<Item=IdTriple>> {
        Box::new(self.subjects().map(|s|s.predicates()).flatten()
                 .map(|p|p.triples()).flatten())
    }

    fn string_triple_to_id(&self, triple: &StringTriple) -> Option<IdTriple> {
        self.subject_id(&triple.subject)
            .and_then(|subject| self.predicate_id(&triple.predicate)
                      .and_then(|predicate| match &triple.object {
                          ObjectType::Node(node) => self.object_node_id(&node),
                          ObjectType::Value(value) => self.object_value_id(&value)
                      }.map(|object| IdTriple {
                          subject,
                          predicate,
                          object
                      })))
    }

    fn string_triple_to_partially_resolved(&self, triple: &StringTriple) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: self.subject_id(&triple.subject)
                .map(|id| PossiblyResolved::Resolved(id))
                .unwrap_or(PossiblyResolved::Unresolved(triple.subject.clone())),
            predicate: self.predicate_id(&triple.predicate)
                .map(|id| PossiblyResolved::Resolved(id))
                .unwrap_or(PossiblyResolved::Unresolved(triple.predicate.clone())),
            object: match &triple.object {
                ObjectType::Node(node) => self.object_node_id(&node)
                    .map(|id| PossiblyResolved::Resolved(id))
                    .unwrap_or(PossiblyResolved::Unresolved(triple.object.clone())),
                ObjectType::Value(value) => self.object_value_id(&value)
                    .map(|id| PossiblyResolved::Resolved(id))
                    .unwrap_or(PossiblyResolved::Unresolved(triple.object.clone())),
            }
        }
    }

    fn id_triple_to_string(&self, triple: &IdTriple) -> Option<StringTriple> {
        self.id_subject(triple.subject)
            .and_then(|subject| self.id_predicate(triple.predicate)
                      .and_then(|predicate| self.id_object(triple.object)
                                .map(|object| StringTriple {
                                    subject,
                                    predicate,
                                    object
                                })))
    }
}

#[derive(Clone)]
pub enum GenericLayer<M:'static+AsRef<[u8]>+Clone> {
    Base(BaseLayer<M>),
    Child(ChildLayer<M>)
}

impl<M:'static+AsRef<[u8]>+Clone> Layer for GenericLayer<M> {
    type PredicateObjectPairsForSubject = GenericPredicateObjectPairsForSubject<M>;
    type SubjectIterator = GenericSubjectIterator<M>;

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

    fn id_subject(&self, id: u64) -> Option<String> {
        match self {
            Self::Base(b) => b.id_subject(id),
            Self::Child(c) => c.id_subject(id)
        }
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        match self {
            Self::Base(b) => b.id_predicate(id),
            Self::Child(c) => c.id_predicate(id)
        }
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        match self {
            Self::Base(b) => b.id_object(id),
            Self::Child(c) => c.id_object(id)
        }
    }

    fn subjects(&self) -> GenericSubjectIterator<M> {
        match self {
            Self::Base(b) => GenericSubjectIterator::Base(b.subjects()),
            Self::Child(c) => GenericSubjectIterator::Child(c.subjects())
        }
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<GenericPredicateObjectPairsForSubject<M>> {
        match self {
            Self::Base(b) => b.predicate_object_pairs_for_subject(subject).map(|b| GenericPredicateObjectPairsForSubject::Base(b)),
            Self::Child(c) => c.predicate_object_pairs_for_subject(subject).map(|c| GenericPredicateObjectPairsForSubject::Child(c)),
        }
    }
}

#[derive(Clone)]
pub enum GenericSubjectIterator<M:'static+AsRef<[u8]>+Clone> {
    Base(BaseSubjectIterator<M>),
    Child(ChildSubjectIterator<M>)
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for GenericSubjectIterator<M> {
    type Item = GenericPredicateObjectPairsForSubject<M>;
    
    fn next(&mut self) -> Option<GenericPredicateObjectPairsForSubject<M>> {
        match self {
            Self::Base(b) => b.next().map(|b|GenericPredicateObjectPairsForSubject::Base(b)),
            Self::Child(c) => c.next().map(|c|GenericPredicateObjectPairsForSubject::Child(c))
        }
    }
}

pub trait PredicateObjectPairsForSubject {
    type Objects: ObjectsForSubjectPredicatePair;
    type PredicateIterator: 'static+Iterator<Item=Self::Objects>;

    fn subject(&self) -> u64;

    fn predicates(&self) -> Self::PredicateIterator;
    fn objects_for_predicate(&self, predicate: u64) -> Option<Self::Objects>;

    fn triples(&self) -> Box<dyn Iterator<Item=IdTriple>> {
        Box::new(self.predicates().map(|p|p.triples()).flatten())
    }
}

pub trait ObjectsForSubjectPredicatePair {
    type ObjectIterator: Iterator<Item=IdTriple>;

    fn subject(&self) -> u64;
    fn predicate(&self) -> u64;

    fn triples(&self) -> Self::ObjectIterator;
    fn triple(&self, object: u64) -> Option<IdTriple>;
}

#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64
}

impl IdTriple {
    pub fn to_resolved(&self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Resolved(self.subject),
            predicate: PossiblyResolved::Resolved(self.predicate),
            object: PossiblyResolved::Resolved(self.object),
        }
    }
}

#[derive(Debug,Clone,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct StringTriple {
    pub subject: String,
    pub predicate: String,
    pub object: ObjectType
}

impl StringTriple {
    pub fn new_node(subject: &str, predicate: &str, object: &str) -> StringTriple {
        StringTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Node(object.to_owned())
        }
    }

    pub fn new_value(subject: &str, predicate: &str, object: &str) -> StringTriple {
        StringTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Value(object.to_owned())
        }
    }

    pub fn to_unresolved(&self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Unresolved(self.subject.clone()),
            predicate: PossiblyResolved::Unresolved(self.predicate.clone()),
            object: PossiblyResolved::Unresolved(self.object.clone()),
        }
    }
}

#[derive(Debug,Clone,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub enum PossiblyResolved<T:Clone+PartialEq+Eq+PartialOrd+Ord+Hash> {
    Unresolved(T),
    Resolved(u64)
}

impl<T:Clone+PartialEq+Eq+PartialOrd+Ord+Hash> PossiblyResolved<T> {
    pub fn is_resolved(&self) -> bool {
        match self {
            Self::Unresolved(_) => false,
            Self::Resolved(_) => true
        }
    }

    pub fn as_ref(&self) -> PossiblyResolved<&T> {
        match self {
            Self::Unresolved(u) => PossiblyResolved::Unresolved(&u),
            Self::Resolved(id) => PossiblyResolved::Resolved(*id)
        }
    }

    pub fn unwrap_unresolved(self) -> T {
        match self {
            Self::Unresolved(u) => u,
            Self::Resolved(_) => panic!("tried to unwrap unresolved, but got a resolved"),
        }
    }

    pub fn unwrap_resolved(self) -> u64 {
        match self {
            Self::Unresolved(_) => panic!("tried to unwrap resolved, but got an unresolved"),
            Self::Resolved(id) => id
        }
    }
}

#[derive(Debug,Clone,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct PartiallyResolvedTriple {
    pub subject: PossiblyResolved<String>,
    pub predicate: PossiblyResolved<String>,
    pub object: PossiblyResolved<ObjectType>,
}

impl PartiallyResolvedTriple {
    pub fn resolve_with(&self, node_map: &HashMap<String, u64>, predicate_map: &HashMap<String, u64>, value_map: &HashMap<String, u64>) -> Option<IdTriple> {
        let subject = match self.subject.as_ref() {
            PossiblyResolved::Unresolved(s) => *node_map.get(s)?,
            PossiblyResolved::Resolved(id) => id
        };
        let predicate = match self.predicate.as_ref() {
            PossiblyResolved::Unresolved(p) => *predicate_map.get(p)?,
            PossiblyResolved::Resolved(id) => id
        };
        let object = match self.object.as_ref() {
            PossiblyResolved::Unresolved(ObjectType::Node(n)) => *node_map.get(n)?,
            PossiblyResolved::Unresolved(ObjectType::Value(v)) => *value_map.get(v)?,
            PossiblyResolved::Resolved(id) => id
        };

        Some(IdTriple { subject, predicate, object })
    }
}

#[derive(Clone)]
pub enum GenericPredicateObjectPairsForSubject<M:'static+AsRef<[u8]>+Clone> {
    Base(BasePredicateObjectPairsForSubject<M>),
    Child(ChildPredicateObjectPairsForSubject<M>)
}

impl<M:'static+AsRef<[u8]>+Clone> PredicateObjectPairsForSubject for GenericPredicateObjectPairsForSubject<M> {
    type Objects = GenericObjectsForSubjectPredicatePair<M>;
    type PredicateIterator = GenericPredicateIterator<M>;

    fn subject(&self) -> u64 {
        match self {
            Self::Base(b) => b.subject(),
            Self::Child(c) => c.subject(),
        }
    }

    fn predicates(&self) -> GenericPredicateIterator<M> {
        match self {
            Self::Base(b) => GenericPredicateIterator::Base(b.predicates()),
            Self::Child(c) => GenericPredicateIterator::Child(c.predicates())
        }
    }

    fn objects_for_predicate(&self, predicate: u64) -> Option<GenericObjectsForSubjectPredicatePair<M>> {
        match self {
            Self::Base(b) => b.objects_for_predicate(predicate).map(|b| GenericObjectsForSubjectPredicatePair::Base(b)),
            Self::Child(c) => c.objects_for_predicate(predicate).map(|c| GenericObjectsForSubjectPredicatePair::Child(c)),
        }
    }
}

#[derive(Clone)]
pub enum GenericPredicateIterator<M:'static+AsRef<[u8]>+Clone> {
    Base(BasePredicateIterator<M>),
    Child(ChildPredicateIterator<M>)
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for GenericPredicateIterator<M> {
    type Item = GenericObjectsForSubjectPredicatePair<M>;

    fn next(&mut self) -> Option<GenericObjectsForSubjectPredicatePair<M>> {
        match self {
            Self::Base(b) => b.next().map(|b|GenericObjectsForSubjectPredicatePair::Base(b)),
            Self::Child(c) => c.next().map(|c|GenericObjectsForSubjectPredicatePair::Child(c)),
        }
    }
}

#[derive(Clone)]
pub enum GenericObjectsForSubjectPredicatePair<M:'static+AsRef<[u8]>+Clone> {
    Base(BaseObjectsForSubjectPredicatePair<M>),
    Child(ChildObjectsForSubjectPredicatePair<M>)
}

impl<M:'static+AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair for GenericObjectsForSubjectPredicatePair<M> {
    type ObjectIterator = GenericObjectIterator<M>;

    fn subject(&self) -> u64 {
        match self {
            Self::Base(b) => b.subject(),
            Self::Child(c) => c.subject()
        }
    }

    fn predicate(&self) -> u64 {
        match self {
            Self::Base(b) => b.predicate(),
            Self::Child(c) => c.predicate()
        }
    }

    fn triples(&self) -> GenericObjectIterator<M> {
        match self {
            Self::Base(b) => GenericObjectIterator::Base(b.triples()),
            Self::Child(c) => GenericObjectIterator::Child(c.triples())
        }
    }

    fn triple(&self, object: u64) -> Option<IdTriple> {
        match self {
            Self::Base(b) => b.triple(object),
            Self::Child(c) => c.triple(object)
        }
    }
}

#[derive(Clone)]
pub enum GenericObjectIterator<M:'static+AsRef<[u8]>+Clone> {
    Base(BaseObjectIterator<M>),
    Child(ChildObjectIterator<M>)
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for GenericObjectIterator<M> {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        match self {
            Self::Base(b) => b.next(),
            Self::Child(c) => c.next()
        }
    }
}

pub struct DictionaryFiles<F:'static+FileLoad+FileStore> {
    pub blocks_file: F,
    pub offsets_file: F
}

pub struct AdjacencyListFiles<F:'static+FileLoad+FileStore> {
    pub bits_file: F,
    pub blocks_file: F,
    pub sblocks_file: F,
    pub nums_file: F,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub enum ObjectType {
    Node(String),
    Value(String)
}
