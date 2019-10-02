//! Common data structures and traits for all layer types
use super::base::*;
use super::child::*;
use crate::storage::file::*;
use std::hash::Hash;
use std::collections::HashMap;
use std::sync::Arc;

/// A layer containing dictionary entries and triples
///
/// A layer can be queried. To answer queries, layers will check their
/// own data structures, and if they have a parent, the parent is
/// queried as well.
pub trait Layer: Send+Sync {
    /// The name of this layer
    fn name(&self) -> [u32;5];

    /// The parent of this layer, or None if this is a base layer
    fn parent(&self) -> Option<Arc<dyn Layer>>;

    /// The amount of nodes and values known to this layer
    /// This also counts entries in the parent.
    fn node_and_value_count(&self) -> usize;
    /// The amount of predicates known to this layer
    /// This also counts entries in the parent.
    fn predicate_count(&self) -> usize;

    /// The numerical id of a subject, or None if the subject cannot be found
    fn subject_id(&self, subject: &str) -> Option<u64>;
    /// The numerical id of a predicate, or None if the predicate cannot be found
    fn predicate_id(&self, predicate: &str) -> Option<u64>;
    /// The numerical id of a node object, or None if the node object cannot be found
    fn object_node_id(&self, object: &str) -> Option<u64>;
    /// The numerical id of a value object, or None if the value object cannot be found
    fn object_value_id(&self, object: &str) -> Option<u64>;
    /// The subject corresponding to a numerical id, or None if it cannot be found
    fn id_subject(&self, id: u64) -> Option<String>;
    /// The predicate corresponding to a numerical id, or None if it cannot be found
    fn id_predicate(&self, id: u64) -> Option<String>;
    /// The object corresponding to a numerical id, or None if it cannot be found
    fn id_object(&self, id: u64) -> Option<ObjectType>;

    /// Returns an iterator over all triple data known to this layer
    ///
    /// This data is returned by
    /// `SubjectLookup`. Each such object stores a
    /// subject id, and knows how to retrieve any linked
    /// predicate-object pair.
    fn subjects(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>>;

    /// Returns a `SubjectLookup` object for the given subject, or None if it cannot be constructed
    ///
    /// Note that even if a value is returned here, that doesn't
    /// necessarily mean that there will be triples for the given
    /// subject. All it means is that this layer or a parent layer has
    /// registered an addition involving this subject. However, later
    /// layers may have then removed every triple involving this
    /// subject.
    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>>;
    
    /// Returns true if the given triple exists, and false otherwise
    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.lookup_subject(subject)
            .and_then(|pairs| pairs.lookup_predicate(predicate))
            .and_then(|objects| objects.triple(object))
            .is_some()
    }

    /// Returns true if the given triple exists, and false otherwise
    fn id_triple_exists(&self, triple: IdTriple) -> bool {
        self.triple_exists(triple.subject, triple.predicate, triple.object)
    }

    /// Returns true if the given triple exists, and false otherwise
    fn string_triple_exists(&self, triple: &StringTriple) -> bool {
        self.string_triple_to_id(triple)
            .map(|t| self.id_triple_exists(t))
            .unwrap_or(false)
    }

    /// Iterator over all triples known to this layer.
    ///
    /// This is a convenient werapper around
    /// `SubjectLookup` and
    /// `SubjectPredicateLookup` style querying.
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

    /// Convert all known strings in the given string triple to ids
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

    /// Convert an id triple to the corresponding string version, returning None if any of those ids could not be converted
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

    /// Returns true if the given layer is an ancestor of this layer, false otherwise
    fn is_ancestor_of(&self, other: &dyn Layer) -> bool {
        match other.parent() {
            None => false,
            Some(parent) => parent.name() == self.name() || self.is_ancestor_of(&*parent)
        }
    }
}

/// The type of a layer - either base or child
#[derive(Clone,Copy)]
pub enum LayerType {
    Base,
    Child
}

/// A wrapper over base and child layer
#[derive(Clone)]
pub enum GenericLayer<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    Base(BaseLayer<M>),
    Child(ChildLayer<M>)
}

impl<M:'static+AsRef<[u8]>+Clone+Send+Sync> Layer for GenericLayer<M> {
    fn name(&self) -> [u32;5] {
        match self {
            Self::Base(b) => b.name(),
            Self::Child(c) => c.name()
        }
    }

    fn parent(&self) -> Option<Arc<dyn Layer>> {
        match self {
            Self::Base(_b) => None,
            Self::Child(c) => c.parent()
        }
    }

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

    fn subjects(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        match self {
            Self::Base(b) => b.subjects(),
            Self::Child(c) => c.subjects()
        }
    }

    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        match self {
            Self::Base(b) => b.lookup_subject(subject),
            Self::Child(c) => c.lookup_subject(subject),
        }
    }
}

/// A trait that caches a lookup in a layer by subject
///
/// This is returned by `Layer::subjects` and
/// `Layer::lookup_subject`. It stores slices of
/// the relevant data structures to allow quick retrieval of
/// predicate-object pairs when one already knows the subject.
pub trait SubjectLookup {
    /// The subject that this lookup is based on
    fn subject(&self) -> u64;

    /// Returns an iterator over predicate lookups
    fn predicates(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>>;
    /// Returns a predicate lookup for the given predicate, or None if no such lookup could be constructed
    ///
    /// Note that even when it can be constructed, that doesn't mean
    /// there's any underlying triples. Having ancestor layers with
    /// additions for a given subject and predicate will cause a
    /// lookup to be constructable, but if subsequent layers deleted
    /// all these triples, none will be retrievable.
    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn SubjectPredicateLookup>>;

    /// Returns an iterator over all triples that can be found by this lookup
    fn triples(&self) -> Box<dyn Iterator<Item=IdTriple>> {
        Box::new(self.predicates().map(|p|p.triples()).flatten())
    }
}

/// a trait that caches a lookup in a layer by subject and predicate
///
/// This is returned by `SubjectLookup::predicates`
/// and `SubjectLookup::lookup_predicate`. It
/// stores slices of the relevant data structures to allow quick
/// retrieval of objects when one already knows the subject and
/// predicate.
pub trait SubjectPredicateLookup {
    /// The subject that this lookup is based on
    fn subject(&self) -> u64;
    /// The predicate that this lookup is based on
    fn predicate(&self) -> u64;

    /// Returns an iterator over all triples that can be found by this lookup
    fn triples(&self) -> Box<dyn Iterator<Item=IdTriple>>;

    /// Returns a triple for the given object, or None if it doesn't exist
    fn triple(&self, object: u64) -> Option<IdTriple>;
}

/// A triple, stored as numerical ids
#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64
}

impl IdTriple {
    /// Construct a new id triple
    pub fn new(subject: u64, predicate: u64, object: u64) -> Self {
        IdTriple { subject, predicate, object }
    }

    /// convert this triple into a `PartiallyResolvedTriple`, which is a data structure used in layer building
    pub fn to_resolved(&self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Resolved(self.subject),
            predicate: PossiblyResolved::Resolved(self.predicate),
            object: PossiblyResolved::Resolved(self.object),
        }
    }
}

/// A triple stored as strings
#[derive(Debug,Clone,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct StringTriple {
    pub subject: String,
    pub predicate: String,
    pub object: ObjectType
}

impl StringTriple {
    /// Construct a triple with a node object
    ///
    /// Nodes may appear in both the subject and object position.
    pub fn new_node(subject: &str, predicate: &str, object: &str) -> StringTriple {
        StringTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Node(object.to_owned())
        }
    }

    /// Construct a triple with a value object
    ///
    /// Values may only appear in the object position.
    pub fn new_value(subject: &str, predicate: &str, object: &str) -> StringTriple {
        StringTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Value(object.to_owned())
        }
    }

    /// Convert this triple to a `PartiallyResolvedTriple`, marking each field as unresolved
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

/// The type of an object in a triple
///
/// Objects in a triple may either be a node or a value. Nodes can be
/// used both in the subject and the object position, while values are
/// only used in the object position.
///
/// Terminus-store keeps track of whether an object was stored as a
/// node or a value, and will return this information in queries. It
/// is possible to have the same string appear both as a node and a
/// value, without this leading to conflicts.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub enum ObjectType {
    Node(String),
    Value(String)
}
