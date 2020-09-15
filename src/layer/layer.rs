//! Common data structures and traits for all layer types.
use std::collections::HashMap;
use std::hash::Hash;
use std::iter::Peekable;

/// A layer containing dictionary entries and triples.
///
/// A layer can be queried. To answer queries, layers will check their
/// own data structures, and if they have a parent, the parent is
/// queried as well.
pub trait Layer: Send + Sync {
    /// The name of this layer.
    fn name(&self) -> [u32; 5];
    fn parent_name(&self) -> Option<[u32; 5]>;

    /// The amount of nodes and values known to this layer.
    /// This also counts entries in the parent.
    fn node_and_value_count(&self) -> usize;
    /// The amount of predicates known to this layer.
    /// This also counts entries in the parent.
    fn predicate_count(&self) -> usize;

    /// The numerical id of a subject, or None if the subject cannot be found.
    fn subject_id(&self, subject: &str) -> Option<u64>;
    /// The numerical id of a predicate, or None if the predicate cannot be found.
    fn predicate_id(&self, predicate: &str) -> Option<u64>;
    /// The numerical id of a node object, or None if the node object cannot be found.
    fn object_node_id(&self, object: &str) -> Option<u64>;
    /// The numerical id of a value object, or None if the value object cannot be found.
    fn object_value_id(&self, object: &str) -> Option<u64>;
    /// The subject corresponding to a numerical id, or None if it cannot be found.
    fn id_subject(&self, id: u64) -> Option<String>;
    /// The predicate corresponding to a numerical id, or None if it cannot be found.
    fn id_predicate(&self, id: u64) -> Option<String>;
    /// The object corresponding to a numerical id, or None if it cannot be found.
    fn id_object(&self, id: u64) -> Option<ObjectType>;

    /// Returns an iterator over all triple data known to this layer.
    ///
    /// This data is returned by
    /// `SubjectLookup`. Each such object stores a
    /// subject id, and knows how to retrieve any linked
    /// predicate-object pair.
    fn subjects(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectLookup>>>;

    /// Returns an iterator over all triple data added by this layer.
    ///
    /// This data is returned by
    /// `SubjectLookup`. Each such object stores a
    /// subject id, and knows how to retrieve any linked
    /// predicate-object pair.
    fn subject_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>>;

    /// A convenience wrapper around `subject_additions`, which returns an iterator over `SubjectLookup`s rather than over `LayerSubjectLookup`s.
    fn subject_additions_generic(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectLookup>>> {
        Box::new(
            GenericSubjectIterator::from_layer_iterator(self.subject_additions())
                .map(|lookup| Box::new(lookup) as Box<dyn SubjectLookup>),
        )
    }

    /// Returns an iterator over all triple data removed by this layer.
    ///
    /// This data is returned by
    /// `SubjectLookup`. Each such object stores a
    /// subject id, and knows how to retrieve any linked
    /// predicate-object pair.
    fn subject_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>>;

    /// A convenience wrapper around `subject_removals`, which returns an iterator over `SubjectLookup`s rather than over `LayerSubjectLookup`s.
    fn subject_removals_generic(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectLookup>>> {
        Box::new(
            GenericSubjectIterator::from_layer_iterator(self.subject_removals())
                .map(|lookup| Box::new(lookup) as Box<dyn SubjectLookup>),
        )
    }

    /// Returns a `SubjectLookup` object for the given subject, or None if it cannot be constructed.
    ///
    /// Note that even if a value is returned here, that doesn't
    /// necessarily mean that there will be triples for the given
    /// subject. All it means is that this layer or a parent layer has
    /// registered an addition involving this subject. However, later
    /// layers may have then removed every triple involving this
    /// subject.
    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>>;

    /// Returns a `LayerSubjectLookup` object for the given subject, or None if it cannot be constructed.
    ///
    /// This will only lookup additions in the current layer.
    ///
    /// Note that even if a value is returned here, that doesn't
    /// necessarily mean that there will be triples for the given
    /// subject. All it means is that this layer or a parent layer has
    /// registered an addition involving this subject. However, later
    /// layers may have then removed every triple involving this
    /// subject.

    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>>;

    /// A convenience wrapper around `lookup_subject_addition`, which returns a `SubjectLookup` rather than a `LayerSubjectLookup`.
    ///
    /// The returned object will look like a lookup into a single
    /// layer that contains just the triples added in this layer.
    fn lookup_subject_addition_generic(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        self.lookup_subject_addition(subject).map(|lookup| {
            Box::new(GenericSubjectLookup::from_layer_lookup(lookup)) as Box<dyn SubjectLookup>
        })
    }

    /// Returns a `LayerSubjectLookup` object for the given subject, or None if it cannot be constructed.
    ///
    /// This will only lookup removals in the current layer.
    fn lookup_subject_removal(&self, subject: u64) -> Option<Box<dyn LayerSubjectLookup>>;

    /// A convenience wrapper around `lookup_subject_removal`, which returns a `SubjectLookup` rather than a `LayerSubjectLookup`.
    ///
    /// The returned object will look like a lookup into a single
    /// layer that contains just the triples removed in this layer.
    fn lookup_subject_removal_generic(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        self.lookup_subject_removal(subject).map(|lookup| {
            Box::new(GenericSubjectLookup::from_layer_lookup(lookup)) as Box<dyn SubjectLookup>
        })
    }

    /// Returns an iterator over all objects known to this layer.
    ///
    /// Objects are returned as an `ObjectLookup`, an object that can
    /// then be queried for subject-predicate pairs pointing to that
    /// object.
    fn objects(&self) -> Box<dyn Iterator<Item = Box<dyn ObjectLookup>>>;

    /// Returns an iterator over all objects added by this layer.
    ///
    /// Objects are returned as an `ObjectLookup`, an object that can
    /// then be queried for subject-predicate pairs pointing to that
    /// object.
    fn object_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>>;

    /// A convenience wrapper around `object_additions`, which returns an iterator over `ObjectLookup`s rather than over `LayerObjectLookup`s.
    fn object_additions_generic(&self) -> Box<dyn Iterator<Item = Box<dyn ObjectLookup>>> {
        Box::new(
            GenericObjectIterator::from_layer_iterator(self.object_additions())
                .map(|lookup| Box::new(lookup) as Box<dyn ObjectLookup>),
        )
    }

    /// Returns an iterator over all objects removed by this layer.
    ///
    /// Objects are returned as an `ObjectLookup`, an object that can
    /// then be queried for subject-predicate pairs pointing to that
    /// object.
    fn object_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>>;

    /// A convenience wrapper around `object_removals`, which returns an iterator over `ObjectLookup`s rather than over `LayerObjectLookup`s.
    fn object_removals_generic(&self) -> Box<dyn Iterator<Item = Box<dyn ObjectLookup>>> {
        Box::new(
            GenericObjectIterator::from_layer_iterator(self.object_removals())
                .map(|lookup| Box::new(lookup) as Box<dyn ObjectLookup>),
        )
    }

    /// Returns an `ObjectLookup` for the given object, or None if it could not be constructed.
    ///
    /// This will only lookup in the current layer.
    /// Note that even if a value is returned here, that doesn't
    /// necessarily mean that there will be triples for the given
    /// object. All it means is that this layer or a parent layer has
    /// registered an addition involving this object. However, later
    /// layers may have then removed every triple involving this
    /// object.
    fn lookup_object(&self, object: u64) -> Option<Box<dyn ObjectLookup>>;

    /// Returns a `LayerObjectLookup` for the given object, or None if it could not be constructed.
    ///
    /// This will only lookup in the current layer.
    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>>;

    /// A convenience wrapper around `lookup_object_addition`, which returns an `ObjectLookup` rather than a `LayerObjectLookup`.
    ///
    /// The returned object will look like a lookup into a single
    /// layer that contains just the triples added in this layer.
    fn lookup_object_addition_generic(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.lookup_object_addition(object).map(|lookup| {
            Box::new(GenericObjectLookup::from_layer_lookup(lookup)) as Box<dyn ObjectLookup>
        })
    }

    /// Returns a `LayerObjectLookup` for the given object, or None if it could not be constructed.
    ///
    /// This will only lookup in the current layer.
    fn lookup_object_removal(&self, object: u64) -> Option<Box<dyn LayerObjectLookup>>;

    /// A convenience wrapper around `lookup_object_removal`, which returns an `ObjectLookup` rather than a `LayerObjectLookup`.
    ///
    /// The returned object will look like a lookup into a single
    /// layer that contains just the triples removed in this layer.
    fn lookup_object_removal_generic(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.lookup_object_removal(object).map(|lookup| {
            Box::new(GenericObjectLookup::from_layer_lookup(lookup)) as Box<dyn ObjectLookup>
        })
    }

    /// Returns a `PredicateLookup` for the given predicate, or None if it could not be constructed.
    ///
    /// Note that even if a value is returned here, that doesn't
    /// necessarily mean that there will be triples for the given
    /// predicate. All it means is that this layer or a parent layer
    /// has registered an addition involving this predicate. However,
    /// later layers may have then removed every triple involving this
    /// predicate.
    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>>;

    /// Returns a `LayerPredicateLookup` for the given predicate, or None if it could not be constructed.
    ///
    /// This will only lookup in the current layer.
    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>>;
    fn lookup_predicate_addition_generic(
        &self,
        predicate: u64,
    ) -> Option<Box<dyn PredicateLookup>> {
        self.lookup_predicate_addition(predicate).map(|lookup| {
            Box::new(GenericPredicateLookup::from_layer_lookup(lookup)) as Box<dyn PredicateLookup>
        })
    }

    /// Returns a `LayerPredicateLookup` for the given predicate, or None if it could not be constructed.
    ///
    /// This will only lookup in the current layer.
    fn lookup_predicate_removal(&self, predicate: u64) -> Option<Box<dyn LayerPredicateLookup>>;

    fn lookup_predicate_removal_generic(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        self.lookup_predicate_removal(predicate).map(|lookup| {
            Box::new(GenericPredicateLookup::from_layer_lookup(lookup)) as Box<dyn PredicateLookup>
        })
    }

    /// Create a struct with all the counts
    fn all_counts(&self) -> LayerCounts;

    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn PredicateLookup>>>;

    fn predicate_additions(&self) -> Box<dyn Iterator<Item = Box<dyn LayerPredicateLookup>>>;

    fn predicate_additions_generic(&self) -> Box<dyn Iterator<Item = Box<dyn PredicateLookup>>> {
        Box::new(self.predicate_additions().map(|lookup| {
            Box::new(GenericPredicateLookup::from_layer_lookup(lookup)) as Box<dyn PredicateLookup>
        }))
    }

    fn predicate_removals(&self) -> Box<dyn Iterator<Item = Box<dyn LayerPredicateLookup>>>;

    fn predicate_removals_generic(&self) -> Box<dyn Iterator<Item = Box<dyn PredicateLookup>>> {
        Box::new(self.predicate_removals().map(|lookup| {
            Box::new(GenericPredicateLookup::from_layer_lookup(lookup)) as Box<dyn PredicateLookup>
        }))
    }

    /// Return a clone of this layer in a box.
    fn clone_boxed(&self) -> Box<dyn Layer>;

    /// Returns true if the given triple exists, and false otherwise.
    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool;

    fn triple_addition_exists(&self, subject: u64, predicate: u64, object: u64) -> bool;
    fn triple_removal_exists(&self, subject: u64, predicate: u64, object: u64) -> bool;

    /// Returns true if the given triple exists, and false otherwise.
    fn id_triple_exists(&self, triple: IdTriple) -> bool {
        self.triple_exists(triple.subject, triple.predicate, triple.object)
    }

    /// Returns true if the given triple exists, and false otherwise.
    fn string_triple_exists(&self, triple: &StringTriple) -> bool {
        self.string_triple_to_id(triple)
            .map(|t| self.id_triple_exists(t))
            .unwrap_or(false)
    }

    fn triple_additions(&self) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triple_removals(&self) -> Box<dyn Iterator<Item = IdTriple>>;

    /// Iterator over all triples known to this layer.
    ///
    /// This is a convenient werapper around
    /// `SubjectLookup` and
    /// `SubjectPredicateLookup` style querying.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>>;

    fn triple_additions_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triple_removals_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple>>;

    fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triples_sp(&self, subject: u64, predicate: u64) -> Box<dyn Iterator<Item = IdTriple>>;

    /// Convert a `StringTriple` to an `IdTriple`, returning None if any of the strings in the triple could not be resolved.
    fn string_triple_to_id(&self, triple: &StringTriple) -> Option<IdTriple> {
        self.subject_id(&triple.subject).and_then(|subject| {
            self.predicate_id(&triple.predicate).and_then(|predicate| {
                match &triple.object {
                    ObjectType::Node(node) => self.object_node_id(&node),
                    ObjectType::Value(value) => self.object_value_id(&value),
                }
                .map(|object| IdTriple {
                    subject,
                    predicate,
                    object,
                })
            })
        })
    }

    fn triple_additions_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triple_removals_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple>>;

    fn triple_additions_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triple_removals_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple>>;
    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple>>;

    /// Convert all known strings in the given string triple to ids.
    fn string_triple_to_partially_resolved(&self, triple: StringTriple) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: self
                .subject_id(&triple.subject)
                .map(|id| PossiblyResolved::Resolved(id))
                .unwrap_or(PossiblyResolved::Unresolved(triple.subject)),
            predicate: self
                .predicate_id(&triple.predicate)
                .map(|id| PossiblyResolved::Resolved(id))
                .unwrap_or(PossiblyResolved::Unresolved(triple.predicate)),
            object: match &triple.object {
                ObjectType::Node(node) => self
                    .object_node_id(&node)
                    .map(|id| PossiblyResolved::Resolved(id))
                    .unwrap_or(PossiblyResolved::Unresolved(triple.object)),
                ObjectType::Value(value) => self
                    .object_value_id(&value)
                    .map(|id| PossiblyResolved::Resolved(id))
                    .unwrap_or(PossiblyResolved::Unresolved(triple.object)),
            },
        }
    }

    /// Convert an id triple to the corresponding string version, returning None if any of those ids could not be converted.
    fn id_triple_to_string(&self, triple: &IdTriple) -> Option<StringTriple> {
        self.id_subject(triple.subject).and_then(|subject| {
            self.id_predicate(triple.predicate).and_then(|predicate| {
                self.id_object(triple.object).map(|object| StringTriple {
                    subject,
                    predicate,
                    object,
                })
            })
        })
    }

    /// Returns the total amount of triple additions in this layer and all its parents.
    fn triple_addition_count(&self) -> usize;

    /// Returns the total amount of triple removals in this layer and all its parents.
    fn triple_removal_count(&self) -> usize;

    /// Returns the total amount of triples in this layer and all its parents.
    fn triple_count(&self) -> usize {
        self.triple_addition_count() - self.triple_removal_count()
    }

    /// Returns the amount of triples that this layer adds.
    fn triple_layer_addition_count(&self) -> usize;
    /// Returns the amount of triples that this layer removes.
    fn triple_layer_removal_count(&self) -> usize;
}

pub struct LayerCounts {
    pub node_count: usize,
    pub predicate_count: usize,
    pub value_count: usize,
}

/// The type of a layer - either base or child.
#[derive(Clone, Copy)]
pub enum LayerType {
    Base,
    Child,
}

pub struct GenericSubjectIterator {
    pub layers: Vec<(
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>>>,
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>>>,
    )>,
}

impl GenericSubjectIterator {
    fn from_layer_iterator(iter: Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>>) -> Self {
        let empty =
            Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Box<dyn LayerSubjectLookup>>>;
        Self {
            layers: vec![(iter.peekable(), empty.peekable())],
        }
    }
}

impl Iterator for GenericSubjectIterator {
    type Item = GenericSubjectLookup;

    fn next(&mut self) -> Option<GenericSubjectLookup> {
        // find the very lowest subject
        let mut min = None;
        for (pos, _neg) in self.layers.iter_mut() {
            let pos_subject = pos.peek().map(|lookup| lookup.subject());

            if pos_subject.is_some() && (min.is_none() || pos_subject < min) {
                min = pos_subject;
            }
        }

        if min.is_none() {
            // there are no more positives left, so we're done
            return None;
        }

        // now collect a vec of lookups
        let min = min.unwrap();
        let lookups = self
            .layers
            .iter_mut()
            .map(|(pos, neg)| {
                let pos_subject = match pos
                    .peek()
                    .map(|lookup| min == lookup.subject())
                    .unwrap_or(false)
                {
                    true => pos.next(),
                    false => None,
                };
                let neg_subject = match neg
                    .peek()
                    .map(|lookup| min == lookup.subject())
                    .unwrap_or(false)
                {
                    true => neg.next(),
                    false => None,
                };

                (pos_subject, neg_subject)
            })
            .filter(|(pos, neg)| pos.is_some() || neg.is_some())
            .collect();

        Some(GenericSubjectLookup {
            subject: min,
            lookups: lookups,
        })
    }
}

/// A trait that caches a lookup in a layer by subject, but only for that layer and not its parents.
///
/// This is returned by `Layer::subjects` and
/// `Layer::lookup_subject`. It stores slices of
/// the relevant data structures to allow quick retrieval of
/// predicate-object pairs when one already knows the subject.
pub trait LayerSubjectLookup {
    /// The subject that this lookup is based on
    fn subject(&self) -> u64;
    /// Returns an iterator over predicate lookups
    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>;
    /// Returns a predicate lookup for the given predicate, or None if no such lookup could be constructed
    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn LayerSubjectPredicateLookup>>;
    /// Returns an iterator over all triples that can be found by this lookup
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(self.predicates().map(|p| p.triples()).flatten())
    }
}

pub fn layer_subject_lookup_into_generic(
    lookup: Box<dyn LayerSubjectLookup>,
) -> Box<dyn SubjectLookup> {
    Box::new(GenericSubjectLookup {
        subject: lookup.subject(),
        lookups: vec![(Some(lookup), None)],
    })
}

/// a trait that caches a lookup in a layer by subject and predicate, but only for that layer and not its parents.
///
/// This is returned by `SubjectLookup::predicates`
/// and `SubjectLookup::lookup_predicate`. It
/// stores slices of the relevant data structures to allow quick
/// retrieval of objects when one already knows the subject and
/// predicate.
pub trait LayerSubjectPredicateLookup {
    /// The subject that this lookup is based on.
    fn subject(&self) -> u64;
    /// The predicate that this lookup is based on.
    fn predicate(&self) -> u64;

    /// Returns an iterator over all objects that can be found by this lookup.
    fn objects(&self) -> Box<dyn Iterator<Item = u64>>;

    /// Returns true if the given object exists, and false otherwise.
    fn has_object(&self, object: u64) -> bool;

    /// Returns an iterator over all triples that can be found by this lookup.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        let subject = self.subject();
        let predicate = self.predicate();
        Box::new(
            self.objects()
                .map(move |o| IdTriple::new(subject, predicate, o)),
        )
    }

    /// Returns a triple for the given object, or None if it doesn't exist.
    fn triple(&self, object: u64) -> Option<IdTriple> {
        if self.has_object(object) {
            Some(IdTriple::new(self.subject(), self.predicate(), object))
        } else {
            None
        }
    }
}

/// A trait that caches a lookup in a layer by subject.
///
/// This is returned by `Layer::subjects` and
/// `Layer::lookup_subject`. It stores slices of
/// the relevant data structures to allow quick retrieval of
/// predicate-object pairs when one already knows the subject.
pub trait SubjectLookup {
    /// The subject that this lookup is based on
    fn subject(&self) -> u64;
    /// Returns an iterator over predicate lookups
    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectPredicateLookup>>>;
    /// Returns a predicate lookup for the given predicate, or None if no such lookup could be constructed
    ///
    /// Note that even when it can be constructed, that doesn't mean
    /// there's any underlying triples. Having ancestor layers with
    /// additions for a given subject and predicate will cause a
    /// lookup to be constructable, but if subsequent layers deleted
    /// all these triples, none will be retrievable.
    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn SubjectPredicateLookup>>;
    /// Returns an iterator over all triples that can be found by this lookup
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(self.predicates().map(|p| p.triples()).flatten())
    }
}

/// A SubjectLookup that is implemented in terms of addition and removal lookups
pub struct GenericSubjectLookup {
    pub subject: u64,
    pub lookups: Vec<(
        Option<Box<dyn LayerSubjectLookup>>,
        Option<Box<dyn LayerSubjectLookup>>,
    )>,
}

impl GenericSubjectLookup {
    fn from_layer_lookup(lookup: Box<dyn LayerSubjectLookup>) -> GenericSubjectLookup {
        GenericSubjectLookup {
            subject: lookup.subject(),
            lookups: vec![(Some(lookup), None)],
        }
    }
}

impl SubjectLookup for GenericSubjectLookup {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicates(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectPredicateLookup>>> {
        let layers = self
            .lookups
            .iter()
            .map(|(pos, neg)| {
                (
                    pos.as_ref()
                        .map(|p| {
                            Box::new(p.predicates())
                                as Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>
                        })
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                    neg.as_ref()
                        .map(|n| {
                            Box::new(n.predicates())
                                as Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>
                        })
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                )
            })
            .collect();

        Box::new(
            GenericSubjectPredicateIterator {
                subject: self.subject,
                layers: layers,
            }
            .map(|lookup| Box::new(lookup) as Box<dyn SubjectPredicateLookup>),
        )
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn SubjectPredicateLookup>> {
        let lookups: Vec<_> = self
            .lookups
            .iter()
            .map(|(pos, neg)| {
                (
                    pos.as_ref().and_then(|p| p.lookup_predicate(predicate)),
                    neg.as_ref().and_then(|n| n.lookup_predicate(predicate)),
                )
            })
            .filter(|(pos, neg)| pos.is_some() || neg.is_some())
            .collect();

        if lookups.iter().find(|(pos, _neg)| pos.is_some()).is_some() {
            Some(Box::new(GenericSubjectPredicateLookup {
                subject: self.subject,
                predicate: predicate,
                lookups: lookups,
            }))
        } else {
            None
        }
    }
}

pub struct GenericSubjectPredicateIterator {
    subject: u64,
    layers: Vec<(
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>>,
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>>,
    )>,
}

impl Iterator for GenericSubjectPredicateIterator {
    type Item = GenericSubjectPredicateLookup;

    fn next(&mut self) -> Option<GenericSubjectPredicateLookup> {
        // find the very lowest predicate
        let mut min = None;
        for (pos, _neg) in self.layers.iter_mut() {
            let pos_predicate = pos.peek().map(|lookup| lookup.predicate());

            if pos_predicate.is_some() && (min.is_none() || pos_predicate < min) {
                min = pos_predicate;
            }
        }

        if min.is_none() {
            // there are no more positives left, so we're done
            return None;
        }

        // now collect a vec of lookups
        let min = min.unwrap();
        let lookups = self
            .layers
            .iter_mut()
            .map(|(pos, neg)| {
                let pos_predicate = match pos
                    .peek()
                    .map(|lookup| min == lookup.predicate())
                    .unwrap_or(false)
                {
                    true => pos.next(),
                    false => None,
                };
                let neg_predicate = match neg
                    .peek()
                    .map(|lookup| min == lookup.predicate())
                    .unwrap_or(false)
                {
                    true => neg.next(),
                    false => None,
                };

                (pos_predicate, neg_predicate)
            })
            .filter(|(pos, neg)| pos.is_some() || neg.is_some())
            .collect();

        Some(GenericSubjectPredicateLookup {
            subject: self.subject,
            predicate: min,
            lookups: lookups,
        })
    }
}

/// a trait that caches a lookup in a layer by subject and predicate.
///
/// This is returned by `SubjectLookup::predicates`
/// and `SubjectLookup::lookup_predicate`. It
/// stores slices of the relevant data structures to allow quick
/// retrieval of objects when one already knows the subject and
/// predicate.
pub trait SubjectPredicateLookup {
    /// The subject that this lookup is based on.
    fn subject(&self) -> u64;
    /// The predicate that this lookup is based on.
    fn predicate(&self) -> u64;

    /// Returns an iterator over all objects that can be found by this lookup.
    fn objects(&self) -> Box<dyn Iterator<Item = u64>>;

    /// Returns true if the given object exists in the additions and false otherwise.
    fn has_pos_object_in_lookup(&self, object: u64) -> bool;

    /// Returns true if the given object exists in the deletions and false otherwise.
    fn has_neg_object_in_lookup(&self, object: u64) -> bool;

    /// Returns true if the given object exists, and false otherwise.
    fn has_object(&self, object: u64) -> bool;

    /// Returns an iterator over all triples that can be found by this lookup.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        let subject = self.subject();
        let predicate = self.predicate();
        Box::new(
            self.objects()
                .map(move |o| IdTriple::new(subject, predicate, o)),
        )
    }

    /// Returns a triple for the given object, or None if it doesn't exist.
    fn triple(&self, object: u64) -> Option<IdTriple> {
        if self.has_object(object) {
            Some(IdTriple::new(self.subject(), self.predicate(), object))
        } else {
            None
        }
    }
}

pub struct GenericSubjectPredicateLookup {
    subject: u64,
    predicate: u64,
    lookups: Vec<(
        Option<Box<dyn LayerSubjectPredicateLookup>>,
        Option<Box<dyn LayerSubjectPredicateLookup>>,
    )>,
}

impl SubjectPredicateLookup for GenericSubjectPredicateLookup {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn objects(&self) -> Box<dyn Iterator<Item = u64>> {
        let layers = self
            .lookups
            .iter()
            .map(|(pos, neg)| {
                (
                    pos.as_ref()
                        .map(|p| Box::new(p.objects()) as Box<dyn Iterator<Item = u64>>)
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                    neg.as_ref()
                        .map(|n| Box::new(n.objects()) as Box<dyn Iterator<Item = u64>>)
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                )
            })
            .collect();

        Box::new(GenericSubjectPredicateObjectIterator { layers })
    }

    fn has_pos_object_in_lookup(&self, object: u64) -> bool {
        self.lookups
            .first()
            .and_then(|last| last.0.as_ref())
            .map(|pos| pos.has_object(object))
            .unwrap_or(false)
    }

    fn has_neg_object_in_lookup(&self, object: u64) -> bool {
        self.lookups
            .first()
            .and_then(|last| last.1.as_ref())
            .map(|pos| pos.has_object(object))
            .unwrap_or(false)
    }

    fn has_object(&self, object: u64) -> bool {
        for (pos, neg) in self.lookups.iter() {
            if pos.as_ref().map(|p| p.has_object(object)).unwrap_or(false) {
                return true;
            }
            if neg.as_ref().map(|p| p.has_object(object)).unwrap_or(false) {
                return false;
            }
        }

        false
    }
}

pub struct GenericSubjectPredicateObjectIterator {
    layers: Vec<(
        Peekable<Box<dyn Iterator<Item = u64>>>,
        Peekable<Box<dyn Iterator<Item = u64>>>,
    )>,
}

impl Iterator for GenericSubjectPredicateObjectIterator {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let mut min;
        loop {
            // what is the lowest number to talk about now?
            min = None;
            let mut deleted = false;
            for (pos, neg) in self.layers.iter_mut().rev() {
                let pos_object = pos.peek().map(|o| *o);
                let neg_object = neg.peek().map(|o| *o);
                if pos_object.is_some() && (min.is_none() || pos_object < min) {
                    deleted = false;
                    min = pos_object;
                } else if deleted && pos_object.is_some() && pos_object == min {
                    deleted = false;
                } else if neg_object == min {
                    deleted = true;
                }
            }

            // advance all iterators until they're either exhausted or beyond the min we found
            // if min is None, we need to exhaust everything
            for (pos, neg) in self.layers.iter_mut() {
                while pos.peek().is_some()
                    && (min.is_none() || pos.peek().map(|o| *o).unwrap() <= min.unwrap())
                {
                    pos.next().unwrap();
                }
                while neg.peek().is_some()
                    && (min.is_none() || neg.peek().map(|o| *o).unwrap() <= min.unwrap())
                {
                    neg.next().unwrap();
                }
            }

            // we advanced all the iterators, but we aren't necessarily done.
            // It could be that the item we intended to return was deleted.
            // In that case we need another retrieval round.
            // If not, or if we ran out of data, we need to return.
            if min.is_none() || !deleted {
                break;
            }
        }
        min
    }
}

/// a trait that caches a lookup by object in a single layer's addition or removals.
pub trait LayerObjectLookup {
    /// The object that this lookup is based on.
    fn object(&self) -> u64;

    /// Returns an iterator over the subject-predicate pairs pointing at this object.
    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item = (u64, u64)>>;

    /// Returns true if the object this lookup is for is connected to the given subject and predicater.
    fn has_subject_predicate_pair(&self, subject: u64, predicate: u64) -> bool {
        for (s, p) in self.subject_predicate_pairs() {
            if s == subject && p == predicate {
                return true;
            }
            if s > subject || (s == subject && p > predicate) {
                // we went past our search, so it's not going to appear anymore
                return false;
            }
        }

        false
    }

    /// Returns the triple consisting of the given subject and predicate, and the object this lookup is for, if it exists. None is returned otherwise.
    fn triple(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        if self.has_subject_predicate_pair(subject, predicate) {
            Some(IdTriple::new(subject, predicate, self.object()))
        } else {
            None
        }
    }

    /// Returns an iterator over all triples with the object of this lookup.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        let object = self.object();
        Box::new(
            self.subject_predicate_pairs()
                .map(move |(s, p)| IdTriple::new(s, p, object)),
        )
    }
}

/// a trait that caches a lookup in a layer by object.
pub trait ObjectLookup {
    /// The object that this lookup is based on.
    fn object(&self) -> u64;

    /// Returns an iterator over the subject-predicate pairs pointing at this object.
    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item = (u64, u64)>>;

    /// Returns true if the object this lookup is for is connected to the given subject and predicater.
    fn has_subject_predicate_pair(&self, subject: u64, predicate: u64) -> bool {
        for (s, p) in self.subject_predicate_pairs() {
            if s == subject && p == predicate {
                return true;
            }
            if s > subject || (s == subject && p > predicate) {
                // we went past our search, so it's not going to appear anymore
                return false;
            }
        }

        false
    }

    /// Returns the triple consisting of the given subject and predicate, and the object this lookup is for, if it exists. None is returned otherwise.
    fn triple(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        if self.has_subject_predicate_pair(subject, predicate) {
            Some(IdTriple::new(subject, predicate, self.object()))
        } else {
            None
        }
    }

    /// Returns an iterator over all triples with the object of this lookup.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        let object = self.object();
        Box::new(
            self.subject_predicate_pairs()
                .map(move |(s, p)| IdTriple::new(s, p, object)),
        )
    }
}

pub struct GenericObjectIterator {
    pub layers: Vec<(
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>>>,
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>>>,
    )>,
}

impl GenericObjectIterator {
    fn from_layer_iterator(iter: Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>>) -> Self {
        let empty =
            Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Box<dyn LayerObjectLookup>>>;
        GenericObjectIterator {
            layers: vec![(iter.peekable(), empty.peekable())],
        }
    }
}

impl Iterator for GenericObjectIterator {
    type Item = GenericObjectLookup;

    fn next(&mut self) -> Option<GenericObjectLookup> {
        let mut min = None;
        for (pos, _neg) in self.layers.iter_mut() {
            let pos_object = pos.peek().map(|lookup| lookup.object());

            if pos_object.is_some() && (min.is_none() || pos_object < min) {
                min = pos_object;
            }
        }

        if min.is_none() {
            // there are no more positives left, so we're done
            return None;
        }

        // now collect a vec of lookups
        let min = min.unwrap();
        let lookups = self
            .layers
            .iter_mut()
            .map(|(pos, neg)| {
                let pos_object = match pos
                    .peek()
                    .map(|lookup| min == lookup.object())
                    .unwrap_or(false)
                {
                    true => pos.next(),
                    false => None,
                };
                let neg_object = match neg
                    .peek()
                    .map(|lookup| min == lookup.object())
                    .unwrap_or(false)
                {
                    true => neg.next(),
                    false => None,
                };

                (pos_object, neg_object)
            })
            .filter(|(pos, neg)| pos.is_some() || neg.is_some())
            .collect();

        Some(GenericObjectLookup {
            object: min,
            lookups: lookups,
        })
    }
}

pub struct GenericObjectLookup {
    pub object: u64,
    pub lookups: Vec<(
        Option<Box<dyn LayerObjectLookup>>,
        Option<Box<dyn LayerObjectLookup>>,
    )>,
}

impl GenericObjectLookup {
    fn from_layer_lookup(lookup: Box<dyn LayerObjectLookup>) -> Self {
        Self {
            object: lookup.object(),
            lookups: vec![(Some(lookup), None)],
        }
    }
}

impl ObjectLookup for GenericObjectLookup {
    fn object(&self) -> u64 {
        self.object
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item = (u64, u64)>> {
        let layers: Vec<_> = self
            .lookups
            .iter()
            .map(|(pos, neg)| {
                (
                    pos.as_ref()
                        .map(|p| p.subject_predicate_pairs())
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                    neg.as_ref()
                        .map(|p| p.subject_predicate_pairs())
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                )
            })
            .collect();

        Box::new(ObjectSubjectPredicatePairIterator { layers })
    }
}

pub struct ObjectSubjectPredicatePairIterator {
    layers: Vec<(
        Peekable<Box<dyn Iterator<Item = (u64, u64)>>>,
        Peekable<Box<dyn Iterator<Item = (u64, u64)>>>,
    )>,
}

impl Iterator for ObjectSubjectPredicatePairIterator {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<(u64, u64)> {
        let mut min;
        loop {
            min = None;
            let mut deleted = false;
            for (pos, neg) in self.layers.iter_mut().rev() {
                let pos_sp = pos.peek().map(|s| *s);
                let neg_sp = neg.peek().map(|s| *s);
                if pos_sp.is_some() && (min.is_none() || pos_sp < min) {
                    deleted = false;
                    min = pos_sp;
                } else if deleted && pos_sp.is_some() && pos_sp == min {
                    deleted = false;
                } else if neg_sp == min {
                    deleted = true;
                }
            }

            // advance all iterators until they're either exhausted or beyond the min we found
            // if min is None, we need to exhaust everything
            for (pos, neg) in self.layers.iter_mut() {
                while pos.peek().is_some()
                    && (min.is_none() || pos.peek().map(|s| *s).unwrap() <= min.unwrap())
                {
                    pos.next().unwrap();
                }
                while neg.peek().is_some()
                    && (min.is_none() || neg.peek().map(|s| *s).unwrap() <= min.unwrap())
                {
                    neg.next().unwrap();
                }
            }

            // we advanced all the iterators, but we aren't necessarily done.
            // It could be that the item we intended to return was deleted.
            // In that case we need another retrieval round.
            // If not, or if we ran out of data, we need to return.
            if min.is_none() || !deleted {
                break;
            }
        }

        min
    }
}

pub trait LayerPredicateLookup {
    fn predicate(&self) -> u64;
    fn subject_predicate_pairs(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>;

    /// Returns an iterator over all triples with the object of this lookup.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            self.subject_predicate_pairs()
                .map(move |sp| sp.triples())
                .flatten(),
        )
    }
}

/// A trait that caches a lookup in a layer by predicate.
pub trait PredicateLookup {
    fn predicate(&self) -> u64;
    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectPredicateLookup>>>;

    /// Returns an iterator over all triples with the object of this lookup.
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple>> {
        Box::new(
            self.subject_predicate_pairs()
                .map(move |sp| sp.triples())
                .flatten(),
        )
    }
}

pub struct GenericPredicateLookup {
    pub predicate: u64,
    pub lookups: Vec<(
        Option<Box<dyn LayerPredicateLookup>>,
        Option<Box<dyn LayerPredicateLookup>>,
    )>,
}

impl GenericPredicateLookup {
    fn from_layer_lookup(lookup: Box<dyn LayerPredicateLookup>) -> Self {
        Self {
            predicate: lookup.predicate(),
            lookups: vec![(Some(lookup), None)],
        }
    }
}

impl PredicateLookup for GenericPredicateLookup {
    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item = Box<dyn SubjectPredicateLookup>>> {
        let layers: Vec<_> = self
            .lookups
            .iter()
            .map(|(pos, neg)| {
                (
                    pos.as_ref()
                        .map(|p| p.subject_predicate_pairs())
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                    neg.as_ref()
                        .map(|n| n.subject_predicate_pairs())
                        .unwrap_or(Box::new(std::iter::empty()))
                        .peekable(),
                )
            })
            .collect();

        Box::new(
            GenericSubjectPredicatePairIterator {
                predicate: self.predicate,
                layers: layers,
            }
            .map(|l| Box::new(l) as Box<dyn SubjectPredicateLookup>),
        )
    }
}

pub struct GenericSubjectPredicatePairIterator {
    predicate: u64,
    layers: Vec<(
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>>,
        Peekable<Box<dyn Iterator<Item = Box<dyn LayerSubjectPredicateLookup>>>>,
    )>,
}

impl Iterator for GenericSubjectPredicatePairIterator {
    type Item = GenericSubjectPredicateLookup;

    fn next(&mut self) -> Option<GenericSubjectPredicateLookup> {
        let mut min = None;
        for (pos, _neg) in self.layers.iter_mut() {
            let subject = pos.peek().map(|l| l.subject());
            if subject.is_some() && (min.is_none() || subject < min) {
                min = subject;
            }
        }

        if min.is_none() {
            return None;
        }

        let mut lookups = Vec::with_capacity(self.layers.len());

        for (pos, neg) in self.layers.iter_mut() {
            let mut pos_lookup = None;
            if pos.peek().map(|l| l.subject()) == min {
                pos_lookup = pos.next();
            }
            let mut neg_lookup = None;
            if neg.peek().map(|l| l.subject()) == min {
                neg_lookup = neg.next();
            }

            if pos_lookup.is_some() || neg_lookup.is_some() {
                lookups.push((pos_lookup, neg_lookup));
            }
        }

        Some(GenericSubjectPredicateLookup {
            subject: min.unwrap(),
            predicate: self.predicate,
            lookups: lookups,
        })
    }
}

/// A triple, stored as numerical ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
}

impl IdTriple {
    /// Construct a new id triple.
    pub fn new(subject: u64, predicate: u64, object: u64) -> Self {
        IdTriple {
            subject,
            predicate,
            object,
        }
    }

    /// convert this triple into a `PartiallyResolvedTriple`, which is a data structure used in layer building.
    pub fn to_resolved(&self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Resolved(self.subject),
            predicate: PossiblyResolved::Resolved(self.predicate),
            object: PossiblyResolved::Resolved(self.object),
        }
    }
}

/// A triple stored as strings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StringTriple {
    pub subject: String,
    pub predicate: String,
    pub object: ObjectType,
}

impl StringTriple {
    /// Construct a triple with a node object.
    ///
    /// Nodes may appear in both the subject and object position.
    pub fn new_node(subject: &str, predicate: &str, object: &str) -> StringTriple {
        StringTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Node(object.to_owned()),
        }
    }

    /// Construct a triple with a value object.
    ///
    /// Values may only appear in the object position.
    pub fn new_value(subject: &str, predicate: &str, object: &str) -> StringTriple {
        StringTriple {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: ObjectType::Value(object.to_owned()),
        }
    }

    /// Convert this triple to a `PartiallyResolvedTriple`, marking each field as unresolved.
    pub fn to_unresolved(self) -> PartiallyResolvedTriple {
        PartiallyResolvedTriple {
            subject: PossiblyResolved::Unresolved(self.subject),
            predicate: PossiblyResolved::Unresolved(self.predicate),
            object: PossiblyResolved::Unresolved(self.object),
        }
    }
}

/// Either a resolved id or an unresolved inner type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PossiblyResolved<T: Clone + PartialEq + Eq + PartialOrd + Ord + Hash> {
    Unresolved(T),
    Resolved(u64),
}

impl<T: Clone + PartialEq + Eq + PartialOrd + Ord + Hash> PossiblyResolved<T> {
    /// Returns true if this is a resolved id, and false otherwise.
    pub fn is_resolved(&self) -> bool {
        match self {
            Self::Unresolved(_) => false,
            Self::Resolved(_) => true,
        }
    }

    /// Return a PossiblyResolved with the inner value as a reference.
    pub fn as_ref(&self) -> PossiblyResolved<&T> {
        match self {
            Self::Unresolved(u) => PossiblyResolved::Unresolved(&u),
            Self::Resolved(id) => PossiblyResolved::Resolved(*id),
        }
    }

    /// Unwrap to the unresolved inner value, or panic if this was actually a resolved id.
    pub fn unwrap_unresolved(self) -> T {
        match self {
            Self::Unresolved(u) => u,
            Self::Resolved(_) => panic!("tried to unwrap unresolved, but got a resolved"),
        }
    }

    /// Unwrap to the resolved id, or panic if this was actually an unresolved value.
    pub fn unwrap_resolved(self) -> u64 {
        match self {
            Self::Unresolved(_) => panic!("tried to unwrap resolved, but got an unresolved"),
            Self::Resolved(id) => id,
        }
    }
}

/// A triple where the subject, predicate and object can all either be fully resolved to an id, or unresolved.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartiallyResolvedTriple {
    pub subject: PossiblyResolved<String>,
    pub predicate: PossiblyResolved<String>,
    pub object: PossiblyResolved<ObjectType>,
}

impl PartiallyResolvedTriple {
    /// Resolve the unresolved ids in this triple using the given hashmaps for nodes, predicates and values.
    pub fn resolve_with(
        &self,
        node_map: &HashMap<String, u64>,
        predicate_map: &HashMap<String, u64>,
        value_map: &HashMap<String, u64>,
    ) -> Option<IdTriple> {
        let subject = match self.subject.as_ref() {
            PossiblyResolved::Unresolved(s) => *node_map.get(s)?,
            PossiblyResolved::Resolved(id) => id,
        };
        let predicate = match self.predicate.as_ref() {
            PossiblyResolved::Unresolved(p) => *predicate_map.get(p)?,
            PossiblyResolved::Resolved(id) => id,
        };
        let object = match self.object.as_ref() {
            PossiblyResolved::Unresolved(ObjectType::Node(n)) => *node_map.get(n)?,
            PossiblyResolved::Unresolved(ObjectType::Value(v)) => *value_map.get(v)?,
            PossiblyResolved::Resolved(id) => id,
        };

        Some(IdTriple {
            subject,
            predicate,
            object,
        })
    }

    pub fn as_resolved(&self) -> Option<IdTriple> {
        if !self.subject.is_resolved()
            || !self.predicate.is_resolved()
            || !self.object.is_resolved()
        {
            None
        } else {
            Some(IdTriple::new(
                self.subject.as_ref().unwrap_resolved(),
                self.predicate.as_ref().unwrap_resolved(),
                self.object.as_ref().unwrap_resolved(),
            ))
        }
    }
}

/// The type of an object in a triple.
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
    Value(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::base::tests::base_layer_files;
    use crate::layer::base::BaseLayer;
    use crate::layer::child::tests::child_layer_files;
    use crate::layer::child::ChildLayer;
    use crate::layer::internal::InternalLayer;
    use crate::layer::simple_builder::{LayerBuilder, SimpleLayerBuilder};
    use futures::prelude::*;
    use futures::sync::oneshot;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[test]
    fn find_triple_after_adjacent_removal() {
        let runtime = Runtime::new().unwrap();
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("cow", "says", "sniff"));

        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .wait()
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));
        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base.clone(), &files)
                .wait()
                .unwrap()
                .into(),
        );

        let base_triples_additions: Vec<_> = base
            .triple_additions()
            //.map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        println!("base triple additions: {:?}", base_triples_additions);

        let _triples_additions: Vec<_> = child
            .triple_additions()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        let _triples_removals: Vec<_> = child
            .triple_removals()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        let triples: Vec<_> = child
            .triples()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            vec![StringTriple::new_value("cow", "says", "sniff")],
            triples
        );
    }

    #[test]
    fn find_triple_after_removal_and_readdition() {
        let runtime = Runtime::new().unwrap();
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));

        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .wait()
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));
        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base, &files)
                .wait()
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 2], child.clone(), files.clone());
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 2], child, &files)
                .wait()
                .unwrap()
                .into(),
        );

        let triples: Vec<_> = child
            .triples()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(vec![StringTriple::new_value("cow", "says", "moo")], triples);
    }

    #[test]
    fn find_triple_by_object_after_adjacent_removal() {
        let runtime = Runtime::new().unwrap();
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_string_triple(StringTriple::new_value("cow", "hears", "moo"));
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));

        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .wait()
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_string_triple(StringTriple::new_value("cow", "hears", "moo"));
        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let child = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base, &files)
                .wait()
                .unwrap(),
        ) as Arc<dyn Layer>;

        let triples: Vec<_> = child
            .objects()
            .map(|o| o.triples())
            .flatten()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(vec![StringTriple::new_value("cow", "says", "moo")], triples);
    }

    #[test]
    fn find_triple_by_object_after_removal_and_readdition() {
        let runtime = Runtime::new().unwrap();
        let files = base_layer_files();
        let mut builder = SimpleLayerBuilder::new([1, 2, 3, 4, 5], files.clone());

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));

        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let base: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
                .wait()
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 1], base.clone(), files.clone());
        builder.remove_string_triple(StringTriple::new_value("cow", "says", "moo"));
        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 1], base, &files)
                .wait()
                .unwrap()
                .into(),
        );

        let files = child_layer_files();
        let mut builder =
            SimpleLayerBuilder::from_parent([5, 4, 3, 2, 2], child.clone(), files.clone());
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        oneshot::spawn(builder.commit(), &runtime.executor())
            .wait()
            .unwrap();

        let child: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([5, 4, 3, 2, 2], child, &files)
                .wait()
                .unwrap()
                .into(),
        );

        let triples: Vec<_> = child
            .objects()
            .map(|o| o.triples())
            .flatten()
            .map(|t| child.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(vec![StringTriple::new_value("cow", "says", "moo")], triples);
    }
}
