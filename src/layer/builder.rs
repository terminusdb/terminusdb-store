use super::layer::*;
use super::base::*;
use super::child::*;
use crate::structure::storage::*;
use futures::stream;
use futures::prelude::*;
use std::collections::{HashMap,BTreeSet};

#[derive(Clone)]
struct SimpleLayerBuilder<F:'static+FileLoad+FileStore+Clone> {
    parent: Option<GenericLayer<F::Map>>,
    files: LayerFiles<F>,
    additions: BTreeSet<PartiallyResolvedTriple>,
    removals: BTreeSet<IdTriple>, // always resolved!
}

impl<F:'static+FileLoad+FileStore+Clone> SimpleLayerBuilder<F> {
    pub fn new(files: BaseLayerFiles<F>) -> Self {
        Self {
            parent: None,
            files: LayerFiles::Base(files),
            additions: BTreeSet::new(),
            removals: BTreeSet::new()
        }
    }

    pub fn from_parent(files: ChildLayerFiles<F>, parent: GenericLayer<F::Map>) -> Self {
        Self {
            parent: Some(parent),
            files: LayerFiles::Child(files),
            additions: BTreeSet::new(),
            removals: BTreeSet::new()
        }
    }

    pub fn add_string_triple(&mut self, triple: &StringTriple) {
        if self.parent.is_some() {
            self.additions.insert(self.parent.as_ref().unwrap().string_triple_to_partially_resolved(triple));
        }
        else {
            self.additions.insert(triple.to_unresolved());
        }
    }

    pub fn add_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        if self.parent.as_mut()
            .map(|parent|
                 !parent.id_triple_exists(triple)
                 && parent.id_subject(triple.subject).is_some()
                 && parent.id_predicate(triple.predicate).is_some()
                 && parent.id_object(triple.object).is_some())
            .unwrap_or(false) {
                self.additions.insert(triple.to_resolved());
                Some(())
            }
        else {
            None
        }
    }

    pub fn remove_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        if self.parent.is_none() {
            return None;
        }

        let parent = self.parent.as_ref().unwrap();

        if parent.id_triple_exists(triple) {
            self.removals.insert(triple);

            Some(())
        }
        else {
            None
        }
    }

    pub fn remove_string_triple(&mut self, triple: StringTriple) -> Option<()> {
        self.parent.as_ref().and_then(|p|p.string_triple_to_id(&triple))
            .and_then(|t| self.remove_id_triple(t))
    }

    fn unresolved_strings(&self) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut node_builder:BTreeSet<String> = BTreeSet::new();
        let mut predicate_builder:BTreeSet<String> = BTreeSet::new();
        let mut value_builder:BTreeSet<String> = BTreeSet::new();
        for PartiallyResolvedTriple {subject, predicate, object} in self.additions.iter() {
            // todo - should only copy the string if we actually need to insert it
            if !subject.is_resolved() {
                let unresolved = subject.as_ref().unwrap_unresolved();
                node_builder.insert(unresolved.to_owned());
            }
            if !predicate.is_resolved() {
                let unresolved = predicate.as_ref().unwrap_unresolved();
                predicate_builder.insert(unresolved.to_owned());
            }
            if !object.is_resolved() {
                let unresolved = object.as_ref().unwrap_unresolved();
                match unresolved {
                    ObjectType::Node(node) => node_builder.insert(node.to_owned()),
                    ObjectType::Value(value) => value_builder.insert(value.to_owned())
                };
            }
        }

        (node_builder.into_iter().collect(),
         predicate_builder.into_iter().collect(),
         value_builder.into_iter().collect())

    }

    pub fn finalize(self) -> Box<dyn Future<Item=(), Error=std::io::Error>> {
        let (unresolved_nodes, unresolved_predicates, unresolved_values) = self.unresolved_strings();
        let additions = self.additions;
        let removals = self.removals;
        // store a copy. The original will be used to build the dictionaries.
        // The copy will be used later on to map unresolved strings to their id's before inserting
        let unresolved_nodes2 = unresolved_nodes.clone();
        let unresolved_predicates2 = unresolved_predicates.clone();
        let unresolved_values2 = unresolved_values.clone();
        match self.parent {
            Some(parent) => {
                let files = self.files.into_child();
                let builder = ChildLayerFileBuilder::from_files(parent, &files);
                
                Box::new(builder.add_nodes(unresolved_nodes)
                         .and_then(|(nodes,b)|b.add_predicates(unresolved_predicates)
                                   .and_then(|(predicates,b)|b.add_values(unresolved_values)
                                             .and_then(|(values, b)| b.into_phase2()
                                                       .map(move |b| (b, nodes, predicates, values)))))
                         .and_then(move |(builder, node_ids, predicate_ids, value_ids)| {
                             let mut node_map = HashMap::new();
                             for (node,id) in unresolved_nodes2.into_iter().zip(node_ids) {
                                 node_map.insert(node,id);
                             }
                             let mut predicate_map = HashMap::new();
                             for (predicate,id) in unresolved_predicates2.into_iter().zip(predicate_ids) {
                                 predicate_map.insert(predicate,id);
                             }
                             let mut value_map = HashMap::new();
                             for (value,id) in unresolved_values2.into_iter().zip(value_ids) {
                                 value_map.insert(value,id);
                             }

                             let triples: Vec<_> = additions.into_iter().map(|t|t.resolve_with(&node_map, &predicate_map, &value_map).expect("triple should have been resolvable")).collect();

                             builder.add_id_triples(triples)
                                 .and_then(|b| b.finalize())
                         }))
            },
            None => {
                let files = self.files.into_base();
                let builder = BaseLayerFileBuilder::from_files(&files);

                // TODO - this is exactly the same as above. We should generalize builder and run it once on the generalized instead.
                Box::new(builder.add_nodes(unresolved_nodes)
                         .and_then(|(nodes,b)|b.add_predicates(unresolved_predicates)
                                   .and_then(|(predicates,b)|b.add_values(unresolved_values)
                                             .and_then(|(values, b)| b.into_phase2()
                                                       .map(move |b| (b, nodes, predicates, values)))))
                         .and_then(move |(builder, node_ids, predicate_ids, value_ids)| {
                             let mut node_map = HashMap::new();
                             for (node,id) in unresolved_nodes2.into_iter().zip(node_ids) {
                                 node_map.insert(node,id);
                             }
                             let mut predicate_map = HashMap::new();
                             for (predicate,id) in unresolved_predicates2.into_iter().zip(predicate_ids) {
                                 predicate_map.insert(predicate,id);
                             }
                             let mut value_map = HashMap::new();
                             for (value,id) in unresolved_values2.into_iter().zip(value_ids) {
                                 value_map.insert(value,id);
                             }

                             let triples: Vec<_> = additions.into_iter().map(|t|t.resolve_with(&node_map, &predicate_map, &value_map).expect("triple should have been resolvable")).collect();

                             builder.add_id_triples(triples)
                                 .and_then(|b| b.finalize())
                         }))
            }
        }
    }
}

#[derive(Clone)]
pub enum LayerFiles<F:FileLoad+FileStore+Clone> {
    Base(BaseLayerFiles<F>),
    Child(ChildLayerFiles<F>)
}

impl<F:FileLoad+FileStore+Clone> LayerFiles<F> {
    fn into_base(self) -> BaseLayerFiles<F> {
        match self {
            Self::Base(b) => b,
            _ => panic!("layer files are not for base")
        }
    }

    fn into_child(self) -> ChildLayerFiles<F> {
        match self {
            Self::Child(c) => c,
            _ => panic!("layer files are not for child")
        }
    }
}
