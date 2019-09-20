use super::layer::*;
use super::base::*;
use super::child::*;
use crate::storage::file::*;
use futures::prelude::*;
use std::collections::{HashMap,BTreeSet};

#[derive(Clone)]
pub struct SimpleLayerBuilder<F:'static+FileLoad+FileStore+Clone> {
    name: [u32;5],
    parent: Option<GenericLayer<F::Map>>,
    files: LayerFiles<F>,
    additions: BTreeSet<PartiallyResolvedTriple>,
    removals: BTreeSet<IdTriple>, // always resolved!
}

impl<F:'static+FileLoad+FileStore+Clone> SimpleLayerBuilder<F> {
    pub fn new(name: [u32;5], files: BaseLayerFiles<F>) -> Self {
        Self {
            name,
            parent: None,
            files: LayerFiles::Base(files),
            additions: BTreeSet::new(),
            removals: BTreeSet::new()
        }
    }

    pub fn from_parent(name: [u32;5], parent: GenericLayer<F::Map>, files: ChildLayerFiles<F>) -> Self {
        Self {
            name,
            parent: Some(parent),
            files: LayerFiles::Child(files),
            additions: BTreeSet::new(),
            removals: BTreeSet::new()
        }
    }

    pub fn name(&self) -> [u32;5] {
        self.name
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

    pub fn remove_string_triple(&mut self, triple: &StringTriple) -> Option<()> {
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

    pub fn finalize(self) -> Box<dyn Future<Item=GenericLayer<F::Map>, Error=std::io::Error>> {
        let (unresolved_nodes, unresolved_predicates, unresolved_values) = self.unresolved_strings();
        let name = self.name;
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
                let builder = ChildLayerFileBuilder::from_files(parent.clone(), &files);
                
                Box::new(builder.add_nodes(unresolved_nodes)
                         .and_then(|(nodes,b)|b.add_predicates(unresolved_predicates)
                                   .and_then(|(predicates,b)|b.add_values(unresolved_values)
                                             .and_then(|(values, b)| b.into_phase2()
                                                       .map(move |b| (b, nodes, predicates, values)))))
                         .and_then(move |(builder, node_ids, predicate_ids, value_ids)| {
                             let parent_node_offset = parent.node_and_value_count() as u64;
                             let parent_predicate_offset = parent.predicate_count() as u64;
                             let mut node_map = HashMap::new();
                             for (node,id) in unresolved_nodes2.into_iter().zip(node_ids) {
                                 node_map.insert(node,id+parent_node_offset);
                             }
                             let mut predicate_map = HashMap::new();
                             for (predicate,id) in unresolved_predicates2.into_iter().zip(predicate_ids) {
                                 predicate_map.insert(predicate,id+parent_predicate_offset);
                             }
                             let mut value_map = HashMap::new();
                             for (value,id) in unresolved_values2.into_iter().zip(value_ids) {
                                 value_map.insert(value,id+parent_node_offset+node_map.len() as u64);
                             }

                             let mut add_triples: Vec<_> = additions.into_iter().map(|t|t.resolve_with(&node_map, &predicate_map, &value_map).expect("triple should have been resolvable")).collect();
                             add_triples.sort();
                             let remove_triples: Vec<_> = removals.into_iter().collect(); // comes out of a btreeset, so sorted

                             builder.add_id_triples(add_triples)
                                 .and_then(move |b| b.remove_id_triples(remove_triples))
                                 .and_then(|b| b.finalize())
                                 .and_then(move |_| ChildLayer::load_from_files(name, parent, &files))
                                 .map(|layer| GenericLayer::Child(layer))
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
                                 value_map.insert(value,id + node_map.len() as u64);
                             }

                             let mut triples: Vec<_> = additions.into_iter().map(|t|t.resolve_with(&node_map, &predicate_map, &value_map).expect("triple should have been resolvable")).collect();
                             triples.sort();

                             builder.add_id_triples(triples)
                                 .and_then(|b| b.finalize())
                                 .and_then(move |_| BaseLayer::load_from_files(name, &files))
                                 .map(|layer| GenericLayer::Base(layer))
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
    pub fn into_base(self) -> BaseLayerFiles<F> {
        match self {
            Self::Base(b) => b,
            _ => panic!("layer files are not for base")
        }
    }

    pub fn into_child(self) -> ChildLayerFiles<F> {
        match self {
            Self::Child(c) => c,
            _ => panic!("layer files are not for child")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn new_base_files() -> BaseLayerFiles<MemoryBackedStore> {
        let files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        BaseLayerFiles {
            node_dictionary_blocks_file: files[0].clone(),
            node_dictionary_offsets_file: files[1].clone(),

            predicate_dictionary_blocks_file: files[2].clone(),
            predicate_dictionary_offsets_file: files[3].clone(),

            value_dictionary_blocks_file: files[4].clone(),
            value_dictionary_offsets_file: files[5].clone(),

            s_p_adjacency_list_bits_file: files[6].clone(),
            s_p_adjacency_list_blocks_file: files[7].clone(),
            s_p_adjacency_list_sblocks_file: files[8].clone(),
            s_p_adjacency_list_nums_file: files[9].clone(),

            sp_o_adjacency_list_bits_file: files[10].clone(),
            sp_o_adjacency_list_blocks_file: files[11].clone(),
            sp_o_adjacency_list_sblocks_file: files[12].clone(),
            sp_o_adjacency_list_nums_file: files[13].clone()
        }
    }

    fn new_child_files() -> ChildLayerFiles<MemoryBackedStore> {
        let files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();
        ChildLayerFiles {
            node_dictionary_blocks_file: files[0].clone(),
            node_dictionary_offsets_file: files[1].clone(),

            predicate_dictionary_blocks_file: files[2].clone(),
            predicate_dictionary_offsets_file: files[3].clone(),

            value_dictionary_blocks_file: files[4].clone(),
            value_dictionary_offsets_file: files[5].clone(),

            pos_subjects_file: files[6].clone(),
            neg_subjects_file: files[7].clone(),

            pos_s_p_adjacency_list_bits_file: files[8].clone(),
            pos_s_p_adjacency_list_blocks_file: files[9].clone(),
            pos_s_p_adjacency_list_sblocks_file: files[10].clone(),
            pos_s_p_adjacency_list_nums_file: files[11].clone(),

            pos_sp_o_adjacency_list_bits_file: files[12].clone(),
            pos_sp_o_adjacency_list_blocks_file: files[13].clone(),
            pos_sp_o_adjacency_list_sblocks_file: files[14].clone(),
            pos_sp_o_adjacency_list_nums_file: files[15].clone(),

            neg_s_p_adjacency_list_bits_file: files[16].clone(),
            neg_s_p_adjacency_list_blocks_file: files[17].clone(),
            neg_s_p_adjacency_list_sblocks_file: files[18].clone(),
            neg_s_p_adjacency_list_nums_file: files[19].clone(),

            neg_sp_o_adjacency_list_bits_file: files[20].clone(),
            neg_sp_o_adjacency_list_blocks_file: files[21].clone(),
            neg_sp_o_adjacency_list_sblocks_file: files[22].clone(),
            neg_sp_o_adjacency_list_nums_file: files[23].clone(),
        }
    }

    fn example_base_layer() -> GenericLayer<<MemoryBackedStore as FileLoad>::Map> {
        let files = new_base_files();
        let mut builder = SimpleLayerBuilder::new([1,2,3,4,5],files.clone());

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.finalize().wait().unwrap()
    }

    #[test]
    fn simple_base_layer_construction() {
        let layer = example_base_layer();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[test]
    fn simple_child_layer_construction() {
        let base_layer = example_base_layer();
        let files = new_child_files();
        let mut builder = SimpleLayerBuilder::from_parent([0,0,0,0,0],base_layer, files);

        builder.add_string_triple(&StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(&StringTriple::new_node("horse", "likes", "cow"));
        builder.remove_string_triple(&StringTriple::new_value("duck", "says", "quack"));

        let child_layer = builder.finalize().wait().unwrap();
        assert!(child_layer.string_triple_exists(&StringTriple::new_value("horse", "says", "neigh")));
        assert!(child_layer.string_triple_exists(&StringTriple::new_node("horse", "likes", "cow")));
        assert!(child_layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(child_layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(!child_layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[test]
    fn multi_level_layers() {
        let base_layer = example_base_layer();
        let mut builder = SimpleLayerBuilder::from_parent([0,0,0,0,0],base_layer, new_child_files());

        builder.add_string_triple(&StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(&StringTriple::new_node("horse", "likes", "cow"));
        builder.remove_string_triple(&StringTriple::new_value("duck", "says", "quack"));

        let layer2 = builder.finalize().wait().unwrap();

        builder = SimpleLayerBuilder::from_parent([0,0,0,0,1], layer2, new_child_files());
        builder.remove_string_triple(&StringTriple::new_node("horse", "likes", "cow"));
        builder.add_string_triple(&StringTriple::new_node("horse", "likes", "pig"));
        builder.add_string_triple(&StringTriple::new_value("duck", "says", "quack"));

        let layer3 = builder.finalize().wait().unwrap();

        builder = SimpleLayerBuilder::from_parent([0,0,0,0,2], layer3, new_child_files());
        builder.remove_string_triple(&StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(&StringTriple::new_node("cow", "likes", "horse"));
        let layer4 = builder.finalize().wait().unwrap();

        assert!(layer4.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer4.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
        assert!(layer4.string_triple_exists(&StringTriple::new_value("horse", "says", "neigh")));
        assert!(layer4.string_triple_exists(&StringTriple::new_node("horse", "likes", "pig")));
        assert!(layer4.string_triple_exists(&StringTriple::new_node("cow", "likes", "horse")));

        assert!(!layer4.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(!layer4.string_triple_exists(&StringTriple::new_node("horse", "likes", "cow")));
    }
}
