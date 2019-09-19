use crate::layer::layer::GenericLayer;
use crate::layer::builder::{SimpleLayerBuilder, LayerFiles};
use crate::layer::base::{BaseLayer,BaseLayerFiles};
use crate::layer::child::{ChildLayer,ChildLayerFiles};
use super::file::*;

use std::collections::HashMap;
use rand;

pub trait LayerStore {
    type File: FileLoad+FileStore+Clone;
    fn layers(&self) -> Vec<[u32;5]>;
    fn create_base_layer(&mut self) -> SimpleLayerBuilder<Self::File>;
    fn create_child_layer(&mut self, parent: [u32;5]) -> Option<SimpleLayerBuilder<Self::File>>;
    fn get_layer(&self, name: [u32;5]) -> Option<GenericLayer<<Self::File as FileLoad>::Map>>;
}

pub struct MemoryLayerStore {
    layers: HashMap<[u32;5],(Option<[u32;5]>,LayerFiles<MemoryBackedStore>)>
}

impl MemoryLayerStore {
    pub fn new() -> MemoryLayerStore {
        MemoryLayerStore {
            layers: HashMap::new()
        }
    }
}

impl LayerStore for MemoryLayerStore {
    type File = MemoryBackedStore;

    fn layers(&self) -> Vec<[u32;5]> {
        self.layers.keys().map(|k|k.clone()).collect()
    }

    fn create_base_layer(&mut self) -> SimpleLayerBuilder<MemoryBackedStore> {
        let name = rand::random();

        let files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let blf = BaseLayerFiles {
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
        };

        self.layers.insert(name, (None, LayerFiles::Base(blf.clone())));

        SimpleLayerBuilder::new(name, blf)
    }

    fn create_child_layer(&mut self, parent: [u32;5]) -> Option<SimpleLayerBuilder<MemoryBackedStore>> {
        if let Some(parent_layer) = self.get_layer(parent) {
            let name = rand::random();
            let files: Vec<_> = (0..24).map(|_| MemoryBackedStore::new()).collect();
            
            let clf = ChildLayerFiles {
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
            };

            self.layers.insert(name, (Some(parent), LayerFiles::Child(clf.clone())));

            Some(SimpleLayerBuilder::from_parent(name, parent_layer, clf))
        }
        else {
            None
        }
    }

    fn get_layer(&self, name: [u32;5]) -> Option<GenericLayer<<MemoryBackedStore as FileLoad>::Map>> {
        self.layers.get(&name)
            .map(|(parent_name, files)| {
                if parent_name.is_some() {
                    let parent = self.get_layer(parent_name.unwrap()).expect("expected parent layer to exist");
                    let layer = ChildLayer::load_from_files(name, parent, &files.clone().into_child());

                    GenericLayer::Child(layer)
                }
                else {
                    let layer = BaseLayer::load_from_files(name, &files.clone().into_base());

                    GenericLayer::Base(layer)
                }
            })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::layer::layer::*;
    use futures::prelude::*;
    
    #[test]
    fn create_layers_from_memory_store() {
        let mut store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.finalize().wait().unwrap();

        builder = store.create_child_layer(base_name).unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
        builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

        builder.finalize().wait().unwrap();

        let layer = store.get_layer(child_name).unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }
}
