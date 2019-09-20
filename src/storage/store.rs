use crate::layer::layer::{GenericLayer,LayerType};
use crate::layer::builder::{SimpleLayerBuilder, LayerFiles};
use crate::layer::base::{BaseLayer,BaseLayerFiles};
use crate::layer::child::{ChildLayer,ChildLayerFiles};
use super::file::*;
use super::consts::FILENAMES;
use std::fs;

use futures::prelude::*;
use futures::future;

use std::collections::HashMap;
use std::path::PathBuf;
use rand;

pub trait LayerStore {
    type File: FileLoad+FileStore+Clone;
    fn layers(&self) -> Vec<[u32;5]>;
    fn create_base_layer(&mut self) -> Box<dyn Future<Item=SimpleLayerBuilder<Self::File>, Error=std::io::Error>>;
    fn create_child_layer(&mut self, parent: [u32;5]) -> Box<dyn Future<Item=Option<SimpleLayerBuilder<Self::File>>,Error=std::io::Error>>;
    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<GenericLayer<<Self::File as FileLoad>::Map>>,Error=std::io::Error>>;
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

    fn get_layer_immediate(&self, name: [u32;5]) -> Option<GenericLayer<<MemoryBackedStore as FileLoad>::Map>> {
        self.layers.get(&name)
            .map(|(parent_name, files)| {
                if parent_name.is_some() {
                    let parent = self.get_layer_immediate(parent_name.unwrap()).expect("expected parent layer to exist");
                    let layer = ChildLayer::load_from_files(name, parent, &files.clone().into_child()).wait().unwrap();

                    GenericLayer::Child(layer)
                }
                else {
                    let layer = BaseLayer::load_from_files(name, &files.clone().into_base()).wait().unwrap();

                    GenericLayer::Base(layer)
                }
            })
    }
}

impl LayerStore for MemoryLayerStore {
    type File = MemoryBackedStore;

    fn layers(&self) -> Vec<[u32;5]> {
        self.layers.keys().map(|k|k.clone()).collect()
    }

    fn create_base_layer(&mut self) -> Box<dyn Future<Item=SimpleLayerBuilder<MemoryBackedStore>,Error=std::io::Error>> {
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

        Box::new(future::ok(SimpleLayerBuilder::new(name, blf)))
    }

    fn create_child_layer(&mut self, parent: [u32;5]) -> Box<dyn Future<Item=Option<SimpleLayerBuilder<MemoryBackedStore>>, Error=std::io::Error>> {
        Box::new(if let Some(parent_layer) = self.get_layer_immediate(parent) {
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

            future::ok(Some(SimpleLayerBuilder::from_parent(name, parent_layer, clf)))
        }
                 else {
                     future::ok(None)
                 })
    }

    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<GenericLayer<<MemoryBackedStore as FileLoad>::Map>>,Error=std::io::Error>> {
        Box::new(future::ok(self.get_layer_immediate(name)))
    }
}

/*
pub trait PersistentLayerStore {
    type File: FileLoad+FileStore+Clone;

    fn directories(&self) -> Vec<[u32; 5]>;
    fn create_directory(&self, name: [u32; 5]) -> Result<(), std::io::Error>;
    fn directory_exists(&self, name: [u32; 5]) -> bool;
    fn get_file(&self, directory: [u32;5], name: &str) -> Self::File;
    fn file_exists(&self, directory: [u32;5], file: &str) -> bool;

    fn layer_type(&self, name: [u32;5]) -> LayerType {
        if self.file_exists(name, FILENAMES.parent) {
            LayerType::Child
        }
        else {
            LayerType::Base
        }
    }

    fn base_layer_files(&self, name: [u32;5]) -> BaseLayerFiles<Self::File> {
        BaseLayerFiles {
            node_dictionary_blocks_file: self.get_file(name, FILENAMES.node_dictionary_blocks),
            node_dictionary_offsets_file: self.get_file(name, FILENAMES.node_dictionary_offsets),

            predicate_dictionary_blocks_file: self.get_file(name, FILENAMES.predicate_dictionary_blocks),
            predicate_dictionary_offsets_file: self.get_file(name, FILENAMES.predicate_dictionary_offsets),

            value_dictionary_blocks_file: self.get_file(name, FILENAMES.value_dictionary_blocks),
            value_dictionary_offsets_file: self.get_file(name, FILENAMES.value_dictionary_offsets),

            s_p_adjacency_list_bits_file: self.get_file(name, FILENAMES.base_s_p_adjacency_list_bits),
            s_p_adjacency_list_blocks_file: self.get_file(name, FILENAMES.base_s_p_adjacency_list_bit_index_blocks),
            s_p_adjacency_list_sblocks_file: self.get_file(name, FILENAMES.base_s_p_adjacency_list_bit_index_sblocks),
            s_p_adjacency_list_nums_file: self.get_file(name, FILENAMES.base_s_p_adjacency_list_nums),

            sp_o_adjacency_list_bits_file: self.get_file(name, FILENAMES.base_sp_o_adjacency_list_bits),
            sp_o_adjacency_list_blocks_file: self.get_file(name, FILENAMES.base_sp_o_adjacency_list_bit_index_blocks),
            sp_o_adjacency_list_sblocks_file: self.get_file(name, FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks),
            sp_o_adjacency_list_nums_file: self.get_file(name, FILENAMES.base_sp_o_adjacency_list_nums)
        }
    }

    fn child_layer_files(&self, name: [u32;5]) -> ChildLayerFiles<Self::File> {
        ChildLayerFiles {
            node_dictionary_blocks_file: self.get_file(name, FILENAMES.node_dictionary_blocks),
            node_dictionary_offsets_file: self.get_file(name, FILENAMES.node_dictionary_offsets),

            predicate_dictionary_blocks_file: self.get_file(name, FILENAMES.predicate_dictionary_blocks),
            predicate_dictionary_offsets_file: self.get_file(name, FILENAMES.predicate_dictionary_offsets),

            value_dictionary_blocks_file: self.get_file(name, FILENAMES.value_dictionary_blocks),
            value_dictionary_offsets_file: self.get_file(name, FILENAMES.value_dictionary_offsets),

            pos_subjects_file: self.get_file(name, FILENAMES.pos_subjects),
            neg_subjects_file: self.get_file(name, FILENAMES.neg_subjects),

            pos_s_p_adjacency_list_bits_file: self.get_file(name, FILENAMES.pos_s_p_adjacency_list_bits),
            pos_s_p_adjacency_list_blocks_file: self.get_file(name, FILENAMES.pos_s_p_adjacency_list_bit_index_blocks),
            pos_s_p_adjacency_list_sblocks_file: self.get_file(name, FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks),
            pos_s_p_adjacency_list_nums_file: self.get_file(name, FILENAMES.pos_s_p_adjacency_list_nums),

            pos_sp_o_adjacency_list_bits_file: self.get_file(name, FILENAMES.pos_sp_o_adjacency_list_bits),
            pos_sp_o_adjacency_list_blocks_file: self.get_file(name, FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks),
            pos_sp_o_adjacency_list_sblocks_file: self.get_file(name, FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks),
            pos_sp_o_adjacency_list_nums_file: self.get_file(name, FILENAMES.pos_sp_o_adjacency_list_nums),

            neg_s_p_adjacency_list_bits_file: self.get_file(name, FILENAMES.neg_s_p_adjacency_list_bits),
            neg_s_p_adjacency_list_blocks_file: self.get_file(name, FILENAMES.neg_s_p_adjacency_list_bit_index_blocks),
            neg_s_p_adjacency_list_sblocks_file: self.get_file(name, FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks),
            neg_s_p_adjacency_list_nums_file: self.get_file(name, FILENAMES.neg_s_p_adjacency_list_nums),

            neg_sp_o_adjacency_list_bits_file: self.get_file(name, FILENAMES.neg_sp_o_adjacency_list_bits),
            neg_sp_o_adjacency_list_blocks_file: self.get_file(name, FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks),
            neg_sp_o_adjacency_list_sblocks_file: self.get_file(name, FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks),
            neg_sp_o_adjacency_list_nums_file: self.get_file(name, FILENAMES.neg_sp_o_adjacency_list_nums)
        }
    }
}

impl<F:'static+FileLoad+FileStore+Clone,T: 'static+PersistentLayerStore<File=F>> LayerStore for T {
    type File = F;

    fn layers(&self) -> Vec<[u32;5]> {
        self.directories()
    }
    
    fn create_base_layer(&mut self) -> Box<dyn Future<Item=SimpleLayerBuilder<F>,Error=std::io::Error>> {
        let name: [u32;5] = rand::random();
        self.create_directory(name).expect("dir creation failure");

        let blf = self.base_layer_files(name);
        Box::new(future::ok(SimpleLayerBuilder::new(name, blf)))
    }

    fn create_child_layer(&mut self, parent: [u32;5]) -> Box<dyn Future<Item=Option<SimpleLayerBuilder<F>>,Error=std::io::Error>> {
        let name: [u32;5] = rand::random();
        self.create_directory(name).expect("dir creation failure");

        Box::new(self.get_layer(parent)
                 .map(move |parent_layer| {
                     let clf = self.child_layer_files(name);

                     Some(SimpleLayerBuilder::from_parent(name, parent_layer?, clf))
                 }))
    }

    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<GenericLayer<<F as FileLoad>::Map>>,Error=std::io::Error>> {
        /*
        if self.directory_exists(name) {
            Some(match self.layer_type(name) {
                LayerType::Base => GenericLayer::Base(BaseLayer::load_from_files(name, &self.base_layer_files(name))),
                LayerType::Child => {
                    panic!("argh");
                }
            })
        }
        else {
            None
        }
        */
        panic!("oh no");
    }
}
*/

/*

struct DirectoryLayerStore {
    path: PathBuf
}

impl DirectoryLayerStore {
    pub fn new<P:Into<PathBuf>>(path: P) -> DirectoryLayerStore {
        DirectoryLayerStore {
            path: path.into()
        }
    }

    fn dir_paths(&self) -> Result<Vec<PathBuf>,std::io::Error> {
        Ok(fs::read_dir(self.path.clone())?
           .filter(|entry| entry.as_ref().map(|e|e.path().is_dir()).unwrap_or(false))
           .map(|entry| entry.unwrap().path())
           .collect())
    }

    fn layer_dir_path(&self, name: &[u32;5]) -> PathBuf {
        let mut buf = self.path.clone();
        let name_string = format!("{:x}{:x}{:x}{:x}{:x}", name[0], name[1], name[2], name[3], name[4]);
        buf.push(name_string);

        buf
    }

    fn layer_file_path(&self, layer_name: &[u32;5], file_name: &str) -> PathBuf {
        let mut path = self.layer_dir_path(layer_name);
        path.push(file_name);

        path
    }

    fn layer_file(&self, layer_name: &[u32;5], file_name: &str) -> FileBackedStore {
        FileBackedStore::new(self.layer_file_path(layer_name, file_name))
    }

    fn base_layer_files(&self, name: &[u32;5]) -> BaseLayerFiles<FileBackedStore> {
        BaseLayerFiles {
            node_dictionary_blocks_file: self.layer_file(name, FILENAMES.node_dictionary_blocks),
            node_dictionary_offsets_file: self.layer_file(name, FILENAMES.node_dictionary_offsets),

            predicate_dictionary_blocks_file: self.layer_file(name, FILENAMES.predicate_dictionary_blocks),
            predicate_dictionary_offsets_file: self.layer_file(name, FILENAMES.predicate_dictionary_offsets),

            value_dictionary_blocks_file: self.layer_file(name, FILENAMES.value_dictionary_blocks),
            value_dictionary_offsets_file: self.layer_file(name, FILENAMES.value_dictionary_offsets),

            s_p_adjacency_list_bits_file: self.layer_file(name, FILENAMES.base_s_p_adjacency_list_bits),
            s_p_adjacency_list_blocks_file: self.layer_file(name, FILENAMES.base_s_p_adjacency_list_bit_index_blocks),
            s_p_adjacency_list_sblocks_file: self.layer_file(name, FILENAMES.base_s_p_adjacency_list_bit_index_sblocks),
            s_p_adjacency_list_nums_file: self.layer_file(name, FILENAMES.base_s_p_adjacency_list_nums),

            sp_o_adjacency_list_bits_file: self.layer_file(name, FILENAMES.base_sp_o_adjacency_list_bits),
            sp_o_adjacency_list_blocks_file: self.layer_file(name, FILENAMES.base_sp_o_adjacency_list_bit_index_blocks),
            sp_o_adjacency_list_sblocks_file: self.layer_file(name, FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks),
            sp_o_adjacency_list_nums_file: self.layer_file(name, FILENAMES.base_sp_o_adjacency_list_nums)
        }
    }
}

impl LayerStore for DirectoryLayerStore {
    type File = FileBackedStore;

    fn layers(&self) -> Vec<[u32;5]> {
        self.dir_paths().unwrap_or(vec![]).iter()
            .filter_map(|p|Some(p.file_name()?.to_str()?.to_owned()))
            .filter(|p| p.len() == 40)
            .filter_map(|p| {
                let s1 = &p[0..4];
                let s2 = &p[4..8];
                let s3 = &p[8..12];
                let s4 = &p[12..16];
                let s5 = &p[16..20];

                let n1 = u32::from_str_radix(s1, 16).ok()?;
                let n2 = u32::from_str_radix(s2, 16).ok()?;
                let n3 = u32::from_str_radix(s3, 16).ok()?;
                let n4 = u32::from_str_radix(s4, 16).ok()?;
                let n5 = u32::from_str_radix(s5, 16).ok()?;

                Some([n1,n2,n3,n4,n5])
            })
            .collect()
    }

    fn create_base_layer(&mut self) -> SimpleLayerBuilder<FileBackedStore> {
        let name: [u32;5] = rand::random();
        fs::create_dir(self.layer_dir_path(&name)).expect("dir creation failure");
        // todo gotta write metadata so it is clear that this is a base layer

        let blf = self.base_layer_files(&name);
        SimpleLayerBuilder::new(name, blf)
    }

    fn create_child_layer(&mut self, parent: [u32;5]) -> Option<SimpleLayerBuilder<FileBackedStore>> {
        panic!("oh no");
    }

    fn get_layer(&self, name: [u32;5]) -> Option<GenericLayer<<FileBackedStore as FileLoad>::Map>> {
        panic!("oh no");
    }
}
*/

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::layer::layer::*;
    use futures::prelude::*;
    
    #[test]
    fn create_layers_from_memory_store() {
        let mut store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.finalize().wait().unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap().unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
        builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

        builder.finalize().wait().unwrap();

        let layer = store.get_layer(child_name).wait().unwrap().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }
}
