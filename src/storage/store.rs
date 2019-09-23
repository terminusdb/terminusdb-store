use crate::layer::layer::{GenericLayer,LayerType};
use crate::layer::builder::{SimpleLayerBuilder, LayerFiles};
use crate::layer::base::{BaseLayer,BaseLayerFiles};
use crate::layer::child::{ChildLayer,ChildLayerFiles};
use super::file::*;
use super::consts::FILENAMES;
use tokio::fs;
use std::io;

use futures::prelude::*;
use futures::future;

use std::collections::HashMap;
use std::path::PathBuf;
use rand;

pub trait LayerStore {
    type File: FileLoad+FileStore+Clone;
    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>>;
    fn create_base_layer(&mut self) -> Box<dyn Future<Item=SimpleLayerBuilder<Self::File>, Error=io::Error>>;
    fn create_child_layer(&mut self, parent: [u32;5]) -> Box<dyn Future<Item=SimpleLayerBuilder<Self::File>,Error=io::Error>>;
    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<GenericLayer<<Self::File as FileLoad>::Map>>,Error=io::Error>>;
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

    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>> {
        Box::new(future::ok(self.layers.keys().map(|k|k.clone()).collect()))
    }

    fn create_base_layer(&mut self) -> Box<dyn Future<Item=SimpleLayerBuilder<MemoryBackedStore>,Error=io::Error>> {
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

    fn create_child_layer(&mut self, parent: [u32;5]) -> Box<dyn Future<Item=SimpleLayerBuilder<MemoryBackedStore>, Error=io::Error>> {
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

            future::ok(SimpleLayerBuilder::from_parent(name, parent_layer, clf))
        }
                 else {
                     future::err(io::Error::new(io::ErrorKind::NotFound, "parent layer not found"))
                 })
    }

    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<GenericLayer<<MemoryBackedStore as FileLoad>::Map>>,Error=io::Error>> {
        Box::new(future::ok(self.get_layer_immediate(name)))
    }
}

pub trait PersistentLayerStore : 'static+Clone {
    type File: FileLoad+FileStore+Clone;

    fn directories(&self) -> Box<dyn Future<Item=Vec<[u32; 5]>, Error=io::Error>>;
    fn create_directory(&self) -> Box<dyn Future<Item=[u32;5], Error=io::Error>>;
    fn directory_exists(&self, name: [u32; 5]) -> Box<dyn Future<Item=bool,Error=io::Error>>;
    fn get_file(&self, directory: [u32;5], name: &str) -> Box<dyn Future<Item=Self::File, Error=io::Error>>;
    fn file_exists(&self, directory: [u32;5], file: &str) -> Box<dyn Future<Item=bool,Error=io::Error>>;

    fn layer_type(&self, name: [u32;5]) -> Box<dyn Future<Item=LayerType,Error=io::Error>> {
        Box::new(self.file_exists(name, FILENAMES.parent)
                 .map(|b| match b {
                     true => LayerType::Child,
                     false => LayerType::Base
                 }))
    }

    fn base_layer_files(&self, name: [u32;5]) -> Box<dyn Future<Item=BaseLayerFiles<Self::File>, Error=io::Error>> {
        let filenames = vec![FILENAMES.node_dictionary_blocks,
                             FILENAMES.node_dictionary_offsets,

                             FILENAMES.predicate_dictionary_blocks,
                             FILENAMES.predicate_dictionary_offsets,

                             FILENAMES.value_dictionary_blocks,
                             FILENAMES.value_dictionary_offsets,

                             FILENAMES.base_s_p_adjacency_list_bits,
                             FILENAMES.base_s_p_adjacency_list_bit_index_blocks,
                             FILENAMES.base_s_p_adjacency_list_bit_index_sblocks,
                             FILENAMES.base_s_p_adjacency_list_nums,

                             FILENAMES.base_sp_o_adjacency_list_bits,
                             FILENAMES.base_sp_o_adjacency_list_bit_index_blocks,
                             FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks,
                             FILENAMES.base_sp_o_adjacency_list_nums];

        let clone = self.clone();

        Box::new(future::join_all(filenames.into_iter().map(move |f| clone.get_file(name, f)))
                 .map(|files| BaseLayerFiles {
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
                     sp_o_adjacency_list_nums_file: files[13].clone(),
                 }))
    }

    fn child_layer_files(&self, name: [u32;5]) -> Box<dyn Future<Item=ChildLayerFiles<Self::File>,Error=io::Error>> {
        let filenames = vec![FILENAMES.node_dictionary_blocks,
                             FILENAMES.node_dictionary_offsets,

                             FILENAMES.predicate_dictionary_blocks,
                             FILENAMES.predicate_dictionary_offsets,

                             FILENAMES.value_dictionary_blocks,
                             FILENAMES.value_dictionary_offsets,

                             FILENAMES.pos_subjects,
                             FILENAMES.neg_subjects,

                             FILENAMES.pos_s_p_adjacency_list_bits,
                             FILENAMES.pos_s_p_adjacency_list_bit_index_blocks,
                             FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks,
                             FILENAMES.pos_s_p_adjacency_list_nums,

                             FILENAMES.pos_sp_o_adjacency_list_bits,
                             FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks,
                             FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks,
                             FILENAMES.pos_sp_o_adjacency_list_nums,

                             FILENAMES.neg_s_p_adjacency_list_bits,
                             FILENAMES.neg_s_p_adjacency_list_bit_index_blocks,
                             FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks,
                             FILENAMES.neg_s_p_adjacency_list_nums,

                             FILENAMES.neg_sp_o_adjacency_list_bits,
                             FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks,
                             FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks,
                             FILENAMES.neg_sp_o_adjacency_list_nums];

        let cloned = self.clone();

        Box::new(future::join_all(filenames.into_iter().map(move |f| cloned.get_file(name, f)))
                 .map(|files| ChildLayerFiles {
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
                 }))
    }

    fn write_parent_file(&self, dir_name: [u32;5], parent_name: [u32;5]) -> Box<dyn Future<Item=(), Error=std::io::Error>> {
        let parent_string = format!("{:x}{:x}{:x}{:x}{:x}", parent_name[0], parent_name[1], parent_name[2], parent_name[3], parent_name[4]);

        Box::new(self.get_file(dir_name, FILENAMES.parent)
                 .map(|f|f.open_write())
                 .and_then(|writer| tokio::io::write_all(writer, parent_string))
                 .map(|_|()))
    }

    fn read_parent_file(&self, dir_name: [u32;5]) -> Box<dyn Future<Item=[u32;5], Error=std::io::Error>> {
        Box::new(self.get_file(dir_name, FILENAMES.parent)
                 .map(|f|f.open_read())
                 .and_then(|reader| tokio::io::read_exact(reader, vec![0;20]))
                 .and_then(|(_, buf)| bytes_to_name(&buf)))
    }
}

fn string_to_name(string: &str) -> Result<[u32;5], std::io::Error> {
    let n1 = u32::from_str_radix(&string[..4], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n2 = u32::from_str_radix(&string[4..8], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n3 = u32::from_str_radix(&string[8..12], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n4 = u32::from_str_radix(&string[12..16], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n5 = u32::from_str_radix(&string[16..20], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok([n1,n2,n3,n4,n5])
}

fn bytes_to_name(bytes: &Vec<u8>) -> Result<[u32;5],std::io::Error> {
    if bytes.len() != 20 {
        Err(io::Error::new(io::ErrorKind::Other, "bytes not len 20"))
    }
    else {
        let string = String::from_utf8_lossy(&bytes);

        string_to_name(&string)
    }
}

impl<F:'static+FileLoad+FileStore+Clone,T: 'static+PersistentLayerStore<File=F>> LayerStore for T {
    type File = F;

    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>> {
        self.directories()
    }
    
    fn create_base_layer(&mut self) -> Box<dyn Future<Item=SimpleLayerBuilder<F>,Error=io::Error>> {
        let cloned = self.clone();
        Box::new(self.create_directory()
                 .and_then(move |dir_name| cloned.base_layer_files(dir_name)
                           .map(move |blf| SimpleLayerBuilder::new(dir_name, blf))))
    }

    fn create_child_layer(&mut self, parent: [u32;5]) -> Box<dyn Future<Item=SimpleLayerBuilder<F>,Error=io::Error>> {
        let cloned = self.clone();
        Box::new(self.get_layer(parent)
                 .and_then(|parent_layer|
                           match parent_layer {
                               None => Err(io::Error::new(io::ErrorKind::NotFound, "parent layer not found")),
                               Some(parent_layer) => Ok(parent_layer)
                           })
                 .and_then(move |parent_layer|
                           cloned.create_directory()
                           .and_then(move |dir_name| cloned.write_parent_file(dir_name, parent)
                                     .and_then(move |_| cloned.child_layer_files(dir_name)
                                               .map(move |clf| SimpleLayerBuilder::from_parent(dir_name, parent_layer, clf))))))
    }

    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<GenericLayer<<F as FileLoad>::Map>>,Error=io::Error>> {
        let cloned = self.clone();
        Box::new(self.directory_exists(name)
                 .and_then(move |b| match b {
                     false => {
                         let result: Box<dyn Future<Item=_,Error=_>> = Box::new(future::ok(None));

                         result
                     },
                     true => Box::new(cloned.layer_type(name)
                                      .and_then(move |t| {
                                          let result: Box<dyn Future<Item=Option<GenericLayer<<F as FileLoad>::Map>>,Error=io::Error>> =
                                              match t {
                                                  LayerType::Base => Box::new(cloned.base_layer_files(name)
                                                                              .and_then(move |blf| BaseLayer::load_from_files(name, &blf))
                                                                              .map(|bl| Some(GenericLayer::Base(bl)))),
                                                  LayerType::Child => Box::new(cloned.read_parent_file(name)
                                                                               .and_then(move |parent_name| cloned.get_layer(parent_name)
                                                                                         .and_then(|parent_layer| match parent_layer {
                                                                                             None => Err(io::Error::new(io::ErrorKind::NotFound, "parent layer not found")),
                                                                                             Some(parent_layer) => Ok(parent_layer)
                                                                                         })
                                                                                         .and_then(move |parent_layer| cloned.child_layer_files(name)
                                                                                                   .and_then(move |clf| ChildLayer::load_from_files(name, parent_layer, &clf))))
                                                                               .map(|cl| Some(GenericLayer::Child(cl))))
                                              };

                                          result
                                      }))
                 }))
    }
}

#[derive(Clone)]
struct DirectoryLayerStore {
    path: PathBuf
}

impl DirectoryLayerStore {
    pub fn new<P:Into<PathBuf>>(path: P) -> DirectoryLayerStore {
        DirectoryLayerStore {
            path: path.into()
        }
    }
}

impl PersistentLayerStore for DirectoryLayerStore {
    type File = FileBackedStore;

    fn directories(&self) -> Box<dyn Future<Item=Vec<[u32; 5]>, Error=std::io::Error>> {
        Box::new(fs::read_dir(self.path.clone()).flatten_stream()
                 .map(|direntry| (direntry.file_name(), direntry))
                 .and_then(|(dir_name, direntry)| future::poll_fn(move || direntry.poll_file_type())
                           .map(move |ft| (dir_name, ft.is_dir())))
                 .filter_map(|(dir_name, is_dir)| match is_dir {
                     true => Some(dir_name),
                     false => None
                 })
                 .and_then(|dir_name| dir_name.to_str().ok_or(io::Error::new(io::ErrorKind::InvalidData, "unexpected non-utf8 directory name")).map(|s|s.to_owned()))
                 .and_then(|s| string_to_name(&s))
                 .collect())
    }

    fn create_directory(&self) -> Box<dyn Future<Item=[u32;5], Error=io::Error>> {
        panic!("not implemented");
    }

    fn directory_exists(&self, name: [u32; 5]) -> Box<dyn Future<Item=bool,Error=io::Error>> {
        panic!("not implemented");
    }

    fn get_file(&self, directory: [u32;5], name: &str) -> Box<dyn Future<Item=Self::File, Error=io::Error>> {
        panic!("not implemented");
    }
    
    fn file_exists(&self, directory: [u32;5], file: &str) -> Box<dyn Future<Item=bool,Error=io::Error>> {
        panic!("not implemented");
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
