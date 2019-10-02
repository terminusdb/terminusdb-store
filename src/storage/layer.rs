use crate::layer::layer::{Layer,LayerType};
use crate::layer::builder::{LayerBuilder,SimpleLayerBuilder, LayerFiles};
use crate::layer::base::{BaseLayer,BaseLayerFiles};
use crate::layer::child::{ChildLayer,ChildLayerFiles};
use super::file::*;
use super::consts::FILENAMES;
use tokio::fs;
use std::io;
use std::sync::{Arc,Weak};

use futures::prelude::*;
use futures::future;
use futures_locks::RwLock;

use std::collections::HashMap;
use std::path::PathBuf;
use rand;

pub trait LayerRetriever: 'static+Send+Sync {
    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>+Send+Sync>;
    fn get_layer_with_retriever(&self, name: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Option<Arc<dyn Layer>>,Error=io::Error>+Send+Sync>;
    fn boxed_retriever(&self) -> Box<dyn LayerRetriever>;
    fn get_layer(&self, name: [u32;5]) -> Box<dyn Future<Item=Option<Arc<dyn Layer>>,Error=io::Error>+Send+Sync> {
        self.get_layer_with_retriever(name, self.boxed_retriever())
    }
}

pub trait LayerStore: LayerRetriever {
    fn create_base_layer(&self) -> Box<dyn Future<Item=Box<dyn LayerBuilder>, Error=io::Error>+Send+Sync>;
    fn create_child_layer_with_retriever(&self, parent: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync>;
    fn create_child_layer(&self, parent: [u32;5]) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync> {
        self.create_child_layer_with_retriever(parent, self.boxed_retriever())
    }
}

#[derive(Clone)]
pub struct MemoryLayerStore {
    layers: RwLock<HashMap<[u32;5],(Option<[u32;5]>,LayerFiles<MemoryBackedStore>)>>
}

impl MemoryLayerStore {
    pub fn new() -> MemoryLayerStore {
        MemoryLayerStore {
            layers: RwLock::new(HashMap::new())
        }
    }
}

impl LayerRetriever for MemoryLayerStore {
    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>+Send+Sync> {
        Box::new(self.layers.read()
                 .then(|layers|Ok(layers.expect("rwlock read cannot fail").keys().map(|k|k.clone()).collect())))
    }

    fn get_layer_with_retriever(&self, name: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Option<Arc<dyn Layer>>,Error=io::Error>+Send+Sync> {
        Box::new(self.layers.read()
                 .then(move |layers| {
                     let layers = layers.expect("rwlock read should always succeed");
                     let saved = layers.get(&name).map(|x|x.clone());
                     let fut: Box<dyn Future<Item=_,Error=_>+Send+Sync> = match saved {
                         None => Box::new(future::ok(None)),
                         Some(saved) => Box::new(
                             future::ok(saved)
                                 .and_then(move |(parent_name, files)| {
                                     let fut: Box<dyn Future<Item=_,Error=_>+Send+Sync> = 
                                         if parent_name.is_some() {
                                             let files = files.clone().into_child();
                                             Box::new(retriever.get_layer(parent_name.unwrap())
                                                      .and_then(|parent| match parent {
                                                          None => Err(io::Error::new(io::ErrorKind::InvalidData, "expected parent layer to exist")),
                                                          Some(p) => Ok(p)
                                                      })
                                                      .and_then(move |parent| ChildLayer::load_from_files(name, parent, &files))
                                                      .map(|layer| Some(Arc::new(layer) as Arc<dyn Layer>)))
                                         } else {
                                             Box::new(BaseLayer::load_from_files(name, &files.clone().into_base())
                                                      .map(|layer| Some(Arc::new(layer) as Arc<dyn Layer>)))
                                         };
                                     fut
                                 }))
                     };

                     fut
                 }))

    }

    fn boxed_retriever(&self) -> Box<dyn LayerRetriever> {
        Box::new(self.clone())
    }
}

impl LayerStore for MemoryLayerStore {
    fn create_base_layer(&self) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync> {
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

        Box::new(self.layers.write()
                 .then(move |layers| {
                     layers.expect("rwlock write should always succeed").insert(name, (None, LayerFiles::Base(blf.clone())));
                     Ok(Box::new(SimpleLayerBuilder::new(name, blf)) as Box<dyn LayerBuilder>)
                 }))
    }

    fn create_child_layer_with_retriever(&self, parent: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync> {
        let layers = self.layers.clone();
        Box::new(retriever.get_layer(parent)
                 .and_then(|parent_layer| match parent_layer {
                     None => future::err(io::Error::new(io::ErrorKind::NotFound, "parent layer not found")),
                     Some(parent_layer) => future::ok(parent_layer)
                 })
                 .and_then(move |parent_layer| {
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

                     layers.write()
                         .then(move |layers| {
                             layers.expect("rwlock write should always succeed").insert(name, (Some(parent), LayerFiles::Child(clf.clone())));
                             Ok(Box::new(SimpleLayerBuilder::from_parent(name, parent_layer, clf)) as Box<dyn LayerBuilder>)
                         })
                 }))
    }
}

pub trait PersistentLayerStore : 'static+Send+Sync+Clone {
    type File: FileLoad+FileStore+Clone;
    fn directories(&self) -> Box<dyn Future<Item=Vec<[u32; 5]>, Error=io::Error>+Send+Sync>;
    fn create_directory(&self) -> Box<dyn Future<Item=[u32;5], Error=io::Error>+Send+Sync>;
    fn directory_exists(&self, name: [u32; 5]) -> Box<dyn Future<Item=bool,Error=io::Error>+Send+Sync>;
    fn get_file(&self, directory: [u32;5], name: &str) -> Box<dyn Future<Item=Self::File, Error=io::Error>+Send+Sync>;
    fn file_exists(&self, directory: [u32;5], file: &str) -> Box<dyn Future<Item=bool,Error=io::Error>+Send+Sync>;

    fn layer_type(&self, name: [u32;5]) -> Box<dyn Future<Item=LayerType,Error=io::Error>+Send+Sync> {
        Box::new(self.file_exists(name, FILENAMES.parent)
                 .map(|b| match b {
                     true => LayerType::Child,
                     false => LayerType::Base
                 }))
    }

    fn base_layer_files(&self, name: [u32;5]) -> Box<dyn Future<Item=BaseLayerFiles<Self::File>, Error=io::Error>+Send+Sync> {
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

    fn child_layer_files(&self, name: [u32;5]) -> Box<dyn Future<Item=ChildLayerFiles<Self::File>,Error=io::Error>+Send+Sync> {
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

    fn write_parent_file(&self, dir_name: [u32;5], parent_name: [u32;5]) -> Box<dyn Future<Item=(), Error=std::io::Error>+Send+Sync> {
        let parent_string = name_to_string(parent_name);

        Box::new(self.get_file(dir_name, FILENAMES.parent)
                 .map(|f|f.open_write())
                 .and_then(|writer| tokio::io::write_all(writer, parent_string))
                 .map(|_|()))
    }

    fn read_parent_file(&self, dir_name: [u32;5]) -> Box<dyn Future<Item=[u32;5], Error=std::io::Error>+Send+Sync> {
        Box::new(self.get_file(dir_name, FILENAMES.parent)
                 .map(|f|f.open_read())
                 .and_then(|reader| tokio::io::read_exact(reader, vec![0;40]))
                 .and_then(|(_, buf)| bytes_to_name(&buf)))
    }
}

pub fn name_to_string(name: [u32;5]) -> String {
    format!("{:08x}{:08x}{:08x}{:08x}{:08x}", name[0], name[1], name[2], name[3], name[4])
}

pub fn string_to_name(string: &str) -> Result<[u32;5], std::io::Error> {
    if string.len() != 40 {
        return Err(io::Error::new(io::ErrorKind::Other, "string not len 40"));
    }
    let n1 = u32::from_str_radix(&string[..8], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n2 = u32::from_str_radix(&string[8..16], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n3 = u32::from_str_radix(&string[16..24], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n4 = u32::from_str_radix(&string[24..32], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let n5 = u32::from_str_radix(&string[32..40], 16)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok([n1,n2,n3,n4,n5])
}

pub fn bytes_to_name(bytes: &Vec<u8>) -> Result<[u32;5],std::io::Error> {
    if bytes.len() != 40 {
        Err(io::Error::new(io::ErrorKind::Other, "bytes not len 40"))
    }
    else {
        let string = String::from_utf8_lossy(&bytes);

        string_to_name(&string)
    }
}

impl<F:'static+FileLoad+FileStore+Clone,T: 'static+PersistentLayerStore<File=F>> LayerRetriever for T {
    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>+Send+Sync> {
        self.directories()
    }
    
    fn get_layer_with_retriever(&self, name: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Option<Arc<dyn Layer>>,Error=io::Error>+Send+Sync> {
        let cloned = self.clone();
        Box::new(self.directory_exists(name)
                 .and_then(move |b| match b {
                     false => {
                         let result: Box<dyn Future<Item=_,Error=_>+Send+Sync> = Box::new(future::ok(None));

                         result
                     },
                     true => Box::new(cloned.layer_type(name)
                                      .and_then(move |t| {
                                          let result: Box<dyn Future<Item=Option<Arc<dyn Layer>>,Error=io::Error>+Send+Sync> =
                                              match t {
                                                  LayerType::Base => Box::new(cloned.base_layer_files(name)
                                                                              .and_then(move |blf| BaseLayer::load_from_files(name, &blf))
                                                                              .map(|bl| Some(Arc::new(bl) as Arc<dyn Layer>))),
                                                  LayerType::Child => Box::new(cloned.read_parent_file(name)
                                                                               .and_then(move |parent_name| retriever.get_layer(parent_name)
                                                                                         .and_then(|parent_layer| match parent_layer {
                                                                                             None => Err(io::Error::new(io::ErrorKind::NotFound, "parent layer not found")),
                                                                                             Some(parent_layer) => Ok(parent_layer)
                                                                                         })
                                                                                         .and_then(move |parent_layer| cloned.child_layer_files(name)
                                                                                                   .and_then(move |clf| ChildLayer::load_from_files(name, parent_layer, &clf))))
                                                                               .map(|cl| Some(Arc::new(cl) as Arc<dyn Layer>)))
                                              };

                                          result
                                      }))
                 }))
    }

    fn boxed_retriever(&self) -> Box<dyn LayerRetriever> {
        Box::new(self.clone())
    }
}

impl<F:'static+FileLoad+FileStore+Clone,T: 'static+PersistentLayerStore<File=F>> LayerStore for T {
    fn create_base_layer(&self) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync> {
        let cloned = self.clone();
        Box::new(self.create_directory()
                 .and_then(move |dir_name| cloned.base_layer_files(dir_name)
                           .map(move |blf| Box::new(SimpleLayerBuilder::new(dir_name, blf)) as Box<dyn LayerBuilder>)))
    }

    fn create_child_layer_with_retriever(&self, parent: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync> {
        let cloned = self.clone();
        Box::new(retriever.get_layer(parent)
                 .and_then(|parent_layer|
                           match parent_layer {
                               None => Err(io::Error::new(io::ErrorKind::NotFound, "parent layer not found")),
                               Some(parent_layer) => Ok(parent_layer)
                           })
                 .and_then(move |parent_layer|
                           cloned.create_directory()
                           .and_then(move |dir_name| cloned.write_parent_file(dir_name, parent)
                                     .and_then(move |_| cloned.child_layer_files(dir_name)
                                               .map(move |clf| Box::new(SimpleLayerBuilder::from_parent(dir_name, parent_layer, clf)) as Box<dyn LayerBuilder>)))))
    }

}

#[derive(Clone)]
pub struct DirectoryLayerStore {
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
    fn directories(&self) -> Box<dyn Future<Item=Vec<[u32; 5]>, Error=std::io::Error>+Send+Sync> {
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

    fn create_directory(&self) -> Box<dyn Future<Item=[u32;5], Error=io::Error>+Send+Sync> {
        let name = rand::random();
        let mut p = self.path.clone();
        p.push(name_to_string(name));

        Box::new(fs::create_dir(p)
                 .map(move |_| name))
    }

    fn directory_exists(&self, name: [u32; 5]) -> Box<dyn Future<Item=bool,Error=io::Error>+Send+Sync> {
        let mut p = self.path.clone();
        p.push(name_to_string(name));

        Box::new(fs::metadata(p)
                 .then(|result| match result {
                     Ok(f) => Ok(f.is_dir()),
                     Err(_) => Ok(false)
                 }))
    }

    fn get_file(&self, directory: [u32;5], name: &str) -> Box<dyn Future<Item=Self::File, Error=io::Error>+Send+Sync> {
        let mut p = self.path.clone();
        p.push(name_to_string(directory));
        p.push(name);
        Box::new(future::ok(FileBackedStore::new(p)))
    }
    
    fn file_exists(&self, directory: [u32;5], file: &str) -> Box<dyn Future<Item=bool,Error=io::Error>+Send+Sync> {
        let mut p = self.path.clone();
        p.push(name_to_string(directory));
        p.push(file);
        Box::new(fs::metadata(p)
                 .then(|result| match result {
                     Ok(f) => Ok(f.is_file()),
                     Err(_) => Ok(false)
                 }))
    }
}

#[derive(Clone)]
pub struct CachedLayerStore {
    inner: Arc<dyn LayerStore>,
    cache: RwLock<HashMap<[u32;5],Weak<dyn Layer>>>
}


impl CachedLayerStore {
    pub fn new<S:LayerStore>(inner: S) -> CachedLayerStore {
        CachedLayerStore {
            inner: Arc::new(inner),
            cache: RwLock::new(HashMap::new())
        }
    }
}

impl LayerRetriever for CachedLayerStore {
    fn layers(&self) -> Box<dyn Future<Item=Vec<[u32;5]>, Error=io::Error>+Send+Sync> {
        self.inner.layers()
    }

    fn get_layer_with_retriever(&self, name: [u32;5], retriever: Box<dyn LayerRetriever+>) -> Box<dyn Future<Item=Option<Arc<dyn Layer>>,Error=io::Error>+Send+Sync> {
        let cloned = self.clone();
        Box::new(
            self.cache.read()
                .then(move |cache| Ok(cache.expect("rwlock read should always succeed")
                                 .get(&name)
                                 .map(|cached| cached.clone())))
                .and_then(move |cached| {
                    let fut:Box<dyn Future<Item=_,Error=_>+Send+Sync> =
                        match cached {
                            None => Box::new(
                                cloned.inner.get_layer_with_retriever(name, retriever)
                                    .and_then(move |layer| {
                                        let fut:Box<dyn Future<Item=_,Error=_>+Send+Sync> =
                                            match layer {
                                                None => Box::new(future::ok(None)),
                                                Some(layer) => Box::new(cloned.cache.write()
                                                                        .then(move |cache| {
                                                                            cache.expect("rwlock write should always succeed")
                                                                                .insert(name, Arc::downgrade(&layer));
                                                                            Ok(Some(layer))
                                                                        }))
                                            };
                                        fut
                                    })),
                            Some(cached) => match cached.upgrade() {
                                None => Box::new(cloned.cache.write()
                                                 .then(move |cache| {
                                                     cache.expect("rwlock write should always succeed")
                                                         .remove(&name);

                                                     cloned.get_layer_with_retriever(name, retriever)
                                    })),
                                Some(cached) => Box::new(future::ok(Some(cached)))
                            }
                        };

                    fut
                }))
    }

    fn boxed_retriever(&self) -> Box<dyn LayerRetriever> {
        Box::new(self.clone())
    }
}

impl LayerStore for CachedLayerStore {
    fn create_base_layer(&self) -> Box<dyn Future<Item=Box<dyn LayerBuilder>, Error=io::Error>+Send+Sync> {
        self.inner.create_base_layer()
    }

    fn create_child_layer_with_retriever(&self, parent: [u32;5], retriever: Box<dyn LayerRetriever>) -> Box<dyn Future<Item=Box<dyn LayerBuilder>,Error=io::Error>+Send+Sync> {
        self.inner.create_child_layer_with_retriever(parent, retriever)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::layer::*;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;
    use futures::sync::oneshot;
    
    #[test]
    fn create_layers_from_memory_store() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
        builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

        builder.commit_boxed().wait().unwrap();

        let layer = store.get_layer(child_name).wait().unwrap().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }
    
    #[test]
    fn create_layers_from_directory_store() {
        let runtime = Runtime::new().unwrap();
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        let task = store.create_base_layer()
            .and_then(|mut builder| {
                let base_name = builder.name();

                builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
                builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
                builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

                builder.commit_boxed()
                    .map(move |_| base_name)
            })
            .and_then(move |base_name| store.create_child_layer(base_name)
                      .and_then(|mut builder| {
                          let child_name = builder.name();

                          builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
                          builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

                          builder.commit_boxed()
                              .map(move |_| child_name)
                      })
                      .and_then(move |child_name| store.get_layer(child_name)));

        let layer = oneshot::spawn(task, &runtime.executor()).wait().unwrap().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[test]
    fn cached_layer_store_returns_same_layer_multiple_times() {
        let store = CachedLayerStore::new(MemoryLayerStore::new());
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
        builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

        builder.commit_boxed().wait().unwrap();

        let layer1 = store.get_layer(child_name).wait().unwrap().unwrap();
        let layer2 = store.get_layer(child_name).wait().unwrap().unwrap();

        let base_layer = store.get_layer(base_name).wait().unwrap().unwrap();

        assert!(Arc::ptr_eq(&layer1, &layer2));
        assert!(Arc::ptr_eq(&base_layer, &layer1.parent().unwrap()));
    }

    #[test]
    fn cached_layer_store_forgets_entries_when_they_are_dropped() {
        let store = CachedLayerStore::new(MemoryLayerStore::new());
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.commit_boxed().wait().unwrap();

        let layer = store.get_layer(base_name).wait().unwrap().unwrap();
        let weak = Arc::downgrade(&layer);

        // we expect 2 weak pointers, the one we made above and the one stored in cache
        assert_eq!(2, Arc::weak_count(&layer));


        // forget the layers
        std::mem::drop(layer);

        // according to our weak reference, there's no longer any strong reference around
        assert!(weak.upgrade().is_none());

        // retrieving the same layer again works just fine
        let layer = store.get_layer(base_name).wait().unwrap().unwrap();

        // and only has one weak pointer pointing to it, the newly cached one
        assert_eq!(1, Arc::weak_count(&layer));
    }
}
