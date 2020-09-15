use super::consts::FILENAMES;
use super::file::*;
use crate::layer::{
    BaseLayer, ChildLayer, InternalLayer, Layer, LayerBuilder, LayerType, SimpleLayerBuilder,
};
use std::io;
use std::sync::{Arc, Weak};

use futures::future;
use futures::prelude::*;
use std::sync::RwLock;

use std::collections::HashMap;

pub trait LayerCache: 'static + Send + Sync {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>>;
    fn cache_layer(&self, layer: Arc<InternalLayer>);
}

pub struct NoCache;

impl LayerCache for NoCache {
    fn get_layer_from_cache(&self, _name: [u32; 5]) -> Option<Arc<InternalLayer>> {
        None
    }

    fn cache_layer(&self, _layer: Arc<InternalLayer>) {}
}

lazy_static! {
    static ref NOCACHE: Arc<dyn LayerCache> = Arc::new(NoCache);
}

pub trait LayerStore: 'static + Send + Sync {
    fn layers(&self) -> Box<dyn Future<Item = Vec<[u32; 5]>, Error = io::Error> + Send>;
    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Option<Arc<dyn Layer>>, Error = io::Error> + Send>;
    fn get_layer(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = Option<Arc<dyn Layer>>, Error = io::Error> + Send> {
        self.get_layer_with_cache(name, NOCACHE.clone())
    }

    fn create_base_layer(
        &self,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send>;
    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send>;
    fn create_child_layer(
        &self,
        parent: [u32; 5],
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        self.create_child_layer_with_cache(parent, NOCACHE.clone())
    }

    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8>;
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error>;

    fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> Box<dyn Future<Item = bool, Error = io::Error> + Send>;
}

pub trait PersistentLayerStore: 'static + Send + Sync + Clone {
    type File: FileLoad + FileStore + Clone;
    fn directories(&self) -> Box<dyn Future<Item = Vec<[u32; 5]>, Error = io::Error> + Send>;
    fn create_directory(&self) -> Box<dyn Future<Item = [u32; 5], Error = io::Error> + Send>;
    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8>;
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error>;

    fn directory_exists(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = bool, Error = io::Error> + Send>;
    fn get_file(
        &self,
        directory: [u32; 5],
        name: &str,
    ) -> Box<dyn Future<Item = Self::File, Error = io::Error> + Send>;
    fn file_exists(
        &self,
        directory: [u32; 5],
        file: &str,
    ) -> Box<dyn Future<Item = bool, Error = io::Error> + Send>;

    fn layer_type(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = LayerType, Error = io::Error> + Send> {
        Box::new(self.file_exists(name, FILENAMES.parent).map(|b| match b {
            true => LayerType::Child,
            false => LayerType::Base,
        }))
    }

    fn base_layer_files(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = BaseLayerFiles<Self::File>, Error = io::Error> + Send> {
        let filenames = vec![
            FILENAMES.node_dictionary_blocks,
            FILENAMES.node_dictionary_offsets,
            FILENAMES.predicate_dictionary_blocks,
            FILENAMES.predicate_dictionary_offsets,
            FILENAMES.value_dictionary_blocks,
            FILENAMES.value_dictionary_offsets,
            FILENAMES.base_subjects,
            FILENAMES.base_objects,
            FILENAMES.base_s_p_adjacency_list_bits,
            FILENAMES.base_s_p_adjacency_list_bit_index_blocks,
            FILENAMES.base_s_p_adjacency_list_bit_index_sblocks,
            FILENAMES.base_s_p_adjacency_list_nums,
            FILENAMES.base_sp_o_adjacency_list_bits,
            FILENAMES.base_sp_o_adjacency_list_bit_index_blocks,
            FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks,
            FILENAMES.base_sp_o_adjacency_list_nums,
            FILENAMES.base_o_ps_adjacency_list_bits,
            FILENAMES.base_o_ps_adjacency_list_bit_index_blocks,
            FILENAMES.base_o_ps_adjacency_list_bit_index_sblocks,
            FILENAMES.base_o_ps_adjacency_list_nums,
            FILENAMES.base_predicate_wavelet_tree_bits,
            FILENAMES.base_predicate_wavelet_tree_bit_index_blocks,
            FILENAMES.base_predicate_wavelet_tree_bit_index_sblocks,
        ];

        let clone = self.clone();

        Box::new(
            future::join_all(filenames.into_iter().map(move |f| clone.get_file(name, f))).map(
                |files| BaseLayerFiles {
                    node_dictionary_files: DictionaryFiles {
                        blocks_file: files[0].clone(),
                        offsets_file: files[1].clone(),
                    },
                    predicate_dictionary_files: DictionaryFiles {
                        blocks_file: files[2].clone(),
                        offsets_file: files[3].clone(),
                    },
                    value_dictionary_files: DictionaryFiles {
                        blocks_file: files[4].clone(),
                        offsets_file: files[5].clone(),
                    },

                    subjects_file: files[6].clone(),
                    objects_file: files[7].clone(),

                    s_p_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[8].clone(),
                            blocks_file: files[9].clone(),
                            sblocks_file: files[10].clone(),
                        },
                        nums_file: files[11].clone(),
                    },
                    sp_o_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[12].clone(),
                            blocks_file: files[13].clone(),
                            sblocks_file: files[14].clone(),
                        },
                        nums_file: files[15].clone(),
                    },
                    o_ps_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[16].clone(),
                            blocks_file: files[17].clone(),
                            sblocks_file: files[18].clone(),
                        },
                        nums_file: files[19].clone(),
                    },
                    predicate_wavelet_tree_files: BitIndexFiles {
                        bits_file: files[20].clone(),
                        blocks_file: files[21].clone(),
                        sblocks_file: files[22].clone(),
                    },
                },
            ),
        )
    }

    fn child_layer_files(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = ChildLayerFiles<Self::File>, Error = io::Error> + Send> {
        let filenames = vec![
            FILENAMES.node_dictionary_blocks,
            FILENAMES.node_dictionary_offsets,
            FILENAMES.predicate_dictionary_blocks,
            FILENAMES.predicate_dictionary_offsets,
            FILENAMES.value_dictionary_blocks,
            FILENAMES.value_dictionary_offsets,
            FILENAMES.pos_subjects,
            FILENAMES.pos_objects,
            FILENAMES.neg_subjects,
            FILENAMES.neg_objects,
            FILENAMES.pos_s_p_adjacency_list_bits,
            FILENAMES.pos_s_p_adjacency_list_bit_index_blocks,
            FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks,
            FILENAMES.pos_s_p_adjacency_list_nums,
            FILENAMES.pos_sp_o_adjacency_list_bits,
            FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks,
            FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks,
            FILENAMES.pos_sp_o_adjacency_list_nums,
            FILENAMES.pos_o_ps_adjacency_list_bits,
            FILENAMES.pos_o_ps_adjacency_list_bit_index_blocks,
            FILENAMES.pos_o_ps_adjacency_list_bit_index_sblocks,
            FILENAMES.pos_o_ps_adjacency_list_nums,
            FILENAMES.neg_s_p_adjacency_list_bits,
            FILENAMES.neg_s_p_adjacency_list_bit_index_blocks,
            FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks,
            FILENAMES.neg_s_p_adjacency_list_nums,
            FILENAMES.neg_sp_o_adjacency_list_bits,
            FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks,
            FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks,
            FILENAMES.neg_sp_o_adjacency_list_nums,
            FILENAMES.neg_o_ps_adjacency_list_bits,
            FILENAMES.neg_o_ps_adjacency_list_bit_index_blocks,
            FILENAMES.neg_o_ps_adjacency_list_bit_index_sblocks,
            FILENAMES.neg_o_ps_adjacency_list_nums,
            FILENAMES.pos_predicate_wavelet_tree_bits,
            FILENAMES.pos_predicate_wavelet_tree_bit_index_blocks,
            FILENAMES.pos_predicate_wavelet_tree_bit_index_sblocks,
            FILENAMES.neg_predicate_wavelet_tree_bits,
            FILENAMES.neg_predicate_wavelet_tree_bit_index_blocks,
            FILENAMES.neg_predicate_wavelet_tree_bit_index_sblocks,
        ];

        let cloned = self.clone();

        Box::new(
            future::join_all(filenames.into_iter().map(move |f| cloned.get_file(name, f))).map(
                |files| ChildLayerFiles {
                    node_dictionary_files: DictionaryFiles {
                        blocks_file: files[0].clone(),
                        offsets_file: files[1].clone(),
                    },
                    predicate_dictionary_files: DictionaryFiles {
                        blocks_file: files[2].clone(),
                        offsets_file: files[3].clone(),
                    },
                    value_dictionary_files: DictionaryFiles {
                        blocks_file: files[4].clone(),
                        offsets_file: files[5].clone(),
                    },

                    pos_subjects_file: files[6].clone(),
                    pos_objects_file: files[7].clone(),
                    neg_subjects_file: files[8].clone(),
                    neg_objects_file: files[9].clone(),

                    pos_s_p_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[10].clone(),
                            blocks_file: files[11].clone(),
                            sblocks_file: files[12].clone(),
                        },
                        nums_file: files[13].clone(),
                    },
                    pos_sp_o_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[14].clone(),
                            blocks_file: files[15].clone(),
                            sblocks_file: files[16].clone(),
                        },
                        nums_file: files[17].clone(),
                    },
                    pos_o_ps_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[18].clone(),
                            blocks_file: files[19].clone(),
                            sblocks_file: files[20].clone(),
                        },
                        nums_file: files[21].clone(),
                    },
                    neg_s_p_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[22].clone(),
                            blocks_file: files[23].clone(),
                            sblocks_file: files[24].clone(),
                        },
                        nums_file: files[25].clone(),
                    },
                    neg_sp_o_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[26].clone(),
                            blocks_file: files[27].clone(),
                            sblocks_file: files[28].clone(),
                        },
                        nums_file: files[29].clone(),
                    },
                    neg_o_ps_adjacency_list_files: AdjacencyListFiles {
                        bitindex_files: BitIndexFiles {
                            bits_file: files[30].clone(),
                            blocks_file: files[31].clone(),
                            sblocks_file: files[32].clone(),
                        },
                        nums_file: files[33].clone(),
                    },
                    pos_predicate_wavelet_tree_files: BitIndexFiles {
                        bits_file: files[34].clone(),
                        blocks_file: files[35].clone(),
                        sblocks_file: files[36].clone(),
                    },
                    neg_predicate_wavelet_tree_files: BitIndexFiles {
                        bits_file: files[37].clone(),
                        blocks_file: files[38].clone(),
                        sblocks_file: files[39].clone(),
                    },
                },
            ),
        )
    }

    fn write_parent_file(
        &self,
        dir_name: [u32; 5],
        parent_name: [u32; 5],
    ) -> Box<dyn Future<Item = (), Error = std::io::Error> + Send> {
        let parent_string = name_to_string(parent_name);

        Box::new(
            self.get_file(dir_name, FILENAMES.parent)
                .map(|f| f.open_write())
                .and_then(|writer| tokio::io::write_all(writer, parent_string))
                .map(|_| ()),
        )
    }

    fn read_parent_file(
        &self,
        dir_name: [u32; 5],
    ) -> Box<dyn Future<Item = [u32; 5], Error = std::io::Error> + Send> {
        Box::new(
            self.get_file(dir_name, FILENAMES.parent)
                .map(|f| f.open_read())
                .and_then(|reader| tokio::io::read_exact(reader, vec![0; 40]))
                .and_then(|(_, buf)| bytes_to_name(&buf)),
        )
    }

    fn retrieve_layer_stack_names(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = Vec<[u32; 5]>, Error = std::io::Error> + Send> {
        let cloned = self.clone();
        let mut result = Vec::new();
        result.push(name);
        Box::new(future::loop_fn(
            (cloned, result),
            |(retriever, mut result)| {
                retriever
                    .layer_type(*result.last().unwrap())
                    .and_then(|t| match t {
                        LayerType::Base => {
                            result.reverse();
                            future::Either::A(future::ok(future::Loop::Break(result)))
                        }
                        LayerType::Child => future::Either::B(
                            retriever
                                .read_parent_file(*result.last().unwrap())
                                .map(|p| {
                                    result.push(p);
                                    future::Loop::Continue((retriever, result))
                                }),
                        ),
                    })
            },
        ))
    }
}

pub fn name_to_string(name: [u32; 5]) -> String {
    format!(
        "{:08x}{:08x}{:08x}{:08x}{:08x}",
        name[0], name[1], name[2], name[3], name[4]
    )
}

pub fn string_to_name(string: &str) -> Result<[u32; 5], std::io::Error> {
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

    Ok([n1, n2, n3, n4, n5])
}

pub fn bytes_to_name(bytes: &Vec<u8>) -> Result<[u32; 5], std::io::Error> {
    if bytes.len() != 40 {
        Err(io::Error::new(io::ErrorKind::Other, "bytes not len 40"))
    } else {
        let string = String::from_utf8_lossy(&bytes);

        string_to_name(&string)
    }
}

impl<F: 'static + FileLoad + FileStore + Clone, T: 'static + PersistentLayerStore<File = F>>
    LayerStore for T
{
    fn layers(&self) -> Box<dyn Future<Item = Vec<[u32; 5]>, Error = io::Error> + Send> {
        self.directories()
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Option<Arc<dyn Layer>>, Error = io::Error> + Send> {
        if let Some(layer) = cache.get_layer_from_cache(name) {
            return Box::new(future::ok(Some(layer as Arc<dyn Layer>)));
        }

        let cloned = self.clone();
        let cloned2 = self.clone();
        let mut result = Vec::new();
        result.push(name);
        Box::new(
            self.directory_exists(name)
                .and_then(move |b| {
                    match b {
                        false => future::Either::A(future::ok(None)),
                        true => future::Either::B(
                            future::loop_fn(
                                (cloned.clone(), cache, result),
                                |(retriever, cache, mut result)| {
                                    match cache.get_layer_from_cache(*result.last().unwrap()) {
                                        None => future::Either::A(
                                            retriever.layer_type(*result.last().unwrap()).and_then(
                                                |t| match t {
                                                    LayerType::Base => future::Either::A(
                                                        future::ok(future::Loop::Break((
                                                            None, result, cache,
                                                        ))),
                                                    ),
                                                    LayerType::Child => future::Either::B(
                                                        retriever
                                                            .read_parent_file(
                                                                *result.last().unwrap(),
                                                            )
                                                            .map(|p| {
                                                                result.push(p);
                                                                future::Loop::Continue((
                                                                    retriever, cache, result,
                                                                ))
                                                            }),
                                                    ),
                                                },
                                            ),
                                        ),
                                        Some(layer) => {
                                            // remove found cached layer from ids to retrieve
                                            result.pop().unwrap();
                                            future::Either::B(future::ok(future::Loop::Break((
                                                Some(layer),
                                                result,
                                                cache,
                                            ))))
                                        }
                                    }
                                },
                            )
                            .and_then(move |(layer, mut ids, cache)| match layer {
                                Some(layer) => {
                                    ids.reverse();
                                    future::Either::A(future::ok((layer, ids, cache)))
                                }
                                None => {
                                    let base = ids.pop().unwrap();
                                    ids.reverse();
                                    future::Either::B(
                                        cloned
                                            .base_layer_files(base)
                                            .and_then(move |files| {
                                                BaseLayer::load_from_files(base, &files)
                                            })
                                            .map(move |l| {
                                                let result =
                                                    Arc::new(l.into()) as Arc<InternalLayer>;
                                                cache.cache_layer(result.clone());
                                                (result, ids, cache)
                                            }),
                                    )
                                }
                            })
                            .and_then(|(parent, ids, cache)| {
                                futures::stream::iter_ok(ids)
                                    .fold(parent, move |parent, id| {
                                        let cache = cache.clone();
                                        cloned2
                                            .child_layer_files(id)
                                            .and_then(move |files| {
                                                ChildLayer::load_from_files(id, parent, &files)
                                            })
                                            .map(move |l| {
                                                let result =
                                                    Arc::new(l.into()) as Arc<InternalLayer>;
                                                cache.cache_layer(result.clone());
                                                result
                                            })
                                    })
                                    .map(move |l| Some(l))
                            }),
                        ),
                    }
                })
                .map(|l| l.map(|layer| layer as Arc<dyn Layer>)),
        )
    }

    fn create_base_layer(
        &self,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        let cloned = self.clone();
        Box::new(self.create_directory().and_then(move |dir_name| {
            cloned.base_layer_files(dir_name).map(move |blf| {
                Box::new(SimpleLayerBuilder::new(dir_name, blf)) as Box<dyn LayerBuilder>
            })
        }))
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        let cloned = self.clone();
        Box::new(
            self.get_layer_with_cache(parent, cache)
                .and_then(|parent_layer| match parent_layer {
                    None => Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "parent layer not found",
                    )),
                    Some(parent_layer) => Ok(parent_layer),
                })
                .and_then(move |parent_layer| {
                    cloned.create_directory().and_then(move |dir_name| {
                        cloned
                            .write_parent_file(dir_name, parent)
                            .and_then(move |_| {
                                cloned.child_layer_files(dir_name).map(move |clf| {
                                    Box::new(SimpleLayerBuilder::from_parent(
                                        dir_name,
                                        parent_layer,
                                        clf,
                                    )) as Box<dyn LayerBuilder>
                                })
                            })
                    })
                }),
        )
    }
    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        Self::export_layers(self, layer_ids)
    }
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        Self::import_layers(self, pack, layer_ids)
    }

    fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> Box<dyn Future<Item = bool, Error = io::Error> + Send> {
        let cloned = self.clone();
        Box::new(future::loop_fn(
            (cloned, descendant),
            move |(retriever, descendant)| {
                if ancestor == descendant {
                    future::Either::A(future::ok(future::Loop::Break(true)))
                } else {
                    future::Either::B(
                        retriever
                            .read_parent_file(descendant)
                            .map(|parent| future::Loop::Continue((retriever, parent)))
                            .or_else(|e| {
                                if e.kind() == io::ErrorKind::NotFound {
                                    future::ok(future::Loop::Break(false))
                                } else {
                                    future::err(e)
                                }
                            }),
                    )
                }
            },
        ))
    }
}

// locking isn't really ideal but the lock window will be relatively small so it shouldn't hurt performance too much except on heavy updates.
// ideally we should be using some concurrent hashmap implementation instead.
// furthermore, there should be some logic to remove stale entries, like a periodic pass. right now, there isn't.
pub struct LockingHashMapLayerCache {
    cache: RwLock<HashMap<[u32; 5], Weak<InternalLayer>>>,
}

impl LockingHashMapLayerCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }
}

impl LayerCache for LockingHashMapLayerCache {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>> {
        let cache = self
            .cache
            .read()
            .expect("rwlock read should always succeed");

        let result = cache.get(&name).map(|c| c.to_owned());
        std::mem::drop(cache);

        match result {
            None => None,
            Some(weak) => match weak.upgrade() {
                None => {
                    self.cache
                        .write()
                        .expect("rwlock write should always succeed")
                        .remove(&name);
                    None
                }
                Some(result) => Some(result),
            },
        }
    }

    fn cache_layer(&self, layer: Arc<InternalLayer>) {
        let mut cache = self
            .cache
            .write()
            .expect("rwlock write should always succeed");
        cache.insert(layer.name(), Arc::downgrade(&layer));
    }
}

#[derive(Clone)]
pub struct CachedLayerStore {
    inner: Arc<dyn LayerStore>,
    cache: Arc<dyn LayerCache>,
}

impl CachedLayerStore {
    pub fn new<S: LayerStore, C: LayerCache>(inner: S, cache: C) -> CachedLayerStore {
        CachedLayerStore {
            inner: Arc::new(inner),
            cache: Arc::new(cache),
        }
    }
}

impl LayerStore for CachedLayerStore {
    fn layers(&self) -> Box<dyn Future<Item = Vec<[u32; 5]>, Error = io::Error> + Send> {
        self.inner.layers()
    }

    fn get_layer(
        &self,
        name: [u32; 5],
    ) -> Box<dyn Future<Item = Option<Arc<dyn Layer>>, Error = io::Error> + Send> {
        self.inner.get_layer_with_cache(name, self.cache.clone())
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Option<Arc<dyn Layer>>, Error = io::Error> + Send> {
        self.inner.get_layer_with_cache(name, cache)
    }

    fn create_base_layer(
        &self,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        self.inner.create_base_layer()
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        self.inner.create_child_layer_with_cache(parent, cache)
    }

    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        self.inner.export_layers(layer_ids)
    }
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        self.inner.import_layers(pack, layer_ids)
    }

    fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> Box<dyn Future<Item = bool, Error = io::Error> + Send> {
        self.inner.layer_is_ancestor_of(descendant, ancestor)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::directory::*;
    use crate::storage::memory::*;
    use futures::sync::oneshot;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    fn cached_layer_eq(layer1: &dyn Layer, layer2: &dyn Layer) -> bool {
        // a trait object consists of two parts, a pointer to the concrete data, followed by a vtable.
        // we consider two layers equal if that first part, the pointer to the concrete data, is equal.
        unsafe {
            let ptr1 = *(layer1 as *const dyn Layer as *const usize);
            let ptr2 = *(layer2 as *const dyn Layer as *const usize);
            ptr1 == ptr2
        }
    }

    #[test]
    fn cached_memory_layer_store_returns_same_layer_multiple_times() {
        let runtime = Runtime::new().unwrap();
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        let layer1 = store.get_layer(child_name).wait().unwrap().unwrap();
        let layer2 = store.get_layer(child_name).wait().unwrap().unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = store.get_layer(base_name).wait().unwrap().unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[test]
    fn cached_directory_layer_store_returns_same_layer_multiple_times() {
        let dir = tempdir().unwrap();
        let runtime = Runtime::new().unwrap();
        let store = CachedLayerStore::new(
            DirectoryLayerStore::new(dir.path()),
            LockingHashMapLayerCache::new(),
        );
        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = oneshot::spawn(store.create_child_layer(base_name), &runtime.executor())
            .wait()
            .unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        let layer1 = oneshot::spawn(store.get_layer(child_name), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();
        let layer2 = oneshot::spawn(store.get_layer(child_name), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = oneshot::spawn(store.get_layer(base_name), &runtime.executor())
            .wait()
            .unwrap()
            .unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[test]
    fn cached_layer_store_forgets_entries_when_they_are_dropped() {
        let runtime = Runtime::new().unwrap();
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = oneshot::spawn(store.create_base_layer(), &runtime.executor())
            .wait()
            .unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

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

    #[test]
    fn retrieve_layer_stack_names_retrieves_correctly() {
        //let store = CachedLayerStore::new(MemoryLayerStore::new());
        //let builder = store.create_base_layer().wait().unwrap();
    }
}
