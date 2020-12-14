use super::consts::FILENAMES;
use super::file::*;
use crate::layer::{
    BaseLayer, ChildLayer, InternalLayer, Layer, LayerBuilder, LayerType, SimpleLayerBuilder, delta_rollup, delta_rollup_upto
};
use std::io;
use std::sync::{Arc, Weak};

use futures::future::{self, Future};
use std::sync::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::collections::HashMap;
use std::pin::Pin;

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
    fn layers(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>>;
    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>>;
    fn get_layer(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        self.get_layer_with_cache(name, NOCACHE.clone())
    }

    fn create_base_layer(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>>;
    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>>;
    fn create_child_layer(
        &self,
        parent: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.create_child_layer_with_cache(parent, NOCACHE.clone())
    }

    fn perform_rollup(&self, layer: Arc<InternalLayer>) -> Pin<Box<dyn Future<Output=io::Result<[u32;5]>>+Send>>;
    fn perform_rollup_upto(&self, layer: Arc<InternalLayer>, upto: [u32;5], cache: Arc<dyn LayerCache>) -> Pin<Box<dyn Future<Output=io::Result<[u32;5]>>+Send>>;
    fn register_rollup(&self, layer: [u32;5], rollup: [u32;5]) -> Pin<Box<dyn Future<Output = io::Result<()>>+Send>>;

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
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>>;
}

pub trait PersistentLayerStore: 'static + Send + Sync + Clone {
    type File: FileLoad + FileStore + Clone;
    fn directories(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>>;
    fn create_directory(&self) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>>;
    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8>;
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error>;

    fn directory_exists(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>>;
    fn get_file(
        &self,
        directory: [u32; 5],
        name: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::File>> + Send>>;
    fn file_exists(
        &self,
        directory: [u32; 5],
        file: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>>;

    fn layer_type(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<LayerType>> + Send>> {
        let file_exists = self.file_exists(name, FILENAMES.parent);
        Box::pin(async {
            if file_exists.await? {
                Ok(LayerType::Child)
            } else {
                Ok(LayerType::Base)
            }
        })
    }

    fn base_layer_files(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<BaseLayerFiles<Self::File>>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            let filenames = vec![
                FILENAMES.node_dictionary_blocks,
                FILENAMES.node_dictionary_offsets,
                FILENAMES.predicate_dictionary_blocks,
                FILENAMES.predicate_dictionary_offsets,
                FILENAMES.value_dictionary_blocks,
                FILENAMES.value_dictionary_offsets,
                FILENAMES.node_value_idmap_bits,
                FILENAMES.node_value_idmap_bit_index_blocks,
                FILENAMES.node_value_idmap_bit_index_sblocks,
                FILENAMES.predicate_idmap_bits,
                FILENAMES.predicate_idmap_bit_index_blocks,
                FILENAMES.predicate_idmap_bit_index_sblocks,
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

            let mut files = Vec::with_capacity(filenames.len());

            for filename in filenames {
                files.push(self_.get_file(name, filename).await?);
            }

            Ok(BaseLayerFiles {
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

                id_map_files: IdMapFiles {
                    node_value_idmap_files: BitIndexFiles {
                        bits_file: files[6].clone(),
                        blocks_file: files[7].clone(),
                        sblocks_file: files[8].clone(),
                    },
                    predicate_idmap_files: BitIndexFiles {
                        bits_file: files[9].clone(),
                        blocks_file: files[10].clone(),
                        sblocks_file: files[11].clone(),
                    },
                },

                subjects_file: files[12].clone(),
                objects_file: files[13].clone(),

                s_p_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[14].clone(),
                        blocks_file: files[15].clone(),
                        sblocks_file: files[16].clone(),
                    },
                    nums_file: files[17].clone(),
                },
                sp_o_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[18].clone(),
                        blocks_file: files[19].clone(),
                        sblocks_file: files[20].clone(),
                    },
                    nums_file: files[21].clone(),
                },
                o_ps_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[22].clone(),
                        blocks_file: files[23].clone(),
                        sblocks_file: files[24].clone(),
                    },
                    nums_file: files[25].clone(),
                },
                predicate_wavelet_tree_files: BitIndexFiles {
                    bits_file: files[26].clone(),
                    blocks_file: files[27].clone(),
                    sblocks_file: files[28].clone(),
                },
            })
        })
    }

    fn child_layer_files(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<ChildLayerFiles<Self::File>>> + Send>> {
        let self_ = self.clone();

        Box::pin(async move {
            let filenames = vec![
                FILENAMES.node_dictionary_blocks,
                FILENAMES.node_dictionary_offsets,
                FILENAMES.predicate_dictionary_blocks,
                FILENAMES.predicate_dictionary_offsets,
                FILENAMES.value_dictionary_blocks,
                FILENAMES.value_dictionary_offsets,
                FILENAMES.node_value_idmap_bits,
                FILENAMES.node_value_idmap_bit_index_blocks,
                FILENAMES.node_value_idmap_bit_index_sblocks,
                FILENAMES.predicate_idmap_bits,
                FILENAMES.predicate_idmap_bit_index_blocks,
                FILENAMES.predicate_idmap_bit_index_sblocks,
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

            let mut files = Vec::with_capacity(filenames.len());
            for filename in filenames {
                files.push(self_.get_file(name, filename).await?);
            }

            Ok(ChildLayerFiles {
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

                id_map_files: IdMapFiles {
                    node_value_idmap_files: BitIndexFiles {
                        bits_file: files[6].clone(),
                        blocks_file: files[7].clone(),
                        sblocks_file: files[8].clone(),
                    },
                    predicate_idmap_files: BitIndexFiles {
                        bits_file: files[9].clone(),
                        blocks_file: files[10].clone(),
                        sblocks_file: files[11].clone(),
                    },
                },

                pos_subjects_file: files[12].clone(),
                pos_objects_file: files[13].clone(),
                neg_subjects_file: files[14].clone(),
                neg_objects_file: files[15].clone(),

                pos_s_p_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[16].clone(),
                        blocks_file: files[17].clone(),
                        sblocks_file: files[18].clone(),
                    },
                    nums_file: files[19].clone(),
                },
                pos_sp_o_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[20].clone(),
                        blocks_file: files[21].clone(),
                        sblocks_file: files[22].clone(),
                    },
                    nums_file: files[23].clone(),
                },
                pos_o_ps_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[24].clone(),
                        blocks_file: files[25].clone(),
                        sblocks_file: files[26].clone(),
                    },
                    nums_file: files[27].clone(),
                },
                neg_s_p_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[28].clone(),
                        blocks_file: files[29].clone(),
                        sblocks_file: files[30].clone(),
                    },
                    nums_file: files[31].clone(),
                },
                neg_sp_o_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[32].clone(),
                        blocks_file: files[33].clone(),
                        sblocks_file: files[34].clone(),
                    },
                    nums_file: files[35].clone(),
                },
                neg_o_ps_adjacency_list_files: AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: files[36].clone(),
                        blocks_file: files[37].clone(),
                        sblocks_file: files[38].clone(),
                    },
                    nums_file: files[39].clone(),
                },
                pos_predicate_wavelet_tree_files: BitIndexFiles {
                    bits_file: files[40].clone(),
                    blocks_file: files[41].clone(),
                    sblocks_file: files[42].clone(),
                },
                neg_predicate_wavelet_tree_files: BitIndexFiles {
                    bits_file: files[43].clone(),
                    blocks_file: files[44].clone(),
                    sblocks_file: files[45].clone(),
                },
            })
        })
    }

    fn write_parent_file(
        &self,
        dir_name: [u32; 5],
        parent_name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let parent_string = name_to_string(parent_name);

        let get_file = self.get_file(dir_name, FILENAMES.parent);
        Box::pin(async move {
            let file = get_file.await?;
            let mut writer = file.open_write();

            writer.write_all(parent_string.as_bytes()).await?;

            Ok(())
        })
    }

    fn read_parent_file(
        &self,
        dir_name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let get_file = self.get_file(dir_name, FILENAMES.parent);
        Box::pin(async move {
            let file = get_file.await?;
            let mut reader = file.open_read();

            let mut buf = [0; 40];
            reader.read_exact(&mut buf).await?;

            bytes_to_name(&buf)
        })
    }

    // TODO this should check if the rollup is better than what is there
    fn write_rollup_file(
        &self,
        dir_name: [u32;5],
        rollup_name: [u32;5]
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let rollup_string = name_to_string(rollup_name);

        let get_file = self.get_file(dir_name, FILENAMES.rollup);
        Box::pin(async move {
            let file = get_file.await?;
            let mut writer = file.open_write();

            writer.write_all(rollup_string.as_bytes()).await?;

            Ok(())
        })
    }

    fn read_rollup_file(
        &self,
        dir_name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let get_file = self.get_file(dir_name, FILENAMES.rollup);
        Box::pin(async move {
            let file = get_file.await?;
            let mut reader = file.open_read();

            let mut buf = [0; 40];
            reader.read_exact(&mut buf).await?;

            bytes_to_name(&buf)
        })
    }

    fn retrieve_layer_stack_names(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let self_ = self.clone();
        let mut result = Vec::new();
        result.push(name);

        Box::pin(async move {
            loop {
                match self_.layer_type(*result.last().unwrap()).await? {
                    LayerType::Base => {
                        result.reverse();

                        return Ok(result);
                    }
                    LayerType::Child => {
                        let parent = self_.read_parent_file(*result.last().unwrap()).await?;
                        result.push(parent);
                    }
                }
            }
        })
    }

    fn create_child_layer_files_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<([u32;5], Arc<InternalLayer>, ChildLayerFiles<Self::File>)>>+Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            let parent_layer = match self_.get_layer_with_cache(parent, cache).await? {
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "parent layer not found",
                    ))
                }
                Some(parent_layer) => Ok::<_, io::Error>(parent_layer),
            }?;

            let layer_dir = self_.create_directory().await?;
            self_.write_parent_file(layer_dir, parent).await?;
            let child_layer_files = self_.child_layer_files(layer_dir).await?;

            Ok((layer_dir, parent_layer, child_layer_files))
        })
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

pub fn bytes_to_name(bytes: &[u8]) -> Result<[u32; 5], std::io::Error> {
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
    fn layers(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        self.directories()
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        if let Some(layer) = cache.get_layer_from_cache(name) {
            return Box::pin(future::ok(Some(layer)));
        }

        let mut layers_to_load = Vec::new();
        layers_to_load.push(name);
        let self_ = self.clone();
        Box::pin(async move {
            if !self_.directory_exists(name).await? {
                return Ok(None);
            }

            // find an ancestor in cache
            let mut ancestor = None;
            loop {
                match cache.get_layer_from_cache(*layers_to_load.last().unwrap()) {
                    Some(layer) => {
                        // remove found cached layer from ids to retrieve
                        layers_to_load.pop().unwrap();
                        ancestor = Some(layer);
                        break;
                    }
                    None => {
                        match self_.layer_type(*layers_to_load.last().unwrap()).await? {
                            LayerType::Base => break, // we got all the way to the base layer without finding a cached version
                            LayerType::Child => {
                                let parent = self_
                                    .read_parent_file(*layers_to_load.last().unwrap())
                                    .await?;
                                layers_to_load.push(parent);
                            }
                        }
                    }
                }
            }

            if ancestor.is_none() {
                // load the base layer
                let base_id = layers_to_load.pop().unwrap();
                let files = self_.base_layer_files(base_id).await?;
                let base_layer: Arc<InternalLayer> =
                    Arc::new(BaseLayer::load_from_files(base_id, &files).await?.into());

                cache.cache_layer(base_layer.clone());
                ancestor = Some(base_layer);
            }

            let mut ancestor = ancestor.unwrap();
            layers_to_load.reverse();

            for layer_id in layers_to_load {
                let files = self_.child_layer_files(layer_id).await?;
                let child_layer: Arc<InternalLayer> = Arc::new(
                    ChildLayer::load_from_files(layer_id, ancestor, &files)
                        .await?
                        .into(),
                );

                cache.cache_layer(child_layer.clone());
                ancestor = child_layer;
            }

            Ok(Some(ancestor))
        })
    }

    fn create_base_layer(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            let dir_name = self_.create_directory().await?;
            let files = self_.base_layer_files(dir_name).await?;
            Ok(Box::new(SimpleLayerBuilder::new(dir_name, files)) as Box<dyn LayerBuilder>)
        })
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        let create_files = self.create_child_layer_files_with_cache(parent, cache);
        Box::pin(async move {
            let (layer_dir, parent_layer, child_layer_files) = create_files.await?;

            
            Ok(Box::new(SimpleLayerBuilder::from_parent(
                layer_dir,
                parent_layer,
                child_layer_files,
            )) as Box<dyn LayerBuilder>)
        })
    }

    fn perform_rollup(&self, layer: Arc<InternalLayer>) -> Pin<Box<dyn Future<Output=io::Result<[u32;5]>>+Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            let dir_name = self_.create_directory().await?;
            let files = self_.base_layer_files(dir_name).await?;
            delta_rollup(&layer, files).await?;

            Ok(dir_name)
        })
    }

    fn perform_rollup_upto(&self, layer: Arc<InternalLayer>, upto: [u32;5], cache: Arc<dyn LayerCache>) -> Pin<Box<dyn Future<Output=io::Result<[u32;5]>>+Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            let (layer_dir, _parent_layer, child_layer_files) = self_.create_child_layer_files_with_cache(upto, cache).await?;
            delta_rollup_upto(&layer, upto, child_layer_files).await?;
            Ok(layer_dir)
        })
    }
    
    fn register_rollup(&self, layer: [u32;5], rollup: [u32;5]) -> Pin<Box<dyn Future<Output = io::Result<()>>+Send>> {
        self.write_rollup_file(layer, rollup)
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
        mut descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            loop {
                if ancestor == descendant {
                    return Ok(true);
                }

                let parent = self_.read_parent_file(descendant).await;
                match parent {
                    Ok(parent) => descendant = parent,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            return Ok(false);
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        })
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
    fn layers(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        self.inner.layers()
    }

    fn get_layer(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        self.inner.get_layer_with_cache(name, self.cache.clone())
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        self.inner.get_layer_with_cache(name, cache)
    }

    fn create_base_layer(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.inner.create_base_layer()
    }

    fn create_child_layer(
        &self,
        parent: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.inner
            .create_child_layer_with_cache(parent, self.cache.clone())
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.inner.create_child_layer_with_cache(parent, cache)
    }

    fn perform_rollup(&self, layer: Arc<InternalLayer>) -> Pin<Box<dyn Future<Output=io::Result<[u32;5]>>+Send>> {
        self.inner.perform_rollup(layer)
    }

    fn perform_rollup_upto(&self, layer: Arc<InternalLayer>, upto: [u32;5], cache: Arc<dyn LayerCache>) -> Pin<Box<dyn Future<Output=io::Result<[u32;5]>>+Send>> {
        self.inner.perform_rollup_upto(layer, upto, cache)
    }

    fn register_rollup(&self, layer: [u32;5], rollup: [u32;5]) -> Pin<Box<dyn Future<Output = io::Result<()>>+Send>> {
        self.inner.register_rollup(layer, rollup)
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
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        self.inner.layer_is_ancestor_of(descendant, ancestor)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::directory::*;
    use crate::storage::memory::*;
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
        let mut runtime = Runtime::new().unwrap();
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = runtime.block_on(store.create_base_layer()).unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        runtime.block_on(builder.commit_boxed()).unwrap();

        builder = runtime
            .block_on(store.create_child_layer(base_name))
            .unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        runtime.block_on(builder.commit_boxed()).unwrap();

        let layer1 = runtime
            .block_on(store.get_layer(child_name))
            .unwrap()
            .unwrap();
        let layer2 = runtime
            .block_on(store.get_layer(child_name))
            .unwrap()
            .unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = runtime
            .block_on(store.get_layer(base_name))
            .unwrap()
            .unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[test]
    fn cached_directory_layer_store_returns_same_layer_multiple_times() {
        let dir = tempdir().unwrap();
        let mut runtime = Runtime::new().unwrap();
        let store = CachedLayerStore::new(
            DirectoryLayerStore::new(dir.path()),
            LockingHashMapLayerCache::new(),
        );
        let mut builder = runtime.block_on(store.create_base_layer()).unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        runtime.block_on(builder.commit_boxed()).unwrap();

        builder = runtime
            .block_on(store.create_child_layer(base_name))
            .unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        runtime.block_on(builder.commit_boxed()).unwrap();

        let layer1 = runtime
            .block_on(store.get_layer(child_name))
            .unwrap()
            .unwrap();
        let layer2 = runtime
            .block_on(store.get_layer(child_name))
            .unwrap()
            .unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = runtime
            .block_on(store.get_layer(base_name))
            .unwrap()
            .unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[test]
    fn cached_layer_store_forgets_entries_when_they_are_dropped() {
        let mut runtime = Runtime::new().unwrap();
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = runtime.block_on(store.create_base_layer()).unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        runtime.block_on(builder.commit_boxed()).unwrap();

        let layer = runtime
            .block_on(store.get_layer(base_name))
            .unwrap()
            .unwrap();
        let weak = Arc::downgrade(&layer);

        // we expect 2 weak pointers, the one we made above and the one stored in cache
        assert_eq!(2, Arc::weak_count(&layer));

        // forget the layers
        std::mem::drop(layer);

        // according to our weak reference, there's no longer any strong reference around
        assert!(weak.upgrade().is_none());

        // retrieving the same layer again works just fine
        let layer = runtime
            .block_on(store.get_layer(base_name))
            .unwrap()
            .unwrap();

        // and only has one weak pointer pointing to it, the newly cached one
        assert_eq!(1, Arc::weak_count(&layer));
    }

    #[test]
    fn retrieve_layer_stack_names_retrieves_correctly() {
        //let store = CachedLayerStore::new(MemoryLayerStore::new());
        //let builder = store.create_base_layer().wait().unwrap();
    }
}
