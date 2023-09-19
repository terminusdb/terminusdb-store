use super::cache::*;
use super::consts::FILENAMES;
use super::delta::*;
use super::file::*;
use super::pack::Packable;
use crate::layer::base_merge::merge_base_layers;
use crate::layer::builder::DictionarySetFileBuilder;
use crate::layer::BaseLayerFileBuilder;
use crate::layer::ChildLayerFileBuilderPhase2;
use crate::layer::TripleChange;
use crate::layer::{
    layer_triple_exists, BaseLayer, ChildLayer, IdMap, IdTriple, InternalLayer,
    InternalLayerTripleObjectIterator, InternalLayerTriplePredicateIterator,
    InternalLayerTripleSubjectIterator, InternalTripleStackIterator, LayerBuilder,
    OptInternalLayerTriplePredicateIterator, OptInternalLayerTripleSubjectIterator, RollupLayer,
    SimpleLayerBuilder,
};
use crate::structure::bitarray::bitarray_len_from_file;
use crate::structure::dict_file_get_count;
use crate::structure::logarray::logarray_file_get_length_and_width;
use crate::structure::StringDict;
use crate::structure::TypedDict;
use crate::structure::{util, AdjacencyList, BitIndex, LogArray, MonotonicLogArray, WaveletTree};
use crate::Layer;

use bitvec::prelude::*;
use std::convert::TryInto;
use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use async_trait::async_trait;

macro_rules! walk_backwards_from_disk {
    ($store:ident, $name:ident, $current:ident, $body:block) => {
        let mut $current = $name;
        loop {
            $body

            if let Some(parent) = $store.get_layer_parent_name($current).await? {
                $current = parent;
            }
            else {
                break;
            }
        }
    }
}

macro_rules! walk_backwards_from_disk_upto {
    ($store:ident, $name:ident, $upto:ident, $current:ident, $body:block) => {
        let mut $current = $name;
        loop {
            if $current == $upto {
                break Ok(());
            }

            $body

            if let Some(parent) = $store.get_layer_parent_name($current).await? {
                $current = parent;
            }
            else {
                break Err(io::Error::new(io::ErrorKind::NotFound, "expected a parent layer, but it was not found"));
            }
        }?
    }
}

#[async_trait]
pub trait LayerStore: 'static + Packable + Send + Sync {
    async fn layers(&self) -> io::Result<Vec<[u32; 5]>>;
    async fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<Option<Arc<InternalLayer>>>;
    async fn get_layer(&self, name: [u32; 5]) -> io::Result<Option<Arc<InternalLayer>>> {
        self.get_layer_with_cache(name, NOCACHE.clone()).await
    }

    async fn finalize_layer(&self, _name: [u32; 5]) -> io::Result<()> {
        Ok(())
    }

    async fn get_layer_parent_name(&self, name: [u32; 5]) -> io::Result<Option<[u32; 5]>>;

    async fn get_node_dictionary(&self, name: [u32; 5]) -> io::Result<Option<StringDict>>;

    async fn get_predicate_dictionary(&self, name: [u32; 5]) -> io::Result<Option<StringDict>>;

    async fn get_value_dictionary(&self, name: [u32; 5]) -> io::Result<Option<TypedDict>>;

    async fn get_node_count(&self, name: [u32; 5]) -> io::Result<Option<u64>>;

    async fn get_predicate_count(&self, name: [u32; 5]) -> io::Result<Option<u64>>;

    async fn get_value_count(&self, name: [u32; 5]) -> io::Result<Option<u64>>;

    async fn get_node_value_idmap(&self, name: [u32; 5]) -> io::Result<Option<IdMap>>;

    async fn get_predicate_idmap(&self, name: [u32; 5]) -> io::Result<Option<IdMap>>;

    async fn create_base_layer(&self) -> io::Result<Box<dyn LayerBuilder>>;
    async fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<Box<dyn LayerBuilder>>;
    async fn create_child_layer(&self, parent: [u32; 5]) -> io::Result<Box<dyn LayerBuilder>> {
        self.create_child_layer_with_cache(parent, NOCACHE.clone())
            .await
    }

    async fn perform_rollup(&self, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]>;
    async fn perform_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]>;
    async fn perform_rollup_upto(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> io::Result<[u32; 5]> {
        self.perform_rollup_upto_with_cache(layer, upto, NOCACHE.clone())
            .await
    }
    async fn perform_imprecise_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]>;
    async fn perform_imprecise_rollup_upto(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> io::Result<[u32; 5]> {
        self.perform_rollup_upto_with_cache(layer, upto, NOCACHE.clone())
            .await
    }
    async fn register_rollup(&self, layer: [u32; 5], rollup: [u32; 5]) -> io::Result<()>;

    /// Create a new rollup layer which rolls up all triples in the given layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    async fn rollup(self: Arc<Self>, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]> {
        let name = layer.name();
        let rollup = self.perform_rollup(layer).await?;
        self.register_rollup(name, rollup).await?;

        Ok(rollup)
    }

    async fn rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]> {
        let name = layer.name();
        let rollup = self
            .perform_rollup_upto_with_cache(layer, upto, cache)
            .await?;
        self.register_rollup(name, rollup).await?;

        Ok(rollup)
    }

    /// Create a new rollup layer which rolls up all triples in the given layer, as well as all ancestors up to (but not including) the given ancestor.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    async fn rollup_upto(&self, layer: Arc<InternalLayer>, upto: [u32; 5]) -> io::Result<[u32; 5]> {
        self.rollup_upto_with_cache(layer, upto, NOCACHE.clone())
            .await
    }

    async fn imprecise_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]> {
        let name = layer.name();
        let rollup = self
            .perform_imprecise_rollup_upto_with_cache(layer, upto, cache)
            .await?;
        self.register_rollup(name, rollup).await?;

        Ok(rollup)
    }

    /// Create a new rollup layer which rolls up all triples in the given layer, as well as all ancestors up to (but not including) the given ancestor.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    async fn imprecise_rollup_upto(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> io::Result<[u32; 5]> {
        self.imprecise_rollup_upto_with_cache(layer, upto, NOCACHE.clone())
            .await
    }

    async fn squash(&self, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]>;
    async fn squash_upto(&self, layer: Arc<InternalLayer>, upto: [u32; 5]) -> io::Result<[u32; 5]>;

    async fn merge_base_layer(&self, layers: &[[u32; 5]], temp_dir: &Path) -> io::Result<[u32; 5]>;

    async fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> io::Result<bool>;

    async fn triple_addition_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool>;

    async fn triple_removal_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool>;

    async fn triple_additions(
        &self,
        layer: [u32; 5],
    ) -> io::Result<OptInternalLayerTripleSubjectIterator>;

    async fn triple_removals(
        &self,
        layer: [u32; 5],
    ) -> io::Result<OptInternalLayerTripleSubjectIterator>;

    async fn triple_additions_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_removals_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_additions_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_removals_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_additions_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_removals_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_additions_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_removals_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>;

    async fn triple_layer_addition_count(&self, layer: [u32; 5]) -> io::Result<usize>;

    async fn triple_layer_removal_count(&self, layer: [u32; 5]) -> io::Result<usize>;

    async fn retrieve_layer_stack_names(&self, name: [u32; 5]) -> io::Result<Vec<[u32; 5]>>;

    async fn retrieve_layer_stack_names_upto(
        &self,
        name: [u32; 5],
        upto: [u32; 5],
    ) -> io::Result<Vec<[u32; 5]>>;

    async fn layer_changes(&self, name: [u32; 5]) -> io::Result<InternalTripleStackIterator> {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        walk_backwards_from_disk!(self, name, current, {
            positives.push(self.triple_additions(name).await?);
            negatives.push(self.triple_removals(name).await?);
        });
        positives.reverse();
        negatives.reverse();

        Ok(InternalTripleStackIterator::from_parts(
            positives, negatives,
        ))
    }

    async fn layer_changes_upto(
        &self,
        name: [u32; 5],
        upto: [u32; 5],
    ) -> io::Result<InternalTripleStackIterator> {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        walk_backwards_from_disk_upto!(self, name, upto, current, {
            positives.push(self.triple_additions(current).await?);
            negatives.push(self.triple_removals(current).await?);
        });
        positives.reverse();
        negatives.reverse();

        Ok(InternalTripleStackIterator::from_parts(
            positives, negatives,
        ))
    }
}

#[async_trait]
pub trait PersistentLayerStore: 'static + Send + Sync + Clone {
    type File: FileLoad + FileStore + Clone;
    async fn directories(&self) -> io::Result<Vec<[u32; 5]>>;
    async fn create_named_directory(&self, id: [u32; 5]) -> io::Result<[u32; 5]>;
    async fn create_directory(&self) -> io::Result<[u32; 5]> {
        let name = rand::random();
        self.create_named_directory(name).await
    }

    async fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool>;
    async fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File>;
    async fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool>;

    async fn finalize(&self, _directory: [u32; 5]) -> io::Result<()> {
        Ok(())
    }

    async fn layer_has_rollup(&self, name: [u32; 5]) -> io::Result<bool> {
        self.file_exists(name, FILENAMES.rollup).await
    }

    async fn layer_has_parent(&self, name: [u32; 5]) -> io::Result<bool> {
        self.file_exists(name, FILENAMES.parent).await
    }

    async fn layer_parent(&self, name: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        if self.directory_exists(name).await? {
            if self.layer_has_parent(name).await? {
                let parent = self.read_parent_file(name).await?;
                Ok(Some(parent))
            } else {
                Ok(None)
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "parent layer not found",
            ))
        }
    }

    async fn base_layer_files(&self, name: [u32; 5]) -> io::Result<BaseLayerFiles<Self::File>> {
        let filenames = vec![
            FILENAMES.node_dictionary_blocks,
            FILENAMES.node_dictionary_offsets,
            FILENAMES.predicate_dictionary_blocks,
            FILENAMES.predicate_dictionary_offsets,
            FILENAMES.value_dictionary_types_present,
            FILENAMES.value_dictionary_type_offsets,
            FILENAMES.value_dictionary_offsets,
            FILENAMES.value_dictionary_blocks,
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
            FILENAMES.index_property_subjects,
            FILENAMES.index_property_adjacency_list_nums,
            FILENAMES.index_property_adjacency_list_bits,
            FILENAMES.index_property_adjacency_list_bit_index_blocks,
            FILENAMES.index_property_adjacency_list_bit_index_sblocks,
            FILENAMES.index_property_objects,
        ];

        let mut files = Vec::with_capacity(filenames.len());

        for filename in filenames {
            files.push(self.get_file(name, filename).await?);
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
            value_dictionary_files: TypedDictionaryFiles {
                types_present_file: files[4].clone(),
                type_offsets_file: files[5].clone(),
                offsets_file: files[6].clone(),
                blocks_file: files[7].clone(),
            },

            id_map_files: IdMapFiles {
                node_value_idmap_files: BitIndexFiles {
                    bits_file: files[8].clone(),
                    blocks_file: files[9].clone(),
                    sblocks_file: files[10].clone(),
                },
                predicate_idmap_files: BitIndexFiles {
                    bits_file: files[11].clone(),
                    blocks_file: files[12].clone(),
                    sblocks_file: files[13].clone(),
                },
            },

            subjects_file: files[14].clone(),
            objects_file: files[15].clone(),

            s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[16].clone(),
                    blocks_file: files[17].clone(),
                    sblocks_file: files[18].clone(),
                },
                nums_file: files[19].clone(),
            },
            sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[20].clone(),
                    blocks_file: files[21].clone(),
                    sblocks_file: files[22].clone(),
                },
                nums_file: files[23].clone(),
            },
            o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[24].clone(),
                    blocks_file: files[25].clone(),
                    sblocks_file: files[26].clone(),
                },
                nums_file: files[27].clone(),
            },
            predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[28].clone(),
                blocks_file: files[29].clone(),
                sblocks_file: files[30].clone(),
            },
            index_property_files: IndexPropertyFiles {
                subjects_logarray_file: files[31].clone(),
                adjacency_files: AdjacencyListFiles {
                    nums_file: files[32].clone(),
                    bitindex_files: BitIndexFiles {
                        bits_file: files[33].clone(),
                        blocks_file: files[34].clone(),
                        sblocks_file: files[35].clone(),
                    },
                },
                objects_logarray_file: files[36].clone(),
            },
        })
    }

    async fn child_layer_files(&self, name: [u32; 5]) -> io::Result<ChildLayerFiles<Self::File>> {
        let filenames = vec![
            FILENAMES.node_dictionary_blocks,
            FILENAMES.node_dictionary_offsets,
            FILENAMES.predicate_dictionary_blocks,
            FILENAMES.predicate_dictionary_offsets,
            FILENAMES.value_dictionary_types_present,
            FILENAMES.value_dictionary_type_offsets,
            FILENAMES.value_dictionary_offsets,
            FILENAMES.value_dictionary_blocks,
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
            files.push(self.get_file(name, filename).await?);
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
            value_dictionary_files: TypedDictionaryFiles {
                types_present_file: files[4].clone(),
                type_offsets_file: files[5].clone(),
                offsets_file: files[6].clone(),
                blocks_file: files[7].clone(),
            },

            id_map_files: IdMapFiles {
                node_value_idmap_files: BitIndexFiles {
                    bits_file: files[8].clone(),
                    blocks_file: files[9].clone(),
                    sblocks_file: files[10].clone(),
                },
                predicate_idmap_files: BitIndexFiles {
                    bits_file: files[11].clone(),
                    blocks_file: files[12].clone(),
                    sblocks_file: files[13].clone(),
                },
            },

            pos_subjects_file: files[14].clone(),
            pos_objects_file: files[15].clone(),
            neg_subjects_file: files[16].clone(),
            neg_objects_file: files[17].clone(),

            pos_s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[18].clone(),
                    blocks_file: files[19].clone(),
                    sblocks_file: files[20].clone(),
                },
                nums_file: files[21].clone(),
            },
            pos_sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[22].clone(),
                    blocks_file: files[23].clone(),
                    sblocks_file: files[24].clone(),
                },
                nums_file: files[25].clone(),
            },
            pos_o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[26].clone(),
                    blocks_file: files[27].clone(),
                    sblocks_file: files[28].clone(),
                },
                nums_file: files[29].clone(),
            },
            neg_s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[30].clone(),
                    blocks_file: files[31].clone(),
                    sblocks_file: files[32].clone(),
                },
                nums_file: files[33].clone(),
            },
            neg_sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[34].clone(),
                    blocks_file: files[35].clone(),
                    sblocks_file: files[36].clone(),
                },
                nums_file: files[37].clone(),
            },
            neg_o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[38].clone(),
                    blocks_file: files[39].clone(),
                    sblocks_file: files[40].clone(),
                },
                nums_file: files[41].clone(),
            },
            pos_predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[42].clone(),
                blocks_file: files[43].clone(),
                sblocks_file: files[44].clone(),
            },
            neg_predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[45].clone(),
                blocks_file: files[46].clone(),
                sblocks_file: files[47].clone(),
            },
        })
    }

    async fn write_parent_file(&self, dir_name: [u32; 5], parent_name: [u32; 5]) -> io::Result<()> {
        let parent_string = name_to_string(parent_name);

        let file = self.get_file(dir_name, FILENAMES.parent).await?;
        let mut writer = file.open_write().await?;

        writer.write_all(parent_string.as_bytes()).await?;
        writer.flush().await?;
        writer.sync_all().await?;

        Ok(())
    }

    async fn read_parent_file(&self, dir_name: [u32; 5]) -> io::Result<[u32; 5]> {
        let file = self.get_file(dir_name, FILENAMES.parent).await?;
        let mut reader = file.open_read().await?;

        let mut buf = [0; 40];
        reader.read_exact(&mut buf).await?;

        bytes_to_name(&buf)
    }

    // TODO this should check if the rollup is better than what is there
    async fn write_rollup_file(&self, dir_name: [u32; 5], rollup_name: [u32; 5]) -> io::Result<()> {
        let rollup_string = name_to_string(rollup_name);

        let file = self.get_file(dir_name, FILENAMES.rollup).await?;
        let mut writer = file.open_write().await?;

        let contents = format!("{}\n{}\n", 1, rollup_string);
        writer.write_all(contents.as_bytes()).await?;
        writer.flush().await?;
        writer.sync_all().await?;

        Ok(())
    }

    async fn read_rollup_file(&self, dir_name: [u32; 5]) -> io::Result<[u32; 5]> {
        let file = self.get_file(dir_name, FILENAMES.rollup).await?;
        let mut reader = file.open_read().await?;

        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;

        let s = String::from_utf8_lossy(&data);
        let lines: Vec<&str> = s.lines().collect();
        if lines.len() != 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected rollup file to have two lines. contents were ({:?})",
                    lines
                ),
            ));
        }

        let _version_str = &lines[0];
        let layer_str = &lines[1];

        string_to_name(layer_str)
    }

    async fn create_child_layer_files_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<([u32; 5], Arc<InternalLayer>, ChildLayerFiles<Self::File>)> {
        let parent_layer = match self.get_layer_with_cache(parent, cache).await? {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found",
                ))
            }
            Some(parent_layer) => Ok::<_, io::Error>(parent_layer),
        }?;

        let layer_dir = self.create_directory().await?;
        self.write_parent_file(layer_dir, parent).await?;
        let child_layer_files = self.child_layer_files(layer_dir).await?;

        Ok((layer_dir, parent_layer, child_layer_files))
    }

    async fn node_dictionary_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<DictionaryFiles<Self::File>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let offsets_file = self
                .get_file(layer, FILENAMES.node_dictionary_offsets)
                .await?;
            let blocks_file = self
                .get_file(layer, FILENAMES.node_dictionary_blocks)
                .await?;

            Ok(DictionaryFiles {
                blocks_file,
                offsets_file,
            })
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn predicate_dictionary_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<DictionaryFiles<Self::File>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let offsets_file = self
                .get_file(layer, FILENAMES.predicate_dictionary_offsets)
                .await?;
            let blocks_file = self
                .get_file(layer, FILENAMES.predicate_dictionary_blocks)
                .await?;

            Ok(DictionaryFiles {
                blocks_file,
                offsets_file,
            })
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn value_dictionary_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<TypedDictionaryFiles<Self::File>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let types_present_file = self
                .get_file(layer, FILENAMES.value_dictionary_types_present)
                .await?;
            let type_offsets_file = self
                .get_file(layer, FILENAMES.value_dictionary_type_offsets)
                .await?;
            let offsets_file = self
                .get_file(layer, FILENAMES.value_dictionary_offsets)
                .await?;
            let blocks_file = self
                .get_file(layer, FILENAMES.value_dictionary_blocks)
                .await?;

            Ok(TypedDictionaryFiles {
                types_present_file,
                type_offsets_file,
                blocks_file,
                offsets_file,
            })
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn node_value_idmap_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<BitIndexFiles<Self::File>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let bits_file = self
                .get_file(layer, FILENAMES.node_value_idmap_bits)
                .await?;
            let blocks_file = self
                .get_file(layer, FILENAMES.node_value_idmap_bit_index_blocks)
                .await?;
            let sblocks_file = self
                .get_file(layer, FILENAMES.node_value_idmap_bit_index_sblocks)
                .await?;

            Ok(BitIndexFiles {
                bits_file,
                blocks_file,
                sblocks_file,
            })
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn predicate_idmap_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<BitIndexFiles<Self::File>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let bits_file = self.get_file(layer, FILENAMES.predicate_idmap_bits).await?;
            let blocks_file = self
                .get_file(layer, FILENAMES.predicate_idmap_bit_index_blocks)
                .await?;
            let sblocks_file = self
                .get_file(layer, FILENAMES.predicate_idmap_bit_index_sblocks)
                .await?;

            Ok(BitIndexFiles {
                bits_file,
                blocks_file,
                sblocks_file,
            })
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn triple_addition_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<(
        Self::File,
        AdjacencyListFiles<Self::File>,
        AdjacencyListFiles<Self::File>,
    )> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let (
                s_p_aj_nums_file,
                s_p_aj_bits_file,
                s_p_aj_bit_index_blocks_file,
                s_p_aj_bit_index_sblocks_file,
                subjects_file,
            );
            let (
                sp_o_aj_nums_file,
                sp_o_aj_bits_file,
                sp_o_aj_bit_index_blocks_file,
                sp_o_aj_bit_index_sblocks_file,
            );
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                s_p_aj_nums_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_nums)
                    .await?;
                s_p_aj_bits_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_bits)
                    .await?;
                s_p_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_bit_index_blocks)
                    .await?;
                s_p_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks)
                    .await?;

                sp_o_aj_nums_file = self
                    .get_file(layer, FILENAMES.pos_sp_o_adjacency_list_nums)
                    .await?;
                sp_o_aj_bits_file = self
                    .get_file(layer, FILENAMES.pos_sp_o_adjacency_list_bits)
                    .await?;
                sp_o_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks)
                    .await?;
                sp_o_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks)
                    .await?;

                subjects_file = self.get_file(layer, FILENAMES.pos_subjects).await?;
            } else {
                // this is a base layer
                s_p_aj_nums_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_nums)
                    .await?;
                s_p_aj_bits_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_bits)
                    .await?;
                s_p_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_bit_index_blocks)
                    .await?;
                s_p_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_bit_index_sblocks)
                    .await?;

                sp_o_aj_nums_file = self
                    .get_file(layer, FILENAMES.base_sp_o_adjacency_list_nums)
                    .await?;
                sp_o_aj_bits_file = self
                    .get_file(layer, FILENAMES.base_sp_o_adjacency_list_bits)
                    .await?;
                sp_o_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.base_sp_o_adjacency_list_bit_index_blocks)
                    .await?;
                sp_o_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks)
                    .await?;

                subjects_file = self.get_file(layer, FILENAMES.base_subjects).await?;
            }

            let s_p_aj_files = AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: s_p_aj_bits_file,
                    blocks_file: s_p_aj_bit_index_blocks_file,
                    sblocks_file: s_p_aj_bit_index_sblocks_file,
                },
                nums_file: s_p_aj_nums_file,
            };
            let sp_o_aj_files = AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: sp_o_aj_bits_file,
                    blocks_file: sp_o_aj_bit_index_blocks_file,
                    sblocks_file: sp_o_aj_bit_index_sblocks_file,
                },
                nums_file: sp_o_aj_nums_file,
            };

            Ok((subjects_file, s_p_aj_files, sp_o_aj_files))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn triple_removal_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<
        Option<(
            Self::File,
            AdjacencyListFiles<Self::File>,
            AdjacencyListFiles<Self::File>,
        )>,
    > {
        // does layer exist?
        if self.directory_exists(layer).await? {
            if self.layer_has_parent(layer).await? {
                let (
                    s_p_aj_nums_file,
                    s_p_aj_bits_file,
                    s_p_aj_bit_index_blocks_file,
                    s_p_aj_bit_index_sblocks_file,
                    subjects_file,
                );
                let (
                    sp_o_aj_nums_file,
                    sp_o_aj_bits_file,
                    sp_o_aj_bit_index_blocks_file,
                    sp_o_aj_bit_index_sblocks_file,
                );

                s_p_aj_nums_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_nums)
                    .await?;
                s_p_aj_bits_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_bits)
                    .await?;
                s_p_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_bit_index_blocks)
                    .await?;
                s_p_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks)
                    .await?;

                sp_o_aj_nums_file = self
                    .get_file(layer, FILENAMES.neg_sp_o_adjacency_list_nums)
                    .await?;
                sp_o_aj_bits_file = self
                    .get_file(layer, FILENAMES.neg_sp_o_adjacency_list_bits)
                    .await?;
                sp_o_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks)
                    .await?;
                sp_o_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks)
                    .await?;

                subjects_file = self.get_file(layer, FILENAMES.neg_subjects).await?;
                let s_p_aj_files = AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: s_p_aj_bits_file,
                        blocks_file: s_p_aj_bit_index_blocks_file,
                        sblocks_file: s_p_aj_bit_index_sblocks_file,
                    },
                    nums_file: s_p_aj_nums_file,
                };
                let sp_o_aj_files = AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: sp_o_aj_bits_file,
                        blocks_file: sp_o_aj_bit_index_blocks_file,
                        sblocks_file: sp_o_aj_bit_index_sblocks_file,
                    },
                    nums_file: sp_o_aj_nums_file,
                };

                Ok(Some((subjects_file, s_p_aj_files, sp_o_aj_files)))
            } else {
                // base layer, so removal does not exist
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn predicate_wavelet_addition_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<BitIndexFiles<Self::File>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let (wavelet_bits_file, wavelet_bit_index_blocks_file, wavelet_bit_index_sblocks_file);
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                wavelet_bits_file = self
                    .get_file(layer, FILENAMES.pos_predicate_wavelet_tree_bits)
                    .await?;
                wavelet_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.pos_predicate_wavelet_tree_bit_index_blocks)
                    .await?;
                wavelet_bit_index_sblocks_file = self
                    .get_file(
                        layer,
                        FILENAMES.pos_predicate_wavelet_tree_bit_index_sblocks,
                    )
                    .await?;
            } else {
                // this is a base layer
                wavelet_bits_file = self
                    .get_file(layer, FILENAMES.base_predicate_wavelet_tree_bits)
                    .await?;
                wavelet_bit_index_blocks_file = self
                    .get_file(
                        layer,
                        FILENAMES.base_predicate_wavelet_tree_bit_index_blocks,
                    )
                    .await?;
                wavelet_bit_index_sblocks_file = self
                    .get_file(
                        layer,
                        FILENAMES.base_predicate_wavelet_tree_bit_index_sblocks,
                    )
                    .await?;
            }

            let bitindex_files = BitIndexFiles {
                bits_file: wavelet_bits_file,
                blocks_file: wavelet_bit_index_blocks_file,
                sblocks_file: wavelet_bit_index_sblocks_file,
            };
            Ok(bitindex_files)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn predicate_wavelet_removal_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<Option<BitIndexFiles<Self::File>>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                let wavelet_bits_file = self
                    .get_file(layer, FILENAMES.neg_predicate_wavelet_tree_bits)
                    .await?;
                let wavelet_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.neg_predicate_wavelet_tree_bit_index_blocks)
                    .await?;
                let wavelet_bit_index_sblocks_file = self
                    .get_file(
                        layer,
                        FILENAMES.neg_predicate_wavelet_tree_bit_index_sblocks,
                    )
                    .await?;
                let bitindex_files = BitIndexFiles {
                    bits_file: wavelet_bits_file,
                    blocks_file: wavelet_bit_index_blocks_file,
                    sblocks_file: wavelet_bit_index_sblocks_file,
                };
                Ok(Some(bitindex_files))
            } else {
                // this is a base layer
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn triple_addition_files_by_object(
        &self,
        layer: [u32; 5],
    ) -> io::Result<(
        Self::File,
        Self::File,
        AdjacencyListFiles<Self::File>,
        AdjacencyListFiles<Self::File>,
    )> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let (
                subjects_file,
                objects_file,
                o_ps_aj_nums_file,
                o_ps_aj_bits_file,
                o_ps_aj_bit_index_blocks_file,
                o_ps_aj_bit_index_sblocks_file,
            );
            let (
                s_p_aj_nums_file,
                s_p_aj_bits_file,
                s_p_aj_bit_index_blocks_file,
                s_p_aj_bit_index_sblocks_file,
            );
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                o_ps_aj_nums_file = self
                    .get_file(layer, FILENAMES.pos_o_ps_adjacency_list_nums)
                    .await?;
                o_ps_aj_bits_file = self
                    .get_file(layer, FILENAMES.pos_o_ps_adjacency_list_bits)
                    .await?;
                o_ps_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.pos_o_ps_adjacency_list_bit_index_blocks)
                    .await?;
                o_ps_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.pos_o_ps_adjacency_list_bit_index_sblocks)
                    .await?;

                s_p_aj_nums_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_nums)
                    .await?;
                s_p_aj_bits_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_bits)
                    .await?;
                s_p_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_bit_index_blocks)
                    .await?;
                s_p_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks)
                    .await?;

                subjects_file = self.get_file(layer, FILENAMES.pos_subjects).await?;
                objects_file = self.get_file(layer, FILENAMES.pos_objects).await?;
            } else {
                // this is a base layer
                o_ps_aj_nums_file = self
                    .get_file(layer, FILENAMES.base_o_ps_adjacency_list_nums)
                    .await?;
                o_ps_aj_bits_file = self
                    .get_file(layer, FILENAMES.base_o_ps_adjacency_list_bits)
                    .await?;
                o_ps_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.base_o_ps_adjacency_list_bit_index_blocks)
                    .await?;
                o_ps_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.base_o_ps_adjacency_list_bit_index_sblocks)
                    .await?;

                s_p_aj_nums_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_nums)
                    .await?;
                s_p_aj_bits_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_bits)
                    .await?;
                s_p_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_bit_index_blocks)
                    .await?;
                s_p_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_bit_index_sblocks)
                    .await?;

                subjects_file = self.get_file(layer, FILENAMES.base_subjects).await?;
                objects_file = self.get_file(layer, FILENAMES.base_objects).await?;
            }

            let o_ps_aj_files = AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: o_ps_aj_bits_file,
                    blocks_file: o_ps_aj_bit_index_blocks_file,
                    sblocks_file: o_ps_aj_bit_index_sblocks_file,
                },
                nums_file: o_ps_aj_nums_file,
            };
            let s_p_aj_files = AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: s_p_aj_bits_file,
                    blocks_file: s_p_aj_bit_index_blocks_file,
                    sblocks_file: s_p_aj_bit_index_sblocks_file,
                },
                nums_file: s_p_aj_nums_file,
            };

            Ok((subjects_file, objects_file, o_ps_aj_files, s_p_aj_files))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn triple_removal_files_by_object(
        &self,
        layer: [u32; 5],
    ) -> io::Result<
        Option<(
            Self::File,
            Self::File,
            AdjacencyListFiles<Self::File>,
            AdjacencyListFiles<Self::File>,
        )>,
    > {
        // does layer exist?
        if self.directory_exists(layer).await? {
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                let o_ps_aj_nums_file = self
                    .get_file(layer, FILENAMES.neg_o_ps_adjacency_list_nums)
                    .await?;
                let o_ps_aj_bits_file = self
                    .get_file(layer, FILENAMES.neg_o_ps_adjacency_list_bits)
                    .await?;
                let o_ps_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.neg_o_ps_adjacency_list_bit_index_blocks)
                    .await?;
                let o_ps_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.neg_o_ps_adjacency_list_bit_index_sblocks)
                    .await?;

                let s_p_aj_nums_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_nums)
                    .await?;
                let s_p_aj_bits_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_bits)
                    .await?;
                let s_p_aj_bit_index_blocks_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_bit_index_blocks)
                    .await?;
                let s_p_aj_bit_index_sblocks_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks)
                    .await?;

                let subjects_file = self.get_file(layer, FILENAMES.neg_subjects).await?;
                let objects_file = self.get_file(layer, FILENAMES.neg_objects).await?;

                let o_ps_aj_files = AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: o_ps_aj_bits_file,
                        blocks_file: o_ps_aj_bit_index_blocks_file,
                        sblocks_file: o_ps_aj_bit_index_sblocks_file,
                    },
                    nums_file: o_ps_aj_nums_file,
                };
                let s_p_aj_files = AdjacencyListFiles {
                    bitindex_files: BitIndexFiles {
                        bits_file: s_p_aj_bits_file,
                        blocks_file: s_p_aj_bit_index_blocks_file,
                        sblocks_file: s_p_aj_bit_index_sblocks_file,
                    },
                    nums_file: s_p_aj_nums_file,
                };

                Ok(Some((
                    subjects_file,
                    objects_file,
                    o_ps_aj_files,
                    s_p_aj_files,
                )))
            } else {
                // this is a base layer
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn triple_layer_addition_count_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<(Self::File, Self::File, BitIndexFiles<Self::File>)> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            let (s_p_nums_file, sp_o_bits_file);
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                s_p_nums_file = self
                    .get_file(layer, FILENAMES.pos_s_p_adjacency_list_nums)
                    .await?;
                sp_o_bits_file = self
                    .get_file(layer, FILENAMES.pos_sp_o_adjacency_list_bits)
                    .await?;
            } else {
                // this is a base layer
                s_p_nums_file = self
                    .get_file(layer, FILENAMES.base_s_p_adjacency_list_nums)
                    .await?;
                sp_o_bits_file = self
                    .get_file(layer, FILENAMES.base_sp_o_adjacency_list_bits)
                    .await?;
            }

            let predicate_wavelet_files = self.predicate_wavelet_addition_files(layer).await?;
            Ok((s_p_nums_file, sp_o_bits_file, predicate_wavelet_files))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    async fn triple_layer_removal_count_files(
        &self,
        layer: [u32; 5],
    ) -> io::Result<Option<(Self::File, Self::File, BitIndexFiles<Self::File>)>> {
        // does layer exist?
        if self.directory_exists(layer).await? {
            if self.layer_has_parent(layer).await? {
                // this is a child layer
                let s_p_nums_file = self
                    .get_file(layer, FILENAMES.neg_s_p_adjacency_list_nums)
                    .await?;
                let sp_o_bits_file = self
                    .get_file(layer, FILENAMES.neg_sp_o_adjacency_list_bits)
                    .await?;
                let predicate_wavelet_files = self
                    .predicate_wavelet_removal_files(layer)
                    .await?
                    .expect("expected wavelet removal files to exist");
                Ok(Some((
                    s_p_nums_file,
                    sp_o_bits_file,
                    predicate_wavelet_files,
                )))
            } else {
                // this is a base layer
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
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
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("string not len 40: {}", string),
        ));
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
        let string = String::from_utf8_lossy(bytes);

        string_to_name(&string)
    }
}

#[async_trait]
impl<F: 'static + FileLoad + FileStore + Clone, T: 'static + PersistentLayerStore<File = F>>
    LayerStore for T
{
    async fn layers(&self) -> io::Result<Vec<[u32; 5]>> {
        self.directories().await
    }

    async fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<Option<Arc<InternalLayer>>> {
        if let Some(layer) = cache.get_layer_from_cache(name) {
            return Ok(Some(layer));
        }

        let mut layers_to_load: Vec<([u32; 5], Option<([u32; 5], Option<[u32; 5]>)>)> =
            vec![(name, None)];

        if !self.directory_exists(name).await? {
            return Ok(None);
        }

        // find an ancestor in cache
        let mut ancestor = None;
        loop {
            let (original_layer, rollup_option) = *layers_to_load.last().unwrap();
            let current_layer;
            if let Some((rollup, _original_parent)) = rollup_option {
                current_layer = rollup;
            } else {
                current_layer = original_layer;
            }

            match cache.get_layer_from_cache(current_layer) {
                Some(layer) => {
                    // remove found cached layer from ids to retrieve
                    layers_to_load.pop().unwrap();

                    // if this is a rollup, the behavior has to be slightly different
                    if let Some((_, original_parent)) = rollup_option {
                        if layer.immediate_parent().is_some() {
                            ancestor = Some(Arc::new(
                                RollupLayer::from_child_layer(
                                    layer,
                                    original_layer,
                                    original_parent.unwrap(),
                                )
                                .into(),
                            ));
                        } else {
                            ancestor = Some(Arc::new(
                                RollupLayer::from_base_layer(
                                    layer,
                                    original_layer,
                                    original_parent,
                                )
                                .into(),
                            ));
                        }
                    } else {
                        ancestor = Some(layer);
                    }
                    break;
                }
                None => {
                    if self.layer_has_rollup(current_layer).await? {
                        let rollup = self.read_rollup_file(current_layer).await?;
                        if rollup == current_layer {
                            panic!("infinite rollup loop for layer {:?}", rollup);
                        }

                        let original_parent;
                        if self.layer_has_parent(current_layer).await? {
                            original_parent = Some(self.read_parent_file(current_layer).await?);
                        } else {
                            original_parent = None;
                        }

                        layers_to_load.pop().unwrap(); // we don't want to load this, we want to load the rollup instead!
                        layers_to_load.push((current_layer, Some((rollup, original_parent))));
                    } else if self.layer_has_parent(current_layer).await? {
                        let parent = self.read_parent_file(current_layer).await?;
                        layers_to_load.push((parent, None));
                    } else {
                        break;
                    }
                }
            }
        }

        if ancestor.is_none() {
            // load the base layer
            let (base_id, rollup) = layers_to_load.pop().unwrap();
            let layer: Arc<InternalLayer>;
            match rollup {
                None => {
                    let files = self.base_layer_files(base_id).await?;
                    let base_layer = BaseLayer::load_from_files(base_id, &files).await?;

                    layer = Arc::new(base_layer.into());
                }
                Some((rollup_id, original_parent_id_option)) => {
                    let files = self.base_layer_files(rollup_id).await?;
                    let base_layer: Arc<InternalLayer> =
                        Arc::new(BaseLayer::load_from_files(rollup_id, &files).await?.into());
                    cache.cache_layer(base_layer.clone());

                    layer = Arc::new(
                        RollupLayer::from_base_layer(
                            base_layer,
                            base_id,
                            original_parent_id_option,
                        )
                        .into(),
                    );
                }
            }

            cache.cache_layer(layer.clone());
            ancestor = Some(layer);
        }

        let mut ancestor = ancestor.unwrap();
        layers_to_load.reverse();

        for (layer_id, rollup) in layers_to_load {
            let layer: Arc<InternalLayer>;
            match rollup {
                None => {
                    let files = self.child_layer_files(layer_id).await?;
                    let child_layer =
                        ChildLayer::load_from_files(layer_id, ancestor, &files).await?;
                    layer = Arc::new(child_layer.into());
                }
                Some((rollup_id, original_parent_id_option)) => {
                    let original_parent_id = original_parent_id_option
                        .expect("child rollup layer should always have original parent id");

                    let files = self.child_layer_files(rollup_id).await?;
                    let child_layer: Arc<InternalLayer> = Arc::new(
                        ChildLayer::load_from_files(rollup_id, ancestor, &files)
                            .await?
                            .into(),
                    );
                    cache.cache_layer(child_layer.clone());

                    layer = Arc::new(
                        RollupLayer::from_child_layer(child_layer, layer_id, original_parent_id)
                            .into(),
                    );
                }
            }

            cache.cache_layer(layer.clone());
            ancestor = layer;
        }

        debug_assert_eq!(name, ancestor.name());

        Ok(Some(ancestor))
    }

    async fn finalize_layer(&self, name: [u32; 5]) -> io::Result<()> {
        self.finalize(name).await
    }

    async fn get_layer_parent_name(&self, name: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        self.layer_parent(name).await
    }

    async fn get_node_dictionary(&self, name: [u32; 5]) -> io::Result<Option<StringDict>> {
        if self.directory_exists(name).await? {
            let files = self.node_dictionary_files(name).await?;
            let maps = files.map_all().await?;

            Ok(Some(StringDict::parse(maps.offsets_map, maps.blocks_map)))
        } else {
            Ok(None)
        }
    }

    async fn get_predicate_dictionary(&self, name: [u32; 5]) -> io::Result<Option<StringDict>> {
        if self.directory_exists(name).await? {
            let files = self.predicate_dictionary_files(name).await?;
            let maps = files.map_all().await?;

            Ok(Some(StringDict::parse(maps.offsets_map, maps.blocks_map)))
        } else {
            Ok(None)
        }
    }

    async fn get_value_dictionary(&self, name: [u32; 5]) -> io::Result<Option<TypedDict>> {
        if self.directory_exists(name).await? {
            let files = self.value_dictionary_files(name).await?;
            let maps = files.map_all().await?;

            Ok(Some(TypedDict::from_parts(
                maps.types_present_map,
                maps.type_offsets_map,
                maps.offsets_map,
                maps.blocks_map,
            )))
        } else {
            Ok(None)
        }
    }

    async fn get_node_count(&self, name: [u32; 5]) -> io::Result<Option<u64>> {
        if self.directory_exists(name).await? {
            let file = self.node_dictionary_files(name).await?.blocks_file;
            Ok(Some(dict_file_get_count(file).await?))
        } else {
            Ok(None)
        }
    }

    async fn get_predicate_count(&self, name: [u32; 5]) -> io::Result<Option<u64>> {
        if self.directory_exists(name).await? {
            let file = self.predicate_dictionary_files(name).await?.blocks_file;
            Ok(Some(dict_file_get_count(file).await?))
        } else {
            Ok(None)
        }
    }

    async fn get_value_count(&self, name: [u32; 5]) -> io::Result<Option<u64>> {
        if self.directory_exists(name).await? {
            let file = self.value_dictionary_files(name).await?.blocks_file;
            Ok(Some(dict_file_get_count(file).await?))
        } else {
            Ok(None)
        }
    }

    async fn get_node_value_idmap(&self, name: [u32; 5]) -> io::Result<Option<IdMap>> {
        if self.directory_exists(name).await? {
            let size = self.get_node_count(name).await?.unwrap()
                + self.get_value_count(name).await?.unwrap();
            let width = util::calculate_width(size);
            let files = self.node_value_idmap_files(name).await?;
            let maps = files.map_all_if_exists().await?;

            let wtree = maps.map(|m| {
                WaveletTree::from_parts(
                    BitIndex::from_maps(m.bits_map, m.blocks_map, m.sblocks_map),
                    width,
                )
            });
            let idmap = IdMap::from_parts(wtree);

            Ok(Some(idmap))
        } else {
            Ok(None)
        }
    }

    async fn get_predicate_idmap(&self, name: [u32; 5]) -> io::Result<Option<IdMap>> {
        if self.directory_exists(name).await? {
            let size = self.get_predicate_count(name).await?.unwrap();
            let width = util::calculate_width(size);
            let files = self.predicate_idmap_files(name).await?;
            let maps = files.map_all_if_exists().await?;

            let wtree = maps.map(|m| {
                WaveletTree::from_parts(
                    BitIndex::from_maps(m.bits_map, m.blocks_map, m.sblocks_map),
                    width,
                )
            });
            let idmap = IdMap::from_parts(wtree);

            Ok(Some(idmap))
        } else {
            Ok(None)
        }
    }

    async fn create_base_layer(&self) -> io::Result<Box<dyn LayerBuilder>> {
        let dir_name = self.create_directory().await?;
        let files = self.base_layer_files(dir_name).await?;
        Ok(Box::new(SimpleLayerBuilder::new(dir_name, files)) as Box<dyn LayerBuilder>)
    }

    async fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<Box<dyn LayerBuilder>> {
        let (layer_dir, parent_layer, child_layer_files) = self
            .create_child_layer_files_with_cache(parent, cache)
            .await?;

        Ok(Box::new(SimpleLayerBuilder::from_parent(
            layer_dir,
            parent_layer,
            child_layer_files,
        )) as Box<dyn LayerBuilder>)
    }

    async fn perform_rollup(&self, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]> {
        if layer.parent_name().is_none() {
            // we're already a base layer. there's nothing that can be rolled up.
            // returning our own name will inhibit writing a rollup file.
            return Ok(layer.name());
        }

        // check if there's already an equivalent rollup
        if self.layer_has_rollup(layer.name()).await? {
            let rollup = self.read_rollup_file(layer.name()).await?;
            // the rollup is equivalent if it is a base layer
            if !self.layer_has_parent(rollup).await? {
                // yup, equivalent. let's just return the rollup we know about.
                return Ok(rollup);
            }
        }

        let dir_name = self.create_directory().await?;
        let files = self.base_layer_files(dir_name).await?;
        delta_rollup(&layer, files).await?;
        self.finalize(dir_name).await?;

        Ok(dir_name)
    }

    async fn perform_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]> {
        if layer.name() == upto {
            // rolling up upto ourselves is pretty pointless. Let's not do that.
            return Ok(layer.name());
        }

        if layer.parent_name() == Some(upto) {
            // rolling up to our parent is just going to create a clone of this child layer. Let's not do that.
            return Ok(layer.name());
        }

        // check if there's already an equivalent rollup
        if self.layer_has_rollup(layer.name()).await? {
            let rollup = self.read_rollup_file(layer.name()).await?;

            // get rollup parent. if it is the same as upto, we're requesting something equivalent.
            if upto == self.read_parent_file(rollup).await? {
                // yup, equivalent. Let's just return the rollup we know about.
                return Ok(rollup);
            }
        }

        let (layer_dir, _parent_layer, child_layer_files) = self
            .create_child_layer_files_with_cache(upto, cache)
            .await?;
        delta_rollup_upto(self, &layer, upto, child_layer_files).await?;
        self.finalize(layer_dir).await?;
        Ok(layer_dir)
    }

    async fn perform_imprecise_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]> {
        if layer.name() == upto {
            // rolling up upto ourselves is pretty pointless. Let's not do that.
            return Ok(layer.name());
        }

        if layer.parent_name() == Some(upto) {
            // rolling up to our parent is just going to create a clone of this child layer. Let's not do that.
            return Ok(layer.name());
        }

        // check if there's already an equivalent rollup
        if self.layer_has_rollup(layer.name()).await? {
            let rollup = self.read_rollup_file(layer.name()).await?;

            // get rollup parent. if it is the same as upto, we're requesting something equivalent.
            if upto == self.read_parent_file(rollup).await? {
                // yup, equivalent. Let's just return the rollup we know about.
                return Ok(rollup);
            }
        }

        let (layer_dir, _parent_layer, child_layer_files) = self
            .create_child_layer_files_with_cache(upto, cache)
            .await?;
        imprecise_delta_rollup_upto(self, &layer, upto, child_layer_files).await?;
        self.finalize(layer_dir).await?;
        Ok(layer_dir)
    }

    async fn register_rollup(&self, layer: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        if layer == rollup {
            // let's not create a loop
            Ok(())
        } else {
            self.write_rollup_file(layer, rollup).await
        }
    }

    async fn squash(&self, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]> {
        // we create a new base layer
        // we then build a new set of dictionaries by sorting what we got in all layers
        // keep track of how the ids move so we have a mapping
        // then iterate over all id triples and map them
        // sort the mapped triples, insert them
        // finalize

        let stack = layer.immediate_layers();
        let node_count: usize = stack
            .iter()
            .map(|l| l.node_dictionary().num_entries())
            .sum();
        let predicate_count: usize = stack
            .iter()
            .map(|l| l.predicate_dictionary().num_entries())
            .sum();
        let value_count: usize = stack
            .iter()
            .map(|l| l.value_dictionary().num_entries())
            .sum();

        // figure out what keys are actually used through a prepass over all triples
        let mut node_value_existences = bitvec![0;node_count+value_count+1];
        let mut predicate_existences = bitvec![0;predicate_count+1];
        let mut num_triples = 0;
        for triple in layer.triples() {
            num_triples += 1;
            node_value_existences.set(triple.subject as usize, true);
            predicate_existences.set(triple.predicate as usize, true);
            node_value_existences.set(triple.object as usize, true);
        }

        let mut nodes = Vec::with_capacity(node_count);
        let mut predicates = Vec::with_capacity(predicate_count);
        let mut values = Vec::with_capacity(value_count);
        let mut node_value_count = 0;
        let mut pred_count = 0;

        for layer in stack {
            nodes.extend(
                layer
                    .node_dictionary()
                    .iter()
                    .enumerate()
                    .flat_map(|(i, x)| {
                        let mapped_node = layer.node_value_id_map().inner_to_outer(i as u64 + 1)
                            + node_value_count;
                        if node_value_existences[mapped_node as usize] {
                            Some((x, mapped_node))
                        } else {
                            None
                        }
                    }),
            );
            predicates.extend(layer.predicate_dictionary().iter().enumerate().flat_map(
                |(i, x)| {
                    let mapped_predicate =
                        layer.predicate_id_map().inner_to_outer(i as u64 + 1) + pred_count;
                    if predicate_existences[mapped_predicate as usize] {
                        Some((x, mapped_predicate))
                    } else {
                        None
                    }
                },
            ));
            values.extend(
                layer
                    .value_dictionary()
                    .iter()
                    .enumerate()
                    .flat_map(|(i, x)| {
                        let mapped_value = layer
                            .node_value_id_map()
                            .inner_to_outer(i as u64 + layer.node_dict_len() as u64 + 1)
                            + node_value_count;
                        if node_value_existences[mapped_value as usize] {
                            Some((x, mapped_value))
                        } else {
                            None
                        }
                    }),
            );

            node_value_count += (layer.node_dictionary().num_entries()
                + layer.value_dictionary().num_entries()) as u64;
            pred_count += layer.predicate_dictionary().num_entries() as u64;
        }
        nodes.sort();
        predicates.sort();
        values.sort();

        let mut node_value_map: Vec<u64> = vec![0; node_count + value_count + 1];
        for (remapped, (_, old)) in nodes.iter().enumerate() {
            node_value_map[*old as usize] = remapped as u64 + 1;
        }

        let node_count = nodes.len();
        for (remapped, (_, old)) in values.iter().enumerate() {
            node_value_map[*old as usize] = (remapped + node_count + 1) as u64;
        }

        let mut pred_map: Vec<u64> = vec![0; predicate_count + 1];
        for (remapped, (_, old)) in predicates.iter().enumerate() {
            pred_map[*old as usize] = remapped as u64 + 1;
        }

        let layer_name = self.create_directory().await?;
        let base_layer_files = self.base_layer_files(layer_name).await?;
        let mut builder = BaseLayerFileBuilder::from_files(&base_layer_files).await?;
        builder.add_nodes_bytes(nodes.into_iter().map(|(x, _)| x.to_bytes()));
        builder.add_predicates_bytes(predicates.into_iter().map(|(x, _)| x.to_bytes()));
        builder.add_values(values.into_iter().map(|(x, _)| x));
        let mut builder = builder.into_phase2().await?;
        let mut triples = Vec::with_capacity(num_triples);
        triples.extend(layer.triples().map(move |t| {
            IdTriple::new(
                node_value_map[t.subject as usize],
                pred_map[t.predicate as usize],
                node_value_map[t.object as usize],
            )
        }));
        triples.sort();

        builder.add_id_triples(triples.into_iter()).await?;
        builder.finalize().await?;

        self.finalize_layer(layer_name).await?;

        Ok(layer_name)
    }

    async fn squash_upto(&self, layer: Arc<InternalLayer>, upto: [u32; 5]) -> io::Result<[u32; 5]> {
        let mut base_node_count = 0;
        let mut base_pred_count = 0;
        let mut base_value_count = 0;
        walk_backwards_from_disk!(self, upto, current, {
            base_node_count += self.get_node_count(current).await?.unwrap_or(0) as u64;
            base_value_count += self.get_value_count(current).await?.unwrap_or(0) as u64;
            base_pred_count += self.get_predicate_count(current).await?.unwrap_or(0) as u64;
        });
        let base_node_value_count = base_node_count + base_value_count;
        let mut node_value_count = base_node_value_count;
        let mut pred_count = base_pred_count;
        let stack_names = self
            .retrieve_layer_stack_names_upto(layer.name(), upto)
            .await?;

        let mut structures = Vec::with_capacity(stack_names.len());
        for layer_name in stack_names {
            let node_dict = self.get_node_dictionary(layer_name).await?;
            let predicate_dict = self.get_predicate_dictionary(layer_name).await?;
            let value_dict = self.get_value_dictionary(layer_name).await?;
            let node_value_id_map = self.get_node_value_idmap(layer_name).await?;
            let predicate_id_map = self.get_predicate_idmap(layer_name).await?;

            structures.push((
                node_dict,
                predicate_dict,
                value_dict,
                node_value_id_map,
                predicate_id_map,
            ));
        }

        let stack_node_count = structures
            .iter()
            .map(|d| d.0.as_ref().map(|d| d.num_entries()).unwrap_or(0))
            .sum();
        let stack_pred_count = structures
            .iter()
            .map(|d| d.1.as_ref().map(|d| d.num_entries()).unwrap_or(0))
            .sum();
        let stack_value_count = structures
            .iter()
            .map(|d| d.2.as_ref().map(|d| d.num_entries()).unwrap_or(0))
            .sum();
        let stack_node_value_count = stack_node_count + stack_value_count;

        // figure out what keys are actually used through a prepass over all triples
        let mut node_value_existences = bitvec![0;stack_node_value_count as usize+1];
        let mut predicate_existences = bitvec![0;stack_pred_count as usize+1];
        let mut num_triple_changes = 0;
        let layer_changes_upto = self.layer_changes_upto(layer.name(), upto).await?;
        for (change_type, triple) in layer_changes_upto.clone() {
            num_triple_changes += 1;
            if change_type == TripleChange::Removal {
                // removals can't possibly have dictionary entries
                // that are part of this layer stack, as they are
                // relative to upto, and therefore cannot contain
                // dictionary entries that didn't yet exist for the
                // upto layer.
                continue;
            }
            if triple.subject >= base_node_value_count {
                node_value_existences.set((triple.subject - base_node_value_count) as usize, true);
            }
            if triple.predicate >= base_pred_count {
                predicate_existences.set((triple.predicate - base_pred_count) as usize, true);
            }
            if triple.object >= base_node_value_count {
                node_value_existences.set((triple.object - base_node_value_count) as usize, true);
            }
        }

        let mut nodes = Vec::with_capacity(stack_node_count);
        let mut predicates = Vec::with_capacity(stack_pred_count);
        let mut values = Vec::with_capacity(stack_value_count);

        for (node_dict, predicate_dict, value_dict, node_value_id_map, predicate_id_map) in
            structures
        {
            let node_dict_len = node_dict.as_ref().map(|d| d.num_entries()).unwrap_or(0) as u64;
            let predicate_dict_len = predicate_dict
                .as_ref()
                .map(|d| d.num_entries())
                .unwrap_or(0) as u64;
            let value_dict_len = value_dict.as_ref().map(|d| d.num_entries()).unwrap_or(0) as u64;
            if let Some(node_dict) = node_dict.as_ref() {
                nodes.extend(node_dict.iter().enumerate().flat_map(|(i, x)| {
                    let mapped_node = node_value_id_map
                        .as_ref()
                        .map(|m| m.inner_to_outer(i as u64 + 1) + node_value_count)
                        .unwrap_or(i as u64 + node_value_count + 1);
                    if node_value_existences[(mapped_node - base_node_value_count) as usize] {
                        Some((x, mapped_node))
                    } else {
                        None
                    }
                }));
            }
            if let Some(predicate_dict) = predicate_dict.as_ref() {
                predicates.extend(predicate_dict.iter().enumerate().flat_map(|(i, x)| {
                    let mapped_predicate = predicate_id_map
                        .as_ref()
                        .map(|m| m.inner_to_outer(i as u64 + 1) + pred_count)
                        .unwrap_or(i as u64 + pred_count + 1);
                    if predicate_existences[(mapped_predicate - base_pred_count) as usize] {
                        Some((x, mapped_predicate))
                    } else {
                        None
                    }
                }));
            }
            if let Some(value_dict) = value_dict.as_ref() {
                values.extend(value_dict.iter().enumerate().flat_map(|(i, x)| {
                    let mapped_value = node_value_id_map
                        .as_ref()
                        .map(|m| m.inner_to_outer(i as u64 + node_dict_len + 1) + node_value_count)
                        .unwrap_or(i as u64 + node_value_count + node_dict_len + 1);
                    if node_value_existences[(mapped_value - base_node_value_count) as usize] {
                        Some((x, mapped_value))
                    } else {
                        None
                    }
                }));
            }
            node_value_count += node_dict_len + value_dict_len;
            pred_count += predicate_dict_len;
        }
        nodes.sort();
        predicates.sort();
        values.sort();

        let mut node_value_map: Vec<u64> = vec![0; stack_node_count + stack_value_count + 1];
        for (remapped, (_, old)) in nodes.iter().enumerate() {
            node_value_map[(*old - base_node_value_count) as usize] =
                remapped as u64 + base_node_value_count + 1;
        }
        let node_count = nodes.len();
        for (remapped, (_, old)) in values.iter().enumerate() {
            node_value_map[(*old - base_node_value_count) as usize] =
                (remapped + node_count + 1) as u64 + base_node_value_count;
        }

        let mut pred_map: Vec<u64> = vec![0; stack_pred_count + 1];
        for (remapped, (_, old)) in predicates.iter().enumerate() {
            pred_map[(*old - base_pred_count) as usize] = remapped as u64 + base_pred_count + 1;
        }

        let predicate_count = predicates.len();
        let value_count = values.len();

        let upto_layer = self.get_layer(upto).await?.expect("expected upto to exist");
        let layer_name = self.create_directory().await?;
        let child_layer_files = self.child_layer_files(layer_name).await?;
        let mut builder = DictionarySetFileBuilder::from_files(
            child_layer_files.node_dictionary_files.clone(),
            child_layer_files.predicate_dictionary_files.clone(),
            child_layer_files.value_dictionary_files.clone(),
        )
        .await?;

        builder.add_nodes_bytes(nodes.into_iter().map(|(x, _)| x.to_bytes()));
        builder.add_predicates_bytes(predicates.into_iter().map(|(x, _)| x.to_bytes()));
        builder.add_values(values.into_iter().map(|(x, _)| x));
        builder.finalize().await?;

        // TODO use more inner stuff to avoid parent checks as they are unnecessary here
        let mut builder = ChildLayerFileBuilderPhase2::new(
            upto_layer,
            child_layer_files,
            node_count,
            predicate_count,
            value_count,
        )
        .await?;
        let mut triple_changes = Vec::with_capacity(num_triple_changes);
        for (change_type, t) in layer_changes_upto {
            let mapped_subject = if t.subject <= base_node_value_count {
                t.subject
            } else {
                node_value_map[(t.subject - base_node_value_count) as usize]
            };
            let mapped_predicate = if t.predicate <= base_pred_count {
                t.predicate
            } else {
                pred_map[(t.predicate - base_pred_count) as usize]
            };
            let mapped_object = if t.object <= base_node_value_count {
                t.object
            } else {
                node_value_map[(t.object - base_node_value_count) as usize]
            };
            triple_changes.push((
                change_type,
                IdTriple::new(mapped_subject, mapped_predicate, mapped_object),
            ));
        }
        triple_changes.sort();
        for (change_type, mapped_triple) in triple_changes.into_iter() {
            match change_type {
                TripleChange::Addition => {
                    builder
                        .add_triple_unchecked(
                            mapped_triple.subject,
                            mapped_triple.predicate,
                            mapped_triple.object,
                        )
                        .await?
                }
                TripleChange::Removal => {
                    builder
                        .remove_triple_unchecked(
                            mapped_triple.subject,
                            mapped_triple.predicate,
                            mapped_triple.object,
                        )
                        .await?
                }
            }
        }
        builder.finalize().await?;
        self.write_parent_file(layer_name, upto).await?;
        self.finalize_layer(layer_name).await?;

        Ok(layer_name)
    }

    async fn merge_base_layer(
        &self,
        layers: &[[u32; 5]],
        temp_path: &Path,
    ) -> io::Result<[u32; 5]> {
        let mut layer_files = Vec::with_capacity(layers.len());
        for layer in layers {
            if self.layer_has_parent(*layer).await? {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "given layer is not a base layer: {}",
                        name_to_string(*layer)
                    ),
                ));
            }
            layer_files.push(self.base_layer_files(*layer).await?);
        }

        let output_name = self.create_directory().await?;
        let output_layer_files = self.base_layer_files(output_name).await?;

        merge_base_layers(&layer_files, output_layer_files, temp_path).await?;

        self.finalize(output_name).await?;

        Ok(output_name)
    }

    async fn layer_is_ancestor_of(
        &self,
        mut descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> io::Result<bool> {
        loop {
            if ancestor == descendant {
                return Ok(true);
            }

            if self.layer_has_parent(descendant).await? {
                let parent = self.read_parent_file(descendant).await?;
                descendant = parent;
            } else {
                return Ok(false);
            }
        }
    }

    async fn triple_addition_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        let (subjects_file, s_p_aj_files, sp_o_aj_files) =
            self.triple_addition_files(layer).await?;

        file_triple_exists(
            subjects_file,
            s_p_aj_files,
            sp_o_aj_files,
            subject,
            predicate,
            object,
        )
        .await
    }

    async fn triple_removal_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) =
            self.triple_removal_files(layer).await?
        {
            file_triple_exists(
                subjects_file,
                s_p_aj_files,
                sp_o_aj_files,
                subject,
                predicate,
                object,
            )
            .await
        } else {
            Ok(false)
        }
    }

    async fn triple_additions(
        &self,
        layer: [u32; 5],
    ) -> io::Result<OptInternalLayerTripleSubjectIterator> {
        let (subjects_file, s_p_aj_files, sp_o_aj_files) =
            self.triple_addition_files(layer).await?;

        Ok(OptInternalLayerTripleSubjectIterator(Some(
            file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files).await?,
        )))
    }

    async fn triple_removals(
        &self,
        layer: [u32; 5],
    ) -> io::Result<OptInternalLayerTripleSubjectIterator> {
        if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) =
            self.triple_removal_files(layer).await?
        {
            Ok(OptInternalLayerTripleSubjectIterator(Some(
                file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files).await?,
            )))
        } else {
            Ok(OptInternalLayerTripleSubjectIterator(None))
        }
    }

    async fn triple_additions_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let (subjects_file, s_p_aj_files, sp_o_aj_files) =
            self.triple_addition_files(layer).await?;

        Ok(Box::new(
            file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                .await?
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        ) as Box<dyn Iterator<Item = _> + Send>)
    }

    async fn triple_removals_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) =
            self.triple_removal_files(layer).await?
        {
            Ok(Box::new(
                file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                    .await?
                    .seek_subject(subject)
                    .take_while(move |t| t.subject == subject),
            ) as Box<dyn Iterator<Item = _> + Send>)
        } else {
            Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
        }
    }

    async fn triple_additions_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let (subjects_file, s_p_aj_files, sp_o_aj_files) =
            self.triple_addition_files(layer).await?;

        Ok(Box::new(
            file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                .await?
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.predicate == predicate && t.subject == subject),
        ) as Box<dyn Iterator<Item = _> + Send>)
    }

    async fn triple_removals_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) =
            self.triple_removal_files(layer).await?
        {
            Ok(Box::new(
                file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                    .await?
                    .seek_subject_predicate(subject, predicate)
                    .take_while(move |t| t.predicate == predicate && t.subject == subject),
            ) as Box<dyn Iterator<Item = _> + Send>)
        } else {
            Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
        }
    }

    async fn triple_additions_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let (subjects_file, s_p_aj_files, sp_o_aj_files) =
            self.triple_addition_files(layer).await?;
        let predicate_wavelet_files = self.predicate_wavelet_addition_files(layer).await?;

        Ok(Box::new(
            file_triple_iterator_by_predicate(
                subjects_file,
                s_p_aj_files,
                sp_o_aj_files,
                predicate_wavelet_files,
                predicate,
            )
            .await?,
        ) as Box<dyn Iterator<Item = _> + Send>)
    }

    async fn triple_removals_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let (Some((subjects_file, s_p_aj_files, sp_o_aj_files)), Some(predicate_wavelet_files)) = (
            self.triple_removal_files(layer).await?,
            self.predicate_wavelet_removal_files(layer).await?,
        ) {
            Ok(Box::new(
                file_triple_iterator_by_predicate(
                    subjects_file,
                    s_p_aj_files,
                    sp_o_aj_files,
                    predicate_wavelet_files,
                    predicate,
                )
                .await?,
            ) as Box<dyn Iterator<Item = _> + Send>)
        } else {
            Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
        }
    }

    async fn triple_additions_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let (subjects_file, objects_file, o_ps_aj_files, s_p_aj_files) =
            self.triple_addition_files_by_object(layer).await?;

        Ok(Box::new(
            file_triple_iterator_by_object(
                subjects_file,
                objects_file,
                o_ps_aj_files,
                s_p_aj_files,
                object,
            )
            .await?
            .take_while(move |t| t.object == object),
        ) as Box<dyn Iterator<Item = _> + Send>)
    }

    async fn triple_removals_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some((subjects_file, objects_file, o_ps_aj_files, s_p_aj_files)) =
            self.triple_removal_files_by_object(layer).await?
        {
            Ok(Box::new(
                file_triple_iterator_by_object(
                    subjects_file,
                    objects_file,
                    o_ps_aj_files,
                    s_p_aj_files,
                    object,
                )
                .await?
                .take_while(move |t| t.object == object),
            ) as Box<dyn Iterator<Item = _> + Send>)
        } else {
            Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
        }
    }

    async fn triple_layer_addition_count(&self, layer: [u32; 5]) -> io::Result<usize> {
        let (s_p_nums_file, sp_o_bits_file, predicate_wavelet_files) =
            self.triple_layer_addition_count_files(layer).await?;
        file_triple_layer_count(s_p_nums_file, sp_o_bits_file, predicate_wavelet_files).await
    }

    async fn triple_layer_removal_count(&self, layer: [u32; 5]) -> io::Result<usize> {
        if let Some((s_p_nums_file, sp_o_bits_file, predicate_wavelet_files)) =
            self.triple_layer_removal_count_files(layer).await?
        {
            file_triple_layer_count(s_p_nums_file, sp_o_bits_file, predicate_wavelet_files).await
        } else {
            Ok(0)
        }
    }

    async fn retrieve_layer_stack_names(&self, name: [u32; 5]) -> io::Result<Vec<[u32; 5]>> {
        let mut result = vec![name];

        loop {
            if self.layer_has_parent(*result.last().unwrap()).await? {
                let parent = self.read_parent_file(*result.last().unwrap()).await?;
                result.push(parent);
            } else {
                result.reverse();

                return Ok(result);
            }
        }
    }

    async fn retrieve_layer_stack_names_upto(
        &self,
        name: [u32; 5],
        upto: [u32; 5],
    ) -> io::Result<Vec<[u32; 5]>> {
        let mut result = vec![name];

        loop {
            if self.layer_has_parent(*result.last().unwrap()).await? {
                let parent = self.read_parent_file(*result.last().unwrap()).await?;
                if parent == upto {
                    break;
                }
                result.push(parent);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found while retrieving names of layer stack",
                ));
            }
        }

        result.reverse();
        Ok(result)
    }
}

pub(crate) async fn file_triple_exists<F: FileLoad + FileStore>(
    subjects_file: F,
    s_p_adjacency_list_files: AdjacencyListFiles<F>,
    sp_o_adjacency_list_files: AdjacencyListFiles<F>,
    subject: u64,
    predicate: u64,
    object: u64,
) -> io::Result<bool> {
    let s_p_maps = s_p_adjacency_list_files.map_all().await?;
    let sp_o_maps = sp_o_adjacency_list_files.map_all().await?;

    let subjects: Option<MonotonicLogArray> = subjects_file
        .map_if_exists()
        .await?
        .map(|l| LogArray::parse(l).unwrap().into());
    let s_p_aj = s_p_maps.into();
    let sp_o_aj = sp_o_maps.into();

    Ok(layer_triple_exists(
        subjects.as_ref(),
        &s_p_aj,
        &sp_o_aj,
        subject,
        predicate,
        object,
    ))
}

pub(crate) async fn file_triple_iterator<F: FileLoad + FileStore>(
    subjects_file: F,
    s_p_adjacency_list_files: AdjacencyListFiles<F>,
    sp_o_adjacency_list_files: AdjacencyListFiles<F>,
) -> io::Result<InternalLayerTripleSubjectIterator> {
    let s_p_maps = s_p_adjacency_list_files.map_all().await?;
    let sp_o_maps = sp_o_adjacency_list_files.map_all().await?;

    let subjects: Option<MonotonicLogArray> = subjects_file
        .map_if_exists()
        .await?
        .map(|l| LogArray::parse(l).unwrap().into());
    let s_p_aj = s_p_maps.into();
    let sp_o_aj = sp_o_maps.into();

    Ok(InternalLayerTripleSubjectIterator::new(
        subjects, s_p_aj, sp_o_aj,
    ))
}

pub(crate) async fn file_triple_iterator_by_predicate<F: FileLoad + FileStore>(
    subjects_file: F,
    s_p_adjacency_list_files: AdjacencyListFiles<F>,
    sp_o_adjacency_list_files: AdjacencyListFiles<F>,
    predicate_wavelet_files: BitIndexFiles<F>,
    predicate: u64,
) -> io::Result<impl Iterator<Item = IdTriple> + Send> {
    let s_p_maps = s_p_adjacency_list_files.map_all().await?;
    let sp_o_maps = sp_o_adjacency_list_files.map_all().await?;
    let predicate_wavelet_maps = predicate_wavelet_files.map_all().await?;

    let subjects: Option<MonotonicLogArray> = subjects_file
        .map_if_exists()
        .await?
        .map(|l| LogArray::parse(l).unwrap().into());
    let s_p_aj: AdjacencyList = s_p_maps.into();
    let sp_o_aj: AdjacencyList = sp_o_maps.into();

    let width = s_p_aj.nums().width();
    let wavelet_bits = predicate_wavelet_maps.into();
    let wtree = WaveletTree::from_parts(wavelet_bits, width);
    Ok(match wtree.lookup(predicate) {
        Some(lookup) => OptInternalLayerTriplePredicateIterator(Some(
            InternalLayerTriplePredicateIterator::new(lookup, subjects, s_p_aj, sp_o_aj),
        )),
        None => OptInternalLayerTriplePredicateIterator(None),
    })
}

pub(crate) async fn file_triple_iterator_by_object<F: FileLoad + FileStore>(
    subjects_file: F,
    objects_file: F,
    o_ps_adjacency_list_files: AdjacencyListFiles<F>,
    s_p_adjacency_list_files: AdjacencyListFiles<F>,
    object: u64,
) -> io::Result<impl Iterator<Item = IdTriple> + Send> {
    let subjects: Option<MonotonicLogArray> = subjects_file
        .map_if_exists()
        .await?
        .map(|l| LogArray::parse(l).unwrap().into());
    let objects: Option<MonotonicLogArray> = objects_file
        .map_if_exists()
        .await?
        .map(|l| LogArray::parse(l).unwrap().into());

    let o_ps_maps = o_ps_adjacency_list_files.map_all().await?;
    let s_p_maps = s_p_adjacency_list_files.map_all().await?;
    let o_ps_aj: AdjacencyList = o_ps_maps.into();
    let s_p_aj: AdjacencyList = s_p_maps.into();

    Ok(
        InternalLayerTripleObjectIterator::new(subjects, objects, o_ps_aj, s_p_aj)
            .seek_object(object),
    )
}

pub(crate) async fn file_triple_layer_count<F: FileLoad + FileStore>(
    s_p_nums_file: F,
    sp_o_bits_file: F,
    predicate_wavelet_files: BitIndexFiles<F>,
) -> io::Result<usize> {
    let (_, width) = logarray_file_get_length_and_width(s_p_nums_file).await?;
    let bits_len: usize = bitarray_len_from_file(sp_o_bits_file)
        .await?
        .try_into()
        .unwrap();
    let predicate_wavelet_maps = predicate_wavelet_files.map_all().await?;
    let wavelet_bits = predicate_wavelet_maps.into();
    let wtree = WaveletTree::from_parts(wavelet_bits, width);

    Ok(bits_len - wtree.lookup(0).map(|l| l.len()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::{Layer, ObjectType, ValueTriple};
    use crate::storage::directory::DirectoryLayerStore;
    use crate::storage::memory::MemoryLayerStore;
    use std::collections::HashMap;
    use std::io;
    use tempfile::{tempdir, TempDir};
    // these tests are for both the memory store and the directory store
    // They test functionality that should really work for both

    lazy_static! {
        static ref BASE_TRIPLES: Vec<ValueTriple> = vec![
            ValueTriple::new_string_value("cow", "says", "moo"),
            ValueTriple::new_string_value("cow", "says", "mooo"),
            ValueTriple::new_node("cow", "likes", "duck"),
            ValueTriple::new_node("cow", "likes", "pig"),
            ValueTriple::new_string_value("cow", "name", "clarabelle"),
            ValueTriple::new_string_value("pig", "says", "oink"),
            ValueTriple::new_node("pig", "hates", "cow"),
            ValueTriple::new_string_value("duck", "says", "quack"),
            ValueTriple::new_node("duck", "hates", "cow"),
            ValueTriple::new_node("duck", "hates", "pig"),
            ValueTriple::new_string_value("duck", "name", "donald"),
        ];
        static ref CHILD_ADDITION_TRIPLES: Vec<ValueTriple> = vec![
            ValueTriple::new_string_value("cow", "says", "moooo"),
            ValueTriple::new_string_value("cow", "says", "mooooo"),
            ValueTriple::new_node("cow", "likes", "horse"),
            ValueTriple::new_node("pig", "likes", "platypus"),
            ValueTriple::new_node("duck", "hates", "platypus"),
        ];
        static ref CHILD_REMOVAL_TRIPLES: Vec<ValueTriple> = vec![
            ValueTriple::new_string_value("cow", "says", "mooo"),
            ValueTriple::new_string_value("cow", "name", "clarabelle"),
            ValueTriple::new_node("pig", "hates", "cow"),
            ValueTriple::new_node("duck", "hates", "cow"),
            ValueTriple::new_node("duck", "hates", "pig"),
            ValueTriple::new_string_value("duck", "name", "donald"),
        ];
    }

    async fn example_base_layer<S: LayerStore>(
        store: &S,
        invalidate: bool,
    ) -> io::Result<(
        [u32; 5],
        Option<Arc<InternalLayer>>,
        HashMap<ValueTriple, IdTriple>,
    )> {
        let mut builder = store.create_base_layer().await?;
        let name = builder.name();
        for t in BASE_TRIPLES.iter() {
            builder.add_value_triple(t.clone());
        }
        builder.commit_boxed().await?;
        let layer = store.get_layer(name).await?.unwrap();

        let mut contents = HashMap::with_capacity(BASE_TRIPLES.len());
        for t in BASE_TRIPLES.iter() {
            let t_id = layer.value_triple_to_id(t).unwrap();
            contents.insert(t.clone(), t_id);
        }

        let layer_opt = match invalidate {
            true => None,
            false => Some(layer),
        };

        Ok((name, layer_opt, contents))
    }

    async fn example_child_layer<S: LayerStore>(
        store: &S,
        invalidate: bool,
    ) -> io::Result<(
        [u32; 5],
        Option<Arc<InternalLayer>>,
        HashMap<ValueTriple, IdTriple>,
        HashMap<ValueTriple, IdTriple>,
    )> {
        let (base_name, _base_layer, _) = example_base_layer(store, false).await?;
        let mut builder = store.create_child_layer(base_name).await?;
        let name = builder.name();
        for t in CHILD_ADDITION_TRIPLES.iter() {
            builder.add_value_triple(t.clone());
        }
        for t in CHILD_REMOVAL_TRIPLES.iter() {
            builder.remove_value_triple(t.clone());
        }
        builder.commit_boxed().await?;
        let layer = store.get_layer(name).await?.unwrap();

        let mut add_contents = HashMap::with_capacity(BASE_TRIPLES.len());
        for t in CHILD_ADDITION_TRIPLES.iter() {
            let t_id = layer.value_triple_to_id(t).unwrap();
            add_contents.insert(t.clone(), t_id);
        }

        let mut remove_contents = HashMap::with_capacity(BASE_TRIPLES.len());
        for t in CHILD_REMOVAL_TRIPLES.iter() {
            let t_id = layer.value_triple_to_id(t).unwrap();
            remove_contents.insert(t.clone(), t_id);
        }

        let layer_opt = match invalidate {
            true => None,
            false => Some(layer),
        };

        Ok((name, layer_opt, add_contents, remove_contents))
    }

    async fn base_layer_counts<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _) = example_base_layer(store, invalidate).await?;
        assert_eq!(11, store.triple_layer_addition_count(name).await?);
        assert_eq!(0, store.triple_layer_removal_count(name).await?);

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_counts() {
        let store = MemoryLayerStore::new();
        base_layer_counts(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_counts() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_counts(&store, false).await.unwrap();
    }

    async fn child_layer_counts<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _, _) = example_child_layer(store, invalidate).await?;
        assert_eq!(5, store.triple_layer_addition_count(name).await?);
        assert_eq!(6, store.triple_layer_removal_count(name).await?);

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_counts() {
        let store = MemoryLayerStore::new();
        child_layer_counts(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_counts() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_counts(&store, false).await.unwrap();
    }

    async fn base_layer_addition_exists<S: LayerStore>(
        store: &S,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, _layer, triples) = example_base_layer(store, invalidate).await?;

        for t in triples.values() {
            assert!(
                store
                    .triple_addition_exists(name, t.subject, t.predicate, t.object)
                    .await?
            );
        }

        assert!(!store.triple_addition_exists(name, 42, 42, 42).await?);
        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_addition_exists() {
        let store = MemoryLayerStore::new();
        base_layer_addition_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_addition_exists() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_addition_exists(&store, false).await.unwrap();
    }

    async fn base_layer_additions<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, triples) = example_base_layer(store, invalidate).await?;
        let mut values: Vec<_> = triples.values().cloned().collect();
        values.sort();

        let additions: Vec<_> = store.triple_additions(name).await?.collect();
        assert_eq!(values, additions);

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_additions() {
        let store = MemoryLayerStore::new();
        base_layer_additions(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_additions() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_additions(&store, false).await.unwrap();
    }

    async fn base_layer_additions_s<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(5, triples.len());

        let result: Vec<_> = store
            .triple_additions_s(name, triples[0].subject)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_s(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_additions_s() {
        let store = MemoryLayerStore::new();
        base_layer_additions_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_additions_s() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_additions_s(&store, false).await.unwrap();
    }

    async fn base_layer_additions_sp<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow" && t.predicate == "says")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_sp(name, triples[0].subject, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store
            .triple_additions_sp(name, 42, 42)
            .await?
            .next()
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_additions_sp() {
        let store = MemoryLayerStore::new();
        base_layer_additions_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_additions_sp() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_additions_sp(&store, false).await.unwrap();
    }

    async fn base_layer_additions_p<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.predicate == "says")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(4, triples.len());

        let result: Vec<_> = store
            .triple_additions_p(name, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_p(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_additions_p() {
        let store = MemoryLayerStore::new();
        base_layer_additions_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_additions_p() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_additions_p(&store, false).await.unwrap();
    }

    async fn base_layer_additions_o<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.object == ObjectType::Node("cow".to_string()))
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_o(name, triples[0].object)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_additions_o() {
        let store = MemoryLayerStore::new();
        base_layer_additions_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_additions_o() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_additions_o(&store, false).await.unwrap();
    }

    async fn base_layer_removals<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _) = example_base_layer(store, invalidate).await?;

        assert!(!store.triple_removal_exists(name, 42, 42, 42).await?);
        assert!(store.triple_removals(name).await?.next().is_none());
        assert!(store.triple_removals_s(name, 42).await?.next().is_none());
        assert!(store
            .triple_removals_sp(name, 42, 42)
            .await?
            .next()
            .is_none());
        assert!(store.triple_removals_p(name, 42).await?.next().is_none());
        assert!(store.triple_removals_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_base_layer_removals() {
        let store = MemoryLayerStore::new();
        base_layer_removals(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_base_layer_removals() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        base_layer_removals(&store, false).await.unwrap();
    }

    async fn child_layer_addition_exists<S: LayerStore>(
        store: &S,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, _layer, triples, _) = example_child_layer(store, invalidate).await?;

        for t in triples.values() {
            assert!(
                store
                    .triple_addition_exists(name, t.subject, t.predicate, t.object)
                    .await?
            );
        }

        assert!(!store.triple_addition_exists(name, 42, 42, 42).await?);
        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_addition_exists() {
        let store = MemoryLayerStore::new();
        child_layer_addition_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_addition_exists() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_addition_exists(&store, false).await.unwrap();
    }

    async fn child_layer_additions<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, triples, _) = example_child_layer(store, invalidate).await?;
        let mut values: Vec<_> = triples.values().cloned().collect();
        values.sort();

        let additions: Vec<_> = store.triple_additions(name).await?.collect();
        assert_eq!(values, additions);

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_additions() {
        let store = MemoryLayerStore::new();
        child_layer_additions(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_additions() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_additions(&store, false).await.unwrap();
    }

    async fn child_layer_additions_s<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(3, triples.len());

        let result: Vec<_> = store
            .triple_additions_s(name, triples[0].subject)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_s(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_additions_s() {
        let store = MemoryLayerStore::new();
        child_layer_additions_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_additions_s() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_additions_s(&store, false).await.unwrap();
    }

    async fn child_layer_additions_sp<S: LayerStore>(
        store: &S,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, _layer, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow" && t.predicate == "says")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_sp(name, triples[0].subject, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store
            .triple_additions_sp(name, 42, 42)
            .await?
            .next()
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_additions_sp() {
        let store = MemoryLayerStore::new();
        child_layer_additions_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_additions_sp() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_additions_sp(&store, false).await.unwrap();
    }

    async fn child_layer_additions_p<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.predicate == "likes")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_p(name, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_p(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_additions_p() {
        let store = MemoryLayerStore::new();
        child_layer_additions_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_additions_p() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_additions_p(&store, false).await.unwrap();
    }

    async fn child_layer_additions_o<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.object == ObjectType::Node("platypus".to_string()))
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_o(name, triples[0].object)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_additions_o() {
        let store = MemoryLayerStore::new();
        child_layer_additions_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_additions_o() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_additions_o(&store, false).await.unwrap();
    }

    async fn child_layer_removal_exists<S: LayerStore>(
        store: &S,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, _layer, _additions, removals) = example_child_layer(store, invalidate).await?;

        for t in removals.values() {
            assert!(
                store
                    .triple_removal_exists(name, t.subject, t.predicate, t.object)
                    .await?
            );
        }

        assert!(!store.triple_removal_exists(name, 42, 42, 42).await?);
        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_removal_exists() {
        let store = MemoryLayerStore::new();
        child_layer_removal_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_removal_exists() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_removal_exists(&store, false).await.unwrap();
    }

    async fn child_layer_removals<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _additions, removals) = example_child_layer(store, invalidate).await?;
        let mut values: Vec<_> = removals.values().cloned().collect();
        values.sort();

        let removals: Vec<_> = store.triple_removals(name).await?.collect();
        assert_eq!(values, removals);

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_removals() {
        let store = MemoryLayerStore::new();
        child_layer_removals(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_removals() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_removals(&store, false).await.unwrap();
    }

    async fn child_layer_removals_s<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.subject == "duck")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(3, triples.len());

        let result: Vec<_> = store
            .triple_removals_s(name, triples[0].subject)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_removals_s(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_removals_s() {
        let store = MemoryLayerStore::new();
        child_layer_removals_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_removals_s() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_removals_s(&store, false).await.unwrap();
    }

    async fn child_layer_removals_sp<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.subject == "duck" && t.predicate == "hates")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_removals_sp(name, triples[0].subject, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store
            .triple_removals_sp(name, 42, 42)
            .await?
            .next()
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_removals_sp() {
        let store = MemoryLayerStore::new();
        child_layer_removals_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_removals_sp() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_removals_sp(&store, false).await.unwrap();
    }

    async fn child_layer_removals_p<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.predicate == "hates")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(3, triples.len());

        let result: Vec<_> = store
            .triple_removals_p(name, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_removals_p(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_removals_p() {
        let store = MemoryLayerStore::new();
        child_layer_removals_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_removals_p() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_removals_p(&store, false).await.unwrap();
    }

    async fn child_layer_removals_o<S: LayerStore>(store: &S, invalidate: bool) -> io::Result<()> {
        let (name, _layer, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.object == ObjectType::Node("cow".to_string()))
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_removals_o(name, triples[0].object)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_removals_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn memory_child_layer_removals_o() {
        let store = MemoryLayerStore::new();
        child_layer_removals_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn directory_child_layer_removals_o() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        child_layer_removals_o(&store, false).await.unwrap();
    }

    fn make_cached_store() -> (TempDir, CachedLayerStore) {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        let cache = CachedLayerStore::new(store, LockingHashMapLayerCache::new());

        (dir, cache)
    }

    #[tokio::test]
    async fn cached_base_layer_counts() {
        let (_dir, store) = make_cached_store();
        base_layer_counts(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_counts() {
        let (_dir, store) = make_cached_store();
        base_layer_counts(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_counts() {
        let (_dir, store) = make_cached_store();
        child_layer_counts(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_counts() {
        let (_dir, store) = make_cached_store();
        child_layer_counts(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_addition_exists() {
        let (_dir, store) = make_cached_store();
        base_layer_addition_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_addition_exists() {
        let (_dir, store) = make_cached_store();
        base_layer_addition_exists(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_additions() {
        let (_dir, store) = make_cached_store();
        base_layer_additions(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions() {
        let (_dir, store) = make_cached_store();
        base_layer_additions(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_additions_s() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_s() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_s(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_additions_sp() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_sp() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_sp(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_additions_p() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_p() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_p(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_additions_o() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_o() {
        let (_dir, store) = make_cached_store();
        base_layer_additions_o(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_base_layer_removals() {
        let (_dir, store) = make_cached_store();
        base_layer_removals(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_removals() {
        let (_dir, store) = make_cached_store();
        base_layer_removals(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_addition_exists() {
        let (_dir, store) = make_cached_store();
        child_layer_addition_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_addition_exists() {
        let (_dir, store) = make_cached_store();
        child_layer_addition_exists(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_additions() {
        let (_dir, store) = make_cached_store();
        child_layer_additions(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions() {
        let (_dir, store) = make_cached_store();
        child_layer_additions(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_additions_s() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_s() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_s(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_additions_sp() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_sp() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_sp(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_additions_p() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_p() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_p(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_additions_o() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_o() {
        let (_dir, store) = make_cached_store();
        child_layer_additions_o(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_removal_exists() {
        let (_dir, store) = make_cached_store();
        child_layer_removal_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removal_exists() {
        let (_dir, store) = make_cached_store();
        child_layer_removal_exists(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_removals() {
        let (_dir, store) = make_cached_store();
        child_layer_removals(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals() {
        let (_dir, store) = make_cached_store();
        child_layer_removals(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_removals_s() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_s() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_s(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_removals_sp() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_sp() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_sp(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_removals_p() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_p() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_p(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn cached_child_layer_removals_o() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_o() {
        let (_dir, store) = make_cached_store();
        child_layer_removals_o(&store, true).await.unwrap();
    }
}
