//! storage traits that the builders and loaders can rely on

use bytes::Bytes;
use futures::future::{self, Future};
use futures::io;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::structure::{AdjacencyList, BitIndex};

pub trait FileStore: Clone + Send + Sync {
    type Write: AsyncWrite + Unpin + Send;
    fn open_write(&self) -> Self::Write {
        self.open_write_from(0)
    }
    fn open_write_from(&self, offset: usize) -> Self::Write;
}

pub trait FileLoad: Clone + Send + Sync {
    type Read: AsyncRead + Unpin + Send;

    // TODO - exists and size should also be future-enabled
    fn exists(&self) -> bool;
    fn size(&self) -> usize;
    fn open_read(&self) -> Self::Read {
        self.open_read_from(0)
    }
    fn open_read_from(&self, offset: usize) -> Self::Read;
    fn map(&self) -> Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>>;

    fn map_if_exists(&self) -> Pin<Box<dyn Future<Output = io::Result<Option<Bytes>>> + Send>> {
        Box::pin(match self.exists() {
            false => future::Either::Left(future::ok(None)),
            true => {
                let fut = self.map();
                future::Either::Right(async { Ok(Some(fut.await?)) })
            }
        })
    }
}

/// The files required for storing a layer
#[derive(Clone)]
pub enum LayerFiles<F: 'static + FileLoad + FileStore + Clone> {
    Base(BaseLayerFiles<F>),
    Child(ChildLayerFiles<F>),
}

impl<F: 'static + FileLoad + FileStore + Clone> LayerFiles<F> {
    pub fn into_base(self) -> BaseLayerFiles<F> {
        match self {
            Self::Base(b) => b,
            _ => panic!("layer files are not for base"),
        }
    }

    pub fn into_child(self) -> ChildLayerFiles<F> {
        match self {
            Self::Child(c) => c,
            _ => panic!("layer files are not for child"),
        }
    }
}

#[derive(Clone)]
pub struct BaseLayerFiles<F: 'static + FileLoad + FileStore> {
    pub node_dictionary_files: DictionaryFiles<F>,
    pub predicate_dictionary_files: DictionaryFiles<F>,
    pub value_dictionary_files: DictionaryFiles<F>,

    pub id_map_files: IdMapFiles<F>,

    pub subjects_file: F,
    pub objects_file: F,

    pub s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pub sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    pub o_ps_adjacency_list_files: AdjacencyListFiles<F>,

    pub predicate_wavelet_tree_files: BitIndexFiles<F>,
}

#[derive(Clone)]
pub struct BaseLayerMaps {
    pub node_dictionary_maps: DictionaryMaps,
    pub predicate_dictionary_maps: DictionaryMaps,
    pub value_dictionary_maps: DictionaryMaps,

    pub id_map_maps: IdMapMaps,

    pub subjects_map: Option<Bytes>,
    pub objects_map: Option<Bytes>,

    pub s_p_adjacency_list_maps: AdjacencyListMaps,
    pub sp_o_adjacency_list_maps: AdjacencyListMaps,

    pub o_ps_adjacency_list_maps: AdjacencyListMaps,

    pub predicate_wavelet_tree_maps: BitIndexMaps,
}

impl<F: FileLoad + FileStore> BaseLayerFiles<F> {
    pub async fn map_all(&self) -> io::Result<BaseLayerMaps> {
        let node_dictionary_maps = self.node_dictionary_files.map_all().await?;
        let predicate_dictionary_maps = self.predicate_dictionary_files.map_all().await?;
        let value_dictionary_maps = self.value_dictionary_files.map_all().await?;

        let id_map_maps = self.id_map_files.map_all().await?;

        let subjects_map = self.subjects_file.map_if_exists().await?;
        let objects_map = self.objects_file.map_if_exists().await?;

        let s_p_adjacency_list_maps = self.s_p_adjacency_list_files.map_all().await?;
        let sp_o_adjacency_list_maps = self.sp_o_adjacency_list_files.map_all().await?;
        let o_ps_adjacency_list_maps = self.o_ps_adjacency_list_files.map_all().await?;

        let predicate_wavelet_tree_maps = self.predicate_wavelet_tree_files.map_all().await?;

        Ok(BaseLayerMaps {
            node_dictionary_maps,
            predicate_dictionary_maps,
            value_dictionary_maps,

            id_map_maps,

            subjects_map,
            objects_map,

            s_p_adjacency_list_maps,
            sp_o_adjacency_list_maps,
            o_ps_adjacency_list_maps,

            predicate_wavelet_tree_maps,
        })
    }
}

#[derive(Clone)]
pub struct ChildLayerFiles<F: 'static + FileLoad + FileStore + Clone + Send + Sync> {
    pub node_dictionary_files: DictionaryFiles<F>,
    pub predicate_dictionary_files: DictionaryFiles<F>,
    pub value_dictionary_files: DictionaryFiles<F>,

    pub id_map_files: IdMapFiles<F>,

    pub pos_subjects_file: F,
    pub pos_objects_file: F,
    pub neg_subjects_file: F,
    pub neg_objects_file: F,

    pub pos_s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pub pos_sp_o_adjacency_list_files: AdjacencyListFiles<F>,
    pub pos_o_ps_adjacency_list_files: AdjacencyListFiles<F>,
    pub neg_s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pub neg_sp_o_adjacency_list_files: AdjacencyListFiles<F>,
    pub neg_o_ps_adjacency_list_files: AdjacencyListFiles<F>,

    pub pos_predicate_wavelet_tree_files: BitIndexFiles<F>,
    pub neg_predicate_wavelet_tree_files: BitIndexFiles<F>,
}

#[derive(Clone)]
pub struct ChildLayerMaps {
    pub node_dictionary_maps: DictionaryMaps,
    pub predicate_dictionary_maps: DictionaryMaps,
    pub value_dictionary_maps: DictionaryMaps,

    pub id_map_maps: IdMapMaps,

    pub pos_subjects_map: Bytes,
    pub pos_objects_map: Bytes,
    pub neg_subjects_map: Bytes,
    pub neg_objects_map: Bytes,

    pub pos_s_p_adjacency_list_maps: AdjacencyListMaps,
    pub pos_sp_o_adjacency_list_maps: AdjacencyListMaps,
    pub pos_o_ps_adjacency_list_maps: AdjacencyListMaps,
    pub neg_s_p_adjacency_list_maps: AdjacencyListMaps,
    pub neg_sp_o_adjacency_list_maps: AdjacencyListMaps,
    pub neg_o_ps_adjacency_list_maps: AdjacencyListMaps,

    pub pos_predicate_wavelet_tree_maps: BitIndexMaps,
    pub neg_predicate_wavelet_tree_maps: BitIndexMaps,
}

impl<F: FileLoad + FileStore + Clone> ChildLayerFiles<F> {
    pub async fn map_all(&self) -> io::Result<ChildLayerMaps> {
        let node_dictionary_maps = self.node_dictionary_files.map_all().await?;
        let predicate_dictionary_maps = self.predicate_dictionary_files.map_all().await?;
        let value_dictionary_maps = self.value_dictionary_files.map_all().await?;

        let id_map_maps = self.id_map_files.map_all().await?;

        let pos_subjects_map = self.pos_subjects_file.map().await?;
        let neg_subjects_map = self.neg_subjects_file.map().await?;
        let pos_objects_map = self.pos_objects_file.map().await?;
        let neg_objects_map = self.neg_objects_file.map().await?;

        let pos_s_p_adjacency_list_maps = self.pos_s_p_adjacency_list_files.map_all().await?;
        let pos_sp_o_adjacency_list_maps = self.pos_sp_o_adjacency_list_files.map_all().await?;
        let pos_o_ps_adjacency_list_maps = self.pos_o_ps_adjacency_list_files.map_all().await?;

        let neg_s_p_adjacency_list_maps = self.neg_s_p_adjacency_list_files.map_all().await?;
        let neg_sp_o_adjacency_list_maps = self.neg_sp_o_adjacency_list_files.map_all().await?;
        let neg_o_ps_adjacency_list_maps = self.neg_o_ps_adjacency_list_files.map_all().await?;

        let pos_predicate_wavelet_tree_maps =
            self.pos_predicate_wavelet_tree_files.map_all().await?;
        let neg_predicate_wavelet_tree_maps =
            self.neg_predicate_wavelet_tree_files.map_all().await?;

        Ok(ChildLayerMaps {
            node_dictionary_maps,
            predicate_dictionary_maps,
            value_dictionary_maps,

            id_map_maps,

            pos_subjects_map,
            pos_objects_map,
            neg_subjects_map,
            neg_objects_map,

            pos_s_p_adjacency_list_maps,
            pos_sp_o_adjacency_list_maps,
            pos_o_ps_adjacency_list_maps,
            neg_s_p_adjacency_list_maps,
            neg_sp_o_adjacency_list_maps,
            neg_o_ps_adjacency_list_maps,

            pos_predicate_wavelet_tree_maps,
            neg_predicate_wavelet_tree_maps,
        })
    }
}

#[derive(Clone)]
pub struct DictionaryMaps {
    pub blocks_map: Bytes,
    pub offsets_map: Bytes,
}

#[derive(Clone)]
pub struct DictionaryFiles<F: 'static + FileLoad + FileStore> {
    pub blocks_file: F,
    pub offsets_file: F,
    //    pub map_files: Option<BitIndexFiles<F>>
}

impl<F: 'static + FileLoad + FileStore> DictionaryFiles<F> {
    pub async fn map_all(&self) -> io::Result<DictionaryMaps> {
        let blocks_map = self.blocks_file.map().await?;
        let offsets_map = self.offsets_file.map().await?;

        Ok(DictionaryMaps {
            blocks_map,
            offsets_map,
        })
    }
}

#[derive(Clone)]
pub struct IdMapMaps {
    pub node_value_idmap_maps: Option<BitIndexMaps>,
    pub predicate_idmap_maps: Option<BitIndexMaps>,
}

#[derive(Clone)]
pub struct IdMapFiles<F: 'static + FileLoad + FileStore> {
    pub node_value_idmap_files: BitIndexFiles<F>,
    pub predicate_idmap_files: BitIndexFiles<F>,
}

impl<F: 'static + FileLoad + FileStore> IdMapFiles<F> {
    pub async fn map_all(&self) -> io::Result<IdMapMaps> {
        let node_value_idmap_maps = self.node_value_idmap_files.map_all_if_exists().await?;
        let predicate_idmap_maps = self.predicate_idmap_files.map_all_if_exists().await?;

        Ok(IdMapMaps {
            node_value_idmap_maps,
            predicate_idmap_maps,
        })
    }
}

#[derive(Clone)]
pub struct BitIndexMaps {
    pub bits_map: Bytes,
    pub blocks_map: Bytes,
    pub sblocks_map: Bytes,
}

impl Into<BitIndex> for BitIndexMaps {
    fn into(self) -> BitIndex {
        BitIndex::from_maps(self.bits_map, self.blocks_map, self.sblocks_map)
    }
}

#[derive(Clone)]
pub struct BitIndexFiles<F: 'static + FileLoad> {
    pub bits_file: F,
    pub blocks_file: F,
    pub sblocks_file: F,
}

impl<F: 'static + FileLoad + FileStore> BitIndexFiles<F> {
    pub async fn map_all(&self) -> io::Result<BitIndexMaps> {
        let bits_map = self.bits_file.map().await?;
        let blocks_map = self.blocks_file.map().await?;
        let sblocks_map = self.sblocks_file.map().await?;

        Ok(BitIndexMaps {
            bits_map,
            blocks_map,
            sblocks_map,
        })
    }

    pub async fn map_all_if_exists(&self) -> io::Result<Option<BitIndexMaps>> {
        if self.bits_file.exists() {
            Ok(Some(self.map_all().await?))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct AdjacencyListMaps {
    pub bitindex_maps: BitIndexMaps,
    pub nums_map: Bytes,
}

impl Into<AdjacencyList> for AdjacencyListMaps {
    fn into(self) -> AdjacencyList {
        AdjacencyList::parse(
            self.nums_map,
            self.bitindex_maps.bits_map,
            self.bitindex_maps.blocks_map,
            self.bitindex_maps.sblocks_map,
        )
    }
}

#[derive(Clone)]
pub struct AdjacencyListFiles<F: 'static + FileLoad> {
    pub bitindex_files: BitIndexFiles<F>,
    pub nums_file: F,
}

impl<F: 'static + FileLoad + FileStore> AdjacencyListFiles<F> {
    pub async fn map_all(&self) -> io::Result<AdjacencyListMaps> {
        let bitindex_maps = self.bitindex_files.map_all().await?;
        let nums_map = self.nums_file.map().await?;

        Ok(AdjacencyListMaps {
            bitindex_maps,
            nums_map,
        })
    }
}
