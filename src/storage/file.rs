//! storage traits that the builders and loaders can rely on

use std::io;

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use async_trait::async_trait;

use crate::structure::{
    indexed_property::IndexPropertyBuffers, AdjacencyList, AdjacencyListBuffers, BitIndex,
};

#[async_trait]
pub trait SyncableFile: AsyncWrite + Unpin + Send {
    async fn sync_all(self) -> io::Result<()>;
}

#[async_trait]
pub trait FileStore: Clone + Send + Sync {
    type Write: SyncableFile;
    async fn open_write(&self) -> io::Result<Self::Write>;
    async fn write_bytes(&self, mut bytes: Bytes) -> io::Result<()> {
        let mut writable = self.open_write().await?;
        writable.write_all_buf(&mut bytes).await?;
        writable.flush().await?;
        writable.sync_all().await
    }
}

#[async_trait]
pub trait FileLoad: Clone + Send + Sync {
    type Read: AsyncRead + Unpin + Send;

    async fn exists(&self) -> io::Result<bool>;
    async fn size(&self) -> io::Result<usize>;
    async fn open_read(&self) -> io::Result<Self::Read> {
        self.open_read_from(0).await
    }
    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read>;
    async fn map(&self) -> io::Result<Bytes>;

    async fn map_if_exists(&self) -> io::Result<Option<Bytes>> {
        match self.exists().await? {
            false => Ok(None),
            true => {
                let mapped = self.map().await?;
                Ok(Some(mapped))
            }
        }
    }
}

/// The files required for storing a layer
#[derive(Clone)]
pub enum LayerFiles<F: 'static + FileLoad + FileStore + Clone> {
    Base(BaseLayerFiles<F>),
    Child(ChildLayerFiles<F>),
}

impl<F: 'static + FileLoad + FileStore + Clone> LayerFiles<F> {
    pub fn node_dictionary_files(&self) -> &DictionaryFiles<F> {
        match self {
            Self::Base(b) => &b.node_dictionary_files,
            Self::Child(c) => &c.node_dictionary_files,
        }
    }

    pub fn predicate_dictionary_files(&self) -> &DictionaryFiles<F> {
        match self {
            Self::Base(b) => &b.predicate_dictionary_files,
            Self::Child(c) => &c.predicate_dictionary_files,
        }
    }

    pub fn value_dictionary_files(&self) -> &TypedDictionaryFiles<F> {
        match self {
            Self::Base(b) => &b.value_dictionary_files,
            Self::Child(c) => &c.value_dictionary_files,
        }
    }

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
    pub value_dictionary_files: TypedDictionaryFiles<F>,

    pub id_map_files: IdMapFiles<F>,

    pub subjects_file: F,
    pub objects_file: F,

    pub s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pub sp_o_adjacency_list_files: AdjacencyListFiles<F>,
    pub indexed_property_files: IndexedPropertyFiles<F>,

    pub o_ps_adjacency_list_files: AdjacencyListFiles<F>,

    pub predicate_wavelet_tree_files: BitIndexFiles<F>,
}

#[derive(Clone)]
pub struct BaseLayerMaps {
    pub node_dictionary_maps: DictionaryMaps,
    pub predicate_dictionary_maps: DictionaryMaps,
    pub value_dictionary_maps: TypedDictionaryMaps,

    pub id_map_maps: IdMapMaps,

    pub subjects_map: Option<Bytes>,
    pub objects_map: Option<Bytes>,

    pub s_p_adjacency_list_maps: AdjacencyListMaps,
    pub sp_o_adjacency_list_maps: AdjacencyListMaps,

    pub o_ps_adjacency_list_maps: AdjacencyListMaps,

    pub predicate_wavelet_tree_maps: BitIndexMaps,
    pub index_property_maps: Option<IndexPropertyMaps>,
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
        let index_property_maps = self.indexed_property_files.map_all_if_exists().await?;
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
            index_property_maps,

            o_ps_adjacency_list_maps,
            predicate_wavelet_tree_maps,
        })
    }
}

#[derive(Clone)]
pub struct ChildLayerFiles<F: 'static + FileLoad + FileStore + Clone + Send + Sync> {
    pub node_dictionary_files: DictionaryFiles<F>,
    pub predicate_dictionary_files: DictionaryFiles<F>,
    pub value_dictionary_files: TypedDictionaryFiles<F>,

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

    pub indexed_property_files: IndexedPropertyFiles<F>,
}

#[derive(Clone)]
pub struct ChildLayerMaps {
    pub node_dictionary_maps: DictionaryMaps,
    pub predicate_dictionary_maps: DictionaryMaps,
    pub value_dictionary_maps: TypedDictionaryMaps,

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
    pub index_property_maps: Option<IndexPropertyMaps>,
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

        let index_property_maps = self.indexed_property_files.map_all_if_exists().await?;

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
            index_property_maps,
        })
    }
}

#[derive(Clone)]
pub struct TypedDictionaryMaps {
    pub types_present_map: Bytes,
    pub type_offsets_map: Bytes,
    pub blocks_map: Bytes,
    pub offsets_map: Bytes,
}

#[derive(Clone)]
pub struct TypedDictionaryFiles<F: 'static + FileLoad + FileStore> {
    pub types_present_file: F,
    pub type_offsets_file: F,
    pub blocks_file: F,
    pub offsets_file: F,
}

impl<F: 'static + FileLoad + FileStore> TypedDictionaryFiles<F> {
    pub async fn map_all(&self) -> io::Result<TypedDictionaryMaps> {
        let types_present_map = self.types_present_file.map().await?;
        let type_offsets_map = self.type_offsets_file.map().await?;
        let offsets_map = self.offsets_file.map().await?;
        let blocks_map = self.blocks_file.map().await?;

        Ok(TypedDictionaryMaps {
            types_present_map,
            type_offsets_map,
            offsets_map,
            blocks_map,
        })
    }

    pub async fn write_all_from_bufs<B1: Buf, B2: Buf, B3: Buf, B4: Buf>(
        &self,
        types_present_buf: &mut B1,
        type_offsets_buf: &mut B2,
        offsets_buf: &mut B3,
        blocks_buf: &mut B4,
    ) -> io::Result<()> {
        let mut types_present_writer = self.types_present_file.open_write().await?;
        let mut type_offsets_writer = self.type_offsets_file.open_write().await?;
        let mut offsets_writer = self.offsets_file.open_write().await?;
        let mut blocks_writer = self.blocks_file.open_write().await?;

        types_present_writer
            .write_all_buf(types_present_buf)
            .await?;
        type_offsets_writer.write_all_buf(type_offsets_buf).await?;
        offsets_writer.write_all_buf(offsets_buf).await?;
        blocks_writer.write_all_buf(blocks_buf).await?;

        types_present_writer.flush().await?;
        types_present_writer.sync_all().await?;

        type_offsets_writer.flush().await?;
        type_offsets_writer.sync_all().await?;

        offsets_writer.flush().await?;
        offsets_writer.sync_all().await?;

        blocks_writer.flush().await?;
        blocks_writer.sync_all().await?;

        Ok(())
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
        let offsets_map = self.offsets_file.map().await?;
        let blocks_map = self.blocks_file.map().await?;

        Ok(DictionaryMaps {
            offsets_map,
            blocks_map,
        })
    }

    pub async fn write_all_from_bufs<B1: Buf, B2: Buf>(
        &self,
        blocks_buf: &mut B1,
        offsets_buf: &mut B2,
    ) -> io::Result<()> {
        let mut offsets_writer = self.offsets_file.open_write().await?;
        let mut blocks_writer = self.blocks_file.open_write().await?;

        offsets_writer.write_all_buf(offsets_buf).await?;
        blocks_writer.write_all_buf(blocks_buf).await?;

        offsets_writer.flush().await?;
        offsets_writer.sync_all().await?;

        blocks_writer.flush().await?;
        blocks_writer.sync_all().await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct IndexedPropertyFiles<F: 'static + FileLoad + FileStore> {
    pub subjects_logarray_file: F,
    pub adjacency_files: AdjacencyListFiles<F>,
    pub objects_logarray_file: F,
}

#[derive(Clone)]
pub struct IndexPropertyMaps {
    pub subjects_logarray_map: Bytes,
    pub adjacency_maps: AdjacencyListMaps,
    pub objects_logarray_map: Bytes,
}

impl<F: 'static + FileLoad + FileStore> IndexedPropertyFiles<F> {
    pub async fn map_all_if_exists(&self) -> io::Result<Option<IndexPropertyMaps>> {
        if let Some(subjects_logarray_map) = self.subjects_logarray_file.map_if_exists().await? {
            Ok(Some(IndexPropertyMaps {
                subjects_logarray_map,
                adjacency_maps: self.adjacency_files.map_all().await?,
                objects_logarray_map: self.objects_logarray_file.map().await?,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn write_from_maps(&self, maps: IndexPropertyMaps) -> io::Result<()> {
        self.subjects_logarray_file
            .write_bytes(maps.subjects_logarray_map)
            .await?;
        self.adjacency_files
            .write_from_maps(maps.adjacency_maps)
            .await?;
        self.objects_logarray_file
            .write_bytes(maps.objects_logarray_map)
            .await?;

        Ok(())
    }
}

// a little silly
impl Into<IndexPropertyBuffers> for IndexPropertyMaps {
    fn into(self) -> IndexPropertyBuffers {
        IndexPropertyBuffers {
            subjects_logarray_buf: self.subjects_logarray_map,
            adjacency_bufs: self.adjacency_maps.into(),
            objects_logarray_buf: self.objects_logarray_map,
        }
    }
}
impl From<IndexPropertyBuffers> for IndexPropertyMaps {
    fn from(value: IndexPropertyBuffers) -> Self {
        Self {
            subjects_logarray_map: value.subjects_logarray_buf,
            adjacency_maps: value.adjacency_bufs.into(),
            objects_logarray_map: value.objects_logarray_buf,
        }
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
        if self.bits_file.exists().await? {
            Ok(Some(self.map_all().await?))
        } else {
            Ok(None)
        }
    }

    pub async fn write_from_maps(&self, maps: BitIndexMaps) -> io::Result<()> {
        self.bits_file.write_bytes(maps.bits_map).await?;
        self.blocks_file.write_bytes(maps.blocks_map).await?;
        self.sblocks_file.write_bytes(maps.sblocks_map).await?;

        Ok(())
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

impl Into<AdjacencyListBuffers> for AdjacencyListMaps {
    fn into(self) -> AdjacencyListBuffers {
        AdjacencyListBuffers {
            nums: self.nums_map,
            bits: self.bitindex_maps.bits_map,
            bitindex_blocks: self.bitindex_maps.blocks_map,
            bitindex_sblocks: self.bitindex_maps.sblocks_map,
        }
    }
}

impl From<AdjacencyListBuffers> for AdjacencyListMaps {
    fn from(value: AdjacencyListBuffers) -> Self {
        Self {
            nums_map: value.nums,
            bitindex_maps: BitIndexMaps {
                bits_map: value.bits,
                blocks_map: value.bitindex_blocks,
                sblocks_map: value.bitindex_sblocks,
            },
        }
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

    pub async fn write_from_maps(&self, maps: AdjacencyListMaps) -> io::Result<()> {
        self.bitindex_files
            .write_from_maps(maps.bitindex_maps)
            .await?;
        self.nums_file.write_bytes(maps.nums_map).await?;

        Ok(())
    }
}
