//! storage traits that the builders and loaders can rely on

use std::io;

use bytes::Bytes;
pub use tdb_succinct::storage::{
    AdjacencyListFiles, AdjacencyListMaps, BitIndexFiles, BitIndexMaps, DictionaryFiles,
    DictionaryMaps, FileLoad, FileStore, SyncableFile, TypedDictionaryFiles, TypedDictionaryMaps,
};

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
