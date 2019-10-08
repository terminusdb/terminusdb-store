//! storage traits that the builders and loaders can rely on

use futures::prelude::*;
use tokio::prelude::*;

pub trait FileStore: Clone+Send+Sync {
    type Write: AsyncWrite+Send+Sync;
    fn open_write(&self) -> Self::Write {
        self.open_write_from(0)
    }
    fn open_write_from(&self, offset: usize) -> Self::Write;
}

pub trait FileLoad: Clone+Send+Sync {
    type Read: AsyncRead+Send+Sync;
    type Map: AsRef<[u8]>+Clone+Send+Sync;
    
    fn size(&self) -> usize;
    fn open_read(&self) -> Self::Read {
        self.open_read_from(0)
    }
    fn open_read_from(&self, offset: usize) -> Self::Read;
    fn map(&self) -> Box<dyn Future<Item=Self::Map, Error=std::io::Error>+Send+Sync>;
}

/// The files required for storing a layer
#[derive(Clone)]
pub enum LayerFiles<F:'static+FileLoad+FileStore+Clone> {
    Base(BaseLayerFiles<F>),
    Child(ChildLayerFiles<F>)
}

impl<F:'static+FileLoad+FileStore+Clone> LayerFiles<F> {
    pub fn into_base(self) -> BaseLayerFiles<F> {
        match self {
            Self::Base(b) => b,
            _ => panic!("layer files are not for base")
        }
    }

    pub fn into_child(self) -> ChildLayerFiles<F> {
        match self {
            Self::Child(c) => c,
            _ => panic!("layer files are not for child")
        }
    }
}

#[derive(Clone)]
pub struct BaseLayerFiles<F:'static+FileLoad+FileStore> {
    pub node_dictionary_files: DictionaryFiles<F>,
    pub predicate_dictionary_files: DictionaryFiles<F>,
    pub value_dictionary_files: DictionaryFiles<F>,

    pub s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pub sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    pub o_ps_adjacency_list_files: AdjacencyListFiles<F>,

    pub predicate_wavelet_tree_files: BitIndexFiles<F>,
}

#[derive(Clone)]
pub struct BaseLayerMaps<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    pub node_dictionary_maps: DictionaryMaps<M>,
    pub predicate_dictionary_maps: DictionaryMaps<M>,
    pub value_dictionary_maps: DictionaryMaps<M>,

    pub s_p_adjacency_list_maps: AdjacencyListMaps<M>,
    pub sp_o_adjacency_list_maps: AdjacencyListMaps<M>,

    pub o_ps_adjacency_list_maps: AdjacencyListMaps<M>,

    pub predicate_wavelet_tree_maps: BitIndexMaps<M>,
}

impl<F:FileLoad+FileStore> BaseLayerFiles<F> {
    pub fn map_all(&self) -> impl Future<Item=BaseLayerMaps<F::Map>,Error=std::io::Error> {
        let dict_futs = vec![self.node_dictionary_files.map_all(),
                             self.predicate_dictionary_files.map_all(),
                             self.value_dictionary_files.map_all()];

        let aj_futs = vec![self.s_p_adjacency_list_files.map_all(),
                           self.sp_o_adjacency_list_files.map_all(),
                           self.o_ps_adjacency_list_files.map_all()];

        future::join_all(dict_futs).join(future::join_all(aj_futs)).join(self.predicate_wavelet_tree_files.map_all())
            .map(|((dict_results, aj_results), predicate_wavelet_tree_maps)| BaseLayerMaps {
                node_dictionary_maps: dict_results[0].clone(),
                predicate_dictionary_maps: dict_results[1].clone(),
                value_dictionary_maps: dict_results[2].clone(),

                s_p_adjacency_list_maps: aj_results[0].clone(),
                sp_o_adjacency_list_maps: aj_results[1].clone(),

                o_ps_adjacency_list_maps: aj_results[2].clone(),

                predicate_wavelet_tree_maps
            })
    }
}

#[derive(Clone)]
pub struct ChildLayerFiles<F:'static+FileLoad+FileStore+Clone+Send+Sync> {
    pub node_dictionary_files: DictionaryFiles<F>,
    pub predicate_dictionary_files: DictionaryFiles<F>,
    pub value_dictionary_files: DictionaryFiles<F>,

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

    pub predicate_wavelet_tree_files: BitIndexFiles<F>,
}

#[derive(Clone)]
pub struct ChildLayerMaps<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    pub node_dictionary_maps: DictionaryMaps<M>,
    pub predicate_dictionary_maps: DictionaryMaps<M>,
    pub value_dictionary_maps: DictionaryMaps<M>,

    pub pos_subjects_map: M,
    pub pos_objects_map: M,
    pub neg_subjects_map: M,
    pub neg_objects_map: M,

    pub pos_s_p_adjacency_list_maps: AdjacencyListMaps<M>,
    pub pos_sp_o_adjacency_list_maps: AdjacencyListMaps<M>,
    pub pos_o_ps_adjacency_list_maps: AdjacencyListMaps<M>,
    pub neg_s_p_adjacency_list_maps: AdjacencyListMaps<M>,
    pub neg_sp_o_adjacency_list_maps: AdjacencyListMaps<M>,
    pub neg_o_ps_adjacency_list_maps: AdjacencyListMaps<M>,

    pub predicate_wavelet_tree_maps: BitIndexMaps<M>,
}

impl<F:FileLoad+FileStore+Clone> ChildLayerFiles<F> {
    pub fn map_all(&self) -> impl Future<Item=ChildLayerMaps<F::Map>,Error=std::io::Error> {
        let dict_futs = vec![self.node_dictionary_files.map_all(),
                             self.predicate_dictionary_files.map_all(),
                             self.value_dictionary_files.map_all()];

        let sub_futs = vec![self.pos_subjects_file.map(),
                            self.pos_objects_file.map(),
                            self.neg_subjects_file.map(),
                            self.neg_objects_file.map()];

        let aj_futs = vec![self.pos_s_p_adjacency_list_files.map_all(),
                           self.pos_sp_o_adjacency_list_files.map_all(),
                           self.pos_o_ps_adjacency_list_files.map_all(),
                           self.neg_s_p_adjacency_list_files.map_all(),
                           self.neg_sp_o_adjacency_list_files.map_all(),
                           self.neg_o_ps_adjacency_list_files.map_all()];

        future::join_all(dict_futs)
            .join(future::join_all(sub_futs))
            .join(future::join_all(aj_futs))
            .join(self.predicate_wavelet_tree_files.map_all())
            .map(|(((dict_results, sub_results), aj_results), predicate_wavelet_tree_maps)| ChildLayerMaps {
                node_dictionary_maps: dict_results[0].clone(),
                predicate_dictionary_maps: dict_results[1].clone(),
                value_dictionary_maps: dict_results[2].clone(),

                pos_subjects_map: sub_results[0].clone(),
                pos_objects_map: sub_results[1].clone(),
                neg_subjects_map: sub_results[2].clone(),
                neg_objects_map: sub_results[3].clone(),

                pos_s_p_adjacency_list_maps: aj_results[0].clone(),
                pos_sp_o_adjacency_list_maps: aj_results[1].clone(),
                pos_o_ps_adjacency_list_maps: aj_results[2].clone(),
                neg_s_p_adjacency_list_maps: aj_results[3].clone(),
                neg_sp_o_adjacency_list_maps: aj_results[4].clone(),
                neg_o_ps_adjacency_list_maps: aj_results[5].clone(),

                predicate_wavelet_tree_maps,
            })
    }
}

#[derive(Clone)]
pub struct DictionaryMaps<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    pub blocks_map: M,
    pub offsets_map: M
}

#[derive(Clone)]
pub struct AdjacencyListMaps<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    pub bitindex_maps: BitIndexMaps<M>,
    pub nums_map: M,
}

#[derive(Clone)]
pub struct DictionaryFiles<F:'static+FileLoad+FileStore> {
    pub blocks_file: F,
    pub offsets_file: F
}

impl<F:'static+FileLoad+FileStore> DictionaryFiles<F> {
    pub fn map_all(&self) -> impl Future<Item=DictionaryMaps<F::Map>, Error=std::io::Error> {
        let futs = vec![self.blocks_file.map(), self.offsets_file.map()];
        future::join_all(futs)
            .map(|results| DictionaryMaps {
                blocks_map: results[0].clone(),
                offsets_map: results[1].clone()
            })
    }
}

#[derive(Clone)]
pub struct AdjacencyListFiles<F:'static+FileLoad+FileStore> {
    pub bitindex_files: BitIndexFiles<F>,
    pub nums_file: F,
}

impl<F:'static+FileLoad+FileStore> AdjacencyListFiles<F> {
    pub fn map_all(&self) -> impl Future<Item=AdjacencyListMaps<F::Map>, Error=std::io::Error> {
        self.bitindex_files.map_all().join(self.nums_file.map())
            .map(|(bitindex_maps, nums_map)| AdjacencyListMaps {
                bitindex_maps,
                nums_map
            })
    }
}

#[derive(Clone)]
pub struct BitIndexMaps<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    pub bits_map: M,
    pub blocks_map: M,
    pub sblocks_map: M,
}

#[derive(Clone)]
pub struct BitIndexFiles<F:'static+FileLoad+FileStore> {
    pub bits_file: F,
    pub blocks_file: F,
    pub sblocks_file: F,
}

impl<F:'static+FileLoad+FileStore> BitIndexFiles<F> {
    pub fn map_all(&self) -> impl Future<Item=BitIndexMaps<F::Map>, Error=std::io::Error> {
        let futs = vec![self.bits_file.map(), self.blocks_file.map(), self.sblocks_file.map()];
        future::join_all(futs)
            .map(|results| BitIndexMaps {
                bits_map: results[0].clone(),
                blocks_map: results[1].clone(),
                sblocks_map: results[2].clone(),
            })
    }
}
