#![allow(dead_code)]
use super::*;
use crate::storage::{BitIndexMaps, FileLoad, FileStore, IdMapFiles};
use crate::structure::util::sorted_iterator;
use crate::structure::*;
use std::convert::TryInto;
use std::io;

#[derive(Clone)]
pub struct IdMap {
    pub id_wtree: Option<WaveletTree>,
}

impl Default for IdMap {
    fn default() -> Self {
        Self::from_parts(None)
    }
}

impl IdMap {
    pub fn from_maps(maps: BitIndexMaps, width: u8) -> Self {
        let bitindex = BitIndex::from_maps(maps.bits_map, maps.blocks_map, maps.sblocks_map);
        let id_wtree = WaveletTree::from_parts(bitindex, width);

        Self::from_parts(Some(id_wtree))
    }

    pub fn from_parts(id_wtree: Option<WaveletTree>) -> Self {
        IdMap { id_wtree }
    }

    pub fn outer_to_inner(&self, id: u64) -> u64 {
        self.id_wtree
            .as_ref()
            .and_then(|wtree| {
                if id >= wtree.len() as u64 {
                    None
                } else {
                    Some(wtree.lookup_one(id).unwrap())
                }
            })
            .unwrap_or(id)
    }

    pub fn inner_to_outer(&self, id: u64) -> u64 {
        self.id_wtree
            .as_ref()
            .and_then(|wtree| {
                if id >= wtree.len() as u64 {
                    None
                } else {
                    Some(wtree.decode_one(id.try_into().unwrap()))
                }
            })
            .unwrap_or(id)
    }
}

pub async fn construct_idmaps<F: 'static + FileLoad + FileStore>(
    input: &InternalLayer,
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let layers = input.immediate_layers();

    construct_idmaps_from_slice(&layers, 0, 0, idmap_files).await
}

pub async fn construct_idmaps_upto<F: 'static + FileLoad + FileStore>(
    input: &InternalLayer,
    upto_layer_id: [u32; 5],
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let layers = input.immediate_layers_upto(upto_layer_id);
    let node_value_offset = layers
        .first()
        .map(|l| l.parent_node_value_count())
        .unwrap_or(0);
    let predicate_offset = layers
        .first()
        .map(|l| l.parent_predicate_count())
        .unwrap_or(0);

    construct_idmaps_from_slice(&layers, node_value_offset, predicate_offset, idmap_files).await
}

async fn construct_idmaps_from_slice<F: 'static + FileLoad + FileStore>(
    layers: &[&InternalLayer],
    node_value_offset: usize,
    predicate_offset: usize,
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let node_iters = layers
        .iter()
        .map(|layer| layer.node_dict_entries_zero_index())
        .collect();

    let value_iters = layers
        .iter()
        .map(|layer| layer.value_dict_entries_zero_index())
        .collect();

    let predicate_iters: Vec<_> = layers
        .iter()
        .map(|layer| layer.predicate_dict_entries_zero_index())
        .collect();

    let entry_comparator = |vals: &[Option<&(u64, PfcDictEntry)>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .min_by(|(_, x), (_, y)| x.unwrap().1.cmp(&y.unwrap().1))
            .map(|x| x.0)
    };

    let sorted_node_iter = sorted_iterator(node_iters, entry_comparator);
    let sorted_value_iter = sorted_iterator(value_iters, entry_comparator);
    let sorted_node_value_iter = sorted_node_iter.chain(sorted_value_iter);
    let sorted_predicate_iter = sorted_iterator(predicate_iters, entry_comparator);

    let node_value_width = util::calculate_width(
        (layers.last().unwrap().node_and_value_count() - node_value_offset) as u64,
    );
    let node_value_build_task = tokio::spawn(build_wavelet_tree_from_iter(
        node_value_width,
        sorted_node_value_iter.map(move |(id, _)| id - node_value_offset as u64),
        idmap_files.node_value_idmap_files.bits_file,
        idmap_files.node_value_idmap_files.blocks_file,
        idmap_files.node_value_idmap_files.sblocks_file,
    ));
    let predicate_width =
        util::calculate_width((layers.last().unwrap().predicate_count() - predicate_offset) as u64);
    let predicate_build_task = tokio::spawn(build_wavelet_tree_from_iter(
        predicate_width,
        sorted_predicate_iter.map(move |(id, _)| id - predicate_offset as u64),
        idmap_files.predicate_idmap_files.bits_file,
        idmap_files.predicate_idmap_files.blocks_file,
        idmap_files.predicate_idmap_files.sblocks_file,
    ));

    node_value_build_task.await??;
    predicate_build_task.await?
}
