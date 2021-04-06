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

pub async fn memory_construct_idmaps<F: 'static + FileLoad + FileStore>(
    input: &InternalLayer,
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let layers = input.immediate_layers();

    construct_idmaps_from_layers(&layers, idmap_files).await
}

pub async fn memory_construct_idmaps_upto<F: 'static + FileLoad + FileStore>(
    input: &InternalLayer,
    upto_layer_id: [u32; 5],
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let layers = input.immediate_layers_upto(upto_layer_id);

    construct_idmaps_from_layers(&layers, idmap_files).await
}

pub async fn construct_idmaps_from_structures<F: 'static + FileLoad + FileStore>(
    node_dicts: &[PfcDict],
    predicate_dicts: &[PfcDict],
    value_dicts: &[PfcDict],
    node_value_idmaps: &[IdMap],
    predicate_idmaps: &[IdMap],
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    debug_assert!(node_dicts.len() == predicate_dicts.len());
    debug_assert!(node_dicts.len() == value_dicts.len());
    debug_assert!(node_dicts.len() == node_value_idmaps.len());
    debug_assert!(node_dicts.len() == predicate_idmaps.len());

    let mut node_iters = Vec::with_capacity(node_dicts.len());
    let mut node_offset = 0;
    for (ix, dict) in node_dicts.iter().enumerate() {
        let idmap = node_value_idmaps[ix].clone();
        node_iters.push(
            dict.entries()
                .enumerate()
                .map(move |(i, e)| (idmap.inner_to_outer(i as u64) + node_offset as u64, e)),
        );

        node_offset += dict.len() + value_dicts[ix].len();
    }

    let mut value_iters = Vec::with_capacity(node_dicts.len());
    let mut value_offset = 0;
    for (ix, dict) in value_dicts.iter().enumerate() {
        let idmap = node_value_idmaps[ix].clone();
        let node_count = node_dicts[ix].len();
        value_iters.push(dict.entries().enumerate().map(move |(i, e)| {
            (
                idmap.inner_to_outer(i as u64 + node_count as u64) + value_offset as u64,
                e,
            )
        }));

        value_offset += node_count + dict.len();
    }

    let mut predicate_iters = Vec::with_capacity(node_dicts.len());
    let mut predicate_offset = 0;
    for (ix, dict) in predicate_dicts.iter().enumerate() {
        let idmap = predicate_idmaps[ix].clone();
        predicate_iters.push(
            dict.entries()
                .enumerate()
                .map(move |(i, e)| (idmap.inner_to_outer(i as u64) + predicate_offset as u64, e)),
        );

        predicate_offset += dict.len();
    }

    let entry_comparator = |vals: &[Option<&(u64, PfcDictEntry)>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .min_by(|(_, x), (_, y)| x.unwrap().1.cmp(&y.unwrap().1))
            .map(|x| x.0)
    };

    let sorted_node_iter = sorted_iterator(node_iters, entry_comparator);
    let sorted_value_iter = sorted_iterator(value_iters, entry_comparator);
    let sorted_node_value_iter = sorted_node_iter.chain(sorted_value_iter).map(|(id, _)| id);
    let sorted_predicate_iter =
        sorted_iterator(predicate_iters, entry_comparator).map(|(id, _)| id);

    let node_value_width = util::calculate_width(node_offset as u64);
    let node_value_build_task = tokio::spawn(build_wavelet_tree_from_iter(
        node_value_width,
        sorted_node_value_iter,
        idmap_files.node_value_idmap_files.bits_file,
        idmap_files.node_value_idmap_files.blocks_file,
        idmap_files.node_value_idmap_files.sblocks_file,
    ));
    let predicate_width = util::calculate_width(predicate_offset as u64);
    let predicate_build_task = tokio::spawn(build_wavelet_tree_from_iter(
        predicate_width,
        sorted_predicate_iter,
        idmap_files.predicate_idmap_files.bits_file,
        idmap_files.predicate_idmap_files.blocks_file,
        idmap_files.predicate_idmap_files.sblocks_file,
    ));

    node_value_build_task.await??;
    predicate_build_task.await?
}

async fn construct_idmaps_from_layers<F: 'static + FileLoad + FileStore>(
    layers: &[&InternalLayer],
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let node_dicts: Vec<_> = layers
        .iter()
        .map(|layer| layer.node_dictionary().clone())
        .collect();

    let predicate_dicts: Vec<_> = layers
        .iter()
        .map(|layer| layer.predicate_dictionary().clone())
        .collect();

    let value_dicts: Vec<_> = layers
        .iter()
        .map(|layer| layer.value_dictionary().clone())
        .collect();

    let node_value_idmaps: Vec<_> = layers
        .iter()
        .map(|layer| layer.node_value_id_map().clone())
        .collect();

    let predicate_idmaps: Vec<_> = layers
        .iter()
        .map(|layer| layer.node_value_id_map().clone())
        .collect();

    construct_idmaps_from_structures(
        &node_dicts,
        &predicate_dicts,
        &value_dicts,
        &node_value_idmaps,
        &predicate_idmaps,
        idmap_files,
    )
    .await
}
