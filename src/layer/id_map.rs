#![allow(dead_code)]
use super::*;
use crate::storage::{FileLoad, FileStore, IdMapFiles};
use crate::structure::util::sorted_iterator;
use crate::structure::*;
use std::io;

pub struct IdMap {
    id_wtree: Option<WaveletTree>,
}

impl IdMap {
    pub fn from_parts(id_wtree: Option<WaveletTree>) -> Self {
        IdMap { id_wtree }
    }

    pub fn outer_to_inner(&self, id: u64) -> u64 {
        self.id_wtree
            .as_ref()
            .and_then(|wtree| {
                if id < wtree.len() as u64 {
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
                if id < wtree.len() as u64 {
                    None
                } else {
                    Some(wtree.decode_one(id as usize))
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

    construct_idmaps_from_slice(&layers, idmap_files).await
}

pub async fn construct_idmaps_upto<F: 'static + FileLoad + FileStore>(
    input: &InternalLayer,
    upto_layer_id: [u32; 5],
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let layers = input.immediate_layers_upto(upto_layer_id);

    construct_idmaps_from_slice(&layers, idmap_files).await
}

async fn construct_idmaps_from_slice<F: 'static + FileLoad + FileStore>(
    layers: &[&InternalLayer],
    idmap_files: IdMapFiles<F>,
) -> io::Result<()> {
    let node_iters = layers
        .iter()
        .map(|layer| layer.node_dict_entries_zero_index());
    let value_iters = layers
        .iter()
        .map(|layer| layer.value_dict_entries_zero_index());

    let node_value_iters: Vec<_> = node_iters.chain(value_iters).collect();
    let predicate_iters: Vec<_> = layers
        .iter()
        .map(|layer| layer.predicate_dict_entries_zero_index())
        .collect();

    let entry_comparator = |vals: &[Option<&(u64, PfcDictEntry)>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .min_by(|(_, x), (_, y)| x.cmp(y))
            .map(|x| x.0)
    };

    let sorted_node_value_iter = sorted_iterator(node_value_iters, entry_comparator);
    let sorted_predicate_iter = sorted_iterator(predicate_iters, entry_comparator);

    let node_value_build_task = tokio::spawn(build_wavelet_tree_from_iter(
        util::calculate_width(layers.last().unwrap().node_and_value_count() as u64),
        sorted_node_value_iter.map(|(id, _)| id),
        idmap_files.node_value_idmap_files.bits_file,
        idmap_files.node_value_idmap_files.blocks_file,
        idmap_files.node_value_idmap_files.sblocks_file,
    ));
    let predicate_build_task = tokio::spawn(build_wavelet_tree_from_iter(
        util::calculate_width(layers.last().unwrap().node_and_value_count() as u64),
        sorted_predicate_iter.map(|(id, _)| id),
        idmap_files.predicate_idmap_files.bits_file,
        idmap_files.predicate_idmap_files.blocks_file,
        idmap_files.predicate_idmap_files.sblocks_file,
    ));

    node_value_build_task.await??;
    predicate_build_task.await?
}
