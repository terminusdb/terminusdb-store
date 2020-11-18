#![allow(dead_code)]
use super::*;
use crate::structure::*;
use crate::structure::util::sorted_iterator;
use crate::storage::{FileLoad, FileStore, BitIndexFiles};
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

pub async fn construct_idmaps<F:'static+FileLoad+FileStore>(input: InternalLayer, node_value_idmap_files: BitIndexFiles<F>, predicate_idmap_files: BitIndexFiles<F>) -> io::Result<()> {
    // there's two idmaps to construct, one for the node+value, and one for the predicate
    // the input is a layer.
    // we gotta construct an iterator over all the dictionary entry ids
    // this iterator has to be sorted by the dictionary entry itself, so that we yield a list of ids in lex order
    // this is our id map which will work with the merged dict
    // remember that for node+value, in the merged dict all nodes will precede all values.
    // so the sorted iterator for it will need to put all nodes before all values too.
    // so i guess the initial work is just to be able to construct those iterators.
    let layers = input.immediate_layers();

    let node_iters = layers.iter().map(|layer| layer.node_dict_entries_zero_index());
    let value_iters = layers.iter().map(|layer| layer.value_dict_entries_zero_index());

    let node_value_iters:Vec<_> = node_iters.chain(value_iters).collect();
    let predicate_iters: Vec<_> = layers.iter().map(|layer| layer.predicate_dict_entries_zero_index()).collect();

    let entry_comparator = |vals:&[Option<&(u64, PfcDictEntry)>]| {
        vals.iter()
            .enumerate()
            .filter(|(_,x)|x.is_some())
            .min_by(|(_,x), (_,y)| x.cmp(y))
            .map(|x| x.0)
    };

    let sorted_node_value_iter = sorted_iterator(node_value_iters,
                                                 entry_comparator);
    let sorted_predicate_iter = sorted_iterator(predicate_iters,
                                                entry_comparator);

    let node_value_build_task = tokio::spawn(
        build_wavelet_tree_from_iter(util::calculate_width(input.node_and_value_count() as u64),
                                     sorted_node_value_iter
                                     .map(|(id,_)|id),
                                     node_value_idmap_files.bits_file,
                                     node_value_idmap_files.blocks_file,
                                     node_value_idmap_files.sblocks_file));
    let predicate_build_task = tokio::spawn(
        build_wavelet_tree_from_iter(util::calculate_width(input.node_and_value_count() as u64),
                                     sorted_predicate_iter
                                     .map(|(id,_)|id),
                                     predicate_idmap_files.bits_file,
                                     predicate_idmap_files.blocks_file,
                                     predicate_idmap_files.sblocks_file));

    node_value_build_task.await??;
    predicate_build_task.await?
}
