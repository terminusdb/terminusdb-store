#![allow(unused_variables)]
#![allow(dead_code)]
use std::io;

use super::internal::*;
use crate::layer::builder::{build_indexes, TripleFileBuilder};
use crate::layer::id_map::{construct_idmaps, construct_idmaps_upto};
use crate::layer::Layer;
use crate::storage::*;
use crate::structure::*;

/*
pub async fn node_value_dictionary_rollup<
    F:'static+FileLoad+FileStore,
    I1:Iterator<Item=&PfcDict>,
    I2:Iterator<Item=&PfcDict>>(
    node_dictionaries: I1,
    value_dictionaries: I2,
    node_dict_files: DictionaryFiles<F>,
    value_dict_files: DictionaryFiles<F>,
    node_value_idmap_files: BitIndexFiles<F>
) -> io::Result<()> {
    merge_dictionaries(node_dictionaries, node_dict_files).await?;
    merge_dictionaries(value_dictionaries, value_dict_files).await?;


    unimplemented!();
}

pub async fn predicate_dictionary_rollup<
    I:Iterator<Item=&PfcDict>,
    F:'static+FileLoad+FileStore>(
    predicate_dictionaries: I,
    predicate_dict_files: DictionaryFiles<F>,
    predicate_idmap_files: BitIndexFiles<F>
) -> io::Result<()> {
    unimplemented!();
}
*/

pub async fn dictionary_rollup<F: 'static + FileLoad + FileStore>(
    layer: &InternalLayer,
    files: &BaseLayerFiles<F>,
) -> io::Result<()> {
    let node_dicts = layer
        .immediate_layers()
        .into_iter()
        .map(|l| l.node_dictionary());
    let predicate_dicts = layer
        .immediate_layers()
        .into_iter()
        .map(|l| l.predicate_dictionary());
    let value_dicts = layer
        .immediate_layers()
        .into_iter()
        .map(|l| l.value_dictionary());

    merge_dictionaries(node_dicts, files.node_dictionary_files.clone()).await?;
    merge_dictionaries(predicate_dicts, files.predicate_dictionary_files.clone()).await?;
    merge_dictionaries(value_dicts, files.value_dictionary_files.clone()).await?;

    construct_idmaps(layer, files.id_map_files.clone()).await
}

pub async fn dictionary_rollup_upto<F: 'static + FileLoad + FileStore>(
    layer: &InternalLayer,
    upto: [u32; 5],
    files: &ChildLayerFiles<F>,
) -> io::Result<()> {
    let node_dicts = layer
        .immediate_layers_upto(upto)
        .into_iter()
        .map(|l| l.node_dictionary());
    let predicate_dicts = layer
        .immediate_layers_upto(upto)
        .into_iter()
        .map(|l| l.predicate_dictionary());
    let value_dicts = layer
        .immediate_layers_upto(upto)
        .into_iter()
        .map(|l| l.value_dictionary());

    merge_dictionaries(node_dicts, files.node_dictionary_files.clone()).await?;
    merge_dictionaries(predicate_dicts, files.predicate_dictionary_files.clone()).await?;
    merge_dictionaries(value_dicts, files.value_dictionary_files.clone()).await?;

    construct_idmaps_upto(layer, upto, files.id_map_files.clone()).await
}

pub async fn delta_rollup<F: 'static + FileLoad + FileStore>(
    layer: &InternalLayer,
    files: BaseLayerFiles<F>,
) -> io::Result<()> {
    dictionary_rollup(layer, &files).await?;

    let counts = layer.all_counts();

    let mut builder = TripleFileBuilder::new(
        files.s_p_adjacency_list_files.clone(),
        files.sp_o_adjacency_list_files.clone(),
        counts.node_count,
        counts.predicate_count,
        counts.value_count,
        None,
    );

    builder.add_id_triples(layer.triples()).await?;
    builder.finalize().await?;

    build_indexes(
        files.s_p_adjacency_list_files.clone(),
        files.sp_o_adjacency_list_files.clone(),
        files.o_ps_adjacency_list_files.clone(),
        None,
        files.predicate_wavelet_tree_files.clone(),
    )
    .await
}

pub async fn delta_rollup_upto<F: 'static + FileLoad + FileStore>(
    layer: &InternalLayer,
    upto: [u32; 5],
    files: ChildLayerFiles<F>,
) -> io::Result<()> {
    dictionary_rollup_upto(layer, upto, &files).await?;

    let counts = layer.all_counts();

    let mut pos_builder = TripleFileBuilder::new(
        files.pos_s_p_adjacency_list_files.clone(),
        files.pos_sp_o_adjacency_list_files.clone(),
        counts.node_count,
        counts.predicate_count,
        counts.value_count,
        Some(files.pos_subjects_file),
    );

    let mut neg_builder = TripleFileBuilder::new(
        files.neg_s_p_adjacency_list_files.clone(),
        files.neg_sp_o_adjacency_list_files.clone(),
        counts.node_count,
        counts.predicate_count,
        counts.value_count,
        Some(files.neg_subjects_file),
    );

    let additions = InternalTripleStackIterator::from_layer_stack(layer, upto)
        .expect("upto not found")
        .filter(|(sort, _)| *sort == TripleChange::Addition)
        .map(|(_, t)| t);

    let removals = InternalTripleStackIterator::from_layer_stack(layer, upto)
        .expect("upto not found")
        .filter(|(sort, _)| *sort == TripleChange::Removal)
        .map(|(_, t)| t);

    pos_builder.add_id_triples(additions).await?;
    pos_builder.finalize().await?;

    neg_builder.add_id_triples(removals).await?;
    neg_builder.finalize().await?;

    build_indexes(
        files.pos_s_p_adjacency_list_files.clone(),
        files.pos_sp_o_adjacency_list_files.clone(),
        files.pos_o_ps_adjacency_list_files.clone(),
        Some(files.pos_objects_file.clone()),
        files.pos_predicate_wavelet_tree_files.clone(),
    )
    .await?;

    build_indexes(
        files.neg_s_p_adjacency_list_files.clone(),
        files.neg_sp_o_adjacency_list_files.clone(),
        files.neg_o_ps_adjacency_list_files.clone(),
        Some(files.neg_objects_file.clone()),
        files.neg_predicate_wavelet_tree_files.clone(),
    )
    .await
}
