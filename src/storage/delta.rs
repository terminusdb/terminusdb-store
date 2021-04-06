use std::io;

use crate::layer::builder::{build_indexes, TripleFileBuilder};
use crate::layer::id_map::{construct_idmaps, construct_idmaps_upto};
use crate::layer::*;
use crate::storage::*;
use crate::structure::*;

async fn safe_upto_bound<S: LayerStore>(
    store: &S,
    layer: &InternalLayer,
    upto: [u32; 5],
) -> io::Result<[u32; 5]> {
    if InternalLayerImpl::name(&*layer) == upto {
        return Ok(upto);
    }
    let mut disk_layer_names = store
        .retrieve_layer_stack_names_upto(InternalLayerImpl::name(&*layer), upto)
        .await?;

    let mut l = &*layer;
    loop {
        println!("hi {:?}", Layer::name(l));
        let parent = match l.immediate_parent() {
            None => {
                // previous was the last found layer and therefore the bound
                return Ok(InternalLayerImpl::name(&*l));
            }
            Some(p) => p,
        };

        if InternalLayerImpl::name(parent) == upto {
            // reached our destination and all is swell.
            return Ok(upto);
        } else {
            // is parent in the disk layers?
            if let Some(ix) = disk_layer_names
                .iter()
                .position(|&n| n == InternalLayerImpl::name(parent))
            {
                // yes, so move further back
                disk_layer_names.truncate(ix);
                l = parent;
            } else {
                // no! safe bound was the last thing
                return Ok(InternalLayerImpl::name(l));
            }
        }
    }
}

async fn get_node_dicts_from_disk<S: LayerStore>(
    store: &S,
    name: [u32; 5],
    upto: [u32; 5],
) -> io::Result<Vec<PfcDict>> {
    let mut result = Vec::new();
    walk_backwards_from_disk_upto!(store, name, upto, current, {
        let dict = store
            .get_node_dictionary(current)
            .await?
            .expect("expected dictionary to exist");
        result.push(dict);
    });

    result.reverse();

    Ok(result)
}

async fn get_predicate_dicts_from_disk<S: LayerStore>(
    store: &S,
    name: [u32; 5],
    upto: [u32; 5],
) -> io::Result<Vec<PfcDict>> {
    let mut result = Vec::new();
    walk_backwards_from_disk_upto!(store, name, upto, current, {
        let dict = store
            .get_predicate_dictionary(current)
            .await?
            .expect("expected dictionary to exist");
        result.push(dict);
    });

    result.reverse();

    Ok(result)
}

async fn get_value_dicts_from_disk<S: LayerStore>(
    store: &S,
    name: [u32; 5],
    upto: [u32; 5],
) -> io::Result<Vec<PfcDict>> {
    let mut result = Vec::new();
    walk_backwards_from_disk_upto!(store, name, upto, current, {
        let dict = store
            .get_value_dictionary(current)
            .await?
            .expect("expected dictionary to exist");
        result.push(dict);
    });

    result.reverse();

    Ok(result)
}

async fn get_node_value_idmaps_from_disk<S: LayerStore>(
    store: &S,
    name: [u32; 5],
    upto: [u32; 5],
) -> io::Result<Vec<IdMap>> {
    let mut result = Vec::new();
    walk_backwards_from_disk_upto!(store, name, upto, current, {
        let dict = store
            .get_node_value_idmap(current)
            .await?
            .expect("expected idmap to be retrievable");
        result.push(dict);
    });

    result.reverse();

    Ok(result)
}

async fn get_predicate_idmaps_from_disk<S: LayerStore>(
    store: &S,
    name: [u32; 5],
    upto: [u32; 5],
) -> io::Result<Vec<IdMap>> {
    let mut result = Vec::new();
    walk_backwards_from_disk_upto!(store, name, upto, current, {
        let dict = store
            .get_predicate_idmap(current)
            .await?
            .expect("expected idmap to be retrievable");
        result.push(dict);
    });

    result.reverse();

    Ok(result)
}

async fn dictionary_rollup_upto<S: LayerStore, F: 'static + FileLoad + FileStore>(
    store: &S,
    layer: &InternalLayer,
    memory_upto: [u32; 5],
    upto: [u32; 5],
    files: &ChildLayerFiles<F>,
) -> io::Result<()> {
    let disk_node_dicts = get_node_dicts_from_disk(store, memory_upto, upto).await?;
    let disk_predicate_dicts = get_predicate_dicts_from_disk(store, memory_upto, upto).await?;
    let disk_value_dicts = get_value_dicts_from_disk(store, memory_upto, upto).await?;
    let disk_node_value_idmaps = get_node_value_idmaps_from_disk(store, memory_upto, upto).await?;
    let disk_predicate_idmaps = get_predicate_idmaps_from_disk(store, memory_upto, upto).await?;
    let node_dicts: Vec<_> = disk_node_dicts
        .into_iter()
        .chain(
            layer
                .immediate_layers_upto(memory_upto)
                .into_iter()
                .map(|l| l.node_dictionary().clone()),
        )
        .collect();
    let predicate_dicts: Vec<_> = disk_predicate_dicts
        .into_iter()
        .chain(
            layer
                .immediate_layers_upto(memory_upto)
                .into_iter()
                .map(|l| l.predicate_dictionary().clone()),
        )
        .collect();
    let value_dicts: Vec<_> = disk_value_dicts
        .into_iter()
        .chain(
            layer
                .immediate_layers_upto(memory_upto)
                .into_iter()
                .map(|l| l.value_dictionary().clone()),
        )
        .collect();

    let node_value_idmaps: Vec<_> = disk_node_value_idmaps
        .into_iter()
        .chain(
            layer
                .immediate_layers_upto(memory_upto)
                .into_iter()
                .map(|l| l.node_value_id_map().clone()),
        )
        .collect();

    let predicate_idmaps: Vec<_> = disk_predicate_idmaps
        .into_iter()
        .chain(
            layer
                .immediate_layers_upto(memory_upto)
                .into_iter()
                .map(|l| l.node_value_id_map().clone()),
        )
        .collect();

    merge_dictionaries(node_dicts.iter(), files.node_dictionary_files.clone()).await?;
    merge_dictionaries(
        predicate_dicts.iter(),
        files.predicate_dictionary_files.clone(),
    )
    .await?;
    merge_dictionaries(value_dicts.iter(), files.value_dictionary_files.clone()).await?;

    construct_idmaps_from_structures(
        &node_dicts,
        &predicate_dicts,
        &value_dicts,
        &node_value_idmaps,
        &predicate_idmaps,
        files.id_map_files.clone(),
    )
    .await
}

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

async fn memory_dictionary_rollup_upto<F: 'static + FileLoad + FileStore>(
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

pub async fn imprecise_delta_rollup_upto<S: LayerStore, F: 'static + FileLoad + FileStore>(
    store: &S,
    layer: &InternalLayer,
    upto: [u32; 5],
    files: ChildLayerFiles<F>,
) -> io::Result<()> {
    let bound = safe_upto_bound(store, layer, upto).await?;
    memory_dictionary_rollup_upto(layer, bound, &files).await?;

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

    let additions = InternalTripleStackIterator::from_layer_stack(layer, bound)
        .expect("bound not found")
        .filter(|(sort, _)| *sort == TripleChange::Addition)
        .map(|(_, t)| t);

    let removals = InternalTripleStackIterator::from_layer_stack(layer, bound)
        .expect("bound not found")
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

pub async fn delta_rollup_upto<S: LayerStore, F: 'static + FileLoad + FileStore>(
    store: &S,
    layer: &InternalLayer,
    upto: [u32; 5],
    files: ChildLayerFiles<F>,
) -> io::Result<()> {
    let bound = safe_upto_bound(store, layer, upto).await?;
    dictionary_rollup_upto(store, layer, bound, upto, &files).await?;

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

    let disk_changes = store.layer_changes_upto(bound, upto).await?;
    let memory_changes =
        InternalTripleStackIterator::from_layer_stack(layer, bound).expect("upto not found");
    let changes = InternalTripleStackIterator::merge(vec![disk_changes, memory_changes]);
    let additions = changes
        .clone()
        .filter(|(sort, _)| *sort == TripleChange::Addition)
        .map(|(_, t)| t);
    let removals = changes
        .clone()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use std::sync::Arc;
    async fn build_three_layers<S: LayerStore>(
        store: &S,
    ) -> io::Result<(Arc<InternalLayer>, Arc<InternalLayer>, Arc<InternalLayer>)> {
        let mut builder = store.create_base_layer().await?;
        let base_name = builder.name();
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));

        builder.commit_boxed().await?;
        let base_layer = store.get_layer(base_name).await?.unwrap();

        builder = store.create_child_layer(base_name).await?;
        let child1_name = builder.name();
        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("pig", "likes", "pig"));

        builder.commit_boxed().await?;
        let child1_layer = store.get_layer(child1_name).await?.unwrap();

        builder = store.create_child_layer(child1_name).await?;
        let child2_name = builder.name();
        builder.remove_string_triple(StringTriple::new_node("pig", "likes", "pig"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("sheep", "says", "baah"));
        builder.add_string_triple(StringTriple::new_value("pig", "likes", "sheep"));
        builder.commit_boxed().await?;

        let child2_layer = store.get_layer(child2_name).await?.unwrap();

        Ok((base_layer, child1_layer, child2_layer))
    }

    #[tokio::test]
    async fn rollup_three_layers() {
        let store = MemoryLayerStore::new();
        let (_, _, layer) = build_three_layers(&store).await.unwrap();

        let delta_files = base_layer_memory_files();
        delta_rollup(&layer, delta_files.clone()).await.unwrap();

        let delta_layer: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([0, 0, 0, 0, 4], &delta_files)
                .await
                .unwrap()
                .into(),
        );

        let expected: Vec<_> = layer
            .triples()
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            expected,
            delta_layer
                .triples()
                .map(|t| delta_layer.id_triple_to_string(&t).unwrap())
                .collect::<Vec<_>>()
        );

        for t in expected {
            assert!(delta_layer.string_triple_exists(&t));
        }
    }

    #[tokio::test]
    async fn rollup_two_of_three_layers() {
        let store = MemoryLayerStore::new();
        let (base_layer, _, child_layer) = build_three_layers(&store).await.unwrap();
        let base_name = Layer::name(&*base_layer);

        let delta_files = child_layer_memory_files();
        delta_rollup_upto(&store, &child_layer, base_name, delta_files.clone())
            .await
            .unwrap();

        let delta_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([0, 0, 0, 0, 4], base_layer, &delta_files)
                .await
                .unwrap()
                .into(),
        );

        let expected: Vec<_> = child_layer
            .triples()
            .map(|t| child_layer.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            expected,
            delta_layer
                .triples()
                .map(|t| delta_layer.id_triple_to_string(&t).unwrap())
                .collect::<Vec<_>>()
        );

        for t in expected {
            assert!(delta_layer.string_triple_exists(&t));
        }

        let change_expected: Vec<_> =
            InternalTripleStackIterator::from_layer_stack(&*child_layer, base_name)
                .unwrap()
                .collect();
        let change_actual: Vec<_> =
            InternalTripleStackIterator::from_layer_stack(&*delta_layer, base_name)
                .unwrap()
                .collect();

        assert_eq!(change_expected, change_actual);
    }

    #[tokio::test]
    async fn rollup_twice() {
        let store = MemoryLayerStore::new();
        let (base_layer, _, child_layer) = build_three_layers(&store).await.unwrap();

        let delta1_files = child_layer_memory_files();
        delta_rollup_upto(
            &store,
            &child_layer,
            Layer::name(&*base_layer),
            delta1_files.clone(),
        )
        .await
        .unwrap();

        let delta_layer1: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([0, 0, 0, 0, 4], base_layer, &delta1_files)
                .await
                .unwrap()
                .into(),
        );

        let delta2_files = base_layer_memory_files();
        delta_rollup(&delta_layer1, delta2_files.clone())
            .await
            .unwrap();

        let delta_layer2: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([0, 0, 0, 0, 5], &delta2_files)
                .await
                .unwrap()
                .into(),
        );

        let expected: Vec<_> = child_layer
            .triples()
            .map(|t| child_layer.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            expected,
            delta_layer2
                .triples()
                .map(|t| delta_layer2.id_triple_to_string(&t).unwrap())
                .collect::<Vec<_>>()
        );

        for t in expected {
            assert!(delta_layer2.string_triple_exists(&t));
        }
    }
}
