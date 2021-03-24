use std::io;
use std::sync::Arc;

use crate::layer::builder::{build_indexes, TripleFileBuilder};
use crate::layer::id_map::{construct_idmaps, construct_idmaps_upto};
use crate::layer::*;
use crate::storage::*;
use crate::structure::*;

async fn safe_upto_bound<S:LayerStore>(
    store: &S,
    layer: Arc<InternalLayer>,
    upto: [u32; 5]
) -> io::Result<[u32;5]> {
    if InternalLayerImpl::name(&*layer) == upto {
        return Ok(upto);
    }
    let mut disk_layer_names = store.retrieve_layer_stack_names_upto(InternalLayerImpl::name(&*layer), upto).await?;

    let mut l = &*layer;
    loop {
        let parent = match l.immediate_parent() {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found",
                ));
            },
            Some(p) => p
        };

        if InternalLayerImpl::name(parent) == upto {
            // reached our destination and all is swell.
            return Ok(upto);
        }
        else {
            // is parent in the disk layers?
            if let Some(ix) = disk_layer_names.iter().position(|&n| n == InternalLayerImpl::name(parent)) {
                // yes, so move further back
                disk_layer_names.truncate(ix);
                l = parent;
            }
            else {
                // no! safe bound was the last thing
                return Ok(InternalLayerImpl::name(l));
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use std::sync::Arc;
    async fn build_three_layers(
    ) -> io::Result<(Arc<InternalLayer>, Arc<InternalLayer>, Arc<InternalLayer>)> {
        let base_files = base_layer_memory_files();
        let mut builder = SimpleLayerBuilder::new([0, 0, 0, 0, 1], base_files.clone());
        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));

        builder.commit().await?;
        let base_layer: Arc<InternalLayer> = Arc::new(
            BaseLayer::load_from_files([0, 0, 0, 0, 1], &base_files)
                .await?
                .into(),
        );

        let child1_files = child_layer_memory_files();
        builder = SimpleLayerBuilder::from_parent(
            [0, 0, 0, 0, 2],
            base_layer.clone(),
            child1_files.clone(),
        );
        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("pig", "likes", "pig"));

        builder.commit().await?;
        let child1_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([0, 0, 0, 0, 2], base_layer.clone(), &child1_files)
                .await?
                .into(),
        );

        let child2_files = child_layer_memory_files();
        builder = SimpleLayerBuilder::from_parent(
            [0, 0, 0, 0, 3],
            child1_layer.clone(),
            child2_files.clone(),
        );
        builder.remove_string_triple(StringTriple::new_node("pig", "likes", "pig"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("sheep", "says", "baah"));
        builder.add_string_triple(StringTriple::new_value("pig", "likes", "sheep"));
        builder.commit().await?;
        let child2_layer: Arc<InternalLayer> = Arc::new(
            ChildLayer::load_from_files([0, 0, 0, 0, 3], child1_layer.clone(), &child2_files)
                .await?
                .into(),
        );

        Ok((base_layer, child1_layer, child2_layer))
    }

    #[tokio::test]
    async fn rollup_three_layers() {
        let (_, _, layer) = build_three_layers().await.unwrap();

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
        let (base_layer, _, child_layer) = build_three_layers().await.unwrap();

        let delta_files = child_layer_memory_files();
        delta_rollup_upto(&child_layer, [0, 0, 0, 0, 1], delta_files.clone())
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
            InternalTripleStackIterator::from_layer_stack(&*child_layer, [0, 0, 0, 0, 1])
                .unwrap()
                .collect();
        let change_actual: Vec<_> =
            InternalTripleStackIterator::from_layer_stack(&*delta_layer, [0, 0, 0, 0, 1])
                .unwrap()
                .collect();

        assert_eq!(change_expected, change_actual);
    }

    #[tokio::test]
    async fn rollup_twice() {
        let (base_layer, _, child_layer) = build_three_layers().await.unwrap();

        let delta1_files = child_layer_memory_files();
        delta_rollup_upto(&child_layer, [0, 0, 0, 0, 1], delta1_files.clone())
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
