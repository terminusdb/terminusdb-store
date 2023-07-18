use std::{
    io,
    path::{Path, PathBuf},
};

use futures::{Stream, StreamExt, TryStreamExt};
use tempfile::TempDir;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

use crate::{
    chrono_log,
    layer::{builder::build_indexes, open_base_triple_stream, BaseLayerFileBuilderPhase2},
    storage::{
        directory::DirectoryLayerStore, AdjacencyListFiles, BaseLayerFiles, DictionaryFiles,
        FileLoad, FileStore, PersistentLayerStore, TypedDictionaryFiles,
    },
    structure::{
        dedup_merge_string_dictionaries_stream, dedup_merge_typed_dictionary_streams,
        stream::{TfcDictStream, TfcTypedDictStream},
        util::heap_sorted_stream,
    },
};

#[allow(unused)]
fn try_enumerate<T, E, S: Stream<Item = Result<T, E>> + Send>(
    s: S,
) -> impl Stream<Item = Result<(usize, T), E>> + Send {
    s.enumerate().map(|(ix, r)| r.map(|v| (ix, v)))
}

async fn dicts_to_map<
    F1: FileLoad + FileStore + 'static,
    F2: FileLoad + FileStore + 'static,
    I: ExactSizeIterator<Item = DictionaryFiles<F1>>,
>(
    inputs: I,
    output: DictionaryFiles<F2>,
) -> io::Result<(Vec<Vec<usize>>, usize)> {
    let mut streams = Vec::with_capacity(inputs.len());
    for input in inputs {
        let reader = input.blocks_file.open_read().await?;
        let stream = TfcDictStream::new(reader)
            .map_ok(|e| e.0)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        streams.push(stream);
    }
    let map_result = dedup_merge_string_dictionaries_stream(streams, output).await?;

    Ok(map_result)
}

async fn typed_dicts_to_map<
    F1: FileLoad + FileStore + 'static,
    F2: FileLoad + FileStore + 'static,
    I: ExactSizeIterator<Item = TypedDictionaryFiles<F1>>,
>(
    inputs: I,
    output: TypedDictionaryFiles<F2>,
) -> io::Result<(Vec<Vec<usize>>, usize)> {
    let mut streams = Vec::with_capacity(inputs.len());
    for input in inputs {
        let reader = input.blocks_file.open_read().await?;
        let raw_stream = TfcTypedDictStream::new(
            reader,
            input.types_present_file.map().await?,
            input.type_offsets_file.map().await?,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let stream = raw_stream.map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        streams.push(stream);
    }
    let map_result = dedup_merge_typed_dictionary_streams(streams, output).await?;

    Ok(map_result)
}

fn map_triple(
    triple: (u64, u64, u64),
    node_map: &[usize],
    predicate_map: &[usize],
    value_map: &[usize],
    num_nodes: usize,
) -> (u64, u64, u64) {
    let s = node_map[triple.0 as usize - 1] as u64 + 1;
    let p = predicate_map[triple.1 as usize - 1] as u64 + 1;

    let o = if (triple.2 as usize - 1) < node_map.len() {
        node_map[triple.2 as usize - 1] as u64 + 1
    } else {
        value_map[triple.2 as usize - 1 - node_map.len()] as u64 + num_nodes as u64 + 1
    };

    (s, p, o)
}

async fn test_write_file<F: FileLoad + FileStore + 'static, P: Into<PathBuf>>(
    file: &F,
    to: P,
) -> io::Result<()> {
    let mut input_file = file.open_read().await?;

    let to = to.into();
    let mut options = OpenOptions::new();
    options.create(true);
    options.write(true);
    let mut output_file = options.open(&to).await?;

    tokio::io::copy(&mut input_file, &mut output_file).await?;
    output_file.flush().await?;
    chrono_log!("wrote {to:?}");
    Ok(())
}

#[allow(unused)]
async fn test_write_dict<F: FileLoad + FileStore + 'static>(
    prefix: &str,
    files: &DictionaryFiles<F>,
) -> io::Result<()> {
    chrono_log!("writing {prefix} files");
    let blocks_path = format!("{prefix}.blocks");
    let offsets_path = format!("{prefix}.offsets");

    test_write_file(&files.blocks_file, &blocks_path).await?;
    test_write_file(&files.offsets_file, &offsets_path).await?;

    Ok(())
}

#[allow(unused)]
async fn test_write_typed_dict<F: FileLoad + FileStore + 'static>(
    prefix: &str,
    files: &TypedDictionaryFiles<F>,
) -> io::Result<()> {
    chrono_log!("writing {prefix} files");
    let types_present_path = format!("{prefix}.types_present");
    let type_offsets_path = format!("{prefix}.type_offsets");
    let blocks_path = format!("{prefix}.blocks");
    let offsets_path = format!("{prefix}.offsets");

    test_write_file(&files.types_present_file, &types_present_path).await?;
    test_write_file(&files.type_offsets_file, &type_offsets_path).await?;
    test_write_file(&files.blocks_file, &blocks_path).await?;
    test_write_file(&files.offsets_file, &offsets_path).await?;

    Ok(())
}

#[allow(unused)]
async fn test_write_adjacency_list_files<F: FileLoad + FileStore + 'static>(
    prefix: &str,
    files: &AdjacencyListFiles<F>,
) -> io::Result<()> {
    let nums_path = format!("{prefix}.nums");
    let bits_path = format!("{prefix}.bits");
    let bit_index_blocks_path = format!("{prefix}.bit_index_blocks");
    let bit_index_sblocks_path = format!("{prefix}.bit_index_sblocks");

    test_write_file(&files.nums_file, &nums_path).await?;
    test_write_file(&files.bitindex_files.bits_file, &bits_path).await?;
    test_write_file(&files.bitindex_files.blocks_file, &bit_index_blocks_path).await?;
    test_write_file(&files.bitindex_files.sblocks_file, &bit_index_sblocks_path).await?;

    Ok(())
}

pub async fn merge_base_layers<F: FileLoad + FileStore + 'static, P: AsRef<Path>>(
    inputs: &[BaseLayerFiles<F>],
    output: BaseLayerFiles<F>,
    temp_path: P,
) -> io::Result<()> {
    chrono_log!("started merge of base layers");

    // we are going to assume that this is a big expensive merge. all
    // files will be constructed on disk first and only after being
    // fully built will we actually copy things over to the output.
    let temp_dir = TempDir::new_in(&temp_path)?;
    let temp_store = DirectoryLayerStore::new(temp_dir.path());
    let temp_layer_id = temp_store.create_directory().await?;
    let temp_output_files = temp_store.base_layer_files(temp_layer_id).await?;

    let node_dicts: Vec<_> = inputs
        .iter()
        .map(|i| i.node_dictionary_files.clone())
        .collect();
    let predicate_dicts: Vec<_> = inputs
        .iter()
        .map(|i| i.predicate_dictionary_files.clone())
        .collect();
    let value_dicts: Vec<_> = inputs
        .iter()
        .map(|i| i.value_dictionary_files.clone())
        .collect();
    let node_map_task = tokio::spawn(dicts_to_map(
        node_dicts.into_iter(),
        temp_output_files.node_dictionary_files.clone(),
    ));
    let predicate_map_task = tokio::spawn(dicts_to_map(
        predicate_dicts.into_iter(),
        temp_output_files.predicate_dictionary_files.clone(),
    ));
    let value_map_task = tokio::spawn(typed_dicts_to_map(
        value_dicts.into_iter(),
        temp_output_files.value_dictionary_files.clone(),
    ));

    let (node_map, node_count) = node_map_task.await??;
    chrono_log!(" merged node dicts");
    let (predicate_map, predicate_count) = predicate_map_task.await??;
    chrono_log!(" merged predicate dicts");
    let (value_map, value_count) = value_map_task.await??;
    chrono_log!("merged value dicts");
    //test_write_dict("/tmp/node_dict", &temp_output_files.node_dictionary_files).await?;
    //test_write_dict(
    //    "/tmp/predicate_dict",
    //    &temp_output_files.predicate_dictionary_files,
    //)
    //.await?;
    //test_write_typed_dict("/tmp/value_dict", &temp_output_files.value_dictionary_files).await?;

    let mut triple_streams = Vec::with_capacity(inputs.len());
    for (ix, input) in inputs.into_iter().enumerate() {
        let raw_stream = open_base_triple_stream(
            input.s_p_adjacency_list_files.clone(),
            input.sp_o_adjacency_list_files.clone(),
        )
        .await?;
        let inner_node_map = &node_map[ix];
        let inner_predicate_map = &predicate_map[ix];
        let inner_value_map = &value_map[ix];
        let stream = raw_stream.map_ok(move |triple| {
            map_triple(
                triple,
                inner_node_map,
                inner_predicate_map,
                inner_value_map,
                node_count,
            )
        });
        triple_streams.push(stream);
    }

    let mut merged_triples = heap_sorted_stream(triple_streams).await?;

    let mut builder = BaseLayerFileBuilderPhase2::new(
        temp_output_files.clone(),
        node_count,
        predicate_count,
        value_count,
    )
    .await?;
    chrono_log!("constructed phase 2 builder");

    let mut last_triple = None;
    let mut tally: u64 = 0;
    while let Some(triple) = merged_triples.try_next().await? {
        if Some(triple) == last_triple {
            continue;
        }

        last_triple = Some(triple);
        builder.add_triple(triple.0, triple.1, triple.2).await?;
        tally += 1;
        if tally % 1000000 == 0 {
            chrono_log!("wrote {tally} triples");
        }
    }
    chrono_log!("added all merged triples");

    let files = builder.partial_finalize().await?;
    chrono_log!("finalized triple map");
    //test_write_adjacency_list_files("/tmp/triples_s_p", &files.s_p_adjacency_list_files).await?;
    //test_write_adjacency_list_files("/tmp/triples_sp_o", &files.sp_o_adjacency_list_files).await?;

    let s_p_adjacency_list_files = files.s_p_adjacency_list_files.clone();
    let sp_o_adjacency_list_files = files.sp_o_adjacency_list_files.clone();
    let o_ps_adjacency_list_files = files.o_ps_adjacency_list_files.clone();
    let predicate_wavelet_tree_files = files.predicate_wavelet_tree_files.clone();
    build_indexes(
        s_p_adjacency_list_files,
        sp_o_adjacency_list_files,
        o_ps_adjacency_list_files,
        None,
        predicate_wavelet_tree_files,
    )
    .await?;
    chrono_log!(" built indexes");

    // now that everything has been constructed on disk, copy over to the actual layer store
    output.copy_from(&temp_output_files).await?;

    Ok(())
}
