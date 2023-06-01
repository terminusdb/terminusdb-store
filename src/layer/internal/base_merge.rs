use std::io;

use futures::{Stream, StreamExt, TryStreamExt};

use crate::{
    layer::{open_base_triple_stream, BaseLayerFileBuilderPhase2},
    storage::{BaseLayerFiles, DictionaryFiles, FileLoad, FileStore, TypedDictionaryFiles},
    structure::{
        dedup_merge_string_dictionaries_stream, dedup_merge_typed_dictionary_streams,
        stream::{TfcDictStream, TfcTypedDictStream},
        util::{compare_or_result, sorted_stream},
    },
};

fn try_enumerate<T, E, S: Stream<Item = Result<T, E>> + Send>(
    s: S,
) -> impl Stream<Item = Result<(usize, T), E>> + Send {
    s.enumerate().map(|(ix, r)| r.map(|v| (ix, v)))
}

async fn dicts_to_map<
    'a,
    F: FileLoad + FileStore + 'static,
    I: ExactSizeIterator<Item = &'a DictionaryFiles<F>>,
>(
    inputs: I,
    output: DictionaryFiles<F>,
) -> io::Result<(Vec<Vec<usize>>, usize)> {
    let mut streams = Vec::with_capacity(inputs.len());
    for input in inputs {
        let reader = input.blocks_file.open_read().await?;
        let stream = try_enumerate(
            TfcDictStream::new(reader)
                .map_ok(|e| e.0)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
        );

        streams.push(stream);
    }
    let map_result = dedup_merge_string_dictionaries_stream(streams, output).await?;

    Ok(map_result)
}

async fn typed_dicts_to_map<
    'a,
    F: FileLoad + FileStore + 'static,
    I: ExactSizeIterator<Item = &'a TypedDictionaryFiles<F>>,
>(
    inputs: I,
    output: TypedDictionaryFiles<F>,
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
        let stream = try_enumerate(raw_stream).map_err(|e| io::Error::new(io::ErrorKind::Other, e));

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
) -> (u64, u64, u64) {
    let s = node_map[triple.0 as usize - 1] as u64 + 1;
    let p = predicate_map[triple.1 as usize - 1] as u64 + 1;

    let o = if (triple.2 as usize - 1) < node_map.len() {
        node_map[triple.2 as usize - 1] as u64 + 1
    } else {
        value_map[triple.2 as usize - 1 - node_map.len()] as u64 + 1
    };

    (s, p, o)
}

pub async fn merge_base_layers<F: FileLoad + FileStore + 'static>(
    inputs: &[BaseLayerFiles<F>],
    output: BaseLayerFiles<F>,
) -> io::Result<()> {
    let (node_map, node_count) = dicts_to_map(
        inputs.iter().map(|i| &i.node_dictionary_files),
        output.node_dictionary_files.clone(),
    )
    .await?;
    let (predicate_map, predicate_count) = dicts_to_map(
        inputs.iter().map(|i| &i.predicate_dictionary_files),
        output.predicate_dictionary_files.clone(),
    )
    .await?;
    let (value_map, value_count) = typed_dicts_to_map(
        inputs.iter().map(|i| &i.value_dictionary_files),
        output.value_dictionary_files.clone(),
    )
    .await?;

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
            map_triple(triple, inner_node_map, inner_predicate_map, inner_value_map)
        });
        triple_streams.push(stream);
    }

    let pick_fn = |vals: &[Option<&io::Result<(u64, u64, u64)>>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| compare_or_result(x.as_ref().unwrap(), y.as_ref().unwrap()))
            .map(|(ix, _)| ix)
    };
    let mut merged_triples = sorted_stream(triple_streams, pick_fn);

    let mut builder =
        BaseLayerFileBuilderPhase2::new(output, node_count, predicate_count, value_count).await?;

    let mut last_triple = None;
    while let Some(triple) = merged_triples.try_next().await? {
        if Some(triple) == last_triple {
            continue;
        }

        last_triple = Some(triple);
        builder.add_triple(triple.0, triple.1, triple.2).await?;
    }

    builder.finalize().await
}
