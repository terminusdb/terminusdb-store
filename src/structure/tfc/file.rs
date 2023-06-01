use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt};
use std::fmt::Debug;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::structure::util::compare_or_result;
use crate::{
    storage::*,
    structure::util::{sorted_iterator, sorted_stream},
};

use super::*;

pub async fn dedup_merge_string_dictionaries_stream<
    'a,
    F: 'static + FileLoad + FileStore,
    E: Debug + Into<io::Error>,
    S: Stream<Item = Result<SizedDictEntry, E>> + Send + Unpin,
    N: From<usize> + Into<usize> + Copy + Debug,
>(
    dictionaries: Vec<S>,
    dict_files: DictionaryFiles<F>,
) -> io::Result<(Vec<Vec<N>>, usize)> {
    let dictionaries_len = dictionaries.len();
    let annotated_dictionary_streams: Vec<_> = dictionaries
        .into_iter()
        .enumerate()
        .map(|(ix, s)| s.map_ok(move |e| (e, ix)))
        .collect();
    let pick_fn = |vals: &[Option<&Result<(SizedDictEntry, usize), E>>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| compare_or_result(x.as_ref().unwrap(), y.as_ref().unwrap()))
            .map(|(ix, _)| ix)
    };

    let mut sorted_stream = sorted_stream(annotated_dictionary_streams, pick_fn)
        .map_ok(|(elt, ix)| (elt.to_bytes(), ix));

    let mut blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let mut offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut result: Vec<(N, bool)> = Vec::new();
    let mut builder = StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());

    let mut tally = 0;
    let mut last_item: Option<Bytes> = None;
    while let Some((item, dict_index)) = sorted_stream.try_next().await.map_err(|e| e.into())? {
        if Some(&item) != last_item.as_ref() {
            result.push((dict_index.into(), true));
            builder.add(item.clone());
            last_item = Some(item);
            tally += 1;
        } else {
            result.push((dict_index.into(), false));
        }
    }
    let (offsets_buf, data_buf) = builder.finalize();

    offsets_file_writer.write_all(offsets_buf.as_ref()).await?;
    offsets_file_writer.flush().await?;
    offsets_file_writer.sync_all().await?;

    blocks_file_writer.write_all(data_buf.as_ref()).await?;
    blocks_file_writer.flush().await?;
    blocks_file_writer.sync_all().await?;

    Ok((as_maps(dictionaries_len, &result), tally))
}

pub fn as_maps<T: Into<usize> + Copy + Debug, N: From<usize>>(
    total_size: usize,
    picks: &[(T, bool)],
) -> Vec<Vec<N>> {
    let mut maps: Vec<Vec<N>> = Vec::with_capacity(total_size);
    for _ in 0..total_size {
        maps.push(Vec::new());
    }
    let mut count = 0;
    for (p, picked) in picks {
        let dict: usize = (*p).into();
        let map: &mut Vec<N> = &mut maps[dict];
        if *picked {
            count += 1;
        }
        map.push((count - 1).into());
    }

    maps
}

pub async fn merge_string_dictionaries<
    'a,
    F: 'static + FileLoad + FileStore,
    I: Iterator<Item = &'a StringDict> + 'a,
>(
    dictionaries: I,
    dict_files: DictionaryFiles<F>,
) -> io::Result<()> {
    let iterators: Vec<_> = dictionaries.map(|d| d.iter()).collect();

    let pick_fn = |vals: &[Option<&SizedDictEntry>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| x.cmp(y))
            .map(|(ix, _)| ix)
    };

    let sorted_iterator = sorted_iterator(iterators, pick_fn).map(|elt| elt.to_bytes());

    let mut blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let mut offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut builder = StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());
    builder.add_all(sorted_iterator);
    let (offsets_buf, data_buf) = builder.finalize();

    offsets_file_writer.write_all(offsets_buf.as_ref()).await?;
    offsets_file_writer.flush().await?;
    offsets_file_writer.sync_all().await?;

    blocks_file_writer.write_all(data_buf.as_ref()).await?;
    blocks_file_writer.flush().await?;
    blocks_file_writer.sync_all().await?;

    Ok(())
}

pub async fn merge_typed_dictionaries<
    'a,
    F: 'static + FileLoad + FileStore,
    I: Iterator<Item = &'a TypedDict> + 'a,
>(
    dictionaries: I,
    dict_files: TypedDictionaryFiles<F>,
) -> io::Result<()> {
    let iterators: Vec<_> = dictionaries.map(|d| d.iter()).collect();

    let pick_fn = |vals: &[Option<&TypedDictEntry>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| x.cmp(y))
            .map(|(ix, _)| ix)
    };

    let sorted_iterator = sorted_iterator(iterators, pick_fn);

    let mut types_present_file_writer = dict_files.types_present_file.open_write().await?;
    let mut type_offsets_file_writer = dict_files.type_offsets_file.open_write().await?;
    let mut blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let mut offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut builder = TypedDictBufBuilder::new(
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
    );
    builder.add_all(sorted_iterator);
    let (types_present_buf, type_offsets_buf, offsets_buf, data_buf) = builder.finalize();

    types_present_file_writer
        .write_all(types_present_buf.as_ref())
        .await?;
    types_present_file_writer.flush().await?;
    types_present_file_writer.sync_all().await?;

    type_offsets_file_writer
        .write_all(type_offsets_buf.as_ref())
        .await?;
    type_offsets_file_writer.flush().await?;
    type_offsets_file_writer.sync_all().await?;

    offsets_file_writer.write_all(offsets_buf.as_ref()).await?;
    offsets_file_writer.flush().await?;
    offsets_file_writer.sync_all().await?;

    blocks_file_writer.write_all(data_buf.as_ref()).await?;
    blocks_file_writer.flush().await?;
    blocks_file_writer.sync_all().await?;

    Ok(())
}

pub async fn dedup_merge_typed_dictionary_streams<
    'a,
    F: 'static + FileLoad + FileStore,
    E: Debug + Into<io::Error>,
    S: Stream<Item = Result<TypedDictEntry, E>> + Send + Unpin,
    N: From<usize> + Into<usize> + Copy + Debug,
>(
    dictionaries: Vec<S>,
    dict_files: TypedDictionaryFiles<F>,
) -> io::Result<(Vec<Vec<N>>, usize)> {
    let dictionaries_len = dictionaries.len();
    let annotated_dictionary_streams: Vec<_> = dictionaries
        .into_iter()
        .enumerate()
        .map(|(ix, s)| s.map_ok(move |e| (e, ix)))
        .collect();
    let pick_fn = |vals: &[Option<&Result<(TypedDictEntry, usize), E>>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| compare_or_result(x.as_ref().unwrap(), y.as_ref().unwrap()))
            .map(|(ix, _)| ix)
    };

    let mut sorted_stream = sorted_stream(annotated_dictionary_streams, pick_fn);

    let mut types_present_file_writer = dict_files.types_present_file.open_write().await?;
    let mut type_offsets_file_writer = dict_files.type_offsets_file.open_write().await?;
    let mut blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let mut offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut result: Vec<(N, bool)> = Vec::new();
    let mut builder = TypedDictBufBuilder::new(
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
        BytesMut::new(),
    );

    let mut tally = 0;
    let mut last_item: Option<TypedDictEntry> = None;
    while let Some((item, dict_index)) = sorted_stream.try_next().await.map_err(|e| e.into())? {
        if Some(&item) != last_item.as_ref() {
            result.push((dict_index.into(), true));
            builder.add(item.clone());
            last_item = Some(item);
            tally += 1;
        } else {
            result.push((dict_index.into(), false));
        }
    }
    let (types_present_buf, type_offsets_buf, offsets_buf, data_buf) = builder.finalize();

    types_present_file_writer
        .write_all(types_present_buf.as_ref())
        .await?;
    types_present_file_writer.flush().await?;
    types_present_file_writer.sync_all().await?;

    type_offsets_file_writer
        .write_all(type_offsets_buf.as_ref())
        .await?;
    type_offsets_file_writer.flush().await?;
    type_offsets_file_writer.sync_all().await?;

    offsets_file_writer.write_all(offsets_buf.as_ref()).await?;
    offsets_file_writer.flush().await?;
    offsets_file_writer.sync_all().await?;

    blocks_file_writer.write_all(data_buf.as_ref()).await?;
    blocks_file_writer.flush().await?;
    blocks_file_writer.sync_all().await?;

    Ok((as_maps(dictionaries_len, &result), tally))
}

pub async fn dict_file_get_count<F: 'static + FileLoad>(file: F) -> io::Result<u64> {
    let mut result = vec![0; 8];
    file.open_read_from(file.size().await? - 8)
        .await?
        .read_exact(&mut result)
        .await?;
    Ok(BigEndian::read_u64(&result))
}
