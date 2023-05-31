use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use futures::{Stream, StreamExt, TryStreamExt};
use std::fmt::Debug;
use std::{cmp::Ordering, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    storage::*,
    structure::util::{sorted_iterator, sorted_stream},
};

use super::*;

fn compare_or_result<T: Ord, E: Debug>(
    r1: &Result<(usize, T), E>,
    r2: &Result<(usize, T), E>,
) -> Ordering {
    if r1.is_err() {
        if r2.is_err() {
            Ordering::Equal
        } else {
            Ordering::Less
        }
    } else if r2.is_err() {
        Ordering::Greater
    } else {
        r1.as_ref().unwrap().1.cmp(&r2.as_ref().unwrap().1)
    }
}

pub async fn merge_string_dictionaries_stream<
    'a,
    F: 'static + FileLoad + FileStore,
    E: Debug + Into<io::Error>,
    S: Stream<Item = Result<(usize, SizedDictEntry), E>> + Send + Unpin,
    N: From<usize>,
>(
    dictionaries: Vec<S>,
    dict_files: DictionaryFiles<F>,
) -> io::Result<Vec<(N, N)>> {
    let pick_fn = |vals: &[Option<&Result<(usize, SizedDictEntry), E>>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| compare_or_result(x.as_ref().unwrap(), y.as_ref().unwrap()))
            .map(|(ix, _)| ix)
    };

    let sorted_stream =
        sorted_stream(dictionaries, pick_fn).map_ok(|(ix, elt)| (ix, elt.to_bytes()));

    let mut blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let mut offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut result = Vec::new();
    let mut builder = StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());
    let mut enumerated_stream = sorted_stream.enumerate().map(|(ix, r)| match r {
        Ok(x) => Ok((ix, x)),
        Err(e) => Err(e),
    });

    while let Some((dict_index, (elt_index, item))) =
        enumerated_stream.try_next().await.map_err(|e| e.into())?
    {
        result.push((dict_index.into(), (elt_index + 1).into()));
        builder.add(item);
    }
    let (offsets_buf, data_buf) = builder.finalize();

    offsets_file_writer.write_all(offsets_buf.as_ref()).await?;
    offsets_file_writer.flush().await?;
    offsets_file_writer.sync_all().await?;

    blocks_file_writer.write_all(data_buf.as_ref()).await?;
    blocks_file_writer.flush().await?;
    blocks_file_writer.sync_all().await?;

    Ok(result)
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

pub async fn dict_file_get_count<F: 'static + FileLoad>(file: F) -> io::Result<u64> {
    let mut result = vec![0; 8];
    file.open_read_from(file.size().await? - 8)
        .await?
        .read_exact(&mut result)
        .await?;
    Ok(BigEndian::read_u64(&result))
}
