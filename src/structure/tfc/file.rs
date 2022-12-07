use byteorder::{BigEndian, ByteOrder};
use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use crate::{storage::*, structure::util::sorted_iterator};

use super::*;

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

    let mut builder = TypedDictBufBuilder::new(BytesMut::new(), BytesMut::new(), BytesMut::new(), BytesMut::new());
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
