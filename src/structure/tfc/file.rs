use bytes::BytesMut;
use std::io;
use tokio::io::AsyncWriteExt;

use crate::{storage::*, structure::util::sorted_iterator};

use super::{
    dict::{build_dict_unchecked, build_offset_logarray},
    *,
};

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

    let mut offsets = Vec::new();
    let mut offsets_buf = BytesMut::new();
    let mut data_buf = BytesMut::new();
    build_dict_unchecked(None, 0, &mut offsets, &mut data_buf, sorted_iterator);
    build_offset_logarray(&mut offsets_buf, offsets);

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

    let pick_fn = |vals: &[Option<&(Datatype, SizedDictEntry)>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| x.cmp(y))
            .map(|(ix, _)| ix)
    };

    let sorted_iterator = sorted_iterator(iterators, pick_fn).map(|(dt, elt)| (dt, elt.to_bytes()));

    let mut types_present_file_writer = dict_files.types_present_file.open_write().await?;
    let mut type_offsets_file_writer = dict_files.type_offsets_file.open_write().await?;
    let mut blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let mut offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut types_present_buf = BytesMut::new();
    let mut type_offsets_buf = BytesMut::new();
    let mut offsets_buf = BytesMut::new();
    let mut data_buf = BytesMut::new();
    build_multiple_segments(
        &mut types_present_buf,
        &mut type_offsets_buf,
        &mut offsets_buf,
        &mut data_buf,
        sorted_iterator,
    );

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
