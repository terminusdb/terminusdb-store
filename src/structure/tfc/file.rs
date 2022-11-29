use bytes::BytesMut;

use crate::{storage::*, structure::util::sorted_iterator};

use super::{*, dict::{build_dict_unchecked, build_offset_logarray}};

pub struct StringDictFileBuilder<W:SyncableFile> {
    /// the file that this builder writes the pfc blocks to
    blocks_file: W,
    /// the file that this builder writes the block offsets to
    block_offsets_file: W,

    strings: Vec<SizedDictEntry>,
}

impl<W:SyncableFile> StringDictFileBuilder<W> {
    pub fn new(blocks_file: W, block_offsets_file: W) -> Self {
        Self {
            blocks_file,
            block_offsets_file,
            strings: Vec::new()
        }
    }
}

pub async fn merge_string_dictionaries<
    'a,
    F: 'static + FileLoad + FileStore,
    I: Iterator<Item = &'a StringDict>,
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

    let sorted_iterator = sorted_iterator(iterators, pick_fn);

    let blocks_file_writer = dict_files.blocks_file.open_write().await?;
    let offsets_file_writer = dict_files.offsets_file.open_write().await?;

    let mut offsets = Vec::new();
    let mut offsets_buf = BytesMut::new();
    let mut data_buf = BytesMut::new();
    build_dict_unchecked(0, &mut offsets, &mut data_buf, sorted_iterator);
    build_offset_logarray(&mut offsets_buf, offsets);




    builder.add_all_entries(sorted_iterator).await?;
    builder.finalize().await
}
