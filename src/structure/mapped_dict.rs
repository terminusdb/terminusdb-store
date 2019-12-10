#![allow(unused)]

use super::pfc::*;
use super::wavelettree::*;
use std::io;
use futures::prelude::*;
use futures::future;
use crate::storage::*;
use super::util::*;

pub struct MappedPfcDict<M:AsRef<[u8]>+Clone> {
    inner: PfcDict<M>,
    id_wtree: WaveletTree<M>
}

impl<M:AsRef<[u8]>+Clone> MappedPfcDict<M> {
    pub fn from_parts(dict: PfcDict<M>, wtree: WaveletTree<M>) -> MappedPfcDict<M> {
        MappedPfcDict {
            inner: dict,
            id_wtree: wtree
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn get(&self, ix: usize) -> String {
        if ix >= self.len() {
            panic!("index too large for mapped pfc dict");
        }

        let mapped_id = self.id_wtree.lookup_one(ix as u64 - 1).unwrap();
        self.inner.get(mapped_id as usize + 1)
    }

    pub fn id(&self, s: &str) -> Option<u64> {
        self.inner.id(s)
            .map(|mapped_id| self.id_wtree.decode_one(mapped_id as usize - 1) + 1)
    }
}

pub fn merge_dictionary_stack<F:'static+FileLoad+FileStore>(stack: Vec<F>, blocks_file: F, block_offset_file: F, wavelet_bits_file: F, wavelet_blocks_file: F, wavelet_sblocks_file: F) -> impl Future<Item=(), Error=io::Error>+Send {
    let dict_builder = PfcDictFileBuilder::new(blocks_file.open_write(), block_offset_file.open_write());

    future::join_all(stack.clone().into_iter().map(|f|dict_file_get_count(f)))
        .map(|counts: Vec<u64>| {
            counts.into_iter().scan(0, |mut tally, c| {
                let prev = *tally;
                *tally += c;

                Some(prev)
            })
                .zip(stack.into_iter())
                .map(|(offset, file)| dict_reader_to_indexed_stream(file.open_read(), offset))
                .collect::<Vec<_>>()
        })
        .map(|streams| sorted_stream(streams, |results| results.iter()
                                     .enumerate()
                                     .filter(|&(_, item)| item.is_some())
                                     .min_by_key(|&(_, item)| item)
                                     .map(|x|x.0)))
        .and_then(|stream| stream.fold((dict_builder, Vec::new()), |(builder, mut indexes), (ix, s)| {
            indexes.push(ix - 1); // dictionary entries start at 1, but for slight space efficiency we start at 0 for the wavelet tree
            builder.add(&s)
                .map(|(_, b)| (b, indexes))
        }))
        .and_then(|(builder, indexes)| {
            let f1 = builder.finalize();
            let max = indexes.iter().max().map(|x|*x).unwrap_or(0) + 1;
            let width = (max as f32).log2().ceil() as u8;
            let stream_constructor = move || futures::stream::iter_ok(indexes.clone());
            let f2 = build_wavelet_tree_from_stream(width, stream_constructor, wavelet_bits_file, wavelet_blocks_file, wavelet_sblocks_file);
            f1.join(f2).map(|_|())
        })
}

