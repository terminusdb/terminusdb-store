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

        let mapped_id = self.id_wtree.lookup_one(ix as u64).unwrap();
        self.inner.get(mapped_id as usize)
    }

    pub fn id(&self, s: &str) -> Option<u64> {
        self.inner.id(s)
            .map(|mapped_id| self.id_wtree.decode_one(mapped_id as usize))
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
                                     .min_by_key(|&(a, item)| &item.unwrap().1)
                                     .map(|x| x.0)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use crate::structure::bitindex::*;

    #[test]
    fn bla() {
        let contents1 = vec![
            "aaaaa",
            "abcdefghijk",
            "arf",
            "bapofsi",
            "berf",
            "bzwas baraf",
            "eadfpoicvu",
            "faadsafdfaf sdfasdf",
            "gahh",
            ];

        let contents2 = vec![
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "addeeerafa",
            "barf",
            "boo boo boo boo",
            "dradsfadfvbbb",
            "eeeee ee e eee",
            "frumps framps fremps",
            "hai hai hai"
            ];

        let blocks1 = MemoryBackedStore::new();
        let offsets1 = MemoryBackedStore::new();
        let builder1 = PfcDictFileBuilder::new(blocks1.open_write(), offsets1.open_write());

        builder1.add_all(contents1.clone().into_iter().map(|s|s.to_string()))
            .and_then(|(_,b)|b.finalize())
            .wait().unwrap();

        let blocks2 = MemoryBackedStore::new();
        let offsets2 = MemoryBackedStore::new();
        let builder2 = PfcDictFileBuilder::new(blocks2.open_write(), offsets2.open_write());

        builder2.add_all(contents2.clone().into_iter().map(|s|s.to_string()))
            .and_then(|(_,b)|b.finalize())
            .wait().unwrap();

        let blocks3 = MemoryBackedStore::new();
        let offsets3 = MemoryBackedStore::new();
        let wavelet_bits3 = MemoryBackedStore::new();
        let wavelet_blocks3 = MemoryBackedStore::new();
        let wavelet_sblocks3 = MemoryBackedStore::new();

        merge_dictionary_stack(vec![blocks1, blocks2], blocks3.clone(), offsets3.clone(), wavelet_bits3.clone(), wavelet_blocks3.clone(), wavelet_sblocks3.clone()).wait().unwrap();

        let dict = PfcDict::parse(blocks3.map().wait().unwrap(), offsets3.map().wait().unwrap()).unwrap();
        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits3.map().wait().unwrap(), wavelet_blocks3.map().wait().unwrap(), wavelet_sblocks3.map().wait().unwrap());
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 5);

        let mapped_dict = MappedPfcDict::from_parts(dict, wavelet_tree);

        let stream = dict_reader_to_stream(blocks3.open_read());
        let result: Vec<String> = stream.collect().wait().unwrap();

        let mut total_contents = Vec::with_capacity(contents1.len()+contents2.len());
        total_contents.extend(contents1);
        total_contents.extend(contents2);

        for i in 0..18 {
            let s = mapped_dict.get(i);
            assert_eq!(total_contents[i], s);
            let id = mapped_dict.id(&s).unwrap();
            assert_eq!(i as u64, id);
        }
    }
}
