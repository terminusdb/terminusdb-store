use std::cmp::Ordering;

use crate::structure::{util::calculate_width, LogArray, LogArrayBufBuilder};
use bytes::{BufMut, Bytes};
use itertools::Itertools;

use super::block::*;

pub fn build_dict_unchecked<B: BufMut, R: AsRef<[u8]>, I: Iterator<Item = R>>(
    offsets: &mut Vec<u64>,
    data_buf: &mut B,
    iter: I,
) {
    let chunk_iter = iter.chunks(BLOCK_SIZE);

    let mut offset = 0;
    for chunk in &chunk_iter {
        let slices: Vec<R> = chunk.collect();
        let borrows: Vec<&[u8]> = slices.iter().map(|s| s.as_ref()).collect();
        let size = build_block_unchecked(data_buf, &borrows);
        offset += size;
        offsets.push(offset as u64);
    }
}
pub fn build_offset_logarray<B: BufMut>(buf: &mut B, mut offsets: Vec<u64>) {
    // the last offset doesn't matter as it's implied by the total size
    offsets.pop();

    let largest_element = offsets.last().cloned().unwrap_or(0);
    let width = calculate_width(largest_element);
    let mut array_builder = LogArrayBufBuilder::new(buf, width);

    array_builder.push_vec(offsets);
    array_builder.finalize();
}

pub struct SizedDict {
    offsets: LogArray,
    data: Bytes,
}

impl SizedDict {
    pub fn from_parts(offsets: Bytes, data: Bytes) -> Self {
        let offsets = LogArray::parse(offsets).unwrap();
        Self { offsets, data }
    }

    fn block_offset(&self, block_index: usize) -> usize {
        let offset: usize;
        if block_index == 0 {
            offset = 0;
        } else {
            offset = self.offsets.entry(block_index - 1) as usize;
        }

        offset
    }

    pub fn block_bytes(&self, block_index: usize) -> Bytes {
        let offset = self.block_offset(block_index);
        let block_bytes;
        if block_index == self.offsets.len() {
            block_bytes = self.data.slice(offset..);
        } else {
            let end = self.offsets.entry(block_index) as usize;
            block_bytes = self.data.slice(offset..end);
        }

        block_bytes
    }

    pub fn block(&self, block_index: usize) -> SizedDictBlock {
        let mut block_bytes = self.block_bytes(block_index);
        SizedDictBlock::parse(&mut block_bytes).unwrap()
    }

    pub fn block_head(&self, block_index: usize) -> Bytes {
        let block_bytes = self.block_bytes(block_index);
        block_head(block_bytes).unwrap()
    }

    pub fn block_num_elements(&self, block_index: usize) -> u8 {
        let offset = self.block_offset(block_index);

        self.data[offset]
    }

    pub fn num_blocks(&self) -> usize {
        self.offsets.len() + 1
    }

    pub fn entry(&self, index: u64) -> SizedDictEntry {
        let block = self.block((index / 8) as usize);
        block.entry((index % 8) as usize)
    }

    pub fn id(&self, slice: &[u8]) -> IdLookupResult {
        // let's binary search
        let mut min = 0;
        let mut max = self.offsets.len();
        let mut mid: usize;

        while min <= max {
            mid = (min + max) / 2;

            let head_slice = self.block_head(mid);

            match slice.cmp(&head_slice[..]) {
                Ordering::Less => {
                    if mid == 0 {
                        // we checked the first block and determined that the string should be in the previous block, if it exists.
                        // but since this is the first block, the string doesn't exist.
                        return IdLookupResult::NotFound;
                    }
                    max = mid - 1;
                }
                Ordering::Greater => min = mid + 1,
                Ordering::Equal => return IdLookupResult::Found((mid * BLOCK_SIZE) as u64), // what luck! turns out the string we were looking for was the block head
            }
        }

        let found = max;

        // we found the block the string should be part of.
        let block = self.block(found);
        let block_id = block.id(slice);
        let offset = (found * BLOCK_SIZE) as u64;
        let result = block_id.offset(offset);
        if found != 0 {
            // the default value will fill in the last index of the
            // previous block if the entry was not found in the
            // current block. This is only possible if the block as
            // not the very first one.
            result.default(self.block_num_elements(found - 1) as u64 + offset - 1)
        } else {
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn build_dict_and_offsets<B1: BufMut, B2: BufMut, R: AsRef<[u8]>, I: Iterator<Item=R>>(array_buf: &mut B1, data_buf: &mut B2, vals: I) {
        let mut offsets = Vec::new();
        build_dict_unchecked(&mut offsets, data_buf, vals);
        build_offset_logarray(array_buf, offsets);
    }

    #[test]
    fn build_dict_of_two_blocks() {
        let strings: Vec<&[u8]> = vec![
            b"aaaaaaaa",
            b"bbbbbbbb",
            b"bbbcccdaaaa",
            b"f",
            b"fafasdfas",
            b"gafovp",
            b"gdfasfa",
            b"gdfbbbbbb",
            b"hello",
            b"iguana",
            b"illusion",
            b"illustrated",
            b"jetengine",
            b"jetplane",
        ];

        let mut array_buf = BytesMut::new();
        let mut data_buf = BytesMut::new();
        build_dict_and_offsets(&mut array_buf, &mut data_buf, strings.clone().into_iter());

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::from_parts(array_bytes, data_bytes);

        assert_eq!(2, dict.num_blocks());
        assert_eq!(b"aaaaaaaa", &dict.block_head(0)[..]);
        assert_eq!(b"hello", &dict.block_head(1)[..]);

        let block0 = dict.block(0);
        let block1 = dict.block(1);

        assert_eq!(8, block0.num_entries());
        assert_eq!(6, block1.num_entries());

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(s, &dict.entry(ix as u64).to_bytes()[..]);
        }
    }

    #[test]
    fn lookup_entries_by_slice() {
        let strings: Vec<&[u8]> = vec![
            b"aaaaaaaa",
            b"bbbbbbbb",
            b"bbbcccdaaaa",
            b"f",
            b"fafasdfas",
            b"gafovp",
            b"gdfasfa",
            b"gdfbbbbbb",
            b"hello",
            b"iguana",
            b"illusion",
            b"illustrated",
            b"jetengine",
            b"jetplane",
        ];

        let mut array_buf = BytesMut::new();
        let mut data_buf = BytesMut::new();
        build_dict_and_offsets(&mut array_buf, &mut data_buf, strings.clone().into_iter());

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::from_parts(array_bytes, data_bytes);

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(IdLookupResult::Found(ix as u64), dict.id(s));
        }
    }

    #[test]
    fn lookup_nonmatching_entries_by_slice() {
        let strings: Vec<&[u8]> = vec![
            b"aaaaaaaa",
            b"bbbbbbbb",
            b"bbbcccdaaaa",
            b"f",
            b"fafasdfas",
            b"gafovp",
            b"gdfasfa",
            b"gdfbbbbbb",
            b"hello",
            b"iguana",
            b"illusion",
            b"illustrated",
            b"jetengine",
            b"jetplane",
        ];

        let mut array_buf = BytesMut::new();
        let mut data_buf = BytesMut::new();
        build_dict_and_offsets(&mut array_buf, &mut data_buf, strings.clone().into_iter());

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::from_parts(array_bytes, data_bytes);

        assert_eq!(IdLookupResult::NotFound, dict.id(b"a"));
        assert_eq!(IdLookupResult::Closest(0), dict.id(b"ab"));
        assert_eq!(IdLookupResult::Closest(7), dict.id(b"hallo"));
        assert_eq!(IdLookupResult::Closest(8), dict.id(b"hello!"));
        assert_eq!(IdLookupResult::Closest(13), dict.id(b"zebra"));
    }
}
