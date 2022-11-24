use itertools::Itertools;
use bytes::{BufMut, Bytes};
use crate::structure::{util::calculate_width, LogArrayBufBuilder, LogArray};

use super::block::*;

fn build_dict_unchecked<'a, B1:BufMut, B2:BufMut,I:Iterator<Item=&'a [u8]>>(array_buf: &mut B1, data_buf: &mut B2, iter: I) {
    let chunk_iter = iter.chunks(BLOCK_SIZE);
    let mut offsets = Vec::new();

    let mut offset = 0;
    for chunk in &chunk_iter {
        let slices: Vec<&[u8]> = chunk.collect();
        let size = build_block_unchecked(data_buf, &slices);
        offset += size;
        offsets.push(offset as u64);
    }

    offsets.pop();

    let largest_element = offsets.last().cloned().unwrap_or(0);
    let width = calculate_width(largest_element);
    let mut array_builder = LogArrayBufBuilder::new(array_buf, width);

    array_builder.push_vec(offsets);
    array_builder.finalize();
}

pub struct TfcDict {
    offsets: LogArray,
    data: Bytes
}

impl TfcDict {
    pub fn from_parts(offsets: Bytes, data: Bytes) -> Self {
        let offsets = LogArray::parse(offsets).unwrap();
        Self {
            offsets, data
        }
    }

    pub fn block_bytes(&self, block_index:usize) -> Bytes {
        let offset: usize;
        if block_index == 0 {
            offset = 0;
        }
        else {
            offset = self.offsets.entry(block_index-1) as usize;
        }

        let block_bytes;
        if block_index == self.offsets.len() {
            block_bytes = self.data.slice(offset..);
        }
        else {
            let end = self.offsets.entry(block_index) as usize;
            block_bytes = self.data.slice(offset..end);
        }

        block_bytes
    }

    pub fn block(&self, block_index: usize) -> TfcBlock {
        let mut block_bytes = self.block_bytes(block_index);
        TfcBlock::parse(&mut block_bytes).unwrap()
    }

    pub fn block_head(&self, block_index: usize) -> Bytes {
        let block_bytes = self.block_bytes(block_index);
        block_head(block_bytes).unwrap()
    }

    pub fn num_blocks(&self) -> usize {
        self.offsets.len() + 1
    }

    pub fn entry(&self, index: u64) -> TfcDictEntry {
        let block = self.block((index / 8) as usize);
        block.entry((index % 8) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

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
        build_dict_unchecked(&mut array_buf, &mut data_buf, strings.clone().into_iter());

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict =TfcDict::from_parts(array_bytes, data_bytes);

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
}
