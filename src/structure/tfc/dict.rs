use std::{borrow::Cow, cmp::Ordering};

use crate::structure::{
    util::calculate_width, LateLogArrayBufBuilder, LogArrayBufBuilder, MonotonicLogArray,
};
use bytes::{BufMut, Bytes};

use super::block::*;

pub struct SizedDictBufBuilder<B1: BufMut, B2: BufMut> {
    pub(crate) record_type: RecordType,
    block_offset: u64,
    id_offset: u64,
    offsets: LateLogArrayBufBuilder<B1>,
    data_buf: B2,
    current_block: Vec<Bytes>,
}

impl<B1: BufMut, B2: BufMut> SizedDictBufBuilder<B1, B2> {
    pub fn new(
        record_type: RecordType,
        block_offset: u64,
        id_offset: u64,
        offsets: LateLogArrayBufBuilder<B1>,
        data_buf: B2,
    ) -> Self {
        Self {
            record_type,
            block_offset,
            id_offset,
            offsets,
            data_buf,
            current_block: Vec::with_capacity(8),
        }
    }

    pub fn id_offset(&self) -> u64 {
        self.id_offset
    }

    pub fn block_offset(&self) -> u64 {
        self.block_offset
    }

    pub fn add(&mut self, value: Bytes) -> u64 {
        self.current_block.push(value);
        self.id_offset += 1;
        if self.current_block.len() == BLOCK_SIZE {
            let current_block: Vec<&[u8]> = self.current_block.iter().map(|e| e.as_ref()).collect();
            let size = build_block_unchecked(&self.record_type, &mut self.data_buf, &current_block);
            self.block_offset += size as u64;
            self.offsets.push(self.block_offset);

            self.current_block.truncate(0);
        }

        self.id_offset
    }

    pub fn add_entry(&mut self, e: &SizedDictEntry) -> u64 {
        self.add(e.to_bytes())
    }

    pub fn add_all<I: Iterator<Item = Bytes>>(&mut self, it: I) -> Vec<u64> {
        it.map(|val| self.add(val)).collect()
    }

    pub fn finalize(mut self) -> (LateLogArrayBufBuilder<B1>, B2, u64, u64) {
        if !self.current_block.is_empty() {
            let current_block: Vec<&[u8]> = self.current_block.iter().map(|e| e.as_ref()).collect();
            let size = build_block_unchecked(&self.record_type, &mut self.data_buf, &current_block);
            self.block_offset += size as u64;
            self.offsets.push(self.block_offset);
        }

        (
            self.offsets,
            self.data_buf,
            self.block_offset,
            self.id_offset,
        )
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

#[derive(Clone, Debug)]
pub struct SizedDict {
    offsets: MonotonicLogArray,
    pub(crate) data: Bytes,
    dict_offset: u64,
}

impl SizedDict {
    pub fn parse(offsets: Bytes, data: Bytes, dict_offset: u64) -> Self {
        let offsets = MonotonicLogArray::parse(offsets).unwrap();
        Self::from_parts(offsets, data, dict_offset)
    }

    pub fn from_parts(offsets: MonotonicLogArray, data: Bytes, dict_offset: u64) -> Self {
        Self {
            offsets,
            data,
            dict_offset,
        }
    }

    fn block_offset(&self, block_index: usize) -> usize {
        let offset: usize;
        if block_index == 0 {
            offset = 0;
        } else {
            offset = (self.offsets.entry(block_index - 1) - self.dict_offset) as usize;
        }

        offset
    }

    pub fn block_bytes(&self, block_index: usize) -> Bytes {
        if self.data.is_empty() {
            panic!("empty dictionary has no block");
        }
        let offset = self.block_offset(block_index);
        self.data.slice(offset..)
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
        if self.data.is_empty() {
            0
        } else {
            let offset = self.block_offset(block_index);
            parse_block_control_records(self.data[offset])
        }
    }

    pub fn num_blocks(&self) -> usize {
        if self.data.is_empty() {
            0
        } else {
            self.offsets.len() + 1
        }
    }

    pub fn entry(&self, index: usize) -> Option<SizedDictEntry> {
        if index > self.num_entries() {
            return None;
        }
        let block = self.block((index - 1) / 8);
        Some(block.entry((index - 1) % 8))
    }

    pub fn id(&self, slice: &[u8]) -> IdLookupResult {
        // let's binary search
        let mut min = 0;
        let mut max = self.offsets.len();
        let mut mid: usize;
        if self.is_empty() {
            return IdLookupResult::NotFound;
        }
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
                Ordering::Equal => return IdLookupResult::Found((mid * BLOCK_SIZE + 1) as u64), // what luck! turns out the string we were looking for was the block head
            }
        }

        let found = max;

        // we found the block the string should be part of.
        let block = self.block(found);
        let block_id = block.id(slice);
        let offset = (found * BLOCK_SIZE) as u64 + 1;
        let result = block_id.offset(offset).default(offset - 1);
        /*
        if found != 0 {
            // the default value will fill in the last index of the
            // previous block if the entry was not found in the
            // current block. This is only possible if the block as
            // not the very first one.
            result.default(self.block_num_elements(found - 1) as u64 + offset - 1)
        } else {
            result
        }
         */

        result
    }

    pub fn block_iter<'a>(&'a self) -> SizedDictBlockIterator<'a> {
        SizedDictBlockIterator {
            dict: Cow::Borrowed(self),
            index: 0,
        }
    }

    pub fn into_block_iter(self) -> OwnedSizedDictBlockIterator {
        SizedDictBlockIterator {
            dict: Cow::Owned(self),
            index: 0,
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = SizedDictEntry> + 'a + Clone {
        self.block_iter().flat_map(|b| b.into_iter())
    }

    pub fn into_iter(self) -> impl Iterator<Item = SizedDictEntry> + Clone {
        self.into_block_iter().flat_map(|b| b.into_iter())
    }

    pub fn num_entries(&self) -> usize {
        let num_blocks = self.num_blocks();
        if num_blocks == 0 {
            0
        } else {
            let last_block_size = self.block_num_elements(num_blocks - 1);

            (num_blocks - 1) * BLOCK_SIZE + last_block_size as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

type OwnedSizedDictBlockIterator = SizedDictBlockIterator<'static>;

#[derive(Clone)]
pub struct SizedDictBlockIterator<'a> {
    dict: Cow<'a, SizedDict>,
    index: usize,
}

impl<'a> Iterator for SizedDictBlockIterator<'a> {
    type Item = SizedDictBlock;

    fn next(&mut self) -> Option<SizedDictBlock> {
        if self.index >= self.dict.num_blocks() {
            return None;
        }

        let block = self.dict.block(self.index);
        self.index += 1;

        Some(block)
    }
}

/*
pub struct SizedDictIterator<'a> {
    dict: SizedDictBlockIterator<'a>,
    block: Option<SizedBlockIterator<'a>>,
}

impl<'a> Iterator for SizedDictIterator<'a> {
    type Item = SizedDictEntry;

    fn next(&mut self) -> Option<SizedDictEntry> {
        if let Some(entry) = self.block.as_ref().and_then(|b|b.next()) {
            Some(entry)
        }
        else {
            let next_block = self.dict.next();
            if next_block.is_none() {
                return None;
            }
            let next_block = next_block.unwrap();
            let next_block_iter = next_block.iter();

            let result = next_block_iter.next();

            self.block = Some(next_block_iter);

            result
        }
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn build_dict_and_offsets<B1: BufMut, B2: BufMut, I: Iterator<Item = Bytes>>(
        array_buf: B1,
        data_buf: B2,
        vals: I,
    ) -> (B1, B2) {
        let offsets = LateLogArrayBufBuilder::new(array_buf);
        let mut builder = SizedDictBufBuilder::new(RecordType::Arbitrary, 0, 0, offsets, data_buf);
        builder.add_all(vals);
        let (mut array, data_buf, _, _) = builder.finalize();
        array.pop();
        let array_buf = array.finalize();

        (array_buf, data_buf)
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
        build_dict_and_offsets(
            &mut array_buf,
            &mut data_buf,
            strings.clone().into_iter().map(|s| Bytes::from(s)),
        );

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::parse(array_bytes, data_bytes, 0);

        assert_eq!(2, dict.num_blocks());
        assert_eq!(b"aaaaaaaa", &dict.block_head(0)[..]);
        assert_eq!(b"hello", &dict.block_head(1)[..]);

        let block0 = dict.block(0);
        let block1 = dict.block(1);

        assert_eq!(8, block0.num_entries());
        assert_eq!(6, block1.num_entries());

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(s, &dict.entry(ix + 1).unwrap().to_bytes()[..]);
        }
    }

    #[test]
    fn build_dict_of_two_blocks_with_builder() {
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
        let data_buf = BytesMut::new();

        let logarray_builder = LateLogArrayBufBuilder::new(&mut array_buf);

        let mut builder =
            SizedDictBufBuilder::new(RecordType::Arbitrary, 0, 0, logarray_builder, data_buf);
        builder.add_all(strings.clone().into_iter().map(|v| Bytes::from_static(v)));
        let (mut logarray_builder, data_buf, _, _) = builder.finalize();
        logarray_builder.pop();
        logarray_builder.finalize();

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::parse(array_bytes, data_bytes, 0);

        assert_eq!(2, dict.num_blocks());
        assert_eq!(b"aaaaaaaa", &dict.block_head(0)[..]);
        assert_eq!(b"hello", &dict.block_head(1)[..]);

        let block0 = dict.block(0);
        let block1 = dict.block(1);

        assert_eq!(8, block0.num_entries());
        assert_eq!(6, block1.num_entries());

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(s, &dict.entry(ix + 1).unwrap().to_bytes()[..]);
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
        build_dict_and_offsets(
            &mut array_buf,
            &mut data_buf,
            strings.clone().into_iter().map(Bytes::from),
        );

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::parse(array_bytes, data_bytes, 0);

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(IdLookupResult::Found((ix + 1) as u64), dict.id(s));
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
        build_dict_and_offsets(
            &mut array_buf,
            &mut data_buf,
            strings.clone().into_iter().map(Bytes::from),
        );

        let array_bytes = array_buf.freeze();
        let data_bytes = data_buf.freeze();
        let dict = SizedDict::parse(array_bytes, data_bytes, 0);

        assert_eq!(IdLookupResult::NotFound, dict.id(b"a"));
        assert_eq!(IdLookupResult::Closest(1), dict.id(b"ab"));
        assert_eq!(IdLookupResult::Closest(8), dict.id(b"hallo"));
        assert_eq!(IdLookupResult::Closest(9), dict.id(b"hello!"));
        assert_eq!(IdLookupResult::Closest(14), dict.id(b"zebra"));
    }
}
