use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::structure::{
    util::find_common_prefix,
    vbyte::{self, encode_array},
};

const BLOCK_SIZE: usize = 8;

#[derive(Debug)]
pub enum TfcError {
    InvalidCoding,
    NotEnoughData,
}

#[derive(Debug, PartialEq)]
pub struct TfcBlockHeader {
    num_entries: u8,
    buffer_length: usize,
    sizes: [usize; BLOCK_SIZE],
    shareds: [usize; BLOCK_SIZE - 1],
}

impl From<vbyte::DecodeError> for TfcError {
    fn from(e: vbyte::DecodeError) -> Self {
        match e {
            vbyte::DecodeError::UnexpectedEndOfBuffer => Self::NotEnoughData,
            _ => Self::InvalidCoding,
        }
    }
}

impl TfcBlockHeader {
    fn parse<B: Buf>(buf: &mut B) -> Result<Self, TfcError> {
        let num_entries = buf.get_u8();
        let mut sizes = [0_usize; BLOCK_SIZE];
        let mut shareds = [0_usize; BLOCK_SIZE - 1];

        let (first_size, _) = vbyte::decode_buf(buf)?;
        sizes[0] = first_size as usize;

        for i in 0..(num_entries - 1) as usize {
            let (shared, _) = vbyte::decode_buf(buf)?;
            let (size, _) = vbyte::decode_buf(buf)?;

            sizes[i + 1] = size as usize;
            shareds[i] = shared as usize;
        }

        let buffer_length = sizes.iter().sum();

        Ok(Self {
            num_entries,
            buffer_length,
            sizes,
            shareds,
        })
    }
}

#[derive(Debug)]
pub struct TfcEntry<'a>(Vec<&'a [u8]>);

impl<'a> TfcEntry<'a> {
    fn as_vec(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.0.iter().map(|s| s.len()).sum());

        for slice in self.0.iter() {
            v.extend_from_slice(slice);
        }

        v
    }
}

pub struct TfcBlock {
    header: TfcBlockHeader,
    data: Bytes,
}

impl TfcBlock {
    pub fn parse(bytes: &mut Bytes) -> Result<Self, TfcError> {
        let header = TfcBlockHeader::parse(bytes)?;
        if bytes.remaining() < header.buffer_length {
            return Err(TfcError::NotEnoughData);
        }

        let data = bytes.split_to(header.buffer_length);

        Ok(Self { header, data })
    }

    pub fn is_incomplete(&self) -> bool {
        self.header.num_entries != BLOCK_SIZE as u8
    }

    pub fn entry(&self, index: usize) -> TfcEntry {
        if index == 0 {
            return TfcEntry(vec![&self.data[..self.header.sizes[0]]]);
        }

        let mut v = Vec::with_capacity(7);
        let mut last = self.header.shareds[index - 1];
        if last != 0 {
            v.push(last);
        }
        if last != 0 {
            for i in (0..index - 1).rev() {
                let shared = self.header.shareds[i];
                if shared == 0 {
                    break;
                }

                if shared < last {
                    v.push(shared);
                    last = shared;
                } else {
                    v.push(last);
                }
            }
        }

        let start = index - v.len();

        let mut taken = 0;
        let mut slices = Vec::with_capacity(v.len() + 1);

        let mut offset = self.header.sizes.iter().take(start).sum();
        for (ix, shared) in v.iter().rev().enumerate() {
            let have_to_take = shared - taken;
            let cur_offset = offset;
            offset += self.header.sizes[start + ix];
            if have_to_take == 0 {
                continue;
            }
            let slice = &self.data[cur_offset..cur_offset + have_to_take];
            slices.push(slice);
            taken += have_to_take;
        }

        let suffix_size = self.header.sizes[index];
        slices.push(&self.data[offset..offset + suffix_size]);

        TfcEntry(slices)
    }
}

fn build_block_unchecked<B: BufMut>(buf: &mut B, slices: &[&[u8]]) {
    let slices_len = slices.len();
    debug_assert!(slices_len <= BLOCK_SIZE && slices_len != 0);
    buf.put_u8(slices_len as u8);

    let first = slices[0];
    let (vbyte, vbyte_len) = encode_array(first.len() as u64);
    buf.put_slice(&vbyte[..vbyte_len]);

    let mut last = first;

    let mut suffixes: Vec<&[u8]> = Vec::with_capacity(slices.len());
    suffixes.push(last);
    for i in 1..slices.len() {
        let cur = slices[i];
        let common_prefix = find_common_prefix(last, cur);
        let (vbyte, vbyte_len) = encode_array(common_prefix as u64);
        buf.put_slice(&vbyte[..vbyte_len]);

        let suffix_len = cur.len() - common_prefix;
        let (vbyte, vbyte_len) = encode_array(suffix_len as u64);
        buf.put_slice(&vbyte[..vbyte_len]);
        suffixes.push(&cur[common_prefix..]);
        last = cur;
    }

    for suffix in suffixes {
        buf.put_slice(suffix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    #[test]
    fn blah() {
        let slice = b"asdfasfd";
        let mut argh = slice as &[u8];
        let first = argh.get_u8();
        let second = argh.get_u8();

        panic!("{} {} {:?}", first, second, argh);
    }

    fn build_incomplete_block(strings: &[&[u8]]) -> TfcBlock {
        let mut buf = BytesMut::new();
        build_block_unchecked(&mut buf, &strings);

        let mut bytes: Bytes = buf.freeze();

        TfcBlock::parse(&mut bytes).unwrap()
    }

    #[test]
    fn build_and_parse_block() {
        let strings: [&[u8]; 5] = [b"aaaaaa", b"aabb", b"cccc", b"cdef", b"cdff"];

        let block = build_incomplete_block(&strings);

        let expected_header = TfcBlockHeader {
            num_entries: 5,
            buffer_length: 17,
            sizes: [6, 2, 4, 3, 2, 0, 0, 0],
            shareds: [2, 0, 1, 2, 0, 0, 0],
        };

        assert_eq!(expected_header, block.header);

        let expected_bytes = b"aaaaaabbccccdefff";
        assert_eq!(expected_bytes, &block.data[..]);
    }

    #[test]
    fn entry_in_block() {
        let strings: [&[u8]; 5] = [b"aaaaaa", b"aabb", b"cccc", b"cdef", b"cdff"];
        let block = build_incomplete_block(&strings);

        for (ix, string) in strings.iter().enumerate() {
            assert_eq!(*string, &block.entry(ix).as_vec()[..]);
        }
    }

    #[test]
    fn entry_in_complete_block() {
        let strings: [&[u8]; 8] = [
            b"aaaaaa",
            b"aabb",
            b"cccc",
            b"cdef",
            b"cdff",
            b"cdffasdf",
            b"cdffeeee",
            b"ceeeeeeeeeeeeeee",
        ];
        let block = build_incomplete_block(&strings);

        for (ix, string) in strings.iter().enumerate() {
            assert_eq!(*string, &block.entry(ix).as_vec()[..]);
        }
    }
}
