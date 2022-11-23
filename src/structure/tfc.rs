use bytes::{Bytes, Buf, BytesMut, BufMut};

use crate::structure::{vbyte::{self,encode_array}, util::find_common_prefix};

const BLOCK_SIZE: usize = 8;

pub struct TfcBlock {
    data: Bytes
}

#[derive(Debug, PartialEq)]
pub struct TfcBlockHeader {
    size: u8,
    sizes: [u64;BLOCK_SIZE],
    shareds: [u64;BLOCK_SIZE-1]
}

#[derive(Debug)]
pub enum TfcError {
    InvalidCoding,
    NotEnoughData,
}

impl From<vbyte::DecodeError> for TfcError {
    fn from(e: vbyte::DecodeError) -> Self {
        match e {
            vbyte::DecodeError::UnexpectedEndOfBuffer => Self::NotEnoughData,
            _ => Self::InvalidCoding
        }
    }
}

impl TfcBlockHeader {
    fn parse<B:Buf>(buf: &mut B) -> Result<Self, TfcError> {
        let size = buf.get_u8();
        let mut sizes = [0;BLOCK_SIZE];
        let mut shareds = [0;BLOCK_SIZE-1];

        
        let (first_size, _) = vbyte::decode_buf(buf)?;
        sizes[0] = first_size;

        for i in 0..(size-1) as usize {
            let (shared, _) = vbyte::decode_buf(buf)?;
            let (size, _) = vbyte::decode_buf(buf)?;

            sizes[i+1] = size;
            shareds[i] = shared;
        }

        Ok(Self {
            size,
            sizes,
            shareds
        })
    }
}

pub struct TfcBlockBuilder {
}

fn build_block_unchecked<B:BufMut>(buf: &mut B, slices: &[&[u8]]) {
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

    #[test]
    fn build_and_parse_block() {
        let strings: [&[u8];5] = [
            b"aaaaaa",
            b"aabb",
            b"cccc",
            b"cdef",
            b"cdff"
        ];

        let mut buf = BytesMut::new();
        build_block_unchecked(&mut buf, &strings);
        let mut bytes: Bytes = buf.freeze();

        let header = TfcBlockHeader::parse(&mut bytes).unwrap();

        let expected = TfcBlockHeader {
            size: 5,
            sizes: [6, 2, 4, 3, 2, 0, 0, 0],
            shareds: [2, 0, 1, 2, 0, 0, 0]
        };

        assert_eq!(expected, header);

        assert_eq!(b"aaaaaabbccccdefff", &bytes[..]);
    }
}
