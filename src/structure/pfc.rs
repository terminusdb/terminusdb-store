//! Implementation for a Plain Front-Coding (PFC) dictionary.

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};
use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::fmt::Display;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, FramedRead};

use super::logarray::*;
use super::util::*;
use super::vbyte;
use crate::storage::*;

#[derive(Debug)]
pub enum PfcError {
    InvalidCoding,
    NotEnoughData,
}

impl Display for PfcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{:?}", self)
    }
}

impl From<LogArrayError> for PfcError {
    fn from(_err: LogArrayError) -> PfcError {
        PfcError::InvalidCoding
    }
}

impl Error for PfcError {}

impl From<PfcError> for io::Error {
    fn from(err: PfcError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err)
    }
}

#[derive(Clone)]
pub struct PfcBlock {
    encoded_strings: Bytes,
    n_strings: usize,
}

const BLOCK_SIZE: usize = 8;

// the owned version is pretty much equivalent. There should be a way to make this one implementation with generics but I haven't figured out how!
pub struct PfcBlockIterator {
    block: PfcBlock,
    count: usize,
    pos: usize,
    string: Vec<u8>,
}

impl Iterator for PfcBlockIterator {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.pos == 0 {
            // we gotta read the initial prefix first (a nul-terminated string)
            self.string = self.block.head();

            self.count = 1;
            self.pos = self.string.len() + 1;
        } else if self.count < self.block.n_strings {
            // at pos we read a vbyte with the length of the common prefix
            let (common, common_len) =
                vbyte::decode(&self.block.encoded_strings.as_ref()[self.pos..])
                    .expect("encoding error in self-managed data");
            self.string.truncate(common as usize);
            self.pos += common_len;

            // next up is the suffix, again as a nul-terminated string.
            let postfix_end = self.pos
                + self.block.encoded_strings.as_ref()[self.pos..]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap();

            self.string
                .extend_from_slice(&self.block.encoded_strings.as_ref()[self.pos..postfix_end]);

            self.pos = postfix_end + 1;
            self.count += 1;
        } else {
            return None;
        }

        Some(String::from_utf8(self.string.clone()).unwrap())
    }
}

impl PfcBlock {
    pub fn parse(data: Bytes) -> Result<PfcBlock, PfcError> {
        Ok(PfcBlock {
            encoded_strings: data,
            n_strings: BLOCK_SIZE,
        })
    }

    pub fn parse_incomplete(data: Bytes, n_strings: usize) -> Result<PfcBlock, PfcError> {
        Ok(PfcBlock {
            encoded_strings: data,
            n_strings,
        })
    }

    pub fn head(&self) -> Vec<u8> {
        let first_end = self
            .encoded_strings
            .as_ref()
            .iter()
            .position(|&b| b == 0)
            .unwrap();
        let mut v = Vec::new();
        v.extend_from_slice(&self.encoded_strings.as_ref()[..first_end]);

        v
    }

    pub fn strings(&self) -> PfcBlockIterator {
        PfcBlockIterator {
            block: self.clone(),
            count: 0,
            pos: 0,
            string: Vec::new(),
        }
    }

    pub fn get(&self, index: usize) -> Option<String> {
        if index < self.n_strings {
            self.strings().nth(index)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        let len = self.encoded_strings.as_ref().len();
        vbyte::encoding_len(len as u64) + len
    }
}

#[derive(Clone)]
pub struct PfcDict {
    n_strings: u64,
    block_offsets: LogArray,
    blocks: Bytes,
}

pub struct PfcDictIterator {
    dict: PfcDict,
    block_index: usize,
    block: Option<PfcBlockIterator>,
}

impl Iterator for PfcDictIterator {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.block_index >= self.dict.block_offsets.len() + 1 {
            return None;
        } else if self.block.is_none() {
            let block_offset = if self.block_index == 0 {
                0
            } else {
                self.dict.block_offsets.entry(self.block_index - 1)
            } as usize;
            let remainder = self.dict.n_strings as usize - self.block_index * BLOCK_SIZE;
            let mut block = self.dict.blocks.clone();
            block.advance(block_offset);
            if remainder >= BLOCK_SIZE {
                self.block = Some(PfcBlock::parse(block).unwrap().strings());
            } else {
                self.block = Some(
                    PfcBlock::parse_incomplete(block, remainder)
                        .unwrap()
                        .strings(),
                );
            }
        }

        match self.block.as_mut().unwrap().next() {
            None => {
                self.block_index += 1;
                self.block = None;
                self.next()
            }
            Some(s) => Some(s),
        }
    }
}

impl PfcDict {
    pub fn parse(blocks: Bytes, offsets: Bytes) -> Result<PfcDict, PfcError> {
        let n_strings = BigEndian::read_u64(&blocks.as_ref()[blocks.as_ref().len() - 8..]);

        let block_offsets = LogArray::parse(offsets)?;

        Ok(PfcDict {
            n_strings: n_strings,
            block_offsets: block_offsets,
            blocks: blocks,
        })
    }

    pub fn len(&self) -> usize {
        self.n_strings as usize
    }

    pub fn get(&self, ix: usize) -> Option<String> {
        if (ix as u64) < self.n_strings {
            let block_index = ix / BLOCK_SIZE;
            let block_offset = if block_index == 0 {
                0
            } else {
                self.block_offsets.entry(block_index - 1)
            };
            let mut block = self.blocks.clone();
            block.advance(block_offset as usize);
            let block = PfcBlock::parse(block).unwrap();

            let index_in_block = ix % BLOCK_SIZE;
            block.get(index_in_block)
        } else {
            None
        }
    }

    pub fn id(&self, s: &str) -> Option<u64> {
        // let's binary search
        let mut min = 0;
        let mut max = self.block_offsets.len();
        let mut mid: usize;

        while min <= max {
            mid = (min + max) / 2;

            let block_offset = if mid == 0 {
                0
            } else {
                self.block_offsets.entry(mid - 1) as usize
            };
            let block_slice = &self.blocks.as_ref()[block_offset..]; // this is probably more than one block, but we're only interested in the first string anyway
            let head_end = block_slice.iter().position(|&b| b == 0).unwrap();
            let head_slice = &block_slice[..head_end];

            let head = String::from_utf8(head_slice.to_vec()).unwrap();

            match s.cmp(&head) {
                Ordering::Less => {
                    if mid == 0 {
                        // we checked the first block and determined that the string should be in the previous block, if it exists.
                        // but since this is the first block, the string doesn't exist.
                        return None;
                    }
                    max = mid - 1;
                }
                Ordering::Greater => min = mid + 1,
                Ordering::Equal => return Some((mid * BLOCK_SIZE) as u64), // what luck! turns out the string we were looking for was the block head
            }
        }

        let found = max;

        // we found the block the string should be part of.
        let block_start = if found == 0 {
            0
        } else {
            self.block_offsets.entry(found - 1) as usize
        };
        let remainder = self.n_strings as usize - (found * BLOCK_SIZE);
        let mut block = self.blocks.clone();
        block.advance(block_start);
        let block = if remainder >= BLOCK_SIZE {
            PfcBlock::parse(block).unwrap()
        } else {
            PfcBlock::parse_incomplete(block, remainder as usize).unwrap()
        };

        let mut count = 0;
        for block_string in block.strings() {
            if block_string == s {
                return Some((found * BLOCK_SIZE + count) as u64);
            }
            count += 1;
        }

        None
    }

    pub fn strings(&self) -> PfcDictIterator {
        PfcDictIterator {
            dict: self.clone(),
            block_index: 0,
            block: None,
        }
    }
}

pub struct PfcDictFileBuilder<W: tokio::io::AsyncWrite + Unpin + Send> {
    /// the file that this builder writes the pfc blocks to
    pfc_blocks_file: W,
    /// the file that this builder writes the block offsets to
    pfc_block_offsets_file: W,
    /// the amount of strings in this dict so far
    count: usize,
    /// the size in bytes of the pfc data structure so far
    size: usize,
    last: Option<Vec<u8>>,
    index: Vec<u64>,
}

impl<W: 'static + tokio::io::AsyncWrite + Unpin + Send> PfcDictFileBuilder<W> {
    pub fn new(pfc_blocks_file: W, pfc_block_offsets_file: W) -> PfcDictFileBuilder<W> {
        PfcDictFileBuilder {
            pfc_blocks_file,
            pfc_block_offsets_file,
            count: 0,
            size: 0,
            last: None,
            index: Vec::new(),
        }
    }
    pub async fn add(&mut self, s: &str) -> io::Result<u64> {
        let bytes = s.as_bytes().to_vec();
        if self.count % BLOCK_SIZE == 0 {
            if self.count != 0 {
                // this is the start of a block, but not the start of the first block
                // we need to store an index
                self.index.push(self.size as u64);
            }
            let len = write_nul_terminated_bytes(&mut self.pfc_blocks_file, &bytes).await?;
            self.size += len;
        } else {
            let s_bytes = s.as_bytes();
            let common = find_common_prefix(&self.last.as_ref().unwrap(), s_bytes);
            let postfix = s_bytes[common..].to_vec();
            let common_len = vbyte::write_async(&mut self.pfc_blocks_file, common as u64).await?;
            let slice_len = write_nul_terminated_bytes(&mut self.pfc_blocks_file, &postfix).await?;
            self.size += common_len + slice_len;
        }

        self.count += 1;
        self.last = Some(bytes);

        Ok(self.count as u64)
    }

    pub async fn add_all<'a, I: 'static + Iterator<Item = &'a str> + Send>(
        &mut self,
        mut it: I,
    ) -> io::Result<Vec<u64>> {
        let mut result = Vec::new();
        while let Some(next) = it.next() {
            let r = self.add(next).await?;
            result.push(r);
        }

        Ok(result)
    }

    /// finish the data structure
    pub async fn finalize(mut self) -> io::Result<()> {
        let width = if self.index.is_empty() {
            1
        } else {
            64 - self.index[self.index.len() - 1].leading_zeros()
        };
        let mut builder = LogArrayFileBuilder::new(self.pfc_block_offsets_file, width as u8);
        let count = self.count as u64;

        builder.push_all(stream_iter_ok(self.index)).await?;
        builder.finalize().await?;

        write_padding(&mut self.pfc_blocks_file, self.size, 8).await?;
        write_u64(&mut self.pfc_blocks_file, count).await?;
        self.pfc_blocks_file.flush().await?;

        Ok(())
    }
}

struct PfcDecoder {
    last: Option<BytesMut>,
    index: usize,
    done: bool,
}

impl PfcDecoder {
    fn new() -> Self {
        Self {
            last: None,
            index: 0,
            done: false,
        }
    }
}

impl Decoder for PfcDecoder {
    type Item = String;
    type Error = io::Error;
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<String>, io::Error> {
        if self.done {
            bytes.clear();
            return Ok(None);
        }

        // once bytes contains a 0-byte, enough has been read to actually extract a string.
        let pos = bytes.iter().position(|&b| b == 0);
        if pos == Some(0) {
            self.done = true;
            bytes.clear();
            return Ok(None);
        }

        match pos {
            None => Ok(None),
            Some(pos) => match self.index % 8 == 0 {
                true => {
                    // this is the start of a block. we expect a 0-delimited cstring
                    let b = bytes.split_to(pos);
                    bytes.advance(1);
                    let s = String::from_utf8(b.to_vec()).expect("expected utf8 string");
                    self.last = Some(b);
                    self.index += 1;

                    Ok(Some(s))
                }
                false => {
                    // This is in the middle of some block. we expect a vbyte followed by some 0-delimited cstring
                    let last = self.last.as_ref().unwrap();
                    let (prefix_len, vbyte_len) = vbyte::decode(&bytes).expect("expected vbyte");
                    bytes.advance(vbyte_len);
                    let b = bytes.split_to(pos - vbyte_len);
                    bytes.advance(1);
                    let mut full = BytesMut::with_capacity(prefix_len as usize + b.len());
                    full.extend_from_slice(&last[..prefix_len as usize]);
                    full.extend_from_slice(&b);

                    let s = String::from_utf8(full.to_vec()).expect("expected utf8 string");
                    self.last = Some(full);
                    self.index += 1;

                    Ok(Some(s))
                }
            },
        }
    }
}

pub async fn dict_file_get_count<F: 'static + FileLoad>(file: F) -> io::Result<u64> {
    let mut result = vec![0; 8];
    file.open_read_from(file.size() - 8)
        .read_exact(&mut result)
        .await?;
    Ok(BigEndian::read_u64(&result))
}

pub fn dict_reader_to_stream<A: 'static + AsyncRead + Unpin + Send>(
    r: A,
) -> impl Stream<Item = io::Result<String>> + Unpin + Send {
    FramedRead::new(r, PfcDecoder::new())
}

pub fn dict_reader_to_indexed_stream<A: 'static + AsyncRead + Unpin + Send>(
    r: A,
    offset: u64,
) -> impl Stream<Item = io::Result<(u64, String)>> + Send {
    let dict_stream = dict_reader_to_stream(r);

    dict_stream.enumerate().map(move |(i, x)| match x {
        Ok(x) => Ok(((i + 1) as u64 + offset, x)),
        Err(e) => Err(e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use futures::executor::block_on;
    use futures::stream::TryStreamExt;

    #[test]
    fn can_create_pfc_dict_small() {
        let contents = vec!["aaaaa", "aabbb", "ccccc"];
        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());
        block_on(async {
            builder.add_all(contents.into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let p = PfcDict::parse(
            block_on(blocks.map()).unwrap(),
            block_on(offsets.map()).unwrap(),
        )
        .unwrap();

        assert_eq!(Some("aaaaa".to_string()), p.get(0));
        assert_eq!(Some("aabbb".to_string()), p.get(1));
        assert_eq!(Some("ccccc".to_string()), p.get(2));
        assert_eq!(None, p.get(4));

        let mut i = p.strings();

        assert_eq!(Some("aaaaa".to_string()), i.next());
        assert_eq!(Some("aabbb".to_string()), i.next());
        assert_eq!(Some("ccccc".to_string()), i.next());
        assert_eq!(None, i.next());
    }

    #[test]
    fn can_create_pfc_dict_large() {
        let contents = vec![
            "aaaaa",
            "aabbb",
            "ccccc",
            "ddddd asfdl;kfasf opxcvucvkhf asfopihvpvoihfasdfjv;xivh",
            "deasdfvv apobk,naf;libpoiujsafd",
            "deasdfvv apobk,x",
            "ee",
            "eee",
            "eeee",
            "great scott",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let p = PfcDict::parse(
            block_on(blocks.map()).unwrap(),
            block_on(offsets.map()).unwrap(),
        )
        .unwrap();

        assert_eq!(Some("aaaaa".to_string()), p.get(0));
        assert_eq!(Some("aabbb".to_string()), p.get(1));
        assert_eq!(Some("ccccc".to_string()), p.get(2));
        assert_eq!(Some("eeee".to_string()), p.get(8));
        assert_eq!(Some("great scott".to_string()), p.get(9));
        assert_eq!(None, p.get(10));
    }

    #[test]
    fn retrieve_id_from_dict() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
            "arf",
            "bapofsi",
            "barf",
            "berf",
            "boo boo boo boo",
            "bzwas baraf",
            "dradsfadfvbbb",
            "eadfpoicvu",
            "eeeee ee e eee",
            "faadsafdfaf sdfasdf",
            "frumps framps fremps",
            "gahh",
            "hai hai hai",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let dict = PfcDict::parse(
            block_on(blocks.map()).unwrap(),
            block_on(offsets.map()).unwrap(),
        )
        .unwrap();

        assert_eq!(Some(0), dict.id("aaaaa"));
        assert_eq!(Some(5), dict.id("arf"));
        assert_eq!(Some(7), dict.id("barf"));
        assert_eq!(Some(8), dict.id("berf"));
        assert_eq!(Some(15), dict.id("frumps framps fremps"));
        assert_eq!(Some(16), dict.id("gahh"));
        assert_eq!(Some(17), dict.id("hai hai hai"));
        assert_eq!(None, dict.id("arrf"));
        assert_eq!(None, dict.id("a"));
        assert_eq!(None, dict.id("zzz"));
    }

    #[test]
    fn retrieve_all_strings() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
            "arf",
            "bapofsi",
            "barf",
            "berf",
            "boo boo boo boo",
            "bzwas baraf",
            "dradsfadfvbbb",
            "eadfpoicvu",
            "eeeee ee e eee",
            "faadsafdfaf sdfasdf",
            "frumps framps fremps",
            "gahh",
            "hai hai hai",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.clone().into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let dict = PfcDict::parse(
            block_on(blocks.map()).unwrap(),
            block_on(offsets.map()).unwrap(),
        )
        .unwrap();

        let result: Vec<String> = dict.strings().collect();
        assert_eq!(contents, result);
    }

    #[test]
    fn retrieve_all_strings_from_file() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
            "arf",
            "bapofsi",
            "barf",
            "berf",
            "boo boo boo boo",
            "bzwas baraf",
            "dradsfadfvbbb",
            "eadfpoicvu",
            "eeeee ee e eee",
            "faadsafdfaf sdfasdf",
            "frumps framps fremps",
            "gahh",
            "hai hai hai",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.clone().into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let stream = dict_reader_to_stream(blocks.open_read());

        let result: Vec<String> = block_on(stream.try_collect()).unwrap();
        assert_eq!(contents, result);
    }

    #[test]
    fn retrieve_all_strings_from_file_multiple_of_eight() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
            "arf",
            "bapofsi",
            "barf",
            "berf",
            "boo boo boo boo",
            "bzwas baraf",
            "dradsfadfvbbb",
            "eadfpoicvu",
            "eeeee ee e eee",
            "faadsafdfaf sdfasdf",
            "frumps framps fremps",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.clone().into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let stream = dict_reader_to_stream(blocks.open_read());

        let result: Vec<String> = block_on(stream.try_collect()).unwrap();
        assert_eq!(contents, result);
    }

    #[test]
    fn retrieve_all_indexed_strings_from_file() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
            "arf",
            "bapofsi",
            "barf",
            "berf",
            "boo boo boo boo",
            "bzwas baraf",
            "dradsfadfvbbb",
            "eadfpoicvu",
            "eeeee ee e eee",
            "faadsafdfaf sdfasdf",
            "frumps framps fremps",
            "gahh",
            "hai hai hai",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.clone().into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let stream = dict_reader_to_indexed_stream(blocks.open_read(), 0);

        let result: Vec<(u64, String)> = block_on(stream.try_collect()).unwrap();
        assert_eq!((1, "aaaaa".to_string()), result[0]);
        assert_eq!((8, "barf".to_string()), result[7]);
        assert_eq!((9, "berf".to_string()), result[8]);
    }

    #[test]
    fn get_pfc_count_from_file() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
            "arf",
            "bapofsi",
            "barf",
            "berf",
            "boo boo boo boo",
            "bzwas baraf",
            "dradsfadfvbbb",
            "eadfpoicvu",
            "eeeee ee e eee",
            "faadsafdfaf sdfasdf",
            "frumps framps fremps",
            "gahh",
            "hai hai hai",
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        block_on(async {
            builder.add_all(contents.clone().into_iter()).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let count = block_on(dict_file_get_count(blocks)).unwrap();

        assert_eq!(18, count);
    }
}
