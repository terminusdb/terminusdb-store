use byteorder::{ByteOrder,BigEndian};
use futures::prelude::*;
use futures::future;
use std::error::Error;
use std::fmt::Display;
use std::cmp::{Ord, Ordering};

use super::vbyte::*;
use super::logarray::*;
use super::util::*;

#[derive(Debug)]
pub enum PfcError {
    InvalidCoding,
    NotEnoughData
}

impl Display for PfcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{:?}", self)
    }
}

impl From<VByteError> for PfcError {
    fn from(_err: VByteError) -> PfcError {
        PfcError::InvalidCoding
    }
}

impl From<LogArrayError> for PfcError {
    fn from(_err: LogArrayError) -> PfcError {
        PfcError::InvalidCoding
    }
}

impl Error for PfcError {
}

impl Into<std::io::Error> for PfcError {
    fn into(self) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, self)
    }
}

#[derive(Clone)]
pub struct PfcBlock<M:AsRef<[u8]>+Clone> {
    encoded_strings: M,
    n_strings: usize
}

const BLOCK_SIZE: usize = 8;

pub struct PfcBlockIterator<'a,M:AsRef<[u8]>+Clone> {
    block: &'a PfcBlock<M>,
    count: usize,
    pos: usize,
    string: Vec<u8>
}

impl<'a, M:AsRef<[u8]>+Clone> Iterator for PfcBlockIterator<'a,M> {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.pos == 0 {
            // we gotta read the initial prefix first (a nul-terminated string)
            self.string = self.block.head();

            self.count = 1;
            self.pos = self.string.len() + 1;
        }
        else if self.count < self.block.n_strings {
            // at pos we read a vbyte with the length of the common prefix
            let v = VByte::parse(&self.block.encoded_strings.as_ref()[self.pos..]).expect("encoding error in self-managed data");
            self.string.truncate(v.unpack() as usize);
            self.pos += v.len();

            // next up is the suffix, again as a nul-terminated string.
            let postfix_end = self.pos + self.block.encoded_strings.as_ref()[self.pos..].iter().position(|&b|b==0).unwrap();

            self.string.extend_from_slice(&self.block.encoded_strings.as_ref()[self.pos..postfix_end]);

            self.pos = postfix_end + 1;
            self.count += 1;
        }
        else {
            return None;
        }

        Some(String::from_utf8(self.string.clone()).unwrap())
    }
}

// the owned version is pretty much equivalent. There should be a way to make this one implementation with generics but I haven't figured out how!
pub struct OwnedPfcBlockIterator<M:AsRef<[u8]>+Clone> {
    block: PfcBlock<M>,
    count: usize,
    pos: usize,
    string: Vec<u8>
}

impl<M:AsRef<[u8]>+Clone> Iterator for OwnedPfcBlockIterator<M> {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.pos == 0 {
            // we gotta read the initial prefix first (a nul-terminated string)
            self.string = self.block.head();

            self.count = 1;
            self.pos = self.string.len() + 1;
        }
        else if self.count < self.block.n_strings {
            // at pos we read a vbyte with the length of the common prefix
            let v = VByte::parse(&self.block.encoded_strings.as_ref()[self.pos..]).expect("encoding error in self-managed data");
            self.string.truncate(v.unpack() as usize);
            self.pos += v.len();

            // next up is the suffix, again as a nul-terminated string.
            let postfix_end = self.pos + self.block.encoded_strings.as_ref()[self.pos..].iter().position(|&b|b==0).unwrap();

            self.string.extend_from_slice(&self.block.encoded_strings.as_ref()[self.pos..postfix_end]);

            self.pos = postfix_end + 1;
            self.count += 1;
        }
        else {
            return None;
        }

        Some(String::from_utf8(self.string.clone()).unwrap())
    }
}


impl<M:AsRef<[u8]>+Clone> PfcBlock<M> {
    pub fn parse(data: M) -> Result<PfcBlock<M>,PfcError> {
        Ok(PfcBlock { encoded_strings: data, n_strings: BLOCK_SIZE })
    }

    pub fn parse_incomplete(data: M, n_strings: usize) -> Result<PfcBlock<M>,PfcError> {
        Ok(PfcBlock { encoded_strings: data, n_strings })
    }

    pub fn head(&self) -> Vec<u8> {
        let first_end = self.encoded_strings.as_ref().iter().position(|&b|b == 0).unwrap();
        let mut v = Vec::new();
        v.extend_from_slice(&self.encoded_strings.as_ref()[..first_end]);

        v
    }

    pub fn strings(&self) -> PfcBlockIterator<M> {
        PfcBlockIterator {
            block: &self,
            count: 0,
            pos: 0,
            string: Vec::new()
        }
    }

    pub fn into_strings(self) -> OwnedPfcBlockIterator<M> {
        OwnedPfcBlockIterator {
            block: self,
            count: 0,
            pos: 0,
            string: Vec::new()
        }
    }

    pub fn get(&self, index: usize) -> String {
        if index >= self.n_strings {
            panic!("index too high");
        }
        self.strings().nth(index).unwrap()
    }

    pub fn len(&self) -> usize {
        let vbyte_len = VByte::required_len(self.encoded_strings.as_ref().len() as u64);

        vbyte_len + self.encoded_strings.as_ref().len()
    }
}

#[derive(Clone)]
pub struct PfcDict<M:AsRef<[u8]>+Clone> {
    n_strings: u64,
    block_offsets: LogArray<M>,
    blocks: M
}

pub struct PfcDictIterator<'a,M:AsRef<[u8]>+Clone> {
    dict: &'a PfcDict<M>,
    block_index: usize,
    block: Option<OwnedPfcBlockIterator<&'a [u8]>>
}

impl<'a,M:AsRef<[u8]>+Clone> Iterator for PfcDictIterator<'a,M> {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.block_index >= self.dict.block_offsets.len() + 1 {
            return None
        }
        else if self.block.is_none() {
            let block_offset = if self.block_index == 0 { 0 } else { self.dict.block_offsets.entry(self.block_index-1) } as usize;
            let remainder = self.dict.n_strings as usize - self.block_index * BLOCK_SIZE;
            if remainder >= BLOCK_SIZE {
                self.block = Some(PfcBlock::parse(&self.dict.blocks.as_ref()[block_offset..]).unwrap().into_strings());
            }
            else {
                self.block = Some(PfcBlock::parse_incomplete(&self.dict.blocks.as_ref()[block_offset..], remainder).unwrap().into_strings());
            }
        }

        match self.block.as_mut().unwrap().next() {
            None => {
                self.block_index += 1;
                self.block = None;
                self.next()
            }
            Some(s) => Some(s)
        }
    }
}

impl<M:AsRef<[u8]>+Clone> PfcDict<M> {
    pub fn parse(blocks: M, offsets: M) -> Result<PfcDict<M>,PfcError> {
        let n_strings = BigEndian::read_u64(&blocks.as_ref()[blocks.as_ref().len()-8..]);

        let block_offsets = LogArray::parse(offsets)?;

        Ok(PfcDict {
            n_strings: n_strings,
            block_offsets: block_offsets,
            blocks: blocks
        })
    }

    pub fn len(&self) -> usize {
        self.n_strings as usize
    }

    pub fn get(&self, ix: usize) -> String {
        if ix as u64 >= self.n_strings {
            panic!("index too high");
        }

        let block_index = ix / BLOCK_SIZE;
        let block_offset = if block_index == 0 { 0 } else { self.block_offsets.entry(block_index-1) };
        let block = PfcBlock::parse(&self.blocks.as_ref()[block_offset as usize..]).unwrap();

        let index_in_block = ix % BLOCK_SIZE;
        block.get(index_in_block)
    }

    pub fn id(&self, s: &str) -> Option<u64> {
        // let's binary search
        let mut min = 0;
        let mut max = self.block_offsets.len();
        let mut mid = 0; // it's going to get overwritten below, but rust seems to think it may not be

        while min <= max {
            mid = (min + max) / 2;

            let block_offset = if mid == 0 { 0 } else {self.block_offsets.entry(mid-1) as usize};
            let block_slice = &self.blocks.as_ref()[block_offset..]; // this is probably more than one block, but we're only interested in the first string anyway
            let head_end = block_slice.iter().position(|&b|b==0).unwrap();
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
                },
                Ordering::Greater => min = mid + 1,
                Ordering::Equal => return Some((mid * BLOCK_SIZE) as u64) // what luck! turns out the string we were looking for was the block head
            }
        }

        let found = max;

        // we found the block the string should be part of.
        let block_start = if found == 0 { 0 } else {self.block_offsets.entry(found-1) as usize};
        let remainder = self.n_strings as usize - (found * BLOCK_SIZE);
        let block = if remainder >= BLOCK_SIZE {
            PfcBlock::parse(&self.blocks.as_ref()[block_start..]).unwrap()
        } else {
            PfcBlock::parse_incomplete(&self.blocks.as_ref()[block_start..], remainder as usize).unwrap()
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

    pub fn strings(&self) -> PfcDictIterator<M> {
        PfcDictIterator {
            dict: &self,
            block_index: 0,
            block: None
        }
    }
}

pub struct PfcDictFileBuilder<W:tokio::io::AsyncWrite> {
    /// the file that this builder writes the pfc blocks to
    pfc_blocks_file: W,
    /// the file that this builder writes the block offsets to
    pfc_block_offsets_file: W,
    /// the amount of strings in this dict so far
    count: usize,
    /// the size in bytes of the pfc data structure so far
    size: usize,
    last: Option<Vec<u8>>,
    index: Vec<u64>
}

impl<W:'static+tokio::io::AsyncWrite> PfcDictFileBuilder<W> {
    pub fn new(pfc_blocks_file: W, pfc_block_offsets_file: W) -> PfcDictFileBuilder<W> {
        PfcDictFileBuilder {
            pfc_blocks_file,
            pfc_block_offsets_file,
            count: 0,
            size: 0,
            last: None,
            index: Vec::new()
        }
    }
    pub fn add(self, s: &str) -> Box<dyn Future<Item=(u64, PfcDictFileBuilder<W>),Error=std::io::Error>> {
        let count = self.count;
        let size = self.size;
        let mut index = self.index;

        let bytes = s.as_bytes().to_vec();
        if self.count % BLOCK_SIZE == 0 {
            if self.count != 0 {
                // this is the start of a block, but not the start of the first block
                // we need to store an index
                index.push(size as u64);
            }
            let pfc_block_offsets_file = self.pfc_block_offsets_file;
            Box::new(write_nul_terminated_bytes(self.pfc_blocks_file, bytes.clone())
                     .and_then(move |(f, len)| future::ok(((count+1) as u64, PfcDictFileBuilder {
                         pfc_blocks_file: f,
                         pfc_block_offsets_file,
                         count: count + 1,
                         size: size + len,
                         last: Some(bytes),
                         index: index
                     }))))
        }
        else {
            let s_bytes = s.as_bytes();
            let common = find_common_prefix(&self.last.unwrap(), s_bytes);
            let postfix = s_bytes[common..].to_vec();
            let pfc_block_offsets_file = self.pfc_block_offsets_file;
            Box::new(VByte::write(common as u64, self.pfc_blocks_file)
                .and_then(move |(pfc_blocks_file,vbyte_len)| write_nul_terminated_bytes(pfc_blocks_file, postfix)
                          .map(move |(pfc_blocks_file, slice_len)| ((count+1) as u64, PfcDictFileBuilder {
                              pfc_blocks_file,
                              pfc_block_offsets_file,
                              count: count + 1,
                              size: size + vbyte_len + slice_len,
                              last: Some(bytes),
                              index: index
                          }))))
        }
    }

    fn add_all_1<I:'static+Iterator<Item=String>>(self, mut it:I, mut result: Vec<u64>) -> Box<dyn Future<Item=(Vec<u64>, PfcDictFileBuilder<W>), Error=std::io::Error>> {
        let next = it.next();
        match next {
            None => Box::new(future::ok((result, self))),
            Some(s) => Box::new(self.add(&s)
                                .and_then(move |(r,b)| {
                                    result.push(r);
                                    b.add_all_1(it, result)
                                }))
        }
    }

    pub fn add_all<I:'static+Iterator<Item=String>>(self, it:I) -> Box<dyn Future<Item=(Vec<u64>, PfcDictFileBuilder<W>), Error=std::io::Error>> {
        self.add_all_1(it, Vec::new())
    }

    /// finish the data structure
    pub fn finalize(self) -> impl Future<Item=(),Error=std::io::Error> {
        let width = if self.index.len() == 0 { 1 } else {64-self.index[self.index.len()-1].leading_zeros()};
        let builder = LogArrayFileBuilder::new(self.pfc_block_offsets_file, width as u8);
        let count = self.count;

        let write_offsets = builder.push_all(futures::stream::iter_ok(self.index))
            .and_then(|b|b.finalize());

        let finalize_blocks = write_padding(self.pfc_blocks_file, self.size, 8)
            .and_then(move |(w, _n_pad)| {
                let mut bytes = vec![0;8];
                BigEndian::write_u64(&mut bytes, count as u64);
                tokio::io::write_all(w, bytes)
            })
            .and_then(|(w,_)| tokio::io::flush(w));

        write_offsets.join(finalize_blocks)
            .map(|_|())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_io::io::AllowStdIo;
    use crate::storage::file::*;

    #[test]
    fn can_create_pfc_dict_small() {
        let contents = vec!["aaaaa",
                            "aabbb",
                            "ccccc"];
        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());
        builder.add_all(contents.into_iter().map(|s|s.to_string()))
            .and_then(|(_,b)|b.finalize())
            .wait().unwrap();

        let blocks_map = blocks.map();
        let offsets_map = offsets.map();

        let p = PfcDict::parse(blocks.map(), offsets.map()).unwrap();

        assert_eq!("aaaaa", p.get(0));
        assert_eq!("aabbb", p.get(1));
        assert_eq!("ccccc", p.get(2));
    }

    #[test]
    fn can_create_pfc_dict_large() {
        let contents = vec!["aaaaa",
                            "aabbb",
                            "ccccc",
                            "ddddd asfdl;kfasf opxcvucvkhf asfopihvpvoihfasdfjv;xivh",
                            "deasdfvv apobk,naf;libpoiujsafd",
                            "deasdfvv apobk,x",
                            "ee",
                            "eee",
                            "eeee",
                            "great scott"
        ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        builder.add_all(contents.into_iter().map(|s|s.to_string()))
            .and_then(|(_,b)|b.finalize())
            .wait().unwrap();

        let p = PfcDict::parse(blocks.map(), offsets.map()).unwrap();

        assert_eq!("aaaaa", p.get(0));
        assert_eq!("aabbb", p.get(1));
        assert_eq!("ccccc", p.get(2));
        assert_eq!("eeee", p.get(8));
        assert_eq!("great scott", p.get(9));
    }

    #[test]
    fn retrieve_id_from_dict() {
        let contents = vec![
            "aaaaa",
            "aaaaaaaaaa",
            "arf",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
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
            "hai hai hai"
            ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        builder.add_all(contents.into_iter().map(|s|s.to_string()))
            .and_then(|(_,b)|b.finalize())
            .wait().unwrap();

        let dict = PfcDict::parse(blocks.map(), offsets.map()).unwrap();

        assert_eq!(Some(0), dict.id("aaaaa"));
        assert_eq!(Some(2), dict.id("arf"));
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
            "arf",
            "aaaabbbbbb",
            "abcdefghijk",
            "addeeerafa",
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
            "hai hai hai"
            ];

        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());

        builder.add_all(contents.clone().into_iter().map(|s|s.to_string()))
            .and_then(|(_,b)|b.finalize())
            .wait().unwrap();

        let dict = PfcDict::parse(blocks.map(), offsets.map()).unwrap();

        let result: Vec<String> = dict.strings().collect();
        assert_eq!(contents, result);
    }
}
