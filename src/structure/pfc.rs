//! Implementation for a Plain Front-Coding (PFC) dictionary.

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};
use std::cmp::{Ord, Ordering};
use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
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

impl fmt::Display for PfcError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
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

pub struct PfcBlockEntryIterator {
    block: PfcBlock,
    count: usize,
    pos: usize,
}

impl Iterator for PfcBlockEntryIterator {
    type Item = (usize, Bytes);

    fn next(&mut self) -> Option<(usize, Bytes)> {
        if self.pos == 0 {
            self.count = 1;
            let head = self.block.head();
            self.pos = head.len() + 1;

            Some((0, head))
        } else if self.count < self.block.n_strings {
            // at pos we read a vbyte with the length of the common prefix
            let (common, common_len) =
                vbyte::decode(&self.block.encoded_strings.as_ref()[self.pos..])
                    .expect("encoding error in self-managed data");

            self.pos += common_len;

            // next up is the suffix, again as a nul-terminated string.
            let postfix_end = self.pos
                + self.block.encoded_strings.as_ref()[self.pos..]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap();

            let result = (
                common
                    .try_into()
                    .expect("string prefix was too long to fit in a usize"),
                self.block.encoded_strings.slice(self.pos..postfix_end),
            );

            self.pos = postfix_end + 1;
            self.count += 1;

            Some(result)
        } else {
            None
        }
    }
}

pub struct PfcBlockIterator {
    entry_iterator: PfcBlockEntryIterator,
    string: Vec<u8>,
}

impl Iterator for PfcBlockIterator {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if let Some((prefix_size, postfix)) = self.entry_iterator.next() {
            let mut prefix = self.string[..prefix_size].to_vec();
            prefix.extend_from_slice(postfix.as_ref());
            self.string = prefix;

            Some(String::from_utf8(self.string.clone()).unwrap())
        } else {
            None
        }
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

    pub fn head(&self) -> Bytes {
        let first_end = self
            .encoded_strings
            .as_ref()
            .iter()
            .position(|&b| b == 0)
            .unwrap();

        self.encoded_strings.slice(..first_end)
    }

    fn block_entries(&self) -> PfcBlockEntryIterator {
        PfcBlockEntryIterator {
            block: self.clone(),
            count: 0,
            pos: 0,
        }
    }

    fn entries(&self) -> PfcDictEntryIterator {
        PfcDictEntryIterator {
            block_iter: self.block_entries(),
            parts: Vec::with_capacity(BLOCK_SIZE),
        }
    }

    pub fn strings(&self) -> PfcBlockIterator {
        PfcBlockIterator {
            entry_iterator: self.block_entries(),
            string: Vec::with_capacity(BLOCK_SIZE),
        }
    }

    pub fn entry(&self, index: usize) -> Option<PfcDictEntry> {
        if index < self.n_strings {
            let entries: Vec<_> = self.block_entries().take(index + 1).collect();
            let mut take_prefix_lengths = vec![0_usize; entries.len() - 1];

            // first, gather all the prefix lengths.
            // we scan the prefix lengths in reverse order, and make sure that
            // each prefix that we write down is less than or equal to a later
            // prefix. This way we never take too much.
            let (mut last, _) = entries[index];
            for ix in (1..entries.len()).rev() {
                let (prefix, _) = entries[ix];
                if prefix < last {
                    take_prefix_lengths[ix - 1] = prefix;
                    last = prefix;
                } else {
                    take_prefix_lengths[ix - 1] = last;
                }

                if last == 0 {
                    break;
                }
            }

            // Having written down the prefixes, we now turn it into a list
            // of how much prefix we're interested in for every individual string.
            // This is a simple matter of subtracting two adjacent entries.
            for ix in (1..take_prefix_lengths.len()).rev() {
                take_prefix_lengths[ix] -= take_prefix_lengths[ix - 1];
            }

            let (_, postfix) = &entries[index];
            let mut result = Vec::with_capacity(BLOCK_SIZE);

            for ((_, entry), take) in entries.iter().zip(take_prefix_lengths.iter()) {
                result.push(entry.slice(..*take));
            }

            result.push(postfix.clone());

            Some(PfcDictEntry::new_optimized(result))
        } else {
            None
        }
    }

    pub fn get(&self, index: usize) -> Option<String> {
        if let Some(entry) = self.entry(index) {
            Some(entry.to_string())
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        let len = self.encoded_strings.as_ref().len();
        vbyte::encoding_len(len as u64) + len
    }
}

pub struct PfcDictBlockIterator {
    dict: PfcDict,
    block_index: usize,
}

impl PfcDictBlockIterator {
    fn new(dict: PfcDict) -> Self {
        Self {
            dict,
            block_index: 0,
        }
    }
}

impl Iterator for PfcDictBlockIterator {
    type Item = PfcBlock;

    fn next(&mut self) -> Option<PfcBlock> {
        if self.block_index > self.dict.block_offsets.len() {
            None
        } else {
            let block_offset = if self.block_index == 0 {
                0
            } else {
                self.dict.block_offsets.entry(self.block_index - 1)
            } as usize;
            let remainder = self.dict.n_strings as usize - self.block_index * BLOCK_SIZE;

            if remainder == 0 {
                return None;
            }

            self.block_index += 1;

            let mut block = self.dict.blocks.clone();
            block.advance(block_offset);
            if remainder >= BLOCK_SIZE {
                Some(PfcBlock::parse(block).unwrap())
            } else {
                Some(PfcBlock::parse_incomplete(block, remainder).unwrap())
            }
        }
    }
}

pub struct PfcDictEntryIterator {
    block_iter: PfcBlockEntryIterator,
    parts: Vec<Bytes>,
}

impl Iterator for PfcDictEntryIterator {
    type Item = PfcDictEntry;
    fn next(&mut self) -> Option<PfcDictEntry> {
        if let Some((mut prefix_len, bytes)) = self.block_iter.next() {
            let mut end;
            if prefix_len == 0 {
                end = 0;
            } else {
                end = self.parts.len();
                for (index, part) in self.parts.iter_mut().enumerate() {
                    match prefix_len.cmp(&part.len()) {
                        Ordering::Greater => {
                            prefix_len -= part.len();
                        }
                        Ordering::Less => {
                            end = index + 1;
                            let new_bytes = part.slice(..prefix_len);
                            *part = new_bytes;
                            break;
                        }
                        Ordering::Equal => {
                            end = index + 1;
                            break;
                        }
                    }
                }
            }

            self.parts.truncate(end);
            self.parts.push(bytes);

            Some(PfcDictEntry::new_optimized(self.parts.clone()))
        } else {
            None
        }
    }
}

/// An entry in a pfc dictionary.
///
/// This is a low-memory structure, basically just holding a pointer, some metadata and a block entry number.
/// Its purpose is for use in sorting of dictionary entries without having to copy a lot of strings.
#[derive(Clone, Debug)]
pub struct PfcDictEntry {
    parts: Vec<Bytes>,
}

impl PfcDictEntry {
    pub fn new(parts: Vec<Bytes>) -> Self {
        Self { parts }
    }

    pub fn new_optimized(parts: Vec<Bytes>) -> Self {
        let mut entry = Self::new(parts);
        entry.optimize();

        entry
    }

    pub fn len(&self) -> usize {
        self.parts.iter().map(|b| b.len()).sum::<usize>()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let len = self.len();
        let mut vec = Vec::with_capacity(len);

        for part in self.parts.iter() {
            vec.extend(part);
        }

        vec
    }

    /// optimize size
    ///
    /// For short strings, a list of pointers may be much less
    /// efficient than a copy of the string.  This will copy the
    /// underlying string if that is the case.
    pub fn optimize(&mut self) {
        let overhead_size = std::mem::size_of::<Bytes>() * self.parts.len();

        if std::mem::size_of::<Bytes>() + self.len() < overhead_size {
            let mut bytes = BytesMut::with_capacity(self.len());
            for part in self.parts.iter() {
                bytes.extend(part);
            }

            self.parts = vec![bytes.freeze()];
        }
    }

    pub fn buf_eq<B: Buf>(&self, mut b: B) -> bool {
        if self.len() != b.remaining() {
            false
        } else if self.len() == 0 {
            true
        } else {
            let mut it = self.parts.iter();
            let mut part = it.next().unwrap();
            loop {
                let slice = b.chunk();

                match part.len().cmp(&slice.len()) {
                    Ordering::Less => {
                        if part.as_ref() != &slice[..part.len()] {
                            return false;
                        }
                    }
                    Ordering::Equal => {
                        if part != slice {
                            return false;
                        }

                        assert!(it.next().is_none());
                        return true;
                    }
                    Ordering::Greater => {
                        panic!("This should never happen because it'd mean our entry is larger than the buffer passed in, but we already checked to make sure that is not the case");
                    }
                }

                b.advance(part.len());
                part = it.next().unwrap();
            }
        }
    }
}

impl PartialEq for PfcDictEntry {
    fn eq(&self, other: &Self) -> bool {
        // unequal length, so can't be equal
        if self.len() != other.len() {
            return false;
        }

        self.cmp(other) == Ordering::Equal
    }
}

impl fmt::Display for PfcDictEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let vec = self.to_bytes();
        write!(f, "{}", String::from_utf8(vec).unwrap())
    }
}

impl Eq for PfcDictEntry {}

impl Hash for PfcDictEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for part in self.parts.iter() {
            state.write(part);
        }
    }
}

impl Ord for PfcDictEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // both are empty, so equal
        if self.len() == 0 && other.len() == 0 {
            return Ordering::Equal;
        }

        let mut it1 = self.parts.iter();
        let mut it2 = other.parts.iter();
        let mut part1 = it1.next().unwrap().clone();
        let mut part2 = it2.next().unwrap().clone();

        loop {
            match part1.len().cmp(&part2.len()) {
                Ordering::Equal => {
                    match part1.cmp(&part2) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => {}
                    }

                    let p1_next = it1.next();
                    let p2_next = it2.next();

                    if let (Some(p1), Some(p2)) = (p1_next, p2_next) {
                        part1 = p1.clone();
                        part2 = p2.clone();
                    } else if p1_next.is_none() && p2_next.is_none() {
                        // done! everything has been compared equally and nothign remains.
                        return Ordering::Equal;
                    } else if p1_next.is_none() {
                        // the left side is a prefix of the right side

                        return Ordering::Less;
                    } else {
                        return Ordering::Greater;
                    }
                }
                Ordering::Less => {
                    let part2_slice = part2.slice(0..part1.len());
                    match part1.cmp(&part2_slice) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => {}
                    }

                    part2 = part2.slice(part1.len()..);
                    let part1_option = it1.next();
                    if part1_option.is_none() {
                        return Ordering::Less;
                    }
                    part1 = part1_option.unwrap().clone();
                }
                Ordering::Greater => {
                    let part1_slice = part1.slice(0..part2.len());
                    match part1_slice.cmp(&part2) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => {}
                    }

                    part1 = part1.slice(part2.len()..);
                    let part2_option = it2.next();
                    if part2_option.is_none() {
                        return Ordering::Greater;
                    }
                    part2 = part2_option.unwrap().clone();
                }
            }
        }
    }
}

impl PartialOrd for PfcDictEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub struct PfcDict {
    n_strings: u64,
    block_offsets: LogArray,
    blocks: Bytes,
}

impl PfcDict {
    pub fn parse(blocks: Bytes, offsets: Bytes) -> Result<PfcDict, PfcError> {
        let n_strings = BigEndian::read_u64(&blocks.as_ref()[blocks.as_ref().len() - 8..]);

        let block_offsets = LogArray::parse(offsets)?;

        Ok(PfcDict {
            n_strings,
            block_offsets,
            blocks,
        })
    }

    pub fn len(&self) -> usize {
        self.n_strings as usize
    }

    fn calculate_block_offset_index(&self, ix: usize) -> Option<(u64, usize)> {
        if (ix as u64) < self.n_strings {
            let block_index = ix / BLOCK_SIZE;
            let block_offset = if block_index == 0 {
                0
            } else {
                self.block_offsets.entry(block_index - 1)
            };

            let index_in_block = ix % BLOCK_SIZE;
            Some((block_offset, index_in_block))
        } else {
            None
        }
    }

    pub fn entry(&self, ix: usize) -> Option<PfcDictEntry> {
        if let Some((block_offset, index_in_block)) = self.calculate_block_offset_index(ix) {
            let mut block_bytes = self.blocks.clone();
            block_bytes.advance(block_offset as usize);

            let block = PfcBlock::parse(block_bytes).unwrap();
            block.entry(index_in_block)
        } else {
            None
        }
    }

    pub fn get(&self, ix: usize) -> Option<String> {
        if let Some((block_offset, index_in_block)) = self.calculate_block_offset_index(ix) {
            let mut block_bytes = self.blocks.clone();
            block_bytes.advance(block_offset as usize);

            let block = PfcBlock::parse(block_bytes).unwrap();
            block.get(index_in_block)
        } else {
            None
        }
    }

    pub fn id(&self, s: &str) -> Option<u64> {
        let s_bytes = s.as_bytes();
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

            match s_bytes.cmp(head_slice) {
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

        for (count, block_entry) in block.entries().enumerate() {
            if block_entry.buf_eq(s_bytes) {
                return Some((found * BLOCK_SIZE + count) as u64);
            }
        }

        None
    }

    pub fn strings(&self) -> impl Iterator<Item = String> {
        let block_iterator = PfcDictBlockIterator::new(self.clone());

        block_iterator.flat_map(|block| block.strings())
    }

    pub fn entries(&self) -> impl Iterator<Item = PfcDictEntry> {
        let block_iterator = PfcDictBlockIterator::new(self.clone());

        block_iterator.flat_map(|block| block.entries())
    }
}

pub struct PfcDictFileBuilder<W: SyncableFile> {
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

impl<W: 'static + SyncableFile> PfcDictFileBuilder<W> {
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

    pub async fn add_entry(&mut self, e: &PfcDictEntry) -> io::Result<u64> {
        let bytes = e.to_bytes();
        self.add_bytes(&bytes).await
    }

    pub async fn add(&mut self, s: &str) -> io::Result<u64> {
        let bytes = s.as_bytes();
        self.add_bytes(bytes).await
    }

    pub async fn add_bytes(&mut self, bytes: &[u8]) -> io::Result<u64> {
        if self.count % BLOCK_SIZE == 0 {
            if self.count != 0 {
                // this is the start of a block, but not the start of the first block
                // we need to store an index
                self.index.push(self.size as u64);
            }
            let len = write_nul_terminated_bytes(&mut self.pfc_blocks_file, bytes).await?;
            self.size += len;
        } else {
            let common = find_common_prefix(&self.last.as_ref().unwrap(), bytes);
            let postfix = bytes[common..].to_vec();
            let common_len = vbyte::write_async(&mut self.pfc_blocks_file, common as u64).await?;
            let slice_len = write_nul_terminated_bytes(&mut self.pfc_blocks_file, &postfix).await?;
            self.size += common_len + slice_len;
        }

        self.count += 1;
        self.last = Some(bytes.to_vec());

        Ok(self.count as u64)
    }

    pub async fn add_all_entries<I: 'static + Iterator<Item = PfcDictEntry> + Send>(
        &mut self,
        it: I,
    ) -> io::Result<Vec<u64>> {
        let mut result = Vec::new();
        for next in it {
            let r = self.add_entry(&next).await?;
            result.push(r);
        }

        Ok(result)
    }

    pub async fn add_all<'a, I: 'static + Iterator<Item = &'a str> + Send>(
        &mut self,
        it: I,
    ) -> io::Result<Vec<u64>> {
        let mut result = Vec::new();
        for next in it {
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

        builder.push_vec(self.index).await?;
        builder.finalize().await?;

        write_padding(&mut self.pfc_blocks_file, self.size, 8).await?;
        write_u64(&mut self.pfc_blocks_file, count).await?;
        self.pfc_blocks_file.flush().await?;
        self.pfc_blocks_file.sync_all().await?;

        Ok(())
    }
}

struct PfcDecoder {
    last: Option<BytesMut>,
    index: usize,
    total: u64,
}

impl PfcDecoder {
    fn new(total: u64) -> Self {
        Self {
            last: None,
            index: 0,
            total,
        }
    }
}

impl Decoder for PfcDecoder {
    type Item = String;
    type Error = io::Error;
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<String>, io::Error> {
        if self.index as u64 == self.total {
            bytes.clear();
            return Ok(None);
        }

        let pos;
        let vbyte;

        match self.index % 8 == 0 {
            true => {
                // this is the start of a block. we expect a 0-delimited cstring
                pos = bytes.iter().position(|&b| b == 0);
                vbyte = None;
            }
            false => {
                // This is in the middle of some block. we expect a vbyte followed by some 0-delimited cstring
                match vbyte::decode(&bytes) {
                    Ok((prefix_len, vbyte_len)) => {
                        pos = bytes.iter().skip(vbyte_len).position(|&b| b == 0);
                        if pos.is_none() {
                            // we haven't read enough yet to extract a full string. don't advance anything.
                            return Ok(None);
                        }

                        bytes.advance(vbyte_len);
                        vbyte = Some(prefix_len);
                    }
                    // The buffer might be in the middle of a vbyte. If that's the case, prompt for more data.
                    Err(vbyte::DecodeError::UnexpectedEndOfBuffer) => return Ok(None),
                    Err(e) => panic!("error decoding vbyte in pfc block: {:?}", e),
                }
            }
        };

        match pos {
            None => Ok(None),
            Some(pos) => {
                let b = bytes.split_to(pos);
                bytes.advance(1);
                match vbyte {
                    None => {
                        let s = String::from_utf8(b.to_vec()).expect("expected utf8 string");
                        self.last = Some(b);
                        self.index += 1;

                        Ok(Some(s))
                    }
                    Some(prefix_len) => {
                        let last = self.last.as_ref().unwrap();
                        let mut full = BytesMut::with_capacity(prefix_len as usize + b.len());
                        full.extend_from_slice(&last[..prefix_len as usize]);
                        full.extend_from_slice(&b);

                        let s = String::from_utf8(full.to_vec()).expect("expected utf8 string");
                        self.last = Some(full);
                        self.index += 1;

                        Ok(Some(s))
                    }
                }
            }
        }
    }
}

pub async fn dict_file_get_count<F: 'static + FileLoad>(file: F) -> io::Result<u64> {
    let mut result = vec![0; 8];
    file.open_read_from(file.size().await? - 8)
        .await?
        .read_exact(&mut result)
        .await?;
    Ok(BigEndian::read_u64(&result))
}

pub async fn dict_file_to_stream<F: 'static + FileLoad>(
    file: F,
) -> io::Result<impl Stream<Item = io::Result<String>> + Unpin + Send> {
    let total = dict_file_get_count(file.clone()).await?;
    Ok(dict_reader_to_stream(file.open_read().await?, total))
}

pub fn dict_reader_to_stream<A: 'static + AsyncRead + Unpin + Send>(
    r: A,
    total: u64,
) -> impl Stream<Item = io::Result<String>> + Unpin + Send {
    FramedRead::new(r, PfcDecoder::new(total))
}

pub async fn dict_file_to_indexed_stream<F: 'static + FileLoad>(
    file: F,
    offset: u64,
) -> io::Result<impl Stream<Item = io::Result<(u64, String)>> + Unpin + Send> {
    let total = dict_file_get_count(file.clone()).await?;
    Ok(dict_reader_to_indexed_stream(
        file.open_read().await?,
        offset,
        total,
    ))
}

pub fn dict_reader_to_indexed_stream<A: 'static + AsyncRead + Unpin + Send>(
    r: A,
    offset: u64,
    total: u64,
) -> impl Stream<Item = io::Result<(u64, String)>> + Send {
    let dict_stream = dict_reader_to_stream(r, total);

    dict_stream.enumerate().map(move |(i, x)| match x {
        Ok(x) => Ok(((i + 1) as u64 + offset, x)),
        Err(e) => Err(e),
    })
}

pub async fn merge_dictionaries<
    'a,
    F: 'static + FileLoad + FileStore,
    I: Iterator<Item = &'a PfcDict>,
>(
    dictionaries: I,
    dict_files: DictionaryFiles<F>,
) -> io::Result<()> {
    let iterators: Vec<_> = dictionaries.map(|d| d.entries()).collect();

    let pick_fn = |vals: &[Option<&PfcDictEntry>]| {
        vals.iter()
            .enumerate()
            .filter(|(_, v)| v.is_some())
            .min_by(|(_, x), (_, y)| x.cmp(y))
            .map(|(ix, _)| ix)
    };

    let sorted_iterator = sorted_iterator(iterators, pick_fn);

    let mut builder = PfcDictFileBuilder::new(
        dict_files.blocks_file.open_write().await?,
        dict_files.offsets_file.open_write().await?,
    );

    builder.add_all_entries(sorted_iterator).await?;
    builder.finalize().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use futures::stream::TryStreamExt;

    #[tokio::test]
    async fn can_create_pfc_dict_small() {
        let contents = vec!["aaaaa", "aabbb", "ccccc"];
        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );
        builder.add_all(contents.into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let p = PfcDict::parse(blocks.map().await.unwrap(), offsets.map().await.unwrap()).unwrap();

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

    #[tokio::test]
    async fn can_create_pfc_dict_large() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let p = PfcDict::parse(blocks.map().await.unwrap(), offsets.map().await.unwrap()).unwrap();

        assert_eq!(Some("aaaaa".to_string()), p.get(0));
        assert_eq!(Some("aabbb".to_string()), p.get(1));
        assert_eq!(Some("ccccc".to_string()), p.get(2));
        assert_eq!(Some("eeee".to_string()), p.get(8));
        assert_eq!(Some("great scott".to_string()), p.get(9));
        assert_eq!(None, p.get(10));
    }

    #[tokio::test]
    async fn retrieve_id_from_dict() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let dict =
            PfcDict::parse(blocks.map().await.unwrap(), offsets.map().await.unwrap()).unwrap();

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

    #[tokio::test]
    async fn retrieve_all_strings() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.clone().into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let dict =
            PfcDict::parse(blocks.map().await.unwrap(), offsets.map().await.unwrap()).unwrap();

        let result: Vec<String> = dict.strings().collect();
        assert_eq!(contents, result);
    }

    #[tokio::test]
    async fn retrieve_all_strings_from_file() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.clone().into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let stream = dict_file_to_stream(blocks).await.unwrap();

        let result: Vec<String> = stream.try_collect().await.unwrap();
        assert_eq!(contents, result);
    }

    #[tokio::test]
    async fn retrieve_all_strings_from_file_multiple_of_eight() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.clone().into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let stream = dict_file_to_stream(blocks).await.unwrap();

        let result: Vec<String> = stream.try_collect().await.unwrap();
        assert_eq!(contents, result);
    }

    #[tokio::test]
    async fn retrieve_all_indexed_strings_from_file() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.clone().into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let stream = dict_file_to_indexed_stream(blocks, 0).await.unwrap();

        let result: Vec<(u64, String)> = stream.try_collect().await.unwrap();
        assert_eq!((1, "aaaaa".to_string()), result[0]);
        assert_eq!((8, "barf".to_string()), result[7]);
        assert_eq!((9, "berf".to_string()), result[8]);
    }

    #[tokio::test]
    async fn get_pfc_count_from_file() {
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
        let mut builder = PfcDictFileBuilder::new(
            blocks.open_write().await.unwrap(),
            offsets.open_write().await.unwrap(),
        );

        builder.add_all(contents.clone().into_iter()).await.unwrap();
        builder.finalize().await.unwrap();

        let count = dict_file_get_count(blocks).await.unwrap();

        assert_eq!(18, count);
    }

    #[test]
    fn bufeq_empty_entry() {
        let entry = PfcDictEntry::new(Vec::new());

        assert!(entry.buf_eq(Bytes::from(b"".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"a".as_ref())));
    }

    #[test]
    fn bufeq_single_part_entry() {
        let entry = PfcDictEntry::new(vec![Bytes::from(b"aaaaa".as_ref())]);

        assert!(entry.buf_eq(Bytes::from(b"aaaaa".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"a".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"".as_ref())));
    }

    #[test]
    fn bufeq_multi_part_entry() {
        let contents: Vec<&[u8]> = vec![b"abcde", b"fghijkl", b"mnop"];

        let entry = PfcDictEntry::new(contents.into_iter().map(|b| Bytes::from(b)).collect());

        assert!(entry.buf_eq(Bytes::from(b"abcdefghijklmnop".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"abcde".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"abcdefghijkl".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"abcdefghijklxxxx".as_ref())));
        assert!(!entry.buf_eq(Bytes::from(b"".as_ref())));
    }

    #[test]
    fn compare_empty_entries() {
        let contents1: Vec<&[u8]> = Vec::new();
        let contents2: Vec<&[u8]> = Vec::new();

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert_eq!(entry1, entry2);
    }

    #[test]
    fn compare_entries_unequal_length_less() {
        let contents1: Vec<&[u8]> = vec![b"a"];
        let contents2: Vec<&[u8]> = vec![b"aaaaa"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert!(entry1 < entry2);
    }

    #[test]
    fn compare_entries_unequal_length_greater() {
        let contents1: Vec<&[u8]> = vec![b"aaaaa"];
        let contents2: Vec<&[u8]> = vec![b"a"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert!(entry1 > entry2);
    }

    #[test]
    fn compare_entries_equal_single_part() {
        let contents1: Vec<&[u8]> = vec![b"aaaaa"];
        let contents2: Vec<&[u8]> = vec![b"aaaaa"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert_eq!(entry1, entry2);
    }

    #[test]
    fn compare_entries_equal_multi_part() {
        let contents1: Vec<&[u8]> = vec![b"aaaaa", b"bcde", b"xyz"];
        let contents2: Vec<&[u8]> = vec![b"aaaaa", b"bcde", b"xyz"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert_eq!(entry1, entry2);
    }

    #[test]
    fn compare_entries_equal_but_different_parts() {
        let contents1: Vec<&[u8]> = vec![b"aaaaa", b"bcde", b"xyz"];
        let contents2: Vec<&[u8]> = vec![b"aaa", b"aabcd", b"ex", b"yz"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert_eq!(entry1, entry2);
    }

    #[test]
    fn compare_entries_equal_part_lengths_but_less() {
        let contents1: Vec<&[u8]> = vec![b"aaaaa", b"bcde", b"xyz"];
        let contents2: Vec<&[u8]> = vec![b"aaaaa", b"bdde", b"xyz"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert!(entry1 < entry2);
    }

    #[test]
    fn compare_entries_equal_part_lengths_but_greater() {
        let contents1: Vec<&[u8]> = vec![b"aaaaa", b"bdde", b"xyz"];
        let contents2: Vec<&[u8]> = vec![b"aaaaa", b"bcde", b"xyz"];

        let entry1 = PfcDictEntry::new(contents1.into_iter().map(|b| Bytes::from(b)).collect());
        let entry2 = PfcDictEntry::new(contents2.into_iter().map(|b| Bytes::from(b)).collect());

        assert!(entry1 > entry2);
    }
}
