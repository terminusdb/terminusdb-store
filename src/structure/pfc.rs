use byteorder::{ByteOrder,BigEndian};
use futures::prelude::*;
use futures::future;

use super::vbyte::*;
use super::logarray::*;
use super::util::*;

#[derive(Debug)]
pub enum PfcError {
    InvalidCoding,
    NotEnoughData
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

pub struct PfcBlock<M:AsRef<[u8]>+Clone> {
    encoded_strings: M
}

const BLOCK_SIZE: usize = 8;

impl<M:AsRef<[u8]>+Clone> PfcBlock<M> {
    pub fn parse(data: M) -> Result<PfcBlock<M>,PfcError> {
        Ok(PfcBlock { encoded_strings: data })
    }

    pub fn get(&self, mut index: usize) -> String {
        let first_end = self.encoded_strings.as_ref().iter().position(|&b|b == 0).unwrap();
        let mut x: Vec<u8> = Vec::new();
        x.extend_from_slice(&self.encoded_strings.as_ref()[..first_end]);

        let mut pos = first_end + 1;

        while index != 0 {
            let v = VByte::parse(&self.encoded_strings.as_ref()[pos..]).expect("encoding error in self-managed data");
            x.truncate(v.unpack() as usize);
            pos += v.len();

            let postfix_end = pos + self.encoded_strings.as_ref()[pos..].iter().position(|&b|b==0).unwrap();

            x.extend_from_slice(&self.encoded_strings.as_ref()[pos..postfix_end]);
            pos = postfix_end + 1;
            index -= 1;
        }
    
        String::from_utf8(x).unwrap()
    }

    pub fn len(&self) -> usize {
        let vbyte_len = VByte::required_len(self.encoded_strings.as_ref().len() as u64);

        vbyte_len + self.encoded_strings.as_ref().len()
    }
}

pub struct PfcDict<M:AsRef<[u8]>+Clone> {
    n_strings: u64,
    block_offsets: LogArray<M>,
    blocks: M
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
        let block_index = ix / BLOCK_SIZE;
        let block_offset = if block_index == 0 { 0 } else { self.block_offsets.entry(block_index-1) };
        let block = PfcBlock::parse(&self.blocks.as_ref()[block_offset as usize..]).unwrap();

        let index_in_block = ix % BLOCK_SIZE;
        block.get(index_in_block)
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
    pub fn add(self, s: &str) -> Box<dyn Future<Item=PfcDictFileBuilder<W>,Error=std::io::Error>> {
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
                     .and_then(move |(f, len)| future::ok(PfcDictFileBuilder {
                         pfc_blocks_file: f,
                         pfc_block_offsets_file,
                         count: count + 1,
                         size: size + len,
                         last: Some(bytes),
                         index: index
                     })))
        }
        else {
            let s_bytes = s.as_bytes();
            let common = find_common_prefix(&self.last.unwrap(), s_bytes);
            let postfix = s_bytes[common..].to_vec();
            let pfc_block_offsets_file = self.pfc_block_offsets_file;
            Box::new(VByte::write(common as u64, self.pfc_blocks_file)
                .and_then(move |(pfc_blocks_file,vbyte_len)| write_nul_terminated_bytes(pfc_blocks_file, postfix)
                          .map(move |(pfc_blocks_file, slice_len)| PfcDictFileBuilder {
                              pfc_blocks_file,
                              pfc_block_offsets_file,
                              count: count + 1,
                              size: size + vbyte_len + slice_len,
                              last: Some(bytes),
                              index: index
                          })))
        }
    }

    pub fn add_all<I:'static+Iterator<Item=String>>(self, mut it:I) -> Box<dyn Future<Item=PfcDictFileBuilder<W>, Error=std::io::Error>> {
        let next = it.next();
        match next {
            None => Box::new(future::ok(self)),
            Some(s) => Box::new(self.add(&s)
                                .and_then(move |b| b.add_all(it)))
            
        }
    }

    /// finish the data structure
    pub fn finalize(self) -> impl Future<Item=(),Error=std::io::Error> {
        let width = if self.index.len() == 0 { 1 } else {64-self.index[self.index.len()-1].leading_zeros()};
        let builder = LogArrayFileBuilder::new(self.pfc_block_offsets_file, width as u8);
        let size = self.size;
        let count = self.count;

        println!("finalizing with index {:?}", self.index);
        let write_offsets = builder.push_all(futures::stream::iter_ok(self.index))
            .and_then(|b|b.finalize());

        let finalize_blocks = write_padding(self.pfc_blocks_file, self.size, 8)
            .and_then(move |(w, n_pad)| {
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
    use super::super::storage::*;

    #[test]
    fn can_create_pfc_dict_small() {
        let contents = vec!["aaaaa",
                            "aabbb",
                            "ccccc"];
        let blocks = MemoryBackedStore::new();
        let offsets = MemoryBackedStore::new();
        let builder = PfcDictFileBuilder::new(blocks.open_write(), offsets.open_write());
        builder.add_all(contents.into_iter().map(|s|s.to_string()))
            .and_then(|b|b.finalize())
            .wait().unwrap();

        let blocks_map = blocks.map();
        let offsets_map = offsets.map();

        println!("blocks: {:?}, offsets: {:?}", blocks_map, offsets_map);

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
            .and_then(|b|b.finalize())
            .wait().unwrap();

        let p = PfcDict::parse(blocks.map(), offsets.map()).unwrap();

        assert_eq!("aaaaa", p.get(0));
        assert_eq!("aabbb", p.get(1));
        assert_eq!("ccccc", p.get(2));
        assert_eq!("eeee", p.get(8));
        assert_eq!("great scott", p.get(9));
    }
}
