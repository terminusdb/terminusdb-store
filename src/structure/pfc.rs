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

pub struct PfcBlock<'a> {
    encoded_strings: &'a [u8]
}

const BLOCK_SIZE: usize = 8;

impl<'a> PfcBlock<'a> {
    pub fn parse(data: &[u8]) -> Result<PfcBlock,PfcError> {
        Ok(PfcBlock { encoded_strings: data }) // todo maybe actually try to shorten the slice
    }

    pub fn get(&self, mut index: usize) -> String {
        let first_end = self.encoded_strings.iter().position(|&b|b == 0).unwrap();
        let mut x: Vec<u8> = Vec::new();
        x.extend_from_slice(&self.encoded_strings[..first_end]);

        let mut pos = first_end + 1;

        while index != 0 {
            let v = VByte::parse(&self.encoded_strings[pos..]).expect("encoding error in self-managed data");
            x.truncate(v.unpack() as usize);
            pos += v.len();

            let postfix_end = pos + self.encoded_strings[pos..].iter().position(|&b|b==0).unwrap();

            x.extend_from_slice(&self.encoded_strings[pos..postfix_end]);
            pos = postfix_end + 1;
            index -= 1;
        }
    
        String::from_utf8(x).unwrap()
    }

    pub fn len(&self) -> usize {
        let vbyte_len = VByte::required_len(self.encoded_strings.len() as u64);

        vbyte_len + self.encoded_strings.len()
    }
}

pub struct PfcDict<'a> {
    n_strings: u64,
    block_offsets: LogArray<&'a [u8]>,
    blocks: &'a [u8]
}

impl<'a> PfcDict<'a> {
    pub fn parse(data: &[u8]) -> Result<PfcDict,PfcError> {
        let n_strings = BigEndian::read_u64(&data[data.len()-8..]);
        let index_offset = BigEndian::read_u64(&data[data.len()-16..]);
        let blocks = &data[..index_offset as usize];

        let block_offsets = LogArray::parse(&data[index_offset as usize..data.len()-16])?;

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
        let block = PfcBlock::parse(&self.blocks[block_offset as usize..]).unwrap();

        let index_in_block = ix % BLOCK_SIZE;
        block.get(index_in_block)
    }
}

pub struct PfcDictFileBuilder<W:tokio::io::AsyncWrite> {
    /// the file that this builder writes the pfc blocks to
    pfc_file: W,
    /// the amount of strings in this dict so far
    count: usize,
    /// the size in bytes of the pfc data structure so far
    size: usize,
    last: Option<Vec<u8>>,
    index: Vec<u64>
}

impl<W:'static+tokio::io::AsyncWrite> PfcDictFileBuilder<W> {
    pub fn new(pfc: W) -> PfcDictFileBuilder<W> {
        PfcDictFileBuilder {
            pfc_file: pfc,
            count: 0,
            size: 0,
            last: None,
            index: Vec::new()
        }
    }
    pub fn add(self, s: &str) -> Box<Future<Item=PfcDictFileBuilder<W>,Error=std::io::Error>> {
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
            Box::new(write_nul_terminated_bytes(self.pfc_file, bytes.clone())
                     .and_then(move |(f, len)| future::ok(PfcDictFileBuilder {
                         pfc_file: f,
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
            Box::new(VByte::write(common as u64, self.pfc_file)
                .and_then(move |(pfc_file,vbyte_len)| write_nul_terminated_bytes(pfc_file, postfix)
                          .map(move |(pfc_file, slice_len)| PfcDictFileBuilder {
                              pfc_file: pfc_file,
                              count: count + 1,
                              size: size + vbyte_len + slice_len,
                              last: Some(bytes),
                              index: index
                          })))
        }
    }

    pub fn add_all<I:'static+Iterator<Item=String>>(self, mut it:I) -> Box<Future<Item=PfcDictFileBuilder<W>, Error=std::io::Error>> {
        let next = it.next();
        match next {
            None => Box::new(future::ok(self)),
            Some(s) => Box::new(self.add(&s)
                                .and_then(move |b| b.add_all(it)))
            
        }
    }

    /// finish the data structure
    pub fn finalize(self) -> impl Future<Item=W,Error=std::io::Error> {
        // so what do we need to do?
        // we're going to append to the pfc file (rather than separate file)
        // we need to pad just to make sure we're at a 8 byte offset
        // then we write the block indexes.
        // pad again to 8 bytes
        // write offset of block indexes as u64
        // write total number of entries as u64


        let width = if self.index.len() == 0 { 1 } else {64-self.index[self.index.len()-1].leading_zeros()};
        let builder = LogArrayBuilder::from_iter(width as u8, self.index.iter().map(|&i|i));
        let size = self.size;
        let count = self.count;

        write_padding(self.pfc_file, self.size, 8)
            .and_then(move |(w, n_pad)| {
                let index_offset = size + n_pad;
                builder.write(w)
                    .and_then(move |(w, _)| {
                        let mut bytes = vec![0;16];
                        BigEndian::write_u64(&mut bytes, index_offset as u64);
                        BigEndian::write_u64(&mut bytes[8..], count as u64);
                        tokio::io::write_all(w, bytes)
                    })
            })
            .and_then(|(w,_)| tokio::io::flush(w))
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use tokio_io::io::AllowStdIo;

    #[test]
    fn can_create_pfc_dict_small() {
        let contents = vec!["aaaaa",
                            "aabbb",
                            "ccccc"];
        let v = Vec::new();
        let wrapper = AllowStdIo::new(v);
        let builder = PfcDictFileBuilder::new(wrapper);
        let result = builder.add_all(contents.into_iter().map(|s|s.to_string()))
            .and_then(|b|b.finalize())
            .map(|w|w.into_inner())
            .wait().unwrap();

        let p = PfcDict::parse(&result).unwrap();

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
        let v = Vec::new();
        let wrapper = AllowStdIo::new(v);
        let builder = PfcDictFileBuilder::new(wrapper);
        let result = builder.add_all(contents.into_iter().map(|s|s.to_string()))
            .and_then(|b|b.finalize())
            .map(|w|w.into_inner())
            .wait().unwrap();

        let p = PfcDict::parse(&result).unwrap();

        assert_eq!("aaaaa", p.get(0));
        assert_eq!("aabbb", p.get(1));
        assert_eq!("ccccc", p.get(2));
        assert_eq!("eeee", p.get(8));
        assert_eq!("great scott", p.get(9));
    }
}
