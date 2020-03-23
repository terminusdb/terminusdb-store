//! Logic for storing, loading and using arrays of bits.
use super::util::*;
use crate::storage::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use tokio::codec::{Decoder, FramedRead};
use tokio::prelude::*;

#[derive(Clone)]
pub struct BitArray {
    bits: Bytes,
    /// how many bits are being used in the last 8 bytes?
    count: u64,
}

impl BitArray {
    pub fn from_bits(mut bits: Bytes) -> BitArray {
        let len = bits.len();
        if len < 8 || len % 8 != 0 {
            panic!("unexpected bitarray length");
        }

        let count = BigEndian::read_u64(&bits.split_off(len - 8));

        BitArray { bits, count }
    }

    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    pub fn len(&self) -> usize {
        self.count as usize // TODO on 32 bit platform this'll cut off
    }

    pub fn get(&self, index: usize) -> bool {
        if index > self.len() {
            panic!("index too high");
        }

        let byte = self.bits.as_ref()[index / 8];
        let mask: u8 = 128 >> (index % 8);

        byte & mask != 0
    }
}

pub struct BitArrayFileBuilder<W>
where
    W: 'static + AsyncWrite + Send,
{
    current_byte: u8,
    current_bit_pos: u8,
    bit_output: W,
    pub count: u64,
}

impl<W> BitArrayFileBuilder<W>
where
    W: 'static + AsyncWrite + Send,
{
    pub fn new(output: W) -> BitArrayFileBuilder<W> {
        BitArrayFileBuilder {
            current_byte: 0,
            current_bit_pos: 0,
            bit_output: output,
            count: 0,
        }
    }

    fn flush_current(
        self,
    ) -> Box<dyn Future<Item = BitArrayFileBuilder<W>, Error = std::io::Error> + Send> {
        let count = self.count;
        Box::new(
            tokio::io::write_all(self.bit_output, vec![self.current_byte]).map(move |(w, _)| {
                BitArrayFileBuilder {
                    current_byte: 0,
                    current_bit_pos: 0,
                    bit_output: w,
                    count: count,
                }
            }),
        )
    }

    pub fn push(
        mut self,
        bit: bool,
    ) -> Box<dyn Future<Item = BitArrayFileBuilder<W>, Error = std::io::Error> + Send> {
        let mut b = match bit {
            true => 128,
            false => 0,
        };
        b >>= self.current_bit_pos;
        self.current_byte |= b;
        self.current_bit_pos += 1;
        self.count += 1;

        if self.current_bit_pos == 8 {
            self.flush_current()
        } else {
            Box::new(future::ok(self))
        }
    }

    pub fn push_all<S: 'static + Stream<Item = bool, Error = std::io::Error> + Send>(
        self,
        stream: S,
    ) -> Box<dyn Future<Item = BitArrayFileBuilder<W>, Error = std::io::Error> + Send> {
        Box::new(stream.fold(self, |builder, bit| builder.push(bit)))
    }

    fn pad(self) -> impl Future<Item = W, Error = std::io::Error> {
        write_padding(self.bit_output, (self.count as usize + 7) / 8, 8)
            .map(|(bit_output, _)| bit_output)
    }

    pub fn finalize(self) -> impl Future<Item = W, Error = std::io::Error> {
        let count = self.count;
        let flush_current: Box<
            dyn Future<Item = BitArrayFileBuilder<W>, Error = std::io::Error> + Send,
        > = if count % 8 == 0 {
            Box::new(future::ok(self))
        } else {
            Box::new(self.flush_current())
        };

        flush_current
            .and_then(|b| b.pad())
            .and_then(move |w| write_u64(w, count))
            .and_then(|w| tokio::io::flush(w))
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

pub struct BitArrayBlockDecoder {
    readahead: Option<u64>,
}

impl Decoder for BitArrayBlockDecoder {
    type Item = u64;
    type Error = std::io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<u64>, std::io::Error> {
        if src.len() < 8 {
            return Ok(None);
        }

        let read_buf = src.split_to(8);
        let read = BigEndian::read_u64(&read_buf);

        let current = self.readahead;
        self.readahead = Some(read);
        match current {
            None => self.decode(src),
            Some(ra) => Ok(Some(ra)),
        }
    }
}

pub fn bitarray_stream_blocks<R: AsyncRead>(r: R) -> FramedRead<R, BitArrayBlockDecoder> {
    FramedRead::new(r, BitArrayBlockDecoder { readahead: None })
}

fn bitarray_count_from_file<F: FileLoad>(f: F) -> impl Future<Item = u64, Error = std::io::Error> {
    let offset = f.size() - 8;
    tokio::io::read_exact(f.open_read_from(offset), vec![0; 8])
        .map(|(_, buf)| BigEndian::read_u64(&buf))
}

fn block_bits(block: u64) -> Vec<bool> {
    let mut mask = 0x8000000000000000;
    let mut result = Vec::with_capacity(64);
    for _ in 0..64 {
        result.push(block & mask != 0);
        mask >>= 1;
    }

    result
}

pub fn bitarray_stream_bits<F: FileLoad>(f: F) -> impl Stream<Item = bool, Error = std::io::Error> {
    bitarray_count_from_file(f.clone())
        .into_stream()
        .map(move |count| {
            bitarray_stream_blocks(f.open_read())
                .map(|block| stream::iter_ok(block_bits(block).into_iter()))
                .flatten()
                .take(count)
        })
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;

    #[test]
    pub fn construct_and_parse_small_bitarray() {
        let x = MemoryBackedStore::new();
        let contents = vec![true, true, false, false, true];

        let builder = BitArrayFileBuilder::new(x.open_write());
        let _written = builder
            .push_all(stream::iter_ok(contents))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let loaded = x.map().wait().unwrap();

        let bitarray = BitArray::from_bits(loaded);

        assert_eq!(true, bitarray.get(0));
        assert_eq!(true, bitarray.get(1));
        assert_eq!(false, bitarray.get(2));
        assert_eq!(false, bitarray.get(3));
        assert_eq!(true, bitarray.get(4));
    }

    #[test]
    pub fn construct_and_parse_large_bitarray() {
        let x = MemoryBackedStore::new();
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        let builder = BitArrayFileBuilder::new(x.open_write());
        let _written = builder
            .push_all(stream::iter_ok(contents))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let loaded = x.map().wait().unwrap();

        let bitarray = BitArray::from_bits(loaded);

        for i in 0..bitarray.len() {
            assert_eq!(i % 3 == 0, bitarray.get(i));
        }
    }

    #[test]
    pub fn stream_blocks() {
        let x = MemoryBackedStore::new();
        let contents = (0..).map(|n| n % 4 == 1).take(256);

        let builder = BitArrayFileBuilder::new(x.open_write());
        let _written = builder
            .push_all(stream::iter_ok(contents))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let stream = bitarray_stream_blocks(x.open_read());

        stream
            .for_each(|block| Ok(assert_eq!(0x4444444444444444, block)))
            .wait()
            .unwrap();
    }

    #[test]
    fn stream_bits() {
        let x = MemoryBackedStore::new();
        let contents: Vec<_> = (0..).map(|n| n % 4 == 1).take(123).collect();

        let builder = BitArrayFileBuilder::new(x.open_write());
        let _written = builder
            .push_all(stream::iter_ok(contents.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let result = bitarray_stream_bits(x).collect().wait().unwrap();

        assert_eq!(contents, result);
    }
}
