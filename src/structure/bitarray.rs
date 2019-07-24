use byteorder::{ByteOrder,BigEndian};
use futures::prelude::*;
use tokio::prelude::*;
use tokio::codec::{FramedRead,Decoder};
use bytes::BytesMut;
use super::util::*;

#[derive(Clone)]
pub struct BitArray<'a> {
    bits: &'a [u8],
    /// how many bits are being used in the last 8 bytes?
    count: u64
}

impl<'a> BitArray<'a> {
    pub fn from_bits(bits: &[u8]) -> BitArray {
        if bits.len() < 8 || bits.len() % 8 != 0 {
            panic!("unexpected bitarray length");
        }

        let count = BigEndian::read_u64(&bits[bits.len()-8..]);

        BitArray {
            bits: &bits[..bits.len()-8],
            count: count
        }
    }

    pub fn bits(&self) -> &[u8] {
        self.bits
    }

    pub fn len(&self) -> usize {
        self.count as usize // TODO on 32 bit platform this'll cut off
    }

    pub fn get(&self, index: usize) -> bool {
        if index > self.len() {
            panic!("index too high");
        }

        let byte = self.bits[index/8];
        let mask: u8 = 128>>(index%8);

        byte & mask != 0
    }
}

pub struct BitArrayBuilder {
    bits: Vec<u8>,
    count: u64,
    last_use: u8
}

impl BitArrayBuilder {
    pub fn new() -> BitArrayBuilder {
        BitArrayBuilder {
            bits: Vec::new(),
            count: 0,
            last_use: 0,
        }
    }

    pub fn len(&self) -> usize {
        if self.bits.len() == 0 {
            0
        }
        else {
            (self.bits.len()-1) * 8 + self.last_use as usize
        }
    }

    #[inline]
    pub fn as_bitarray(&self) -> BitArray {
        BitArray { bits: &self.bits, count: self.count }
    }

    pub fn push(&mut self, val: bool) {
        if self.last_use % 8 == 0 {
            self.bits.push(0);

            self.last_use = 0;
        }
        let mut shifted = if val { 128 } else { 0 };
        shifted >>= self.last_use % 8;
        let bits_len = self.bits.len();
        self.bits[bits_len-1] |= shifted;

        self.last_use += 1;
        self.count += 1;
    }

    pub fn set(&mut self, ix: usize, val: bool) {
        let mask = !(128>>(ix%8));
        let mut shifted = if val { 128 } else { 0 };
        shifted >>= ix%8;
        self.bits[ix/8] &= mask;
        self.bits[ix/8] |= shifted;
    }
}

pub struct BitArrayFileBuilder<W>
where W: 'static+AsyncWrite {
    current_byte: u8,
    current_bit_pos: u8,
    bit_output: W,
    count: u64
}

impl<W> BitArrayFileBuilder<W>
where W: 'static+AsyncWrite {
    pub fn new(output: W) -> BitArrayFileBuilder<W> {
        BitArrayFileBuilder {
            current_byte: 0,
            current_bit_pos: 0,
            bit_output: output,
            count: 0
        }
    }

    fn flush_current(self) -> Box<Future<Item=BitArrayFileBuilder<W>, Error=std::io::Error>> {
        let count = self.count;
        Box::new(tokio::io::write_all(self.bit_output, vec![self.current_byte])
                 .map(move |(w,_)| BitArrayFileBuilder {
                     current_byte: 0,
                     current_bit_pos: 0,
                     bit_output: w,
                     count: count
                 }))
    }

    pub fn push(mut self, bit: bool) -> Box<Future<Item=BitArrayFileBuilder<W>, Error=std::io::Error>> {
        let mut b = match bit { true => 128, false => 0 };
        b >>= self.current_bit_pos;
        self.current_byte |= b;
        self.current_bit_pos += 1;
        self.count += 1;

        if self.current_bit_pos == 8 {
            self.flush_current()
        }
        else {
            Box::new(future::ok(self))
        }
    }

    pub fn push_all<S:'static+Stream<Item=bool,Error=std::io::Error>>(self, stream: S) -> Box<Future<Item=BitArrayFileBuilder<W>,Error=std::io::Error>> {
        Box::new(stream.fold(self, |builder, bit| builder.push(bit)))
    }

    fn pad(self) -> impl Future<Item=W, Error=std::io::Error> {
        write_padding(self.bit_output, (self.count as usize+7)/8, 8)
            .map(|(bit_output,_)| bit_output)
    }

    pub fn finalize(self) -> impl Future<Item=W, Error=std::io::Error> {
        let count = self.count;
        let flush_current: Box<Future<Item=BitArrayFileBuilder<W>,Error=std::io::Error>> =
            if count % 8 == 0 {
                Box::new(future::ok(self))
            } else {
                Box::new(self.flush_current())
            };

        flush_current
            .and_then(|b| b.pad())
            .and_then(move |w| write_u64(w, count))
            .and_then(|w| tokio::io::flush(w))
    }
}

pub struct BitArrayBlockDecoder {
    readahead: Option<u64>
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
            Some(ra) => Ok(Some(ra))
        }
    }
}

pub fn bitarray_stream_blocks<R:'static+AsyncRead>(r: R) -> FramedRead<R, BitArrayBlockDecoder> {
    FramedRead::new(r, BitArrayBlockDecoder { readahead: None })
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::super::storage::*;
    
    #[test]
    pub fn construct_and_parse_small_bitarray() {
        let x = MemoryBackedStore::new();
        let contents = vec![true,true,false,false,true];
        
        let builder = BitArrayFileBuilder::new(x.open_write());
        let _written = builder.push_all(stream::iter_ok(contents))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let loaded = x.map();

        let bitarray = BitArray::from_bits(&loaded);

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
        let _written = builder.push_all(stream::iter_ok(contents))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let loaded = x.map();

        let bitarray = BitArray::from_bits(&loaded);

        for i in 0..bitarray.len() {
            assert_eq!(i % 3 == 0, bitarray.get(i));
        }
    }

    #[test]
    pub fn stream_blocks() {
        let x = MemoryBackedStore::new();
        let contents = (0..).map(|n| n % 4 == 1).take(256);


        let builder = BitArrayFileBuilder::new(x.open_write());
        let _written = builder.push_all(stream::iter_ok(contents))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let stream = bitarray_stream_blocks(x.open_read());

        stream.for_each(|block| Ok(assert_eq!(0x4444444444444444, block))).wait().unwrap();
    }
}
