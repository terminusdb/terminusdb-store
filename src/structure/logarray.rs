use tokio::codec::{FramedRead,Decoder};
use byteorder::{ByteOrder,BigEndian};
use bytes::BytesMut;
use futures::prelude::*;
use futures::future;
use super::util::*;
use super::storage::*;

#[derive(Clone)]
pub struct LogArray<M:AsRef<[u8]>+Clone> {
    len: u32,
    width: u8,
    len_bytes: usize,
    data: M
}

#[derive(Debug)]
pub enum LogArrayError {
    InvalidCoding
}

impl<M:AsRef<[u8]>+Clone> LogArray<M> {
    pub fn parse(data: M) -> Result<LogArray<M>,LogArrayError> {
        let len = BigEndian::read_u32(&data.as_ref()[data.as_ref().len()-8..]);
        let width = data.as_ref()[data.as_ref().len()-4];
        let len_bytes = (len as usize * width as usize + 7) / 8 as usize;

        Ok(LogArray {
            len,
            width,
            len_bytes,
            data
        })
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn len_bytes(&self) -> usize {
        self.len_bytes
    }

    fn nums_for_index(&self, index: usize) -> (u64, u64) {
        let start_bit = self.width as usize * index;
        let start_byte = start_bit / 8;

        let start_u64_offset = start_byte / 8 * 8;

        if start_u64_offset + 16 > self.len_bytes {
            let fragment_size = self.len_bytes - start_u64_offset;
            let mut x = vec![0;16];
            x[..fragment_size].copy_from_slice(&self.data.as_ref()[start_u64_offset..self.len_bytes]);
            
            let n1 = BigEndian::read_u64(&x);
            let n2 = BigEndian::read_u64(&x[8..]);
            (n1,n2)
        }
        else {
            let n1 = BigEndian::read_u64(&self.data.as_ref()[start_u64_offset..self.len_bytes]);
            let n2 = BigEndian::read_u64(&self.data.as_ref()[start_u64_offset+8..self.len_bytes]);
            (n1,n2)
        }
    }

    fn shift_for_index(&self, index:usize) -> i8 {
        64 - self.width as i8 - (index * self.width as usize % 64) as i8
    }

    fn mask_for_index(&self, index:usize) -> (u64, u64) {
        let shift_for_index = self.shift_for_index(index);
        if shift_for_index < 0 {
            let mut m1 = 0xFFFFFFFF;
            let s1 = 64 - (self.width as i8 + shift_for_index) as u8;
            m1 <<= s1;
            m1 >>= s1;

            let mut m2 = 0xFFFFFFFF;
            let s2 = (64 + shift_for_index) as u8;
            m2 >>= s2;
            m2 <<= s2;

            (m1, m2)
        }
        else {
            let mut m = 0xFFFFFFFF;
            m <<= 64 - self.width;
            m >>= 64 - (self.width as i8 + shift_for_index) as u8;
            
            (m, 0)
        }
    }

    pub fn entry(&self, index:usize) -> u64 {
        let (n1,n2) = self.nums_for_index(index);
        let shift_for_index = self.shift_for_index(index);
        if shift_for_index < 0 {
            // crossing an u64 boundary. we need to shift left
            let mut x = n1;
            x <<= 64 - (self.width as i8 + shift_for_index) as u8;
            x >>= 64-self.width; // x contains the first part in the correct position
            let mut y = n2;
            y >>= 64 + shift_for_index;
            x |= y;

            x
        }
        else {
            // no boundaries are crossed. all that matters is n1
            let mut x = n1;
            x <<= 64 - (self.width as i8 + shift_for_index) as u8;
            x >>= 64 - self.width;

            x
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> LogArraySlice<M> {
        if self.len() < offset + length {
            panic!("slice out of bounds");
        }
        LogArraySlice {
            original: self.clone(),
            offset,
            length
        }
    }
}

pub struct LogArraySlice<M:AsRef<[u8]>+Clone> {
    original: LogArray<M>,
    offset: usize,
    length: usize
}

impl<M:AsRef<[u8]>+Clone> LogArraySlice<M> {
    pub fn len(&self) -> usize {
        self.length
    }
    
    pub fn entry(&self, index: usize) -> u64 {
        if index >= self.length {
            panic!("index too large for slice");
        }

        self.original.entry(index+self.offset)
    }
}

#[derive(Debug)]
pub struct LogArrayBuilder {
    len: u32,
    width: u8,
    data: Vec<u8>
}

impl LogArrayBuilder {
    pub fn new(width: u8) -> LogArrayBuilder {
        LogArrayBuilder {
            len: 0,
            width: width,
            data: Vec::new()
        }
    }

    pub fn from_iter<I>(width: u8, col: I) -> LogArrayBuilder
    where I: IntoIterator<Item=u64> {
        let mut builder = LogArrayBuilder::new(width);
        for b in col.into_iter() {
            builder.push(b);
        }

        builder
    }

    pub fn as_logarray(&self) -> LogArray<&[u8]> {
        LogArray {
            len: self.len,
            width: self.width,
            len_bytes: self.data.len(),
            data: &self.data
        }
    }

    pub fn push(&mut self, val: u64) {
        if 64 - val.leading_zeros() as u8 > self.width {
            panic!("val {} too large to fit in width {}", val, self.width);
        }

        let offset = self.width as usize * self.len as usize / 64 * 8;

        self.data.resize(offset+16, 0);
        let (mut n1, mut n2, shift) = {
            let logarray = self.as_logarray();
            let shift = logarray.shift_for_index(self.len as usize);
            let (n1, n2) = logarray.nums_for_index(self.len as usize);
            let (m1, m2) = logarray.mask_for_index(self.len as usize);
            (n1&!m1, n2&!m2, shift)
        };

        if shift < 0 {
            // crossing a boundary
            n1 |= val >> -1 * shift;
            n2 |= val << 64 + shift;
        }
        else {
            n1 |= val << shift;
        }

        BigEndian::write_u64(&mut self.data[offset..], n1);
        BigEndian::write_u64(&mut self.data[offset+8..], n2);

        self.len += 1;
        self.data.truncate((self.width as usize * self.len as usize + 7) / 8);
    }

    pub fn write<W:tokio::io::AsyncWrite>(self, w:W) -> impl Future<Item=(W, usize),Error=std::io::Error> {
        // first, write the number of entries as an u32
        // then, write the width as a single byte
        // write 3 padding bytes
        // with all that out of the way, actually write the logarray.
        let mut header = vec![0u8;8];
        BigEndian::write_u32(&mut header, self.len);
        header[4] = self.width;
        let data = self.data;
        let final_len = (data.len() + 7) / 8 * 8 + 8;
        tokio::io::write_all(w, data)
            .and_then(move |(w, slice)| write_padding(w, slice.len(), 8))
            .and_then(|(w, _)| tokio::io::write_all(w, header))
            .map(move |(w, _)| (w, final_len))
    }
}

/// write a logarray directly to an AsyncWrite
pub struct LogArrayFileBuilder<W:'static+tokio::io::AsyncWrite> {
    file: W,
    width: u8,
    current: u64,
    current_offset: u8,
    count: u32
}

impl<W:'static+tokio::io::AsyncWrite> LogArrayFileBuilder<W> {
    pub fn new(w: W, width: u8) -> LogArrayFileBuilder<W> {
        LogArrayFileBuilder {
            file: w,
            width: width,
            current: 0,
            current_offset: 0,
            count: 0
        }
    }

    pub fn push(mut self, val: u64) -> Box<Future<Item=LogArrayFileBuilder<W>,Error=std::io::Error>> {
        if val.leading_zeros() < 64 - self.width as u32 {
            panic!("value too large for width");
        }

        let mut addition = val << (64 - self.width);
        addition >>= self.current_offset;

        self.current |= addition;
        self.count += 1;

        if self.current_offset + self.width >= 64 {
            // we filled up 64 bits, time to write
            let mut buf = vec![0u8;8];
            BigEndian::write_u64(&mut buf, self.current);

            let new_offset = self.current_offset + self.width - 64;
            let remainder = if new_offset == 0 { 0 } else { val << (64 - new_offset) };
            
            let LogArrayFileBuilder {
                file,
                width,
                count,
                current: _,
                current_offset: _
            } = self;
            
            Box::new(tokio::io::write_all(file, buf)
                     .map(move |(file, _)| LogArrayFileBuilder {
                         file: file,
                         width: width,
                         current: remainder,
                         current_offset: new_offset,
                         count: count
                     }))
        }
        else {
            self.current_offset += self.width;
            Box::new(future::ok(self))
        }
    }

    pub fn push_all<S:Stream<Item=u64,Error=std::io::Error>>(self, vals: S) -> impl Future<Item=LogArrayFileBuilder<W>,Error=std::io::Error> {
        vals.fold(self, |x, val| x.push(val))
    }

    pub fn finalize(self) -> impl Future<Item=W, Error=std::io::Error> {
        let LogArrayFileBuilder {
            file, width, count, current, current_offset: _
        } = self;
        

        let write_last_bits: Box<Future<Item=W, Error=std::io::Error>> = if count % 8 == 0 {
            Box::new(future::ok(file))
        }
        else {
            let mut buf = vec![0u8;8];
            BigEndian::write_u64(&mut buf, current);
            Box::new(tokio::io::write_all(file, buf)
                     .map(|(file,_)|file))
        };

        write_last_bits
            .and_then(move |file| {
                let mut buf = vec![0u8;8];
                BigEndian::write_u32(&mut buf, count);
                buf[4] = width;
                tokio::io::write_all(file, buf)
                     .map(|(file,_)|file)
            })
    }
}

#[derive(Debug)]
pub struct LogArrayDecoder {
    current: u64,
    width: u8,
    offset: u8,
    remaining: u32
}

impl Decoder for LogArrayDecoder {
    type Item = u64;
    type Error = std::io::Error;

    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<u64>, std::io::Error> {
        if self.remaining == 0 {
            // we're out of things to read. All that remains is the footer with the length and things.
            bytes.clear();
            return Ok(None);
        }
        if self.offset + self.width <= 64 {
            // we can just return the first thingie no problem
            let result = (self.current << self.offset) >> (64-self.width);
            self.offset += self.width;
            self.remaining -= 1;
            return Ok(Some(result));
        }

        // it is necessary to read more. Since we store in blocks of 64 bits, it should always be possible to read 64 more bits.
        if bytes.len() < 8 {
            // there's not enough bytes in the buffer yet, read a bit more.
            return Ok(None);
        }

        let current = self.current;
        let fragment_bytes = bytes.split_to(8);
        let fragment = BigEndian::read_u64(&fragment_bytes);
        self.current = fragment;
        self.remaining -= 1;

        if self.offset == 64 {
            // it is possible that we exactly reached the end of the current 64 bit num on the last read.
            // in that case, we start at the beginning of the number just read.
            self.offset = self.width;

            return Ok(Some(fragment>>(64-self.width)));
        }
        else {
            // we've not yet reached the end of our current 64 bit chunk. the current entry is divided over the current and the next chunk.
            let big: u64 = (current << self.offset) >> self.offset;
            let big_len = 64 - self.offset;
            let small_len = self.width - big_len;
            let small: u64 = fragment >> (64 - small_len);

            self.offset = small_len as u8;

            return Ok(Some((big << small_len) + small));
        }
    }
}

pub fn open_logarray_stream<F:'static+FileLoad>(f: F) -> impl Future<Item=Box<'static+Stream<Item=u64,Error=std::io::Error>>,Error=std::io::Error> {
    let end_offset = f.size() - 8;
    // read the length and width
    tokio::io::read_exact(f.open_read_from(end_offset), vec![0;8])
        .map(move |(_,buf)| {
            let len = BigEndian::read_u32(&buf);
            let width = buf[4];

            let b: Box<Stream<Item=u64, Error=std::io::Error>> = Box::new(FramedRead::new(f.open_read(), LogArrayDecoder { current: 0, width, offset: 64, remaining: len }));

            b
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[test]
    fn logarray_can_be_built() {
        let mut foo = LogArrayBuilder::new(3);
        foo.push(3);
        foo.push(4);
        foo.push(2);
        foo.push(1);
        foo.push(7);

        let x = foo.as_logarray();
        assert_eq!(3, x.entry(0));
        assert_eq!(4, x.entry(1));
        assert_eq!(2, x.entry(2));
        assert_eq!(1, x.entry(3));
        assert_eq!(7, x.entry(4));
    }

    #[test]
    fn build_from_vec_works() {
        let l = LogArrayBuilder::from_iter(7, vec![1,3,2,5,12,42,38]);
        assert_eq!(42, l.as_logarray().entry(5));
    }

    #[test]
    fn generate_then_parse_works() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        builder.push_all(stream::iter_ok(vec![1,3,2,5,12,31,18]))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map();

        let logarray = LogArray::parse(&content).unwrap();

        assert_eq!(1, logarray.entry(0));
        assert_eq!(3, logarray.entry(1));
        assert_eq!(2, logarray.entry(2));
        assert_eq!(5, logarray.entry(3));
        assert_eq!(12, logarray.entry(4));
        assert_eq!(31, logarray.entry(5));
        assert_eq!(18, logarray.entry(6));
    }

    #[test]
    fn generate_then_stream_works() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        builder.push_all(stream::iter_ok(0..31))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let entries: Vec<u64> = open_logarray_stream(store)
            .and_then(|s| s.collect())
            .wait()
            .unwrap();

        let expected: Vec<u64> = (0..31).collect();

        assert_eq!(expected, entries);
    }
}
