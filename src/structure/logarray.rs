use tokio::codec::{FramedRead,Decoder};
use byteorder::{ByteOrder,BigEndian};
use bytes::BytesMut;
use futures::prelude::*;
use futures::future;
use super::storage::*;
use std::cmp::Ordering;

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

pub struct LogArrayIterator<'a, M:AsRef<[u8]>+Clone> {
    logarray: &'a LogArray<M>,
    pos: usize,
    end: usize
}

impl<'a, M:AsRef<[u8]>+Clone> Iterator for LogArrayIterator<'a, M> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.pos == self.end {
            None
        }
        else {
            let result = self.logarray.entry(self.pos);
            self.pos += 1;

            Some(result)
        }
    }
}

pub struct OwnedLogArrayIterator<M:AsRef<[u8]>+Clone> {
    logarray: LogArray<M>,
    pos: usize,
    end: usize
}

impl<M:AsRef<[u8]>+Clone> Iterator for OwnedLogArrayIterator<M> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.pos == self.end {
            None
        }
        else {
            let result = self.logarray.entry(self.pos);
            self.pos += 1;

            Some(result)
        }
    }
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

    pub fn iter(&self) -> LogArrayIterator<M> {
        LogArrayIterator {
            logarray: self,
            pos: 0,
            end: self.len()
        }
    }

    pub fn into_iter(self) -> OwnedLogArrayIterator<M> {
        OwnedLogArrayIterator {
            end: self.len(),
            logarray: self,
            pos: 0,
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

#[derive(Clone)]
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

    pub fn iter(&self) -> LogArrayIterator<M> {
        LogArrayIterator {
            logarray: &self.original,
            pos: self.offset,
            end: self.offset + self.length
        }
    }

    pub fn into_iter(self) -> OwnedLogArrayIterator<M> {
        OwnedLogArrayIterator {
            pos: self.offset,
            end: self.offset + self.length,
            logarray: self.original,
        }
    }
}

/// write a logarray directly to an AsyncWrite
pub struct LogArrayFileBuilder<W:'static+tokio::io::AsyncWrite> {
    file: W,
    width: u8,
    current: u64,
    current_offset: u8,
    pub count: u32
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

    pub fn push(mut self, val: u64) -> Box<dyn Future<Item=LogArrayFileBuilder<W>,Error=std::io::Error>> {
        if val.leading_zeros() < 64 - self.width as u32 {
            panic!("value {} too large for width {}", val, self.width);
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
        

        let write_last_bits: Box<dyn Future<Item=W, Error=std::io::Error>> = if count % 8 == 0 {
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

pub fn open_logarray_stream<F:'static+FileLoad>(f: F) -> impl Future<Item=Box<dyn 'static+Stream<Item=u64,Error=std::io::Error>>,Error=std::io::Error> {
    let end_offset = f.size() - 8;
    // read the length and width
    tokio::io::read_exact(f.open_read_from(end_offset), vec![0;8])
        .map(move |(_,buf)| {
            let len = BigEndian::read_u32(&buf);
            let width = buf[4];

            let b: Box<dyn Stream<Item=u64, Error=std::io::Error>> = Box::new(FramedRead::new(f.open_read(), LogArrayDecoder { current: 0, width, offset: 64, remaining: len }));

            b
        })
}

#[derive(Clone)]
pub struct MonotonicLogArray<M:AsRef<[u8]>+Clone>(LogArray<M>);

impl<M:AsRef<[u8]>+Clone> MonotonicLogArray<M> {
    pub fn from_logarray(logarray: LogArray<M>) -> MonotonicLogArray<M> {
        if cfg!(debug_assertions) {
            let mut iter = logarray.iter();
            let first = iter.next();
            if first.is_some() {
                // check if this is actually monotonic
                let mut prev = first.unwrap();
                for cur in iter {
                    if cur <= prev {
                        panic!("logarray not monotonic ({} is smaller than or equal to its predecessor {})", cur, prev);
                    }
                }
            }
        }

        MonotonicLogArray(logarray)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn entry(&self, index:usize) -> u64 {
        self.0.entry(index)
    }

    pub fn iter(&self) -> LogArrayIterator<M> {
        self.0.iter()
    }

    pub fn into_iter(self) -> OwnedLogArrayIterator<M> {
        self.0.into_iter()
    }

    pub fn index_of(&self, element: u64) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }

        let mut min = 0;
        let mut max = self.len() - 1;
        while min <= max {
            let mid = (min + max) / 2;
            match element.cmp(&self.entry(mid)) {
                Ordering::Equal => return Some(mid),
                Ordering::Greater => min = mid+1,
                Ordering::Less => {
                    if mid == 0 {
                        return None;
                    }
                    max = mid - 1
                }
            }
        }

        None
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

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

    #[test]
    fn iterate_over_logarray() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1,3,2,5,12,31,18];
        builder.push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map();

        let logarray = LogArray::parse(&content).unwrap();

        let result: Vec<u64> = logarray.iter().collect();

        assert_eq!(original, result);
    }

    #[test]
    fn owned_iterate_over_logarray() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1,3,2,5,12,31,18];
        builder.push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map();

        let logarray = LogArray::parse(&content).unwrap();

        let result: Vec<u64> = logarray.into_iter().collect();

        assert_eq!(original, result);
    }

    #[test]
    fn iterate_over_logarray_slice() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1,3,2,5,12,31,18];
        builder.push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map();

        let logarray = LogArray::parse(&content).unwrap();
        let slice = logarray.slice(2,3);

        let result: Vec<u64> = slice.iter().collect();

        assert_eq!(vec![2,5,12], result);
    }

    #[test]
    fn owned_iterate_over_logarray_slice() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1,3,2,5,12,31,18];
        builder.push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map();

        let logarray = LogArray::parse(&content).unwrap();
        let slice = logarray.slice(2,3);

        let result: Vec<u64> = slice.into_iter().collect();

        assert_eq!(vec![2,5,12], result);
    }

    #[test]
    fn monotonic_logarray_index_lookup() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1,3,5,6,7,10,11,15,16,18,20,25,31];
        builder.push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map();

        let logarray = LogArray::parse(&content).unwrap();
        let monotonic = MonotonicLogArray::from_logarray(logarray);

        for i in 0..original.len() {
            assert_eq!(i, monotonic.index_of(original[i]).unwrap());
        }

        assert_eq!(None, monotonic.index_of(12));
    }
}
