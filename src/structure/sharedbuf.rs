use core::ops::{Deref, RangeBounds};
use memmap::Mmap;
use std::fs::File;
use std::io::Error;
use std::sync::Arc;

#[derive(Debug)]
enum Buf {
    Mmap(Mmap),
    Slice(&'static [u8]),
    Vec(Vec<u8>),
}

impl Deref for Buf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        use Buf::*;
        match self {
            Mmap(m) => m.as_ref(),
            Slice(s) => s,
            Vec(v) => v.as_ref(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SharedBuf {
    buf: Arc<Buf>,
    offset: usize,
    len: usize,
}

impl SharedBuf {
    #[inline]
    pub fn new() -> Self {
        SharedBuf::from_vec(Vec::new())
    }

    #[inline]
    fn from_buf(buf: Buf) -> Self {
        let len = buf.len();
        SharedBuf {
            buf: Arc::new(buf),
            offset: 0,
            len,
        }
    }

    #[inline]
    pub fn from_vec(v: Vec<u8>) -> Self {
        SharedBuf::from_buf(Buf::Vec(v))
    }

    #[inline]
    pub fn from_static(s: &'static [u8]) -> Self {
        SharedBuf::from_buf(Buf::Slice(s))
    }

    /// Creates a `SharedBuf` from a slice by copying it.
    pub fn copy_from_slice(data: &[u8]) -> Self {
        SharedBuf::from_vec(data.to_vec())
    }

    #[inline]
    pub unsafe fn from_file_mmap(f: &File) -> Result<Self, Error> {
        Ok(SharedBuf::from_buf(Buf::Mmap(Mmap::map(f)?)))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the `SharedBuf` has a length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.buf[self.offset..self.offset + self.len]
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        use core::ops::Bound::*;

        let begin = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };

        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len,
        };

        assert!(
            begin <= end,
            "SharedBuf::slice: bad range: lower bound ({:?}) > upper bound ({:?})",
            begin,
            end,
        );
        assert!(
            end <= self.len,
            "SharedBuf::slice: bad range: upper bound ({:?}) > length ({:?})",
            end,
            self.len,
        );

        if begin == end {
            return SharedBuf::new();
        }

        let mut clone = self.clone();
        clone.offset += begin;
        clone.len = end - begin;
        clone
    }

    #[inline]
    fn advance_offset(&mut self, index: usize) {
        self.offset += index;
        self.len -= index;
    }

    pub fn split_off(&mut self, index: usize) -> SharedBuf {
        assert!(
            index <= self.len,
            "SharedBuf::split_off: index ({:?}) > length ({:?})",
            index,
            self.len
        );

        if index == self.len {
            return SharedBuf::new();
        }

        if index == 0 {
            return std::mem::replace(self, SharedBuf::new());
        }

        let mut clone = self.clone();
        clone.advance_offset(index);
        self.len = index;
        clone
    }

    pub fn split_to(&mut self, index: usize) -> SharedBuf {
        assert!(
            index <= self.len(),
            "SharedBuf::split_to: index ({:?}) > length ({:?})",
            index,
            self.len()
        );

        if index == self.len() {
            return std::mem::replace(self, SharedBuf::new());
        }

        if index == 0 {
            return SharedBuf::new();
        }

        let mut clone = self.clone();
        self.advance_offset(index);
        clone.len = index;
        clone
    }

    pub fn advance(&mut self, index: usize) {
        assert!(
            index <= self.len(),
            "SharedBuf::advance: index ({:?}) > length ({:?})",
            index,
            self.len(),
        );

        self.advance_offset(index);
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
        }
    }
}

impl Deref for SharedBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for SharedBuf {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

// == PartialEq ==

impl PartialEq for SharedBuf {
    fn eq(&self, other: &SharedBuf) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<[u8]> for SharedBuf {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<str> for SharedBuf {
    fn eq(&self, other: &str) -> bool {
        self.as_slice() == other.as_bytes()
    }
}

impl PartialEq<SharedBuf> for [u8] {
    fn eq(&self, other: &SharedBuf) -> bool {
        *other == *self
    }
}

impl PartialEq<SharedBuf> for &[u8] {
    fn eq(&self, other: &SharedBuf) -> bool {
        *other == *self
    }
}

impl<'a, T: ?Sized> PartialEq<&'a T> for SharedBuf
where
    SharedBuf: PartialEq<T>,
{
    fn eq(&self, other: &&'a T) -> bool {
        *self == **other
    }
}

// == From ==

impl From<&'static [u8]> for SharedBuf {
    fn from(s: &'static [u8]) -> SharedBuf {
        SharedBuf::from_static(s)
    }
}

impl From<&'static str> for SharedBuf {
    fn from(s: &'static str) -> SharedBuf {
        SharedBuf::from_static(s.as_bytes())
    }
}

impl From<String> for SharedBuf {
    fn from(s: String) -> SharedBuf {
        SharedBuf::from(s.into_bytes())
    }
}

impl From<Vec<u8>> for SharedBuf {
    fn from(v: Vec<u8>) -> SharedBuf {
        SharedBuf::from_vec(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LONG: &'static [u8] = b"mary had a little lamb, little lamb, little lamb";
    const SHORT: &'static [u8] = b"hello world";

    #[test]
    fn layout() {
        use std::mem::size_of;
        assert_eq!(size_of::<Buf>(), size_of::<usize>() * 4);
        assert_eq!(size_of::<SharedBuf>(), size_of::<usize>() * 3);
        assert_eq!(size_of::<SharedBuf>(), size_of::<Option<SharedBuf>>());
    }

    #[test]
    fn len() {
        assert_eq!(SharedBuf::from(&b"abcdefg"[..]).len(), 7);
        assert!(SharedBuf::from(&b""[..]).is_empty());
    }

    #[test]
    fn slice() {
        let a = SharedBuf::from(&b"hello world"[..]);
        assert_eq!(a.slice(3..5), b"lo"[..]);
        assert_eq!(a.slice(0..0), b""[..]);
        assert_eq!(a.slice(3..3), b""[..]);
        assert_eq!(a.slice(a.len()..a.len()), b""[..]);
        assert_eq!(a.slice(..5), b"hello"[..]);
        assert_eq!(a.slice(3..), b"lo world"[..]);
    }

    #[test]
    #[should_panic]
    fn slice_panic_1() {
        let a = SharedBuf::from(&b"hello world"[..]);
        a.slice(..13);
    }

    #[test]
    #[should_panic]
    fn slice_panic_2() {
        let a = SharedBuf::from(&b"hello world"[..]);
        a.slice(7..2);
    }

    #[test]
    #[should_panic]
    fn slice_panic_3() {
        let a = SharedBuf::from(&b"hello world"[..]);
        let a = a.slice(..4);
        assert_eq!(a, b"hell"[..]);
        a.slice(..6);
    }

    #[test]
    fn split_off() {
        let mut hello = SharedBuf::from(&b"helloworld"[..]);
        let world = hello.split_off(5);
        assert_eq!(hello, &b"hello"[..]);
        assert_eq!(world, &b"world"[..]);
    }

    #[test]
    #[should_panic]
    fn split_off_panic() {
        let mut hello = SharedBuf::from(&b"helloworld"[..]);
        let _ = hello.split_off(44);
    }

    #[test]
    fn split_off_split_to_loop() {
        let s = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        for i in 0..=s.len() {
            {
                let mut buf = SharedBuf::from(&s[..]);
                let off = buf.split_off(i);
                assert_eq!(i, buf.len());
                let mut sum = Vec::new();
                sum.extend(buf.iter());
                sum.extend(off.iter());
                assert_eq!(&s[..], &sum[..]);
            }
            {
                let mut buf = SharedBuf::from(&s[..]);
                let off = buf.split_to(i);
                assert_eq!(i, off.len());
                let mut sum = Vec::new();
                sum.extend(off.iter());
                sum.extend(buf.iter());
                assert_eq!(&s[..], &sum[..]);
            }
        }
    }
    #[test]
    fn split_to_1() {
        // Static
        let mut a = SharedBuf::from_static(SHORT);
        let b = a.split_to(4);

        assert_eq!(SHORT[4..], a);
        assert_eq!(SHORT[..4], b);

        // Allocated
        let mut a = SharedBuf::copy_from_slice(LONG);
        let b = a.split_to(4);

        assert_eq!(LONG[4..], a);
        assert_eq!(LONG[..4], b);

        let mut a = SharedBuf::copy_from_slice(LONG);
        let b = a.split_to(30);

        assert_eq!(LONG[30..], a);
        assert_eq!(LONG[..30], b);
    }

    #[test]
    fn split_to_2() {
        let mut a = SharedBuf::from(LONG);
        assert_eq!(LONG, a);

        let b = a.split_to(1);

        assert_eq!(LONG[1..], a);
        drop(b);
    }

    #[test]
    #[should_panic]
    fn split_to_panic() {
        let mut hello = SharedBuf::from(&b"helloworld"[..]);
        let _ = hello.split_to(33);
    }

    #[test]
    fn advance_static() {
        let mut a = SharedBuf::from_static(b"hello world");
        a.advance(6);
        assert_eq!(a, &b"world"[..]);
    }

    #[test]
    fn advance_vec() {
        let mut a = SharedBuf::from(b"hello world boooo yah world zomg wat wat".to_vec());
        a.advance(16);
        assert_eq!(a, b"o yah world zomg wat wat"[..]);

        a.advance(4);
        assert_eq!(a, b"h world zomg wat wat"[..]);

        a.advance(6);
        assert_eq!(a, b"d zomg wat wat"[..]);
    }

    #[test]
    #[should_panic]
    fn advance_past_len() {
        let mut a = SharedBuf::from("hello world");
        a.advance(20);
    }

    #[test]
    fn truncate() {
        let s = &b"helloworld"[..];
        let mut hello = SharedBuf::from(s);
        hello.truncate(15);
        assert_eq!(hello, s);
        hello.truncate(10);
        assert_eq!(hello, s);
        hello.truncate(5);
        assert_eq!(hello, "hello");
    }
}
