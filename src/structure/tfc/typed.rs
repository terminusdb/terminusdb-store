use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::marker::PhantomData;

use crate::structure::MonotonicLogArray;

use super::{
    block::IdLookupResult,
    dict::{build_dict_unchecked, SizedDict},
};

pub struct TypedDict {
    types_present: MonotonicLogArray,
    type_offsets: Option<MonotonicLogArray>,
    data: Bytes,
}

pub struct TypedDictSegment<T: TdbDataType> {
    dict: SizedDict,
    _x: PhantomData<T>,
}

impl<T: TdbDataType> TypedDictSegment<T> {
    pub fn from_parts(offsets: Bytes, data: Bytes) -> Self {
        let dict = SizedDict::from_parts(offsets, data);
        Self {
            dict,
            _x: Default::default(),
        }
    }

    pub fn get(&self, index: u64) -> T {
        let entry = self.dict.entry(index);
        T::from_lexical(entry.into_buf())
    }

    pub fn id(&self, val: &T) -> IdLookupResult {
        let slice = val.to_lexical();
        self.dict.id(&slice[..])
    }
}

pub enum Datatype {
    String,
    UInt64,
}

pub trait TdbDataType {
    fn datatype() -> Datatype;

    fn to_lexical(&self) -> Bytes;

    fn from_lexical<B: Buf>(b: B) -> Self;
}

impl TdbDataType for String {
    fn datatype() -> Datatype {
        Datatype::String
    }

    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }

    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        String::from_utf8(vec).unwrap()
    }
}

impl TdbDataType for u64 {
    fn datatype() -> Datatype {
        Datatype::UInt64
    }

    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }

    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u64::<BigEndian>().unwrap()
    }
}

pub fn build_segment<B1: BufMut, B2: BufMut, T: TdbDataType, I: Iterator<Item = T>>(
    array_buf: &mut B1,
    data_buf: &mut B2,
    iter: I,
) {
    let slices = iter.map(|val| val.to_lexical());

    build_dict_unchecked(array_buf, data_buf, slices);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_parse_string_dictionary() {
        let strings: Vec<_> = [
            "aaaaaaaa",
            "bbbbbbbb",
            "bbbcccdaaaa",
            "f",
            "fafasdfas",
            "gafovp",
            "gdfasfa",
            "gdfbbbbbb",
            "hello",
            "iguana",
            "illusion",
            "illustrated",
            "jetengine",
            "jetplane",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        let mut offsets = BytesMut::new();
        let mut data = BytesMut::new();

        build_segment(&mut offsets, &mut data, strings.clone().into_iter());

        let segment = TypedDictSegment::from_parts(offsets.freeze(), data.freeze());

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(IdLookupResult::Found(ix as u64), segment.id(&s));
            assert_eq!(s, segment.get(ix as u64));
        }
    }

    #[test]
    fn build_and_parse_u64_dictionary() {
        let nums: Vec<_> = vec![
            2, 5, 42, 2324, 256463, 256464, 1234567, 803050303, 999999999, 9999999999,
        ];

        let mut offsets = BytesMut::new();
        let mut data = BytesMut::new();

        build_segment(&mut offsets, &mut data, nums.clone().into_iter());

        let segment = TypedDictSegment::from_parts(offsets.freeze(), data.freeze());

        for (ix, s) in nums.into_iter().enumerate() {
            assert_eq!(IdLookupResult::Found(ix as u64), segment.id(&s));
            assert_eq!(s, segment.get(ix as u64));
        }
    }
}
