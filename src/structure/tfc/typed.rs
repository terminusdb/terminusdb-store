use crate::structure::MonotonicLogArray;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rug::Integer;
use std::marker::PhantomData;

use super::{
    block::IdLookupResult,
    decimal::{decimal_to_storage, storage_to_decimal},
    dict::{build_dict_unchecked, SizedDict},
    integer::{bigint_to_storage, storage_to_bigint},
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
    UInt32,
    Int64,
    Int32,
    Float32,
    Float64,
    Decimal,
    BigInt,
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

const I64_BYTE_MASK: u64 = 0b1000_0000 << (7 * 8);
impl TdbDataType for i64 {
    fn datatype() -> Datatype {
        Datatype::Int64
    }

    fn to_lexical(&self) -> Bytes {
        let sign_flip = I64_BYTE_MASK ^ (*self as u64);
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }

    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        (I64_BYTE_MASK ^ i) as i64
    }
}

const I32_BYTE_MASK: u32 = 0b1000_0000 << (3 * 8);
impl TdbDataType for i32 {
    fn datatype() -> Datatype {
        Datatype::Int32
    }

    fn to_lexical(&self) -> Bytes {
        let sign_flip = I32_BYTE_MASK ^ (*self as u32);
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }

    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        (I32_BYTE_MASK ^ i) as i32
    }
}

const F32_SIGN_MASK: u32 = 0x8000_0000;
const F32_COMPLEMENT: u32 = 0xffff_ffff;
impl TdbDataType for f32 {
    fn datatype() -> Datatype {
        Datatype::Float32
    }

    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u32;
        if f.signum() == -1.0 {
            g = f.to_bits() ^ F32_COMPLEMENT;
        } else {
            g = f.to_bits() ^ F32_SIGN_MASK;
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }

    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        if i & F32_SIGN_MASK > 0 {
            f32::from_bits(i ^ F32_SIGN_MASK)
        } else {
            f32::from_bits(i ^ F32_COMPLEMENT)
        }
    }
}

const F64_SIGN_MASK: u64 = 0x8000_0000_0000_0000;
const F64_COMPLEMENT: u64 = 0xffff_ffff_ffff_ffff;
impl TdbDataType for f64 {
    fn datatype() -> Datatype {
        Datatype::Float64
    }

    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u64;
        if f.signum() == -1.0 {
            g = f.to_bits() ^ F64_COMPLEMENT;
        } else {
            g = f.to_bits() ^ F64_SIGN_MASK;
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }

    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        if i & F64_SIGN_MASK > 0 {
            f64::from_bits(i ^ F64_SIGN_MASK)
        } else {
            f64::from_bits(i ^ F64_COMPLEMENT)
        }
    }
}

impl TdbDataType for Integer {
    fn datatype() -> Datatype {
        Datatype::Float64
    }

    fn to_lexical(&self) -> Bytes {
        Bytes::from(bigint_to_storage(self.clone()))
    }

    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_bigint(&mut b)
    }
}

#[derive(PartialEq, Debug)]
pub struct Decimal(String);

impl TdbDataType for Decimal {
    fn datatype() -> Datatype {
        Datatype::Decimal
    }

    fn to_lexical(&self) -> Bytes {
        Bytes::from(decimal_to_storage(&self.0))
    }

    fn from_lexical<B: Buf>(mut b: B) -> Self {
        Decimal(storage_to_decimal(&mut b))
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
        let nums: Vec<u64> = vec![
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

    use std::fmt::Debug;

    fn cycle<D>(d: D)
    where
        D: TdbDataType + PartialEq + Debug,
    {
        let j = D::from_lexical(d.to_lexical());
        assert_eq!(d, j)
    }

    #[test]
    fn cycle_i64() {
        cycle(-1_i64);
        cycle(-23423423_i64);
        cycle(0_i64);
        cycle(i64::MAX);
        cycle(i64::MIN);
        cycle(324323_i64);
    }

    #[test]
    fn cycle_i32() {
        cycle(-1_i32);
        cycle(-23423423_i32);
        cycle(0_i32);
        cycle(i32::MAX);
        cycle(i32::MIN);
        cycle(324323_i32);
    }

    #[test]
    fn cycle_f32() {
        cycle(-1_f32);
        cycle(-23423423_f32);
        cycle(0_f32);
        cycle(324323_f32);
        cycle(324323.2343_f32);
        cycle(-324323.2343_f32);
        cycle(f32::MAX);
        cycle(f32::MIN);
        cycle(f32::NEG_INFINITY);
        cycle(f32::INFINITY);

        let j = f32::from_lexical(f32::NAN.to_lexical());
        assert!(j.is_nan())
    }

    #[test]
    fn cycle_f64() {
        cycle(-1_f64);
        cycle(-23423423_f64);
        cycle(0_f64);
        cycle(-0_f64);
        cycle(324323_f64);
        cycle(324323.2343_f64);
        cycle(-324323.2343_f64);
        cycle(f64::MAX);
        cycle(f64::MIN);
        cycle(f64::NEG_INFINITY);
        cycle(f64::INFINITY);

        let j = f64::from_lexical(f64::NAN.to_lexical());
        assert!(j.is_nan())
    }

    fn int(s: &str) -> Integer {
        s.parse::<Integer>().unwrap()
    }

    #[test]
    fn cycle_integer() {
        cycle(int("-1"));
        cycle(int("-12342343"));
        cycle(int("0"));
        cycle(int("234239847938724"));
        cycle(int("983423984793872423423423432312698"));
        cycle(int("-983423984793872423423423432312698"));
    }

    #[test]
    fn cycle_decimal() {
        cycle(Decimal("-1".to_string()));
        cycle(Decimal("-12342343".to_string()));
        cycle(Decimal("0".to_string()));
        cycle(Decimal("-0.1".to_string()));
        cycle(Decimal("-0.0".to_string()));
        cycle(Decimal("-0.1239343".to_string()));
        cycle(Decimal("234239847938724.23423421".to_string()));
        cycle(Decimal("983423984793872423423423432312698".to_string()));
        cycle(Decimal("-983423984793872423423423432312698".to_string()));
    }
}
