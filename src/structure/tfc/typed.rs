use crate::structure::{util::calculate_width, LogArrayBufBuilder, MonotonicLogArray, tfc::block::BLOCK_SIZE};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::*;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use rug::Integer;
use std::marker::PhantomData;

use super::{
    block::{IdLookupResult, SizedDictEntry},
    decimal::{decimal_to_storage, storage_to_decimal},
    dict::{build_dict_unchecked, build_offset_logarray, SizedDict},
    integer::{bigint_to_storage, storage_to_bigint},
};

#[derive(Debug)]
pub struct TypedDict {
    types_present: MonotonicLogArray,
    type_offsets: MonotonicLogArray,
    block_offsets: MonotonicLogArray,
    type_id_offsets: Vec<u64>,
    data: Bytes,
}

impl TypedDict {
    pub fn from_parts(types_present: Bytes, type_offsets: Bytes, block_offsets: Bytes, data: Bytes) -> Self {
        let types_present = MonotonicLogArray::parse(types_present).unwrap();
        let type_offsets = MonotonicLogArray::parse(type_offsets).unwrap();
        let block_offsets = MonotonicLogArray::parse(block_offsets).unwrap();

        let mut tally: u64 = 0;
        let mut type_id_offsets = Vec::with_capacity(types_present.len()-1);
        dbg!(&type_offsets);
        for type_offset in type_offsets.iter() {
            let last_block_len;
            if type_offset == 0 {
                last_block_len = data[0];
            }
            else {
                let last_block_offset_of_previous_type = block_offsets.entry(type_offset as usize - 1);
                dbg!(last_block_offset_of_previous_type);
                last_block_len = data[last_block_offset_of_previous_type as usize];
            }
            let gap = BLOCK_SIZE as u8 - last_block_len;
            dbg!(gap);
            tally += gap as u64;
            dbg!(tally);
            type_id_offsets.push((type_offset + 1)*8 - tally);
        }

        dbg!(&type_id_offsets);

        Self {
            types_present,
            type_offsets,
            block_offsets,
            type_id_offsets,
            data,
        }
    }

    pub fn id<T:TdbDataType>(&self, v:&T) -> IdLookupResult {
        let (datatype, bytes) = v.make_entry();

        self.id_slice(datatype, bytes.as_ref())
    }

    pub fn get<T:TdbDataType>(&self, id:u64) -> T {
        let (datatype, slice) = self.entry(id);
        datatype.cast(slice.into_buf())
    }

    fn inner_type_segment(&self, i: usize) -> (SizedDict, u64) {
        dbg!(i);
        let type_offset;
        let block_offset;
        let id_offset;
        if i == 0 {
            type_offset = 0;
            block_offset = 0;
            id_offset = 0;
        }
        else {
            type_offset = self.type_offsets.entry(i-1) as usize;
            id_offset = self.type_id_offsets[type_offset];
            block_offset = self.block_offsets.entry(type_offset as usize) as usize;
        }
        dbg!(type_offset);
        dbg!(block_offset);

        let len;
        if i == self.types_present.len()-1 {
            eprintln!("last type");
            if i == 0 {
                len = self.block_offsets.len() - type_offset;
            }
            else {
                len = self.block_offsets.len() - type_offset - 1;
            }
        }
        else {
            let next_offset = self.type_offsets.entry(i) as usize;
            if i == 0 {
                len = next_offset - type_offset;
            }
            else {
                len = next_offset - type_offset - 1;
            }

        }
        dbg!(len);
        dbg!(self.data.len());

        let logarray_slice = self.block_offsets.slice(type_offset+1, len);
        let data_slice = self.data.slice(block_offset..);
        dbg!(data_slice.len());

        (SizedDict::from_parts(logarray_slice, data_slice, type_offset as u64), id_offset as u64)
    }

    pub fn type_segment(&self, dt: Datatype) -> Option<(SizedDict, u64)> {
        if let Some(i) = self.types_present.index_of(dt as u64) {
            Some(self.inner_type_segment(i))
        } else {
            None
        }
    }

    pub fn id_slice(&self, dt: Datatype, slice: &[u8]) -> IdLookupResult {
        if let Some((dict, offset)) = self.type_segment(dt) {
            dbg!(&dict.data);
            let result = dict.id(slice)
                .offset(offset);

            if offset != 0 {
                result.default(offset)
            }
            else {
                result
            }
        } else {
            IdLookupResult::NotFound
        }
    }

    fn type_index_for_id(&self, id: u64) -> usize {
        for (ix, offset) in self.type_id_offsets.iter().enumerate() {
            if *offset > (id-1) {
                return ix;
            }
        }

        self.type_id_offsets.len()
    }

    fn type_for_type_index(&self, type_index: usize) -> Datatype {
        FromPrimitive::from_u64(self.types_present.entry(type_index)).unwrap()
    }

    pub fn entry(&self, id: u64) -> (Datatype, SizedDictEntry) {
        let type_index = self.type_index_for_id(id);

        let (dict, offset) = self.inner_type_segment(type_index);
        let dt = self.type_for_type_index(type_index);
        (dt, dict.entry(id - offset))
    }
}

pub struct TypedDictSegment<T: TdbDataType> {
    dict: SizedDict,
    _x: PhantomData<T>,
}

impl<T: TdbDataType> TypedDictSegment<T> {
    pub fn parse(offsets: Bytes, data: Bytes, dict_offset: u64) -> Self {
        let dict = SizedDict::parse(offsets, data, dict_offset);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, FromPrimitive)]
pub enum Datatype {
    String = 0,
    UInt32,
    Int32,
    UInt64,
    Int64,
    Float32,
    Float64,
    Decimal,
    BigInt,
}

impl Datatype {
    pub fn cast<T:TdbDataType, B: Buf>(self, b: B) -> T {
        if T::datatype() != self {
            panic!("not the right datatype");
        }

        T::from_lexical(b)
    }
}

pub trait TdbDataType {
    fn datatype() -> Datatype;

    fn to_lexical(&self) -> Bytes;

    fn from_lexical<B: Buf>(b: B) -> Self;

    fn make_entry(&self) -> (Datatype, Bytes) {
        (Self::datatype(), self.to_lexical())
    }
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

impl TdbDataType for u32 {
    fn datatype() -> Datatype {
        Datatype::UInt32
    }

    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }

    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u32::<BigEndian>().unwrap()
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

pub fn build_segment<B: BufMut, T: TdbDataType, I: Iterator<Item = T>>(
    offsets: &mut Vec<u64>,
    data_buf: &mut B,
    iter: I,
) {
    let slices = iter.map(|val| val.to_lexical());

    build_dict_unchecked(0, offsets, data_buf, slices);
}

pub fn build_multiple_segments<
    B1: BufMut,
    B2: BufMut,
    B3: BufMut,
    B4: BufMut,
    R: AsRef<[u8]>,
    I: Iterator<Item = (Datatype, R)>,
>(
    used_types_buf: &mut B1,
    type_offsets_buf: &mut B2,
    block_offsets_buf: &mut B3,
    data_buf: &mut B4,
    iter: I,
) {
    let mut types: Vec<Datatype> = Vec::new();
    let mut type_offsets: Vec<u64> = Vec::new();
    let mut offsets = Vec::with_capacity(iter.size_hint().0);
    for (key, group) in iter.group_by(|v| v.0).into_iter() {
        let start_offset = offsets.last().map(|t| *t).unwrap_or(0_u64);
        let start_type_offset = offsets.len();
        types.push(key);
        type_offsets.push(start_type_offset as u64);
        build_dict_unchecked(start_offset, &mut offsets, data_buf, group.map(|v| v.1));
    }

    build_offset_logarray(block_offsets_buf, offsets);
    eprintln!("types: {types:?}");
    let largest_type = types.last().unwrap();
    let largest_type_offset = type_offsets.last().unwrap();

    let types_width = calculate_width(*largest_type as u64);
    let type_offsets_width = calculate_width(*largest_type_offset);

    let mut types_builder = LogArrayBufBuilder::new(used_types_buf, types_width);
    let mut type_offsets_builder = LogArrayBufBuilder::new(type_offsets_buf, type_offsets_width);

    for t in types {
        types_builder.push(t as u64);
    }

    for o in type_offsets.into_iter().skip(1) {
        type_offsets_builder.push(o - 1);
    }

    types_builder.finalize();
    type_offsets_builder.finalize();
}

#[cfg(test)]
mod tests {
    use crate::structure::{tfc::dict::build_offset_logarray};

    use super::*;

    fn build_segment_and_offsets<B1: BufMut, B2: BufMut, T: TdbDataType, I: Iterator<Item = T>>(
        array_buf: &mut B1,
        data_buf: &mut B2,
        iter: I,
    ) {
        let mut offsets = Vec::new();
        build_segment(&mut offsets, data_buf, iter);
        build_offset_logarray(array_buf, offsets);
    }

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

        build_segment_and_offsets(&mut offsets, &mut data, strings.clone().into_iter());

        let segment = TypedDictSegment::parse(offsets.freeze(), data.freeze(), 0);

        for (ix, s) in strings.into_iter().enumerate() {
            assert_eq!(IdLookupResult::Found((ix+1) as u64), segment.id(&s));
            assert_eq!(s, segment.get((ix+1) as u64));
        }
    }

    #[test]
    fn build_and_parse_u64_dictionary() {
        let nums: Vec<u64> = vec![
            2, 5, 42, 2324, 256463, 256464, 1234567, 803050303, 999999999, 9999999999,
        ];

        let mut offsets = BytesMut::new();
        let mut data = BytesMut::new();

        build_segment_and_offsets(&mut offsets, &mut data, nums.clone().into_iter());

        let segment = TypedDictSegment::parse(offsets.freeze(), data.freeze(), 0);

        for (ix, s) in nums.into_iter().enumerate() {
            assert_eq!(IdLookupResult::Found((ix+1) as u64), segment.id(&s));
            assert_eq!(s, segment.get((ix+1) as u64));
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

    #[test]
    fn test_multi_segment() {
        let mut vec: Vec<(Datatype, Bytes)> = vec![
            Decimal("-1".to_string()).make_entry(),
            "asdf".to_string().make_entry(),
            Decimal("-12342343.2348973".to_string()).make_entry(),
            "Batty".to_string().make_entry(),
            "Batman".to_string().make_entry(),
            (-3_i64).make_entry(),
            Decimal("2348973".to_string()).make_entry(),
            4.389832_f32.make_entry(),
            "apple".to_string().make_entry(),
            23434.389832_f32.make_entry(),
            "apply".to_string().make_entry(),
            (-500_i32).make_entry(),
            20_u32.make_entry(),
        ];
        vec.sort();
        let mut used_types = BytesMut::new();
        let mut type_offsets = BytesMut::new();
        let mut block_offsets = BytesMut::new();
        let mut data = BytesMut::new();
        build_multiple_segments(
            &mut used_types,
            &mut type_offsets,
            &mut block_offsets,
            &mut data,
            vec.clone().into_iter(),
        );
        eprintln!("used_types : {used_types:?}");
        eprintln!("type_offsets : {type_offsets:?}");
        eprintln!("block_offsets : {block_offsets:?}");
        eprintln!("data : {data:?}");

        let dict = TypedDict::from_parts(used_types.freeze(), type_offsets.freeze(), block_offsets.freeze(), data.freeze());
        eprintln!("{dict:?}");

        let id = dict.id(&"Batty".to_string());
        assert_eq!(IdLookupResult::Found(2), id);
        assert_eq!(IdLookupResult::Found(6), dict.id(&20_u32));
        assert_eq!(IdLookupResult::Found(7), dict.id(&(-500_i32)));

        for i in 1..vec.len()+1 {
            eprintln!("!!!!!!!!!!!! {i} {:?}", dict.entry(i as u64));
        }

        assert_eq!(Decimal("-12342343.2348973".to_string()), dict.get(11));

        panic!();
    }
}
