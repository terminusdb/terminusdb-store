use super::{
    datetime::{datetime_to_storage, storage_to_datetime},
    decimal::{decimal_to_storage, storage_to_decimal},
    integer::{bigint_to_storage, storage_to_bigint},
    TypedDictEntry,
};
use base64::display::Base64Display;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{NaiveDateTime, NaiveTime};
use num_derive::FromPrimitive;
use rug::Integer;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, FromPrimitive, Hash)]
pub enum Datatype {
    String = 0,
    UInt32,
    Int32,
    Float32,
    UInt64,
    Int64,
    Float64,
    Decimal,
    BigInt,
    Boolean,
    LangString,
    AnyURI,
    Language,
    NormalizedString,
    Token,
    NMToken,
    Name,
    NCName,
    Notation,
    QName,
    ID,
    IDRef,
    Entity,
    PositiveInteger,
    NonNegativeInteger,
    NonPositiveInteger,
    NegativeInteger,
    Date,
    DateTime,
    DateTimeStamp,
    Time,
    GYear,
    GMonth,
    GDay,
    GYearMonth,
    GMonthDay,
    Duration,
    YearMonthDuration,
    DayTimeDuration,
    UInt8,
    Int8,
    UInt16,
    Int16,
    Base64Binary,
    HexBinary,
    AnySimpleType,
}

impl Datatype {
    pub fn cast<T: TdbDataType, B: Buf>(self, b: B) -> T {
        if T::datatype() != self {
            panic!("not the right datatype");
        }

        T::from_lexical(b)
    }

    pub fn record_size(&self) -> Option<u8> {
        match self {
            Datatype::Boolean => None,
            Datatype::String => None,
            Datatype::UInt32 => Some(4),
            Datatype::Int32 => Some(4),
            Datatype::UInt64 => Some(8),
            Datatype::Int64 => Some(8),
            Datatype::Float32 => Some(4),
            Datatype::Float64 => Some(8),
            Datatype::Decimal => None,
            Datatype::BigInt => None,
            Datatype::Token => None,
            Datatype::LangString => None,
            _ => None,
        }
    }
}

pub trait TdbDataType: FromLexical<Self> + ToLexical<Self> {
    fn datatype() -> Datatype;

    fn make_entry<T>(val: &T) -> TypedDictEntry
    where
        T: ToLexical<Self> + ?Sized,
    {
        TypedDictEntry::new(Self::datatype(), val.to_lexical().into())
    }
}

pub trait ToLexical<T: ?Sized> {
    fn to_lexical(&self) -> Bytes;
}

pub trait FromLexical<T: ?Sized> {
    fn from_lexical<B: Buf>(b: B) -> Self;
}

impl<T: AsRef<str>> ToLexical<String> for T {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_ref().as_bytes())
    }
}

impl FromLexical<String> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        String::from_utf8(vec).unwrap()
    }
}

impl TdbDataType for String {
    fn datatype() -> Datatype {
        Datatype::String
    }
}

impl TdbDataType for u8 {
    fn datatype() -> Datatype {
        Datatype::UInt8
    }
}

impl FromLexical<u8> for u8 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u8().unwrap()
    }
}

impl ToLexical<u8> for u8 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u8(*self).unwrap();

        buf.into_inner().freeze()
    }
}

impl TdbDataType for u16 {
    fn datatype() -> Datatype {
        Datatype::UInt16
    }
}

impl FromLexical<u16> for u16 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u16::<BigEndian>().unwrap()
    }
}

impl ToLexical<u16> for u16 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u16::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

impl TdbDataType for u32 {
    fn datatype() -> Datatype {
        Datatype::UInt32
    }
}

impl FromLexical<u32> for u32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u32::<BigEndian>().unwrap()
    }
}

impl ToLexical<u32> for u32 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

const I8_BYTE_MASK: u8 = 0b1000_0000;
impl TdbDataType for i8 {
    fn datatype() -> Datatype {
        Datatype::Int8
    }
}

impl FromLexical<i8> for i8 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u8().unwrap();
        (I8_BYTE_MASK ^ i) as i8
    }
}

impl ToLexical<i8> for i8 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I8_BYTE_MASK ^ (*self as u8);
        let mut buf = BytesMut::new().writer();
        buf.write_u8(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const I16_BYTE_MASK: u16 = 0b1000_0000 << 8;
impl TdbDataType for i16 {
    fn datatype() -> Datatype {
        Datatype::Int16
    }
}

impl FromLexical<i16> for i16 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u16::<BigEndian>().unwrap();
        (I16_BYTE_MASK ^ i) as i16
    }
}

impl ToLexical<i16> for i16 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I16_BYTE_MASK ^ (*self as u16);
        let mut buf = BytesMut::new().writer();
        buf.write_u16::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const I32_BYTE_MASK: u32 = 0b1000_0000 << (3 * 8);
impl TdbDataType for i32 {
    fn datatype() -> Datatype {
        Datatype::Int32
    }
}

impl FromLexical<i32> for i32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        (I32_BYTE_MASK ^ i) as i32
    }
}

impl ToLexical<i32> for i32 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I32_BYTE_MASK ^ (*self as u32);
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

impl TdbDataType for u64 {
    fn datatype() -> Datatype {
        Datatype::UInt64
    }
}

impl FromLexical<u64> for u64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u64::<BigEndian>().unwrap()
    }
}

impl ToLexical<u64> for u64 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

const I64_BYTE_MASK: u64 = 0b1000_0000 << (7 * 8);
impl TdbDataType for i64 {
    fn datatype() -> Datatype {
        Datatype::Int64
    }
}

impl FromLexical<i64> for i64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        (I64_BYTE_MASK ^ i) as i64
    }
}

impl ToLexical<i64> for i64 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I64_BYTE_MASK ^ (*self as u64);
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const F32_SIGN_MASK: u32 = 0x8000_0000;
const F32_COMPLEMENT: u32 = 0xffff_ffff;
impl TdbDataType for f32 {
    fn datatype() -> Datatype {
        Datatype::Float32
    }
}

impl FromLexical<f32> for f32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        if i & F32_SIGN_MASK > 0 {
            f32::from_bits(i ^ F32_SIGN_MASK)
        } else {
            f32::from_bits(i ^ F32_COMPLEMENT)
        }
    }
}

impl FromLexical<f32> for f64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        f32::from_lexical(b) as f64
    }
}

impl ToLexical<f32> for f32 {
    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u32 = if f.signum() == -1.0 {
            f.to_bits() ^ F32_COMPLEMENT
        } else {
            f.to_bits() ^ F32_SIGN_MASK
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }
}

const F64_SIGN_MASK: u64 = 0x8000_0000_0000_0000;
const F64_COMPLEMENT: u64 = 0xffff_ffff_ffff_ffff;
impl TdbDataType for f64 {
    fn datatype() -> Datatype {
        Datatype::Float64
    }
}

impl FromLexical<f64> for f64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        if i & F64_SIGN_MASK > 0 {
            f64::from_bits(i ^ F64_SIGN_MASK)
        } else {
            f64::from_bits(i ^ F64_COMPLEMENT)
        }
    }
}

impl ToLexical<f64> for f64 {
    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u64 = if f.signum() == -1.0 {
            f.to_bits() ^ F64_COMPLEMENT
        } else {
            f.to_bits() ^ F64_SIGN_MASK
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }
}

impl TdbDataType for Integer {
    fn datatype() -> Datatype {
        Datatype::BigInt
    }
}

impl FromLexical<Integer> for Integer {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_bigint(&mut b)
    }
}

impl FromLexical<Integer> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        // TODO make this better
        storage_to_bigint(&mut b).to_string()
    }
}

impl ToLexical<Integer> for Integer {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(bigint_to_storage(self.clone()))
    }
}

#[derive(PartialEq, Debug)]
pub struct Decimal(pub String);

impl TdbDataType for Decimal {
    fn datatype() -> Datatype {
        Datatype::Decimal
    }
}

impl FromLexical<Decimal> for Decimal {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        Decimal(storage_to_decimal(&mut b))
    }
}

impl FromLexical<Decimal> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        // TODO make this better
        Decimal::from_lexical(b).0
    }
}

impl ToLexical<Decimal> for Decimal {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(decimal_to_storage(&self.0))
    }
}

impl TdbDataType for bool {
    fn datatype() -> Datatype {
        Datatype::Boolean
    }
}

impl FromLexical<bool> for bool {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let num = b.get_u8();
        num != 0
    }
}

impl ToLexical<bool> for bool {
    fn to_lexical(&self) -> Bytes {
        if *self {
            vec![1].into()
        } else {
            vec![0].into()
        }
    }
}

impl TdbDataType for NaiveDateTime {
    fn datatype() -> Datatype {
        Datatype::DateTime
    }
}

impl ToLexical<NaiveDateTime> for NaiveDateTime {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(datetime_to_storage(self))
    }
}

impl FromLexical<NaiveDateTime> for NaiveDateTime {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_datetime(&mut b)
    }
}

pub struct DateTimeStamp(pub NaiveDateTime);

impl TdbDataType for DateTimeStamp {
    fn datatype() -> Datatype {
        Datatype::DateTimeStamp
    }
}

impl ToLexical<DateTimeStamp> for DateTimeStamp {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(datetime_to_storage(&self.0))
    }
}

impl FromLexical<DateTimeStamp> for DateTimeStamp {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        DateTimeStamp(storage_to_datetime(&mut b))
    }
}

impl FromLexical<DateTimeStamp> for NaiveDateTime {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_datetime(&mut b)
    }
}

impl TdbDataType for NaiveTime {
    fn datatype() -> Datatype {
        Datatype::Time
    }
}

impl ToLexical<NaiveTime> for NaiveTime {
    fn to_lexical(&self) -> Bytes {
        self.to_string().into()
    }
}

impl FromLexical<NaiveTime> for NaiveTime {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        String::from_utf8(vec)
            .unwrap()
            .parse::<NaiveTime>()
            .unwrap()
    }
}

pub struct Date {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub offset: i16,
}

impl TdbDataType for Date {
    fn datatype() -> Datatype {
        Datatype::Date
    }
}

impl ToLexical<Date> for Date {
    fn to_lexical(&self) -> Bytes {
        let year = self.year.to_lexical();
        let month = self.month.to_lexical();
        let day = self.day.to_lexical();
        let offset = self.offset.to_lexical();
        [year, month, day, offset].concat().into()
    }
}

impl FromLexical<Date> for Date {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let year = i64::from_lexical(&mut b);
        let month = u8::from_lexical(&mut b);
        let day = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        Date {
            year,
            month,
            day,
            offset,
        }
    }
}

impl FromLexical<Date> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let date = Date::from_lexical(b);
        let year = date.year;
        let month = date.month;
        let day = date.day;
        let offset = offset_string(date.offset);
        format!("{year:04}-{month:02}-{day:02}{offset:}")
    }
}

pub struct GYear {
    pub year: i64,
    pub offset: i16,
}

impl TdbDataType for GYear {
    fn datatype() -> Datatype {
        Datatype::GYear
    }
}

impl ToLexical<GYear> for GYear {
    fn to_lexical(&self) -> Bytes {
        let year = self.year.to_lexical();
        let offset = self.offset.to_lexical();
        [year, offset].concat().into()
    }
}

impl FromLexical<GYear> for GYear {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let year = i64::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GYear { year, offset }
    }
}

fn offset_string(offset: i16) -> String {
    if offset == 0 {
        "".to_string()
    } else {
        let hours = offset / 60;
        let minutes = offset % 60;
        if hours < 0 {
            format!("-{hours}:{minutes}")
        } else {
            format!("+{hours}:{minutes}")
        }
    }
}

impl FromLexical<GYear> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gyear = GYear::from_lexical(b);
        let year = gyear.year;
        let offset = offset_string(gyear.offset);
        format!("{year:04}{offset:}")
    }
}

pub struct GMonth {
    pub month: u8,
    pub offset: i16,
}

impl TdbDataType for GMonth {
    fn datatype() -> Datatype {
        Datatype::GMonth
    }
}

impl ToLexical<GMonth> for GMonth {
    fn to_lexical(&self) -> Bytes {
        let month = self.month.to_lexical();
        let offset = self.offset.to_lexical();
        [month, offset].concat().into()
    }
}

impl FromLexical<GMonth> for GMonth {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let month = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GMonth { month, offset }
    }
}

impl FromLexical<GMonth> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gmonth = GMonth::from_lexical(b);
        let month = gmonth.month;
        let offset = offset_string(gmonth.offset);
        format!("-{month:02}{offset:}")
    }
}

pub struct GDay {
    pub day: u8,
    pub offset: i16,
}

impl TdbDataType for GDay {
    fn datatype() -> Datatype {
        Datatype::GDay
    }
}

impl ToLexical<GDay> for GDay {
    fn to_lexical(&self) -> Bytes {
        let day = self.day.to_lexical();
        let offset = self.offset.to_lexical();
        [day, offset].concat().into()
    }
}

impl FromLexical<GDay> for GDay {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let day = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GDay { day, offset }
    }
}

impl FromLexical<GDay> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gday = GDay::from_lexical(b);
        let day = gday.day;
        let offset = offset_string(gday.offset);
        format!("--{day:02}{offset:}")
    }
}

pub struct GYearMonth {
    pub year: i64,
    pub month: u8,
    pub offset: i16,
}

impl TdbDataType for GYearMonth {
    fn datatype() -> Datatype {
        Datatype::GYearMonth
    }
}

impl ToLexical<GYearMonth> for GYearMonth {
    fn to_lexical(&self) -> Bytes {
        let year = self.year.to_lexical();
        let month = self.month.to_lexical();
        let offset = self.offset.to_lexical();
        [year, month, offset].concat().into()
    }
}

impl FromLexical<GYearMonth> for GYearMonth {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let year = i64::from_lexical(&mut b);
        let month = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GYearMonth {
            year,
            month,
            offset,
        }
    }
}

impl FromLexical<GYearMonth> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gyearmonth = GYearMonth::from_lexical(b);
        let year = gyearmonth.year;
        let month = gyearmonth.month;
        let offset = offset_string(gyearmonth.offset);
        format!("{year:04}-{month:02}{offset:}")
    }
}

pub struct GMonthDay {
    pub month: u8,
    pub day: u8,
    pub offset: i16,
}

impl TdbDataType for GMonthDay {
    fn datatype() -> Datatype {
        Datatype::GMonthDay
    }
}

impl ToLexical<GMonthDay> for GMonthDay {
    fn to_lexical(&self) -> Bytes {
        let month = self.month.to_lexical();
        let day = self.day.to_lexical();
        let offset = self.offset.to_lexical();
        [month, day, offset].concat().into()
    }
}

impl FromLexical<GMonthDay> for GMonthDay {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let month = u8::from_lexical(&mut b);
        let day = u8::from_lexical(&mut b);
        let offset = i16::from_lexical(b);
        GMonthDay { month, day, offset }
    }
}

impl FromLexical<GMonthDay> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let gmonthday = GMonthDay::from_lexical(b);
        let month = gmonthday.month;
        let day = gmonthday.day;
        let offset = offset_string(gmonthday.offset);
        format!("-{month:02}-{day:02}{offset:}")
    }
}

pub struct Base64Binary(pub Vec<u8>);

impl ToLexical<Base64Binary> for Base64Binary {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(&self.0[..])
    }
}

impl FromLexical<Base64Binary> for Base64Binary {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        Base64Binary(vec)
    }
}

impl FromLexical<Base64Binary> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        let wrapper = Base64Display::with_config(&vec, base64::STANDARD);
        format!("{wrapper}")
    }
}

impl TdbDataType for Base64Binary {
    fn datatype() -> Datatype {
        Datatype::Base64Binary
    }
}

pub struct HexBinary(pub Vec<u8>);

impl ToLexical<HexBinary> for HexBinary {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(&self.0[..])
    }
}

impl FromLexical<HexBinary> for HexBinary {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        HexBinary(vec)
    }
}

impl FromLexical<HexBinary> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        hex::encode(vec)
    }
}

impl TdbDataType for HexBinary {
    fn datatype() -> Datatype {
        Datatype::HexBinary
    }
}

macro_rules! stringy_type {
    ($ty:ident) => {
        stringy_type!($ty, $ty);
    };
    ($ty:ident, $datatype:ident) => {
        #[derive(PartialEq, Debug)]
        pub struct $ty(String);

        impl AsRef<str> for $ty {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl TdbDataType for $ty {
            fn datatype() -> Datatype {
                Datatype::$datatype
            }
        }

        impl<T: AsRef<str>> ToLexical<$ty> for T {
            fn to_lexical(&self) -> Bytes {
                Bytes::copy_from_slice(self.as_ref().as_bytes())
            }
        }

        impl FromLexical<$ty> for $ty {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                let mut vec = vec![0; b.remaining()];
                b.copy_to_slice(&mut vec);
                $ty(String::from_utf8(vec).unwrap())
            }
        }

        impl FromLexical<$ty> for String {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                let mut vec = vec![0; b.remaining()];
                b.copy_to_slice(&mut vec);
                String::from_utf8(vec).unwrap()
            }
        }
    };
}

macro_rules! biginty_type {
    ($ty:ident) => {
        biginty_type!($ty, $ty);
    };
    ($ty:ident, $datatype:ident) => {
        #[derive(PartialEq, Debug)]
        pub struct $ty(pub Integer);

        impl TdbDataType for $ty {
            fn datatype() -> Datatype {
                Datatype::$datatype
            }
        }

        impl FromLexical<$ty> for $ty {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                $ty(storage_to_bigint(&mut b))
            }
        }

        impl FromLexical<$ty> for String {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                storage_to_bigint(&mut b).to_string()
            }
        }

        impl FromLexical<$ty> for Integer {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                storage_to_bigint(&mut b)
            }
        }

        impl ToLexical<$ty> for $ty {
            fn to_lexical(&self) -> Bytes {
                Bytes::from(bigint_to_storage(self.0.clone()))
            }
        }
    };
}

stringy_type!(LangString);
stringy_type!(NCName);
stringy_type!(Name);
stringy_type!(Token);
stringy_type!(NMToken);
stringy_type!(NormalizedString);
stringy_type!(Language);
stringy_type!(AnyURI);
stringy_type!(Notation);
stringy_type!(QName);
stringy_type!(ID);
stringy_type!(IDRef);
stringy_type!(Entity);

stringy_type!(Duration);
stringy_type!(YearMonthDuration);
stringy_type!(DayTimeDuration);

stringy_type!(AnySimpleType);

biginty_type!(PositiveInteger);
biginty_type!(NonNegativeInteger);
biginty_type!(NegativeInteger);
biginty_type!(NonPositiveInteger);
