use std::io::Write;
use futures::prelude::*;
use tokio::io::{write_all,AsyncWrite};
use std::fmt::{self,Debug};
pub struct VByte<'a> {
    packed: &'a [u8]
}

#[derive(Debug)]
pub struct VByteError;

impl<'a> Debug for VByte<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "VByte({})", self.unpack())?;
        Ok(())
    }
}

impl<'a> VByte<'a> {
    pub fn parse(bytes: &[u8]) -> Result<VByte,VByteError> {
        let pos = bytes.iter().position(|&b| b & 0x80 != 0).unwrap();
        // todo error checking, limit to certain amount of bytes that fit in u64
        Ok(VByte { packed: &bytes[..pos+1] })
    }

    pub fn insert(mut num: u64, slice: &mut [u8]) -> Result<VByte,VByteError> {
        let num_bits = 64 - num.leading_zeros();
        let mut remainder = num_bits;
        let mut index = 0;
        while remainder > 7 {
            let byte: u8 = (num & 0x7f) as u8;
            slice[index] = byte;
            index += 1;
            remainder -= 7;
            num >>= 7;
        }

        slice[index] = (0x80 | num) as u8;

        Ok(VByte { packed: &slice[..index+1] })
    }

    pub fn write_sync<W>(num: u64, dest: &mut W) -> Result<(),std::io::Error>
    where W: 'static+Write {
        let mut buf = [0;10];
        let trunc_len = {
            let v = VByte::insert(num, &mut buf).unwrap();
            v.len()
        };

        dest.write_all(&buf[..trunc_len])
    }

    pub fn write<A>(num: u64, dest: A) -> Box<dyn Future<Item=(A,usize),Error=tokio::io::Error>>
    where A: 'static+AsyncWrite {
        let mut buf = vec![0;10];
        let trunc_len = {
            let v = VByte::insert(num, &mut buf).unwrap();
            v.len()
        };
        buf.truncate(trunc_len);

        Box::new(write_all(dest, buf).map(move |(d,_)|(d,trunc_len)))
    }

    pub fn unpack(&self) -> u64 {
        let mut result : u64 = 0;
        let mut shift : u64 = 0;
        for b in self.packed {
            result |= (((b)&0x7f as u8) as u64) << shift;
            if b & 0x80 != 0 {
                break;
            }
            
            shift += 7;
        }

        result
    }

    pub fn len(&self) -> usize {
        self.packed.len()
    }

    pub fn required_len(num: u64) -> usize {
        let used_bits = 64 - num.leading_zeros();
        (used_bits as usize + 6) / 7
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn insert_then_unpack_returns_same_small() {
        let num = 42;
        let mut vec = vec![0;10];

        let vbyte = VByte::insert(num, &mut vec).unwrap();
        assert_eq!(42, vbyte.unpack());
    }

    #[test]
    fn insert_then_unpack_returns_same_medium() {
        let num = 424;
        let mut vec = vec![0;10];

        let vbyte = VByte::insert(num, &mut vec).unwrap();
        assert_eq!(424, vbyte.unpack());
    }

    #[test]
    fn insert_then_unpack_returns_same_large() {
        let num = 42424242;
        let mut vec = vec![0;10];

        let vbyte = VByte::insert(num, &mut vec).unwrap();
        assert_eq!(42424242, vbyte.unpack());
    }

    #[test]
    fn parse_then_unpack_returns_expected_small() {
        let vec: Vec<u8> = vec![0x80|42];
        let vbyte = VByte::parse(&vec).unwrap();
        assert_eq!(42, vbyte.unpack());
    }

    #[test]
    fn parse_then_unpack_returns_expected_medium() {
        let vec: Vec<u8> = vec![0x28,0x80|3];
        let vbyte = VByte::parse(&vec).unwrap();
        assert_eq!(424, vbyte.unpack());
    }

    #[test]
    fn parse_then_unpack_returns_expected_large() {
        let vec: Vec<u8> = vec![0x32,0x2f,0x1d,0x80|0x14];
        let vbyte = VByte::parse(&vec).unwrap();
        assert_eq!(42424242, vbyte.unpack());
    }

    #[test]
    fn write_then_parse_returns_expected() {
        let num = 42424242;
        let mut vec = vec![0;10];
        let cursor = Cursor::new(vec);

        let fut = VByte::write(num, cursor);
        let (c,_length) = fut.wait().unwrap();

        vec = c.into_inner();

        let parsed = VByte::parse(&vec).unwrap();

        assert_eq!(42424242, parsed.unpack());
    }
}
