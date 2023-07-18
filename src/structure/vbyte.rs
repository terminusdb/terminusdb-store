//! A variable-byte encoding implementation for `u64`.
//!
//! The canonical reference for this variable-byte encoding technique appears to be:
//!
//! Hugh E. Williams and Justin Zobel.
//! Compressing integers for fast file access.
//! The Computer Journal, 42:193–201, 1999.
//!
//! There are a number of different implementations for variable-byte encoding. This particular
//! implementation follows the [reference Java implementation] for the RDF HDT project.
//! Another popular implementation is the one for Google's [Protocol Buffers]; however, that
//! differs on where the most significant bit (msb) is set and cleared.
//!
//! [reference Java implementation]: https://github.com/rdfhdt/hdt-java/blob/master/hdt-java-core/src/main/java/org/rdfhdt/hdt/compact/integer/VByte.java
//! [Protocol Buffers]: https://developers.google.com/protocol-buffers/docs/encoding

use futures::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use bytes::Buf;

/// The maximum number of bytes required for any `u64` in a variable-byte encoding.
pub const MAX_ENCODING_LEN: usize = 10;

/// Returns the number of bytes required for a `u64` in its variable-byte encoding.
pub fn encoding_len(num: u64) -> usize {
    match num {
        0 => 1,
        num => ((64 + 6) - num.leading_zeros() as usize) / 7,
    }
}

#[derive(Debug, PartialEq, Error)]
/// An error returned by `decode`.
pub enum DecodeError {
    /// `decode` cannot fit the encoded value into a `u64`.
    #[error("cannot fit the encoded value into a u64")]
    EncodedValueTooLarge,
    /// `decode` did not find the last encoded byte before reaching the end of the buffer.
    #[error("could not find the last encoded byte before reaching the end of the buffer")]
    UnexpectedEndOfBuffer,
    /// `decode` did not find the last encoded byte before reaching the maximum encoding length.
    #[error("could not find the last encoded byte before reaching the maximum encoding length")]
    UnexpectedEncodingLen,
}

#[derive(Debug, Error)]
pub enum DecodeReaderError {
    #[error(transparent)]
    Io(io::Error),
    #[error(transparent)]
    DecodeError(#[from] DecodeError),
}

impl From<io::Error> for DecodeReaderError {
    fn from(value: io::Error) -> Self {
        match value.kind() {
            io::ErrorKind::UnexpectedEof => {
                DecodeReaderError::DecodeError(DecodeError::UnexpectedEndOfBuffer)
            }
            _ => DecodeReaderError::Io(value),
        }
    }
}

/// Returns `true` if the most significant bit (msb) of the byte is set. This indicates the byte is
/// the last of the encoding.
#[inline]
const fn is_last_encoded_byte(byte: u8) -> bool {
    byte >= 0x80
}

/// Mask the byte to ignore the most significant bit (msb).
#[inline]
const fn clear_msb(byte: u8) -> u8 {
    byte & 0x7f
}

/// Set the most significant bit (msb) of the byte.
#[inline]
const fn set_msb(byte: u8) -> u8 {
    byte | 0x80
}

/// Returns `true` if `shift` indicates we're at the maximum possible byte of the encoding and if
/// that byte value is too large for the result to fit into the unsigned 64-bit value.
#[inline]
fn max_byte_too_large(shift: u32, byte: u8) -> bool {
    shift == 63 && byte > 0x81
}

/// Decodes a `u64` from a variable-byte-encoded slice.
///
/// On success, this function returns `Ok` with the decoded value and encoding length. Otherwise,
/// the slice data is invalid, and the function returns `Err` with the corresponding `DecodeError`
/// giving the reason.
///
/// This function expects the encoded value to start at the beginning of the slice; and the slice
/// must be large enough to include all of the encoded bytes of one value. Decoding stops at the
/// end of the encoded value, so it doesn't matter if the slice is longer.
pub fn decode(mut buf: &[u8]) -> Result<(u64, usize), DecodeError> {
    decode_buf(&mut buf)
}

/// Decodes a `u64` from a variable-byte-encoded slice.
///
/// On success, this function returns `Ok` with the decoded value and encoding length. Otherwise,
/// the slice data is invalid, and the function returns `Err` with the corresponding `DecodeError`
/// giving the reason.
///
/// This function expects the encoded value to start at the beginning of the slice; and the slice
/// must be large enough to include all of the encoded bytes of one value. Decoding stops at the
/// end of the encoded value, so it doesn't matter if the slice is longer.
pub fn decode_buf<B: Buf>(buf: &mut B) -> Result<(u64, usize), DecodeError> {
    // This will be the decoded result.
    let mut num: u64 = 0;
    // This is how many bits we shift `num` by on each iteration in increments of 7.
    let mut shift: u32 = 0;
    // Loop through each 8-bit byte value with its index.
    let mut count = 0;
    loop {
        if !buf.has_remaining() {
            return Err(DecodeError::UnexpectedEndOfBuffer);
        }

        let b = buf.get_u8();
        count += 1;

        if is_last_encoded_byte(b) {
            return if max_byte_too_large(shift, b) {
                Err(DecodeError::EncodedValueTooLarge)
            } else {
                // Return the result (clearing the msb) and the encoding length.
                Ok((num | ((clear_msb(b) as u64) << shift), count))
            };
        }
        // This is not the last byte. Update the result.
        num |= (b as u64) << shift;
        // Increment the shift amount for the next 7 bits.
        shift += 7;
        // Stop if we are about to exceed the maximum encoding length.
        if shift > 64 {
            // We have reached the maximum encoding length without encountering the last encoded
            // byte.
            return Err(DecodeError::UnexpectedEncodingLen);
        }
    }
}

/// Decodes a `u64` from a variable-byte-encoded slice.
///
/// On success, this function returns `Ok` with the decoded value and encoding length. Otherwise,
/// the slice data is invalid, and the function returns `Err` with the corresponding `DecodeError`
/// giving the reason.
///
/// This function expects the encoded value to start at the beginning of the slice; and the slice
/// must be large enough to include all of the encoded bytes of one value. Decoding stops at the
/// end of the encoded value, so it doesn't matter if the slice is longer.
pub async fn decode_reader<R: AsyncRead + Unpin>(
    mut reader: R,
) -> Result<(u64, usize), DecodeReaderError> {
    // This will be the decoded result.
    let mut num: u64 = 0;
    // This is how many bits we shift `num` by on each iteration in increments of 7.
    let mut shift: u32 = 0;
    // Loop through each 8-bit byte value with its index.
    let mut count = 0;
    loop {
        let b = reader.read_u8().await?;
        count += 1;

        if is_last_encoded_byte(b) {
            return if max_byte_too_large(shift, b) {
                Err(DecodeError::EncodedValueTooLarge.into())
            } else {
                // Return the result (clearing the msb) and the encoding length.
                Ok((num | ((clear_msb(b) as u64) << shift), count))
            };
        }
        // This is not the last byte. Update the result.
        num |= (b as u64) << shift;
        // Increment the shift amount for the next 7 bits.
        shift += 7;
        // Stop if we are about to exceed the maximum encoding length.
        if shift > 64 {
            // We have reached the maximum encoding length without encountering the last encoded
            // byte.
            return Err(DecodeError::UnexpectedEncodingLen.into());
        }
    }
}

/// Returns `true` if more than 7 bits remain to be encoded.
#[inline]
const fn more_than_7bits_remain(num: u64) -> bool {
    num >= 0x80
}

/// Encodes a `u64` by writing its variable-byte encoding to a slice.
///
/// Returns the encoding length.
///
/// This function does not ensure that `buf` is large enough to include the encoding length of the
/// number. In particular, there are no bounds checks on indexing. The caller of this function must
/// ensure that `buf` is large enough for the encoded `num`. This can be done, for example, by
/// using `MAX_ENCODING_LEN` to create the `buf` or by using `encoding_len` to validate the length
/// of `buf`.
unsafe fn encode_unchecked(buf: &mut [u8], mut num: u64) -> usize {
    // Initialize the buffer index. This will be used for the encoding length at the end.
    let mut i = 0;
    // Loop through all 7-bit strings of the number.
    while more_than_7bits_remain(num) {
        // This is not the last encoded byte.
        *buf.get_unchecked_mut(i) = clear_msb(num as u8);
        // Get the next 7 bits.
        num >>= 7;
        // Increment the index.
        i += 1;
    }
    // This is the last encoded byte.
    *buf.get_unchecked_mut(i) = set_msb(num as u8);
    // Return the encoding length.
    i + 1
}

/// Encodes a `u64` by writing its variable-byte encoding to a slice.
///
/// On success, this function returns `Some` encoding length. Otherwise, the target slice is not
/// large enough, and the function returns `None`.
pub fn encode_slice(buf: &mut [u8], num: u64) -> Option<usize> {
    // Validate the length of the buffer.
    if encoding_len(num) > buf.len() {
        None
    } else {
        // Safety: We have verified that `buf.len()` is large enough to hold the encoded bytes of
        // `num`.
        unsafe { Some(encode_unchecked(buf, num)) }
    }
}

/// Encodes a `u64` with a variable-byte encoding in a `Vec`.
///
/// The length of the resultant `Vec` is the encoding length of `num`.
pub fn encode_vec(num: u64) -> Vec<u8> {
    // Allocate a `Vec` of the right size.
    let mut vec = vec![0; encoding_len(num)];
    // Safety: We have created `vec` with the length of the encoded bytes of `num`.
    unsafe { encode_unchecked(&mut vec, num) };
    vec
}

/// Encodes a `u64` with a variable-byte encoding in an array.
///
/// The array is always length 10. Additinally, the actual size of the vbyte is returned.
pub fn encode_array(num: u64) -> ([u8; 10], usize) {
    // Allocate a `Vec` of the right size.
    let mut buf = [0; 10];
    // Safety: We have created `vec` with the length of the encoded bytes of `num`.
    let size = unsafe { encode_unchecked(&mut buf, num) };
    (buf, size)
}

/*
pub fn encode_into_writer<W:Write>(writer: &mut W, mut num: u64) -> std::io::Result<usize> {
    let mut i = 0;
    // Loop through all 7-bit strings of the number.
    while more_than_7bits_remain(num) {
        // This is not the last encoded byte.
        let b = clear_msb(num as u8);
        writer.write_u8(b)?;
        // Get the next 7 bits.
        num >>= 7;
        i+=1;
    }
    // This is the last encoded byte.
    let b = set_msb(num as u8);
    // Return the encoding length.
    writer.write_u8(b)?;
    Ok(i + 1)
}
*/

/// Encodes a `u64` with a variable-byte encoding in a `Vec` and writes that `Vec` to the
/// destination `dest` in a future.
pub async fn write_async<A>(dest: &mut A, num: u64) -> io::Result<usize>
where
    A: 'static + AsyncWrite + Unpin + Send,
{
    let vec = encode_vec(num);
    let len = vec.len();
    dest.write_all(&vec).await?;

    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_decode_success(buf: &mut [u8], expected: &[u8], num: u64) {
        assert_eq!(Some(expected.len()), encode_slice(buf, num));
        assert_eq!(expected, buf);
        let (n, len) = decode(&buf).unwrap();
        assert_eq!(num, n);
        assert_eq!(expected.len(), len);
    }

    #[test]
    fn encode_decode_1_byte() {
        let mut buf = [0; 1];
        encode_decode_success(&mut buf, &[set_msb(0)], 0);
        encode_decode_success(&mut buf, &[set_msb(1)], 1);
        encode_decode_success(&mut buf, &[set_msb(42)], 42);
    }

    #[test]
    fn encode_decode_2_bytes() {
        let mut buf = [0; 2];
        encode_decode_success(&mut buf, &[0b0101000, set_msb(0b1)], 0b1_0101000);
        let expected = [0b0000000, set_msb(0b1111111)];
        encode_decode_success(&mut buf, &expected, 0b1111111_0000000);
    }

    #[test]
    fn encode_decode_4_bytes() {
        let mut buf = [0; 4];
        let expected = [0b0110010, 0b0101111, 0b0011101, set_msb(0b10100)];
        encode_decode_success(&mut buf, &expected, 0b10100_0011101_0101111_0110010);
    }

    #[test]
    fn encode_decode_7_bytes() {
        let mut buf = [0; 7];
        let expected = [
            0b0100000,
            0b0010010,
            0b1010101,
            0b1001010,
            0b1001000,
            0b1000110,
            set_msb(0b1100001),
        ];
        let num = 0b1100001_1000110_1001000_1001010_1010101_0010010_0100000;
        encode_decode_success(&mut buf, &expected, num);
    }

    #[test]
    fn encode_decode_max_bytes() {
        let mut buf = [0; MAX_ENCODING_LEN];
        let mut expected = [0x7f; 10];
        assert_eq!(Err(DecodeError::UnexpectedEncodingLen), decode(&buf));
        expected[9] = set_msb(0x01);
        encode_decode_success(&mut buf, &expected, u64::max_value());
        expected[0] -= 1;
        encode_decode_success(&mut buf, &expected, u64::max_value() - 1);
    }

    #[test]
    fn encode_decode_4_bytes_in_20_bytes() {
        let mut buf = [0; 20];
        assert_eq!(Some(4), encode_slice(&mut buf, 194984659));
        let (n, len) = decode(&buf).unwrap();
        assert_eq!(194984659, n);
        assert_eq!(4, len);
    }

    #[test]
    fn encode_0_bytes_fails() {
        let mut buf = [];
        assert_eq!(None, encode_slice(&mut buf, 0));
    }

    #[test]
    fn encode_4_bytes_to_3_bytes_fails() {
        let mut buf = [0; 3];
        let num = 0b1011100_1111100_1110101_1010011;
        assert_eq!(None, encode_slice(&mut buf, num));
    }

    #[test]
    fn decode_0_bytes_fails() {
        let buf = [];
        assert_eq!(Err(DecodeError::UnexpectedEndOfBuffer), decode(&buf));
    }

    #[test]
    fn decode_1_byte_without_msb_fails() {
        let buf = [0b0001000];
        assert_eq!(Err(DecodeError::UnexpectedEndOfBuffer), decode(&buf));
    }

    #[test]
    fn encoded_value_too_large_fails() {
        let mut buf = [0; 10];
        buf[9] = set_msb(0x02);
        assert_eq!(Err(DecodeError::EncodedValueTooLarge), decode(&buf));
    }

    #[test]
    fn decode_11_bytes_fails() {
        let mut buf = [0; 11];
        buf[10] = set_msb(0x01);
        assert_eq!(Err(DecodeError::UnexpectedEncodingLen), decode(&buf));
    }

    #[test]
    fn encoded_len_tests() {
        for &(len, num) in &[
            (1, 0),
            (1, 1),
            (2, 0b11_0101000),
            (7, 0b1100001_1000110_1001000_1001010_1010101_0010010_0100000),
            (10, u64::max_value() - 1),
            (MAX_ENCODING_LEN, u64::max_value()),
        ] {
            assert_eq!(len, encoding_len(num));
        }
    }
}
