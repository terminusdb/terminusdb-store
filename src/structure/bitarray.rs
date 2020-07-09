#![allow(clippy::precedence, clippy::verbose_bit_mask)]

//! Code for reading, writing, and using bit arrays.
//!
//! A bit array is a contiguous sequence of N bits contained in L words. By choosing L as the
//! minimal number of words required for N bits, the sequence is compressed and yet aligned on a
//! word boundary.
//!
//! # Notes
//!
//! * All words are stored in a standard big-endian encoding.
//! * The maximum number of bits is 2^64-1.
//!
//! # Naming
//!
//! Because of the ambiguity of the English language and the possibility to confuse the meanings of
//! the words used to describe aspects of this code, we try to use the following definitions
//! consistently throughout:
//!
//! * buffer: a contiguous sequence of bytes
//!
//! * size: the number of bytes in a buffer
//!
//! * word: a 64-bit contiguous sequence aligned on 8-byte boundaries starting at the beginning of
//!     the input buffer
//!
//! * index: the logical address of a bit in the data buffer.
//!
//! * length: the number of usable bits in the bit array

use super::util;
use crate::storage::*;
use crate::structure::bititer::BitIter;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use std::{convert::TryFrom, error, fmt, io};
use tokio::{
    codec::{Decoder, FramedRead},
    prelude::*,
};

/// A thread-safe, reference-counted, compressed bit sequence.
///
/// A `BitArray` is a wrapper around a [`Bytes`] that provides a view of the underlying data as a
/// compressed sequence of bits.
///
/// [`Bytes`]: ../../../bytes/struct.Bytes.html
///
/// As with other types in [`structures`], a `BitArray` is created from an existing buffer, rather
/// than constructed from parts. The buffer may be read from a file or other source and may be very
/// large. A `BitArray` preserves the buffer to save memory but provides a simple abstraction of
/// being a vector of `bool`s.
///
/// [`structures`]: ../index.html
#[derive(Clone)]
pub struct BitArray {
    /// Number of usable bits in the array.
    len: u64,

    /// Shared reference to the buffer containing the sequence of bits.
    ///
    /// The buffer does not contain the control word.
    buf: Bytes,
}

/// An error that occurred during a bit array operation.
#[derive(Debug, PartialEq)]
pub enum BitArrayError {
    InputBufferTooSmall(usize),
    UnexpectedInputBufferSize(u64, u64, u64),
}

impl BitArrayError {
    /// Validate the input buffer size.
    ///
    /// It must have at least the control word.
    fn validate_input_buf_size(input_buf_size: usize) -> Result<(), Self> {
        if input_buf_size < 8 {
            return Err(BitArrayError::InputBufferTooSmall(input_buf_size));
        }
        Ok(())
    }

    /// Validate the length.
    ///
    /// The input buffer size should be the appropriate multiple of 8 to include the number of bits
    /// plus the control word.
    fn validate_len(input_buf_size: usize, len: u64) -> Result<(), Self> {
        // Calculate the expected input buffer size. This includes the control word.
        let expected_buf_size = {
            // The following steps are necessary to avoid overflow. If we add first and shift
            // second, the addition might result in a value greater than `u64::max_value()`.
            // Therefore, we right-shift first to produce a value that cannot overflow, check how
            // much we need to add, and add it.
            let after_shifting = len >> 6 << 3;
            if len & 63 == 0 {
                // The number of bits fit evenly into 64-bit words. Add only the control word.
                after_shifting + 8
            } else {
                // The number of bits do not fit evenly into 64-bit words. Add a word for the
                // leftovers plus the control word.
                after_shifting + 16
            }
        };
        let input_buf_size = u64::try_from(input_buf_size).unwrap();

        if input_buf_size != expected_buf_size {
            return Err(BitArrayError::UnexpectedInputBufferSize(
                input_buf_size,
                expected_buf_size,
                len,
            ));
        }

        Ok(())
    }
}

impl fmt::Display for BitArrayError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use BitArrayError::*;
        match self {
            InputBufferTooSmall(input_buf_size) => {
                write!(f, "expected input buffer size ({}) >= 8", input_buf_size)
            }
            UnexpectedInputBufferSize(input_buf_size, expected_buf_size, len) => write!(
                f,
                "expected input buffer size ({}) to be {} for {} bits",
                input_buf_size, expected_buf_size, len
            ),
        }
    }
}

impl error::Error for BitArrayError {}

impl From<BitArrayError> for io::Error {
    fn from(err: BitArrayError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

/// Read the length from the control word buffer. `buf` must start at the first word after the data
/// buffer. `input_buf_size` is used for validation.
fn read_control_word(buf: &[u8], input_buf_size: usize) -> Result<u64, BitArrayError> {
    let len = BigEndian::read_u64(buf);
    BitArrayError::validate_len(input_buf_size, len)?;
    Ok(len)
}

impl BitArray {
    /// Construct a `BitArray` by parsing a `Bytes` buffer.
    pub fn from_bits(mut buf: Bytes) -> Result<BitArray, BitArrayError> {
        let input_buf_size = buf.len();
        BitArrayError::validate_input_buf_size(input_buf_size)?;

        let len = read_control_word(&buf.split_off(input_buf_size - 8), input_buf_size)?;

        Ok(BitArray { buf, len })
    }

    /// Returns a reference to the buffer slice.
    pub fn bits(&self) -> &[u8] {
        &self.buf
    }

    /// Returns the number of usable bits in the bit array.
    pub fn len(&self) -> usize {
        usize::try_from(self.len).unwrap_or_else(|_| {
            panic!(
                "expected length ({}) to fit in {} bytes",
                self.len,
                std::mem::size_of::<usize>()
            )
        })
    }

    /// Returns `true` if there are no usable bits.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reads the data buffer and returns the logical value of the bit at the bit `index`.
    ///
    /// Panics if `index` is >= the length of the bit array.
    pub fn get(&self, index: usize) -> bool {
        let len = self.len();
        assert!(index < len, "expected index ({}) < length ({})", index, len);

        let byte = self.buf[index / 8];
        let mask = 0b1000_0000 >> index % 8;

        byte & mask != 0
    }
}

pub struct BitArrayFileBuilder<W> {
    /// Destination of the bit array data.
    dest: W,
    /// Storage for the next word to be written.
    current: u64,
    /// Number of bits written to the buffer
    count: u64,
}

impl<W: AsyncWrite> BitArrayFileBuilder<W> {
    pub fn new(dest: W) -> BitArrayFileBuilder<W> {
        BitArrayFileBuilder {
            dest,
            current: 0,
            count: 0,
        }
    }

    pub fn push(self, bit: bool) -> impl Future<Item = BitArrayFileBuilder<W>, Error = io::Error> {
        let BitArrayFileBuilder {
            current,
            count,
            dest,
        } = self;

        // Set the bit in the current word.
        let current = if bit {
            // Determine the position of the bit to be set from `count`.
            let pos = count & 0b11_1111;
            current | 0x8000_0000_0000_0000 >> pos
        } else {
            current
        };

        // Advance the bit count.
        let count = count + 1;

        // Check if the new `count` has reached a word boundary.
        if count & 0b11_1111 == 0 {
            // We have filled `current`, so write it to the destination.
            future::Either::A(util::write_u64(dest, current).map(move |dest| {
                BitArrayFileBuilder {
                    // Initialize `current` for bitwise OR-ing new values.
                    current: 0,
                    count,
                    dest,
                }
            }))
        } else {
            // We have not filled `current`, so return and wait for another `push`.
            future::Either::B(future::ok(BitArrayFileBuilder {
                current,
                count,
                dest,
            }))
        }
    }

    pub fn push_all<S: Stream<Item = bool, Error = io::Error>>(
        self,
        stream: S,
    ) -> impl Future<Item = BitArrayFileBuilder<W>, Error = io::Error> {
        stream.fold(self, |builder, bit| builder.push(bit))
    }

    fn finalize_data(self) -> impl Future<Item = W, Error = io::Error> {
        let BitArrayFileBuilder {
            current,
            count,
            dest,
        } = self;
        if count & 0b11_1111 == 0 {
            future::Either::A(future::ok(dest))
        } else {
            future::Either::B(util::write_u64(dest, current))
        }
    }

    pub fn finalize(self) -> impl Future<Item = W, Error = io::Error> {
        let count = self.count;
        // Write the final data word.
        self.finalize_data()
            // Write the control word.
            .and_then(move |dest| util::write_u64(dest, count))
            // Flush the `dest`.
            .and_then(tokio::io::flush)
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

pub struct BitArrayBlockDecoder {
    /// The next word, if it exists, to return.
    ///
    /// This is used to make sure that `decode` always returns one word behind the current word, so
    /// that when we reach the end, we don't return the last word, which is the control word.
    readahead: Option<u64>,
}

impl Decoder for BitArrayBlockDecoder {
    type Item = u64;
    type Error = io::Error;

    /// Decode the next block of the bit array.
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<u64>, io::Error> {
        // If there isn't a full word available in the buffer, stop.
        if bytes.len() < 8 {
            return Ok(None);
        }

        // Read the next word. If `self.readahead` was `Some`, return that value; otherwise,
        // recurse to read a second word and then return the first word.
        //
        // This trick means that we don't return the last word in the buffer, which is the control
        // word. The consequence is that we read an extra word at the beginning of the decoding
        // process.
        match self
            .readahead
            .replace(BigEndian::read_u64(&bytes.split_to(8)))
        {
            Some(word) => Ok(Some(word)),
            None => self.decode(bytes),
        }
    }
}

pub fn bitarray_stream_blocks<R: AsyncRead>(r: R) -> FramedRead<R, BitArrayBlockDecoder> {
    FramedRead::new(r, BitArrayBlockDecoder { readahead: None })
}

/// Read the length (number of bits) from a `FileLoad`.
fn bitarray_len_from_file<F: FileLoad>(f: F) -> impl Future<Item = (F, u64), Error = io::Error> {
    BitArrayError::validate_input_buf_size(f.size())
        .map_or_else(|e| Err(e.into()), |_| Ok(f))
        .into_future()
        .and_then(|f| {
            tokio::io::read_exact(f.open_read_from(f.size() - 8), [0; 8]).map(|(_, buf)| (f, buf))
        })
        .and_then(|(f, control_word)| {
            read_control_word(&control_word, f.size())
                .map_or_else(|e| Err(e.into()), |len| Ok((f, len)))
                .into_future()
        })
}

pub fn bitarray_stream_bits<F: FileLoad>(f: F) -> impl Stream<Item = bool, Error = io::Error> {
    // Read the length.
    bitarray_len_from_file(f)
        .into_stream()
        .map(move |(f, len)| {
            // Read the words into a `Stream`.
            bitarray_stream_blocks(f.open_read())
                // For each word, read the bits into a `Stream`.
                .map(|block| stream::iter_ok(BitIter::new(block)))
                // Turn the `Stream` of bit `Stream`s into a bit `Stream`.
                .flatten()
                // Cut the `Stream` off after the length of bits is reached.
                .take(len)
        })
        // Turn the `Stream` of bit `Stream`s into a bit `Stream`.
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;

    #[test]
    fn bit_array_error() {
        // Display
        assert_eq!(
            "expected input buffer size (7) >= 8",
            BitArrayError::InputBufferTooSmall(7).to_string()
        );
        assert_eq!(
            "expected input buffer size (9) to be 8 for 0 bits",
            BitArrayError::UnexpectedInputBufferSize(9, 8, 0).to_string()
        );

        // From<BitArrayError> for io::Error
        assert_eq!(
            io::Error::new(
                io::ErrorKind::InvalidData,
                BitArrayError::InputBufferTooSmall(7)
            )
            .to_string(),
            io::Error::from(BitArrayError::InputBufferTooSmall(7)).to_string()
        );
    }

    #[test]
    fn validate_input_buf_size() {
        let val = |buf_size| BitArrayError::validate_input_buf_size(buf_size);
        let err = |buf_size| Err(BitArrayError::InputBufferTooSmall(buf_size));
        assert_eq!(err(7), val(7));
        assert_eq!(Ok(()), val(8));
        assert_eq!(Ok(()), val(9));
        assert_eq!(Ok(()), val(usize::max_value()));
    }

    #[test]
    fn validate_len() {
        let val = |buf_size, len| BitArrayError::validate_len(buf_size, len);
        let err = |buf_size, expected, len| {
            Err(BitArrayError::UnexpectedInputBufferSize(
                buf_size, expected, len,
            ))
        };

        assert_eq!(err(0, 8, 0), val(0, 0));
        assert_eq!(Ok(()), val(16, 1));
        assert_eq!(Ok(()), val(16, 2));

        #[cfg(target_pointer_width = "64")]
        assert_eq!(
            Ok(()),
            val(
                usize::try_from(u128::from(u64::max_value()) + 65 >> 6 << 3).unwrap(),
                u64::max_value()
            )
        );
    }

    #[test]
    fn decode() {
        let mut decoder = BitArrayBlockDecoder { readahead: None };
        let mut bytes = BytesMut::from([0u8; 8].as_ref());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
    }

    #[test]
    pub fn empty() {
        assert!(BitArray::from_bits(Bytes::from([0u8; 8].as_ref()))
            .unwrap()
            .is_empty());
    }

    #[test]
    pub fn construct_and_parse_small_bitarray() {
        let x = MemoryBackedStore::new();
        let contents = vec![true, true, false, false, true];

        BitArrayFileBuilder::new(x.open_write())
            .push_all(stream::iter_ok(contents))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let loaded = x.map().wait().unwrap();

        let bitarray = BitArray::from_bits(loaded).unwrap();

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

        BitArrayFileBuilder::new(x.open_write())
            .push_all(stream::iter_ok(contents))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let loaded = x.map().wait().unwrap();

        let bitarray = BitArray::from_bits(loaded).unwrap();

        for i in 0..bitarray.len() {
            assert_eq!(i % 3 == 0, bitarray.get(i));
        }
    }

    #[test]
    fn bitarray_len_from_file_errors() {
        let store = MemoryBackedStore::new();
        let _ = tokio::io::write_all(store.open_write(), [0, 0, 0]).wait();
        assert_eq!(
            io::Error::from(BitArrayError::InputBufferTooSmall(3)).to_string(),
            bitarray_len_from_file(store)
                .wait()
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let _ = tokio::io::write_all(store.open_write(), [0, 0, 0, 0, 0, 0, 0, 2]).wait();
        assert_eq!(
            io::Error::from(BitArrayError::UnexpectedInputBufferSize(8, 16, 2)).to_string(),
            bitarray_len_from_file(store)
                .wait()
                .err()
                .unwrap()
                .to_string()
        );
    }

    #[test]
    pub fn stream_blocks() {
        let x = MemoryBackedStore::new();
        let contents = (0..).map(|n| n % 4 == 1).take(256);

        BitArrayFileBuilder::new(x.open_write())
            .push_all(stream::iter_ok(contents))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let stream = bitarray_stream_blocks(x.open_read());

        stream
            .for_each(|block| Ok(assert_eq!(0x4444444444444444, block)))
            .wait()
            .unwrap();
    }

    #[test]
    fn stream_bits() {
        let x = MemoryBackedStore::new();
        let contents: Vec<_> = (0..).map(|n| n % 4 == 1).take(123).collect();

        BitArrayFileBuilder::new(x.open_write())
            .push_all(stream::iter_ok(contents.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let result = bitarray_stream_bits(x).collect().wait().unwrap();

        assert_eq!(contents, result);
    }
}
