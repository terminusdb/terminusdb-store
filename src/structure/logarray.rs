#![allow(clippy::precedence, clippy::verbose_bit_mask)]

//! Code for storing, loading, and using log arrays.
//!
//! A log array is a contiguous sequence of N unsigned integers, with each value occupying exactly
//! W bits. By choosing W as the minimal bit width required for the largest value in the array, the
//! whole sequence can be compressed while increasing the constant cost of indexing by a few
//! operations over the typical byte-aligned array.
//!
//! The log array operations in this module use the following implementation:
//!
//! 1. The input buffer can be evenly divided into L+1 words, where a word is 64 bits.
//! 2. The first L words are the data buffer, a contiguous sequence of elements, where an element
//!    is an unsigned integer represented by W bits.
//! 3. The L+1 word is the control word and contains the following sequence:
//!    1. a 32-bit unsigned integer representing N, the number of elements,
//!    2. an 8-bit unsigned integer representing W, the number of bits used to store each element,
//!       and
//!    3. 24 unused bits.
//!
//! # Notes
//!
//! * All integers are stored in a standard big-endian encoding.
//! * The maximum bit width W is 64.
//! * The maximum number of elements is 2^32-1.
//!
//! # Naming
//!
//! Because of the ambiguity of the English language and possibility to confuse the meanings of the
//! words used to describe aspects of this code, we try to use the following definitions
//! consistently throughout:
//!
//! * buffer: a contiguous sequence of bytes
//!
//! * size: the number of bytes in a buffer
//!
//! * word: a 64-bit contiguous sequence aligned on 8-byte boundaries starting at the beginning of
//!     the input buffer
//!
//! * element: a logical unsigned integer value that is a member of the log array
//!
//! * index: the logical address of an element in the data buffer. A physical index is preceded by
//!     word, byte, or bit to indicate the address precision of the index.
//!
//! * offset: the number of bits preceding the msb of an element within the first word containing
//!     that element
//!
//! * width: the number of bits that every element occupies in the log array
//!
//! * length: the number of elements in the log array

use super::util;
use crate::storage::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use futures::stream::{self, Stream, StreamExt};
use std::pin::Pin;
use std::{cmp::Ordering, convert::TryFrom, error, fmt, io};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{Decoder, FramedRead};

// Static assertion: We expect the system architecture bus width to be >= 32 bits. If it is not,
// the following line will cause a compiler error. (Ignore the unrelated error message itself.)
const _: usize = 0 - !(std::mem::size_of::<usize>() >= 32 >> 3) as usize;

/// An in-memory log array
#[derive(Clone)]
pub struct LogArray {
    /// Index of the first accessible element
    ///
    /// For an original log array, this is initialized to 0. For a slice, this is the index to the
    /// first element of the slice.
    first: u32,

    /// Number of accessible elements
    ///
    /// For an original log array, this is initialized to the value read from the control word. For
    /// a slice, it is the length of the slice.
    len: u32,

    /// Bit width of each element
    width: u8,

    /// Shared reference to the input buffer
    ///
    /// Index 0 points to the first byte of the first element. The last word is the control word.
    input_buf: Bytes,
}

/// An error that occurred during a log array operation.
#[derive(Debug, PartialEq)]
pub enum LogArrayError {
    InputBufferTooSmall(usize),
    WidthTooLarge(u8),
    UnexpectedInputBufferSize(u64, u64, u32, u8),
}

impl LogArrayError {
    /// Validate the input buffer size.
    ///
    /// It must have at least the control word.
    fn validate_input_buf_size(input_buf_size: usize) -> Result<(), Self> {
        if input_buf_size < 8 {
            return Err(LogArrayError::InputBufferTooSmall(input_buf_size));
        }
        Ok(())
    }

    /// Validate the number of elements and bit width against the input buffer size.
    ///
    /// The bit width should no greater than 64 since each word is 64 bits.
    ///
    /// The input buffer size should be the appropriate multiple of 8 to include the exact number
    /// of encoded elements plus the control word.
    fn validate_len_and_width(input_buf_size: usize, len: u32, width: u8) -> Result<(), Self> {
        if width > 64 {
            return Err(LogArrayError::WidthTooLarge(width));
        }

        // Calculate the expected input buffer size. This includes the control word.
        // To avoid overflow, convert `len: u32` to `u64` and do the addition in `u64`.
        let expected_buf_size = u64::from(len) * u64::from(width) + 127 >> 6 << 3;
        let input_buf_size = u64::try_from(input_buf_size).unwrap();

        if input_buf_size != expected_buf_size {
            return Err(LogArrayError::UnexpectedInputBufferSize(
                input_buf_size,
                expected_buf_size,
                len,
                width,
            ));
        }

        Ok(())
    }
}

impl fmt::Display for LogArrayError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use LogArrayError::*;
        match self {
            InputBufferTooSmall(input_buf_size) => {
                write!(f, "expected input buffer size ({}) >= 8", input_buf_size)
            }
            WidthTooLarge(width) => write!(f, "expected width ({}) <= 64", width),
            UnexpectedInputBufferSize(input_buf_size, expected_buf_size, len, width) => write!(
                f,
                "expected input buffer size ({}) to be {} for {} elements and width {}",
                input_buf_size, expected_buf_size, len, width
            ),
        }
    }
}

impl error::Error for LogArrayError {}

impl From<LogArrayError> for io::Error {
    fn from(err: LogArrayError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

#[derive(Clone)]
pub struct LogArrayIterator {
    logarray: LogArray,
    pos: usize,
    end: usize,
}

impl Iterator for LogArrayIterator {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if self.pos == self.end {
            None
        } else {
            let result = self.logarray.entry(self.pos);
            self.pos += 1;

            Some(result)
        }
    }
}

/// Read the length and bit width from the control word buffer. `buf` must start at the first word
/// after the data buffer. `input_buf_size` is used for validation.
fn read_control_word(buf: &[u8], input_buf_size: usize) -> Result<(u32, u8), LogArrayError> {
    let len = BigEndian::read_u32(buf);
    let width = buf[4];
    LogArrayError::validate_len_and_width(input_buf_size, len, width)?;
    Ok((len, width))
}

impl LogArray {
    /// Construct a `LogArray` by parsing a `Bytes` buffer.
    pub fn parse(input_buf: Bytes) -> Result<LogArray, LogArrayError> {
        let input_buf_size = input_buf.len();
        LogArrayError::validate_input_buf_size(input_buf_size)?;
        let (len, width) = read_control_word(&input_buf[input_buf_size - 8..], input_buf_size)?;
        Ok(LogArray {
            first: 0,
            len,
            width,
            input_buf,
        })
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        // `usize::try_from` succeeds if `std::mem::size_of::<usize>()` >= 4.
        usize::try_from(self.len).unwrap()
    }

    /// Returns `true` if there are no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the bit width.
    pub fn width(&self) -> u8 {
        self.width
    }

    /// Reads the data buffer and returns the element at the `index`.
    ///
    /// Panics if `index` is >= the length of the log array.
    pub fn entry(&self, index: usize) -> u64 {
        assert!(
            index < self.len(),
            "expected index ({}) < length ({})",
            index,
            self.len
        );

        // `usize::try_from` succeeds if `std::mem::size_of::<usize>()` >= 4.
        let bit_index = usize::from(self.width) * (usize::try_from(self.first).unwrap() + index);

        // Read the words that contain the element.
        let (first_word, second_word) = {
            // Calculate the byte index from the bit index.
            let byte_index = bit_index >> 6 << 3;

            let buf = &self.input_buf;

            // Read the first word.
            let first_word = BigEndian::read_u64(&buf[byte_index..]);

            // Read the second word (optimistically).
            //
            // This relies on the buffer having the control word at the end. If that is not there,
            // this may panic.
            let second_word = BigEndian::read_u64(&buf[byte_index + 8..]);

            (first_word, second_word)
        };

        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - self.width;

        // Get the bit offset in `first_word`.
        let offset = (bit_index & 0b11_1111) as u8;

        // If the element fits completely in `first_word`, we can return it immediately.
        if offset + self.width <= 64 {
            // Decode by introducing leading zeros and shifting all the way to the right.
            return first_word << offset >> leading_zeros;
        }

        // At this point, we have an element split over `first_word` and `second_word`. The bottom
        // bits of `first_word` become the upper bits of the decoded value, and the top bits of
        // `second_word` become the lower bits of the decoded value.

        // These are the bit widths of the important parts in `first_word` and `second_word`.
        let first_width = 64 - offset;
        let second_width = self.width - first_width;

        // These are the parts of the element with the unimportant parts removed.

        // Introduce leading zeros and trailing zeros where the `second_part` will go.
        let first_part = first_word << offset >> offset << second_width;

        // Introduce leading zeros where the `first_part` will go.
        let second_part = second_word >> 64 - second_width;

        // Decode by combining the first and second parts.
        first_part | second_part
    }

    pub fn iter(&self) -> LogArrayIterator {
        LogArrayIterator {
            logarray: self.clone(),
            pos: 0,
            end: self.len(),
        }
    }

    /// Returns a logical slice of the elements in a log array.
    ///
    /// Panics if `index` + `length` is >= the length of the log array.
    pub fn slice(&self, offset: usize, len: usize) -> LogArray {
        let offset = u32::try_from(offset)
            .unwrap_or_else(|_| panic!("expected 32-bit slice offset ({})", offset));
        let len =
            u32::try_from(len).unwrap_or_else(|_| panic!("expected 32-bit slice length ({})", len));
        let slice_end = offset.checked_add(len).unwrap_or_else(|| {
            panic!("overflow from slice offset ({}) + length ({})", offset, len)
        });
        assert!(
            slice_end <= self.len,
            "expected slice offset ({}) + length ({}) <= source length ({})",
            offset,
            len,
            self.len
        );
        LogArray {
            first: self.first + offset,
            len,
            width: self.width,
            input_buf: self.input_buf.clone(),
        }
    }
}

/// write a logarray directly to an AsyncWrite
pub struct LogArrayFileBuilder<W: AsyncWrite + Unpin> {
    /// Destination of the log array data
    file: W,
    /// Bit width of an element
    width: u8,
    /// Storage for the next word to be written to the buffer
    current: u64,
    /// Bit offset in `current` for the msb of the next encoded element
    offset: u8,
    /// Number of elements written to the buffer
    count: u32,
}

impl<W: AsyncWrite + Unpin> LogArrayFileBuilder<W> {
    pub fn new(w: W, width: u8) -> LogArrayFileBuilder<W> {
        LogArrayFileBuilder {
            file: w,
            width,
            // Zero is needed for bitwise OR-ing new values.
            current: 0,
            // Start at the beginning of `current`.
            offset: 0,
            // No elements have been written.
            count: 0,
        }
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub async fn push(&mut self, val: u64) -> io::Result<()> {
        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - self.width;

        // If `val` does not fit in the `width`, return an error.
        if val.leading_zeros() < u32::from(leading_zeros) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected value ({}) to fit in {} bits", val, self.width),
            ));
        }

        // Otherwise, push `val` onto the log array.
        // Advance the element count since we know we're going to write `val`.
        self.count += 1;

        // Write the first part of `val` to `current`, putting the msb of `val` at the `offset`
        // bit. This may be either the upper bits of `val` only or all of it. We check later.
        self.current |= val << leading_zeros >> self.offset;

        // Increment `offset` past `val`.
        self.offset += self.width;

        // Check if the new `offset` is larger than 64.
        if self.offset >= 64 {
            // We have filled `current`, so write it to the destination.
            util::write_u64(&mut self.file, self.current).await?;
            // Wrap the offset with the word size.
            self.offset -= 64;

            // Initialize the new `current`.
            self.current = if self.offset == 0 {
                // Zero is needed for bitwise OR-ing new values.
                0
            } else {
                // This is the second part of `val`: the lower bits.
                val << 64 - self.offset
            };
        }

        Ok(())
    }

    pub async fn push_all<S: Stream<Item = io::Result<u64>> + Unpin>(
        &mut self,
        mut vals: S,
    ) -> io::Result<()> {
        while let Some(val) = vals.next().await {
            self.push(val?).await?;
        }

        Ok(())
    }

    async fn finalize_data(&mut self) -> io::Result<()> {
        if u64::from(self.count) * u64::from(self.width) & 0b11_1111 != 0 {
            util::write_u64(&mut self.file, self.current).await?;
        }

        Ok(())
    }

    pub async fn finalize(mut self) -> io::Result<W> {
        let len = self.count;
        let width = self.width;

        // Write the final data word.
        self.finalize_data().await?;

        // Write the control word.
        let mut buf = [0; 8];
        BigEndian::write_u32(&mut buf, len);
        buf[4] = width;
        self.file.write_all(&buf).await?;

        self.file.flush().await?;

        Ok(self.file)
    }
}

struct LogArrayDecoder {
    /// Storage for the most recent word read from the buffer
    current: u64,
    /// Bit width of an element
    width: u8,
    /// Bit offset from the msb of `current` to the msb of the encoded element
    offset: u8,
    /// Number of elements remaining to decode
    remaining: u32,
}

impl fmt::Debug for LogArrayDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogArrayDecoder {{ current: ")?;
        write!(f, "{:#066b}", self.current)?;
        write!(f, ", width: ")?;
        write!(f, "{:?}", self.width)?;
        write!(f, ", offset: ")?;
        write!(f, "{:?}", self.offset)?;
        write!(f, ", remaining: ")?;
        write!(f, "{:?}", self.remaining)?;
        write!(f, " }}")
    }
}

impl LogArrayDecoder {
    /// Construct a new `LogArrayDecoder`.
    ///
    /// This function does not validate the parameters. Validation of `width` and `remaining` must
    /// be done before calling this function.
    fn new_unchecked(width: u8, remaining: u32) -> Self {
        LogArrayDecoder {
            // The initial value of `current` is ignored by `decode()` because `offset` is 64.
            current: 0,
            // The initial value of `offset` is interpreted in `decode()` to begin reading a new
            // word and ignore the initial value of `current`.
            offset: 64,
            width,
            remaining,
        }
    }
}

impl Decoder for LogArrayDecoder {
    type Item = u64;
    type Error = io::Error;

    /// Decode the next element of the log array.
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<u64>, io::Error> {
        // If we have no elements remaining to decode, clean up and exit.
        if self.remaining == 0 {
            bytes.clear();
            return Ok(None);
        }

        // At this point, we have at least one element to decode.

        // Declare some immutable working values. After this, `self.<field>` only appears on the
        // lhs of `=`.
        let first_word = self.current;
        let offset = self.offset;
        let width = self.width;

        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - width;

        // If the next element fits completely in `first_word`, we can return it immediately.
        if offset + width <= 64 {
            // Increment to the msb of the next element.
            self.offset += width;
            // Decrement since we're returning a decoded element.
            self.remaining -= 1;
            // Decode by introducing leading zeros and shifting all the way to the right.
            return Ok(Some(first_word << offset >> leading_zeros));
        }

        // At this point, we need to read another word because we do not have enough bits in
        // `first_word` to decode.

        // If there isn't a full word available in the buffer, stop until there is.
        if bytes.len() < 8 {
            return Ok(None);
        }

        // Load the `second_word` and advance `bytes` by 1 word.
        let second_word = BigEndian::read_u64(&bytes.split_to(8));
        self.current = second_word;

        // Decrement to indicate we will return another decoded element.
        self.remaining -= 1;

        // If the `offset` is 64, it means that the element is completely included in the
        // `second_word`.
        if offset == 64 {
            // Increment the `offset` to the msb of the next element.
            self.offset = width;

            // Decode by shifting all the way to the right. Since the msb of `second_word` and the
            // encoded value are the same, this naturally introduces leading zeros.
            return Ok(Some(second_word >> leading_zeros));
        }

        // At this point, we have an element split over `first_word` and `second_word`. The bottom
        // bits of `first_word` become the upper bits of the decoded value, and the top bits of
        // `second_word` become the lower bits of the decoded value.

        // These are the bit widths of the important parts in `first_word` and `second_word`.
        let first_width = 64 - offset;
        let second_width = width - first_width;

        // These are the parts of the element with the unimportant parts removed.

        // Introduce leading zeros and trailing zeros where the `second_part` will go.
        let first_part = first_word << offset >> offset << second_width;

        // Introduce leading zeros where the `first_part` will go.
        let second_part = second_word >> 64 - second_width;

        // Increment the `offset` to the msb of the next element.
        self.offset = second_width;

        // Decode by combining the first and second parts.
        Ok(Some(first_part | second_part))
    }
}

pub async fn logarray_file_get_length_and_width<F: FileLoad>(f: F) -> io::Result<(u32, u8)> {
    LogArrayError::validate_input_buf_size(f.size())?;

    let mut buf = [0; 8];
    f.open_read_from(f.size() - 8).read_exact(&mut buf).await?;
    Ok(read_control_word(&buf, f.size())?)
}

pub fn logarray_stream_entries<F: 'static + FileLoad>(
    f: F,
) -> impl Stream<Item = io::Result<u64>> + Unpin + Send {
    Box::pin(
        logarray_file_get_length_and_width(f.clone())
            .map(move |result| match result {
                Ok((len, width)) => Box::pin(FramedRead::new(
                    f.open_read(),
                    LogArrayDecoder::new_unchecked(width, len),
                ))
                    as Pin<Box<dyn Stream<Item = io::Result<u64>> + Send>>,
                Err(e) => Box::pin(stream::iter(vec![Err(e)])),
            })
            .into_stream()
            .flatten(),
    )
}

#[derive(Clone)]
pub struct MonotonicLogArray(LogArray);

impl MonotonicLogArray {
    pub fn from_logarray(logarray: LogArray) -> MonotonicLogArray {
        if cfg!(debug_assertions) {
            // Validate that the elements are monotonically increasing.
            let mut iter = logarray.iter();
            if let Some(mut pred) = iter.next() {
                for succ in iter {
                    assert!(
                        pred <= succ,
                        "not monotonic: expected predecessor ({}) <= successor ({})",
                        pred,
                        succ
                    );
                    pred = succ;
                }
            }
        }

        MonotonicLogArray(logarray)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn entry(&self, index: usize) -> u64 {
        self.0.entry(index)
    }

    pub fn iter(&self) -> LogArrayIterator {
        self.0.iter()
    }

    pub fn index_of(&self, element: u64) -> Option<usize> {
        let index = self.nearest_index_of(element);
        if index >= self.len() || self.entry(index) != element {
            None
        } else {
            Some(index)
        }
    }

    pub fn nearest_index_of(&self, element: u64) -> usize {
        if self.is_empty() {
            return 0;
        }

        let mut min = 0;
        let mut max = self.len() - 1;
        while min <= max {
            let mid = (min + max) / 2;
            match element.cmp(&self.entry(mid)) {
                Ordering::Equal => return mid,
                Ordering::Greater => min = mid + 1,
                Ordering::Less => {
                    if mid == 0 {
                        return 0;
                    }
                    max = mid - 1
                }
            }
        }

        (min + max) / 2 + 1
    }
}

impl From<LogArray> for MonotonicLogArray {
    fn from(l: LogArray) -> Self {
        Self::from_logarray(l)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use crate::structure::util::stream_iter_ok;
    use futures::executor::block_on;
    use futures::stream::TryStreamExt;

    #[test]
    fn log_array_error() {
        // Display
        assert_eq!(
            "expected input buffer size (7) >= 8",
            LogArrayError::InputBufferTooSmall(7).to_string()
        );
        assert_eq!(
            "expected width (69) <= 64",
            LogArrayError::WidthTooLarge(69).to_string()
        );
        assert_eq!(
            "expected input buffer size (9) to be 8 for 0 elements and width 17",
            LogArrayError::UnexpectedInputBufferSize(9, 8, 0, 17).to_string()
        );

        // From<LogArrayError> for io::Error
        assert_eq!(
            io::Error::new(
                io::ErrorKind::InvalidData,
                LogArrayError::InputBufferTooSmall(7)
            )
            .to_string(),
            io::Error::from(LogArrayError::InputBufferTooSmall(7)).to_string()
        );
    }

    #[test]
    fn validate_input_buf_size() {
        let val = |buf_size| LogArrayError::validate_input_buf_size(buf_size);
        let err = |buf_size| Err(LogArrayError::InputBufferTooSmall(buf_size));
        assert_eq!(err(7), val(7));
        assert_eq!(Ok(()), val(8));
        assert_eq!(Ok(()), val(9));
        assert_eq!(Ok(()), val(usize::max_value()));
    }

    #[test]
    fn validate_len_and_width() {
        let val =
            |buf_size, len, width| LogArrayError::validate_len_and_width(buf_size, len, width);

        let err = |width| Err(LogArrayError::WidthTooLarge(width));

        // width: 65
        assert_eq!(err(65), val(0, 0, 65));

        let err = |buf_size, expected, len, width| {
            Err(LogArrayError::UnexpectedInputBufferSize(
                buf_size, expected, len, width,
            ))
        };

        // width: 0
        assert_eq!(err(0, 8, 0, 0), val(0, 0, 0));

        // width: 1
        assert_eq!(Ok(()), val(8, 0, 1));
        assert_eq!(err(9, 8, 0, 1), val(9, 0, 1));
        assert_eq!(Ok(()), val(16, 1, 1));

        // width: 64
        assert_eq!(Ok(()), val(16, 1, 64));
        assert_eq!(err(16, 24, 2, 64), val(16, 2, 64));
        assert_eq!(err(24, 16, 1, 64), val(24, 1, 64));

        #[cfg(target_pointer_width = "64")]
        assert_eq!(
            Ok(()),
            val(
                usize::try_from(u64::from(u32::max_value()) + 1 << 3).unwrap(),
                u32::max_value(),
                64
            )
        );

        // width: 5
        assert_eq!(err(16, 24, 13, 5), val(16, 13, 5));
        assert_eq!(Ok(()), val(24, 13, 5));
    }

    #[test]
    pub fn empty() {
        let logarray = LogArray::parse(Bytes::from([0u8; 8].as_ref())).unwrap();
        assert!(logarray.is_empty());
        assert!(MonotonicLogArray::from_logarray(logarray).is_empty());
    }

    #[test]
    #[should_panic(expected = "expected value (8) to fit in 3 bits")]
    fn log_array_file_builder_panic() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 3);
        block_on(builder.push(8)).unwrap();
    }

    #[test]
    fn generate_then_parse_works() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 5);
        block_on(async {
            builder
                .push_all(stream_iter_ok(vec![1, 3, 2, 5, 12, 31, 18]))
                .await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();

        assert_eq!(1, logarray.entry(0));
        assert_eq!(3, logarray.entry(1));
        assert_eq!(2, logarray.entry(2));
        assert_eq!(5, logarray.entry(3));
        assert_eq!(12, logarray.entry(4));
        assert_eq!(31, logarray.entry(5));
        assert_eq!(18, logarray.entry(6));
    }

    const TEST0_DATA: [u8; 8] = [
        0b00000000,
        0b00000000,
        0b1_0000000,
        0b00000000,
        0b10_000000,
        0b00000000,
        0b011_00000,
        0b00000000,
    ];
    const TEST0_CONTROL: [u8; 8] = [0, 0, 0, 3, 17, 0, 0, 0];
    const TEST1_DATA: [u8; 8] = [
        0b0100_0000,
        0b00000000,
        0b00101_000,
        0b00000000,
        0b000110_00,
        0b00000000,
        0b0000111_0,
        0b00000000,
    ];

    fn test0_logarray() -> LogArray {
        let mut content = Vec::new();
        content.extend_from_slice(&TEST0_DATA);
        content.extend_from_slice(&TEST0_CONTROL);
        LogArray::parse(Bytes::from(content)).unwrap()
    }

    #[test]
    #[should_panic(expected = "expected index (3) < length (3)")]
    fn entry_panic() {
        let _ = test0_logarray().entry(3);
    }

    #[test]
    #[should_panic(expected = "expected slice offset (2) + length (2) <= source length (3)")]
    fn slice_panic1() {
        let _ = test0_logarray().slice(2, 2);
    }

    #[test]
    #[should_panic(expected = "expected 32-bit slice offset (4294967296)")]
    #[cfg(target_pointer_width = "64")]
    fn slice_panic2() {
        let _ = test0_logarray().slice(usize::try_from(u32::max_value()).unwrap() + 1, 2);
    }

    #[test]
    #[should_panic(expected = "expected 32-bit slice length (4294967296)")]
    #[cfg(target_pointer_width = "64")]
    fn slice_panic3() {
        let _ = test0_logarray().slice(0, usize::try_from(u32::max_value()).unwrap() + 1);
    }

    #[test]
    #[should_panic(expected = "overflow from slice offset (4294967295) + length (1)")]
    fn slice_panic4() {
        let _ = test0_logarray().slice(usize::try_from(u32::max_value()).unwrap(), 1);
    }

    #[test]
    #[should_panic(expected = "expected index (2) < length (2)")]
    fn slice_entry_panic() {
        let _ = test0_logarray().slice(1, 2).entry(2);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "not monotonic: expected predecessor (2) <= successor (1)")]
    fn monotonic_panic() {
        let content = [0u8, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 32, 0, 0, 0].as_ref();
        MonotonicLogArray::from_logarray(LogArray::parse(Bytes::from(content)).unwrap());
    }

    #[test]
    fn decode() {
        let mut decoder = LogArrayDecoder::new_unchecked(17, 1);
        let mut bytes = BytesMut::from(TEST0_DATA.as_ref());
        assert_eq!(Some(1), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
        decoder = LogArrayDecoder::new_unchecked(17, 4);
        bytes = BytesMut::from(TEST0_DATA.as_ref());
        assert_eq!(Some(1), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(
            "LogArrayDecoder { current: \
             0b0000000000000000100000000000000010000000000000000110000000000000, width: 17, \
             offset: 17, remaining: 3 }",
            format!("{:?}", decoder)
        );
        assert_eq!(Some(2), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(Some(3), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
        bytes.extend(TEST1_DATA.iter());
        assert_eq!(Some(4), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
    }

    #[test]
    fn logarray_file_get_length_and_width_errors() {
        let store = MemoryBackedStore::new();
        let _ = block_on(store.open_write().write_all(&[0, 0, 0]));
        assert_eq!(
            io::Error::from(LogArrayError::InputBufferTooSmall(3)).to_string(),
            block_on(logarray_file_get_length_and_width(store))
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let _ = block_on(store.open_write().write_all(&[0, 0, 0, 0, 65, 0, 0, 0]));
        assert_eq!(
            io::Error::from(LogArrayError::WidthTooLarge(65)).to_string(),
            block_on(logarray_file_get_length_and_width(store))
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let _ = block_on(store.open_write().write_all(&[0, 0, 0, 1, 17, 0, 0, 0]));
        assert_eq!(
            io::Error::from(LogArrayError::UnexpectedInputBufferSize(8, 16, 1, 17)).to_string(),
            block_on(logarray_file_get_length_and_width(store))
                .err()
                .unwrap()
                .to_string()
        );
    }

    #[test]
    fn generate_then_stream_works() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 5);
        block_on(async {
            builder.push_all(stream_iter_ok(0..31)).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let entries: Vec<u64> =
            block_on(logarray_stream_entries(store).try_collect::<Vec<u64>>()).unwrap();
        let expected: Vec<u64> = (0..31).collect();
        assert_eq!(expected, entries);
    }

    #[test]
    fn iterate_over_logarray() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 2, 5, 12, 31, 18];
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();

        let result: Vec<u64> = logarray.iter().collect();

        assert_eq!(original, result);
    }

    #[test]
    fn iterate_over_logarray_slice() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original: Vec<u64> = vec![1, 3, 2, 5, 12, 31, 18];
        block_on(async {
            builder.push_all(stream_iter_ok(original)).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();
        let slice = logarray.slice(2, 3);

        let result: Vec<u64> = slice.iter().collect();

        assert_eq!([2, 5, 12], result.as_ref());
    }

    #[test]
    fn monotonic_logarray_index_lookup() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 5, 6, 7, 10, 11, 15, 16, 18, 20, 25, 31];
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();
        let monotonic = MonotonicLogArray::from_logarray(logarray);

        for (i, &val) in original.iter().enumerate() {
            assert_eq!(i, monotonic.index_of(val).unwrap());
        }

        assert_eq!(None, monotonic.index_of(12));
        assert_eq!(original.len(), monotonic.len());
    }

    #[test]
    fn monotonic_logarray_near_index_lookup() {
        let store = MemoryBackedStore::new();
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![3, 5, 6, 7, 10, 11, 15, 16, 18, 20, 25, 31];
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;
            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();

        let logarray = LogArray::parse(content).unwrap();
        let monotonic = MonotonicLogArray::from_logarray(logarray);

        for (i, &val) in original.iter().enumerate() {
            assert_eq!(i, monotonic.index_of(val).unwrap());
        }

        let nearest: Vec<_> = (1..=32).map(|i| monotonic.nearest_index_of(i)).collect();
        let expected = vec![
            0, 0, 0, 1, 1, 2, 3, 4, 4, 4, 5, 6, 6, 6, 6, 7, 8, 8, 9, 9, 10, 10, 10, 10, 10, 11, 11,
            11, 11, 11, 11, 12,
        ];
        assert_eq!(expected, nearest);
    }

    #[test]
    fn writing_64_bits_of_data() {
        let store = MemoryBackedStore::new();
        let original = vec![1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        let mut builder = LogArrayFileBuilder::new(store.open_write(), 4);
        block_on(async {
            builder.push_all(stream_iter_ok(original.clone())).await?;
            builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let content = block_on(store.map()).unwrap();
        let logarray = LogArray::parse(content).unwrap();
        assert_eq!(original, logarray.iter().collect::<Vec<_>>());
        assert_eq!(16, logarray.len());
        assert_eq!(4, logarray.width());
    }
}
