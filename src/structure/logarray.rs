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
//! Notes:
//!
//! * All integers are stored in a standard big-endian encoding.
//! * The maximum bit width W is 64.
//! * The maximum number of elements is 2^32-1.
//!
//! Naming:
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
use bytes::BytesMut;
use futures::{future, prelude::*};
use std::{cmp::Ordering, error, fmt, io};
use tokio::codec::{Decoder, FramedRead};

// Static assertion: We expect the system architecture bus width to be >= 32 bits. If it is not,
// the following line will cause a compiler error. (Ignore the unrelated error message itself.)
const _: usize = 0 - !(std::mem::size_of::<usize>() >= 32 >> 3) as usize;

/// An in-memory log array
#[derive(Clone)]
pub struct LogArray<M: AsRef<[u8]>> {
    /// Number of elements
    len: u32,
    /// Bit width of each element
    width: u8,
    /// Owned reference to the data buffer.
    ///
    /// The `0` index points to the first byte of the first element.
    data: M,
}

/// An error that occurred during a log array operation.
#[derive(Debug, PartialEq)]
pub enum LogArrayError {
    InputBufTooSmall(usize),
    WidthTooLarge(u8),
    UnexpectedBufSize(usize, usize, u32, u8),
}

impl LogArrayError {
    /// Validate the input buffer size.
    ///
    /// It must have at least the control word.
    fn validate_input_buf_size(input_buf_size: usize) -> Result<(), Self> {
        if input_buf_size < 8 {
            return Err(LogArrayError::InputBufTooSmall(input_buf_size));
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
        let minimum_buf_bit_size = len as usize * width as usize;
        let minimum_buf_size = minimum_buf_bit_size + 7 >> 3;
        let expected_buf_size = minimum_buf_size + 15 >> 3 << 3;

        if input_buf_size != expected_buf_size {
            return Err(LogArrayError::UnexpectedBufSize(
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
            InputBufTooSmall(input_buf_size) => {
                write!(f, "expected input buffer size ({}) >= 8", input_buf_size)
            }
            WidthTooLarge(width) => write!(f, "expected width ({}) <= 64", width),
            UnexpectedBufSize(input_buf_size, expected_buf_size, len, width) => write!(
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

pub struct LogArrayIterator<'a, M: AsRef<[u8]>> {
    logarray: &'a LogArray<M>,
    pos: usize,
    end: usize,
}

impl<'a, M: AsRef<[u8]>> Iterator for LogArrayIterator<'a, M> {
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

pub struct OwnedLogArrayIterator<M: AsRef<[u8]>> {
    logarray: LogArray<M>,
    pos: usize,
    end: usize,
}

impl<M: AsRef<[u8]>> Iterator for OwnedLogArrayIterator<M> {
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

impl<M: AsRef<[u8]>> LogArray<M> {
    /// Take ownership of a buffer, read the control word, validate it, and construct a log array
    /// around the buffer.
    pub fn parse(data: M) -> Result<LogArray<M>, LogArrayError> {
        let input_buf = data.as_ref();
        let input_buf_size = input_buf.len();
        LogArrayError::validate_input_buf_size(input_buf_size)?;

        let (len, width) = read_control_word(&input_buf[input_buf_size - 8..], input_buf_size)?;

        Ok(LogArray { len, width, data })
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns the bit width.
    pub fn width(&self) -> u8 {
        self.width
    }

    /// Reads the data buffer and returns the element at the given index.
    ///
    /// Panics if `index` is >= the length of the log array.
    pub fn entry(&self, index: usize) -> u64 {
        assert!(
            index < self.len as usize,
            "expected index ({}) < length ({})",
            index,
            self.len
        );

        let bit_index = self.width as usize * index;

        // Read the words that contain the element.
        let (first_word, second_word) = {
            // Calculate the byte index from the bit index.
            let byte_index = bit_index >> 6 << 3;

            let buf = self.data.as_ref();

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

    pub fn iter(&self) -> LogArrayIterator<M> {
        LogArrayIterator {
            logarray: self,
            pos: 0,
            end: self.len(),
        }
    }

    pub fn into_iter(self) -> OwnedLogArrayIterator<M> {
        OwnedLogArrayIterator {
            end: self.len(),
            logarray: self,
            pos: 0,
        }
    }

    /// Returns a logical slice of the elements in a log array.
    ///
    /// Panics if `index` + `length` is >= the length of the log array.
    pub fn slice(&self, offset: usize, length: usize) -> LogArraySlice<M>
    where
        M: Clone,
    {
        assert!(
            offset + length <= self.len(),
            "expected slice offset ({}) + length ({}) <= source length ({})",
            offset,
            length,
            self.len()
        );
        LogArraySlice {
            original: self.clone(),
            offset,
            length,
        }
    }
}

#[derive(Clone)]
pub struct LogArraySlice<M: AsRef<[u8]>> {
    original: LogArray<M>,
    offset: usize,
    length: usize,
}

impl<M: AsRef<[u8]>> LogArraySlice<M> {
    pub fn len(&self) -> usize {
        self.length
    }

    /// Reads the data buffer and returns the element at the given index in the slice.
    ///
    /// Panics if `index` is >= the length of the slice.
    pub fn entry(&self, index: usize) -> u64 {
        assert!(
            index < self.length,
            "expected slice index ({}) < length ({})",
            index,
            self.length
        );
        self.original.entry(index + self.offset)
    }

    pub fn iter(&self) -> LogArrayIterator<M> {
        LogArrayIterator {
            logarray: &self.original,
            pos: self.offset,
            end: self.offset + self.length,
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
pub struct LogArrayFileBuilder<W: 'static + tokio::io::AsyncWrite + Send> {
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

impl<W: 'static + tokio::io::AsyncWrite + Send> LogArrayFileBuilder<W> {
    pub fn new(w: W, width: u8) -> LogArrayFileBuilder<W> {
        LogArrayFileBuilder {
            file: w,
            width: width,
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

    pub fn push(self, val: u64) -> impl Future<Item = LogArrayFileBuilder<W>, Error = io::Error> {
        let LogArrayFileBuilder {
            file,
            width,
            current,
            offset,
            count,
        } = self;

        // This is the minimum number of leading zeros that a decoded value should have.
        let leading_zeros = 64 - width;

        // If `val` does not fit in the `width`, return an error.
        future::result(if val.leading_zeros() < leading_zeros as u32 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected value ({}) to fit in {} bits", val, width),
            ))
        } else {
            Ok(())
        })
        // Otherwise, push `val` onto the log array.
        .and_then(move |_| {
            // Advance the element count since we know we're going to write `val`.
            let count = count + 1;

            // Write the first part of `val` to `current`, putting the msb of `val` at the `offset`
            // bit. This may be either the upper bits of `val` only or all of it. We check later.
            let current = current | val << leading_zeros >> offset;

            // Increment `offset` past `val`.
            let offset = offset + width;

            // Check if the new `offset` is larger than 64.
            if offset >= 64 {
                // We have filled `current` with a word of data, so write it to the destination.
                future::Either::A(util::write_u64(file, current).map(move |file| {
                    // Wrap the offset with the word size.
                    let offset = offset - 64;

                    // Initialize the new `current`.
                    let current = if offset == 0 {
                        // Zero is needed for bitwise OR-ing new values.
                        0
                    } else {
                        // This is the second part of `val`: the lower bits.
                        val << 64 - offset
                    };

                    LogArrayFileBuilder {
                        file,
                        width,
                        count,
                        current,
                        offset,
                    }
                }))
            } else {
                future::Either::B(future::ok(LogArrayFileBuilder {
                    file,
                    width,
                    count,
                    current,
                    offset,
                }))
            }
        })
    }

    pub fn push_all<S: Stream<Item = u64, Error = io::Error>>(
        self,
        vals: S,
    ) -> impl Future<Item = LogArrayFileBuilder<W>, Error = io::Error> {
        vals.fold(self, |x, val| x.push(val))
    }

    fn write_last_data(self) -> impl Future<Item = W, Error = io::Error> {
        if self.count as u64 * self.width as u64 & 0b11_1111 == 0 {
            future::Either::A(future::ok(self.file))
        } else {
            future::Either::B(util::write_u64(self.file, self.current))
        }
    }

    pub fn finalize(self) -> impl Future<Item = W, Error = io::Error> {
        let len = self.count;
        let width = self.width;

        self.write_last_data().and_then(move |file| {
            // Write the control word.
            let mut buf = [0; 8];
            BigEndian::write_u32(&mut buf, len);
            buf[4] = width;
            util::write_all(file, buf)
        })
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

pub fn logarray_file_get_length_and_width<F: FileLoad>(
    f: F,
) -> impl Future<Item = (F, u32, u8), Error = io::Error> {
    LogArrayError::validate_input_buf_size(f.size())
        .map_or_else(|e| Err(e.into()), |_| Ok(f))
        .into_future()
        .and_then(|f| {
            tokio::io::read_exact(f.open_read_from(f.size() - 8), [0; 8]).map(|(_, buf)| (f, buf))
        })
        .and_then(|(f, buf)| {
            read_control_word(&buf, f.size())
                .map_or_else(|e| Err(e.into()), |(len, width)| Ok((f, len, width)))
                .into_future()
        })
}

pub fn logarray_stream_entries<F: FileLoad>(f: F) -> impl Stream<Item = u64, Error = io::Error> {
    logarray_file_get_length_and_width(f)
        .map(|(f, len, width)| {
            FramedRead::new(f.open_read(), LogArrayDecoder::new_unchecked(width, len))
        })
        .into_stream()
        .flatten()
}

#[derive(Clone)]
pub struct MonotonicLogArray<M: AsRef<[u8]>>(LogArray<M>);

impl<M: AsRef<[u8]>> MonotonicLogArray<M> {
    pub fn from_logarray(logarray: LogArray<M>) -> MonotonicLogArray<M> {
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

    pub fn entry(&self, index: usize) -> u64 {
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
                Ordering::Greater => min = mid + 1,
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
    use crate::storage::memory::*;
    use futures::stream;

    #[test]
    fn log_array_error() {
        // Display
        assert_eq!(
            "expected input buffer size (7) >= 8",
            LogArrayError::InputBufTooSmall(7).to_string()
        );
        assert_eq!(
            "expected width (69) <= 64",
            LogArrayError::WidthTooLarge(69).to_string()
        );
        assert_eq!(
            "expected input buffer size (9) to be 8 for 0 elements and width 17",
            LogArrayError::UnexpectedBufSize(9, 8, 0, 17).to_string()
        );

        // From<LogArrayError> for io::Error
        assert_eq!(
            io::Error::new(
                io::ErrorKind::InvalidData,
                LogArrayError::InputBufTooSmall(7)
            )
            .to_string(),
            io::Error::from(LogArrayError::InputBufTooSmall(7)).to_string()
        );
    }

    #[test]
    fn validate_input_buf_size() {
        let val = |buf_size| LogArrayError::validate_input_buf_size(buf_size);
        let err = |buf_size| Err(LogArrayError::InputBufTooSmall(buf_size));
        assert_eq!(err(7), val(7));
        assert_eq!(Ok(()), val(8));
        assert_eq!(Ok(()), val(9));
    }

    #[test]
    fn validate_len_and_width() {
        let val =
            |buf_size, len, width| LogArrayError::validate_len_and_width(buf_size, len, width);

        let err = |width| Err(LogArrayError::WidthTooLarge(width));

        // width: 65
        assert_eq!(err(65), val(0, 0, 65));

        let err = |buf_size, expected, len, width| {
            Err(LogArrayError::UnexpectedBufSize(
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

        // width: 5
        assert_eq!(err(16, 24, 13, 5), val(16, 13, 5));
        assert_eq!(Ok(()), val(24, 13, 5));
    }

    #[test]
    #[should_panic(expected = "expected value (8) to fit in 3 bits")]
    fn log_array_file_builder_panic() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 3);
        builder.push(8).wait().unwrap();
    }

    #[test]
    fn generate_then_parse_works() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        builder
            .push_all(stream::iter_ok(vec![1, 3, 2, 5, 12, 31, 18]))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();

        let logarray = LogArray::parse(&content).unwrap();

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

    #[test]
    #[should_panic(expected = "expected index (3) < length (3)")]
    fn entry_panic() {
        let mut content = Vec::new();
        content.extend_from_slice(&TEST0_DATA);
        content.extend_from_slice(&TEST0_CONTROL);
        let logarray = LogArray::parse(&content).unwrap();
        // Out of bounds
        let _ = logarray.entry(3);
    }

    #[test]
    #[should_panic(expected = "expected slice offset (2) + length (2) <= source length (3)")]
    fn slice_panic() {
        let mut content = Vec::new();
        content.extend_from_slice(&TEST0_DATA);
        content.extend_from_slice(&TEST0_CONTROL);
        let logarray = LogArray::parse(&content).unwrap();
        // Out of bounds
        let _ = logarray.slice(2, 2);
    }

    #[test]
    #[should_panic(expected = "expected slice index (2) < length (2)")]
    fn slice_entry_panic() {
        let mut content = Vec::new();
        content.extend_from_slice(&TEST0_DATA);
        content.extend_from_slice(&TEST0_CONTROL);
        let logarray = LogArray::parse(&content).unwrap();
        let logarray = logarray.slice(1, 2);
        // Out of bounds
        let _ = logarray.entry(2);
    }

    #[test]
    #[should_panic(expected = "not monotonic: expected predecessor (2) <= successor (1)")]
    fn monotonic_panic() {
        let content: [u8; 16] = [0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 32, 0, 0, 0];
        MonotonicLogArray::from_logarray(LogArray::parse(content).unwrap());
    }

    #[test]
    fn decode() {
        let mut decoder = LogArrayDecoder::new_unchecked(17, 1);
        let mut bytes = BytesMut::from(&TEST0_DATA as &[u8]);
        assert_eq!(Some(1), Decoder::decode(&mut decoder, &mut bytes).unwrap());
        assert_eq!(None, Decoder::decode(&mut decoder, &mut bytes).unwrap());
        decoder = LogArrayDecoder::new_unchecked(17, 4);
        bytes = BytesMut::from(&TEST0_DATA as &[u8]);
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
        let _ = tokio::io::write_all(store.open_write(), [0, 0, 0]).wait();
        assert_eq!(
            io::Error::from(LogArrayError::InputBufTooSmall(3)).to_string(),
            logarray_file_get_length_and_width(store)
                .wait()
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let _ = tokio::io::write_all(store.open_write(), [0, 0, 0, 0, 65, 0, 0, 0]).wait();
        assert_eq!(
            io::Error::from(LogArrayError::WidthTooLarge(65)).to_string(),
            logarray_file_get_length_and_width(store)
                .wait()
                .err()
                .unwrap()
                .to_string()
        );

        let store = MemoryBackedStore::new();
        let _ = tokio::io::write_all(store.open_write(), [0, 0, 0, 1, 17, 0, 0, 0]).wait();
        assert_eq!(
            io::Error::from(LogArrayError::UnexpectedBufSize(8, 16, 1, 17)).to_string(),
            logarray_file_get_length_and_width(store)
                .wait()
                .err()
                .unwrap()
                .to_string()
        );
    }

    #[test]
    fn generate_then_stream_works() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        builder
            .push_all(stream::iter_ok(0..31))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let entries: Vec<u64> = logarray_stream_entries(store).collect().wait().unwrap();
        let expected: Vec<u64> = (0..31).collect();
        assert_eq!(expected, entries);
    }

    #[test]
    fn iterate_over_logarray() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 2, 5, 12, 31, 18];
        builder
            .push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();

        let logarray = LogArray::parse(&content).unwrap();

        let result: Vec<u64> = logarray.iter().collect();

        assert_eq!(original, result);
    }

    #[test]
    fn owned_iterate_over_logarray() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 2, 5, 12, 31, 18];
        builder
            .push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();

        let logarray = LogArray::parse(&content).unwrap();

        let result: Vec<u64> = logarray.into_iter().collect();

        assert_eq!(original, result);
    }

    #[test]
    fn iterate_over_logarray_slice() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 2, 5, 12, 31, 18];
        builder
            .push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();

        let logarray = LogArray::parse(&content).unwrap();
        let slice = logarray.slice(2, 3);

        let result: Vec<u64> = slice.iter().collect();

        assert_eq!(vec![2, 5, 12], result);
    }

    #[test]
    fn owned_iterate_over_logarray_slice() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 2, 5, 12, 31, 18];
        builder
            .push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();

        let logarray = LogArray::parse(&content).unwrap();
        let slice = logarray.slice(2, 3);

        let result: Vec<u64> = slice.into_iter().collect();

        assert_eq!(vec![2, 5, 12], result);
    }

    #[test]
    fn monotonic_logarray_index_lookup() {
        let store = MemoryBackedStore::new();
        let builder = LogArrayFileBuilder::new(store.open_write(), 5);
        let original = vec![1, 3, 5, 6, 7, 10, 11, 15, 16, 18, 20, 25, 31];
        builder
            .push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();

        let logarray = LogArray::parse(&content).unwrap();
        let monotonic = MonotonicLogArray::from_logarray(logarray);

        for (i, &val) in original.iter().enumerate() {
            assert_eq!(i, monotonic.index_of(val).unwrap());
        }

        assert_eq!(None, monotonic.index_of(12));
        assert_eq!(original.len(), monotonic.len());
    }

    #[test]
    fn writing_64_bits_of_data() {
        let store = MemoryBackedStore::new();
        let original = vec![1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        let builder = LogArrayFileBuilder::new(store.open_write(), 4);
        builder
            .push_all(stream::iter_ok(original.clone()))
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        let content = store.map().wait().unwrap();
        let logarray = LogArray::parse(&content).unwrap();
        assert_eq!(original, logarray.iter().collect::<Vec<_>>());
        assert_eq!(16, logarray.len());
        assert_eq!(4, logarray.width());
    }
}
