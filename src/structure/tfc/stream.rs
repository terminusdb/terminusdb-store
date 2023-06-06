use std::{io, ops::DerefMut, pin::Pin, task::Poll};

use bytes::Bytes;
use futures::{stream::Stream, Future, StreamExt};
use tokio::io::{AsyncRead, BufReader, ReadBuf};

use num_traits::FromPrimitive;

use crate::structure::{block::SizedDictBlock, LogArrayError, MonotonicLogArray};

use super::{
    block::{OwnedSizedBlockIterator, SizedDictReaderError},
    Datatype, SizedDictEntry, TypedDictEntry,
};

type StreamStateReader<R> = BufReader<DontReadLastU64Reader<R>>;

enum StreamState<'a, R> {
    Intermediate,
    Start(StreamStateReader<R>),
    ReadBlockEntries((OwnedSizedBlockIterator, StreamStateReader<R>)),
    LoadingBlock(
        Pin<
            Box<
                dyn Future<
                        Output = Result<
                            (OwnedSizedBlockIterator, StreamStateReader<R>),
                            SizedDictReaderError,
                        >,
                    > + Send
                    + 'a,
            >,
        >,
    ),
    Error,
}

pub struct TfcDictStream<'a, R> {
    state: StreamState<'a, R>,
}

impl<'a, R: AsyncRead + Unpin> TfcDictStream<'a, R> {
    pub fn new(reader: R) -> Self {
        Self {
            state: StreamState::Start(BufReader::new(DontReadLastU64Reader::new(reader))),
        }
    }

    /*
    pub fn into_inner(self) -> R {
        match self.state {
            StreamState::Start(r) => r.inner,
            _ => panic!("tfc dict stream is not in a state where we can return the inner reader"),
        }
    }
    */
}

async fn parse_single_tfc_block<R: AsyncRead + Unpin>(
    mut reader: R,
) -> Result<(OwnedSizedBlockIterator, R), SizedDictReaderError> {
    let block = SizedDictBlock::parse_from_reader(&mut reader).await?;
    Ok((block.into_iter(), reader))
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> Stream for TfcDictStream<'a, R> {
    type Item = Result<(SizedDictEntry, bool), SizedDictReaderError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let x = self.deref_mut();
        let mut start_of_block = false;
        loop {
            let mut state = StreamState::Intermediate;
            std::mem::swap(&mut state, &mut x.state);
            match state {
                StreamState::Start(reader) => {
                    let future = Box::pin(parse_single_tfc_block(reader));
                    x.state = StreamState::LoadingBlock(future);
                    continue;
                }
                StreamState::ReadBlockEntries((mut iter, reader)) => {
                    if let Some(next) = iter.next() {
                        x.state = StreamState::ReadBlockEntries((iter, reader));
                        return Poll::Ready(Some(Ok((next, start_of_block))));
                    } else {
                        x.state = StreamState::Start(reader);
                        continue;
                    }
                }
                StreamState::LoadingBlock(mut f) => {
                    match Future::poll(f.as_mut(), cx) {
                        Poll::Ready(Ok((iter, reader))) => {
                            start_of_block = true;
                            x.state = StreamState::ReadBlockEntries((iter, reader));
                            continue;
                        }
                        Poll::Ready(Err(e)) => {
                            // check eof
                            if e.is_unexpected_eof() {
                                return Poll::Ready(None);
                            }
                            x.state = StreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => {
                            x.state = StreamState::LoadingBlock(f);
                            return Poll::Pending;
                        }
                    }
                }
                StreamState::Error => {
                    x.state = StreamState::Error;
                    return Poll::Ready(Some(Err(SizedDictReaderError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "stream returned error on previous poll",
                    )))));
                }
                StreamState::Intermediate => panic!("intermediate state should never be reached"),
            }
        }
    }
}

pub struct TfcTypedDictStream<'a, R> {
    inner: TfcDictStream<'a, R>,
    types_present: MonotonicLogArray,
    type_offsets: MonotonicLogArray,
    block_index: usize,
    offset: usize,
}

impl<'a, R: AsyncRead + Unpin> TfcTypedDictStream<'a, R> {
    pub fn from_parts(
        blocks_reader: R,
        types_present: MonotonicLogArray,
        type_offsets: MonotonicLogArray,
    ) -> Self {
        Self {
            inner: TfcDictStream::new(blocks_reader),
            types_present,
            type_offsets,
            block_index: 0,
            offset: 0,
        }
    }

    pub fn new(
        blocks_reader: R,
        types_present_bytes: Bytes,
        type_offsets_bytes: Bytes,
    ) -> Result<Self, LogArrayError> {
        let types_present = MonotonicLogArray::parse(types_present_bytes)?;
        let type_offsets = MonotonicLogArray::parse(type_offsets_bytes)?;

        Ok(Self::from_parts(blocks_reader, types_present, type_offsets))
    }
}

impl<'a, R: AsyncRead + Unpin + Send + 'a> Stream for TfcTypedDictStream<'a, R> {
    type Item = Result<TypedDictEntry, SizedDictReaderError>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let x = self.deref_mut();
        match x.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok((d, b)))) => {
                if b {
                    // we could have hit a type boundary. let's check
                    if x.block_index != 0
                        && x.offset < x.type_offsets.len()
                        && x.block_index as u64 == x.type_offsets.entry(x.offset) + 1
                    {
                        x.offset += 1;
                    }
                    x.block_index += 1;
                }
                let data_type = Datatype::from_u64(x.types_present.entry(x.offset)).unwrap();

                Poll::Ready(Some(Ok(TypedDictEntry::new(data_type, d))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct DontReadLastU64Reader<R> {
    inner: R,
    buf: [u8; 8],
    already_read: usize,
}

impl<R> DontReadLastU64Reader<R> {
    pub fn new(r: R) -> Self {
        Self {
            inner: r,
            buf: [0; 8],
            already_read: 0,
        }
    }
}

impl<R: AsyncRead + Unpin> DontReadLastU64Reader<R> {
    /// This will read data but ensure that the last 8 bytes are not
    /// immedately returned. If they end up being the last 8 bytes
    /// before the real end of the buffer, they will never be
    /// returned.
    fn poll_read_with_buf(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let start_len = buf.filled().len();
        if self.already_read > 0 {
            buf.put_slice(&self.buf[..self.already_read]);
        }
        loop {
            let before_read_len = buf.filled().len();
            let result = AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf);
            let filled = buf.filled();
            let total_read_len = filled.len() - start_len;
            let step_read_len = filled.len() - before_read_len;
            match result {
                Poll::Ready(Ok(())) => {
                    if step_read_len == 0 {
                        // We reached the end of the underlying stream.
                        // Just return here.
                        self.already_read = std::cmp::min(total_read_len, 8);
                        self.buf[..self.already_read].copy_from_slice(&filled[start_len..]);
                        buf.set_filled(start_len);
                        return Poll::Ready(Ok(()));
                    } else if total_read_len <= 8 {
                        // we don't yet know if this is a control word. More has to be read.
                        continue;
                    } else {
                        // we have read enough! Take off the end as our new potential control word
                        self.already_read = std::cmp::min(total_read_len, 8);
                        self.buf
                            .copy_from_slice(&filled[filled.len() - self.already_read..]);
                        buf.set_filled(filled.len() - 8);
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Pending => {
                    if total_read_len != 0 {
                        // we filled up something, but it was not
                        // enough to return before we hit a pending on
                        // the underlying stream.

                        // no idea when we'll be back. save our work
                        self.already_read = std::cmp::min(total_read_len, 8);
                        self.buf[..self.already_read].copy_from_slice(&filled[start_len..]);
                        buf.set_filled(start_len);
                    }

                    return Poll::Pending;
                }
                _ => return result,
            }
        }
    }
}

/*
impl<R:AsyncRead+Unpin> AsyncRead for DontReadLastU64Reader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf)
    }
}
*/

impl<R: AsyncRead + Unpin> AsyncRead for DontReadLastU64Reader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() > 8 {
            // this buffer is good enough for us to just read to directly.
            // read however much, and if the result is more than 8, we got ourselves a result.
            // if not, store the result in our own buf for use later.
            self.poll_read_with_buf(cx, buf)
        } else {
            // This is a cute tiny buffer that needs special treatment.
            // We can't read into it directly cause the control world will take up all available space.
            // Instead, we're gonna have to read into another buffer and then copy over to this one
            let mut inner_data = vec![0; buf.remaining() + 8];
            let mut inner_buf = ReadBuf::new(&mut inner_data);
            match self.poll_read_with_buf(cx, &mut inner_buf) {
                Poll::Ready(Ok(())) => {
                    // copy over result
                    let filled = inner_buf.filled();
                    assert!(filled.len() <= buf.remaining());
                    buf.put_slice(filled);

                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::TryStreamExt;
    use tokio::io::AsyncReadExt;

    use bytes::{Bytes, BytesMut};

    use crate::structure::{StringDictBufBuilder, TdbDataType, TypedDictBufBuilder};

    #[tokio::test]
    async fn stream_a_dict() {
        let mut builder = StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());
        let input = vec![
            Bytes::from("aaa".to_string()),
            Bytes::from("aab".to_string()),
            Bytes::from("aac".to_string()),
            Bytes::from("aad".to_string()),
            Bytes::from("aba".to_string()),
            Bytes::from("abb".to_string()),
            Bytes::from("abc".to_string()),
            Bytes::from("abd".to_string()),
            Bytes::from("baa".to_string()),
            Bytes::from("bab".to_string()),
        ];
        builder.add_all(input.iter().cloned());

        let (_, data) = builder.finalize();

        let stream = TfcDictStream::new(data.as_ref());
        let result: Vec<_> = stream.try_collect().await.unwrap();
        let boundary_result: Vec<bool> = result.iter().map(|(_, b)| *b).collect();
        let data_result: Vec<Bytes> = result.into_iter().map(|(e, _)| e.to_bytes()).collect();
        assert_eq!(input, data_result);
        assert_eq!(
            vec![true, false, false, false, false, false, false, false, true, false],
            boundary_result
        );
    }

    async fn typed_dict_test(mut input: Vec<TypedDictEntry>) {
        input.sort();

        let mut builder = TypedDictBufBuilder::new(
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
        );

        builder.add_all(input.iter().cloned());
        let (types_present, type_offsets, _, data) = builder.finalize();
        let stream =
            TfcTypedDictStream::new(data.as_ref(), types_present.freeze(), type_offsets.freeze())
                .unwrap();
        let result: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(input, result);
    }

    #[tokio::test]
    async fn test_a_typed_dict() {
        let input = vec![
            String::make_entry(&"a fun string"),
            String::make_entry(&"a fun string2"),
            String::make_entry(&"a fun string3"),
            String::make_entry(&"a fun string4"),
            String::make_entry(&"a fun string5"),
            String::make_entry(&"a fun string6"),
            String::make_entry(&"a fun string7"),
            String::make_entry(&"a fun string8"),
            String::make_entry(&"a fun string9"),
            u32::make_entry(&25),
            u32::make_entry(&42),
            u32::make_entry(&65),
            u32::make_entry(&66),
            u32::make_entry(&67),
            u32::make_entry(&68),
            u32::make_entry(&69),
            u32::make_entry(&75),
            u32::make_entry(&85),
            f64::make_entry(&3.1415),
        ];

        typed_dict_test(input).await;
    }

    #[tokio::test]
    async fn single_element_typed_dict() {
        let input = vec![String::make_entry(&"a fun string")];

        typed_dict_test(input).await;
    }

    #[tokio::test]
    async fn empty_typed_dict() {
        let input = vec![];

        typed_dict_test(input).await;
    }

    #[tokio::test]
    async fn read_small_buf() {
        let data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut reader = DontReadLastU64Reader::new(data.as_ref());
        assert_eq!(0, reader.read_u8().await.unwrap());
        assert_eq!(1, reader.read_u8().await.unwrap());
        assert_eq!(2, reader.read_u8().await.unwrap());
        assert_eq!(
            io::ErrorKind::UnexpectedEof,
            reader.read_u8().await.err().unwrap().kind()
        );
    }

    #[tokio::test]
    async fn read_large_buf() {
        let data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut reader = DontReadLastU64Reader::new(data.as_ref());
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        assert_eq!(vec![0, 1, 2], output);
    }
}
