use std::{io, ops::DerefMut, pin::Pin, task::Poll};

use bytes::Bytes;
use futures::{stream::Stream, Future, StreamExt};
use tokio::io::AsyncRead;

use num_traits::FromPrimitive;

use crate::structure::{block::SizedDictBlock, LogArrayError, MonotonicLogArray};

use super::{
    block::{OwnedSizedBlockIterator, SizedDictReaderError},
    Datatype, SizedDictEntry, TypedDictEntry,
};

enum StreamState<'a, R> {
    Intermediate,
    Start(R),
    ReadBlockEntries((OwnedSizedBlockIterator, R)),
    LoadingBlock(
        Pin<
            Box<
                dyn Future<Output = Result<(OwnedSizedBlockIterator, R), SizedDictReaderError>>
                    + 'a,
            >,
        >,
    ),
    Error,
}

pub struct TfcDictStream<'a, R> {
    state: StreamState<'a, R>,
}

impl<'a, R> TfcDictStream<'a, R> {
    pub fn new(reader: R) -> Self {
        Self {
            state: StreamState::Start(reader),
        }
    }

    pub fn into_inner(self) -> R {
        match self.state {
            StreamState::Start(r) => r,
            _ => panic!("tfc dict stream is not in a state where we can return the inner reader"),
        }
    }
}

async fn parse_single_tfc_block<R: AsyncRead + Unpin>(
    mut reader: R,
) -> Result<(OwnedSizedBlockIterator, R), SizedDictReaderError> {
    let block = SizedDictBlock::parse_from_reader(&mut reader).await?;
    Ok((block.into_iter(), reader))
}

impl<'a, R: AsyncRead + Unpin + 'a> Stream for TfcDictStream<'a, R> {
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

impl<'a, R> TfcTypedDictStream<'a, R> {
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

impl<'a, R: AsyncRead + Unpin + 'a> Stream for TfcTypedDictStream<'a, R> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::TryStreamExt;

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
}
