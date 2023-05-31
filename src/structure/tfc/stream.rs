use std::{io, ops::DerefMut, pin::Pin, task::Poll};

use futures::{stream::Stream, Future};
use tokio::io::AsyncRead;

use crate::structure::block::SizedDictBlock;

use super::{
    block::{OwnedSizedBlockIterator, SizedDictReaderError},
    SizedDictEntry,
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
    type Item = Result<SizedDictEntry, SizedDictReaderError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let x = self.deref_mut();
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
                        return Poll::Ready(Some(Ok(next)));
                    } else {
                        x.state = StreamState::Start(reader);
                        continue;
                    }
                }
                StreamState::LoadingBlock(mut f) => {
                    match Future::poll(f.as_mut(), cx) {
                        Poll::Ready(Ok((iter, reader))) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::TryStreamExt;

    use bytes::{Bytes, BytesMut};

    use crate::structure::StringDictBufBuilder;

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
        let result: Vec<_> = stream.map_ok(|e| e.to_bytes()).try_collect().await.unwrap();
        assert_eq!(input, result);
    }
}
