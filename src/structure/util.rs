use futures::io::Result;
use futures::stream::{Peekable, Stream, StreamExt};
use futures::task::{Context, Poll};
use std::marker::Unpin;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub fn find_common_prefix(b1: &[u8], b2: &[u8]) -> usize {
    let mut common = 0;
    while common < b1.len() && common < b2.len() {
        if b1[common] == b2[common] {
            common += 1;
        } else {
            break;
        }
    }

    common
}

pub async fn write_nul_terminated_bytes<W: AsyncWrite + Unpin>(
    w: &mut W,
    bytes: &[u8],
) -> Result<usize> {
    w.write_all(&bytes).await?;
    w.write_all(&[0]).await?;

    let count = bytes.len() + 1;

    Ok(count)
}

/// Write a buffer to `w`.
pub async fn write_padding<W: AsyncWrite + Unpin>(
    w: &mut W,
    current_pos: usize,
    width: u8,
) -> Result<()> {
    let required_padding = (width as usize - current_pos % width as usize) % width as usize;
    w.write_all(&vec![0; required_padding]).await?;

    Ok(())
}

/// Write a `u64` in big-endian order to `w`.
pub async fn write_u64<W: AsyncWrite + Unpin>(w: &mut W, num: u64) -> Result<()> {
    w.write_all(&num.to_be_bytes()).await?;

    Ok(())
}

struct SortedStream<
    T,
    S: 'static + Stream<Item = T> + Unpin + Send,
    F: 'static + Fn(&[Option<&T>]) -> Option<usize>,
> {
    streams: Vec<Peekable<S>>,
    pick_fn: F,
}

impl<
        T,
        S: 'static + Stream<Item = T> + Unpin + Send,
        F: 'static + Fn(&[Option<&T>]) -> Option<usize> + Unpin,
    > Stream for SortedStream<T, S, F>
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<T>> {
        let mut v = Vec::with_capacity(self.streams.len());
        let self_ = self.get_mut();
        for s in self_.streams.iter_mut() {
            match Pin::new(s).poll_peek(cx) {
                Poll::Ready(val) => v.push(val),
                Poll::Pending => return Poll::Pending,
            }
        }

        let ix = (self_.pick_fn)(&v[..]);

        match ix {
            None => Poll::Ready(None),
            Some(ix) => {
                let next = Pin::new(&mut self_.streams[ix]).poll_next(cx);
                match next {
                    Poll::Ready(next) => Poll::Ready(next),
                    _ => panic!("unexpected result in stream polling - reported ready earlier but not on later poll")
                }
            }
        }
    }
}

pub fn sorted_stream<
    T,
    S: 'static + Stream<Item = T> + Unpin + Send,
    F: 'static + Fn(&[Option<&T>]) -> Option<usize> + Unpin,
>(
    streams: Vec<S>,
    pick_fn: F,
) -> impl Stream<Item = T> {
    let peekable_streams = streams.into_iter().map(|s| s.peekable()).collect();
    SortedStream {
        streams: peekable_streams,
        pick_fn,
    }
}

struct SortedIterator<
    T,
    I: 'static + Iterator<Item = T> + Send,
    F: 'static + Fn(&[Option<&T>]) -> Option<usize>,
> {
    iters: Vec<std::iter::Peekable<I>>,
    pick_fn: F,
}

impl<
        T,
        I: 'static + Iterator<Item = T> + Send,
        F: 'static + Fn(&[Option<&T>]) -> Option<usize>,
    > Iterator for SortedIterator<T, I, F>
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let mut v = Vec::with_capacity(self.iters.len());
        for s in self.iters.iter_mut() {
            v.push(s.peek());
        }

        let ix = (self.pick_fn)(&v[..]);

        match ix {
            None => None,
            Some(ix) => self.iters[ix].next(),
        }
    }
}

pub fn sorted_iterator<
    T,
    I: 'static + Iterator<Item = T> + Send,
    F: 'static + Fn(&[Option<&T>]) -> Option<usize>,
>(
    iters: Vec<I>,
    pick_fn: F,
) -> impl Iterator<Item = T> {
    let peekable_iters = iters
        .into_iter()
        .map(|s| std::iter::Iterator::peekable(s))
        .collect();
    SortedIterator {
        iters: peekable_iters,
        pick_fn,
    }
}

pub fn stream_iter_ok<T, E, I: IntoIterator<Item = T>>(
    iter: I,
) -> impl Stream<Item = std::result::Result<T, E>> {
    futures::stream::iter(iter).map(|x| Ok::<T, E>(x))
}

pub fn assert_poll_next<T, S: Stream<Item = T>>(stream: Pin<&mut S>, cx: &mut Context) -> T {
    match stream.poll_next(cx) {
        Poll::Ready(Some(item)) => item,
        _ => panic!("stream was expected to have a result but did not."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn sort_some_streams() {
        let v1 = vec![1, 3, 5, 8, 12];
        let v2 = vec![7, 9, 15];
        let v3 = vec![0, 1, 2, 3, 4];

        let streams = vec![
            futures::stream::iter(v1),
            futures::stream::iter(v2),
            futures::stream::iter(v3),
        ];

        let sorted = sorted_stream(streams, |results| {
            results
                .iter()
                .enumerate()
                .filter(|&(_, item)| item.is_some())
                .min_by_key(|&(_, item)| item)
                .map(|x| x.0)
        });

        let result: Vec<_> = block_on(sorted.collect());

        assert_eq!(vec![0, 1, 1, 2, 3, 3, 4, 5, 7, 8, 9, 12, 15], result);
    }
}
