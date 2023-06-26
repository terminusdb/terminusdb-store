use futures::io::Result;
use futures::stream::{Peekable, Stream, StreamExt};
use futures::task::{Context, Poll};
use futures::TryStreamExt;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt;
use std::marker::{PhantomData, Unpin};
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

pub fn find_common_prefix_ord(b1: &[u8], b2: &[u8]) -> (usize, Ordering) {
    let common_prefix = find_common_prefix(b1, b2);

    if common_prefix == b1.len() && b1.len() == b2.len() {
        (common_prefix, Ordering::Equal)
    } else if b1.len() == common_prefix {
        (common_prefix, Ordering::Less)
    } else if b2.len() == common_prefix {
        (common_prefix, Ordering::Greater)
    } else {
        (common_prefix, b1[common_prefix].cmp(&b2[common_prefix]))
    }
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

pub struct HeapSortedIterator<'a, T: Ord, I: 'a + Iterator<Item = T> + Unpin + Send> {
    iters: Vec<I>,
    heap: BinaryHeap<(Reverse<T>, usize)>,
    _x: PhantomData<&'a ()>,
}

pub fn heap_sorted_iter<'a, T: Ord, I: 'a + Iterator<Item = T> + Unpin + Send>(
    mut iters: Vec<I>,
) -> HeapSortedIterator<'a, T, I> {
    let mut heap = BinaryHeap::with_capacity(iters.len());

    for (ix, i) in iters.iter_mut().enumerate() {
        if let Some(item) = i.next() {
            heap.push((Reverse(item), ix));
        }
    }

    HeapSortedIterator {
        iters,
        heap,
        _x: Default::default(),
    }
}

impl<'a, T: Ord + Unpin, I: 'a + Iterator<Item = T> + Unpin + Send> Iterator
    for HeapSortedIterator<'a, T, I>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ix) = self.heap.peek().map(|(_, ix)| *ix) {
            // we're about to pop an element from the heap. we'll need to read the next item in its corresponding stream to add to the heap afterwards.
            let iter = &mut self.iters[ix];
            match iter.next() {
                Some(next_item) => {
                    let item = self.heap.pop().unwrap();
                    self.heap.push((Reverse(next_item), ix));

                    Some(item.0 .0)
                }
                None => {
                    let item = self.heap.pop().unwrap();
                    Some(item.0 .0)
                }
            }
        } else {
            None
        }
    }
}

pub struct HeapSortedStream<
    'a,
    T: Ord,
    E,
    S: 'a + Stream<Item = std::result::Result<T, E>> + Unpin + Send,
> {
    streams: Vec<S>,
    heap: BinaryHeap<(Reverse<T>, usize)>,
    _x: PhantomData<&'a ()>,
}

pub async fn heap_sorted_stream<
    'a,
    T: Ord,
    E,
    S: 'a + Stream<Item = std::result::Result<T, E>> + Unpin + Send,
>(
    mut streams: Vec<S>,
) -> std::result::Result<HeapSortedStream<'a, T, E, S>, E> {
    let mut heap = BinaryHeap::with_capacity(streams.len());

    for (ix, s) in streams.iter_mut().enumerate() {
        if let Some(item) = s.try_next().await? {
            heap.push((Reverse(item), ix));
        }
    }

    Ok(HeapSortedStream {
        streams,
        heap,
        _x: Default::default(),
    })
}

impl<'a, T: Ord + Unpin, E, S: 'a + Stream<Item = std::result::Result<T, E>> + Unpin + Send> Stream
    for HeapSortedStream<'a, T, E, S>
{
    type Item = std::result::Result<T, E>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<std::result::Result<T, E>>> {
        let self_ = self.get_mut();
        if let Some(ix) = self_.heap.peek().map(|(_, ix)| *ix) {
            // we're about to pop an element from the heap. we'll need to read the next item in its corresponding stream to add to the heap afterwards.
            let stream = &mut self_.streams[ix];
            match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(Ok(next_item))) => {
                    let item = self_.heap.pop().unwrap();
                    self_.heap.push((Reverse(next_item), ix));

                    Poll::Ready(Some(Ok(item.0 .0)))
                }
                Poll::Ready(None) => {
                    let item = self_.heap.pop().unwrap();
                    Poll::Ready(Some(Ok(item.0 .0)))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct SortedStream<
    'a,
    T,
    S: 'a + Stream<Item = T> + Unpin + Send,
    F: 'a + Fn(&[Option<&T>]) -> Option<usize>,
> {
    streams: Vec<Peekable<S>>,
    pick_fn: F,
    _x: PhantomData<&'a ()>,
}

impl<
        'a,
        T,
        S: 'a + Stream<Item = T> + Unpin + Send,
        F: 'a + Fn(&[Option<&T>]) -> Option<usize> + Unpin,
    > Stream for SortedStream<'a, T, S, F>
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
    'a,
    T: 'a,
    S: 'a + Stream<Item = T> + Unpin + Send,
    F: 'a + Fn(&[Option<&T>]) -> Option<usize> + Unpin,
>(
    streams: Vec<S>,
    pick_fn: F,
) -> SortedStream<'a, T, S, F> {
    let peekable_streams = streams.into_iter().map(|s| s.peekable()).collect();
    SortedStream {
        streams: peekable_streams,
        pick_fn,
        _x: Default::default(),
    }
}

pub fn compare_or_result<T: Ord, E: fmt::Debug>(
    r1: &std::result::Result<T, E>,
    r2: &std::result::Result<T, E>,
) -> Ordering {
    if r1.is_err() {
        if r2.is_err() {
            Ordering::Equal
        } else {
            Ordering::Less
        }
    } else if r2.is_err() {
        Ordering::Greater
    } else {
        r1.as_ref().unwrap().cmp(r2.as_ref().unwrap())
    }
}

struct SortedIterator<
    T,
    I: Iterator<Item = T> + Send,
    F: 'static + Fn(&[Option<&T>]) -> Option<usize>,
> {
    iters: Vec<std::iter::Peekable<I>>,
    pick_fn: F,
}

impl<'a, T, I: 'a + Iterator<Item = T> + Send, F: 'static + Fn(&[Option<&T>]) -> Option<usize>>
    Iterator for SortedIterator<T, I, F>
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
    'a,
    T: 'a,
    I: 'a + Iterator<Item = T> + Send,
    F: 'static + Fn(&[Option<&T>]) -> Option<usize>,
>(
    iters: Vec<I>,
    pick_fn: F,
) -> impl Iterator<Item = T> + 'a {
    let peekable_iters = iters
        .into_iter()
        .map(std::iter::Iterator::peekable)
        .collect();
    SortedIterator {
        iters: peekable_iters,
        pick_fn,
    }
}

pub fn stream_iter_ok<T, E, I: IntoIterator<Item = T>>(
    iter: I,
) -> impl Stream<Item = std::result::Result<T, E>> {
    futures::stream::iter(iter).map(Ok::<T, E>)
}

pub fn assert_poll_next<T, S: Stream<Item = T>>(stream: Pin<&mut S>, cx: &mut Context) -> T {
    match stream.poll_next(cx) {
        Poll::Ready(Some(item)) => item,
        _ => panic!("stream was expected to have a result but did not."),
    }
}

pub fn calculate_width(size: u64) -> u8 {
    let mut msb = u64::BITS - size.leading_zeros();
    // zero is a degenerate case, but needs to be represented with one bit.
    if msb == 0 {
        msb = 1
    };
    msb as u8
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
