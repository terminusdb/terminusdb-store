use futures::prelude::*;
use std::io::Error;
use tokio::io::AsyncWrite;
use futures::stream::{Peekable,Stream};
use byteorder::{ByteOrder,BigEndian};

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

pub fn write_nul_terminated_bytes<W: AsyncWrite>(
    w: W,
    bytes: Vec<u8>,
) -> impl Future<Item = (W, usize), Error = Error> {
    tokio::io::write_all(w, bytes).and_then(|(w, slice)| {
        let count = slice.len() + 1;
        tokio::io::write_all(w, [0]).map(move |(w, _)| (w, count))
    })
}

/// Write a buffer to `w`. Don't pass the buffer to the result.
pub fn write_all<W, B>(w: W, b: B) -> impl Future<Item = W, Error = Error>
where
    W: AsyncWrite,
    B: AsRef<[u8]>,
{
    tokio::io::write_all(w, b).map(|(w, _)| w)
}

/// Write a buffer to `w`.
pub fn write_padding<W: AsyncWrite>(
    w: W,
    current_pos: usize,
    width: u8,
) -> impl Future<Item = W, Error = Error> {
    let required_padding = (width as usize - current_pos % width as usize) % width as usize;
    write_all(w, vec![0; required_padding]) // there has to be a better way
}

/// Write a `u64` in big-endian order to `w`.
pub fn write_u64<W: AsyncWrite>(w: W, num: u64) -> impl Future<Item = W, Error = Error> {
    write_all(w, num.to_be_bytes())
}

struct SortedStream<T,E,S:'static+Stream<Item=T,Error=E>+Send, F:'static+Fn(&[Option<&T>])->Option<usize>> {
    streams: Vec<Peekable<S>>,
    pick_fn: F
}

impl<T,E,S:'static+Stream<Item=T,Error=E>+Send, F:'static+Fn(&[Option<&T>])->Option<usize>> Stream for SortedStream<T,E,S,F> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Result<Async<Option<T>>, E> {
        let mut v = Vec::with_capacity(self.streams.len());
        for s in self.streams.iter_mut() {
            match s.peek() {
                Ok(Async::Ready(val)) => v.push(val),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => return Err(e)
            }
        }

        let ix = (self.pick_fn)(&v[..]);

        match ix {
            None => Ok(Async::Ready(None)),
            Some(ix) => {
                let next = self.streams[ix].poll();
                match next {
                    Ok(Async::Ready(next)) => Ok(Async::Ready(next)),
                    _ => panic!("unexpected result in stream polling - reported ready earlier but not on later poll")
                }
            }
        }
    }
}

pub fn sorted_stream<T,E,S:'static+Stream<Item=T,Error=E>+Send, F:'static+Fn(&[Option<&T>])->Option<usize>>(streams: Vec<S>, pick_fn: F) -> impl Stream<Item=T,Error=E> {
    let peekable_streams = streams.into_iter().map(|s|s.peekable()).collect();
    SortedStream {
        streams: peekable_streams,
        pick_fn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_some_streams() {
        let v1 = vec![1,3,5,8,12];
        let v2 = vec![7,9,15];
        let v3 = vec![0,1,2,3,4];

        let streams = vec![futures::stream::iter_ok::<_,()>(v1),
                           futures::stream::iter_ok(v2),
                           futures::stream::iter_ok(v3)];


        let sorted = sorted_stream(streams, |results| results.iter()
                                   .enumerate()
                                   .filter(|&(_, item)| item.is_some())
                                   .min_by_key(|&(_, item)| item)
                                   .map(|x|x.0));

        let result = sorted.collect().wait().unwrap();

        assert_eq!(vec![0,1,1,2,3,3,4,5,7,8,9,12,15], result);
    }
}
