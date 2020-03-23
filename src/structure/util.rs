use futures::prelude::*;
use std::io::Error;
use tokio::io::AsyncWrite;

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
