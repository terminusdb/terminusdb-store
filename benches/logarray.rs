#![feature(test)]
extern crate test;
use rand::prelude::*;
use test::Bencher;
use tokio::runtime::Runtime;

use terminus_store::storage::memory::*;
use terminus_store::storage::*;
use terminus_store::structure::util::stream_iter_ok;
use terminus_store::structure::LogArrayFileBuilder;

fn logarray_test(b: &mut Bencher, width: u8, size: usize, as_vec: bool) {
    let rt = Runtime::new().unwrap();
    let seed = b"the quick brown fox jumped over ";
    let mut rand = StdRng::from_seed(*seed);

    let ceil = 1 << width;
    let mut data = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(rand.gen_range(0..ceil));
    }

    b.iter(move || {
        let file = MemoryBackedStore::new();
        let w = file.open_write();
        let mut builder = LogArrayFileBuilder::new(w, width);
        let data = data.clone();
        rt.block_on(async move {
            if as_vec {
                builder.push_all_vec(data).await.unwrap();
            } else {
                builder.push_all(stream_iter_ok(data)).await.unwrap();
            }
            builder.finalize().await.unwrap();
        });
    });
}

#[bench]
fn logarray_w5_empty(b: &mut Bencher) {
    logarray_test(b, 5, 0, false);
}

#[bench]
fn logarray_w5_1(b: &mut Bencher) {
    logarray_test(b, 5, 1, false);
}

#[bench]
fn logarray_w5_10(b: &mut Bencher) {
    logarray_test(b, 5, 10, false);
}

#[bench]
fn logarray_w5_100(b: &mut Bencher) {
    logarray_test(b, 5, 100, false);
}

#[bench]
fn logarray_w5_1000(b: &mut Bencher) {
    logarray_test(b, 5, 1000, false);
}

#[bench]
fn logarray_w5_10000(b: &mut Bencher) {
    logarray_test(b, 5, 10000, false);
}

#[bench]
fn logarray_w5_10000_as_vec(b: &mut Bencher) {
    logarray_test(b, 5, 10000, true);
}

#[bench]
fn logarray_w10_1000(b: &mut Bencher) {
    logarray_test(b, 10, 1000, false);
}
