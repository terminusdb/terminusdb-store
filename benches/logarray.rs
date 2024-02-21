#![feature(test)]
extern crate test;
use rand::prelude::*;
use tempfile::tempdir;
use test::Bencher;
use tokio::runtime::Runtime;

use tdb_succinct::util::stream_iter_ok;
use tdb_succinct::LogArrayFileBuilder;
use terminus_store::storage::directory::*;
use terminus_store::storage::memory::*;
use terminus_store::storage::*;

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
        let data = data.clone();
        rt.block_on(async move {
            let w = file.open_write().await.unwrap();
            let mut builder = LogArrayFileBuilder::new(w, width);
            if as_vec {
                builder.push_vec(data).await.unwrap();
            } else {
                builder.push_all(stream_iter_ok(data)).await.unwrap();
            }
            builder.finalize().await.unwrap();
        });
    });
}

fn logarray_test_persistent(b: &mut Bencher, width: u8, size: usize, as_vec: bool) {
    let rt = Runtime::new().unwrap();
    let seed = b"the quick brown fox jumped over ";
    let mut rand = StdRng::from_seed(*seed);

    let ceil = 1 << width;
    let mut data = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(rand.gen_range(0..ceil));
    }

    b.iter(move || {
        let dir = tempdir().unwrap();
        let file = FileBackedStore::new(dir.path().join("file"));
        let data = data.clone();
        rt.block_on(async move {
            let w = file.open_write().await.unwrap();
            let mut builder = LogArrayFileBuilder::new(w, width);
            if as_vec {
                builder.push_vec(data).await.unwrap();
            } else {
                builder.push_all(stream_iter_ok(data)).await.unwrap();
            }
            builder.finalize().await.unwrap();
        });
    });
}

#[bench]
fn logarray_w5_empty(b: &mut Bencher) {
    logarray_test(b, 5, 0, true);
}

#[bench]
fn logarray_w5_1(b: &mut Bencher) {
    logarray_test(b, 5, 1, true);
}

#[bench]
fn logarray_w5_10(b: &mut Bencher) {
    logarray_test(b, 5, 10, true);
}

#[bench]
fn logarray_w5_100(b: &mut Bencher) {
    logarray_test(b, 5, 100, true);
}

#[bench]
fn logarray_w5_1000(b: &mut Bencher) {
    logarray_test(b, 5, 1000, true);
}

#[bench]
fn logarray_w5_10000(b: &mut Bencher) {
    logarray_test(b, 5, 10000, true);
}

#[bench]
fn logarray_w5_10000_as_stream(b: &mut Bencher) {
    logarray_test(b, 5, 10000, false);
}

#[bench]
fn logarray_w5_10000_persistent(b: &mut Bencher) {
    logarray_test_persistent(b, 5, 10000, true);
}

#[bench]
fn logarray_w5_10000_persistent_as_stream(b: &mut Bencher) {
    logarray_test_persistent(b, 5, 10000, false);
}

#[bench]
fn logarray_w10_1000(b: &mut Bencher) {
    logarray_test(b, 10, 1000, true);
}
