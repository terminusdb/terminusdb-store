#![feature(test)]
extern crate test;
mod data;

use rand::prelude::*;
use tempfile::tempdir;
use terminus_store;
use terminus_store::storage::directory::NoFilenameEncoding;
use test::Bencher;

use data::*;

#[bench]
fn build_empty_base_layer(b: &mut Bencher) {
    let dir = tempdir().unwrap();
    let store = terminus_store::open_sync_directory_store(dir.path(), NoFilenameEncoding {});

    b.iter(|| {
        let builder = store.create_base_layer().unwrap();
        builder.commit().unwrap();
    });
}

#[bench]
fn build_base_layer_1000(b: &mut Bencher) {
    let dir = tempdir().unwrap();
    let store = terminus_store::open_sync_directory_store(dir.path(), NoFilenameEncoding {});

    let seed = b"the quick brown fox jumped over ";
    let rand = StdRng::from_seed(*seed);
    let mut data = TestData::new(rand, 100, 25, 500);

    let num_triples = 1000;
    let mut triples = Vec::with_capacity(num_triples);
    for _ in 0..num_triples {
        triples.push(data.random_triple());
    }
    b.iter(|| {
        let builder = store.create_base_layer().unwrap();

        for triple in triples.iter() {
            builder.add_value_triple(triple.clone()).unwrap();
        }

        let _base_layer = builder.commit().unwrap();
    });
}

#[bench]
fn build_empty_child_layer_on_empty_base_layer(b: &mut Bencher) {
    let dir = tempdir().unwrap();
    let store = terminus_store::open_sync_directory_store(dir.path(), NoFilenameEncoding {});
    let builder = store.create_base_layer().unwrap();
    let base_layer = builder.commit().unwrap();

    b.iter(|| {
        let builder = base_layer.open_write().unwrap();
        builder.commit().unwrap();
    });
}

#[bench]
fn build_nonempty_child_layer_on_empty_base_layer(b: &mut Bencher) {
    let dir = tempdir().unwrap();
    let store = terminus_store::open_sync_directory_store(dir.path(), NoFilenameEncoding {});
    let builder = store.create_base_layer().unwrap();
    let base_layer = builder.commit().unwrap();

    let seed = b"the quick brown fox jumped over ";
    let rand = StdRng::from_seed(*seed);
    let mut data = TestData::new(rand, 100, 25, 500);

    let num_triples = 1000;
    let mut triples = Vec::with_capacity(num_triples);
    for _ in 0..num_triples {
        triples.push(data.random_triple());
    }
    b.iter(move || {
        let builder = base_layer.open_write().unwrap();

        for triple in triples.iter() {
            builder.add_value_triple(triple.clone()).unwrap();
        }

        builder.commit().unwrap();
    });
}

#[bench]
fn build_nonempty_child_layer_on_nonempty_base_layer(b: &mut Bencher) {
    let dir = tempdir().unwrap();
    let store = terminus_store::open_sync_directory_store(dir.path(), NoFilenameEncoding {});

    let seed = b"the quick brown fox jumped over ";
    let rand = StdRng::from_seed(*seed);
    let mut data = TestData::new(rand, 100, 25, 500);

    let builder = store.create_base_layer().unwrap();

    for _ in 0..1000 {
        builder.add_value_triple(data.random_triple()).unwrap();
    }
    let base_layer = builder.commit().unwrap();

    let num_triples = 1000;
    let mut triples = Vec::with_capacity(num_triples);
    for _ in 0..num_triples {
        triples.push(data.random_triple());
    }
    b.iter(move || {
        let builder = base_layer.open_write().unwrap();

        for triple in triples.iter() {
            builder.add_value_triple(triple.clone()).unwrap();
        }

        builder.commit().unwrap();
    });
}
