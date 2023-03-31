#![feature(test)]
extern crate test;

use tempfile::tempdir;
use terminus_store;
use terminus_store::layer::ValueTriple;
use terminus_store::storage::directory::NoFilenameEncoding;
use test::Bencher;

#[bench]
fn bench_add_string_triple(b: &mut Bencher) {
    let dir = tempdir().unwrap();
    let sync_store = terminus_store::open_sync_directory_store(dir.path(), NoFilenameEncoding);
    let layer_builder = sync_store.create_base_layer().unwrap();
    let mut count = 1;
    b.iter(|| {
        layer_builder
            .add_value_triple(ValueTriple::new_string_value(
                &count.to_string(),
                &count.to_string(),
                &count.to_string(),
            ))
            .unwrap();
        count += 1;
    });
}
