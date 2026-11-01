//! Benchmark: interval relation lookup — ordered-scan vs index probes.
//!
//! The "scan" column is what the query layer used to do: enumerate every
//! triple, decode its object, evaluate the Allen relation. The "indexed"
//! column resolves the relation to start/end boundary probes over the
//! ordered dictionary and the wavelet end index, then verifies candidates.
//!
//! Run: cargo run --release --example interval_relation_bench

use std::time::Instant;

use bytes::Bytes;
use tdb_succinct::tfc::block::SizedDictEntry;
use tdb_succinct::tfc::lexical_interval::{
    encode_lexical_interval, entry_relation_bounds, interval_relation, literal_as_interval,
    parse_lexical_interval, AllenRelation, LexicalInterval,
};
use tdb_succinct::{Datatype, TypedDictEntry};
use terminus_store::layer::{ObjectType, ValueTriple};
use terminus_store::store::sync::*;
use terminus_store::Layer;

const N: usize = 50_000;

fn iv_entry(s: &str) -> TypedDictEntry {
    TypedDictEntry::new(
        Datatype::DateTimeInterval,
        SizedDictEntry::from(Bytes::from(encode_lexical_interval(
            &parse_lexical_interval(s).unwrap(),
        ))),
    )
}

/// Scan truth: decode every triple's object and evaluate — the pre-index
/// query path.
fn scan_candidates(layer: &SyncStoreLayer, rel: AllenRelation, reference: &LexicalInterval) -> usize {
    let mut n = 0;
    for t in layer.triples() {
        if let Some(ObjectType::Value(e)) = layer.id_object(t.object) {
            if let Some(iv) = entry_relation_bounds(&e) {
                if interval_relation(&iv, rel, reference) {
                    n += 1;
                }
            }
        }
    }
    n
}

/// Indexed path: boundary probes to candidate ids, then the same decode +
/// verify on candidates only.
fn index_candidates(
    layer: &SyncStoreLayer,
    rel: AllenRelation,
    reference: &LexicalInterval,
) -> usize {
    let mut n = 0;
    for oid in layer.interval_relation_object_ids(rel, reference) {
        if let Some(ObjectType::Value(e)) = layer.id_object(oid) {
            if let Some(iv) = entry_relation_bounds(&e) {
                if interval_relation(&iv, rel, reference) {
                    n += 1;
                }
            }
        }
    }
    n
}

fn main() {
    let store = open_sync_memory_store();
    let builder = store.create_base_layer().unwrap();

    // 50k quarterly intervals, one triple each — 2020-01-01 + i*7d spans of
    // 30d, so ~8% of objects satisfy a season-window `during`.
    for i in 0..N {
        let start = chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .checked_add_days(chrono::Days::new((i * 7) as u64))
            .unwrap();
        let end = start.checked_add_days(chrono::Days::new(30)).unwrap();
        let s = format!(
            "{}T00:00:00Z/{}T00:00:00Z",
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        );
        builder
            .add_value_triple(ValueTriple {
                subject: format!("sub/{i}"),
                predicate: "period".to_string(),
                object: ObjectType::Value(iv_entry(&s)),
            })
            .unwrap();
    }
    let layer = builder.commit().unwrap();

    let cases: &[(&str, AllenRelation, &str)] = &[
        (
            "during season",
            AllenRelation::During,
            "2025-04-01T00:00:00Z/2025-07-01T00:00:00Z",
        ),
        (
            "meets season start",
            AllenRelation::Meets,
            "2025-04-01T00:00:00Z/2025-07-01T00:00:00Z",
        ),
        (
            "overlaps season",
            AllenRelation::Overlaps,
            "2025-04-01T00:00:00Z/2025-07-01T00:00:00Z",
        ),
        (
            "before season",
            AllenRelation::Before,
            "2025-04-01T00:00:00Z/2025-07-01T00:00:00Z",
        ),
        (
            "as-of point",
            AllenRelation::Contains,
            "2025-06-15T00:00:00Z",
        ),
    ];

    println!(
        "{:<18} {:>12} {:>12} {:>12} {:>8} {:>8}",
        "case", "scan", "probes", "idx+verify", "hits", "speedup"
    );
    for (name, rel, ref_str) in cases {
        let reference = literal_as_interval(ref_str).unwrap();
        let t = Instant::now();
        let scan = scan_candidates(&layer, *rel, &reference);
        let d_scan = t.elapsed();
        let t = Instant::now();
        let _cand = layer.interval_relation_object_ids(*rel, &reference);
        let d_probe = t.elapsed();
        let t = Instant::now();
        let idx = index_candidates(&layer, *rel, &reference);
        let d_idx = t.elapsed();
        assert_eq!(scan, idx, "{name}: index misses hits");
        println!(
            "{:<18} {:>12.1?} {:>12.1?} {:>12.1?} {:>8} {:>7.1}x",
            name,
            d_scan,
            d_probe,
            d_idx,
            idx,
            d_scan.as_secs_f64() / d_idx.as_secs_f64().max(1e-9)
        );
    }
}
