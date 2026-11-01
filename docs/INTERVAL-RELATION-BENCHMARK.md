# Interval relation benchmark: scan vs indexed candidates

How the wavelet end index and the ordered start dictionary change the cost of
Allen-relation queries over `xdd:dateTimeInterval` objects.

## Method

`examples/interval_relation_bench.rs` builds a memory store with 50,000 triples,
one interval object each: 30-day spans starting weekly from 2020-01-01. Two code
paths answer the same relation query:

- **scan** — enumerate every triple, decode its object, evaluate the Allen
  relation on components. This was the original query path.
- **indexed** — resolve the relation to boundary probes
  (`interval_ids_start_*` over the ordered dictionary, `positions_end_*` over
  the wavelet end index), intersect to a candidate id set, then run the same
  decode + verify on candidates only.

Both paths go through `SyncStoreLayer`; the indexed path is what
`interval_relation_object_ids` / `triples_interval_relation` now do. Run with:

```bash
cargo run --release --example interval_relation_bench
```

## Results

Reference intervals are season windows (e.g.
`2025-04-01T00:00:00Z/2025-07-01T00:00:00Z`) and a single instant for the as-of
query.

| relation          | scan      | indexed     | speedup        |
|-------------------|-----------|-------------|----------------|
| during season     | ~54–55 ms | ~0.32 ms    | ~165–173×      |
| meets season start| ~54–55 ms | <1 µs       | ~86,000–94,000×|
| overlaps season   | ~53–55 ms | ~0.021 ms   | ~2,500–2,600×  |
| before season     | ~53–56 ms | ~0.58 ms    | ~90–96×        |
| as-of point       | ~53–55 ms | ~0.09 ms    | ~594×          |

Scan cost is flat (~55 ms): it decodes all 50k objects regardless of
selectivity. Indexed cost tracks the size of the boundary probes, not the
object count — `meets` resolves to one end-equality lookup, `during` to a start
range plus an end lower bound.

## What made `as-of` fast

The as-of query (`contains` on a zero-duration reference) needs ends above a
lower bound: `positions_end_ge(pos, bound)`. The first implementation widened
that to `positions_in_range(pos, bound, u64::MAX)`, whose broad-range branch
decoded the whole end sequence and ran one dictionary lookup per distinct
value — ~68 ms, slower than scan. `WaveletTree::positions_in_range` now decodes
positions directly over the requested position window instead: no
allocations, no per-value lookups. That moved as-of from 68 ms to ~0.09 ms.

## Caveats

- Candidates are a **superset**. The index can only prove bounds, so every
  candidate is decoded and re-checked with the exact evaluator
  (`interval_relation` in Rust, `woql_interval_relation_typed` in WOQL). The
  superset property is asserted by a differential test over all 13 relations,
  point literals, open/unknown/unspecified endpoints, and child layers.
- Numbers are wall-clock on one machine (memory store, release build). The
  shape — probe cost instead of scan cost — is the point, not the constants.
- File-backed layers behave the same: the end index is written at commit and
  read back at open; the seam test in
  `interval_relation_tests` covers the directory-store path.
