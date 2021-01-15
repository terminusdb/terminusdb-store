# terminusdb-store, a tokio-enabled data store for triple data

[![Build Status](https://github.com/terminusdb/terminusdb-store/workflows/Build/badge.svg)](https://github.com/terminusdb/terminusdb-store/actions)
[![Crate](https://img.shields.io/crates/v/terminus-store.svg)](https://crates.io/crates/terminus-store")
[![Documentation](https://docs.rs/terminus-store/badge.svg)](https://docs.rs/terminus-store/)
[![codecov](https://codecov.io/gh/terminusdb/terminus-store/branch/master/graph/badge.svg)](https://codecov.io/gh/terminusdb/terminus-store)

## Overview
This library implements a way to store triple data - data that
consists of a subject, predicate and an object, where object can
either be some value, or a node (a string that can appear both in
subject and object position).

An example of triple data is:
````
cow says value(moo).
duck says value(quack).
cow likes node(duck).
duck hates node(cow).
````
In `cow says value(moo)`, `cow` is the subject, `says` is the
predicate, and `value(moo)` is the object.

In `cow likes node(duck)`, `cow` is the subject, `likes` is the
predicate, and `node(duck)` is the object.

terminusdb-store allows you to store a lot of such facts, and search
through them efficiently.

This library is intended as a common base for anyone who wishes to
build a database containing triple data. It makes very few assumptions
on what valid data is, only focusing on the actual storage aspect.

This library is tokio-enabled. Any i/o and locking happens through
futures, and as a result, many of the functions in this library return
futures. These futures are intended to run on a tokio runtime, and
many of them will fail outside of one. If you do not wish to use
tokio, there's a small sync wrapper in `store::sync` which embeds its
own tokio runtime, exposing a purely synchronous API.

## Usage
Add this to your `Cargo.toml`:

```toml
[dependencies]
terminus-store = "0.12"
```

create a directory where you want the store to be, then open that store with
```rust
let future = terminus_store::open_directory_store("/path/to/store");
```

Or use the sync wrapper:
```rust
let store = terminus_store::open_sync_directory_store("/path/to/store").unwrap();
```

For more information, [visit the documentation on docs.rs](https://docs.rs/terminus-store/).

## Roadmap

We are constantly developing terminusdb-store to make it a high quality succinct graph representation versioned datastorage layer. To help facilitate understanding of our aims for this project we have laid out a [Roadmap](./docs/ROADMAP.md). If you would like to assist in the development of terminusdb-store, or you think something should be added to the roadmap please contact us.

## License
terminus-store is licensed under Apache 2.0.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)

## See also
- The Terminus database, for which this library was written: [Website](https://terminusdb.com) - [GitHub](https://github.com/terminusdb/)
- Our prolog bindings for this library: [terminus_store_prolog](https://github.com/terminusdb/terminus_store_prolog/)
- The HDT format, which the terminusdb-store layer format is based on: [Website](http://www.rdfhdt.org/)
