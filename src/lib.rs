//! Tokio-enabled data store for triple data
//!
//! This library implements a way to store triple data - data that
//! consists of a subject, predicate and an object, where object can
//! either be some value, or a node (a string that can appear both in
//! subject and object position).
//!
//! This library is intended as a common base for anyone who wishes to
//! build a database containing triple data. It makes very few
//! assumptions on what valid data is, only focusing on the actual
//! storage aspect.
//!
//! This library is tokio-enabled. Any i/o and locking happens through
//! futures, and as a result, many of the functions in this library
//! return futures. These futures are intended to run on a tokio
//! runtime, and many of them will fail outside of one. If you do not
//! wish to use tokio, there's a small sync wrapper in `store::sync`
//! which embeds its own tokio runtime, exposing a purely synchronous
//! API.
//!
//! Most users will probably only need to use the types and functions
//! in the `store` module (or `store::sync` for the synchronous
//! version). This module provides a high-level API which should be
//! sufficient for creating and querying databases.
//!
//! The `structure`, `layer`, and `storage` module expose the inner
//! workings of terminus-store. They are useful for implementing new
//! storage backends, or writing analysis and recovery tools.
#[macro_use]
extern crate lazy_static;

pub mod layer;
pub mod storage;
pub mod structure;

pub mod store;

pub use layer::Layer;
pub use store::sync::{open_sync_directory_store, open_sync_memory_store};
pub use store::{open_directory_store, open_memory_store};
