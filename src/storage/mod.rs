//! Logic for dealing with various storage backends
//!
//! Storage in terminus-store is set up in a generic way. Many data
//! structures simply rely on something that implements `FileLoad` and
//! `FileStore`, leaving the details of retrieval and storage to the
//! implementer.
//!
//! Two mechanisms are provided in this library:
//! - a memory backend
//! - a file backend
//!
//! Terminus-store stores databases as part of 2 data structures: a
//! layer store and a label store.
//!
//! A layer store is a set of directories (though it is up to a
//! specific implementation whether or not this is actually a
//! directory on a filesystem or some other mechanism). Each directory
//! is given a unique name of 20 bytes in hexadecimal format, and
//! stores the layer's primitive data structures as files inside that
//! directory.
//!
//! A label store is a set of files. The file name is of the format
//! `foo.label`, for database `foo`. This file contains the name of
//! the layer this label is pointing at.
pub mod consts;
pub mod file;
pub mod layer;
pub mod label;

pub use file::{FileLoad,FileStore,MemoryBackedStore,FileBackedStore};
pub use layer::{LayerRetriever,LayerStore,PersistentLayerStore,MemoryLayerStore,DirectoryLayerStore,CachedLayerStore};
pub use label::{Label, LabelStore, MemoryLabelStore, DirectoryLabelStore};
