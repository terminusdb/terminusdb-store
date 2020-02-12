//! Data structures on which terminus-store is built.
//!
//! This module contains various succinct data structures, as well as
//! the logic to load, parse and store them.

mod util;

pub mod adjacencylist;
pub mod bitarray;
pub mod bitindex;
pub mod logarray;
pub mod pfc;
pub mod vbyte;
pub mod wavelettree;

pub use adjacencylist::*;
pub use bitarray::*;
pub use bitindex::*;
pub use logarray::*;
pub use pfc::*;
pub use wavelettree::*;
