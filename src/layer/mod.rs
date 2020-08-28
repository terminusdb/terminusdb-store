//! Logic for working with layers.
//!
//! Databases in terminus-store are stacks of layers. The first layer
//! in such a stack is a base layer, which contains an intial data
//! set. On top of that, each layer stores additions and removals.
mod internal;
mod base;
mod child;
mod builder;
mod simple_builder;
mod layer;

pub use internal::*;
pub use base::*;
pub use child::*;
pub use simple_builder::*;
pub use layer::*;
