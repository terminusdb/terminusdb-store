//! Logic for working with layers.
//!
//! Databases in terminus-store are stacks of layers. The first layer
//! in such a stack is a base layer, which contains an intial data
//! set. On top of that, each layer stores additions and removals.
mod internal;
mod base;
mod builder;
mod child;
mod layer;

pub use internal::*;
pub use base::*;
pub use builder::*;
pub use child::*;
pub use layer::*;
