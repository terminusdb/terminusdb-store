//! Logic for working with layers
//!
//! Databases in terminus-store are stacks of layers. The first layer
//! in such a stack is a base layer, which contains an intial data
//! set. On top of that, each layer stores additions and removals.
pub mod layer;
pub mod base;
pub mod child;
pub mod builder;

pub use base::{BaseLayer, BaseLayerFiles};
pub use child::{ChildLayer, ChildLayerFiles};
pub use layer::{Layer,IdTriple,StringTriple,ObjectType,SubjectLookup,SubjectPredicateLookup};
pub use builder::{LayerBuilder,SimpleLayerBuilder};
