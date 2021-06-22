use super::*;
use std::sync::Arc;

#[derive(Clone)]
pub struct RollupLayer {
    pub(super) internal: Arc<InternalLayer>,
    pub(super) original: [u32; 5],
    pub(super) original_parent: Option<[u32; 5]>, // TODO something with a light delta structure for answering delta queries?
}

impl RollupLayer {
    pub fn from_base_layer(
        layer: Arc<InternalLayer>,
        original: [u32; 5],
        original_parent: Option<[u32; 5]>,
    ) -> Self {
        Self {
            internal: layer,
            original,
            original_parent,
        }
    }

    pub fn from_child_layer(
        layer: Arc<InternalLayer>,
        original: [u32; 5],
        original_parent: [u32; 5],
    ) -> Self {
        Self {
            internal: layer,
            original,
            original_parent: Some(original_parent),
        }
    }
}
