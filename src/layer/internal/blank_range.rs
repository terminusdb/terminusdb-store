pub struct BlankNodes;

impl BlankNodes {
    pub fn is_blank_node(&self, id: usize) -> bool {
        false
    }
}

pub struct Indexes;

impl Indexes {
    pub fn max_index(&self) -> usize {
        0
    }
    pub fn id_for_index(&self, index: usize) -> Option<u64> {
        None
    }

    pub fn index_for_id(&self, id: u64) -> Option<usize> {
        None
    }
}
