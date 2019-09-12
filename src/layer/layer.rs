pub trait Layer {
    fn node_count(&self) -> usize;
    fn predicate_count(&self) -> usize;
    fn value_count(&self) -> usize;

    fn subject_id(&self, subject: &str) -> Option<u64>;
    fn predicate_id(&self, predicate: &str) -> Option<u64>;
    fn object_node_id(&self, object: &str) -> Option<u64>;
    fn object_value_id(&self, object: &str) -> Option<u64>;
}
