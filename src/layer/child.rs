use super::base::*;
use super::layer::*;
use crate::structure::*;

#[derive(Clone)]
pub enum ParentLayer<M:AsRef<[u8]>+Clone> {
    Base(Box<BaseLayer<M>>),
    Child(Box<ChildLayer<M>>)
}

impl<M:AsRef<[u8]>+Clone> Layer for ParentLayer<M> {
    fn node_count(&self) -> usize {
        match self {
            Self::Base(b) => b.node_count(),
            Self::Child(c) => c.node_count()
        }
    }

    fn predicate_count(&self) -> usize {
        match self {
            Self::Base(b) => b.predicate_count(),
            Self::Child(c) => c.predicate_count()
        }
    }

    fn value_count(&self) -> usize {
        match self {
            Self::Base(b) => b.value_count(),
            Self::Child(c) => c.value_count()
        }
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.subject_id(subject),
            Self::Child(c) => c.subject_id(subject)
        }
    }

    

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.predicate_id(predicate),
            Self::Child(c) => c.predicate_id(predicate)
        }
    }
    fn object_node_id(&self, node: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.object_node_id(node),
            Self::Child(c) => c.object_node_id(node)
        }
    }
    fn object_value_id(&self, value: &str) -> Option<u64> {
        match self {
            Self::Base(b) => b.object_value_id(value),
            Self::Child(c) => c.object_value_id(value)
        }
    }
}

#[derive(Clone)]
pub struct ChildLayer<M:AsRef<[u8]>+Clone> {
    parent: ParentLayer<M>,

    node_dictionary: Option<PfcDict<M>>,
    predicate_dictionary: Option<PfcDict<M>>,
    value_dictionary: Option<PfcDict<M>>,

    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,

    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:AsRef<[u8]>+Clone> Layer for ChildLayer<M> {
    fn node_count(&self) -> usize {
        self.node_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.node_count()
    }

    fn predicate_count(&self) -> usize {
        self.predicate_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.predicate_count()
    }

    fn value_count(&self) -> usize {
        self.value_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.value_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self.node_dictionary.as_ref().and_then(|dict| dict.id(subject)) {
            Some(id) => Some(self.parent.node_count() as u64 + id + 1),
            None => self.parent.subject_id(subject)
        }
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self.predicate_dictionary.as_ref().and_then(|dict| dict.id(predicate)) {
            Some(id) => Some(self.parent.predicate_count() as u64 + id + 1),
            None => self.parent.predicate_id(predicate)
        }
    }

    fn object_node_id(&self, node: &str) -> Option<u64> {
        match self.node_dictionary.as_ref().and_then(|dict| dict.id(node)) {
            Some(id) => Some(self.parent.node_count() as u64 + id + 1),
            None => self.parent.object_node_id(node)
        }
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        match self.value_dictionary.as_ref().and_then(|dict| dict.id(value)) {
            Some(id) => Some(self.parent.value_count() as u64 + id + 1),
            None => self.parent.object_value_id(value)
        }
    }
}

impl<M:AsRef<[u8]>+Clone> ChildLayer<M> {
}
