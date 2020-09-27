use super::super::layer::*;
use super::InternalLayerImpl;
use crate::structure::*;
use std::convert::TryInto;
use thiserror::Error;

#[derive(Clone)]
pub struct InternalLayerTripleSubjectIterator {
    subjects: Option<MonotonicLogArray>,
    s_p_adjacency_list: AdjacencyList,
    sp_o_adjacency_list: AdjacencyList,
    s_position: u64,
    s_p_position: u64,
    sp_o_position: u64,
    peeked: Option<IdTriple>,
}

impl InternalLayerTripleSubjectIterator {
    pub fn new(
        subjects: Option<&MonotonicLogArray>,
        s_p_adjacency_list: &AdjacencyList,
        sp_o_adjacency_list: &AdjacencyList,
    ) -> Self {
        Self {
            subjects: subjects.map(|s| s.clone()),
            s_p_adjacency_list: s_p_adjacency_list.clone(),
            sp_o_adjacency_list: sp_o_adjacency_list.clone(),
            s_position: 0,
            s_p_position: 0,
            sp_o_position: 0,
            peeked: None,
        }
    }

    pub fn seek_subject(mut self, subject: u64) -> Self {
        self.seek_subject_ref(subject);

        self
    }

    pub fn seek_subject_ref(&mut self, subject: u64) {
        self.peeked = None;
        if subject == 0 {
            self.s_position = 0;
            self.s_p_position = 0;
            self.sp_o_position = 0;

            return;
        }

        self.s_position = match self.subjects.as_ref() {
            None => subject - 1,
            Some(subjects) => subjects.nearest_index_of(subject) as u64,
        };

        if self.s_position >= self.s_p_adjacency_list.left_count() as u64 {
            self.s_p_position = self.s_p_adjacency_list.right_count() as u64;
            self.sp_o_position = self.sp_o_adjacency_list.right_count() as u64;
        } else {
            self.s_p_position = self.s_p_adjacency_list.offset_for(self.s_position + 1);
            self.sp_o_position = self.sp_o_adjacency_list.offset_for(self.s_p_position + 1);
        }
    }

    pub fn seek_subject_predicate(mut self, subject: u64, predicate: u64) -> Self {
        self.seek_subject_predicate_ref(subject, predicate);

        self
    }

    pub fn seek_subject_predicate_ref(&mut self, subject: u64, predicate: u64) {
        if predicate == 0 {
            // equivalent to seeking subject
            self.seek_subject_ref(subject);
            return;
        }

        self.peeked = None;
        if subject == 0 {
            self.s_position = 0;
            self.s_p_position = 0;
            self.sp_o_position = 0;

            return;
        }

        self.s_position = match self.subjects.as_ref() {
            None => subject - 1,
            Some(subjects) => subjects.nearest_index_of(subject) as u64,
        };

        if self.s_position >= self.s_p_adjacency_list.left_count() as u64 {
            self.s_p_position = self.s_p_adjacency_list.right_count() as u64;
            self.sp_o_position = self.sp_o_adjacency_list.right_count() as u64;
        } else {
            let mut s_p_position = self.s_p_adjacency_list.offset_for(self.s_position + 1);
            while self.s_p_adjacency_list.num_at_pos(s_p_position) < predicate {
                s_p_position += 1;

                if self.s_p_adjacency_list.bit_at_pos(s_p_position - 1) {
                    // we just moved past the end for this subject, without finding the predicate.
                    // so this is where we have to stop
                    self.s_position += 1;
                    break;
                }
            }
            self.s_p_position = s_p_position;
            self.sp_o_position = self.sp_o_adjacency_list.offset_for(self.s_p_position + 1);
        }
    }

    pub fn seek_s_p_pos(&mut self, pos: u64) {
        self.peeked = None;

        self.s_p_position = pos;
        self.s_position = self.s_p_adjacency_list.left_at_pos(pos) - 1;
        self.sp_o_position = self.sp_o_adjacency_list.offset_for(self.s_p_position + 1);
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.peeked = self.next();

        self.peeked.as_ref()
    }
}

impl Iterator for InternalLayerTripleSubjectIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        if self.peeked.is_some() {
            let peeked = self.peeked;
            self.peeked = None;

            return peeked;
        }
        loop {
            if self.sp_o_position >= self.sp_o_adjacency_list.right_count() as u64 {
                return None;
            } else {
                let subject = match self.subjects.as_ref() {
                    Some(subjects) => subjects.entry(self.s_position.try_into().unwrap()),
                    None => self.s_position + 1,
                };

                let s_p_bit = self.s_p_adjacency_list.bit_at_pos(self.s_p_position);
                let predicate = self.s_p_adjacency_list.num_at_pos(self.s_p_position);
                if predicate == 0 {
                    if s_p_bit {
                        self.s_position += 1;
                    }
                    self.s_p_position += 1;
                    self.sp_o_position += 1;
                    continue;
                }

                let sp_o_bit = self.sp_o_adjacency_list.bit_at_pos(self.sp_o_position);
                let object = self.sp_o_adjacency_list.num_at_pos(self.sp_o_position);
                if sp_o_bit {
                    self.s_p_position += 1;
                    if s_p_bit {
                        self.s_position += 1;
                    }
                }
                self.sp_o_position += 1;

                if object == 0 {
                    continue;
                }

                return Some(IdTriple::new(subject, predicate, object));
            }
        }
    }
}

#[derive(Clone)]
pub struct OptInternalLayerTripleSubjectIterator(pub Option<InternalLayerTripleSubjectIterator>);

impl OptInternalLayerTripleSubjectIterator {
    pub fn seek_subject_ref(&mut self, subject: u64) {
        self.0.as_mut().map(|i| i.seek_subject_ref(subject));
    }

    pub fn seek_subject(self, subject: u64) -> Self {
        OptInternalLayerTripleSubjectIterator(self.0.map(|i| i.seek_subject(subject)))
    }

    pub fn seek_subject_predicate_ref(&mut self, subject: u64, predicate: u64) {
        self.0
            .as_mut()
            .map(|i| i.seek_subject_predicate_ref(subject, predicate));
    }

    pub fn seek_subject_predicate(self, subject: u64, predicate: u64) -> Self {
        OptInternalLayerTripleSubjectIterator(
            self.0.map(|i| i.seek_subject_predicate(subject, predicate)),
        )
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.0.as_mut().and_then(|i| i.peek())
    }
}

impl Iterator for OptInternalLayerTripleSubjectIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        self.0.as_mut().and_then(|i| i.next())
    }
}

#[derive(Clone)]
pub struct InternalTripleSubjectIterator {
    positives: Vec<OptInternalLayerTripleSubjectIterator>,
    negatives: Vec<OptInternalLayerTripleSubjectIterator>,
}

impl InternalTripleSubjectIterator {
    pub fn from_layer<T: 'static + InternalLayerImpl>(layer: &T) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        positives.push(layer.internal_triple_additions());
        negatives.push(layer.internal_triple_removals());

        let mut layer_opt = layer.immediate_parent();

        while layer_opt.is_some() {
            positives.push(layer_opt.unwrap().internal_triple_additions());
            negatives.push(layer_opt.unwrap().internal_triple_removals());

            layer_opt = layer_opt.unwrap().immediate_parent();
        }

        Self {
            positives,
            negatives,
        }
    }

    pub fn seek_subject(mut self, subject: u64) -> Self {
        for p in self.positives.iter_mut() {
            p.seek_subject_ref(subject);
        }

        for n in self.negatives.iter_mut() {
            n.seek_subject_ref(subject);
        }

        self
    }

    pub fn seek_subject_predicate(mut self, subject: u64, predicate: u64) -> Self {
        for p in self.positives.iter_mut() {
            p.seek_subject_predicate_ref(subject, predicate);
        }

        for n in self.negatives.iter_mut() {
            n.seek_subject_predicate_ref(subject, predicate);
        }

        self
    }
}

impl Iterator for InternalTripleSubjectIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        'outer: loop {
            // find the lowest triple.
            // if that triple appears multiple times, we want the most recent one, which should be the one appearing the earliest in the positives list.
            let lowest_index = self
                .positives
                .iter_mut()
                .map(|p| p.peek())
                .enumerate()
                .filter(|(_, elt)| elt.is_some())
                .min_by_key(|(_, elt)| elt.unwrap())
                .map(|(index, _)| index);

            match lowest_index {
                None => return None,
                Some(lowest_index) => {
                    let lowest = self.positives[lowest_index].next().unwrap();
                    // check all negative layers below the lowest_index for a removal
                    // if there's a removal, we continue after advancing. if not, it is the result.
                    // we can be sure that there's only one removal, or we'd have found another addition.
                    for iter in self.negatives[0..lowest_index].iter_mut() {
                        if iter.peek() == Some(&lowest) {
                            iter.next().unwrap();
                            continue 'outer;
                        }
                    }

                    return Some(lowest);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct InternalTripleStackIterator {
    positives: Vec<OptInternalLayerTripleSubjectIterator>,
    negatives: Vec<OptInternalLayerTripleSubjectIterator>,
}

#[derive(Error, Debug)]
pub enum LayerStackError {
    #[error("provided parent was not found")]
    ParentNotFound,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TripleChange {
    Addition,
    Removal,
}

impl InternalTripleStackIterator {
    pub fn from_layer_stack<T: 'static + InternalLayerImpl>(
        layer: &T,
        parent_id: [u32; 5],
    ) -> Result<Self, LayerStackError> {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        positives.push(layer.internal_triple_additions());
        negatives.push(layer.internal_triple_removals());

        let mut layer_opt = layer.immediate_parent();

        while layer_opt.is_some() && InternalLayerImpl::name(layer_opt.unwrap()) != parent_id {
            positives.push(layer_opt.unwrap().internal_triple_additions());
            negatives.push(layer_opt.unwrap().internal_triple_removals());

            layer_opt = layer_opt.unwrap().immediate_parent();
        }

        if layer_opt.is_none() || InternalLayerImpl::name(layer_opt.unwrap()) != parent_id {
            return Err(LayerStackError::ParentNotFound);
        }

        Ok(Self {
            positives,
            negatives,
        })
    }
}

impl Iterator for InternalTripleStackIterator {
    type Item = (TripleChange, IdTriple);

    fn next(&mut self) -> Option<(TripleChange, IdTriple)> {
        'outer: loop {
            let lowest_pos_index = self
                .positives
                .iter_mut()
                .map(|p| p.peek())
                .enumerate()
                .filter(|(_, elt)| elt.is_some())
                .min_by_key(|(_, elt)| elt.unwrap())
                .map(|(index, _)| index);

            let lowest_neg_index = self
                .negatives
                .iter_mut()
                .map(|p| p.peek())
                .enumerate()
                .filter(|(_, elt)| elt.is_some())
                .min_by_key(|(_, elt)| elt.unwrap())
                .map(|(index, _)| index);

            match (lowest_pos_index, lowest_neg_index) {
                (None, None) => return None,
                (Some(lowest_pos_index), None) => {
                    return Some((
                        TripleChange::Addition,
                        self.positives[lowest_pos_index].next().unwrap(),
                    ))
                }
                (None, Some(lowest_neg_index)) => {
                    return Some((
                        TripleChange::Removal,
                        self.negatives[lowest_neg_index].next().unwrap(),
                    ))
                }
                (Some(lowest_pos_index), Some(lowest_neg_index)) => {
                    let lowest_pos = self.positives[lowest_pos_index].peek().unwrap();
                    let lowest_neg = self.negatives[lowest_neg_index].peek().unwrap();
                    if lowest_pos < lowest_neg {
                        // next change is an addition, and there's no matching removal
                        return Some((
                            TripleChange::Addition,
                            self.positives[lowest_pos_index].next().unwrap(),
                        ));
                    } else if lowest_pos > lowest_neg {
                        // next change is a removal, and there's no mathcinga ddition
                        return Some((
                            TripleChange::Removal,
                            self.negatives[lowest_neg_index].next().unwrap(),
                        ));
                    } else {
                        // we found both an addition and a removal for the same triple. They cancel eachother.
                        self.positives[lowest_pos_index].next().unwrap();
                        self.negatives[lowest_neg_index].next().unwrap();
                        continue 'outer;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::layer::base::tests::*;
    use crate::layer::child::tests::*;
    use crate::layer::*;

    use futures::prelude::*;
    use futures::sync::oneshot;
    use std::sync::Arc;
    use tokio::runtime::{Runtime, TaskExecutor};

    #[test]
    fn base_triple_iterator() {
        let runtime = Runtime::new().unwrap();
        let base_layer: InternalLayer = example_base_layer(&runtime.executor()).into();

        let triples: Vec<_> = base_layer.triple_additions().collect();
        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(2, 1, 1),
            IdTriple::new(2, 1, 3),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_removal_iterator() {
        let runtime = Runtime::new().unwrap();
        let base_layer: InternalLayer = example_base_layer(&runtime.executor()).into();

        let triples: Vec<_> = base_layer.triple_removals().collect();
        assert!(triples.is_empty());
    }

    #[test]
    fn base_stubs_triple_iterator() {
        let runtime = Runtime::new().unwrap();
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        oneshot::spawn(future, &runtime.executor()).wait().unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap();

        let triples: Vec<_> = layer.triple_additions().collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(3, 2, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    fn layer_for_seek_tests(executor: &TaskExecutor) -> BaseLayer {
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 3, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        oneshot::spawn(future, executor).wait().unwrap();

        BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap()
    }

    #[test]
    fn base_triple_iterator_seek_to_subject() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_tests(&runtime.executor());

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(3).collect();

        let expected = vec![
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_nonexistent() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_tests(&runtime.executor());

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(4).collect();

        let expected = vec![IdTriple::new(5, 3, 6)];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_past_end() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_tests(&runtime.executor());

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(7).collect();

        assert!(triples.is_empty());
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_0() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_tests(&runtime.executor());

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(0).collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_before_begin() {
        let runtime = Runtime::new().unwrap();
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 3, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        oneshot::spawn(future, &runtime.executor()).wait().unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap();

        let triples: Vec<_> = layer.internal_triple_additions().seek_subject(2).collect();

        let expected = vec![
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 3, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    fn layer_for_seek_sp_tests(executor: &TaskExecutor) -> BaseLayer {
        let files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&files);

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll", "xyz", "yyy"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(3, 2, 4))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 4, 2))
            .and_then(|b| b.add_triple(3, 4, 3))
            .and_then(|b| b.add_triple(3, 4, 5))
            .and_then(|b| b.add_triple(5, 3, 6))
            .and_then(|b| b.finalize());

        oneshot::spawn(future, executor).wait().unwrap();

        BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap()
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 4)
            .collect();

        let expected = vec![
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_nonexistent() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 3)
            .collect();

        let expected = vec![
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred0() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 0)
            .collect();

        let expected = vec![
            IdTriple::new(3, 2, 4),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_sub0() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(0, 2)
            .collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(3, 2, 4),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred_before() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 1)
            .collect();

        let expected = vec![
            IdTriple::new(3, 2, 4),
            IdTriple::new(3, 2, 5),
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
            IdTriple::new(5, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred_past_end_of_subject() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(3, 6)
            .collect();

        let expected = vec![IdTriple::new(5, 3, 6)];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_seek_to_subject_predicate_pred_past_end() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let triples: Vec<_> = layer
            .internal_triple_additions()
            .seek_subject_predicate(5, 4)
            .collect();

        assert!(triples.is_empty());
    }

    #[test]
    fn base_triple_iterator_additions_for_subject() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_tests(&runtime.executor());

        let triples: Vec<_> = layer.triple_additions_s(3).collect();

        let expected = vec![IdTriple::new(3, 2, 5), IdTriple::new(3, 3, 5)];

        assert_eq!(expected, triples);
    }

    #[test]
    fn base_triple_iterator_additions_for_subject_predicate() {
        let runtime = Runtime::new().unwrap();
        let layer = layer_for_seek_sp_tests(&runtime.executor());

        let expected = vec![
            IdTriple::new(3, 4, 2),
            IdTriple::new(3, 4, 3),
            IdTriple::new(3, 4, 5),
        ];

        let triples: Vec<_> = layer.triple_additions_sp(3, 4).collect();

        assert_eq!(expected, triples);
    }

    fn child_layer(executor: &TaskExecutor) -> InternalLayer {
        let base_layer = example_base_layer(executor);
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = child_builder
            .into_phase2()
            .and_then(|b| b.add_triple(1, 2, 3))
            .and_then(|b| b.add_triple(3, 3, 4))
            .and_then(|b| b.add_triple(3, 5, 6))
            .and_then(|b| b.remove_triple(1, 1, 1))
            .and_then(|b| b.remove_triple(2, 1, 3))
            .and_then(|b| b.remove_triple(4, 3, 6))
            .and_then(|b| b.finalize());

        oneshot::spawn(fut, executor).wait().unwrap();

        ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .wait()
            .unwrap()
            .into()
    }

    #[test]
    fn child_triple_addition_iterator() {
        let runtime = Runtime::new().unwrap();
        let layer = child_layer(&runtime.executor());

        let triples: Vec<_> = layer.triple_additions().collect();

        let expected = vec![
            IdTriple::new(1, 2, 3),
            IdTriple::new(3, 3, 4),
            IdTriple::new(3, 5, 6),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn child_triple_removal_iterator() {
        let runtime = Runtime::new().unwrap();
        let layer = child_layer(&runtime.executor());

        let triples: Vec<_> = layer.triple_removals().collect();

        let expected = vec![
            IdTriple::new(1, 1, 1),
            IdTriple::new(2, 1, 3),
            IdTriple::new(4, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    use crate::storage::memory::*;
    use crate::storage::LayerStore;
    #[test]
    fn combined_iterator_for_subject() {
        let runtime = Runtime::new().unwrap();
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child1_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "horse"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child1_name).wait().unwrap();
        let child2_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child2_name).wait().unwrap();
        let child3_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child3_name).wait().unwrap();
        let child4_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        let layer = store.get_layer(child4_name).wait().unwrap().unwrap();

        let subject_id = layer.subject_id("duck").unwrap();
        let triples: Vec<_> = layer
            .triples_s(subject_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_value("duck", "says", "quack"),
        ];

        assert_eq!(expected, triples);
    }

    #[test]
    fn combined_iterator_for_subject_predicate() {
        let runtime = Runtime::new().unwrap();
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child1_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "horse"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child1_name).wait().unwrap();
        let child2_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "horse"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child2_name).wait().unwrap();
        let child3_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "pig"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child3_name).wait().unwrap();
        let child4_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.remove_string_triple(StringTriple::new_node("duck", "likes", "horse"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "rabbit"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        let layer = store.get_layer(child4_name).wait().unwrap().unwrap();

        let subject_id = layer.subject_id("duck").unwrap();
        let predicate_id = layer.predicate_id("likes").unwrap();
        let triples: Vec<_> = layer
            .triples_sp(subject_id, predicate_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_node("duck", "likes", "pig"),
            StringTriple::new_node("duck", "likes", "rabbit"),
        ];

        assert_eq!(expected, triples);
    }

    fn create_stack_for_partial_tests() -> ([u32; 5], Arc<InternalLayer>) {
        let runtime = Runtime::new().unwrap();
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("sheep", "says", "baa"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child1_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("horse", "says", "woof"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "horse"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child1_name).wait().unwrap();
        let child2_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("horse", "says", "woof"));
        builder.remove_string_triple(StringTriple::new_value("sheep", "says", "baa"));

        builder.add_string_triple(StringTriple::new_value("horse", "says", "quack"));
        builder.add_string_triple(StringTriple::new_value("rabbit", "says", "sniff"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        builder = store.create_child_layer(child2_name).wait().unwrap();
        let child3_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.remove_string_triple(StringTriple::new_value("horse", "says", "quack"));

        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        oneshot::spawn(builder.commit_boxed(), &runtime.executor())
            .wait()
            .unwrap();

        (
            child1_name,
            store.get_layer(child3_name).wait().unwrap().unwrap(),
        )
    }

    #[test]
    fn iterate_partial_stack() {
        let (parent_id, layer) = create_stack_for_partial_tests();

        let iterator = InternalTripleStackIterator::from_layer_stack(&*layer, parent_id).unwrap();
        let changes: Vec<_> = iterator
            .map(|t| (t.0, layer.id_triple_to_string(&t.1).unwrap()))
            .collect();

        let additions: Vec<_> = changes
            .clone()
            .into_iter()
            .filter(|(sort, _)| *sort == TripleChange::Addition)
            .map(|(_, t)| t)
            .collect();

        let removals: Vec<_> = changes
            .into_iter()
            .filter(|(sort, _)| *sort == TripleChange::Removal)
            .map(|(_, t)| t)
            .collect();

        let expected_additions = vec![
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_value("horse", "says", "neigh"),
            StringTriple::new_value("rabbit", "says", "sniff"),
        ];

        let expected_removals = vec![
            StringTriple::new_node("duck", "hates", "cow"),
            StringTriple::new_value("sheep", "says", "baa"),
            StringTriple::new_value("horse", "says", "woof"),
        ];

        assert_eq!(expected_additions, additions);
        assert_eq!(expected_removals, removals);
    }
}
