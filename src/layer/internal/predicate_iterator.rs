use crate::layer::*;
use crate::structure::*;
use std::convert::TryInto;

#[derive(Clone)]
pub struct InternalLayerTriplePredicateIterator {
    len: usize,
    predicate_wavelet_lookup: WaveletLookup,
    subject_iterator: InternalLayerTripleSubjectIterator,
    predicate_pos: u64,
    sp_boundary: bool,
    peeked: Option<IdTriple>,
}

impl InternalLayerTriplePredicateIterator {
    pub fn new(
        predicate_wavelet_lookup: WaveletLookup,
        subjects: Option<MonotonicLogArray>,
        s_p_adjacency_list: AdjacencyList,
        sp_o_adjacency_list: AdjacencyList,
    ) -> Self {
        let len = predicate_wavelet_lookup.len();
        let subject_iterator = InternalLayerTripleSubjectIterator::new(
            subjects,
            s_p_adjacency_list,
            sp_o_adjacency_list,
        );

        Self {
            len,
            predicate_wavelet_lookup,
            subject_iterator,
            predicate_pos: 0,
            sp_boundary: true,
            peeked: None,
        }
    }

    fn next_pos(&mut self) -> bool {
        if self.predicate_pos >= self.len as u64 {
            return false;
        }

        let s_p_pos = self
            .predicate_wavelet_lookup
            .entry(self.predicate_pos.try_into().unwrap());
        self.subject_iterator.seek_s_p_pos(s_p_pos);

        self.predicate_pos += 1;

        true
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.peeked = self.next();

        self.peeked.as_ref()
    }
}

impl Iterator for InternalLayerTriplePredicateIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        if self.peeked.is_some() {
            let peeked = self.peeked;
            self.peeked = None;

            return peeked;
        }
        if self.sp_boundary {
            // We have reached the end of our previous lookup.
            // we need to look up the next predicate.
            if !self.next_pos() {
                // but there is no next pedicate, so return here.
                return None;
            }
        }
        let result = self.subject_iterator.next();

        // Check the next entry of the subject iterator.
        // If it has a different subject and predicate than before, we
        // set the sp_boundary flag, ensuring that upon the subsequent
        // next() call, we move on to the next predicate.
        let next = self.subject_iterator.peek();
        if next.is_none()
            || next.map(|t| (t.subject, t.predicate)) != result.map(|t| (t.subject, t.predicate))
        {
            self.sp_boundary = true;
        } else {
            self.sp_boundary = false;
        }

        result
    }
}

#[derive(Clone)]
pub struct OptInternalLayerTriplePredicateIterator(
    pub Option<InternalLayerTriplePredicateIterator>,
);

impl OptInternalLayerTriplePredicateIterator {
    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.0.as_mut().and_then(|i| i.peek())
    }
}

impl Iterator for OptInternalLayerTriplePredicateIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        self.0.as_mut().and_then(|i| i.next())
    }
}

#[derive(Clone)]
pub struct InternalTriplePredicateIterator {
    predicate: u64,
    positives: Vec<OptInternalLayerTriplePredicateIterator>,
    negatives: Vec<OptInternalLayerTriplePredicateIterator>,
}

impl InternalTriplePredicateIterator {
    pub fn from_layer<T: 'static + InternalLayerImpl>(layer: &T, predicate: u64) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        positives.push(layer.internal_triple_additions_p(predicate));
        negatives.push(layer.internal_triple_removals_p(predicate));

        let mut layer_opt = layer.immediate_parent();

        while layer_opt.is_some() {
            positives.push(layer_opt.unwrap().internal_triple_additions_p(predicate));
            negatives.push(layer_opt.unwrap().internal_triple_removals_p(predicate));

            layer_opt = layer_opt.unwrap().immediate_parent();
        }

        Self {
            predicate,
            positives,
            negatives,
        }
    }
}

impl Iterator for InternalTriplePredicateIterator {
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

#[cfg(test)]
mod tests {
    use crate::layer::base::tests::*;
    use crate::layer::child::tests::*;
    use crate::layer::*;

    use std::sync::Arc;

    #[tokio::test]
    async fn base_triple_predicate_iterator() {
        let base_layer: InternalLayer = example_base_layer().await.into();

        let triples: Vec<_> = base_layer.internal_triple_additions_p(3).collect();
        let expected = vec![
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];

        assert_eq!(expected, triples);
    }

    async fn child_layer() -> InternalLayer {
        let base_layer = example_base_layer().await;
        let parent: Arc<InternalLayer> = Arc::new(base_layer.into());

        let child_files = child_layer_files();

        let child_builder = ChildLayerFileBuilder::from_files(parent.clone(), &child_files);
        let fut = async {
            let mut builder = child_builder.into_phase2().await?;
            builder.add_triple(1, 2, 3).await?;
            builder.add_triple(3, 3, 4).await?;
            builder.add_triple(3, 5, 6).await?;
            builder.remove_triple(1, 1, 1).await?;
            builder.remove_triple(2, 1, 3).await?;
            builder.remove_triple(2, 3, 6).await?;
            builder.remove_triple(4, 3, 6).await?;
            builder.finalize().await
        };

        fut.await.unwrap();

        ChildLayer::load_from_files([5, 4, 3, 2, 1], parent, &child_files)
            .await
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn child_triple_addition_iterator() {
        let layer = child_layer().await;

        let triples: Vec<_> = layer.internal_triple_additions_p(3).collect();

        let expected = vec![IdTriple::new(3, 3, 4)];

        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn child_triple_removal_iterator() {
        let layer = child_layer().await;

        let triples: Vec<_> = layer.internal_triple_removals_p(3).collect();

        let expected = vec![IdTriple::new(2, 3, 6), IdTriple::new(4, 3, 6)];

        assert_eq!(expected, triples);
    }

    use crate::storage::memory::*;
    use crate::storage::LayerStore;
    #[tokio::test]
    async fn combined_iterator_for_predicate() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "duck"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(base_name).await.unwrap();
        let child1_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("horse", "says", "neigh"));
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "horse"));
        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(child1_name).await.unwrap();
        let child2_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(child2_name).await.unwrap();
        let child3_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(child3_name).await.unwrap();
        let child4_name = builder.name();

        builder.remove_string_triple(StringTriple::new_node("duck", "hates", "cow"));
        builder.add_string_triple(StringTriple::new_node("duck", "likes", "cow"));
        builder.commit_boxed().await.unwrap();

        let layer = store.get_layer(child4_name).await.unwrap().unwrap();

        let predicate_id = layer.predicate_id("likes").unwrap();
        let triples: Vec<_> = layer
            .triples_p(predicate_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("cow", "likes", "duck"),
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_node("horse", "likes", "horse"),
        ];

        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn one_subject_two_objects() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_node("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_node("cow", "says", "quack"));
        builder.commit_boxed().await.unwrap();

        let layer = store.get_layer(base_name).await.unwrap().unwrap();
        let predicate_id = layer.predicate_id("says").unwrap();
        let triples: Vec<_> = layer
            .triples_p(predicate_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("cow", "says", "moo"),
            StringTriple::new_node("cow", "says", "quack"),
        ];

        assert_eq!(expected, triples);
    }
}
