use crate::layer::*;
use crate::structure::*;
use std::convert::TryInto;

#[derive(Clone)]
pub struct InternalLayerTripleObjectIterator {
    subjects: Option<MonotonicLogArray>,
    objects: Option<MonotonicLogArray>,
    o_ps_adjacency_list: AdjacencyList,
    s_p_adjacency_list: AdjacencyList,

    o_position: u64,
    o_ps_position: u64,
    peeked: Option<IdTriple>,
}

impl InternalLayerTripleObjectIterator {
    pub fn new(
        subjects: Option<MonotonicLogArray>,
        objects: Option<MonotonicLogArray>,
        o_ps_adjacency_list: AdjacencyList,
        s_p_adjacency_list: AdjacencyList,
    ) -> Self {
        Self {
            subjects: subjects,
            objects: objects,
            o_ps_adjacency_list: o_ps_adjacency_list,
            s_p_adjacency_list: s_p_adjacency_list,
            o_position: 0,
            o_ps_position: 0,
            peeked: None,
        }
    }

    pub fn seek_object(mut self, object: u64) -> Self {
        self.seek_object_ref(object);

        self
    }

    pub fn seek_object_ref(&mut self, object: u64) {
        self.peeked = None;

        if object == 0 {
            self.o_position = 0;
            self.o_ps_position = 0;

            return;
        }

        self.o_position = match self.objects.as_ref() {
            None => object - 1,
            Some(objects) => objects.nearest_index_of(object) as u64,
        };

        if self.o_position >= self.o_ps_adjacency_list.left_count() as u64 {
            self.o_ps_position = self.o_ps_adjacency_list.right_count() as u64;
        } else {
            self.o_ps_position = self.o_ps_adjacency_list.offset_for(self.o_position + 1);
        }
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.peeked = self.next();

        self.peeked.as_ref()
    }
}

impl Iterator for InternalLayerTripleObjectIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        if self.peeked.is_some() {
            let peeked = self.peeked;
            self.peeked = None;

            return peeked;
        }

        loop {
            if self.o_ps_position >= self.o_ps_adjacency_list.right_count() as u64 {
                return None;
            } else {
                let object = match self.objects.as_ref() {
                    Some(objects) => objects.entry(self.o_position.try_into().unwrap()),
                    None => self.o_position + 1,
                };

                let o_ps_bit = self.o_ps_adjacency_list.bit_at_pos(self.o_ps_position);
                let sp_pair_num = self.o_ps_adjacency_list.num_at_pos(self.o_ps_position);

                if o_ps_bit {
                    self.o_position += 1;
                }
                self.o_ps_position += 1;

                if sp_pair_num == 0 {
                    continue;
                }

                let (mapped_subject, predicate) =
                    self.s_p_adjacency_list.pair_at_pos(sp_pair_num - 1);

                let subject = match self.subjects.as_ref() {
                    Some(subjects) => subjects.entry(mapped_subject as usize - 1),
                    None => mapped_subject,
                };

                return Some(IdTriple::new(subject, predicate, object));
            }
        }
    }
}

pub struct OptInternalLayerTripleObjectIterator(pub Option<InternalLayerTripleObjectIterator>);

impl OptInternalLayerTripleObjectIterator {
    pub fn seek_object(self, object: u64) -> Self {
        OptInternalLayerTripleObjectIterator(self.0.map(|i| i.seek_object(object)))
    }

    pub fn seek_object_ref(&mut self, object: u64) {
        if let Some(i) = self.0.as_mut() {
            i.seek_object_ref(object)
        }
    }

    pub fn peek(&mut self) -> Option<&IdTriple> {
        self.0.as_mut().and_then(|i| i.peek())
    }
}

impl Iterator for OptInternalLayerTripleObjectIterator {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        match self.0.as_mut() {
            Some(i) => i.next(),
            None => None,
        }
    }
}

pub struct InternalTripleObjectIterator {
    positives: Vec<OptInternalLayerTripleObjectIterator>,
    negatives: Vec<OptInternalLayerTripleObjectIterator>,
}

impl InternalTripleObjectIterator {
    pub fn from_layer<T: 'static + InternalLayerImpl>(layer: &T) -> Self {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        positives.push(layer.internal_triple_additions_by_object());
        negatives.push(layer.internal_triple_removals_by_object());

        let mut layer_opt = layer.immediate_parent();

        while layer_opt.is_some() {
            positives.push(layer_opt.unwrap().internal_triple_additions_by_object());
            negatives.push(layer_opt.unwrap().internal_triple_removals_by_object());

            layer_opt = layer_opt.unwrap().immediate_parent();
        }

        Self {
            positives,
            negatives,
        }
    }

    pub fn seek_object(mut self, object: u64) -> Self {
        for p in self.positives.iter_mut() {
            p.seek_object_ref(object);
        }

        for n in self.negatives.iter_mut() {
            n.seek_object_ref(object);
        }

        self
    }
}

impl Iterator for InternalTripleObjectIterator {
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
                .min_by_key(|(_, elt)| {
                    let e = elt.unwrap();
                    // we need to restructure because we need to order by object
                    (e.object, e.subject, e.predicate)
                })
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
    use super::*;
    use crate::layer::base::tests::*;
    use crate::storage::memory::*;
    use crate::storage::*;

    async fn example_base_layer_files() -> BaseLayerFiles<MemoryBackedStore> {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_layer_files = base_layer_files();

        let mut builder = BaseLayerFileBuilder::from_files(&base_layer_files);

        let future = async {
            builder
                .add_nodes(nodes.into_iter().map(|s| s.to_string()))
                .await?;
            builder
                .add_predicates(predicates.into_iter().map(|s| s.to_string()))
                .await?;
            builder
                .add_values(values.into_iter().map(|s| s.to_string()))
                .await?;
            let mut builder = builder.into_phase2().await?;

            builder.add_triple(1, 1, 2).await?;
            builder.add_triple(2, 1, 2).await?;
            builder.add_triple(2, 1, 3).await?;
            builder.add_triple(2, 1, 5).await?;
            builder.add_triple(2, 3, 6).await?;
            builder.add_triple(3, 2, 5).await?;
            builder.add_triple(3, 3, 6).await?;
            builder.add_triple(4, 1, 5).await?;
            builder.add_triple(4, 3, 6).await?;
            builder.finalize().await
        };

        future.await.unwrap();

        base_layer_files
    }

    async fn example_base_layer() -> BaseLayer {
        let base_layer_files = example_base_layer_files().await;

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &base_layer_files)
            .await
            .unwrap();

        layer
    }

    #[tokio::test]
    async fn object_iterator() {
        let base_layer = example_base_layer().await;

        let iterator = base_layer.internal_triple_additions_by_object();
        let triples: Vec<_> = iterator.collect();

        let expected = vec![
            IdTriple::new(1, 1, 2),
            IdTriple::new(2, 1, 2),
            IdTriple::new(2, 1, 3),
            IdTriple::new(2, 1, 5),
            IdTriple::new(3, 2, 5),
            IdTriple::new(4, 1, 5),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];
        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn object_iterator_seek() {
        let base_layer = example_base_layer().await;

        let iterator = base_layer.internal_triple_additions_by_object();
        let triples: Vec<_> = iterator.seek_object(5).collect();

        let expected = vec![
            IdTriple::new(2, 1, 5),
            IdTriple::new(3, 2, 5),
            IdTriple::new(4, 1, 5),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];
        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn object_iterator_seek_0() {
        let base_layer = example_base_layer().await;

        let iterator = base_layer.internal_triple_additions_by_object();
        let triples: Vec<_> = iterator.seek_object(0).collect();

        let expected = vec![
            IdTriple::new(1, 1, 2),
            IdTriple::new(2, 1, 2),
            IdTriple::new(2, 1, 3),
            IdTriple::new(2, 1, 5),
            IdTriple::new(3, 2, 5),
            IdTriple::new(4, 1, 5),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];
        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn object_iterator_seek_before_begin() {
        let base_layer = example_base_layer().await;

        let iterator = base_layer.internal_triple_additions_by_object();
        let triples: Vec<_> = iterator.seek_object(1).collect();

        let expected = vec![
            IdTriple::new(1, 1, 2),
            IdTriple::new(2, 1, 2),
            IdTriple::new(2, 1, 3),
            IdTriple::new(2, 1, 5),
            IdTriple::new(3, 2, 5),
            IdTriple::new(4, 1, 5),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];
        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn object_iterator_seek_nonexistent() {
        let base_layer = example_base_layer().await;

        let iterator = base_layer.internal_triple_additions_by_object();
        let triples: Vec<_> = iterator.seek_object(4).collect();

        let expected = vec![
            IdTriple::new(2, 1, 5),
            IdTriple::new(3, 2, 5),
            IdTriple::new(4, 1, 5),
            IdTriple::new(2, 3, 6),
            IdTriple::new(3, 3, 6),
            IdTriple::new(4, 3, 6),
        ];
        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn object_iterator_seek_past_end() {
        let base_layer = example_base_layer().await;

        let iterator = base_layer.internal_triple_additions_by_object();
        let triples: Vec<_> = iterator.seek_object(7).collect();
        assert!(triples.is_empty());
    }

    #[tokio::test]
    async fn object_additions_iterator_for_object() {
        let base_layer = example_base_layer().await;

        let triples: Vec<_> = base_layer.internal_triple_additions_o(5).collect();

        let expected = vec![
            IdTriple::new(2, 1, 5),
            IdTriple::new(3, 2, 5),
            IdTriple::new(4, 1, 5),
        ];

        assert_eq!(expected, triples);
    }

    #[tokio::test]
    async fn object_additions_iterator_for_nonexistent_object() {
        let base_layer = example_base_layer().await;

        let triples: Vec<_> = base_layer.internal_triple_additions_o(4).collect();

        assert!(triples.is_empty());
    }

    #[tokio::test]
    async fn combined_iterator_for_object() {
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
        builder.add_string_triple(StringTriple::new_node("horse", "likes", "cow"));
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
        builder.add_string_triple(StringTriple::new_node("field", "contains", "cow"));
        builder.commit_boxed().await.unwrap();

        let layer = store.get_layer(child4_name).await.unwrap().unwrap();

        let object_id = layer.object_node_id("cow").unwrap();
        let triples: Vec<_> = layer
            .triples_o(object_id)
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        let expected = vec![
            StringTriple::new_node("duck", "likes", "cow"),
            StringTriple::new_node("horse", "likes", "cow"),
            StringTriple::new_node("field", "contains", "cow"),
        ];

        assert_eq!(expected, triples);
    }
}
