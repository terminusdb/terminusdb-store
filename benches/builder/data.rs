use rand::distributions::Alphanumeric;
use rand::prelude::*;
use std::iter;
use terminus_store::layer::StringTriple;

fn random_string<R: Rng>(rand: &mut R, len_min: usize, len_max: usize) -> String {
    let len: usize = rand.gen_range(len_min..len_max);
    iter::repeat(())
        .map(|_| rand.sample(Alphanumeric))
        .take(len)
        .map(|c| c as char)
        .collect()
}

pub struct TestData<R: Rng> {
    nodes: Vec<String>,
    predicates: Vec<String>,
    values: Vec<String>,
    rand: R,
}

impl<R: Rng> TestData<R> {
    pub fn new(
        mut rand: R,
        num_nodes: usize,
        num_predicates: usize,
        num_values: usize,
    ) -> TestData<R> {
        let mut nodes = Vec::with_capacity(num_nodes);
        let mut predicates = Vec::with_capacity(num_predicates);
        let mut values = Vec::with_capacity(num_values);

        for _ in 0..num_nodes {
            nodes.push(random_string(&mut rand, 5, 50));
        }

        for _ in 0..num_predicates {
            predicates.push(random_string(&mut rand, 5, 50));
        }

        for _ in 0..num_values {
            values.push(random_string(&mut rand, 5, 5000));
        }

        TestData {
            nodes,
            predicates,
            values,
            rand,
        }
    }

    pub fn random_triple(&mut self) -> StringTriple {
        let subject_ix = self.rand.gen_range(0..self.nodes.len());
        let predicate_ix = self.rand.gen_range(0..self.predicates.len());
        if self.rand.gen() {
            let object_ix = self.rand.gen_range(0..self.nodes.len());
            StringTriple::new_node(
                &self.nodes[subject_ix],
                &self.predicates[predicate_ix],
                &self.nodes[object_ix],
            )
        } else {
            let object_ix = self.rand.gen_range(0..self.values.len());
            StringTriple::new_value(
                &self.nodes[subject_ix],
                &self.predicates[predicate_ix],
                &self.values[object_ix],
            )
        }
    }
}
