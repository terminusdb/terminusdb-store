use std::collections::BTreeSet;
use std::io;

use futures::future;
use futures::prelude::*;
use futures::stream;

use super::layer::*;
use crate::storage::*;
use crate::structure::*;

pub struct DictionarySetFileBuilder<F: 'static + FileStore> {
    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F: 'static + FileLoad + FileStore> DictionarySetFileBuilder<F> {
    pub fn from_files(
        node_files: DictionaryFiles<F>,
        predicate_files: DictionaryFiles<F>,
        value_files: DictionaryFiles<F>,
    ) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(
            node_files.blocks_file.open_write(),
            node_files.offsets_file.open_write(),
        );
        let predicate_dictionary_builder = PfcDictFileBuilder::new(
            predicate_files.blocks_file.open_write(),
            predicate_files.offsets_file.open_write(),
        );
        let value_dictionary_builder = PfcDictFileBuilder::new(
            value_files.blocks_file.open_write(),
            value_files.offsets_file.open_write(),
        );

        Self {
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        }
    }

    /// Add a node string.
    ///
    /// Panics if the given node string is not a lexical successor of the previous node string.
    pub fn add_node(self, node: &str) -> impl Future<Item = (u64, Self), Error = std::io::Error> {
        let DictionarySetFileBuilder {
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        node_dictionary_builder
            .add(node)
            .map(move |(result, node_dictionary_builder)| {
                (
                    result,
                    DictionarySetFileBuilder {
                        node_dictionary_builder,
                        predicate_dictionary_builder,
                        value_dictionary_builder,
                    },
                )
            })
    }

    /// Add a predicate string.
    ///
    /// Panics if the given predicate string is not a lexical successor of the previous node string.
    pub fn add_predicate(
        self,
        predicate: &str,
    ) -> impl Future<Item = (u64, Self), Error = std::io::Error> {
        let DictionarySetFileBuilder {
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        predicate_dictionary_builder.add(predicate).map(
            move |(result, predicate_dictionary_builder)| {
                (
                    result,
                    DictionarySetFileBuilder {
                        node_dictionary_builder,
                        predicate_dictionary_builder,
                        value_dictionary_builder,
                    },
                )
            },
        )
    }

    /// Add a value string.
    ///
    /// Panics if the given value string is not a lexical successor of the previous value string.
    pub fn add_value(self, value: &str) -> impl Future<Item = (u64, Self), Error = std::io::Error> {
        let DictionarySetFileBuilder {
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        value_dictionary_builder
            .add(value)
            .map(move |(result, value_dictionary_builder)| {
                (
                    result,
                    DictionarySetFileBuilder {
                        node_dictionary_builder,
                        predicate_dictionary_builder,
                        value_dictionary_builder,
                    },
                )
            })
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous added nodes are a lexical succesor of any of these nodes.
    pub fn add_nodes<I: 'static + IntoIterator<Item = String> + Send + Sync>(
        self,
        nodes: I,
    ) -> impl Future<Item = (Vec<u64>, Self), Error = std::io::Error>
    where
        <I as std::iter::IntoIterator>::IntoIter: Send + Sync,
    {
        stream::iter_ok(nodes.into_iter()).fold(
            (Vec::new(), self),
            |(mut result, builder), node| {
                builder.add_node(&node).map(|(id, builder)| {
                    result.push(id);

                    (result, builder)
                })
            },
        )
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if previous added predicates are a lexical succesor of any of these predicates.
    pub fn add_predicates<I: 'static + IntoIterator<Item = String> + Send + Sync>(
        self,
        predicates: I,
    ) -> impl Future<Item = (Vec<u64>, Self), Error = std::io::Error>
    where
        <I as std::iter::IntoIterator>::IntoIter: Send + Sync,
    {
        stream::iter_ok(predicates.into_iter()).fold(
            (Vec::new(), self),
            |(mut result, builder), predicate| {
                builder.add_predicate(&predicate).map(|(id, builder)| {
                    result.push(id);

                    (result, builder)
                })
            },
        )
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous added values are a lexical succesor of any of these values.
    pub fn add_values<I: 'static + IntoIterator<Item = String> + Send + Sync>(
        self,
        values: I,
    ) -> impl Future<Item = (Vec<u64>, Self), Error = std::io::Error>
    where
        <I as std::iter::IntoIterator>::IntoIter: Send + Sync,
    {
        stream::iter_ok(values.into_iter()).fold(
            (Vec::new(), self),
            |(mut result, builder), value| {
                builder.add_value(&value).map(|(id, builder)| {
                    result.push(id);

                    (result, builder)
                })
            },
        )
    }

    pub fn finalize(self) -> impl Future<Item = (), Error = io::Error> + Send {
        let finalize_nodedict = self.node_dictionary_builder.finalize();
        let finalize_preddict = self.predicate_dictionary_builder.finalize();
        let finalize_valdict = self.value_dictionary_builder.finalize();

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict]).map(|_| ())
    }
}

pub struct TripleFileBuilder<F: 'static + FileLoad + FileStore> {
    subjects_file: Option<F>,
    subjects: Option<Vec<u64>>,

    s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    last_subject: u64,
    last_predicate: u64,
}

impl<F: 'static + FileLoad + FileStore> TripleFileBuilder<F> {
    pub fn new(
        s_p_adjacency_list_files: AdjacencyListFiles<F>,
        sp_o_adjacency_list_files: AdjacencyListFiles<F>,
        num_nodes: usize,
        num_predicates: usize,
        num_values: usize,
        subjects_file: Option<F>,
    ) -> Self {
        let s_p_width = ((num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((num_nodes + num_values + 1) as f32).log2().ceil() as u8;

        let s_p_adjacency_list_builder = AdjacencyListBuilder::new(
            s_p_adjacency_list_files.bitindex_files.bits_file,
            s_p_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write(),
            s_p_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write(),
            s_p_adjacency_list_files.nums_file.open_write(),
            s_p_width,
        );

        let sp_o_adjacency_list_builder = AdjacencyListBuilder::new(
            sp_o_adjacency_list_files.bitindex_files.bits_file,
            sp_o_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write(),
            sp_o_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write(),
            sp_o_adjacency_list_files.nums_file.open_write(),
            sp_o_width,
        );

        let subjects = match subjects_file.is_some() {
            true => Some(Vec::new()),
            false => None,
        };

        Self {
            subjects,
            subjects_file,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject: 0,
            last_predicate: 0,
        }
    }

    /// Add the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been added.
    pub fn add_triple(
        self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> impl Future<Item = Self, Error = std::io::Error> + Send {
        let TripleFileBuilder {
            mut subjects,
            subjects_file,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject,
            last_predicate,
        } = self;

        if subject < last_subject {
            panic!("layer builder got addition in wrong order (subject is {} while previously {} was pushed)", subject, last_subject)
        } else if last_subject == subject && last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = s_p_adjacency_list_builder.count() + 1;
            future::Either::A(sp_o_adjacency_list_builder.push(count, object).map(
                move |sp_o_adjacency_list_builder| TripleFileBuilder {
                    subjects,
                    subjects_file,
                    s_p_adjacency_list_builder,
                    sp_o_adjacency_list_builder,
                    last_subject: subject,
                    last_predicate: predicate,
                },
            ))
        } else {
            // both list have to be pushed to
            if subjects.is_some() && subject != last_subject {
                subjects.as_mut().unwrap().push(subject);
            }
            let mapped_subject = subjects.as_ref().map(|s| s.len() as u64).unwrap_or(subject);
            future::Either::B(
                s_p_adjacency_list_builder
                    .push(mapped_subject, predicate)
                    .and_then(move |s_p_adjacency_list_builder| {
                        let count = s_p_adjacency_list_builder.count() + 1;
                        sp_o_adjacency_list_builder.push(count, object).map(
                            move |sp_o_adjacency_list_builder| TripleFileBuilder {
                                subjects,
                                subjects_file,
                                s_p_adjacency_list_builder,
                                sp_o_adjacency_list_builder,
                                last_subject: subject,
                                last_predicate: predicate,
                            },
                        )
                    }),
            )
        }
    }

    /// Add the given triples.
    ///
    /// This will panic if a greater triple has already been added.
    pub fn add_id_triples<I: 'static + IntoIterator<Item = IdTriple>>(
        self,
        triples: I,
    ) -> impl Future<Item = Self, Error = std::io::Error> {
        stream::iter_ok(triples).fold(self, |b, triple| {
            b.add_triple(triple.subject, triple.predicate, triple.object)
        })
    }

    pub fn finalize(self) -> impl Future<Item = (), Error = std::io::Error> + Send {
        let aj_futs = vec![
            self.s_p_adjacency_list_builder.finalize(),
            self.sp_o_adjacency_list_builder.finalize(),
        ];

        let subjects_fut = match self.subjects {
            None => future::Either::A(future::ok(())),
            Some(subjects) => {
                // isn't this just last_subject?
                let max_subject = if subjects.len() == 0 {
                    0
                } else {
                    subjects[subjects.len() - 1]
                };
                let subjects_width = 1 + (max_subject as f32).log2().ceil() as u8;
                let subjects_logarray_builder = LogArrayFileBuilder::new(
                    self.subjects_file.unwrap().open_write(),
                    subjects_width,
                );
                future::Either::B(
                    subjects_logarray_builder
                        .push_all(stream::iter_ok(subjects))
                        .and_then(|b| b.finalize())
                        .map(|_| ()),
                )
            }
        };

        future::join_all(aj_futs).join(subjects_fut).map(|_| ())
    }
}

pub fn build_object_index<F: 'static + FileLoad + FileStore>(
    sp_o_files: AdjacencyListFiles<F>,
    o_ps_files: AdjacencyListFiles<F>,
    objects_file: Option<F>,
) -> impl Future<Item = (), Error = std::io::Error> {
    adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file)
        .map(|(left, right)| (right, left))
        .fold(
            (BTreeSet::new(), BTreeSet::new(), 0),
            |(mut pairs_set, mut objects_set, _), (left, right)| {
                pairs_set.insert((left, right));
                objects_set.insert(left);
                future::ok::<_, std::io::Error>((pairs_set, objects_set, right))
            },
        )
        .and_then(move |(pairs, objects, greatest_sp)| {
            let aj_width = ((greatest_sp + 1) as f32).log2().ceil() as u8;
            let o_ps_adjacency_list_builder = AdjacencyListBuilder::new(
                o_ps_files.bitindex_files.bits_file,
                o_ps_files.bitindex_files.blocks_file.open_write(),
                o_ps_files.bitindex_files.sblocks_file.open_write(),
                o_ps_files.nums_file.open_write(),
                aj_width,
            );

            let objects_builder;
            let build_o_ps_task;
            if let Some(objects_file) = objects_file {
                let greatest_object = objects.iter().next_back().unwrap_or(&0);
                let objects_width = ((*greatest_object + 1) as f32).log2().ceil() as u8;
                objects_builder = Some(LogArrayFileBuilder::new(
                    objects_file.open_write(),
                    objects_width,
                ));
                let iter = pairs
                    .into_iter()
                    .scan((0, 0), |(compressed, last), (left, right)| {
                        if left > *last {
                            *compressed += 1;
                        }

                        *last = left;

                        Some((*compressed, right))
                    });
                build_o_ps_task = future::Either::A(
                    o_ps_adjacency_list_builder
                        .push_all(stream::iter_ok(iter))
                        .and_then(|builder| builder.finalize())
                );
            } else {
                objects_builder = None;
                build_o_ps_task = future::Either::B(
                    o_ps_adjacency_list_builder
                        .push_all(stream::iter_ok(pairs))
                        .and_then(|builder| builder.finalize())
                );
            }

            let build_objects_task = match objects_builder {
                None => future::Either::A(future::ok(())),
                Some(objects_builder) => future::Either::B(
                    objects_builder
                        .push_all(stream::iter_ok(objects))
                        .and_then(|builder| builder.finalize())
                        .map(|_| ()),
                ),
            };

            build_o_ps_task.join(build_objects_task)
        })
        .map(|_| ())
}

pub fn build_predicate_index<FLoad: 'static + FileLoad, F: 'static + FileLoad + FileStore>(
    source: FLoad,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> impl Future<Item = (), Error = std::io::Error> + Send {
    build_wavelet_tree_from_logarray(
        source,
        destination_bits,
        destination_blocks,
        destination_sblocks,
    )
}
