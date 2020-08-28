use std::io;

use futures::prelude::*;
use futures::future;
use futures::stream;

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
            value_dictionary_builder
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

    pub fn finalize(self) -> impl Future<Item = (), Error = io::Error>+Send {
        let finalize_nodedict = self.node_dictionary_builder.finalize();
        let finalize_preddict = self.predicate_dictionary_builder.finalize();
        let finalize_valdict = self.value_dictionary_builder.finalize();
        
        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .map(|_|())
    }
}
                      
