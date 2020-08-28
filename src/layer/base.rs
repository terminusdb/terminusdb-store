//! Base layer implementation.
//!
//! A base layer stores triple data without referring to a parent.
use futures::future;
use futures::prelude::*;
use futures::stream;
use futures::stream::Peekable;

use super::layer::*;
use super::internal::*;
use crate::storage::*;
use crate::structure::*;

use std::collections::BTreeSet;
use std::io;

/// A base layer.
///
/// This layer type has no parent, and therefore does not store any
/// additions or removals. It stores all its triples plus indexes
/// directly.
#[derive(Clone)]
pub struct BaseLayer {
    name: [u32; 5],
    node_dictionary: PfcDict,
    predicate_dictionary: PfcDict,
    value_dictionary: PfcDict,

    subjects: Option<MonotonicLogArray>,
    objects: Option<MonotonicLogArray>,

    s_p_adjacency_list: AdjacencyList,
    sp_o_adjacency_list: AdjacencyList,
    o_ps_adjacency_list: AdjacencyList,

    predicate_wavelet_tree: WaveletTree,
}

impl BaseLayer {
    pub fn load_from_files<F: FileLoad + FileStore>(
        name: [u32; 5],
        files: &BaseLayerFiles<F>,
    ) -> impl Future<Item = Self, Error = std::io::Error> {
        files.map_all().map(move |maps| Self::load(name, maps))
    }

    pub fn load(name: [u32; 5], maps: BaseLayerMaps) -> BaseLayer {
        let node_dictionary = PfcDict::parse(
            maps.node_dictionary_maps.blocks_map,
            maps.node_dictionary_maps.offsets_map,
        )
        .unwrap();
        let predicate_dictionary = PfcDict::parse(
            maps.predicate_dictionary_maps.blocks_map,
            maps.predicate_dictionary_maps.offsets_map,
        )
        .unwrap();
        let value_dictionary = PfcDict::parse(
            maps.value_dictionary_maps.blocks_map,
            maps.value_dictionary_maps.offsets_map,
        )
        .unwrap();

        let subjects =
            maps.subjects_map.map(|subjects_map|MonotonicLogArray::from_logarray(LogArray::parse(subjects_map).unwrap()));
        let objects =
            maps.objects_map.map(|objects_map|MonotonicLogArray::from_logarray(LogArray::parse(objects_map).unwrap()));

        let s_p_adjacency_list = AdjacencyList::parse(
            maps.s_p_adjacency_list_maps.nums_map,
            maps.s_p_adjacency_list_maps.bitindex_maps.bits_map,
            maps.s_p_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.s_p_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let sp_o_adjacency_list = AdjacencyList::parse(
            maps.sp_o_adjacency_list_maps.nums_map,
            maps.sp_o_adjacency_list_maps.bitindex_maps.bits_map,
            maps.sp_o_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.sp_o_adjacency_list_maps.bitindex_maps.sblocks_map,
        );
        let o_ps_adjacency_list = AdjacencyList::parse(
            maps.o_ps_adjacency_list_maps.nums_map,
            maps.o_ps_adjacency_list_maps.bitindex_maps.bits_map,
            maps.o_ps_adjacency_list_maps.bitindex_maps.blocks_map,
            maps.o_ps_adjacency_list_maps.bitindex_maps.sblocks_map,
        );

        let predicate_wavelet_tree_width = s_p_adjacency_list.nums().width();
        let predicate_wavelet_tree = WaveletTree::from_parts(
            BitIndex::from_maps(
                maps.predicate_wavelet_tree_maps.bits_map,
                maps.predicate_wavelet_tree_maps.blocks_map,
                maps.predicate_wavelet_tree_maps.sblocks_map,
            ),
            predicate_wavelet_tree_width,
        );

        BaseLayer {
            name,
            node_dictionary,
            predicate_dictionary,
            value_dictionary,

            subjects,
            objects,

            s_p_adjacency_list,
            sp_o_adjacency_list,

            o_ps_adjacency_list,

            predicate_wavelet_tree,
        }
    }
}

impl InternalLayerImpl for BaseLayer {
    fn name(&self) -> [u32;5] {
        self.name
    }

    fn layer_type(&self) -> LayerType {
        LayerType::Base
    }

    fn parent_name(&self) -> Option<[u32;5]> {
        None
    }

    fn immediate_parent(&self) -> Option<&InternalLayer> {
        None
    }

    fn node_dictionary(&self) -> &PfcDict {
        &self.node_dictionary
    }

    fn predicate_dictionary(&self) -> &PfcDict {
        &self.predicate_dictionary
    }

    fn value_dictionary(&self) -> &PfcDict {
        &self.value_dictionary
    }

    fn pos_s_p_adjacency_list(&self) -> &AdjacencyList {
        &self.s_p_adjacency_list
    }

    fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList {
        &self.sp_o_adjacency_list
    }

    fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList {
        &self.o_ps_adjacency_list
    }

    fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList> {
        None
    }

    fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList> {
        None
    }

    fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList> {
        None
    }

    fn pos_predicate_wavelet_tree(&self) -> &WaveletTree {
        &self.predicate_wavelet_tree
    }

    fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree> {
        None
    }

    fn pos_subjects(&self) -> Option<&MonotonicLogArray> {
        self.subjects.as_ref()
    }

    fn pos_objects(&self) -> Option<&MonotonicLogArray> {
        self.objects.as_ref()
    }

    fn neg_subjects(&self) -> Option<&MonotonicLogArray> {
        None
    }

    fn neg_objects(&self) -> Option<&MonotonicLogArray> {
        None
    }
}

/// A builder for a base layer.
///
/// This builder takes node, predicate and value strings in lexical
/// order through the corresponding `add_<thing>` methods. When
/// they're all added, `into_phase2()` is to be called to turn this
/// builder into a second builder that takes triple data.
pub struct BaseLayerFileBuilder<F: 'static + FileLoad + FileStore> {
    files: BaseLayerFiles<F>,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F: 'static + FileLoad + FileStore + Clone> BaseLayerFileBuilder<F> {
    /// Create the builder from the given files.
    pub fn from_files(files: &BaseLayerFiles<F>) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(
            files.node_dictionary_files.blocks_file.open_write(),
            files.node_dictionary_files.offsets_file.open_write(),
        );
        let predicate_dictionary_builder = PfcDictFileBuilder::new(
            files.predicate_dictionary_files.blocks_file.open_write(),
            files.predicate_dictionary_files.offsets_file.open_write(),
        );
        let value_dictionary_builder = PfcDictFileBuilder::new(
            files.value_dictionary_files.blocks_file.open_write(),
            files.value_dictionary_files.offsets_file.open_write(),
        );

        BaseLayerFileBuilder {
            files: files.clone(),
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        }
    }

    /// Add a node string.
    ///
    /// Panics if the given node string is not a lexical successor of the previous node string.
    pub fn add_node(self, node: &str) -> impl Future<Item = (u64, Self), Error = std::io::Error> {
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        node_dictionary_builder
            .add(node)
            .map(move |(result, node_dictionary_builder)| {
                (
                    result,
                    BaseLayerFileBuilder {
                        files,

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
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        predicate_dictionary_builder.add(predicate).map(
            move |(result, predicate_dictionary_builder)| {
                (
                    result,
                    BaseLayerFileBuilder {
                        files,

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
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        value_dictionary_builder
            .add(value)
            .map(move |(result, value_dictionary_builder)| {
                (
                    result,
                    BaseLayerFileBuilder {
                        files,

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

    /// Turn this builder into a phase 2 builder that will take triple data.
    pub fn into_phase2(
        self,
    ) -> impl Future<Item = BaseLayerFileBuilderPhase2<F>, Error = std::io::Error> {
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        } = self;

        let finalize_nodedict = node_dictionary_builder.finalize();
        let finalize_preddict = predicate_dictionary_builder.finalize();
        let finalize_valdict = value_dictionary_builder.finalize();

        let dict_maps_fut = vec![
            files.node_dictionary_files.blocks_file.map(),
            files.node_dictionary_files.offsets_file.map(),
            files.predicate_dictionary_files.blocks_file.map(),
            files.predicate_dictionary_files.offsets_file.map(),
            files.value_dictionary_files.blocks_file.map(),
            files.value_dictionary_files.offsets_file.map(),
        ];

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .and_then(|_| future::join_all(dict_maps_fut))
            .and_then(move |dict_maps| {
                let node_dict_r = PfcDict::parse(dict_maps[0].clone(), dict_maps[1].clone());
                if node_dict_r.is_err() {
                    return future::err(node_dict_r.err().unwrap().into());
                }
                let node_dict = node_dict_r.unwrap();

                let pred_dict_r = PfcDict::parse(dict_maps[2].clone(), dict_maps[3].clone());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(dict_maps[4].clone(), dict_maps[5].clone());
                if val_dict_r.is_err() {
                    return future::err(val_dict_r.err().unwrap().into());
                }
                let val_dict = val_dict_r.unwrap();

                let num_nodes = node_dict.len();
                let num_predicates = pred_dict.len();
                let num_values = val_dict.len();

                future::ok(BaseLayerFileBuilderPhase2::new(
                    files,
                    num_nodes,
                    num_predicates,
                    num_values,
                ))
            })
    }
}

/// Second phase of base layer building.
///
/// This builder takes ordered triple data. When all data has been
/// added, `finalize()` will build a layer.
pub struct BaseLayerFileBuilderPhase2<F: 'static + FileLoad + FileStore> {
    files: BaseLayerFiles<F>,
    s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    last_subject: u64,
    last_predicate: u64,
    object_count: usize,
}

impl<F: 'static + FileLoad + FileStore> BaseLayerFileBuilderPhase2<F> {
    fn new(
        files: BaseLayerFiles<F>,

        num_nodes: usize,
        num_predicates: usize,
        num_values: usize,
    ) -> Self {
        let s_p_width = ((num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((num_nodes + num_values + 1) as f32).log2().ceil() as u8;
        let f = files.clone();
        let s_p_adjacency_list_builder = AdjacencyListBuilder::new(
            files.s_p_adjacency_list_files.bitindex_files.bits_file,
            files
                .s_p_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write(),
            files
                .s_p_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write(),
            files.s_p_adjacency_list_files.nums_file.open_write(),
            s_p_width,
        );

        let sp_o_adjacency_list_builder = AdjacencyListBuilder::new(
            files.sp_o_adjacency_list_files.bitindex_files.bits_file,
            files
                .sp_o_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write(),
            files
                .sp_o_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write(),
            files.sp_o_adjacency_list_files.nums_file.open_write(),
            sp_o_width,
        );

        BaseLayerFileBuilderPhase2 {
            files: f,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject: 0,
            last_predicate: 0,
            object_count: num_nodes + num_values,
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
    ) -> Box<dyn Future<Item = Self, Error = std::io::Error> + Send> {
        let BaseLayerFileBuilderPhase2 {
            files,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject,
            last_predicate,
            object_count,
        } = self;

        if last_subject == subject && last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = s_p_adjacency_list_builder.count() + 1;
            Box::new(sp_o_adjacency_list_builder.push(count, object).map(
                move |sp_o_adjacency_list_builder| BaseLayerFileBuilderPhase2 {
                    files,
                    s_p_adjacency_list_builder,
                    sp_o_adjacency_list_builder,
                    last_subject: subject,
                    last_predicate: predicate,
                    object_count,
                },
            ))
        } else {
            // both list have to be pushed to
            Box::new(
                s_p_adjacency_list_builder
                    .push(subject, predicate)
                    .and_then(move |s_p_adjacency_list_builder| {
                        let count = s_p_adjacency_list_builder.count() + 1;
                        sp_o_adjacency_list_builder.push(count, object).map(
                            move |sp_o_adjacency_list_builder| BaseLayerFileBuilderPhase2 {
                                files,
                                s_p_adjacency_list_builder,
                                sp_o_adjacency_list_builder,
                                last_subject: subject,
                                last_predicate: predicate,
                                object_count,
                            },
                        )
                    }),
            )
        }
    }

    /// Add the given triple.
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

    pub fn finalize(self) -> impl Future<Item = (), Error = std::io::Error> {
        let s_p_adjacency_list_files = self.files.s_p_adjacency_list_files;
        let sp_o_adjacency_list_files = self.files.sp_o_adjacency_list_files;
        let o_ps_adjacency_list_files = self.files.o_ps_adjacency_list_files;
        let predicate_wavelet_tree_files = self.files.predicate_wavelet_tree_files;
        let object_count = self.object_count;
        future::join_all(vec![
            self.s_p_adjacency_list_builder.finalize(),
            self.sp_o_adjacency_list_builder.finalize(),
        ])
        .and_then(move |_| {
            adjacency_list_stream_pairs(
                sp_o_adjacency_list_files.bitindex_files.bits_file,
                sp_o_adjacency_list_files.nums_file,
            )
            .map(|(left, right)| (right, left))
            .fold(
                (BTreeSet::new(), 0),
                |(mut set, mut greatest_right), (left, right)| {
                    set.insert((left, right));
                    if right > greatest_right {
                        greatest_right = right;
                    }
                    future::ok::<_, std::io::Error>((set, greatest_right))
                },
            )
        })
        .and_then(move |(mut tuples, greatest_right)| {
            let (greatest_left, _) = tuples.iter().next_back().unwrap_or(&(0, 0));
            for pad_object in (*greatest_left + 1)..(object_count as u64) + 1 {
                tuples.insert((pad_object, 0));
            }
            let width = ((greatest_right + 1) as f32).log2().ceil() as u8;

            let o_ps_adjacency_list_builder = AdjacencyListBuilder::new(
                o_ps_adjacency_list_files.bitindex_files.bits_file,
                o_ps_adjacency_list_files
                    .bitindex_files
                    .blocks_file
                    .open_write(),
                o_ps_adjacency_list_files
                    .bitindex_files
                    .sblocks_file
                    .open_write(),
                o_ps_adjacency_list_files.nums_file.open_write(),
                width,
            );

            let build_o_ps_index = o_ps_adjacency_list_builder
                .push_all(stream::iter_ok(tuples))
                .and_then(|builder| builder.finalize());
            let build_predicate_index = build_wavelet_tree_from_logarray(
                s_p_adjacency_list_files.nums_file,
                predicate_wavelet_tree_files.bits_file,
                predicate_wavelet_tree_files.blocks_file,
                predicate_wavelet_tree_files.sblocks_file,
            );

            build_o_ps_index.join(build_predicate_index)
        })
        .map(|_| ())
    }
}

pub struct BaseTripleStream<S: Stream<Item = (u64, u64), Error = io::Error> + Send> {
    s_p_stream: Peekable<S>,
    sp_o_stream: Peekable<S>,
    last_s_p: (u64, u64),
    last_sp: u64,
}

impl<S: Stream<Item = (u64, u64), Error = io::Error> + Send> BaseTripleStream<S> {
    fn new(s_p_stream: S, sp_o_stream: S) -> BaseTripleStream<S> {
        BaseTripleStream {
            s_p_stream: s_p_stream.peekable(),
            sp_o_stream: sp_o_stream.peekable(),
            last_s_p: (0, 0),
            last_sp: 0,
        }
    }
}

impl<S: Stream<Item = (u64, u64), Error = io::Error> + Send> Stream for BaseTripleStream<S> {
    type Item = (u64, u64, u64);
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<(u64, u64, u64)>>, io::Error> {
        let sp_o = self.sp_o_stream.peek().map(|x| x.map(|x| x.map(|x| *x)));
        match sp_o {
            Err(e) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(Some((sp, o)))) => {
                if sp > self.last_sp {
                    let s_p = self.s_p_stream.peek().map(|x| x.map(|x| x.map(|x| *x)));
                    match s_p {
                        Err(e) => Err(e),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Ok(Async::Ready(None)) => Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected end of s_p_stream",
                        )),
                        Ok(Async::Ready(Some((s, p)))) => {
                            self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");
                            self.s_p_stream.poll().expect("peeked stream s_p_stream with confirmed result did not have result on poll");
                            self.last_s_p = (s, p);
                            self.last_sp = sp;

                            Ok(Async::Ready(Some((s, p, o))))
                        }
                    }
                } else {
                    self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");

                    Ok(Async::Ready(Some((self.last_s_p.0, self.last_s_p.1, o))))
                }
            }
        }
    }
}

pub fn open_base_triple_stream<F: 'static + FileLoad + FileStore>(
    s_p_files: AdjacencyListFiles<F>,
    sp_o_files: AdjacencyListFiles<F>,
) -> impl Stream<Item = (u64, u64, u64), Error = io::Error> + Send {
    let s_p_stream =
        adjacency_list_stream_pairs(s_p_files.bitindex_files.bits_file, s_p_files.nums_file);
    let sp_o_stream =
        adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file);

    BaseTripleStream::new(s_p_stream, sp_o_stream)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::memory::*;

    pub fn base_layer_files() -> BaseLayerFiles<MemoryBackedStore> {
        BaseLayerFiles {
            node_dictionary_files: DictionaryFiles {
                blocks_file: MemoryBackedStore::new(),
                offsets_file: MemoryBackedStore::new(),
            },
            predicate_dictionary_files: DictionaryFiles {
                blocks_file: MemoryBackedStore::new(),
                offsets_file: MemoryBackedStore::new(),
            },
            value_dictionary_files: DictionaryFiles {
                blocks_file: MemoryBackedStore::new(),
                offsets_file: MemoryBackedStore::new(),
            },

            subjects_file: MemoryBackedStore::new(),
            objects_file: MemoryBackedStore::new(),

            s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: MemoryBackedStore::new(),
                    blocks_file: MemoryBackedStore::new(),
                    sblocks_file: MemoryBackedStore::new(),
                },
                nums_file: MemoryBackedStore::new(),
            },
            predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
        }
    }

    pub fn example_base_layer_files() -> BaseLayerFiles<MemoryBackedStore> {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_layer_files = base_layer_files();

        let builder = BaseLayerFileBuilder::from_files(&base_layer_files);

        let future = builder
            .add_nodes(nodes.into_iter().map(|s| s.to_string()))
            .and_then(move |(_, b)| b.add_predicates(predicates.into_iter().map(|s| s.to_string())))
            .and_then(move |(_, b)| b.add_values(values.into_iter().map(|s| s.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(2, 1, 1))
            .and_then(|b| b.add_triple(2, 1, 3))
            .and_then(|b| b.add_triple(2, 3, 6))
            .and_then(|b| b.add_triple(3, 2, 5))
            .and_then(|b| b.add_triple(3, 3, 6))
            .and_then(|b| b.add_triple(4, 3, 6))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        base_layer_files
    }

    pub fn example_base_layer() -> BaseLayer {
        let base_layer_files = example_base_layer_files();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &base_layer_files)
            .wait()
            .unwrap();

        layer
    }

    pub fn empty_base_layer() -> BaseLayer {
        let files = base_layer_files();
        let base_builder = BaseLayerFileBuilder::from_files(&files);
        base_builder
            .into_phase2()
            .and_then(|b| b.finalize())
            .wait()
            .unwrap();

        BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .wait()
            .unwrap()
    }

    #[test]
    fn build_and_query_base_layer() {
        let layer = example_base_layer();

        assert!(layer.triple_exists(1, 1, 1));
        assert!(layer.triple_exists(2, 1, 1));
        assert!(layer.triple_exists(2, 1, 3));
        assert!(layer.triple_exists(2, 3, 6));
        assert!(layer.triple_exists(3, 2, 5));
        assert!(layer.triple_exists(3, 3, 6));
        assert!(layer.triple_exists(4, 3, 6));

        assert!(!layer.triple_exists(2, 2, 0));
    }

    #[test]
    fn dictionary_entries_in_base() {
        let base_layer = example_base_layer();

        assert_eq!(3, base_layer.subject_id("bbbbb").unwrap());
        assert_eq!(2, base_layer.predicate_id("fghij").unwrap());
        assert_eq!(1, base_layer.object_node_id("aaaaa").unwrap());
        assert_eq!(6, base_layer.object_value_id("chicken").unwrap());

        assert_eq!("bbbbb", base_layer.id_subject(3).unwrap());
        assert_eq!("fghij", base_layer.id_predicate(2).unwrap());
        assert_eq!(
            ObjectType::Node("aaaaa".to_string()),
            base_layer.id_object(1).unwrap()
        );
        assert_eq!(
            ObjectType::Value("chicken".to_string()),
            base_layer.id_object(6).unwrap()
        );
    }

    #[test]
    fn subject_iteration() {
        let layer = example_base_layer();
        let subjects: Vec<_> = layer.subjects().map(|s| s.subject()).collect();

        assert_eq!(vec![1, 2, 3, 4], subjects);
    }

    #[test]
    fn predicates_iterator() {
        let layer = example_base_layer();
        let p1: Vec<_> = layer
            .lookup_subject(1)
            .unwrap()
            .predicates()
            .map(|p| p.predicate())
            .collect();
        assert_eq!(vec![1], p1);
        let p2: Vec<_> = layer
            .lookup_subject(2)
            .unwrap()
            .predicates()
            .map(|p| p.predicate())
            .collect();
        assert_eq!(vec![1, 3], p2);
        let p3: Vec<_> = layer
            .lookup_subject(3)
            .unwrap()
            .predicates()
            .map(|p| p.predicate())
            .collect();
        assert_eq!(vec![2, 3], p3);
        let p4: Vec<_> = layer
            .lookup_subject(4)
            .unwrap()
            .predicates()
            .map(|p| p.predicate())
            .collect();
        assert_eq!(vec![3], p4);
    }

    #[test]
    fn objects_iterator() {
        let layer = example_base_layer();
        let objects: Vec<_> = layer
            .lookup_subject(2)
            .unwrap()
            .lookup_predicate(1)
            .unwrap()
            .triples()
            .map(|o| o.object)
            .collect();

        assert_eq!(vec![1, 3], objects);
    }

    #[test]
    fn everything_iterator() {
        let layer = example_base_layer();
        let triples: Vec<_> = layer
            .triples()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(
            vec![
                (1, 1, 1),
                (2, 1, 1),
                (2, 1, 3),
                (2, 3, 6),
                (3, 2, 5),
                (3, 3, 6),
                (4, 3, 6)
            ],
            triples
        );
    }

    #[test]
    fn lookup_by_object() {
        let layer = example_base_layer();

        let lookup = layer.lookup_object(1).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(1, 1), (2, 1)], pairs);

        let lookup = layer.lookup_object(3).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(2, 1)], pairs);

        let lookup = layer.lookup_object(5).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(3, 2)], pairs);

        let lookup = layer.lookup_object(6).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(2, 3), (3, 3), (4, 3)], pairs);
    }

    #[test]
    fn lookup_by_predicate() {
        let layer = example_base_layer();

        let lookup = layer.lookup_predicate(1).unwrap();
        let pairs: Vec<_> = lookup
            .subject_predicate_pairs()
            .map(|sp| sp.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1, 1, 1), (2, 1, 1), (2, 1, 3)], pairs);

        let lookup = layer.lookup_predicate(2).unwrap();
        let pairs: Vec<_> = lookup
            .subject_predicate_pairs()
            .map(|sp| sp.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(3, 2, 5)], pairs);

        let lookup = layer.lookup_predicate(3).unwrap();
        let pairs: Vec<_> = lookup
            .subject_predicate_pairs()
            .map(|sp| sp.triples())
            .flatten()
            .map(|t| (t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(2, 3, 6), (3, 3, 6), (4, 3, 6)], pairs);

        let lookup = layer.lookup_predicate(4);

        assert!(lookup.is_none());
    }

    #[test]
    fn lookup_objects() {
        let layer = example_base_layer();

        let triples_by_object: Vec<_> = layer
            .objects()
            .map(|o| {
                o.subject_predicate_pairs()
                    .map(move |(s, p)| (s, p, o.object()))
            })
            .flatten()
            .collect();

        assert_eq!(
            vec![
                (1, 1, 1),
                (2, 1, 1),
                (2, 1, 3),
                (3, 2, 5),
                (2, 3, 6),
                (3, 3, 6),
                (4, 3, 6)
            ],
            triples_by_object
        );
    }

    #[test]
    fn create_empty_base_layer() {
        let base_layer_files = base_layer_files();
        let builder = BaseLayerFileBuilder::from_files(&base_layer_files);

        let future = builder.into_phase2().and_then(|b| b.finalize());

        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &base_layer_files)
            .wait()
            .unwrap();
        assert_eq!(0, layer.node_and_value_count());
        assert_eq!(0, layer.predicate_count());
    }

    #[test]
    fn base_layer_with_multiple_pairs_pointing_at_same_object() {
        let base_layer_files = base_layer_files();
        let builder = BaseLayerFileBuilder::from_files(&base_layer_files);

        let future = builder
            .add_nodes(vec!["a", "b"].into_iter().map(|x| x.to_string()))
            .and_then(|(_, b)| b.add_predicates(vec!["c", "d"].into_iter().map(|x| x.to_string())))
            .and_then(|(_, b)| b.add_values(vec!["e"].into_iter().map(|x| x.to_string())))
            .and_then(|(_, b)| b.into_phase2())
            .and_then(|b| b.add_triple(1, 1, 1))
            .and_then(|b| b.add_triple(1, 2, 1))
            .and_then(|b| b.add_triple(2, 1, 1))
            .and_then(|b| b.add_triple(2, 2, 1))
            .and_then(|b| b.finalize());

        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &base_layer_files)
            .wait()
            .unwrap();

        let triples_by_object: Vec<_> = layer
            .objects()
            .map(|o| {
                o.subject_predicate_pairs()
                    .map(move |(s, p)| (s, p, o.object()))
            })
            .flatten()
            .collect();

        assert_eq!(
            vec![(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)],
            triples_by_object
        );
    }

    #[test]
    fn stream_base_triples() {
        let layer_files = example_base_layer_files();

        let stream = open_base_triple_stream(
            layer_files.s_p_adjacency_list_files,
            layer_files.sp_o_adjacency_list_files,
        );

        let triples: Vec<_> = stream.collect().wait().unwrap();

        assert_eq!(
            vec![
                (1, 1, 1),
                (2, 1, 1),
                (2, 1, 3),
                (2, 3, 6),
                (3, 2, 5),
                (3, 3, 6),
                (4, 3, 6)
            ],
            triples
        );
    }

    #[test]
    fn count_triples() {
        let layer_files = example_base_layer_files();
        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &layer_files)
            .wait()
            .unwrap();

        assert_eq!(7, layer.triple_layer_addition_count());
        assert_eq!(0, layer.triple_layer_removal_count());
        assert_eq!(7, layer.triple_addition_count());
        assert_eq!(0, layer.triple_removal_count());
        assert_eq!(7, layer.triple_count());
    }
}
