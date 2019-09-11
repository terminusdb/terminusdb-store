use futures::prelude::*;
use futures::future;

use crate::structure::*;

#[derive(Clone)]
pub struct BaseLayer<M:AsRef<[u8]>+Clone> {
    node_dictionary: PfcDict<M>,
    predicate_dictionary: PfcDict<M>,
    value_dictionary: PfcDict<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>
}

#[derive(Debug, Clone)]
pub enum ObjectType {
    Node(String),
    Value(String)
}

impl<M:AsRef<[u8]>+Clone> BaseLayer<M> {
    pub fn load(node_dictionary_blocks_file: M,
               node_dictionary_offsets_file: M,

               predicate_dictionary_blocks_file: M,
               predicate_dictionary_offsets_file: M,

               value_dictionary_blocks_file: M,
               value_dictionary_offsets_file: M,

               s_p_adjacency_list_bits_file: M,
               s_p_adjacency_list_blocks_file: M,
               s_p_adjacency_list_sblocks_file: M,
               s_p_adjacency_list_nums_file: M,

               sp_o_adjacency_list_bits_file: M,
               sp_o_adjacency_list_blocks_file: M,
               sp_o_adjacency_list_sblocks_file: M,
               sp_o_adjacency_list_nums_file: M) -> BaseLayer<M> {
        let node_dictionary = PfcDict::parse(node_dictionary_blocks_file, node_dictionary_offsets_file).unwrap();
        let predicate_dictionary = PfcDict::parse(predicate_dictionary_blocks_file, predicate_dictionary_offsets_file).unwrap();
        let value_dictionary = PfcDict::parse(value_dictionary_blocks_file, value_dictionary_offsets_file).unwrap();

        let s_p_adjacency_list = AdjacencyList::parse(s_p_adjacency_list_nums_file, s_p_adjacency_list_bits_file, s_p_adjacency_list_blocks_file, s_p_adjacency_list_sblocks_file);
        let sp_o_adjacency_list = AdjacencyList::parse(sp_o_adjacency_list_nums_file, sp_o_adjacency_list_bits_file, sp_o_adjacency_list_blocks_file, sp_o_adjacency_list_sblocks_file);

        BaseLayer {
            node_dictionary,
            predicate_dictionary,
            value_dictionary,

            s_p_adjacency_list,
            sp_o_adjacency_list
        }
    }

    pub fn subject_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary.id(subject)
    }

    pub fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary.id(predicate)
    }

    pub fn object_node_id(&self, object: &str) -> Option<u64> {
        self.node_dictionary.id(object)
    }

    pub fn object_value_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary.id(value)
            .map(|id| id + self.node_dictionary.len() as u64)
    }

    pub fn id_subject(&self, id: u64) -> Option<String> {
        match (self.node_dictionary.len() as u64) < id {
            true => Some(self.node_dictionary.get(id as usize)),
            false => None
        }
    }

    pub fn id_predicate(&self, id: u64) -> Option<String> {
        match (self.predicate_dictionary.len() as u64) < id {
            true => Some(self.predicate_dictionary.get(id as usize)),
            false => None
        }
    }

    pub fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id >= (self.node_dictionary.len() as u64) {
            let val_id = id - (self.node_dictionary.len() as u64);
            if val_id >= (self.value_dictionary.len() as u64) {
                None
            }
            else {
                Some(ObjectType::Value(self.value_dictionary.get(val_id as usize)))
            }
        }
        else {
            Some(ObjectType::Node(self.node_dictionary.get(id as usize)))
        }
    }
}

struct DictionaryFiles<F:'static+FileLoad+FileStore> {
    blocks_file: F,
    offsets_file: F
}

struct AdjacencyListFiles<F:'static+FileLoad+FileStore> {
    bits_file: F,
    blocks_file: F,
    sblocks_file: F,
    nums_file: F,
}

pub struct BaseLayerFileBuilder<F:'static+FileLoad+FileStore> {
    node_dictionary_files: DictionaryFiles<F>,
    predicate_dictionary_files: DictionaryFiles<F>,
    value_dictionary_files: DictionaryFiles<F>,

    s_p_adjacency_list_files: AdjacencyListFiles<F>,
    sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F:'static+FileLoad+FileStore> BaseLayerFileBuilder<F> {
    pub fn new(node_dictionary_blocks_file: F,
               node_dictionary_offsets_file: F,

               predicate_dictionary_blocks_file: F,
               predicate_dictionary_offsets_file: F,

               value_dictionary_blocks_file: F,
               value_dictionary_offsets_file: F,

               s_p_adjacency_list_bits_file: F,
               s_p_adjacency_list_blocks_file: F,
               s_p_adjacency_list_sblocks_file: F,
               s_p_adjacency_list_nums_file: F,

               sp_o_adjacency_list_bits_file: F,
               sp_o_adjacency_list_blocks_file: F,
               sp_o_adjacency_list_sblocks_file: F,
               sp_o_adjacency_list_nums_file: F
    ) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(node_dictionary_blocks_file.open_write(), node_dictionary_offsets_file.open_write());
        let predicate_dictionary_builder = PfcDictFileBuilder::new(predicate_dictionary_blocks_file.open_write(), predicate_dictionary_offsets_file.open_write());
        let value_dictionary_builder = PfcDictFileBuilder::new(value_dictionary_blocks_file.open_write(), value_dictionary_offsets_file.open_write());

        let node_dictionary_files = DictionaryFiles { 
            blocks_file: node_dictionary_blocks_file,
            offsets_file: node_dictionary_offsets_file,
        };

        let predicate_dictionary_files = DictionaryFiles { 
            blocks_file: predicate_dictionary_blocks_file,
            offsets_file: predicate_dictionary_offsets_file,
        };

        let value_dictionary_files = DictionaryFiles { 
            blocks_file: value_dictionary_blocks_file,
            offsets_file: value_dictionary_offsets_file,
        };

        let s_p_adjacency_list_files = AdjacencyListFiles {
            bits_file: s_p_adjacency_list_bits_file,
            blocks_file: s_p_adjacency_list_blocks_file,
            sblocks_file: s_p_adjacency_list_sblocks_file,
            nums_file: s_p_adjacency_list_nums_file,
        };

        let sp_o_adjacency_list_files = AdjacencyListFiles {
            bits_file: sp_o_adjacency_list_bits_file,
            blocks_file: sp_o_adjacency_list_blocks_file,
            sblocks_file: sp_o_adjacency_list_sblocks_file,
            nums_file: sp_o_adjacency_list_nums_file,
        };

        BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        }
    }

    pub fn add_node(self, node: &str) -> impl Future<Item=(u64, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        node_dictionary_builder.add(node)
            .map(move|(result, node_dictionary_builder)| (result, BaseLayerFileBuilder {
                node_dictionary_files,
                predicate_dictionary_files,
                value_dictionary_files,

                s_p_adjacency_list_files,
                sp_o_adjacency_list_files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }
    
    pub fn add_predicate(self, predicate: &str) -> impl Future<Item=(u64, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        predicate_dictionary_builder.add(predicate)
            .map(move|(result, predicate_dictionary_builder)| (result, BaseLayerFileBuilder {
                node_dictionary_files,
                predicate_dictionary_files,
                value_dictionary_files,

                s_p_adjacency_list_files,
                sp_o_adjacency_list_files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    pub fn add_value(self, value: &str) -> impl Future<Item=(u64, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        value_dictionary_builder.add(value)
            .map(move|(result, value_dictionary_builder)| (result, BaseLayerFileBuilder {
                node_dictionary_files,
                predicate_dictionary_files,
                value_dictionary_files,

                s_p_adjacency_list_files,
                sp_o_adjacency_list_files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    pub fn add_nodes<I:'static+IntoIterator<Item=String>>(self, nodes: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        node_dictionary_builder.add_all(nodes.into_iter())
            .map(move|(result, node_dictionary_builder)| (result, BaseLayerFileBuilder {
                node_dictionary_files,
                predicate_dictionary_files,
                value_dictionary_files,

                s_p_adjacency_list_files,
                sp_o_adjacency_list_files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    pub fn add_predicates<I:'static+IntoIterator<Item=String>>(self, predicates: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        predicate_dictionary_builder.add_all(predicates.into_iter())
            .map(move|(result, predicate_dictionary_builder)| (result, BaseLayerFileBuilder {
                node_dictionary_files,
                predicate_dictionary_files,
                value_dictionary_files,

                s_p_adjacency_list_files,
                sp_o_adjacency_list_files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    pub fn add_values<I:'static+IntoIterator<Item=String>>(self, values: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        value_dictionary_builder.add_all(values.into_iter())
            .map(move|(result, value_dictionary_builder)| (result, BaseLayerFileBuilder {
                node_dictionary_files,
                predicate_dictionary_files,
                value_dictionary_files,

                s_p_adjacency_list_files,
                sp_o_adjacency_list_files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    pub fn into_phase2(self) -> impl Future<Item=BaseLayerFileBuilderPhase2<F>,Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            s_p_adjacency_list_files,
            sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        let finalize_nodedict = node_dictionary_builder.finalize();
        let finalize_preddict = predicate_dictionary_builder.finalize();
        let finalize_valdict = value_dictionary_builder.finalize();

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .and_then(move |_| {
                let node_dict_r = PfcDict::parse(node_dictionary_files.blocks_file.map(),
                                                 node_dictionary_files.offsets_file.map());
                if node_dict_r.is_err() {
                    return future::err(node_dict_r.err().unwrap().into());
                }
                let node_dict = node_dict_r.unwrap();

                let pred_dict_r = PfcDict::parse(predicate_dictionary_files.blocks_file.map(),
                                                 predicate_dictionary_files.offsets_file.map());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(value_dictionary_files.blocks_file.map(),
                                                value_dictionary_files.offsets_file.map());
                if val_dict_r.is_err() {
                    return future::err(val_dict_r.err().unwrap().into());
                }
                let val_dict = val_dict_r.unwrap();

                let num_nodes = node_dict.len();
                let num_predicates = pred_dict.len();
                let num_values = val_dict.len();

                future::ok(BaseLayerFileBuilderPhase2::new(s_p_adjacency_list_files.bits_file,
                                                           s_p_adjacency_list_files.blocks_file,
                                                           s_p_adjacency_list_files.sblocks_file,
                                                           s_p_adjacency_list_files.nums_file,

                                                           sp_o_adjacency_list_files.bits_file,
                                                           sp_o_adjacency_list_files.blocks_file,
                                                           sp_o_adjacency_list_files.sblocks_file,
                                                           sp_o_adjacency_list_files.nums_file,

                                                           num_nodes,
                                                           num_predicates,
                                                           num_values))
            })
    }
}

pub struct BaseLayerFileBuilderPhase2<F:'static+FileLoad+FileStore> {
    s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    last_subject: u64,
    last_predicate: u64,
}

impl<F:'static+FileLoad+FileStore> BaseLayerFileBuilderPhase2<F> {
    fn new(s_p_adjacency_list_bits_file: F,
           s_p_adjacency_list_blocks_file: F,
           s_p_adjacency_list_sblocks_file: F,
           s_p_adjacency_list_nums_file: F,
           
           sp_o_adjacency_list_bits_file: F,
           sp_o_adjacency_list_blocks_file: F,
           sp_o_adjacency_list_sblocks_file: F,
           sp_o_adjacency_list_nums_file: F,

           num_nodes: usize,
           num_predicates: usize,
           num_values: usize
    ) -> Self {
        let s_p_width = (num_predicates as f32).log2().ceil() as u8;
        let sp_o_width = ((num_nodes + num_values) as f32).log2().ceil() as u8;
        let s_p_adjacency_list_builder = AdjacencyListBuilder::new(s_p_adjacency_list_bits_file,
                                                                   s_p_adjacency_list_blocks_file.open_write(),
                                                                   s_p_adjacency_list_sblocks_file.open_write(),
                                                                   s_p_adjacency_list_nums_file.open_write(),
                                                                   s_p_width);

        let sp_o_adjacency_list_builder = AdjacencyListBuilder::new(sp_o_adjacency_list_bits_file,
                                                                    sp_o_adjacency_list_blocks_file.open_write(),
                                                                    sp_o_adjacency_list_sblocks_file.open_write(),
                                                                    sp_o_adjacency_list_nums_file.open_write(),
                                                                    sp_o_width);

        BaseLayerFileBuilderPhase2 {
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject: 0,
            last_predicate: 0,
        }
    }

    pub fn add_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>> {
        let BaseLayerFileBuilderPhase2 {
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject,
            last_predicate
        } = self;

        if last_subject == subject && last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = s_p_adjacency_list_builder.count();
            Box::new(sp_o_adjacency_list_builder.push(count, object)
                     .map(move |sp_o_adjacency_list_builder| {
                         BaseLayerFileBuilderPhase2 {
                             s_p_adjacency_list_builder,
                             sp_o_adjacency_list_builder,
                             last_subject: subject,
                             last_predicate: predicate
                         }
                     }))
        }
        else {
            // both list have to be pushed to
            Box::new(
                s_p_adjacency_list_builder.push(subject, predicate)
                    .and_then(move |s_p_adjacency_list_builder| {
                        let count = s_p_adjacency_list_builder.count();
                        sp_o_adjacency_list_builder.push(count, object)
                            .map(move |sp_o_adjacency_list_builder| {
                                BaseLayerFileBuilderPhase2 {
                                    s_p_adjacency_list_builder,
                                    sp_o_adjacency_list_builder,
                                    last_subject: subject,
                                    last_predicate: predicate
                                }
                            })
                    }))
        }
    }

    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        future::join_all(vec![self.s_p_adjacency_list_builder.finalize(), self.sp_o_adjacency_list_builder.finalize()])
            .map(|_|())
    }
}
