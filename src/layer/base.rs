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
        self.node_dictionary.id(subject).map(|id| id + 1)
    }

    pub fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary.id(predicate).map(|id| id + 1)
    }

    pub fn object_node_id(&self, object: &str) -> Option<u64> {
        self.node_dictionary.id(object).map(|id| id + 1)
    }

    pub fn object_value_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary.id(value)
            .map(|id| id + self.node_dictionary.len() as u64 + 1)
    }

    pub fn id_subject(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;

        match (self.node_dictionary.len() as u64) < corrected_id {
            true => Some(self.node_dictionary.get(corrected_id as usize)),
            false => None
        }
    }

    pub fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;

        match (self.predicate_dictionary.len() as u64) < corrected_id {
            true => Some(self.predicate_dictionary.get(corrected_id as usize)),
            false => None
        }
    }

    pub fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;

        if corrected_id >= (self.node_dictionary.len() as u64) {
            let val_id = corrected_id - (self.node_dictionary.len() as u64);
            if val_id >= (self.value_dictionary.len() as u64) {
                None
            }
            else {
                Some(ObjectType::Value(self.value_dictionary.get(val_id as usize)))
            }
        }
        else {
            Some(ObjectType::Node(self.node_dictionary.get(corrected_id as usize)))
        }
    }

    pub fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<PredicateObjectPairsForSubject<M>> {
        if subject == 0 || subject >= (self.s_p_adjacency_list.left_count() + 1) as u64 {
            None
        }
        else {
            Some(PredicateObjectPairsForSubject {
                subject: subject,
                predicates: self.s_p_adjacency_list.get(subject),
                sp_offset: self.s_p_adjacency_list.offset_for(subject),
                sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
            })
        }
    }

    pub fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.predicate_object_pairs_for_subject(subject)
            .and_then(|pairs| pairs.objects_for_predicate(predicate))
            .and_then(|objects| objects.triple(object))
            .is_some()
    }
}

#[derive(Clone)]
pub struct PredicateObjectPairsForSubject<M:AsRef<[u8]>+Clone> {
    pub subject: u64,
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:AsRef<[u8]>+Clone> PredicateObjectPairsForSubject<M> {
    pub fn objects_for_predicate(&self, predicate: u64) -> Option<ObjectsForSubjectPredicatePair<M>> {
        let pos = self.predicates.iter().position(|p| p == predicate);
        match pos {
            None => None,
            Some(pos) => Some(ObjectsForSubjectPredicatePair {
                subject: self.subject,
                predicate: predicate,
                objects: self.sp_o_adjacency_list.get(self.sp_offset+(pos as u64)+1)
            })
        }
    }
}

#[derive(Clone)]
pub struct ObjectsForSubjectPredicatePair<M:AsRef<[u8]>+Clone> {
    pub subject: u64,
    pub predicate: u64,
    objects: LogArraySlice<M>
}

impl<M:AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair<M> {
    pub fn triple(&self, object: u64) -> Option<IdTriple> {
        if self.objects.iter().find(|&o|o==object).is_some() {
            Some(IdTriple {
                subject: self.subject,
                predicate: self.predicate,
                object: object
            })
        }
        else {
            None
        }
    }
}

#[derive(Clone,Copy)]
pub struct IdTriple {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64
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
        let s_p_width = ((num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((num_nodes + num_values + 1) as f32).log2().ceil() as u8;
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
            let count = s_p_adjacency_list_builder.count() + 1;
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
                        let count = s_p_adjacency_list_builder.count() + 1;
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


#[cfg(test)]
mod tests {
    use crate::structure::storage::*;
    use super::*;
    use tokio;
    
    #[test]
    fn build_and_query_base_layer() {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let builder = BaseLayerFileBuilder::new(files[0].clone(), files[1].clone(), files[2].clone(), files[3].clone(), files[4].clone(), files[5].clone(), files[6].clone(), files[7].clone(), files[8].clone(), files[9].clone(), files[10].clone(), files[11].clone(), files[12].clone(), files[13].clone());

        let future = builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
            .and_then(move |(_,b)| b.add_predicates(predicates.into_iter().map(|s|s.to_string())))
            .and_then(move |(_,b)| b.add_values(values.into_iter().map(|s|s.to_string())))
            .and_then(|(_,b)| b.into_phase2())

            .and_then(|b| b.add_triple(1,1,1))
            .and_then(|b| b.add_triple(2,1,1))
            .and_then(|b| b.add_triple(2,1,3))
            .and_then(|b| b.add_triple(2,3,6))
            .and_then(|b| b.add_triple(3,2,5))
            .and_then(|b| b.add_triple(3,3,6))
            .and_then(|b| b.add_triple(4,3,6))
            .and_then(|b| b.finalize());


        let result = future.wait().unwrap();

        let layer = BaseLayer::load(files[0].clone().map(), files[1].clone().map(), files[2].clone().map(), files[3].clone().map(), files[4].clone().map(), files[5].clone().map(), files[6].clone().map(), files[7].clone().map(), files[8].clone().map(), files[9].clone().map(), files[10].clone().map(), files[11].clone().map(), files[12].clone().map(), files[13].clone().map());

        assert!(layer.triple_exists(1,1,1));
        assert!(layer.triple_exists(2,1,1));
        assert!(layer.triple_exists(2,1,3));
        assert!(layer.triple_exists(2,3,6));
        assert!(layer.triple_exists(3,2,5));
        assert!(layer.triple_exists(3,3,6));
        assert!(layer.triple_exists(4,3,6));
    }
}
