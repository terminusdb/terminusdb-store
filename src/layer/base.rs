use futures::prelude::*;
use futures::future;
use futures::stream;

use crate::structure::*;
use crate::storage::file::*;
use super::layer::*;

#[derive(Clone)]
pub struct BaseLayerFiles<F:FileLoad+FileStore> {
    pub node_dictionary_blocks_file: F,
    pub node_dictionary_offsets_file: F,

    pub predicate_dictionary_blocks_file: F,
    pub predicate_dictionary_offsets_file: F,

    pub value_dictionary_blocks_file: F,
    pub value_dictionary_offsets_file: F,

    pub s_p_adjacency_list_bits_file: F,
    pub s_p_adjacency_list_blocks_file: F,
    pub s_p_adjacency_list_sblocks_file: F,
    pub s_p_adjacency_list_nums_file: F,

    pub sp_o_adjacency_list_bits_file: F,
    pub sp_o_adjacency_list_blocks_file: F,
    pub sp_o_adjacency_list_sblocks_file: F,
    pub sp_o_adjacency_list_nums_file: F,
}

#[derive(Clone)]
pub struct BaseLayerMaps<M:AsRef<[u8]>+Clone> {
    pub node_dictionary_blocks_map: M,
    pub node_dictionary_offsets_map: M,

    pub predicate_dictionary_blocks_map: M,
    pub predicate_dictionary_offsets_map: M,

    pub value_dictionary_blocks_map: M,
    pub value_dictionary_offsets_map: M,

    pub s_p_adjacency_list_bits_map: M,
    pub s_p_adjacency_list_blocks_map: M,
    pub s_p_adjacency_list_sblocks_map: M,
    pub s_p_adjacency_list_nums_map: M,

    pub sp_o_adjacency_list_bits_map: M,
    pub sp_o_adjacency_list_blocks_map: M,
    pub sp_o_adjacency_list_sblocks_map: M,
    pub sp_o_adjacency_list_nums_map: M,
}

impl<F:FileLoad+FileStore> BaseLayerFiles<F> {
    pub fn map_all(&self) -> impl Future<Item=BaseLayerMaps<F::Map>,Error=std::io::Error> {
        let futs = vec![self.node_dictionary_blocks_file.map(),
                        self.node_dictionary_offsets_file.map(),

                        self.predicate_dictionary_blocks_file.map(),
                        self.predicate_dictionary_offsets_file.map(),

                        self.value_dictionary_blocks_file.map(),
                        self.value_dictionary_offsets_file.map(),

                        self.s_p_adjacency_list_bits_file.map(),
                        self.s_p_adjacency_list_blocks_file.map(),
                        self.s_p_adjacency_list_sblocks_file.map(),
                        self.s_p_adjacency_list_nums_file.map(),

                        self.sp_o_adjacency_list_bits_file.map(),
                        self.sp_o_adjacency_list_blocks_file.map(),
                        self.sp_o_adjacency_list_sblocks_file.map(),
                        self.sp_o_adjacency_list_nums_file.map()];

        future::join_all(futs)
            .map(|results| BaseLayerMaps {
                node_dictionary_blocks_map: results[0].clone(),
                node_dictionary_offsets_map: results[1].clone(),

                predicate_dictionary_blocks_map: results[2].clone(),
                predicate_dictionary_offsets_map: results[3].clone(),

                value_dictionary_blocks_map: results[4].clone(),
                value_dictionary_offsets_map: results[5].clone(),

                s_p_adjacency_list_bits_map: results[6].clone(),
                s_p_adjacency_list_blocks_map: results[7].clone(),
                s_p_adjacency_list_sblocks_map: results[8].clone(),
                s_p_adjacency_list_nums_map: results[9].clone(),

                sp_o_adjacency_list_bits_map: results[10].clone(),
                sp_o_adjacency_list_blocks_map: results[11].clone(),
                sp_o_adjacency_list_sblocks_map: results[12].clone(),
                sp_o_adjacency_list_nums_map: results[13].clone(),
            })
    }
}

#[derive(Clone)]
pub struct BaseLayer<M:'static+AsRef<[u8]>+Clone> {
    name: [u32;5],
    node_dictionary: PfcDict<M>,
    predicate_dictionary: PfcDict<M>,
    value_dictionary: PfcDict<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> BaseLayer<M> {
    pub fn load_from_files<F:FileLoad<Map=M>+FileStore>(name: [u32;5], files: &BaseLayerFiles<F>) -> impl Future<Item=Self,Error=std::io::Error> {
        files.map_all()
            .map(move |maps| Self::load(name, maps))
    }

    pub fn load(name: [u32;5],
                maps: BaseLayerMaps<M>) -> BaseLayer<M> {
        let node_dictionary = PfcDict::parse(maps.node_dictionary_blocks_map, maps.node_dictionary_offsets_map).unwrap();
        let predicate_dictionary = PfcDict::parse(maps.predicate_dictionary_blocks_map, maps.predicate_dictionary_offsets_map).unwrap();
        let value_dictionary = PfcDict::parse(maps.value_dictionary_blocks_map, maps.value_dictionary_offsets_map).unwrap();

        let s_p_adjacency_list = AdjacencyList::parse(maps.s_p_adjacency_list_nums_map, maps.s_p_adjacency_list_bits_map, maps.s_p_adjacency_list_blocks_map, maps.s_p_adjacency_list_sblocks_map);
        let sp_o_adjacency_list = AdjacencyList::parse(maps.sp_o_adjacency_list_nums_map, maps.sp_o_adjacency_list_bits_map, maps.sp_o_adjacency_list_blocks_map, maps.sp_o_adjacency_list_sblocks_map);

        BaseLayer {
            name,
            node_dictionary,
            predicate_dictionary,
            value_dictionary,

            s_p_adjacency_list,
            sp_o_adjacency_list
        }
    }
}

impl<M:'static+AsRef<[u8]>+Clone> Layer for BaseLayer<M> {
    type PredicateObjectPairsForSubject = BasePredicateObjectPairsForSubject<M>;
    type SubjectIterator = BaseSubjectIterator<M>;

    fn name(&self) -> [u32;5] {
        self.name
    }

    fn subjects(&self) -> BaseSubjectIterator<M> {
        BaseSubjectIterator {
            pos: 0,
            s_p_adjacency_list: self.s_p_adjacency_list.clone(),
            sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
        }
    }

    fn node_and_value_count(&self) -> usize {
        self.node_dictionary.len() + self.value_dictionary.len()
    }

    fn predicate_count(&self) -> usize {
        self.predicate_dictionary.len()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary.id(subject).map(|id| id + 1)
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary.id(predicate).map(|id| id + 1)
    }

    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.node_dictionary.id(object).map(|id| id + 1)
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary.id(value)
            .map(|id| id + self.node_dictionary.len() as u64 + 1)
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;

        match corrected_id < (self.node_dictionary.len() as u64) {
            true => Some(self.node_dictionary.get(corrected_id as usize)),
            false => None
        }
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;

        match corrected_id < (self.predicate_dictionary.len() as u64) {
            true => Some(self.predicate_dictionary.get(corrected_id as usize)),
            false => None
        }
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
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

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<BasePredicateObjectPairsForSubject<M>> {
        if subject == 0 || subject >= (self.s_p_adjacency_list.left_count() + 1) as u64 {
            None
        }
        else {
            Some(BasePredicateObjectPairsForSubject {
                subject: subject,
                predicates: self.s_p_adjacency_list.get(subject),
                sp_offset: self.s_p_adjacency_list.offset_for(subject),
                sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
            })
        }
    }
}

#[derive(Clone)]
pub struct BaseSubjectIterator<M:'static+AsRef<[u8]>+Clone> {
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>,
    pos: u64,
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for BaseSubjectIterator<M> {
    type Item = BasePredicateObjectPairsForSubject<M>;

    fn next(&mut self) -> Option<BasePredicateObjectPairsForSubject<M>> {
        if self.pos >= self.s_p_adjacency_list.left_count() as u64 {
            None
        }
        else {
            let subject = self.pos + 1;
            self.pos += 1;
            let predicates = self.s_p_adjacency_list.get(subject);
            if predicates.entry(0) == 0 {
                // stub slice, skip
                self.next()
            }
            else {
                Some(BasePredicateObjectPairsForSubject {
                    subject,
                    predicates: self.s_p_adjacency_list.get(subject),
                    sp_offset: self.s_p_adjacency_list.offset_for(subject),
                    sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
                })
            }
        }
    }
}

#[derive(Clone)]
pub struct BasePredicateObjectPairsForSubject<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> PredicateObjectPairsForSubject for BasePredicateObjectPairsForSubject<M> {
    type Objects = BaseObjectsForSubjectPredicatePair<M>;
    type PredicateIterator = BasePredicateIterator<M>;

    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicates(&self) -> BasePredicateIterator<M> {
        BasePredicateIterator {
            subject: self.subject,
            pos: 0,
            predicates: self.predicates.clone(),
            sp_offset: self.sp_offset,
            sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
        }
    }

    fn objects_for_predicate(&self, predicate: u64) -> Option<BaseObjectsForSubjectPredicatePair<M>> {
        let pos = self.predicates.iter().position(|p| p == predicate);
        match pos {
            None => None,
            Some(pos) => Some(BaseObjectsForSubjectPredicatePair {
                subject: self.subject,
                predicate: predicate,
                objects: self.sp_o_adjacency_list.get(self.sp_offset+(pos as u64)+1)
            })
        }
    }
}

#[derive(Clone)]
pub struct BasePredicateIterator<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    pos: usize,
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for BasePredicateIterator<M> {
    type Item = BaseObjectsForSubjectPredicatePair<M>;

    fn next(&mut self) -> Option<BaseObjectsForSubjectPredicatePair<M>> {
        if self.pos >= self.predicates.len() {
            None
        }
        else {
            let predicate = self.predicates.entry(self.pos);
            let objects = self.sp_o_adjacency_list.get(self.sp_offset+(self.pos as u64) + 1);
            self.pos += 1;

            if objects.entry(0) == 0 {
                // stub slice, ignore
                self.next()
            }
            else {
                Some(BaseObjectsForSubjectPredicatePair {
                    subject: self.subject,
                    predicate,
                    objects
                })
            }
        }
    }
}

#[derive(Clone)]
pub struct BaseObjectsForSubjectPredicatePair<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    predicate: u64,
    objects: LogArraySlice<M>
}

impl<M:'static+AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair for BaseObjectsForSubjectPredicatePair<M> {
    type ObjectIterator = BaseObjectIterator<M>;

    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn triples(&self) -> BaseObjectIterator<M> {
        BaseObjectIterator {
            subject: self.subject,
            predicate: self.predicate,
            objects: self.objects.clone(),
            pos: 0
        }
    }

    fn triple(&self, object: u64) -> Option<IdTriple> {
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

#[derive(Clone)]
pub struct BaseObjectIterator<M:'static+AsRef<[u8]>+Clone> {
    pub subject: u64,
    pub predicate: u64,
    objects: LogArraySlice<M>,
    pos: usize
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for BaseObjectIterator<M> {
    type Item = IdTriple;

    fn next(&mut self) -> Option<IdTriple> {
        if self.pos >= self.objects.len() {
            None
        }
        else {
            let object = self.objects.entry(self.pos);
            self.pos += 1;

            if object == 0 {
                None
            }
            else {
                Some(IdTriple {
                    subject: self.subject,
                    predicate: self.predicate,
                    object
                })
            }
        }
    }
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

impl<F:'static+FileLoad+FileStore+Clone> BaseLayerFileBuilder<F> {
    pub fn from_files(files: &BaseLayerFiles<F>) -> Self {
        Self::new(files.node_dictionary_blocks_file.clone(),
                  files.node_dictionary_offsets_file.clone(),

                  files.predicate_dictionary_blocks_file.clone(),
                  files.predicate_dictionary_offsets_file.clone(),

                  files.value_dictionary_blocks_file.clone(),
                  files.value_dictionary_offsets_file.clone(),

                  files.s_p_adjacency_list_bits_file.clone(),
                  files.s_p_adjacency_list_blocks_file.clone(),
                  files.s_p_adjacency_list_sblocks_file.clone(),
                  files.s_p_adjacency_list_nums_file.clone(),

                  files.sp_o_adjacency_list_bits_file.clone(),
                  files.sp_o_adjacency_list_blocks_file.clone(),
                  files.sp_o_adjacency_list_sblocks_file.clone(),
                  files.sp_o_adjacency_list_nums_file.clone())
    }

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

        let dict_maps_fut = vec![node_dictionary_files.blocks_file.map(),
                                 node_dictionary_files.offsets_file.map(),
                                 predicate_dictionary_files.blocks_file.map(),
                                 predicate_dictionary_files.offsets_file.map(),
                                 value_dictionary_files.blocks_file.map(),
                                 value_dictionary_files.offsets_file.map()];

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .and_then(|_| future::join_all(dict_maps_fut))
            .and_then(move |dict_maps| {
                let node_dict_r = PfcDict::parse(dict_maps[0].clone(),
                                                 dict_maps[1].clone());
                if node_dict_r.is_err() {
                    return future::err(node_dict_r.err().unwrap().into());
                }
                let node_dict = node_dict_r.unwrap();

                let pred_dict_r = PfcDict::parse(dict_maps[2].clone(),
                                                 dict_maps[3].clone());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(dict_maps[4].clone(),
                                                dict_maps[5].clone());
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

    pub fn add_id_triples<I:'static+IntoIterator<Item=IdTriple>>(self, triples: I) -> impl Future<Item=Self, Error=std::io::Error> {
        stream::iter_ok(triples)
                 .fold(self, |b, triple| b.add_triple(triple.subject, triple.predicate, triple.object))
    }

    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        future::join_all(vec![self.s_p_adjacency_list_builder.finalize(), self.sp_o_adjacency_list_builder.finalize()])
            .map(|_|())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file::*;
    use super::*;

    fn example_base_layer() -> BaseLayer<Vec<u8>> {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_layer_files = BaseLayerFiles {
            node_dictionary_blocks_file: files[0].clone(),
            node_dictionary_offsets_file: files[1].clone(),

            predicate_dictionary_blocks_file: files[2].clone(),
            predicate_dictionary_offsets_file: files[3].clone(),

            value_dictionary_blocks_file: files[4].clone(),
            value_dictionary_offsets_file: files[5].clone(),

            s_p_adjacency_list_bits_file: files[6].clone(),
            s_p_adjacency_list_blocks_file: files[7].clone(),
            s_p_adjacency_list_sblocks_file: files[8].clone(),
            s_p_adjacency_list_nums_file: files[9].clone(),

            sp_o_adjacency_list_bits_file: files[10].clone(),
            sp_o_adjacency_list_blocks_file: files[11].clone(),
            sp_o_adjacency_list_sblocks_file: files[12].clone(),
            sp_o_adjacency_list_nums_file: files[13].clone(),
        };

        let builder = BaseLayerFileBuilder::from_files(&base_layer_files);

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


        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1,2,3,4,5], &base_layer_files).wait().unwrap();

        layer
    }
    
    #[test]
    fn build_and_query_base_layer() {
        let layer = example_base_layer();

        assert!(layer.triple_exists(1,1,1));
        assert!(layer.triple_exists(2,1,1));
        assert!(layer.triple_exists(2,1,3));
        assert!(layer.triple_exists(2,3,6));
        assert!(layer.triple_exists(3,2,5));
        assert!(layer.triple_exists(3,3,6));
        assert!(layer.triple_exists(4,3,6));

        assert!(!layer.triple_exists(2,2,0));
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
        assert_eq!(ObjectType::Node("aaaaa".to_string()), base_layer.id_object(1).unwrap());
        assert_eq!(ObjectType::Value("chicken".to_string()), base_layer.id_object(6).unwrap());
    }

    #[test]
    fn subject_iteration() {
        let layer = example_base_layer();
        let subjects: Vec<_> = layer.subjects().map(|s|s.subject).collect();

        assert_eq!(vec![1,2,3,4], subjects);
    }

    #[test]
    fn predicates_iterator() {
        let layer = example_base_layer();
        let p1: Vec<_> = layer.predicate_object_pairs_for_subject(1).unwrap().predicates().map(|p|p.predicate).collect();
        assert_eq!(vec![1], p1);
        let p2: Vec<_> = layer.predicate_object_pairs_for_subject(2).unwrap().predicates().map(|p|p.predicate).collect();
        assert_eq!(vec![1,3], p2);
        let p3: Vec<_> = layer.predicate_object_pairs_for_subject(3).unwrap().predicates().map(|p|p.predicate).collect();
        assert_eq!(vec![2,3], p3);
        let p4: Vec<_> = layer.predicate_object_pairs_for_subject(4).unwrap().predicates().map(|p|p.predicate).collect();
        assert_eq!(vec![3], p4);
    }

    #[test]
    fn objects_iterator() {
        let layer = example_base_layer();
        let objects: Vec<_> = layer
            .predicate_object_pairs_for_subject(2).unwrap()
            .objects_for_predicate(1).unwrap()
            .triples().map(|o|o.object).collect();

        assert_eq!(vec![1,3], objects);
    }

    #[test]
    fn everything_iterator() {
        let layer = example_base_layer();
        let triples: Vec<_> = layer.triples()
            .map(|t|(t.subject, t.predicate, t.object))
            .collect();

        assert_eq!(vec![(1,1,1),
                        (2,1,1),
                        (2,1,3),
                        (2,3,6),
                        (3,2,5),
                        (3,3,6),
                        (4,3,6)],
                   triples);
    }
}
