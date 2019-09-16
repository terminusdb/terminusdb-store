use super::layer::*;
use crate::structure::*;
use futures::prelude::*;
use futures::future;
use futures::stream;

#[derive(Clone)]
pub struct ChildLayer<M:AsRef<[u8]>+Clone> {
    parent: Box<ParentLayer<M>>,

    node_dictionary: Option<PfcDict<M>>,
    predicate_dictionary: Option<PfcDict<M>>,
    value_dictionary: Option<PfcDict<M>>,

    subjects: MonotonicLogArray<M>,
    pos_s_p_adjacency_list: AdjacencyList<M>,
    pos_sp_o_adjacency_list: AdjacencyList<M>,

    neg_s_p_adjacency_list: AdjacencyList<M>,
    neg_sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:AsRef<[u8]>+Clone> ChildLayer<M> {
    pub fn load(parent: ParentLayer<M>,
                node_dictionary_blocks_file: M,
                node_dictionary_offsets_file: M,

                predicate_dictionary_blocks_file: M,
                predicate_dictionary_offsets_file: M,

                value_dictionary_blocks_file: M,
                value_dictionary_offsets_file: M,

                subjects_file: M,

                pos_s_p_adjacency_list_bits_file: M,
                pos_s_p_adjacency_list_blocks_file: M,
                pos_s_p_adjacency_list_sblocks_file: M,
                pos_s_p_adjacency_list_nums_file: M,

                pos_sp_o_adjacency_list_bits_file: M,
                pos_sp_o_adjacency_list_blocks_file: M,
                pos_sp_o_adjacency_list_sblocks_file: M,
                pos_sp_o_adjacency_list_nums_file: M,

                neg_s_p_adjacency_list_bits_file: M,
                neg_s_p_adjacency_list_blocks_file: M,
                neg_s_p_adjacency_list_sblocks_file: M,
                neg_s_p_adjacency_list_nums_file: M,

                neg_sp_o_adjacency_list_bits_file: M,
                neg_sp_o_adjacency_list_blocks_file: M,
                neg_sp_o_adjacency_list_sblocks_file: M,
                neg_sp_o_adjacency_list_nums_file: M,
    ) -> ChildLayer<M> {
        let node_dictionary = PfcDict::parse(node_dictionary_blocks_file, node_dictionary_offsets_file).unwrap();
        let predicate_dictionary = PfcDict::parse(predicate_dictionary_blocks_file, predicate_dictionary_offsets_file).unwrap();
        let value_dictionary = PfcDict::parse(value_dictionary_blocks_file, value_dictionary_offsets_file).unwrap();

        let subjects = MonotonicLogArray::from_logarray(LogArray::parse(subjects_file).unwrap());

        let pos_s_p_adjacency_list = AdjacencyList::parse(pos_s_p_adjacency_list_nums_file, pos_s_p_adjacency_list_bits_file, pos_s_p_adjacency_list_blocks_file, pos_s_p_adjacency_list_sblocks_file);
        let pos_sp_o_adjacency_list = AdjacencyList::parse(pos_sp_o_adjacency_list_nums_file, pos_sp_o_adjacency_list_bits_file, pos_sp_o_adjacency_list_blocks_file, pos_sp_o_adjacency_list_sblocks_file);

        let neg_s_p_adjacency_list = AdjacencyList::parse(neg_s_p_adjacency_list_nums_file, neg_s_p_adjacency_list_bits_file, neg_s_p_adjacency_list_blocks_file, neg_s_p_adjacency_list_sblocks_file);
        let neg_sp_o_adjacency_list = AdjacencyList::parse(neg_sp_o_adjacency_list_nums_file, neg_sp_o_adjacency_list_bits_file, neg_sp_o_adjacency_list_blocks_file, neg_sp_o_adjacency_list_sblocks_file);

        ChildLayer {
            parent: Box::new(parent),
            
            node_dictionary: Some(node_dictionary),
            predicate_dictionary: Some(predicate_dictionary),
            value_dictionary: Some(value_dictionary),

            subjects,

            pos_s_p_adjacency_list,
            pos_sp_o_adjacency_list,

            neg_s_p_adjacency_list,
            neg_sp_o_adjacency_list,
        }
    }
}

impl<M:AsRef<[u8]>+Clone> Layer for ChildLayer<M> {
    type PredicateObjectPairsForSubject = ChildPredicateObjectPairsForSubject<M>;

    fn node_and_value_count(&self) -> usize {
        self.node_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.value_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.predicate_dictionary.as_ref().map(|d|d.len()).unwrap_or(0) + self.parent.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        match self.node_dictionary.as_ref().and_then(|dict| dict.id(subject)) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.subject_id(subject)
        }
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        match self.predicate_dictionary.as_ref().and_then(|dict| dict.id(predicate)) {
            Some(id) => Some(self.parent.predicate_count() as u64 + id + 1),
            None => self.parent.predicate_id(predicate)
        }
    }

    fn object_node_id(&self, node: &str) -> Option<u64> {
        match self.node_dictionary.as_ref().and_then(|dict| dict.id(node)) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.object_node_id(node)
        }
    }

    fn object_value_id(&self, value: &str) -> Option<u64> {
        match self.value_dictionary.as_ref().and_then(|dict| dict.id(value)) {
            Some(id) => Some(self.parent.node_and_value_count() as u64 + id + 1),
            None => self.parent.object_value_id(value)
        }
    }

    fn predicate_object_pairs_for_subject(&self, subject: u64) -> Option<ChildPredicateObjectPairsForSubject<M>> {
        if subject == 0 {
            return None;
        }

        let mut pos: Option<AdjacencyStuff<M>> = None;
        let mut neg: Option<AdjacencyStuff<M>> = None;
        
        // first determine where we should be looking.
        let index = self.subjects.index_of(subject);
        let parent = self.parent.predicate_object_pairs_for_subject(subject).map(|p|Box::new(p));
        if index.is_none() && parent.is_none() {
            return None;
        }

        if index.is_some() {
            // subject is mentioned in this layer (as an insert or delete), and might be in the parent layer as well
            let mapped_subject = index.unwrap() as u64 + 1;
            if mapped_subject <= self.pos_s_p_adjacency_list.left_count() as u64 {
                let pos_predicates = self.pos_s_p_adjacency_list.get(mapped_subject);
                let pos_sp_offset = self.pos_s_p_adjacency_list.offset_for(mapped_subject);
                pos = Some(AdjacencyStuff {
                    predicates: pos_predicates,
                    sp_offset: pos_sp_offset,
                    sp_o_adjacency_list: self.pos_sp_o_adjacency_list.clone()
                });
            }

            if mapped_subject <= self.neg_s_p_adjacency_list.left_count() as u64 {
                let neg_predicates = self.neg_s_p_adjacency_list.get(mapped_subject);
                let neg_sp_offset = self.neg_s_p_adjacency_list.offset_for(mapped_subject);

                neg = Some(AdjacencyStuff {
                    predicates: neg_predicates,
                    sp_offset: neg_sp_offset,
                    sp_o_adjacency_list: self.neg_sp_o_adjacency_list.clone()
                });
            }
        }

        Some(ChildPredicateObjectPairsForSubject {
            parent,
            subject,
            pos,
            neg
        })
    }
}

#[derive(Clone)]
struct AdjacencyStuff<M:AsRef<[u8]>+Clone> {
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}


#[derive(Clone)]
pub struct ChildPredicateObjectPairsForSubject<M:AsRef<[u8]>+Clone> {
    parent: Option<Box<ParentPredicateObjectPairsForSubject<M>>>,
    subject: u64,

    pos: Option<AdjacencyStuff<M>>,
    neg: Option<AdjacencyStuff<M>>,
}

impl<M:AsRef<[u8]>+Clone> PredicateObjectPairsForSubject for ChildPredicateObjectPairsForSubject<M> {
    type Objects = ChildObjectsForSubjectPredicatePair<M>;
    fn objects_for_predicate(&self, predicate: u64) -> Option<ChildObjectsForSubjectPredicatePair<M>> {
        if predicate == 0 {
            return None;
        }

        let parent_objects = self.parent.as_ref().and_then(|parent|parent.objects_for_predicate(predicate).map(|ofp|Box::new(ofp)));

        if self.pos.is_none() && parent_objects.is_none() {
            None
        }
        else {
            let pos_objects = self.pos.as_ref()
                .and_then(|pos| pos.predicates.iter().position(|p|p==predicate)
                          .map(|position_in_pos_predicates|
                               pos.sp_o_adjacency_list.get(pos.sp_offset+(position_in_pos_predicates as u64)+1)));
            let neg_objects = self.neg.as_ref()
                .and_then(|neg| neg.predicates.iter().position(|p|p==predicate)
                          .map(|position_in_neg_predicates|
                               neg.sp_o_adjacency_list.get(neg.sp_offset+(position_in_neg_predicates as u64)+1)));

            Some(ChildObjectsForSubjectPredicatePair {
                parent: parent_objects,
                subject: self.subject,
                predicate,
                pos_objects,
                neg_objects
            })
        }
    }
}

#[derive(Clone)]
pub struct ChildObjectsForSubjectPredicatePair<M:AsRef<[u8]>+Clone> {
    parent: Option<Box<ParentObjectsForSubjectPredicatePair<M>>>,
    subject: u64,
    predicate: u64,
    pos_objects: Option<LogArraySlice<M>>,
    neg_objects: Option<LogArraySlice<M>>
}

impl<M:AsRef<[u8]>+Clone> ObjectsForSubjectPredicatePair for ChildObjectsForSubjectPredicatePair<M> {
    fn triple(&self, object: u64) -> Option<IdTriple> {
        if object == 0 {
            return None;
        }
        // this should check pos (if it is there), then neg (if it is there), and finally parent.
        // if it is in neg, return None. otherwise return the triple (if in pos) or whatever comes out of parent.
        self.pos_objects.as_ref()
            .and_then(|po|po.iter().position(|o|o == object))
            .map(|_| IdTriple { subject: self.subject, predicate: self.predicate, object: object })
            .or_else(|| {
                if self.neg_objects.as_ref().and_then(|no|no.iter().position(|o|o == object)).is_some() {
                    None
                }
                else {
                    self.parent.as_ref().and_then(|p|p.triple(object))
                }
            })
    }
}

struct ChildLayerFileBuilder<F:'static+FileLoad+FileStore> {
    parent: ParentLayer<F::Map>,
    node_dictionary_files: DictionaryFiles<F>,
    predicate_dictionary_files: DictionaryFiles<F>,
    value_dictionary_files: DictionaryFiles<F>,

    subjects_file: F,
    subjects: Vec<u64>,

    pos_s_p_adjacency_list_files: AdjacencyListFiles<F>,
    pos_sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    neg_s_p_adjacency_list_files: AdjacencyListFiles<F>,
    neg_sp_o_adjacency_list_files: AdjacencyListFiles<F>,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F:'static+FileLoad+FileStore> ChildLayerFileBuilder<F> {
    pub fn new(parent: ParentLayer<F::Map>,
               node_dictionary_blocks_file: F,
               node_dictionary_offsets_file: F,

               predicate_dictionary_blocks_file: F,
               predicate_dictionary_offsets_file: F,

               value_dictionary_blocks_file: F,
               value_dictionary_offsets_file: F,

               subjects_file: F,

               pos_s_p_adjacency_list_bits_file: F,
               pos_s_p_adjacency_list_blocks_file: F,
               pos_s_p_adjacency_list_sblocks_file: F,
               pos_s_p_adjacency_list_nums_file: F,

               pos_sp_o_adjacency_list_bits_file: F,
               pos_sp_o_adjacency_list_blocks_file: F,
               pos_sp_o_adjacency_list_sblocks_file: F,
               pos_sp_o_adjacency_list_nums_file: F,

               neg_s_p_adjacency_list_bits_file: F,
               neg_s_p_adjacency_list_blocks_file: F,
               neg_s_p_adjacency_list_sblocks_file: F,
               neg_s_p_adjacency_list_nums_file: F,

               neg_sp_o_adjacency_list_bits_file: F,
               neg_sp_o_adjacency_list_blocks_file: F,
               neg_sp_o_adjacency_list_sblocks_file: F,
               neg_sp_o_adjacency_list_nums_file: F,
    ) -> Self {
        let subjects = Vec::new();
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

        let pos_s_p_adjacency_list_files = AdjacencyListFiles {
            bits_file: pos_s_p_adjacency_list_bits_file,
            blocks_file: pos_s_p_adjacency_list_blocks_file,
            sblocks_file: pos_s_p_adjacency_list_sblocks_file,
            nums_file: pos_s_p_adjacency_list_nums_file,
        };

        let pos_sp_o_adjacency_list_files = AdjacencyListFiles {
            bits_file: pos_sp_o_adjacency_list_bits_file,
            blocks_file: pos_sp_o_adjacency_list_blocks_file,
            sblocks_file: pos_sp_o_adjacency_list_sblocks_file,
            nums_file: pos_sp_o_adjacency_list_nums_file,
        };

        let neg_s_p_adjacency_list_files = AdjacencyListFiles {
            bits_file: neg_s_p_adjacency_list_bits_file,
            blocks_file: neg_s_p_adjacency_list_blocks_file,
            sblocks_file: neg_s_p_adjacency_list_sblocks_file,
            nums_file: neg_s_p_adjacency_list_nums_file,
        };

        let neg_sp_o_adjacency_list_files = AdjacencyListFiles {
            bits_file: neg_sp_o_adjacency_list_bits_file,
            blocks_file: neg_sp_o_adjacency_list_blocks_file,
            sblocks_file: neg_sp_o_adjacency_list_sblocks_file,
            nums_file: neg_sp_o_adjacency_list_nums_file,
        };

        ChildLayerFileBuilder {
            parent,
            
            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            subjects_file,
            subjects,

            pos_s_p_adjacency_list_files,
            pos_sp_o_adjacency_list_files,
            neg_s_p_adjacency_list_files,
            neg_sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        }
    }

    pub fn add_node(self, node: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>> {
        let offset = self.parent.node_and_value_count() as u64;

        match self.parent.subject_id(node) {
            None => {
                let ChildLayerFileBuilder {
                    parent,
                    
                    node_dictionary_files,
                    predicate_dictionary_files,
                    value_dictionary_files,

                    subjects_file,
                    subjects,

                    pos_s_p_adjacency_list_files,
                    pos_sp_o_adjacency_list_files,
                    neg_s_p_adjacency_list_files,
                    neg_sp_o_adjacency_list_files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;
                Box::new(node_dictionary_builder.add(node)
                         .map(move|(result, node_dictionary_builder)| (offset+result, ChildLayerFileBuilder {
                             parent,

                             node_dictionary_files,
                             predicate_dictionary_files,
                             value_dictionary_files,

                             subjects_file,
                             subjects,

                             pos_s_p_adjacency_list_files,
                             pos_sp_o_adjacency_list_files,
                             neg_s_p_adjacency_list_files,
                             neg_sp_o_adjacency_list_files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }
    
    pub fn add_predicate(self, predicate: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>> {
        let offset = self.parent.predicate_count() as u64;

        match self.parent.predicate_id(predicate) {
            None => {
                let ChildLayerFileBuilder {
                    parent,

                    node_dictionary_files,
                    predicate_dictionary_files,
                    value_dictionary_files,

                    subjects_file,
                    subjects,

                    pos_s_p_adjacency_list_files,
                    pos_sp_o_adjacency_list_files,
                    neg_s_p_adjacency_list_files,
                    neg_sp_o_adjacency_list_files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;

                Box::new(predicate_dictionary_builder.add(predicate)
                         .map(move|(result, predicate_dictionary_builder)| (offset+result, ChildLayerFileBuilder {
                             parent,

                             node_dictionary_files,
                             predicate_dictionary_files,
                             value_dictionary_files,

                             subjects_file,
                             subjects,

                             pos_s_p_adjacency_list_files,
                             pos_sp_o_adjacency_list_files,
                             neg_s_p_adjacency_list_files,
                             neg_sp_o_adjacency_list_files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    pub fn add_value(self, value: &str) -> Box<dyn Future<Item=(u64, Self), Error=std::io::Error>> {
        let offset = self.parent.node_and_value_count() as u64;

        match self.parent.object_value_id(value) {
            None => {
                let ChildLayerFileBuilder {
                    parent,

                    node_dictionary_files,
                    predicate_dictionary_files,
                    value_dictionary_files,

                    subjects_file,
                    subjects,

                    pos_s_p_adjacency_list_files,
                    pos_sp_o_adjacency_list_files,
                    neg_s_p_adjacency_list_files,
                    neg_sp_o_adjacency_list_files,

                    node_dictionary_builder,
                    predicate_dictionary_builder,
                    value_dictionary_builder
                } = self;
                Box::new(value_dictionary_builder.add(value)
                         .map(move|(result, value_dictionary_builder)| (offset+result, ChildLayerFileBuilder {
                             parent,

                             node_dictionary_files,
                             predicate_dictionary_files,
                             value_dictionary_files,

                             subjects_file,
                             subjects,

                             pos_s_p_adjacency_list_files,
                             pos_sp_o_adjacency_list_files,
                             neg_s_p_adjacency_list_files,
                             neg_sp_o_adjacency_list_files,

                             node_dictionary_builder,
                             predicate_dictionary_builder,
                             value_dictionary_builder
                         })))
            },
            Some(id) => Box::new(future::ok((id,self)))
        }
    }

    pub fn add_subject(mut self, subject: u64) -> Self {
        self.subjects.push(subject);
        self
    }

    pub fn add_subjects<I:'static+IntoIterator<Item=u64>>(self, subjects: I) -> Self {
        subjects.into_iter().fold(self, |b,s| b.add_subject(s))
    }

    pub fn add_nodes<I:'static+IntoIterator<Item=String>>(self, nodes: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(nodes.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), node|
                  builder.add_node(&node)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    pub fn add_predicates<I:'static+IntoIterator<Item=String>>(self, predicates: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(predicates.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), predicate|
                  builder.add_predicate(&predicate)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    pub fn add_values<I:'static+IntoIterator<Item=String>>(self, values: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error> {
        stream::iter_ok(values.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), value|
                  builder.add_value(&value)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    pub fn into_phase2(self) -> impl Future<Item=ChildLayerFileBuilderPhase2<F>,Error=std::io::Error> {
        let ChildLayerFileBuilder {
            parent,

            node_dictionary_files,
            predicate_dictionary_files,
            value_dictionary_files,

            subjects_file,
            subjects,

            pos_s_p_adjacency_list_files,
            pos_sp_o_adjacency_list_files,
            neg_s_p_adjacency_list_files,
            neg_sp_o_adjacency_list_files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        let finalize_nodedict: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(node_dictionary_builder.finalize());
        let finalize_preddict: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(predicate_dictionary_builder.finalize());
        let finalize_valdict: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(value_dictionary_builder.finalize());
        let max_subject = if subjects.len() == 0 { 0 } else { subjects[subjects.len()-1] };
        let build_subjects_logarray: Box<dyn Future<Item=(),Error=std::io::Error>> = Box::new(LogArrayFileBuilder::new(subjects_file.open_write(), (1.0 + max_subject as f32).log2().ceil() as u8)
                                                                                              .push_all(stream::iter_ok(subjects))
                                                                                              .and_then(|b|b.finalize())
                                                                                              .map(|_|()));

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict, build_subjects_logarray])
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

                future::ok(ChildLayerFileBuilderPhase2::new(parent,
                                                            subjects_file,
                                                            
                                                            pos_s_p_adjacency_list_files.bits_file,
                                                            pos_s_p_adjacency_list_files.blocks_file,
                                                            pos_s_p_adjacency_list_files.sblocks_file,
                                                            pos_s_p_adjacency_list_files.nums_file,

                                                            pos_sp_o_adjacency_list_files.bits_file,
                                                            pos_sp_o_adjacency_list_files.blocks_file,
                                                            pos_sp_o_adjacency_list_files.sblocks_file,
                                                            pos_sp_o_adjacency_list_files.nums_file,

                                                            neg_s_p_adjacency_list_files.bits_file,
                                                            neg_s_p_adjacency_list_files.blocks_file,
                                                            neg_s_p_adjacency_list_files.sblocks_file,
                                                            neg_s_p_adjacency_list_files.nums_file,

                                                            neg_sp_o_adjacency_list_files.bits_file,
                                                            neg_sp_o_adjacency_list_files.blocks_file,
                                                            neg_sp_o_adjacency_list_files.sblocks_file,
                                                            neg_sp_o_adjacency_list_files.nums_file,

                                                            num_nodes,
                                                            num_predicates,
                                                            num_values))
            })
    }
}

pub struct ChildLayerFileBuilderPhase2<F:'static+FileLoad+FileStore> {
    parent: ParentLayer<F::Map>,

    subjects: MonotonicLogArray<F::Map>,

    pos_s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    pos_sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    pos_last_subject: u64,
    pos_last_predicate: u64,

    neg_s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    neg_sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    neg_last_subject: u64,
    neg_last_predicate: u64,
}

impl<F:'static+FileLoad+FileStore> ChildLayerFileBuilderPhase2<F> {
    fn new(parent: ParentLayer<F::Map>,
           subjects_file: F,

           pos_s_p_adjacency_list_bits_file: F,
           pos_s_p_adjacency_list_blocks_file: F,
           pos_s_p_adjacency_list_sblocks_file: F,
           pos_s_p_adjacency_list_nums_file: F,
           
           pos_sp_o_adjacency_list_bits_file: F,
           pos_sp_o_adjacency_list_blocks_file: F,
           pos_sp_o_adjacency_list_sblocks_file: F,
           pos_sp_o_adjacency_list_nums_file: F,

           neg_s_p_adjacency_list_bits_file: F,
           neg_s_p_adjacency_list_blocks_file: F,
           neg_s_p_adjacency_list_sblocks_file: F,
           neg_s_p_adjacency_list_nums_file: F,
           
           neg_sp_o_adjacency_list_bits_file: F,
           neg_sp_o_adjacency_list_blocks_file: F,
           neg_sp_o_adjacency_list_sblocks_file: F,
           neg_sp_o_adjacency_list_nums_file: F,

           num_nodes: usize,
           num_predicates: usize,
           num_values: usize
    ) -> Self {
        let subjects = MonotonicLogArray::from_logarray(LogArray::parse(subjects_file.map()).unwrap());
        let s_p_width = ((parent.predicate_count() + num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((parent.node_and_value_count() + num_nodes + num_values + 1) as f32).log2().ceil() as u8;
        let pos_s_p_adjacency_list_builder = AdjacencyListBuilder::new(pos_s_p_adjacency_list_bits_file,
                                                                       pos_s_p_adjacency_list_blocks_file.open_write(),
                                                                       pos_s_p_adjacency_list_sblocks_file.open_write(),
                                                                       pos_s_p_adjacency_list_nums_file.open_write(),
                                                                       s_p_width);

        let pos_sp_o_adjacency_list_builder = AdjacencyListBuilder::new(pos_sp_o_adjacency_list_bits_file,
                                                                        pos_sp_o_adjacency_list_blocks_file.open_write(),
                                                                        pos_sp_o_adjacency_list_sblocks_file.open_write(),
                                                                        pos_sp_o_adjacency_list_nums_file.open_write(),
                                                                        sp_o_width);

        let neg_s_p_adjacency_list_builder = AdjacencyListBuilder::new(neg_s_p_adjacency_list_bits_file,
                                                                       neg_s_p_adjacency_list_blocks_file.open_write(),
                                                                       neg_s_p_adjacency_list_sblocks_file.open_write(),
                                                                       neg_s_p_adjacency_list_nums_file.open_write(),
                                                                       s_p_width);

        let neg_sp_o_adjacency_list_builder = AdjacencyListBuilder::new(neg_sp_o_adjacency_list_bits_file,
                                                                        neg_sp_o_adjacency_list_blocks_file.open_write(),
                                                                        neg_sp_o_adjacency_list_sblocks_file.open_write(),
                                                                        neg_sp_o_adjacency_list_nums_file.open_write(),
                                                                        sp_o_width);

        ChildLayerFileBuilderPhase2 {
            parent,
            subjects,

            pos_s_p_adjacency_list_builder,
            pos_sp_o_adjacency_list_builder,
            pos_last_subject: 0,
            pos_last_predicate: 0,

            neg_s_p_adjacency_list_builder,
            neg_sp_o_adjacency_list_builder,
            neg_last_subject: 0,
            neg_last_predicate: 0,
        }
    }

    pub fn add_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>> {
        if self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            // TODO maybe return an error instead?
            return Box::new(future::ok(self));
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            subjects,

            pos_s_p_adjacency_list_builder,
            pos_sp_o_adjacency_list_builder,
            pos_last_subject,
            pos_last_predicate,

            neg_s_p_adjacency_list_builder,
            neg_sp_o_adjacency_list_builder,
            neg_last_subject,
            neg_last_predicate,
        } = self;

        // TODO make this a proper error, rather than a panic
        let mapped_subject = subjects.index_of(subject).expect("layer builder doesn't know about subject") as u64 + 1;
        
        if pos_last_subject == subject && pos_last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = pos_s_p_adjacency_list_builder.count() + 1;
            Box::new(pos_sp_o_adjacency_list_builder.push(count, object)
                     .map(move |pos_sp_o_adjacency_list_builder| {
                         ChildLayerFileBuilderPhase2 {
                             parent,
                             subjects,
                             
                             pos_s_p_adjacency_list_builder,
                             pos_sp_o_adjacency_list_builder,
                             pos_last_subject: mapped_subject,
                             pos_last_predicate: predicate,

                             neg_s_p_adjacency_list_builder,
                             neg_sp_o_adjacency_list_builder,
                             neg_last_subject,
                             neg_last_predicate,
                         }
                     }))
        }
        else {
            // both list have to be pushed to
            Box::new(
                pos_s_p_adjacency_list_builder.push(mapped_subject, predicate)
                    .and_then(move |pos_s_p_adjacency_list_builder| {
                        let count = pos_s_p_adjacency_list_builder.count() + 1;
                        pos_sp_o_adjacency_list_builder.push(count, object)
                            .map(move |pos_sp_o_adjacency_list_builder| {
                                ChildLayerFileBuilderPhase2 {
                                    parent,
                                    subjects,
                                    
                                    pos_s_p_adjacency_list_builder,
                                    pos_sp_o_adjacency_list_builder,
                                    pos_last_subject: subject,
                                    pos_last_predicate: predicate,

                                    neg_s_p_adjacency_list_builder,
                                    neg_sp_o_adjacency_list_builder,
                                    neg_last_subject,
                                    neg_last_predicate,
                                }
                            })
                    }))
        }
    }

    pub fn remove_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>> {
        if !self.parent.triple_exists(subject, predicate, object) {
            // no need to do anything
            // TODO maybe return an error instead?
            return Box::new(future::ok(self));
        }

        let ChildLayerFileBuilderPhase2 {
            parent,
            subjects,

            pos_s_p_adjacency_list_builder,
            pos_sp_o_adjacency_list_builder,
            pos_last_subject,
            pos_last_predicate,

            neg_s_p_adjacency_list_builder,
            neg_sp_o_adjacency_list_builder,
            neg_last_subject,
            neg_last_predicate,
        } = self;

        // TODO make this a proper error, rather than a panic
        let mapped_subject = subjects.index_of(subject).expect("layer builder doesn't know about subject") as u64 + 1;
        
        if neg_last_subject == subject && neg_last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = neg_s_p_adjacency_list_builder.count() + 1;
            Box::new(neg_sp_o_adjacency_list_builder.push(count, object)
                     .map(move |neg_sp_o_adjacency_list_builder| {
                         ChildLayerFileBuilderPhase2 {
                             parent,
                             subjects,
                             
                             pos_s_p_adjacency_list_builder,
                             pos_sp_o_adjacency_list_builder,
                             pos_last_subject,
                             pos_last_predicate,

                             neg_s_p_adjacency_list_builder,
                             neg_sp_o_adjacency_list_builder,
                             neg_last_subject: mapped_subject,
                             neg_last_predicate: predicate,
                         }
                     }))
        }
        else {
            // both list have to be pushed to
            Box::new(
                neg_s_p_adjacency_list_builder.push(mapped_subject, predicate)
                    .and_then(move |neg_s_p_adjacency_list_builder| {
                        let count = neg_s_p_adjacency_list_builder.count() + 1;
                        neg_sp_o_adjacency_list_builder.push(count, object)
                            .map(move |neg_sp_o_adjacency_list_builder| {
                                ChildLayerFileBuilderPhase2 {
                                    parent,
                                    subjects,
                                    
                                    pos_s_p_adjacency_list_builder,
                                    pos_sp_o_adjacency_list_builder,
                                    pos_last_subject,
                                    pos_last_predicate,

                                    neg_s_p_adjacency_list_builder,
                                    neg_sp_o_adjacency_list_builder,
                                    neg_last_subject: subject,
                                    neg_last_predicate: predicate,
                                }
                            })
                    }))
        }
    }

    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        future::join_all(vec![self.pos_s_p_adjacency_list_builder.finalize(), self.pos_sp_o_adjacency_list_builder.finalize(), self.neg_s_p_adjacency_list_builder.finalize(), self.neg_sp_o_adjacency_list_builder.finalize()])
            .map(|_|())
    }
}

#[cfg(test)]
mod tests {
    use crate::structure::storage::*;
    use crate::layer::base::*;
    use super::*;
    use tokio;
    
    #[test]
    fn empty_child_layer_equivalent_to_parent() {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_builder = BaseLayerFileBuilder::new(base_files[0].clone(), base_files[1].clone(), base_files[2].clone(), base_files[3].clone(), base_files[4].clone(), base_files[5].clone(), base_files[6].clone(), base_files[7].clone(), base_files[8].clone(), base_files[9].clone(), base_files[10].clone(), base_files[11].clone(), base_files[12].clone(), base_files[13].clone());

        let future = base_builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
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

        let base_layer = BaseLayer::load(base_files[0].clone().map(), base_files[1].clone().map(), base_files[2].clone().map(), base_files[3].clone().map(), base_files[4].clone().map(), base_files[5].clone().map(), base_files[6].clone().map(), base_files[7].clone().map(), base_files[8].clone().map(), base_files[9].clone().map(), base_files[10].clone().map(), base_files[11].clone().map(), base_files[12].clone().map(), base_files[13].clone().map());

        let parent = ParentLayer::Base(base_layer);

        let child_files: Vec<_> = (0..23).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone());
        child_builder.into_phase2()
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map());

        assert!(child_layer.triple_exists(1,1,1));
        assert!(child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }
    
    #[test]
    fn child_layer_can_have_inserts() {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_builder = BaseLayerFileBuilder::new(base_files[0].clone(), base_files[1].clone(), base_files[2].clone(), base_files[3].clone(), base_files[4].clone(), base_files[5].clone(), base_files[6].clone(), base_files[7].clone(), base_files[8].clone(), base_files[9].clone(), base_files[10].clone(), base_files[11].clone(), base_files[12].clone(), base_files[13].clone());

        let future = base_builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
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

        let base_layer = BaseLayer::load(base_files[0].clone().map(), base_files[1].clone().map(), base_files[2].clone().map(), base_files[3].clone().map(), base_files[4].clone().map(), base_files[5].clone().map(), base_files[6].clone().map(), base_files[7].clone().map(), base_files[8].clone().map(), base_files[9].clone().map(), base_files[10].clone().map(), base_files[11].clone().map(), base_files[12].clone().map(), base_files[13].clone().map());

        let parent = ParentLayer::Base(base_layer);

        let child_files: Vec<_> = (0..23).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone());
        child_builder.add_subjects(vec![2,3]).into_phase2()
            .and_then(|b| b.add_triple(2,1,2))
            .and_then(|b| b.add_triple(3,3,3))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map());

        assert!(child_layer.triple_exists(1,1,1));
        assert!(child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,2));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,3));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }
    
    #[test]
    fn child_layer_can_have_deletes() {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_builder = BaseLayerFileBuilder::new(base_files[0].clone(), base_files[1].clone(), base_files[2].clone(), base_files[3].clone(), base_files[4].clone(), base_files[5].clone(), base_files[6].clone(), base_files[7].clone(), base_files[8].clone(), base_files[9].clone(), base_files[10].clone(), base_files[11].clone(), base_files[12].clone(), base_files[13].clone());

        let future = base_builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
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

        let base_layer = BaseLayer::load(base_files[0].clone().map(), base_files[1].clone().map(), base_files[2].clone().map(), base_files[3].clone().map(), base_files[4].clone().map(), base_files[5].clone().map(), base_files[6].clone().map(), base_files[7].clone().map(), base_files[8].clone().map(), base_files[9].clone().map(), base_files[10].clone().map(), base_files[11].clone().map(), base_files[12].clone().map(), base_files[13].clone().map());

        let parent = ParentLayer::Base(base_layer);

        let child_files: Vec<_> = (0..23).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone());
        child_builder.add_subjects(vec![1,2,3]).into_phase2()
            .and_then(|b| b.remove_triple(2,1,1))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map());

        assert!(child_layer.triple_exists(1,1,1));
        assert!(!child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(!child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }
    
    #[test]
    fn child_layer_can_have_inserts_and_deletes() {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_builder = BaseLayerFileBuilder::new(base_files[0].clone(), base_files[1].clone(), base_files[2].clone(), base_files[3].clone(), base_files[4].clone(), base_files[5].clone(), base_files[6].clone(), base_files[7].clone(), base_files[8].clone(), base_files[9].clone(), base_files[10].clone(), base_files[11].clone(), base_files[12].clone(), base_files[13].clone());

        let future = base_builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
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

        let base_layer = BaseLayer::load(base_files[0].clone().map(), base_files[1].clone().map(), base_files[2].clone().map(), base_files[3].clone().map(), base_files[4].clone().map(), base_files[5].clone().map(), base_files[6].clone().map(), base_files[7].clone().map(), base_files[8].clone().map(), base_files[9].clone().map(), base_files[10].clone().map(), base_files[11].clone().map(), base_files[12].clone().map(), base_files[13].clone().map());

        let parent = ParentLayer::Base(base_layer);

        let child_files: Vec<_> = (0..23).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone());
        child_builder.add_subjects(vec![1,2,3]).into_phase2()
            .and_then(|b| b.add_triple(1,2,3))
            .and_then(|b| b.add_triple(2,3,4))
            .and_then(|b| b.remove_triple(3,2,5))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map());

        assert!(child_layer.triple_exists(1,1,1));
        assert!(child_layer.triple_exists(1,2,3));
        assert!(child_layer.triple_exists(2,1,1));
        assert!(child_layer.triple_exists(2,1,3));
        assert!(child_layer.triple_exists(2,3,4));
        assert!(child_layer.triple_exists(2,3,6));
        assert!(!child_layer.triple_exists(3,2,5));
        assert!(child_layer.triple_exists(3,3,6));
        assert!(child_layer.triple_exists(4,3,6));

        assert!(!child_layer.triple_exists(2,2,0));
    }

    #[test]
    fn adding_new_nodes_predicates_and_values_in_child() {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        let base_builder = BaseLayerFileBuilder::new(base_files[0].clone(), base_files[1].clone(), base_files[2].clone(), base_files[3].clone(), base_files[4].clone(), base_files[5].clone(), base_files[6].clone(), base_files[7].clone(), base_files[8].clone(), base_files[9].clone(), base_files[10].clone(), base_files[11].clone(), base_files[12].clone(), base_files[13].clone());

        let future = base_builder.add_nodes(nodes.into_iter().map(|s|s.to_string()))
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

        let base_layer = BaseLayer::load(base_files[0].clone().map(), base_files[1].clone().map(), base_files[2].clone().map(), base_files[3].clone().map(), base_files[4].clone().map(), base_files[5].clone().map(), base_files[6].clone().map(), base_files[7].clone().map(), base_files[8].clone().map(), base_files[9].clone().map(), base_files[10].clone().map(), base_files[11].clone().map(), base_files[12].clone().map(), base_files[13].clone().map());

        let parent = ParentLayer::Base(base_layer);

        let child_files: Vec<_> = (0..23).map(|_| MemoryBackedStore::new()).collect();

        let child_builder = ChildLayerFileBuilder::new(parent.clone(), child_files[0].clone(), child_files[1].clone(), child_files[2].clone(), child_files[3].clone(), child_files[4].clone(), child_files[5].clone(), child_files[6].clone(), child_files[7].clone(), child_files[8].clone(), child_files[9].clone(), child_files[10].clone(), child_files[11].clone(), child_files[12].clone(), child_files[13].clone(), child_files[14].clone(), child_files[15].clone(), child_files[16].clone(), child_files[17].clone(), child_files[18].clone(), child_files[19].clone(), child_files[20].clone(), child_files[21].clone(), child_files[22].clone());
        child_builder.add_subjects(vec![11,12]).into_phase2()
            .and_then(|b| b.add_triple(11,2,3))
            .and_then(|b| b.add_triple(12,3,4))
            .and_then(|b|b.finalize()).wait().unwrap();

        let child_layer = ChildLayer::load(parent, child_files[0].clone().map(), child_files[1].clone().map(), child_files[2].clone().map(), child_files[3].clone().map(), child_files[4].clone().map(), child_files[5].clone().map(), child_files[6].clone().map(), child_files[7].clone().map(), child_files[8].clone().map(), child_files[9].clone().map(), child_files[10].clone().map(), child_files[11].clone().map(), child_files[12].clone().map(), child_files[13].clone().map(), child_files[14].clone().map(), child_files[15].clone().map(), child_files[16].clone().map(), child_files[17].clone().map(), child_files[18].clone().map(), child_files[19].clone().map(), child_files[20].clone().map(), child_files[21].clone().map(), child_files[22].clone().map());

        assert!(child_layer.triple_exists(11,2,3));
        assert!(child_layer.triple_exists(12,3,4));
    }
}
