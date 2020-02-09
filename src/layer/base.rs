//! Base layer implementation.
//!
//! A base layer stores triple data without referring to a parent.
use futures::prelude::*;
use futures::future;
use futures::stream;
use futures::stream::Peekable;

use crate::structure::*;
use crate::storage::*;
use super::layer::*;

use std::collections::BTreeSet;
use std::io;

/// A base layer.
///
/// This layer type has no parent, and therefore does not store any
/// additions or removals. It stores all its triples plus indexes
/// directly.
#[derive(Clone)]
pub struct BaseLayer<M:'static+AsRef<[u8]>+Clone+Send+Sync> {
    name: [u32;5],
    node_dictionary: PfcDict<M>,
    predicate_dictionary: PfcDict<M>,
    value_dictionary: PfcDict<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>,
    o_ps_adjacency_list: AdjacencyList<M>,

    predicate_wavelet_tree: WaveletTree<M>,
}

impl<M:'static+AsRef<[u8]>+Clone+Send+Sync> BaseLayer<M> {
    pub fn load_from_files<F:FileLoad<Map=M>+FileStore>(name: [u32;5], files: &BaseLayerFiles<F>) -> impl Future<Item=Self,Error=std::io::Error> {
        files.map_all()
            .map(move |maps| Self::load(name, maps))
    }

    pub fn load(name: [u32;5],
                maps: BaseLayerMaps<M>) -> BaseLayer<M> {
        let node_dictionary = PfcDict::parse(maps.node_dictionary_maps.blocks_map, maps.node_dictionary_maps.offsets_map).unwrap();
        let predicate_dictionary = PfcDict::parse(maps.predicate_dictionary_maps.blocks_map, maps.predicate_dictionary_maps.offsets_map).unwrap();
        let value_dictionary = PfcDict::parse(maps.value_dictionary_maps.blocks_map, maps.value_dictionary_maps.offsets_map).unwrap();

        let s_p_adjacency_list = AdjacencyList::parse(maps.s_p_adjacency_list_maps.nums_map, maps.s_p_adjacency_list_maps.bitindex_maps.bits_map, maps.s_p_adjacency_list_maps.bitindex_maps.blocks_map, maps.s_p_adjacency_list_maps.bitindex_maps.sblocks_map);
        let sp_o_adjacency_list = AdjacencyList::parse(maps.sp_o_adjacency_list_maps.nums_map, maps.sp_o_adjacency_list_maps.bitindex_maps.bits_map, maps.sp_o_adjacency_list_maps.bitindex_maps.blocks_map, maps.sp_o_adjacency_list_maps.bitindex_maps.sblocks_map);
        let o_ps_adjacency_list = AdjacencyList::parse(maps.o_ps_adjacency_list_maps.nums_map, maps.o_ps_adjacency_list_maps.bitindex_maps.bits_map, maps.o_ps_adjacency_list_maps.bitindex_maps.blocks_map, maps.o_ps_adjacency_list_maps.bitindex_maps.sblocks_map);

        let predicate_wavelet_tree_width = s_p_adjacency_list.nums().width();
        let predicate_wavelet_tree = WaveletTree::from_parts(BitIndex::from_maps(maps.predicate_wavelet_tree_maps.bits_map,
                                                                                 maps.predicate_wavelet_tree_maps.blocks_map,
                                                                                 maps.predicate_wavelet_tree_maps.sblocks_map),
                                                             predicate_wavelet_tree_width);

        BaseLayer {
            name,
            node_dictionary,
            predicate_dictionary,
            value_dictionary,

            s_p_adjacency_list,
            sp_o_adjacency_list,

            o_ps_adjacency_list,

            predicate_wavelet_tree,
        }
    }
}

impl<M:'static+AsRef<[u8]>+Clone+Send+Sync> Layer for BaseLayer<M> {
    fn name(&self) -> [u32;5] {
        self.name
    }

    fn parent(&self) -> Option<&dyn Layer> {
        None
    }

    fn subjects(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        Box::new(BaseSubjectIterator {
            pos: 0,
            s_p_adjacency_list: self.s_p_adjacency_list.clone(),
            sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
        })
    }

    fn subjects_current_layer(&self, _parent: Box<dyn Iterator<Item=Box<dyn SubjectLookup>>>) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        self.subjects()
    }

    fn subject_additions(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        self.subjects()
    }

    fn subject_removals(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectLookup>>> {
        Box::new(std::iter::empty())
    }

    fn node_dict_len(&self) -> usize {
        self.node_dictionary.len()
    }

    fn node_dict_get(&self, id: usize) -> Option<String> {
        self.node_dictionary.get(id)
    }

    fn predicate_dict_get(&self, id: usize) -> Option<String> {
        self.predicate_dictionary.get(id)
    }

    fn value_dict_get(&self, id: usize) -> Option<String> {
        self.value_dictionary.get(id)
    }

    fn predicate_dict_len(&self) -> usize {
        self.predicate_dictionary.len()
    }

    fn value_dict_len(&self) -> usize {
        self.value_dictionary.len()
    }

    fn node_and_value_count(&self) -> usize {
        self.node_dictionary.len() + self.value_dictionary.len()
    }

    fn predicate_count(&self) -> usize {
        self.predicate_dictionary.len()
    }

    fn node_dict_id(&self, subject: &str) -> Option<u64> {
        self.node_dictionary.id(&subject)
    }

    fn predicate_dict_id(&self, predicate: &str) -> Option<u64> {
        self.predicate_dictionary.id(predicate)
    }

    fn value_dict_id(&self, value: &str) -> Option<u64> {
        self.value_dictionary.id(value)
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
        self.node_dict_get(corrected_id as usize)
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;
        self.predicate_dict_get(corrected_id as usize)
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id == 0 {
            return None;
        }
        let corrected_id = id - 1;

        if corrected_id >= (self.node_dictionary.len() as u64) {
            let val_id = corrected_id - (self.node_dictionary.len() as u64);
            self.value_dict_get(val_id as usize).map(ObjectType::Value)
        }
        else {
            self.node_dictionary.get(corrected_id as usize).map(ObjectType::Node)
        }
    }


    fn lookup_subject_current_layer(&self, subject: u64, _parent: Option<Box<dyn SubjectLookup>>) -> Option<Box<dyn SubjectLookup>> {
        self.lookup_subject(subject)
    }

    fn lookup_subject(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        if subject == 0 || subject >= (self.s_p_adjacency_list.left_count() + 1) as u64 {
            None
        }
        else {
            Some(Box::new(BaseSubjectLookup {
                subject: subject,
                predicates: self.s_p_adjacency_list.get(subject),
                sp_offset: self.s_p_adjacency_list.offset_for(subject),
                sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
            }))
        }
    }

    fn lookup_subject_addition(&self, subject: u64) -> Option<Box<dyn SubjectLookup>> {
        self.lookup_subject(subject)
    }

    fn lookup_subject_removal(&self, _subject: u64) -> Option<Box<dyn SubjectLookup>> {
        None
    }

    fn objects(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        // todo: there might be a more efficient method than doing
        // this lookup over and over, due to sequentiality of the
        // underlying data structures
        let cloned = self.clone();
        Box::new((0..self.node_and_value_count())
                 .map(move |object| cloned.lookup_object((object+1) as u64).unwrap()))
    }

    fn object_additions(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        self.objects()
    }

    fn object_removals(&self) -> Box<dyn Iterator<Item=Box<dyn ObjectLookup>>> {
        Box::new(std::iter::empty())
    }

    fn lookup_object_current_layer(&self, object: u64, _parent: Option<Box<dyn ObjectLookup>>) -> Option<Box<dyn ObjectLookup>> {
        self.lookup_object(object)
    }

    fn lookup_object(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        if object == 0 || object > self.node_and_value_count() as u64 {
            None
        }
        else {
            let sp_slice = self.o_ps_adjacency_list.get(object);
            Some(Box::new(BaseObjectLookup {
                object,
                sp_slice,
                s_p_adjacency_list: self.s_p_adjacency_list.clone(),
            }))
        }
    }

    fn lookup_object_addition(&self, object: u64) -> Option<Box<dyn ObjectLookup>> {
        self.lookup_object(object)
    }

    fn lookup_object_removal(&self, _object: u64) -> Option<Box<dyn ObjectLookup>> {
        None
    }

    fn lookup_predicate_current_layer(&self, predicate: u64, _parent: Option<Box<dyn PredicateLookup>>) -> Option<Box<dyn PredicateLookup>> {
        self.lookup_predicate(predicate)
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let sp_o_adjacency_list = self.sp_o_adjacency_list.clone();
        self.predicate_wavelet_tree.lookup(predicate)
            .map(|lookup| Box::new(BasePredicateLookup {
                lookup,
                s_p_adjacency_list,
                sp_o_adjacency_list
            }) as Box<dyn PredicateLookup>)
    }

    fn lookup_predicate_addition(&self, predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        self.lookup_predicate(predicate)
    }

    fn lookup_predicate_removal(&self, _predicate: u64) -> Option<Box<dyn PredicateLookup>> {
        None
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct BaseSubjectIterator<M:'static+AsRef<[u8]>+Clone> {
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>,
    pos: u64,
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for BaseSubjectIterator<M> {
    type Item = Box<dyn SubjectLookup>;

    fn next(&mut self) -> Option<Box<dyn SubjectLookup>> {
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
                Some(Box::new(BaseSubjectLookup {
                    subject,
                    predicates: self.s_p_adjacency_list.get(subject),
                    sp_offset: self.s_p_adjacency_list.offset_for(subject),
                    sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
                }))
            }
        }
    }
}

#[derive(Clone)]
struct BaseSubjectLookup<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> SubjectLookup for BaseSubjectLookup<M> {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn parent(&self) -> Option<&dyn SubjectLookup> {
        None
    }

    fn predicates_current_layer(&self, _parent: Option<Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>>>) -> Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>> {
        self.predicates()
    }

    fn predicates(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>> {
        Box::new(BasePredicateIterator {
            subject: self.subject,
            pos: 0,
            predicates: self.predicates.clone(),
            sp_offset: self.sp_offset,
            sp_o_adjacency_list: self.sp_o_adjacency_list.clone()
        })
    }

    fn lookup_predicate_current(&self, predicate: u64, _parent: Option<Box<dyn SubjectPredicateLookup>>) -> Option<Box<dyn SubjectPredicateLookup>> {
        self.lookup_predicate(predicate)
    }

    fn lookup_predicate(&self, predicate: u64) -> Option<Box<dyn SubjectPredicateLookup>> {
        let pos = self.predicates.iter().position(|p| p == predicate);
        match pos {
            None => None,
            Some(pos) => Some(Box::new(BaseSubjectPredicateLookup {
                subject: self.subject,
                predicate: predicate,
                objects: self.sp_o_adjacency_list.get(self.sp_offset+(pos as u64)+1)
            }))
        }
    }
}

#[derive(Clone)]
struct BasePredicateIterator<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    pos: usize,
    predicates: LogArraySlice<M>,
    sp_offset: u64,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for BasePredicateIterator<M> {
    type Item = Box<dyn SubjectPredicateLookup>;

    fn next(&mut self) -> Option<Box<dyn SubjectPredicateLookup>> {
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
                Some(Box::new(BaseSubjectPredicateLookup {
                    subject: self.subject,
                    predicate,
                    objects
                }))
            }
        }
    }
}

#[derive(Clone)]
struct BaseSubjectPredicateLookup<M:'static+AsRef<[u8]>+Clone> {
    subject: u64,
    predicate: u64,
    objects: LogArraySlice<M>
}

impl<M:'static+AsRef<[u8]>+Clone> SubjectPredicateLookup for BaseSubjectPredicateLookup<M> {
    fn subject(&self) -> u64 {
        self.subject
    }

    fn predicate(&self) -> u64 {
        self.predicate
    }

    fn objects(&self) -> Box<dyn Iterator<Item=u64>> {
        Box::new(BaseObjectIterator {
            subject: self.subject,
            predicate: self.predicate,
            objects: self.objects.clone(),
            pos: 0
        })
    }

    fn has_pos_object_in_lookup(&self, object: u64) -> bool {
        // todo: use monotoniclogarray here to find object quicker
        self.objects.iter().find(|&o|o==object).is_some()
    }

    fn has_neg_object_in_lookup(&self, _object: u64) -> bool {
        false
    }

    fn has_object(&self, object: u64) -> bool {
        self.has_pos_object_in_lookup(object)
    }

    fn parent(&self) -> Option<&dyn SubjectPredicateLookup> {
        None
    }
}

#[derive(Clone)]
struct BaseObjectIterator<M:'static+AsRef<[u8]>+Clone> {
    pub subject: u64,
    pub predicate: u64,
    objects: LogArraySlice<M>,
    pos: usize
}

impl<M:'static+AsRef<[u8]>+Clone> Iterator for BaseObjectIterator<M> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
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
                Some(object)
            }
        }
    }
}

#[derive(Clone)]
struct BaseObjectLookup<M:AsRef<[u8]>+Clone> {
    object: u64,
    sp_slice: LogArraySlice<M>,
    s_p_adjacency_list: AdjacencyList<M>,
}

impl<M:'static+AsRef<[u8]>+Clone> ObjectLookup for BaseObjectLookup<M> {
    fn object(&self) -> u64 {
        self.object
    }

    fn parent(&self) -> Option<&dyn ObjectLookup> {
        None
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item=(u64, u64)>> {
        let cloned = self.clone();
        Box::new(self.sp_slice.clone().into_iter()
                 .filter_map(move |i| {
                     if i == 0 {
                         None
                     }
                     else {
                         Some(cloned.s_p_adjacency_list.pair_at_pos(i-1))
                     }
                 }))
    }
}

struct BasePredicateLookup<M:'static+AsRef<[u8]>+Clone> {
    lookup: WaveletLookup<M>,
    s_p_adjacency_list: AdjacencyList<M>,
    sp_o_adjacency_list: AdjacencyList<M>
}

impl<M:'static+AsRef<[u8]>+Clone> PredicateLookup for BasePredicateLookup<M> {
    fn predicate(&self) -> u64 {
        self.lookup.entry
    }

    fn subject_predicate_pairs(&self) -> Box<dyn Iterator<Item=Box<dyn SubjectPredicateLookup>>> {
        let predicate = self.predicate();
        let s_p_adjacency_list = self.s_p_adjacency_list.clone();
        let sp_o_adjacency_list = self.sp_o_adjacency_list.clone();
        Box::new(self.lookup.iter()
                 .map(move |pos| {
                     let (subject, _) = s_p_adjacency_list.pair_at_pos(pos);
                     let objects = sp_o_adjacency_list.get(pos+1);

                     Box::new(BaseSubjectPredicateLookup {
                         subject,
                         predicate,
                         objects
                     }) as Box<dyn SubjectPredicateLookup>
                 }))
    }
}

/// A builder for a base layer.
///
/// This builder takes node, predicate and value strings in lexical
/// order through the corresponding `add_<thing>` methods. When
/// they're all added, `into_phase2()` is to be called to turn this
/// builder into a second builder that takes triple data.
pub struct BaseLayerFileBuilder<F:'static+FileLoad+FileStore> {
    files: BaseLayerFiles<F>,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F:'static+FileLoad+FileStore+Clone> BaseLayerFileBuilder<F> {
    /// Create the builder from the given files.
    pub fn from_files(files: &BaseLayerFiles<F>) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(files.node_dictionary_files.blocks_file.open_write(), files.node_dictionary_files.offsets_file.open_write());
        let predicate_dictionary_builder = PfcDictFileBuilder::new(files.predicate_dictionary_files.blocks_file.open_write(), files.predicate_dictionary_files.offsets_file.open_write());
        let value_dictionary_builder = PfcDictFileBuilder::new(files.value_dictionary_files.blocks_file.open_write(), files.value_dictionary_files.offsets_file.open_write());

        BaseLayerFileBuilder {
            files: files.clone(),
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder

        }
    }

    /// Add a node string.
    ///
    /// Panics if the given node string is not a lexical successor of the previous node string.
    pub fn add_node(self, node: &str) -> impl Future<Item=(u64, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        node_dictionary_builder.add(node)
            .map(move|(result, node_dictionary_builder)| (result, BaseLayerFileBuilder {
                files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    /// Add a predicate string.
    ///
    /// Panics if the given predicate string is not a lexical successor of the previous node string.
    pub fn add_predicate(self, predicate: &str) -> impl Future<Item=(u64, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        predicate_dictionary_builder.add(predicate)
            .map(move|(result, predicate_dictionary_builder)| (result, BaseLayerFileBuilder {
                files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    /// Add a value string.
    ///
    /// Panics if the given value string is not a lexical successor of the previous value string.
    pub fn add_value(self, value: &str) -> impl Future<Item=(u64, Self), Error=std::io::Error> {
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        value_dictionary_builder.add(value)
            .map(move|(result, value_dictionary_builder)| (result, BaseLayerFileBuilder {
                files,

                node_dictionary_builder,
                predicate_dictionary_builder,
                value_dictionary_builder
            }))
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous added nodes are a lexical succesor of any of these nodes.
    pub fn add_nodes<I:'static+IntoIterator<Item=String>+Send+Sync>(self, nodes: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error>
    where <I as std::iter::IntoIterator>::IntoIter: Send+Sync {
        stream::iter_ok(nodes.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), node|
                  builder.add_node(&node)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if previous added predicates are a lexical succesor of any of these predicates.
    pub fn add_predicates<I:'static+IntoIterator<Item=String>+Send+Sync>(self, predicates: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error>
    where <I as std::iter::IntoIterator>::IntoIter: Send+Sync {
        stream::iter_ok(predicates.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), predicate|
                  builder.add_predicate(&predicate)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous added values are a lexical succesor of any of these values.
    pub fn add_values<I:'static+IntoIterator<Item=String>+Send+Sync>(self, values: I) -> impl Future<Item=(Vec<u64>, Self), Error=std::io::Error>
    where <I as std::iter::IntoIterator>::IntoIter: Send+Sync {
        stream::iter_ok(values.into_iter())
            .fold((Vec::new(), self), |(mut result, builder), value|
                  builder.add_value(&value)
                  .map(|(id, builder)| {
                      result.push(id);

                      (result, builder)
                  }))
    }

    /// Turn this builder into a phase 2 builder that will take triple data.
    pub fn into_phase2(self) -> impl Future<Item=BaseLayerFileBuilderPhase2<F>,Error=std::io::Error> {
        let BaseLayerFileBuilder {
            files,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        let finalize_nodedict = node_dictionary_builder.finalize();
        let finalize_preddict = predicate_dictionary_builder.finalize();
        let finalize_valdict = value_dictionary_builder.finalize();

        let dict_maps_fut = vec![files.node_dictionary_files.blocks_file.map(),
                                 files.node_dictionary_files.offsets_file.map(),
                                 files.predicate_dictionary_files.blocks_file.map(),
                                 files.predicate_dictionary_files.offsets_file.map(),
                                 files.value_dictionary_files.blocks_file.map(),
                                 files.value_dictionary_files.offsets_file.map()];

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

                future::ok(BaseLayerFileBuilderPhase2::new(files,

                                                           num_nodes,
                                                           num_predicates,
                                                           num_values))
            })
    }
}

/// Second phase of base layer building.
///
/// This builder takes ordered triple data. When all data has been
/// added, `finalize()` will build a layer.
pub struct BaseLayerFileBuilderPhase2<F:'static+FileLoad+FileStore> {
    files: BaseLayerFiles<F>,
    s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    last_subject: u64,
    last_predicate: u64,
    object_count: usize
}

impl<F:'static+FileLoad+FileStore> BaseLayerFileBuilderPhase2<F> {
    fn new(files: BaseLayerFiles<F>,

           num_nodes: usize,
           num_predicates: usize,
           num_values: usize
    ) -> Self {
        let s_p_width = ((num_predicates + 1) as f32).log2().ceil() as u8;
        let sp_o_width = ((num_nodes + num_values + 1) as f32).log2().ceil() as u8;
        let f = files.clone();
        let s_p_adjacency_list_builder = AdjacencyListBuilder::new(files.s_p_adjacency_list_files.bitindex_files.bits_file,
                                                                   files.s_p_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                   files.s_p_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                   files.s_p_adjacency_list_files.nums_file.open_write(),
                                                                   s_p_width);

        let sp_o_adjacency_list_builder = AdjacencyListBuilder::new(files.sp_o_adjacency_list_files.bitindex_files.bits_file,
                                                                    files.sp_o_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                    files.sp_o_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                    files.sp_o_adjacency_list_files.nums_file.open_write(),
                                                                    sp_o_width);

        BaseLayerFileBuilderPhase2 {
            files: f,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject: 0,
            last_predicate: 0,
            object_count: num_nodes + num_values
        }
    }

    /// Add the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been added.
    pub fn add_triple(self, subject: u64, predicate: u64, object: u64) -> Box<dyn Future<Item=Self, Error=std::io::Error>+Send> {
        let BaseLayerFileBuilderPhase2 {
            files,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject,
            last_predicate,
            object_count
        } = self;

        if last_subject == subject && last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = s_p_adjacency_list_builder.count() + 1;
            Box::new(sp_o_adjacency_list_builder.push(count, object)
                     .map(move |sp_o_adjacency_list_builder| {
                         BaseLayerFileBuilderPhase2 {
                             files,
                             s_p_adjacency_list_builder,
                             sp_o_adjacency_list_builder,
                             last_subject: subject,
                             last_predicate: predicate,
                             object_count,
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
                                    files,
                                    s_p_adjacency_list_builder,
                                    sp_o_adjacency_list_builder,
                                    last_subject: subject,
                                    last_predicate: predicate,
                                    object_count
                                }
                            })
                    }))
        }
    }

    /// Add the given triple.
    ///
    /// This will panic if a greater triple has already been added.
    pub fn add_id_triples<I:'static+IntoIterator<Item=IdTriple>>(self, triples: I) -> impl Future<Item=Self, Error=std::io::Error> {
        stream::iter_ok(triples)
                 .fold(self, |b, triple| b.add_triple(triple.subject, triple.predicate, triple.object))
    }

    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        let s_p_adjacency_list_files = self.files.s_p_adjacency_list_files;
        let sp_o_adjacency_list_files = self.files.sp_o_adjacency_list_files;
        let o_ps_adjacency_list_files = self.files.o_ps_adjacency_list_files;
        let predicate_wavelet_tree_files = self.files.predicate_wavelet_tree_files;
        let object_count = self.object_count;
        future::join_all(vec![self.s_p_adjacency_list_builder.finalize(), self.sp_o_adjacency_list_builder.finalize()])
            .and_then(move |_| adjacency_list_stream_pairs(sp_o_adjacency_list_files.bitindex_files.bits_file, sp_o_adjacency_list_files.nums_file)
                      .map(|(left, right)| (right, left))
                      .fold((BTreeSet::new(),0), |(mut set, mut greatest_right), (left, right)| {
                          set.insert((left, right));
                          if right > greatest_right {
                              greatest_right = right;
                          }
                          future::ok::<_,std::io::Error>((set, greatest_right))
                      }))
            .and_then(move |(mut tuples, greatest_right)| {
                let (greatest_left,_) = tuples.iter().next_back().unwrap_or(&(0,0));
                for pad_object in (*greatest_left+1)..(object_count as u64)+1 {
                    tuples.insert((pad_object, 0));
                }
                let width = ((greatest_right+1) as f32).log2().ceil() as u8;

                let o_ps_adjacency_list_builder = AdjacencyListBuilder::new(o_ps_adjacency_list_files.bitindex_files.bits_file,
                                                                            o_ps_adjacency_list_files.bitindex_files.blocks_file.open_write(),
                                                                            o_ps_adjacency_list_files.bitindex_files.sblocks_file.open_write(),
                                                                            o_ps_adjacency_list_files.nums_file.open_write(),
                                                                            width);

                let build_o_ps_index = o_ps_adjacency_list_builder.push_all(stream::iter_ok(tuples))
                    .and_then(|builder| builder.finalize());
                let build_predicate_index = build_wavelet_tree_from_logarray(s_p_adjacency_list_files.nums_file,
                                                                             predicate_wavelet_tree_files.bits_file,
                                                                             predicate_wavelet_tree_files.blocks_file,
                                                                             predicate_wavelet_tree_files.sblocks_file);

                build_o_ps_index.join(build_predicate_index)
            })
            .map(|_|())
    }
}

pub struct BaseTripleStream<S: Stream<Item=(u64,u64),Error=io::Error>+Send> {
    s_p_stream: Peekable<S>,
    sp_o_stream: Peekable<S>,
    last_s_p: (u64, u64),
    last_sp: u64
}

impl<S:Stream<Item=(u64,u64),Error=io::Error>+Send> BaseTripleStream<S> {
    fn new(s_p_stream: S, sp_o_stream: S) -> BaseTripleStream<S> {
        BaseTripleStream {
            s_p_stream: s_p_stream.peekable(),
            sp_o_stream: sp_o_stream.peekable(),
            last_s_p: (0,0),
            last_sp: 0
        }
    }
}

impl<S:Stream<Item=(u64,u64),Error=io::Error>+Send> Stream for BaseTripleStream<S> {
    type Item = (u64,u64,u64);
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<(u64,u64,u64)>>,io::Error> {
        let sp_o = self.sp_o_stream.peek().map(|x|x.map(|x|x.map(|x|*x)));
        match sp_o {
            Err(e) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::Ready(Some((sp,o)))) => {
                if sp > self.last_sp {
                    let s_p = self.s_p_stream.peek().map(|x|x.map(|x|x.map(|x|*x)));
                    match s_p {
                        Err(e) => Err(e),
                        Ok(Async::NotReady) => Ok(Async::NotReady),
                        Ok(Async::Ready(None)) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of s_p_stream")),
                        Ok(Async::Ready(Some((s,p)))) => {
                            self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");
                            self.s_p_stream.poll().expect("peeked stream s_p_stream with confirmed result did not have result on poll");
                            self.last_s_p = (s,p);
                            self.last_sp = sp;

                            Ok(Async::Ready(Some((s,p,o))))
                        }
                    }
                }
                else {
                    self.sp_o_stream.poll().expect("peeked stream sp_o_stream with confirmed result did not have result on poll");

                    Ok(Async::Ready(Some((self.last_s_p.0, self.last_s_p.1, o))))
                }
            }
        }
    }
}

pub fn open_base_triple_stream<F:'static+FileLoad+FileStore>(s_p_files: AdjacencyListFiles<F>, sp_o_files: AdjacencyListFiles<F>) -> impl Stream<Item=(u64,u64,u64),Error=io::Error>+Send {
    let s_p_stream = adjacency_list_stream_pairs(s_p_files.bitindex_files.bits_file, s_p_files.nums_file);
    let sp_o_stream = adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file);

    BaseTripleStream::new(s_p_stream, sp_o_stream)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::memory::*;

    pub fn base_layer_files() -> BaseLayerFiles<MemoryBackedStore> {
        let files: Vec<_> = (0..21).map(|_| MemoryBackedStore::new()).collect();
        BaseLayerFiles {
            node_dictionary_files: DictionaryFiles {
                blocks_file: files[0].clone(),
                offsets_file: files[1].clone()
            },
            predicate_dictionary_files: DictionaryFiles {
                blocks_file: files[2].clone(),
                offsets_file: files[3].clone()
            },
            value_dictionary_files: DictionaryFiles {
                blocks_file: files[4].clone(),
                offsets_file: files[5].clone()
            },
            s_p_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[6].clone(),
                    blocks_file: files[7].clone(),
                    sblocks_file: files[8].clone(),
                },
                nums_file: files[9].clone()
            },
            sp_o_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[10].clone(),
                    blocks_file: files[11].clone(),
                    sblocks_file: files[12].clone(),
                },
                nums_file: files[13].clone()
            },
            o_ps_adjacency_list_files: AdjacencyListFiles {
                bitindex_files: BitIndexFiles {
                    bits_file: files[14].clone(),
                    blocks_file: files[15].clone(),
                    sblocks_file: files[16].clone(),
                },
                nums_file: files[17].clone()
            },
            predicate_wavelet_tree_files: BitIndexFiles {
                bits_file: files[18].clone(),
                blocks_file: files[19].clone(),
                sblocks_file: files[20].clone(),
            },
        }
    }

    pub fn example_base_layer_files() -> BaseLayerFiles<MemoryBackedStore> {
        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let base_layer_files = base_layer_files();

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

        base_layer_files
    }

    pub fn example_base_layer() -> BaseLayer<SharedVec> {
        let base_layer_files = example_base_layer_files();

        let layer = BaseLayer::load_from_files([1,2,3,4,5], &base_layer_files).wait().unwrap();

        layer
    }

    pub fn empty_base_layer() -> BaseLayer<SharedVec> {
        let files = base_layer_files();
        let base_builder = BaseLayerFileBuilder::from_files(&files);
        base_builder.into_phase2().and_then(|b|b.finalize()).wait().unwrap();

        BaseLayer::load_from_files([1,2,3,4,5], &files).wait().unwrap()
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
        let subjects: Vec<_> = layer.subjects().map(|s|s.subject()).collect();

        assert_eq!(vec![1,2,3,4], subjects);
    }

    #[test]
    fn predicates_iterator() {
        let layer = example_base_layer();
        let p1: Vec<_> = layer.lookup_subject(1).unwrap().predicates().map(|p|p.predicate()).collect();
        assert_eq!(vec![1], p1);
        let p2: Vec<_> = layer.lookup_subject(2).unwrap().predicates().map(|p|p.predicate()).collect();
        assert_eq!(vec![1,3], p2);
        let p3: Vec<_> = layer.lookup_subject(3).unwrap().predicates().map(|p|p.predicate()).collect();
        assert_eq!(vec![2,3], p3);
        let p4: Vec<_> = layer.lookup_subject(4).unwrap().predicates().map(|p|p.predicate()).collect();
        assert_eq!(vec![3], p4);
    }

    #[test]
    fn objects_iterator() {
        let layer = example_base_layer();
        let objects: Vec<_> = layer
            .lookup_subject(2).unwrap()
            .lookup_predicate(1).unwrap()
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

    #[test]
    fn lookup_by_object() {
        let layer = example_base_layer();

        let lookup = layer.lookup_object(1).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(1,1), (2,1)], pairs);

        let lookup = layer.lookup_object(3).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(2,1)], pairs);

        let lookup = layer.lookup_object(5).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(3,2)], pairs);

        let lookup = layer.lookup_object(6).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs().collect();
        assert_eq!(vec![(2,3), (3,3), (4,3)], pairs);
    }

    #[test]
    fn lookup_by_predicate() {
        let layer = example_base_layer();

        let lookup = layer.lookup_predicate(1).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs()
            .map(|sp|sp.triples())
            .flatten()
            .map(|t|(t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(1,1,1), (2,1,1), (2,1,3)], pairs);

        let lookup = layer.lookup_predicate(2).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs()
            .map(|sp|sp.triples())
            .flatten()
            .map(|t|(t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(3,2,5)], pairs);

        let lookup = layer.lookup_predicate(3).unwrap();
        let pairs: Vec<_> = lookup.subject_predicate_pairs()
            .map(|sp|sp.triples())
            .flatten()
            .map(|t|(t.subject,t.predicate,t.object))
            .collect();

        assert_eq!(vec![(2,3,6),(3,3,6),(4,3,6)], pairs);

        let lookup = layer.lookup_predicate(4);

        assert!(lookup.is_none());
    }

    #[test]
    fn lookup_objects() {
        let layer = example_base_layer();

        let triples_by_object: Vec<_> = layer.objects()
            .map(|o|o.subject_predicate_pairs()
                 .map(move |(s,p)|(s,p,o.object())))
            .flatten()
            .collect();

        assert_eq!(vec![(1,1,1),
                        (2,1,1),
                        (2,1,3),
                        (3,2,5),
                        (2,3,6),
                        (3,3,6),
                        (4,3,6)],
                   triples_by_object);
    }

    #[test]
    fn create_empty_base_layer() {
        let base_layer_files = base_layer_files();
        let builder = BaseLayerFileBuilder::from_files(&base_layer_files);

        let future = builder.into_phase2()
            .and_then(|b| b.finalize());


        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1,2,3,4,5], &base_layer_files).wait().unwrap();
        assert_eq!(0, layer.node_and_value_count());
        assert_eq!(0, layer.predicate_count());
    }

    #[test]
    fn base_layer_with_multiple_pairs_pointing_at_same_object() {
        let base_layer_files = base_layer_files();
        let builder = BaseLayerFileBuilder::from_files(&base_layer_files);

        let future = builder.add_nodes(vec!["a","b"].into_iter().map(|x|x.to_string()))
            .and_then(|(_,b)| b.add_predicates(vec!["c","d"].into_iter().map(|x|x.to_string())))
            .and_then(|(_,b)| b.add_values(vec!["e"].into_iter().map(|x|x.to_string())))
            .and_then(|(_,b)| b.into_phase2())
            .and_then(|b| b.add_triple(1,1,1))
            .and_then(|b| b.add_triple(1,2,1))
            .and_then(|b| b.add_triple(2,1,1))
            .and_then(|b| b.add_triple(2,2,1))
            .and_then(|b| b.finalize());


        future.wait().unwrap();

        let layer = BaseLayer::load_from_files([1,2,3,4,5], &base_layer_files).wait().unwrap();

        let triples_by_object: Vec<_> = layer.objects()
            .map(|o|o.subject_predicate_pairs()
                 .map(move |(s,p)|(s,p,o.object())))
            .flatten()
            .collect();

        assert_eq!(vec![(1,1,1),
                        (1,2,1),
                        (2,1,1),
                        (2,2,1)],
                   triples_by_object);
    }

    #[test]
    fn stream_base_triples() {
        let layer_files = example_base_layer_files();

        let stream = open_base_triple_stream(layer_files.s_p_adjacency_list_files, layer_files.sp_o_adjacency_list_files);

        let triples: Vec<_> = stream.collect().wait().unwrap();

        assert_eq!(vec![(1,1,1),
                        (2,1,1),
                        (2,1,3),
                        (2,3,6),
                        (3,2,5),
                        (3,3,6),
                        (4,3,6)], triples);
    }
}
