//! In-memory implementation of storage traits.

use bytes::Bytes;
use futures_locks;
use std::collections::HashMap;
use std::io;
use std::sync::{self, Arc, RwLock};
use tokio::prelude::*;

use super::*;
use crate::layer::{BaseLayer, ChildLayer, InternalLayer, Layer, LayerBuilder, SimpleLayerBuilder};

pub struct MemoryBackedStoreWriter {
    vec: Arc<sync::RwLock<Vec<u8>>>,
    pos: usize,
}

impl Write for MemoryBackedStoreWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let mut v = self.vec.write().unwrap();
        if v.len() - self.pos < buf.len() {
            v.resize(self.pos + buf.len(), 0);
        }

        v[self.pos..self.pos + buf.len()].copy_from_slice(buf);

        self.pos += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl AsyncWrite for MemoryBackedStoreWriter {
    fn shutdown(&mut self) -> Result<Async<()>, io::Error> {
        Ok(Async::Ready(()))
    }
}

pub struct MemoryBackedStoreReader {
    vec: Arc<sync::RwLock<Vec<u8>>>,
    pos: usize,
}

impl Read for MemoryBackedStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let v = self.vec.read().unwrap();

        if self.pos >= v.len() {
            return Ok(0);
        }

        let slice = &v[self.pos..];
        if slice.len() >= buf.len() {
            buf.copy_from_slice(&slice[..buf.len()]);
            self.pos += buf.len();

            Ok(buf.len())
        } else {
            buf[..slice.len()].copy_from_slice(slice);
            self.pos += slice.len();

            Ok(slice.len())
        }
    }
}

impl AsyncRead for MemoryBackedStoreReader {}

#[derive(Clone)]
pub struct MemoryBackedStore {
    exists: Arc<RwLock<bool>>,
    vec: Arc<sync::RwLock<Vec<u8>>>,
}

impl MemoryBackedStore {
    pub fn new() -> MemoryBackedStore {
        MemoryBackedStore {
            vec: Default::default(),
            exists: Arc::new(RwLock::new(false)),
        }
    }
}

impl FileStore for MemoryBackedStore {
    type Write = MemoryBackedStoreWriter;

    fn open_write_from(&self, pos: usize) -> MemoryBackedStoreWriter {
        *self.exists.write().unwrap() = true;
        MemoryBackedStoreWriter {
            vec: self.vec.clone(),
            pos,
        }
    }
}

impl FileLoad for MemoryBackedStore {
    type Read = MemoryBackedStoreReader;

    fn exists(&self) -> bool {
        return *self.exists.read().unwrap();
    }

    fn size(&self) -> usize {
        self.vec.read().unwrap().len()
    }

    fn open_read_from(&self, offset: usize) -> MemoryBackedStoreReader {
        MemoryBackedStoreReader {
            vec: self.vec.clone(),
            pos: offset,
        }
    }

    fn map(&self) -> Box<dyn Future<Item = Bytes, Error = std::io::Error> + Send> {
        let vec = self.vec.clone();
        Box::new(future::lazy(move || {
            future::ok(Bytes::from(vec.read().unwrap().clone()))
        }))
    }
}

#[derive(Clone)]
pub struct MemoryLayerStore {
    layers:
        futures_locks::RwLock<HashMap<[u32; 5], (Option<[u32; 5]>, LayerFiles<MemoryBackedStore>)>>,
}

impl MemoryLayerStore {
    pub fn new() -> MemoryLayerStore {
        MemoryLayerStore {
            layers: futures_locks::RwLock::new(HashMap::new()),
        }
    }
}

impl LayerStore for MemoryLayerStore {
    fn layers(&self) -> Box<dyn Future<Item = Vec<[u32; 5]>, Error = io::Error> + Send> {
        Box::new(self.layers.read().then(|layers| {
            Ok(layers
                .expect("rwlock read cannot fail")
                .keys()
                .map(|k| k.clone())
                .collect())
        }))
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Option<Arc<dyn Layer>>, Error = io::Error> + Send> {
        if let Some(layer) = cache.get_layer_from_cache(name) {
            return Box::new(future::ok(Some(layer as Arc<dyn Layer>)));
        }

        // not in cache, time to do a clever

        Box::new(
            self.layers
                .read()
                .then(move |layers| {
                    let layers = layers.expect("rwlock read should always succeed");

                    let mut ids = Vec::new();
                    // collect ids until we get a cache hit
                    let mut id = name;
                    let mut first = true;
                    let mut cached = None;
                    loop {
                        match cache.get_layer_from_cache(id) {
                            None => {
                                ids.push(id);
                                if let Some((parent, _)) = layers.get(&id) {
                                    first = false;
                                    match parent {
                                        None => break, // we traversed all the way to the base layer without finding a cached layer
                                        Some(parent) => {
                                            id = *parent;
                                        }
                                    }
                                } else if first {
                                    // the requested layer does not exist.
                                    return future::Either::A(future::ok(None));
                                } else {
                                    // layer parent does not exist. this should never happen
                                    panic!("expected to find parent layer, but not found");
                                }
                            }
                            Some(layer) => {
                                // great, we found a cached layer, now we can iteratively build all the child layers.
                                cached = Some(layer);
                                break;
                            }
                        }
                    }

                    // at this point we have a list of layer ids, and optionally, we have a cached layer
                    // starting with the cached layer, we need to construct child layers iteratively.
                    // lacking a cached layer, the very last item in the vec is a base layer and that is our starting point.

                    let cache2 = cache.clone();

                    let layer_future = match cached {
                        None => {
                            // construct base layer out of last id, then pop it
                            let base_id = ids.pop().unwrap();
                            let (_, files) = layers.get(&base_id).unwrap();
                            future::Either::A(
                                BaseLayer::load_from_files(base_id, &files.clone().into_base())
                                    .map(move |l| {
                                        let result = Arc::new(l.into()) as Arc<InternalLayer>;
                                        cache.cache_layer(result.clone());

                                        result
                                    }),
                            )
                        }
                        Some(layer) => future::Either::B(future::ok(layer)),
                    };

                    ids.reverse();

                    future::Either::B(
                        layer_future
                            .and_then(move |layer| {
                                stream::iter_ok(ids).fold(layer, move |layer, id| {
                                    let (_, files) = layers.get(&id).unwrap();
                                    let cache = cache2.clone();
                                    ChildLayer::load_from_files(
                                        name,
                                        layer,
                                        &files.clone().into_child(),
                                    )
                                    .map(move |l| {
                                        let result = Arc::new(l.into()) as Arc<InternalLayer>;
                                        cache.cache_layer(result.clone());
                                        result
                                    })
                                })
                            })
                            .map(move |l| Some(l)),
                    )
                })
                .map(|l| l.map(|layer| layer as Arc<dyn Layer>)),
        )
    }

    fn create_base_layer(
        &self,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        let name = rand::random();
        let blf = BaseLayerFiles {
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
        };

        Box::new(self.layers.write().then(move |layers| {
            layers
                .expect("rwlock write should always succeed")
                .insert(name, (None, LayerFiles::Base(blf.clone())));
            Ok(Box::new(SimpleLayerBuilder::new(name, blf)) as Box<dyn LayerBuilder>)
        }))
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Box<dyn Future<Item = Box<dyn LayerBuilder>, Error = io::Error> + Send> {
        let layers = self.layers.clone();
        Box::new(
            self.get_layer_with_cache(parent, cache)
                .and_then(|parent_layer| match parent_layer {
                    None => future::err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "parent layer not found",
                    )),
                    Some(parent_layer) => future::ok(parent_layer),
                })
                .and_then(move |parent_layer| {
                    let name = rand::random();
                    let clf = ChildLayerFiles {
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

                        pos_subjects_file: MemoryBackedStore::new(),
                        pos_objects_file: MemoryBackedStore::new(),
                        neg_subjects_file: MemoryBackedStore::new(),
                        neg_objects_file: MemoryBackedStore::new(),

                        pos_s_p_adjacency_list_files: AdjacencyListFiles {
                            bitindex_files: BitIndexFiles {
                                bits_file: MemoryBackedStore::new(),
                                blocks_file: MemoryBackedStore::new(),
                                sblocks_file: MemoryBackedStore::new(),
                            },
                            nums_file: MemoryBackedStore::new(),
                        },
                        pos_sp_o_adjacency_list_files: AdjacencyListFiles {
                            bitindex_files: BitIndexFiles {
                                bits_file: MemoryBackedStore::new(),
                                blocks_file: MemoryBackedStore::new(),
                                sblocks_file: MemoryBackedStore::new(),
                            },
                            nums_file: MemoryBackedStore::new(),
                        },
                        pos_o_ps_adjacency_list_files: AdjacencyListFiles {
                            bitindex_files: BitIndexFiles {
                                bits_file: MemoryBackedStore::new(),
                                blocks_file: MemoryBackedStore::new(),
                                sblocks_file: MemoryBackedStore::new(),
                            },
                            nums_file: MemoryBackedStore::new(),
                        },
                        neg_s_p_adjacency_list_files: AdjacencyListFiles {
                            bitindex_files: BitIndexFiles {
                                bits_file: MemoryBackedStore::new(),
                                blocks_file: MemoryBackedStore::new(),
                                sblocks_file: MemoryBackedStore::new(),
                            },
                            nums_file: MemoryBackedStore::new(),
                        },
                        neg_sp_o_adjacency_list_files: AdjacencyListFiles {
                            bitindex_files: BitIndexFiles {
                                bits_file: MemoryBackedStore::new(),
                                blocks_file: MemoryBackedStore::new(),
                                sblocks_file: MemoryBackedStore::new(),
                            },
                            nums_file: MemoryBackedStore::new(),
                        },
                        neg_o_ps_adjacency_list_files: AdjacencyListFiles {
                            bitindex_files: BitIndexFiles {
                                bits_file: MemoryBackedStore::new(),
                                blocks_file: MemoryBackedStore::new(),
                                sblocks_file: MemoryBackedStore::new(),
                            },
                            nums_file: MemoryBackedStore::new(),
                        },
                        pos_predicate_wavelet_tree_files: BitIndexFiles {
                            bits_file: MemoryBackedStore::new(),
                            blocks_file: MemoryBackedStore::new(),
                            sblocks_file: MemoryBackedStore::new(),
                        },
                        neg_predicate_wavelet_tree_files: BitIndexFiles {
                            bits_file: MemoryBackedStore::new(),
                            blocks_file: MemoryBackedStore::new(),
                            sblocks_file: MemoryBackedStore::new(),
                        },
                    };

                    layers.write().then(move |layers| {
                        layers
                            .expect("rwlock write should always succeed")
                            .insert(name, (Some(parent), LayerFiles::Child(clf.clone())));
                        Ok(
                            Box::new(SimpleLayerBuilder::from_parent(name, parent_layer, clf))
                                as Box<dyn LayerBuilder>,
                        )
                    })
                }),
        )
    }

    fn export_layers(&self, _layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        unimplemented!();
    }
    fn import_layers(
        &self,
        _pack: &[u8],
        _layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        unimplemented!();
    }

    fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> Box<dyn Future<Item = bool, Error = io::Error> + Send> {
        Box::new(
            self.layers
                .read()
                .map(move |layers| {
                    let mut d = descendant;
                    loop {
                        if d == ancestor {
                            return true;
                        }

                        match layers.get(&d) {
                            Some((Some(parent), _)) => d = *parent,
                            _ => return false,
                        }
                    }
                })
                .map_err(|_| panic!("no errors expected")),
        )
    }
}

#[derive(Clone)]
pub struct MemoryLabelStore {
    labels: futures_locks::RwLock<HashMap<String, Label>>,
}

impl MemoryLabelStore {
    pub fn new() -> MemoryLabelStore {
        MemoryLabelStore {
            labels: futures_locks::RwLock::new(HashMap::new()),
        }
    }
}

impl LabelStore for MemoryLabelStore {
    fn labels(&self) -> Box<dyn Future<Item = Vec<Label>, Error = std::io::Error> + Send> {
        Box::new(self.labels.read().then(|l| {
            Ok(l.expect("rwlock read should always succeed")
                .values()
                .map(|v| v.clone())
                .collect())
        }))
    }

    fn create_label(
        &self,
        name: &str,
    ) -> Box<dyn Future<Item = Label, Error = std::io::Error> + Send> {
        let label = Label::new_empty(name);

        Box::new(self.labels.write().then(move |l| {
            let mut labels = l.expect("rwlock write should always succeed");
            if labels.get(&label.name).is_some() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "label already exists",
                ))
            } else {
                labels.insert(label.name.clone(), label.clone());
                Ok(label)
            }
        }))
    }

    fn get_label(
        &self,
        name: &str,
    ) -> Box<dyn Future<Item = Option<Label>, Error = std::io::Error> + Send> {
        let name = name.to_owned();
        Box::new(self.labels.read().then(move |l| {
            Ok(l.expect("rwlock read should always succeed")
                .get(&name)
                .map(|label| label.clone()))
        }))
    }

    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> Box<dyn Future<Item = Option<Label>, Error = std::io::Error> + Send> {
        let new_label = label.with_updated_layer(layer);

        Box::new(self.labels.write().then(move |l| {
            let mut labels = l.expect("rwlock write should always succeed");

            match labels.get(&new_label.name) {
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "label does not exist",
                )),
                Some(old_label) => {
                    if old_label.version + 1 != new_label.version {
                        Ok(None)
                    } else {
                        labels.insert(new_label.name.clone(), new_label.clone());

                        Ok(Some(new_label))
                    }
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;

    #[test]
    fn write_and_read_memory_backed() {
        let file = MemoryBackedStore::new();

        let w = file.open_write();
        let buf = tokio::io::write_all(w, [1, 2, 3])
            .and_then(move |_| tokio::io::read_to_end(file.open_read(), Vec::new()))
            .map(|(_, buf)| buf)
            .wait()
            .unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[test]
    fn write_and_map_memory_backed() {
        let file = MemoryBackedStore::new();

        let w = file.open_write();
        tokio::io::write_all(w, [1, 2, 3]).wait().unwrap();

        assert_eq!(vec![1, 2, 3], file.map().wait().unwrap().as_ref());
    }

    #[test]
    fn create_layers_from_memory_store() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().wait().unwrap();
        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(&StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(&StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().wait().unwrap();

        builder = store.create_child_layer(base_name).wait().unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(&StringTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().wait().unwrap();

        let layer = store.get_layer(child_name).wait().unwrap().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[test]
    fn memory_create_and_retrieve_equal_label() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();
        assert_eq!(foo, store.get_label("foo").wait().unwrap().unwrap());
    }

    #[test]
    fn memory_update_label_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();

        assert_eq!(
            1,
            store
                .set_label(&foo, [6, 7, 8, 9, 10])
                .wait()
                .unwrap()
                .unwrap()
                .version
        );

        assert_eq!(1, store.get_label("foo").wait().unwrap().unwrap().version);
    }

    #[test]
    fn memory_update_label_twice_from_same_label_object_fails() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();

        assert!(store
            .set_label(&foo, [6, 7, 8, 9, 10])
            .wait()
            .unwrap()
            .is_some());
        assert!(store
            .set_label(&foo, [1, 1, 1, 1, 1])
            .wait()
            .unwrap()
            .is_none());
    }

    #[test]
    fn memory_update_label_twice_from_updated_label_object_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").wait().unwrap();

        let foo2 = store
            .set_label(&foo, [6, 7, 8, 9, 10])
            .wait()
            .unwrap()
            .unwrap();
        assert!(store
            .set_label(&foo2, [1, 1, 1, 1, 1])
            .wait()
            .unwrap()
            .is_some());
    }
}
