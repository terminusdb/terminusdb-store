//! In-memory implementation of storage traits.

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use bytes::Bytes;
use futures::future::{self, Future};
use futures::io;
use futures::task::{Context, Poll};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{self, Arc, RwLock};

use super::*;
use crate::layer::{
    delta_rollup, delta_rollup_upto, BaseLayer, ChildLayer, IdTriple, InternalLayer, LayerBuilder,
    RollupLayer, SimpleLayerBuilder,
};

pub struct MemoryBackedStoreWriter {
    vec: Arc<sync::RwLock<Vec<u8>>>,
    pos: usize,
}

impl std::io::Write for MemoryBackedStoreWriter {
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
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(std::io::Write::write(self.get_mut(), buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
        Poll::Ready(std::io::Write::flush(self.get_mut()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.poll_flush(cx)
    }
}

pub struct MemoryBackedStoreReader {
    vec: Arc<sync::RwLock<Vec<u8>>>,
    pos: usize,
}

impl std::io::Read for MemoryBackedStoreReader {
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

impl AsyncRead for MemoryBackedStoreReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        let slice = buf.initialize_unfilled();
        let count = std::io::Read::read(self.get_mut(), slice);
        if count.is_ok() {
            buf.advance(*count.as_ref().unwrap());
        }

        Poll::Ready(count.map(|_| ()))
    }
}

#[derive(Clone, Default)]
pub struct MemoryBackedStore {
    exists: Arc<RwLock<bool>>,
    vec: Arc<sync::RwLock<Vec<u8>>>,
}

impl MemoryBackedStore {
    pub fn new() -> MemoryBackedStore {
        Default::default()
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

    fn map(&self) -> Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>> {
        let vec = self.vec.clone();
        Box::pin(future::lazy(move |_| {
            Ok(Bytes::from(vec.read().unwrap().clone()))
        }))
    }
}

#[derive(Clone, Default)]
pub struct MemoryLayerStore {
    layers: futures_locks::RwLock<
        HashMap<
            [u32; 5],
            (
                Option<[u32; 5]>,
                Option<[u32; 5]>,
                LayerFiles<MemoryBackedStore>,
            ),
        >,
    >,
}

impl MemoryLayerStore {
    pub fn new() -> MemoryLayerStore {
        Default::default()
    }

    fn triple_addition_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<
        Box<
            dyn Future<
                    Output = io::Result<(
                        MemoryBackedStore,
                        AdjacencyListFiles<MemoryBackedStore>,
                        AdjacencyListFiles<MemoryBackedStore>,
                    )>,
                > + Send,
        >,
    > {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                let (s_p_aj_files, sp_o_aj_files, subjects_file);
                match files {
                    LayerFiles::Base(files) => {
                        s_p_aj_files = files.s_p_adjacency_list_files.clone();
                        sp_o_aj_files = files.sp_o_adjacency_list_files.clone();
                        subjects_file = files.subjects_file.clone();
                    }
                    LayerFiles::Child(files) => {
                        s_p_aj_files = files.pos_s_p_adjacency_list_files.clone();
                        sp_o_aj_files = files.pos_sp_o_adjacency_list_files.clone();
                        subjects_file = files.pos_subjects_file.clone();
                    }
                }

                Ok((subjects_file, s_p_aj_files, sp_o_aj_files))
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn triple_removal_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<
        Box<
            dyn Future<
                    Output = io::Result<
                        Option<(
                            MemoryBackedStore,
                            AdjacencyListFiles<MemoryBackedStore>,
                            AdjacencyListFiles<MemoryBackedStore>,
                        )>,
                    >,
                > + Send,
        >,
    > {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                match files {
                    LayerFiles::Base(_files) => {
                        // base layer has no removals
                        Ok(None)
                    }
                    LayerFiles::Child(files) => {
                        let (s_p_aj_files, sp_o_aj_files, subjects_file);
                        s_p_aj_files = files.neg_s_p_adjacency_list_files.clone();
                        sp_o_aj_files = files.neg_sp_o_adjacency_list_files.clone();
                        subjects_file = files.neg_subjects_file.clone();

                        Ok(Some((subjects_file, s_p_aj_files, sp_o_aj_files)))
                    }
                }
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn predicate_wavelet_addition_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<BitIndexFiles<MemoryBackedStore>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                let predicate_wavelet_files;
                match files {
                    LayerFiles::Base(files) => {
                        predicate_wavelet_files = files.predicate_wavelet_tree_files.clone();
                    }
                    LayerFiles::Child(files) => {
                        predicate_wavelet_files = files.pos_predicate_wavelet_tree_files.clone();
                    }
                }

                Ok(predicate_wavelet_files)
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn predicate_wavelet_removal_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<BitIndexFiles<MemoryBackedStore>>>> + Send>>
    {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                match files {
                    LayerFiles::Base(_files) => {
                        // base layer has no removals
                        Ok(None)
                    }
                    LayerFiles::Child(files) => {
                        let predicate_wavelet_files =
                            files.neg_predicate_wavelet_tree_files.clone();

                        Ok(Some(predicate_wavelet_files))
                    }
                }
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn triple_addition_files_by_object(
        &self,
        layer: [u32; 5],
    ) -> Pin<
        Box<
            dyn Future<
                    Output = io::Result<(
                        MemoryBackedStore,
                        MemoryBackedStore,
                        AdjacencyListFiles<MemoryBackedStore>,
                        AdjacencyListFiles<MemoryBackedStore>,
                    )>,
                > + Send,
        >,
    > {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                let (o_ps_aj_files, s_p_aj_files, subjects_file, objects_file);
                match files {
                    LayerFiles::Base(files) => {
                        o_ps_aj_files = files.o_ps_adjacency_list_files.clone();
                        s_p_aj_files = files.s_p_adjacency_list_files.clone();
                        subjects_file = files.subjects_file.clone();
                        objects_file = files.objects_file.clone();
                    }
                    LayerFiles::Child(files) => {
                        o_ps_aj_files = files.pos_o_ps_adjacency_list_files.clone();
                        s_p_aj_files = files.pos_s_p_adjacency_list_files.clone();
                        subjects_file = files.pos_subjects_file.clone();
                        objects_file = files.pos_objects_file.clone();
                    }
                }

                Ok((subjects_file, objects_file, o_ps_aj_files, s_p_aj_files))
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn triple_removal_files_by_object(
        &self,
        layer: [u32; 5],
    ) -> Pin<
        Box<
            dyn Future<
                    Output = io::Result<
                        Option<(
                            MemoryBackedStore,
                            MemoryBackedStore,
                            AdjacencyListFiles<MemoryBackedStore>,
                            AdjacencyListFiles<MemoryBackedStore>,
                        )>,
                    >,
                > + Send,
        >,
    > {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                match files {
                    LayerFiles::Base(_files) => {
                        // base layer has no removals
                        Ok(None)
                    }
                    LayerFiles::Child(files) => {
                        let o_ps_aj_files = files.neg_o_ps_adjacency_list_files.clone();
                        let s_p_aj_files = files.neg_s_p_adjacency_list_files.clone();
                        let subjects_file = files.neg_subjects_file.clone();
                        let objects_file = files.neg_objects_file.clone();

                        Ok(Some((
                            subjects_file,
                            objects_file,
                            o_ps_aj_files,
                            s_p_aj_files,
                        )))
                    }
                }
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn triple_layer_addition_count_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<
        Box<
            dyn Future<
                    Output = io::Result<(
                        MemoryBackedStore,
                        MemoryBackedStore,
                        BitIndexFiles<MemoryBackedStore>,
                    )>,
                > + Send,
        >,
    > {
        let guard = self.layers.read();
        let predicate_wavelet_files_fut = self.predicate_wavelet_addition_files(layer);
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                let (s_p_nums_file, sp_o_bits_file);
                match files {
                    LayerFiles::Base(files) => {
                        s_p_nums_file = files.s_p_adjacency_list_files.nums_file.clone();
                        sp_o_bits_file = files
                            .sp_o_adjacency_list_files
                            .bitindex_files
                            .bits_file
                            .clone();
                    }
                    LayerFiles::Child(files) => {
                        s_p_nums_file = files.pos_s_p_adjacency_list_files.nums_file.clone();
                        sp_o_bits_file = files
                            .pos_sp_o_adjacency_list_files
                            .bitindex_files
                            .bits_file
                            .clone();
                    }
                }

                let predicate_wavelet_files = predicate_wavelet_files_fut.await?;
                Ok((s_p_nums_file, sp_o_bits_file, predicate_wavelet_files))
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn triple_layer_removal_count_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<
        Box<
            dyn Future<
                    Output = io::Result<
                        Option<(
                            MemoryBackedStore,
                            MemoryBackedStore,
                            BitIndexFiles<MemoryBackedStore>,
                        )>,
                    >,
                > + Send,
        >,
    > {
        let guard = self.layers.read();
        let predicate_wavelet_files_fut = self.predicate_wavelet_addition_files(layer);
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                match files {
                    LayerFiles::Base(_files) => Ok(None),
                    LayerFiles::Child(files) => {
                        let s_p_nums_file = files.pos_s_p_adjacency_list_files.nums_file.clone();
                        let sp_o_bits_file = files
                            .pos_sp_o_adjacency_list_files
                            .bitindex_files
                            .bits_file
                            .clone();
                        let predicate_wavelet_files = predicate_wavelet_files_fut.await?;

                        Ok(Some((
                            s_p_nums_file,
                            sp_o_bits_file,
                            predicate_wavelet_files,
                        )))
                    }
                }
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }
}

pub fn base_layer_memory_files() -> BaseLayerFiles<MemoryBackedStore> {
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

        id_map_files: IdMapFiles {
            node_value_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            predicate_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
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

pub fn child_layer_memory_files() -> ChildLayerFiles<MemoryBackedStore> {
    ChildLayerFiles {
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

        id_map_files: IdMapFiles {
            node_value_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            predicate_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
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
    }
}

impl LayerStore for MemoryLayerStore {
    fn layers(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async {
            let layers = guard.await;
            Ok(layers.keys().cloned().collect())
        })
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        if let Some(layer) = cache.get_layer_from_cache(name) {
            return Box::pin(future::ok(Some(layer)));
        }

        let mut layers_to_load: Vec<([u32; 5], Option<([u32; 5], Option<[u32; 5]>)>)> =
            vec![(name, None)];

        let guard = self.layers.read();
        Box::pin(async move {
            let layers = guard.await;

            if layers.get(&name).is_none() {
                return Ok(None);
            }

            // find an ancestor in cache
            let mut ancestor = None;
            loop {
                let (last, _) = *layers_to_load.last().unwrap();
                match cache.get_layer_from_cache(last) {
                    Some(layer) => {
                        // remove found cached layer from ids to retrieve
                        layers_to_load.pop().unwrap();
                        ancestor = Some(layer);
                        break;
                    }
                    None => {
                        let (parent, rollup, _files) = layers.get(&last).unwrap();
                        if rollup.is_some() {
                            let rollup = rollup.unwrap();

                            if rollup == last {
                                panic!("infinite rollup loop for layer {:?}", rollup);
                            }

                            layers_to_load.pop().unwrap(); // we don't want to load this, we want to load the rollup instead!
                            layers_to_load.push((last, Some((rollup, *parent))));
                        } else if parent.is_some() {
                            let parent = parent.unwrap();
                            layers_to_load.push((parent, None));
                        } else {
                            break;
                        }
                    }
                }
            }

            if ancestor.is_none() {
                // load the base layer
                let (base_id, rollup) = layers_to_load.pop().unwrap();
                let layer: Arc<InternalLayer>;
                match rollup {
                    None => {
                        let (_, _, files) = layers.get(&base_id).unwrap();
                        let base_layer =
                            BaseLayer::load_from_files(base_id, &files.clone().into_base()).await?;

                        layer = Arc::new(base_layer.into());
                    }
                    Some((rollup_id, original_parent_id_option)) => {
                        let (_, _, files) = layers.get(&rollup_id).unwrap();
                        let base_layer: Arc<InternalLayer> = Arc::new(
                            BaseLayer::load_from_files(rollup_id, &files.clone().into_base())
                                .await?
                                .into(),
                        );
                        cache.cache_layer(base_layer.clone());

                        layer = Arc::new(
                            RollupLayer::from_base_layer(
                                base_layer,
                                base_id,
                                original_parent_id_option,
                            )
                            .into(),
                        );
                    }
                }

                cache.cache_layer(layer.clone());
                ancestor = Some(layer);
            }

            let mut ancestor = ancestor.unwrap();
            layers_to_load.reverse();

            for (layer_id, rollup) in layers_to_load {
                let layer: Arc<InternalLayer>;
                match rollup {
                    None => {
                        let (_, _, files) = layers.get(&layer_id).unwrap();
                        let child_layer = ChildLayer::load_from_files(
                            layer_id,
                            ancestor,
                            &files.clone().into_child(),
                        )
                        .await?;
                        layer = Arc::new(child_layer.into());
                    }
                    Some((rollup_id, original_parent_id_option)) => {
                        let original_parent_id = original_parent_id_option
                            .expect("child rollup layer should always have original parent id");

                        let (_, _, files) = layers.get(&rollup_id).unwrap();
                        let child_layer: Arc<InternalLayer> = Arc::new(
                            ChildLayer::load_from_files(
                                rollup_id,
                                ancestor,
                                &files.clone().into_child(),
                            )
                            .await?
                            .into(),
                        );
                        cache.cache_layer(child_layer.clone());

                        layer = Arc::new(
                            RollupLayer::from_child_layer(
                                child_layer,
                                layer_id,
                                original_parent_id,
                            )
                            .into(),
                        );
                    }
                }

                cache.cache_layer(layer.clone());
                ancestor = layer;
            }

            Ok(Some(ancestor))
        })
    }
    /*
        {
        if let Some(layer) = cache.get_layer_from_cache(name) {
            return Box::pin(future::ok(Some(layer)));
        }

        let guard = self.layers.read();
        Box::pin(async move {
            let layers = guard.await;

            let mut ids = Vec::new();
            // collect ids until we get a cache hit
            let mut id = name;
            let mut first = true;
            let mut cached = None;
            loop {
                match cache.get_layer_from_cache(id) {
                    None => {
                        if let Some((parent, rollup, _)) = layers.get(&id) {
                            first = false;
                            match rollup {
                                None => {
                                    match parent {
                                        None => break, // we traversed all the way to the base layer without finding a cached layer
                                        Some(parent) => {
                                            id = *parent;
                                        }
                                    }
                                    ids.push(id);
                                }
                                Some(rollup) => {
                                    id = rollup;
                                }
                            }
                        } else if first {
                            // the requested layer does not exist.
                            return Ok(None);
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

            let mut layer = match cached {
                None => {
                    // construct base layer out of last id, then pop it
                    let base_id = ids.pop().unwrap();
                    let (_, _, files) = layers.get(&base_id).unwrap();
                    let base_layer =
                        BaseLayer::load_from_files(base_id, &files.clone().into_base()).await?;

                    let result = Arc::new(base_layer.into()) as Arc<InternalLayer>;
                    cache.cache_layer(result.clone());

                    result
                }
                Some(layer) => layer,
            };

            ids.reverse();

            for id in ids {
                let (_, _, files) = layers.get(&id).unwrap();
                let child =
                    ChildLayer::load_from_files(id, layer, &files.clone().into_child()).await?;

                layer = Arc::new(child.into()) as Arc<InternalLayer>;
                cache.cache_layer(layer.clone());
            }

            Ok(Some(layer))
        })
    }
    */

    fn create_base_layer(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        let name = rand::random();
        let blf = base_layer_memory_files();

        let guard = self.layers.write();
        Box::pin(async move {
            let mut layers = guard.await;
            layers.insert(name, (None, None, LayerFiles::Base(blf.clone())));
            Ok(Box::new(SimpleLayerBuilder::new(name, blf)) as Box<dyn LayerBuilder>)
        })
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        let layers = self.layers.clone();
        let get_layer_with_cache = self.get_layer_with_cache(parent, cache);
        Box::pin(async move {
            let parent_layer_opt = get_layer_with_cache.await?;
            let parent_layer = match parent_layer_opt {
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found",
                )),
                Some(parent_layer) => Ok::<_, io::Error>(parent_layer),
            }?;

            let name = rand::random();
            let clf = child_layer_memory_files();

            layers
                .write()
                .await
                .insert(name, (Some(parent), None, LayerFiles::Child(clf.clone())));
            Ok(
                Box::new(SimpleLayerBuilder::from_parent(name, parent_layer, clf))
                    as Box<dyn LayerBuilder>,
            )
        })
    }

    fn perform_rollup(
        &self,
        layer: Arc<InternalLayer>,
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let name = rand::random();
        let blf = base_layer_memory_files();

        let layers = self.layers.clone();
        Box::pin(async move {
            delta_rollup(&layer, blf.clone()).await?;
            layers
                .write()
                .await
                .insert(name, (None, None, LayerFiles::Base(blf)));

            Ok(name)
        })
    }

    fn perform_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        _cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let name = rand::random();
        let clf = child_layer_memory_files();

        let layers = self.layers.clone();
        Box::pin(async move {
            delta_rollup_upto(&layer, upto, clf.clone()).await?;
            layers
                .write()
                .await
                .insert(name, (Some(upto), None, LayerFiles::Child(clf)));

            Ok(name)
        })
    }

    fn register_rollup(
        &self,
        layer: [u32; 5],
        rollup: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        // make sure that layer and rollup are already in the big table
        let layers = self.layers.clone();
        Box::pin(async move {
            let mut map = layers.write().await;

            // todo check if this rollup is valid

            if !map.contains_key(&layer) {
                // i dunno some kind of error
                Err(io::Error::new(io::ErrorKind::Other, "layer does not exist"))
            } else if !map.contains_key(&rollup) {
                // i dunno some kind of error
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "rollup does not exist",
                ))
            } else {
                let (parent, files) = {
                    let (p, _, f) = &map[&layer];

                    (*p, f.clone())
                };
                map.insert(layer, (parent, Some(rollup), files));

                Ok(())
            }
        })
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
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            let layers = guard.await;

            let mut d = descendant;
            loop {
                if d == ancestor {
                    return Ok(true);
                }

                match layers.get(&d) {
                    Some((Some(parent), _, _)) => d = *parent,
                    _ => return Ok(false),
                }
            }
        })
    }

    fn triple_addition_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let files_fut = self.triple_addition_files(layer);
        Box::pin(async move {
            let (subjects_file, s_p_aj_files, sp_o_aj_files) = files_fut.await?;

            file_triple_exists(
                subjects_file,
                s_p_aj_files,
                sp_o_aj_files,
                subject,
                predicate,
                object,
            )
            .await
        })
    }

    fn triple_removal_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let files_fut = self.triple_removal_files(layer);
        Box::pin(async move {
            if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) = files_fut.await? {
                file_triple_exists(
                    subjects_file,
                    s_p_aj_files,
                    sp_o_aj_files,
                    subject,
                    predicate,
                    object,
                )
                .await
            } else {
                Ok(false)
            }
        })
    }

    fn triple_additions(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let files_fut = self.triple_addition_files(layer);
        Box::pin(async move {
            let (subjects_file, s_p_aj_files, sp_o_aj_files) = files_fut.await?;

            Ok(
                Box::new(file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files).await?)
                    as Box<dyn Iterator<Item = _> + Send>,
            )
        })
    }

    fn triple_removals(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let files_fut = self.triple_removal_files(layer);
        Box::pin(async move {
            if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) = files_fut.await? {
                Ok(
                    Box::new(
                        file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files).await?,
                    ) as Box<dyn Iterator<Item = _> + Send>,
                )
            } else {
                Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
            }
        })
    }

    fn triple_additions_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let self_ = self.clone();
        Box::pin(async move {
            let (subjects_file, s_p_aj_files, sp_o_aj_files) =
                self_.triple_addition_files(layer).await?;

            Ok(Box::new(
                file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                    .await?
                    .seek_subject(subject)
                    .take_while(move |t| t.subject == subject),
            ) as Box<dyn Iterator<Item = _> + Send>)
        })
    }

    fn triple_removals_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let files_fut = self.triple_removal_files(layer);
        Box::pin(async move {
            if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) = files_fut.await? {
                Ok(Box::new(
                    file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                        .await?
                        .seek_subject(subject)
                        .take_while(move |t| t.subject == subject),
                ) as Box<dyn Iterator<Item = _> + Send>)
            } else {
                Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
            }
        })
    }

    fn triple_additions_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let self_ = self.clone();
        Box::pin(async move {
            let (subjects_file, s_p_aj_files, sp_o_aj_files) =
                self_.triple_addition_files(layer).await?;

            Ok(Box::new(
                file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                    .await?
                    .seek_subject_predicate(subject, predicate)
                    .take_while(move |t| t.predicate == predicate && t.subject == subject),
            ) as Box<dyn Iterator<Item = _> + Send>)
        })
    }

    fn triple_removals_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let files_fut = self.triple_removal_files(layer);
        Box::pin(async move {
            if let Some((subjects_file, s_p_aj_files, sp_o_aj_files)) = files_fut.await? {
                Ok(Box::new(
                    file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files)
                        .await?
                        .seek_subject_predicate(subject, predicate)
                        .take_while(move |t| t.predicate == predicate && t.subject == subject),
                ) as Box<dyn Iterator<Item = _> + Send>)
            } else {
                Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
            }
        })
    }

    fn triple_additions_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let self_ = self.clone();
        Box::pin(async move {
            let (subjects_file, s_p_aj_files, sp_o_aj_files) =
                self_.triple_addition_files(layer).await?;
            let predicate_wavelet_files = self_.predicate_wavelet_addition_files(layer).await?;

            Ok(Box::new(
                file_triple_iterator_by_predicate(
                    subjects_file,
                    s_p_aj_files,
                    sp_o_aj_files,
                    predicate_wavelet_files,
                    predicate,
                )
                .await?,
            ) as Box<dyn Iterator<Item = _> + Send>)
        })
    }

    fn triple_removals_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let files_fut = self.triple_removal_files(layer);
        let wavelet_files_fut = self.predicate_wavelet_removal_files(layer);
        Box::pin(async move {
            if let (
                Some((subjects_file, s_p_aj_files, sp_o_aj_files)),
                Some(predicate_wavelet_files),
            ) = (files_fut.await?, wavelet_files_fut.await?)
            {
                Ok(Box::new(
                    file_triple_iterator_by_predicate(
                        subjects_file,
                        s_p_aj_files,
                        sp_o_aj_files,
                        predicate_wavelet_files,
                        predicate,
                    )
                    .await?,
                ) as Box<dyn Iterator<Item = _> + Send>)
            } else {
                Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
            }
        })
    }

    fn triple_additions_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let self_ = self.clone();
        Box::pin(async move {
            let (subjects_file, objects_file, o_ps_aj_files, s_p_aj_files) =
                self_.triple_addition_files_by_object(layer).await?;

            Ok(Box::new(
                file_triple_iterator_by_object(
                    subjects_file,
                    objects_file,
                    o_ps_aj_files,
                    s_p_aj_files,
                    object,
                )
                .await?
                .take_while(move |t| t.object == object),
            ) as Box<dyn Iterator<Item = _> + Send>)
        })
    }

    fn triple_removals_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        let self_ = self.clone();
        Box::pin(async move {
            if let Some((subjects_file, objects_file, o_ps_aj_files, s_p_aj_files)) =
                self_.triple_removal_files_by_object(layer).await?
            {
                Ok(Box::new(
                    file_triple_iterator_by_object(
                        subjects_file,
                        objects_file,
                        o_ps_aj_files,
                        s_p_aj_files,
                        object,
                    )
                    .await?
                    .take_while(move |t| t.object == object),
                ) as Box<dyn Iterator<Item = _> + Send>)
            } else {
                Ok(Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _> + Send>)
            }
        })
    }

    fn triple_layer_addition_count(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send>> {
        let files_fut = self.triple_layer_addition_count_files(layer);
        Box::pin(async move {
            let (s_p_nums_file, sp_o_bits_file, predicate_wavelet_files) = files_fut.await?;
            file_triple_layer_count(s_p_nums_file, sp_o_bits_file, predicate_wavelet_files).await
        })
    }

    fn triple_layer_removal_count(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send>> {
        let files_fut = self.triple_layer_removal_count_files(layer);
        Box::pin(async move {
            if let Some((s_p_nums_file, sp_o_bits_file, predicate_wavelet_files)) =
                files_fut.await?
            {
                file_triple_layer_count(s_p_nums_file, sp_o_bits_file, predicate_wavelet_files)
                    .await
            } else {
                Ok(0)
            }
        })
    }
}

#[derive(Clone, Default)]
pub struct MemoryLabelStore {
    labels: futures_locks::RwLock<HashMap<String, Label>>,
}

impl MemoryLabelStore {
    pub fn new() -> MemoryLabelStore {
        Default::default()
    }
}

impl LabelStore for MemoryLabelStore {
    fn labels(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<Label>>> + Send>> {
        let guard = self.labels.read();
        Box::pin(async move {
            let labels = guard.await;
            Ok(labels.values().cloned().collect())
        })
    }

    fn create_label(&self, name: &str) -> Pin<Box<dyn Future<Output = io::Result<Label>> + Send>> {
        let label = Label::new_empty(name);

        let guard = self.labels.write();
        Box::pin(async move {
            let mut labels = guard.await;
            if labels.get(&label.name).is_some() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "label already exists",
                ))
            } else {
                labels.insert(label.name.clone(), label.clone());
                Ok(label)
            }
        })
    }

    fn get_label(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>> {
        let name = name.to_owned();
        let guard = self.labels.read();
        Box::pin(async move {
            let labels = guard.await;
            Ok(labels.get(&name).cloned())
        })
    }

    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>> {
        let new_label = label.with_updated_layer(layer);

        let guard = self.labels.write();
        Box::pin(async move {
            let mut labels = guard.await;

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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn write_and_read_memory_backed() {
        let file = MemoryBackedStore::new();

        let mut w = file.open_write();
        let buf = async {
            w.write_all(&[1, 2, 3]).await?;
            let mut result = Vec::new();
            file.open_read().read_to_end(&mut result).await?;

            Ok::<_, io::Error>(result)
        }
        .await
        .unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[tokio::test]
    async fn write_and_map_memory_backed() {
        let file = MemoryBackedStore::new();

        let mut w = file.open_write();
        let map = async {
            w.write_all(&[1, 2, 3]).await?;
            file.map().await
        }
        .await
        .unwrap();

        assert_eq!(vec![1, 2, 3], map.as_ref());
    }

    #[tokio::test]
    async fn create_layers_from_memory_store() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let layer = store.get_layer(child_name).await.unwrap().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn memory_create_and_retrieve_equal_label() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").await.unwrap();
        assert_eq!(foo, store.get_label("foo").await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn memory_update_label_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").await.unwrap();

        assert_eq!(
            1,
            store
                .set_label(&foo, [6, 7, 8, 9, 10])
                .await
                .unwrap()
                .unwrap()
                .version
        );

        assert_eq!(1, store.get_label("foo").await.unwrap().unwrap().version);
    }

    #[tokio::test]
    async fn memory_update_label_twice_from_same_label_object_fails() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").await.unwrap();

        assert!(store
            .set_label(&foo, [6, 7, 8, 9, 10])
            .await
            .unwrap()
            .is_some());
        assert!(store
            .set_label(&foo, [1, 1, 1, 1, 1])
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn memory_update_label_twice_from_updated_label_object_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").await.unwrap();

        let foo2 = store
            .set_label(&foo, [6, 7, 8, 9, 10])
            .await
            .unwrap()
            .unwrap();
        assert!(store
            .set_label(&foo2, [1, 1, 1, 1, 1])
            .await
            .unwrap()
            .is_some());
    }
}
