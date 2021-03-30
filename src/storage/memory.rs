//! In-memory implementation of storage traits.

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use bytes::Bytes;
use futures::future::{self, Future};
use futures::io;
use futures::task::{Context, Poll};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{self, Arc, RwLock};

use super::delta::*;
use super::*;
use crate::layer::{
    BaseLayer, ChildLayer, IdTriple, InternalLayer, LayerBuilder,
    OptInternalLayerTripleSubjectIterator, RollupLayer, SimpleLayerBuilder,
};
use crate::structure::PfcDict;

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

impl SyncableFile for MemoryBackedStoreWriter {
    fn sync_all(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        // nothing to do
        Box::pin(future::ok(()))
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

    fn layer_files_exist(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move { Ok(guard.await.get(&layer).is_some()) })
    }

    fn node_dictionary_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<DictionaryFiles<MemoryBackedStore>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                Ok(files.node_dictionary_files().clone())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn predicate_dictionary_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<DictionaryFiles<MemoryBackedStore>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                Ok(files.predicate_dictionary_files().clone())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
    }

    fn value_dictionary_files(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<DictionaryFiles<MemoryBackedStore>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                Ok(files.value_dictionary_files().clone())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
            }
        })
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
        let predicate_wavelet_files_fut = self.predicate_wavelet_removal_files(layer);
        Box::pin(async move {
            if let Some((_, _, files)) = guard.await.get(&layer) {
                match files {
                    LayerFiles::Base(_files) => Ok(None),
                    LayerFiles::Child(files) => {
                        let s_p_nums_file = files.neg_s_p_adjacency_list_files.nums_file.clone();
                        let sp_o_bits_file = files
                            .neg_sp_o_adjacency_list_files
                            .bitindex_files
                            .bits_file
                            .clone();
                        let predicate_wavelet_files = predicate_wavelet_files_fut
                            .await?
                            .expect("expected predicate removal wavelet files to exist");

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
                let (last, rollup_option) = *layers_to_load.last().unwrap();
                let current_layer;

                if let Some((rollup, _original_parent)) = rollup_option {
                    current_layer = rollup;
                } else {
                    current_layer = last;
                }
                match cache.get_layer_from_cache(current_layer) {
                    Some(layer) => {
                        // remove found cached layer from ids to retrieve
                        layers_to_load.pop().unwrap();

                        // if this is a rollup, the behavior has to be slightly different
                        if let Some((_, original_parent)) = rollup_option {
                            if layer.immediate_parent().is_some() {
                                ancestor = Some(Arc::new(
                                    RollupLayer::from_child_layer(
                                        layer,
                                        last,
                                        original_parent.unwrap(),
                                    )
                                    .into(),
                                ));
                            } else {
                                ancestor = Some(Arc::new(
                                    RollupLayer::from_base_layer(layer, last, original_parent)
                                        .into(),
                                ));
                            }
                        } else {
                            ancestor = Some(layer);
                        }
                        break;
                    }
                    None => {
                        let (parent, rollup, _files) = layers.get(&current_layer).unwrap();
                        if rollup.is_some() {
                            let rollup = rollup.unwrap();

                            if rollup == current_layer {
                                panic!("infinite rollup loop for layer {:?}", rollup);
                            }

                            layers_to_load.pop().unwrap(); // we don't want to load this, we want to load the rollup instead!
                            layers_to_load.push((current_layer, Some((rollup, *parent))));
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

    fn get_layer_parent_name(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<[u32; 5]>>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            if let Some((parent, _, _)) = self_.layers.read().await.get(&name) {
                Ok(parent.clone())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found",
                ))
            }
        })
    }

    fn get_node_dictionary(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<PfcDict>>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            if !self_.layer_files_exist(name).await? {
                return Ok(None);
            }

            let files = self_.node_dictionary_files(name).await?;
            let maps = files.map_all().await?;

            Ok(Some(PfcDict::parse(maps.blocks_map, maps.offsets_map)?))
        })
    }

    fn get_predicate_dictionary(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<PfcDict>>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            if !self_.layer_files_exist(name).await? {
                return Ok(None);
            }

            let files = self_.predicate_dictionary_files(name).await?;
            let maps = files.map_all().await?;

            Ok(Some(PfcDict::parse(maps.blocks_map, maps.offsets_map)?))
        })
    }

    fn get_value_dictionary(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<PfcDict>>> + Send>> {
        let self_ = self.clone();
        Box::pin(async move {
            if !self_.layer_files_exist(name).await? {
                return Ok(None);
            }

            let files = self_.value_dictionary_files(name).await?;
            let maps = files.map_all().await?;

            Ok(Some(PfcDict::parse(maps.blocks_map, maps.offsets_map)?))
        })
    }

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
        if layer.parent_name().is_none() {
            // we're already a base layer. there's nothing that can be rolled up.
            // returning our own name will inhibit writing a rollup file.
            return Box::pin(future::ok(layer.name()));
        }

        let layers = self.layers.clone();
        Box::pin(async move {
            {
                let layers_r = layers.read().await;
                if let Some((_, Some(rollup), _)) = layers_r.get(&layer.name()) {
                    // the rollup is equivalent if it is a base layer
                    if let Some((rollup_parent, _, _)) = layers_r.get(rollup) {
                        if rollup_parent.is_none() {
                            // yup, equivalent. let's just return the rollup we know about.
                            return Ok(*rollup);
                        }
                    }
                }
            }

            let name = rand::random();
            let blf = base_layer_memory_files();

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
        if layer.parent_name() == Some(upto) {
            // rolling up to our parent is just going to create a clone of this child layer. Let's not do that.
            return Box::pin(future::ok(layer.name()));
        }

        let layers = self.layers.clone();
        Box::pin(async move {
            {
                let layers_r = layers.read().await;
                if let Some((_, Some(rollup), _)) = layers_r.get(&layer.name()) {
                    // get rollup parent. if it is the same as upto, we're requesting something equivalent.
                    if let Some((Some(rollup_parent), _, _)) = layers_r.get(rollup) {
                        if *rollup_parent == upto {
                            // yup, equivalent. let's just return the rollup we know about.
                            return Ok(*rollup);
                        }
                    }
                }
            }

            let name = rand::random();
            let clf = child_layer_memory_files();

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
        if layer == rollup {
            // let's not create a loop
            return Box::pin(future::ok(()));
        }

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
    ) -> Pin<Box<dyn Future<Output = io::Result<OptInternalLayerTripleSubjectIterator>> + Send>>
    {
        let files_fut = self.triple_addition_files(layer);
        Box::pin(async move {
            let (subjects_file, s_p_aj_files, sp_o_aj_files) = files_fut.await?;

            Ok(OptInternalLayerTripleSubjectIterator(Some(
                file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files).await?,
            )))
        })
    }

    fn triple_removals(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<OptInternalLayerTripleSubjectIterator>> + Send>>
    {
        let files_fut = self.triple_removal_files(layer);
        Box::pin(async move {
            Ok(OptInternalLayerTripleSubjectIterator(
                match files_fut.await? {
                    Some((subjects_file, s_p_aj_files, sp_o_aj_files)) => Some(
                        file_triple_iterator(subjects_file, s_p_aj_files, sp_o_aj_files).await?,
                    ),
                    None => None,
                },
            ))
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

    fn retrieve_layer_stack_names(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            let layers = guard.await;
            let mut result = vec![name];
            let mut d = name;
            loop {
                match layers.get(&d) {
                    Some((Some(parent), _, _)) => {
                        d = *parent;
                        result.push(d)
                    }
                    _ => {
                        result.reverse();

                        return Ok(result);
                    }
                }
            }
        })
    }

    fn retrieve_layer_stack_names_upto(
        &self,
        name: [u32; 5],
        upto: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move {
            let layers = guard.await;
            let mut result = vec![name];
            let mut d = name;
            loop {
                match layers.get(&d) {
                    Some((Some(parent), _, _)) => {
                        if upto == *parent {
                            break;
                        }
                        d = *parent;
                        result.push(d)
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "parent layer not found",
                        ));
                    }
                }
            }

            result.reverse();

            Ok(result)
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
