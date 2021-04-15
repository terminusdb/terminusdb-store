//! In-memory implementation of storage traits.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use futures::task::{Context, Poll};
use futures::{future, Future};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use async_trait::async_trait;

use super::file::*;
use super::label::*;
use super::layer::*;

use bytes::{Bytes, BytesMut};
enum MemoryBackedStoreContents {
    Nonexistent,
    Existent(Bytes),
}

#[derive(Clone)]
pub struct MemoryBackedStore {
    contents: Arc<RwLock<MemoryBackedStoreContents>>,
}

impl MemoryBackedStore {
    pub fn new() -> Self {
        Self {
            contents: Arc::new(RwLock::new(MemoryBackedStoreContents::Nonexistent)),
        }
    }
}

pub struct MemoryBackedStoreWriter {
    file: MemoryBackedStore,
    bytes: BytesMut,
}

impl SyncableFile for MemoryBackedStoreWriter {
    fn sync_all(self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        let mut contents = self.file.contents.write().unwrap();
        *contents = MemoryBackedStoreContents::Existent(self.bytes.freeze());

        Box::pin(future::ok(()))
    }
}

impl std::io::Write for MemoryBackedStoreWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.bytes.extend_from_slice(buf);

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

impl FileStore for MemoryBackedStore {
    type Write = MemoryBackedStoreWriter;

    fn open_write(&self) -> Self::Write {
        MemoryBackedStoreWriter {
            file: self.clone(),
            bytes: BytesMut::new(),
        }
    }
}

pub struct MemoryBackedStoreReader {
    bytes: Bytes,
    pos: usize,
}

impl std::io::Read for MemoryBackedStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        if self.bytes.len() == self.pos {
            // end of file
            Ok(0)
        } else if self.bytes.len() < self.pos + buf.len() {
            // read up to end
            let len = self.bytes.len() - self.pos;
            buf[..len].copy_from_slice(&self.bytes[self.pos..]);

            self.pos += len;

            Ok(len)
        } else {
            // read full buf
            buf.copy_from_slice(&self.bytes[self.pos..self.pos + buf.len()]);

            self.pos += buf.len();

            Ok(buf.len())
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

impl FileLoad for MemoryBackedStore {
    type Read = MemoryBackedStoreReader;

    fn exists(&self) -> bool {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => false,
            _ => true,
        }
    }

    fn size(&self) -> usize {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to retrieve size of nonexistent memory file")
            }
            MemoryBackedStoreContents::Existent(bytes) => bytes.len(),
        }
    }

    fn open_read_from(&self, offset: usize) -> MemoryBackedStoreReader {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to open nonexistent memory file for reading")
            }
            MemoryBackedStoreContents::Existent(bytes) => MemoryBackedStoreReader {
                bytes: bytes.clone(),
                pos: offset,
            },
        }
    }

    fn map(&self) -> Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => Box::pin(future::err(io::Error::new(
                io::ErrorKind::NotFound,
                "tried to open a nonexistent memory file for reading",
            ))),
            MemoryBackedStoreContents::Existent(bytes) => Box::pin(future::ok(bytes.clone())),
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryLayerStore {
    layers: futures_locks::RwLock<HashMap<[u32; 5], HashMap<String, MemoryBackedStore>>>,
}

impl MemoryLayerStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PersistentLayerStore for MemoryLayerStore {
    type File = MemoryBackedStore;

    fn directories(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move { Ok(guard.await.keys().cloned().collect()) })
    }

    fn create_named_directory(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let guard = self.layers.write();
        Box::pin(async move {
            guard.await.insert(name, HashMap::new());

            Ok(name)
        })
    }

    fn directory_exists(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let guard = self.layers.read();
        Box::pin(async move { Ok(guard.await.contains_key(&name)) })
    }

    fn file_exists(
        &self,
        directory: [u32; 5],
        file: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        let guard = self.layers.read();
        let file = file.to_owned();
        Box::pin(async move {
            if let Some(files) = guard.await.get(&directory) {
                if let Some(file) = files.get(&file) {
                    Ok(file.exists())
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        })
    }

    fn get_file(
        &self,
        directory: [u32; 5],
        name: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::File>> + Send>> {
        let layers = self.layers.clone();
        let name = name.to_owned();
        Box::pin(async move {
            let guard = layers.read().await;
            if let Some(files) = guard.get(&directory) {
                if let Some(file) = files.get(&name) {
                    Ok(file.clone())
                } else {
                    std::mem::drop(guard); // release read lock cause it is time to write
                    let mut guard = layers.write().await;
                    let files = guard.get_mut(&directory).unwrap();
                    let file = MemoryBackedStore::new();
                    let result = file.clone();
                    files.insert(name, file);
                    Ok(result)
                }
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
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

#[async_trait]
impl LabelStore for MemoryLabelStore {
    async fn labels(&self) -> io::Result<Vec<Label>> {
        let labels = self.labels.read().await;
        Ok(labels.values().cloned().collect())
    }

    async fn create_label(&self, name: &str) -> io::Result<Label> {
        let label = Label::new_empty(name);

        let mut labels = self.labels.write().await;
        if labels.get(&label.name).is_some() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "label already exists",
            ))
        } else {
            labels.insert(label.name.clone(), label.clone());
            Ok(label)
        }
    }

    async fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        let name = name.to_owned();
        let labels = self.labels.read().await;
        Ok(labels.get(&name).cloned())
    }

    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);

        let mut labels = self.labels.write().await;

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
    }
}

#[cfg(test)]
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

#[cfg(test)]
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
            w.sync_all().await?;
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
            w.sync_all().await?;
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
