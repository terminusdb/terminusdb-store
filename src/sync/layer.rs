use crate::layer::GenericLayer;
use crate::storage::{FileLoad, FileStore,LayerStore};
use tokio::runtime::TaskExecutor;
use std::io;
use std::sync::Arc;
use futures::sync::oneshot;
use futures::prelude::*;

use crate::sync::builder::SyncLayerBuilder;

pub struct SyncLayerStore<F:FileLoad+FileStore+Clone,L:LayerStore<Map=F::Map,File=F>> {
    inner: L,
    executor: TaskExecutor
}

impl<F:FileLoad+FileStore+Clone,L:LayerStore<Map=F::Map,File=F>> SyncLayerStore<F,L> {
    pub fn wrap(layer_store: L, executor: TaskExecutor) -> Self {
        SyncLayerStore {
            inner: layer_store,
            executor
        }
    }

    pub fn layers(&self) -> Result<Vec<[u32;5]>, io::Error> {
        oneshot::spawn(self.inner.layers(), &self.executor).wait()
    }

    pub fn get_layer(&self, name: [u32;5]) -> Result<Option<Arc<GenericLayer<F::Map>>>, io::Error> {
        oneshot::spawn(self.inner.get_layer(name), &self.executor).wait()
    }

    pub fn create_base_layer(&self) -> Result<SyncLayerBuilder<F>, io::Error> {
        let builder = oneshot::spawn(self.inner.create_base_layer(), &self.executor).wait()?;

        Ok(SyncLayerBuilder::wrap(builder, self.executor.clone()))
    }

    pub fn create_child_layer(&self, parent: [u32;5]) -> Result<SyncLayerBuilder<F>, io::Error> {
        let builder = oneshot::spawn(self.inner.create_child_layer(parent), &self.executor).wait()?;

        Ok(SyncLayerBuilder::wrap(builder, self.executor.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use tempfile::tempdir;

    use crate::layer::{Layer,StringTriple};
    use crate::storage::{DirectoryLayerStore,CachedLayerStore};

    #[test]
    fn create_layers_from_sync_cached_directory_store() {
        let runtime = Runtime::new().unwrap();
        let dir = tempdir().unwrap();
        let store = SyncLayerStore::wrap(CachedLayerStore::new(DirectoryLayerStore::new(dir.path())),
                                         runtime.executor().clone());
        let mut builder = store.create_base_layer().unwrap();

        let base_name = builder.name();

        builder.add_string_triple(&StringTriple::new_value("cow","says","moo"));
        builder.add_string_triple(&StringTriple::new_value("pig","says","oink"));
        builder.add_string_triple(&StringTriple::new_value("duck","says","quack"));

        builder.finalize().unwrap();

        builder = store.create_child_layer(base_name).unwrap();

        let child_name = builder.name();

        builder.remove_string_triple(&StringTriple::new_value("duck","says","quack"));
        builder.add_string_triple(&StringTriple::new_node("cow","likes","pig"));

        builder.finalize().unwrap();

        let layer = store.get_layer(child_name).unwrap().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_value("cow", "says", "moo")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("pig", "says", "oink")));
        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.string_triple_exists(&StringTriple::new_value("duck", "says", "quack")));
    }
}
