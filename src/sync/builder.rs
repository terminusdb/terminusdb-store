use tokio::runtime::TaskExecutor;

use futures::sync::oneshot;
use futures::prelude::*;
use std::sync::Arc;

use crate::storage::{FileLoad,FileStore};
use crate::layer::{SimpleLayerBuilder,GenericLayer,IdTriple,StringTriple,BaseLayerFiles,ChildLayerFiles};

#[derive(Clone)]
pub struct SyncLayerBuilder<F:'static+FileLoad+FileStore+Clone> {
    inner: SimpleLayerBuilder<F>,
    executor: TaskExecutor
}

impl<F:'static+FileLoad+FileStore+Clone> SyncLayerBuilder<F> {
    pub fn new(name: [u32;5], files: BaseLayerFiles<F>, executor: TaskExecutor) -> Self {
        let inner = SimpleLayerBuilder::new(name, files);

        Self::wrap(inner, executor)
    }

    pub fn from_parent(name: [u32;5], parent: Arc<GenericLayer<F::Map>>, files: ChildLayerFiles<F>, executor: TaskExecutor) -> Self {
        let inner = SimpleLayerBuilder::from_parent(name, parent, files);

        Self::wrap(inner, executor)
    }

    pub fn wrap(builder: SimpleLayerBuilder<F>, executor: TaskExecutor) -> Self {
        SyncLayerBuilder {
            inner: builder,
            executor
        }
    }

    pub fn name(&self) -> [u32;5] {
        self.inner.name()
    }

    pub fn add_string_triple(&mut self, triple: &StringTriple) {
        self.inner.add_string_triple(triple)
    }

    pub fn add_id_triple(&mut self, triple: IdTriple) -> bool {
        self.inner.add_id_triple(triple)
    }


    pub fn remove_id_triple(&mut self, triple: IdTriple) -> bool {
        self.inner.remove_id_triple(triple)
    }

    
    pub fn remove_string_triple(&mut self, triple: &StringTriple) -> bool {
        self.inner.remove_string_triple(triple)
    }

    pub fn commit(self) ->Result<GenericLayer<F::Map>, std::io::Error> {
        oneshot::spawn(self.inner.commit(), &self.executor).wait()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use crate::storage::MemoryBackedStore;
    use crate::layer::Layer;
    
    fn new_base_files() -> BaseLayerFiles<MemoryBackedStore> {
        let files: Vec<_> = (0..14).map(|_| MemoryBackedStore::new()).collect();
        BaseLayerFiles {
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
            sp_o_adjacency_list_nums_file: files[13].clone()
        }
    }

    #[test]
    fn sync_layer_building() {
        let runtime = Runtime::new().unwrap();
        let files = new_base_files();
        let mut builder = SyncLayerBuilder::wrap(SimpleLayerBuilder::new([1,2,3,4,5], files), runtime.executor().clone());

        builder.add_string_triple(&StringTriple::new_node("cow", "says", "cow_sound"));
        builder.add_string_triple(&StringTriple::new_value("cow_sound", "sounds_like", "moo"));

        let layer = builder.commit().unwrap();

        assert!(layer.string_triple_exists(&StringTriple::new_node("cow", "says", "cow_sound")));
        assert!(layer.string_triple_exists(&StringTriple::new_value("cow_sound", "sounds_like", "moo")));

    }
}
