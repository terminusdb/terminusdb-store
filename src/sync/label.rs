use crate::storage::{Label,LabelStore};
use tokio::runtime::TaskExecutor;
use futures::prelude::*;
use futures::sync::oneshot;
use std::io;

pub struct SyncLabelStore<L:LabelStore> {
    inner: L,
    executor: TaskExecutor
}

impl<L:LabelStore> SyncLabelStore<L> {
    pub fn wrap(label_store: L, executor: TaskExecutor) -> Self {
        SyncLabelStore {
            inner: label_store,
            executor
        }
    }

    pub fn labels(&self) -> Result<Vec<Label>,std::io::Error> {
        oneshot::spawn(self.inner.labels(), &self.executor).wait()
    }

    pub fn create_label(&self, name: &str) -> Result<Label, io::Error> {
        oneshot::spawn(self.inner.create_label(name), &self.executor).wait()
    }

    pub fn get_label(&self, name: &str) -> Result<Option<Label>, io::Error> {
        oneshot::spawn(self.inner.get_label(name), &self.executor).wait()
    }

    pub fn set_label(&self, label: &Label, layer: [u32;5]) -> Result<Option<Label>, io::Error> {
        oneshot::spawn(self.inner.set_label(label, layer), &self.executor).wait()
    }

    pub fn clear_label(&self, label: &Label) -> Result<Option<Label>, io::Error> {
        oneshot::spawn(self.inner.clear_label(label), &self.executor).wait()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DirectoryLabelStore;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    #[test]
    fn sync_directory_label_manipulation_succeeds() {
        let runtime = Runtime::new().unwrap();
        let dir = tempdir().unwrap();

        let store = SyncLabelStore::wrap(DirectoryLabelStore::new(dir.path()), runtime.executor().clone());

        let label = store.create_label("foo").unwrap();
        assert_eq!(label, store.get_label("foo").unwrap().unwrap());
        assert_eq!("foo", label.name);
        assert_eq!(None, label.layer);

        let label2 = store.set_label(&label, [1,2,3,4,5]).unwrap().unwrap();
        assert_eq!(label2, store.get_label("foo").unwrap().unwrap());
        assert_eq!([1,2,3,4,5], label2.layer.unwrap());

        assert!(store.set_label(&label, [6,7,8,9,0]).unwrap().is_none());
        assert_eq!(label2, store.get_label("foo").unwrap().unwrap());

        let label3 = store.clear_label(&label2).unwrap().unwrap();
        assert_eq!(label3, store.get_label("foo").unwrap().unwrap());
        assert_eq!(2, label3.version);
        assert!(label3.layer.is_none());
    }
}
