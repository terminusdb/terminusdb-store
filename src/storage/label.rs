use std::io;

use async_trait::async_trait;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Label {
    pub name: String,
    pub layer: Option<[u32; 5]>,
    pub version: u64,
}

impl Label {
    pub fn new_empty(name: &str) -> Label {
        Label {
            name: name.to_owned(),
            layer: None,
            version: 0,
        }
    }
    pub fn new(name: &str, layer: [u32; 5]) -> Label {
        Label {
            name: name.to_owned(),
            layer: Some(layer),
            version: 0,
        }
    }

    pub fn with_updated_layer(&self, layer: Option<[u32; 5]>) -> Label {
        Label {
            name: self.name.clone(),
            layer,
            version: self.version + 1,
        }
    }
}

#[async_trait]
pub trait LabelStore: Send + Sync {
    async fn labels(&self) -> io::Result<Vec<Label>>;
    async fn create_label(&self, name: &str) -> io::Result<Label>;
    async fn get_label(&self, name: &str) -> io::Result<Option<Label>>;
    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>>;
    async fn delete_label(
        &self,
        name: &str
    ) -> io::Result<bool>;

    async fn set_label(&self, label: &Label, layer: [u32; 5]) -> io::Result<Option<Label>> {
        self.set_label_option(label, Some(layer)).await
    }

    async fn clear_label(&self, label: &Label) -> io::Result<Option<Label>> {
        self.set_label_option(label, None).await
    }
}
