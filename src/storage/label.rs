use futures::future::Future;
use std::io;
use std::pin::Pin;

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

pub trait LabelStore: Send + Sync {
    fn labels(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<Label>>> + Send>>;
    fn create_label(&self, name: &str) -> Pin<Box<dyn Future<Output = io::Result<Label>> + Send>>;
    fn get_label(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>>;
    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>>;

    fn set_label(
        &self,
        label: &Label,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>> {
        self.set_label_option(label, Some(layer))
    }

    fn clear_label(
        &self,
        label: &Label,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Label>>> + Send>> {
        self.set_label_option(label, None)
    }
}
