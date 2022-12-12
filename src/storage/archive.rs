// File format:
// <header>
//  [<filetype present>]*
//  [<offsets>]*
//

use std::io;

use async_trait::async_trait;
use bytes::{Buf, Bytes};

#[async_trait]
pub trait ArchiveLayerStore {
    async fn get_layer_names(&self) -> Vec<[u32; 5]>;
    async fn get_layer_file(&self, id: [u32; 5]) -> io::Result<Bytes>;
    async fn store_layer_file<B: Buf>(&self, id: [u32; 5], bytes: B) -> io::Result<()>;

    async fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
    async fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()>;
}
