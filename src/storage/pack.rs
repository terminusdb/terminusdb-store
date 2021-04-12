use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::Future;

use super::consts::*;
use super::file::*;
use super::layer::*;

use tar::*;
//use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

pub trait Packable {
    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    fn export_layers<'a>(
        &'a self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send + 'a>>;

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    fn import_layers<'a>(
        &'a self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

impl<T: PersistentLayerStore> Packable for T {
    fn export_layers<'a>(
        &'a self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send + 'a>> {
        let mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Box::pin(async move {
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            {
                let mut tar = tar::Builder::new(&mut enc);
                for id in layer_ids {
                    tar_append_layer(&mut tar, self, id, mtime).await?;
                }
                tar.finish().unwrap();
            }
            // TODO: Proper error handling
            Ok(enc.finish().unwrap())
        })
    }

    fn import_layers<'a>(
        &'a self,
        _pack: &[u8],
        _layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        todo!();
    }
}

async fn tar_append_file<S: PersistentLayerStore, W: io::Write>(
    store: &S,
    tar: &mut tar::Builder<W>,
    layer: [u32; 5],
    layer_path: &PathBuf,
    file_name: &str,
    mtime: u64,
) -> io::Result<()> {
    if store.file_exists(layer, file_name).await? {
        let file = store.get_file(layer, file_name).await?;
        let contents = file.map().await?;
        let cursor = io::Cursor::new(&contents);

        let path = layer_path.join(file_name);

        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(file.size() as u64);
        header.set_mtime(mtime);
        tar.append_data(&mut header, path, cursor).unwrap();

        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "file does not exist",
        ))
    }
}

async fn tar_append_file_if_exists<S: PersistentLayerStore, W: io::Write>(
    store: &S,
    tar: &mut tar::Builder<W>,
    layer: [u32; 5],
    layer_path: &PathBuf,
    file_name: &str,
    mtime: u64,
) -> io::Result<()> {
    if store.file_exists(layer, file_name).await? {
        let file = store.get_file(layer, file_name).await?;
        let contents = file.map().await?;
        let cursor = io::Cursor::new(&contents);

        let path = layer_path.join(file_name);

        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(file.size() as u64);
        header.set_mtime(mtime);
        tar.append_data(&mut header, path, cursor).unwrap();
    }

    Ok(())
}

async fn tar_append_layer<W: io::Write, S: PersistentLayerStore>(
    tar: &mut tar::Builder<W>,
    store: &S,
    layer: [u32; 5],
    mtime: u64,
) -> io::Result<()> {
    let mut header = Header::new_gnu();
    header.set_mode(0o755);
    header.set_entry_type(EntryType::Directory);
    header.set_mtime(mtime);
    let layer_name = name_to_string(layer);
    let mut path = PathBuf::new();
    path.push(layer_name);
    tar.append_data(&mut header, &path, std::io::empty())
        .unwrap();

    for f in &SHARED_REQUIRED_FILES {
        tar_append_file(store, tar, layer, &path, f, mtime).await?;
    }
    for f in &SHARED_OPTIONAL_FILES {
        if f == &FILENAMES.rollup {
            // skip the rollup file. It will not be resolvable remotely.
            continue;
        }
        tar_append_file_if_exists(store, tar, layer, &path, f, mtime).await?;
    }
    if store.file_exists(layer, FILENAMES.parent).await? {
        // this is a child layer
        for f in &CHILD_LAYER_REQUIRED_FILES {
            tar_append_file(store, tar, layer, &path, f, mtime).await?;
        }
        for f in &CHILD_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(store, tar, layer, &path, f, mtime).await?;
        }
    } else {
        // this is a base layer
        for f in &BASE_LAYER_REQUIRED_FILES {
            tar_append_file(store, tar, layer, &path, f, mtime).await?;
        }
        for f in &BASE_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(store, tar, layer, &path, f, mtime).await?;
        }
    }

    Ok(())
}
