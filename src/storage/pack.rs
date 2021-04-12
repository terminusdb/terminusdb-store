use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use super::cache::*;
use super::consts::*;
use super::file::*;
use super::layer::*;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tar::*;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait Packable {
    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    async fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>>;

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    async fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()>;
}

#[async_trait]
impl<T: PersistentLayerStore> Packable for T {
    async fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        let mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

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
    }

    async fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        let mut layer_id_set = HashSet::new();
        for id in layer_ids {
            layer_id_set.insert(name_to_string(id));
            self.create_named_directory(id).await?;
        }

        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| {
            let cursor = io::Cursor::new(pack);
            let tar = GzDecoder::new(cursor);
            let mut archive = Archive::new(tar);

            // TODO we actually need to validate that these layers, when extracted, will make for a valid store.
            // In terminus-server we are currently already doing this validation. Due to time constraints, we're not implementing it here.
            //
            // This should definitely be done in the future though, to make this part of the library independently usable in a safe manner.
            for e in archive.entries()? {
                let mut entry = e?;
                let path = entry.path()?;
                let os_file_name = path.file_name().unwrap();
                let file_name = os_file_name
                    .to_str()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unexpected non-utf8 directory name",
                        )
                    })?
                    .to_owned();

                // check if entry is prefixed with a layer id we are interested in
                let layer_id = path.iter().next().and_then(|p| p.to_str()).unwrap_or("");

                if layer_id_set.contains(layer_id) {
                    // this conversion should always work cause we are
                    // only able to match things that went through the
                    // conversion in the opposite direction.
                    let layer_id_arr = string_to_name(layer_id).unwrap();

                    let header = entry.header();
                    if !header.entry_type().is_file() {
                        continue;
                    }

                    let mut content = Vec::with_capacity(header.size()? as usize);
                    entry.read_to_end(&mut content)?;

                    handle.block_on(async move {
                        let file = self.get_file(layer_id_arr, &file_name).await?;
                        let mut writer = file.open_write();
                        writer.write_all(&content).await?;
                        writer.flush().await?;
                        writer.sync_all().await?;

                        Ok::<_, io::Error>(())
                    })?;
                }
            }

            Ok(())
        })
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
        tokio::task::block_in_place(|| tar.append_data(&mut header, path, cursor).unwrap());

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
        tokio::task::block_in_place(|| tar.append_data(&mut header, path, cursor).unwrap());
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
    header.set_size(0);
    let layer_name = name_to_string(layer);
    let mut path = PathBuf::new();
    path.push(layer_name);
    tokio::task::block_in_place(|| {
        tar.append_data(&mut header, &path, std::io::empty())
            .unwrap()
    });

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

#[derive(Debug)]
pub enum PackError {
    LayerNotFound,
    Io(io::Error),
    Utf8Error(std::str::Utf8Error),
}

pub fn pack_layer_parents<R: io::Read>(
    readable: R,
) -> Result<HashMap<[u32; 5], Option<[u32; 5]>>, PackError> {
    let tar = GzDecoder::new(readable);
    let mut archive = Archive::new(tar);

    // build a set out of the layer ids for easy retrieval
    let mut result_map = HashMap::new();

    println!("about to go into loop");
    for e in archive.entries()? {
        println!("a");
        let mut entry = e?;
        println!("b");
        let path = entry.path()?;
        println!("c");
        println!("path: {:?}", path);

        let id = string_to_name(
            path.iter()
                .next()
                .expect("expected path to have at least one component")
                .to_str()
                .expect("expected proper unicode path"),
        )?;

        if path.file_name().expect("expected path to have a filename") == "parent.hex" {
            println!("hello a");
            // this is an element we want to know the parent of
            // lets read it
            let mut parent_id_bytes = [0u8; 40];
            entry.read_exact(&mut parent_id_bytes)?;
            let parent_id_str = std::str::from_utf8(&parent_id_bytes)?;
            let parent_id = string_to_name(parent_id_str)?;

            result_map.insert(id, Some(parent_id));
        } else if !result_map.contains_key(&id) {
            println!("hello b");
            // Ensure that an entry for this layer exists
            // If we encounter the parent file later on, this'll be overwritten with the parent id.
            // If not, it can be assumed to not have a parent.
            result_map.insert(id, None);
        }
    }

    Ok(result_map)
}

#[async_trait]
impl Packable for CachedLayerStore {
    async fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        self.inner.export_layers(layer_ids).await
    }

    async fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        self.inner.import_layers(pack, layer_ids).await
    }
}
