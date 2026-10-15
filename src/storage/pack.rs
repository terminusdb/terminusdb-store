use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::io::{self, Read};
use std::path::{Component, PathBuf};
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
    /// Import the specified layers from the given pack.
    ///
    /// Layers that do not yet exist in this store are created. Layers
    /// that already exist are not overwritten; instead their files are
    /// verified to be identical to the pack contents, as layer ids are
    /// random names and not content hashes. Files that only exist in
    /// the store (such as rollups written after export) are ignored.
    ///
    /// Returns the list of layer ids that already existed with
    /// differing contents.
    ///
    /// Pack entries with file names that are unknown to this binary are
    /// skipped with a warning.
    async fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<[u32; 5]>>;
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
    ) -> io::Result<Vec<[u32; 5]>> {
        let mut layer_id_set = HashSet::new();
        let mut existing_id_set = HashSet::new();
        for id in layer_ids {
            let layer_id = name_to_string(id);
            layer_id_set.insert(layer_id.clone());
            // A layer that already exists is verified file by file
            // against the pack contents instead of being overwritten.
            if self.directory_exists(id).await? {
                existing_id_set.insert(layer_id);
            }
        }

        let mut mismatched = HashSet::new();
        let mut created_id_set = HashSet::new();
        let mut skipped_files = HashSet::new();
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

                // check if entry is prefixed with a layer id we are interested in
                let layer_id = path
                    .iter()
                    .next()
                    .and_then(|p| p.to_str())
                    .unwrap_or("")
                    .to_string();

                if !layer_id_set.contains(&layer_id) {
                    continue;
                }

                // this conversion should always work cause we are
                // only able to match things that went through the
                // conversion in the opposite direction.
                let layer_id_arr = string_to_name(&layer_id).unwrap();

                let header = entry.header();
                if !header.entry_type().is_file() {
                    continue;
                }

                // a layer only contains flat <layer>/<file> entries;
                // anything else is malformed and must not be reduced to
                // its last component
                let mut components = path.components();
                if !matches!(components.next(), Some(Component::Normal(_)))
                    || !matches!(components.next(), Some(Component::Normal(_)))
                    || components.next().is_some()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("malformed pack entry path: {}", path.display()),
                    ));
                }

                let Some(os_file_name) = path.file_name() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("pack entry has no file name: {}", path.display()),
                    ));
                };
                let file_name = os_file_name
                    .to_str()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "unexpected non-utf8 file name")
                    })?
                    .to_owned();

                // a file this binary does not know was written by a newer
                // version and is optional by definition, so we skip it
                if !FILENAME_ENUM_MAP.contains_key(file_name.as_str()) {
                    skipped_files.insert(format!("{layer_id}/{file_name}"));
                    continue;
                }

                let mut content = Vec::new();
                entry.read_to_end(&mut content)?;

                if existing_id_set.contains(&layer_id) {
                    // verify this file against the stored layer
                    let identical = handle.block_on(async {
                        if !self.file_exists(layer_id_arr, &file_name).await? {
                            return Ok::<_, io::Error>(false);
                        }
                        let file = self.get_file(layer_id_arr, &file_name).await?;
                        let existing = file.map().await?;
                        Ok(&*existing == content.as_slice())
                    })?;

                    if !identical {
                        mismatched.insert(layer_id_arr);
                    }
                } else {
                    // create the layer only when it actually contributes a
                    // file; absent or fully-skipped layers leave nothing
                    // behind
                    if !created_id_set.contains(&layer_id_arr) {
                        handle.block_on(self.create_named_directory(layer_id_arr))?;
                        created_id_set.insert(layer_id_arr);
                    }
                    handle.block_on(async move {
                        let file = self.get_file(layer_id_arr, &file_name).await?;
                        let mut writer = file.open_write().await?;
                        writer.write_all(&content).await?;
                        writer.flush().await?;
                        writer.sync_all().await?;

                        Ok::<_, io::Error>(())
                    })?;
                }
            }

            // a skipped file means the pack was produced by a newer
            // binary, an event the operator needs to see
            for skipped_file in &skipped_files {
                eprintln!("warning: skipping unknown layer file in pack: {skipped_file}");
            }

            // only layers that were actually created get finalized
            for layer_id_arr in &created_id_set {
                handle.block_on(self.finalize_layer(*layer_id_arr))?;
            }

            Ok::<_, io::Error>(())
        })?;

        let mut result: Vec<[u32; 5]> = mismatched.into_iter().collect();
        result.sort_unstable();
        Ok(result)
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
        header.set_size(file.size().await? as u64);
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
        header.set_size(file.size().await? as u64);
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

impl Display for PackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{:?}", self)
    }
}

impl From<io::Error> for PackError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
impl From<std::str::Utf8Error> for PackError {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Utf8Error(err)
    }
}

pub fn pack_layer_parents<R: io::Read>(
    readable: R,
) -> Result<HashMap<[u32; 5], Option<[u32; 5]>>, PackError> {
    let tar = GzDecoder::new(readable);
    let mut archive = Archive::new(tar);

    // build a set out of the layer ids for easy retrieval
    let mut result_map = HashMap::new();

    for e in archive.entries()? {
        let mut entry = e?;
        let path = entry.path()?;

        let id = string_to_name(
            path.iter()
                .next()
                .expect("expected path to have at least one component")
                .to_str()
                .expect("expected proper unicode path"),
        )?;

        if path.file_name().expect("expected path to have a filename") == "parent.hex" {
            // this is an element we want to know the parent of
            // lets read it
            let mut parent_id_bytes = [0u8; 40];
            entry.read_exact(&mut parent_id_bytes)?;
            let parent_id_str = std::str::from_utf8(&parent_id_bytes)?;
            let parent_id = string_to_name(parent_id_str)?;

            result_map.insert(id, Some(parent_id));
        } else {
            // Ensure that an entry for this layer exists
            // If we encounter the parent file later on, this'll be overwritten with the parent id.
            // If not, it can be assumed to not have a parent.
            result_map.entry(id).or_insert(None);
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
    ) -> io::Result<Vec<[u32; 5]>> {
        self.inner.import_layers(pack, layer_ids).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::archive::{ArchiveLayerStore, DirectoryArchiveBackend};
    use crate::storage::directory::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test(flavor = "multi_thread")]
    async fn export_import_layer_with_rollup() {
        let dir1 = tempdir().unwrap();
        let store1 = Arc::new(DirectoryLayerStore::new(dir1.path()));
        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));

        let mut builder = store1.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.add_value_triple(ValueTriple::new_node("duck", "hates", "cow"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store1.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_node("duck", "hates", "cow"));
        builder.add_value_triple(ValueTriple::new_node("duck", "likes", "cow"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store1.get_layer(child_name).await.unwrap().unwrap();

        store1.clone().rollup(unrolled_layer).await.unwrap();

        let export = store1
            .export_layers(Box::new(vec![base_name, child_name].into_iter()))
            .await
            .unwrap();

        store2
            .import_layers(&export, Box::new(vec![base_name, child_name].into_iter()))
            .await
            .unwrap();

        let imported_layer = store2.get_layer(child_name).await.unwrap().unwrap();
        let triples: Vec<_> = imported_layer
            .triples()
            .map(|t| imported_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("duck", "likes", "cow")
            ],
            triples
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_verifies_identical() {
        let dir1 = tempdir().unwrap();
        let store1 = Arc::new(DirectoryLayerStore::new(dir1.path()));
        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));

        let mut builder = store1.create_base_layer().await.unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.add_value_triple(ValueTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().await.unwrap();

        let export = store1
            .export_layers(Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();

        // an unknown layer is imported without mismatches
        assert_eq!(
            Vec::<[u32; 5]>::new(),
            store2
                .import_layers(&export, Box::new(vec![base_name].into_iter()))
                .await
                .unwrap()
        );

        // an identical layer verifies cleanly and is reused
        assert_eq!(
            Vec::<[u32; 5]>::new(),
            store2
                .import_layers(&export, Box::new(vec![base_name].into_iter()))
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_detects_mismatch() {
        let dir1 = tempdir().unwrap();
        let store1 = Arc::new(DirectoryLayerStore::new(dir1.path()));
        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));

        let mut builder = store1.create_base_layer().await.unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.commit_boxed().await.unwrap();

        let export = store1
            .export_layers(Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();

        // an empty layer with the same name has different contents
        store2.create_base_layer_with_name(base_name).await.unwrap();

        assert_eq!(
            vec![base_name],
            store2
                .import_layers(&export, Box::new(vec![base_name].into_iter()))
                .await
                .unwrap()
        );

        // the existing layer was not overwritten
        let layer = store2.get_layer(base_name).await.unwrap().unwrap();
        assert_eq!(0, layer.node_and_value_count());
    }

    async fn export_base_layer_pack() -> (tempfile::TempDir, [u32; 5], Vec<u8>) {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.add_value_triple(ValueTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().await.unwrap();

        let export = store
            .export_layers(Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();

        (dir, base_name, export)
    }

    /// Returns a copy of the given pack, with `add_entry` invoked on the
    /// builder after all original entries have been copied over.
    fn repack_with_extra(
        pack: &[u8],
        add_entry: impl FnOnce(&mut tar::Builder<&mut GzEncoder<Vec<u8>>>),
    ) -> Vec<u8> {
        let mut decompressed = Vec::new();
        GzDecoder::new(pack).read_to_end(&mut decompressed).unwrap();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut builder = tar::Builder::new(&mut encoder);
            let mut source = tar::Archive::new(io::Cursor::new(&decompressed));
            for entry in source.entries().unwrap() {
                let mut entry = entry.unwrap();
                let header = entry.header().clone();
                builder.append(&header, &mut entry).unwrap();
            }
            add_entry(&mut builder);
            builder.finish().unwrap();
        }
        encoder.finish().unwrap()
    }

    /// Appends a regular file entry to the pack. `tweak_header` runs after
    /// the standard fields are set, so tests can deliberately lie about
    /// e.g. the entry size.
    fn inject_pack_entry(
        pack: &[u8],
        path: &str,
        contents: &[u8],
        tweak_header: impl FnOnce(&mut Header),
    ) -> Vec<u8> {
        repack_with_extra(pack, |builder| {
            let mut header = Header::new_gnu();
            header.set_mode(0o644);
            header.set_entry_type(EntryType::Regular);
            header.set_size(contents.len() as u64);
            tweak_header(&mut header);
            builder
                .append_data(&mut header, path, io::Cursor::new(contents))
                .unwrap();
        })
    }

    /// Appends a regular file entry whose path is written verbatim into the
    /// header name field. `append_data` refuses `..` components, so a
    /// hostile path needs the bytes to be set directly.
    fn inject_raw_path_entry(pack: &[u8], path: &[u8], contents: &[u8]) -> Vec<u8> {
        repack_with_extra(pack, |builder| {
            let mut header = Header::new_gnu();
            header.set_mode(0o644);
            header.set_entry_type(EntryType::Regular);
            header.set_size(contents.len() as u64);
            header.as_gnu_mut().unwrap().name[..path.len()].copy_from_slice(path);
            header.set_cksum();
            builder.append(&header, io::Cursor::new(contents)).unwrap();
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_skips_unknown_files() {
        let (_dir, base_name, export) = export_base_layer_pack().await;
        let layer_name = name_to_string(base_name);
        let injected = inject_pack_entry(
            &export,
            &format!("{layer_name}/zzz_future_file.bin"),
            b"future data",
            |_| {},
        );

        // directory backend
        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));
        let mismatched = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();
        assert!(mismatched.is_empty());
        assert!(store2.get_layer(base_name).await.unwrap().is_some());
        assert!(!store2
            .file_exists(base_name, "zzz_future_file.bin")
            .await
            .unwrap());

        // archive backend
        let archive_dir = tempdir().unwrap();
        let backend = DirectoryArchiveBackend::new(archive_dir.path().to_path_buf());
        let store3 = ArchiveLayerStore::new(backend.clone(), backend);
        let mismatched = store3
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();
        assert!(mismatched.is_empty());
        assert!(store3.get_layer(base_name).await.unwrap().is_some());
        assert!(!store3
            .file_exists(base_name, "zzz_future_file.bin")
            .await
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_unknown_file_does_not_flag_mismatch() {
        let (_dir, base_name, export) = export_base_layer_pack().await;
        let layer_name = name_to_string(base_name);
        let injected = inject_pack_entry(
            &export,
            &format!("{layer_name}/zzz_future_file.bin"),
            b"future data",
            |_| {},
        );

        let archive_dir = tempdir().unwrap();
        let backend = DirectoryArchiveBackend::new(archive_dir.path().to_path_buf());
        let store2 = ArchiveLayerStore::new(backend.clone(), backend);

        // first import creates the layer, skipping the unknown file
        let mismatched = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();
        assert!(mismatched.is_empty());

        // second import verifies the existing layer; the unknown file
        // must not participate in verification
        let mismatched = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await
            .unwrap();
        assert!(mismatched.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_rejects_nested_path() {
        let (_dir, base_name, export) = export_base_layer_pack().await;
        let layer_name = name_to_string(base_name);
        let injected = inject_pack_entry(
            &export,
            &format!("{layer_name}/sub/parent.hex"),
            b"evil",
            |_| {},
        );

        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));
        let err = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await
            .unwrap_err();
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_rejects_traversal() {
        let (_dir, base_name, export) = export_base_layer_pack().await;
        let layer_name = name_to_string(base_name);
        let injected = inject_raw_path_entry(
            &export,
            format!("{layer_name}/../parent.hex").as_bytes(),
            b"evil",
        );

        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));
        let err = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await
            .unwrap_err();
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_no_panic_on_nameless_entry() {
        let (_dir, base_name, export) = export_base_layer_pack().await;
        let layer_name = name_to_string(base_name);
        let injected =
            inject_raw_path_entry(&export, format!("{layer_name}/..").as_bytes(), b"evil");

        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));
        let result = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_ignores_absent_requested_layers() {
        let (_dir, _base_name, export) = export_base_layer_pack().await;
        let absent_name = [9, 8, 7, 6, 5];

        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));
        let mismatched = store2
            .import_layers(&export, Box::new(vec![absent_name].into_iter()))
            .await
            .unwrap();
        assert!(mismatched.is_empty());
        assert!(!store2.directory_exists(absent_name).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn import_layers_huge_declared_size_does_not_abort() {
        let (_dir, base_name, export) = export_base_layer_pack().await;
        let layer_name = name_to_string(base_name);
        // a declared size beyond what memory can hold must not cause an
        // allocation based on the attacker-controlled header value.
        // anything larger overflows u64 during tar's own entry scanning.
        let injected = inject_pack_entry(
            &export,
            &format!("{layer_name}/parent.hex"),
            b"evil",
            |header| header.set_size(isize::MAX as u64 + 1),
        );

        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));
        let result = store2
            .import_layers(&injected, Box::new(vec![base_name].into_iter()))
            .await;
        // no allocation is attempted from the declared size; the entry is
        // then flagged as truncated by tar's own consistency check
        assert!(result.is_err());
    }
}
