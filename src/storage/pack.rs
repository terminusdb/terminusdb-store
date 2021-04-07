use std::io;
use std::path::PathBuf;

use super::layer::*;
use super::consts::*;

pub trait Packable {
    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8>;

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error>;
}



fn tar_append_file<W: io::Write>(
    tar: &mut tar::Builder<W>,
    destination: &PathBuf,
    origin: &PathBuf,
    file: &str,
) -> io::Result<()> {
    let file_path = origin.join(file);
    tar.append_path_with_name(file_path, destination.join(file))
}

fn tar_append_file_if_exists<W: io::Write>(
    tar: &mut tar::Builder<W>,
    destination: &PathBuf,
    origin: &PathBuf,
    file: &str,
) -> io::Result<()> {
    let file_path = origin.join(file);
    if file_path.exists() {
        tar.append_path_with_name(file_path, destination.join(file))
    } else {
        Ok(())
    }
}

fn tar_append_layer<W: io::Write>(
    tar: &mut tar::Builder<W>,
    tar_path: &PathBuf,
    layer_path: &PathBuf,
) -> io::Result<()> {
    // this appends known layer files, excluding the rollup file.
    tar.append_dir(tar_path, layer_path)?;

    for f in &SHARED_REQUIRED_FILES {
        tar_append_file(tar, tar_path, layer_path, f)?;
    }
    for f in &SHARED_OPTIONAL_FILES {
        if f == &FILENAMES.rollup {
            // skip the rollup file. It will not be resolvable remotely.
            continue;
        }
        tar_append_file_if_exists(tar, tar_path, layer_path, f)?;
    }
    if layer_path.join(FILENAMES.parent).exists() {
        // this is a child layer
        for f in &CHILD_LAYER_REQUIRED_FILES {
            tar_append_file(tar, tar_path, layer_path, f)?;
        }
        for f in &CHILD_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(tar, tar_path, layer_path, f)?;
        }
    } else {
        // this is a base layer
        for f in &BASE_LAYER_REQUIRED_FILES {
            tar_append_file(tar, tar_path, layer_path, f)?;
        }
        for f in &BASE_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(tar, tar_path, layer_path, f)?;
        }
    }

    Ok(())
}

