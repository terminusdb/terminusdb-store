use std::path::PathBuf;
use std::collections::HashMap;
use crate::storage::file::*;

pub trait MultiFileStore {
    type FileBackend: FileLoad+FileStore;

    fn name(&self) -> String;
    fn open(&mut self, file: &str) -> Self::FileBackend;
    fn open_ro(&self, file: &str) -> Option<ReadOnlyFile<Self::FileBackend>>;
}

pub struct MemoryBackedMultiFileStore {
    name: String,
    files: HashMap<String,MemoryBackedStore>
}

impl MultiFileStore for MemoryBackedMultiFileStore {
    type FileBackend = MemoryBackedStore;

    fn name(&self) -> String {
        self.name.clone()
    }

    fn open_ro(&self, file: &str) -> Option<ReadOnlyFile<MemoryBackedStore>> {
        self.files.get(file).map(|f|ReadOnlyFile::new(f.clone()))
    }

    fn open(&mut self, file: &str) -> MemoryBackedStore {
        match self.files.get(file) {
            None => {
                let store = MemoryBackedStore::new();
                self.files.insert(file.to_owned(), store.clone());

                store
            },
            Some(f) => f.clone()
        }
    }
}

pub struct FileBackedMultiFileStore {
    directory: PathBuf
}

impl FileBackedMultiFileStore {
    pub fn new<P:Into<PathBuf>>(path: P) -> FileBackedMultiFileStore {
        let p:PathBuf = path.into();
        if !p.is_dir() {
            panic!("tried to create a FileBackedMultiFileStore with a path that did not point at a directory");
        }

        FileBackedMultiFileStore { directory: p }
    }
}


impl MultiFileStore for FileBackedMultiFileStore {
    type FileBackend = FileBackedStore;

    fn name(&self) -> String {
        self.directory.to_str().unwrap().to_string()
    }

    fn open_ro(&self, file: &str) -> Option<ReadOnlyFile<FileBackedStore>> {
        let mut full_path = self.directory.clone();
        full_path.push(file);

        match full_path.is_file() {
            true => Some(ReadOnlyFile::new(FileBackedStore::new(full_path))),
            false => None
        }
    }

    fn open(&mut self, file: &str) -> FileBackedStore {
        let mut full_path = self.directory.clone();
        full_path.push(file);

        FileBackedStore::new(full_path)
    }

    /*
    pub fn base_layer_builder_from_store(store: &mut S) -> Self {
        let node_dictionary_blocks_file = store.open(FILENAMES.node_dictionary_blocks);
        let node_dictionary_offsets_file = store.open(FILENAMES.node_dictionary_offsets);

        let predicate_dictionary_blocks_file = store.open(FILENAMES.predicate_dictionary_blocks);
        let predicate_dictionary_offsets_file = store.open(FILENAMES.predicate_dictionary_offsets);

        let value_dictionary_blocks_file = store.open(FILENAMES.value_dictionary_blocks);
        let value_dictionary_offsets_file = store.open(FILENAMES.value_dictionary_offsets);

        let base_s_v_adjacency_list_nums_file = store.open(FILENAMES.base_s_v_adjacency_list_nums);
        let base_s_v_adjacency_list_bits_file = store.open(FILENAMES.base_s_v_adjacency_list_bits);
        let base_s_v_adjacency_list_bit_index_blocks_file = store.open(FILENAMES.base_s_v_adjacency_list_bit_index_blocks);
        let base_s_v_adjacency_list_bit_index_sblocks_file = store.open(FILENAMES.base_s_v_adjacency_list_bit_index_sblocks);

        let base_sv_o_adjacency_list_nums_file = store.open(FILENAMES.base_sv_o_adjacency_list_nums);
        let base_sv_o_adjacency_list_bits_file = store.open(FILENAMES.base_sv_o_adjacency_list_bits);
        let base_sv_o_adjacency_list_bit_index_blocks_file = store.open(FILENAMES.base_sv_o_adjacency_list_bit_index_blocks);
        let base_sv_o_adjacency_list_bit_index_sblocks_file = store.open(FILENAMES.base_sv_o_adjacency_list_bit_index_sblocks);

        Self::new(node_dictionary_blocks_file,
                  node_dictionary_offsets_file,

                  predicate_dictionary_blocks_file,
                  predicate_dictionary_offsets_file,

                  value_dictionary_blocks_file,
                  value_dictionary_offsets_file,

                  base_s_v_adjacency_list_nums_file,
                  base_s_v_adjacency_list_bits_file,
                  base_s_v_adjacency_list_bit_index_blocks_file,
                  base_s_v_adjacency_list_bit_index_sblocks_file,

                  base_sv_o_adjacency_list_nums_file,
                  base_sv_o_adjacency_list_bits_file,
                  base_sv_o_adjacency_list_bit_index_blocks_file,
                  base_sv_o_adjacency_list_bit_index_sblocks_file)
    }
    */
}

pub struct ReadOnlyFile<T:FileLoad> {
    file: T
}

impl<T:FileLoad> ReadOnlyFile<T> {
    pub fn new(file: T) -> ReadOnlyFile<T> {
        ReadOnlyFile { file }
    }
}

impl<T:FileLoad> FileLoad for ReadOnlyFile<T> {
    type Read = T::Read;
    type Map = T::Map;

    fn size(&self) -> usize {
        self.file.size()
    }

    fn open_read_from(&self, offset: usize) -> Self::Read {
        self.file.open_read_from(offset)
    }

    fn map(&self) -> Self::Map {
        self.file.map()
    }
}

pub trait StoreContainer {
    type MFS: MultiFileStore+Sized;

    fn create(name: &str) -> Self::MFS;
    fn open(name: &str) -> Self::MFS;
}

pub struct MemoryBackedStorageContainer {
}
