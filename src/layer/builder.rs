use std::io;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use rayon::prelude::*;

use super::layer::*;
use crate::storage::*;
use crate::structure::util;
use crate::structure::*;

pub struct DictionarySetFileBuilder<F: 'static + FileLoad + FileStore> {
    node_files: DictionaryFiles<F>,
    predicate_files: DictionaryFiles<F>,
    value_files: TypedDictionaryFiles<F>,
    node_dictionary_builder: StringDictBufBuilder<BytesMut, BytesMut>,
    predicate_dictionary_builder: StringDictBufBuilder<BytesMut, BytesMut>,
    value_dictionary_builder: TypedDictBufBuilder<BytesMut, BytesMut, BytesMut, BytesMut>,
}

impl<F: 'static + FileLoad + FileStore> DictionarySetFileBuilder<F> {
    pub async fn from_files(
        node_files: DictionaryFiles<F>,
        predicate_files: DictionaryFiles<F>,
        value_files: TypedDictionaryFiles<F>,
    ) -> io::Result<Self> {
        let node_dictionary_builder = StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());
        let predicate_dictionary_builder =
            StringDictBufBuilder::new(BytesMut::new(), BytesMut::new());
        let value_dictionary_builder = TypedDictBufBuilder::new(
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
            BytesMut::new(),
        );

        Ok(Self {
            node_files,
            predicate_files,
            value_files,
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        })
    }

    /// Add a node string.
    ///
    /// Panics if the given node string is not a lexical successor of the previous node string.
    pub fn add_node(&mut self, node: &str) -> u64 {
        let id = self
            .node_dictionary_builder
            .add(Bytes::copy_from_slice(node.as_bytes()));

        id
    }

    pub fn add_node_bytes(&mut self, node: Bytes) -> u64 {
        let id = self.node_dictionary_builder.add(node);

        id
    }

    /// Add a predicate string.
    ///
    /// Panics if the given predicate string is not a lexical successor of the previous node string.
    pub fn add_predicate(&mut self, predicate: &str) -> u64 {
        let id = self
            .predicate_dictionary_builder
            .add(Bytes::copy_from_slice(predicate.as_bytes()));

        id
    }

    pub fn add_predicate_bytes(&mut self, predicate: Bytes) -> u64 {
        let id = self.predicate_dictionary_builder.add(predicate);

        id
    }

    /// Add a value string.
    ///
    /// Panics if the given value string is not a lexical successor of the previous value string.
    pub fn add_value(&mut self, value: TypedDictEntry) -> u64 {
        let id = self.value_dictionary_builder.add(value);

        id
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous added nodes are a lexical succesor of any of these nodes.
    pub fn add_nodes<I: 'static + IntoIterator<Item = String> + Unpin + Send + Sync>(
        &mut self,
        nodes: I,
    ) -> Vec<u64>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for node in nodes {
            let id = self.add_node(&node);
            ids.push(id);
        }

        ids
    }

    pub fn add_nodes_bytes<I: 'static + IntoIterator<Item = Bytes> + Unpin + Send + Sync>(
        &mut self,
        nodes: I,
    ) -> Vec<u64>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for node in nodes {
            let id = self.add_node_bytes(node);
            ids.push(id);
        }

        ids
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if previous added predicates are a lexical succesor of any of these predicates.
    pub fn add_predicates<I: 'static + IntoIterator<Item = String> + Unpin + Send + Sync>(
        &mut self,
        predicates: I,
    ) -> Vec<u64>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for predicate in predicates {
            let id = self.add_predicate(&predicate);
            ids.push(id);
        }

        ids
    }

    pub fn add_predicates_bytes<I: 'static + IntoIterator<Item = Bytes> + Unpin + Send + Sync>(
        &mut self,
        predicates: I,
    ) -> Vec<u64>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for predicate in predicates {
            let id = self.add_predicate_bytes(predicate);
            ids.push(id);
        }

        ids
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous added values are a lexical succesor of any of these values.
    pub fn add_values<I: 'static + IntoIterator<Item = TypedDictEntry> + Unpin + Send + Sync>(
        &mut self,
        values: I,
    ) -> Vec<u64>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for value in values {
            let id = self.add_value(value);
            ids.push(id);
        }

        ids
    }

    pub async fn finalize(self) -> io::Result<()> {
        let (mut node_offsets_buf, mut node_data_buf) = self.node_dictionary_builder.finalize();
        let (mut predicate_offsets_buf, mut predicate_data_buf) =
            self.predicate_dictionary_builder.finalize();
        let (
            mut value_types_present_buf,
            mut value_type_offsets_buf,
            mut value_offsets_buf,
            mut value_data_buf,
        ) = self.value_dictionary_builder.finalize();

        self.node_files
            .write_all_from_bufs(&mut node_data_buf, &mut node_offsets_buf)
            .await?;
        self.predicate_files
            .write_all_from_bufs(&mut predicate_data_buf, &mut predicate_offsets_buf)
            .await?;

        self.value_files
            .write_all_from_bufs(
                &mut value_types_present_buf,
                &mut value_type_offsets_buf,
                &mut value_offsets_buf,
                &mut value_data_buf,
            )
            .await?;

        Ok(())
    }
}

pub struct TripleFileBuilder<F: 'static + FileLoad + FileStore> {
    subjects_file: Option<F>,
    subjects: Option<Vec<u64>>,

    s_p_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    sp_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    last_subject: u64,
    last_predicate: u64,
}

impl<F: 'static + FileLoad + FileStore> TripleFileBuilder<F> {
    pub async fn new(
        s_p_adjacency_list_files: AdjacencyListFiles<F>,
        sp_o_adjacency_list_files: AdjacencyListFiles<F>,
        num_nodes: usize,
        num_predicates: usize,
        num_values: usize,
        subjects_file: Option<F>,
    ) -> io::Result<Self> {
        let s_p_width = util::calculate_width(num_predicates as u64);
        let sp_o_width = util::calculate_width((num_nodes + num_values) as u64);

        let s_p_adjacency_list_builder = AdjacencyListBuilder::new(
            s_p_adjacency_list_files.bitindex_files.bits_file,
            s_p_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write()
                .await?,
            s_p_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write()
                .await?,
            s_p_adjacency_list_files.nums_file.open_write().await?,
            s_p_width,
        )
        .await?;

        let sp_o_adjacency_list_builder = AdjacencyListBuilder::new(
            sp_o_adjacency_list_files.bitindex_files.bits_file,
            sp_o_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write()
                .await?,
            sp_o_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write()
                .await?,
            sp_o_adjacency_list_files.nums_file.open_write().await?,
            sp_o_width,
        )
        .await?;

        let subjects = match subjects_file.is_some() {
            true => Some(Vec::new()),
            false => None,
        };

        Ok(Self {
            subjects,
            subjects_file,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject: 0,
            last_predicate: 0,
        })
    }

    /// Add the given subject, predicate and object.
    ///
    /// This will panic if a greater triple has already been added.
    pub async fn add_triple(
        &mut self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<()> {
        if subject == 0 || predicate == 0 || object == 0 {
            return Ok(());
        }

        if subject < self.last_subject {
            panic!("layer builder got addition in wrong order (subject is {} while previously {} was pushed)", subject, self.last_subject)
        } else if self.last_subject == subject && self.last_predicate == predicate {
            // only the second adjacency list has to be pushed to
            let count = self.s_p_adjacency_list_builder.count() + 1;

            self.sp_o_adjacency_list_builder.push(count, object).await?;
        } else {
            // both list have to be pushed to
            if self.subjects.is_some() && subject != self.last_subject {
                self.subjects.as_mut().unwrap().push(subject);
            }
            let mapped_subject = self
                .subjects
                .as_ref()
                .map(|s| s.len() as u64)
                .unwrap_or(subject);
            self.s_p_adjacency_list_builder
                .push(mapped_subject, predicate)
                .await?;
            let count = self.s_p_adjacency_list_builder.count() + 1;

            self.sp_o_adjacency_list_builder.push(count, object).await?;
        }

        self.last_subject = subject;
        self.last_predicate = predicate;

        Ok(())
    }

    /// Add the given triples.
    ///
    /// This will panic if a greater triple has already been added.
    pub async fn add_id_triples<I: 'static + IntoIterator<Item = IdTriple>>(
        &mut self,
        triples: I,
    ) -> io::Result<()> {
        for triple in triples {
            self.add_triple(triple.subject, triple.predicate, triple.object)
                .await?;
        }

        Ok(())
    }

    pub async fn finalize(self) -> io::Result<()> {
        self.s_p_adjacency_list_builder.finalize().await?;
        self.sp_o_adjacency_list_builder.finalize().await?;

        if let Some(subjects) = self.subjects {
            // isn't this just last_subject?
            let max_subject = if subjects.is_empty() {
                0
            } else {
                subjects[subjects.len() - 1]
            };

            let subjects_width = util::calculate_width(max_subject);
            let mut subjects_logarray_builder = LogArrayFileBuilder::new(
                self.subjects_file.unwrap().open_write().await?,
                subjects_width,
            );

            subjects_logarray_builder.push_vec(subjects).await?;
            subjects_logarray_builder.finalize().await?;
        };

        Ok(())
    }
}

pub async fn build_object_index_from_direct_files<
    FLoad: 'static + FileLoad,
    F: 'static + FileLoad + FileStore,
>(
    sp_o_nums_file: FLoad,
    sp_o_bits_file: FLoad,
    o_ps_files: AdjacencyListFiles<F>,
    objects_file: Option<F>,
) -> io::Result<()> {
    eprintln!("{:?}: starting object index build", chrono::offset::Local::now());
    let build_sparse_index = objects_file.is_some();
    let (count, _) = logarray_file_get_length_and_width(sp_o_nums_file.clone()).await?;
    let mut aj_stream = adjacency_list_stream_pairs(sp_o_bits_file, sp_o_nums_file).await?;
    let mut pairs = Vec::with_capacity(count as usize);
    let mut greatest_sp = 0;
    eprintln!("{:?}: opened sp_o stream", chrono::offset::Local::now());
    let mut tally: u64 = 0;
    // gather up pairs
    while let Some((sp, object)) = aj_stream.try_next().await? {
        greatest_sp = sp;
        pairs.push((object, sp));
        tally += 1;
        if tally % 10000000 == 0 {
            eprintln!("{:?}: collected {tally} pairs for o_ps index ({}%)", chrono::offset::Local::now(), (tally*100/count));
        }
    }
    eprintln!("{:?}: collected object pairs", chrono::offset::Local::now());

    // par_sort_unstable unfortunately can run out of stack for very
    // large sorts. If so, we have to do something else.
    const SINGLE_SORT_LIMIT: u64 = 0x1_0000_0000;
    if count > SINGLE_SORT_LIMIT {
        eprintln!("{:?}: perform multi sort", chrono::offset::Local::now());
        let mut tally: u64 = 0;
        while tally < count {
            let end = std::cmp::min(count as usize, (tally+SINGLE_SORT_LIMIT) as usize);
            let slice = &mut pairs[tally as usize..end];
            slice.par_sort_unstable();
            tally += SINGLE_SORT_LIMIT;
        }
        eprintln!("{:?}: perform final sort", chrono::offset::Local::now());
        // we use the normal sort as it is fast for cases where you
        // have a bunch of appended sorted slices.
        pairs.sort();

    } else {
        eprintln!("{:?}: perform single sort", chrono::offset::Local::now());
        pairs.par_sort_unstable();
    }
    eprintln!("{:?}: sorted object pairs", chrono::offset::Local::now());

    let aj_width = util::calculate_width(greatest_sp);
    let mut o_ps_adjacency_list_builder = AdjacencyListBuilder::new(
        o_ps_files.bitindex_files.bits_file,
        o_ps_files.bitindex_files.blocks_file.open_write().await?,
        o_ps_files.bitindex_files.sblocks_file.open_write().await?,
        o_ps_files.nums_file.open_write().await?,
        aj_width,
    )
    .await?;

    if build_sparse_index {
        // a sparse index compresses the adjacency list so that all objects in use are remapped to form a continuous range.
        // We need to iterate over the pairs, and write them out without gaps.

        let mut objects = Vec::new();
        let mut last_object = 0;
        let mut object_ix = 0;
        for (object, sp) in pairs {
            if object > last_object {
                object_ix += 1;
                last_object = object;

                // keep track of all objects in use in a separate list
                objects.push(object);
            }

            o_ps_adjacency_list_builder.push(object_ix, sp).await?;
        }
        let objects_width = util::calculate_width(last_object);

        // write out the object list
        let mut objects_builder =
            LogArrayFileBuilder::new(objects_file.unwrap().open_write().await?, objects_width);
        objects_builder.push_vec(objects).await?;
        objects_builder.finalize().await?;
    } else {
        o_ps_adjacency_list_builder
            .push_all(util::stream_iter_ok(pairs))
            .await?;
    }
    eprintln!(
        "{:?}: added object pairs to adjacency list builder",
        chrono::offset::Local::now()
    );

    o_ps_adjacency_list_builder.finalize().await?;
    eprintln!("{:?}: finalized object index", chrono::offset::Local::now());

    Ok(())
}

pub async fn build_object_index<FLoad: 'static + FileLoad, F: 'static + FileLoad + FileStore>(
    sp_o_files: AdjacencyListFiles<FLoad>,
    o_ps_files: AdjacencyListFiles<F>,
    objects_file: Option<F>,
) -> io::Result<()> {
    build_object_index_from_direct_files(
        sp_o_files.nums_file,
        sp_o_files.bitindex_files.bits_file,
        o_ps_files,
        objects_file,
    )
    .await
}

pub async fn build_predicate_index<FLoad: 'static + FileLoad, F: 'static + FileLoad + FileStore>(
    source: FLoad,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> io::Result<()> {
    build_wavelet_tree_from_logarray(
        source,
        destination_bits,
        destination_blocks,
        destination_sblocks,
    )
    .await
}

pub async fn build_indexes<FLoad: 'static + FileLoad, F: 'static + FileLoad + FileStore>(
    s_p_files: AdjacencyListFiles<FLoad>,
    sp_o_files: AdjacencyListFiles<FLoad>,
    o_ps_files: AdjacencyListFiles<F>,
    objects_file: Option<F>,
    wavelet_files: BitIndexFiles<F>,
) -> io::Result<()> {
    let object_index_task = tokio::spawn(build_object_index(sp_o_files, o_ps_files, objects_file));
    let predicate_index_task = tokio::spawn(build_predicate_index(
        s_p_files.nums_file,
        wavelet_files.bits_file,
        wavelet_files.blocks_file,
        wavelet_files.sblocks_file,
    ));

    object_index_task.await??;
    eprintln!("{:?}: built object index", chrono::offset::Local::now());
    predicate_index_task.await??;
    eprintln!("{:?}: built predicate index", chrono::offset::Local::now());

    Ok(())
}
