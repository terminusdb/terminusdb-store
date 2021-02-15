use std::io;

use futures::stream::TryStreamExt;
use rayon::prelude::*;

use super::layer::*;
use crate::storage::*;
use crate::structure::util;
use crate::structure::*;

pub struct DictionarySetFileBuilder<F: 'static + FileStore> {
    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F: 'static + FileLoad + FileStore> DictionarySetFileBuilder<F> {
    pub fn from_files(
        node_files: DictionaryFiles<F>,
        predicate_files: DictionaryFiles<F>,
        value_files: DictionaryFiles<F>,
    ) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(
            node_files.blocks_file.open_write(),
            node_files.offsets_file.open_write(),
        );
        let predicate_dictionary_builder = PfcDictFileBuilder::new(
            predicate_files.blocks_file.open_write(),
            predicate_files.offsets_file.open_write(),
        );
        let value_dictionary_builder = PfcDictFileBuilder::new(
            value_files.blocks_file.open_write(),
            value_files.offsets_file.open_write(),
        );

        Self {
            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder,
        }
    }

    /// Add a node string.
    ///
    /// Panics if the given node string is not a lexical successor of the previous node string.
    pub async fn add_node(&mut self, node: &str) -> io::Result<u64> {
        let id = self.node_dictionary_builder.add(node).await?;

        Ok(id)
    }

    /// Add a predicate string.
    ///
    /// Panics if the given predicate string is not a lexical successor of the previous node string.
    pub async fn add_predicate(&mut self, predicate: &str) -> io::Result<u64> {
        let id = self.predicate_dictionary_builder.add(predicate).await?;

        Ok(id)
    }

    /// Add a value string.
    ///
    /// Panics if the given value string is not a lexical successor of the previous value string.
    pub async fn add_value(&mut self, value: &str) -> io::Result<u64> {
        let id = self.value_dictionary_builder.add(value).await?;

        Ok(id)
    }

    /// Add nodes from an iterable.
    ///
    /// Panics if the nodes are not in lexical order, or if previous added nodes are a lexical succesor of any of these nodes.
    pub async fn add_nodes<I: 'static + IntoIterator<Item = String> + Unpin + Send + Sync>(
        &mut self,
        nodes: I,
    ) -> io::Result<Vec<u64>>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for node in nodes {
            let id = self.add_node(&node).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    /// Add predicates from an iterable.
    ///
    /// Panics if the predicates are not in lexical order, or if previous added predicates are a lexical succesor of any of these predicates.
    pub async fn add_predicates<I: 'static + IntoIterator<Item = String> + Unpin + Send + Sync>(
        &mut self,
        predicates: I,
    ) -> io::Result<Vec<u64>>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for predicate in predicates {
            let id = self.add_predicate(&predicate).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    /// Add values from an iterable.
    ///
    /// Panics if the values are not in lexical order, or if previous added values are a lexical succesor of any of these values.
    pub async fn add_values<I: 'static + IntoIterator<Item = String> + Unpin + Send + Sync>(
        &mut self,
        values: I,
    ) -> io::Result<Vec<u64>>
    where
        <I as std::iter::IntoIterator>::IntoIter: Unpin + Send + Sync,
    {
        let mut ids = Vec::new();
        for value in values {
            let id = self.add_value(&value).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    pub async fn finalize(self) -> io::Result<()> {
        self.node_dictionary_builder.finalize().await?;
        self.predicate_dictionary_builder.finalize().await?;
        self.value_dictionary_builder.finalize().await?;

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
    pub fn new(
        s_p_adjacency_list_files: AdjacencyListFiles<F>,
        sp_o_adjacency_list_files: AdjacencyListFiles<F>,
        num_nodes: usize,
        num_predicates: usize,
        num_values: usize,
        subjects_file: Option<F>,
    ) -> Self {
        let s_p_width = util::calculate_width(num_predicates as u64);
        let sp_o_width = util::calculate_width((num_nodes + num_values) as u64);

        let s_p_adjacency_list_builder = AdjacencyListBuilder::new(
            s_p_adjacency_list_files.bitindex_files.bits_file,
            s_p_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write(),
            s_p_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write(),
            s_p_adjacency_list_files.nums_file.open_write(),
            s_p_width,
        );

        let sp_o_adjacency_list_builder = AdjacencyListBuilder::new(
            sp_o_adjacency_list_files.bitindex_files.bits_file,
            sp_o_adjacency_list_files
                .bitindex_files
                .blocks_file
                .open_write(),
            sp_o_adjacency_list_files
                .bitindex_files
                .sblocks_file
                .open_write(),
            sp_o_adjacency_list_files.nums_file.open_write(),
            sp_o_width,
        );

        let subjects = match subjects_file.is_some() {
            true => Some(Vec::new()),
            false => None,
        };

        Self {
            subjects,
            subjects_file,
            s_p_adjacency_list_builder,
            sp_o_adjacency_list_builder,
            last_subject: 0,
            last_predicate: 0,
        }
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
            let mut subjects_logarray_builder =
                LogArrayFileBuilder::new(self.subjects_file.unwrap().open_write(), subjects_width);

            subjects_logarray_builder.push_vec(subjects).await?;
            subjects_logarray_builder.finalize().await?;
        };

        Ok(())
    }
}

pub async fn build_object_index<FLoad: 'static + FileLoad, F: 'static + FileLoad + FileStore>(
    sp_o_files: AdjacencyListFiles<FLoad>,
    o_ps_files: AdjacencyListFiles<F>,
    objects_file: Option<F>,
) -> io::Result<()> {
    let build_sparse_index = objects_file.is_some();
    let mut aj_stream =
        adjacency_list_stream_pairs(sp_o_files.bitindex_files.bits_file, sp_o_files.nums_file);
    let mut pairs = Vec::new();
    let mut greatest_sp = 0;
    // gather up pairs
    while let Some((sp, object)) = aj_stream.try_next().await? {
        greatest_sp = sp;
        pairs.push((object, sp));
    }
    pairs.par_sort_unstable();

    let aj_width = util::calculate_width(greatest_sp);
    let mut o_ps_adjacency_list_builder = AdjacencyListBuilder::new(
        o_ps_files.bitindex_files.bits_file,
        o_ps_files.bitindex_files.blocks_file.open_write(),
        o_ps_files.bitindex_files.sblocks_file.open_write(),
        o_ps_files.nums_file.open_write(),
        aj_width,
    );

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
            LogArrayFileBuilder::new(objects_file.unwrap().open_write(), objects_width);
        objects_builder.push_vec(objects).await?;
        objects_builder.finalize().await?;
    } else {
        o_ps_adjacency_list_builder
            .push_all(util::stream_iter_ok(pairs))
            .await?;
    }

    o_ps_adjacency_list_builder.finalize().await
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
    predicate_index_task.await??;

    Ok(())
}
