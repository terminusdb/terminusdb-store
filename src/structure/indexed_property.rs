use bytes::{BufMut, Bytes};

use super::{util::calculate_width, LogArrayBufBuilder, AdjacencyListBuilder, UnindexedAdjacencyListBuilder, BitArrayFileBuilder};

struct IndexPropertyBuilderBuffers<'a, B: BufMut> {
    subjects_logarray_buf: &'a mut B,
    index_logarray_buf: &'a mut B,
    boundaries_bitarray_buf: &'a mut B,
    boundaries_bitindex_blocks_buf: &'a mut B,
    boundaries_bitindex_sblocks_buf: &'a mut B,
}

struct IndexedPropertyBuilder {
    added: Vec<(u64, usize, u64)>,
}

impl IndexedPropertyBuilder {
    fn new() -> Self {
        Self { added: Vec::new() }
    }
    fn add(&mut self, subject_id: u64, array_index: usize, object_id: u64) {
        self.added.push((subject_id, array_index, object_id));
    }

    fn remove(&mut self, subject_id: u64, array_index: usize) {
        self.added.push((subject_id, array_index, 0));
    }

    fn finalize<B: BufMut>(mut self, buffers: &mut IndexPropertyBuilderBuffers<B>) {
        if self.added.len() == 0 {
            panic!("no data was added");
        }


        self.added.sort();
        self.added.dedup();
        let mut last = (0, 0);

        let max_subject = self.added.iter().map(|a| a.0).max().unwrap_or(0);
        let max_index = self.added.iter().map(|a| a.1).max().unwrap_or(0);
        let subjects_width = calculate_width(max_subject);
        let index_width = calculate_width(max_index as u64);

        let mut subjects_logarray =
            LogArrayBufBuilder::new(buffers.subjects_logarray_buf, subjects_width);
        let mut predicates_logarray = LogArrayBufBuilder::new(buffers.index_logarray_buf, index_width);
        let mut boundary_bitindex = BitArrayFileBuilder;

        for added in self.added {
            if (added.0, added.1) == last {
                panic!("multiple indexed properties for same index and node");
            }
            last = (added.0, added.1);
            subjects_logarray.push(added.0);
            predicates_logarray.push(added.1 as u64);
        }
    }
}
