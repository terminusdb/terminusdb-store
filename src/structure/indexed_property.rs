use bytes::{Bytes, BytesMut};

use super::{
    util::calculate_width, AdjacencyList, AdjacencyListBufBuilder, AdjacencyListBuffers, LogArray,
    LogArrayBufBuilder, MonotonicLogArray,
};

#[derive(Clone)]
pub struct IndexPropertyBuffers {
    subjects_logarray_buf: Bytes,
    adjacency_bufs: AdjacencyListBuffers,
    objects_logarray_buf: Bytes,
}

pub struct IndexedPropertyBuilder {
    added: Vec<(u64, usize, u64)>,
}

impl IndexedPropertyBuilder {
    pub fn new() -> Self {
        Self { added: Vec::new() }
    }
    pub fn add(&mut self, subject_id: u64, array_index: usize, object_id: u64) {
        self.added.push((subject_id, array_index, object_id));
    }

    pub fn remove(&mut self, subject_id: u64, array_index: usize) {
        self.added.push((subject_id, array_index, 0));
    }

    pub fn finalize(mut self) -> IndexPropertyBuffers {
        if self.added.len() == 0 {
            panic!("no data was added");
        }

        self.added.sort();
        self.added.dedup();
        let mut last = (0, 0);

        let max_subject = self.added.iter().map(|a| a.0).max().unwrap_or(0);
        let max_index = self.added.iter().map(|a| a.1).max().unwrap_or(0);
        let max_object = self.added.iter().map(|a| a.2).max().unwrap_or(0);
        let subjects_width = calculate_width(max_subject);
        let index_width = calculate_width(max_index as u64);
        let object_width = calculate_width(max_object);

        let mut subjects_logarray = LogArrayBufBuilder::new(BytesMut::new(), subjects_width);
        let mut aj_builder = AdjacencyListBufBuilder::new(index_width);
        let mut objects_logarray = LogArrayBufBuilder::new(BytesMut::new(), object_width);

        let mut index = 0;
        for added in self.added {
            if (added.0, added.1) == last {
                panic!("multiple indexed properties for same index and node");
            }
            if added.0 != last.0 {
                // we moved on to a next item
                index += 1;
                subjects_logarray.push(added.0);
            }
            last = (added.0, added.1);
            aj_builder.push(index, added.1 as u64);
            objects_logarray.push(added.2);
        }

        let subjects_buf = subjects_logarray.finalize().freeze();
        let aj_bufs = aj_builder.finalize();
        let objects_buf = objects_logarray.finalize().freeze();

        IndexPropertyBuffers {
            subjects_logarray_buf: subjects_buf,
            adjacency_bufs: aj_bufs,
            objects_logarray_buf: objects_buf,
        }
    }
}

pub struct IndexedPropertyCollection {
    subjects: MonotonicLogArray,
    adjacencies: AdjacencyList,
    objects: LogArray,
}

impl IndexedPropertyCollection {
    pub fn from_buffers(buffers: IndexPropertyBuffers) -> Self {
        Self {
            subjects: MonotonicLogArray::parse(buffers.subjects_logarray_buf).unwrap(),
            adjacencies: AdjacencyList::from_buffers(buffers.adjacency_bufs),
            objects: LogArray::parse(buffers.objects_logarray_buf).unwrap(),
        }
    }

    pub fn lookup_index(&self, subject: u64, index: usize) -> Option<u64> {
        if let Some(subject_index) = self.subjects.index_of(subject) {
            let subject_index = subject_index + 1;
            let offset = self.adjacencies.offset_for(subject_index as u64);
            let indexes = self.adjacencies.get(subject_index as u64);
            if let Some(index_index) = indexes.index_of(index as u64) {
                let total_offset = offset as usize + index_index;
                let object = self.objects.entry(total_offset);

                return Some(object);
            }
        }

        None
    }

    pub fn indexes_for<'a>(&'a self, subject: u64) -> impl Iterator<Item = (usize, u64)> + 'a {
        if let Some(subject_index) = self.subjects.index_of(subject) {
            let subject_index = subject_index + 1;
            let offset = self.adjacencies.offset_for(subject_index as u64);
            let indexes = self.adjacencies.get(subject_index as u64);
            itertools::Either::Left(
                indexes.iter().enumerate().map(move |(ix_ix, ix)| {
                    (ix as usize, self.objects.entry(ix_ix + offset as usize))
                }),
            )
        } else {
            itertools::Either::Right(std::iter::empty())
        }
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u64, usize, u64)> + 'a {
        // TODO this can be a lot more clever by walking the structure more lowlevel
        self.subjects
            .iter()
            .enumerate()
            .flat_map(move |(subject_ix, subject)| {
                let subject_ix = subject_ix + 1;
                let offset = self.adjacencies.offset_for(subject_ix as u64);
                let indexes = self.adjacencies.get(subject_ix as u64);
                indexes.iter().enumerate().map(move |(ix_ix, ix)| {
                    (
                        subject,
                        ix as usize,
                        self.objects.entry(ix_ix + offset as usize),
                    )
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn build_and_lookup_indexed_property_collection() {
        let mut builder = IndexedPropertyBuilder::new();
        builder.add(3, 4, 42);
        builder.add(3, 7, 420);
        builder.add(5, 1, 21);
        let buffers = builder.finalize();
        let collection = IndexedPropertyCollection::from_buffers(buffers);

        assert_eq!(Some(42), collection.lookup_index(3, 4));
        assert_eq!(Some(420), collection.lookup_index(3, 7));
        assert_eq!(Some(21), collection.lookup_index(5, 1));
        assert_eq!(None, collection.lookup_index(5, 2));

        assert_eq!(
            vec![(4, 42), (7, 420)],
            collection.indexes_for(3).collect::<Vec<_>>()
        );

        assert_eq!(vec![(1, 21)], collection.indexes_for(5).collect::<Vec<_>>());

        assert_eq!(
            vec![(3, 4, 42), (3, 7, 420), (5, 1, 21)],
            collection.iter().collect::<Vec<_>>()
        );
    }
}
