use tokio::prelude::*;

use super::bitarray::*;
use super::bitindex::*;
use super::logarray::*;
use crate::storage::file::*;

#[derive(Clone)]
pub struct AdjacencyList<M:AsRef<[u8]>+Clone> {
    pub nums: LogArray<M>,
    pub bits: BitIndex<M>,
}

impl<M:AsRef<[u8]>+Clone> AdjacencyList<M> {
    pub fn from_parts(nums: LogArray<M>, bits: BitIndex<M>) -> AdjacencyList<M> {
        debug_assert_eq!(nums.len(),bits.len());
        AdjacencyList { nums, bits }
    }

    pub fn parse(nums_slice: M, bits_slice: M, bits_block_slice: M, bits_sblock_slice: M) -> AdjacencyList<M> {
        let nums = LogArray::parse(nums_slice).unwrap();
        let bit_array = BitArray::from_bits(bits_slice);
        let bits_block_array = LogArray::parse(bits_block_slice).unwrap();
        let bits_sblock_array = LogArray::parse(bits_sblock_slice).unwrap();
        let bits = BitIndex::from_parts(bit_array, bits_block_array, bits_sblock_array);

        Self::from_parts(nums, bits)
    }

    pub fn left_count(&self) -> usize {
        if self.bits.len() == 0 {
            0
        }
        else {
            self.bits.rank((self.bits.len() as u64)-1) as usize
        }
    }

    pub fn right_count(&self) -> usize {
        self.bits.len()
    }

    pub fn offset_for(&self, index: u64) -> u64 {
        if index == 1 {
            0
        }
        else {
            self.bits.select(index-1)+1
        }
    }

    pub fn get(&self, index: u64) -> LogArraySlice<M> {
        if index < 1 {
            panic!("minimum index has to be 1");
        }

        let start = self.offset_for(index);
        let end = self.bits.select(index);
        let length = end - start + 1;

        self.nums.slice(start as usize, length as usize)
    }
}


pub struct AdjacencyListBuilder<F,W1,W2,W3>
where F: 'static+FileLoad+FileStore,
      W1: 'static+AsyncWrite+Send+Sync,
      W2: 'static+AsyncWrite+Send+Sync,
      W3: 'static+AsyncWrite+Send+Sync {
    bitfile: F,
    bitarray: BitArrayFileBuilder<F::Write>,
    bitindex_blocks: W1,
    bitindex_sblocks: W2,
    nums: LogArrayFileBuilder<W3>,
    last_left: u64,
    last_right: u64
}

impl<F,W1,W2,W3> AdjacencyListBuilder<F,W1,W2,W3>
where F: 'static+FileLoad+FileStore,
      W1: 'static+AsyncWrite+Send+Sync,
      W2: 'static+AsyncWrite+Send+Sync,
      W3: 'static+AsyncWrite+Send+Sync {
    pub fn new(bitfile: F, bitindex_blocks: W1, bitindex_sblocks: W2, nums_writer: W3, width: u8) -> AdjacencyListBuilder<F,W1,W2,W3> {
        let bitarray = BitArrayFileBuilder::new(bitfile.open_write());

        let nums = LogArrayFileBuilder::new(nums_writer, width);

        AdjacencyListBuilder { bitfile, bitarray, bitindex_blocks, bitindex_sblocks, nums, last_left: 0, last_right: 0 }
    }

    pub fn push(self, left: u64, right: u64) -> impl Future<Item=Self, Error=std::io::Error> {
        // the tricky thing with this code is that the bitarray lags one entry behind the logarray.
        // The reason for this is that at push time, we do not yet know if this entry is going to be
        // the last entry for `left`, we only know this when we push a greater `left` later on.
        let AdjacencyListBuilder { bitfile, bitarray, bitindex_blocks, bitindex_sblocks, nums, last_left, last_right } = self;

        if left < self.last_left || (left == last_left && right <= last_right) {
            panic!("tried to push an unordered adjacent pair");
        }

        // the left hand side of the adjacencylist is expected to be a continuous range from 1 up to the max
        // but when adding entries, there may be holes. We handle holes by writing a '0' to the logarray
        // (which is otherwise an invalid right-hand side) and pushing a 1 onto the bitarray to immediately close the segment.
        let skip = left - self.last_left;
        
        let f1: Box<dyn Future<Item=(BitArrayFileBuilder<F::Write>, LogArrayFileBuilder<W3>), Error=std::io::Error>+Send+Sync> = 
            if last_left == 0 && skip == 1 {
                // this is the first entry. we can't push a bit yet
                Box::new(future::ok((bitarray, nums)))
            }
            else if skip == 0 {
            // same `left` as before. so the previous entry was not the last one, and the bitarray gets a 0 appended.
                Box::new(bitarray.push(false)
                         .map(move |bitarray| (bitarray, nums)))
            } else {
                // if this is the first element, but we do need to skip, make sure we write one less bit than we'd usually do
                let bitskip = if last_left == 0 { skip - 1 } else { skip };
                // there's a different `left`. we push a bunch of 1s to the bitarray, and 0s to the num array.
                Box::new(bitarray.push_all(stream::iter_ok((0..bitskip).map(|_|true)))
                         .and_then(move |bitarray| nums.push_all(stream::iter_ok(0..skip-1).map(|_|0))
                                   .map(move |nums| (bitarray, nums))))
            };

        // finally push right to the logarray
        f1.and_then(move |(bitarray, nums)| nums.push(right)
                    .map(move |nums| AdjacencyListBuilder {
                        bitfile,
                        bitarray,
                        bitindex_blocks,
                        bitindex_sblocks,
                        nums,
                        last_left: left,
                        last_right: right
                    }))
    }

    pub fn push_all<S:Stream<Item=(u64,u64),Error=std::io::Error>>(self, stream:S) -> impl Future<Item=Self, Error=std::io::Error> {
        stream.fold(self, |x, (left, right)| x.push(left, right))
    }

    pub fn finalize(self) -> impl Future<Item=(), Error=std::io::Error> {
        let AdjacencyListBuilder { bitfile, bitarray, bitindex_blocks, bitindex_sblocks, nums, last_left: _, last_right: _ } = self;
        let fut: Box<dyn Future<Item=BitArrayFileBuilder<_>, Error=std::io::Error>+Send+Sync> =
            if nums.count == 0 {
                Box::new(future::ok(bitarray))
            } else {
                Box::new(bitarray.push(true))
            };
        fut
            .and_then(|b|b.finalize())
            .and_then(|_|nums.finalize())
            .and_then(move |_| build_bitindex(bitfile.open_read(), bitindex_blocks, bitindex_sblocks))
            .map(|_|())
    }

    pub fn count(&self) -> u64 {
        self.bitarray.count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn can_build_and_parse_adjacencylist() {
        let bitfile = MemoryBackedStore::new();
        let bitindex_blocks_file = MemoryBackedStore::new();
        let bitindex_sblocks_file = MemoryBackedStore::new();
        let nums_file = MemoryBackedStore::new();

        let builder = AdjacencyListBuilder::new(bitfile.clone(), bitindex_blocks_file.open_write(), bitindex_sblocks_file.open_write(), nums_file.open_write(), 8);
        builder.push_all(stream::iter_ok(vec![(1,1), (1,3), (2,5), (7,4)]))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let bitfile_contents = bitfile.map().wait().unwrap();
        let bitindex_blocks_contents = bitindex_blocks_file.map().wait().unwrap();
        let bitindex_sblocks_contents = bitindex_sblocks_file.map().wait().unwrap();
        let nums_contents = nums_file.map().wait().unwrap();

        let adjacencylist = AdjacencyList::parse(&nums_contents, &bitfile_contents, &bitindex_blocks_contents, &bitindex_sblocks_contents);

        let slice = adjacencylist.get(1);
        assert_eq!(2, slice.len());
        assert_eq!(1, slice.entry(0));
        assert_eq!(3, slice.entry(1));

        let slice = adjacencylist.get(2);
        assert_eq!(1, slice.len());
        assert_eq!(5, slice.entry(0));

        let slice = adjacencylist.get(3);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(4);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(5);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(6);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(7);
        assert_eq!(1, slice.len());
        assert_eq!(4, slice.entry(0));
    }

    #[test]
    fn empty_adjacencylist() {
        let bitfile = MemoryBackedStore::new();
        let bitindex_blocks_file = MemoryBackedStore::new();
        let bitindex_sblocks_file = MemoryBackedStore::new();
        let nums_file = MemoryBackedStore::new();

        let builder = AdjacencyListBuilder::new(bitfile.clone(), bitindex_blocks_file.open_write(), bitindex_sblocks_file.open_write(), nums_file.open_write(), 8);
        builder.push_all(stream::iter_ok(Vec::new()))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let bitfile_contents = bitfile.map().wait().unwrap();
        let bitindex_blocks_contents = bitindex_blocks_file.map().wait().unwrap();
        let bitindex_sblocks_contents = bitindex_sblocks_file.map().wait().unwrap();
        let nums_contents = nums_file.map().wait().unwrap();

        let adjacencylist = AdjacencyList::parse(&nums_contents, &bitfile_contents, &bitindex_blocks_contents, &bitindex_sblocks_contents);

        assert_eq!(0, adjacencylist.left_count());
    }

    #[test]
    fn adjacencylist_with_skip_at_start() {
        let bitfile = MemoryBackedStore::new();
        let bitindex_blocks_file = MemoryBackedStore::new();
        let bitindex_sblocks_file = MemoryBackedStore::new();
        let nums_file = MemoryBackedStore::new();

        let builder = AdjacencyListBuilder::new(bitfile.clone(), bitindex_blocks_file.open_write(), bitindex_sblocks_file.open_write(), nums_file.open_write(), 8);
        builder.push_all(stream::iter_ok(vec![(3,2), (7,4)]))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let bitfile_contents = bitfile.map().wait().unwrap();
        let bitindex_blocks_contents = bitindex_blocks_file.map().wait().unwrap();
        let bitindex_sblocks_contents = bitindex_sblocks_file.map().wait().unwrap();
        let nums_contents = nums_file.map().wait().unwrap();

        let adjacencylist = AdjacencyList::parse(&nums_contents, &bitfile_contents, &bitindex_blocks_contents, &bitindex_sblocks_contents);

        let slice = adjacencylist.get(1);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(2);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        /*
        let slice = adjacencylist.get(3);
        assert_eq!(1, slice.len());
        assert_eq!(2, slice.entry(0));
        */

        let slice = adjacencylist.get(4);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(5);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(6);
        assert_eq!(1, slice.len());
        assert_eq!(0, slice.entry(0));

        let slice = adjacencylist.get(7);
        assert_eq!(1, slice.len());
        assert_eq!(4, slice.entry(0));
    }
}
