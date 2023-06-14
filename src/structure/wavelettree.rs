//! A succinct data structure for quick lookup of entry positions in a sequence.

use bitvec::vec::BitVec;
use futures::Stream;
use futures::TryStreamExt;

use super::bitarray::*;
use super::bitindex::*;
use super::logarray::*;
use super::util;
use crate::storage::*;

use std::convert::TryInto;
use std::io;

/// A wavelet tree, encoding a u64 array for fast lookup of number positions.
///
/// A wavelet tree consists of a layer of bitarrays (stored as one big
/// bitarray). The amount of layers is the log2 of the alphabet size,
/// rounded up to make it an integer. Since we're encoding u64 values,
/// the number of layers can never be larger than 64.
#[derive(Clone)]
pub struct WaveletTree {
    bits: BitIndex,
    num_layers: u8,
}

/// A lookup for all positions of a particular entry.
///
/// This struct caches part of the calculation required to get
/// positions out of a wavelet tree, allowing for quick iteration over
/// all positions for a given entry.
#[derive(Clone)]
pub struct WaveletLookup {
    /// the entry this lookup was created for.
    pub entry: u64,
    tree: WaveletTree,
    slices: Vec<(bool, u64, u64)>,
}

impl WaveletLookup {
    /// Returns the amount of positions found in this lookup.
    pub fn len(&self) -> usize {
        let (b, start, end) = *self.slices.last().unwrap();

        if b {
            self.tree.bits.rank1_from_range(start, end) as usize
        } else {
            self.tree.bits.rank0_from_range(start, end) as usize
        }
    }

    /// Returns the position of the index'th entry of this lookup
    pub fn entry(&self, index: usize) -> u64 {
        if index >= self.len() {
            panic!("entry is out of bounds");
        }

        let mut result = (index + 1) as u64;
        for &(b, start_index, end_index) in self.slices.iter().rev() {
            if b {
                result = self
                    .tree
                    .bits
                    .select1_from_range(result, start_index, end_index)
                    .unwrap()
                    - start_index
                    + 1;
            } else {
                result = self
                    .tree
                    .bits
                    .select0_from_range(result, start_index, end_index)
                    .unwrap()
                    - start_index
                    + 1;
            }
        }

        result - 1
    }

    /// Returns an Iterator over all positions for the entry of this lookup
    pub fn iter(&self) -> impl Iterator<Item = u64> {
        let cloned = self.clone();
        (0..self.len()).map(move |i| cloned.entry(i))
    }
}

impl WaveletTree {
    /// Construct a wavelet tree from a bitindex and a layer count.
    pub fn from_parts(bits: BitIndex, num_layers: u8) -> WaveletTree {
        if num_layers != 0 && bits.len() % num_layers as usize != 0 {
            panic!("the bitarray length is not a multiple of the number of layers");
        }

        WaveletTree { bits, num_layers }
    }

    /// Returns the length of the encoded array.
    pub fn len(&self) -> usize {
        if self.num_layers == 0 {
            0
        } else {
            self.bits.len() / self.num_layers as usize
        }
    }

    /// Returns the amount of layers.
    pub fn num_layers(&self) -> usize {
        self.num_layers as usize
    }

    /// Decode the wavelet tree to the original u64 sequence. This returns an iterator.
    pub fn decode(&self) -> impl Iterator<Item = u64> {
        let owned = self.clone();
        (0..self.len()).map(move |i| owned.decode_one(i))
    }

    /// Decode a single position of the original u64 sequence.
    pub fn decode_one(&self, index: usize) -> u64 {
        let len = self.len() as u64;
        let mut offset = index as u64;
        let mut alphabet_start = 0;
        let mut alphabet_end = 2_u64.pow(self.num_layers as u32) as u64;
        let mut range_start = 0;
        let mut range_end = len;
        for i in 0..self.num_layers as u64 {
            let index = i * len + range_start + offset;
            if index as usize >= self.bits.len() {
                panic!("inner loop reached an index that is too high");
            }
            let bit = self.bits.get(index);

            let range_start_index = i * len + range_start;
            let range_end_index = i * len + range_end;
            if bit {
                alphabet_start = (alphabet_start + alphabet_end) / 2;
                offset = self.bits.rank1_from_range(range_start_index, index + 1) - 1;

                let zeros_in_range = self
                    .bits
                    .rank0_from_range(range_start_index, range_end_index);
                range_start += zeros_in_range;
            } else {
                alphabet_end = (alphabet_start + alphabet_end) / 2;
                offset = self.bits.rank0_from_range(range_start_index, index + 1) - 1;

                let ones_in_range = self
                    .bits
                    .rank1_from_range(range_start_index, range_end_index);
                range_end -= ones_in_range;
            }
        }

        assert!(alphabet_start == alphabet_end - 1);

        alphabet_start
    }

    /// Lookup the given entry. This returns a `WaveletLookup` which can then be used to find all positions.
    pub fn lookup(&self, entry: u64) -> Option<WaveletLookup> {
        if self.num_layers == 0 {
            // without any layers, there's not going to be any elements
            return None;
        }

        let width = self.len() as u64;
        let mut slices = Vec::with_capacity(self.num_layers as usize);
        let mut alphabet_start = 0;
        let mut alphabet_end = 2_u64.pow(self.num_layers as u32) as u64;

        if entry >= alphabet_end {
            return None;
        }

        let mut start_index = 0_u64;
        let mut end_index = self.len() as u64;
        for i in 0..self.num_layers {
            let full_start_index = (i as u64) * width + start_index;
            let full_end_index = (i as u64) * width + end_index;
            let b = entry >= (alphabet_start + alphabet_end) / 2;
            slices.push((b, full_start_index, full_end_index));
            if b {
                alphabet_start += 2_u64.pow((self.num_layers - i - 1) as u32);
                start_index += self.bits.rank0_from_range(full_start_index, full_end_index);
            } else {
                alphabet_end -= 2_u64.pow((self.num_layers - i - 1) as u32);
                end_index -= self.bits.rank1_from_range(full_start_index, full_end_index);
            }

            if start_index == end_index {
                return None;
            }
        }

        Some(WaveletLookup {
            entry,
            slices,
            tree: self.clone(),
        })
    }

    /// Lookup the given entry. This returns a single result, even if there's multiple.
    pub fn lookup_one(&self, entry: u64) -> Option<u64> {
        self.lookup(entry).map(|l| l.entry(0))
    }
}

#[derive(Debug)]
struct FragmentBuilder {
    fragment_start: u64,
    fragment_half: u64,
    fragment_end: u64,
    bits: BitVec,
}

impl FragmentBuilder {
    fn new(fragment_start: u64, fragment_end: u64) -> Self {
        let fragment_half = (fragment_start + fragment_end) / 2;

        Self {
            fragment_start,
            fragment_half,
            fragment_end,
            bits: BitVec::new()
        }
    }

    fn push(&mut self, num: u64) {
        if num < self.fragment_start || num >= self.fragment_end {
            // this number doesn't fit in this fragment so ignore
            return;
        }

        self.bits.push(num >= self.fragment_half);
    }
}

impl IntoIterator for FragmentBuilder {
    type Item = bool;
    type IntoIter = bitvec::boxed::IntoIter<usize>;

    fn into_iter(self) -> Self::IntoIter {
        self.bits.into_iter()
    }
}

fn create_fragments(width: u8) -> Vec<FragmentBuilder> {
    let upper = 2_u64.pow(width as u32);

    let len = 2_usize.pow(width as u32) - 1;
    let mut result = Vec::with_capacity(len);

    for i in 0..width {
        let increment = upper >> i;
        let num = 2_u64.pow(i as u32);
        for j in 0..num {
            result.push(FragmentBuilder::new(j * increment, (j + 1) * increment));
        }
    }

    result
}

fn push_to_fragments(num: u64, width: u8, fragments: &mut Vec<FragmentBuilder>) {
    let mut num_it: usize = num.try_into().unwrap(); // this will ensure that we get some sort of error on 32 bit for large nums
    for i in 0..width {
        num_it >>= 1;
        let index = num_it + 2_usize.pow((width - i - 1) as u32) - 1;
        fragments[index].push(num);
    }
}

/// Build a wavelet tree from a stream
pub async fn build_wavelet_tree_from_stream<
    S: Stream<Item = io::Result<u64>> + Unpin,
    F: 'static + FileLoad + FileStore,
>(
    width: u8,
    mut source: S,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> io::Result<()> {
    let mut bits = BitArrayFileBuilder::new(destination_bits.open_write().await?);
    let mut fragments = create_fragments(width);

    while let Some(num) = source.try_next().await? {
        push_to_fragments(num, width, &mut fragments);
    }

    let iter = fragments.into_iter().flat_map(|f| f.into_iter());

    bits.push_all(util::stream_iter_ok(iter)).await?;
    bits.finalize().await?;

    build_bitindex(
        destination_bits.open_read().await?,
        destination_blocks.open_write().await?,
        destination_sblocks.open_write().await?,
    )
    .await?;

    Ok(())
}

/// Build a wavelet tree from an iterator
pub async fn build_wavelet_tree_from_iter<
    I: Iterator<Item = u64>,
    F: 'static + FileLoad + FileStore,
>(
    width: u8,
    source: I,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> io::Result<()> {
    let mut bits = BitArrayFileBuilder::new(destination_bits.open_write().await?);
    let mut fragments = create_fragments(width);

    for num in source {
        push_to_fragments(num, width, &mut fragments);
    }

    let iter = fragments.into_iter().flat_map(|f| f.into_iter());

    bits.push_all(util::stream_iter_ok(iter)).await?;
    bits.finalize().await?;

    build_bitindex(
        destination_bits.open_read().await?,
        destination_blocks.open_write().await?,
        destination_sblocks.open_write().await?,
    )
    .await?;

    Ok(())
}

/// Build a wavelet tree from a file storing a logarray.
pub async fn build_wavelet_tree_from_logarray<
    FLoad: 'static + FileLoad,
    F: 'static + FileLoad + FileStore,
>(
    source: FLoad,
    destination_bits: F,
    destination_blocks: F,
    destination_sblocks: F,
) -> io::Result<()> {
    let (_, width) = logarray_file_get_length_and_width(source.clone()).await?;
    let stream = logarray_stream_entries(source).await?;

    build_wavelet_tree_from_stream(
        width,
        stream,
        destination_bits,
        destination_blocks,
        destination_sblocks,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use futures::executor::block_on;

    #[test]
    fn generate_and_decode_wavelet_tree_from_vec() {
        let contents = vec![21, 1, 30, 13, 23, 21, 3, 0, 21, 21, 12, 11];
        let contents_closure = contents.clone();
        let contents_len = contents.len();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_iter(
            5,
            contents_closure.into_iter(),
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 5);

        assert_eq!(contents_len, wavelet_tree.len());

        assert_eq!(contents, wavelet_tree.decode().collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn generate_and_decode_wavelet_tree_from_logarray() {
        let logarray_file = MemoryBackedStore::new();
        let mut logarray_builder =
            LogArrayFileBuilder::new(logarray_file.open_write().await.unwrap(), 5);
        let contents = vec![21, 1, 30, 13, 23, 21, 3, 0, 21, 21, 12, 11];
        let contents_len = contents.len();
        block_on(async {
            logarray_builder
                .push_all(util::stream_iter_ok(contents.clone()))
                .await?;
            logarray_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_logarray(
            logarray_file,
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 5);

        assert_eq!(contents_len, wavelet_tree.len());

        assert_eq!(contents, wavelet_tree.decode().collect::<Vec<_>>());
    }

    #[test]
    fn slice_wavelet_tree() {
        let contents = vec![8, 3, 8, 8, 1, 2, 3, 2, 8, 9, 3, 3, 6, 7, 0, 4, 8, 7, 3];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_iter(
            4,
            contents_closure.into_iter(),
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        let slice = wavelet_tree.lookup(8).unwrap();
        assert_eq!(vec![0, 2, 3, 8, 16], slice.iter().collect::<Vec<_>>());
        let slice = wavelet_tree.lookup(3).unwrap();
        assert_eq!(vec![1, 6, 10, 11, 18], slice.iter().collect::<Vec<_>>());
        let slice = wavelet_tree.lookup(0).unwrap();
        assert_eq!(vec![14], slice.iter().collect::<Vec<_>>());
        let slice = wavelet_tree.lookup(5);
        assert!(slice.is_none());
    }

    #[test]
    fn empty_wavelet_tree() {
        let contents = Vec::new();
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_iter(
            4,
            contents_closure.into_iter(),
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert!(wavelet_tree.lookup(3).is_none());
    }

    #[test]
    fn lookup_wavelet_beyond_end() {
        let contents = vec![8, 3, 8, 8, 1, 2, 3, 2, 8, 9, 3, 3, 6, 7, 0, 4, 8, 7, 3];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_iter(
            4,
            contents_closure.into_iter(),
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert!(wavelet_tree.lookup(100).is_none());
    }

    #[test]
    fn lookup_wavelet_with_just_one_char_type() {
        let contents = vec![5, 5, 5, 5, 5, 5, 5, 5, 5, 5];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_iter(
            4,
            contents_closure.into_iter(),
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert_eq!(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            wavelet_tree.lookup(5).unwrap().iter().collect::<Vec<_>>()
        );
        assert!(wavelet_tree.lookup(4).is_none());
        assert!(wavelet_tree.lookup(6).is_none());
    }

    #[test]
    fn wavelet_lookup_one() {
        let contents = vec![3, 6, 2, 1, 8, 5, 4, 7];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        block_on(build_wavelet_tree_from_iter(
            4,
            contents_closure.into_iter(),
            wavelet_bits_file.clone(),
            wavelet_blocks_file.clone(),
            wavelet_sblocks_file.clone(),
        ))
        .unwrap();

        let wavelet_bits = block_on(wavelet_bits_file.map()).unwrap();
        let wavelet_blocks = block_on(wavelet_blocks_file.map()).unwrap();
        let wavelet_sblocks = block_on(wavelet_sblocks_file.map()).unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert_eq!(Some(3), wavelet_tree.lookup_one(1));
        assert_eq!(Some(2), wavelet_tree.lookup_one(2));
        assert_eq!(Some(6), wavelet_tree.lookup_one(4));
        assert_eq!(Some(5), wavelet_tree.lookup_one(5));
        assert_eq!(Some(1), wavelet_tree.lookup_one(6));
        assert_eq!(Some(7), wavelet_tree.lookup_one(7));
        assert_eq!(Some(4), wavelet_tree.lookup_one(8));
    }
}
