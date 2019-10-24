//! A succinct data structure for quick lookup of entry positions in a sequence.

use futures::prelude::*;
use tokio::prelude::*;
use super::logarray::*;
use super::bitarray::*;
use super::bitindex::*;
use crate::storage::*;

/// A wavelet tree, encoding a u64 array for fast lookup of number positions.
///
/// A wavelet tree consists of a layer of bitarrays (stored as one big
/// bitarray). The amount of layers is the log2 of the alphabet size,
/// rounded up to make it an integer. Since we're encoding u64 values,
/// the number of layers can never be larger than 64.
#[derive(Clone)]
pub struct WaveletTree<M:AsRef<[u8]>+Clone> {
    bits: BitIndex<M>,
    num_layers: u8
}

/// A lookup for all positions of a particular entry.
///
/// This struct caches part of the calculation required to get
/// positions out of a wavelet tree, allowing for quick iteration over
/// all positions for a given entry.
#[derive(Clone)]
pub struct WaveletLookup<M:AsRef<[u8]>+Clone> {
    /// the entry this lookup was created for.
    pub entry: u64,
    tree: WaveletTree<M>,
    slices: Vec<(bool,u64,u64)>
}

impl<M:AsRef<[u8]>+Clone> WaveletLookup<M> {
    /// Returns the amount of positions found in this lookup.
    pub fn len(&self) -> usize {
        let (b, start, end) = *self.slices.last().unwrap();

        if b {
            self.tree.bits.rank1_from_range(start, end) as usize
        }
        else {
            self.tree.bits.rank0_from_range(start, end) as usize
        }
    }

    /// Returns the position of the index'th entry of this lookup
    pub fn entry(&self, index: usize) -> u64 {
        if index >= self.len() {
            panic!("entry is out of bounds");
        }
        
        let mut result = (index+1) as u64;
        for &(b, start_index, end_index) in self.slices.iter().rev() {
            if b {
                result = self.tree.bits.select1_from_range(result, start_index, end_index).unwrap() - start_index + 1;
            }
            else {
                result = self.tree.bits.select0_from_range(result, start_index, end_index).unwrap() - start_index + 1;
            }
        }

        result - 1
    }

    /// Returns an Iterator over all positions for the entry of this lookup
    pub fn iter(&self) -> impl Iterator<Item=u64> {
        let cloned = self.clone();
        (0..self.len()).map(move |i|cloned.entry(i))
    }
}

impl<M:AsRef<[u8]>+Clone> WaveletTree<M> {
    /// Construct a wavelet tree from a bitindex and a layer count.
    pub fn from_parts(bits: BitIndex<M>, num_layers: u8) -> WaveletTree<M> {
        if num_layers != 0 && bits.len() % num_layers as usize != 0 {
            panic!("the bitarray length is not a multiple of the number of layers");
        }

        WaveletTree { bits, num_layers }
    }

    /// Returns the length of the encoded array.
    pub fn len(&self) -> usize {
        if self.num_layers == 0 {
            0
        }
        else {
            self.bits.len() / self.num_layers as usize
        }
    }

    /// Returns the amount of layers.
    pub fn num_layers(&self) -> usize {
        self.num_layers as usize
    }

    /// Decode the wavelet tree to the original u64 sequence. This returns an iterator.
    pub fn decode(&self) -> impl Iterator<Item=u64> {
        let owned = self.clone();
        (0..self.len()).map(move |i|owned.decode_one(i))
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
            let index = i*len + range_start + offset;
            if index as usize >= self.bits.len() {
                panic!("inner loop reached an index that is too high");
            }
            let bit = self.bits.get(index);

            let range_start_index = i * len + range_start;
            let range_end_index = i * len + range_end;
            if bit {
                alphabet_start = (alphabet_start+alphabet_end) / 2;
                offset = self.bits.rank1_from_range(range_start_index, index+1) - 1;

                let zeros_in_range = self.bits.rank0_from_range(range_start_index, range_end_index);
                range_start += zeros_in_range;
            }
            else {
                alphabet_end = (alphabet_start+alphabet_end) / 2;
                offset = self.bits.rank0_from_range(range_start_index, index+1) - 1;

                let ones_in_range = self.bits.rank1_from_range(range_start_index, range_end_index);
                range_end -= ones_in_range;
            }
        }

        assert!(alphabet_start == alphabet_end - 1);

        alphabet_start
    }

    /// Lookup the given entry. This returns a `WaveletLookup` which can then be used to find all positions.
    pub fn lookup(&self, entry: u64) -> Option<WaveletLookup<M>> {
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
            let full_start_index = (i as u64)*width+start_index;
            let full_end_index = (i as u64)*width+end_index;
            let b = entry >= (alphabet_start + alphabet_end)/2;
            slices.push((b, full_start_index, full_end_index));
            if b {
                alphabet_start += 2_u64.pow((self.num_layers - i - 1) as u32);
                start_index += self.bits.rank0_from_range(full_start_index, full_end_index);
            }
            else {
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
            tree: self.clone()
        })
    }
}

fn build_wavelet_fragment<S:Stream<Item=u64,Error=std::io::Error>+Send, W:AsyncWrite+Send>(stream: S, write: BitArrayFileBuilder<W>, alphabet: usize, layer: usize, fragment: usize) -> impl Future<Item=BitArrayFileBuilder<W>,Error=std::io::Error>+Send {
    let step = (alphabet / 2_usize.pow(layer as u32)) as u64;
    let alphabet_start = step * fragment as u64;
    let alphabet_end = step * (fragment+1) as u64;
    let alphabet_mid = ((alphabet_start+alphabet_end)/2) as u64;

    stream.fold(write, move |w, num| {
        let result: Box<dyn Future<Item=BitArrayFileBuilder<W>,Error=std::io::Error>+Send> =
        if num >= alphabet_start && num < alphabet_end {
            Box::new(w.push(num >= alphabet_mid))
        }
        else {
            Box::new(future::ok(w))
        };

        result
    })
}

/// Build a wavelet tree from the given width and source stream constructor.
///
/// Source stream constructor is a function that returns a new stream
/// on demand. The wavelet tree constructor will iterate over this
/// stream multiple times, which is why we need a constructor function
/// here rather than just the stream itself.
pub fn build_wavelet_tree_from_stream<SFn: FnMut()->S+Send, S: Stream<Item=u64,Error=std::io::Error>+Send, F: 'static+FileLoad+FileStore>(width: u8, mut source: SFn, destination_bits: F, destination_blocks: F, destination_sblocks: F) -> impl Future<Item=(),Error=std::io::Error>+Send {
    let alphabet_size = 2_usize.pow(width as u32);
    let bits = BitArrayFileBuilder::new(destination_bits.open_write());
    stream::iter_ok::<_,std::io::Error>((0..width as usize)
                                        .map(|layer| (0..2_usize.pow(layer as u32))
                                             .map(move |fragment| (layer, fragment)))
                                        .flatten())
        .fold(bits, move |b, (layer, fragment)| {
            let stream = source();
            build_wavelet_fragment(stream, b, alphabet_size, layer, fragment)
        })
        .and_then(|b| b.finalize())
        .and_then(move |_| build_bitindex(destination_bits.open_read(), destination_blocks.open_write(), destination_sblocks.open_write()))
        .map(|_|())
}

/// Build a wavelet tree from a file storing a logarray.
pub fn build_wavelet_tree_from_logarray<FLoad: 'static+FileLoad+Clone, F: 'static+FileLoad+FileStore>(source: FLoad, destination_bits: F, destination_blocks: F, destination_sblocks: F) -> impl Future<Item=(),Error=std::io::Error>+Send {
    logarray_file_get_length_and_width(&source)
        .and_then(|(_, width)| build_wavelet_tree_from_stream(width,
                                                              move ||logarray_stream_entries(source.clone()),
                                                              destination_bits,
                                                              destination_blocks,
                                                              destination_sblocks))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;

    #[test]
    fn generate_and_decode_wavelet_tree_from_vec() {
        let contents = vec![21,1,30,13,23,21,3,0,21,21,12,11];
        let contents_closure = contents.clone();
        let contents_len = contents.len();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        build_wavelet_tree_from_stream(5, move ||stream::iter_ok(contents_closure.clone()), wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 5);

        assert_eq!(contents_len, wavelet_tree.len());

        assert_eq!(contents, wavelet_tree.decode().collect::<Vec<_>>());
    }

    #[test]
    fn generate_and_decode_wavelet_tree_from_logarray() {
        let logarray_file = MemoryBackedStore::new();
        let logarray_builder = LogArrayFileBuilder::new(logarray_file.open_write(), 5);
        let contents = vec![21,1,30,13,23,21,3,0,21,21,12,11];
        let contents_len = contents.len();
        logarray_builder.push_all(stream::iter_ok(contents.clone()))
            .and_then(|b|b.finalize())
            .wait().unwrap();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        build_wavelet_tree_from_logarray(logarray_file, wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 5);

        assert_eq!(contents_len, wavelet_tree.len());

        assert_eq!(contents, wavelet_tree.decode().collect::<Vec<_>>());
    }

    #[test]
    fn slice_wavelet_tree() {
        let contents = vec![8,3,8,8,1,2,3,2,8,9,3,3,6,7,0,4,8,7,3];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        build_wavelet_tree_from_stream(4, move ||stream::iter_ok(contents_closure.clone()), wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

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

        build_wavelet_tree_from_stream(4, move ||stream::iter_ok(contents_closure.clone()), wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert!(wavelet_tree.lookup(3).is_none());
    }

    #[test]
    fn lookup_wavelet_byeond_end() {
        let contents = vec![8,3,8,8,1,2,3,2,8,9,3,3,6,7,0,4,8,7,3];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        build_wavelet_tree_from_stream(4, move ||stream::iter_ok(contents_closure.clone()), wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert!(wavelet_tree.lookup(100).is_none());
    }

    #[test]
    fn lookup_wavelet_with_just_one_char_type() {
        let contents = vec![5,5,5,5,5,5,5,5,5,5];
        let contents_closure = contents.clone();

        let wavelet_bits_file = MemoryBackedStore::new();
        let wavelet_blocks_file = MemoryBackedStore::new();
        let wavelet_sblocks_file = MemoryBackedStore::new();

        build_wavelet_tree_from_stream(4, move ||stream::iter_ok(contents_closure.clone()), wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

        let wavelet_bitindex = BitIndex::from_maps(wavelet_bits, wavelet_blocks, wavelet_sblocks);
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 4);

        assert_eq!(vec![0,1,2,3,4,5,6,7,8,9], wavelet_tree.lookup(5).unwrap().iter().collect::<Vec<_>>());
        assert!(wavelet_tree.lookup(4).is_none());
        assert!(wavelet_tree.lookup(6).is_none());
    }
}
