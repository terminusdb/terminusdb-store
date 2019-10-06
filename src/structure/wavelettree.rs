use futures::prelude::*;
use tokio::prelude::*;
use super::logarray::*;
use super::bitarray::*;
use super::bitindex::*;
use crate::storage::*;

#[derive(Clone)]
pub struct WaveletTree<M:AsRef<[u8]>+Clone> {
    bits: BitIndex<M>,
    num_layers: usize
}

impl<M:AsRef<[u8]>+Clone> WaveletTree<M> {
    pub fn from_parts(bits: BitIndex<M>, num_layers: usize) -> WaveletTree<M> {
        assert!(num_layers != 0);
        if bits.len() % num_layers != 0 {
            panic!("the bitarray length is not a multiple of the number of layers");
        }

        WaveletTree { bits, num_layers }
    }

    pub fn len(&self) -> usize {
        self.bits.len() / self.num_layers
    }

    pub fn num_layers(&self) -> usize {
        self.num_layers
    }

    pub fn decode(&self) -> Vec<u64> {
        let owned = self.clone();
        (0..self.len()).map(move |i|owned.decode_one(i)).collect()
    }

    pub fn decode_one(&self, index: usize) -> u64 {
        println!("decode index {}", index);
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
                offset = self.bits.rank1_range(range_start_index, index+1) - 1;

                let zeros_in_range = self.bits.rank0_range(range_start_index, range_end_index);
                range_start += zeros_in_range;
            }
            else {
                alphabet_end = (alphabet_start+alphabet_end) / 2;
                offset = self.bits.rank0_range(range_start_index, index+1) - 1;

                let ones_in_range = self.bits.rank1_range(range_start_index, range_end_index);
                range_end -= ones_in_range;
            }
        }

        assert!(alphabet_start == alphabet_end - 1);

        alphabet_start
    }
}

fn build_wavelet_fragment<S:Stream<Item=u64,Error=std::io::Error>, W:AsyncWrite+Send+Sync>(stream: S, write: BitArrayFileBuilder<W>, alphabet: usize, layer: usize, fragment: usize) -> impl Future<Item=BitArrayFileBuilder<W>,Error=std::io::Error> {
    let step = (alphabet / 2_usize.pow(layer as u32)) as u64;
    let alphabet_start = step * fragment as u64;
    let alphabet_end = step * (fragment+1) as u64;
    let alphabet_mid = ((alphabet_start+alphabet_end)/2) as u64;

    stream.fold(write, move |w, num| {
        let result: Box<dyn Future<Item=BitArrayFileBuilder<W>,Error=std::io::Error>> =
        if num >= alphabet_start && num < alphabet_end {
            Box::new(w.push(num >= alphabet_mid))
        }
        else {
            Box::new(future::ok(w))
        };

        result
    })
}

pub fn build_wavelet_tree<FLoad: 'static+FileLoad+Clone, F1: 'static+FileLoad+FileStore, F2: 'static+FileStore, F3: 'static+FileStore>(source: FLoad, destination_bits: F1, destination_blocks: F2, destination_sblocks: F3) -> impl Future<Item=(),Error=std::io::Error> {
    let bits = BitArrayFileBuilder::new(destination_bits.open_write());

    logarray_file_get_length_and_width(&source)
        .map(|(_, width)| (width as usize, 2_usize.pow(width as u32)))
        .and_then(|(num_layers, alphabet_size)| stream::iter_ok::<_,std::io::Error>((0..num_layers)
                                                                                    .map(|layer| (0..2_usize.pow(layer as u32))
                                                                                         .map(move |fragment| (layer, fragment)))
                                                                                    .flatten())
                  .fold(bits, move |b, (layer, fragment)| {
                      let stream = logarray_stream_entries(source.clone());
                      build_wavelet_fragment(stream, b, alphabet_size, layer, fragment)
                  })
                  .and_then(|b| b.finalize())
                  .and_then(move |_| build_bitindex(destination_bits.open_read(), destination_blocks.open_write(), destination_sblocks.open_write()))
                  .map(|_|()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generate_and_decode_wavelet_tree() {
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

        build_wavelet_tree(logarray_file, wavelet_bits_file.clone(), wavelet_blocks_file.clone(), wavelet_sblocks_file.clone())
            .wait()
            .unwrap();

        let wavelet_bits = wavelet_bits_file.map().wait().unwrap();
        let wavelet_blocks = wavelet_blocks_file.map().wait().unwrap();
        let wavelet_sblocks = wavelet_sblocks_file.map().wait().unwrap();

        let wavelet_bitindex = BitIndex::from_parts(BitArray::from_bits(&wavelet_bits), LogArray::parse(&wavelet_blocks).unwrap(), LogArray::parse(&wavelet_sblocks).unwrap());
        let wavelet_tree = WaveletTree::from_parts(wavelet_bitindex, 5);

        assert_eq!(contents_len, wavelet_tree.len());

        assert_eq!(contents, wavelet_tree.decode());
    }
}
