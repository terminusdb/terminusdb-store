use byteorder::{ByteOrder,BigEndian};

use super::bitarray::*;
use super::logarray::*;
use futures::prelude::*;
use tokio::prelude::*;

// a block is 64 bit, which is the register size on modern architectures
// Block size is not tunable, and therefore no const is defined here.

/// The amount of 64-bit blocks that go into a superblock.
const SBLOCK_SIZE: usize = 52;

#[derive(Clone)]
pub struct BitIndex<M:AsRef<[u8]>+Clone> {
    array: BitArray<M>,
    blocks: LogArray<M>,
    sblocks: LogArray<M>
}

impl<M:AsRef<[u8]>+Clone> BitIndex<M> {
    pub fn from_parts(array: BitArray<M>, blocks: LogArray<M>, sblocks: LogArray<M>) -> BitIndex<M> {
        assert!(sblocks.len() == (blocks.len() + SBLOCK_SIZE - 1) / SBLOCK_SIZE);
        assert!(blocks.len() == (array.len() + 63) / 64);

        BitIndex {
            array, blocks, sblocks
        }
    }

    fn block_bits(&self, block_index: usize) -> &[u8] {
        let bit_index = block_index * 8;

        &self.array.bits()[bit_index..bit_index+8]
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn get(&self, index: u64) -> bool {
        self.array.get(index as usize)
    }

    pub fn rank1(&self, index: u64) -> u64 {
        let block_index = index / 64;
        let sblock_index = block_index / SBLOCK_SIZE as u64;

        let block_rank = self.blocks.entry(block_index as usize);
        let sblock_rank = self.sblocks.entry(sblock_index as usize);
        let bits = self.block_bits(block_index as usize);
        assert!(bits.len() == 8);

        let mut bits_num = BigEndian::read_u64(bits);
        bits_num >>= 63 - index % 64; // shift out numbers we don't care about
        let bits_rank = bits_num.count_ones() as u64;

        sblock_rank - block_rank + bits_rank
    }

    pub fn rank1_range(&self, start: u64, end: u64) -> u64 {
        let mut rank = self.rank1(end-1);
        if start != 0 {
            rank -= self.rank1(start-1);
        }

        rank
    }

    fn select1_sblock(&self, rank: u64) -> usize {
        let mut start = 0;
        let mut end = self.sblocks.len()-1;
        let mut mid;


        loop {
            mid = (start + end)/2;
            if start == end {
                break;
            }

            let r = self.sblocks.entry(mid);
            match r < rank {
                true => start = mid + 1,
                false => end = mid
            }
        }

        mid
    }

    fn select1_block(&self, sblock: usize, subrank: u64) -> usize {
        let mut start = sblock * SBLOCK_SIZE;
        let mut end = start + SBLOCK_SIZE-1;
        if end > self.blocks.len() - 1 {
            end = self.blocks.len() - 1;
        }
        let mut mid;

        // inside a superblock, block subranks cache superblock_rank - sum_i<block_(blockrank_i).
        // Or another way to think of this, each block subrank specifies where in the superblock
        // this block starts. if a superblock has a rank of 1000, and the first block has a rank of 50,
        // the second block will have a subrank of 1000-50=950.
        // Suppose the second block has a rank of 20, then the third block will have a subrank of 950-20=930.
        //
        // To find the proper block, we're trying to find the rightmost block with a subrank greater than the
        // subrank we're looking for.
        loop {
            mid = (start + end + 1)/2;
            if start == end {
                break;
            }

            let r = self.blocks.entry(mid);
            match r > subrank {
                true => start = mid,
                false => end = mid - 1
            }
        }

        mid
    }

    pub fn select1(&self, rank: u64) -> u64 {
        let sblock = self.select1_sblock(rank);
        let sblock_rank = self.sblocks.entry(sblock);
        let block = self.select1_block(sblock, sblock_rank - rank);
        let block_subrank = self.blocks.entry(block);
        let rank_in_block = rank - (sblock_rank - block_subrank);
        assert!(rank_in_block <= 64);
        let bits = self.block_bits(block);

        let mut bits_num = BigEndian::read_u64(bits);
        let mut tally = rank_in_block;
        for i in 0..64 {
            if bits_num & 0x8000000000000000 != 0 {
                tally -= 1;

                if tally == 0 {
                    return block as u64 * 64 + i;
                }
            }

            bits_num <<= 1;
        }

        panic!("reached end of select function without a result");
    }

    pub fn select1_from_range(&self, subrank: u64, start: u64, _end: u64) -> u64 {
        // todo this is a dumb implementation. we can actually do a much faster select by making sblock/block lookup ranged. for now this will work.
        let rank_offset = if start == 0 { 0 } else { self.rank1(start-1) };

        self.select1(rank_offset + subrank)
    }

    pub fn rank0(&self, index: u64) -> u64 {
        let r0 = self.rank1(index);
        1+index - r0
    }

    pub fn rank0_range(&self, start: u64, end: u64) -> u64 {
        let mut rank = self.rank0(end-1);
        if start != 0 {
            rank -= self.rank0(start-1);
        }

        rank
    }

    fn select0_sblock(&self, rank: u64) -> usize {
        let mut start = 0;
        let mut end = self.sblocks.len()-1;
        let mut mid;


        loop {
            mid = (start + end)/2;
            if start == end {
                break;
            }

            let r = ((1+mid) * SBLOCK_SIZE) as u64 * 64 - self.sblocks.entry(mid);
            match r < rank {
                true => start = mid + 1,
                false => end = mid
            }
        }

        mid
    }

    fn select0_block(&self, sblock: usize, subrank: u64) -> usize {
        let mut start = sblock * SBLOCK_SIZE;
        let mut end = start + SBLOCK_SIZE-1;
        if end > self.blocks.len() - 1 {
            end = self.blocks.len() - 1;
        }

        let mut mid;

        // inside a superblock, block subranks cache superblock_rank - sum_i<block_(blockrank_i).
        // Or another way to think of this, each block subrank specifies where in the superblock
        // this block starts. if a superblock has a rank of 1000, and the first block has a rank of 50,
        // the second block will have a subrank of 1000-50=950.
        // Suppose the second block has a rank of 20, then the third block will have a subrank of 950-20=930.
        //
        // To find the proper block, we're trying to find the rightmost block with a subrank greater than the
        // subrank we're looking for.
        loop {
            mid = (start + end + 1)/2;
            if start == end {
                break;
            }

            let r = (SBLOCK_SIZE - mid%SBLOCK_SIZE) as u64 * 64 - self.blocks.entry(mid);
            match r > subrank {
                true => start = mid,
                false => end = mid - 1
            }
        }

        mid
    }

    pub fn select0(&self, rank: u64) -> u64 {
        let sblock = self.select0_sblock(rank);
        let sblock_rank = ((1+sblock)*SBLOCK_SIZE*64) as u64 - self.sblocks.entry(sblock);
        let block = self.select0_block(sblock, sblock_rank - rank);
        let block_subrank = (SBLOCK_SIZE-block%SBLOCK_SIZE) as u64 *64 - self.blocks.entry(block);
        let rank_in_block = rank - (sblock_rank - block_subrank);
        assert!(rank_in_block <= 64);
        let bits = self.block_bits(block);

        let mut bits_num = BigEndian::read_u64(bits);
        let mut tally = rank_in_block;
        for i in 0..64 {
            if bits_num & 0x8000000000000000 == 0 {
                tally -= 1;

                if tally == 0 {
                    return block as u64 * 64 + i;
                }
            }

            bits_num <<= 1;
        }

        panic!("reached end of select function without a result");
    }

    pub fn select0_from_range(&self, subrank: u64, start: u64, _end: u64) -> u64 {
        // todo this is a dumb implementation. we can actually do a much faster select by making sblock/block lookup ranged. for now this will work.
        let rank_offset = if start == 0 { 0 } else { self.rank0(start-1) };

        self.select0(rank_offset + subrank)
    }
}

pub fn build_bitindex<R:'static+AsyncRead+Send+Sync,W1:'static+AsyncWrite+Send+Sync, W2:'static+AsyncWrite+Send+Sync>(bitarray:R, blocks:W1, sblocks:W2) -> Box<dyn Future<Item=(W1, W2),Error=std::io::Error>+Send+Sync> {
    let block_stream = bitarray_stream_blocks(bitarray);
    // the following widths are unoptimized, but should always be large enough
    let blocks_builder = LogArrayFileBuilder::new(blocks, 64-(SBLOCK_SIZE*64).leading_zeros() as u8);
    let sblocks_builder = LogArrayFileBuilder::new(sblocks, 64);
    // we chunk block_stream into blocks of SBLOCK size for further processing
    Box::new(block_stream.chunks(SBLOCK_SIZE)
             .fold((sblocks_builder, blocks_builder, 0), |(sblocks_builder, blocks_builder, tally), chunk| {
                 let block_ranks: Vec<u8> = chunk.iter().map(|b| b.count_ones() as u8).collect();
                 let sblock_subrank = block_ranks.iter().fold(0u64, |s,&i| s+i as u64);
                 let sblock_rank = sblock_subrank + tally;
                 stream::iter_ok(block_ranks)
                     .fold((blocks_builder, sblock_subrank),
                           |(builder, rank), block_rank|
                           builder.push(rank as u64)
                           .map(move |blocks_builder| (blocks_builder, rank - block_rank as u64)))
                     .and_then(move |(blocks_builder, _)|
                               sblocks_builder.push(sblock_rank)
                               .map(move |sblocks_builder| (sblocks_builder, blocks_builder, sblock_rank)))
             })
             .and_then(|(sblocks_builder, blocks_builder, _)| blocks_builder.finalize()
                       .and_then(|blocks_file| sblocks_builder.finalize()
                                 .map(move |sblocks_file| (blocks_file, sblocks_file))))) // TODO it would be better to return the various streams here. However, we have no access to block_stream as it was consumed.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::*;
    #[test]
    pub fn rank_and_select_work() {
        let bits = MemoryBackedStore::new();
        let ba_builder = BitArrayFileBuilder::new(bits.open_write());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);
        ba_builder.push_all(stream::iter_ok(contents))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        build_bitindex(bits.open_read(), index_blocks.open_write(), index_sblocks.open_write())
            .wait()
            .unwrap();

        let ba = BitArray::from_bits(bits.map().wait().unwrap());
        let blocks_logarray = LogArray::parse(index_blocks.map().wait().unwrap()).unwrap();
        let sblocks_logarray = LogArray::parse(index_sblocks.map().wait().unwrap()).unwrap();

        let index = BitIndex::from_parts(ba, blocks_logarray, sblocks_logarray);

        for i in 0..123456 {
            assert_eq!(i/3 + 1, index.rank1(i));
        }

        for i in 1..(123456/3) {
            assert_eq!((i-1)*3,index.select1(i));
        }
    }

    #[test]
    pub fn rank0_and_select0_work() {
        let bits = MemoryBackedStore::new();
        let ba_builder = BitArrayFileBuilder::new(bits.open_write());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);
        ba_builder.push_all(stream::iter_ok(contents))
            .and_then(|b|b.finalize())
            .wait()
            .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        build_bitindex(bits.open_read(), index_blocks.open_write(), index_sblocks.open_write())
            .wait()
            .unwrap();

        let ba = BitArray::from_bits(bits.map().wait().unwrap());
        let blocks_logarray = LogArray::parse(index_blocks.map().wait().unwrap()).unwrap();
        let sblocks_logarray = LogArray::parse(index_sblocks.map().wait().unwrap()).unwrap();

        let index = BitIndex::from_parts(ba, blocks_logarray, sblocks_logarray);

        for i in 0..123456 {
            assert_eq!(1+i - (i/3 + 1), index.rank0(i));
        }

        for i in 1..(123456*2/3) {
            assert_eq!(i + (i - 1) / 2,index.select0(i));
        }
    }
}
