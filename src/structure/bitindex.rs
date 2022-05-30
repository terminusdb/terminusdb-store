//! Logic for building and using an index over a bitarray which provides rank and select.
use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;

use super::bitarray::*;
use super::logarray::*;

use crate::storage::SyncableFile;

use futures::io;
use futures::stream::StreamExt;
use tokio::io::AsyncRead;

// a block is 64 bit, which is the register size on modern architectures
// Block size is not tunable, and therefore no const is defined here.

/// The amount of 64-bit blocks that go into a superblock.
const SBLOCK_SIZE: usize = 52;

/// A bitarray with an index, supporting rank and select queries.
#[derive(Clone)]
pub struct BitIndex {
    array: BitArray,
    blocks: LogArray,
    sblocks: LogArray,
}

impl BitIndex {
    pub fn from_maps(bitarray_map: Bytes, blocks_map: Bytes, sblocks_map: Bytes) -> BitIndex {
        let bitarray = BitArray::from_bits(bitarray_map).unwrap();
        let blocks_logarray = LogArray::parse(blocks_map).unwrap();
        let sblocks_logarray = LogArray::parse(sblocks_map).unwrap();

        BitIndex::from_parts(bitarray, blocks_logarray, sblocks_logarray)
    }

    pub fn from_parts(array: BitArray, blocks: LogArray, sblocks: LogArray) -> BitIndex {
        assert!(sblocks.len() == (blocks.len() + SBLOCK_SIZE - 1) / SBLOCK_SIZE);
        assert!(blocks.len() == (array.len() + 63) / 64);

        BitIndex {
            array,
            blocks,
            sblocks,
        }
    }

    fn block_bits(&self, block_index: usize) -> &[u8] {
        let bit_index = block_index * 8;

        &self.array.bits()[bit_index..bit_index + 8]
    }

    /// Returns the length of the underlying bitarray.
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns the bit at the given index.
    pub fn get(&self, index: u64) -> bool {
        self.array.get(index as usize)
    }

    /// Returns the amount of 1-bits in the bitarray up to and including the given index.
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

    /// Returns the amount of 1-bits in the given range (up to but excluding end).
    pub fn rank1_from_range(&self, start: u64, end: u64) -> u64 {
        if start == end {
            return 0;
        }
        let mut rank = self.rank1(end - 1);
        if start != 0 {
            rank -= self.rank1(start - 1);
        }

        rank
    }

    fn select1_sblock_from_range(&self, rank: u64, start: u64, end: Option<u64>) -> usize {
        let mut start = start as usize / (64 * SBLOCK_SIZE);
        let mut end = match end {
            Some(end) => end as usize / (64 * SBLOCK_SIZE),
            None => self.sblocks.len() - 1,
        };
        let mut mid;

        loop {
            mid = (start + end) / 2;
            if start == end {
                break;
            }

            let r = self.sblocks.entry(mid);
            match r < rank {
                true => start = mid + 1,
                false => end = mid,
            }
        }

        mid
    }

    fn select1_block(&self, sblock: usize, subrank: u64) -> usize {
        let mut start = sblock * SBLOCK_SIZE;
        let mut end = start + SBLOCK_SIZE - 1;
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
            mid = (start + end + 1) / 2;
            if start == end {
                break;
            }

            let r = self.blocks.entry(mid);
            match r > subrank {
                true => start = mid,
                false => end = mid - 1,
            }
        }

        mid
    }

    /// Returns the index of the 1-bit in the bitarray corresponding with the given rank.
    pub fn select1(&self, rank: u64) -> Option<u64> {
        self.select1_from_range_opt(rank, 0, None)
    }

    pub fn select1_from_range(&self, subrank: u64, start: u64, end: u64) -> Option<u64> {
        self.select1_from_range_opt(subrank, start, Some(end))
    }

    fn select1_from_range_opt(&self, subrank: u64, start: u64, end: Option<u64>) -> Option<u64> {
        let rank = match start {
            0 => subrank,
            n => self.rank1(n - 1) + subrank,
        };
        let sblock = self.select1_sblock_from_range(rank, start, end);
        let sblock_rank = self.sblocks.entry(sblock);
        if sblock_rank < rank {
            return None;
        }

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
                    let result = block as u64 * 64 + i;
                    if result < start
                        && (end.is_none() || start < end.unwrap())
                        && subrank == 0
                        && !self.get(start)
                    {
                        return Some(start);
                    } else if result < start || (end.is_some() && result >= end.unwrap()) {
                        return None;
                    }
                    return Some(result);
                }
            }

            bits_num <<= 1;
        }

        None
    }

    /// Returns the amount of 0-bits in the bitarray up to and including the given index.
    pub fn rank0(&self, index: u64) -> u64 {
        let r0 = self.rank1(index);
        1 + index - r0
    }

    /// Returns the amount of 0-bits in the given range (up to but excluding end).
    pub fn rank0_from_range(&self, start: u64, end: u64) -> u64 {
        if start == end {
            return 0;
        }
        let mut rank = self.rank0(end - 1);
        if start != 0 {
            rank -= self.rank0(start - 1);
        }

        rank
    }

    fn select0_sblock_from_range(&self, rank: u64, start: u64, end: Option<u64>) -> usize {
        let mut start = start as usize / (64 * SBLOCK_SIZE);
        let mut end = match end {
            Some(end) => end as usize / (64 * SBLOCK_SIZE),
            None => self.sblocks.len() - 1,
        };
        let mut mid;

        loop {
            mid = (start + end) / 2;
            if start == end {
                break;
            }

            let r = ((1 + mid) * SBLOCK_SIZE) as u64 * 64 - self.sblocks.entry(mid);
            match r < rank {
                true => start = mid + 1,
                false => end = mid,
            }
        }

        mid
    }

    fn select0_block(&self, sblock: usize, subrank: u64) -> usize {
        let mut start = sblock * SBLOCK_SIZE;
        let mut end = start + SBLOCK_SIZE - 1;
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
            mid = (start + end + 1) / 2;
            if start == end {
                break;
            }

            let r = (SBLOCK_SIZE - mid % SBLOCK_SIZE) as u64 * 64 - self.blocks.entry(mid);
            match r > subrank {
                true => start = mid,
                false => end = mid - 1,
            }
        }

        mid
    }

    /// Returns the index of the 0-bit in the bitarray corresponding with the given rank.
    pub fn select0(&self, rank: u64) -> Option<u64> {
        self.select0_from_range_opt(rank, 0, None)
    }

    pub fn select0_from_range(&self, subrank: u64, start: u64, end: u64) -> Option<u64> {
        self.select0_from_range_opt(subrank, start, Some(end))
    }

    pub fn select0_from_range_opt(
        &self,
        subrank: u64,
        start: u64,
        end: Option<u64>,
    ) -> Option<u64> {
        let rank = match start {
            0 => subrank,
            n => self.rank0(n - 1) + subrank,
        };
        let sblock = self.select0_sblock_from_range(rank, start, end);
        let sblock_rank = ((1 + sblock) * SBLOCK_SIZE * 64) as u64 - self.sblocks.entry(sblock);

        if sblock_rank < rank {
            return None;
        }

        let block = self.select0_block(sblock, sblock_rank - rank);
        let block_subrank =
            (SBLOCK_SIZE - block % SBLOCK_SIZE) as u64 * 64 - self.blocks.entry(block);
        let rank_in_block = rank - (sblock_rank - block_subrank);
        assert!(rank_in_block <= 64);
        let bits = self.block_bits(block);

        let mut bits_num = BigEndian::read_u64(bits);
        let mut tally = rank_in_block;
        for i in 0..64 {
            if bits_num & 0x8000000000000000 == 0 {
                tally -= 1;

                if tally == 0 {
                    let result = block as u64 * 64 + i;
                    if result < start
                        && (end.is_none() || start < end.unwrap())
                        && subrank == 0
                        && self.get(start)
                    {
                        return Some(start);
                    } else if result < start || (end.is_some() && result >= end.unwrap()) {
                        return None;
                    }
                    return Some(result);
                }
            }

            bits_num <<= 1;
        }

        None
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> {
        self.array.iter()
    }
}

pub async fn build_bitindex<
    R: 'static + AsyncRead + Unpin + Send,
    W1: 'static + SyncableFile + Send,
    W2: 'static + SyncableFile + Send,
>(
    bitarray: R,
    blocks: W1,
    sblocks: W2,
) -> io::Result<()> {
    let block_stream = bitarray_stream_blocks(bitarray);
    // the following widths are unoptimized, but should always be large enough
    let mut blocks_builder =
        LogArrayFileBuilder::new(blocks, 64 - (SBLOCK_SIZE * 64).leading_zeros() as u8);
    let mut sblocks_builder = LogArrayFileBuilder::new(sblocks, 64);

    // we chunk block_stream into blocks of SBLOCK size for further processing
    let mut sblock_rank = 0;
    let mut stream = block_stream.chunks(SBLOCK_SIZE);
    while let Some(chunk) = stream.next().await {
        let mut block_ranks = Vec::with_capacity(chunk.len());
        for num in chunk {
            block_ranks.push(num?.count_ones() as u64);
        }

        let mut sblock_subrank = block_ranks.iter().sum();
        sblock_rank += sblock_subrank;

        for block_rank in block_ranks {
            blocks_builder.push(sblock_subrank).await?;
            sblock_subrank -= block_rank;
        }

        sblocks_builder.push(sblock_rank).await?;
    }

    blocks_builder.finalize().await?;
    sblocks_builder.finalize().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::*;
    use crate::storage::*;
    use crate::structure::util::stream_iter_ok;
    use futures::executor::block_on;

    #[tokio::test]
    async fn rank1_works() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        for i in 0..123456 {
            assert_eq!(i / 3 + 1, index.rank1(i));
        }
    }

    #[tokio::test]
    async fn select1_works() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        for i in 1..(123456 / 3) {
            assert_eq!((i - 1) * 3, index.select1(i).unwrap());
        }

        assert!(index.select1(123456 * 2 / 3).is_none());
    }

    #[tokio::test]
    async fn rank1_ranged() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        assert_eq!(0, index.rank1_from_range(6, 6));
        assert_eq!(1, index.rank1_from_range(6, 7));
        assert_eq!(1, index.rank1_from_range(6, 8));
        assert_eq!(2, index.rank1_from_range(6, 12));
        assert_eq!(2, index.rank1_from_range(4, 12));
    }

    #[tokio::test]
    async fn select1_ranged() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        assert_eq!(None, index.select1_from_range(0, 6, 6));
        assert_eq!(None, index.select1_from_range(0, 6, 7));
        assert_eq!(Some(6), index.select1_from_range(1, 6, 7));
        assert_eq!(Some(7), index.select1_from_range(0, 7, 8));
        assert_eq!(Some(9), index.select1_from_range(2, 5, 11));
        assert_eq!(None, index.select1_from_range(123456, 5, 10));
    }

    #[tokio::test]
    async fn rank0_works() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        for i in 0..123456 {
            assert_eq!(1 + i - (i / 3 + 1), index.rank0(i));
        }
    }

    #[tokio::test]
    async fn select0_works() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        for i in 1..=(123456 * 2 / 3) {
            assert_eq!(i + (i - 1) / 2, index.select0(i).unwrap());
        }

        assert_eq!(None, index.select0(123456 * 2 / 3 + 1));
    }

    #[tokio::test]
    async fn rank0_ranged() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        assert_eq!(0, index.rank0_from_range(5, 5));
        assert_eq!(1, index.rank0_from_range(5, 6));
        assert_eq!(0, index.rank0_from_range(6, 6));
        assert_eq!(2, index.rank0_from_range(5, 8));
        assert_eq!(4, index.rank0_from_range(6, 12));
        assert_eq!(6, index.rank0_from_range(4, 12));
    }

    #[tokio::test]
    async fn select0_ranged() {
        let bits = MemoryBackedStore::new();
        let mut ba_builder = BitArrayFileBuilder::new(bits.open_write().await.unwrap());
        let contents = (0..).map(|n| n % 3 == 0).take(123456);

        block_on(async {
            ba_builder.push_all(stream_iter_ok(contents)).await?;
            ba_builder.finalize().await?;

            Ok::<_, io::Error>(())
        })
        .unwrap();

        let index_blocks = MemoryBackedStore::new();
        let index_sblocks = MemoryBackedStore::new();
        block_on(build_bitindex(
            bits.open_read().await.unwrap(),
            index_blocks.open_write().await.unwrap(),
            index_sblocks.open_write().await.unwrap(),
        ))
        .unwrap();

        let index = BitIndex::from_maps(
            block_on(bits.map()).unwrap(),
            block_on(index_blocks.map()).unwrap(),
            block_on(index_sblocks.map()).unwrap(),
        );

        assert_eq!(None, index.select0_from_range(0, 6, 6));
        assert_eq!(Some(6), index.select0_from_range(0, 6, 7));
        assert_eq!(None, index.select0_from_range(1, 6, 7));
        assert_eq!(Some(7), index.select0_from_range(1, 6, 8));
        assert_eq!(None, index.select0_from_range(0, 7, 8));
        assert_eq!(Some(10), index.select0_from_range(4, 5, 11));
        assert_eq!(None, index.select0_from_range(123456, 5, 10));
    }
}
