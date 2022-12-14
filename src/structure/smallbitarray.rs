#[derive(Clone)]
pub struct SmallBitArray {
    val: u64,
}

impl std::fmt::Debug for SmallBitArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{:b}>", self.val)
    }
}

impl SmallBitArray {
    pub const LEN: usize = u64::BITS as usize - 1;
    pub fn new(val: u64) -> Self {
        if val & 1 != 0 {
            panic!("lsb set for a small bit array. this is reserved for future expansion");
        }

        Self { val }
    }

    pub fn get(&self, index: usize) -> bool {
        if index >= Self::LEN {
            panic!("index too high");
        }

        (self.val >> (Self::LEN - index) & 1) != 0
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> {
        SmallBitArrayIter {
            val: self.val,
            ix: 0,
        }
    }

    pub fn inner(&self) -> u64 {
        self.val
    }

    pub fn rank1(&self, index: usize) -> usize {
        if index >= Self::LEN {
            panic!("index too high");
        }

        let mask = !(u64::MAX >> (index as u32 + 1));
        (self.val & mask).count_ones() as usize
    }
}

#[derive(Clone)]
pub struct SmallBitArrayIter {
    val: u64,
    ix: usize,
}

impl Iterator for SmallBitArrayIter {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        if self.ix >= SmallBitArray::LEN {
            return None;
        }

        let result = (self.val & 0x80000000_00000000) != 0;

        self.val <<= 1;
        self.ix += 1;

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[should_panic(
        expected = "lsb set for a small bit array. this is reserved for future expansion"
    )]
    fn panic_with_set_lsb() {
        let val: u64 = 0b01101011_10111001_10010010_00000111_10010001_01100101_00000000_11111111;

        let _x = SmallBitArray::new(val);
    }
    #[test]
    fn get_from_small_bit_array() {
        let val: u64 = 0b01101011_10111001_10010010_00000111_10010001_01100101_00000000_11111110;

        let arr = SmallBitArray::new(val);

        #[rustfmt::skip]
        let expecteds = [
            false, true, true, false, true, false, true, true,
            true, false, true, true, true, false, false, true,
            true, false, false, true, false, false, true, false,
            false, false, false, false, false, true, true, true,
            true, false, false, true, false, false, false, true,
            false, true, true, false, false, true, false, true,
            false, false, false, false, false, false, false, false,
            true, true, true, true, true, true, true
        ];

        for (ix, &expected) in expecteds.iter().enumerate() {
            assert_eq!(expected, arr.get(ix));
        }
    }
    #[test]

    fn iterate_small_bit_array() {
        let val: u64 = 0b01101011_10111001_10010010_00000111_10010001_01100101_00000000_11111110;

        let arr = SmallBitArray::new(val);

        #[rustfmt::skip]
        let expecteds = [
            false, true, true, false, true, false, true, true,
            true, false, true, true, true, false, false, true,
            true, false, false, true, false, false, true, false,
            false, false, false, false, false, true, true, true,
            true, false, false, true, false, false, false, true,
            false, true, true, false, false, true, false, true,
            false, false, false, false, false, false, false, false,
            true, true, true, true, true, true, true
        ];

        let iter = arr.iter();

        for (&expected, actual) in expecteds.iter().zip(iter) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn small_bit_array_rank() {
        let val: u64 = 0b01101011_10111001_10010010_00000111_10010001_01100101_00000000_11111110;

        let arr = SmallBitArray::new(val);

        #[rustfmt::skip]
        let expecteds = [
            0, 1, 2, 2, 3, 3, 4, 5,
            6, 6, 7, 8, 9, 9, 9, 10,
            11, 11, 11, 12, 12, 12, 13, 13,
            13, 13, 13, 13, 13, 14, 15, 16,
            17, 17, 17, 18, 18, 18, 18, 19,
            19, 20, 21, 21, 21, 22, 22, 23,
            23, 23, 23, 23, 23, 23, 23, 23,
            24, 25, 26, 27, 28, 29, 30
        ];

        for (ix, &expected) in expecteds.iter().enumerate() {
            let rank = arr.rank1(ix);
            assert_eq!(expected, rank);
        }
    }
}
