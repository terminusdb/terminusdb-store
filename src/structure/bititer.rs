//! Bit iterator for `u64`

/// An efficient bit iterator from msb to lsb for `u64`.
///
/// For each bit, starting from the msb and ending with the lsb, if the bit is `1`, the iterator
/// produces `true`, and if the bit is `0`, the iterator produces `false`.
pub(crate) struct BitIter {
    /// The value whose bits are iterated through.
    value: u64,

    /// A bit mask used to select a single bit for every iteration.
    ///
    /// Loop invariant: The mask will always have at most 1 bit set.
    mask: u64,
}

impl BitIter {
    /// Create a `BitIter` from a `u64`.
    pub(crate) const fn new(value: u64) -> Self {
        BitIter {
            value,
            // Initialize the mask to select the msb.
            mask: 0x8000_0000_0000_0000,
        }
    }
}

impl Iterator for BitIter {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        let mask = self.mask;
        // Check if the masked bit has been shifted past the lsb.
        if mask != 0 {
            // We're not done, yet. Shift the mask for the next iteration and return the currently
            // selected bit as a `bool`.
            self.mask >>= 1;
            // `true` if the bit selected by the mask is `1`, `false` if `0`.
            Some(self.value & mask != 0)
        } else {
            // We're done iterating.
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn zero() {
        for b in BitIter::new(0) {
            assert!(!b);
        }
    }

    #[test]
    pub fn max() {
        for b in BitIter::new(u64::max_value()) {
            assert!(b);
        }
    }

    #[test]
    pub fn alternating() {
        let mut expected = true;
        for b in BitIter::new(0xaaaa_aaaa_aaaa_aaaa) {
            assert_eq!(expected, b);
            expected = !expected;
        }
    }
}
