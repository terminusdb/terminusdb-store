use bytes::Buf;
use rug::Integer;

const TERMINAL: u8 = 0;
const FIRST_SIGN: u8 = 0b1000_0000u8;
const FIRST_TERMINAL: u8 = 0b0000_0000u8;
const CONTINUATION: u8 = 0b1000_0000u8;
const FIRST_CONTINUATION: u8 = 0b0100_0000u8;
const BASE_MASK: u8 = !CONTINUATION;
const FIRST_MASK: u8 = !(FIRST_SIGN | FIRST_CONTINUATION);
const FIRST_MAX: u8 = FIRST_CONTINUATION;
pub const NEGATIVE_ZERO: u8 = 0b0111_1111;

// Leave in reverse order for the convenience of the caller
fn size_encode(size: u32) -> Vec<u8> {
    if size == 0 {
        return vec![NEGATIVE_ZERO]; // just the positive sign bit (allows negative zero)
    }
    let mut remainder = size;
    let mut v = vec![];
    let mut last = true;
    while remainder > 0 {
        if remainder >= CONTINUATION as u32 {
            let continued = if last { TERMINAL } else { CONTINUATION };
            let byte = continued | ((remainder & BASE_MASK as u32) as u8);
            v.push(byte);
        } else if remainder >= FIRST_MAX as u32 {
            // special case where we fit in 7 bits but not 6
            // and we need a zero padded initial byte.
            let continued = if last { TERMINAL } else { CONTINUATION };
            let byte = continued | ((remainder & BASE_MASK as u32) as u8);
            v.push(byte);
            let byte = FIRST_SIGN | FIRST_CONTINUATION;
            v.push(byte)
        } else {
            let continued = if last {
                FIRST_TERMINAL
            } else {
                FIRST_CONTINUATION
            };
            let byte = FIRST_SIGN | continued | ((remainder & FIRST_MASK as u32) as u8);
            v.push(byte)
        }
        remainder >>= 7;
        last = false;
    }
    v
}

fn size_decode<B: Buf>(v: &mut B) -> (bool, u32, usize) {
    let mut size: u32 = 0;
    let mut sign = true;
    let mut i = 0;
    while v.has_remaining() {
        let vi = v.get_u8();
        if i == 0 {
            sign = vi & FIRST_SIGN != 0;
            let vi = if sign { vi } else { !vi };
            let val = (vi & FIRST_MASK) as u32;
            if vi & FIRST_CONTINUATION == 0 {
                return (sign, val, i + 1);
            } else {
                size += val
            }
        } else {
            let vi = if sign { vi } else { !vi };
            let val = (vi & BASE_MASK) as u32;
            if vi & CONTINUATION == 0 {
                return (sign, size + val, i + 1);
            } else {
                size += val
            }
        }
        size <<= 7;
        i += 1;
    }
    (sign, size, i)
}

pub fn bigint_to_storage(bigint: Integer) -> Vec<u8> {
    let is_neg = bigint < 0;
    let mut int = bigint.abs();
    let size = int.significant_bits() + 1;
    let num_bytes = (size / 8) + u32::from(size % 8 != 0);
    let size_bytes = size_encode(num_bytes);
    let mut number_vec = Vec::with_capacity(size_bytes.len() + num_bytes as usize + 1);
    for _ in 0..num_bytes {
        let byte = int.to_u8_wrapping();
        number_vec.push(byte);
        int >>= 8;
    }
    number_vec.extend(size_bytes);
    if is_neg {
        for e in number_vec.iter_mut() {
            *e = !*e;
        }
    }
    number_vec.reverse();
    number_vec
}

pub fn storage_to_bigint_and_sign<B: Buf>(bytes: &mut B) -> (Integer, bool) {
    let (is_pos, size, _) = size_decode(bytes);
    let mut int = Integer::new();
    if size == 0 {
        return (int, is_pos);
    }
    for _ in 0..size {
        int <<= 8;
        let b = bytes.get_u8();
        int += if is_pos { b } else { !b };
    }
    if !is_pos {
        int = -int;
    }
    (int, is_pos)
}

pub fn storage_to_bigint<B: Buf>(bytes: &mut B) -> Integer {
    storage_to_bigint_and_sign(bytes).0
}
