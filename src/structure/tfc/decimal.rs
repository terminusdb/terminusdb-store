use bytes::Buf;
use rug::Integer;

use super::integer::{bigint_to_storage, storage_to_bigint_and_sign, NEGATIVE_ZERO};

fn encode_fraction(fraction: Option<&str>) -> Vec<u8> {
    if let Some(f) = fraction {
        if f.is_empty() {
            return vec![0x00]; // a "false zero" so we don't represent it at all.
        }
        let len = f.len();
        let size = len / 2 + usize::from(len % 2 != 0);
        let mut bcd = Vec::with_capacity(size);
        for i in 0..size {
            let last = if i * 2 + 2 > len {
                i * 2 + 1
            } else {
                i * 2 + 2
            };
            let two = &f[2 * i..last];
            let mut this_int = centary_decimal_encode(two);
            this_int <<= 1;
            if i != size - 1 {
                this_int |= 1 // add continuation bit.
            }
            bcd.push(this_int)
        }
        bcd
    } else {
        vec![0x00] // a "false zero" so we don't represent no fraction as a fraction
    }
}

fn centary_decimal_encode(s: &str) -> u8 {
    if s.len() == 1 {
        let i = s.parse::<u8>().unwrap();
        i * 11 + 1
    } else {
        let i = s.parse::<u8>().unwrap();
        let o = i / 10 + 1;
        i + o + 1
    }
}

fn centary_decimal_decode(i: u8) -> String {
    let j = i - 1;
    if j % 11 == 0 {
        let num = j / 11;
        format!("{num:}")
    } else {
        let d = j / 11;
        let num = j - d - 1;
        format!("{num:02}")
    }
}

pub fn decode_fraction<B: Buf>(fraction_buf: &mut B, is_pos: bool) -> String {
    let mut first_byte = fraction_buf.chunk()[0];
    if !is_pos {
        first_byte = !first_byte;
    }
    if first_byte == 0x00 {
        "".to_string()
    } else {
        let mut s = String::new();
        while fraction_buf.has_remaining() {
            let mut byte = fraction_buf.get_u8();
            if !is_pos {
                byte = !byte;
            }
            let num = byte >> 1;
            let res = centary_decimal_decode(num);
            s.push_str(&res);
            if res.len() == 1 || byte & 1 == 0 {
                break;
            }
        }
        s
    }
}

pub fn decimal_to_storage(decimal: &str) -> Vec<u8> {
    let mut parts = decimal.split('.');
    let bigint = parts.next().unwrap_or(decimal);
    let fraction = parts.next();
    let integer_part = bigint.parse::<Integer>().unwrap();
    let is_neg = decimal.starts_with('-');
    integer_and_fraction_to_storage(is_neg, integer_part, fraction)
}

pub fn storage_to_decimal<B: Buf>(bytes: &mut B) -> String {
    let (int, is_pos) = storage_to_bigint_and_sign(bytes);
    let fraction = decode_fraction(bytes, is_pos);
    let decimal = if fraction.is_empty() {
        format!("{int:}")
    } else {
        let sign = if int == 0 && !is_pos { "-" } else { "" };
        format!("{sign:}{int:}.{fraction:}")
    };
    decimal
}

pub fn integer_and_fraction_to_storage(
    is_neg: bool,
    integer: Integer,
    fraction: Option<&str>,
) -> Vec<u8> {
    let prefix = bigint_to_storage(integer.clone());
    let mut prefix = if integer == 0 && is_neg {
        vec![NEGATIVE_ZERO] // negative zero
    } else {
        prefix
    };
    let suffix = if is_neg {
        let mut suffix = encode_fraction(fraction);
        for i in 0..suffix.len() {
            suffix[i] = !suffix[i]
        }
        suffix
    } else {
        encode_fraction(fraction)
    };
    prefix.extend(suffix);
    prefix
}
