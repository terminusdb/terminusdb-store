use bytes::Buf;
use chrono::NaiveDateTime;
use rug::Integer;

use super::{
    decimal::{decode_fraction, integer_and_fraction_to_storage},
    integer::storage_to_bigint_and_sign,
};

pub fn datetime_to_parts(datetime: &NaiveDateTime) -> (bool, Integer, u32) {
    let mut seconds = Integer::from(datetime.timestamp());
    let is_neg = seconds < 0;
    let mut nanos = datetime.timestamp_subsec_nanos();
    if is_neg {
        if nanos != 0 {
            seconds += 1;
        }
        nanos = 1_000_000_000 - nanos;
    }
    (is_neg, seconds, nanos)
}

pub fn datetime_to_storage(datetime: &NaiveDateTime) -> Vec<u8> {
    let (is_neg, seconds, nanos) = datetime_to_parts(datetime);
    let fraction = if nanos == 0 {
        None
    } else if nanos % 1_000_000 == 0 {
        Some(format!("{:02}", nanos / 1_000_000))
    } else if nanos % 1_000 == 0 {
        Some(format!("{:05}", nanos / 1_000))
    } else {
        Some(format!("{nanos:08}"))
    };
    integer_and_fraction_to_storage(is_neg, seconds, fraction.as_ref().map(|b| b.as_ref()))
}

fn count_leading_zeros(string: &str) -> usize {
    string
        .chars()
        .take_while(|ch| *ch == '0')
        .map(|ch| ch.len_utf8())
        .sum()
}

pub fn storage_to_datetime<B: Buf>(bytes: &mut B) -> NaiveDateTime {
    let (int, is_pos) = storage_to_bigint_and_sign(bytes);
    let fraction = decode_fraction(bytes, is_pos);
    let seconds = int
        .to_i64()
        .expect("This is a surprisingly large number of seconds!");
    if fraction.is_empty() {
        if is_pos {
            NaiveDateTime::from_timestamp_opt(seconds, 0).unwrap()
        } else {
            NaiveDateTime::from_timestamp_opt(-seconds, 0).unwrap()
        }
    } else {
        let leading_zeros = count_leading_zeros(&fraction);
        let nanos = fraction
            .parse::<u32>()
            .expect("Nano seconds should actually fit in u32")
            * u32::pow(10, leading_zeros as u32);
        if is_pos {
            NaiveDateTime::from_timestamp_opt(seconds, nanos).unwrap()
        } else {
            NaiveDateTime::from_timestamp_opt(seconds - 1, 1_000_000_000 - nanos).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_few_nanos_before_epoch() {
        let dt = NaiveDateTime::from_timestamp_opt(-1, 234).unwrap();
        let result = datetime_to_parts(&dt);
        assert_eq!((true, Integer::from(0), 999999766_u32), result)
    }
}
