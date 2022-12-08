use chrono::{DateTime, Utc};
use rug::Integer;

use super::decimal::integer_and_fraction_to_storage;

fn datetime_to_parts(datetime: &DateTime<Utc>) -> (bool, Integer, u32) {
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

fn datetime_to_storage(datetime: &DateTime<Utc>) -> Vec<u8> {
    let (is_neg, seconds, nanos) = datetime_to_parts(datetime);
    let fraction = if nanos == 0 {
        None
    } else if nanos % 1_000_000 == 0 {
        Some(format!("{:02}", nanos / 1_000_000))
    } else if nanos % 1_000 == 0 {
        Some(format!("{:05}", nanos / 1_000))
    } else {
        Some(format!("{:08}", nanos))
    };
    integer_and_fraction_to_storage(is_neg, seconds, fraction.as_ref().map(|b| b.as_ref()))
}

/*
fn storage_to_datetime<B: Buf>(bytes: &mut B) -> DateTime<Utc> {
    let (int, is_pos) = storage_to_bigint_and_sign(bytes);
    let fraction = u32::parse(decode_fraction(bytes, is_pos));
    Utc.timestamp(int) + .opt_with_nanoseconds(fraction)
}
*/

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn a_few_nanos_before_epoch() {
        let dt = Utc.timestamp_opt(-1, 234).unwrap();
        let result = dbg!(datetime_to_parts(&dt));
        assert_eq!((true, Integer::from(0), 999999766_u32), result)
    }
}
