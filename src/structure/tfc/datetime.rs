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
    if is_neg && nanos != 0 {
        seconds += 1;
        nanos = 1_000_000_000 - nanos;
    }
    (is_neg, seconds, nanos)
}

pub fn datetime_to_storage(datetime: &NaiveDateTime) -> Vec<u8> {
    let (is_neg, seconds, nanos) = datetime_to_parts(datetime);
    let fraction = if nanos == 0 {
        None
    } else if nanos % 1_000_000 == 0 {
        Some(format!("{:03}", nanos / 1_000_000))
    } else if nanos % 1_000 == 0 {
        Some(format!("{:06}", nanos / 1_000))
    } else {
        Some(format!("{nanos:09}"))
    };
    integer_and_fraction_to_storage(is_neg, seconds, fraction.as_ref().map(|b| b.as_ref()))
}

pub fn storage_to_datetime<B: Buf>(bytes: &mut B) -> NaiveDateTime {
    let (int, is_pos) = storage_to_bigint_and_sign(bytes);
    let fraction = decode_fraction(bytes, is_pos);
    let seconds = int
        .to_i64()
        .expect("This is a surprisingly large number of seconds!");
    if fraction.is_empty() {
        NaiveDateTime::from_timestamp_opt(seconds, 0).unwrap()
    } else {
        let zeros = "0".repeat(9 - fraction.len());
        let fraction = format!("{fraction}{zeros}");
        let nanos = fraction
            .parse::<u32>()
            .expect("Nano seconds should actually fit in u32");
        if is_pos {
            NaiveDateTime::from_timestamp_opt(seconds, nanos).unwrap()
        } else {
            NaiveDateTime::from_timestamp_opt(seconds - 1, 1_000_000_000 - nanos).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn a_few_nanos_before_epoch() {
        let dt = NaiveDateTime::from_timestamp_opt(-1, 234).unwrap();
        let result = datetime_to_parts(&dt);
        assert_eq!((true, Integer::from(0), 999999766_u32), result)
    }

    #[test]
    fn a_few_ms() {
        let year: i32 = 2002;
        let month: u32 = 11;
        let day: u32 = 4;
        let hour: u32 = 11;
        let minute: u32 = 30;
        let second: u32 = 12;
        let nano: u32 = 333_000_000;
        let dt = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_nano_opt(hour, minute, second, nano)
            .unwrap();
        let result = datetime_to_parts(&dt);
        assert_eq!((false, Integer::from(1036409412), 333_000_000_u32), result);
        let storage = datetime_to_storage(&dt);
        let dt_storage = storage_to_datetime(&mut storage.as_slice());
        assert_eq!(dt, dt_storage)
    }

    #[test]
    fn a_few_ns() {
        let year: i32 = 2002;
        let month: u32 = 11;
        let day: u32 = 4;
        let hour: u32 = 11;
        let minute: u32 = 30;
        let second: u32 = 12;
        let nano: u32 = 333;
        let dt = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_nano_opt(hour, minute, second, nano)
            .unwrap();
        let result = datetime_to_parts(&dt);
        assert_eq!((false, Integer::from(1036409412), 333_u32), result);
        let storage = datetime_to_storage(&dt);
        let dt_storage = storage_to_datetime(&mut storage.as_slice());
        assert_eq!(dt, dt_storage)
    }

    #[test]
    fn no_time_ns() {
        let year: i32 = 1959;
        let month: u32 = 10;
        let day: u32 = 11;
        let hour: u32 = 0;
        let minute: u32 = 0;
        let second: u32 = 0;
        let nano: u32 = 0;
        let dt = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_nano_opt(hour, minute, second, nano)
            .unwrap();
        let result = datetime_to_parts(&dt);
        assert_eq!((true, Integer::from(-322704000), 0_u32), result);
        let storage = datetime_to_storage(&dt);
        let dt_storage = storage_to_datetime(&mut storage.as_slice());
        assert_eq!(dt, dt_storage)
    }

    #[test]
    fn a_bit_of_ns() {
        let year: i32 = 1959;
        let month: u32 = 10;
        let day: u32 = 11;
        let hour: u32 = 0;
        let minute: u32 = 0;
        let second: u32 = 0;
        let nano: u32 = 3;
        let dt = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_nano_opt(hour, minute, second, nano)
            .unwrap();
        let result = datetime_to_parts(&dt);
        assert_eq!((true, Integer::from(-322703999), 999999997_u32), result);
        let storage = datetime_to_storage(&dt);
        let dt_storage = storage_to_datetime(&mut storage.as_slice());
        assert_eq!(dt, dt_storage)
    }
}
