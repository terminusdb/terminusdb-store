use bytes::Buf;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rug::Integer;

use super::{
    decimal::{decode_fraction, integer_and_fraction_to_storage},
    integer::storage_to_bigint_and_sign,
    BigDateTime, DateTime,
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

pub fn datetime_to_storage(datetime: &DateTime) -> Vec<u8> {
    match datetime {
        DateTime::Naive(datetime) => {
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
        DateTime::Big(datetime) => {
            let is_leap = leapyear(&datetime.year);
            let leap = if is_leap { 1 } else { 0 };
            let year_seconds = datetime.year.clone() * 3600 * 24 * (365 + leap);
            let false_year = if is_leap { 4 } else { 1 };
            let dt = NaiveDate::from_ymd_opt(false_year, datetime.month, datetime.day)
                .unwrap()
                .and_hms_nano_opt(
                    datetime.hour,
                    datetime.minute,
                    datetime.second,
                    datetime.nano,
                )
                .unwrap();
            let (is_neg, seconds, nanos) = datetime_to_parts(&dt);

            let fraction = if nanos == 0 {
                None
            } else if nanos % 1_000_000 == 0 {
                Some(format!("{:03}", nanos / 1_000_000))
            } else if nanos % 1_000 == 0 {
                Some(format!("{:06}", nanos / 1_000))
            } else {
                Some(format!("{nanos:09}"))
            };

            integer_and_fraction_to_storage(
                is_neg,
                year_seconds + seconds,
                fraction.as_ref().map(|b| b.as_ref()),
            )
        }
    }
}

fn leapyear(year: &Integer) -> bool {
    (year.clone() % 4 == 0 || year.clone() % 100 != 0) || year.clone() % 400 == 0
}

pub fn storage_to_datetime<B: Buf>(bytes: &mut B) -> DateTime {
    let (int, is_pos) = storage_to_bigint_and_sign(bytes);
    let fraction = decode_fraction(bytes, is_pos);

    let seconds: DateTime = int
        .to_i64()
        .and_then(|seconds| {
            if fraction.is_empty() {
                if is_pos {
                    NaiveDateTime::from_timestamp_opt(seconds, 0).map(DateTime::Naive)
                } else {
                    NaiveDateTime::from_timestamp_opt(-seconds, 0).map(DateTime::Naive)
                }
            } else {
                let zeros = "0".repeat(9 - fraction.len());
                let fraction = format!("{fraction}{zeros}");
                let nanos = fraction
                    .parse::<u32>()
                    .expect("Nano seconds should actually fit in u32");
                if is_pos {
                    NaiveDateTime::from_timestamp_opt(seconds, nanos).map(DateTime::Naive)
                } else {
                    NaiveDateTime::from_timestamp_opt(seconds - 1, 1_000_000_000 - nanos)
                        .map(DateTime::Naive)
                }
            }
        })
        .unwrap_or_else(move || {
            let minimal_seconds: i64 = (int.clone() % (3600 * 24 * 365 as i64)).to_i64().unwrap();
            let sign = if is_pos { 1 } else { -1 };
            let year = (sign * int) - minimal_seconds;
            let zeros = "0".repeat(9 - fraction.len());
            let fraction = format!("{fraction}{zeros}");
            let nanos = fraction
                .parse::<u32>()
                .expect("Nano seconds should actually fit in u32");
            let dt: NaiveDateTime = if is_pos {
                NaiveDateTime::from_timestamp_opt(minimal_seconds, nanos)
            } else {
                NaiveDateTime::from_timestamp_opt(minimal_seconds - 1, 1_000_000_000 - nanos)
            }
            .unwrap();
            DateTime::Big(BigDateTime {
                year,
                month: dt.month(),
                day: dt.day(),
                hour: dt.hour(),
                minute: dt.minute(),
                second: dt.second(),
                nano: dt.nanosecond(),
            })
        });
    seconds
}

pub fn format_datetime(dt: &DateTime) -> String {
    match dt {
        DateTime::Naive(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(),
        DateTime::Big(dt) => {
            let is_leap = leapyear(&dt.year);
            let false_year = if is_leap { 4 } else { 1 };
            let ndt = NaiveDate::from_ymd_opt(false_year, dt.month, dt.day)
                .unwrap()
                .and_hms_nano_opt(dt.hour, dt.minute, dt.second, dt.nano)
                .unwrap();
            let yearless = ndt.format("%m-%dT%H:%M:%S%.fZ").to_string();
            format!("{}-{yearless}", dt.year)
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
}
