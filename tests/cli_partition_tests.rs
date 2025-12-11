use bqdrift::schema::{PartitionKey, PartitionType};
use chrono::{NaiveDate, NaiveDateTime};

#[test]
fn test_parse_day_partition() {
    let key = PartitionKey::parse("2024-06-15", &PartitionType::Day).unwrap();
    assert_eq!(key, PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()));
}

#[test]
fn test_parse_day_partition_invalid() {
    let err = PartitionKey::parse("not-a-date", &PartitionType::Day).unwrap_err();
    assert!(err.contains("Invalid day partition"));
}

#[test]
fn test_parse_hour_partition_full() {
    let key = PartitionKey::parse("2024-06-15T10:00:00", &PartitionType::Hour).unwrap();
    let expected = NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    assert_eq!(key, PartitionKey::Hour(expected));
}

#[test]
fn test_parse_hour_partition_short() {
    let key = PartitionKey::parse("2024-06-15T10", &PartitionType::Hour).unwrap();
    let expected = NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    assert_eq!(key, PartitionKey::Hour(expected));
}

#[test]
fn test_parse_hour_partition_invalid() {
    let err = PartitionKey::parse("2024-06-15", &PartitionType::Hour).unwrap_err();
    assert!(err.contains("Invalid hour partition"));
}

#[test]
fn test_parse_month_partition() {
    let key = PartitionKey::parse("2024-06", &PartitionType::Month).unwrap();
    assert_eq!(key, PartitionKey::Month { year: 2024, month: 6 });
}

#[test]
fn test_parse_month_partition_invalid_month() {
    let err = PartitionKey::parse("2024-13", &PartitionType::Month).unwrap_err();
    assert!(err.contains("Month must be 1-12"));
}

#[test]
fn test_parse_month_partition_invalid_format() {
    let err = PartitionKey::parse("2024/06", &PartitionType::Month).unwrap_err();
    assert!(err.contains("Invalid month partition"));
}

#[test]
fn test_parse_year_partition() {
    let key = PartitionKey::parse("2024", &PartitionType::Year).unwrap();
    assert_eq!(key, PartitionKey::Year(2024));
}

#[test]
fn test_parse_year_partition_invalid() {
    let err = PartitionKey::parse("abcd", &PartitionType::Year).unwrap_err();
    assert!(err.contains("Invalid year partition"));
}

#[test]
fn test_parse_range_partition() {
    let key = PartitionKey::parse("12345", &PartitionType::Range).unwrap();
    assert_eq!(key, PartitionKey::Range(12345));
}

#[test]
fn test_parse_range_partition_negative() {
    let key = PartitionKey::parse("-100", &PartitionType::Range).unwrap();
    assert_eq!(key, PartitionKey::Range(-100));
}

#[test]
fn test_parse_range_partition_invalid() {
    let err = PartitionKey::parse("abc", &PartitionType::Range).unwrap_err();
    assert!(err.contains("Invalid range partition"));
}

#[test]
fn test_partition_key_iteration_day() {
    let start = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 28).unwrap());
    let end = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 7, 2).unwrap());

    let mut partitions = Vec::new();
    let mut current = start.clone();
    while current <= end {
        partitions.push(current.clone());
        current = current.next();
    }

    assert_eq!(partitions.len(), 5);
    assert_eq!(partitions[0], start);
    assert_eq!(partitions[4], end);
}

#[test]
fn test_partition_key_iteration_month() {
    let start = PartitionKey::Month { year: 2024, month: 10 };
    let end = PartitionKey::Month { year: 2025, month: 2 };

    let mut partitions = Vec::new();
    let mut current = start.clone();
    while current <= end {
        partitions.push(current.clone());
        current = current.next();
    }

    assert_eq!(partitions.len(), 5);
    assert_eq!(partitions[0], PartitionKey::Month { year: 2024, month: 10 });
    assert_eq!(partitions[1], PartitionKey::Month { year: 2024, month: 11 });
    assert_eq!(partitions[2], PartitionKey::Month { year: 2024, month: 12 });
    assert_eq!(partitions[3], PartitionKey::Month { year: 2025, month: 1 });
    assert_eq!(partitions[4], PartitionKey::Month { year: 2025, month: 2 });
}

#[test]
fn test_partition_key_iteration_year() {
    let start = PartitionKey::Year(2022);
    let end = PartitionKey::Year(2025);

    let mut partitions = Vec::new();
    let mut current = start.clone();
    while current <= end {
        partitions.push(current.clone());
        current = current.next();
    }

    assert_eq!(partitions.len(), 4);
    assert_eq!(partitions[0], PartitionKey::Year(2022));
    assert_eq!(partitions[3], PartitionKey::Year(2025));
}

#[test]
fn test_partition_key_to_naive_date() {
    let hour = PartitionKey::Hour(NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
    let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    let month = PartitionKey::Month { year: 2024, month: 6 };
    let year = PartitionKey::Year(2024);

    assert_eq!(hour.to_naive_date(), NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    assert_eq!(day.to_naive_date(), NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    assert_eq!(month.to_naive_date(), NaiveDate::from_ymd_opt(2024, 6, 1).unwrap());
    assert_eq!(year.to_naive_date(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
}

#[test]
fn test_partition_key_decorator() {
    let hour = PartitionKey::Hour(NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
    let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    let month = PartitionKey::Month { year: 2024, month: 6 };
    let year = PartitionKey::Year(2024);
    let range = PartitionKey::Range(12345);

    assert_eq!(hour.decorator(), "$2024061510");
    assert_eq!(day.decorator(), "$20240615");
    assert_eq!(month.decorator(), "$202406");
    assert_eq!(year.decorator(), "$2024");
    assert_eq!(range.decorator(), "$12345");
}

#[test]
fn test_partition_key_sql_literal() {
    let hour = PartitionKey::Hour(NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
    let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    let month = PartitionKey::Month { year: 2024, month: 6 };
    let year = PartitionKey::Year(2024);
    let range = PartitionKey::Range(12345);

    assert_eq!(hour.sql_literal(), "TIMESTAMP '2024-06-15 10:00:00'");
    assert_eq!(day.sql_literal(), "DATE '2024-06-15'");
    assert_eq!(month.sql_literal(), "DATE '2024-06-01'");
    assert_eq!(year.sql_literal(), "DATE '2024-01-01'");
    assert_eq!(range.sql_literal(), "12345");
}

#[test]
fn test_partition_key_display() {
    let hour = PartitionKey::Hour(NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
    let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    let month = PartitionKey::Month { year: 2024, month: 6 };
    let year = PartitionKey::Year(2024);
    let range = PartitionKey::Range(12345);

    assert_eq!(format!("{}", hour), "2024-06-15T10");
    assert_eq!(format!("{}", day), "2024-06-15");
    assert_eq!(format!("{}", month), "2024-06");
    assert_eq!(format!("{}", year), "2024");
    assert_eq!(format!("{}", range), "12345");
}

#[test]
fn test_partition_key_type_detection() {
    let hour = PartitionKey::Hour(NaiveDateTime::parse_from_str("2024-06-15 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
    let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    let month = PartitionKey::Month { year: 2024, month: 6 };
    let year = PartitionKey::Year(2024);
    let range = PartitionKey::Range(12345);

    assert_eq!(hour.partition_type(), PartitionType::Hour);
    assert_eq!(day.partition_type(), PartitionType::Day);
    assert_eq!(month.partition_type(), PartitionType::Month);
    assert_eq!(year.partition_type(), PartitionType::Year);
    assert_eq!(range.partition_type(), PartitionType::Range);
}

#[test]
fn test_partition_key_ordering_cross_type() {
    let day1 = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 14).unwrap());
    let day2 = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
    let day3 = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 16).unwrap());

    assert!(day1 < day2);
    assert!(day2 < day3);
    assert!(day1 < day3);
    assert!(day2 > day1);
}

#[test]
fn test_partition_key_next_by_range() {
    let start = PartitionKey::Range(0);
    let next = start.next_by(100);
    assert_eq!(next, PartitionKey::Range(100));

    let next2 = next.next_by(100);
    assert_eq!(next2, PartitionKey::Range(200));
}

#[test]
fn test_ingestion_time_uses_day_parsing() {
    let key = PartitionKey::parse("2024-06-15", &PartitionType::IngestionTime).unwrap();
    assert_eq!(key, PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()));
}
