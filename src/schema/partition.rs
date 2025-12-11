use serde::{Deserialize, Serialize};
use chrono::{NaiveDate, NaiveDateTime};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum PartitionType {
    Hour,
    #[default]
    Day,
    Month,
    Year,
    Range,
    IngestionTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PartitionKey {
    Hour(NaiveDateTime),
    Day(NaiveDate),
    Month { year: i32, month: u32 },
    Year(i32),
    Range(i64),
}

impl PartitionKey {
    pub fn parse(s: &str, partition_type: &PartitionType) -> Result<Self, String> {
        match partition_type {
            PartitionType::Hour => {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                    Ok(PartitionKey::Hour(dt))
                } else if let Ok(dt) = NaiveDateTime::parse_from_str(&format!("{}:00:00", s), "%Y-%m-%dT%H:%M:%S") {
                    Ok(PartitionKey::Hour(dt))
                } else {
                    Err(format!("Invalid hour partition: '{}'. Expected format: YYYY-MM-DDTHH", s))
                }
            }
            PartitionType::Day | PartitionType::IngestionTime => {
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map(PartitionKey::Day)
                    .map_err(|_| format!("Invalid day partition: '{}'. Expected format: YYYY-MM-DD", s))
            }
            PartitionType::Month => {
                let parts: Vec<&str> = s.split('-').collect();
                if parts.len() == 2 {
                    let year = parts[0].parse::<i32>()
                        .map_err(|_| format!("Invalid year in month partition: '{}'", s))?;
                    let month = parts[1].parse::<u32>()
                        .map_err(|_| format!("Invalid month in month partition: '{}'", s))?;
                    if month >= 1 && month <= 12 {
                        Ok(PartitionKey::Month { year, month })
                    } else {
                        Err(format!("Month must be 1-12, got: {}", month))
                    }
                } else {
                    Err(format!("Invalid month partition: '{}'. Expected format: YYYY-MM", s))
                }
            }
            PartitionType::Year => {
                s.parse::<i32>()
                    .map(PartitionKey::Year)
                    .map_err(|_| format!("Invalid year partition: '{}'. Expected format: YYYY", s))
            }
            PartitionType::Range => {
                s.parse::<i64>()
                    .map(PartitionKey::Range)
                    .map_err(|_| format!("Invalid range partition: '{}'. Expected integer", s))
            }
        }
    }

    pub fn decorator(&self) -> String {
        match self {
            PartitionKey::Hour(dt) => format!("${}", dt.format("%Y%m%d%H")),
            PartitionKey::Day(d) => format!("${}", d.format("%Y%m%d")),
            PartitionKey::Month { year, month } => format!("${}{:02}", year, month),
            PartitionKey::Year(y) => format!("${}", y),
            PartitionKey::Range(n) => format!("${}", n),
        }
    }

    pub fn sql_literal(&self) -> String {
        match self {
            PartitionKey::Hour(dt) => format!("TIMESTAMP '{}'", dt.format("%Y-%m-%d %H:%M:%S")),
            PartitionKey::Day(d) => format!("DATE '{}'", d.format("%Y-%m-%d")),
            PartitionKey::Month { year, month } => format!("DATE '{}-{:02}-01'", year, month),
            PartitionKey::Year(y) => format!("DATE '{}-01-01'", y),
            PartitionKey::Range(n) => n.to_string(),
        }
    }

    pub fn sql_value(&self) -> String {
        match self {
            PartitionKey::Hour(dt) => format!("{}", dt.format("%Y-%m-%d %H:%M:%S")),
            PartitionKey::Day(d) => format!("{}", d.format("%Y-%m-%d")),
            PartitionKey::Month { year, month } => format!("{}-{:02}-01", year, month),
            PartitionKey::Year(y) => format!("{}-01-01", y),
            PartitionKey::Range(n) => n.to_string(),
        }
    }

    pub fn next(&self) -> Self {
        match self {
            PartitionKey::Hour(dt) => {
                PartitionKey::Hour(*dt + chrono::Duration::hours(1))
            }
            PartitionKey::Day(d) => {
                PartitionKey::Day(d.succ_opt().unwrap_or(*d))
            }
            PartitionKey::Month { year, month } => {
                if *month == 12 {
                    PartitionKey::Month { year: year + 1, month: 1 }
                } else {
                    PartitionKey::Month { year: *year, month: month + 1 }
                }
            }
            PartitionKey::Year(y) => PartitionKey::Year(y + 1),
            PartitionKey::Range(n) => PartitionKey::Range(n + 1),
        }
    }

    pub fn next_by(&self, interval: i64) -> Self {
        match self {
            PartitionKey::Range(n) => PartitionKey::Range(n + interval),
            _ => self.next(),
        }
    }

    pub fn to_naive_date(&self) -> NaiveDate {
        match self {
            PartitionKey::Hour(dt) => dt.date(),
            PartitionKey::Day(d) => *d,
            PartitionKey::Month { year, month } => {
                NaiveDate::from_ymd_opt(*year, *month, 1).unwrap_or_default()
            }
            PartitionKey::Year(y) => {
                NaiveDate::from_ymd_opt(*y, 1, 1).unwrap_or_default()
            }
            PartitionKey::Range(_) => NaiveDate::default(),
        }
    }

    pub fn partition_type(&self) -> PartitionType {
        match self {
            PartitionKey::Hour(_) => PartitionType::Hour,
            PartitionKey::Day(_) => PartitionType::Day,
            PartitionKey::Month { .. } => PartitionType::Month,
            PartitionKey::Year(_) => PartitionType::Year,
            PartitionKey::Range(_) => PartitionType::Range,
        }
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionKey::Hour(dt) => write!(f, "{}", dt.format("%Y-%m-%dT%H")),
            PartitionKey::Day(d) => write!(f, "{}", d.format("%Y-%m-%d")),
            PartitionKey::Month { year, month } => write!(f, "{}-{:02}", year, month),
            PartitionKey::Year(y) => write!(f, "{}", y),
            PartitionKey::Range(n) => write!(f, "{}", n),
        }
    }
}

impl PartialOrd for PartitionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (PartitionKey::Hour(a), PartitionKey::Hour(b)) => a.cmp(b),
            (PartitionKey::Day(a), PartitionKey::Day(b)) => a.cmp(b),
            (PartitionKey::Month { year: y1, month: m1 }, PartitionKey::Month { year: y2, month: m2 }) => {
                (y1, m1).cmp(&(y2, m2))
            }
            (PartitionKey::Year(a), PartitionKey::Year(b)) => a.cmp(b),
            (PartitionKey::Range(a), PartitionKey::Range(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl From<NaiveDate> for PartitionKey {
    fn from(date: NaiveDate) -> Self {
        PartitionKey::Day(date)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionConfig {
    #[serde(default)]
    pub field: Option<String>,
    #[serde(rename = "type", default)]
    pub partition_type: PartitionType,
    #[serde(default)]
    pub granularity: Option<PartitionType>,
    #[serde(default)]
    pub start: Option<i64>,
    #[serde(default)]
    pub end: Option<i64>,
    #[serde(default)]
    pub interval: Option<i64>,
}

impl PartitionConfig {
    pub fn day(field: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            partition_type: PartitionType::Day,
            granularity: None,
            start: None,
            end: None,
            interval: None,
        }
    }

    pub fn hour(field: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            partition_type: PartitionType::Hour,
            granularity: None,
            start: None,
            end: None,
            interval: None,
        }
    }

    pub fn month(field: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            partition_type: PartitionType::Month,
            granularity: None,
            start: None,
            end: None,
            interval: None,
        }
    }

    pub fn year(field: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            partition_type: PartitionType::Year,
            granularity: None,
            start: None,
            end: None,
            interval: None,
        }
    }

    pub fn range(field: impl Into<String>, start: i64, end: i64, interval: i64) -> Self {
        Self {
            field: Some(field.into()),
            partition_type: PartitionType::Range,
            granularity: None,
            start: Some(start),
            end: Some(end),
            interval: Some(interval),
        }
    }

    pub fn ingestion_time(granularity: PartitionType) -> Self {
        Self {
            field: None,
            partition_type: PartitionType::IngestionTime,
            granularity: Some(granularity),
            start: None,
            end: None,
            interval: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn test_partition_key_parse_day() {
        let key = PartitionKey::parse("2024-01-15", &PartitionType::Day).unwrap();
        assert_eq!(key, PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()));
    }

    #[test]
    fn test_partition_key_parse_hour() {
        let key = PartitionKey::parse("2024-01-15T10", &PartitionType::Hour).unwrap();
        if let PartitionKey::Hour(dt) = key {
            assert_eq!(dt.date(), NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
            assert_eq!(dt.hour(), 10);
        } else {
            panic!("Expected Hour partition");
        }
    }

    #[test]
    fn test_partition_key_parse_month() {
        let key = PartitionKey::parse("2024-03", &PartitionType::Month).unwrap();
        assert_eq!(key, PartitionKey::Month { year: 2024, month: 3 });
    }

    #[test]
    fn test_partition_key_parse_year() {
        let key = PartitionKey::parse("2024", &PartitionType::Year).unwrap();
        assert_eq!(key, PartitionKey::Year(2024));
    }

    #[test]
    fn test_partition_key_parse_range() {
        let key = PartitionKey::parse("1000", &PartitionType::Range).unwrap();
        assert_eq!(key, PartitionKey::Range(1000));
    }

    #[test]
    fn test_partition_key_decorator_day() {
        let key = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(key.decorator(), "$20240115");
    }

    #[test]
    fn test_partition_key_decorator_hour() {
        let dt = NaiveDateTime::parse_from_str("2024-01-15T10:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
        let key = PartitionKey::Hour(dt);
        assert_eq!(key.decorator(), "$2024011510");
    }

    #[test]
    fn test_partition_key_decorator_month() {
        let key = PartitionKey::Month { year: 2024, month: 3 };
        assert_eq!(key.decorator(), "$202403");
    }

    #[test]
    fn test_partition_key_decorator_year() {
        let key = PartitionKey::Year(2024);
        assert_eq!(key.decorator(), "$2024");
    }

    #[test]
    fn test_partition_key_decorator_range() {
        let key = PartitionKey::Range(1000);
        assert_eq!(key.decorator(), "$1000");
    }

    #[test]
    fn test_partition_key_sql_literal_day() {
        let key = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(key.sql_literal(), "DATE '2024-01-15'");
    }

    #[test]
    fn test_partition_key_sql_literal_hour() {
        let dt = NaiveDateTime::parse_from_str("2024-01-15T10:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
        let key = PartitionKey::Hour(dt);
        assert_eq!(key.sql_literal(), "TIMESTAMP '2024-01-15 10:00:00'");
    }

    #[test]
    fn test_partition_key_sql_literal_month() {
        let key = PartitionKey::Month { year: 2024, month: 3 };
        assert_eq!(key.sql_literal(), "DATE '2024-03-01'");
    }

    #[test]
    fn test_partition_key_next_day() {
        let key = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let next = key.next();
        assert_eq!(next, PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 16).unwrap()));
    }

    #[test]
    fn test_partition_key_next_hour() {
        let dt = NaiveDateTime::parse_from_str("2024-01-15T23:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
        let key = PartitionKey::Hour(dt);
        let next = key.next();
        if let PartitionKey::Hour(next_dt) = next {
            assert_eq!(next_dt.date(), NaiveDate::from_ymd_opt(2024, 1, 16).unwrap());
            assert_eq!(next_dt.hour(), 0);
        } else {
            panic!("Expected Hour partition");
        }
    }

    #[test]
    fn test_partition_key_next_month() {
        let key = PartitionKey::Month { year: 2024, month: 12 };
        let next = key.next();
        assert_eq!(next, PartitionKey::Month { year: 2025, month: 1 });
    }

    #[test]
    fn test_partition_key_next_year() {
        let key = PartitionKey::Year(2024);
        let next = key.next();
        assert_eq!(next, PartitionKey::Year(2025));
    }

    #[test]
    fn test_partition_key_next_range() {
        let key = PartitionKey::Range(1000);
        let next = key.next();
        assert_eq!(next, PartitionKey::Range(1001));
    }

    #[test]
    fn test_partition_key_next_by_range() {
        let key = PartitionKey::Range(0);
        let next = key.next_by(1000);
        assert_eq!(next, PartitionKey::Range(1000));
    }

    #[test]
    fn test_partition_key_ordering() {
        let key1 = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let key2 = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 16).unwrap());
        assert!(key1 < key2);
    }

    #[test]
    fn test_partition_key_display() {
        let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(format!("{}", day), "2024-01-15");

        let month = PartitionKey::Month { year: 2024, month: 3 };
        assert_eq!(format!("{}", month), "2024-03");

        let year = PartitionKey::Year(2024);
        assert_eq!(format!("{}", year), "2024");

        let range = PartitionKey::Range(1000);
        assert_eq!(format!("{}", range), "1000");
    }

    #[test]
    fn test_partition_key_to_naive_date() {
        let day = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(day.to_naive_date(), NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());

        let month = PartitionKey::Month { year: 2024, month: 3 };
        assert_eq!(month.to_naive_date(), NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());

        let year = PartitionKey::Year(2024);
        assert_eq!(year.to_naive_date(), NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }
}
