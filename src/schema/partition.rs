use serde::{Deserialize, Serialize};

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
