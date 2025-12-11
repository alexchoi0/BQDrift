use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::schema::PartitionKey;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    pub query_name: String,
    pub partition_date: NaiveDate,
    pub version: u32,
    pub sql_revision: Option<u32>,
    pub effective_from: NaiveDate,
    pub sql_checksum: String,
    pub schema_checksum: String,
    pub yaml_checksum: String,
    pub executed_sql_b64: Option<String>,
    pub upstream_states: HashMap<String, DateTime<Utc>>,
    pub executed_at: DateTime<Utc>,
    pub execution_time_ms: Option<i64>,
    pub rows_written: Option<i64>,
    pub bytes_processed: Option<i64>,
    pub status: ExecutionStatus,
}

impl PartitionState {
    pub fn partition_key(&self) -> PartitionKey {
        PartitionKey::Day(self.partition_date)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExecutionStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DriftState {
    Current,
    SqlChanged,
    SchemaChanged,
    VersionUpgraded,
    UpstreamChanged,
    NeverRun,
    Failed,
}

impl DriftState {
    pub fn as_str(&self) -> &'static str {
        match self {
            DriftState::Current => "current",
            DriftState::SqlChanged => "sql_changed",
            DriftState::SchemaChanged => "schema_changed",
            DriftState::VersionUpgraded => "version_upgraded",
            DriftState::UpstreamChanged => "upstream_changed",
            DriftState::NeverRun => "never_run",
            DriftState::Failed => "failed",
        }
    }

    pub fn needs_rerun(&self) -> bool {
        !matches!(self, DriftState::Current)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionDrift {
    pub query_name: String,
    pub partition_key: PartitionKey,
    pub state: DriftState,
    pub current_version: u32,
    pub executed_version: Option<u32>,
    pub caused_by: Option<String>,
    pub executed_sql_b64: Option<String>,
    pub current_sql: Option<String>,
}

impl PartitionDrift {
    pub fn partition_date(&self) -> NaiveDate {
        self.partition_key.to_naive_date()
    }
}

#[derive(Debug, Default)]
pub struct DriftReport {
    pub partitions: Vec<PartitionDrift>,
}

impl DriftReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, drift: PartitionDrift) {
        self.partitions.push(drift);
    }

    pub fn by_query(&self) -> HashMap<String, Vec<&PartitionDrift>> {
        let mut grouped: HashMap<String, Vec<&PartitionDrift>> = HashMap::new();
        for p in &self.partitions {
            grouped.entry(p.query_name.clone()).or_default().push(p);
        }
        grouped
    }

    pub fn by_state(&self) -> HashMap<DriftState, Vec<&PartitionDrift>> {
        let mut grouped: HashMap<DriftState, Vec<&PartitionDrift>> = HashMap::new();
        for p in &self.partitions {
            grouped.entry(p.state).or_default().push(p);
        }
        grouped
    }

    pub fn needs_rerun(&self) -> Vec<&PartitionDrift> {
        self.partitions.iter().filter(|p| p.state.needs_rerun()).collect()
    }

    pub fn is_current(&self) -> bool {
        self.partitions.iter().all(|p| p.state == DriftState::Current)
    }

    pub fn summary(&self) -> HashMap<DriftState, usize> {
        let mut counts: HashMap<DriftState, usize> = HashMap::new();
        for p in &self.partitions {
            *counts.entry(p.state).or_default() += 1;
        }
        counts
    }
}
