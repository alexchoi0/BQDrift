use chrono::NaiveDate;
use std::collections::HashMap;
use crate::error::Result;
use crate::dsl::QueryDef;
use super::checksum::Checksums;
use super::state::{PartitionState, DriftState, DriftReport, PartitionDrift};

pub struct DriftDetector {
    queries: HashMap<String, QueryDef>,
    yaml_contents: HashMap<String, String>,
}

impl DriftDetector {
    pub fn new(queries: Vec<QueryDef>, yaml_contents: HashMap<String, String>) -> Self {
        let queries = queries.into_iter().map(|q| (q.name.clone(), q)).collect();
        Self { queries, yaml_contents }
    }

    pub fn detect(
        &self,
        stored_states: &[PartitionState],
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<DriftReport> {
        let mut report = DriftReport::new();

        let stored_map: HashMap<(String, NaiveDate), &PartitionState> = stored_states
            .iter()
            .map(|s| ((s.query_name.clone(), s.partition_date), s))
            .collect();

        for (query_name, query) in &self.queries {
            let yaml_content = self.yaml_contents.get(query_name).map(|s| s.as_str()).unwrap_or("");

            let mut current = from;
            while current <= to {
                let drift = self.detect_partition(
                    query,
                    current,
                    stored_map.get(&(query_name.clone(), current)),
                    yaml_content,
                );
                report.add(drift);
                current = current.succ_opt().unwrap_or(current);
            }
        }

        Ok(report)
    }

    fn detect_partition(
        &self,
        query: &QueryDef,
        partition_date: NaiveDate,
        stored: Option<&&PartitionState>,
        yaml_content: &str,
    ) -> PartitionDrift {
        let version = query.get_version_for_date(partition_date);

        let (state, executed_version, caused_by) = match (version, stored) {
            (None, _) => (DriftState::NeverRun, None, None),

            (Some(_), None) => (DriftState::NeverRun, None, None),

            (Some(v), Some(stored)) => {
                if stored.status == super::state::ExecutionStatus::Failed {
                    (DriftState::Failed, Some(stored.version), None)
                } else {
                    let current_checksums = Checksums::from_version(
                        v,
                        yaml_content,
                        chrono::Utc::now().date_naive(),
                    );

                    if current_checksums.schema != stored.schema_checksum {
                        (DriftState::SchemaChanged, Some(stored.version), None)
                    } else if current_checksums.sql != stored.sql_checksum {
                        (DriftState::SqlChanged, Some(stored.version), None)
                    } else if v.version != stored.version {
                        (DriftState::VersionUpgraded, Some(stored.version), None)
                    } else {
                        // TODO: Check upstream_changed
                        (DriftState::Current, Some(stored.version), None)
                    }
                }
            }
        };

        PartitionDrift {
            query_name: query.name.clone(),
            partition_date,
            state,
            current_version: version.map(|v| v.version).unwrap_or(0),
            executed_version,
            caused_by,
        }
    }

    /// Check if any upstream dependency was re-run after this partition
    /// Returns the name of the upstream query that changed, if any
    pub fn detect_upstream_changed(
        &self,
        _query: &QueryDef,
        stored: &PartitionState,
        all_states: &[PartitionState],
    ) -> Option<String> {
        // Check each upstream dependency recorded in the state
        for (upstream_name, recorded_time) in &stored.upstream_states {
            // Find the latest execution of the upstream query for this partition date
            let upstream_latest = all_states
                .iter()
                .filter(|s| &s.query_name == upstream_name && s.partition_date == stored.partition_date)
                .max_by_key(|s| s.executed_at);

            if let Some(upstream) = upstream_latest {
                // If upstream ran after we recorded it, we're stale
                if upstream.executed_at > *recorded_time {
                    return Some(upstream_name.clone());
                }
            }
        }
        None
    }
}
