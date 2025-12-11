use chrono::NaiveDate;
use std::collections::HashMap;
use crate::error::Result;
use crate::dsl::QueryDef;
use crate::schema::PartitionKey;
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
        let current_sql = version.map(|v| v.get_sql_for_date(chrono::Utc::now().date_naive()).to_string());

        let (state, executed_version, caused_by, executed_sql_b64) = match (version, stored) {
            (None, _) => (DriftState::NeverRun, None, None, None),

            (Some(_), None) => (DriftState::NeverRun, None, None, None),

            (Some(v), Some(stored)) => {
                if stored.status == super::state::ExecutionStatus::Failed {
                    (DriftState::Failed, Some(stored.version), None, stored.executed_sql_b64.clone())
                } else {
                    let current_checksums = Checksums::from_version(
                        v,
                        yaml_content,
                        chrono::Utc::now().date_naive(),
                    );

                    if current_checksums.schema != stored.schema_checksum {
                        (DriftState::SchemaChanged, Some(stored.version), None, stored.executed_sql_b64.clone())
                    } else if current_checksums.sql != stored.sql_checksum {
                        (DriftState::SqlChanged, Some(stored.version), None, stored.executed_sql_b64.clone())
                    } else if v.version != stored.version {
                        (DriftState::VersionUpgraded, Some(stored.version), None, stored.executed_sql_b64.clone())
                    } else {
                        // TODO: Check upstream_changed
                        (DriftState::Current, Some(stored.version), None, stored.executed_sql_b64.clone())
                    }
                }
            }
        };

        PartitionDrift {
            query_name: query.name.clone(),
            partition_key: PartitionKey::Day(partition_date),
            state,
            current_version: version.map(|v| v.version).unwrap_or(0),
            executed_version,
            caused_by,
            executed_sql_b64,
            current_sql,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{VersionDef, Destination};
    use crate::schema::{Schema, PartitionConfig};
    use crate::drift::checksum::{Checksums, compress_to_base64};
    use crate::invariant::InvariantsDef;
    use chrono::{NaiveDate, Utc};
    use std::collections::HashSet;

    fn create_test_query(name: &str, sql_content: &str) -> QueryDef {
        QueryDef {
            name: name.to_string(),
            destination: Destination {
                dataset: "test_dataset".to_string(),
                table: "test_table".to_string(),
                partition: PartitionConfig::day("date"),
                cluster: None,
            },
            description: None,
            owner: None,
            tags: vec![],
            versions: vec![VersionDef {
                version: 1,
                effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                source: "test.sql".to_string(),
                sql_content: sql_content.to_string(),
                revisions: vec![],
                description: None,
                backfill_since: None,
                schema: Schema::default(),
                dependencies: HashSet::new(),
                invariants: InvariantsDef::default(),
            }],
            cluster: None,
        }
    }

    fn create_stored_state(
        query_name: &str,
        partition_date: NaiveDate,
        sql_content: &str,
        yaml_content: &str,
    ) -> PartitionState {
        let checksums = Checksums::compute(sql_content, &Schema::default(), yaml_content);
        PartitionState {
            query_name: query_name.to_string(),
            partition_date,
            version: 1,
            sql_revision: None,
            effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            sql_checksum: checksums.sql,
            schema_checksum: checksums.schema,
            yaml_checksum: checksums.yaml,
            executed_sql_b64: Some(compress_to_base64(sql_content)),
            upstream_states: HashMap::new(),
            executed_at: Utc::now(),
            execution_time_ms: Some(100),
            rows_written: Some(1000),
            bytes_processed: Some(10000),
            status: super::super::state::ExecutionStatus::Success,
        }
    }

    #[test]
    fn test_detect_never_run_has_current_sql() {
        let query = create_test_query("test_query", "SELECT * FROM source");
        let yaml_contents = HashMap::from([("test_query".to_string(), "name: test_query".to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let report = detector.detect(&[], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::NeverRun);
        assert!(drift.current_sql.is_some());
        assert!(drift.current_sql.as_ref().unwrap().contains("SELECT * FROM source"));
        assert!(drift.executed_sql_b64.is_none());
    }

    #[test]
    fn test_detect_current_preserves_executed_sql() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let stored = create_stored_state("test_query", date, sql, yaml);

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::Current);
        assert!(drift.current_sql.is_some());
        assert!(drift.executed_sql_b64.is_some());
    }

    #[test]
    fn test_detect_sql_changed_has_both_sqls() {
        let old_sql = "SELECT user_id FROM users";
        let new_sql = "SELECT COALESCE(user_id, 'anon') FROM users";
        let yaml = "name: test_query";

        let query = create_test_query("test_query", new_sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let stored = create_stored_state("test_query", date, old_sql, yaml);

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::SqlChanged);
        assert!(drift.current_sql.is_some());
        assert!(drift.current_sql.as_ref().unwrap().contains("COALESCE"));
        assert!(drift.executed_sql_b64.is_some());

        let executed = crate::diff::decode_sql(drift.executed_sql_b64.as_ref().unwrap());
        assert!(executed.is_none()); // executed_sql_b64 uses gzip compression, not plain base64
    }

    #[test]
    fn test_detect_sql_changed_executed_sql_decompresses() {
        let old_sql = "SELECT user_id FROM users";
        let new_sql = "SELECT COALESCE(user_id, 'anon') FROM users";
        let yaml = "name: test_query";

        let query = create_test_query("test_query", new_sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let stored = create_stored_state("test_query", date, old_sql, yaml);

        let report = detector.detect(&[stored], date, date).unwrap();

        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::SqlChanged);

        let executed = crate::drift::decompress_from_base64(drift.executed_sql_b64.as_ref().unwrap());
        assert!(executed.is_some());
        assert_eq!(executed.unwrap(), old_sql);
    }

    #[test]
    fn test_detect_failed_state_preserves_executed_sql() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let mut stored = create_stored_state("test_query", date, sql, yaml);
        stored.status = super::super::state::ExecutionStatus::Failed;

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::Failed);
        assert!(drift.executed_sql_b64.is_some());
    }

    #[test]
    fn test_detect_schema_changed_preserves_executed_sql() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let mut stored = create_stored_state("test_query", date, sql, yaml);
        stored.schema_checksum = "different_checksum".to_string();

        let report = detector.detect(&[stored], date, date).unwrap();

        assert_eq!(report.partitions.len(), 1);
        let drift = &report.partitions[0];
        assert_eq!(drift.state, DriftState::SchemaChanged);
        assert!(drift.executed_sql_b64.is_some());
        assert!(drift.current_sql.is_some());
    }

    #[test]
    fn test_detect_multiple_dates() {
        let sql = "SELECT * FROM source";
        let yaml = "name: test_query";
        let query = create_test_query("test_query", sql);
        let yaml_contents = HashMap::from([("test_query".to_string(), yaml.to_string())]);
        let detector = DriftDetector::new(vec![query], yaml_contents);

        let from = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();

        let report = detector.detect(&[], from, to).unwrap();

        assert_eq!(report.partitions.len(), 5);
        for drift in &report.partitions {
            assert_eq!(drift.state, DriftState::NeverRun);
            assert!(drift.current_sql.is_some());
        }
    }
}
