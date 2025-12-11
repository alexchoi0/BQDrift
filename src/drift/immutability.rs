use std::collections::HashMap;
use crate::dsl::QueryDef;
use super::state::PartitionState;
use super::checksum::decompress_from_base64;

#[derive(Debug, Clone)]
pub struct ImmutabilityViolation {
    pub query_name: String,
    pub version: u32,
    pub revision: Option<u32>,
    pub source: String,
    pub affected_partitions: Vec<chrono::NaiveDate>,
    pub stored_sql: String,
    pub current_sql: String,
}

impl ImmutabilityViolation {
    pub fn stored_sql_preview(&self, max_len: usize) -> &str {
        if self.stored_sql.len() <= max_len {
            &self.stored_sql
        } else {
            &self.stored_sql[..max_len]
        }
    }

    pub fn current_sql_preview(&self, max_len: usize) -> &str {
        if self.current_sql.len() <= max_len {
            &self.current_sql
        } else {
            &self.current_sql[..max_len]
        }
    }
}

#[derive(Debug, Default)]
pub struct ImmutabilityReport {
    pub violations: Vec<ImmutabilityViolation>,
}

impl ImmutabilityReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn add(&mut self, violation: ImmutabilityViolation) {
        self.violations.push(violation);
    }

    pub fn total_affected_partitions(&self) -> usize {
        self.violations.iter().map(|v| v.affected_partitions.len()).sum()
    }
}

pub struct ImmutabilityChecker<'a> {
    queries: &'a [QueryDef],
}

impl<'a> ImmutabilityChecker<'a> {
    pub fn new(queries: &'a [QueryDef]) -> Self {
        Self { queries }
    }

    pub fn check(&self, stored_states: &[PartitionState]) -> ImmutabilityReport {
        let mut report = ImmutabilityReport::new();

        let states_by_query: HashMap<&str, Vec<&PartitionState>> = stored_states
            .iter()
            .fold(HashMap::new(), |mut acc, state| {
                acc.entry(state.query_name.as_str()).or_default().push(state);
                acc
            });

        for query in self.queries {
            let Some(query_states) = states_by_query.get(query.name.as_str()) else {
                continue;
            };

            let version_violations = self.check_version_immutability(query, query_states);
            for violation in version_violations {
                report.add(violation);
            }
        }

        report
    }

    fn check_version_immutability(
        &self,
        query: &QueryDef,
        states: &[&PartitionState],
    ) -> Vec<ImmutabilityViolation> {
        let mut violations = Vec::new();

        let mut states_by_version: HashMap<(u32, Option<u32>), Vec<&PartitionState>> = HashMap::new();
        for state in states {
            states_by_version
                .entry((state.version, state.sql_revision))
                .or_default()
                .push(state);
        }

        for ((version_num, revision_num), version_states) in states_by_version {
            let Some(version) = query.versions.iter().find(|v| v.version == version_num) else {
                continue;
            };

            let (current_sql, source) = match revision_num {
                Some(rev_num) => {
                    match version.revisions.iter().find(|r| r.revision == rev_num) {
                        Some(rev) => (rev.sql_content.as_str(), rev.source.as_str()),
                        None => continue,
                    }
                }
                None => (version.sql_content.as_str(), version.source.as_str()),
            };

            let Some(reference_state) = version_states
                .iter()
                .find(|s| s.executed_sql_b64.is_some())
            else {
                continue;
            };

            let Some(ref executed_b64) = reference_state.executed_sql_b64 else {
                continue;
            };

            let Some(stored_sql) = decompress_from_base64(executed_b64) else {
                continue;
            };

            if stored_sql != current_sql {
                let affected_partitions: Vec<_> = version_states
                    .iter()
                    .map(|s| s.partition_date)
                    .collect();

                violations.push(ImmutabilityViolation {
                    query_name: query.name.clone(),
                    version: version_num,
                    revision: revision_num,
                    source: source.to_string(),
                    affected_partitions,
                    stored_sql,
                    current_sql: current_sql.to_string(),
                });
            }
        }

        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{VersionDef, Destination, ResolvedRevision};
    use crate::schema::{Schema, PartitionConfig};
    use crate::invariant::InvariantsDef;
    use crate::drift::checksum::compress_to_base64;
    use crate::drift::state::ExecutionStatus;
    use chrono::{NaiveDate, Utc};
    use std::collections::HashSet;

    fn create_test_query(name: &str, versions: Vec<VersionDef>) -> QueryDef {
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
            versions,
            cluster: None,
        }
    }

    fn create_version(version: u32, sql: &str) -> VersionDef {
        VersionDef {
            version,
            effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            source: format!("query.v{}.sql", version),
            sql_content: sql.to_string(),
            revisions: vec![],
            description: None,
            backfill_since: None,
            schema: Schema::default(),
            dependencies: HashSet::new(),
            invariants: InvariantsDef::default(),
        }
    }

    fn create_version_with_revision(version: u32, sql: &str, rev_sql: &str) -> VersionDef {
        VersionDef {
            version,
            effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            source: format!("query.v{}.sql", version),
            sql_content: sql.to_string(),
            revisions: vec![ResolvedRevision {
                revision: 1,
                effective_from: NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(),
                source: format!("query.v{}.r1.sql", version),
                sql_content: rev_sql.to_string(),
                reason: Some("Bug fix".to_string()),
                backfill_since: None,
                dependencies: HashSet::new(),
            }],
            description: None,
            backfill_since: None,
            schema: Schema::default(),
            dependencies: HashSet::new(),
            invariants: InvariantsDef::default(),
        }
    }

    fn create_stored_state(
        query_name: &str,
        partition_date: NaiveDate,
        version: u32,
        revision: Option<u32>,
        executed_sql: &str,
    ) -> PartitionState {
        PartitionState {
            query_name: query_name.to_string(),
            partition_date,
            version,
            sql_revision: revision,
            effective_from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            sql_checksum: "checksum".to_string(),
            schema_checksum: "schema".to_string(),
            yaml_checksum: "yaml".to_string(),
            executed_sql_b64: Some(compress_to_base64(executed_sql)),
            upstream_states: HashMap::new(),
            executed_at: Utc::now(),
            execution_time_ms: Some(100),
            rows_written: Some(1000),
            bytes_processed: Some(10000),
            status: ExecutionStatus::Success,
        }
    }

    #[test]
    fn test_no_violation_when_sql_unchanged() {
        let sql = "SELECT * FROM source WHERE date = @partition_date";
        let query = create_test_query("test_query", vec![create_version(1, sql)]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), 1, None, sql),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(report.is_clean());
        assert_eq!(report.violations.len(), 0);
    }

    #[test]
    fn test_violation_when_sql_changed() {
        let original_sql = "SELECT COUNT(*) FROM source";
        let modified_sql = "SELECT COUNT(DISTINCT user_id) FROM source";

        let query = create_test_query("test_query", vec![create_version(1, modified_sql)]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, original_sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), 1, None, original_sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 17).unwrap(), 1, None, original_sql),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(!report.is_clean());
        assert_eq!(report.violations.len(), 1);

        let violation = &report.violations[0];
        assert_eq!(violation.query_name, "test_query");
        assert_eq!(violation.version, 1);
        assert_eq!(violation.revision, None);
        assert_eq!(violation.affected_partitions.len(), 3);
        assert_eq!(violation.stored_sql, original_sql);
        assert_eq!(violation.current_sql, modified_sql);
    }

    #[test]
    fn test_violation_when_revision_sql_changed() {
        let original_rev_sql = "SELECT user_id FROM users";
        let modified_rev_sql = "SELECT COALESCE(user_id, 0) FROM users";
        let base_sql = "SELECT * FROM users";

        let query = create_test_query(
            "test_query",
            vec![create_version_with_revision(1, base_sql, modified_rev_sql)],
        );
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), 1, Some(1), original_rev_sql),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(!report.is_clean());
        assert_eq!(report.violations.len(), 1);

        let violation = &report.violations[0];
        assert_eq!(violation.version, 1);
        assert_eq!(violation.revision, Some(1));
        assert_eq!(violation.stored_sql, original_rev_sql);
        assert_eq!(violation.current_sql, modified_rev_sql);
    }

    #[test]
    fn test_multiple_versions_independent_violations() {
        let v1_original = "SELECT 1";
        let v1_modified = "SELECT 2";
        let v2_sql = "SELECT 3";

        let query = create_test_query(
            "test_query",
            vec![
                create_version(1, v1_modified),
                create_version(2, v2_sql),
            ],
        );
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, v1_original),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 2, 15).unwrap(), 2, None, v2_sql),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(!report.is_clean());
        assert_eq!(report.violations.len(), 1);
        assert_eq!(report.violations[0].version, 1);
    }

    #[test]
    fn test_no_violation_for_different_versions() {
        let v1_sql = "SELECT 1";
        let v2_sql = "SELECT 2";

        let mut v2 = create_version(2, v2_sql);
        v2.effective_from = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();

        let query = create_test_query(
            "test_query",
            vec![create_version(1, v1_sql), v2],
        );
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, v1_sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 2, 15).unwrap(), 2, None, v2_sql),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(report.is_clean());
    }

    #[test]
    fn test_no_states_returns_clean() {
        let query = create_test_query("test_query", vec![create_version(1, "SELECT 1")]);
        let queries = vec![query];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&[]);

        assert!(report.is_clean());
    }

    #[test]
    fn test_state_without_executed_sql_skipped() {
        let query = create_test_query("test_query", vec![create_version(1, "SELECT modified")]);
        let queries = vec![query];

        let mut stored = create_stored_state(
            "test_query",
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            1,
            None,
            "SELECT original",
        );
        stored.executed_sql_b64 = None;

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&[stored]);

        assert!(report.is_clean());
    }

    #[test]
    fn test_multiple_queries_violations() {
        let q1_original = "SELECT 1";
        let q1_modified = "SELECT 2";
        let q2_original = "SELECT a";
        let q2_modified = "SELECT b";

        let query1 = create_test_query("query_1", vec![create_version(1, q1_modified)]);
        let query2 = create_test_query("query_2", vec![create_version(1, q2_modified)]);
        let queries = vec![query1, query2];

        let stored = vec![
            create_stored_state("query_1", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, q1_original),
            create_stored_state("query_2", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, q2_original),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(!report.is_clean());
        assert_eq!(report.violations.len(), 2);
        assert_eq!(report.total_affected_partitions(), 2);
    }

    #[test]
    fn test_whitespace_change_is_violation() {
        let original = "SELECT * FROM source";
        let with_newline = "SELECT *\nFROM source";

        let query = create_test_query("test_query", vec![create_version(1, with_newline)]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, original),
        ];

        let checker = ImmutabilityChecker::new(&queries);
        let report = checker.check(&stored);

        assert!(!report.is_clean());
    }

    #[test]
    fn test_preview_truncation() {
        let long_sql = "SELECT ".to_string() + &"a, ".repeat(100) + "FROM table";
        let violation = ImmutabilityViolation {
            query_name: "test".to_string(),
            version: 1,
            revision: None,
            source: "test.sql".to_string(),
            affected_partitions: vec![],
            stored_sql: long_sql.clone(),
            current_sql: long_sql,
        };

        let preview = violation.stored_sql_preview(50);
        assert_eq!(preview.len(), 50);
    }
}
