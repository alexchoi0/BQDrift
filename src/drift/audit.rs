use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tabled::Tabled;
use crate::dsl::QueryDef;
use super::state::PartitionState;
use super::checksum::decompress_from_base64;

#[derive(Debug, Clone, Tabled)]
pub struct AuditTableRow {
    #[tabled(rename = "Query")]
    pub query: String,
    #[tabled(rename = "Version")]
    pub version: String,
    #[tabled(rename = "Source")]
    pub source: String,
    #[tabled(rename = "Status")]
    pub status: String,
    #[tabled(rename = "Partitions")]
    pub partitions: String,
    #[tabled(rename = "Executed")]
    pub executed: String,
}

impl From<&SourceAuditEntry> for AuditTableRow {
    fn from(entry: &SourceAuditEntry) -> Self {
        let version = match entry.revision {
            Some(rev) => format!("v{}.r{}", entry.version, rev),
            None => format!("v{}", entry.version),
        };

        let status = format!("{} {}", entry.status.symbol(), entry.status.as_str());

        let partitions = if entry.partition_count > 0 {
            entry.partition_count.to_string()
        } else {
            "-".to_string()
        };

        let executed = match (entry.first_executed, entry.last_executed) {
            (Some(first), Some(last)) if first == last => {
                first.format("%Y-%m-%d").to_string()
            }
            (Some(first), Some(last)) => {
                format!("{} to {}", first.format("%Y-%m-%d"), last.format("%Y-%m-%d"))
            }
            _ => "-".to_string(),
        };

        let source = if entry.source == "<inline>" {
            truncate_sql_preview(&entry.current_sql, 30)
        } else {
            entry.source.clone()
        };

        AuditTableRow {
            query: entry.query_name.clone(),
            version,
            source,
            status,
            partitions,
            executed,
        }
    }
}

fn truncate_sql_preview(sql: &str, max_len: usize) -> String {
    let normalized: String = sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if normalized.len() <= max_len {
        normalized
    } else {
        format!("{}...", &normalized[..max_len.saturating_sub(3)])
    }
}

#[derive(Debug, Clone)]
pub struct SourceAuditEntry {
    pub query_name: String,
    pub version: u32,
    pub revision: Option<u32>,
    pub source: String,
    pub status: SourceStatus,
    pub current_sql: String,
    pub stored_sql: Option<String>,
    pub first_executed: Option<DateTime<Utc>>,
    pub last_executed: Option<DateTime<Utc>>,
    pub partition_count: usize,
}

impl Serialize for SourceAuditEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let show_stored_sql = match &self.stored_sql {
            Some(stored) => stored != &self.current_sql,
            None => false,
        };

        let field_count = if show_stored_sql { 10 } else { 9 };
        let mut state = serializer.serialize_struct("SourceAuditEntry", field_count)?;

        state.serialize_field("query_name", &self.query_name)?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("current_sql", &self.current_sql)?;

        if show_stored_sql {
            state.serialize_field("stored_sql", &self.stored_sql)?;
        }

        state.serialize_field("first_executed", &self.first_executed)?;
        state.serialize_field("last_executed", &self.last_executed)?;
        state.serialize_field("partition_count", &self.partition_count)?;

        state.end()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    Current,
    Modified,
    NeverExecuted,
}

impl SourceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceStatus::Current => "current",
            SourceStatus::Modified => "modified",
            SourceStatus::NeverExecuted => "never_executed",
        }
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            SourceStatus::Current => "✓",
            SourceStatus::Modified => "⚠",
            SourceStatus::NeverExecuted => "○",
        }
    }
}

#[derive(Debug, Default)]
pub struct SourceAuditReport {
    pub entries: Vec<SourceAuditEntry>,
}

impl SourceAuditReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, entry: SourceAuditEntry) {
        self.entries.push(entry);
    }

    pub fn has_modifications(&self) -> bool {
        self.entries.iter().any(|e| e.status == SourceStatus::Modified)
    }

    pub fn modified_count(&self) -> usize {
        self.entries.iter().filter(|e| e.status == SourceStatus::Modified).count()
    }

    pub fn current_count(&self) -> usize {
        self.entries.iter().filter(|e| e.status == SourceStatus::Current).count()
    }

    pub fn never_executed_count(&self) -> usize {
        self.entries.iter().filter(|e| e.status == SourceStatus::NeverExecuted).count()
    }

    pub fn by_query(&self) -> HashMap<String, Vec<&SourceAuditEntry>> {
        let mut grouped: HashMap<String, Vec<&SourceAuditEntry>> = HashMap::new();
        for entry in &self.entries {
            grouped.entry(entry.query_name.clone()).or_default().push(entry);
        }
        grouped
    }

    pub fn modified_entries(&self) -> Vec<&SourceAuditEntry> {
        self.entries.iter().filter(|e| e.status == SourceStatus::Modified).collect()
    }
}

pub struct SourceAuditor<'a> {
    queries: &'a [QueryDef],
}

impl<'a> SourceAuditor<'a> {
    pub fn new(queries: &'a [QueryDef]) -> Self {
        Self { queries }
    }

    pub fn audit(&self, stored_states: &[PartitionState]) -> SourceAuditReport {
        let mut report = SourceAuditReport::new();

        let states_by_query: HashMap<&str, Vec<&PartitionState>> = stored_states
            .iter()
            .fold(HashMap::new(), |mut acc, state| {
                acc.entry(state.query_name.as_str()).or_default().push(state);
                acc
            });

        for query in self.queries {
            let query_states = states_by_query.get(query.name.as_str());
            let entries = self.audit_query(query, query_states.map(|v| v.as_slice()).unwrap_or(&[]));
            for entry in entries {
                report.add(entry);
            }
        }

        report
    }

    fn audit_query(&self, query: &QueryDef, states: &[&PartitionState]) -> Vec<SourceAuditEntry> {
        let mut entries = Vec::new();

        let mut states_by_version: HashMap<(u32, Option<u32>), Vec<&PartitionState>> = HashMap::new();
        for state in states {
            states_by_version
                .entry((state.version, state.sql_revision))
                .or_default()
                .push(state);
        }

        for version in &query.versions {
            let entry = self.audit_version_source(
                query,
                version.version,
                None,
                &version.source,
                &version.sql_content,
                states_by_version.get(&(version.version, None)),
            );
            entries.push(entry);

            for revision in &version.revisions {
                let entry = self.audit_version_source(
                    query,
                    version.version,
                    Some(revision.revision),
                    &revision.source,
                    &revision.sql_content,
                    states_by_version.get(&(version.version, Some(revision.revision))),
                );
                entries.push(entry);
            }
        }

        entries
    }

    fn audit_version_source(
        &self,
        query: &QueryDef,
        version: u32,
        revision: Option<u32>,
        source: &str,
        current_sql: &str,
        states: Option<&Vec<&PartitionState>>,
    ) -> SourceAuditEntry {
        let states = states.map(|v| v.as_slice()).unwrap_or(&[]);

        if states.is_empty() {
            return SourceAuditEntry {
                query_name: query.name.clone(),
                version,
                revision,
                source: source.to_string(),
                status: SourceStatus::NeverExecuted,
                current_sql: current_sql.to_string(),
                stored_sql: None,
                first_executed: None,
                last_executed: None,
                partition_count: 0,
            };
        }

        let first_executed = states.iter().map(|s| s.executed_at).min();
        let last_executed = states.iter().map(|s| s.executed_at).max();
        let partition_count = states.len();

        let stored_sql = states
            .iter()
            .find_map(|s| s.executed_sql_b64.as_ref())
            .and_then(|b64| decompress_from_base64(b64));

        let status = match &stored_sql {
            Some(stored) if stored == current_sql => SourceStatus::Current,
            Some(_) => SourceStatus::Modified,
            None => SourceStatus::NeverExecuted,
        };

        SourceAuditEntry {
            query_name: query.name.clone(),
            version,
            revision,
            source: source.to_string(),
            status,
            current_sql: current_sql.to_string(),
            stored_sql,
            first_executed,
            last_executed,
            partition_count,
        }
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
    fn test_audit_no_states_all_never_executed() {
        let query = create_test_query("test_query", vec![create_version(1, "SELECT 1")]);
        let queries = vec![query];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&[]);

        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].status, SourceStatus::NeverExecuted);
        assert_eq!(report.never_executed_count(), 1);
        assert!(!report.has_modifications());
    }

    #[test]
    fn test_audit_current_source() {
        let sql = "SELECT * FROM source";
        let query = create_test_query("test_query", vec![create_version(1, sql)]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, sql),
        ];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&stored);

        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].status, SourceStatus::Current);
        assert_eq!(report.current_count(), 1);
        assert!(!report.has_modifications());
    }

    #[test]
    fn test_audit_modified_source() {
        let original_sql = "SELECT 1";
        let modified_sql = "SELECT 2";

        let query = create_test_query("test_query", vec![create_version(1, modified_sql)]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, original_sql),
        ];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&stored);

        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].status, SourceStatus::Modified);
        assert_eq!(report.entries[0].current_sql, modified_sql);
        assert_eq!(report.entries[0].stored_sql, Some(original_sql.to_string()));
        assert_eq!(report.modified_count(), 1);
        assert!(report.has_modifications());
    }

    #[test]
    fn test_audit_multiple_versions() {
        let v1_sql = "SELECT 1";
        let v2_sql = "SELECT 2";

        let query = create_test_query("test_query", vec![
            create_version(1, v1_sql),
            create_version(2, v2_sql),
        ]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, v1_sql),
        ];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&stored);

        assert_eq!(report.entries.len(), 2);

        let v1_entry = report.entries.iter().find(|e| e.version == 1).unwrap();
        let v2_entry = report.entries.iter().find(|e| e.version == 2).unwrap();

        assert_eq!(v1_entry.status, SourceStatus::Current);
        assert_eq!(v2_entry.status, SourceStatus::NeverExecuted);
    }

    #[test]
    fn test_audit_with_revisions() {
        let base_sql = "SELECT * FROM users";
        let rev_sql = "SELECT COALESCE(user_id, 0) FROM users";

        let query = create_test_query("test_query", vec![
            create_version_with_revision(1, base_sql, rev_sql),
        ]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, base_sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(), 1, Some(1), rev_sql),
        ];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&stored);

        assert_eq!(report.entries.len(), 2);

        let base_entry = report.entries.iter().find(|e| e.revision.is_none()).unwrap();
        let rev_entry = report.entries.iter().find(|e| e.revision == Some(1)).unwrap();

        assert_eq!(base_entry.status, SourceStatus::Current);
        assert_eq!(rev_entry.status, SourceStatus::Current);
    }

    #[test]
    fn test_audit_partition_count() {
        let sql = "SELECT 1";
        let query = create_test_query("test_query", vec![create_version(1, sql)]);
        let queries = vec![query];

        let stored = vec![
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(), 1, None, sql),
            create_stored_state("test_query", NaiveDate::from_ymd_opt(2024, 1, 17).unwrap(), 1, None, sql),
        ];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&stored);

        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].partition_count, 3);
    }

    #[test]
    fn test_audit_multiple_queries() {
        let q1_sql = "SELECT 1";
        let q2_sql = "SELECT 2";
        let q2_modified = "SELECT 3";

        let query1 = create_test_query("query_1", vec![create_version(1, q1_sql)]);
        let query2 = create_test_query("query_2", vec![create_version(1, q2_modified)]);
        let queries = vec![query1, query2];

        let stored = vec![
            create_stored_state("query_1", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, q1_sql),
            create_stored_state("query_2", NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(), 1, None, q2_sql),
        ];

        let auditor = SourceAuditor::new(&queries);
        let report = auditor.audit(&stored);

        assert_eq!(report.entries.len(), 2);
        assert_eq!(report.current_count(), 1);
        assert_eq!(report.modified_count(), 1);

        let by_query = report.by_query();
        assert_eq!(by_query.len(), 2);
    }

    #[test]
    fn test_source_status_symbols() {
        assert_eq!(SourceStatus::Current.symbol(), "✓");
        assert_eq!(SourceStatus::Modified.symbol(), "⚠");
        assert_eq!(SourceStatus::NeverExecuted.symbol(), "○");
    }

    #[test]
    fn test_truncate_sql_preview_short() {
        let sql = "SELECT 1";
        assert_eq!(truncate_sql_preview(sql, 30), "SELECT 1");
    }

    #[test]
    fn test_truncate_sql_preview_long() {
        let sql = "SELECT user_id, COUNT(*) as count FROM users WHERE status = 'active' GROUP BY 1";
        let result = truncate_sql_preview(sql, 30);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 30);
        assert!(result.starts_with("SELECT user_id"));
    }

    #[test]
    fn test_truncate_sql_preview_normalizes_whitespace() {
        let sql = "SELECT\n  user_id,\n  COUNT(*)\nFROM users";
        let result = truncate_sql_preview(sql, 50);
        assert_eq!(result, "SELECT user_id, COUNT(*) FROM users");
        assert!(!result.contains('\n'));
    }

    #[test]
    fn test_audit_table_row_inline_source() {
        let entry = SourceAuditEntry {
            query_name: "test".to_string(),
            version: 1,
            revision: None,
            source: "<inline>".to_string(),
            status: SourceStatus::NeverExecuted,
            current_sql: "SELECT user_id, name, email FROM users WHERE active = true".to_string(),
            stored_sql: None,
            first_executed: None,
            last_executed: None,
            partition_count: 0,
        };

        let row = AuditTableRow::from(&entry);
        assert!(row.source.ends_with("..."));
        assert!(row.source.len() <= 30);
        assert!(row.source.starts_with("SELECT"));
    }

    #[test]
    fn test_audit_table_row_file_source() {
        let entry = SourceAuditEntry {
            query_name: "test".to_string(),
            version: 1,
            revision: None,
            source: "query.v1.sql".to_string(),
            status: SourceStatus::NeverExecuted,
            current_sql: "SELECT * FROM table".to_string(),
            stored_sql: None,
            first_executed: None,
            last_executed: None,
            partition_count: 0,
        };

        let row = AuditTableRow::from(&entry);
        assert_eq!(row.source, "query.v1.sql");
    }
}
