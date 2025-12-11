use bqdrift::dsl::QueryLoader;
use bqdrift::{DriftDetector, DriftState, PartitionState, ExecutionStatus, compress_to_base64, decompress_from_base64, Checksums};
use bqdrift::ImmutabilityChecker;
use bqdrift::schema::Schema;
use bqdrift::diff::{decode_sql, format_sql_diff, has_changes};
use chrono::{NaiveDate, Utc};
use std::collections::HashMap;
use std::path::Path;

fn fixtures_path() -> &'static Path {
    Path::new("tests/fixtures")
}

fn create_stored_state_for_query(
    query_name: &str,
    partition_date: NaiveDate,
    sql_content: &str,
    yaml_content: &str,
    schema: &Schema,
) -> PartitionState {
    create_stored_state_with_version(query_name, partition_date, 1, None, sql_content, yaml_content, schema)
}

fn create_stored_state_with_version(
    query_name: &str,
    partition_date: NaiveDate,
    version: u32,
    revision: Option<u32>,
    sql_content: &str,
    yaml_content: &str,
    schema: &Schema,
) -> PartitionState {
    let checksums = Checksums::compute(sql_content, schema, yaml_content);
    PartitionState {
        query_name: query_name.to_string(),
        partition_date,
        version,
        sql_revision: revision,
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
        status: ExecutionStatus::Success,
    }
}

#[test]
fn test_drift_detector_with_fixture_queries() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let detector = DriftDetector::new(queries, yaml_contents);

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let report = detector.detect(&[], date, date).unwrap();

    assert!(!report.partitions.is_empty());
    for drift in &report.partitions {
        assert_eq!(drift.state, DriftState::NeverRun);
        assert!(drift.current_sql.is_some());
        assert!(drift.executed_sql_b64.is_none());
    }
}

#[test]
fn test_drift_detector_current_state_with_fixture() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();
    let current_sql = version.get_sql_for_date(date);

    let stored = create_stored_state_for_query(
        "simple_query",
        date,
        current_sql,
        yaml_content,
        &version.schema,
    );

    let detector = DriftDetector::new(vec![simple_query.clone()], yaml_contents.clone());
    let report = detector.detect(&[stored], date, date).unwrap();

    let drift = report.partitions.iter()
        .find(|p| p.query_name == "simple_query")
        .unwrap();

    assert_eq!(drift.state, DriftState::Current);
    assert!(drift.current_sql.is_some());
    assert!(drift.executed_sql_b64.is_some());
}

#[test]
fn test_drift_detector_sql_changed_with_fixture() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();
    let old_sql = "SELECT 'old_version' as marker, date FROM source";

    let stored = create_stored_state_for_query(
        "simple_query",
        date,
        old_sql,
        yaml_content,
        &version.schema,
    );

    let detector = DriftDetector::new(vec![simple_query.clone()], yaml_contents.clone());
    let report = detector.detect(&[stored], date, date).unwrap();

    let drift = report.partitions.iter()
        .find(|p| p.query_name == "simple_query")
        .unwrap();

    assert_eq!(drift.state, DriftState::SqlChanged);
    assert!(drift.current_sql.is_some());
    assert!(drift.executed_sql_b64.is_some());

    let executed_sql = decompress_from_base64(drift.executed_sql_b64.as_ref().unwrap()).unwrap();
    assert_eq!(executed_sql, old_sql);
    assert!(drift.current_sql.as_ref().unwrap().contains("@partition_date"));
}

#[test]
fn test_sql_diff_integration() {
    let old_sql = "SELECT\n  date,\n  region,\n  COUNT(*) as count\nFROM source\nWHERE date = @partition_date";
    let new_sql = "SELECT\n  date,\n  region,\n  COALESCE(country, 'Unknown') as country,\n  COUNT(*) as count\nFROM source\nWHERE date = @partition_date";

    assert!(has_changes(old_sql, new_sql));

    let diff_output = format_sql_diff(old_sql, new_sql);
    assert!(diff_output.contains("COALESCE"));
    assert!(diff_output.contains("country"));
}

#[test]
fn test_sql_diff_no_changes() {
    let sql = "SELECT * FROM table";
    assert!(!has_changes(sql, sql));
    assert!(!has_changes(sql, "  SELECT * FROM table  "));
}

#[test]
fn test_compress_decompress_roundtrip_with_real_sql() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();
    let sql = version.get_sql_for_date(date);

    let compressed = compress_to_base64(sql);
    let decompressed = decompress_from_base64(&compressed).unwrap();

    assert_eq!(sql, &decompressed);
}

#[test]
fn test_diff_module_decode_vs_drift_decompress() {
    let sql = "SELECT * FROM users WHERE id = 1";

    let compressed = compress_to_base64(sql);
    let simple_b64 = bqdrift::diff::encode_sql(sql);

    assert!(decompress_from_base64(&compressed).is_some());
    assert!(decode_sql(&simple_b64).is_some());

    assert!(decode_sql(&compressed).is_none());

    assert_eq!(decompress_from_base64(&compressed).unwrap(), sql);
    assert_eq!(decode_sql(&simple_b64).unwrap(), sql);
}

#[test]
fn test_drift_report_summary_with_mixed_states() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date1 = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let date2 = NaiveDate::from_ymd_opt(2024, 6, 16).unwrap();
    let date3 = NaiveDate::from_ymd_opt(2024, 6, 17).unwrap();

    let version = simple_query.get_version_for_date(date1).unwrap();
    let current_sql = version.get_sql_for_date(date1);

    let stored_current = create_stored_state_for_query("simple_query", date1, current_sql, yaml_content, &version.schema);
    let stored_changed = create_stored_state_for_query("simple_query", date2, "SELECT 'old' FROM x", yaml_content, &version.schema);

    let detector = DriftDetector::new(vec![simple_query.clone()], yaml_contents.clone());
    let report = detector.detect(&[stored_current, stored_changed], date1, date3).unwrap();

    let summary = report.summary();
    assert!(summary.contains_key(&DriftState::Current));
    assert!(summary.contains_key(&DriftState::SqlChanged));
    assert!(summary.contains_key(&DriftState::NeverRun));

    assert_eq!(*summary.get(&DriftState::Current).unwrap_or(&0), 1);
    assert_eq!(*summary.get(&DriftState::SqlChanged).unwrap_or(&0), 1);
    assert_eq!(*summary.get(&DriftState::NeverRun).unwrap_or(&0), 1);
}

#[test]
fn test_needs_rerun_filters_correctly() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date1 = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let date2 = NaiveDate::from_ymd_opt(2024, 6, 16).unwrap();

    let version = simple_query.get_version_for_date(date1).unwrap();
    let current_sql = version.get_sql_for_date(date1);

    let stored_current = create_stored_state_for_query("simple_query", date1, current_sql, yaml_content, &version.schema);

    let detector = DriftDetector::new(vec![simple_query.clone()], yaml_contents.clone());
    let report = detector.detect(&[stored_current], date1, date2).unwrap();

    let needs_rerun = report.needs_rerun();

    assert_eq!(needs_rerun.len(), 1);
    assert_eq!(needs_rerun[0].partition_date(), date2);
    assert_eq!(needs_rerun[0].state, DriftState::NeverRun);
}

// ============================================================================
// Immutability Checker Integration Tests
// ============================================================================

#[test]
fn test_immutability_check_no_violation_with_fixtures() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();
    let current_sql = version.get_sql_for_date(date);

    let stored_states = vec![
        create_stored_state_for_query("simple_query", date, current_sql, yaml_content, &version.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(report.is_clean());
    assert_eq!(report.violations.len(), 0);
}

#[test]
fn test_immutability_check_violation_when_source_modified() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();

    let original_executed_sql = "SELECT 'was_different' as marker FROM source WHERE date = @partition_date";

    let stored_states = vec![
        create_stored_state_for_query("simple_query", date, original_executed_sql, yaml_content, &version.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(!report.is_clean());
    assert_eq!(report.violations.len(), 1);

    let violation = &report.violations[0];
    assert_eq!(violation.query_name, "simple_query");
    assert_eq!(violation.version, 1);
    assert_eq!(violation.revision, None);
    assert_eq!(violation.affected_partitions.len(), 1);
    assert_eq!(violation.stored_sql, original_executed_sql);
    assert!(violation.current_sql.contains("@partition_date"));
}

#[test]
fn test_immutability_check_multiple_partitions_same_version() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date1 = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let date2 = NaiveDate::from_ymd_opt(2024, 6, 16).unwrap();
    let date3 = NaiveDate::from_ymd_opt(2024, 6, 17).unwrap();
    let version = simple_query.get_version_for_date(date1).unwrap();

    let original_executed_sql = "SELECT 'original' FROM source";

    let stored_states = vec![
        create_stored_state_for_query("simple_query", date1, original_executed_sql, yaml_content, &version.schema),
        create_stored_state_for_query("simple_query", date2, original_executed_sql, yaml_content, &version.schema),
        create_stored_state_for_query("simple_query", date3, original_executed_sql, yaml_content, &version.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(!report.is_clean());
    assert_eq!(report.violations.len(), 1);
    assert_eq!(report.violations[0].affected_partitions.len(), 3);
    assert_eq!(report.total_affected_partitions(), 3);
}

#[test]
fn test_immutability_check_no_stored_states_is_clean() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&[]);

    assert!(report.is_clean());
}

#[test]
fn test_immutability_check_multiple_queries_independent() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();
    let current_sql = version.get_sql_for_date(date);

    let stored_states = vec![
        create_stored_state_for_query("simple_query", date, current_sql, yaml_content, &version.schema),
        create_stored_state_for_query("nonexistent_query", date, "SELECT 1", "", &Schema::default()),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(report.is_clean());
}

#[test]
fn test_immutability_check_versioned_query_v1_unchanged() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let versioned_query = queries.iter().find(|q| q.name == "versioned_query").unwrap();
    let yaml_content = yaml_contents.get("versioned_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 2, 15).unwrap();
    let version = versioned_query.get_version_for_date(date).unwrap();
    assert_eq!(version.version, 1);
    let current_sql = version.get_sql_for_date(date);

    let stored_states = vec![
        create_stored_state_with_version("versioned_query", date, 1, None, current_sql, yaml_content, &version.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(report.is_clean());
}

#[test]
fn test_immutability_check_versioned_query_v3_unchanged() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let versioned_query = queries.iter().find(|q| q.name == "versioned_query").unwrap();
    let yaml_content = yaml_contents.get("versioned_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = versioned_query.get_version_for_date(date).unwrap();
    assert_eq!(version.version, 3); // v3 effective from 2024-06-01
    let current_sql = version.get_sql_for_date(date);

    let stored_states = vec![
        create_stored_state_with_version("versioned_query", date, 3, None, current_sql, yaml_content, &version.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(report.is_clean());
}

#[test]
fn test_immutability_check_different_version_no_cross_contamination() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let versioned_query = queries.iter().find(|q| q.name == "versioned_query").unwrap();
    let yaml_content = yaml_contents.get("versioned_query").unwrap();

    let v1_date = NaiveDate::from_ymd_opt(2024, 2, 15).unwrap(); // v1 effective
    let v3_date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(); // v3 effective (not v2!)

    let v1 = versioned_query.get_version_for_date(v1_date).unwrap();
    let v3 = versioned_query.get_version_for_date(v3_date).unwrap();

    assert_eq!(v1.version, 1);
    assert_eq!(v3.version, 3);

    let v1_sql = v1.get_sql_for_date(v1_date);
    let v3_sql = v3.get_sql_for_date(v3_date);

    let stored_states = vec![
        create_stored_state_with_version("versioned_query", v1_date, 1, None, v1_sql, yaml_content, &v1.schema),
        create_stored_state_with_version("versioned_query", v3_date, 3, None, v3_sql, yaml_content, &v3.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(report.is_clean());
}

#[test]
fn test_immutability_check_whitespace_change_is_violation() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();
    let current_sql = version.get_sql_for_date(date);

    let sql_with_extra_whitespace = current_sql.to_string() + "   \n\n";

    let stored_states = vec![
        create_stored_state_for_query("simple_query", date, &sql_with_extra_whitespace, yaml_content, &version.schema),
    ];

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(!report.is_clean());
}

#[test]
fn test_immutability_report_total_affected_partitions() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let version = simple_query.get_version_for_date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()).unwrap();

    let dates: Vec<NaiveDate> = (15..=20)
        .map(|d| NaiveDate::from_ymd_opt(2024, 6, d).unwrap())
        .collect();

    let stored_states: Vec<PartitionState> = dates.iter()
        .map(|&date| create_stored_state_for_query("simple_query", date, "SELECT 'old'", yaml_content, &version.schema))
        .collect();

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&stored_states);

    assert!(!report.is_clean());
    assert_eq!(report.total_affected_partitions(), 6);
}

#[test]
fn test_immutability_check_state_without_executed_sql_skipped() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path()).unwrap();
    let yaml_contents = loader.load_yaml_contents(fixtures_path()).unwrap();

    let simple_query = queries.iter().find(|q| q.name == "simple_query").unwrap();
    let yaml_content = yaml_contents.get("simple_query").unwrap();

    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let version = simple_query.get_version_for_date(date).unwrap();

    let mut stored_state = create_stored_state_for_query("simple_query", date, "SELECT 'different'", yaml_content, &version.schema);
    stored_state.executed_sql_b64 = None;

    let checker = ImmutabilityChecker::new(&queries);
    let report = checker.check(&[stored_state]);

    assert!(report.is_clean());
}
