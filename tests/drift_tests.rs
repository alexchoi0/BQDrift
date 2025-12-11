use bqdrift::dsl::QueryLoader;
use bqdrift::{DriftDetector, DriftState, PartitionState, ExecutionStatus, compress_to_base64, decompress_from_base64, Checksums};
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
    let checksums = Checksums::compute(sql_content, schema, yaml_content);
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
    assert_eq!(needs_rerun[0].partition_date, date2);
    assert_eq!(needs_rerun[0].state, DriftState::NeverRun);
}
