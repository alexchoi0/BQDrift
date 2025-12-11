use bqdrift::dsl::QueryLoader;
use bqdrift::{BqType, Severity};
use bqdrift::invariant::InvariantCheck;
use chrono::NaiveDate;
use std::path::Path;

fn fixtures_path() -> &'static Path {
    Path::new("tests/fixtures")
}

#[test]
fn test_load_simple_query() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/simple_query.yaml"));

    assert!(query.is_ok());
    let query = query.unwrap();

    assert_eq!(query.name, "simple_query");
    assert_eq!(query.destination.dataset, "test_dataset");
    assert_eq!(query.destination.table, "simple_table");
    assert_eq!(query.description, Some("A simple test query".to_string()));
    assert_eq!(query.owner, Some("test-team".to_string()));
    assert_eq!(query.tags, vec!["test"]);
}

#[test]
fn test_load_simple_query_schema() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/simple_query.yaml")).unwrap();

    assert_eq!(query.versions.len(), 1);
    let v1 = &query.versions[0];
    assert_eq!(v1.version, 1);
    assert_eq!(v1.schema.fields.len(), 3);
    assert!(v1.schema.has_field("date"));
    assert!(v1.schema.has_field("region"));
    assert!(v1.schema.has_field("count"));
}

#[test]
fn test_load_simple_query_sql() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/simple_query.yaml")).unwrap();

    let v1 = &query.versions[0];
    assert!(v1.sql_content.contains("SELECT"));
    assert!(v1.sql_content.contains("@partition_date"));
}

#[test]
fn test_load_simple_query_partition() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/simple_query.yaml")).unwrap();

    assert_eq!(query.destination.partition.field, Some("date".to_string()));
}

#[test]
fn test_load_simple_query_cluster() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/simple_query.yaml")).unwrap();

    assert!(query.cluster.is_some());
    assert_eq!(query.cluster.as_ref().unwrap().fields, vec!["region"]);
}

#[test]
fn test_load_versioned_query() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml"));

    assert!(query.is_ok());
    let query = query.unwrap();
    assert_eq!(query.versions.len(), 4);
}

#[test]
fn test_versioned_query_schema_reference() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let v1 = &query.versions[0];
    let v2 = &query.versions[1];

    assert_eq!(v1.schema.fields.len(), v2.schema.fields.len());
    assert_eq!(v1.schema.fields[0].name, v2.schema.fields[0].name);
}

#[test]
fn test_versioned_query_schema_extension() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let v2 = &query.versions[1];
    let v3 = &query.versions[2];

    assert_eq!(v2.schema.fields.len(), 3);
    assert_eq!(v3.schema.fields.len(), 4);
    assert!(v3.schema.has_field("session_count"));
}

#[test]
fn test_versioned_query_revisions() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let v2 = &query.versions[1];
    assert_eq!(v2.revisions.len(), 1);

    let rev = &v2.revisions[0];
    assert_eq!(rev.revision, 1);
    assert_eq!(rev.reason, Some("Fixed null handling".to_string()));
    assert!(rev.sql_content.contains("COALESCE"));
}

#[test]
fn test_get_version_for_date() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let date_v1 = NaiveDate::from_ymd_opt(2024, 2, 15).unwrap();
    let version = query.get_version_for_date(date_v1);
    assert!(version.is_some());
    assert_eq!(version.unwrap().version, 1);

    let date_v2 = NaiveDate::from_ymd_opt(2024, 4, 15).unwrap();
    let version = query.get_version_for_date(date_v2);
    assert!(version.is_some());
    assert_eq!(version.unwrap().version, 2);

    let date_v3 = NaiveDate::from_ymd_opt(2024, 7, 15).unwrap();
    let version = query.get_version_for_date(date_v3);
    assert!(version.is_some());
    assert_eq!(version.unwrap().version, 3);
}

#[test]
fn test_get_version_for_date_before_first() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let date_before = NaiveDate::from_ymd_opt(2023, 12, 1).unwrap();
    let version = query.get_version_for_date(date_before);
    assert!(version.is_none());
}

#[test]
fn test_get_sql_for_date_with_revision() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let v2 = &query.versions[1];

    let before_revision = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
    let sql = v2.get_sql_for_date(before_revision);
    assert!(!sql.contains("COALESCE"));

    let after_revision = NaiveDate::from_ymd_opt(2024, 3, 20).unwrap();
    let sql = v2.get_sql_for_date(after_revision);
    assert!(sql.contains("COALESCE"));
}

#[test]
fn test_latest_version() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let latest = query.latest_version();
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().version, 4);
}

#[test]
fn test_load_directory() {
    let loader = QueryLoader::new();
    let queries = loader.load_dir(fixtures_path());

    assert!(queries.is_ok());
    let queries = queries.unwrap();
    assert_eq!(queries.len(), 3);
}

#[test]
fn test_load_nonexistent_yaml() {
    let loader = QueryLoader::new();
    let result = loader.load_query("nonexistent.yaml");
    assert!(result.is_err());
}

#[test]
fn test_effective_from_dates() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    assert_eq!(
        query.versions[0].effective_from,
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
    );
    assert_eq!(
        query.versions[1].effective_from,
        NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()
    );
    assert_eq!(
        query.versions[2].effective_from,
        NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()
    );
}

#[test]
fn test_versioned_query_schema_modify() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/versioned_query.yaml")).unwrap();

    let v3 = &query.versions[2];
    let v4 = &query.versions[3];

    // v3 has events as INT64
    let events_v3 = v3.schema.get_field("events").unwrap();
    assert_eq!(events_v3.field_type, BqType::Int64);

    // v4 modified events to FLOAT64
    let events_v4 = v4.schema.get_field("events").unwrap();
    assert_eq!(events_v4.field_type, BqType::Float64);

    // v4 should still have all fields from v3
    assert_eq!(v4.schema.fields.len(), v3.schema.fields.len());
    assert!(v4.schema.has_field("date"));
    assert!(v4.schema.has_field("user_id"));
    assert!(v4.schema.has_field("events"));
    assert!(v4.schema.has_field("session_count"));
}

#[test]
fn test_sql_dependencies_auto_extracted() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/simple_query.yaml")).unwrap();

    let v1 = &query.versions[0];
    // Dependencies should be auto-extracted from SQL
    assert!(!v1.dependencies.is_empty());
    // Should contain raw.events table from the SQL
    assert!(v1.dependencies.iter().any(|d| d.contains("raw.events") || d.contains("events")));
}

#[test]
fn test_load_query_with_invariants() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml"));

    assert!(query.is_ok());
    let query = query.unwrap();
    assert_eq!(query.name, "query_with_invariants");
    assert_eq!(query.versions.len(), 2);
}

#[test]
fn test_invariants_v1_before_checks() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v1 = &query.versions[0];
    assert_eq!(v1.invariants.before.len(), 1);

    let check = &v1.invariants.before[0];
    assert_eq!(check.name, "source_data_check");
    assert_eq!(check.severity, Severity::Error);
    match &check.check {
        InvariantCheck::RowCount { max, .. } => {
            assert_eq!(*max, Some(0));
        }
        _ => panic!("Expected RowCount check"),
    }
}

#[test]
fn test_invariants_v1_after_checks() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v1 = &query.versions[0];
    assert_eq!(v1.invariants.after.len(), 4);

    let names: Vec<_> = v1.invariants.after.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"min_rows"));
    assert!(names.contains(&"null_check"));
    assert!(names.contains(&"count_positive"));
    assert!(names.contains(&"region_cardinality"));
}

#[test]
fn test_invariants_row_count_check() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v1 = &query.versions[0];
    let min_rows = v1.invariants.after.iter().find(|i| i.name == "min_rows").unwrap();

    match &min_rows.check {
        InvariantCheck::RowCount { min, max, .. } => {
            assert_eq!(*min, Some(10));
            assert_eq!(*max, None);
        }
        _ => panic!("Expected RowCount check"),
    }
}

#[test]
fn test_invariants_null_percentage_check() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v1 = &query.versions[0];
    let null_check = v1.invariants.after.iter().find(|i| i.name == "null_check").unwrap();

    assert_eq!(null_check.severity, Severity::Warning);
    match &null_check.check {
        InvariantCheck::NullPercentage { column, max_percentage, .. } => {
            assert_eq!(column, "region");
            assert!((max_percentage - 5.0).abs() < 0.001);
        }
        _ => panic!("Expected NullPercentage check"),
    }
}

#[test]
fn test_invariants_value_range_check() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v1 = &query.versions[0];
    let count_positive = v1.invariants.after.iter().find(|i| i.name == "count_positive").unwrap();

    match &count_positive.check {
        InvariantCheck::ValueRange { column, min, max, .. } => {
            assert_eq!(column, "count");
            assert_eq!(*min, Some(0.0));
            assert_eq!(*max, None);
        }
        _ => panic!("Expected ValueRange"),
    }
}

#[test]
fn test_invariants_distinct_count_check() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v1 = &query.versions[0];
    let cardinality = v1.invariants.after.iter().find(|i| i.name == "region_cardinality").unwrap();

    match &cardinality.check {
        InvariantCheck::DistinctCount { column, min, max, .. } => {
            assert_eq!(column, "region");
            assert_eq!(*min, Some(1));
            assert_eq!(*max, Some(100));
        }
        _ => panic!("Expected DistinctCount check"),
    }
}

#[test]
fn test_invariants_v2_extended() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v2 = &query.versions[1];

    // Before checks should be inherited from v1
    assert_eq!(v2.invariants.before.len(), 1);

    // After checks: 4 from v1 - 1 removed (null_check) + 1 added (new_check) = 4
    assert_eq!(v2.invariants.after.len(), 4);

    let names: Vec<_> = v2.invariants.after.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"min_rows"));
    assert!(names.contains(&"count_positive"));
    assert!(names.contains(&"region_cardinality"));
    assert!(names.contains(&"new_check"));
    assert!(!names.contains(&"null_check"));
}

#[test]
fn test_invariants_v2_modified_check() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v2 = &query.versions[1];
    let min_rows = v2.invariants.after.iter().find(|i| i.name == "min_rows").unwrap();

    match &min_rows.check {
        InvariantCheck::RowCount { min, .. } => {
            assert_eq!(*min, Some(100));
        }
        _ => panic!("Expected RowCount check"),
    }
}

#[test]
fn test_invariants_v2_added_check() {
    let loader = QueryLoader::new();
    let query = loader.load_query(fixtures_path().join("analytics/query_with_invariants.yaml")).unwrap();

    let v2 = &query.versions[1];
    let new_check = v2.invariants.after.iter().find(|i| i.name == "new_check").unwrap();

    assert_eq!(new_check.severity, Severity::Warning);
    match &new_check.check {
        InvariantCheck::RowCount { max, .. } => {
            assert_eq!(*max, Some(1000000));
        }
        _ => panic!("Expected RowCount check"),
    }
}
