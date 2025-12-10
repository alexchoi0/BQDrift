use bqdrift::schema::{BqType, Field, FieldMode, Schema, PartitionConfig, PartitionType, ClusterConfig};

#[test]
fn test_field_creation() {
    let field = Field::new("user_id", BqType::String);
    assert_eq!(field.name, "user_id");
    assert_eq!(field.field_type, BqType::String);
    assert_eq!(field.mode, FieldMode::Nullable);
    assert!(field.nullable);
}

#[test]
fn test_field_required() {
    let field = Field::new("id", BqType::Int64).required();
    assert_eq!(field.mode, FieldMode::Required);
    assert!(!field.nullable);
}

#[test]
fn test_field_repeated() {
    let field = Field::new("tags", BqType::String).repeated();
    assert_eq!(field.mode, FieldMode::Repeated);
}

#[test]
fn test_field_with_description() {
    let field = Field::new("count", BqType::Int64)
        .with_description("Total event count");
    assert_eq!(field.description, Some("Total event count".to_string()));
}

#[test]
fn test_nested_record_field() {
    let nested = vec![
        Field::new("city", BqType::String),
        Field::new("country", BqType::String),
    ];
    let field = Field::new("address", BqType::Record).with_fields(nested);
    assert!(field.fields.is_some());
    assert_eq!(field.fields.as_ref().unwrap().len(), 2);
}

#[test]
fn test_schema_creation() {
    let schema = Schema::new()
        .add_field(Field::new("date", BqType::Date))
        .add_field(Field::new("count", BqType::Int64));
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn test_schema_from_fields() {
    let fields = vec![
        Field::new("date", BqType::Date),
        Field::new("region", BqType::String),
    ];
    let schema = Schema::from_fields(fields);
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn test_schema_get_field() {
    let schema = Schema::new()
        .add_field(Field::new("date", BqType::Date))
        .add_field(Field::new("count", BqType::Int64));

    let field = schema.get_field("date");
    assert!(field.is_some());
    assert_eq!(field.unwrap().field_type, BqType::Date);

    assert!(schema.get_field("nonexistent").is_none());
}

#[test]
fn test_schema_has_field() {
    let schema = Schema::new()
        .add_field(Field::new("date", BqType::Date));

    assert!(schema.has_field("date"));
    assert!(!schema.has_field("nonexistent"));
}

#[test]
fn test_schema_remove_field() {
    let schema = Schema::new()
        .add_field(Field::new("date", BqType::Date))
        .add_field(Field::new("count", BqType::Int64))
        .remove_field("count");

    assert_eq!(schema.fields.len(), 1);
    assert!(schema.has_field("date"));
    assert!(!schema.has_field("count"));
}

#[test]
fn test_partition_config_day() {
    let config = PartitionConfig::day("date");
    assert_eq!(config.field, Some("date".to_string()));
    assert_eq!(config.partition_type, PartitionType::Day);
}

#[test]
fn test_partition_config_hour() {
    let config = PartitionConfig::hour("timestamp");
    assert_eq!(config.partition_type, PartitionType::Hour);
}

#[test]
fn test_partition_config_range() {
    let config = PartitionConfig::range("customer_id", 0, 1000000, 1000);
    assert_eq!(config.partition_type, PartitionType::Range);
    assert_eq!(config.start, Some(0));
    assert_eq!(config.end, Some(1000000));
    assert_eq!(config.interval, Some(1000));
}

#[test]
fn test_partition_config_ingestion_time() {
    let config = PartitionConfig::ingestion_time(PartitionType::Day);
    assert_eq!(config.partition_type, PartitionType::IngestionTime);
    assert_eq!(config.granularity, Some(PartitionType::Day));
    assert!(config.field.is_none());
}

#[test]
fn test_cluster_config_valid() {
    let config = ClusterConfig::new(vec![
        "region".to_string(),
        "country".to_string(),
    ]);
    assert!(config.is_ok());
    assert_eq!(config.unwrap().len(), 2);
}

#[test]
fn test_cluster_config_max_four_fields() {
    let config = ClusterConfig::new(vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
        "e".to_string(),
    ]);
    assert!(config.is_err());
}

#[test]
fn test_cluster_config_from_fields() {
    let config = ClusterConfig::from_fields(["region", "country"]);
    assert!(config.is_ok());
    assert_eq!(config.unwrap().fields, vec!["region", "country"]);
}

#[test]
fn test_bq_types() {
    assert_eq!(BqType::String, BqType::String);
    assert_eq!(BqType::Int64, BqType::Int64);
    assert_eq!(BqType::Float64, BqType::Float64);
    assert_eq!(BqType::Bool, BqType::Bool);
    assert_eq!(BqType::Date, BqType::Date);
    assert_eq!(BqType::Timestamp, BqType::Timestamp);
    assert_eq!(BqType::Numeric, BqType::Numeric);
    assert_eq!(BqType::Record, BqType::Record);
}
