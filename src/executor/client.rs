use chrono::{DateTime, Utc};
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use gcp_bigquery_client::model::time_partitioning::TimePartitioning;
use gcp_bigquery_client::model::clustering::Clustering;
use crate::error::{BqDriftError, Result, parse_bq_error, ErrorContext};
use crate::schema::{BqType, Field, FieldMode, Schema, PartitionConfig, PartitionType, ClusterConfig};
use crate::dsl::QueryDef;

#[derive(Clone)]
pub struct BqClient {
    client: Client,
    project_id: String,
}

impl BqClient {
    pub async fn new(project_id: impl Into<String>) -> Result<Self> {
        let client = Client::from_application_default_credentials()
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new().with_operation("client_init");
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        Ok(Self {
            client,
            project_id: project_id.into(),
        })
    }

    pub async fn create_table(&self, query_def: &QueryDef) -> Result<()> {
        let latest = query_def.latest_version()
            .ok_or_else(|| BqDriftError::Schema("No versions defined".into()))?;

        let schema = self.build_table_schema(&latest.schema);
        let time_partitioning = self.build_time_partitioning(&query_def.destination.partition);
        let clustering = query_def.cluster.as_ref().map(|c| self.build_clustering(c));

        let mut table = Table::new(
            &self.project_id,
            &query_def.destination.dataset,
            &query_def.destination.table,
            schema,
        );

        table.time_partitioning = Some(time_partitioning);
        if let Some(c) = clustering {
            table.clustering = Some(c);
        }

        self.client
            .table()
            .create(table)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("create_table")
                    .with_table(&self.project_id, &query_def.destination.dataset, &query_def.destination.table);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        Ok(())
    }

    pub async fn execute_query(&self, sql: &str) -> Result<()> {
        let request = QueryRequest::new(sql);

        self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("execute_query")
                    .with_sql(sql);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        Ok(())
    }

    pub async fn table_exists(&self, dataset: &str, table: &str) -> Result<bool> {
        match self.client.table().get(&self.project_id, dataset, table, None).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn build_table_schema(&self, schema: &Schema) -> TableSchema {
        let fields: Vec<TableFieldSchema> = schema
            .fields
            .iter()
            .map(|f| self.build_field_schema(f))
            .collect();

        TableSchema { fields: Some(fields) }
    }

    fn build_field_schema(&self, field: &Field) -> TableFieldSchema {
        let field_type = self.to_field_type(&field.field_type);
        let mut tfs = TableFieldSchema::new(&field.name, field_type);

        tfs.mode = Some(match field.mode {
            FieldMode::Nullable => "NULLABLE".to_string(),
            FieldMode::Required => "REQUIRED".to_string(),
            FieldMode::Repeated => "REPEATED".to_string(),
        });

        if let Some(desc) = &field.description {
            tfs.description = Some(desc.clone());
        }

        if let Some(nested) = &field.fields {
            tfs.fields = Some(nested.iter().map(|f| self.build_field_schema(f)).collect());
        }

        tfs
    }

    fn to_field_type(&self, bq_type: &BqType) -> FieldType {
        match bq_type {
            BqType::String => FieldType::String,
            BqType::Bytes => FieldType::Bytes,
            BqType::Int64 => FieldType::Int64,
            BqType::Float64 => FieldType::Float64,
            BqType::Numeric => FieldType::Numeric,
            BqType::Bignumeric => FieldType::Bignumeric,
            BqType::Bool => FieldType::Bool,
            BqType::Date => FieldType::Date,
            BqType::Datetime => FieldType::Datetime,
            BqType::Time => FieldType::Time,
            BqType::Timestamp => FieldType::Timestamp,
            BqType::Geography => FieldType::Geography,
            BqType::Json => FieldType::Json,
            BqType::Record => FieldType::Record,
        }
    }

    fn build_time_partitioning(&self, config: &PartitionConfig) -> TimePartitioning {
        let mut tp = TimePartitioning::default();

        tp.r#type = match config.partition_type {
            PartitionType::Hour => "HOUR",
            PartitionType::Day => "DAY",
            PartitionType::Month => "MONTH",
            PartitionType::Year => "YEAR",
            _ => "DAY",
        }.to_string();

        if let Some(field) = &config.field {
            tp.field = Some(field.clone());
        }

        tp
    }

    fn build_clustering(&self, config: &ClusterConfig) -> Clustering {
        Clustering {
            fields: Some(config.fields.clone()),
        }
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Execute a query and return the row count from the first column of the first row.
    /// Useful for COUNT(*) queries or invariant checks.
    pub async fn query_row_count(&self, sql: &str) -> Result<i64> {
        let request = QueryRequest::new(sql);

        let result = self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("query_row_count")
                    .with_sql(sql);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        // Get the first row, first column as integer
        if let Some(rows) = result.rows.as_ref() {
            if let Some(first_row) = rows.first() {
                if let Some(cells) = first_row.columns.as_ref() {
                    if let Some(first_cell) = cells.first() {
                        if let Some(value) = &first_cell.value {
                            if let Some(s) = value.as_str() {
                                return s.parse::<i64>().map_err(|_| {
                                    BqDriftError::Schema(format!("Could not parse count value: {}", s))
                                });
                            } else if let Some(n) = value.as_i64() {
                                return Ok(n);
                            }
                        }
                    }
                }
            }
        }

        Ok(0)
    }

    /// Execute a query and return a single float value from the first column of the first row.
    pub async fn query_single_float(&self, sql: &str) -> Result<Option<f64>> {
        let request = QueryRequest::new(sql);

        let result = self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("query_single_float")
                    .with_sql(sql);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        if let Some(rows) = result.rows.as_ref() {
            if let Some(first_row) = rows.first() {
                if let Some(cells) = first_row.columns.as_ref() {
                    if let Some(first_cell) = cells.first() {
                        if let Some(value) = &first_cell.value {
                            if let Some(s) = value.as_str() {
                                return Ok(Some(s.parse::<f64>().map_err(|_| {
                                    BqDriftError::Schema(format!("Could not parse float value: {}", s))
                                })?));
                            } else if let Some(n) = value.as_f64() {
                                return Ok(Some(n));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Execute a query and return a single integer value from the first column of the first row.
    pub async fn query_single_int(&self, sql: &str) -> Result<Option<i64>> {
        let request = QueryRequest::new(sql);

        let result = self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("query_single_int")
                    .with_sql(sql);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        if let Some(rows) = result.rows.as_ref() {
            if let Some(first_row) = rows.first() {
                if let Some(cells) = first_row.columns.as_ref() {
                    if let Some(first_cell) = cells.first() {
                        if let Some(value) = &first_cell.value {
                            if let Some(s) = value.as_str() {
                                return Ok(Some(s.parse::<i64>().map_err(|_| {
                                    BqDriftError::Schema(format!("Could not parse int value: {}", s))
                                })?));
                            } else if let Some(n) = value.as_i64() {
                                return Ok(Some(n));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Execute a query and return two float values from first two columns of the first row.
    /// Useful for MIN/MAX queries.
    pub async fn query_two_floats(&self, sql: &str) -> Result<(Option<f64>, Option<f64>)> {
        let request = QueryRequest::new(sql);

        let result = self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("query_two_floats")
                    .with_sql(sql);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        let mut first: Option<f64> = None;
        let mut second: Option<f64> = None;

        if let Some(rows) = result.rows.as_ref() {
            if let Some(first_row) = rows.first() {
                if let Some(cells) = first_row.columns.as_ref() {
                    if let Some(cell) = cells.get(0) {
                        if let Some(value) = &cell.value {
                            if let Some(s) = value.as_str() {
                                first = s.parse::<f64>().ok();
                            } else if let Some(n) = value.as_f64() {
                                first = Some(n);
                            }
                        }
                    }
                    if let Some(cell) = cells.get(1) {
                        if let Some(value) = &cell.value {
                            if let Some(s) = value.as_str() {
                                second = s.parse::<f64>().ok();
                            } else if let Some(n) = value.as_f64() {
                                second = Some(n);
                            }
                        }
                    }
                }
            }
        }

        Ok((first, second))
    }

    pub async fn ensure_dataset(&self, dataset: &str) -> Result<()> {
        match self.client.dataset().get(&self.project_id, dataset).await {
            Ok(_) => Ok(()),
            Err(_) => {
                let ds = Dataset::new(&self.project_id, dataset);
                self.client
                    .dataset()
                    .create(ds)
                    .await
                    .map_err(|e| {
                        let ctx = ErrorContext::new()
                            .with_operation("create_dataset");
                        BqDriftError::BigQuery(parse_bq_error(e, ctx))
                    })?;
                Ok(())
            }
        }
    }

    pub async fn drop_table(&self, dataset: &str, table: &str) -> Result<()> {
        match self.client.table().delete(&self.project_id, dataset, table).await {
            Ok(_) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    pub async fn create_table_with_expiration(
        &self,
        dataset: &str,
        table: &str,
        schema: &Schema,
        partition_config: &PartitionConfig,
        cluster_config: Option<&ClusterConfig>,
        expiration: DateTime<Utc>,
    ) -> Result<()> {
        let table_schema = self.build_table_schema(schema);
        let time_partitioning = self.build_time_partitioning(partition_config);
        let clustering = cluster_config.map(|c| self.build_clustering(c));

        let mut tbl = Table::new(
            &self.project_id,
            dataset,
            table,
            table_schema,
        );

        tbl.time_partitioning = Some(time_partitioning);
        if let Some(c) = clustering {
            tbl.clustering = Some(c);
        }
        tbl.expiration_time = Some(expiration.timestamp_millis().to_string());

        self.client
            .table()
            .create(tbl)
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("create_table_with_expiration")
                    .with_table(&self.project_id, dataset, table);
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        Ok(())
    }

    pub async fn list_tables(&self, dataset: &str) -> Result<Vec<String>> {
        let tables = self.client
            .table()
            .list(&self.project_id, dataset, Default::default())
            .await
            .map_err(|e| {
                let ctx = ErrorContext::new()
                    .with_operation("list_tables");
                BqDriftError::BigQuery(parse_bq_error(e, ctx))
            })?;

        let table_names: Vec<String> = tables
            .tables
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.table_reference.table_id)
            .collect();

        Ok(table_names)
    }
}
