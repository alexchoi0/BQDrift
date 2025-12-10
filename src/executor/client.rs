use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use gcp_bigquery_client::model::time_partitioning::TimePartitioning;
use gcp_bigquery_client::model::clustering::Clustering;
use crate::error::{BqDriftError, Result};
use crate::schema::{BqType, Field, FieldMode, Schema, PartitionConfig, PartitionType, ClusterConfig};
use crate::dsl::QueryDef;

pub struct BqClient {
    client: Client,
    project_id: String,
}

impl BqClient {
    pub async fn new(project_id: impl Into<String>) -> Result<Self> {
        let client = Client::from_application_default_credentials()
            .await
            .map_err(|e| BqDriftError::Client(e.to_string()))?;

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
            .map_err(|e| BqDriftError::Client(e.to_string()))?;

        Ok(())
    }

    pub async fn execute_query(&self, sql: &str) -> Result<()> {
        let request = QueryRequest::new(sql);

        self.client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(|e| BqDriftError::Client(e.to_string()))?;

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
}
