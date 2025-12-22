pub use bq_runner::{Executor, ExecutorMode, QueryResult, ColumnDef, ColumnInfo};

use chrono::{NaiveDate, Utc};
use crate::error::{BqDriftError, Result};
use crate::dsl::QueryDef;
use crate::schema::PartitionKey;

#[derive(Debug)]
pub struct ExecutorRunReport {
    pub stats: Vec<ExecutorWriteStats>,
    pub failures: Vec<ExecutorRunFailure>,
}

#[derive(Debug)]
pub struct ExecutorWriteStats {
    pub query_name: String,
    pub partition_key: PartitionKey,
    pub rows_affected: u64,
}

#[derive(Debug)]
pub struct ExecutorRunFailure {
    pub query_name: String,
    pub partition_key: PartitionKey,
    pub error: String,
}

pub struct ExecutorRunner<'a> {
    executor: &'a Executor,
    queries: Vec<QueryDef>,
}

impl<'a> ExecutorRunner<'a> {
    pub fn new(executor: &'a Executor, queries: Vec<QueryDef>) -> Self {
        Self { executor, queries }
    }

    pub fn mode(&self) -> ExecutorMode {
        self.executor.mode()
    }

    pub async fn run_today(&self) -> Result<ExecutorRunReport> {
        let today = Utc::now().date_naive();
        self.run_for_date(today).await
    }

    pub async fn run_for_date(&self, date: NaiveDate) -> Result<ExecutorRunReport> {
        self.run_for_partition(PartitionKey::Day(date)).await
    }

    pub async fn run_for_partition(&self, partition_key: PartitionKey) -> Result<ExecutorRunReport> {
        let mut stats = Vec::new();
        let mut failures = Vec::new();

        for query in &self.queries {
            match self.execute_query(query, partition_key.clone()).await {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(ExecutorRunFailure {
                    query_name: query.name.clone(),
                    partition_key: partition_key.clone(),
                    error: e.to_string(),
                }),
            }
        }

        Ok(ExecutorRunReport { stats, failures })
    }

    pub async fn run_query(&self, query_name: &str, date: NaiveDate) -> Result<ExecutorWriteStats> {
        self.run_query_partition(query_name, PartitionKey::Day(date)).await
    }

    pub async fn run_query_partition(&self, query_name: &str, partition_key: PartitionKey) -> Result<ExecutorWriteStats> {
        let query = self.queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| BqDriftError::DslParse(
                format!("Query '{}' not found", query_name)
            ))?;

        self.execute_query(query, partition_key).await
    }

    pub async fn backfill(
        &self,
        query_name: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<ExecutorRunReport> {
        self.backfill_partitions(
            query_name,
            PartitionKey::Day(from),
            PartitionKey::Day(to),
            None,
        ).await
    }

    pub async fn backfill_partitions(
        &self,
        query_name: &str,
        from: PartitionKey,
        to: PartitionKey,
        interval: Option<i64>,
    ) -> Result<ExecutorRunReport> {
        let query = self.queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| BqDriftError::DslParse(
                format!("Query '{}' not found", query_name)
            ))?;

        let mut stats = Vec::new();
        let mut failures = Vec::new();
        let mut current = from;

        while current <= to {
            match self.execute_query(query, current.clone()).await {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(ExecutorRunFailure {
                    query_name: query_name.to_string(),
                    partition_key: current.clone(),
                    error: e.to_string(),
                }),
            }
            current = match interval {
                Some(i) => current.next_by(i),
                None => current.next(),
            };
        }

        Ok(ExecutorRunReport { stats, failures })
    }

    pub fn queries(&self) -> &[QueryDef] {
        &self.queries
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<u64> {
        self.executor
            .execute(sql)
            .await
            .map_err(|e| BqDriftError::Executor(e.to_string()))
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.executor
            .query(sql)
            .await
            .map_err(|e| BqDriftError::Executor(e.to_string()))
    }

    async fn execute_query(&self, query_def: &QueryDef, partition_key: PartitionKey) -> Result<ExecutorWriteStats> {
        let partition_date = partition_key.to_naive_date();
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| BqDriftError::Partition(
                format!("No version found for partition {}", partition_key)
            ))?;

        let sql = version.get_sql_for_date(Utc::now().date_naive());
        let full_sql = self.build_merge_sql(query_def, sql, &partition_key);

        let rows_affected = self.executor
            .execute(&full_sql)
            .await
            .map_err(|e| BqDriftError::Executor(e.to_string()))?;

        Ok(ExecutorWriteStats {
            query_name: query_def.name.clone(),
            partition_key,
            rows_affected,
        })
    }

    fn build_merge_sql(
        &self,
        query_def: &QueryDef,
        sql: &str,
        partition_key: &PartitionKey,
    ) -> String {
        let dest_table = format!(
            "{}.{}",
            query_def.destination.dataset,
            query_def.destination.table
        );

        let partition_field = query_def
            .destination
            .partition
            .field
            .as_deref()
            .unwrap_or("date");

        let parameterized_sql = sql.replace("@partition_date", &format!("'{}'", partition_key.sql_value()));

        let partition_condition = match partition_key {
            PartitionKey::Hour(_) => format!(
                "TIMESTAMP_TRUNC(target.{}, HOUR) = {}",
                partition_field,
                partition_key.sql_literal()
            ),
            PartitionKey::Day(_) => format!(
                "target.{} = {}",
                partition_field,
                partition_key.sql_literal()
            ),
            PartitionKey::Month { .. } => format!(
                "DATE_TRUNC(target.{}, MONTH) = {}",
                partition_field,
                partition_key.sql_literal()
            ),
            PartitionKey::Year(_) => format!(
                "DATE_TRUNC(target.{}, YEAR) = {}",
                partition_field,
                partition_key.sql_literal()
            ),
            PartitionKey::Range(_) => format!(
                "target.{} = {}",
                partition_field,
                partition_key.sql_literal()
            ),
        };

        format!(
            r#"
            MERGE `{dest_table}` AS target
            USING (
                {parameterized_sql}
            ) AS source
            ON FALSE
            WHEN NOT MATCHED BY SOURCE AND {partition_condition} THEN DELETE
            WHEN NOT MATCHED BY TARGET THEN INSERT ROW
            "#,
            dest_table = dest_table,
            parameterized_sql = parameterized_sql,
            partition_condition = partition_condition,
        )
    }
}

pub fn create_mock_executor() -> Result<Executor> {
    Executor::mock().map_err(|e| BqDriftError::Executor(e.to_string()))
}

pub async fn create_bigquery_executor() -> Result<Executor> {
    Executor::bigquery()
        .await
        .map_err(|e| BqDriftError::Executor(e.to_string()))
}
