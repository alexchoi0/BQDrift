use chrono::{NaiveDate, Utc};
use crate::error::Result;
use crate::dsl::QueryDef;
use crate::schema::PartitionKey;
use super::client::BqClient;
use super::partition_writer::{PartitionWriter, PartitionWriteStats};

#[derive(Debug)]
pub struct RunReport {
    pub stats: Vec<PartitionWriteStats>,
    pub failures: Vec<RunFailure>,
}

#[derive(Debug)]
pub struct RunFailure {
    pub query_name: String,
    pub partition_key: PartitionKey,
    pub error: String,
}

pub struct Runner {
    writer: PartitionWriter,
    queries: Vec<QueryDef>,
}

impl Runner {
    pub fn new(client: BqClient, queries: Vec<QueryDef>) -> Self {
        Self {
            writer: PartitionWriter::new(client),
            queries,
        }
    }

    pub async fn run_today(&self) -> Result<RunReport> {
        let today = Utc::now().date_naive();
        self.run_for_date(today).await
    }

    pub async fn run_for_date(&self, date: NaiveDate) -> Result<RunReport> {
        self.run_for_partition(PartitionKey::Day(date)).await
    }

    pub async fn run_for_partition(&self, partition_key: PartitionKey) -> Result<RunReport> {
        let mut stats = Vec::new();
        let mut failures = Vec::new();

        for query in &self.queries {
            match self.writer.write_partition(query, partition_key.clone()).await {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(RunFailure {
                    query_name: query.name.clone(),
                    partition_key: partition_key.clone(),
                    error: e.to_string(),
                }),
            }
        }

        Ok(RunReport { stats, failures })
    }

    pub async fn run_query(&self, query_name: &str, date: NaiveDate) -> Result<PartitionWriteStats> {
        self.run_query_partition(query_name, PartitionKey::Day(date)).await
    }

    pub async fn run_query_partition(&self, query_name: &str, partition_key: PartitionKey) -> Result<PartitionWriteStats> {
        let query = self.queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| crate::error::BqDriftError::DslParse(
                format!("Query '{}' not found", query_name)
            ))?;

        self.writer.write_partition(query, partition_key).await
    }

    pub async fn backfill(
        &self,
        query_name: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<RunReport> {
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
    ) -> Result<RunReport> {
        let query = self.queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| crate::error::BqDriftError::DslParse(
                format!("Query '{}' not found", query_name)
            ))?;

        let mut stats = Vec::new();
        let mut failures = Vec::new();
        let mut current = from;

        while current <= to {
            match self.writer.write_partition(query, current.clone()).await {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(RunFailure {
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

        Ok(RunReport { stats, failures })
    }

    pub fn queries(&self) -> &[QueryDef] {
        &self.queries
    }
}
