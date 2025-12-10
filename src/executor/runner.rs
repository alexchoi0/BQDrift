use chrono::{NaiveDate, Utc};
use crate::error::Result;
use crate::dsl::QueryDef;
use super::client::BqClient;
use super::partition_writer::{PartitionWriter, WriteStats};

#[derive(Debug)]
pub struct RunReport {
    pub stats: Vec<WriteStats>,
    pub failures: Vec<RunFailure>,
}

#[derive(Debug)]
pub struct RunFailure {
    pub query_name: String,
    pub partition_date: NaiveDate,
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
        let mut stats = Vec::new();
        let mut failures = Vec::new();

        for query in &self.queries {
            match self.writer.write_partition(query, date).await {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(RunFailure {
                    query_name: query.name.clone(),
                    partition_date: date,
                    error: e.to_string(),
                }),
            }
        }

        Ok(RunReport { stats, failures })
    }

    pub async fn run_query(&self, query_name: &str, date: NaiveDate) -> Result<WriteStats> {
        let query = self.queries
            .iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| crate::error::BqDriftError::DslParse(
                format!("Query '{}' not found", query_name)
            ))?;

        self.writer.write_partition(query, date).await
    }

    pub async fn backfill(
        &self,
        query_name: &str,
        from: NaiveDate,
        to: NaiveDate,
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
            match self.writer.write_partition(query, current).await {
                Ok(s) => stats.push(s),
                Err(e) => failures.push(RunFailure {
                    query_name: query_name.to_string(),
                    partition_date: current,
                    error: e.to_string(),
                }),
            }
            current = current.succ_opt().unwrap_or(current);
        }

        Ok(RunReport { stats, failures })
    }

    pub fn queries(&self) -> &[QueryDef] {
        &self.queries
    }
}
