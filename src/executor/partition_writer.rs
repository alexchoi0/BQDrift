use chrono::NaiveDate;
use crate::error::{BqDriftError, Result};
use crate::dsl::{QueryDef, VersionDef};
use super::client::BqClient;

#[derive(Debug, Clone)]
pub struct WriteStats {
    pub query_name: String,
    pub version: u32,
    pub partition_date: NaiveDate,
    pub rows_written: Option<i64>,
    pub bytes_processed: Option<i64>,
}

pub struct PartitionWriter {
    client: BqClient,
}

impl PartitionWriter {
    pub fn new(client: BqClient) -> Self {
        Self { client }
    }

    pub async fn write_partition(
        &self,
        query_def: &QueryDef,
        partition_date: NaiveDate,
    ) -> Result<WriteStats> {
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| BqDriftError::Partition(
                format!("No version found for date {}", partition_date)
            ))?;

        let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
        let full_sql = self.build_merge_sql(query_def, version, sql, partition_date);

        self.client.execute_query(&full_sql).await?;

        Ok(WriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_date,
            rows_written: None,
            bytes_processed: None,
        })
    }

    fn build_merge_sql(
        &self,
        query_def: &QueryDef,
        _version: &VersionDef,
        sql: &str,
        partition_date: NaiveDate,
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

        let parameterized_sql = sql.replace("@partition_date", &format!("'{}'", partition_date));

        format!(
            r#"
            MERGE `{dest_table}` AS target
            USING (
                {parameterized_sql}
            ) AS source
            ON FALSE
            WHEN NOT MATCHED BY SOURCE AND target.{partition_field} = '{partition_date}' THEN DELETE
            WHEN NOT MATCHED BY TARGET THEN INSERT ROW
            "#,
            dest_table = dest_table,
            parameterized_sql = parameterized_sql,
            partition_field = partition_field,
            partition_date = partition_date,
        )
    }

    pub async fn write_partition_truncate(
        &self,
        query_def: &QueryDef,
        partition_date: NaiveDate,
    ) -> Result<WriteStats> {
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| BqDriftError::Partition(
                format!("No version found for date {}", partition_date)
            ))?;

        let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());

        let dest_table = format!(
            "{}.{}${}",
            query_def.destination.dataset,
            query_def.destination.table,
            partition_date.format("%Y%m%d")
        );

        let parameterized_sql = sql.replace("@partition_date", &format!("'{}'", partition_date));

        let insert_sql = format!(
            r#"
            INSERT INTO `{dest_table}`
            {parameterized_sql}
            "#,
            dest_table = dest_table,
            parameterized_sql = parameterized_sql,
        );

        let delete_sql = format!(
            "DELETE FROM `{}` WHERE TRUE",
            dest_table
        );

        self.client.execute_query(&delete_sql).await?;
        self.client.execute_query(&insert_sql).await?;

        Ok(WriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_date,
            rows_written: None,
            bytes_processed: None,
        })
    }
}
