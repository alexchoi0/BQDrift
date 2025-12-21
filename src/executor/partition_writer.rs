use crate::error::{BqDriftError, Result};
use crate::dsl::{QueryDef, VersionDef};
use crate::schema::PartitionKey;
use crate::invariant::{
    InvariantChecker, InvariantReport, CheckStatus, Severity,
    resolve_invariants_def,
};
use super::client::BqClient;

#[derive(Debug, Clone)]
pub struct PartitionWriteStats {
    pub query_name: String,
    pub version: u32,
    pub partition_key: PartitionKey,
    pub rows_written: Option<i64>,
    pub bytes_processed: Option<i64>,
    pub invariant_report: Option<InvariantReport>,
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
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_with_invariants(query_def, partition_key, true).await
    }

    pub async fn write_partition_skip_invariants(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_with_invariants(query_def, partition_key, false).await
    }

    async fn write_partition_with_invariants(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
        run_invariants: bool,
    ) -> Result<PartitionWriteStats> {
        let partition_date = partition_key.to_naive_date();
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| BqDriftError::Partition(
                format!("No version found for partition {}", partition_key)
            ))?;

        let mut invariant_report = InvariantReport::default();

        if run_invariants {
            let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);

            if !before_checks.is_empty() {
                let checker = InvariantChecker::new(&self.client, &query_def.destination, partition_date);
                let results = checker.run_checks(&before_checks).await?;

                let has_error = results.iter().any(|r| {
                    r.status == CheckStatus::Failed && r.severity == Severity::Error
                });

                invariant_report.before = results;

                if has_error {
                    return Err(BqDriftError::InvariantFailed(
                        "Before invariant check(s) failed with error severity".to_string()
                    ));
                }
            }

            let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
            let full_sql = self.build_merge_sql(query_def, version, sql, &partition_key);
            self.client.execute_query(&full_sql).await?;

            if !after_checks.is_empty() {
                let checker = InvariantChecker::new(&self.client, &query_def.destination, partition_date);
                let results = checker.run_checks(&after_checks).await?;
                invariant_report.after = results;
            }
        } else {
            let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
            let full_sql = self.build_merge_sql(query_def, version, sql, &partition_key);
            self.client.execute_query(&full_sql).await?;
        }

        Ok(PartitionWriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_key,
            rows_written: None,
            bytes_processed: None,
            invariant_report: if run_invariants { Some(invariant_report) } else { None },
        })
    }

    fn build_merge_sql(
        &self,
        query_def: &QueryDef,
        _version: &VersionDef,
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

    pub async fn write_partition_truncate(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_truncate_with_invariants(query_def, partition_key, true).await
    }

    pub async fn write_partition_truncate_skip_invariants(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
    ) -> Result<PartitionWriteStats> {
        self.write_partition_truncate_with_invariants(query_def, partition_key, false).await
    }

    async fn write_partition_truncate_with_invariants(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
        run_invariants: bool,
    ) -> Result<PartitionWriteStats> {
        let partition_date = partition_key.to_naive_date();
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| BqDriftError::Partition(
                format!("No version found for partition {}", partition_key)
            ))?;

        let mut invariant_report = InvariantReport::default();

        let dest_table = format!(
            "{}.{}{}",
            query_def.destination.dataset,
            query_def.destination.table,
            partition_key.decorator()
        );

        if run_invariants {
            let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);

            if !before_checks.is_empty() {
                let checker = InvariantChecker::new(&self.client, &query_def.destination, partition_date);
                let results = checker.run_checks(&before_checks).await?;

                let has_error = results.iter().any(|r| {
                    r.status == CheckStatus::Failed && r.severity == Severity::Error
                });

                invariant_report.before = results;

                if has_error {
                    return Err(BqDriftError::InvariantFailed(
                        "Before invariant check(s) failed with error severity".to_string()
                    ));
                }
            }

            let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
            let parameterized_sql = sql.replace("@partition_date", &format!("'{}'", partition_key.sql_value()));

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

            if !after_checks.is_empty() {
                let checker = InvariantChecker::new(&self.client, &query_def.destination, partition_date);
                let results = checker.run_checks(&after_checks).await?;
                invariant_report.after = results;
            }
        } else {
            let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
            let parameterized_sql = sql.replace("@partition_date", &format!("'{}'", partition_key.sql_value()));

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
        }

        Ok(PartitionWriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_key,
            rows_written: None,
            bytes_processed: None,
            invariant_report: if run_invariants { Some(invariant_report) } else { None },
        })
    }
}
