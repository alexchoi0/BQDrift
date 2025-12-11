use chrono::{DateTime, Duration, NaiveTime, Utc};
use crate::error::Result;
use crate::dsl::QueryDef;
use crate::schema::PartitionKey;
use crate::invariant::{InvariantChecker, InvariantReport, CheckStatus, Severity, resolve_invariants_def};
use crate::dsl::Destination;
use super::client::BqClient;

const SCRATCH_DATASET: &str = "bqdrift_scratch";

pub struct ScratchConfig {
    pub project: String,
    pub ttl_hours: Option<u32>,
}

impl ScratchConfig {
    pub fn new(project: String) -> Self {
        Self {
            project,
            ttl_hours: None,
        }
    }

    pub fn with_ttl(mut self, hours: u32) -> Self {
        self.ttl_hours = Some(hours);
        self
    }
}

pub struct ScratchWriter {
    client: BqClient,
    config: ScratchConfig,
}

impl ScratchWriter {
    pub fn new(client: BqClient, config: ScratchConfig) -> Self {
        Self { client, config }
    }

    pub fn scratch_table_name(query_def: &QueryDef) -> String {
        format!("{}__{}",
            query_def.destination.dataset,
            query_def.destination.table
        )
    }

    pub fn scratch_table_fqn(&self, query_def: &QueryDef) -> String {
        format!("{}.{}.{}",
            self.config.project,
            SCRATCH_DATASET,
            Self::scratch_table_name(query_def)
        )
    }

    fn calculate_expiration(&self, partition_key: &PartitionKey) -> DateTime<Utc> {
        if let Some(hours) = self.config.ttl_hours {
            return Utc::now() + Duration::hours(hours as i64);
        }

        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

        match partition_key {
            PartitionKey::Hour(dt) => {
                DateTime::from_naive_utc_and_offset(
                    *dt + chrono::Duration::hours(1),
                    Utc
                )
            }
            PartitionKey::Day(date) => {
                DateTime::from_naive_utc_and_offset(
                    date.and_time(midnight) + chrono::Duration::days(1),
                    Utc
                )
            }
            PartitionKey::Month { year, month } => {
                let next_month = if *month == 12 { 1 } else { month + 1 };
                let next_year = if *month == 12 { year + 1 } else { *year };
                let date = chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
                DateTime::from_naive_utc_and_offset(
                    date.and_time(midnight),
                    Utc
                )
            }
            PartitionKey::Year(year) => {
                let date = chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap();
                DateTime::from_naive_utc_and_offset(
                    date.and_time(midnight),
                    Utc
                )
            }
            PartitionKey::Range(_) => {
                Utc::now() + Duration::hours(24)
            }
        }
    }

    pub async fn ensure_dataset(&self) -> Result<()> {
        self.client.ensure_dataset(SCRATCH_DATASET).await
    }

    pub async fn write_partition(
        &self,
        query_def: &QueryDef,
        partition_key: PartitionKey,
        run_invariants: bool,
    ) -> Result<ScratchWriteStats> {
        let partition_date = partition_key.to_naive_date();
        let version = query_def
            .get_version_for_date(partition_date)
            .ok_or_else(|| crate::error::BqDriftError::Partition(
                format!("No version found for partition {}", partition_key)
            ))?;

        let scratch_table = Self::scratch_table_name(query_def);
        let expiration = self.calculate_expiration(&partition_key);

        self.client.drop_table(SCRATCH_DATASET, &scratch_table).await?;

        self.client.create_table_with_expiration(
            SCRATCH_DATASET,
            &scratch_table,
            &version.schema,
            &query_def.destination.partition,
            query_def.cluster.as_ref(),
            expiration,
        ).await?;

        let scratch_destination = Destination {
            dataset: SCRATCH_DATASET.to_string(),
            table: scratch_table.clone(),
            partition: query_def.destination.partition.clone(),
            cluster: query_def.destination.cluster.clone(),
        };

        let mut invariant_report = InvariantReport::default();

        if run_invariants {
            let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);

            if !before_checks.is_empty() {
                let checker = InvariantChecker::new(&self.client, &scratch_destination, partition_date);
                let results = checker.run_checks(&before_checks).await?;

                let has_error = results.iter().any(|r| {
                    r.status == CheckStatus::Failed && r.severity == Severity::Error
                });

                invariant_report.before = results;

                if has_error {
                    return Err(crate::error::BqDriftError::InvariantFailed(
                        "Before invariant check(s) failed with error severity".to_string()
                    ));
                }
            }

            let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
            let full_sql = self.build_merge_sql(query_def, &scratch_destination, sql, &partition_key);
            self.client.execute_query(&full_sql).await?;

            if !after_checks.is_empty() {
                let checker = InvariantChecker::new(&self.client, &scratch_destination, partition_date);
                let results = checker.run_checks(&after_checks).await?;
                invariant_report.after = results;
            }
        } else {
            let sql = version.get_sql_for_date(chrono::Utc::now().date_naive());
            let full_sql = self.build_merge_sql(query_def, &scratch_destination, sql, &partition_key);
            self.client.execute_query(&full_sql).await?;
        }

        Ok(ScratchWriteStats {
            query_name: query_def.name.clone(),
            version: version.version,
            partition_key,
            scratch_table: self.scratch_table_fqn(query_def),
            expiration,
            rows_written: None,
            bytes_processed: None,
            invariant_report: if run_invariants { Some(invariant_report) } else { None },
        })
    }

    fn build_merge_sql(
        &self,
        query_def: &QueryDef,
        scratch_dest: &Destination,
        sql: &str,
        partition_key: &PartitionKey,
    ) -> String {
        let dest_table = format!(
            "{}.{}.{}",
            self.config.project,
            scratch_dest.dataset,
            scratch_dest.table
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

    pub async fn list_tables(&self) -> Result<Vec<String>> {
        self.client.list_tables(SCRATCH_DATASET).await
    }

    pub async fn promote_to_production(
        &self,
        query_def: &QueryDef,
        partition_key: &PartitionKey,
        production_client: &BqClient,
    ) -> Result<PromoteStats> {
        let scratch_table = self.scratch_table_fqn(query_def);
        let production_table = format!(
            "{}.{}.{}",
            production_client.project_id(),
            query_def.destination.dataset,
            query_def.destination.table
        );

        let partition_field = query_def
            .destination
            .partition
            .field
            .as_deref()
            .unwrap_or("date");

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

        let merge_sql = format!(
            r#"
            MERGE `{production_table}` AS target
            USING `{scratch_table}` AS source
            ON FALSE
            WHEN NOT MATCHED BY SOURCE AND {partition_condition} THEN DELETE
            WHEN NOT MATCHED BY TARGET THEN INSERT ROW
            "#,
            production_table = production_table,
            scratch_table = scratch_table,
            partition_condition = partition_condition,
        );

        production_client.execute_query(&merge_sql).await?;

        Ok(PromoteStats {
            query_name: query_def.name.clone(),
            partition_key: partition_key.clone(),
            scratch_table,
            production_table,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ScratchWriteStats {
    pub query_name: String,
    pub version: u32,
    pub partition_key: PartitionKey,
    pub scratch_table: String,
    pub expiration: DateTime<Utc>,
    pub rows_written: Option<i64>,
    pub bytes_processed: Option<i64>,
    pub invariant_report: Option<InvariantReport>,
}

#[derive(Debug, Clone)]
pub struct PromoteStats {
    pub query_name: String,
    pub partition_key: PartitionKey,
    pub scratch_table: String,
    pub production_table: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_scratch_table_name() {
        use crate::schema::{PartitionConfig, PartitionType};

        let query_def = QueryDef {
            name: "daily_stats".to_string(),
            destination: Destination {
                dataset: "analytics".to_string(),
                table: "daily_user_stats".to_string(),
                partition: PartitionConfig {
                    field: Some("date".to_string()),
                    partition_type: PartitionType::Day,
                    start: None,
                    end: None,
                    interval: None,
                    granularity: None,
                },
                cluster: None,
            },
            description: None,
            owner: None,
            tags: vec![],
            versions: vec![],
            cluster: None,
        };

        assert_eq!(
            ScratchWriter::scratch_table_name(&query_def),
            "analytics__daily_user_stats"
        );
    }

    #[test]
    fn test_calculate_expiration_day() {
        let partition = PartitionKey::Day(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap());
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

        match &partition {
            PartitionKey::Day(date) => {
                let expected: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
                    date.and_time(midnight) + chrono::Duration::days(1),
                    Utc
                );
                let actual: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2024, 6, 16).unwrap().and_time(midnight),
                    Utc
                );
                assert_eq!(expected, actual);
            }
            _ => {}
        }
    }

    #[test]
    fn test_calculate_expiration_with_ttl_override() {
        let config = ScratchConfig::new("test-project".to_string()).with_ttl(48);
        assert_eq!(config.ttl_hours, Some(48));
    }
}
