use chrono::NaiveDate;
use crate::error::Result;
use crate::dsl::Destination;
use crate::executor::BqClient;
use super::types::{Severity, InvariantsDef, InvariantDef, InvariantCheck, SqlSource};
use super::result::CheckResult;

pub struct ResolvedInvariant {
    pub name: String,
    pub description: Option<String>,
    pub severity: Severity,
    pub check: ResolvedCheck,
}

pub enum ResolvedCheck {
    ZeroRows {
        sql_content: String,
    },
    RowCount {
        source_sql: Option<String>,
        min: Option<i64>,
        max: Option<i64>,
    },
    NullPercentage {
        source_sql: Option<String>,
        column: String,
        max_percentage: f64,
    },
    ColumnCheck {
        source_sql: Option<String>,
        column: String,
        check: String,
    },
    DistinctCount {
        source_sql: Option<String>,
        column: String,
        min: Option<i64>,
        max: Option<i64>,
    },
}

pub struct InvariantChecker<'a> {
    client: &'a BqClient,
    destination: &'a Destination,
    partition_date: NaiveDate,
}

impl<'a> InvariantChecker<'a> {
    pub fn new(
        client: &'a BqClient,
        destination: &'a Destination,
        partition_date: NaiveDate,
    ) -> Self {
        Self {
            client,
            destination,
            partition_date,
        }
    }

    pub async fn run_checks(&self, invariants: &[ResolvedInvariant]) -> Result<Vec<CheckResult>> {
        let mut results = Vec::new();

        for inv in invariants {
            let result = self.run_check(inv).await?;
            results.push(result);
        }

        Ok(results)
    }

    async fn run_check(&self, inv: &ResolvedInvariant) -> Result<CheckResult> {
        match &inv.check {
            ResolvedCheck::ZeroRows { sql_content } => {
                self.check_zero_rows(&inv.name, inv.severity, sql_content).await
            }
            ResolvedCheck::RowCount { source_sql, min, max } => {
                self.check_row_count(&inv.name, inv.severity, source_sql.as_deref(), *min, *max).await
            }
            ResolvedCheck::NullPercentage { source_sql, column, max_percentage } => {
                self.check_null_percentage(&inv.name, inv.severity, source_sql.as_deref(), column, *max_percentage).await
            }
            ResolvedCheck::ColumnCheck { source_sql, column, check } => {
                self.check_column(&inv.name, inv.severity, source_sql.as_deref(), column, check).await
            }
            ResolvedCheck::DistinctCount { source_sql, column, min, max } => {
                self.check_distinct_count(&inv.name, inv.severity, source_sql.as_deref(), column, *min, *max).await
            }
        }
    }

    fn destination_table(&self) -> String {
        format!("`{}.{}`", self.destination.dataset, self.destination.table)
    }

    fn default_source_sql(&self) -> String {
        let partition_field = self.destination.partition.field.as_deref().unwrap_or("date");
        format!(
            "SELECT * FROM {} WHERE {} = '{}'",
            self.destination_table(),
            partition_field,
            self.partition_date
        )
    }

    fn resolve_placeholders(&self, sql: &str) -> String {
        sql.replace("{destination}", &self.destination_table())
           .replace("@partition_date", &format!("'{}'", self.partition_date))
    }

    async fn check_zero_rows(
        &self,
        name: &str,
        severity: Severity,
        sql_content: &str,
    ) -> Result<CheckResult> {
        let sql = self.resolve_placeholders(sql_content);
        let count = self.client.query_row_count(&sql).await?;

        if count == 0 {
            Ok(CheckResult::passed(name, severity, "Query returned 0 rows"))
        } else {
            Ok(CheckResult::failed(name, severity, format!("Query returned {} rows (expected 0)", count))
                .with_details(format!("Violation count: {}", count)))
        }
    }

    async fn check_row_count(
        &self,
        name: &str,
        severity: Severity,
        source_sql: Option<&str>,
        min: Option<i64>,
        max: Option<i64>,
    ) -> Result<CheckResult> {
        let source = source_sql
            .map(|s| self.resolve_placeholders(s))
            .unwrap_or_else(|| self.default_source_sql());

        let count_sql = format!("SELECT COUNT(*) as cnt FROM ({}) _source", source);
        let count = self.client.query_row_count(&count_sql).await?;

        let mut violations = Vec::new();
        if let Some(min_val) = min {
            if count < min_val {
                violations.push(format!("count {} < min {}", count, min_val));
            }
        }
        if let Some(max_val) = max {
            if count > max_val {
                violations.push(format!("count {} > max {}", count, max_val));
            }
        }

        if violations.is_empty() {
            Ok(CheckResult::passed(name, severity, format!("Row count: {}", count)))
        } else {
            Ok(CheckResult::failed(name, severity, violations.join(", "))
                .with_details(format!("Actual row count: {}", count)))
        }
    }

    async fn check_null_percentage(
        &self,
        name: &str,
        severity: Severity,
        source_sql: Option<&str>,
        column: &str,
        max_percentage: f64,
    ) -> Result<CheckResult> {
        let source = source_sql
            .map(|s| self.resolve_placeholders(s))
            .unwrap_or_else(|| self.default_source_sql());

        let check_sql = format!(
            "SELECT COUNTIF({} IS NULL) * 100.0 / NULLIF(COUNT(*), 0) as null_pct FROM ({}) _source",
            column, source
        );

        let null_pct = self.client.query_single_float(&check_sql).await?.unwrap_or(0.0);

        if null_pct <= max_percentage {
            Ok(CheckResult::passed(name, severity, format!("Null percentage: {:.2}%", null_pct)))
        } else {
            Ok(CheckResult::failed(
                name,
                severity,
                format!("Null percentage {:.2}% > max {:.2}%", null_pct, max_percentage),
            ).with_details(format!("Column: {}, Actual: {:.2}%", column, null_pct)))
        }
    }

    async fn check_column(
        &self,
        name: &str,
        severity: Severity,
        source_sql: Option<&str>,
        column: &str,
        check_expr: &str,
    ) -> Result<CheckResult> {
        let source = source_sql
            .map(|s| self.resolve_placeholders(s))
            .unwrap_or_else(|| self.default_source_sql());

        let resolved_check = check_expr.replace("{column}", column);

        let check_sql = format!(
            "SELECT CASE WHEN {} THEN 1 ELSE 0 END as result FROM ({}) _source",
            resolved_check, source
        );

        let result = self.client.query_single_int(&check_sql).await?.unwrap_or(0);

        if result == 1 {
            Ok(CheckResult::passed(name, severity, format!("Column check passed: {}", resolved_check)))
        } else {
            Ok(CheckResult::failed(
                name,
                severity,
                format!("Column check failed: {}", resolved_check),
            ).with_details(format!("Column: {}, Expression: {}", column, resolved_check)))
        }
    }

    async fn check_distinct_count(
        &self,
        name: &str,
        severity: Severity,
        source_sql: Option<&str>,
        column: &str,
        min: Option<i64>,
        max: Option<i64>,
    ) -> Result<CheckResult> {
        let source = source_sql
            .map(|s| self.resolve_placeholders(s))
            .unwrap_or_else(|| self.default_source_sql());

        let check_sql = format!(
            "SELECT COUNT(DISTINCT {}) as cnt FROM ({}) _source",
            column, source
        );

        let count = self.client.query_row_count(&check_sql).await?;

        let mut violations = Vec::new();
        if let Some(min_val) = min {
            if count < min_val {
                violations.push(format!("distinct count {} < min {}", count, min_val));
            }
        }
        if let Some(max_val) = max {
            if count > max_val {
                violations.push(format!("distinct count {} > max {}", count, max_val));
            }
        }

        if violations.is_empty() {
            Ok(CheckResult::passed(name, severity, format!("Distinct count for {}: {}", column, count)))
        } else {
            Ok(CheckResult::failed(name, severity, violations.join(", "))
                .with_details(format!("Column: {}, Actual distinct count: {}", column, count)))
        }
    }
}

pub fn resolve_invariants_def(def: &InvariantsDef) -> (Vec<ResolvedInvariant>, Vec<ResolvedInvariant>) {
    let before = def.before.iter().map(resolve_invariant_def).collect();
    let after = def.after.iter().map(resolve_invariant_def).collect();
    (before, after)
}

fn resolve_invariant_def(inv: &InvariantDef) -> ResolvedInvariant {
    ResolvedInvariant {
        name: inv.name.clone(),
        description: inv.description.clone(),
        severity: inv.severity,
        check: resolve_check(&inv.check),
    }
}

fn resolve_check(check: &InvariantCheck) -> ResolvedCheck {
    match check {
        InvariantCheck::ZeroRows { source } => {
            let sql_content = match source {
                SqlSource::Source(path) => path.clone(),
                SqlSource::SourceInline(sql) => sql.clone(),
            };
            ResolvedCheck::ZeroRows { sql_content }
        }
        InvariantCheck::RowCount { source, min, max } => {
            let source_sql = source.as_ref().map(|s| match s {
                SqlSource::Source(path) => path.clone(),
                SqlSource::SourceInline(sql) => sql.clone(),
            });
            ResolvedCheck::RowCount {
                source_sql,
                min: *min,
                max: *max,
            }
        }
        InvariantCheck::NullPercentage { source, column, max_percentage } => {
            let source_sql = source.as_ref().map(|s| match s {
                SqlSource::Source(path) => path.clone(),
                SqlSource::SourceInline(sql) => sql.clone(),
            });
            ResolvedCheck::NullPercentage {
                source_sql,
                column: column.clone(),
                max_percentage: *max_percentage,
            }
        }
        InvariantCheck::ColumnCheck { source, column, check } => {
            let source_sql = source.as_ref().map(|s| match s {
                SqlSource::Source(path) => path.clone(),
                SqlSource::SourceInline(sql) => sql.clone(),
            });
            ResolvedCheck::ColumnCheck {
                source_sql,
                column: column.clone(),
                check: check.clone(),
            }
        }
        InvariantCheck::DistinctCount { source, column, min, max } => {
            let source_sql = source.as_ref().map(|s| match s {
                SqlSource::Source(path) => path.clone(),
                SqlSource::SourceInline(sql) => sql.clone(),
            });
            ResolvedCheck::DistinctCount {
                source_sql,
                column: column.clone(),
                min: *min,
                max: *max,
            }
        }
    }
}
