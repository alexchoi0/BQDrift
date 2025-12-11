use std::path::PathBuf;
use chrono::{Datelike, Timelike, NaiveDate, Utc};
use crate::error::{BqDriftError, Result};
use crate::dsl::{QueryDef, QueryLoader, QueryValidator};
use crate::schema::{PartitionKey, PartitionType};
use crate::executor::BqClient;
use crate::invariant::{InvariantChecker, CheckStatus, Severity, resolve_invariants_def};
use super::commands::{ReplCommand, ReplResult};

pub struct ReplSession {
    project: Option<String>,
    queries_path: PathBuf,
    loader: QueryLoader,
    cached_queries: Option<Vec<QueryDef>>,
    client: Option<BqClient>,
}

impl ReplSession {
    pub fn new(project: Option<String>, queries_path: PathBuf) -> Self {
        Self {
            project,
            queries_path,
            loader: QueryLoader::new(),
            cached_queries: None,
            client: None,
        }
    }

    pub fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }

    pub fn set_project(&mut self, project: String) {
        self.project = Some(project);
        self.client = None;
    }

    pub fn query_names(&self) -> Vec<String> {
        self.cached_queries
            .as_ref()
            .map(|qs| qs.iter().map(|q| q.name.clone()).collect())
            .unwrap_or_default()
    }

    pub fn queries(&self) -> Option<&Vec<QueryDef>> {
        self.cached_queries.as_ref()
    }

    fn ensure_queries(&mut self) -> Result<&Vec<QueryDef>> {
        if self.cached_queries.is_none() {
            let queries = self.loader.load_dir(&self.queries_path)?;
            self.cached_queries = Some(queries);
        }
        Ok(self.cached_queries.as_ref().unwrap())
    }

    async fn ensure_client(&mut self) -> Result<&BqClient> {
        let project = self.project.as_ref()
            .ok_or_else(|| BqDriftError::Repl("No project set. Use --project flag or set GCP_PROJECT_ID".to_string()))?;

        if self.client.is_none() {
            let client = BqClient::new(project).await?;
            self.client = Some(client);
        }
        Ok(self.client.as_ref().unwrap())
    }

    pub fn reload_queries(&mut self) -> Result<usize> {
        let queries = self.loader.load_dir(&self.queries_path)?;
        let count = queries.len();
        self.cached_queries = Some(queries);
        Ok(count)
    }

    pub async fn execute(&mut self, cmd: ReplCommand) -> ReplResult {
        match cmd {
            ReplCommand::Exit => ReplResult::empty_success(),
            ReplCommand::Help => self.cmd_help(),
            ReplCommand::Status => self.cmd_status(),
            ReplCommand::Reload => self.cmd_reload(),
            ReplCommand::Validate => self.cmd_validate(),
            ReplCommand::List { detailed } => self.cmd_list(detailed),
            ReplCommand::Show { query, version } => self.cmd_show(&query, version),
            ReplCommand::Run { query, partition, dry_run, skip_invariants, scratch, scratch_ttl } => {
                self.cmd_run(query, partition, dry_run, skip_invariants, scratch, scratch_ttl).await
            }
            ReplCommand::Backfill { query, from, to, dry_run, skip_invariants } => {
                self.cmd_backfill(&query, &from, &to, dry_run, skip_invariants).await
            }
            ReplCommand::Check { query, partition, before, after } => {
                self.cmd_check(&query, partition, before, after).await
            }
            ReplCommand::Init { dataset } => {
                self.cmd_init(&dataset).await
            }
            ReplCommand::Sync { from, to, dry_run, tracking_dataset, allow_source_mutation } => {
                self.cmd_sync(from, to, dry_run, &tracking_dataset, allow_source_mutation).await
            }
            ReplCommand::Audit { query, modified_only, diff, output } => {
                self.cmd_audit(query, modified_only, diff, &output)
            }
            ReplCommand::ScratchList { project } => {
                self.cmd_scratch_list(&project).await
            }
            ReplCommand::ScratchPromote { query, partition, scratch_project } => {
                self.cmd_scratch_promote(&query, &partition, &scratch_project).await
            }
        }
    }

    fn cmd_help(&self) -> ReplResult {
        let help = r#"Available commands:
  list [--detailed]                    List all queries
  show <query> [--version N]           Show query details
  validate                             Validate all query definitions
  run [--query Q] [--partition P]      Run query (all if no query specified)
      [--dry-run] [--skip-invariants]
      [--scratch PROJECT] [--scratch-ttl H]
  backfill <query> --from DATE --to DATE
      [--dry-run] [--skip-invariants]
  check <query> [--partition P] [--before] [--after]
  init [--dataset D]                   Initialize tracking table
  sync [--from DATE] [--to DATE] [--dry-run]
      [--tracking-dataset D] [--allow-source-mutation]
  audit [--query Q] [--modified-only] [--diff] [--output FORMAT]
  scratch list --project P             List scratch tables
  scratch promote --query Q --partition P --scratch-project P
  reload                               Reload queries from disk
  status                               Show session status
  help                                 Show this help
  exit                                 Exit REPL"#;

        ReplResult::success_with_output(help.to_string())
    }

    fn cmd_status(&self) -> ReplResult {
        let project_str = self.project.as_deref().unwrap_or("(not set)");
        let queries_count = self.cached_queries.as_ref().map(|q| q.len()).unwrap_or(0);
        let client_status = if self.client.is_some() { "connected" } else { "not connected" };

        let output = format!(
            "Project: {}\nQueries path: {}\nQueries loaded: {}\nClient: {}",
            project_str,
            self.queries_path.display(),
            queries_count,
            client_status
        );

        let data = serde_json::json!({
            "project": self.project,
            "queries_path": self.queries_path.to_string_lossy(),
            "queries_loaded": queries_count,
            "client_connected": self.client.is_some()
        });

        ReplResult::success_with_both(output, data)
    }

    fn cmd_reload(&mut self) -> ReplResult {
        match self.reload_queries() {
            Ok(count) => {
                let output = format!("✓ Reloaded {} queries", count);
                let data = serde_json::json!({"queries_loaded": count});
                ReplResult::success_with_both(output, data)
            }
            Err(e) => ReplResult::failure(e.to_string()),
        }
    }

    fn cmd_validate(&mut self) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let mut output_lines = Vec::new();
        let mut total_errors = 0;
        let mut total_warnings = 0;
        let mut results = Vec::new();

        for query in &queries {
            let result = QueryValidator::validate(query);
            let status = if result.is_valid() {
                if result.has_warnings() { "⚠" } else { "✓" }
            } else {
                "✗"
            };

            output_lines.push(format!("{} {}", status, query.name));

            for err in &result.errors {
                output_lines.push(format!("    ✗ [{}] {}", err.code, err.message));
            }
            for warn in &result.warnings {
                output_lines.push(format!("    ⚠ [{}] {}", warn.code, warn.message));
            }

            total_errors += result.errors.len();
            total_warnings += result.warnings.len();

            results.push(serde_json::json!({
                "query": query.name,
                "valid": result.is_valid(),
                "errors": result.errors.len(),
                "warnings": result.warnings.len()
            }));
        }

        if total_errors > 0 {
            output_lines.push(format!("\n✗ Validation failed: {} errors, {} warnings", total_errors, total_warnings));
        } else if total_warnings > 0 {
            output_lines.push(format!("\n⚠ {} queries validated with {} warnings", queries.len(), total_warnings));
        } else {
            output_lines.push(format!("\n✓ {} queries validated successfully", queries.len()));
        }

        let data = serde_json::json!({
            "queries_validated": queries.len(),
            "errors": total_errors,
            "warnings": total_warnings,
            "results": results
        });

        if total_errors > 0 {
            ReplResult {
                success: false,
                output: Some(output_lines.join("\n")),
                data: Some(data),
                error: Some("Validation failed".to_string()),
            }
        } else {
            ReplResult::success_with_both(output_lines.join("\n"), data)
        }
    }

    fn cmd_list(&mut self, detailed: bool) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        if queries.is_empty() {
            return ReplResult::success_with_output(format!("No queries found in {}", self.queries_path.display()));
        }

        let mut output_lines = Vec::new();
        let mut data_list = Vec::new();

        for query in &queries {
            if detailed {
                output_lines.push(query.name.clone());
                output_lines.push(format!("  destination: {}.{}", query.destination.dataset, query.destination.table));
                if let Some(desc) = &query.description {
                    output_lines.push(format!("  description: {}", desc));
                }
                if let Some(owner) = &query.owner {
                    output_lines.push(format!("  owner: {}", owner));
                }
                output_lines.push(format!("  versions: {}", query.versions.len()));
                if let Some(latest) = query.latest_version() {
                    output_lines.push(format!("  latest: v{} (effective {})", latest.version, latest.effective_from));
                }
                output_lines.push(String::new());
            } else {
                let latest = query.latest_version().map(|v| v.version).unwrap_or(0);
                output_lines.push(format!("{:<30} v{:<3} {}.{}",
                    query.name,
                    latest,
                    query.destination.dataset,
                    query.destination.table
                ));
            }

            data_list.push(serde_json::json!({
                "name": query.name,
                "dataset": query.destination.dataset,
                "table": query.destination.table,
                "latest_version": query.latest_version().map(|v| v.version),
                "versions_count": query.versions.len()
            }));
        }

        let data = serde_json::json!({
            "queries": data_list,
            "count": queries.len()
        });

        ReplResult::success_with_both(output_lines.join("\n"), data)
    }

    fn cmd_show(&mut self, query_name: &str, version_num: Option<u32>) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let query = match queries.iter().find(|q| q.name == query_name) {
            Some(q) => q,
            None => return ReplResult::failure(format!("Query '{}' not found", query_name)),
        };

        let mut output_lines = Vec::new();
        output_lines.push(format!("Name: {}", query.name));
        output_lines.push(format!("Destination: {}.{}", query.destination.dataset, query.destination.table));

        if let Some(desc) = &query.description {
            output_lines.push(format!("Description: {}", desc));
        }
        if let Some(owner) = &query.owner {
            output_lines.push(format!("Owner: {}", owner));
        }

        output_lines.push(format!("\nPartition:"));
        output_lines.push(format!("  field: {}", query.destination.partition.field.as_deref().unwrap_or("_PARTITIONTIME")));
        output_lines.push(format!("  type: {:?}", query.destination.partition.partition_type));

        if let Some(cluster) = &query.cluster {
            output_lines.push(format!("\nCluster: {}", cluster.fields.join(", ")));
        }

        output_lines.push(format!("\nVersions:"));
        for version in &query.versions {
            output_lines.push(format!("\n  Version {}", version.version));
            output_lines.push(format!("  effective_from: {}", version.effective_from));
            output_lines.push(format!("  source: {}", version.source));
            output_lines.push(format!("  schema: {} fields", version.schema.fields.len()));
        }

        if let Some(v) = version_num {
            if let Some(version) = query.versions.iter().find(|ver| ver.version == v) {
                output_lines.push(format!("\n--- SQL (v{}) ---\n{}", v, version.sql_content));
            } else {
                output_lines.push(format!("\nVersion {} not found", v));
            }
        }

        let data = serde_json::json!({
            "name": query.name,
            "destination": {
                "dataset": query.destination.dataset,
                "table": query.destination.table
            },
            "versions": query.versions.iter().map(|v| serde_json::json!({
                "version": v.version,
                "effective_from": v.effective_from.to_string(),
                "source": v.source,
                "schema_fields": v.schema.fields.len()
            })).collect::<Vec<_>>()
        });

        ReplResult::success_with_both(output_lines.join("\n"), data)
    }

    async fn cmd_run(
        &mut self,
        query_name: Option<String>,
        partition: Option<String>,
        dry_run: bool,
        skip_invariants: bool,
        scratch: Option<String>,
        scratch_ttl: Option<u32>,
    ) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        if dry_run {
            return self.cmd_run_dry_run(query_name, partition, skip_invariants, &queries);
        }

        if let Some(scratch_project) = scratch {
            return self.cmd_run_scratch(query_name, partition, skip_invariants, scratch_project, scratch_ttl, &queries).await;
        }

        let client = match self.ensure_client().await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let runner = crate::Runner::new(client.clone(), queries.clone());

        match query_name {
            Some(name) => {
                let query = match queries.iter().find(|q| q.name == name) {
                    Some(q) => q,
                    None => return ReplResult::failure(format!("Query '{}' not found", name)),
                };
                let partition_type = &query.destination.partition.partition_type;
                let partition_key = match Self::parse_partition(&partition, partition_type) {
                    Ok(k) => k,
                    Err(e) => return ReplResult::failure(e),
                };

                match runner.run_query_partition(&name, partition_key.clone()).await {
                    Ok(stats) => {
                        let output = format!("✓ {} v{} completed for {}", stats.query_name, stats.version, stats.partition_key);
                        let data = serde_json::json!({
                            "query": stats.query_name,
                            "version": stats.version,
                            "partition": stats.partition_key.to_string()
                        });
                        ReplResult::success_with_both(output, data)
                    }
                    Err(e) => ReplResult::failure(e.to_string()),
                }
            }
            None => {
                let partition_key = match Self::parse_partition(&partition, &PartitionType::Day) {
                    Ok(k) => k,
                    Err(e) => return ReplResult::failure(e),
                };

                match runner.run_for_partition(partition_key).await {
                    Ok(report) => {
                        let mut output_lines = Vec::new();
                        for stats in &report.stats {
                            output_lines.push(format!("✓ {} v{} completed for {}", stats.query_name, stats.version, stats.partition_key));
                        }
                        for failure in &report.failures {
                            output_lines.push(format!("✗ {} ({}): {}", failure.query_name, failure.partition_key, failure.error));
                        }
                        output_lines.push(format!("\n{} succeeded, {} failed", report.stats.len(), report.failures.len()));

                        let data = serde_json::json!({
                            "succeeded": report.stats.len(),
                            "failed": report.failures.len()
                        });
                        ReplResult::success_with_both(output_lines.join("\n"), data)
                    }
                    Err(e) => ReplResult::failure(e.to_string()),
                }
            }
        }
    }

    fn cmd_run_dry_run(
        &self,
        query_name: Option<String>,
        partition: Option<String>,
        skip_invariants: bool,
        queries: &[QueryDef],
    ) -> ReplResult {
        let queries_to_run: Vec<_> = match &query_name {
            Some(name) => queries.iter().filter(|q| &q.name == name).collect(),
            None => queries.iter().collect(),
        };

        if queries_to_run.is_empty() {
            if let Some(name) = query_name {
                return ReplResult::failure(format!("Query '{}' not found", name));
            }
            return ReplResult::success_with_output("No queries to run".to_string());
        }

        let mut output_lines = Vec::new();
        let mut results = Vec::new();

        for query in queries_to_run {
            let partition_type = &query.destination.partition.partition_type;
            let partition_key = match Self::parse_partition(&partition, partition_type) {
                Ok(k) => k,
                Err(e) => return ReplResult::failure(e),
            };

            output_lines.push(format!("Query: {}", query.name));
            output_lines.push(format!("Destination: {}.{}", query.destination.dataset, query.destination.table));
            output_lines.push(format!("Partition type: {:?}", partition_type));

            let date_for_version = partition_key.to_naive_date();
            if let Some(version) = query.get_version_for_date(date_for_version) {
                output_lines.push(format!("Version: {}", version.version));
                output_lines.push(format!("Source: {}", version.source));
                output_lines.push(format!("\n--- SQL ---\n{}\n-----------\n", version.get_sql_for_date(date_for_version)));

                if !skip_invariants {
                    let before_count = version.invariants.before.len();
                    let after_count = version.invariants.after.len();
                    if before_count > 0 || after_count > 0 {
                        output_lines.push(format!("Invariants: {} before, {} after", before_count, after_count));
                    }
                }

                results.push(serde_json::json!({
                    "query": query.name,
                    "version": version.version,
                    "partition": partition_key.to_string(),
                    "dry_run": true
                }));
            } else {
                output_lines.push(format!("No version found for date {}", date_for_version));
            }
        }

        let data = serde_json::json!({
            "dry_run": true,
            "results": results
        });
        ReplResult::success_with_both(output_lines.join("\n"), data)
    }

    async fn cmd_run_scratch(
        &mut self,
        query_name: Option<String>,
        partition: Option<String>,
        skip_invariants: bool,
        scratch_project: String,
        scratch_ttl: Option<u32>,
        queries: &[QueryDef],
    ) -> ReplResult {
        use crate::executor::{ScratchConfig, ScratchWriter};

        let query_name = match query_name {
            Some(n) => n,
            None => return ReplResult::failure("Query name required for scratch mode".to_string()),
        };

        let query = match queries.iter().find(|q| q.name == query_name) {
            Some(q) => q.clone(),
            None => return ReplResult::failure(format!("Query '{}' not found", query_name)),
        };

        let partition_type = &query.destination.partition.partition_type;
        let partition_key = match Self::parse_partition(&partition, partition_type) {
            Ok(k) => k,
            Err(e) => return ReplResult::failure(e),
        };

        let scratch_client = match BqClient::new(&scratch_project).await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(format!("Failed to create scratch client: {}", e)),
        };

        let mut config = ScratchConfig::new(scratch_project.clone());
        if let Some(ttl) = scratch_ttl {
            config = config.with_ttl(ttl);
        }

        let scratch_writer = ScratchWriter::new(scratch_client, config);

        if let Err(e) = scratch_writer.ensure_dataset().await {
            return ReplResult::failure(format!("Failed to ensure scratch dataset: {}", e));
        }

        match scratch_writer.write_partition(&query, partition_key.clone(), !skip_invariants).await {
            Ok(stats) => {
                let mut output_lines = Vec::new();
                output_lines.push(format!("✓ {} v{} completed (scratch)", stats.query_name, stats.version));
                output_lines.push(format!("  Destination: {}", stats.scratch_table));
                output_lines.push(format!("  Partition: {}", stats.partition_key));
                output_lines.push(format!("  Expires: {}", stats.expiration.format("%Y-%m-%dT%H:%M:%SZ")));
                output_lines.push(format!("\nTo promote to production:"));
                output_lines.push(format!("  scratch promote --query {} --partition {} --scratch-project {}", stats.query_name, stats.partition_key, scratch_project));

                let data = serde_json::json!({
                    "query": stats.query_name,
                    "version": stats.version,
                    "partition": stats.partition_key.to_string(),
                    "scratch_table": stats.scratch_table,
                    "expiration": stats.expiration.to_rfc3339()
                });
                ReplResult::success_with_both(output_lines.join("\n"), data)
            }
            Err(e) => ReplResult::failure(e.to_string()),
        }
    }

    async fn cmd_backfill(
        &mut self,
        query_name: &str,
        from: &str,
        to: &str,
        dry_run: bool,
        skip_invariants: bool,
    ) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let query = match queries.iter().find(|q| q.name == query_name) {
            Some(q) => q,
            None => return ReplResult::failure(format!("Query '{}' not found", query_name)),
        };

        let partition_type = &query.destination.partition.partition_type;
        let from_key = match PartitionKey::parse(from, partition_type) {
            Ok(k) => k,
            Err(e) => return ReplResult::failure(format!("Invalid from partition: {}", e)),
        };
        let to_key = match PartitionKey::parse(to, partition_type) {
            Ok(k) => k,
            Err(e) => return ReplResult::failure(format!("Invalid to partition: {}", e)),
        };

        if dry_run {
            let mut output_lines = Vec::new();
            let mut current = from_key.clone();
            while current <= to_key {
                let date = current.to_naive_date();
                if let Some(version) = query.get_version_for_date(date) {
                    output_lines.push(format!("{}: v{} ({})", current, version.version, version.source));
                } else {
                    output_lines.push(format!("{}: no version available", current));
                }
                current = current.next();
            }
            return ReplResult::success_with_output(output_lines.join("\n"));
        }

        let _ = skip_invariants;

        let client = match self.ensure_client().await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let runner = crate::Runner::new(client.clone(), queries);

        match runner.backfill_partitions(query_name, from_key, to_key, None).await {
            Ok(report) => {
                let mut output_lines = Vec::new();
                for stats in &report.stats {
                    output_lines.push(format!("✓ {} v{} completed for {}", stats.query_name, stats.version, stats.partition_key));
                }
                for failure in &report.failures {
                    output_lines.push(format!("✗ {}: {}", failure.partition_key, failure.error));
                }
                output_lines.push(format!("\n{} succeeded, {} failed", report.stats.len(), report.failures.len()));

                let data = serde_json::json!({
                    "succeeded": report.stats.len(),
                    "failed": report.failures.len()
                });
                ReplResult::success_with_both(output_lines.join("\n"), data)
            }
            Err(e) => ReplResult::failure(e.to_string()),
        }
    }

    async fn cmd_check(
        &mut self,
        query_name: &str,
        partition: Option<String>,
        run_before: bool,
        run_after: bool,
    ) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let query = match queries.iter().find(|q| q.name == query_name) {
            Some(q) => q,
            None => return ReplResult::failure(format!("Query '{}' not found", query_name)),
        };

        let partition_type = &query.destination.partition.partition_type;
        let partition_key = match Self::parse_partition(&partition, partition_type) {
            Ok(k) => k,
            Err(e) => return ReplResult::failure(e),
        };
        let date_for_version = partition_key.to_naive_date();

        let version = match query.get_version_for_date(date_for_version) {
            Some(v) => v,
            None => return ReplResult::failure(format!("No version found for date {}", date_for_version)),
        };

        let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);
        let run_all = !run_before && !run_after;

        let client = match self.ensure_client().await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let checker = InvariantChecker::new(client, &query.destination, date_for_version);

        let mut output_lines = Vec::new();
        let mut total_passed = 0;
        let mut total_failed = 0;
        let mut has_errors = false;

        output_lines.push(format!("Running invariant checks for '{}' v{} on {}", query.name, version.version, partition_key));

        if (run_all || run_before) && !before_checks.is_empty() {
            output_lines.push("\nBefore checks:".to_string());
            match checker.run_checks(&before_checks).await {
                Ok(results) => {
                    for result in &results {
                        let icon = match result.status {
                            CheckStatus::Passed => { total_passed += 1; "✓" }
                            CheckStatus::Failed => {
                                total_failed += 1;
                                if result.severity == Severity::Error { has_errors = true; "✗" }
                                else { "⚠" }
                            }
                            CheckStatus::Skipped => "○",
                        };
                        output_lines.push(format!("  {} {}: {}", icon, result.name, result.message));
                    }
                }
                Err(e) => return ReplResult::failure(e.to_string()),
            }
        }

        if (run_all || run_after) && !after_checks.is_empty() {
            output_lines.push("\nAfter checks:".to_string());
            match checker.run_checks(&after_checks).await {
                Ok(results) => {
                    for result in &results {
                        let icon = match result.status {
                            CheckStatus::Passed => { total_passed += 1; "✓" }
                            CheckStatus::Failed => {
                                total_failed += 1;
                                if result.severity == Severity::Error { has_errors = true; "✗" }
                                else { "⚠" }
                            }
                            CheckStatus::Skipped => "○",
                        };
                        output_lines.push(format!("  {} {}: {}", icon, result.name, result.message));
                    }
                }
                Err(e) => return ReplResult::failure(e.to_string()),
            }
        }

        if total_passed == 0 && total_failed == 0 {
            output_lines.push("\nNo invariant checks defined for this query/version.".to_string());
        } else {
            output_lines.push(format!("\n{} passed, {} failed", total_passed, total_failed));
        }

        let data = serde_json::json!({
            "passed": total_passed,
            "failed": total_failed,
            "has_errors": has_errors
        });

        if has_errors {
            ReplResult {
                success: false,
                output: Some(output_lines.join("\n")),
                data: Some(data),
                error: Some("Invariant checks failed".to_string()),
            }
        } else {
            ReplResult::success_with_both(output_lines.join("\n"), data)
        }
    }

    async fn cmd_init(&mut self, dataset: &str) -> ReplResult {
        let client = match self.ensure_client().await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let tracker = crate::MigrationTracker::new(client.clone(), dataset);

        match tracker.ensure_tracking_table().await {
            Ok(_) => {
                let output = format!("✓ Tracking table created: {}._bqdrift_query_runs", dataset);
                ReplResult::success_with_output(output)
            }
            Err(e) => ReplResult::failure(e.to_string()),
        }
    }

    async fn cmd_sync(
        &mut self,
        from: Option<String>,
        to: Option<String>,
        dry_run: bool,
        _tracking_dataset: &str,
        _allow_source_mutation: bool,
    ) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let yaml_contents = match self.loader.load_yaml_contents(&self.queries_path) {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let today = Utc::now().date_naive();
        let from_date = match from {
            Some(s) => match NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                Ok(d) => d,
                Err(_) => return ReplResult::failure(format!("Invalid from date: {}", s)),
            },
            None => today - chrono::Duration::days(30),
        };
        let to_date = match to {
            Some(s) => match NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                Ok(d) => d,
                Err(_) => return ReplResult::failure(format!("Invalid to date: {}", s)),
            },
            None => today,
        };

        let stored_states = vec![];
        let detector = crate::DriftDetector::new(queries, yaml_contents);
        let report = match detector.detect(&stored_states, from_date, to_date) {
            Ok(r) => r,
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let drifted: Vec<_> = report.needs_rerun();

        if drifted.is_empty() {
            return ReplResult::success_with_output("✓ All partitions are current".to_string());
        }

        let summary = report.summary();
        let mut output_lines = Vec::new();
        output_lines.push("Drift summary:".to_string());
        for (state, count) in &summary {
            if *state != crate::DriftState::Current {
                output_lines.push(format!("  {} {}", count, state.as_str()));
            }
        }

        if dry_run {
            output_lines.push(format!("\nRun without --dry-run to execute {} drifted partitions", drifted.len()));
        } else {
            output_lines.push("\nSync execution not yet implemented.".to_string());
        }

        let data = serde_json::json!({
            "drifted_count": drifted.len(),
            "dry_run": dry_run
        });
        ReplResult::success_with_both(output_lines.join("\n"), data)
    }

    fn cmd_audit(
        &mut self,
        query_filter: Option<String>,
        modified_only: bool,
        _show_diff: bool,
        output: &str,
    ) -> ReplResult {
        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let queries_to_audit: Vec<_> = match &query_filter {
            Some(name) => queries.iter().filter(|q| &q.name == name).cloned().collect(),
            None => queries,
        };

        if queries_to_audit.is_empty() {
            if let Some(name) = query_filter {
                return ReplResult::failure(format!("Query '{}' not found", name));
            }
            return ReplResult::success_with_output("No queries found".to_string());
        }

        let stored_states = vec![];
        let auditor = crate::SourceAuditor::new(&queries_to_audit);
        let report = auditor.audit(&stored_states);

        let entries_to_show: Vec<_> = if modified_only {
            report.entries.iter().filter(|e| e.status == crate::SourceStatus::Modified).cloned().collect()
        } else {
            report.entries.clone()
        };

        match output {
            "json" => {
                match serde_json::to_string_pretty(&entries_to_show) {
                    Ok(json) => ReplResult::success_with_output(json),
                    Err(e) => ReplResult::failure(e.to_string()),
                }
            }
            "yaml" => {
                match serde_yaml::to_string(&entries_to_show) {
                    Ok(yaml) => ReplResult::success_with_output(yaml),
                    Err(e) => ReplResult::failure(e.to_string()),
                }
            }
            _ => {
                let mut output_lines = Vec::new();
                output_lines.push("Source Audit Report".to_string());
                output_lines.push(format!("  ✓ {} current", report.current_count()));
                output_lines.push(format!("  ⚠ {} modified", report.modified_count()));
                output_lines.push(format!("  ○ {} never executed", report.never_executed_count()));

                let data = serde_json::json!({
                    "current": report.current_count(),
                    "modified": report.modified_count(),
                    "never_executed": report.never_executed_count()
                });
                ReplResult::success_with_both(output_lines.join("\n"), data)
            }
        }
    }

    async fn cmd_scratch_list(&mut self, project: &str) -> ReplResult {
        use crate::executor::{ScratchConfig, ScratchWriter};

        let client = match BqClient::new(project).await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(format!("Failed to connect: {}", e)),
        };

        let config = ScratchConfig::new(project.to_string());
        let writer = ScratchWriter::new(client, config);

        match writer.list_tables().await {
            Ok(tables) => {
                if tables.is_empty() {
                    ReplResult::success_with_output(format!("No scratch tables found in {}.bqdrift_scratch", project))
                } else {
                    let output = format!("Scratch tables in {}.bqdrift_scratch:\n{}", project, tables.iter().map(|t| format!("  {}", t)).collect::<Vec<_>>().join("\n"));
                    let data = serde_json::json!({"tables": tables});
                    ReplResult::success_with_both(output, data)
                }
            }
            Err(e) => ReplResult::failure(e.to_string()),
        }
    }

    async fn cmd_scratch_promote(
        &mut self,
        query_name: &str,
        partition_str: &str,
        scratch_project: &str,
    ) -> ReplResult {
        use crate::executor::{ScratchConfig, ScratchWriter};

        let queries = match self.ensure_queries() {
            Ok(q) => q.clone(),
            Err(e) => return ReplResult::failure(e.to_string()),
        };

        let query = match queries.iter().find(|q| q.name == query_name) {
            Some(q) => q,
            None => return ReplResult::failure(format!("Query '{}' not found", query_name)),
        };

        let partition_type = &query.destination.partition.partition_type;
        let partition_key = match PartitionKey::parse(partition_str, partition_type) {
            Ok(k) => k,
            Err(e) => return ReplResult::failure(format!("Invalid partition: {}", e)),
        };

        let production_project = match &self.project {
            Some(p) => p.clone(),
            None => return ReplResult::failure("Production project not set".to_string()),
        };

        let scratch_client = match BqClient::new(scratch_project).await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(format!("Failed to connect to scratch: {}", e)),
        };

        let production_client = match BqClient::new(&production_project).await {
            Ok(c) => c,
            Err(e) => return ReplResult::failure(format!("Failed to connect to production: {}", e)),
        };

        let config = ScratchConfig::new(scratch_project.to_string());
        let scratch_writer = ScratchWriter::new(scratch_client, config);

        match scratch_writer.promote_to_production(query, &partition_key, &production_client).await {
            Ok(stats) => {
                let output = format!(
                    "✓ Promoted {} to production\n  From: {}\n  To: {}\n  Partition: {}",
                    stats.query_name, stats.scratch_table, stats.production_table, stats.partition_key
                );
                let data = serde_json::json!({
                    "query": stats.query_name,
                    "scratch_table": stats.scratch_table,
                    "production_table": stats.production_table,
                    "partition": stats.partition_key.to_string()
                });
                ReplResult::success_with_both(output, data)
            }
            Err(e) => ReplResult::failure(e.to_string()),
        }
    }

    fn parse_partition(partition: &Option<String>, partition_type: &PartitionType) -> std::result::Result<PartitionKey, String> {
        match partition {
            Some(p) => PartitionKey::parse(p, partition_type)
                .map_err(|e| e.to_string()),
            None => Ok(Self::default_partition_key(partition_type)),
        }
    }

    fn default_partition_key(partition_type: &PartitionType) -> PartitionKey {
        let today = Utc::now().date_naive();
        match partition_type {
            PartitionType::Hour => {
                let now = Utc::now().naive_utc();
                PartitionKey::Hour(now.date().and_hms_opt(now.time().hour(), 0, 0).unwrap())
            }
            PartitionType::Day | PartitionType::IngestionTime => PartitionKey::Day(today),
            PartitionType::Month => PartitionKey::Month { year: today.year(), month: today.month() },
            PartitionType::Year => PartitionKey::Year(today.year()),
            PartitionType::Range => PartitionKey::Range(0),
        }
    }
}
