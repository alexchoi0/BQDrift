use clap::{Parser, Subcommand};
use chrono::NaiveDate;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::{info, error, warn};
use tracing_subscriber::EnvFilter;

use bqdrift::{QueryLoader, QueryValidator, Runner, CheckStatus, Severity, InvariantChecker, resolve_invariants_def};
use bqdrift::{DriftDetector, DriftState, decode_sql, format_sql_diff, has_changes};
use bqdrift::executor::BqClient;
use bqdrift::error::{BqDriftError, BigQueryError};
use bqdrift::executor::WriteStats;

#[derive(Parser)]
#[command(name = "bqdrift")]
#[command(about = "BigQuery schema versioning and OLAP query orchestration")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to queries directory
    #[arg(short, long, default_value = "./queries")]
    queries: PathBuf,

    /// GCP project ID
    #[arg(short, long, env = "GCP_PROJECT_ID")]
    project: Option<String>,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate all query definitions
    Validate,

    /// List all queries
    List {
        /// Show detailed version info
        #[arg(short, long)]
        detailed: bool,
    },

    /// Run queries for a specific date
    Run {
        /// Query name (runs all if not specified)
        #[arg(short, long)]
        query: Option<String>,

        /// Partition date (defaults to today)
        #[arg(short, long)]
        date: Option<NaiveDate>,

        /// Dry run - validate and show SQL without executing
        #[arg(long)]
        dry_run: bool,

        /// Skip invariant checks
        #[arg(long)]
        skip_invariants: bool,
    },

    /// Backfill a query for a date range
    Backfill {
        /// Query name
        query: String,

        /// Start date (inclusive)
        #[arg(short, long)]
        from: NaiveDate,

        /// End date (inclusive)
        #[arg(short, long)]
        to: NaiveDate,

        /// Dry run - show what would be executed
        #[arg(long)]
        dry_run: bool,

        /// Skip invariant checks
        #[arg(long)]
        skip_invariants: bool,
    },

    /// Run invariant checks only (no query execution)
    Check {
        /// Query name
        query: String,

        /// Partition date (defaults to today)
        #[arg(short, long)]
        date: Option<NaiveDate>,

        /// Run only before checks
        #[arg(long)]
        before: bool,

        /// Run only after checks
        #[arg(long)]
        after: bool,
    },

    /// Show query details
    Show {
        /// Query name
        query: String,

        /// Show SQL for specific version
        #[arg(short, long)]
        version: Option<u32>,
    },

    /// Initialize tracking table in BigQuery
    Init {
        /// Dataset for tracking table
        #[arg(short, long, default_value = "bqdrift")]
        dataset: String,
    },

    /// Detect and sync drifted partitions
    Sync {
        /// Date range start (defaults to 30 days ago)
        #[arg(short, long)]
        from: Option<NaiveDate>,

        /// Date range end (defaults to today)
        #[arg(short, long)]
        to: Option<NaiveDate>,

        /// Dry run - show what would be synced with SQL diffs
        #[arg(long)]
        dry_run: bool,

        /// Skip invariant checks when syncing
        #[arg(long)]
        skip_invariants: bool,

        /// Dataset for tracking table
        #[arg(long, default_value = "bqdrift")]
        tracking_dataset: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let filter = if cli.verbose {
        EnvFilter::new("bqdrift=debug,info")
    } else {
        EnvFilter::new("bqdrift=info,warn")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    match run(cli).await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            print_error(e);
            ExitCode::FAILURE
        }
    }
}

fn print_error(err: Box<dyn std::error::Error>) {
    if let Some(bq_err) = err.downcast_ref::<BqDriftError>() {
        if let BqDriftError::BigQuery(bq) = bq_err {
            print_bq_error(bq);
            return;
        }
    }

    eprintln!("\x1b[31m✗ Error:\x1b[0m {}", err);
}

fn print_bq_error(err: &BigQueryError) {
    eprintln!("\n\x1b[31m✗ BigQuery Error [{}]\x1b[0m", err.error_code());
    eprintln!("  {}", err);
    eprintln!("\n\x1b[33mSuggestion:\x1b[0m");
    for line in err.suggestion().lines() {
        eprintln!("  {}", line);
    }
    eprintln!();
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let loader = QueryLoader::new();

    match cli.command {
        Commands::Validate => {
            cmd_validate(&loader, &cli.queries)?;
        }

        Commands::List { detailed } => {
            cmd_list(&loader, &cli.queries, detailed)?;
        }

        Commands::Run { query, date, dry_run, skip_invariants } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_run(&loader, &cli.queries, &project, query, date, dry_run, skip_invariants).await?;
        }

        Commands::Backfill { query, from, to, dry_run, skip_invariants } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_backfill(&loader, &cli.queries, &project, &query, from, to, dry_run, skip_invariants).await?;
        }

        Commands::Check { query, date, before, after } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_check(&loader, &cli.queries, &project, &query, date, before, after).await?;
        }

        Commands::Show { query, version } => {
            cmd_show(&loader, &cli.queries, &query, version)?;
        }

        Commands::Init { dataset } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_init(&project, &dataset).await?;
        }

        Commands::Sync { from, to, dry_run, skip_invariants: _, tracking_dataset } => {
            let project = if dry_run {
                cli.project.unwrap_or_default()
            } else {
                cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?
            };
            cmd_sync(&loader, &cli.queries, &project, from, to, dry_run, &tracking_dataset).await?;
        }
    }

    Ok(())
}

fn cmd_validate(loader: &QueryLoader, queries_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    info!("Validating queries in {}", queries_path.display());

    let queries = loader.load_dir(queries_path)?;

    let mut total_errors = 0;
    let mut total_warnings = 0;
    let mut failed_queries = Vec::new();

    for query in &queries {
        let result = QueryValidator::validate(&query);

        let status = if result.is_valid() {
            if result.has_warnings() { "⚠" } else { "✓" }
        } else {
            "✗"
        };

        println!("{} {}", status, query.name);

        // Show version summary
        for version in &query.versions {
            let schema_fields = version.schema.fields.len();
            let revisions = version.sql_revisions.len();

            if revisions > 0 {
                println!("  v{}: {} fields, {} SQL revisions", version.version, schema_fields, revisions);
            } else {
                println!("  v{}: {} fields", version.version, schema_fields);
            }
        }

        // Show errors
        for err in &result.errors {
            println!("    {} [{}] {}", "\x1b[31m✗\x1b[0m", err.code, err.message);
        }

        // Show warnings
        for warn in &result.warnings {
            println!("    {} [{}] {}", "\x1b[33m⚠\x1b[0m", warn.code, warn.message);
        }

        total_errors += result.errors.len();
        total_warnings += result.warnings.len();

        if !result.is_valid() {
            failed_queries.push(query.name.clone());
        }
    }

    println!();

    if total_errors > 0 {
        println!("✗ Validation failed: {} errors, {} warnings in {} queries",
            total_errors, total_warnings, queries.len());
        println!("  Failed: {}", failed_queries.join(", "));
        return Err("Validation failed".into());
    } else if total_warnings > 0 {
        println!("⚠ {} queries validated with {} warnings", queries.len(), total_warnings);
    } else {
        println!("✓ {} queries validated successfully", queries.len());
    }

    Ok(())
}

fn cmd_list(loader: &QueryLoader, queries_path: &PathBuf, detailed: bool) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;

    if queries.is_empty() {
        println!("No queries found in {}", queries_path.display());
        return Ok(());
    }

    for query in &queries {
        if detailed {
            println!("{}", query.name);
            println!("  destination: {}.{}", query.destination.dataset, query.destination.table);

            if let Some(desc) = &query.description {
                println!("  description: {}", desc);
            }

            if let Some(owner) = &query.owner {
                println!("  owner: {}", owner);
            }

            println!("  versions: {}", query.versions.len());

            if let Some(latest) = query.latest_version() {
                println!("  latest: v{} (effective {})", latest.version, latest.effective_from);
            }

            if let Some(cluster) = &query.cluster {
                println!("  cluster: {}", cluster.fields.join(", "));
            }

            println!();
        } else {
            let latest = query.latest_version().map(|v| v.version).unwrap_or(0);
            println!("{:<30} v{:<3} {}.{}",
                query.name,
                latest,
                query.destination.dataset,
                query.destination.table
            );
        }
    }

    Ok(())
}

async fn cmd_run(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    project: &str,
    query_name: Option<String>,
    date: Option<NaiveDate>,
    dry_run: bool,
    skip_invariants: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;
    let date = date.unwrap_or_else(|| chrono::Utc::now().date_naive());

    if dry_run {
        info!("Dry run for date: {}", date);

        let queries_to_run: Vec<_> = match &query_name {
            Some(name) => queries.iter().filter(|q| &q.name == name).collect(),
            None => queries.iter().collect(),
        };

        if queries_to_run.is_empty() {
            if let Some(name) = query_name {
                error!("Query '{}' not found", name);
            }
            return Ok(());
        }

        for query in queries_to_run {
            println!("Query: {}", query.name);
            println!("Destination: {}.{}", query.destination.dataset, query.destination.table);

            if let Some(version) = query.get_version_for_date(date) {
                println!("Version: {}", version.version);
                println!("SQL file: {}", version.sql);
                println!("\n--- SQL ---\n{}\n-----------\n", version.get_sql_for_date(date));

                if !skip_invariants {
                    let before_count = version.invariants.before.len();
                    let after_count = version.invariants.after.len();
                    if before_count > 0 || after_count > 0 {
                        println!("Invariants: {} before, {} after", before_count, after_count);
                    }
                }
            } else {
                warn!("No version found for date {}", date);
            }
        }

        return Ok(());
    }

    let client = BqClient::new(project).await?;
    let runner = Runner::new(client, queries);

    if skip_invariants {
        info!("Running with invariants skipped");
    }

    match query_name {
        Some(name) => {
            info!("Running query '{}' for date {}", name, date);
            let stats = runner.run_query(&name, date).await?;
            print_stats(&stats, skip_invariants);
        }
        None => {
            info!("Running all queries for date {}", date);
            let report = runner.run_for_date(date).await?;

            for stats in &report.stats {
                print_stats(stats, skip_invariants);
            }

            for failure in &report.failures {
                eprintln!("\x1b[31m✗\x1b[0m {} ({}): {}", failure.query_name, failure.partition_key, failure.error);
            }

            println!("\n{} succeeded, {} failed", report.stats.len(), report.failures.len());
        }
    }

    Ok(())
}

fn print_stats(stats: &WriteStats, skip_invariants: bool) {
    println!("✓ {} v{} completed for {}", stats.query_name, stats.version, stats.partition_key);

    if !skip_invariants {
        if let Some(report) = &stats.invariant_report {
            let mut passed = 0;
            let mut failed_warnings = 0;
            let mut failed_errors = 0;

            for result in report.before.iter().chain(report.after.iter()) {
                match result.status {
                    CheckStatus::Passed => passed += 1,
                    CheckStatus::Failed => {
                        if result.severity == Severity::Warning {
                            failed_warnings += 1;
                        } else {
                            failed_errors += 1;
                        }
                    }
                    CheckStatus::Skipped => {}
                }
            }

            if passed > 0 || failed_warnings > 0 || failed_errors > 0 {
                print!("  Invariants: {} passed", passed);
                if failed_warnings > 0 {
                    print!(", \x1b[33m{} warnings\x1b[0m", failed_warnings);
                }
                if failed_errors > 0 {
                    print!(", \x1b[31m{} errors\x1b[0m", failed_errors);
                }
                println!();

                for result in report.before.iter().chain(report.after.iter()) {
                    if result.status == CheckStatus::Failed {
                        let color = if result.severity == Severity::Warning { "33" } else { "31" };
                        println!("    \x1b[{}m{}\x1b[0m {}: {}", color,
                            if result.severity == Severity::Warning { "⚠" } else { "✗" },
                            result.name, result.message);
                    }
                }
            }
        }
    }
}

async fn cmd_backfill(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    project: &str,
    query_name: &str,
    from: NaiveDate,
    to: NaiveDate,
    dry_run: bool,
    skip_invariants: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;

    let query = queries.iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("Query '{}' not found", query_name))?;

    let days = (to - from).num_days() + 1;
    info!("Backfilling '{}' from {} to {} ({} days)", query_name, from, to, days);

    if dry_run {
        let mut current = from;
        while current <= to {
            if let Some(version) = query.get_version_for_date(current) {
                println!("{}: v{} ({})", current, version.version, version.sql);
            } else {
                println!("{}: no version available", current);
            }
            current = current.succ_opt().unwrap_or(current);
        }
        return Ok(());
    }

    if skip_invariants {
        info!("Running with invariants skipped");
    }

    let client = BqClient::new(project).await?;
    let runner = Runner::new(client, queries);

    let report = runner.backfill(query_name, from, to).await?;

    for stats in &report.stats {
        print_stats(stats, skip_invariants);
    }

    for failure in &report.failures {
        eprintln!("\x1b[31m✗\x1b[0m {}: {}", failure.partition_key, failure.error);
    }

    println!("\n{} succeeded, {} failed", report.stats.len(), report.failures.len());

    Ok(())
}

async fn cmd_check(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    project: &str,
    query_name: &str,
    date: Option<NaiveDate>,
    run_before: bool,
    run_after: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;
    let date = date.unwrap_or_else(|| chrono::Utc::now().date_naive());

    let query = queries.iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("Query '{}' not found", query_name))?;

    let version = query.get_version_for_date(date)
        .ok_or_else(|| format!("No version found for date {}", date))?;

    let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);

    let run_all = !run_before && !run_after;

    let client = BqClient::new(project).await?;
    let checker = InvariantChecker::new(&client, &query.destination, date);

    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut has_errors = false;

    println!("Running invariant checks for '{}' v{} on {}", query.name, version.version, date);
    println!();

    if (run_all || run_before) && !before_checks.is_empty() {
        println!("Before checks:");
        let results = checker.run_checks(&before_checks).await?;

        for result in &results {
            let status_icon = match result.status {
                CheckStatus::Passed => {
                    total_passed += 1;
                    "\x1b[32m✓\x1b[0m"
                }
                CheckStatus::Failed => {
                    total_failed += 1;
                    if result.severity == Severity::Error {
                        has_errors = true;
                        "\x1b[31m✗\x1b[0m"
                    } else {
                        "\x1b[33m⚠\x1b[0m"
                    }
                }
                CheckStatus::Skipped => "○",
            };

            println!("  {} {}: {}", status_icon, result.name, result.message);
            if let Some(details) = &result.details {
                println!("    {}", details);
            }
        }
        println!();
    }

    if (run_all || run_after) && !after_checks.is_empty() {
        println!("After checks:");
        let results = checker.run_checks(&after_checks).await?;

        for result in &results {
            let status_icon = match result.status {
                CheckStatus::Passed => {
                    total_passed += 1;
                    "\x1b[32m✓\x1b[0m"
                }
                CheckStatus::Failed => {
                    total_failed += 1;
                    if result.severity == Severity::Error {
                        has_errors = true;
                        "\x1b[31m✗\x1b[0m"
                    } else {
                        "\x1b[33m⚠\x1b[0m"
                    }
                }
                CheckStatus::Skipped => "○",
            };

            println!("  {} {}: {}", status_icon, result.name, result.message);
            if let Some(details) = &result.details {
                println!("    {}", details);
            }
        }
        println!();
    }

    if total_passed == 0 && total_failed == 0 {
        println!("No invariant checks defined for this query/version.");
    } else {
        println!("{} passed, {} failed", total_passed, total_failed);
        if has_errors {
            return Err("Invariant checks failed with errors".into());
        }
    }

    Ok(())
}

fn cmd_show(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    query_name: &str,
    version_num: Option<u32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;

    let query = queries.iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("Query '{}' not found", query_name))?;

    println!("Name: {}", query.name);
    println!("Destination: {}.{}", query.destination.dataset, query.destination.table);

    if let Some(desc) = &query.description {
        println!("Description: {}", desc);
    }

    if let Some(owner) = &query.owner {
        println!("Owner: {}", owner);
    }

    if !query.tags.is_empty() {
        println!("Tags: {}", query.tags.join(", "));
    }

    println!("\nPartition:");
    println!("  field: {}", query.destination.partition.field.as_deref().unwrap_or("_PARTITIONTIME"));
    println!("  type: {:?}", query.destination.partition.partition_type);

    if let Some(cluster) = &query.cluster {
        println!("\nCluster: {}", cluster.fields.join(", "));
    }

    println!("\nVersions:");
    for version in &query.versions {
        println!("\n  Version {}", version.version);
        println!("  effective_from: {}", version.effective_from);
        println!("  sql: {}", version.sql);

        if let Some(desc) = &version.description {
            println!("  description: {}", desc);
        }

        if let Some(backfill) = &version.backfill_since {
            println!("  backfill_since: {}", backfill);
        }

        if !version.sql_revisions.is_empty() {
            println!("  revisions:");
            for rev in &version.sql_revisions {
                print!("    r{}: {} ({})", rev.revision, rev.sql, rev.effective_from);
                if let Some(reason) = &rev.reason {
                    print!(" - {}", reason);
                }
                println!();
            }
        }

        println!("  schema: {} fields", version.schema.fields.len());
        for field in &version.schema.fields {
            println!("    - {}: {:?}", field.name, field.field_type);
        }

        if !version.dependencies.is_empty() {
            println!("  dependencies (auto-detected):");
            for dep in &version.dependencies {
                println!("    - {}", dep);
            }
        }
    }

    if let Some(v) = version_num {
        if let Some(version) = query.versions.iter().find(|ver| ver.version == v) {
            println!("\n--- SQL (v{}) ---\n{}", v, version.sql_content);
        } else {
            println!("\nVersion {} not found", v);
        }
    }

    Ok(())
}

async fn cmd_init(project: &str, dataset: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Initializing tracking table in {}.{}", project, dataset);

    let client = BqClient::new(project).await?;
    let tracker = bqdrift::MigrationTracker::new(client, dataset);

    tracker.ensure_tracking_table().await?;

    println!("✓ Tracking table created: {}._bqdrift_query_runs", dataset);

    Ok(())
}

async fn cmd_sync(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    _project: &str,
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
    dry_run: bool,
    _tracking_dataset: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;
    let yaml_contents = loader.load_yaml_contents(queries_path)?;

    let today = chrono::Utc::now().date_naive();
    let from = from.unwrap_or_else(|| today - chrono::Duration::days(30));
    let to = to.unwrap_or(today);

    info!("Detecting drift from {} to {}", from, to);

    let detector = DriftDetector::new(queries.clone(), yaml_contents);
    let report = detector.detect(&[], from, to)?;

    let drifted: Vec<_> = report.needs_rerun();

    if drifted.is_empty() {
        println!("✓ All partitions are current");
        return Ok(());
    }

    let summary = report.summary();
    println!("\nDrift summary:");
    for (state, count) in &summary {
        if *state != DriftState::Current {
            let icon = match state {
                DriftState::SqlChanged => "\x1b[33m◇\x1b[0m",
                DriftState::SchemaChanged => "\x1b[31m◆\x1b[0m",
                DriftState::VersionUpgraded => "\x1b[34m▲\x1b[0m",
                DriftState::UpstreamChanged => "\x1b[35m↺\x1b[0m",
                DriftState::NeverRun => "\x1b[36m○\x1b[0m",
                DriftState::Failed => "\x1b[31m✗\x1b[0m",
                DriftState::Current => "",
            };
            println!("  {} {} {}", icon, count, state.as_str());
        }
    }

    if dry_run {
        println!("\n--- Dry run: showing drifted partitions ---\n");

        let by_query = report.by_query();
        for (query_name, partitions) in by_query {
            let drifted_partitions: Vec<_> = partitions.iter()
                .filter(|p| p.state.needs_rerun())
                .collect();

            if drifted_partitions.is_empty() {
                continue;
            }

            println!("\x1b[1m{}\x1b[0m", query_name);

            for partition in drifted_partitions {
                let state_str = match partition.state {
                    DriftState::SqlChanged => "\x1b[33msql_changed\x1b[0m",
                    DriftState::SchemaChanged => "\x1b[31mschema_changed\x1b[0m",
                    DriftState::VersionUpgraded => "\x1b[34mversion_upgraded\x1b[0m",
                    DriftState::UpstreamChanged => "\x1b[35mupstream_changed\x1b[0m",
                    DriftState::NeverRun => "\x1b[36mnever_run\x1b[0m",
                    DriftState::Failed => "\x1b[31mfailed\x1b[0m",
                    DriftState::Current => "current",
                };

                println!("  {} [{}] v{}", partition.partition_key, state_str, partition.current_version);

                if partition.state == DriftState::SqlChanged {
                    if let (Some(executed_b64), Some(current_sql)) = (&partition.executed_sql_b64, &partition.current_sql) {
                        if let Some(executed_sql) = decode_sql(executed_b64) {
                            if has_changes(&executed_sql, current_sql) {
                                println!();
                                println!("{}", format_sql_diff(&executed_sql, current_sql));
                                println!();
                            }
                        }
                    }
                }
            }
            println!();
        }

        println!("Run without --dry-run to execute {} drifted partitions", drifted.len());
    } else {
        println!("\nSync execution not yet implemented. Use --dry-run to preview changes.");
    }

    Ok(())
}
