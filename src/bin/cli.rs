use clap::{Parser, Subcommand, ValueEnum};
use chrono::{Datelike, NaiveDate, Timelike};
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::{info, error, warn};
use tracing_subscriber::EnvFilter;

use bqdrift::{QueryLoader, QueryValidator, Runner, CheckStatus, Severity, InvariantChecker, resolve_invariants_def};
use bqdrift::{DriftDetector, DriftState, decode_sql, format_sql_diff, has_changes, ImmutabilityChecker, ImmutabilityViolation, SourceAuditor, SourceStatus, AuditTableRow};
use tabled::{Table, settings::Style};
use bqdrift::executor::BqClient;
use bqdrift::error::{BqDriftError, BigQueryError};
use bqdrift::executor::PartitionWriteStats;
use bqdrift::schema::{PartitionKey, PartitionType};

#[derive(Parser)]
#[command(name = "bqdrift")]
#[command(about = "BigQuery schema versioning and OLAP query orchestration")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to queries directory
    #[arg(short, long, default_value = "./queries")]
    queries: PathBuf,

    /// GCP project ID
    #[arg(short, long, env = "GCP_PROJECT_ID")]
    project: Option<String>,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Start interactive REPL or JSON-RPC server mode
    #[arg(long)]
    repl: bool,

    /// Force server mode (JSON-RPC over stdin/stdout) even if TTY detected
    #[arg(long, requires = "repl")]
    server: bool,

    /// Maximum number of concurrent sessions (server mode only)
    #[arg(long, default_value = "100", requires = "repl")]
    max_sessions: usize,

    /// Default session idle timeout in seconds (server mode only)
    #[arg(long, default_value = "300", requires = "repl")]
    idle_timeout: u64,

    /// Maximum allowed idle timeout in seconds (server mode only)
    #[arg(long, default_value = "3600", requires = "repl")]
    max_idle_timeout: u64,
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

    /// Run queries for a specific partition
    Run {
        /// Query name (runs all if not specified)
        #[arg(short, long)]
        query: Option<String>,

        /// Partition key (e.g., 2024-01-15, 2024-01-15T10, 2024-01, 2024, or integer for RANGE). Defaults to today.
        #[arg(short, long)]
        partition: Option<String>,

        /// Dry run - validate and show SQL without executing
        #[arg(long)]
        dry_run: bool,

        /// Skip invariant checks
        #[arg(long)]
        skip_invariants: bool,

        /// Scratch project for testing (writes to scratch instead of production)
        #[arg(long, env = "BQDRIFT_SCRATCH_PROJECT")]
        scratch: Option<String>,

        /// TTL for scratch tables in hours (default: auto based on partition type)
        #[arg(long)]
        scratch_ttl: Option<u32>,
    },

    /// Backfill a query for a date range
    Backfill {
        /// Query name
        query: String,

        /// Start partition (inclusive, e.g., 2024-01-15, 2024-01-15T10, 2024-01, 2024)
        #[arg(short, long)]
        from: String,

        /// End partition (inclusive, e.g., 2024-01-15, 2024-01-15T10, 2024-01, 2024)
        #[arg(short, long)]
        to: String,

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

        /// Partition key (e.g., 2024-01-15, 2024-01-15T10, 2024-01, 2024, or integer for RANGE). Defaults to today.
        #[arg(short, long)]
        partition: Option<String>,

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
        /// Date range start (defaults to 30 days ago, e.g., 2024-01-15)
        #[arg(short, long)]
        from: Option<String>,

        /// Date range end (defaults to today, e.g., 2024-01-15)
        #[arg(short, long)]
        to: Option<String>,

        /// Dry run - show what would be synced with SQL diffs
        #[arg(long)]
        dry_run: bool,

        /// Skip invariant checks when syncing
        #[arg(long)]
        skip_invariants: bool,

        /// Dataset for tracking table
        #[arg(long, default_value = "bqdrift")]
        tracking_dataset: String,

        /// Allow modifying SQL sources that have already been executed (breaks immutability)
        #[arg(long)]
        allow_source_mutation: bool,
    },

    /// Audit source files against executed SQL to detect modifications
    Audit {
        /// Query name (audits all if not specified)
        #[arg(short, long)]
        query: Option<String>,

        /// Show only modified sources
        #[arg(long)]
        modified_only: bool,

        /// Show SQL diff for modified sources
        #[arg(long)]
        diff: bool,

        /// Output format: table, yaml, json
        #[arg(short, long, default_value = "table")]
        output: OutputFormat,

        /// Dataset for tracking table
        #[arg(long, default_value = "bqdrift")]
        tracking_dataset: String,
    },

    /// Manage scratch tables
    Scratch {
        #[command(subcommand)]
        action: ScratchAction,
    },
}

#[derive(Subcommand)]
enum ScratchAction {
    /// List scratch tables
    List {
        /// Scratch project
        #[arg(long, env = "BQDRIFT_SCRATCH_PROJECT")]
        project: String,
    },
    /// Promote scratch table to production (copy without re-executing query)
    Promote {
        /// Query name
        #[arg(long)]
        query: String,

        /// Partition key (e.g., 2024-01-15)
        #[arg(long)]
        partition: String,

        /// Scratch project
        #[arg(long, env = "BQDRIFT_SCRATCH_PROJECT")]
        scratch_project: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Table,
    Yaml,
    Json,
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

fn parse_partition_key(s: &str, partition_type: &PartitionType) -> Result<PartitionKey, Box<dyn std::error::Error>> {
    PartitionKey::parse(s, partition_type)
        .map_err(|e| e.into())
}

fn default_partition_key(partition_type: &PartitionType) -> PartitionKey {
    let today = chrono::Utc::now().date_naive();
    match partition_type {
        PartitionType::Hour => {
            let now = chrono::Utc::now().naive_utc();
            PartitionKey::Hour(now.date().and_hms_opt(now.time().hour(), 0, 0).unwrap())
        }
        PartitionType::Day | PartitionType::IngestionTime => PartitionKey::Day(today),
        PartitionType::Month => PartitionKey::Month { year: today.year(), month: today.month() },
        PartitionType::Year => PartitionKey::Year(today.year()),
        PartitionType::Range => PartitionKey::Range(0),
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    if cli.repl {
        return run_repl(cli).await;
    }

    let command = cli.command.ok_or("No command specified. Use --help for usage or --repl for interactive mode.")?;

    let loader = QueryLoader::new();

    match command {
        Commands::Validate => {
            cmd_validate(&loader, &cli.queries)?;
        }

        Commands::List { detailed } => {
            cmd_list(&loader, &cli.queries, detailed)?;
        }

        Commands::Run { query, partition, dry_run, skip_invariants, scratch, scratch_ttl } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_run(&loader, &cli.queries, &project, query, partition, dry_run, skip_invariants, scratch, scratch_ttl).await?;
        }

        Commands::Backfill { query, from, to, dry_run, skip_invariants } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_backfill(&loader, &cli.queries, &project, &query, from, to, dry_run, skip_invariants).await?;
        }

        Commands::Check { query, partition, before, after } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_check(&loader, &cli.queries, &project, &query, partition, before, after).await?;
        }

        Commands::Show { query, version } => {
            cmd_show(&loader, &cli.queries, &query, version)?;
        }

        Commands::Init { dataset } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_init(&project, &dataset).await?;
        }

        Commands::Sync { from, to, dry_run, skip_invariants: _, tracking_dataset, allow_source_mutation } => {
            let project = if dry_run {
                cli.project.unwrap_or_default()
            } else {
                cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?
            };
            cmd_sync(&loader, &cli.queries, &project, from, to, dry_run, &tracking_dataset, allow_source_mutation).await?;
        }

        Commands::Audit { query, modified_only, diff, output, tracking_dataset: _ } => {
            cmd_audit(&loader, &cli.queries, query, modified_only, diff, output)?;
        }

        Commands::Scratch { action } => {
            match action {
                ScratchAction::List { project } => {
                    cmd_scratch_list(&project).await?;
                }
                ScratchAction::Promote { query, partition, scratch_project } => {
                    let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
                    cmd_scratch_promote(&loader, &cli.queries, &project, &scratch_project, &query, &partition).await?;
                }
            }
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
            let revisions = version.revisions.len();

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
    partition: Option<String>,
    dry_run: bool,
    skip_invariants: bool,
    scratch: Option<String>,
    scratch_ttl: Option<u32>,
) -> Result<(), Box<dyn std::error::Error>> {
    use bqdrift::executor::{ScratchConfig, ScratchWriter};

    let queries = loader.load_dir(queries_path)?;

    if dry_run {
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
            let partition_type = &query.destination.partition.partition_type;
            let partition_key = match &partition {
                Some(p) => parse_partition_key(p, partition_type)?,
                None => default_partition_key(partition_type),
            };

            info!("Dry run for partition: {}", partition_key);
            println!("Query: {}", query.name);
            println!("Destination: {}.{}", query.destination.dataset, query.destination.table);
            println!("Partition type: {:?}", partition_type);

            let date_for_version = partition_key.to_naive_date();
            if let Some(version) = query.get_version_for_date(date_for_version) {
                println!("Version: {}", version.version);
                println!("Source: {}", version.source);
                println!("\n--- SQL ---\n{}\n-----------\n", version.get_sql_for_date(date_for_version));

                if !skip_invariants {
                    let before_count = version.invariants.before.len();
                    let after_count = version.invariants.after.len();
                    if before_count > 0 || after_count > 0 {
                        println!("Invariants: {} before, {} after", before_count, after_count);
                    }
                }
            } else {
                warn!("No version found for date {}", date_for_version);
            }
        }

        return Ok(());
    }

    if skip_invariants {
        info!("Running with invariants skipped");
    }

    if let Some(scratch_project) = scratch {
        let query_name = query_name.ok_or("Query name required for scratch mode (--query)")?;

        let query = queries.iter()
            .find(|q| q.name == query_name)
            .ok_or_else(|| format!("Query '{}' not found", query_name))?;

        let partition_type = &query.destination.partition.partition_type;
        let partition_key = match &partition {
            Some(p) => parse_partition_key(p, partition_type)?,
            None => default_partition_key(partition_type),
        };

        info!("Running in scratch mode");
        info!("Scratch project: {}", scratch_project);

        let scratch_client = BqClient::new(&scratch_project).await?;
        let mut config = ScratchConfig::new(scratch_project.clone());
        if let Some(ttl) = scratch_ttl {
            config = config.with_ttl(ttl);
        }

        let scratch_writer = ScratchWriter::new(scratch_client, config);
        scratch_writer.ensure_dataset().await?;

        info!("Writing to scratch table: {}", scratch_writer.scratch_table_fqn(query));

        let stats = scratch_writer.write_partition(query, partition_key, !skip_invariants).await?;

        println!("\n✓ {} v{} completed (scratch)", stats.query_name, stats.version);
        println!("  Destination: {}", stats.scratch_table);
        println!("  Partition: {}", stats.partition_key);
        println!("  Expires: {}", stats.expiration.format("%Y-%m-%dT%H:%M:%SZ"));

        if !skip_invariants {
            if let Some(report) = &stats.invariant_report {
                print_scratch_invariants(report);
            }
        }

        println!("\nTo promote to production (copy scratch data):");
        println!("  bqdrift scratch promote --query {} --partition {} --scratch-project {}", stats.query_name, stats.partition_key, scratch_project);

        return Ok(());
    }

    match query_name {
        Some(name) => {
            let query = queries.iter()
                .find(|q| q.name == name)
                .ok_or_else(|| format!("Query '{}' not found", name))?;
            let partition_type = &query.destination.partition.partition_type;
            let partition_key = match &partition {
                Some(p) => parse_partition_key(p, partition_type)?,
                None => default_partition_key(partition_type),
            };

            let client = BqClient::new(project).await?;
            let runner = Runner::new(client, queries);

            info!("Running query '{}' for partition {}", name, partition_key);
            let stats = runner.run_query_partition(&name, partition_key).await?;
            print_stats(&stats, skip_invariants);
        }
        None => {
            let partition_key = match &partition {
                Some(p) => parse_partition_key(p, &PartitionType::Day)?,
                None => default_partition_key(&PartitionType::Day),
            };

            let client = BqClient::new(project).await?;
            let runner = Runner::new(client, queries);

            info!("Running all queries for partition {}", partition_key);
            let report = runner.run_for_partition(partition_key).await?;

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

fn print_scratch_invariants(report: &bqdrift::invariant::InvariantReport) {
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
            _ => {}
        }
    }

    if passed > 0 || failed_warnings > 0 || failed_errors > 0 {
        println!("\n  Invariants:");
        for result in report.before.iter().chain(report.after.iter()) {
            let icon = match result.status {
                CheckStatus::Passed => "✓",
                CheckStatus::Failed => if result.severity == Severity::Warning { "⚠" } else { "✗" },
                _ => "?",
            };
            println!("    {} {}: {}", icon, result.name, result.message);
        }
    }
}

fn print_stats(stats: &PartitionWriteStats, skip_invariants: bool) {
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
    from: String,
    to: String,
    dry_run: bool,
    skip_invariants: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;

    let query = queries.iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("Query '{}' not found", query_name))?;

    let partition_type = &query.destination.partition.partition_type;
    let from_key = parse_partition_key(&from, partition_type)?;
    let to_key = parse_partition_key(&to, partition_type)?;

    info!("Backfilling '{}' from {} to {}", query_name, from_key, to_key);

    if dry_run {
        let mut current = from_key.clone();
        while current <= to_key {
            let date = current.to_naive_date();
            if let Some(version) = query.get_version_for_date(date) {
                println!("{}: v{} ({})", current, version.version, version.source);
            } else {
                println!("{}: no version available", current);
            }
            current = current.next();
        }
        return Ok(());
    }

    if skip_invariants {
        info!("Running with invariants skipped");
    }

    let client = BqClient::new(project).await?;
    let runner = Runner::new(client, queries);

    let report = runner.backfill_partitions(query_name, from_key, to_key, None).await?;

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
    partition: Option<String>,
    run_before: bool,
    run_after: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;

    let query = queries.iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("Query '{}' not found", query_name))?;

    let partition_type = &query.destination.partition.partition_type;
    let partition_key = match &partition {
        Some(p) => parse_partition_key(p, partition_type)?,
        None => default_partition_key(partition_type),
    };
    let date_for_version = partition_key.to_naive_date();

    let version = query.get_version_for_date(date_for_version)
        .ok_or_else(|| format!("No version found for date {}", date_for_version))?;

    let (before_checks, after_checks) = resolve_invariants_def(&version.invariants);

    let run_all = !run_before && !run_after;

    let client = BqClient::new(project).await?;
    let checker = InvariantChecker::new(&client, &query.destination, date_for_version);

    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut has_errors = false;

    println!("Running invariant checks for '{}' v{} on {}", query.name, version.version, partition_key);
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
        println!("  source: {}", version.source);

        if let Some(desc) = &version.description {
            println!("  description: {}", desc);
        }

        if let Some(backfill) = &version.backfill_since {
            println!("  backfill_since: {}", backfill);
        }

        if !version.revisions.is_empty() {
            println!("  revisions:");
            for rev in &version.revisions {
                print!("    r{}: {} ({})", rev.revision, rev.source, rev.effective_from);
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
    from: Option<String>,
    to: Option<String>,
    dry_run: bool,
    _tracking_dataset: &str,
    allow_source_mutation: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;
    let yaml_contents = loader.load_yaml_contents(queries_path)?;

    let today = chrono::Utc::now().date_naive();
    let from = match from {
        Some(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
            .map_err(|_| format!("Invalid date format: '{}'. Expected YYYY-MM-DD", s))?,
        None => today - chrono::Duration::days(30),
    };
    let to = match to {
        Some(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
            .map_err(|_| format!("Invalid date format: '{}'. Expected YYYY-MM-DD", s))?,
        None => today,
    };

    info!("Detecting drift from {} to {}", from, to);

    // TODO: Fetch stored states from BigQuery tracking table
    // For now, we pass empty states (no immutability check possible without stored states)
    let stored_states = vec![];

    if !allow_source_mutation && !stored_states.is_empty() {
        let immutability_checker = ImmutabilityChecker::new(&queries);
        let immutability_report = immutability_checker.check(&stored_states);

        if !immutability_report.is_clean() {
            print_immutability_violations(&immutability_report.violations);
            return Err("Source immutability violated. Use --allow-source-mutation to override.".into());
        }
    }

    let detector = DriftDetector::new(queries.clone(), yaml_contents);
    let report = detector.detect(&stored_states, from, to)?;

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

fn print_immutability_violations(violations: &[ImmutabilityViolation]) {
    eprintln!("\n\x1b[31m⚠️  IMMUTABILITY VIOLATION DETECTED\x1b[0m\n");
    eprintln!("The following SQL sources have been modified after being executed:\n");

    for violation in violations {
        eprintln!("\x1b[1mQuery:\x1b[0m {}", violation.query_name);
        eprintln!("\x1b[1mVersion:\x1b[0m {}", violation.version);
        if let Some(rev) = violation.revision {
            eprintln!("\x1b[1mRevision:\x1b[0m {}", rev);
        }
        eprintln!("\x1b[1mSource:\x1b[0m {}", violation.source);
        eprintln!("\x1b[1mAffected partitions:\x1b[0m {} partitions", violation.affected_partitions.len());

        if violation.affected_partitions.len() <= 5 {
            for date in &violation.affected_partitions {
                eprintln!("  - {}", date);
            }
        } else {
            let first = violation.affected_partitions.first().unwrap();
            let last = violation.affected_partitions.last().unwrap();
            eprintln!("  {} to {} ({} partitions)", first, last, violation.affected_partitions.len());
        }

        eprintln!();
        eprintln!("\x1b[1mDiff:\x1b[0m");
        eprintln!("{}", format_sql_diff(&violation.stored_sql, &violation.current_sql));
        eprintln!();
    }

    eprintln!("This breaks the immutability guarantee. Options:");
    eprintln!("  1. Revert your changes to the source file");
    eprintln!("  2. Create a new version with the updated SQL");
    eprintln!("  3. Create a revision under the current version with backfill_since");
    eprintln!("  4. Re-run with \x1b[33m--allow-source-mutation\x1b[0m to force (not recommended)");
    eprintln!();
}

fn cmd_audit(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    query_filter: Option<String>,
    modified_only: bool,
    show_diff: bool,
    output: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = loader.load_dir(queries_path)?;

    let queries_to_audit: Vec<_> = match &query_filter {
        Some(name) => queries.iter().filter(|q| &q.name == name).cloned().collect(),
        None => queries,
    };

    if queries_to_audit.is_empty() {
        if let Some(name) = query_filter {
            return Err(format!("Query '{}' not found", name).into());
        }
        println!("No queries found in {}", queries_path.display());
        return Ok(());
    }

    info!("Auditing {} queries", queries_to_audit.len());

    // TODO: Fetch stored states from BigQuery tracking table
    // For now, we pass empty states (demonstration mode)
    let stored_states = vec![];

    let auditor = SourceAuditor::new(&queries_to_audit);
    let report = auditor.audit(&stored_states);

    let entries_to_show: Vec<_> = if modified_only {
        report.entries.iter().filter(|e| e.status == SourceStatus::Modified).cloned().collect()
    } else {
        report.entries.clone()
    };

    if entries_to_show.is_empty() {
        if modified_only {
            println!("✓ No modified sources found");
        } else {
            println!("No source entries to display");
        }
        return Ok(());
    }

    match output {
        OutputFormat::Yaml => {
            let yaml = serde_yaml::to_string(&entries_to_show)?;
            println!("{}", yaml);
        }
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&entries_to_show)?;
            println!("{}", json);
        }
        OutputFormat::Table => {
            println!("\nSource Audit Report\n");

            let rows: Vec<AuditTableRow> = entries_to_show.iter().map(|e| AuditTableRow::from(e)).collect();
            let mut table = Table::new(rows);
            table.with(Style::markdown());
            println!("{}", table);

            println!("\nSummary:");
            println!("  ✓ {} current", report.current_count());
            println!("  ⚠ {} modified", report.modified_count());
            println!("  ○ {} never executed", report.never_executed_count());

            if show_diff && report.has_modifications() {
                println!("\nModified Sources:\n");
                for entry in report.modified_entries() {
                    if let Some(stored_sql) = &entry.stored_sql {
                        let version_str = match entry.revision {
                            Some(rev) => format!("v{}.r{}", entry.version, rev),
                            None => format!("v{}", entry.version),
                        };
                        println!("{}  {} ({})", entry.query_name, version_str, entry.source);
                        println!("{}", format_sql_diff(stored_sql, &entry.current_sql));
                        println!();
                    }
                }
            }

            if report.has_modifications() {
                println!("Warning: Modified sources detected. Consider:");
                println!("  - Creating new versions for schema changes");
                println!("  - Creating revisions for bug fixes");
                println!("  - Running 'bqdrift sync --allow-source-mutation' to force update");
            }
        }
    }

    Ok(())
}

async fn cmd_scratch_list(project: &str) -> Result<(), Box<dyn std::error::Error>> {
    use bqdrift::executor::{ScratchConfig, ScratchWriter};

    let client = BqClient::new(project).await?;
    let config = ScratchConfig::new(project.to_string());
    let writer = ScratchWriter::new(client, config);

    let tables = writer.list_tables().await?;

    if tables.is_empty() {
        println!("No scratch tables found in {}.bqdrift_scratch", project);
    } else {
        println!("Scratch tables in {}.bqdrift_scratch:\n", project);
        for table in tables {
            println!("  {}", table);
        }
    }

    Ok(())
}

async fn cmd_scratch_promote(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    production_project: &str,
    scratch_project: &str,
    query_name: &str,
    partition_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use bqdrift::executor::{ScratchConfig, ScratchWriter};

    let queries = loader.load_dir(queries_path)?;

    let query = queries.iter()
        .find(|q| q.name == query_name)
        .ok_or_else(|| format!("Query '{}' not found", query_name))?;

    let partition_type = &query.destination.partition.partition_type;
    let partition_key = parse_partition_key(partition_str, partition_type)?;

    info!("Promoting scratch to production");
    info!("  Scratch project: {}", scratch_project);
    info!("  Production project: {}", production_project);

    let scratch_client = BqClient::new(scratch_project).await?;
    let production_client = BqClient::new(production_project).await?;

    let config = ScratchConfig::new(scratch_project.to_string());
    let scratch_writer = ScratchWriter::new(scratch_client, config);

    let stats = scratch_writer.promote_to_production(query, &partition_key, &production_client).await?;

    println!("\n✓ Promoted {} to production", stats.query_name);
    println!("  From: {}", stats.scratch_table);
    println!("  To: {}", stats.production_table);
    println!("  Partition: {}", stats.partition_key);

    Ok(())
}

async fn run_repl(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    use bqdrift::repl::{ReplSession, InteractiveRepl, AsyncJsonRpcServer, ServerConfig};

    let is_tty = atty::is(atty::Stream::Stdin);
    let force_server = cli.server;

    if is_tty && !force_server {
        let session = ReplSession::new(cli.project, cli.queries);
        let mut repl = InteractiveRepl::new(session)?;
        repl.run().await?;
    } else {
        let config = ServerConfig::new(cli.project, cli.queries)
            .with_max_sessions(cli.max_sessions)
            .with_idle_timeout(cli.idle_timeout)
            .with_max_idle_timeout(cli.max_idle_timeout);
        AsyncJsonRpcServer::run(config).await?;
    }

    Ok(())
}
