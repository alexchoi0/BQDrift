use clap::{Parser, Subcommand};
use chrono::NaiveDate;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::{info, error, warn};
use tracing_subscriber::EnvFilter;

use bqdrift::{QueryLoader, QueryValidator, Runner};
use bqdrift::executor::BqClient;

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
            error!("{}", e);
            ExitCode::FAILURE
        }
    }
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

        Commands::Run { query, date, dry_run } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_run(&loader, &cli.queries, &project, query, date, dry_run).await?;
        }

        Commands::Backfill { query, from, to, dry_run } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_backfill(&loader, &cli.queries, &project, &query, from, to, dry_run).await?;
        }

        Commands::Show { query, version } => {
            cmd_show(&loader, &cli.queries, &query, version)?;
        }

        Commands::Init { dataset } => {
            let project = cli.project.ok_or("Project ID required (--project or GCP_PROJECT_ID)")?;
            cmd_init(&project, &dataset).await?;
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
            } else {
                warn!("No version found for date {}", date);
            }
        }

        return Ok(());
    }

    let client = BqClient::new(project).await?;
    let runner = Runner::new(client, queries);

    match query_name {
        Some(name) => {
            info!("Running query '{}' for date {}", name, date);
            let stats = runner.run_query(&name, date).await?;
            println!("✓ {} v{} completed for {}", stats.query_name, stats.version, stats.partition_date);
        }
        None => {
            info!("Running all queries for date {}", date);
            let report = runner.run_for_date(date).await?;

            for stats in &report.stats {
                println!("✓ {} v{}", stats.query_name, stats.version);
            }

            for failure in &report.failures {
                println!("✗ {}: {}", failure.query_name, failure.error);
            }

            println!("\n{} succeeded, {} failed", report.stats.len(), report.failures.len());
        }
    }

    Ok(())
}

async fn cmd_backfill(
    loader: &QueryLoader,
    queries_path: &PathBuf,
    project: &str,
    query_name: &str,
    from: NaiveDate,
    to: NaiveDate,
    dry_run: bool,
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

    let client = BqClient::new(project).await?;
    let runner = Runner::new(client, queries);

    let report = runner.backfill(query_name, from, to).await?;

    for stats in &report.stats {
        println!("✓ {} v{} for {}", stats.query_name, stats.version, stats.partition_date);
    }

    for failure in &report.failures {
        println!("✗ {}: {}", failure.partition_date, failure.error);
    }

    println!("\n{} succeeded, {} failed", report.stats.len(), report.failures.len());

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
