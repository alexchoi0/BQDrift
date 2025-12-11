# bqdrift

BigQuery schema versioning, partition management, and OLAP query orchestration.

## Overview

bqdrift manages versioned OLAP queries for BigQuery with:

- **Schema versioning** - Track schema changes over time
- **SQL revisions** - Fix bugs without creating new versions
- **Partition management** - Hourly jobs overwrite daily partitions
- **Backfill support** - Rewrite historical partitions when bugs are found
- **YAML DSL** - Define queries in readable YAML with SQL files
- **Invariant checks** - Validate data quality before/after execution
- **Drift detection** - Automatically detect when queries need re-running
- **Source immutability** - Detect unauthorized modifications to executed sources
- **DAG dependencies** - Cascade re-runs to downstream queries
- **Server-side execution** - All queries run on BigQuery, no data downloaded

## Installation

```toml
[dependencies]
bqdrift = "0.1"
```

Or install the CLI:

```bash
cargo install bqdrift
```

## CLI Usage

```bash
# Validate all query definitions
bqdrift --queries ./queries validate

# List all queries
bqdrift --queries ./queries list
bqdrift --queries ./queries list --detailed

# Show query details
bqdrift --queries ./queries show daily_user_stats

# Run all queries for today (dry run)
bqdrift --queries ./queries --project my-gcp-project run --dry-run

# Run specific query for a date
bqdrift --queries ./queries --project my-gcp-project run --query daily_user_stats --partition 2024-06-15

# Backfill a date range
bqdrift --queries ./queries --project my-gcp-project backfill daily_user_stats --from 2024-06-01 --to 2024-06-30

# Initialize tracking table
bqdrift --project my-gcp-project init --dataset bqdrift
```

### Partition Key Formats

The CLI accepts flexible partition key formats based on the query's partition type:

| Partition Type | Format | Example |
|----------------|--------|---------|
| `HOUR` | `YYYY-MM-DDTHH` | `--partition 2024-06-15T10` |
| `DAY` | `YYYY-MM-DD` | `--partition 2024-06-15` |
| `MONTH` | `YYYY-MM` | `--partition 2024-06` |
| `YEAR` | `YYYY` | `--partition 2024` |
| `RANGE` | integer | `--partition 12345` |

```bash
# Hourly partitioned query
bqdrift run --query hourly_events --partition 2024-06-15T10

# Monthly partitioned query
bqdrift backfill monthly_summary --from 2024-01 --to 2024-06

# Yearly partitioned query
bqdrift run --query annual_report --partition 2024
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `validate` | Validate queries with comprehensive checks |
| `list` | List all queries with versions |
| `show <query>` | Show detailed query info and schema |
| `run` | Run queries for a specific date |
| `backfill <query>` | Backfill a query for a date range |
| `check <query>` | Run invariant checks only (no query execution) |
| `status` | Show drift status (what needs re-running) |
| `sync` | Re-run drifted partitions |
| `audit` | Audit sources against executed SQL for modifications |
| `scratch list` | List scratch tables in a project |
| `scratch promote` | Copy scratch table to production |
| `graph` | Show query dependency graph |
| `init` | Create tracking tables in BigQuery |
| `repl` | Start interactive REPL or JSON-RPC server |

### REPL / Server Mode

bqdrift includes an interactive REPL and JSON-RPC server for programmatic access:

```bash
# Interactive REPL with tab completion
bqdrift --repl --project my-project --queries ./queries

# JSON-RPC server over stdin/stdout
bqdrift --repl --server --project my-project --queries ./queries

# Production server with resource limits
bqdrift --repl --server \
  --project my-project \
  --queries ./queries \
  --max-sessions 50 \
  --idle-timeout 300
```

**Features:**
- Session-based parallelism (parallel across sessions, sequential within)
- Auto-cleanup of idle sessions
- Per-session configuration (project/queries override)
- Health check endpoint (`ping`)

See [src/repl/README.md](src/repl/README.md) for full documentation and TypeScript client example.

### Environment Variables

- `GCP_PROJECT_ID` - Default GCP project (alternative to `--project`)
- `BQDRIFT_SCRATCH_PROJECT` - Default scratch project for testing

### Validation Checks

The `validate` command performs comprehensive validation:

**Errors (fail validation):**

| Code | Description |
|------|-------------|
| E001 | Partition field not found in schema |
| E002 | Cluster field not found in schema |
| E003 | Duplicate version number |
| E004 | RECORD field missing nested fields |

**Warnings (pass with warnings):**

| Code | Description |
|------|-------------|
| W001 | `effective_from` dates not in chronological order |
| W002 | Duplicate revision number within a version |
| W003 | Field removed between versions (breaking change) |
| W004 | Field type changed between versions |
| W005 | SQL missing `@partition_date` placeholder |
| W006 | Schema has no fields |

## Quick Start

### 1. Define a Query

**queries/analytics/daily_user_stats.yaml**

```yaml
name: daily_user_stats
destination:
  dataset: analytics
  table: daily_user_stats
  partition:
    field: date
    type: DAY
  cluster:
    - region
    - user_tier

description: Daily aggregated user statistics
owner: data-team
tags: [analytics, users, daily]

versions:
  - version: 1
    effective_from: 2024-01-15
    source: ${{ file: daily_user_stats.v1.sql }}
    schema:
      - name: date
        type: DATE
      - name: region
        type: STRING
      - name: user_tier
        type: STRING
      - name: unique_users
        type: INT64
      - name: total_events
        type: INT64
```

**queries/analytics/daily_user_stats.v1.sql**

```sql
SELECT
    DATE(created_at) AS date,
    region,
    user_tier,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events
FROM raw.events
WHERE DATE(created_at) = @partition_date
GROUP BY 1, 2, 3
```

### 2. Run Queries

```rust
use bqdrift::{QueryLoader, Runner, BqClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = BqClient::new("my-project").await?;
    let loader = QueryLoader::new();
    let queries = loader.load_dir("./queries")?;

    let runner = Runner::new(client, queries);

    // Run all queries for today
    let report = runner.run_today().await?;

    // Or backfill a date range
    let report = runner.backfill(
        "daily_user_stats",
        "2024-06-01".parse()?,
        "2024-06-30".parse()?,
    ).await?;

    Ok(())
}
```

## SQL Source Options

Query SQL can be defined as inline or via file include:

```yaml
versions:
  # Inline SQL
  - version: 1
    effective_from: 2024-01-01
    source: |
      SELECT
        DATE(created_at) AS date,
        COUNT(*) AS count
      FROM raw.events
      WHERE DATE(created_at) = @partition_date
      GROUP BY 1
    schema: [...]

  # File include (relative to YAML)
  - version: 2
    effective_from: 2024-06-01
    source: ${{ file: query.v2.sql }}
    schema: [...]
```

## File Includes

Use `${{ file: path }}` to include external YAML or SQL files. This works for any YAML value:

```yaml
# Include SQL from file
source: ${{ file: queries/complex_query.sql }}

# Include schema from file
schema: ${{ file: schemas/large_schema.yaml }}

# Include invariants from file
invariants: ${{ file: invariants/standard_checks.yaml }}
```

File paths are relative to the YAML file containing the include. Includes are processed recursively, so included files can contain their own `${{ file: }}` references.

**Circular include detection**: The preprocessor detects and prevents circular includes.

### Example: Externalizing Large Schemas

**queries/analytics/daily_stats.yaml**
```yaml
name: daily_stats
destination:
  dataset: analytics
  table: daily_stats
  partition:
    field: date
    type: DAY

versions:
  - version: 1
    effective_from: 2024-01-01
    source: ${{ file: daily_stats.v1.sql }}
    schema: ${{ file: schemas/daily_stats_schema.yaml }}
```

**queries/analytics/schemas/daily_stats_schema.yaml**
```yaml
- name: date
  type: DATE
- name: region
  type: STRING
- name: user_tier
  type: STRING
- name: unique_users
  type: INT64
- name: total_events
  type: INT64
```

## Schema Versioning

When schema changes, create a new version:

```yaml
versions:
  - version: 1
    effective_from: 2024-01-15
    source: ${{ file: daily_user_stats.v1.sql }}
    schema:
      - name: date
        type: DATE
      - name: unique_users
        type: INT64

  - version: 2
    effective_from: 2024-06-01
    source: ${{ file: daily_user_stats.v2.sql }}
    schema:
      base: ${{ versions.1.schema }}
      add:
        - name: avg_session_duration
          type: FLOAT64
          nullable: true
```

### Schema References

| Pattern | Description |
|---------|-------------|
| `schema: [...]` | Full inline schema |
| `schema: ${{ versions.1.schema }}` | Reuse schema from version 1 |
| `schema: { base: ..., add: [...] }` | Inherit and add fields |
| `schema: { base: ..., modify: [...] }` | Inherit and change field types |
| `schema: { base: ..., remove: [...] }` | Inherit and remove fields |

### Modifying Column Types

Change a column's type or properties without rewriting the full schema:

```yaml
versions:
  - version: 1
    effective_from: 2024-01-15
    source: ${{ file: query.v1.sql }}
    schema:
      - name: date
        type: DATE
      - name: count
        type: INT64

  - version: 2
    effective_from: 2024-06-01
    source: ${{ file: query.v2.sql }}
    schema:
      base: ${{ versions.1.schema }}
      modify:
        - name: count
          type: FLOAT64
          nullable: true
      add:
        - name: avg_duration
          type: FLOAT64
```

Operations are applied in order: **remove → modify → add**

## SQL Revisions

Fix SQL bugs without creating a new schema version:

```yaml
versions:
  - version: 2
    effective_from: 2024-03-01
    source: ${{ file: query.v2.sql }}
    revisions:
      - revision: 1
        effective_from: 2024-03-15
        source: ${{ file: query.v2.r1.sql }}
        reason: Fixed null handling in join
        backfill_since: 2024-03-01
      - revision: 2
        effective_from: 2024-04-01
        source: ${{ file: query.v2.r2.sql }}
        reason: Performance optimization
    schema: ${{ versions.1.schema }}
```

**Resolution logic:**
1. Find version where `effective_from <= partition_date`
2. Within that version, find latest revision where `effective_from <= today`
3. Use that SQL file

## Invariant Checks

Validate data quality with invariant checks that run **before** and/or **after** query execution:

```yaml
versions:
  - version: 1
    effective_from: 2024-01-15
    source: ${{ file: daily_user_stats.v1.sql }}
    schema: [...]

    invariants:
      before:
        - name: source_has_data
          type: row_count
          source: |
            SELECT 1 FROM raw.events
            WHERE DATE(created_at) = @partition_date
          min: 1
          severity: error

      after:
        - name: min_rows
          type: row_count
          min: 100
          severity: error

        - name: null_check
          type: null_percentage
          column: user_id
          max_percentage: 5.0
          severity: warning

        - name: count_positive
          type: value_range
          column: total_events
          min: 0
          severity: error

        - name: region_cardinality
          type: distinct_count
          column: region
          min: 1
          max: 100
          severity: warning
```

### Check Types

| Type | Description | Parameters |
|------|-------------|------------|
| `row_count` | Validate row count bounds | `min`, `max`, optional `source` |
| `null_percentage` | Check % of nulls in column | `column`, `max_percentage` |
| `value_range` | Validate min/max values for column | `column`, `min`, `max` |
| `distinct_count` | Validate column cardinality | `column`, `min`, `max` |

### Severity Levels

| Severity | Before Check Fails | After Check Fails |
|----------|-------------------|-------------------|
| `error` | Skip query execution | Mark run as failed |
| `warning` | Log warning, continue | Log warning, continue |

### Source Options

```yaml
# File include
- name: check1
  type: row_count
  source: ${{ file: checks/my_check.sql }}
  min: 1

# Inline SQL
- name: check2
  type: row_count
  source: SELECT * FROM my_table WHERE status = 'active'
  min: 10
  max: 1000

# Multiline inline SQL
- name: check3
  type: row_count
  source: |
    SELECT 1 FROM raw.events
    WHERE DATE(created_at) = @partition_date
  min: 1
```

### SQL Placeholders

| Placeholder | Description |
|-------------|-------------|
| `@partition_date` | The partition date being processed |
| `{destination}` | Full table path (`dataset.table`) |

### Invariant Inheritance

Like schemas, invariants support inheritance:

```yaml
versions:
  - version: 1
    invariants:
      after:
        - name: min_rows
          type: row_count
          min: 100
          severity: error
        - name: null_check
          type: null_percentage
          column: user_id
          max_percentage: 5.0
          severity: warning

  - version: 2
    invariants:
      base: ${{ versions.1.invariants }}
      add:
        after:
          - name: new_check
            type: row_count
            max: 1000000
            severity: warning
      modify:
        after:
          - name: min_rows
            type: row_count
            min: 500  # Increased threshold
            severity: error
      remove:
        after:
          - null_check  # Remove by name
```

### CLI Commands

```bash
# Run with invariant checks (default)
bqdrift run --query daily_user_stats --partition 2024-12-01

# Skip invariant checks
bqdrift run --query daily_user_stats --skip-invariants

# Run invariants only (no query execution)
bqdrift check daily_user_stats --partition 2024-12-01
bqdrift check daily_user_stats --before  # Only before checks
bqdrift check daily_user_stats --after   # Only after checks
```

## Partition Configuration

```yaml
# Time-based (default: DAY)
partition:
  field: date
  type: DAY  # HOUR, DAY, MONTH, YEAR

# Integer range
partition:
  field: customer_id
  type: RANGE
  start: 0
  end: 1000000
  interval: 1000

# Ingestion time
partition:
  type: INGESTION_TIME
  granularity: DAY
```

## Clustering

```yaml
cluster:
  - region      # Most filtered first
  - user_tier
  - country     # Max 4 fields
```

## Supported Types

| BigQuery Type | YAML |
|---------------|------|
| STRING | `STRING` |
| INT64 | `INT64` |
| FLOAT64 | `FLOAT64` |
| NUMERIC | `NUMERIC` |
| BIGNUMERIC | `BIGNUMERIC` |
| BOOL | `BOOL` |
| DATE | `DATE` |
| DATETIME | `DATETIME` |
| TIME | `TIME` |
| TIMESTAMP | `TIMESTAMP` |
| BYTES | `BYTES` |
| GEOGRAPHY | `GEOGRAPHY` |
| JSON | `JSON` |
| RECORD | `RECORD` |

### Nested Records

```yaml
schema:
  - name: user
    type: RECORD
    fields:
      - name: id
        type: STRING
      - name: email
        type: STRING
  - name: tags
    type: STRING
    mode: REPEATED
```

## Field Modes

```yaml
schema:
  - name: id
    type: STRING
    mode: REQUIRED    # NOT NULL
  - name: email
    type: STRING
    mode: NULLABLE    # Default
  - name: tags
    type: STRING
    mode: REPEATED    # Array
```

## Directory Structure

```
queries/
├── analytics/
│   ├── daily_user_stats.yaml
│   ├── daily_user_stats.v1.sql
│   ├── daily_user_stats.v2.sql
│   ├── daily_user_stats.v2.r1.sql
│   └── daily_user_stats.v3.sql
└── reporting/
    ├── weekly_summary.yaml
    └── weekly_summary.v1.sql
```

## DAG Dependencies

Dependencies are **automatically extracted** from SQL by parsing the AST. No manual `depends_on` configuration needed.

```sql
-- weekly_summary.v1.sql
SELECT
    DATE_TRUNC(date, WEEK) AS week_start,
    SUM(total_users) AS total_users
FROM analytics.daily_user_stats  -- auto-detected dependency
WHERE date >= @partition_date
GROUP BY 1
```

When you run `bqdrift show`, dependencies are displayed:

```
$ bqdrift show weekly_summary

Name: weekly_summary
...
Versions:
  Version 1
  effective_from: 2024-01-01
  source: weekly_summary.v1.sql
  schema: 2 fields
  dependencies (auto-detected):
    - analytics.daily_user_stats
```

When upstream queries change, downstream queries are automatically marked as stale.

### View Dependency Graph

```bash
$ bqdrift graph

raw.events
    │
    ├── analytics.daily_user_stats (v3)
    │       │
    │       └── analytics.weekly_summary (v2)
    │               │
    │               └── reporting.monthly_report (v1)
    │
    └── analytics.revenue_by_region (v2)
            │
            └── analytics.weekly_summary (v2)
```

## Drift Detection

bqdrift tracks what was executed for each partition. When YAML/SQL files change, it detects which partitions need re-running.

### Drift States

| State | Description |
|-------|-------------|
| `current` | Partition is up to date |
| `sql_changed` | SQL content changed since last run |
| `schema_changed` | Schema changed since last run |
| `version_upgraded` | New version is now effective |
| `upstream_changed` | A dependency was re-run |
| `never_run` | Partition has never been executed |
| `failed` | Last execution failed |

### Check Status

```bash
# Show drift status for all queries
$ bqdrift status

daily_user_stats:
  current: 45 partitions
  sql_changed: 15 partitions (2024-12-01 to 2024-12-15)

weekly_summary:
  current: 10 partitions
  upstream_changed: 3 partitions
    └── caused by: daily_user_stats

# Check specific date range
$ bqdrift status --from 2024-12-01 --to 2024-12-31

# Check specific query
$ bqdrift status --query daily_user_stats
```

### Sync Drifted Partitions

```bash
# Preview what will be re-run
$ bqdrift sync --dry-run

Will re-run:
  daily_user_stats: 15 partitions (sql_changed)

SQL diff:
───────────────────────────────────────
- COUNT(DISTINCT user_id) AS unique_users,
+ COUNT(DISTINCT COALESCE(user_id, 'anon')) AS unique_users,
───────────────────────────────────────

# Sync only direct changes
$ bqdrift sync --query daily_user_stats

# Sync with downstream cascade
$ bqdrift sync --query daily_user_stats --cascade

Execution plan:
  1. daily_user_stats  (15 partitions)
  2. weekly_summary    (3 partitions)
  3. monthly_report    (1 partition)

Total: 19 partitions
Proceed? [y/N]
```

### Source Audit

The `audit` command compares current source files against executed SQL stored in BigQuery to detect modifications:

```bash
# Audit all queries (default table format)
$ bqdrift audit

Source Audit Report

| Query            | Version | Source                  | Status           | Partitions | Executed                 |
|------------------|---------|-------------------------|------------------|------------|--------------------------|
| daily_user_stats | v1      | daily_user_stats.v1.sql | ✓ current        | 45         | 2024-01-15 to 2024-03-01 |
| daily_user_stats | v2      | daily_user_stats.v2.sql | ✓ current        | 90         | 2024-03-01 to 2024-06-01 |
| daily_user_stats | v3      | daily_user_stats.v3.sql | ⚠ modified       | 30         | 2024-06-01 to 2024-07-01 |
| daily_user_stats | v4      | daily_user_stats.v4.sql | ○ never_executed | -          | -                        |

Summary:
  ✓ 2 current
  ⚠ 1 modified
  ○ 1 never executed

# Audit specific query
$ bqdrift audit --query daily_user_stats

# Show only modified sources
$ bqdrift audit --modified-only

# Show SQL diffs for modified sources
$ bqdrift audit --diff

# Combine options
$ bqdrift audit --query daily_user_stats --modified-only --diff
```

#### Output Formats

Use `-o` / `--output` to choose the output format:

```bash
# Table format (default) - truncated display
$ bqdrift audit -o table

# YAML format - full SQL without truncation
$ bqdrift audit -o yaml

# JSON format - full SQL without truncation
$ bqdrift audit -o json
```

YAML and JSON formats include complete SQL content without truncation, useful for programmatic access or detailed inspection. The `stored_sql` field is only included when it differs from `current_sql` (i.e., for modified sources):

```yaml
# Current source (stored_sql omitted since it matches current_sql)
- query_name: daily_user_stats
  version: 1
  revision: null
  source: daily_user_stats.v1.sql
  status: current
  current_sql: |
    SELECT
      DATE(created_at) AS date,
      COUNT(*) AS count
    FROM raw.events
    WHERE DATE(created_at) = @partition_date
    GROUP BY 1
  partition_count: 45
  first_executed: 2024-01-15T10:00:00Z
  last_executed: 2024-03-01T10:00:00Z

# Modified source (stored_sql included to show difference)
- query_name: daily_user_stats
  version: 2
  revision: null
  source: daily_user_stats.v2.sql
  status: modified
  current_sql: |
    SELECT COUNT(DISTINCT user_id) AS count ...
  stored_sql: |
    SELECT COUNT(*) AS count ...
  partition_count: 30
  first_executed: 2024-03-01T10:00:00Z
  last_executed: 2024-06-01T10:00:00Z
```

#### Audit Status Legend

| Symbol | Status | Description |
|--------|--------|-------------|
| ✓ | `current` | Source matches executed SQL |
| ⚠ | `modified` | Source differs from executed SQL |
| ○ | `never_executed` | Source has never been run |

## Source Immutability

bqdrift enforces **source immutability** by default. Once a SQL source (version or revision) has been executed for any partition, it should not be modified. This ensures reproducibility and audit compliance.

### Why Immutability?

1. **Reproducibility** - Re-running a partition should produce the same result
2. **Audit trail** - Know exactly what SQL was executed for each partition
3. **Debugging** - Trace issues back to the exact query that ran
4. **Compliance** - Meet data governance requirements

### How It Works

When you run `bqdrift sync`, the system:

1. Retrieves the stored `executed_sql_b64` from `_bqdrift_state` for each version/revision
2. Compares it against the current SQL source on disk
3. If they differ, reports an **immutability violation**

```bash
$ bqdrift sync --from 2024-01-01 --to 2024-12-01

⚠️  IMMUTABILITY VIOLATION DETECTED

The following SQL sources have been modified after being executed:

Query: analytics/daily_stats
Version: 1
Source: daily_stats.v1.sql
Affected partitions: 182 partitions
  2024-01-01 to 2024-06-30 (182 partitions)

Diff:
───────────────────────────────────────
- SELECT COUNT(*) as count
+ SELECT COUNT(DISTINCT user_id) as count
───────────────────────────────────────

This breaks the immutability guarantee. Options:
  1. Revert your changes to the source file
  2. Create a new version with the updated SQL
  3. Create a revision under the current version with backfill_since
  4. Re-run with --allow-source-mutation to force (not recommended)
```

### Proper Ways to Change SQL

Instead of modifying an existing source file:

**Option 1: Create a new version** (schema change or major logic change)

```yaml
versions:
  - version: 1
    effective_from: 2024-01-01
    source: ${{ file: query.v1.sql }}  # Don't modify this file
    schema: [...]

  - version: 2
    effective_from: 2024-07-01  # New version takes effect
    source: ${{ file: query.v2.sql }}  # New SQL file
    schema: [...]
```

**Option 2: Create a revision** (bug fix, same schema)

```yaml
versions:
  - version: 1
    effective_from: 2024-01-01
    source: ${{ file: query.v1.sql }}  # Don't modify this file
    revisions:
      - revision: 1
        effective_from: 2024-07-01
        source: ${{ file: query.v1.r1.sql }}  # New SQL file with fix
        reason: Fixed null handling
        backfill_since: 2024-01-01  # Backfill all affected partitions
    schema: [...]
```

### Overriding Immutability

In rare cases (e.g., fixing a typo, development environments), you can override:

```bash
# Force sync despite immutability violations
$ bqdrift sync --allow-source-mutation

⚠️  Source mutation override enabled

Proceeding with modified sources. The following will be re-executed:
  - analytics/daily_stats v1: 182 partitions

Continue? [y/N]: y
```

**Warning:** Using `--allow-source-mutation` breaks the audit trail. The stored checksums will be updated, but you lose the ability to know what SQL originally ran for those partitions.

## Scratch Mode

Scratch mode provides a safe testing environment between `--dry-run` and production execution. Queries run against production source tables but write results to a scratch project for validation.

### Usage

```bash
# Run query to scratch project
bqdrift run --query daily_user_stats --partition 2024-06-15 --scratch my-scratch-project

# With custom TTL (hours)
bqdrift run --query daily_user_stats --partition 2024-06-15 --scratch my-scratch-project --scratch-ttl 48

# Using environment variable
export BQDRIFT_SCRATCH_PROJECT=my-scratch-project
bqdrift run --query daily_user_stats --partition 2024-06-15 --scratch
```

### How It Works

1. **Source tables**: Reads from production (upstream dependencies unchanged)
2. **Destination**: Writes to scratch project instead of production
3. **Invariants**: Before/after checks run against scratch tables
4. **Auto-expiration**: Tables automatically deleted by BigQuery based on TTL

### Table Naming

Scratch tables are created in a flat `bqdrift_scratch` dataset:

```
<scratch-project>.bqdrift_scratch.<dataset>__<table>
```

Example: `my-scratch.bqdrift_scratch.analytics__daily_user_stats`

### TTL / Expiration

Tables auto-expire based on partition type (or `--scratch-ttl` override):

| Partition Type | Default Expiration |
|----------------|-------------------|
| `HOUR` | End of partition hour |
| `DAY` | End of partition day |
| `MONTH` | End of partition month |
| `YEAR` | End of partition year |
| `RANGE` | 24 hours |

### Managing Scratch Tables

```bash
# List scratch tables
bqdrift scratch list --project my-scratch-project

# Promote scratch to production (copies data without re-executing query)
bqdrift scratch promote --query daily_user_stats --partition 2024-06-15 --scratch-project my-scratch-project

# Or with environment variables
export BQDRIFT_SCRATCH_PROJECT=my-scratch-project
export GCP_PROJECT_ID=my-production-project
bqdrift scratch list
bqdrift scratch promote --query daily_user_stats --partition 2024-06-15
```

### Promoting to Production

After validating data in scratch, use `scratch promote` to copy the data to production. This is more cost-effective than re-running the query because:

1. **No query re-execution**: Data is copied directly from scratch table
2. **No invariant timing issues**: Invariants already validated against the exact data being promoted
3. **Faster**: Simple MERGE copy vs full query execution

```bash
$ bqdrift scratch promote --query daily_user_stats --partition 2024-06-15 --scratch-project my-scratch

✓ Promoted daily_user_stats to production
  From: my-scratch.bqdrift_scratch.analytics__daily_user_stats
  To: my-production.analytics.daily_user_stats
  Partition: 2024-06-15
```

### Example Workflow

```bash
# 1. Run to scratch for validation
$ bqdrift run --query daily_user_stats --partition 2024-06-15 --scratch my-scratch-project

✓ daily_user_stats v3 (scratch)
  Partition: 2024-06-15
  Expires: 2024-06-16T00:00:00Z

  Invariants:
    ✓ min_rows: 12345 >= 100
    ✓ null_check: 0.0% nulls (max 5.0%)

To promote to production (copy scratch data):
  bqdrift scratch promote --query daily_user_stats --partition 2024-06-15 --scratch-project my-scratch-project

# 2. Review data in scratch if needed
# SELECT * FROM `my-scratch.bqdrift_scratch.analytics__daily_user_stats`

# 3. Promote to production
$ bqdrift scratch promote --query daily_user_stats --partition 2024-06-15 --scratch-project my-scratch-project

✓ Promoted daily_user_stats to production
```

## Tracking Tables

bqdrift creates two tables for tracking:

### `_bqdrift_state` (Current State)

```sql
CREATE TABLE _bqdrift_state (
    query_name STRING NOT NULL,
    partition_date DATE NOT NULL,
    version INT64 NOT NULL,
    sql_revision INT64,
    sql_checksum STRING NOT NULL,
    schema_checksum STRING NOT NULL,
    yaml_checksum STRING NOT NULL,
    upstream_states JSON,
    executed_at TIMESTAMP NOT NULL,
    execution_time_ms INT64,
    rows_written INT64,
    status STRING NOT NULL
) PARTITION BY partition_date
CLUSTER BY query_name
```

### `_bqdrift_history` (Audit Trail)

```sql
CREATE TABLE _bqdrift_history (
    id STRING NOT NULL,
    query_name STRING NOT NULL,
    partition_date DATE NOT NULL,
    version INT64 NOT NULL,
    sql_checksum STRING NOT NULL,
    sql_content STRING,
    executed_at TIMESTAMP NOT NULL,
    status STRING NOT NULL,
    triggered_by STRING
) PARTITION BY DATE(executed_at)
CLUSTER BY query_name, partition_date
```

## Workflow Example

```bash
# 1. Make changes to query
vim queries/analytics/daily_user_stats.v3.sql

# 2. Validate changes
bqdrift validate

# 3. Check what's affected
bqdrift status
# daily_user_stats: sql_changed (15 partitions)
# weekly_summary: upstream_changed (3 partitions)

# 4. Preview sync
bqdrift sync --cascade --dry-run

# 5. Execute sync
bqdrift sync --cascade

# 6. Verify
bqdrift status
# All current ✓
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Copyright (c) 2025 Alex Choi
