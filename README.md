# bqdrift

BigQuery schema versioning, partition management, and OLAP query orchestration.

## Overview

bqdrift manages versioned OLAP queries for BigQuery with:

- **Schema versioning** - Track schema changes over time
- **SQL revisions** - Fix bugs without creating new versions
- **Partition management** - Hourly jobs overwrite daily partitions
- **Backfill support** - Rewrite historical partitions when bugs are found
- **YAML DSL** - Define queries in readable YAML with SQL files
- **Drift detection** - Automatically detect when queries need re-running
- **DAG dependencies** - Cascade re-runs to downstream queries

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
bqdrift --queries ./queries --project my-gcp-project run --query daily_user_stats --date 2024-06-15

# Backfill a date range
bqdrift --queries ./queries --project my-gcp-project backfill daily_user_stats --from 2024-06-01 --to 2024-06-30

# Initialize tracking table
bqdrift --project my-gcp-project init --dataset bqdrift
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `validate` | Validate all query YAML and SQL files |
| `list` | List all queries with versions |
| `show <query>` | Show detailed query info and schema |
| `run` | Run queries for a specific date |
| `backfill <query>` | Backfill a query for a date range |
| `status` | Show drift status (what needs re-running) |
| `sync` | Re-run drifted partitions |
| `graph` | Show query dependency graph |
| `history <query>` | Show execution history |
| `init` | Create tracking tables in BigQuery |

### Environment Variables

- `GCP_PROJECT_ID` - Default GCP project (alternative to `--project`)

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
    sql: daily_user_stats.v1.sql
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

## Schema Versioning

When schema changes, create a new version:

```yaml
versions:
  - version: 1
    effective_from: 2024-01-15
    sql: daily_user_stats.v1.sql
    schema:
      - name: date
        type: DATE
      - name: unique_users
        type: INT64

  - version: 2
    effective_from: 2024-06-01
    sql: daily_user_stats.v2.sql
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
| `schema: { base: ${{ versions.1.schema }}, add: [...] }` | Inherit and add fields |
| `schema: { base: ${{ versions.1.schema }}, remove: [...] }` | Inherit and remove fields |

## SQL Revisions

Fix SQL bugs without creating a new schema version:

```yaml
versions:
  - version: 2
    effective_from: 2024-03-01
    sql: query.v2.sql
    sql_revisions:
      - revision: 1
        effective_from: 2024-03-15
        sql: query.v2.r1.sql
        reason: Fixed null handling in join
        backfill_since: 2024-03-01
      - revision: 2
        effective_from: 2024-04-01
        sql: query.v2.r2.sql
        reason: Performance optimization
    schema: ${{ versions.1.schema }}
```

**Resolution logic:**
1. Find version where `effective_from <= partition_date`
2. Within that version, find latest revision where `effective_from <= today`
3. Use that SQL file

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

Define query dependencies for cascade detection:

```yaml
name: weekly_summary
destination:
  dataset: analytics
  table: weekly_summary
  partition:
    field: week_start
    type: DAY

depends_on:
  - analytics.daily_user_stats
  - analytics.revenue_by_region

versions:
  - version: 1
    effective_from: 2024-01-01
    sql: weekly_summary.v1.sql
    schema:
      - name: week_start
        type: DATE
      - name: total_users
        type: INT64
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

### Execution History

```bash
$ bqdrift history daily_user_stats --partition 2024-12-01

EXECUTED_AT          VERSION  SQL_CHECKSUM  STATUS   TRIGGERED_BY
2024-12-03 10:00:00  3        abc123        SUCCESS  sync
2024-12-02 14:00:00  3        def456        SUCCESS  sync
2024-12-01 10:00:00  3        abc123        SUCCESS  run
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

MIT
