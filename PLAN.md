# bqdrift - Implementation Plan

## Overview

bqdrift is a BigQuery schema versioning and OLAP query orchestration tool with:
- YAML DSL for query definitions
- SQL file versioning with revisions
- Partition management
- **Drift detection and automatic sync**
- **DAG dependency cascade**

---

## Phase 1: Core (Completed)

- [x] YAML DSL parser with `${{ }}` variable references
- [x] Schema versioning with `base:` inheritance
- [x] SQL revisions for bug fixes
- [x] Partition and cluster configuration
- [x] BigQuery client wrapper
- [x] CLI with validate, list, show, run, backfill commands
- [x] Basic execution tracking

---

## Phase 2: Drift Detection

### Goal

Automatically detect when YAML/SQL files change and identify which partitions need re-running.

### Metadata Tables

#### `_bqdrift_state` (Current State)

Fast lookups for drift detection.

```sql
CREATE TABLE _bqdrift_state (
    query_name STRING NOT NULL,
    partition_date DATE NOT NULL,

    -- Version info
    version INT64 NOT NULL,
    sql_revision INT64,
    effective_from DATE NOT NULL,

    -- Checksums for drift detection
    sql_checksum STRING NOT NULL,       -- SHA256 of resolved SQL
    schema_checksum STRING NOT NULL,    -- SHA256 of schema JSON
    yaml_checksum STRING NOT NULL,      -- SHA256 of full YAML

    -- Upstream lineage (for cascade detection)
    upstream_states JSON,               -- {"query_name": "last_executed_at", ...}

    -- Execution info
    executed_at TIMESTAMP NOT NULL,
    execution_time_ms INT64,
    rows_written INT64,
    bytes_processed INT64,
    status STRING NOT NULL              -- SUCCESS, FAILED
)
PARTITION BY partition_date
CLUSTER BY query_name
```

#### `_bqdrift_history` (Audit Trail)

Full history for forensics and rollback analysis.

```sql
CREATE TABLE _bqdrift_history (
    id STRING NOT NULL,                 -- UUID
    query_name STRING NOT NULL,
    partition_date DATE NOT NULL,

    -- Version info
    version INT64 NOT NULL,
    sql_revision INT64,

    -- Checksums
    sql_checksum STRING NOT NULL,
    schema_checksum STRING NOT NULL,

    -- Store actual content for forensics
    sql_content STRING,

    -- Execution info
    executed_at TIMESTAMP NOT NULL,
    execution_time_ms INT64,
    rows_written INT64,
    bytes_processed INT64,
    status STRING NOT NULL,
    error_message STRING,

    -- Context
    triggered_by STRING,                -- manual, sync, backfill, cascade
    executed_by STRING                  -- hostname or user
)
PARTITION BY DATE(executed_at)
CLUSTER BY query_name, partition_date
```

### Checksum Computation

```rust
pub struct Checksums {
    pub sql: String,      // SHA256 of SQL content (with revisions resolved)
    pub schema: String,   // SHA256 of schema as JSON
    pub yaml: String,     // SHA256 of raw YAML file
}

impl Checksums {
    pub fn compute(query: &QueryDef, version: &VersionDef, yaml_content: &str) -> Self {
        Self {
            sql: sha256(version.get_sql_for_date(today)),
            schema: sha256(serde_json::to_string(&version.schema)),
            yaml: sha256(yaml_content),
        }
    }
}
```

### Drift States

| State | Condition | Action |
|-------|-----------|--------|
| `current` | All checksums match | None |
| `sql_changed` | sql_checksum differs | Re-run partition |
| `schema_changed` | schema_checksum differs | Migration + re-run |
| `version_upgraded` | New version now effective for date | Re-run with new version |
| `never_run` | No record in _bqdrift_state | Initial run needed |
| `failed` | status = FAILED | Retry needed |
| `upstream_changed` | Upstream dependency re-ran after this | Re-run (cascade) |

### Drift Detection Flow

```
1. Load all query definitions from YAML
2. Compute current checksums for each query
3. Query _bqdrift_state for specified date range
4. For each partition:
   a. No record? → never_run
   b. status = FAILED? → failed
   c. sql_checksum differs? → sql_changed
   d. schema_checksum differs? → schema_changed
   e. version effective_from changed? → version_upgraded
   f. Check upstream_states for cascade
5. Return drift report
```

### CLI Commands

```bash
# Show drift status
bqdrift status
bqdrift status --query daily_user_stats
bqdrift status --from 2024-01-01 --to 2024-12-31
bqdrift status --days 30  # Last 30 days

# Sync drifted partitions
bqdrift sync
bqdrift sync --query daily_user_stats
bqdrift sync --from 2024-12-01 --to 2024-12-15
bqdrift sync --dry-run
bqdrift sync --cascade  # Include downstream dependencies
bqdrift sync --all      # Sync everything that's drifted
```

---

## Phase 3: DAG Dependencies

### Goal

Track query dependencies and cascade re-runs when upstream changes.

### YAML Syntax

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

### Dependency Graph

```rust
pub struct DagRunner {
    queries: HashMap<String, QueryDef>,
    graph: DiGraph<String, ()>,  // petgraph
}

impl DagRunner {
    pub fn build_graph(queries: &[QueryDef]) -> Self {
        let mut graph = DiGraph::new();
        // Add nodes and edges from depends_on
    }

    pub fn topological_order(&self) -> Vec<&str> {
        // Return queries in execution order
    }

    pub fn downstream(&self, query: &str) -> Vec<&str> {
        // Return all queries that depend on this one
    }

    pub fn upstream(&self, query: &str) -> Vec<&str> {
        // Return all queries this one depends on
    }
}
```

### Cascade Detection

When checking drift for downstream queries:

```rust
fn check_upstream_changed(
    &self,
    query: &QueryDef,
    partition_date: NaiveDate,
    state: &PartitionState,
) -> bool {
    for upstream_name in &query.depends_on {
        let upstream_last_run = self.get_latest_execution(upstream_name, partition_date);
        let recorded_upstream = state.upstream_states.get(upstream_name);

        if upstream_last_run > recorded_upstream {
            return true;  // Upstream changed after this partition ran
        }
    }
    false
}
```

### Recording Upstream State

When a partition runs successfully:

```rust
fn record_execution(&self, query: &QueryDef, partition_date: NaiveDate) {
    let mut upstream_states = HashMap::new();

    for upstream_name in &query.depends_on {
        let upstream_executed_at = self.get_latest_execution(upstream_name, partition_date);
        upstream_states.insert(upstream_name, upstream_executed_at);
    }

    // Store in _bqdrift_state
}
```

### Cascade Sync

```bash
$ bqdrift sync --query daily_user_stats --cascade --dry-run

Drift detected:
  daily_user_stats: 15 partitions (sql_changed)

Cascade analysis:
  weekly_summary: 3 partitions (upstream_changed)
    └── depends on: daily_user_stats
  monthly_report: 1 partition (upstream_changed)
    └── depends on: weekly_summary

Execution plan (topological order):
  1. daily_user_stats  (15 partitions)
  2. weekly_summary    (3 partitions)
  3. monthly_report    (1 partition)

Total: 19 partitions
```

### CLI: Graph Visualization

```bash
$ bqdrift graph

raw.events
    │
    ├── analytics.daily_user_stats (v3)
    │       │
    │       ├── analytics.weekly_summary (v2)
    │       │       │
    │       │       └── reporting.monthly_report (v1)
    │       │
    │       └── analytics.user_cohorts (v1)
    │
    └── analytics.revenue_by_region (v2)
            │
            └── analytics.weekly_summary (v2)
```

---

## Phase 4: Safety Features

### Dry Run with Diff

```bash
$ bqdrift sync --dry-run

daily_user_stats: 15 partitions will be re-run

SQL changes detected:
───────────────────────────────────────
- COUNT(DISTINCT user_id) AS unique_users,
+ COUNT(DISTINCT COALESCE(user_id, 'anon')) AS unique_users,
───────────────────────────────────────

Schema unchanged.
```

### Confirmation Prompts

```bash
$ bqdrift sync --query daily_user_stats

WARNING: This will overwrite 15 partitions
  Query: daily_user_stats
  Range: 2024-12-01 to 2024-12-15
  Last successful run: 2 hours ago

Type 'yes' to confirm:
```

### Execution History

```bash
$ bqdrift history daily_user_stats --partition 2024-12-01

EXECUTED_AT          VERSION  SQL_CHECKSUM  STATUS   TRIGGERED_BY
2024-12-03 10:00:00  3        abc123        SUCCESS  sync
2024-12-02 14:00:00  3        def456        SUCCESS  sync (reverted)
2024-12-01 10:00:00  3        abc123        SUCCESS  run

$ bqdrift history daily_user_stats --partition 2024-12-01 --show-sql

# Shows full SQL content from that execution
```

---

## Implementation Tasks

### Phase 2: Drift Detection

| Task | Description | Priority |
|------|-------------|----------|
| 2.1 | Add SHA256 checksum computation | High |
| 2.2 | Create _bqdrift_state table schema | High |
| 2.3 | Create _bqdrift_history table schema | High |
| 2.4 | Record state on successful execution | High |
| 2.5 | Implement drift detection logic | High |
| 2.6 | Add `status` CLI command | High |
| 2.7 | Add `sync` CLI command | High |
| 2.8 | Add dry-run with diff | Medium |
| 2.9 | Add confirmation prompts | Medium |
| 2.10 | Add `history` CLI command | Low |

### Phase 3: DAG Dependencies

| Task | Description | Priority |
|------|-------------|----------|
| 3.1 | Parse depends_on from YAML | High |
| 3.2 | Build dependency graph | High |
| 3.3 | Implement topological sort | High |
| 3.4 | Record upstream_states on execution | High |
| 3.5 | Detect upstream_changed drift | High |
| 3.6 | Implement --cascade flag | High |
| 3.7 | Add `graph` CLI command | Medium |
| 3.8 | Validate no circular dependencies | Medium |

### Phase 4: Safety

| Task | Description | Priority |
|------|-------------|----------|
| 4.1 | SQL diff in dry-run output | Medium |
| 4.2 | Confirmation prompts for sync | Medium |
| 4.3 | Rate limiting for large syncs | Low |
| 4.4 | Rollback documentation | Low |

---

## Open Questions

1. **Default date range for status/sync** - Last N days? Require explicit range?

2. **Auto-cascade** - Should `sync` cascade by default, or require `--cascade`?

3. **Partial failure handling** - If partition 5/15 fails, continue or stop?

4. **Concurrent execution** - Run independent partitions in parallel?

5. **Schema migration** - Auto-migrate or require manual intervention?

6. **Partition date mapping** - How does weekly_summary know which daily partitions to check?

---

## Example Workflow

```bash
# 1. Make changes to query
vim queries/analytics/daily_user_stats.v3.sql

# 2. Check what's affected
bqdrift status
# daily_user_stats: sql_changed (15 partitions)
# weekly_summary: upstream_changed (3 partitions)

# 3. Preview sync
bqdrift sync --cascade --dry-run
# Shows execution plan and SQL diff

# 4. Execute sync
bqdrift sync --cascade
# Re-runs in topological order

# 5. Verify
bqdrift status
# All current ✓
```
