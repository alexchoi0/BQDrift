pub mod error;
pub mod schema;
pub mod dsl;
pub mod executor;
pub mod migration;
pub mod drift;
pub mod invariant;
pub mod diff;
pub mod repl;

pub use error::{BqDriftError, Result};
pub use schema::{BqType, Field, FieldMode, Schema, PartitionConfig, PartitionType, PartitionKey, ClusterConfig};
pub use dsl::{QueryDef, VersionDef, Revision, ResolvedRevision, QueryLoader, QueryValidator, ValidationResult, SqlDependencies};
pub use executor::{PartitionWriter, Runner, BqClient};
pub use executor::{Executor, ExecutorMode, ExecutorRunner, QueryResult, ColumnDef, ColumnInfo, create_mock_executor, create_bigquery_executor};
pub use migration::MigrationTracker;
pub use drift::{Checksums, ExecutionArtifact, DriftDetector, DriftReport, DriftState, PartitionState, PartitionDrift, ExecutionStatus, compress_to_base64, decompress_from_base64, ImmutabilityChecker, ImmutabilityReport, ImmutabilityViolation, SourceAuditor, SourceAuditReport, SourceAuditEntry, SourceStatus, AuditTableRow};
pub use diff::{encode_sql, decode_sql, format_sql_diff, has_changes};
pub use invariant::{
    InvariantsRef, InvariantsDef, InvariantDef, InvariantCheck, Severity,
    InvariantChecker, CheckResult, CheckStatus, InvariantReport,
    resolve_invariants_def,
};
pub use repl::{ReplSession, ReplCommand, ReplResult, InteractiveRepl, AsyncJsonRpcServer, ServerConfig, SessionManager, SessionInfo, ServerConfigInfo};
