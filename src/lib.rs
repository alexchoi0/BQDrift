pub mod error;
pub mod schema;
pub mod dsl;
pub mod executor;
pub mod migration;
pub mod drift;
pub mod invariant;
pub mod diff;

pub use error::{BqDriftError, Result};
pub use schema::{BqType, Field, FieldMode, Schema, PartitionConfig, PartitionType, PartitionKey, ClusterConfig};
pub use dsl::{QueryDef, VersionDef, Revision, ResolvedRevision, QueryLoader, QueryValidator, ValidationResult, SqlDependencies};
pub use executor::{PartitionWriter, Runner, BqClient};
pub use migration::MigrationTracker;
pub use drift::{Checksums, ExecutionArtifact, DriftDetector, DriftReport, DriftState, PartitionState, PartitionDrift, ExecutionStatus, compress_to_base64, decompress_from_base64};
pub use diff::{encode_sql, decode_sql, format_sql_diff, has_changes};
pub use invariant::{
    InvariantsRef, InvariantsDef, InvariantDef, InvariantCheck, Severity,
    InvariantChecker, CheckResult, CheckStatus, InvariantReport,
    resolve_invariants_def,
};
