pub mod error;
pub mod schema;
pub mod dsl;
pub mod executor;
pub mod migration;
pub mod drift;

pub use error::{BqDriftError, Result};
pub use schema::{BqType, Field, FieldMode, Schema, PartitionConfig, ClusterConfig};
pub use dsl::{QueryDef, VersionDef, SqlRevision, QueryLoader};
pub use executor::{PartitionWriter, Runner, BqClient};
pub use migration::MigrationTracker;
pub use drift::{Checksums, ExecutionArtifact, DriftDetector, DriftReport, DriftState, PartitionState, compress_to_base64, decompress_from_base64};
