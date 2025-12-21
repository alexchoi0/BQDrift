mod bq_error;
mod parser;

use thiserror::Error;

pub use bq_error::{BigQueryError, QueryErrorLocation};
pub use parser::{parse_bq_error, ErrorContext};

#[derive(Error, Debug)]
pub enum BqDriftError {
    #[error("BigQuery error: {0}")]
    BigQuery(#[from] BigQueryError),

    #[error("BigQuery client error: {0}")]
    Client(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("DSL parse error: {0}")]
    DslParse(String),

    #[error("Variable resolution error: {0}")]
    VariableResolution(String),

    #[error("SQL file not found: {0}")]
    SqlFileNotFound(String),

    #[error("YAML file not found: {0}")]
    YamlFileNotFound(String),

    #[error("Invalid version reference: {0}")]
    InvalidVersionRef(String),

    #[error("Invalid revision reference: {0}")]
    InvalidRevisionRef(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Partition error: {0}")]
    Partition(String),

    #[error("Cluster error: {0}")]
    Cluster(String),

    #[error("Invariant check failed: {0}")]
    InvariantFailed(String),

    #[error("REPL error: {0}")]
    Repl(String),

    #[error("File include error: {0}")]
    FileInclude(String),

    #[error("Executor error: {0}")]
    Executor(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, BqDriftError>;
