use std::fmt;

#[derive(Debug, Clone)]
pub enum BigQueryError {
    AuthenticationFailed {
        reason: String,
        help: String,
    },

    InvalidQuery {
        sql_preview: String,
        message: String,
        location: Option<QueryErrorLocation>,
    },

    TableNotFound {
        project: String,
        dataset: String,
        table: String,
    },

    DatasetNotFound {
        project: String,
        dataset: String,
    },

    AccessDenied {
        resource: String,
        required_permission: Option<String>,
    },

    QuotaExceeded {
        quota_type: String,
        message: String,
    },

    ResourcesExceeded {
        message: String,
        suggestion: String,
    },

    Timeout {
        operation: String,
        duration_ms: Option<u64>,
    },

    SchemaMismatch {
        message: String,
        field: Option<String>,
    },

    ConnectionFailed {
        reason: String,
    },

    InvalidCredentials {
        path: Option<String>,
        reason: String,
    },

    Unknown {
        code: Option<String>,
        message: String,
        raw_error: String,
    },
}

#[derive(Debug, Clone)]
pub struct QueryErrorLocation {
    pub line: Option<u32>,
    pub column: Option<u32>,
    pub offset: Option<u32>,
}

impl BigQueryError {
    pub fn suggestion(&self) -> String {
        match self {
            BigQueryError::AuthenticationFailed { .. } => {
                "Try:\n  \
                 • Run: gcloud auth application-default login\n  \
                 • Or set GOOGLE_APPLICATION_CREDENTIALS to your service account key file".to_string()
            }

            BigQueryError::InvalidQuery { .. } => {
                "Check your SQL for:\n  \
                 • Syntax errors (typos, missing keywords)\n  \
                 • Correct table/column names\n  \
                 • Proper quoting for identifiers".to_string()
            }

            BigQueryError::TableNotFound { project, dataset, table } => {
                format!(
                    "Verify the table exists:\n  \
                     • Run: bq show {project}:{dataset}.{table}\n  \
                     • Check for typos in the table name\n  \
                     • Ensure you have access to the dataset"
                )
            }

            BigQueryError::DatasetNotFound { project, dataset } => {
                format!(
                    "Verify the dataset exists:\n  \
                     • Run: bq show {project}:{dataset}\n  \
                     • Check for typos in the dataset name\n  \
                     • Ensure you have access to the project"
                )
            }

            BigQueryError::AccessDenied { resource, required_permission } => {
                let perm = required_permission.as_deref().unwrap_or("bigquery.tables.getData");
                format!(
                    "Request access to {resource}:\n  \
                     • Required permission: {perm}\n  \
                     • Contact your project admin\n  \
                     • Or run: gcloud projects add-iam-policy-binding PROJECT_ID \\\n    \
                       --member=user:YOUR_EMAIL --role=roles/bigquery.dataViewer"
                )
            }

            BigQueryError::QuotaExceeded { quota_type, .. } => {
                format!(
                    "Quota '{quota_type}' exceeded:\n  \
                     • Wait and retry later\n  \
                     • Request quota increase in Cloud Console\n  \
                     • Optimize query to use fewer resources"
                )
            }

            BigQueryError::ResourcesExceeded { suggestion, .. } => {
                suggestion.clone()
            }

            BigQueryError::Timeout { operation, .. } => {
                format!(
                    "Operation '{operation}' timed out:\n  \
                     • Reduce query complexity\n  \
                     • Add filters to reduce data scanned\n  \
                     • Consider partitioning your tables"
                )
            }

            BigQueryError::SchemaMismatch { field, .. } => {
                let field_info = field.as_ref().map(|f| format!(" for field '{f}'")).unwrap_or_default();
                format!(
                    "Schema mismatch{field_info}:\n  \
                     • Check column types match expected schema\n  \
                     • Verify nullable/required settings\n  \
                     • Run: bq show --schema PROJECT:DATASET.TABLE"
                )
            }

            BigQueryError::ConnectionFailed { .. } => {
                "Connection failed:\n  \
                 • Check your internet connection\n  \
                 • Verify BigQuery API is enabled for your project\n  \
                 • Try again in a few moments".to_string()
            }

            BigQueryError::InvalidCredentials { path, .. } => {
                let path_info = path.as_ref()
                    .map(|p| format!(" ({})", p))
                    .unwrap_or_default();
                format!(
                    "Invalid credentials{path_info}:\n  \
                     • Check GOOGLE_APPLICATION_CREDENTIALS path\n  \
                     • Verify the service account key is valid\n  \
                     • Run: gcloud auth application-default login"
                )
            }

            BigQueryError::Unknown { .. } => {
                "An unexpected error occurred:\n  \
                 • Check the error message for details\n  \
                 • Verify your BigQuery configuration\n  \
                 • Check BigQuery status: https://status.cloud.google.com/".to_string()
            }
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            BigQueryError::AuthenticationFailed { .. } => "AUTH_FAILED",
            BigQueryError::InvalidQuery { .. } => "INVALID_QUERY",
            BigQueryError::TableNotFound { .. } => "TABLE_NOT_FOUND",
            BigQueryError::DatasetNotFound { .. } => "DATASET_NOT_FOUND",
            BigQueryError::AccessDenied { .. } => "ACCESS_DENIED",
            BigQueryError::QuotaExceeded { .. } => "QUOTA_EXCEEDED",
            BigQueryError::ResourcesExceeded { .. } => "RESOURCES_EXCEEDED",
            BigQueryError::Timeout { .. } => "TIMEOUT",
            BigQueryError::SchemaMismatch { .. } => "SCHEMA_MISMATCH",
            BigQueryError::ConnectionFailed { .. } => "CONNECTION_FAILED",
            BigQueryError::InvalidCredentials { .. } => "INVALID_CREDENTIALS",
            BigQueryError::Unknown { .. } => "UNKNOWN",
        }
    }
}

impl fmt::Display for BigQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BigQueryError::AuthenticationFailed { reason, .. } => {
                write!(f, "Authentication failed: {reason}")
            }

            BigQueryError::InvalidQuery { message, sql_preview, location } => {
                write!(f, "Invalid SQL: {message}")?;
                if let Some(loc) = location {
                    if let Some(line) = loc.line {
                        write!(f, " (line {line}")?;
                        if let Some(col) = loc.column {
                            write!(f, ", column {col}")?;
                        }
                        write!(f, ")")?;
                    }
                }
                if !sql_preview.is_empty() {
                    write!(f, "\n\nSQL preview:\n  {sql_preview}")?;
                }
                Ok(())
            }

            BigQueryError::TableNotFound { project, dataset, table } => {
                write!(f, "Table not found: {project}.{dataset}.{table}")
            }

            BigQueryError::DatasetNotFound { project, dataset } => {
                write!(f, "Dataset not found: {project}.{dataset}")
            }

            BigQueryError::AccessDenied { resource, required_permission } => {
                write!(f, "Access denied to {resource}")?;
                if let Some(perm) = required_permission {
                    write!(f, " (requires {perm})")?;
                }
                Ok(())
            }

            BigQueryError::QuotaExceeded { quota_type, message } => {
                write!(f, "Quota exceeded ({quota_type}): {message}")
            }

            BigQueryError::ResourcesExceeded { message, .. } => {
                write!(f, "Resources exceeded: {message}")
            }

            BigQueryError::Timeout { operation, duration_ms } => {
                write!(f, "Timeout during {operation}")?;
                if let Some(ms) = duration_ms {
                    write!(f, " (after {}ms)", ms)?;
                }
                Ok(())
            }

            BigQueryError::SchemaMismatch { message, field } => {
                write!(f, "Schema mismatch")?;
                if let Some(fld) = field {
                    write!(f, " on field '{fld}'")?;
                }
                write!(f, ": {message}")
            }

            BigQueryError::ConnectionFailed { reason } => {
                write!(f, "Connection failed: {reason}")
            }

            BigQueryError::InvalidCredentials { reason, path } => {
                write!(f, "Invalid credentials: {reason}")?;
                if let Some(p) = path {
                    write!(f, " (path: {p})")?;
                }
                Ok(())
            }

            BigQueryError::Unknown { code, message, .. } => {
                if let Some(c) = code {
                    write!(f, "BigQuery error [{c}]: {message}")
                } else {
                    write!(f, "BigQuery error: {message}")
                }
            }
        }
    }
}

impl std::error::Error for BigQueryError {}
