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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(BigQueryError::AuthenticationFailed {
            reason: "test".into(),
            help: "help".into(),
        }.error_code(), "AUTH_FAILED");

        assert_eq!(BigQueryError::InvalidQuery {
            sql_preview: "".into(),
            message: "".into(),
            location: None,
        }.error_code(), "INVALID_QUERY");

        assert_eq!(BigQueryError::TableNotFound {
            project: "p".into(),
            dataset: "d".into(),
            table: "t".into(),
        }.error_code(), "TABLE_NOT_FOUND");

        assert_eq!(BigQueryError::DatasetNotFound {
            project: "p".into(),
            dataset: "d".into(),
        }.error_code(), "DATASET_NOT_FOUND");

        assert_eq!(BigQueryError::AccessDenied {
            resource: "r".into(),
            required_permission: None,
        }.error_code(), "ACCESS_DENIED");

        assert_eq!(BigQueryError::QuotaExceeded {
            quota_type: "q".into(),
            message: "m".into(),
        }.error_code(), "QUOTA_EXCEEDED");

        assert_eq!(BigQueryError::ResourcesExceeded {
            message: "m".into(),
            suggestion: "s".into(),
        }.error_code(), "RESOURCES_EXCEEDED");

        assert_eq!(BigQueryError::Timeout {
            operation: "o".into(),
            duration_ms: None,
        }.error_code(), "TIMEOUT");

        assert_eq!(BigQueryError::SchemaMismatch {
            message: "m".into(),
            field: None,
        }.error_code(), "SCHEMA_MISMATCH");

        assert_eq!(BigQueryError::ConnectionFailed {
            reason: "r".into(),
        }.error_code(), "CONNECTION_FAILED");

        assert_eq!(BigQueryError::InvalidCredentials {
            path: None,
            reason: "r".into(),
        }.error_code(), "INVALID_CREDENTIALS");

        assert_eq!(BigQueryError::Unknown {
            code: None,
            message: "m".into(),
            raw_error: "r".into(),
        }.error_code(), "UNKNOWN");
    }

    #[test]
    fn test_display_authentication_failed() {
        let err = BigQueryError::AuthenticationFailed {
            reason: "No token available".into(),
            help: "Run gcloud auth".into(),
        };
        assert_eq!(err.to_string(), "Authentication failed: No token available");
    }

    #[test]
    fn test_display_invalid_query_with_location() {
        let err = BigQueryError::InvalidQuery {
            sql_preview: "SELECT * FROM".into(),
            message: "Syntax error".into(),
            location: Some(QueryErrorLocation {
                line: Some(1),
                column: Some(15),
                offset: None,
            }),
        };
        let display = err.to_string();
        assert!(display.contains("Invalid SQL: Syntax error"));
        assert!(display.contains("line 1"));
        assert!(display.contains("column 15"));
        assert!(display.contains("SELECT * FROM"));
    }

    #[test]
    fn test_display_invalid_query_without_location() {
        let err = BigQueryError::InvalidQuery {
            sql_preview: "".into(),
            message: "Unknown error".into(),
            location: None,
        };
        assert_eq!(err.to_string(), "Invalid SQL: Unknown error");
    }

    #[test]
    fn test_display_table_not_found() {
        let err = BigQueryError::TableNotFound {
            project: "my-project".into(),
            dataset: "my_dataset".into(),
            table: "my_table".into(),
        };
        assert_eq!(err.to_string(), "Table not found: my-project.my_dataset.my_table");
    }

    #[test]
    fn test_display_dataset_not_found() {
        let err = BigQueryError::DatasetNotFound {
            project: "my-project".into(),
            dataset: "my_dataset".into(),
        };
        assert_eq!(err.to_string(), "Dataset not found: my-project.my_dataset");
    }

    #[test]
    fn test_display_access_denied_with_permission() {
        let err = BigQueryError::AccessDenied {
            resource: "project.dataset.table".into(),
            required_permission: Some("bigquery.tables.getData".into()),
        };
        let display = err.to_string();
        assert!(display.contains("Access denied to project.dataset.table"));
        assert!(display.contains("requires bigquery.tables.getData"));
    }

    #[test]
    fn test_display_access_denied_without_permission() {
        let err = BigQueryError::AccessDenied {
            resource: "my_resource".into(),
            required_permission: None,
        };
        assert_eq!(err.to_string(), "Access denied to my_resource");
    }

    #[test]
    fn test_display_quota_exceeded() {
        let err = BigQueryError::QuotaExceeded {
            quota_type: "daily query limit".into(),
            message: "Exceeded 1TB".into(),
        };
        assert_eq!(err.to_string(), "Quota exceeded (daily query limit): Exceeded 1TB");
    }

    #[test]
    fn test_display_timeout_with_duration() {
        let err = BigQueryError::Timeout {
            operation: "query".into(),
            duration_ms: Some(30000),
        };
        assert_eq!(err.to_string(), "Timeout during query (after 30000ms)");
    }

    #[test]
    fn test_display_timeout_without_duration() {
        let err = BigQueryError::Timeout {
            operation: "export".into(),
            duration_ms: None,
        };
        assert_eq!(err.to_string(), "Timeout during export");
    }

    #[test]
    fn test_display_schema_mismatch_with_field() {
        let err = BigQueryError::SchemaMismatch {
            message: "Expected INT64, got STRING".into(),
            field: Some("user_id".into()),
        };
        assert_eq!(err.to_string(), "Schema mismatch on field 'user_id': Expected INT64, got STRING");
    }

    #[test]
    fn test_display_schema_mismatch_without_field() {
        let err = BigQueryError::SchemaMismatch {
            message: "Column count mismatch".into(),
            field: None,
        };
        assert_eq!(err.to_string(), "Schema mismatch: Column count mismatch");
    }

    #[test]
    fn test_display_connection_failed() {
        let err = BigQueryError::ConnectionFailed {
            reason: "Network unreachable".into(),
        };
        assert_eq!(err.to_string(), "Connection failed: Network unreachable");
    }

    #[test]
    fn test_display_invalid_credentials_with_path() {
        let err = BigQueryError::InvalidCredentials {
            path: Some("/path/to/key.json".into()),
            reason: "File not found".into(),
        };
        let display = err.to_string();
        assert!(display.contains("Invalid credentials: File not found"));
        assert!(display.contains("path: /path/to/key.json"));
    }

    #[test]
    fn test_display_unknown_with_code() {
        let err = BigQueryError::Unknown {
            code: Some("INTERNAL".into()),
            message: "Something went wrong".into(),
            raw_error: "raw".into(),
        };
        assert_eq!(err.to_string(), "BigQuery error [INTERNAL]: Something went wrong");
    }

    #[test]
    fn test_display_unknown_without_code() {
        let err = BigQueryError::Unknown {
            code: None,
            message: "Unknown error".into(),
            raw_error: "raw".into(),
        };
        assert_eq!(err.to_string(), "BigQuery error: Unknown error");
    }

    #[test]
    fn test_suggestion_table_not_found() {
        let err = BigQueryError::TableNotFound {
            project: "proj".into(),
            dataset: "ds".into(),
            table: "tbl".into(),
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("bq show proj:ds.tbl"));
        assert!(suggestion.contains("Check for typos"));
    }

    #[test]
    fn test_suggestion_dataset_not_found() {
        let err = BigQueryError::DatasetNotFound {
            project: "proj".into(),
            dataset: "ds".into(),
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("bq show proj:ds"));
    }

    #[test]
    fn test_suggestion_access_denied() {
        let err = BigQueryError::AccessDenied {
            resource: "my_table".into(),
            required_permission: Some("bigquery.tables.getData".into()),
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("bigquery.tables.getData"));
        assert!(suggestion.contains("gcloud projects add-iam-policy-binding"));
    }

    #[test]
    fn test_suggestion_quota_exceeded() {
        let err = BigQueryError::QuotaExceeded {
            quota_type: "concurrent queries".into(),
            message: "limit reached".into(),
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("concurrent queries"));
        assert!(suggestion.contains("Wait and retry"));
    }

    #[test]
    fn test_suggestion_timeout() {
        let err = BigQueryError::Timeout {
            operation: "big_query".into(),
            duration_ms: None,
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("big_query"));
        assert!(suggestion.contains("Reduce query complexity"));
    }

    #[test]
    fn test_suggestion_schema_mismatch_with_field() {
        let err = BigQueryError::SchemaMismatch {
            message: "type error".into(),
            field: Some("amount".into()),
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("field 'amount'"));
        assert!(suggestion.contains("bq show --schema"));
    }

    #[test]
    fn test_suggestion_invalid_credentials_with_path() {
        let err = BigQueryError::InvalidCredentials {
            path: Some("/my/path.json".into()),
            reason: "invalid".into(),
        };
        let suggestion = err.suggestion();
        assert!(suggestion.contains("/my/path.json"));
        assert!(suggestion.contains("GOOGLE_APPLICATION_CREDENTIALS"));
    }

    #[test]
    fn test_bigquery_error_is_error_trait() {
        fn assert_error<E: std::error::Error>(_: &E) {}

        let err = BigQueryError::Unknown {
            code: None,
            message: "test".into(),
            raw_error: "raw".into(),
        };
        assert_error(&err);
    }

    #[test]
    fn test_bigquery_error_is_clone() {
        let err = BigQueryError::TableNotFound {
            project: "p".into(),
            dataset: "d".into(),
            table: "t".into(),
        };
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }

    #[test]
    fn test_query_error_location_debug() {
        let loc = QueryErrorLocation {
            line: Some(10),
            column: Some(5),
            offset: Some(100),
        };
        let debug = format!("{:?}", loc);
        assert!(debug.contains("10"));
        assert!(debug.contains("5"));
        assert!(debug.contains("100"));
    }
}
