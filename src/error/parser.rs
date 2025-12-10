use gcp_bigquery_client::error::{BQError, ResponseError};
use super::bq_error::{BigQueryError, QueryErrorLocation};
use regex::Regex;

pub fn parse_bq_error(error: BQError, context: ErrorContext) -> BigQueryError {
    match &error {
        BQError::ResponseError { error: resp } => parse_response_error(resp, context),

        BQError::RequestError(req_err) => {
            BigQueryError::ConnectionFailed {
                reason: req_err.to_string(),
            }
        }

        BQError::NoToken => {
            BigQueryError::AuthenticationFailed {
                reason: "No authentication token available".to_string(),
                help: "Ensure you are authenticated with GCP".to_string(),
            }
        }

        BQError::AuthError(auth_err) => {
            BigQueryError::AuthenticationFailed {
                reason: format!("{:?}", auth_err),
                help: "Check your authentication configuration".to_string(),
            }
        }

        BQError::YupAuthError(yup_err) => {
            BigQueryError::AuthenticationFailed {
                reason: yup_err.to_string(),
                help: "OAuth authentication failed".to_string(),
            }
        }

        BQError::InvalidServiceAccountKey(io_err) => {
            BigQueryError::InvalidCredentials {
                path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
                reason: io_err.to_string(),
            }
        }

        BQError::InvalidServiceAccountAuthenticator(io_err) => {
            BigQueryError::InvalidCredentials {
                path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
                reason: io_err.to_string(),
            }
        }

        BQError::InvalidApplicationDefaultCredentialsAuthenticator(io_err) => {
            BigQueryError::AuthenticationFailed {
                reason: io_err.to_string(),
                help: "Application default credentials are invalid or not configured".to_string(),
            }
        }

        BQError::NoDataAvailable => {
            BigQueryError::Unknown {
                code: None,
                message: "No data available in response".to_string(),
                raw_error: error.to_string(),
            }
        }

        BQError::SerializationError(serde_err) => {
            BigQueryError::Unknown {
                code: Some("SERIALIZATION".to_string()),
                message: serde_err.to_string(),
                raw_error: error.to_string(),
            }
        }

        BQError::ConnectionPoolError(msg) => {
            BigQueryError::ConnectionFailed {
                reason: msg.clone(),
            }
        }

        BQError::TonicTransportError(tonic_err) => {
            BigQueryError::ConnectionFailed {
                reason: tonic_err.to_string(),
            }
        }

        BQError::TonicStatusError(status) => {
            BigQueryError::Unknown {
                code: Some(format!("{:?}", status.code())),
                message: status.message().to_string(),
                raw_error: error.to_string(),
            }
        }

        _ => BigQueryError::Unknown {
            code: None,
            message: error.to_string(),
            raw_error: format!("{:?}", error),
        }
    }
}

fn parse_response_error(
    resp: &ResponseError,
    context: ErrorContext,
) -> BigQueryError {
    let status = resp.error.code;
    let message = &resp.error.message;
    let errors = &resp.error.errors;

    // Get the first error reason if available (errors is Vec<HashMap<String, String>>)
    let reason = errors.first().and_then(|e| e.get("reason").map(|s| s.as_str()));

    match (status, reason) {
        // 400 Bad Request
        (400, Some("invalidQuery")) => {
            let location = extract_query_location(message);
            BigQueryError::InvalidQuery {
                sql_preview: context.sql.unwrap_or_default(),
                message: message.clone(),
                location,
            }
        }

        (400, Some("invalid")) => {
            // Generic invalid - could be query or other
            if message.to_lowercase().contains("syntax") ||
               message.to_lowercase().contains("query") {
                BigQueryError::InvalidQuery {
                    sql_preview: context.sql.unwrap_or_default(),
                    message: message.clone(),
                    location: extract_query_location(message),
                }
            } else {
                BigQueryError::Unknown {
                    code: Some("invalid".to_string()),
                    message: message.clone(),
                    raw_error: format!("{:?}", resp),
                }
            }
        }

        (400, Some("resourcesExceeded")) => {
            BigQueryError::ResourcesExceeded {
                message: message.clone(),
                suggestion: "Try:\n  \
                    • Add filters to reduce data scanned\n  \
                    • Use LIMIT clause for testing\n  \
                    • Partition tables by date\n  \
                    • Break query into smaller parts".to_string(),
            }
        }

        (400, Some("timeout")) => {
            BigQueryError::Timeout {
                operation: context.operation.unwrap_or_else(|| "query".to_string()),
                duration_ms: None,
            }
        }

        (400, Some("backendError")) => {
            BigQueryError::Unknown {
                code: Some("backendError".to_string()),
                message: format!("BigQuery backend error: {}", message),
                raw_error: format!("{:?}", resp),
            }
        }

        // 403 Forbidden
        (403, Some("accessDenied")) => {
            let resource = context.resource.unwrap_or_else(|| "resource".to_string());
            let required_perm = extract_required_permission(message);
            BigQueryError::AccessDenied {
                resource,
                required_permission: required_perm,
            }
        }

        (403, Some("quotaExceeded")) | (403, Some("rateLimitExceeded")) => {
            let quota_type = extract_quota_type(message).unwrap_or_else(|| "API".to_string());
            BigQueryError::QuotaExceeded {
                quota_type,
                message: message.clone(),
            }
        }

        (403, Some("responseTooLarge")) => {
            BigQueryError::ResourcesExceeded {
                message: message.clone(),
                suggestion: "Response too large. Try:\n  \
                    • Add LIMIT clause\n  \
                    • Export to GCS instead\n  \
                    • Remove ORDER BY if not needed".to_string(),
            }
        }

        // 404 Not Found
        (404, _) => {
            parse_not_found_error(message, &context)
        }

        // 409 Conflict
        (409, Some("duplicate")) => {
            BigQueryError::Unknown {
                code: Some("duplicate".to_string()),
                message: format!("Resource already exists: {}", message),
                raw_error: format!("{:?}", resp),
            }
        }

        // 500+ Server errors
        (500..=599, _) => {
            BigQueryError::Unknown {
                code: Some(format!("HTTP_{}", status)),
                message: format!("BigQuery server error: {}", message),
                raw_error: format!("{:?}", resp),
            }
        }

        // Default
        _ => BigQueryError::Unknown {
            code: reason.map(|s| s.to_string()),
            message: message.clone(),
            raw_error: format!("{:?}", resp),
        }
    }
}

fn parse_not_found_error(message: &str, context: &ErrorContext) -> BigQueryError {
    let msg_lower = message.to_lowercase();

    // Try to extract table info from message
    if msg_lower.contains("table") || msg_lower.contains("not found") {
        // Try to parse "Not found: Table project:dataset.table"
        let table_re = Regex::new(r"(?i)table\s+([^:\s]+):([^.\s]+)\.([^\s]+)").ok();
        if let Some(re) = table_re {
            if let Some(caps) = re.captures(message) {
                return BigQueryError::TableNotFound {
                    project: caps.get(1).map(|m| m.as_str().to_string()).unwrap_or_default(),
                    dataset: caps.get(2).map(|m| m.as_str().to_string()).unwrap_or_default(),
                    table: caps.get(3).map(|m| m.as_str().to_string()).unwrap_or_default(),
                };
            }
        }

        // Fallback with context
        if let (Some(project), Some(dataset), Some(table)) =
            (&context.project, &context.dataset, &context.table) {
            return BigQueryError::TableNotFound {
                project: project.clone(),
                dataset: dataset.clone(),
                table: table.clone(),
            };
        }
    }

    if msg_lower.contains("dataset") {
        // Try to parse "Not found: Dataset project:dataset"
        let dataset_re = Regex::new(r"(?i)dataset\s+([^:\s]+):([^\s]+)").ok();
        if let Some(re) = dataset_re {
            if let Some(caps) = re.captures(message) {
                return BigQueryError::DatasetNotFound {
                    project: caps.get(1).map(|m| m.as_str().to_string()).unwrap_or_default(),
                    dataset: caps.get(2).map(|m| m.as_str().to_string()).unwrap_or_default(),
                };
            }
        }

        if let (Some(project), Some(dataset)) = (&context.project, &context.dataset) {
            return BigQueryError::DatasetNotFound {
                project: project.clone(),
                dataset: dataset.clone(),
            };
        }
    }

    // Generic not found
    BigQueryError::Unknown {
        code: Some("notFound".to_string()),
        message: message.to_string(),
        raw_error: message.to_string(),
    }
}

fn extract_query_location(message: &str) -> Option<QueryErrorLocation> {
    // Try to extract line/column from error message
    // Common format: "at [line:column]" or "line X, column Y"

    let line_col_re = Regex::new(r"\[(\d+):(\d+)\]").ok()?;
    if let Some(caps) = line_col_re.captures(message) {
        return Some(QueryErrorLocation {
            line: caps.get(1).and_then(|m| m.as_str().parse().ok()),
            column: caps.get(2).and_then(|m| m.as_str().parse().ok()),
            offset: None,
        });
    }

    let verbose_re = Regex::new(r"line\s+(\d+).*column\s+(\d+)").ok()?;
    if let Some(caps) = verbose_re.captures(message) {
        return Some(QueryErrorLocation {
            line: caps.get(1).and_then(|m| m.as_str().parse().ok()),
            column: caps.get(2).and_then(|m| m.as_str().parse().ok()),
            offset: None,
        });
    }

    None
}

fn extract_required_permission(message: &str) -> Option<String> {
    // Try to extract permission from message like "requires bigquery.tables.getData"
    let perm_re = Regex::new(r"(bigquery\.[a-zA-Z.]+)").ok()?;
    perm_re.captures(message)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn extract_quota_type(message: &str) -> Option<String> {
    let msg_lower = message.to_lowercase();

    if msg_lower.contains("concurrent") {
        Some("concurrent queries".to_string())
    } else if msg_lower.contains("daily") {
        Some("daily query limit".to_string())
    } else if msg_lower.contains("rate") {
        Some("rate limit".to_string())
    } else if msg_lower.contains("bytes") {
        Some("bytes scanned".to_string())
    } else {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct ErrorContext {
    pub sql: Option<String>,
    pub operation: Option<String>,
    pub resource: Option<String>,
    pub project: Option<String>,
    pub dataset: Option<String>,
    pub table: Option<String>,
}

impl ErrorContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        let full_sql = sql.into();
        // Keep first 500 chars as preview
        self.sql = Some(if full_sql.len() > 500 {
            format!("{}...", &full_sql[..500])
        } else {
            full_sql
        });
        self
    }

    pub fn with_operation(mut self, op: impl Into<String>) -> Self {
        self.operation = Some(op.into());
        self
    }

    pub fn with_table(mut self, project: impl Into<String>, dataset: impl Into<String>, table: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self.dataset = Some(dataset.into());
        self.table = Some(table.into());
        self.resource = Some(format!("{}.{}.{}",
            self.project.as_ref().unwrap(),
            self.dataset.as_ref().unwrap(),
            self.table.as_ref().unwrap()
        ));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_query_location_brackets() {
        let msg = "Syntax error: Unexpected identifier at [3:15]";
        let loc = extract_query_location(msg).unwrap();
        assert_eq!(loc.line, Some(3));
        assert_eq!(loc.column, Some(15));
    }

    #[test]
    fn test_extract_query_location_verbose() {
        let msg = "Error at line 10, column 25: unexpected token";
        let loc = extract_query_location(msg).unwrap();
        assert_eq!(loc.line, Some(10));
        assert_eq!(loc.column, Some(25));
    }

    #[test]
    fn test_extract_required_permission() {
        let msg = "Access denied: User does not have bigquery.tables.getData permission";
        let perm = extract_required_permission(msg).unwrap();
        assert_eq!(perm, "bigquery.tables.getData");
    }

    #[test]
    fn test_error_context_sql_truncation() {
        let long_sql = "SELECT ".to_string() + &"x, ".repeat(500);
        let ctx = ErrorContext::new().with_sql(long_sql);
        assert!(ctx.sql.as_ref().unwrap().len() <= 503); // 500 + "..."
        assert!(ctx.sql.as_ref().unwrap().ends_with("..."));
    }
}
