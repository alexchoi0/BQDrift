use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;

pub const SESSION_EXPIRED: i32 = -32001;
pub const SESSION_LIMIT: i32 = -32002;
pub const INVALID_SESSION_CONFIG: i32 = -32003;

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub session: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcResponse {
    pub fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Option<Value>, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
            id,
        }
    }

    pub fn error_with_data(id: Option<Value>, code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: Some(data),
            }),
            id,
        }
    }

    pub fn parse_error() -> Self {
        Self::error(None, PARSE_ERROR, "Parse error")
    }

    pub fn invalid_request(id: Option<Value>) -> Self {
        Self::error(id, INVALID_REQUEST, "Invalid request")
    }

    pub fn method_not_found(id: Option<Value>, method: &str) -> Self {
        Self::error(id, METHOD_NOT_FOUND, format!("Method not found: {}", method))
    }

    pub fn invalid_params(id: Option<Value>, message: impl Into<String>) -> Self {
        Self::error(id, INVALID_PARAMS, message)
    }

    pub fn internal_error(id: Option<Value>, message: impl Into<String>) -> Self {
        Self::error(id, INTERNAL_ERROR, message)
    }
}

impl JsonRpcRequest {
    pub fn is_valid(&self) -> bool {
        self.jsonrpc == "2.0"
    }

    pub fn session_id(&self) -> &str {
        self.session.as_deref().unwrap_or("default")
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub id: String,
    pub created_at: String,
    pub last_activity: String,
    pub request_count: u64,
    pub idle_timeout_secs: u64,
    pub expires_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queries_path: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerConfigInfo {
    pub max_sessions: usize,
    pub current_sessions: usize,
    pub default_idle_timeout_secs: u64,
    pub max_idle_timeout_secs: u64,
    pub default_project: Option<String>,
    pub default_queries_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request() {
        let json = r#"{"jsonrpc":"2.0","method":"list","params":{"detailed":true},"id":1}"#;
        let request: JsonRpcRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.method, "list");
        assert!(request.params.is_some());
        assert_eq!(request.id, Some(Value::Number(1.into())));
    }

    #[test]
    fn test_parse_request_no_params() {
        let json = r#"{"jsonrpc":"2.0","method":"validate","id":"abc"}"#;
        let request: JsonRpcRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.method, "validate");
        assert!(request.params.is_none());
        assert_eq!(request.id, Some(Value::String("abc".to_string())));
    }

    #[test]
    fn test_success_response() {
        let response = JsonRpcResponse::success(
            Some(Value::Number(1.into())),
            serde_json::json!({"queries": 5}),
        );

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"result\""));
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_error_response() {
        let response = JsonRpcResponse::method_not_found(
            Some(Value::Number(1.into())),
            "unknown_method",
        );

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"error\""));
        assert!(json.contains("-32601"));
        assert!(!json.contains("\"result\""));
    }
}
