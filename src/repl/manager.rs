use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};
use tokio::sync::{mpsc, oneshot};
use super::commands::ReplCommand;
use super::protocol::{JsonRpcRequest, JsonRpcResponse, SessionInfo, ServerConfigInfo, SESSION_EXPIRED, SESSION_LIMIT};
use super::session::ReplSession;

pub struct ServerConfig {
    pub default_project: Option<String>,
    pub default_queries_path: PathBuf,
    pub max_sessions: usize,
    pub default_idle_timeout_secs: u64,
    pub max_idle_timeout_secs: u64,
    pub cleanup_interval_secs: u64,
}

impl ServerConfig {
    pub fn new(project: Option<String>, queries_path: PathBuf) -> Self {
        Self {
            default_project: project,
            default_queries_path: queries_path,
            max_sessions: 100,
            default_idle_timeout_secs: 300,
            max_idle_timeout_secs: 3600,
            cleanup_interval_secs: 60,
        }
    }

    pub fn with_max_sessions(mut self, max: usize) -> Self {
        self.max_sessions = max;
        self
    }

    pub fn with_idle_timeout(mut self, secs: u64) -> Self {
        self.default_idle_timeout_secs = secs;
        self
    }

    pub fn with_max_idle_timeout(mut self, secs: u64) -> Self {
        self.max_idle_timeout_secs = secs;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionCreateParams {
    pub session_id: Option<String>,
    pub project: Option<String>,
    pub queries_path: Option<PathBuf>,
    pub idle_timeout_secs: Option<u64>,
    pub metadata: HashMap<String, String>,
}

impl SessionCreateParams {
    pub fn from_json(params: Option<&serde_json::Value>) -> Self {
        let mut result = Self::default();

        if let Some(p) = params {
            if let Some(s) = p.get("session").and_then(|v| v.as_str()) {
                result.session_id = Some(s.to_string());
            }
            if let Some(s) = p.get("project").and_then(|v| v.as_str()) {
                result.project = Some(s.to_string());
            }
            if let Some(s) = p.get("queries_path").and_then(|v| v.as_str()) {
                result.queries_path = Some(PathBuf::from(s));
            }
            if let Some(n) = p.get("idle_timeout").and_then(|v| v.as_u64()) {
                result.idle_timeout_secs = Some(n);
            }
            if let Some(obj) = p.get("metadata").and_then(|v| v.as_object()) {
                for (k, v) in obj {
                    if let Some(s) = v.as_str() {
                        result.metadata.insert(k.clone(), s.to_string());
                    }
                }
            }
        }

        result
    }
}

struct SessionRequest {
    request: JsonRpcRequest,
    response_tx: oneshot::Sender<JsonRpcResponse>,
}

pub struct SessionHandle {
    id: String,
    request_tx: mpsc::Sender<SessionRequest>,
    created_at: DateTime<Utc>,
    last_activity: Arc<AtomicI64>,
    request_count: Arc<AtomicU64>,
    idle_timeout_secs: u64,
    project: Option<String>,
    queries_path: Option<PathBuf>,
    metadata: HashMap<String, String>,
}

impl SessionHandle {
    pub fn touch(&self) {
        self.last_activity.store(Utc::now().timestamp(), Ordering::Relaxed);
    }

    pub fn last_activity_time(&self) -> DateTime<Utc> {
        let ts = self.last_activity.load(Ordering::Relaxed);
        DateTime::from_timestamp(ts, 0).unwrap_or(self.created_at)
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.last_activity_time() + Duration::seconds(self.idle_timeout_secs as i64)
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at()
    }

    pub fn info(&self) -> SessionInfo {
        SessionInfo {
            id: self.id.clone(),
            created_at: self.created_at.to_rfc3339(),
            last_activity: self.last_activity_time().to_rfc3339(),
            request_count: self.request_count.load(Ordering::Relaxed),
            idle_timeout_secs: self.idle_timeout_secs,
            expires_at: self.expires_at().to_rfc3339(),
            project: self.project.clone(),
            queries_path: self.queries_path.as_ref().map(|p| p.to_string_lossy().to_string()),
            metadata: self.metadata.clone(),
        }
    }
}

struct SessionActor {
    #[allow(dead_code)]
    id: String,
    session: ReplSession,
    request_rx: mpsc::Receiver<SessionRequest>,
    request_count: Arc<AtomicU64>,
    last_activity: Arc<AtomicI64>,
}

impl SessionActor {
    fn new(
        id: String,
        session: ReplSession,
        request_rx: mpsc::Receiver<SessionRequest>,
        request_count: Arc<AtomicU64>,
        last_activity: Arc<AtomicI64>,
    ) -> Self {
        Self {
            id,
            session,
            request_rx,
            request_count,
            last_activity,
        }
    }

    async fn run(mut self) {
        while let Some(req) = self.request_rx.recv().await {
            self.last_activity.store(Utc::now().timestamp(), Ordering::Relaxed);
            self.request_count.fetch_add(1, Ordering::Relaxed);
            let response = self.handle_request(req.request).await;
            let _ = req.response_tx.send(response);
        }
    }

    async fn handle_request(&mut self, request: JsonRpcRequest) -> JsonRpcResponse {
        if !request.is_valid() {
            return JsonRpcResponse::invalid_request(request.id);
        }

        let cmd = match ReplCommand::from_json_rpc(&request.method, request.params.as_ref()) {
            Ok(c) => c,
            Err(e) => {
                if e.to_string().contains("Unknown method") {
                    return JsonRpcResponse::method_not_found(request.id, &request.method);
                }
                return JsonRpcResponse::invalid_params(request.id, e.to_string());
            }
        };

        let result = self.session.execute(cmd).await;

        if result.success {
            let response_data = if let Some(data) = result.data {
                data
            } else if let Some(output) = result.output {
                serde_json::json!({"output": output})
            } else {
                serde_json::json!({"success": true})
            };
            JsonRpcResponse::success(request.id, response_data)
        } else {
            let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());
            JsonRpcResponse::internal_error(request.id, error_msg)
        }
    }
}

pub struct SessionManager {
    sessions: HashMap<String, SessionHandle>,
    config: ServerConfig,
}

impl SessionManager {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            sessions: HashMap::new(),
            config,
        }
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    pub fn server_info(&self) -> ServerConfigInfo {
        ServerConfigInfo {
            max_sessions: self.config.max_sessions,
            current_sessions: self.sessions.len(),
            default_idle_timeout_secs: self.config.default_idle_timeout_secs,
            max_idle_timeout_secs: self.config.max_idle_timeout_secs,
            default_project: self.config.default_project.clone(),
            default_queries_path: self.config.default_queries_path.to_string_lossy().to_string(),
        }
    }

    pub fn can_create_session(&self) -> bool {
        self.sessions.len() < self.config.max_sessions
    }

    pub fn get_or_create(&mut self, session_id: &str) -> Result<&SessionHandle, JsonRpcResponse> {
        if !self.sessions.contains_key(session_id) {
            if !self.can_create_session() {
                return Err(JsonRpcResponse::error(
                    None,
                    SESSION_LIMIT,
                    format!("Session limit reached (max: {})", self.config.max_sessions),
                ));
            }
            let params = SessionCreateParams {
                session_id: Some(session_id.to_string()),
                ..Default::default()
            };
            let handle = self.create_session(params);
            self.sessions.insert(session_id.to_string(), handle);
        }
        Ok(self.sessions.get(session_id).unwrap())
    }

    pub fn create_session_with_params(&mut self, params: SessionCreateParams) -> Result<SessionInfo, JsonRpcResponse> {
        if !self.can_create_session() {
            return Err(JsonRpcResponse::error(
                None,
                SESSION_LIMIT,
                format!("Session limit reached (max: {})", self.config.max_sessions),
            ));
        }

        let session_id = params.session_id.clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if self.sessions.contains_key(&session_id) {
            return Ok(self.sessions.get(&session_id).unwrap().info());
        }

        let handle = self.create_session(params);
        let info = handle.info();
        self.sessions.insert(session_id, handle);
        Ok(info)
    }

    fn create_session(&self, params: SessionCreateParams) -> SessionHandle {
        let id = params.session_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let project = params.project.or_else(|| self.config.default_project.clone());
        let queries_path = params.queries_path.clone()
            .unwrap_or_else(|| self.config.default_queries_path.clone());

        let idle_timeout = params.idle_timeout_secs
            .map(|t| t.min(self.config.max_idle_timeout_secs))
            .unwrap_or(self.config.default_idle_timeout_secs);

        let session = ReplSession::new(project.clone(), queries_path.clone());

        let (request_tx, request_rx) = mpsc::channel(32);
        let request_count = Arc::new(AtomicU64::new(0));
        let last_activity = Arc::new(AtomicI64::new(Utc::now().timestamp()));
        let created_at = Utc::now();

        let actor = SessionActor::new(
            id.clone(),
            session,
            request_rx,
            Arc::clone(&request_count),
            Arc::clone(&last_activity),
        );

        tokio::spawn(actor.run());

        SessionHandle {
            id,
            request_tx,
            created_at,
            last_activity,
            request_count,
            idle_timeout_secs: idle_timeout,
            project,
            queries_path: params.queries_path,
            metadata: params.metadata,
        }
    }

    pub async fn send_request(
        &mut self,
        session_id: &str,
        request: JsonRpcRequest,
    ) -> JsonRpcResponse {
        if let Some(handle) = self.sessions.get(session_id) {
            if handle.is_expired() {
                self.sessions.remove(session_id);
                return JsonRpcResponse::error(
                    request.id,
                    SESSION_EXPIRED,
                    format!("Session '{}' has expired", session_id),
                );
            }
        }

        let handle = match self.get_or_create(session_id) {
            Ok(h) => h,
            Err(e) => return e,
        };

        handle.touch();

        let (response_tx, response_rx) = oneshot::channel();

        let session_request = SessionRequest {
            request: request.clone(),
            response_tx,
        };

        if handle.request_tx.send(session_request).await.is_err() {
            return JsonRpcResponse::internal_error(
                request.id,
                format!("Session '{}' is no longer available", session_id),
            );
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => JsonRpcResponse::internal_error(
                request.id,
                "Session actor terminated unexpectedly".to_string(),
            ),
        }
    }

    pub fn keepalive(&mut self, session_id: &str) -> bool {
        if let Some(handle) = self.sessions.get(session_id) {
            if !handle.is_expired() {
                handle.touch();
                return true;
            }
        }
        false
    }

    pub fn destroy_session(&mut self, session_id: &str) -> bool {
        self.sessions.remove(session_id).is_some()
    }

    pub fn cleanup_expired(&mut self) -> usize {
        let expired: Vec<String> = self.sessions
            .iter()
            .filter(|(_, h)| h.is_expired())
            .map(|(id, _)| id.clone())
            .collect();

        let count = expired.len();
        for id in expired {
            self.sessions.remove(&id);
        }
        count
    }

    pub fn list_sessions(&self) -> Vec<SessionInfo> {
        self.sessions.values().map(|h| h.info()).collect()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}
