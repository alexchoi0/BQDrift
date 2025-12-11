use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, Duration};
use crate::error::Result;
use super::manager::{ServerConfig, SessionManager, SessionCreateParams};
use super::protocol::{JsonRpcRequest, JsonRpcResponse};

pub struct AsyncJsonRpcServer {
    manager: Arc<Mutex<SessionManager>>,
    response_tx: mpsc::UnboundedSender<JsonRpcResponse>,
}

impl AsyncJsonRpcServer {
    pub async fn run(config: ServerConfig) -> Result<()> {
        let cleanup_interval = config.cleanup_interval_secs;
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        let manager = Arc::new(Mutex::new(SessionManager::new(config)));

        let server = Self {
            manager: Arc::clone(&manager),
            response_tx,
        };

        let stdout = tokio::io::stdout();
        tokio::spawn(async move {
            let mut stdout = BufWriter::new(stdout);
            while let Some(response) = response_rx.recv().await {
                if let Ok(json) = serde_json::to_string(&response) {
                    let _ = stdout.write_all(json.as_bytes()).await;
                    let _ = stdout.write_all(b"\n").await;
                    let _ = stdout.flush().await;
                }
            }
        });

        let cleanup_manager = Arc::clone(&manager);
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(cleanup_interval));
            loop {
                ticker.tick().await;
                let mut mgr = cleanup_manager.lock().await;
                mgr.cleanup_expired();
            }
        });

        let stdin = tokio::io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }

            let should_exit = server.dispatch_request(&line).await;
            if should_exit {
                break;
            }
        }

        Ok(())
    }

    async fn dispatch_request(&self, line: &str) -> bool {
        let request: JsonRpcRequest = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(_) => {
                let _ = self.response_tx.send(JsonRpcResponse::parse_error());
                return false;
            }
        };

        let session_id = request.session_id().to_string();
        let is_exit = matches!(request.method.as_str(), "exit" | "quit");

        match request.method.as_str() {
            "ping" => {
                let _ = self.response_tx.send(JsonRpcResponse::success(
                    request.id,
                    serde_json::json!({"pong": true}),
                ));
                return false;
            }

            "sessions" => {
                let mgr = self.manager.lock().await;
                let sessions = mgr.list_sessions();
                let _ = self.response_tx.send(JsonRpcResponse::success(
                    request.id,
                    serde_json::to_value(sessions).unwrap_or_default(),
                ));
                return false;
            }

            "server_config" => {
                let mgr = self.manager.lock().await;
                let info = mgr.server_info();
                let _ = self.response_tx.send(JsonRpcResponse::success(
                    request.id,
                    serde_json::to_value(info).unwrap_or_default(),
                ));
                return false;
            }

            "session_create" => {
                let params = SessionCreateParams::from_json(request.params.as_ref());
                let mut mgr = self.manager.lock().await;
                match mgr.create_session_with_params(params) {
                    Ok(info) => {
                        let _ = self.response_tx.send(JsonRpcResponse::success(
                            request.id,
                            serde_json::to_value(info).unwrap_or_default(),
                        ));
                    }
                    Err(mut err) => {
                        err.id = request.id;
                        let _ = self.response_tx.send(err);
                    }
                }
                return false;
            }

            "session_destroy" => {
                let session_id = request.params
                    .as_ref()
                    .and_then(|p| p.get("session"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");

                let mut mgr = self.manager.lock().await;
                let destroyed = mgr.destroy_session(session_id);
                let _ = self.response_tx.send(JsonRpcResponse::success(
                    request.id,
                    serde_json::json!({"destroyed": destroyed, "session": session_id}),
                ));
                return false;
            }

            "session_keepalive" => {
                let session_id = request.params
                    .as_ref()
                    .and_then(|p| p.get("session"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("default");

                let mut mgr = self.manager.lock().await;
                let success = mgr.keepalive(session_id);
                let _ = self.response_tx.send(JsonRpcResponse::success(
                    request.id,
                    serde_json::json!({"success": success, "session": session_id}),
                ));
                return false;
            }

            _ => {}
        }

        let mut mgr = self.manager.lock().await;
        let response = mgr.send_request(&session_id, request).await;
        let _ = self.response_tx.send(response);

        is_exit
    }
}

#[cfg(test)]
mod tests {
    use super::super::protocol::{PARSE_ERROR, INVALID_REQUEST};
    use super::*;

    #[test]
    fn test_parse_error() {
        let response = JsonRpcResponse::parse_error();
        assert!(response.error.is_some());
        assert_eq!(response.error.as_ref().unwrap().code, PARSE_ERROR);
    }

    #[test]
    fn test_invalid_request() {
        let response = JsonRpcResponse::invalid_request(Some(serde_json::json!(1)));
        assert!(response.error.is_some());
        assert_eq!(response.error.as_ref().unwrap().code, INVALID_REQUEST);
    }
}
