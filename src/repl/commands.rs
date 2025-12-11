use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::error::Result;

#[derive(Debug, Clone)]
pub enum ReplCommand {
    Run {
        query: Option<String>,
        partition: Option<String>,
        dry_run: bool,
        skip_invariants: bool,
        scratch: Option<String>,
        scratch_ttl: Option<u32>,
    },
    Backfill {
        query: String,
        from: String,
        to: String,
        dry_run: bool,
        skip_invariants: bool,
    },
    Check {
        query: String,
        partition: Option<String>,
        before: bool,
        after: bool,
    },
    List {
        detailed: bool,
    },
    Show {
        query: String,
        version: Option<u32>,
    },
    Validate,
    Sync {
        from: Option<String>,
        to: Option<String>,
        dry_run: bool,
        tracking_dataset: String,
        allow_source_mutation: bool,
    },
    Audit {
        query: Option<String>,
        modified_only: bool,
        diff: bool,
        output: String,
    },
    Init {
        dataset: String,
    },
    ScratchList {
        project: String,
    },
    ScratchPromote {
        query: String,
        partition: String,
        scratch_project: String,
    },
    Reload,
    Status,
    Help,
    Exit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ReplResult {
    pub fn success_with_output(output: String) -> Self {
        Self {
            success: true,
            output: Some(output),
            data: None,
            error: None,
        }
    }

    pub fn success_with_data(data: Value) -> Self {
        Self {
            success: true,
            output: None,
            data: Some(data),
            error: None,
        }
    }

    pub fn success_with_both(output: String, data: Value) -> Self {
        Self {
            success: true,
            output: Some(output),
            data: Some(data),
            error: None,
        }
    }

    pub fn failure(error: String) -> Self {
        Self {
            success: false,
            output: None,
            data: None,
            error: Some(error),
        }
    }

    pub fn empty_success() -> Self {
        Self {
            success: true,
            output: None,
            data: None,
            error: None,
        }
    }
}

impl ReplCommand {
    pub fn parse_interactive(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(crate::error::BqDriftError::Repl("Empty command".to_string()));
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        let cmd = parts[0].to_lowercase();

        match cmd.as_str() {
            "exit" | "quit" | "q" => Ok(ReplCommand::Exit),
            "help" | "?" => Ok(ReplCommand::Help),
            "reload" => Ok(ReplCommand::Reload),
            "status" => Ok(ReplCommand::Status),
            "validate" => Ok(ReplCommand::Validate),
            "list" => {
                let detailed = parts.iter().any(|&p| p == "--detailed" || p == "-d");
                Ok(ReplCommand::List { detailed })
            }
            "show" => {
                let query = find_arg(&parts, "--query", "-q")
                    .or_else(|| parts.get(1).map(|s| s.to_string()))
                    .ok_or_else(|| crate::error::BqDriftError::Repl("show requires --query".to_string()))?;
                let version = find_arg(&parts, "--version", "-v")
                    .and_then(|v| v.parse().ok());
                Ok(ReplCommand::Show { query, version })
            }
            "run" => {
                let query = find_arg(&parts, "--query", "-q");
                let partition = find_arg(&parts, "--partition", "-p");
                let dry_run = has_flag(&parts, "--dry-run");
                let skip_invariants = has_flag(&parts, "--skip-invariants");
                let scratch = find_arg(&parts, "--scratch", "-s");
                let scratch_ttl = find_arg(&parts, "--scratch-ttl", "")
                    .and_then(|v| v.parse().ok());
                Ok(ReplCommand::Run {
                    query,
                    partition,
                    dry_run,
                    skip_invariants,
                    scratch,
                    scratch_ttl,
                })
            }
            "backfill" => {
                let query = find_arg(&parts, "--query", "-q")
                    .or_else(|| parts.get(1).map(|s| s.to_string()))
                    .ok_or_else(|| crate::error::BqDriftError::Repl("backfill requires query name".to_string()))?;
                let from = find_arg(&parts, "--from", "-f")
                    .ok_or_else(|| crate::error::BqDriftError::Repl("backfill requires --from".to_string()))?;
                let to = find_arg(&parts, "--to", "-t")
                    .ok_or_else(|| crate::error::BqDriftError::Repl("backfill requires --to".to_string()))?;
                let dry_run = has_flag(&parts, "--dry-run");
                let skip_invariants = has_flag(&parts, "--skip-invariants");
                Ok(ReplCommand::Backfill {
                    query,
                    from,
                    to,
                    dry_run,
                    skip_invariants,
                })
            }
            "check" => {
                let query = find_arg(&parts, "--query", "-q")
                    .or_else(|| parts.get(1).map(|s| s.to_string()))
                    .ok_or_else(|| crate::error::BqDriftError::Repl("check requires query name".to_string()))?;
                let partition = find_arg(&parts, "--partition", "-p");
                let before = has_flag(&parts, "--before");
                let after = has_flag(&parts, "--after");
                Ok(ReplCommand::Check {
                    query,
                    partition,
                    before,
                    after,
                })
            }
            "sync" => {
                let from = find_arg(&parts, "--from", "-f");
                let to = find_arg(&parts, "--to", "-t");
                let dry_run = has_flag(&parts, "--dry-run");
                let tracking_dataset = find_arg(&parts, "--tracking-dataset", "")
                    .unwrap_or_else(|| "bqdrift".to_string());
                let allow_source_mutation = has_flag(&parts, "--allow-source-mutation");
                Ok(ReplCommand::Sync {
                    from,
                    to,
                    dry_run,
                    tracking_dataset,
                    allow_source_mutation,
                })
            }
            "audit" => {
                let query = find_arg(&parts, "--query", "-q");
                let modified_only = has_flag(&parts, "--modified-only");
                let diff = has_flag(&parts, "--diff");
                let output = find_arg(&parts, "--output", "-o")
                    .unwrap_or_else(|| "table".to_string());
                Ok(ReplCommand::Audit {
                    query,
                    modified_only,
                    diff,
                    output,
                })
            }
            "init" => {
                let dataset = find_arg(&parts, "--dataset", "-d")
                    .unwrap_or_else(|| "bqdrift".to_string());
                Ok(ReplCommand::Init { dataset })
            }
            "scratch" => {
                let action = parts.get(1).map(|s| s.to_lowercase());
                match action.as_deref() {
                    Some("list") => {
                        let project = find_arg(&parts, "--project", "-p")
                            .ok_or_else(|| crate::error::BqDriftError::Repl("scratch list requires --project".to_string()))?;
                        Ok(ReplCommand::ScratchList { project })
                    }
                    Some("promote") => {
                        let query = find_arg(&parts, "--query", "-q")
                            .ok_or_else(|| crate::error::BqDriftError::Repl("scratch promote requires --query".to_string()))?;
                        let partition = find_arg(&parts, "--partition", "-p")
                            .ok_or_else(|| crate::error::BqDriftError::Repl("scratch promote requires --partition".to_string()))?;
                        let scratch_project = find_arg(&parts, "--scratch-project", "")
                            .ok_or_else(|| crate::error::BqDriftError::Repl("scratch promote requires --scratch-project".to_string()))?;
                        Ok(ReplCommand::ScratchPromote {
                            query,
                            partition,
                            scratch_project,
                        })
                    }
                    _ => Err(crate::error::BqDriftError::Repl("scratch requires action: list or promote".to_string())),
                }
            }
            _ => Err(crate::error::BqDriftError::Repl(format!("Unknown command: {}", cmd))),
        }
    }

    pub fn from_json_rpc(method: &str, params: Option<&Value>) -> Result<Self> {
        match method {
            "exit" | "quit" => Ok(ReplCommand::Exit),
            "help" => Ok(ReplCommand::Help),
            "reload" => Ok(ReplCommand::Reload),
            "status" => Ok(ReplCommand::Status),
            "validate" => Ok(ReplCommand::Validate),
            "list" => {
                let detailed = params
                    .and_then(|p| p.get("detailed"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(ReplCommand::List { detailed })
            }
            "show" => {
                let query = params
                    .and_then(|p| p.get("query"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("show requires 'query' param".to_string()))?;
                let version = params
                    .and_then(|p| p.get("version"))
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32);
                Ok(ReplCommand::Show { query, version })
            }
            "run" => {
                let query = params
                    .and_then(|p| p.get("query"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let partition = params
                    .and_then(|p| p.get("partition"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let dry_run = params
                    .and_then(|p| p.get("dry_run"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let skip_invariants = params
                    .and_then(|p| p.get("skip_invariants"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let scratch = params
                    .and_then(|p| p.get("scratch"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let scratch_ttl = params
                    .and_then(|p| p.get("scratch_ttl"))
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32);
                Ok(ReplCommand::Run {
                    query,
                    partition,
                    dry_run,
                    skip_invariants,
                    scratch,
                    scratch_ttl,
                })
            }
            "backfill" => {
                let query = params
                    .and_then(|p| p.get("query"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("backfill requires 'query' param".to_string()))?;
                let from = params
                    .and_then(|p| p.get("from"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("backfill requires 'from' param".to_string()))?;
                let to = params
                    .and_then(|p| p.get("to"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("backfill requires 'to' param".to_string()))?;
                let dry_run = params
                    .and_then(|p| p.get("dry_run"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let skip_invariants = params
                    .and_then(|p| p.get("skip_invariants"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(ReplCommand::Backfill {
                    query,
                    from,
                    to,
                    dry_run,
                    skip_invariants,
                })
            }
            "check" => {
                let query = params
                    .and_then(|p| p.get("query"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("check requires 'query' param".to_string()))?;
                let partition = params
                    .and_then(|p| p.get("partition"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let before = params
                    .and_then(|p| p.get("before"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let after = params
                    .and_then(|p| p.get("after"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(ReplCommand::Check {
                    query,
                    partition,
                    before,
                    after,
                })
            }
            "sync" => {
                let from = params
                    .and_then(|p| p.get("from"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let to = params
                    .and_then(|p| p.get("to"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let dry_run = params
                    .and_then(|p| p.get("dry_run"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let tracking_dataset = params
                    .and_then(|p| p.get("tracking_dataset"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "bqdrift".to_string());
                let allow_source_mutation = params
                    .and_then(|p| p.get("allow_source_mutation"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(ReplCommand::Sync {
                    from,
                    to,
                    dry_run,
                    tracking_dataset,
                    allow_source_mutation,
                })
            }
            "audit" => {
                let query = params
                    .and_then(|p| p.get("query"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let modified_only = params
                    .and_then(|p| p.get("modified_only"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let diff = params
                    .and_then(|p| p.get("diff"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let output = params
                    .and_then(|p| p.get("output"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "table".to_string());
                Ok(ReplCommand::Audit {
                    query,
                    modified_only,
                    diff,
                    output,
                })
            }
            "init" => {
                let dataset = params
                    .and_then(|p| p.get("dataset"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "bqdrift".to_string());
                Ok(ReplCommand::Init { dataset })
            }
            "scratch_list" => {
                let project = params
                    .and_then(|p| p.get("project"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("scratch_list requires 'project' param".to_string()))?;
                Ok(ReplCommand::ScratchList { project })
            }
            "scratch_promote" => {
                let query = params
                    .and_then(|p| p.get("query"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("scratch_promote requires 'query' param".to_string()))?;
                let partition = params
                    .and_then(|p| p.get("partition"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("scratch_promote requires 'partition' param".to_string()))?;
                let scratch_project = params
                    .and_then(|p| p.get("scratch_project"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| crate::error::BqDriftError::Repl("scratch_promote requires 'scratch_project' param".to_string()))?;
                Ok(ReplCommand::ScratchPromote {
                    query,
                    partition,
                    scratch_project,
                })
            }
            _ => Err(crate::error::BqDriftError::Repl(format!("Unknown method: {}", method))),
        }
    }
}

fn find_arg(parts: &[&str], long: &str, short: &str) -> Option<String> {
    for (i, &part) in parts.iter().enumerate() {
        if part == long || (!short.is_empty() && part == short) {
            return parts.get(i + 1).map(|s| s.to_string());
        }
        if part.starts_with(&format!("{}=", long)) {
            return Some(part.split('=').nth(1)?.to_string());
        }
        if !short.is_empty() && part.starts_with(&format!("{}=", short)) {
            return Some(part.split('=').nth(1)?.to_string());
        }
    }
    None
}

fn has_flag(parts: &[&str], flag: &str) -> bool {
    parts.iter().any(|&p| p == flag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exit() {
        let cmd = ReplCommand::parse_interactive("exit").unwrap();
        assert!(matches!(cmd, ReplCommand::Exit));

        let cmd = ReplCommand::parse_interactive("quit").unwrap();
        assert!(matches!(cmd, ReplCommand::Exit));
    }

    #[test]
    fn test_parse_list() {
        let cmd = ReplCommand::parse_interactive("list").unwrap();
        assert!(matches!(cmd, ReplCommand::List { detailed: false }));

        let cmd = ReplCommand::parse_interactive("list --detailed").unwrap();
        assert!(matches!(cmd, ReplCommand::List { detailed: true }));
    }

    #[test]
    fn test_parse_run() {
        let cmd = ReplCommand::parse_interactive("run --query my_query --partition 2024-01-15").unwrap();
        if let ReplCommand::Run { query, partition, dry_run, .. } = cmd {
            assert_eq!(query, Some("my_query".to_string()));
            assert_eq!(partition, Some("2024-01-15".to_string()));
            assert!(!dry_run);
        } else {
            panic!("Expected Run command");
        }
    }

    #[test]
    fn test_parse_run_dry_run() {
        let cmd = ReplCommand::parse_interactive("run --query test --dry-run").unwrap();
        if let ReplCommand::Run { dry_run, .. } = cmd {
            assert!(dry_run);
        } else {
            panic!("Expected Run command");
        }
    }

    #[test]
    fn test_from_json_rpc_list() {
        let params = serde_json::json!({"detailed": true});
        let cmd = ReplCommand::from_json_rpc("list", Some(&params)).unwrap();
        assert!(matches!(cmd, ReplCommand::List { detailed: true }));
    }

    #[test]
    fn test_from_json_rpc_run() {
        let params = serde_json::json!({
            "query": "my_query",
            "partition": "2024-01-15",
            "dry_run": true
        });
        let cmd = ReplCommand::from_json_rpc("run", Some(&params)).unwrap();
        if let ReplCommand::Run { query, partition, dry_run, .. } = cmd {
            assert_eq!(query, Some("my_query".to_string()));
            assert_eq!(partition, Some("2024-01-15".to_string()));
            assert!(dry_run);
        } else {
            panic!("Expected Run command");
        }
    }
}
