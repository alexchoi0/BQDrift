use std::borrow::Cow;
use std::path::PathBuf;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::history::DefaultHistory;
use rustyline::{Config, Editor, Helper};
use crate::error::Result;
use super::commands::ReplCommand;
use super::session::ReplSession;

const COMMANDS: &[&str] = &[
    "list", "show", "validate", "run", "backfill", "check",
    "sync", "audit", "init", "scratch", "reload", "status", "help", "exit", "quit",
];

const FLAGS: &[&str] = &[
    "--query", "--partition", "--version", "--detailed", "--dry-run",
    "--skip-invariants", "--scratch", "--scratch-ttl", "--from", "--to",
    "--before", "--after", "--tracking-dataset", "--allow-source-mutation",
    "--modified-only", "--diff", "--output", "--dataset", "--project",
    "--scratch-project",
];

struct ReplHelper {
    queries: Vec<String>,
}

impl ReplHelper {
    fn new(queries: Vec<String>) -> Self {
        Self { queries }
    }

    fn update_queries(&mut self, queries: Vec<String>) {
        self.queries = queries;
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line_to_pos = &line[..pos];
        let words: Vec<&str> = line_to_pos.split_whitespace().collect();

        if words.is_empty() || (words.len() == 1 && !line_to_pos.ends_with(' ')) {
            let prefix = words.first().copied().unwrap_or("");
            let start = line_to_pos.rfind(char::is_whitespace).map(|i| i + 1).unwrap_or(0);

            let completions: Vec<Pair> = COMMANDS
                .iter()
                .filter(|cmd| cmd.starts_with(prefix))
                .map(|cmd| Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                })
                .collect();

            return Ok((start, completions));
        }

        let last_word = words.last().copied().unwrap_or("");

        if last_word.starts_with('-') {
            let start = line_to_pos.rfind(char::is_whitespace).map(|i| i + 1).unwrap_or(0);

            let completions: Vec<Pair> = FLAGS
                .iter()
                .filter(|flag| flag.starts_with(last_word))
                .map(|flag| Pair {
                    display: flag.to_string(),
                    replacement: flag.to_string(),
                })
                .collect();

            return Ok((start, completions));
        }

        let prev_word = if words.len() >= 2 {
            words[words.len() - 2]
        } else {
            ""
        };

        if prev_word == "--query" || prev_word == "-q" {
            let start = if line_to_pos.ends_with(' ') {
                pos
            } else {
                line_to_pos.rfind(char::is_whitespace).map(|i| i + 1).unwrap_or(0)
            };

            let prefix = if line_to_pos.ends_with(' ') { "" } else { last_word };

            let completions: Vec<Pair> = self.queries
                .iter()
                .filter(|q| q.starts_with(prefix))
                .map(|q| Pair {
                    display: q.clone(),
                    replacement: q.clone(),
                })
                .collect();

            return Ok((start, completions));
        }

        if words.len() == 1 && line_to_pos.ends_with(' ') {
            let cmd = words[0];
            if cmd == "show" || cmd == "check" || cmd == "backfill" {
                let completions: Vec<Pair> = self.queries
                    .iter()
                    .map(|q| Pair {
                        display: q.clone(),
                        replacement: q.clone(),
                    })
                    .collect();

                return Ok((pos, completions));
            }
        }

        Ok((pos, Vec::new()))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }

        let words: Vec<&str> = line.split_whitespace().collect();
        if words.is_empty() {
            return None;
        }

        let last_word = words.last().copied().unwrap_or("");

        if words.len() == 1 && !line.ends_with(' ') {
            for cmd in COMMANDS {
                if cmd.starts_with(last_word) && *cmd != last_word {
                    return Some(cmd[last_word.len()..].to_string());
                }
            }
        }

        None
    }
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        Cow::Borrowed(line)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        false
    }
}

impl Validator for ReplHelper {}

impl Helper for ReplHelper {}

pub struct InteractiveRepl {
    session: ReplSession,
    editor: Editor<ReplHelper, DefaultHistory>,
    history_path: PathBuf,
}

impl InteractiveRepl {
    pub fn new(session: ReplSession) -> Result<Self> {
        let config = Config::builder()
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .build();

        let mut editor = Editor::with_config(config)
            .map_err(|e| crate::error::BqDriftError::Repl(e.to_string()))?;

        let helper = ReplHelper::new(session.query_names());
        editor.set_helper(Some(helper));

        let history_path = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".bqdrift_history");

        let _ = editor.load_history(&history_path);

        Ok(Self {
            session,
            editor,
            history_path,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("bqdrift REPL - Type 'help' for commands, 'exit' to quit");

        if let Err(e) = self.session.reload_queries() {
            eprintln!("Warning: Failed to load queries: {}", e);
        } else if let Some(helper) = self.editor.helper_mut() {
            helper.update_queries(self.session.query_names());
        }

        loop {
            let prompt = format!(
                "{}> ",
                self.session.project().unwrap_or("bqdrift")
            );

            match self.editor.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    let _ = self.editor.add_history_entry(line);

                    match ReplCommand::parse_interactive(line) {
                        Ok(cmd) => {
                            let is_exit = matches!(cmd, ReplCommand::Exit);
                            let is_reload = matches!(cmd, ReplCommand::Reload);

                            let result = self.session.execute(cmd).await;

                            if let Some(output) = &result.output {
                                println!("{}", output);
                            }
                            if !result.success {
                                if let Some(error) = &result.error {
                                    eprintln!("Error: {}", error);
                                }
                            }

                            if is_reload {
                                if let Some(helper) = self.editor.helper_mut() {
                                    helper.update_queries(self.session.query_names());
                                }
                            }

                            if is_exit {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("exit");
                    break;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    break;
                }
            }
        }

        let _ = self.editor.save_history(&self.history_path);

        Ok(())
    }
}
