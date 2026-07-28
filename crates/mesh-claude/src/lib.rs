//! Adapter for `claude` in headless streaming mode.
//!
//! Claude does not speak ACP, and its JSON-RPC surfaces point the wrong way for this purpose.
//! It is a capable MCP *client* (it connects out to MCP servers, which is how agent-mesh gets
//! attached to it), and `claude mcp serve` makes it an MCP *server* too — but that server
//! exposes Claude Code's own toolbox (`Bash`, `Read`, `Edit`, `Agent`, ...), not its
//! conversation. There is no `session/new` or `session/prompt` equivalent, so there is no way
//! to drive a session through it.
//!
//! What it does offer is a true persistent bidirectional loop over newline-delimited JSON
//! (not JSON-RPC: no `id`, no method dispatch, no request correlation):
//!
//! ```text
//! claude -p --input-format stream-json --output-format stream-json --session-id <uuid>
//! ```
//!
//! Newline-delimited user messages go in, newline-delimited events come out, and the process
//! stays alive across turns. Claude is also the only agent that lets the caller *pin* the session
//! id up front, so the mesh chooses ids here rather than reading them back.

pub mod events;

use events::{ResultEnvelope, StreamEvent};
use mesh_core::{
    AgentId, AgentTransport, Attached, Capabilities, CostMicros, Opened, Reply, Transcript,
    TransportError, Usage, VendorSessionId,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc};

/// Skips permission prompts. An orchestrated session has no human to approve a tool call, so
/// without this every tool use would stall the turn.
const PERMISSION_BYPASS: &str = "bypassPermissions";

#[derive(Debug, Clone)]
pub struct ClaudeLaunch {
    pub agent: AgentId,
    pub program: String,
    pub model: Option<String>,
}

/// One live `claude` process, dedicated to a single session.
struct Session {
    stdin: Mutex<ChildStdin>,
    events: Mutex<mpsc::UnboundedReceiver<StreamEvent>>,
    child: Mutex<Child>,
    /// Highest `total_cost_usd` seen from this process, in micros. Claude reports spend
    /// cumulatively for the life of the process, so a per-turn cost is the difference from the
    /// previous turn. Scoped to the process because the counter restarts from zero when the
    /// session is resumed into a new one.
    billed: Mutex<CostMicros>,
}

impl Session {
    /// Kill the process backing this session. `kill_on_drop` covers the normal path, but a session
    /// replaced in the map (e.g. reattached) would otherwise leave its old process running.
    async fn shutdown(&self) {
        let _ = self.child.lock().await.start_kill();
    }
}

pub struct ClaudeTransport {
    launch: ClaudeLaunch,
    sessions: Mutex<HashMap<VendorSessionId, Arc<Session>>>,
}

impl ClaudeTransport {
    pub fn new(launch: ClaudeLaunch) -> Self {
        Self {
            launch,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub fn agent(&self) -> &AgentId {
        &self.launch.agent
    }

    /// Spawn a process bound to `session`. `--session-id` pins a new session; `--resume` reaches
    /// an existing one. Both are valid with `-p`.
    async fn spawn(
        &self,
        session: &VendorSessionId,
        cwd: &Path,
        resume: bool,
    ) -> Result<Arc<Session>, TransportError> {
        let mut args = vec![
            "-p".to_owned(),
            "--input-format".to_owned(),
            "stream-json".to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
            "--verbose".to_owned(),
            "--permission-mode".to_owned(),
            PERMISSION_BYPASS.to_owned(),
        ];
        if resume {
            args.push("--resume".to_owned());
            args.push(session.as_str().to_owned());
        } else {
            args.push("--session-id".to_owned());
            args.push(session.as_str().to_owned());
        }
        if let Some(model) = self.launch.model.as_deref() {
            args.push("--model".to_owned());
            args.push(model.to_owned());
        }

        let mut child = Command::new(&self.launch.program)
            .args(&args)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|source| TransportError::Spawn {
                command: format!("{} {}", self.launch.program, args.join(" ")),
                source,
            })?;

        let stdin = child.stdin.take().ok_or_else(|| TransportError::Protocol {
            agent: self.launch.agent.clone(),
            detail: "claude stdin was not piped".to_owned(),
        })?;
        let stdout = child.stdout.take().ok_or_else(|| TransportError::Protocol {
            agent: self.launch.agent.clone(),
            detail: "claude stdout was not piped".to_owned(),
        })?;

        if let Some(stderr) = child.stderr.take() {
            let agent = self.launch.agent.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(agent = %agent, "stderr: {line}");
                }
            });
        }

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<StreamEvent>(&line) {
                    Ok(event) => {
                        if tx.send(event).is_err() {
                            break;
                        }
                    }
                    Err(err) => tracing::debug!("unparsed claude event: {err}: {line}"),
                }
            }
        });

        Ok(Arc::new(Session {
            stdin: Mutex::new(stdin),
            events: Mutex::new(rx),
            child: Mutex::new(child),
            billed: Mutex::new(CostMicros(0)),
        }))
    }

    async fn session(&self, id: &VendorSessionId) -> Result<Arc<Session>, TransportError> {
        self.sessions
            .lock()
            .await
            .get(id)
            .map(Arc::clone)
            .ok_or_else(|| TransportError::Protocol {
                agent: self.launch.agent.clone(),
                detail: format!("no live claude process for session {id}"),
            })
    }
}

#[async_trait::async_trait]
impl AgentTransport for ClaudeTransport {
    async fn open(&self, cwd: &Path) -> Result<Opened, TransportError> {
        // Claude accepts a caller-chosen id, so the mesh mints one rather than parsing it back.
        let vendor = VendorSessionId::new(uuid::Uuid::new_v4().to_string());
        let session = self.spawn(&vendor, cwd, false).await?;
        if let Some(replaced) = self.sessions.lock().await.insert(vendor.clone(), session) {
            replaced.shutdown().await;
        }
        Ok(Opened { vendor })
    }

    async fn attach(
        &self,
        vendor: &VendorSessionId,
        cwd: &Path,
    ) -> Result<Attached, TransportError> {
        let session = self.spawn(vendor, cwd, true).await?;
        if let Some(replaced) = self.sessions.lock().await.insert(vendor.clone(), session) {
            replaced.shutdown().await;
        }

        // Claude does not replay the conversation over the stream on resume; the transcript lives
        // on disk as JSONL, which is the only way to read prior turns back.
        Ok(Attached {
            vendor: vendor.clone(),
            replayed: read_transcript(cwd, vendor).unwrap_or_default(),
        })
    }

    async fn prompt(&self, vendor: &VendorSessionId, text: &str) -> Result<Reply, TransportError> {
        let session = self.session(vendor).await?;

        let message = serde_json::json!({
            "type": "user",
            "message": { "role": "user", "content": [{ "type": "text", "text": text }] }
        });
        let mut wire = serde_json::to_vec(&message).map_err(|e| TransportError::Protocol {
            agent: self.launch.agent.clone(),
            detail: format!("could not encode prompt: {e}"),
        })?;
        wire.push(b'\n');

        {
            let mut stdin = session.stdin.lock().await;
            stdin.write_all(&wire).await.map_err(|_| {
                TransportError::ConnectionClosed {
                    agent: self.launch.agent.clone(),
                }
            })?;
            stdin.flush().await.map_err(|_| {
                TransportError::ConnectionClosed {
                    agent: self.launch.agent.clone(),
                }
            })?;
        }

        let mut events = session.events.lock().await;
        while let Some(event) = events.recv().await {
            match event {
                StreamEvent::Result(envelope) => {
                    let mut billed = session.billed.lock().await;
                    return finish(envelope, &self.launch.agent, &mut billed);
                }
                StreamEvent::Other => continue,
            }
        }

        Err(TransportError::ConnectionClosed {
            agent: self.launch.agent.clone(),
        })
    }

    async fn list_sessions(&self, cwd: &Path) -> Result<Vec<VendorSessionId>, TransportError> {
        // Claude has no listing command; sessions are files named `<session-id>.jsonl` under a
        // slug of the working directory.
        let dir = transcript_dir(cwd);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        Ok(entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                if path.extension()? != "jsonl" {
                    return None;
                }
                let stem = path.file_stem()?.to_string_lossy().to_string();
                Some(VendorSessionId::new(stem))
            })
            .collect())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            resume: true,
            list_sessions: true,
            cancel: true,
            // Claude is one of only two agents that reports spend.
            reports_cost: true,
        }
    }
}

/// Turn a terminal `result` event into a reply.
///
/// `billed` is this process's running cost total and is updated in place: Claude reports
/// `total_cost_usd` cumulatively, so the cost *of this turn* is the increment over the last one.
/// Reporting the raw field would bill turn 1 again on turn 2, making an N-turn session cost
/// O(N^2).
fn finish(
    envelope: ResultEnvelope,
    agent: &AgentId,
    billed: &mut CostMicros,
) -> Result<Reply, TransportError> {
    if envelope.is_error {
        return Err(TransportError::AgentRefused {
            agent: agent.clone(),
            message: envelope.result,
        });
    }

    let cost = envelope.total_cost_usd.map(events::usd_to_micros).map(|reported| {
        // A cumulative total should only ever climb. If it dropped, the counter was reset rather
        // than incremented, so treat the whole figure as this turn's cost instead of computing a
        // meaningless negative delta.
        let delta = if reported.0 >= billed.0 {
            CostMicros(reported.0 - billed.0)
        } else {
            reported
        };
        *billed = reported;
        delta
    });

    let input_tokens = envelope.usage.total_input();
    let output_tokens = envelope.usage.output_tokens;
    Ok(Reply {
        text: envelope.result,
        usage: Usage {
            input_tokens,
            output_tokens,
            total_tokens: input_tokens.saturating_add(output_tokens),
        },
        cost,
    })
}

/// Claude's transcript directory: the absolute cwd with `/` and `.` replaced by `-`.
pub fn transcript_dir(cwd: &Path) -> PathBuf {
    let slug: String = cwd
        .to_string_lossy()
        .chars()
        .map(|c| if c == '/' || c == '.' { '-' } else { c })
        .collect();
    home().join(".claude").join("projects").join(slug)
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

/// Read a session transcript from Claude's on-disk JSONL.
pub fn read_transcript(cwd: &Path, session: &VendorSessionId) -> Option<Transcript> {
    let path = transcript_dir(cwd).join(format!("{session}.jsonl"));
    let raw = std::fs::read_to_string(path).ok()?;
    Some(events::parse_transcript(&raw))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use mesh_core::CostMicros;

    #[test]
    fn transcript_dir_slugifies_path_like_claude_does() {
        let dir = transcript_dir(Path::new("/Users/x/l/litellm"));
        assert!(
            dir.ends_with("-Users-x-l-litellm"),
            "unexpected slug: {dir:?}"
        );
    }

    /// Dots become dashes too, otherwise hidden directories would resolve to the wrong folder.
    #[test]
    fn transcript_dir_replaces_dots() {
        let dir = transcript_dir(Path::new("/Users/x/.config/app"));
        assert!(dir.ends_with("-Users-x--config-app"), "got {dir:?}");
    }

    #[test]
    fn error_result_becomes_a_refusal_not_a_reply() {
        let envelope = ResultEnvelope {
            result: "Credit balance is too low".to_owned(),
            is_error: true,
            total_cost_usd: Some(0.0),
            usage: Default::default(),
            session_id: None,
        };

        let outcome = finish(envelope, &AgentId::new("claude"), &mut CostMicros(0));

        assert!(
            matches!(outcome, Err(TransportError::AgentRefused { .. })),
            "an is_error result must not be handed back as a successful reply"
        );
    }

    #[test]
    fn successful_result_carries_text_usage_and_cost() {
        let envelope = ResultEnvelope {
            result: "MANGO".to_owned(),
            is_error: false,
            total_cost_usd: Some(0.0123),
            usage: events::UsageEnvelope {
                input_tokens: 10,
                output_tokens: 4,
                ..Default::default()
            },
            session_id: None,
        };

        let reply =
            finish(envelope, &AgentId::new("claude"), &mut CostMicros(0)).expect("should succeed");

        assert_eq!(reply.text, "MANGO");
        assert_eq!(reply.usage.total_tokens, 14);
        assert_eq!(reply.cost, Some(CostMicros(12_300)));
    }

    /// Absent cost must stay absent rather than read as free.
    #[test]
    fn missing_cost_stays_none() {
        let envelope = ResultEnvelope {
            result: "ok".to_owned(),
            is_error: false,
            total_cost_usd: None,
            usage: Default::default(),
            session_id: None,
        };

        assert_eq!(
            finish(envelope, &AgentId::new("claude"), &mut CostMicros(0))
                .expect("ok")
                .cost,
            None
        );
    }

    fn envelope(cost: f64, usage: events::UsageEnvelope) -> ResultEnvelope {
        ResultEnvelope {
            result: "ok".to_owned(),
            is_error: false,
            total_cost_usd: Some(cost),
            usage,
            session_id: None,
        }
    }

    /// Recorded from a live session: `total_cost_usd` is cumulative for the process, so summing
    /// the raw field bills every earlier turn again and an N-turn session costs O(N^2). Observed
    /// live: three trivial replies reported 0.362, 0.416, 0.447 for a true spend of ~0.09.
    #[test]
    fn cost_is_the_per_turn_delta_not_the_cumulative_total() {
        let agent = AgentId::new("claude");
        let mut billed = CostMicros(0);

        let first = finish(envelope(0.36210625, Default::default()), &agent, &mut billed)
            .expect("first turn");
        let second = finish(envelope(0.40636275, Default::default()), &agent, &mut billed)
            .expect("second turn");

        assert_eq!(first.cost, Some(CostMicros(362_106)));
        assert_eq!(
            second.cost,
            Some(CostMicros(44_257)),
            "second turn must bill only its own increment"
        );
    }

    /// Resuming a session spawns a new process whose cost counter restarts, so the reported total
    /// can drop. Observed live: 0.406 followed by 0.029 after `--resume`. A naive subtraction
    /// would underflow.
    #[test]
    fn a_reset_cost_counter_is_billed_whole_not_negative() {
        let agent = AgentId::new("claude");
        let mut billed = CostMicros(406_363);

        let reply =
            finish(envelope(0.02908775, Default::default()), &agent, &mut billed).expect("resumed");

        assert_eq!(
            reply.cost,
            Some(CostMicros(29_088)),
            "a dropped total means a fresh counter, so bill the figure as-is"
        );
    }

    /// Cached reads are billed, just more cheaply. Reading only `input_tokens` reports 4 for a
    /// turn that actually read ~58k tokens, making a long session look nearly free.
    #[test]
    fn input_tokens_include_cache_creation_and_reads() {
        let usage = events::UsageEnvelope {
            input_tokens: 4,
            cache_creation_input_tokens: 11,
            cache_read_input_tokens: 57_848,
            output_tokens: 3,
        };

        let reply = finish(envelope(0.0, usage), &AgentId::new("claude"), &mut CostMicros(0))
            .expect("ok");

        assert_eq!(reply.usage.input_tokens, 57_863);
        assert_eq!(reply.usage.total_tokens, 57_866);
    }
}
