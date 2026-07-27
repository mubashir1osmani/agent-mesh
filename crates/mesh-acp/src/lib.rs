//! ACP (Agent Client Protocol) transport. One implementation drives every agent that speaks
//! ACP: `opencode acp`, `gemini --acp`, `grok agent stdio`, and `cursor-agent acp`.
//!
//! The control plane acts as the ACP *client*: it issues `session/new`, `session/load` and
//! `session/prompt`, and it must answer the requests agents make of it (permission prompts,
//! file reads) or a prompt will hang forever waiting on approval.

pub mod conn;
pub mod responder;

use agent_client_protocol_schema::v1::{
    ContentBlock, ContentChunk, LoadSessionRequest, LoadSessionResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SessionConfigId, SessionConfigOptionValue,
    SessionConfigValueId, SessionId, SessionNotification, SessionUpdate,
    SetSessionConfigOptionRequest, TextContent,
};

/// The config option id agents use to expose model selection.
const MODEL_CONFIG_ID: &str = "model";
use conn::{Connection, Inbound};
use mesh_core::{
    AgentId, AgentTransport, Attached, Capabilities, Opened, Reply, Speaker, Transcript,
    TransportError, Turn, Usage, VendorSessionId,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// How to launch an ACP agent.
#[derive(Debug, Clone)]
pub struct AcpLaunch {
    pub agent: AgentId,
    pub program: String,
    pub args: Vec<String>,
    /// Model to select via `session/set_config_option` after creating a session, when the agent
    /// exposes a `model` option. `None` leaves the agent's own default alone.
    pub model: Option<String>,
}

/// An ACP transport for a single agent. Holds one process per `cwd`, since ACP sessions are
/// rooted at a working directory and sharing a process across unrelated roots would let
/// workspace context leak between them.
pub struct AcpTransport {
    launch: AcpLaunch,
    connections: Mutex<Vec<(PathBuf, Arc<Connection>)>>,
}

impl AcpTransport {
    pub fn new(launch: AcpLaunch) -> Self {
        Self {
            launch,
            connections: Mutex::new(Vec::new()),
        }
    }

    pub fn agent(&self) -> &AgentId {
        &self.launch.agent
    }

    /// Get or create the connection for `cwd`, performing the ACP handshake on first use.
    async fn connection(&self, cwd: &Path) -> Result<Arc<Connection>, TransportError> {
        {
            let existing = self.connections.lock().await;
            if let Some((_, conn)) = existing.iter().find(|(root, _)| root == cwd) {
                return Ok(Arc::clone(conn));
            }
        }

        let conn = Connection::spawn(
            self.launch.agent.clone(),
            &self.launch.program,
            &self.launch.args,
            cwd,
        )
        .await?;

        // Answer the agent's own requests for as long as this connection lives. Without this a
        // tool call needing approval stalls the prompt indefinitely, since the mesh runs agents
        // non-interactively and there is no human to ask.
        responder::spawn(Arc::clone(&conn), conn.subscribe().await);

        let init: serde_json::Value = conn
            .request(
                "initialize",
                &serde_json::json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": { "readTextFile": true, "writeTextFile": true },
                        "terminal": false
                    },
                    "clientInfo": { "name": "agent-mesh", "version": env!("CARGO_PKG_VERSION") }
                }),
            )
            .await?;

        if !supports_load_session(&init) {
            tracing::warn!(
                agent = %self.launch.agent,
                "agent does not advertise loadSession; cross-process resume will not work"
            );
        }

        let mut guard = self.connections.lock().await;
        // Another task may have raced us here; reuse whatever landed first so we never keep two
        // processes for one root.
        if let Some((_, existing)) = guard.iter().find(|(root, _)| root == cwd) {
            let winner = Arc::clone(existing);
            drop(guard);
            conn.shutdown().await;
            return Ok(winner);
        }
        guard.push((cwd.to_path_buf(), Arc::clone(&conn)));
        Ok(conn)
    }

    /// The connection that owns `vendor`, or an error if no session has been opened yet.
    async fn connection_for(
        &self,
        vendor: &VendorSessionId,
    ) -> Result<Arc<Connection>, TransportError> {
        let guard = self.connections.lock().await;
        // Sessions are keyed by cwd at the connection level; with one root per connection the
        // last opened connection owns the session. Track explicitly if multi-root fan-out grows.
        guard
            .last()
            .map(|(_, c)| Arc::clone(c))
            .ok_or_else(|| TransportError::Protocol {
                agent: self.launch.agent.clone(),
                detail: format!("no connection open for session {vendor}"),
            })
    }

    async fn select_model(
        &self,
        conn: &Connection,
        session: &VendorSessionId,
    ) -> Result<(), TransportError> {
        let Some(model) = self.launch.model.as_deref() else {
            return Ok(());
        };
        // Built from the typed schema rather than hand-written JSON: the wire field is
        // `configId` (not `optionId`) and the value is a tagged union, which a hand-rolled
        // object gets wrong silently until the agent rejects it.
        let request = SetSessionConfigOptionRequest::new(
            SessionId::new(session.as_str()),
            SessionConfigId::new(MODEL_CONFIG_ID),
            SessionConfigOptionValue::ValueId {
                value: SessionConfigValueId::new(model),
            },
        );
        let _: serde_json::Value = conn
            .request("session/set_config_option", &request)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentTransport for AcpTransport {
    async fn open(&self, cwd: &Path) -> Result<Opened, TransportError> {
        let conn = self.connection(cwd).await?;
        let request = NewSessionRequest::new(cwd.to_path_buf());
        let response: NewSessionResponse = conn.request("session/new", &request).await?;
        let vendor = VendorSessionId::new(response.session_id.0.to_string());
        self.select_model(&conn, &vendor).await?;
        Ok(Opened { vendor })
    }

    async fn attach(
        &self,
        vendor: &VendorSessionId,
        cwd: &Path,
    ) -> Result<Attached, TransportError> {
        let conn = self.connection(cwd).await?;

        // Subscribe BEFORE issuing the request: `session/load` replays the prior conversation as
        // notifications that arrive ahead of the response, so a late subscriber sees nothing.
        let mut updates = conn.subscribe().await;

        let request =
            LoadSessionRequest::new(SessionId::new(vendor.as_str()), cwd.to_path_buf());
        let _: LoadSessionResponse = conn.request("session/load", &request).await?;

        let mut turns: Vec<Turn> = Vec::new();
        while let Ok(inbound) = updates.try_recv() {
            if let Some((speaker, text)) = message_chunk(&inbound, vendor) {
                push_chunk(&mut turns, speaker, text);
            }
        }
        drop(updates);
        conn.prune_subscribers().await;

        Ok(Attached {
            vendor: vendor.clone(),
            replayed: Transcript::from_turns(turns),
        })
    }

    async fn prompt(&self, vendor: &VendorSessionId, text: &str) -> Result<Reply, TransportError> {
        let conn = self.connection_for(vendor).await?;
        let mut updates = conn.subscribe().await;

        let request = PromptRequest::new(
            SessionId::new(vendor.as_str()),
            vec![ContentBlock::Text(TextContent::new(text))],
        );
        let response: PromptResponse = conn.request("session/prompt", &request).await?;

        // Collect what the agent streamed for this session while the request was in flight.
        let mut collected = String::new();
        while let Ok(inbound) = updates.try_recv() {
            if let Some((Speaker::Agent, chunk)) = message_chunk(&inbound, vendor) {
                collected.push_str(&chunk);
            }
        }
        drop(updates);
        conn.prune_subscribers().await;

        Ok(Reply {
            text: collected,
            usage: response
                .usage
                .as_ref()
                .map(|u| Usage {
                    input_tokens: u.input_tokens,
                    output_tokens: u.output_tokens,
                    total_tokens: u.total_tokens,
                })
                .unwrap_or_default(),
            // ACP carries no cost field; claude and grok report spend through their own paths.
            cost: None,
        })
    }

    async fn list_sessions(&self, cwd: &Path) -> Result<Vec<VendorSessionId>, TransportError> {
        let conn = self.connection(cwd).await?;
        let response: serde_json::Value = conn
            .request("session/list", &serde_json::json!({ "cwd": cwd }))
            .await?;
        Ok(response
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|s| s.get("sessionId").and_then(serde_json::Value::as_str))
                    .map(VendorSessionId::new)
                    .collect()
            })
            .unwrap_or_default())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            resume: true,
            list_sessions: true,
            cancel: true,
            reports_cost: false,
        }
    }
}

fn supports_load_session(init: &serde_json::Value) -> bool {
    init.get("agentCapabilities")
        .and_then(|c| c.get("loadSession"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Extract a user/agent message chunk for `session` from a raw inbound message.
pub fn message_chunk(inbound: &Inbound, session: &VendorSessionId) -> Option<(Speaker, String)> {
    if inbound.method != "session/update" {
        return None;
    }
    let parsed: SessionNotification = serde_json::from_value(inbound.params.clone()).ok()?;
    if parsed.session_id.0.as_ref() != session.as_str() {
        return None;
    }
    match parsed.update {
        SessionUpdate::UserMessageChunk(chunk) => Some((Speaker::User, chunk_text(&chunk)?)),
        SessionUpdate::AgentMessageChunk(chunk) => Some((Speaker::Agent, chunk_text(&chunk)?)),
        SessionUpdate::AgentThoughtChunk(chunk) => {
            Some((Speaker::AgentThought, chunk_text(&chunk)?))
        }
        _ => None,
    }
}

fn chunk_text(chunk: &ContentChunk) -> Option<String> {
    match &chunk.content {
        ContentBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    }
}

/// Append a chunk, coalescing consecutive chunks from the same speaker into one turn so a
/// streamed reply reads as a single message rather than dozens of fragments.
pub fn push_chunk(turns: &mut Vec<Turn>, speaker: Speaker, text: String) {
    match turns.last_mut() {
        Some(last) if last.speaker == speaker => last.text.push_str(&text),
        _ => turns.push(Turn { speaker, text }),
    }
}
