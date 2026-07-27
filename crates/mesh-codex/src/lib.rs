//! Adapter for `codex app-server`.
//!
//! Codex does not speak ACP; it has its own JSON-RPC-over-stdio protocol whose wire types it can
//! generate itself (`codex app-server generate-json-schema`). The shapes in `proto` were read
//! from that generated schema and confirmed against a live process.
//!
//! Codex is also the one agent that will not let a caller pin a session id: `thread/start` mints
//! the id and hands it back, so the mesh reads it rather than choosing it.

pub mod proto;

use mesh_core::jsonrpc::{Connection, Inbound};
use mesh_core::{
    AgentId, AgentTransport, Attached, Capabilities, Opened, Reply, TransportError, Usage,
    VendorSessionId,
};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

pub use proto::{Event, classify, transcript_from_turns};

/// Approval policy sent on every thread. `never` is the only workable value for an orchestrated
/// session: codex documents it as "never ask for user approval; execution failures are
/// immediately returned to the model", so a failing command becomes model-visible text rather
/// than a prompt nobody can answer.
const APPROVAL_NEVER: &str = "never";

/// Allow edits inside the workspace but not outside it.
const SANDBOX_WORKSPACE_WRITE: &str = "workspace-write";

pub struct CodexTransport {
    agent: AgentId,
    program: String,
    conn: Mutex<Option<Arc<Connection>>>,
}

impl CodexTransport {
    pub fn new(agent: AgentId, program: impl Into<String>) -> Self {
        Self {
            agent,
            program: program.into(),
            conn: Mutex::new(None),
        }
    }

    pub fn agent(&self) -> &AgentId {
        &self.agent
    }

    async fn connection(&self, cwd: &Path) -> Result<Arc<Connection>, TransportError> {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(Arc::clone(conn));
        }

        let conn = Connection::spawn(
            self.agent.clone(),
            &self.program,
            &["app-server".to_owned()],
            cwd,
        )
        .await?;

        let _: serde_json::Value = conn
            .request(
                "initialize",
                &serde_json::json!({
                    "clientInfo": {
                        "name": "agent-mesh",
                        "title": "agent-mesh",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )
            .await?;

        *guard = Some(Arc::clone(&conn));
        Ok(conn)
    }

    async fn active(&self) -> Result<Arc<Connection>, TransportError> {
        self.conn
            .lock()
            .await
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| TransportError::Protocol {
                agent: self.agent.clone(),
                detail: "no codex app-server connection open".to_owned(),
            })
    }
}

#[async_trait::async_trait]
impl AgentTransport for CodexTransport {
    async fn open(&self, cwd: &Path) -> Result<Opened, TransportError> {
        let conn = self.connection(cwd).await?;
        let response: proto::ThreadEnvelope = conn
            .request(
                "thread/start",
                &serde_json::json!({
                    "cwd": cwd,
                    "approvalPolicy": APPROVAL_NEVER,
                    "sandbox": SANDBOX_WORKSPACE_WRITE,
                }),
            )
            .await?;
        Ok(Opened {
            vendor: VendorSessionId::new(response.thread.id),
        })
    }

    async fn attach(
        &self,
        vendor: &VendorSessionId,
        cwd: &Path,
    ) -> Result<Attached, TransportError> {
        let conn = self.connection(cwd).await?;

        let _: serde_json::Value = conn
            .request(
                "thread/resume",
                &serde_json::json!({
                    "threadId": vendor.as_str(),
                    "approvalPolicy": APPROVAL_NEVER,
                    "sandbox": SANDBOX_WORKSPACE_WRITE,
                }),
            )
            .await?;

        // Unlike ACP, codex does not replay the conversation on resume; it has to be read back.
        let read: proto::ThreadRead = conn
            .request(
                "thread/read",
                &serde_json::json!({ "threadId": vendor.as_str(), "includeTurns": true }),
            )
            .await?;

        Ok(Attached {
            vendor: vendor.clone(),
            replayed: transcript_from_turns(&read),
        })
    }

    async fn prompt(&self, vendor: &VendorSessionId, text: &str) -> Result<Reply, TransportError> {
        let conn = self.active().await?;
        let mut events = conn.subscribe().await;

        let _: serde_json::Value = conn
            .request(
                "turn/start",
                &serde_json::json!({
                    "threadId": vendor.as_str(),
                    "input": [{ "type": "text", "text": text }],
                }),
            )
            .await?;

        // `turn/start` returns as soon as the turn is accepted, so the reply is assembled from
        // the event stream until a terminal turn event arrives.
        let outcome = collect_turn(&mut events, vendor, &self.agent).await;
        drop(events);
        conn.prune_subscribers().await;
        outcome
    }

    async fn list_sessions(&self, cwd: &Path) -> Result<Vec<VendorSessionId>, TransportError> {
        let conn = self.connection(cwd).await?;
        let listed: proto::ThreadList = conn.request("thread/list", &serde_json::json!({})).await?;
        Ok(listed
            .data
            .into_iter()
            .map(|t| VendorSessionId::new(t.id))
            .collect())
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            resume: true,
            list_sessions: true,
            cancel: true,
            // Codex reports tokens only; there is no cost field on the wire.
            reports_cost: false,
        }
    }
}

/// Assemble one turn's reply from the event stream.
async fn collect_turn(
    events: &mut mpsc::UnboundedReceiver<Inbound>,
    vendor: &VendorSessionId,
    agent: &AgentId,
) -> Result<Reply, TransportError> {
    let mut text = String::new();
    let mut usage = Usage::default();

    while let Some(event) = events.recv().await {
        if !proto::belongs_to(&event.params, vendor) {
            continue;
        }
        match classify(&event) {
            Event::AgentText(chunk) => text.push_str(&chunk),
            Event::AgentMessageComplete(full) => {
                // The completed item carries the authoritative text; prefer it over accumulated
                // deltas so a dropped delta cannot silently truncate the reply.
                if !full.is_empty() {
                    text = full;
                }
            }
            Event::TokenUsage(u) => usage = u,
            Event::TurnCompleted => {
                return Ok(Reply {
                    text,
                    usage,
                    cost: None,
                });
            }
            Event::TurnFailed(message) => {
                return Err(TransportError::AgentRefused {
                    agent: agent.clone(),
                    message,
                });
            }
            Event::Other => {}
        }
    }

    Err(TransportError::ConnectionClosed {
        agent: agent.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_core::{Speaker, Turn};

    fn event(method: &str, params: serde_json::Value) -> Inbound {
        Inbound {
            method: method.to_owned(),
            params,
            id: None,
        }
    }

    #[test]
    fn transcript_reads_user_and_agent_messages() {
        let read: proto::ThreadRead = serde_json::from_value(serde_json::json!({
            "thread": { "turns": [{
                "items": [
                    { "type": "userMessage", "content": [{ "type": "text", "text": "hello" }] },
                    { "type": "reasoning", "summary": "thinking" },
                    { "type": "agentMessage", "text": "hi there" }
                ]
            }] }
        }))
        .expect("schema shape");

        let transcript = transcript_from_turns(&read);

        assert_eq!(
            transcript.turns,
            vec![
                Turn { speaker: Speaker::User, text: "hello".to_owned() },
                Turn { speaker: Speaker::Agent, text: "hi there".to_owned() },
            ],
            "only conversation items belong in the transcript"
        );
    }

    /// Recorded verbatim from a live `codex app-server` response. Turns are nested under `thread`
    /// and the list lives under `data`; both were originally coded as top-level fields, which made
    /// every transcript read come back empty while looking like a working call.
    #[test]
    fn parses_real_thread_read_payload() {
        let recorded = serde_json::json!({
            "thread": {
                "id": "019fa211-1857-7932-a5ad-fe02e3d79311",
                "preview": "Remember this: the deploy key is ORCHID-77. Reply with just: stored",
                "cwd": "/private/tmp/mesh-cross",
                "status": { "type": "notLoaded" },
                "turns": [{
                    "id": "019fa211-188d-74e0-8091-55a73d7d65e3",
                    "items": [
                        { "type": "userMessage", "id": "item-1", "clientId": null,
                          "content": [{ "type": "text",
                                        "text": "Remember this: the deploy key is ORCHID-77. Reply with just: stored",
                                        "text_elements": [] }] },
                        { "type": "agentMessage", "id": "item-2", "text": "stored",
                          "phase": "final_answer", "memoryCitation": null }
                    ],
                    "itemsView": "full", "status": "completed", "error": null
                }]
            }
        });

        let read: proto::ThreadRead = serde_json::from_value(recorded).expect("real payload");
        let transcript = transcript_from_turns(&read);

        assert_eq!(transcript.turns.len(), 2, "got {:?}", transcript.turns);
        assert!(transcript.turns[0].text.contains("ORCHID-77"));
        assert_eq!(transcript.turns[1].text, "stored");
    }

    /// `thread/list` returns its array under `data`. Reading the wrong key yields an empty list
    /// that looks like "no sessions" rather than a bug.
    #[test]
    fn parses_real_thread_list_payload() {
        let recorded = serde_json::json!({
            "data": [
                { "id": "019fa211-1857-7932-a5ad-fe02e3d79311", "preview": "one" },
                { "id": "019fa211-aaaa-7932-a5ad-fe02e3d79312", "preview": "two" }
            ],
            "nextCursor": null,
            "backwardsCursor": null
        });

        let listed: proto::ThreadList = serde_json::from_value(recorded).expect("real payload");

        assert_eq!(listed.data.len(), 2);
        assert_eq!(listed.data[0].id, "019fa211-1857-7932-a5ad-fe02e3d79311");
    }

    #[test]
    fn transcript_is_empty_when_no_turns_present() {
        let read: proto::ThreadRead =
            serde_json::from_value(serde_json::json!({})).expect("schema shape");
        assert!(transcript_from_turns(&read).is_empty());
    }

    #[test]
    fn classifies_agent_message_delta() {
        let e = event("item/agentMessage/delta", serde_json::json!({ "delta": "MAN" }));
        assert!(matches!(classify(&e), Event::AgentText(t) if t == "MAN"));
    }

    #[test]
    fn classifies_completed_agent_message() {
        let e = event(
            "item/completed",
            serde_json::json!({ "item": { "type": "agentMessage", "text": "MANGO" } }),
        );
        assert!(matches!(classify(&e), Event::AgentMessageComplete(t) if t == "MANGO"));
    }

    /// A completed *user* message must not be read as the agent's reply, or the mesh would echo
    /// the prompt back as the answer.
    #[test]
    fn completed_user_message_is_not_agent_text() {
        let e = event(
            "item/completed",
            serde_json::json!({ "item": { "type": "userMessage", "content": [] } }),
        );
        assert!(matches!(classify(&e), Event::Other));
    }

    #[test]
    fn classifies_terminal_turn_events() {
        assert!(matches!(
            classify(&event("turn/completed", serde_json::json!({}))),
            Event::TurnCompleted
        ));
        assert!(matches!(
            classify(&event("turn/failed", serde_json::json!({ "error": { "message": "boom" } }))),
            Event::TurnFailed(m) if m == "boom"
        ));
    }

    /// Usage must come from `last` (this turn), not `total` (cumulative for the thread). On a
    /// long-running session `total` reaches millions, which would be reported as the cost of one
    /// question. Observed live: a single prompt on a 40-turn session reported 16.8M input tokens.
    #[test]
    fn extracts_per_turn_usage_not_cumulative() {
        let e = event(
            "thread/tokenUsage/updated",
            serde_json::json!({
                "tokenUsage": {
                    "last":  { "totalTokens": 10993, "inputTokens": 10987, "outputTokens": 6 },
                    "total": { "totalTokens": 16825520, "inputTokens": 16800000, "outputTokens": 68564 }
                }
            }),
        );
        match classify(&e) {
            Event::TokenUsage(u) => {
                assert_eq!(
                    (u.input_tokens, u.output_tokens, u.total_tokens),
                    (10987, 6, 10993),
                    "must report this turn, not the thread total"
                );
            }
            other => panic!("expected token usage, got {other:?}"),
        }
    }

    /// Older payloads may only carry `total`; fall back rather than reporting zero.
    #[test]
    fn falls_back_to_total_when_last_is_absent() {
        let e = event(
            "thread/tokenUsage/updated",
            serde_json::json!({
                "tokenUsage": { "total": {
                    "totalTokens": 500, "inputTokens": 480, "outputTokens": 20
                }}
            }),
        );
        match classify(&e) {
            Event::TokenUsage(u) => assert_eq!(u.total_tokens, 500),
            other => panic!("expected token usage, got {other:?}"),
        }
    }

    /// One app-server connection multiplexes every thread, so events for a different thread must
    /// be ignored or concurrent sessions contaminate each other.
    #[test]
    fn events_are_filtered_by_thread() {
        let mine = VendorSessionId::new("thread-a");
        assert!(!proto::belongs_to(
            &serde_json::json!({ "threadId": "thread-b" }),
            &mine
        ));
        assert!(proto::belongs_to(
            &serde_json::json!({ "threadId": "thread-a" }),
            &mine
        ));
    }
}
