//! Wire shapes for the `codex app-server` protocol.
//!
//! Field names come from `codex app-server generate-json-schema` and were confirmed against a
//! live process. Only the parts the mesh needs are modelled; codex exposes ~90 methods and
//! mirroring all of them would be dead weight.

use mesh_core::{Inbound, Speaker, Transcript, Turn, Usage, VendorSessionId};
use serde::Deserialize;

/// Response to `thread/start`: the thread is nested under a `thread` key.
#[derive(Debug, Deserialize)]
pub struct ThreadEnvelope {
    pub thread: ThreadInfo,
}

#[derive(Debug, Deserialize)]
pub struct ThreadInfo {
    pub id: String,
}

/// Response to `thread/list`. The array is under `data`, not `threads`, and is paginated.
#[derive(Debug, Default, Deserialize)]
pub struct ThreadList {
    #[serde(default)]
    pub data: Vec<ThreadInfo>,
}

/// Response to `thread/read` with `includeTurns: true`. Turns are nested under `thread`, not at
/// the top level.
#[derive(Debug, Default, Deserialize)]
pub struct ThreadRead {
    #[serde(default)]
    pub thread: Option<ThreadBody>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ThreadBody {
    #[serde(default)]
    pub turns: Vec<ThreadTurn>,
}

impl ThreadRead {
    fn turns(&self) -> &[ThreadTurn] {
        self.thread.as_ref().map(|t| t.turns.as_slice()).unwrap_or(&[])
    }
}

#[derive(Debug, Deserialize)]
pub struct ThreadTurn {
    #[serde(default)]
    pub items: Vec<Item>,
}

/// A conversation item. Codex tags these with a `type` discriminator; the mesh only cares about
/// the two that carry conversation text, and ignores reasoning, tool calls and patches.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Item {
    #[serde(rename = "userMessage")]
    UserMessage {
        #[serde(default)]
        content: Vec<ContentPart>,
    },
    #[serde(rename = "agentMessage")]
    AgentMessage {
        #[serde(default)]
        text: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub struct ContentPart {
    #[serde(default)]
    pub text: String,
}

/// Build a normalized transcript from a `thread/read` response.
pub fn transcript_from_turns(read: &ThreadRead) -> Transcript {
    Transcript::from_turns(read.turns().iter().flat_map(|turn| {
        turn.items.iter().filter_map(|item| match item {
            Item::UserMessage { content } => {
                let text = content
                    .iter()
                    .map(|p| p.text.as_str())
                    .collect::<Vec<_>>()
                    .join("");
                (!text.is_empty()).then_some(Turn {
                    speaker: Speaker::User,
                    text,
                })
            }
            Item::AgentMessage { text } => (!text.is_empty()).then(|| Turn {
                speaker: Speaker::Agent,
                text: text.clone(),
            }),
            Item::Other => None,
        })
    }))
}

/// The subset of codex events that drive a prompt turn to completion.
#[derive(Debug)]
pub enum Event {
    /// An incremental chunk of the agent's reply.
    AgentText(String),
    /// A finished agent message, carrying the authoritative full text.
    AgentMessageComplete(String),
    TokenUsage(Usage),
    TurnCompleted,
    TurnFailed(String),
    Other,
}

/// Does this event belong to `session`? A single app-server connection multiplexes every thread,
/// so without this filter concurrent sessions would contaminate each other's replies.
pub fn belongs_to(params: &serde_json::Value, session: &VendorSessionId) -> bool {
    match params.get("threadId").and_then(serde_json::Value::as_str) {
        Some(id) => id == session.as_str(),
        // Events without a threadId (protocol-level status) are not session traffic; let them
        // through so terminal events are never dropped on a technicality.
        None => true,
    }
}

pub fn classify(event: &Inbound) -> Event {
    match event.method.as_str() {
        "item/agentMessage/delta" => event
            .params
            .get("delta")
            .and_then(serde_json::Value::as_str)
            .map(|d| Event::AgentText(d.to_owned()))
            .unwrap_or(Event::Other),

        "item/completed" => {
            let item = event.params.get("item");
            let is_agent_message = item
                .and_then(|i| i.get("type"))
                .and_then(serde_json::Value::as_str)
                == Some("agentMessage");
            if !is_agent_message {
                return Event::Other;
            }
            item.and_then(|i| i.get("text"))
                .and_then(serde_json::Value::as_str)
                .map(|t| Event::AgentMessageComplete(t.to_owned()))
                .unwrap_or(Event::Other)
        }

        // `tokenUsage` carries both `last` (this turn) and `total` (cumulative for the thread).
        // Report `last`: on a long-lived session `total` runs to millions and would read as the
        // cost of a single question.
        "thread/tokenUsage/updated" => event
            .params
            .pointer("/tokenUsage/last")
            .or_else(|| event.params.pointer("/tokenUsage/total"))
            .map(|breakdown| {
                let field = |name: &str| {
                    breakdown
                        .get(name)
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default()
                };
                Event::TokenUsage(Usage {
                    input_tokens: field("inputTokens"),
                    output_tokens: field("outputTokens"),
                    total_tokens: field("totalTokens"),
                })
            })
            .unwrap_or(Event::Other),

        "turn/completed" => Event::TurnCompleted,
        "turn/failed" | "turn/aborted" => Event::TurnFailed(
            event
                .params
                .pointer("/error/message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("turn failed")
                .to_owned(),
        ),
        _ => Event::Other,
    }
}
