//! Answers the requests an ACP agent makes of the client.
//!
//! The mesh drives agents non-interactively, so there is no human to approve a tool call. Every
//! agent-initiated request must still get a reply: ACP requests block the agent's turn, so an
//! unanswered permission prompt hangs the prompt forever.

use mesh_core::jsonrpc::{Connection, Inbound};
use mesh_core::AgentId;
use std::sync::Arc;
use tokio::sync::mpsc;

/// What the client should send back for a given agent request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// A JSON-RPC result to return at the request's id.
    Result(serde_json::Value),
    /// Nothing to send (the message was a notification, not a request).
    Ignore,
}

/// Decide how to answer one inbound message. Pure so it can be unit-tested without a process.
pub fn answer(inbound: &Inbound) -> Answer {
    if !inbound.is_request() {
        return Answer::Ignore;
    }

    match inbound.method.as_str() {
        "session/request_permission" => Answer::Result(approve_first_option(&inbound.params)),
        "fs/read_text_file" => Answer::Result(read_text_file(&inbound.params)),
        "fs/write_text_file" => Answer::Result(write_text_file(&inbound.params)),
        // Anything else still needs *a* reply, and an empty result is the least surprising one.
        _ => Answer::Result(serde_json::json!({})),
    }
}

/// Select the first offered permission option. ACP does not label options semantically, so
/// there is no reliable way to pick "allow" by name across agents; the first option is the
/// vendor's own default and is what non-interactive ACP clients use.
fn approve_first_option(params: &serde_json::Value) -> serde_json::Value {
    let first = params
        .get("options")
        .and_then(serde_json::Value::as_array)
        .and_then(|opts| opts.first())
        .and_then(|opt| opt.get("optionId"))
        .and_then(serde_json::Value::as_str);

    match first {
        Some(option_id) => serde_json::json!({
            "outcome": { "outcome": "selected", "optionId": option_id }
        }),
        None => serde_json::json!({ "outcome": { "outcome": "cancelled" } }),
    }
}

fn read_text_file(params: &serde_json::Value) -> serde_json::Value {
    let Some(path) = params.get("path").and_then(serde_json::Value::as_str) else {
        return serde_json::json!({ "content": "" });
    };
    match std::fs::read_to_string(path) {
        Ok(content) => serde_json::json!({ "content": content }),
        Err(err) => {
            tracing::debug!("fs/read_text_file {path} failed: {err}");
            serde_json::json!({ "content": "" })
        }
    }
}

fn write_text_file(params: &serde_json::Value) -> serde_json::Value {
    let path = params.get("path").and_then(serde_json::Value::as_str);
    let content = params
        .get("content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if let Some(path) = path
        && let Err(err) = std::fs::write(path, content)
    {
        tracing::debug!("fs/write_text_file {path} failed: {err}");
    }
    serde_json::json!({})
}

/// Pump inbound messages, replying to every request.
pub fn spawn(conn: Arc<Connection>, mut rx: mpsc::UnboundedReceiver<Inbound>) {
    tokio::spawn(async move {
        while let Some(inbound) = rx.recv().await {
            let Answer::Result(result) = answer(&inbound) else {
                continue;
            };
            let Some(id) = inbound.id.clone() else {
                continue;
            };
            if let Err(err) = conn.respond(id, result).await {
                tracing::debug!("could not answer {}: {err}", inbound.method);
                break;
            }
        }
    });
}

/// Used by the transport when logging which agent a responder belongs to.
pub fn describe(agent: &AgentId) -> String {
    format!("responder[{agent}]")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(method: &str, params: serde_json::Value) -> Inbound {
        Inbound {
            method: method.to_owned(),
            params,
            id: Some(serde_json::json!(7)),
        }
    }

    /// A permission request must be answered by selecting an offered option; anything else
    /// leaves the agent's turn blocked forever.
    #[test]
    fn permission_request_selects_first_option() {
        let inbound = request(
            "session/request_permission",
            serde_json::json!({
                "options": [
                    { "optionId": "allow-once", "name": "Allow once" },
                    { "optionId": "reject", "name": "Reject" }
                ]
            }),
        );

        assert_eq!(
            answer(&inbound),
            Answer::Result(serde_json::json!({
                "outcome": { "outcome": "selected", "optionId": "allow-once" }
            }))
        );
    }

    /// With no options there is nothing to select, so cancel rather than invent an id.
    #[test]
    fn permission_request_without_options_cancels() {
        let inbound = request("session/request_permission", serde_json::json!({}));

        assert_eq!(
            answer(&inbound),
            Answer::Result(serde_json::json!({ "outcome": { "outcome": "cancelled" } }))
        );
    }

    /// A notification carries no id, so replying to it would be a protocol violation.
    #[test]
    fn notification_is_ignored() {
        let note = Inbound {
            method: "session/update".to_owned(),
            params: serde_json::json!({}),
            id: None,
        };

        assert_eq!(answer(&note), Answer::Ignore);
    }

    /// An unrecognised *request* must still get a reply, or the agent stalls.
    #[test]
    fn unknown_request_still_gets_a_reply() {
        let inbound = request("terminal/create", serde_json::json!({}));

        assert!(matches!(answer(&inbound), Answer::Result(_)));
    }

    #[test]
    fn read_text_file_returns_contents() {
        let dir = std::env::temp_dir().join(format!("mesh-acp-{}", uuid_like()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("hello.txt");
        std::fs::write(&path, "hi there").expect("write");

        let inbound = request(
            "fs/read_text_file",
            serde_json::json!({ "path": path.to_string_lossy() }),
        );

        assert_eq!(
            answer(&inbound),
            Answer::Result(serde_json::json!({ "content": "hi there" }))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A missing file must not panic or hang; the agent gets an empty read and moves on.
    #[test]
    fn read_missing_file_is_not_fatal() {
        let inbound = request(
            "fs/read_text_file",
            serde_json::json!({ "path": "/nonexistent/mesh/path.txt" }),
        );

        assert_eq!(
            answer(&inbound),
            Answer::Result(serde_json::json!({ "content": "" }))
        );
    }

    fn uuid_like() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    }
}
