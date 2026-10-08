//! `agent-mesh hook <event>`: delivery into Claude Code sessions that are not in tmux.
//!
//! Without tmux nothing can type into an idle TUI, so queued messages ride along on the next turn
//! boundary instead. `UserPromptSubmit` attaches them to whatever the user just typed; `Stop`
//! keeps Claude going when a peer wrote mid-turn, so a busy session picks replies up on its own.
//!
//! A hook must never break the session it runs in: every failure here is silent and exits 0.

use crate::client;
use crate::hub::{self, Message, Request};
use serde::Deserialize;
use tokio::io::AsyncReadExt;

#[derive(Debug, Default, Deserialize)]
struct HookInput {
    /// True when this Stop is already the result of a previous block. Blocking again would loop.
    #[serde(default)]
    stop_hook_active: bool,
}

pub async fn run(event: &str) {
    let mut raw = String::new();
    let _ = tokio::io::stdin().read_to_string(&mut raw).await;
    let input: HookInput = serde_json::from_str(&raw).unwrap_or_default();

    if event == "stop" && input.stop_hook_active {
        return;
    }

    // Exec-form hooks run as direct children of claude, the same process the MCP server
    // registered as its node, so the parent pid finds the right inbox.
    let pid = std::os::unix::process::parent_id();
    let Some(data) = client::call_if_running(&Request::Inbox {
        node: None,
        pid: Some(pid),
    })
    .await
    else {
        return;
    };
    let messages: Vec<Message> = serde_json::from_value(data).unwrap_or_default();
    if messages.is_empty() {
        return;
    }

    let body = messages
        .iter()
        .map(|m| hub::frame(m, "you"))
        .collect::<Vec<_>>()
        .join("\n\n");

    let output = match event {
        "user-prompt-submit" => serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "UserPromptSubmit",
                "additionalContext": body,
            }
        }),
        "stop" => serde_json::json!({ "decision": "block", "reason": body }),
        _ => return,
    };
    println!("{output}");
}
