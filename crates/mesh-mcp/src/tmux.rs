//! Spawning agents into tmux and typing into their panes.
//!
//! tmux is what makes a spawned node both drivable and watchable: the hub pastes messages into
//! the pane, and the user can `tmux attach` to see the agent work in its real TUI.

use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Set on everything the mesh launches. An agent-mesh started under it is part of a node the hub
/// already knows (or a headless `ask_agent` child) and must not register a second one.
pub const NODE_ENV: &str = "AGENT_MESH_NODE";

/// Claude Code drops an Enter that lands in the same instant as a paste; this gap was enough in
/// manual testing against both claude and codex.
const SUBMIT_DELAY: Duration = Duration::from_millis(400);

#[derive(Debug, thiserror::Error)]
pub enum TmuxError {
    #[error("could not run tmux (is it installed?): {0}")]
    Spawn(#[from] std::io::Error),
    #[error("tmux {command} failed: {stderr}")]
    Failed { command: String, stderr: String },
    #[error("tmux returned unexpected output: {0}")]
    Parse(String),
}

pub struct Launch<'a> {
    pub agent: &'a str,
    pub program: &'a str,
    pub cwd: &'a Path,
    pub node_id: &'a str,
    pub prompt: Option<&'a str>,
}

pub struct Spawned {
    pub session: String,
    pub pane: String,
    pub pid: u32,
}

/// The interactive command line for an agent. The initial prompt goes in as an argument rather
/// than being pasted, because a first run in a new directory can open a trust or settings dialog
/// that a paste would answer.
///
/// Permissions are bypassed to match what the mesh's headless transports already do: there is
/// nobody watching a spawned pane by default to approve tool calls.
pub fn command_line(agent: &str, program: &str, prompt: Option<&str>) -> Vec<String> {
    let mut argv = vec![program.to_owned()];
    let flags: &[&str] = match agent {
        "claude" => &["--dangerously-skip-permissions"],
        // `--no-daemon` keeps codex off the shared app-server, whose feature-mismatch dialog
        // otherwise blocks startup.
        "codex" => &["--no-daemon", "--dangerously-bypass-approvals-and-sandbox"],
        "gemini" => &["--yolo"],
        "grok" => &["--always-approve"],
        _ => &[],
    };
    argv.extend(flags.iter().map(|f| (*f).to_owned()));

    if let Some(prompt) = prompt {
        match agent {
            "opencode" => argv.extend(["--prompt".to_owned(), prompt.to_owned()]),
            // gemini's `-p` is one-shot and exits; its interactive equivalent is `-i`.
            "gemini" => argv.extend(["-i".to_owned(), prompt.to_owned()]),
            _ => argv.push(prompt.to_owned()),
        }
    }
    argv
}

pub async fn spawn(launch: &Launch<'_>) -> Result<Spawned, TmuxError> {
    let session = format!("mesh-{}", launch.node_id);
    let argv = command_line(launch.agent, launch.program, launch.prompt);

    let mut args: Vec<String> = [
        "new-session",
        "-d",
        "-s",
        &session,
        "-x",
        "200",
        "-y",
        "50",
        "-c",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    args.push(launch.cwd.display().to_string());
    args.extend(["-e".to_owned(), format!("{NODE_ENV}={}", launch.node_id)]);
    // A pane inherits the tmux server's environment, not ours. Carry over what decides which hub
    // and which agent-mesh binary the new node's MCP server will find.
    for key in ["AGENT_MESH_HOME", "PATH"] {
        if let Ok(value) = std::env::var(key) {
            args.extend(["-e".to_owned(), format!("{key}={value}")]);
        }
    }
    args.extend([
        "-P".to_owned(),
        "-F".to_owned(),
        "#{pane_id} #{pane_pid}".to_owned(),
    ]);
    // tmux runs a lone trailing argument through the shell but execs multiple ones directly, which
    // is what we want: the prompt must not be shell-interpreted.
    args.extend(argv);

    let out = run(&args).await?;
    let mut parts = out.split_whitespace();
    let (Some(pane), Some(pid)) = (parts.next(), parts.next()) else {
        return Err(TmuxError::Parse(out));
    };
    let pid = pid.parse().map_err(|_| TmuxError::Parse(out.clone()))?;

    Ok(Spawned {
        session,
        pane: pane.to_owned(),
        pid,
    })
}

/// Type `text` into `pane` and submit it. Bracketed paste keeps embedded newlines from submitting
/// early and stops tmux reading words like `Enter` as key names.
pub async fn paste(pane: &str, text: &str) -> Result<(), TmuxError> {
    let buffer = format!("agent-mesh-{}", uuid::Uuid::new_v4().simple());

    let mut child = Command::new("tmux")
        .args(["load-buffer", "-b", &buffer, "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(text.as_bytes()).await?;
    }
    let loaded = child.wait_with_output().await?;
    if !loaded.status.success() {
        return Err(TmuxError::Failed {
            command: "load-buffer".to_owned(),
            stderr: String::from_utf8_lossy(&loaded.stderr).trim().to_owned(),
        });
    }

    run(&[
        "paste-buffer".to_owned(),
        "-p".to_owned(),
        "-d".to_owned(),
        "-b".to_owned(),
        buffer,
        "-t".to_owned(),
        pane.to_owned(),
    ])
    .await?;
    tokio::time::sleep(SUBMIT_DELAY).await;
    run(&[
        "send-keys".to_owned(),
        "-t".to_owned(),
        pane.to_owned(),
        "Enter".to_owned(),
    ])
    .await?;
    Ok(())
}

/// The visible contents of a pane, for `peek_node`.
pub async fn capture(pane: &str, lines: usize) -> Result<String, TmuxError> {
    let start = format!("-{lines}");
    run(&[
        "capture-pane".to_owned(),
        "-p".to_owned(),
        "-t".to_owned(),
        pane.to_owned(),
        "-S".to_owned(),
        start,
    ])
    .await
}

async fn run(args: &[String]) -> Result<String, TmuxError> {
    let out = Command::new("tmux").args(args).output().await?;
    if !out.status.success() {
        return Err(TmuxError::Failed {
            command: args.first().cloned().unwrap_or_default(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim_end().to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn prompt_is_one_argv_element_never_split() {
        let argv = command_line("claude", "claude", Some("two words; rm -rf /"));
        assert_eq!(argv.last().unwrap(), "two words; rm -rf /");
    }

    #[test]
    fn codex_avoids_the_daemon_dialog() {
        let argv = command_line("codex", "codex", None);
        assert!(argv.contains(&"--no-daemon".to_owned()));
    }

    #[test]
    fn agents_without_a_positional_prompt_get_their_flag() {
        assert_eq!(
            command_line("opencode", "opencode", Some("hi")),
            vec!["opencode", "--prompt", "hi"]
        );
        let gemini = command_line("gemini", "gemini", Some("hi"));
        assert_eq!(&gemini[gemini.len() - 2..], ["-i", "hi"]);
    }
}
