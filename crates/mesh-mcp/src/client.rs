//! The hub as seen from one agent-mesh instance: connect (starting the hub if nobody has), and
//! keep this instance's node registered.

use crate::hub::{self, HubError, Node, PROTOCOL_VERSION, Request, Response};
use crate::tmux::NODE_ENV;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Who this instance is on the mesh. `None` for an instance under a headless `ask_agent` child,
/// which is plumbing rather than a session anyone should address.
#[derive(Debug, Clone)]
pub struct Identity {
    pub node: Option<Node>,
}

impl Identity {
    /// Work out which node this process belongs to. The MCP server's parent is the agent CLI
    /// itself (verified for claude), so the parent pid identifies the session.
    pub async fn detect() -> Self {
        if std::env::var_os(mesh_core::HEADLESS_ENV).is_some() {
            return Self { node: None };
        }

        let pid = std::os::unix::process::parent_id();
        let agent = parent_agent(pid).await;
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();

        // A node the hub spawned is already registered under this id, with its tmux session.
        let id = std::env::var(NODE_ENV).unwrap_or_else(|_| hub::mint_id(&agent));

        Self {
            node: Some(Node {
                id,
                agent,
                cwd,
                pid: Some(pid),
                tmux_pane: std::env::var("TMUX_PANE").ok(),
                tmux_session: None,
                // The hub fills these in for nodes it spawned; a self-registering session is the
                // user's own.
                spawned_by: None,
                started_at_unix: 0,
            }),
        }
    }
}

/// Map the parent's executable name onto a mesh agent name.
async fn parent_agent(pid: u32) -> String {
    let comm = tokio::process::Command::new("ps")
        .args(["-o", "comm=", "-p", &pid.to_string()])
        .output()
        .await
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default();
    let base = comm.rsplit('/').next().unwrap_or_default().to_lowercase();
    ["claude", "codex", "opencode", "gemini", "grok"]
        .into_iter()
        .find(|a| base.contains(a))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if base.is_empty() {
                "unknown".to_owned()
            } else {
                base
            }
        })
}

/// Send one request to the hub, starting the hub first if none is running.
pub async fn call(request: &Request) -> Result<serde_json::Value, HubError> {
    let stream = match UnixStream::connect(hub::socket_path()).await {
        Ok(s) => s,
        Err(_) => {
            start_hub()?;
            connect_with_retry().await?
        }
    };
    exchange(stream, request).await
}

/// Send one request only if a hub is already up. Hooks use this: a hook firing on every prompt
/// must never be the thing that starts a daemon.
pub async fn call_if_running(request: &Request) -> Option<serde_json::Value> {
    let stream = UnixStream::connect(hub::socket_path()).await.ok()?;
    exchange(stream, request).await.ok()
}

/// Like `call_if_running`, but reports why it failed. For `agent-mesh kill`, which must say so
/// when the hub refuses rather than silently doing nothing.
pub async fn call_existing(request: &Request) -> Result<serde_json::Value, HubError> {
    let stream = UnixStream::connect(hub::socket_path()).await.map_err(|_| {
        HubError::Refused("no agent-mesh hub is running, so there is nothing to stop".to_owned())
    })?;
    exchange(stream, request).await
}

async fn exchange(stream: UnixStream, request: &Request) -> Result<serde_json::Value, HubError> {
    let (read, mut write) = stream.into_split();
    // Serialize by reference: the envelope only borrows the request.
    let mut out = serde_json::to_vec(&serde_json::json!({
        "v": PROTOCOL_VERSION,
        "request": request,
    }))?;
    out.push(b'\n');
    write.write_all(&out).await?;

    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    match serde_json::from_str::<Response>(&line)? {
        Response::Ok { data } => Ok(data),
        Response::Error { message, internal: false } => Err(HubError::Refused(message)),
        Response::Error { message, internal: true } => Err(HubError::Internal(message)),
    }
}

/// Re-register before each operation. Registration is idempotent, and doing it every time means
/// a hub that restarted learns about this node again without any reconnect logic.
pub async fn call_as(
    identity: &Identity,
    request: &Request,
) -> Result<serde_json::Value, HubError> {
    register(identity).await?;
    call(request).await
}

/// Register this instance and return the id the hub knows it by, which can differ from the one
/// it asked for when the hub already tracks this agent process under another id.
pub async fn register(identity: &Identity) -> Result<Option<String>, HubError> {
    let Some(node) = &identity.node else {
        return Ok(None);
    };
    let reply = call(&Request::Register { node: node.clone() }).await?;
    Ok(Some(
        reply["id"]
            .as_str()
            .map_or_else(|| node.id.clone(), str::to_owned),
    ))
}

/// Launch `agent-mesh hub` detached: its own process group so it outlives this MCP client, and
/// none of our stdio, because our stdout is the MCP stream.
fn start_hub() -> Result<(), HubError> {
    use std::os::unix::process::CommandExt;

    let dir = hub::home_dir();
    std::fs::create_dir_all(&dir)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("hub.log"))?;

    // Started from home so the hub's config (it is hub-wide now) never depends on which client's
    // working directory happened to launch it.
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    let mut command = std::process::Command::new(std::env::current_exe()?);
    if let Some(home) = home {
        command.current_dir(home);
    }
    command
        .arg("hub")
        .env("PATH", hub_path())
        .env_remove(mesh_core::HEADLESS_ENV)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .process_group(0)
        .spawn()?;
    Ok(())
}

/// The hub outlives whichever client started it and serves every agent, so it must not inherit
/// one client's idea of PATH: codex, for one, rewrites PATH for its MCP servers. Keep the
/// caller's entries and make sure the usual install locations for tmux and the agent CLIs follow.
fn hub_path() -> std::ffi::OsString {
    let mut dirs: Vec<std::path::PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    let mut wanted: Vec<std::path::PathBuf> = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ]
    .iter()
    .map(std::path::PathBuf::from)
    .collect();
    if let Some(home) = home {
        for rel in [".local/bin", ".cargo/bin", ".claude/local", ".opencode/bin", ".grok/bin"] {
            wanted.push(home.join(rel));
        }
    }
    for dir in wanted {
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    std::env::join_paths(dirs).unwrap_or_default()
}

async fn connect_with_retry() -> Result<UnixStream, HubError> {
    let mut last = None;
    for _ in 0..40 {
        match UnixStream::connect(hub::socket_path()).await {
            Ok(s) => return Ok(s),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last
        .unwrap_or_else(|| std::io::Error::other("hub did not start"))
        .into())
}
