//! The hub: one long-lived process every agent-mesh instance on the machine reports to.
//!
//! Each MCP client launches its own agent-mesh over stdio, so without a hub those processes are
//! strangers to each other. The hub owns the shared view: which agent sessions are alive (nodes),
//! a mailbox per node, and the tmux sessions it spawned. It speaks newline-delimited JSON over a
//! unix socket in a 0700 directory, one request per connection.
//!
//! Delivery is push where it can be and pull where it cannot. A node running in a tmux pane gets
//! its message pasted straight into the TUI; anything else waits in the node's inbox until the
//! agent calls `check_inbox` or a Claude hook drains it.

use crate::config::Config;
use crate::tmux;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Bumped on any incompatible change to `Request` or `Response`, so an old client talking to a
/// new hub fails with a clear message instead of a decode error.
pub const PROTOCOL_VERSION: u32 = 1;

/// How many messages one node may send another per `RATE_WINDOW`. Two agents replying to each
/// other's replies would otherwise ping-pong for as long as the hop limit allows, burning tokens
/// on both sides.
const RATE_LIMIT: usize = 6;
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// A live agent session on this machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct Node {
    /// Stable name other agents address this node by.
    pub id: String,
    pub agent: String,
    pub cwd: String,
    /// The agent CLI's process id; the node is dropped when it exits.
    #[serde(default)]
    pub pid: Option<u32>,
    /// tmux pane the agent runs in, when it runs in one. Messages to it are pasted in directly.
    #[serde(default)]
    pub tmux_pane: Option<String>,
    /// tmux session name, set for nodes the hub spawned. `tmux attach -t <this>` to watch.
    #[serde(default)]
    pub tmux_session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Message {
    pub from: String,
    pub text: String,
    /// How many agent-to-agent hops led to this message. Replies carry `hops + 1`.
    pub hops: u32,
    pub sent_at_unix: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Insert or refresh a node. Idempotent, so clients re-send it before every operation and a
    /// restarted hub repopulates itself.
    Register {
        node: Node,
    },
    List,
    Send {
        from: String,
        to: String,
        text: String,
        hops: u32,
    },
    /// Drain undelivered messages, addressed by node id or by the agent's pid (hooks only know the
    /// pid of the CLI that ran them).
    Inbox {
        #[serde(default)]
        node: Option<String>,
        #[serde(default)]
        pid: Option<u32>,
    },
    Spawn {
        agent: String,
        cwd: String,
        #[serde(default)]
        prompt: Option<String>,
        #[serde(default)]
        from: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u32,
    pub request: Request,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok {
        #[serde(default)]
        data: serde_json::Value,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("hub i/o failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("hub sent something unreadable: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("{0}")]
    Refused(String),
}

/// `~/.agent-mesh`, or `$AGENT_MESH_HOME`. Tests and side-by-side installs point it elsewhere.
pub fn home_dir() -> PathBuf {
    if let Some(explicit) = std::env::var_os("AGENT_MESH_HOME") {
        return PathBuf::from(explicit);
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(".agent-mesh")
}

pub fn socket_path() -> PathBuf {
    home_dir().join("hub.sock")
}

/// Anything that can reach this socket can type into a peer agent that may be running with
/// permissions bypassed, so the directory is private to the user.
fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}

/// Run the hub until killed. Returns early, successfully, if another hub already owns the socket.
pub async fn serve(config: Arc<Config>) -> Result<(), HubError> {
    let path = socket_path();
    if let Some(dir) = path.parent() {
        ensure_private_dir(dir)?;
    }

    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
            if UnixStream::connect(&path).await.is_ok() {
                tracing::info!("a hub is already running; exiting");
                return Ok(());
            }
            // A socket file nobody answers on is left over from a hub that died.
            std::fs::remove_file(&path)?;
            UnixListener::bind(&path)?
        }
        Err(err) => return Err(err.into()),
    };
    tracing::info!(socket = %path.display(), "hub listening");

    let hub = Arc::new(Hub::new(config));
    loop {
        let (stream, _) = listener.accept().await?;
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            if let Err(err) = handle(stream, &hub).await {
                tracing::warn!(%err, "hub connection failed");
            }
        });
    }
}

async fn handle(stream: UnixStream, hub: &Hub) -> Result<(), HubError> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;

    let response = match serde_json::from_str::<Envelope>(&line) {
        Ok(env) if env.v != PROTOCOL_VERSION => Response::Error {
            message: format!(
                "protocol mismatch: client speaks v{}, hub speaks v{PROTOCOL_VERSION}; restart \
                 the older one (kill the `agent-mesh hub` process to restart the hub)",
                env.v
            ),
        },
        Ok(env) => match hub.dispatch(env.request).await {
            Ok(data) => Response::Ok { data },
            Err(message) => Response::Error { message },
        },
        Err(err) => Response::Error {
            message: format!("bad request: {err}"),
        },
    };

    let mut out = serde_json::to_vec(&response)?;
    out.push(b'\n');
    write.write_all(&out).await?;
    Ok(())
}

#[derive(Default)]
struct State {
    nodes: BTreeMap<String, Node>,
    inboxes: BTreeMap<String, VecDeque<Message>>,
    /// Recent send times per (from, to), for the rate limit.
    recent: BTreeMap<(String, String), VecDeque<Instant>>,
}

struct Hub {
    config: Arc<Config>,
    state: Mutex<State>,
}

impl Hub {
    fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            state: Mutex::new(State::default()),
        }
    }

    /// The lock is only ever held for map access, never across an await.
    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    async fn dispatch(&self, request: Request) -> Result<serde_json::Value, String> {
        match request {
            Request::Register { node } => {
                let id = self.with(|s| register(s, node));
                Ok(serde_json::json!({ "id": id }))
            }
            Request::List => {
                self.prune().await;
                let nodes: Vec<Node> = self.with(|s| s.nodes.values().cloned().collect());
                to_value(&nodes)
            }
            Request::Send {
                from,
                to,
                text,
                hops,
            } => self.send(from, to, text, hops).await,
            Request::Inbox { node, pid } => {
                let messages = self.with(|s| {
                    let id = node.or_else(|| {
                        let pid = pid?;
                        s.nodes
                            .values()
                            .find(|n| n.pid == Some(pid))
                            .map(|n| n.id.clone())
                    })?;
                    Some(s.inboxes.get_mut(&id)?.drain(..).collect::<Vec<_>>())
                });
                to_value(&messages.unwrap_or_default())
            }
            Request::Spawn {
                agent,
                cwd,
                prompt,
                from,
            } => self.spawn(agent, cwd, prompt, from).await,
        }
    }

    async fn send(
        &self,
        from: String,
        to: String,
        text: String,
        hops: u32,
    ) -> Result<serde_json::Value, String> {
        let limit = u32::try_from(self.config.max_ask_depth).unwrap_or(u32::MAX);
        if hops > limit {
            return Err(format!(
                "refusing to relay: this message is {hops} hops deep, past max_ask_depth \
                 ({limit}); agents replying to replies would otherwise loop"
            ));
        }
        if from == to {
            return Err("a node cannot message itself".to_owned());
        }

        self.prune().await;
        let target = self
            .with(|s| s.nodes.get(&to).cloned())
            .ok_or_else(|| format!("no live node `{to}`; call list_nodes to see who is online"))?;

        self.with(|s| {
            let window = s.recent.entry((from.clone(), to.clone())).or_default();
            let now = Instant::now();
            while window
                .front()
                .is_some_and(|t| now.duration_since(*t) > RATE_WINDOW)
            {
                window.pop_front();
            }
            if window.len() >= RATE_LIMIT {
                return Err(format!(
                    "rate limited: `{from}` already sent `{to}` {RATE_LIMIT} messages in the last \
                     {}s",
                    RATE_WINDOW.as_secs()
                ));
            }
            window.push_back(now);
            Ok(())
        })?;

        let message = Message {
            from,
            text,
            hops,
            sent_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };

        if let Some(pane) = target.tmux_pane.as_deref() {
            match tmux::paste(pane, &frame(&message, &to)).await {
                Ok(()) => return Ok(serde_json::json!({ "delivery": "pushed", "to": to })),
                // The pane went away or tmux is unhappy; fall back to the inbox rather than lose
                // the message.
                Err(err) => tracing::warn!(%err, pane, "tmux delivery failed; queueing"),
            }
        }

        self.with(|s| s.inboxes.entry(to.clone()).or_default().push_back(message));
        Ok(serde_json::json!({ "delivery": "queued", "to": to }))
    }

    async fn spawn(
        &self,
        agent: String,
        cwd: String,
        prompt: Option<String>,
        from: Option<String>,
    ) -> Result<serde_json::Value, String> {
        let program = self
            .config
            .agents
            .get(&agent)
            .filter(|c| c.enabled())
            .map(|c| c.command().to_owned())
            .ok_or_else(|| {
                format!(
                    "unknown agent `{agent}`; available: {}",
                    self.config
                        .agent_ids()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;
        let cwd = mesh_core::absolute_cwd(Path::new(&cwd))
            .map_err(|e| format!("`{cwd}` is not a usable working directory: {e}"))?;

        let id = mint_id(&agent);
        // A prompt from a peer node is framed like any other message so the spawned agent knows
        // who asked and how to answer. One with no sender came from a human and goes in as-is.
        let initial = prompt.map(|text| match from {
            Some(from) => frame(
                &Message {
                    from,
                    text,
                    hops: 0,
                    sent_at_unix: 0,
                },
                &id,
            ),
            None => text,
        });

        let launch = tmux::Launch {
            agent: &agent,
            program: &program,
            cwd: &cwd,
            node_id: &id,
            prompt: initial.as_deref(),
        };
        let spawned = tmux::spawn(&launch).await.map_err(|e| e.to_string())?;

        let node = Node {
            id: id.clone(),
            agent,
            cwd: cwd.display().to_string(),
            pid: Some(spawned.pid),
            tmux_pane: Some(spawned.pane.clone()),
            tmux_session: Some(spawned.session.clone()),
        };
        self.with(|s| {
            s.inboxes.entry(id.clone()).or_default();
            s.nodes.insert(id.clone(), node.clone());
        });

        to_value(&node)
    }

    /// Drop nodes whose agent has exited, so `list_nodes` never offers a dead peer.
    async fn prune(&self) {
        let nodes: Vec<Node> = self.with(|s| s.nodes.values().cloned().collect());
        let mut dead = Vec::new();
        for node in nodes {
            if let Some(pid) = node.pid
                && !pid_alive(pid).await
            {
                dead.push(node.id);
            }
        }
        if !dead.is_empty() {
            self.with(|s| {
                for id in &dead {
                    s.nodes.remove(id);
                    s.inboxes.remove(id);
                }
            });
        }
    }
}

/// Insert or refresh a node and return the id it is known by.
///
/// Some CLIs (codex) strip the environment of the MCP servers they launch, so a spawned node's
/// own server cannot see `AGENT_MESH_NODE` and registers under a fresh id. The agent pid is the
/// ground truth: a registration for a pid the hub already has keeps the existing id, and the
/// tmux details recorded at spawn time.
fn register(state: &mut State, mut node: Node) -> String {
    let existing = state
        .nodes
        .values()
        .find(|n| n.id == node.id || (node.pid.is_some() && n.pid == node.pid))
        .cloned();
    if let Some(known) = existing {
        node.id = known.id;
        node.tmux_session = node.tmux_session.or(known.tmux_session);
        node.tmux_pane = node.tmux_pane.or(known.tmux_pane);
    }
    let id = node.id.clone();
    state.inboxes.entry(id.clone()).or_default();
    state.nodes.insert(id.clone(), node);
    id
}

fn to_value<T: Serialize>(value: &T) -> Result<serde_json::Value, String> {
    serde_json::to_value(value).map_err(|e| e.to_string())
}

/// `claude-3f9a1c2e`: readable enough for an agent to type, unique enough not to collide.
pub fn mint_id(agent: &str) -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    format!("{agent}-{}", &uuid[..8])
}

/// How a message appears inside the receiving agent. The header makes it unmistakable that this
/// came from a peer agent, not from the user at the keyboard, and tells the agent how to answer.
pub fn frame(message: &Message, to: &str) -> String {
    format!(
        "[agent-mesh message from `{from}` to you (`{to}`), hops={hops}. This is a peer agent, not \
         your user. To reply, call the agent-mesh `send_message` tool with to=\"{from}\" and \
         hops={next}.]\n{text}",
        from = message.from,
        hops = message.hops,
        next = message.hops.saturating_add(1),
        text = message.text,
    )
}

/// `kill -0` without `unsafe`: the shell utility does the syscall for us. macOS has no `/proc`.
async fn pid_alive(pid: u32) -> bool {
    tokio::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn hub() -> Hub {
        Hub::new(Arc::new(Config::default_agents()))
    }

    fn node(id: &str) -> Node {
        Node {
            id: id.to_owned(),
            agent: "claude".to_owned(),
            cwd: "/tmp".to_owned(),
            // Our own pid: alive for the duration of the test, so pruning keeps it.
            pid: Some(std::process::id()),
            tmux_pane: None,
            tmux_session: None,
        }
    }

    /// Registers with no pid, so test nodes are told apart by id alone.
    async fn register(hub: &Hub, id: &str) {
        hub.dispatch(Request::Register {
            node: Node {
                pid: None,
                ..node(id)
            },
        })
        .await
        .unwrap();
    }

    fn send(from: &str, to: &str, hops: u32) -> Request {
        Request::Send {
            from: from.to_owned(),
            to: to.to_owned(),
            text: "hi".to_owned(),
            hops,
        }
    }

    #[tokio::test]
    async fn a_message_to_a_paneless_node_waits_in_its_inbox_until_drained() {
        let hub = hub();
        register(&hub, "a").await;
        register(&hub, "b").await;

        let sent = hub.dispatch(send("a", "b", 0)).await.unwrap();
        assert_eq!(sent["delivery"], "queued");

        let drained = hub
            .dispatch(Request::Inbox {
                node: Some("b".to_owned()),
                pid: None,
            })
            .await
            .unwrap();
        assert_eq!(drained.as_array().unwrap().len(), 1);

        // Draining is destructive, so a hook cannot deliver the same message twice.
        let again = hub
            .dispatch(Request::Inbox {
                node: Some("b".to_owned()),
                pid: None,
            })
            .await
            .unwrap();
        assert!(again.as_array().unwrap().is_empty());
    }

    /// Hooks know only the pid of the CLI that ran them.
    #[tokio::test]
    async fn inbox_can_be_addressed_by_pid() {
        let hub = hub();
        register(&hub, "a").await;
        hub.dispatch(Request::Register {
            node: Node {
                // Our parent (cargo's test runner): alive, and distinct from `a`'s pid.
                pid: Some(std::os::unix::process::parent_id()),
                ..node("b")
            },
        })
        .await
        .unwrap();
        hub.dispatch(send("a", "b", 0)).await.unwrap();

        let drained = hub
            .dispatch(Request::Inbox {
                node: None,
                pid: Some(std::os::unix::process::parent_id()),
            })
            .await
            .unwrap();
        assert_eq!(drained.as_array().unwrap().len(), 1);
    }

    /// codex strips the env that carries a spawned node's id; the pid must reunite them.
    #[tokio::test]
    async fn re_registering_a_known_pid_keeps_the_spawned_id_and_pane() {
        let hub = hub();
        hub.dispatch(Request::Register {
            node: Node {
                tmux_pane: Some("%7".to_owned()),
                ..node("codex-spawned")
            },
        })
        .await
        .unwrap();

        let reply = hub
            .dispatch(Request::Register {
                node: node("codex-random"),
            })
            .await
            .unwrap();
        assert_eq!(reply["id"], "codex-spawned");

        let listed: Vec<Node> =
            serde_json::from_value(hub.dispatch(Request::List).await.unwrap()).unwrap();
        assert_eq!(listed.len(), 1, "one process must be one node");
        assert_eq!(listed[0].tmux_pane.as_deref(), Some("%7"));
    }

    #[tokio::test]
    async fn too_many_hops_is_refused() {
        let hub = hub();
        register(&hub, "a").await;
        register(&hub, "b").await;

        let err = hub.dispatch(send("a", "b", 99)).await.unwrap_err();
        assert!(err.contains("max_ask_depth"), "got: {err}");
    }

    #[tokio::test]
    async fn a_chatty_pair_is_rate_limited() {
        let hub = hub();
        register(&hub, "a").await;
        register(&hub, "b").await;

        for _ in 0..RATE_LIMIT {
            hub.dispatch(send("a", "b", 0)).await.unwrap();
        }
        let err = hub.dispatch(send("a", "b", 0)).await.unwrap_err();
        assert!(err.contains("rate limited"), "got: {err}");
        // The limit is per pair: b can still answer a.
        hub.dispatch(send("b", "a", 1)).await.unwrap();
    }

    #[tokio::test]
    async fn sending_to_an_unknown_node_names_the_fix() {
        let hub = hub();
        register(&hub, "a").await;
        let err = hub.dispatch(send("a", "ghost", 0)).await.unwrap_err();
        assert!(err.contains("list_nodes"), "got: {err}");
    }

    #[tokio::test]
    async fn nodes_whose_process_exited_are_pruned() {
        let hub = hub();
        hub.dispatch(Request::Register {
            node: Node {
                // Far above any real pid on macOS or Linux.
                pid: Some(99_999_999),
                ..node("gone")
            },
        })
        .await
        .unwrap();

        let listed = hub.dispatch(Request::List).await.unwrap();
        assert!(listed.as_array().unwrap().is_empty());
    }

    #[test]
    fn frame_tells_the_receiver_who_sent_it_and_how_to_reply() {
        let framed = frame(
            &Message {
                from: "codex-1".to_owned(),
                text: "body".to_owned(),
                hops: 2,
                sent_at_unix: 0,
            },
            "claude-2",
        );
        assert!(framed.contains("not your user"));
        assert!(framed.contains("to=\"codex-1\""));
        assert!(framed.contains("hops=3"));
        assert!(framed.ends_with("\nbody"));
    }

    #[test]
    fn protocol_round_trips() {
        let env = Envelope {
            v: PROTOCOL_VERSION,
            request: send("a", "b", 0),
        };
        let raw = serde_json::to_string(&env).unwrap();
        let back: Envelope = serde_json::from_str(&raw).unwrap();
        assert!(matches!(back.request, Request::Send { hops: 0, .. }));
    }
}
