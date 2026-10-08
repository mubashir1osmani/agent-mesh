//! The hub: one long-lived process per user that owns every agent process the mesh runs.
//!
//! Each MCP client launches its own agent-mesh over stdio. Those instances are thin: they forward
//! to the hub, which holds the session registry, the transports (and so every headless agent
//! process), the tmux nodes it spawned, and a mailbox per node. Owning all of it in one place is
//! what lets the hub cap how many agent processes run, reap idle ones, and give the user a single
//! `agent-mesh ps` / `agent-mesh kill` over everything. It speaks newline-delimited JSON over a
//! unix socket in a 0700 directory, one request per connection.
//!
//! Delivery is push where it can be and pull where it cannot. A node running in a tmux pane gets
//! its message pasted straight into the TUI; anything else waits in the node's inbox until the
//! agent calls `check_inbox` or a Claude hook drains it.

use crate::config::Config;
use crate::mesh::{Mesh, MeshError};
use crate::tmux;
use mesh_core::{AgentId, AskChain, SessionRef, VendorSessionId};
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
pub const PROTOCOL_VERSION: u32 = 2;

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
    /// Node that asked the hub to spawn this one, or `user`. Absent for sessions the user started
    /// themselves, which the hub lists but never counts against the cap or kills.
    #[serde(default)]
    pub spawned_by: Option<String>,
    #[serde(default)]
    pub started_at_unix: u64,
}

impl Node {
    /// Spawned by the hub, so counted against the cap and closable by `kill`.
    pub fn is_managed(&self) -> bool {
        self.spawned_by.is_some()
    }
}

/// One process the hub is responsible for, as shown by `agent-mesh ps`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProcessInfo {
    /// Node id for a tmux node, or `<agent>/headless` for a background `ask_agent` process.
    pub id: String,
    pub agent: String,
    /// `tmux` or `headless`.
    pub kind: String,
    pub pid: u32,
    #[serde(default)]
    pub spawned_by: Option<String>,
    /// Seconds since it started, when known.
    #[serde(default)]
    pub age_seconds: Option<u64>,
    /// Seconds since a message was last delivered to it, for tmux nodes.
    #[serde(default)]
    pub idle_seconds: Option<u64>,
    /// Resident memory of the process and all its children, in MiB.
    pub rss_mib: u64,
    #[serde(default)]
    pub tmux_session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProcessReport {
    pub max_processes: usize,
    pub processes: Vec<ProcessInfo>,
    /// Sessions the user started themselves. Listed for visibility; never capped or killed.
    pub unmanaged_nodes: Vec<Node>,
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
    /// Everything the hub is running, with memory, for `agent-mesh ps`.
    Ps,
    /// Stop one hub-owned process (a node id, or `<agent>/headless`), or all of them.
    Kill {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        all: bool,
    },
    // Session tools, forwarded from each MCP instance so every process lives in the hub.
    OpenSession {
        agent: String,
        cwd: String,
    },
    AttachSession {
        agent: String,
        session_id: String,
        cwd: String,
    },
    Ask {
        session: String,
        prompt: String,
        #[serde(default)]
        via: Vec<String>,
    },
    ReadSession {
        session: String,
    },
    ListSessions {
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        discover_in: Option<String>,
    },
    Usage,
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
        /// The agent or transport failed, rather than the request being wrong.
        #[serde(default)]
        internal: bool,
    },
}

/// Why the hub could not do what was asked.
#[derive(Debug)]
pub struct Failure {
    pub message: String,
    pub internal: bool,
}

impl From<String> for Failure {
    /// Hub-level refusals (unknown node, loop guard, cap) are the caller's to fix.
    fn from(message: String) -> Self {
        Self {
            message,
            internal: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("hub i/o failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("hub sent something unreadable: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("{0}")]
    Refused(String),
    #[error("{0}")]
    Internal(String),
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

    let hub = Hub::new(config);
    hub.recover().await;

    // Reap idle spawned nodes and headless processes on a timer.
    let reaper = Arc::clone(&hub);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            reaper.reap_idle().await;
        }
    });

    // On SIGTERM/SIGINT stop the headless children explicitly. tmux nodes are left running: they
    // are recorded on disk and re-adopted by the next hub.
    let stopper = Arc::clone(&hub);
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("hub stopping; shutting down headless agents");
        stopper.mesh.shutdown_all().await;
        let _ = std::fs::remove_file(socket_path());
        std::process::exit(0);
    });

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
            internal: false,
            message: format!(
                "protocol mismatch: client speaks v{}, hub speaks v{PROTOCOL_VERSION}; restart \
                 the older one (kill the `agent-mesh hub` process to restart the hub)",
                env.v
            ),
        },
        Ok(env) => match hub.dispatch(env.request).await {
            Ok(data) => Response::Ok { data },
            Err(failure) => Response::Error {
                message: failure.message,
                internal: failure.internal,
            },
        },
        Err(err) => Response::Error {
            message: format!("bad request: {err}"),
            internal: false,
        },
    };

    let mut out = serde_json::to_vec(&response)?;
    out.push(b'\n');
    write.write_all(&out).await?;
    Ok(())
}

async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        // Without signal handlers, never return: the hub keeps running and the kernel still
        // closes headless children's pipes when it dies.
        std::future::pending::<()>().await;
        return;
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[derive(Default)]
struct State {
    nodes: BTreeMap<String, Node>,
    inboxes: BTreeMap<String, VecDeque<Message>>,
    /// Recent send times per (from, to), for the rate limit.
    recent: BTreeMap<(String, String), VecDeque<Instant>>,
    /// Last time each managed node was given work, for idle reaping.
    last_active: BTreeMap<String, Instant>,
    /// Last time each agent's headless process handled an ask.
    headless_active: BTreeMap<String, Instant>,
}

struct Hub {
    config: Arc<Config>,
    mesh: Mesh,
    state: Mutex<State>,
    /// Held across "count, then spawn" so concurrent spawns cannot both pass the cap.
    spawn_gate: tokio::sync::Mutex<()>,
}

impl Hub {
    fn new(config: Arc<Config>) -> Arc<Self> {
        Arc::new_cyclic(|weak: &std::sync::Weak<Hub>| {
            let weak = weak.clone();
            let admission: crate::mesh::Admission = Arc::new(move || {
                let weak = weak.clone();
                Box::pin(async move {
                    let hub = weak.upgrade()?;
                    hub.capacity_refusal().await
                })
            });
            Self {
                mesh: Mesh::from_config(&config).with_admission(admission),
                config,
                state: Mutex::new(State::default()),
                spawn_gate: tokio::sync::Mutex::new(()),
            }
        })
    }

    /// How many agent processes the hub owns right now: spawned tmux nodes plus headless
    /// children. The user's own sessions are never counted.
    async fn managed_count(&self) -> usize {
        self.prune().await;
        let tmux = self.with(|s| s.nodes.values().filter(|n| n.is_managed()).count());
        tmux + self.mesh.processes().await.len()
    }

    /// `None` when another process may start; otherwise a refusal naming what is running so the
    /// caller can choose what to close.
    async fn capacity_refusal(&self) -> Option<String> {
        let max = self.config.max_processes;
        if self.managed_count().await < max {
            return None;
        }
        let running = self
            .ps()
            .await
            .processes
            .iter()
            .map(|p| {
                format!(
                    "{} ({}, {} MiB{})",
                    p.id,
                    p.kind,
                    p.rss_mib,
                    p.idle_seconds
                        .map(|s| format!(", idle {}m", s / 60))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!(
            "the mesh is already running {max} agent processes, its max_processes limit. \
             Running: {running}. Close one with kill_node (or ask the user to run \
             `agent-mesh kill <id>`), or raise max_processes in agents.toml."
        ))
    }

    /// The lock is only ever held for map access, never across an await.
    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    async fn dispatch(&self, request: Request) -> Result<serde_json::Value, Failure> {
        match request {
            Request::Register { node } => {
                let id = self.with(|s| register(s, node));
                Ok(serde_json::json!({ "id": id }))
            }
            Request::List => {
                self.prune().await;
                let nodes: Vec<Node> = self.with(|s| s.nodes.values().cloned().collect());
                Ok(to_value(&nodes)?)
            }
            Request::Send {
                from,
                to,
                text,
                hops,
            } => Ok(self.send(from, to, text, hops).await?),
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
                Ok(to_value(&messages.unwrap_or_default())?)
            }
            Request::Spawn {
                agent,
                cwd,
                prompt,
                from,
            } => Ok(self.spawn(agent, cwd, prompt, from).await?),
            Request::Ps => Ok(to_value(&self.ps().await)?),
            Request::Kill { target, all } => Ok(self.kill(target, all).await?),
            Request::OpenSession { agent, cwd } => {
                let session = self
                    .mesh
                    .open_session(&AgentId::new(agent.as_str()), Path::new(&cwd))
                    .map_err(mesh_err)?;
                Ok(serde_json::json!({ "session": session.to_string(), "agent": agent }))
            }
            Request::AttachSession {
                agent,
                session_id,
                cwd,
            } => {
                let (session, transcript) = self
                    .mesh
                    .attach_session(
                        &AgentId::new(agent.as_str()),
                        &VendorSessionId::new(session_id.as_str()),
                        Path::new(&cwd),
                    )
                    .await
                    .map_err(mesh_err)?;
                self.touch_headless(&agent);
                Ok(serde_json::json!({
                    "session": session.to_string(),
                    "agent": agent,
                    "transcript": transcript,
                }))
            }
            Request::Ask {
                session,
                prompt,
                via,
            } => {
                let session = SessionRef::parse(session.as_str());
                let chain =
                    AskChain::from_hops(via.iter().map(|h| SessionRef::parse(h.as_str())));
                let (reply, next) = self
                    .mesh
                    .ask(&session, &prompt, &chain)
                    .await
                    .map_err(mesh_err)?;
                let agent = self
                    .mesh
                    .sessions(None)
                    .into_iter()
                    .find(|e| e.session == session)
                    .map(|e| e.agent.to_string())
                    .unwrap_or_default();
                self.touch_headless(&agent);
                Ok(serde_json::json!({
                    "reply": reply,
                    "agent": agent,
                    "via": next.hops().iter().map(SessionRef::to_string).collect::<Vec<_>>(),
                }))
            }
            Request::ReadSession { session } => {
                let session = SessionRef::parse(session.as_str());
                let transcript = self.mesh.read_session(&session).await.map_err(mesh_err)?;
                let agent = self
                    .mesh
                    .sessions(None)
                    .into_iter()
                    .find(|e| e.session == session)
                    .map(|e| e.agent.to_string())
                    .unwrap_or_default();
                self.touch_headless(&agent);
                Ok(serde_json::json!({ "agent": agent, "transcript": transcript }))
            }
            Request::ListSessions { agent, discover_in } => {
                let filter = agent.as_deref().map(AgentId::new);
                let known = self.mesh.sessions(filter.as_ref());
                let discovered = match (discover_in.as_deref(), filter.as_ref()) {
                    (Some(cwd), Some(agent)) => {
                        let found = self
                            .mesh
                            .discover(agent, Path::new(cwd))
                            .await
                            .map_err(mesh_err)?;
                        self.touch_headless(agent.as_str());
                        found.into_iter().map(|v| v.to_string()).collect()
                    }
                    _ => Vec::<String>::new(),
                };
                Ok(serde_json::json!({ "known": known, "discovered": discovered }))
            }
            Request::Usage => {
                let usage: Vec<serde_json::Value> = self
                    .mesh
                    .usage()
                    .all()
                    .into_iter()
                    .map(|(agent, u)| {
                        serde_json::json!({
                            "agent": agent.to_string(),
                            "turns": u.turns,
                            "input_tokens": u.input_tokens,
                            "output_tokens": u.output_tokens,
                            "cost_usd": u.cost_usd(),
                            "cost_is_complete": u.cost_is_complete(),
                        })
                    })
                    .collect();
                Ok(serde_json::json!(usage))
            }
        }
    }

    fn touch_headless(&self, agent: &str) {
        if !agent.is_empty() {
            self.with(|s| {
                s.headless_active.insert(agent.to_owned(), Instant::now());
            });
        }
    }

    /// Everything the hub is running.
    async fn ps(&self) -> ProcessReport {
        self.prune().await;
        let now = Instant::now();
        let now_unix = unix_now();
        let (nodes, last_active, headless_active) = self.with(|s| {
            (
                s.nodes.values().cloned().collect::<Vec<_>>(),
                s.last_active.clone(),
                s.headless_active.clone(),
            )
        });
        let tree = process_tree().await;

        let mut processes = Vec::new();
        let mut unmanaged_nodes = Vec::new();
        for node in nodes {
            match (node.is_managed(), node.pid) {
                (true, Some(pid)) => processes.push(ProcessInfo {
                    id: node.id.clone(),
                    agent: node.agent.clone(),
                    kind: "tmux".to_owned(),
                    pid,
                    spawned_by: node.spawned_by.clone(),
                    age_seconds: (node.started_at_unix > 0)
                        .then(|| now_unix.saturating_sub(node.started_at_unix)),
                    idle_seconds: last_active
                        .get(&node.id)
                        .map(|t| now.duration_since(*t).as_secs()),
                    rss_mib: tree_rss_kib(&tree, pid) / 1024,
                    tmux_session: node.tmux_session.clone(),
                }),
                _ => unmanaged_nodes.push(node),
            }
        }
        for (agent, pid) in self.mesh.processes().await {
            processes.push(ProcessInfo {
                id: format!("{agent}/headless"),
                agent: agent.to_string(),
                kind: "headless".to_owned(),
                pid,
                spawned_by: None,
                age_seconds: None,
                idle_seconds: headless_active
                    .get(agent.as_str())
                    .map(|t| now.duration_since(*t).as_secs()),
                rss_mib: tree_rss_kib(&tree, pid) / 1024,
                tmux_session: None,
            });
        }

        ProcessReport {
            max_processes: self.config.max_processes,
            processes,
            unmanaged_nodes,
        }
    }

    /// Stop hub-owned processes. Sessions the user started are never touched.
    async fn kill(&self, target: Option<String>, all: bool) -> Result<serde_json::Value, String> {
        let managed: Vec<Node> =
            self.with(|s| s.nodes.values().filter(|n| n.is_managed()).cloned().collect());
        let mut killed = Vec::new();

        let wanted = |id: &str| all || target.as_deref() == Some(id);

        for node in &managed {
            if wanted(&node.id) {
                self.stop_node(node).await;
                killed.push(node.id.clone());
            }
        }

        let headless: Vec<String> = self
            .mesh
            .processes()
            .await
            .into_iter()
            .map(|(agent, _)| agent.to_string())
            .collect();
        let mut seen = Vec::new();
        for agent in headless {
            let id = format!("{agent}/headless");
            if wanted(&id) && !seen.contains(&agent) {
                self.mesh.shutdown_agent(&AgentId::new(agent.as_str())).await;
                seen.push(agent);
                killed.push(id);
            }
        }

        if killed.is_empty() && !all {
            let target = target.unwrap_or_default();
            let unmanaged = self.with(|s| s.nodes.get(&target).is_some_and(|n| !n.is_managed()));
            return Err(if unmanaged {
                format!(
                    "`{target}` is a session the user started, not one the mesh spawned; the mesh \
                     never closes those"
                )
            } else {
                format!("no mesh process `{target}`; run `agent-mesh ps` to see what is running")
            });
        }
        self.persist();
        Ok(serde_json::json!({ "killed": killed }))
    }

    async fn stop_node(&self, node: &Node) {
        if let Some(session) = node.tmux_session.as_deref() {
            let _ = tmux::kill_session(session).await;
        }
        self.with(|s| {
            s.nodes.remove(&node.id);
            s.inboxes.remove(&node.id);
            s.last_active.remove(&node.id);
        });
    }

    /// Close spawned nodes and headless processes nobody has used for `idle_timeout_minutes`.
    async fn reap_idle(&self) {
        let limit = Duration::from_secs(self.config.idle_timeout_minutes.saturating_mul(60));
        if limit.is_zero() {
            return;
        }
        let now = Instant::now();
        let (stale_nodes, stale_agents) = self.with(|s| {
            let nodes: Vec<Node> = s
                .nodes
                .values()
                .filter(|n| n.is_managed())
                .filter(|n| {
                    s.last_active
                        .get(&n.id)
                        .is_some_and(|t| now.duration_since(*t) > limit)
                })
                .cloned()
                .collect();
            let agents: Vec<String> = s
                .headless_active
                .iter()
                .filter(|(_, t)| now.duration_since(**t) > limit)
                .map(|(a, _)| a.clone())
                .collect();
            (nodes, agents)
        });
        for node in &stale_nodes {
            tracing::info!(node = %node.id, "reaping idle node");
            self.stop_node(node).await;
        }
        for agent in &stale_agents {
            tracing::info!(%agent, "reaping idle headless agent");
            self.mesh.shutdown_agent(&AgentId::new(agent.as_str())).await;
            self.with(|s| {
                s.headless_active.remove(agent);
            });
        }
        if !stale_nodes.is_empty() {
            self.persist();
        }
    }

    /// Save spawned nodes so a restarted hub keeps counting and can still kill them.
    fn persist(&self) {
        let nodes: Vec<Node> =
            self.with(|s| s.nodes.values().filter(|n| n.is_managed()).cloned().collect());
        let path = home_dir().join("nodes.json");
        match serde_json::to_vec_pretty(&nodes) {
            Ok(raw) => {
                if let Err(err) = std::fs::write(&path, raw) {
                    tracing::warn!(%err, "could not save nodes.json");
                }
            }
            Err(err) => tracing::warn!(%err, "could not encode nodes"),
        }
    }

    /// Re-adopt spawned nodes after a restart. tmux is the source of truth (a `mesh-` session is
    /// ours whether or not nodes.json survived); the file only adds who spawned it and when.
    async fn recover(&self) {
        let saved: Vec<Node> = std::fs::read(home_dir().join("nodes.json"))
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_default();
        let live = tmux::list_mesh_sessions().await.unwrap_or_default();

        let now = Instant::now();
        let mut adopted = 0;
        for found in live {
            let id = found.session.trim_start_matches(tmux::SESSION_PREFIX).to_owned();
            let node = saved
                .iter()
                .find(|n| n.id == id)
                .cloned()
                .map(|n| Node {
                    pid: Some(found.pid),
                    tmux_pane: Some(found.pane.clone()),
                    ..n
                })
                .unwrap_or_else(|| Node {
                    agent: id.split('-').next().unwrap_or("unknown").to_owned(),
                    id: id.clone(),
                    cwd: String::new(),
                    pid: Some(found.pid),
                    tmux_pane: Some(found.pane.clone()),
                    tmux_session: Some(found.session.clone()),
                    spawned_by: Some("unknown".to_owned()),
                    started_at_unix: 0,
                });
            self.with(|s| {
                s.inboxes.entry(node.id.clone()).or_default();
                s.last_active.insert(node.id.clone(), now);
                s.nodes.insert(node.id.clone(), node);
            });
            adopted += 1;
        }
        if adopted > 0 {
            tracing::info!(adopted, "re-adopted spawned nodes from tmux");
        }
        self.persist();
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

        if target.is_managed() {
            self.with(|s| {
                s.last_active.insert(to.clone(), Instant::now());
            });
        }

        if let Some(pane) = target.tmux_pane.as_deref() {
            match tmux::paste(pane, &frame(&message)).await {
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

        // Check the cap and record the node under one gate, so parallel spawns cannot overshoot.
        let _gate = self.spawn_gate.lock().await;
        if let Some(refusal) = self.capacity_refusal().await {
            return Err(refusal);
        }

        let id = mint_id(&agent);
        // A prompt from a peer node is framed like any other message so the spawned agent knows
        // who asked and how to answer. One with no sender came from a human and goes in as-is.
        let initial = prompt.map(|text| match from.as_deref() {
            Some(from) => frame(&Message {
                from: from.to_owned(),
                text,
                hops: 0,
                sent_at_unix: 0,
            }),
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
            spawned_by: Some(from.unwrap_or_else(|| "user".to_owned())),
            started_at_unix: unix_now(),
        };
        self.with(|s| {
            s.inboxes.entry(id.clone()).or_default();
            s.last_active.insert(id.clone(), Instant::now());
            s.nodes.insert(id.clone(), node.clone());
        });
        self.persist();

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
                    s.last_active.remove(id);
                }
            });
            self.persist();
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
        node.spawned_by = node.spawned_by.or(known.spawned_by);
        if node.started_at_unix == 0 {
            node.started_at_unix = known.started_at_unix;
        }
    }
    let id = node.id.clone();
    state.inboxes.entry(id.clone()).or_default();
    state.nodes.insert(id.clone(), node);
    id
}

fn mesh_err(err: MeshError) -> Failure {
    Failure {
        internal: !err.is_callers_fault(),
        message: err.to_string(),
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `pid -> (ppid, rss KiB)` for every process, from one `ps` call. macOS has no `/proc`.
async fn process_tree() -> BTreeMap<u32, (u32, u64)> {
    let out = tokio::process::Command::new("ps")
        .args(["-axo", "pid=,ppid=,rss="])
        .output()
        .await;
    let Ok(out) = out else {
        return BTreeMap::new();
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut cols = line.split_whitespace().map(str::parse::<u64>);
            let (Some(Ok(pid)), Some(Ok(ppid)), Some(Ok(rss))) =
                (cols.next(), cols.next(), cols.next())
            else {
                return None;
            };
            Some((u32::try_from(pid).ok()?, (u32::try_from(ppid).ok()?, rss)))
        })
        .collect()
}

/// Memory of `root` and every descendant. An agent's MCP servers and tool subprocesses are its
/// cost too.
fn tree_rss_kib(tree: &BTreeMap<u32, (u32, u64)>, root: u32) -> u64 {
    let mut total = 0;
    let mut stack = vec![root];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(pid) = stack.pop() {
        if !seen.insert(pid) {
            continue;
        }
        if let Some((_, rss)) = tree.get(&pid) {
            total += rss;
        }
        stack.extend(
            tree.iter()
                .filter(|(_, (ppid, _))| *ppid == pid)
                .map(|(child, _)| *child),
        );
    }
    total
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
///
/// Messages travel with the user's authority: every node on the mesh is one of the user's own
/// sessions, routed through the user's private hub. The footer only says where it came from and
/// how to answer, so the agent replies instead of treating the sender as an untrusted stranger.
pub fn frame(message: &Message) -> String {
    format!(
        "{text}\n\n(Sent to you over agent-mesh by `{from}` on your user's behalf. When you have \
         an answer, send it back with the agent-mesh send_message tool: to=\"{from}\", \
         hops={next}.)",
        from = message.from,
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

    fn hub() -> Arc<Hub> {
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
            spawned_by: None,
            started_at_unix: 0,
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

        let err = hub.dispatch(send("a", "b", 99)).await.unwrap_err().message;
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
        let err = hub.dispatch(send("a", "b", 0)).await.unwrap_err().message;
        assert!(err.contains("rate limited"), "got: {err}");
        // The limit is per pair: b can still answer a.
        hub.dispatch(send("b", "a", 1)).await.unwrap();
    }

    #[tokio::test]
    async fn sending_to_an_unknown_node_names_the_fix() {
        let hub = hub();
        register(&hub, "a").await;
        let err = hub.dispatch(send("a", "ghost", 0)).await.unwrap_err().message;
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
    fn frame_carries_the_users_authority_and_says_how_to_reply() {
        let framed = frame(&Message {
            from: "codex-1".to_owned(),
            text: "body".to_owned(),
            hops: 2,
            sent_at_unix: 0,
        });
        // The text leads, as a normal prompt; nothing tells the receiver to distrust it.
        assert!(framed.starts_with("body\n"));
        assert!(!framed.contains("not your user"));
        assert!(framed.contains("on your user's behalf"));
        assert!(framed.contains("to=\"codex-1\""));
        assert!(framed.contains("hops=3"));
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
