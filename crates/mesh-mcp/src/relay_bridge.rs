use super::{Hub, Message, Node};
use async_trait::async_trait;
use mesh_core::AgentTransport;
use mesh_pro::{PeerNode, RegisteredNode, RelayMessage};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const AUTO: u8 = 0;
const FORCE_ON: u8 = 1;
const FORCE_OFF: u8 = 2;

pub(super) struct RelayHandle {
    mode: AtomicU8,
    enabled: AtomicBool,
    slot: Mutex<Option<Slot>>,
    peers: Mutex<Vec<PeerNode>>,
    surfaced: Mutex<HashSet<String>>,
    wake_budget: Mutex<crate::wake::Budget>,
}

struct Slot {
    token: String,
    remote: Arc<dyn RemoteMesh>,
}

impl RelayHandle {
    pub(super) fn new(wake_limit: u64) -> Self {
        Self {
            mode: AtomicU8::new(AUTO),
            enabled: AtomicBool::new(false),
            slot: Mutex::new(None),
            peers: Mutex::new(Vec::new()),
            surfaced: Mutex::new(HashSet::new()),
            wake_budget: Mutex::new(crate::wake::Budget::new(wake_limit)),
        }
    }
}

pub(crate) struct Outbound {
    pub from_node: String,
    pub to_machine: Option<String>,
    pub to_node: String,
    pub to_email: Option<String>,
    pub text: String,
    pub hops: u32,
}

#[derive(Debug)]
pub(crate) enum RemoteError {
    Unknown(String),
    Failed(String),
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown(message) | Self::Failed(message) => f.write_str(message),
        }
    }
}

#[async_trait]
pub(crate) trait RemoteMesh: Send + Sync {
    async fn send(&self, message: &Outbound) -> Result<mesh_pro::SendAck, RemoteError>;
    async fn register(&self, nodes: &[RegisteredNode]) -> Result<(), RemoteError>;
    async fn poll(&self, wait_ms: u64) -> Result<Vec<RelayMessage>, RemoteError>;
    async fn peers(&self) -> Result<Vec<PeerNode>, RemoteError>;
    async fn ack(&self, ids: &[String]) -> Result<(), RemoteError>;
    async fn surface(&self, ids: &[String]) -> Result<(), RemoteError>;
    async fn allowlist(&self) -> Result<Vec<String>, RemoteError>;
}

struct LiveRemote {
    client: mesh_pro::RelayClient,
}

#[async_trait]
impl RemoteMesh for LiveRemote {
    async fn send(&self, message: &Outbound) -> Result<mesh_pro::SendAck, RemoteError> {
        if let Some(email) = &message.to_email {
            return self
                .client
                .send_to_email(
                    &message.from_node,
                    email,
                    &message.to_node,
                    &message.text,
                    message.hops,
                )
                .await
                .map_err(remote_error);
        }
        self.client
            .send(
                &message.from_node,
                message.to_machine.as_deref(),
                &message.to_node,
                &message.text,
                message.hops,
            )
            .await
            .map_err(remote_error)
    }

    async fn register(&self, nodes: &[RegisteredNode]) -> Result<(), RemoteError> {
        self.client.register(nodes).await.map_err(remote_error)
    }

    async fn poll(&self, wait_ms: u64) -> Result<Vec<RelayMessage>, RemoteError> {
        self.client.poll(wait_ms).await.map_err(remote_error)
    }

    async fn peers(&self) -> Result<Vec<PeerNode>, RemoteError> {
        self.client.peers().await.map_err(remote_error)
    }

    async fn ack(&self, ids: &[String]) -> Result<(), RemoteError> {
        self.client.ack(ids).await.map_err(remote_error)
    }

    async fn surface(&self, ids: &[String]) -> Result<(), RemoteError> {
        self.client.surface(ids).await.map_err(remote_error)
    }

    async fn allowlist(&self) -> Result<Vec<String>, RemoteError> {
        self.client.allowlist().await.map_err(remote_error)
    }
}

fn remote_error(err: mesh_pro::AccountError) -> RemoteError {
    match err {
        mesh_pro::AccountError::Unknown(message) => RemoteError::Unknown(message),
        mesh_pro::AccountError::NotPro(message) => RemoteError::Failed(message),
        other => RemoteError::Failed(other.to_string()),
    }
}

pub(super) async fn run(hub: Arc<Hub>) {
    let mut was_on = false;
    loop {
        let on = mesh_pro::logged_in() && mesh_pro::pro_enabled().await;
        if on != was_on {
            if on {
                tracing::info!("relay enabled");
            } else if was_on {
                tracing::info!("relay disabled");
                disable(&hub);
            }
            was_on = on;
        }
        if !on {
            let pause = if mesh_pro::logged_in() { 30 } else { 5 };
            tokio::time::sleep(Duration::from_secs(pause)).await;
            continue;
        }
        hub.relay.enabled.store(true, Ordering::Release);
        if let Err(err) = sync(&hub).await {
            tracing::warn!(%err, "crewmesh relay sync failed");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

pub(super) async fn try_remote(
    hub: &Hub,
    from: &str,
    to: &str,
    text: &str,
    hops: u32,
) -> Result<Option<serde_json::Value>, String> {
    let explicit = mesh_pro::parse_remote_target(to);
    let addressed = node_email(to);
    let enabled = pro_on(hub, explicit.is_some() || addressed.is_some()).await;
    if explicit.is_none() && addressed.is_none() && !enabled {
        return Ok(None);
    }
    if !enabled {
        return Err(mesh_pro::upgrade_message());
    }

    let from_label = hub.with(|state| {
        state
            .nodes
            .get(from)
            .and_then(|node| node.name.clone())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| from.to_owned())
    });
    let (to_machine, to_node, to_email, speculative) = if let Some((node, email)) = addressed {
        let machine = lock(&hub.relay.peers).iter().find_map(|peer| {
            (peer.email.as_deref() == Some(email) && peer.name.as_deref() == Some(node))
                .then(|| peer.machine_id.clone())
        });
        (machine, node.to_owned(), Some(email.to_owned()), false)
    } else if let Some((machine, node)) = explicit {
        (Some(machine), node, None, false)
    } else if let Some(peer) = unique_peer(hub, to)? {
        let to_node = if peer.email.is_some() {
            peer.name.clone().unwrap_or_else(|| peer.id.clone())
        } else {
            peer.id.clone()
        };
        (Some(peer.machine_id), to_node, peer.email, false)
    } else {
        (None, to.to_owned(), None, true)
    };
    if (to_node == from || to_node == from_label)
        && to_machine.as_deref() == Some(our_machine().as_str())
    {
        return Err("a node cannot message itself".to_owned());
    }

    let remote = remote_for(hub)?;
    let rate_key = match &to_machine {
        Some(machine) => mesh_pro::remote_target_id(machine, &to_node),
        None => format!("relay:{to_node}"),
    };
    hub.rate_limit(from, &rate_key)?;
    match remote
        .send(&Outbound {
            from_node: from_label,
            to_machine,
            to_node,
            to_email,
            text: text.to_owned(),
            hops,
        })
        .await
    {
        Ok(ack) => {
            hub.touch_pair(from, from);
            Ok(Some(serde_json::json!({
                "delivery": "relay",
                "to": mesh_pro::remote_target_id(&ack.to_machine, &ack.to_node),
                "receipt": ack.id,
            })))
        }
        Err(RemoteError::Unknown(_)) if speculative => Ok(None),
        Err(RemoteError::Unknown(message)) => {
            Err(format!("{message}; call list_nodes to see who is online"))
        }
        Err(RemoteError::Failed(message)) => Err(message),
    }
}

pub(super) fn remote_nodes(hub: &Hub) -> Vec<Node> {
    let mode = hub.relay.mode.load(Ordering::Acquire);
    let on = mode == FORCE_ON || hub.relay.enabled.load(Ordering::Acquire);
    if !on {
        return Vec::new();
    }
    lock(&hub.relay.peers)
        .iter()
        .map(|peer| Node {
            id: mesh_pro::remote_target_id(&peer.machine_id, &peer.id),
            name: peer.name.clone(),
            agent: peer.agent.clone(),
            cwd: String::new(),
            pid: None,
            tmux_pane: None,
            tmux_session: None,
            spawned_by: None,
            started_at_unix: 0,
            machine_id: Some(peer.machine_id.clone()),
        })
        .collect()
}

pub(super) struct Handed {
    pub pushed: bool,
    pub agent: String,
    pub session: Option<String>,
    pub cwd: String,
}

pub(super) async fn deliver(hub: &Hub, message: RelayMessage) -> Option<Handed> {
    let Some(target) = hub.with(|state| {
        let locals: Vec<Node> = state
            .nodes
            .values()
            .filter(|node| node.machine_id.is_none())
            .cloned()
            .collect();
        locals
            .iter()
            .find(|node| {
                node.id == message.to_node || node.name.as_deref() == Some(message.to_node.as_str())
            })
            .cloned()
            .or_else(|| {
                let matched: Vec<Node> = locals
                    .iter()
                    .filter(|node| stable_name(&node.agent) == message.to_node)
                    .cloned()
                    .collect();
                if matched.len() == 1 {
                    matched.into_iter().next()
                } else {
                    None
                }
            })
    }) else {
        tracing::info!(node = %message.to_node, "relay message has no local node yet");
        return None;
    };
    let handed = Handed {
        pushed: target.tmux_pane.is_some(),
        agent: target.agent.clone(),
        session: session_of(&target),
        cwd: target.cwd.clone(),
    };
    let local = Message {
        from: mesh_pro::remote_target_id(&message.from_machine, &message.from_node),
        text: message.text,
        hops: message.hops,
        sent_at_unix: message.sent_at_unix,
    };
    let to = target.id.clone();
    if hub.deliver_local(&to, &target, local).await.is_err() {
        return None;
    }
    Some(handed)
}

fn session_of(node: &Node) -> Option<String> {
    if node.name.as_deref() == Some(node.id.as_str()) {
        None
    } else {
        Some(node.id.clone())
    }
}

#[cfg(test)]
pub(crate) fn install_for_test(hub: &Hub, remote: Arc<dyn RemoteMesh>, peers: Vec<PeerNode>) {
    hub.relay.mode.store(FORCE_ON, Ordering::Release);
    hub.relay.enabled.store(true, Ordering::Release);
    *lock(&hub.relay.slot) = Some(Slot {
        token: "test".to_owned(),
        remote,
    });
    *lock(&hub.relay.peers) = peers;
}

#[cfg(test)]
pub(crate) fn force_free_for_test(hub: &Hub) {
    hub.relay.mode.store(FORCE_OFF, Ordering::Release);
    hub.relay.enabled.store(false, Ordering::Release);
}

async fn sync(hub: &Hub) -> Result<(), String> {
    let remote = ensure(hub)?;
    let nodes = local_nodes(hub);
    remote
        .register(&nodes)
        .await
        .map_err(|err| err.to_string())?;
    let peers = remote.peers().await.map_err(|err| err.to_string())?;
    *lock(&hub.relay.peers) = peers;
    let allowed = remote.allowlist().await.unwrap_or_default();
    let mut acked = Vec::new();
    for message in remote.poll(2_000).await.map_err(|err| err.to_string())? {
        let id = message.id.clone();
        if id.is_empty() {
            let _ = deliver(hub, message).await;
            continue;
        }
        if lock(&hub.relay.surfaced).contains(&id) {
            acked.push(id);
            continue;
        }
        let Some(handed) = deliver(hub, message.clone()).await else {
            continue;
        };
        lock(&hub.relay.surfaced).insert(id.clone());
        let shown = if handed.pushed {
            true
        } else {
            wake_idle(hub, &handed, &message, &allowed).await
        };
        if shown {
            remote
                .surface(std::slice::from_ref(&id))
                .await
                .map_err(|err| err.to_string())?;
            lock(&hub.relay.surfaced).insert(id.clone());
            acked.push(id);
        }
    }
    if !acked.is_empty() {
        remote.ack(&acked).await.map_err(|err| err.to_string())?;
    }
    Ok(())
}

async fn wake_idle(hub: &Hub, handed: &Handed, message: &RelayMessage, allowed: &[String]) -> bool {
    let Some(wake) = crate::wake::plan(
        &handed.agent,
        handed.session.as_deref(),
        &message.text,
        message.from_email.as_deref(),
        allowed,
        true,
        true,
    ) else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    if !lock(&hub.relay.wake_budget).allow(now) {
        return false;
    }
    run_wake(wake, Path::new(&handed.cwd)).await.is_ok()
}

async fn run_wake(wake: crate::wake::Wake, cwd: &Path) -> Result<(), String> {
    use crate::wake::{Wake, claude_print_args, codex_exec_args};
    match wake {
        Wake::CodexApp { session, prompt } => {
            let transport =
                mesh_codex::CodexTransport::new(mesh_core::AgentId::new("codex"), "codex");
            let vendor = mesh_core::VendorSessionId::new(session.clone());
            if transport.attach(&vendor, cwd).await.is_ok()
                && transport.prompt(&vendor, &prompt).await.is_ok()
            {
                return Ok(());
            }
            let args = codex_exec_args("codex", Some(&session), &prompt);
            crate::wake::run(&args[0], &args, cwd).await
        }
        Wake::CodexExec { session, prompt } => {
            let args = codex_exec_args("codex", session.as_deref(), &prompt);
            crate::wake::run(&args[0], &args, cwd).await
        }
        Wake::ClaudePrint { session, prompt } => {
            let args = claude_print_args("claude", session.as_deref(), &prompt);
            crate::wake::run(&args[0], &args, cwd).await
        }
    }
}

fn local_nodes(hub: &Hub) -> Vec<RegisteredNode> {
    hub.with(|state| {
        state
            .nodes
            .values()
            .filter(|node| node.machine_id.is_none())
            .map(|node| RegisteredNode {
                id: node.id.clone(),
                name: Some(
                    node.name
                        .clone()
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or_else(|| stable_name(&node.agent)),
                ),
                agent: node.agent.clone(),
            })
            .collect()
    })
}

fn ensure(hub: &Hub) -> Result<Arc<dyn RemoteMesh>, String> {
    let creds = mesh_pro::Store::from_env()
        .load()
        .ok_or_else(|| "not logged in".to_owned())?;
    let mut slot = lock(&hub.relay.slot);
    if let Some(existing) = slot.as_ref()
        && existing.token == creds.token
    {
        return Ok(Arc::clone(&existing.remote));
    }
    let machine = machine_id()?;
    let client = mesh_pro::RelayClient::new(&creds.api_base, &creds.token, machine)
        .map_err(|err| err.to_string())?;
    let remote: Arc<dyn RemoteMesh> = Arc::new(LiveRemote { client });
    *slot = Some(Slot {
        token: creds.token,
        remote: Arc::clone(&remote),
    });
    Ok(remote)
}

fn remote_for(hub: &Hub) -> Result<Arc<dyn RemoteMesh>, String> {
    if hub.relay.mode.load(Ordering::Acquire) == FORCE_ON {
        return current(hub).ok_or_else(|| "relay is not connected".to_owned());
    }
    if let Some(remote) = current(hub) {
        return Ok(remote);
    }
    ensure(hub)
}

fn current(hub: &Hub) -> Option<Arc<dyn RemoteMesh>> {
    lock(&hub.relay.slot)
        .as_ref()
        .map(|slot| Arc::clone(&slot.remote))
}

fn disable(hub: &Hub) {
    hub.relay.enabled.store(false, Ordering::Release);
    *lock(&hub.relay.slot) = None;
    *lock(&hub.relay.peers) = Vec::new();
    lock(&hub.relay.surfaced).clear();
}

async fn pro_on(hub: &Hub, explicit: bool) -> bool {
    match hub.relay.mode.load(Ordering::Acquire) {
        FORCE_ON => true,
        FORCE_OFF => false,
        _ => {
            if hub.relay.enabled.load(Ordering::Acquire) {
                return true;
            }

            if !explicit || !mesh_pro::logged_in() {
                return false;
            }
            let on = mesh_pro::pro_enabled().await;
            hub.relay.enabled.store(on, Ordering::Release);
            on
        }
    }
}

fn unique_peer(hub: &Hub, to: &str) -> Result<Option<PeerNode>, String> {
    let peers = lock(&hub.relay.peers);
    let hits: Vec<&PeerNode> = peers
        .iter()
        .filter(|peer| peer.id == to || peer.name.as_deref() == Some(to))
        .collect();
    match hits.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some((*one).clone())),
        many => Err(format!(
            "`{to}` matches more than one remote node ({}); address them as node@email",
            many.iter()
                .map(|peer| mesh_pro::remote_target_id(&peer.machine_id, &peer.id))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn node_email(raw: &str) -> Option<(&str, &str)> {
    let (node, email) = raw.split_once('@')?;
    if node.is_empty() || !email.contains('@') || !email.contains('.') {
        return None;
    }
    Some((node, email))
}

fn stable_name(agent: &str) -> String {
    let path = super::home_dir().join("names").join(agent);
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let name = existing.trim();
        if !name.is_empty() && name.len() <= 200 && !name.contains(['/', '\n', '\r']) {
            return name.to_owned();
        }
    }
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&path, agent);
    agent.to_owned()
}

fn our_machine() -> String {
    std::fs::read_to_string(super::home_dir().join("machine-id"))
        .map(|text| text.trim().to_owned())
        .unwrap_or_default()
}

fn machine_id() -> Result<String, String> {
    let path = super::home_dir().join("machine-id");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let id = existing.trim();
        if !id.is_empty() && id.len() <= 128 && !id.contains(['/', '\n', ':']) {
            return Ok(id.to_owned());
        }
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    std::fs::write(&path, &id).map_err(|err| err.to_string())?;
    Ok(id)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}
