//! Cross-machine delivery for a logged-in Crewmesh Pro user.
//!
//! The hub's accept loop never waits on this. When nobody has logged in, [`run`] only stats the
//! credentials file. It builds an HTTP client after a Pro entitlement is known.

use super::{Hub, Message, Node};
use async_trait::async_trait;
use mesh_pro::{PeerNode, RegisteredNode, RelayMessage};
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
}

struct Slot {
    token: String,
    remote: Arc<dyn RemoteMesh>,
}

impl RelayHandle {
    pub(super) fn new() -> Self {
        Self {
            mode: AtomicU8::new(AUTO),
            enabled: AtomicBool::new(false),
            slot: Mutex::new(None),
            peers: Mutex::new(Vec::new()),
        }
    }
}

pub(crate) struct Outbound {
    pub from_node: String,
    pub to_machine: Option<String>,
    pub to_node: String,
    pub text: String,
    pub hops: u32,
}

#[derive(Debug)]
pub(crate) enum RemoteError {
    NotPro(String),
    Unknown(String),
    Failed(String),
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPro(message) | Self::Unknown(message) | Self::Failed(message) => {
                f.write_str(message)
            }
        }
    }
}

#[async_trait]
pub(crate) trait RemoteMesh: Send + Sync {
    async fn send(&self, message: &Outbound) -> Result<mesh_pro::SendAck, RemoteError>;
    async fn register(&self, nodes: &[RegisteredNode]) -> Result<(), RemoteError>;
    async fn poll(&self, wait_ms: u64) -> Result<Vec<RelayMessage>, RemoteError>;
    async fn peers(&self) -> Result<Vec<PeerNode>, RemoteError>;
}

struct LiveRemote {
    client: mesh_pro::RelayClient,
}

#[async_trait]
impl RemoteMesh for LiveRemote {
    async fn send(&self, message: &Outbound) -> Result<mesh_pro::SendAck, RemoteError> {
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
}

fn remote_error(err: mesh_pro::AccountError) -> RemoteError {
    match err {
        mesh_pro::AccountError::NotPro(message) => RemoteError::NotPro(message),
        mesh_pro::AccountError::Unknown(message) => RemoteError::Unknown(message),
        other => RemoteError::Failed(other.to_string()),
    }
}

/// Background sync. Returns only when the process exits.
pub(super) async fn run(hub: Arc<Hub>) {
    let mut was_on = false;
    loop {
        if hub.relay.mode.load(Ordering::Acquire) != AUTO {
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }
        let on = mesh_pro::logged_in() && mesh_pro::pro_enabled().await;
        if on != was_on {
            if on {
                tracing::info!("Crewmesh Pro relay enabled");
            } else {
                tracing::info!("Crewmesh Pro relay disabled");
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

/// `Ok(None)` means "not a remote send"; the caller keeps today's local error.
pub(super) async fn try_remote(
    hub: &Hub,
    from: &str,
    to: &str,
    text: &str,
    hops: u32,
) -> Result<Option<serde_json::Value>, String> {
    let explicit = mesh_pro::parse_remote_target(to);
    let enabled = pro_on(hub, explicit.is_some()).await;
    if explicit.is_none() && !enabled {
        return Ok(None);
    }
    if !enabled {
        return Err(mesh_pro::upgrade_message());
    }

    let (to_machine, to_node, speculative) = if let Some((machine, node)) = explicit {
        (Some(machine), node, false)
    } else if let Some(peer) = unique_peer(hub, to)? {
        (Some(peer.machine_id), peer.id, false)
    } else {
        (None, to.to_owned(), true)
    };
    if to_node == from && to_machine.as_deref() == Some(our_machine().as_str()) {
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
            from_node: from.to_owned(),
            to_machine,
            to_node,
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
            })))
        }
        Err(RemoteError::Unknown(_)) if speculative => Ok(None),
        Err(RemoteError::Unknown(message)) => {
            Err(format!("{message}; call list_nodes to see who is online"))
        }
        Err(RemoteError::NotPro(message) | RemoteError::Failed(message)) => Err(message),
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

pub(super) async fn deliver(hub: &Hub, message: RelayMessage) -> bool {
    let Some(target) = hub.with(|state| state.nodes.get(&message.to_node).cloned()) else {
        tracing::info!(node = %message.to_node, "dropping a relay message for a node that is gone");
        return false;
    };
    let local = Message {
        from: mesh_pro::remote_target_id(&message.from_machine, &message.from_node),
        text: message.text,
        hops: message.hops,
        sent_at_unix: message.sent_at_unix,
    };
    let to = target.id.clone();
    hub.deliver_local(&to, &target, local).await.is_ok()
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
    for message in remote.poll(2_000).await.map_err(|err| err.to_string())? {
        deliver(hub, message).await;
    }
    Ok(())
}

fn local_nodes(hub: &Hub) -> Vec<RegisteredNode> {
    hub.with(|state| {
        state
            .nodes
            .values()
            .filter(|node| node.machine_id.is_none())
            .map(|node| RegisteredNode {
                id: node.id.clone(),
                name: node.name.clone(),
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
    if let Some(existing) = slot.as_ref() {
        if existing.token == creds.token {
            return Ok(Arc::clone(&existing.remote));
        }
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
}

async fn pro_on(hub: &Hub, explicit: bool) -> bool {
    match hub.relay.mode.load(Ordering::Acquire) {
        FORCE_ON => true,
        FORCE_OFF => false,
        _ => {
            if hub.relay.enabled.load(Ordering::Acquire) {
                return true;
            }
            // A plain unknown name stays on the local path. Only an explicit `remote:` id, from
            // someone who has logged in, is allowed to refresh entitlement.
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
            "`{to}` matches more than one remote node ({}); use the node id instead",
            many.iter()
                .map(|peer| mesh_pro::remote_target_id(&peer.machine_id, &peer.id))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
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
