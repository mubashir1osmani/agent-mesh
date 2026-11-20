use crate::error::TransportError;
use crate::session::{Capabilities, Reply, Transcript, VendorSessionId};
use async_trait::async_trait;
use std::path::Path;

/// One agent process a transport keeps alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Process {
    pub pid: u32,
    /// The one session this process serves, for transports with a process per session (claude).
    /// `None` for a process shared by many sessions.
    pub session: Option<VendorSessionId>,
}

/// What stopping a process took down, so the caller knows which sessions must reattach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stopped {
    /// Only this session lost its process.
    Session(VendorSessionId),
    /// A shared process; any session of this agent may have been using it.
    Shared,
}

/// A freshly opened session plus whatever the vendor handed back at creation time.
#[derive(Debug, Clone)]
pub struct Opened {
    pub vendor: VendorSessionId,
}

/// The result of reaching an existing session from a new process. The transcript is part of the
/// return value rather than a side effect because ACP's `session/load` replays the prior
/// conversation as notifications during the call; discarding it would throw away the context
/// that makes cross-agent handoff useful.
#[derive(Debug, Clone)]
pub struct Attached {
    pub vendor: VendorSessionId,
    pub replayed: Transcript,
}

#[async_trait]
pub trait AgentTransport: Send + Sync {
    /// Create a new vendor session rooted at `cwd`.
    async fn open(&self, cwd: &Path) -> Result<Opened, TransportError>;

    /// Reach an existing vendor session, returning any transcript the vendor replays.
    async fn attach(&self, vendor: &VendorSessionId, cwd: &Path) -> Result<Attached, TransportError>;

    /// Send a prompt into a live session and wait for the agent to finish its turn.
    async fn prompt(&self, vendor: &VendorSessionId, text: &str) -> Result<Reply, TransportError>;

    /// Sessions the vendor knows about, for discovery of work started outside the mesh.
    async fn list_sessions(&self, cwd: &Path) -> Result<Vec<VendorSessionId>, TransportError>;

    fn capabilities(&self) -> Capabilities;

    /// Processes this transport currently keeps alive, so the hub can count them against its cap
    /// and report their memory. A shared process (one codex app-server for many threads) is
    /// listed once.
    async fn processes(&self) -> Vec<Process>;

    /// Whether reaching a session rooted at `cwd` would start a new process, as opposed to
    /// reusing one already running. The hub only enforces its process cap when this is true.
    async fn would_spawn(&self, cwd: &Path) -> bool;

    /// Whether `list_sessions` for `cwd` would start a process. Separate from `would_spawn`
    /// because some agents list sessions straight from disk.
    async fn discovery_spawns(&self, cwd: &Path) -> bool {
        self.would_spawn(cwd).await
    }

    /// Stop the process with this pid, if this transport owns it.
    async fn stop(&self, pid: u32) -> Option<Stopped>;

    /// Stop every process this transport owns. The next prompt starts a fresh one and resumes.
    async fn shutdown(&self);
}

/// Convenience for tests and for the registry: a boxed transport.
pub type DynTransport = std::sync::Arc<dyn AgentTransport>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_trait_is_object_safe() {
        fn assert_dyn(_: Option<&dyn AgentTransport>) {}
        assert_dyn(None);
    }
}
