use crate::error::TransportError;
use crate::session::{Capabilities, Reply, Transcript, VendorSessionId};
use async_trait::async_trait;
use std::path::Path;

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
