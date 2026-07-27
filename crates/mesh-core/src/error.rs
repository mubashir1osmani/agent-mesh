use crate::session::{AgentId, SessionRef};
use thiserror::Error;

/// Transport failures as values. Every variant carries enough context for the MCP layer to map
/// it to a caller-facing error without re-parsing strings.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("agent `{agent}` is not reachable: {reason}")]
    Unreachable { agent: AgentId, reason: String },

    #[error("failed to spawn `{command}`: {source}")]
    Spawn {
        command: String,
        #[source]
        source: std::io::Error,
    },

    #[error("agent `{agent}` closed the connection unexpectedly")]
    ConnectionClosed { agent: AgentId },

    #[error("protocol violation from `{agent}`: {detail}")]
    Protocol { agent: AgentId, detail: String },

    #[error("agent `{agent}` rejected the request: {message}")]
    AgentRefused { agent: AgentId, message: String },

    /// The vendor does not support reaching an existing session from a new process, so the
    /// control plane cannot bridge into it.
    #[error("agent `{agent}` cannot resume sessions")]
    ResumeUnsupported { agent: AgentId },

    #[error("session `{session}` is not known to the mesh")]
    UnknownSession { session: SessionRef },

    #[error("timed out after {seconds}s waiting for `{agent}`")]
    Timeout { agent: AgentId, seconds: u64 },

    #[error("request to `{agent}` was cancelled")]
    Cancelled { agent: AgentId },

    #[error("could not decode response from `{agent}`: {detail}")]
    Decode { agent: AgentId, detail: String },
}
