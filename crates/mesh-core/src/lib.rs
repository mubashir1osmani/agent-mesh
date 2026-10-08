//! Core types for the agent mesh: session identity, the transport contract, and the registry
//! that tracks which vendor sessions exist and whether they are currently attached.

pub mod error;
pub mod jsonrpc;
pub mod registry;
pub mod session;
pub mod transport;

pub use error::TransportError;
pub use jsonrpc::{Connection, Inbound};
pub use registry::{AskChain, ChainRejection, Route, SessionRegistry, absolute_cwd};
pub use session::{
    AgentId, Capabilities, CostMicros, Reply, SessionEntry, SessionRef, SessionState, Speaker,
    Transcript, Turn, Usage, VendorSessionId,
};
pub use transport::{AgentTransport, Attached, DynTransport, Opened, Process, Stopped};

/// Set on every agent process a transport spawns. An agent-mesh MCP server started under one of
/// those headless agents is plumbing for a single `ask_agent`, not a session anyone should
/// address, so it must not register itself with the hub.
pub const HEADLESS_ENV: &str = "AGENT_MESH_HEADLESS";
