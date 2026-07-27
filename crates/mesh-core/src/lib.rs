//! Core types for the agent mesh: session identity, the transport contract, and the registry
//! that tracks which vendor sessions exist and whether they are currently attached.

pub mod error;
pub mod registry;
pub mod session;
pub mod transport;

pub use error::TransportError;
pub use registry::{AskChain, ChainRejection, Route, SessionRegistry, absolute_cwd};
pub use session::{
    AgentId, Capabilities, CostMicros, Reply, SessionEntry, SessionRef, SessionState, Speaker,
    Transcript, Turn, Usage, VendorSessionId,
};
pub use transport::{AgentTransport, Attached, DynTransport, Opened};
