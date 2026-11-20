//! Crewmesh Pro account client.
//!
//! The local mesh never has to call into this. [`pro_enabled`] returns `false` immediately when
//! nobody has logged in, without opening a socket. A logged-in user's answer is cached on disk
//! until it expires; a failed refresh does not block past a short timeout, and a still-valid
//! cached "pro" answer keeps working offline for a grace period.

mod entitlement;
mod error;
mod http;
mod protocol;
mod relay_client;

pub use entitlement::{
    Credentials, DEFAULT_API_BASE, DEFAULT_GRACE_SECONDS, DEFAULT_UPGRADE_URL, Status, Store,
    logged_in, logout, pro_enabled, upgrade_message, upgrade_url,
};
pub use error::AccountError;
pub use http::ApiClient;
pub use protocol::{
    CheckoutSession, DEFAULT_CACHE_TTL_SECONDS, DevicePoll, DeviceStart, Entitlement, PeerNode,
    PeersResponse, PollResponse, RegisterBody, RegisteredNode, RelayMessage, SendAck, SendBody,
    parse_remote_target, remote_target_id,
};
pub use relay_client::RelayClient;
