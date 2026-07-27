use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AgentId(Arc<str>);

impl AgentId {
    pub fn new(raw: impl Into<Arc<str>>) -> Self {
        Self(raw.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A session id as minted by the vendor CLI. Opaque on purpose: the six agents use
/// incompatible formats (`ses_05e1...` for opencode, a UUID for claude, a UUIDv7 for codex),
/// so nothing above the transport may parse or construct one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VendorSessionId(Arc<str>);

impl VendorSessionId {
    pub fn new(raw: impl Into<Arc<str>>) -> Self {
        Self(raw.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for VendorSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Mesh-side handle for a session, stable across vendor id formats and process restarts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionRef(Arc<str>);

impl SessionRef {
    pub fn mint(agent: &AgentId) -> Self {
        Self(format!("{agent}:{}", uuid::Uuid::new_v4()).into())
    }

    pub fn parse(raw: impl Into<Arc<str>>) -> Self {
        Self(raw.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a session is in its lifecycle. This exists because the CLIs disagree on session-id
/// discipline: `claude` accepts a pinned id at any time, `grok`/`gemini` accept a pinned id
/// only for a session that does not yet exist (and hard-error otherwise), and `codex` will not
/// accept one at all. Transports dispatch on this state rather than guessing, so the
/// create-vs-resume decision is never implicit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionState {
    /// Registered but never sent a prompt; the vendor session does not exist yet.
    NotStarted,
    /// Live in the vendor CLI, with a process/connection currently attached.
    Live { vendor: VendorSessionId },
    /// Exists in the vendor CLI but no process is attached; needs a resume/load to prompt.
    Detached { vendor: VendorSessionId },
}

impl SessionState {
    pub fn vendor(&self) -> Option<&VendorSessionId> {
        match self {
            Self::NotStarted => None,
            Self::Live { vendor } | Self::Detached { vendor } => Some(vendor),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEntry {
    pub session: SessionRef,
    pub agent: AgentId,
    pub cwd: PathBuf,
    pub state: SessionState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Speaker {
    User,
    Agent,
    /// Agent reasoning/thinking, kept distinct so a peer reading a transcript can tell
    /// deliberation from a committed answer.
    AgentThought,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    pub speaker: Speaker,
    pub text: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transcript {
    pub turns: Vec<Turn>,
}

impl Transcript {
    pub fn from_turns(turns: impl IntoIterator<Item = Turn>) -> Self {
        Self {
            turns: turns.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.turns.is_empty()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

/// Spend for a turn. Only `claude` and `grok` report cost at all; `codex`, `gemini` and
/// `cursor-agent` report tokens only, hence `Option` at the call site rather than a
/// silently-zero float that would read as "free".
///
/// Held as integer micros because grok reports integer-exact ticks and its own docs warn that
/// summing float dollars will not reconcile against the vendor's usage export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostMicros(pub u64);

impl CostMicros {
    pub fn as_usd(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    pub text: String,
    pub usage: Usage,
    pub cost: Option<CostMicros>,
}

/// What a transport can actually do, probed from the vendor's own handshake rather than
/// assumed. `resume` is the load-bearing one: without it a session cannot be reached from a
/// second process, which is the whole premise of a control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub resume: bool,
    pub list_sessions: bool,
    pub cancel: bool,
    pub reports_cost: bool,
}
