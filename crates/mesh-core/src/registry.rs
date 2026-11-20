use crate::error::TransportError;
use crate::session::{AgentId, SessionEntry, SessionRef, SessionState, VendorSessionId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// What must happen before a prompt can be delivered. Returned by the registry so the caller
/// dispatches on an explicit decision instead of inferring create-vs-resume, which is the bug
/// that would work on turn 1 and fail on turn 2 for `grok` and `gemini`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// No vendor session exists yet; create one.
    Create { cwd: PathBuf },
    /// Vendor session exists and a connection is attached; prompt directly.
    PromptDirect { vendor: VendorSessionId },
    /// Vendor session exists but nothing is attached; load/resume first, then prompt.
    ReattachThenPrompt { vendor: VendorSessionId, cwd: PathBuf },
}

/// Tracks the mapping from mesh-side `SessionRef` to vendor session state.
///
/// Uses interior mutability behind an `RwLock` because the MCP server hands out `&self` to
/// concurrent tool calls; the lock is held only for map access, never across an await.
#[derive(Debug, Default)]
pub struct SessionRegistry {
    entries: RwLock<BTreeMap<SessionRef, SessionEntry>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session the mesh has not yet created in the vendor.
    pub fn register_new(&self, agent: AgentId, cwd: PathBuf) -> SessionRef {
        let session = SessionRef::mint(&agent);
        let entry = SessionEntry {
            session: session.clone(),
            agent,
            cwd,
            state: SessionState::NotStarted,
        };
        self.write(|m| {
            m.insert(session.clone(), entry);
        });
        session
    }

    /// Register a session that already exists in the vendor, discovered rather than created.
    pub fn register_existing(
        &self,
        agent: AgentId,
        cwd: PathBuf,
        vendor: VendorSessionId,
    ) -> SessionRef {
        let session = SessionRef::mint(&agent);
        let entry = SessionEntry {
            session: session.clone(),
            agent,
            cwd,
            state: SessionState::Detached { vendor },
        };
        self.write(|m| {
            m.insert(session.clone(), entry);
        });
        session
    }

    pub fn get(&self, session: &SessionRef) -> Result<SessionEntry, TransportError> {
        self.read(|m| m.get(session).cloned())
            .ok_or_else(|| TransportError::UnknownSession {
                session: session.clone(),
            })
    }

    /// Decide what has to happen before `session` can accept a prompt.
    pub fn route(&self, session: &SessionRef) -> Result<Route, TransportError> {
        let entry = self.get(session)?;
        Ok(match entry.state {
            SessionState::NotStarted => Route::Create { cwd: entry.cwd },
            SessionState::Live { vendor } => Route::PromptDirect { vendor },
            SessionState::Detached { vendor } => Route::ReattachThenPrompt {
                vendor,
                cwd: entry.cwd,
            },
        })
    }

    /// Record that the vendor session now exists and is attached.
    pub fn mark_live(
        &self,
        session: &SessionRef,
        vendor: VendorSessionId,
    ) -> Result<(), TransportError> {
        self.mutate(session, |e| {
            e.state = SessionState::Live { vendor };
        })
    }

    /// Record that the vendor session still exists but nothing is attached to it.
    pub fn mark_detached(&self, session: &SessionRef) -> Result<(), TransportError> {
        let entry = self.get(session)?;
        let Some(vendor) = entry.state.vendor().cloned() else {
            return Ok(());
        };
        self.mutate(session, |e| {
            e.state = SessionState::Detached { vendor };
        })
    }

    pub fn list(&self, agent: Option<&AgentId>) -> Vec<SessionEntry> {
        self.read(|m| {
            m.values()
                .filter(|e| agent.is_none_or(|a| &e.agent == a))
                .cloned()
                .collect()
        })
    }

    /// Look up a mesh session by the vendor's own id, so an agent can be handed a raw
    /// vendor id and still land on the existing mesh entry rather than duplicating it.
    pub fn find_by_vendor(&self, agent: &AgentId, vendor: &VendorSessionId) -> Option<SessionEntry> {
        self.read(|m| {
            m.values()
                .find(|e| &e.agent == agent && e.state.vendor() == Some(vendor))
                .cloned()
        })
    }

    fn mutate(
        &self,
        session: &SessionRef,
        f: impl FnOnce(&mut SessionEntry),
    ) -> Result<(), TransportError> {
        let mut guard = self.entries.write().unwrap_or_else(|e| e.into_inner());
        match guard.get_mut(session) {
            Some(entry) => {
                f(entry);
                Ok(())
            }
            None => Err(TransportError::UnknownSession {
                session: session.clone(),
            }),
        }
    }

    fn read<T>(&self, f: impl FnOnce(&BTreeMap<SessionRef, SessionEntry>) -> T) -> T {
        let guard = self.entries.read().unwrap_or_else(|e| e.into_inner());
        f(&guard)
    }

    fn write<T>(&self, f: impl FnOnce(&mut BTreeMap<SessionRef, SessionEntry>) -> T) -> T {
        let mut guard = self.entries.write().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }
}

/// Bounds an `ask_agent` relay so agents cannot ping-pong forever.
///
/// Two distinct hazards, deliberately treated differently:
///
/// - **Immediate self-ask** (a session asking itself) is always a mistake and is refused outright.
/// - **Unbounded relay** is bounded by depth, not by forbidding revisits. Coming back to a session
///   you already spoke to is normal and useful ("ask codex, then go back and tell opencode what it
///   said"), so refusing every revisit would break the main cross-agent workflow. Depth is what
///   guarantees termination.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AskChain {
    hops: Arc<Vec<SessionRef>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainRejection {
    /// The target session is the one currently asking, which would recurse immediately.
    SelfAsk { session: SessionRef },
    /// The relay is longer than the configured limit.
    TooDeep { limit: usize },
}

impl AskChain {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn from_hops(hops: impl IntoIterator<Item = SessionRef>) -> Self {
        Self {
            hops: Arc::new(hops.into_iter().collect()),
        }
    }

    pub fn depth(&self) -> usize {
        self.hops.len()
    }

    pub fn hops(&self) -> &[SessionRef] {
        &self.hops
    }

    /// Extend the relay with `next`, refusing an immediate self-ask or an over-long relay.
    pub fn push(&self, next: &SessionRef, limit: usize) -> Result<Self, ChainRejection> {
        // Only the *most recent* hop matters: a session asking itself recurses with no progress,
        // while returning to an earlier session is a legitimate relay.
        if self.hops.last().is_some_and(|last| last == next) {
            return Err(ChainRejection::SelfAsk {
                session: next.clone(),
            });
        }
        if self.hops.len() >= limit {
            return Err(ChainRejection::TooDeep { limit });
        }
        Ok(Self {
            hops: Arc::new(
                self.hops
                    .iter()
                    .cloned()
                    .chain(std::iter::once(next.clone()))
                    .collect(),
            ),
        })
    }
}

/// Resolve a `cwd` argument to an existing absolute directory.
///
/// Absolute paths are still checked rather than trusted: a nonexistent directory has to fail here
/// with a clear error, otherwise it surfaces much later as an opaque spawn failure from whichever
/// agent was asked to start there.
pub fn absolute_cwd(cwd: &Path) -> Result<PathBuf, std::io::Error> {
    let resolved = std::fs::canonicalize(cwd)?;
    if !resolved.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!("{} is not a directory", resolved.display()),
        ));
    }
    Ok(resolved)
}
