//! The control plane: owns the agent transports, the session registry, and the rules that make
//! cross-agent prompting safe (loop guard, per-turn timeout).

use crate::config::{AgentConfig, Config};
use mesh_acp::{AcpLaunch, AcpTransport};
use mesh_claude::{ClaudeLaunch, ClaudeTransport};
use mesh_codex::CodexTransport;
use mesh_core::registry::Route;
use mesh_core::{
    AgentId, AgentTransport, AskChain, Capabilities, ChainRejection, Reply, SessionEntry,
    SessionRef, SessionRegistry, Stopped, Transcript, TransportError, VendorSessionId,
};
use mesh_telemetry::{AskOutcome, UsageRecorder};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Why a mesh operation failed. Distinct from `TransportError` because the mesh has failure modes
/// no single transport has: an unknown agent, a refused ask chain.
#[derive(Debug, thiserror::Error)]
pub enum MeshError {
    #[error("unknown agent `{requested}`; available: {available}")]
    UnknownAgent { requested: String, available: String },

    #[error("refusing to route this ask: {reason}")]
    AskRefused { reason: String },

    /// The hub's process cap is reached. Carries the message the hub built, which lists what is
    /// running so the caller can decide what to close.
    #[error("{0}")]
    AtCapacity(String),

    #[error("`{cwd}` is not a usable working directory: {source}")]
    BadCwd {
        cwd: String,
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    Transport(#[from] TransportError),
}

/// Decides whether a new agent process may start. Returns the refusal message when it may not.
/// The hub supplies one that counts every process it owns; without one, nothing is capped.
pub type Admission = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Option<String>> + Send>> + Send + Sync>;

impl MeshError {
    /// True when the caller can fix this by asking differently (bad argument, refused loop, cap
    /// reached), as opposed to an agent or transport failing. Exhaustive so a new failure mode
    /// cannot silently fall into the wrong bucket.
    pub fn is_callers_fault(&self) -> bool {
        match self {
            Self::UnknownAgent { .. }
            | Self::BadCwd { .. }
            | Self::AskRefused { .. }
            | Self::AtCapacity(_) => true,
            Self::Transport(inner) => match inner {
                TransportError::UnknownSession { .. } => true,
                TransportError::ResumeUnsupported { .. }
                | TransportError::Unreachable { .. }
                | TransportError::Spawn { .. }
                | TransportError::ConnectionClosed { .. }
                | TransportError::Protocol { .. }
                | TransportError::AgentRefused { .. }
                | TransportError::Timeout { .. }
                | TransportError::Cancelled { .. }
                | TransportError::Decode { .. } => false,
            },
        }
    }
}

pub struct Mesh {
    transports: BTreeMap<AgentId, Arc<dyn AgentTransport>>,
    registry: SessionRegistry,
    usage: Arc<UsageRecorder>,
    max_ask_depth: usize,
    turn_timeout: Duration,
    admission: Option<Admission>,
    /// Serializes "check the cap, then start a process", so parallel starts cannot both pass the
    /// check and overshoot it. Shared with the hub, which takes it for tmux spawns too.
    spawn_gate: Arc<tokio::sync::Mutex<()>>,
}

/// One headless agent process, as the hub reports it.
#[derive(Debug, Clone)]
pub struct HeadlessProcess {
    pub agent: AgentId,
    pub pid: u32,
    /// Mesh sessions this process serves: exactly one for claude, every session of the agent for
    /// a shared process.
    pub sessions: Vec<SessionRef>,
}

impl Mesh {
    pub fn from_config(config: &Config) -> Self {
        let transports = config
            .agents
            .iter()
            .filter(|(_, cfg)| cfg.enabled())
            .map(|(name, cfg)| {
                let agent = AgentId::new(name.as_str());
                let transport = build_transport(&agent, cfg);
                (agent, transport)
            })
            .collect();

        Self {
            transports,
            registry: SessionRegistry::new(),
            usage: Arc::new(UsageRecorder::new()),
            max_ask_depth: config.max_ask_depth,
            turn_timeout: Duration::from_secs(config.turn_timeout_seconds),
            admission: None,
            spawn_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn with_admission(
        mut self,
        admission: Admission,
        gate: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        self.admission = Some(admission);
        self.spawn_gate = gate;
        self
    }

    /// Every agent process this mesh's transports keep alive.
    pub async fn processes(&self) -> Vec<HeadlessProcess> {
        let mut out = Vec::new();
        for (agent, transport) in &self.transports {
            for process in transport.processes().await {
                let sessions = self
                    .registry
                    .list(Some(agent))
                    .into_iter()
                    .filter(|e| match &process.session {
                        Some(vendor) => e.state.vendor() == Some(vendor),
                        None => e.state.vendor().is_some(),
                    })
                    .map(|e| e.session)
                    .collect();
                out.push(HeadlessProcess {
                    agent: agent.clone(),
                    pid: process.pid,
                    sessions,
                });
            }
        }
        out
    }

    /// Stop one headless process and detach the sessions it served, so they reattach on their
    /// next ask. Returns false if no transport owns `pid`.
    pub async fn stop_process(&self, pid: u32) -> bool {
        for (agent, transport) in &self.transports {
            let Some(stopped) = transport.stop(pid).await else {
                continue;
            };
            for entry in self.registry.list(Some(agent)) {
                let affected = match &stopped {
                    Stopped::Session(vendor) => entry.state.vendor() == Some(vendor),
                    Stopped::Shared => true,
                };
                if affected {
                    let _ = self.registry.mark_detached(&entry.session);
                }
            }
            return true;
        }
        false
    }

    /// Stop every headless process and detach every session. Nothing is lost: sessions resume on
    /// their next ask.
    pub async fn shutdown_all(&self) {
        for transport in self.transports.values() {
            transport.shutdown().await;
        }
        for entry in self.registry.list(None) {
            let _ = self.registry.mark_detached(&entry.session);
        }
    }

    /// Run `step` only if the cap allows starting a process. `spawns` says whether this step
    /// would start one; if not, it runs ungated.
    async fn admitted<T>(
        &self,
        spawns: bool,
        step: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, MeshError> {
        let Some(admission) = self.admission.as_ref().filter(|_| spawns) else {
            return Ok(step.await?);
        };
        let _gate = self.spawn_gate.lock().await;
        if let Some(refusal) = admission().await {
            return Err(MeshError::AtCapacity(refusal));
        }
        Ok(step.await?)
    }

    /// Accumulated token and cost totals per agent.
    pub fn usage(&self) -> &UsageRecorder {
        &self.usage
    }

    pub fn agents(&self) -> impl Iterator<Item = (&AgentId, Capabilities)> {
        self.transports
            .iter()
            .map(|(id, t)| (id, t.capabilities()))
    }


    fn transport(&self, agent: &AgentId) -> Result<Arc<dyn AgentTransport>, MeshError> {
        self.transports
            .get(agent)
            .map(Arc::clone)
            .ok_or_else(|| MeshError::UnknownAgent {
                requested: agent.to_string(),
                available: self
                    .transports
                    .keys()
                    .map(AgentId::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            })
    }

    /// Register a new session for `agent`. The vendor session is created lazily on first prompt,
    /// so opening a session is cheap and cannot fail on a cold agent.
    pub fn open_session(&self, agent: &AgentId, cwd: &Path) -> Result<SessionRef, MeshError> {
        self.transport(agent)?;
        let cwd = resolve_cwd(cwd)?;
        Ok(self.registry.register_new(agent.clone(), cwd))
    }

    /// Adopt a session that already exists inside an agent, so work started outside the mesh can
    /// be joined.
    pub async fn attach_session(
        &self,
        agent: &AgentId,
        vendor: &VendorSessionId,
        cwd: &Path,
    ) -> Result<(SessionRef, Transcript), MeshError> {
        let transport = self.transport(agent)?;
        let cwd = resolve_cwd(cwd)?;

        // Reuse an existing mesh entry for this vendor session rather than minting a second ref
        // that would race with the first over one conversation.
        let session = match self.registry.find_by_vendor(agent, vendor) {
            Some(existing) => existing.session,
            None => self
                .registry
                .register_existing(agent.clone(), cwd.clone(), vendor.clone()),
        };

        let attached = self
            .admitted(transport.would_spawn(&cwd).await, transport.attach(vendor, &cwd))
            .await?;
        self.registry.mark_live(&session, attached.vendor)?;
        Ok((session, attached.replayed))
    }

    pub fn sessions(&self, agent: Option<&AgentId>) -> Vec<SessionEntry> {
        self.registry.list(agent)
    }

    /// Sessions the agent itself knows about, including any the mesh never created.
    pub async fn discover(
        &self,
        agent: &AgentId,
        cwd: &Path,
    ) -> Result<Vec<VendorSessionId>, MeshError> {
        let transport = self.transport(agent)?;
        let cwd = resolve_cwd(cwd)?;
        self.admitted(
            transport.discovery_spawns(&cwd).await,
            transport.list_sessions(&cwd),
        )
        .await
    }

    /// Send a prompt into a session and return the agent's reply.
    ///
    /// `chain` carries the sessions already visited by this ask so a cycle is refused rather than
    /// recursing; a caller with no chain passes `AskChain::root()`.
    pub async fn ask(
        &self,
        session: &SessionRef,
        prompt: &str,
        chain: &AskChain,
    ) -> Result<(Reply, AskChain), MeshError> {
        let started = Instant::now();
        let next = chain.push(session, self.max_ask_depth).map_err(|why| {
            if let Ok(entry) = self.registry.get(session) {
                mesh_telemetry::record_ask(&entry.agent, AskOutcome::Refused, started.elapsed());
            }
            MeshError::AskRefused {
                reason: match why {
                    ChainRejection::SelfAsk { session } => format!(
                        "session {session} is asking itself, which would recurse without progress"
                    ),
                    ChainRejection::TooDeep { limit } => format!(
                        "this relay already passed through {limit} sessions, the configured \
                         max_ask_depth; raise it in agents.toml if the chain is legitimate"
                    ),
                },
            }
        })?;

        let entry = self.registry.get(session)?;
        let transport = self.transport(&entry.agent)?;

        // Bring the session to a promptable state. Which step is needed depends on whether the
        // vendor session exists yet and whether anything is attached to it.
        let vendor = match self.registry.route(session)? {
            Route::Create { cwd } => {
                let opened = self.admitted(transport.would_spawn(&cwd).await, transport.open(&cwd)).await?;
                self.registry.mark_live(session, opened.vendor.clone())?;
                opened.vendor
            }
            Route::PromptDirect { vendor } => vendor,
            Route::ReattachThenPrompt { vendor, cwd } => {
                let attached = self
                    .admitted(transport.would_spawn(&cwd).await, transport.attach(&vendor, &cwd))
                    .await?;
                self.registry.mark_live(session, attached.vendor.clone())?;
                attached.vendor
            }
        };

        let outcome = tokio::time::timeout(self.turn_timeout, transport.prompt(&vendor, prompt))
            .await
            .map_err(|_| {
                // The process may still be mid-turn; mark it detached so the next ask reattaches
                // rather than assuming a live connection.
                let _ = self.registry.mark_detached(session);
                mesh_telemetry::record_ask(&entry.agent, AskOutcome::Timeout, started.elapsed());
                TransportError::Timeout {
                    agent: entry.agent.clone(),
                    seconds: self.turn_timeout.as_secs(),
                }
            })?;

        let reply = match outcome {
            Ok(reply) => reply,
            Err(err) => {
                mesh_telemetry::record_ask(&entry.agent, AskOutcome::AgentError, started.elapsed());
                return Err(err.into());
            }
        };

        self.usage.record(&entry.agent, &reply);
        mesh_telemetry::record_ask(&entry.agent, AskOutcome::Success, started.elapsed());
        Ok((reply, next))
    }

    /// The transcript of a session, read from whichever source the vendor exposes.
    pub async fn read_session(&self, session: &SessionRef) -> Result<Transcript, MeshError> {
        let entry = self.registry.get(session)?;
        let transport = self.transport(&entry.agent)?;

        let Some(vendor) = entry.state.vendor() else {
            // Never started: an empty transcript is the honest answer.
            return Ok(Transcript::default());
        };

        let attached = self
            .admitted(
                transport.would_spawn(&entry.cwd).await,
                transport.attach(vendor, &entry.cwd),
            )
            .await?;
        self.registry.mark_live(session, attached.vendor)?;
        Ok(attached.replayed)
    }
}

/// Is this agent's executable actually on PATH? Reported by `list_agents` so a caller learns an
/// agent is unusable before trying to prompt it.
pub fn is_installed(config: &Config, agent: &AgentId) -> bool {
    config
        .agents
        .get(agent.as_str())
        .map(|cfg| which(cfg.command()))
        .unwrap_or(false)
}

fn build_transport(agent: &AgentId, cfg: &AgentConfig) -> Arc<dyn AgentTransport> {
    match cfg {
        AgentConfig::Acp {
            command,
            args,
            model,
            ..
        } => Arc::new(AcpTransport::new(AcpLaunch {
            agent: agent.clone(),
            program: command.clone(),
            args: args.clone(),
            model: model.clone(),
        })),
        AgentConfig::Claude { command, model, .. } => Arc::new(ClaudeTransport::new(ClaudeLaunch {
            agent: agent.clone(),
            program: command.clone(),
            model: model.clone(),
        })),
        AgentConfig::Codex { command, .. } => {
            Arc::new(CodexTransport::new(agent.clone(), command.clone()))
        }
    }
}

/// ACP requires an absolute cwd, and a relative one would silently resolve against the mesh
/// process's directory rather than the caller's.
fn resolve_cwd(cwd: &Path) -> Result<PathBuf, MeshError> {
    mesh_core::absolute_cwd(cwd).map_err(|source| MeshError::BadCwd {
        cwd: cwd.display().to_string(),
        source,
    })
}

fn which(command: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&paths).any(|dir| {
        let candidate = dir.join(command);
        std::fs::metadata(&candidate)
            .map(|m| m.is_file())
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mesh() -> Mesh {
        Mesh::from_config(&Config::default_agents())
    }

    #[test]
    fn unknown_agent_lists_what_is_available() {
        let mesh = mesh();
        let outcome = mesh.open_session(&AgentId::new("nonexistent"), Path::new("/tmp"));

        match outcome {
            Err(MeshError::UnknownAgent { available, .. }) => {
                assert!(available.contains("opencode"), "got: {available}");
            }
            other => panic!("expected UnknownAgent, got {other:?}"),
        }
    }

    #[test]
    fn opening_a_session_registers_it_without_starting_a_process() {
        let mesh = mesh();
        let agent = AgentId::new("opencode");

        let session = mesh
            .open_session(&agent, Path::new("/tmp"))
            .expect("should register");

        let listed = mesh.sessions(Some(&agent));
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session, session);
        assert_eq!(listed[0].state, mesh_core::SessionState::NotStarted);
    }

    /// A relative cwd must be rejected or resolved, never passed through: ACP requires absolute
    /// paths and would otherwise resolve against the wrong directory.
    #[test]
    fn relative_cwd_is_resolved_to_absolute() {
        let mesh = mesh();
        let session = mesh
            .open_session(&AgentId::new("codex"), Path::new("."))
            .expect("cwd `.` exists so it resolves");

        let entry = mesh.sessions(None).into_iter().find(|e| e.session == session);
        assert!(
            entry.expect("registered").cwd.is_absolute(),
            "stored cwd must be absolute"
        );
    }

    #[test]
    fn nonexistent_cwd_is_an_error() {
        let mesh = mesh();
        let outcome = mesh.open_session(
            &AgentId::new("codex"),
            Path::new("/definitely/not/a/real/dir/xyz"),
        );
        assert!(matches!(outcome, Err(MeshError::BadCwd { .. })));
    }

    /// The guard must fire before any process is spawned, so a bad ask costs nothing.
    #[tokio::test]
    async fn ask_refuses_a_self_ask_without_touching_the_agent() {
        let mesh = mesh();
        let session = mesh
            .open_session(&AgentId::new("opencode"), Path::new("/tmp"))
            .expect("register");

        // A chain that already visited this session.
        let chain = AskChain::root()
            .push(&session, 8)
            .expect("first hop is fine");

        let outcome = mesh.ask(&session, "hello", &chain).await;

        match outcome {
            Err(MeshError::AskRefused { reason }) => {
                assert!(reason.contains("asking itself"), "got: {reason}");
            }
            other => panic!("expected AskRefused, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ask_refuses_when_chain_is_too_deep() {
        let config = Config {
            max_ask_depth: 1,
            ..Config::default_agents()
        };
        let mesh = Mesh::from_config(&config);
        let a = mesh
            .open_session(&AgentId::new("opencode"), Path::new("/tmp"))
            .expect("register");
        let b = mesh
            .open_session(&AgentId::new("codex"), Path::new("/tmp"))
            .expect("register");

        let chain = AskChain::root().push(&a, 1).expect("one hop allowed");
        let outcome = mesh.ask(&b, "hello", &chain).await;

        match outcome {
            Err(MeshError::AskRefused { reason }) => {
                assert!(reason.contains("max_ask_depth"), "got: {reason}");
            }
            other => panic!("expected AskRefused, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn asking_an_unknown_session_is_an_error() {
        let mesh = mesh();
        let bogus = SessionRef::parse("opencode:nope");

        let outcome = mesh.ask(&bogus, "hello", &AskChain::root()).await;

        assert!(matches!(
            outcome,
            Err(MeshError::Transport(TransportError::UnknownSession { .. }))
        ));
    }

    /// A session that never started has no transcript; reading it must not spawn a process or
    /// invent content.
    #[tokio::test]
    async fn reading_an_unstarted_session_returns_empty() {
        let mesh = mesh();
        let session = mesh
            .open_session(&AgentId::new("opencode"), Path::new("/tmp"))
            .expect("register");

        let transcript = mesh.read_session(&session).await.expect("should succeed");

        assert!(transcript.is_empty());
    }

    /// After the hub stops an agent's processes (kill or idle reap), its sessions must come back as
    /// detached, so the next ask resumes them rather than writing to a pipe nobody reads.
    #[tokio::test]
    async fn stopping_an_agent_detaches_its_sessions_for_reattach() {
        let mesh = mesh();
        let session = mesh
            .open_session(&AgentId::new("claude"), Path::new("/tmp"))
            .expect("register");
        mesh.registry
            .mark_live(&session, VendorSessionId::new("v1"))
            .expect("mark live");

        mesh.shutdown_all().await;

        assert_eq!(
            mesh.registry.route(&session).expect("route"),
            Route::ReattachThenPrompt {
                vendor: VendorSessionId::new("v1"),
                cwd: PathBuf::from("/tmp").canonicalize().expect("tmp exists"),
            }
        );
    }

    #[test]
    fn every_configured_agent_gets_a_transport() {
        let mesh = mesh();
        let names: Vec<_> = mesh.agents().map(|(id, _)| id.to_string()).collect();
        for expected in ["claude", "codex", "opencode", "gemini", "grok"] {
            assert!(names.contains(&expected.to_owned()), "missing {expected}");
        }
    }

    #[test]
    fn capabilities_reflect_which_agents_report_cost() {
        let mesh = mesh();
        let caps: BTreeMap<_, _> = mesh
            .agents()
            .map(|(id, c)| (id.to_string(), c))
            .collect();

        // Only claude reports spend among the wired agents.
        assert!(caps["claude"].reports_cost);
        assert!(!caps["codex"].reports_cost);
        assert!(!caps["opencode"].reports_cost);
        // Resume is what makes cross-agent reach possible; all three must have it.
        assert!(caps["claude"].resume && caps["codex"].resume && caps["opencode"].resume);
    }
}
