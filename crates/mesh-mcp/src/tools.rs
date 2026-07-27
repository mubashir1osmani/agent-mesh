//! The MCP tool surface agents call.
//!
//! Tool descriptions are written for the *agent* reading them, not for a human skimming docs: an
//! agent decides whether to call `ask_agent` purely from this text.

use crate::config::Config;
use crate::mesh::{Mesh, MeshError};
use mesh_core::{AgentId, AskChain, SessionRef, Speaker, VendorSessionId};
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::{ErrorData, tool, tool_router};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

pub struct MeshServer {
    mesh: Arc<Mesh>,
    config: Arc<Config>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct OpenSessionArgs {
    /// Which agent to open a session with, as reported by `list_agents`.
    pub agent: String,
    /// Absolute path the agent should treat as its working directory.
    pub cwd: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AttachSessionArgs {
    /// Which agent owns the session.
    pub agent: String,
    /// The agent's own session id (e.g. an opencode `ses_...` or a codex thread uuid).
    pub session_id: String,
    /// Absolute working directory the session belongs to.
    pub cwd: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AskArgs {
    /// A `session` value returned by `open_session`, `attach_session`, or `list_sessions`.
    pub session: String,
    /// What to say to that agent.
    pub prompt: String,
    /// Sessions this ask has already passed through, from a previous `ask_agent` result. Pass it
    /// back when relaying so the mesh can refuse a loop.
    #[serde(default)]
    pub via: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadSessionArgs {
    /// A `session` value returned by `open_session`, `attach_session`, or `list_sessions`.
    pub session: String,
    /// Return only the last N turns. Omit for the whole conversation.
    #[serde(default)]
    pub last: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListSessionsArgs {
    /// Restrict to one agent. Omit to list every session the mesh knows.
    #[serde(default)]
    pub agent: Option<String>,
    /// Also ask the agent itself which sessions exist, finding conversations the mesh did not
    /// create. Requires `cwd`.
    #[serde(default)]
    pub discover_in: Option<String>,
}

/// MCP requires a tool's output schema to have an object at its root, so list-shaped results are
/// wrapped rather than returned as bare arrays.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct AgentList {
    pub agents: Vec<AgentInfo>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SessionList {
    pub sessions: Vec<SessionInfo>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct AgentInfo {
    pub agent: String,
    /// False when the agent's executable is not on PATH, so it cannot be used.
    pub installed: bool,
    /// Whether a session can be reached from a new process. Without this the mesh cannot bridge
    /// into the agent's conversations.
    pub can_resume: bool,
    /// Whether the agent reports what a turn cost.
    pub reports_cost: bool,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SessionInfo {
    /// Pass this back as `session` to other tools.
    pub session: String,
    pub agent: String,
    pub cwd: String,
    /// `not_started`, `live`, or `detached`.
    pub state: String,
    /// The agent's own id for this session, once one exists.
    pub agent_session_id: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct AskResult {
    /// What the agent said.
    pub reply: String,
    pub agent: String,
    // Serialized as plain JSON integers. `u64` would emit `"format": "uint64"`, which is not a
    // standard JSON Schema format and makes MCP clients log a warning on every connect.
    #[schemars(with = "i64")]
    pub input_tokens: u64,
    #[schemars(with = "i64")]
    pub output_tokens: u64,
    /// Cost in USD, present only for agents that report spend. Absent means unreported, not free.
    pub cost_usd: Option<f64>,
    /// The updated ask chain. Pass as `via` if this agent's answer causes you to ask another
    /// agent, so the mesh can detect a loop.
    pub via: Vec<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct TurnInfo {
    /// `user`, `agent`, or `agent_thought`.
    pub speaker: String,
    pub text: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct TranscriptResult {
    pub agent: String,
    pub turns: Vec<TurnInfo>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SessionOpened {
    /// Pass this as `session` to `ask_agent` and `read_session`.
    pub session: String,
    pub agent: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct SessionAttached {
    pub session: String,
    pub agent: String,
    /// Conversation recovered from the agent, so the caller can see the context it just joined.
    pub turns: Vec<TurnInfo>,
}

#[tool_router(server_handler)]
impl MeshServer {
    pub fn new(mesh: Arc<Mesh>, config: Arc<Config>) -> Self {
        Self { mesh, config }
    }

    #[tool(
        name = "list_agents",
        description = "List the coding agents this mesh can reach (claude, codex, opencode, \
                       gemini, grok). Call this first to see which agents are installed and \
                       whether their sessions can be resumed. Returns one entry per agent."
    )]
    fn list_agents(&self) -> Json<AgentList> {
        Json(AgentList {
            agents: self
                .mesh
                .agents()
                .map(|(agent, caps)| AgentInfo {
                    agent: agent.to_string(),
                    installed: self.mesh.is_installed(&self.config, agent),
                    can_resume: caps.resume,
                    reports_cost: caps.reports_cost,
                })
                .collect(),
        })
    }

    #[tool(
        name = "open_session",
        description = "Start a new conversation with another agent, rooted at a working \
                       directory. Returns a session handle to pass to ask_agent. The agent \
                       process starts on the first ask_agent call, not here."
    )]
    fn open_session(
        &self,
        Parameters(args): Parameters<OpenSessionArgs>,
    ) -> Result<Json<SessionOpened>, ErrorData> {
        let agent = AgentId::new(args.agent.as_str());
        let session = self
            .mesh
            .open_session(&agent, &PathBuf::from(&args.cwd))
            .map_err(to_mcp_error)?;
        Ok(Json(SessionOpened {
            session: session.to_string(),
            agent: agent.to_string(),
        }))
    }

    #[tool(
        name = "attach_session",
        description = "Join a conversation that already exists inside an agent, using that \
                       agent's own session id. Returns a session handle plus the conversation \
                       so far, so you can read what was already discussed before continuing it. \
                       Use list_sessions with discover_in to find ids."
    )]
    async fn attach_session(
        &self,
        Parameters(args): Parameters<AttachSessionArgs>,
    ) -> Result<Json<SessionAttached>, ErrorData> {
        let agent = AgentId::new(args.agent.as_str());
        let (session, transcript) = self
            .mesh
            .attach_session(
                &agent,
                &VendorSessionId::new(args.session_id.as_str()),
                &PathBuf::from(&args.cwd),
            )
            .await
            .map_err(to_mcp_error)?;

        Ok(Json(SessionAttached {
            session: session.to_string(),
            agent: agent.to_string(),
            turns: to_turns(&transcript, None),
        }))
    }

    #[tool(
        name = "ask_agent",
        description = "Send a prompt into another agent's session and wait for its reply. This \
                       is how you consult a peer: the target agent answers with its own model \
                       and its own conversation context intact. Returns the reply, token usage, \
                       and a `via` chain to pass along if the answer leads you to ask yet \
                       another agent."
    )]
    async fn ask_agent(
        &self,
        Parameters(args): Parameters<AskArgs>,
    ) -> Result<Json<AskResult>, ErrorData> {
        let session = SessionRef::parse(args.session.as_str());
        let chain = AskChain::from_hops(args.via.iter().map(|h| SessionRef::parse(h.as_str())));

        let (reply, next) = self
            .mesh
            .ask(&session, &args.prompt, &chain)
            .await
            .map_err(to_mcp_error)?;

        let agent = self
            .mesh
            .sessions(None)
            .into_iter()
            .find(|e| e.session == session)
            .map(|e| e.agent.to_string())
            .unwrap_or_default();

        Ok(Json(AskResult {
            reply: reply.text,
            agent,
            input_tokens: reply.usage.input_tokens,
            output_tokens: reply.usage.output_tokens,
            cost_usd: reply.cost.map(|c| c.as_usd()),
            via: next.hops().iter().map(SessionRef::to_string).collect(),
        }))
    }

    #[tool(
        name = "read_session",
        description = "Read another agent's conversation without prompting it. Use this to see \
                       what a peer has already done or concluded before deciding what to ask."
    )]
    async fn read_session(
        &self,
        Parameters(args): Parameters<ReadSessionArgs>,
    ) -> Result<Json<TranscriptResult>, ErrorData> {
        let session = SessionRef::parse(args.session.as_str());
        let transcript = self
            .mesh
            .read_session(&session)
            .await
            .map_err(to_mcp_error)?;

        let agent = self
            .mesh
            .sessions(None)
            .into_iter()
            .find(|e| e.session == session)
            .map(|e| e.agent.to_string())
            .unwrap_or_default();

        Ok(Json(TranscriptResult {
            agent,
            turns: to_turns(&transcript, args.last),
        }))
    }

    #[tool(
        name = "list_sessions",
        description = "List conversations the mesh knows about. Set discover_in to a directory \
                       to also ask an agent which sessions it has of its own, including ones \
                       started outside the mesh."
    )]
    async fn list_sessions(
        &self,
        Parameters(args): Parameters<ListSessionsArgs>,
    ) -> Result<Json<SessionList>, ErrorData> {
        let filter = args.agent.as_deref().map(AgentId::new);

        let known = self.mesh.sessions(filter.as_ref());
        let mut listed: Vec<SessionInfo> = known
            .iter()
            .map(|entry| SessionInfo {
                session: entry.session.to_string(),
                agent: entry.agent.to_string(),
                cwd: entry.cwd.display().to_string(),
                state: state_name(&entry.state),
                agent_session_id: entry.state.vendor().map(VendorSessionId::to_string),
            })
            .collect();

        // Discovery needs a specific agent to ask, since ids are per-agent.
        if let (Some(cwd), Some(agent)) = (args.discover_in.as_deref(), filter.as_ref()) {
            let found = self
                .mesh
                .discover(agent, &PathBuf::from(cwd))
                .await
                .map_err(to_mcp_error)?;
            let already: Vec<_> = known
                .iter()
                .filter_map(|e| e.state.vendor().map(VendorSessionId::to_string))
                .collect();
            listed.extend(
                found
                    .into_iter()
                    .filter(|v| !already.contains(&v.to_string()))
                    .map(|vendor| SessionInfo {
                        session: String::new(),
                        agent: agent.to_string(),
                        cwd: cwd.to_owned(),
                        state: "unattached".to_owned(),
                        agent_session_id: Some(vendor.to_string()),
                    }),
            );
        }

        Ok(Json(SessionList { sessions: listed }))
    }
}

fn state_name(state: &mesh_core::SessionState) -> String {
    match state {
        mesh_core::SessionState::NotStarted => "not_started",
        mesh_core::SessionState::Live { .. } => "live",
        mesh_core::SessionState::Detached { .. } => "detached",
    }
    .to_owned()
}

fn to_turns(transcript: &mesh_core::Transcript, last: Option<usize>) -> Vec<TurnInfo> {
    let turns = &transcript.turns;
    let start = last.map_or(0, |n| turns.len().saturating_sub(n));
    turns[start..]
        .iter()
        .map(|t| TurnInfo {
            speaker: match t.speaker {
                Speaker::User => "user",
                Speaker::Agent => "agent",
                Speaker::AgentThought => "agent_thought",
            }
            .to_owned(),
            text: t.text.clone(),
        })
        .collect()
}

/// Map a mesh failure onto MCP's error shape. Exhaustive so a new failure mode cannot silently
/// fall through to a generic message.
fn to_mcp_error(err: MeshError) -> ErrorData {
    let message = err.to_string();
    match err {
        // Caller passed something wrong; these are recoverable by asking differently.
        MeshError::UnknownAgent { .. } | MeshError::BadCwd { .. } | MeshError::AskRefused { .. } => {
            ErrorData::invalid_params(message, None)
        }
        MeshError::Transport(inner) => match inner {
            mesh_core::TransportError::UnknownSession { .. } => {
                ErrorData::invalid_params(message, None)
            }
            mesh_core::TransportError::ResumeUnsupported { .. }
            | mesh_core::TransportError::Unreachable { .. }
            | mesh_core::TransportError::Spawn { .. }
            | mesh_core::TransportError::ConnectionClosed { .. }
            | mesh_core::TransportError::Protocol { .. }
            | mesh_core::TransportError::AgentRefused { .. }
            | mesh_core::TransportError::Timeout { .. }
            | mesh_core::TransportError::Cancelled { .. }
            | mesh_core::TransportError::Decode { .. } => ErrorData::internal_error(message, None),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_core::{Transcript, Turn};

    fn transcript() -> Transcript {
        Transcript::from_turns([
            Turn { speaker: Speaker::User, text: "one".to_owned() },
            Turn { speaker: Speaker::Agent, text: "two".to_owned() },
            Turn { speaker: Speaker::User, text: "three".to_owned() },
        ])
    }

    #[test]
    fn last_n_returns_the_most_recent_turns() {
        let turns = to_turns(&transcript(), Some(2));
        assert_eq!(
            turns.iter().map(|t| t.text.as_str()).collect::<Vec<_>>(),
            vec!["two", "three"],
            "`last` must take from the end, not the start"
        );
    }

    /// Asking for more turns than exist must not panic on an out-of-range slice.
    #[test]
    fn last_larger_than_transcript_is_safe() {
        assert_eq!(to_turns(&transcript(), Some(99)).len(), 3);
        assert_eq!(to_turns(&Transcript::default(), Some(5)).len(), 0);
    }

    #[test]
    fn omitting_last_returns_everything() {
        assert_eq!(to_turns(&transcript(), None).len(), 3);
    }

    #[test]
    fn speaker_names_are_stable_for_agents_to_match_on() {
        let t = Transcript::from_turns([Turn {
            speaker: Speaker::AgentThought,
            text: "hmm".to_owned(),
        }]);
        assert_eq!(to_turns(&t, None)[0].speaker, "agent_thought");
    }

    /// A refused loop is the caller's mistake, so it must surface as invalid params rather than an
    /// internal error the agent cannot act on.
    #[test]
    fn ask_refused_maps_to_invalid_params() {
        let err = to_mcp_error(MeshError::AskRefused {
            reason: "would loop".to_owned(),
        });
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[test]
    fn unknown_agent_maps_to_invalid_params() {
        let err = to_mcp_error(MeshError::UnknownAgent {
            requested: "x".to_owned(),
            available: "y".to_owned(),
        });
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    /// A timeout is not something the caller can fix by changing arguments.
    #[test]
    fn timeout_maps_to_internal_error() {
        let err = to_mcp_error(MeshError::Transport(
            mesh_core::TransportError::Timeout {
                agent: AgentId::new("codex"),
                seconds: 30,
            },
        ));
        assert_eq!(err.code, rmcp::model::ErrorCode::INTERNAL_ERROR);
    }

    #[test]
    fn error_messages_name_the_agent_or_reason() {
        let err = to_mcp_error(MeshError::UnknownAgent {
            requested: "ghost".to_owned(),
            available: "claude, codex".to_owned(),
        });
        let text = err.message.to_string();
        assert!(text.contains("ghost") && text.contains("claude"), "got: {text}");
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    /// MCP requires every tool's `outputSchema` to have an object at its root. rmcp only discovers
    /// a violation when the tool is first listed at runtime, where it surfaces as a panic inside
    /// the serving task rather than a build failure, so assert it here instead.
    #[test]
    fn every_tool_output_schema_has_an_object_root() {
        let router = MeshServer::tool_router();

        let tools = router.list_all();
        assert!(!tools.is_empty(), "the router must expose tools");

        for tool in tools {
            let schema = tool
                .output_schema
                .as_ref()
                .unwrap_or_else(|| panic!("{} has no output schema", tool.name));
            let root = schema
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<missing>");
            assert_eq!(
                root, "object",
                "tool `{}` returns a root `{root}`; MCP requires an object, so wrap it in a struct",
                tool.name
            );
        }
    }

    /// The tool names agents are told to call must stay stable; renaming one silently breaks every
    /// agent configured against it.
    #[test]
    fn exposes_the_documented_tool_surface() {
        let names: Vec<String> = MeshServer::tool_router()
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect();

        for expected in [
            "list_agents",
            "open_session",
            "attach_session",
            "ask_agent",
            "read_session",
            "list_sessions",
        ] {
            assert!(names.contains(&expected.to_owned()), "missing tool {expected}");
        }
    }

    /// An agent picks a tool from its description alone, so an empty one makes the tool unusable.
    #[test]
    fn every_tool_has_a_description() {
        for tool in MeshServer::tool_router().list_all() {
            let described = tool.description.as_deref().unwrap_or("");
            assert!(
                described.len() > 30,
                "tool `{}` needs a description an agent can act on",
                tool.name
            );
        }
    }
}
