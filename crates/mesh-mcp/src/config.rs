//! Agent registry configuration.
//!
//! Agents are declared in TOML rather than hardcoded, so adding one of the remaining ACP agents
//! (gemini, grok, cursor-agent) is a config change instead of a code change.

use mesh_core::AgentId;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Maximum length of an ask chain before it is refused, guarding against agents relaying to
    /// each other indefinitely.
    #[serde(default = "default_max_chain")]
    pub max_ask_depth: usize,

    /// How long to wait for one agent turn before giving up, so a wedged peer cannot hang the
    /// caller forever.
    #[serde(default = "default_timeout")]
    pub turn_timeout_seconds: u64,

    #[serde(default)]
    pub agents: BTreeMap<String, AgentConfig>,

    #[serde(default)]
    pub telemetry: mesh_telemetry::TelemetryConfig,
}

fn default_max_chain() -> usize {
    4
}

fn default_timeout() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum AgentConfig {
    /// Any agent speaking the Agent Client Protocol.
    Acp {
        /// Executable, e.g. `opencode`.
        command: String,
        /// Arguments that put it into ACP mode, e.g. `["acp"]`.
        args: Vec<String>,
        #[serde(default)]
        model: Option<String>,
        #[serde(default = "enabled_by_default")]
        enabled: bool,
    },
    /// `claude` over its stream-json stdio loop.
    Claude {
        #[serde(default = "claude_command")]
        command: String,
        #[serde(default)]
        model: Option<String>,
        #[serde(default = "enabled_by_default")]
        enabled: bool,
    },
    /// `codex app-server`.
    Codex {
        #[serde(default = "codex_command")]
        command: String,
        #[serde(default = "enabled_by_default")]
        enabled: bool,
    },
}

fn enabled_by_default() -> bool {
    true
}

fn claude_command() -> String {
    "claude".to_owned()
}

fn codex_command() -> String {
    "codex".to_owned()
}

impl AgentConfig {
    pub fn enabled(&self) -> bool {
        match self {
            Self::Acp { enabled, .. } | Self::Claude { enabled, .. } | Self::Codex { enabled, .. } => {
                *enabled
            }
        }
    }

    /// The executable this agent needs on PATH.
    pub fn command(&self) -> &str {
        match self {
            Self::Acp { command, .. } | Self::Claude { command, .. } | Self::Codex { command, .. } => {
                command
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("could not read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("could not parse {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.display().to_string(),
            source,
        })
    }

    /// The built-in registry, used when no config file is present so the server is useful with
    /// zero setup.
    pub fn default_agents() -> Self {
        Self {
            max_ask_depth: default_max_chain(),
            turn_timeout_seconds: default_timeout(),
            telemetry: mesh_telemetry::TelemetryConfig::default(),
            agents: BTreeMap::from([
                (
                    "opencode".to_owned(),
                    AgentConfig::Acp {
                        command: "opencode".to_owned(),
                        args: vec!["acp".to_owned()],
                        model: None,
                        enabled: true,
                    },
                ),
                (
                    "gemini".to_owned(),
                    AgentConfig::Acp {
                        command: "gemini".to_owned(),
                        args: vec!["--acp".to_owned()],
                        model: None,
                        enabled: true,
                    },
                ),
                (
                    "grok".to_owned(),
                    AgentConfig::Acp {
                        command: "grok".to_owned(),
                        args: vec!["agent".to_owned(), "stdio".to_owned()],
                        model: None,
                        enabled: true,
                    },
                ),
                (
                    "claude".to_owned(),
                    AgentConfig::Claude {
                        command: claude_command(),
                        model: None,
                        enabled: true,
                    },
                ),
                (
                    "codex".to_owned(),
                    AgentConfig::Codex {
                        command: codex_command(),
                        enabled: true,
                    },
                ),
            ]),
        }
    }

    pub fn agent_ids(&self) -> impl Iterator<Item = AgentId> + '_ {
        self.agents
            .iter()
            .filter(|(_, cfg)| cfg.enabled())
            .map(|(name, _)| AgentId::new(name.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_config() {
        let toml = r#"
            max_ask_depth = 3
            turn_timeout_seconds = 90

            [agents.opencode]
            transport = "acp"
            command = "opencode"
            args = ["acp"]
            model = "opencode/deepseek-v4-flash-free"

            [agents.claude]
            transport = "claude"
            model = "claude-haiku-4-5-20251001"

            [agents.codex]
            transport = "codex"

            [agents.gemini]
            transport = "acp"
            command = "gemini"
            args = ["--acp"]
            enabled = false
        "#;

        let cfg: Config = toml::from_str(toml).expect("valid config");

        assert_eq!(cfg.max_ask_depth, 3);
        assert_eq!(cfg.turn_timeout_seconds, 90);
        assert_eq!(cfg.agents.len(), 4);
        // A disabled agent must be excluded from the usable set.
        let ids: Vec<_> = cfg.agent_ids().map(|a| a.to_string()).collect();
        assert_eq!(ids, vec!["claude", "codex", "opencode"]);
    }

    /// Defaults must exist, or a minimal config would leave the loop guard and timeout unset.
    #[test]
    fn defaults_fill_in_when_omitted() {
        let cfg: Config = toml::from_str(
            r#"
            [agents.codex]
            transport = "codex"
        "#,
        )
        .expect("valid config");

        assert_eq!(cfg.max_ask_depth, 4);
        assert_eq!(cfg.turn_timeout_seconds, 300);
        assert_eq!(cfg.agents["codex"].command(), "codex");
        assert!(cfg.agents["codex"].enabled());
    }

    #[test]
    fn unknown_transport_is_rejected_rather_than_ignored() {
        let outcome: Result<Config, _> = toml::from_str(
            r#"
            [agents.mystery]
            transport = "telepathy"
        "#,
        );
        assert!(outcome.is_err(), "an unknown transport must not parse");
    }

    #[test]
    fn builtin_registry_covers_every_wired_agent() {
        let cfg = Config::default_agents();
        let ids: Vec<_> = cfg.agent_ids().map(|a| a.to_string()).collect();
        assert!(ids.contains(&"opencode".to_owned()));
        assert!(ids.contains(&"claude".to_owned()));
        assert!(ids.contains(&"codex".to_owned()));
    }
}
