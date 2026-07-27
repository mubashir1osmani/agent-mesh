//! agent-mesh: an MCP server that lets coding agents talk to each other's sessions.

mod config;
mod mesh;
mod tools;

use config::Config;
use mesh::Mesh;
use rmcp::ServiceExt;
use std::path::PathBuf;
use std::sync::Arc;
use tools::MeshServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logs go to stderr: stdout is the MCP transport, so anything written there corrupts the
    // protocol stream.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("AGENT_MESH_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Arc::new(load_config()?);
    tracing::info!(
        agents = config.agent_ids().count(),
        max_ask_depth = config.max_ask_depth,
        turn_timeout_seconds = config.turn_timeout_seconds,
        "agent-mesh starting"
    );

    let mesh = Arc::new(Mesh::from_config(&config));
    let server = MeshServer::new(mesh, config);

    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

/// Config search order: `$AGENT_MESH_CONFIG`, then `./agents.toml`, then
/// `~/.config/agent-mesh/agents.toml`, then the built-in defaults so the server works with no
/// setup at all.
fn load_config() -> Result<Config, config::ConfigError> {
    for candidate in candidates() {
        if candidate.is_file() {
            tracing::info!(path = %candidate.display(), "loading config");
            return Config::load(&candidate);
        }
    }
    tracing::info!("no config file found; using built-in agent registry");
    Ok(Config::default_agents())
}

fn candidates() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(explicit) = std::env::var_os("AGENT_MESH_CONFIG") {
        paths.push(PathBuf::from(explicit));
    }
    paths.push(PathBuf::from("agents.toml"));
    if let Some(home) = std::env::var_os("HOME") {
        paths.push(
            PathBuf::from(home)
                .join(".config")
                .join("agent-mesh")
                .join("agents.toml"),
        );
    }
    paths
}
