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

const USAGE: &str = "\
agent-mesh -- an MCP control plane that lets coding agents talk to each other's sessions

Usage:
  agent-mesh                 Serve MCP over stdio (how MCP clients launch it)
  agent-mesh --version       Print the version and exit
  agent-mesh --help          Print this message and exit

Configuration is read from $AGENT_MESH_CONFIG, ./agents.toml, or
~/.config/agent-mesh/agents.toml. With none of those, a built-in agent registry is used.

Set AGENT_MESH_LOG=debug for verbose logging (always on stderr, never stdout).
";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Handled before anything else: an MCP client launches this with no arguments, so anything
    // here came from a human at a terminal.
    if let Some(arg) = std::env::args().nth(1) {
        match arg.as_str() {
            "--version" | "-V" => println!("agent-mesh {}", env!("CARGO_PKG_VERSION")),
            "--help" | "-h" => print!("{USAGE}"),
            other => {
                eprintln!("agent-mesh: unrecognized argument `{other}`\n\n{USAGE}");
                std::process::exit(2);
            }
        }
        return Ok(());
    }

    let config = Arc::new(load_config()?);

    // Sets up stderr logging plus any configured exporters. Logs must never touch stdout: that is
    // the MCP transport, and a stray line there corrupts the protocol stream.
    let _telemetry = mesh_telemetry::init(&config.telemetry)?;
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
