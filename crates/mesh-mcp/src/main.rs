//! agent-mesh: an MCP server that lets coding agents talk to each other's sessions.

mod client;
mod config;
mod hook;
mod hub;
mod mesh;
mod ps;
mod tmux;
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
  agent-mesh hub             Run the shared hub in the foreground (normally auto-started)
  agent-mesh ps              List every agent process the hub is running, with memory
  agent-mesh kill <id>       Stop one of them (an id from `ps`)
  agent-mesh kill --all      Stop every agent process the mesh spawned
  agent-mesh hook <event>    Claude Code hook: deliver queued mesh messages
                             (<event> is user-prompt-submit or stop)
  agent-mesh --version       Print the version and exit
  agent-mesh --help          Print this message and exit

Configuration is read from $AGENT_MESH_CONFIG, ./agents.toml, or
~/.config/agent-mesh/agents.toml. With none of those, a built-in agent registry is used.
The hub reads it once, from your home directory, and it applies to every agent on the mesh.

Set AGENT_MESH_LOG=debug for verbose logging (always on stderr, never stdout).
";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Handled before anything else: an MCP client launches this with no arguments, so anything
    // here came from a human at a terminal.
    let mut args = std::env::args().skip(1);
    if let Some(arg) = args.next() {
        match arg.as_str() {
            "hub" => return run_hub().await,
            "ps" => return ps::ps().await,
            "kill" => return ps::kill(args.next()).await,
            "hook" => {
                hook::run(args.next().as_deref().unwrap_or_default()).await;
                return Ok(());
            }
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

    let identity = Arc::new(client::Identity::detect().await);
    if let Some(node) = &identity.node {
        // Join the mesh now so this session shows up in list_nodes before it calls any tool. Every
        // tool goes through the hub, so a failure here is logged and retried on the next call.
        match client::call(&hub::Request::Register { node: node.clone() }).await {
            Ok(_) => tracing::info!(node = %node.id, "registered with hub"),
            Err(err) => tracing::warn!(%err, "could not reach the hub; tools will retry"),
        }
    }

    // Capabilities are static, so they come from a local, process-free Mesh; every session and
    // process lives in the hub.
    let agents = Mesh::from_config(&config).agents().map(|(a, c)| (a.clone(), c)).collect();
    let server = MeshServer::new(agents, config, identity);

    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn run_hub() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(load_config()?);
    let _telemetry = mesh_telemetry::init(&config.telemetry)?;
    hub::serve(config).await?;
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
