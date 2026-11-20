//! `agent-mesh-relay` — the hosted side of Crewmesh Pro.
//!
//! With `CREWMESH_STRIPE_SECRET_KEY` unset this process runs in stub billing mode: checkout does
//! not contact Stripe, and `POST /v1/stripe/webhook` accepts unsigned events. Set the Stripe
//! variables before exposing it beyond localhost. See docs/pro.md.

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("AGENT_MESH_LOG"))
        .unwrap_or_else(|_| "info".to_owned());
    let filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match mesh_relay::ServerConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("agent-mesh-relay: {err}");
            std::process::exit(2);
        }
    };
    if let Err(err) = mesh_relay::serve(config).await {
        eprintln!("agent-mesh-relay: {err}");
        std::process::exit(1);
    }
}
