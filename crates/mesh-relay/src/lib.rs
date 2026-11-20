//! Crewmesh Pro relay.
//!
//! Local agent-mesh hubs speak newline-delimited JSON to each other on one machine. This process
//! is the hosted hop between machines. It also owns the billing seam: stub checkout when Stripe
//! is not configured, and a real Checkout Session plus a signed webhook when it is.
//!
//! See `docs/pro.md` for every environment variable.

mod billing;
mod config;
mod routes;
mod state;

pub use billing::{
    WEBHOOK_TOLERANCE_SECONDS, checkout_form, stripe_signature_header, verify_stripe_signature,
};
pub use config::{ServerConfig, StripeConfig};
use std::sync::Arc;

/// Router with the wall clock. Tests that need a fake clock use [`app_with_clock`].
pub fn app(config: ServerConfig) -> axum::Router {
    app_with_clock(config, Arc::new(routes::unix_now))
}

pub fn app_with_clock(
    config: ServerConfig,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> axum::Router {
    routes::build(config, now)
}

/// Bind `config.bind` and serve until the process is killed.
pub async fn serve(config: ServerConfig) -> Result<(), std::io::Error> {
    let bind = config.bind.clone();
    let mode = config.billing_mode().to_owned();
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, billing = %mode, "crewmesh relay listening");
    if mode == "stub" {
        tracing::warn!(
            "CREWMESH_STRIPE_SECRET_KEY is unset; checkout is stubbed and unsigned webhooks are \
             accepted. Do not expose this process to the internet."
        );
    }
    axum::serve(listener, app(config)).await
}
