mod billing;
mod config;
mod routes;
mod state;

pub use billing::{
    WEBHOOK_TOLERANCE_SECONDS, checkout_form, stripe_signature_header, verify_stripe_signature,
};
pub use config::{ServerConfig, StripeConfig};
use std::sync::Arc;

pub fn app(config: ServerConfig) -> axum::Router {
    app_with_clock(config, Arc::new(routes::unix_now))
}

pub fn app_with_clock(
    config: ServerConfig,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> axum::Router {
    routes::build(config, now)
}

pub async fn serve(config: ServerConfig) -> Result<(), std::io::Error> {
    let bind = config.bind.clone();
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "crewmesh relay listening");
    axum::serve(listener, app(config)).await
}
