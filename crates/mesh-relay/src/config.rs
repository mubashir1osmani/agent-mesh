//! Process configuration. Every secret comes from the environment; nothing here has a default key.

use std::path::PathBuf;

/// Stripe Checkout, used only when `CREWMESH_STRIPE_SECRET_KEY` is set.
#[derive(Clone, Debug)]
pub struct StripeConfig {
    pub secret_key: String,
    pub webhook_secret: String,
    pub price_monthly: String,
    pub price_yearly: String,
    pub success_url: String,
    pub cancel_url: String,
    /// `https://api.stripe.com` in production. Tests point this at a local stand-in.
    pub api_base: String,
}

/// How this process is bound and how it bills.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind: String,
    pub public_url: String,
    /// When set, accounts, device codes, and checkout sessions are stored here (mode 0600).
    /// Unset means memory only: a restart logs everyone out.
    pub data_path: Option<PathBuf>,
    pub upgrade_url: String,
    /// Sent to clients as `cache_ttl_seconds`.
    pub cache_ttl_seconds: u64,
    pub stripe: Option<StripeConfig>,
}

impl ServerConfig {
    pub fn billing_mode(&self) -> &'static str {
        if self.stripe.is_some() {
            "stripe"
        } else {
            "stub"
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let bind = env_or("CREWMESH_RELAY_BIND", "127.0.0.1:8787");
        let port = bind.rsplit(':').next().unwrap_or("8787");
        let public_url = env_nonempty("CREWMESH_PUBLIC_URL")
            .unwrap_or_else(|| format!("http://127.0.0.1:{port}"));
        let upgrade_url = env_or("CREWMESH_UPGRADE_URL", "https://crewmesh.dev/pro");
        let cache_ttl_seconds = env_u64("CREWMESH_ENTITLEMENT_TTL_SECONDS", 900)?;
        let data_path = env_nonempty("CREWMESH_RELAY_DATA").map(PathBuf::from);
        let stripe = match env_nonempty("CREWMESH_STRIPE_SECRET_KEY") {
            None => None,
            Some(secret_key) => Some(StripeConfig {
                secret_key,
                webhook_secret: required("CREWMESH_STRIPE_WEBHOOK_SECRET")?,
                price_monthly: required("CREWMESH_STRIPE_PRICE_MONTHLY")?,
                price_yearly: required("CREWMESH_STRIPE_PRICE_YEARLY")?,
                success_url: env_nonempty("CREWMESH_STRIPE_SUCCESS_URL")
                    .unwrap_or_else(|| format!("{public_url}/billing/success")),
                cancel_url: env_nonempty("CREWMESH_STRIPE_CANCEL_URL")
                    .unwrap_or_else(|| format!("{public_url}/billing/cancel")),
                api_base: env_or("CREWMESH_STRIPE_API_BASE", "https://api.stripe.com"),
            }),
        };
        Ok(Self {
            bind,
            public_url,
            data_path,
            upgrade_url,
            cache_ttl_seconds,
            stripe,
        })
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn env_or(name: &str, default: &str) -> String {
    env_nonempty(name).unwrap_or_else(|| default.to_owned())
}

fn required(name: &str) -> Result<String, String> {
    env_nonempty(name)
        .ok_or_else(|| format!("{name} is required when CREWMESH_STRIPE_SECRET_KEY is set"))
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match env_nonempty(name) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .map_err(|_| format!("{name} must be an integer, got `{value}`")),
    }
}
