//! Telemetry for the mesh: OpenTelemetry traces, per-agent usage and cost, and an optional
//! Prometheus scrape endpoint.
//!
//! Note the transport asymmetry, which drives the design here. agent-mesh normally runs over
//! stdio, which means one process *per MCP client* — so anything pull-based needs a stable port
//! that N concurrent processes cannot all bind. Traces and usage are therefore push-based and
//! always available, while the Prometheus listener is opt-in and fails loudly if its port is
//! already taken rather than silently reporting nothing.

pub mod metrics_names;
pub mod usage;

use mesh_core::AgentId;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use serde::Deserialize;
use std::net::SocketAddr;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub use usage::{AgentUsage, UsageRecorder};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TelemetryConfig {
    /// OTLP endpoint for trace export, e.g. `http://localhost:4317`. Absent disables tracing
    /// export; local `tracing` logs to stderr are unaffected.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// Address for a Prometheus scrape endpoint, e.g. `127.0.0.1:9464`.
    ///
    /// Off by default. Under stdio each MCP client spawns its own agent-mesh process, so only one
    /// can hold the port; enable this when running a single long-lived instance.
    #[serde(default)]
    pub prometheus_listen: Option<String>,

    /// Value reported as `service.name` on exported spans.
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

fn default_service_name() -> String {
    "agent-mesh".to_owned()
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("`{value}` is not a valid listen address: {source}")]
    BadAddress {
        value: String,
        #[source]
        source: std::net::AddrParseError,
    },

    /// Surfaced rather than swallowed: a metrics endpoint that silently failed to bind looks
    /// identical to one reporting nothing, which is the worst possible failure mode for telemetry.
    #[error("could not start the Prometheus endpoint on {addr}: {reason}")]
    PrometheusBind { addr: SocketAddr, reason: String },

    #[error("could not initialise the OTLP trace exporter: {reason}")]
    OtlpInit { reason: String },
}

/// Guard whose drop flushes pending spans. Held for the process lifetime; dropping it early loses
/// any spans still buffered in the exporter.
pub struct TelemetryGuard {
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("agent-mesh: could not flush traces on shutdown: {err}");
        }
    }
}

/// Initialise logging, trace export, and (optionally) the Prometheus endpoint.
///
/// Logs always go to stderr: stdout is the MCP transport, and a stray line there corrupts the
/// protocol stream.
pub fn init(config: &TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    let filter = tracing_subscriber::EnvFilter::try_from_env("AGENT_MESH_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    let provider = match config.otlp_endpoint.as_deref() {
        Some(endpoint) => Some(build_tracer(endpoint, &config.service_name)?),
        None => None,
    };

    match provider.as_ref() {
        Some(provider) => {
            let tracer = provider.tracer(config.service_name.clone());
            tracing_subscriber::registry()
                .with(filter)
                .with(stderr_layer)
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .init();
        }
        None => {
            tracing_subscriber::registry()
                .with(filter)
                .with(stderr_layer)
                .init();
        }
    }

    if let Some(listen) = config.prometheus_listen.as_deref() {
        start_prometheus(listen)?;
    }

    Ok(TelemetryGuard { provider })
}

fn build_tracer(
    endpoint: &str,
    service_name: &str,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, TelemetryError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| TelemetryError::OtlpInit {
            reason: e.to_string(),
        })?;

    Ok(opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.to_owned())
                .build(),
        )
        .build())
}

fn start_prometheus(listen: &str) -> Result<(), TelemetryError> {
    let addr: SocketAddr = listen.parse().map_err(|source| TelemetryError::BadAddress {
        value: listen.to_owned(),
        source,
    })?;

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| TelemetryError::PrometheusBind {
            addr,
            reason: e.to_string(),
        })?;

    tracing::info!(%addr, "serving Prometheus metrics");
    Ok(())
}

/// Record the outcome of one ask. Called on both success and failure so error rates are visible,
/// not just happy-path counts.
pub fn record_ask(agent: &AgentId, outcome: AskOutcome, elapsed: std::time::Duration) {
    metrics::counter!(
        metrics_names::ASKS_TOTAL,
        "agent" => agent.to_string(),
        "outcome" => outcome.label(),
    )
    .increment(1);

    metrics::histogram!(
        metrics_names::ASK_DURATION_SECONDS,
        "agent" => agent.to_string(),
    )
    .record(elapsed.as_secs_f64());
}

/// How an ask ended. Distinguished so a refused relay is not lumped in with an agent error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskOutcome {
    Success,
    Timeout,
    /// The relay guard refused it (self-ask or too deep).
    Refused,
    /// The agent itself failed.
    AgentError,
}

impl AskOutcome {
    pub fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Refused => "refused",
            Self::AgentError => "agent_error",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_have_distinct_labels() {
        let labels = [
            AskOutcome::Success.label(),
            AskOutcome::Timeout.label(),
            AskOutcome::Refused.label(),
            AskOutcome::AgentError.label(),
        ];
        let unique: std::collections::BTreeSet<_> = labels.iter().collect();
        assert_eq!(unique.len(), labels.len(), "labels must be distinguishable");
    }

    #[test]
    fn config_defaults_disable_exporters() {
        let cfg: TelemetryConfig = serde_json::from_str("{}").expect("empty config");
        assert!(cfg.otlp_endpoint.is_none());
        assert!(
            cfg.prometheus_listen.is_none(),
            "the metrics port must be opt-in: under stdio there is one process per client"
        );
        assert_eq!(cfg.service_name, "agent-mesh");
    }

    /// A malformed address must be reported, not silently ignored, or the operator believes
    /// metrics are being served when nothing is listening.
    #[test]
    fn bad_prometheus_address_is_an_error() {
        let outcome = start_prometheus("not-an-address");
        assert!(matches!(outcome, Err(TelemetryError::BadAddress { .. })));
    }
}
