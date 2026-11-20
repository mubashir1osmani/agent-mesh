//! Client for the cross-machine relay. Constructing it does not open a connection.

use crate::error::AccountError;
use crate::http::ApiClient;
use crate::protocol::{
    PeerNode, PeersResponse, PollResponse, RegisterBody, RegisteredNode, RelayMessage, SendAck,
    SendBody,
};
use std::time::Duration;

pub struct RelayClient {
    http: ApiClient,
    api_base: String,
    token: String,
    machine_id: String,
}

impl RelayClient {
    pub fn new(
        api_base: impl Into<String>,
        token: impl Into<String>,
        machine_id: impl Into<String>,
    ) -> Result<Self, AccountError> {
        Ok(Self {
            http: ApiClient::new()?,
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            token: token.into(),
            machine_id: machine_id.into(),
        })
    }

    pub fn machine_id(&self) -> &str {
        &self.machine_id
    }

    pub async fn register(&self, nodes: &[RegisteredNode]) -> Result<(), AccountError> {
        let url = format!("{}/v1/relay/register", self.api_base);
        let body = RegisterBody {
            machine_id: self.machine_id.clone(),
            nodes: nodes.to_vec(),
        };
        self.http
            .post_json(&url, &self.token, &body, Duration::from_secs(10))
            .await
            .map(|_| ())
    }

    pub async fn send(
        &self,
        from_node: &str,
        to_machine: Option<&str>,
        to_node: &str,
        text: &str,
        hops: u32,
    ) -> Result<SendAck, AccountError> {
        let url = format!("{}/v1/relay/send", self.api_base);
        let body = SendBody {
            from_machine: self.machine_id.clone(),
            from_node: from_node.to_owned(),
            text: text.to_owned(),
            hops,
            to_machine: to_machine.map(str::to_owned),
            to_node: to_node.to_owned(),
        };
        let raw = self
            .http
            .post_json(&url, &self.token, &body, Duration::from_secs(10))
            .await?;
        serde_json::from_str(&raw).map_err(AccountError::from)
    }

    pub async fn poll(&self, wait_ms: u64) -> Result<Vec<RelayMessage>, AccountError> {
        let url = format!("{}/v1/relay/poll", self.api_base);
        let body = serde_json::json!({
            "machine_id": self.machine_id,
            "wait_ms": wait_ms,
        });
        let timeout =
            Duration::from_millis(wait_ms.saturating_add(5_000)).max(Duration::from_secs(10));
        let raw = self
            .http
            .post_json(&url, &self.token, &body, timeout)
            .await?;
        let parsed: PollResponse = serde_json::from_str(&raw)?;
        Ok(parsed.messages)
    }

    pub async fn peers(&self) -> Result<Vec<PeerNode>, AccountError> {
        let url = format!(
            "{}/v1/relay/peers?machine_id={}",
            self.api_base,
            urlencoding_component(&self.machine_id)
        );
        let raw = self
            .http
            .get_json(&url, &self.token, Duration::from_secs(10))
            .await?;
        let parsed: PeersResponse = serde_json::from_str(&raw)?;
        Ok(parsed.peers)
    }
}

/// Encode the few characters a machine id might contain. Machine ids are hex, so this stays short.
fn urlencoding_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
