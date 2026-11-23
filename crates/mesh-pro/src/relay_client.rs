use crate::error::AccountError;
use crate::http::ApiClient;
use crate::protocol::{
    AckBody, AllowBody, AllowList, PeerNode, PeersResponse, PollBody, PollResponse, Receipt,
    ReceiptQuery, ReceiptsResponse, RegisterBody, RegisteredNode, RelayMessage, SendAck, SendBody,
    SurfaceBody, TeamCreateBody, TeamInviteBody, TeamState,
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
        self.post_send(SendBody {
            from_machine: self.machine_id.clone(),
            from_node: from_node.to_owned(),
            text: text.to_owned(),
            hops,
            to_machine: to_machine.map(str::to_owned),
            to_node: to_node.to_owned(),
            to_email: None,
        })
        .await
    }

    pub async fn poll(&self, wait_ms: u64) -> Result<Vec<RelayMessage>, AccountError> {
        let url = format!("{}/v1/relay/poll", self.api_base);
        let body = PollBody {
            machine_id: self.machine_id.clone(),
            wait_ms,
        };
        let timeout =
            Duration::from_millis(wait_ms.saturating_add(5_000)).max(Duration::from_secs(10));
        let raw = self
            .http
            .post_json(&url, &self.token, &body, timeout)
            .await?;
        let parsed: PollResponse = serde_json::from_str(&raw)?;
        Ok(parsed.messages)
    }

    pub async fn send_to_email(
        &self,
        from_node: &str,
        to_email: &str,
        to_node: &str,
        text: &str,
        hops: u32,
    ) -> Result<SendAck, AccountError> {
        self.post_send(SendBody {
            from_machine: self.machine_id.clone(),
            from_node: from_node.to_owned(),
            text: text.to_owned(),
            hops,
            to_machine: None,
            to_node: to_node.to_owned(),
            to_email: Some(to_email.to_owned()),
        })
        .await
    }

    async fn post_send(&self, body: SendBody) -> Result<SendAck, AccountError> {
        let url = format!("{}/v1/relay/send", self.api_base);
        let raw = self
            .http
            .post_json(&url, &self.token, &body, Duration::from_secs(10))
            .await?;
        serde_json::from_str(&raw).map_err(AccountError::from)
    }

    pub async fn surface(&self, ids: &[String]) -> Result<(), AccountError> {
        let url = format!("{}/v1/relay/surface", self.api_base);
        self.http
            .post_json(
                &url,
                &self.token,
                &SurfaceBody { ids: ids.to_vec() },
                Duration::from_secs(10),
            )
            .await
            .map(|_| ())
    }

    pub async fn receipts(&self, ids: &[String]) -> Result<Vec<Receipt>, AccountError> {
        let url = format!("{}/v1/relay/receipts", self.api_base);
        let raw = self
            .http
            .post_json(
                &url,
                &self.token,
                &ReceiptQuery { ids: ids.to_vec() },
                Duration::from_secs(10),
            )
            .await?;
        let parsed: ReceiptsResponse = serde_json::from_str(&raw)?;
        Ok(parsed.receipts)
    }

    pub async fn allowlist(&self) -> Result<Vec<String>, AccountError> {
        let url = format!("{}/v1/allow", self.api_base);
        let raw = self
            .http
            .get_json(&url, &self.token, Duration::from_secs(10))
            .await?;
        let parsed: AllowList = serde_json::from_str(&raw)?;
        Ok(parsed.emails)
    }

    pub async fn ack(&self, ids: &[String]) -> Result<(), AccountError> {
        let url = format!("{}/v1/relay/ack", self.api_base);
        let body = AckBody { ids: ids.to_vec() };
        self.http
            .post_json(&url, &self.token, &body, Duration::from_secs(10))
            .await
            .map(|_| ())
    }

    pub async fn create_team(&self, name: &str) -> Result<TeamState, AccountError> {
        self.team_post(
            "/v1/team",
            &TeamCreateBody {
                name: name.to_owned(),
            },
        )
        .await
    }

    pub async fn invite(&self, email: &str) -> Result<TeamState, AccountError> {
        self.team_post(
            "/v1/team/invite",
            &TeamInviteBody {
                email: email.to_owned(),
            },
        )
        .await
    }

    pub async fn allow_email(&self, email: &str) -> Result<(), AccountError> {
        let url = format!("{}/v1/allow", self.api_base);
        self.http
            .post_json(
                &url,
                &self.token,
                &AllowBody {
                    email: email.to_owned(),
                },
                Duration::from_secs(10),
            )
            .await
            .map(|_| ())
    }

    async fn team_post<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<TeamState, AccountError> {
        let url = format!("{}{path}", self.api_base);
        let raw = self
            .http
            .post_json(&url, &self.token, body, Duration::from_secs(10))
            .await?;
        serde_json::from_str(&raw).map_err(AccountError::from)
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
