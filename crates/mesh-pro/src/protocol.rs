use serde::{Deserialize, Serialize};

pub const DEFAULT_CACHE_TTL_SECONDS: u64 = 900;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entitlement {
    pub plan: String,
    pub pro: bool,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default = "default_ttl")]
    pub cache_ttl_seconds: u64,
}

fn default_ttl() -> u64 {
    DEFAULT_CACHE_TTL_SECONDS
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_url: String,
    pub expires_in: u64,
    pub interval: u64,
    pub account_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DevicePoll {
    Pending {
        interval: u64,
    },
    Approved {
        token: String,
        plan: String,
        pro: bool,
        account_id: String,
        #[serde(default)]
        email: Option<String>,
        cache_ttl_seconds: u64,
    },
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceTokenBody {
    pub device_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisteredNode {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub agent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterBody {
    pub machine_id: String,
    pub nodes: Vec<RegisteredNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerNode {
    pub machine_id: String,
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub agent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendBody {
    pub from_machine: String,
    pub from_node: String,
    pub text: String,
    pub hops: u32,

    #[serde(default)]
    pub to_machine: Option<String>,
    pub to_node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendAck {
    pub to_machine: String,
    pub to_node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayMessage {
    pub to_node: String,
    pub from_machine: String,
    pub from_node: String,
    pub text: String,
    pub hops: u32,
    pub sent_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PollBody {
    pub machine_id: String,
    #[serde(default)]
    pub wait_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PollResponse {
    pub messages: Vec<RelayMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeersResponse {
    pub peers: Vec<PeerNode>,
}

pub fn remote_target_id(machine_id: &str, node_id: &str) -> String {
    format!("remote:{machine_id}:{node_id}")
}

pub fn parse_remote_target(id: &str) -> Option<(String, String)> {
    let rest = id.strip_prefix("remote:")?;
    let (machine, node) = rest.split_once(':')?;
    if machine.is_empty() || node.is_empty() {
        return None;
    }
    Some((machine.to_owned(), node.to_owned()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn remote_ids_round_trip() {
        let id = remote_target_id("abc", "codex-12");
        assert_eq!(
            parse_remote_target(&id),
            Some(("abc".to_owned(), "codex-12".to_owned()))
        );
        assert_eq!(parse_remote_target("codex-12"), None);
        assert_eq!(parse_remote_target("remote:onlyone"), None);
        assert_eq!(parse_remote_target("remote::node"), None);
    }
}
