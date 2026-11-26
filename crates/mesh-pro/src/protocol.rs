use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

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
    #[serde(default)]
    pub email: Option<String>,
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
    #[serde(default)]
    pub to_email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendAck {
    pub to_machine: String,
    pub to_node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayMessage {
    #[serde(default)]
    pub id: String,
    pub to_node: String,
    pub from_machine: String,
    pub from_node: String,
    #[serde(default)]
    pub from_email: Option<String>,
    pub text: String,
    pub hops: u32,
    pub sent_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckBody {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SurfaceBody {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptQuery {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub id: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptsResponse {
    pub receipts: Vec<Receipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AllowList {
    pub emails: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamCreateBody {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamInviteBody {
    pub email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AllowBody {
    pub email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamMember {
    pub email: String,
    pub account_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TeamState {
    pub id: String,
    pub name: String,
    pub members: Vec<TeamMember>,
    #[serde(default)]
    pub owner_account_id: String,
    #[serde(default)]
    pub pending: Vec<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardMember {
    pub account_id: String,
    pub machine_id: String,
    pub node: String,
    pub agent: String,
    #[serde(default)]
    pub email: String,
    pub joined_at: u64,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardSession {
    pub id: String,
    pub team_id: String,
    pub name: String,
    pub created_by: String,
    pub created_at: u64,
    #[serde(default)]
    pub closed_at: Option<u64>,
    pub members: Vec<BoardMember>,
    pub rev: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BoardActor {
    #[serde(default)]
    pub account_id: String,
    pub node: String,
}

impl<'de> Deserialize<'de> for BoardActor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(node) => Ok(Self {
                account_id: String::new(),
                node,
            }),
            serde_json::Value::Object(map) => Ok(Self {
                account_id: map
                    .get("account_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                node: map
                    .get("node")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            }),
            _ => Err(D::Error::custom("actor must be a string or an object")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardNote {
    pub at: u64,
    pub by: BoardActor,
    pub kind: String,
    pub text: String,
    #[serde(default)]
    pub receipts: Vec<String>,
    #[serde(default)]
    pub delivery: Vec<BoardDelivery>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardDelivery {
    pub to: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardTask {
    pub id: String,
    pub session_id: String,
    pub title: String,
    pub status: String,
    #[serde(default)]
    pub owner: Option<BoardActor>,
    pub created_by: BoardActor,
    pub updated_at: u64,
    pub rev: u64,
    #[serde(default)]
    pub notes: Vec<BoardNote>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardView {
    pub session: BoardSession,
    pub code: String,
    pub tasks: Vec<BoardTask>,
    pub rev: u64,
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
