use mesh_pro::{
    DevicePoll, DeviceStart, Entitlement, PeerNode, RegisteredNode, RelayMessage, SendAck,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::io::Read;

const PRESENCE_TTL_SECONDS: u64 = 45;
const DEVICE_TTL_SECONDS: u64 = 900;
pub const MAX_RELAY_HOPS: u32 = 16;
const MAX_QUEUE: usize = 100;
const MAX_TEXT: usize = 64_000;
const MAX_ID: usize = 200;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Plan {
    Free,
    Pro,
}

impl Plan {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Pro => "pro",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "free" => Ok(Self::Free),
            "pro" => Ok(Self::Pro),
            other => Err(format!("plan must be \"free\" or \"pro\", got `{other}`")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    #[serde(default)]
    pub email: Option<String>,
    pub token: String,
    pub plan: Plan,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Device {
    pub device_code: String,
    pub user_code: String,
    pub account_id: String,
    pub expires_at: u64,
    pub approved: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkout {
    pub id: String,
    pub account_id: String,
    #[serde(default)]
    pub device_code: Option<String>,
    pub interval: String,
}

#[derive(Clone, Debug)]
struct Presence {
    account_id: String,
    machine_id: String,
    nodes: Vec<RegisteredNode>,
    seen_at: u64,
}

#[derive(Clone, Debug)]
struct Target {
    machine_id: String,
    node_id: String,
}

pub struct Outgoing<'a> {
    pub account_id: &'a str,
    pub from_machine: &'a str,
    pub from_node: &'a str,
    pub to_machine: Option<&'a str>,
    pub to_node: &'a str,
    pub text: String,
    pub hops: u32,
}

#[derive(Debug)]
pub enum RouteError {
    Unknown,
    SameMachine,
    Ambiguous(Vec<String>),
    TooDeep,
    TooLarge,
    Invalid(String),
}

#[derive(Default)]
pub struct Inner {
    pub accounts: Vec<Account>,
    pub devices: Vec<Device>,
    pub checkouts: Vec<Checkout>,
    machines: Vec<Presence>,
    queues: HashMap<(String, String), VecDeque<RelayMessage>>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct Disk {
    pub accounts: Vec<Account>,
    pub devices: Vec<Device>,
    pub checkouts: Vec<Checkout>,
}

impl Inner {
    pub fn from_disk(disk: Disk) -> Self {
        Self {
            accounts: disk.accounts,
            devices: disk.devices,
            checkouts: disk.checkouts,
            ..Self::default()
        }
    }

    pub fn disk(&self) -> Disk {
        Disk {
            accounts: self.accounts.clone(),
            devices: self.devices.clone(),
            checkouts: self.checkouts.clone(),
        }
    }

    pub fn start_device(&mut self, now: u64, public_url: &str) -> std::io::Result<DeviceStart> {
        let account_id = format!("acct_{}", random_hex(8)?);
        let token = random_hex(32)?;
        let device_code = random_hex(24)?;
        let user_code = user_code()?;
        self.accounts.push(Account {
            id: account_id.clone(),
            email: None,
            token,
            plan: Plan::Free,
        });
        self.devices.push(Device {
            device_code: device_code.clone(),
            user_code: user_code.clone(),
            account_id: account_id.clone(),
            expires_at: now.saturating_add(DEVICE_TTL_SECONDS),
            approved: false,
        });
        Ok(DeviceStart {
            device_code,
            user_code: user_code.clone(),
            verification_url: format!(
                "{}/v1/device/verify?code={user_code}",
                public_url.trim_end_matches('/')
            ),
            expires_in: DEVICE_TTL_SECONDS,
            interval: 1,
            account_id,
        })
    }

    pub fn poll_device(&self, device_code: &str, now: u64, ttl: u64) -> DevicePoll {
        let Some(device) = self
            .devices
            .iter()
            .find(|device| device.device_code == device_code)
        else {
            return DevicePoll::Expired;
        };
        if now >= device.expires_at {
            return DevicePoll::Expired;
        }
        if !device.approved {
            return DevicePoll::Pending { interval: 1 };
        }
        let Some(account) = self
            .accounts
            .iter()
            .find(|account| account.id == device.account_id)
        else {
            return DevicePoll::Expired;
        };
        DevicePoll::Approved {
            token: account.token.clone(),
            plan: account.plan.as_str().to_owned(),
            pro: account.plan == Plan::Pro,
            account_id: account.id.clone(),
            email: account.email.clone(),
            cache_ttl_seconds: ttl,
        }
    }

    pub fn approve(
        &mut self,
        user_code: &str,
        plan: Plan,
        email: Option<String>,
        now: u64,
    ) -> Result<(), String> {
        let account_id = {
            let device = self
                .devices
                .iter_mut()
                .find(|device| device.user_code.eq_ignore_ascii_case(user_code))
                .ok_or_else(|| format!("unknown user code `{user_code}`"))?;
            if now >= device.expires_at {
                return Err("that code has expired".to_owned());
            }
            device.approved = true;
            device.account_id.clone()
        };
        let account = self
            .accounts
            .iter_mut()
            .find(|account| account.id == account_id)
            .ok_or_else(|| format!("account `{account_id}` is missing"))?;
        account.plan = plan;
        if email.is_some() {
            account.email = email;
        }
        Ok(())
    }

    pub fn account_by_token(&self, token: &str) -> Option<&Account> {
        self.accounts
            .iter()
            .find(|account| constant_time_eq(account.token.as_bytes(), token.as_bytes()))
    }

    pub fn entitlement(&self, token: &str, ttl: u64) -> Option<Entitlement> {
        let account = self.account_by_token(token)?;
        Some(Entitlement {
            plan: account.plan.as_str().to_owned(),
            pro: account.plan == Plan::Pro,
            account_id: account.id.clone(),
            email: account.email.clone(),
            cache_ttl_seconds: ttl,
        })
    }

    pub fn account_for_checkout(
        &self,
        device_code: Option<&str>,
        token: Option<&str>,
        now: u64,
    ) -> Result<(String, Option<String>), String> {
        if let Some(code) = device_code {
            let device = self
                .devices
                .iter()
                .find(|device| device.device_code == code)
                .ok_or_else(|| "unknown device_code".to_owned())?;
            if now >= device.expires_at {
                return Err("that device code has expired".to_owned());
            }
            return Ok((device.account_id.clone(), Some(code.to_owned())));
        }
        let token = token.ok_or_else(|| {
            "checkout needs a device_code or an Authorization bearer token".to_owned()
        })?;
        let account = self
            .account_by_token(token)
            .ok_or_else(|| "unknown token".to_owned())?;
        Ok((account.id.clone(), None))
    }

    pub fn open_checkout(
        &mut self,
        id: String,
        account_id: String,
        device_code: Option<String>,
        interval: String,
    ) {
        self.checkouts.push(Checkout {
            id,
            account_id,
            device_code,
            interval,
        });
    }

    pub fn checkout(&self, id: &str) -> Option<&Checkout> {
        self.checkouts.iter().find(|checkout| checkout.id == id)
    }

    pub fn grant_pro(&mut self, account_id: &str, device_code: Option<&str>) -> bool {
        if !self.accounts.iter().any(|account| account.id == account_id) {
            return false;
        }
        if let Some(account) = self
            .accounts
            .iter_mut()
            .find(|account| account.id == account_id)
        {
            account.plan = Plan::Pro;
        }
        if let Some(code) = device_code {
            if let Some(device) = self
                .devices
                .iter_mut()
                .find(|device| device.device_code == code)
            {
                device.approved = true;
            }
        }
        true
    }

    pub fn revoke_pro(&mut self, account_id: &str) -> bool {
        let Some(account) = self
            .accounts
            .iter_mut()
            .find(|account| account.id == account_id)
        else {
            return false;
        };
        account.plan = Plan::Free;
        true
    }

    pub fn register(
        &mut self,
        account_id: &str,
        machine_id: &str,
        nodes: Vec<RegisteredNode>,
        now: u64,
    ) -> Result<(), RouteError> {
        validate_id(machine_id)?;
        if nodes.len() > 100 {
            return Err(RouteError::Invalid(
                "a machine may register at most 100 nodes".to_owned(),
            ));
        }
        for node in &nodes {
            validate_id(&node.id)?;
            if let Some(name) = &node.name {
                validate_id(name)?;
            }
            if node.agent.is_empty() || node.agent.len() > MAX_ID {
                return Err(RouteError::Invalid(
                    "agent name is empty or too long".to_owned(),
                ));
            }
        }
        if let Some(existing) = self
            .machines
            .iter_mut()
            .find(|machine| machine.account_id == account_id && machine.machine_id == machine_id)
        {
            existing.nodes = nodes;
            existing.seen_at = now;
        } else {
            self.machines.push(Presence {
                account_id: account_id.to_owned(),
                machine_id: machine_id.to_owned(),
                nodes,
                seen_at: now,
            });
        }
        Ok(())
    }

    pub fn send(&mut self, message: Outgoing<'_>, now: u64) -> Result<SendAck, RouteError> {
        if message.hops > MAX_RELAY_HOPS {
            return Err(RouteError::TooDeep);
        }
        if message.text.len() > MAX_TEXT {
            return Err(RouteError::TooLarge);
        }
        validate_id(message.from_machine)?;
        validate_id(message.from_node)?;
        validate_id(message.to_node)?;
        self.expire(now);
        let target = self.resolve(
            message.account_id,
            message.from_machine,
            message.to_machine,
            message.to_node,
        )?;
        let queued = RelayMessage {
            to_node: target.node_id.clone(),
            from_machine: message.from_machine.to_owned(),
            from_node: message.from_node.to_owned(),
            text: message.text,
            hops: message.hops,
            sent_at_unix: now,
        };
        let queue = self
            .queues
            .entry((message.account_id.to_owned(), target.machine_id.clone()))
            .or_default();
        if queue.len() >= MAX_QUEUE {
            queue.pop_front();
        }
        queue.push_back(queued);
        Ok(SendAck {
            to_machine: target.machine_id,
            to_node: target.node_id,
        })
    }

    pub fn touch(&mut self, account_id: &str, machine_id: &str, now: u64) {
        if let Some(machine) = self
            .machines
            .iter_mut()
            .find(|machine| machine.account_id == account_id && machine.machine_id == machine_id)
        {
            machine.seen_at = now;
        }
    }

    pub fn drain(&mut self, account_id: &str, machine_id: &str) -> Vec<RelayMessage> {
        self.queues
            .remove(&(account_id.to_owned(), machine_id.to_owned()))
            .map(|queue| queue.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn peers(&mut self, account_id: &str, machine_id: &str, now: u64) -> Vec<PeerNode> {
        self.expire(now);
        self.machines
            .iter()
            .filter(|machine| machine.account_id == account_id && machine.machine_id != machine_id)
            .flat_map(|machine| {
                machine.nodes.iter().map(|node| PeerNode {
                    machine_id: machine.machine_id.clone(),
                    id: node.id.clone(),
                    name: node.name.clone(),
                    agent: node.agent.clone(),
                })
            })
            .collect()
    }

    fn resolve(
        &self,
        account_id: &str,
        from_machine: &str,
        to_machine: Option<&str>,
        to_node: &str,
    ) -> Result<Target, RouteError> {
        if to_machine == Some(from_machine) {
            return Err(RouteError::SameMachine);
        }
        let machines: Vec<&Presence> = self
            .machines
            .iter()
            .filter(|machine| {
                machine.account_id == account_id && machine.machine_id != from_machine
            })
            .collect();
        if let Some(machine_id) = to_machine {
            let Some(machine) = machines
                .iter()
                .find(|machine| machine.machine_id == machine_id)
            else {
                return Err(RouteError::Unknown);
            };
            let Some(node) = machine
                .nodes
                .iter()
                .find(|node| node.id == to_node || node.name.as_deref() == Some(to_node))
            else {
                return Err(RouteError::Unknown);
            };
            return Ok(Target {
                machine_id: machine.machine_id.clone(),
                node_id: node.id.clone(),
            });
        }
        let mut hits = Vec::new();
        for machine in machines {
            for node in &machine.nodes {
                if node.id == to_node || node.name.as_deref() == Some(to_node) {
                    hits.push(Target {
                        machine_id: machine.machine_id.clone(),
                        node_id: node.id.clone(),
                    });
                }
            }
        }
        match hits.len() {
            0 => Err(RouteError::Unknown),
            1 => Ok(hits.remove(0)),
            _ => Err(RouteError::Ambiguous(
                hits.into_iter()
                    .map(|hit| format!("{}:{}", hit.machine_id, hit.node_id))
                    .collect(),
            )),
        }
    }

    fn expire(&mut self, now: u64) {
        self.machines
            .retain(|machine| now.saturating_sub(machine.seen_at) <= PRESENCE_TTL_SECONDS);
    }
}

fn validate_id(value: &str) -> Result<(), RouteError> {
    if value.is_empty() || value.len() > MAX_ID || value.contains(['/', '\n', '\r']) {
        Err(RouteError::Invalid(format!(
            "`{value}` is empty, too long, or contains a slash or newline"
        )))
    } else {
        Ok(())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right) {
        diff |= a ^ b;
    }
    diff == 0
}

fn random_hex(bytes: usize) -> std::io::Result<String> {
    let mut buf = vec![0u8; bytes];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(hex_encode(&buf))
}

fn user_code() -> std::io::Result<String> {
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut buf = [0u8; 8];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    let chars: String = buf
        .iter()
        .map(|byte| ALPHABET[(*byte as usize) % ALPHABET.len()] as char)
        .collect();
    Ok(format!("{}-{}", &chars[..4], &chars[4..]))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}
