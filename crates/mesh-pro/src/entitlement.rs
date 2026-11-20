use crate::error::AccountError;
use crate::http::ApiClient;
use crate::protocol::{DEFAULT_CACHE_TTL_SECONDS, Entitlement};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_API_BASE: &str = "https://api.crewmesh.dev";

const DEFAULT_GRACE_SECONDS: u64 = 86_400;

const CREDENTIALS_FILE: &str = "credentials.json";
const CACHE_FILE: &str = "entitlement.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Credentials {
    pub token: String,
    pub api_base: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Cache {
    pro: bool,
    plan: String,
    account_id: String,
    #[serde(default)]
    email: Option<String>,
    checked_at_unix: u64,
    valid_until_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub logged_in: bool,
    pub account_id: Option<String>,
    pub email: Option<String>,
    pub plan: Option<String>,
    pub pro: bool,
    pub api_base: Option<String>,
    pub cache_valid_until_unix: Option<u64>,
    pub offline: bool,
}

impl Status {
    fn logged_out() -> Self {
        Self {
            logged_in: false,
            account_id: None,
            email: None,
            plan: None,
            pro: false,
            api_base: None,
            cache_valid_until_unix: None,
            offline: false,
        }
    }
}

pub trait EntitlementApi: Send + Sync {
    fn fetch<'a>(
        &'a self,
        api_base: &'a str,
        token: &'a str,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<Entitlement, AccountError>> + Send + 'a;
}

impl EntitlementApi for ApiClient {
    fn fetch<'a>(
        &'a self,
        api_base: &'a str,
        token: &'a str,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<Entitlement, AccountError>> + Send + 'a {
        self.fetch_entitlement(api_base, token, timeout)
    }
}

pub struct Store {
    dir: PathBuf,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    grace_seconds: u64,
    timeout: Duration,

    api_base_override: Option<String>,
}

impl Store {
    pub fn from_env() -> Self {
        let override_base = std::env::var("AGENT_MESH_API_BASE")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_owned())
            .filter(|value| !value.is_empty());
        Self {
            dir: config_dir(),
            now: Arc::new(system_now),
            grace_seconds: env_u64(
                "AGENT_MESH_ENTITLEMENT_GRACE_SECONDS",
                DEFAULT_GRACE_SECONDS,
            ),
            timeout: Duration::from_millis(env_u64("AGENT_MESH_ENTITLEMENT_TIMEOUT_MS", 2_000)),
            api_base_override: override_base,
        }
    }

    pub fn new(
        dir: PathBuf,
        now: Arc<dyn Fn() -> u64 + Send + Sync>,
        grace_seconds: u64,
        timeout: Duration,
    ) -> Self {
        Self {
            dir,
            now,
            grace_seconds,
            timeout,
            api_base_override: None,
        }
    }

    pub fn load(&self) -> Option<Credentials> {
        let mut creds = self.read_credentials()?;
        creds.api_base = self.api_base(&creds);
        Some(creds)
    }

    pub async fn is_pro(&self, api: &impl EntitlementApi) -> bool {
        let Some(creds) = self.load() else {
            return false;
        };
        self.decide(&creds, api)
            .await
            .is_some_and(|decision| decision.pro)
    }

    pub async fn login_with_token(
        &self,
        api: &impl EntitlementApi,
        api_base: &str,
        token: &str,
    ) -> Result<Entitlement, AccountError> {
        let base = api_base.trim().trim_end_matches('/').to_owned();
        let ent = api.fetch(&base, token, self.timeout).await?;
        self.save_session(&base, token, &ent)?;
        Ok(ent)
    }

    pub fn save_session(
        &self,
        api_base: &str,
        token: &str,
        ent: &Entitlement,
    ) -> Result<(), AccountError> {
        let creds = Credentials {
            token: token.to_owned(),
            api_base: api_base.trim().trim_end_matches('/').to_owned(),
            account_id: ent.account_id.clone(),
            email: ent.email.clone(),
        };
        self.write_credentials(&creds)?;
        self.write_cache(&creds, ent, (self.now)())?;
        Ok(())
    }

    pub async fn status(&self, api: &impl EntitlementApi) -> Result<Status, AccountError> {
        let Some(creds) = self.load() else {
            return Ok(Status::logged_out());
        };
        let now = (self.now)();
        if let Some(cache) = self.fresh_cache(&creds, now) {
            return Ok(status_from(&creds, &cache, false));
        }
        match api.fetch(&creds.api_base, &creds.token, self.timeout).await {
            Ok(ent) => {
                let valid_until = now.saturating_add(ttl_of(&ent));
                let account_id = account_of(&creds, &ent);
                self.write_cache(&creds, &ent, now)?;
                Ok(Status {
                    logged_in: true,
                    account_id: Some(account_id),
                    email: ent.email.clone().or(creds.email.clone()),
                    plan: Some(ent.plan),
                    pro: ent.pro,
                    api_base: Some(creds.api_base),
                    cache_valid_until_unix: Some(valid_until),
                    offline: false,
                })
            }
            Err(AccountError::Unauthorized) => {
                self.clear_cache();
                Err(AccountError::Unauthorized)
            }
            Err(err) => {
                if let Some(cache) = self
                    .read_cache()
                    .filter(|cache| self.matches(&creds, cache))
                {
                    let within = now.saturating_sub(cache.checked_at_unix) <= self.grace_seconds;
                    let mut status = status_from(&creds, &cache, true);
                    if !within {
                        status.pro = false;
                        status.plan = None;
                    }
                    Ok(status)
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn decide(&self, creds: &Credentials, api: &impl EntitlementApi) -> Option<Cache> {
        let now = (self.now)();
        if let Some(cache) = self.fresh_cache(creds, now) {
            return Some(cache);
        }
        match api.fetch(&creds.api_base, &creds.token, self.timeout).await {
            Ok(ent) => {
                let cache = cache_from(creds, &ent, now);
                if self.write_cache_value(&cache).is_err() {
                    tracing::warn!("could not write the entitlement cache");
                }
                Some(cache)
            }

            Err(AccountError::Unauthorized) => {
                self.clear_cache();
                None
            }
            Err(AccountError::NotPro(_)) => {
                let ent = Entitlement {
                    plan: "free".to_owned(),
                    pro: false,
                    account_id: creds.account_id.clone(),
                    email: creds.email.clone(),
                    cache_ttl_seconds: 60,
                };
                let cache = cache_from(creds, &ent, now);
                let _ = self.write_cache_value(&cache);
                Some(cache)
            }
            Err(_) => self.grace_cache(creds, now),
        }
    }

    fn fresh_cache(&self, creds: &Credentials, now: u64) -> Option<Cache> {
        let cache = self.read_cache()?;
        if !self.matches(creds, &cache) || now >= cache.valid_until_unix {
            return None;
        }
        Some(cache)
    }

    fn grace_cache(&self, creds: &Credentials, now: u64) -> Option<Cache> {
        let cache = self.read_cache()?;
        if !self.matches(creds, &cache) || !cache.pro {
            return None;
        }
        if now.saturating_sub(cache.checked_at_unix) <= self.grace_seconds {
            Some(cache)
        } else {
            None
        }
    }

    fn matches(&self, creds: &Credentials, cache: &Cache) -> bool {
        creds.account_id.is_empty() || cache.account_id == creds.account_id
    }

    fn api_base(&self, creds: &Credentials) -> String {
        self.api_base_override
            .clone()
            .unwrap_or_else(|| creds.api_base.trim_end_matches('/').to_owned())
    }

    fn read_credentials(&self) -> Option<Credentials> {
        let path = self.dir.join(CREDENTIALS_FILE);
        let bytes = std::fs::read(&path).ok()?;
        match serde_json::from_slice(&bytes) {
            Ok(creds) => Some(creds),
            Err(err) => {
                tracing::warn!(path = %path.display(), %err, "ignoring unreadable credentials");
                None
            }
        }
    }

    fn write_credentials(&self, creds: &Credentials) -> Result<(), AccountError> {
        let bytes = serde_json::to_vec_pretty(creds)?;
        write_private(&self.dir.join(CREDENTIALS_FILE), &bytes)?;
        Ok(())
    }

    fn write_cache(
        &self,
        creds: &Credentials,
        ent: &Entitlement,
        now: u64,
    ) -> Result<(), AccountError> {
        self.write_cache_value(&cache_from(creds, ent, now))
    }

    fn write_cache_value(&self, cache: &Cache) -> Result<(), AccountError> {
        let bytes = serde_json::to_vec_pretty(cache)?;
        write_private(&self.dir.join(CACHE_FILE), &bytes)?;
        Ok(())
    }

    fn read_cache(&self) -> Option<Cache> {
        let bytes = std::fs::read(self.dir.join(CACHE_FILE)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn clear_cache(&self) {
        let path = self.dir.join(CACHE_FILE);
        if let Err(err) = std::fs::remove_file(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), %err, "could not remove entitlement cache");
        }
    }
}

fn cache_from(creds: &Credentials, ent: &Entitlement, now: u64) -> Cache {
    let ttl = ttl_of(ent);
    Cache {
        pro: ent.pro,
        plan: ent.plan.clone(),
        account_id: account_of(creds, ent),
        email: ent.email.clone().or(creds.email.clone()),
        checked_at_unix: now,
        valid_until_unix: now.saturating_add(ttl),
    }
}

fn ttl_of(ent: &Entitlement) -> u64 {
    if ent.cache_ttl_seconds == 0 {
        DEFAULT_CACHE_TTL_SECONDS
    } else {
        ent.cache_ttl_seconds
    }
}

fn account_of(creds: &Credentials, ent: &Entitlement) -> String {
    if ent.account_id.is_empty() {
        creds.account_id.clone()
    } else {
        ent.account_id.clone()
    }
}

fn status_from(creds: &Credentials, cache: &Cache, offline: bool) -> Status {
    Status {
        logged_in: true,
        account_id: Some(cache.account_id.clone()),
        email: cache.email.clone().or(creds.email.clone()),
        plan: Some(cache.plan.clone()),
        pro: cache.pro,
        api_base: Some(creds.api_base.clone()),
        cache_valid_until_unix: Some(cache.valid_until_unix),
        offline,
    }
}

pub fn config_dir() -> PathBuf {
    if let Some(explicit) = std::env::var_os("AGENT_MESH_CONFIG_DIR") {
        return PathBuf::from(explicit);
    }
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        return PathBuf::from(xdg).join("agent-mesh");
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(".config")
        .join("agent-mesh")
}

pub fn upgrade_message() -> String {
    "cross-machine messaging needs a Pro account. Run `agent-mesh login`.".to_owned()
}

pub fn logged_in() -> bool {
    Store::from_env().load().is_some()
}

pub fn logout() -> Result<(), AccountError> {
    let dir = config_dir();
    for name in [CREDENTIALS_FILE, CACHE_FILE] {
        match std::fs::remove_file(dir.join(name)) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

pub async fn pro_enabled() -> bool {
    let store = Store::from_env();
    let Some(creds) = store.load() else {
        return false;
    };
    let now = (store.now)();
    if let Some(cache) = store.fresh_cache(&creds, now) {
        return cache.pro;
    }
    match ApiClient::new() {
        Ok(api) => store.is_pro(&api).await,
        Err(_) => store
            .grace_cache(&creds, now)
            .is_some_and(|cache| cache.pro),
    }
}

fn system_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

    if let Some(parent) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::error::AccountError;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    struct Script {
        calls: AtomicUsize,
        results: Mutex<Vec<Result<Entitlement, AccountError>>>,
    }

    impl Script {
        fn new(results: Vec<Result<Entitlement, AccountError>>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                results: Mutex::new(results),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl EntitlementApi for Script {
        async fn fetch(
            &self,
            _api_base: &str,
            _token: &str,
            _timeout: Duration,
        ) -> Result<Entitlement, AccountError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.results
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .pop()
                .unwrap_or_else(|| {
                    Err(AccountError::Network {
                        url: "script".to_owned(),
                        message: "no scripted response left".to_owned(),
                    })
                })
        }
    }

    fn clock(start: u64) -> (Arc<AtomicU64>, Arc<dyn Fn() -> u64 + Send + Sync>) {
        let ticks = Arc::new(AtomicU64::new(start));
        let ticks_for_fn = Arc::clone(&ticks);
        let now: Arc<dyn Fn() -> u64 + Send + Sync> =
            Arc::new(move || ticks_for_fn.load(Ordering::SeqCst));
        (ticks, now)
    }

    struct Tmp(PathBuf);

    impl Tmp {
        fn new() -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "mesh-pro-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn store(dir: &Path, now: Arc<dyn Fn() -> u64 + Send + Sync>, grace: u64) -> Store {
        Store::new(dir.to_path_buf(), now, grace, Duration::from_secs(2))
    }

    fn pro_ent(ttl: u64) -> Entitlement {
        Entitlement {
            plan: "pro".to_owned(),
            pro: true,
            account_id: "acct_1".to_owned(),
            email: Some("ada@example.com".to_owned()),
            cache_ttl_seconds: ttl,
        }
    }

    fn free_ent(ttl: u64) -> Entitlement {
        Entitlement {
            plan: "free".to_owned(),
            pro: false,
            account_id: "acct_1".to_owned(),
            email: Some("ada@example.com".to_owned()),
            cache_ttl_seconds: ttl,
        }
    }

    #[tokio::test]
    async fn logged_out_never_calls_the_api() {
        let tmp = Tmp::new();
        let (_ticks, now) = clock(1_000);
        let store = store(&tmp.0, now, 100);
        let api = Script::new(vec![Ok(pro_ent(50))]);

        assert!(!store.is_pro(&api).await);
        assert_eq!(api.calls(), 0);
        assert!(!store.status(&api).await.unwrap().logged_in);
        assert_eq!(api.calls(), 0);
    }

    #[tokio::test]
    async fn a_pro_answer_is_cached_until_it_expires_and_a_later_free_answer_wins() {
        let tmp = Tmp::new();
        let (ticks, now) = clock(1_000);
        let store = store(&tmp.0, now, 10_000);

        let api = Script::new(vec![Ok(free_ent(50)), Ok(pro_ent(100))]);

        let ent = store
            .login_with_token(&api, "http://relay.test", "token-1")
            .await
            .unwrap();
        assert!(ent.pro);
        assert!(store.is_pro(&api).await, "fresh cache must not refetch");
        assert_eq!(api.calls(), 1);

        ticks.store(1_099, Ordering::SeqCst);
        assert!(store.is_pro(&api).await);
        assert_eq!(
            api.calls(),
            1,
            "one second before expiry still uses the cache"
        );

        ticks.store(1_100, Ordering::SeqCst);
        assert!(
            !store.is_pro(&api).await,
            "an expired cache is refreshed, and free is not Pro"
        );
        assert_eq!(api.calls(), 2);
    }

    #[tokio::test]
    async fn an_offline_refresh_keeps_pro_inside_grace_and_drops_it_after() {
        let tmp = Tmp::new();
        let (ticks, now) = clock(1_000);
        let store = store(&tmp.0, now, 100);
        let api = Script::new(vec![
            Err(AccountError::Network {
                url: "http://relay.test".to_owned(),
                message: "down".to_owned(),
            }),
            Err(AccountError::Network {
                url: "http://relay.test".to_owned(),
                message: "down".to_owned(),
            }),
            Ok(pro_ent(50)),
        ]);

        store
            .login_with_token(&api, "http://relay.test", "token-1")
            .await
            .unwrap();
        assert_eq!(api.calls(), 1);

        ticks.store(1_080, Ordering::SeqCst);
        assert!(store.is_pro(&api).await);
        assert_eq!(api.calls(), 2);

        ticks.store(1_101, Ordering::SeqCst);
        assert!(
            !store.is_pro(&api).await,
            "grace is measured from the last check"
        );
        assert_eq!(api.calls(), 3);
    }

    #[tokio::test]
    async fn a_revoked_token_does_not_keep_pro_via_grace() {
        let tmp = Tmp::new();
        let (ticks, now) = clock(1_000);
        let store = store(&tmp.0, now, 10_000);
        let api = Script::new(vec![Err(AccountError::Unauthorized), Ok(pro_ent(10))]);

        store
            .login_with_token(&api, "http://relay.test", "token-1")
            .await
            .unwrap();
        ticks.store(2_000, Ordering::SeqCst);
        assert!(!store.is_pro(&api).await);
        assert!(!tmp.0.join(CACHE_FILE).exists());
    }

    #[tokio::test]
    async fn a_free_plan_is_not_pro_and_is_cached() {
        let tmp = Tmp::new();
        let (_ticks, now) = clock(1_000);
        let store = store(&tmp.0, now, 100);
        let api = Script::new(vec![Ok(free_ent(100))]);

        let ent = store
            .login_with_token(&api, "http://relay.test", "token-1")
            .await
            .unwrap();
        assert!(!ent.pro);
        assert!(!store.is_pro(&api).await);
        assert_eq!(api.calls(), 1);
    }

    #[tokio::test]
    async fn credentials_are_owner_readable_only() {
        let tmp = Tmp::new();
        let (_ticks, now) = clock(1_000);
        let store = store(&tmp.0, now, 100);
        let api = Script::new(vec![Ok(pro_ent(100))]);
        store
            .login_with_token(&api, "http://relay.test", "token-1")
            .await
            .unwrap();

        use std::os::unix::fs::PermissionsExt;
        let creds = std::fs::metadata(tmp.0.join(CREDENTIALS_FILE)).unwrap();
        let cache = std::fs::metadata(tmp.0.join(CACHE_FILE)).unwrap();
        let dir = std::fs::metadata(&tmp.0).unwrap();
        assert_eq!(creds.permissions().mode() & 0o777, 0o600);
        assert_eq!(cache.permissions().mode() & 0o777, 0o600);
        assert_eq!(dir.permissions().mode() & 0o777, 0o700);
    }
}
