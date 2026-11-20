use crate::billing::{self, WEBHOOK_TOLERANCE_SECONDS};
use crate::config::ServerConfig;
use crate::state::{Inner, Plan, RouteError};
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mesh_pro::{
    CheckoutSession, DevicePoll, DeviceStart, Entitlement, PeersResponse, PollResponse,
    RegisterBody, SendAck, SendBody,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct App {
    pub inner: Arc<Mutex<Inner>>,
    pub config: Arc<ServerConfig>,
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub http: reqwest::Client,
    wakes: Arc<Mutex<HashMap<(String, String), Arc<Notify>>>>,
}

pub fn router(app: App) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/meta", get(meta))
        .route("/billing/success", get(billing_success))
        .route("/billing/cancel", get(billing_cancel))
        .route("/v1/device/start", post(device_start))
        .route("/v1/device/token", post(device_token))
        .route("/v1/device/approve", post(device_approve))
        .route("/v1/device/verify", get(device_verify))
        .route("/v1/entitlement", get(entitlement))
        .route("/v1/checkout/session", post(checkout_session))
        .route("/v1/checkout/stub/{id}", get(stub_page))
        .route("/v1/checkout/stub/{id}/complete", post(stub_complete))
        .route("/v1/stripe/webhook", post(webhook))
        .route("/v1/relay/register", post(relay_register))
        .route("/v1/relay/send", post(relay_send))
        .route("/v1/relay/poll", post(relay_poll))
        .route("/v1/relay/peers", get(relay_peers))
        .with_state(app)
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

struct ApiError {
    status: StatusCode,
    body: Value,
}

impl ApiError {
    fn new(status: StatusCode, error: impl Into<String>) -> Self {
        Self {
            status,
            body: serde_json::json!({ "error": error.into() }),
        }
    }

    fn pro_required() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "cross-machine messaging needs a Pro account. Run `agent-mesh login`",
        )
    }

    fn stub_disabled() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not available")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

fn lock(app: &App) -> std::sync::MutexGuard<'_, Inner> {
    app.inner.lock().unwrap_or_else(|err| err.into_inner())
}

fn persist(app: &App) {
    let Some(path) = &app.config.data_path else {
        return;
    };
    let disk = lock(app).disk();
    if let Err(err) = write_disk(path, &disk) {
        tracing::warn!(%err, path = %path.display(), "could not persist relay state");
    }
}

fn write_disk(path: &std::path::Path, disk: &crate::state::Disk) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(disk)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn meta() -> Json<Value> {
    Json(serde_json::json!({
        "service": "crewmesh-relay",
    }))
}

async fn billing_success() -> Html<&'static str> {
    Html("<p>You can close this tab and return to <code>agent-mesh login</code>.</p>")
}

async fn billing_cancel() -> Html<&'static str> {
    Html("<p>You can close this tab.</p>")
}

async fn device_start(State(app): State<App>) -> Result<Json<DeviceStart>, ApiError> {
    let now = (app.now)();
    let started = lock(&app)
        .start_device(now, &app.config.public_url)
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    persist(&app);
    Ok(Json(started))
}

#[derive(Deserialize)]
struct DeviceTokenBody {
    device_code: String,
}

async fn device_token(
    State(app): State<App>,
    Json(body): Json<DeviceTokenBody>,
) -> Json<DevicePoll> {
    let now = (app.now)();
    Json(lock(&app).poll_device(&body.device_code, now, app.config.cache_ttl_seconds))
}

#[derive(Deserialize)]
struct ApproveBody {
    user_code: String,
    #[serde(default)]
    plan: Option<String>,
    #[serde(default)]
    email: Option<String>,
}

async fn device_approve(
    State(app): State<App>,
    Json(body): Json<ApproveBody>,
) -> Result<Json<Value>, ApiError> {
    if app.config.stripe.is_some() {
        return Err(ApiError::stub_disabled());
    }
    let plan = Plan::parse(body.plan.as_deref().unwrap_or("free"))
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;
    let now = (app.now)();
    lock(&app)
        .approve(&body.user_code, plan, body.email, now)
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;
    persist(&app);
    Ok(Json(
        serde_json::json!({ "ok": true, "stub": true, "plan": plan.as_str() }),
    ))
}

#[derive(Deserialize)]
struct CodeQuery {
    code: String,
}

async fn device_verify(Query(query): Query<CodeQuery>) -> Html<String> {
    let code = escape(&query.code);
    Html(format!(
        "<!doctype html><meta charset=\"utf-8\"><title>Crewmesh</title>\
         <h1>Crewmesh login</h1><p>Code: <strong>{code}</strong></p>"
    ))
}

async fn entitlement(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<Entitlement>, ApiError> {
    let token = bearer(&headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    lock(&app)
        .entitlement(&token, app.config.cache_ttl_seconds)
        .map(Json)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "unknown token"))
}

#[derive(Deserialize)]
struct CheckoutBody {
    #[serde(default)]
    device_code: Option<String>,
    #[serde(default)]
    interval: Option<String>,
}

async fn checkout_session(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<CheckoutBody>,
) -> Result<Json<CheckoutSession>, ApiError> {
    let interval = body.interval.unwrap_or_else(|| "month".to_owned());
    if interval != "month" && interval != "year" {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "interval must be \"month\" or \"year\"",
        ));
    }
    let now = (app.now)();
    let token = bearer(&headers);
    let (account_id, device_code) = lock(&app)
        .account_for_checkout(body.device_code.as_deref(), token.as_deref(), now)
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err))?;

    if let Some(stripe) = &app.config.stripe {
        let session = billing::create_stripe_session(
            &app.http,
            stripe,
            &account_id,
            device_code.as_deref(),
            &interval,
        )
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_GATEWAY, err))?;
        lock(&app).open_checkout(session.id.clone(), account_id, device_code, interval);
        persist(&app);
        return Ok(Json(session));
    }

    let id = format!(
        "cs_stub_{}",
        random_suffix()
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?
    );
    let url = format!(
        "{}/v1/checkout/stub/{id}",
        app.config.public_url.trim_end_matches('/')
    );
    lock(&app).open_checkout(id.clone(), account_id, device_code, interval);
    persist(&app);
    Ok(Json(CheckoutSession {
        id,
        url,
        stub: true,
    }))
}

async fn stub_page(
    State(app): State<App>,
    Path(id): Path<String>,
) -> Result<Html<String>, ApiError> {
    if app.config.stripe.is_some() {
        return Err(ApiError::stub_disabled());
    }
    let known = lock(&app).checkout(&id).is_some();
    if !known {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "unknown checkout session",
        ));
    }
    let public = escape(app.config.public_url.trim_end_matches('/'));
    let id = escape(&id);
    Ok(Html(format!(
        "<!doctype html><meta charset=\"utf-8\"><title>Crewmesh</title>\
         <p><a href=\"{public}/v1/checkout/stub/{id}/complete\">Continue</a></p>"
    )))
}

async fn stub_complete(
    State(app): State<App>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if app.config.stripe.is_some() {
        return Err(ApiError::stub_disabled());
    }
    let checkout = lock(&app)
        .checkout(&id)
        .cloned()
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "unknown checkout session"))?;
    if !lock(&app).grant_pro(&checkout.account_id, checkout.device_code.as_deref()) {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "account missing"));
    }
    persist(&app);
    Ok(Json(serde_json::json!({
        "ok": true,
        "stub": true,
        "account_id": checkout.account_id,
        "plan": "pro",
    })))
}

async fn webhook(
    State(app): State<App>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    if let Some(stripe) = &app.config.stripe {
        let header = headers
            .get("stripe-signature")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "missing signature"))?;
        if !billing::verify_stripe_signature(
            &stripe.webhook_secret,
            header,
            &body,
            (app.now)(),
            WEBHOOK_TOLERANCE_SECONDS,
        ) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "signature did not verify",
            ));
        }
    }

    let event: StripeEvent = serde_json::from_slice(&body)
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, format!("bad event: {err}")))?;
    let metadata = event.data.object.metadata.clone().unwrap_or_default();
    let result = match event.kind.as_str() {
        "checkout.session.completed" => {
            apply_completed(&app, &event, &metadata)?;
            "pro"
        }
        "customer.subscription.deleted" => {
            apply_deleted(&app, &metadata)?;
            "free"
        }
        _ => {
            return Ok(Json(serde_json::json!({
                "ignored": true,
                "stub": app.config.stripe.is_none(),
            })));
        }
    };
    persist(&app);
    Ok(Json(serde_json::json!({
        "ok": true,
        "plan": result,
        "stub": app.config.stripe.is_none(),
    })))
}

fn apply_completed(
    app: &App,
    event: &StripeEvent,
    metadata: &Map<String, Value>,
) -> Result<(), ApiError> {
    let mut account_id = event
        .data
        .object
        .client_reference_id
        .clone()
        .or_else(|| string_field(metadata, "account_id"));
    let mut device_code = string_field(metadata, "device_code");
    if let Some(session_id) = event.data.object.id.as_deref() {
        if let Some(checkout) = lock(app).checkout(session_id).cloned() {
            if account_id.is_none() {
                account_id = Some(checkout.account_id);
            }
            if device_code.is_none() {
                device_code = checkout.device_code;
            }
        }
    }
    let Some(account_id) = account_id else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "checkout.session.completed is missing client_reference_id and metadata.account_id",
        ));
    };
    if !lock(app).grant_pro(&account_id, device_code.as_deref()) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("unknown account {account_id}"),
        ));
    }
    Ok(())
}

fn apply_deleted(app: &App, metadata: &Map<String, Value>) -> Result<(), ApiError> {
    let Some(account_id) = string_field(metadata, "account_id") else {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "customer.subscription.deleted is missing metadata.account_id",
        ));
    };
    if !lock(app).revoke_pro(&account_id) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("unknown account {account_id}"),
        ));
    }
    Ok(())
}

fn string_field(metadata: &Map<String, Value>, key: &str) -> Option<String> {
    metadata.get(key).and_then(Value::as_str).map(str::to_owned)
}

#[derive(Deserialize)]
struct StripeEvent {
    #[serde(rename = "type")]
    kind: String,
    data: StripeData,
}

#[derive(Deserialize)]
struct StripeData {
    object: StripeObject,
}

#[derive(Deserialize)]
struct StripeObject {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    client_reference_id: Option<String>,
    #[serde(default)]
    metadata: Option<Map<String, Value>>,
}

async fn relay_register(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<RegisterBody>,
) -> Result<Json<Value>, ApiError> {
    let authed = require_pro(&app, &headers)?;
    let now = (app.now)();
    lock(&app)
        .register(&authed.account_id, &body.machine_id, body.nodes, now)
        .map_err(route_error)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn relay_send(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<SendBody>,
) -> Result<Json<SendAck>, ApiError> {
    let authed = require_pro(&app, &headers)?;
    let now = (app.now)();
    let ack = lock(&app)
        .send(
            crate::state::Outgoing {
                account_id: &authed.account_id,
                from_machine: &body.from_machine,
                from_node: &body.from_node,
                to_machine: body.to_machine.as_deref(),
                to_node: &body.to_node,
                text: body.text,
                hops: body.hops,
            },
            now,
        )
        .map_err(route_error)?;
    wake(&app, &authed.account_id, &ack.to_machine);
    Ok(Json(ack))
}

#[derive(Deserialize)]
struct PollBody {
    machine_id: String,
    #[serde(default)]
    wait_ms: u64,
}

async fn relay_poll(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<PollBody>,
) -> Result<Json<PollResponse>, ApiError> {
    let authed = require_pro(&app, &headers)?;
    let wait = body.wait_ms.min(15_000);
    let notify = wake_handle(&app, &authed.account_id, &body.machine_id);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait);
    loop {
        let now = (app.now)();
        let messages = {
            let mut inner = lock(&app);
            inner.touch(&authed.account_id, &body.machine_id, now);
            inner.drain(&authed.account_id, &body.machine_id)
        };
        if !messages.is_empty() || wait == 0 || tokio::time::Instant::now() >= deadline {
            return Ok(Json(PollResponse { messages }));
        }
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
}

#[derive(Deserialize)]
struct PeersQuery {
    machine_id: String,
}

async fn relay_peers(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<PeersQuery>,
) -> Result<Json<PeersResponse>, ApiError> {
    let authed = require_pro(&app, &headers)?;
    let now = (app.now)();
    let peers = lock(&app).peers(&authed.account_id, &query.machine_id, now);
    Ok(Json(PeersResponse { peers }))
}

struct Authed {
    account_id: String,
}

fn require_pro(app: &App, headers: &HeaderMap) -> Result<Authed, ApiError> {
    let token = bearer(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "missing bearer token"))?;
    let account = lock(app)
        .account_by_token(&token)
        .map(|account| (account.id.clone(), account.plan))
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "unknown token"))?;
    if account.1 != Plan::Pro {
        return Err(ApiError::pro_required());
    }
    Ok(Authed {
        account_id: account.0,
    })
}

fn route_error(err: RouteError) -> ApiError {
    match err {
        RouteError::Unknown => ApiError::new(
            StatusCode::NOT_FOUND,
            "no remote node matches that id or name",
        ),
        RouteError::SameMachine => ApiError::new(
            StatusCode::NOT_FOUND,
            "that node is on the sending machine; deliver it locally",
        ),
        RouteError::Ambiguous(ids) => ApiError::new(
            StatusCode::CONFLICT,
            format!(
                "that name matches more than one remote node ({}); pass to_machine",
                ids.join(", ")
            ),
        ),
        RouteError::TooDeep => ApiError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "refusing to relay a message past {} hops",
                crate::state::MAX_RELAY_HOPS
            ),
        ),
        RouteError::TooLarge => ApiError::new(StatusCode::BAD_REQUEST, "message text is too large"),
        RouteError::Invalid(message) => ApiError::new(StatusCode::BAD_REQUEST, message),
    }
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

fn wake(app: &App, account_id: &str, machine_id: &str) {
    wake_handle(app, account_id, machine_id).notify_waiters();
}

fn wake_handle(app: &App, account_id: &str, machine_id: &str) -> Arc<Notify> {
    let mut wakes = app.wakes.lock().unwrap_or_else(|err| err.into_inner());
    wakes
        .entry((account_id.to_owned(), machine_id.to_owned()))
        .or_insert_with(|| Arc::new(Notify::new()))
        .clone()
}

pub fn build(config: ServerConfig, now: Arc<dyn Fn() -> u64 + Send + Sync>) -> Router {
    let inner = match &config.data_path {
        Some(path) => load_disk(path),
        None => Inner::default(),
    };
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let app = App {
        inner: Arc::new(Mutex::new(inner)),
        config: Arc::new(config),
        now,
        http,
        wakes: Arc::new(Mutex::new(HashMap::new())),
    };
    router(app)
}

fn load_disk(path: &std::path::Path) -> Inner {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Inner::default(),
        Err(err) => {
            tracing::warn!(%err, path = %path.display(), "could not read relay state");
            return Inner::default();
        }
    };
    match serde_json::from_slice::<crate::state::Disk>(&bytes) {
        Ok(disk) => Inner::from_disk(disk),
        Err(err) => {
            tracing::warn!(%err, path = %path.display(), "ignoring unreadable relay state");
            Inner::default()
        }
    }
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn random_suffix() -> std::io::Result<String> {
    use std::io::Read;
    let mut buf = [0u8; 8];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(buf.iter().map(|byte| format!("{byte:02x}")).collect())
}
