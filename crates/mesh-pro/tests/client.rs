#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use mesh_pro::{
    AccountError, ApiClient, DevicePoll, DeviceStart, DeviceTokenBody, Entitlement, PeerNode,
    PeersResponse, PollBody, PollResponse, RegisterBody, RegisteredNode, RelayClient, RelayMessage,
    SendAck, SendBody,
};
use serde::Deserialize;

#[derive(Clone, Default)]
struct App {
    inner: Arc<Mutex<Store>>,
}

#[derive(Default)]
struct Store {
    nodes: HashMap<(String, String), Vec<RegisteredNode>>,
    queues: HashMap<(String, String), Vec<RelayMessage>>,
    next_device: u64,
}

#[tokio::test]
async fn the_client_reaches_a_peer_through_the_relay_api() {
    let base = spawn().await;
    let alice = RelayClient::new(&base, "pro-token", "machine-a").unwrap();
    let bob = RelayClient::new(&base, "pro-token", "machine-b").unwrap();
    alice
        .register(&[RegisteredNode {
            id: "codex-1".into(),
            name: Some("reviewer".into()),
            agent: "codex".into(),
        }])
        .await
        .unwrap();
    bob.register(&[RegisteredNode {
        id: "claude-1".into(),
        name: None,
        agent: "claude".into(),
    }])
    .await
    .unwrap();

    let ack = alice
        .send("codex-1", None, "claude-1", "hello", 0)
        .await
        .unwrap();
    assert_eq!(ack.to_machine, "machine-b");
    assert_eq!(ack.to_node, "claude-1");

    let messages = bob.poll(0).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].text, "hello");
    assert_eq!(messages[0].from_machine, "machine-a");
    assert_eq!(messages[0].from_node, "codex-1");
    assert!(bob.poll(0).await.unwrap().is_empty());

    let peers = alice.peers().await.unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].id, "claude-1");
    assert_eq!(peers[0].machine_id, "machine-b");
}

#[tokio::test]
async fn a_free_token_is_refused_and_an_unknown_token_is_unauthorized() {
    let base = spawn().await;
    let free = RelayClient::new(&base, "free-token", "machine-a").unwrap();
    let err = free
        .register(&[RegisteredNode {
            id: "codex-1".into(),
            name: None,
            agent: "codex".into(),
        }])
        .await
        .unwrap_err();
    assert!(matches!(err, AccountError::NotPro(_)));

    let unknown = RelayClient::new(&base, "nope", "machine-a").unwrap();
    let err = unknown.poll(0).await.unwrap_err();
    assert!(matches!(err, AccountError::Unauthorized));
}

#[tokio::test]
async fn login_and_entitlement_use_the_client_shapes() {
    let base = spawn().await;
    let api = ApiClient::new().unwrap();
    let start = api.device_start(&base).await.unwrap();
    let poll = api.device_poll(&base, &start.device_code).await.unwrap();
    let DevicePoll::Approved { token, pro, .. } = poll else {
        panic!("expected approval");
    };
    assert!(pro);
    let entitlement = api
        .fetch_entitlement(&base, &token, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(entitlement.pro);
    assert_eq!(entitlement.account_id, "acct_1");
}

async fn spawn() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let base = format!("http://{addr}");
    let http = reqwest::Client::new();
    for _ in 0..50 {
        if http
            .get(format!("{base}/healthz"))
            .send()
            .await
            .ok()
            .is_some_and(|response| response.status().is_success())
        {
            return base;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("mock did not start");
}

fn router() -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/entitlement", get(entitlement))
        .route("/v1/device/start", post(device_start))
        .route("/v1/device/token", post(device_token))
        .route("/v1/relay/register", post(register))
        .route("/v1/relay/send", post(send))
        .route("/v1/relay/poll", post(poll))
        .route("/v1/relay/peers", get(peers))
        .with_state(App::default())
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn pro_token(headers: &HeaderMap) -> Result<String, StatusCode> {
    match bearer(headers).as_deref() {
        Some("pro-token") => Ok("pro-token".to_owned()),
        Some("free-token") => Err(StatusCode::FORBIDDEN),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

async fn entitlement(headers: HeaderMap) -> Result<Json<Entitlement>, StatusCode> {
    let pro = match bearer(&headers).as_deref() {
        Some("pro-token") => true,
        Some("free-token") => false,
        _ => return Err(StatusCode::UNAUTHORIZED),
    };
    Ok(Json(Entitlement {
        plan: if pro { "pro" } else { "free" }.to_owned(),
        pro,
        account_id: "acct_1".into(),
        email: None,
        cache_ttl_seconds: 900,
    }))
}

async fn device_start(State(app): State<App>) -> Json<DeviceStart> {
    let mut store = app.inner.lock().unwrap_or_else(|err| err.into_inner());
    store.next_device += 1;
    let code = format!("dev-{}", store.next_device);
    Json(DeviceStart {
        device_code: code.clone(),
        user_code: format!("CODE-{}", store.next_device),
        verification_url: format!("http://relay.test/{code}"),
        expires_in: 900,
        interval: 1,
        account_id: "acct_1".into(),
    })
}

async fn device_token(Json(body): Json<DeviceTokenBody>) -> Result<Json<DevicePoll>, StatusCode> {
    if !body.device_code.starts_with("dev-") {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Json(DevicePoll::Approved {
        token: "pro-token".into(),
        plan: "pro".into(),
        pro: true,
        account_id: "acct_1".into(),
        email: None,
        cache_ttl_seconds: 900,
    }))
}

async fn register(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<RegisterBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = pro_token(&headers)?;
    app.inner
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .nodes
        .insert((token, body.machine_id), body.nodes);
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn send(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<SendBody>,
) -> Result<Json<SendAck>, StatusCode> {
    let token = pro_token(&headers)?;
    let mut store = app.inner.lock().unwrap_or_else(|err| err.into_inner());
    let mut hits = Vec::new();
    for ((owner, machine), nodes) in &store.nodes {
        if owner != &token || machine == &body.from_machine {
            continue;
        }
        if body.to_machine.as_ref().is_some_and(|to| to != machine) {
            continue;
        }
        for node in nodes {
            if node.id == body.to_node || node.name.as_deref() == Some(body.to_node.as_str()) {
                hits.push((machine.clone(), node.id.clone()));
            }
        }
    }
    let (to_machine, to_node) = match hits.as_slice() {
        [one] => one.clone(),
        _ => return Err(StatusCode::NOT_FOUND),
    };
    store
        .queues
        .entry((token, to_machine.clone()))
        .or_default()
        .push(RelayMessage {
            id: "m1".to_owned(),
            to_node: to_node.clone(),
            from_machine: body.from_machine,
            from_node: body.from_node,
            text: body.text,
            hops: body.hops,
            sent_at_unix: 1,
        });
    Ok(Json(SendAck {
        to_machine,
        to_node,
    }))
}

async fn poll(
    State(app): State<App>,
    headers: HeaderMap,
    Json(body): Json<PollBody>,
) -> Result<Json<PollResponse>, StatusCode> {
    let token = pro_token(&headers)?;
    let messages = app
        .inner
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .queues
        .remove(&(token, body.machine_id))
        .unwrap_or_default();
    Ok(Json(PollResponse { messages }))
}

#[derive(Deserialize)]
struct PeersQuery {
    machine_id: String,
}

async fn peers(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<PeersQuery>,
) -> Result<Json<PeersResponse>, StatusCode> {
    let token = pro_token(&headers)?;
    let store = app.inner.lock().unwrap_or_else(|err| err.into_inner());
    let peers = store
        .nodes
        .iter()
        .filter(|((owner, machine), _)| owner == &token && machine != &query.machine_id)
        .flat_map(|((_, machine), nodes)| {
            nodes.iter().map(|node| PeerNode {
                machine_id: machine.clone(),
                id: node.id.clone(),
                name: node.name.clone(),
                agent: node.agent.clone(),
                email: None,
            })
        })
        .collect();
    Ok(Json(PeersResponse { peers }))
}
