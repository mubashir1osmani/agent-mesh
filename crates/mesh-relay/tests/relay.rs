#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use mesh_pro::{DevicePoll, DeviceStart, RegisteredNode, RelayClient};
use mesh_relay::{ServerConfig, StripeConfig, stripe_signature_header};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

async fn spawn(config: ServerConfig) -> String {
    spawn_app(mesh_relay::app(config)).await
}

async fn spawn_app(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
    panic!("relay did not start");
}

fn stub_config() -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".into(),
        public_url: "http://relay.test".into(),
        data_path: None,
        cache_ttl_seconds: 900,
        stripe: None,
    }
}

async fn approve(base: &str, plan: &str) -> (String, String) {
    let http = reqwest::Client::new();
    let start: DeviceStart = http
        .post(format!("{base}/v1/device/start"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    http.post(format!("{base}/v1/device/approve"))
        .json(&json!({
            "user_code": start.user_code,
            "plan": plan,
            "email": "ada@example.com",
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let poll: DevicePoll = http
        .post(format!("{base}/v1/device/token"))
        .json(&json!({ "device_code": start.device_code }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let DevicePoll::Approved {
        token,
        account_id,
        pro,
        ..
    } = poll
    else {
        panic!("expected an approved device, got {poll:?}");
    };
    assert_eq!(pro, plan == "pro");
    (token, account_id)
}

fn client(base: &str, token: &str, machine: &str) -> RelayClient {
    RelayClient::new(base, token, machine).unwrap()
}

#[tokio::test]
async fn two_machines_exchange_a_message_and_a_third_account_cannot_see_it() {
    let base = spawn(stub_config()).await;
    let (token, _) = approve(&base, "pro").await;
    let other = approve(&base, "pro").await.0;
    let alice = client(&base, &token, "machine-a");
    let bob = client(&base, &token, "machine-b");
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
        .send("codex-1", None, "claude-1", "the key is ORCHID", 0)
        .await
        .unwrap();
    assert_eq!(ack.to_machine, "machine-b");
    assert_eq!(ack.to_node, "claude-1");

    let messages = bob.poll(0).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].text, "the key is ORCHID");
    assert_eq!(messages[0].from_node, "codex-1");
    assert_eq!(messages[0].from_machine, "machine-a");
    assert_eq!(messages[0].to_node, "claude-1");
    assert!(
        bob.poll(0).await.unwrap().is_empty(),
        "poll drains the queue"
    );

    let peers = alice.peers().await.unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].id, "claude-1");

    let stranger = client(&base, &other, "machine-c");
    assert!(stranger.poll(0).await.unwrap().is_empty());
    let missed = stranger
        .send("stranger", Some("machine-b"), "claude-1", "nope", 0)
        .await;
    assert!(
        missed.is_err(),
        "another account's nodes are not addressable"
    );
}

#[tokio::test]
async fn a_free_token_is_rejected_by_the_relay_and_unknown_tokens_are_unauthorized() {
    let base = spawn(stub_config()).await;
    let (token, _) = approve(&base, "free").await;
    let http = reqwest::Client::new();
    let denied = http
        .post(format!("{base}/v1/relay/register"))
        .bearer_auth(&token)
        .json(&json!({ "machine_id": "m", "nodes": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    let body: Value = denied.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("Pro account"));
    assert!(body["error"].as_str().unwrap().contains("agent-mesh login"));

    let missing = http
        .get(format!("{base}/v1/entitlement"))
        .bearer_auth("not-a-token")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 401);

    let entitlement: Value = http
        .get(format!("{base}/v1/entitlement"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entitlement["pro"], false);
    assert_eq!(entitlement["plan"], "free");
}

#[tokio::test]
async fn stub_checkout_flips_entitlement_and_approves_the_device() {
    let base = spawn(stub_config()).await;
    let http = reqwest::Client::new();
    let start: DeviceStart = http
        .post(format!("{base}/v1/device/start"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let session: Value = http
        .post(format!("{base}/v1/checkout/session"))
        .json(&json!({ "device_code": start.device_code, "interval": "year" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(session["stub"], true);
    let id = session["id"].as_str().unwrap();
    assert!(id.starts_with("cs_stub_"));

    let completed = http
        .post(format!("{base}/v1/checkout/stub/{id}/complete"))
        .send()
        .await
        .unwrap();
    assert_eq!(completed.status(), 200);

    let poll: DevicePoll = http
        .post(format!("{base}/v1/device/token"))
        .json(&json!({ "device_code": start.device_code }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let DevicePoll::Approved { token, pro, .. } = poll else {
        panic!("stub completion should approve the device");
    };
    assert!(pro);
    let entitlement: Value = http
        .get(format!("{base}/v1/entitlement"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entitlement["pro"], true);
    assert_eq!(entitlement["plan"], "pro");
}

#[tokio::test]
async fn unsigned_webhooks_work_only_in_stub_mode_and_a_signature_flips_the_plan() {
    let base = spawn(stub_config()).await;
    let http = reqwest::Client::new();
    let start: DeviceStart = http
        .post(format!("{base}/v1/device/start"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let event = json!({
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs_test_1",
                "client_reference_id": start.account_id,
                "metadata": { "device_code": start.device_code },
            }
        }
    });
    let accepted = http
        .post(format!("{base}/v1/stripe/webhook"))
        .json(&event)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 200);
    let body: Value = accepted.json().await.unwrap();
    assert_eq!(body["stub"], true);
    assert_eq!(body["plan"], "pro");

    let stripe = stripe_server().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut config = stub_config();
    config.stripe = Some(StripeConfig {
        secret_key: "sk_test_123".into(),
        webhook_secret: "whsec_test".into(),
        price_monthly: "price_month".into(),
        price_yearly: "price_year".into(),
        success_url: "https://example.test/ok".into(),
        cancel_url: "https://example.test/cancel".into(),
        api_base: stripe,
    });
    let live = spawn(config).await;
    let start: DeviceStart = http
        .post(format!("{live}/v1/device/start"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let payload = serde_json::to_vec(&json!({
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "client_reference_id": start.account_id,
                "metadata": { "device_code": start.device_code, "account_id": start.account_id },
            }
        }
    }))
    .unwrap();
    let rejected = http
        .post(format!("{live}/v1/stripe/webhook"))
        .header("content-type", "application/json")
        .body(payload.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);

    let header = stripe_signature_header("whsec_test", now, &payload).unwrap();
    let accepted = http
        .post(format!("{live}/v1/stripe/webhook"))
        .header("content-type", "application/json")
        .header("stripe-signature", header)
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 200, "{}", accepted.text().await.unwrap());

    let poll: DevicePoll = http
        .post(format!("{live}/v1/device/token"))
        .json(&json!({ "device_code": start.device_code }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let DevicePoll::Approved {
        token,
        pro,
        account_id,
        ..
    } = poll
    else {
        panic!("signed webhook should approve the device");
    };
    assert!(pro);

    let deleted = serde_json::to_vec(&json!({
        "type": "customer.subscription.deleted",
        "data": { "object": { "metadata": { "account_id": account_id } } }
    }))
    .unwrap();
    let header = stripe_signature_header("whsec_test", now, &deleted).unwrap();
    http.post(format!("{live}/v1/stripe/webhook"))
        .header("stripe-signature", header)
        .body(deleted)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let entitlement: Value = http
        .get(format!("{live}/v1/entitlement"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entitlement["pro"], false);

    let stub_gone = http
        .post(format!("{live}/v1/device/approve"))
        .json(&json!({ "user_code": "AAAA-BBBB", "plan": "pro" }))
        .send()
        .await
        .unwrap();
    assert_eq!(stub_gone.status(), 404);
}

#[tokio::test]
async fn checkout_posts_the_stripe_form_when_a_secret_is_configured() {
    let seen = Arc::new(tokio::sync::Mutex::new(None));
    let seen_route = Arc::clone(&seen);
    let stripe = axum::Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .route(
            "/v1/checkout/sessions",
            axum::routing::post(move |headers: axum::http::HeaderMap, body: String| {
                let seen_route = Arc::clone(&seen_route);
                async move {
                    let auth = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("")
                        .to_owned();
                    *seen_route.lock().await = Some((auth, body));
                    axum::Json(json!({
                        "id": "cs_test_live",
                        "url": "https://checkout.stripe.com/c/pay/cs_test_live",
                    }))
                }
            }),
        );
    let stripe_base = spawn_app(stripe).await;

    let mut config = stub_config();
    config.stripe = Some(StripeConfig {
        secret_key: "sk_test_123".into(),
        webhook_secret: "whsec_test".into(),
        price_monthly: "price_month".into(),
        price_yearly: "price_year".into(),
        success_url: "https://example.test/ok".into(),
        cancel_url: "https://example.test/cancel".into(),
        api_base: stripe_base,
    });
    let base = spawn(config).await;
    let http = reqwest::Client::new();
    let start: DeviceStart = http
        .post(format!("{base}/v1/device/start"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let session: Value = http
        .post(format!("{base}/v1/checkout/session"))
        .json(&json!({ "device_code": start.device_code, "interval": "month" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(session["stub"], false);
    assert_eq!(session["id"], "cs_test_live");
    let (auth, body) = seen.lock().await.clone().unwrap();
    assert_eq!(auth, "Bearer sk_test_123");
    assert!(body.contains("price_month"), "{body}");
    assert!(body.contains(&start.account_id), "{body}");
    assert!(
        body.contains("subscription_data%5Bmetadata%5D%5Baccount_id%5D"),
        "{body}"
    );
}

#[tokio::test]
async fn a_machine_that_stops_refreshing_drops_off_the_relay() {
    let now = Arc::new(AtomicU64::new(5_000));
    let clock = Arc::clone(&now);
    let app = mesh_relay::app_with_clock(
        stub_config(),
        Arc::new(move || clock.load(Ordering::SeqCst)),
    );
    let base = spawn_app(app).await;
    let (token, _) = approve(&base, "pro").await;
    let alice = client(&base, &token, "machine-a");
    let bob = client(&base, &token, "machine-b");
    bob.register(&[RegisteredNode {
        id: "claude-1".into(),
        name: None,
        agent: "claude".into(),
    }])
    .await
    .unwrap();
    alice
        .send("codex-1", Some("machine-b"), "claude-1", "hi", 0)
        .await
        .unwrap();

    now.store(5_000 + 46, Ordering::SeqCst);
    let missed = alice
        .send("codex-1", Some("machine-b"), "claude-1", "late", 0)
        .await;
    assert!(
        missed.is_err(),
        "presence older than the ttl is not routable"
    );
}

async fn stripe_server() -> String {
    spawn_app(
        axum::Router::new()
            .route("/healthz", axum::routing::get(|| async { "ok" }))
            .route(
                "/v1/checkout/sessions",
                axum::routing::post(|| async {
                    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no")
                }),
            ),
    )
    .await
}
