use crate::config::StripeConfig;
use hmac::{Hmac, Mac};
use mesh_pro::CheckoutSession;
use sha2::Sha256;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

pub const WEBHOOK_TOLERANCE_SECONDS: u64 = 300;

pub fn checkout_form(
    stripe: &StripeConfig,
    account_id: &str,
    device_code: Option<&str>,
    price: &str,
) -> Vec<(String, String)> {
    let mut pairs = vec![
        ("mode".to_owned(), "subscription".to_owned()),
        ("success_url".to_owned(), stripe.success_url.clone()),
        ("cancel_url".to_owned(), stripe.cancel_url.clone()),
        ("client_reference_id".to_owned(), account_id.to_owned()),
        ("line_items[0][price]".to_owned(), price.to_owned()),
        ("line_items[0][quantity]".to_owned(), "1".to_owned()),
        ("metadata[account_id]".to_owned(), account_id.to_owned()),
        (
            "subscription_data[metadata][account_id]".to_owned(),
            account_id.to_owned(),
        ),
    ];
    if let Some(code) = device_code {
        pairs.push(("metadata[device_code]".to_owned(), code.to_owned()));
    }
    pairs
}

pub fn price_for<'a>(stripe: &'a StripeConfig, interval: &str) -> Result<&'a str, String> {
    match interval {
        "month" => Ok(stripe.price_monthly.as_str()),
        "year" => Ok(stripe.price_yearly.as_str()),
        other => Err(format!(
            "interval must be \"month\" or \"year\", got `{other}`"
        )),
    }
}

pub async fn create_stripe_session(
    http: &reqwest::Client,
    stripe: &StripeConfig,
    account_id: &str,
    device_code: Option<&str>,
    interval: &str,
) -> Result<CheckoutSession, String> {
    let price = price_for(stripe, interval)?;
    let body = form_encode(&checkout_form(stripe, account_id, device_code, price));
    let url = format!(
        "{}/v1/checkout/sessions",
        stripe.api_base.trim_end_matches('/')
    );
    let response = http
        .post(url)
        .bearer_auth(&stripe.secret_key)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|err| format!("upstream request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("upstream response could not be read: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "upstream returned {}: {}",
            status.as_u16(),
            stripe_message(&text)
        ));
    }
    let parsed: StripeSession = serde_json::from_str(&text)
        .map_err(|err| format!("upstream returned unexpected JSON: {err}"))?;
    let url = parsed
        .url
        .ok_or_else(|| "upstream session did not include a url".to_owned())?;
    Ok(CheckoutSession {
        id: parsed.id,
        url,
        stub: false,
    })
}

#[derive(serde::Deserialize)]
struct StripeSession {
    id: String,
    url: Option<String>,
}

#[derive(serde::Deserialize)]
struct StripeErrorBody {
    error: Option<StripeErrorMessage>,
}

#[derive(serde::Deserialize)]
struct StripeErrorMessage {
    message: Option<String>,
}

fn stripe_message(body: &str) -> String {
    serde_json::from_str::<StripeErrorBody>(body)
        .ok()
        .and_then(|parsed| parsed.error.and_then(|error| error.message))
        .unwrap_or_else(|| {
            let mut text = body.trim().to_owned();
            if text.len() > 300 {
                text.truncate(300);
            }
            if text.is_empty() {
                "empty body".to_owned()
            } else {
                text
            }
        })
}

pub fn stripe_signature_header(secret: &str, timestamp: u64, payload: &[u8]) -> Option<String> {
    let digest = sign(secret, &timestamp.to_string(), payload)?;
    Some(format!("t={timestamp},v1={digest}"))
}

pub fn verify_stripe_signature(
    secret: &str,
    header: &str,
    payload: &[u8],
    now: u64,
    tolerance: u64,
) -> bool {
    let Some(timestamp_raw) = sig_value(header, "t") else {
        return false;
    };
    let Ok(timestamp) = timestamp_raw.parse::<u64>() else {
        return false;
    };
    if now.abs_diff(timestamp) > tolerance {
        return false;
    }
    let Some(expected) = sign(secret, timestamp_raw, payload) else {
        return false;
    };
    header.split(',').any(|part| {
        part.trim()
            .strip_prefix("v1=")
            .is_some_and(|sig| constant_time_eq(sig.as_bytes(), expected.as_bytes()))
    })
}

fn sign(secret: &str, timestamp: &str, payload: &[u8]) -> Option<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload);
    Some(hex_encode(&mac.finalize().into_bytes()))
}

fn sig_value<'a>(header: &'a str, key: &str) -> Option<&'a str> {
    header.split(',').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name == key).then_some(value)
    })
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

pub fn form_encode(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn encode(value: &str) -> String {
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

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn stripe() -> StripeConfig {
        StripeConfig {
            secret_key: "sk_test".to_owned(),
            webhook_secret: "whsec_test".to_owned(),
            price_monthly: "price_month".to_owned(),
            price_yearly: "price_year".to_owned(),
            success_url: "https://example.test/ok".to_owned(),
            cancel_url: "https://example.test/cancel".to_owned(),
            api_base: "https://api.stripe.com".to_owned(),
        }
    }

    #[test]
    fn checkout_form_names_the_account_on_the_session_and_the_subscription() {
        let stripe = stripe();
        let pairs = checkout_form(&stripe, "acct_1", Some("dev_1"), "price_month");
        let value = |key: &str| {
            pairs
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.as_str())
        };
        assert_eq!(value("mode"), Some("subscription"));
        assert_eq!(value("client_reference_id"), Some("acct_1"));
        assert_eq!(value("line_items[0][price]"), Some("price_month"));
        assert_eq!(value("metadata[account_id]"), Some("acct_1"));
        assert_eq!(value("metadata[device_code]"), Some("dev_1"));
        assert_eq!(
            value("subscription_data[metadata][account_id]"),
            Some("acct_1")
        );
    }

    #[test]
    fn signatures_accept_the_header_we_would_send_and_reject_tampering() {
        let payload = br#"{"type":"checkout.session.completed"}"#;
        let header = stripe_signature_header("whsec_test", 1_700_000_000, payload).unwrap();
        assert!(header.starts_with("t=1700000000,v1="));
        assert!(verify_stripe_signature(
            "whsec_test",
            &header,
            payload,
            1_700_000_100,
            WEBHOOK_TOLERANCE_SECONDS
        ));
        assert!(!verify_stripe_signature(
            "whsec_other",
            &header,
            payload,
            1_700_000_100,
            WEBHOOK_TOLERANCE_SECONDS
        ));
        assert!(
            !verify_stripe_signature(
                "whsec_test",
                &header,
                payload,
                1_700_000_000 + WEBHOOK_TOLERANCE_SECONDS + 1,
                WEBHOOK_TOLERANCE_SECONDS
            ),
            "a stale timestamp is rejected"
        );
        let flipped = header.replace("v1=", "v1=00");
        assert!(!verify_stripe_signature(
            "whsec_test",
            &flipped,
            payload,
            1_700_000_000,
            WEBHOOK_TOLERANCE_SECONDS
        ));
    }
}
