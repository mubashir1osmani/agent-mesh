use crate::error::AccountError;
use crate::protocol::{DevicePoll, DeviceStart, DeviceTokenBody, Entitlement};
use serde::Serialize;
use std::time::Duration;

#[derive(Clone)]
pub struct ApiClient {
    http: reqwest::Client,
}

impl ApiClient {
    pub fn new() -> Result<Self, AccountError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(concat!("agent-mesh/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|err| AccountError::Network {
                url: "local http client".to_owned(),
                message: err.to_string(),
            })?;
        Ok(Self { http })
    }

    pub async fn fetch_entitlement(
        &self,
        api_base: &str,
        token: &str,
        timeout: Duration,
    ) -> Result<Entitlement, AccountError> {
        let url = format!("{}/v1/entitlement", trim(api_base));
        let body = self
            .send(
                reqwest::Method::GET,
                &url,
                Some(token),
                None::<&()>,
                timeout,
            )
            .await?;
        decode(&body)
    }

    pub async fn device_start(&self, api_base: &str) -> Result<DeviceStart, AccountError> {
        let url = format!("{}/v1/device/start", trim(api_base));
        let body = self
            .send(
                reqwest::Method::POST,
                &url,
                None,
                Some(&serde_json::json!({})),
                Duration::from_secs(15),
            )
            .await?;
        decode(&body)
    }

    pub async fn device_poll(
        &self,
        api_base: &str,
        device_code: &str,
    ) -> Result<DevicePoll, AccountError> {
        let url = format!("{}/v1/device/token", trim(api_base));
        let body = self
            .send(
                reqwest::Method::POST,
                &url,
                None,
                Some(&DeviceTokenBody {
                    device_code: device_code.to_owned(),
                }),
                Duration::from_secs(15),
            )
            .await?;
        decode(&body)
    }

    pub(crate) async fn post_json<T: Serialize>(
        &self,
        url: &str,
        token: &str,
        body: &T,
        timeout: Duration,
    ) -> Result<String, AccountError> {
        self.send(reqwest::Method::POST, url, Some(token), Some(body), timeout)
            .await
    }

    pub(crate) async fn get_json(
        &self,
        url: &str,
        token: &str,
        timeout: Duration,
    ) -> Result<String, AccountError> {
        self.send(reqwest::Method::GET, url, Some(token), None::<&()>, timeout)
            .await
    }

    async fn send<T: Serialize>(
        &self,
        method: reqwest::Method,
        url: &str,
        token: Option<&str>,
        body: Option<&T>,
        timeout: Duration,
    ) -> Result<String, AccountError> {
        let mut request = self.http.request(method, url).timeout(timeout);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|err| AccountError::Network {
            url: url.to_owned(),
            message: err.to_string(),
        })?;
        let status = response.status().as_u16();
        let text = response.text().await.map_err(|err| AccountError::Network {
            url: url.to_owned(),
            message: err.to_string(),
        })?;
        classify(status, text)
    }
}

fn trim(api_base: &str) -> &str {
    api_base.trim_end_matches('/')
}

fn decode<T: serde::de::DeserializeOwned>(body: &str) -> Result<T, AccountError> {
    serde_json::from_str(body).map_err(AccountError::from)
}

fn classify(status: u16, body: String) -> Result<String, AccountError> {
    if (200..300).contains(&status) {
        return Ok(body);
    }
    let message = error_message(&body);
    match status {
        401 => Err(AccountError::Unauthorized),
        403 => Err(AccountError::NotPro(message)),
        404 => Err(AccountError::Unknown(message)),
        _ => Err(AccountError::Api {
            status,
            body: message,
        }),
    }
}

fn error_message(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| truncate(body))
}

fn truncate(body: &str) -> String {
    let mut text = body.trim().to_owned();
    if text.len() > 300 {
        text.truncate(300);
        text.push('…');
    }
    if text.is_empty() {
        "empty response".to_owned()
    } else {
        text
    }
}
