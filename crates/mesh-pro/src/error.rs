#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("could not store credentials: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not read credentials: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("token was rejected")]
    Unauthorized,

    #[error("{0}")]
    NotPro(String),

    #[error("{0}")]
    Unknown(String),
    #[error("request failed ({status}): {body}")]
    Api { status: u16, body: String },
    #[error("could not reach {url}: {message}")]
    Network { url: String, message: String },
}
