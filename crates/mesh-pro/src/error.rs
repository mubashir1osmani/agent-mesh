#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("could not store Crewmesh credentials: {0}")]
    Io(#[from] std::io::Error),
    #[error("Crewmesh credentials are unreadable: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("the Crewmesh API rejected the token")]
    Unauthorized,

    #[error("{0}")]
    NotPro(String),

    #[error("{0}")]
    Unknown(String),
    #[error("Crewmesh API error ({status}): {body}")]
    Api { status: u16, body: String },
    #[error("could not reach the Crewmesh API at {url}: {message}")]
    Network { url: String, message: String },
}
