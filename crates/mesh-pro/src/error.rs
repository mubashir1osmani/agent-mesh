/// Why a Crewmesh account or relay call failed.
#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("could not store Crewmesh credentials: {0}")]
    Io(#[from] std::io::Error),
    #[error("Crewmesh credentials are unreadable: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("the Crewmesh API rejected the token")]
    Unauthorized,
    /// The token is real, and the account is not Pro. The string is safe to show a user.
    #[error("{0}")]
    NotPro(String),
    /// The relay has no peer under that name.
    #[error("{0}")]
    Unknown(String),
    #[error("Crewmesh API error ({status}): {body}")]
    Api { status: u16, body: String },
    #[error("could not reach the Crewmesh API at {url}: {message}")]
    Network { url: String, message: String },
}
