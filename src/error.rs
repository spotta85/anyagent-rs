//! Public error type. Adapters and the engine both speak it.

use crate::agent::{AgentId, LoginMethod};

/// Every failure a caller can see. Non-exhaustive so new cases do not break consumers.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum AgentError {
    #[error("agent not installed: {0}")]
    NotInstalled(AgentId),
    #[error("could not start agent: {0}")]
    SpawnFailed(String),
    #[error("agent needs login")]
    AuthRequired { login: Vec<LoginMethod> },
    #[error("agent did not finish its handshake in time")]
    HandshakeTimeout,
    #[error("this agent does not support {0}")]
    UnsupportedFeature(String),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("could not resume session: {0}")]
    ResumeFailed(String),
    #[error("session is busy with a running turn")]
    SessionBusy,
    #[error("protocol failure: {0}")]
    ProtocolFailed(String),
    #[error("agent process exited ({status}): {stderr}")]
    ProcessExited { status: String, stderr: String },
    #[error("session is closed")]
    SessionClosed,
}
