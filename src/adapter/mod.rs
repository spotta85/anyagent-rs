//! The private seam between the session engine and protocol adapters.
//!
//! An adapter connects to one agent and translates its wire into the driver
//! vocabulary below. It never decides turn rules: the engine owns start,
//! steer-or-queue, request lifetimes, completion, and cleanup.

use std::num::NonZeroU32;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::agent::{
    AgentDetails, AgentInstallation, ConfigSelection, Input, ResumeToken, SessionConfiguration,
    SessionOptions,
};
use crate::error::AgentError;
use crate::event::{Answer, EventKind, Extensions, RequestId, StopReason, ToolId};

pub(crate) mod acp;
pub(crate) mod attach;
pub(crate) mod claude;
#[cfg(test)]
mod conformance;
#[cfg(test)]
pub(crate) mod mock;

/// What the engine asks an adapter to do.
#[derive(Debug)]
pub(crate) enum DriverCommand {
    /// Begin a turn with this input. A wire rejection is reported as
    /// `DriverEvent::TurnEnded(Failed)`.
    StartTurn {
        input: Input,
    },
    /// Inject into the running turn. Must be answered with `DriverEvent::Steered`.
    Steer {
        input: Input,
    },
    Answer {
        request: RequestId,
        answer: Answer,
    },
    Configure(ConfigSelection),
    Rollback(NonZeroU32),
    /// Interrupt the running turn. The adapter also drops any pending wire
    /// requests; the engine has already emitted their `RequestClosed`.
    Cancel,
    /// End the provider session and let the event stream close.
    Close,
}

/// What an adapter reports back.
#[derive(Debug)]
// Driver events cross a bounded channel; boxing would allocate every content frame.
#[allow(clippy::large_enum_variant)]
pub(crate) enum DriverEvent {
    /// Normalized content. The engine adds the session, turn, and sequence
    /// envelope. Adapters never send engine-owned kinds (`TurnStarted`,
    /// `TurnEnded`, `RequestClosed`, `SessionUpdated`); those are dropped.
    Event {
        kind: EventKind,
        parent_tool_id: Option<ToolId>,
        extensions: Extensions,
    },
    /// Wire evidence that the current turn ended.
    TurnEnded(StopReason),
    /// Outcome of the last `Steer` command.
    Steered(bool),
    /// The agent changed advertised details or configuration.
    InfoChanged(DriverInfo),
    /// The agent's credentials stopped working mid-session. The engine fails
    /// the turn, surfaces `AuthRequired { login }`, and closes the session.
    AuthLost {
        login: Vec<crate::agent::LoginMethod>,
    },
    /// The agent process died: how it exited and its last stderr lines.
    /// Sent once, right before the adapter closes the event channel.
    Exited { status: String, stderr: String },
}

impl DriverEvent {
    pub(crate) fn event(kind: EventKind) -> Self {
        DriverEvent::Event {
            kind,
            parent_tool_id: None,
            extensions: Extensions::new(),
        }
    }
}

/// Facts the engine needs about the connection it just got.
#[derive(Debug, Clone)]
pub(crate) struct DriverInfo {
    pub details: AgentDetails,
    pub configuration: SessionConfiguration,
    pub resume_token: Option<ResumeToken>,
    pub title: Option<String>,
    /// The wire ends prompted turns with its own terminal frame. When false,
    /// the engine infers completion after a quiet window.
    pub deterministic_turn_end: bool,
    /// Same for agent-originated (background wake) turns.
    pub deterministic_agent_turn_end: bool,
}

/// Applies a confirmed option change to the advertised state, returning
/// whether anything actually changed (callers skip `InfoChanged` otherwise).
pub(crate) fn apply_selection(
    info: &mut DriverInfo,
    id: &crate::agent::ConfigId,
    value: &crate::agent::ConfigValue,
) -> bool {
    let stored = info.configuration.options.get(id);
    if stored == Some(value) {
        return false;
    }
    info.configuration.options.insert(id.clone(), value.clone());
    if let Some(option) = info.details.config_options.iter_mut().find(|o| &o.id == id) {
        option.current = Some(value.clone());
    }
    true
}

pub(crate) struct ConnectRequest {
    pub installation: AgentInstallation,
    pub options: SessionOptions,
}

pub(crate) struct DriverConnection {
    pub info: DriverInfo,
    pub commands: mpsc::Sender<DriverCommand>,
    pub events: mpsc::Receiver<DriverEvent>,
}

#[async_trait]
pub(crate) trait Adapter: Send + Sync {
    /// Launch, handshake, and create the provider session.
    async fn connect(&self, request: ConnectRequest) -> Result<DriverConnection, AgentError>;
}
