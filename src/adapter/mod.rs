//! The private seam between the session engine and protocol adapters.
//!
//! An adapter connects to one agent and translates its wire into the driver
//! vocabulary below. It never decides turn rules: the engine owns start,
//! steer-or-queue, request lifetimes, completion, and cleanup.

use std::num::NonZeroU32;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::agent::{
    AgentDetails, AgentInstallation, ConfigId, ConfigValue, Input, ResumeToken, RollbackScope,
    SessionConfiguration, SessionOptions,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Diagnostic, DiagnosticLevel, EventKind, Extensions, RequestId, StopReason, ToolId,
};

pub(crate) mod acp;
pub(crate) mod attach;
pub(crate) mod claude;
pub(crate) mod codex;
#[cfg(test)]
mod conformance;
#[cfg(test)]
pub(crate) mod mock;
pub(crate) mod opencode;
pub(crate) mod pi;

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
    Configure(ConfigId, ConfigValue),
    Rollback(NonZeroU32, RollbackScope),
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
    /// Acknowledges `StartTurn`, sent before any of the new turn's events.
    /// Content and turn ends delivered before it belong to earlier turns;
    /// the engine drops them instead of attributing them to the new one.
    TurnAck,
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

/// The per-agent config-home environment override for this session, as
/// `envs` for the child, or an empty set when `config_home` is unset. Fails
/// typed for an agent with no known config-home variable (an ad-hoc ACP
/// install), so an isolation request is never silently dropped.
pub(crate) fn config_home_env(
    installation: &AgentInstallation,
    options: &SessionOptions,
) -> Result<Vec<(String, String)>, AgentError> {
    let Some(dir) = &options.config_home else {
        return Ok(Vec::new());
    };
    match crate::catalog::config_home_env(installation.id.as_str()) {
        Some(var) => Ok(vec![(var.to_owned(), dir.to_string_lossy().into_owned())]),
        None => Err(AgentError::InvalidConfiguration(format!(
            "{} has no config-home environment variable to isolate its login",
            installation.id
        ))),
    }
}

/// Tees raw protocol frames to a JSONL file when `record_wire` is set: one
/// `{"dir":"in"|"out","frame":<frame>}` per line, append-only and flushed per
/// line so a crash keeps the tail. It is a plain local debug artifact — no
/// buffering, rotation, or redaction. A write failure is reported once as a
/// `Diagnostic` and then dropped; recording never fails a turn.
#[derive(Clone)]
pub(crate) struct WireRecorder {
    lines: mpsc::UnboundedSender<Vec<u8>>,
}

impl WireRecorder {
    /// The session's recorder when `record_wire` is set; `None` otherwise.
    /// An open failure is surfaced as one diagnostic on `events` and recording
    /// stays off, rather than failing the session.
    pub(crate) async fn for_session(
        options: &SessionOptions,
        events: &mpsc::Sender<DriverEvent>,
    ) -> Option<Self> {
        let path = options.record_wire.as_deref()?;
        let events = events.clone();
        let file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                warn(&events, format!("wire recording is off: {e}")).await;
                return None;
            }
        };
        let (lines, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            let mut file = file;
            while let Some(bytes) = rx.recv().await {
                if let Err(e) = append(&mut file, &bytes).await {
                    warn(&events, format!("wire recording stopped: {e}")).await;
                    break; // report once, then drop the rest silently
                }
            }
        });
        Some(Self { lines })
    }

    /// Records one frame in the given direction. Never blocks and never errors
    /// the caller: a full or gone writer just loses the frame.
    pub(crate) fn record(&self, dir: &'static str, frame: &Value) {
        let mut line = json!({ "dir": dir, "frame": frame }).to_string();
        line.push('\n');
        let _ = self.lines.send(line.into_bytes());
    }
}

async fn append(file: &mut tokio::fs::File, bytes: &[u8]) -> std::io::Result<()> {
    file.write_all(bytes).await?;
    file.flush().await
}

async fn warn(events: &mpsc::Sender<DriverEvent>, message: String) {
    let _ = events
        .send(DriverEvent::event(EventKind::Diagnostic(Diagnostic {
            level: DiagnosticLevel::Warning,
            message,
        })))
        .await;
}

/// Runnable login methods from the catalog, for a logged-out handshake and
/// for mid-session auth loss.
pub(crate) fn login_methods(installation: &AgentInstallation) -> Vec<crate::agent::LoginMethod> {
    crate::catalog::PROFILES
        .iter()
        .find(|p| p.id == installation.id.as_str())
        .map(|p| crate::discovery::login_methods(p, &installation.executable_path))
        .unwrap_or_default()
}

/// Adds the child's stderr to a handshake failure (a logged-out CLI prints
/// its complaint there and closes the wire).
pub(crate) fn with_stderr(error: AgentError, child: &crate::process::Child) -> AgentError {
    let stderr = child.stderr_tail();
    match (error, stderr.is_empty()) {
        (AgentError::ProtocolFailed(message), false) => {
            AgentError::ProtocolFailed(format!("{message}: {stderr}"))
        }
        (error, _) => error,
    }
}

/// Truncates to `at` bytes on a char boundary; tool output stays bounded.
pub(crate) fn cap(mut s: String, at: usize) -> String {
    if s.len() > at {
        let mut end = at;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

#[derive(Clone)]
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

    /// Plan quota for the logged-in account, from a short-lived process.
    /// Default: this agent has no quota to report.
    async fn plan_usage(
        &self,
        installation: &AgentInstallation,
    ) -> Result<crate::event::PlanUsage, AgentError> {
        let _ = installation;
        Err(AgentError::UnsupportedFeature("plan usage".into()))
    }
}

/// A `todos` array (`{content, status}` items, the Claude/opencode shape) as
/// plan entries.
pub(crate) fn plan_entries(todos: &Value) -> Vec<crate::event::PlanEntry> {
    use crate::event::{PlanEntry, PlanStatus};
    todos
        .as_array()
        .into_iter()
        .flatten()
        .map(|todo| PlanEntry {
            text: todo["content"].as_str().unwrap_or_default().to_owned(),
            status: match todo["status"].as_str().unwrap_or_default() {
                "in_progress" => PlanStatus::InProgress,
                "completed" => PlanStatus::Completed,
                _ => PlanStatus::Pending,
            },
        })
        .collect()
}
