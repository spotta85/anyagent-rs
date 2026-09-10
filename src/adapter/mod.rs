//! The private seam between the session engine and protocol adapters.
//!
//! An adapter connects to one agent and translates its wire into the driver
//! vocabulary below. It never decides turn rules: the engine owns start,
//! steer-or-queue, request lifetimes, completion, and cleanup.
//!
//! High level: the `Adapter` trait (`connect`, `plan_usage`), the
//! `DriverCommand` / `DriverEvent` vocabulary, and the helpers every adapter
//! shares: `Emitter` (events to the engine), option helpers, login methods,
//! and error mapping.

use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::agent::{
    AgentDetails, AgentInstallation, ConfigChoice, ConfigId, ConfigKind, ConfigOption, ConfigValue,
    Input, LoginMethod, ResumeToken, RollbackScope, SessionConfiguration, SessionOptions,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Diagnostic, DiagnosticLevel, EventKind, Extensions, RequestId, StopReason, ToolId,
};

pub(crate) mod acp;
pub(crate) mod antigravity;
pub(crate) mod attach;
pub(crate) mod claude;
pub(crate) mod codex;
#[cfg(test)]
mod conformance;
#[cfg(any(test, feature = "mock"))]
pub mod mock;
pub(crate) mod opencode;
pub(crate) mod pi;
pub(crate) mod wire;

pub(crate) use wire::{FRAME_BUFFER, LineWire, WireRecorder};

/// Launch plus handshake must finish within this.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// SIGTERM grace before SIGKILL when a child is shut down.
pub(crate) const CLOSE_GRACE: Duration = Duration::from_secs(2);
/// Tool output kept per snapshot; the rest is truncated.
pub(crate) const OUTPUT_CAP: usize = 16 * 1024;

// ---------------------------------------------------------------------------
// SEAM: what the engine sends and what adapters report
// ---------------------------------------------------------------------------

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
    /// Summarize the session's context now. Most wires run it as a turn of
    /// their own; the adapter only triggers it and lets its frames decode.
    Compact,
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
    /// A content event with no parent and no extensions.
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
    /// The adapter honoured `SessionOptions::no_tools`: the agent cannot
    /// run any tool this session.
    pub tools_disabled: bool,
    /// The wire id behind the `effort` option when the agent names it
    /// differently (qwen: `reasoning_effort`); `None` when it is `effort`.
    pub effort_wire: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ConnectRequest {
    pub installation: AgentInstallation,
    pub options: SessionOptions,
}

pub(crate) struct DriverConnection {
    pub info: DriverInfo,
    /// Unbounded so the engine never parks on a busy adapter (commands are
    /// app-driven and small; a full bounded channel could deadlock with the
    /// adapter waiting on the event channel).
    pub commands: mpsc::UnboundedSender<DriverCommand>,
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

// ---------------------------------------------------------------------------
// DRIVE-TASK HELPERS: sending to the engine
// ---------------------------------------------------------------------------

/// The engine or the agent is gone; the drive task unwinds.
pub(crate) struct Gone;

impl From<std::io::Error> for Gone {
    fn from(_: std::io::Error) -> Self {
        Gone
    }
}

/// A drive task's channel to the engine, with the shapes every adapter
/// sends. Every send fails with `Gone` once the engine dropped its receiver.
#[derive(Clone)]
pub(crate) struct Emitter(mpsc::Sender<DriverEvent>);

impl Emitter {
    /// Wraps the sending half of the driver event channel.
    pub(crate) fn new(events: mpsc::Sender<DriverEvent>) -> Self {
        Self(events)
    }

    /// Any driver event, as is.
    pub(crate) async fn send(&self, event: DriverEvent) -> Result<(), Gone> {
        self.0.send(event).await.map_err(|_| Gone)
    }

    /// A content event with no parent and no extensions.
    pub(crate) async fn event(&self, kind: EventKind) -> Result<(), Gone> {
        self.send(DriverEvent::event(kind)).await
    }

    /// A content event attributed to a subagent tool, with extensions.
    pub(crate) async fn content(
        &self,
        kind: EventKind,
        parent_tool_id: Option<ToolId>,
        extensions: Extensions,
    ) -> Result<(), Gone> {
        self.send(DriverEvent::Event {
            kind,
            parent_tool_id,
            extensions,
        })
        .await
    }

    /// A diagnostic outside any tool or subagent.
    pub(crate) async fn diagnostic(
        &self,
        level: DiagnosticLevel,
        message: impl Into<String>,
    ) -> Result<(), Gone> {
        self.event(EventKind::Diagnostic(Diagnostic {
            level,
            message: message.into(),
        }))
        .await
    }

    /// The agent went away: report how it died before the stream closes.
    pub(crate) async fn exited(&self, child: &mut crate::process::Child) {
        let status = child.exit_status(CLOSE_GRACE).await;
        let stderr = child.stderr_tail();
        self.send(DriverEvent::Exited { status, stderr }).await.ok();
    }
}

// ---------------------------------------------------------------------------
// OPTION HELPERS: advertised config options and their current values
// ---------------------------------------------------------------------------

/// Applies a confirmed option change to the advertised state, returning
/// whether anything actually changed (callers skip `InfoChanged` otherwise).
pub(crate) fn apply_selection(info: &mut DriverInfo, id: &ConfigId, value: &ConfigValue) -> bool {
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

/// The selected text value of option `id`, if any.
pub(crate) fn selected(info: &DriverInfo, id: &str) -> Option<String> {
    match info.configuration.options.get(&ConfigId::new(id)) {
        Some(ConfigValue::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

/// Whether the advertised select `id` offers `value`.
pub(crate) fn offers(info: &DriverInfo, id: &str, value: &ConfigValue) -> bool {
    info.details.config_options.iter().any(|o| {
        o.id.as_str() == id
            && matches!((&o.kind, value), (ConfigKind::Select { choices }, ConfigValue::Text(v))
                if choices.iter().any(|c| &c.value == v))
    })
}

/// Replaces the select option `id` with these choices and current value;
/// no choices means no option. `current` is dropped unless offered.
pub(crate) fn set_select_option(
    info: &mut DriverInfo,
    id: &str,
    name: &str,
    category: &str,
    choices: Vec<ConfigChoice>,
    current: Option<String>,
) {
    let id = ConfigId::new(id);
    info.details.config_options.retain(|o| o.id != id);
    info.configuration.options.remove(&id);
    if choices.is_empty() {
        return;
    }
    let current = current
        .filter(|c| choices.iter().any(|choice| &choice.value == c))
        .map(ConfigValue::Text);
    if let Some(current) = &current {
        info.configuration
            .options
            .insert(id.clone(), current.clone());
    }
    info.details.config_options.push(ConfigOption {
        id,
        name: name.into(),
        category: Some(category.into()),
        kind: ConfigKind::Select { choices },
        current,
        live: true,
    });
}

/// The `effort` option follows the selected model: these are the new
/// model's levels, and the old selection survives only when still offered.
pub(crate) fn set_effort_option(
    info: &mut DriverInfo,
    choices: Vec<ConfigChoice>,
    current: Option<String>,
) {
    set_select_option(
        info,
        "effort",
        "Reasoning effort",
        "thought_level",
        choices,
        current,
    );
}

/// Levels as plain choices (value = label), for wires that list them by name.
pub(crate) fn level_choices<'a>(levels: impl IntoIterator<Item = &'a str>) -> Vec<ConfigChoice> {
    levels
        .into_iter()
        .map(|level| ConfigChoice {
            value: level.to_owned(),
            label: level.to_owned(),
            description: None,
        })
        .collect()
}

/// Shows Fast mode only for supported models, keeping its value in sync.
pub(crate) fn set_fast_option(info: &mut DriverInfo, current: Option<bool>, live: bool) {
    let id = ConfigId::new("fast");
    info.details.config_options.retain(|option| option.id != id);
    info.configuration.options.remove(&id);
    if let Some(current) = current {
        let value = ConfigValue::Bool(current);
        let position = info
            .details
            .config_options
            .iter()
            .position(|option| option.id.as_str() == "model")
            .map_or(0, |index| index + 1);
        info.details.config_options.insert(
            position,
            ConfigOption {
                id: id.clone(),
                name: "Fast mode".into(),
                category: Some("speed".into()),
                kind: ConfigKind::Boolean,
                current: Some(value.clone()),
                live,
            },
        );
        info.configuration.options.insert(id, value);
    }
}

/// A `todos`-style array as plan entries; `key` names the text field
/// (`content` for claude and opencode, `step` for codex).
pub(crate) fn plan_entries(todos: &Value, key: &str) -> Vec<crate::event::PlanEntry> {
    use crate::event::{PlanEntry, PlanStatus};
    todos
        .as_array()
        .into_iter()
        .flatten()
        .map(|todo| PlanEntry {
            text: todo[key].as_str().unwrap_or_default().to_owned(),
            status: match todo["status"].as_str().unwrap_or_default() {
                "in_progress" | "inProgress" => PlanStatus::InProgress,
                "completed" => PlanStatus::Completed,
                _ => PlanStatus::Pending,
            },
        })
        .collect()
}

// ---------------------------------------------------------------------------
// LAUNCH HELPERS: environment, login methods, error mapping
// ---------------------------------------------------------------------------

/// The per-agent config-home override for this session as child `envs`, or
/// empty when `config_home` is unset. Fails typed for an agent with no known
/// variable, so an isolation request is never silently dropped.
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

/// Runnable login methods from the catalog, for a logged-out handshake and
/// for mid-session auth loss. Given the session's options they carry its
/// config-home variable, so the login lands where the session looks.
pub(crate) fn login_methods(
    installation: &AgentInstallation,
    options: Option<&SessionOptions>,
) -> Vec<LoginMethod> {
    let login = crate::catalog::PROFILES
        .iter()
        .find(|p| p.id == installation.id.as_str())
        .map(|p| crate::discovery::login_methods(p, &installation.executable_path))
        .unwrap_or_default();
    let env = options
        .and_then(|o| config_home_env(installation, o).ok())
        .unwrap_or_default();
    login_in(login, &env)
}

/// Terminal login methods with the session's config-home variable attached.
pub(crate) fn login_in(mut login: Vec<LoginMethod>, env: &[(String, String)]) -> Vec<LoginMethod> {
    for method in &mut login {
        if let LoginMethod::Terminal { env: vars, .. } = method {
            vars.extend(env.iter().cloned());
        }
    }
    login
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

/// Types a logged-out open as `AuthRequired` with login methods that carry
/// the session's config-home variable. An error already typed is stamped;
/// a failure is matched against the profile's probed fingerprints (kiro
/// exits before speaking ACP, hermes fails session/new with a plain
/// internal error, agy exits before `init`); anything else passes.
pub(crate) fn auth_hinted(
    error: AgentError,
    profile: Option<&crate::catalog::AgentProfile>,
    exe: &Path,
    env: &[(String, String)],
) -> AgentError {
    if let AgentError::AuthRequired { login } = error {
        return AgentError::AuthRequired {
            login: login_in(login, env),
        };
    }
    let Some(profile) = profile else { return error };
    // Only failure shapes carry the agent's own words; other typed errors
    // (UnsupportedFeature, …) must pass through untouched.
    if !matches!(
        error,
        AgentError::ProtocolFailed(_) | AgentError::ProcessExited { .. }
    ) {
        return error;
    }
    let text = error.to_string().to_lowercase();
    if !profile
        .auth_error_hints
        .iter()
        .any(|h| text.contains(&h.to_lowercase()))
    {
        return error;
    }
    // An ACP upgrade has no login of its own: the base CLI's is the flow.
    let exe = match &profile.upgrade {
        Some(u) if exe.file_name().is_some_and(|n| n == u.cli) => Path::new(profile.cli),
        _ => exe,
    };
    let mut login = crate::discovery::login_methods(profile, exe);
    // No login command in the catalog (grok, agy): the agent's own TUI is
    // the flow.
    if !login
        .iter()
        .any(|m| matches!(m, LoginMethod::Terminal { .. }))
    {
        login.insert(
            0,
            LoginMethod::Terminal {
                description: format!("{} logs in on launch; run it in a terminal", profile.name),
                command: vec![exe.to_string_lossy().into_owned()],
                env: std::collections::BTreeMap::new(),
            },
        );
    }
    AgentError::AuthRequired {
        login: login_in(login, env),
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
