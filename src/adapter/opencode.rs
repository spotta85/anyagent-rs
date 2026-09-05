//! Native opencode adapter: drives `opencode serve` over its HTTP + SSE wire
//! (validated live 2026-09-03 against opencode 1.18.24). One server process
//! per session, bound to a free localhost port with the session dir as cwd.
//! Commands are HTTP POSTs; content arrives on the `/event` SSE bus, filtered
//! to this session. Turn end is deterministic: the `session.idle` that
//! follows the server going busy for our prompt, confirmed against the
//! server's status (an abort republishes idle once more, late).
//!
//! We drive the legacy ("v1") engine on purpose. It emits permission,
//! question, and idle events the "v2" engine makes a client poll for, and its
//! fork works. The one thing v1 lacks is steering, which opencode has over ACP
//! too; the engine queues prompts instead. All routes and event names live in
//! this file so a future v2 swap is contained.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::adapter::plan_entries;
use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
    WireRecorder, apply_selection, attach, cap, login_methods,
};
use crate::agent::{
    AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigChoice, ConfigId,
    ConfigKind, ConfigOption, ConfigValue, Input, LoginMethod, PermissionMode, ResumeToken,
    SessionConfiguration, SessionOptions, SessionStart, SlashCommand,
};
use crate::error::AgentError;
use crate::event::Extensions;
use crate::event::{
    Answer, Choice, ChoiceId, CompletionSource, Diagnostic, DiagnosticLevel, EventKind, MessageId,
    PermissionChoice, PermissionRequest, Question, QuestionAnswer, QuestionId, QuestionRequest,
    RawTool, Request, RequestId, StopReason, ToolId, ToolInput, ToolKind, ToolStatus, ToolUpdate,
};
use crate::process::{self, Spawn};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a taken prompt may sit without the server going busy.
const ADMIT_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_GRACE: Duration = Duration::from_secs(2);
const HEALTH_POLL: Duration = Duration::from_millis(100);
const FRAME_BUFFER: usize = 64;
const OUTPUT_CAP: usize = 16 * 1024;

/// Launches `opencode serve` and speaks its HTTP wire.
pub(crate) struct OpencodeAdapter;

impl OpencodeAdapter {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Adapter for OpencodeAdapter {
    /// Spawns the server, creates or resumes the session, reads the catalogs,
    /// and hands the live SSE bus to the drive task.
    async fn connect(&self, request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let (ev_tx, ev_rx) = mpsc::channel(FRAME_BUFFER);
        let recorder = WireRecorder::for_session(&request.options, &ev_tx).await;
        let launched = launch(&request, recorder).await?;
        let info = launched.info.clone();
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        tokio::spawn(
            Drive {
                http: launched.http,
                server: launched.server,
                frames: launched.frames,
                events: ev_tx,
                info: launched.info,
                session_id: launched.session_id,
                windows: launched.windows,
                variants: launched.variants,
                login: login_methods(&request.installation),
                messages: HashMap::new(),
                ended: HashSet::new(),
                tide: String::new(),
                parts: HashMap::new(),
                tools: HashMap::new(),
                children: HashSet::new(),
                child_of: HashMap::new(),
                spawn_order: Vec::new(),
                child_messages: HashMap::new(),
                requests: HashMap::new(),
                cost: 0.0,
                turn: Turn::Idle,
                admit_deadline: None,
                aborting: false,
                turn_error: None,
                next_message: 0,
                next_request: 0,
            }
            .run(cmd_rx),
        );
        Ok(DriverConnection {
            info,
            commands: cmd_tx,
            events: ev_rx,
        })
    }
}

// ---------------------------------------------------------------------------
// Launch and handshake
// ---------------------------------------------------------------------------

/// A booted server with its session bound.
struct Launched {
    server: process::Child,
    http: Http,
    frames: mpsc::Receiver<Value>,
    info: DriverInfo,
    session_id: String,
    windows: HashMap<String, u64>,
    variants: HashMap<String, Vec<String>>,
}

/// Spawns the server, waits for health, subscribes to the event bus, binds
/// the session, and reads the catalogs behind the advertised options.
async fn launch(
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
) -> Result<Launched, AgentError> {
    if !request.options.mcp_servers.is_empty() {
        // Client MCP servers would need a `POST /mcp` per server after open;
        // deferred until a consumer needs it.
        return Err(AgentError::UnsupportedFeature(
            "client-declared MCP servers on opencode".into(),
        ));
    }
    let port = free_port()?;
    let secret = secret();
    let mut server = spawn_server(request, port, &secret).await?;
    let http = Http {
        port,
        directory: request.options.cwd().to_string_lossy().into_owned(),
        auth: format!(
            "Basic {}",
            attach::base64(format!("opencode:{secret}").as_bytes())
        ),
        recorder: recorder.clone(),
    };
    // The piped stdout must be drained or the server blocks on a full pipe.
    drain(server.stdout.take());
    let boot = async {
        let version = await_health(&http).await?;
        let frames = open_bus(&http, recorder.clone()).await?;
        let (info, session_id, windows, variants) =
            handshake(&http, request, recorder, version).await?;
        Ok((frames, info, session_id, windows, variants))
    };
    let outcome = tokio::time::timeout(HANDSHAKE_TIMEOUT, boot).await;
    // A squatter on the picked port (the pick-then-bind race) makes opencode
    // exit at once, so whoever answered above was not our server.
    if !server.is_running() {
        let status = server.exit_status(CLOSE_GRACE).await;
        let stderr = server.stderr_tail();
        return Err(AgentError::ProcessExited { status, stderr });
    }
    match outcome {
        Ok(Ok((frames, info, session_id, windows, variants))) => Ok(Launched {
            server,
            http,
            frames,
            info,
            session_id,
            windows,
            variants,
        }),
        Ok(Err(e)) => {
            let e = crate::adapter::with_stderr(e, &server);
            server.shutdown(CLOSE_GRACE).await;
            Err(e)
        }
        Err(_) => {
            server.shutdown(CLOSE_GRACE).await;
            Err(AgentError::HandshakeTimeout)
        }
    }
}

/// Launches `opencode serve` on the given port, with the session dir as cwd,
/// the basic-auth gate keyed to this session, and the permission mode as
/// config.
async fn spawn_server(
    request: &ConnectRequest,
    port: u16,
    secret: &str,
) -> Result<process::Child, AgentError> {
    let mut env = crate::adapter::config_home_env(&request.installation, &request.options)?;
    // The server's basic-auth gate: a per-session secret, so no other local
    // process can drive the port.
    env.push(("OPENCODE_SERVER_USERNAME".into(), "opencode".into()));
    env.push(("OPENCODE_SERVER_PASSWORD".into(), secret.to_owned()));
    env.push((
        "OPENCODE_CONFIG_CONTENT".into(),
        config_content(request.options.permission_mode).to_string(),
    ));
    process::spawn(Spawn {
        exec_path: request.installation.executable_path.clone(),
        args: vec![
            "serve".into(),
            "--hostname".into(),
            "127.0.0.1".into(),
            "--port".into(),
            port.to_string(),
        ],
        cwd: request.options.cwd().clone(),
        env,
    })
    .await
}

/// Waits for the server, binds the session, and folds the catalogs into the
/// engine vocabulary.
async fn handshake(
    http: &Http,
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
    version: Option<String>,
) -> Result<
    (
        DriverInfo,
        String,
        HashMap<String, u64>,
        HashMap<String, Vec<String>>,
    ),
    AgentError,
> {
    let providers = http.get("/config/providers").await?;
    let connected = http.get("/provider").await?["connected"].clone();
    let commands = http.get("/command").await?;
    if let Some(recorder) = &recorder {
        recorder.record("in", &providers);
    }
    let session = bind_session(http, request).await?;
    let session_id = session["id"]
        .as_str()
        .ok_or_else(|| AgentError::ProtocolFailed("session create returned no id".into()))?
        .to_owned();
    let choices = model_choices(&providers, &connected);
    let (model, effort) = start_config(&request.options, &choices)?;
    let mut info = driver_info(
        choices,
        &commands,
        &session,
        auth_status(request, &connected),
        version,
    );
    // Creation-time `configure` overrides the session defaults (v1 has no
    // model endpoint, so model and effort ride every prompt).
    if let Some(model) = model {
        apply_selection(
            &mut info,
            &ConfigId::new("model"),
            &ConfigValue::Text(model),
        );
    }
    let variants = model_variants(&providers);
    sync_effort(&mut info, &variants);
    if let Some(effort) = effort {
        let value = ConfigValue::Text(effort.clone());
        if !offers(&info, "effort", &value) {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{effort}` is not a choice for `effort`"
            )));
        }
        apply_selection(&mut info, &ConfigId::new("effort"), &value);
    }
    // The `ses_…` id is the durable handle: resuming re-adopts it.
    info.resume_token = Some(ResumeToken::new(&session_id));
    Ok((info, session_id, model_windows(&providers), variants))
}

/// Polls `/global/health` until the server answers, returning its version.
async fn await_health(http: &Http) -> Result<Option<String>, AgentError> {
    loop {
        if let Ok(health) = http.get("/global/health").await {
            return Ok(health["version"].as_str().map(str::to_owned));
        }
        tokio::time::sleep(HEALTH_POLL).await;
    }
}

/// Creates, resumes, or forks the provider session and returns its record.
async fn bind_session(http: &Http, request: &ConnectRequest) -> Result<Value, AgentError> {
    match &request.options.start {
        SessionStart::New => http.post("/session", json!({})).await,
        // Re-adopting the id IS the resume: opencode scopes history by it.
        SessionStart::Resume(token) => http
            .get(&format!("/session/{}", token.as_str()))
            .await
            .map_err(|e| AgentError::ResumeFailed(e.to_string())),
        SessionStart::Fork { from, at } => {
            let body = match at {
                None => json!({}),
                Some(at) => {
                    let messages = http
                        .get(&format!("/session/{}/message", from.as_str()))
                        .await?;
                    fork_body(&messages, at.as_str())?
                }
            };
            http.post(&format!("/session/{}/fork", from.as_str()), body)
                .await
        }
    }
}

/// opencode reports login only as which providers are connected; an empty
/// list is the one honest "logged out" (probed 2026-09-03). A connected
/// provider proves a working credential, not its kind — the offline
/// marker's kind stands where discovery read one.
fn auth_status(request: &ConnectRequest, connected: &Value) -> AuthStatus {
    let any = connected.as_array().is_some_and(|c| !c.is_empty());
    if !any {
        return AuthStatus::Unauthenticated {
            login: login_methods(&request.installation),
        };
    }
    match &request.installation.auth {
        Some(auth @ AuthStatus::Authenticated { .. }) => auth.clone(),
        _ => AuthStatus::Authenticated {
            kind: AuthKind::Other("connected provider".into()),
            account: None,
        },
    }
}

/// The server's `OPENCODE_CONFIG_CONTENT`: the host's own value, if any (a
/// sandbox or tool policy survives), with our permission rules layered on.
fn config_content(mode: PermissionMode) -> Value {
    let host = std::env::var("OPENCODE_CONFIG_CONTENT")
        .ok()
        .and_then(|c| serde_json::from_str::<Value>(&c).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    with_permission(host, permission_config(mode))
}

/// Layers permission rules onto a config, ours winning on the same key.
fn with_permission(mut config: Value, ours: Value) -> Value {
    let mut permission = config["permission"].take();
    match permission.as_object_mut() {
        Some(host) => host.extend(ours.as_object().cloned().unwrap_or_default()),
        None => permission = ours,
    }
    config["permission"] = permission;
    config
}

/// The permission rules for a mode. `Ask` forces every tool to prompt but
/// lets the question tool run so it surfaces as a question, not a permission.
fn permission_config(mode: PermissionMode) -> Value {
    match mode {
        PermissionMode::Ask => json!({ "*": "ask", "question": "allow" }),
        PermissionMode::AutoApprove => json!({ "*": "allow" }),
    }
}

/// Folds the handshake responses into the advertised state.
fn driver_info(
    choices: Vec<ConfigChoice>,
    commands: &Value,
    session: &Value,
    auth: AuthStatus,
    version: Option<String>,
) -> DriverInfo {
    let model = model_value(&session["model"]);
    let mut configuration = SessionConfiguration::default();
    if let Some(model) = &model {
        configuration
            .options
            .insert(ConfigId::new("model"), ConfigValue::Text(model.clone()));
    }
    let config_options = vec![ConfigOption {
        id: ConfigId::new("model"),
        name: "Model".into(),
        category: Some("model".into()),
        kind: ConfigKind::Select { choices },
        current: model.map(ConfigValue::Text),
        live: true,
    }];
    DriverInfo {
        details: AgentDetails {
            version,
            auth,
            // Steer is absent (v1 queues); RollbackFiles, PlanUsage, and
            // Subagents are not on this wire honourably.
            capabilities: Capabilities::new([
                Capability::Images,
                Capability::Resume,
                Capability::Fork,
                Capability::Permissions,
                Capability::Questions,
                Capability::Rollback,
                Capability::Compact,
                Capability::SlashCommands,
                Capability::Plan,
                Capability::ContextUsage,
            ]),
            config_options,
            commands: slash_commands(commands),
        },
        configuration,
        resume_token: None,
        // A fresh session's title is a dated placeholder, not a title.
        title: real_title(&session["title"]),
        // `session.idle` ends every prompted turn.
        deterministic_turn_end: true,
        // No agent-originated turns on opencode; the same signal covers it.
        deterministic_agent_turn_end: true,
    }
}

/// The models of the connected providers as config choices, valued
/// `providerID/modelID` because a prompt needs both halves.
fn model_choices(providers: &Value, connected: &Value) -> Vec<ConfigChoice> {
    let connected: Vec<&str> = connected
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let mut choices = Vec::new();
    for provider in providers["providers"].as_array().into_iter().flatten() {
        let Some(pid) = provider["id"].as_str() else {
            continue;
        };
        if !connected.contains(&pid) {
            continue;
        }
        for (mid, model) in provider["models"].as_object().into_iter().flatten() {
            choices.push(ConfigChoice {
                value: format!("{pid}/{mid}"),
                label: model["name"].as_str().unwrap_or(mid).to_owned(),
                description: None,
            });
        }
    }
    choices
}

/// `providerID/modelID` from a session or provider model object.
fn model_value(model: &Value) -> Option<String> {
    let provider = model["providerID"].as_str()?;
    let id = model["id"].as_str().or_else(|| model["modelID"].as_str())?;
    (!provider.is_empty() && !id.is_empty()).then(|| format!("{provider}/{id}"))
}

/// Every model's context window from the provider catalog, keyed
/// `providerID/modelID`, so a live model switch keeps `ContextUsage` whole.
fn model_windows(providers: &Value) -> HashMap<String, u64> {
    let mut windows = HashMap::new();
    for provider in providers["providers"].as_array().into_iter().flatten() {
        let Some(pid) = provider["id"].as_str() else {
            continue;
        };
        for (mid, model) in provider["models"].as_object().into_iter().flatten() {
            if let Some(window) = model["limit"]["context"].as_u64().filter(|w| *w > 0) {
                windows.insert(format!("{pid}/{mid}"), window);
            }
        }
    }
    windows
}

/// Commands, prompt templates, and skills, all invoked with `/`.
fn slash_commands(commands: &Value) -> Vec<SlashCommand> {
    commands
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|command| {
            Some(SlashCommand {
                name: command["name"].as_str()?.to_owned(),
                description: command["description"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                input_hint: None,
            })
        })
        .collect()
}

/// The model to send with each prompt, from the advertised configuration.
fn current_model(info: &DriverInfo) -> Option<String> {
    match info.configuration.options.get(&ConfigId::new("model")) {
        Some(ConfigValue::Text(model)) => Some(model.clone()),
        _ => None,
    }
}

/// Creation-time `configure` values as (model, effort): only those two,
/// and the model only from the advertised choices (an unknown model would
/// fail the first turn). Effort is checked once the model is known.
fn start_config(
    options: &SessionOptions,
    choices: &[ConfigChoice],
) -> Result<(Option<String>, Option<String>), AgentError> {
    let mut model = None;
    let mut effort = None;
    for (id, value) in &options.configure {
        let slot = match id.as_str() {
            "model" => &mut model,
            "effort" => &mut effort,
            _ => {
                return Err(AgentError::InvalidConfiguration(format!(
                    "`{id}` is not a creation-time option of this agent"
                )));
            }
        };
        let ConfigValue::Text(text) = value else {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{id}` takes a text value"
            )));
        };
        if id.as_str() == "model" && !choices.iter().any(|c| &c.value == text) {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{text}` is not a choice for `model`"
            )));
        }
        *slot = Some(text.clone());
    }
    Ok((model, effort))
}

/// Every model's reasoning variants from the provider catalog, keyed
/// `providerID/modelID` and in ladder order; models without any are absent.
fn model_variants(providers: &Value) -> HashMap<String, Vec<String>> {
    const LADDER: [&str; 7] = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];
    let mut variants = HashMap::new();
    for provider in providers["providers"].as_array().into_iter().flatten() {
        let Some(pid) = provider["id"].as_str() else {
            continue;
        };
        for (mid, model) in provider["models"].as_object().into_iter().flatten() {
            let mut names: Vec<String> = model["variants"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(name, _)| name.clone())
                .collect();
            if names.is_empty() {
                continue;
            }
            names.sort_by_key(|n| LADDER.iter().position(|l| l == n).unwrap_or(LADDER.len()));
            variants.insert(format!("{pid}/{mid}"), names);
        }
    }
    variants
}

/// Makes the `effort` option match the selected model: its variants as
/// choices, the selection kept when the model offers it, and no option for
/// a model without variants. The value rides every prompt as `variant`.
fn sync_effort(info: &mut DriverInfo, variants: &HashMap<String, Vec<String>>) {
    let id = ConfigId::new("effort");
    let current = match info.configuration.options.remove(&id) {
        Some(ConfigValue::Text(effort)) => Some(effort),
        _ => None,
    };
    info.details.config_options.retain(|o| o.id != id);
    let Some(names) = current_model(info).and_then(|model| variants.get(&model)) else {
        return;
    };
    let current = current.filter(|c| names.contains(c)).map(ConfigValue::Text);
    if let Some(current) = &current {
        info.configuration
            .options
            .insert(id.clone(), current.clone());
    }
    info.details.config_options.push(ConfigOption {
        id,
        name: "Reasoning effort".into(),
        category: Some("thought_level".into()),
        kind: ConfigKind::Select {
            choices: names
                .iter()
                .map(|name| ConfigChoice {
                    value: name.clone(),
                    label: name.clone(),
                    description: None,
                })
                .collect(),
        },
        current,
        live: true,
    });
}

/// Whether the advertised select `id` offers `value`.
fn offers(info: &DriverInfo, id: &str, value: &ConfigValue) -> bool {
    info.details.config_options.iter().any(|o| {
        o.id.as_str() == id
            && matches!((&o.kind, value), (ConfigKind::Select { choices }, ConfigValue::Text(v)) if choices.iter().any(|c| &c.value == v))
    })
}

// ---------------------------------------------------------------------------
// Drive task: engine commands out (HTTP), SSE frames in
// ---------------------------------------------------------------------------

/// One open request awaiting the caller's answer. The payload is kept so a
/// refused reply can reopen the request for a retry.
struct Pending {
    reply: Reply,
    request: Request,
}

/// How the answer reaches the server. Permissions reply on their owning
/// session: a task-tool child asks on its own session id.
enum Reply {
    Permission {
        permission_id: String,
        session_id: String,
    },
    Question {
        question_id: String,
    },
}

struct Drive {
    http: Http,
    server: process::Child,
    /// Decoded SSE frames from the `/event` bus; closes when the server dies.
    frames: mpsc::Receiver<Value>,
    events: mpsc::Sender<DriverEvent>,
    info: DriverInfo,
    /// The opencode `ses_…` id this session drives.
    session_id: String,
    /// Login methods for a mid-session credential loss.
    login: Vec<LoginMethod>,
    /// Context window per model, for `ContextUsage`.
    windows: HashMap<String, u64>,
    /// Reasoning variants per model, behind the `effort` option.
    variants: HashMap<String, Vec<String>>,
    /// Assistant `msg_…` id → our streaming message id. Only assistant
    /// messages are minted, so membership gates what streams (the user
    /// message carries a replay of our own prompt).
    messages: HashMap<String, MessageId>,
    /// Messages already closed by a completed `message.updated` (the
    /// completed snapshot republishes, so each closes once).
    ended: HashSet<String>,
    /// Highest assistant `msg_…` id ever minted. Ids sort by creation time,
    /// so an unknown id at or below it is a bookkeeping republish of a
    /// settled turn (an abort replays its message), never new content.
    tide: String,
    /// `prt_…` id → (kind, bytes already emitted), for delta/snapshot dedup.
    parts: HashMap<String, PartState>,
    /// Tool snapshots by `callID`, for the permission that references one.
    tools: HashMap<String, ToolUpdate>,
    /// Task-tool child sessions of this turn; their permission and question
    /// asks reach the caller, and bound children's turn content streams
    /// under their spawning tool call.
    children: HashSet<String>,
    /// Child session id → spawning task-tool call, for transcript nesting.
    /// Bound at `session.created`; cleared with `children` at turn end.
    child_of: HashMap<String, ToolId>,
    /// Own task-tool `callID`s in first-seen order, for oldest-unbound
    /// binding. A child's own task tools stay out: grandchildren bind
    /// through their parent, never through this list.
    spawn_order: Vec<String>,
    /// Child assistant `msg_…` id → spawning task-tool call, so the turn-end
    /// sweep closes stragglers under the right parent.
    child_messages: HashMap<String, ToolId>,
    requests: HashMap<RequestId, Pending>,
    /// Session cost so far, summed over step-finishes.
    cost: f64,
    turn: Turn,
    /// When a taken prompt must have gone busy by; a dropped one fails
    /// loudly instead of hanging the deterministic turn forever.
    admit_deadline: Option<tokio::time::Instant>,
    /// A cancel is in flight, so the next idle is a cancellation.
    aborting: bool,
    /// The running turn's failure, from an assistant message error.
    turn_error: Option<StopReason>,
    next_message: u64,
    next_request: u64,
}

/// Streaming state of one text or reasoning part.
struct PartState {
    kind: PartKind,
    emitted: usize,
}

/// Where the prompted turn is: nothing in flight, prompt sent but the
/// server not yet busy with it, or busy (its idle ends the turn).
#[derive(Clone, Copy, PartialEq)]
enum Turn {
    Idle,
    Sent,
    Busy,
}

#[derive(Clone, Copy, PartialEq)]
enum PartKind {
    Text,
    Reasoning,
}

impl Drive {
    /// Main loop until the engine or the server goes away.
    async fn run(mut self, mut commands: mpsc::Receiver<DriverCommand>) {
        loop {
            tokio::select! {
                cmd = commands.recv() => match cmd {
                    None | Some(DriverCommand::Close) => break,
                    Some(cmd) => {
                        if self.handle_command(cmd).await.is_err() {
                            break;
                        }
                    }
                },
                frame = self.frames.recv() => match frame {
                    Some(frame) => {
                        if self.handle_frame(&frame).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        self.report_exit().await;
                        break;
                    }
                },
                _ = admission(self.admit_deadline), if self.turn == Turn::Sent => {
                    if self.check_admission().await.is_err() {
                        break;
                    }
                }
            }
        }
        self.server.shutdown(CLOSE_GRACE).await;
    }

    /// Turns one engine command into HTTP calls.
    async fn handle_command(&mut self, cmd: DriverCommand) -> Result<(), Gone> {
        match cmd {
            DriverCommand::StartTurn { input } => {
                self.emit(DriverEvent::TurnAck).await?;
                self.turn = Turn::Sent;
                self.aborting = false;
                self.turn_error = None;
                self.start_turn(&input).await?;
            }
            // `summarize` streams the summary and settles like a turn, so
            // the drive loop tracks it as one (probed 2026-09-04, 1.18.27).
            DriverCommand::Compact => self.compact().await?,
            // Never reached: Steer is unadvertised, so the engine queues
            // mid-turn prompts and re-sends them as `StartTurn`.
            DriverCommand::Steer { .. } => self.emit(DriverEvent::Steered(false)).await?,
            DriverCommand::Answer { request, answer } => self.answer(request, answer).await?,
            DriverCommand::Cancel => {
                // Armed only once the server took the abort, so a refused
                // one lets the turn end as what it was.
                let abort = format!("/session/{}/abort", self.session_id);
                match self.http.post(&abort, json!({})).await {
                    Ok(_) => self.aborting = true,
                    Err(e) => {
                        self.diagnostic(DiagnosticLevel::Warning, format!("cancel not taken: {e}"))
                            .await?
                    }
                }
            }
            DriverCommand::Configure(id, value) => self.configure(id, value).await?,
            DriverCommand::Rollback(turns, _) => self.rollback(turns.get()).await?,
            DriverCommand::Close => unreachable!("handled in run"),
        }
        Ok(())
    }

    /// Sends the prompt, or routes an advertised `/command` to its own
    /// endpoint; other `/…` text is plain text. A wire rejection fails the
    /// turn the engine already started.
    async fn start_turn(&mut self, input: &Input) -> Result<(), Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.clone()) {
            self.diagnostic(DiagnosticLevel::Warning, problem).await?;
        }
        let text = attach::with_refs(input.as_text(), &loaded);
        if let Some((command, arguments)) =
            slash_command(&text).filter(|(name, _)| self.has_command(name))
        {
            if loaded.iter().any(|l| l.image.is_some()) {
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    "images do not ride slash commands and were dropped",
                )
                .await?;
            }
            return self.start_command(command, arguments);
        }
        let mut parts = vec![json!({ "type": "text", "text": text })];
        for image in loaded.iter().filter_map(|l| l.image.as_ref()) {
            parts.push(json!({
                "type": "file",
                "mime": image.mime,
                "url": format!("data:{};base64,{}", image.mime, image.base64),
            }));
        }
        let mut body = json!({ "parts": parts });
        if let Some(model) = self.model_body() {
            body["model"] = model;
        }
        if let Some(ConfigValue::Text(effort)) = self
            .info
            .configuration
            .options
            .get(&ConfigId::new("effort"))
        {
            body["variant"] = json!(effort);
        }
        let path = format!("/session/{}/prompt_async", self.session_id);
        match self.http.post(&path, body).await {
            Ok(_) => {
                self.admit_deadline = Some(tokio::time::Instant::now() + ADMIT_TIMEOUT);
                Ok(())
            }
            Err(e) => {
                self.turn = Turn::Idle;
                self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                    message: e.to_string(),
                }))
                .await
            }
        }
    }

    /// Runs a `/command` turn. The endpoint is synchronous (it answers with
    /// the finished message), so the POST runs off the loop with its body
    /// dropped: the bus streams the same turn and its idle ends it, while a
    /// rejection surfaces as a diagnostic and the admission check fails the
    /// turn.
    fn start_command(&mut self, command: String, arguments: String) -> Result<(), Gone> {
        let mut body = json!({ "command": command, "arguments": arguments });
        if let Some(model) = current_model(&self.info) {
            body["model"] = Value::from(model);
        }
        let http = self.http.clone();
        let path = format!("/session/{}/command", self.session_id);
        let events = self.events.clone();
        tokio::spawn(async move {
            if let Err(e) = http.post_unbounded(&path, body).await {
                let kind = EventKind::Diagnostic(Diagnostic {
                    level: DiagnosticLevel::Warning,
                    message: format!("command rejected: {e}"),
                });
                events.send(DriverEvent::event(kind)).await.ok();
            }
        });
        self.admit_deadline = Some(tokio::time::Instant::now() + ADMIT_TIMEOUT);
        Ok(())
    }

    /// Whether `/name` is one of the server's advertised commands.
    fn has_command(&self, name: &str) -> bool {
        self.info.details.commands.iter().any(|c| c.name == name)
    }

    /// Routes one SSE frame by type, ignoring frames for other sessions.
    async fn handle_frame(&mut self, frame: &Value) -> Result<(), Gone> {
        // Insurance against the envelope drift that broke T3 (#2691): a
        // future server may wrap events as `{payload: {type, properties}}`.
        let frame = frame
            .get("payload")
            .filter(|p| p.is_object())
            .unwrap_or(frame);
        let props = &frame["properties"];
        let kind = frame["type"].as_str().unwrap_or_default();
        // Global frames (`server.connected`, installation notices) have no
        // session; session frames not ours are other sessions on the bus.
        // A task-tool child's input requests reach the caller (it stalls
        // parked otherwise), and a bound child's turn content streams under
        // its spawning tool call. Turn state (busy/idle/admission) never
        // listens to child frames, and an abort of the root cascades to
        // children server-side (probed 2026-09-04).
        if let Some(session) = props["sessionID"].as_str()
            && session != self.session_id
        {
            // `session.created` frames carry the newborn's own id; one whose
            // parent is ours registers a child.
            if kind == "session.created" {
                self.on_session_created(&props["info"]);
                return Ok(());
            }
            if !self.children.contains(session) {
                return Ok(());
            }
            // An unbound child's content is dropped: no transcript beats a
            // misattributed one.
            let parent = self.child_of.get(session).cloned();
            return match kind {
                "permission.asked" => self.on_permission(props).await,
                "question.asked" => self.on_question(props).await,
                "message.updated" if parent.is_some() => self.on_message(props, parent).await,
                "message.part.updated" if parent.is_some() => {
                    self.on_part(&props["part"], parent).await
                }
                "message.part.delta" if parent.is_some() => self.on_delta(props, parent).await,
                _ => Ok(()),
            };
        }
        // The generated title lands as post-idle bookkeeping, so it is read
        // before the turn gate.
        if kind == "session.updated" {
            return self.on_session_updated(&props["info"]).await;
        }
        // Bookkeeping trails every idle (diff, re-published messages) and an
        // abort emits a second idle, so only a prompted turn's frames are
        // decoded, and only a busy turn's idle ends it.
        if self.turn == Turn::Idle && kind != "session.error" {
            return Ok(());
        }
        match kind {
            "session.status" if busy_status(&props["status"]) => {
                if self.turn == Turn::Sent {
                    self.turn = Turn::Busy;
                }
                if props["status"]["type"].as_str() == Some("retry") {
                    let message = props["status"]["message"]
                        .as_str()
                        .unwrap_or("the provider is retrying");
                    return self.diagnostic(DiagnosticLevel::Info, message).await;
                }
                Ok(())
            }
            "session.idle" if self.turn == Turn::Busy => {
                // An abort's tail republishes idle, and that copy can land
                // after the next prompt went busy; the server's live status
                // settles whose idle this is (T3 reconciles the same way).
                if self.server_busy().await {
                    Ok(())
                } else {
                    self.on_idle().await
                }
            }
            "message.updated" => self.on_message(props, None).await,
            "message.part.updated" => self.on_part(&props["part"], None).await,
            "message.part.delta" => self.on_delta(props, None).await,
            "permission.asked" => self.on_permission(props).await,
            "question.asked" => self.on_question(props).await,
            "session.compacted" => self.emit_kind(EventKind::ContextCompacted).await,
            "session.error" => self.on_error(props).await,
            // The rest is bookkeeping and reply echoes the engine already owns.
            _ => Ok(()),
        }
    }

    /// An assistant message: mint its streaming id, record any error, and
    /// close it once the server marks it completed (a turn can hold several
    /// assistant messages). The user-role echo of our own prompt is dropped.
    /// A bound child's message streams under its task tool (`parent`): no
    /// tide gate (that guards our own settled-turn republishes) and no turn
    /// failure capture — the child's failure surfaces through its task tool.
    async fn on_message(&mut self, props: &Value, parent: Option<ToolId>) -> Result<(), Gone> {
        let info = &props["info"];
        if info["role"].as_str() != Some("assistant") {
            return Ok(());
        }
        let Some(oc_id) = info["id"].as_str() else {
            return Ok(());
        };
        // A republished message of a settled turn must not re-mint: its
        // part snapshots would stream into the next turn.
        if parent.is_none() && !self.messages.contains_key(oc_id) && oc_id <= self.tide.as_str() {
            return Ok(());
        }
        let message_id = self.message_id(oc_id);
        if let Some(parent) = &parent {
            self.child_messages.insert(oc_id.to_owned(), parent.clone());
        } else if let Some(error) = info.get("error").filter(|e| !e.is_null()) {
            self.turn_error = Some(message_error(error));
        }
        if info["time"]["completed"].is_null() || !self.ended.insert(oc_id.to_owned()) {
            return Ok(());
        }
        self.end_message(oc_id.to_owned(), message_id, parent).await
    }

    /// `MessageEnded`, carrying the message's own id as the fork anchor:
    /// opencode can fork at any message boundary, and ids sort by time.
    /// A child's close rides its task tool and carries no fork anchor.
    async fn end_message(
        &mut self,
        oc_id: String,
        message_id: MessageId,
        parent: Option<ToolId>,
    ) -> Result<(), Gone> {
        let mut extensions = Extensions::new();
        if parent.is_none() {
            extensions.insert("opencode/fork_point".into(), Value::from(oc_id));
        }
        self.emit(DriverEvent::Event {
            kind: EventKind::MessageEnded { message_id },
            parent_tool_id: parent,
            extensions,
        })
        .await
    }

    /// Registers a task-tool child: a newborn session whose parent is us or
    /// one of ours. A direct child binds to its spawning task tool (oldest
    /// still-running spawn without a child of its own); a grandchild binds
    /// through its parent. Parallel same-parent tasks can misattribute —
    /// documented, accepted: nesting under a sibling beats silence, and an
    /// unbound child stays silent rather than guessing.
    fn on_session_created(&mut self, info: &Value) {
        let (Some(id), Some(parent)) = (info["id"].as_str(), info["parentID"].as_str()) else {
            return;
        };
        if parent == self.session_id {
            self.children.insert(id.to_owned());
            if let Some(spawn) = select_spawn(&self.spawn_order, &self.tools, &self.child_of) {
                self.child_of.insert(id.to_owned(), spawn);
            }
        } else if self.children.contains(parent) {
            self.children.insert(id.to_owned());
            if let Some(spawn) = self.child_of.get(parent).cloned() {
                self.child_of.insert(id.to_owned(), spawn);
            }
        }
    }

    /// Post-turn bookkeeping renames the session; a real (non-placeholder)
    /// title is advertised.
    async fn on_session_updated(&mut self, info: &Value) -> Result<(), Gone> {
        if info["id"].as_str() != Some(self.session_id.as_str()) {
            return Ok(());
        }
        let Some(title) = real_title(&info["title"]) else {
            return Ok(());
        };
        if self.info.title.as_ref() == Some(&title) {
            return Ok(());
        }
        self.info.title = Some(title);
        self.emit(DriverEvent::InfoChanged(self.info.clone())).await
    }

    /// One part snapshot: stream text and reasoning, track tools. Parts of the
    /// user message (our own prompt) are ignored. A bound child's parts ride
    /// its task tool (`parent`); its step finishes stay out — usage
    /// attribution across models is murky, so fail silent, not wrong.
    async fn on_part(&mut self, part: &Value, parent: Option<ToolId>) -> Result<(), Gone> {
        let oc_id = part["messageID"].as_str().unwrap_or_default();
        let Some(message_id) = self.messages.get(oc_id).cloned() else {
            return Ok(());
        };
        match part["type"].as_str().unwrap_or_default() {
            "text" => {
                self.stream_part(part, PartKind::Text, message_id, parent)
                    .await
            }
            "reasoning" => {
                self.stream_part(part, PartKind::Reasoning, message_id, parent)
                    .await
            }
            "tool" if part["tool"].as_str() == Some("todowrite") => {
                self.on_plan(part, parent).await
            }
            // Questions surface as requests (`question.asked`), not tools.
            "tool" if part["tool"].as_str() == Some("question") => Ok(()),
            "tool" => self.on_tool(part, parent).await,
            "step-finish" if parent.is_none() => self.on_step_finish(part).await,
            _ => Ok(()),
        }
    }

    /// A token-level delta for a known text or reasoning part.
    async fn on_delta(&mut self, props: &Value, parent: Option<ToolId>) -> Result<(), Gone> {
        if props["field"].as_str() != Some("text") {
            return Ok(());
        }
        let part_id = props["partID"].as_str().unwrap_or_default();
        // Deltas of an unregistered part are for a non-streamed message (the
        // user prompt) or raced ahead of the part; either way, skip.
        let Some(state) = self.parts.get_mut(part_id) else {
            return Ok(());
        };
        let kind = state.kind;
        let delta = props["delta"].as_str().unwrap_or_default().to_owned();
        state.emitted += delta.len();
        let oc_id = props["messageID"].as_str().unwrap_or_default();
        let message_id = self.message_id(oc_id);
        if let Some(parent) = &parent {
            self.child_messages.insert(oc_id.to_owned(), parent.clone());
        }
        self.emit_text(kind, message_id, delta, parent).await
    }

    /// Emits whatever of a part's full snapshot has not been streamed yet.
    async fn stream_part(
        &mut self,
        part: &Value,
        kind: PartKind,
        message_id: MessageId,
        parent: Option<ToolId>,
    ) -> Result<(), Gone> {
        let text = part["text"].as_str().unwrap_or_default();
        let part_id = part["id"].as_str().unwrap_or_default().to_owned();
        let state = self
            .parts
            .entry(part_id)
            .or_insert(PartState { kind, emitted: 0 });
        if text.len() <= state.emitted {
            return Ok(());
        }
        // A revised snapshot (not prefixed by the streamed deltas) can put
        // the cut inside a multi-byte character; dropping it is safe — the
        // deltas carry the live content and the next snapshot retries.
        let Some(delta) = text.get(state.emitted..).map(str::to_owned) else {
            return Ok(());
        };
        state.emitted = text.len();
        self.emit_text(kind, message_id, delta, parent).await
    }

    async fn emit_text(
        &mut self,
        kind: PartKind,
        message_id: MessageId,
        text: String,
        parent: Option<ToolId>,
    ) -> Result<(), Gone> {
        let kind = match kind {
            PartKind::Text => EventKind::TextDelta { message_id, text },
            PartKind::Reasoning => EventKind::ReasoningDelta { message_id, text },
        };
        self.emit(DriverEvent::Event {
            kind,
            parent_tool_id: parent,
            extensions: Extensions::new(),
        })
        .await
    }

    /// The tool lifecycle from a `tool` part's state. A child's tools ride
    /// its task tool; own task tools register spawn order for child binding
    /// (a grandchild binds through its parent, never through this list).
    async fn on_tool(&mut self, part: &Value, parent: Option<ToolId>) -> Result<(), Gone> {
        let call_id = part["callID"].as_str().unwrap_or_default().to_owned();
        let name = part["tool"].as_str().unwrap_or_default();
        let state = &part["state"];
        let mut tool = self
            .tools
            .remove(&call_id)
            .unwrap_or_else(|| fresh_tool(&call_id, name));
        apply_state(&mut tool, name, state);
        let done = matches!(tool.status, ToolStatus::Completed | ToolStatus::Failed);
        if parent.is_none()
            && tool.kind == ToolKind::Subagent
            && !done
            && !self.spawn_order.contains(&call_id)
        {
            self.spawn_order.push(call_id.clone());
        }
        if !done {
            self.tools.insert(call_id, tool.clone());
        }
        self.emit(DriverEvent::Event {
            kind: EventKind::ToolUpdated(tool),
            parent_tool_id: parent,
            extensions: Extensions::new(),
        })
        .await
    }

    /// The agent's task list is a plan, not a tool call: one snapshot per
    /// finished write. A child's plan rides its task tool, never the
    /// parent's plan chip.
    async fn on_plan(&mut self, part: &Value, parent: Option<ToolId>) -> Result<(), Gone> {
        if part["state"]["status"].as_str() != Some("completed") {
            return Ok(());
        }
        self.emit(DriverEvent::Event {
            kind: EventKind::PlanUpdated {
                entries: plan_entries(&part["state"]["input"]["todos"]),
            },
            parent_tool_id: parent,
            extensions: Extensions::new(),
        })
        .await
    }

    /// A step boundary carries the turn's running token and cost totals.
    async fn on_step_finish(&mut self, part: &Value) -> Result<(), Gone> {
        let tokens = &part["tokens"];
        self.cost += part["cost"].as_f64().unwrap_or_default();
        // Other step-finish shapes carry the components without a `total`.
        let sum = |v: &Value| v.as_u64().unwrap_or_default();
        let used = tokens["total"]
            .as_u64()
            .filter(|t| *t > 0)
            .unwrap_or_else(|| {
                sum(&tokens["input"])
                    + sum(&tokens["output"])
                    + sum(&tokens["reasoning"])
                    + sum(&tokens["cache"]["read"])
                    + sum(&tokens["cache"]["write"])
            });
        if used == 0 {
            return Ok(());
        }
        self.emit_kind(EventKind::ContextUsage {
            used_tokens: used,
            window_tokens: current_model(&self.info).and_then(|m| self.windows.get(&m).copied()),
            cost_usd: (self.cost > 0.0).then_some(self.cost),
        })
        .await
    }

    /// A permission prompt becomes a request the caller answers.
    async fn on_permission(&mut self, props: &Value) -> Result<(), Gone> {
        let Some(permission_id) = props["id"].as_str() else {
            return Ok(());
        };
        let id = self.request_id();
        let call_id = props["tool"]["callID"].as_str().unwrap_or_default();
        let tool = self
            .tools
            .get(call_id)
            .cloned()
            .unwrap_or_else(|| permission_tool(props));
        let request = Request::Permission(PermissionRequest {
            id: id.clone(),
            tool,
            options: vec![
                PermissionChoice::AllowOnce,
                PermissionChoice::AllowAlways,
                PermissionChoice::DenyOnce,
            ],
            detail: permission_detail(props),
        });
        let session_id = props["sessionID"]
            .as_str()
            .unwrap_or(&self.session_id)
            .to_owned();
        self.requests.insert(
            id,
            Pending {
                reply: Reply::Permission {
                    permission_id: permission_id.to_owned(),
                    session_id,
                },
                request: request.clone(),
            },
        );
        self.emit_kind(EventKind::RequestOpened(request)).await
    }

    /// The question tool becomes a question request.
    async fn on_question(&mut self, props: &Value) -> Result<(), Gone> {
        let Some(question_id) = props["id"].as_str() else {
            return Ok(());
        };
        let id = self.request_id();
        let questions = props["questions"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(i, q)| question(&id, i, q))
            .collect();
        let request = Request::Question(QuestionRequest {
            id: id.clone(),
            questions,
        });
        self.requests.insert(
            id,
            Pending {
                reply: Reply::Question {
                    question_id: question_id.to_owned(),
                },
                request: request.clone(),
            },
        );
        self.emit_kind(EventKind::RequestOpened(request)).await
    }

    /// Replies to one open request in the shape opencode expects.
    async fn answer(&mut self, request: RequestId, answer: Answer) -> Result<(), Gone> {
        let Some(pending) = self.requests.remove(&request) else {
            return Ok(());
        };
        let sent = match (&pending.reply, &answer) {
            (
                Reply::Permission {
                    permission_id,
                    session_id,
                },
                Answer::Permission(choice),
            ) => {
                let response = match choice {
                    PermissionChoice::AllowOnce => "once",
                    PermissionChoice::AllowAlways => "always",
                    _ => "reject",
                };
                self.http
                    .post(
                        &format!("/session/{session_id}/permissions/{permission_id}"),
                        json!({ "response": response }),
                    )
                    .await
            }
            (Reply::Question { question_id }, Answer::Question(answers)) => {
                self.http
                    .post(
                        &format!("/question/{question_id}/reply"),
                        json!({ "answers": question_answers(answers) }),
                    )
                    .await
            }
            // Unreachable past engine shape validation; reopen rather than
            // leave the agent parked on a silently dropped request.
            _ => {
                let reopened = pending.request.clone();
                self.requests.insert(request, pending);
                return self.emit_kind(EventKind::RequestOpened(reopened)).await;
            }
        };
        match sent {
            Ok(_) => Ok(()),
            // The agent is still parked on it: report the refusal and reopen
            // the request under the same id so the caller can answer again.
            Err(e) => {
                self.diagnostic(DiagnosticLevel::Warning, format!("answer not taken: {e}"))
                    .await?;
                let reopened = pending.request.clone();
                self.requests.insert(request, pending);
                self.emit_kind(EventKind::RequestOpened(reopened)).await
            }
        }
    }

    /// `session.idle`: the running turn is really over.
    async fn on_idle(&mut self) -> Result<(), Gone> {
        self.turn = Turn::Idle;
        let stop = if std::mem::take(&mut self.aborting) {
            StopReason::Cancelled
        } else {
            self.turn_error.take().unwrap_or(StopReason::Completed {
                source: CompletionSource::Protocol,
            })
        };
        // An aborted message never completes; close whatever is still open,
        // in time order — a straggler from a bound child closes under its
        // task tool.
        let mut open: Vec<_> = self
            .messages
            .iter()
            .filter(|(oc_id, _)| !self.ended.contains(*oc_id))
            .map(|(o, m)| (o.clone(), m.clone()))
            .collect();
        open.sort();
        for (oc_id, message_id) in open {
            let parent = self.child_messages.get(&oc_id).cloned();
            self.end_message(oc_id, message_id, parent).await?;
        }
        // A settled turn leaves nothing more for its parts, tools, children,
        // child bindings, or an unanswered dialog (the engine already closed
        // the request).
        self.parts.clear();
        self.tools.clear();
        self.messages.clear();
        self.ended.clear();
        self.children.clear();
        self.child_of.clear();
        self.spawn_order.clear();
        self.child_messages.clear();
        self.requests.clear();
        self.emit(DriverEvent::TurnEnded(stop)).await
    }

    /// A session error: a dead credential closes the session; the rest is a
    /// diagnostic.
    async fn on_error(&mut self, props: &Value) -> Result<(), Gone> {
        let error = &props["error"];
        match error["name"].as_str() {
            Some("ProviderAuthError") => {
                self.emit(DriverEvent::AuthLost {
                    login: self.login.clone(),
                })
                .await
            }
            // Our own cancel; the aborted message already ends the turn.
            Some("MessageAbortedError") => Ok(()),
            _ if self.turn == Turn::Idle => Ok(()),
            _ => {
                // The turn is failing; idle normally follows and reports it.
                // A turn that died outright is already idle server-side and
                // sends nothing more, so settle now.
                let message = error["data"]["message"]
                    .as_str()
                    .unwrap_or("the agent reported an error");
                self.turn_error = Some(StopReason::Failed {
                    message: message.to_owned(),
                });
                if self.turn == Turn::Busy && !self.server_busy().await {
                    return self.on_idle().await;
                }
                Ok(())
            }
        }
    }

    /// Summarizes the session in place. The server streams the summary and
    /// ends with `session.compacted`, so the turn state follows a prompt's.
    async fn compact(&mut self) -> Result<(), Gone> {
        let body = self.model_body().unwrap_or_else(|| json!({}));
        self.turn = Turn::Sent;
        self.aborting = false;
        self.turn_error = None;
        let path = format!("/session/{}/summarize", self.session_id);
        match self.http.post(&path, body).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.turn = Turn::Idle;
                self.diagnostic(DiagnosticLevel::Warning, format!("compaction refused: {e}"))
                    .await?;
                self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                    message: e.to_string(),
                }))
                .await
            }
        }
    }

    /// Drops the rolled-back turns; opencode restores conversation, not files.
    /// Note `GET /message` still lists reverted messages afterward: the revert
    /// trims the model's context, not the listing.
    async fn rollback(&mut self, turns: u32) -> Result<(), Gone> {
        let messages = self
            .http
            .get(&format!("/session/{}/message", self.session_id))
            .await
            .ok();
        let Some(anchor) = messages.and_then(|m| user_anchor(&m, turns)) else {
            return self
                .diagnostic(DiagnosticLevel::Warning, "nothing to roll back")
                .await;
        };
        let reverted = self
            .http
            .post(
                &format!("/session/{}/revert", self.session_id),
                json!({ "messageID": anchor }),
            )
            .await;
        match reverted {
            // Nothing advertised changes (the session rewinds in place), but
            // the resulting `SessionUpdated` is the documented confirmation.
            Ok(_) => self.emit(DriverEvent::InfoChanged(self.info.clone())).await,
            Err(e) => {
                self.diagnostic(DiagnosticLevel::Warning, format!("rollback rejected: {e}"))
                    .await
            }
        }
    }

    /// Applies a live model change; it rides the next prompt.
    /// Model and effort are per-prompt values: switching is a matter of
    /// remembering the selection. Effort's choices follow the model.
    async fn configure(&mut self, id: ConfigId, value: ConfigValue) -> Result<(), Gone> {
        let ConfigValue::Text(text) = &value else {
            return Ok(());
        };
        match id.as_str() {
            "model" if text.split_once('/').is_none() => {
                return self
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        format!("`{text}` is not a `provider/model` value"),
                    )
                    .await;
            }
            "model" | "effort" => {}
            _ => return Ok(()),
        }
        if apply_selection(&mut self.info, &id, &value) {
            if id.as_str() == "model" {
                sync_effort(&mut self.info, &self.variants);
            }
            self.emit(DriverEvent::InfoChanged(self.info.clone()))
                .await?;
        }
        Ok(())
    }

    /// The `{providerID, modelID}` body for the advertised model.
    fn model_body(&self) -> Option<Value> {
        let model = current_model(&self.info)?;
        let (provider, model) = model.split_once('/')?;
        Some(json!({ "providerID": provider, "modelID": model }))
    }

    /// Whether the server still reports this session busy. An unreachable
    /// server counts as idle: it is dying, and the bus closing follows. The
    /// short timeout keeps a wedged handler from stalling the drive loop.
    async fn server_busy(&self) -> bool {
        match self.http.get_quick("/session/status").await {
            Ok(status) => busy_status(&status[&self.session_id]),
            Err(_) => false,
        }
    }

    /// The admission deadline passed without the server going busy: adopt a
    /// busy whose frame was missed, or end the turn rather than hang. A
    /// cancel taken while the prompt sat unadmitted ends it as cancelled.
    async fn check_admission(&mut self) -> Result<(), Gone> {
        if self.server_busy().await {
            self.turn = Turn::Busy;
            return Ok(());
        }
        self.turn = Turn::Idle;
        let stop = if std::mem::take(&mut self.aborting) {
            StopReason::Cancelled
        } else {
            self.turn_error.take().unwrap_or(StopReason::Failed {
                message: "opencode took the prompt but never started on it".into(),
            })
        };
        self.emit(DriverEvent::TurnEnded(stop)).await
    }

    /// Mints the next request id.
    fn request_id(&mut self) -> RequestId {
        self.next_request += 1;
        RequestId::new(format!("r{}", self.next_request))
    }

    /// The streaming id for an opencode message, minting one on first sight.
    fn message_id(&mut self, oc_id: &str) -> MessageId {
        if let Some(id) = self.messages.get(oc_id) {
            return id.clone();
        }
        if oc_id > self.tide.as_str() {
            self.tide = oc_id.to_owned();
        }
        self.next_message += 1;
        let id = MessageId::new(format!("m{}", self.next_message));
        self.messages.insert(oc_id.to_owned(), id.clone());
        id
    }

    async fn report_exit(&mut self) {
        let status = self.server.exit_status(CLOSE_GRACE).await;
        let stderr = self.server.stderr_tail();
        self.emit(DriverEvent::Exited { status, stderr }).await.ok();
    }

    async fn diagnostic(
        &mut self,
        level: DiagnosticLevel,
        message: impl Into<String>,
    ) -> Result<(), Gone> {
        self.emit_kind(EventKind::Diagnostic(Diagnostic {
            level,
            message: message.into(),
        }))
        .await
    }

    async fn emit_kind(&mut self, kind: EventKind) -> Result<(), Gone> {
        self.emit(DriverEvent::event(kind)).await
    }

    async fn emit(&mut self, event: DriverEvent) -> Result<(), Gone> {
        self.events.send(event).await.map_err(|_| Gone)
    }
}

/// The engine or the server is gone; the drive task unwinds.
struct Gone;

/// Sleeps until the admission deadline; pends forever without one.
async fn admission(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Busy or retrying (a provider hiccup being retried) both mean running;
/// opencode's own client reconciles the same way.
fn busy_status(status: &Value) -> bool {
    matches!(status["type"].as_str(), Some("busy") | Some("retry"))
}

/// The spawn a newborn direct child binds to: oldest still-running spawn
/// without a child of its own. `None` leaves the child unbound (silent).
fn select_spawn(
    spawn_order: &[String],
    tools: &HashMap<String, ToolUpdate>,
    child_of: &HashMap<String, ToolId>,
) -> Option<ToolId> {
    let taken: HashSet<&str> = child_of.values().map(ToolId::as_str).collect();
    spawn_order
        .iter()
        .filter(|call| !taken.contains(call.as_str()))
        .filter_map(|call| tools.get(call.as_str()))
        .filter(|tool| {
            tool.kind == ToolKind::Subagent
                && matches!(tool.status, ToolStatus::Pending | ToolStatus::Running)
        })
        .map(|tool| tool.id.clone())
        .next()
}

/// A session title that is not the server's dated placeholder.
fn real_title(title: &Value) -> Option<String> {
    let title = title.as_str().filter(|t| !t.is_empty())?;
    let placeholder = title.starts_with("New session - ") || title.starts_with("Child session - ");
    (!placeholder).then(|| title.to_owned())
}

// ---------------------------------------------------------------------------
// Frame decoding
// ---------------------------------------------------------------------------

/// A `/command` prompt split into name and arguments, or `None` for plain text.
fn slash_command(text: &str) -> Option<(String, String)> {
    let rest = text.strip_prefix('/')?;
    let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let name = &rest[..name_end];
    if name.is_empty() {
        return None;
    }
    Some((name.to_owned(), rest[name_end..].trim_start().to_owned()))
}

/// A tool call opencode has only named so far.
fn fresh_tool(call_id: &str, name: &str) -> ToolUpdate {
    ToolUpdate {
        id: ToolId::new(call_id),
        kind: tool_kind(name),
        title: name.to_owned(),
        status: ToolStatus::Pending,
        input: ToolInput::None,
        output: None,
        diffs: Vec::new(),
        locations: Vec::new(),
        raw: None,
    }
}

/// opencode's built-in tool names to the portable kind.
fn tool_kind(name: &str) -> ToolKind {
    match name {
        "bash" => ToolKind::Execute,
        "read" | "list" => ToolKind::Read,
        "edit" | "write" | "patch" => ToolKind::Edit,
        "grep" | "glob" => ToolKind::Search,
        "webfetch" => ToolKind::Fetch,
        "task" => ToolKind::Subagent,
        _ => ToolKind::Other,
    }
}

/// Applies a tool state snapshot: status, decoded input, and output.
fn apply_state(tool: &mut ToolUpdate, name: &str, state: &Value) {
    let input = &state["input"];
    apply_input(tool, name, input);
    match state["status"].as_str().unwrap_or_default() {
        "pending" => tool.status = ToolStatus::Pending,
        "running" => tool.status = ToolStatus::Running,
        "completed" => {
            tool.status = ToolStatus::Completed;
            if let Some(output) = state["output"].as_str() {
                tool.output = Some(cap(output.to_owned(), OUTPUT_CAP));
            }
        }
        "error" => {
            tool.status = ToolStatus::Failed;
            if let Some(error) = state["error"].as_str() {
                tool.output = Some(cap(error.to_owned(), OUTPUT_CAP));
            }
        }
        _ => {}
    }
}

/// Fills in the decoded arguments and a human title from a tool's input.
fn apply_input(tool: &mut ToolUpdate, name: &str, input: &Value) {
    if input.as_object().is_none_or(|o| o.is_empty()) {
        return;
    }
    let field = |key: &str| input[key].as_str().filter(|v| !v.is_empty());
    let path = |key: &str| field(key).map_or(ToolInput::None, |p| ToolInput::Path(p.into()));
    let decoded = match name {
        "bash" => ToolInput::Command {
            command: field("command").unwrap_or_default().to_owned(),
            cwd: None,
        },
        "read" | "write" | "edit" | "patch" => path("filePath"),
        "list" => path("path"),
        "grep" | "glob" => ToolInput::Pattern(field("pattern").unwrap_or_default().to_owned()),
        "webfetch" => ToolInput::Url(field("url").unwrap_or_default().to_owned()),
        _ => {
            tool.raw = Some(RawTool {
                name: name.to_owned(),
                input: input.clone(),
            });
            ToolInput::None
        }
    };
    set_input(tool, name, decoded);
}

/// Sets a tool's input plus the location and title that follow from it.
fn set_input(tool: &mut ToolUpdate, name: &str, input: ToolInput) {
    if let ToolInput::Path(path) = &input {
        tool.locations = vec![path.clone()];
    }
    tool.title = tool_title(name, &input);
    tool.input = input;
}

/// The tool name plus its most telling argument.
fn tool_title(name: &str, input: &ToolInput) -> String {
    match input {
        ToolInput::Command { command, .. } => format!("{name} {command}"),
        ToolInput::Path(path) => format!("{name} {}", path.display()),
        ToolInput::Pattern(p) | ToolInput::Url(p) => format!("{name} {p}"),
        _ => name.to_owned(),
    }
}

/// A stub tool for a permission that names no tracked call, so the approval
/// card still shows what it approves.
fn permission_tool(props: &Value) -> ToolUpdate {
    let name = props["permission"].as_str().unwrap_or("tool");
    let mut tool = fresh_tool(props["tool"]["callID"].as_str().unwrap_or_default(), name);
    if let Some(command) = props["metadata"]["command"].as_str() {
        set_input(
            &mut tool,
            name,
            ToolInput::Command {
                command: command.to_owned(),
                cwd: None,
            },
        );
    } else if let Some(path) = props["metadata"]["filepath"].as_str() {
        set_input(&mut tool, name, ToolInput::Path(path.into()));
    }
    tool
}

/// The most telling detail of a permission: the command or file it covers,
/// else its first pattern.
fn permission_detail(props: &Value) -> Option<String> {
    props["metadata"]["command"]
        .as_str()
        .or_else(|| props["metadata"]["filepath"].as_str())
        .or_else(|| props["patterns"][0].as_str())
        .map(str::to_owned)
}

/// One assistant-message error as a stop reason.
fn message_error(error: &Value) -> StopReason {
    match error["name"].as_str() {
        Some("MessageAbortedError") => StopReason::Cancelled,
        _ => StopReason::Failed {
            message: error["data"]["message"]
                .as_str()
                .unwrap_or("the agent failed")
                .to_owned(),
        },
    }
}

/// One opencode question as a portable question.
fn question(request: &RequestId, index: usize, q: &Value) -> Question {
    let choices: Vec<Choice> = q["options"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|option| {
            let label = option["label"].as_str()?;
            Some(Choice {
                id: ChoiceId::new(label),
                label: label.to_owned(),
                description: option["description"].as_str().map(str::to_owned),
            })
        })
        .collect();
    Question {
        id: QuestionId::new(format!("{}#{index}", request.as_str())),
        text: q["question"].as_str().unwrap_or_default().to_owned(),
        header: q["header"].as_str().map(str::to_owned),
        allows_free_text: q["custom"].as_bool().unwrap_or(false) || choices.is_empty(),
        multi_select: q["multiple"].as_bool().unwrap_or(false),
        choices,
    }
}

/// Each answer as opencode's array-of-selected-labels shape.
fn question_answers(answers: &[QuestionAnswer]) -> Vec<Vec<String>> {
    answers
        .iter()
        .map(|answer| match answer {
            QuestionAnswer::Choices(choices) => {
                choices.iter().map(|c| c.as_str().to_owned()).collect()
            }
            QuestionAnswer::Text(text) => vec![text.clone()],
        })
        .collect()
}

/// The fork body for a `fork_point` anchor. opencode copies the messages
/// *before* `messageID`, so the cut is the anchor's successor; an anchor at
/// the tip is a plain tip fork.
fn fork_body(messages: &Value, anchor: &str) -> Result<Value, AgentError> {
    let ids: Vec<&str> = messages
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["info"]["id"].as_str())
        .collect();
    let index = ids.iter().position(|id| *id == anchor).ok_or_else(|| {
        AgentError::InvalidConfiguration(format!(
            "fork point {anchor} is not a message of that session"
        ))
    })?;
    Ok(ids
        .get(index + 1)
        .map_or(json!({}), |next| json!({ "messageID": next })))
}

/// The `msg_…` id of the user message `turns` back from the tip, the anchor a
/// revert cuts from.
fn user_anchor(messages: &Value, turns: u32) -> Option<String> {
    let users: Vec<&str> = messages
        .as_array()?
        .iter()
        .filter(|m| m["info"]["role"].as_str() == Some("user"))
        .filter_map(|m| m["info"]["id"].as_str())
        .collect();
    let index = users.len().checked_sub(turns as usize)?;
    users.get(index).map(|id| (*id).to_owned())
}

// ---------------------------------------------------------------------------
// HTTP + SSE over localhost
// ---------------------------------------------------------------------------

/// A minimal HTTP/1.1 client for the local opencode server. Every call is one
/// connection; the session dir rides as a query param and the session secret
/// as basic auth.
#[derive(Clone)]
struct Http {
    port: u16,
    directory: String,
    /// The `Authorization` header value.
    auth: String,
    /// Tees outgoing requests when wire recording is on.
    recorder: Option<WireRecorder>,
}

/// How long a request/response call may take; a wedged server must not
/// block the drive loop (and with it cancel and close) forever.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
/// Status reads reconcile turn ends inline in the drive loop, so they get
/// a much shorter leash.
const STATUS_TIMEOUT: Duration = Duration::from_secs(3);

impl Http {
    async fn get(&self, path: &str) -> Result<Value, AgentError> {
        self.request("GET", path, None, Some(HTTP_TIMEOUT)).await
    }

    /// A GET that must answer fast or not at all (status reconciles).
    async fn get_quick(&self, path: &str) -> Result<Value, AgentError> {
        self.request("GET", path, None, Some(STATUS_TIMEOUT)).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, AgentError> {
        self.request("POST", path, Some(body), Some(HTTP_TIMEOUT))
            .await
    }

    /// A POST that legitimately runs as long as a turn (`/command` answers
    /// with the finished message); callers run it off the drive loop.
    async fn post_unbounded(&self, path: &str, body: Value) -> Result<Value, AgentError> {
        self.request("POST", path, Some(body), None).await
    }

    /// Sends one request and returns the decoded JSON body (or `Null`).
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        limit: Option<Duration>,
    ) -> Result<Value, AgentError> {
        if let Some(recorder) = &self.recorder {
            recorder.record(
                "out",
                &json!({ "method": method, "path": path, "body": body }),
            );
        }
        let io = self.exchange(method, path, body);
        match limit {
            Some(limit) => tokio::time::timeout(limit, io)
                .await
                .map_err(|_| AgentError::ProtocolFailed(format!("{method} {path} timed out")))?,
            None => io.await,
        }
    }

    /// One connection, one request, one length-framed response.
    async fn exchange(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, AgentError> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port))
            .await
            .map_err(|e| closed(&e.to_string()))?;
        stream
            .write_all(&self.frame(method, path, body))
            .await
            .map_err(|e| closed(&e.to_string()))?;
        // Bun keeps some sockets open despite `Connection: close`, so the
        // body is read by its declared length, not until the server hangs up.
        let mut reader = BufReader::new(stream);
        let head = read_head(&mut reader)
            .await
            .map_err(|e| closed(&e.to_string()))?;
        let status = head.status;
        let mut payload = Vec::new();
        let read = match head.length {
            Some(n) => {
                payload.resize(n, 0);
                reader.read_exact(&mut payload).await.map(|_| ())
            }
            None => reader.read_to_end(&mut payload).await.map(|_| ()),
        };
        read.map_err(|e| closed(&e.to_string()))?;
        if !(200..300).contains(&status) {
            let detail = String::from_utf8_lossy(&payload);
            return Err(AgentError::ProtocolFailed(format!(
                "{method} {path} -> {status}: {}",
                detail.trim()
            )));
        }
        Ok(serde_json::from_slice(&payload).unwrap_or(Value::Null))
    }

    /// A JSON request the server answers and closes.
    fn frame(&self, method: &str, path: &str, body: Option<Value>) -> Vec<u8> {
        let body = body.map(|b| b.to_string()).unwrap_or_default();
        let headers = format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close",
            body.len()
        );
        self.raw(method, path, &headers, &body)
    }

    /// A raw HTTP/1.1 request with the directory query param.
    fn raw(&self, method: &str, path: &str, headers: &str, body: &str) -> Vec<u8> {
        let target = format!("{path}?directory={}", encode(&self.directory));
        format!(
            "{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: {}\r\n\
             {headers}\r\n\r\n{body}",
            self.auth
        )
        .into_bytes()
    }
}

/// Opens the SSE `/event` bus: one long-lived request whose decoded frames
/// feed the returned channel. Returns once the server has accepted the
/// subscription, so a prompt sent right after cannot lose its frames. The
/// stream closing (server death) closes the channel.
async fn open_bus(
    http: &Http,
    recorder: Option<WireRecorder>,
) -> Result<mpsc::Receiver<Value>, AgentError> {
    let mut stream = TcpStream::connect(("127.0.0.1", http.port))
        .await
        .map_err(|e| closed(&e.to_string()))?;
    stream
        .write_all(&http.raw("GET", "/event", "Accept: text/event-stream", ""))
        .await
        .map_err(|e| closed(&e.to_string()))?;
    let mut reader = BufReader::new(stream);
    let head = read_head(&mut reader)
        .await
        .map_err(|e| closed(&e.to_string()))?;
    if head.status != 200 {
        return Err(AgentError::ProtocolFailed(format!(
            "GET /event -> {}",
            head.status
        )));
    }
    let (tx, frames) = mpsc::channel(FRAME_BUFFER);
    tokio::spawn(read_bus(reader, head.chunked, tx, recorder));
    Ok(frames)
}

/// Forwards each decoded SSE frame until the stream or the channel ends.
async fn read_bus(
    mut reader: BufReader<TcpStream>,
    chunked: bool,
    tx: mpsc::Sender<Value>,
    recorder: Option<WireRecorder>,
) {
    let mut sse = SseDecoder::new(chunked);
    let mut chunk = [0u8; 4096];
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        for frame in sse.feed(&chunk[..n]) {
            let Ok(value) = serde_json::from_str::<Value>(&frame) else {
                continue;
            };
            if let Some(recorder) = &recorder {
                recorder.record("in", &value);
            }
            if tx.send(value).await.is_err() {
                return;
            }
        }
    }
}

/// The status line and headers of a response.
struct Head {
    status: u16,
    length: Option<usize>,
    chunked: bool,
}

/// Reads the status line and headers up to the blank line.
async fn read_head<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<Head> {
    let mut line = String::new();
    let mut head = Head {
        status: 0,
        length: None,
        chunked: false,
    };
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        let line = line.trim_end();
        if head.status == 0 {
            let status = line.split_whitespace().nth(1).and_then(|c| c.parse().ok());
            head.status = status.ok_or_else(|| std::io::Error::other("missing HTTP status"))?;
        } else if line.is_empty() {
            return Ok(head);
        } else if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                head.length = value.parse().ok();
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                head.chunked = value.contains("chunked");
            }
        }
    }
}

/// Reassembles SSE `data:` payloads from the `/event` body, fed in arbitrary
/// byte slices. HTTP chunk framing is stripped first when the response is
/// chunked, and a frame is decoded only once whole, so a multi-byte character
/// split across reads stays intact. `\r` is dropped so a frame always ends
/// at `\n\n` (JSON never carries a raw `\r`).
struct SseDecoder {
    chunked: bool,
    /// Undecoded chunked bytes.
    raw: Vec<u8>,
    /// Payload bytes still owed by the current chunk.
    remaining: usize,
    /// The SSE body so far.
    body: Vec<u8>,
}

impl SseDecoder {
    fn new(chunked: bool) -> Self {
        Self {
            chunked,
            raw: Vec::new(),
            remaining: 0,
            body: Vec::new(),
        }
    }

    /// Feeds raw bytes, returning any complete SSE data payloads.
    fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        if self.chunked {
            self.raw.extend_from_slice(bytes);
            self.dechunk();
        } else {
            self.body.extend(bytes.iter().filter(|b| **b != b'\r'));
        }
        let mut frames = Vec::new();
        while let Some(end) = self.body.windows(2).position(|w| w == b"\n\n") {
            let frame: Vec<u8> = self.body.drain(..end + 2).collect();
            // Multiple `data:` lines are one payload joined by `\n` (SSE spec).
            let data = String::from_utf8_lossy(&frame)
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|line| line.strip_prefix(' ').unwrap_or(line))
                .collect::<Vec<_>>()
                .join("\n");
            if !data.is_empty() {
                frames.push(data);
            }
        }
        frames
    }

    /// Moves the payload of every complete-enough chunk from `raw` to
    /// `body`, dropping the size lines and the CRLF after each payload.
    fn dechunk(&mut self) {
        loop {
            if self.remaining == 0 {
                let Some(end) = self.raw.windows(2).position(|w| w == b"\r\n") else {
                    return;
                };
                let line = String::from_utf8_lossy(&self.raw[..end]).into_owned();
                self.raw.drain(..end + 2);
                // An empty line is the CRLF that closed the previous payload.
                if line.is_empty() {
                    continue;
                }
                let size = line.split(';').next().unwrap_or_default().trim();
                self.remaining = usize::from_str_radix(size, 16).unwrap_or(0);
                if self.remaining == 0 {
                    return; // the last chunk
                }
            }
            let take = self.remaining.min(self.raw.len());
            self.body
                .extend(self.raw.drain(..take).filter(|b| *b != b'\r'));
            self.remaining -= take;
            if self.remaining > 0 {
                return;
            }
        }
    }
}

/// Percent-encodes a query value; the directory can hold spaces and such.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn closed(detail: &str) -> AgentError {
    AgentError::ProtocolFailed(format!("opencode server connection failed: {detail}"))
}

/// Drains a child's stdout so a full pipe never blocks the server.
fn drain(stdout: Option<tokio::process::ChildStdout>) {
    if let Some(mut stdout) = stdout {
        tokio::spawn(async move {
            let mut sink = [0u8; 4096];
            while stdout.read(&mut sink).await.is_ok_and(|n| n > 0) {}
        });
    }
}

/// A free localhost TCP port, handed to the server. Another binder can win
/// the pick-then-bind race; opencode then exits and `launch` reports it.
fn free_port() -> Result<u16, AgentError> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .and_then(|l| l.local_addr())
        .map(|addr| addr.port())
        .map_err(|e| AgentError::SpawnFailed(format!("could not pick a port: {e}")))
}

/// A per-session secret for the server's basic-auth gate: two hashes under
/// the process's OS-seeded random keys, 128 bits in hex.
fn secret() -> String {
    use std::hash::{BuildHasher, Hasher};
    let keys = std::collections::hash_map::RandomState::new();
    (0..2u64)
        .map(|i| {
            let mut hasher = keys.build_hasher();
            hasher.write_u64(i);
            format!("{:016x}", hasher.finish())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn sse_decoder_reassembles_frames_split_across_reads() {
        let mut sse = SseDecoder::new(false);
        assert!(sse.feed(b"data: {\"a\":").is_empty());
        let frames = sse.feed(b"1}\n\ndata: {\"b\":2}\n\n");
        assert_eq!(frames, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
    }

    #[test]
    fn sse_decoder_strips_chunk_framing_and_keeps_split_characters() {
        // One event over two HTTP chunks; the second chunk arrives in two
        // reads that split the em dash (three bytes) in the middle.
        let event = "data: {\"t\":\"a \u{2014} b\"}\n\n".as_bytes();
        let (first, second) = event.split_at(12);
        let mut wire = format!("{:x}\r\n", first.len()).into_bytes();
        wire.extend(first);
        wire.extend(format!("\r\n{:x}\r\n", second.len()).bytes());
        let cut = wire.len() + 3; // inside the em dash of the second chunk
        wire.extend(second);
        wire.extend(b"\r\n0\r\n\r\n");
        let mut sse = SseDecoder::new(true);
        assert!(sse.feed(&wire[..cut]).is_empty());
        let frames = sse.feed(&wire[cut..]);
        assert_eq!(frames, vec!["{\"t\":\"a \u{2014} b\"}"]);
    }

    #[test]
    fn permission_rules_layer_onto_the_host_config() {
        let host = json!({ "sandbox": true, "permission": { "bash": "deny" } });
        let merged = with_permission(host, permission_config(PermissionMode::Ask));
        assert_eq!(
            merged,
            json!({ "sandbox": true, "permission": { "bash": "deny", "*": "ask", "question": "allow" } })
        );
        let bare = with_permission(json!({}), permission_config(PermissionMode::AutoApprove));
        assert_eq!(bare, json!({ "permission": { "*": "allow" } }));
    }

    #[test]
    fn slash_command_splits_name_and_arguments() {
        assert_eq!(
            slash_command("/review the diff"),
            Some(("review".into(), "the diff".into()))
        );
        assert_eq!(slash_command("/init"), Some(("init".into(), String::new())));
        assert_eq!(slash_command("hello"), None);
        assert_eq!(slash_command("/"), None);
    }

    #[tokio::test]
    async fn read_head_parses_status_length_and_chunking() {
        let mut raw: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nVary: Origin\r\n\r\n{}";
        let head = read_head(&mut raw).await.unwrap();
        assert_eq!(
            (head.status, head.length, head.chunked),
            (200, Some(2), false)
        );
        assert_eq!(raw, b"{}");
        let mut sse: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        let head = read_head(&mut sse).await.unwrap();
        assert_eq!((head.status, head.length, head.chunked), (200, None, true));
    }

    #[test]
    fn fork_body_cuts_after_the_anchor() {
        let messages = json!([
            { "info": { "id": "msg_1" } }, { "info": { "id": "msg_2" } }, { "info": { "id": "msg_3" } },
        ]);
        assert_eq!(
            fork_body(&messages, "msg_2").unwrap(),
            json!({ "messageID": "msg_3" })
        );
        assert_eq!(fork_body(&messages, "msg_3").unwrap(), json!({}));
        assert!(fork_body(&messages, "msg_9").is_err());
    }

    #[test]
    fn effort_follows_the_selected_model_variants() {
        let providers = json!({ "providers": [{ "id": "p", "models": {
            "a": { "variants": { "max": {}, "high": {}, "low": {}, "custom": {} } },
            "b": { "variants": { "high": {} } },
            "c": { "variants": {} },
        } }] });
        let variants = model_variants(&providers);
        assert_eq!(variants["p/a"], ["low", "high", "max", "custom"]);
        assert!(!variants.contains_key("p/c"));

        let choices = model_choices(&providers, &json!(["p"]));
        let mut info = driver_info(choices, &json!([]), &json!({}), AuthStatus::Unknown, None);
        let select = |info: &mut DriverInfo, id: &str, value: &str| {
            apply_selection(info, &ConfigId::new(id), &ConfigValue::Text(value.into()));
        };
        let effort = |info: &DriverInfo| {
            info.details
                .config_options
                .iter()
                .find(|o| o.id.as_str() == "effort")
                .map(|o| o.current.clone())
        };
        select(&mut info, "model", "p/a");
        sync_effort(&mut info, &variants);
        assert_eq!(effort(&info), Some(None));
        select(&mut info, "effort", "high");
        // A model that still offers `high` keeps it; one that doesn't drops it.
        select(&mut info, "model", "p/b");
        sync_effort(&mut info, &variants);
        assert_eq!(effort(&info), Some(Some(ConfigValue::Text("high".into()))));
        select(&mut info, "model", "p/c");
        sync_effort(&mut info, &variants);
        assert_eq!(effort(&info), None);
        assert!(
            !info
                .configuration
                .options
                .contains_key(&ConfigId::new("effort"))
        );
    }

    #[test]
    fn model_choices_keep_connected_providers_only() {
        let providers = json!({ "providers": [
            { "id": "opencode", "models": { "big-pickle": { "name": "Big Pickle" } } },
            { "id": "offline", "models": { "x": { "name": "X" } } },
        ] });
        let choices = model_choices(&providers, &json!(["opencode"]));
        assert_eq!(choices.len(), 1);
        assert_eq!(choices[0].value, "opencode/big-pickle");
        assert_eq!(choices[0].label, "Big Pickle");
    }

    #[test]
    fn sse_decoder_joins_multi_data_lines_with_newline() {
        let mut sse = SseDecoder::new(false);
        let frames = sse.feed(b"data: {\"a\":\ndata: 1}\n\n");
        assert_eq!(frames, vec!["{\"a\":\n1}"]);
    }

    #[test]
    fn real_title_drops_the_dated_placeholders() {
        assert_eq!(
            real_title(&json!("New session - 2026-09-04T00:56:28.393Z")),
            None
        );
        assert_eq!(real_title(&json!("Child session - 2026-09-04")), None);
        assert_eq!(real_title(&json!("")), None);
        assert_eq!(real_title(&json!(null)), None);
        assert_eq!(
            real_title(&json!("Fix the adapter")),
            Some("Fix the adapter".into())
        );
    }

    #[test]
    fn busy_status_counts_retry_as_busy() {
        assert!(busy_status(&json!({ "type": "busy" })));
        assert!(busy_status(&json!({ "type": "retry" })));
        assert!(!busy_status(&json!({ "type": "idle" })));
        assert!(!busy_status(&json!(null)));
    }

    /// A task tool plus the selection inputs around it.
    fn spawn(call: &str, status: ToolStatus) -> (String, ToolUpdate) {
        let mut tool = fresh_tool(call, "task");
        tool.status = status;
        (call.to_owned(), tool)
    }

    #[test]
    fn select_spawn_prefers_the_oldest_running_unbound_spawn() {
        let mut tools = HashMap::new();
        let (a, tool_a) = spawn("call_a", ToolStatus::Running);
        let (b, tool_b) = spawn("call_b", ToolStatus::Running);
        tools.insert(a.clone(), tool_a);
        tools.insert(b.clone(), tool_b);
        let order = vec![a.clone(), b.clone()];
        // Nothing bound: oldest wins.
        assert_eq!(
            select_spawn(&order, &tools, &HashMap::new()).as_ref(),
            Some(&ToolId::new(a.clone()))
        );
        // First bound elsewhere: second wins.
        let bound = HashMap::from([("ses_child".to_owned(), ToolId::new(a.clone()))]);
        assert_eq!(
            select_spawn(&order, &tools, &bound).as_ref(),
            Some(&ToolId::new(b.clone()))
        );
    }

    #[test]
    fn select_spawn_skips_settled_and_non_task_tools() {
        let mut tools = HashMap::new();
        let (done, tool_done) = spawn("call_done", ToolStatus::Completed);
        tools.insert(done, tool_done);
        let mut read = fresh_tool("call_read", "read");
        read.status = ToolStatus::Running;
        tools.insert("call_read".into(), read);
        let order = vec!["call_done".to_owned(), "call_read".to_owned()];
        assert_eq!(select_spawn(&order, &tools, &HashMap::new()), None);
    }

    #[test]
    fn permission_carries_the_command_it_covers() {
        // The live `permission.asked` shape for bash (captured 2026-09-03).
        let props = json!({
            "id": "per_1", "permission": "bash",
            "patterns": ["echo hi"], "metadata": { "command": "echo hi" },
            "tool": { "messageID": "msg_1", "callID": "call_1" },
        });
        assert_eq!(permission_detail(&props).as_deref(), Some("echo hi"));
        let tool = permission_tool(&props);
        assert_eq!(tool.title, "bash echo hi");
        assert!(
            matches!(tool.input, ToolInput::Command { ref command, .. } if command == "echo hi")
        );
    }

    #[test]
    fn user_anchor_counts_user_turns_from_the_tip() {
        let messages = json!([
            { "info": { "id": "msg_1", "role": "user" } },
            { "info": { "id": "msg_2", "role": "assistant" } },
            { "info": { "id": "msg_3", "role": "user" } },
            { "info": { "id": "msg_4", "role": "assistant" } },
        ]);
        assert_eq!(user_anchor(&messages, 1), Some("msg_3".into()));
        assert_eq!(user_anchor(&messages, 2), Some("msg_1".into()));
        assert_eq!(user_anchor(&messages, 3), None);
    }

    #[test]
    fn question_answers_flatten_choices_and_text() {
        let answers = vec![
            QuestionAnswer::Choices(vec![ChoiceId::new("Red"), ChoiceId::new("Blue")]),
            QuestionAnswer::Text("free form".into()),
        ];
        assert_eq!(
            question_answers(&answers),
            vec![
                vec!["Red".to_owned(), "Blue".to_owned()],
                vec!["free form".to_owned()]
            ]
        );
    }

    #[test]
    fn tool_state_decodes_write_and_error() {
        let mut tool = fresh_tool("call1", "write");
        apply_state(
            &mut tool,
            "write",
            &json!({ "status": "error", "input": { "filePath": "/tmp/a.txt" },
                     "error": "denied" }),
        );
        assert_eq!(tool.status, ToolStatus::Failed);
        assert_eq!(tool.locations, vec![PathBuf::from("/tmp/a.txt")]);
        assert_eq!(tool.output.as_deref(), Some("denied"));
    }
}
