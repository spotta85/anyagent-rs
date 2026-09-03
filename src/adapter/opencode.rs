//! Native opencode adapter: drives `opencode serve` over its HTTP + SSE wire
//! (validated live 2026-09-03 against opencode 1.18.24). One server process
//! per session, bound to a free localhost port with the session dir as cwd.
//! Commands are HTTP POSTs; content arrives on the `/event` SSE bus, filtered
//! to this session. Turn end is deterministic: the `session.idle` that
//! follows the server going busy for our prompt.
//!
//! We drive the legacy ("v1") engine on purpose. It emits permission,
//! question, and idle events the "v2" engine makes a client poll for, and its
//! fork works. The one thing v1 lacks is steering, which opencode has over ACP
//! too; the engine queues prompts instead. All routes and event names live in
//! this file so a future v2 swap is contained.

use std::collections::HashMap;
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
        let (server, http, info, session_id, window) = launch(&request, recorder.clone()).await?;
        let frames = open_bus(http.clone(), recorder);
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        tokio::spawn(
            Drive {
                http,
                server,
                frames,
                events: ev_tx,
                info: info.clone(),
                session_id,
                window,
                login: login_methods(&request.installation),
                messages: HashMap::new(),
                parts: HashMap::new(),
                tools: HashMap::new(),
                requests: HashMap::new(),
                cost: 0.0,
                turn: Turn::Idle,
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

/// Spawns the server, waits for health, binds the session, and reads the
/// catalogs behind the advertised options. Returns the current model's
/// context window alongside.
async fn launch(
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
) -> Result<(process::Child, Http, DriverInfo, String, Option<u64>), AgentError> {
    if !request.options.mcp_servers.is_empty() {
        // Client MCP servers would need a `POST /mcp` per server after open;
        // deferred until a consumer needs it.
        return Err(AgentError::UnsupportedFeature(
            "client-declared MCP servers on opencode".into(),
        ));
    }
    let port = free_port()?;
    let mut server = spawn_server(request, port).await?;
    let http = Http {
        port,
        directory: request.options.cwd().to_string_lossy().into_owned(),
    };
    // The piped stdout must be drained or the server blocks on a full pipe.
    drain(server.stdout.take());
    let outcome =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake(&http, request, recorder)).await;
    match outcome {
        Ok(Ok((info, session_id, window))) => Ok((server, http, info, session_id, window)),
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
/// the basic-auth gate stripped (localhost), and the permission mode as
/// config.
async fn spawn_server(request: &ConnectRequest, port: u16) -> Result<process::Child, AgentError> {
    let mut env = crate::adapter::config_home_env(&request.installation, &request.options)?;
    // The desktop app exports these to secure its own server; empty values
    // mean "unsecured" and let the adapter talk to localhost without a header.
    env.push(("OPENCODE_SERVER_PASSWORD".into(), String::new()));
    env.push(("OPENCODE_SERVER_USERNAME".into(), String::new()));
    env.push((
        "OPENCODE_CONFIG_CONTENT".into(),
        permission_config(request.options.permission_mode).to_string(),
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
) -> Result<(DriverInfo, String, Option<u64>), AgentError> {
    let version = await_health(http).await?;
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
    let mut info = driver_info(
        &providers,
        &connected,
        &commands,
        &session,
        auth_status(request, &connected),
        version,
    );
    // A creation-time `configure("model", …)` overrides the session default
    // (v1 has no model endpoint, so it rides every prompt) — apply it before
    // reading the window so the window matches the model actually used.
    if let Some(model) = configured_model(&request.options) {
        apply_selection(
            &mut info,
            &ConfigId::new("model"),
            &ConfigValue::Text(model),
        );
    }
    let window = context_window(&providers, current_model(&info).as_deref());
    // The `ses_…` id is the durable handle: resuming re-adopts it.
    info.resume_token = Some(ResumeToken::new(&session_id));
    Ok((info, session_id, window))
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
        SessionStart::Resume(token) => http.get(&format!("/session/{}", token.as_str())).await,
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
/// list is the one honest "logged out" (probed 2026-09-03).
fn auth_status(request: &ConnectRequest, connected: &Value) -> AuthStatus {
    let any = connected.as_array().is_some_and(|c| !c.is_empty());
    if any {
        AuthStatus::Authenticated {
            kind: AuthKind::Subscription,
            account: None,
        }
    } else {
        AuthStatus::Unauthenticated {
            login: login_methods(&request.installation),
        }
    }
}

/// The permission config merged into the server via `OPENCODE_CONFIG_CONTENT`.
/// `Ask` forces every tool to prompt but lets the question tool run so it
/// surfaces as a question, not a permission.
fn permission_config(mode: PermissionMode) -> Value {
    match mode {
        PermissionMode::Ask => json!({ "permission": { "*": "ask", "question": "allow" } }),
        PermissionMode::AutoApprove => json!({ "permission": { "*": "allow" } }),
    }
}

/// Folds the handshake responses into the advertised state.
fn driver_info(
    providers: &Value,
    connected: &Value,
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
        kind: ConfigKind::Select {
            choices: model_choices(providers, connected),
        },
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
                Capability::SlashCommands,
                Capability::Plan,
                Capability::ContextUsage,
            ]),
            config_options,
            commands: slash_commands(commands),
        },
        configuration,
        resume_token: None,
        title: session["title"].as_str().map(str::to_owned),
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

/// The current model's context window, from the provider catalog.
fn context_window(providers: &Value, model: Option<&str>) -> Option<u64> {
    let (pid, mid) = model?.split_once('/')?;
    providers["providers"]
        .as_array()?
        .iter()
        .find(|p| p["id"].as_str() == Some(pid))?["models"][mid]["limit"]["context"]
        .as_u64()
        .filter(|w| *w > 0)
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

/// The creation-time `configure("model", …)` value, if any.
fn configured_model(options: &SessionOptions) -> Option<String> {
    options
        .configure
        .iter()
        .find_map(|(id, value)| match value {
            ConfigValue::Text(model) if id.as_str() == "model" => Some(model.clone()),
            _ => None,
        })
}

// ---------------------------------------------------------------------------
// Drive task: engine commands out (HTTP), SSE frames in
// ---------------------------------------------------------------------------

/// One open request awaiting the caller's answer.
enum PendingRequest {
    Permission { permission_id: String },
    Question { question_id: String },
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
    /// Current model's context window, for `ContextUsage`.
    window: Option<u64>,
    /// Assistant `msg_…` id → our streaming message id. Only assistant
    /// messages are minted, so membership gates what streams (the user
    /// message carries a replay of our own prompt).
    messages: HashMap<String, MessageId>,
    /// `prt_…` id → (kind, bytes already emitted), for delta/snapshot dedup.
    parts: HashMap<String, PartState>,
    /// Tool snapshots by `callID`, for the permission that references one.
    tools: HashMap<String, ToolUpdate>,
    requests: HashMap<RequestId, PendingRequest>,
    /// Session cost so far, summed over step-finishes.
    cost: f64,
    turn: Turn,
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
            // Never reached: Steer is unadvertised, so the engine queues
            // mid-turn prompts and re-sends them as `StartTurn`.
            DriverCommand::Steer { .. } => self.emit(DriverEvent::Steered(false)).await?,
            DriverCommand::Answer { request, answer } => self.answer(request, answer).await?,
            DriverCommand::Cancel => {
                self.aborting = true;
                self.post(&format!("/session/{}/abort", self.session_id), json!({}))
                    .await;
            }
            DriverCommand::Configure(id, value) => self.configure(id, value).await?,
            DriverCommand::Rollback(turns, _) => self.rollback(turns.get()).await?,
            DriverCommand::Close => unreachable!("handled in run"),
        }
        Ok(())
    }

    /// Sends the prompt, or routes a `/command` to its own endpoint. A wire
    /// rejection fails the turn the engine already started.
    async fn start_turn(&mut self, input: &Input) -> Result<(), Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.clone()) {
            self.diagnostic(DiagnosticLevel::Warning, problem).await?;
        }
        let text = attach::with_refs(input.as_text(), &loaded);
        let (path, body) = match slash_command(&text) {
            Some((command, arguments)) => (
                format!("/session/{}/command", self.session_id),
                json!({ "command": command, "arguments": arguments }),
            ),
            None => {
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
                (format!("/session/{}/prompt_async", self.session_id), body)
            }
        };
        match self.http.post(&path, body).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                    message: e.to_string(),
                }))
                .await
            }
        }
    }

    /// Routes one SSE frame by type, ignoring frames for other sessions.
    async fn handle_frame(&mut self, frame: &Value) -> Result<(), Gone> {
        let props = &frame["properties"];
        let kind = frame["type"].as_str().unwrap_or_default();
        // Global frames (`server.connected`, installation notices) have no
        // session; session frames not ours are other sessions on the bus.
        if let Some(session) = props["sessionID"].as_str()
            && session != self.session_id
        {
            return Ok(());
        }
        // Bookkeeping trails every idle (title, diff, re-published messages)
        // and an abort emits a second idle, so only a prompted turn's frames
        // are decoded, and only a busy turn's idle ends it.
        if self.turn == Turn::Idle && kind != "session.error" {
            return Ok(());
        }
        match kind {
            "session.status" if props["status"]["type"].as_str() == Some("busy") => {
                if self.turn == Turn::Sent {
                    self.turn = Turn::Busy;
                }
                Ok(())
            }
            "session.idle" if self.turn == Turn::Busy => self.on_idle().await,
            "message.updated" => self.on_message(props).await,
            "message.part.updated" => self.on_part(&props["part"]).await,
            "message.part.delta" => self.on_delta(props).await,
            "permission.asked" => self.on_permission(props).await,
            "question.asked" => self.on_question(props).await,
            "session.error" => self.on_error(props).await,
            // The rest is bookkeeping and reply echoes the engine already owns.
            _ => Ok(()),
        }
    }

    /// An assistant message: mint its streaming id and record any error. The
    /// user-role echo of our own prompt is dropped.
    async fn on_message(&mut self, props: &Value) -> Result<(), Gone> {
        let info = &props["info"];
        if info["role"].as_str() != Some("assistant") {
            return Ok(());
        }
        let Some(oc_id) = info["id"].as_str() else {
            return Ok(());
        };
        self.message_id(oc_id);
        if let Some(error) = info.get("error").filter(|e| !e.is_null()) {
            self.turn_error = Some(message_error(error));
        }
        Ok(())
    }

    /// One part snapshot: stream text and reasoning, track tools. Parts of the
    /// user message (our own prompt) are ignored.
    async fn on_part(&mut self, part: &Value) -> Result<(), Gone> {
        let oc_id = part["messageID"].as_str().unwrap_or_default();
        let Some(message_id) = self.messages.get(oc_id).cloned() else {
            return Ok(());
        };
        match part["type"].as_str().unwrap_or_default() {
            "text" => self.stream_part(part, PartKind::Text, message_id).await,
            "reasoning" => {
                self.stream_part(part, PartKind::Reasoning, message_id)
                    .await
            }
            "tool" if part["tool"].as_str() == Some("todowrite") => self.on_plan(part).await,
            "tool" => self.on_tool(part).await,
            "step-finish" => self.on_step_finish(part).await,
            _ => Ok(()),
        }
    }

    /// A token-level delta for a known text or reasoning part.
    async fn on_delta(&mut self, props: &Value) -> Result<(), Gone> {
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
        let message_id = self.message_id(props["messageID"].as_str().unwrap_or_default());
        self.emit_text(kind, message_id, delta).await
    }

    /// Emits whatever of a part's full snapshot has not been streamed yet.
    async fn stream_part(
        &mut self,
        part: &Value,
        kind: PartKind,
        message_id: MessageId,
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
        let delta = text[state.emitted..].to_owned();
        state.emitted = text.len();
        self.emit_text(kind, message_id, delta).await
    }

    async fn emit_text(
        &mut self,
        kind: PartKind,
        message_id: MessageId,
        text: String,
    ) -> Result<(), Gone> {
        let kind = match kind {
            PartKind::Text => EventKind::TextDelta { message_id, text },
            PartKind::Reasoning => EventKind::ReasoningDelta { message_id, text },
        };
        self.emit_kind(kind).await
    }

    /// The tool lifecycle from a `tool` part's state.
    async fn on_tool(&mut self, part: &Value) -> Result<(), Gone> {
        let call_id = part["callID"].as_str().unwrap_or_default().to_owned();
        let name = part["tool"].as_str().unwrap_or_default();
        let state = &part["state"];
        let mut tool = self
            .tools
            .remove(&call_id)
            .unwrap_or_else(|| fresh_tool(&call_id, name));
        apply_state(&mut tool, name, state);
        let done = matches!(tool.status, ToolStatus::Completed | ToolStatus::Failed);
        if !done {
            self.tools.insert(call_id, tool.clone());
        }
        self.emit_kind(EventKind::ToolUpdated(tool)).await
    }

    /// The agent's task list is a plan, not a tool call: one snapshot per
    /// finished write.
    async fn on_plan(&mut self, part: &Value) -> Result<(), Gone> {
        if part["state"]["status"].as_str() != Some("completed") {
            return Ok(());
        }
        self.emit_kind(EventKind::PlanUpdated {
            entries: plan_entries(&part["state"]["input"]["todos"]),
        })
        .await
    }

    /// A step boundary carries the turn's running token and cost totals.
    async fn on_step_finish(&mut self, part: &Value) -> Result<(), Gone> {
        let tokens = &part["tokens"];
        self.cost += part["cost"].as_f64().unwrap_or_default();
        let Some(used) = tokens["total"].as_u64().filter(|t| *t > 0) else {
            return Ok(());
        };
        self.emit_kind(EventKind::ContextUsage {
            used_tokens: used,
            window_tokens: self.window,
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
        self.requests.insert(
            id.clone(),
            PendingRequest::Permission {
                permission_id: permission_id.to_owned(),
            },
        );
        self.emit_kind(EventKind::RequestOpened(Request::Permission(
            PermissionRequest {
                id,
                tool,
                options: vec![
                    PermissionChoice::AllowOnce,
                    PermissionChoice::AllowAlways,
                    PermissionChoice::DenyOnce,
                ],
                detail: props["metadata"]["filepath"].as_str().map(str::to_owned),
            },
        )))
        .await
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
        self.requests.insert(
            id.clone(),
            PendingRequest::Question {
                question_id: question_id.to_owned(),
            },
        );
        self.emit_kind(EventKind::RequestOpened(Request::Question(
            QuestionRequest { id, questions },
        )))
        .await
    }

    /// Replies to one open request in the shape opencode expects.
    async fn answer(&mut self, request: RequestId, answer: Answer) -> Result<(), Gone> {
        let Some(pending) = self.requests.remove(&request) else {
            return Ok(());
        };
        match (pending, answer) {
            (PendingRequest::Permission { permission_id }, Answer::Permission(choice)) => {
                let response = match choice {
                    PermissionChoice::AllowOnce => "once",
                    PermissionChoice::AllowAlways => "always",
                    _ => "reject",
                };
                self.post(
                    &format!("/session/{}/permissions/{permission_id}", self.session_id),
                    json!({ "response": response }),
                )
                .await;
            }
            (PendingRequest::Question { question_id }, Answer::Question(answers)) => {
                self.post(
                    &format!("/question/{question_id}/reply"),
                    json!({ "answers": question_answers(&answers) }),
                )
                .await;
            }
            _ => {}
        }
        Ok(())
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
        // The turn's last assistant message anchors `fork_from(_, at)`;
        // opencode ids sort by time. Its `MessageEnded` carries the anchor.
        let last = self.messages.iter().max_by(|a, b| a.0.cmp(b.0));
        if let Some((oc_id, message_id)) = last.map(|(o, m)| (o.clone(), m.clone())) {
            let mut extensions = Extensions::new();
            extensions.insert("opencode/fork_point".into(), Value::from(oc_id));
            self.emit(DriverEvent::Event {
                kind: EventKind::MessageEnded { message_id },
                parent_tool_id: None,
                extensions,
            })
            .await?;
        }
        // A settled turn leaves nothing more for its parts or tools.
        self.parts.clear();
        self.tools.clear();
        self.messages.clear();
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
                let message = error["data"]["message"]
                    .as_str()
                    .unwrap_or("the agent reported an error");
                self.diagnostic(DiagnosticLevel::Error, message).await
            }
        }
    }

    /// Drops the rolled-back turns; opencode restores conversation, not files.
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
        self.post(
            &format!("/session/{}/revert", self.session_id),
            json!({ "messageID": anchor }),
        )
        .await;
        Ok(())
    }

    /// Applies a live model change; it rides the next prompt.
    async fn configure(&mut self, id: ConfigId, value: ConfigValue) -> Result<(), Gone> {
        if id.as_str() != "model" {
            return Ok(());
        }
        let ConfigValue::Text(model) = &value else {
            return Ok(());
        };
        if model.split_once('/').is_none() {
            return self
                .diagnostic(
                    DiagnosticLevel::Warning,
                    format!("`{model}` is not a `provider/model` value"),
                )
                .await;
        }
        // The window belongs to the old model; the next connect re-reads it.
        self.window = None;
        if apply_selection(&mut self.info, &id, &value) {
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
        self.next_message += 1;
        let id = MessageId::new(format!("m{}", self.next_message));
        self.messages.insert(oc_id.to_owned(), id.clone());
        id
    }

    /// Fire-and-forget POST: a command whose body we do not need.
    async fn post(&self, path: &str, body: Value) {
        let _ = self.http.post(path, body).await;
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

/// A stub tool for a permission that names no tracked call.
fn permission_tool(props: &Value) -> ToolUpdate {
    let name = props["permission"].as_str().unwrap_or("tool");
    let mut tool = fresh_tool(props["tool"]["callID"].as_str().unwrap_or_default(), name);
    if let Some(path) = props["metadata"]["filepath"].as_str() {
        set_input(&mut tool, name, ToolInput::Path(path.into()));
    }
    tool
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
/// connection closed by the server; the session dir rides as a query param.
#[derive(Clone)]
struct Http {
    port: u16,
    directory: String,
}

impl Http {
    async fn get(&self, path: &str) -> Result<Value, AgentError> {
        self.request("GET", path, None).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, AgentError> {
        self.request("POST", path, Some(body)).await
    }

    /// Sends one request and returns the decoded JSON body (or `Null`).
    async fn request(
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
        let (status, length) = read_head(&mut reader)
            .await
            .map_err(|e| closed(&e.to_string()))?;
        let mut payload = Vec::new();
        let read = match length {
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
        format!("{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n{headers}\r\n\r\n{body}")
            .into_bytes()
    }
}

/// Opens the SSE `/event` bus: one long-lived request whose decoded frames
/// feed the returned channel. The stream closing (server death) closes it.
fn open_bus(http: Http, recorder: Option<WireRecorder>) -> mpsc::Receiver<Value> {
    let (tx, frames) = mpsc::channel(FRAME_BUFFER);
    tokio::spawn(read_bus(http, tx, recorder));
    frames
}

/// Connects to `/event` and forwards each decoded SSE frame until the stream
/// or the channel ends.
async fn read_bus(http: Http, tx: mpsc::Sender<Value>, recorder: Option<WireRecorder>) {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", http.port)).await else {
        return;
    };
    let request = http.raw("GET", "/event", "Accept: text/event-stream", "");
    if stream.write_all(&request).await.is_err() {
        return;
    }
    let mut reader = BufReader::new(stream);
    if read_head(&mut reader).await.is_err() {
        return;
    }
    let mut sse = SseDecoder::new();
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

/// Reads the status line and headers up to the blank line, returning the
/// status code and the `Content-Length` if one was sent.
async fn read_head<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<(u16, Option<usize>)> {
    let mut line = String::new();
    let mut status = None;
    let mut length = None;
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        let line = line.trim_end();
        if line.is_empty() {
            return status
                .map(|s| (s, length))
                .ok_or_else(|| std::io::Error::other("missing HTTP status"));
        }
        if status.is_none() {
            status = Some(
                line.split_whitespace()
                    .nth(1)
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0),
            );
        } else if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().ok();
        }
    }
}

/// Reassembles SSE frames from a chunked, arbitrarily-split byte stream. It
/// tolerates the HTTP chunk-size lines interleaved with the body by keeping
/// only `data:` lines, which the size lines never are. `\r` is dropped up
/// front so a frame always ends at `\n\n` (JSON payloads never carry a raw
/// `\r`).
struct SseDecoder {
    buffer: String,
}

impl SseDecoder {
    fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    /// Feeds raw bytes, returning any complete SSE data payloads.
    fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        self.buffer
            .push_str(&String::from_utf8_lossy(bytes).replace('\r', ""));
        let mut frames = Vec::new();
        while let Some(end) = self.buffer.find("\n\n") {
            let frame: String = self.buffer.drain(..end + 2).collect();
            let data: String = frame
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|line| line.strip_prefix(' ').unwrap_or(line))
                .collect();
            if !data.is_empty() {
                frames.push(data);
            }
        }
        frames
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

/// A free localhost TCP port, handed to the server. A tiny race with another
/// binder is possible; the server would then fail health and time out.
fn free_port() -> Result<u16, AgentError> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .and_then(|l| l.local_addr())
        .map(|addr| addr.port())
        .map_err(|e| AgentError::SpawnFailed(format!("could not pick a port: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn sse_decoder_reassembles_split_and_chunked_frames() {
        let mut sse = SseDecoder::new();
        // A chunk-size line, then a frame split across two feeds.
        assert!(sse.feed(b"2a\r\ndata: {\"a\":").is_empty());
        let frames = sse.feed(b"1}\n\n1c\r\ndata: {\"b\":2}\n\n");
        assert_eq!(frames, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
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
    async fn read_head_parses_status_and_length() {
        let mut raw: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nVary: Origin\r\n\r\n{}";
        let (status, length) = read_head(&mut raw).await.unwrap();
        assert_eq!((status, length), (200, Some(2)));
        assert_eq!(raw, b"{}");
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
