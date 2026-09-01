//! Native pi adapter: drives `pi` over its RPC wire
//! (`--mode rpc`, validated live 2026-08-30 against pi 0.84.4). One JSON
//! object per line each way — commands in with a correlating `id`, responses
//! and events out. Turn end is deterministic: `agent_settled` is the only
//! frame that means no retry, compaction, or queued continuation is still
//! coming (`agent_end` fires per low-level run). The engine owns turn rules.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
    WireRecorder, attach, cap, login_methods, with_stderr,
};
use crate::agent::{
    AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigChoice, ConfigId,
    ConfigKind, ConfigOption, ConfigValue, Input, ResumeToken, SessionConfiguration,
    SessionOptions, SessionStart, SlashCommand,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Choice, ChoiceId, CompletionSource, Diagnostic, DiagnosticLevel, EventKind, Extensions,
    MessageId, Question, QuestionAnswer, QuestionId, QuestionRequest, RawTool, Request, RequestId,
    StopReason, ToolId, ToolInput, ToolKind, ToolStatus, ToolUpdate,
};
use crate::process::{self, Spawn};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_GRACE: Duration = Duration::from_secs(2);
/// `--version` and `auth check` are sub-second; this only bounds a hang.
const SIDE_PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_BUFFER: usize = 64;
const OUTPUT_CAP: usize = 16 * 1024;

/// Launches a pi-dialect CLI in RPC mode. The catalog entry supplies the
/// executable; a fork that shares the wire (omp) would be one more profile.
pub(crate) struct PiAdapter;

impl PiAdapter {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Adapter for PiAdapter {
    /// Spawns the CLI, reads its state and catalogs, and hands the live wire
    /// to the drive task.
    async fn connect(&self, request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let (ev_tx, ev_rx) = mpsc::channel(FRAME_BUFFER);
        let recorder = WireRecorder::for_session(&request.options, &ev_tx).await;
        let (child, wire, info, window) = launch(&request, recorder).await?;
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        tokio::spawn(
            Drive {
                wire,
                child,
                events: ev_tx,
                info: info.clone(),
                window,
                message: None,
                tools: HashMap::new(),
                streamed: HashMap::new(),
                requests: HashMap::new(),
                pending: HashMap::new(),
                stop: None,
                aborting: false,
                cost: 0.0,
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

/// Spawns the CLI and handshakes within the timeout. Also returns the current
/// model's context window, which only the handshake sees.
async fn launch(
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
) -> Result<(process::Child, Wire, DriverInfo, Option<u64>), AgentError> {
    let mut args = vec!["--mode".to_owned(), "rpc".to_owned()];
    args.extend(launch_args(&request.options)?);
    let env = crate::adapter::config_home_env(&request.installation, &request.options)?;
    let mut child = process::spawn(Spawn {
        exec_path: request.installation.executable_path.clone(),
        args,
        cwd: request.options.cwd().clone(),
        env: env.clone(),
    })
    .await?;
    let mut wire = Wire::over(&mut child, recorder);
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake(&mut wire, request, &env)).await {
        Ok(Ok(info)) => Ok((child, wire, info.0, info.1)),
        Ok(Err(e)) => {
            let e = with_stderr(e, &child);
            child.shutdown(CLOSE_GRACE).await;
            Err(e)
        }
        Err(_) => {
            child.shutdown(CLOSE_GRACE).await;
            Err(AgentError::HandshakeTimeout)
        }
    }
}

/// Session start and creation-time config as launch flags. Anything the CLI
/// cannot take is refused here rather than silently dropped.
fn launch_args(options: &SessionOptions) -> Result<Vec<String>, AgentError> {
    if !options.mcp_servers.is_empty() {
        return Err(AgentError::UnsupportedFeature(
            "client-declared MCP servers (pi configures MCP in its own settings)".into(),
        ));
    }
    let mut args = Vec::new();
    match &options.start {
        SessionStart::New => {}
        SessionStart::Resume(token) => {
            args.push("--session".to_owned());
            args.push(token.as_str().to_owned());
        }
        // `--fork` exists but only ever forks at the tip: pi cuts history by
        // entry id, and entry ids never appear on the streaming wire, so
        // `fork_from(token, at)` cannot be honoured.
        SessionStart::Fork { .. } => {
            return Err(AgentError::UnsupportedFeature(
                "fork (pi cuts history by entry id, which its event stream never names)".into(),
            ));
        }
    }
    for (id, value) in &options.configure {
        match (id.as_str(), value) {
            ("model", ConfigValue::Text(model)) => {
                let (provider, model_id) = split_model(model)?;
                args.push("--provider".to_owned());
                args.push(provider.to_owned());
                args.push("--model".to_owned());
                args.push(model_id.to_owned());
            }
            ("thinking", ConfigValue::Text(level)) => {
                args.push("--thinking".to_owned());
                args.push(level.clone());
            }
            _ => {
                return Err(AgentError::InvalidConfiguration(format!(
                    "`{id}` is not a creation-time option of this agent"
                )));
            }
        }
    }
    Ok(args)
}

/// Reads the live session state and the catalogs behind the advertised
/// options. Auth and version come from two short side processes, run
/// together: the RPC wire reports neither.
async fn handshake(
    wire: &mut Wire,
    request: &ConnectRequest,
    env: &[(String, String)],
) -> Result<(DriverInfo, Option<u64>), AgentError> {
    let state = wire.roundtrip("get_state").await?;
    let models = wire.roundtrip("get_available_models").await?;
    let levels = wire.roundtrip("get_available_thinking_levels").await?;
    let commands = wire.roundtrip("get_commands").await?;
    let exe = &request.installation.executable_path;
    let provider = state["model"]["provider"].as_str().unwrap_or_default();
    let (auth, version) = tokio::join!(auth_status(request, provider, env), version(exe, env));
    let window = state["model"]["contextWindow"].as_u64().filter(|w| *w > 0);
    Ok((
        driver_info(&state, &models, &levels, &commands, auth, version),
        window,
    ))
}

/// pi's own readiness check for the session's provider. This is the only
/// honest answer: a session opens fine with no credentials at all, and
/// `auth.json` exists in both states (both probed 2026-08-30).
async fn auth_status(
    request: &ConnectRequest,
    provider: &str,
    env: &[(String, String)],
) -> AuthStatus {
    let unauthenticated = || AuthStatus::Unauthenticated {
        login: login_methods(&request.installation),
    };
    // No credentials anywhere means no model resolves, and pi says so.
    if provider.is_empty() || provider == "unknown" {
        return unauthenticated();
    }
    let exe = &request.installation.executable_path;
    let args = ["auth", "check", "--provider", provider, "--json"];
    let Some(report) = output(exe, &args, env).await else {
        return AuthStatus::Unknown;
    };
    let Ok(report) = serde_json::from_str::<Value>(&report) else {
        return AuthStatus::Unknown;
    };
    if report["status"].as_str() != Some("ready") {
        return unauthenticated();
    }
    AuthStatus::Authenticated {
        kind: match report["authType"].as_str() {
            Some("oauth") => AuthKind::Subscription,
            _ => AuthKind::ApiKey,
        },
        account: None,
    }
}

/// `<bin> --version`; the RPC wire never reports it.
async fn version(exe: &Path, env: &[(String, String)]) -> Option<String> {
    let version = output(exe, &["--version"], env).await?;
    let version = version.trim().to_owned();
    (!version.is_empty()).then_some(version)
}

/// Captured stdout of a short side process; `None` if it fails or hangs.
async fn output(exe: &Path, args: &[&str], env: &[(String, String)]) -> Option<String> {
    let mut command = tokio::process::Command::new(exe);
    command
        .args(args)
        .envs(env.iter().cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let out = tokio::time::timeout(SIDE_PROCESS_TIMEOUT, command.output())
        .await
        .ok()?
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// What the handshake responses tell us, folded into the engine vocabulary.
fn driver_info(
    state: &Value,
    models: &Value,
    levels: &Value,
    commands: &Value,
    auth: AuthStatus,
    version: Option<String>,
) -> DriverInfo {
    let model = model_value(&state["model"]);
    let thinking = state["thinkingLevel"].as_str().map(str::to_owned);
    let mut configuration = SessionConfiguration::default();
    let mut config_options = vec![ConfigOption {
        id: ConfigId::new("model"),
        name: "Model".into(),
        category: Some("model".into()),
        kind: ConfigKind::Select {
            choices: model_choices(&models["models"]),
        },
        current: model.clone().map(ConfigValue::Text),
        live: true,
    }];
    if let Some(option) = thinking_option(&levels["levels"], thinking.clone()) {
        config_options.push(option);
    }
    for (id, value) in [("model", model), ("thinking", thinking)] {
        if let Some(value) = value {
            configuration
                .options
                .insert(ConfigId::new(id), ConfigValue::Text(value));
        }
    }
    DriverInfo {
        details: AgentDetails {
            version,
            auth,
            // No `Permissions` (pi has no permission protocol), `Rollback`,
            // `Fork`, `PlanUsage`, or `Subagents`: none of them are on the
            // wire. `Questions` is the extension-UI dialog protocol.
            capabilities: Capabilities::new([
                Capability::Steer,
                Capability::Images,
                Capability::Resume,
                Capability::SlashCommands,
                Capability::ContextUsage,
                Capability::Questions,
            ]),
            config_options,
            commands: slash_commands(&commands["commands"]),
        },
        configuration,
        // Session files are the durable handle, and one already exists.
        resume_token: state["sessionFile"]
            .as_str()
            .map(ResumeToken::new)
            .filter(|t| !t.as_str().is_empty()),
        title: state["sessionName"].as_str().map(str::to_owned),
        // Every prompt settles with exactly one `agent_settled`.
        deterministic_turn_end: true,
        deterministic_agent_turn_end: true,
    }
}

/// The model catalog as config choices, valued `provider/modelId` because
/// `set_model` needs both halves.
fn model_choices(models: &Value) -> Vec<ConfigChoice> {
    models
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let value = model_value(model)?;
            Some(ConfigChoice {
                label: model["name"].as_str().unwrap_or(&value).to_owned(),
                value,
                description: None,
            })
        })
        .collect()
}

/// `provider/modelId`, or `None` for pi's "no model resolved" sentinel.
fn model_value(model: &Value) -> Option<String> {
    let provider = model["provider"].as_str()?;
    let id = model["id"].as_str()?;
    (provider != "unknown" && !provider.is_empty() && !id.is_empty())
        .then(|| format!("{provider}/{id}"))
}

/// Splits a `provider/modelId` choice back into the two fields the wire and
/// the launch flags want. Model ids contain slashes, so only the first cuts.
fn split_model(value: &str) -> Result<(&str, &str), AgentError> {
    match value.split_once('/') {
        Some((provider, model)) if !provider.is_empty() && !model.is_empty() => {
            Ok((provider, model))
        }
        _ => Err(AgentError::InvalidConfiguration(format!(
            "`{value}` is not a `provider/model` value"
        ))),
    }
}

/// The thinking levels of the *current* model; a model without reasoning
/// support reports `off` alone, which is not worth advertising.
fn thinking_option(levels: &Value, current: Option<String>) -> Option<ConfigOption> {
    let choices: Vec<ConfigChoice> = levels
        .as_array()?
        .iter()
        .filter_map(|level| level.as_str())
        .map(|level| ConfigChoice {
            value: level.to_owned(),
            label: level.to_owned(),
            description: None,
        })
        .collect();
    // A level the new model does not offer is not the current one.
    let current = current.filter(|level| choices.iter().any(|c| &c.value == level));
    (choices.len() > 1).then(|| ConfigOption {
        id: ConfigId::new("thinking"),
        name: "Thinking level".into(),
        category: Some("thought_level".into()),
        kind: ConfigKind::Select { choices },
        current: current.map(ConfigValue::Text),
        live: true,
    })
}

/// Extension commands, prompt templates, and skills, all invoked with `/`.
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

// ---------------------------------------------------------------------------
// Drive task: engine commands out, wire frames in
// ---------------------------------------------------------------------------

/// A command whose response still matters.
enum Pending {
    /// A rejected prompt has to fail the turn the engine already started.
    Prompt,
    Steer,
    Configure(ConfigId, ConfigValue),
    /// Thinking levels belong to the model, so a model change re-reads them.
    Thinking,
}

struct Drive {
    wire: Wire,
    child: process::Child,
    events: mpsc::Sender<DriverEvent>,
    /// Current advertised state; mutated and re-sent as `InfoChanged`.
    info: DriverInfo,
    /// Context window of the current model, for `ContextUsage`.
    window: Option<u64>,
    /// Id of the assistant message being streamed.
    message: Option<MessageId>,
    /// Tool snapshots by `toolCallId`, dropped when the call ends.
    tools: HashMap<String, ToolUpdate>,
    /// Bytes of each running tool's output already sent: `partialResult`
    /// accumulates, so only its new tail is a delta.
    streamed: HashMap<String, usize>,
    /// Open extension dialogs: our request id to the wire id and method.
    requests: HashMap<RequestId, (String, String)>,
    pending: HashMap<String, Pending>,
    /// The running turn's stop, from the last assistant message.
    stop: Option<StopReason>,
    /// A `Cancel` is in flight, so the next settle is a cancellation.
    aborting: bool,
    /// Session cost so far, summed over assistant messages.
    cost: f64,
    next_message: u64,
    next_request: u64,
}

impl Drive {
    /// Main loop until the engine or the agent goes away.
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
                frame = self.wire.frames.recv() => match frame {
                    Some(frame) => {
                        if self.handle_frame(frame).await.is_err() {
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
        self.child.shutdown(CLOSE_GRACE).await;
    }

    async fn handle_command(&mut self, cmd: DriverCommand) -> Result<(), Gone> {
        match cmd {
            // Never `follow_up`: the engine owns the prompt queue, so a
            // mid-turn prompt is queued by it and arrives as its own turn.
            DriverCommand::StartTurn { input } => {
                self.emit(DriverEvent::TurnAck).await?;
                // A cancel that raced the previous turn's natural end must
                // not bleed into this one.
                self.aborting = false;
                let id = self.send_input("prompt", &input).await?;
                self.pending.insert(id, Pending::Prompt);
            }
            DriverCommand::Steer { input } => {
                let id = self.send_input("steer", &input).await?;
                self.pending.insert(id, Pending::Steer);
            }
            DriverCommand::Answer { request, answer } => self.answer(request, answer).await?,
            DriverCommand::Cancel => {
                self.aborting = true;
                // `abort` alone resumes whatever is still in pi's own
                // steering queue as a fresh run (probed 2026-08-30), so the
                // queue is cleared first — pi's own interrupt recipe.
                self.wire.send("clear_queue", json!({})).await?;
                self.wire.send("abort", json!({})).await?;
                // Unblock every extension dialog still waiting on a reply.
                for (wire_id, _) in std::mem::take(&mut self.requests).into_values() {
                    self.wire.cancel_dialog(&wire_id).await?;
                }
            }
            DriverCommand::Configure(id, value) => self.configure(id, value).await?,
            DriverCommand::Rollback(..) => {
                // Not advertised: pi forks a new session instead of rewinding.
                self.diagnostic(DiagnosticLevel::Warning, "rollback is not supported on pi")
                    .await?;
            }
            DriverCommand::Close => unreachable!("handled in run"),
        }
        Ok(())
    }

    /// Routes one wire frame by its `type`.
    async fn handle_frame(&mut self, frame: Value) -> Result<(), Gone> {
        match frame["type"].as_str().unwrap_or_default() {
            "response" => self.on_response(&frame).await,
            "message_start" => self.on_message_start(&frame),
            "message_update" => self.on_update(&frame).await,
            "message_end" => self.on_message_end(&frame).await,
            "tool_execution_start" | "tool_execution_update" | "tool_execution_end" => {
                self.on_tool(&frame).await
            }
            "extension_ui_request" => self.on_ui_request(&frame).await,
            "agent_settled" => self.on_settled().await,
            "compaction_end" => self.emit_kind(EventKind::ContextCompacted).await,
            "extension_error" => {
                let error = frame["error"].as_str().unwrap_or("extension failed");
                self.diagnostic(DiagnosticLevel::Error, error).await
            }
            "auto_retry_start" | "summarization_retry_scheduled" => {
                let error = frame["errorMessage"].as_str().unwrap_or("transient error");
                self.diagnostic(DiagnosticLevel::Warning, format!("retrying after {error}"))
                    .await
            }
            // Narration the engine already owns or does not need:
            // `agent_end` is per low-level run, `turn_*` are per LLM call,
            // and `queue_update` describes pi's own queue, which we never use.
            "agent_start"
            | "agent_end"
            | "turn_start"
            | "turn_end"
            | "queue_update"
            | "compaction_start"
            | "auto_retry_end"
            | "summarization_retry_attempt_start"
            | "summarization_retry_finished"
            | "bash_execution_update" => Ok(()),
            other => {
                let mut extensions = Extensions::new();
                extensions.insert("pi/raw_frame".into(), frame.clone());
                self.emit(DriverEvent::Event {
                    kind: EventKind::Diagnostic(Diagnostic {
                        level: DiagnosticLevel::Info,
                        message: format!("unrecognized pi frame `{other}`"),
                    }),
                    parent_tool_id: None,
                    extensions,
                })
                .await
            }
        }
    }

    /// A command receipt: the only place a steer, a config change, or a
    /// rejected prompt is confirmed.
    async fn on_response(&mut self, frame: &Value) -> Result<(), Gone> {
        let Some(pending) = frame["id"].as_str().and_then(|id| self.pending.remove(id)) else {
            return Ok(());
        };
        let success = frame["success"].as_bool().unwrap_or(false);
        let error = frame["error"]
            .as_str()
            .unwrap_or("the agent refused the command");
        match pending {
            Pending::Prompt if success => Ok(()),
            Pending::Prompt => {
                self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                    message: error.to_owned(),
                }))
                .await
            }
            Pending::Steer => self.emit(DriverEvent::Steered(success)).await,
            Pending::Configure(id, _) if !success => {
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    format!("`{id}` was refused: {error}"),
                )
                .await
            }
            Pending::Configure(id, value) => {
                if crate::adapter::apply_selection(&mut self.info, &id, &value) {
                    self.emit(DriverEvent::InfoChanged(self.info.clone()))
                        .await?;
                }
                if id.as_str() != "model" {
                    return Ok(());
                }
                self.window = frame["data"]["contextWindow"].as_u64().filter(|w| *w > 0);
                let wire_id = self
                    .wire
                    .send("get_available_thinking_levels", json!({}))
                    .await?;
                self.pending.insert(wire_id, Pending::Thinking);
                Ok(())
            }
            Pending::Thinking if !success => Ok(()),
            Pending::Thinking => {
                let current = self.selected("thinking");
                let option = thinking_option(&frame["data"]["levels"], current);
                self.info
                    .details
                    .config_options
                    .retain(|o| o.id.as_str() != "thinking");
                self.info
                    .configuration
                    .options
                    .remove(&ConfigId::new("thinking"));
                if let Some(option) = option {
                    if let Some(ConfigValue::Text(level)) = option.current.clone() {
                        self.info
                            .configuration
                            .options
                            .insert(ConfigId::new("thinking"), ConfigValue::Text(level));
                    }
                    self.info.details.config_options.push(option);
                }
                self.emit(DriverEvent::InfoChanged(self.info.clone())).await
            }
        }
    }

    /// Assistant messages get a streaming id. The wire also replays our own
    /// prompts and every tool result as messages; the engine already has both.
    fn on_message_start(&mut self, frame: &Value) -> Result<(), Gone> {
        if frame["message"]["role"].as_str() == Some("assistant") {
            self.next_message += 1;
            self.message = Some(MessageId::new(format!("m{}", self.next_message)));
        }
        Ok(())
    }

    /// One streamed delta of the assistant message.
    async fn on_update(&mut self, frame: &Value) -> Result<(), Gone> {
        let event = &frame["assistantMessageEvent"];
        let message_id = self.message();
        let text = event["delta"].as_str().unwrap_or_default().to_owned();
        match event["type"].as_str().unwrap_or_default() {
            "text_delta" => {
                self.emit_kind(EventKind::TextDelta { message_id, text })
                    .await
            }
            "thinking_delta" => {
                self.emit_kind(EventKind::ReasoningDelta { message_id, text })
                    .await
            }
            // The call is named before its arguments finish streaming;
            // `tool_execution_start` delivers them.
            "toolcall_start" => {
                let id = event["id"].as_str().unwrap_or_default();
                let name = event["toolName"].as_str().unwrap_or_default();
                self.update_tool(fresh_tool(id, name), Extensions::new())
                    .await
            }
            _ => Ok(()),
        }
    }

    /// Closes the streamed message, records the turn's stop evidence, and
    /// reports context occupancy: `usage.totalTokens` is exactly what pi's
    /// own `contextUsage` reports (probed 2026-08-30).
    async fn on_message_end(&mut self, frame: &Value) -> Result<(), Gone> {
        let message = &frame["message"];
        if message["role"].as_str() != Some("assistant") {
            return Ok(());
        }
        self.stop = Some(stop_reason(message));
        if let Some(message_id) = self.message.take() {
            self.emit_kind(EventKind::MessageEnded { message_id })
                .await?;
        }
        let usage = &message["usage"];
        self.cost += usage["cost"]["total"].as_f64().unwrap_or_default();
        let Some(used_tokens) = usage["totalTokens"].as_u64().filter(|t| *t > 0) else {
            return Ok(());
        };
        self.emit_kind(EventKind::ContextUsage {
            used_tokens,
            window_tokens: self.window,
            cost_usd: (self.cost > 0.0).then_some(self.cost),
        })
        .await
    }

    /// The tool lifecycle: arguments and status from `tool_execution_*`, with
    /// the growing result streamed as output deltas.
    async fn on_tool(&mut self, frame: &Value) -> Result<(), Gone> {
        let id = frame["toolCallId"].as_str().unwrap_or_default().to_owned();
        let name = frame["toolName"].as_str().unwrap_or_default();
        let done = frame["type"].as_str() == Some("tool_execution_end");
        let mut tool = self
            .tools
            .remove(&id)
            .unwrap_or_else(|| fresh_tool(&id, name));
        let mut extensions = Extensions::new();
        if done {
            tool.status = match frame["isError"].as_bool().unwrap_or(false) {
                true => ToolStatus::Failed,
                false => ToolStatus::Completed,
            };
            // pi reports an edit as a unified patch, not as before/after file
            // text, so it cannot fill a `FileDiff` — it rides as-is instead.
            if let Some(patch) = frame["result"]["details"]["patch"].as_str() {
                extensions.insert("pi/patch".into(), patch.into());
            }
        } else {
            apply_args(&mut tool, name, &frame["args"]);
            tool.status = ToolStatus::Running;
        }
        let result = &frame[if done { "result" } else { "partialResult" }];
        if let Some(text) = result_text(result) {
            self.stream_output(&id, &text).await?;
            if done {
                tool.output = Some(cap(text, OUTPUT_CAP));
            }
        }
        if done {
            self.streamed.remove(&id);
            return self.emit_tool(tool, extensions).await;
        }
        self.update_tool(tool, extensions).await
    }

    /// Emits whatever of the accumulated result has not been sent yet.
    async fn stream_output(&mut self, id: &str, text: &str) -> Result<(), Gone> {
        let sent = self.streamed.entry(id.to_owned()).or_default();
        if text.len() <= *sent {
            return Ok(());
        }
        let delta = text[*sent..].to_owned();
        *sent = text.len();
        self.emit_kind(EventKind::ToolOutputDelta {
            tool_id: ToolId::new(id),
            text: delta,
        })
        .await
    }

    /// Extension dialogs become questions. Fire-and-forget notices become
    /// diagnostics; the rest is TUI decoration with nowhere to go.
    async fn on_ui_request(&mut self, frame: &Value) -> Result<(), Gone> {
        let wire_id = frame["id"].as_str().unwrap_or_default().to_owned();
        let method = frame["method"].as_str().unwrap_or_default();
        if method == "notify" {
            let message = frame["message"].as_str().unwrap_or_default();
            let level = match frame["notifyType"].as_str() {
                Some("error") => DiagnosticLevel::Error,
                Some("warning") => DiagnosticLevel::Warning,
                _ => DiagnosticLevel::Info,
            };
            return self.diagnostic(level, message).await;
        }
        self.next_request += 1;
        let id = RequestId::new(format!("r{}", self.next_request));
        let Some(question) = dialog_question(&id, method, frame) else {
            return Ok(());
        };
        self.requests
            .insert(id.clone(), (wire_id, method.to_owned()));
        let mut extensions = Extensions::new();
        // pi resolves the dialog itself when this expires and ignores a late
        // answer; an app can use it to withdraw the question in time.
        if let Some(timeout) = frame["timeout"].as_u64() {
            extensions.insert("pi/timeout_ms".into(), timeout.into());
        }
        self.emit(DriverEvent::Event {
            kind: EventKind::RequestOpened(Request::Question(QuestionRequest {
                id,
                questions: vec![question],
            })),
            parent_tool_id: None,
            extensions,
        })
        .await
    }

    /// Replies to one open dialog in the shape its method expects.
    async fn answer(&mut self, request: RequestId, answer: Answer) -> Result<(), Gone> {
        let Some((wire_id, method)) = self.requests.remove(&request) else {
            return Ok(());
        };
        let answer = match answer {
            Answer::Question(answers) => answers.into_iter().next(),
            _ => None,
        };
        let mut frame = dialog_response(&method, answer);
        frame["type"] = "extension_ui_response".into();
        frame["id"] = wire_id.into();
        self.wire.write(frame).await?;
        Ok(())
    }

    /// `agent_settled`, the one frame that means the run is really over.
    async fn on_settled(&mut self) -> Result<(), Gone> {
        let stop = match std::mem::take(&mut self.aborting) {
            true => StopReason::Cancelled,
            false => self.stop.take().unwrap_or(StopReason::Completed {
                source: CompletionSource::Protocol,
            }),
        };
        self.stop = None;
        // An aborted tool never gets its `tool_execution_end`; nothing from
        // a settled run is still coming, so its bookkeeping goes with it.
        self.tools.clear();
        self.streamed.clear();
        self.emit(DriverEvent::TurnEnded(stop)).await
    }

    /// Sends a `prompt` or `steer` carrying the input's text and its images;
    /// every attachment also rides the text as a path ref.
    async fn send_input(&mut self, command: &str, input: &Input) -> Result<String, Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.clone()) {
            self.diagnostic(DiagnosticLevel::Warning, problem).await?;
        }
        let images: Vec<Value> = loaded
            .iter()
            .filter_map(|l| l.image.as_ref())
            .map(|image| json!({ "type": "image", "data": image.base64, "mimeType": image.mime }))
            .collect();
        let mut body = json!({ "message": attach::with_refs(input.as_text(), &loaded) });
        if !images.is_empty() {
            body["images"] = Value::Array(images);
        }
        Ok(self.wire.send(command, body).await?)
    }

    /// Applies one live option change; the response confirms it.
    async fn configure(&mut self, id: ConfigId, value: ConfigValue) -> Result<(), Gone> {
        let ConfigValue::Text(text) = &value else {
            return Ok(());
        };
        let (command, body) = match id.as_str() {
            "model" => match split_model(text) {
                Ok((provider, model)) => (
                    "set_model",
                    json!({ "provider": provider, "modelId": model }),
                ),
                Err(e) => {
                    return self
                        .diagnostic(DiagnosticLevel::Warning, e.to_string())
                        .await;
                }
            },
            "thinking" => ("set_thinking_level", json!({ "level": text })),
            _ => return Ok(()),
        };
        let wire_id = self.wire.send(command, body).await?;
        self.pending.insert(wire_id, Pending::Configure(id, value));
        Ok(())
    }

    /// Emits the tool snapshot and keeps it for the next lifecycle frame.
    async fn update_tool(&mut self, tool: ToolUpdate, extensions: Extensions) -> Result<(), Gone> {
        self.tools.insert(tool.id.to_string(), tool.clone());
        self.emit_tool(tool, extensions).await
    }

    async fn emit_tool(&mut self, tool: ToolUpdate, extensions: Extensions) -> Result<(), Gone> {
        self.emit(DriverEvent::Event {
            kind: EventKind::ToolUpdated(tool),
            parent_tool_id: None,
            extensions,
        })
        .await
    }

    /// The message being streamed, minting one if a delta arrives first.
    fn message(&mut self) -> MessageId {
        self.message
            .get_or_insert_with(|| {
                self.next_message += 1;
                MessageId::new(format!("m{}", self.next_message))
            })
            .clone()
    }

    /// The currently advertised value of one option.
    fn selected(&self, id: &str) -> Option<String> {
        match self.info.configuration.options.get(&ConfigId::new(id)) {
            Some(ConfigValue::Text(value)) => Some(value.clone()),
            _ => None,
        }
    }

    async fn report_exit(&mut self) {
        let status = self.child.exit_status(CLOSE_GRACE).await;
        let stderr = self.child.stderr_tail();
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

/// The engine or the agent is gone; the drive task unwinds.
struct Gone;

impl From<std::io::Error> for Gone {
    fn from(_: std::io::Error) -> Self {
        Gone
    }
}

// ---------------------------------------------------------------------------
// Frame decoding
// ---------------------------------------------------------------------------

/// The last assistant message's own verdict. Our own `abort` is recognised by
/// the drive task instead: pi reports it as `error` with an aborted message,
/// never as `aborted` (probed 2026-08-30).
fn stop_reason(message: &Value) -> StopReason {
    match message["stopReason"].as_str().unwrap_or("stop") {
        "aborted" => StopReason::Cancelled,
        "error" => StopReason::Failed {
            message: message["errorMessage"]
                .as_str()
                .unwrap_or("the agent failed")
                .to_owned(),
        },
        _ => StopReason::Completed {
            source: CompletionSource::Protocol,
        },
    }
}

/// A tool call the wire has only named so far.
fn fresh_tool(id: &str, name: &str) -> ToolUpdate {
    ToolUpdate {
        id: ToolId::new(id),
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

/// pi's built-in tool names to the portable kind; anything else is an
/// extension or MCP tool and keeps its raw shape.
fn tool_kind(name: &str) -> ToolKind {
    match name {
        "bash" | "powershell" => ToolKind::Execute,
        "read" | "ls" => ToolKind::Read,
        "edit" | "write" => ToolKind::Edit,
        "grep" | "find" => ToolKind::Search,
        _ => ToolKind::Other,
    }
}

/// Fills in the decoded arguments once the wire delivers them.
fn apply_args(tool: &mut ToolUpdate, name: &str, args: &Value) {
    let field = |key: &str| args[key].as_str().filter(|v| !v.is_empty());
    tool.title = title(name, args);
    tool.input = match name {
        "bash" | "powershell" => ToolInput::Command {
            command: field("command").unwrap_or_default().to_owned(),
            cwd: None,
        },
        "read" | "write" | "edit" | "ls" => match field("path") {
            Some(path) => ToolInput::Path(PathBuf::from(path)),
            None => ToolInput::None,
        },
        "grep" | "find" => ToolInput::Pattern(field("pattern").unwrap_or_default().to_owned()),
        _ => {
            tool.raw = Some(RawTool {
                name: name.to_owned(),
                input: args.clone(),
            });
            ToolInput::None
        }
    };
    if let ToolInput::Path(path) = &tool.input {
        tool.locations = vec![path.clone()];
    }
}

/// Human title: the tool name plus its most telling argument.
fn title(name: &str, args: &Value) -> String {
    match ["command", "path", "pattern"]
        .iter()
        .find_map(|key| args[*key].as_str().filter(|v| !v.is_empty()))
    {
        Some(detail) => format!("{name} {detail}"),
        None => name.to_owned(),
    }
}

/// The text blocks of a tool result, joined; `None` when it has none yet.
fn result_text(result: &Value) -> Option<String> {
    let text: String = result["content"]
        .as_array()?
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect();
    (!text.is_empty()).then_some(text)
}

/// One extension dialog as a question. `select` and `confirm` offer choices;
/// `input` and `editor` are free text.
fn dialog_question(id: &RequestId, method: &str, frame: &Value) -> Option<Question> {
    let title = frame["title"].as_str().unwrap_or_default().to_owned();
    let choices = |labels: Vec<&str>| -> Vec<Choice> {
        labels
            .into_iter()
            .map(|label| Choice {
                id: ChoiceId::new(label),
                label: label.to_owned(),
                description: None,
            })
            .collect()
    };
    let (text, choices) = match method {
        "select" => {
            let options = frame["options"]
                .as_array()?
                .iter()
                .filter_map(|option| option.as_str())
                .collect();
            (title, choices(options))
        }
        "confirm" => {
            let detail = frame["message"].as_str().unwrap_or_default();
            let text = match detail.is_empty() {
                true => title,
                false => format!("{title}\n\n{detail}"),
            };
            (text, choices(vec![CONFIRM_YES, CONFIRM_NO]))
        }
        "input" | "editor" => (title, Vec::new()),
        // setStatus, setWidget, setTitle, set_editor_text: TUI decoration.
        _ => return None,
    };
    Some(Question {
        id: QuestionId::new(id.as_str()),
        text,
        header: None,
        allows_free_text: choices.is_empty(),
        multi_select: false,
        choices,
    })
}

const CONFIRM_YES: &str = "Yes";
const CONFIRM_NO: &str = "No";

/// The `extension_ui_response` body for an answer: `confirm` takes a
/// boolean under `confirmed`, every other dialog takes text under `value`,
/// and no usable answer cancels the dialog.
fn dialog_response(method: &str, answer: Option<QuestionAnswer>) -> Value {
    let text = match answer {
        Some(QuestionAnswer::Text(text)) => Some(text),
        Some(QuestionAnswer::Choices(choices)) => choices
            .into_iter()
            .next()
            .map(|choice| choice.as_str().to_owned()),
        None => None,
    };
    match (method, text) {
        ("confirm", Some(choice)) => json!({ "confirmed": choice == CONFIRM_YES }),
        (_, Some(text)) => json!({ "value": text }),
        _ => json!({ "cancelled": true }),
    }
}

// ---------------------------------------------------------------------------
// Wire
// ---------------------------------------------------------------------------

/// The JSONL command/event wire. Commands carry a correlating `id`; every
/// other line is an event.
struct Wire {
    stdin: tokio::process::ChildStdin,
    /// All frames the reader task saw, bounded; pipe backpressure beyond.
    frames: mpsc::Receiver<Value>,
    next_id: u64,
    recorder: Option<WireRecorder>,
}

impl Wire {
    /// Takes the child's stdio and starts the line-reader task. Records split
    /// on `\n` only, with a trailing `\r` stripped, as the protocol demands.
    fn over(child: &mut process::Child, recorder: Option<WireRecorder>) -> Self {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, frames) = mpsc::channel(FRAME_BUFFER);
        let reader_recorder = recorder.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(recorder) = &reader_recorder {
                    recorder.record("in", &frame);
                }
                if tx.send(frame).await.is_err() {
                    break;
                }
            }
        });
        Self {
            stdin,
            frames,
            next_id: 1,
            recorder,
        }
    }

    /// Sends a command and returns the id its response will carry.
    async fn send(&mut self, command: &str, body: Value) -> std::io::Result<String> {
        let id = format!("c{}", self.next_id);
        self.next_id += 1;
        let mut frame = body;
        frame["type"] = command.into();
        frame["id"] = id.clone().into();
        self.write(frame).await?;
        Ok(id)
    }

    /// Dismisses one extension dialog so the extension stops blocking.
    async fn cancel_dialog(&mut self, wire_id: &str) -> std::io::Result<()> {
        self.write(json!({
            "type": "extension_ui_response", "id": wire_id, "cancelled": true,
        }))
        .await
    }

    /// Handshake only: sends a command and blocks on its response, skipping
    /// any event the CLI emits at startup.
    async fn roundtrip(&mut self, command: &str) -> Result<Value, AgentError> {
        let id = self
            .send(command, json!({}))
            .await
            .map_err(|_| closed(command))?;
        loop {
            let frame = self.frames.recv().await.ok_or_else(|| closed(command))?;
            if frame["type"].as_str() != Some("response") || frame["id"].as_str() != Some(&id) {
                continue;
            }
            if !frame["success"].as_bool().unwrap_or(false) {
                let error = frame["error"].as_str().unwrap_or("command failed");
                return Err(AgentError::ProtocolFailed(format!("{command}: {error}")));
            }
            return Ok(frame["data"].clone());
        }
    }

    async fn write(&mut self, frame: Value) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        if let Some(recorder) = &self.recorder {
            recorder.record("out", &frame);
        }
        let mut line = frame.to_string();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await
    }
}

fn closed(command: &str) -> AgentError {
    AgentError::ProtocolFailed(format!("agent closed the wire during `{command}`"))
}
