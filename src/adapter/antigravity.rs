//! Native Antigravity adapter: drives `agy` over its headless stream-json
//! wire (`--input-format=stream-json --output-format=stream-json`, probed
//! 2026-09-02 against agy 1.1.24). One JSON object per line each way: a
//! `user` message in, `init` / `step_update` / `result` frames out. Exactly
//! one `result` per turn, so turn end is deterministic.
//!
//! The headless wire cannot prompt: permission-gated tools are auto-denied
//! unless launched with `--dangerously-skip-permissions`, `ask_question` is
//! skipped, and mid-turn input queues. Cancel is the process: SIGINT kills
//! it, so `Cancel` kills and respawns on the same conversation. Google's
//! separate ACP server has the interactive features; discovery prefers it
//! when installed (see the catalog's `Upgrade`).
//!
//! High level: `connect` → `launch` (`start`: spawn and wait for `init`,
//! beside the model catalog and version side processes) → `driver_info`; then
//! `Drive::run` turns commands into `user` frames (`handle_command`) and
//! frames into events (`handle_frame`, `on_*`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, CLOSE_GRACE, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
    Emitter, FRAME_BUFFER, Gone, HANDSHAKE_TIMEOUT, LineWire, OUTPUT_CAP, WireRecorder, attach,
    auth_hinted, cap, with_stderr,
};
use crate::agent::{
    AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigChoice, ConfigId,
    ConfigKind, ConfigOption, ConfigValue, Input, PermissionMode, ResumeToken,
    SessionConfiguration, SessionOptions, SessionStart,
};
use crate::catalog::AgentProfile;
use crate::error::AgentError;
use crate::event::{
    CompletionSource, DiagnosticLevel, EventKind, MessageId, RawTool, StopReason, ToolId,
    ToolInput, ToolKind, ToolStatus, ToolUpdate,
};
use crate::process::{self, Spawn};

/// `--version` and the model list are a few seconds at most; this only
/// bounds a hang.
const SIDE_PROCESS_TIMEOUT: Duration = Duration::from_secs(15);
/// `--mode` values from `agy --help`; the wire never echoes the choice.
const MODES: &[&str] = &["accept-edits", "plan"];

/// Launches `agy` in stream-json mode.
pub(crate) struct AntigravityAdapter {
    /// The catalog entry: its logged-out fingerprints type the open failure.
    profile: &'static AgentProfile,
}

impl AntigravityAdapter {
    /// One instance drives every antigravity session.
    pub(crate) fn new(profile: &'static AgentProfile) -> Self {
        Self { profile }
    }
}

#[async_trait]
impl Adapter for AntigravityAdapter {
    /// Spawns the CLI, waits for `init`, and hands the wire to the drive task.
    async fn connect(&self, mut request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let (ev_tx, ev_rx) = mpsc::channel(FRAME_BUFFER);
        let events = Emitter::new(ev_tx);
        let recorder = WireRecorder::for_session(&request.options, &events).await;
        let env = crate::adapter::config_home_env(&request.installation, &request.options)?;
        let (child, wire, info) = launch(&request, recorder.clone(), &env)
            .await
            .map_err(|e| {
                auth_hinted(
                    e,
                    Some(self.profile),
                    &request.installation.executable_path,
                    &env,
                )
            })?;
        // A cancel respawns on the conversation this open landed on.
        if let Some(token) = &info.resume_token {
            request.options.start = SessionStart::Resume(token.clone());
        }
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        tokio::spawn(
            Drive {
                wire,
                child,
                events,
                recorder,
                request,
                message: None,
                next_message: 0,
                tools: BTreeMap::new(),
                last_usage: None,
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
// LAUNCH AND HANDSHAKE
// ---------------------------------------------------------------------------

/// Spawns the CLI and waits for its `init` frame, while the model catalog
/// and version are read from two side processes.
async fn launch(
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
    env: &[(String, String)],
) -> Result<(process::Child, LineWire, DriverInfo), AgentError> {
    let exe = &request.installation.executable_path;
    let (started, models, version) = tokio::join!(
        start(request, recorder),
        models(exe, env),
        version(exe, env)
    );
    let (child, wire, init) = started?;
    Ok((
        child,
        wire,
        driver_info(&init, models, version, &request.options),
    ))
}

/// A process past `init`, or the reason it stopped short with its stderr
/// attached. A cancel uses this alone to resume the conversation.
async fn start(
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
) -> Result<(process::Child, LineWire, Value), AgentError> {
    let mut child = spawn(request).await?;
    let mut wire = LineWire::over(&mut child, recorder);
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, wait_init(&mut wire)).await {
        Ok(Ok(init)) => Ok((child, wire, init)),
        Ok(Err(e)) => {
            // Shutdown first: it joins the stderr reader, so the tail is
            // complete before the error is rendered and hint-matched.
            child.shutdown(CLOSE_GRACE).await;
            Err(with_stderr(e, &child))
        }
        Err(_) => {
            child.shutdown(CLOSE_GRACE).await;
            Err(AgentError::HandshakeTimeout)
        }
    }
}

/// The child on the session's cwd with the session's launch flags. The cwd
/// is also added to the workspace: without `--add-dir` the agent treats the
/// process cwd as scratch and writes new files under `~/.gemini/…/scratch`
/// (probed 2026-09-07, both ways).
async fn spawn(request: &ConnectRequest) -> Result<process::Child, AgentError> {
    let cwd = request.options.cwd().clone();
    let mut args = vec![
        "--input-format=stream-json".to_owned(),
        "--output-format=stream-json".to_owned(),
        "--add-dir".to_owned(),
        cwd.to_string_lossy().into_owned(),
    ];
    args.extend(launch_args(&request.options)?);
    process::spawn(Spawn {
        exec_path: request.installation.executable_path.clone(),
        args,
        cwd,
        env: crate::adapter::config_home_env(&request.installation, &request.options)?,
    })
    .await
}

/// Session start, permission mode, and creation-time config as launch
/// flags. Anything the CLI cannot take is refused here, never dropped.
fn launch_args(options: &SessionOptions) -> Result<Vec<String>, AgentError> {
    if !options.mcp_servers.is_empty() {
        return Err(AgentError::UnsupportedFeature(
            "client-declared MCP servers (agy configures MCP with `agy mcp add`)".into(),
        ));
    }
    if options.no_tools {
        return Err(AgentError::UnsupportedFeature(
            "disabling tools (agy has no launch flag for it)".into(),
        ));
    }
    let mut args = Vec::new();
    if options.permission_mode == PermissionMode::AutoApprove {
        args.push("--dangerously-skip-permissions".to_owned());
    }
    match &options.start {
        SessionStart::New => {}
        SessionStart::Resume(token) => {
            args.extend(["--conversation".to_owned(), token.as_str().to_owned()]);
        }
        SessionStart::Fork { .. } => {
            return Err(AgentError::UnsupportedFeature(
                "fork (agy resumes a conversation in place and cannot branch it)".into(),
            ));
        }
    }
    for (id, value) in &options.configure {
        match (id.as_str(), value) {
            ("model", ConfigValue::Text(model)) => {
                args.extend(["--model".to_owned(), model.clone()]);
            }
            ("mode", ConfigValue::Text(mode)) if MODES.contains(&mode.as_str()) => {
                args.extend(["--mode".to_owned(), mode.clone()]);
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

/// The `init` frame, or the `result` that replaced it: a bad flag or a
/// logged-out account ends the process with an ERROR result and no `init`.
async fn wait_init(wire: &mut LineWire) -> Result<Value, AgentError> {
    loop {
        let frame =
            wire.frames.recv().await.ok_or_else(|| {
                AgentError::ProtocolFailed("agy closed the wire before init".into())
            })?;
        match frame["event"].as_str() {
            Some("init") => return Ok(frame),
            Some("result") => {
                let error = frame["result"]["error"]
                    .as_str()
                    .unwrap_or("agy refused to start");
                return Err(match error.contains("invalid model selection") {
                    true => AgentError::InvalidConfiguration(error.to_owned()),
                    false => AgentError::ProtocolFailed(error.to_owned()),
                });
            }
            _ => continue,
        }
    }
}

/// `agy --output-format=json models` (the flag is global, before the
/// subcommand). Empty when the fetch fails; the option is then omitted.
async fn models(exe: &Path, env: &[(String, String)]) -> Vec<ConfigChoice> {
    let Some(out) = output(exe, env, &["--output-format=json", "models"]).await else {
        return Vec::new();
    };
    let Ok(report) = serde_json::from_str::<Value>(&out) else {
        return Vec::new();
    };
    report["command"]["data"]["models"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let value = model["id"].as_str()?.to_owned();
            Some(ConfigChoice {
                label: model["label"].as_str().unwrap_or(&value).to_owned(),
                value,
                description: None,
            })
        })
        .collect()
}

/// `agy --version`; the wire never reports it.
async fn version(exe: &Path, env: &[(String, String)]) -> Option<String> {
    let version = output(exe, env, &["--version"]).await?;
    let version = version.trim().to_owned();
    (!version.is_empty()).then_some(version)
}

/// Captured stdout of a short side process in the session's config home;
/// `None` if it fails or hangs.
async fn output(exe: &Path, env: &[(String, String)], args: &[&str]) -> Option<String> {
    let mut command = tokio::process::Command::new(exe);
    command
        .args(args)
        .envs(env.iter().cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let out = tokio::time::timeout(SIDE_PROCESS_TIMEOUT, command.output())
        .await
        .ok()?
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// What the handshake learned, in the engine vocabulary. A successful
/// `init` proves the login: logged out, the process never gets this far.
fn driver_info(
    init: &Value,
    models: Vec<ConfigChoice>,
    version: Option<String>,
    options: &SessionOptions,
) -> DriverInfo {
    let mut configuration = SessionConfiguration::default();
    let mut config_options = Vec::new();
    // Both are launch flags the wire never echoes, so the current value is
    // whatever this session was opened with.
    let requested = |id: &str| match options.configure.iter().find(|(k, _)| k.as_str() == id) {
        Some((_, ConfigValue::Text(v))) => Some(v.clone()),
        _ => None,
    };
    for (id, name, category, choices) in [
        ("model", "Model", "model", models),
        (
            "mode",
            "Mode",
            "mode",
            crate::adapter::level_choices(MODES.iter().copied()),
        ),
    ] {
        if choices.is_empty() {
            continue;
        }
        let current = requested(id).map(ConfigValue::Text);
        if let Some(current) = &current {
            configuration
                .options
                .insert(ConfigId::new(id), current.clone());
        }
        config_options.push(ConfigOption {
            id: ConfigId::new(id),
            name: name.into(),
            category: Some(category.into()),
            kind: ConfigKind::Select { choices },
            current,
            live: false,
        });
    }
    DriverInfo {
        details: AgentDetails {
            version,
            auth: AuthStatus::Authenticated {
                kind: AuthKind::Subscription,
                account: None,
            },
            // No `Permissions`, `Questions`, or `Steer`: the headless wire
            // auto-denies, auto-skips, and queues (probed 2026-09-02).
            capabilities: Capabilities::new([Capability::Resume, Capability::ContextUsage]),
            config_options,
            commands: Vec::new(),
        },
        configuration,
        resume_token: init["conversation_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(ResumeToken::new),
        title: None,
        deterministic_turn_end: true,
        deterministic_agent_turn_end: true,
        tools_disabled: false,
    }
}

// ---------------------------------------------------------------------------
// DRIVE TASK: engine commands out, wire frames in
// ---------------------------------------------------------------------------

struct Drive {
    wire: LineWire,
    child: process::Child,
    events: Emitter,
    recorder: Option<WireRecorder>,
    /// The open request, pointed at this conversation for the respawn a
    /// cancel needs.
    request: ConnectRequest,
    /// The assistant message being streamed.
    message: Option<MessageId>,
    next_message: u64,
    /// Tools still running: a kill ends them without a DONE frame.
    tools: BTreeMap<ToolId, ToolUpdate>,
    /// Context occupancy from the turn's last model call: `result.usage`
    /// sums every step's snapshot instead (recorded), so it is not the size.
    last_usage: Option<u64>,
}

impl Drive {
    /// Main loop until the engine or the agent goes away.
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<DriverCommand>) {
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
                        self.events.exited(&mut self.child).await;
                        break;
                    }
                },
            }
        }
        self.child.shutdown(CLOSE_GRACE).await;
    }

    /// One engine command as wire frames, or the respawn a cancel needs.
    async fn handle_command(&mut self, cmd: DriverCommand) -> Result<(), Gone> {
        match cmd {
            DriverCommand::StartTurn { input } => {
                self.events.send(DriverEvent::TurnAck).await?;
                self.send_user(&input).await
            }
            // Never sent: `Steer`, `Answer`, and `Compact` are not
            // advertised. Refusing a steer requeues it.
            DriverCommand::Steer { .. } => self.events.send(DriverEvent::Steered(false)).await,
            DriverCommand::Answer { .. } | DriverCommand::Compact => Ok(()),
            DriverCommand::Cancel => self.cancel().await,
            DriverCommand::Configure(id, _) => {
                self.events
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        format!("`{id}` is creation-only on agy: reopen with the resume token"),
                    )
                    .await
            }
            DriverCommand::Rollback(..) => {
                self.events
                    .diagnostic(DiagnosticLevel::Warning, "rollback is not supported on agy")
                    .await
            }
            DriverCommand::Close => unreachable!("handled in run"),
        }
    }

    /// The only way to stop a turn is to stop the process (probed: SIGINT
    /// ends it with exit 1 and a dead pipe). The conversation survives on
    /// disk, so the turn ends `Cancelled` and a fresh process resumes it.
    async fn cancel(&mut self) -> Result<(), Gone> {
        self.child.shutdown(CLOSE_GRACE).await;
        self.message = None;
        self.last_usage = None;
        self.settle_tools().await?;
        self.events
            .send(DriverEvent::TurnEnded(StopReason::Cancelled))
            .await?;
        match start(&self.request, self.recorder.clone()).await {
            Ok((child, wire, _)) => {
                self.child = child;
                self.wire = wire;
                Ok(())
            }
            Err(e) => {
                self.events
                    .diagnostic(
                        DiagnosticLevel::Error,
                        format!("could not resume after cancel: {e}"),
                    )
                    .await?;
                self.events.exited(&mut self.child).await;
                Err(Gone)
            }
        }
    }

    /// Routes one wire frame by its `event`.
    async fn handle_frame(&mut self, frame: Value) -> Result<(), Gone> {
        match frame["event"].as_str().unwrap_or_default() {
            "step_update" => self.on_step(&frame["step_update"]).await,
            "result" => self.on_result(&frame["result"]).await,
            _ => Ok(()),
        }
    }

    /// One step of the turn: streamed text, a tool, a subagent, or a
    /// question the headless wire skipped.
    async fn on_step(&mut self, step: &Value) -> Result<(), Gone> {
        let done = step["state"].as_str() != Some("ACTIVE");
        match step["step_type"].as_str().unwrap_or_default() {
            // A message exists once text arrives: a step with no text (a
            // tool-only response) opens nothing to end.
            "agent_response" => {
                if let Some(used) = step["usage"]["total_tokens"].as_u64().filter(|t| *t > 0) {
                    self.last_usage = Some(used);
                }
                if let Some(text) = step["text_delta"].as_str().filter(|t| !t.is_empty()) {
                    let message_id = self.message();
                    self.events
                        .event(EventKind::TextDelta {
                            message_id,
                            text: text.to_owned(),
                        })
                        .await?;
                }
                if let Some(message_id) = self.message.take_if(|_| done) {
                    self.events
                        .event(EventKind::MessageEnded { message_id })
                        .await?;
                }
                Ok(())
            }
            "tool" | "subagent" => {
                let tool = tool(step);
                match tool.status.is_active() {
                    true => self.tools.insert(tool.id.clone(), tool.clone()),
                    false => self.tools.remove(&tool.id),
                };
                self.events.event(EventKind::ToolUpdated(tool)).await
            }
            // `ask_question` in headless mode: the agent moves on without
            // an answer, and the wire only shows an unnamed step.
            "unknown" => {
                self.events
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        "the agent asked a question that agy's headless mode skipped",
                    )
                    .await
            }
            // `user_input` echoes our prompt; `system_message` is narration.
            _ => Ok(()),
        }
    }

    /// Exactly one `result` per turn: usage, then the turn's end.
    async fn on_result(&mut self, result: &Value) -> Result<(), Gone> {
        self.message = None;
        self.settle_tools().await?;
        if let Some(used) = self.last_usage.take() {
            self.events
                .event(EventKind::ContextUsage {
                    used_tokens: used,
                    window_tokens: None,
                    cost_usd: None,
                })
                .await?;
        }
        let stop = match result["status"].as_str() {
            Some("SUCCESS") => StopReason::Completed {
                source: CompletionSource::Protocol,
            },
            _ => StopReason::Failed {
                message: result["error"]
                    .as_str()
                    .unwrap_or("the agent failed")
                    .to_owned(),
            },
        };
        self.events.send(DriverEvent::TurnEnded(stop)).await
    }

    /// One `user` frame; attachments ride the text as path refs (the wire
    /// takes text only).
    async fn send_user(&mut self, input: &Input) -> Result<(), Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.clone()) {
            self.events
                .diagnostic(DiagnosticLevel::Warning, problem)
                .await?;
        }
        let content = attach::with_refs(input.as_text(), &loaded);
        self.wire
            .write(json!({ "event": "user", "message": { "content": content } }))
            .await?;
        Ok(())
    }

    /// Tools the wire never finished (a kill, or a turn ending around
    /// them) are cancelled so the caller's tool view drains.
    async fn settle_tools(&mut self) -> Result<(), Gone> {
        for (_, mut tool) in std::mem::take(&mut self.tools) {
            tool.status = ToolStatus::Cancelled;
            self.events.event(EventKind::ToolUpdated(tool)).await?;
        }
        Ok(())
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
}

// ---------------------------------------------------------------------------
// FRAME DECODING
// ---------------------------------------------------------------------------

/// A `tool` or `subagent` step as a tool snapshot. The step index is the
/// id: ACTIVE and DONE frames of one call share it.
fn tool(step: &Value) -> ToolUpdate {
    let name = step["tool_name"].as_str().unwrap_or("tool");
    let info = &step["tool_info"];
    let params = &info["parameters"];
    let subagent = step["step_type"].as_str() == Some("subagent");
    let field = |key: &str| params[key].as_str().filter(|v| !v.is_empty());
    let input = match name {
        "run_command" => ToolInput::Command {
            command: field("CommandLine").unwrap_or_default().to_owned(),
            cwd: field("Cwd").map(PathBuf::from),
        },
        "grep_search" | "find_by_name" => ToolInput::Pattern(
            field("Query")
                .or(field("Pattern"))
                .unwrap_or_default()
                .to_owned(),
        ),
        "read_url_content" | "open_browser_url" => {
            ToolInput::Url(field("Url").unwrap_or_default().to_owned())
        }
        "search_web" => ToolInput::Query(field("query").unwrap_or_default().to_owned()),
        _ => match field("TargetFile")
            .or(field("AbsolutePath"))
            .or(field("DirectoryPath"))
        {
            Some(path) => ToolInput::Path(PathBuf::from(path)),
            None => ToolInput::None,
        },
    };
    let output = match step["state"].as_str() {
        Some("ERROR") => info["error"]["message"].as_str(),
        _ => info["output"].as_str(),
    }
    .map(|text| cap(text.to_owned(), OUTPUT_CAP));
    let locations = match &input {
        ToolInput::Path(path) => vec![path.clone()],
        _ => Vec::new(),
    };
    ToolUpdate {
        id: ToolId::new(format!(
            "s{}",
            step["step_index"].as_u64().unwrap_or_default()
        )),
        kind: match subagent {
            true => ToolKind::Subagent,
            false => tool_kind(name),
        },
        title: title(name, &input, &step["subagent_info"]),
        status: match step["state"].as_str() {
            Some("ACTIVE") => ToolStatus::Running,
            Some("ERROR") => ToolStatus::Failed,
            _ => ToolStatus::Completed,
        },
        input,
        output,
        diffs: Vec::new(),
        locations,
        raw: Some(RawTool {
            name: name.to_owned(),
            input: match subagent {
                true => step["subagent_info"].clone(),
                false => params.clone(),
            },
        }),
    }
}

/// agy's built-in tool names to the portable kind.
fn tool_kind(name: &str) -> ToolKind {
    match name {
        "run_command" | "send_command_input" | "notebook_execution" => ToolKind::Execute,
        "view_file" | "list_dir" | "read_resource" | "read_browser_page" => ToolKind::Read,
        "write_to_file"
        | "replace_file_content"
        | "multi_replace_file_content"
        | "sed_file"
        | "notebook_edit" => ToolKind::Edit,
        "grep_search" | "find_by_name" => ToolKind::Search,
        "read_url_content" | "search_web" | "open_browser_url" => ToolKind::Fetch,
        _ => ToolKind::Other,
    }
}

/// Human title: the tool name plus its most telling argument, or the
/// subagent's role.
fn title(name: &str, input: &ToolInput, subagent_info: &Value) -> String {
    if let Some(role) = subagent_info["subagents"][0]["role"].as_str() {
        return format!("subagent {role}");
    }
    match input {
        ToolInput::Command { command, .. } => format!("{name} {command}"),
        ToolInput::Path(path) => format!("{name} {}", path.display()),
        ToolInput::Pattern(p) | ToolInput::Url(p) | ToolInput::Query(p) => format!("{name} {p}"),
        _ => name.to_owned(),
    }
}
