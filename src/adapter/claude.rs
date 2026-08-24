//! Native Claude Code adapter: drives `claude` over its stream-json wire
//! (validated 2026-08-23, ticket 04). Turn end is deterministic: exactly one
//! `result` frame per turn. The engine owns all turn rules.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo, attach,
};
use crate::agent::{
    AccountInfo, AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigChoice,
    ConfigId, ConfigKind, ConfigOption, ConfigSelection, ConfigValue, Input, LoginMethod,
    McpConnection, McpServer, McpTransport, ResumeToken, SessionConfiguration, SlashCommand,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Choice, ChoiceId, CompletionSource, Diagnostic, DiagnosticLevel, EventKind, Extensions,
    FileDiff, MessageId, PermissionChoice, PermissionRequest, PlanEntry, PlanStatus, Question,
    QuestionAnswer, QuestionId, QuestionRequest, RawTool, Request, RequestId, StopReason, ToolId,
    ToolInput, ToolKind, ToolStatus, ToolUpdate,
};
use crate::process::{self, Spawn};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_GRACE: Duration = Duration::from_secs(2);
const FRAME_BUFFER: usize = 64;
const OUTPUT_CAP: usize = 16 * 1024;

/// Launches `claude` in stream-json mode; one instance serves every session.
pub(crate) struct ClaudeAdapter;

impl ClaudeAdapter {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Adapter for ClaudeAdapter {
    /// Spawns the CLI, runs the `initialize` control handshake, and hands the
    /// live wire to the drive task.
    async fn connect(&self, request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let mut args: Vec<String> = [
            "-p",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--permission-prompt-tool",
            "stdio",
            "--replay-user-messages",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        // Mint the session id so the resume token exists at open; the CLI
        // only reports its own with the first prompt's `system/init`.
        let minted = match &request.options.resume {
            Some(token) => {
                args.push("--resume".into());
                args.push(token.as_str().to_owned());
                None
            }
            None => {
                let uuid = mint_uuid(0);
                args.push("--session-id".into());
                args.push(uuid.clone());
                Some(uuid)
            }
        };
        if !request.options.mcp_servers.is_empty() {
            args.push("--mcp-config".into());
            args.push(mcp_config(&request.options.mcp_servers).to_string());
        }
        // Creation-time config rides the launch flags.
        for (id, value) in &request.options.configure {
            match (id.as_str(), value) {
                ("mode", ConfigValue::Text(mode)) => {
                    args.push("--permission-mode".into());
                    args.push(mode.clone());
                }
                ("model", ConfigValue::Text(model)) => {
                    args.push("--model".into());
                    args.push(model.clone());
                }
                ("effort", ConfigValue::Text(effort)) => {
                    args.push("--effort".into());
                    args.push(effort.clone());
                }
                _ => {
                    return Err(AgentError::InvalidConfiguration(format!(
                        "`{id}` is not a creation-time option of this agent"
                    )));
                }
            }
        }
        let mut child = process::spawn(Spawn {
            program: request.installation.executable.clone(),
            args,
            cwd: request.options.cwd().clone(),
            env: Vec::new(),
        })
        .await?;
        let mut wire = Wire::over(&mut child);

        let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake(&mut wire, &request));
        let info = match handshake.await {
            Ok(Ok(mut info)) => {
                if let Some(uuid) = minted {
                    info.resume_token = Some(ResumeToken::new(&uuid));
                }
                info
            }
            Ok(Err(e)) => {
                let e = with_stderr(e, &child);
                child.shutdown(CLOSE_GRACE).await;
                return Err(e);
            }
            Err(_) => {
                child.shutdown(CLOSE_GRACE).await;
                return Err(AgentError::HandshakeTimeout);
            }
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (ev_tx, ev_rx) = mpsc::channel(FRAME_BUFFER);
        tokio::spawn(
            Drive {
                wire,
                child,
                events: ev_tx,
                info: info.clone(),
                tools: HashMap::new(),
                requests: HashMap::new(),
                messages: HashMap::new(),
                config: None,
                turn_uuid: None,
                login: login_methods(&request.installation),
                last_usage: None,
                next_uuid: 1,
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

/// `initialize` then `get_binary_version`, both over the control channel.
async fn handshake(wire: &mut Wire, request: &ConnectRequest) -> Result<DriverInfo, AgentError> {
    let init = wire
        .roundtrip(json!({ "subtype": "initialize", "hooks": {} }))
        .await
        .map_err(WireError::into_error)?;
    let version = wire
        .roundtrip(json!({ "subtype": "get_binary_version" }))
        .await
        .ok()
        .and_then(|v| v["version"].as_str().map(str::to_owned));
    Ok(driver_info(&init, version, request))
}

/// Runnable login methods from the catalog, for mid-session auth loss.
fn login_methods(installation: &crate::agent::AgentInstallation) -> Vec<LoginMethod> {
    crate::catalog::PROFILES
        .iter()
        .find(|p| p.id == installation.id.as_str())
        .map(|p| crate::discovery::login_methods(p, &installation.executable))
        .unwrap_or_default()
}

/// Adds the child's stderr to a handshake failure (a logged-out CLI prints
/// its complaint there and closes the wire).
fn with_stderr(error: AgentError, child: &process::Child) -> AgentError {
    let stderr = child.stderr_tail();
    match (error, stderr.is_empty()) {
        (AgentError::ProtocolFailed(message), false) => {
            AgentError::ProtocolFailed(format!("{message}: {stderr}"))
        }
        (error, _) => error,
    }
}

/// A creation-time `configure` value by id, when it was given as text.
fn creation_option(request: &ConnectRequest, id: &str) -> Option<String> {
    request.options.configure.iter().find_map(|(i, v)| match v {
        ConfigValue::Text(text) if i.as_str() == id => Some(text.clone()),
        _ => None,
    })
}

/// The current model's effort levels as a creation-only option; `None` when
/// the catalog has no levels for it.
fn effort_option(models: &Value, model: &str, configured: Option<String>) -> Option<ConfigOption> {
    let entries = models.as_array()?;
    let entry = entries
        .iter()
        .find(|m| m["value"].as_str() == Some(model))
        .or_else(|| entries.first())?;
    let choices: Vec<ConfigChoice> = entry["supportedEffortLevels"]
        .as_array()?
        .iter()
        .filter_map(|level| level.as_str())
        .map(|level| ConfigChoice {
            value: level.to_owned(),
            label: level.to_owned(),
            description: None,
        })
        .collect();
    (!choices.is_empty()).then(|| ConfigOption {
        id: ConfigId::new("effort"),
        name: "Reasoning effort".into(),
        kind: ConfigKind::Select { choices },
        // `None`: the CLI keeps its own default and never reports it.
        current: configured.map(ConfigValue::Text),
        live: false,
    })
}

/// The `initialize` model catalog as config choices.
fn model_choices(models: &Value) -> Vec<ConfigChoice> {
    models
        .as_array()
        .into_iter()
        .flatten()
        .map(|m| ConfigChoice {
            value: m["value"].as_str().unwrap_or_default().to_owned(),
            label: m["displayName"].as_str().unwrap_or_default().to_owned(),
            description: m["description"].as_str().map(str::to_owned),
        })
        .collect()
}

/// What the `initialize` response tells us, folded into the engine vocabulary.
fn driver_info(init: &Value, version: Option<String>, request: &ConnectRequest) -> DriverInfo {
    let auth = match init.get("account").filter(|a| a.is_object()) {
        Some(account) => AuthStatus::Authenticated {
            kind: if account["subscriptionType"].is_string() {
                AuthKind::Subscription
            } else {
                AuthKind::ApiKey
            },
            account: Some(AccountInfo {
                email: account["email"].as_str().map(str::to_owned),
                plan: account["subscriptionType"].as_str().map(str::to_owned),
            }),
        },
        None => request
            .installation
            .auth
            .clone()
            .unwrap_or(AuthStatus::Unknown),
    };
    let commands = init["commands"]
        .as_array()
        .map(|commands| {
            commands
                .iter()
                .map(|c| SlashCommand {
                    name: c["name"].as_str().unwrap_or_default().to_owned(),
                    description: c["description"].as_str().unwrap_or_default().to_owned(),
                    input_hint: c["argumentHint"]
                        .as_str()
                        .filter(|h| !h.is_empty())
                        .map(str::to_owned),
                })
                .collect()
        })
        .unwrap_or_default();
    // The CLI's fixed permission modes, current from `initialize`.
    let mode = init["current_permission_mode"]
        .as_str()
        .unwrap_or("default")
        .to_owned();
    let mode_option = ConfigOption {
        id: ConfigId::new("mode"),
        name: "Permission mode".into(),
        kind: ConfigKind::Select {
            choices: ["default", "acceptEdits", "plan", "bypassPermissions"]
                .map(|value| ConfigChoice {
                    value: value.into(),
                    label: value.into(),
                    description: None,
                })
                .to_vec(),
        },
        current: Some(ConfigValue::Text(mode.clone())),
        live: true,
    };
    // The CLI's model catalog from `initialize`; the current value is what
    // `--model` set at launch, else the CLI's "default" alias.
    let model = creation_option(request, "model").unwrap_or_else(|| "default".into());
    let model_option = ConfigOption {
        id: ConfigId::new("model"),
        name: "Model".into(),
        kind: ConfigKind::Select {
            choices: model_choices(&init["models"]),
        },
        current: Some(ConfigValue::Text(model.clone())),
        live: true,
    };
    // Effort is creation-only (`--effort`): the wire has no live switch — the
    // `/effort` command runs as its own synthetic turn (probed 2026-08-24).
    // A live change is a reopen with the resume token.
    let effort_option = effort_option(&init["models"], &model, creation_option(request, "effort"));
    let mut configuration = SessionConfiguration::default();
    configuration
        .options
        .insert(ConfigId::new("mode"), ConfigValue::Text(mode));
    configuration
        .options
        .insert(ConfigId::new("model"), ConfigValue::Text(model));
    if let Some(current) = effort_option.as_ref().and_then(|o| o.current.clone()) {
        configuration
            .options
            .insert(ConfigId::new("effort"), current);
    }
    DriverInfo {
        details: AgentDetails {
            version,
            auth,
            capabilities: {
                // No `Steer`: the CLI queues mid-turn user messages as their
                // own turns, so the engine's queue handles them instead.
                let mut capabilities = Capabilities::new([
                    Capability::Images,
                    Capability::Permissions,
                    Capability::Questions,
                    Capability::Subagents,
                    Capability::ContextUsage,
                    Capability::SlashCommands,
                    Capability::Resume,
                ]);
                capabilities.mcp_transports =
                    vec![McpTransport::Stdio, McpTransport::Http, McpTransport::Sse];
                capabilities
            },
            config_options: [Some(mode_option), Some(model_option), effort_option]
                .into_iter()
                .flatten()
                .collect(),
            commands,
        },
        configuration,
        // Resuming keeps the token; a new session's arrives with `system/init`.
        resume_token: request.options.resume.clone(),
        title: None,
        // Every turn shape ends with its own `result` frame.
        deterministic_turn_end: true,
        deterministic_agent_turn_end: true,
    }
}

// ---------------------------------------------------------------------------
// Drive task: engine commands out, wire frames in
// ---------------------------------------------------------------------------

/// A `can_use_tool` request waiting for `answer`: what we need to build the
/// control response.
struct PendingRequest {
    wire_id: String,
    /// Original tool input, echoed back as `updatedInput`.
    input: Value,
    /// `permission_suggestions`, applied on `AllowAlways`.
    suggestions: Value,
    /// Present when the request is an `AskUserQuestion`.
    questions: Option<Vec<Question>>,
}

struct Drive {
    wire: Wire,
    child: process::Child,
    events: mpsc::Sender<DriverEvent>,
    /// Current advertised state; mutated and re-sent as `InfoChanged`.
    info: DriverInfo,
    /// Tool snapshots by `tool_use_id`, completed by `tool_result` frames.
    tools: HashMap<String, ToolUpdate>,
    requests: HashMap<RequestId, PendingRequest>,
    /// Streaming message id per transcript (main turn and each subagent).
    messages: HashMap<Option<String>, MessageId>,
    /// An in-flight configure: wire id plus the selection to apply on success.
    config: Option<(String, ConfigId, ConfigValue)>,
    /// Context occupancy of the latest assistant message.
    last_usage: Option<u64>,
    /// The running turn's user-message uuid. An interrupt that lands before
    /// the CLI starts the turn cancels the message out of its queue and never
    /// sends a `result`; the interrupt receipt names this uuid instead.
    turn_uuid: Option<String>,
    next_uuid: u64,
    /// Runnable login methods from the catalog, for mid-session auth loss.
    login: Vec<LoginMethod>,
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
            DriverCommand::StartTurn { input } => {
                self.turn_uuid = Some(self.send_user(&input).await?);
            }
            // The CLI queues mid-turn user messages as their own turns
            // (`command_lifecycle`: queued, then started after the running
            // turn completes) — that is queueing, not steering, so the
            // adapter does not advertise `Steer` and refuses the fallback.
            DriverCommand::Steer { .. } => {
                self.emit(DriverEvent::Steered(false)).await?;
            }
            DriverCommand::Answer { request, answer } => self.answer(request, answer).await?,
            DriverCommand::Cancel => {
                // `cancel_queued` clears anything the CLI parked as a queued
                // message; then unblock any waiting permission.
                self.wire
                    .control(json!({ "subtype": "interrupt", "cancel_queued": true }))
                    .await?;
                for (_, pending) in std::mem::take(&mut self.requests) {
                    self.wire
                        .respond(
                            &pending.wire_id,
                            json!({ "behavior": "deny", "message": "cancelled" }),
                        )
                        .await?;
                }
            }
            DriverCommand::Configure(ConfigSelection::Option { id, value }) => {
                let request = match (id.as_str(), &value) {
                    ("mode", ConfigValue::Text(mode)) => {
                        json!({ "subtype": "set_permission_mode", "mode": mode })
                    }
                    ("model", ConfigValue::Text(model)) => {
                        json!({ "subtype": "set_model", "model": model })
                    }
                    _ => return Ok(()),
                };
                let wire_id = self.wire.control(request).await?;
                self.config = Some((wire_id, id, value));
            }
            DriverCommand::Rollback(turns) => {
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    format!("rollback({turns}) is not supported by the Claude adapter yet"),
                )
                .await?;
            }
            DriverCommand::Close => unreachable!("handled in run"),
        }
        Ok(())
    }

    /// Routes one wire frame by its `type`.
    async fn handle_frame(&mut self, frame: Value) -> Result<(), Gone> {
        match frame["type"].as_str().unwrap_or_default() {
            "stream_event" => self.on_stream(&frame).await,
            "assistant" => self.on_assistant(&frame).await,
            "user" => self.on_user(&frame).await,
            "control_request" => self.on_control_request(frame).await,
            "control_response" => self.on_control_response(&frame).await,
            "result" => self.on_result(&frame).await,
            "system" => self.on_system(&frame).await,
            // Plan usage lands in P2; per-turn rate pushes are dropped for
            // now. Lifecycle frames only narrate the CLI's own queue.
            "rate_limit_event" | "control_cancel_request" | "command_lifecycle" => Ok(()),
            other => {
                let mut extensions = Extensions::new();
                extensions.insert("claude/raw_frame".into(), frame.clone());
                self.emit(DriverEvent::Event {
                    kind: EventKind::Diagnostic(Diagnostic {
                        level: DiagnosticLevel::Info,
                        message: format!("unrecognized claude frame `{other}`"),
                    }),
                    parent_tool_id: None,
                    extensions,
                })
                .await
            }
        }
    }

    /// Streaming deltas: text, thinking, message boundaries.
    async fn on_stream(&mut self, frame: &Value) -> Result<(), Gone> {
        let parent = parent_of(frame);
        let event = &frame["event"];
        let kind = match event["type"].as_str().unwrap_or_default() {
            "message_start" => {
                let id = event["message"]["id"].as_str().unwrap_or("m0");
                self.messages.insert(parent.clone(), MessageId::new(id));
                return Ok(());
            }
            "content_block_delta" => {
                let delta = &event["delta"];
                let message_id = self.message_id(&parent);
                match delta["type"].as_str().unwrap_or_default() {
                    "text_delta" => {
                        text(&delta["text"]).map(|text| EventKind::TextDelta { message_id, text })
                    }
                    "thinking_delta" => text(&delta["thinking"])
                        .map(|text| EventKind::ReasoningDelta { message_id, text }),
                    _ => None,
                }
            }
            "message_stop" => {
                let message_id = self.message_id(&parent);
                self.messages.remove(&parent);
                Some(EventKind::MessageEnded { message_id })
            }
            _ => None,
        };
        if let Some(kind) = kind {
            self.emit_content(kind, parent).await?;
        }
        Ok(())
    }

    /// Complete assistant frames carry finished `tool_use` blocks. Text and
    /// thinking already streamed; the message's usage tracks context occupancy.
    async fn on_assistant(&mut self, frame: &Value) -> Result<(), Gone> {
        // A synthetic API-error message with the typed auth marker means the
        // credentials died; the engine fails the turn and closes the session.
        if frame["error"].as_str() == Some("authentication_failed") && !self.login.is_empty() {
            return self
                .emit(DriverEvent::AuthLost {
                    login: self.login.clone(),
                })
                .await;
        }
        let parent = parent_of(frame);
        if let Some(used) = context_tokens(&frame["message"]["usage"]) {
            self.last_usage = Some(used);
        }
        let blocks = frame["message"]["content"].as_array().cloned();
        for block in blocks.unwrap_or_default() {
            if block["type"].as_str() != Some("tool_use") {
                continue;
            }
            let name = block["name"].as_str().unwrap_or_default();
            let kind = match name {
                // The agent's task list is a plan, not a tool call.
                "TodoWrite" => EventKind::PlanUpdated {
                    entries: plan_entries(&block["input"]["todos"]),
                },
                // Questions surface through `can_use_tool`, not as a tool.
                "AskUserQuestion" => continue,
                _ => {
                    let tool = fresh_tool(&block);
                    self.tools.insert(tool.id.as_str().to_owned(), tool.clone());
                    EventKind::ToolUpdated(tool)
                }
            };
            self.emit_content(kind, parent.clone()).await?;
        }
        Ok(())
    }

    /// User frames: tool results for our tracked tools, and user-role content
    /// a parent agent injected into a subagent.
    async fn on_user(&mut self, frame: &Value) -> Result<(), Gone> {
        let parent = parent_of(frame);
        let content = &frame["message"]["content"];
        if let Some(blocks) = content.as_array() {
            for block in blocks {
                if block["type"].as_str() != Some("tool_result") {
                    continue;
                }
                let id = block["tool_use_id"].as_str().unwrap_or_default();
                let Some(mut tool) = self.tools.get(id).cloned() else {
                    continue;
                };
                complete_tool(&mut tool, block, &frame["tool_use_result"]);
                self.tools.insert(id.to_owned(), tool.clone());
                self.emit_content(EventKind::ToolUpdated(tool), parent.clone())
                    .await?;
            }
            return Ok(());
        }
        // Plain text with a parent is the parent steering its subagent; our
        // own prompts also come back as replays (no parent) and are dropped.
        if let (Some(text), Some(_)) = (content.as_str(), &parent) {
            let message_id = MessageId::new(frame["uuid"].as_str().unwrap_or("m0"));
            self.emit_content(
                EventKind::UserMessage {
                    message_id,
                    text: text.to_owned(),
                },
                parent,
            )
            .await?;
        }
        Ok(())
    }

    /// `can_use_tool` becomes a permission or a question; anything else is
    /// declined so the CLI does not hang on us.
    async fn on_control_request(&mut self, frame: Value) -> Result<(), Gone> {
        let wire_id = frame["request_id"].as_str().unwrap_or_default().to_owned();
        let request = &frame["request"];
        let subtype = request["subtype"].as_str().unwrap_or_default();
        if subtype != "can_use_tool" {
            self.wire
                .respond_error(&wire_id, &format!("unsupported request: {subtype}"))
                .await?;
            return self
                .diagnostic(
                    DiagnosticLevel::Warning,
                    format!("declined agent control request {subtype}"),
                )
                .await;
        }
        let id = RequestId::new(format!("r{wire_id}"));
        let input = request["input"].clone();
        let questions = (request["tool_name"].as_str() == Some("AskUserQuestion"))
            .then(|| questions(&input["questions"]));
        let open = match &questions {
            Some(questions) => Request::Question(QuestionRequest {
                id: id.clone(),
                questions: questions.clone(),
            }),
            None => Request::Permission(PermissionRequest {
                id: id.clone(),
                tool: self.tool_for(request),
                options: permission_options(&request["permission_suggestions"]),
                detail: request["description"].as_str().map(str::to_owned),
            }),
        };
        self.requests.insert(
            id,
            PendingRequest {
                wire_id,
                input,
                suggestions: request["permission_suggestions"].clone(),
                questions,
            },
        );
        self.emit(DriverEvent::event(EventKind::RequestOpened(open)))
            .await
    }

    /// Control receipts: an interrupt receipt that cancelled the turn's
    /// still-queued prompt ends the turn (no `result` will come); a configure
    /// receipt confirms or rejects the pending mode change.
    async fn on_control_response(&mut self, frame: &Value) -> Result<(), Gone> {
        let response = &frame["response"];
        let cancelled_queued = response["response"]["cancelled"]
            .as_array()
            .zip(self.turn_uuid.as_deref())
            .is_some_and(|(cancelled, uuid)| cancelled.iter().any(|c| c.as_str() == Some(uuid)));
        if cancelled_queued {
            self.turn_uuid = None;
            return self
                .emit(DriverEvent::TurnEnded(StopReason::Cancelled))
                .await;
        }
        let for_config = self
            .config
            .as_ref()
            .is_some_and(|(id, _, _)| response["request_id"].as_str() == Some(id));
        if for_config {
            let (_, config_id, value) = self.config.take().expect("checked");
            if response["subtype"].as_str() == Some("error") {
                let message = response["error"].as_str().unwrap_or("rejected");
                return self
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        format!("agent rejected configure `{config_id}`: {message}"),
                    )
                    .await;
            }
            if crate::adapter::apply_selection(&mut self.info, &config_id, &value) {
                return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
            }
        }
        Ok(())
    }

    /// Exactly one `result` per turn.
    async fn on_result(&mut self, frame: &Value) -> Result<(), Gone> {
        self.turn_uuid = None;
        // Backgrounded tools stay tracked; `task_notification` finishes them.
        self.tools.retain(|_, tool| tool.status.is_active());
        if let Some(used) = self.last_usage.take() {
            self.emit(DriverEvent::event(EventKind::ContextUsage {
                used_tokens: used,
                window_tokens: context_window(&frame["modelUsage"]),
                cost_usd: frame["total_cost_usd"].as_f64(),
            }))
            .await?;
        }
        self.emit(DriverEvent::TurnEnded(stop_reason(frame))).await
    }

    /// `system/init` carries the provider session id (= resume token).
    async fn on_system(&mut self, frame: &Value) -> Result<(), Gone> {
        match frame["subtype"].as_str().unwrap_or_default() {
            "init" => {
                let token = frame["session_id"].as_str().map(ResumeToken::new);
                if token != self.info.resume_token {
                    self.info.resume_token = token;
                    return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
                }
                Ok(())
            }
            "status" if frame["status"].as_str() == Some("compacting") => {
                self.emit(DriverEvent::event(EventKind::ContextCompacted))
                    .await
            }
            // A background task finished: complete the tool it ran under.
            "task_notification" => {
                let Some(id) = frame["tool_use_id"].as_str() else {
                    return Ok(());
                };
                let Some(mut tool) = self.tools.remove(id) else {
                    return Ok(());
                };
                tool.status = if frame["status"].as_str() == Some("failed") {
                    ToolStatus::Failed
                } else {
                    ToolStatus::Completed
                };
                self.emit(DriverEvent::event(EventKind::ToolUpdated(tool)))
                    .await
            }
            // Hooks, task bookkeeping, statuses: nothing the engine needs.
            _ => Ok(()),
        }
    }

    /// Answers one stored `can_use_tool` request on the control channel.
    async fn answer(&mut self, request: RequestId, answer: Answer) -> Result<(), Gone> {
        let Some(pending) = self.requests.remove(&request) else {
            return Ok(());
        };
        let response = match (&pending.questions, answer) {
            (None, Answer::Permission(choice)) => permission_response(&pending, choice),
            (Some(questions), Answer::Question(answers)) => {
                question_response(&pending, questions, &answers)
            }
            _ => json!({ "behavior": "deny", "message": "unsupported answer" }),
        };
        self.wire.respond(&pending.wire_id, response).await?;
        Ok(())
    }

    /// The tool a permission request is about: the tracked snapshot, or a
    /// fresh one built from the request itself.
    fn tool_for(&self, request: &Value) -> ToolUpdate {
        let id = request["tool_use_id"].as_str().unwrap_or_default();
        self.tools.get(id).cloned().unwrap_or_else(|| {
            fresh_tool(&json!({
                "id": id,
                "name": request["tool_name"],
                "input": request["input"],
            }))
        })
    }

    /// Writes one user message and returns its uuid. Attachments become
    /// path refs in the text; images additionally inline as content blocks.
    async fn send_user(&mut self, input: &Input) -> Result<String, Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.as_deref()) {
            self.diagnostic(DiagnosticLevel::Warning, problem.to_owned())
                .await?;
        }
        let content = if loaded.is_empty() {
            json!(input.as_text())
        } else {
            let mut blocks: Vec<Value> = loaded
                .iter()
                .filter_map(|l| l.image.as_ref())
                .map(|image| {
                    json!({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": image.mime,
                            "data": image.base64,
                        },
                    })
                })
                .collect();
            blocks.push(json!({
                "type": "text",
                "text": attach::with_refs(input.as_text(), &loaded),
            }));
            json!(blocks)
        };
        let uuid = mint_uuid(self.next_uuid);
        self.next_uuid += 1;
        self.wire
            .write(json!({
                "type": "user",
                "uuid": uuid,
                "message": { "role": "user", "content": content },
                "parent_tool_use_id": null,
            }))
            .await?;
        Ok(uuid)
    }

    fn message_id(&self, parent: &Option<String>) -> MessageId {
        self.messages
            .get(parent)
            .cloned()
            .unwrap_or_else(|| MessageId::new("m0"))
    }

    /// The agent went away: report how it died before the stream closes.
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
        self.emit(DriverEvent::event(EventKind::Diagnostic(Diagnostic {
            level,
            message: message.into(),
        })))
        .await
    }

    async fn emit_content(&mut self, kind: EventKind, parent: Option<String>) -> Result<(), Gone> {
        self.emit(DriverEvent::Event {
            kind,
            parent_tool_id: parent.map(ToolId::new),
            extensions: Extensions::new(),
        })
        .await
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
// Translation helpers
// ---------------------------------------------------------------------------

/// Declared MCP servers as a `--mcp-config` inline JSON value. The CLI takes
/// every transport, so nothing is refused.
fn mcp_config(servers: &[McpServer]) -> Value {
    let mut entries = serde_json::Map::new();
    for server in servers {
        let entry = match &server.connection {
            McpConnection::Stdio { command, args, env } => {
                json!({ "command": command, "args": args, "env": env })
            }
            McpConnection::Http { url, headers } => {
                json!({ "type": "http", "url": url, "headers": headers })
            }
            McpConnection::Sse { url, headers } => {
                json!({ "type": "sse", "url": url, "headers": headers })
            }
        };
        entries.insert(server.name.clone(), entry);
    }
    json!({ "mcpServers": entries })
}

fn parent_of(frame: &Value) -> Option<String> {
    frame["parent_tool_use_id"].as_str().map(str::to_owned)
}

fn text(value: &Value) -> Option<String> {
    value.as_str().filter(|t| !t.is_empty()).map(str::to_owned)
}

/// A `tool_use` block becomes a running tool snapshot.
fn fresh_tool(block: &Value) -> ToolUpdate {
    let name = block["name"].as_str().unwrap_or_default();
    let input = &block["input"];
    let (kind, tool_input) = decode_tool(name, input);
    ToolUpdate {
        id: ToolId::new(block["id"].as_str().unwrap_or_default()),
        kind,
        title: title(name, input),
        status: ToolStatus::Running,
        input: tool_input,
        output: None,
        diffs: Vec::new(),
        locations: Vec::new(),
        raw: Some(RawTool {
            name: name.to_owned(),
            input: input.clone(),
        }),
    }
}

/// Claude tool names to the portable kind and decoded input.
fn decode_tool(name: &str, input: &Value) -> (ToolKind, ToolInput) {
    let path = || ToolInput::Path(input["file_path"].as_str().unwrap_or_default().into());
    let pattern = || ToolInput::Pattern(input["pattern"].as_str().unwrap_or_default().into());
    match name {
        "Bash" => (
            ToolKind::Execute,
            ToolInput::Command {
                command: input["command"].as_str().unwrap_or_default().to_owned(),
                cwd: None,
            },
        ),
        "Read" | "NotebookRead" => (ToolKind::Read, path()),
        "Edit" | "Write" | "NotebookEdit" => (ToolKind::Edit, path()),
        "Glob" | "Grep" => (ToolKind::Search, pattern()),
        "WebFetch" => (
            ToolKind::Fetch,
            ToolInput::Url(input["url"].as_str().unwrap_or_default().to_owned()),
        ),
        "WebSearch" => (
            ToolKind::Search,
            ToolInput::Query(input["query"].as_str().unwrap_or_default().to_owned()),
        ),
        "Task" | "Agent" => (
            ToolKind::Subagent,
            ToolInput::Text(input["description"].as_str().unwrap_or_default().to_owned()),
        ),
        mcp if mcp.starts_with("mcp__") => {
            let mut parts = mcp.splitn(3, "__").skip(1);
            (
                ToolKind::Mcp {
                    server: parts.next().unwrap_or_default().to_owned(),
                    tool: parts.next().unwrap_or_default().to_owned(),
                },
                ToolInput::None,
            )
        }
        _ => (ToolKind::Other, ToolInput::None),
    }
}

/// Human title: the tool name plus its most telling argument.
fn title(name: &str, input: &Value) -> String {
    let detail = [
        "description",
        "file_path",
        "command",
        "pattern",
        "url",
        "query",
    ]
    .iter()
    .find_map(|key| input[*key].as_str())
    .unwrap_or_default();
    match detail {
        "" => name.to_owned(),
        detail => format!("{name} {detail}"),
    }
}

/// Applies a `tool_result` block and its typed `tool_use_result`. A result
/// carrying a `backgroundTaskId` means the tool keeps running in the
/// background; `task_notification` finishes it later.
fn complete_tool(tool: &mut ToolUpdate, block: &Value, typed: &Value) {
    tool.status = if typed["backgroundTaskId"].is_string() {
        ToolStatus::Running
    } else if block["is_error"].as_bool().unwrap_or(false) {
        ToolStatus::Failed
    } else {
        ToolStatus::Completed
    };
    tool.output = tool_output(block, typed).map(|o| cap(o, OUTPUT_CAP));
    if let Some(path) = typed["filePath"].as_str() {
        tool.locations = vec![path.into()];
        if let Some(diff) = file_diff(path, typed) {
            tool.diffs = vec![diff];
        }
    }
}

/// Output text: Bash's stdout/stderr when typed, else the block content.
fn tool_output(block: &Value, typed: &Value) -> Option<String> {
    if typed["stdout"].is_string() || typed["stderr"].is_string() {
        let out = [&typed["stdout"], &typed["stderr"]]
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        return (!out.is_empty()).then_some(out);
    }
    let content = &block["content"];
    if let Some(text) = content.as_str() {
        return Some(text.to_owned());
    }
    let joined = content
        .as_array()?
        .iter()
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("");
    (!joined.is_empty()).then_some(joined)
}

/// Write/Edit results carry the change: old/new strings, or the whole file.
fn file_diff(path: &str, typed: &Value) -> Option<FileDiff> {
    if let Some(new_text) = typed["newString"].as_str() {
        return Some(FileDiff {
            path: path.into(),
            old_text: typed["oldString"].as_str().map(str::to_owned),
            new_text: new_text.to_owned(),
        });
    }
    typed["content"].as_str().map(|content| FileDiff {
        path: path.into(),
        old_text: typed["originalFile"].as_str().map(str::to_owned),
        new_text: content.to_owned(),
    })
}

fn cap(mut s: String, at: usize) -> String {
    if s.len() > at {
        let mut end = at;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

fn plan_entries(todos: &Value) -> Vec<PlanEntry> {
    todos
        .as_array()
        .map(|todos| {
            todos
                .iter()
                .map(|todo| PlanEntry {
                    text: todo["content"].as_str().unwrap_or_default().to_owned(),
                    status: match todo["status"].as_str().unwrap_or_default() {
                        "in_progress" => PlanStatus::InProgress,
                        "completed" => PlanStatus::Completed,
                        _ => PlanStatus::Pending,
                    },
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Claude offers allow/deny; `AllowAlways` exists when the CLI suggested a
/// persistent rule to apply.
fn permission_options(suggestions: &Value) -> Vec<PermissionChoice> {
    let mut options = vec![PermissionChoice::AllowOnce, PermissionChoice::DenyOnce];
    if suggestions.as_array().is_some_and(|s| !s.is_empty()) {
        options.insert(1, PermissionChoice::AllowAlways);
    }
    options
}

fn permission_response(pending: &PendingRequest, choice: PermissionChoice) -> Value {
    match choice {
        PermissionChoice::AllowOnce => {
            json!({ "behavior": "allow", "updatedInput": pending.input })
        }
        PermissionChoice::AllowAlways => json!({
            "behavior": "allow",
            "updatedInput": pending.input,
            "updatedPermissions": pending.suggestions,
        }),
        _ => json!({ "behavior": "deny", "message": "User denied this action" }),
    }
}

/// `AskUserQuestion`'s `input.questions` to the portable shape. Choice ids
/// are the labels: that is what the answer echoes back.
fn questions(input: &Value) -> Vec<Question> {
    input
        .as_array()
        .map(|questions| {
            questions
                .iter()
                .enumerate()
                .map(|(i, q)| Question {
                    id: QuestionId::new(format!("q{i}")),
                    text: q["question"].as_str().unwrap_or_default().to_owned(),
                    header: q["header"].as_str().map(str::to_owned),
                    choices: q["options"]
                        .as_array()
                        .map(|options| {
                            options
                                .iter()
                                .map(|o| Choice {
                                    id: ChoiceId::new(o["label"].as_str().unwrap_or_default()),
                                    label: o["label"].as_str().unwrap_or_default().to_owned(),
                                    description: o["description"].as_str().map(str::to_owned),
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    multi_select: q["multiSelect"].as_bool().unwrap_or(false),
                    allows_free_text: false,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Answers keyed by question text, as the wire expects.
fn question_response(
    pending: &PendingRequest,
    questions: &[Question],
    answers: &[QuestionAnswer],
) -> Value {
    let mut map = serde_json::Map::new();
    for (question, answer) in questions.iter().zip(answers) {
        let value = match answer {
            QuestionAnswer::Choices(ids) => ids
                .iter()
                .map(|id| id.as_str().to_owned())
                .collect::<Vec<_>>()
                .join(", "),
            QuestionAnswer::Text(text) => text.clone(),
        };
        map.insert(question.text.clone(), Value::String(value));
    }
    let mut updated = pending.input.clone();
    updated["answers"] = Value::Object(map);
    json!({ "behavior": "allow", "updatedInput": updated })
}

/// Context occupancy of one assistant message.
fn context_tokens(usage: &Value) -> Option<u64> {
    if !usage.is_object() {
        return None;
    }
    let sum = [
        "input_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "output_tokens",
    ]
    .iter()
    .filter_map(|k| usage[*k].as_u64())
    .sum();
    Some(sum)
}

fn context_window(model_usage: &Value) -> Option<u64> {
    model_usage
        .as_object()?
        .values()
        .filter_map(|m| m["contextWindow"].as_u64())
        .max()
}

/// A v4-shaped uuid, clock-seeded so it never repeats across resumes of the
/// same conversation: the CLI dedupes user messages by uuid, and a bare
/// per-session counter silently swallowed the first resumed prompt.
fn mint_uuid(seq: u64) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!(
        "{:08x}-{:04x}-4{:03x}-8000-{:012x}",
        (nanos >> 32) as u32,
        (nanos >> 16) as u16,
        nanos & 0xfff,
        seq
    )
}

fn stop_reason(frame: &Value) -> StopReason {
    // An API-error result still says `subtype: "success"`; `is_error` is
    // the real verdict (probed live 2026-08-23 with no stored login).
    let is_error = frame["is_error"].as_bool().unwrap_or(false);
    if !is_error && frame["subtype"].as_str() == Some("success") {
        return StopReason::Completed {
            source: CompletionSource::Protocol,
        };
    }
    if frame["terminal_reason"].as_str() == Some("aborted_streaming") {
        return StopReason::Cancelled;
    }
    let message = frame["result"]
        .as_str()
        .or(frame["subtype"].as_str())
        .unwrap_or("turn failed")
        .to_owned();
    StopReason::Failed { message }
}

// ---------------------------------------------------------------------------
// Wire: line-delimited stream-json over the child's stdio
// ---------------------------------------------------------------------------

struct Wire {
    stdin: tokio::process::ChildStdin,
    /// All frames the reader task saw, bounded; pipe backpressure beyond.
    frames: mpsc::Receiver<Value>,
    next_id: u64,
}

impl Wire {
    /// Takes the child's stdio and starts the line-reader task.
    fn over(child: &mut process::Child) -> Self {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, frames) = mpsc::channel(FRAME_BUFFER);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if tx.send(frame).await.is_err() {
                    break;
                }
            }
        });
        Self {
            stdin,
            frames,
            next_id: 1,
        }
    }

    /// Sends a control request and returns its id.
    async fn control(&mut self, request: Value) -> std::io::Result<String> {
        let id = format!("c{}", self.next_id);
        self.next_id += 1;
        self.write(json!({ "type": "control_request", "request_id": id, "request": request }))
            .await?;
        Ok(id)
    }

    /// Answers one of the CLI's control requests.
    async fn respond(&mut self, request_id: &str, response: Value) -> std::io::Result<()> {
        self.write(json!({
            "type": "control_response",
            "response": { "subtype": "success", "request_id": request_id, "response": response },
        }))
        .await
    }

    async fn respond_error(&mut self, request_id: &str, message: &str) -> std::io::Result<()> {
        self.write(json!({
            "type": "control_response",
            "response": { "subtype": "error", "request_id": request_id, "error": message },
        }))
        .await
    }

    /// Handshake only: sends a control request and blocks on its response,
    /// skipping unrelated startup frames.
    async fn roundtrip(&mut self, request: Value) -> Result<Value, WireError> {
        let id = self.control(request).await.map_err(|_| WireError::Closed)?;
        loop {
            let frame = self.frames.recv().await.ok_or(WireError::Closed)?;
            if frame["type"].as_str() != Some("control_response") {
                continue;
            }
            let response = &frame["response"];
            if response["request_id"].as_str() != Some(&id) {
                continue;
            }
            if response["subtype"].as_str() == Some("error") {
                return Err(WireError::Control(
                    response["error"].as_str().unwrap_or("error").to_owned(),
                ));
            }
            return Ok(response["response"].clone());
        }
    }

    async fn write(&mut self, frame: Value) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut line = frame.to_string();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await
    }
}

enum WireError {
    Closed,
    Control(String),
}

impl WireError {
    fn into_error(self) -> AgentError {
        match self {
            WireError::Closed => AgentError::ProtocolFailed("agent closed the wire".into()),
            WireError::Control(message) => AgentError::ProtocolFailed(message),
        }
    }
}
