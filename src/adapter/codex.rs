//! Native Codex adapter: drives `codex app-server` over line-delimited
//! JSON-RPC 2.0 (validated 2026-08-27, ticket 10). Turn end is deterministic:
//! exactly one `turn/completed` per turn. The engine owns all turn rules.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
    WireRecorder, attach, cap, login_methods, with_stderr,
};
use crate::agent::{
    AccountInfo, AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigChoice,
    ConfigId, ConfigKind, ConfigOption, ConfigValue, Input, ResumeToken, SessionConfiguration,
    SessionStart, SlashCommand,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Choice, ChoiceId, CompletionSource, Diagnostic, DiagnosticLevel, EventKind, Extensions,
    FileDiff, MessageId, PermissionChoice, PermissionRequest, PlanEntry, PlanStatus, PlanUsage,
    Question, QuestionAnswer, QuestionId, QuestionRequest, RawTool, Request, RequestId, StopReason,
    ToolId, ToolInput, ToolKind, ToolStatus, ToolUpdate, UsageWindow,
};
use crate::process::{self, Spawn};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_GRACE: Duration = Duration::from_secs(2);
const FRAME_BUFFER: usize = 64;
const OUTPUT_CAP: usize = 16 * 1024;

/// Prefix on every `clientUserMessageId` we mint; echoed user-message items
/// carrying it are our own prompts and steers, and are dropped.
const CLIENT_MSG_PREFIX: &str = "anyagent-m";

/// `approvalPolicy` values observed live ("on-request" is the default the
/// wire reports; the others were probed).
const MODES: [&str; 3] = ["untrusted", "on-request", "never"];
/// `sandbox` values, matching the `permissionProfile/list` ids.
const SANDBOXES: [&str; 3] = ["read-only", "workspace-write", "danger-full-access"];

/// Launches `codex app-server`; one instance serves every session.
pub(crate) struct CodexAdapter;

impl CodexAdapter {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Adapter for CodexAdapter {
    /// Spawns the server, handshakes, binds the thread, and hands the live
    /// wire to the drive task.
    async fn connect(&self, request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        if !request.options.mcp_servers.is_empty() {
            // Codex reads MCP servers from its own config.toml; the wire has
            // no per-session declaration.
            return Err(AgentError::UnsupportedFeature("MCP forwarding".into()));
        }
        let (ev_tx, ev_rx) = mpsc::channel(FRAME_BUFFER);
        let recorder = WireRecorder::for_session(&request.options, &ev_tx).await;
        let (child, wire, info, models, thread_id) = launch(&request, recorder).await?;
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        tokio::spawn(
            Drive {
                wire,
                child,
                events: ev_tx,
                info: info.clone(),
                models,
                thread_id,
                turn: None,
                turn_started: false,
                pending_steer: None,
                cancel_pending: false,
                pending: HashMap::new(),
                tools: HashMap::new(),
                children: HashMap::new(),
                child_tool: None,
                requests: HashMap::new(),
                open_reasoning: std::collections::HashSet::new(),
                auth_lost: false,
                next_msg: 1,
                request,
            }
            .run(cmd_rx),
        );
        Ok(DriverConnection {
            info,
            commands: cmd_tx,
            events: ev_rx,
        })
    }

    /// Quota probe: spawn, `initialize`, `account/rateLimits/read` (~0.4 s),
    /// shut down.
    async fn plan_usage(
        &self,
        installation: &crate::agent::AgentInstallation,
    ) -> Result<PlanUsage, AgentError> {
        let mut child = process::spawn(Spawn {
            exec_path: installation.executable_path.clone(),
            args: vec!["app-server".into()],
            cwd: std::env::temp_dir(),
            env: Vec::new(),
        })
        .await?;
        let mut wire = Wire::over(&mut child, None);
        let fetch = async {
            wire.roundtrip("initialize", json!({ "clientInfo": client_info() }))
                .await?;
            wire.notify("initialized").await?;
            wire.roundtrip("account/rateLimits/read", json!({})).await
        };
        let result = match tokio::time::timeout(HANDSHAKE_TIMEOUT, fetch).await {
            Ok(Ok(response)) => plan_usage(&response["rateLimits"]).ok_or_else(|| {
                AgentError::UnsupportedFeature("no plan quota for this login".into())
            }),
            // Logged out: "codex account authentication required to read rate limits".
            Ok(Err(WireError::Rpc(m))) if m.contains("authentication required") => {
                Err(AgentError::AuthRequired {
                    login: login_methods(installation),
                })
            }
            Ok(Err(e)) => Err(with_stderr(e.into_error(), &child)),
            Err(_) => Err(AgentError::HandshakeTimeout),
        };
        child.shutdown(CLOSE_GRACE).await;
        result
    }
}

fn client_info() -> Value {
    json!({ "name": "anyagent", "version": env!("CARGO_PKG_VERSION") })
}

/// Spawns the server and handshakes within the timeout.
async fn launch(
    request: &ConnectRequest,
    recorder: Option<WireRecorder>,
) -> Result<(process::Child, Wire, DriverInfo, Value, String), AgentError> {
    let env = crate::adapter::config_home_env(&request.installation, &request.options)?;
    // CODEX_HOME must already exist or the server exits at startup
    // (probed 2026-08-27).
    if let Some((_, dir)) = env.first() {
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| AgentError::SpawnFailed(format!("could not create config home: {e}")))?;
    }
    let mut child = process::spawn(Spawn {
        exec_path: request.installation.executable_path.clone(),
        args: vec!["app-server".into()],
        cwd: request.options.cwd().clone(),
        env,
    })
    .await?;
    let mut wire = Wire::over(&mut child, recorder);
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake(&mut wire, request)).await {
        Ok(Ok((info, models, thread_id))) => Ok((child, wire, info, models, thread_id)),
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

/// `initialize` → `initialized`, then account, model catalog, and skills, then
/// the thread bind from `options.start`.
async fn handshake(
    wire: &mut Wire,
    request: &ConnectRequest,
) -> Result<(DriverInfo, Value, String), AgentError> {
    let init = wire
        .roundtrip("initialize", json!({ "clientInfo": client_info() }))
        .await
        .map_err(WireError::into_error)?;
    wire.notify("initialized")
        .await
        .map_err(WireError::into_error)?;
    let account = wire
        .roundtrip("account/read", json!({}))
        .await
        .map_err(WireError::into_error)?;
    let models = wire
        .roundtrip("model/list", json!({}))
        .await
        .map_err(WireError::into_error)?["data"]
        .clone();
    // Skills (the slash commands) are fetched by the drive task after open:
    // the roundtrip costs ~0.5 s (probed, 78 skills) and lands as
    // `SessionUpdated`, like ACP's late command list.
    let commands = Vec::new();
    let config = start_config(request, &models)?;
    let thread = open_thread(wire, request, &config).await?;
    let thread_id = thread["thread"]["id"]
        .as_str()
        .ok_or_else(|| AgentError::ProtocolFailed("thread bind returned no id".into()))?
        .to_owned();
    let info = driver_info(
        &init, &account, &models, &thread, &config, commands, request,
    );
    Ok((info, models, thread_id))
}

/// Skills are codex's slash commands. `data` groups them by root and the same
/// skill appears under every root, so dedupe by name keeping first appearance.
/// A host with no skills, or a build without `skills/list`, reports none.
fn parse_skill_commands(response: &Value) -> Vec<SlashCommand> {
    let mut seen = std::collections::HashSet::new();
    response["data"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|group| group["skills"].as_array().into_iter().flatten())
        .filter_map(|skill| {
            let name = skill["name"]
                .as_str()
                .map(str::trim)
                .filter(|name| !name.is_empty())?;
            // The interface's short description is picker-sized; the
            // top-level one is a model-facing paragraph, so it is a fallback.
            seen.insert(name.to_owned()).then(|| SlashCommand {
                name: name.to_owned(),
                description: skill["interface"]["shortDescription"]
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .or_else(|| skill["description"].as_str())
                    .unwrap_or_default()
                    .to_owned(),
                input_hint: None,
            })
        })
        .collect()
}

/// Creation-time `configure` values, validated. The wire accepts an unknown
/// model at request time and only fails the turn with a provider 400
/// (probed 2026-08-27), so bad values are refused here instead.
#[derive(Default)]
struct StartConfig {
    model: Option<String>,
    effort: Option<String>,
    fast: Option<bool>,
    tier: Option<String>,
    mode: Option<String>,
    sandbox: Option<String>,
}

fn start_config(request: &ConnectRequest, models: &Value) -> Result<StartConfig, AgentError> {
    let mut config = StartConfig::default();
    for (id, value) in &request.options.configure {
        if id.as_str() == "fast" {
            let ConfigValue::Bool(fast) = value else {
                return Err(AgentError::InvalidConfiguration(
                    "`fast` takes a boolean".into(),
                ));
            };
            config.fast = Some(*fast);
            continue;
        }
        let ConfigValue::Text(text) = value else {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{id}` takes a text value"
            )));
        };
        let (slot, valid) = match id.as_str() {
            "model" => (&mut config.model, model_entry(models, text).is_some()),
            "effort" => (&mut config.effort, true), // checked against the model below
            "serviceTier" => (
                &mut config.tier,
                tier_choices(models).iter().any(|c| &c.value == text),
            ),
            "mode" => (&mut config.mode, MODES.contains(&text.as_str())),
            "sandbox" => (&mut config.sandbox, SANDBOXES.contains(&text.as_str())),
            _ => {
                return Err(AgentError::InvalidConfiguration(format!(
                    "`{id}` is not a creation-time option of this agent"
                )));
            }
        };
        if !valid {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{text}` is not a choice for `{id}`"
            )));
        }
        *slot = Some(text.clone());
    }
    if let Some(effort) = &config.effort {
        let model = config.model.clone().or_else(|| default_model(models));
        let supported = model.as_deref().map(|m| effort_choices(models, m));
        if !supported.is_some_and(|choices| choices.iter().any(|c| &c.value == effort)) {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{effort}` is not an effort level of the selected model"
            )));
        }
    }
    if config.fast == Some(true)
        && config
            .model
            .clone()
            .or_else(|| default_model(models))
            .is_none_or(|model| fast_tier(models, &model).is_none())
    {
        return Err(AgentError::InvalidConfiguration(
            "the selected model does not support fast mode".into(),
        ));
    }
    Ok(config)
}

/// Binds the provider thread: start, resume, or fork per `options.start`.
async fn open_thread(
    wire: &mut Wire,
    request: &ConnectRequest,
    config: &StartConfig,
) -> Result<Value, AgentError> {
    let mut params = json!({ "cwd": request.options.cwd() });
    if let Some(mode) = &config.mode {
        params["approvalPolicy"] = json!(mode);
    }
    if let Some(sandbox) = &config.sandbox {
        params["sandbox"] = json!(sandbox);
    }
    let method = match &request.options.start {
        SessionStart::New => "thread/start",
        SessionStart::Resume(token) => {
            params["threadId"] = json!(token.as_str());
            "thread/resume"
        }
        SessionStart::Fork { from, at } => {
            params["threadId"] = json!(from.as_str());
            // The fork anchor is a wire turn id, exposed as the
            // `codex/fork_point` extension on `MessageEnded`.
            if let Some(at) = at {
                params["lastTurnId"] = json!(at.as_str());
            }
            "thread/fork"
        }
    };
    wire.roundtrip(method, params).await.map_err(|e| match e {
        WireError::Rpc(m) if method == "thread/resume" => AgentError::ResumeFailed(m),
        e => e.into_error(),
    })
}

/// Login state from `account/read`, offline and instant. `OPENAI_API_KEY` is
/// ignored by app-server 0.147.0 (probed), so the environment is not consulted.
fn account_status(account: &Value, request: &ConnectRequest) -> AuthStatus {
    match account["account"]["type"].as_str() {
        Some("chatgpt") => AuthStatus::Authenticated {
            kind: AuthKind::Subscription,
            account: Some(AccountInfo {
                email: account["account"]["email"].as_str().map(str::to_owned),
                plan: account["account"]["planType"].as_str().map(str::to_owned),
            }),
        },
        Some("apiKey") => AuthStatus::Authenticated {
            kind: AuthKind::ApiKey,
            account: None,
        },
        Some(other) => AuthStatus::Authenticated {
            kind: AuthKind::Other(other.to_owned()),
            account: None,
        },
        None => AuthStatus::Unauthenticated {
            login: login_methods(&request.installation),
        },
    }
}

/// What the handshake responses tell us, folded into the engine vocabulary.
fn driver_info(
    init: &Value,
    account: &Value,
    models: &Value,
    thread: &Value,
    config: &StartConfig,
    commands: Vec<SlashCommand>,
    request: &ConnectRequest,
) -> DriverInfo {
    // `userAgent` is "anyagent/0.147.0 (…)"; the version rides after the slash.
    let version = init["userAgent"]
        .as_str()
        .and_then(|ua| ua.split('/').nth(1))
        .and_then(|rest| rest.split(' ').next())
        .map(str::to_owned);
    let auth = account_status(account, request);
    // Configured model/effort win: they ride every `turn/start`, while the
    // thread bind reports only the config-file default. The bind and the
    // catalog fill in where nothing was configured.
    let model = config
        .model
        .clone()
        .or_else(|| thread["model"].as_str().map(str::to_owned))
        .or_else(|| default_model(models));
    let effort = config
        .effort
        .clone()
        .or_else(|| thread["reasoningEffort"].as_str().map(str::to_owned))
        .or_else(|| model.as_deref().and_then(|m| default_effort(models, m)));
    let mode = thread["approvalPolicy"].as_str().unwrap_or("on-request");
    let sandbox = sandbox_name(&thread["sandbox"]);
    // model, effort and serviceTier are per-turn `turn/start` parameters, so
    // they switch live with no wire call; mode and sandbox are thread-creation
    // settings.
    let model_option = ConfigOption {
        id: ConfigId::new("model"),
        name: "Model".into(),
        category: Some("model".into()),
        kind: ConfigKind::Select {
            choices: model_choices(models),
        },
        current: model.clone().map(ConfigValue::Text),
        live: true,
    };
    let effort_option = model.as_deref().and_then(|m| {
        let choices = effort_choices(models, m);
        (!choices.is_empty()).then(|| ConfigOption {
            id: ConfigId::new("effort"),
            name: "Reasoning effort".into(),
            category: Some("thought_level".into()),
            kind: ConfigKind::Select { choices },
            current: effort.clone().map(ConfigValue::Text),
            live: true,
        })
    });
    let tier = config.tier.clone().unwrap_or_else(|| "default".into());
    let tier_choices = tier_choices(models);
    let tier_option = (tier_choices.len() > 1).then(|| ConfigOption {
        id: ConfigId::new("serviceTier"),
        name: "Service tier".into(),
        category: Some("service_tier".into()),
        kind: ConfigKind::Select {
            choices: tier_choices,
        },
        current: Some(ConfigValue::Text(tier.clone())),
        live: true,
    });
    let select = |values: &[&str], current: &str| ConfigKind::Select {
        choices: values
            .iter()
            .chain(std::iter::once(&current).filter(|c| !values.contains(*c)))
            .map(|value| ConfigChoice {
                value: (*value).to_owned(),
                label: (*value).to_owned(),
                description: None,
            })
            .collect(),
    };
    let mode_option = ConfigOption {
        id: ConfigId::new("mode"),
        name: "Approval policy".into(),
        category: Some("mode".into()),
        kind: select(&MODES, mode),
        current: Some(ConfigValue::Text(mode.to_owned())),
        live: false,
    };
    let sandbox_option = ConfigOption {
        id: ConfigId::new("sandbox"),
        name: "Sandbox".into(),
        category: Some("sandbox".into()),
        kind: select(&SANDBOXES, &sandbox),
        current: Some(ConfigValue::Text(sandbox.clone())),
        live: false,
    };
    let mut configuration = SessionConfiguration::default();
    for (id, value) in [
        ("model", model),
        ("effort", effort),
        ("serviceTier", tier_option.is_some().then_some(tier)),
        ("mode", Some(mode.to_owned())),
        ("sandbox", Some(sandbox)),
    ] {
        if let Some(value) = value {
            configuration
                .options
                .insert(ConfigId::new(id), ConfigValue::Text(value));
        }
    }
    let mut info = DriverInfo {
        details: AgentDetails {
            version,
            auth,
            // Not advertised: Images (input blocks unprobed on this wire),
            // Questions (`item/tool/requestUserInput` never fired live —
            // ticket 10; the handler exists defensively), Rollback
            // (`thread/rollback` is deprecated upstream; deferred).
            capabilities: Capabilities::new([
                Capability::Steer,
                Capability::Permissions,
                Capability::Resume,
                Capability::Fork,
                Capability::Compact,
                Capability::Plan,
                Capability::Subagents,
                Capability::SlashCommands,
                Capability::ContextUsage,
                Capability::PlanUsage,
            ]),
            config_options: [
                Some(model_option),
                effort_option,
                tier_option,
                Some(mode_option),
                Some(sandbox_option),
            ]
            .into_iter()
            .flatten()
            .collect(),
            commands,
        },
        configuration,
        // `thread/start` returns the id immediately, so the resume token
        // exists at open with no minting trick.
        resume_token: thread["thread"]["id"].as_str().map(ResumeToken::new),
        title: thread["thread"]["name"].as_str().map(str::to_owned),
        // Exactly one `turn/completed` per turn, even interrupted or 401.
        deterministic_turn_end: true,
        deterministic_agent_turn_end: true,
    };
    let model = info.configuration.options.get(&ConfigId::new("model"));
    let supported =
        matches!(model, Some(ConfigValue::Text(model)) if fast_tier(models, model).is_some());
    let fast = config.fast.unwrap_or(matches!(
        thread["serviceTier"].as_str(),
        Some("priority" | "fast")
    ));
    crate::adapter::set_fast_option(&mut info, supported.then_some(fast), true);
    info
}

// ---------------------------------------------------------------------------
// Drive task: engine commands out, wire frames in
// ---------------------------------------------------------------------------

/// A client request awaiting its JSON-RPC response.
enum Pending {
    StartTurn,
    Steer,
    Interrupt,
    Skills,
    Compact,
}

/// A server→client request waiting for `answer`.
struct PendingRequest {
    wire_id: u64,
    /// Present when the request is a `requestUserInput`.
    questions: Option<Vec<Question>>,
}

struct Drive {
    wire: Wire,
    child: process::Child,
    events: mpsc::Sender<DriverEvent>,
    /// Current advertised state; mutated and re-sent as `InfoChanged`.
    info: DriverInfo,
    /// The `model/list` catalog, kept to rebuild effort choices on a switch.
    models: Value,
    thread_id: String,
    /// The running wire turn, once `turn/start`'s response names it.
    turn: Option<String>,
    /// `turn/started` seen. A steer sent before it is refused by the wire
    /// (probed 2026-08-27), so one waits in `pending_steer`.
    turn_started: bool,
    pending_steer: Option<Input>,
    /// A cancel that arrived before the turn id did.
    cancel_pending: bool,
    /// In-flight client requests by wire id.
    pending: HashMap<u64, Pending>,
    /// Active tool items by id. An interrupted turn leaves them with no
    /// `item/completed` (probed 2026-08-27); they are cancelled at turn end.
    tools: HashMap<String, ToolUpdate>,
    /// Subagent child threads: child threadId → the `subAgentActivity` tool
    /// that owns it. Cleared at turn end with the tools it points into.
    children: HashMap<String, ToolId>,
    /// Set while a child thread's frame is being translated, so every content
    /// event it produces rides that subagent tool.
    child_tool: Option<ToolId>,
    requests: HashMap<RequestId, PendingRequest>,
    /// Reasoning items that streamed deltas; only those get a `MessageEnded`.
    open_reasoning: std::collections::HashSet<String>,
    /// The first 401 already surfaced `AuthLost`; the retries stay quiet.
    auth_lost: bool,
    next_msg: u64,
    request: ConnectRequest,
}

impl Drive {
    /// Main loop until the engine or the agent goes away.
    async fn run(mut self, mut commands: mpsc::Receiver<DriverCommand>) {
        // Late skills fetch (see `handshake`); a send failure means the wire
        // is already gone and the loop below will report it.
        if let Ok(id) = self.wire.request("skills/list", json!({})).await {
            self.pending.insert(id, Pending::Skills);
        }
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
                self.emit(DriverEvent::TurnAck).await?;
                let text = self.input_text(&input).await?;
                // model, effort and serviceTier ride on every turn (per-turn
                // parameters). `summary` opts into reasoning summaries: without
                // it no reasoning deltas stream at all (probed 2026-09-03).
                let mut params = json!({
                    "threadId": self.thread_id,
                    "clientUserMessageId": self.mint(),
                    "input": [{ "type": "text", "text": text }],
                    "summary": "auto",
                });
                for key in ["model", "effort", "serviceTier"] {
                    if let Some(ConfigValue::Text(value)) =
                        self.info.configuration.options.get(&ConfigId::new(key))
                        && (key != "serviceTier" || value != "default")
                    {
                        params[key] = json!(value);
                    }
                }
                let fast = self.info.configuration.options.get(&ConfigId::new("fast"))
                    == Some(&ConfigValue::Bool(true));
                // An explicit serviceTier wins over the fast shorthand; fast
                // only resolves the tier when none was chosen directly.
                let tier_explicit = self
                    .info
                    .configuration
                    .options
                    .contains_key(&ConfigId::new("serviceTier"));
                if fast || !tier_explicit {
                    let tier = params["model"]
                        .as_str()
                        .and_then(|model| fast_tier(&self.models, model));
                    params["serviceTier"] = json!(if fast {
                        tier.unwrap_or("default")
                    } else {
                        "default"
                    });
                }
                let id = self.wire.request("turn/start", params).await?;
                self.pending.insert(id, Pending::StartTurn);
            }
            DriverCommand::Steer { input } => {
                if self.turn_started {
                    self.send_steer(input).await?;
                } else {
                    self.pending_steer = Some(input);
                }
            }
            DriverCommand::Answer { request, answer } => self.answer(request, answer).await?,
            DriverCommand::Cancel => {
                self.pending_steer = None;
                for (_, pending) in std::mem::take(&mut self.requests) {
                    let response = match pending.questions {
                        Some(_) => json!({ "answers": {} }),
                        None => json!({ "decision": "cancel" }),
                    };
                    self.wire.respond(pending.wire_id, response).await?;
                }
                if let Some(turn) = self.turn.clone() {
                    self.interrupt(&turn).await?;
                } else if self
                    .pending
                    .values()
                    .any(|p| matches!(p, Pending::StartTurn))
                {
                    // The turn id has not arrived yet; interrupt on receipt.
                    self.cancel_pending = true;
                }
            }
            DriverCommand::Configure(id, value) => {
                // model, effort, fast and serviceTier are per-turn parameters:
                // apply locally, the next `turn/start` carries them. mode and
                // sandbox are creation-only, so the engine never forwards them.
                // An explicit serviceTier wins over the fast shorthand below.
                if matches!(id.as_str(), "model" | "effort" | "fast" | "serviceTier")
                    && crate::adapter::apply_selection(&mut self.info, &id, &value)
                {
                    if let ("model", ConfigValue::Text(model)) = (id.as_str(), &value) {
                        refresh_effort(&mut self.info, &self.models, model);
                        let fast = self.info.configuration.options.get(&ConfigId::new("fast"))
                            == Some(&ConfigValue::Bool(true));
                        crate::adapter::set_fast_option(
                            &mut self.info,
                            fast_tier(&self.models, model).map(|_| fast),
                            true,
                        );
                    }
                    self.emit(DriverEvent::InfoChanged(self.info.clone()))
                        .await?;
                }
            }
            DriverCommand::Compact => {
                let id = self
                    .wire
                    .request(
                        "thread/compact/start",
                        json!({ "threadId": self.thread_id }),
                    )
                    .await?;
                self.pending.insert(id, Pending::Compact);
            }
            DriverCommand::Rollback(..) => {
                // Not advertised: deferred (tickets 07/09). Note 0.152.0
                // still ships `thread/rollback` and `thread/revert` (and T3
                // calls the former), so this is implementable natively —
                // probe the params before picking it up.
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    "rollback is not supported on codex",
                )
                .await?;
            }
            DriverCommand::Close => unreachable!("handled in run"),
        }
        Ok(())
    }

    /// Routes one wire frame: our response, a server request, or a notification.
    async fn handle_frame(&mut self, frame: Value) -> Result<(), Gone> {
        match (frame["method"].as_str(), frame["id"].is_null()) {
            (Some(_), false) => self.on_server_request(&frame).await,
            (Some(method), true) => {
                let method = method.to_owned();
                self.on_notification(&method, &frame["params"]).await
            }
            (None, _) => self.on_response(&frame).await,
        }
    }

    /// A response to one of our requests, matched by id.
    async fn on_response(&mut self, frame: &Value) -> Result<(), Gone> {
        let Some(pending) = frame["id"].as_u64().and_then(|id| self.pending.remove(&id)) else {
            return Ok(());
        };
        let error = frame["error"]["message"].as_str();
        match pending {
            // A wire rejection of the turn is a failed turn. A cancel that
            // raced this start has nothing left to interrupt — a stale flag
            // would cancel the next turn at its start.
            Pending::StartTurn => match error {
                Some(message) => {
                    self.cancel_pending = false;
                    self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                        message: message.to_owned(),
                    }))
                    .await?
                }
                None => {
                    self.turn = frame["result"]["turn"]["id"].as_str().map(str::to_owned);
                    if self.cancel_pending {
                        self.cancel_pending = false;
                        if let Some(turn) = self.turn.clone() {
                            self.interrupt(&turn).await?;
                        }
                    }
                }
            },
            Pending::Steer => self.emit(DriverEvent::Steered(error.is_none())).await?,
            // "no active turn to interrupt" means already idle: success.
            Pending::Interrupt => {}
            // Compaction runs as its own wire turn, which ends the engine's.
            // A refusal never starts one, so it ends that turn itself.
            Pending::Compact => {
                if let Some(message) = error {
                    self.diagnostic(
                        DiagnosticLevel::Warning,
                        format!("compaction refused: {message}"),
                    )
                    .await?;
                    self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                        message: message.to_owned(),
                    }))
                    .await?;
                }
            }
            Pending::Skills => {
                let commands = parse_skill_commands(&frame["result"]);
                if !commands.is_empty() {
                    self.info.details.commands = commands;
                    self.emit(DriverEvent::InfoChanged(self.info.clone()))
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// A server-pushed notification. Frames on a registered subagent child
    /// thread take the child path; everything else is the parent thread's.
    async fn on_notification(&mut self, method: &str, params: &Value) -> Result<(), Gone> {
        let child = params["threadId"]
            .as_str()
            .and_then(|thread| self.children.get(thread))
            .cloned();
        match child {
            Some(tool) => self.on_child_frame(method, params, tool).await,
            None => self.on_parent_frame(method, params).await,
        }
    }

    /// A child thread's frame: content rides the parent's subagent tool, and
    /// the child's turn bookkeeping is consumed so it can never settle the
    /// parent turn. Unknown methods fall through to the parent path.
    async fn on_child_frame(
        &mut self,
        method: &str,
        params: &Value,
        tool: ToolId,
    ) -> Result<(), Gone> {
        match method {
            "turn/completed" | "turn/failed" | "turn/aborted" => {
                self.settle_subagent(tool, &params["turn"]).await
            }
            // Consumed: the child's plan is a whole-list replacement and its
            // usage is its own context window, so neither may reach the
            // parent's, and its turn frames must not move the parent's turn.
            "thread/tokenUsage/updated" | "thread/status/changed" => Ok(()),
            _ if method.starts_with("turn/") => Ok(()),
            // Content: the parent's translation, attributed to the subagent.
            _ if method.starts_with("item/") || method == "error" => {
                self.child_tool = Some(tool);
                let result = self.on_parent_frame(method, params).await;
                self.child_tool = None;
                result
            }
            _ => self.on_parent_frame(method, params).await,
        }
    }

    /// A child turn ended: its subagent tool takes the outcome, so a failed
    /// child is visible instead of vanishing into a cancel at parent turn end.
    async fn settle_subagent(&mut self, tool: ToolId, turn: &Value) -> Result<(), Gone> {
        let Some(mut update) = self.tools.remove(tool.as_str()) else {
            return Ok(());
        };
        update.status = match turn["status"].as_str().unwrap_or_default() {
            "completed" => ToolStatus::Completed,
            "interrupted" => ToolStatus::Cancelled,
            _ => ToolStatus::Failed,
        };
        if let Some(message) = turn["error"]["message"].as_str() {
            update.output = Some(message.to_owned());
        }
        self.content(EventKind::ToolUpdated(update)).await
    }

    /// A parent-thread notification, routed by method.
    async fn on_parent_frame(&mut self, method: &str, params: &Value) -> Result<(), Gone> {
        match method {
            "turn/started" => {
                self.turn_started = true;
                if let Some(id) = params["turn"]["id"].as_str() {
                    self.turn.get_or_insert_with(|| id.to_owned());
                }
                if let Some(input) = self.pending_steer.take() {
                    self.send_steer(input).await?;
                }
                Ok(())
            }
            // 0.152.0 ends every turn with `turn/completed` (status carries
            // the outcome — probed 2026-09-03); failed/aborted are defensive
            // so a wire that emits them can never hang the turn.
            "turn/completed" | "turn/failed" | "turn/aborted" => {
                self.on_turn_completed(params).await
            }
            "item/started" | "item/updated" => self.on_item(params, false).await,
            "item/completed" => self.on_item(params, true).await,
            "item/agentMessage/delta" => {
                self.content(EventKind::TextDelta {
                    message_id: MessageId::new(params["itemId"].as_str().unwrap_or("m0")),
                    text: params["delta"].as_str().unwrap_or_default().to_owned(),
                })
                .await
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let id = params["itemId"].as_str().unwrap_or("m0").to_owned();
                self.open_reasoning.insert(id.clone());
                self.content(EventKind::ReasoningDelta {
                    message_id: MessageId::new(id),
                    text: params["delta"].as_str().unwrap_or_default().to_owned(),
                })
                .await
            }
            "item/commandExecution/outputDelta" | "item/fileChange/outputDelta" => {
                self.content(EventKind::ToolOutputDelta {
                    tool_id: ToolId::new(params["itemId"].as_str().unwrap_or_default()),
                    text: params["delta"].as_str().unwrap_or_default().to_owned(),
                })
                .await
            }
            "turn/plan/updated" => {
                self.content(EventKind::PlanUpdated {
                    entries: plan_entries(&params["plan"]),
                })
                .await
            }
            "thread/tokenUsage/updated" => {
                // `last` is the latest model call = current context occupancy;
                // the window rides in the same frame.
                let usage = &params["tokenUsage"];
                let used = usage["last"]["totalTokens"]
                    .as_u64()
                    .or_else(|| usage["total"]["totalTokens"].as_u64());
                match used {
                    Some(used) => {
                        self.content(EventKind::ContextUsage {
                            used_tokens: used,
                            window_tokens: usage["modelContextWindow"].as_u64(),
                            cost_usd: None,
                        })
                        .await
                    }
                    None => Ok(()),
                }
            }
            "account/rateLimits/updated" => match plan_usage(&params["rateLimits"]) {
                Some(usage) => self.content(EventKind::PlanUsageUpdated(usage)).await,
                None => Ok(()),
            },
            "error" => self.on_error(params).await,
            "warning" | "guardianWarning" | "configWarning" | "model/rerouted" => {
                self.diagnostic(DiagnosticLevel::Warning, notice_text(params))
                    .await
            }
            "deprecationNotice" => {
                self.diagnostic(DiagnosticLevel::Info, notice_text(params))
                    .await
            }
            // Startup chatter; only a failed MCP server is worth surfacing.
            "mcpServer/startupStatus/updated" => {
                match (params["status"].as_str(), params["error"].as_str()) {
                    (Some("failed"), Some(error)) => {
                        self.diagnostic(DiagnosticLevel::Warning, error.to_owned())
                            .await
                    }
                    _ => Ok(()),
                }
            }
            // Session-state echoes and login bookkeeping the engine owns or
            // does not need.
            "thread/started"
            | "thread/status/changed"
            | "serverRequest/resolved"
            | "remoteControl/status/changed"
            | "account/updated"
            | "account/login/completed"
            | "turn/diff/updated" => Ok(()),
            other => {
                let mut extensions = Extensions::new();
                extensions.insert("codex/raw_frame".into(), params.clone());
                self.emit(DriverEvent::Event {
                    kind: EventKind::Diagnostic(Diagnostic {
                        level: DiagnosticLevel::Info,
                        message: format!("unrecognized codex frame `{other}`"),
                    }),
                    parent_tool_id: None,
                    extensions,
                })
                .await
            }
        }
    }

    /// One thread item snapshot: user echo, message, reasoning, or tool.
    async fn on_item(&mut self, params: &Value, completed: bool) -> Result<(), Gone> {
        let item = &params["item"];
        let id = item["id"].as_str().unwrap_or("m0").to_owned();
        match item["type"].as_str().unwrap_or_default() {
            // Our own prompts and steers echo back tagged with our client id.
            "userMessage" => {
                let ours = item["clientId"]
                    .as_str()
                    .is_some_and(|c| c.starts_with(CLIENT_MSG_PREFIX));
                if !completed || ours {
                    return Ok(());
                }
                self.content(EventKind::UserMessage {
                    message_id: MessageId::new(id),
                    text: item_text(item),
                })
                .await
            }
            "agentMessage" => {
                if !completed {
                    return Ok(());
                }
                // The wire turn id is the fork anchor for `fork_from(_, at)`;
                // a subagent's message is not an anchor on the parent thread.
                let mut extensions = Extensions::new();
                if let (None, Some(turn)) = (&self.child_tool, &self.turn) {
                    extensions.insert("codex/fork_point".into(), Value::from(turn.clone()));
                }
                self.emit(DriverEvent::Event {
                    kind: EventKind::MessageEnded {
                        message_id: MessageId::new(id),
                    },
                    parent_tool_id: self.child_tool.clone(),
                    extensions,
                })
                .await
            }
            "reasoning" => {
                if completed && self.open_reasoning.remove(&id) {
                    return self
                        .content(EventKind::MessageEnded {
                            message_id: MessageId::new(id),
                        })
                        .await;
                }
                Ok(())
            }
            "contextCompaction" => {
                if completed {
                    return self.content(EventKind::ContextCompacted).await;
                }
                Ok(())
            }
            // The subagent's own thread; its frames route to this tool.
            "subAgentActivity" => {
                if let Some(child) = item["agentThreadId"].as_str() {
                    self.children.insert(child.to_owned(), ToolId::new(&id));
                }
                self.on_tool_item(&id, item).await
            }
            _ => self.on_tool_item(&id, item).await,
        }
    }

    /// A tool-shaped item: emit the snapshot and keep the active ones, so an
    /// interrupt or a child turn end can still settle them.
    async fn on_tool_item(&mut self, id: &str, item: &Value) -> Result<(), Gone> {
        let tool = tool_update(item);
        if tool.status.is_active() {
            self.tools.insert(id.to_owned(), tool.clone());
        } else {
            self.tools.remove(id);
        }
        self.content(EventKind::ToolUpdated(tool)).await
    }

    /// Exactly one `turn/completed` per turn, holding even for interrupts
    /// and auth failures.
    async fn on_turn_completed(&mut self, params: &Value) -> Result<(), Gone> {
        // An interrupted turn leaves in-flight tool items with no
        // `item/completed`; cancel them so the caller's tool view drains.
        for (_, mut tool) in std::mem::take(&mut self.tools) {
            if tool.status.is_active() {
                tool.status = ToolStatus::Cancelled;
                self.content(EventKind::ToolUpdated(tool)).await?;
            }
        }
        self.requests.clear();
        self.children.clear();
        self.open_reasoning.clear();
        self.turn = None;
        self.turn_started = false;
        self.pending_steer = None;
        self.cancel_pending = false;
        let turn = &params["turn"];
        let stop = match turn["status"].as_str().unwrap_or_default() {
            "completed" => StopReason::Completed {
                source: CompletionSource::Protocol,
            },
            "interrupted" | "aborted" => StopReason::Cancelled,
            _ => StopReason::Failed {
                message: turn["error"]["message"]
                    .as_str()
                    .unwrap_or("turn failed")
                    .to_owned(),
            },
        };
        self.emit(DriverEvent::TurnEnded(stop)).await
    }

    /// `error` notifications; a 401 means the credentials died.
    async fn on_error(&mut self, params: &Value) -> Result<(), Gone> {
        let error = &params["error"];
        let status =
            error["codexErrorInfo"]["responseStreamDisconnected"]["httpStatusCode"].as_u64();
        if status == Some(401) && !self.auth_lost {
            // The server retries the 401 five times before failing the turn;
            // surface the login need on the first one (probed 2026-08-27).
            self.auth_lost = true;
            return self
                .emit(DriverEvent::AuthLost {
                    login: login_methods(&self.request.installation),
                })
                .await;
        }
        let level = if params["willRetry"].as_bool().unwrap_or(false) {
            DiagnosticLevel::Warning
        } else {
            DiagnosticLevel::Error
        };
        self.diagnostic(
            level,
            error["message"]
                .as_str()
                .unwrap_or("agent error")
                .to_owned(),
        )
        .await
    }

    /// Approvals and questions arrive as server→client JSON-RPC requests;
    /// anything else is declined so the server does not hang on us.
    async fn on_server_request(&mut self, frame: &Value) -> Result<(), Gone> {
        let wire_id = frame["id"].as_u64().unwrap_or_default();
        let method = frame["method"].as_str().unwrap_or_default();
        let params = &frame["params"];
        let id = RequestId::new(format!("r{wire_id}"));
        let open = match method {
            // Approvals fire on sandbox escalation, not per tool. The request
            // names only the item; the preceding `item/started` carries the
            // command or the diff.
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                self.requests.insert(
                    id.clone(),
                    PendingRequest {
                        wire_id,
                        questions: None,
                    },
                );
                Request::Permission(PermissionRequest {
                    id,
                    tool: self.tool_for(method, params),
                    options: vec![
                        PermissionChoice::AllowOnce,
                        PermissionChoice::AllowAlways,
                        PermissionChoice::DenyOnce,
                    ],
                    detail: params["reason"].as_str().map(str::to_owned),
                })
            }
            // Schema-confirmed, never observed live on 0.147.0 (ticket 10):
            // translated defensively; `Capability::Questions` stays off.
            "item/tool/requestUserInput" => {
                let questions = questions(&params["questions"]);
                self.requests.insert(
                    id.clone(),
                    PendingRequest {
                        wire_id,
                        questions: Some(questions.clone()),
                    },
                );
                Request::Question(QuestionRequest { id, questions })
            }
            other => {
                self.wire
                    .respond_error(wire_id, &format!("unsupported request: {other}"))
                    .await?;
                return self
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        format!("declined agent request {other}"),
                    )
                    .await;
            }
        };
        self.emit(DriverEvent::event(EventKind::RequestOpened(open)))
            .await
    }

    /// Answers one stored server request.
    async fn answer(&mut self, request: RequestId, answer: Answer) -> Result<(), Gone> {
        let Some(pending) = self.requests.remove(&request) else {
            return Ok(());
        };
        let response = match (&pending.questions, answer) {
            (None, Answer::Permission(choice)) => json!({ "decision": match choice {
                PermissionChoice::AllowOnce => "accept",
                PermissionChoice::AllowAlways => "acceptForSession",
                _ => "decline",
            }}),
            (Some(questions), Answer::Question(answers)) => question_response(questions, &answers),
            _ => json!({ "decision": "decline" }),
        };
        self.wire.respond(pending.wire_id, response).await?;
        Ok(())
    }

    /// The tool a request is about: the tracked snapshot, or a stub carrying
    /// only what the request itself says.
    fn tool_for(&self, method: &str, params: &Value) -> ToolUpdate {
        let item_id = params["itemId"].as_str().unwrap_or_default();
        self.tools
            .get(item_id)
            .cloned()
            .unwrap_or_else(|| ToolUpdate {
                id: ToolId::new(item_id),
                kind: if method.contains("fileChange") {
                    ToolKind::Edit
                } else {
                    ToolKind::Execute
                },
                title: "Approval required".into(),
                status: ToolStatus::Running,
                input: ToolInput::None,
                output: None,
                diffs: Vec::new(),
                locations: Vec::new(),
                raw: None,
            })
    }

    async fn send_steer(&mut self, input: Input) -> Result<(), Gone> {
        let Some(turn) = self.turn.clone() else {
            self.emit(DriverEvent::Steered(false)).await?;
            return Ok(());
        };
        let text = self.input_text(&input).await?;
        let params = json!({
            "threadId": self.thread_id,
            "expectedTurnId": turn,
            "clientUserMessageId": self.mint(),
            "input": [{ "type": "text", "text": text }],
        });
        let id = self.wire.request("turn/steer", params).await?;
        self.pending.insert(id, Pending::Steer);
        Ok(())
    }

    async fn interrupt(&mut self, turn: &str) -> Result<(), Gone> {
        let id = self
            .wire
            .request(
                "turn/interrupt",
                json!({ "threadId": self.thread_id, "turnId": turn }),
            )
            .await?;
        self.pending.insert(id, Pending::Interrupt);
        Ok(())
    }

    /// Prompt text with attachment path refs. Image blocks are unprobed on
    /// this wire, so attachments ride as refs only.
    async fn input_text(&mut self, input: &Input) -> Result<String, Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.as_deref()) {
            self.diagnostic(DiagnosticLevel::Warning, problem.to_owned())
                .await?;
        }
        Ok(attach::with_refs(input.as_text(), &loaded))
    }

    fn mint(&mut self) -> String {
        let id = format!("{CLIENT_MSG_PREFIX}{}", self.next_msg);
        self.next_msg += 1;
        id
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
        self.content(EventKind::Diagnostic(Diagnostic {
            level,
            message: message.into(),
        }))
        .await
    }

    /// Emits one content event, attributed to the subagent tool when the frame
    /// came from a child thread.
    async fn content(&mut self, kind: EventKind) -> Result<(), Gone> {
        self.emit(DriverEvent::Event {
            kind,
            parent_tool_id: self.child_tool.clone(),
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

/// Joined text of a message item's content blocks.
fn item_text(item: &Value) -> String {
    item["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("")
}

/// A tool-shaped thread item as a cumulative snapshot.
fn tool_update(item: &Value) -> ToolUpdate {
    let item_type = item["type"].as_str().unwrap_or_default();
    let (kind, input, detail) = match item_type {
        "commandExecution" => (
            ToolKind::Execute,
            ToolInput::Command {
                command: item["command"].as_str().unwrap_or_default().to_owned(),
                cwd: item["cwd"].as_str().map(PathBuf::from),
            },
            item["command"].as_str(),
        ),
        "fileChange" => (
            ToolKind::Edit,
            item["changes"][0]["path"]
                .as_str()
                .map(|p| ToolInput::Path(p.into()))
                .unwrap_or(ToolInput::None),
            item["changes"][0]["path"].as_str(),
        ),
        "mcpToolCall" => (
            ToolKind::Mcp {
                server: item["server"].as_str().unwrap_or_default().to_owned(),
                tool: item["tool"].as_str().unwrap_or_default().to_owned(),
            },
            ToolInput::None,
            item["tool"].as_str(),
        ),
        "webSearch" => (
            ToolKind::Search,
            item["query"]
                .as_str()
                .map(|q| ToolInput::Query(q.to_owned()))
                .unwrap_or(ToolInput::None),
            item["query"].as_str(),
        ),
        // A subagent's thread, and one collab operation on it (spawn, input,
        // wait). Only `collabAgentToolCall` carries a status, so the thread
        // item stays Running until its child turn ends.
        "subAgentActivity" => (
            ToolKind::Subagent,
            ToolInput::None,
            item["agentPath"].as_str(),
        ),
        "collabAgentToolCall" => (
            ToolKind::Subagent,
            item["prompt"]
                .as_str()
                .map(|p| ToolInput::Text(p.to_owned()))
                .unwrap_or(ToolInput::None),
            item["tool"].as_str(),
        ),
        _ => (ToolKind::Other, ToolInput::None, None),
    };
    let (locations, diffs) = file_changes(&item["changes"]);
    ToolUpdate {
        id: ToolId::new(item["id"].as_str().unwrap_or_default()),
        kind,
        title: match detail {
            Some(detail) => format!("{item_type} {detail}"),
            None => item_type.to_owned(),
        },
        status: match item["status"].as_str().unwrap_or_default() {
            "completed" => ToolStatus::Completed,
            "failed" => ToolStatus::Failed,
            "declined" => ToolStatus::Cancelled,
            _ => ToolStatus::Running,
        },
        input,
        output: item["aggregatedOutput"]
            .as_str()
            .filter(|o| !o.is_empty())
            .map(|o| cap(o.to_owned(), OUTPUT_CAP)),
        locations,
        diffs,
        raw: Some(RawTool {
            name: item_type.to_owned(),
            input: item.clone(),
        }),
    }
}

/// `fileChange.changes[]` as locations, plus diffs where the shape is known:
/// an `add` carries the whole new file in `diff` (probed 2026-08-27); other
/// kinds ride only in `raw`.
fn file_changes(changes: &Value) -> (Vec<PathBuf>, Vec<FileDiff>) {
    let mut locations = Vec::new();
    let mut diffs = Vec::new();
    for change in changes.as_array().into_iter().flatten() {
        let Some(path) = change["path"].as_str() else {
            continue;
        };
        locations.push(PathBuf::from(path));
        if change["kind"]["type"].as_str() == Some("add")
            && let Some(content) = change["diff"].as_str()
        {
            diffs.push(FileDiff {
                path: path.into(),
                old_text: None,
                new_text: content.to_owned(),
            });
        }
    }
    (locations, diffs)
}

fn plan_entries(plan: &Value) -> Vec<PlanEntry> {
    plan.as_array()
        .into_iter()
        .flatten()
        .map(|entry| PlanEntry {
            text: entry["step"].as_str().unwrap_or_default().to_owned(),
            status: match entry["status"].as_str().unwrap_or_default() {
                "inProgress" | "in_progress" => PlanStatus::InProgress,
                "completed" => PlanStatus::Completed,
                _ => PlanStatus::Pending,
            },
        })
        .collect()
}

/// `requestUserInput` questions to the portable shape. Choice ids are the
/// labels: that is what the answer echoes back.
fn questions(input: &Value) -> Vec<Question> {
    input
        .as_array()
        .into_iter()
        .flatten()
        .map(|q| Question {
            id: QuestionId::new(q["id"].as_str().unwrap_or_default()),
            text: q["question"].as_str().unwrap_or_default().to_owned(),
            header: q["header"].as_str().map(str::to_owned),
            choices: q["options"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|o| Choice {
                    id: ChoiceId::new(o["label"].as_str().unwrap_or_default()),
                    label: o["label"].as_str().unwrap_or_default().to_owned(),
                    description: o["description"].as_str().map(str::to_owned),
                })
                .collect(),
            multi_select: false,
            allows_free_text: q["isOther"].as_bool().unwrap_or(false),
        })
        .collect()
}

/// Answers keyed by question id: `{answers: {<id>: {answers: [..]}}}`.
fn question_response(questions: &[Question], answers: &[QuestionAnswer]) -> Value {
    let mut map = serde_json::Map::new();
    for (question, answer) in questions.iter().zip(answers) {
        let values: Vec<Value> = match answer {
            QuestionAnswer::Choices(ids) => ids.iter().map(|id| Value::from(id.as_str())).collect(),
            QuestionAnswer::Text(text) => vec![Value::from(text.clone())],
        };
        map.insert(question.id.to_string(), json!({ "answers": values }));
    }
    json!({ "answers": map })
}

/// The human text of a warning-shaped notification.
fn notice_text(params: &Value) -> String {
    params["message"]
        .as_str()
        .or_else(|| params["summary"].as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| params.to_string())
}

/// `account/rateLimits` → quota windows; `primary` and `secondary` are the
/// plan's two windows (300 min and 10080 min observed), and `planType` names
/// the plan. `windowDurationMins` is sometimes absent; T3 falls back to the
/// plan's known pair (5h/weekly, the secondary monthly on free/go plans).
fn plan_usage(rate_limits: &Value) -> Option<PlanUsage> {
    let plan = rate_limits["planType"].as_str().map(str::to_owned);
    let monthly = matches!(plan.as_deref(), Some("free" | "go"));
    let secondary_default = if monthly { 43200 } else { 10080 };
    let windows: Vec<UsageWindow> = [("primary", 300), ("secondary", secondary_default)]
        .iter()
        .filter_map(|(key, default_mins)| {
            let window = &rate_limits[*key];
            let mins = window["windowDurationMins"]
                .as_u64()
                .unwrap_or(*default_mins);
            Some(UsageWindow {
                label: window_label(mins),
                used_percent: window["usedPercent"].as_f64()?.round().clamp(0.0, 100.0) as u8,
                resets_at: window["resetsAt"]
                    .as_u64()
                    .map(|secs| UNIX_EPOCH + Duration::from_secs(secs)),
            })
        })
        .collect();
    (!windows.is_empty()).then(|| PlanUsage {
        plan,
        windows,
        fetched_at: SystemTime::now(),
    })
}

/// "Session" for the short window, "Week" for the 7-day one, "Month" for a
/// 28–31-day one, hours otherwise.
fn window_label(mins: u64) -> String {
    match mins {
        0..=720 => "Session".into(),
        10080 => "Week".into(),
        40320..=44640 => "Month".into(),
        m => format!("{}h", m / 60),
    }
}

/// The wire's camelCase sandbox report back to its kebab-case setting name.
fn sandbox_name(sandbox: &Value) -> String {
    match sandbox["type"].as_str().unwrap_or_default() {
        "readOnly" => "read-only".into(),
        "workspaceWrite" => "workspace-write".into(),
        "dangerFullAccess" => "danger-full-access".into(),
        other => other.to_owned(),
    }
}

fn model_entry<'a>(models: &'a Value, id: &str) -> Option<&'a Value> {
    models
        .as_array()?
        .iter()
        .find(|m| m["id"].as_str() == Some(id))
}

/// Uses the advertised Fast tier, including the older speed-tier catalog.
fn fast_tier<'a>(models: &'a Value, model: &str) -> Option<&'a str> {
    let entry = model_entry(models, model)?;
    entry["serviceTiers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tier| tier["id"].as_str())
        .find(|id| matches!(*id, "priority" | "fast"))
        .or_else(|| {
            entry["additionalSpeedTiers"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .find(|id| *id == "fast")
        })
}

fn default_model(models: &Value) -> Option<String> {
    let entries = models.as_array()?;
    entries
        .iter()
        .find(|m| m["isDefault"].as_bool() == Some(true))
        .or_else(|| entries.first())?["id"]
        .as_str()
        .map(str::to_owned)
}

fn default_effort(models: &Value, model: &str) -> Option<String> {
    model_entry(models, model)?["defaultReasoningEffort"]
        .as_str()
        .map(str::to_owned)
}

/// The `model/list` catalog as config choices; hidden entries stay hidden.
fn model_choices(models: &Value) -> Vec<ConfigChoice> {
    models
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| m["hidden"].as_bool() != Some(true))
        .map(|m| ConfigChoice {
            value: m["id"].as_str().unwrap_or_default().to_owned(),
            label: m["displayName"].as_str().unwrap_or_default().to_owned(),
            description: m["description"].as_str().map(str::to_owned),
        })
        .collect()
}

/// One model's `supportedReasoningEfforts` as config choices.
fn effort_choices(models: &Value, model: &str) -> Vec<ConfigChoice> {
    model_entry(models, model)
        .and_then(|m| m["supportedReasoningEfforts"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|level| {
            Some(ConfigChoice {
                value: level["reasoningEffort"].as_str()?.to_owned(),
                label: level["reasoningEffort"].as_str()?.to_owned(),
                description: level["description"].as_str().map(str::to_owned),
            })
        })
        .collect()
}

/// "Standard" plus every service tier any model reports (`serviceTiers` in
/// `model/list`); "default" is never sent on the wire. Tiers are identical
/// across the models that have them, so one static option serves all.
fn tier_choices(models: &Value) -> Vec<ConfigChoice> {
    let mut choices = vec![ConfigChoice {
        value: "default".into(),
        label: "Standard".into(),
        description: None,
    }];
    for tier in models
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["serviceTiers"].as_array())
        .flatten()
    {
        let Some(id) = tier["id"].as_str() else {
            continue;
        };
        if choices.iter().all(|c| c.value != id) {
            choices.push(ConfigChoice {
                value: id.to_owned(),
                label: tier["name"].as_str().unwrap_or(id).to_owned(),
                description: tier["description"].as_str().map(str::to_owned),
            });
        }
    }
    choices
}

/// Effort choices follow the selected model; a current level the new model
/// does not support falls back to its default.
fn refresh_effort(info: &mut DriverInfo, models: &Value, model: &str) {
    let choices = effort_choices(models, model);
    let effort_id = ConfigId::new("effort");
    let current = info
        .configuration
        .options
        .get(&effort_id)
        .and_then(|v| match v {
            ConfigValue::Text(text) => Some(text.clone()),
            ConfigValue::Bool(_) => None,
        })
        .filter(|current| choices.iter().any(|c| &c.value == current))
        .or_else(|| default_effort(models, model));
    if let Some(option) = info
        .details
        .config_options
        .iter_mut()
        .find(|o| o.id == effort_id)
    {
        option.kind = ConfigKind::Select { choices };
        option.current = current.clone().map(ConfigValue::Text);
    }
    match current {
        Some(effort) => {
            info.configuration
                .options
                .insert(effort_id, ConfigValue::Text(effort));
        }
        None => {
            info.configuration.options.remove(&effort_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Wire: line-delimited JSON-RPC 2.0 over the child's stdio
// ---------------------------------------------------------------------------

struct Wire {
    stdin: tokio::process::ChildStdin,
    /// All frames the reader task saw, bounded; pipe backpressure beyond.
    frames: mpsc::Receiver<Value>,
    next_id: u64,
    recorder: Option<WireRecorder>,
}

impl Wire {
    /// Takes the child's stdio and starts the line-reader task.
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

    /// Sends a request and returns its id.
    async fn request(&mut self, method: &str, params: Value) -> std::io::Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;
        Ok(id)
    }

    async fn notify(&mut self, method: &str) -> Result<(), WireError> {
        self.write(json!({ "jsonrpc": "2.0", "method": method, "params": {} }))
            .await
            .map_err(|_| WireError::Closed)
    }

    /// Answers one of the server's requests. Server ids live in the server's
    /// own id space, separate from ours.
    async fn respond(&mut self, id: u64, result: Value) -> std::io::Result<()> {
        self.write(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
            .await
    }

    async fn respond_error(&mut self, id: u64, message: &str) -> std::io::Result<()> {
        self.write(
            json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": message } }),
        )
        .await
    }

    /// Handshake only: sends a request and blocks on its response, skipping
    /// notifications and other startup frames.
    async fn roundtrip(&mut self, method: &str, params: Value) -> Result<Value, WireError> {
        let id = self
            .request(method, params)
            .await
            .map_err(|_| WireError::Closed)?;
        loop {
            let frame = self.frames.recv().await.ok_or(WireError::Closed)?;
            if !frame["method"].is_null() || frame["id"].as_u64() != Some(id) {
                continue;
            }
            if let Some(message) = frame["error"]["message"].as_str() {
                return Err(WireError::Rpc(message.to_owned()));
            }
            return Ok(frame["result"].clone());
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

enum WireError {
    Closed,
    Rpc(String),
}

impl WireError {
    fn into_error(self) -> AgentError {
        match self {
            WireError::Closed => AgentError::ProtocolFailed("agent closed the wire".into()),
            WireError::Rpc(message) => AgentError::ProtocolFailed(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_usage_falls_back_when_durations_are_absent() {
        // The observed shape carries durations; older frames may not.
        let usage = plan_usage(&json!({
            "planType": "plus",
            "primary": { "usedPercent": 12.4 },
            "secondary": { "usedPercent": 55.6 },
        }))
        .unwrap();
        let labels: Vec<&str> = usage.windows.iter().map(|w| w.label.as_str()).collect();
        assert_eq!(labels, vec!["Session", "Week"]);
        assert_eq!(usage.windows[0].used_percent, 12);

        // free/go plans meter the secondary window monthly.
        let usage = plan_usage(&json!({
            "planType": "free",
            "primary": { "usedPercent": 1.0 },
            "secondary": { "usedPercent": 2.0 },
        }))
        .unwrap();
        assert_eq!(usage.windows[1].label, "Month");
    }

    #[test]
    fn window_label_names_the_known_windows() {
        assert_eq!(window_label(300), "Session");
        assert_eq!(window_label(10080), "Week");
        assert_eq!(window_label(43200), "Month");
        assert_eq!(window_label(20160), "336h");
    }
}
