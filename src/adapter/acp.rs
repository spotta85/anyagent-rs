//! ACP adapter: drives any ACP v1 agent over stdio. Owns a small JSON-RPC
//! reader (S0 decision: bounded channel, raw frames, typed parsing per frame
//! with raw fallback); the engine owns all turn rules.

use std::collections::HashMap;
use std::time::Duration;

use agent_client_protocol_schema::v1 as acp;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
    WireRecorder, attach,
};
use crate::agent::{
    AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigChoice, ConfigId,
    ConfigKind, ConfigOption, ConfigValue, Input, LoginMethod, McpConnection, McpServer,
    McpTransport, ResumeToken, SessionConfiguration, SessionStart, SlashCommand,
};
use crate::error::AgentError;
use crate::event::{
    Choice, ChoiceId, CompletionSource, Diagnostic, DiagnosticLevel, EventKind, Extensions,
    FileDiff, MessageId, PermissionChoice, PermissionRequest, PlanEntry, PlanStatus, Question,
    QuestionAnswer, QuestionId, QuestionRequest, RawTool, Request, RequestId, StopReason, ToolId,
    ToolInput, ToolKind, ToolStatus, ToolUpdate,
};
use crate::process::{self, Spawn};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_GRACE: Duration = Duration::from_secs(2);
const FRAME_BUFFER: usize = 64;
const AUTH_REQUIRED_CODE: i64 = -32000;

/// One instance per ACP agent; `args` put the CLI in protocol mode.
pub(crate) struct AcpAdapter {
    args: Vec<String>,
    /// Catalog facts for auth truth (open-proves-auth, logged-out
    /// fingerprints); `None` for an ad-hoc ACP install.
    profile: Option<&'static crate::catalog::AgentProfile>,
}

impl AcpAdapter {
    pub(crate) fn new(args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            args: args.into_iter().map(Into::into).collect(),
            profile: None,
        }
    }

    /// A catalog agent: protocol args and auth facts from its profile.
    pub(crate) fn for_profile(profile: &'static crate::catalog::AgentProfile) -> Self {
        let crate::catalog::Connection::Acp { args } = &profile.connection else {
            unreachable!("for_profile is only called for ACP profiles");
        };
        Self {
            args: args.iter().map(|s| (*s).to_owned()).collect(),
            profile: Some(profile),
        }
    }
}

#[async_trait]
impl Adapter for AcpAdapter {
    /// Spawns the CLI, runs `initialize` + `session/new` (or `session/load`),
    /// and hands the live wire to the drive task.
    async fn connect(&self, request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let env = crate::adapter::config_home_env(&request.installation, &request.options)?;
        let (ev_tx, ev_rx) = mpsc::channel(FRAME_BUFFER);
        let recorder = WireRecorder::for_session(&request.options, &ev_tx).await;
        let mut child = process::spawn(Spawn {
            exec_path: request.installation.executable_path.clone(),
            args: self.args.clone(),
            cwd: request.options.cwd().clone(),
            env,
        })
        .await?;
        let mut wire = Wire::over(&mut child, recorder);

        let open_auth_kind = self.profile.and_then(|p| p.open_auth_kind.clone());
        let handshake = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            handshake(&mut wire, &request, open_auth_kind),
        );
        let (info, session_id, login, first_class_model, kiro) = match handshake.await {
            Ok(Ok(ok)) => ok,
            Ok(Err(e)) => {
                // Shutdown first: it joins the stderr reader, so the tail is
                // complete before the error is rendered and hint-matched.
                child.shutdown(CLOSE_GRACE).await;
                let e = crate::adapter::with_stderr(e, &child);
                return Err(auth_hinted(
                    e,
                    self.profile,
                    &request.installation.executable_path,
                ));
            }
            Err(_) => {
                child.shutdown(CLOSE_GRACE).await;
                return Err(AgentError::HandshakeTimeout);
            }
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        tokio::spawn(
            Drive {
                wire,
                child,
                session_id,
                events: ev_tx,
                info: info.clone(),
                tools: HashMap::new(),
                permissions: HashMap::new(),
                questions: HashMap::new(),
                prompt_id: None,
                prompt_meta: None,
                prompt_seq: 0,
                steer_id: None,
                configs: Vec::new(),
                first_class_model,
                kiro,
                effort_id: None,
                pending_effort: None,
                held_prompt: None,
                login,
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

/// `initialize`, then `session/new` or `session/load`. A `session/new` error
/// with the auth code becomes `AuthRequired` with runnable login methods;
/// the same methods ride along for auth failures later in the session.
async fn handshake(
    wire: &mut Wire,
    request: &ConnectRequest,
    open_auth_kind: Option<AuthKind>,
) -> Result<(DriverInfo, String, Vec<LoginMethod>, bool, bool), AgentError> {
    let init = wire
        .roundtrip(
            "initialize",
            json!({ "protocolVersion": 1, "clientCapabilities": {} }),
        )
        .await
        .map_err(|e| e.into_error(&[], &request.installation.executable_path))?;
    let init: acp::InitializeResponse = parse(init, "initialize response")?;

    let resume = match &request.options.start {
        SessionStart::Resume(token) => Some(token),
        SessionStart::Fork { .. } => {
            return Err(AgentError::UnsupportedFeature(
                "fork (no ACP agent advertises it)".into(),
            ));
        }
        SessionStart::New => None,
    };
    if resume.is_some() && !init.agent_capabilities.load_session {
        return Err(AgentError::ResumeFailed(
            "this agent does not support session/load".into(),
        ));
    }
    let mcp_servers = mcp_entries(&request.options.mcp_servers, &init)?;
    let (method, params, session_id) = match resume {
        Some(token) => (
            "session/load",
            json!({ "sessionId": token.as_str(), "cwd": request.options.cwd(), "mcpServers": mcp_servers }),
            token.as_str().to_owned(),
        ),
        None => (
            "session/new",
            json!({ "cwd": request.options.cwd(), "mcpServers": mcp_servers }),
            String::new(),
        ),
    };
    let response = wire
        .roundtrip(method, params)
        .await
        .map_err(|e| e.into_error(&init.auth_methods, &request.installation.executable_path))?;

    let mut info = driver_info(&init, &request.installation.auth, open_auth_kind);
    let first_class_models = response.get("models").cloned();
    let session_id = if session_id.is_empty() {
        let new: acp::NewSessionResponse = parse(response, "session/new response")?;
        apply_session_config(&mut info, new.modes.as_ref(), new.config_options.as_deref());
        new.session_id.0.to_string()
    } else {
        // A resumed session reports its modes and options the same way a new
        // one does (probed against opencode 1.18: `session/load` answers with
        // the full `configOptions`). Skipping this left a reattached thread
        // with no model or mode to switch. Parsing is best-effort: an agent
        // that answers `null` still resumes, just without the surface.
        if let Ok(loaded) = parse::<acp::LoadSessionResponse>(response, "session/load response") {
            apply_session_config(
                &mut info,
                loaded.modes.as_ref(),
                loaded.config_options.as_deref(),
            );
        }
        session_id
    };
    let first_class_model =
        first_class_models.is_some_and(|models| apply_first_class_models(&mut info, &models));
    let kiro = is_kiro(&init);
    info.resume_token = Some(ResumeToken::new(&session_id));
    // Creation-time config: apply each requested option before the first
    // turn; a refusal fails the open instead of silently running misconfigured.
    // Kiro's effort waits for the model, since its choices depend on it.
    let mut effort = None;
    for (id, value) in &request.options.configure {
        if kiro && id.as_str() == "effort" {
            effort = Some(value);
            continue;
        }
        let first_class = first_class_model
            .then(|| selected(&info, "model"))
            .flatten();
        let (method, params) = config_call(&session_id, id, value, first_class.as_deref());
        wire.roundtrip(method, params).await.map_err(|e| match e {
            WireError::Rpc { message, .. } => {
                AgentError::InvalidConfiguration(format!("agent rejected `{id}`: {message}"))
            }
            e => e.into_error(&init.auth_methods, &request.installation.executable_path),
        })?;
        crate::adapter::apply_selection(&mut info, id, value);
    }
    if kiro {
        sync_effort(&mut info);
    }
    if let Some(value) = effort {
        if !offers(&info, "effort", value) {
            return Err(AgentError::InvalidConfiguration(
                "effort is not supported by the selected model".into(),
            ));
        }
        // The ack chunk is skipped with the other handshake noise.
        wire.roundtrip("session/prompt", effort_prompt(&session_id, value))
            .await
            .map_err(|e| e.into_error(&init.auth_methods, &request.installation.executable_path))?;
        crate::adapter::apply_selection(&mut info, &ConfigId::new("effort"), value);
    }
    let login = init
        .auth_methods
        .iter()
        .filter_map(|m| login_method(m, &request.installation.executable_path))
        .collect();
    Ok((info, session_id, login, first_class_model, kiro))
}

/// Kiro is the one ACP agent with a prompt-driven effort switch.
fn is_kiro(init: &acp::InitializeResponse) -> bool {
    init.agent_info
        .as_ref()
        .is_some_and(|i| i.name.starts_with("Kiro"))
}

/// Makes kiro's `effort` option match the selected model: the shared
/// levels for models that have effort, no option for the ones that don't.
/// Kiro advertises nothing for it on the wire (probed 2.20.1).
fn sync_effort(info: &mut DriverInfo) {
    let model = selected(info, "model").unwrap_or_default();
    let choices = crate::adapter::effort_choices(&model).unwrap_or_default();
    let current = selected(info, "effort")
        .map(ConfigValue::Text)
        .filter(|c| offers_choice(&choices, c));
    replace_select(info, "effort", choices, current);
}

/// The selected text value of option `id`.
fn selected(info: &DriverInfo, id: &str) -> Option<String> {
    match info.configuration.options.get(&ConfigId::new(id)) {
        Some(ConfigValue::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

/// Replaces the `model` or `effort` select with these choices and current
/// value; no choices means no option.
fn replace_select(
    info: &mut DriverInfo,
    id: &str,
    choices: Vec<ConfigChoice>,
    current: Option<ConfigValue>,
) {
    let (name, category) = match id {
        "model" => ("Model", "model"),
        _ => ("Reasoning effort", "thought_level"),
    };
    let id = ConfigId::new(id);
    info.details.config_options.retain(|o| o.id != id);
    info.configuration.options.remove(&id);
    if choices.is_empty() {
        return;
    }
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

/// Whether the advertised option `id` offers `value`.
fn offers(info: &DriverInfo, id: &str, value: &ConfigValue) -> bool {
    info.details.config_options.iter().any(|o| {
        o.id.as_str() == id
            && matches!(&o.kind, ConfigKind::Select { choices } if offers_choice(choices, value))
    })
}

fn offers_choice(choices: &[ConfigChoice], value: &ConfigValue) -> bool {
    matches!(value, ConfigValue::Text(v) if choices.iter().any(|c| &c.value == v))
}

/// Kiro has no wire call for effort: `/effort <level>` runs as its own
/// prompt (probed 2026-09-05: replies "Effort set to <level>", end_turn).
fn effort_prompt(session_id: &str, value: &ConfigValue) -> Value {
    let level = match value {
        ConfigValue::Text(level) => level.clone(),
        ConfigValue::Bool(on) => on.to_string(),
    };
    json!({ "sessionId": session_id, "prompt": [{ "type": "text", "text": format!("/effort {level}") }] })
}

/// One config selection as its wire call: the well-known `mode` id maps to
/// session/set_mode; with the first-class models surface (`first_class` is
/// the selected model), `model` maps to session/set_model and `effort` to
/// the same call carrying grok's `_meta.reasoningEffort` (verified live,
/// 2026-09-05); anything else to session/set_config_option.
fn config_call(
    session_id: &str,
    id: &ConfigId,
    value: &ConfigValue,
    first_class: Option<&str>,
) -> (&'static str, Value) {
    match (id.as_str(), value, first_class) {
        ("mode", ConfigValue::Text(mode), _) => (
            "session/set_mode",
            json!({ "sessionId": session_id, "modeId": mode }),
        ),
        ("model", ConfigValue::Text(model), Some(_)) => (
            "session/set_model",
            json!({ "sessionId": session_id, "modelId": model }),
        ),
        ("effort", ConfigValue::Text(effort), Some(model)) => (
            "session/set_model",
            json!({ "sessionId": session_id, "modelId": model, "_meta": { "reasoningEffort": effort } }),
        ),
        (_, ConfigValue::Text(chosen), _) => (
            "session/set_config_option",
            json!({ "sessionId": session_id, "configId": id.as_str(), "value": chosen }),
        ),
        (_, ConfigValue::Bool(on), _) => (
            "session/set_config_option",
            json!({ "sessionId": session_id, "configId": id.as_str(), "type": "boolean", "value": on }),
        ),
    }
}

/// Grok's first-class model surface: `session/new` carries
/// `models: {currentModelId, availableModels}` instead of a config option,
/// and switching requires `session/set_model`. Not in the typed schema yet,
/// so it is read raw (shape verified against grok by comet and T3 Code).
/// Skipped when the agent already advertises a `model` config option.
fn apply_first_class_models(info: &mut DriverInfo, models: &Value) -> bool {
    let already_advertised = info
        .details
        .config_options
        .iter()
        .any(|o| o.id.as_str() == "model");
    let empty = models["availableModels"]
        .as_array()
        .is_none_or(|list| list.is_empty());
    if already_advertised || empty {
        return false;
    }
    sync_first_class_models(info, models);
    true
}

/// Rebuilds `model` from a first-class models state, and `effort` from the
/// current model's `_meta.reasoningEfforts` when the agent reports them
/// (grok). The `_x.ai/models/update` notification repeats the shape before
/// every turn with a stale `reasoningEffort`, so a selection the model still
/// offers wins over the reported one; `model_changed` carries the truth.
fn sync_first_class_models(info: &mut DriverInfo, models: &Value) {
    let list = models["availableModels"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let current_id = models["currentModelId"].as_str();
    let choices = list
        .iter()
        .filter_map(|m| {
            let id = m["modelId"].as_str()?;
            Some(ConfigChoice {
                value: id.to_owned(),
                label: m["name"].as_str().unwrap_or(id).to_owned(),
                description: m["description"].as_str().map(str::to_owned),
            })
        })
        .collect();
    let current = list.iter().find(|m| m["modelId"].as_str() == current_id);
    let efforts: Vec<ConfigChoice> = current
        .and_then(|m| m["_meta"]["reasoningEfforts"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let value = e["value"].as_str().or(e["id"].as_str())?;
            Some(ConfigChoice {
                value: value.to_owned(),
                label: e["label"].as_str().unwrap_or(value).to_owned(),
                description: e["description"].as_str().map(str::to_owned),
            })
        })
        .collect();
    let effort = selected(info, "effort")
        .map(ConfigValue::Text)
        .filter(|e| offers_choice(&efforts, e))
        .or_else(|| {
            current
                .and_then(|m| m["_meta"]["reasoningEffort"].as_str())
                .map(|e| ConfigValue::Text(e.to_owned()))
        });
    replace_select(
        info,
        "model",
        choices,
        current_id.map(|c| ConfigValue::Text(c.to_owned())),
    );
    if !efforts.is_empty() {
        replace_select(info, "effort", efforts, effort);
    }
}

/// What `initialize` tells us, folded into the engine's vocabulary.
fn driver_info(
    init: &acp::InitializeResponse,
    auth: &Option<AuthStatus>,
    open_auth_kind: Option<AuthKind>,
) -> DriverInfo {
    let mut features = vec![Capability::Permissions];
    if init.agent_capabilities.prompt_capabilities.image {
        features.push(Capability::Images);
    }
    if init.agent_capabilities.load_session {
        features.push(Capability::Resume);
    }
    if steering_advertised(init) {
        features.push(Capability::Steer);
    }
    let mut capabilities = Capabilities::new(features);
    capabilities.mcp_transports = mcp_transports(init);
    DriverInfo {
        details: AgentDetails {
            version: init.agent_info.as_ref().map(|i| i.version.clone()),
            // ACP has no auth-status field on the wire. A marker with a kind
            // wins (it knows subscription vs key); otherwise reaching this
            // point proves login for agents that refuse to open logged out
            // (`open_auth_kind`, probed per agent); the rest stay best-effort.
            auth: match (auth, open_auth_kind) {
                (Some(a @ AuthStatus::Authenticated { .. }), _) => a.clone(),
                (_, Some(kind)) => AuthStatus::Authenticated {
                    kind,
                    account: None,
                },
                (a, None) => a.clone().unwrap_or(AuthStatus::Unknown),
            },
            capabilities,
            config_options: Vec::new(),
            commands: Vec::new(),
        },
        configuration: SessionConfiguration::default(),
        resume_token: None,
        title: None,
        // The prompt response ends prompted turns; nothing ends unprompted ones.
        deterministic_turn_end: true,
        deterministic_agent_turn_end: false,
    }
}

/// Transports this agent takes MCP servers over. Stdio is ACP's baseline.
fn mcp_transports(init: &acp::InitializeResponse) -> Vec<McpTransport> {
    let mcp = &init.agent_capabilities.mcp_capabilities;
    let mut transports = vec![McpTransport::Stdio];
    if mcp.http {
        transports.push(McpTransport::Http);
    }
    if mcp.sse {
        transports.push(McpTransport::Sse);
    }
    transports
}

/// Declared MCP servers as ACP `mcpServers` entries. A declaration the agent
/// cannot take is refused, never silently dropped.
fn mcp_entries(servers: &[McpServer], init: &acp::InitializeResponse) -> Result<Value, AgentError> {
    let supported = mcp_transports(init);
    let pairs = |map: &std::collections::BTreeMap<String, String>| -> Vec<Value> {
        map.iter()
            .map(|(name, value)| json!({ "name": name, "value": value }))
            .collect()
    };
    let mut entries = Vec::with_capacity(servers.len());
    for server in servers {
        if !supported.contains(&server.transport()) {
            return Err(AgentError::UnsupportedFeature(format!(
                "{:?} MCP servers",
                server.transport()
            )));
        }
        entries.push(match &server.connection {
            McpConnection::Stdio { command, args, env } => json!({
                "name": server.name, "command": command, "args": args, "env": pairs(env),
            }),
            McpConnection::Http { url, headers } => json!({
                "type": "http", "name": server.name, "url": url, "headers": pairs(headers),
            }),
            McpConnection::Sse { url, headers } => json!({
                "type": "sse", "name": server.name, "url": url, "headers": pairs(headers),
            }),
        });
    }
    Ok(Value::Array(entries))
}

/// The `_session/steering` extension is advertised under capability `_meta`.
fn steering_advertised(init: &acp::InitializeResponse) -> bool {
    init.agent_capabilities
        .meta
        .as_ref()
        .and_then(|m| m.get("steering"))
        .and_then(|s| s.get("supported"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Folds session modes and config options into details and configuration.
fn apply_session_config(
    info: &mut DriverInfo,
    modes: Option<&acp::SessionModeState>,
    options: Option<&[acp::SessionConfigOption]>,
) {
    if let Some(modes) = modes {
        info.details.config_options.push(ConfigOption {
            id: ConfigId::new("mode"),
            name: "Mode".into(),
            category: Some("mode".into()),
            kind: ConfigKind::Select {
                choices: modes
                    .available_modes
                    .iter()
                    .map(|m| ConfigChoice {
                        value: m.id.0.to_string(),
                        label: m.name.clone(),
                        description: m.description.clone(),
                    })
                    .collect(),
            },
            current: Some(ConfigValue::Text(modes.current_mode_id.0.to_string())),
            live: true,
        });
        info.configuration.options.insert(
            ConfigId::new("mode"),
            ConfigValue::Text(modes.current_mode_id.0.to_string()),
        );
    }
    for option in options.unwrap_or_default() {
        let (kind, current) = match &option.kind {
            acp::SessionConfigKind::Select(select) => (
                ConfigKind::Select {
                    choices: select_choices(&select.options),
                },
                Some(ConfigValue::Text(select.current_value.0.to_string())),
            ),
            acp::SessionConfigKind::Boolean(b) => (
                ConfigKind::Boolean,
                Some(ConfigValue::Bool(b.current_value)),
            ),
            _ => continue,
        };
        if let Some(current) = &current {
            info.configuration
                .options
                .insert(ConfigId::new(option.id.0.as_ref()), current.clone());
        }
        info.details.config_options.push(ConfigOption {
            id: ConfigId::new(option.id.0.as_ref()),
            name: option.name.clone(),
            category: option.category.as_ref().map(config_category),
            kind,
            current,
            live: true,
        });
    }
}

/// The wire category as its canonical string (spec ids, `Other` verbatim).
fn config_category(category: &acp::SessionConfigOptionCategory) -> String {
    use acp::SessionConfigOptionCategory as C;
    match category {
        C::Mode => "mode".into(),
        C::Model => "model".into(),
        C::ModelConfig => "model_config".into(),
        C::ThoughtLevel => "thought_level".into(),
        C::Other(other) => other.clone(),
        _ => "other".into(),
    }
}

fn select_choices(options: &acp::SessionConfigSelectOptions) -> Vec<ConfigChoice> {
    let flat: Vec<&acp::SessionConfigSelectOption> = match options {
        acp::SessionConfigSelectOptions::Ungrouped(o) => o.iter().collect(),
        acp::SessionConfigSelectOptions::Grouped(groups) => {
            groups.iter().flat_map(|g| g.options.iter()).collect()
        }
        _ => Vec::new(),
    };
    flat.iter()
        .map(|o| ConfigChoice {
            value: o.value.0.to_string(),
            label: o.name.clone(),
            description: o.description.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Drive task: engine commands out, wire frames in
// ---------------------------------------------------------------------------

/// A permission request waiting for `answer`: its wire id and the offered
/// options, so a `PermissionChoice` maps back to the agent's option id.
struct PendingPermission {
    wire_id: Value,
    options: Vec<(PermissionChoice, String)>,
}

/// A grok `_x.ai/ask_user_question` waiting for `answer`: its wire id and the
/// raw question objects, so choice ids map back to the labels grok expects.
struct PendingQuestion {
    wire_id: Value,
    questions: Vec<Value>,
}

struct Drive {
    wire: Wire,
    child: process::Child,
    session_id: String,
    events: mpsc::Sender<DriverEvent>,
    /// Current advertised state; mutated and re-sent as `InfoChanged`.
    info: DriverInfo,
    /// Cumulative tool snapshots, merged from partial wire updates.
    tools: HashMap<String, ToolUpdate>,
    permissions: HashMap<RequestId, PendingPermission>,
    questions: HashMap<RequestId, PendingQuestion>,
    prompt_id: Option<u64>,
    /// `_meta.promptId` minted for the outstanding prompt. Grok echoes it in
    /// `_x.ai/session/prompt_complete`, so a stale replay can't end a newer
    /// turn (verified live against grok 1.0.4 by comet).
    prompt_meta: Option<String>,
    prompt_seq: u64,
    steer_id: Option<u64>,
    /// In-flight configures: wire id plus the selection to apply on success.
    configs: Vec<(u64, ConfigId, ConfigValue)>,
    /// The agent advertises the first-class `models` state (grok): `model`
    /// selections ride `session/set_model`.
    first_class_model: bool,
    /// The agent is kiro: `effort` selections ride a `/effort` prompt.
    kiro: bool,
    /// Wire id of an in-flight `/effort` prompt; its chunks stay internal.
    effort_id: Option<u64>,
    /// An effort switch requested mid-turn, sent once the turn ends.
    pending_effort: Option<ConfigValue>,
    /// A turn (params, prompt id) that arrived mid-switch, sent once it ends.
    held_prompt: Option<(Value, String)>,
    /// Runnable login methods from `initialize`, for mid-session auth loss.
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
                self.emit(DriverEvent::TurnAck).await?;
                // `_meta.promptId` lets grok's prompt-complete extension name
                // this exact prompt; spec-conformant agents ignore `_meta`.
                self.prompt_seq += 1;
                let pid = format!("p{}", self.prompt_seq);
                let params = json!({
                    "sessionId": self.session_id,
                    "prompt": self.prompt_blocks(&input).await?,
                    "_meta": { "promptId": pid, "requestId": pid },
                });
                // One prompt on the wire at a time: a turn that lands while
                // an effort switch runs waits for it.
                if self.effort_id.is_some() {
                    self.held_prompt = Some((params, pid));
                } else {
                    self.send_prompt(params, pid).await?;
                }
            }
            DriverCommand::Steer { input } => {
                let params = json!({
                    "sessionId": self.session_id,
                    "prompt": self.prompt_blocks(&input).await?,
                });
                self.steer_id = Some(self.wire.request("_session/steering", params).await?);
            }
            DriverCommand::Answer { request, answer } => self.answer(request, answer).await?,
            DriverCommand::Cancel => {
                // A turn still waiting behind an effort switch never reached
                // the agent; it just ends here.
                if self.held_prompt.take().is_some() {
                    return self
                        .emit(DriverEvent::TurnEnded(StopReason::Cancelled))
                        .await;
                }
                // Cancel first, then unblock pending wire requests: an agent
                // parked on a permission resumes only after its response, and
                // it must already know the turn is cancelled by then.
                self.wire
                    .notify("session/cancel", json!({ "sessionId": self.session_id }))
                    .await?;
                for (_, pending) in std::mem::take(&mut self.permissions) {
                    self.wire
                        .respond(
                            pending.wire_id,
                            json!({ "outcome": { "outcome": "cancelled" } }),
                        )
                        .await?;
                }
                for (_, pending) in std::mem::take(&mut self.questions) {
                    self.wire
                        .respond(pending.wire_id, json!({ "outcome": "cancelled" }))
                        .await?;
                }
            }
            DriverCommand::Configure(id, value) => {
                if self.kiro && id.as_str() == "effort" {
                    // The `/effort` prompt is a turn of its own: it waits for
                    // the running turn (or switch) to end. Latest wins.
                    if self.prompt_id.is_some() || self.effort_id.is_some() {
                        self.pending_effort = Some(value);
                    } else {
                        self.send_effort(value).await?;
                    }
                    return Ok(());
                }
                let first_class = self
                    .first_class_model
                    .then(|| selected(&self.info, "model"))
                    .flatten();
                let (method, params) =
                    config_call(&self.session_id, &id, &value, first_class.as_deref());
                let wire_id = self.wire.request(method, params).await?;
                self.configs.push((wire_id, id, value));
            }
            // Never advertised: ACP keeps compaction unstable in schema 1.7,
            // and the one agent with a `/compact` command (kiro) runs it
            // asynchronously with no completion the caller can wait on
            // (probed 2026-09-04).
            DriverCommand::Compact => {
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    "compaction is not supported by the ACP adapter",
                )
                .await?;
                self.emit(DriverEvent::TurnEnded(StopReason::Failed {
                    message: "compaction is not supported by the ACP adapter".into(),
                }))
                .await?;
            }
            DriverCommand::Rollback(turns, _) => {
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    format!("rollback({turns}) is not supported by the ACP adapter"),
                )
                .await?;
            }
            DriverCommand::Close => unreachable!("handled in run"),
        }
        Ok(())
    }

    /// Routes one wire frame: session update, permission request, response.
    async fn handle_frame(&mut self, frame: Value) -> Result<(), Gone> {
        let method = frame.get("method").and_then(Value::as_str);
        match method {
            Some("session/update") => self.on_update(frame).await,
            Some("session/request_permission") => self.on_permission(frame).await,
            // Grok extensions (also implemented by T3 Code; wire shapes
            // cross-checked against both consumers).
            Some("_x.ai/ask_user_question" | "x.ai/ask_user_question")
                if frame.get("id").is_some() =>
            {
                self.on_question(frame).await
            }
            Some("_x.ai/session/prompt_complete") => self.on_prompt_complete(&frame).await,
            Some("_x.ai/models/update") => self.on_models_update(&frame).await,
            Some("_x.ai/session_notification")
                if frame["params"]["update"]["sessionUpdate"] == "model_changed" =>
            {
                self.on_model_changed(&frame["params"]["update"]).await
            }
            // Kiro ships its slash commands here instead of in an
            // `availableCommandsUpdate` (probed 2.20.1).
            Some("_kiro.dev/commands/available") => self.on_kiro_commands(&frame).await,
            Some("_kiro.dev/metadata") => self.on_kiro_metadata(&frame).await,
            Some(other) => {
                if frame.get("id").is_some() {
                    let other = other.to_owned();
                    self.wire
                        .respond_error(frame["id"].clone(), -32601, "method not found")
                        .await?;
                    self.diagnostic(
                        DiagnosticLevel::Warning,
                        format!("declined agent request {other}"),
                    )
                    .await
                } else {
                    // Extension notification: surfaced, not interpreted.
                    let mut extensions = Extensions::new();
                    extensions.insert(other.to_owned(), frame["params"].clone());
                    self.emit(DriverEvent::Event {
                        kind: EventKind::Diagnostic(Diagnostic {
                            level: DiagnosticLevel::Info,
                            message: format!("extension notification {other}"),
                        }),
                        parent_tool_id: None,
                        extensions,
                    })
                    .await
                }
            }
            None => self.on_response(frame).await,
        }
    }

    /// A typed session update, or a raw fallback that loses nothing.
    async fn on_update(&mut self, frame: Value) -> Result<(), Gone> {
        let params = frame.get("params").cloned().unwrap_or_default();
        // Kiro's "Effort set to <level>" chunk is the switch's ack, not
        // content; every other update still flows.
        if self.effort_id.is_some() && params["update"]["sessionUpdate"] == "agent_message_chunk" {
            return Ok(());
        }
        match serde_json::from_value::<acp::SessionNotification>(params.clone()) {
            Ok(notification) => self.translate(notification).await,
            Err(e) => {
                let kind = params["update"]["sessionUpdate"]
                    .as_str()
                    .unwrap_or("?")
                    .to_owned();
                let mut extensions = Extensions::new();
                extensions.insert("acp/raw_update".into(), params["update"].clone());
                self.emit(DriverEvent::Event {
                    kind: EventKind::Diagnostic(Diagnostic {
                        level: DiagnosticLevel::Info,
                        message: format!("unrecognized ACP update `{kind}`: {e}"),
                    }),
                    parent_tool_id: None,
                    extensions,
                })
                .await
            }
        }
    }

    /// One `SessionUpdate` becomes one driver event (or an info change).
    async fn translate(&mut self, notification: acp::SessionNotification) -> Result<(), Gone> {
        use acp::SessionUpdate as U;
        let extensions = ext(notification.meta);
        let kind = match notification.update {
            U::AgentMessageChunk(chunk) => text_kind(chunk, |message_id, text| {
                EventKind::TextDelta { message_id, text }
            }),
            U::AgentThoughtChunk(chunk) => text_kind(chunk, |message_id, text| {
                EventKind::ReasoningDelta { message_id, text }
            }),
            U::UserMessageChunk(chunk) => text_kind(chunk, |message_id, text| {
                EventKind::UserMessage { message_id, text }
            }),
            U::ToolCall(call) => {
                let tool = fresh_tool(call);
                self.tools.insert(tool.id.as_str().to_owned(), tool.clone());
                Some(EventKind::ToolUpdated(tool))
            }
            U::ToolCallUpdate(update) => Some(EventKind::ToolUpdated(self.merge_tool(update))),
            U::Plan(plan) => Some(EventKind::PlanUpdated {
                entries: plan.entries.into_iter().map(plan_entry).collect(),
            }),
            U::UsageUpdate(usage) => Some(EventKind::ContextUsage {
                used_tokens: usage.used,
                window_tokens: Some(usage.size),
                cost_usd: usage.cost.as_ref().map(|c| c.amount),
            }),
            U::AvailableCommandsUpdate(update) => {
                let commands = update
                    .available_commands
                    .into_iter()
                    .map(|c| SlashCommand {
                        name: c.name,
                        description: c.description,
                        input_hint: None,
                    })
                    .collect();
                return self.set_commands(commands).await;
            }
            U::CurrentModeUpdate(update) => {
                let changed = crate::adapter::apply_selection(
                    &mut self.info,
                    &ConfigId::new("mode"),
                    &ConfigValue::Text(update.current_mode_id.0.to_string()),
                );
                if changed {
                    return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
                }
                return Ok(());
            }
            U::ConfigOptionUpdate(update) => {
                self.info.details.config_options.clear();
                self.info.configuration.options.clear();
                apply_session_config(&mut self.info, None, Some(&update.config_options));
                return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
            }
            U::SessionInfoUpdate(update) => {
                update.title.update_to(&mut self.info.title);
                return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
            }
            _ => Some(EventKind::Diagnostic(Diagnostic {
                level: DiagnosticLevel::Info,
                message: "unhandled ACP update kind".into(),
            })),
        };
        if let Some(kind) = kind {
            self.emit(DriverEvent::Event {
                kind,
                parent_tool_id: None,
                extensions,
            })
            .await?;
        }
        Ok(())
    }

    /// `session/request_permission`: remember the wire id and option map,
    /// surface a typed request.
    async fn on_permission(&mut self, frame: Value) -> Result<(), Gone> {
        let wire_id = frame["id"].clone();
        let request: acp::RequestPermissionRequest =
            match serde_json::from_value(frame["params"].clone()) {
                Ok(r) => r,
                Err(e) => {
                    self.wire
                        .respond_error(wire_id, -32602, "unparseable request")
                        .await?;
                    return self
                        .diagnostic(
                            DiagnosticLevel::Warning,
                            format!("bad permission request: {e}"),
                        )
                        .await;
                }
            };
        // The raw JSON-RPC id (number or string) keeps distinct requests distinct.
        let id = RequestId::new(format!("r{wire_id}"));
        let options: Vec<(PermissionChoice, String)> = request
            .options
            .iter()
            .map(|o| (permission_choice(&o.kind), o.option_id.0.to_string()))
            .collect();
        let tool = self.merge_tool(request.tool_call);
        self.permissions.insert(
            id.clone(),
            PendingPermission {
                wire_id,
                options: options.clone(),
            },
        );
        self.emit(DriverEvent::event(EventKind::RequestOpened(
            Request::Permission(PermissionRequest {
                id,
                tool,
                options: options.into_iter().map(|(choice, _)| choice).collect(),
                detail: None,
            }),
        )))
        .await
    }

    /// Grok's `_x.ai/ask_user_question` extension request: typed questions
    /// the agent blocks on (params sometimes arrive wrapped as
    /// `{method, params}`; both shapes are in the field).
    async fn on_question(&mut self, frame: Value) -> Result<(), Gone> {
        let wire_id = frame["id"].clone();
        let params = &frame["params"];
        let params = params.get("params").unwrap_or(params);
        let Some(list) = params.get("questions").and_then(Value::as_array).cloned() else {
            self.wire
                .respond_error(wire_id, -32602, "unparseable request")
                .await?;
            return self
                .diagnostic(DiagnosticLevel::Warning, "bad ask_user_question request")
                .await;
        };
        let questions = list.iter().map(typed_question).collect();
        let id = RequestId::new(format!("r{wire_id}"));
        self.questions.insert(
            id.clone(),
            PendingQuestion {
                wire_id,
                questions: list,
            },
        );
        self.emit(DriverEvent::event(EventKind::RequestOpened(
            Request::Question(QuestionRequest { id, questions }),
        )))
        .await
    }

    /// Grok's AUTHORITATIVE turn end: its `session/prompt` RPC can hang after
    /// the turn really finished. Guards: session match, an outstanding
    /// prompt, and (when present) an exact promptId echo — a stale replay of
    /// an earlier prompt must never end a newer turn. The abandoned RPC
    /// response arrives later and is ignored (its id no longer matches).
    async fn on_prompt_complete(&mut self, frame: &Value) -> Result<(), Gone> {
        let params = &frame["params"];
        let session_matches = params["sessionId"].as_str() == Some(self.session_id.as_str());
        let pid = params["promptId"].as_str();
        let fresh = pid.is_none() || pid == self.prompt_meta.as_deref();
        if self.prompt_id.is_none() || !session_matches || !fresh {
            return Ok(());
        }
        self.prompt_id = None;
        self.prompt_meta = None;
        self.tools.clear();
        let stop = params["stopReason"].as_str().unwrap_or("end_turn");
        self.emit(DriverEvent::TurnEnded(stop_reason(
            &json!({ "result": { "stopReason": stop } }),
        )))
        .await
    }

    /// Kiro's command list, in the same place a standard `availableCommands`
    /// update would land.
    async fn on_kiro_commands(&mut self, frame: &Value) -> Result<(), Gone> {
        let params = &frame["params"];
        if params["sessionId"].as_str() != Some(self.session_id.as_str()) {
            return Ok(());
        }
        let Some(commands) = params["commands"].as_array() else {
            return Ok(());
        };
        let commands = commands
            .iter()
            .filter_map(|c| {
                Some(SlashCommand {
                    name: c["name"].as_str()?.to_owned(),
                    description: c["description"].as_str().unwrap_or_default().to_owned(),
                    input_hint: c["meta"]["hint"]
                        .as_str()
                        .filter(|h| !h.is_empty())
                        .map(str::to_owned),
                })
            })
            .collect();
        self.set_commands(commands).await
    }

    /// Grok republishes its models state after a switch, with the new
    /// model's effort levels.
    async fn on_models_update(&mut self, frame: &Value) -> Result<(), Gone> {
        if !self.first_class_model {
            return Ok(());
        }
        sync_first_class_models(&mut self.info, &frame["params"]);
        self.emit(DriverEvent::InfoChanged(self.info.clone())).await
    }

    /// Grok confirms a model or effort switch with `model_changed`; it is
    /// the one frame that reports the effort actually in force.
    async fn on_model_changed(&mut self, update: &Value) -> Result<(), Gone> {
        let mut changed = false;
        for (id, key) in [("model", "model_id"), ("effort", "reasoning_effort")] {
            if let Some(value) = update[key].as_str() {
                let value = ConfigValue::Text(value.to_owned());
                changed |=
                    crate::adapter::apply_selection(&mut self.info, &ConfigId::new(id), &value);
            }
        }
        if changed {
            return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
        }
        Ok(())
    }

    /// Kiro's per-turn metadata carries the current effort level.
    async fn on_kiro_metadata(&mut self, frame: &Value) -> Result<(), Gone> {
        let params = &frame["params"];
        if params["sessionId"].as_str() != Some(self.session_id.as_str()) {
            return Ok(());
        }
        let Some(effort) = params["effort"].as_str() else {
            return Ok(());
        };
        let value = ConfigValue::Text(effort.to_owned());
        if !offers(&self.info, "effort", &value) {
            return Ok(());
        }
        let changed =
            crate::adapter::apply_selection(&mut self.info, &ConfigId::new("effort"), &value);
        if changed {
            return self.emit(DriverEvent::InfoChanged(self.info.clone())).await;
        }
        Ok(())
    }

    /// Adopts a new command list and republishes the advertised details.
    async fn set_commands(&mut self, commands: Vec<SlashCommand>) -> Result<(), Gone> {
        self.info.details.commands = commands;
        self.emit(DriverEvent::InfoChanged(self.info.clone())).await
    }

    /// The prompt response ends the turn; the steering response reports back.
    async fn on_response(&mut self, frame: Value) -> Result<(), Gone> {
        let Some(id) = frame.get("id").and_then(Value::as_u64) else {
            return Ok(());
        };
        if Some(id) == self.prompt_id {
            self.prompt_id = None;
            self.tools.clear();
            // A switch queued behind this turn goes out before the turn is
            // reported over, however it ended, so the next prompt lines up
            // behind it.
            if let Some(value) = self.pending_effort.take() {
                self.send_effort(value).await?;
            }
            // An errored prompt fails the turn; the auth code means the
            // credentials died and the engine should close the session.
            if let Some(error) = frame.get("error") {
                let code = error["code"].as_i64().unwrap_or_default();
                let message = error["message"]
                    .as_str()
                    .unwrap_or("turn failed")
                    .to_owned();
                let ev = if code == AUTH_REQUIRED_CODE && !self.login.is_empty() {
                    DriverEvent::AuthLost {
                        login: self.login.clone(),
                    }
                } else {
                    DriverEvent::TurnEnded(StopReason::Failed {
                        message: format!("{message} ({code})"),
                    })
                };
                return self.emit(ev).await;
            }
            return self.emit(DriverEvent::TurnEnded(stop_reason(&frame))).await;
        }
        if Some(id) == self.steer_id {
            self.steer_id = None;
            let accepted = frame["result"]["accepted"].as_bool().unwrap_or(false);
            return self.emit(DriverEvent::Steered(accepted)).await;
        }
        if let Some(at) = self.configs.iter().position(|(c, _, _)| *c == id) {
            let (_, config_id, value) = self.configs.remove(at);
            if let Some(error) = frame.get("error") {
                let message = error["message"].as_str().unwrap_or("rejected");
                self.diagnostic(
                    DiagnosticLevel::Warning,
                    format!("agent rejected configure `{config_id}`: {message}"),
                )
                .await?;
            } else if crate::adapter::apply_selection(&mut self.info, &config_id, &value) {
                // Kiro's effort choices follow the model.
                if self.kiro && config_id.as_str() == "model" {
                    sync_effort(&mut self.info);
                }
                self.emit(DriverEvent::InfoChanged(self.info.clone()))
                    .await?;
            }
            if Some(id) == self.effort_id.take() {
                return self.after_effort().await;
            }
        }
        Ok(())
    }

    /// Sends a prompt as the running turn.
    async fn send_prompt(&mut self, params: Value, pid: String) -> Result<(), Gone> {
        self.prompt_id = Some(self.wire.request("session/prompt", params).await?);
        self.prompt_meta = Some(pid);
        Ok(())
    }

    /// Runs kiro's `/effort <level>` prompt as a configure in flight.
    async fn send_effort(&mut self, value: ConfigValue) -> Result<(), Gone> {
        let params = effort_prompt(&self.session_id, &value);
        let wire_id = self.wire.request("session/prompt", params).await?;
        self.effort_id = Some(wire_id);
        self.configs.push((wire_id, ConfigId::new("effort"), value));
        Ok(())
    }

    /// After an effort switch: a newer switch goes first, then any held turn.
    async fn after_effort(&mut self) -> Result<(), Gone> {
        if let Some(value) = self.pending_effort.take() {
            return self.send_effort(value).await;
        }
        if let Some((params, pid)) = self.held_prompt.take() {
            return self.send_prompt(params, pid).await;
        }
        Ok(())
    }

    /// Answers one stored permission or question request on the wire.
    async fn answer(
        &mut self,
        request: RequestId,
        answer: crate::event::Answer,
    ) -> Result<(), Gone> {
        if let Some(pending) = self.permissions.remove(&request) {
            let outcome = match answer {
                crate::event::Answer::Permission(choice) => option_for(choice, &pending.options)
                    .map(|option_id| json!({ "outcome": "selected", "optionId": option_id })),
                crate::event::Answer::Question(_) => None,
            };
            let outcome = outcome.unwrap_or(json!({ "outcome": "cancelled" }));
            self.wire
                .respond(pending.wire_id, json!({ "outcome": outcome }))
                .await?;
            return Ok(());
        }
        let Some(pending) = self.questions.remove(&request) else {
            return Ok(());
        };
        let response = match answer {
            crate::event::Answer::Question(answers) => {
                question_response(&pending.questions, &answers)
            }
            crate::event::Answer::Permission(_) => None,
        };
        let response = response.unwrap_or(json!({ "outcome": "cancelled" }));
        self.wire.respond(pending.wire_id, response).await?;
        Ok(())
    }

    /// Applies a partial wire update to the cumulative snapshot.
    fn merge_tool(&mut self, update: acp::ToolCallUpdate) -> ToolUpdate {
        let tool = self
            .tools
            .entry(update.tool_call_id.0.to_string())
            .or_insert_with(|| blank_tool(update.tool_call_id.0.as_ref()));
        apply_fields(tool, update.fields);
        tool.clone()
    }

    /// Content blocks for one prompt: inlined images first (when the agent
    /// takes them), then the text carrying every attachment's path ref.
    async fn prompt_blocks(&mut self, input: &Input) -> Result<Value, Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.as_deref()) {
            self.diagnostic(DiagnosticLevel::Warning, problem.to_owned())
                .await?;
        }
        let mut blocks = Vec::new();
        if self.info.details.capabilities.supports(Capability::Images) {
            for image in loaded.iter().filter_map(|l| l.image.as_ref()) {
                blocks.push(json!({
                    "type": "image",
                    "data": image.base64,
                    "mimeType": image.mime,
                }));
            }
        }
        blocks.push(json!({
            "type": "text",
            "text": attach::with_refs(input.as_text(), &loaded),
        }));
        Ok(Value::Array(blocks))
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

/// Text-bearing chunk to an event; non-text content is dropped for now (P1).
fn text_kind(
    chunk: acp::ContentChunk,
    make: impl FnOnce(MessageId, String) -> EventKind,
) -> Option<EventKind> {
    let message_id = chunk
        .message_id
        .map(|id| MessageId::new(id.0.as_ref()))
        .unwrap_or_else(|| MessageId::new("m0"));
    match chunk.content {
        acp::ContentBlock::Text(text) => Some(make(message_id, text.text)),
        _ => None,
    }
}

fn fresh_tool(call: acp::ToolCall) -> ToolUpdate {
    let mut tool = blank_tool(call.tool_call_id.0.as_ref());
    tool.kind = tool_kind(call.kind);
    tool.status = tool_status(call.status);
    tool.title = call.title;
    tool.locations = call.locations.into_iter().map(|l| l.path).collect();
    apply_content(&mut tool, call.content);
    apply_raw_input(&mut tool, call.raw_input);
    tool
}

fn blank_tool(id: &str) -> ToolUpdate {
    ToolUpdate {
        id: ToolId::new(id),
        kind: ToolKind::Other,
        title: String::new(),
        status: ToolStatus::Pending,
        input: ToolInput::None,
        output: None,
        diffs: Vec::new(),
        locations: Vec::new(),
        raw: None,
    }
}

fn apply_fields(tool: &mut ToolUpdate, fields: acp::ToolCallUpdateFields) {
    if let Some(kind) = fields.kind {
        tool.kind = tool_kind(kind);
    }
    if let Some(status) = fields.status {
        tool.status = tool_status(status);
    }
    if let Some(title) = fields.title {
        tool.title = title;
    }
    if let Some(locations) = fields.locations {
        tool.locations = locations.into_iter().map(|l| l.path).collect();
    }
    if let Some(content) = fields.content {
        apply_content(tool, content);
    }
    apply_raw_input(tool, fields.raw_input);
}

/// Diff items become `diffs`; text items append to `output`.
fn apply_content(tool: &mut ToolUpdate, content: Vec<acp::ToolCallContent>) {
    for item in content {
        match item {
            acp::ToolCallContent::Diff(diff) => tool.diffs.push(FileDiff {
                path: diff.path,
                old_text: diff.old_text,
                new_text: diff.new_text,
            }),
            acp::ToolCallContent::Content(content) => {
                if let acp::ContentBlock::Text(text) = content.content {
                    tool.output.get_or_insert_default().push_str(&text.text);
                }
            }
            _ => {}
        }
    }
}

fn apply_raw_input(tool: &mut ToolUpdate, raw_input: Option<Value>) {
    if let Some(input) = raw_input {
        tool.raw = Some(RawTool {
            name: String::new(),
            input,
        });
    }
}

fn tool_kind(kind: acp::ToolKind) -> ToolKind {
    use acp::ToolKind as K;
    match kind {
        K::Read => ToolKind::Read,
        K::Edit => ToolKind::Edit,
        K::Delete => ToolKind::Delete,
        K::Move => ToolKind::Move,
        K::Search => ToolKind::Search,
        K::Execute => ToolKind::Execute,
        K::Think => ToolKind::Think,
        K::Fetch => ToolKind::Fetch,
        _ => ToolKind::Other,
    }
}

fn tool_status(status: acp::ToolCallStatus) -> ToolStatus {
    use acp::ToolCallStatus as S;
    match status {
        S::Pending => ToolStatus::Pending,
        S::InProgress => ToolStatus::Running,
        S::Completed => ToolStatus::Completed,
        S::Failed => ToolStatus::Failed,
        _ => ToolStatus::Running,
    }
}

fn plan_entry(entry: acp::PlanEntry) -> PlanEntry {
    use acp::PlanEntryStatus as S;
    PlanEntry {
        text: entry.content,
        status: match entry.status {
            S::Pending => PlanStatus::Pending,
            S::InProgress => PlanStatus::InProgress,
            S::Completed => PlanStatus::Completed,
            _ => PlanStatus::Pending,
        },
    }
}

fn permission_choice(kind: &acp::PermissionOptionKind) -> PermissionChoice {
    use acp::PermissionOptionKind as K;
    match kind {
        K::AllowOnce => PermissionChoice::AllowOnce,
        K::AllowAlways => PermissionChoice::AllowAlways,
        K::RejectOnce => PermissionChoice::DenyOnce,
        K::RejectAlways => PermissionChoice::DenyAlways,
        _ => PermissionChoice::DenyOnce,
    }
}

/// The offered option for a choice: exact match, else same allow/deny family.
fn option_for(choice: PermissionChoice, options: &[(PermissionChoice, String)]) -> Option<String> {
    let allow = |c: &PermissionChoice| {
        matches!(
            c,
            PermissionChoice::AllowOnce | PermissionChoice::AllowAlways
        )
    };
    options
        .iter()
        .find(|(c, _)| *c == choice)
        .or_else(|| options.iter().find(|(c, _)| allow(c) == allow(&choice)))
        .map(|(_, id)| id.clone())
}

/// One raw grok question object as the typed [`Question`]. Ids fall back to
/// the question/label text (grok marks them optional).
fn typed_question(q: &Value) -> Question {
    let text = q["question"].as_str().unwrap_or_default();
    Question {
        id: QuestionId::new(q["id"].as_str().unwrap_or(text)),
        text: text.to_owned(),
        header: None,
        choices: q["options"]
            .as_array()
            .map(|options| {
                options
                    .iter()
                    .map(|o| {
                        let label = o["label"].as_str().unwrap_or_default();
                        Choice {
                            id: ChoiceId::new(o["id"].as_str().unwrap_or(label)),
                            label: label.to_owned(),
                            description: o["description"].as_str().map(str::to_owned),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
        multi_select: q["multiSelect"].as_bool().unwrap_or(false),
        allows_free_text: false,
    }
}

/// Grok's accepted answer: `answers` keyed by question text, values the
/// selected option LABELS (choice ids map back through the raw options).
/// `None` (mismatched shapes) degrades to `cancelled` — never a fabricated
/// answer.
fn question_response(questions: &[Value], answers: &[QuestionAnswer]) -> Option<Value> {
    if answers.len() != questions.len() {
        return None;
    }
    let empty = Vec::new();
    let mut map = serde_json::Map::new();
    for (q, answer) in questions.iter().zip(answers) {
        let options = q["options"].as_array().unwrap_or(&empty);
        let labels: Vec<Value> = match answer {
            QuestionAnswer::Choices(ids) => ids
                .iter()
                .filter_map(|id| {
                    options.iter().find(|o| {
                        o["id"].as_str() == Some(id.as_str())
                            || o["label"].as_str() == Some(id.as_str())
                    })
                })
                .filter_map(|o| o["label"].as_str())
                .map(|label| Value::String(label.to_owned()))
                .collect(),
            QuestionAnswer::Text(text) => vec![Value::String(text.clone())],
        };
        map.insert(
            q["question"].as_str().unwrap_or_default().to_owned(),
            Value::Array(labels),
        );
    }
    Some(json!({ "outcome": "accepted", "answers": map }))
}

fn stop_reason(frame: &Value) -> StopReason {
    match frame["result"]["stopReason"].as_str().unwrap_or_default() {
        "end_turn" => StopReason::Completed {
            source: CompletionSource::Protocol,
        },
        "cancelled" => StopReason::Cancelled,
        "refusal" => StopReason::Refused,
        other => StopReason::Failed {
            message: format!("turn stopped: {other}"),
        },
    }
}

fn ext(meta: Option<acp::Meta>) -> Extensions {
    meta.map(|m| m.into_iter().collect()).unwrap_or_default()
}

fn parse<T: serde::de::DeserializeOwned>(value: Value, what: &str) -> Result<T, AgentError> {
    serde_json::from_value(value).map_err(|e| AgentError::ProtocolFailed(format!("{what}: {e}")))
}

// ---------------------------------------------------------------------------
// Wire: line-delimited JSON-RPC over the child's stdio
// ---------------------------------------------------------------------------

struct Wire {
    stdin: ChildStdin,
    /// All frames the reader task saw, bounded; pipe backpressure beyond.
    frames: mpsc::Receiver<Value>,
    next_id: u64,
    recorder: Option<WireRecorder>,
}

impl Wire {
    /// Takes the child's stdio and starts the line-reader task.
    fn over(child: &mut process::Child, recorder: Option<WireRecorder>) -> Self {
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

    /// Sends a request and returns its id; the response arrives in `frames`.
    async fn request(&mut self, method: &str, params: Value) -> std::io::Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;
        Ok(id)
    }

    async fn notify(&mut self, method: &str, params: Value) -> std::io::Result<()> {
        self.write(json!({ "jsonrpc": "2.0", "method": method, "params": params }))
            .await
    }

    async fn respond(&mut self, id: Value, result: Value) -> std::io::Result<()> {
        self.write(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
            .await
    }

    async fn respond_error(&mut self, id: Value, code: i64, message: &str) -> std::io::Result<()> {
        self.write(
            json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }),
        )
        .await
    }

    /// Handshake only: sends a request and blocks on its response, skipping
    /// unrelated frames (startup notifications, resume replay).
    async fn roundtrip(&mut self, method: &str, params: Value) -> Result<Value, WireError> {
        let id = self
            .request(method, params)
            .await
            .map_err(|_| WireError::Closed)?;
        loop {
            let frame = self.frames.recv().await.ok_or(WireError::Closed)?;
            if frame.get("method").is_none() && frame.get("id").and_then(Value::as_u64) == Some(id)
            {
                if let Some(error) = frame.get("error") {
                    // `data` rides along, bounded: agents put the useful words
                    // there (hermes's "No LLM provider configured" is in
                    // `data`), but it can also be an arbitrary blob.
                    let mut message = error["message"].as_str().unwrap_or("error").to_owned();
                    if let Some(data) = frame["error"].get("data").filter(|d| !d.is_null()) {
                        message =
                            format!("{message}: {}", crate::adapter::cap(data.to_string(), 500));
                    }
                    return Err(WireError::Rpc {
                        code: error["code"].as_i64().unwrap_or_default(),
                        message,
                    });
                }
                return Ok(frame.get("result").cloned().unwrap_or_default());
            }
        }
    }

    async fn write(&mut self, frame: Value) -> std::io::Result<()> {
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
    Rpc { code: i64, message: String },
}

impl WireError {
    /// Handshake failure to a caller error; the auth code becomes
    /// `AuthRequired` with runnable login methods.
    fn into_error(self, methods: &[acp::AuthMethod], exe: &std::path::Path) -> AgentError {
        match self {
            WireError::Closed => AgentError::ProtocolFailed("agent closed the wire".into()),
            WireError::Rpc {
                code: AUTH_REQUIRED_CODE,
                message,
            } => {
                let login: Vec<_> = methods
                    .iter()
                    .filter_map(|m| login_method(m, exe))
                    .collect();
                if login.is_empty() {
                    // No runnable method to offer (agent-driven auth is P2, and
                    // seen in the field: gemini's untyped methods, or a shutdown
                    // notice behind the auth code) — the agent's own words are
                    // more useful than a bare "needs login".
                    AgentError::ProtocolFailed(message)
                } else {
                    AgentError::AuthRequired { login }
                }
            }
            WireError::Rpc { code, message } => {
                AgentError::ProtocolFailed(format!("{message} ({code})"))
            }
        }
    }
}

/// Maps an agent's own logged-out error to `AuthRequired` using the
/// profile's probed fingerprints (kiro exits before speaking ACP, hermes
/// fails session/new with a plain internal error); other failures pass.
fn auth_hinted(
    error: AgentError,
    profile: Option<&crate::catalog::AgentProfile>,
    exe: &std::path::Path,
) -> AgentError {
    let Some(profile) = profile else { return error };
    // Only failure shapes carry the agent's own words; typed errors
    // (AuthRequired, UnsupportedFeature, …) must pass through untouched.
    if !matches!(
        error,
        AgentError::ProtocolFailed(_) | AgentError::ProcessExited { .. }
    ) {
        return error;
    }
    let text = error.to_string();
    if !profile.auth_error_hints.iter().any(|h| text.contains(h)) {
        return error;
    }
    let mut login = crate::discovery::login_methods(profile, exe);
    // No login command in the catalog (grok): the agent's own TUI is the flow.
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
    AgentError::AuthRequired { login }
}

/// A terminal auth method becomes a runnable command; agent-driven auth
/// belongs to `Runtime::login` (P2). Qwen predates the typed variant and
/// advertises `{type: "terminal", args}` inside `_meta` (probed 0.22.0) —
/// read that shape too.
fn login_method(method: &acp::AuthMethod, exe: &std::path::Path) -> Option<LoginMethod> {
    let (name, args, env) = match method {
        acp::AuthMethod::Terminal(terminal) => (
            &terminal.name,
            terminal.args.to_vec(),
            terminal
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        acp::AuthMethod::Agent(agent) => {
            let meta = agent.meta.as_ref()?;
            if meta.get("type").and_then(Value::as_str) != Some("terminal") {
                return None;
            }
            let args = meta.get("args").and_then(Value::as_array)?;
            (
                &agent.name,
                args.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect(),
                std::collections::BTreeMap::new(),
            )
        }
        _ => return None,
    };
    let mut command = vec![exe.to_string_lossy().into_owned()];
    command.extend(args);
    Some(LoginMethod::Terminal {
        description: name.clone(),
        command,
        env,
    })
}
