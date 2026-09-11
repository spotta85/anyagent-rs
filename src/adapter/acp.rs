//! ACP adapter: drives any ACP v1 agent over stdio, with typed parsing per
//! frame and a raw fallback; the engine owns all turn rules.
//!
//! High level: `connect` → spawn + `handshake` (`initialize`, `session/new`
//! or `session/load`, creation-time config) → `driver_info`; then
//! `Drive::run` turns commands into requests (`handle_command`) and
//! notifications into events (`handle_frame`, `translate`, `on_*`).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use agent_client_protocol_schema::v1 as acp;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, CLOSE_GRACE, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
    Emitter, FRAME_BUFFER, Gone, HANDSHAKE_TIMEOUT, LineWire, WireRecorder, attach, level_choices,
    offers, plan_entries, selected, set_effort_option, set_select_option,
};
use crate::agent::{
    AccountInfo, AgentDetails, AgentInstallation, AuthKind, AuthStatus, Capabilities, Capability,
    ConfigChoice, ConfigId, ConfigKind, ConfigOption, ConfigValue, Input, LoginMethod,
    McpConnection, McpServer, McpTransport, ResumeToken, SessionConfiguration, SessionStart,
    SlashCommand,
};
use crate::error::AgentError;
use crate::event::{
    Choice, ChoiceId, CompletionSource, Diagnostic, DiagnosticLevel, EventKind, Extensions,
    FileDiff, MessageId, PermissionChoice, PermissionRequest, PlanEntry, PlanStatus, Question,
    QuestionAnswer, QuestionId, QuestionRequest, RawTool, Request, RequestId, StopReason, ToolId,
    ToolInput, ToolKind, ToolStatus, ToolUpdate,
};
use crate::process::{self, Spawn};

const AUTH_REQUIRED_CODE: i64 = -32000;
/// Adopting an existing login is ~2 s; a browser flow would never return.
const ADOPT_LOGIN_WAIT: Duration = Duration::from_secs(10);

/// One instance per ACP agent; `args` put the CLI in protocol mode.
pub(crate) struct AcpAdapter {
    args: Vec<String>,
    /// Catalog facts for auth truth (open-proves-auth, logged-out
    /// fingerprints); `None` for an ad-hoc ACP install.
    profile: Option<&'static crate::catalog::AgentProfile>,
}

impl AcpAdapter {
    /// Explicit protocol args, with the catalog's auth facts when the agent
    /// has an entry (an ACP upgrade) and none for an ad-hoc install.
    pub(crate) fn with_args(
        profile: Option<&'static crate::catalog::AgentProfile>,
        args: Vec<String>,
    ) -> Self {
        Self { args, profile }
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
        let events = Emitter::new(ev_tx);
        let recorder = WireRecorder::for_session(&request.options, &events).await;
        // Cursor's version and login come from `about` (0.5s): its ACP
        // `authenticate` opens a browser login when logged out.
        let about = match self.profile {
            Some(p) if p.id == "cursor" => {
                cursor_about(&request.installation, &request.options).await?
            }
            _ => CursorAbout::default(),
        };
        let mut child = process::spawn(Spawn {
            exec_path: request.installation.executable_path.clone(),
            args: self.args.clone(),
            cwd: request.options.cwd().clone(),
            env: env.clone(),
        })
        .await?;
        let mut wire = Wire::over(&mut child, recorder);

        // ACP never says which credential opened the session: a documented
        // API key in the env is taken as the one in use, else the catalog's
        // proven kind.
        let open_auth_kind = self.profile.and_then(|p| {
            let keyed = p
                .api_key_env
                .iter()
                .any(|var| std::env::var(var).is_ok_and(|v| !v.trim().is_empty()));
            p.open_auth_kind
                .clone()
                .map(|kind| if keyed { AuthKind::ApiKey } else { kind })
        });
        let adopt_login = self
            .profile
            .and_then(|p| p.upgrade.as_ref())
            .and_then(|u| u.acp_auth_method);
        let handshake = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            handshake(&mut wire, &request, open_auth_kind, adopt_login),
        );
        let Handshake {
            mut info,
            session_id,
            login,
            first_class_model,
            kiro,
            antigravity,
            prompt_media,
            estimate,
        } = match handshake.await {
            Ok(Ok(ok)) => ok,
            Ok(Err(e)) => {
                // Shutdown first: it joins the stderr reader, so the tail is
                // complete before the error is rendered and hint-matched.
                child.shutdown(CLOSE_GRACE).await;
                let e = crate::adapter::with_stderr(e, &child);
                return Err(crate::adapter::auth_hinted(
                    e,
                    self.profile,
                    &request.installation.executable_path,
                    &env,
                ));
            }
            Err(_) => {
                child.shutdown(CLOSE_GRACE).await;
                return Err(AgentError::HandshakeTimeout);
            }
        };

        if about.version.is_some() {
            info.details.version = about.version;
        }
        if let Some(account) = about.account {
            info.details.auth = AuthStatus::Authenticated {
                kind: AuthKind::Subscription,
                account: Some(account),
            };
        }

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        tokio::spawn(
            Drive {
                wire,
                child,
                session_id,
                events,
                info: info.clone(),
                tools: HashMap::new(),
                hidden_tools: HashSet::new(),
                permissions: HashMap::new(),
                questions: HashMap::new(),
                prompt_id: None,
                prompt_meta: None,
                prompt_seq: 0,
                steer_id: None,
                configs: Vec::new(),
                first_class_model,
                kiro,
                antigravity,
                prompt_media,
                estimate,
                usage_chars: 0,
                todos: Vec::new(),
                effort_id: None,
                pending_effort: None,
                held_prompt: None,
                retry: None,
                login: crate::adapter::login_in(login, &env),
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

/// What the handshake settled, for the drive task.
struct Handshake {
    info: DriverInfo,
    session_id: String,
    /// Runnable login methods, for auth failures later in the session.
    login: Vec<LoginMethod>,
    first_class_model: bool,
    kiro: bool,
    antigravity: bool,
    prompt_media: PromptMedia,
    /// A new cursor session: context usage is estimated (it sends none).
    estimate: bool,
}

/// `initialize`, then `session/new` or `session/load`. A `session/new` error
/// with the auth code becomes `AuthRequired` with runnable login methods;
/// the same methods ride along for auth failures later in the session.
async fn handshake(
    wire: &mut Wire,
    request: &ConnectRequest,
    open_auth_kind: Option<AuthKind>,
    adopt_login: Option<&'static str>,
) -> Result<Handshake, AgentError> {
    // `parameterizedModelPicker` makes Cursor list the selected model's own
    // options (effort, fast, thinking, context) in every config response;
    // spec-conformant agents ignore `_meta` (probed 2026-09-07).
    let init = wire
        .roundtrip(
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": { "_meta": { "parameterizedModelPicker": true } },
                "clientInfo": { "name": "anyagent", "version": env!("CARGO_PKG_VERSION") },
            }),
        )
        .await
        .map_err(|e| e.into_error(&[], &request.installation.executable_path))?;
    let init: acp::InitializeResponse = parse(init, "initialize response")?;
    let cursor = is_cursor(&init);
    // A resumed cursor session carries history the wire never replays, so
    // there is no baseline to estimate usage from: no estimate at all.
    let estimate = cursor && matches!(request.options.start, SessionStart::New);
    // Cursor refuses every session call until `authenticate` has read its
    // keychain login (0.8–7s, probed 2026-09-07).
    if cursor {
        wire.roundtrip("authenticate", json!({ "methodId": "cursor_login" }))
            .await
            .map_err(|e| e.into_error(&init.auth_methods, &request.installation.executable_path))?;
    }

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
    let mut response = wire.roundtrip(method, params.clone()).await;
    // An ACP upgrade with its own auth choice (antigravity's server) adopts
    // the CLI's login in-protocol: one `authenticate`, then the same call
    // again. Logged out of the CLI too, the server starts Google's browser
    // login instead (probed 2026-09-07), so the wait is short and the
    // original refusal stands on any other outcome.
    if let (Err(WireError::Rpc { code, .. }), Some(method_id)) = (&response, adopt_login)
        && *code == AUTH_REQUIRED_CODE
    {
        let authenticate = wire.roundtrip("authenticate", json!({ "methodId": method_id }));
        if matches!(
            tokio::time::timeout(ADOPT_LOGIN_WAIT, authenticate).await,
            Ok(Ok(_))
        ) {
            response = wire.roundtrip(method, params).await;
        }
    }
    let response = response
        .map_err(|e| e.into_error(&init.auth_methods, &request.installation.executable_path))?;

    let mut info = driver_info(&init, open_auth_kind);
    let first_class_models = response.get("models").cloned();
    let session_id = if session_id.is_empty() {
        let new: acp::NewSessionResponse = parse(response, "session/new response")?;
        apply_session_config(&mut info, new.modes.as_ref(), new.config_options.as_deref());
        new.session_id.0.to_string()
    } else {
        // A loaded session reports modes and options like a new one (probed
        // opencode 1.18); an agent answering `null` still resumes.
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
    let antigravity = is_antigravity(&init);
    let prompt_media = PromptMedia {
        audio: init.agent_capabilities.prompt_capabilities.audio,
        embedded_context: init.agent_capabilities.prompt_capabilities.embedded_context,
    };
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
        let (method, params) = config_call(
            &session_id,
            id,
            value,
            first_class.as_deref(),
            info.effort_wire.as_deref(),
        );
        let result = wire.roundtrip(method, params).await.map_err(|e| match e {
            WireError::Rpc { message, .. } => {
                AgentError::InvalidConfiguration(format!("agent rejected `{id}`: {message}"))
            }
            e => e.into_error(&init.auth_methods, &request.installation.executable_path),
        })?;
        adopt_config_options(&mut info, &result, first_class_model, kiro);
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
    Ok(Handshake {
        info,
        session_id,
        login,
        first_class_model,
        kiro,
        antigravity,
        prompt_media,
        estimate,
    })
}

/// Google's Antigravity server asks questions as `interaction_*` permissions.
fn is_antigravity(init: &acp::InitializeResponse) -> bool {
    init.agent_info
        .as_ref()
        .is_some_and(|i| i.name == "antigravity-acp")
}

/// Kiro is the one ACP agent with a prompt-driven effort switch.
fn is_kiro(init: &acp::InitializeResponse) -> bool {
    init.agent_info
        .as_ref()
        .is_some_and(|i| i.name.starts_with("Kiro"))
}

/// Cursor is the ACP agent whose login method is `cursor_login`.
fn is_cursor(init: &acp::InitializeResponse) -> bool {
    init.auth_methods
        .iter()
        .any(|m| m.id().0.as_ref() == "cursor_login")
}

/// What `cursor-agent about --format json` knows: the CLI version and the
/// logged-in account (email and subscription tier).
#[derive(Default)]
struct CursorAbout {
    version: Option<String>,
    account: Option<AccountInfo>,
}

/// Runs `about` before the ACP handshake. `userEmail: null` means logged
/// out and fails typed with the catalog login command, since Cursor's own
/// `authenticate` would start a browser login instead (read from the
/// 2026.09.02 bundle). A failed or unparseable `about` reports nothing.
async fn cursor_about(
    installation: &AgentInstallation,
    options: &crate::agent::SessionOptions,
) -> Result<CursorAbout, AgentError> {
    let mut command = tokio::process::Command::new(&installation.executable_path);
    command
        .args(["about", "--format", "json"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let Ok(Ok(output)) = tokio::time::timeout(Duration::from_secs(10), async {
        crate::process::retry_busy(|| command.spawn())
            .await?
            .wait_with_output()
            .await
    })
    .await
    else {
        return Ok(CursorAbout::default());
    };
    let Ok(json) = serde_json::from_slice::<Value>(&output.stdout) else {
        return Ok(CursorAbout::default());
    };
    if json.get("userEmail").is_some_and(Value::is_null) {
        return Err(AgentError::AuthRequired {
            login: crate::adapter::login_methods(installation, Some(options)),
        });
    }
    Ok(CursorAbout {
        version: json["cliVersion"].as_str().map(str::to_owned),
        account: json["userEmail"].as_str().map(|email| AccountInfo {
            email: Some(email.to_owned()),
            plan: json["subscriptionTier"].as_str().map(str::to_owned),
        }),
    })
}

/// A config response's `configOptions` (the full list, per spec) replaces
/// the advertised options; Cursor's includes the selected model's own
/// (probed 2026-09-07). Returns whether anything was adopted.
fn adopt_config_options(
    info: &mut DriverInfo,
    result: &Value,
    first_class: bool,
    kiro: bool,
) -> bool {
    let Some(options) = result.get("configOptions") else {
        return false;
    };
    let Ok(options) = serde_json::from_value::<Vec<acp::SessionConfigOption>>(options.clone())
    else {
        return false;
    };
    replace_config_options(info, &options, first_class, kiro);
    true
}

/// The wire replaces its own options; `mode` (from the mode state),
/// first-class `model`/`effort` (from `models`), and kiro's synthesized
/// `effort` are ours to keep.
fn replace_config_options(
    info: &mut DriverInfo,
    options: &[acp::SessionConfigOption],
    first_class: bool,
    kiro: bool,
) {
    let ours = move |id: &str| id == "mode" || (first_class && matches!(id, "model" | "effort"));
    info.details.config_options.retain(|o| ours(o.id.as_str()));
    info.configuration.options.retain(|id, _| ours(id.as_str()));
    // The new list may name effort differently, or plainly.
    info.effort_wire = None;
    apply_session_config(info, None, Some(options));
    if kiro {
        sync_effort(info);
    }
}

/// Characters a prompt costs for the usage estimate: its text (attachment
/// refs included) plus a flat allowance per inlined image (~1.5k tokens).
fn prompt_chars(blocks: &Value) -> usize {
    const IMAGE_CHARS: usize = 6_000;
    blocks
        .as_array()
        .into_iter()
        .flatten()
        .map(|b| match b["type"].as_str() {
            Some("image") => IMAGE_CHARS,
            _ => b["text"].as_str().map_or(0, str::len),
        })
        .sum()
}

/// Cursor's `context` option value ("300k", "1m") as a token count.
fn context_window(info: &DriverInfo) -> Option<u64> {
    let value = selected(info, "context")?.to_lowercase();
    let digits = value.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let n: u64 = digits.parse().ok()?;
    Some(match &value[digits.len()..] {
        "k" => n * 1_000,
        "m" => n * 1_000_000,
        _ => n,
    })
}

/// Makes kiro's `effort` option match the selected model; kiro advertises
/// nothing for it on the wire (probed 2.20.1).
fn sync_effort(info: &mut DriverInfo) {
    let model = selected(info, "model").unwrap_or_default();
    set_effort_option(info, kiro_effort_choices(&model), selected(info, "effort"));
}

/// The effort levels kiro's models share; empty for the models without
/// effort (kiro-cli 2.20.1's list, probed 2026-09-05).
fn kiro_effort_choices(model: &str) -> Vec<ConfigChoice> {
    const NO_EFFORT: [&str; 9] = [
        "auto",
        "claude-sonnet-4.5",
        "claude-sonnet-4",
        "claude-haiku-4.5",
        "deepseek-3.2",
        "minimax-m2.5",
        "minimax-m2.1",
        "glm-5",
        "qwen3-coder-next",
    ];
    if NO_EFFORT.contains(&model) {
        return Vec::new();
    }
    level_choices(["low", "medium", "high", "xhigh", "max"])
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
/// 2026-09-05); anything else to session/set_config_option under the wire's
/// own id (`effort_wire` when the agent names effort differently).
fn config_call(
    session_id: &str,
    id: &ConfigId,
    value: &ConfigValue,
    first_class: Option<&str>,
    effort_wire: Option<&str>,
) -> (&'static str, Value) {
    let wire_id = match (id.as_str(), effort_wire) {
        ("effort", Some(wire)) => wire,
        (id, _) => id,
    };
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
            json!({ "sessionId": session_id, "configId": wire_id, "value": chosen }),
        ),
        (_, ConfigValue::Bool(on), _) => (
            "session/set_config_option",
            json!({ "sessionId": session_id, "configId": wire_id, "type": "boolean", "value": on }),
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
        .filter(|e| efforts.iter().any(|c| &c.value == e))
        .or_else(|| {
            current
                .and_then(|m| m["_meta"]["reasoningEffort"].as_str())
                .map(str::to_owned)
        });
    set_select_option(
        info,
        "model",
        "Model",
        "model",
        choices,
        current_id.map(str::to_owned),
    );
    set_effort_option(info, efforts, effort);
}

/// What `initialize` tells us, folded into the engine's vocabulary.
fn driver_info(init: &acp::InitializeResponse, open_auth_kind: Option<AuthKind>) -> DriverInfo {
    // Plan and slash commands (sent as prompt text) are core ACP;
    // ContextUsage and Questions are added on first sight, since the
    // handshake does not say whether an agent sends them.
    let mut features = vec![
        Capability::Permissions,
        Capability::Plan,
        Capability::SlashCommands,
    ];
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
    // Grok sends no `agentInfo`; its version rides in `_meta`.
    let meta_version = || {
        let meta = init.meta.as_ref()?;
        Some(meta.get("agentVersion")?.as_str()?.to_owned())
    };
    DriverInfo {
        details: AgentDetails {
            version: init
                .agent_info
                .as_ref()
                .map(|i| i.version.clone())
                .or_else(meta_version),
            // ACP has no auth-status field on the wire: reaching this point
            // proves login for agents that refuse to open logged out
            // (`open_auth_kind`, probed per agent); the rest are unknown.
            auth: match open_auth_kind {
                Some(kind) => AuthStatus::Authenticated {
                    kind,
                    account: None,
                },
                None => AuthStatus::Unknown,
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
        tools_disabled: false,
        effort_wire: None,
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
        // One effort id everywhere: `<x>_effort` (qwen: `reasoning_effort`)
        // is `effort`, its wire id kept for `config_call`. Not by category:
        // cursor lists `thinking` and `effort` both as thought levels.
        let wire_id = option.id.0.as_ref();
        let id = if wire_id.ends_with("_effort") {
            info.effort_wire = Some(wire_id.to_owned());
            "effort"
        } else {
            wire_id
        };
        // Cursor lists `mode` here as well as in the mode state.
        if info
            .details
            .config_options
            .iter()
            .any(|o| o.id.as_str() == id)
        {
            continue;
        }
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
                .insert(ConfigId::new(id), current.clone());
        }
        info.details.config_options.push(ConfigOption {
            id: ConfigId::new(id),
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

/// Select options, grouped or not, as flat choices.
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
// DRIVE TASK: engine commands out, wire frames in
// ---------------------------------------------------------------------------

/// A permission request waiting for `answer`: its wire id and the offered
/// options, so a `PermissionChoice` maps back to the agent's option id.
struct PendingPermission {
    wire_id: Value,
    options: Vec<(PermissionChoice, String)>,
}

/// A question request waiting for `answer`: its wire id and the raw
/// question objects, so choice ids map back to what the agent expects
/// (grok: option labels; cursor: option ids; antigravity: the permission
/// option id).
struct PendingQuestion {
    wire_id: Value,
    questions: Vec<Value>,
    wire: QuestionWire,
}

/// Which agent's question shape a pending question answers in.
#[derive(Clone, Copy, PartialEq)]
enum QuestionWire {
    Grok,
    Cursor,
    /// Antigravity's ACP server: a permission request on an `interaction_*`
    /// tool call, every option `allow_once` with the choice as its name
    /// (recorded 2026-09-07).
    Interaction,
}

/// Prompt content the agent advertised beyond text and images.
#[derive(Clone, Copy)]
struct PromptMedia {
    audio: bool,
    embedded_context: bool,
}

struct Drive {
    wire: Wire,
    child: process::Child,
    session_id: String,
    events: Emitter,
    /// Current advertised state; mutated and re-sent as `InfoChanged`.
    info: DriverInfo,
    /// Cumulative tool snapshots, merged from partial wire updates.
    tools: HashMap<String, ToolUpdate>,
    /// Question tool calls dropped on arrival: their later updates are
    /// dropped too (grok's completion update carries no tag).
    hidden_tools: HashSet<String>,
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
    /// The agent is Antigravity's server: its `interaction_*` permission
    /// requests are questions (see `QuestionWire::Interaction`).
    antigravity: bool,
    /// Which inlined media the agent takes in prompts, from `initialize`.
    prompt_media: PromptMedia,
    /// A new cursor session: context usage is estimated (it sends none).
    estimate: bool,
    /// Characters that crossed the wire this session, for the estimate.
    usage_chars: usize,
    /// Cursor's todo list, kept across `cursor/update_todos` merges.
    todos: Vec<Value>,
    /// Wire id of an in-flight `/effort` prompt; its chunks stay internal.
    effort_id: Option<u64>,
    /// An effort switch requested mid-turn, sent once the turn ends.
    pending_effort: Option<ConfigValue>,
    /// A turn (params, prompt id) that arrived mid-switch, sent once it ends.
    held_prompt: Option<(Value, String)>,
    /// Kiro: the running prompt, re-sent once if it comes back `cancelled`
    /// with nothing said and no cancel of ours — its cancel races the next
    /// prompt (probed 2026-08-27, kiro 2.19.1). Cleared by any content.
    retry: Option<Value>,
    /// Runnable login methods from `initialize`, for mid-session auth loss.
    login: Vec<LoginMethod>,
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
                frame = self.wire.line.frames.recv() => match frame {
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

    /// One engine command as JSON-RPC requests.
    async fn handle_command(&mut self, cmd: DriverCommand) -> Result<(), Gone> {
        match cmd {
            DriverCommand::StartTurn { input } => {
                self.events.send(DriverEvent::TurnAck).await?;
                // `_meta.promptId` lets grok's prompt-complete extension name
                // this exact prompt; spec-conformant agents ignore `_meta`.
                self.prompt_seq += 1;
                let pid = format!("p{}", self.prompt_seq);
                let blocks = self.prompt_blocks(&input).await?;
                self.usage_chars += prompt_chars(&blocks);
                let params = json!({
                    "sessionId": self.session_id,
                    "prompt": blocks,
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
                        .events
                        .send(DriverEvent::TurnEnded(StopReason::Cancelled))
                        .await;
                }
                // Cancel first, then unblock pending wire requests: an agent
                // parked on a permission resumes only after its response, and
                // it must already know the turn is cancelled by then.
                self.retry = None;
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
                        .respond(pending.wire_id, cancelled_question(pending.wire))
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
                    .then(|| {
                        // An effort change must target the latest requested model,
                        // even when its receipt has not reached us yet.
                        self.configs
                            .iter()
                            .rev()
                            .find_map(|(_, id, value)| match (id.as_str(), value) {
                                ("model", ConfigValue::Text(model)) => Some(model.clone()),
                                _ => None,
                            })
                            .or_else(|| selected(&self.info, "model"))
                    })
                    .flatten();
                let (method, params) = config_call(
                    &self.session_id,
                    &id,
                    &value,
                    first_class.as_deref(),
                    self.info.effort_wire.as_deref(),
                );
                let wire_id = self.wire.request(method, params).await?;
                self.configs.push((wire_id, id, value));
            }
            // Never advertised: schema 1.7 keeps compaction unstable, and
            // kiro's `/compact` runs with no completion to wait on.
            DriverCommand::Compact => {
                self.events
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        "compaction is not supported by the ACP adapter",
                    )
                    .await?;
                self.events
                    .send(DriverEvent::TurnEnded(StopReason::Failed {
                        message: "compaction is not supported by the ACP adapter".into(),
                    }))
                    .await?;
            }
            DriverCommand::Rollback(turns, _) => {
                self.events
                    .diagnostic(
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
                self.on_question(frame, false).await
            }
            // Cursor extensions (docs.cursor.com/cli/acp; verified 2026-09-07).
            Some("cursor/ask_question") if frame.get("id").is_some() => {
                self.on_question(frame, true).await
            }
            Some(
                "cursor/create_plan"
                | "cursor/update_todos"
                | "cursor/task"
                | "cursor/generate_image",
            ) => self.on_cursor_extension(frame).await,
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
                    self.events
                        .diagnostic(
                            DiagnosticLevel::Warning,
                            format!("declined agent request {other}"),
                        )
                        .await
                } else {
                    // Extension notification: surfaced, not interpreted.
                    let mut extensions = Extensions::new();
                    extensions.insert(other.to_owned(), frame["params"].clone());
                    self.events
                        .send(DriverEvent::Event {
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
        // Content proves the turn ran: a cancel after it is real.
        if matches!(
            params["update"]["sessionUpdate"].as_str(),
            Some("agent_message_chunk" | "agent_thought_chunk" | "tool_call")
        ) {
            self.retry = None;
        }
        // Kiro's "Effort set to <level>" chunk is the switch's ack, not
        // content; every other update still flows.
        if self.effort_id.is_some() && params["update"]["sessionUpdate"] == "agent_message_chunk" {
            return Ok(());
        }
        // Parsed by reference: the raw value is still needed for the fallback.
        match <acp::SessionNotification as serde::Deserialize>::deserialize(&params) {
            Ok(notification) => self.translate(notification).await,
            Err(e) => {
                let kind = params["update"]["sessionUpdate"]
                    .as_str()
                    .unwrap_or("?")
                    .to_owned();
                let mut extensions = Extensions::new();
                extensions.insert("acp/raw_update".into(), params["update"].clone());
                self.events
                    .send(DriverEvent::Event {
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
            // A question is surfaced by its request alone (antigravity's
            // interaction permission, grok's `_x.ai/ask_user_question`);
            // the tool call around it is wire noise.
            U::ToolCall(call) if self.is_question_tool(&call.tool_call_id.0, &call.meta) => {
                self.hidden_tools.insert(call.tool_call_id.0.to_string());
                None
            }
            U::ToolCallUpdate(update)
                if self.is_question_tool(&update.tool_call_id.0, &update.meta) =>
            {
                None
            }
            U::ToolCall(call) => {
                let tool = fresh_tool(call);
                self.tools.insert(tool.id.as_str().to_owned(), tool.clone());
                Some(EventKind::ToolUpdated(tool))
            }
            U::ToolCallUpdate(update) => {
                // Tool results reach the model as `rawOutput` on cursor.
                let raw = update
                    .fields
                    .raw_output
                    .as_ref()
                    .map_or(0, |v| v.to_string().len());
                let (tool, appended) = self.merge_tool(update);
                self.usage_chars += appended.len() + raw;
                if !appended.is_empty() {
                    self.events
                        .event(EventKind::ToolOutputDelta {
                            tool_id: tool.id.clone(),
                            text: appended,
                        })
                        .await?;
                }
                Some(EventKind::ToolUpdated(tool))
            }
            U::Plan(plan) => Some(EventKind::PlanUpdated {
                entries: plan.entries.into_iter().map(plan_entry).collect(),
            }),
            U::UsageUpdate(usage) => {
                self.advertise(Capability::ContextUsage).await?;
                Some(EventKind::ContextUsage {
                    used_tokens: usage.used,
                    window_tokens: Some(usage.size),
                    cost_usd: usage.cost.as_ref().map(|c| c.amount),
                })
            }
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
                    return self
                        .events
                        .send(DriverEvent::InfoChanged(self.info.clone()))
                        .await;
                }
                return Ok(());
            }
            U::ConfigOptionUpdate(update) => {
                replace_config_options(
                    &mut self.info,
                    &update.config_options,
                    self.first_class_model,
                    self.kiro,
                );
                return self
                    .events
                    .send(DriverEvent::InfoChanged(self.info.clone()))
                    .await;
            }
            U::SessionInfoUpdate(update) => {
                update.title.update_to(&mut self.info.title);
                return self
                    .events
                    .send(DriverEvent::InfoChanged(self.info.clone()))
                    .await;
            }
            _ => Some(EventKind::Diagnostic(Diagnostic {
                level: DiagnosticLevel::Info,
                message: "unhandled ACP update kind".into(),
            })),
        };
        if let Some(kind) = kind {
            if let EventKind::TextDelta { text, .. } | EventKind::ReasoningDelta { text, .. } =
                &kind
            {
                self.usage_chars += text.len();
            }
            self.events
                .send(DriverEvent::Event {
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
                        .events
                        .diagnostic(
                            DiagnosticLevel::Warning,
                            format!("bad permission request: {e}"),
                        )
                        .await;
                }
            };
        if self.is_interaction(&request.tool_call.tool_call_id.0)
            && !request.options.is_empty()
            && request
                .options
                .iter()
                .all(|o| matches!(o.kind, acp::PermissionOptionKind::AllowOnce))
        {
            return self.on_interaction(wire_id, request).await;
        }
        // The raw JSON-RPC id (number or string) keeps distinct requests distinct.
        let id = RequestId::new(format!("r{wire_id}"));
        let options: Vec<(PermissionChoice, String)> = request
            .options
            .iter()
            .map(|o| (permission_choice(&o.kind), o.option_id.0.to_string()))
            .collect();
        let (tool, _) = self.merge_tool(request.tool_call);
        self.permissions.insert(
            id.clone(),
            PendingPermission {
                wire_id,
                options: options.clone(),
            },
        );
        self.events
            .send(DriverEvent::event(EventKind::RequestOpened(
                Request::Permission(PermissionRequest {
                    id,
                    tool,
                    options: options.into_iter().map(|(choice, _)| choice).collect(),
                    detail: None,
                }),
            )))
            .await
    }

    /// Antigravity's `interaction_*` tool calls are its questions; any
    /// other agent's ids are opaque.
    fn is_interaction(&self, tool_call_id: &str) -> bool {
        self.antigravity && tool_call_id.starts_with("interaction_")
    }

    /// The tool call an agent wraps its question in: antigravity's
    /// interaction, or grok's `ask_user_question` (`_meta["x.ai/tool"].kind`
    /// is `ask_user`, wire-captured 2026-09-09).
    fn is_question_tool(&self, tool_call_id: &str, meta: &Option<acp::Meta>) -> bool {
        let kind = meta
            .as_ref()
            .and_then(|m| m.get("x.ai/tool")?.get("kind")?.as_str());
        kind == Some("ask_user")
            || self.hidden_tools.contains(tool_call_id)
            || self.is_interaction(tool_call_id)
    }

    /// Antigravity's question (see `QuestionWire::Interaction`): the title
    /// is the question, one choice per option, the answer its `optionId`.
    async fn on_interaction(
        &mut self,
        wire_id: Value,
        request: acp::RequestPermissionRequest,
    ) -> Result<(), Gone> {
        self.advertise(Capability::Questions).await?;
        let call = &request.tool_call;
        let question = Question {
            id: QuestionId::new(call.tool_call_id.0.to_string()),
            text: call.fields.title.clone().unwrap_or_default(),
            header: None,
            choices: request
                .options
                .iter()
                .map(|o| Choice {
                    id: ChoiceId::new(o.option_id.0.to_string()),
                    label: o.name.clone(),
                    description: None,
                })
                .collect(),
            multi_select: false,
            allows_free_text: false,
        };
        let id = RequestId::new(format!("r{wire_id}"));
        self.questions.insert(
            id.clone(),
            PendingQuestion {
                wire_id,
                questions: request
                    .options
                    .iter()
                    .map(|o| Value::String(o.option_id.0.to_string()))
                    .collect(),
                wire: QuestionWire::Interaction,
            },
        );
        self.events
            .send(DriverEvent::event(EventKind::RequestOpened(
                Request::Question(QuestionRequest {
                    id,
                    questions: vec![question],
                }),
            )))
            .await
    }

    /// Grok's `_x.ai/ask_user_question` or cursor's `cursor/ask_question`:
    /// typed questions the agent blocks on (grok's params sometimes arrive
    /// wrapped as `{method, params}`; both shapes are in the field).
    async fn on_question(&mut self, frame: Value, cursor: bool) -> Result<(), Gone> {
        let wire_id = frame["id"].clone();
        let params = &frame["params"];
        let params = params.get("params").unwrap_or(params);
        let Some(list) = params.get("questions").and_then(Value::as_array).cloned() else {
            self.wire
                .respond_error(wire_id, -32602, "unparseable request")
                .await?;
            return self
                .events
                .diagnostic(DiagnosticLevel::Warning, "bad ask_user_question request")
                .await;
        };
        self.advertise(Capability::Questions).await?;
        let questions = list.iter().map(|q| typed_question(q, cursor)).collect();
        let id = RequestId::new(format!("r{wire_id}"));
        self.questions.insert(
            id.clone(),
            PendingQuestion {
                wire_id,
                questions: list,
                wire: match cursor {
                    true => QuestionWire::Cursor,
                    false => QuestionWire::Grok,
                },
            },
        );
        self.events
            .send(DriverEvent::event(EventKind::RequestOpened(
                Request::Question(QuestionRequest { id, questions }),
            )))
            .await
    }

    /// Cursor's other extension methods. They arrive as requests with an id
    /// (the docs call them notifications), so each is answered: todos become
    /// the plan, a plan proposal is accepted, a subagent task header is
    /// acknowledged, image generation is declined. Everything but todos is
    /// also surfaced raw under its method name for apps that want the
    /// plan markdown or the subagent header.
    async fn on_cursor_extension(&mut self, frame: Value) -> Result<(), Gone> {
        let method = frame["method"].as_str().unwrap_or_default().to_owned();
        let params = frame["params"].clone();
        let outcome = match method.as_str() {
            "cursor/update_todos" => {
                self.merge_todos(&params);
                let entries = plan_entries(&Value::Array(self.todos.clone()), "content");
                self.events
                    .event(EventKind::PlanUpdated { entries })
                    .await?;
                json!({ "outcome": "accepted", "todos": self.todos })
            }
            "cursor/create_plan" => json!({ "outcome": "accepted" }),
            "cursor/task" => json!({ "outcome": "completed" }),
            _ => json!({ "outcome": "rejected", "reason": "not supported by this client" }),
        };
        if let Some(id) = frame.get("id") {
            self.wire
                .respond(id.clone(), json!({ "outcome": outcome }))
                .await?;
        }
        if method == "cursor/update_todos" {
            return Ok(());
        }
        let mut extensions = Extensions::new();
        extensions.insert(method.clone(), params);
        self.events
            .content(
                EventKind::Diagnostic(Diagnostic {
                    level: DiagnosticLevel::Info,
                    message: format!("extension request {method}"),
                }),
                None,
                extensions,
            )
            .await
    }

    /// Cursor's todo list: `merge: true` updates by id, else it replaces.
    fn merge_todos(&mut self, params: &Value) {
        let incoming = params["todos"].as_array().cloned().unwrap_or_default();
        if params["merge"].as_bool() != Some(true) {
            self.todos = incoming;
            return;
        }
        for todo in incoming {
            match self.todos.iter_mut().find(|t| t["id"] == todo["id"]) {
                Some(existing) => *existing = todo,
                None => self.todos.push(todo),
            }
        }
    }

    /// Cursor sends no usage frames (probed 2026-09-07), so every turn ends
    /// with an estimate from what crossed the wire (4 chars ≈ 1 token),
    /// labelled `anyagent/estimated` in the event's extensions. The window
    /// is the selected model's `context` option; Auto has none.
    async fn report_estimate(&mut self) -> Result<(), Gone> {
        if !self.estimate {
            return Ok(());
        }
        self.advertise(Capability::ContextUsage).await?;
        let mut extensions = Extensions::new();
        extensions.insert("anyagent/estimated".into(), Value::Bool(true));
        self.events
            .content(
                EventKind::ContextUsage {
                    used_tokens: (self.usage_chars / 4) as u64,
                    window_tokens: context_window(&self.info),
                    cost_usd: None,
                },
                None,
                extensions,
            )
            .await
    }

    /// Grok's authoritative turn end (its `session/prompt` RPC can hang after
    /// the turn finished). Guarded by session, an outstanding prompt, and
    /// the promptId echo, so a stale replay never ends a newer turn.
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
        self.hidden_tools.clear();
        let stop = params["stopReason"].as_str().unwrap_or("end_turn");
        self.events
            .send(DriverEvent::TurnEnded(stop_reason(
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
        self.events
            .send(DriverEvent::InfoChanged(self.info.clone()))
            .await
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
            return self
                .events
                .send(DriverEvent::InfoChanged(self.info.clone()))
                .await;
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
            return self
                .events
                .send(DriverEvent::InfoChanged(self.info.clone()))
                .await;
        }
        Ok(())
    }

    /// Adds a capability the wire just proved, republishing the details.
    async fn advertise(&mut self, cap: Capability) -> Result<(), Gone> {
        if self.info.details.capabilities.supports(cap.clone()) {
            return Ok(());
        }
        self.info.details.capabilities.add(cap);
        self.events
            .send(DriverEvent::InfoChanged(self.info.clone()))
            .await
    }

    /// Adopts a new command list and republishes the advertised details.
    async fn set_commands(&mut self, commands: Vec<SlashCommand>) -> Result<(), Gone> {
        self.info.details.commands = commands;
        self.events
            .send(DriverEvent::InfoChanged(self.info.clone()))
            .await
    }

    /// The prompt response ends the turn; the steering response reports back.
    async fn on_response(&mut self, frame: Value) -> Result<(), Gone> {
        let Some(id) = frame.get("id").and_then(Value::as_u64) else {
            return Ok(());
        };
        if Some(id) == self.prompt_id {
            // Kiro's spurious cancel: the same prompt goes out once more.
            if frame["result"]["stopReason"] == "cancelled"
                && let Some(params) = self.retry.take()
            {
                self.prompt_id = Some(self.wire.request("session/prompt", params).await?);
                return Ok(());
            }
            self.prompt_id = None;
            self.tools.clear();
            self.hidden_tools.clear();
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
                return self.events.send(ev).await;
            }
            self.report_estimate().await?;
            return self
                .events
                .send(DriverEvent::TurnEnded(stop_reason(&frame)))
                .await;
        }
        if Some(id) == self.steer_id {
            self.steer_id = None;
            let accepted = frame["result"]["accepted"].as_bool().unwrap_or(false);
            return self.events.send(DriverEvent::Steered(accepted)).await;
        }
        if let Some(at) = self.configs.iter().position(|(c, _, _)| *c == id) {
            let (_, config_id, value) = self.configs.remove(at);
            if let Some(error) = frame.get("error") {
                let message = error["message"].as_str().unwrap_or("rejected");
                self.events
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        format!("agent rejected configure `{config_id}`: {message}"),
                    )
                    .await?;
            } else {
                let adopted = adopt_config_options(
                    &mut self.info,
                    &frame["result"],
                    self.first_class_model,
                    self.kiro,
                );
                let changed = crate::adapter::apply_selection(&mut self.info, &config_id, &value);
                // Kiro's effort choices follow the model.
                if changed && self.kiro && config_id.as_str() == "model" {
                    sync_effort(&mut self.info);
                }
                if adopted || changed {
                    self.events
                        .send(DriverEvent::InfoChanged(self.info.clone()))
                        .await?;
                }
            }
            if Some(id) == self.effort_id {
                self.effort_id = None;
                return self.after_effort().await;
            }
        }
        Ok(())
    }

    /// Sends a prompt as the running turn.
    async fn send_prompt(&mut self, params: Value, pid: String) -> Result<(), Gone> {
        self.retry = self.kiro.then(|| params.clone());
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
        let response = match (answer, pending.wire) {
            (crate::event::Answer::Question(answers), QuestionWire::Grok) => {
                question_response(&pending.questions, &answers)
            }
            (crate::event::Answer::Question(answers), QuestionWire::Cursor) => {
                cursor_question_response(&pending.questions, &answers)
            }
            (crate::event::Answer::Question(answers), QuestionWire::Interaction) => {
                interaction_response(&pending.questions, &answers)
            }
            (crate::event::Answer::Permission(_), _) => None,
        };
        let response = response.unwrap_or_else(|| cancelled_question(pending.wire));
        self.wire.respond(pending.wire_id, response).await?;
        Ok(())
    }

    /// Applies a partial wire update to the cumulative snapshot; also returns
    /// the output text this update appended.
    fn merge_tool(&mut self, update: acp::ToolCallUpdate) -> (ToolUpdate, String) {
        let tool = self
            .tools
            .entry(update.tool_call_id.0.to_string())
            .or_insert_with(|| blank_tool(update.tool_call_id.0.as_ref()));
        let appended = apply_fields(tool, update.fields);
        (tool.clone(), appended)
    }

    /// Content blocks for one prompt: inlined media first, each kind only
    /// when the agent advertised it (images, audio, PDFs as embedded
    /// resources), then the text carrying every attachment's path ref.
    async fn prompt_blocks(&mut self, input: &Input) -> Result<Value, Gone> {
        let loaded = attach::load(&input.attachments).await;
        for problem in loaded.iter().filter_map(|l| l.problem.as_deref()) {
            self.events
                .diagnostic(DiagnosticLevel::Warning, problem.to_owned())
                .await?;
        }
        let images = self.info.details.capabilities.supports(Capability::Images);
        let mut blocks = Vec::new();
        for l in &loaded {
            let Some(inline) = &l.inline else { continue };
            let block = match inline.media {
                attach::Media::Image if images => {
                    json!({ "type": "image", "data": inline.base64(), "mimeType": inline.mime })
                }
                attach::Media::Audio if self.prompt_media.audio => {
                    json!({ "type": "audio", "data": inline.base64(), "mimeType": inline.mime })
                }
                attach::Media::Pdf if self.prompt_media.embedded_context => json!({
                    "type": "resource",
                    "resource": {
                        "uri": attach::file_uri(&l.path),
                        "mimeType": inline.mime,
                        "blob": inline.base64(),
                    },
                }),
                _ => continue,
            };
            blocks.push(block);
        }
        blocks.push(json!({
            "type": "text",
            "text": attach::with_refs(input.as_text(), &loaded),
        }));
        Ok(Value::Array(blocks))
    }
}

// ---------------------------------------------------------------------------
// FRAME DECODING
// ---------------------------------------------------------------------------

/// Text-bearing chunk to an event; non-text content is dropped.
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

/// A `tool_call` notification as a full snapshot.
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

/// An empty snapshot for a tool the wire updates before it announces.
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

/// Merges a partial update's present fields into the snapshot; returns the
/// output text it appended.
fn apply_fields(tool: &mut ToolUpdate, fields: acp::ToolCallUpdateFields) -> String {
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
    let appended = match fields.content {
        Some(content) => apply_content(tool, content),
        None => String::new(),
    };
    apply_raw_input(tool, fields.raw_input);
    appended
}

/// Diff items become `diffs`; text items append to `output`. Returns the
/// text appended.
fn apply_content(tool: &mut ToolUpdate, content: Vec<acp::ToolCallContent>) -> String {
    let mut appended = String::new();
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
                    appended.push_str(&text.text);
                }
            }
            _ => {}
        }
    }
    appended
}

/// Keeps the agent's own input when the wire carries it, named when the
/// agent names the tool inside it (cursor's `_toolName`). Cursor's `task`
/// stays `Other`: it runs a subagent but reports only a header, never the
/// nested events `ToolKind::Subagent` promises (probed 2026-09-07).
fn apply_raw_input(tool: &mut ToolUpdate, raw_input: Option<Value>) {
    if let Some(input) = raw_input {
        let name = input["_toolName"].as_str().unwrap_or_default().to_owned();
        tool.raw = Some(RawTool { name, input });
    }
}

/// ACP tool kinds to ours.
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

/// ACP tool statuses to ours.
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

/// One ACP plan entry to ours.
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

/// ACP permission option kinds to ours.
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

/// One raw question object as the typed [`Question`]: grok's
/// `question`/`multiSelect` or cursor's `prompt`/`allowMultiple`. Ids fall
/// back to the question/label text (grok marks them optional). A cursor
/// question without options takes free text (it rides as the option id).
fn typed_question(q: &Value, cursor: bool) -> Question {
    let text = q["question"]
        .as_str()
        .or(q["prompt"].as_str())
        .unwrap_or_default();
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
        multi_select: q["multiSelect"]
            .as_bool()
            .or(q["allowMultiple"].as_bool())
            .unwrap_or(false),
        allows_free_text: cursor && q["options"].as_array().is_none_or(Vec::is_empty),
    }
}

/// Cursor's accepted answer (docs.cursor.com/cli/acp): per question, the
/// selected option ids (the engine only lets offered ids through); free
/// text rides as the id itself. `None` (mismatched shapes) degrades to
/// `cancelled`.
fn cursor_question_response(questions: &[Value], answers: &[QuestionAnswer]) -> Option<Value> {
    if answers.len() != questions.len() {
        return None;
    }
    let answers: Vec<Value> = questions
        .iter()
        .zip(answers)
        .map(|(q, answer)| {
            let ids: Vec<&str> = match answer {
                QuestionAnswer::Choices(ids) => ids.iter().map(ChoiceId::as_str).collect(),
                QuestionAnswer::Text(text) => vec![text.as_str()],
            };
            json!({ "questionId": q["id"], "selectedOptionIds": ids })
        })
        .collect();
    Some(json!({ "outcome": { "outcome": "answered", "answers": answers } }))
}

/// The cancelled answer in the agent's own shape.
fn cancelled_question(wire: QuestionWire) -> Value {
    match wire {
        QuestionWire::Grok => json!({ "outcome": "cancelled" }),
        _ => json!({ "outcome": { "outcome": "cancelled" } }),
    }
}

/// Antigravity's accepted answer: the one chosen option, as the permission
/// outcome. `None` (no single known choice) degrades to `cancelled`.
fn interaction_response(options: &[Value], answers: &[QuestionAnswer]) -> Option<Value> {
    let [QuestionAnswer::Choices(ids)] = answers else {
        return None;
    };
    let [id] = ids.as_slice() else { return None };
    options
        .iter()
        .any(|o| o.as_str() == Some(id.as_str()))
        .then(|| json!({ "outcome": { "outcome": "selected", "optionId": id.as_str() } }))
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

/// The prompt response's `stopReason` as ours.
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

/// A notification's `_meta` as extensions.
fn ext(meta: Option<acp::Meta>) -> Extensions {
    meta.map(|m| m.into_iter().collect()).unwrap_or_default()
}

/// Typed parse of a handshake response; a mismatch is a protocol failure.
fn parse<T: serde::de::DeserializeOwned>(value: Value, what: &str) -> Result<T, AgentError> {
    serde_json::from_value(value).map_err(|e| AgentError::ProtocolFailed(format!("{what}: {e}")))
}

// ---------------------------------------------------------------------------
// WIRE: JSON-RPC requests, responses, and notifications
// ---------------------------------------------------------------------------

struct Wire {
    line: LineWire,
    next_id: u64,
}

impl Wire {
    /// Takes the child's stdio and starts the line-reader task.
    fn over(child: &mut process::Child, recorder: Option<WireRecorder>) -> Self {
        Self {
            line: LineWire::over(child, recorder),
            next_id: 1,
        }
    }

    /// Sends a request and returns its id; the response arrives in `frames`.
    async fn request(&mut self, method: &str, params: Value) -> std::io::Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        self.line
            .write(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;
        Ok(id)
    }

    /// A notification (no id, no response).
    async fn notify(&mut self, method: &str, params: Value) -> std::io::Result<()> {
        self.line
            .write(json!({ "jsonrpc": "2.0", "method": method, "params": params }))
            .await
    }

    /// Answers one of the agent's requests; the raw id keeps its type.
    async fn respond(&mut self, id: Value, result: Value) -> std::io::Result<()> {
        self.line
            .write(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
            .await
    }

    /// Declines one of the agent's requests.
    async fn respond_error(&mut self, id: Value, code: i64, message: &str) -> std::io::Result<()> {
        self.line.write(
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
            let frame = self.line.frames.recv().await.ok_or(WireError::Closed)?;
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
                    // No runnable method to offer (agent-driven auth, or a
                    // shutdown notice behind the auth code): the agent's own
                    // words beat a bare "needs login".
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

/// A terminal auth method becomes a runnable command; agent-driven auth is
/// not offered. Qwen advertises `{type: "terminal", args}` inside `_meta`
/// (probed 0.22.0), so that shape is read too.
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
