//! Entry point: find agents, open sessions.
//!
//! High level: `Runtime::new` registers one adapter per catalog agent;
//! `discover` scans offline, `probe`/`probe_auth` open a throwaway session,
//! `open` connects and starts the engine, `generate` runs one prompt to text,
//! `plan_usage` reads account quota.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::adapter::{Adapter, ConnectRequest};
use crate::agent::{
    AgentDetails, AgentId, AgentInstallation, AuthStatus, Capabilities, Capability, Input,
    InstallationSource, PermissionMode, SessionOptions, SessionStart,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Diagnostic, EventKind, PermissionChoice, PlanUsage, QuestionAnswer, Request, StopReason,
};
use crate::session::{self, Events, Session};

const USAGE_CACHE_TTL: Duration = Duration::from_secs(60);
/// How long `probe` waits for a command list that arrives after the
/// handshake (ACP `availableCommands`, codex skills).
const PROBE_COMMANDS_WAIT: Duration = Duration::from_secs(2);

/// The one object an application creates. Holds the adapter registry.
pub struct Runtime {
    adapters: HashMap<AgentId, Arc<dyn Adapter>>,
    /// Installations known without discovery (tests and pinned agents).
    pinned: Vec<AgentInstallation>,
    profiles: &'static [crate::catalog::AgentProfile],
    /// `plan_usage` results per installation, kept for `USAGE_CACHE_TTL`.
    usage_cache: Mutex<HashMap<(AgentId, PathBuf), (Instant, PlanUsage)>>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    /// Registers one adapter per catalog agent whose adapter exists.
    pub fn new() -> Self {
        use crate::catalog::{Connection, NativeKind};
        let mut adapters: HashMap<AgentId, Arc<dyn Adapter>> = HashMap::new();
        for profile in crate::catalog::PROFILES {
            let adapter: Arc<dyn Adapter> = match &profile.connection {
                Connection::Acp { .. } => {
                    Arc::new(crate::adapter::acp::AcpAdapter::for_profile(profile))
                }
                Connection::Native(NativeKind::Claude) => {
                    Arc::new(crate::adapter::claude::ClaudeAdapter::new())
                }
                Connection::Native(NativeKind::Codex) => {
                    Arc::new(crate::adapter::codex::CodexAdapter::new())
                }
                Connection::Native(NativeKind::Pi) => {
                    Arc::new(crate::adapter::pi::PiAdapter::new())
                }
                Connection::Native(NativeKind::Opencode) => {
                    Arc::new(crate::adapter::opencode::OpencodeAdapter::new())
                }
                Connection::Native(NativeKind::Antigravity) => Arc::new(
                    crate::adapter::antigravity::AntigravityAdapter::new(profile),
                ),
            };
            adapters.insert(AgentId::new(profile.id), adapter);
        }
        Self {
            adapters,
            pinned: Vec::new(),
            profiles: crate::catalog::PROFILES,
            usage_cache: Mutex::new(HashMap::new()),
        }
    }

    /// A runtime whose only agent plays `script`, registered as `mock`: the
    /// real engine underneath, no subprocess. For testing an app's UI and
    /// routing against a scripted agent.
    #[cfg(any(test, feature = "mock"))]
    pub fn with_mock(script: crate::mock::Script) -> Self {
        Self::with_test_adapter(crate::mock::MockAdapter::new(script))
    }

    /// A runtime whose only agent is the given adapter, registered as `mock`.
    /// The real catalog is not scanned.
    #[cfg(any(test, feature = "mock"))]
    pub(crate) fn with_test_adapter(adapter: impl Adapter + 'static) -> Self {
        let id = AgentId::new("mock");
        let mut runtime = Self::new();
        runtime.profiles = &[];
        runtime.adapters.insert(id.clone(), Arc::new(adapter));
        runtime.pinned.push(AgentInstallation {
            name: "Mock".into(),
            id,
            executable_path: PathBuf::from("mock"),
            source: InstallationSource::Pinned,
            upgrade: None,
            acp_args: None,
        });
        runtime
    }

    /// Read-only inventory: which agents exist and where. Never launches an
    /// agent; login state comes from `probe` or `probe_auth`. An agent
    /// whose adapter is not implemented never appears as usable.
    pub async fn discover(&self) -> DiscoveryReport {
        let mut report = crate::discovery::discover(self.profiles).await;
        report.agents.retain(|a| self.adapters.contains_key(&a.id));
        report.missing.retain(|m| self.adapters.contains_key(&m.id));
        report.agents.splice(0..0, self.pinned.iter().cloned());
        report
    }

    /// Captures the login-shell PATH in the background so the first
    /// `discover` is instant. Optional; apps call it at boot.
    pub fn prewarm(&self) {
        tokio::spawn(crate::process::login_shell_path());
    }

    /// Launches the agent, completes the handshake, and returns the command
    /// handle plus the event stream.
    pub async fn open(
        &self,
        agent: &AgentInstallation,
        options: SessionOptions,
    ) -> Result<(Session, Events), AgentError> {
        // ACP args drive ACP even for a catalog agent with a native adapter:
        // an explicit `AgentInstallation::acp`, or discovery having found the
        // agent's ACP upgrade. Only the discovered one keeps the catalog's
        // auth facts; a pinned ad-hoc install named like a catalog agent is
        // still ad-hoc.
        let profile = (agent.source != InstallationSource::Pinned)
            .then(|| self.profiles.iter().find(|p| p.id == agent.id.as_str()))
            .flatten();
        let adapter: Arc<dyn Adapter> = match (self.adapters.get(&agent.id), &agent.acp_args) {
            (_, Some(args)) => Arc::new(crate::adapter::acp::AcpAdapter::with_args(
                profile,
                args.clone(),
            )),
            (Some(adapter), None) => Arc::clone(adapter),
            (None, None) => {
                return Err(AgentError::ProtocolFailed(format!(
                    "no adapter implemented for {} yet",
                    agent.id
                )));
            }
        };
        let connection = adapter
            .connect(ConnectRequest {
                installation: agent.clone(),
                options: options.clone(),
            })
            .await?;
        Ok(session::start(agent.clone(), connection, &options))
    }
    /// One-shot generation: prompt in, the agent's reply text out. Opens a
    /// throwaway session with tools disabled where the wire allows (claude,
    /// pi) and every permission declined elsewhere, gathers the text until
    /// the turn ends, and closes. Requires a new session; a tool event or a
    /// question requiring a choice cancels generation. Include context
    /// inline: path attachments cannot be opened without tools.
    pub async fn generate(
        &self,
        agent: &AgentInstallation,
        options: SessionOptions,
        prompt: impl Into<Input>,
    ) -> Result<String, AgentError> {
        if !matches!(options.start, SessionStart::New) {
            return Err(AgentError::InvalidConfiguration(
                "generate requires a new session".into(),
            ));
        }
        // Hands-off regardless of the caller's mode: AutoApprove would let
        // the agent run tools before any request reached this loop.
        let mut options = options.permission_mode(PermissionMode::Ask);
        options.no_tools = true;
        let (session, mut events) = self.open(agent, options).await?;
        // Hands-off needs one of: tools switched off at launch, or every
        // tool gated by a permission request this loop can decline.
        let hands_off = session.tools_disabled()
            || session
                .info()
                .details
                .capabilities
                .supports(Capability::Permissions);
        if !hands_off {
            session.close().await.ok();
            return Err(AgentError::UnsupportedFeature(
                "generate requires tool permissions or launch-time tool disabling".into(),
            ));
        }
        let reply = collect_reply(&session, &mut events, prompt.into()).await;
        session.close().await.ok();
        reply
    }

    /// Opens a throwaway session in the temp dir, reads the details the
    /// handshake learned, and closes. A logged-out agent is a result, not
    /// an error.
    pub async fn probe(&self, agent: &AgentInstallation) -> Result<AgentDetails, AgentError> {
        let opened = self
            .open(agent, SessionOptions::in_dir(std::env::temp_dir()))
            .await;
        // Not logged is reported as a detail.
        let (session, mut events) = match opened {
            Err(AgentError::AuthRequired { login }) => {
                return Ok(AgentDetails {
                    version: None,
                    auth: AuthStatus::Unauthenticated { login },
                    capabilities: Capabilities::default(),
                    config_options: Vec::new(),
                    commands: Vec::new(),
                });
            }
            other => other?,
        };
        // ACP agents deliver `availableCommands` just after `session/new`
        // and codex fetches skills after open; wait briefly for the list.
        // Agents that report commands at handshake (claude) or have none to
        // report skip the wait.
        let deadline = tokio::time::Instant::now() + PROBE_COMMANDS_WAIT;
        let has_commands = session
            .info()
            .details
            .capabilities
            .supports(Capability::SlashCommands);
        while has_commands && session.info().details.commands.is_empty() {
            let Ok(Some(Ok(_))) = tokio::time::timeout_at(deadline, events.next()).await else {
                break;
            };
        }
        let details = session.info().details;
        session.close().await.ok();
        Ok(details)
    }

    /// Fast auth-only probe: does not wait for `availableCommands` (saves
    /// `PROBE_COMMANDS_WAIT`). Use when only `auth` is needed (e.g. kiro).
    pub async fn probe_auth(&self, agent: &AgentInstallation) -> Result<AuthStatus, AgentError> {
        let opened = self
            .open(agent, SessionOptions::in_dir(std::env::temp_dir()))
            .await;
        match opened {
            Err(AgentError::AuthRequired { login }) => Ok(AuthStatus::Unauthenticated { login }),
            Err(e) => Err(e),
            Ok((session, _events)) => {
                let auth = session.info().details.auth.clone();
                // _events dropped here; close shuts down the child.
                session.close().await.ok();
                Ok(auth)
            }
        }
    }

    /// Plan quota for the logged-in account. Agents without quota (or with
    /// an API-key login) return `UnsupportedFeature`. May spawn a short-lived
    /// agent process; results are cached for 60 s.
    pub async fn plan_usage(&self, agent: &AgentInstallation) -> Result<PlanUsage, AgentError> {
        let key = (agent.id.clone(), agent.executable_path.clone());
        if let Some((at, usage)) = self.usage_cache.lock().unwrap().get(&key)
            && at.elapsed() < USAGE_CACHE_TTL
        {
            return Ok(usage.clone());
        }
        let adapter = self
            .adapters
            .get(&agent.id)
            .ok_or_else(|| AgentError::UnsupportedFeature("plan usage".into()))?;
        let usage = adapter.plan_usage(agent).await?;
        self.usage_cache
            .lock()
            .unwrap()
            .insert(key, (Instant::now(), usage.clone()));
        Ok(usage)
    }

    /// One call for a usage page: every discovered agent with its quota or
    /// the typed reason it has none. Per-agent probes run concurrently.
    pub async fn plan_usage_all(&self) -> Vec<AgentPlanUsage> {
        let report = self.discover().await;
        let probes = report.agents.into_iter().map(|agent| async move {
            let usage = self.plan_usage(&agent).await;
            AgentPlanUsage { agent, usage }
        });
        futures::future::join_all(probes).await
    }
}

/// Sends the prompt and gathers the agent's own text (not subagents') until
/// the turn ends. Requests are declined so the agent stays hands-off.
async fn collect_reply(
    session: &Session,
    events: &mut Events,
    prompt: Input,
) -> Result<String, AgentError> {
    session.prompt(prompt).await?;
    let mut text = String::new();
    while let Some(event) = events.next().await {
        let event = event?;
        let nested = event
            .turn_info
            .as_ref()
            .is_some_and(|t| t.parent_tool_id.is_some());
        match event.kind {
            EventKind::TextDelta { text: delta, .. } if !nested => text.push_str(&delta),
            // Stop even on a proposed tool call; generation is text-only.
            EventKind::ToolUpdated(_) => {
                session.cancel(true).await?;
                return Err(AgentError::ProtocolFailed(
                    "generate: the agent attempted to use a tool".into(),
                ));
            }
            EventKind::RequestOpened(request) => match decline(&request) {
                Some(answer) => session.answer(request.id(), answer).await?,
                None => {
                    session.cancel(true).await?;
                    return Err(AgentError::ProtocolFailed(
                        "generate: the request cannot be declined without making a choice".into(),
                    ));
                }
            },
            EventKind::TurnEnded {
                stop: StopReason::Completed { .. },
                ..
            } => return Ok(text),
            EventKind::TurnEnded { stop, .. } => {
                return Err(AgentError::ProtocolFailed(format!(
                    "generate: turn ended with {stop:?}"
                )));
            }
            _ => {}
        }
    }
    Err(AgentError::SessionClosed)
}

/// Deny permissions and leave free-text answers blank. A request requiring
/// a choice cannot be declined safely, so the caller cancels generation.
fn decline(request: &Request) -> Option<Answer> {
    match request {
        Request::Permission(p) => [PermissionChoice::DenyOnce, PermissionChoice::DenyAlways]
            .into_iter()
            .find(|choice| p.options.contains(choice))
            .map(Answer::Permission),
        Request::Question(q) if q.questions.iter().all(|q| q.allows_free_text) => {
            Some(Answer::Question(
                q.questions
                    .iter()
                    .map(|_| QuestionAnswer::Text(String::new()))
                    .collect(),
            ))
        }
        Request::Question(_) => None,
    }
}

/// One row of a usage page: the agent and its quota, or why it has none.
#[derive(Debug)]
pub struct AgentPlanUsage {
    pub agent: AgentInstallation,
    pub usage: Result<PlanUsage, AgentError>,
}

/// What `discover` found and what it could not read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryReport {
    pub agents: Vec<AgentInstallation>,
    /// Known agents that were not found, with where we looked and how to
    /// install them.
    pub missing: Vec<MissingAgent>,
    pub diagnostics: Vec<Diagnostic>,
}

impl DiscoveryReport {
    /// The installed agent with this id, or `NotInstalled`.
    pub fn require(&self, id: impl AsRef<str>) -> Result<&AgentInstallation, AgentError> {
        let id = id.as_ref();
        self.agents
            .iter()
            .find(|a| a.id.as_str() == id)
            .ok_or_else(|| AgentError::NotInstalled(AgentId::new(id)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingAgent {
    pub id: AgentId,
    pub name: String,
    pub searched: Vec<PathBuf>,
    pub install_hint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `generate` returns the turn's text and declines the permission
    /// request on the way, leaving nothing open.
    #[tokio::test]
    async fn generate_collects_text_and_declines_requests() {
        use crate::adapter::mock::MockAdapter;
        let runtime = Runtime::with_test_adapter(MockAdapter::permission_flow());
        let agent = runtime.discover().await.require("mock").unwrap().clone();
        let dir = tempfile::tempdir().unwrap();
        let text = runtime
            .generate(&agent, SessionOptions::in_dir(dir.path()), "title this")
            .await
            .unwrap();
        assert_eq!(text, "Let me check. Done.");
    }

    /// `generate` uses the offered deny and blank free text, but never picks
    /// a choice or accepts a permission without a deny.
    #[tokio::test]
    async fn generate_declines_within_what_the_request_offers() {
        use crate::adapter::mock::{MockAdapter, Script, Step, completed, text, tool};
        use crate::event::{
            Choice, ChoiceId, PermissionRequest, Question, QuestionId, QuestionRequest, RequestId,
            ToolStatus,
        };
        let EventKind::ToolUpdated(pending) = tool("tool-1", ToolStatus::Pending) else {
            unreachable!()
        };
        let permission = |options: Vec<PermissionChoice>| {
            EventKind::RequestOpened(Request::Permission(PermissionRequest {
                id: RequestId::new("r1"),
                tool: pending.clone(),
                options,
                detail: None,
            }))
        };
        let question = |allows_free_text| {
            EventKind::RequestOpened(Request::Question(QuestionRequest {
                id: RequestId::new("q1"),
                questions: vec![Question {
                    id: QuestionId::new("q"),
                    text: "Which?".into(),
                    header: None,
                    choices: vec![Choice {
                        id: ChoiceId::new("a"),
                        label: "A".into(),
                        description: None,
                    }],
                    multi_select: false,
                    allows_free_text,
                }],
            }))
        };
        let script = Script::default().turn(vec![
            Step::Emit(text("m1", "one ")),
            Step::Emit(permission(vec![
                PermissionChoice::AllowOnce,
                PermissionChoice::DenyAlways,
            ])),
            Step::AwaitAnswer,
            Step::Emit(question(true)),
            Step::AwaitAnswer,
            Step::Emit(text("m1", "two")),
            Step::End(completed()),
        ]);
        let runtime = Runtime::with_test_adapter(MockAdapter::new(script));
        let agent = runtime.discover().await.require("mock").unwrap().clone();
        let dir = tempfile::tempdir().unwrap();
        let text = runtime
            .generate(&agent, SessionOptions::in_dir(dir.path()), "go")
            .await
            .unwrap();
        assert_eq!(text, "one two");

        // A permission with no deny at all cannot be declined: generate fails
        // instead of allowing it.
        for request in [
            permission(vec![PermissionChoice::AllowOnce]),
            question(false),
        ] {
            let script = Script::default().turn(vec![
                Step::Emit(request),
                Step::AwaitAnswer,
                Step::End(completed()),
            ]);
            let runtime = Runtime::with_test_adapter(MockAdapter::new(script));
            let agent = runtime.discover().await.require("mock").unwrap().clone();
            let refused = runtime
                .generate(&agent, SessionOptions::in_dir(dir.path()), "go")
                .await;
            assert!(
                matches!(refused, Err(AgentError::ProtocolFailed(_))),
                "{refused:?}"
            );
        }
    }

    #[tokio::test]
    async fn generate_rejects_existing_sessions_before_launch() {
        let runtime = Runtime::new();
        let agent = AgentInstallation::at("pi", "/nonexistent/pi");
        for start in [
            SessionStart::Resume(crate::agent::ResumeToken::new("existing")),
            SessionStart::Fork {
                from: crate::agent::ResumeToken::new("existing"),
                at: None,
            },
        ] {
            let mut options = SessionOptions::in_dir(std::env::temp_dir());
            options.start = start;
            assert!(matches!(
                runtime.generate(&agent, options, "go").await,
                Err(AgentError::InvalidConfiguration(_))
            ));
        }
    }

    #[tokio::test]
    async fn generate_rejects_agents_without_tool_enforcement() {
        use crate::adapter::mock::{MockAdapter, Script};
        let adapter = MockAdapter::new(Script {
            permissions: false,
            ..Script::default()
        });
        let runtime = Runtime::with_test_adapter(adapter);
        let agent = runtime.discover().await.require("mock").unwrap().clone();
        let result = runtime
            .generate(&agent, SessionOptions::in_dir(std::env::temp_dir()), "go")
            .await;
        assert!(
            matches!(result, Err(AgentError::UnsupportedFeature(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn generate_fails_on_tool_activity() {
        use crate::adapter::mock::{MockAdapter, Script, Step, completed, tool};
        use crate::event::ToolStatus;
        for status in [
            ToolStatus::Pending,
            ToolStatus::Running,
            ToolStatus::Completed,
        ] {
            let script = Script::default().turn(vec![
                Step::Emit(tool("tool-1", status)),
                Step::End(completed()),
            ]);
            let runtime = Runtime::with_test_adapter(MockAdapter::new(script));
            let agent = runtime.discover().await.require("mock").unwrap().clone();
            let result = runtime
                .generate(&agent, SessionOptions::in_dir(std::env::temp_dir()), "go")
                .await;
            assert!(
                matches!(result, Err(AgentError::ProtocolFailed(ref message)) if message.contains("tool")),
                "{result:?}"
            );
        }
    }

    /// `generate` stays hands-off even when the caller asked for AutoApprove.
    #[tokio::test]
    async fn generate_forces_ask_mode() {
        use crate::adapter::mock::MockAdapter;
        let runtime = Runtime::with_test_adapter(MockAdapter::permission_flow());
        let agent = runtime.discover().await.require("mock").unwrap().clone();
        let dir = tempfile::tempdir().unwrap();
        let options =
            SessionOptions::in_dir(dir.path()).permission_mode(PermissionMode::AutoApprove);
        // The mock's permission flow only reaches "Done." after an answer;
        // the text proves the request came through this loop, not auto-approval.
        let text = runtime
            .generate(&agent, options, "title this")
            .await
            .unwrap();
        assert_eq!(text, "Let me check. Done.");
    }

    /// Discovery through the runtime, over fixture shims on disk.
    mod discovery {
        use super::*;
        use std::sync::{Mutex, OnceLock};

        fn env_lock() -> &'static Mutex<()> {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            LOCK.get_or_init(|| Mutex::new(()))
        }

        use crate::testutil::{HOME_VAR, shim, stub as make_exe};

        #[tokio::test]
        // The lock deliberately spans the awaits: it serializes tests that
        // mutate process-wide HOME/PATH.
        #[allow(clippy::await_holding_lock)]
        async fn discover_prefers_the_upgrade_and_names_it_when_missing() {
            let _guard = env_lock().lock().unwrap();
            let home = tempfile::tempdir().unwrap();
            let bin = home.path().join(".local/bin");
            let agy = make_exe(&bin, "agy");
            // The fixture wrappers exec `node`, so the real PATH stays behind
            // the fake bin.
            let path = std::env::var_os("PATH").unwrap_or_default();
            let mut paths = vec![bin.clone()];
            paths.extend(std::env::split_paths(&path));
            let _env = EnvGuard::set(&[
                (HOME_VAR, home.path().as_os_str().to_owned()),
                ("PATH", std::env::join_paths(paths).unwrap()),
            ]);

            // Only the CLI: it is the installation, and the ACP server is the
            // named upgrade with its own install hint.
            let report = Runtime::new().discover().await;
            let agent = report.require("antigravity").unwrap();
            assert_eq!(agent.executable_path, agy);
            assert!(agent.acp_args.is_none());
            let upgrade = agent.upgrade.as_ref().expect("upgrade named");
            assert_eq!(upgrade.name, "Antigravity ACP server");
            assert!(upgrade.install_hint.contains("antigravity-acp"));
            assert!(
                upgrade
                    .searched
                    .contains(&home.path().join(".local/agy-acp-server"))
            );

            // The server installed: it wins, over ACP, with nothing left to add.
            let server = make_exe(
                &home.path().join(".local/agy-acp-server"),
                "agy_acp_server.par",
            );
            let report = Runtime::new().discover().await;
            let agent = report.require("antigravity").unwrap();
            assert_eq!(agent.executable_path, server);
            assert_eq!(agent.acp_args.as_deref(), Some(&[][..]));
            assert!(agent.upgrade.is_none());

            // Opening it goes through the ACP adapter with the catalog's facts:
            // the server refuses `session/new` until `authenticate` adopts the
            // CLI's login, and the handshake does that itself.
            let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/acp/fixture.mjs");
            shim(
                server.parent().unwrap(),
                "agy_acp_server.par",
                &fixture,
                "--auth-adopt",
            );
            let (session, _events) = Runtime::new()
                .open(agent, SessionOptions::in_dir(home.path()))
                .await
                .expect("open adopts the login");
            let info = session.info();
            assert!(matches!(
                info.details.auth,
                AuthStatus::Authenticated {
                    kind: crate::agent::AuthKind::Subscription,
                    ..
                }
            ));
            assert!(info.details.capabilities.supports(Capability::Permissions));
            session.close().await.unwrap();

            // Refused for good (the server's capitalised "Authentication
            // required", no runnable method of its own): typed, and the login
            // it names is the CLI's TUI, not the server.
            shim(
                server.parent().unwrap(),
                "agy_acp_server.par",
                &fixture,
                "--auth-required --capitalized-auth --no-auth-methods",
            );
            let err = Runtime::new()
                .open(agent, SessionOptions::in_dir(home.path()))
                .await
                .err()
                .expect("refused");
            let AgentError::AuthRequired { login } = err else {
                panic!("expected AuthRequired, got {err:?}");
            };
            assert!(
                matches!(&login[0], crate::agent::LoginMethod::Terminal { command, .. } if command == &["agy"]),
                "{login:?}"
            );
        }

        #[tokio::test]
        // The lock deliberately spans the awaits: it serializes tests that
        // mutate process-wide HOME/PATH.
        #[allow(clippy::await_holding_lock)]
        async fn env_override_to_the_cli_still_names_a_missing_upgrade() {
            let _guard = env_lock().lock().unwrap();
            let home = tempfile::tempdir().unwrap();
            let bin = home.path().join(".local/bin");
            let agy = make_exe(&bin, "agy");
            // The fixture wrappers exec `node`, so the real PATH stays behind
            // the fake bin.
            let path = std::env::var_os("PATH").unwrap_or_default();
            let mut paths = vec![bin.clone()];
            paths.extend(std::env::split_paths(&path));
            let _env = EnvGuard::set(&[
                (HOME_VAR, home.path().as_os_str().to_owned()),
                ("PATH", std::env::join_paths(paths).unwrap()),
                ("ANYAGENT_ANTIGRAVITY_BIN", agy.as_os_str().to_owned()),
            ]);

            // The override pins the CLI, but the missing server still rides
            // along as installation guidance.
            let report = Runtime::new().discover().await;
            let agent = report.require("antigravity").unwrap();
            assert_eq!(agent.executable_path, agy);
            assert!(agent.acp_args.is_none());
            let upgrade = agent.upgrade.as_ref().expect("upgrade named");
            assert_eq!(upgrade.name, "Antigravity ACP server");

            // The server installed: the override still forces headless, and
            // there is nothing missing left to name.
            make_exe(
                &home.path().join(".local/agy-acp-server"),
                "agy_acp_server.par",
            );
            let report = Runtime::new().discover().await;
            let agent = report.require("antigravity").unwrap();
            assert_eq!(agent.executable_path, agy);
            assert!(agent.acp_args.is_none());
            assert!(agent.upgrade.is_none());
        }

        /// Process-wide env vars set for one test and restored on drop, so a
        /// failed assertion cannot leak them into the next test.
        struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

        impl EnvGuard {
            fn set(vars: &[(&'static str, std::ffi::OsString)]) -> Self {
                let saved = vars
                    .iter()
                    .map(|(name, value)| {
                        let orig = std::env::var_os(name);
                        unsafe { std::env::set_var(name, value) };
                        (*name, orig)
                    })
                    .collect();
                Self(saved)
            }
        }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                for (name, orig) in self.0.drain(..) {
                    unsafe {
                        match orig {
                            Some(v) => std::env::set_var(name, v),
                            None => std::env::remove_var(name),
                        }
                    }
                }
            }
        }
    }
}
