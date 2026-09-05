//! Entry point: find agents, open sessions.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::adapter::{Adapter, ConnectRequest};
use crate::agent::{
    AgentDetails, AgentId, AgentInstallation, AuthStatus, Capabilities, Capability, Input,
    PermissionMode, SessionOptions, SessionStart,
};
use crate::error::AgentError;
use crate::event::{
    Answer, Diagnostic, EventKind, PermissionChoice, PlanUsage, QuestionAnswer, Request, StopReason,
};
use crate::session::{self, Events, Session};

const USAGE_CACHE_TTL: Duration = Duration::from_secs(60);
/// special case for acp probe.
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
                // Antigravity's wire is unvalidated (ticket 05); no driver yet.
                Connection::Native(NativeKind::Antigravity) => continue,
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

    /// A runtime whose only agent is the given adapter, registered as `mock`.
    /// The real catalog is not scanned.
    #[cfg(test)]
    pub(crate) fn with_test_adapter(adapter: impl Adapter + 'static) -> Self {
        use crate::agent::InstallationSource;
        let id = AgentId::new("mock");
        let mut runtime = Self::new();
        runtime.profiles = &[];
        runtime.adapters.insert(id.clone(), Arc::new(adapter));
        runtime.pinned.push(AgentInstallation {
            name: "Mock".into(),
            id,
            executable_path: PathBuf::from("mock"),
            source: InstallationSource::Pinned,
            auth: None,
            acp_args: None,
        });
        runtime
    }

    /// Best-effort, read-only inventory: which agents exist, where, and
    /// whether a login marker is present. Never launches an agent. An agent
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
    /// handle plus the event stream. Used in probe and session open
    pub async fn open(
        &self,
        agent: &AgentInstallation,
        options: SessionOptions,
    ) -> Result<(Session, Events), AgentError> {
        // An explicit `AgentInstallation::acp` drives ACP even for a catalog
        // agent with a native adapter; otherwise the catalog adapter.
        let adapter: Arc<dyn Adapter> = match (self.adapters.get(&agent.id), &agent.acp_args) {
            (_, Some(args)) => Arc::new(crate::adapter::acp::AcpAdapter::new(args.clone())),
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
    /// throwaway session, denies every tool permission, gathers the text
    /// until the turn ends, and closes. Requires a new session; a tool event
    /// or a question requiring a choice cancels generation. Native Pi disables
    /// tools and session persistence at launch. Include its context inline:
    /// path attachments cannot be opened without tools.
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
        // Only native Pi enforces no_tools at launch; an ACP override does not.
        let native_pi = agent.id.as_str() == "pi" && agent.acp_args.is_none();
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Permissions)
            && !native_pi
        {
            session.close().await.ok();
            return Err(AgentError::UnsupportedFeature(
                "generate requires tool permissions or launch-time tool disabling".into(),
            ));
        }
        let reply = collect_reply(&session, &mut events, prompt.into()).await;
        session.close().await.ok();
        reply
    }

    // spawn agent in temp dir, wait for handshake, read details, close.
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
        // ACP agents often deliver `availableCommands` as an update just
        // after `session/new`; wait briefly for it before giving up on an
        // empty list. Agents that report commands at handshake (claude) skip
        // the wait entirely.
        let deadline = tokio::time::Instant::now() + PROBE_COMMANDS_WAIT;
        while session.info().details.commands.is_empty() {
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

    /// Agents without quota
    /// (or with an API-key login) return `UnsupportedFeature`. May spawn a
    /// short-lived agent process; results are cached for 60 s.
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
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn make_exe(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let exe = dir.join(name);
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        exe
    }

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

    #[tokio::test]
    // The lock deliberately spans the awaits: it serializes tests that
    // mutate process-wide HOME/PATH.
    #[allow(clippy::await_holding_lock)]
    async fn discover_hides_agents_without_adapter() {
        let _guard = env_lock().lock().unwrap();
        // `antigravity` has no adapter; even if its `agy` binary is on disk
        // it must not appear as usable.
        let home = tempfile::tempdir().unwrap();
        let bin = home.path().join(".local/bin");
        make_exe(&bin, "agy");
        // Also create a supported agent so the scan has something to find.
        make_exe(&bin, "claude");

        let orig_home = std::env::var_os("HOME");
        let orig_path = std::env::var_os("PATH");
        unsafe {
            std::env::set_var("HOME", home.path());
            std::env::set_var("PATH", bin.to_string_lossy().to_string());
        }

        // Raw discovery (no filtering) would find `agy`.
        let raw = crate::discovery::discover(crate::catalog::PROFILES).await;
        assert!(
            raw.agents.iter().any(|a| a.id.as_str() == "antigravity"),
            "raw discover should find agy when present"
        );

        // Runtime::discover hides it.
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        assert!(
            !report.agents.iter().any(|a| a.id.as_str() == "antigravity"),
            "antigravity must not appear as usable"
        );
        assert!(
            !report
                .missing
                .iter()
                .any(|m| m.id.as_str() == "antigravity"),
            "antigravity must not appear as missing either"
        );
        assert!(
            report.agents.iter().any(|a| a.id.as_str() == "claude"),
            "supported agents still appear"
        );

        unsafe {
            if let Some(v) = orig_home {
                std::env::set_var("HOME", v);
            } else {
                std::env::remove_var("HOME");
            }
            if let Some(v) = orig_path {
                std::env::set_var("PATH", v);
            } else {
                std::env::remove_var("PATH");
            }
        }
    }
}
