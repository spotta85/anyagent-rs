//! Entry point: find agents, open sessions.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::adapter::{Adapter, ConnectRequest};
use crate::agent::{
    AgentDetails, AgentId, AgentInstallation, AuthStatus, Capabilities, SessionOptions,
};
use crate::error::AgentError;
use crate::event::{Diagnostic, PlanUsage};
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
    /// `plan_usage` results, kept for `USAGE_CACHE_TTL`.
    usage_cache: Mutex<HashMap<AgentId, (Instant, PlanUsage)>>,
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
                Connection::Acp { args } => {
                    Arc::new(crate::adapter::acp::AcpAdapter::new(args.iter().copied()))
                }
                Connection::Native(NativeKind::Claude) => {
                    Arc::new(crate::adapter::claude::ClaudeAdapter::new())
                }
                Connection::Native(NativeKind::Codex) => {
                    Arc::new(crate::adapter::codex::CodexAdapter::new())
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
    /// whether a login marker is present. Never launches an agent.
    pub async fn discover(&self) -> DiscoveryReport {
        let mut report = crate::discovery::discover(self.profiles).await;
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
        // Use the catalog adapter, or create ACP for an ad-hoc installation.
        let adapter: Arc<dyn Adapter> = match (self.adapters.get(&agent.id), &agent.acp_args) {
            (Some(adapter), _) => Arc::clone(adapter),
            (None, Some(args)) => Arc::new(crate::adapter::acp::AcpAdapter::new(args.clone())),
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

    /// Agents without quota
    /// (or with an API-key login) return `UnsupportedFeature`. May spawn a
    /// short-lived agent process; results are cached for 60 s.
    pub async fn plan_usage(&self, agent: &AgentInstallation) -> Result<PlanUsage, AgentError> {
        if let Some((at, usage)) = self.usage_cache.lock().unwrap().get(&agent.id)
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
            .insert(agent.id.clone(), (Instant::now(), usage.clone()));
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
