//! Entry point: find agents, open sessions.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::adapter::{Adapter, ConnectRequest};
use crate::agent::{AgentId, AgentInstallation, SessionOptions};
use crate::error::AgentError;
use crate::event::Diagnostic;
use crate::session::{self, Events, Session};

/// The one object an application creates. Holds the adapter registry.
pub struct Runtime {
    adapters: HashMap<AgentId, Arc<dyn Adapter>>,
    /// Installations known without discovery (tests and pinned agents).
    pinned: Vec<AgentInstallation>,
    profiles: &'static [crate::catalog::AgentProfile],
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
                // Codex (P3) and Antigravity (adapter pending) have no driver yet.
                Connection::Native(NativeKind::Codex | NativeKind::Antigravity) => continue,
            };
            adapters.insert(AgentId::new(profile.id), adapter);
        }
        Self {
            adapters,
            pinned: Vec::new(),
            profiles: crate::catalog::PROFILES,
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
    /// handle plus the event stream.
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
