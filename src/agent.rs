// Structs and stuff for discover, and session management and stuff.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<T: Into<String>> From<T> for $name {
            fn from(value: T) -> Self {
                Self(value.into())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}
pub(crate) use string_id;

string_id!(AgentId); // "Claude", "Codex" etc. 
string_id!(ConfigId); // session settings like "model", "effort"
string_id!(ResumeToken); // session resume token

/// Where discovery found an executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
/// Source of agents installation
pub enum InstallationSource {
    EnvOverride,    // ANYAGENT_<ID>_BIN env var pointed here
    Path,           // found on  $PATH
    LoginShellPath, // found on PATH from a login shell
    VersionManager, // via a version manager (nvm, asdff, etc.)
    KnownLocation,  // a standard install dir we check
    Pinned,         // app gave
}

/// One installed agent, as returned by `Runtime::discover`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInstallation {
    pub id: AgentId,
    pub name: String,
    pub executable_path: PathBuf,
    pub source: InstallationSource,
    pub auth: Option<AuthStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) acp_args: Option<Vec<String>>, // launch args for acp harness
}

impl AgentInstallation {
    /// Uses an exact executable, for catalog agents if we know path.
    pub fn at(id: impl Into<AgentId>, executable: impl Into<PathBuf>) -> Self {
        let id = id.into();
        Self {
            name: id.to_string(),
            id,
            executable_path: executable.into(),
            source: InstallationSource::Pinned,
            auth: None,
            acp_args: None,
        }
    }

    /// Any ACP agent not in the catalog: generic ACP launch with the given
    /// args. For new agents not in catalog.
    pub fn acp(name: impl Into<String>, executable: impl Into<PathBuf>, args: Vec<String>) -> Self {
        let name = name.into();
        Self {
            id: AgentId::new(&name),
            name,
            executable_path: executable.into(),
            source: InstallationSource::Pinned,
            auth: None,
            acp_args: Some(args),
        }
    }
}

/// Whether, and how, an agent is logged in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthStatus {
    Authenticated {
        kind: AuthKind,
        account: Option<AccountInfo>,
    },
    Unauthenticated {
        login: Vec<LoginMethod>,
    },
    Unknown,
}

/// The login kind decides which features exist (plan usage needs a subscription).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthKind {
    Subscription,
    ApiKey,
    CloudProvider,
    Other(String), // I hate dealing with auth
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountInfo {
    pub email: Option<String>,
    pub plan: Option<String>,
}

/// How the user can log in - for login hints
/// Feature: think on how we can allow login through us.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum LoginMethod {
    /// Run this full argv in a terminal the user can see.
    Terminal {
        command: Vec<String>,
        env: BTreeMap<String, String>,
        description: String,
    },
    /// Set this environment variable before `open`.
    EnvVar { name: String },
}

/// Basic harness capabilities. Filled by harness on supports()
/// TODO: need ot explore and add more.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Capability {
    Images,
    Resume,
    Steer,
    Permissions,
    Questions,
    Rollback,
    Fork,
    SlashCommands,
    Plan,
    Subagents,
    ContextUsage,
    PlanUsage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum McpTransport {
    Stdio,
    Http,
    Sse,
}

/// A client-owned MCP server the agent should connect to, forwarded at open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServer {
    pub(crate) name: String,
    pub(crate) connection: McpConnection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum McpConnection {
    Stdio {
        command: PathBuf,
        args: Vec<String>,
        env: BTreeMap<String, String>,
    },
    Http {
        url: String,
        headers: BTreeMap<String, String>,
    },
    Sse {
        url: String,
        headers: BTreeMap<String, String>,
    },
}

impl McpServer {
    /// A server the agent launches itself. `command` should be absolute.
    pub fn stdio(
        name: impl Into<String>,
        command: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            connection: McpConnection::Stdio {
                command: command.into(),
                args: args.into_iter().map(Into::into).collect(),
                env: BTreeMap::new(),
            },
        }
    }

    /// A server already running over streamable HTTP.
    pub fn http(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connection: McpConnection::Http {
                url: url.into(),
                headers: BTreeMap::new(),
            },
        }
    }

    /// A server already running over SSE.
    pub fn sse(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connection: McpConnection::Sse {
                url: url.into(),
                headers: BTreeMap::new(),
            },
        }
    }

    /// Connection metadata: an env var for stdio, an HTTP header otherwise.
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        match &mut self.connection {
            McpConnection::Stdio { env, .. } => env.insert(key.into(), value.into()),
            McpConnection::Http { headers, .. } | McpConnection::Sse { headers, .. } => {
                headers.insert(key.into(), value.into())
            }
        };
        self
    }

    pub(crate) fn transport(&self) -> McpTransport {
        match self.connection {
            McpConnection::Stdio { .. } => McpTransport::Stdio,
            McpConnection::Http { .. } => McpTransport::Http,
            McpConnection::Sse { .. } => McpTransport::Sse,
        }
    }
}

/// Effective caller actions for one agent or session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    features: BTreeSet<Capability>,
    pub mcp_transports: Vec<McpTransport>,
}

impl Capabilities {
    pub fn new(features: impl IntoIterator<Item = Capability>) -> Self {
        Self {
            features: features.into_iter().collect(),
            mcp_transports: Vec::new(),
        }
    }

    pub fn supports(&self, cap: Capability) -> bool {
        self.features.contains(&cap)
    }
}

/// A session setting the agent advertises. Well-known ids: `model`, `effort`,
/// `mode`, `sandbox`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigOption {
    pub id: ConfigId,                 // "model", "effort", "mode", "sandbox"
    pub name: String,                 // human label
    pub category: Option<String>,     // wire's own grouping ("model", "mode"...)
    pub kind: ConfigKind,             // what values it accepts
    pub current: Option<ConfigValue>, // what it's set to now
    pub live: bool,                   // can it change mid-session?
}
//TODO: Look into implementation of changing non-live ones and if we should make interface useage the same.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConfigKind {
    Select { choices: Vec<ConfigChoice> },
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigChoice {
    pub value: String,
    pub label: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigValue {
    Text(String),
    Bool(bool),
}

impl From<&str> for ConfigValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<bool> for ConfigValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlashCommand {
    pub name: String,
    pub description: String,
    pub input_hint: Option<String>,
}

/// What `probe` and `open` learn about an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDetails {
    pub version: Option<String>,
    pub auth: AuthStatus,
    pub capabilities: Capabilities,
    pub config_options: Vec<ConfigOption>,
    pub commands: Vec<SlashCommand>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionConfiguration {
    pub options: BTreeMap<ConfigId, ConfigValue>,
}

/// One live change to an advertised option. The model is the option `model`
/// on every agent that can switch it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConfigSelection {
    Option { id: ConfigId, value: ConfigValue },
}

impl ConfigSelection {
    pub fn option(id: impl Into<ConfigId>, value: impl Into<ConfigValue>) -> Self {
        Self::Option {
            id: id.into(),
            value: value.into(),
        }
    }
}

/// Who answers tool permission requests. ANYAGENT POLICY not harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PermissionMode {
    // TODO: Review to see if we need this simplification.
    Ask,         // sends evey request to your app
    AutoApprove, // auto approve every request.
}

/// Creation-time settings for `Runtime::open`.
#[derive(Debug, Clone)]
pub struct SessionOptions {
    pub(crate) cwd: PathBuf,
    pub(crate) resume: Option<ResumeToken>,
    pub(crate) permission_mode: PermissionMode,
    pub(crate) quiet_window: Option<Duration>,
    pub(crate) mcp_servers: Vec<McpServer>,
    pub(crate) configure: Vec<(ConfigId, ConfigValue)>,
}
// Stuff you start the session with.
impl SessionOptions {
    /// Working directory the agent runs in. Required.
    pub fn in_dir(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            resume: None,
            permission_mode: PermissionMode::Ask,
            quiet_window: None,
            mcp_servers: Vec::new(),
            configure: Vec::new(),
        }
    }

    /// Set an advertised config option at creation — the path for options
    /// with `live: false` (e.g. a non-live `model`). `open` fails with
    /// `InvalidConfiguration` when the agent refuses the value.
    pub fn configure(mut self, id: impl Into<ConfigId>, value: impl Into<ConfigValue>) -> Self {
        self.configure.push((id.into(), value.into()));
        self
    }

    /// Resume a provider session instead of creating a new one.
    pub fn resume(mut self, token: ResumeToken) -> Self {
        self.resume = Some(token);
        self
    }

    /// Forward a client-owned MCP server. `open` fails with
    /// `UnsupportedFeature` when the agent lacks the server's transport.
    pub fn mcp_server(mut self, server: McpServer) -> Self {
        self.mcp_servers.push(server);
        self
    }

    pub fn permission_mode(mut self, mode: PermissionMode) -> Self {
        self.permission_mode = mode;
        self
    }

    /// Overrides the quiet window used to infer turn end on agents whose
    /// wire does not end turns deterministically. Diagnostics only.
    pub fn quiet_window(mut self, window: Duration) -> Self {
        self.quiet_window = Some(window);
        self
    }

    pub fn cwd(&self) -> &PathBuf {
        &self.cwd
    }
}

/// One prompt: text plus optional attachments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Input {
    pub(crate) text: String,
    pub(crate) attachments: Vec<PathBuf>,
}

impl Input {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            attachments: Vec::new(),
        }
    }

    /// Attach a file by path. Every attachment is referenced by path in the
    /// prompt text so the agent can open it; images under the inline cap are
    /// additionally sent as image blocks when the wire supports them.
    pub fn attach(mut self, path: impl Into<PathBuf>) -> Self {
        self.attachments.push(path.into());
        self
    }

    pub fn as_text(&self) -> &str {
        &self.text
    }
}

impl From<&str> for Input {
    fn from(text: &str) -> Self {
        Self::text(text)
    }
}

impl From<String> for Input {
    fn from(text: String) -> Self {
        Self::text(text)
    }
}
