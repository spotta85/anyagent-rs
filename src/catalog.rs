//! Per-agent facts, kept as data. Adding a supported agent is one entry here.

use crate::agent::AuthKind;

/// Everything discovery and the adapters need to know about one agent.
pub(crate) struct AgentProfile {
    pub id: &'static str,
    pub name: &'static str,
    /// Binary the user installs (`claude`, `codex`, `hermes`).
    pub cli: &'static str,
    /// Env var that overrides executable resolution (tests, custom installs).
    pub executable_env: &'static str,
    /// Config directory under the user's home (`.claude`), and the env var
    /// that relocates it.
    pub config_dir: &'static str,
    pub config_home_env: Option<&'static str>,
    /// Which adapter drives it and how to put the CLI in protocol mode.
    pub connection: Connection,
    /// Presence of any marker means "logged in", read offline. Empty when
    /// the agent has no known marker (auth reported as `None`).
    pub auth_markers: &'static [AuthMarker],
    /// Args after the executable that start the agent's own login flow;
    /// empty when the agent only logs in interactively.
    pub login_args: &'static [&'static str],
    /// Shown in `MissingAgent` when the agent is not found.
    pub install_hint: &'static str,
    /// Extra install locations searched last. `~`-relative unless absolute.
    pub extra_paths: &'static [&'static str],
}

pub(crate) enum Connection {
    Acp { args: &'static [&'static str] },
    Native(NativeKind),
}

pub(crate) enum NativeKind {
    Claude,
    Codex,
    /// Antigravity's `agy` CLI: its own stream-json event dialect
    /// (`--input-format=stream-json`, validated 2026-08-23). Adapter pending.
    Antigravity,
}

/// One offline sign that the user is logged in, and what kind of login it is.
pub(crate) enum AuthMarker {
    /// File under the agent's config home.
    ConfigFile(&'static str, AuthKind),
    /// macOS keychain generic-password service.
    Keychain(&'static str, AuthKind),
    /// API key environment variable; doubles as the `EnvVar` login method.
    ApiKeyEnv(&'static str),
}

/// The supported agents. ACP agents beyond these land with their verified
/// launch quirks (P2); guessed wire flags do not ship.
pub(crate) static PROFILES: &[AgentProfile] = &[
    AgentProfile {
        id: "claude",
        name: "Claude Code",
        cli: "claude",
        executable_env: "ANYAGENT_CLAUDE_BIN",
        config_dir: ".claude",
        config_home_env: Some("CLAUDE_CONFIG_DIR"),
        connection: Connection::Native(NativeKind::Claude),
        auth_markers: &[
            AuthMarker::ConfigFile(".credentials.json", AuthKind::Subscription),
            AuthMarker::Keychain("Claude Code-credentials", AuthKind::Subscription),
            AuthMarker::ApiKeyEnv("ANTHROPIC_API_KEY"),
        ],
        login_args: &["auth", "login"],
        install_hint: "npm install -g @anthropic-ai/claude-code",
        extra_paths: &[".claude/local", ".local/bin"],
    },
    AgentProfile {
        id: "codex",
        name: "Codex",
        cli: "codex",
        executable_env: "ANYAGENT_CODEX_BIN",
        config_dir: ".codex",
        config_home_env: Some("CODEX_HOME"),
        connection: Connection::Native(NativeKind::Codex),
        auth_markers: &[
            AuthMarker::ConfigFile("auth.json", AuthKind::Subscription),
            AuthMarker::ApiKeyEnv("OPENAI_API_KEY"),
        ],
        login_args: &["login"],
        install_hint: "npm install -g @openai/codex",
        extra_paths: &[],
    },
    // Gemini CLI is deprecated upstream (personal OAuth sunset, users moved
    // to Antigravity); its profile was replaced 2026-08-23.
    AgentProfile {
        id: "antigravity",
        name: "Antigravity",
        cli: "agy",
        executable_env: "ANYAGENT_ANTIGRAVITY_BIN",
        config_dir: ".gemini",
        config_home_env: None,
        connection: Connection::Native(NativeKind::Antigravity),
        auth_markers: &[
            AuthMarker::ConfigFile("jetski-standalone-oauth-token", AuthKind::Subscription),
            AuthMarker::ConfigFile("oauth_creds.json", AuthKind::Subscription),
        ],
        login_args: &[],
        install_hint: "install Antigravity from https://antigravity.google, then run `agy install`",
        extra_paths: &[".local/bin"],
    },
    AgentProfile {
        id: "grok",
        name: "Grok",
        cli: "grok",
        executable_env: "ANYAGENT_GROK_BIN",
        config_dir: ".grok",
        config_home_env: None,
        // Flag placement verified against grok 1.0.4 (comet field notes):
        // `--no-auto-update` is TOP-LEVEL and kills a silent multi-second
        // launch-time update check; `--no-leader` (on the subcommand) starts
        // a fresh agent instead of attaching to a shared leader process via
        // ~/.grok/leader.sock — a wedged/stale leader reads as total silence.
        connection: Connection::Acp {
            args: &["--no-auto-update", "agent", "--no-leader", "stdio"],
        },
        auth_markers: &[AuthMarker::ApiKeyEnv("XAI_API_KEY")],
        login_args: &[],
        install_hint: "npm install -g @xai-official/grok \
             (or `curl -fsSL https://x.ai/cli/install.sh | bash`)",
        extra_paths: &[".local/bin", ".grok/bin", ".npm-global/bin"],
    },
    AgentProfile {
        id: "hermes",
        name: "Hermes Agent",
        cli: "hermes",
        executable_env: "ANYAGENT_HERMES_BIN",
        config_dir: ".hermes",
        config_home_env: None,
        connection: Connection::Acp { args: &["acp"] },
        auth_markers: &[AuthMarker::ConfigFile("auth.json", AuthKind::ApiKey)],
        login_args: &["login"],
        install_hint: "see https://github.com/NousResearch/hermes-agent",
        extra_paths: &[".local/bin"],
    },
    AgentProfile {
        id: "opencode",
        name: "opencode",
        cli: "opencode",
        executable_env: "ANYAGENT_OPENCODE_BIN",
        // Data home; `opencode auth login` writes auth.json here.
        config_dir: ".local/share/opencode",
        config_home_env: None,
        connection: Connection::Acp { args: &["acp"] },
        auth_markers: &[AuthMarker::ConfigFile("auth.json", AuthKind::Subscription)],
        login_args: &["auth", "login"],
        install_hint: "brew install sst/tap/opencode",
        extra_paths: &[],
    },
    AgentProfile {
        id: "qwen",
        name: "Qwen Code",
        cli: "qwen",
        executable_env: "ANYAGENT_QWEN_BIN",
        config_dir: ".qwen",
        config_home_env: None,
        connection: Connection::Acp {
            args: &["--experimental-acp"],
        },
        auth_markers: &[AuthMarker::ConfigFile(
            "oauth_creds.json",
            AuthKind::Subscription,
        )],
        login_args: &[],
        install_hint: "npm install -g @qwen-code/qwen-code",
        extra_paths: &[],
    },
];
