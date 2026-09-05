//! One Rust interface to the coding agents installed on a machine.
//!
//! anyagent finds the agent CLIs a user already has — Claude Code, Codex,
//! Grok, Hermes, OpenCode, Kiro, Qwen — speaks each one's protocol (native
//! or ACP), and exposes them all through three objects:
//!
//! - [`Runtime`] — find agents and open sessions: [`Runtime::discover`],
//!   [`Runtime::probe`], [`Runtime::open`], [`Runtime::generate`],
//!   [`Runtime::plan_usage`].
//! - [`Session`] — one live conversation: [`Session::prompt`],
//!   [`Session::answer`], [`Session::configure`], [`Session::cancel`],
//!   [`Session::rollback`], [`Session::close`].
//! - [`Events`] — the stream everything arrives on, as the same typed
//!   [`EventKind`]s for every agent.
//!
//! The `examples/` directory shows the patterns; `chat.rs` is the core loop:
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use anyagent::{EventKind, Runtime, SessionOptions};
//! use futures::StreamExt;
//!
//! let runtime = Runtime::new();
//! let report = runtime.discover().await;
//! let agent = report.require("claude")?;
//! let (session, mut events) = runtime.open(agent, SessionOptions::in_dir(".")).await?;
//!
//! session.prompt("explain this repo").await?;
//! while let Some(event) = events.next().await {
//!     match event?.kind {
//!         EventKind::TextDelta { text, .. } => print!("{text}"),
//!         EventKind::TurnEnded { .. } => break,
//!         _ => {}
//!     }
//! }
//! session.close().await?;
//! # Ok(()) }
//! ```
//!
//! # Features
//!
//! **Discovery.** [`Runtime::discover`] is read-only and instant: it finds
//! executables (env overrides, PATH, the login-shell PATH, known install
//! dirs) and offline login markers, and lists supported-but-missing agents
//! with install hints. It never launches anything.
//!
//! **Probe.** [`Runtime::probe`] opens a throwaway session (~1 s) to learn
//! what only the agent can tell you, returned as [`AgentDetails`]: version,
//! real login state ([`AuthStatus`] — who is logged in, or runnable
//! [`LoginMethod`]s if not), capabilities, config options, and slash
//! commands.
//!
//! **Sessions and events.** [`Runtime::open`] spawns the agent and returns a
//! [`Session`] handle plus one [`Events`] stream. Every agent produces the
//! same events — streamed text and reasoning, typed tool calls with diffs
//! ([`ToolUpdate`]), plan updates, context usage, turn boundaries — so your
//! app is written once. Turn rules are enforced by anyagent, not trusted
//! from the provider: exactly one `TurnEnded` per turn, no phantom turns.
//! Anything provider-specific rides in [`Event::extensions`] instead of
//! leaking into the types. The UI state — [`SessionStatus`]: working,
//! needing input, idle — is pushed as `StatusChanged` on every flip and
//! read on demand with [`Session::status`], so thread lists and "needs
//! you" badges need no event reducer.
//!
//! **One-shot generation.** [`Runtime::generate`] is prompt in, string out:
//! it opens a throwaway session, declines every tool request, and returns
//! the reply text. Thread titles, commit messages, branch names, and PR
//! bodies need this, not a conversation.
//!
//! **Prompting.** [`Session::prompt`] takes text or an [`Input`] with file
//! attachments (images inline when the wire supports them). A prompt sent
//! mid-turn steers the running turn on agents that support it, and queues
//! otherwise — the returned [`Delivery`] says which happened. Slash commands
//! from [`AgentDetails::commands`] are sent as plain text.
//!
//! **Permissions and questions.** When the agent wants to run a tool or ask
//! the user something, a [`Request`] arrives on the stream; answer it with
//! [`Session::answer`]. Or set [`PermissionMode::AutoApprove`] at open for
//! unattended runs.
//!
//! **Configuration.** Agents advertise their settings — model, effort, mode,
//! sandbox — as [`ConfigOption`]s with typed choices; your picker reads them
//! instead of hardcoding a catalog. Set creation-only options through
//! [`SessionOptions::configure`], switch live ones with
//! [`Session::configure`]; the applied change comes back as
//! `SessionUpdated`.
//!
//! **Resume, fork, rollback.** Every session mints a [`ResumeToken`] at
//! open: store it and continue the conversation from any process with
//! [`SessionOptions::resume`]. [`SessionOptions::fork_from`] branches a
//! conversation; [`Session::rollback`] rewinds it, optionally restoring the
//! files the dropped turns changed. All gated by [`Capability`], so
//! unsupported agents refuse with a typed error instead of misbehaving.
//!
//! **Auth.** [`Runtime::probe_auth`] answers the real login state cheaply —
//! from the agent itself, not just credential-file guesses. A logged-out
//! agent reports [`AuthStatus::Unauthenticated`] with runnable
//! [`LoginMethod`]s (the exact terminal command, or the env var to set) for
//! your app to surface.
//!
//! **Plan usage.** [`Runtime::plan_usage`] reads the logged-in account's
//! quota windows (session/week, plan name) from the agent itself — no HTTP
//! endpoints, no token handling.
//!
//! **MCP forwarding.** Hand the agent your app's MCP servers at open with
//! [`SessionOptions::mcp_server`] (stdio, HTTP, or SSE, checked against the
//! agent's transports).
//!
//! **Capabilities.** [`AgentDetails::capabilities`] says what each agent
//! supports — [`Capability::Steer`], `Fork`, `Rollback`, `Subagents`,
//! `PlanUsage`, … — so the UI can gate features per agent instead of
//! special-casing agent names.
//!
//! Your application keeps what is rightfully its own: the transcript store,
//! titles, worktrees, and UI. anyagent owns the processes, the wires, and
//! the turn rules.

mod adapter;
mod agent;
mod catalog;
mod discovery;
mod error;
mod event;
mod process;
mod runtime;
mod session;

pub use agent::*;
pub use error::AgentError;
pub use event::*;
pub use runtime::{AgentPlanUsage, DiscoveryReport, MissingAgent, Runtime};
pub use session::{Events, Session, SessionInfo, SessionStatus};
