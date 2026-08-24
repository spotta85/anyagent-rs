# anyagent

> **Ollama, but for coding agents.** One Rust dependency finds supported coding
> agents already installed on a machine, connects through the best available
> protocol, and gives the application one typed interface.

Status: public interface drafted for P0, revised 2026-08-22 after reviewing
Comet, laptop-agent, T3 Code, vibe-kanban, ACP Kit, and the official ACP
adapters. S0 ran 2026-08-23 and settled the ACP implementation (own wire +
schema types, see "ACP implementation"). No crate has been implemented yet. Decisions and extension rules are listed at the end;
wayfinder map and tickets live in `.scratch/anyagent/`.

## Vision

Applications such as laptop-agent, Comet, and T3 Code should not have to build
the same agent infrastructure:

```text
discover executable
  -> build a GUI-safe environment
  -> launch the agent
  -> connect and handshake
  -> correlate protocol requests
  -> manage turns, steering, permissions, and cancellation
  -> normalize provider events
  -> clean up the child process
```

Anyagent owns that work once. Applications keep their product behavior, storage,
UI, worktrees, voice, Git integration, and orchestration.

```text
Application
    |
    | Runtime / Session / Events
    v
anyagent session engine
    |
    | private driver commands and events
    v
ACP adapter | Codex adapter | future native adapters
    |
    v
installed agent CLI
```

ACP is an implementation detail. Anyagent may choose ACP for an ACP-native agent
and a native protocol where it is more reliable or exposes needed behavior.

Rust applications use the crate directly. TypeScript and Swift can attach later
through a JSONL sidecar, bindings, or a persistent daemon.

## What clients get

| Feature | Client behavior | Phase |
|---|---|---|
| Local discovery | List supported agents installed through PATH, known locations, and common version managers; list known-but-missing agents with install hints; read login markers offline | P0 |
| Live inspection | Read version, auth status, capabilities, models, configuration options, and commands without keeping a session open | P0 to P2 |
| Auth status | Know whether an agent is logged in, how (subscription, API key, cloud provider), and what to run to log in; get a typed `AuthRequired` when a session loses auth | P0 status, P2 login hints |
| Unified connection | Let Anyagent select ACP or a native protocol and return one ready session | ACP and native Claude in P0, Codex in P3 |
| Persistent sessions | Start turns on one provider session, cancel a turn without losing context, resume, fork, or roll back when supported | New in P0, resume, fork, and rollback in P2 |
| Prompt delivery | Start immediately when idle, steer a live turn when supported, or queue safely; inspect and drop queued prompts | P0 to P1 |
| Permissions and questions | Receive typed requests, answer them once, and choose ask or automatic permission modes | P0 to P1 |
| Typed event stream | Consume ordered text, reasoning, tool, request, diagnostic, and completion events | P0 to P2 |
| Usage | Context-window usage per session (most agents) and plan/rate-limit windows per account (Claude and Codex only), each gated by a capability | Context in P2, plan usage in P2 (Claude) and P3 (Codex) |
| Rich input and MCP | Send text and images, then forward client-owned MCP server declarations | P1 |
| Agent configuration | Read and select models, effort, modes, sandbox, commands, and other advertised session options | P1 to P2 |
| Process lifecycle | Get typed spawn, auth, protocol, and exit failures with stderr diagnostics and guaranteed cleanup | P0 |
| Agent catalog | Add a new ACP agent by adding one profile entry (launch args, env override, install paths, quirks), not a new driver | P0 |
| Account isolation | Point a session at a separate config home so one machine can run several logins of the same agent | P2 |
| Wire recording | Record raw protocol frames for a session to a file for bug reports | P2 |
| Cross-language access | Control the same interface through a versioned JSONL sidecar and later bindings | P4 |

Anyagent does not promise that every agent supports every feature. It reports
caller-actionable capabilities and normalizes fallback behavior where it can.
Capabilities depend on the agent, the protocol path, and sometimes the login
kind (plan usage exists for a Claude subscription but not for an API key), so
they are reported per session after `open`, not hard-coded per agent.

## Client example

```rust
let runtime = anyagent::Runtime::new();

// Read-only inventory. No agent is launched.
let report = runtime.discover().await;
let agent = report.require("hermes")?;

// Optional: if not logged in, drive the agent's own login flow first.
if let AuthStatus::Unauthenticated { .. } = runtime.probe(agent).await?.auth {
    let mut login = runtime.login(agent, None).await?;
    while let Some(step) = login.events.next().await {
        ui.show_login_step(step); // OpenUrl / Output / Finished
    }
}

// Anyagent selects the connection, launches the CLI, completes the handshake,
// and returns the command handle plus the event stream.
let (session, mut events) = runtime
    .open(
        agent,
        SessionOptions::in_dir(repo)
            .permission_mode(PermissionMode::Ask),
    )
    .await?;
let delivery = session.prompt("fix the failing tests").await?;

while let Some(event) = events.next().await {
    match event?.kind {
        EventKind::TextDelta { text, .. } => ui.append(text),
        EventKind::ReasoningDelta { text, .. } => ui.thinking(text),
        EventKind::ToolUpdated(tool) => ui.tool(tool),
        EventKind::RequestOpened(request) => {
            let id = request.id();
            session.answer(id, ui.answer(&request).await).await?;
        }
        EventKind::TurnEnded { stop, .. } => {
            ui.finish(delivery, stop);
            break;
        }
        _ => {}
    }
}

session.close().await?;
```

The client does not spawn a process, parse JSON-RPC, perform an ACP handshake,
track request IDs, decide whether to steer or queue, or clean up the child.
Those behaviors stay behind the public interface.

## What success means

The deletion test is the product test. If Anyagent disappeared, this complexity
would return to every consumer.

| Consumer | Anyagent should replace | Consumer keeps |
|---|---|---|
| laptop-agent | ACP connection, framing, session setup, MCP forwarding, permissions, steering, cancellation, and event translation | Voice, screen capture, routing, and approval UI |
| Comet | Discovery, shell environment resolution, ACP drivers, process lifecycle, and shared normalization | Worktrees, Git, persistence, remote sync, run journals, and UI |
| T3 Code-style clients | Provider launch, protocol connection, capability probing, and event translation | Reducers, checkpoints, workspaces, terminals, and frontend |

P1 succeeds only when laptop-agent can delete its ACP implementation. P2 succeeds
only when Comet can delete its shared ACP layer. P3 proves the same public
interface with native Codex.

"Drop-in" means a thin product bridge, not a source-compatible trait swap.
Existing applications keep their event reducer while deleting their harness
mechanics:

| Existing consumer concept | Anyagent replacement |
|---|---|
| Spawn configuration and connection setup | `Runtime::open` |
| Command channel or run controls | cloneable `Session` |
| Harness event receiver or run stream | `Events` |
| Steering flags and prompt queue | `Session::prompt` plus `Delivery` |
| Model, mode, and option probing | `probe` or `Session::info` |
| Provider session ID | opaque `ResumeToken` |
| Provider-context rollback | optional `Session::rollback` |
| Active-session directory and stop-all | consumer-owned collection of `Session` handles |

laptop-agent keeps voice-specific behavior such as `Speak`; Comet keeps its
document reducer and run journal. Within a migrated adapter path, neither keeps
JSON-RPC, ACP state, process ownership, prompt queuing, or completion watchdogs.
P2 replaces only Comet's shared ACP path; its native drivers remain until their
later adapter phases.

## Evidence behind the design

| Reference | Current approach | Size |
|---|---|---|
| Comet `crates/harness` | Native Claude (stream-json + control channel), Codex (app-server JSON-RPC), Cursor (`@cursor/sdk` shim) plus a shared ACP driver for Grok, Hermes, opencode, pi; mock; managed npm adapter installs; login-shell PATH composition; stderr tail; SIGTERM→SIGKILL | ~17.7k lines |
| Comet `crates/engine` | Agent accounts (credential swap, OAuth add-account, plan usage probes over HTTP), quiesce watchdog, parked sessions, run journal recovery | ~2k lines for accounts alone |
| laptop-agent `core/src/harness` | One ACP driver: prompt/steer/queue, permissions, `set_config_option` for mode, image blocks, MCP forwarding | ~900 lines |
| [T3 Code](https://github.com/pingdotgg/t3code) `apps/server/src/provider` | Native Claude (Agent SDK), Codex (app-server), ACP for Cursor/Grok, opencode SDK. Provider snapshot = installed, version, auth {status, type, email}, models, slash commands, skills, update advisory. Events include `auth.status`, `account.rate-limits.updated`, `thread.token-usage.updated`, `model.rerouted`, `mcp.status.updated` | Claude adapter alone 4.6k lines |
| [vibe-kanban](https://github.com/BloopAI/vibe-kanban) `crates/executors` (Rust) | Spawn-per-turn executors (`spawn`, `spawn_follow_up(session_id, reset_to_message_id)`), log normalizers, `AvailabilityInfo`, capabilities `SessionFork`/`SetupHelper`/`ContextUsage`; uses `agent-client-protocol` 0.8 for its ACP executor | ~26k lines |
| [ACP Kit](https://github.com/AcpKit/acp-kit) (TypeScript) | Runtime over the official SDK: agent profiles, PATH detection, normalized events with ids, `auth_required` retry, inspector, session recording | the TS equivalent of this crate |
| [claude-agent-acp](https://github.com/agentclientprotocol/claude-agent-acp) / [codex-acp](https://github.com/agentclientprotocol/codex-acp) | The only ACP paths to Claude Code and Codex; npm packages. Steering via `_session/steering`; Claude puts rate limits in `usage_update._meta["_claude/rateLimit"]`; codex-acp does not expose rate limits over ACP at all | — |
| [Official ACP Rust SDK](https://github.com/agentclientprotocol/rust-sdk) | 2.0.0 on crates.io (pins schema 1.5.0; spec is 1.7.0); `Send` futures; transport → incoming queue is unbounded; typed matchers reject unknown `sessionUpdate` kinds; draft v2 behind `unstable_protocol_v2` | — |

What the research settled:

- Every consumer reduces to the same shape: spawn child, frame JSONL, correlate
  ids, map to one event enum, steering mailbox, cancellation token, kill
  escalation. Anyagent owns exactly that.
- Comet tried ACP adapters for Claude and Codex, then restored native drivers
  after turn-completion behavior caused wrong done states. T3 and vibe-kanban
  never used ACP for them. The ACP/native seam is necessary, and native Claude
  and Codex are the two adapters users will actually want first.
- Plan usage (5-hour / weekly windows) never comes over ACP in a usable form.
  Claude emits `rate_limit_event` on its native wire; Codex emits
  `account/rateLimits/updated` on app-server; Comet also polls HTTP endpoints
  with the CLI's OAuth token. Context-window usage does come over ACP
  (`usage_update {used, size}`), so these are two separate features.
- Auth status is probed three ways in the wild: read credential files (Comet),
  `account/read` on Codex app-server, and a no-prompt Claude init probe
  (`system/init.account`, T3). ACP exposes `authMethods`, `auth_required`
  errors, and a `terminal` auth method that tells the client to run the CLI in
  a terminal.
- Multiple logins per agent are done by config-home isolation
  (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`; T3) or credential-file swapping (Comet).
  Home isolation is the part a library can own safely.
- Desktop discovery is real work. A GUI process often cannot see binaries
  installed through nvm, fnm, volta, pnpm, bun, Homebrew, or shell-specific
  PATH configuration. Discovery belongs in P0.
- Per-agent quirks are data: launch args, env override var, extra install
  paths, startup timeout (opencode needs minutes), stdout line filters, a
  proprietary turn-end notification (Grok), steering support. Both Comet and
  ACP Kit keep an agent profile table; anyagent needs one too.

## Design rules

1. One deep public module hides discovery, process, protocol, and session behavior.
2. ACP and provider types never appear in public signatures.
3. The session engine owns behavior shared by every provider.
4. Adapters only connect and translate their protocol.
5. Discovery never launches an agent, installs software, or accesses the network.
6. `open` does not require a prior `probe`.
7. A turn produces exactly one terminal event unless the session itself fails.
8. Installation is always explicit. Runtime code never runs `npx -y` or `@latest`.
9. Start as one crate. Split only when the sidecar or bindings create a dependency need.
10. Keep the adapter seam private until ACP and native Codex both validate it.
11. Callers may invoke optional features directly. Capability inspection is for
    UI and optional setup, not a required preflight before every command.
12. Discovery is optional. A caller may pin a supported agent to an explicit
    executable, while the adapter still owns its launch arguments and protocol.
13. Agent-specific facts live in a catalog of profiles (data), not in code
    branches. Adding an ACP-native agent means adding a profile entry.
14. Capabilities are an open set reported per session. A feature that depends
    on login kind or model is reported after `open`, and changes arrive as
    `SessionUpdated`.
15. Anyagent never stores, copies, or refreshes credentials. It reads login
    state and tells the application what command to run to log in.
16. Every "something is missing" answer is typed and actionable, never a
    string to parse: `MissingAgent` carries searched dirs and an install
    hint, `Unauthenticated` carries runnable `LoginMethod`s. Neither Comet
    nor T3 exposes this; keep it as a differentiator when adding features
    (e.g. an outdated-version report would carry the update command).

## Public interface

Callers learn one entry point and two session handles:

```text
Runtime
  discover()   read-only inventory
  probe()      optional live inspection
  open()       launch, handshake, and create a ready session
                    /           \
               Session          Events
            commands in      ordered stream out
```

### Runtime

```rust
pub struct Runtime {
    /* private registry, configuration, and host dependencies */
}

impl Runtime {
    pub fn new() -> Self;

    /// Best-effort, read-only inventory: which agents exist, where, and
    /// whether a login marker is present. Reads the login-shell PATH once
    /// (cached), never launches an agent, never uses the network.
    pub async fn discover(&self) -> DiscoveryReport;

    /// Captures the login-shell PATH in the background so the first
    /// `discover` is instant. Optional; apps call it at boot.
    pub fn prewarm(&self);

    /// Optional live handshake for settings and diagnostics.
    /// It leaves no persistent session behind.
    pub async fn probe(
        &self,
        agent: &AgentInstallation,
    ) -> Result<AgentDetails, AgentError>;

    /// Returns only after launch, authentication checks, handshake, and
    /// provider session creation succeed.
    pub async fn open(
        &self,
        agent: &AgentInstallation,
        options: SessionOptions,
    ) -> Result<(Session, Events), AgentError>;

    /// Plan quota windows for the logged-in account. Only agents advertising
    /// `Capability::PlanUsage` (Claude and Codex subscriptions) answer; others
    /// return `UnsupportedFeature`. May spawn a short-lived agent process
    /// (Claude: `initialize` → `get_usage` → close, ~1-2 s); cached 60 s.
    /// Live sessions push the same data as `PlanUsageUpdated` after each
    /// turn, so chat UIs never need to call this.
    pub async fn plan_usage(
        &self,
        agent: &AgentInstallation,
    ) -> Result<PlanUsage, AgentError>;

    /// One call for a usage page: every discovered agent with its plan usage
    /// or the typed reason it has none (`UnsupportedFeature` for API-key
    /// logins and agents without quota, `AuthRequired`, ...). Runs the
    /// per-agent calls concurrently.
    pub async fn plan_usage_all(&self) -> Vec<AgentPlanUsage>;

    /// Drives the agent's own login flow and streams what the user must do
    /// (open a URL, enter a code) until it finishes. Anyagent never sees or
    /// stores the resulting credential; the CLI writes it where it always does.
    pub async fn login(
        &self,
        agent: &AgentInstallation,
        method: Option<LoginMethodId>,
    ) -> Result<LoginSession, AgentError>;
}

pub struct AgentPlanUsage {
    pub agent: AgentInstallation,
    pub usage: Result<PlanUsage, AgentError>,
}

pub struct LoginSession {
    /// Ordered events; ends with `Finished`.
    pub events: BoxStream<'static, LoginEvent>,
    /// Aborts the flow and kills any child it started.
    pub cancel: CancelHandle,
}

#[non_exhaustive]
pub enum LoginEvent {
    /// Show or open this URL; `code` is shown next to it when present.
    OpenUrl { url: String, code: Option<String> },
    /// A raw line from the login process, for a fallback console view.
    Output { line: String },
    Finished { status: AuthStatus },
}

pub struct PlanUsage {
    pub windows: Vec<UsageWindow>,
    pub fetched_at: SystemTime,
}

pub struct UsageWindow {
    /// "Session" (5h), "Week", or the agent's own label.
    pub label: String,
    pub used_percent: u8,
    pub resets_at: Option<SystemTime>,
}
```

Discovery returns partial results with diagnostics instead of failing the whole
scan because one PATH entry or known location is unreadable.

```rust
pub struct DiscoveryReport {
    pub agents: Vec<AgentInstallation>,
    /// Known agents that were not found, with where we looked and how to
    /// install them, so a settings page can render them.
    pub missing: Vec<MissingAgent>,
    pub diagnostics: Vec<Diagnostic>,
}

pub struct MissingAgent {
    pub id: AgentId,
    pub name: String,
    pub searched: Vec<PathBuf>,
    pub install_hint: String,
}

impl DiscoveryReport {
    pub fn require(&self, id: impl AsRef<str>)
        -> Result<&AgentInstallation, AgentError>;
}

pub struct AgentInstallation {
    pub id: AgentId,
    pub name: String,
    pub executable: PathBuf,
    pub source: InstallationSource,
    /// Login state read from offline markers (credential file, keychain
    /// item). `None` when the agent has no known marker. `probe` confirms.
    pub auth: Option<AuthStatus>,
}

impl AgentInstallation {
    /// Uses an exact executable for a catalog agent instead of discovery.
    pub fn at(
        id: impl Into<AgentId>,
        executable: impl Into<PathBuf>,
    ) -> Self;

    /// Any ACP agent not in the catalog: generic ACP launch with the given
    /// args. The escape hatch for trying a new agent before a release.
    pub fn acp(
        name: impl Into<String>,
        executable: impl Into<PathBuf>,
        args: Vec<String>,
    ) -> Self;
}

/// String-backed so new built-in agents do not break consumers.
#[serde(transparent)]
pub struct AgentId(String);

impl AgentId {
    pub fn new(value: impl Into<String>) -> Self;
}

pub struct AgentDetails {
    pub version: Option<String>,
    pub auth: AuthStatus,
    pub capabilities: Capabilities,
    pub config_options: Vec<ConfigOption>,
    pub commands: Vec<SlashCommand>,
}

// The model is not a separate type: it is the config option `model`, with
// the agent's catalog as its Select choices (value, label, description).
// One switching mechanism for every option on every wire (Review 2026-08-24).

#[non_exhaustive]
pub enum AuthStatus {
    /// Logged in. `kind` tells subscription from API key from cloud provider,
    /// which is what decides whether plan usage exists.
    Authenticated {
        kind: AuthKind,
        account: Option<AccountInfo>,
    },
    /// Not logged in. `login` says how to fix it.
    Unauthenticated { login: Vec<LoginMethod> },
    /// Could not tell without network or a prompt. Callers may still `open`.
    Unknown,
}

#[non_exhaustive]
pub enum AuthKind { Subscription, ApiKey, CloudProvider, Other(String) }

pub struct AccountInfo {
    pub email: Option<String>,
    pub plan: Option<String>,
}

#[non_exhaustive]
pub enum LoginMethod {
    /// Run this full argv (agent executable + args) in a terminal the user can see.
    Terminal { command: Vec<String>, env: BTreeMap<String, String>, description: String },
    /// Set this environment variable (API key) before `open`.
    EnvVar { name: String },
}
```

Discovery reads login state without network (credential files, `auth.json`,
keychain presence). `probe` may confirm it through the protocol
(`account/read`, a no-prompt init, ACP `authMethods`). A session that hits an
auth error mid-turn ends the turn with `StopReason::Failed` and the stream
yields `AgentError::AuthRequired` carrying the same `LoginMethod` list.

`Runtime::login` fixes the problem instead of only reporting it. Each adapter
drives the agent's own flow and Anyagent never implements OAuth itself:

| Agent path | What `login` does |
|---|---|
| ACP agent with an `agent` auth method | Calls `authenticate`; the agent handles the browser or device flow and reports completion |
| ACP agent with a `terminal` auth method | Anyagent sends the terminal-auth client capability in `initialize`; on login it spawns the agent executable with the advertised `args`/`env` in a PTY, streams lines as `Output`, extracts URLs and device codes into `OpenUrl`, finishes on exit (zero status = success) |
| Native Codex | `account/login/start` returns the auth URL; `account/login/completed` ends the flow |
| Native Claude | Spawns `claude auth login [--console]` over plain pipes (no PTY needed, validated 2026-08-23): it prints the URL (`OpenUrl`), reads a pasted `code#state` line from stdin, exits 0 on `Login successful.` |
| API-key agents | `LoginMethod::EnvVar`; nothing to drive, the app sets the variable and reopens |

Credential copying and account switching by file swap stay in the application
(Comet does this; it is product-specific and fragile). `config_home` is the
supported way to keep logins apart.

Capabilities describe effective caller actions after adapter and runtime
fallbacks. They do not expose framing, connection type, or watchdog behavior.

```rust
pub struct Capabilities {
    features: BTreeSet<Capability>,
    pub mcp_transports: Vec<McpTransport>,
}

impl Capabilities {
    pub fn supports(&self, cap: Capability) -> bool;
}

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
```

`Capability` is an open enum so new features do not break consumers. Collections
represent choices: configuration options, commands, and MCP transports.
An empty collection means the choice is unavailable. This matters for clients
such as laptop-agent, which can enable its voice MCP server only when HTTP MCP
is supported, and which shows "Steered" versus "Queued" based on `Steer`.

```rust
/// A client-owned MCP server the agent should connect to, forwarded at open.
pub struct McpServer { /* private */ }
impl McpServer {
    pub fn stdio(name, command: impl Into<PathBuf>, args) -> Self;
    pub fn http(name, url) -> Self;
    pub fn sse(name, url) -> Self;
    /// Connection metadata: an env var for stdio, an HTTP header otherwise.
    pub fn with(self, key, value) -> Self;
}
```

MCP forwarding (P1 slice 2, done 2026-08-23): `SessionOptions::mcp_server`
declares servers; adapters translate them — ACP `mcpServers` in `session/new`
and `session/load` (name/value pair shapes), Claude an inline-JSON
`--mcp-config` launch arg (map shapes; no `--strict-mcp-config`, the agent's
own servers stay). `capabilities.mcp_transports` is populated on both wires:
ACP always Stdio plus Http/Sse from `mcpCapabilities`; Claude all three. A
declaration whose transport the agent lacks fails `open` with
`UnsupportedFeature` — never silently dropped. Live-verified 2026-08-23:
real claude (stdio, via `--mcp-config`) and real hermes (stdio, via
`session/new`) each connected a scripted one-tool MCP server and returned
its magic word; the tool surfaced typed as `ToolKind::Mcp {server, tool}`.

Creation-time config (added 2026-08-24, v0 live finding 9):
`SessionOptions::configure(id, value)` sets an advertised option at open —
the only path for `live: false` options like opencode's `model`. ACP applies
each one after `session/new`/`session/load` (`session/set_mode` for `mode`,
`session/set_config_option` otherwise); Claude maps `mode` to the
`--permission-mode` launch arg and refuses other ids. A refusal fails `open`
with `InvalidConfiguration` — never a silently misconfigured session.

`Steer` is advertised because UIs change their composer for it, but `prompt`
still works either way and reports what happened through `Delivery`. Turn-end
reliability stays internal because Anyagent guarantees normalized completion.
`probe` returns a point-in-time inspection; details from `open` are
authoritative for that session, and `SessionUpdated` reports later changes
(login completed, model switched, agent advertised new commands).

`SessionOptions` requires a working directory. Its builder accepts an optional
resume token or `fork_from(token, at: Option<MessageId>)`, initial
configuration, permission mode, MCP server declarations, environment
overrides, a config home, a wire recording path, and namespaced adapter
extensions. Fork opens a *new* provider session that starts with the old
conversation (Claude `--fork-session` / `resumeSessionAt`, Codex
`thread/fork`); the original stays untouched and the new session gets its own
`ResumeToken`. Fork is one shape of `open`, not a separate method. `Ask` is the default permission mode. Each `ConfigOption`
describes its allowed values, default, and whether it is creation-only or live.

Well-known config ids let applications build pickers without knowing the
agent: `model`, `effort`, `mode`, `sandbox`. Adapters map these onto whatever
the agent calls them (ACP `thought_level`, Codex `reasoning_effort`, Claude
`effort`). Any other advertised option keeps the agent's own id.

`config_home(path)` points the agent at a separate configuration directory
(`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, or the ACP agent's equivalent env var). That
is how one machine runs two logins of the same agent. Anyagent only sets the
variable; the application owns what lives in that directory.

The interaction surface stays narrow:

| Need | One public path |
|---|---|
| Choose creation-only settings | `SessionOptions` |
| Send text, images, a slash command, or per-turn selections | `Session::prompt(Input)` |
| Drop a prompt waiting behind a live turn | `Session::dequeue` |
| Branch a conversation into a new session | `Runtime::open` with `SessionOptions::fork_from` |
| Change a live model, effort, mode, or advertised option | `Session::configure(ConfigSelection)` |
| Resolve a permission or agent question | `Session::answer` |
| Rewind provider context when supported | `Session::rollback` |
| Interrupt or shut down | `Session::cancel` / `Session::close` |
| Read plan quota for the logged-in account | `Runtime::plan_usage(agent)`; all agents at once: `plan_usage_all()` |
| Observe everything else | `Events` |

```rust
session
    .configure(ConfigSelection::option("model", "sonnet"))
    .await?;
session
    .prompt(Input::text("explain this screenshot").attach("shot.png"))
    .await?;
session.prompt("/review").await?;
```

The same calls work for every adapter. The session engine validates support,
handles safe timing, and returns `UnsupportedFeature` or
`InvalidConfiguration` when it cannot honor a request. The caller never sends
raw ACP or provider JSON.

`SessionOptions` and `Input` may carry namespaced `Extensions` for a documented
adapter-only knob that has no portable meaning yet. Unknown keys fail instead
of being ignored. There is deliberately no arbitrary raw-RPC method: it could
bypass request correlation and corrupt normalized session state. If multiple
consumers need an extension, promote it into the typed interface.

### Session vocabulary and lifecycle

Anyagent uses `Session` for provider concepts also called a thread or
conversation. There is no second public `Thread` type:

- `open` without a resume token creates a new provider session.
- `open` with an opaque `ResumeToken` resumes it when supported.
- `open` with `fork_from(token, at)` branches it into a new provider session
  when `Capability::Fork` is advertised.
- `SessionId` identifies the current live handle and correlates its events.
- `ResumeToken` is serializable provider-owned data that applications store
  with their conversation record but never parse.

```rust
let options = SessionOptions::in_dir(repo).resume(saved_token);
let opened = runtime.open(agent, options).await?;
if let Some(token) = opened.info().resume_token.clone() {
    store_resume_token(token);
}
```

Anyagent does not list or store application conversations. The application owns
that database and chooses when to create or resume a provider session. Each
`open` is independent, so concurrent chats or worktrees use multiple sessions.

Session state is presented by the command result and ordered events, avoiding a
second state snapshot that can race the stream:

| Observation | Client-visible state |
|---|---|
| `open` is pending | Connecting |
| `open` returns | Ready |
| `TurnStarted` | Running |
| `RequestOpened` | Waiting for the caller; the turn remains active |
| `TurnEnded` | Ready again |
| terminal stream error or closure | Closed |

Callers do not inspect state before issuing commands. `prompt` decides start,
steer, or queue; `cancel` is idempotent; and `answer` validates request
lifetimes.

### Session and Events

```rust
pub struct SessionInfo {
    pub id: SessionId,
    pub agent: AgentInstallation,
    pub details: AgentDetails,
    pub configuration: SessionConfiguration,
    pub resume_token: Option<ResumeToken>,
    /// Agent-suggested title when the protocol provides one.
    pub title: Option<String>,
}

#[derive(Clone)]
pub struct Session {
    /* private command sender and shared state */
}

impl Session {
    pub fn id(&self) -> &SessionId;

    /// Current snapshot (resume token, configuration, capabilities, title);
    /// the same value the last `SessionUpdated` carried. Cheap clone, no await.
    pub fn info(&self) -> SessionInfo;

    /// Starts a turn when idle. During a live turn, steers when possible or
    /// queues the input. The return value reports the chosen delivery.
    pub async fn prompt(
        &self,
        input: impl Into<Input>,
    ) -> Result<Delivery, AgentError>;

    pub async fn answer(
        &self,
        request: RequestId,
        answer: Answer,
    ) -> Result<(), AgentError>;

    /// Accepts one advertised model or configuration-option selection.
    /// Anyagent applies it immediately or at the next safe boundary. Modes
    /// are regular configuration options rather than a special API.
    pub async fn configure(
        &self,
        selection: ConfigSelection,
    ) -> Result<(), AgentError>;

    /// Rewinds provider-owned conversation context by completed turns so the
    /// agent forgets them. Files on disk and the application's transcript
    /// are untouched. Codex does this in place. Claude has no in-place
    /// rollback: the adapter emulates it by respawning the CLI forked at the
    /// cut point (`--resume-session-at`), so the resume token changes and a
    /// `SessionUpdated` event carries the new one; the old session stays on
    /// disk.
    pub async fn rollback(
        &self,
        turns: NonZeroU32,
    ) -> Result<(), AgentError>;

    /// Drops one queued prompt. Returns `InvalidRequest` if it already started.
    /// There is no `queue()` getter: the app learns queue position from
    /// `Delivery::Queued` and learns departure from `TurnStarted`.
    pub async fn dequeue(&self, prompt: PromptId) -> Result<(), AgentError>;

    /// Idempotently interrupts the active turn. The session and the queue
    /// survive; the next queued prompt starts unless `clear_queue` is set.
    pub async fn cancel(&self, clear_queue: bool) -> Result<(), AgentError>;

    /// Ends the provider session and child process. Dropping all handles also
    /// triggers cleanup, but close allows deterministic application shutdown.
    pub async fn close(&self) -> Result<(), AgentError>;
}

pub struct SessionConfiguration {
    pub options: BTreeMap<ConfigId, ConfigValue>,
}

#[non_exhaustive]
pub enum ConfigSelection {
    Option { id: ConfigId, value: ConfigValue },
}

impl ConfigSelection {
    pub fn option(
        id: impl Into<ConfigId>,
        value: impl Into<ConfigValue>,
    ) -> Self;
}

pub struct Delivery {
    /// Stable across immediate delivery and later queue promotion.
    pub prompt_id: PromptId,
    pub kind: DeliveryKind,
}

#[non_exhaustive]
pub enum DeliveryKind {
    Started { turn_id: TurnId },
    Steered { turn_id: TurnId },
    Queued { position: u32 },
}

pub struct Events(
    BoxStream<'static, Result<Event, AgentError>>
);
```

The command handle and stream are separate because real applications control a
session from multiple tasks while one task drains events. No `Arc<Mutex<Session>>`
is required.

### Events

```rust
pub struct Event {
    /// Monotonic within this session.
    pub sequence: u64,
    pub session_id: SessionId,
    pub turn: Option<TurnContext>,
    pub kind: EventKind,
    /// Namespaced provider data for behavior Anyagent has not normalized yet.
    pub extensions: Extensions,
}

/// Keys use `provider/name`, for example `codex/thread_status`.
pub type Extensions = BTreeMap<String, serde_json::Value>;

pub struct TurnContext {
    pub id: TurnId,
    /// Present for events produced by a subagent spawned from this tool call.
    pub parent_tool_id: Option<ToolId>,
}

#[non_exhaustive]
pub enum EventKind {
    TurnStarted {
        origin: TurnOrigin,
    },
    TextDelta {
        message_id: MessageId,
        text: String,
    },
    ReasoningDelta {
        message_id: MessageId,
        text: String,
    },
    /// Complete provider-originated user-role content. Most commonly a parent
    /// agent steering a subagent identified by `TurnContext::parent_tool_id`.
    UserMessage {
        message_id: MessageId,
        text: String,
    },
    /// No more deltas will arrive for this streamed assistant message.
    MessageEnded {
        message_id: MessageId,
    },
    /// Cumulative snapshot of one tool call: emitted on start, on each status
    /// or content change, and on completion.
    ToolUpdated(ToolUpdate),
    /// Streamed stdout/stderr of a running command tool.
    ToolOutputDelta {
        tool_id: ToolId,
        text: String,
    },
    /// The agent's full current task list; replaces the previous one.
    PlanUpdated {
        entries: Vec<PlanEntry>,
    },
    RequestOpened(Request),
    RequestClosed {
        request_id: RequestId,
    },
    SessionUpdated(SessionInfo),
    /// Context-window occupancy for this session. ACP `usage_update`,
    /// Codex `thread/tokenUsage/updated`, Claude `result.usage`.
    ContextUsage {
        used_tokens: u64,
        window_tokens: Option<u64>,
        cost_usd: Option<f64>,
    },
    /// The agent compacted its context. The next `ContextUsage` drops.
    ContextCompacted,
    /// Plan quota for the account, refreshed by the adapter after each turn
    /// (Claude `get_usage`, Codex `account/rateLimits`). Same shape as
    /// `Runtime::plan_usage`.
    PlanUsageUpdated(PlanUsage),
    Diagnostic(Diagnostic),
    TurnEnded {
        stop: StopReason,
        /// Tools still running when the turn ended (subagents, backgrounded
        /// shells). Non-empty means background work continues; a later
        /// agent-originated turn will carry these ids in `parent_tool_id`.
        background: Vec<ToolId>,
    },
}

#[non_exhaustive]
pub enum Request {
    Permission(PermissionRequest),
    Question(QuestionRequest),
}

#[non_exhaustive]
pub enum StopReason {
    Completed { source: CompletionSource },
    Cancelled,
    Refused,
    Failed { message: String },
}

#[non_exhaustive]
pub enum CompletionSource {
    /// The provider protocol explicitly ended the turn.
    Protocol,
    /// Anyagent inferred completion after the provider became quiescent.
    Inferred,
}

#[non_exhaustive]
pub enum TurnOrigin {
    /// A prompt started immediately or was promoted from the queue.
    Prompt(PromptId),
    /// The provider began meaningful work without a new client prompt.
    Agent,
}
```

Tool calls, diffs, plans, and subagents are where UIs spend most of their
rendering effort, so they are typed rather than left in extensions:

```rust
pub struct ToolUpdate {
    pub id: ToolId,
    pub kind: ToolKind,
    /// Human title from the agent ("Read src/main.rs", "cargo test").
    pub title: String,
    pub status: ToolStatus,
    /// Decoded, kind-specific input: path, command, pattern, url, query.
    pub input: ToolInput,
    /// Output text, capped by Anyagent (default 16 KiB; configurable).
    pub output: Option<String>,
    /// File changes this call produced. Apps aggregate these per turn to
    /// show "files changed" and review diffs.
    pub diffs: Vec<FileDiff>,
    /// Files the call touched, for editors that follow the agent.
    pub locations: Vec<PathBuf>,
    /// Agent's own tool name and raw input, for unknown or MCP tools.
    pub raw: Option<RawTool>,
}

#[non_exhaustive]
pub enum ToolKind {
    Read, Edit, Delete, Move, Search, Execute, Fetch, Think,
    Mcp { server: String, tool: String },
    /// Spawned a subagent. Nested events carry this id in
    /// `TurnContext::parent_tool_id`.
    Subagent,
    Other,
}

#[non_exhaustive]
pub enum ToolStatus { Pending, Running, Completed, Failed, Cancelled }

pub struct FileDiff {
    pub path: PathBuf,
    /// `None` means a new file.
    pub old_text: Option<String>,
    pub new_text: String,
}

pub struct PlanEntry {
    pub text: String,
    pub status: PlanStatus, // Pending | InProgress | Completed
}
```

Mapping rules:

- Each agent has its own tool names (Claude `Bash`/`Edit`/`Task`, Codex
  `command_execution`/`file_change`, ACP `ToolKind`). Adapters decode them into
  `ToolKind` and `ToolInput`; the original name stays in `raw`.
- Subagents: the spawning call is `ToolKind::Subagent`; every nested event
  (text, reasoning, tools, even nested subagents) carries `parent_tool_id`.
  Apps that do not render nested transcripts ignore events with a
  `parent_tool_id`. Where the protocol needs opting in (claude-agent-acp
  `_meta["subagent-transcript"]`, Claude `--forward-subagent-text`), the
  adapter opts in.
- Plans and TODO lists (Claude `TodoWrite`, Codex `todo_list`, ACP `plan`)
  are `PlanUpdated`, not tool calls. The agent always sends the complete list.
- Anyagent declines ACP client-side `fs` and `terminal` capabilities by
  default; agents fall back to their own tools and everything still arrives as
  `ToolUpdate`. Hosting terminals for agents is fog, listed below.

Requests and input are the other types an app touches every turn, so they are
defined here (added in the 2026-08-23 review):

```rust
pub struct PermissionRequest {
    pub id: RequestId,
    /// The tool call awaiting approval, as the app already saw it in
    /// `ToolUpdated` (kind, title, input, diffs for a preview).
    pub tool: ToolUpdate,
    /// What the agent offers; the engine rejects an answer outside this list.
    /// Claude: Allow/Deny (+AllowAlways when `permission_suggestions` has a
    /// rule); ACP: its `options` kinds; Codex: approved / approved_for_session
    /// / denied.
    pub options: Vec<PermissionChoice>,
    /// Agent's own reason text, when it gives one.
    pub detail: Option<String>,
}

#[non_exhaustive]
pub enum PermissionChoice { AllowOnce, AllowAlways, DenyOnce, DenyAlways }

pub struct QuestionRequest {
    pub id: RequestId,
    pub questions: Vec<Question>,
}

pub struct Question {
    pub id: QuestionId,
    pub text: String,
    pub header: Option<String>,
    pub choices: Vec<Choice>,       // empty = free text
    pub multi_select: bool,
    pub allows_free_text: bool,
}

pub struct Choice { pub id: ChoiceId, pub label: String, pub description: Option<String> }

#[non_exhaustive]
pub enum Answer {
    Permission(PermissionChoice),
    /// One entry per question, in order.
    Question(Vec<QuestionAnswer>),
}

pub enum QuestionAnswer { Choices(Vec<ChoiceId>), Text(String) }

/// One prompt. `impl From<&str>` so `session.prompt("hi")` works.
pub struct Input { /* private */ }
impl Input {
    pub fn text(text: impl Into<String>) -> Self;
    /// Attach a file by path — any file type, on any agent.
    pub fn attach(self, path: impl Into<PathBuf>) -> Self;
}
```

Attachments are path-based, not bytes-based (decided 2026-08-23; Comet, T3,
and laptop-agent all stage files on disk and pass paths). The rule: every
attachment rides the prompt text as an absolute path ref, so any file type
works on any agent — the agent opens it with its own tools. Images under a
5 MiB cap (magic-byte sniff: png/jpeg/gif/webp) are *additionally* inlined
as image blocks when the wire takes them (`Capability::Images`; ACP image
content blocks, Claude base64 image blocks — both verified live 2026-08-23,
claude and hermes each answered from an inlined image and a path-ref'd text
file in one turn). No image processing: resizing and format handling are the
agent's job, same as drag-and-drop into the CLI. An unreadable file is a
`Diagnostic`, never a failed turn; an oversized or non-image file just rides
as its path ref.

```rust

#[non_exhaustive]
pub enum ToolInput {
    Path(PathBuf),
    Command { command: String, cwd: Option<PathBuf> },
    Pattern(String),
    Url(String),
    Query(String),
    Text(String),
    None,
}

pub struct RawTool { pub name: String, pub input: serde_json::Value }
```

Slash commands are plain text (`session.prompt("/review")`); the engine passes
them through and the agent decides.

`Question` covers every structured user prompt an agent can raise: Claude
`AskUserQuestion`, Codex `tool/requestUserInput`, ACP `elicitation/create`
form mode, and the question-shaped permission requests the ACP adapters send.
One normalized shape, answered through `Session::answer`.

Message and tool identifiers live only on variants where they are valid. Raw
provider data uses namespaced extension keys and is not a stable cross-provider
contract. `UserMessage` is not a replay of a prompt submitted by the client; it
preserves user-role content initiated within the provider, including nested
subagent steering. `MessageEnded` lets transcript reducers close one assistant
segment without treating it as the end of the turn.

Infrastructure failures use the stream's `Err`. An agent-level failed turn uses
`StopReason::Failed`. A stream error is terminal and closes the session.
`CompletionSource` tells callers whether completion came from the protocol or
from Anyagent's quiescence rule without exposing watchdog configuration.

## Interface invariants

- `discover` is best-effort and reports per-location diagnostics.
- `probe` is optional and never creates persistent application state.
- `open` returns a usable session or a typed error. Callers may prompt immediately.
- `open` creates a new provider session unless `SessionOptions` contains a
  `ResumeToken`. Failed resume never silently creates a new conversation.
- Commands from cloned `Session` handles are serialized by the session engine.
- Every turn emits one `TurnStarted` before turn content and exactly one `TurnEnded`.
- `prompt` allocates a stable `PromptId`. If queued input later starts a turn,
  `TurnOrigin::Prompt` carries the same ID. Agent-initiated work uses
  `TurnOrigin::Agent`.
- Events are ordered by `sequence` within a session. Session-level events
  (`SessionUpdated`, `Diagnostic`) may arrive between turns and before a
  turn's `TurnStarted`; only in-turn events are bracketed by
  `TurnStarted`/`TurnEnded`.
- Every assistant message that emits text or reasoning emits one `MessageEnded`.
  No later delta uses that `MessageId`. On wires with no end-of-message signal
  (ACP), the engine synthesizes the `MessageEnded` at turn end.
- A request can be answered once. Unknown, expired, or answered IDs return `InvalidRequest`.
- `cancel` affects only the active turn and is safe to repeat.
- `configure` accepts only advertised values supported by the current session;
  successful changes are confirmed by `SessionUpdated`.
- `rollback` is available only when advertised, requires an idle session, and
  emits `SessionUpdated` after the provider accepts the new context. An active
  turn returns `SessionBusy`; Anyagent never cancels work implicitly.
- No event is attached to a turn after its `TurnEnded`. A provider update that
  represents meaningful self-continued work starts a new agent-originated turn.
  Trailing noise is dropped and produces a throttled `Diagnostic`.
- A terminal stream error is followed by stream closure.
- `close`, or dropping every handle, cleans up the provider session and process.
- The queue is library-owned and FIFO. `cancel(false)` ends the active turn
  and lets the next queued prompt start; `cancel(true)` also empties the queue.
  A turn that ends with `Failed` does not drain the queue automatically; the
  next queued prompt still starts, and the application may `dequeue` first.
- Resuming through a protocol that replays history (ACP `session/load`) drops
  the replay by default. The application owns the transcript. No `Event` is
  emitted for replayed content.
- An auth failure reported by the agent ends the turn with `StopReason::Failed`
  and yields `AgentError::AuthRequired { login }` on the stream. The session is
  closed; the application reopens after the user logs in.
- `ContextUsage` and `PlanUsageUpdated` are only emitted when the matching
  capability is advertised. Absence of the event never means zero usage.
  Anyagent never parses usage out of slash-command text (`/usage`, `/status`);
  those remain ordinary commands whose output is agent text. When a wire
  exposes usage only that way, the capability is simply absent.
- `Events` buffers 256 normalized events. Anyagent may coalesce adjacent text or
  reasoning deltas for the same message before assigning sequence numbers. It
  never drops semantic events. A full buffer applies backpressure, and the
  application must keep draining: the engine is one task, so a full buffer
  parks it mid-send and it stops servicing commands until the consumer reads
  again. Making `cancel` and `answer` survive a stalled consumer needs a second
  delivery task; deferred until a real client needs it.

Public errors distinguish at least these cases:

```text
NotInstalled
AmbiguousInstallation
SpawnFailed
AuthRequired
UnsupportedVersion
HandshakeTimeout
UnsupportedFeature
InvalidConfiguration
InvalidRequest
ResumeFailed
SessionBusy
ProtocolFailed
ProcessExited
SessionClosed
```

## Internal design

### The session engine owns shared behavior

The session engine is the deep module. It owns:

- Turn state and exactly-once completion
- Prompt delivery, steering fallback, and queues
- Permission policy and request lifetimes
- Session, turn, message, tool, and request identifiers
- Event ordering and envelopes
- Completion watchdogs
- Cancellation and shutdown
- Translation from internal failures to public errors

Adapters do not reimplement those rules.

### Session engine internals (decided 2026-08-23, ticket 02)

One tokio task per session owns the adapter connection, the state machine,
and the queue. `Session` and `Events` are channel ends. No locks, no registry.

```text
run_session()                      select! { command, driver event, watchdog }
  handle_command()                 prompt | dequeue | answer | configure | rollback | cancel | close
  handle_driver_event()            DriverEvent -> Event envelope, advance TurnState
  tick_watchdog()                  inferred completion, non-deterministic profiles only

TurnState = Idle
          | Running { turn_id, origin, steer_in_flight, open_requests, running_tools }
          | Closing
Queue     = VecDeque<(PromptId, Input)>      uncapped, in memory
```

Rules:

| Situation | Rule |
|---|---|
| `prompt` while Idle | start; `TurnStarted` emitted at the decision (same instant as `Delivery::Started`); a wire rejection yields `TurnEnded{Failed}` |
| `prompt` while Running, `Steer` advertised, no steer in flight | steer; `Delivery::Steered` |
| `prompt` while Running otherwise | queue; `Delivery::Queued{position}` |
| steer rejected or failed | requeue at the **head** |
| queued prompts | promoted only at `TurnEnded`, never steered mid-turn |
| `cancel(false)` | end active turn; next queued prompt auto-starts |
| `cancel(true)` | empty queue, then cancel |
| `cancel` with an open request | reject the request on the wire, `RequestClosed`, then cancel |
| `configure` | send immediately; confirmed value arrives as `SessionUpdated` |
| agent-originated turn while prompts are queued | let it run; promote after |
| `close` or last handle dropped | `RequestClosed` for open requests and `TurnEnded{Cancelled}` for a running turn, then protocol Close → 5 s grace → SIGTERM → SIGKILL, in a spawned task |

Identifiers: the engine owns `SessionId`, `TurnId`, `PromptId`, `sequence`. The
wire owns `MessageId`, `ToolId`, `RequestId` (kept as opaque strings so
`answer` and `parent_tool_id` route without a mapping table); the engine mints
a `MessageId` only when the wire has none.

Turn end, so we never say "done" when it is not:

| Path | Turn end | Agent-originated (background wake) turn |
|---|---|---|
| Native Claude / Codex | deterministic (`result`, `turn/completed`); **no timer** | ends with the wire's own terminal frame |
| ACP agent with a reliable `session/prompt` response | deterministic (`stopReason`) | no prompt outstanding → 20 s quiet window |
| ACP agent with a known hang (Grok) | trust the profile's proprietary end notification if any; else 120 s quiet window | 20 s quiet window |

The quiet window resets on any stream activity and never fires while a tool is
running or a request is open. `SessionOptions::quiet_window` overrides it for
diagnostics. `Completed{source: Inferred}` is a distinct value; apps render it
as "idle", not the same checkmark as `Protocol`.

After `TurnEnded`, events are classified by **kind**, never by timing: content
(text, reasoning, a tool starting) opens a new `TurnOrigin::Agent` turn;
bookkeeping (a late tool status, usage, config, a late stop frame) is applied
with `turn: None` or dropped with a throttled `Diagnostic`. A real stop that
arrives after an inferred one is bookkeeping. `TurnEnded.background` lists the
tools still running at that moment, so UIs can show "1 task still running" and
link the later wake turn to it through `parent_tool_id`.

### The private adapter seam

```rust
#[async_trait]
pub(crate) trait Adapter: Send + Sync {
    async fn connect(
        &self,
        request: ConnectRequest,
    ) -> Result<DriverConnection, AgentError>;
}

pub(crate) struct DriverConnection {
    pub info: DriverInfo,
    pub commands: DriverCommandSink,
    pub events: DriverEventStream,
}
```

The adapter owns:

- Provider launch arguments
- Handshake and authentication exchange
- Protocol framing and correlation
- Encoding driver commands onto the provider wire
- Decoding provider frames into driver events
- Provider-specific compatibility fixes

The private driver vocabulary is `DriverCommand` (start turn, steer, answer,
configure, rollback, cancel, close) and `DriverEvent`, an enum of
`Event { kind: EventKind, parent_tool_id, extensions }`, `TurnEnded(StopReason)`,
`Steered(bool)`, `InfoChanged(DriverInfo)`, and `Exited { status, stderr }` —
the adapter's exit report, sent once right before it closes the event channel
so the engine's `ProcessExited` error carries the real exit status and stderr
tail instead of "unknown" (review fix 2026-08-23). Driver events reuse the public
`EventKind` so adapters cannot drift from the public vocabulary; the other
three variants are the only things adapters know that callers do not. Adapters
never send engine-owned kinds (`TurnStarted`, `TurnEnded`, `RequestClosed`,
`SessionUpdated`); the engine drops those with a `Diagnostic`. The engine adds
turn, sequence, and session envelope fields. `DriverInfo` carries two flags,
`deterministic_turn_end` and `deterministic_agent_turn_end`, which decide
whether the quiet-window watchdog is armed for prompted and agent-originated
turns (slice 1, 2026-08-23).

The runtime decides whether to steer, queue, retry, or close a normalized turn.
The adapter only reports what its protocol accepted and observed.

`process.rs` owns environment composition (PATH = executable dir → own PATH →
login-shell PATH, deduped), spawn, the stderr tail, and SIGTERM → grace →
SIGKILL escalation, as plain functions (`spawn(Spawn) -> Child`) rather than a
`ProcessHost` trait: its tests run against real short-lived processes, so the
trait seam had no consumer (slice 2, 2026-08-23). Real adapters call `spawn`.
The mock adapter uses an in-process connection and does not need a child.

The adapter registry stores reusable `Arc<dyn Adapter>` values. `connect` borrows
the adapter, so opening one session does not consume the registered adapter.

### The agent catalog

Per-agent facts are one `AgentProfile` per supported agent, kept as data:

```rust
pub(crate) struct AgentProfile {
    id: &'static str,
    name: &'static str,
    /// Binary the user installs (`claude`, `codex`, `hermes`).
    cli: &'static str,
    /// Extra install locations after PATH and version-manager bins.
    extra_paths: fn() -> Vec<PathBuf>,
    /// Env var that overrides executable resolution (tests, custom installs).
    executable_env: &'static str,
    /// Env var that relocates the agent's config home, for `config_home`.
    config_home_env: Option<&'static str>,
    /// Which adapter drives it and how to put the CLI in protocol mode.
    connection: Connection, // Acp { args, startup_timeout, quirks } | Native(kind)
    /// Files or keychain items whose presence means "logged in" without a
    /// network call (Claude: `~/.claude/.credentials.json` or the macOS
    /// Keychain item `Claude Code-credentials`; Codex: `$CODEX_HOME/auth.json`).
    auth_markers: &'static [AuthMarker],
    /// Shown in `MissingAgent` when the agent is not found.
    install_hint: &'static str,
}
```

`connection` is `Acp { args, startup_timeout, quirks }` for ACP-native agents
and `Native(Claude | Codex)` for the two agents that get their own adapter.
One path per agent: no ACP bridge profiles for Claude or Codex (decided
2026-08-23, ticket 03).

Quirks that have already shown up in the field and belong in the ACP profile,
not in the driver: startup timeout (opencode loads plugins for minutes),
stdout lines to filter before JSON-RPC parsing, a proprietary turn-end
notification to trust (Grok), and whether `_session/steering` is expected.
Comet's `AcpAgentSpec` and ACP Kit's `AgentProfile` are the references.

### Why Claude and Codex are native only

Claude Code and Codex only speak ACP through npm adapter packages
(`@agentclientprotocol/claude-agent-acp`, `codex-acp`): a second program the
user must install, which drops plan usage, fork, and rollback, and which Comet
found holds turns open for background work the CLIs settle eagerly. Comet, T3,
and vibe-kanban all drive the native wires. Anyagent does the same and ships
no bridge profiles: the ACP adapter comes first (S0 is done, laptop-agent is
ACP-only, one adapter unlocks many agents), native Claude second in the same
phase as the harder adapter that proves the seam, native Codex after.

### ACP implementation

Decided by the S0 spike (2026-08-23, `spikes/s0-acp/`, SDK 2.0.0, fixture agent
that emits text, thought, tool call with diff and vendor fields, plan, commands,
usage with `_meta`, an unknown update kind, an extension notification, a
permission request, late trailing noise, EOF mid-turn, cancel, and a 200k-chunk
flood while the consumer stalls 5 s):

| Check | `ActiveSession` | `Client.builder()` typed handlers | Own JSON-RPC reader |
|---|---|---|---|
| Raw frames reachable | yes (`UntypedMessage`) | only what typed handlers do not claim | yes |
| Unknown `sessionUpdate` kind | typed matcher errors; raw still available | **silently dropped** | kept |
| `session/new` `configOptions` | **dropped** (2.0.0 bug, fixed on main) | kept | kept |
| Prompt response `_meta` | dropped (only `StopReason`) | kept | kept |
| Prompt input | text only | full `ContentBlock`s | full |
| RSS after 200k chunks with stalled consumer | 280 MiB | 320 MiB (our bounded channel cannot push back) | 3 MiB (pipe backpressure) |
| EOF mid-turn | `connect_with` aborts, our future is cancelled | prompt resolves with error | clean EOF |
| Cancel, ordering, permissions, late noise | ok | ok | ok |
| Code / extra crates | 62 lines / ~190 crates | 61 / ~190 | 75 / 0 |

Decision: `adapter/acp.rs` owns a small JSON-RPC reader over `ProcessHost`
stdio (bounded channel, pending-id map, raw `serde_json::Value` frames — the
shape laptop-agent `conn.rs` and Comet `jsonrpc.rs` already use). Typed parsing
uses `agent-client-protocol-schema` (`default-features = false`) per frame:
parse known shapes, and on failure keep the raw frame, emit a `Diagnostic`, and
continue. The SDK runtime is not used: it adds unbounded buffering, strict typed
matching that hides updates, a schema pin that lags the spec, ~190 crates, and
forces session logic inside its `connect_with` future. Revisit only when v2 is
stable and shipped by agents; the wire is small enough to rewrite then.

Spec note from the spike: ACP `terminal` auth methods carry `args` and `env`
that are appended to the *agent's own* invocation, and agents advertise them only
when the client sends the terminal-auth capability in `initialize`. `LoginMethod`
and the ACP `login` path follow that.

Field notes (2026-08-23, gemini-cli 0.55.1): real agents ship *untyped*
`authMethods` (`{id, name, description}`, no `type`) — the schema crate skips
them, so they cannot become `LoginMethod`s until agent-driven `authenticate`
lands with `Runtime::login` (P2). And the auth error code (-32000) is used for
non-auth failures too (Google's Gemini Code Assist shutdown notice). Rule:
`AuthRequired` only when at least one runnable login method exists; otherwise
the agent's own error message passes through as `ProtocolFailed`. Google has
sunset personal-OAuth Gemini Code Assist for this client path; per Sid
(2026-08-23) the gemini profile was replaced by an **antigravity** profile
(`agy` CLI, found under `~/.local/bin`, state in `~/.gemini/antigravity-cli/`,
auth markers `jetski-standalone-oauth-token` / `oauth_creds.json` sitting
directly under `~/.gemini/`, not in the state subdirectory). `agy`
1.1.19 speaks its own stream-json event dialect (`init` / `step_update` /
`result`), not ACP — validated with a live turn; the native adapter is a
future slice (research in `.scratch/anyagent/issues/05-antigravity-wire.md`).
Until then discovery lists it and `open` returns the typed
"no adapter implemented" error. The ACP half of the P0 exit gate closed the
same day through **opencode** (1.18.21, `opencode acp`, added to the catalog:
data home `.local/share/opencode`, marker `auth.json`, login `opencode auth
login`): probed live — initialize 0.4 s, session/new 0.6 s, full turn 2.8 s,
so the "minutes-long startup" quirk did not reproduce and no profile timeout
knob shipped. `examples/chat.rs` completed a text turn and a write-tool turn
through it. Note opencode also ships untyped `authMethods`, so its login
methods stay empty until agent-driven `authenticate` (P2). **The P0 exit gate
is met (2026-08-23): chat through installed claude and installed opencode,
both adapters through the unchanged engine.** A follow-up live pass the same
day verified resume (codeword survives close → reopen with the token, on
both claude `--resume` and opencode `session/load`), mid-turn steering
(folded into the running turn, `Delivery::Steered`), and cancel mid-stream
(`StopReason::Cancelled`) against the real CLIs — and caught the uuid-dedupe
resume bug recorded in the Claude wire notes.

**Hermes Agent** (0.20.5) landed in the catalog 2026-08-23 once Sid got it
installed (`hermes acp`, config home `.hermes`, marker `auth.json`, login
`hermes login`, binary in `~/.local/bin`). It is the cleanest ACP citizen so
far: `loadSession`, image prompts, fork/list/resume session capabilities, and
properly *typed* `authMethods` (unlike opencode's). A second live pass the
same day closed the remaining never-run-live paths, all with zero code
changes: hermes full turn and resume (codeword survives `session/load`),
cancel mid-stream over ACP (opencode, `StopReason::Cancelled`), the queue
path (a prompt mid-turn on an ACP agent returns `Queued {position: 0}` and
runs after the turn), and a real `session/request_permission` round-trip
under `PermissionMode::Ask` — hermes asks "Approve edit" with
allow-once/reject-once options, `AllowOnce` crosses the wire, the write
lands. opencode's default config auto-allows writes internally, so it never
sends permission requests; that is agent behavior, not a gap on our side.

There is no shared `jsonrpc.rs` in P0. Native Codex may get a private wire helper
inside its adapter in P3. Extract a shared helper only if a second implementation
actually uses it.

### Native Claude wire (validated 2026-08-23, ticket 04)

Recorded against `claude` 2.1.241 with `@anthropic-ai/claude-agent-sdk`
0.3.241 `sdk.d.ts` as the type reference. Recordings live in
`tests/fixtures/claude/` (README there). The adapter is built against these
facts; the conformance tests replay the fixtures.

Launch: `claude -p --output-format stream-json --input-format stream-json
--verbose --include-partial-messages --permission-prompt-tool stdio
--replay-user-messages [--model M] [--permission-mode P] [--effort E]
[--resume ID [--fork-session] [--resume-session-at=UUID]]`, cwd = workspace.
First write a `control_request {subtype: initialize}`; its response carries
`account`, `models` (with effort levels), `commands`, `agents`. `system/init`
arrives only with the first turn, carrying `session_id`, `model`,
`permissionMode`, `tools`, and `capabilities`
(`interrupt_receipt_v1`, `interrupt_cancel_queued_v1`, `msg_lifecycle_v1`).
Because `system/init` is that late, the adapter mints the session id itself
and passes `--session-id` (probed 2026-08-24: the CLI adopts it), so the
resume token exists at open instead of after the first turn.

| Anyagent | Claude wire |
|---|---|
| `prompt` (idle) | write `{type: user, uuid, message: {role: user, content}}`; `command_lifecycle` reports `queued → started → completed / cancelled / discarded / refused` for that uuid |
| `prompt` while running | not sent — the CLI cannot steer. A mid-turn user message is parked (`command_lifecycle queued`) and runs as its own turn after the current `result` (probed 2026-08-24). The adapter does not advertise `Steer`; the engine queues and delivers after the turn, keeping prompt ids aligned |
| `TurnStarted { origin: Prompt }` | `command_lifecycle started` for our uuid |
| `TurnStarted { origin: Agent }` | activity (`status: requesting` / `stream_event` / `assistant`) while idle with no prompt in flight; happens after `task_notification` (background shell or subagent finished) |
| `TextDelta` / `ReasoningDelta` | `stream_event` `content_block_delta` with `text_delta` / `thinking_delta`; `assistant` frames carry the finished block |
| `UserMessage` | replayed `user` frames (`--replay-user-messages`) |
| `ToolUpdated` | `assistant` `tool_use` block → running; `user` `tool_result` block (matched by `tool_use_id`) → completed/failed; `tool_use_result` carries typed output (Write/Edit: `structuredPatch` → `FileDiff`; Bash: stdout/stderr; Agent: subagent report). Subagent frames carry `parent_tool_use_id` + `subagent_type` |
| `RequestOpened` Permission | `control_request {subtype: can_use_tool, tool_name, input, tool_use_id, permission_suggestions, description}`; answer `control_response {behavior: allow, updatedInput[, updatedPermissions]}` or `{behavior: deny, message[, interrupt]}` |
| `RequestOpened` Question | same frame with `tool_name: AskUserQuestion`, `requires_user_interaction: true`, `input.questions[]`; answer with `updatedInput = {questions, answers: {question: choice}}` |
| `ContextUsage` | each `assistant.message.usage` (input + cache_read + cache_creation) against `result.modelUsage.<model>.contextWindow`; `get_context_usage` control request on demand (`totalTokens`, `maxTokens`) |
| `ContextCompacted` | `system/status compacting` → `status` with `compact_result`; `/compact` may be sent as a user message |
| `PlanUsageUpdated` / `plan_usage()` | `get_usage` control request → `rate_limits.{five_hour, seven_day, ...}.{utilization, resets_at}` and `limits[]` (documented experimental); `rate_limit_event` only pushes `status` + `resetsAt` + `rateLimitType` |
| `TurnEnded` | exactly one `result` per turn: `subtype success` → Completed(Protocol); `error_during_execution` + `terminal_reason aborted_streaming` → Cancelled; other `error_*` → Failed; `background` falls out of tool tracking: a `tool_result` carrying `backgroundTaskId` keeps its tool Running past the turn, and `task_notification` completes it (slice 4 — simpler than mirroring `background_tasks_changed`) |
| `cancel(clear_queue)` | `control_request {subtype: interrupt, cancel_queued}`; response lists `still_queued` / `cancelled`; a running Bash is moved to background, not killed. If the interrupt lands before the turn's `command_lifecycle started`, the prompt is cancelled out of the CLI's queue and no `result` comes — the receipt naming the turn's uuid is the turn end (probed 2026-08-24) |
| `configure` model / mode / effort | `set_model {model}`, `set_permission_mode {mode}`, `/effort <level>` as a user message; `list_models` for the catalog |
| `open` resume / fork / fork at | `--resume ID` (same id, history not replayed on the wire); `--fork-session` (new id); `--resume-session-at=UUID` (history cut after that message) |
| file rewind | `rewind_files {user_message_id, dry_run}` with env `CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING=true`; files only, no conversation rollback |
| `probe` | `claude --version` (0.06 s), `claude auth status --json` (0.2 s, offline; `loggedIn`, `authMethod`, `email`, `subscriptionType`; exit 1 when logged out), then `initialize` for models/commands |
| `login` | `claude auth login [--console]` over pipes: stdout `If the browser didn't open, visit: <url>` → `OpenUrl`; stdin takes `code#state`; exit 0 = success |
| `Diagnostic` | `system/api_retry {attempt, max_retries, error_status, error}`, `auth_status`, unknown frames |
| `close` | close stdin; the process exits on its own |

Facts that shape the engine:

- A host's `ANTHROPIC_BASE_URL` / `ANTHROPIC_API_KEY` in the inherited
  environment redirect the child (observed: 401 retried ten times under
  Claude Desktop). Not scrubbed — see below.
- `rollback` on Claude = kill + respawn with `--resume <id> --fork-session
  --resume-session-at=<last kept message uuid>`; new session id → new resume
  token via `SessionUpdated`. Decided with Sid 2026-08-23: do what the user
  would do by hand, say so in the doc comment.
- Inherited environment is passed through unchanged (a user may have set
  `ANTHROPIC_BASE_URL` on purpose); `AuthStatus`/`AgentDetails` surface
  `apiKeySource` so the app can show which credential is in use.
- Unknown control subtypes get `control_response {subtype: error}`; the
  adapter must treat that as `UnsupportedFeature`, not a crash.
- `session_state_changed` (`idle | running | requires_action`) exists in the
  types but was not observed; do not depend on it.
- Slash commands (`/compact`, `/effort`, `/model`) work over stdin and produce
  a zero-turn `result`.
- The CLI dedupes user messages by uuid **across resumes of the same
  conversation**: a resumed session that reuses an old uuid gets its prompt
  silently swallowed (no lifecycle, no result). Found live 2026-08-23; the
  adapter clock-seeds its uuids.
- The transcript is on disk at `~/.claude/projects/<cwd-slug>/<session_id>.jsonl`;
  resume does not replay it. History rendering stays application-owned.

### Dependency strategy

| Dependency | Treatment |
|---|---|
| Session state, policy, IDs, and normalization | In-process implementation tested through the public interface |
| Filesystem, PATH, process, clock, and stdio | Private local test stand-ins |
| Installed agent CLIs and provider behavior | Private adapters, recorded fixtures, and controlled smoke tests |
| Future daemon | Versioned local protocol when a real multi-client need exists |

## Code structure

Start flat. Add directories only when a file contains independent concepts.

```text
anyagent/
  Cargo.toml
  PLAN.md
  src/
    lib.rs                 public re-exports only
    runtime.rs             discover, probe, open orchestration
    agent.rs               installation, details, options, capabilities
    session.rs             session engine, Session, Events, Delivery
    event.rs               events, requests, tools, usage, stop reasons
    error.rs               public AgentError and private error mapping
    catalog.rs             AgentProfile table: one entry per supported agent
    discovery.rs           executable resolution, version, offline auth markers
    process.rs             environment, spawn, stderr, and shutdown
    adapter/
      mod.rs               private Adapter seam and driver vocabulary
      acp.rs               ACP adapter: own JSON-RPC reader + schema types (S0 decision)
      mock.rs              in-process scripted adapter
      conformance.rs       private adapter and session contract tests
  examples/
    chat.rs                terminal chat with an installed agent
  tests/
    public_interface.rs    builders, serialization, and public invariants
```

Do not create Codex, daemon, bindings, installer, or shared JSON-RPC modules
until their phase begins.

Data types such as events, tools, capabilities, models, and discovery results
derive `Serialize` and `Deserialize`. Runtime handles do not. Public enums use
`#[non_exhaustive]`. Config structs use constructors or builders.

## Testing

Three layers catch different failures:

1. The mock adapter tests the whole public interface without a subprocess.
   It is also public behind the `mock` cargo feature (`anyagent::mock::Script`,
   `Runtime::with_mock(script)`): consumers test their own UI and routing
   against a scripted agent with the real engine underneath — Comet's 521-line
   `mock.rs` and laptop-agent's hand-faked events disappear.
2. Recorded protocol fixtures test each adapter's command and event translation.
3. Scheduled real-agent smoke tests catch changed flags, login behavior, and handshakes.

The private conformance suite can inject adapters without exposing the adapter
seam publicly. Tests assert on public outcomes, not adapter state. Each adapter
runs common session tests plus capability-specific tests it claims to support.

Every confirmed provider quirk gets a fixture. Real-agent tests run only where
credentials and licenses permit them.

## Build order

| Phase | Work | Exit gate |
|---|---|---|
| S0 | ACP SDK viability spike with a fixture child owned by `ProcessHost` — **done 2026-08-23** | Own wire + schema types chosen; results in "ACP implementation"; fixture seeds `tests/fixtures/acp/` |
| P0 | Public interface, session engine, mock adapter, agent catalog, discovery (offline auth markers, missing agents with install hints), process lifecycle (macOS + Linux), the ACP adapter, then the native Claude adapter through the same engine; prompt streaming, typed tools and diffs, permissions, cancellation, cleanup | Mock conformance passes; `examples/chat.rs` completes a turn through an installed ACP agent **and** through `claude`; both adapters pass the same contract |
| P1 | Images, MCP transport-aware forwarding, steering, dequeue, permission and mode configuration, `AuthRequired` mid-session, laptop-agent integration; Windows resolution and shutdown verified | laptop-agent golden demo works after deleting its ACP implementation |
| P2 | Models, well-known config ids, commands, resume (replay dropped), fork and rollback (Claude), config home isolation, message boundaries, plans, tool output deltas, context usage, plan usage for Claude, subagent context, diagnostics, wire recording, `Runtime::login`, three ACP agent profiles | Comet replaces its shared ACP layer and can route Claude through anyagent |
| P3 | Native Codex adapter with plan usage, fork, rollback, login; ACP/native selection through the same session engine | Three adapters pass the same contract; publish v0.1 |
| P4 | Versioned JSONL sidecar and TypeScript and Swift bindings | Rust, Electron, and Swift clients can control sessions; a T3-style TypeScript bridge can replace supported provider-driver paths |
| Later | Explicit installer, persistent daemon, native Cursor, hosting ACP terminals/fs for agents, elicitation URL mode, skills and hooks listing, HTTP plan-usage polling, budgets, and policy packages | Added only after a consumer needs them |

An ACP-only release may ship as `0.0.x`. The adapter seam is not proven until
ACP and native Codex cross it without changing public types.

## First implementation step

S0 is done (see "ACP implementation"). **Slice 1 is done (2026-08-23):** the
crate exists with the public data types, the session engine, the private
driver seam, the scripted mock adapter (test-only for now; the public `mock`
feature comes with P2), and ten conformance tests in
`src/adapter/conformance.rs` covering the contract below plus queue promotion,
steer accept/reject, inferred completion, agent-originated continuation,
trailing noise, backpressure, cancel with and without queue clear, and request
and dequeue validation. **Slice 2 is done (2026-08-23):** `catalog.rs` (four
grounded profiles: claude and codex native, gemini and qwen ACP; the wider ACP
roster ships with verified quirks in P2), `discovery.rs` (env override → PATH →
login-shell PATH → version-manager bins → known locations, offline auth
markers incl. the macOS keychain, missing agents with searched dirs and
install hints), and `process.rs` (spawn, stderr tail, SIGTERM → SIGKILL,
cached login-shell PATH, `Runtime::prewarm`). **Slice 3 is done (2026-08-23):**
`adapter/acp.rs` (own JSON-RPC reader per S0; handshake with `AuthRequired`
mapping; cumulative tool snapshots from partial wire updates; unknown update
kinds kept raw in `extensions`; cancel-before-unblocking-requests ordering;
stderr surfaced on agent death), `AgentInstallation::acp` (its args ride on the
installation), ACP adapters auto-registered from the catalog,
`PermissionMode::AutoApprove` enforced in the engine (uniform across adapters,
requests never surface), `examples/chat.rs`, and seven fixture integration
tests in `tests/acp.rs` (full-turn mapping, handshake info, cancel, steering,
agent death, auth-required, 500-chunk flood). Field findings recorded in "ACP
implementation". **Slice 4 is done (2026-08-23):** `adapter/claude.rs` (own
stream-json wire per ticket 04: `initialize` + `get_binary_version` handshake,
deltas from `stream_event`, tools from `tool_use`/`tool_result` with typed
diffs, `TodoWrite` → `PlanUpdated`, `can_use_tool` → permission or Question,
`parent_tool_use_id` → subagent context, `ContextUsage` from message usage +
`modelUsage.contextWindow`, deterministic turn end on `result` for prompted
and agent-originated turns alike), registered from the catalog so
`open("claude")` works like any other agent — zero public-surface change and
zero engine change. Two wire subtleties handled: a steer racing turn end (the
CLI queues it as its own turn; the adapter holds `TurnEnded` until the steer's
`command_lifecycle` resolves, and `interrupt{cancel_queued}` clears a parked
one on cancel, so a prompt can never run twice) and background tools (a
`backgroundTaskId` result keeps the tool Running so `TurnEnded.background`
lists it; `task_notification` completes it as bookkeeping). Scripted
`tests/fixtures/claude/fixture.mjs` plus nine integration tests in
`tests/claude.rs` through the public API (`AgentInstallation::at("claude",
wrapper)`). Live-verified against installed claude 2.1.241: text turn and a
permissioned Write turn through `examples/chat.rs`. The ticket-04 env hijack
reproduced live (this repo's own agent harness exports `ANTHROPIC_BASE_URL`);
pass-through behavior kept as decided. **The P0 exit gate is met**: chat
completed real turns through installed claude and installed opencode (see the
ACP field notes for the opencode verification). Next: P1.

**P1 slice 1, attachments, is done (2026-08-23):** `Input::attach(path)`
replaced the bytes-based `Image` (rule and evidence in the interface section
above). Implementation is one shared loader (`adapter/attach.rs`:
absolutize, read, magic-byte sniff, 5 MiB cap, hand-rolled encode-only
base64 — no new dependency) plus a content-block builder in each adapter;
the ACP adapter gates inlining on the agent's advertised image capability,
Claude always inlines and now advertises `Capability::Images`. Fixtures echo
the received wire shape; five new tests (three unit, one per wire) cover
inline + path ref + unreadable→`Diagnostic`. Live-verified on both wires:
claude and hermes each answered "Red PERSIMMON" from an inlined 64×64 png
and a path-ref'd text file in a single turn.

**P1 slice 3, mode configuration, is done (2026-08-23):** `configure` now
works. The engine validates every selection against the advertised options
(`InvalidConfiguration` for unknown ids, non-offered choices, wrong value
types, and creation-only options; `UnsupportedFeature` for model switches
until P2) before anything touches a wire. Adapters translate the well-known
`mode` id — Claude a `set_permission_mode` control request (probed live:
success echoes `{"mode"}`), ACP `session/set_mode`; other advertised ACP
options go to `session/set_config_option`. The Claude adapter now advertises
the CLI's four fixed permission modes as a `mode` `ConfigOption` (current
from `initialize.current_permission_mode`). Confirmation is asynchronous by
design: `Ok` from `configure` means validated-and-sent; the applied change
arrives as `SessionUpdated` (from the wire receipt, or the agent's own
`current_mode_update`, deduplicated by a changed-check so redundant echoes
emit nothing). A rejected wire configure surfaces as a `Diagnostic`.

**Live edge pass (2026-08-23, all three P1 features, three harnesses):**
plan mode set through `configure` really blocks writes on real claude (its
`ExitPlanMode` permission was denied — first live exercise of the deny path
— and the file never appeared); opencode confirmed `session/set_mode` via
`current_mode_update`; invalid mode values and unknown options are typed
engine rejections that never reach the wire; a 6.3 MB png rides as path ref
only and claude still answered its exact dimensions through its Read tool; a
missing attachment is one `Diagnostic` and the turn completes (hermes); an
http MCP declaration on hermes (stdio-only) fails `open` typed; a dead http
MCP URL on opencode and a nonexistent stdio MCP binary on claude both leave
`open` and turns working; a queued prompt carrying an attachment still
inlines when promoted (opencode answered "Red"). Two hermes/opencode
model-backend flakes were observed and reproduced as flakes, not code
paths.

**P1 slice 4, dequeue live + `AuthRequired` mid-session, is done (2026-08-23):**
`DriverEvent::AuthLost { login }` is the seam: the engine ends the turn
`Failed`, puts `Err(AuthRequired { login })` on the stream, and closes the
session (the application reopens after login). ACP maps a `session/prompt`
error with the auth code (-32000) using the login methods captured from
`initialize`; other prompt errors now end the turn `Failed` with the wire's
message and code instead of losing them, and the session survives. Claude has
a typed marker probed live with `CLAUDE_CONFIG_DIR` pointed at an empty dir:
the synthetic assistant frame carries `error: "authentication_failed"` (text
"Not logged in · Please run /login"); its login methods come from the catalog
profile (`claude auth login` terminal command plus the `ANTHROPIC_API_KEY`
env var). The same probe exposed a stop-reason bug: the api-error result
frame says `subtype: "success"` with `is_error: true`, which previously
mapped to `Completed` — `is_error` now wins. A late wire turn-end during
`Closing` is expected and no longer a diagnostic. When no runnable login
method exists the adapters degrade to a `Failed` turn (session stays open),
matching the open-time rule. Live: auth loss verified on real claude (no
credentials — `Failed` turn, `AuthRequired` with both methods, stream close,
`SessionClosed` on re-prompt); dequeue verified end-to-end on opencode
(dequeued from position 1 mid-queue while position 0 still ran) and claude
(steer slot occupied, queued prompt dequeued mid-turn); in both, the repeat
dequeue is `InvalidRequest`, the dequeued marker never appears in any
output, and a later prompt completes normally.

```rust
#[tokio::test]
async fn prompt_request_answer_and_completion_share_one_contract() {
    let runtime = Runtime::with_test_adapter(MockAdapter::permission_flow());
    let report = runtime.discover().await;
    let agent = report.require("mock").unwrap();

    let project = tempfile::tempdir().unwrap();
    let (session, mut events) = runtime
        .open(agent, SessionOptions::in_dir(project.path()))
        .await
        .unwrap();
    let delivery = session.prompt("Fix the test").await.unwrap();
    assert!(matches!(delivery.kind, DeliveryKind::Started { .. }));

    let mut saw_correlated_start = false;
    while let Some(event) = events.next().await {
        match event.unwrap().kind {
            EventKind::TurnStarted {
                origin: TurnOrigin::Prompt(prompt_id),
            } => {
                assert_eq!(prompt_id, delivery.prompt_id);
                saw_correlated_start = true;
            }
            EventKind::RequestOpened(request) => {
                session
                    .answer(request.id(), Answer::Permission(PermissionChoice::AllowOnce))
                    .await
                    .unwrap();
            }
            EventKind::TurnEnded {
                stop: StopReason::Completed { .. },
            } => {
                assert!(saw_correlated_start);
                break;
            }
            _ => {}
        }
    }

    session.close().await.unwrap();
}
```

`Runtime::with_test_adapter` is crate-private. The test lives inside the crate.

The first production slice creates only the public data types, session engine,
private driver seam, mock adapter, and contract test. Add conformance cases for
queued prompt promotion, protocol and inferred completion, late trailing noise,
agent-originated continuation, and event-buffer backpressure. The next slice
adds discovery and process handling. The third implements ACP using the S0
decision and moves `spikes/s0-acp/fixture.mjs` under `tests/fixtures/acp/`.

## Quality bar

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-features`
- Public documentation and runnable examples checked in CI
- Pinned minimum supported Rust version
- `cargo deny check` before publishing
- MIT OR Apache-2.0 dual license before accepting contributions
- Short decision records only for compatibility or dependency choices that are hard to reverse

## Scope limits

Anyagent does not own conversation storage, worktrees, Git, UI state, remote
sync, voice, screen context, or task orchestration. It forwards MCP configuration
but does not become an MCP registry. It reports installable agents but never
installs without an explicit application action and user confirmation.

Anyagent does not implement OAuth, copy or swap credential files, or refresh
tokens. `Runtime::login` only drives the agent's own login flow and relays
what the user must do. Comet's account switcher stays in Comet, and
`config_home` is the supported way to keep logins apart.

Anyagent does not own a durable prompt queue. Its queue lives in memory for the
life of one `Session`; applications that need offline or cross-device queues
(Comet's command plane, T3's orchestration) keep their own and hand prompts to
`Session::prompt` when the session is live.

The library does not become a conversation database or a provider-session
browser. A future daemon may retain provider session metadata, but applications
continue to own their product data. Native session listing or forking is added
only when a real consumer requires it and enough adapters can support it.

Automatic process restart is outside v0.1. A process exit terminates `Events`;
the application may explicitly reopen with the last `ResumeToken`. Silent
restart or silent fallback to a fresh conversation could lose work or change
context without the caller knowing.

## Risks

- Agent protocols and launch flags change. Fixtures and scheduled smoke tests reduce repeated breakage.
- TypeScript runtimes and official SDKs may add overlapping behavior. Anyagent must prove value by deleting consumer code and supporting ACP plus native protocols.
- Discovery can become platform-specific. Keep platform code private in `discovery.rs`/`process.rs`; macOS and Linux in P0, Windows (`.cmd` shims, `PATHEXT`, kill-only shutdown) verified on a real machine in P1.
- The working name collides with existing GitHub projects. Settle the package name before publishing.
- A daemon can distract from the library. Build it only after the crate works in laptop-agent and Comet.
- Plan usage depends on undocumented endpoints and wire events that providers can change. Keep it behind a capability and degrade to `UnsupportedFeature`, never to wrong numbers.
- Native Claude and Codex wires are semi-documented and version-pinned in every consumer (Comet pins CLI versions; T3 pins SDK versions). Each native adapter records the version it was validated against and smoke-tests it on a schedule.

## Review 2026-08-23 (vision + codebase-design pass)

Walked Comet (`crates/harness` + `engine/sessions.rs`), laptop-agent
(`core/src/harness`), and vibe-kanban (`crates/executors`) against the public
interface. All three harness layers map 1:1 onto `Runtime` + `Session` +
`Events`; what they keep (transcript store, worktrees, titles, journals) is
correctly application-owned. Gaps fixed in this revision: `Session::info()`, the public `mock` feature,
the `plan_usage` mechanism (short-lived process on demand, pushed after each
turn on live sessions), and the per-turn types that were referenced but never
defined (`PermissionRequest`, `Question`, `Answer`, `Input`, `Image`,
`ToolInput`, `RawTool`). Added `Runtime::plan_usage_all()` so a usage page
is one call (Sid: users must easily see limits per harness).
Design check: `Runtime` and `Session`/`Events` are deep (Comet's 900-line
`drive_run` reappears in every consumer without them); the driver and
`ProcessHost` seams each have two real adapters in P0; the interface carries
its invariants. Rule to keep `EventKind` honest: a new variant needs two wires
that emit it, otherwise it rides in `extensions`.

## Review 2026-08-23 (consumer pass, after slice 3)

Mapped all four consumers' harness surfaces item by item onto the public
interface: Comet (`Harness` trait, `RunControls`, `RunRequest`, `AgentEvent`,
process/PATH helpers), laptop-agent (`HarnessCommand`/`HarnessEvent`),
vibe-kanban (executor trait, `AvailabilityInfo`, log normalizers), and T3 Code
(`ProviderAdapterShape`, provider snapshot, runtime events). Everything each
app should delete has a named replacement; what they keep (docs, worktrees,
titles, voice, orchestration, transcript stores) is correctly app-owned. The
core flow (open, prompt, drain, store token, resume) is 10 lines. Three fixes
recorded: `Runtime::login` moved back into `impl Runtime` (was nested inside
`AgentPlanUsage` by a formatting slip), `Model` gained per-model `options` so
pickers can render each model's effort levels (Comet and T3 both need this;
code lands with P2 models), and `Image` synced to `Vec<u8>` with `from_path`
deferred to the P1 images slice. Design check: deletion test passes for all
four consumers; the adapter seam has two real crossings (mock, ACP) with
native Claude next; every `EventKind` variant keeps two emitting wires;
Claude-only behavior (hooks, model rerouting) stays in `extensions`.

## Review 2026-08-23 (external P0 review)

An outside review of P0 landed four fixes: `rust-version` bumped to 1.88 (the
ACP schema crate requires it); ACP resume now returns `ResumeFailed` when the
agent lacks `loadSession` instead of silently opening a fresh session (our own
no-silent-fallback rule); `DriverEvent::Exited` carries the real exit status
and stderr tail into `ProcessExited` (the adapter waits for the process and
its stderr reader before reporting, closing the EOF race); and the engine now
stores each open request's shape, so `Session::answer` rejects a mismatched
answer type or an unoffered permission choice with `InvalidRequest`, leaving
the request open — making the documented "answers outside this list are
rejected" promise true. Rejected from the review: advertising `auth.terminal`
before the P2 terminal-login driver exists (would advertise a capability we
cannot drive), replacing the best-effort pre-promote drain with an adapter
signal (the agent can always continue after any signal; the race is
inherent), and "fixing" the Antigravity auth-marker paths (verified on disk:
the markers live directly under `~/.gemini/`; `~/.gemini/antigravity-cli/`
holds only CLI state).

## Review 2026-08-24 (v0 live test pass, three real harnesses)

A full v0 live matrix (V0_LIVE_TESTS.md, run by a separate agent) surfaced
nine findings. Fixed in anyagent:

1. **`MessageEnded` was never emitted on the ACP wire.** ACP has no
   end-of-message signal, so the engine now tracks streamed message ids per
   turn and synthesizes `MessageEnded` for any still open at turn end — one
   engine fix for every ACP agent.
2. **Claude cannot steer; the adapter claimed it could.** Wire probe: a
   mid-turn user message is parked (`command_lifecycle queued`) and runs as
   its own turn after the current `result`. The old adapter read that late
   `started` as steer-accepted, which shifted every later turn's prompt id
   and broke `cancel` after a "steer". The adapter no longer advertises
   `Steer`; the engine queues, ids stay aligned, cancel works.
3. **Claude's resume token was `None` until the first turn** (`system/init`
   arrives that late). The adapter now mints the session id and passes
   `--session-id`, so the token exists at open.
4. **`live: false` options were unreachable** (opencode's `model`).
   `SessionOptions::configure(id, value)` applies advertised options at open;
   refusals fail `open` with `InvalidConfiguration`.

Attributed to agents, recorded as quirks, no code change:

- **hermes** sends `tool_call` with no status and never a
  `tool_call_update`: tools appear stuck pending even after completing. A UI
  on hermes cannot show tool completion; the turn itself ends correctly.
- **hermes** may re-ask after a denied permission and complete the write
  anyway (3/5 runs). Anyagent delivers the deny correctly; callers must not
  treat a deny as a guarantee on hermes.
- **opencode** may write outside the session `cwd` (we send `cwd` correctly;
  hermes honors the same field). Callers sandboxing by cwd should not trust
  it on opencode.

Spec sharpened, not a defect: session-level events (`SessionUpdated`,
`Diagnostic`) may precede a turn's `TurnStarted`; only in-turn events are
bracketed. The test plan's "first event is TurnStarted" criterion was too
strict.

**Round 2 (same day): claude cancel could wedge the session.** Wire probe:
the CLI parks even the *first* user message as `command_lifecycle queued` and
only `started` starts it seconds later (hooks run in between). An
`interrupt{cancel_queued}` landing in that window cancels the prompt out of
the CLI's queue — the receipt names its uuid in `cancelled` and **no `result`
frame ever comes**, so the adapter waited forever. Fix: the adapter tracks
the running turn's user-message uuid; an interrupt receipt naming it ends the
turn as `Cancelled`. The retest's "queue-dependent" shape was a red herring —
the race is purely interrupt-vs-`started` timing.

**Model switching landed (same day), and the typed model surface is gone.**
Sid pulled the P2 model slice forward for the consumer integrations. Survey:
Comet respawns per run with `--model`+`--resume`; T3 calls the SDK's
`setModel` (the `set_model` control request) lazily per turn; the ACP world
models it as the config option `model` (opencode does today). Decision: one
mechanism — the model IS the config option `model`. Claude advertises it from
`initialize`'s catalog (value/displayName/description → `ConfigChoice`, which
gained `description`), maps creation-time configure to `--model` and live
configure to `set_model` (probed live: haiku at open answered "Haiku 4.5",
switch to sonnet answered "Sonnet 5"). The never-implemented typed path —
`Model`, `ModelId`, `AgentDetails::models`, `SessionConfiguration::model`,
`ConfigSelection::Model`, `Capability::LiveModelSwitch`, `Input::model` — is
deleted: two mechanisms for one thing, and the per-turn `Input::model` would
force the engine to sequence model changes against the queue for no consumer
that needs it. Configure-then-prompt is the per-turn pattern.

**The laptop-agent half of the P1 exit gate is met (same day).** On its
`anyagent-integration` branch, laptop-agent deleted `core/src/harness/acp`
(~950 lines: JSON-RPC conn, session setup, steering machinery, ACP
translation) and replaced it with one `harness/driver.rs` against
`Runtime` + `Session` + `Events` — its router, protocol, and all 24 of its
tests untouched. Two integration notes: its voice MCP server needs an
`Authorization` header, carried by `McpServer::with` (surface already fit);
its pre-allowed MCP tools became driver-side auto-answer of `AllowOnce`
(one round trip; promote to a creation option only if latency ever shows).
Verified live in its `--repl` against installed claude: session ready with
honest capabilities (steering false), a text turn, and the Write approval
flow ending with the file on disk. Remaining for P1: Windows
resolution/shutdown; Comet is the next consumer.

## Decided 2026-08-22

1. **Native Claude ships in P0, right after the ACP adapter; no ACP bridge
   profiles.** (Revised 2026-08-23.) Claude Code only speaks ACP through an npm
   adapter that Comet found unreliable and that drops plan usage, fork, and
   rollback. Comet, T3, and vibe-kanban all drive the native wire. ACP goes
   first because S0 is done and laptop-agent is ACP-only; Claude second because
   a seam is only real once two adapters cross it. Codex native in P3.
2. **`Capability::Steer` is advertised.** laptop-agent changes its composer on
   it; `Delivery` still reports what actually happened. (Only on wires that
   can actually steer — Claude's CLI queues instead, so its adapter does not
   advertise it; see Review 2026-08-24.)
3. **Plan usage comes from the CLI, never from our own HTTP.** Claude: the
   `get_usage` control request (on demand, utilisation % per window) plus
   `rate_limit_event` pushes; Codex: `account/rateLimits`. HTTP polling of the
   CLIs' own endpoints stays behind a cargo feature, added only if asked.
4. **Login is an action.** `Runtime::login` drives the CLI's own flow; Anyagent
   never implements OAuth or touches credential files.
5. **No `queue()` getter.** Queue position and departure are already visible
   through `Delivery::Queued` and `TurnStarted`.
6. **`discover` never spawns** (2026-08-23, ticket 03). It finds binaries,
   reads login markers, and lists missing agents with install hints. Version,
   confirmed auth, models, and commands come from `probe`.
7. **Login-shell PATH** is captured once per process with a 5 s cap, cached
   (including a negative result), disabled by `ANYAGENT_NO_LOGIN_SHELL=1`, and
   can be prewarmed.
8. **Any ACP agent** can be used without a catalog entry through
   `AgentInstallation::acp(name, path, args)`.
9. **Platforms:** macOS and Linux in P0; Windows verified in P1.
6. **ACP wire is ours, types are upstream's (S0, 2026-08-23).** Own ~80-line
   JSON-RPC reader over `ProcessHost` stdio; `agent-client-protocol-schema` for
   typed parsing with raw fallback; no SDK runtime. See "ACP implementation".

## How to extend

| Want to add | Touch |
|---|---|
| A new ACP-native agent | One `AgentProfile` entry in `catalog.rs` |
| A new native protocol | One private `Adapter` in `adapter/`, plus its fixtures |
| A new cross-agent feature | One `Capability` variant, one `EventKind` or `Session` method, one private driver command; adapters opt in |
| A provider-only knob | An `Extensions` key documented on the adapter; promote to typed when two consumers need it |
| A new host language | The P4 sidecar speaks the same event and command vocabulary |

The public surface stays: `Runtime` (discover, probe, open, plan_usage, login),
`Session` (prompt, dequeue, answer, configure, rollback, cancel, close), and
`Events`. If a feature cannot fit there, the design is wrong, not the surface.
