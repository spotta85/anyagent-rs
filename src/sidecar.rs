//! The public API over a pair of byte streams, one JSON object per line.
//! This is what the `anyagent serve` binary runs and what every language
//! wrapper talks to, so the crate stays the only place with logic.
//!
//! ```text
//! out  {"hello": {"protocol": 1, "anyagent": "0.0.3"}}          first line
//! in   {"id": 1, "cmd": "open", "agent": "claude", "dir": "."}
//! out  {"id": 1, "ok": {..SessionInfo..}}                       or {"id": 1, "error": {..}}
//! out  {"event": {..Event..}}                                   carries session_id
//! out  {"session": "s1", "error": {..}}                         the stream failed
//! out  {"closed": "s1"}                                         the stream ended
//! ```
//!
//! Commands map one to one onto `Runtime` and `Session`; events are the
//! crate's own serialization, unchanged. The `open` reply always precedes
//! that session's first frame, and EOF on the input closes every session.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::{
    AgentError, AgentInstallation, Answer, ConfigId, ConfigValue, DiscoveryReport, Events, Input,
    McpServer, MessageId, PermissionMode, PromptId, RequestId, ResumeToken, RollbackScope, Runtime,
    Session, SessionId, SessionOptions,
};

// ---------------------------------------------------------------------------
// PUBLIC: the protocol version, the serve loop, the wire schema
// ---------------------------------------------------------------------------

/// Bumped only when a frame or command changes shape incompatibly.
pub const PROTOCOL: u32 = 1;

/// Runs the sidecar until `input` ends. Reads command lines, writes reply
/// and event lines to `output`, then closes every open session.
pub async fn serve(
    runtime: Runtime,
    input: impl AsyncBufRead + Unpin,
    output: impl AsyncWrite + Unpin + Send + 'static,
) -> std::io::Result<()> {
    let (out, rx) = mpsc::channel::<String>(256);
    let writer = tokio::spawn(write_lines(output, rx));
    let hello = Hello {
        protocol: PROTOCOL,
        anyagent: env!("CARGO_PKG_VERSION"),
    };
    let _ = out.send(Line::Hello { hello }.json()).await;

    let state = Arc::new(State::new(runtime));
    let mut lines = input.lines();
    while let Some(line) = lines.next_line().await? {
        if out.is_closed() {
            break;
        }
        let (id, cmd) = match parse(&line) {
            Ok(frame) => frame,
            Err((id, error)) => {
                let _ = out.send(Line::Error { id, error }.json()).await;
                continue;
            }
        };
        let (state, out) = (Arc::clone(&state), out.clone());
        tokio::spawn(async move {
            match handle(&state, cmd).await {
                Ok(Reply { ok, forward }) => {
                    // The open reply goes out before the first event.
                    let _ = out.send(Line::Reply { id, ok }.json()).await;
                    if let Some((session, events)) = forward {
                        tokio::spawn(forward_events(session, events, out));
                    }
                }
                Err(fail) => {
                    let error = fail.body();
                    let _ = out
                        .send(
                            Line::Error {
                                id: Some(id),
                                error,
                            }
                            .json(),
                        )
                        .await;
                }
            }
        });
    }
    // Each forwarder holds a sender, so the writer drains until the last
    // `closed` is written.
    state.close_all().await;
    drop(out);
    writer.await.map_err(std::io::Error::other)?
}

/// Every wire type in one JSON schema (draft 7), so each wrapper's types
/// come from the same file: `cargo run --example schema --features schema`.
#[cfg(feature = "schema")]
pub fn schema() -> schemars::Schema {
    schemars::generate::SchemaSettings::draft07()
        .into_generator()
        .into_root_schema_for::<Protocol>()
}

// ---------------------------------------------------------------------------
// SERVE HELPERS: one command in, one session's events out
// ---------------------------------------------------------------------------

/// Dispatches one command to the crate.
async fn handle(state: &State, cmd: Cmd) -> Result<Reply, Fail> {
    match cmd {
        Cmd::Discover => {
            let report = state.runtime.discover().await;
            let reply = Reply::ok(&report);
            *state.report.lock().unwrap() = Some(report);
            reply
        }
        Cmd::Probe { agent } => {
            let agent = state.resolve(agent).await?;
            Reply::ok(state.runtime.probe(&agent).await?)
        }
        Cmd::PlanUsage { agent } => {
            let agent = state.resolve(agent).await?;
            Reply::ok(state.runtime.plan_usage(&agent).await?)
        }
        Cmd::Generate {
            agent,
            dir,
            prompt,
            options,
        } => {
            let agent = state.resolve(agent).await?;
            let options = options.into_session_options(dir);
            Reply::ok(state.runtime.generate(&agent, options, prompt).await?)
        }
        Cmd::Open {
            agent,
            dir,
            options,
        } => {
            let agent = state.resolve(agent).await?;
            let options = options.into_session_options(dir);
            let (session, events) = state.runtime.open(&agent, options).await?;
            let (id, info) = (session.id().clone(), session.info());
            if !state.register(session.clone()) {
                let _ = session.close().await;
            }
            Ok(Reply {
                ok: serde_json::to_value(info)?,
                forward: Some((id, events)),
            })
        }
        Cmd::Prompt {
            session,
            text,
            attachments,
        } => {
            let input = attachments
                .into_iter()
                .fold(Input::text(text), |input, path| input.attach(path));
            Reply::ok(state.session(&session)?.prompt(input).await?)
        }
        Cmd::Dequeue { session, prompt } => {
            Reply::ok(state.session(&session)?.dequeue(prompt).await?)
        }
        Cmd::Answer {
            session,
            request,
            answer,
        } => Reply::ok(state.session(&session)?.answer(request, answer).await?),
        Cmd::Configure {
            session,
            option,
            value,
        } => Reply::ok(state.session(&session)?.configure(option, value).await?),
        Cmd::Rollback {
            session,
            turns,
            scope,
        } => Reply::ok(state.session(&session)?.rollback(turns, scope).await?),
        Cmd::Compact { session } => Reply::ok(state.session(&session)?.compact().await?),
        Cmd::Cancel {
            session,
            clear_queue,
        } => Reply::ok(state.session(&session)?.cancel(clear_queue).await?),
        Cmd::Info { session } => Reply::ok(state.session(&session)?.info()),
        Cmd::Close { session } => Reply::ok(state.session(&session)?.close().await?),
    }
}

/// Copies one session's events to the output. A stream error is written
/// under the session's id; `closed` always follows the end of the stream.
async fn forward_events(id: SessionId, mut events: Events, out: mpsc::Sender<String>) {
    while let Some(event) = events.next().await {
        let line = match event {
            Ok(event) => Line::Event { event },
            Err(e) => Line::SessionError {
                session: id.clone(),
                error: error_body(&e),
            },
        };
        if out.send(line.json()).await.is_err() {
            return;
        }
    }
    let _ = out.send(Line::Closed { closed: id }.json()).await;
}

// ---------------------------------------------------------------------------
// WIRE TYPES: the lines out and the commands in
// ---------------------------------------------------------------------------

/// One line to the app. Untagged, so each variant is a flat object.
#[derive(Serialize)]
// Built, serialized, dropped: the size gap between variants costs nothing.
#[allow(clippy::large_enum_variant)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
enum Line {
    Hello { hello: Hello },
    Reply { id: u64, ok: Value },
    Error { id: Option<u64>, error: Value },
    Event { event: crate::Event },
    SessionError { session: SessionId, error: Value },
    Closed { closed: SessionId },
}

impl Line {
    fn json(&self) -> String {
        serde_json::to_string(self).expect("wire types serialize")
    }
}

/// The first line: which protocol, from which crate version.
#[derive(Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
struct Hello {
    protocol: u32,
    anyagent: &'static str,
}

/// One line from the app: the id to reply to, and the command.
#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
struct Frame {
    id: u64,
    #[serde(flatten)]
    cmd: Cmd,
}

/// Every command, named after the crate call it makes.
#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum Cmd {
    Discover,
    Probe {
        agent: AgentRef,
    },
    PlanUsage {
        agent: AgentRef,
    },
    Generate {
        agent: AgentRef,
        dir: PathBuf,
        prompt: String,
        #[serde(flatten)]
        options: OpenOptions,
    },
    Open {
        agent: AgentRef,
        dir: PathBuf,
        #[serde(flatten)]
        options: OpenOptions,
    },
    Prompt {
        session: SessionId,
        text: String,
        #[serde(default)]
        attachments: Vec<PathBuf>,
    },
    Dequeue {
        session: SessionId,
        prompt: PromptId,
    },
    Answer {
        session: SessionId,
        request: RequestId,
        answer: Answer,
    },
    Configure {
        session: SessionId,
        option: ConfigId,
        value: ConfigValue,
    },
    Rollback {
        session: SessionId,
        turns: NonZeroU32,
        scope: RollbackScope,
    },
    Compact {
        session: SessionId,
    },
    Cancel {
        session: SessionId,
        #[serde(default)]
        clear_queue: bool,
    },
    Info {
        session: SessionId,
    },
    Close {
        session: SessionId,
    },
}

/// A catalog id like `"claude"`, or an ACP agent the catalog does not know.
#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
enum AgentRef {
    Id(String),
    Acp { acp: AcpSpec },
}

#[derive(Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
struct AcpSpec {
    name: String,
    path: PathBuf,
    #[serde(default)]
    args: Vec<String>,
}

/// The `SessionOptions` the wire exposes, as top-level fields of `open`
/// and `generate`.
#[derive(Deserialize, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
struct OpenOptions {
    resume: Option<ResumeToken>,
    fork: Option<ResumeToken>,
    fork_at: Option<MessageId>,
    permission_mode: Option<PermissionMode>,
    #[serde(default)]
    mcp_servers: Vec<McpServer>,
    #[serde(default)]
    configure: BTreeMap<ConfigId, ConfigValue>,
}

impl OpenOptions {
    /// Applies each given field through the crate's builder.
    fn into_session_options(self, dir: PathBuf) -> SessionOptions {
        let mut options = SessionOptions::in_dir(dir);
        if let Some(token) = self.resume {
            options = options.resume(token);
        }
        if let Some(token) = self.fork {
            options = options.fork_from(token, self.fork_at);
        }
        if let Some(mode) = self.permission_mode {
            options = options.permission_mode(mode);
        }
        for server in self.mcp_servers {
            options = options.mcp_server(server);
        }
        for (id, value) in self.configure {
            options = options.configure(id, value);
        }
        options
    }
}

// ---------------------------------------------------------------------------
// REPLIES AND ERRORS: what a command produced, or why it failed
// ---------------------------------------------------------------------------

/// What a command produced: the `ok` payload and, for `open`, the stream
/// to start forwarding once the reply is written.
struct Reply {
    ok: Value,
    forward: Option<(SessionId, Events)>,
}

impl Reply {
    fn ok(value: impl Serialize) -> Result<Self, Fail> {
        Ok(Self {
            ok: serde_json::to_value(value)?,
            forward: None,
        })
    }
}

/// Why a command failed: the crate said so, or the session id is unknown.
enum Fail {
    Agent(AgentError),
    UnknownSession(SessionId),
}

impl From<AgentError> for Fail {
    fn from(e: AgentError) -> Self {
        Self::Agent(e)
    }
}

/// A reply that will not encode is the crate's own protocol failure.
impl From<serde_json::Error> for Fail {
    fn from(e: serde_json::Error) -> Self {
        Self::Agent(AgentError::ProtocolFailed(format!(
            "could not encode the reply: {e}"
        )))
    }
}

impl Fail {
    fn body(self) -> Value {
        match self {
            Self::Agent(e) => error_body(&e),
            Self::UnknownSession(id) => json!({
                "kind": "UnknownSession",
                "message": format!("no session {id}"),
                "session": id,
            }),
        }
    }
}

/// `kind`, `message`, and the variant's own fields, so nothing typed is lost.
fn error_body(e: &AgentError) -> Value {
    let (kind, mut body) = match e {
        AgentError::NotInstalled(agent) => ("NotInstalled", json!({ "agent": agent })),
        AgentError::SpawnFailed(d) => ("SpawnFailed", json!({ "detail": d })),
        AgentError::AuthRequired { login } => ("AuthRequired", json!({ "login": login })),
        AgentError::HandshakeTimeout => ("HandshakeTimeout", json!({})),
        AgentError::UnsupportedFeature(d) => ("UnsupportedFeature", json!({ "detail": d })),
        AgentError::InvalidConfiguration(d) => ("InvalidConfiguration", json!({ "detail": d })),
        AgentError::InvalidRequest(d) => ("InvalidRequest", json!({ "detail": d })),
        AgentError::ResumeFailed(d) => ("ResumeFailed", json!({ "detail": d })),
        AgentError::SessionBusy => ("SessionBusy", json!({})),
        AgentError::ProtocolFailed(d) => ("ProtocolFailed", json!({ "detail": d })),
        AgentError::ProcessExited { status, stderr } => (
            "ProcessExited",
            json!({ "status": status, "stderr": stderr }),
        ),
        AgentError::SessionClosed => ("SessionClosed", json!({})),
    };
    body["kind"] = json!(kind);
    body["message"] = json!(e.to_string());
    body
}

fn bad_frame(e: serde_json::Error) -> Value {
    json!({ "kind": "BadFrame", "message": format!("not a command: {e}"), "detail": e.to_string() })
}

// ---------------------------------------------------------------------------
// STATE: the runtime, the last discovery, the open sessions
// ---------------------------------------------------------------------------

/// Everything one `serve` call owns: the runtime, the last discovery, and
/// the open sessions. `sessions` is `None` once the input has ended.
struct State {
    runtime: Runtime,
    report: Mutex<Option<DiscoveryReport>>,
    sessions: Mutex<Option<HashMap<SessionId, Session>>>,
}

impl State {
    fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            report: Mutex::new(None),
            sessions: Mutex::new(Some(HashMap::new())),
        }
    }

    /// Keeps a session's handle. `false` once the input has ended, so the
    /// caller closes the session instead of leaving it running.
    fn register(&self, session: Session) -> bool {
        match self.sessions.lock().unwrap().as_mut() {
            Some(open) => {
                open.insert(session.id().clone(), session);
                true
            }
            None => false,
        }
    }

    /// A catalog id resolves against the last discovery, running one if
    /// needed; an inline ACP spec needs no lookup.
    async fn resolve(&self, agent: AgentRef) -> Result<AgentInstallation, Fail> {
        let id = match agent {
            AgentRef::Acp { acp } => {
                return Ok(AgentInstallation::acp(acp.name, acp.path, acp.args));
            }
            AgentRef::Id(id) => id,
        };
        let cached = self
            .report
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|report| report.require(&id).ok().cloned());
        if let Some(agent) = cached {
            return Ok(agent);
        }
        let report = self.runtime.discover().await;
        let found = report.require(&id).cloned();
        *self.report.lock().unwrap() = Some(report);
        Ok(found?)
    }

    /// The handle for an opened session. Closed sessions stay in the map
    /// so a late command gets the crate's `SessionClosed`, not `UnknownSession`.
    fn session(&self, id: &SessionId) -> Result<Session, Fail> {
        self.sessions
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|open| open.get(id))
            .cloned()
            .ok_or_else(|| Fail::UnknownSession(id.clone()))
    }

    /// Stops taking sessions and closes every open one; each forwarder
    /// then writes its `closed`.
    async fn close_all(&self) {
        let open = self.sessions.lock().unwrap().take().unwrap_or_default();
        futures::future::join_all(open.values().map(Session::close)).await;
    }
}

// ---------------------------------------------------------------------------
// LINE HELPERS: parse one line in, write every line out
// ---------------------------------------------------------------------------

/// Splits a line into its id and command. A line that is not a command
/// yields the id it carried, if any, so the app can match the error.
fn parse(line: &str) -> Result<(u64, Cmd), (Option<u64>, Value)> {
    let value: Value = serde_json::from_str(line).map_err(|e| (None, bad_frame(e)))?;
    let id = value.get("id").and_then(Value::as_u64);
    let frame: Frame = serde_json::from_value(value).map_err(|e| (id, bad_frame(e)))?;
    Ok((frame.id, frame.cmd))
}

/// The single writer: every line on the output goes through here, so
/// replies and events never interleave mid-line.
async fn write_lines(
    mut output: impl AsyncWrite + Unpin,
    mut rx: mpsc::Receiver<String>,
) -> std::io::Result<()> {
    while let Some(mut line) = rx.recv().await {
        line.push('\n');
        output.write_all(line.as_bytes()).await?;
        output.flush().await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SCHEMA TYPES: the shapes the generated schema needs
// ---------------------------------------------------------------------------

/// The wire's entry points; each field puts one type under `definitions`.
#[cfg(feature = "schema")]
#[derive(schemars::JsonSchema)]
#[allow(dead_code)]
struct Protocol {
    command: Frame,
    line: Line,
    event: crate::Event,
    error: ErrorBody,
    discovery: DiscoveryReport,
    details: crate::AgentDetails,
    plan_usage: crate::PlanUsage,
    session_info: crate::SessionInfo,
    delivery: crate::Delivery,
}

/// The `error` object: `kind`, `message`, and the variant's own fields.
#[cfg(feature = "schema")]
#[derive(schemars::JsonSchema)]
#[allow(dead_code)]
struct ErrorBody {
    kind: String,
    message: String,
    #[serde(flatten)]
    fields: BTreeMap<String, Value>,
}
