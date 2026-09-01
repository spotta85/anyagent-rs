//! Everything that flows out of a session: events, tools, requests, answers,
//! usage, and the stop reasons that end a turn.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::agent::string_id;
use crate::session::{SessionInfo, SessionStatus};

string_id!(SessionId);
string_id!(TurnId);
string_id!(PromptId);
string_id!(MessageId);
string_id!(ToolId);
string_id!(RequestId);
string_id!(QuestionId);
string_id!(ChoiceId);

/// Namespaced provider data Anyagent has not normalized yet. Keys use
/// `provider/name`, for example `codex/thread_status`.
pub type Extensions = BTreeMap<String, serde_json::Value>;

/// One normalized event produced by anyagent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Starts at 1 and increases for every session event.
    pub sequence: u64,
    /// When the engine produced this event — wall-clock, so transcripts can
    /// be stored and merged without the app stamping receive time. Order
    /// across events is guaranteed by `sequence`, not by this clock.
    pub occurred_at: std::time::SystemTime,
    pub session_id: SessionId,
    /// Present when the event belongs to a turn.
    pub turn_info: Option<TurnContext>,
    pub kind: EventKind,
    /// Provider-specific data that has not been normalized.
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnContext {
    pub id: TurnId,
    /// Present for events produced by a subagent spawned from this tool call.
    pub parent_tool_id: Option<ToolId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Provider-originated user-role content, e.g. a parent agent steering
    /// a subagent. Never a replay of the caller's own prompt.
    UserMessage {
        message_id: MessageId,
        text: String,
    },
    /// No more deltas will arrive for this message.
    MessageEnded {
        message_id: MessageId,
    },
    /// Cumulative snapshot of one tool call: start, each change, completion.
    ToolUpdated(ToolUpdate),
    /// Streamed output of a running command tool.
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
    /// The session's UI state flipped: working, needing input, or idle.
    /// Emitted only on change; `session.status()` answers without the stream.
    StatusChanged(SessionStatus),
    /// Context-window occupancy for this session.
    ContextUsage {
        used_tokens: u64,
        window_tokens: Option<u64>,
        cost_usd: Option<f64>,
    },
    /// The agent compacted its context; the next `ContextUsage` drops.
    ContextCompacted,
    /// Plan quota for the account, pushed by the adapter.
    PlanUsageUpdated(PlanUsage),
    Diagnostic(Diagnostic),
    TurnEnded {
        stop: StopReason,
        background: Vec<ToolId>, // still running after the turn ended
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TurnOrigin {
    Prompt(PromptId),
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum StopReason {
    Completed { source: CompletionSource },
    Cancelled,
    Refused,
    Failed { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CompletionSource {
    /// The provider protocol explicitly ended the turn.
    Protocol,
    /// Anyagent inferred completion after the provider went quiet.
    Inferred,
}

// ---------------------------------------------------------------------------
// Tools and plans
// ---------------------------------------------------------------------------

/// Cumulative snapshot of one tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolUpdate {
    pub id: ToolId,
    pub kind: ToolKind,
    pub title: String,
    pub status: ToolStatus,
    pub input: ToolInput,
    pub output: Option<String>,
    pub diffs: Vec<FileDiff>,
    pub locations: Vec<PathBuf>,
    /// Agent's own tool name and raw input, for unknown or MCP tools.
    pub raw: Option<RawTool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ToolKind {
    Read,
    Edit,
    Delete,
    Move,
    Search,
    Execute,
    Fetch,
    Think,
    Mcp {
        server: String,
        tool: String,
    },
    /// Spawned a subagent; nested events carry this id in `parent_tool_id`.
    Subagent,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ToolStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl ToolStatus {
    /// Still occupying the agent: the turn is not over while this is true.
    pub fn is_active(self) -> bool {
        matches!(self, ToolStatus::Pending | ToolStatus::Running)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ToolInput {
    Path(PathBuf),
    Command {
        command: String,
        cwd: Option<PathBuf>,
    },
    Pattern(String),
    Url(String),
    Query(String),
    Text(String),
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawTool {
    pub name: String,
    pub input: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDiff {
    pub path: PathBuf,
    /// `None` means a new file.
    pub old_text: Option<String>,
    pub new_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanEntry {
    pub text: String,
    pub status: PlanStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PlanStatus {
    Pending,
    InProgress,
    Completed,
}

// ---------------------------------------------------------------------------
// Requests and answers
// ---------------------------------------------------------------------------

/// Something the agent is waiting on the caller for. Answer once with
/// `Session::answer`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
// Public request records stay by value; boxing would move allocation to callers.
#[allow(clippy::large_enum_variant)]
pub enum Request {
    Permission(PermissionRequest),
    Question(QuestionRequest),
}

impl Request {
    pub fn id(&self) -> RequestId {
        match self {
            Request::Permission(r) => r.id.clone(),
            Request::Question(r) => r.id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: RequestId,
    /// The tool call awaiting approval, as the app already saw it.
    pub tool: ToolUpdate,
    /// What the agent offers; answers outside this list are rejected.
    pub options: Vec<PermissionChoice>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PermissionChoice {
    AllowOnce,
    AllowAlways,
    DenyOnce,
    DenyAlways,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionRequest {
    pub id: RequestId,
    pub questions: Vec<Question>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    pub id: QuestionId,
    pub text: String,
    pub header: Option<String>,
    /// Empty means free text only.
    pub choices: Vec<Choice>,
    pub multi_select: bool,
    pub allows_free_text: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Choice {
    pub id: ChoiceId,
    pub label: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Answer {
    Permission(PermissionChoice),
    /// One entry per question, in order.
    Question(Vec<QuestionAnswer>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuestionAnswer {
    Choices(Vec<ChoiceId>),
    Text(String),
}

// ---------------------------------------------------------------------------
// Usage and diagnostics
// ---------------------------------------------------------------------------

/// Plan quota windows for the logged-in account.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanUsage {
    /// The plan the quota belongs to ("max", "edu"), when the agent names it.
    pub plan: Option<String>,
    pub windows: Vec<UsageWindow>,
    pub fetched_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageWindow {
    /// "Session" (5h), "Week", or an agent-provided label.
    pub label: String,
    pub used_percent: u8,
    pub resets_at: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: DiagnosticLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DiagnosticLevel {
    Info,
    Warning,
    Error,
}

/// Immediate result of submitting a prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivery {
    /// Stable across immediate delivery and later queue promotion.
    pub prompt_id: PromptId,
    pub kind: DeliveryKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DeliveryKind {
    Started {
        turn_id: TurnId,
    },
    Steered {
        turn_id: TurnId,
    },
    Queued {
        /// Number of prompts ahead of this one.
        position: u32,
    },
}
