//! The session engine owns the adapter connection, turn state, and prompt
//! queue. Applications hold only `Session` and `Events`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::adapter::{DriverCommand, DriverConnection, DriverEvent, DriverInfo};
use crate::agent::{
    AgentDetails, AgentInstallation, Capability, ConfigId, ConfigKind, ConfigValue, Input,
    LoginMethod, PermissionMode, ResumeToken, RollbackScope, SessionConfiguration, SessionOptions,
};
use crate::error::AgentError;
use crate::event::{
    Answer, ChoiceId, CompletionSource, Delivery, DeliveryKind, Diagnostic, DiagnosticLevel, Event,
    EventKind, Extensions, MessageId, PermissionChoice, PromptId, QuestionAnswer, Request,
    RequestId, SessionId, StopReason, ToolId, TurnContext, TurnId, TurnOrigin,
};

/// Consumer event buffer. Generous because the engine never waits on it: a
/// consumer that falls this far behind is treated as gone (see `push`).
const EVENT_BUFFER: usize = 1024;
const CLOSE_GRACE: Duration = Duration::from_secs(5);
const QUIET_USER_TURN: Duration = Duration::from_secs(120);
const QUIET_AGENT_TURN: Duration = Duration::from_secs(20);

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

/// Snapshot of a live session. Also carried by `EventKind::SessionUpdated`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: SessionId,
    pub agent: AgentInstallation,
    pub details: AgentDetails,
    pub configuration: SessionConfiguration,
    pub resume_token: Option<ResumeToken>,
    /// Agent-suggested title when the wire provides one.
    pub title: Option<String>,
    /// Current UI state; kept fresh by the engine.
    #[serde(default)]
    pub status: SessionStatus,
}

/// What a UI should show for the session right now. Changes arrive as
/// `EventKind::StatusChanged`; `Session::status` reads it without the stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SessionStatus {
    /// No turn running and nothing queued.
    #[default]
    Idle,
    /// A turn is running.
    Working,
    /// The agent is blocked on an answer to at least one open request.
    NeedsInput,
}

/// Command handle. Cheap to clone; every clone talks to the same engine task.
#[derive(Clone)]
pub struct Session {
    id: SessionId,
    commands: mpsc::Sender<Command>,
    info: Arc<Mutex<SessionInfo>>,
}

/// Ordered event stream, buffering up to 1024 undrained events.
///
/// Drain it continuously. The engine never waits on this buffer, so `cancel`
/// and `answer` stay responsive — but a consumer that falls a full buffer
/// behind is treated as gone: delivery stops and the session closes, the same
/// as dropping `Events`.
pub struct Events(mpsc::Receiver<Result<Event, AgentError>>);

impl Stream for Events {
    type Item = Result<Event, AgentError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}
/// One reply from the engine to a session command.
type Reply<T> = oneshot::Sender<Result<T, AgentError>>;

/// Commands accepted by the session engine.
enum Command {
    Prompt(Input, Reply<Delivery>),
    Dequeue(PromptId, Reply<()>),
    Answer(RequestId, Answer, Reply<()>),
    Configure(ConfigId, ConfigValue, Reply<()>),
    Rollback(NonZeroU32, RollbackScope, Reply<()>),
    Cancel { clear_queue: bool, reply: Reply<()> },
    Close(Reply<()>),
}

impl Session {
    pub fn id(&self) -> &SessionId {
        &self.id
    }

    /// Current snapshot; the same value the last `SessionUpdated` carried.
    pub fn info(&self) -> SessionInfo {
        self.info.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// What a UI should show right now: working, needing input, or idle.
    /// The push form of the same fact is `EventKind::StatusChanged`.
    pub fn status(&self) -> SessionStatus {
        self.info.lock().unwrap_or_else(|e| e.into_inner()).status
    }

    /// Starts a turn when idle. During a turn, steers when possible or queues.
    pub async fn prompt(&self, input: impl Into<Input>) -> Result<Delivery, AgentError> {
        self.send(|reply| Command::Prompt(input.into(), reply))
            .await
    }

    /// Drops one queued prompt. `InvalidRequest` if it already started.
    pub async fn dequeue(&self, prompt: PromptId) -> Result<(), AgentError> {
        self.send(|reply| Command::Dequeue(prompt, reply)).await
    }

    /// Answers one open permission or question request, exactly once.
    pub async fn answer(&self, request: RequestId, answer: Answer) -> Result<(), AgentError> {
        self.send(|reply| Command::Answer(request, answer, reply))
            .await
    }

    /// Applies one advertised option (`model`, `mode`, …); confirmed by
    /// `SessionUpdated`.
    pub async fn configure(
        &self,
        id: impl Into<ConfigId>,
        value: impl Into<ConfigValue>,
    ) -> Result<(), AgentError> {
        let (id, value) = (id.into(), value.into());
        self.send(|reply| Command::Configure(id, value, reply))
            .await
    }

    /// Rewinds provider-owned conversation context by completed turns; the
    /// files scope also restores agent-changed files to the cut point.
    /// Requires an idle session and rollback support. `SessionUpdated`
    /// confirms success; a diagnostic reports rejection.
    pub async fn rollback(
        &self,
        turns: NonZeroU32,
        scope: RollbackScope,
    ) -> Result<(), AgentError> {
        self.send(|reply| Command::Rollback(turns, scope, reply))
            .await
    }

    /// Stops the active turn. The session and the queue survive; the next
    /// queued prompt starts unless `clear_queue` is set.
    pub async fn cancel(&self, clear_queue: bool) -> Result<(), AgentError> {
        self.send(|reply| Command::Cancel { clear_queue, reply })
            .await
    }

    /// Ends the agent session and waits for cleanup, capped by a grace period.
    /// Agent-spawned background processes may outlive the session.
    pub async fn close(&self) -> Result<(), AgentError> {
        self.send(Command::Close).await
    }

    /// Sends one command and awaits the engine's reply.
    async fn send<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T, AgentError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(make(tx))
            .await
            .map_err(|_| AgentError::SessionClosed)?;
        rx.await.map_err(|_| AgentError::SessionClosed)?
    }
}

/// Spawns the engine task for a fresh driver connection and hands back the
/// two handles. The engine never waits on the consumer buffer (`push` uses
/// `try_send`), so `cancel` stays responsive and memory stays bounded.
pub(crate) fn start(
    agent: AgentInstallation,
    connection: DriverConnection,
    options: &SessionOptions,
) -> (Session, Events) {
    let id = SessionId::new(format!("s{}", NEXT_SESSION.fetch_add(1, Ordering::Relaxed)));
    let info = Arc::new(Mutex::new(session_info(
        id.clone(),
        agent,
        &connection.info,
    )));
    let (commands_tx, commands_rx) = mpsc::channel(64);
    let (events_tx, events_rx) = mpsc::channel(EVENT_BUFFER);

    let engine = Engine {
        id: id.clone(),
        info: Arc::clone(&info),
        driver: connection.commands,
        events: events_tx,
        events_alive: true,
        seq: 0,
        next_turn: 1,
        next_prompt: 1,
        state: TurnState::Idle,
        queue: VecDeque::new(),
        steer: None,
        steer_supported: connection
            .info
            .details
            .capabilities
            .supports(Capability::Steer),
        quiet_user: quiet_window(
            connection.info.deterministic_turn_end,
            options,
            QUIET_USER_TURN,
        ),
        quiet_agent: quiet_window(
            connection.info.deterministic_agent_turn_end,
            options,
            QUIET_AGENT_TURN,
        ),
        deadline: None,
        closing: None,
        auto_approve: matches!(options.permission_mode, PermissionMode::AutoApprove),
        exit: None,
        noise_reported: false,
        awaiting_ack: false,
        last_status: SessionStatus::Idle,
        done: false,
    };
    tokio::spawn(engine.run(commands_rx, connection.events));

    (
        Session {
            id,
            commands: commands_tx,
            info,
        },
        Events(events_rx),
    )
}

/// The quiet window for inferred completion, or `None` when the wire ends
/// turns itself.
fn quiet_window(
    deterministic: bool,
    options: &SessionOptions,
    default: Duration,
) -> Option<Duration> {
    if deterministic {
        return None;
    }
    Some(options.quiet_window.unwrap_or(default))
}

fn session_info(id: SessionId, agent: AgentInstallation, info: &DriverInfo) -> SessionInfo {
    SessionInfo {
        id,
        agent,
        details: info.details.clone(),
        configuration: info.configuration.clone(),
        resume_token: info.resume_token.clone(),
        title: info.title.clone(),
        status: SessionStatus::Idle,
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

enum TurnState {
    Idle,
    Running {
        turn: TurnId,
        origin: TurnOrigin,
        open_requests: BTreeMap<RequestId, RequestShape>,
        running_tools: BTreeSet<ToolId>,
        /// Messages that streamed deltas but have no `MessageEnded` yet; the
        /// engine closes them at turn end for wires with no end-of-message
        /// signal (ACP).
        open_messages: BTreeSet<MessageId>,
    },
    Closing,
}

/// What an open request accepts, kept for answer validation.
enum RequestShape {
    Permission(Vec<PermissionChoice>),
    Question(Vec<QuestionShape>),
}

/// What one question accepts.
struct QuestionShape {
    choices: Vec<ChoiceId>,
    multi_select: bool,
    allows_free_text: bool,
}

impl RequestShape {
    fn of(request: &Request) -> Self {
        match request {
            Request::Permission(r) => RequestShape::Permission(r.options.clone()),
            Request::Question(r) => RequestShape::Question(
                r.questions
                    .iter()
                    .map(|q| QuestionShape {
                        choices: q.choices.iter().map(|c| c.id.clone()).collect(),
                        multi_select: q.multi_select,
                        allows_free_text: q.allows_free_text,
                    })
                    .collect(),
            ),
        }
    }

    /// Rejects an answer the request did not offer.
    fn accepts(&self, answer: &Answer) -> Result<(), AgentError> {
        let complaint = match (self, answer) {
            (RequestShape::Permission(options), Answer::Permission(choice)) => {
                if options.contains(choice) {
                    return Ok(());
                }
                format!("choice {choice:?} was not offered")
            }
            (RequestShape::Question(questions), Answer::Question(answers)) => {
                if answers.len() != questions.len() {
                    format!(
                        "expected {} answers, got {}",
                        questions.len(),
                        answers.len()
                    )
                } else {
                    match questions
                        .iter()
                        .zip(answers)
                        .find_map(|(q, a)| question_complaint(q, a))
                    {
                        Some(complaint) => complaint,
                        None => return Ok(()),
                    }
                }
            }
            _ => "answer type does not match the request".into(),
        };
        Err(AgentError::InvalidRequest(complaint))
    }
}

/// The first thing wrong with one question's answer, if anything.
fn question_complaint(question: &QuestionShape, answer: &QuestionAnswer) -> Option<String> {
    match answer {
        QuestionAnswer::Text(_) if question.allows_free_text => None,
        QuestionAnswer::Text(_) => Some("free text was not allowed".into()),
        QuestionAnswer::Choices(choices) => {
            if choices.is_empty() {
                return Some("no choice given".into());
            }
            if choices.len() > 1 && !question.multi_select {
                return Some("multiple choices for a single-select question".into());
            }
            choices
                .iter()
                .find(|c| !question.choices.contains(c))
                .map(|c| format!("choice `{}` was not offered", c.as_str()))
        }
    }
}

struct Engine {
    id: SessionId,
    info: Arc<Mutex<SessionInfo>>,
    driver: mpsc::Sender<DriverCommand>,
    events: mpsc::Sender<Result<Event, AgentError>>,
    events_alive: bool,
    seq: u64,
    next_turn: u64,
    next_prompt: u64,
    state: TurnState,
    queue: VecDeque<(PromptId, Input)>,
    /// A steer the adapter has not answered yet; resolved by `Steered`.
    steer: Option<(PromptId, Input, Reply<Delivery>)>,
    steer_supported: bool,
    quiet_user: Option<Duration>,
    quiet_agent: Option<Duration>,
    /// Inferred completion (or close grace) fires here unless reset.
    deadline: Option<Instant>,
    /// `close` callers waiting for the driver to finish.
    closing: Option<Vec<Reply<()>>>,
    /// `PermissionMode::AutoApprove`: allow each permission request once.
    auto_approve: bool,
    /// Exit report from the adapter, delivered just before its channel closes.
    exit: Option<(String, String)>,
    noise_reported: bool,
    /// A `StartTurn` is unacknowledged: content arriving now is stale.
    awaiting_ack: bool,
    /// The last status emitted, so `StatusChanged` fires only on change.
    last_status: SessionStatus,
    done: bool,
}

impl Engine {
    /// Main loop: commands in, driver events in, watchdog ticks, then promote
    /// queued prompts.
    async fn run(
        mut self,
        mut commands: mpsc::Receiver<Command>,
        mut driver_events: mpsc::Receiver<DriverEvent>,
    ) {
        let mut commands_open = true;
        while !self.done {
            let deadline = self.deadline.unwrap_or_else(Instant::now);
            tokio::select! {
                biased;
                cmd = commands.recv(), if commands_open => match cmd {
                    Some(cmd) => self.handle_command(cmd).await,
                    None => {
                        commands_open = false;
                        self.shutdown(None).await;
                    }
                },
                ev = driver_events.recv() => match ev {
                    Some(ev) => self.handle_driver_event(ev).await,
                    None => self.driver_gone().await,
                },
                _ = tokio::time::sleep_until(deadline), if self.deadline.is_some() => {
                    self.on_quiet().await;
                }
            }
            // Frames already queued may still belong to the turn that just
            // ended, so drain them before promoting. This only covers frames
            // the adapter has delivered; one arriving later still loses the
            // race and lands in the promoted turn.
            if driver_events.is_empty() {
                self.promote().await;
            }
            // After promotion, so a turn ending with another prompt queued
            // reads as continuously Working, never a flash of Idle.
            self.sync_status().await;
        }
    }

    /// Emits `StatusChanged` and refreshes the snapshot when the UI state
    /// actually flipped.
    async fn sync_status(&mut self) {
        let status = match &self.state {
            TurnState::Running { open_requests, .. } if !open_requests.is_empty() => {
                SessionStatus::NeedsInput
            }
            TurnState::Running { .. } => SessionStatus::Working,
            // Queued prompts never promote past a close; they must not keep
            // the final snapshot on Working.
            TurnState::Closing => SessionStatus::Idle,
            // A queued prompt promotes on the next pass (trailing driver
            // events can defer it); the gap must not flash Idle.
            _ if !self.queue.is_empty() => SessionStatus::Working,
            _ => SessionStatus::Idle,
        };
        if status == self.last_status {
            return;
        }
        self.last_status = status;
        self.info.lock().unwrap_or_else(|e| e.into_inner()).status = status;
        self.emit(EventKind::StatusChanged(status), false).await;
    }

    // -- commands -----------------------------------------------------------

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Prompt(input, reply) => self.handle_prompt(input, reply).await,
            Command::Dequeue(id, reply) => {
                let before = self.queue.len();
                self.queue.retain(|(p, _)| *p != id);
                let _ = reply.send(if self.queue.len() < before {
                    Ok(())
                } else {
                    Err(AgentError::InvalidRequest(format!(
                        "prompt {id} is not queued"
                    )))
                });
            }
            Command::Answer(request, answer, reply) => {
                let result = self.handle_answer(request, answer).await;
                // Snapshot before the reply, as for `prompt`.
                self.sync_status().await;
                let _ = reply.send(result);
            }
            Command::Configure(id, value, reply) => {
                let _ = reply.send(self.handle_configure(id, value).await);
            }
            Command::Rollback(turns, scope, reply) => {
                let _ = reply.send(self.handle_rollback(turns, scope).await);
            }
            Command::Cancel { clear_queue, reply } => {
                let _ = reply.send(self.handle_cancel(clear_queue).await);
            }
            Command::Close(reply) => self.shutdown(Some(reply)).await,
        }
    }

    /// Idle → start; running with steering → steer; otherwise queue.
    async fn handle_prompt(&mut self, input: Input, reply: Reply<Delivery>) {
        let prompt_id = PromptId::new(format!("p{}", self.next_prompt));
        self.next_prompt += 1;
        match &self.state {
            TurnState::Closing => {
                let _ = reply.send(Err(AgentError::SessionClosed));
            }
            TurnState::Idle => {
                let turn_id = self.start_turn(prompt_id.clone(), input).await;
                // Snapshot first, so `status()` right after `prompt()`
                // returns already reads Working.
                self.sync_status().await;
                let _ = reply.send(Ok(Delivery {
                    prompt_id,
                    kind: DeliveryKind::Started { turn_id },
                }));
            }
            TurnState::Running { .. } if self.steer_supported && self.steer.is_none() => {
                self.steer = Some((prompt_id, input.clone(), reply));
                if self.forward(DriverCommand::Steer { input }).await.is_err() {
                    self.resolve_steer(false).await;
                }
            }
            TurnState::Running { .. } => {
                self.queue.push_back((prompt_id.clone(), input));
                let _ = reply.send(Ok(Delivery {
                    prompt_id,
                    kind: DeliveryKind::Queued {
                        position: self.queue.len() as u32 - 1,
                    },
                }));
            }
        }
    }

    /// Validates the answer against the stored request shape, then forwards.
    /// A rejected answer leaves the request open.
    async fn handle_answer(
        &mut self,
        request: RequestId,
        answer: Answer,
    ) -> Result<(), AgentError> {
        let shape = match &self.state {
            TurnState::Running { open_requests, .. } => open_requests.get(&request),
            _ => None,
        };
        let Some(shape) = shape else {
            return Err(AgentError::InvalidRequest(format!(
                "request {request} is not open"
            )));
        };
        shape.accepts(&answer)?;
        if let TurnState::Running { open_requests, .. } = &mut self.state {
            open_requests.remove(&request);
        }
        self.forward(DriverCommand::Answer {
            request: request.clone(),
            answer,
        })
        .await?;
        self.emit(
            EventKind::RequestClosed {
                request_id: request,
            },
            true,
        )
        .await;
        self.arm_deadline();
        Ok(())
    }

    /// Validates a selection against the advertised options, then forwards.
    /// The applied change comes back as `SessionUpdated`.
    async fn handle_configure(
        &mut self,
        id: ConfigId,
        value: ConfigValue,
    ) -> Result<(), AgentError> {
        let details = self
            .info
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .details
            .clone();
        let Some(option) = details.config_options.iter().find(|o| o.id == id) else {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{id}` is not an advertised option"
            )));
        };
        if !option.live {
            return Err(AgentError::InvalidConfiguration(format!(
                "`{id}` is creation-only; set it through SessionOptions"
            )));
        }
        match (&option.kind, &value) {
            (ConfigKind::Select { choices }, ConfigValue::Text(chosen)) => {
                if !choices.iter().any(|c| &c.value == chosen) {
                    return Err(AgentError::InvalidConfiguration(format!(
                        "`{chosen}` is not a choice for `{id}`"
                    )));
                }
            }
            (ConfigKind::Boolean, ConfigValue::Bool(_)) => {}
            _ => {
                return Err(AgentError::InvalidConfiguration(format!(
                    "wrong value type for `{id}`"
                )));
            }
        }
        self.forward(DriverCommand::Configure(id, value)).await
    }

    async fn handle_rollback(
        &mut self,
        turns: NonZeroU32,
        scope: RollbackScope,
    ) -> Result<(), AgentError> {
        let capabilities = self.info().details.capabilities;
        if !capabilities.supports(Capability::Rollback) {
            return Err(AgentError::UnsupportedFeature("rollback".into()));
        }
        if scope == RollbackScope::ConversationAndFiles
            && !capabilities.supports(Capability::RollbackFiles)
        {
            return Err(AgentError::UnsupportedFeature("file rollback".into()));
        }
        if matches!(self.state, TurnState::Running { .. }) {
            return Err(AgentError::SessionBusy);
        }
        self.forward(DriverCommand::Rollback(turns, scope)).await
    }

    /// Cancels the active turn; open requests close first. Idempotent.
    async fn handle_cancel(&mut self, clear_queue: bool) -> Result<(), AgentError> {
        if clear_queue {
            self.queue.clear();
        }
        let TurnState::Running { open_requests, .. } = &mut self.state else {
            return Ok(());
        };
        for (request, _) in std::mem::take(open_requests) {
            self.emit(
                EventKind::RequestClosed {
                    request_id: request,
                },
                true,
            )
            .await;
        }
        let result = self.forward(DriverCommand::Cancel).await;
        self.arm_deadline();
        result
    }

    /// Fails a pending steer, ends any running turn as Cancelled, then closes.
    /// `close` and dropped handles come through here.
    async fn shutdown(&mut self, reply: Option<Reply<()>>) {
        if let Some((_, _, reply)) = self.steer.take() {
            let _ = reply.send(Err(AgentError::SessionClosed));
        }
        self.end_turn(StopReason::Cancelled).await;
        self.begin_close(reply).await;
    }

    /// Tells the driver to close once and remembers who is waiting for it.
    async fn begin_close(&mut self, reply: Option<Reply<()>>) {
        let first = !matches!(self.state, TurnState::Closing);
        self.state = TurnState::Closing;
        self.closing.get_or_insert_with(Vec::new).extend(reply);
        if first {
            let _ = self.driver.send(DriverCommand::Close).await;
            self.deadline = Some(Instant::now() + CLOSE_GRACE);
        }
    }

    // -- driver events --------------------------------------------------------

    async fn handle_driver_event(&mut self, ev: DriverEvent) {
        // Turn content delivered between our `StartTurn` and the adapter's
        // ack belongs to a turn the engine already ended; attributing it to
        // the new turn would corrupt it, so it is dropped. Session-level
        // bookkeeping (usage receipts, diagnostics) still passes.
        if self.awaiting_ack {
            match &ev {
                DriverEvent::Event { kind, .. } if is_content(kind) => return,
                DriverEvent::TurnEnded(_) => return,
                _ => {}
            }
        }
        match ev {
            DriverEvent::TurnAck => self.awaiting_ack = false,
            DriverEvent::Steered(accepted) => self.resolve_steer(accepted).await,
            DriverEvent::TurnEnded(stop) => self.handle_turn_ended(stop).await,
            DriverEvent::InfoChanged(info) => self.handle_info_changed(info).await,
            DriverEvent::Exited { status, stderr } => self.exit = Some((status, stderr)),
            DriverEvent::AuthLost { login } => self.handle_auth_lost(login).await,
            DriverEvent::Event {
                kind,
                parent_tool_id,
                extensions,
            } => self.handle_content(kind, parent_tool_id, extensions).await,
        }
    }

    /// Routes a normalized event: into the running turn, into a new
    /// agent-originated turn, or as session-level bookkeeping.
    async fn handle_content(
        &mut self,
        kind: EventKind,
        parent_tool_id: Option<ToolId>,
        extensions: Extensions,
    ) {
        if is_engine_owned(&kind) {
            self.diagnostic(format!("adapter sent engine-owned event {kind:?}; dropped"))
                .await;
            return;
        }
        // AutoApprove answers permissions itself; the caller never sees them.
        if self.auto_approve
            && let EventKind::RequestOpened(Request::Permission(request)) = &kind
        {
            let _ = self
                .forward(DriverCommand::Answer {
                    request: request.id.clone(),
                    answer: Answer::Permission(PermissionChoice::AllowOnce),
                })
                .await;
            return;
        }
        if matches!(self.state, TurnState::Idle) && is_content(&kind) {
            self.enter_running(TurnOrigin::Agent).await;
        }
        let turn = match &mut self.state {
            TurnState::Running {
                turn,
                open_requests,
                running_tools,
                open_messages,
                ..
            } => {
                track(&kind, open_requests, running_tools, open_messages);
                Some(TurnContext {
                    id: turn.clone(),
                    parent_tool_id,
                })
            }
            _ => None,
        };
        self.push(turn, kind, extensions).await;
        self.arm_deadline();
    }

    async fn handle_turn_ended(&mut self, stop: StopReason) {
        if matches!(self.state, TurnState::Running { .. }) {
            self.end_turn(stop).await;
        } else if matches!(self.state, TurnState::Closing) {
            // Expected while closing: the wire's own turn end trails ours.
        } else if !self.noise_reported {
            self.noise_reported = true;
            self.diagnostic("late turn end from the agent after the turn already ended; ignored")
                .await;
        }
    }

    async fn handle_info_changed(&mut self, info: DriverInfo) {
        let updated = {
            let mut current = self.info.lock().unwrap_or_else(|e| e.into_inner());
            current.details = info.details;
            current.configuration = info.configuration;
            current.resume_token = info.resume_token;
            current.title = info.title;
            current.clone()
        };
        self.steer_supported = updated.details.capabilities.supports(Capability::Steer);
        self.emit(EventKind::SessionUpdated(updated), false).await;
    }

    /// The steer was accepted, or it was not and the prompt goes to the head
    /// of the queue. On a dead session the caller gets an error instead.
    async fn resolve_steer(&mut self, accepted: bool) {
        let Some((prompt_id, input, reply)) = self.steer.take() else {
            return;
        };
        if self.done {
            let _ = reply.send(Err(AgentError::SessionClosed));
            return;
        }
        let kind = match (&self.state, accepted) {
            (TurnState::Running { turn, .. }, true) => DeliveryKind::Steered {
                turn_id: turn.clone(),
            },
            _ => {
                self.queue.push_front((prompt_id.clone(), input));
                DeliveryKind::Queued { position: 0 }
            }
        };
        let _ = reply.send(Ok(Delivery { prompt_id, kind }));
    }

    /// Auth died mid-session: the turn fails, the stream reports
    /// `AuthRequired`, and the session closes. The application reopens
    /// after the user logs in.
    async fn handle_auth_lost(&mut self, login: Vec<LoginMethod>) {
        self.end_turn(StopReason::Failed {
            message: "agent needs login".into(),
        })
        .await;
        if self.events_alive
            && self
                .events
                .try_send(Err(AgentError::AuthRequired { login }))
                .is_err()
        {
            self.events_alive = false;
        }
        self.shutdown(None).await;
    }

    /// The driver's event stream ended: a clean close, or the agent died.
    async fn driver_gone(&mut self) {
        self.done = true;
        if let Some(waiters) = self.closing.take() {
            for waiter in waiters {
                let _ = waiter.send(Ok(()));
            }
            return;
        }
        self.end_turn(StopReason::Failed {
            message: "agent exited during the turn".into(),
        })
        .await;
        self.state = TurnState::Closing;
        if self.events_alive {
            let (status, stderr) = self
                .exit
                .take()
                .unwrap_or_else(|| ("unknown".into(), String::new()));
            if self
                .events
                .try_send(Err(AgentError::ProcessExited { status, stderr }))
                .is_err()
            {
                self.events_alive = false;
            }
        }
    }

    /// The quiet window elapsed, or the close grace expired.
    async fn on_quiet(&mut self) {
        self.deadline = None;
        match &self.state {
            TurnState::Closing => {
                for waiter in self.closing.take().into_iter().flatten() {
                    let _ = waiter.send(Ok(()));
                }
                self.done = true;
            }
            TurnState::Running {
                open_requests,
                running_tools,
                ..
            } if open_requests.is_empty() && running_tools.is_empty() => {
                self.end_turn(StopReason::Completed {
                    source: CompletionSource::Inferred,
                })
                .await;
            }
            _ => {}
        }
    }

    // -- turn lifecycle -------------------------------------------------------

    /// Starts the next queued prompt when the session is idle.
    async fn promote(&mut self) {
        if !matches!(self.state, TurnState::Idle) {
            return;
        }
        if let Some((prompt_id, input)) = self.queue.pop_front() {
            self.start_turn(prompt_id, input).await;
        }
    }

    /// Enters Running for a prompt and hands the input to the driver. A dead
    /// driver is reported by `driver_gone`, which ends the turn as Failed.
    async fn start_turn(&mut self, prompt_id: PromptId, input: Input) -> TurnId {
        let turn = self.enter_running(TurnOrigin::Prompt(prompt_id)).await;
        self.awaiting_ack = true;
        let _ = self.forward(DriverCommand::StartTurn { input }).await;
        turn
    }

    /// Mints a turn id, flips to Running, emits `TurnStarted`, arms the watchdog.
    async fn enter_running(&mut self, origin: TurnOrigin) -> TurnId {
        let turn = TurnId::new(format!("t{}", self.next_turn));
        self.next_turn += 1;
        self.state = TurnState::Running {
            turn: turn.clone(),
            origin: origin.clone(),
            open_requests: BTreeMap::new(),
            running_tools: BTreeSet::new(),
            open_messages: BTreeSet::new(),
        };
        self.emit(EventKind::TurnStarted { origin }, true).await;
        self.arm_deadline();
        turn
    }

    /// Closes open requests, emits `TurnEnded`, returns to Idle. The main
    /// loop promotes the next queued prompt afterwards.
    async fn end_turn(&mut self, stop: StopReason) {
        if !matches!(self.state, TurnState::Running { .. }) {
            return;
        }
        let TurnState::Running {
            turn,
            open_requests,
            running_tools,
            open_messages,
            ..
        } = std::mem::replace(&mut self.state, TurnState::Idle)
        else {
            return;
        };
        let ctx = |parent_tool_id| {
            Some(TurnContext {
                id: turn.clone(),
                parent_tool_id,
            })
        };
        for message_id in open_messages {
            self.push(
                ctx(None),
                EventKind::MessageEnded { message_id },
                Extensions::new(),
            )
            .await;
        }
        for (request, _) in open_requests {
            self.push(
                ctx(None),
                EventKind::RequestClosed {
                    request_id: request,
                },
                Extensions::new(),
            )
            .await;
        }
        let background = running_tools.into_iter().collect();
        self.push(
            ctx(None),
            EventKind::TurnEnded { stop, background },
            Extensions::new(),
        )
        .await;
        self.deadline = None;
        self.noise_reported = false;
        self.resolve_steer(false).await;
    }

    /// Arms inferred completion when the wire needs it and nothing is pending.
    fn arm_deadline(&mut self) {
        if let TurnState::Running {
            origin,
            open_requests,
            running_tools,
            ..
        } = &self.state
        {
            let window = match origin {
                TurnOrigin::Prompt(_) => self.quiet_user,
                TurnOrigin::Agent => self.quiet_agent,
            };
            let idle = open_requests.is_empty() && running_tools.is_empty();
            self.deadline = window.filter(|_| idle).map(|w| Instant::now() + w);
        }
    }

    // -- helpers ----------------------------------------------------------------

    fn info(&self) -> SessionInfo {
        self.info.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    async fn forward(&self, cmd: DriverCommand) -> Result<(), AgentError> {
        self.driver
            .send(cmd)
            .await
            .map_err(|_| AgentError::SessionClosed)
    }

    async fn diagnostic(&mut self, message: impl Into<String>) {
        let kind = EventKind::Diagnostic(Diagnostic {
            level: DiagnosticLevel::Warning,
            message: message.into(),
        });
        self.emit(kind, false).await;
    }

    /// Emits an engine-made event, attached to the running turn when asked.
    async fn emit(&mut self, kind: EventKind, in_turn: bool) {
        let turn = match (&self.state, in_turn) {
            (TurnState::Running { turn, .. }, true) => Some(TurnContext {
                id: turn.clone(),
                parent_tool_id: None,
            }),
            _ => None,
        };
        self.push(turn, kind, Extensions::new()).await;
    }

    /// Assigns the sequence number and delivers. `try_send` never parks the
    /// engine, so `cancel` stays responsive; a dropped `Events` — or one so
    /// stalled the buffer filled, which bounds memory — stops delivery and
    /// starts shutdown.
    async fn push(&mut self, turn: Option<TurnContext>, kind: EventKind, extensions: Extensions) {
        if !self.events_alive {
            return;
        }
        self.seq += 1;
        let event = Event {
            sequence: self.seq,
            occurred_at: std::time::SystemTime::now(),
            session_id: self.id.clone(),
            turn_info: turn,
            kind,
            extensions,
        };
        if self.events.try_send(Ok(event)).is_err() {
            self.events_alive = false;
            self.begin_close(None).await;
        }
    }
}

/// Content opens an agent-originated turn when the session is idle;
/// everything else is bookkeeping.
fn is_content(kind: &EventKind) -> bool {
    match kind {
        EventKind::TextDelta { .. }
        | EventKind::ReasoningDelta { .. }
        | EventKind::UserMessage { .. }
        | EventKind::RequestOpened(_) => true,
        EventKind::ToolUpdated(tool) => tool.status.is_active(),
        _ => false,
    }
}

/// Kinds only the engine may emit.
fn is_engine_owned(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::TurnStarted { .. }
            | EventKind::TurnEnded { .. }
            | EventKind::RequestClosed { .. }
            | EventKind::SessionUpdated(_)
            | EventKind::StatusChanged(_)
    )
}

/// Keeps the open-request and running-tool sets current for one event.
fn track(
    kind: &EventKind,
    open_requests: &mut BTreeMap<RequestId, RequestShape>,
    running_tools: &mut BTreeSet<ToolId>,
    open_messages: &mut BTreeSet<MessageId>,
) {
    match kind {
        EventKind::RequestOpened(request) => {
            open_requests.insert(request.id(), RequestShape::of(request));
        }
        EventKind::ToolUpdated(tool) if tool.status.is_active() => {
            running_tools.insert(tool.id.clone());
        }
        EventKind::ToolUpdated(tool) => {
            running_tools.remove(&tool.id);
        }
        EventKind::TextDelta { message_id, .. }
        | EventKind::ReasoningDelta { message_id, .. }
        | EventKind::UserMessage { message_id, .. } => {
            open_messages.insert(message_id.clone());
        }
        EventKind::MessageEnded { message_id } => {
            open_messages.remove(message_id);
        }
        _ => {}
    }
}
