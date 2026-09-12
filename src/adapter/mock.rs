//! In-process scripted adapter. Plays a script of steps per turn so the
//! engine and the public interface can be tested without a subprocess.
//! Public behind the `mock` feature: `Runtime::with_mock(script)` gives an
//! app the real engine over a scripted agent.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
};
use crate::agent::{
    AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, ConfigOption,
    SessionConfiguration,
};
use crate::error::AgentError;
use crate::event::{
    CompletionSource, EventKind, MessageId, PermissionChoice, PermissionRequest, Request,
    RequestId, StopReason, ToolId, ToolInput, ToolKind, ToolStatus, ToolUpdate,
};

// ---------------------------------------------------------------------------
// PUBLIC: scripts the tests and `--mock` write
// ---------------------------------------------------------------------------

/// One scripted action inside a turn.
#[derive(Debug, Clone, Deserialize)]
// Test scripts favor direct event construction over per-step heap allocation.
#[allow(clippy::large_enum_variant)]
pub enum Step {
    Emit(EventKind),
    /// Pause until the engine forwards an `Answer`.
    AwaitAnswer,
    /// Report the turn ended. Steps after it play immediately, which is how
    /// a script models agent-originated continuation and trailing noise.
    End(StopReason),
    /// The agent process dies here: exit status 9, then the stream ends.
    Die,
    /// Wait this many milliseconds; paces a flood so a reader can keep up.
    Sleep(u64),
    /// Play `steps` this many times over.
    Repeat {
        times: u32,
        steps: Vec<Step>,
    },
}

/// What the mock agent will do, turn by turn. Deserializes from JSON with
/// every field optional, so `anyagent serve --mock script.json` can load one.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Script {
    /// Each `StartTurn` pops the next list. An exhausted script hangs.
    pub turns: VecDeque<Vec<Step>>,
    /// Advertise steering.
    pub steer: bool,
    /// Reject every steer (to exercise the requeue-at-head rule).
    pub steer_rejects: bool,
    /// Answer steers at all; `false` leaves them pending forever.
    pub steer_ack: bool,
    /// Keep running after `Close`, like a wedged adapter.
    pub ignore_close: bool,
    /// The wire ends prompted turns itself.
    pub deterministic: bool,
    /// Same for agent-originated turns.
    pub deterministic_agent: bool,
    /// Driver event channel capacity; small values exercise backpressure.
    pub buffer: usize,
    /// Emitted before each `StartTurn`'s ack, modelling a frame from the
    /// previous turn that lost the promotion race.
    pub stale_before_ack: Option<EventKind>,
    /// Advertise compaction; `compact` then reports `ContextCompacted`.
    pub compact: bool,
    pub permissions: bool,
    /// Advertised config options; `configure` sets one and reports it back.
    pub options: Vec<ConfigOption>,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            turns: VecDeque::new(),
            steer: false,
            steer_rejects: false,
            steer_ack: true,
            ignore_close: false,
            deterministic: true,
            deterministic_agent: true,
            buffer: 64,
            stale_before_ack: None,
            compact: false,
            permissions: true,
            options: Vec::new(),
        }
    }
}

impl Script {
    /// Appends one turn's steps.
    pub fn turn(mut self, steps: Vec<Step>) -> Self {
        self.turns.push_back(steps);
        self
    }
}

// ---------------------------------------------------------------------------
// ADAPTER: the scripted agent behind `Runtime::with_mock`
// ---------------------------------------------------------------------------

pub struct MockAdapter {
    script: Script,
    /// Driver events delivered so far, for backpressure assertions.
    sent: Arc<AtomicUsize>,
}

impl MockAdapter {
    /// An adapter that plays `script`.
    pub fn new(script: Script) -> Self {
        Self {
            script,
            sent: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// One turn: text, a permission request, more text after the answer, done.
    pub fn permission_flow() -> Self {
        Self::new(Script::default().turn(vec![
            Step::Emit(text("m1", "Let me check. ")),
            Step::Emit(permission("r1")),
            Step::AwaitAnswer,
            Step::Emit(text("m1", "Done.")),
            Step::End(completed()),
        ]))
    }

    /// Driver events delivered so far, for backpressure assertions.
    pub fn sent(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.sent)
    }
}

#[async_trait]
impl Adapter for MockAdapter {
    async fn connect(&self, _request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, ev_rx) = mpsc::channel(self.script.buffer);
        tokio::spawn(drive(
            self.script.clone(),
            cmd_rx,
            ev_tx,
            Arc::clone(&self.sent),
        ));
        Ok(DriverConnection {
            info: info(&self.script, &initial_configuration(&self.script)),
            commands: cmd_tx,
            events: ev_rx,
        })
    }
}

// ---------------------------------------------------------------------------
// HELPERS: play the script, report what it advertises
// ---------------------------------------------------------------------------

/// Plays steps until the script waits for an answer or runs out, then
/// services the next engine command.
async fn drive(
    mut script: Script,
    mut commands: mpsc::UnboundedReceiver<DriverCommand>,
    events: mpsc::Sender<DriverEvent>,
    sent: Arc<AtomicUsize>,
) {
    let mut steps: VecDeque<Step> = VecDeque::new();
    let mut waiting = false;
    let mut turn_open = false;
    let mut configuration = initial_configuration(&script);
    let send = |ev: DriverEvent| {
        let events = events.clone();
        let sent = Arc::clone(&sent);
        async move {
            let ok = events.send(ev).await.is_ok();
            if ok {
                sent.fetch_add(1, Ordering::SeqCst);
            }
            ok
        }
    };
    loop {
        while !waiting {
            let Some(step) = steps.pop_front() else { break };
            let ok = match step {
                Step::Emit(kind) => send(DriverEvent::event(kind)).await,
                Step::AwaitAnswer => {
                    waiting = true;
                    true
                }
                Step::End(stop) => {
                    turn_open = false;
                    send(DriverEvent::TurnEnded(stop)).await
                }
                Step::Die => {
                    send(DriverEvent::Exited {
                        status: "9".into(),
                        stderr: "mock died".into(),
                    })
                    .await;
                    return;
                }
                Step::Sleep(ms) => {
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                    true
                }
                Step::Repeat {
                    times,
                    steps: block,
                } => {
                    for _ in 0..times {
                        for step in block.iter().rev() {
                            steps.push_front(step.clone());
                        }
                    }
                    true
                }
            };
            if !ok {
                return;
            }
        }
        let Some(cmd) = commands.recv().await else {
            return;
        };
        match cmd {
            DriverCommand::StartTurn { .. } => {
                if let Some(kind) = script.stale_before_ack.clone()
                    && !send(DriverEvent::event(kind)).await
                {
                    return;
                }
                if !send(DriverEvent::TurnAck).await {
                    return;
                }
                steps = script.turns.pop_front().unwrap_or_default().into();
                turn_open = true;
            }
            DriverCommand::Steer { .. } => {
                if script.steer_ack
                    && !send(DriverEvent::Steered(script.steer && !script.steer_rejects)).await
                {
                    return;
                }
            }
            DriverCommand::Answer { .. } => waiting = false,
            DriverCommand::Cancel if turn_open => {
                steps.clear();
                waiting = false;
                turn_open = false;
                if !send(DriverEvent::TurnEnded(StopReason::Cancelled)).await {
                    return;
                }
            }
            DriverCommand::Compact => {
                if !send(DriverEvent::event(EventKind::ContextCompacted)).await
                    || !send(DriverEvent::TurnEnded(StopReason::Completed {
                        source: CompletionSource::Protocol,
                    }))
                    .await
                {
                    return;
                }
            }
            DriverCommand::Configure(id, value) => {
                configuration.options.insert(id, value);
                if !send(DriverEvent::InfoChanged(info(&script, &configuration))).await {
                    return;
                }
            }
            DriverCommand::Cancel | DriverCommand::Rollback(..) => {}
            DriverCommand::Close if script.ignore_close => {}
            DriverCommand::Close => return,
        }
    }
}

fn info(script: &Script, configuration: &SessionConfiguration) -> DriverInfo {
    let mut caps = vec![Capability::Questions];
    if script.permissions {
        caps.push(Capability::Permissions);
    }
    if script.steer {
        caps.push(Capability::Steer);
    }
    if script.compact {
        caps.push(Capability::Compact);
    }
    // Each option's `current` follows the configuration.
    let config_options = script
        .options
        .iter()
        .cloned()
        .map(|mut option| {
            if let Some(value) = configuration.options.get(&option.id) {
                option.current = Some(value.clone());
            }
            option
        })
        .collect();
    DriverInfo {
        details: AgentDetails {
            version: Some("mock".into()),
            auth: AuthStatus::Authenticated {
                kind: AuthKind::ApiKey,
                account: None,
            },
            capabilities: Capabilities::new(caps),
            config_options,
            commands: Vec::new(),
        },
        configuration: configuration.clone(),
        resume_token: None,
        title: None,
        deterministic_turn_end: script.deterministic,
        deterministic_agent_turn_end: script.deterministic_agent,
        tools_disabled: false,
        effort_wire: None,
    }
}

/// Each option's `current` value, as the session starts.
fn initial_configuration(script: &Script) -> SessionConfiguration {
    let options = script
        .options
        .iter()
        .filter_map(|o| Some((o.id.clone(), o.current.clone()?)))
        .collect();
    SessionConfiguration { options }
}

// ---------------------------------------------------------------------------
// EVENT BUILDERS: shared with the conformance tests
// ---------------------------------------------------------------------------

/// A text delta on `message`.
pub fn text(message: &str, text: &str) -> EventKind {
    EventKind::TextDelta {
        message_id: MessageId::new(message),
        text: text.into(),
    }
}

/// A `cargo test` execute tool in the given state.
pub fn tool(id: &str, status: ToolStatus) -> EventKind {
    EventKind::ToolUpdated(ToolUpdate {
        id: ToolId::new(id),
        kind: ToolKind::Execute,
        title: "cargo test".into(),
        status,
        input: ToolInput::Command {
            command: "cargo test".into(),
            cwd: None,
        },
        output: None,
        diffs: Vec::new(),
        locations: Vec::new(),
        raw: None,
    })
}

/// A permission request for the tool above, offering allow-once and deny-once.
pub fn permission(id: &str) -> EventKind {
    let EventKind::ToolUpdated(tool) = tool("tool-1", ToolStatus::Pending) else {
        unreachable!()
    };
    EventKind::RequestOpened(Request::Permission(PermissionRequest {
        id: RequestId::new(id),
        tool,
        options: vec![PermissionChoice::AllowOnce, PermissionChoice::DenyOnce],
        detail: None,
    }))
}

/// A protocol-ended completion.
pub fn completed() -> StopReason {
    StopReason::Completed {
        source: CompletionSource::Protocol,
    }
}
