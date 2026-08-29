//! In-process scripted adapter. Plays a script of steps per turn so the
//! engine and the public interface can be tested without a subprocess.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::adapter::{
    Adapter, ConnectRequest, DriverCommand, DriverConnection, DriverEvent, DriverInfo,
};
use crate::agent::{
    AgentDetails, AuthKind, AuthStatus, Capabilities, Capability, SessionConfiguration,
};
use crate::error::AgentError;
use crate::event::{
    CompletionSource, EventKind, MessageId, PermissionChoice, PermissionRequest, Request,
    RequestId, StopReason, ToolId, ToolInput, ToolKind, ToolStatus, ToolUpdate,
};

/// One scripted action inside a turn.
#[derive(Debug, Clone)]
// Test scripts favor direct event construction over per-step heap allocation.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Step {
    Emit(EventKind),
    /// Pause until the engine forwards an `Answer`.
    AwaitAnswer,
    /// Report the turn ended. Steps after it play immediately, which is how
    /// a script models agent-originated continuation and trailing noise.
    End(StopReason),
}

/// What the mock agent will do, turn by turn.
#[derive(Debug, Clone)]
pub(crate) struct Script {
    /// Each `StartTurn` pops the next list. An exhausted script hangs.
    pub turns: VecDeque<Vec<Step>>,
    /// Advertise steering.
    pub steer: bool,
    /// Reject every steer (to exercise the requeue-at-head rule).
    pub steer_rejects: bool,
    /// The wire ends prompted turns itself.
    pub deterministic: bool,
    /// Same for agent-originated turns.
    pub deterministic_agent: bool,
    /// Driver event channel capacity; small values exercise backpressure.
    pub buffer: usize,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            turns: VecDeque::new(),
            steer: false,
            steer_rejects: false,
            deterministic: true,
            deterministic_agent: true,
            buffer: 64,
        }
    }
}

impl Script {
    pub(crate) fn turn(mut self, steps: Vec<Step>) -> Self {
        self.turns.push_back(steps);
        self
    }
}

pub(crate) struct MockAdapter {
    script: Script,
    #[allow(dead_code)]
    /// Driver events delivered so far, for backpressure assertions.
    sent: Arc<AtomicUsize>,
}

impl MockAdapter {
    pub(crate) fn new(script: Script) -> Self {
        Self {
            script,
            sent: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// One turn: text, a permission request, more text after the answer, done.
    pub(crate) fn permission_flow() -> Self {
        Self::new(Script::default().turn(vec![
            Step::Emit(text("m1", "Let me check. ")),
            Step::Emit(permission("r1")),
            Step::AwaitAnswer,
            Step::Emit(text("m1", "Done.")),
            Step::End(completed()),
        ]))
    }

    #[allow(dead_code)]
    pub(crate) fn sent(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.sent)
    }
}

#[async_trait]
impl Adapter for MockAdapter {
    async fn connect(&self, _request: ConnectRequest) -> Result<DriverConnection, AgentError> {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (ev_tx, ev_rx) = mpsc::channel(self.script.buffer);
        tokio::spawn(drive(
            self.script.clone(),
            cmd_rx,
            ev_tx,
            Arc::clone(&self.sent),
        ));
        Ok(DriverConnection {
            info: info(&self.script),
            commands: cmd_tx,
            events: ev_rx,
        })
    }
}

/// Plays steps until the script waits for an answer or runs out, then
/// services the next engine command.
async fn drive(
    mut script: Script,
    mut commands: mpsc::Receiver<DriverCommand>,
    events: mpsc::Sender<DriverEvent>,
    sent: Arc<AtomicUsize>,
) {
    let mut steps: VecDeque<Step> = VecDeque::new();
    let mut waiting = false;
    let mut turn_open = false;
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
                steps = script.turns.pop_front().unwrap_or_default().into();
                turn_open = true;
            }
            DriverCommand::Steer { .. } => {
                if !send(DriverEvent::Steered(script.steer && !script.steer_rejects)).await {
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
            DriverCommand::Cancel | DriverCommand::Configure(..) | DriverCommand::Rollback(..) => {}
            DriverCommand::Close => return,
        }
    }
}

fn info(script: &Script) -> DriverInfo {
    let mut caps = vec![Capability::Permissions, Capability::Questions];
    if script.steer {
        caps.push(Capability::Steer);
    }
    DriverInfo {
        details: AgentDetails {
            version: Some("mock".into()),
            auth: AuthStatus::Authenticated {
                kind: AuthKind::ApiKey,
                account: None,
            },
            capabilities: Capabilities::new(caps),
            config_options: Vec::new(),
            commands: Vec::new(),
        },
        configuration: SessionConfiguration::default(),
        resume_token: None,
        title: None,
        deterministic_turn_end: script.deterministic,
        deterministic_agent_turn_end: script.deterministic_agent,
    }
}

// Event builders shared with the conformance tests.

pub(crate) fn text(message: &str, text: &str) -> EventKind {
    EventKind::TextDelta {
        message_id: MessageId::new(message),
        text: text.into(),
    }
}

pub(crate) fn tool(id: &str, status: ToolStatus) -> EventKind {
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

pub(crate) fn permission(id: &str) -> EventKind {
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

pub(crate) fn completed() -> StopReason {
    StopReason::Completed {
        source: CompletionSource::Protocol,
    }
}
