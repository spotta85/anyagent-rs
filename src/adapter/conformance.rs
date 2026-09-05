//! Session-engine contract tests, driven through the public interface with
//! the scripted mock adapter. Every adapter must satisfy these.

use std::time::Duration;

use futures::StreamExt;

use crate::adapter::mock::{MockAdapter, Script, Step, completed, permission, text, tool};
use crate::{
    Answer, CompletionSource, DeliveryKind, Event, EventKind, Events, PermissionChoice, PromptId,
    RequestId, Runtime, Session, SessionOptions, StopReason, ToolId, ToolStatus, TurnOrigin,
};

/// Opens one session on the mock and returns the two handles.
async fn open(adapter: MockAdapter, options: Option<SessionOptions>) -> (Session, Events) {
    let runtime = Runtime::with_test_adapter(adapter);
    let report = runtime.discover().await;
    let agent = report.require("mock").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let options = options.unwrap_or_else(|| SessionOptions::in_dir(dir.path()));
    runtime.open(agent, options).await.unwrap()
}

/// Next event within two seconds, or panic. Skips `StatusChanged` so the
/// sequence assertions stay about content; the dedicated status test reads
/// the raw stream.
async fn next(events: &mut Events) -> Event {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .expect("timed out waiting for an event")
            .expect("stream ended")
            .expect("stream error");
        if !matches!(event.kind, EventKind::StatusChanged(_)) {
            return event;
        }
    }
}

/// A turn that parks on a permission request until answered, then ends.
fn parked_turn() -> Vec<Step> {
    vec![
        Step::Emit(permission("r1")),
        Step::AwaitAnswer,
        Step::End(completed()),
    ]
}

fn allow() -> Answer {
    Answer::Permission(PermissionChoice::AllowOnce)
}

/// Prompt->TurnStarted(Prompt) correlated id -> RequestOpened -> answer -> TurnEnded(Completed) contract.
#[tokio::test]
async fn prompt_request_answer_and_completion_share_one_contract() {
    let (session, mut events) = open(MockAdapter::permission_flow(), None).await;
    let delivery = session.prompt("Fix the test").await.unwrap();
    assert!(matches!(delivery.kind, DeliveryKind::Started { .. }));

    let mut saw_correlated_start = false;
    let mut text_seen = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TurnStarted {
                origin: TurnOrigin::Prompt(prompt_id),
            } => {
                assert_eq!(prompt_id, delivery.prompt_id);
                saw_correlated_start = true;
            }
            EventKind::TextDelta { text, .. } => text_seen.push_str(&text),
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::TurnEnded {
                stop: StopReason::Completed { .. },
                ..
            } => {
                assert!(saw_correlated_start);
                break;
            }
            _ => {}
        }
    }
    assert_eq!(text_seen, "Let me check. Done.");
    session.close().await.unwrap();
    stream_closes(&mut events).await;
}

/// Asserts the stream ends, tolerating only a trailing status flip.
async fn stream_closes(events: &mut Events) {
    loop {
        match events.next().await {
            None => break,
            Some(Ok(event)) if matches!(event.kind, EventKind::StatusChanged(_)) => {}
            other => panic!("stream should close after close(), got {other:?}"),
        }
    }
}

/// Second prompt queued as Queued{0} and promoted FIFO after first ends.
#[tokio::test]
async fn queued_prompt_is_promoted_after_turn_end() {
    let script = Script::default()
        .turn(parked_turn())
        .turn(vec![Step::End(completed())]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;

    let first = session.prompt("one").await.unwrap();
    let second = session.prompt("two").await.unwrap();
    assert_eq!(second.kind, DeliveryKind::Queued { position: 0 });

    let order = drain_turn_starts(&session, &mut events, 2).await;
    assert_eq!(order, vec![first.prompt_id, second.prompt_id]);
}

/// Steer accepted -> Steered; rejected -> requeued at head as Queued{0}.
#[tokio::test]
async fn steer_is_reported_when_accepted_and_requeued_at_head_when_rejected() {
    let script = Script {
        steer: true,
        ..Script::default()
    }
    .turn(parked_turn())
    .turn(vec![Step::End(completed())]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    let first = session.prompt("one").await.unwrap();
    let DeliveryKind::Started { turn_id } = first.kind else {
        panic!("expected Started")
    };
    let steered = session.prompt("two").await.unwrap();
    assert_eq!(steered.kind, DeliveryKind::Steered { turn_id });
    drain_turn_starts(&session, &mut events, 1).await;

    let script = Script {
        steer: true,
        steer_rejects: true,
        ..Script::default()
    }
    .turn(parked_turn())
    .turn(vec![Step::End(completed())]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    let first = session.prompt("one").await.unwrap();
    let rejected = session.prompt("two").await.unwrap();
    assert_eq!(rejected.kind, DeliveryKind::Queued { position: 0 });
    let order = drain_turn_starts(&session, &mut events, 2).await;
    assert_eq!(order, vec![first.prompt_id, rejected.prompt_id]);
}

/// Quiet non-deterministic agent gets inferred completion after quiet_window.
#[tokio::test(start_paused = true)]
async fn quiet_agent_gets_inferred_completion() {
    let script = Script {
        deterministic: false,
        ..Script::default()
    }
    .turn(vec![Step::Emit(text("m1", "working..."))]);
    let options = SessionOptions::in_dir(".").quiet_window(Duration::from_millis(50));
    let (session, mut events) = open(MockAdapter::new(script), Some(options)).await;
    session.prompt("go").await.unwrap();

    let stop = loop {
        if let EventKind::TurnEnded { stop, .. } = next(&mut events).await.kind {
            break stop;
        }
    };
    assert_eq!(
        stop,
        StopReason::Completed {
            source: CompletionSource::Inferred
        }
    );
}

/// Answering a request closes it and re-arms quiet-window inferred completion.
#[tokio::test(start_paused = true)]
async fn answering_a_request_rearms_inferred_completion() {
    let script = Script {
        deterministic: false,
        ..Script::default()
    }
    .turn(vec![Step::Emit(permission("r1")), Step::AwaitAnswer]);
    let options = SessionOptions::in_dir(".").quiet_window(Duration::from_millis(50));
    let (session, mut events) = open(MockAdapter::new(script), Some(options)).await;
    session.prompt("go").await.unwrap();

    let _started = next(&mut events).await;
    let EventKind::RequestOpened(request) = next(&mut events).await.kind else {
        panic!("expected a request")
    };
    session.answer(request.id(), allow()).await.unwrap();

    // The agent says nothing after the answer, so the quiet window has to
    // restart when the request closes.
    let kinds = collect(&mut events, 2).await;
    assert!(matches!(kinds[0], EventKind::RequestClosed { .. }));
    assert!(matches!(
        kinds[1],
        EventKind::TurnEnded {
            stop: StopReason::Completed {
                source: CompletionSource::Inferred
            },
            ..
        }
    ));
}

/// Late content after TurnEnded opens agent-originated turn with MessageEnded.
#[tokio::test]
async fn content_after_turn_end_opens_an_agent_turn() {
    let script = Script::default().turn(vec![
        Step::End(completed()),
        Step::Emit(text("m2", "by the way")),
        Step::End(completed()),
    ]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("go").await.unwrap();

    let kinds = collect(&mut events, 6).await;
    assert!(matches!(
        kinds[0],
        EventKind::TurnStarted {
            origin: TurnOrigin::Prompt(_)
        }
    ));
    assert!(matches!(kinds[1], EventKind::TurnEnded { .. }));
    assert!(matches!(
        kinds[2],
        EventKind::TurnStarted {
            origin: TurnOrigin::Agent
        }
    ));
    assert!(matches!(kinds[3], EventKind::TextDelta { .. }));
    // The mock wire has no end-of-message signal; the engine closes it.
    assert!(matches!(kinds[4], EventKind::MessageEnded { .. }));
    assert!(matches!(kinds[5], EventKind::TurnEnded { .. }));
}

/// Trailing agent content after TurnEnded takes priority over queued prompt.
#[tokio::test]
async fn trailing_content_beats_a_queued_prompt_to_the_next_turn() {
    let script = Script::default().turn(vec![
        Step::Emit(permission("r1")),
        Step::AwaitAnswer,
        Step::End(completed()),
        Step::Emit(text("m2", "one more thing")),
        Step::End(completed()),
    ]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("go").await.unwrap();

    let _started = next(&mut events).await;
    let EventKind::RequestOpened(request) = next(&mut events).await.kind else {
        panic!("expected a request")
    };
    let queued = session.prompt("then this").await.unwrap();
    assert_eq!(queued.kind, DeliveryKind::Queued { position: 0 });
    session.answer(request.id(), allow()).await.unwrap();

    // The agent keeps talking after its stop frame. That content is its own
    // turn; the queued prompt only starts once the agent is really idle.
    let kinds = collect(&mut events, 6).await;
    assert!(matches!(kinds[0], EventKind::RequestClosed { .. }));
    assert!(matches!(kinds[1], EventKind::TurnEnded { .. }));
    assert!(
        matches!(
            kinds[2],
            EventKind::TurnStarted {
                origin: TurnOrigin::Agent
            }
        ),
        "expected an agent turn, got {:?}",
        kinds[2]
    );
    assert!(matches!(kinds[3], EventKind::TextDelta { .. }));
    assert!(matches!(kinds[4], EventKind::MessageEnded { .. }));
    assert!(matches!(kinds[5], EventKind::TurnEnded { .. }));

    let promoted = next(&mut events).await;
    assert!(matches!(
        promoted.kind,
        EventKind::TurnStarted {
            origin: TurnOrigin::Prompt(ref id)
        } if *id == queued.prompt_id
    ));
}

/// Background bookkeeping after end carries no turn; extra stop becomes Diagnostic.
#[tokio::test]
async fn bookkeeping_after_turn_end_is_not_a_turn_and_late_stops_are_diagnostics() {
    let script = Script::default().turn(vec![
        Step::Emit(tool("bg", ToolStatus::Running)),
        Step::End(completed()),
        Step::Emit(tool("bg", ToolStatus::Completed)),
        Step::End(completed()),
    ]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("go").await.unwrap();

    let _started = next(&mut events).await;
    let _tool = next(&mut events).await;
    let ended = next(&mut events).await;
    assert!(matches!(
        &ended.kind,
        EventKind::TurnEnded { background, .. } if *background == vec![ToolId::new("bg")]
    ));
    let late_tool = next(&mut events).await;
    assert!(matches!(late_tool.kind, EventKind::ToolUpdated(_)));
    assert!(late_tool.turn_info.is_none(), "bookkeeping carries no turn");
    let late_stop = next(&mut events).await;
    assert!(matches!(late_stop.kind, EventKind::Diagnostic(_)));
}

/// Slow consumer with 1000 deltas (<1024) loses nothing and preserves sequence order.
#[tokio::test]
async fn status_flips_working_needs_input_and_back_and_never_flashes_idle() {
    use crate::SessionStatus;

    // A permission turn: Working -> NeedsInput -> Working -> Idle, pushed
    // only on change, and readable from the handle without the stream.
    let (session, mut events) = open(MockAdapter::permission_flow(), None).await;
    assert_eq!(session.status(), SessionStatus::Idle);
    session.prompt("go").await.unwrap();

    let mut statuses = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        match event.kind {
            EventKind::StatusChanged(status) => {
                statuses.push(status);
                if status == SessionStatus::Idle {
                    break;
                }
            }
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            _ => {}
        }
    }
    assert_eq!(
        statuses,
        vec![
            SessionStatus::Working,
            SessionStatus::NeedsInput,
            SessionStatus::Working,
            SessionStatus::Idle,
        ]
    );
    assert_eq!(session.status(), SessionStatus::Idle);

    // A turn ending with another prompt queued stays Working throughout:
    // promotion happens before the status check, so Idle never flashes.
    let script = Script::default()
        .turn(vec![Step::Emit(text("m1", "one")), Step::End(completed())])
        .turn(vec![Step::Emit(text("m2", "two")), Step::End(completed())]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("first").await.unwrap();
    session.prompt("second").await.unwrap();

    let mut statuses = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        if let EventKind::StatusChanged(status) = event.kind {
            statuses.push(status);
            if status == SessionStatus::Idle {
                break;
            }
        }
    }
    assert_eq!(statuses, vec![SessionStatus::Working, SessionStatus::Idle]);
}

#[tokio::test]
async fn a_close_with_a_queued_prompt_reads_idle_not_working() {
    use crate::SessionStatus;

    // Close mid-turn with a prompt still queued: the queue can never promote
    // past Closing, so the final snapshot must not stay Working.
    let script = Script::default().turn(vec![Step::Emit(text("m1", "one"))]); // never ends
    let (session, _events) = open(MockAdapter::new(script), None).await;
    session.prompt("first").await.unwrap();
    session.prompt("queued").await.unwrap();
    session.close().await.unwrap();
    assert_eq!(session.status(), SessionStatus::Idle);
}

#[tokio::test]
async fn trailing_events_do_not_flash_idle_past_a_queued_prompt() {
    use crate::SessionStatus;

    // The turn ends with bookkeeping right behind it in the channel, so
    // promotion of the queued prompt is deferred one pass; the status must
    // read Working across that gap, not flash Idle.
    let script = Script::default()
        .turn(vec![
            Step::Emit(tool("bg", ToolStatus::Running)),
            Step::Emit(permission("r1")),
            Step::AwaitAnswer,
            Step::End(completed()),
            Step::Emit(tool("bg", ToolStatus::Completed)),
        ])
        .turn(vec![Step::Emit(text("m2", "two")), Step::End(completed())]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("first").await.unwrap();

    let mut statuses = Vec::new();
    let mut queued = false;
    let mut turn_ends = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .expect("timed out")
            .expect("stream ended")
            .expect("stream error");
        match event.kind {
            EventKind::StatusChanged(status) => {
                statuses.push(status);
                if status == SessionStatus::Idle {
                    break;
                }
            }
            // Queue the second prompt while the turn is parked, then let it
            // finish into its trailing bookkeeping.
            EventKind::RequestOpened(request) => {
                session.prompt("second").await.unwrap();
                queued = true;
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => turn_ends += 1,
            _ => {}
        }
    }
    assert!(queued, "the second prompt never queued");
    // The one Idle comes after BOTH turns; an Idle between them is the flash.
    assert_eq!(turn_ends, 2, "Idle flashed before the queued turn ran");
    assert_eq!(
        statuses,
        vec![
            SessionStatus::Working,
            SessionStatus::NeedsInput,
            SessionStatus::Working,
            SessionStatus::Idle,
        ]
    );
}

/// Stalled consumer (>1024 buffered events) is disconnected rather than growing memory unbounded.
#[tokio::test]
async fn a_stalled_consumer_is_disconnected_instead_of_growing_memory() {
    // More events than the consumer buffer (1024) holds, never drained.
    let mut steps: Vec<Step> = (0..1300)
        .map(|i| Step::Emit(text("m1", &format!("{i} "))))
        .collect();
    steps.push(Step::End(completed()));
    let (session, mut events) = open(MockAdapter::new(Script::default().turn(steps)), None).await;
    session.prompt("go").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The engine treats a consumer this far behind as gone: the stream
    // delivers what the buffer held, then ends — it never grows unbounded.
    let mut delivered = 0;
    while tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("the stream should end after the disconnect")
        .is_some()
    {
        delivered += 1;
    }
    assert!(
        delivered <= 1024,
        "buffer overflowed: {delivered} delivered"
    );
}

/// Frames losing promotion race (stale_before_ack) are dropped, not reattributed to next turn.
#[tokio::test]
async fn frames_losing_the_promotion_race_are_dropped_not_reattributed() {
    // A frame from an ended turn that arrives after the engine promoted the
    // next prompt used to land in the new turn. Adapters now ack `StartTurn`;
    // the engine drops anything delivered before the ack.
    let script = Script {
        stale_before_ack: Some(text("m-old", "stale tail")),
        ..Script::default()
    }
    .turn(vec![
        Step::Emit(text("m1", "first")),
        Step::End(completed()),
    ])
    .turn(vec![
        Step::Emit(text("m2", "second")),
        Step::End(completed()),
    ]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("go").await.unwrap();
    session.prompt("then").await.unwrap();

    let mut texts = Vec::new();
    let mut ended = 0;
    while ended < 2 {
        match next(&mut events).await.kind {
            EventKind::TextDelta { text, .. } => texts.push(text),
            EventKind::TurnEnded { .. } => ended += 1,
            _ => {}
        }
    }
    assert_eq!(texts, vec!["first", "second"], "stale frames must not land");
}

#[tokio::test]
async fn slow_consumer_does_not_lose_events() {
    let mut steps: Vec<Step> = (0..1000)
        .map(|i| Step::Emit(text("m1", &format!("{i} "))))
        .collect();
    steps.push(Step::End(completed()));
    let adapter = MockAdapter::new(
        Script {
            buffer: 8,
            ..Script::default()
        }
        .turn(steps),
    );
    let (session, mut events) = open(adapter, None).await;
    session.prompt("go").await.unwrap();

    // Slow consumer: do not drain for a bit, then verify nothing was lost
    // and ordering is preserved. 1000 deltas fit in the consumer buffer
    // (1024); the invariant is no loss below the disconnect threshold.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut texts = 0;
    let mut last_seq = 0;
    loop {
        let event = next(&mut events).await;
        assert!(event.sequence > last_seq);
        last_seq = event.sequence;
        match event.kind {
            EventKind::TextDelta { .. } => texts += 1,
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert_eq!(texts, 1000);
}

/// Cancel is not blocked by full bounded event buffer (backpressure fix).
#[tokio::test]
async fn cancel_is_not_blocked_by_full_event_buffer() {
    // Flood the bounded buffer (256) without draining, then cancel.
    // Before the fix the engine parked mid-send and `cancel` would time out.
    let steps: Vec<Step> = (0..500)
        .map(|i| Step::Emit(text("m1", &format!("{i} "))))
        .collect();
    // No End — turn stays open until cancelled.
    let adapter = MockAdapter::new(Script::default().turn(steps));
    let (session, mut events) = open(adapter, None).await;
    session.prompt("go").await.unwrap();

    // Let the engine deliver a large backlog without the consumer draining.
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::time::timeout(Duration::from_secs(1), session.cancel(false))
        .await
        .expect("cancel was blocked by backpressure")
        .expect("cancel failed");

    // Drain until the cancelled turn ends; sequence must stay monotonic.
    let mut last_seq = 0;
    let mut ended = false;
    while let Ok(event) = tokio::time::timeout(Duration::from_secs(2), next(&mut events)).await {
        assert!(event.sequence > last_seq);
        last_seq = event.sequence;
        if matches!(
            event.kind,
            EventKind::TurnEnded {
                stop: StopReason::Cancelled,
                ..
            }
        ) {
            ended = true;
            break;
        }
    }
    assert!(ended, "expected a Cancelled TurnEnded after cancel");
}

/// Cancel(false) closes requests and keeps queue; cancel(true) clears queue.
#[tokio::test]
async fn cancel_closes_requests_and_keeps_or_clears_the_queue() {
    let script = Script::default()
        .turn(parked_turn())
        .turn(vec![Step::End(completed())]);
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("one").await.unwrap();
    let second = session.prompt("two").await.unwrap();
    session.cancel(false).await.unwrap();

    let kinds = collect(&mut events, 5).await;
    assert!(matches!(kinds[1], EventKind::RequestOpened(_)));
    assert!(
        matches!(&kinds[2], EventKind::RequestClosed { request_id } if *request_id == RequestId::new("r1"))
    );
    assert!(matches!(
        kinds[3],
        EventKind::TurnEnded {
            stop: StopReason::Cancelled,
            ..
        }
    ));
    assert!(
        matches!(&kinds[4], EventKind::TurnStarted { origin: TurnOrigin::Prompt(p) } if *p == second.prompt_id)
    );

    let script = Script::default().turn(parked_turn()).turn(parked_turn());
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("one").await.unwrap();
    session.prompt("two").await.unwrap();
    session.cancel(true).await.unwrap();
    let kinds = collect(&mut events, 4).await;
    assert!(matches!(
        kinds[3],
        EventKind::TurnEnded {
            stop: StopReason::Cancelled,
            ..
        }
    ));
    let third = session.prompt("three").await.unwrap();
    assert!(
        matches!(third.kind, DeliveryKind::Started { .. }),
        "queue was cleared"
    );
    assert_eq!(third.prompt_id, PromptId::new("p3"));
}

/// AutoApprove mode answers permissions without emitting RequestOpened/Closed to caller.
#[tokio::test]
async fn auto_approve_answers_permissions_without_the_caller() {
    let dir = tempfile::tempdir().unwrap();
    let options =
        SessionOptions::in_dir(dir.path()).permission_mode(crate::PermissionMode::AutoApprove);
    let (session, mut events) = open(MockAdapter::permission_flow(), Some(options)).await;
    session.prompt("go").await.unwrap();

    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(_) | EventKind::RequestClosed { .. } => {
                panic!("auto-approved requests never reach the caller")
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert_eq!(text, "Let me check. Done.");
}

/// Close during a turn ends it as Cancelled and closes open requests.
#[tokio::test]
async fn close_during_a_turn_ends_it_and_closes_requests() {
    let script = Script::default().turn(parked_turn());
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.prompt("go").await.unwrap();
    let _started = next(&mut events).await;
    let _opened = next(&mut events).await;
    session.close().await.unwrap();

    let kinds = collect(&mut events, 2).await;
    assert!(matches!(&kinds[0], EventKind::RequestClosed { .. }));
    assert!(matches!(
        kinds[1],
        EventKind::TurnEnded {
            stop: StopReason::Cancelled,
            ..
        }
    ));
    stream_closes(&mut events).await;
}

/// Unknown RequestId/double answer/dequeue rejected typed InvalidRequest; dequeued prompt never starts.
#[tokio::test]
async fn unknown_requests_and_prompts_are_rejected() {
    let script = Script::default().turn(parked_turn());
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    assert!(matches!(
        session.answer(RequestId::new("nope"), allow()).await,
        Err(crate::AgentError::InvalidRequest(_))
    ));
    session.prompt("one").await.unwrap();
    let queued = session.prompt("two").await.unwrap();
    session.dequeue(queued.prompt_id.clone()).await.unwrap();
    assert!(matches!(
        session.dequeue(queued.prompt_id).await,
        Err(crate::AgentError::InvalidRequest(_))
    ));
    let _ = next(&mut events).await;
    let request = next(&mut events).await;
    let EventKind::RequestOpened(request) = request.kind else {
        panic!("expected request")
    };
    session.answer(request.id(), allow()).await.unwrap();
    assert!(
        matches!(
            session.answer(request.id(), allow()).await,
            Err(crate::AgentError::InvalidRequest(_))
        ),
        "a request is answered once"
    );
    let kinds = collect(&mut events, 2).await;
    assert!(matches!(kinds[1], EventKind::TurnEnded { .. }));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), next(&mut events))
            .await
            .is_err(),
        "the dequeued prompt never starts"
    );
}

/// Compaction reaches the agent only when it is advertised and the session
/// is idle; `ContextCompacted` is the confirmation.
#[tokio::test]
async fn compact_runs_on_an_idle_session_and_reports_the_compaction() {
    let script = Script {
        compact: true,
        ..Script::default()
    };
    let (session, mut events) = open(MockAdapter::new(script), None).await;
    session.compact().await.unwrap();
    // Compaction occupies the agent, so it is a turn of the agent's own.
    assert!(matches!(
        next(&mut events).await.kind,
        EventKind::TurnStarted {
            origin: TurnOrigin::Agent
        }
    ));
    assert!(matches!(
        next(&mut events).await.kind,
        EventKind::ContextCompacted
    ));
    assert!(matches!(
        next(&mut events).await.kind,
        EventKind::TurnEnded { .. }
    ));
}

/// Pulls events until `count` turns have started, answering any request;
/// returns the prompt ids of those turns in order.
async fn drain_turn_starts(session: &Session, events: &mut Events, count: usize) -> Vec<PromptId> {
    let mut starts = Vec::new();
    while starts.len() < count {
        match next(events).await.kind {
            EventKind::TurnStarted {
                origin: TurnOrigin::Prompt(id),
            } => starts.push(id),
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            _ => {}
        }
    }
    starts
}

async fn collect(events: &mut Events, count: usize) -> Vec<EventKind> {
    let mut kinds = Vec::new();
    while kinds.len() < count {
        kinds.push(next(events).await.kind);
    }
    kinds
}
