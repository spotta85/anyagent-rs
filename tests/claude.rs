//! The native Claude adapter driven end to end through the public interface,
//! against the fixture agent (tests/fixtures/claude/fixture.mjs; needs `node`).
//! A wrapper script pins the catalog's `claude` id to the fixture.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthKind, AuthStatus, Capability, ConfigId, ConfigKind,
    ConfigValue, DeliveryKind, Event, EventKind, Events, Input, LoginMethod, McpServer, MessageId,
    PermissionChoice, PlanStatus, QuestionAnswer, Request, RollbackScope, Runtime, Session,
    SessionOptions, StopReason, ToolKind, ToolStatus, TurnOrigin,
};

/// A temp dir holding one inlineable png, one pdf, and nothing else.
fn attachment_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("anyagent-att-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("shot.png"), b"\x89PNG\r\n\x1a\ndata").unwrap();
    std::fs::write(dir.join("report.pdf"), b"%PDF-1.7 data").unwrap();
    dir
}

/// A `claude` stand-in: a script that execs the fixture with scenario flags,
/// ignoring the real launch flags appended after them.
fn wrapper(name: &str, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/claude/fixture.mjs");
    let dir = std::env::temp_dir().join(format!("anyagent-claude-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("claude");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nexec node {} {flags} \"$@\"\n",
            fixture.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

async fn open(name: &str, flags: &str) -> (Session, Events) {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper(name, flags));
    runtime
        .open(&agent, SessionOptions::in_dir(std::env::temp_dir()))
        .await
        .unwrap()
}

async fn next(events: &mut Events) -> Event {
    tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .expect("timed out waiting for an event")
        .expect("stream ended")
        .expect("stream error")
}

fn allow() -> Answer {
    Answer::Permission(PermissionChoice::AllowOnce)
}

/// Drives one default-fixture turn: answers the Write permission, collects
/// text, and returns it at turn end.
async fn complete_turn(session: &Session, events: &mut Events) -> String {
    let mut text = String::new();
    loop {
        match next(events).await.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(Request::Permission(request)) => {
                session.answer(request.id, allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => return text,
            _ => {}
        }
    }
}

/// Opens a session in its own cwd (for tests that assert on files there).
async fn open_in(name: &str, flags: &str, dir: &Path) -> (Session, Events) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper(name, flags));
    runtime
        .open(&agent, SessionOptions::in_dir(dir))
        .await
        .unwrap()
}

/// Runs one `--echo-uuid` turn and returns the echoed user-message uuid.
async fn echoed_turn(session: &Session, events: &mut Events, prompt: &str) -> String {
    session.prompt(prompt).await.unwrap();
    let text = complete_turn(session, events).await;
    text.split("uuid=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn a_full_turn_maps_every_frame_kind() {
    let (session, mut events) = open("full", "").await;
    session.prompt("hi").await.unwrap();

    let mut text = String::new();
    let mut thoughts = String::new();
    let mut tool_states = Vec::new();
    let mut plan = Vec::new();
    let mut usage = None;
    let mut message_ended = false;
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::ReasoningDelta { text: t, .. } => thoughts.push_str(&t),
            EventKind::ToolUpdated(tool) => tool_states.push(tool),
            EventKind::PlanUpdated { entries } => plan = entries,
            EventKind::MessageEnded { .. } => message_ended = true,
            EventKind::ContextUsage {
                used_tokens,
                window_tokens,
                cost_usd,
            } => usage = Some((used_tokens, window_tokens, cost_usd)),
            EventKind::RequestOpened(Request::Permission(request)) => {
                assert_eq!(request.tool.title, "Write a.txt");
                assert_eq!(request.tool.kind, ToolKind::Edit);
                assert_eq!(request.detail.as_deref(), Some("a.txt"));
                assert_eq!(
                    request.options,
                    vec![
                        PermissionChoice::AllowOnce,
                        PermissionChoice::AllowAlways,
                        PermissionChoice::DenyOnce,
                    ]
                );
                session.answer(request.id, allow()).await.unwrap();
            }
            EventKind::TurnEnded { stop, .. } => {
                assert_eq!(
                    stop,
                    StopReason::Completed {
                        source: anyagent::CompletionSource::Protocol
                    }
                );
                break;
            }
            _ => {}
        }
    }
    assert_eq!(text, "Hello perm=allow done");
    assert_eq!(thoughts, "thinking…");
    assert!(message_ended);
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].text, "step 1");
    assert_eq!(plan[0].status, PlanStatus::InProgress);
    assert_eq!(usage, Some((1200, Some(200_000), Some(0.01))));

    // The Write tool: running, then completed with the file diff.
    let write: Vec<_> = tool_states
        .iter()
        .filter(|t| t.title == "Write a.txt")
        .collect();
    assert_eq!(write[0].status, ToolStatus::Running);
    let done = write.last().unwrap();
    assert_eq!(done.status, ToolStatus::Completed);
    assert_eq!(done.diffs[0].new_text, "ALPHA");
    assert_eq!(done.locations, vec![PathBuf::from("a.txt")]);

    // The provider session id arrived as the resume token.
    assert_eq!(session.info().resume_token.unwrap().as_str(), "sess-c1");
    session.close().await.unwrap();
}

#[tokio::test]
async fn rollback_forks_at_the_cut_point_and_renews_the_token() {
    let (session, mut events) = open("rollback", "--echo-uuid").await;
    let mut uuids = Vec::new();
    for prompt in ["one", "two"] {
        uuids.push(echoed_turn(&session, &mut events, prompt).await);
    }
    assert_eq!(session.info().resume_token.unwrap().as_str(), "sess-c1");

    // Dropping the last turn respawns forked at turn one's final assistant
    // message; the token clears until the fork names itself.
    session
        .rollback(
            std::num::NonZeroU32::new(1).unwrap(),
            RollbackScope::Conversation,
        )
        .await
        .unwrap();
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind {
            assert!(info.resume_token.is_none());
            break;
        }
    }

    session.prompt("three").await.unwrap();
    let text = complete_turn(&session, &mut events).await;
    assert!(
        text.contains(&format!("fork=a-{}", uuids[0])),
        "expected the fork at turn one's last assistant frame, got: {text}"
    );
    assert_eq!(session.info().resume_token.unwrap().as_str(), "sess-fork-1");
    session.close().await.unwrap();
}

#[tokio::test]
async fn files_rollback_rewinds_at_the_first_dropped_turn() {
    let dir = std::env::temp_dir().join(format!("anyagent-rwfiles-{}", std::process::id()));
    let (session, mut events) = open_in("rwfiles", "--echo-uuid", &dir).await;
    let mut uuids = Vec::new();
    for prompt in ["one", "two"] {
        uuids.push(echoed_turn(&session, &mut events, prompt).await);
    }

    session
        .rollback(
            std::num::NonZeroU32::new(1).unwrap(),
            RollbackScope::ConversationAndFiles,
        )
        .await
        .unwrap();
    loop {
        if let EventKind::SessionUpdated(_) = next(&mut events).await.kind {
            break;
        }
    }
    // The fixture wrote the rewind target: the first dropped turn's user uuid.
    let rewound = std::fs::read_to_string(dir.join("rewound-at.txt")).unwrap();
    assert_eq!(rewound, uuids[1]);

    // The conversation fork still cuts at the last kept assistant frame.
    session.prompt("three").await.unwrap();
    let text = complete_turn(&session, &mut events).await;
    assert!(
        text.contains(&format!("fork=a-{}", uuids[0])),
        "got: {text}"
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn files_rollback_refusal_leaves_the_session_untouched() {
    let dir = std::env::temp_dir().join(format!("anyagent-rwfail-{}", std::process::id()));
    let (session, mut events) = open_in("rwfail", "--echo-uuid --rewind-fails", &dir).await;
    for prompt in ["one", "two"] {
        echoed_turn(&session, &mut events, prompt).await;
    }

    // The rejection is a diagnostic; nothing is rewound and nothing respawns.
    session
        .rollback(
            std::num::NonZeroU32::new(1).unwrap(),
            RollbackScope::ConversationAndFiles,
        )
        .await
        .unwrap();
    loop {
        if let EventKind::Diagnostic(d) = next(&mut events).await.kind {
            assert!(
                d.message.contains("rollback rejected"),
                "got: {}",
                d.message
            );
            break;
        }
    }
    assert!(!dir.join("rewound-at.txt").exists());
    assert_eq!(session.info().resume_token.unwrap().as_str(), "sess-c1");
    session.prompt("three").await.unwrap();
    let text = complete_turn(&session, &mut events).await;
    assert!(!text.contains("fork="), "unexpected fork: {text}");
    session.close().await.unwrap();
}

#[tokio::test]
async fn fork_from_branches_at_a_message_and_at_the_tip() {
    let (session, mut events) = open("fork", "--echo-uuid").await;
    // Two turns; each MessageEnded carries the fork anchor as an extension.
    let mut anchors = Vec::new();
    for prompt in ["one", "two"] {
        session.prompt(prompt).await.unwrap();
        loop {
            let event = next(&mut events).await;
            match event.kind {
                EventKind::RequestOpened(Request::Permission(request)) => {
                    session.answer(request.id, allow()).await.unwrap();
                }
                EventKind::MessageEnded { .. } => anchors.push(
                    event.extensions["claude/fork_point"]
                        .as_str()
                        .expect("fork anchor on MessageEnded")
                        .to_owned(),
                ),
                EventKind::TurnEnded { .. } => break,
                _ => {}
            }
        }
    }
    assert_eq!(anchors.len(), 2);
    let token = session.info().resume_token.unwrap();
    session.close().await.unwrap();

    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("fork", "--echo-uuid"));

    // Fork at turn one's last message: the fixture echoes the cut id.
    let (forked, mut fork_events) = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir())
                .fork_from(token.clone(), Some(MessageId::new(&anchors[0]))),
        )
        .await
        .unwrap();
    assert!(forked.info().resume_token.is_none(), "no token before init");
    forked.prompt("three").await.unwrap();
    let text = complete_turn(&forked, &mut fork_events).await;
    assert!(
        text.contains(&format!("fork={}", anchors[0])),
        "expected the cut at turn one's message, got: {text}"
    );
    assert_eq!(forked.info().resume_token.unwrap().as_str(), "sess-fork-1");
    forked.close().await.unwrap();

    // Fork at the tip: new provider session, no cut flag.
    let (tip, mut tip_events) = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir()).fork_from(token, None),
        )
        .await
        .unwrap();
    tip.prompt("four").await.unwrap();
    let text = complete_turn(&tip, &mut tip_events).await;
    assert!(!text.contains("fork="), "tip fork must not cut: {text}");
    assert_eq!(tip.info().resume_token.unwrap().as_str(), "sess-fork-1");
    tip.close().await.unwrap();
}

#[tokio::test]
async fn plan_usage_is_refreshed_after_each_turn() {
    let (session, mut events) = open("usage", "").await;
    session.prompt("hi").await.unwrap();
    loop {
        match next(&mut events).await.kind {
            EventKind::RequestOpened(Request::Permission(request)) => {
                session.answer(request.id, allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    // The adapter fetches `get_usage` after the turn and pushes the windows.
    let usage = loop {
        if let EventKind::PlanUsageUpdated(usage) = next(&mut events).await.kind {
            break usage;
        }
    };
    let windows: Vec<_> = usage
        .windows
        .iter()
        .map(|w| (w.label.as_str(), w.used_percent))
        .collect();
    assert_eq!(
        windows,
        vec![("Session", 42), ("Week", 5), ("Week (Fable)", 9)]
    );
    // The plan label is the receipt's own `subscription_type`.
    assert_eq!(usage.plan.as_deref(), Some("max"));
    // 2026-08-23T08:59:59.746028+00:00, fraction dropped.
    assert_eq!(
        usage.windows[0].resets_at,
        Some(std::time::UNIX_EPOCH + Duration::from_secs(1_787_475_599))
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn runtime_plan_usage_probes_without_a_session_and_caches() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("usage-probe", ""));
    let usage = runtime.plan_usage(&agent).await.unwrap();
    assert_eq!(usage.windows[0].label, "Session");
    assert_eq!(usage.windows[0].used_percent, 42);
    // Inside the 60s TTL the same fetch comes back, no second spawn.
    let again = runtime.plan_usage(&agent).await.unwrap();
    assert_eq!(again.fetched_at, usage.fetched_at);
}

#[tokio::test]
async fn the_handshake_fills_details() {
    let (session, _events) = open("handshake", "").await;
    let details = session.info().details;
    assert_eq!(details.version.as_deref(), Some("2.1.241"));
    let AuthStatus::Authenticated { account, .. } = &details.auth else {
        panic!("expected Authenticated, got {:?}", details.auth);
    };
    let account = account.as_ref().unwrap();
    assert_eq!(account.email.as_deref(), Some("user@example.com"));
    assert_eq!(account.plan.as_deref(), Some("Claude Max"));
    for capability in [
        Capability::Images,
        Capability::Permissions,
        Capability::Questions,
        Capability::Subagents,
        Capability::ContextUsage,
        Capability::PlanUsage,
        Capability::Resume,
        Capability::Rollback,
        Capability::Fork,
    ] {
        assert!(details.capabilities.supports(capability));
    }
    // The CLI queues mid-turn messages; it cannot steer.
    assert!(!details.capabilities.supports(Capability::Steer));
    assert!(details.commands.iter().any(|c| c.name == "compact"));
    // The model catalog from `initialize` becomes the `model` option.
    let model = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .unwrap();
    assert!(model.live);
    assert_eq!(model.current, Some(ConfigValue::Text("default".into())));
    let ConfigKind::Select { choices } = &model.kind else {
        panic!("expected Select, got {:?}", model.kind);
    };
    let sonnet = choices.iter().find(|c| c.value == "sonnet").unwrap();
    assert_eq!(sonnet.label, "Sonnet");
    assert_eq!(
        sonnet.description.as_deref(),
        Some("Fast for everyday tasks")
    );
    // Effort: creation-only, levels from the current model's catalog entry,
    // no current value until configured.
    let effort = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "effort")
        .unwrap();
    assert!(!effort.live);
    assert_eq!(effort.current, None);
    let ConfigKind::Select { choices } = &effort.kind else {
        panic!("expected Select, got {:?}", effort.kind);
    };
    let levels: Vec<&str> = choices.iter().map(|c| c.value.as_str()).collect();
    assert_eq!(levels, ["low", "medium", "high", "xhigh", "max"]);
    // The adapter mints the session id, so the token exists before any turn.
    assert!(session.info().resume_token.is_some());
    session.close().await.unwrap();
}

#[tokio::test]
async fn probe_reports_the_same_details_as_open() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("probe", ""));
    // What an open would report, from a session we then throw away.
    let (session, _events) = runtime
        .open(&agent, SessionOptions::in_dir(std::env::temp_dir()))
        .await
        .unwrap();
    let opened = session.info().details;
    session.close().await.unwrap();
    // Probe reports the identical details without keeping a session.
    let probed = runtime.probe(&agent).await.unwrap();
    assert_eq!(probed, opened);
    // The comet use case: the `model` option and the command list.
    assert!(
        probed
            .config_options
            .iter()
            .any(|o| o.id.as_str() == "model")
    );
    assert!(probed.commands.iter().any(|c| c.name == "compact"));
}

#[tokio::test]
async fn a_logged_out_handshake_reports_unauthenticated_with_login_methods() {
    // The CLI sends an account object either way; only its contents separate
    // a login from none, and a stale offline marker must not override it.
    let agent = AgentInstallation::at("claude", wrapper("logged-out", "--logged-out"));
    let details = Runtime::new().probe(&agent).await.unwrap();
    let AuthStatus::Unauthenticated { login } = &details.auth else {
        panic!("expected Unauthenticated, got {:?}", details.auth);
    };
    assert!(
        login
            .iter()
            .any(|m| matches!(m, LoginMethod::Terminal { .. })),
        "a logged-out claude must carry a runnable login command"
    );
    // A real login still reads as one, with its plan.
    let signed_in = Runtime::new()
        .probe(&AgentInstallation::at("claude", wrapper("signed-in", "")))
        .await
        .unwrap();
    let AuthStatus::Authenticated { kind, account } = &signed_in.auth else {
        panic!("expected Authenticated, got {:?}", signed_in.auth);
    };
    assert_eq!(*kind, AuthKind::Subscription);
    assert_eq!(
        account.as_ref().unwrap().plan.as_deref(),
        Some("Claude Max")
    );
    // An env API key also reads as a login, though it keeps `tokenSource:
    // "none"` — that field alone must never decide.
    let api_key = Runtime::new()
        .probe(&AgentInstallation::at(
            "claude",
            wrapper("api-key", "--api-key"),
        ))
        .await
        .unwrap();
    assert!(matches!(
        api_key.auth,
        AuthStatus::Authenticated {
            kind: AuthKind::ApiKey,
            ..
        }
    ));
    // Some CLI versions name the credential in `tokenSource` itself.
    let token_source = Runtime::new()
        .probe(&AgentInstallation::at(
            "claude",
            wrapper("token-source", "--token-source-key"),
        ))
        .await
        .unwrap();
    assert!(matches!(
        token_source.auth,
        AuthStatus::Authenticated {
            kind: AuthKind::ApiKey,
            ..
        }
    ));
    // Bedrock carries no Anthropic identity — `apiProvider` is the only tell.
    let bedrock = Runtime::new()
        .probe(&AgentInstallation::at(
            "claude",
            wrapper("bedrock", "--bedrock"),
        ))
        .await
        .unwrap();
    assert!(matches!(
        bedrock.auth,
        AuthStatus::Authenticated {
            kind: AuthKind::CloudProvider,
            ..
        }
    ));
}

#[tokio::test]
async fn cancel_reaches_the_agent_and_ends_the_turn() {
    let (session, mut events) = open("cancel", "").await;
    session.prompt("hi").await.unwrap();
    loop {
        if let EventKind::RequestOpened(_) = next(&mut events).await.kind {
            break;
        }
    }
    session.cancel(false).await.unwrap();
    loop {
        if let EventKind::TurnEnded { stop, .. } = next(&mut events).await.kind {
            assert_eq!(stop, StopReason::Cancelled);
            break;
        }
    }
    session.close().await.unwrap();
}

// An interrupt can land before the CLI starts the turn: the prompt is
// cancelled out of the CLI's own queue and no `result` frame ever comes —
// the interrupt receipt ends the turn instead (probed live 2026-08-24).
#[tokio::test]
async fn cancel_before_the_turn_starts_ends_it_and_the_queue_advances() {
    let (session, mut events) = open("prestart", "").await;
    session.prompt("slow-start").await.unwrap();
    let queued = session.prompt("two").await.unwrap();
    assert_eq!(queued.kind, DeliveryKind::Queued { position: 0 });
    session.cancel(false).await.unwrap();
    loop {
        if let EventKind::TurnEnded { stop, .. } = next(&mut events).await.kind {
            assert_eq!(stop, StopReason::Cancelled);
            break;
        }
    }

    // The queued prompt is promoted and completes as its own turn.
    let mut origin = None;
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TurnStarted { origin: o } => origin = Some(o),
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::TurnEnded { stop, .. } => {
                assert!(matches!(stop, StopReason::Completed { .. }), "{stop:?}");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(origin, Some(TurnOrigin::Prompt(queued.prompt_id)));
    assert!(text.contains("Hello"), "queued turn ran: {text:?}");
    session.close().await.unwrap();
}

// The CLI queues mid-turn user messages as their own turns, so the engine
// queues instead of steering — and every turn keeps the right prompt id.
#[tokio::test]
async fn a_mid_turn_prompt_queues_and_runs_with_the_right_prompt_id() {
    let (session, mut events) = open("queue", "").await;
    session.prompt("one").await.unwrap();
    // Park on the permission request so the turn is reliably mid-flight.
    let request = loop {
        if let EventKind::RequestOpened(request) = next(&mut events).await.kind {
            break request;
        }
    };
    let queued = session.prompt("two").await.unwrap();
    assert_eq!(queued.kind, DeliveryKind::Queued { position: 0 });
    session.answer(request.id(), allow()).await.unwrap();

    let mut ends = 0;
    let mut second_origin = None;
    let mut second_text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TurnStarted { origin } if ends == 1 => second_origin = Some(origin),
            EventKind::TextDelta { text, .. } if ends == 1 => second_text.push_str(&text),
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => {
                ends += 1;
                if ends == 2 {
                    break;
                }
            }
            _ => {}
        }
    }
    assert_eq!(
        second_origin,
        Some(TurnOrigin::Prompt(queued.prompt_id)),
        "the queued prompt keeps its own id"
    );
    assert!(
        second_text.contains("Hello"),
        "queued turn ran: {second_text:?}"
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn a_question_is_typed_and_the_answer_reaches_the_agent() {
    let (session, mut events) = open("question", "--question").await;
    session.prompt("hi").await.unwrap();
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::RequestOpened(Request::Question(request)) => {
                let question = &request.questions[0];
                assert_eq!(question.text, "Which color do you prefer?");
                assert_eq!(question.header.as_deref(), Some("Color"));
                assert!(!question.multi_select);
                let labels: Vec<_> = question.choices.iter().map(|c| c.label.as_str()).collect();
                assert_eq!(labels, vec!["Red", "Blue"]);
                session
                    .answer(
                        request.id,
                        Answer::Question(vec![QuestionAnswer::Choices(vec!["Blue".into()])]),
                    )
                    .await
                    .unwrap();
            }
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::TurnEnded { stop, .. } => {
                assert!(matches!(stop, StopReason::Completed { .. }));
                break;
            }
            _ => {}
        }
    }
    assert_eq!(text, "answer=Blue");
    session.close().await.unwrap();
}

#[tokio::test]
async fn agent_death_mid_turn_fails_the_turn() {
    let (session, mut events) = open("eof", "--eof").await;
    session.prompt("hi").await.unwrap();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap()
            }
            EventKind::TurnEnded { stop, .. } => {
                assert!(matches!(stop, StopReason::Failed { .. }));
                break;
            }
            _ => {}
        }
    }
    let error = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .unwrap()
        .unwrap();
    match error {
        Err(AgentError::ProcessExited { status, stderr }) => {
            assert_eq!(status, "exit status: 3");
            assert!(stderr.contains("boom"), "stderr not carried: {stderr:?}");
        }
        other => panic!("expected ProcessExited, got {other:?}"),
    }
}

#[tokio::test]
async fn attachments_inline_images_and_reference_paths() {
    let dir = attachment_dir("claude");
    let (session, mut events) = open("attach", "").await;
    session
        .prompt(
            Input::text("look")
                .attach(dir.join("shot.png"))
                .attach(dir.join("report.pdf"))
                .attach(dir.join("missing.png")),
        )
        .await
        .unwrap();
    let mut text = String::new();
    let mut unreadable = 0;
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::Diagnostic(d) => {
                unreadable += usize::from(d.message.contains("unreadable"));
            }
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap()
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    // One inlined image; pdf and the unreadable file ride as path refs.
    assert!(text.contains("att=1 ref=1"), "wire shape wrong: {text:?}");
    assert_eq!(unreadable, 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn configuring_the_mode_round_trips_and_updates_the_session() {
    let (session, mut events) = open("mode", "").await;
    let advertised = session.info().details.config_options;
    let mode = advertised.iter().find(|o| o.id.as_str() == "mode").unwrap();
    assert_eq!(mode.current, Some(ConfigValue::Text("default".into())));
    session.configure("mode", "plan").await.unwrap();
    loop {
        let event = next(&mut events).await;
        if let EventKind::SessionUpdated(info) = event.kind {
            assert_eq!(
                info.configuration.options.get(&ConfigId::new("mode")),
                Some(&ConfigValue::Text("plan".into()))
            );
            break;
        }
    }
    session.close().await.unwrap();
}

// The model is switched live with the `set_model` control request, through
// the same configure path as `mode` (probed live 2026-08-24).
#[tokio::test]
async fn switching_the_model_round_trips_and_updates_the_session() {
    let (session, mut events) = open("model", "").await;
    session.configure("model", "sonnet").await.unwrap();
    loop {
        let event = next(&mut events).await;
        if let EventKind::SessionUpdated(info) = event.kind {
            assert_eq!(
                info.configuration.options.get(&ConfigId::new("model")),
                Some(&ConfigValue::Text("sonnet".into()))
            );
            break;
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn mcp_servers_ride_the_launch_config() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("mcp", ""));
    let (session, mut events) = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir())
                .mcp_server(McpServer::http("voice", "http://127.0.0.1:1/mcp"))
                .mcp_server(McpServer::stdio("tool", "/bin/echo", ["hi"])),
        )
        .await
        .unwrap();
    session.prompt("hi").await.unwrap();
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap()
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert!(text.contains("http:voice"), "declaration lost: {text:?}");
    assert!(text.contains("stdio:tool"), "declaration lost: {text:?}");
    session.close().await.unwrap();
}

#[tokio::test]
async fn a_mismatched_answer_is_rejected_and_the_request_stays_open() {
    let (session, mut events) = open("validate", "").await;
    session.prompt("hi").await.unwrap();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::RequestOpened(Request::Permission(request)) => {
                let wrong_type = session
                    .answer(request.id.clone(), Answer::Question(Vec::new()))
                    .await;
                assert!(matches!(wrong_type, Err(AgentError::InvalidRequest(_))));
                let not_offered = session
                    .answer(
                        request.id.clone(),
                        Answer::Permission(PermissionChoice::DenyAlways),
                    )
                    .await;
                assert!(matches!(not_offered, Err(AgentError::InvalidRequest(_))));
                // Still open: the valid answer goes through and the turn ends.
                session.answer(request.id, allow()).await.unwrap();
            }
            EventKind::TurnEnded { stop, .. } => {
                assert!(matches!(stop, StopReason::Completed { .. }));
                break;
            }
            _ => {}
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn a_background_task_wakes_an_agent_originated_turn() {
    let (session, mut events) = open("wake", "--wake").await;
    session.prompt("hi").await.unwrap();
    loop {
        if let EventKind::TurnEnded { background, .. } = next(&mut events).await.kind {
            // The backgrounded Bash outlives its turn.
            assert_eq!(background.len(), 1);
            assert_eq!(background[0].as_str(), "toolu_bg");
            break;
        }
    }
    // Its completion arrives as bookkeeping, then the wake turn starts.
    let mut background_done = false;
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::ToolUpdated(tool) if tool.id.as_str() == "toolu_bg" => {
                assert_eq!(tool.status, ToolStatus::Completed);
                assert!(event.turn_info.is_none());
                background_done = true;
            }
            EventKind::TurnStarted { origin } => {
                assert_eq!(origin, TurnOrigin::Agent);
                break;
            }
            _ => {}
        }
    }
    assert!(background_done);
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert_eq!(text, "BG-DONE");
    session.close().await.unwrap();
}

#[tokio::test]
async fn subagent_events_carry_the_parent_tool_id() {
    let (session, mut events) = open("subagent", "--subagent").await;
    session.prompt("hi").await.unwrap();
    let mut spawn_seen = false;
    let mut nested_text = String::new();
    let mut nested_user = None;
    loop {
        let event = next(&mut events).await;
        let parented = event.turn_info.as_ref().is_some_and(|t| {
            t.parent_tool_id
                .as_ref()
                .is_some_and(|p| p.as_str() == "toolu_task")
        });
        match event.kind {
            EventKind::ToolUpdated(tool) if tool.kind == ToolKind::Subagent => spawn_seen = true,
            EventKind::TextDelta { text, .. } if parented => nested_text.push_str(&text),
            EventKind::UserMessage { text, .. } if parented => nested_user = Some(text),
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert!(spawn_seen, "Task spawn tool missing");
    assert_eq!(nested_text, "sub ");
    assert_eq!(nested_user.as_deref(), Some("look deeper"));
    session.close().await.unwrap();
}

#[tokio::test]
async fn losing_auth_mid_session_fails_the_turn_and_closes() {
    let (session, mut events) = open("auth", "").await;
    session.prompt("die-auth").await.unwrap();
    loop {
        let event = next(&mut events).await;
        if let EventKind::TurnEnded { stop, .. } = event.kind {
            assert!(matches!(stop, StopReason::Failed { .. }));
            break;
        }
    }
    let error = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .unwrap()
        .unwrap();
    let Err(AgentError::AuthRequired { login }) = error else {
        panic!("expected AuthRequired, got {error:?}");
    };
    // The catalog's login command plus the API-key env var.
    assert_eq!(login.len(), 2);
    let anyagent::LoginMethod::Terminal { command, .. } = &login[0] else {
        panic!("expected a terminal method");
    };
    assert_eq!(command[1..], ["auth".to_string(), "login".to_string()]);
    assert!(matches!(
        &login[1],
        anyagent::LoginMethod::EnvVar { name } if name == "ANTHROPIC_API_KEY"
    ));
    // Only trailing status flips may remain; then the stream closes.
    loop {
        let end = tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .unwrap();
        match end {
            None => break,
            Some(Ok(event)) if matches!(event.kind, EventKind::StatusChanged(_)) => {}
            other => panic!("stream should close, got {other:?}"),
        }
    }
    assert!(matches!(
        session.prompt("hi").await,
        Err(AgentError::SessionClosed)
    ));
}

#[tokio::test]
async fn creation_time_config_rides_the_launch_args_and_unknown_ids_are_refused() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("creation", ""));
    let (session, _events) = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir())
                .configure("mode", "plan")
                .configure("model", "sonnet")
                .configure("effort", "low"),
        )
        .await
        .unwrap();
    // The fixture echoes `--permission-mode` back as the starting mode; the
    // model's current value is what `--model` was launched with.
    let info = session.info();
    assert_eq!(
        info.configuration.options.get(&ConfigId::new("mode")),
        Some(&ConfigValue::Text("plan".into()))
    );
    assert_eq!(
        info.configuration.options.get(&ConfigId::new("model")),
        Some(&ConfigValue::Text("sonnet".into()))
    );
    assert_eq!(
        info.configuration.options.get(&ConfigId::new("effort")),
        Some(&ConfigValue::Text("low".into()))
    );
    assert!(info.resume_token.is_some());
    session.close().await.unwrap();

    let refused = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir()).configure("sandbox", "on"),
        )
        .await
        .map(|_| ());
    let Err(AgentError::InvalidConfiguration(message)) = refused else {
        panic!("expected InvalidConfiguration");
    };
    assert!(message.contains("sandbox"), "got: {message}");
}

// -- config home isolation + wire recording (P2 smalls) ---------------------

/// `config_home` reaches the child as `CLAUDE_CONFIG_DIR`; the fixture echoes
/// the value it received back in its turn text.
#[tokio::test]
async fn config_home_reaches_the_child_as_an_env_var() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("cfg-home", "--echo-config-home"));
    let home = std::env::temp_dir().join(format!("anyagent-cfg-home-{}", std::process::id()));
    let (session, mut events) = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir()).config_home(&home),
        )
        .await
        .unwrap();
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events).await;
    assert!(
        text.contains(&format!("cfg={}", home.display())),
        "child did not see the config home: {text:?}"
    );
    session.close().await.unwrap();
}

/// Reads the recording file until it has grown past `min` lines or the poll
/// budget runs out; the writer task flushes asynchronously.
async fn recorded_lines(path: &Path, min: usize) -> Vec<serde_json::Value> {
    for _ in 0..40 {
        let body = std::fs::read_to_string(path).unwrap_or_default();
        if body.lines().count() >= min {
            return body
                .lines()
                .map(|l| serde_json::from_str(l).expect("each recorded line is valid JSON"))
                .collect();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("recording never reached {min} lines: {path:?}");
}

/// `record_wire` tees both directions, including the pre-turn handshake, as
/// one valid JSON object per line.
#[tokio::test]
async fn record_wire_tees_both_directions_including_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("wire.jsonl");
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("claude", wrapper("record", ""));
    let (session, mut events) = runtime
        .open(
            &agent,
            SessionOptions::in_dir(std::env::temp_dir()).record_wire(&log),
        )
        .await
        .unwrap();
    session.prompt("hi").await.unwrap();
    complete_turn(&session, &mut events).await;
    session.close().await.unwrap();

    let lines = recorded_lines(&log, 5).await;
    assert!(
        lines.iter().any(|f| f["dir"] == "out"),
        "no outbound frames recorded"
    );
    assert!(
        lines.iter().any(|f| f["dir"] == "in"),
        "no inbound frames recorded"
    );
    // The handshake runs before the first turn: its `initialize` control
    // request must be in the log, proving recording started at open.
    assert!(
        lines
            .iter()
            .any(|f| f["dir"] == "out" && f["frame"]["request"]["subtype"] == "initialize"),
        "handshake initialize was not recorded"
    );
}
