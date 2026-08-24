//! The native Claude adapter driven end to end through the public interface,
//! against the fixture agent (tests/fixtures/claude/fixture.mjs; needs `node`).
//! A wrapper script pins the catalog's `claude` id to the fixture.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthStatus, Capability, ConfigId, ConfigKind,
    ConfigSelection, ConfigValue, DeliveryKind, Event, EventKind, Events, Input, McpServer,
    PermissionChoice, PlanStatus, QuestionAnswer, Request, Runtime, Session, SessionOptions,
    StopReason, ToolKind, ToolStatus, TurnOrigin,
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
        Capability::Resume,
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
    // The adapter mints the session id, so the token exists before any turn.
    assert!(session.info().resume_token.is_some());
    session.close().await.unwrap();
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
    session
        .configure(ConfigSelection::option("mode", "plan"))
        .await
        .unwrap();
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
    session
        .configure(ConfigSelection::option("model", "sonnet"))
        .await
        .unwrap();
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
                assert!(event.turn.is_none());
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
        let parented = event.turn.as_ref().is_some_and(|t| {
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
    let end = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .unwrap();
    assert!(end.is_none(), "stream should close, got {end:?}");
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
                .configure("model", "sonnet"),
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
