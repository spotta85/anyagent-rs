//! The native opencode adapter driven end to end through the public
//! interface, against the fixture server (tests/fixtures/opencode/fixture.mjs;
//! needs `node`). A wrapper script pins the catalog's `opencode` id to it.

#![cfg(unix)]

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthStatus, Capability, ConfigId, ConfigKind,
    ConfigValue, Event, EventKind, Events, Input, MessageId, PermissionChoice, QuestionAnswer,
    Request, RollbackScope, Runtime, Session, SessionOptions, StopReason, ToolKind, ToolStatus,
    TurnOrigin,
};

/// An `opencode` stand-in: a script that execs the fixture with scenario
/// flags, then the real `serve --hostname … --port N` args.
fn wrapper(name: &str, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/opencode/fixture.mjs");
    let dir = std::env::temp_dir().join(format!("anyagent-opencode-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("opencode");
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

async fn open_with(
    name: &str,
    flags: &str,
    options: SessionOptions,
) -> Result<(Session, Events), AgentError> {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("opencode", wrapper(name, flags));
    runtime.open(&agent, options).await
}

async fn open(name: &str, flags: &str) -> (Session, Events) {
    open_with(name, flags, SessionOptions::in_dir(std::env::temp_dir()))
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

/// Drives one turn to its end: answers permissions with `answer`, collects
/// the agent's own text, and returns it.
async fn complete_turn(session: &Session, events: &mut Events, answer: PermissionChoice) -> String {
    let mut text = String::new();
    loop {
        let event = next(events).await;
        let nested = event
            .turn_info
            .as_ref()
            .is_some_and(|t| t.parent_tool_id.is_some());
        match event.kind {
            EventKind::TextDelta { text: t, .. } if !nested => text.push_str(&t),
            EventKind::RequestOpened(Request::Permission(request)) => {
                session
                    .answer(request.id, Answer::Permission(answer))
                    .await
                    .unwrap();
            }
            EventKind::TurnEnded { .. } => return text,
            _ => {}
        }
    }
}

fn text_option(info: &anyagent::SessionInfo, id: &str) -> Option<String> {
    match info.configuration.options.get(&ConfigId::new(id)) {
        Some(ConfigValue::Text(t)) => Some(t.clone()),
        _ => None,
    }
}

/// Handshake reports version, capabilities, the connected providers' models,
/// the model's variants as effort, commands, and the session id as token.
#[tokio::test]
async fn the_handshake_fills_details() {
    let (session, _events) = open("handshake", "").await;
    let info = session.info();
    assert_eq!(info.details.version.as_deref(), Some("1.18.24"));
    assert!(matches!(
        info.details.auth,
        AuthStatus::Authenticated { .. }
    ));
    for cap in [
        Capability::Images,
        Capability::Resume,
        Capability::Fork,
        Capability::Permissions,
        Capability::Questions,
        Capability::Rollback,
        Capability::Compact,
        Capability::SlashCommands,
        Capability::Plan,
        Capability::ContextUsage,
        Capability::Subagents,
    ] {
        assert!(info.details.capabilities.supports(cap.clone()), "{cap:?}");
    }
    assert!(!info.details.capabilities.supports(Capability::Steer));
    let model = info
        .details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .unwrap();
    let ConfigKind::Select { choices } = &model.kind else {
        panic!("model is a select")
    };
    let values: Vec<&str> = choices.iter().map(|c| c.value.as_str()).collect();
    assert_eq!(values, ["opencode/big-pickle", "opencode/small"]);
    assert_eq!(
        text_option(&info, "model").as_deref(),
        Some("opencode/big-pickle")
    );
    let effort = info
        .details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "effort")
        .unwrap();
    let ConfigKind::Select { choices } = &effort.kind else {
        panic!("effort is a select")
    };
    let levels: Vec<&str> = choices.iter().map(|c| c.value.as_str()).collect();
    assert_eq!(levels, ["low", "high"]);
    assert!(effort.live);
    assert!(info.details.commands.iter().any(|c| c.name == "init"));
    assert_eq!(info.title, None, "the dated placeholder is not a title");
    assert_eq!(info.resume_token.unwrap().as_str(), "ses_1");
    session.close().await.unwrap();
}

/// No connected provider is the one honest logged-out; the session still opens.
#[tokio::test]
async fn logged_out_reports_unauthenticated_with_no_models() {
    let (session, _events) = open("logged-out", "--logged-out").await;
    let info = session.info();
    assert!(matches!(
        info.details.auth,
        AuthStatus::Unauthenticated { .. }
    ));
    assert_eq!(
        text_option(&info, "model").as_deref(),
        Some("opencode/big-pickle")
    );
    session.close().await.unwrap();
}

/// A full turn maps text, reasoning, the bash tool with output, the plan,
/// usage with the model's window, one MessageEnded, and a protocol end.
#[tokio::test]
async fn a_full_turn_maps_every_frame_kind() {
    let (session, mut events) = open("full", "").await;
    session.prompt("hi").await.unwrap();
    let mut text = String::new();
    let mut thoughts = String::new();
    let mut tools = Vec::new();
    let mut plan = Vec::new();
    let mut usage = None;
    let mut ended = Vec::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TurnStarted { origin } => {
                assert!(matches!(origin, TurnOrigin::Prompt(_)))
            }
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::ReasoningDelta { text: t, .. } => thoughts.push_str(&t),
            EventKind::ToolUpdated(tool) => tools.push(tool),
            EventKind::PlanUpdated { entries } => {
                plan = entries.into_iter().map(|e| e.text).collect()
            }
            EventKind::ContextUsage {
                used_tokens,
                window_tokens,
                cost_usd,
            } => usage = Some((used_tokens, window_tokens, cost_usd)),
            EventKind::MessageEnded { message_id } => {
                assert!(event.extensions.contains_key("opencode/fork_point"));
                ended.push(message_id);
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
    assert!(
        text.starts_with("Hello model=opencode/big-pickle variant=unset images=0 "),
        "{text}"
    );
    assert!(text.ends_with("done"), "{text}");
    assert_eq!(thoughts, "thinking…");
    let bash: Vec<_> = tools
        .iter()
        .filter(|t| t.kind == ToolKind::Execute)
        .collect();
    assert_eq!(bash.last().unwrap().status, ToolStatus::Completed);
    assert_eq!(bash.last().unwrap().output.as_deref(), Some("PEAR\n"));
    assert_eq!(bash.last().unwrap().title, "bash echo PEAR");
    assert_eq!(plan, ["step 1"]);
    assert_eq!(usage, Some((1200, Some(128000), Some(0.01))));
    assert_eq!(ended, vec![MessageId::new("m1")]);
    session.close().await.unwrap();
}

/// A write asks permission; allow completes it, deny fails it, and both
/// reach the agent as its own reply codes.
#[tokio::test]
async fn permissions_allow_and_deny_the_write() {
    for (name, choice, expected, status) in [
        (
            "perm-allow",
            PermissionChoice::AllowOnce,
            "perm=once",
            ToolStatus::Completed,
        ),
        (
            "perm-deny",
            PermissionChoice::DenyOnce,
            "perm=reject",
            ToolStatus::Failed,
        ),
    ] {
        let (session, mut events) = open(name, "").await;
        session.prompt("write-file please").await.unwrap();
        let mut text = String::new();
        let mut write = None;
        loop {
            match next(&mut events).await.kind {
                EventKind::TextDelta { text: t, .. } => text.push_str(&t),
                EventKind::RequestOpened(Request::Permission(request)) => {
                    assert_eq!(request.tool.title, "write fruit.txt");
                    assert_eq!(request.detail.as_deref(), Some("fruit.txt"));
                    session
                        .answer(request.id, Answer::Permission(choice))
                        .await
                        .unwrap();
                }
                EventKind::ToolUpdated(tool) if tool.kind == ToolKind::Edit => {
                    write = Some(tool.status)
                }
                EventKind::TurnEnded { .. } => break,
                _ => {}
            }
        }
        assert!(text.contains(expected), "{text}");
        assert_eq!(write, Some(status));
        session.close().await.unwrap();
    }
}

/// The question tool becomes a question request; the chosen label goes back.
#[tokio::test]
async fn a_question_round_trips() {
    let (session, mut events) = open("question", "").await;
    session.prompt("question time").await.unwrap();
    let mut text = String::new();
    loop {
        match next(&mut events).await.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(Request::Question(request)) => {
                let q = &request.questions[0];
                assert_eq!(q.text, "Which color?");
                assert_eq!(q.header.as_deref(), Some("Color"));
                assert_eq!(q.choices.len(), 2);
                assert!(!q.allows_free_text);
                session
                    .answer(
                        request.id,
                        Answer::Question(vec![QuestionAnswer::Choices(vec!["Red".into()])]),
                    )
                    .await
                    .unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert!(text.contains("q=Red"), "{text}");
    session.close().await.unwrap();
}

/// Cancel aborts the turn; the abort's second idle is ignored and the next
/// prompt runs normally.
#[tokio::test]
async fn cancel_aborts_and_the_late_idle_is_ignored() {
    let (session, mut events) = open("cancel", "").await;
    session.prompt("sleep for a while").await.unwrap();
    loop {
        if let EventKind::ToolUpdated(tool) = next(&mut events).await.kind
            && tool.status == ToolStatus::Running
        {
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
    tokio::time::sleep(Duration::from_millis(100)).await;
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.ends_with("done"), "{text}");
    session.close().await.unwrap();
}

/// Compaction runs as an agent turn and reports `ContextCompacted`.
#[tokio::test]
async fn compact_reports_the_compaction_as_an_agent_turn() {
    let (session, mut events) = open("compact", "").await;
    session.compact().await.unwrap();
    let mut kinds = Vec::new();
    loop {
        let kind = next(&mut events).await.kind;
        let done = matches!(kind, EventKind::TurnEnded { .. });
        kinds.push(kind);
        if done {
            break;
        }
    }
    assert!(matches!(
        kinds[0],
        EventKind::TurnStarted {
            origin: TurnOrigin::Agent
        }
    ));
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::ContextCompacted))
    );
    session.close().await.unwrap();
}

/// Rollback reverts at the user message `turns` back; `SessionUpdated` confirms.
#[tokio::test]
async fn rollback_reverts_at_the_dropped_turns_user_message() {
    let (session, mut events) = open("rollback", "").await;
    for prompt in ["one", "two"] {
        session.prompt(prompt).await.unwrap();
        complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    }
    session
        .rollback(NonZeroU32::new(1).unwrap(), RollbackScope::Conversation)
        .await
        .unwrap();
    loop {
        if let EventKind::SessionUpdated(_) = next(&mut events).await.kind {
            break;
        }
    }
    session.prompt("three").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("reverted=msg_003"), "{text}");
    session.close().await.unwrap();
}

/// Fork cuts after the anchor message; no anchor forks at the tip.
#[tokio::test]
async fn fork_cuts_after_the_anchor_or_at_the_tip() {
    let (session, mut events) = open("fork", "").await;
    session.prompt("one").await.unwrap();
    complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    let token = session.info().resume_token.unwrap();
    session.close().await.unwrap();
    let agent = AgentInstallation::at("opencode", wrapper("fork", ""));
    for (at, expected) in [
        (Some(MessageId::new("msg_001")), "fork=msg_002"),
        (None, "fork=tip"),
    ] {
        let (session, mut events) = Runtime::new()
            .open(
                &agent,
                SessionOptions::in_dir(std::env::temp_dir()).fork_from(token.clone(), at),
            )
            .await
            .unwrap();
        session.prompt("go").await.unwrap();
        let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
        assert!(text.contains(expected), "{text}");
        session.close().await.unwrap();
    }
}

/// A task-tool child session streams under its task tool and its
/// permission reaches the caller.
#[tokio::test]
async fn a_child_session_nests_under_its_task_tool() {
    let (session, mut events) = open("child", "").await;
    session.prompt("child please").await.unwrap();
    let mut nested_text = String::new();
    let mut text = String::new();
    let mut task_parent = None;
    loop {
        let event = next(&mut events).await;
        let parent = event
            .turn_info
            .as_ref()
            .and_then(|t| t.parent_tool_id.clone());
        match event.kind {
            EventKind::TextDelta { text: t, .. } if parent.is_some() => {
                task_parent = parent;
                nested_text.push_str(&t);
            }
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(Request::Permission(request)) => {
                assert_eq!(request.tool.title, "bash ls");
                session
                    .answer(request.id, Answer::Permission(PermissionChoice::AllowOnce))
                    .await
                    .unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert_eq!(nested_text, "child text");
    assert!(text.contains("child=once"), "{text}");
    assert!(task_parent.unwrap().as_str().starts_with("call_task"));
    session.close().await.unwrap();
}

/// An advertised `/command` routes to the command endpoint; unknown ones
/// are plain text.
#[tokio::test]
async fn slash_commands_route_to_the_command_endpoint() {
    let (session, mut events) = open("command", "").await;
    session.prompt("/init now").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("cmd=init args=now"), "{text}");
    session.prompt("/nope").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("model=opencode/big-pickle"), "{text}");
    session.close().await.unwrap();
}

/// Model and effort ride every prompt; effort follows the model's variants.
#[tokio::test]
async fn model_and_effort_switch_live_and_ride_every_prompt() {
    let (session, mut events) = open("configure", "").await;
    session.configure("effort", "high").await.unwrap();
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind
            && text_option(&info, "effort").as_deref() == Some("high")
        {
            break;
        }
    }
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(
        text.contains("model=opencode/big-pickle variant=high"),
        "{text}"
    );
    // `small` has no variants: the effort option disappears with the switch.
    session.configure("model", "opencode/small").await.unwrap();
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind
            && text_option(&info, "model").as_deref() == Some("opencode/small")
        {
            assert!(
                !info
                    .details
                    .config_options
                    .iter()
                    .any(|o| o.id.as_str() == "effort")
            );
            break;
        }
    }
    session.prompt("again").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(
        text.contains("model=opencode/small variant=unset"),
        "{text}"
    );
    session.close().await.unwrap();
}

/// Creation-time model is validated against the connected providers.
#[tokio::test]
async fn creation_config_is_validated_against_the_catalog() {
    let (session, _events) = open_with(
        "creation",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).configure("model", "opencode/small"),
    )
    .await
    .unwrap();
    assert_eq!(
        text_option(&session.info(), "model").as_deref(),
        Some("opencode/small")
    );
    session.close().await.unwrap();
    let err = open_with(
        "creation-bad",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).configure("model", "offline/x"),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, AgentError::InvalidConfiguration(_)), "{err}");
}

/// Every attachment rides as a path ref; images also ride as file parts.
#[tokio::test]
async fn attachments_ride_as_refs_and_images_as_file_parts() {
    let dir = std::env::temp_dir().join(format!("anyagent-oc-att-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("shot.png"), b"\x89PNG\r\n\x1a\ndata").unwrap();
    std::fs::write(dir.join("report.pdf"), b"%PDF-1.7 data").unwrap();
    let (session, mut events) = open("attach", "").await;
    session
        .prompt(
            Input::text("look")
                .attach(dir.join("shot.png"))
                .attach(dir.join("report.pdf")),
        )
        .await
        .unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("images=1 ref=1"), "{text}");
    session.close().await.unwrap();
}

/// The server's own rename lands as the session title.
#[tokio::test]
async fn a_rename_updates_the_title() {
    let (session, mut events) = open("rename", "--rename").await;
    session.prompt("hi").await.unwrap();
    complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind
            && info.title.is_some()
        {
            assert_eq!(info.title.as_deref(), Some("Pear talk"));
            break;
        }
    }
    session.close().await.unwrap();
}

/// A dead server fails the turn and ends the stream with the exit report.
#[tokio::test]
async fn a_dead_server_surfaces_the_exit() {
    let (session, mut events) = open("die", "").await;
    session.prompt("die now").await.unwrap();
    let mut failed = false;
    let mut exited = false;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .unwrap()
    {
        match event {
            Ok(Event {
                kind: EventKind::TurnEnded { stop, .. },
                ..
            }) => failed = matches!(stop, StopReason::Failed { .. }),
            Err(AgentError::ProcessExited { status, stderr }) => {
                assert!(status.contains('3'), "{status}");
                assert!(stderr.contains("boom"), "{stderr}");
                exited = true;
            }
            _ => {}
        }
    }
    assert!(failed && exited);
    let _ = session;
}

/// Probe reads the same details an open does, without leaving a session.
#[tokio::test]
async fn probe_reports_details() {
    let agent = AgentInstallation::at("opencode", wrapper("probe", ""));
    let details = Runtime::new().probe(&agent).await.unwrap();
    assert_eq!(details.version.as_deref(), Some("1.18.24"));
    assert!(details.commands.iter().any(|c| c.name == "init"));
}
