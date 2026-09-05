//! The native Codex adapter driven end to end through the public interface,
//! against the fixture agent (tests/fixtures/codex/fixture.mjs; needs `node`).
//! A wrapper script pins the catalog's `codex` id to the fixture.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthKind, AuthStatus, Capability, ConfigId, ConfigKind,
    ConfigValue, DeliveryKind, Event, EventKind, Events, Input, McpServer, PermissionChoice,
    PlanStatus, QuestionAnswer, Request, Runtime, Session, SessionOptions, StopReason, ToolInput,
    ToolKind, ToolStatus,
};

/// A `codex` stand-in: a script that execs the fixture with scenario flags,
/// ignoring the real launch args appended after them.
fn wrapper(name: &str, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/fixture.mjs");
    let dir = std::env::temp_dir().join(format!("anyagent-codex-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("codex");
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
    let agent = AgentInstallation::at("codex", wrapper(name, flags));
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
/// text, and returns it.
async fn complete_turn(session: &Session, events: &mut Events, answer: PermissionChoice) -> String {
    let mut text = String::new();
    loop {
        match next(events).await.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
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

fn text_option(session: &anyagent::SessionInfo, id: &str) -> Option<String> {
    session.configuration.options.iter().find_map(|(k, v)| {
        (k.as_str() == id).then(|| match v {
            ConfigValue::Text(t) => t.clone(),
            ConfigValue::Bool(b) => b.to_string(),
        })
    })
}

/// Handshake reports auth, version 0.147.0, capabilities, token, and deduped skills as commands.
#[tokio::test]
async fn handshake_reports_auth_version_options_and_token() {
    let (session, mut events) = open("handshake", "").await;
    // Skills arrive after open as a `SessionUpdated` (the fetch is async so
    // opens stay fast); wait for the list before reading the snapshot.
    while session.info().details.commands.is_empty() {
        next(&mut events).await;
    }
    let info = session.info();
    assert_eq!(info.details.version.as_deref(), Some("0.147.0"));
    let AuthStatus::Authenticated { kind, account } = &info.details.auth else {
        panic!(
            "expected an authenticated login, got {:?}",
            info.details.auth
        );
    };
    assert_eq!(*kind, AuthKind::Subscription);
    let account = account.as_ref().unwrap();
    assert_eq!(account.email.as_deref(), Some("user@example.com"));
    assert_eq!(account.plan.as_deref(), Some("edu"));
    // The resume token exists at open: `thread/start` returns the id.
    assert_eq!(info.resume_token.as_ref().unwrap().as_str(), "th-1");

    let caps = &info.details.capabilities;
    for cap in [Capability::Steer, Capability::Fork, Capability::PlanUsage] {
        assert!(caps.supports(cap.clone()), "missing {cap:?}");
    }
    for cap in [
        Capability::Questions,
        Capability::Rollback,
        Capability::Images,
    ] {
        assert!(!caps.supports(cap.clone()), "over-advertised {cap:?}");
    }

    // Hidden models stay hidden; effort defaults to the model's default.
    let model = info
        .details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .unwrap();
    let ConfigKind::Select { choices } = &model.kind else {
        panic!("model is a select");
    };
    assert_eq!(
        choices.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(),
        vec!["gpt-6", "gpt-6-mini"]
    );
    assert!(model.live);
    assert_eq!(text_option(&info, "model").as_deref(), Some("gpt-6"));
    assert_eq!(text_option(&info, "effort").as_deref(), Some("medium"));
    assert_eq!(text_option(&info, "mode").as_deref(), Some("on-request"));
    assert_eq!(text_option(&info, "sandbox").as_deref(), Some("read-only"));

    // Skills are the slash commands: deduped across roots, junk dropped, and
    // the picker-sized `interface.shortDescription` preferred.
    let commands: Vec<_> = info
        .details
        .commands
        .iter()
        .map(|c| (c.name.as_str(), c.description.as_str()))
        .collect();
    assert_eq!(
        commands,
        vec![("review", "Review a diff."), ("release", "Cut a release.")]
    );
    session.close().await.unwrap();
}

/// Probe returns commands in <2s without waiting full timeout.
#[tokio::test]
async fn probe_reads_commands_without_waiting_them_out() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("codex", wrapper("probe", ""));
    let started = std::time::Instant::now();
    let details = runtime.probe(&agent).await.unwrap();
    assert!(!details.commands.is_empty());
    // An empty command list would cost the probe its full 2 s wait.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

/// A subagent's child thread runs a whole turn inside the parent's: its
/// content must ride the subagent tool and its bookkeeping must not touch the
/// parent turn.
/// Subagent child thread's deltas/tool updates are attributed via parent_tool_id and don't settle parent turn.
#[tokio::test]
async fn a_subagent_child_thread_never_settles_the_parent_turn() {
    let (session, mut events) = open("subagent", "").await;
    session.prompt("subagent please").await.unwrap();

    let mut turns_started = 0;
    let mut turns_ended = 0;
    let mut child_text = Vec::new();
    let mut subagents = Vec::new();
    let mut usage = Vec::new();
    let mut plans = Vec::new();
    let mut diagnostics = Vec::new();
    loop {
        let event = next(&mut events).await;
        let parent = event
            .turn_info
            .as_ref()
            .and_then(|t| t.parent_tool_id.clone());
        match event.kind {
            EventKind::TurnStarted { .. } => turns_started += 1,
            EventKind::TextDelta { text, .. } if parent.is_some() => {
                child_text.push((text, parent.unwrap()));
            }
            EventKind::ToolUpdated(tool) if tool.kind == ToolKind::Subagent => {
                subagents.push(tool);
            }
            EventKind::ContextUsage { used_tokens, .. } => usage.push(used_tokens),
            EventKind::PlanUpdated { entries } => plans.push(entries),
            EventKind::Diagnostic(d) => diagnostics.push(d.message),
            EventKind::TurnEnded { .. } => {
                turns_ended += 1;
                break;
            }
            _ => {}
        }
    }
    assert_eq!((turns_started, turns_ended), (1, 1));
    assert!(diagnostics.is_empty(), "{diagnostics:?}");

    // The `subAgentActivity` tool is the child thread: Running while the child
    // works, Completed once its turn ends.
    let activity = subagents
        .iter()
        .find(|t| t.title.contains("reviewer.md"))
        .expect("a subagent tool for the child thread")
        .id
        .clone();
    let states: Vec<_> = subagents
        .iter()
        .filter(|t| t.id == activity)
        .map(|t| t.status)
        .collect();
    assert_eq!(states, vec![ToolStatus::Running, ToolStatus::Completed]);
    // The child's content is attributed to it.
    assert_eq!(child_text, vec![("child text".to_owned(), activity)]);
    // The collab call is a subagent tool too, carrying its prompt.
    assert!(
        subagents
            .iter()
            .any(|t| t.input == ToolInput::Text("review the diff".into())),
        "{subagents:?}"
    );
    // Neither the child's usage nor its plan reaches the parent's.
    assert_eq!(usage, vec![1200]);
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0][0].text, "step 1");
    session.close().await.unwrap();
}

/// Failed child turn marks its subagent tool as Failed but parent still completes.
#[tokio::test]
async fn a_failed_child_turn_fails_its_subagent_tool() {
    let (session, mut events) = open("subagent-fail", "").await;
    session.prompt("subagent-fails now").await.unwrap();
    let mut failed = None;
    loop {
        match next(&mut events).await.kind {
            EventKind::ToolUpdated(tool)
                if tool.kind == ToolKind::Subagent && tool.status == ToolStatus::Failed =>
            {
                failed = Some(tool);
            }
            // The parent turn still completes normally.
            EventKind::TurnEnded { stop, .. } => {
                assert!(matches!(stop, StopReason::Completed { .. }), "{stop:?}");
                break;
            }
            _ => {}
        }
    }
    let failed = failed.expect("the failed child marks its subagent tool Failed");
    assert_eq!(failed.output.as_deref(), Some("child blew up"));
    session.close().await.unwrap();
}

/// Full turn maps text/reasoning/execute tool, plan, usage, quota, and fork_point extension.
#[tokio::test]
async fn a_full_turn_maps_every_frame_kind() {
    let (session, mut events) = open("full", "").await;
    session.prompt("hi").await.unwrap();

    let mut text = String::new();
    let mut thoughts = String::new();
    let mut tool_states = Vec::new();
    let mut plan = Vec::new();
    let mut usage = None;
    let mut quota = None;
    let mut fork_point = None;
    loop {
        let event = next(&mut events).await;
        if let Some(point) = event.extensions.get("codex/fork_point") {
            fork_point = point.as_str().map(str::to_owned);
        }
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::ReasoningDelta { text: t, .. } => thoughts.push_str(&t),
            EventKind::ToolUpdated(tool) => tool_states.push(tool),
            EventKind::PlanUpdated { entries } => plan = entries,
            EventKind::ContextUsage {
                used_tokens,
                window_tokens,
                cost_usd,
            } => usage = Some((used_tokens, window_tokens, cost_usd)),
            EventKind::PlanUsageUpdated(u) => quota = Some(u),
            EventKind::UserMessage { .. } => panic!("own prompt echoed back"),
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
        text.starts_with("Hello ") && text.ends_with("done"),
        "{text}"
    );
    assert_eq!(thoughts, "thinking…");
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].text, "step 1");
    assert_eq!(plan[0].status, PlanStatus::InProgress);
    assert_eq!(usage, Some((1200, Some(258_400), None)));
    // The wire turn id rides `MessageEnded` as the fork anchor.
    assert_eq!(fork_point.as_deref(), Some("turn-0"));

    let quota = quota.unwrap();
    assert_eq!(quota.plan.as_deref(), Some("edu"));
    assert_eq!(quota.windows[0].label, "Session");
    assert_eq!(quota.windows[0].used_percent, 5);
    assert_eq!(quota.windows[1].label, "Week");
    assert_eq!(quota.windows[1].used_percent, 4);

    // The command: running, then completed with its output.
    let exec: Vec<_> = tool_states
        .iter()
        .filter(|t| t.kind == ToolKind::Execute)
        .collect();
    assert_eq!(exec[0].status, ToolStatus::Running);
    let done = exec.last().unwrap();
    assert_eq!(done.status, ToolStatus::Completed);
    assert_eq!(done.output.as_deref(), Some("PEAR\n"));
    session.close().await.unwrap();
}

/// Model+effort ride every turn; live model switch applies to next turn without wire call.
#[tokio::test]
async fn model_and_effort_ride_every_turn_and_switch_live() {
    let (session, mut events) = open_with(
        "per-turn",
        "",
        SessionOptions::in_dir(std::env::temp_dir())
            .configure("model", "gpt-6-mini")
            .configure("effort", "medium"),
    )
    .await
    .unwrap();
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("model=gpt-6-mini effort=medium"), "{text}");

    // A live switch needs no wire call: the next turn carries it.
    session.configure("model", "gpt-6").await.unwrap();
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind {
            assert_eq!(text_option(&info, "model").as_deref(), Some("gpt-6"));
            break;
        }
    }
    session.prompt("again").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("model=gpt-6 effort=medium"), "{text}");
    session.close().await.unwrap();
}

/// Effort falls back to model's default when new model lacks previous level.
#[tokio::test]
async fn service_tier_rides_turns_and_default_is_omitted() {
    // Configured at creation like Comet does; every turn also opts into
    // reasoning summaries (`summary: "auto"`).
    let (session, mut events) = open_with(
        "tier",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).configure("serviceTier", "priority"),
    )
    .await
    .unwrap();
    let info = session.info();
    let tier = info
        .details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "serviceTier")
        .unwrap();
    assert!(tier.live);
    let ConfigKind::Select { choices } = &tier.kind else {
        panic!("serviceTier is a select");
    };
    assert_eq!(
        choices.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(),
        vec!["default", "priority"]
    );
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("tier=priority summary=auto"), "{text}");

    // Back to Standard: "default" never reaches the wire.
    session.configure("serviceTier", "default").await.unwrap();
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind {
            assert_eq!(
                text_option(&info, "serviceTier").as_deref(),
                Some("default")
            );
            break;
        }
    }
    session.prompt("again").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("tier=unset"), "{text}");
    session.close().await.unwrap();
}

#[tokio::test]
async fn failed_and_aborted_turn_notifications_still_end_the_turn() {
    let (session, mut events) = open("turn-ends", "").await;
    for (prompt, expected) in [
        (
            "end-failed",
            StopReason::Failed {
                message: "wire failed".into(),
            },
        ),
        ("end-aborted", StopReason::Cancelled),
    ] {
        session.prompt(prompt).await.unwrap();
        loop {
            if let EventKind::TurnEnded { stop, .. } = next(&mut events).await.kind {
                assert_eq!(stop, expected);
                break;
            }
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn effort_falls_back_when_the_new_model_lacks_it() {
    let (session, mut events) = open_with(
        "effort-fallback",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).configure("effort", "high"),
    )
    .await
    .unwrap();
    assert_eq!(
        text_option(&session.info(), "effort").as_deref(),
        Some("high")
    );
    // gpt-6-mini has no "high": the effort falls back to its default.
    session.configure("model", "gpt-6-mini").await.unwrap();
    loop {
        if let EventKind::SessionUpdated(info) = next(&mut events).await.kind {
            assert_eq!(text_option(&info, "effort").as_deref(), Some("low"));
            let effort = info
                .details
                .config_options
                .iter()
                .find(|o| o.id.as_str() == "effort")
                .unwrap();
            let ConfigKind::Select { choices } = &effort.kind else {
                panic!("effort is a select");
            };
            assert_eq!(
                choices.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(),
                vec!["low", "medium"]
            );
            break;
        }
    }
    session.close().await.unwrap();
}

/// Invalid model/effort at creation validated locally before reaching wire.
#[tokio::test]
async fn creation_config_is_validated_before_the_wire_sees_it() {
    // The wire would accept the model and fail the turn later; open refuses.
    let err = open_with(
        "bad-model",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).configure("model", "nope"),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, AgentError::InvalidConfiguration(_)), "{err}");

    let err = open_with(
        "bad-effort",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).configure("effort", "ultra"),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, AgentError::InvalidConfiguration(_)), "{err}");
}

/// Mode/sandbox are creation-only; mid-session configure refused typed.
#[tokio::test]
async fn mode_and_sandbox_are_creation_only_thread_settings() {
    let (session, _events) = open_with(
        "mode",
        "",
        SessionOptions::in_dir(std::env::temp_dir())
            .configure("mode", "untrusted")
            .configure("sandbox", "workspace-write"),
    )
    .await
    .unwrap();
    let info = session.info();
    assert_eq!(text_option(&info, "mode").as_deref(), Some("untrusted"));
    assert_eq!(
        text_option(&info, "sandbox").as_deref(),
        Some("workspace-write")
    );
    // Not live: a mid-session change is refused by the engine.
    let err = session.configure("mode", "never").await.err().unwrap();
    assert!(matches!(err, AgentError::InvalidConfiguration(_)), "{err}");
    session.close().await.unwrap();
}

/// Approval accept completes tool with write=accept; decline marks tool Cancelled and turn continues.
#[tokio::test]
async fn approvals_map_accept_and_decline() {
    let (session, mut events) = open("approve", "").await;

    session.prompt("write-file please").await.unwrap();
    let mut text = String::new();
    let mut change_states = Vec::new();
    loop {
        match next(&mut events).await.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::ToolUpdated(tool) if tool.kind == ToolKind::Edit => change_states.push(tool),
            EventKind::RequestOpened(Request::Permission(request)) => {
                // The request names only the item; the tool snapshot carries
                // the diff from the preceding `item/started`.
                assert_eq!(request.tool.kind, ToolKind::Edit);
                assert_eq!(request.tool.diffs[0].new_text, "PEAR\n");
                assert_eq!(request.tool.locations, vec![PathBuf::from("fruit.txt")]);
                assert_eq!(
                    request.options,
                    vec![
                        PermissionChoice::AllowOnce,
                        PermissionChoice::AllowAlways,
                        PermissionChoice::DenyOnce,
                    ]
                );
                session
                    .answer(request.id, Answer::Permission(PermissionChoice::AllowOnce))
                    .await
                    .unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert!(text.contains("write=accept"), "{text}");
    assert_eq!(change_states.last().unwrap().status, ToolStatus::Completed);

    // Decline: the item ends `declined` and the turn continues.
    session.prompt("write-file again").await.unwrap();
    let mut declined = None;
    let text = loop {
        let mut text = String::new();
        match next(&mut events).await.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::ToolUpdated(tool) if tool.kind == ToolKind::Edit => {
                declined = Some(tool.status)
            }
            EventKind::RequestOpened(Request::Permission(request)) => {
                session
                    .answer(request.id, Answer::Permission(PermissionChoice::DenyOnce))
                    .await
                    .unwrap();
            }
            EventKind::TurnEnded { .. } => break text,
            _ => {}
        }
    };
    let _ = text;
    assert_eq!(declined, Some(ToolStatus::Cancelled));
    session.close().await.unwrap();
}

/// Steer sent before turn/started is held until accepted and folded via Steered delivery.
#[tokio::test]
async fn a_steer_folds_into_the_running_turn() {
    let (session, mut events) = open("steer", "").await;
    session.prompt("hi").await.unwrap();
    // Sent before `turn/started` arrives: the adapter holds it until the
    // wire will accept it (an early `turn/steer` is refused).
    let delivery = session.prompt("extra instructions").await.unwrap();
    assert!(
        matches!(delivery.kind, DeliveryKind::Steered { .. }),
        "{delivery:?}"
    );
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("steered=extra instructions"), "{text}");
    session.close().await.unwrap();
}

/// Cancel interrupts running command, marks tool Cancelled, and ends turn as Cancelled; idle cancel is no-op.
#[tokio::test]
async fn cancel_interrupts_and_cancels_inflight_tools() {
    let (session, mut events) = open("cancel", "").await;
    session.prompt("sleep forever").await.unwrap();
    // Wait for the command to be running, then interrupt.
    loop {
        if let EventKind::ToolUpdated(tool) = next(&mut events).await.kind {
            assert_eq!(tool.status, ToolStatus::Running);
            break;
        }
    }
    session.cancel(false).await.unwrap();
    let mut cancelled_tool = false;
    loop {
        match next(&mut events).await.kind {
            // No `item/completed` comes for it; the adapter cancels it.
            EventKind::ToolUpdated(tool) => {
                assert_eq!(tool.status, ToolStatus::Cancelled);
                cancelled_tool = true;
            }
            EventKind::TurnEnded { stop, .. } => {
                assert_eq!(stop, StopReason::Cancelled);
                break;
            }
            _ => {}
        }
    }
    assert!(cancelled_tool);
    // Idle cancel is a no-op, not an error.
    session.cancel(false).await.unwrap();
    session.close().await.unwrap();
}

/// Logged-out reported at handshake; first model call surfaces AuthRequired without retries.
#[tokio::test]
async fn logged_out_is_reported_and_the_first_turn_surfaces_auth_required() {
    let (session, mut events) = open("logged-out", "--logged-out").await;
    let AuthStatus::Unauthenticated { login } = session.info().details.auth else {
        panic!("expected Unauthenticated");
    };
    assert!(!login.is_empty());

    // The server accepts the turn; the 401 appears at the first model call
    // and surfaces as AuthRequired without waiting out the retries.
    session.prompt("hi").await.unwrap();
    let mut failed = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .expect("timed out")
            .expect("stream ended")
        {
            Ok(event) => {
                if let EventKind::TurnEnded { stop, .. } = event.kind {
                    assert!(matches!(stop, StopReason::Failed { .. }), "{stop:?}");
                    failed = true;
                }
            }
            Err(AgentError::AuthRequired { login }) => {
                assert!(!login.is_empty());
                break;
            }
            Err(other) => panic!("unexpected stream error: {other}"),
        }
    }
    assert!(failed);
}

/// Resume keeps thread id; fork at fork_point creates new id with correct fork anchor.
#[tokio::test]
async fn resume_keeps_the_thread_and_fork_cuts_at_the_anchor() {
    let (session, mut events) = open("resume-src", "").await;
    session.prompt("hi").await.unwrap();
    complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    let token = session.info().resume_token.unwrap();
    session.close().await.unwrap();

    let (resumed, _events) = open_with(
        "resume",
        "",
        SessionOptions::in_dir(std::env::temp_dir()).resume(token.clone()),
    )
    .await
    .unwrap();
    assert_eq!(resumed.info().resume_token.unwrap(), token);
    resumed.close().await.unwrap();

    // Fork at a wire turn id (the `codex/fork_point` extension currency).
    let (fork, mut events) = open_with(
        "fork",
        "",
        SessionOptions::in_dir(std::env::temp_dir())
            .fork_from(token, Some(anyagent::MessageId::new("turn-0"))),
    )
    .await
    .unwrap();
    assert_eq!(fork.info().resume_token.unwrap().as_str(), "th-fork-1");
    fork.prompt("hi").await.unwrap();
    let text = complete_turn(&fork, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("fork=turn-0"), "{text}");
    fork.close().await.unwrap();
}

/// Plan usage probe reads Session/Week windows with resets; logged-out typed AuthRequired.
#[tokio::test]
async fn plan_usage_probe_reads_the_windows() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("codex", wrapper("usage", ""));
    let usage = runtime.plan_usage(&agent).await.unwrap();
    assert_eq!(usage.plan.as_deref(), Some("edu"));
    assert_eq!(usage.windows.len(), 2);
    assert_eq!(usage.windows[0].label, "Session");
    assert_eq!(usage.windows[0].used_percent, 5);
    assert!(usage.windows[0].resets_at.is_some());
    assert_eq!(usage.windows[1].label, "Week");

    // Logged out, the refusal is typed as a login problem.
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("codex", wrapper("usage-out", "--logged-out"));
    let err = runtime.plan_usage(&agent).await.err().unwrap();
    assert!(matches!(err, AgentError::AuthRequired { .. }), "{err}");
}

/// requestUserInput question translates both ways even though capability not advertised.
#[tokio::test]
async fn a_question_request_translates_both_ways() {
    // `requestUserInput` is schema-confirmed but unobserved live (ticket 10):
    // the translation exists even though the capability is not advertised.
    let (session, mut events) = open("question", "--question").await;
    session.prompt("hi").await.unwrap();
    let mut text = String::new();
    loop {
        match next(&mut events).await.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(Request::Question(request)) => {
                assert_eq!(request.questions.len(), 1);
                let question = &request.questions[0];
                assert_eq!(question.text, "Which color?");
                assert_eq!(question.header.as_deref(), Some("Color"));
                assert_eq!(question.choices.len(), 2);
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
    assert!(text.contains("answer=Red"), "{text}");
    session.close().await.unwrap();
}

/// MCP forwarding refused typed UnsupportedFeature.
#[tokio::test]
async fn mcp_forwarding_is_refused_typed() {
    let err = open_with(
        "mcp",
        "",
        SessionOptions::in_dir(std::env::temp_dir())
            .mcp_server(McpServer::http("docs", "http://localhost:1")),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, AgentError::UnsupportedFeature(_)), "{err}");
}

/// Config_home creates directory and reaches child as CODEX_HOME; turn echoes cfg path.
#[tokio::test]
async fn config_home_reaches_the_child_and_is_created() {
    let home = std::env::temp_dir().join(format!("anyagent-codex-home-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    let (session, mut events) = open_with(
        "config-home",
        "--echo-config-home",
        SessionOptions::in_dir(std::env::temp_dir()).config_home(&home),
    )
    .await
    .unwrap();
    // The adapter creates the directory: CODEX_HOME must exist at spawn.
    assert!(home.is_dir());
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains(&format!("cfg={}", home.display())), "{text}");
    session.close().await.unwrap();
}

/// Attachments ride as path refs (ref count in wire).
#[tokio::test]
async fn attachments_ride_as_path_refs() {
    let dir = std::env::temp_dir().join(format!("anyagent-codex-att-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("report.pdf"), b"%PDF-1.7 data").unwrap();
    let (session, mut events) = open("attach", "").await;
    session
        .prompt(Input::text("look at this").attach(dir.join("report.pdf")))
        .await
        .unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("ref=1"), "{text}");
    session.close().await.unwrap();
}

/// Dead agent surfaces ProcessExited status 3 and stderr boom.
#[tokio::test]
async fn a_dead_agent_surfaces_the_exit_and_stderr() {
    let (session, mut events) = open("die", "").await;
    session.prompt("die now").await.unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .expect("timed out")
            .expect("stream ended")
        {
            Ok(_) => {}
            Err(AgentError::ProcessExited { status, stderr }) => {
                assert!(status.contains('3'), "{status}");
                assert!(stderr.contains("boom"), "{stderr}");
                break;
            }
            Err(other) => panic!("unexpected stream error: {other}"),
        }
    }
    drop(session);
}

/// Codex compacts through `thread/compact/start`, which runs a turn of its
/// own; a refusal never starts one, so the adapter ends the turn itself.
#[tokio::test]
async fn compact_reports_the_compaction_as_an_agent_turn() {
    let (session, mut events) = open("compact", "").await;
    assert!(
        session
            .info()
            .details
            .capabilities
            .supports(Capability::Compact)
    );
    session.compact().await.unwrap();
    let mut kinds = Vec::new();
    while !matches!(kinds.last(), Some(EventKind::TurnEnded { .. })) {
        kinds.push(next(&mut events).await.kind);
    }
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::ContextCompacted)),
        "{kinds:?}"
    );

    let (session, mut events) = open("compact-refused", "--compact-refuses").await;
    session.compact().await.unwrap();
    let mut kinds = Vec::new();
    while !matches!(kinds.last(), Some(EventKind::TurnEnded { .. })) {
        kinds.push(next(&mut events).await.kind);
    }
    assert!(
        kinds.iter().any(
            |k| matches!(k, EventKind::Diagnostic(d) if d.message.contains("nothing to compact"))
        ),
        "{kinds:?}"
    );
}

/// Fast uses the catalog's tier, changes live, and clears on unsupported models.
#[tokio::test]
async fn fast_mode_follows_the_model_catalog_and_turns() {
    let (session, mut events) = open_with(
        "fast",
        "--no-mini-fast",
        SessionOptions::in_dir(std::env::temp_dir()).configure("fast", true),
    )
    .await
    .unwrap();
    let fast = session
        .info()
        .details
        .config_options
        .into_iter()
        .find(|o| o.id.as_str() == "fast")
        .unwrap();
    assert_eq!(fast.kind, ConfigKind::Boolean);
    assert_eq!(fast.current, Some(ConfigValue::Bool(true)));
    assert!(fast.live);
    // `default` is omitted from the turn params (probed): the fixture echoes
    // it as `unset`.
    for (id, value, tier) in [
        ("fast", ConfigValue::Bool(true), "priority"),
        ("fast", ConfigValue::Bool(false), "unset"),
        ("fast", ConfigValue::Bool(true), "priority"),
        ("model", ConfigValue::from("gpt-6-mini"), "unset"),
        ("model", ConfigValue::from("gpt-6"), "unset"),
    ] {
        session.configure(id, value).await.unwrap();
        session.prompt("hi").await.unwrap();
        let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
        assert!(text.contains(&format!("tier={tier}")), "{text}");
        if id == "model" && tier == "unset" && text.contains("model=gpt-6-mini ") {
            assert!(
                !session
                    .info()
                    .details
                    .config_options
                    .iter()
                    .any(|o| o.id.as_str() == "fast")
            );
            assert!(session.configure("fast", true).await.is_err());
        }
    }
    session.close().await.unwrap();
}

/// Inherited Fast and the older catalog both retain the correct wire tier.
#[tokio::test]
async fn fast_mode_preserves_defaults_and_legacy_tiers() {
    let (session, mut events) = open_with(
        "fast-default",
        "--default-fast",
        SessionOptions::in_dir(std::env::temp_dir()),
    )
    .await
    .unwrap();
    assert_eq!(
        session
            .info()
            .configuration
            .options
            .get(&ConfigId::new("fast")),
        Some(&ConfigValue::Bool(true))
    );
    session.configure("model", "gpt-6-mini").await.unwrap();
    session.prompt("hi").await.unwrap();
    let text = complete_turn(&session, &mut events, PermissionChoice::AllowOnce).await;
    assert!(text.contains("tier=fast"), "{text}");
    session.close().await.unwrap();
}

/// Invalid Fast selections fail before a turn reaches the provider.
#[tokio::test]
async fn invalid_fast_configuration_is_rejected() {
    for (name, model, value) in [
        ("fast-type", "gpt-6", ConfigValue::from("true")),
        ("fast-unsupported", "gpt-6-mini", ConfigValue::Bool(true)),
    ] {
        let result = open_with(
            name,
            "--no-mini-fast",
            SessionOptions::in_dir(std::env::temp_dir())
                .configure("model", model)
                .configure("fast", value),
        )
        .await;
        assert!(matches!(result, Err(AgentError::InvalidConfiguration(_))));
    }
}
