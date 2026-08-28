//! The native Codex adapter driven end to end through the public interface,
//! against the fixture agent (tests/fixtures/codex/fixture.mjs; needs `node`).
//! A wrapper script pins the catalog's `codex` id to the fixture.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthKind, AuthStatus, Capability, ConfigKind,
    ConfigSelection, ConfigValue, DeliveryKind, Event, EventKind, Events, Input, McpServer,
    PermissionChoice, PlanStatus, QuestionAnswer, Request, Runtime, Session, SessionOptions,
    StopReason, ToolKind, ToolStatus,
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

#[tokio::test]
async fn handshake_reports_auth_version_options_and_token() {
    let (session, _events) = open("handshake", "").await;
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
    session.close().await.unwrap();
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
    session
        .configure(ConfigSelection::option("model", "gpt-6"))
        .await
        .unwrap();
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
    session
        .configure(ConfigSelection::option("model", "gpt-6-mini"))
        .await
        .unwrap();
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
    let err = session
        .configure(ConfigSelection::option("mode", "never"))
        .await
        .err()
        .unwrap();
    assert!(matches!(err, AgentError::InvalidConfiguration(_)), "{err}");
    session.close().await.unwrap();
}

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

#[tokio::test]
async fn plan_usage_probe_reads_the_windows() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("codex", wrapper("usage", ""));
    let usage = runtime.plan_usage(&agent).await.unwrap();
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

/// A `codex` stand-in for login flows: the wrapper exports a private
/// CODEX_HOME so the fixture's auth.json is per-test.
fn login_wrapper(name: &str, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/fixture.mjs");
    let dir = std::env::temp_dir().join(format!("anyagent-codex-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("codex");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nCODEX_HOME={} exec node {} {flags} \"$@\"\n",
            dir.display(),
            fixture.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// Drains a login stream: returns the last URL and the final status.
async fn drain_login(login: &mut anyagent::LoginSession) -> (Option<String>, Option<AuthStatus>) {
    let (mut url, mut status) = (None, None);
    let drain = async {
        while let Some(event) = login.events.next().await {
            match event {
                anyagent::LoginEvent::OpenUrl { url: u, .. } => url = Some(u),
                anyagent::LoginEvent::Finished { status: s } => status = Some(s),
                _ => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("login stream did not finish");
    (url, status)
}

#[tokio::test]
async fn login_runs_in_protocol_and_reports_the_new_account() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("codex", login_wrapper("login-ok", "--auth-file"));
    let mut login = runtime.login(&agent, None).await.unwrap();
    let (url, status) = drain_login(&mut login).await;
    assert_eq!(url.as_deref(), Some("https://auth.example.com/oauth?login"));
    // `Finished` is the re-read `account/read` after the completion
    // notification, not an assumption.
    match status.expect("no Finished event") {
        AuthStatus::Authenticated {
            kind: AuthKind::Subscription,
            account,
        } => assert_eq!(account.unwrap().plan.as_deref(), Some("edu")),
        other => panic!("expected a chatgpt login, got {other:?}"),
    }
}

#[tokio::test]
async fn login_cancel_aborts_in_protocol_and_still_finishes() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at(
        "codex",
        login_wrapper("login-cancel", "--auth-file --login-slow"),
    );
    let mut login = runtime.login(&agent, None).await.unwrap();
    // The URL arrives with the session (login/start already answered);
    // cancel sends `account/login/cancel` before the fixture ever completes.
    login.cancel.cancel();
    let (url, status) = drain_login(&mut login).await;
    assert!(url.is_some(), "no OpenUrl before cancel");
    match status.expect("no Finished event") {
        AuthStatus::Unauthenticated { .. } => {}
        other => panic!("expected unauthenticated after cancel, got {other:?}"),
    }
}
