//! The native antigravity adapter driven end to end through the public
//! interface, against the fixture agent (tests/fixtures/antigravity/
//! fixture.mjs; needs `node`). A wrapper script pins the catalog's
//! `antigravity` id to the fixture.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, AuthKind, AuthStatus, Capability, ConfigKind, ConfigValue,
    DiagnosticLevel, Event, EventKind, Events, LoginMethod, McpServer, PermissionMode, ResumeToken,
    Runtime, Session, SessionOptions, StopReason, ToolInput, ToolKind, ToolStatus,
};

/// An `agy` stand-in: a script that execs the fixture with scenario flags
/// before the real launch args.
fn wrapper(name: &str, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/antigravity/fixture.mjs");
    let dir = std::env::temp_dir().join(format!("anyagent-agy-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("agy");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nexec node '{}' {flags} \"$@\"\n",
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
    let agent = AgentInstallation::at("antigravity", wrapper(name, flags));
    Runtime::new().open(&agent, options).await
}

async fn open(name: &str) -> (Session, Events) {
    open_with(name, "", SessionOptions::in_dir(std::env::temp_dir()))
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

/// Drives one turn to its end, returning every event kind it produced.
async fn drain_turn(events: &mut Events) -> Vec<EventKind> {
    let mut kinds = Vec::new();
    loop {
        let kind = next(events).await.kind;
        let done = matches!(kind, EventKind::TurnEnded { .. });
        kinds.push(kind);
        if done {
            return kinds;
        }
    }
}

fn text_of(kinds: &[EventKind]) -> String {
    kinds
        .iter()
        .filter_map(|k| match k {
            EventKind::TextDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn tools_of(kinds: &[EventKind]) -> Vec<&anyagent::ToolUpdate> {
    kinds
        .iter()
        .filter_map(|k| match k {
            EventKind::ToolUpdated(tool) => Some(tool),
            _ => None,
        })
        .collect()
}

fn ended(kinds: &[EventKind]) -> &StopReason {
    match kinds.last() {
        Some(EventKind::TurnEnded { stop, .. }) => stop,
        other => panic!("turn did not end: {other:?}"),
    }
}

/// Handshake: version and login from the side processes and `init`, the
/// honest capability set, and the two creation-only selects.
#[tokio::test]
async fn handshake_advertises_version_auth_capabilities_and_options() {
    let (session, _events) = open("handshake").await;
    let info = session.info();
    let details = &info.details;
    assert_eq!(details.version.as_deref(), Some("1.1.24"));
    assert_eq!(
        details.auth,
        AuthStatus::Authenticated {
            kind: AuthKind::Subscription,
            account: None,
        }
    );
    for capability in [Capability::Resume, Capability::ContextUsage] {
        assert!(
            details.capabilities.supports(capability.clone()),
            "{capability:?}"
        );
    }
    for capability in [
        Capability::Permissions,
        Capability::Questions,
        Capability::Steer,
        Capability::Fork,
        Capability::Rollback,
    ] {
        assert!(
            !details.capabilities.supports(capability.clone()),
            "{capability:?}"
        );
    }
    let option = |id: &str| {
        details
            .config_options
            .iter()
            .find(|o| o.id.as_str() == id)
            .unwrap_or_else(|| panic!("no `{id}` option"))
    };
    let ConfigKind::Select { choices } = &option("model").kind else {
        panic!("model is a select");
    };
    assert_eq!(choices[0].value, "gemini-3.8-flash-high");
    assert_eq!(choices[0].label, "Gemini 3.8 Flash (High)");
    assert!(!option("model").live && !option("mode").live);
    assert_eq!(info.resume_token, Some(ResumeToken::new("c1")));
    assert!(details.commands.is_empty());
    session.close().await.unwrap();
}

/// A turn streams text under one message, reports usage, and ends once.
#[tokio::test]
async fn a_turn_streams_text_reports_usage_and_ends_once() {
    let (session, mut events) = open("pong").await;
    session.prompt("chunks please").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert_eq!(text_of(&kinds), "I have created the file.\n");
    let ids: Vec<_> = kinds
        .iter()
        .filter_map(|k| match k {
            EventKind::TextDelta { message_id, .. } => Some(message_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(ids[0], ids[1], "both deltas belong to one message");
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::MessageEnded { message_id } if *message_id == ids[0]))
    );
    assert!(kinds.iter().any(|k| matches!(
        k,
        EventKind::ContextUsage {
            used_tokens: 13763,
            window_tokens: None,
            ..
        }
    )));
    assert!(matches!(ended(&kinds), StopReason::Completed { .. }));
    session.close().await.unwrap();
}

/// AutoApprove launches with skip-permissions and the tool runs to
/// completion with its output; Ask leaves the wire unable to prompt, so the
/// tool fails with the agent's own denial message.
#[tokio::test]
async fn tools_run_when_auto_approved_and_fail_denied_when_asking() {
    let auto =
        SessionOptions::in_dir(std::env::temp_dir()).permission_mode(PermissionMode::AutoApprove);
    let (session, mut events) = open_with("tool-auto", "", auto).await.unwrap();
    session.prompt("use a tool").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    let tools = tools_of(&kinds);
    assert_eq!(tools[0].status, ToolStatus::Running);
    assert_eq!(tools[0].kind, ToolKind::Execute);
    assert!(
        matches!(&tools[0].input, ToolInput::Command { command, .. } if command == "echo hello > probe.txt")
    );
    assert_eq!(tools[1].status, ToolStatus::Completed);
    assert_eq!(tools[1].id, tools[0].id, "one id across ACTIVE and DONE");
    assert_eq!(tools[1].output.as_deref(), Some("hello\n"));
    assert_eq!(text_of(&kinds), "Done: I wrote the file.\n");
    session.close().await.unwrap();

    let (session, mut events) = open("tool-ask").await;
    session.prompt("use a tool").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    let tools = tools_of(&kinds);
    assert_eq!(tools[1].status, ToolStatus::Failed);
    assert!(
        tools[1]
            .output
            .as_deref()
            .unwrap()
            .contains("user denied permission")
    );
    assert!(matches!(ended(&kinds), StopReason::Completed { .. }));
    session.close().await.unwrap();
}

/// A question the headless wire skipped surfaces as a warning, not a request.
#[tokio::test]
async fn a_skipped_question_is_a_diagnostic() {
    let (session, mut events) = open("ask").await;
    session.prompt("ask me something").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(kinds.iter().any(|k| matches!(
        k,
        EventKind::Diagnostic(d) if d.level == DiagnosticLevel::Warning && d.message.contains("skipped")
    )));
    assert!(
        !kinds
            .iter()
            .any(|k| matches!(k, EventKind::RequestOpened(_)))
    );
    session.close().await.unwrap();
}

/// Cancel kills the process, ends the turn Cancelled, and the next prompt
/// runs on a fresh process resumed on the same conversation.
#[tokio::test]
async fn cancel_kills_and_resumes_the_conversation() {
    let (session, mut events) = open("cancel").await;
    session.prompt("sleep a while").await.unwrap();
    loop {
        if let EventKind::ToolUpdated(tool) = next(&mut events).await.kind {
            assert_eq!(tool.status, ToolStatus::Running);
            break;
        }
    }
    session.cancel(true).await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(matches!(ended(&kinds), StopReason::Cancelled));

    session.prompt("recall").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    let text = text_of(&kinds);
    assert!(text.starts_with("recalled c1 "), "{text}");
    assert!(
        text.contains("--conversation"),
        "resumed on the same id: {text}"
    );
    assert!(matches!(ended(&kinds), StopReason::Completed { .. }));
    session.close().await.unwrap();
}

/// The kill ends the running tool: it is reported cancelled rather than
/// left in `background`, and the interrupted turn's usage does not ride
/// into the next one.
#[tokio::test]
async fn cancel_settles_the_tool_and_drops_its_usage() {
    let (session, mut events) = open("cancel-settle").await;
    session.prompt("sleep a while").await.unwrap();
    while !matches!(next(&mut events).await.kind, EventKind::ToolUpdated(_)) {}
    session.cancel(true).await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(
        matches!(tools_of(&kinds)[..], [t] if t.status == ToolStatus::Cancelled),
        "{kinds:?}"
    );
    assert!(
        matches!(kinds.last(), Some(EventKind::TurnEnded { background, .. }) if background.is_empty())
    );

    session.prompt("fail").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(
        !kinds
            .iter()
            .any(|k| matches!(k, EventKind::ContextUsage { .. })),
        "stale usage: {kinds:?}"
    );
    session.close().await.unwrap();
}

/// Resume passes the token as `--conversation`; the session keeps that token.
#[tokio::test]
async fn resume_passes_the_conversation() {
    let options = SessionOptions::in_dir(std::env::temp_dir()).resume(ResumeToken::new("abc"));
    let (session, mut events) = open_with("resume", "", options).await.unwrap();
    assert_eq!(session.info().resume_token, Some(ResumeToken::new("abc")));
    session.prompt("recall").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(text_of(&kinds).starts_with("recalled abc "));
    session.close().await.unwrap();
}

/// Model and mode are launch flags reported as the current selection; a bad
/// model, an unknown option, fork, MCP forwarding, and tool disabling are
/// all refused typed at open.
#[tokio::test]
async fn creation_time_config_is_launch_flags_and_the_rest_is_refused() {
    let dir = std::env::temp_dir();
    let options = SessionOptions::in_dir(&dir)
        .configure("model", "gemini-3.8-flash-low")
        .configure("mode", "plan");
    let (session, mut events) = open_with("config", "", options).await.unwrap();
    let info = session.info();
    assert_eq!(
        info.configuration.options.get(&"model".into()),
        Some(&ConfigValue::Text("gemini-3.8-flash-low".into()))
    );
    assert_eq!(
        info.configuration.options.get(&"mode".into()),
        Some(&ConfigValue::Text("plan".into()))
    );
    session.prompt("recall").await.unwrap();
    let text = text_of(&drain_turn(&mut events).await);
    assert!(
        text.contains("--model") && text.contains("--mode"),
        "{text}"
    );
    session.close().await.unwrap();

    let bad_model = open_with(
        "bad-model",
        "",
        SessionOptions::in_dir(&dir).configure("model", "nope"),
    )
    .await;
    assert!(
        matches!(bad_model, Err(AgentError::InvalidConfiguration(m)) if m.contains("invalid model selection"))
    );
    let effort = open_with(
        "effort",
        "",
        SessionOptions::in_dir(&dir).configure("effort", "high"),
    )
    .await;
    assert!(matches!(effort, Err(AgentError::InvalidConfiguration(_))));
    let fork = open_with(
        "fork",
        "",
        SessionOptions::in_dir(&dir).fork_from(ResumeToken::new("c1"), None),
    )
    .await;
    assert!(matches!(fork, Err(AgentError::UnsupportedFeature(_))));
    let mcp = open_with(
        "mcp",
        "",
        SessionOptions::in_dir(&dir).mcp_server(McpServer::stdio(
            "x",
            "/bin/true",
            Vec::<String>::new(),
        )),
    )
    .await;
    assert!(matches!(mcp, Err(AgentError::UnsupportedFeature(_))));
    let generate = Runtime::new()
        .generate(
            &AgentInstallation::at("antigravity", wrapper("generate", "")),
            SessionOptions::in_dir(&dir),
            "hi",
        )
        .await;
    assert!(
        matches!(generate, Err(AgentError::UnsupportedFeature(_))),
        "{generate:?}"
    );
}

/// Logged out, agy never reaches `init`: open fails `AuthRequired` naming
/// the executable itself as the login command.
#[tokio::test]
async fn logged_out_fails_typed_with_the_tui_as_login() {
    let exe = wrapper("logged-out", "--logged-out");
    let result = Runtime::new()
        .open(
            &AgentInstallation::at("antigravity", &exe),
            SessionOptions::in_dir(std::env::temp_dir()),
        )
        .await;
    let Err(AgentError::AuthRequired { login }) = result else {
        panic!("expected AuthRequired");
    };
    assert!(
        matches!(&login[0], LoginMethod::Terminal { command, .. } if command == &vec![exe.to_string_lossy().into_owned()])
    );
}

/// An ERROR result fails the turn with the agent's message; a subagent step
/// is a Subagent tool titled by its role.
#[tokio::test]
async fn failures_and_subagents_are_reported() {
    let (session, mut events) = open("fail").await;
    session.prompt("fail please").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(matches!(ended(&kinds), StopReason::Failed { message } if message == "model exploded"));

    session.prompt("run a subagent").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    let tools = tools_of(&kinds);
    assert_eq!(tools[0].kind, ToolKind::Subagent);
    assert_eq!(tools[0].title, "subagent Pong Responder");
    assert_eq!(tools[1].status, ToolStatus::Completed);
    assert!(matches!(ended(&kinds), StopReason::Completed { .. }));
    session.close().await.unwrap();
}

/// The process dying mid-turn ends the stream with `ProcessExited`.
#[tokio::test]
async fn a_dead_process_ends_the_stream() {
    let (session, mut events) = open("die").await;
    session.prompt("die now").await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.next().await {
            if let Err(e) = event {
                return Some(e);
            }
        }
        None
    })
    .await
    .expect("stream hung");
    assert!(
        matches!(outcome, Some(AgentError::ProcessExited { .. })),
        "{outcome:?}"
    );
    drop(session);
}
