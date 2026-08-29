//! The ACP adapter driven end to end through the public interface, against
//! the recorded fixture agent (tests/fixtures/acp/fixture.mjs; needs `node`).

use std::path::Path;
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthStatus, Capability, ChoiceId, ConfigId, ConfigValue,
    DeliveryKind, Event, EventKind, Events, Input, LoginMethod, McpServer, McpTransport,
    PermissionChoice, QuestionAnswer, Request, ResumeToken, Runtime, Session, SessionOptions,
    StopReason,
};

fn fixture(extra: &[&str]) -> AgentInstallation {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/acp/fixture.mjs");
    let mut args = vec![script.to_string_lossy().into_owned()];
    args.extend(extra.iter().map(|s| s.to_string()));
    AgentInstallation::acp("fixture", "node", args)
}

async fn open(extra: &[&str]) -> (Session, Events) {
    let runtime = Runtime::new();
    runtime
        .open(
            &fixture(extra),
            SessionOptions::in_dir(std::env::temp_dir()),
        )
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
async fn a_full_turn_maps_every_update_kind() {
    let (session, mut events) = open(&[]).await;
    session.prompt("hi").await.unwrap();

    let mut text = String::new();
    let mut thoughts = String::new();
    let mut open_messages = std::collections::BTreeSet::new();
    let mut tool_states = Vec::new();
    let mut plan_steps = Vec::new();
    let mut usage = None;
    let mut diagnostics = 0;
    let mut raw_update = false;
    let mut commands_seen = false;
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta {
                text: t,
                message_id,
            } => {
                text.push_str(&t);
                open_messages.insert(message_id);
            }
            EventKind::ReasoningDelta {
                text: t,
                message_id,
            } => {
                thoughts.push_str(&t);
                open_messages.insert(message_id);
            }
            EventKind::MessageEnded { message_id } => {
                open_messages.remove(&message_id);
            }
            EventKind::ToolUpdated(tool) => tool_states.push(tool),
            EventKind::PlanUpdated { entries } => {
                plan_steps = entries.into_iter().map(|e| e.text).collect();
            }
            EventKind::ContextUsage {
                used_tokens,
                window_tokens,
                cost_usd,
            } => usage = Some((used_tokens, window_tokens, cost_usd)),
            EventKind::SessionUpdated(info) => {
                commands_seen |= info.details.commands.iter().any(|c| c.name == "compact");
            }
            EventKind::Diagnostic(_) => {
                diagnostics += 1;
                raw_update |= event.extensions.contains_key("acp/raw_update");
            }
            EventKind::RequestOpened(Request::Permission(request)) => {
                assert_eq!(request.tool.title, "Run tests");
                assert_eq!(
                    request.options,
                    vec![PermissionChoice::AllowOnce, PermissionChoice::DenyOnce]
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
                // ACP has no end-of-message signal; the engine closes every
                // streamed message before ending the turn.
                assert!(open_messages.is_empty(), "unended: {open_messages:?}");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(text, "Hello perm=selected ");
    assert_eq!(thoughts, "thinking…");
    assert_eq!(plan_steps, vec!["step 1"]);
    assert_eq!(usage, Some((1200, Some(200_000), Some(0.01))));
    assert!(commands_seen, "available commands became SessionUpdated");
    assert!(raw_update, "unknown update kind kept raw in extensions");
    assert!(diagnostics >= 2, "unknown kind + extension notification");

    // The edit tool: pending with a diff, then completed with output.
    let edit: Vec<_> = tool_states
        .iter()
        .filter(|t| t.title == "Edit main.rs")
        .collect();
    assert_eq!(edit[0].diffs[0].new_text, "b");
    assert_eq!(edit[0].locations, vec![std::path::PathBuf::from("main.rs")]);
    assert!(edit.last().unwrap().output.as_deref() == Some("done"));

    // Late noise after the turn opens an agent-originated turn.
    loop {
        if let EventKind::TurnStarted { origin } = next(&mut events).await.kind {
            assert_eq!(origin, anyagent::TurnOrigin::Agent);
            break;
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn the_handshake_fills_session_info() {
    let (session, _events) = open(&[]).await;
    let info = session.info();
    assert_eq!(info.details.version.as_deref(), Some("0.0.1"));
    for capability in [
        Capability::Permissions,
        Capability::Steer,
        Capability::Images,
        Capability::Resume,
    ] {
        assert!(info.details.capabilities.supports(capability));
    }
    let ids: Vec<_> = info
        .details
        .config_options
        .iter()
        .map(|o| o.id.as_str().to_owned())
        .collect();
    assert_eq!(ids, vec!["mode", "model"]);
    assert_eq!(info.resume_token.unwrap().as_str(), "sess-1");
    session.close().await.unwrap();
}

#[tokio::test]
async fn probe_reports_details_and_waits_for_late_commands() {
    let details = Runtime::new()
        .probe(&fixture(&["--commands-on-open"]))
        .await
        .unwrap();
    // The same details an open reports: version and config options.
    assert_eq!(details.version.as_deref(), Some("0.0.1"));
    let ids: Vec<_> = details
        .config_options
        .iter()
        .map(|o| o.id.as_str().to_owned())
        .collect();
    assert_eq!(ids, vec!["mode", "model"]);
    // The command list only arrives as a `SessionUpdated` after session/new;
    // seeing it proves probe waited for it instead of returning empty.
    assert!(
        details.commands.iter().any(|c| c.name == "compact"),
        "probe must wait for the ACP available-commands update"
    );
}

#[tokio::test]
async fn cancel_reaches_the_agent_and_ends_the_turn() {
    let (session, mut events) = open(&[]).await;
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

#[tokio::test]
async fn steering_mid_turn_is_accepted() {
    let (session, mut events) = open(&[]).await;
    let first = session.prompt("one").await.unwrap();
    let DeliveryKind::Started { turn_id } = first.kind else {
        panic!("expected Started");
    };
    let steered = session.prompt("two").await.unwrap();
    assert_eq!(steered.kind, DeliveryKind::Steered { turn_id });
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap()
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn attachments_inline_images_and_reference_paths() {
    let dir = std::env::temp_dir().join(format!("anyagent-att-acp-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("shot.png"), b"\x89PNG\r\n\x1a\ndata").unwrap();
    std::fs::write(dir.join("report.pdf"), b"%PDF-1.7 data").unwrap();
    let (session, mut events) = open(&[]).await;
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
async fn agent_death_mid_turn_fails_the_turn() {
    let (session, mut events) = open(&["--eof"]).await;
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
async fn configuring_the_mode_round_trips_and_updates_the_session() {
    let (session, mut events) = open(&[]).await;
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
    // A value the agent never offered, and an option it never advertised,
    // are typed rejections that never reach the wire.
    let bad_value = session.configure("mode", "yolo").await;
    assert!(matches!(
        bad_value,
        Err(AgentError::InvalidConfiguration(_))
    ));
    let unknown = session.configure("nope", "x").await;
    assert!(matches!(unknown, Err(AgentError::InvalidConfiguration(_))));
    session.close().await.unwrap();
}

#[tokio::test]
async fn overlapping_configures_both_apply() {
    // The model reply is delayed past the mode round-trip, so both changes
    // are in flight at once; each confirmation must land, not just the last.
    let (session, mut events) = open(&["--config-slow=150"]).await;
    session.configure("model", "opus").await.unwrap();
    session.configure("mode", "plan").await.unwrap();
    let (mut mode, mut model) = (false, false);
    while !(mode && model) {
        let event = next(&mut events).await;
        if let EventKind::SessionUpdated(info) = event.kind {
            let get = |id: &str| info.configuration.options.get(&ConfigId::new(id)).cloned();
            mode |= get("mode") == Some(ConfigValue::Text("plan".into()));
            model |= get("model") == Some(ConfigValue::Text("opus".into()));
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn mcp_servers_forward_when_the_transport_is_supported() {
    let runtime = Runtime::new();
    let (session, mut events) = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir())
                .mcp_server(
                    McpServer::http("voice", "http://127.0.0.1:1/mcp")
                        .with("Authorization", "Bearer x"),
                )
                .mcp_server(McpServer::stdio("tool", "/bin/echo", ["hi"])),
        )
        .await
        .unwrap();
    assert_eq!(
        session.info().details.capabilities.mcp_transports,
        vec![McpTransport::Stdio, McpTransport::Http]
    );
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
async fn an_unsupported_mcp_transport_is_refused() {
    let runtime = Runtime::new();
    let error = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir())
                .mcp_server(McpServer::sse("events", "http://127.0.0.1:1/sse")),
        )
        .await
        .err()
        .expect("an sse declaration must be refused, not dropped");
    assert!(matches!(error, AgentError::UnsupportedFeature(_)));
}

#[tokio::test]
async fn resuming_an_agent_without_load_session_fails() {
    let runtime = Runtime::new();
    let error = runtime
        .open(
            &fixture(&["--no-load"]),
            SessionOptions::in_dir(std::env::temp_dir()).resume(ResumeToken::new("sess-1")),
        )
        .await
        .err()
        .expect("resume must be refused, not silently replaced");
    assert!(matches!(error, AgentError::ResumeFailed(_)));
}

#[tokio::test]
async fn forking_an_acp_agent_fails_typed() {
    let runtime = Runtime::new();
    let error = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir())
                .fork_from(ResumeToken::new("sess-1"), None),
        )
        .await
        .err()
        .expect("fork must be refused, not silently replaced");
    assert!(matches!(error, AgentError::UnsupportedFeature(_)));
}

#[tokio::test]
async fn auth_required_carries_runnable_login_methods() {
    let runtime = Runtime::new();
    let result = runtime
        .open(
            &fixture(&["--auth-required"]),
            SessionOptions::in_dir(std::env::temp_dir()),
        )
        .await;
    let Err(AgentError::AuthRequired { login }) = result else {
        panic!("expected AuthRequired, got {:?}", result.err());
    };
    let LoginMethod::Terminal { command, .. } = &login[0] else {
        panic!("expected a terminal method");
    };
    assert_eq!(command[0], "node");
    assert_eq!(command[1..], ["auth".to_string(), "login".to_string()]);
}

#[tokio::test]
async fn probe_reports_a_logged_out_agent_instead_of_failing() {
    // `open` refuses a logged-out agent; `probe` inspects it and answers.
    let details = Runtime::new()
        .probe(&fixture(&["--auth-required"]))
        .await
        .expect("probe must report a missing login, not fail on it");
    let AuthStatus::Unauthenticated { login } = &details.auth else {
        panic!("expected Unauthenticated, got {:?}", details.auth);
    };
    let LoginMethod::Terminal { command, .. } = &login[0] else {
        panic!("expected a terminal method");
    };
    assert_eq!(command[1..], ["auth".to_string(), "login".to_string()]);
    // Nothing is advertised until the agent has a login.
    assert!(details.config_options.is_empty());
    assert!(details.commands.is_empty());
}

#[tokio::test]
async fn a_flood_of_chunks_arrives_completely_and_in_order() {
    let (session, mut events) = open(&["--flood=500"]).await;
    session.prompt("hi").await.unwrap();
    let mut chunks = 0;
    let mut last_seq = 0;
    loop {
        let event = next(&mut events).await;
        assert!(event.sequence > last_seq);
        last_seq = event.sequence;
        match event.kind {
            EventKind::TextDelta { text, .. } if text.starts_with('x') => chunks += 1,
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap()
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert_eq!(chunks, 500);
    session.close().await.unwrap();
}

#[tokio::test]
async fn losing_auth_mid_session_fails_the_turn_and_closes() {
    let (session, mut events) = open(&[]).await;
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
    let LoginMethod::Terminal { command, .. } = &login[0] else {
        panic!("expected a terminal method");
    };
    assert_eq!(command[1..], ["auth".to_string(), "login".to_string()]);
    // The stream closes and the session is gone.
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
async fn a_plain_prompt_error_fails_the_turn_and_the_session_survives() {
    let (session, mut events) = open(&[]).await;
    session.prompt("die-rpc").await.unwrap();
    loop {
        let event = next(&mut events).await;
        if let EventKind::TurnEnded { stop, .. } = event.kind {
            let StopReason::Failed { message } = stop else {
                panic!("expected Failed, got {stop:?}");
            };
            assert_eq!(message, "kaput (-32603)");
            break;
        }
    }
    // The session is still usable.
    session.prompt("hi").await.unwrap();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap()
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
async fn creation_time_config_applies_at_open_and_a_refusal_fails_it() {
    let runtime = Runtime::new();
    let (session, _events) = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir())
                .configure("mode", "plan")
                .configure("model", "sonnet"),
        )
        .await
        .unwrap();
    let options = session.info().configuration.options;
    assert_eq!(
        options.get(&ConfigId::new("mode")),
        Some(&ConfigValue::Text("plan".into()))
    );
    session.close().await.unwrap();

    let refused = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir()).configure("bogus", "x"),
        )
        .await
        .map(|_| ());
    let Err(AgentError::InvalidConfiguration(message)) = refused else {
        panic!("expected InvalidConfiguration");
    };
    assert!(message.contains("bogus"), "got: {message}");
}

#[tokio::test]
async fn grok_prompt_complete_ends_a_hung_turn_and_stale_ids_are_ignored() {
    let (session, mut events) = open(&[]).await;
    session.prompt("grok-hang").await.unwrap();
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::TurnEnded { stop, .. } => {
                // The stale frame carried `refusal`; only the frame echoing
                // our promptId may settle the turn.
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
    assert_eq!(text, "grok ");

    // The abandoned session/prompt RPC never responds; the session still
    // runs full turns afterwards.
    session.prompt("hi").await.unwrap();
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(Request::Permission(request)) => {
                session.answer(request.id, allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    assert!(text.starts_with("Hello"), "got: {text}");
}

#[tokio::test]
async fn grok_questions_surface_typed_and_answers_return_labels() {
    let (session, mut events) = open(&[]).await;
    session.prompt("grok-question").await.unwrap();
    let mut text = String::new();
    loop {
        let event = next(&mut events).await;
        match event.kind {
            EventKind::RequestOpened(Request::Question(request)) => {
                let q = &request.questions[0];
                assert_eq!(q.text, "Pick a fruit");
                assert!(!q.multi_select);
                assert_eq!(q.choices[0].label, "Grape");
                assert_eq!(q.choices[0].description.as_deref(), Some("purple"));
                // Mango has no id: the label doubles as the choice id.
                assert_eq!(q.choices[1].id.as_str(), "Mango");
                session
                    .answer(
                        request.id,
                        Answer::Question(vec![QuestionAnswer::Choices(vec![ChoiceId::new("g")])]),
                    )
                    .await
                    .unwrap();
            }
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    // The answer went back keyed by question text, valued by option LABEL.
    assert_eq!(text, r#"q={"Pick a fruit":["Grape"]} "#);
}

#[tokio::test]
async fn grok_first_class_models_surface_as_the_model_option_and_switch_via_set_model() {
    let (session, mut events) = open(&["--grok-models"]).await;
    let details = session.info().details;
    let model = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .expect("model option from the first-class models state");
    assert!(model.live);
    assert_eq!(model.current, Some(ConfigValue::Text("grok-4.5".into())));
    let anyagent::ConfigKind::Select { choices } = &model.kind else {
        panic!("expected select");
    };
    assert_eq!(choices.len(), 2);
    assert_eq!(choices[0].label, "Grok 4.5");
    assert_eq!(choices[0].description.as_deref(), Some("fast"));

    // The fixture rejects session/set_config_option for models under
    // --grok-models, so this passing proves the session/set_model route.
    session.configure("model", "grok-4.6").await.unwrap();
    loop {
        let event = next(&mut events).await;
        if let EventKind::SessionUpdated(info) = event.kind {
            assert_eq!(
                info.configuration.options.get(&ConfigId::new("model")),
                Some(&ConfigValue::Text("grok-4.6".into()))
            );
            break;
        }
    }
}

#[tokio::test]
async fn config_home_on_an_agent_without_a_known_var_is_refused() {
    let runtime = Runtime::new();
    let err = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir()).config_home(std::env::temp_dir()),
        )
        .await
        .err();
    assert!(
        matches!(err, Some(AgentError::InvalidConfiguration(_))),
        "expected InvalidConfiguration, got {err:?}"
    );
}

/// `record_wire` tees the ACP JSON-RPC wire too, both directions and including
/// the handshake, as one valid JSON object per line.
#[tokio::test]
async fn record_wire_tees_the_acp_wire_including_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("wire.jsonl");
    let runtime = Runtime::new();
    let (session, mut events) = runtime
        .open(
            &fixture(&[]),
            SessionOptions::in_dir(std::env::temp_dir()).record_wire(&log),
        )
        .await
        .unwrap();
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
    session.close().await.unwrap();

    let mut lines = Vec::new();
    for _ in 0..40 {
        let body = std::fs::read_to_string(&log).unwrap_or_default();
        if body.lines().count() >= 5 {
            lines = body
                .lines()
                .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("valid JSON line"))
                .collect();
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(lines.len() >= 5, "too few frames recorded: {}", lines.len());
    assert!(
        lines.iter().any(|f| f["dir"] == "out"),
        "no outbound frames recorded"
    );
    assert!(
        lines.iter().any(|f| f["dir"] == "in"),
        "no inbound frames recorded"
    );
    assert!(
        lines
            .iter()
            .any(|f| f["dir"] == "out" && f["frame"]["method"] == "initialize"),
        "handshake initialize was not recorded"
    );
}
