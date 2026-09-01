//! The native pi adapter driven end to end through the public interface,
//! against the fixture agent (tests/fixtures/pi/fixture.mjs; needs `node`).
//! A wrapper script pins the catalog's `pi` id to the fixture.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, AgentInstallation, Answer, AuthKind, AuthStatus, Capability, ConfigKind,
    ConfigValue, DeliveryKind, Event, EventKind, Events, McpServer, QuestionAnswer, Request,
    ResumeToken, Runtime, Session, SessionOptions, StopReason, ToolInput, ToolKind, ToolStatus,
};

/// A `pi` stand-in: a script that execs the fixture with scenario flags,
/// ignoring the real launch args appended after them.
fn wrapper(name: &str, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pi/fixture.mjs");
    let dir = std::env::temp_dir().join(format!("anyagent-pi-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("pi");
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
    let agent = AgentInstallation::at("pi", wrapper(name, flags));
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

#[tokio::test]
async fn handshake_advertises_state_models_levels_and_commands() {
    let (session, _events) = open("handshake", "").await;
    let info = session.info();
    let details = &info.details;

    assert_eq!(details.version.as_deref(), Some("0.84.4"));
    assert_eq!(
        details.auth,
        AuthStatus::Authenticated {
            kind: AuthKind::Subscription,
            account: None,
        }
    );
    for capability in [
        Capability::Steer,
        Capability::Images,
        Capability::Resume,
        Capability::SlashCommands,
        Capability::ContextUsage,
        Capability::Questions,
    ] {
        assert!(
            details.capabilities.supports(capability.clone()),
            "{capability:?}"
        );
    }
    // Nothing pi's wire cannot actually do.
    for capability in [
        Capability::Permissions,
        Capability::Rollback,
        Capability::Fork,
        Capability::PlanUsage,
        Capability::Subagents,
    ] {
        assert!(
            !details.capabilities.supports(capability.clone()),
            "{capability:?}"
        );
    }

    let model = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .expect("a model option");
    assert_eq!(
        model.current,
        Some(ConfigValue::Text("openrouter/nemo-1".into()))
    );
    let ConfigKind::Select { choices } = &model.kind else {
        panic!("model is a select")
    };
    // Valued `provider/modelId`, because `set_model` needs both halves.
    let values: Vec<&str> = choices.iter().map(|c| c.value.as_str()).collect();
    assert_eq!(values, vec!["openrouter/nemo-1", "anthropic/claude-x"]);

    let thinking = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "thinking")
        .expect("a thinking option");
    let ConfigKind::Select { choices } = &thinking.kind else {
        panic!("thinking is a select")
    };
    assert_eq!(
        choices.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(),
        vec!["off", "low", "medium"]
    );

    let names: Vec<&str> = details.commands.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["review", "skill:release"]);
    // The session file is the resume handle, and one exists from the start.
    assert!(
        info.resume_token
            .as_ref()
            .is_some_and(|t| t.as_str().ends_with("sessions/s1.jsonl")),
        "{:?}",
        info.resume_token
    );
}

#[tokio::test]
async fn a_turn_streams_text_reasoning_tools_and_usage_then_settles() {
    let (session, mut events) = open("turn", "").await;
    let delivery = session.prompt("run the tool").await.unwrap();
    assert!(matches!(delivery.kind, DeliveryKind::Started { .. }));
    let kinds = drain_turn(&mut events).await;

    assert_eq!(text_of(&kinds), "ready [m1]");
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::ReasoningDelta { .. })),
        "thinking deltas are reasoning"
    );
    // pi replays our own prompt as a user message; the engine already has it.
    assert!(
        !kinds
            .iter()
            .any(|k| matches!(k, EventKind::UserMessage { .. })),
        "a prompt replay is not provider-originated content"
    );

    let tools: Vec<_> = kinds
        .iter()
        .filter_map(|k| match k {
            EventKind::ToolUpdated(tool) => Some(tool),
            _ => None,
        })
        .collect();
    let first = tools.first().expect("a tool call");
    assert_eq!(first.kind, ToolKind::Execute);
    assert_eq!(first.status, ToolStatus::Pending);
    let last = tools.last().unwrap();
    assert_eq!(last.status, ToolStatus::Completed);
    assert_eq!(last.title, "bash echo hi");
    assert_eq!(
        last.input,
        ToolInput::Command {
            command: "echo hi".into(),
            cwd: None,
        }
    );
    assert_eq!(last.output.as_deref(), Some("one\ntwo\n"));

    // `partialResult` accumulates, so each delta is only the new tail.
    let output: String = kinds
        .iter()
        .filter_map(|k| match k {
            EventKind::ToolOutputDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(output, "one\ntwo\n");

    assert!(kinds.iter().any(|k| matches!(
        k,
        EventKind::ContextUsage {
            used_tokens: 1234,
            window_tokens: Some(100000),
            ..
        }
    )));
    assert!(
        kinds
            .iter()
            .any(|k| matches!(k, EventKind::MessageEnded { .. }))
    );
    assert_eq!(
        kinds.last().unwrap(),
        &EventKind::TurnEnded {
            stop: StopReason::Completed {
                source: anyagent::CompletionSource::Protocol,
            },
            background: Vec::new(),
        },
        "agent_settled ends the turn deterministically"
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn a_mid_turn_prompt_steers_the_running_turn() {
    let (session, mut events) = open("steer", "").await;
    let first = session.prompt("sleep in a tool").await.unwrap();
    let DeliveryKind::Started { turn_id } = first.kind else {
        panic!("expected Started")
    };
    // Wait until the run is really streaming before steering into it.
    loop {
        if matches!(next(&mut events).await.kind, EventKind::ToolUpdated(_)) {
            break;
        }
    }
    let steered = session.prompt("say it now").await.unwrap();
    assert_eq!(steered.kind, DeliveryKind::Steered { turn_id });

    // Cancelling must also drop the steer from pi's own queue: an `abort`
    // alone would resume it as a fresh run and settle only after it spoke.
    session.cancel(false).await.unwrap();
    let mut kinds = Vec::new();
    let stop = loop {
        let kind = next(&mut events).await.kind;
        if let EventKind::TurnEnded { stop, .. } = kind {
            break stop;
        }
        kinds.push(kind);
    };
    assert_eq!(stop, StopReason::Cancelled);
    assert!(
        !text_of(&kinds).contains("[m2]"),
        "the steered message ran anyway: {kinds:?}"
    );
}

#[tokio::test]
async fn cancel_ends_the_turn_as_cancelled() {
    let (session, mut events) = open("cancel", "").await;
    session.prompt("sleep in a tool").await.unwrap();
    loop {
        if matches!(next(&mut events).await.kind, EventKind::ToolUpdated(_)) {
            break;
        }
    }
    session.cancel(false).await.unwrap();

    let stop = loop {
        if let EventKind::TurnEnded { stop, .. } = next(&mut events).await.kind {
            break stop;
        }
    };
    // pi reports an abort as `error` + "aborted"; the adapter knows it asked.
    assert_eq!(stop, StopReason::Cancelled);
}

#[tokio::test]
async fn an_extension_dialog_becomes_a_question_and_the_answer_goes_back() {
    let (session, mut events) = open("dialog", "").await;
    session.prompt("ask me something").await.unwrap();

    let request = loop {
        if let EventKind::RequestOpened(Request::Question(request)) = next(&mut events).await.kind {
            break request;
        }
    };
    let question = &request.questions[0];
    assert_eq!(question.text, "Pick a colour");
    assert!(!question.allows_free_text);
    assert_eq!(
        question
            .choices
            .iter()
            .map(|c| c.label.as_str())
            .collect::<Vec<_>>(),
        vec!["Red", "Green"]
    );
    session
        .answer(
            request.id,
            Answer::Question(vec![QuestionAnswer::Choices(vec![
                question.choices[1].id.clone(),
            ])]),
        )
        .await
        .unwrap();

    let kinds = drain_turn(&mut events).await;
    // The extension echoes the pick back as a `notify`, which is a diagnostic.
    assert!(
        kinds.iter().any(|k| matches!(
            k,
            EventKind::Diagnostic(d) if d.message == "picked Green"
        )),
        "{kinds:?}"
    );
}

#[tokio::test]
async fn a_confirm_dialog_round_trips_both_verdicts_and_carries_its_timeout() {
    for (choice, echo) in [(0, "confirmed true"), (1, "confirmed false")] {
        let (session, mut events) = open("confirm", "").await;
        session.prompt("confirm it").await.unwrap();

        let (request, extensions) = loop {
            let event = next(&mut events).await;
            if let EventKind::RequestOpened(Request::Question(request)) = event.kind {
                break (request, event.extensions);
            }
        };
        // pi resolves the dialog itself when this expires; the app gets it
        // as an extension so it can withdraw the question in time.
        assert_eq!(
            extensions.get("pi/timeout_ms").and_then(|v| v.as_u64()),
            Some(5000)
        );
        let question = &request.questions[0];
        assert_eq!(question.text, "Delete it?\n\nThis cannot be undone.");
        assert_eq!(
            question
                .choices
                .iter()
                .map(|c| c.label.as_str())
                .collect::<Vec<_>>(),
            vec!["Yes", "No"]
        );
        session
            .answer(
                request.id,
                Answer::Question(vec![QuestionAnswer::Choices(vec![
                    question.choices[choice].id.clone(),
                ])]),
            )
            .await
            .unwrap();

        // The fixture echoes what pi's extension received: `confirmed` must
        // arrive as a boolean under its own key, not as `value`.
        let kinds = drain_turn(&mut events).await;
        assert!(
            kinds.iter().any(|k| matches!(
                k,
                EventKind::Diagnostic(d) if d.message == echo
            )),
            "{kinds:?}"
        );
    }
}

#[tokio::test]
async fn a_model_change_applies_and_re_reads_the_new_model_levels() {
    let (session, mut events) = open("configure", "").await;
    session
        .configure("model", ConfigValue::Text("anthropic/claude-x".into()))
        .await
        .unwrap();

    // Two updates: the applied model, then the levels that came with it.
    let mut levels = Vec::new();
    for _ in 0..2 {
        let EventKind::SessionUpdated(info) = next(&mut events).await.kind else {
            continue;
        };
        levels = match info
            .details
            .config_options
            .iter()
            .find(|o| o.id.as_str() == "thinking")
        {
            Some(option) => match &option.kind {
                ConfigKind::Select { choices } => choices.iter().map(|c| c.value.clone()).collect(),
                _ => Vec::new(),
            },
            None => Vec::new(),
        };
    }
    assert_eq!(levels, vec!["off", "high", "max"]);
    let info = session.info();
    assert_eq!(
        info.configuration.options.get(&"model".into()),
        Some(&ConfigValue::Text("anthropic/claude-x".into()))
    );
    // The old level (`medium`) is not one the new model offers, so the
    // adapter stops claiming it rather than advertising a stale value.
    let thinking = info
        .details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "thinking")
        .expect("a thinking option");
    assert_eq!(thinking.current, None);
    assert_eq!(info.configuration.options.get(&"thinking".into()), None);
}

#[tokio::test]
async fn a_failed_model_turn_ends_the_turn_as_failed() {
    let (session, mut events) = open("fail", "").await;
    session.prompt("please fail").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert_eq!(
        kinds.last().unwrap(),
        &EventKind::TurnEnded {
            stop: StopReason::Failed {
                message: "the provider refused".into(),
            },
            background: Vec::new(),
        }
    );
}

#[tokio::test]
async fn a_refused_prompt_fails_the_turn_the_engine_already_started() {
    let (session, mut events) = open("refused", "--reject-prompt").await;
    session.prompt("anything").await.unwrap();
    let kinds = drain_turn(&mut events).await;
    assert!(
        matches!(
            kinds.last().unwrap(),
            EventKind::TurnEnded {
                stop: StopReason::Failed { message },
                ..
            } if message.contains("already processing")
        ),
        "{kinds:?}"
    );
}

#[tokio::test]
async fn logged_out_is_reported_from_pi_s_own_readiness_check() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("pi", wrapper("logged-out", "--logged-out"));
    let auth = runtime.probe_auth(&agent).await.unwrap();
    let AuthStatus::Unauthenticated { login } = auth else {
        panic!("expected Unauthenticated, got {auth:?}")
    };
    // Login is pi's own TUI, so the runnable methods are the API-key vars.
    assert!(
        login.iter().any(
            |m| matches!(m, anyagent::LoginMethod::EnvVar { name } if name == "OPENROUTER_API_KEY")
        ),
        "{login:?}"
    );
}

#[tokio::test]
async fn an_api_key_login_is_reported_as_one() {
    let runtime = Runtime::new();
    let agent = AgentInstallation::at("pi", wrapper("api-key", "--api-key"));
    assert_eq!(
        runtime.probe_auth(&agent).await.unwrap(),
        AuthStatus::Authenticated {
            kind: AuthKind::ApiKey,
            account: None,
        }
    );
}

#[tokio::test]
async fn resume_binds_the_session_file_and_config_home_reaches_the_child() {
    let token = ResumeToken::new("/tmp/anyagent-pi-resume/s9.jsonl");
    let options = SessionOptions::in_dir(std::env::temp_dir()).resume(token.clone());
    let (session, _events) = open_with("resume", "", options).await.unwrap();
    assert_eq!(session.info().resume_token, Some(token));

    // `PI_CODING_AGENT_DIR` is pi's config home; the fixture builds its
    // session path from whatever it received.
    let dir = std::env::temp_dir().join("anyagent-pi-home");
    let options = SessionOptions::in_dir(std::env::temp_dir()).config_home(&dir);
    let (session, _events) = open_with("home", "", options).await.unwrap();
    assert_eq!(
        session.info().resume_token.map(|t| t.as_str().to_owned()),
        Some(dir.join("sessions/s1.jsonl").display().to_string())
    );
}

#[tokio::test]
async fn unsupported_starts_and_declarations_are_refused_typed() {
    let options = SessionOptions::in_dir(std::env::temp_dir())
        .fork_from(ResumeToken::new("/tmp/s.jsonl"), None);
    assert!(matches!(
        open_with("fork", "", options).await,
        Err(AgentError::UnsupportedFeature(_))
    ));

    let options = SessionOptions::in_dir(std::env::temp_dir()).mcp_server(McpServer::stdio(
        "tools",
        "/bin/echo",
        ["hi"],
    ));
    assert!(matches!(
        open_with("mcp", "", options).await,
        Err(AgentError::UnsupportedFeature(_))
    ));

    let options = SessionOptions::in_dir(std::env::temp_dir()).configure("sandbox", "strict");
    assert!(matches!(
        open_with("bad-option", "", options).await,
        Err(AgentError::InvalidConfiguration(_))
    ));

    // A model value with a missing half never reaches the CLI.
    for bad in ["openrouter/", "/nemo-1", "nemo-1"] {
        let options = SessionOptions::in_dir(std::env::temp_dir()).configure("model", bad);
        assert!(
            matches!(
                open_with("bad-model", "", options).await,
                Err(AgentError::InvalidConfiguration(_))
            ),
            "`{bad}` was accepted"
        );
    }
}
