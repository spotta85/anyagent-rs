//! Live feature matrix against the real installed harnesses — the checked-in
//! version of V0_LIVE_TESTS.md. Every test is `#[ignore]`d so plain
//! `cargo test` stays fast and offline. Run explicitly:
//!
//! ```sh
//! ANYAGENT_LIVE=all cargo test --test live -- --ignored --test-threads=1
//! ANYAGENT_LIVE=claude cargo test --test live cancel -- --ignored
//! ```
//!
//! Rules the suite enforces itself: the ANTHROPIC_* env hijack is stripped
//! in-process, opencode auto-skips without OPENROUTER_API_KEY, capability
//! gates print SKIP (which is a pass), and every event wait names the step
//! it hung at. Model-output flakes (wrong word from a weak model) are the
//! operator's judgment call; structural failures fail hard.

#![cfg(unix)]

use std::num::NonZeroU32;
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, Answer, AuthStatus, Capability, ConfigKind, DeliveryKind, Event, EventKind, Events,
    MessageId, PermissionChoice, QuestionAnswer, Request, RequestId, RollbackScope, Runtime,
    Session, SessionOptions, StopReason, ToolStatus, TurnOrigin,
};

const EVENT_TIMEOUT: Duration = Duration::from_secs(120);
const OPENCODE_MODEL: &str = "openrouter/meta/muse-spark-1.2-contributor";
const COUNT: &str = "Count from 1 to 400, one number per line. No other text. No tools.";

// -- gate -------------------------------------------------------------------

/// Harnesses picked by ANYAGENT_LIVE, with the env hijack stripped first.
fn enabled() -> Vec<&'static str> {
    static STRIP: std::sync::Once = std::sync::Once::new();
    STRIP.call_once(|| {
        for var in [
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_MODEL",
            "CLAUDECODE",
        ] {
            // Safe here: called once, before any session spawns threads that
            // read the environment.
            unsafe { std::env::remove_var(var) };
        }
    });
    let Ok(list) = std::env::var("ANYAGENT_LIVE") else {
        println!("SKIP all: ANYAGENT_LIVE is not set");
        return Vec::new();
    };
    ["claude", "opencode", "hermes", "kiro"]
        .into_iter()
        .filter(|h| list == "all" || list.split(',').any(|p| p.trim() == *h))
        .filter(|h| {
            let keyless = *h == "opencode" && std::env::var("OPENROUTER_API_KEY").is_err();
            if keyless {
                println!("SKIP opencode: OPENROUTER_API_KEY is not set");
            }
            !keyless
        })
        .collect()
}

// -- the features -----------------------------------------------------------

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn discovery_finds_authenticated_harnesses() {
    for h in enabled() {
        let report = Runtime::new().discover().await;
        let agent = report
            .require(h)
            .unwrap_or_else(|_| panic!("{h}: not discovered"));
        assert!(agent.executable_path.exists(), "{h}: executable missing");
        // kiro has no honest offline marker (sqlite credential): discovery
        // reports no auth and `probe` answers for real.
        if h == "kiro" {
            assert!(
                agent.auth.is_none(),
                "{h}: unexpected marker: {:?}",
                agent.auth
            );
            pass(h, "discovered (auth unknown by design)");
            continue;
        }
        assert!(
            matches!(agent.auth, Some(AuthStatus::Authenticated { .. })),
            "{h}: not authenticated: {:?}",
            agent.auth
        );
        pass(h, "discovered and authenticated");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn open_reports_token_capabilities_and_options() {
    for h in enabled() {
        let (session, _events, _dir) = open(h).await;
        let info = session.info();
        assert!(info.resume_token.is_some(), "{h}: no resume token at open");
        assert!(info.details.version.is_some(), "{h}: no version");
        let caps = &info.details.capabilities;
        let has_option = |id: &str| {
            info.details
                .config_options
                .iter()
                .any(|o| o.id.as_str() == id)
        };
        assert!(
            caps.supports(Capability::Permissions),
            "{h}: no Permissions"
        );
        assert!(has_option("mode"), "{h}: no `mode` config option");
        if h == "claude" {
            for cap in [
                Capability::Images,
                Capability::Resume,
                Capability::Subagents,
            ] {
                assert!(caps.supports(cap.clone()), "claude: missing {cap:?}");
            }
            assert!(!caps.supports(Capability::Steer), "claude must not steer");
            assert!(
                !info.details.commands.is_empty(),
                "claude: no slash commands"
            );
        }
        if h == "opencode" {
            assert!(has_option("model"), "opencode: no `model` config option");
        }
        session.close().await.unwrap();
        pass(h, "open info complete");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn probe_reports_details_without_a_session() {
    for h in enabled() {
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report
            .require(h)
            .unwrap_or_else(|_| panic!("{h}: not discovered"));
        // Probe promises to leave nothing behind. Claude writes one
        // transcript file per session, so a new one means its throwaway
        // session outlived the probe.
        let transcripts = claude_transcripts();
        let details = runtime
            .probe(agent)
            .await
            .unwrap_or_else(|e| panic!("{h}: probe failed: {e}"));
        assert!(details.version.is_some(), "{h}: probe has no version");
        let model = details
            .config_options
            .iter()
            .find(|o| o.id.as_str() == "model");
        let has_mode = details
            .config_options
            .iter()
            .any(|o| o.id.as_str() == "mode");
        assert!(
            model.is_some() || has_mode,
            "{h}: probe has no model/mode option"
        );
        if h == "claude" {
            let ConfigKind::Select { choices } = &model.expect("claude: model option").kind else {
                panic!("claude: model option is not a select");
            };
            assert!(!choices.is_empty(), "claude: model has no choices");
            assert!(
                !details.commands.is_empty(),
                "claude: probe has no commands"
            );
            let left: Vec<_> = claude_transcripts()
                .difference(&transcripts)
                .cloned()
                .collect();
            assert!(left.is_empty(), "claude: probe left a transcript: {left:?}");
        }
        pass(h, "probe reports details without a session");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn turn_events_are_bracketed_ordered_and_quiet_after_end() {
    for h in enabled() {
        let (session, mut events, _dir) = open(h).await;
        session
            .prompt("Say only the word PINEAPPLE. Do not use any tools.")
            .await
            .unwrap();
        let mut last_seq = 0;
        let mut saw_in_turn = false;
        let mut open_messages = std::collections::BTreeSet::new();
        let mut text = String::new();
        loop {
            let event = next(&mut events, &format!("{h}: turn contract")).await;
            assert!(event.sequence > last_seq, "{h}: sequence not increasing");
            last_seq = event.sequence;
            // The first event inside the turn must be TurnStarted; session
            // level events (turn: None) may legitimately come first.
            if event.turn_info.is_some() && !saw_in_turn {
                saw_in_turn = true;
                assert!(
                    matches!(
                        event.kind,
                        EventKind::TurnStarted {
                            origin: TurnOrigin::Prompt(_)
                        }
                    ),
                    "{h}: first in-turn event was {:?}",
                    event.kind
                );
                continue;
            }
            match event.kind {
                EventKind::TextDelta {
                    message_id,
                    text: t,
                } => {
                    open_messages.insert(message_id);
                    text.push_str(&t);
                }
                EventKind::ReasoningDelta { message_id, .. } => {
                    open_messages.insert(message_id);
                }
                EventKind::MessageEnded { message_id } => {
                    open_messages.remove(&message_id);
                }
                EventKind::Diagnostic(d) => {
                    assert!(
                        d.level != anyagent::DiagnosticLevel::Error,
                        "{h}: error diagnostic: {}",
                        d.message
                    );
                }
                EventKind::TurnEnded { stop, .. } => {
                    assert!(
                        matches!(stop, StopReason::Completed { .. }),
                        "{h}: {stop:?}"
                    );
                    break;
                }
                _ => {}
            }
        }
        assert!(
            open_messages.is_empty(),
            "{h}: unended messages {open_messages:?}"
        );
        assert!(text.contains("PINEAPPLE"), "{h}: text was {text:?}");
        quiet(&mut events, 3, &format!("{h}: after turn end")).await;
        session.close().await.unwrap();
        pass(h, "turn contract holds");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn tools_run_to_completion_and_the_file_lands() {
    for h in enabled() {
        let (session, mut events, dir) = open(h).await;
        session
            .prompt("Create a file named note.txt containing exactly the word HELLO. Use your file tools.")
            .await
            .unwrap();
        let mut completed = Vec::new();
        loop {
            let event = next(&mut events, &format!("{h}: tool lifecycle")).await;
            match event.kind {
                EventKind::ToolUpdated(tool) if tool.status == ToolStatus::Completed => {
                    completed.push(tool.id);
                }
                EventKind::RequestOpened(request) => {
                    session.answer(request.id(), allow()).await.unwrap();
                }
                EventKind::TurnEnded { stop, .. } => {
                    assert!(
                        matches!(stop, StopReason::Completed { .. }),
                        "{h}: {stop:?}"
                    );
                    break;
                }
                _ => {}
            }
        }
        let content = std::fs::read_to_string(dir.path().join("note.txt"))
            .unwrap_or_else(|_| panic!("{h}: note.txt missing"));
        assert_eq!(content.trim(), "HELLO", "{h}: wrong content");
        // hermes never sends status transitions — KNOWN quirk, file is truth.
        if completed.is_empty() {
            assert_eq!(h, "hermes", "{h}: no tool reached Completed");
            pass(h, "file landed — KNOWN (hermes: no status updates)");
        } else {
            pass(h, "tool completed and file landed");
        }
        session.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn permissions_gate_the_write_and_deny_holds() {
    for h in enabled() {
        let write =
            "Create a file named note.txt containing exactly the word HELLO. Use your file tools.";
        // Session A: allow — the request closes and the file lands.
        let (session, mut events, dir) = open(h).await;
        session.prompt(write).await.unwrap();
        let mut asked = false;
        loop {
            let event = next(&mut events, &format!("{h}: permission allow")).await;
            match event.kind {
                EventKind::RequestOpened(Request::Permission(request)) => {
                    asked = true;
                    assert!(
                        request.options.contains(&PermissionChoice::AllowOnce),
                        "{h}: no allow"
                    );
                    assert!(
                        request.options.contains(&PermissionChoice::DenyOnce),
                        "{h}: no deny"
                    );
                    session.answer(request.id, allow()).await.unwrap();
                }
                EventKind::TurnEnded { .. } => break,
                _ => {}
            }
        }
        if !asked {
            assert_eq!(h, "opencode", "{h}: no permission request opened");
            println!("SKIP opencode: permissions (agent auto-allows)");
            session.close().await.unwrap();
            continue;
        }
        assert!(
            dir.path().join("note.txt").exists(),
            "{h}: file missing after allow"
        );
        session.close().await.unwrap();

        // Session B: deny — no file, and the session stays usable.
        let (session, mut events, dir) = open(h).await;
        session.prompt(write).await.unwrap();
        loop {
            let event = next(&mut events, &format!("{h}: permission deny")).await;
            match event.kind {
                EventKind::RequestOpened(request) => {
                    session
                        .answer(request.id(), Answer::Permission(PermissionChoice::DenyOnce))
                        .await
                        .unwrap();
                }
                EventKind::TurnEnded { .. } => break,
                _ => {}
            }
        }
        assert!(
            !dir.path().join("note.txt").exists(),
            "{h}: file exists after deny"
        );
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: post-deny prompt")).await;
        session.close().await.unwrap();
        pass(h, "allow writes, deny holds, session survives");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn a_question_round_trips() {
    for h in enabled() {
        if h != "claude" {
            println!("SKIP {h}: questions (claude only)");
            continue;
        }
        let (session, mut events, _dir) = open(h).await;
        session
            .prompt("Ask me whether I prefer red or blue using your question tool, then answer with just my choice.")
            .await
            .unwrap();
        let mut text = String::new();
        loop {
            let event = next(&mut events, "claude: question").await;
            match event.kind {
                EventKind::RequestOpened(Request::Question(request)) => {
                    let question = &request.questions[0];
                    assert!(question.choices.len() >= 2, "fewer than 2 choices");
                    let red = question
                        .choices
                        .iter()
                        .find(|c| c.label.to_lowercase().contains("red"))
                        .expect("no red choice")
                        .id
                        .clone();
                    session
                        .answer(
                            request.id,
                            Answer::Question(vec![QuestionAnswer::Choices(vec![red])]),
                        )
                        .await
                        .unwrap();
                }
                EventKind::TextDelta { text: t, .. } => text.push_str(&t),
                EventKind::TurnEnded { stop, .. } => {
                    assert!(matches!(stop, StopReason::Completed { .. }), "{stop:?}");
                    break;
                }
                _ => {}
            }
        }
        assert!(text.to_lowercase().contains("red"), "answer was {text:?}");
        session.close().await.unwrap();
        pass(h, "question answered and echoed");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn steering_is_absent_on_claude_and_folds_where_advertised() {
    for h in enabled() {
        let (session, mut events, _dir) = open(h).await;
        let steers = session
            .info()
            .details
            .capabilities
            .supports(Capability::Steer);
        if h == "claude" {
            assert!(!steers, "claude must not advertise Steer");
            session.close().await.unwrap();
            pass(h, "Steer correctly absent");
            continue;
        }
        if !steers {
            println!("SKIP {h}: steering (not advertised)");
            session.close().await.unwrap();
            continue;
        }
        session.prompt(COUNT).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let delivery = session
            .prompt("Stop counting and say only CHERRY.")
            .await
            .unwrap();
        assert!(
            matches!(delivery.kind, DeliveryKind::Steered { .. }),
            "{h}: steer delivered as {:?}",
            delivery.kind
        );
        let text = drain_to_turn_end(&session, &mut events, &format!("{h}: steer")).await;
        assert!(text.contains("CHERRY"), "{h}: steered output was {text:?}");
        session.close().await.unwrap();
        pass(h, "steer folded into the turn");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn the_queue_is_fifo_and_ids_stay_aligned() {
    for h in enabled() {
        let (session, mut events, _dir) = open(h).await;
        session.prompt(COUNT).await.unwrap();
        let kiwi = session.prompt("Say only KIWI. No tools.").await.unwrap();
        let lemon = session.prompt("Say only LEMON. No tools.").await.unwrap();
        assert_eq!(kiwi.kind, DeliveryKind::Queued { position: 0 }, "{h}");
        assert_eq!(lemon.kind, DeliveryKind::Queued { position: 1 }, "{h}");

        // Three turns, in order, each keeping its own prompt id.
        drain_to_turn_end(&session, &mut events, &format!("{h}: count turn")).await;
        for (delivery, word) in [(kiwi, "KIWI"), (lemon, "LEMON")] {
            let mut text = String::new();
            loop {
                let event = next(&mut events, &format!("{h}: {word} turn")).await;
                match event.kind {
                    EventKind::TurnStarted { origin } => {
                        assert_eq!(
                            origin,
                            TurnOrigin::Prompt(delivery.prompt_id.clone()),
                            "{h}: {word} ran under the wrong prompt id"
                        );
                    }
                    EventKind::TextDelta { text: t, .. } => text.push_str(&t),
                    EventKind::TurnEnded { .. } => break,
                    _ => {}
                }
            }
            assert!(text.contains(word), "{h}: {word} turn said {text:?}");
        }
        session.close().await.unwrap();
        pass(h, "queue is FIFO with aligned ids");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn cancel_ends_the_turn_in_every_queue_shape() {
    for h in enabled() {
        // The claude wedge was a timing race (interrupt vs the CLI's own
        // queued→started window), so repeat the raced variant there.
        let reps = if h == "claude" { 3 } else { 1 };
        let (session, mut events, _dir) = open(h).await;

        // Empty queue: cancel ends the turn and the session survives.
        session.prompt(COUNT).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        session.cancel(false).await.unwrap();
        expect_cancelled(&mut events, &format!("{h}: empty-queue cancel")).await;
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: post-cancel prompt")).await;

        // (a) cancel(false) with a queued prompt: it runs next and answers.
        for rep in 0..reps {
            session.prompt(COUNT).await.unwrap();
            // Queue immediately: a fast model can finish COUNT inside a fixed
            // sleep (kiro did), which would make this `Started`, not `Queued`.
            let queued = session.prompt("Say only PEAR. No tools.").await.unwrap();
            assert_eq!(queued.kind, DeliveryKind::Queued { position: 0 }, "{h}");
            tokio::time::sleep(Duration::from_secs(2)).await;
            session.cancel(false).await.unwrap();
            expect_cancelled(&mut events, &format!("{h}: queued cancel rep {rep}")).await;
            let mut text = drain_to_turn_end(
                &session,
                &mut events,
                &format!("{h}: queued prompt rep {rep}"),
            )
            .await;
            // KNOWN (kiro 2.19.1): the agent's cancel races the next prompt —
            // the queued turn can come back spuriously cancelled and empty
            // (probed 2026-08-27). An app's recourse is to re-send; do that.
            if h == "kiro" && text.is_empty() {
                println!("KNOWN kiro: queued turn spuriously cancelled; re-sending");
                session.prompt("Say only PEAR. No tools.").await.unwrap();
                text =
                    drain_to_turn_end(&session, &mut events, &format!("{h}: PEAR re-send")).await;
            }
            assert!(text.contains("PEAR"), "{h}: queued turn said {text:?}");
        }

        // (b) cancel(true): the queued prompt must never run.
        session.prompt(COUNT).await.unwrap();
        session.prompt("Say only PLUM. No tools.").await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        session.cancel(true).await.unwrap();
        expect_cancelled(&mut events, &format!("{h}: clear-queue cancel")).await;
        quiet(&mut events, 5, &format!("{h}: after clear-queue cancel")).await;
        session.close().await.unwrap();
        pass(h, "cancel works in every queue shape");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn resume_recalls_without_replaying() {
    for h in enabled() {
        let (session, mut events, dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Resume)
        {
            let token = session.info().resume_token.expect("token exists at open");
            session.close().await.unwrap();
            let report = Runtime::new().discover().await;
            let agent = report.require(h).unwrap();
            let result = Runtime::new()
                .open(agent, SessionOptions::in_dir(dir.path()).resume(token))
                .await;
            assert!(
                matches!(result, Err(AgentError::ResumeFailed(_))),
                "{h}: resume without the capability should fail typed"
            );
            pass(h, "resume correctly refused (not advertised)");
            continue;
        }
        session
            .prompt("Remember this codeword: FALCON42. Just confirm. No tools.")
            .await
            .unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: codeword turn")).await;
        let token = session.info().resume_token.expect("token after turn");
        session.close().await.unwrap();

        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report.require(h).unwrap();
        let mut options = SessionOptions::in_dir(dir.path()).resume(token);
        if h == "opencode" {
            options = options.configure("model", OPENCODE_MODEL);
        }
        let (session, mut events) = runtime.open(agent, options).await.unwrap();
        // No replay: 3s of pre-prompt drain must carry zero content events.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.next()).await {
            let event = event.unwrap();
            assert!(
                !matches!(
                    event.kind,
                    EventKind::TextDelta { .. }
                        | EventKind::ReasoningDelta { .. }
                        | EventKind::ToolUpdated(_)
                ),
                "{h}: replayed content after resume: {:?}",
                event.kind
            );
        }
        session
            .prompt("What is the codeword? No tools.")
            .await
            .unwrap();
        let text = drain_to_turn_end(&session, &mut events, &format!("{h}: recall turn")).await;
        assert!(text.contains("FALCON42"), "{h}: recall said {text:?}");
        session.close().await.unwrap();
        pass(h, "resumed with no replay and full recall");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn plan_usage_arrives_after_a_turn() {
    for h in enabled() {
        let (session, mut events, _dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::PlanUsage)
        {
            println!("SKIP {h}: plan usage not advertised");
            session.close().await.unwrap();
            continue;
        }
        session.prompt("Say OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: usage turn")).await;
        // The adapter refreshes quota right after the turn.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let usage = loop {
            let event = tokio::time::timeout_at(deadline, events.next())
                .await
                .unwrap_or_else(|_| panic!("{h}: no PlanUsageUpdated within 10s of turn end"))
                .expect("stream ended")
                .unwrap();
            if let EventKind::PlanUsageUpdated(usage) = event.kind {
                break usage;
            }
        };
        assert!(!usage.windows.is_empty(), "{h}: quota with no windows");
        for w in &usage.windows {
            assert!(
                w.used_percent <= 100,
                "{h}: {} at {}%",
                w.label,
                w.used_percent
            );
        }
        assert!(
            usage.windows.iter().any(|w| w.resets_at.is_some()),
            "{h}: no window carries a reset time"
        );
        session.close().await.unwrap();
        let summary: Vec<_> = usage
            .windows
            .iter()
            .map(|w| format!("{} {}%", w.label, w.used_percent))
            .collect();
        pass(h, &format!("plan usage pushed: {}", summary.join(", ")));
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn rollback_forgets_the_rolled_back_turn() {
    for h in enabled() {
        let (session, mut events, _dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Rollback)
        {
            println!("SKIP {h}: rollback not advertised");
            session.close().await.unwrap();
            continue;
        }
        session
            .prompt("Remember this codeword: ALPHA9. Just confirm. No tools.")
            .await
            .unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: codeword one")).await;
        session
            .prompt("Remember a second codeword: ZULU7. Just confirm. No tools.")
            .await
            .unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: codeword two")).await;
        let before = session.info().resume_token.expect("token before rollback");

        session
            .rollback(NonZeroU32::new(1).unwrap(), RollbackScope::Conversation)
            .await
            .unwrap();
        session
            .prompt("List every codeword I told you, comma separated, nothing else. No tools.")
            .await
            .unwrap();
        let text = drain_to_turn_end(&session, &mut events, &format!("{h}: recall turn")).await;
        assert!(
            text.contains("ALPHA9"),
            "{h}: kept turn forgotten: {text:?}"
        );
        assert!(
            !text.contains("ZULU7"),
            "{h}: rolled-back turn recalled: {text:?}"
        );
        // The emulated fork renames the provider session.
        let after = session.info().resume_token.expect("token after rollback");
        assert_ne!(after.as_str(), before.as_str(), "{h}: token did not change");
        session.close().await.unwrap();
        pass(h, "rollback forgot exactly the last turn");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn files_rollback_restores_agent_written_files() {
    for h in enabled() {
        let (session, mut events, dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::RollbackFiles)
        {
            println!("SKIP {h}: file rollback not advertised");
            session.close().await.unwrap();
            continue;
        }
        let note = dir.path().join("note.txt");
        for word in ["alpha", "beta"] {
            session
                .prompt(format!(
                    "Use the Write tool to make {} contain exactly: {word}. Nothing else.",
                    note.display()
                ))
                .await
                .unwrap();
            drain_to_turn_end(&session, &mut events, &format!("{h}: write {word}")).await;
        }
        assert_eq!(std::fs::read_to_string(&note).unwrap().trim(), "beta");

        // Dropping the last turn also rewinds its file change.
        session
            .rollback(
                NonZeroU32::new(1).unwrap(),
                RollbackScope::ConversationAndFiles,
            )
            .await
            .unwrap();
        loop {
            if let EventKind::SessionUpdated(_) =
                next(&mut events, &format!("{h}: rollback")).await.kind
            {
                break;
            }
        }
        assert_eq!(std::fs::read_to_string(&note).unwrap().trim(), "alpha");
        session.close().await.unwrap();
        pass(h, "files rollback restored the previous file state");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn fork_from_branches_at_a_point_and_at_the_tip() {
    for h in enabled() {
        let (session, mut events, dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Fork)
        {
            println!("SKIP {h}: fork not advertised");
            session.close().await.unwrap();
            continue;
        }
        // Two codeword turns; keep each turn's last fork anchor
        // (`claude/fork_point` on MessageEnded).
        let mut anchors = Vec::new();
        for codeword in ["ALPHA9", "ZULU7"] {
            session
                .prompt(format!(
                    "Remember this codeword: {codeword}. Just confirm. No tools."
                ))
                .await
                .unwrap();
            let mut anchor = None;
            loop {
                let event = next(&mut events, &format!("{h}: codeword turn")).await;
                match event.kind {
                    EventKind::MessageEnded { .. } => {
                        if let Some(point) = event.extensions.get("claude/fork_point") {
                            anchor = point.as_str().map(str::to_owned);
                        }
                    }
                    EventKind::TurnEnded { .. } => break,
                    _ => {}
                }
            }
            anchors.push(anchor.expect("fork anchor on the turn's messages"));
        }
        let token = session.info().resume_token.expect("token after turns");
        session.close().await.unwrap();

        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report.require(h).unwrap();
        let recall = "List every codeword I told you, comma separated, nothing else. No tools.";

        // Fork at turn one's anchor: the branch forgets ZULU7.
        let (forked, mut fork_events) = runtime
            .open(
                agent,
                SessionOptions::in_dir(dir.path())
                    .fork_from(token.clone(), Some(MessageId::new(&anchors[0]))),
            )
            .await
            .unwrap();
        forked.prompt(recall).await.unwrap();
        let text = drain_to_turn_end(&forked, &mut fork_events, &format!("{h}: cut fork")).await;
        assert!(
            text.contains("ALPHA9"),
            "{h}: cut fork lost turn 1: {text:?}"
        );
        assert!(
            !text.contains("ZULU7"),
            "{h}: cut fork kept turn 2: {text:?}"
        );
        let fork_token = forked.info().resume_token.expect("fork token");
        assert_ne!(fork_token.as_str(), token.as_str(), "{h}: fork kept the id");
        forked.close().await.unwrap();

        // Fork at the tip: the branch knows both — which also proves the
        // original transcript survived the first fork untouched.
        let (tip, mut tip_events) = runtime
            .open(
                agent,
                SessionOptions::in_dir(dir.path()).fork_from(token.clone(), None),
            )
            .await
            .unwrap();
        tip.prompt(recall).await.unwrap();
        let text = drain_to_turn_end(&tip, &mut tip_events, &format!("{h}: tip fork")).await;
        assert!(
            text.contains("ALPHA9") && text.contains("ZULU7"),
            "{h}: tip fork lost history: {text:?}"
        );
        tip.close().await.unwrap();
        pass(h, "forked at a point and at the tip; original untouched");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn runtime_plan_usage_probes_without_a_session() {
    for h in enabled() {
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report.require(h).unwrap();
        match runtime.plan_usage(agent).await {
            Ok(usage) => {
                assert!(!usage.windows.is_empty(), "{h}: quota with no windows");
                let cached = runtime.plan_usage(agent).await.unwrap();
                assert_eq!(cached.fetched_at, usage.fetched_at, "{h}: cache missed");
                pass(
                    h,
                    &format!("probe returned {} windows, cached", usage.windows.len()),
                );
            }
            Err(AgentError::UnsupportedFeature(_)) => {
                println!("SKIP {h}: plan usage unsupported (typed)");
            }
            Err(e) => panic!("{h}: plan usage probe failed: {e:?}"),
        }
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn a_killed_agent_fails_the_turn_and_closes_the_session() {
    for h in enabled() {
        if h == "hermes" {
            println!("SKIP hermes: agent death (messy process tree; two harnesses prove the path)");
            continue;
        }
        let (session, mut events, _dir) = open(h).await;
        session.prompt(COUNT).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        kill_child(h, &session);

        let mut failed = false;
        loop {
            match tokio::time::timeout(EVENT_TIMEOUT, events.next()).await {
                Ok(Some(Ok(event))) => {
                    if let EventKind::TurnEnded { stop, .. } = event.kind {
                        assert!(matches!(stop, StopReason::Failed { .. }), "{h}: {stop:?}");
                        failed = true;
                    }
                }
                Ok(Some(Err(error))) => {
                    let AgentError::ProcessExited { status, .. } = &error else {
                        panic!("{h}: stream error was {error}");
                    };
                    assert!(status.contains('9'), "{h}: status was {status:?}");
                }
                Ok(None) => break,
                Err(_) => panic!("{h}: hung after kill"),
            }
        }
        assert!(failed, "{h}: no Failed turn end before the stream closed");
        assert!(
            matches!(session.prompt("hi").await, Err(AgentError::SessionClosed)),
            "{h}: prompt after death should be SessionClosed"
        );
        pass(h, "death maps to Failed + ProcessExited + closed");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn close_returns_promptly_and_ends_the_stream() {
    for h in enabled() {
        let (session, mut events, _dir) = open(h).await;
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: short turn")).await;
        tokio::time::timeout(Duration::from_secs(10), session.close())
            .await
            .unwrap_or_else(|_| panic!("{h}: close took over 10s"))
            .unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(10), events.next()).await {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => panic!("{h}: stream did not end after close"),
            }
        }
        pass(h, "close is prompt and the stream ends");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn errors_are_typed() {
    for h in enabled() {
        if h == "hermes" {
            println!("SKIP hermes: typed errors (excluded by plan)");
            continue;
        }
        let (session, mut events, _dir) = open(h).await;
        // (a) an unadvertised feature refuses typed (claude advertises
        // rollback now, so it proves this elsewhere).
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Rollback)
        {
            assert!(
                matches!(
                    session
                        .rollback(NonZeroU32::new(1).unwrap(), RollbackScope::Conversation)
                        .await,
                    Err(AgentError::UnsupportedFeature(_))
                ),
                "{h}: rollback should be UnsupportedFeature"
            );
        }
        // (b) answering a request that is not open refuses typed.
        assert!(
            matches!(
                session.answer(RequestId::new("nope"), allow()).await,
                Err(AgentError::InvalidRequest(_))
            ),
            "{h}: unknown request should be InvalidRequest"
        );
        // (c) a closed session refuses typed.
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: pre-close turn")).await;
        session.close().await.unwrap();
        assert!(
            matches!(session.prompt("hi").await, Err(AgentError::SessionClosed)),
            "{h}: prompt after close should be SessionClosed"
        );
        pass(h, "errors are typed");
    }
}

// -- helpers ----------------------------------------------------------------

/// Opens a live session for one harness in a fresh temp dir.
async fn open(harness: &str) -> (Session, Events, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Runtime::new();
    let report = runtime.discover().await;
    let agent = report
        .require(harness)
        .unwrap_or_else(|_| panic!("{harness}: not discovered"));
    let mut options = SessionOptions::in_dir(dir.path());
    if harness == "opencode" {
        options = options.configure("model", OPENCODE_MODEL);
    }
    let (session, events) = runtime
        .open(agent, options)
        .await
        .unwrap_or_else(|e| panic!("{harness}: open failed: {e}"));
    (session, events, dir)
}

/// Next event within the timeout; a hang fails naming the step.
async fn next(events: &mut Events, step: &str) -> Event {
    match tokio::time::timeout(EVENT_TIMEOUT, events.next()).await {
        Ok(Some(Ok(event))) => event,
        Ok(Some(Err(error))) => panic!("stream error at {step}: {error}"),
        Ok(None) => panic!("stream closed at {step}"),
        Err(_) => panic!("hung at {step}"),
    }
}

/// Drains to `TurnEnded` (auto-allowing permissions) and returns the text.
async fn drain_to_turn_end(session: &Session, events: &mut Events, step: &str) -> String {
    let mut text = String::new();
    loop {
        let event = next(events, step).await;
        match event.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => return text,
            _ => {}
        }
    }
}

/// The next `TurnEnded` must be `Cancelled`, within ~10s.
async fn expect_cancelled(events: &mut Events, step: &str) {
    let wait = async {
        loop {
            if let EventKind::TurnEnded { stop, .. } = next(events, step).await.kind {
                assert_eq!(stop, StopReason::Cancelled, "{step}");
                return;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .unwrap_or_else(|_| panic!("no Cancelled turn end within 10s at {step}"));
}

/// Asserts no turn traffic arrives for `secs` seconds. Diagnostics are the
/// sanctioned form of out-of-turn noise (kiro emits metadata notifications
/// between turns) and don't break the quiet.
async fn quiet(events: &mut Events, secs: u64, step: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.next()).await {
        let kind = event.map(|e| e.kind);
        if !matches!(kind, Ok(EventKind::Diagnostic(_))) {
            panic!("expected quiet at {step}, got {kind:?}");
        }
    }
}

/// kill -9 the session's own agent process, found by a session-unique marker.
fn kill_child(harness: &str, session: &Session) {
    // claude carries our minted session id in argv; opencode is matched by
    // its newest `opencode acp` process.
    let (args, pattern): (&[&str], String) = match harness {
        "claude" => (
            &[],
            session.info().resume_token.unwrap().as_str().to_owned(),
        ),
        // Both halves: `kiro-cli acp` dispatches to a `kiro-cli-chat acp`
        // worker that inherits the pipes — killing only the dispatcher lets
        // the turn complete. Anchored so the user's Kiro apps' own
        // `kiro-cli acp --agent <name>` processes never match.
        "kiro" => (&[], "kiro-cli(-chat)? acp$".to_owned()),
        _ => (&["-n"], "opencode acp".to_owned()),
    };
    let out = std::process::Command::new("pgrep")
        .args(args)
        .arg("-f")
        .arg(&pattern)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let pids: Vec<&str> = stdout.lines().collect();
    if pids.is_empty() {
        panic!("{harness}: no process matched {pattern:?}");
    }
    // kiro matches dispatcher + worker; kill every matched pid.
    let last = if harness == "kiro" {
        pids.clone()
    } else {
        vec![*pids.last().unwrap()]
    };
    for pid in last {
        std::process::Command::new("kill")
            .args(["-9", pid])
            .status()
            .unwrap();
    }
}

fn allow() -> Answer {
    Answer::Permission(PermissionChoice::AllowOnce)
}

fn pass(harness: &str, what: &str) {
    println!("PASS {harness}: {what}");
}

/// Transcript files claude has on disk, one per session it has recorded.
/// Empty when the directory does not exist or cannot be read.
fn claude_transcripts() -> std::collections::BTreeSet<std::path::PathBuf> {
    let mut found = std::collections::BTreeSet::new();
    let home = match std::env::var_os("CLAUDE_CONFIG_DIR") {
        Some(dir) => std::path::PathBuf::from(dir),
        None => match std::env::var_os("HOME") {
            Some(home) => std::path::PathBuf::from(home).join(".claude"),
            None => return found,
        },
    };
    let Ok(projects) = std::fs::read_dir(home.join("projects")) else {
        return found;
    };
    for project in projects.flatten() {
        for file in std::fs::read_dir(project.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            if file.path().extension().is_some_and(|e| e == "jsonl") {
                found.insert(file.path());
            }
        }
    }
    found
}

// -- config home isolation + wire recording (P2 smalls) ---------------------

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn config_home_isolates_login() {
    for h in enabled() {
        if h != "claude" {
            println!("SKIP {h}: config-home isolation asserted on claude");
            continue;
        }
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report.require(h).unwrap();
        // The real login is present (offline marker), proving the default
        // path is authenticated.
        assert!(
            matches!(agent.auth, Some(AuthStatus::Authenticated { .. })),
            "{h}: default login is not authenticated: {:?}",
            agent.auth
        );
        // An empty temp config home has no credentials. NEVER touch ~/.claude
        // itself; the temp dir is discarded at the end of the test.
        let empty = tempfile::tempdir().unwrap();
        let opened = runtime
            .open(
                agent,
                SessionOptions::in_dir(empty.path()).config_home(empty.path()),
            )
            .await;
        match opened {
            Ok((session, _events)) => {
                assert!(
                    matches!(
                        session.info().details.auth,
                        AuthStatus::Unauthenticated { .. }
                    ),
                    "{h}: empty config home is not unauthenticated: {:?}",
                    session.info().details.auth
                );
                session.close().await.ok();
            }
            // A logged-out handshake that fails closed is equally valid.
            Err(AgentError::AuthRequired { .. }) => {}
            Err(e) => panic!("{h}: empty config home errored unexpectedly: {e}"),
        }
        pass(h, "config home isolates login (empty home is logged out)");
    }
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn record_wire_captures_a_live_turn() {
    for h in enabled() {
        if h != "claude" {
            println!("SKIP {h}: recording smoke asserted on claude");
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("wire.jsonl");
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report.require(h).unwrap();
        let (session, mut events) = runtime
            .open(agent, SessionOptions::in_dir(dir.path()).record_wire(&log))
            .await
            .unwrap();
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: record turn")).await;
        session.close().await.unwrap();
        // The writer task flushes asynchronously.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let body = std::fs::read_to_string(&log).unwrap();
        let count = body.lines().count();
        assert!(count > 5, "{h}: too few frames recorded: {count}");
        for line in body.lines() {
            assert!(
                line.starts_with("{\"dir\":"),
                "{h}: recorded line is not a dir/frame object: {line}"
            );
        }
        pass(h, &format!("recorded {count} wire frames over a live turn"));
    }
}
