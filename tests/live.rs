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
//! in-process, a selected harness whose CLI is not installed is warned about
//! and skipped (never a failure) and named again by `zz_summary`, capability
//! gates print SKIP (which is a pass), and every event wait names the step
//! it hung at. Model-output flakes (wrong word from a weak model) are the
//! operator's judgment call; structural failures fail hard.

use std::num::NonZeroU32;
use std::time::Duration;

use futures::StreamExt;

use anyagent::{
    AgentError, Answer, AuthStatus, Capability, ConfigKind, ConfigValue, DeliveryKind, Event,
    EventKind, Events, Input, MessageId, PermissionChoice, PermissionMode, PromptId,
    QuestionAnswer, Request, RequestId, ResumeToken, RollbackScope, Runtime, Session,
    SessionOptions, StopReason, ToolStatus, TurnOrigin,
};

/// Every harness the shared matrix covers, in report order.
const HARNESSES: &[&str] = &[
    "claude",
    "codex",
    "opencode",
    "hermes",
    "kiro",
    "pi",
    "cursor",
    "antigravity",
    "grok",
    "qwen",
];
const EVENT_TIMEOUT: Duration = Duration::from_secs(120);
const OPENCODE_MODEL: &str = "opencode/big-pickle";
/// The host config's default (`gpt-6-astra`) needs a newer CLI; luna is cheap and available.
const CODEX_MODEL: &str = "gpt-5.6-luna";
/// pi's model values are `provider/modelId`.
const PI_MODEL: &str = "openrouter/nvidia/nemotron-3-super-120b-a12b:free";
const COUNT: &str = "Count from 1 to 400, one number per line. No other text. No tools.";
const TITLE: &str = "Title this conversation in at most six words: the user asked how to rename \
a git branch. Reply with only the title. No tools.";
/// The cheap claude alias; the CLI resolves it to the current Haiku.
const CLAUDE_MODEL: &str = "haiku";

// -- gate -------------------------------------------------------------------

/// Harnesses selected by ANYAGENT_LIVE whose CLI is actually installed.
/// The roster is built once; harnesses that are missing are warned about
/// there and listed again by the `summary` test at the end of the run.
async fn enabled() -> Vec<&'static str> {
    roster().await.installed.clone()
}

/// Which harnesses the run covers, and the ones it had to leave out.
struct Roster {
    installed: Vec<&'static str>,
    /// One human line per skipped harness, for the warning and the summary.
    skipped: Vec<String>,
}

/// Whether the discovered `antigravity` is the headless `agy` CLI (its ACP
/// server not installed), which cannot ask and needs AutoApprove.
static HEADLESS_AGY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// The roster, built on first use and shared by every test.
async fn roster() -> &'static Roster {
    static ROSTER: tokio::sync::OnceCell<Roster> = tokio::sync::OnceCell::const_new();
    ROSTER.get_or_init(build_roster).await
}

/// Strips the env hijack, reads ANYAGENT_LIVE, then splits the selection
/// into installed and missing by one discovery pass. Warns about the
/// missing ones so a "not discovered" failure can never be mistaken for a
/// broken adapter.
async fn build_roster() -> Roster {
    for var in [
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_MODEL",
        "CLAUDECODE",
    ] {
        // Safe here: runs once, before any session spawns threads that read
        // the environment.
        unsafe { std::env::remove_var(var) };
    }
    // The host's settings.json may route the CLI through a proxy; the
    // process env wins over it, so pin the real API for the suite.
    unsafe { std::env::set_var("ANTHROPIC_BASE_URL", "https://api.anthropic.com") };

    let Ok(list) = std::env::var("ANYAGENT_LIVE") else {
        println!("SKIP all: ANYAGENT_LIVE is not set");
        return Roster {
            installed: Vec::new(),
            skipped: Vec::new(),
        };
    };
    let selected: Vec<&'static str> = HARNESSES
        .iter()
        .copied()
        .filter(|h| list == "all" || list.split(',').any(|p| p.trim() == *h))
        .collect();

    let report = Runtime::new().discover().await;
    // The CLI is always `agy`; the upgrade is the server binary. Checking
    // the name also covers ANYAGENT_ANTIGRAVITY_BIN pinning the CLI while
    // the server is installed.
    let _ = HEADLESS_AGY.set(
        report
            .require("antigravity")
            .map(|a| a.executable_path.file_name().is_some_and(|n| n == "agy"))
            .unwrap_or(false),
    );
    let mut installed = Vec::new();
    let mut skipped = Vec::new();
    for h in selected {
        match report.require(h) {
            // pi is pinned to an openrouter model, and its login lives in
            // its own auth.json (or the key env var): ask pi, not the env.
            Ok(agent) if h == "pi" && !pi_ready(&agent.executable_path, "openrouter") => {
                skipped.push("pi: not logged in to openrouter (`/login openrouter` in pi)".into());
            }
            Ok(_) => installed.push(h),
            Err(_) => skipped.push(missing_line(&report, h)),
        }
    }
    if !skipped.is_empty() {
        println!(
            "\nWARN: {} selected harness(es) not installed:",
            skipped.len()
        );
        for line in &skipped {
            println!("  {line}");
        }
        println!("Their tests are skipped, not failed.\n");
    }
    Roster { installed, skipped }
}

/// pi's own readiness check for one provider (`pi auth check`).
fn pi_ready(exe: &std::path::Path, provider: &str) -> bool {
    std::process::Command::new(exe)
        .args(["auth", "check", "--provider", provider])
        .output()
        .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).trim() == "ready")
}

/// "codex: not installed (searched 14 dirs) — install: npm i -g ..."
fn missing_line(report: &anyagent::DiscoveryReport, harness: &str) -> String {
    match report.missing.iter().find(|m| m.id.as_str() == harness) {
        Some(m) => format!(
            "{harness}: not installed (searched {} dirs) - install: {}",
            m.searched.len(),
            m.install_hint
        ),
        None => format!("{harness}: not installed"),
    }
}

// -- the features -----------------------------------------------------------

/// Discovery finds each enabled harness, and `probe_auth` confirms its login.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn discovery_finds_authenticated_harnesses() {
    for h in enabled().await {
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report
            .require(h)
            .unwrap_or_else(|_| panic!("{h}: not discovered"));
        assert!(agent.executable_path.exists(), "{h}: executable missing");
        let auth = runtime.probe_auth(agent).await.unwrap();
        assert!(
            matches!(auth, AuthStatus::Authenticated { .. }),
            "{h}: not authenticated: {auth:?}"
        );
        pass(h, "discovered and authenticated by probe");
    }
}

/// A live `mode` option switches mid-session without leaking text, and the
/// choice holds across the next turn.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn mode_switches_live() {
    for h in enabled().await {
        let (session, mut events, _dir) = open(h).await;
        let option = |session: &Session| {
            session
                .info()
                .details
                .config_options
                .into_iter()
                .find(|o| o.id.as_str() == "mode")
        };
        let Some(mode) = option(&session).filter(|o| o.live) else {
            println!("SKIP {h}: no live mode option");
            session.close().await.unwrap();
            continue;
        };
        let ConfigKind::Select { choices } = &mode.kind else {
            panic!("{h}: mode is not a select");
        };
        let target = choices
            .iter()
            .map(|c| c.value.clone())
            .find(|v| Some(ConfigValue::Text(v.clone())) != mode.current)
            .expect("a mode other than the current one");
        session.configure("mode", target.as_str()).await.unwrap();
        let switched = |info: &anyagent::SessionInfo| {
            info.configuration
                .options
                .get(&anyagent::ConfigId::new("mode"))
                == Some(&ConfigValue::Text(target.clone()))
        };
        // The switch must arrive as an event, not only in the snapshot.
        let mut announced = false;
        while !announced {
            let event = next(&mut events, "mode switch").await;
            assert!(
                !matches!(event.kind, EventKind::TextDelta { .. }),
                "{h}: a switch leaked text"
            );
            if let EventKind::SessionUpdated(info) = &event.kind {
                announced = switched(info);
            }
        }
        assert!(switched(&session.info()), "{h}: snapshot lags the event");
        session
            .prompt("Reply with just the word ok.")
            .await
            .unwrap();
        let text = drain_to_turn_end(&session, &mut events, "turn after mode switch").await;
        assert!(text.to_lowercase().contains("ok"), "{h}: got {text:?}");
        assert_eq!(
            option(&session).and_then(|o| o.current),
            Some(ConfigValue::Text(target.clone())),
            "{h}: mode changed under us after the turn"
        );
        session.close().await.unwrap();
        pass(h, &format!("mode switched live to {target}"));
    }
}

/// An attached image reaches the model wherever `Images` is advertised; a
/// PDF reaches antigravity's server, the one wire that takes PDFs inline.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn attachments_reach_the_model() {
    let fixtures =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/attachments");
    for h in enabled().await {
        let (session, mut events, _dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Images)
        {
            println!("SKIP {h}: images not advertised");
            session.close().await.unwrap();
            continue;
        }
        session
            .prompt(
                Input::text("What colour is the attached image? Reply with one word.")
                    .attach(fixtures.join("image.png")),
            )
            .await
            .unwrap();
        let text = drain_to_turn_end(&session, &mut events, "image").await;
        assert!(
            text.to_lowercase().contains("red"),
            "{h}: image answer was {text:?}"
        );
        if h == "antigravity" {
            session
                .prompt(
                    Input::text(
                        "What is the secret word in the attached PDF? Reply with one word.",
                    )
                    .attach(fixtures.join("secret.pdf")),
                )
                .await
                .unwrap();
            let text = drain_to_turn_end(&session, &mut events, "pdf").await;
            assert!(
                text.to_lowercase().contains("pineapple"),
                "{h}: pdf answer was {text:?}"
            );
        }
        session.close().await.unwrap();
        pass(h, "attachments reached the model");
    }
}

/// Open returns resume token, version, harness-correct capabilities (Permissions/Steer/Images) and config options (mode/model/sandbox).
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn open_reports_token_capabilities_and_options() {
    for h in enabled().await {
        let (session, _events, _dir) = open(h).await;
        let info = session.info();
        assert!(info.resume_token.is_some(), "{h}: no resume token at open");
        assert!(info.details.version.is_some(), "{h}: no version");
        // A fresh session has no real title yet; a dated placeholder must
        // not be advertised as one (opencode: "New session - <date>").
        if let Some(title) = &info.title {
            assert!(
                !title.starts_with("New session") && !title.starts_with("Child session"),
                "{h}: placeholder title advertised: {title:?}"
            );
        }
        let caps = &info.details.capabilities;
        let has_option = |id: &str| {
            info.details
                .config_options
                .iter()
                .any(|o| o.id.as_str() == id)
        };
        // pi has no permission protocol on its wire and no permission mode:
        // its `mode`-shaped knob is the model's thinking level. agy's
        // headless wire cannot prompt at all; its ACP server (the upgrade,
        // used when installed) can, and then reads like any ACP agent.
        let headless_agy = h == "antigravity" && !caps.supports(Capability::Permissions);
        if h == "pi" || headless_agy {
            assert_eq!(caps.supports(Capability::Steer), h == "pi", "{h}: Steer");
            assert!(has_option("model"), "{h}: no `model` config option");
        } else {
            assert!(
                caps.supports(Capability::Permissions),
                "{h}: no Permissions"
            );
            // opencode's native wire exposes `model`, not a session `mode`;
            // grok has no modes either, only `model` and `effort`.
            if !matches!(h, "opencode" | "grok") {
                assert!(has_option("mode"), "{h}: no `mode` config option");
            }
        }
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
        if matches!(h, "opencode" | "grok") {
            assert!(has_option("model"), "{h}: no `model` config option");
        }
        if h == "codex" {
            assert!(caps.supports(Capability::Steer), "codex: missing Steer");
            assert!(has_option("model"), "codex: no `model` config option");
            assert!(has_option("sandbox"), "codex: no `sandbox` option");
        }
        session.close().await.unwrap();
        pass(h, "open info complete");
    }
}

/// Effort is one option everywhere it exists: `effort`, a live select whose choices follow the model, switched via `configure` and confirmed by `SessionUpdated`; verified on kiro, grok, opencode, pi, cursor, qwen (whose wire calls it `reasoning_effort`).
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn effort_switches_live() {
    for h in enabled().await {
        if !matches!(h, "kiro" | "grok" | "opencode" | "pi" | "cursor" | "qwen") {
            println!("SKIP {h}: effort asserted on kiro, grok, opencode, pi, cursor, qwen");
            continue;
        }
        let (session, mut events, _dir) = open(h).await;
        let option = |session: &Session| {
            session
                .info()
                .details
                .config_options
                .into_iter()
                .find(|o| o.id.as_str() == "effort")
        };
        // The pinned zen model has no variants; a free one with them also
        // proves the option follows a live model switch.
        if h == "opencode" {
            session
                .configure("model", "opencode/ling-3.0-flash-fin-free")
                .await
                .unwrap();
            while option(&session).is_none() {
                next(&mut events, "effort after model switch").await;
            }
        }
        let Some(effort) = option(&session) else {
            assert_ne!(h, "kiro", "kiro: no `effort` option");
            // One id everywhere: an effort knob under another name (qwen
            // advertises `reasoning_effort`) is an adapter gap, not a model
            // without levels.
            let other = session
                .info()
                .details
                .config_options
                .into_iter()
                .find(|o| o.id.as_str().contains("effort"));
            assert!(
                other.is_none(),
                "{h}: effort advertised as {:?}",
                other.map(|o| o.id)
            );
            println!("SKIP {h}: the selected model has no effort levels");
            session.close().await.unwrap();
            continue;
        };
        assert!(effort.live, "{h}: effort is not live");
        let ConfigKind::Select { choices } = &effort.kind else {
            panic!("{h}: effort is not a select");
        };
        let levels: Vec<&str> = choices.iter().map(|c| c.value.as_str()).collect();
        if h == "kiro" {
            assert_eq!(
                levels,
                ["low", "medium", "high", "xhigh", "max"],
                "{h}: levels"
            );
        }
        // kiro reports the current level in a metadata frame right after open.
        if h == "kiro" {
            while option(&session).is_some_and(|o| o.current.is_none()) {
                next(&mut events, "effort sync").await;
            }
        }
        let current = option(&session).and_then(|o| o.current);
        // Not "thinking off": a provider can refuse it (qwen on OpenRouter's
        // glm-5.3-flash: "Reasoning is mandatory for this endpoint").
        let target = levels
            .iter()
            .filter(|l| !matches!(**l, "none" | "off"))
            .find(|l| Some(ConfigValue::Text((**l).to_owned())) != current)
            .copied()
            .expect("a level other than the current one");
        session.configure("effort", target).await.unwrap();
        while option(&session).and_then(|o| o.current) != Some(ConfigValue::Text(target.into())) {
            let event = next(&mut events, "effort switch").await;
            assert!(
                !matches!(event.kind, EventKind::TextDelta { .. }),
                "{h}: a switch leaked text"
            );
        }
        session
            .prompt("Reply with just the word ok.")
            .await
            .unwrap();
        let text = drain_to_turn_end(&session, &mut events, "turn after switch").await;
        assert!(text.to_lowercase().contains("ok"), "{h}: got {text:?}");
        assert_eq!(
            option(&session).and_then(|o| o.current),
            Some(ConfigValue::Text(target.into())),
            "{h}: effort changed under us after the turn"
        );
        session.close().await.unwrap();
        pass(
            h,
            &format!("effort switched to {target} ({} levels)", levels.len()),
        );
    }
}

/// Probe returns version + model/mode + commands without creating a session or leaving claude transcripts.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn probe_reports_details_without_a_session() {
    for h in enabled().await {
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
        pass(
            h,
            &format!(
                "probe reports details without a session ({} commands)",
                details.commands.len()
            ),
        );
    }
}

/// `generate` is prompt in, text out, with no session to manage.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn generate_returns_text_without_a_session() {
    for h in enabled().await {
        let dir = tempfile::tempdir().unwrap();
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report
            .require(h)
            .unwrap_or_else(|_| panic!("{h}: not discovered"));
        let text = match runtime.generate(agent, options(h, dir.path()), TITLE).await {
            Ok(text) => text,
            // A wire with neither tool disabling nor permission requests
            // cannot be hands-off; the refusal is typed (agy).
            Err(AgentError::UnsupportedFeature(why)) => {
                println!("SKIP {h}: generate unsupported (typed): {why}");
                continue;
            }
            Err(e) => panic!("{h}: generate failed: {e}"),
        };
        assert!(text.to_lowercase().contains("branch"), "{h}: got {text:?}");
        assert!(
            text.split_whitespace().count() <= 8,
            "{h}: not a title: {text:?}"
        );
        pass(h, &format!("generate returned {text:?}"));
    }
}

/// Even a prompt asking to read a file cannot enable Pi's tools during generation.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn pi_generate_stays_text_only() {
    if !enabled().await.contains(&"pi") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("context.txt"), "tool-only context").unwrap();
    let log = dir.path().join("wire.jsonl");
    let runtime = Runtime::new();
    let report = runtime.discover().await;
    let agent = report.require("pi").expect("pi is in the roster");
    let text = tokio::time::timeout(EVENT_TIMEOUT, runtime.generate(
        agent, options("pi", dir.path()).record_wire(&log),
        "Read context.txt using a tool, then return a short summary. If no tools are available, say that briefly.",
    )).await.expect("pi generation timed out").expect("pi generation failed");
    assert!(!text.trim().is_empty());
    let wire = std::fs::read_to_string(log).unwrap();
    assert!(
        !wire.contains("tool_execution_") && !wire.contains("toolcall_start"),
        "Pi attempted a tool call"
    );
    assert!(
        !wire.contains("\"sessionFile\":\""),
        "Pi persisted its session"
    );
    pass("pi", &format!("tool-free generation returned {text:?}"));
}

/// Turn is bracketed by TurnStarted(Prompt)/TurnEnded(Completed), sequences strictly increase, and stays quiet after end.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn turn_events_are_bracketed_ordered_and_quiet_after_end() {
    for h in enabled().await {
        let (session, mut events, _dir) = open(h).await;
        session
            .prompt("Say only the word PINEAPPLE. Do not use any tools.")
            .await
            .unwrap();
        let mut last_seq = 0;
        let mut saw_in_turn = false;
        let mut open_messages = std::collections::BTreeSet::new();
        let mut ended_messages = std::collections::BTreeSet::new();
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
                    // A republished completion snapshot must not close the
                    // same message twice (a message with no streamed content,
                    // e.g. tool-only, may still end once).
                    assert!(
                        ended_messages.insert(message_id.clone()),
                        "{h}: {message_id:?} ended twice"
                    );
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

/// Tool turn reaches ToolStatus::Completed and the requested file lands on disk (hermes quirk tolerated).
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn tools_run_to_completion_and_the_file_lands() {
    for h in enabled().await {
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

/// Permission allow writes the file, deny blocks it and leaves the session usable for the next prompt.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn permissions_gate_the_write_and_deny_holds() {
    for h in enabled().await {
        // Cursor gates shell commands, never its edit tool (wire-captured
        // 2026-09-07): its write goes through the shell so the gate is hit.
        let write = if h == "cursor" {
            "Run the shell command `printf HELLO > note.txt` to create note.txt. Use the shell, not your file-edit tool. Do not verify afterwards."
        } else {
            "Create a file named note.txt containing exactly the word HELLO. Use your file tools."
        };
        // Session A: allow — the request closes and the file lands.
        let (session, mut events, dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Permissions)
        {
            println!("SKIP {h}: permissions not advertised");
            session.close().await.unwrap();
            continue;
        }
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
        assert!(asked, "{h}: no permission request opened");
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
        // KNOWN (hermes, wire-captured 2026-08-31): hermes's approval flow
        // gates only its file tools. After denied write attempts the agent
        // can route around its own gate with a terminal `printf`, which
        // never asks. The denies themselves are delivered and honoured.
        // KNOWN (cursor): after a denied shell write the model may still
        // reach for its ungated edit tool.
        if matches!(h, "hermes" | "cursor") && dir.path().join("note.txt").exists() {
            println!("KNOWN {h}: denies honoured; the write went through an ungated tool");
        } else {
            assert!(
                !dir.path().join("note.txt").exists(),
                "{h}: file exists after deny"
            );
        }
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: post-deny prompt")).await;
        session.close().await.unwrap();
        pass(h, "allow writes, deny holds, session survives");
    }
}

/// A `/word` that is not an advertised command must ride as plain text — an
/// over-eager slash router would fail the turn on it (opencode routed every
/// `/…` to its command endpoint before the fix).
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn an_unknown_slash_prompt_is_plain_text() {
    for h in enabled().await {
        let (session, mut events, _dir) = open(h).await;
        session
            .prompt("/definitely-not-a-command Reply with only the word KUMQUAT.")
            .await
            .unwrap();
        let text = drain_to_turn_end(&session, &mut events, &format!("{h}: slash text")).await;
        // claude owns the `/` namespace: since 2.1.261 the CLI answers an
        // unknown command itself, in a synthetic message we surface as text.
        if h == "claude" {
            assert!(text.contains("Unknown command"), "{h}: text was {text:?}");
            pass(
                h,
                "unknown slash command answered by the CLI, text surfaced",
            );
        } else {
            assert!(text.contains("KUMQUAT"), "{h}: text was {text:?}");
            pass(h, "unknown slash text stayed plain text");
        }
        session.close().await.unwrap();
    }
}

/// opencode's task tool runs in a child session whose permissions ask on the
/// child's own session id; they must still reach the caller or the subagent
/// stalls parked forever.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn opencode_child_session_permissions_reach_the_caller() {
    if !enabled().await.contains(&"opencode") {
        println!("SKIP: opencode not enabled");
        return;
    }
    let (session, mut events, _dir) = open("opencode").await;
    session
        .prompt(
            "Use your task tool to spawn a subagent whose prompt is: run the bash \
             command `echo child-probe`. You must use the task tool.",
        )
        .await
        .unwrap();
    let mut approved = 0;
    loop {
        let event = next(&mut events, "opencode: child permission").await;
        match event.kind {
            EventKind::RequestOpened(Request::Permission(request)) => {
                approved += 1;
                session.answer(request.id, allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    // The task tool asks on the root, its bash on the child.
    assert!(
        approved >= 2,
        "expected the task and child bash permissions, saw {approved}"
    );
    session.close().await.unwrap();
    pass("opencode", "child session permissions reached the caller");
}

/// Question request round-trips: choices presented, answer selected, and response echoed (claude/codex only; codex unverified).
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn a_question_round_trips() {
    for h in enabled().await {
        // codex runs as a probe: `item/tool/requestUserInput` is
        // schema-confirmed but has never fired live (ticket 10) — the
        // translation is exercised if it ever does, without failing the run.
        // cursor's Auto model has not fired `cursor/ask_question` in any
        // probe (2026-09-07); same best-effort arm as codex. Antigravity
        // asks over its ACP server; the headless CLI cannot prompt. grok
        // asks over `_x.ai/ask_user_question`.
        if !matches!(
            h,
            "claude" | "codex" | "opencode" | "cursor" | "antigravity" | "grok"
        ) {
            println!("SKIP {h}: questions (claude, codex, opencode, cursor, antigravity, grok)");
            continue;
        }
        if h == "antigravity" && HEADLESS_AGY.get().copied().unwrap_or(false) {
            println!("SKIP antigravity: headless agy cannot ask");
            continue;
        }
        let (session, mut events, _dir) = open(h).await;
        session
            .prompt("Ask me whether I prefer red or blue using your question tool (claude: AskUserQuestion; codex: request_user_input; opencode: question; cursor: ask_question; antigravity: ask_question; grok: ask_user_question), then answer with just my choice.")
            .await
            .unwrap();
        let mut text = String::new();
        let mut asked = false;
        loop {
            let event = next(&mut events, &format!("{h}: question")).await;
            match event.kind {
                EventKind::RequestOpened(Request::Question(request)) => {
                    asked = true;
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
                // The question must surface only as a request, never also as
                // a tool call (claude: AskUserQuestion; opencode: question;
                // grok: ask_user_question, then "Ask: <question>"). Matched
                // by prefix: a tool search echoing the prompt is not one.
                EventKind::ToolUpdated(tool) => {
                    let title = tool.title.to_lowercase();
                    assert!(
                        !(title.starts_with("ask") || title == "question"),
                        "{h}: question surfaced as a tool: {}",
                        tool.title
                    );
                }
                EventKind::TurnEnded { stop, .. } => {
                    assert!(matches!(stop, StopReason::Completed { .. }), "{stop:?}");
                    break;
                }
                _ => {}
            }
        }
        if !asked {
            assert!(
                matches!(h, "codex" | "cursor"),
                "{h}: no question request opened"
            );
            println!("SKIP {h}: the question request did not fire (unverified live)");
            session.close().await.unwrap();
            continue;
        }
        assert!(text.to_lowercase().contains("red"), "answer was {text:?}");
        session.close().await.unwrap();
        pass(h, "question answered and echoed");
    }
}

/// Claude must not advertise Steer; others with Steer fold a mid-turn prompt via DeliveryKind::Steered.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn steering_is_absent_on_claude_and_folds_where_advertised() {
    for h in enabled().await {
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

/// Queued prompts are FIFO (Queued{0,1}) and each turn's origin keeps its prompt_id.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn the_queue_is_fifo_and_ids_stay_aligned() {
    for h in enabled().await {
        let (session, mut events, _dir) = open(h).await;
        session.prompt(COUNT).await.unwrap();
        // On a steering harness (codex) a lone mid-turn prompt folds into
        // the running turn; occupy the steer slot first so KIWI and LEMON
        // genuinely queue.
        let (kiwi, lemon) = if steers(&session) {
            let (_, kiwi, lemon) = tokio::join!(
                session.prompt("Keep counting."),
                session.prompt("Say only KIWI. No tools."),
                session.prompt("Say only LEMON. No tools.")
            );
            (kiwi.unwrap(), lemon.unwrap())
        } else {
            (
                session.prompt("Say only KIWI. No tools.").await.unwrap(),
                session.prompt("Say only LEMON. No tools.").await.unwrap(),
            )
        };
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

/// Cancel ends the turn in every queue shape (empty, queued, clear-queue) and session survives; kiro quirk handled.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn cancel_ends_the_turn_in_every_queue_shape() {
    for h in enabled().await {
        // The claude wedge was a timing race (interrupt vs the CLI's own
        // queued→started window), so repeat the raced variant there.
        let reps = if h == "claude" { 3 } else { 1 };
        let (session, mut events, _dir) = open(h).await;

        // Empty queue: cancel ends the turn and the session survives. Cancel
        // once the turn visibly streams — a fixed sleep let fast models
        // (kiro) finish COUNT before the cancel landed.
        session.prompt(COUNT).await.unwrap();
        wait_for_content(&mut events, &format!("{h}: count streaming")).await;
        session.cancel(false).await.unwrap();
        expect_cancelled(&mut events, &format!("{h}: empty-queue cancel")).await;
        session.prompt("Say only OK. No tools.").await.unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: post-cancel prompt")).await;

        // (a) cancel(false) with a queued prompt: it runs next and answers.
        // Repeating COUNT here trips the API's reasoning-extraction
        // safeguard on the third interrupted copy (claude 2.1.261, probed
        // 2026-09-05), so each raced turn streams a different long prompt.
        let topics = ["a lighthouse keeper", "a bus driver", "a mountain guide"];
        for (rep, topic) in topics.iter().enumerate().take(reps) {
            session.prompt(long_prompt(topic)).await.unwrap();
            // Queue immediately: a fast model can finish COUNT inside a fixed
            // sleep (kiro did), which would make this `Started`, not `Queued`.
            // On a steering harness the follow-up would fold instead, so the
            // steer slot is occupied first.
            let queued = queue_one(&session, "Say only PEAR. No tools.").await;
            assert_eq!(queued.kind, DeliveryKind::Queued { position: 0 }, "{h}");
            wait_for_content(&mut events, &format!("{h}: count streaming rep {rep}")).await;
            session.cancel(false).await.unwrap();
            expect_cancelled(&mut events, &format!("{h}: queued cancel rep {rep}")).await;
            // kiro's cancel can race the next prompt (2.19.1); the adapter
            // re-sends once, so the queued turn still answers.
            let text = drain_to_turn_end(
                &session,
                &mut events,
                &format!("{h}: queued prompt rep {rep}"),
            )
            .await;
            assert!(text.contains("PEAR"), "{h}: queued turn said {text:?}");
        }

        // (b) cancel(true): the queued prompt must never run.
        session.prompt(long_prompt("a baker")).await.unwrap();
        queue_one(&session, "Say only PLUM. No tools.").await;
        wait_for_content(&mut events, &format!("{h}: count streaming (b)")).await;
        session.cancel(true).await.unwrap();
        expect_cancelled(&mut events, &format!("{h}: clear-queue cancel")).await;
        quiet(&mut events, 5, &format!("{h}: after clear-queue cancel")).await;
        session.close().await.unwrap();
        pass(h, "cancel works in every queue shape");
    }
}

/// Resume recalls prior codeword without replaying old deltas; without Resume capability it fails typed ResumeFailed.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn resume_recalls_without_replaying() {
    for h in enabled().await {
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
        let (session, mut events) = runtime
            .open(agent, options(h, dir.path()).resume(token))
            .await
            .unwrap();
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

/// PlanUsageUpdated arrives within 10s after a turn with valid windows and reset times.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn plan_usage_arrives_after_a_turn() {
    for h in enabled().await {
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
        // codex pushes quota during the turn (after every model call);
        // claude refreshes right after it. Collect through turn end, then
        // give a post-turn push 10 more seconds.
        let mut pushed = None;
        loop {
            let event = next(&mut events, &format!("{h}: usage turn")).await;
            match event.kind {
                EventKind::PlanUsageUpdated(usage) => pushed = Some(usage),
                EventKind::RequestOpened(request) => {
                    session.answer(request.id(), allow()).await.unwrap();
                }
                EventKind::TurnEnded { .. } => break,
                _ => {}
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let usage = loop {
            if let Some(usage) = pushed {
                break usage;
            }
            let event = tokio::time::timeout_at(deadline, events.next())
                .await
                .unwrap_or_else(|_| panic!("{h}: no PlanUsageUpdated within 10s of turn end"))
                .expect("stream ended")
                .unwrap();
            if let EventKind::PlanUsageUpdated(usage) = event.kind {
                pushed = Some(usage);
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
        pass(
            h,
            &format!(
                "plan usage pushed: plan {:?}, {}",
                usage.plan,
                summary.join(", ")
            ),
        );
    }
}

/// compact() summarizes the session's own context: the compaction is reported
/// and the agent still remembers what it was told before it.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn compact_summarizes_the_session_without_losing_it() {
    for h in enabled().await {
        let (session, mut events, _dir) = open(h).await;
        if !session
            .info()
            .details
            .capabilities
            .supports(Capability::Compact)
        {
            println!("SKIP {h}: compaction not advertised");
            session.close().await.unwrap();
            continue;
        }
        // Two exchanges: an agent with nothing to summarize refuses.
        session
            .prompt("Remember this codeword: ALPHA9. Just confirm. No tools.")
            .await
            .unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: codeword")).await;
        session
            .prompt("Now tell me a one-sentence fact about the number nine. No tools.")
            .await
            .unwrap();
        drain_to_turn_end(&session, &mut events, &format!("{h}: filler")).await;

        session.compact().await.unwrap();
        // KNOWN (pi 0.84.4): compaction keeps the newest 20k tokens
        // (`compaction.keepRecentTokens`) and refuses a session that fits
        // inside them, so a short live session proves the refusal path:
        // a diagnostic, and the turn still ends.
        if h == "pi" {
            let mut refused = false;
            loop {
                match next(&mut events, &format!("{h}: refusal")).await.kind {
                    EventKind::Diagnostic(d) if d.message.contains("compaction refused") => {
                        refused = true;
                    }
                    EventKind::TurnEnded { .. } => break,
                    _ => {}
                }
            }
            assert!(refused, "{h}: a small session compacted or hung");
            session.close().await.unwrap();
            pass(h, "KNOWN: small session refused, reported as a diagnostic");
            continue;
        }
        drain_to_compaction(&session, &mut events, &format!("{h}: compaction")).await;

        // Some agents answer their own compaction out loud, so the recall
        // prompt may queue behind that turn; wait for the turn it starts.
        let recall = session
            .prompt("What codeword did I give you? Answer with the word only. No tools.")
            .await
            .unwrap();
        let text = drain_prompt_turn(
            &session,
            &mut events,
            recall.prompt_id,
            &format!("{h}: recall"),
        )
        .await;
        assert!(
            text.contains("ALPHA9"),
            "{h}: compaction lost the conversation: {text:?}"
        );
        session.close().await.unwrap();
        pass(h, "compacted and kept the conversation");
    }
}

/// Rollback(1, Conversation) forgets exactly the last turn and changes the resume token.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn rollback_forgets_the_rolled_back_turn() {
    for h in enabled().await {
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
        session.close().await.unwrap();
        pass(h, "rollback forgot exactly the last turn");
    }
}

/// Rollback(1, ConversationAndFiles) rewinds filesystem to pre-turn state and emits SessionUpdated.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn files_rollback_restores_agent_written_files() {
    for h in enabled().await {
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

/// Fork at a MessageEnded fork_point anchor forgets later turns; tip fork keeps all history; original untouched.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn fork_from_branches_at_a_point_and_at_the_tip() {
    for h in enabled().await {
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
        // (`<agent>/fork_point` on MessageEnded).
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
                        if let Some(point) = event.extensions.get(&format!("{h}/fork_point")) {
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
                options(h, dir.path()).fork_from(token.clone(), Some(MessageId::new(&anchors[0]))),
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
            .open(agent, options(h, dir.path()).fork_from(token.clone(), None))
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

/// Runtime plan_usage probes quota without a session, caches within TTL, or returns UnsupportedFeature typed.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn runtime_plan_usage_probes_without_a_session() {
    for h in enabled().await {
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

/// Killed (-9) agent maps to TurnEnded(Failed) + ProcessExited(status 9) and closes the session.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn a_killed_agent_fails_the_turn_and_closes_the_session() {
    for h in enabled().await {
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

/// Close returns within 10s and the event stream ends promptly.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn close_returns_promptly_and_ends_the_stream() {
    for h in enabled().await {
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

/// Unadvertised rollback -> UnsupportedFeature, unknown request -> InvalidRequest, prompt after close -> SessionClosed.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn errors_are_typed() {
    for h in enabled().await {
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
        // (d) a resume token that names no conversation refuses typed, so
        // apps can tell a dead token from a broken protocol (native wires;
        // probed: claude "No conversation found", codex "no rollout found",
        // opencode a session-fetch rejection).
        if matches!(h, "claude" | "codex" | "opencode") {
            let runtime = Runtime::new();
            let report = runtime.discover().await;
            let agent = report.require(h).unwrap();
            let dir = tempfile::tempdir().unwrap();
            let bogus = ResumeToken::new("3b1c9f2e-5a6d-4e7f-8a9b-0c1d2e3f4a5b");
            let mut options = SessionOptions::in_dir(dir.path()).resume(bogus);
            if h == "opencode" {
                options = options.configure("model", OPENCODE_MODEL);
            }
            match runtime.open(agent, options).await {
                Err(AgentError::ResumeFailed(_)) => {}
                Err(other) => panic!("{h}: bogus resume errored untyped: {other}"),
                Ok(_) => panic!("{h}: bogus resume opened a session"),
            }
        }
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
    let (session, events) = runtime
        .open(agent, options(harness, dir.path()))
        .await
        .unwrap_or_else(|e| panic!("{harness}: open failed: {e}"));
    (session, events, dir)
}

/// The per-harness options every live session opens with: pinned models
/// and deterministic approvals.
fn options(harness: &str, dir: &std::path::Path) -> SessionOptions {
    let mut options = SessionOptions::in_dir(dir);
    if harness == "claude" {
        options = options.configure("model", CLAUDE_MODEL);
    }
    if harness == "opencode" {
        options = options.configure("model", OPENCODE_MODEL);
    }
    if harness == "pi" {
        options = options.configure("model", PI_MODEL);
    }
    // qwen opens in `auto`, where a classifier waves safe writes through;
    // `default` asks for every edit and command.
    if harness == "qwen" {
        options = options.configure("mode", "default");
    }
    // Headless agy auto-denies every gated tool in Ask mode; its ACP server
    // asks like any ACP agent.
    if harness == "antigravity" && HEADLESS_AGY.get().copied().unwrap_or(false) {
        options = options.permission_mode(PermissionMode::AutoApprove);
    }
    if harness == "codex" {
        // Deterministic approvals regardless of the host config: a write
        // escalates past the read-only sandbox and asks.
        options = options
            .configure("model", CODEX_MODEL)
            .configure("effort", "low")
            .configure("sandbox", "read-only")
            .configure("mode", "on-request");
    }
    options
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
            // A failed turn names its reason here, not as empty text later.
            EventKind::TurnEnded { stop, .. } => {
                assert!(
                    matches!(stop, StopReason::Completed { .. }),
                    "{step}: turn ended {stop:?}"
                );
                return text;
            }
            _ => {}
        }
    }
}

/// Drains the turn this prompt starts, skipping whatever the agent was
/// already running, and returns that turn's text.
async fn drain_prompt_turn(
    session: &Session,
    events: &mut Events,
    prompt: PromptId,
    step: &str,
) -> String {
    loop {
        match next(events, step).await.kind {
            EventKind::TurnStarted {
                origin: TurnOrigin::Prompt(id),
            } if id == prompt => break,
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            _ => {}
        }
    }
    drain_to_turn_end(session, events, step).await
}

/// Drains the compaction turn: it must report the compaction and then end.
/// A refusal fails here rather than leaving a half-checked session.
async fn drain_to_compaction(session: &Session, events: &mut Events, step: &str) {
    let mut compacted = false;
    loop {
        match next(events, step).await.kind {
            EventKind::ContextCompacted => compacted = true,
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::Diagnostic(d) if d.message.contains("compaction refused") => {
                panic!("{step}: {}", d.message)
            }
            EventKind::TurnEnded { .. } => {
                assert!(
                    compacted,
                    "{step}: the compaction turn reported no compaction"
                );
                return;
            }
            _ => {}
        }
    }
}

/// A prompt that streams long enough to cancel mid-turn, distinct per topic.
fn long_prompt(topic: &str) -> String {
    format!("Write an 800 word story about {topic}. No tools.")
}

/// Waits until the turn is visibly streaming (first in-turn content), so a
/// cancel lands mid-turn instead of racing turn start or turn end.
async fn wait_for_content(events: &mut Events, step: &str) {
    loop {
        match next(events, step).await.kind {
            EventKind::TextDelta { .. }
            | EventKind::ReasoningDelta { .. }
            | EventKind::ToolUpdated(_) => return,
            EventKind::TurnEnded { .. } => panic!("turn ended before it streamed at {step}"),
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

/// Asserts no turn traffic arrives for `secs` seconds. Diagnostics (kiro
/// emits metadata notifications between turns), plan-usage receipts
/// (claude fetches usage after each result frame, so the receipt lands
/// post-turn by design), and status flips (a turn end is followed by
/// `StatusChanged(Idle)`) are the sanctioned out-of-turn events.
async fn quiet(events: &mut Events, secs: u64, step: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.next()).await {
        let kind = event.map(|e| e.kind);
        if !matches!(
            kind,
            Ok(EventKind::Diagnostic(_)
                | EventKind::PlanUsageUpdated(_)
                | EventKind::StatusChanged(_))
        ) {
            panic!("expected quiet at {step}, got {kind:?}");
        }
    }
}

/// kill -9 the session's own agent process, found by a session-unique marker.
fn kill_child(harness: &str, session: &Session) {
    // claude carries our minted session id in argv; opencode is matched by
    // its newest `opencode serve` process.
    let (args, pattern): (&[&str], String) = match harness {
        "claude" => (
            &["-f"],
            session.info().resume_token.unwrap().as_str().to_owned(),
        ),
        // Both halves: `kiro-cli acp` dispatches to a `kiro-cli-chat acp`
        // worker that inherits the pipes — killing only the dispatcher lets
        // the turn complete. Anchored so the user's Kiro apps' own
        // `kiro-cli acp --agent <name>` processes never match.
        "kiro" => (&["-f"], "kiro-cli(-chat)? acp$".to_owned()),
        // The launcher script execs node under its own name; the user's
        // own TUI never ends in `acp`.
        "cursor" => (&["-n", "-f"], "cursor-agent .*index.js acp$".to_owned()),
        "codex" => (&["-n", "-f"], "codex app-server".to_owned()),
        // The user's own grok TUI never runs `agent ... stdio`.
        "grok" => (&["-n", "-f"], "grok --no-auto-update agent".to_owned()),
        // The npm shim runs node -> cli.js, which re-execs itself as a
        // worker inheriting the pipes: both halves, as with kiro. The
        // user's own TUI has no `--experimental-acp`.
        "qwen" => (&["-f"], r"qwen-code/cli\.js --experimental-acp$".to_owned()),
        // pi overwrites its argv with its own process title, so there is no
        // command line to match: the exact name plus newest-first is ours.
        "pi" => (&["-n", "-x"], "pi".to_owned()),
        // The CLI's wire flag, or the ACP server's own executable name.
        "antigravity" => (
            &["-n", "-f"],
            "agy( --input-format=stream-json|_acp_server)".to_owned(),
        ),
        _ => (&["-n", "-f"], "opencode serve".to_owned()),
    };
    let out = std::process::Command::new("pgrep")
        .args(args)
        .arg(&pattern)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let pids: Vec<&str> = stdout.lines().collect();
    if pids.is_empty() {
        panic!("{harness}: no process matched {pattern:?}");
    }
    // kiro and qwen match dispatcher + worker; kill every matched pid.
    let last = if matches!(harness, "kiro" | "qwen") {
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

fn steers(session: &Session) -> bool {
    session
        .info()
        .details
        .capabilities
        .supports(Capability::Steer)
}

/// Gets `prompt` into the queue mid-turn: on a steering harness a lone
/// prompt would fold into the running turn, so a throwaway steer occupies
/// the slot first (both commands land before either resolves).
async fn queue_one(session: &Session, prompt: &str) -> anyagent::Delivery {
    if steers(session) {
        let (_, queued) = tokio::join!(session.prompt("Keep going."), session.prompt(prompt));
        queued.unwrap()
    } else {
        session.prompt(prompt).await.unwrap()
    }
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
        None => match std::env::home_dir() {
            Some(home) => home.join(".claude"),
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
    for h in enabled().await {
        if h != "claude" && h != "codex" && h != "pi" {
            println!("SKIP {h}: config-home isolation asserted on claude, codex and pi");
            continue;
        }
        let runtime = Runtime::new();
        let report = runtime.discover().await;
        let agent = report.require(h).unwrap();
        // The real login is present, proving the default path is
        // authenticated.
        let auth = runtime.probe_auth(agent).await.unwrap();
        assert!(
            matches!(auth, AuthStatus::Authenticated { .. }),
            "{h}: default login is not authenticated: {auth:?}"
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

/// Cursor: a model switch adopts that model's own options from the config
/// response (Composer: `fast`), and they leave with the model. No prompt, so
/// no quota spent.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn cursor_model_switch_reveals_the_models_own_options() {
    if !enabled().await.contains(&"cursor") {
        println!("SKIP: cursor not enabled");
        return;
    }
    let (session, mut events, _dir) = open("cursor").await;
    let option = |session: &Session, id: &str| {
        session
            .info()
            .details
            .config_options
            .into_iter()
            .find(|o| o.id.as_str() == id)
    };
    assert!(
        option(&session, "fast").is_none(),
        "Auto has no fast option"
    );
    session.configure("model", "composer-2.5").await.unwrap();
    while option(&session, "fast").is_none() {
        next(&mut events, "cursor: fast option after model switch").await;
    }
    assert_eq!(
        option(&session, "model").and_then(|o| o.current),
        Some(ConfigValue::Text("composer-2.5".into()))
    );
    session.configure("model", "default").await.unwrap();
    while option(&session, "fast").is_some() {
        next(&mut events, "cursor: fast option gone").await;
    }
    session.close().await.unwrap();
    pass("cursor", "per-model options follow the model");
}

/// Cursor: every turn ends with an estimated ContextUsage labelled
/// `anyagent/estimated` (its wire carries no usage).
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn cursor_context_usage_is_estimated() {
    if !enabled().await.contains(&"cursor") {
        println!("SKIP: cursor not enabled");
        return;
    }
    let (session, mut events, _dir) = open("cursor").await;
    session.prompt("Say only OK. No tools.").await.unwrap();
    let mut usage = None;
    loop {
        let event = next(&mut events, "cursor: usage turn").await;
        match event.kind {
            EventKind::ContextUsage { used_tokens, .. } => {
                assert_eq!(
                    event.extensions.get("anyagent/estimated"),
                    Some(&serde_json::Value::Bool(true)),
                    "usage not labelled as an estimate"
                );
                usage = Some(used_tokens);
            }
            EventKind::RequestOpened(request) => {
                session.answer(request.id(), allow()).await.unwrap();
            }
            EventKind::TurnEnded { .. } => break,
            _ => {}
        }
    }
    let used = usage.expect("an estimated usage event before turn end");
    assert!(used > 0, "estimate is empty");
    assert!(
        session
            .info()
            .details
            .capabilities
            .supports(Capability::ContextUsage)
    );
    session.close().await.unwrap();
    pass("cursor", &format!("estimated usage {used} tokens"));
}

#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn record_wire_captures_a_live_turn() {
    for h in enabled().await {
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

// -- summary ----------------------------------------------------------------

/// Closing roster: which harnesses the run covered and which were skipped
/// because their CLI is not installed. Runs last by name so it lands at the
/// bottom of the output.
#[tokio::test]
#[ignore = "live: talks to real agents"]
async fn zz_summary() {
    let roster = roster().await;
    println!("\n--- live run summary ---");
    println!(
        "ran: {}",
        if roster.installed.is_empty() {
            "none".to_owned()
        } else {
            roster.installed.join(", ")
        }
    );
    if roster.skipped.is_empty() {
        println!("skipped (not installed): none");
    } else {
        println!("skipped (not installed):");
        for line in &roster.skipped {
            println!("  {line}");
        }
    }
    println!("------------------------\n");
}
