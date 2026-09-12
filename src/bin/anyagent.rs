//! The `anyagent` binary: `list` and `chat` for a terminal, `serve` for
//! apps in other languages (the JSONL sidecar in `anyagent::sidecar`).

use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyagent::{
    AgentInstallation, Answer, AuthStatus, Capability, ConfigKind, EventKind, PermissionChoice,
    Request, Runtime, SessionOptions, SessionStatus, StopReason,
};
use futures::StreamExt;
use tokio::io::AsyncBufReadExt;

const USAGE: &str = "usage: anyagent list | chat [agent] | serve [--mock script.json]";

type Fallible = Result<(), Box<dyn Error>>;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("list") => list().await,
        Some("chat") => chat(args.get(1).map_or("claude", String::as_str)).await,
        Some("serve") => serve(&args[1..]).await,
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    if let Err(e) = result {
        eprintln!("anyagent: {e}");
        std::process::exit(1);
    }
}

/// `serve`: the sidecar over stdin and stdout. `--mock <file>` plays a
/// script instead of real agents.
async fn serve(args: &[String]) -> Fallible {
    let runtime = match args {
        [] => Runtime::new(),
        [flag, file] if flag == "--mock" => mock_runtime(file)?,
        _ => return Err(USAGE.into()),
    };
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    Ok(anyagent::sidecar::serve(runtime, stdin, tokio::io::stdout()).await?)
}

/// `list`: one line per installed agent, then upgrades and missing agents.
async fn list() -> Fallible {
    let runtime = Runtime::new();
    let report = runtime.discover().await;
    // Each probe spawns one short-lived agent process; run them together.
    let probes = report.agents.iter().map(|agent| describe(&runtime, agent));
    for line in futures::future::join_all(probes).await {
        println!("{line}");
    }
    for agent in &report.agents {
        if let Some(upgrade) = &agent.upgrade {
            println!(
                "{:<10} works without {} — install it for more: {}",
                agent.id, upgrade.name, upgrade.install_hint
            );
        }
    }
    for missing in &report.missing {
        println!(
            "{:<10} not installed — {}",
            missing.id, missing.install_hint
        );
    }
    Ok(())
}

/// `chat`: prompt from stdin, stream the answer, allow every permission.
/// `/set <option> <value>` changes a live setting. Ends once stdin has
/// ended and the session is idle, so a piped prompt runs one turn and exits.
async fn chat(id: &str) -> Fallible {
    let runtime = Runtime::new();
    let report = runtime.discover().await;
    let agent = report.require(id)?;
    println!("· {} at {}", agent.name, agent.executable_path.display());

    let (session, mut events) = runtime.open(agent, SessionOptions::in_dir(".")).await?;
    println!("· connected — type a message\n");

    // Prompt from one task, drain events in another; a line sent mid-turn
    // steers the running turn. `eof` is set before the idle check, so an
    // idle event can never slip between the two.
    let eof = Arc::new(AtomicBool::new(false));
    let (prompter, stdin_eof) = (session.clone(), Arc::clone(&eof));
    tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let result = match line
                .strip_prefix("/set ")
                .and_then(|rest| rest.split_once(' '))
            {
                Some((option, value)) => prompter.configure(option, value).await.map(|_| ()),
                None if line.starts_with("/set") => {
                    Err(anyagent::AgentError::InvalidConfiguration(
                        "usage: /set <option> <value>".into(),
                    ))
                }
                None => prompter.prompt(line).await.map(|_| ()),
            };
            if let Err(e) = result {
                eprintln!("! {e}");
            }
        }
        stdin_eof.store(true, Ordering::SeqCst);
        if prompter.status() == SessionStatus::Idle {
            let _ = prompter.close().await;
        }
    });

    while let Some(event) = events.next().await {
        match event?.kind {
            EventKind::TextDelta { text, .. } => {
                print!("{text}");
                std::io::stdout().flush()?;
            }
            EventKind::ToolUpdated(tool) => eprintln!("  [{:?}] {}", tool.status, tool.title),
            EventKind::RequestOpened(Request::Permission(request)) => {
                // The first allow the agent offers, else its first choice.
                let choice = request
                    .options
                    .iter()
                    .find(|c| {
                        matches!(
                            c,
                            PermissionChoice::AllowOnce | PermissionChoice::AllowAlways
                        )
                    })
                    .or(request.options.first())
                    .cloned();
                if let Some(choice) = choice {
                    eprintln!("  [{choice:?}] {}", request.tool.title);
                    session
                        .answer(request.id, Answer::Permission(choice))
                        .await?;
                }
            }
            EventKind::SessionUpdated(info) => {
                let set: Vec<String> = info
                    .configuration
                    .options
                    .iter()
                    .map(|(id, v)| format!("{id}={v:?}"))
                    .collect();
                eprintln!("  [config] {}", set.join(" "));
            }
            // Stdin is gone and nothing is running: the turn's output is
            // complete, so end the session and let the stream close.
            EventKind::StatusChanged(SessionStatus::Idle) if eof.load(Ordering::SeqCst) => {
                let _ = session.close().await;
            }
            EventKind::TurnEnded { stop, .. } => match stop {
                StopReason::Completed { .. } => println!("\n"),
                other => println!("\n· turn ended: {other:?}\n"),
            },
            _ => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// HELPERS: one agent's line, and the mock runtime
// ---------------------------------------------------------------------------

/// One agent, one line: login state, version, models, commands, capabilities.
async fn describe(runtime: &Runtime, agent: &AgentInstallation) -> String {
    let details = match runtime.probe(agent).await {
        Ok(details) => details,
        Err(e) => return format!("{:<10} probe failed: {e}", agent.id),
    };
    let auth = match &details.auth {
        AuthStatus::Authenticated { kind, account } => {
            let who = account
                .as_ref()
                .and_then(|a| a.email.as_deref().or(a.plan.as_deref()))
                .unwrap_or("logged in");
            format!("{kind:?}: {who}")
        }
        AuthStatus::Unauthenticated { login } => format!("logged out ({} ways in)", login.len()),
        _ => "unknown".into(),
    };
    let models = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .map_or(0, |o| match &o.kind {
            ConfigKind::Select { choices } => choices.len(),
            _ => 0,
        });
    let caps: Vec<&str> = [
        (Capability::Steer, "steer"),
        (Capability::Fork, "fork"),
        (Capability::Rollback, "rollback"),
        (Capability::PlanUsage, "plan-usage"),
    ]
    .into_iter()
    .filter(|(cap, _)| details.capabilities.supports(cap.clone()))
    .map(|(_, name)| name)
    .collect();
    format!(
        "{:<10} v{:<10} {auth:<28} {models} models · {} commands · {}",
        agent.id,
        details.version.as_deref().unwrap_or("?"),
        details.commands.len(),
        caps.join(", "),
    )
}

#[cfg(feature = "mock")]
fn mock_runtime(file: &str) -> Result<Runtime, Box<dyn Error>> {
    let script = serde_json::from_str(&std::fs::read_to_string(file)?)?;
    Ok(Runtime::with_mock(script))
}

#[cfg(not(feature = "mock"))]
fn mock_runtime(_file: &str) -> Result<Runtime, Box<dyn Error>> {
    Err("this build has no mock agent; rebuild with --features mock".into())
}
