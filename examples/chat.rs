//! Terminal chat with an installed agent — the core loop most apps build on.
//!
//! ```sh
//! cargo run --example chat -- claude     # or codex, grok, kiro, …
//! ```
//!
//! The shape: `discover` finds the agent, `open` gives you a `Session`
//! handle plus one `Events` stream, and everything the agent does — text,
//! tools, permission requests, turn boundaries — arrives on that stream as
//! the same typed events regardless of which agent is on the other end.
//!
//! `/set <option> <value>` changes a live setting, even mid-turn — try
//! `/set effort low` or `/set model sonnet`; the agent's own list is in
//! `session.info().details.config_options`.

use anyagent::{Answer, EventKind, PermissionChoice, Request, Runtime, SessionOptions, StopReason};
use futures::StreamExt;
use std::io::Write;
use tokio::io::AsyncBufReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let id = std::env::args().nth(1).unwrap_or_else(|| "claude".into());
    let runtime = Runtime::new();
    let report = runtime.discover().await;
    let agent = report.require(&id)?;
    println!("· {} at {}", agent.name, agent.executable_path.display());

    let (session, mut events) = runtime.open(agent, SessionOptions::in_dir(".")).await?;
    println!("· connected — type a message\n");

    // `Session` is cheap to clone; prompt from one task, drain events in
    // another. A prompt sent mid-turn steers the running turn (or queues,
    // if the agent can't steer) — no special API needed.
    let prompter = session.clone();
    tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            // `/set effort low` etc. configures a live option; the change
            // is confirmed by a `SessionUpdated` event, not by the call.
            let result = match line.strip_prefix("/set ") {
                Some(rest) => match rest.split_once(' ') {
                    Some((id, value)) => prompter.configure(id, value).await.map(|_| ()),
                    None => Err(anyagent::AgentError::InvalidConfiguration(
                        "usage: /set <option> <value>".into(),
                    )),
                },
                None if line == "/set" => Err(anyagent::AgentError::InvalidConfiguration(
                    "usage: /set <option> <value>".into(),
                )),
                None => prompter.prompt(line).await.map(|_| ()),
            };
            if let Err(e) = result {
                eprintln!("! {e}");
            }
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
                eprintln!("  [allow] {}", request.tool.title);
                session
                    .answer(request.id, Answer::Permission(PermissionChoice::AllowOnce))
                    .await?;
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
            EventKind::TurnEnded { stop, .. } => match stop {
                StopReason::Completed { .. } => println!("\n"),
                other => println!("\n· turn ended: {other:?}\n"),
            },
            _ => {}
        }
    }
    Ok(())
}
