//! Terminal chat with an installed agent.
//!
//! ```sh
//! cargo run --example chat -- claude
//! ```

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
    println!("· {} at {}", agent.name, agent.executable.display());

    let (session, mut events) = runtime.open(agent, SessionOptions::in_dir(".")).await?;
    println!("· connected — type a message\n");

    let prompter = session.clone();
    tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            if let Err(e) = prompter.prompt(line).await {
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
            EventKind::TurnEnded { stop, .. } => match stop {
                StopReason::Completed { .. } => println!("\n"),
                other => println!("\n· turn ended: {other:?}\n"),
            },
            _ => {}
        }
    }
    Ok(())
}
