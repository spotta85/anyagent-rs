//! Sessions: run several at once, then resume one later.
//!
//! ```sh
//! cargo run --example sessions -- claude
//! ```
//!
//! A `Session` is a handle plus an event stream. Sessions are independent —
//! open as many as you want, on one agent or several, and drive them
//! concurrently. The resume token lets you close a session and pick the
//! conversation back up in a new process.

use anyagent::{
    Answer, EventKind, Events, PermissionChoice, Request, Runtime, Session, SessionOptions,
    StopReason,
};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let id = std::env::args().nth(1).unwrap_or_else(|| "claude".into());
    let runtime = Runtime::new();
    let report = runtime.discover().await;
    let agent = report.require(&id)?;

    // Two sessions on the same agent, same directory. Each `open` spawns its
    // own agent process; nothing is shared between them.
    let options = || SessionOptions::in_dir(".");
    let (s1, mut e1) = runtime.open(agent, options()).await?;
    let (s2, mut e2) = runtime.open(agent, options()).await?;

    // Drive both turns concurrently; each drains its own event stream.
    let (a, b) = tokio::join!(
        turn(&s1, &mut e1, "Pick a color and say only its name."),
        turn(&s2, &mut e2, "Pick an animal and say only its name."),
    );
    println!("session 1: {}", a?.trim());
    println!("session 2: {}", b?.trim());

    // The resume token is minted at open and survives the process: store it
    // with your transcript, and any later process can continue the session.
    let token = s1.info().resume_token.expect("token exists at open");
    s1.close().await?;
    s2.close().await?;

    let (s3, mut e3) = runtime.open(agent, options().resume(token)).await?;
    let answer = turn(&s3, &mut e3, "What did you pick? One word.").await?;
    println!("resumed:   {}", answer.trim()); // same color — the agent remembers
    s3.close().await?;
    Ok(())
}

/// One full turn: prompt, then drain events until the turn ends, collecting
/// the streamed text. Every agent produces this same event shape.
async fn turn(
    session: &Session,
    events: &mut Events,
    prompt: &str,
) -> Result<String, anyagent::AgentError> {
    session.prompt(prompt).await?;
    let mut text = String::new();
    while let Some(event) = events.next().await {
        match event?.kind {
            EventKind::TextDelta { text: t, .. } => text.push_str(&t),
            // Agents may ask before running a tool; a real app forwards this
            // to the user (see the `chat` example). Here we allow once.
            EventKind::RequestOpened(Request::Permission(request)) => {
                session
                    .answer(request.id, Answer::Permission(PermissionChoice::AllowOnce))
                    .await?;
            }
            EventKind::TurnEnded { stop, .. } => {
                if let StopReason::Failed { message } = stop {
                    text = format!("(turn failed: {message})");
                }
                break;
            }
            _ => {}
        }
    }
    Ok(text)
}
