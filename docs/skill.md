---
name: anyagent
description: Rust crate that drives the coding agents installed on a user's machine (Claude Code, Codex, Cursor, opencode, Kiro, Grok, Hermes, Qwen, pi, Antigravity) through one typed API. Read before writing or changing any code that calls anyagent, whether discovering agents, opening sessions, streaming events, answering permission requests, configuring models, or resuming and forking sessions.
license: MIT OR Apache-2.0
compatibility: Rust 1.88+, Tokio runtime. Agents must be installed and logged in on the machine.
metadata:
  author: spotta85
  version: "0.0.1"
---

# anyagent

One crate that finds the coding agents on a machine, speaks each one's
protocol (native or ACP), and exposes them all as three objects.

```text
Runtime::discover()  ->  Runtime::open()  ->  (Session, Events)

  your app ── prompt / answer / configure ──►  Session
                                                  │
                                                  ▼
                                            agent process
                                                  │
  your UI  ◄── text / tools / requests / usage ── Events
```

| Object | Role | Main calls |
|---|---|---|
| `Runtime` | Finds agents, opens sessions | `discover`, `probe`, `open`, `generate`, `plan_usage` |
| `Session` | Command handle for one live conversation | `prompt`, `answer`, `configure`, `cancel`, `rollback`, `compact`, `close` |
| `Events` | Stream of everything the agent does | one `match` on `EventKind` |

anyagent owns the processes, the wires, and the turn rules. Your app owns the
transcript, the UI, and the policy behind permission requests.

## Before you code

The docs are the source of truth; this file is the map. Before writing or
changing code that calls anyagent:

1. List the `Runtime` and `Session` calls and the `EventKind`s the change touches.
2. Read the page for each from the table. Fetch the URL (plain markdown), or query
   the Context7 MCP for library `/spotta85/anyagent-rs`. Done when every call
   you will make and every event you will handle has its row read.
3. Write the code, following the rules below.

| Need | Page |
|---|---|
| Install and first turn | https://anyagent.mintlify.site/quickstart.md |
| Every type, event, error, and `SessionOptions` builder | https://anyagent.mintlify.site/core-api.md |
| Each feature with a snippet (resume, fork, rollback, compact, MCP, mock) | https://anyagent.mintlify.site/features.md |
| Catalog, capability matrix, per-agent quirks, custom ACP agents | https://anyagent.mintlify.site/agents.md |
| Threads, streaming, persistence in a real app | https://anyagent.mintlify.site/building-an-app.md |
| Codebase map, adding an agent adapter | https://anyagent.mintlify.site/architecture.md |
| Full signatures | https://docs.rs/anyagent |
| Every page, one list | https://anyagent.mintlify.site/llms.txt |

## Core loop

```rust
use anyagent::{EventKind, Runtime, SessionOptions};
use futures::StreamExt;

let runtime = Runtime::new();
let report = runtime.discover().await;
let agent = report.require("claude")?;
let (session, mut events) = runtime.open(agent, SessionOptions::in_dir(".")).await?;

session.prompt("explain this repo").await?;
while let Some(event) = events.next().await {
    match event?.kind {
        EventKind::TextDelta { text, .. } => print!("{text}"),
        EventKind::RequestOpened(request) => { /* session.answer(request.id, ..) */ }
        EventKind::TurnEnded { .. } => break,
        _ => {}
    }
}
session.close().await?;
```

## Rules

1. **Discover, require, open.** `discover()` is instant and read-only. `require(id)` returns `NotInstalled` if absent. `SessionOptions::in_dir(path)` is required.
2. **Gate features on capabilities.** Check `session.info().details.capabilities.supports(Capability::X)` before offering steer, fork, rollback, compact, or images. The same agent reports different capabilities over different wires, so the capability is the only reliable switch. Unsupported calls fail typed with `UnsupportedFeature`.
3. **Read `Events` from its own task.** It buffers 1024 events; a consumer that falls behind is dropped and the session closes. Keep a `_ => {}` arm, `EventKind` is `#[non_exhaustive]`.
4. **One turn ends exactly once.** `TurnEnded { stop, background }` is the only turn boundary. `Completed { source: Inferred }` means the wire went quiet; show it as idle.
5. **Answer each request once, clear it on `RequestClosed`.** On `RequestOpened` call `session.answer(id, ..)` with a choice from `r.options`. The close event, which also covers the agent withdrawing the request, is the signal to drop it from the UI.
6. **Tool updates are snapshots.** `ToolUpdated` is cumulative. Replace the stored tool by `id`.
7. **Wait for the confirming event.** `configure` is confirmed by `SessionUpdated`, `compact` by `ContextCompacted`, `cancel` by `TurnEnded { Cancelled }`.
8. **Models and options come from the agent.** Read `details.config_options` from `probe` or `session.info()` and render what it offers.
9. **`prompt` always accepts.** It returns a `Delivery` whose `kind` is `Started`, `Steered`, or `Queued`. Handle all three.
10. **Persist the resume token.** Store `session.info().resume_token` and reopen with `SessionOptions::resume(token)`. `fork_from` branches instead.
11. **`record_wire` output is secret.** It is unredacted protocol traffic. Delete it after debugging.
12. **Test without agents** using the `mock` feature: `Runtime::with_mock(script)` runs the real engine over a scripted agent.
