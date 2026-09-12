---
name: anyagent
description: Rust crate that drives the coding agents installed on a user's machine (Claude Code, Codex, Cursor, opencode, Kiro, Grok, Hermes, Qwen, pi, Antigravity) through one typed API. Read before writing or changing any code that calls anyagent, whether discovering agents, opening sessions, streaming events, answering permission requests, configuring models, or resuming and forking sessions.
license: MIT OR Apache-2.0
compatibility: Rust 1.88+, Tokio runtime. Agents must be installed and logged in on the machine.
metadata:
  author: spotta85
  version: "0.0.3"
---

# anyagent

One crate that finds the coding agents on a machine, speaks each one's
protocol (native or ACP), and exposes them all as three objects. anyagent
owns the processes, the wires, and the turn rules. Your app owns the
transcript, the UI, and the policy behind permission requests.

```text
Runtime::discover()  ->  Runtime::open()  ->  (Session, Events)

  your app ── prompt / answer / configure ──►  Session
                                                  │
                                                  ▼
                                            agent process
                                                  │
  your UI  ◄── text / tools / requests / usage ── Events
```

## Before you code

The docs are the source of truth; this file is the map. Before writing or
changing code that calls anyagent:

1. List the `Runtime` and `Session` calls and the `EventKind`s the change touches.
2. Read the page for each from the table. Fetch the URL (plain markdown), or
   query the Context7 MCP for library `/spotta85/anyagent-rs`. Done when every
   call you will make and every event you will handle has its row read.
3. Write the code, following the rules below.

| Need | Page |
|---|---|
| Install and first turn | https://anyagent.mintlify.site/quickstart.md |
| Every type, event, error, and `SessionOptions` builder | https://anyagent.mintlify.site/core-api.md |
| Each feature with a snippet (resume, fork, rollback, compact, MCP, mock) | https://anyagent.mintlify.site/features.md |
| Catalog, capability matrix, per-agent quirks, custom ACP agents | https://anyagent.mintlify.site/agents.md |
| Threads, streaming, persistence in a real app | https://anyagent.mintlify.site/building-an-app.md |
| Codebase map, adding an agent adapter | https://anyagent.mintlify.site/architecture.md |
| The binary, and the JSON-lines sidecar for apps in other languages | https://anyagent.mintlify.site/sidecar.md |
| Full signatures | https://docs.rs/anyagent |
| Every page, one list | https://anyagent.mintlify.site/llms.txt |

## Rules

1. **Discover, require, open.** `discover()` is instant and read-only; `probe()` launches the agent for real auth, capabilities, and options. `SessionOptions::in_dir(path)` is required.
2. **Gate features on capabilities.** Check `session.info().details.capabilities.supports(Capability::X)` before offering steer, fork, rollback, compact, or images. Unsupported calls fail typed with `UnsupportedFeature`.
3. **Read `Events` from its own task.** A consumer that falls 1024 events behind is dropped and the session closes. Keep a `_ => {}` arm; `EventKind` is `#[non_exhaustive]`.
4. **One turn ends exactly once.** `TurnEnded` is the only turn boundary. `Completed { source: Inferred }` means the wire went quiet; show it as idle.
5. **Answer each request once, clear it on `RequestClosed`.** On `RequestOpened` call `session.answer(id, ..)` with a choice from `r.options`.
6. **Tool updates are snapshots.** `ToolUpdated` is cumulative; replace the stored tool by `id`.
7. **Wait for the confirming event.** `configure` → `SessionUpdated`, `compact` → `ContextCompacted`, `cancel` → `TurnEnded { Cancelled }`.
8. **Models and options come from the agent.** Read `details.config_options` and render what it offers.
9. **`prompt` always accepts.** Its `Delivery.kind` is `Started`, `Steered`, or `Queued`. Handle all three.
10. **One-shot text goes through `Runtime::generate()`.** Titles, commit messages, PR bodies. It opens, prompts, declines tools, and closes for you.
11. **Persist the resume token.** Store `session.info().resume_token`, reopen with `SessionOptions::resume(token)`. `fork_from` branches instead.
12. **Provider data stays in `extensions`.** Match on the shared types; read `Event::extensions` only for provider-specific extras.
13. **`record_wire` output is secret.** Unredacted protocol traffic; delete it after debugging.
14. **Test without agents** using the `mock` feature: `Runtime::with_mock(script)` runs the real engine over a scripted agent.
