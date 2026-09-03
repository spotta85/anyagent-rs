# anyagent
### For apps that want to use the AI subscriptions a user already pays for. 

[![CI](https://github.com/spotta85/anyagent-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/spotta85/anyagent-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/anyagent.svg)](https://crates.io/crates/anyagent)
[![docs](https://img.shields.io/badge/docs-anyagent.mintlify.site-8B5CF6)](https://anyagent.mintlify.site)
[![docs.rs](https://img.shields.io/docsrs/anyagent)](https://docs.rs/anyagent)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

### One Up-to-date, feature-rich Rust interface to the agents already on a users machine.

Users have Claude Code, Codex, Grok, OpenCode, Kiro, and friends on their
machines — each with its own CLI, protocol, and quirks. If you're building an
app on top of them, you end up writing and maintaining a driver per agent.
anyagent is that layer, **once: it finds the agents, speaks each one's protocol
(native or [ACP](https://agentclientprotocol.com)), and gives you one typed
API — `Runtime`, `Session`, and a stream of `Events`**.

#### Your app keeps its own transcript, UI, and policy. anyagent owns the processes, the wires, and the turn rules — every agent behaves the same way through it.
<img width="583" height="346" alt="image" src="https://github.com/user-attachments/assets/6133087d-e12a-4b66-9990-b7c5e8e81011" />



## Use

```toml
[dependencies]
anyagent = "0.0.1"
```

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
        EventKind::TurnEnded { .. } => break,
        _ => {}
    }
}
session.close().await?;
```

That's the whole core flow. Everything else is the same few objects:

## Features

| Capability | API |
|---|---|
| Discover installed agents and check login status | `runtime.discover()`, `runtime.probe(agent)` |
| Plan quota for the logged-in account | `runtime.plan_usage(agent)` |
| Send text, images, slash commands | `session.prompt(input)` |
| Steer or interrupt a running turn | `session.prompt(..)` mid-turn, `session.cancel()` |
| Answer a permission or question the agent raised | `session.answer(id, answer)` |
| Switch model / mode / any advertised option live | `session.configure("model", "sonnet")` |
| Resume, fork, or rewind a conversation | `SessionOptions::resume` / `fork_from`, `session.rollback(..)` |
| Add custom MCP Servers | `SessionOptions::mcp_server(..)` |

Events cover streamed text and reasoning, typed tool calls with diffs, plans,
token usage, permission requests, subagents, and turn boundaries — the same
`EventKind`s for every agent. Anything provider-specific rides in
`extensions` instead of leaking into the types.

## Examples

[`examples/`](examples/) is the tour — small, commented programs that run
against the agents on your machine:

- [`chat.rs`](examples/chat.rs) — the core loop: prompt, stream events,
  answer permissions. Typing mid-turn steers the running turn.
- [`sessions.rs`](examples/sessions.rs) — several sessions at once, then
  resuming one by token.
- [`probe.rs`](examples/probe.rs) — discover installed agents and probe
  their login state, models, commands, and capabilities.

## Supported agents

| Agent | Wire |
|---|---|
| Claude Code | native (stream-json) |
| Codex | native (app-server) |
| Antigravity | native (adapter in progress) |
| pi | native (pi RPC) |
| opencode | native (HTTP + SSE) |
| Grok, Hermes Agent, Kiro CLI, Qwen Code | ACP |

Any other ACP agent works without a catalog entry via
`AgentInstallation::acp(name, path, args)`.

## Docs

Guides, concepts, and the full event reference live at
**[anyagent.mintlify.site](https://anyagent.mintlify.site)**.

The generated API reference — every type and signature — is on
[docs.rs/anyagent](https://docs.rs/anyagent).

## Contributions
Contributions are welcome! Please be sure to open an issue first. Upon approval you may create a pr. Please avoid contributing AI-Slop :) ... unless fable wrote it. Read [contributions.md](contributions.md) for more specifics!

## License

MIT or Apache-2.0.
