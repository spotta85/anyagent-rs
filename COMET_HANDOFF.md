# Comet port: native opencode adapter

Handoff for switching Comet from the shared ACP driver to anyagent's native
opencode adapter (`src/adapter/opencode.rs`). Everything below was verified
live against opencode 1.18.27.

## What this is

Comet used to drive opencode over ACP. anyagent now speaks opencode's own
HTTP + SSE wire: one `opencode serve` process per session, commands as HTTP
POSTs, content on the `/event` bus. The win is a deterministic turn end
(`session.idle` from the server) instead of ACP's guess-when-quiet watchdog.

```
Comet ──> anyagent Session ──> opencode adapter ──HTTP──> opencode serve
                                      ^                        |
                                      └────────SSE /event──────┘
```

## What Comet can now rely on

- **Deterministic turn end.** Every turn ends with a real `TurnEnded`; no
  quiet-timer needed. If the server takes a prompt and never starts it, the
  adapter fails the turn after 10s instead of hanging. **Comet's 60-second
  opencode stall watchdog is redundant — remove it.**
- **Per-message closes.** Each assistant message gets its own `MessageEnded`
  carrying `opencode/fork_point` (the message's own id). Fork at any message
  boundary.
- **Permissions show what they approve.** Bash permissions carry the command
  (`detail` + `ToolInput::Command`), file permissions the path.
- **Subagents work under Ask mode.** The task tool's child sessions ask
  permissions on their own session id; the adapter routes them to Comet as
  normal permission requests. Cancel of the root cascades to children
  server-side.
- **Failed answers reopen.** If opencode refuses a permission/question reply,
  the adapter emits `RequestOpened` again *with the same request id* plus a
  warning diagnostic. Comet's UI must treat a reopened id as answerable
  again, not as a duplicate to ignore.
- **Titles arrive late.** A fresh session has no title (the server's
  "New session - <date>" placeholder is filtered). The real title lands
  after the first turn via `InfoChanged`.
- **Context usage** arrives per step (`ContextUsage` with window and cost).
  Provider retries surface as Info diagnostics, not failures.

## Known limits (by design, v1 wire)

- No steering — the engine queues mid-turn prompts and replays them.
- No client-declared MCP servers (typed `UnsupportedFeature`).
- Images don't ride slash commands (warned, then dropped).
- Rollback rewinds conversation only, not files. `GET /message` still lists
  reverted messages — that's server behavior, not a bug.
- Resume spawns a fresh server, so a session closed mid-permission resumes
  with nothing parked (there is no pending state to recover).

## Porting checklist

1. Point Comet's opencode harness at the anyagent adapter; drop the ACP path
   and the 60s stall watchdog.
2. Handle `RequestOpened` re-emission for an already-closed request id.
3. Show `PermissionRequest.detail` on approval cards (it now carries the
   bash command).
4. Update the session title when `InfoChanged` carries one.
5. Test in Comet: a plain turn, a permission turn, a question, a task-tool
   subagent under Ask mode, cancel mid-turn, resume, fork.

## Verifying

```bash
ANYAGENT_LIVE=opencode cargo test --test live -- --ignored --test-threads=1
```

23 tests, free model (`opencode/big-pickle`), no API key needed. The
question test can flake if the model skips its question tool — rerun it.
