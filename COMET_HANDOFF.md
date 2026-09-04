# Comet port: native opencode adapter

Handoff for switching Comet from the shared ACP driver to anyagent's native
opencode adapter (`src/adapter/opencode.rs`). Everything below was verified
live against opencode 1.18.27, then re-checked against the code both sides
on 2026-09-04.

## Status: what is automatic, what needs a Comet edit

- **Routing is automatic — no Comet change needed.** Comet pins anyagent by
  path (`crates/harness/Cargo.toml`) and `installation()` returns
  `AgentInstallation::at("opencode", exe)` (`crates/harness/src/bridge.rs`),
  which `Runtime::open` routes to the native `OpencodeAdapter`
  (anyagent `src/runtime.rs`: only an explicit `AgentInstallation::acp`
  forces the ACP path for a catalog agent). Picking up the anyagent commit
  *is* the port. Do not pass `acp(...)` for opencode.
- **The rest is bridge translation gaps, not plumbing.** Sections below say
  exactly which `bridge.rs` lines to touch. Nothing is needed in
  `crates/engine`: the engine never sees anyagent ids (the bridge mints its
  own input-request ids via `controls.request_input`), so the reopen and
  id-correlation concerns stay inside the bridge.

```
Comet ──> anyagent Session ──> opencode adapter ──HTTP──> opencode serve
                                      ^                        |
                                      └────────SSE /event──────┘
```

## What Comet can now rely on

- **Deterministic turn end.** Every turn ends with a real `TurnEnded`; no
  quiet-timer needed. If the server takes a prompt and never starts it, the
  adapter fails the turn after 10s (`ADMIT_TIMEOUT`,
  `src/adapter/opencode.rs`) instead of hanging. **But see the watchdog
  note in the checklist — the bridge's 60s stall bound must stay, narrowed,
  not be deleted.**
- **Per-message closes.** Each assistant message gets its own `MessageEnded`
  carrying `opencode/fork_point` (the message's own id). Fork at any message
  boundary. (No Comet UI consumes fork points today — see checklist.)
- **Permissions show what they approve.** Bash permissions carry the command
  (`detail` + `ToolInput::Command`), file permissions the path
  (`permission_detail`, `src/adapter/opencode.rs`). Only matters under Ask
  mode — see checklist.
- **Subagents work under Ask mode.** The task tool's child sessions ask
  permissions on their own session id; the adapter routes them to Comet as
  normal permission requests. Cancel of the root cascades to children
  server-side. Under Comet's AutoApprove the children never ask; their
  transcripts already flow via `parent_tool_id` → `AgentEvent::Subagent`,
  which the bridge wraps today.
- **Failed answers reopen.** If opencode refuses a permission/question reply,
  the adapter emits `RequestOpened` again *with the same request id* plus a
  warning diagnostic. The bridge translates each emission through
  `controls.request_input`, which mints a fresh engine-side id — so this
  surfaces as a second question chip that is answerable in its own right.
  Do not dedupe by anyagent id in the bridge; the engine never sees those
  ids. Visible cost is a duplicate chip after a refused reply; acceptable.
- **Titles arrive late.** A fresh session has no title (the server's
  "New session - <date>" / "Child session - ..." placeholder is filtered).
  The real title lands after the first turn via `InfoChanged`. Comet names
  chats off the first prompt itself, so adopting this is optional — see
  checklist.
- **Context usage** arrives per step (`ContextUsage` with window and cost).
  Provider retries surface as Info diagnostics, not failures. **Both are
  currently dropped by the bridge** (`on_event`'s catch-all) — see
  checklist, because the retry diagnostic interacts with the watchdog.

## Known limits (by design, v1 wire)

- No steering — the engine queues mid-turn prompts and replays them.
  Comet already treats this wire as `SteeringMode::TurnBoundary`; no change.
- No client-declared MCP servers (typed `UnsupportedFeature`).
- Images don't ride slash commands (warned, then dropped).
- Rollback rewinds conversation only, not files. `GET /message` still lists
  reverted messages — that's server behavior, not a bug. (No Comet UI calls
  rollback today.)
- Resume spawns a fresh server, so a session closed mid-permission resumes
  with nothing parked (there is no pending state to recover). Comet resume
  (`request.resume` → `SessionOptions::resume`, token re-read in `done()`)
  keeps working unchanged; opencode resume tokens are the stable session id.
- A dead provider retries forever on the wire without failing the prompt.
  What changed is visibility: it now surfaces per-retry Info diagnostics
  instead of total silence. There is still no terminal provider-failure
  frame — the run stays open until cancelled or the bridge watchdog fires.

## Porting checklist

1. **Keep the 60s opencode stall watchdog, but narrow what disarms it —
   and forward diagnostics.** The draft of this handoff said to remove it;
   that was wrong. `drive()` disarms the bound on *any* event except
   `SessionUpdated`/`TurnStarted`, and `on_event` drops `Diagnostic` — so a
   dead provider would disarm the watchdog with its first retry notice and
   then hang in Working forever with zero visible output (worse than the
   ACP behavior, where silence tripped the bound at 60s). Instead:
   - a. Disarm only on turn-content events (text, reasoning, tool, request,
     message-end, turn-end). A `Diagnostic` must not disarm the bound.
   - b. Translate `Diagnostic` at Warning and above into `AgentEvent::Error`
     (non-fatal, visible): this is how provider-retry storms and
     answer-refused notices reach the user.
   - c. Keep `OPENCODE_STALL` at 60s as the bound for retry-forever.
   (`bridge.rs`: consts at the top, disarm in `drive()`, catch-all in
   `on_event()`.)
2. **Reopened requests: no change needed.** Confirm by code read that the
   bridge never correlates engine input ids back to anyagent request ids —
   it doesn't; `answer_questions` answers with the `Request` it closed
   over. Leave it that way.
3. **Permission `detail`: nothing to do while opencode runs AutoApprove.**
   `options()` sets `AutoApprove` for every agent, so no permission request
   ever arrives and there is no bridge/proto path for one (`on_event`
   drops `Request::Permission`; `InputRequested` carries questions only).
   If Comet ever moves opencode to Ask, that needs a new `AgentEvent`
   variant plus approval cards first; `detail` then rides along.
4. **Session titles: product decision, default to skip.** The bridge drops
   `SessionUpdated` and the engine names chats off the first prompt — that
   keeps working. Adopting the server title would override Comet naming for
   no functional gain. Same for the live command list: `commands()` already
   sources it from `probe`.
5. **Context usage: optional.** The adapter emits per-step `ContextUsage`;
   the bridge drops it. `AgentEvent::Usage { input_tokens, output_tokens }`
   doesn't fit used/window/cost — this needs a proto extension if Comet
   wants a live context meter. Not required for the port.
6. **Cancel, resume, subagents, steering: no change.** Interrupt already
   calls `session.cancel(true)` and drains to `TurnEnded`; aborts map to
   `Cancelled` → `DoneStatus::Interrupted`. `SteeringMode::TurnBoundary`
   and `deterministic_turn_end` stay as they are.

## Test in Comet

No fork/rollback UI exists in `crates/engine` or `crates/proto`, so those
fall off the matrix. Model ids are `provider/model` — already handled
(`probe_models` sources them from `probe`; `apply_acp_config` passes
through only advertised values).

1. Plain turn.
2. Question turn. The allow-all worry is settled: probed live (2026-09-04)
   with Comet's exact `{"*": "allow"}` config, `question.asked` still fires
   — the allow rule runs the tool, and running it is asking. Only the
   Comet-side chip rendering needs confirming.
3. Task-tool subagent: nested transcript lands in the subagent doc.
4. Cancel mid-turn: expect `Interrupted`, settled doc, no phantom segment.
5. Resume via the stored `session_id` from a prior `Done`.
6. Provider-down run: expect retry Errors then the visible 60s stall error —
   not infinite Working.
7. (Optional, hard to force manually) refused-reply reopen → second chip,
   answerable.

## Verifying

```bash
ANYAGENT_LIVE=opencode cargo test --test live -- --ignored --test-threads=1
```

23 tests, free model (`opencode/big-pickle`), no API key needed. The
question test can flake if the model skips its question tool — rerun it.
Comet-side: bridge unit tests plus the seven manual runs above.
