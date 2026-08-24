# v0 Live Test Plan

Verify every v0 feature of anyagent against the real agents installed on this
Mac: **claude** (native wire), **opencode** and **hermes** (ACP wire). P1
features (attachments, MCP forwarding, mode configure, dequeue, auth-lost) are
already live-verified — do not retest them. You are testing, not fixing: when
something fails, record it and move on. Do not change `src/` and do not commit.

## Changed since the first run (2026-08-24)

The first pass's findings were triaged and fixed; this plan is updated to
match. What's different:

1. **claude has a `resume_token` at open now** (we mint the session id and
   pass `--session-id`). T2 asserts it on all three harnesses.
2. **claude no longer advertises `Steer`** — the CLI queues mid-turn messages,
   so the engine queues too. T7 expects `SKIPPED (not advertised)` on all
   three; a mid-turn prompt on claude must come back `Queued`, keep its own
   prompt id, and `cancel` must work around it (T8/T9 criteria updated).
3. **`MessageEnded` is synthesized at turn end** on wires without an
   end-of-message signal — T3 requires it on ACP agents now.
4. **`SessionOptions::configure(id, value)`** sets creation-time options.
   Use it to put opencode on a working model instead of the `OPENCODE_CONFIG`
   workaround: `.configure("model", "openrouter/google/gemini-2.5-flash-lite")`.
5. **Known agent quirks are expected, not findings**: hermes never sends tool
   status transitions (T4: record `KNOWN (hermes: no status updates)` if the
   file is right but statuses stay pending); hermes may write after a deny
   (T5-B: same treatment); opencode may write outside the cwd. Re-flag them
   only if the behavior *changed*.
6. Harness fixes from round 1: count to **400** (not 80) for cancel/steer
   timing, and don't start a `pgrep -f` pattern with `--`.

```
you (tester)
  └─ writes examples/v0_check.rs  (one binary, one subcommand per test)
       └─ anyagent public API: Runtime → Session + Events
            └─ real agent process (claude / opencode / hermes)
```

## Ground rules

1. **Env hijack.** This machine's shell carries `ANTHROPIC_*` vars that reroute
   the claude CLI. Every live run must strip them:

   ```
   env -u ANTHROPIC_BASE_URL -u ANTHROPIC_AUTH_TOKEN -u ANTHROPIC_API_KEY \
       -u ANTHROPIC_MODEL -u CLAUDECODE cargo run --example v0_check <test>
   ```

2. **Never touch real auth.** No deleting/moving `~/.claude`, `~/.hermes`,
   `~/.local/share/opencode`, no logouts. Env-var redirection
   (`CLAUDE_CONFIG_DIR=<empty dir>`) is the only allowed auth trick.
3. **Throwaway harness.** Put all checks in `examples/v0_check.rs` with a
   subcommand per test, print `PASS`/`FAIL` per criterion, delete the file when
   done. Copy patterns from `tests/acp.rs` and `tests/claude.rs` — they show
   exactly how to open, prompt, and drain events.
4. **Capability gating.** Before a feature test, read
   `session.info().details.capabilities`. If the capability is not advertised
   for that harness, record `SKIPPED (not advertised)` — that is a pass for the
   matrix, not a failure.
5. **Flake protocol.** hermes and opencode model backends sometimes return
   empty or off-script replies. If a test fails only on *model output* (wrong
   word, empty text) but the *structure* was right (events, turn end, no
   errors), rerun once. Two identical failures = real finding. A structural
   failure (missing event, wrong error type, hang) is a finding immediately.
6. **Timeouts.** Wrap every event wait in a 120s timeout. A hang past that is a
   FAIL with note "hung at <step>".

## API cheat sheet

```rust
let runtime = Runtime::new();
let report = runtime.discover().await;                  // finds installed agents
let (session, mut events) = runtime.open(&installation,
    SessionOptions::in_dir(dir)).await?;                // Events is a Stream
let delivery = session.prompt("...").await?;            // Started | Steered | Queued
session.answer(request_id, Answer::Permission(PermissionChoice::AllowOnce)).await?;
session.cancel(false).await?;                           // true also clears the queue
session.close().await?;
// Stream items: Result<Event, AgentError>. Event has .sequence, .turn, .kind.
```

Key `EventKind`s: `TurnStarted`, `TextDelta`, `ReasoningDelta`, `MessageEnded`,
`ToolUpdated`, `PlanUpdated`, `RequestOpened`, `RequestClosed`, `ContextUsage`,
`Diagnostic`, `TurnEnded { stop, .. }`.

## The matrix

Run each test on every harness marked ●. ◐ = only if the capability is
advertised.

| # | Test | claude | opencode | hermes |
|---|------|--------|----------|--------|
| T1 | Discovery and auth status | ● | ● | ● |
| T2 | Open and session info | ● | ● | ● |
| T3 | Turn event contract | ● | ● | ● |
| T4 | Tool lifecycle | ● | ● | ● |
| T5 | Permission allow / deny | ● | ◐ | ● |
| T6 | Question round trip | ● | — | — |
| T7 | Steering | — | ◐ | ◐ |
| T8 | Queue is FIFO | ● | ● | ● |
| T9 | Cancel | ● | ● | ● |
| T10 | Resume, no replay | ● | ◐ | ◐ |
| T11 | Agent dies mid-turn | ● | ● | — |
| T12 | Close cleans up | ● | ● | ● |
| T13 | Typed error probes | ● | ● | — |

## The tests

### T1 — Discovery and auth status

`runtime.discover().await`, print the report.

**Success:** claude, opencode, and hermes each appear with an executable path
that exists on disk; each reports `AuthStatus::Authenticated` (claude via
keychain/credentials marker, hermes via `auth.json` ApiKey, opencode via
`auth.json`). Codex may appear or not — record what you see, either is fine.

### T2 — Open and session info

Open a session, inspect `session.info()`.

**Success, all harnesses:** `resume_token` is `Some` **immediately at open,
before any prompt**; `details.version` is `Some` and looks like a semver. **claude:** capabilities include `Images`,
`Permissions`, `Resume`, `Subagents` — and `Steer` must be **absent**; `details.commands` (slash
commands) is non-empty; the `mode` config option exists with current value
`default`. **opencode/hermes:** capabilities include `Permissions`; config
options include `mode` and (opencode) `model`. Record each harness's full
capability list — later tests key off it.

### T3 — Turn event contract

Prompt `"Say only the word PINEAPPLE. Do not use any tools."` and drain to
`TurnEnded`.

**Success:** the first *in-turn* event is `TurnStarted` with `origin: Prompt`
(session-level `SessionUpdated`/`Diagnostic` may legitimately come first);
`sequence` is strictly increasing across all events; every `TextDelta`'s
`message_id` gets a `MessageEnded` before `TurnEnded` (on ACP the engine
synthesizes it at turn end — it must still arrive); concatenated text contains `PINEAPPLE`;
`TurnEnded` stop is `Completed`; no `Diagnostic` of level `Error`; no event
arrives after `TurnEnded` for that turn (wait 3s quiet to confirm).

### T4 — Tool lifecycle

In a temp dir, prompt `"Create a file named note.txt containing exactly the
word HELLO. Use your file tools."` Auto-answer any permission with
`AllowOnce`.

**Success:** at least one `ToolUpdated` arrives with a running/pending status
and later one with `Completed` for the same `tool_id`; the file exists
afterwards with the right content; turn ends `Completed`.
**hermes:** the agent never sends status transitions — if the file is right
but statuses stay pending, record `KNOWN (hermes: no status updates)`.

### T5 — Permission allow / deny

Same write prompt as T4, twice, in two fresh sessions.
Session A: answer the `RequestOpened(Permission)` with `AllowOnce`.
Session B: answer with `Deny`.

**Success A:** request carries at least an allow and a deny choice;
`RequestClosed` follows your answer; file exists; turn completes.
**Success B:** file does **not** exist; the turn still ends (Completed or
Refused — record which); session stays usable (a follow-up prompt answers).
**Note:** opencode may not ask at all (its config auto-allows writes — known
agent behavior). If no request opens on opencode, record
`SKIPPED (agent auto-allows)`. hermes does ask ("Approve edit").

### T6 — Question round trip (claude only)

Prompt `"Ask me whether I prefer red or blue using your question tool, then
answer with just my choice."`

**Success:** a `RequestOpened(Question)` arrives with ≥2 choices; answer with
the choice labeled Red (`Answer::Question(...)`); `RequestClosed` follows; the
final text contains "red" (case-insensitive); turn completes.

### T7 — Steering (◐ `Capability::Steer`)

claude must NOT advertise `Steer` (assert that — an advertised `Steer` on
claude is a finding). On any harness that does advertise it: prompt
`"Count from 1 to 400, one number per line. No tools."` then ~1s later prompt
`"Stop counting and say only CHERRY."`

**Success:** the second delivery is `Steered { .. }` and the turn's combined
output eventually contains `CHERRY`. If neither ACP agent advertises it,
record `SKIPPED (not advertised)` — that is the expected matrix today.

### T8 — Queue is FIFO

Prompt the count-to-400 task, then immediately queue two prompts:
`"Say only KIWI."` and `"Say only LEMON."`

**Success:** the two deliveries report `Queued` positions 0 and 1 (on every
harness — claude included, it no longer steers); after the first turn ends,
both run **in order** — KIWI's turn ends before LEMON's turn starts; each
turn's `TurnStarted { origin: Prompt(id) }` carries **its own** prompt id from
the delivery (a shifted id is a finding); each output contains its word.

### T9 — Cancel

Start the count-to-400 task, wait ~2s, `cancel(false)`.

**Success:** `cancel` returns `Ok`; `TurnEnded` arrives with stop `Cancelled`
within ~10s; the session survives — a follow-up prompt completes. Then two
variants: (a) queue a prompt mid-turn, `cancel(false)` — the turn ends
`Cancelled` and the queued prompt then runs and answers (this was round 1's
claude failure; it must pass now); (b) queue a prompt, `cancel(true)` — the
queued prompt must never run (wait 5s quiet after the cancelled turn end).

### T10 — Resume, no replay (◐ `Capability::Resume`)

Session 1: prompt `"Remember this codeword: FALCON42. Just confirm."`, drain,
save `info().resume_token`, close. Session 2: open with
`SessionOptions::in_dir(..).resume(token)`, then **before prompting** drain
for 3s. Then prompt `"What is the codeword?"`.

**Success:** the 3s pre-prompt drain yields **zero** content events (no replay
of session 1's transcript); the answer contains `FALCON42`. On a harness whose
capabilities lack `Resume`, instead assert that `open` with a resume token
fails with `AgentError::ResumeFailed` — that is the pass.

### T11 — Agent dies mid-turn

Start the count-to-400 task, then from the example kill the child:
find it with `pgrep -f` (claude: newest `claude` process with
`--output-format stream-json`; opencode: `opencode acp`), `kill -9` it.
Skip hermes (its process tree is messier; two harnesses prove the path).

**Success:** `TurnEnded` with stop `Failed`, then the stream yields
`Err(AgentError::ProcessExited { status, .. })` where status mentions signal
9 (not `"unknown"`), then the stream closes (`None`). A later
`session.prompt` returns `Err(SessionClosed)`.

### T12 — Close cleans up

Open, prompt something short, drain, `close()`.

**Success:** `close()` returns `Ok` within 10s; the agent child process is
gone afterwards (`pgrep` finds no new orphan matching the session's process);
the event stream ends (`None`).

### T13 — Typed error probes

On an idle session: (a) `session.rollback(NonZeroU32::new(1).unwrap())` where
`Rollback` is not advertised; (b) answer a permission request correctly, then answer the **same**
request id again; (c) `close()`, then `prompt`.

**Success:** (a) `Err(UnsupportedFeature(..))`; (b) second answer is
`Err(InvalidRequest(..))` and nothing breaks; (c) `Err(SessionClosed)`.

## Report format

One row per (test, harness):

| Test | Harness | Result | Notes |
|------|---------|--------|-------|
| T3 | hermes | PASS | — |
| T5 | opencode | SKIPPED (agent auto-allows) | no request opened |

Result is `PASS`, `FAIL`, or `SKIPPED (reason)`. For every FAIL include: what
you sent, the exact event/error sequence you got (paste the relevant lines),
and whether a rerun reproduced it. Finish with a short list titled
**Findings** — only the FAILs and anything surprising. Then delete
`examples/v0_check.rs` and confirm `cargo test` and
`cargo clippy --all-targets -- -D warnings` still pass untouched.
