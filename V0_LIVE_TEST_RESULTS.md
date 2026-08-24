# v0 Live Test Results — run 2 (2026-08-24)

Second pass of `V0_LIVE_TESTS.md`, run after the round-1 fixes landed. Same
method as run 1: a throwaway `examples/v0_check.rs` harness, one subcommand per
test, deleted afterwards. `src/` was never touched. `cargo test` (54 tests) and
`cargo clippy --all-targets -- -D warnings` pass unchanged.

```
harness (examples/v0_check.rs)
  └─ anyagent public API: Runtime → Session + Events
       └─ real agent process
            ├─ claude    (native wire, subscription auth)
            ├─ opencode  (ACP wire, openrouter/google/gemini-2.5-flash-lite)
            └─ hermes    (ACP wire, google/gemini-2.5-flash-lite)
```

## Results

| Test | Harness | Result | Notes |
|------|---------|--------|-------|
| T1 | claude | PASS | subscription auth, `/Users/spotta/.local/bin/claude` |
| T1 | opencode | PASS | subscription auth, `/opt/homebrew/bin/opencode` |
| T1 | hermes | PASS | api-key auth, `/Users/spotta/.local/bin/hermes` |
| T2 | claude | PASS | resume token at open; `Steer` correctly absent |
| T2 | opencode | PASS | resume token at open; `model` reads back as the configured value |
| T2 | hermes | PASS | resume token at open |
| T3 | claude | PASS | — |
| T3 | opencode | PASS | `MessageEnded` now arrives |
| T3 | hermes | PASS | `MessageEnded` now arrives |
| T4 | claude | PASS | `Running` → `Completed` under one tool_id |
| T4 | opencode | PASS | `Pending` → `Running` → `Completed`; wrote inside the cwd this time |
| T4 | hermes | KNOWN (hermes: no status updates) | file correct, both tools stay `Pending` |
| T5 | claude | PASS | allow writes, deny blocks, session survives |
| T5 | opencode | SKIPPED (agent auto-allows) | no request opened on rerun |
| T5 | hermes | PASS | deny held — the round-1 write-after-deny did **not** recur |
| T6 | claude | PASS | question round trip, final text "red" |
| T7 | claude | — | excluded by plan; `Steer` absence asserted in T2 |
| T7 | opencode | SKIPPED (not advertised) | — |
| T7 | hermes | SKIPPED (not advertised) | — |
| T8 | claude | PASS | 3 queued at 0/1/2, own prompt ids, FIFO, no overlap |
| T8 | opencode | PASS | — |
| T8 | hermes | PASS | — |
| T9 | claude | PASS | was FAIL; retested 5x after the fix, all clean |
| T9 | opencode | PASS | all three variants |
| T9 | hermes | PASS | all three variants |
| T10 | claude | PASS | zero replay, codeword recalled |
| T10 | opencode | PASS | — |
| T10 | hermes | PASS | — |
| T11 | claude | PASS | `signal: 9 (SIGKILL)`, stream closes, then `SessionClosed` |
| T11 | opencode | PASS | — |
| T11 | hermes | — | excluded by plan |
| T12 | claude | PASS | — |
| T12 | opencode | PASS | — |
| T12 | hermes | PASS | — |
| T13 | claude | PASS | (a) `UnsupportedFeature` (b) `InvalidRequest` (c) `SessionClosed` |
| T13 | opencode | PASS | (b) probed via unknown request id — opencode never asks |
| T13 | hermes | — | excluded by plan |

Capabilities advertised this run:

| Agent | Capabilities | Config options | Commands |
|---|---|---|---|
| claude 2.1.241 | images, resume, permissions, questions, slash-commands, subagents, context-usage | `mode=default` | 93 |
| opencode 1.18.21 | images, resume, permissions | `model`, `effort=low`, `mode=build` | 0 |
| hermes 0.20.5 | images, resume, permissions | `mode=default` | 9 |

## Findings

### 1. claude — cancel with a non-empty queue — FIXED, retested

The one FAIL of the main pass. Queue a prompt mid-turn, wait 2s,
`cancel(false)`: cancel returned `Ok`, then the stream went silent — no
`TurnEnded`, the queued prompt never ran, and the 120s follow-up wait timed
out. Reproduced 2/2, and the `cancel(true)` phase hit it on the second run
too. Empty-queue cancel passed every time, and both ACP agents passed all
three variants, so it was claude-adapter specific.

Fixed and retested **5 consecutive runs, all PASS** — every criterion in all
three variants:

```
[PASS] T9(a) mid-turn prompt is Queued
[PASS] T9(a) cancel(false) returns Ok
[PASS] T9(a) counting turn ends Cancelled
[PASS] T9(a) queued prompt then runs and answers
[PASS] T9 cancel(true) returns Ok
[PASS] T9 phase2 TurnEnded Cancelled arrives
[PASS] T9 queued prompt never runs after cancel(true) (5s quiet)
```

Five runs matters here because the original bug was timing-dependent: it
needed the count turn to still be live at cancel time. No flake in any run.

### 2. opencode — one-off permission request that then lost the write

First `T5` run on opencode opened a permission request (round 1 and the rerun
both show it auto-allowing and never asking), and after `AllowOnce` +
`RequestClosed` the file was missing:

```
[PASS] T5-A permission request opened
[PASS] T5-A RequestClosed follows the answer
[FAIL] T5-A file exists after allow — note.txt missing
```

Did not reproduce — the rerun opened no request at all. Recorded as a
one-off, not a finding against anyagent, but worth a second look if it shows
up again.

## Known agent quirks — behavior unchanged

| Quirk | Status this run |
|---|---|
| hermes never sends tool status transitions | Confirmed unchanged. `[Pending] write: note.txt` twice, no `Running`/`Completed`, file correct. Recorded as `KNOWN` per the plan. |
| hermes writes despite a deny | **Did not recur.** T5-B denied cleanly, file absent, turn completed, session usable. |
| opencode writes outside the session cwd | **Did not recur.** T4's write landed at `<cwd>/note.txt`. |

## Round-1 findings — all fixed

| Round-1 finding | Verified by |
|---|---|
| 1. claude `resume_token` `None` at open | T2 claude — non-empty at open |
| 2. claude prompt ids shift after a steer | T8 claude — 3 queued prompts, each with its own `TurnStarted` id, FIFO |
| 3. claude `cancel` dies after a steer | T9 claude — steer path gone, queue path fixed and retested 5x |
| 4. `MessageEnded` never sent on ACP | T3 opencode + hermes — every `TextDelta` message ends |
| 5. hermes tool status stuck `Pending` | Reclassified as a known agent quirk |
| 6. hermes writes despite deny | Did not recur |
| 7. opencode `SessionUpdated` before `TurnStarted` | T3/T4 opencode — first in-turn event is `TurnStarted` |
| 8. opencode writes outside cwd | Did not recur |
| 9. `live: false` options unreachable | `SessionOptions::configure("model", ...)` works; T2 reads back `model=openrouter/google/gemini-2.5-flash-lite` |

## Environment notes

Both ACP agents needed model/auth changes to produce output at all. Neither is
an anyagent defect.

- **opencode** — `~/.config/opencode/opencode.jsonc` still pins the dead
  `openrouter/stealth/ox-alpha` and reads its key from `{env:OPENROUTER_API_KEY}`,
  which is unset in the shell. Without the key every turn ends
  `Failed { message: "Internal error: Missing Authentication header (-32603)" }`.
  Runs set `OPENROUTER_API_KEY` in the child env and the model via
  `SessionOptions::configure`; the config file was **not** modified.
- **hermes** — was on `nvidia/nemotron-3-super-120b-a12b:free`, which now
  returns `HTTP 429: Rate limit exceeded: free-models-per-day`. Switched to
  `google/gemini-2.5-flash-lite` with `hermes config set`; config backed up
  first. This is a config change on your machine — revert with
  `hermes config set model.default nvidia/nemotron-3-super-120b-a12b:free`.
- **claude** — subscription auth, works as-is with the `ANTHROPIC_*` env strip.
