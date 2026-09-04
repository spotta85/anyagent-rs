# Comet port: native codex adapter + claude fast mode

Handoff for two things Comet picks up from anyagent PR #9 (branch
`codex-comet-port`):

1. Replace Comet's own codex driver (`crates/harness/src/codex/`, ~2300
   lines) with anyagent's native adapter (`src/adapter/codex.rs`).
2. Wire up the new speed options: codex `serviceTier` and claude
   `fastMode`.

Everything wire-facing was probed live before coding (codex 0.152.0,
claude 2.1.259, 2026-09-03). Like the opencode port
([COMET_HANDOFF.md](COMET_HANDOFF.md)): routing is automatic, the work is
bridge translation.

```
Comet ──> anyagent Session ──> codex adapter ──stdio JSONL──> codex app-server
                     └───────> claude adapter ──stream-json──> claude CLI
```

## Part 1: the codex port

### Why the old blockers are gone (probed, not assumed)

- **Reasoning summaries stream.** Every `turn/start` now carries
  `summary: "auto"`. Probed: without it codex emits ZERO reasoning
  events — the "thinks in silence for minutes" bug the old driver's
  comments warn about. Nothing for Comet to do.
- **Service tier is a live option.** See Part 2.
- **Per-turn policy is unnecessary.** The old driver sends
  `approvalPolicy`/`sandboxPolicy` on every turn; anyagent binds them at
  `thread/start` only. Probed: a read-only thread blocks a write with no
  turn-level policy — thread-level IS honored, turn-level merely
  overrides. Once at bind is enough.
- **`turn/failed` / `turn/aborted` can't hang a turn.** 0.152.0 only
  ever emits `turn/completed` (probed, including a forced failure), but
  both are mapped defensively on parent and subagent paths.
- **Everything else the old driver did** — multi-agent v2 child threads
  (one narrower registration path: children register from parent-thread
  `subAgentActivity` only, not also child `thread/started` + spawn-source),
  usage/rate limits, resume, interrupt, plans, questions — was already
  covered. Steering is at parity with the old driver's `StepBoundary`
  steering (mid-turn `turn/steer` in both); the win is deleting the
  driver's steering mailbox and queued-steers fallback, since the engine
  owns the queue.

### Bridge checklist (all in `crates/harness/src/bridge.rs`)

1. **Route `HarnessId::Codex` through the bridge** — `AnyagentHarness::codex()`
   in the registry slot (descriptor unchanged: "Codex", StepBoundary, full
   ladder — the stability test passes unmodified). `installation()` resolves
   the `codex` CLI via `CODEX_EXECUTABLE` + `codex_paths()` (the same
   locations the old driver searched). Do NOT pass `acp(...)`. Old driver
   (`crates/harness/src/codex/`, the now-unused `jsonrpc.rs`, its
   integration tests + fixtures) deleted.
2. **`options()`**: a codex branch next to the claude one — creation-only
   `mode`/`sandbox` here, live `model`/`effort`/tier post-open:
   - unattended runs → `configure("mode", "never")` (the old driver's
     pinned `approvalPolicy: "never"`). Leave unset for Ask-mode:
     approvals then surface as permission requests with command/path
     detail — and leave `permission_mode` at its `Ask` default in that
     case (every other harness stays `AutoApprove`).
   - sandbox → `configure("sandbox", ...)` (kebab-case:
     `read-only` / `workspace-write` / `danger-full-access`), always.
   - `model`, reasoning → `effort`, and service tier → post-open in
     `apply_agent_config` (renamed from `apply_acp_config`), each applied
     only when the session advertises the id — unadvertised ids skip and
     the agent default wins. That skip IS the effort fallback (no
     `to_effort` clamp table to maintain) and the stale-tier guard (a
     `"fast"` pick from an old snapshot skips to Standard instead of
     riding the wire). Re-read `session.info()` after the model
     configure: codex effort ladders are per-model.
   - service tier value rides the wire verbatim — see Part 2.
3. **`models()`**: codex joins the `probe_models()` routing (the opencode
   pattern) — live model ids from the `model` option, plus the
   `service_tier` option attached to each model as the tier picker. No
   per-model reasoning ladders (same as opencode/kiro: the descriptor
   ladder drives the picker, above-ladder picks skip to the agent
   default in `apply_agent_config`).
4. **Resume**: the stored thread id already rides `SessionOptions::resume`
   via the shared path; `run()` matches `anyagent::AgentError::ResumeFailed`
   (codex-scoped) and retries without it — the old driver's
   fallback-to-fresh, kept in the bridge.
5. **Turn end / watchdog**: `deterministic_turn_end` was already true for
   all bridge harnesses (no change). codex gets its own 60s silence bound
   (`CODEX_STALL`): codex fails turns loudly, so total silence means a
   wedged process or hung handshake — the disarm set is the shared
   turn-content one.
6. **Steering**: codex advertises `Capability::Steer`, at parity with the
   old driver's `StepBoundary` steering — `steering_mode()` gains a codex
   arm returning `StepBoundary` (keeps the registry descriptor stable).
   The deleted part is the driver's steering mailbox and queued-steers
   fallback: the engine re-queues a refused steer at the head of its own
   queue, and the bridge mailbox only tracks steers for `Steered` events.

## Part 2: the speed options

Both harnesses expose their speed knob as a config option under the same
`service_tier` category:

| harness | option        | kind    | live? | wire mechanism                      |
|---------|---------------|---------|-------|-------------------------------------|
| codex   | `serviceTier` | select  | yes   | rides every `turn/start`            |
| claude  | `fastMode`    | boolean | no    | `--settings '{"fastMode":true}'`    |

### codex `serviceTier`

Choices come from `model/list`'s `serviceTiers` plus a `"default"`
(Standard) entry; `"default"` is never sent on the wire. Settable at
creation (`configure("serviceTier", ...)`) and switchable live.

**Careful:** the live tier id is `"priority"` (label "Fast") — the
`"fast"` in the old `catalog.rs` snapshot is stale, and the wire
silently accepts any string, so the adapter's local validation is the
only guard. Map Comet's tier pick to the ids the adapter's option
advertises, never to hardcoded values.

### claude `fastMode`

Comet's per-model Fast Mode toggle (`catalog.rs` `toggle("fastMode")`)
maps to `configure("fastMode", true)` in `options()`'s claude branch,
next to model/effort. Facts that shape the design:

- The `--settings` flag is also the SDK opt-in: without it the wire
  refuses with `fast_mode_disabled_reason: "sdk_opt_in_required"`.
- Creation-only. The 2.1.259 binary has no `set_fast_mode` control
  request, so changing it mid-chat means reopen with the resume token
  (same rule as claude's `effort`).
- The flag is a request, `fast_mode_state` is the truth. The option's
  `current` mirrors the state (`on`/`cooldown`/`off`) — the account or
  org can keep it off (probed reasons: `model_not_allowed`,
  `extra_usage_disabled`, `preference`). Don't render the toggle's own
  value as confirmation; read it back from the session info.
- The option only appears when the model catalog reports
  `supportsFastMode` on any model — "any", not "current", because
  claude's model switches live while fastMode can't.

## Test in Comet

1. Plain codex turn: reasoning summary text renders (no silent
   thinking, no staleness flip).
2. Tier turn: pick Fast, confirm `serviceTier: "priority"` in the wire
   recording; pick Standard, confirm the param is absent.
3. Cancel mid-turn, resume, and a multi-agent v2 subagent run — the
   old driver's matrix, now against the adapter.
4. Claude with Fast Mode on: launch arg carries the settings flag;
   session info reports the resulting state.

## Verifying (anyagent side, already green)

```bash
cargo test --test codex --test claude   # contract tests, fixture, free
ANYAGENT_LIVE=codex cargo test --test live -- --ignored --test-threads=1
ANYAGENT_LIVE=claude cargo test --test live -- --ignored --test-threads=1
```

Live runs need logged-in CLIs and burn real tokens. Handshake + turn
were re-verified green on both after these changes.
