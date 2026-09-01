# pi RPC wire recordings

Recorded 2026-08-30 against `pi` 0.84.4. One JSON object per line, in wire
order. Lines with `_sent` are what we wrote to stdin; every other line is a
frame the CLI wrote to stdout. Home paths are redacted; the 346-model and
long command catalogs are trimmed to two entries plus a `_trimmed` count.

All runs used:

```
pi --mode rpc --provider openrouter --model nvidia/nemotron-3-super-120b-a12b:free
```

| File | Shows |
|---|---|
| 01-handshake | `get_state`, `get_available_models`, `get_available_thinking_levels`, `get_commands`, `get_session_stats` — everything the adapter's handshake reads |
| 02-turn-with-a-tool | the full turn shape: `agent_start`, `message_update` deltas, `tool_execution_*`, `turn_end` per LLM call, then one `agent_end` and one `agent_settled` |
| 03-steer | `steer` is accepted mid-tool, `queue_update` echoes the queue, the steered text lands as a user message in the next LLM call, and the whole prompt still settles once |
| 04-abort | an aborted run reports `stopReason: "error"` with `errorMessage: "This operation was aborted"` — **not** `"aborted"` — and the `abort` receipt arrives *after* `agent_settled` |
| 05-midturn-prompt-refused | a second `prompt` while streaming is refused with `success: false`; the engine's own queue means the adapter never sends one |
| 06-extension-ui | `select` / `input` / `confirm` request-response round trips and a fire-and-forget `notify` |
| 07-abort-resumes-the-steer-queue | `abort` alone ends the current run, then pi starts a **fresh** `agent_start` for the queued steer and only settles after it |
| 08-clear-queue-then-abort | `clear_queue` before `abort` settles immediately — pi's own interrupt recipe, and what the adapter does |

`fixture.mjs` is the hand-written stand-in these recordings describe; it also
answers the two side processes the adapter shells out to (`--version` and
`auth check --provider <p> --json`).

## Wire notes the spec does not carry

- `get_commands` entries carry `sourceInfo: {path, source, scope, origin}`,
  not the docs' flat `location` / `path`.
- `get_available_thinking_levels` is filtered to the **current** model, so a
  `set_model` invalidates it.
- `message_update.usage.totalTokens` equals `get_session_stats.contextUsage
  .tokens` exactly, so context occupancy needs no extra round trip.
- A session opens fine with no credentials configured; `model.provider` is
  then `"unknown"`, but pinning `--model` hides even that. Auth is only
  honest from `pi auth check --provider <p> --json`.
