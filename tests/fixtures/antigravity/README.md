# agy stream-json wire recordings

Recorded 2026-09-02 against `agy` 1.1.24 with
`.scratch/anyagent/issues/05-drive.py`. One JSON object per line, in wire
order. Lines with `_sent` are what we wrote to stdin, `_note` marks a signal
or exit, every other line is a frame the CLI wrote to stdout. Home paths are
redacted. `.err` files are the stderr of runs that printed any.

All runs used:

```
agy --input-format=stream-json --output-format=stream-json [flags]
```

| File | Shows |
|---|---|
| 01-handshake | the `init` frame: `conversation_id`, `cwd`, 55 tool names, `permission_mode`; nothing about model, mode, or commands |
| 02-pong | the minimal turn: `user_input` → `agent_response` (DONE, with `usage`) → one `result` |
| 03-tool-ask | default mode: `tool` ACTIVE → ERROR, no permission frame; the denial is explained only on stderr (`.err`) |
| 04-tool-skip | `--dangerously-skip-permissions`: `permission_mode: always-proceed`, `tool` DONE with `output`, text streamed ACTIVE then DONE |
| 05-question | `ask_question` in headless mode: an `unknown` step, then the agent says the question was skipped |
| 05-question-answer | a follow-up `user` after `result` is simply turn 2 (`num_turns: 2`) |
| 06-steer | a second `user` mid-turn **queues**: two `result`s, the second answers it; `system_message` steps between turns |
| 07-cancel | SIGINT mid-tool: one ERROR `result` ("timeout waiting for response"), exit 1, dead pipe |
| 07-cancel-resume | `--conversation <id>` after the kill: same id, the model recalls the interrupted request |
| 08-resume-a/b/c | `--conversation <id>` and `--continue` both resume; `init` names the id, no history replay, `step_index` continues |
| 09-config | bad `--model`, or `--effort` with a suffixed model id: an ERROR `result` with empty `conversation_id`, no `init`, exit 1 |
| 09-config-b | `--mode plan` is accepted silently; `init` is unchanged |
| 10-logged-out | `HOME` with no credentials: no `init`, ERROR `result` "authentication failed or timed out", stderr says to run `agy` |
| 12-subagent | `define_subagent` as a tool, then a `subagent` step whose `subagent_info` links the child conversation; no child frames |
| 13-slash | `/help` over the wire is refused (exit 2): slash commands are CLI-side |

## The ACP server (`acp-*.jsonl`)

Recorded 2026-09-07 against Google's `agy_acp_server_20260818_01_RC01`
(registry `antigravity-acp`) with `05-acp-drive.py`: plain JSON-RPC over
stdio, driven by anyagent's existing ACP adapter.

| File | Shows |
|---|---|
| acp-01-handshake | `initialize`: images, audio, embedded context, http and sse MCP, `loadSession`, four agent-driven `authMethods`; then `session/new` refused `-32000` because the server has no auth choice of its own yet |
| acp-02-pong | with `auth.type: oauth-personal` in `~/.gemini/antigravity-acp/settings.json`: `session/new` (15 s the first time) returns modes `default` / `auto_edit` / `yolo`, 11 Gemini models, and the `model` config option; one prompt streams `pong` and ends `end_turn`; `available_commands_update` lists `plan` and `logout`. A raw OAuth URL line was printed on stdout after the turn |
| acp-03-authenticate | no settings file: `authenticate {methodId: oauth-personal}` returns `{}` in 2.3 s with no browser, adopting the `agy` login; the adapter does this on the `-32000` |
| acp-04-question | `ask_question`: a `tool_call` with id `interaction_*` titled by the question, then `session/request_permission` whose options are the choices, every one `allow_once`; the answer is `selected` with the `optionId`, and the model echoes the choice. The adapter surfaces it as a `Question` |

`fixture.mjs` is the hand-written stand-in these recordings describe; it also
answers the two side processes the adapter shells out to (`--version` and
`--output-format=json models`).

## Wire notes

- The ACP server has no steer: a second `session/prompt` while one runs is
  held, and once the first ends the server reports "Concurrent
  receive_steps() calls are not supported" and drops its agent connection
  (probed 2026-09-07). anyagent queues mid-turn prompts instead.

- `usage` is `{input_tokens, output_tokens, thinking_tokens,
  cache_read_tokens, total_tokens}` on every DONE `agent_response` and every
  `result`. The `result` one is the **sum** of the turn's step snapshots (and
  keeps summing across resumed processes: `08-resume-b` reports 27723 after
  two 13.9k calls), so the last `agent_response` snapshot is the context
  size. Nothing names the window.
- `step_type` values seen: `user_input`, `agent_response`, `tool`,
  `subagent`, `unknown` (a skipped question), `system_message`.
- `--output-format=json models` works; `models --output-format=json` does
  not (the flag is global and must come first).
- Without `--add-dir <cwd>` the agent wrote `probe.txt` under
  `~/.gemini/antigravity-cli/scratch/`, not the session cwd; with it (probed
  2026-09-07) the file landed in the cwd via `run_command`, after
  `write_to_file` refused the path as "not a valid artifact path".
