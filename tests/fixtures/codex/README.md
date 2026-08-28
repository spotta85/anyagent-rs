# Codex app-server native wire recordings

Recorded 2026-08-27 against `codex` 0.147.0 (`codex-cli 0.147.0`) on macOS,
ticket 10. One JSON object per line, in wire order. Lines with `_sent` are what
we wrote to stdin; every other line is a frame the server wrote to stdout.
`_t` is seconds since spawn. `_meta` marks a process start (argv + env
overrides), `_note` is a probe annotation saying what the next step is testing.

The wire is plain **JSON-RPC 2.0**, one message per line, both directions.
Client requests use the client's own id space; server→client requests use the
server's, starting at 0 — the two overlap, so match by direction, not by id.

All runs used:

```
codex app-server            # cwd = workspace, CODEX_HOME = a temp dir
```

then `initialize` → `initialized` notification → `thread/start`.

Most runs point `CODEX_HOME` at a throwaway directory with only a symlinked
`auth.json`, which keeps the host's MCP servers, skills, and history out of the
recording. Fixture 01 is the exception: it ran against the real `~/.codex`, so
it shows what an unisolated host looks like (dozens of
`mcpServer/startupStatus/updated` frames before the first item).

| File | Shows |
|---|---|
| 01-handshake-and-turn | `initialize`, `thread/start`, two turns; `item/*` lifecycle, `thread/tokenUsage/updated`, `account/rateLimits/updated`, `turn/completed`. Real `~/.codex`, so also the MCP startup noise |
| 02-approvals-and-tools | `commandExecution` and `fileChange` items; `item/fileChange/requestApproval` accepted and declined; `turn/diff/updated`; a read-only command running with **no** approval under `approvalPolicy: untrusted` |
| 03-steer-and-queue | `turn/steer` folds into the running turn; **`turn/start` mid-turn also folds in** and its returned turn id never materializes; stale `expectedTurnId` → `-32600` |
| 04-interrupt | `turn/interrupt` → `turn/completed status: interrupted`; the running command is killed; the in-flight tool item gets no `item/completed`; interrupt while idle → `-32600` |
| 05-resume-and-fork | `thread/resume` (same id, history returned, works cold in a fresh process); `thread/fork` with `lastTurnId`; `thread/rollback`; `thread/read` |
| 06-probe-recipe | Everything `probe()` needs with no thread and no turn: `account/read`, `model/list`, `account/rateLimits/read`, `permissionProfile/list`, plus timings; unknown method → `-32600` |
| 07-logged-out-and-login | Logged out: `account/read` → `{account: null}`, rate limits refused, `thread/start` still succeeds; `account/login/start` (chatgpt → `authUrl` + `loginId`, and apiKey → immediate) and `account/login/cancel` |
| 08-logged-out-turn-end | A logged-out turn still ends with exactly one `turn/completed`, `status: failed`, carrying the 401 |
| 09-env-sensitivity | `CODEX_HOME` redirection across five cases; `OPENAI_API_KEY` and `OPENAI_BASE_URL` do not change the reported account |
| 10-openai-api-key-ignored | The decisive one: with no `auth.json`, a bogus `OPENAI_API_KEY` produces the *same* "Missing bearer" 401 as no key at all — codex app-server ignores the variable |
| 11-config-plan-compact | `model` / `effort` as per-turn `turn/start` params; an unknown model fails at turn end, not at request time; `turn/plan/updated`; `thread/compact/start` → a `contextCompaction` item |
| 12-steer-race | `turn/steer` between `turn/start`'s response and `turn/started` is refused ("no active turn to steer"); accepted once `turn/started` arrives; refused while idle and during a compact turn |

Authoritative types: `codex app-server generate-json-schema --out DIR` from the
same binary (also `generate-ts`). That is the codex equivalent of `sdk.d.ts`:
`ClientRequest.json` lists all 95 client methods, `ServerNotification.json` all
70 notifications, `ServerRequest.json` all 10 server→client requests.

Emails, install ids, and local paths are redacted. Long host-specific results
(`skills/list`, `mcpServerStatus/list`, `config/read`, `account/usage/read`) are
replaced with a shape marker and an `_redacted` key. The real `~/.codex` was
never written to: logged-out and isolation runs used temp `CODEX_HOME`
directories, and the logged-in ones symlinked `auth.json` read-only.
