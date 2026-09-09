# Windows log

Driven from the Mac over `ssh sshdev@10.0.0.119`. Code is written and
pre-verified on the Mac (`cargo test` plus clippy on
`--target x86_64-pc-windows-msvc`), then pulled and run on the Windows box.

## Step 1 — build and offline tests — 2026-09-09

`cargo build`, `cargo test` clean. 56 unit + 1 smoke + 1 doctest. The Mac ran
65 unit tests; the 9 missing ones were the `#[cfg(all(test, unix))]` mods. All
six fixture suites reported "running 0 tests" (file-level `#![cfg(unix)]`),
which is what step 2 exists to fix.

## Step 2 — fixture suites on Windows — 2026-09-09

`#!/bin/sh` wrappers replaced by a per-OS shim writer: `<exe>.cmd` running
`node "<fixture>" <flags> %*` on Windows, the sh script + 0o755 elsewhere. One
copy in `tests/common/mod.rs` (integration), one in `src/testutil.rs` (unit
tests cannot reach `tests/`). All `#![cfg(unix)]` gates removed.

| suite | before (cc5c95e) | after (d06f9d6) | Mac |
|---|---|---|---|
| lib | 61 / 2 fail | 63 / 63, 0.38s | 65 |
| acp | 43 / 1 fail, 16.8s | 44 / 44, 4.9s | 44, 2.6s |
| antigravity | 11 / 11, 8.6s | 11 / 11, 1.2s | 11, 0.5s |
| claude | 38 / 1 fail, 20.6s | 39 / 39, 3.3s | 39, 0.6s |
| codex | 28 / 1 fail | 29 / 29, 3.1s | 29, 0.7s |
| opencode | hung | 17 / 17, 2.2s | 17, 3.3s |
| pi | not reached | 14 / 2 fail (B5) | 16 |
| smoke, doctest | 1, 1 | 1, 1 | 1, 1 |

Orphaned `node.exe` after a full run: 20 before, 0 after.

### B1 discovery tests redirected the wrong home var
- Symptom: `assertion failed: agent.upgrade.is_none()`, and
  `left: "...\.local/bin\agy.cmd" right: "...\.local/bin\agy"`
- Cause: two things. The tests set `HOME`, but `std::env::home_dir()` reads
  `USERPROFILE` on Windows, so discovery searched the real profile instead of
  the temp one. And one assertion compared against a hardcoded `agy` instead
  of the path the shim writer returned.
- Fix: `HOME_VAR` cfg pair in `src/testutil.rs` + `src/runtime.rs:618`,
  ~6 lines, commits `cc5c95e`, `a8e28a2`. Test-only. Verified: lib 63/63.

### B2 exit status wording differed by OS
- Symptom: `tests/acp.rs:353` left `"exit code: 3"`, right `"exit status: 3"`
- Cause: `ExitStatus`'s Display says "exit code" on Windows, "exit status" on
  unix, and that string is what `AgentError::ProcessExited` hands the caller.
  A cross-platform API should not change its error text per OS.
- Fix: `status_text` cfg pair in `src/process.rs`, ~12 lines, commit
  `a8e28a2`. Abnormal Windows codes keep Rust's hex form. Library fix.

### B3 an ACP upgrade's login named the wrong binary
- Symptom: `tests\runtime.rs:683`, login command was
  `...\agy-acp-server\agy_acp_server.par.cmd` instead of `agy`
- Cause: an ACP upgrade has no login of its own, so `AuthRequired` falls back
  to the base CLI's. The fallback matched the upgrade by exact file name, which
  never hits on Windows where the file is `agy_acp_server.par.cmd`. The error
  then told the caller to run the ACP server binary to log in.
- Fix: `discovery::is_named` matches through `EXE_SUFFIXES`,
  `src/adapter/mod.rs:488`, ~9 lines, commit `6e5c64d`. Library fix.

### B4 no process-tree kill on Windows, and every close burned the grace
- Symptom: three faces of one bug. 20 orphaned `node.exe` after a run; the
  codex probe took 4.086s against a 2s bound (4.096s under load — a fixed
  cost, not contention); the opencode suite never finished. Suites ran ~7x
  slower than the Mac.
- Cause: `Child::kill_group` signalled a unix pgid and did nothing on Windows,
  so a `.cmd` shim's `node` grandchild outlived the `cmd.exe` that died. And
  `shutdown` sent SIGTERM under `#[cfg(unix)]` only — on Windows it sent
  nothing, then waited the full 2s `CLOSE_GRACE` for an exit it had not asked
  for. Stdin stays open until after `shutdown` returns, so there was no EOF to
  act as the ask either.
- Fix: `command-group` v5 owns the group on both sides (pgid on unix, Job
  Object on Windows). `src/process.rs`, ~30 lines net, commit `d06f9d6`. It
  also removed our `libc` dependency, `process_group(0)`, the `pgid` field and
  both hand-rolled `libc::kill` calls. Windows has no ask-to-exit signal, so
  `request_exit` is a no-op there and `shutdown` goes straight to the kill.
  Library fix.

### B5 pi tests hardcoded `/` in expected paths
- Symptom: `left: "...\sessions\s1.jsonl"` vs `right: "...\sessions/s1.jsonl"`
- Cause: the fixture builds its session path with node's `path.join`, which
  emits `\` on Windows. The crate passes the agent's path through unchanged,
  which is correct; the two assertions built their expected value with a
  literal `/`.
- Fix: `Path::ends_with` and a second `join` in `tests/pi.rs:180,547`,
  ~2 lines. Test-only.

## Step 3 — claude — 2026-09-09

claude 2.1.267, native installer (`irm https://claude.ai/install.ps1 | iex`),
`C:\Users\sshdev\.local\bin\claude.exe`.

The npm install on this box never completed: `%APPDATA%\npm\node_modules\
@anthropic-ai` is an empty directory and there are 0 `.cmd` shims, so
`where.exe claude` finds nothing and the two-install comparison was not
available. Only the native install was exercised. Both installs would share
one credential store anyway — auth lives in `%USERPROFILE%\.claude`, not
beside the binary.

| step | result |
|---|---|
| 1. install + location | native installer, `%USERPROFILE%\.local\bin\claude.exe` |
| 2. probe | FOUND, source `KnownLocation` — tier 5 (`extra_paths: .local/bin`), not PATH |
| 3. known-dirs fix | not needed; `extra_paths` already covered it |
| 4. auth marker | `ConfigFile(".credentials.json")` at `%USERPROFILE%\.claude\.credentials.json`. Keychain is macOS-only and correctly unused. Subscription, 5 models, 52 commands |
| 5. live suite | 32 passed, 0 failed, 179s. 3 SKIP (cursor x2, opencode — not enabled) |

Offline suite 221/221. No `node.exe` or `claude.exe` left after either run.

Both bugs below are in the live suite's own harness, not the library. The
library needed no Windows change for claude.

### B6 the live suite killed processes with pgrep
- Symptom: `a_killed_agent_fails_the_turn_and_closes_the_session` panicked
  with `Error { kind: NotFound, message: "program not found" }`
- Cause: `kill_child` shelled out to `pgrep` and `kill`, neither of which
  exists on Windows.
- Fix: `matching_pids` and `kill_pid` cfg pairs in `tests/live.rs` — pgrep on
  unix, a `Win32_Process` query honouring the same `-x`/`-n`/`-f` flags plus
  `taskkill /F` on Windows. ~45 lines, commit `3c115c3`. Test-only.

### B7 the process query matched itself
- Symptom: after B6, the turn ended `Completed { source: Protocol }` instead
  of `Failed`. The query returned two pids where only one was claude.
- Cause: the pattern was inlined into the PowerShell command, so that
  process's own command line contained the session id and matched. Sorted
  newest-first, the query process was the one that got killed — the agent was
  never touched.
- Fix: the pattern rides `ANYAGENT_KILL_PATTERN` in the environment, which is
  not part of any command line. `tests/live.rs`, ~4 lines, commit `e2ac5b4`.
  Test-only. It also removes the quoting hazard of inlining a pattern.

### B8 the kill assertion encoded a unix signal number
- Symptom: `claude: status was "exit status: 1"` against
  `assert!(status.contains('9'))`
- Cause: on unix `kill -9` reports `signal: 9 (SIGKILL)`; on Windows
  `taskkill /F` terminates with exit code 1. The library reported both
  correctly — the assertion just hardcoded the unix number.
- Fix: `KILLED_STATUS` cfg pair in `tests/live.rs`, ~5 lines, commit
  `39a1c91`. Test-only.

### B9 mixed path separators in every home-relative path
- Symptom: `FOUND claude at C:\Users\sshdev\.local/bin\claude.exe`
- Cause: catalog paths are written with forward slashes (`.local/bin`,
  `.local/share/opencode`, `.pi/agent`) and `home.join()` kept them verbatim.
  Functionally fine — Windows accepts both — but it reaches users through
  `MissingAgent.searched`, `config_home` and login commands.
- Fix: `under(home, rel)` in `src/discovery.rs` folds one component at a time;
  used by `version_manager_dirs`, `versions_newest_first`, `search_dirs`
  extras, `resolve_upgrade` extras and `config_home`. ~10 lines, commit
  `cde7882`. Verified: `C:\Users\sshdev\.local\bin\claude.exe`.

## Step 4 — codex — 2026-09-09

codex 0.153.4, `npm install -g @openai/codex`,
`C:\Users\sshdev\AppData\Roaming\npm\codex.cmd`. npm works on this box; the
earlier claude npm failure did not repeat.

`where.exe codex` returns both the bare bash shim and `codex.cmd`;
`EXE_SUFFIXES` correctly skipped the unusable bare one. This is the first real
`.cmd` shim exercised by a live agent rather than a fixture.

| step | result |
|---|---|
| 1. install + location | npm, `%APPDATA%\npm\codex.cmd` |
| 2. probe | FOUND, source `Path` |
| 3. known-dirs fix | not needed; npm's dir is on PATH |
| 4. auth marker | `ConfigFile("auth.json")` at `%USERPROFILE%\.codex\auth.json`. Subscription, 3 models, 6 commands, steer/fork/rollback/plan-usage |
| 5. live suite | 32 passed, 0 failed, 345s. 6 SKIP (2 cursor, 1 opencode not enabled; question request did not fire, file rollback not advertised, recording asserted on claude) |

Offline suite 221/221. No process of ours left after the run: four `codex`/
`node` processes remain, but none carries `app-server` and all predate the run
— they are the user's own codex sessions.

The library again needed no Windows change. The one failure was the live
harness.

### B10 the codex kill pattern assumed a one-process agent
- Symptom: `codex: no process matched "codex app-server"`
- Cause: the npm shim makes the agent a three-link chain on Windows —
  `cmd.exe "codex.cmd" app-server` -> `node.exe "...\codex.js" app-server` ->
  `codex.exe ...\vendor\...\codex.exe app-server`. The real agent is the leaf,
  and none of the three command lines contains the literal `codex app-server`.
- Fix: pattern widened to `codex(\.exe)? app-server`, which matches the leaf on
  both platforms and deliberately misses the `.js"` and `.cmd"` links.
  `tests/live.rs`, 1 line, commit `8759cc4`. Test-only. Killing the leaf
  cascades cleanly: node exits, then cmd, then the pipes close.
  Verified on Windows and on the Mac (`PASS codex: death maps to Failed +
  ProcessExited + closed`).

### Open — graceful shutdown has no cross-platform path
Not a Windows-only issue. `CLOSE_GRACE` means "time to exit after being asked
nicely", but the only ask is a unix SIGTERM. These agents speak JSON-lines over
stdio, so dropping stdin (EOF) would be the portable ask — and would make unix
closes faster too, since agents would exit on EOF instead of on the signal.
Stdin is owned by `adapter/wire.rs`'s writer, so this needs plumbing through
the six adapters. Separate change; the tree kill is needed underneath it either
way.
