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

## Step 5 — opencode — 2026-09-09

opencode 1.18.30, `npm install -g opencode-ai` (**not** the PowerShell
installer), `C:\Users\sshdev\AppData\Roaming\npm\opencode.cmd`. `~\.opencode\bin`
does not exist, so the registry-PATH-edit concern did not arise: npm's dir was
already on PATH.

`opencode models` lists `opencode/big-pickle`, so the pin was valid — but see
B11; it was changed for a different reason.

| step | result |
|---|---|
| 1. install + location | npm, `%APPDATA%\npm\opencode.cmd` |
| 2. probe | FOUND, source `Path`. 7 models, 3 commands, fork/rollback |
| 3. known-dirs fix | not needed; npm's dir is on PATH |
| 4. auth marker | none matched — and that is correct, see B13 |
| 5. live suite | 31 passed, 1 failed, 352s. 8 SKIP |

Offline suite 221/221 (Mac 223). One `opencode.exe` remains on the box but
carries no `serve` and predates the run — the user's own, not a leak.

### B11 the pinned model could not see images, on both platforms
- Symptom: `opencode: image answer was "I can't read the image - this model
  doesn't support image input."`
- Cause: not a Windows issue. `opencode/big-pickle` answers image prompts this
  way about a third of the time on macOS too — measured 2 PASS / 1 FAIL in
  three Mac runs, each failure worded differently. The suite had a latent flake
  that this step happened to surface.
- Fix: `OPENCODE_MODEL` pinned to `opencode/muse-spark-1.2-contributor-free`,
  which passed 3/3 on the Mac and then on Windows. `tests/live.rs`, 1 line,
  commit `27b5565`. Test-only.

### B12 the opencode kill pattern assumed an unquoted command line
- Symptom: `opencode: no process matched "opencode serve"`
- Cause: the npm install leaves a single `opencode.exe`, but its command line
  quotes the executable and pads before the argument:
  `"...\opencode-ai\bin\opencode.exe"    serve --hostname 127.0.0.1 --port N`.
  The literal `opencode serve` appears nowhere.
- Fix: pattern widened to `opencode(\.exe)?"? +serve`, matching both platforms.
  `tests/live.rs`, 1 line, commit `27b5565`. Test-only. Verified on the Mac
  (`PASS opencode: death maps to Failed + ProcessExited + closed`) and Windows.

### B13 discovery_finds_authenticated_harnesses — not a bug, the box is logged out
- Symptom: `opencode: not authenticated: Some(Unauthenticated { ... })`
- Investigated, **no change made**. `opencode auth list` reports 2 credentials
  on the Mac and **0 on Windows**, and names the same path on both
  (`~/.local/share/opencode/auth.json` / `~\.local\share\opencode\auth.json`).
  The catalog's `config_dir` is correct on Windows; there is simply no login on
  that box. Discovery reporting `Unauthenticated` is the truth.
- The other 31 tests pass because the free Zen models need no credential, and
  the adapter's own open-time view reports `Other("connected provider")` —
  a different and also correct notion of auth.
- To clear it: run `opencode auth login` on the Windows box and rerun. Left to
  the user; the suite never touches real auth.

### Windows-vs-Mac triage for this step
Two failures in the first Windows run turned out to be neither Windows bugs nor
reproducible: `opencode_child_session_permissions_reach_the_caller` (handshake
timeout) and `turn_events_are_bracketed_ordered_and_quiet_after_end`
(`SessionUpdated` arriving after turn end). Both passed on the Mac, and both
passed on Windows once the model changed. Recorded here rather than "fixed" —
if either returns, the model swap is the first thing to suspect.

## Step 6 — the rest — 2026-09-09

**Rebased onto main** (2026-09-10), picking up #18 (grok and qwen in the live
matrix) and #19 (discovery stops reporting login state). Items below that
those PRs closed are marked in place.

**Every harness ships for Windows.** Nothing to record as "not available":
all ten in the catalog were installed, discovered and (bar cursor) reported
their auth. Nine resolve through `Path`; only claude needs `extra_paths`. Ten
for ten on PATH means the handoff's predicted Windows `known_dirs()` cfg pair
was **never needed** and has not been written.

| harness | found at | source |
|---|---|---|
| antigravity 1.2.0 | `%LOCALAPPDATA%\agy\bin\agy.exe` | Path |
| cursor | `%LOCALAPPDATA%\cursor-agent\cursor-agent.cmd` | Path |
| grok | `%APPDATA%\npm\grok.cmd` | Path |
| hermes 0.21.1 | `%LOCALAPPDATA%\hermes\bin\hermes.exe` | Path |
| kiro 2.21.2 | `%LOCALAPPDATA%\Kiro-Cli\kiro-cli.exe` | Path |
| pi 0.85.1 | `%APPDATA%\npm\pi.cmd` | Path |
| qwen 0.23.2 | `%APPDATA%\npm\qwen.cmd` | Path |

`ANYAGENT_LIVE=all`: 27 passed / 5 failed at first, 29 / 3 after the fixes
below, 1448 s. B13 closed once the user logged into opencode.

### The recurring root cause
Five separate bugs were one mistake: comparing an executable name exactly,
which never survives Windows' `.exe`/`.cmd` suffix, and matching a command
line that joins the exe to its first argument, which **Rust's `Command`
separates with a quote** (`"...\agy.exe" --input-format=...`). B3 was the
library instance; B14-B17 are the harness ones. A grep of `src/` afterwards
found no others: the only remaining `file_name()` uses are `is_named` itself
and version-directory sorting, where names carry no suffix.

### B14 HEADLESS_AGY missed the .exe suffix
- Symptom: `antigravity: no question request opened`
- Cause: the flag compared `file_name() == "agy"`, but windows installs
  `agy.exe`, so it stayed false and the test's own
  `SKIP antigravity: headless agy cannot ask` branch never fired — it demanded
  a question from a CLI that structurally cannot ask one.
- Fix: `file_stem()`, which still tells the CLI from `agy_acp_server.par`.
  `tests/live.rs`, 1 line. Test-only. Mac still passes.

### B15 kiro kill pattern
- Symptom: `kiro: no process matched "kiro-cli(-chat)? acp$"`
- Cause: windows has no `kiro-cli-chat` worker, and our spawn quotes the path:
  `"...\kiro-cli.exe" acp`. Note a *manually* launched kiro is unquoted — the
  quote comes from how we spawn it, which is why the first fix attempt
  (`(\.exe)?` alone) still failed.
- Fix: `kiro-cli(-chat)?(\.exe)?"? acp$`. The `$` anchor is kept so the user's
  own `kiro-cli acp --agent <name>` processes stay out. Test-only.

### B16 cursor kill pattern
- Symptom: `cursor: no process matched "cursor-agent .*index.js acp$"`
- Cause: windows runs a four-link chain — `cmd -> cursor-agent.ps1 -> node
  index.js acp` — and the node leaf's path reads `cursor-agent\versions\...`,
  a path separator exactly where the pattern demanded a literal space.
- Fix: `cursor-agent.*index\.js acp$`. Test-only.

### B17 antigravity kill pattern
- Symptom: `antigravity: no process matched`
- Cause: the quoted exe path again, between `agy` and ` --input-format=`.
- Fix: `agy(\.exe)?"?( --input-format=stream-json|_acp_server)`. Test-only.

### B18 ANYAGENT_LIVE=all silently selected nothing
- Symptom: 32 tests reported `ok` in 0.02 s with no PASS lines.
- Cause: the comma-list branch trimmed each name but the `all` branch compared
  the raw string, and cmd's `set ANYAGENT_LIVE=all && cargo test` assigns a
  **trailing space**. `claude` worked; `all` did not.
- Fix: `list.trim() == "all"`. Test-only, but it would have silently voided any
  windows `all` run.

### B19 hermes auth lives in %LOCALAPPDATA% — **superseded by #19**
The marker system this fixed no longer exists: discovery reports no login
state, and probe is the only auth source. The `#[cfg]` went with it.
- Symptom: `hermes: not authenticated`, while its own open-time view said
  `Authenticated { kind: ApiKey }`
- Cause: hermes keeps `auth.json` in `~/.hermes` on macOS but in
  `%LOCALAPPDATA%\hermes` on windows, so no marker matched and discovery
  reported a logged-in agent as logged out.
- Fix: `#[cfg]` on the `config_dir` field of the hermes profile only —
  no new struct field, no per-profile `None` noise, no dead code on unix.
  `src/catalog.rs`, 4 lines. **Library fix.** Verified:
  `PASS hermes: discovered and authenticated`.

### Not our bugs, investigated and left alone
- **kiro `effort`**: windows defaults to model `auto`, which lists no effort
  levels; the Mac is pinned to `claude-opus-4.8`, which lists five. Pinning
  the Mac's model on windows reproduces all five. Config, not platform, and
  our parsing of `session/new` has no platform branch.
- **cursor discovery auth**: `.cursor` holds no credential file at all
  (`acp-config.json` 2 B, `agent-cli-state.json` 94 B of tip flags), so cursor
  keeps its token in the OS credential store. The catalog's only non-env
  marker is `Keychain`, which is macOS-only. Reading Windows Credential
  Manager is a feature, not a catalog tweak. **Closed by #19**: discovery no
  longer reads credentials, so there is no store to find.

### Cross-platform findings that Windows merely surfaced
- **Late `SessionUpdated`**: reproduces on the Mac (1 PASS / 1 FAIL in two
  hermes runs). The agent sets the session title asynchronously and the event
  can land after `TurnEnded`; `quiet()` sanctions `Diagnostic`,
  `PlanUsageUpdated` and `StatusChanged` but not `SessionUpdated`. Step 5's
  opencode instance was the same race — the model swap hid it, it did not fix
  it. **Fixed** (Sid's call): `quiet()` now sanctions `SessionUpdated`, since
  agents title a thread asynchronously after the first turn. `tests/live.rs`,
  3 lines.
- **pi is gated on a variable it does not need**: `build_roster` skips pi
  unless `OPENROUTER_API_KEY` is set, but pi is logged in through
  `~/.pi/agent/auth.json` on both machines. pi's profile has **no `ConfigFile`
  marker**, only `ApiKeyEnv` ones, so discovery cannot see that login. Setting
  the variable to pass the gate then makes an empty config home look
  authenticated and fails `config_home_isolates_login`. Two bugs stacked, both
  cross-platform. **Closed by #19**: the gate now runs `pi auth check`, and
  with markers gone there is no `ConfigFile` to add.
- **pi `compact`**: `compaction refused: Nothing to compact (session too
  small)` on both platforms once pi actually runs.
- **grok and qwen have no live coverage anywhere** — they are in the catalog
  and discovered, but absent from `HARNESSES`, so `ANYAGENT_LIVE=all` covers
  eight harnesses, not ten. **Closed by #18.**

### Environment note
`OPENROUTER_API_KEY` was set as a User variable on the windows box during this
step to get past the roster gate. It distorts pi's auth state and should be
removed; the key itself should be rotated, having passed through a command
line and a chat transcript. **Removed** from `HKCU\Environment` on 2026-09-10
(a fresh session no longer sees it); Sid rotates the key.

## Step 6b — rerun on the rebased code — 2026-09-10

First windows run of main's #18/#19. Offline 221 / 0 (Mac 223). fmt and
clippy clean on both.

`ANYAGENT_LIVE=all`: 19 passed / 13 failed, 1921 s. No new failure types:

| cause | tests | status |
|---|---|---|
| grok `turn stopped: rate_limit` | 10 | quota, deferred to Sid's note |
| pi kill pattern | 1 | B20, fixed |
| qwen vision bridge | 1 | box config, see below |
| kiro `effort` | 1 | known, model `auto` (step 6) |

What #19 and the `quiet()` change bought on windows:
- `discovery_finds_authenticated_harnesses`: all ten authenticated by probe,
  cursor included.
- `config_home_isolates_login`: pi passes (the key is gone, the gate asks pi).
- `turn_events_are_bracketed_ordered_and_quiet_after_end`: all eight before
  grok pass, hermes included. Mac: 3 / 3 runs on hermes and opencode.

A panicking harness ends its test for everyone after it in `HARNESSES`, so
grok's failures hid qwen, and kiro's effort failure hid pi and cursor. Those
were rerun alone:

| rerun | result |
|---|---|
| kill test, pi / cursor / antigravity / qwen | 4 PASS (B20 fixed) |
| qwen, whole suite | 30 passed / 2 failed: image (below) and `generate` |
| effort, pi / cursor | pi PASS; cursor SKIP (its model lists no levels) |

qwen `generate` answered a correct title in Chinese (`如何重命名 Git 分支`),
failing the `branch` check — model output from the box's `qwen3-coder`.
Rerun gave the same answer, so it is a finding, not a flake: the same config
gap as the image test below, and the same fix (a qwen model pin). No change.

### B20 the pi kill pattern needs a command line on windows
- Symptom: `pi: no process matched "pi"`
- Cause: unix pi overwrites its argv with its process title, so the test
  matched the exact name. Windows has no title: pi is the npm shim's
  `node.exe`, and its command line is the only marker left.
- Fix: `PI_MATCH` const pair — `-x pi` on unix,
  `pi-coding-agent.*cli\.js"? --mode rpc` on windows. `tests/live.rs`,
  7 lines, commit eec2346. Test-only. Mac still passes.

### qwen image — config, not platform
- Symptom: `Vision bridge (openai/gpt-oss-120b:free) failed`
- Cause: the box's qwen model is `qwen/qwen3-coder` and every provider it
  lists is text-only, so the vision bridge has nothing that sees images. The
  Mac runs `z-ai/glm-5.3-flash`, which the box does not list. The suite pins
  models for claude, codex, opencode and pi, but not qwen.
- **No change.** Either align the box's qwen config or pin a qwen model both
  machines list.

### Open — graceful shutdown has no cross-platform path
The one open design item on this branch.
Not a Windows-only issue. `CLOSE_GRACE` means "time to exit after being asked
nicely", but the only ask is a unix SIGTERM. These agents speak JSON-lines over
stdio, so dropping stdin (EOF) would be the portable ask — and would make unix
closes faster too, since agents would exit on EOF instead of on the signal.
Stdin is owned by `adapter/wire.rs`'s writer, so this needs plumbing through
the six adapters. Separate change; the tree kill is needed underneath it either
way.
