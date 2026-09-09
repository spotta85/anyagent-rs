# Windows support — handoff

You are on the Windows laptop, on branch `windows-support` of anyagent (a Rust
crate that finds coding agents installed on a machine and drives them through
one typed API). Your job: get the crate working on Windows one agent at a time,
log every bug, fix what you can, and stop after each agent so Sid can review
the fixes on the Mac.

Read `README.md` and `docs/architecture.mdx` first. `.claude/` and `.agents/`
are gitignored, so the skills are not on this machine; what you need from them
is inlined below.

## What already happened (Mac side)

Compile fixes are in the branch (`git log windows-support ^main`):

| Change | Where |
|---|---|
| PATH split/join via `std::env::split_paths` / `join_paths` (`;` on Windows) | `src/process.rs`, `src/discovery.rs` |
| Home dir via `std::env::home_dir()` (`USERPROFILE` on Windows) | `src/discovery.rs`, `tests/live.rs` |
| Executables resolve as `.exe` / `.cmd` / `.bat` on Windows | `src/discovery.rs` `EXE_SUFFIXES` |
| No login-shell PATH capture on Windows | `src/process.rs` |
| Process-group field is unix-only | `src/process.rs` |
| Tests that write `sh` wrapper scripts are unix-only for now | `src/*.rs` test mods, `tests/*.rs` `#![cfg(unix)]` |

Verified from the Mac: `cargo clippy --all-targets --target x86_64-pc-windows-msvc -D warnings` is clean.
Nothing has actually *run* on Windows yet. That is your job.

## How Windows-specific code is written here

Pick the first rule that fits. Never sprinkle `#[cfg]` inside a function body.

| Situation | Do this | Example in repo |
|---|---|---|
| std has a cross-platform API | use it, no cfg at all | `split_paths`, `home_dir` |
| One value differs | one `#[cfg(unix)]` / `#[cfg(windows)]` const pair | `EXE_SUFFIXES` in discovery.rs |
| Whole behaviour differs | two same-signature fns, one per OS; callers see no cfg | `is_executable`, `keychain_present` |
| Early return on one OS | `if cfg!(windows) { return ... }` so both branches still type-check | `capture_login_shell_path` |
| Needs an OS API | `[target.'cfg(windows)'.dependencies] windows-sys = ...`, mirroring `libc` | Cargo.toml |
| A test needs a `sh` script | gate the module `#[cfg(all(test, unix))]` or file `#![cfg(unix)]` until step 2 replaces the script | `tests/claude.rs` |

Comments: 1-2 lines per fn saying what it does. Keep diffs small; the simplest
fix that gives the exact expected behaviour wins.

## Rules

1. No subagents.
2. Never touch real auth: no deleting or moving `~/.claude`, `~/.codex`,
   `~/.local/share/opencode`, no logouts. Sid logs in by hand.
3. Fix only *structural* bugs (crash, wrong error type, missing event, hang,
   wrong path). Model-output flakes: rerun once; two identical failures is a
   finding, not something to fix.
4. `SKIP` lines in the live suite are passes. Record them, don't chase them.
5. After each step below that says **STOP**: commit on `windows-support` as
   `windows: <step or harness>`, make sure `WINDOWS_LOG.md` is up to date, and
   end your turn with a short summary so Sid can take the fixes to the Mac.
6. Before every commit: `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

## Steps

### Step 0 — prerequisites

```powershell
cargo --version   # need 1.88+
node --version    # fixtures need node on PATH
git status        # on windows-support, clean
```

### Step 1 — build and offline tests, no agents installed

```powershell
cargo build
cargo clippy --all-targets -- -D warnings
cargo test
cargo test --test smoke -- --nocapture     # expect only MISSING lines
cargo run --example probe                  # expect every agent missing, with install hints
```

Log the test count and any failure. Expect fewer tests than the Mac's 223:
fixture-driven files are gated. Continue if green.

### Step 2 — make the fixture tests run on Windows  **STOP after**

The fixture files (`tests/{acp,claude,codex,opencode,pi,antigravity}.rs`,
`src/runtime.rs` and `src/discovery.rs` test mods) each write a
`#!/bin/sh` wrapper that execs `node fixture.mjs <flags>`. Replace that with
a wrapper that works on both: on Windows write `<name>.cmd` containing
`@echo off` + `node "<fixture>" <flags> %*`; on unix keep the sh script and the
0o755 bit. One shared helper in `tests/common/mod.rs` if the six copies fit
the same signature; otherwise make each one cross-platform in place. Drop the
`#![cfg(unix)]` gates and run `cargo test`.

This step is where Windows process handling shows up with no real agent:
`.cmd` shims spawn through `cmd.exe`, so `node` is a grandchild. Watch for
orphaned `node` processes after tests (`Get-Process node`) and for shutdown
tests that hang. The expected fix is a Job Object (`windows-sys`,
`CreateJobObjectW` + `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`) as the Windows
half of the process-group code in `src/process.rs`. Log it; fix it if it is
under ~40 lines, otherwise log the design and stop.

### Step 3 — claude  **STOP after**

Sid installs and logs in. Then:

```powershell
cargo test --test smoke -- --nocapture         # expect FOUND claude
cargo run --example probe
$env:ANYAGENT_LIVE="claude"; cargo test --test live -- --ignored --nocapture --test-threads=1
```

Fill the log, fix structural bugs, rerun, commit, stop.

### Step 4 — codex  **STOP after**

Same loop. npm installs give `codex.cmd`; note which install method Sid used.

### Step 5 — opencode  **STOP after**

Same loop. Needs `OPENROUTER_API_KEY` in the environment or the suite skips it.
Check where opencode keeps `auth.json` on Windows; the catalog assumes
`~/.local/share/opencode`.

### Step 6 — the rest, all at once  **STOP after**

hermes, kiro, pi, cursor, antigravity, grok, qwen. First check which ones even
ship for Windows and log "not available on Windows" for the ones that don't.
Then one live run with `ANYAGENT_LIVE=all`.

## Where Windows will most likely need more code

| Area | Status | Where |
|---|---|---|
| PATH separator, home dir, exe suffixes, login shell | done | above |
| Kill the whole process tree (Job Object) | expected in step 2 | `src/process.rs` |
| Known install dirs: `%APPDATA%\npm`, `%LOCALAPPDATA%\Programs`, scoop shims, nvm-windows, fnm on Windows | expected in steps 3-6 | `search_dirs`, `version_manager_dirs` in `src/discovery.rs` |
| Per-agent config/auth dirs on Windows (keychain is macOS-only, so file markers must exist) | verify per harness | `src/catalog.rs` |
| `file://` URIs for `C:\` paths in attachments | if attachment tests fail | `src/adapter/attach.rs` `file_uri` |
| CRLF in agent stdout | if a JSON-lines parser fails | adapter readers |
| Linux | after Windows; mostly free, same unix code paths | CI matrix would prove it |

## Log format — `WINDOWS_LOG.md`

One section per step. Keep it terse; Sid reads this on the Mac to review fixes.

```markdown
## Step 3 — claude — 2026-09-10
claude 2.x, native installer, %USERPROFILE%\.local\bin\claude.exe

| test | result | note |
|---|---|---|
| discover | PASS | |
| cancel | FAIL | hung at "waiting for TurnEnded" |

### B1 short title
- Symptom: exact assertion or error text
- Cause: one sentence
- Fix: src/file.rs:line, ~N lines, commit abc123   (or: open, needs Mac review)
```

## Suggested skills

`superpowers:systematic-debugging` for any failure before proposing a fix;
`superpowers:verification-before-completion` before every commit.
