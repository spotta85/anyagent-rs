# Windows log

Driven from the Mac over `ssh sshdev@10.0.0.119`; code is written and
pre-verified on the Mac (unix `cargo test` + `--target x86_64-pc-windows-msvc`
clippy), then pulled and run on the Windows box.

## Step 1 — build and offline tests — 2026-09-09

`cargo build`, `cargo test` clean. 56 unit + 1 smoke + 1 doctest.
Mac ran 65 unit tests: the 9 missing ones were the `#[cfg(all(test, unix))]`
mods. All six fixture suites reported "running 0 tests" (file-level
`#![cfg(unix)]`), which is what step 2 exists to fix.

## Step 2 — fixture suites on Windows — 2026-09-09 — in progress

`#!/bin/sh` wrappers replaced by a per-OS shim writer: `<exe>.cmd` running
`node "<fixture>" <flags> %*` on Windows, the sh script + 0o755 elsewhere.
One copy in `tests/common/mod.rs` (integration), one in `src/testutil.rs`
(unit tests cannot reach `tests/`). All `#![cfg(unix)]` gates removed.

Last full Windows run was commit `cc5c95e`, before the two fixes below. The
run's ssh pipe reset partway, so `opencode`, `pi`, `smoke` and the doctests
have not been observed yet.

| suite | result at cc5c95e |
|---|---|
| lib | 61 pass / 2 fail (B1) |
| acp | 43 pass / 1 fail (B2) |
| antigravity | 11 pass / 0 fail |
| claude | 38 pass / 1 fail (B3, output lost) |
| codex | `probe_reads_commands_without_waiting_them_out` FAILED, rest lost |
| opencode, pi, smoke, doctests | not reached |

### B1 discovery tests redirected the wrong home var
- Symptom: `assertion failed: agent.upgrade.is_none()`, and
  `left: "...\.local/bin\agy.cmd" right: "...\.local/bin\agy"`
- Cause: two things. The tests set `HOME`, but `std::env::home_dir()` reads
  `USERPROFILE` on Windows, so discovery searched the real profile instead of
  the temp one. And one assertion compared against a hardcoded `agy` instead
  of the path the shim writer returned (`agy.cmd` on Windows).
- Fix: `src/testutil.rs` `HOME_VAR` cfg pair + `src/runtime.rs:618`, ~6 lines,
  commits `cc5c95e` and `a8e28a2`. Not re-verified on Windows yet.

### B2 exit status wording differs by OS
- Symptom: `tests/acp.rs:353` left `"exit code: 3"`, right `"exit status: 3"`
- Cause: `std::process::ExitStatus`'s Display says "exit code" on Windows and
  "exit status" on unix, and that string is what `AgentError::ProcessExited`
  carries to the caller. A cross-platform API should not change its error text
  per OS.
- Fix: `status_text` cfg pair in `src/process.rs`, ~12 lines, commit
  `a8e28a2`. Abnormal Windows codes keep Rust's hex form. Not re-verified.

### B3 open — one claude failure, one codex failure
- Symptom: unknown; the ssh pipe reset before the detail printed.
- Next: rerun redirecting to a file on the Windows side, then read the file.

### Open — process tree kill
Not yet observed. `.cmd` shims run through `cmd.exe`, so `node` is a
grandchild and `Child::kill_group` is a unix-only no-op on Windows. Check
`Get-Process node` after a suite run; the expected fix is a Job Object in
`src/process.rs`.
