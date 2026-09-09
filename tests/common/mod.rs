//! Shared helpers for the fixture-driven adapter suites.

use std::path::{Path, PathBuf};

/// An agent stand-in on disk: runs `tests/fixtures/<fixture>/fixture.mjs` with
/// scenario flags, ignoring the real launch args appended after them. `name`
/// only keeps concurrent scenarios in separate temp dirs.
pub fn wrapper(fixture: &str, exe: &str, name: &str, flags: &str) -> PathBuf {
    let js = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(fixture)
        .join("fixture.mjs");
    let dir =
        std::env::temp_dir().join(format!("anyagent-{fixture}-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    shim(&dir, exe, &js, flags)
}

/// A `.cmd` batch shim; `%*` forwards the args cmd.exe passes through.
#[cfg(windows)]
pub fn shim(dir: &Path, exe: &str, js: &Path, flags: &str) -> PathBuf {
    let path = dir.join(format!("{exe}.cmd"));
    let body = format!("@echo off\r\nnode \"{}\" {flags} %*\r\n", js.display());
    std::fs::write(&path, body).unwrap();
    path
}

/// A `#!/bin/sh` script carrying the execute bit.
#[cfg(unix)]
pub fn shim(dir: &Path, exe: &str, js: &Path, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(exe);
    let body = format!("#!/bin/sh\nexec node '{}' {flags} \"$@\"\n", js.display());
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}
