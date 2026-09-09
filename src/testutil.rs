//! Executable stand-ins for the unit tests, written the way each OS can run
//! them. The integration suites keep their own copy in `tests/common`.

use std::path::{Path, PathBuf};

/// A shim at `dir/<exe>` that runs `js` with scenario flags, ignoring the
/// launch args appended after them. Returns the path it actually wrote.
#[cfg(windows)]
pub fn shim(dir: &Path, exe: &str, js: &Path, flags: &str) -> PathBuf {
    let path = dir.join(format!("{exe}.cmd"));
    let body = format!("@echo off\r\nnode \"{}\" {flags} %*\r\n", js.display());
    std::fs::write(&path, body).unwrap();
    path
}

#[cfg(unix)]
pub fn shim(dir: &Path, exe: &str, js: &Path, flags: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(exe);
    let body = format!("#!/bin/sh\nexec node '{}' {flags} \"$@\"\n", js.display());
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// An installed executable that does nothing but exit 0, named so discovery's
/// suffix list finds it on either platform.
pub fn stub(dir: &Path, name: &str) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    write_stub(dir, name)
}

#[cfg(windows)]
fn write_stub(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(format!("{name}.cmd"));
    std::fs::write(&path, "@echo off\r\nexit /b 0\r\n").unwrap();
    path
}

#[cfg(unix)]
fn write_stub(dir: &Path, name: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}
