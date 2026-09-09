//! Launches agent processes and guarantees child cleanup.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::sync::OnceCell;

use crate::error::AgentError;

const STDERR_TAIL_LINES: usize = 6;
const LOGIN_SHELL_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything needed to launch one agent process.
pub(crate) struct Spawn {
    pub exec_path: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
}

/// A running agent process and the process group it leads. Dropping it
/// without `shutdown` kills the whole group.
pub(crate) struct Child {
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    inner: tokio::process::Child,
    /// The group id, captured at spawn: `id()` is gone once the leader is
    /// reaped, but workers in the group may still be running.
    #[cfg(unix)]
    pgid: Option<i32>,
    /// `shutdown` ran; `Drop` has nothing left to kill.
    finished: bool,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
}

/// Launches an agent with a PATH suitable for GUI applications.
pub(crate) async fn spawn(spec: Spawn) -> Result<Child, AgentError> {
    let path = compose_path(
        &spec.exec_path,
        std::env::var("PATH").ok().as_deref(),
        login_shell_path().await.as_deref(),
    );
    let mut command = Command::new(&spec.exec_path);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .env("PATH", path)
        .envs(spec.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Own process group: agents that dispatch to a worker (kiro-cli spawns
    // kiro-cli-chat, which inherits the pipes) are then signalled as a unit,
    // so no worker outlives the session holding stderr open.
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|e| AgentError::SpawnFailed(format!("{}: {e}", spec.exec_path.display())))?;

    let stderr_tail = Arc::new(Mutex::new(VecDeque::new()));
    let stderr_task = child.stderr.take().map(|stderr| {
        let tail = Arc::clone(&stderr_tail);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut tail = tail.lock().unwrap_or_else(|e| e.into_inner());
                if tail.len() == STDERR_TAIL_LINES {
                    tail.pop_front();
                }
                tail.push_back(line);
            }
        })
    });
    Ok(Child {
        stdin: child.stdin.take(),
        stdout: child.stdout.take(),
        #[cfg(unix)]
        pgid: child.id().map(|pid| pid as i32),
        finished: false,
        inner: child,
        stderr_tail,
        stderr_task,
    })
}

impl Child {
    /// Whether the child is still running, without waiting.
    pub fn is_running(&mut self) -> bool {
        matches!(self.inner.try_wait(), Ok(None))
    }

    /// Last stderr lines, for error reports.
    pub fn stderr_tail(&self) -> String {
        let tail = self.stderr_tail.lock().unwrap_or_else(|e| e.into_inner());
        tail.iter().cloned().collect::<Vec<_>>().join("\n")
    }

    /// Waits for the child and stderr reader for at most `grace` each. A
    /// leader that already died takes its workers with it; once reaped, the
    /// pgid may be recycled, so `Drop` must not signal it again.
    pub async fn exit_status(&mut self, grace: Duration) -> String {
        if !self.is_running() {
            self.kill_group();
        }
        let status = tokio::time::timeout(grace, self.inner.wait()).await;
        self.finished = matches!(status, Ok(Ok(_)));
        if let Some(task) = self.stderr_task.take() {
            let _ = tokio::time::timeout(grace, task).await;
        }
        match status {
            Ok(Ok(status)) => status_text(status),
            _ => "unknown".into(),
        }
    }

    /// SIGTERM to the group, a grace period for the leader, then SIGKILL to
    /// the group: workers that ignored the SIGTERM must not outlive it.
    pub async fn shutdown(&mut self, grace: Duration) {
        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            // Negative pid signals the whole group; the child leads its own.
            unsafe { libc::kill(-pgid, libc::SIGTERM) };
        }
        let _ = tokio::time::timeout(grace, self.inner.wait()).await;
        self.kill_group();
        let _ = self.inner.kill().await;
        self.finished = true;
        // The reader ends at stderr EOF; joining it here makes `stderr_tail`
        // complete for error reports (a child that dies at spawn can lose the
        // race between its last lines and the caller reading the tail).
        if let Some(task) = self.stderr_task.take() {
            let _ = tokio::time::timeout(grace, task).await;
        }
    }
}

/// One wording on every platform: windows' `ExitStatus` says "exit code",
/// unix says "exit status". Abnormal windows codes keep their own hex form.
#[cfg(windows)]
fn status_text(status: std::process::ExitStatus) -> String {
    match status.code() {
        Some(code) if code >= 0 => format!("exit status: {code}"),
        _ => status.to_string(),
    }
}

#[cfg(unix)]
fn status_text(status: std::process::ExitStatus) -> String {
    status.to_string()
}

impl Child {
    /// SIGKILL to the group; harmless when it is already gone.
    fn kill_group(&self) {
        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            unsafe { libc::kill(-pgid, libc::SIGKILL) };
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if !self.finished {
            self.kill_group();
        }
    }
}

/// Child PATH in lookup order, with duplicates removed.
fn compose_path(exec_path: &Path, own: Option<&str>, login: Option<&str>) -> OsString {
    let mut seen = std::collections::HashSet::new();
    let dirs = exec_path
        .parent()
        .map(Path::to_path_buf)
        .into_iter()
        .chain(split_path(own))
        .chain(split_path(login))
        .filter(|dir| !dir.as_os_str().is_empty() && seen.insert(dir.clone()));
    std::env::join_paths(dirs).unwrap_or_default()
}

/// The entries of a PATH string, empty ones included (callers filter).
fn split_path(path: Option<&str>) -> impl Iterator<Item = PathBuf> + '_ {
    std::env::split_paths(path.unwrap_or_default())
}

/// Returns the login-shell PATH, captured once per process.
pub(crate) async fn login_shell_path() -> Option<String> {
    static CACHE: OnceCell<Option<String>> = OnceCell::const_new();
    CACHE.get_or_init(capture_login_shell_path).await.clone()
}

/// Runs the login shell with a non-interactive fallback. Windows has no
/// login shell; GUI apps there already get the registry PATH.
async fn capture_login_shell_path() -> Option<String> {
    if cfg!(windows) || std::env::var("ANYAGENT_NO_LOGIN_SHELL").is_ok_and(|v| v == "1") {
        return None;
    }
    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into());
    for flags in ["-lic", "-lc"] {
        if let Some(path) = shell_path(&shell, flags).await {
            return Some(path);
        }
    }
    None
}

/// Runs one shell command and extracts PATH past the output marker.
async fn shell_path(shell: &str, flags: &str) -> Option<String> {
    let output = tokio::time::timeout(
        LOGIN_SHELL_TIMEOUT,
        Command::new(shell)
            .arg(flags)
            .arg("echo __anyagent__$PATH")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("__anyagent__"))
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs `script` under node, the one interpreter both platforms have.
    fn node(script: &str) -> Spawn {
        Spawn {
            exec_path: PathBuf::from("node"),
            args: vec!["-e".into(), script.into()],
            cwd: std::env::temp_dir(),
            env: Vec::new(),
        }
    }

    #[cfg(unix)]
    fn sh(script: &str) -> Spawn {
        Spawn {
            exec_path: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), script.into()],
            cwd: std::env::temp_dir(),
            env: Vec::new(),
        }
    }

    /// compose_path dedupes and orders exec dir > own PATH > login-shell PATH.
    #[test]
    fn compose_path_orders_and_dedupes() {
        let joined = |dirs: [&str; 3]| std::env::join_paths(dirs).unwrap().into_string().unwrap();
        let path = compose_path(
            Path::new("/opt/agent/bin/claude"),
            Some(&joined(["/usr/bin", "/opt/agent/bin", "/usr/bin"])),
            Some(&joined(["/usr/bin", "/home/u/.volta/bin", "/usr/bin"])),
        );
        assert_eq!(
            path,
            OsString::from(joined(["/opt/agent/bin", "/usr/bin", "/home/u/.volta/bin"]))
        );
    }

    /// Shutdown escalates to SIGKILL when child traps SIGTERM within grace.
    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_escalates_to_sigkill_within_grace() {
        let mut child = spawn(sh("trap '' TERM; sleep 30")).await.unwrap();
        let start = std::time::Instant::now();
        child.shutdown(Duration::from_millis(200)).await;
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// Shutdown lets cooperative child exit cleanly on SIGTERM.
    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_lets_a_cooperative_child_exit_on_sigterm() {
        let mut child = spawn(sh("sleep 30")).await.unwrap();
        child.shutdown(Duration::from_secs(5)).await;
    }

    /// stderr_tail retains last 6 lines for ProcessExited reports.
    #[tokio::test]
    async fn stderr_tail_keeps_the_last_lines() {
        let mut child = spawn(node(
            "for (let i = 1; i <= 8; i++) console.error('line' + i)",
        ))
        .await
        .unwrap();
        let status = child.exit_status(Duration::from_secs(5)).await;
        assert_eq!(status, "exit status: 0");
        assert_eq!(
            child.stderr_tail(),
            "line3\nline4\nline5\nline6\nline7\nline8"
        );
    }
}
