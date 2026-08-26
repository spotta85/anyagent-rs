//! Launching and cleaning up harness processes. Harness is a child process, communicating via stdin/stdout. 

use std::collections::VecDeque;
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

/// Harness Process Config.
pub(crate) struct Spawn {
    pub exec_path: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>, // any extra env vars 
}

/// A running agent process. Killed on drop
pub(crate) struct Child {
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    inner: tokio::process::Child, // the actual os process
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
}

/// Launches the program using Spawn config. Returns a Child handle.
pub(crate) async fn spawn(spec: Spawn) -> Result<Child, AgentError> {
    // build child path with exec dir, current PATH, and login-shell PATH. 
    let path = compose_path(
        &spec.exec_path,
        std::env::var("PATH").ok().as_deref(),
        login_shell_path().await.as_deref(),
    );
    // basic tokio process spawn
    let mut child = Command::new(&spec.exec_path)
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .env("PATH", path)
        .envs(spec.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
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
        inner: child,
        stderr_tail,
        stderr_task,
    })
}

impl Child {
    /// Last stderr lines, for error reports.
    pub fn stderr_tail(&self) -> String {
        let tail = self.stderr_tail.lock().unwrap_or_else(|e| e.into_inner());
        tail.iter().cloned().collect::<Vec<_>>().join("\n")
    }

    /// Wait for child to exit w grace period and get exit status and kill stderr tail task.
    pub async fn exit_status(&mut self, grace: Duration) -> String {
        let status = tokio::time::timeout(grace, self.inner.wait()).await;
        if let Some(task) = self.stderr_task.take() {
            let _ = tokio::time::timeout(grace, task).await;
        }
        match status {
            Ok(Ok(status)) => status.to_string(),
            _ => "unknown".into(),
        }
    }

    /// SIGTERM, then SIGKILL when the grace period expires.
    pub async fn shutdown(&mut self, grace: Duration) {
        #[cfg(unix)]
        if let Some(pid) = self.inner.id() {
            unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            if tokio::time::timeout(grace, self.inner.wait()).await.is_ok() {
                return;
            }
        }
        let _ = self.inner.kill().await;
    }
}

/// PATH for a child: combines the exec dir, the current PATH, and the login-shell PATH. 
fn compose_path(exec_path: &Path, own: Option<&str>, login: Option<&str>) -> String {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let dirs = exec_path
        .parent()
        .map(|d| d.to_string_lossy().into_owned())
        .into_iter()
        .chain(split_path(own))
        .chain(split_path(login));
    for dir in dirs {
        if !dir.is_empty() && seen.insert(dir.clone()) {
            out.push(dir);
        }
    }
    out.join(":")
}

fn split_path(path: Option<&str>) -> impl Iterator<Item = String> + '_ {
    path.unwrap_or_default().split(':').map(str::to_owned)
}

/// Get users shell path once and cache for future calls.
pub(crate) async fn login_shell_path() -> Option<String> {
    static CACHE: OnceCell<Option<String>> = OnceCell::const_new();
    CACHE.get_or_init(capture_login_shell_path).await.clone()
}

/// Runs `$SHELL -lic 'echo $PATH'` (fallback `-lc`).
async fn capture_login_shell_path() -> Option<String> {
    if std::env::var("ANYAGENT_NO_LOGIN_SHELL").is_ok_and(|v| v == "1") {
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

/// Get login shell path by running `$SHELL -lic 'echo $PATH'` (fallback `-lc`).
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

    fn sh(script: &str) -> Spawn {
        Spawn {
            exec_path: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), script.into()],
            cwd: std::env::temp_dir(),
            env: Vec::new(),
        }
    }

    #[test]
    fn compose_path_orders_and_dedupes() {
        let path = compose_path(
            Path::new("/opt/agent/bin/claude"),
            Some("/usr/bin:/opt/agent/bin"),
            Some("/usr/bin:/home/u/.volta/bin"),
        );
        assert_eq!(path, "/opt/agent/bin:/usr/bin:/home/u/.volta/bin");
    }

    #[tokio::test]
    async fn shutdown_escalates_to_sigkill_within_grace() {
        let mut child = spawn(sh("trap '' TERM; sleep 30")).await.unwrap();
        let start = std::time::Instant::now();
        child.shutdown(Duration::from_millis(200)).await;
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn shutdown_lets_a_cooperative_child_exit_on_sigterm() {
        let mut child = spawn(sh("sleep 30")).await.unwrap();
        child.shutdown(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn stderr_tail_keeps_the_last_lines() {
        let mut child = spawn(sh("for i in 1 2 3 4 5 6 7 8; do echo line$i 1>&2; done"))
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
