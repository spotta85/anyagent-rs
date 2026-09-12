//! The built binary over real pipes: `anyagent serve --mock` end to end,
//! and the usage exit.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use serde_json::{Value, json};

const SCRIPT: &str = r#"{"turns": [[
  {"Emit": {"TextDelta": {"message_id": "m1", "text": "hi"}}},
  {"End": {"Completed": {"source": "Protocol"}}}
]]}"#;

/// A running `anyagent serve --mock`: stdin to write, stdout frames on a
/// channel so a hang fails at the timeout instead of forever.
struct Bin {
    child: Child,
    stdin: Option<ChildStdin>,
    frames: Receiver<Value>,
}

impl Bin {
    fn serve_mock(dir: &Path) -> Self {
        let script = dir.join("script.json");
        std::fs::write(&script, SCRIPT).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_anyagent"))
            .args(["serve", "--mock"])
            .arg(&script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let (tx, frames) = mpsc::channel();
        std::thread::spawn(move || {
            for line in stdout.lines().map_while(Result::ok) {
                let frame = serde_json::from_str(&line).expect("json line");
                if tx.send(frame).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            frames,
        }
    }

    fn send(&mut self, frame: Value) {
        writeln!(self.stdin.as_mut().expect("stdin open"), "{frame}").unwrap();
    }

    /// Frames until `pred` matches, or a panic naming `step`.
    fn until(&self, step: &str, pred: impl Fn(&Value) -> bool) -> Value {
        loop {
            let frame = self
                .frames
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| panic!("no frame for {step}"));
            if pred(&frame) {
                return frame;
            }
        }
    }
}

/// Hello, open, prompt, turn end, EOF, closed, exit 0: the sidecar's
/// whole life over the binary's stdin and stdout.
#[test]
fn serve_mock_round_trips_over_stdio() {
    let dir = tempfile::tempdir().unwrap();
    let mut bin = Bin::serve_mock(dir.path());
    let hello = bin.until("hello", |f| f.get("hello").is_some());
    assert_eq!(hello["hello"]["protocol"], 1, "{hello}");

    bin.send(json!({"id": 1, "cmd": "open", "agent": "mock", "dir": dir.path()}));
    let reply = bin.until("open reply", |f| f["id"] == 1);
    let session = reply["ok"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("open failed: {reply}"))
        .to_owned();

    bin.send(json!({"id": 2, "cmd": "prompt", "session": session, "text": "go"}));
    let ended = bin.until("turn end", |f| {
        f["event"]["kind"].get("TurnEnded").is_some()
    });
    assert_eq!(ended["event"]["session_id"], session);

    bin.stdin = None;
    let closed = bin.until("closed", |f| f.get("closed").is_some());
    assert_eq!(closed["closed"], session);
    let status = bin.child.wait().unwrap();
    assert!(status.success(), "{status}");
}

#[test]
fn no_subcommand_prints_usage_and_exits_2() {
    let out = Command::new(env!("CARGO_BIN_EXE_anyagent"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));
}
