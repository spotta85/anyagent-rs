//! Line-delimited JSON over a child's stdio, shared by the stdio adapters
//! (claude, codex, acp, pi), plus the optional raw-frame recorder.
//!
//! High level: `LineWire::over` takes the child's pipes and starts the
//! reader task; `write` sends one frame; `frames` receives them.

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::adapter::Emitter;
use crate::agent::SessionOptions;
use crate::event::DiagnosticLevel;
use crate::process::{Child, SharedStdin};

/// Frames buffered between the reader task and the drive task.
pub(crate) const FRAME_BUFFER: usize = 64;

/// One JSON object per line each way. Adapters add their own request ids
/// and response matching on top.
pub(crate) struct LineWire {
    /// Shared with the child so `shutdown` can close it (EOF).
    stdin: SharedStdin,
    /// Every frame the reader task parsed, in order; closes at stdout EOF.
    pub frames: mpsc::Receiver<Value>,
    recorder: Option<WireRecorder>,
}

impl LineWire {
    /// Takes the child's stdio and starts the line-reader task. Unparseable
    /// lines are skipped.
    pub(crate) fn over(child: &mut Child, recorder: Option<WireRecorder>) -> Self {
        let stdin = child.stdin.clone();
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, frames) = mpsc::channel(FRAME_BUFFER);
        let reader_recorder = recorder.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(recorder) = &reader_recorder {
                    recorder.record("in", &frame);
                }
                if tx.send(frame).await.is_err() {
                    break;
                }
            }
        });
        Self {
            stdin,
            frames,
            recorder,
        }
    }

    /// Writes one frame as a line, recording it first. Fails with
    /// `BrokenPipe` once shutdown has closed stdin.
    pub(crate) async fn write(&mut self, frame: Value) -> std::io::Result<()> {
        if let Some(recorder) = &self.recorder {
            recorder.record("out", &frame);
        }
        let mut line = frame.to_string();
        line.push('\n');
        match self.stdin.lock().await.as_mut() {
            Some(stdin) => stdin.write_all(line.as_bytes()).await,
            None => Err(std::io::ErrorKind::BrokenPipe.into()),
        }
    }
}

/// Tees raw protocol frames to a JSONL file when `record_wire` is set: one
/// `{"dir":"in"|"out","frame":<frame>}` per line, append-only and flushed
/// per line. Unredacted, unbounded: a local debug artifact. A write failure
/// is reported once as a `Diagnostic`; recording never fails a turn.
#[derive(Clone)]
pub(crate) struct WireRecorder {
    lines: mpsc::UnboundedSender<Vec<u8>>,
}

impl WireRecorder {
    /// The session's recorder when `record_wire` is set; `None` otherwise.
    /// An open failure is one diagnostic and recording stays off.
    pub(crate) async fn for_session(options: &SessionOptions, events: &Emitter) -> Option<Self> {
        let path = options.record_wire.as_deref()?;
        let events = events.clone();
        let file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                let _ = events
                    .diagnostic(
                        DiagnosticLevel::Warning,
                        format!("wire recording is off: {e}"),
                    )
                    .await;
                return None;
            }
        };
        let (lines, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            let mut file = file;
            while let Some(bytes) = rx.recv().await {
                if let Err(e) = append(&mut file, &bytes).await {
                    let _ = events
                        .diagnostic(
                            DiagnosticLevel::Warning,
                            format!("wire recording stopped: {e}"),
                        )
                        .await;
                    break;
                }
            }
        });
        Some(Self { lines })
    }

    /// Records one frame in the given direction. Never blocks or errors; a
    /// gone writer just loses the frame.
    pub(crate) fn record(&self, dir: &'static str, frame: &Value) {
        let mut line = json!({ "dir": dir, "frame": frame }).to_string();
        line.push('\n');
        let _ = self.lines.send(line.into_bytes());
    }
}

/// Appends and flushes one line.
async fn append(file: &mut tokio::fs::File, bytes: &[u8]) -> std::io::Result<()> {
    file.write_all(bytes).await?;
    file.flush().await
}
