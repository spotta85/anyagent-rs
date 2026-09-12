//! The JSONL sidecar over the mock agent: one in-memory pipe each way, no
//! subprocess. Every guarantee in ticket 13 (G1–G7) has one test here.

use std::time::Duration;

use anyagent::mock::{Script, Step, completed, permission, text};
use anyagent::{Runtime, StopReason};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::task::JoinHandle;

/// A client end of a running sidecar: write command lines, read frames.
struct Wire {
    input: Option<DuplexStream>,
    output: tokio::io::Lines<BufReader<DuplexStream>>,
    serve: JoinHandle<std::io::Result<()>>,
    dir: tempfile::TempDir,
}

impl Wire {
    /// Starts `serve` over `script` and consumes the hello line.
    async fn start(script: Script) -> Self {
        let (client_in, serve_in) = tokio::io::duplex(64 * 1024);
        let (serve_out, client_out) = tokio::io::duplex(64 * 1024);
        let runtime = Runtime::with_mock(script);
        let serve = tokio::spawn(anyagent::sidecar::serve(
            runtime,
            BufReader::new(serve_in),
            serve_out,
        ));
        let mut wire = Self {
            input: Some(client_in),
            output: BufReader::new(client_out).lines(),
            serve,
            dir: tempfile::tempdir().unwrap(),
        };
        let hello = wire.next("hello").await;
        assert_eq!(
            hello["hello"]["protocol"], 1,
            "first line is hello: {hello}"
        );
        wire
    }

    fn dir(&self) -> String {
        self.dir.path().display().to_string()
    }

    /// Writes one raw line.
    async fn send_raw(&mut self, line: &str) {
        let input = self.input.as_mut().expect("input still open");
        input.write_all(line.as_bytes()).await.unwrap();
        input.write_all(b"\n").await.unwrap();
    }

    async fn send(&mut self, frame: Value) {
        self.send_raw(&frame.to_string()).await;
    }

    /// The next frame, or a panic naming `step`.
    async fn next(&mut self, step: &str) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), self.output.next_line())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {step}"))
            .unwrap()
            .unwrap_or_else(|| panic!("output ended before {step}"));
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("{step}: bad json {line}: {e}"))
    }

    /// Reads frames until `pred` matches; returns the frames read before it
    /// and the match.
    async fn until(&mut self, step: &str, pred: impl Fn(&Value) -> bool) -> (Vec<Value>, Value) {
        let mut before = Vec::new();
        loop {
            let frame = self.next(step).await;
            if pred(&frame) {
                return (before, frame);
            }
            before.push(frame);
        }
    }

    /// The reply to `id`, skipping event frames in between.
    async fn reply(&mut self, id: u64) -> Value {
        self.until(&format!("reply {id}"), |f| f["id"] == id)
            .await
            .1
    }

    /// Opens a mock session and returns its id.
    async fn open(&mut self, id: u64) -> String {
        self.send(json!({"id": id, "cmd": "open", "agent": "mock", "dir": self.dir()}))
            .await;
        let reply = self.reply(id).await;
        reply["ok"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("open failed: {reply}"))
            .to_owned()
    }

    /// Closes the client's input: EOF for the sidecar.
    fn hang_up(&mut self) {
        self.input = None;
    }
}

fn kind_name(frame: &Value) -> Option<&str> {
    let kind = frame.get("event")?.get("kind")?;
    kind.as_str()
        .or_else(|| kind.as_object()?.keys().next().map(String::as_str))
}

fn one_turn() -> Script {
    Script::default().turn(vec![
        Step::Emit(text("m1", "Let me check. ")),
        Step::Emit(permission("r1")),
        Step::AwaitAnswer,
        Step::Emit(text("m1", "Done.")),
        Step::End(completed()),
    ])
}

/// `discover` lists the mock agent; the reply carries the request id.
#[tokio::test]
async fn discover_lists_the_mock_agent() {
    let mut wire = Wire::start(Script::default()).await;
    wire.send(json!({"id": 7, "cmd": "discover"})).await;
    let reply = wire.next("discover reply").await;
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["ok"]["agents"][0]["id"], "mock", "{reply}");
}

/// Open, prompt, answer the permission, see the turn end, close: every
/// frame shape on the happy path, in order.
#[tokio::test]
async fn open_prompt_answer_close_round_trip() {
    let mut wire = Wire::start(one_turn()).await;
    let session = wire.open(1).await;

    wire.send(json!({"id": 2, "cmd": "prompt", "session": session, "text": "hi"}))
        .await;
    let delivery = wire.reply(2).await;
    assert!(
        delivery["ok"]["kind"].get("Started").is_some(),
        "{delivery}"
    );

    let (_, request) = wire
        .until("permission request", |f| {
            kind_name(f) == Some("RequestOpened")
        })
        .await;
    assert_eq!(request["event"]["session_id"], session);
    let request_id = request["event"]["kind"]["RequestOpened"]["Permission"]["id"].clone();

    wire.send(json!({"id": 3, "cmd": "answer", "session": session,
        "request": request_id, "answer": {"Permission": "AllowOnce"}}))
        .await;
    assert_eq!(wire.reply(3).await["ok"], Value::Null);

    let (before, _) = wire
        .until("turn end", |f| kind_name(f) == Some("TurnEnded"))
        .await;
    let texts: Vec<&str> = before
        .iter()
        .filter(|f| kind_name(f) == Some("TextDelta"))
        .map(|f| f["event"]["kind"]["TextDelta"]["text"].as_str().unwrap())
        .collect();
    assert_eq!(texts, ["Done."], "text after the answer only: {before:?}");

    wire.send(json!({"id": 4, "cmd": "close", "session": session}))
        .await;
    let (_, closed) = wire.until("closed", |f| f.get("closed").is_some()).await;
    assert_eq!(closed["closed"], session);
}

/// G1: an `open` followed at once by a `prompt` on the same connection
/// works, because the sidecar writes the open reply before anything else
/// about that session and resolves the second command after the first.
#[tokio::test]
async fn open_reply_precedes_every_frame_for_that_session() {
    let mut wire = Wire::start(one_turn()).await;
    let session = wire.open(1).await;
    wire.send(json!({"id": 2, "cmd": "prompt", "session": session, "text": "hi"}))
        .await;
    let (before, _) = wire
        .until("first event", |f| f.get("event").is_some())
        .await;
    assert!(
        before.iter().all(|f| f.get("event").is_none()),
        "no event before the open reply: {before:?}"
    );
}

/// G2: two sessions interleave on one output, each in its own sequence
/// order and only under its own id. Every session plays the same script.
#[tokio::test]
async fn frames_carry_their_session_and_stay_ordered() {
    let script = Script::default().turn(vec![
        Step::Emit(text("m", "one")),
        Step::Emit(text("m", "two")),
        Step::End(completed()),
    ]);
    let mut wire = Wire::start(script).await;
    let a = wire.open(1).await;
    let b = wire.open(2).await;
    assert_ne!(a, b);
    wire.send(json!({"id": 3, "cmd": "prompt", "session": a, "text": "x"}))
        .await;
    wire.send(json!({"id": 4, "cmd": "prompt", "session": b, "text": "y"}))
        .await;
    let mut seen = 0;
    let mut last: std::collections::HashMap<String, u64> = Default::default();
    while seen < 2 {
        let frame = wire.next("events").await;
        let Some(event) = frame.get("event") else {
            continue;
        };
        let id = event["session_id"].as_str().unwrap().to_owned();
        let seq = event["sequence"].as_u64().unwrap();
        assert!(
            seq > last.get(&id).copied().unwrap_or(0),
            "out of order: {frame}"
        );
        last.insert(id, seq);
        if kind_name(&frame) == Some("TurnEnded") {
            seen += 1;
        }
    }
    assert_eq!(last.len(), 2, "both sessions produced events");
}

/// G3 + G4: the agent dying mid-turn arrives as a session error carrying
/// the exit status, then `closed`, exactly once.
#[tokio::test]
async fn a_dying_agent_is_a_session_error_then_closed() {
    let script = Script::default().turn(vec![Step::Emit(text("m1", "hi")), Step::Die]);
    let mut wire = Wire::start(script).await;
    let session = wire.open(1).await;
    wire.send(json!({"id": 2, "cmd": "prompt", "session": session, "text": "go"}))
        .await;
    let (_, error) = wire
        .until("session error", |f| f.get("session").is_some())
        .await;
    assert_eq!(error["session"], session);
    assert_eq!(error["error"]["kind"], "ProcessExited", "{error}");
    assert_eq!(error["error"]["status"], "9", "{error}");
    // The engine's own bookkeeping (a final `StatusChanged`) may still
    // follow the error; `closed` comes after it.
    let (trailing, closed) = wire.until("closed", |f| f.get("closed").is_some()).await;
    assert_eq!(closed["closed"], session, "{closed}");
    assert!(
        trailing.iter().all(|f| f.get("event").is_some()),
        "{trailing:?}"
    );
}

/// G5: hanging up closes every open session, each says `closed`, and
/// `serve` returns.
#[tokio::test]
async fn eof_closes_every_open_session_and_returns() {
    let mut wire = Wire::start(Script::default()).await;
    let a = wire.open(1).await;
    let b = wire.open(2).await;
    wire.hang_up();
    let (_, first) = wire
        .until("first closed", |f| f.get("closed").is_some())
        .await;
    let (_, second) = wire
        .until("second closed", |f| f.get("closed").is_some())
        .await;
    let mut ids = vec![first["closed"].clone(), second["closed"].clone()];
    ids.sort_by_key(Value::to_string);
    let mut want = vec![Value::from(a), Value::from(b)];
    want.sort_by_key(Value::to_string);
    assert_eq!(ids, want);
    assert!(
        wire.output.next_line().await.unwrap().is_none(),
        "output ends"
    );
    tokio::time::timeout(Duration::from_secs(5), wire.serve)
        .await
        .expect("serve returns")
        .unwrap()
        .unwrap();
}

/// G6: a client that stops reading loses only the session it starved,
/// the way the crate closes a lagging consumer. The sidecar survives.
#[tokio::test]
async fn a_starved_session_is_closed_and_the_sidecar_survives() {
    let flood: Vec<Step> = (0..3000)
        .map(|i| Step::Emit(text("m", &i.to_string())))
        .collect();
    let script = Script::default()
        .turn(flood)
        .turn(vec![Step::End(completed())]);
    let mut wire = Wire::start(script).await;
    let session = wire.open(1).await;
    wire.send(json!({"id": 2, "cmd": "prompt", "session": session, "text": "flood"}))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_, closed) = wire.until("closed", |f| f.get("closed").is_some()).await;
    assert_eq!(closed["closed"], session);
    wire.send(json!({"id": 3, "cmd": "discover"})).await;
    assert_eq!(wire.reply(3).await["ok"]["agents"][0]["id"], "mock");
}

/// G7: a line that is not a command gets `BadFrame` and the loop goes on.
#[tokio::test]
async fn a_malformed_line_gets_bad_frame_and_the_loop_continues() {
    let mut wire = Wire::start(Script::default()).await;
    wire.send_raw("not json").await;
    let bad = wire.next("bad frame").await;
    assert_eq!(bad["id"], Value::Null);
    assert_eq!(bad["error"]["kind"], "BadFrame", "{bad}");
    wire.send(json!({"id": 1, "cmd": "discover"})).await;
    assert_eq!(wire.reply(1).await["ok"]["agents"][0]["id"], "mock");
}

/// `configure` names its option `option` on the wire; the mock has no such
/// option, so the crate's typed refusal comes back, not a frame error.
#[tokio::test]
async fn configure_uses_the_option_field() {
    let mut wire = Wire::start(Script::default()).await;
    let session = wire.open(1).await;
    wire.send(json!({"id": 2, "cmd": "configure", "session": session,
        "option": "model", "value": "sonnet"}))
        .await;
    let reply = wire.reply(2).await;
    assert_eq!(reply["error"]["kind"], "InvalidConfiguration", "{reply}");
}

/// Error bodies keep the variant's data next to `kind` and `message`.
#[tokio::test]
async fn error_bodies_keep_their_fields() {
    let mut wire = Wire::start(Script::default()).await;
    wire.send(json!({"id": 1, "cmd": "open", "agent": "nope", "dir": wire.dir()}))
        .await;
    let missing = wire.reply(1).await["error"].clone();
    assert_eq!(missing["kind"], "NotInstalled");
    assert_eq!(missing["agent"], "nope", "{missing}");
    assert!(missing["message"].as_str().unwrap().contains("nope"));

    wire.send(json!({"id": 2, "cmd": "prompt", "session": "ghost", "text": "x"}))
        .await;
    let unknown = wire.reply(2).await["error"].clone();
    assert_eq!(unknown["kind"], "UnknownSession");
    assert_eq!(unknown["session"], "ghost", "{unknown}");
}

/// A prompt after close is the crate's `SessionClosed`, so wrappers can
/// tell "closed" from "never existed".
#[tokio::test]
async fn a_prompt_after_close_is_session_closed() {
    let mut wire = Wire::start(Script::default()).await;
    let session = wire.open(1).await;
    wire.send(json!({"id": 2, "cmd": "close", "session": session}))
        .await;
    wire.until("closed", |f| f.get("closed").is_some()).await;
    wire.send(json!({"id": 3, "cmd": "prompt", "session": session, "text": "x"}))
        .await;
    let reply = wire.reply(3).await;
    assert_eq!(reply["error"]["kind"], "SessionClosed", "{reply}");
}

/// The cancelled stop reason rides `TurnEnded` unchanged, proving events
/// are the crate's own serialization.
#[tokio::test]
async fn events_are_the_crates_serde_output() {
    let script = Script::default().turn(vec![
        Step::Emit(text("m1", "a")),
        Step::End(StopReason::Cancelled),
    ]);
    let mut wire = Wire::start(script).await;
    let session = wire.open(1).await;
    wire.send(json!({"id": 2, "cmd": "prompt", "session": session, "text": "x"}))
        .await;
    let (_, ended) = wire
        .until("turn end", |f| kind_name(f) == Some("TurnEnded"))
        .await;
    assert_eq!(
        ended["event"]["kind"]["TurnEnded"]["stop"], "Cancelled",
        "{ended}"
    );
}
