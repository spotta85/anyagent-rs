//! Temporary: opens one agent, records its wire, prints the selected model
//! and its config options. `cargo run --example wire_dump -- <agent-id>`.

use anyagent::{ConfigKind, Runtime, SessionOptions};

#[tokio::main]
async fn main() {
    let id = std::env::args()
        .nth(1)
        .expect("usage: wire_dump <agent-id>");
    let dir = std::env::temp_dir().join("anyagent-wire-dump");
    std::fs::create_dir_all(&dir).unwrap();
    let log = dir.join(format!("{id}.jsonl"));
    let _ = std::fs::remove_file(&log);

    let runtime = Runtime::new();
    let report = runtime.discover().await;
    let agent = report.require(&id).expect("agent not installed");
    let mut options = SessionOptions::in_dir(&dir).record_wire(&log);
    if let Some(model) = std::env::args().nth(2) {
        println!("configuring model = {model}");
        options = options.configure("model", model);
    }
    let (session, _events) = runtime.open(agent, options).await.expect("open failed");

    let info = session.info();
    println!("version: {:?}", info.details.version);
    for option in &info.details.config_options {
        let current = info.configuration.options.get(&option.id);
        match &option.kind {
            ConfigKind::Select { choices } => println!(
                "option {} = {current:?} choices={:?}",
                option.id.as_str(),
                choices.iter().map(|c| c.value.as_str()).collect::<Vec<_>>()
            ),
            other => println!("option {} = {current:?} kind={other:?}", option.id.as_str()),
        }
    }
    session.close().await.ok();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    println!("--- wire log: {} ---", log.display());
}
