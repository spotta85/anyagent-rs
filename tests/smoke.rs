#[tokio::test]
async fn discover_smoke() {
    let report = anyagent::Runtime::new().discover().await;
    for a in &report.agents {
        println!(
            "FOUND {} at {} ({:?})",
            a.id,
            a.executable.display(),
            a.source
        );
    }
    for m in &report.missing {
        println!("MISSING {}", m.id);
    }
}
