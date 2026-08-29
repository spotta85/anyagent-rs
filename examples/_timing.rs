use anyagent::Runtime;
#[tokio::main]
async fn main() {
    let rt = Runtime::new();
    for a in rt.discover().await.agents {
        let t = std::time::Instant::now();
        let n = rt.probe(&a).await.map(|d| d.commands.len());
        println!("{:10} {:>16?}  cmds={:?}", a.id.as_str(), t.elapsed(), n.ok());
    }
}
