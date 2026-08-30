//! Discover and probe: what agents are on this machine, and what can they do?
//!
//! ```sh
//! cargo run --example probe
//! ```
//!
//! `discover()` is read-only and instant — it finds executables and login
//! markers without launching anything. `probe(agent)` opens a throwaway
//! session (~1 s) to learn what only the agent itself can tell you: its real
//! login state, version, capabilities, config options, and slash commands.
//! Apps typically discover at startup and probe on demand (a settings page,
//! an agent picker).

use anyagent::{AgentInstallation, AuthStatus, Capability, ConfigKind, Runtime};

#[tokio::main]
async fn main() {
    let runtime = Runtime::new();
    let report = runtime.discover().await;

    // Each probe spawns one short-lived agent process; run them concurrently.
    let probes = report.agents.iter().map(|agent| describe(&runtime, agent));
    for line in futures::future::join_all(probes).await {
        println!("{line}");
    }

    // Agents anyagent supports but did not find, with how to install them.
    for missing in &report.missing {
        println!(
            "{:<10} not installed — {}",
            missing.id, missing.install_hint
        );
    }
}

/// One agent, one line: login state, version, and what it advertises.
async fn describe(runtime: &Runtime, agent: &AgentInstallation) -> String {
    let details = match runtime.probe(agent).await {
        Ok(details) => details,
        Err(e) => return format!("{:<10} probe failed: {e}", agent.id),
    };

    // `AuthStatus` is the real login state, read from the agent itself —
    // including who is logged in, when the wire says.
    let auth = match &details.auth {
        AuthStatus::Authenticated { kind, account } => {
            let who = account
                .as_ref()
                .and_then(|a| a.email.as_deref().or(a.plan.as_deref()))
                .unwrap_or("logged in");
            format!("{kind:?}: {who}")
        }
        // `login` carries runnable commands your app can show the user.
        AuthStatus::Unauthenticated { login } => format!("logged out ({} ways in)", login.len()),
        _ => "unknown".into(),
    };

    // Config options are how models, modes, and effort are exposed: every
    // agent advertises its own list, and your picker reads the choices.
    let models = details
        .config_options
        .iter()
        .find(|o| o.id.as_str() == "model")
        .map_or(0, |o| match &o.kind {
            ConfigKind::Select { choices } => choices.len(),
            _ => 0,
        });

    // Capabilities are how an app gates its UI: only show a fork button,
    // steer box, or rollback control when the agent supports it.
    let caps: Vec<&str> = [
        (Capability::Steer, "steer"),
        (Capability::Fork, "fork"),
        (Capability::Rollback, "rollback"),
        (Capability::PlanUsage, "plan-usage"),
    ]
    .into_iter()
    .filter(|(cap, _)| details.capabilities.supports(cap.clone()))
    .map(|(_, name)| name)
    .collect();

    format!(
        "{:<10} v{:<10} {auth:<28} {models} models · {} commands · {}",
        agent.id,
        details.version.as_deref().unwrap_or("?"),
        details.commands.len(),
        caps.join(", "),
    )
}
