//! Login/logout detection check: for every agent in the catalog, print what
//! discovery guessed offline (the credential marker) next to what a real
//! `probe` open reports, so the two can be compared at a glance.
//!
//!   cargo run --example probe                    logged-in view
//!   HOME=$(mktemp -d) cargo run --example probe  logged-out view
//!
//! The logged-out run redirects every agent's config home under the temp
//! HOME, so real credential files are never read or written.

use anyagent::{AgentInstallation, AuthStatus, Runtime};

#[tokio::main]
async fn main() {
    let runtime = Runtime::new();
    let report = runtime.discover().await;

    // Probes spawn one agent process each; run them together.
    let probes = report.agents.iter().map(|agent| row(&runtime, agent));
    let mut rows = futures::future::join_all(probes).await;
    rows.extend(report.missing.iter().map(|m| Row {
        agent: m.id.as_str().to_owned(),
        marker: "-".into(),
        probe: "-".into(),
        verdict: "not installed".into(),
    }));

    print_table(&rows);
    println!("\nHOME = {}", std::env::var("HOME").unwrap_or_default());
    for d in &report.diagnostics {
        println!("note: {}", d.message);
    }
}

/// One agent's offline marker vs. its live probe, plus whether they agree.
async fn row(runtime: &Runtime, agent: &AgentInstallation) -> Row {
    let marker = describe(agent.auth.as_ref());
    let (probe, probed) = match runtime.probe(agent).await {
        Ok(details) => (describe(Some(&details.auth)), Some(details.auth)),
        Err(e) => (format!("failed: {e}"), None),
    };
    Row {
        agent: agent.id.as_str().to_owned(),
        marker,
        probe,
        verdict: verdict(agent.auth.as_ref(), probed.as_ref()),
    }
}

/// The probe is the truth; the marker is the offline guess. Says which of the
/// two moved, so a wrong catalog marker is visible.
fn verdict(marker: Option<&AuthStatus>, probe: Option<&AuthStatus>) -> String {
    let Some(probe) = probe else {
        return "probe failed".into();
    };
    match (marker, probe) {
        (Some(m), p) if m == p => "agree".into(),
        (None | Some(AuthStatus::Unknown), AuthStatus::Authenticated { .. }) => {
            "probe found login".into()
        }
        (None | Some(AuthStatus::Unknown), _) => "probe answered".into(),
        (Some(AuthStatus::Authenticated { .. }), AuthStatus::Unauthenticated { .. }) => {
            "MARKER WRONG: says in, is out".into()
        }
        (Some(AuthStatus::Unauthenticated { .. }), AuthStatus::Authenticated { .. }) => {
            "MARKER WRONG: says out, is in".into()
        }
        _ => "differs".into(),
    }
}

/// One-line auth summary: in/out plus the kind and account when known.
fn describe(auth: Option<&AuthStatus>) -> String {
    match auth {
        None => "unknown (no marker)".into(),
        Some(AuthStatus::Unknown) => "unknown".into(),
        Some(AuthStatus::Unauthenticated { login }) => {
            format!("out ({} login methods)", login.len())
        }
        Some(AuthStatus::Authenticated { kind, account }) => {
            let who = account
                .as_ref()
                .and_then(|a| a.email.clone().or_else(|| a.plan.clone()))
                .map(|w| format!(", {w}"))
                .unwrap_or_default();
            format!("in ({kind:?}{who})")
        }
        Some(_) => "other".into(),
    }
}

struct Row {
    agent: String,
    marker: String,
    probe: String,
    verdict: String,
}

/// Prints the rows as a table, each column sized to its widest cell.
fn print_table(rows: &[Row]) {
    fn cells(r: &Row) -> [&str; 4] {
        [&r.agent, &r.marker, &r.probe, &r.verdict]
    }
    let headers = ["AGENT", "MARKER (offline)", "PROBE (live open)", "VERDICT"];
    let mut widths = headers.map(str::len);
    for row in rows {
        for (w, cell) in widths.iter_mut().zip(cells(row)) {
            *w = (*w).max(cell.chars().count());
        }
    }
    let line = |values: [&str; 4]| {
        let padded: Vec<String> = values
            .iter()
            .zip(widths)
            .map(|(v, w)| format!("{v:<w$}"))
            .collect();
        println!("{}", padded.join("  ").trim_end());
    };
    line(headers);
    line(widths.map(|w| "-".repeat(w)).each_ref().map(String::as_str));
    for row in rows {
        line(cells(row));
    }
}
