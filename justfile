# anyagent dev commands — run `just` to list them.

# List recipes.
default:
    @just --list --unsorted

# Fast offline checks: format, lint, unit + fixture tests.
check:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo test

# List the live feature tests (each is one feature, run via `just live`).
features:
    @grep -A2 '#\[ignore' tests/live.rs | grep -oE 'async fn [a-z_]+' | cut -d' ' -f3

# harness: claude|codex|opencode|hermes|kiro|pi|all — feature: substring from `just features`, empty = all
live harness feature='':
    ANYAGENT_LIVE={{harness}} cargo test --test live {{feature}} -- --ignored --nocapture --test-threads=1

# Discover installed agents and probe what each can do.
probe:
    cargo run --example probe

# Interactive chat with one agent.
chat harness='claude':
    cargo run --example chat -- {{harness}}

# Several concurrent sessions, close and resume.
sessions harness='claude':
    cargo run --example sessions -- {{harness}}
