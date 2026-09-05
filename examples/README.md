# Examples

Small, commented programs showing how to build on anyagent. Each one runs
against the real agents installed on your machine.

| Example | Shows | Run |
|---|---|---|
| [`chat.rs`](chat.rs) | The core loop: open a session, prompt, stream events, answer permission requests. Typing mid-turn steers the running turn; `/set effort low` changes a live setting. | `cargo run --example chat -- claude` |
| [`sessions.rs`](sessions.rs) | Several sessions at once, driven concurrently, then closing one and resuming it by token. | `cargo run --example sessions -- claude` |
| [`probe.rs`](probe.rs) | Discover every installed agent, then probe each: login state, version, models, commands, capabilities. | `cargo run --example probe` |

Start with `chat.rs` — it is the pattern every app builds on.
