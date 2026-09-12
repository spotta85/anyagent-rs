# Examples

Small, commented programs showing how to build on anyagent. Each one runs
against the real agents installed on your machine.

| Example | Shows | Run |
|---|---|---|
| [`sessions.rs`](sessions.rs) | Several sessions at once, driven concurrently, then closing one and resuming it by token. | `cargo run --example sessions -- claude` |
| [`schema.rs`](schema.rs) | Prints the wire JSON schema for every type the sidecar sends; what `just schema` writes to `packages/schema.json`. | `cargo run --example schema --features schema` |

The core loop lives in the binary now: `anyagent chat` (src/bin/anyagent.rs),
with the short version in the crate docs (src/lib.rs). `anyagent list` replaced
`probe.rs`.
