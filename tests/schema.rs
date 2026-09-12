//! The checked-in `packages/schema.json` is what the crate generates, so
//! every wrapper's types come from the same file the Rust types produce.
//! Regenerate: `cargo run --example schema --features schema > packages/schema.json`.
#![cfg(feature = "schema")]

#[test]
fn the_checked_in_schema_is_current() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/packages/schema.json");
    let checked_in: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let generated = serde_json::to_value(anyagent::sidecar::schema()).unwrap();
    assert!(
        checked_in == generated,
        "packages/schema.json is stale: cargo run --example schema --features schema > packages/schema.json"
    );
}
