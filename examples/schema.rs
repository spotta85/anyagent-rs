//! Prints the wire's JSON schema.
//!
//! ```sh
//! cargo run -q --example schema --features schema > packages/schema.json
//! ```

fn main() {
    println!(
        "{}",
        serde_json::to_string_pretty(&anyagent::sidecar::schema()).unwrap()
    );
}
