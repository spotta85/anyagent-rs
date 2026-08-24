//! anyagent — one Rust interface to the coding agents installed on a machine.
//!
//! ```text
//! Runtime::discover() -> Runtime::open() -> (Session, Events)
//! ```
//!
//! See `PLAN.md` for the full interface contract.

mod adapter;
mod agent;
mod catalog;
mod discovery;
mod error;
mod event;
mod process;
mod runtime;
mod session;

pub use agent::*;
pub use error::AgentError;
pub use event::*;
pub use runtime::{DiscoveryReport, MissingAgent, Runtime};
pub use session::{Events, Session, SessionInfo};
