//! Sans-IO protocol state machine behind
//! [`centrifuge-client`](https://docs.rs/centrifuge-client).
//!
//! **Use `centrifuge-client`, not this crate.** It does no IO and has no
//! runtime or clock, so it builds for every target, including
//! `wasm32-unknown-unknown`. Its API outside what `centrifuge-client`
//! re-exports has no stability guarantees.
//!
//! The design is in
//! [`docs/spec.md` §9](https://github.com/centrifugal/centrifuge-rust/blob/main/docs/spec.md).

/// Version of this crate, as published on crates.io.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
