# centrifuge-client-core

The sans-IO protocol state machine behind [`centrifuge-client`](https://crates.io/crates/centrifuge-client), the Rust real-time SDK for [Centrifugo](https://centrifugal.dev).

**Use `centrifuge-client`, not this crate.** It's published only because `centrifuge-client` depends on it. It's released in lockstep with the client, and its API outside what the client re-exports has no stability guarantees.
