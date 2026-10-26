# centrifuge-rust

[![crates.io](https://img.shields.io/crates/v/centrifuge-client.svg)](https://crates.io/crates/centrifuge-client)
[![docs.rs](https://img.shields.io/docsrs/centrifuge-client)](https://docs.rs/centrifuge-client)
[![CI](https://github.com/centrifugal/centrifuge-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/centrifugal/centrifuge-rust/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/crates/msrv/centrifuge-client)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Real-time client SDK in Rust for [Centrifugo](https://centrifugal.dev) and the [Centrifuge](https://github.com/centrifugal/centrifuge) library.

> **Status: under development.** The crate is published to reserve its name and is not usable yet. The design is in [`docs/spec.md`](docs/spec.md).

```toml
[dependencies]
centrifuge-client = "0.0.1"
```

```rust
use centrifuge::VERSION;
```

The package is `centrifuge-client`; the library is imported as `centrifuge`.

## Planned features

WebSocket with HTTP-streaming fallback, JSON and Protobuf, automatic reconnect, token refresh, subscriptions with recovery, server-side subscriptions, history, presence, RPC, proxies (HTTP CONNECT, SOCKS5, system settings) and OS trust store TLS. See the [spec](docs/spec.md) for the full scope and phases.

## Minimum supported Rust version

Rust 1.85. Raising it is a minor-version change and is noted in the [changelog](CHANGELOG.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
