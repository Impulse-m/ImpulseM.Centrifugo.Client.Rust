//! Ping watchdog of the connection: a silent connection is closed and reconnects.
//!
//! The connect reply carries the server's ping interval. The window of silence
//! allowed is that interval plus `max_server_ping_delay`. The sans-IO machine of
//! the core arms a no-ping timer for the window, re-arms it on every item
//! received (a command reply, a push or a server ping), and when it fires closes
//! the transport with code 2 (`no ping`) and reconnects. A server ping is
//! answered with a pong when the server asks for one.
//!
//! The watchdog has no code of its own here: the timer and its window are in
//! the core, and the behaviour is tested against a silent WebSocket server in
//! `tests/ping.rs`.
