//! Real-time client SDK for [Centrifugo](https://centrifugal.dev) and the
//! [Centrifuge](https://github.com/centrifugal/centrifuge) library.
//!
//! WebSocket and HTTP-streaming transports with fallback, client-side and
//! server-side subscriptions, automatic reconnect and resubscribe, recovery of
//! missed publications, token refresh, delta compression, proxies and TLS
//! with the OS trust store.
//!
//! ```no_run
//! use centrifuge::{Client, HistoryOptions};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder("wss://example.com/connection/websocket")
//!     .token("<connection JWT>")
//!     .on_connected(|_client, e| println!("connected, client id {}", e.client_id))
//!     .on_disconnected(|_client, e| println!("disconnected: {} {}", e.code, e.reason))
//!     .build()?;
//!
//! let sub = client
//!     .new_subscription("news")
//!     .recoverable(true)
//!     .on_publication(|_sub, e| println!("{}: {:?}", e.offset, e.data))
//!     .build()?;
//!
//! sub.subscribe(); // never waits; sent once connected
//! client.connect()?;
//! sub.ready(None).await?;
//! sub.publish(r#"{"text":"hello"}"#).await?;
//! let recent = sub.history(HistoryOptions::new().limit(10)).await?;
//! println!("{} recent publications", recent.publications.len());
//! client.close().await;
//! # Ok(()) }
//! ```
//!
//! See the [README](https://github.com/centrifugal/centrifuge-rust) for a
//! guide and the [design](https://github.com/centrifugal/centrifuge-rust/blob/main/docs/spec.md)
//! for the exact behaviour.
#![cfg_attr(docsrs, feature(doc_cfg))]

mod calls;
mod client;
mod config;
mod dispatch;
mod driver;
mod error;
mod events;
mod maybe_send;
mod shared;
mod stats;
mod stream;
mod subscription;
mod transport;

pub use bytes::Bytes;
pub use centrifuge_core::codes;
pub use centrifuge_core::filter::{Filter, FilterNode};
pub use centrifuge_core::machine::{State, SubState};
pub use centrifuge_core::types::{
    ClientInfo, HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult, Publication,
    RpcResult, StreamPosition,
};
pub use client::{Client, ClientBuilder, ConnectionTokenContext, WeakClient};
pub use config::{
    EventDelivery, Protocol, ProxyAuth, ProxyConfig, ProxyTarget, TlsConfig, TransportEndpoint,
    TransportKind,
};
pub use error::{
    ArcError, BoxError, ClientError, ClientErrorKind, Error, ServerError, SubscriptionError,
    SubscriptionErrorKind, TokenError,
};
pub use events::{
    ClientEvent, ConnectedEvent, ConnectingEvent, DisconnectedEvent, ErrorEvent, JoinEvent,
    LeaveEvent, MessageEvent, PublicationEvent, ServerJoinEvent, ServerLeaveEvent,
    ServerPublicationEvent, ServerSubscribedEvent, ServerSubscribingEvent, ServerUnsubscribedEvent,
    SubscribedEvent, SubscribingEvent, SubscriptionErrorEvent, SubscriptionEvent,
    UnsubscribedEvent,
};
pub use maybe_send::{MaybeSend, MaybeSync};
pub use stats::Statistics;
pub use stream::EventStream;
pub use subscription::{
    DeltaType, Subscription, SubscriptionBuilder, SubscriptionGetStateContext,
    SubscriptionTokenContext,
};

/// Version of this crate, as published on crates.io.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn released_in_lockstep_with_core() {
        assert_eq!(VERSION, centrifuge_core::VERSION);
    }
}
