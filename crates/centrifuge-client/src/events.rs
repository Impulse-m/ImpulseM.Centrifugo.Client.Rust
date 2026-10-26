//! Events delivered to handlers and streams (docs/spec.md §4.4–§4.5).

use bytes::Bytes;

use centrifuge_core::machine::TransportKind;
use centrifuge_core::types::{ClientInfo, Publication, StreamPosition};

use crate::error::{ClientError, SubscriptionError};

macro_rules! code_reason_event {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        #[non_exhaustive]
        pub struct $name {
            /// Code (`centrifuge::codes`).
            pub code: u32,
            /// Human-readable reason.
            pub reason: String,
        }

        #[cfg(feature = "test-util")]
        impl $name {
            /// For unit-testing handlers.
            #[must_use]
            pub fn new(code: u32, reason: impl Into<String>) -> Self {
                Self { code, reason: reason.into() }
            }
        }
    };
}

code_reason_event!(
    /// The client started connecting or reconnecting.
    ConnectingEvent
);
code_reason_event!(
    /// The client is disconnected.
    DisconnectedEvent
);
code_reason_event!(
    /// A subscription started subscribing or resubscribing.
    SubscribingEvent
);
code_reason_event!(
    /// A subscription is unsubscribed.
    UnsubscribedEvent
);

/// The client connected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectedEvent {
    /// Client ID the server assigned.
    pub client_id: String,
    /// Server version.
    pub version: String,
    /// Data from the connect reply, if any.
    pub data: Option<Bytes>,
    /// The transport in use.
    pub transport: TransportKind,
}

/// A background client error.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ErrorEvent {
    /// The error.
    pub error: ClientError,
}

/// A background subscription error.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubscriptionErrorEvent {
    /// The error.
    pub error: SubscriptionError,
}

/// An async message from the server.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MessageEvent {
    /// The payload.
    pub data: Bytes,
}

impl MessageEvent {
    /// Deserializes the payload as JSON.
    ///
    /// # Errors
    ///
    /// The `serde_json` error if the payload isn't valid JSON for `T`.
    #[cfg(feature = "serde")]
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }
}

/// A subscription is subscribed.
#[allow(clippy::struct_excessive_bools)] // the protocol's flags
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscribedEvent {
    /// A recovery was attempted.
    pub was_recovering: bool,
    /// Every missed publication was recovered. Only meaningful when
    /// `was_recovering`.
    pub recovered: bool,
    /// The subscription is recoverable.
    pub recoverable: bool,
    /// The subscription is positioned.
    pub positioned: bool,
    /// The stream position the server replied with, for positioned or
    /// recoverable subscriptions. After a recovery it's the position recovered
    /// from; checkpoint from delivered publications instead.
    pub stream_position: Option<StreamPosition>,
    /// Data from the subscribe reply, if any.
    pub data: Option<Bytes>,
}

/// A publication in a subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PublicationEvent {
    /// The publication.
    pub publication: Publication,
}

impl std::ops::Deref for PublicationEvent {
    type Target = Publication;
    fn deref(&self) -> &Publication {
        &self.publication
    }
}

/// Someone joined the channel.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct JoinEvent {
    /// Who.
    pub info: ClientInfo,
}

impl std::ops::Deref for JoinEvent {
    type Target = ClientInfo;
    fn deref(&self) -> &ClientInfo {
        &self.info
    }
}

/// Someone left the channel.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LeaveEvent {
    /// Who.
    pub info: ClientInfo,
}

impl std::ops::Deref for LeaveEvent {
    type Target = ClientInfo;
    fn deref(&self) -> &ClientInfo {
        &self.info
    }
}

/// A server-side subscription is subscribed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerSubscribedEvent {
    /// The channel.
    pub channel: String,
    /// The same details as [`SubscribedEvent`].
    pub info: SubscribedEvent,
}

/// A server-side subscription waits for a reconnect.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerSubscribingEvent {
    /// The channel.
    pub channel: String,
}

/// A server-side subscription ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerUnsubscribedEvent {
    /// The channel.
    pub channel: String,
}

/// A publication in a server-side subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerPublicationEvent {
    /// The channel.
    pub channel: String,
    /// The publication.
    pub publication: Publication,
}

impl std::ops::Deref for ServerPublicationEvent {
    type Target = Publication;
    fn deref(&self) -> &Publication {
        &self.publication
    }
}

/// A join in a server-side subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerJoinEvent {
    /// The channel.
    pub channel: String,
    /// Who.
    pub info: ClientInfo,
}

/// A leave in a server-side subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerLeaveEvent {
    /// The channel.
    pub channel: String,
    /// Who.
    pub info: ClientInfo,
}

/// Every client-level event, for [`crate::Client::events`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ClientEvent {
    /// See [`ConnectingEvent`].
    Connecting(ConnectingEvent),
    /// See [`ConnectedEvent`].
    Connected(ConnectedEvent),
    /// See [`DisconnectedEvent`].
    Disconnected(DisconnectedEvent),
    /// See [`ErrorEvent`].
    Error(ErrorEvent),
    /// See [`MessageEvent`].
    Message(MessageEvent),
    /// See [`ServerSubscribedEvent`].
    ServerSubscribed(ServerSubscribedEvent),
    /// See [`ServerSubscribingEvent`].
    ServerSubscribing(ServerSubscribingEvent),
    /// See [`ServerUnsubscribedEvent`].
    ServerUnsubscribed(ServerUnsubscribedEvent),
    /// See [`ServerPublicationEvent`].
    ServerPublication(ServerPublicationEvent),
    /// See [`ServerJoinEvent`].
    ServerJoin(ServerJoinEvent),
    /// See [`ServerLeaveEvent`].
    ServerLeave(ServerLeaveEvent),
}

/// Every subscription event, for [`crate::Subscription::events`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SubscriptionEvent {
    /// See [`SubscribingEvent`].
    Subscribing(SubscribingEvent),
    /// See [`SubscribedEvent`].
    Subscribed(SubscribedEvent),
    /// See [`UnsubscribedEvent`].
    Unsubscribed(UnsubscribedEvent),
    /// See [`SubscriptionErrorEvent`].
    Error(SubscriptionErrorEvent),
    /// See [`PublicationEvent`].
    Publication(PublicationEvent),
    /// See [`JoinEvent`].
    Join(JoinEvent),
    /// See [`LeaveEvent`].
    Leave(LeaveEvent),
}

fn opt_bytes(b: Bytes) -> Option<Bytes> {
    if b.is_empty() { None } else { Some(b) }
}

pub(crate) fn subscribed_event(i: centrifuge_core::machine::SubscribedInfo) -> SubscribedEvent {
    SubscribedEvent {
        was_recovering: i.was_recovering,
        recovered: i.recovered,
        recoverable: i.recoverable,
        positioned: i.positioned,
        stream_position: i.stream_position,
        data: opt_bytes(i.data),
    }
}

pub(crate) fn connected_event(
    client_id: String,
    version: String,
    data: Bytes,
    transport: TransportKind,
) -> ConnectedEvent {
    ConnectedEvent {
        client_id,
        version,
        data: opt_bytes(data),
        transport,
    }
}

#[cfg(feature = "test-util")]
mod test_util {
    use super::{
        Bytes, ConnectedEvent, MessageEvent, Publication, PublicationEvent, StreamPosition,
        SubscribedEvent, TransportKind,
    };

    impl ConnectedEvent {
        /// For unit-testing handlers.
        #[must_use]
        pub fn new(client_id: impl Into<String>, transport: TransportKind) -> Self {
            Self {
                client_id: client_id.into(),
                version: String::new(),
                data: None,
                transport,
            }
        }
    }

    impl PublicationEvent {
        /// For unit-testing handlers.
        #[must_use]
        pub fn new(publication: Publication) -> Self {
            Self { publication }
        }
    }

    impl MessageEvent {
        /// For unit-testing handlers.
        #[must_use]
        pub fn new(data: impl Into<Bytes>) -> Self {
            Self { data: data.into() }
        }
    }

    impl SubscribedEvent {
        /// For unit-testing handlers.
        #[must_use]
        pub fn new(recovered: bool, stream_position: Option<StreamPosition>) -> Self {
            Self {
                was_recovering: recovered,
                recovered,
                recoverable: stream_position.is_some(),
                positioned: stream_position.is_some(),
                stream_position,
                data: None,
            }
        }
    }
}
