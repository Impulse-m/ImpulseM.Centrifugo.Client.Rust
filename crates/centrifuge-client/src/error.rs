//! Errors (docs/spec.md §4.7).

use std::sync::Arc;

pub use centrifuge_client_core::machine::{ClientErrorKind, SubscriptionErrorKind};
pub use centrifuge_client_core::types::ServerError;

/// A boxed error from app code.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A shareable underlying error, carried by error events (which are `Clone`).
pub type ArcError = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// Why a call or an operation failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// No reply within the configured `timeout`.
    Timeout,
    /// The client is disconnected (or the connection ended while waiting).
    ClientDisconnected,
    /// The connection was lost while an rpc call was on it. The call is not
    /// resent on the next connection.
    ConnectionClosed,
    /// The client was closed.
    ClientClosed,
    /// The subscription is unsubscribed (or became so while waiting).
    SubscriptionUnsubscribed,
    /// A subscription to this channel is already registered.
    DuplicateSubscription,
    /// The server answered with an error.
    Server(ServerError),
    /// A payload is invalid for the protocol (the JSON protocol needs valid JSON).
    InvalidData(String),
    /// The transport rejected or failed the request.
    #[non_exhaustive]
    Transport {
        /// HTTP status, when there was one (e.g. 413 from the emulation endpoint).
        status: Option<u16>,
        /// A description.
        message: String,
    },
    /// Invalid configuration, or an operation that can't work as configured.
    Configuration(String),
    /// Waiting here could never end: the SDK's event loop is waiting for the
    /// handler (or callback) this call was made from (docs/spec.md §9.5).
    WouldDeadlock,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => f.write_str("timeout"),
            Self::ClientDisconnected => f.write_str("client disconnected"),
            Self::ConnectionClosed => f.write_str("connection closed"),
            Self::ClientClosed => f.write_str("client closed"),
            Self::SubscriptionUnsubscribed => f.write_str("subscription unsubscribed"),
            Self::DuplicateSubscription => f.write_str("duplicate subscription"),
            Self::Server(e) => write!(f, "server error: {e}"),
            Self::InvalidData(e) => write!(f, "invalid data: {e}"),
            Self::Transport { status: Some(s), message } => write!(f, "transport error (HTTP {s}): {message}"),
            Self::Transport { status: None, message } => write!(f, "transport error: {message}"),
            Self::Configuration(e) => write!(f, "configuration error: {e}"),
            Self::WouldDeadlock => f.write_str(
                "would deadlock: this waits for the SDK's event loop, which is waiting for the handler or callback \
                 it was called from (see docs/spec.md §9.5); spawn the call instead",
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Server(e) => Some(e),
            _ => None,
        }
    }
}

/// The result a token getter returns to the SDK (docs/spec.md §4.2).
///
/// `?` works inside getters: any standard error becomes [`TokenError::Other`],
/// which the SDK retries later. Return [`TokenError::Unauthorized`] to stop.
///
/// It deliberately doesn't implement `std::error::Error`: it's an instruction
/// for the SDK, not an error to propagate, and implementing it would make the
/// blanket `From` impl overlap with `impl<T> From<T> for T`.
#[derive(Debug)]
#[non_exhaustive]
pub enum TokenError {
    /// Stop: the user isn't allowed. The client goes to `disconnected(1)`, a
    /// subscription to `unsubscribed(1)`.
    Unauthorized,
    /// Any other failure: retried with backoff.
    Other(BoxError),
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for TokenError {
    fn from(e: E) -> Self {
        Self::Other(Box::new(e))
    }
}

/// A background client failure, delivered through the `error` event.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientError {
    /// What failed.
    pub kind: ClientErrorKind,
    /// A description.
    pub message: String,
    /// The underlying error, if any.
    pub source: Option<ArcError>,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// A background subscription failure, delivered through the subscription's
/// `error` event.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubscriptionError {
    /// What failed.
    pub kind: SubscriptionErrorKind,
    /// A description.
    pub message: String,
    /// The underlying error, if any.
    pub source: Option<ArcError>,
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for SubscriptionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}
