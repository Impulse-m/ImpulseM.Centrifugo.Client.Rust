//! Public data types, re-exported by `centrifuge-client` (docs/spec.md §4.6).
//!
//! Payloads are [`Bytes`] in both protocols: under JSON, the exact bytes of the
//! JSON value as received; under Protobuf, the bytes as published.

use std::collections::HashMap;

use bytes::Bytes;

use crate::proto;

fn opt_bytes(b: Bytes) -> Option<Bytes> {
    if b.is_empty() { None } else { Some(b) }
}

/// Information about a client connection: who published, joined or left.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ClientInfo {
    /// User ID (empty for anonymous connections).
    pub user: String,
    /// Client (connection) ID.
    pub client: String,
    /// Connection info attached by the server, if any.
    pub conn_info: Option<Bytes>,
    /// Channel info attached by the server, if any.
    pub chan_info: Option<Bytes>,
}

#[doc(hidden)] // the wire model is an implementation detail
impl From<proto::ClientInfo> for ClientInfo {
    fn from(i: proto::ClientInfo) -> Self {
        Self {
            user: i.user,
            client: i.client,
            conn_info: opt_bytes(i.conn_info),
            chan_info: opt_bytes(i.chan_info),
        }
    }
}

/// A message published into a channel.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Publication {
    /// The payload. With delta compression this is always the full payload.
    pub data: Bytes,
    /// Position in the channel's stream; 0 when the channel has no stream.
    pub offset: u64,
    /// Tags set by the publisher.
    pub tags: HashMap<String, String>,
    /// The publishing client, for publications made by clients.
    pub info: Option<ClientInfo>,
    /// The concrete channel, set when it differs from the subscription's.
    pub channel: Option<String>,
    /// Publication time in milliseconds since the Unix epoch, when the server
    /// sets it.
    pub time: Option<i64>,
}

impl Publication {
    /// Builds the public publication from a wire one, with `data` already
    /// resolved (delta applied).
    #[must_use]
    #[doc(hidden)] // the wire model is an implementation detail
    pub fn from_proto(p: proto::Publication, data: Bytes) -> Self {
        Self {
            data,
            offset: p.offset,
            tags: p.tags,
            info: p.info.map(Into::into),
            channel: if p.channel.is_empty() {
                None
            } else {
                Some(p.channel)
            },
            time: if p.time == 0 { None } else { Some(p.time) },
        }
    }

    /// Deserializes the payload as JSON.
    ///
    /// # Errors
    ///
    /// The `serde_json` error if the payload isn't valid JSON for `T`.
    #[cfg(feature = "json")]
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }
}

/// A position in a channel's stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct StreamPosition {
    /// Offset of the last publication.
    pub offset: u64,
    /// Identifies the stream; changes when the stream is recreated.
    pub epoch: String,
}

impl StreamPosition {
    /// A stream position, e.g. one an app saved, or returns from `get_state`.
    #[must_use]
    pub fn new(offset: u64, epoch: impl Into<String>) -> Self {
        Self {
            offset,
            epoch: epoch.into(),
        }
    }
}

/// Options of a history request. The default asks for the stream position
/// only, as centrifuge-js `history()` without arguments.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HistoryOptions {
    pub(crate) limit: i32,
    pub(crate) since: Option<StreamPosition>,
    pub(crate) reverse: bool,
}

impl HistoryOptions {
    /// Position only, no publications.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Up to `n` publications.
    #[must_use]
    pub fn limit(mut self, n: u32) -> Self {
        self.limit = i32::try_from(n).unwrap_or(i32::MAX);
        self
    }

    /// Every publication the server keeps.
    #[must_use]
    pub fn all(mut self) -> Self {
        self.limit = -1;
        self
    }

    /// The stream position only (the default).
    #[must_use]
    pub fn position_only(mut self) -> Self {
        self.limit = 0;
        self
    }

    /// Publications after `pos`.
    #[must_use]
    pub fn since(mut self, pos: StreamPosition) -> Self {
        self.since = Some(pos);
        self
    }

    /// Newest first.
    #[must_use]
    pub fn reverse(mut self, on: bool) -> Self {
        self.reverse = on;
        self
    }

    /// The wire request for `channel`.
    #[must_use]
    #[doc(hidden)] // the wire model is an implementation detail
    pub fn to_proto(&self, channel: &str) -> proto::HistoryRequest {
        proto::HistoryRequest {
            channel: channel.to_owned(),
            limit: self.limit,
            since: self.since.as_ref().map(|s| proto::StreamPosition {
                offset: s.offset,
                epoch: s.epoch.clone(),
            }),
            reverse: self.reverse,
        }
    }
}

/// Result of a history request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct HistoryResult {
    /// The publications asked for (none with the default options).
    pub publications: Vec<Publication>,
    /// The stream's current top offset.
    pub offset: u64,
    /// The stream's epoch.
    pub epoch: String,
}

#[doc(hidden)] // the wire model is an implementation detail
impl From<proto::HistoryResult> for HistoryResult {
    fn from(h: proto::HistoryResult) -> Self {
        Self {
            publications: h
                .publications
                .into_iter()
                .map(|p| {
                    let data = p.data.clone();
                    Publication::from_proto(p, data)
                })
                .collect(),
            offset: h.offset,
            epoch: h.epoch,
        }
    }
}

/// Clients currently subscribed to a channel.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PresenceResult {
    /// By client ID.
    pub clients: HashMap<String, ClientInfo>,
}

#[doc(hidden)] // the wire model is an implementation detail
impl From<proto::PresenceResult> for PresenceResult {
    fn from(p: proto::PresenceResult) -> Self {
        Self {
            clients: p.presence.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

/// Counts of clients and unique users in a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PresenceStatsResult {
    /// Connections subscribed.
    pub num_clients: u32,
    /// Unique users among them.
    pub num_users: u32,
}

#[doc(hidden)] // the wire model is an implementation detail
impl From<proto::PresenceStatsResult> for PresenceStatsResult {
    fn from(p: proto::PresenceStatsResult) -> Self {
        Self {
            num_clients: p.num_clients,
            num_users: p.num_users,
        }
    }
}

/// Result of an RPC call.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RpcResult {
    /// The payload the server returned.
    pub data: Bytes,
}

#[doc(hidden)] // the wire model is an implementation detail
impl From<proto::RpcResult> for RpcResult {
    fn from(r: proto::RpcResult) -> Self {
        Self { data: r.data }
    }
}

impl RpcResult {
    /// Deserializes the payload as JSON.
    ///
    /// # Errors
    ///
    /// The `serde_json` error if the payload isn't valid JSON for `T`.
    #[cfg(feature = "json")]
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.data)
    }
}

/// An error returned by the server for a command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct ServerError {
    /// Error code (docs/spec.md §6.5; centrifugal.dev "client protocol codes").
    pub code: u32,
    /// Human-readable message. Don't match on it; use `code`.
    pub message: String,
    /// Whether retrying later may succeed.
    pub temporary: bool,
}

#[doc(hidden)] // the wire model is an implementation detail
impl From<proto::Error> for ServerError {
    fn from(e: proto::Error) -> Self {
        Self {
            code: e.code,
            message: e.message,
            temporary: e.temporary,
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for ServerError {}
