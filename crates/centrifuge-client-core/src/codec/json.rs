//! JSON codec (docs/spec.md §5.4).
//!
//! - Field names are exactly the proto field names, as Centrifugo v6.10+
//!   decodes strictly.
//! - Default values are omitted when encoding, never sent as `null`.
//! - `bytes` fields are embedded raw JSON, not base64. Decoding locates them as
//!   borrowed [`RawValue`]s inside the frame and hands them out as `Bytes`
//!   slices of it, so payloads are never copied.
//! - 64-bit integers decode from a JSON number or a string.

use std::collections::HashMap;

use bytes::Bytes;
use serde::de::{self, Deserializer, Visitor};
use serde::ser::{SerializeMap, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{DecodeError, EncodeError};
use crate::proto;

// ───────────────────────────── decoding ─────────────────────────────

/// Decodes one JSON reply (a single line of a frame). `frame` is the buffer
/// `line` was sliced from, so `bytes` fields can be returned as sub-slices.
pub(crate) fn decode_reply(frame: &Bytes, line: &[u8]) -> Result<proto::Reply, DecodeError> {
    // serde accepts a sequence for a struct; the protocol only has objects.
    if line.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
        return Err(DecodeError::Json("reply is not a JSON object".into()));
    }
    let r: JReply<'_> =
        serde_json::from_slice(line).map_err(|e| DecodeError::Json(e.to_string()))?;
    Ok(r.into_proto(frame))
}

fn raw_bytes(frame: &Bytes, raw: Option<&RawValue>) -> Bytes {
    match raw {
        Some(r) => frame.slice_ref(r.get().as_bytes()),
        None => Bytes::new(),
    }
}

fn de_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct V;
    impl Visitor<'_> for V {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an unsigned integer as a number or a string")
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(E::custom)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<u64, E> {
            v.parse().map_err(E::custom)
        }
        fn visit_unit<E: de::Error>(self) -> Result<u64, E> {
            Ok(0)
        }
    }
    d.deserialize_any(V)
}

fn de_i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    struct V;
    impl Visitor<'_> for V {
        type Value = i64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an integer as a number or a string")
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i64, E> {
            i64::try_from(v).map_err(E::custom)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i64, E> {
            v.parse().map_err(E::custom)
        }
        fn visit_unit<E: de::Error>(self) -> Result<i64, E> {
            Ok(0)
        }
    }
    d.deserialize_any(V)
}

type Raw<'a> = Option<&'a RawValue>;

#[derive(Deserialize, Default)]
#[serde(default)]
struct JReply<'a> {
    id: u32,
    error: Option<JError>,
    #[serde(borrow)]
    push: Option<JPush<'a>>,
    #[serde(borrow)]
    connect: Option<JConnectResult<'a>>,
    #[serde(borrow)]
    subscribe: Option<JSubscribeResult<'a>>,
    unsubscribe: Option<de::IgnoredAny>,
    publish: Option<de::IgnoredAny>,
    #[serde(borrow)]
    presence: Option<JPresenceResult<'a>>,
    presence_stats: Option<JPresenceStats>,
    #[serde(borrow)]
    history: Option<JHistoryResult<'a>>,
    ping: Option<de::IgnoredAny>,
    #[serde(borrow)]
    rpc: Option<JRpcResult<'a>>,
    refresh: Option<JRefreshResult>,
    #[serde(borrow)]
    sub_refresh: Option<JSubRefreshResult<'a>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JError {
    code: u32,
    message: String,
    temporary: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JPush<'a> {
    #[serde(deserialize_with = "de_i64")]
    id: i64,
    channel: String,
    #[serde(borrow, rename = "pub")]
    publication: Option<JPublication<'a>>,
    #[serde(borrow)]
    join: Option<JJoinLeave<'a>>,
    #[serde(borrow)]
    leave: Option<JJoinLeave<'a>>,
    unsubscribe: Option<JUnsubscribe>,
    #[serde(borrow)]
    message: Option<JMessage<'a>>,
    #[serde(borrow)]
    subscribe: Option<JSubscribe<'a>>,
    #[serde(borrow)]
    connect: Option<JConnectResult<'a>>,
    disconnect: Option<JDisconnect>,
    refresh: Option<JRefresh>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JClientInfo<'a> {
    user: String,
    client: String,
    #[serde(borrow)]
    conn_info: Raw<'a>,
    #[serde(borrow)]
    chan_info: Raw<'a>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JPublication<'a> {
    #[serde(borrow)]
    data: Raw<'a>,
    #[serde(borrow)]
    info: Option<JClientInfo<'a>>,
    #[serde(deserialize_with = "de_u64")]
    offset: u64,
    tags: HashMap<String, String>,
    delta: bool,
    #[serde(deserialize_with = "de_i64")]
    time: i64,
    channel: String,
    key: String,
    removed: bool,
    #[serde(deserialize_with = "de_i64")]
    score: i64,
    epoch: String,
    #[serde(borrow)]
    prev_data: Raw<'a>,
    #[serde(deserialize_with = "de_u64")]
    version: u64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JJoinLeave<'a> {
    #[serde(borrow)]
    info: Option<JClientInfo<'a>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JUnsubscribe {
    code: u32,
    reason: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JMessage<'a> {
    #[serde(borrow)]
    data: Raw<'a>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JSubscribe<'a> {
    recoverable: bool,
    epoch: String,
    #[serde(deserialize_with = "de_u64")]
    offset: u64,
    positioned: bool,
    #[serde(borrow)]
    data: Raw<'a>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JDisconnect {
    code: u32,
    reason: String,
    reconnect: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JRefresh {
    expires: bool,
    ttl: u32,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JConnectResult<'a> {
    client: String,
    version: String,
    expires: bool,
    ttl: u32,
    #[serde(borrow)]
    data: Raw<'a>,
    #[serde(borrow)]
    subs: HashMap<String, JSubscribeResult<'a>>,
    ping: u32,
    pong: bool,
    session: String,
    node: String,
    #[serde(deserialize_with = "de_i64")]
    time: i64,
    #[serde(deserialize_with = "de_i64")]
    flag: i64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JSubscribeResult<'a> {
    expires: bool,
    ttl: u32,
    recoverable: bool,
    epoch: String,
    #[serde(borrow)]
    publications: Vec<JPublication<'a>>,
    recovered: bool,
    #[serde(deserialize_with = "de_u64")]
    offset: u64,
    positioned: bool,
    #[serde(borrow)]
    data: Raw<'a>,
    was_recovering: bool,
    delta: bool,
    #[serde(deserialize_with = "de_i64")]
    id: i64,
    publish_debounce: u32,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JPresenceResult<'a> {
    #[serde(borrow)]
    presence: HashMap<String, JClientInfo<'a>>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JPresenceStats {
    num_clients: u32,
    num_users: u32,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JHistoryResult<'a> {
    #[serde(borrow)]
    publications: Vec<JPublication<'a>>,
    epoch: String,
    #[serde(deserialize_with = "de_u64")]
    offset: u64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JRpcResult<'a> {
    #[serde(borrow)]
    data: Raw<'a>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JRefreshResult {
    client: String,
    version: String,
    expires: bool,
    ttl: u32,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct JSubRefreshResult<'a> {
    expires: bool,
    ttl: u32,
    #[serde(borrow)]
    items: Vec<JPublication<'a>>,
}

impl JReply<'_> {
    fn into_proto(self, f: &Bytes) -> proto::Reply {
        proto::Reply {
            id: self.id,
            error: self.error.map(|e| proto::Error {
                code: e.code,
                message: e.message,
                temporary: e.temporary,
            }),
            push: self.push.map(|p| p.into_proto(f)),
            connect: self.connect.map(|c| c.into_proto(f)),
            subscribe: self.subscribe.map(|s| s.into_proto(f)),
            unsubscribe: self.unsubscribe.map(|_| proto::UnsubscribeResult {}),
            publish: self.publish.map(|_| proto::PublishResult {}),
            presence: self.presence.map(|p| proto::PresenceResult {
                presence: p
                    .presence
                    .into_iter()
                    .map(|(k, v)| (k, v.into_proto(f)))
                    .collect(),
            }),
            presence_stats: self.presence_stats.map(|p| proto::PresenceStatsResult {
                num_clients: p.num_clients,
                num_users: p.num_users,
            }),
            history: self.history.map(|h| proto::HistoryResult {
                publications: h
                    .publications
                    .into_iter()
                    .map(|p| p.into_proto(f))
                    .collect(),
                epoch: h.epoch,
                offset: h.offset,
            }),
            ping: self.ping.map(|_| proto::PingResult {}),
            rpc: self.rpc.map(|r| proto::RpcResult {
                data: raw_bytes(f, r.data),
            }),
            refresh: self.refresh.map(|r| proto::RefreshResult {
                client: r.client,
                version: r.version,
                expires: r.expires,
                ttl: r.ttl,
            }),
            sub_refresh: self.sub_refresh.map(|r| proto::SubRefreshResult {
                expires: r.expires,
                ttl: r.ttl,
                items: r.items.into_iter().map(|p| p.into_proto(f)).collect(),
            }),
        }
    }
}

impl JPush<'_> {
    fn into_proto(self, f: &Bytes) -> proto::Push {
        proto::Push {
            id: self.id,
            channel: self.channel,
            r#pub: self.publication.map(|p| p.into_proto(f)),
            join: self.join.map(|j| proto::Join {
                info: j.info.map(|i| i.into_proto(f)),
            }),
            leave: self.leave.map(|l| proto::Leave {
                info: l.info.map(|i| i.into_proto(f)),
            }),
            unsubscribe: self.unsubscribe.map(|u| proto::Unsubscribe {
                code: u.code,
                reason: u.reason,
            }),
            message: self.message.map(|m| proto::Message {
                data: raw_bytes(f, m.data),
            }),
            subscribe: self.subscribe.map(|s| proto::Subscribe {
                recoverable: s.recoverable,
                epoch: s.epoch,
                offset: s.offset,
                positioned: s.positioned,
                data: raw_bytes(f, s.data),
            }),
            connect: self.connect.map(|c| {
                let r = c.into_proto(f);
                proto::Connect {
                    client: r.client,
                    version: r.version,
                    data: r.data,
                    subs: r.subs,
                    expires: r.expires,
                    ttl: r.ttl,
                    ping: r.ping,
                    pong: r.pong,
                    session: r.session,
                    node: r.node,
                    time: r.time,
                }
            }),
            disconnect: self.disconnect.map(|d| proto::Disconnect {
                code: d.code,
                reason: d.reason,
                reconnect: d.reconnect,
            }),
            refresh: self.refresh.map(|r| proto::Refresh {
                expires: r.expires,
                ttl: r.ttl,
            }),
        }
    }
}

impl JClientInfo<'_> {
    fn into_proto(self, f: &Bytes) -> proto::ClientInfo {
        proto::ClientInfo {
            user: self.user,
            client: self.client,
            conn_info: raw_bytes(f, self.conn_info),
            chan_info: raw_bytes(f, self.chan_info),
        }
    }
}

impl JPublication<'_> {
    fn into_proto(self, f: &Bytes) -> proto::Publication {
        proto::Publication {
            data: raw_bytes(f, self.data),
            info: self.info.map(|i| i.into_proto(f)),
            offset: self.offset,
            tags: self.tags,
            delta: self.delta,
            time: self.time,
            channel: self.channel,
            key: self.key,
            removed: self.removed,
            score: self.score,
            epoch: self.epoch,
            prev_data: raw_bytes(f, self.prev_data),
            version: self.version,
        }
    }
}

impl JConnectResult<'_> {
    fn into_proto(self, f: &Bytes) -> proto::ConnectResult {
        proto::ConnectResult {
            client: self.client,
            version: self.version,
            expires: self.expires,
            ttl: self.ttl,
            data: raw_bytes(f, self.data),
            subs: self
                .subs
                .into_iter()
                .map(|(k, v)| (k, v.into_proto(f)))
                .collect(),
            ping: self.ping,
            pong: self.pong,
            session: self.session,
            node: self.node,
            time: self.time,
            flag: self.flag,
            dict: None,
        }
    }
}

impl JSubscribeResult<'_> {
    fn into_proto(self, f: &Bytes) -> proto::SubscribeResult {
        proto::SubscribeResult {
            expires: self.expires,
            ttl: self.ttl,
            recoverable: self.recoverable,
            epoch: self.epoch,
            publications: self
                .publications
                .into_iter()
                .map(|p| p.into_proto(f))
                .collect(),
            recovered: self.recovered,
            offset: self.offset,
            positioned: self.positioned,
            data: raw_bytes(f, self.data),
            was_recovering: self.was_recovering,
            delta: self.delta,
            id: self.id,
            publish_debounce: self.publish_debounce,
            ..Default::default()
        }
    }
}

// ───────────────────────────── encoding ─────────────────────────────

/// A `bytes` field as embedded raw JSON. Empty means absent.
fn raw<'a>(field: &'static str, b: &'a Bytes) -> Result<Option<&'a RawValue>, EncodeError> {
    if b.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice::<&RawValue>(b)
        .map(Some)
        .map_err(|_| EncodeError::InvalidJson { field })
}

/// Encodes one command as a single JSON line (no trailing newline).
pub(crate) fn encode_command(cmd: &proto::Command, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    let j = JCommand::from_proto(cmd)?;
    serde_json::to_writer(out, &j).map_err(|e| EncodeError::Json(e.to_string()))
}

/// Encodes an emulation request: the encoded command frame travels as a JSON
/// string (centrifuge `emulation.go`).
pub(crate) fn encode_emulation_request(
    node: &str,
    session: &str,
    frame: &[u8],
) -> Result<Vec<u8>, EncodeError> {
    #[derive(Serialize)]
    struct J<'a> {
        session: &'a str,
        node: &'a str,
        data: &'a str,
    }
    let data = std::str::from_utf8(frame).map_err(|_| EncodeError::InvalidJson {
        field: "emulation data",
    })?;
    serde_json::to_vec(&J {
        session,
        node,
        data,
    })
    .map_err(|e| EncodeError::Json(e.to_string()))
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}
fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
fn is_false(v: &bool) -> bool {
    !*v
}
fn is_empty_str(v: &&str) -> bool {
    v.is_empty()
}

#[derive(Serialize)]
struct JCommand<'a> {
    #[serde(skip_serializing_if = "is_zero_u32")]
    id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    connect: Option<JConnectRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscribe: Option<JSubscribeRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsubscribe: Option<JChannel<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publish: Option<JPublishRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence: Option<JChannel<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    presence_stats: Option<JChannel<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<JHistoryRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    send: Option<JData<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rpc: Option<JRpcRequest<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh: Option<JToken<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub_refresh: Option<JSubRefreshRequest<'a>>,
}

#[derive(Serialize)]
struct JConnectRequest<'a> {
    #[serde(skip_serializing_if = "is_empty_str")]
    token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    subs: HashMap<&'a str, JSubscribeRequest<'a>>,
    #[serde(skip_serializing_if = "is_empty_str")]
    name: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    version: &'a str,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    headers: HashMap<&'a str, &'a str>,
    #[serde(skip_serializing_if = "is_zero_i64")]
    flag: i64,
}

#[derive(Serialize)]
struct JSubscribeRequest<'a> {
    #[serde(skip_serializing_if = "is_empty_str")]
    channel: &'a str,
    #[serde(skip_serializing_if = "is_empty_str")]
    token: &'a str,
    #[serde(skip_serializing_if = "is_false")]
    recover: bool,
    #[serde(skip_serializing_if = "is_empty_str")]
    epoch: &'a str,
    #[serde(skip_serializing_if = "is_zero_u64")]
    offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "is_false")]
    positioned: bool,
    #[serde(skip_serializing_if = "is_false")]
    recoverable: bool,
    #[serde(skip_serializing_if = "is_false")]
    join_leave: bool,
    #[serde(skip_serializing_if = "is_empty_str")]
    delta: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tf: Option<JFilter<'a>>,
    #[serde(skip_serializing_if = "is_zero_i64")]
    flag: i64,
    #[serde(rename = "type", skip_serializing_if = "is_zero_i32")]
    kind: i32,
}

struct JFilter<'a>(&'a proto::FilterNode);

impl Serialize for JFilter<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let n = self.0;
        let mut m = s.serialize_map(None)?;
        if !n.op.is_empty() {
            m.serialize_entry("op", &n.op)?;
        }
        if !n.key.is_empty() {
            m.serialize_entry("key", &n.key)?;
        }
        if !n.cmp.is_empty() {
            m.serialize_entry("cmp", &n.cmp)?;
        }
        if !n.val.is_empty() {
            m.serialize_entry("val", &n.val)?;
        }
        if !n.vals.is_empty() {
            m.serialize_entry("vals", &n.vals)?;
        }
        if !n.nodes.is_empty() {
            let nodes: Vec<JFilter<'_>> = n.nodes.iter().map(JFilter).collect();
            m.serialize_entry("nodes", &nodes)?;
        }
        m.end()
    }
}

#[derive(Serialize)]
struct JChannel<'a> {
    channel: &'a str,
}

#[derive(Serialize)]
struct JPublishRequest<'a> {
    channel: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
}

struct JHistoryRequest<'a>(&'a proto::HistoryRequest);

impl Serialize for JHistoryRequest<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let h = self.0;
        let mut st = s.serialize_struct("HistoryRequest", 4)?;
        st.serialize_field("channel", &h.channel)?;
        if h.limit != 0 {
            st.serialize_field("limit", &h.limit)?;
        }
        if let Some(sp) = &h.since {
            #[derive(Serialize)]
            struct P<'b> {
                #[serde(skip_serializing_if = "is_zero_u64")]
                offset: u64,
                #[serde(skip_serializing_if = "is_empty_str")]
                epoch: &'b str,
            }
            st.serialize_field(
                "since",
                &P {
                    offset: sp.offset,
                    epoch: &sp.epoch,
                },
            )?;
        }
        if h.reverse {
            st.serialize_field("reverse", &true)?;
        }
        st.end()
    }
}

#[derive(Serialize)]
struct JData<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
}

#[derive(Serialize)]
struct JRpcRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "is_empty_str")]
    method: &'a str,
}

#[derive(Serialize)]
struct JToken<'a> {
    token: &'a str,
}

#[derive(Serialize)]
struct JSubRefreshRequest<'a> {
    channel: &'a str,
    token: &'a str,
}

impl<'a> JSubscribeRequest<'a> {
    fn from_proto(r: &'a proto::SubscribeRequest) -> Result<Self, EncodeError> {
        Ok(Self {
            channel: &r.channel,
            token: &r.token,
            recover: r.recover,
            epoch: &r.epoch,
            offset: r.offset,
            data: raw("subscribe data", &r.data)?,
            positioned: r.positioned,
            recoverable: r.recoverable,
            join_leave: r.join_leave,
            delta: &r.delta,
            tf: r.tf.as_ref().map(JFilter),
            flag: r.flag,
            kind: r.r#type,
        })
    }
}

impl<'a> JCommand<'a> {
    fn from_proto(c: &'a proto::Command) -> Result<Self, EncodeError> {
        Ok(Self {
            id: c.id,
            connect: match &c.connect {
                Some(r) => Some(JConnectRequest {
                    token: &r.token,
                    data: raw("connect data", &r.data)?,
                    subs: r
                        .subs
                        .iter()
                        .map(|(k, v)| JSubscribeRequest::from_proto(v).map(|s| (k.as_str(), s)))
                        .collect::<Result<_, _>>()?,
                    name: &r.name,
                    version: &r.version,
                    headers: r
                        .headers
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect(),
                    flag: r.flag,
                }),
                None => None,
            },
            subscribe: match &c.subscribe {
                Some(r) => Some(JSubscribeRequest::from_proto(r)?),
                None => None,
            },
            unsubscribe: c.unsubscribe.as_ref().map(|r| JChannel {
                channel: &r.channel,
            }),
            publish: match &c.publish {
                Some(r) => Some(JPublishRequest {
                    channel: &r.channel,
                    data: raw("publish data", &r.data)?,
                }),
                None => None,
            },
            presence: c.presence.as_ref().map(|r| JChannel {
                channel: &r.channel,
            }),
            presence_stats: c.presence_stats.as_ref().map(|r| JChannel {
                channel: &r.channel,
            }),
            history: c.history.as_ref().map(JHistoryRequest),
            send: match &c.send {
                Some(r) => Some(JData {
                    data: raw("send data", &r.data)?,
                }),
                None => None,
            },
            rpc: match &c.rpc {
                Some(r) => Some(JRpcRequest {
                    data: raw("rpc data", &r.data)?,
                    method: &r.method,
                }),
                None => None,
            },
            refresh: c.refresh.as_ref().map(|r| JToken { token: &r.token }),
            sub_refresh: c.sub_refresh.as_ref().map(|r| JSubRefreshRequest {
                channel: &r.channel,
                token: &r.token,
            }),
        })
    }
}
