//! The server side of both protocols: commands in, replies out.

use bytes::{Buf as _, Bytes};
use centrifuge_client_core::proto;
use prost::Message as _;
use serde_json::{Map, Value, json};

/// The wire protocol of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// JSON lines.
    Json,
    /// Varint-delimited Protobuf.
    Protobuf,
}

/// Decodes a frame of commands.
pub(crate) fn decode_commands(
    protocol: Protocol,
    data: &[u8],
) -> Result<Vec<proto::Command>, String> {
    match protocol {
        Protocol::Protobuf => {
            let mut buf = data;
            let mut out = Vec::new();
            while buf.has_remaining() {
                out.push(
                    proto::Command::decode_length_delimited(&mut buf).map_err(|e| e.to_string())?,
                );
            }
            Ok(out)
        }
        Protocol::Json => data
            .split(|b| *b == b'\n')
            .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
            .map(|l| {
                let v: Value = serde_json::from_slice(l).map_err(|e| e.to_string())?;
                Ok(command_from_json(&v))
            })
            .collect(),
    }
}

/// Decodes an emulation request body: `(session, node, commands)`.
pub(crate) fn decode_emulation(
    protocol: Protocol,
    body: &[u8],
) -> Result<(String, String, Vec<proto::Command>), String> {
    match protocol {
        Protocol::Protobuf => {
            let req = proto::EmulationRequest::decode(body).map_err(|e| e.to_string())?;
            let cmds = decode_commands(protocol, &req.data)?;
            Ok((req.session, req.node, cmds))
        }
        Protocol::Json => {
            let v: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
            let data = v.get("data").and_then(Value::as_str).unwrap_or_default();
            let cmds = decode_commands(protocol, data.as_bytes())?;
            Ok((s(&v, "session"), s(&v, "node"), cmds))
        }
    }
}

/// Encodes replies as one WebSocket frame (no trailing newline for JSON).
pub(crate) fn encode_frame(protocol: Protocol, replies: &[proto::Reply]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, r) in replies.iter().enumerate() {
        match protocol {
            Protocol::Protobuf => r.encode_length_delimited(&mut out).unwrap_or_default(),
            Protocol::Json => {
                if i > 0 {
                    out.push(b'\n');
                }
                out.extend(serde_json::to_vec(&reply_to_json(r)).unwrap_or_default());
            }
        }
    }
    out
}

/// Encodes replies for an HTTP stream (JSON: every line ends with a newline).
pub(crate) fn encode_stream(protocol: Protocol, replies: &[proto::Reply]) -> Vec<u8> {
    let mut out = encode_frame(protocol, replies);
    if protocol == Protocol::Json && !out.is_empty() {
        out.push(b'\n');
    }
    out
}

fn s(v: &Value, k: &str) -> String {
    v.get(k)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn b(v: &Value, k: &str) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or_default()
}

fn u(v: &Value, k: &str) -> u64 {
    match v.get(k) {
        Some(Value::Number(n)) => n.as_u64().unwrap_or_default(),
        Some(Value::String(s)) => s.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn u32_of(v: &Value, k: &str) -> u32 {
    u32::try_from(u(v, k)).unwrap_or(u32::MAX)
}

/// A raw JSON payload field as bytes.
fn raw(v: &Value, k: &str) -> Bytes {
    match v.get(k) {
        None | Some(Value::Null) => Bytes::new(),
        Some(x) => Bytes::from(serde_json::to_vec(x).unwrap_or_default()),
    }
}

fn sub_req(v: &Value, channel: String) -> proto::SubscribeRequest {
    proto::SubscribeRequest {
        channel,
        token: s(v, "token"),
        recover: b(v, "recover"),
        epoch: s(v, "epoch"),
        offset: u(v, "offset"),
        data: raw(v, "data"),
        positioned: b(v, "positioned"),
        recoverable: b(v, "recoverable"),
        join_leave: b(v, "join_leave"),
        delta: s(v, "delta"),
        ..Default::default()
    }
}

fn command_from_json(v: &Value) -> proto::Command {
    let mut c = proto::Command {
        id: u32_of(v, "id"),
        ..Default::default()
    };
    if let Some(x) = v.get("connect") {
        let subs = x
            .get("subs")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .map(|(k, sv)| (k.clone(), sub_req(sv, String::new())))
                    .collect()
            })
            .unwrap_or_default();
        let headers = x
            .get("headers")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .map(|(k, hv)| (k.clone(), hv.as_str().unwrap_or_default().to_owned()))
                    .collect()
            })
            .unwrap_or_default();
        c.connect = Some(proto::ConnectRequest {
            token: s(x, "token"),
            data: raw(x, "data"),
            subs,
            name: s(x, "name"),
            version: s(x, "version"),
            headers,
            ..Default::default()
        });
    }
    if let Some(x) = v.get("subscribe") {
        c.subscribe = Some(sub_req(x, s(x, "channel")));
    }
    if let Some(x) = v.get("unsubscribe") {
        c.unsubscribe = Some(proto::UnsubscribeRequest {
            channel: s(x, "channel"),
        });
    }
    if let Some(x) = v.get("publish") {
        c.publish = Some(proto::PublishRequest {
            channel: s(x, "channel"),
            data: raw(x, "data"),
            ..Default::default()
        });
    }
    if let Some(x) = v.get("presence") {
        c.presence = Some(proto::PresenceRequest {
            channel: s(x, "channel"),
        });
    }
    if let Some(x) = v.get("presence_stats") {
        c.presence_stats = Some(proto::PresenceStatsRequest {
            channel: s(x, "channel"),
        });
    }
    if let Some(x) = v.get("history") {
        c.history = Some(proto::HistoryRequest {
            channel: s(x, "channel"),
            limit: x
                .get("limit")
                .and_then(Value::as_i64)
                .and_then(|l| i32::try_from(l).ok())
                .unwrap_or_default(),
            since: x.get("since").map(|p| proto::StreamPosition {
                offset: u(p, "offset"),
                epoch: s(p, "epoch"),
            }),
            reverse: b(x, "reverse"),
        });
    }
    if v.get("ping").is_some() {
        c.ping = Some(proto::PingRequest {});
    }
    if let Some(x) = v.get("send") {
        c.send = Some(proto::SendRequest {
            data: raw(x, "data"),
        });
    }
    if let Some(x) = v.get("rpc") {
        c.rpc = Some(proto::RpcRequest {
            data: raw(x, "data"),
            method: s(x, "method"),
        });
    }
    if let Some(x) = v.get("refresh") {
        c.refresh = Some(proto::RefreshRequest {
            token: s(x, "token"),
        });
    }
    if let Some(x) = v.get("sub_refresh") {
        c.sub_refresh = Some(proto::SubRefreshRequest {
            channel: s(x, "channel"),
            token: s(x, "token"),
            ..Default::default()
        });
    }
    c
}

/// Inserts non-default values only, as the real server's JSON encoder does.
struct Obj(Map<String, Value>);

impl Obj {
    fn new() -> Self {
        Self(Map::new())
    }
    fn str(mut self, k: &str, v: &str) -> Self {
        if !v.is_empty() {
            self.0.insert(k.into(), json!(v));
        }
        self
    }
    fn num(mut self, k: &str, v: u64) -> Self {
        if v != 0 {
            self.0.insert(k.into(), json!(v));
        }
        self
    }
    fn int(mut self, k: &str, v: i64) -> Self {
        if v != 0 {
            self.0.insert(k.into(), json!(v));
        }
        self
    }
    fn flag(mut self, k: &str, v: bool) -> Self {
        if v {
            self.0.insert(k.into(), json!(true));
        }
        self
    }
    /// A raw JSON payload. Invalid JSON is embedded as a string, so a test can
    /// still see it.
    fn raw(mut self, k: &str, v: &Bytes) -> Self {
        if !v.is_empty() {
            let val =
                serde_json::from_slice(v).unwrap_or_else(|_| json!(String::from_utf8_lossy(v)));
            self.0.insert(k.into(), val);
        }
        self
    }
    fn val(mut self, k: &str, v: Option<Value>) -> Self {
        if let Some(v) = v {
            self.0.insert(k.into(), v);
        }
        self
    }
    fn done(self) -> Value {
        Value::Object(self.0)
    }
}

fn info_json(i: &proto::ClientInfo) -> Value {
    Obj::new()
        .str("user", &i.user)
        .str("client", &i.client)
        .raw("conn_info", &i.conn_info)
        .raw("chan_info", &i.chan_info)
        .done()
}

fn pub_json(p: &proto::Publication) -> Value {
    let tags = (!p.tags.is_empty()).then(|| json!(p.tags));
    // On delta channels the connection layer has already wrapped `data` in a
    // JSON string, as the real server does.
    Obj::new()
        .raw("data", &p.data)
        .flag("delta", p.delta)
        .val("info", p.info.as_ref().map(info_json))
        .num("offset", p.offset)
        .val("tags", tags)
        .int("time", p.time)
        .str("channel", &p.channel)
        .done()
}

fn pubs_json(ps: &[proto::Publication]) -> Option<Value> {
    (!ps.is_empty()).then(|| Value::Array(ps.iter().map(pub_json).collect()))
}

fn sub_result_json(r: &proto::SubscribeResult) -> Value {
    Obj::new()
        .flag("expires", r.expires)
        .num("ttl", r.ttl.into())
        .flag("recoverable", r.recoverable)
        .str("epoch", &r.epoch)
        .val("publications", pubs_json(&r.publications))
        .flag("recovered", r.recovered)
        .num("offset", r.offset)
        .flag("positioned", r.positioned)
        .raw("data", &r.data)
        .flag("was_recovering", r.was_recovering)
        .flag("delta", r.delta)
        .done()
}

fn subs_json(subs: &std::collections::HashMap<String, proto::SubscribeResult>) -> Option<Value> {
    (!subs.is_empty()).then(|| {
        Value::Object(
            subs.iter()
                .map(|(k, v)| (k.clone(), sub_result_json(v)))
                .collect(),
        )
    })
}

fn connect_json(r: &proto::ConnectResult) -> Value {
    Obj::new()
        .str("client", &r.client)
        .str("version", &r.version)
        .flag("expires", r.expires)
        .num("ttl", r.ttl.into())
        .raw("data", &r.data)
        .val("subs", subs_json(&r.subs))
        .num("ping", r.ping.into())
        .flag("pong", r.pong)
        .str("session", &r.session)
        .str("node", &r.node)
        .int("time", r.time)
        .done()
}

fn push_json(p: &proto::Push) -> Value {
    let mut o = Obj::new().str("channel", &p.channel).int("id", p.id);
    if let Some(x) = &p.r#pub {
        o = o.val("pub", Some(pub_json(x)));
    }
    if let Some(x) = &p.join {
        o = o.val(
            "join",
            Some(json!({ "info": x.info.as_ref().map(info_json) })),
        );
    }
    if let Some(x) = &p.leave {
        o = o.val(
            "leave",
            Some(json!({ "info": x.info.as_ref().map(info_json) })),
        );
    }
    if let Some(x) = &p.unsubscribe {
        o = o.val(
            "unsubscribe",
            Some(
                Obj::new()
                    .num("code", x.code.into())
                    .str("reason", &x.reason)
                    .done(),
            ),
        );
    }
    if let Some(x) = &p.message {
        o = o.val("message", Some(Obj::new().raw("data", &x.data).done()));
    }
    if let Some(x) = &p.subscribe {
        o = o.val(
            "subscribe",
            Some(
                Obj::new()
                    .flag("recoverable", x.recoverable)
                    .str("epoch", &x.epoch)
                    .num("offset", x.offset)
                    .flag("positioned", x.positioned)
                    .raw("data", &x.data)
                    .done(),
            ),
        );
    }
    if let Some(x) = &p.connect {
        o = o.val(
            "connect",
            Some(
                Obj::new()
                    .str("client", &x.client)
                    .str("version", &x.version)
                    .raw("data", &x.data)
                    .val("subs", subs_json(&x.subs))
                    .flag("expires", x.expires)
                    .num("ttl", x.ttl.into())
                    .num("ping", x.ping.into())
                    .flag("pong", x.pong)
                    .str("session", &x.session)
                    .str("node", &x.node)
                    .done(),
            ),
        );
    }
    if let Some(x) = &p.disconnect {
        o = o.val(
            "disconnect",
            Some(
                Obj::new()
                    .num("code", x.code.into())
                    .str("reason", &x.reason)
                    .flag("reconnect", x.reconnect)
                    .done(),
            ),
        );
    }
    if let Some(x) = &p.refresh {
        o = o.val(
            "refresh",
            Some(
                Obj::new()
                    .flag("expires", x.expires)
                    .num("ttl", x.ttl.into())
                    .done(),
            ),
        );
    }
    o.done()
}

pub(crate) fn reply_to_json(r: &proto::Reply) -> Value {
    let mut o = Obj::new().num("id", r.id.into());
    if let Some(e) = &r.error {
        o = o.val(
            "error",
            Some(
                Obj::new()
                    .num("code", e.code.into())
                    .str("message", &e.message)
                    .flag("temporary", e.temporary)
                    .done(),
            ),
        );
    }
    if let Some(p) = &r.push {
        o = o.val("push", Some(push_json(p)));
    }
    if let Some(x) = &r.connect {
        o = o.val("connect", Some(connect_json(x)));
    }
    if let Some(x) = &r.subscribe {
        o = o.val("subscribe", Some(sub_result_json(x)));
    }
    if r.unsubscribe.is_some() {
        o = o.val("unsubscribe", Some(json!({})));
    }
    if r.publish.is_some() {
        o = o.val("publish", Some(json!({})));
    }
    if let Some(x) = &r.presence {
        let m: Map<String, Value> = x
            .presence
            .iter()
            .map(|(k, v)| (k.clone(), info_json(v)))
            .collect();
        o = o.val("presence", Some(json!({ "presence": m })));
    }
    if let Some(x) = &r.presence_stats {
        o = o.val(
            "presence_stats",
            Some(
                Obj::new()
                    .num("num_clients", x.num_clients.into())
                    .num("num_users", x.num_users.into())
                    .done(),
            ),
        );
    }
    if let Some(x) = &r.history {
        o = o.val(
            "history",
            Some(
                Obj::new()
                    .val("publications", pubs_json(&x.publications))
                    .str("epoch", &x.epoch)
                    .num("offset", x.offset)
                    .done(),
            ),
        );
    }
    if r.ping.is_some() {
        o = o.val("ping", Some(json!({})));
    }
    if let Some(x) = &r.rpc {
        o = o.val("rpc", Some(Obj::new().raw("data", &x.data).done()));
    }
    if let Some(x) = &r.refresh {
        o = o.val(
            "refresh",
            Some(
                Obj::new()
                    .str("client", &x.client)
                    .str("version", &x.version)
                    .flag("expires", x.expires)
                    .num("ttl", x.ttl.into())
                    .done(),
            ),
        );
    }
    if let Some(x) = &r.sub_refresh {
        o = o.val(
            "sub_refresh",
            Some(
                Obj::new()
                    .flag("expires", x.expires)
                    .num("ttl", x.ttl.into())
                    .done(),
            ),
        );
    }
    o.done()
}
