//! An in-process fake Centrifugo for `centrifuge-client` tests and the
//! validation harness (docs/spec.md §11.2).
//!
//! It speaks both protocols over WebSocket and HTTP-streaming (with the
//! `/emulation` endpoint), answers with sensible defaults, records every
//! command, and exposes hooks, server-initiated traffic and transport faults.
//!
//! ```no_run
//! # async fn run() {
//! use centrifuge_fake_server::{FakeServer, FakeConfig};
//! let srv = FakeServer::start(FakeConfig::default()).await;
//! let url = srv.ws_url();
//! // ... connect a client to `url` ...
//! srv.publish("news", br#"{"n":1}"#.to_vec());
//! srv.wait_for(|r| r.command.subscribe.is_some()).await;
//! # }
//! ```

#![allow(missing_docs)] // test support: the field names are the protocol's
#![allow(
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::missing_errors_doc
)]
#![allow(
    clippy::needless_pass_by_value,
    clippy::large_enum_variant,
    clippy::struct_excessive_bools,
    clippy::assigning_clones,
    clippy::items_after_statements,
    clippy::single_match_else,
    clippy::map_unwrap_or
)]

mod codec;
mod server;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
pub use centrifuge_client_core::proto;
pub use codec::Protocol;
use tokio::sync::{mpsc, watch};

/// Which transport a connection uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    WebSocket,
    HttpStream,
}

/// A command the server received.
#[derive(Debug, Clone)]
pub struct Received {
    /// The connection (1, 2, … in order of arrival).
    pub conn: u64,
    pub transport: Transport,
    pub protocol: Protocol,
    pub command: proto::Command,
}

/// What a command hook does instead of the default handling.
#[derive(Debug, Clone)]
pub enum Action {
    /// Reply with this (its `id` is set to the command's).
    Reply(proto::Reply),
    /// Reply with an error.
    Error {
        code: u32,
        message: String,
        temporary: bool,
    },
    /// Don't reply.
    NoReply,
    /// Disconnect with this code (WebSocket close frame, or a disconnect push
    /// on HTTP-streaming).
    Disconnect { code: u32, reason: String },
    /// Send these frames as they are (adversarial orders), each one frame.
    Frames(Vec<Vec<proto::Reply>>),
    /// The default handling (or another action), after a delay.
    Delay(Duration, Option<Box<Action>>),
}

impl Action {
    pub fn error(code: u32, message: impl Into<String>, temporary: bool) -> Self {
        Self::Error {
            code,
            message: message.into(),
            temporary,
        }
    }
}

type CommandHook = Arc<dyn Fn(&Received) -> Option<Action> + Send + Sync>;
type SubscribeHook = Arc<
    dyn Fn(&proto::SubscribeRequest) -> Result<proto::SubscribeResult, proto::Error> + Send + Sync,
>;
type ConnectHook =
    Arc<dyn Fn(&proto::ConnectRequest) -> Result<proto::ConnectResult, proto::Error> + Send + Sync>;

/// Server settings at start; most can also change later.
#[derive(Debug, Clone)]
pub struct FakeConfig {
    /// The default connect result. An empty `client` gets a unique id.
    pub connect_result: proto::ConnectResult,
    /// Publications kept per channel for history and recovery.
    pub history_size: usize,
    /// Address to listen on (default `127.0.0.1:0`).
    pub bind: Option<SocketAddr>,
}

impl Default for FakeConfig {
    fn default() -> Self {
        Self {
            connect_result: proto::ConnectResult {
                version: "fake".into(),
                ping: 25,
                pong: true,
                ..Default::default()
            },
            history_size: 100,
            bind: None,
        }
    }
}

/// What the connection task writes.
#[derive(Debug)]
pub(crate) enum Out {
    Replies(Vec<proto::Reply>),
    Raw(Bytes),
    /// WebSocket: a close frame; HTTP-stream: a disconnect push, then EOF.
    Close(u32, String),
    /// Drop the connection abruptly.
    Terminate,
}

#[derive(Debug, Clone)]
pub(crate) struct SubInfo {
    pub(crate) delta: bool,
}

#[derive(Debug)]
pub(crate) struct Conn {
    pub(crate) transport: Transport,
    pub(crate) protocol: Protocol,
    pub(crate) tx: mpsc::UnboundedSender<Out>,
    pub(crate) connected: bool,
    pub(crate) client: String,
    pub(crate) user: String,
    pub(crate) session: String,
    pub(crate) subs: HashMap<String, SubInfo>,
}

#[derive(Debug, Default)]
pub(crate) struct Channel {
    pub(crate) epoch: String,
    pub(crate) offset: u64,
    pub(crate) pubs: VecDeque<proto::Publication>,
}

#[derive(Default)]
pub(crate) struct Faults {
    pub(crate) hold_replies: bool,
    pub(crate) refuse_upgrade: Option<u16>,
    pub(crate) hang_upgrade: bool,
    pub(crate) refuse_stream: Option<u16>,
    pub(crate) hang_stream: bool,
    pub(crate) emulation_status: Option<u16>,
    pub(crate) hang_emulation: bool,
    pub(crate) split_writes: Option<usize>,
    pub(crate) stale_close: Option<Duration>,
    pub(crate) pause_reads: Option<Duration>,
    pub(crate) html_stream: bool,
}

pub(crate) struct St {
    pub(crate) cfg: FakeConfig,
    pub(crate) on_command: Option<CommandHook>,
    pub(crate) on_subscribe: Option<SubscribeHook>,
    pub(crate) on_connect: Option<ConnectHook>,
    pub(crate) conns: BTreeMap<u64, Conn>,
    pub(crate) next_conn: u64,
    pub(crate) received: Vec<Received>,
    pub(crate) channels: HashMap<String, Channel>,
    pub(crate) faults: Faults,
    pub(crate) connects: usize,
    /// Every HTTP request: path and headers (upgrades, streams, emulation).
    pub(crate) requests: Vec<(String, hyper::HeaderMap)>,
    pub(crate) next_epoch: u64,
}

pub(crate) struct Inner {
    pub(crate) addr: SocketAddr,
    pub(crate) st: Mutex<St>,
    /// Bumped on every change tests may wait for.
    pub(crate) version: watch::Sender<u64>,
    pub(crate) accept: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Inner {
    pub(crate) fn st(&self) -> MutexGuard<'_, St> {
        self.st
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn bump(&self) {
        self.version.send_modify(|v| *v += 1);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(t) = self.accept.get_mut().ok().and_then(Option::take) {
            t.abort();
        }
    }
}

/// The fake server. Cheap to clone; it stops when the last clone is dropped.
#[derive(Clone)]
pub struct FakeServer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for FakeServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeServer")
            .field("addr", &self.inner.addr)
            .finish_non_exhaustive()
    }
}

/// Paths the fake serves.
pub const WS_PATH: &str = "/connection/websocket";
pub const HTTP_STREAM_PATH: &str = "/connection/http_stream";
pub const EMULATION_PATH: &str = "/emulation";

impl FakeServer {
    /// Starts on a random local port.
    pub async fn start(cfg: FakeConfig) -> Self {
        let bind = cfg
            .bind
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
        let listener = tokio::net::TcpListener::bind(bind)
            .await
            .expect("bind a local port");
        let addr = listener.local_addr().expect("local address");
        let inner = Arc::new(Inner {
            addr,
            st: Mutex::new(St {
                cfg,
                on_command: None,
                on_subscribe: None,
                on_connect: None,
                conns: BTreeMap::new(),
                next_conn: 1,
                received: Vec::new(),
                channels: HashMap::new(),
                faults: Faults::default(),
                connects: 0,
                requests: Vec::new(),
                next_epoch: 1,
            }),
            version: watch::Sender::new(0),
            accept: Mutex::new(None),
        });
        let weak = Arc::downgrade(&inner);
        let task = tokio::spawn(server::accept(listener, weak));
        *inner.accept.lock().expect("fresh mutex") = Some(task);
        Self { inner }
    }

    pub fn addr(&self) -> SocketAddr {
        self.inner.addr
    }

    pub fn ws_url(&self) -> String {
        format!("ws://{}{WS_PATH}", self.inner.addr)
    }

    pub fn http_stream_url(&self) -> String {
        format!("http://{}{HTTP_STREAM_PATH}", self.inner.addr)
    }

    pub fn emulation_url(&self) -> String {
        format!("http://{}{EMULATION_PATH}", self.inner.addr)
    }

    // ---- defaults and hooks ----

    pub fn set_connect_result(&self, r: proto::ConnectResult) {
        self.inner.st().cfg.connect_result = r;
    }

    /// Decides connect replies (after the command hook).
    pub fn on_connect(
        &self,
        f: impl Fn(&proto::ConnectRequest) -> Result<proto::ConnectResult, proto::Error>
        + Send
        + Sync
        + 'static,
    ) {
        self.inner.st().on_connect = Some(Arc::new(f));
    }

    /// Decides subscribe replies (after the command hook). The default
    /// recovers from the channel's history.
    pub fn on_subscribe(
        &self,
        f: impl Fn(&proto::SubscribeRequest) -> Result<proto::SubscribeResult, proto::Error>
        + Send
        + Sync
        + 'static,
    ) {
        self.inner.st().on_subscribe = Some(Arc::new(f));
    }

    /// Sees every command first; `Some(action)` replaces the default handling.
    pub fn on_command(&self, f: impl Fn(&Received) -> Option<Action> + Send + Sync + 'static) {
        self.inner.st().on_command = Some(Arc::new(f));
    }

    pub fn clear_hooks(&self) {
        let mut st = self.inner.st();
        st.on_command = None;
        st.on_subscribe = None;
        st.on_connect = None;
    }

    // ---- server-initiated traffic ----

    /// Publishes into a channel's stream (history) and to its subscribers.
    /// Returns the publication's offset.
    pub fn publish(&self, channel: &str, data: impl Into<Bytes>) -> u64 {
        self.publish_full(
            channel,
            proto::Publication {
                data: data.into(),
                ..Default::default()
            },
        )
    }

    /// Like [`publish`](Self::publish) with every publication field (tags,
    /// info, `delta`…). The offset is assigned.
    pub fn publish_full(&self, channel: &str, p: proto::Publication) -> u64 {
        server::publish(&self.inner, channel, p)
    }

    /// Sends a publication to subscribers without storing it (offset as given).
    pub fn publish_raw(&self, channel: &str, p: proto::Publication) {
        server::broadcast_pub(&self.inner, channel, &p);
    }

    /// The channel's stream position.
    pub fn stream_position(&self, channel: &str) -> (u64, String) {
        let mut st = self.inner.st();
        let ch = server::channel(&mut st, channel);
        (ch.offset, ch.epoch.clone())
    }

    /// Starts a new epoch for a channel (history lost). Subscribers can't
    /// continue: they're unsubscribed with "insufficient state" (2500, a
    /// resubscribe code), as the real server does.
    pub fn reset_stream(&self, channel: &str) {
        {
            let mut st = self.inner.st();
            let epoch = format!("e{}", st.next_epoch);
            st.next_epoch += 1;
            let ch = server::channel(&mut st, channel);
            *ch = Channel {
                epoch,
                ..Channel::default()
            };
        }
        self.unsubscribe_push(channel, 2500, "insufficient state");
    }

    pub fn join(&self, channel: &str, info: proto::ClientInfo) {
        self.push_to_subscribers(
            channel,
            proto::Push {
                join: Some(proto::Join { info: Some(info) }),
                ..Default::default()
            },
        );
    }

    pub fn leave(&self, channel: &str, info: proto::ClientInfo) {
        self.push_to_subscribers(
            channel,
            proto::Push {
                leave: Some(proto::Leave { info: Some(info) }),
                ..Default::default()
            },
        );
    }

    /// An async message to every connection.
    pub fn message(&self, data: impl Into<Bytes>) {
        self.push_all(proto::Push {
            message: Some(proto::Message { data: data.into() }),
            ..Default::default()
        });
    }

    pub fn refresh_push(&self, expires: bool, ttl: u32) {
        self.push_all(proto::Push {
            refresh: Some(proto::Refresh { expires, ttl }),
            ..Default::default()
        });
    }

    /// Unsubscribes every connection from a channel, with a push.
    pub fn unsubscribe_push(&self, channel: &str, code: u32, reason: &str) {
        let mut st = self.inner.st();
        for c in st.conns.values_mut() {
            if c.subs.remove(channel).is_some() || channel.is_empty() {
                let push = proto::Push {
                    channel: channel.to_owned(),
                    unsubscribe: Some(proto::Unsubscribe {
                        code,
                        reason: reason.to_owned(),
                    }),
                    ..Default::default()
                };
                let _ = c.tx.send(Out::Replies(vec![server::push_reply(push)]));
            }
        }
    }

    /// A server-side subscription push to every connection.
    pub fn subscribe_push(&self, channel: &str, sub: proto::Subscribe) {
        let mut st = self.inner.st();
        for c in st.conns.values_mut().filter(|c| c.connected) {
            c.subs.insert(channel.to_owned(), SubInfo { delta: false });
            let push = proto::Push {
                channel: channel.to_owned(),
                subscribe: Some(sub.clone()),
                ..Default::default()
            };
            let _ = c.tx.send(Out::Replies(vec![server::push_reply(push)]));
        }
    }

    /// Disconnects every connection with a code (close frame or push).
    pub fn disconnect(&self, code: u32, reason: &str) {
        self.send_all(|| Out::Close(code, reason.to_owned()));
    }

    /// WebSocket close frame with this code (HTTP-stream: disconnect push).
    pub fn close_with(&self, code: u32, reason: &str) {
        self.disconnect(code, reason);
    }

    /// Several replies and pushes in one frame, to every connection.
    pub fn send_frame(&self, replies: Vec<proto::Reply>) {
        self.send_all(|| Out::Replies(replies.clone()));
    }

    /// Bytes as they are, to every connection.
    pub fn send_raw(&self, data: impl Into<Bytes>) {
        let data = data.into();
        self.send_all(|| Out::Raw(data.clone()));
    }

    /// A server ping (empty reply) to every connection.
    pub fn ping(&self) {
        self.send_all(|| Out::Replies(vec![proto::Reply::default()]));
    }

    /// A graceful close (WebSocket: close frame 3000-ish normal; HTTP: EOF).
    pub fn close_connection(&self) {
        self.send_all(|| Out::Close(0, String::new()));
    }

    /// Drops every connection abruptly.
    pub fn terminate(&self) {
        self.send_all(|| Out::Terminate);
    }

    fn send_all(&self, out: impl Fn() -> Out) {
        let st = self.inner.st();
        for c in st.conns.values() {
            let _ = c.tx.send(out());
        }
    }

    fn push_all(&self, push: proto::Push) {
        let st = self.inner.st();
        for c in st.conns.values().filter(|c| c.connected) {
            let _ =
                c.tx.send(Out::Replies(vec![server::push_reply(push.clone())]));
        }
    }

    fn push_to_subscribers(&self, channel: &str, mut push: proto::Push) {
        push.channel = channel.to_owned();
        let st = self.inner.st();
        for c in st.conns.values().filter(|c| c.subs.contains_key(channel)) {
            let _ =
                c.tx.send(Out::Replies(vec![server::push_reply(push.clone())]));
        }
    }

    // ---- faults ----

    fn faults(&self, f: impl FnOnce(&mut Faults)) {
        f(&mut self.inner.st().faults);
    }

    /// Accept commands but never reply.
    pub fn hold_replies(&self, on: bool) {
        self.faults(|f| f.hold_replies = on);
    }

    /// Stop reading from clients for a while after each message.
    pub fn pause_reads(&self, d: Option<Duration>) {
        self.faults(|f| f.pause_reads = d);
    }

    /// Answer WebSocket upgrades with this HTTP status.
    pub fn refuse_upgrade(&self, status: Option<u16>) {
        self.faults(|f| f.refuse_upgrade = status);
    }

    /// Never answer WebSocket upgrades.
    pub fn hang_upgrade(&self, on: bool) {
        self.faults(|f| f.hang_upgrade = on);
    }

    /// Answer HTTP-stream requests with this status.
    pub fn refuse_stream(&self, status: Option<u16>) {
        self.faults(|f| f.refuse_stream = status);
    }

    /// Never answer HTTP-stream requests.
    pub fn hang_stream(&self, on: bool) {
        self.faults(|f| f.hang_stream = on);
    }

    /// Answer HTTP-stream requests with 200 and an HTML page (captive portal).
    pub fn html_stream(&self, on: bool) {
        self.faults(|f| f.html_stream = on);
    }

    /// Answer emulation requests with this status (commands dropped).
    pub fn emulation_status(&self, status: Option<u16>) {
        self.faults(|f| f.emulation_status = status);
    }

    /// Never answer emulation requests.
    pub fn hang_emulation(&self, on: bool) {
        self.faults(|f| f.hang_emulation = on);
    }

    /// Write HTTP-stream data in chunks of at most `n` bytes.
    pub fn split_writes(&self, n: Option<usize>) {
        self.faults(|f| f.split_writes = n);
    }

    /// Close connections that send no connect command within `d`.
    pub fn stale_close_delay(&self, d: Option<Duration>) {
        self.faults(|f| f.stale_close = d);
    }

    // ---- assertions ----

    /// Every command received, in order.
    pub fn received(&self) -> Vec<Received> {
        self.inner.st().received.clone()
    }

    /// Just the commands.
    pub fn commands(&self) -> Vec<proto::Command> {
        self.inner
            .st()
            .received
            .iter()
            .map(|r| r.command.clone())
            .collect()
    }

    pub fn clear_received(&self) {
        self.inner.st().received.clear();
    }

    pub fn last_subscribe(&self) -> Option<proto::SubscribeRequest> {
        self.inner
            .st()
            .received
            .iter()
            .rev()
            .find_map(|r| r.command.subscribe.clone())
    }

    /// Commands matching `pred`.
    pub fn count(&self, pred: impl Fn(&Received) -> bool) -> usize {
        self.inner.st().received.iter().filter(|r| pred(r)).count()
    }

    /// Every HTTP request received: path and headers (WebSocket upgrades,
    /// HTTP-stream requests, emulation requests), in order.
    pub fn requests(&self) -> Vec<(String, hyper::HeaderMap)> {
        self.inner.st().requests.clone()
    }

    /// Open connections.
    pub fn connection_count(&self) -> usize {
        self.inner.st().conns.len()
    }

    /// Connections that sent a successful connect and are still open.
    pub fn connected_count(&self) -> usize {
        self.inner
            .st()
            .conns
            .values()
            .filter(|c| c.connected)
            .count()
    }

    /// Successful connect replies sent.
    pub fn connects(&self) -> usize {
        self.inner.st().connects
    }

    /// Waits (up to 5 s) for a received command matching `pred`, and returns it.
    pub async fn wait_for(&self, pred: impl Fn(&Received) -> bool) -> Received {
        self.wait_for_timeout(Duration::from_secs(5), pred)
            .await
            .expect("timed out waiting for a command")
    }

    pub async fn wait_for_timeout(
        &self,
        timeout: Duration,
        pred: impl Fn(&Received) -> bool,
    ) -> Option<Received> {
        self.wait_until_timeout(timeout, |s| s.received().into_iter().find(|r| pred(r)))
            .await
    }

    /// Waits (up to 5 s) until `f` returns `Some`.
    pub async fn wait_until<T>(&self, f: impl Fn(&Self) -> Option<T>) -> T {
        self.wait_until_timeout(Duration::from_secs(5), f)
            .await
            .expect("timed out waiting for the fake server")
    }

    pub async fn wait_until_timeout<T>(
        &self,
        timeout: Duration,
        f: impl Fn(&Self) -> Option<T>,
    ) -> Option<T> {
        let mut rx = self.inner.version.subscribe();
        let fut = async {
            loop {
                rx.borrow_and_update();
                if let Some(v) = f(self) {
                    return v;
                }
                if rx.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::time::timeout(timeout, fut).await.ok()
    }

    /// Waits until `n` connections are connected.
    pub async fn wait_connected(&self, n: usize) {
        self.wait_until(|s| (s.connected_count() >= n).then_some(()))
            .await;
    }
}
