//! The ping watchdog and the server time of the client.
//!
//! The fake server keeps pinging on its own, so a connection that must go silent
//! is served by a JSON WebSocket server of this file. The server time needs the
//! top-level `time` of a protobuf reply, which the fake server sets through its
//! command hook.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use centrifuge::{Client, ClientBuilder, Protocol, ProxyConfig, State, TransportKind};
use common::{Events, Recorder, proto};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// The local wall clock in milliseconds since the unix epoch.
fn wall_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d: Duration| {
            i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
        })
}

/// A JSON client with a short ping slack, so a silent connection is detected in
/// about a second.
fn json_client(url: &str) -> ClientBuilder {
    Client::builder(url)
        .protocol(Protocol::Json)
        .proxy(ProxyConfig::None)
        .min_reconnect_delay(Duration::from_millis(20))
        .max_reconnect_delay(Duration::from_millis(200))
        .max_server_ping_delay(Duration::from_millis(200))
}

/// A JSON-protocol WebSocket server with ping interval 1 s. It answers each
/// connect, then sends server pings every `ping_every`, or nothing when `None`.
/// Each connection that got its connect reply is reported on the returned
/// receiver.
async fn silent_server(ping_every: Option<Duration>) -> (String, mpsc::UnboundedReceiver<()>) {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let accepted: mpsc::UnboundedSender<()> = accepted_tx.clone();
            tokio::spawn(async move {
                serve_connection(stream, ping_every, accepted).await;
            });
        }
    });
    (format!("ws://{addr}/connection/websocket"), accepted_rx)
}

async fn serve_connection(
    stream: TcpStream,
    ping_every: Option<Duration>,
    accepted: mpsc::UnboundedSender<()>,
) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let Some(Ok(Message::Text(text))) = ws.next().await else {
        return;
    };
    let command: serde_json::Value = serde_json::from_str(&text).unwrap();
    let id: u64 = command["id"].as_u64().unwrap();
    let reply: serde_json::Value = json!({
        "id": id,
        "connect": {
            "client": "silent-client",
            "version": "test",
            "ping": 1,
            "pong": false,
        },
    });
    if ws
        .send(Message::Text(format!("{reply}\n").into()))
        .await
        .is_err()
    {
        return;
    }
    let _ = accepted.send(());
    let Some(every) = ping_every else {
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                return;
            }
        }
        return;
    };
    let mut tick: tokio::time::Interval = tokio::time::interval(every);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if ws.send(Message::Text("{}\n".into())).await.is_err() {
                    return;
                }
            }
            next = ws.next() => {
                match next {
                    Some(Ok(message)) if !message.is_close() => {}
                    _ => return,
                }
            }
        }
    }
}

/// A connection that goes silent for the ping window closes with code 2 and
/// reconnects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_silent_connection_closes_with_no_ping_and_reconnects() {
    let (url, mut accepted) = silent_server(None).await;
    let (rec, mut events): (Recorder, Events) = common::recorder();
    let client: Client = common::record_client(json_client(&url), &rec)
        .build()
        .unwrap();
    client.connect().unwrap();
    events.wait("connected").await;
    let connected_at: Instant = Instant::now();
    assert_eq!(
        events.wait("connecting 2 no ping").await,
        "connecting 2 no ping"
    );
    assert!(
        connected_at.elapsed() >= Duration::from_millis(900),
        "closed before the ping window elapsed"
    );
    accepted.recv().await.expect("first connection");
    tokio::time::timeout(Duration::from_secs(5), accepted.recv())
        .await
        .expect("reconnect within 5 s")
        .expect("server still running");
    client.close().await;
}

/// Server pings inside the window keep the connection open: any item received
/// counts as activity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_server_pings_keep_the_connection_open() {
    let (url, _accepted) = silent_server(Some(Duration::from_millis(300))).await;
    let (rec, mut events): (Recorder, Events) = common::recorder();
    let client: Client = common::record_client(json_client(&url), &rec)
        .build()
        .unwrap();
    client.connect().unwrap();
    events.wait("connected").await;
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let during: Vec<String> = events.drain();
    assert!(
        !during
            .iter()
            .any(|e: &String| e.starts_with("disconnected")),
        "unexpected disconnect: {during:?}"
    );
    assert_eq!(client.state(), State::Connected);
    client.close().await;
}

/// Answers every connect with a reply whose top-level server time is `time`, or
/// without a time when `time` is 0.
fn answer_connect_with_time(srv: &common::FakeServer, time: i64) {
    srv.on_command(move |received: &common::Received| {
        received.command.connect.as_ref()?;
        Some(common::Action::Reply(proto::Reply {
            time,
            connect: Some(proto::ConnectResult {
                client: "server-time-client".into(),
                version: "fake".into(),
                ping: 25,
                pong: true,
                ..Default::default()
            }),
            ..Default::default()
        }))
    });
}

/// A connect reply carrying the server time sets the server time of the client:
/// the local clock plus the offset, one hour here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_time_reply_sets_the_offset_of_the_client() {
    let hour: i64 = 3_600_000;
    let srv = common::server().await;
    answer_connect_with_time(&srv, wall_millis() + hour);
    let client: Client = common::builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    client.connect().unwrap();
    client
        .ready(Some(Duration::from_secs(5)))
        .await
        .expect("connected");
    let expected: i64 = wall_millis() + hour;
    assert!(
        (client.server_time_millis() - expected).abs() < 5_000,
        "server time is not one hour ahead of the local clock"
    );
    client.close().await;
}

/// Without a reply that carries a time, the server time is the local time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_time_without_a_time_is_the_local_time() {
    let srv = common::server().await;
    answer_connect_with_time(&srv, 0);
    let client: Client = common::builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    assert!((client.server_time_millis() - wall_millis()).abs() < 5_000);
    client.connect().unwrap();
    client
        .ready(Some(Duration::from_secs(5)))
        .await
        .expect("connected");
    assert!((client.server_time_millis() - wall_millis()).abs() < 5_000);
    client.close().await;
}

/// A server time more than 365 days from the local clock is ignored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_time_far_from_the_local_clock_is_ignored() {
    let srv = common::server().await;
    answer_connect_with_time(&srv, wall_millis() + 400 * 86_400_000);
    let client: Client = common::builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    client.connect().unwrap();
    client
        .ready(Some(Duration::from_secs(5)))
        .await
        .expect("connected");
    assert!((client.server_time_millis() - wall_millis()).abs() < 5_000);
    client.close().await;
}
