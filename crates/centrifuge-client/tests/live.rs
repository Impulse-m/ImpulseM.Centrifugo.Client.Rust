//! The live matrix against a real Centrifugo v6: the proxies (HTTP CONNECT and
//! SOCKS5, started by the test) in front of the live server, the rpc pool and
//! the close codes through a proxy. Run with `docker compose up -d` in the
//! repository root, then `cargo test --features live --test live`.
//!
//! The insecure server is on `127.0.0.1:8010` and the secure one on
//! `127.0.0.1:8012` (`docker-compose.yml`). A test whose server port is closed
//! prints why it skipped and returns. Cases that need the `ImpulseM` fork server
//! (`msg_id`, `ack`, `cancel`, `instance`, publish debounce window) are
//! `#[ignore]`d with the reason in the attribute; run them with `-- --ignored`
//! against that server.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use centrifuge::{
    Client, ClientBuilder, Error, ErrorEvent, HistoryOptions, Protocol, ProxyAuth, ProxyConfig,
    RpcCall, RpcOptions, RpcPool, RpcPoolCallOptions, RpcPoolOptions, State, TransportEndpoint,
    TransportKind,
};
use common::proxy::{Seen, http_proxy, socks5_proxy};
use common::{MATRIX, init_tracing, record_client, record_sub, recorder};

/// The insecure test server (`centrifugo` in `docker-compose.yml`).
const LIVE: &str = "127.0.0.1:8010";
/// The secure test server (`centrifugo_secure`): connection tokens required.
const LIVE_SECURE: &str = "127.0.0.1:8012";

/// Basic credentials "user:secret" in base64.
const BASIC: &str = "dXNlcjpzZWNyZXQ=";

/// Whether a test server accepts connections. Prints the reason when it does
/// not, so the caller returns early.
async fn live_up(host: &str) -> bool {
    init_tracing();
    let open =
        tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(host)).await;
    if matches!(open, Ok(Ok(_))) {
        return true;
    }
    eprintln!(
        "skipped: no Centrifugo on {host}; run `docker compose up -d` in the repository root"
    );
    false
}

fn endpoint(host: &str, kind: TransportKind) -> TransportEndpoint {
    match kind {
        TransportKind::HttpStream => {
            TransportEndpoint::http_stream(format!("http://{host}/connection/http_stream"))
        }
        _ => TransportEndpoint::websocket(format!("ws://{host}/connection/websocket")),
    }
}

fn builder(
    host: &str,
    kind: TransportKind,
    protocol: Protocol,
    proxy: ProxyConfig,
) -> ClientBuilder {
    Client::builder_with_transports([endpoint(host, kind)])
        .protocol(protocol)
        .proxy(proxy)
        .timeout(Duration::from_secs(6))
        .min_reconnect_delay(Duration::from_millis(50))
        .max_reconnect_delay(Duration::from_millis(500))
}

fn unique(prefix: &str) -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}{n}")
}

/// Subscribes, publishes through the subscription and gets the publication
/// back, for every transport and protocol pair, all through `proxy`.
async fn round_trip_through(host: &str, proxy: &ProxyConfig, label: &str) {
    for (kind, protocol) in MATRIX {
        let ch = unique("recovery:proxy");
        let (r, mut ev) = recorder();
        let client = record_client(builder(host, kind, protocol, proxy.clone()), &r)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription(&ch), &r)
            .build()
            .unwrap();
        sub.subscribe();
        client.connect().unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        sub.publish(r#"{"via":"proxy"}"#).await.unwrap();
        assert_eq!(
            ev.wait("pub").await,
            r#"pub 1 {"via":"proxy"}"#,
            "{label} {kind:?} {protocol:?}"
        );
        client.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_http_connect_proxy_matrix() {
    if !live_up(LIVE).await {
        return;
    }
    let (port, seen): (u16, Arc<Seen>) = http_proxy(Some(BASIC)).await;
    let proxy: ProxyConfig = ProxyConfig::http(
        format!("http://127.0.0.1:{port}").parse().unwrap(),
        Some(ProxyAuth::new("user", "secret")),
    );
    round_trip_through(LIVE, &proxy, "http connect").await;
    // Every tunnel names the live server; each pair opens at least one.
    assert!(
        seen.connects.load(Ordering::SeqCst) >= MATRIX.len(),
        "tunnels opened"
    );
    let reqs: Vec<String> = seen.requests.lock().unwrap().clone();
    assert!(
        reqs.iter()
            .all(|r: &String| r.starts_with("CONNECT 127.0.0.1:8010 HTTP/1.1")),
        "{reqs:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_http_connect_wrong_credentials_keeps_retrying() {
    if !live_up(LIVE).await {
        return;
    }
    let (port, _seen): (u16, Arc<Seen>) = http_proxy(Some(BASIC)).await;
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let e: Arc<Mutex<Vec<String>>> = errors.clone();
    let client: Client = Client::builder(format!("ws://{LIVE}/connection/websocket"))
        .proxy(ProxyConfig::http(
            format!("http://127.0.0.1:{port}").parse().unwrap(),
            Some(ProxyAuth::new("user", "wrong")),
        ))
        .min_reconnect_delay(Duration::from_millis(100))
        .on_error(move |_: &Client, ev: ErrorEvent| {
            e.lock().unwrap().push(format!("{:?}", ev.error.kind));
        })
        .build()
        .unwrap();
    client.connect().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_ne!(client.state(), State::Connected, "the proxy refuses");
    let seen_errors: Vec<String> = errors.lock().unwrap().clone();
    assert!(
        seen_errors
            .iter()
            .any(|x: &String| x.contains("status: Some(407)")),
        "{seen_errors:?}"
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_socks5_proxy_matrix() {
    if !live_up(LIVE).await {
        return;
    }
    let (port, seen): (u16, Arc<Seen>) = socks5_proxy(Some(("user", "secret"))).await;
    // Remote DNS sends the name `localhost`; local resolution sends an
    // address, so the address form uses 127.0.0.1.
    for (remote_dns, host) in [(true, "localhost:8010"), (false, LIVE)] {
        let proxy: ProxyConfig = ProxyConfig::socks5(
            format!("socks5://127.0.0.1:{port}").parse().unwrap(),
            Some(ProxyAuth::new("user", "secret")),
            remote_dns,
        );
        round_trip_through(host, &proxy, "socks5").await;
    }
    assert!(
        seen.connects.load(Ordering::SeqCst) >= 2 * MATRIX.len(),
        "tunnels opened"
    );
    let reqs: Vec<String> = seen.requests.lock().unwrap().clone();
    assert!(reqs.iter().any(|r: &String| r == "domain"), "{reqs:?}");
    assert!(reqs.iter().any(|r: &String| r == "ipv4"), "{reqs:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_socks5_without_auth() {
    if !live_up(LIVE).await {
        return;
    }
    let (port, seen): (u16, Arc<Seen>) = socks5_proxy(None).await;
    let proxy: ProxyConfig = ProxyConfig::socks5(
        format!("socks5://127.0.0.1:{port}").parse().unwrap(),
        None,
        false,
    );
    let ch = unique("recovery:noauth");
    let (r, mut ev) = recorder();
    let client = record_client(
        builder(LIVE, TransportKind::WebSocket, Protocol::Protobuf, proxy),
        &r,
    )
    .build()
    .unwrap();
    let sub = record_sub(client.new_subscription(&ch), &r)
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(Some(Duration::from_secs(5))).await.unwrap();
    sub.publish(r#"{"n":1}"#).await.unwrap();
    assert_eq!(ev.wait("pub").await, r#"pub 1 {"n":1}"#);
    assert!(seen.connects.load(Ordering::SeqCst) >= 1);
    client.close().await;
}

/// The close codes reach the application through a proxy: an invalid token
/// on the secure server is refused with 3500 over both transports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_proxy_keeps_the_close_code() {
    if !live_up(LIVE_SECURE).await {
        return;
    }
    let (port, _seen): (u16, Arc<Seen>) = http_proxy(None).await;
    let proxy: ProxyConfig =
        ProxyConfig::http(format!("http://127.0.0.1:{port}").parse().unwrap(), None);
    for kind in [TransportKind::WebSocket, TransportKind::HttpStream] {
        let (r, mut ev) = recorder();
        let bad: Client = record_client(
            builder(LIVE_SECURE, kind, Protocol::Protobuf, proxy.clone()).token("not-a-jwt"),
            &r,
        )
        .build()
        .unwrap();
        bad.connect().unwrap();
        let e: String = ev.wait("disconnected").await;
        assert!(e.starts_with("disconnected 3500"), "{kind:?}: {e}");
        bad.close().await;
    }
}

/// Calls of the rpc pool reach the live server. It has no rpc handler for
/// `missing`, so the server answers with an error reply, which the pool
/// returns as a server error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_rpc_pool_calls_reach_the_server() {
    if !live_up(LIVE).await {
        return;
    }
    let main: Client = Client::builder(format!("ws://{LIVE}/connection/websocket"))
        .proxy(ProxyConfig::None)
        .build()
        .unwrap();
    main.connect().unwrap();
    main.ready(Some(Duration::from_secs(5))).await.unwrap();
    // The pool fleet selects Protobuf in the query, not in the subprotocol.
    let url: String = format!("ws://{LIVE}/connection/websocket?cf_protocol=protobuf");
    let pool: RpcPool =
        RpcPool::new(&main, RpcPoolOptions::new(move |_: &str| vec![url.clone()])).unwrap();
    let reply: Result<_, Error> = pool
        .rpc("live", "missing", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(reply, Err(Error::Server(_))), "{reply:?}");
    assert!(pool.stats().connections >= 1, "{:?}", pool.stats());
    pool.close();
    main.close().await;
}

/// Tracked rpc with the instance of connect. The `msg_id`, `ack` and `cancel`
/// rules are checked against the fake server in `fork_fields.rs`. Upstream
/// Centrifugo v6 ignores those fields, so only the `ImpulseM` fork server can
/// confirm the server side of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the ImpulseM fork server: upstream Centrifugo v6 does not read the instance of connect or the msg_id, ack and cancel of rpc"]
async fn live_tracked_rpc_with_instance() {
    if !live_up(LIVE).await {
        return;
    }
    let main: Client = Client::builder(format!("ws://{LIVE}/connection/websocket"))
        .proxy(ProxyConfig::None)
        .instance("live-tracked-instance-0123456789")
        .max_pending_rpc(8)
        .build()
        .unwrap();
    main.connect().unwrap();
    main.ready(Some(Duration::from_secs(5))).await.unwrap();
    let call: RpcCall = main.rpc_call("missing", "{}", RpcOptions::default());
    let reply: Result<_, Error> = call.wait().await;
    assert!(matches!(reply, Err(Error::Server(_))), "{reply:?}");
    main.close().await;
}

/// The publish debounce window comes from the subscribe reply of the fork
/// server; upstream Centrifugo v6 sends none, so every publish is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the ImpulseM fork server: the publish debounce window is in its subscribe reply, upstream Centrifugo v6 sends none"]
async fn live_publish_debounce_coalesces_a_burst() {
    if !live_up(LIVE).await {
        return;
    }
    let ch: String = unique("recovery:debounce");
    let (r, _events) = recorder();
    let client: Client = builder(
        LIVE,
        TransportKind::WebSocket,
        Protocol::Protobuf,
        ProxyConfig::None,
    )
    .build()
    .unwrap();
    let sub = record_sub(client.new_subscription(&ch), &r)
        .build()
        .unwrap();
    sub.subscribe();
    client.connect().unwrap();
    sub.ready(Some(Duration::from_secs(5))).await.unwrap();
    for value in ["a", "b", "c"] {
        sub.publish(value).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let h = sub.history(HistoryOptions::new().all()).await.unwrap();
    assert_eq!(
        h.publications.len(),
        2,
        "the first and the last of the window"
    );
    client.close().await;
}
