//! The rpc pool against the fake server: admission, the line, the slots, the
//! idle close and the close of the pool.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use centrifuge::{
    Bytes, Client, Error, Protocol, RpcPool, RpcPoolCallOptions, RpcPoolOptions, RpcPoolOutcome,
    RpcPoolRedirect, RpcPoolStats, TransportKind,
};
use common::*;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Pool options for one endpoint URL, with the defaults otherwise.
fn options_for(url: String) -> RpcPoolOptions {
    RpcPoolOptions::new(move |_: &str| vec![url.clone()])
}

/// A main client that is connected to the fake server.
async fn connected_main(srv: &FakeServer) -> Client {
    let main: Client = builder(srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    main.connect().unwrap();
    main.ready(None).await.unwrap();
    main
}

/// Swallows every `method` request, so its call stays in flight.
fn hold_method(srv: &FakeServer, method: &'static str) {
    srv.on_command(move |received: &Received| {
        let is_method: bool = received
            .command
            .rpc
            .as_ref()
            .is_some_and(|rpc: &proto::RpcRequest| rpc.method == method);
        if is_method {
            return Some(Action::NoReply);
        }
        None
    });
}

/// Waits until one call holds a slot, so the next call finds the connection
/// full.
async fn wait_for_inflight(pool: &RpcPool) {
    for _ in 0..400 {
        if pool.stats().inflight == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("no call took a slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_call_before_the_main_client_connects_fails_with_client_disconnected() {
    let srv = server().await;
    let main: Client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    let pool: RpcPool = RpcPool::new(&main, options_for(srv.ws_url())).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::ClientDisconnected)));
    assert_eq!(pool.stats(), RpcPoolStats::default());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_call_is_answered_by_a_pooled_connection() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let pool: RpcPool = RpcPool::new(&main, options_for(srv.ws_url())).unwrap();
    let reply = pool
        .rpc("chat", "echo", "payload", RpcPoolCallOptions::default())
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), b"payload");
    assert_eq!(pool.stats().connections, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_affinity_without_the_affinity_header_fails_with_configuration_error() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let pool: RpcPool = RpcPool::new(&main, options_for(srv.ws_url())).unwrap();
    let options: RpcPoolCallOptions = RpcPoolCallOptions {
        affinity: Some("shard-1".to_owned()),
        ..Default::default()
    };
    let result = pool.rpc("chat", "echo", "{}", options).await;
    assert!(matches!(result, Err(Error::Configuration(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_call_timeout_above_the_pool_timeout_fails_with_configuration_error() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let pool: RpcPool = RpcPool::new(&main, options_for(srv.ws_url())).unwrap();
    let options: RpcPoolCallOptions = RpcPoolCallOptions {
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    let result = pool.rpc("chat", "echo", "{}", options).await;
    assert!(matches!(result, Err(Error::Configuration(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_calls_share_one_connection_while_it_has_slots() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.max_inflight_per_connection = 2;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let first = pool.rpc("chat", "echo", "one", RpcPoolCallOptions::default());
    let second = pool.rpc("chat", "echo", "two", RpcPoolCallOptions::default());
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap().data.as_ref(), b"one");
    assert_eq!(second.unwrap().data.as_ref(), b"two");
    assert_eq!(pool.stats().connections, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_full_line_refuses_a_call_with_code_16() {
    let srv = server().await;
    hold_method(&srv, "slow");
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.max_connections_per_endpoint = 1;
    options.max_queued = 0;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let held_pool: RpcPool = pool.clone();
    let held = tokio::spawn(async move {
        held_pool
            .rpc("chat", "slow", "{}", RpcPoolCallOptions::default())
            .await
    });
    wait_for_inflight(&pool).await;
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    match result {
        Err(Error::Server(error)) => assert_eq!(error.code, 16),
        other => panic!("expected code 16, got {other:?}"),
    }
    pool.close();
    let _ = held.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_waiting_call_fails_with_timeout_at_its_deadline() {
    let srv = server().await;
    hold_method(&srv, "slow");
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.max_connections_per_endpoint = 1;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let held_pool: RpcPool = pool.clone();
    let held = tokio::spawn(async move {
        held_pool
            .rpc("chat", "slow", "{}", RpcPoolCallOptions::default())
            .await
    });
    wait_for_inflight(&pool).await;
    let waiting: RpcPoolCallOptions = RpcPoolCallOptions {
        timeout: Some(Duration::from_millis(200)),
        ..Default::default()
    };
    let result = pool.rpc("chat", "echo", "{}", waiting).await;
    assert!(matches!(result, Err(Error::Timeout)));
    assert_eq!(pool.stats().queued, 0);
    pool.close();
    let _ = held.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_idle_connection_closes_after_the_idle_timeout() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.idle_timeout = Duration::from_millis(100);
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    pool.rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(pool.stats().connections, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_zero_idle_timeout_keeps_the_connection_until_close() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.idle_timeout = Duration::ZERO;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    pool.rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(pool.stats().connections, 1);
    pool.close();
    assert_eq!(pool.stats().connections, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_closed_pool_refuses_later_calls_with_client_closed() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let pool: RpcPool = RpcPool::new(&main, options_for(srv.ws_url())).unwrap();
    pool.close();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::ClientClosed)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_close_fails_a_waiting_call_with_code_17() {
    let srv = server().await;
    hold_method(&srv, "slow");
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.max_connections_per_endpoint = 1;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let held_pool: RpcPool = pool.clone();
    let held = tokio::spawn(async move {
        held_pool
            .rpc("chat", "slow", "{}", RpcPoolCallOptions::default())
            .await
    });
    wait_for_inflight(&pool).await;
    let waiting_pool: RpcPool = pool.clone();
    let waiting = tokio::spawn(async move {
        waiting_pool
            .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
            .await
    });
    for _ in 0..400 {
        if pool.stats().queued == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    pool.close();
    match waiting.await.unwrap() {
        Err(Error::Server(error)) => assert_eq!(error.code, 17),
        other => panic!("expected code 17, got {other:?}"),
    }
    assert!(held.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_call_after_the_main_client_closed_ends_the_idle_connections() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.idle_timeout = Duration::ZERO;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    pool.rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await
        .unwrap();
    assert_eq!(pool.stats().connections, 1);
    main.close().await;
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::ClientClosed)));
    assert_eq!(pool.stats().connections, 0);
}

/// Refuses every `method` call with a server error, with the given metadata,
/// and counts the calls it refuses.
fn refuse_method(
    srv: &FakeServer,
    method: &'static str,
    attempts: Arc<AtomicUsize>,
    metadata: HashMap<String, String>,
) {
    srv.on_command(move |received: &Received| {
        let is_method: bool = received
            .command
            .rpc
            .as_ref()
            .is_some_and(|rpc: &proto::RpcRequest| rpc.method == method);
        if !is_method {
            return None;
        }
        attempts.fetch_add(1, Ordering::SeqCst);
        Some(Action::Reply(proto::Reply {
            error: Some(proto::Error {
                code: 100,
                message: "refused".into(),
                temporary: true,
                metadata: metadata.clone(),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });
}

/// A local HTTP server that answers each connection with `body` under `status`,
/// and returns the URL of its one resource.
async fn serve_bytes(status: &'static str, body: &'static [u8]) -> String {
    let listener: tokio::net::TcpListener =
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: std::net::SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request: [u8; 4096] = [0; 4096];
                let _ = socket.read(&mut request).await;
                let head: String = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    format!("http://{address}/bytes")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_later_signal_resends_until_retries_run_out() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let attempts: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let metadata: HashMap<String, String> =
        HashMap::from([("x-centrifugo-retry".to_owned(), "later".to_owned())]);
    refuse_method(&srv, "echo", Arc::clone(&attempts), metadata);
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.retries = 2;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::Server(ref error)) if error.code == 100));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "one call and two resends"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_zero_retries_sends_no_resend_on_the_later_signal() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let attempts: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let metadata: HashMap<String, String> =
        HashMap::from([("x-centrifugo-retry".to_owned(), "later".to_owned())]);
    refuse_method(&srv, "echo", Arc::clone(&attempts), metadata);
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.retries = 0;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::Server(ref error)) if error.code == 100));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_call_lost_to_a_message_size_close_is_not_resent() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let attempts: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let counted: Arc<AtomicUsize> = Arc::clone(&attempts);
    srv.on_command(move |received: &Received| {
        let is_echo: bool = received
            .command
            .rpc
            .as_ref()
            .is_some_and(|rpc: &proto::RpcRequest| rpc.method == "echo");
        if !is_echo {
            return None;
        }
        counted.fetch_add(1, Ordering::SeqCst);
        Some(Action::Disconnect {
            code: 1009,
            reason: "message too big".into(),
        })
    });
    let pool: RpcPool = RpcPool::new(&main, options_for(srv.ws_url())).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::ConnectionClosed)));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a message-size close settles the call with no resend"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_later_resend_on_a_full_line_is_not_refused_with_code_16() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let attempts: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let metadata: HashMap<String, String> =
        HashMap::from([("x-centrifugo-retry".to_owned(), "later".to_owned())]);
    refuse_method(&srv, "echo", Arc::clone(&attempts), metadata);
    let mut options: RpcPoolOptions = options_for(srv.ws_url());
    options.max_connections_per_endpoint = 1;
    options.max_queued = 0;
    options.retries = 1;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(
        matches!(result, Err(Error::Server(ref error)) if error.code == 100),
        "the resend is answered by the server, not refused: {result:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "one call and one resend"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_reroute_moves_a_refused_call_to_the_key_the_hook_names() {
    let refusing: FakeServer = server().await;
    let answering: FakeServer = server().await;
    let main: Client = connected_main(&refusing).await;
    let attempts: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    refuse_method(&refusing, "echo", Arc::clone(&attempts), HashMap::new());
    let refusing_url: String = refusing.ws_url();
    let answering_url: String = answering.ws_url();
    let options: RpcPoolOptions = RpcPoolOptions::new(move |key: &str| match key {
        "first" => vec![refusing_url.clone()],
        "second" => vec![answering_url.clone()],
        _ => Vec::new(),
    })
    .reroute(|error: &centrifuge::ServerError| (error.code == 100).then(|| "second".to_owned()));
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let result = pool
        .rpc("first", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(result.is_ok(), "the moved call is answered: {result:?}");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// Waits until the lines of the pool hold `expected` calls.
async fn wait_for_queued(pool: &RpcPool, expected: usize) {
    for _ in 0..400 {
        if pool.stats().queued == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the lines did not reach {expected} calls");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_reroute_move_joins_the_front_of_the_target_line() {
    let refusing: FakeServer = server().await;
    let answering: FakeServer = server().await;
    let main: Client = connected_main(&refusing).await;
    let attempts: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    refuse_method(&refusing, "echo", Arc::clone(&attempts), HashMap::new());
    let order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded: Arc<Mutex<Vec<String>>> = Arc::clone(&order);
    answering.on_command(move |received: &Received| {
        let rpc: &proto::RpcRequest = received.command.rpc.as_ref()?;
        recorded.lock().unwrap().push(rpc.method.clone());
        if rpc.method == "hold" {
            return Some(Action::NoReply);
        }
        None
    });
    let refusing_url: String = refusing.ws_url();
    let answering_url: String = answering.ws_url();
    let mut options: RpcPoolOptions = RpcPoolOptions::new(move |key: &str| match key {
        "first" => vec![refusing_url.clone()],
        "second" => vec![answering_url.clone()],
        _ => Vec::new(),
    })
    .reroute(|error: &centrifuge::ServerError| (error.code == 100).then(|| "second".to_owned()));
    options.max_connections_per_endpoint = 1;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let holder_pool: RpcPool = pool.clone();
    let holder = tokio::spawn(async move {
        holder_pool
            .rpc(
                "second",
                "hold",
                "{}",
                RpcPoolCallOptions {
                    timeout: Some(Duration::from_secs(2)),
                    ..Default::default()
                },
            )
            .await
    });
    wait_for_inflight(&pool).await;
    let waiting_pool: RpcPool = pool.clone();
    let waiting = tokio::spawn(async move {
        waiting_pool
            .rpc("second", "wait", "{}", RpcPoolCallOptions::default())
            .await
    });
    wait_for_queued(&pool, 1).await;
    let moved_pool: RpcPool = pool.clone();
    let moved = tokio::spawn(async move {
        moved_pool
            .rpc("first", "echo", "{}", RpcPoolCallOptions::default())
            .await
    });
    wait_for_queued(&pool, 2).await;
    let holder_result = holder.await.unwrap();
    assert!(
        matches!(holder_result, Err(Error::Timeout)),
        "the holder times out and frees its slot: {holder_result:?}"
    );
    let moved_result = moved.await.unwrap();
    assert!(
        moved_result.is_ok(),
        "the moved call is answered: {moved_result:?}"
    );
    let waiting_result = waiting.await.unwrap();
    assert!(
        waiting_result.is_ok(),
        "the waiting call is answered: {waiting_result:?}"
    );
    let arrivals: Vec<String> = order.lock().unwrap().clone();
    assert_eq!(arrivals, ["hold", "echo", "wait"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_redirect_returns_the_bytes_the_hook_names() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let file_url: String = serve_bytes("200 OK", b"fetched-bytes").await;
    let options: RpcPoolOptions =
        options_for(srv.ws_url()).redirect(move |outcome: &RpcPoolOutcome| match outcome {
            RpcPoolOutcome::Reply(_) => Some(RpcPoolRedirect::new(file_url.clone())),
            _ => None,
        });
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await
        .unwrap();
    assert_eq!(result.data, Bytes::from_static(b"fetched-bytes"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_redirect_body_over_max_redirect_bytes_fails_with_code_18() {
    let srv = server().await;
    let main: Client = connected_main(&srv).await;
    let file_url: String = serve_bytes("200 OK", b"fetched-bytes").await;
    let mut options: RpcPoolOptions =
        options_for(srv.ws_url()).redirect(move |outcome: &RpcPoolOutcome| match outcome {
            RpcPoolOutcome::Reply(_) => Some(RpcPoolRedirect::new(file_url.clone())),
            _ => None,
        });
    options.max_redirect_bytes = 4;
    let pool: RpcPool = RpcPool::new(&main, options).unwrap();
    let result = pool
        .rpc("chat", "echo", "{}", RpcPoolCallOptions::default())
        .await;
    assert!(matches!(result, Err(Error::Server(ref error)) if error.code == 18));
}
