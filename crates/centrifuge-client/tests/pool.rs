//! The rpc pool against the fake server: admission, the line, the slots, the
//! idle close and the close of the pool.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::time::Duration;

use centrifuge::{
    Client, Error, Protocol, RpcPool, RpcPoolCallOptions, RpcPoolOptions, RpcPoolStats,
    TransportKind,
};
use common::*;

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
