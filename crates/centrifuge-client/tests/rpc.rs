//! Tracked rpc calls against the fake server: a call lost with its connection is
//! sent again with the same `msg_id`, and the reply of an earlier call is
//! acknowledged by the next command.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)] // test code

mod common;

use std::time::Duration;

use centrifuge::{Error, Protocol, RpcOptions, TransportKind};
use common::*;

/// An instance that passes the 22 to 64 character rule.
const INSTANCE: &str = "rpc-test-instance-0123456789";

/// The connect reply that confirms the RPC options (flag bit 32).
fn confirmed() -> proto::ConnectResult {
    proto::ConnectResult {
        flag: 1_i64 << 32,
        ..Default::default()
    }
}

/// Whether the command is an rpc request to `method`, received on `conn`.
fn rpc_on(received: &Received, method: &str, conn: u64) -> bool {
    received.conn == conn
        && received
            .command
            .rpc
            .as_ref()
            .is_some_and(|rpc: &proto::RpcRequest| rpc.method == method)
}

/// Swallows the first `method` request of the first connection, so its call
/// stays waiting on that connection.
fn hold_first_reply(srv: &FakeServer, method: &'static str) {
    srv.on_command(move |received: &Received| {
        if rpc_on(received, method, 1) {
            return Some(Action::NoReply);
        }
        None
    });
}

/// A tracked call lost with its connection is sent again on the next one, with
/// the same `msg_id` and `retransmit` set, and its reply settles it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_tracked_call_is_resent_with_the_same_msg_id_after_reconnect() {
    let srv = server().await;
    srv.set_connect_result(confirmed());
    hold_first_reply(&srv, "slow");
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .instance(INSTANCE)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    let waiter = tokio::spawn(client.rpc_call("slow", "{}", RpcOptions::default()).wait());
    srv.wait_for(|r: &Received| rpc_on(r, "slow", 1)).await;
    srv.terminate();
    let resent: Received = srv.wait_for(|r: &Received| rpc_on(r, "slow", 2)).await;

    let first: proto::RpcRequest = srv
        .received()
        .into_iter()
        .find(|r: &Received| rpc_on(r, "slow", 1))
        .and_then(|r: Received| r.command.rpc)
        .unwrap();
    let second: proto::RpcRequest = resent.command.rpc.unwrap();
    assert!(first.msg_id > 0, "{first:?}");
    assert!(!first.retransmit, "{first:?}");
    assert_eq!(second.msg_id, first.msg_id, "{second:?}");
    assert!(second.retransmit, "{second:?}");

    let result = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the call settles")
        .expect("the task ends")
        .expect("the reply arrives");
    assert_eq!(result.data.as_ref(), b"{}");
    client.close().await;
}

/// The reply of a tracked call is acknowledged by the next command written on a
/// capable connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_reply_is_acknowledged_by_the_next_tracked_call() {
    let srv = server().await;
    srv.set_connect_result(confirmed());
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .instance(INSTANCE)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    client
        .rpc_call("echo", "{}", RpcOptions::default())
        .wait()
        .await
        .unwrap();
    client
        .rpc_call("echo", "{}", RpcOptions::default())
        .wait()
        .await
        .unwrap();

    let echoes: Vec<proto::RpcRequest> = srv
        .received()
        .into_iter()
        .filter_map(|r: Received| r.command.rpc)
        .filter(|rpc: &proto::RpcRequest| rpc.method == "echo")
        .collect();
    assert_eq!(echoes.len(), 2, "{echoes:?}");
    assert!(echoes[0].ack.is_empty(), "{:?}", echoes[0]);
    assert_eq!(echoes[1].ack, vec![echoes[0].msg_id]);
    client.close().await;
}

/// A plain call lost with its connection fails with `ConnectionClosed`: it has no
/// `msg_id`, so it is not sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_plain_call_lost_with_its_connection_fails_with_connection_closed() {
    let srv = server().await;
    hold_first_reply(&srv, "slow");
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    let waiter = tokio::spawn(client.rpc_call("slow", "{}", RpcOptions::default()).wait());
    srv.wait_for(|r: &Received| rpc_on(r, "slow", 1)).await;
    srv.terminate();
    let outcome: Result<_, Error> = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the call settles")
        .expect("the task ends");
    assert!(matches!(outcome, Err(Error::ConnectionClosed)));
    assert!(
        !srv.received()
            .iter()
            .any(|r: &Received| rpc_on(r, "slow", 2))
    );
    client.close().await;
}
