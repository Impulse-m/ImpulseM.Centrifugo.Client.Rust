//! The fork fields of the rpc and connect commands against the fake server: the
//! `msg_id` of a tracked call is run at most once and answered from the server's
//! record when it is sent again, the record is dropped when the client
//! acknowledges the answer, a cancel reaches the server for a sent call, and the
//! `instance` travels in the connect command of a tracked client only.

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
const INSTANCE: &str = "fork-fields-instance-0123456789";

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

/// Whether the command is a cancel for `msg_id`.
fn cancel_for(received: &Received, msg_id: u64) -> bool {
    received
        .command
        .rpc
        .as_ref()
        .is_some_and(|rpc: &proto::RpcRequest| rpc.cancel == msg_id)
}

/// The rpc request of the first `method` call received on `conn`.
fn first_rpc(srv: &FakeServer, method: &str, conn: u64) -> proto::RpcRequest {
    srv.received()
        .into_iter()
        .find(|r: &Received| rpc_on(r, method, conn))
        .and_then(|r: Received| r.command.rpc)
        .expect("the call was received")
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

/// A tracked call whose answer was lost after the server ran it is sent again
/// with the same `msg_id`, and the server answers from its record: the call runs
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_tracked_call_run_once_is_answered_from_the_record_after_a_lost_reply() {
    let srv = server().await;
    srv.set_connect_result(confirmed());
    srv.lose_next_rpc_replies(1);
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .instance(INSTANCE)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    let waiter = tokio::spawn(client.rpc_call("echo", "{}", RpcOptions::default()).wait());
    srv.wait_for(|r: &Received| rpc_on(r, "echo", 1)).await;
    srv.terminate();
    srv.wait_for(|r: &Received| rpc_on(r, "echo", 2)).await;

    let first: proto::RpcRequest = first_rpc(&srv, "echo", 1);
    let second: proto::RpcRequest = first_rpc(&srv, "echo", 2);
    assert!(first.msg_id > 0, "{first:?}");
    assert!(!first.retransmit, "{first:?}");
    assert_eq!(second.msg_id, first.msg_id, "{second:?}");
    assert!(second.retransmit, "{second:?}");

    let result = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the call settles")
        .expect("the task ends")
        .expect("the answer arrives");
    assert_eq!(result.data.as_ref(), b"{}");
    assert_eq!(srv.rpc_executions(first.msg_id), 1);
    client.close().await;
}

/// The answer of a tracked call stays in the server's record until the client
/// acknowledges it with its next tracked call; then only the new call is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_record_is_dropped_when_the_client_acknowledges_the_answer() {
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
    let first: proto::RpcRequest = first_rpc(&srv, "echo", 1);
    assert_eq!(srv.rpc_record_msg_ids(), vec![first.msg_id]);

    client
        .rpc_call("echo", "{}", RpcOptions::default())
        .wait()
        .await
        .unwrap();
    let second: proto::RpcRequest = srv
        .received()
        .into_iter()
        .filter_map(|r: Received| r.command.rpc)
        .filter(|rpc: &proto::RpcRequest| rpc.method == "echo")
        .last()
        .expect("the second call was received");
    assert_eq!(second.ack, vec![first.msg_id]);
    assert_eq!(srv.rpc_record_msg_ids(), vec![second.msg_id]);
    client.close().await;
}

/// Cancelling a sent tracked call fails it with code 17 and sends the server a
/// cancel for its `msg_id`; the held call never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_cancel_of_a_sent_call_sends_a_cancel_for_its_msg_id() {
    let srv = server().await;
    srv.set_connect_result(confirmed());
    hold_first_reply(&srv, "slow");
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .instance(INSTANCE)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    let call = client.rpc_call("slow", "{}", RpcOptions::default());
    let stopper = call.clone();
    let waiter = tokio::spawn(call.wait());
    srv.wait_for(|r: &Received| rpc_on(r, "slow", 1)).await;
    let msg_id: u64 = first_rpc(&srv, "slow", 1).msg_id;
    stopper.cancel();

    let outcome: Result<_, Error> = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the call settles")
        .expect("the task ends");
    assert!(
        matches!(outcome, Err(Error::Server(ref error)) if error.code == 17),
        "{outcome:?}"
    );

    let cancel: Received = srv.wait_for(|r: &Received| cancel_for(r, msg_id)).await;
    let rpc: proto::RpcRequest = cancel.command.rpc.unwrap();
    assert!(rpc.method.is_empty(), "{rpc:?}");
    assert!(rpc.data.is_empty(), "{rpc:?}");
    assert_eq!(srv.rpc_executions(msg_id), 0);
    client.close().await;
}

/// A tracked client sends its instance in the connect command.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_carries_the_instance_of_a_tracked_client() {
    let srv = server().await;
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .instance(INSTANCE)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    let connect: Received = srv
        .wait_for(|r: &Received| r.command.connect.is_some())
        .await;
    let request: proto::ConnectRequest = connect.command.connect.unwrap();
    assert_eq!(request.instance, INSTANCE);
    client.close().await;
}

/// A plain client sends no instance, and its calls carry no `msg_id`, even on a
/// connection that confirmed the RPC options: nothing is recorded for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_client_sends_no_instance_and_calls_without_msg_id() {
    let srv = server().await;
    srv.set_connect_result(confirmed());
    let client = builder(&srv, TransportKind::WebSocket, Protocol::Protobuf)
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(None).await.unwrap();

    client
        .rpc_call("echo", "{}", RpcOptions::default())
        .wait()
        .await
        .unwrap();

    let connect: Received = srv
        .wait_for(|r: &Received| r.command.connect.is_some())
        .await;
    assert!(connect.command.connect.unwrap().instance.is_empty());
    let rpc: proto::RpcRequest = first_rpc(&srv, "echo", 1);
    assert_eq!(rpc.msg_id, 0, "{rpc:?}");
    assert!(!rpc.retransmit, "{rpc:?}");
    assert!(srv.rpc_record_msg_ids().is_empty());
    client.close().await;
}
