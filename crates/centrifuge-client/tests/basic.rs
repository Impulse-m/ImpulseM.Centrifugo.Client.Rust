//! Connect, subscribe, publications and calls over every transport and codec.

mod common;

use std::time::Duration;

use centrifuge::{Error, HistoryOptions, State, SubState};
use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_subscribe_publish_matrix() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        let (r, mut ev) = recorder();
        let client = record_client(builder(&srv, kind, protocol), &r)
            .build()
            .unwrap();
        let sub = record_sub(client.new_subscription("news").recoverable(true), &r)
            .build()
            .unwrap();
        sub.subscribe();
        assert_eq!(sub.state(), SubState::Subscribing);
        client.connect().unwrap();
        assert_eq!(client.state(), State::Connecting);
        client.ready(Some(Duration::from_secs(5))).await.unwrap();
        sub.ready(Some(Duration::from_secs(5))).await.unwrap();
        assert_eq!(ev.wait("connected").await, format!("connected {kind:?}"));
        ev.wait("subscribed").await;

        srv.publish("news", r#"{"n":1}"#);
        assert_eq!(
            ev.wait("pub").await,
            r#"pub 1 {"n":1}"#,
            "{kind:?} {protocol:?}"
        );

        sub.publish(r#"{"n":2}"#).await.unwrap();
        assert_eq!(ev.wait("pub").await, r#"pub 2 {"n":2}"#);

        let rpc = client.rpc("echo", r#"{"x":true}"#).await.unwrap();
        assert_eq!(&rpc.data[..], br#"{"x":true}"#);

        let h = sub.history(HistoryOptions::new().limit(10)).await.unwrap();
        assert_eq!(h.publications.len(), 2);
        assert_eq!(h.offset, 2);
        let top = sub.history(HistoryOptions::new()).await.unwrap();
        assert_eq!(top.publications, [] as [centrifuge::Publication; 0]);
        assert_eq!(top.offset, 2);

        let p = sub.presence().await.unwrap();
        assert_eq!(p.clients.len(), 1);
        let ps = sub.presence_stats().await.unwrap();
        assert_eq!(ps.num_clients, 1);

        client.send(r#"{"async":1}"#).await.unwrap();
        srv.wait_for(|r| r.command.send.is_some()).await;

        srv.message(r#"{"m":1}"#);
        assert_eq!(ev.wait("message").await, r#"message {"m":1}"#);

        let stats = client.statistics();
        assert_eq!(stats.state, State::Connected);
        assert_eq!(stats.transport, Some(kind));
        assert_eq!(stats.connects, 1);
        assert_eq!(stats.subscriptions, 1);
        assert!(stats.bytes_in > 0 && stats.bytes_out > 0);

        client.close().await;
        assert_eq!(client.state(), State::Closed);
        assert_eq!(sub.state(), SubState::Unsubscribed);
        assert!(matches!(client.connect(), Err(Error::ClientClosed)));
        let r = sub.publish("{}").await;
        assert!(matches!(r, Err(Error::ClientClosed)), "{r:?}");
        let tail = ev.drain();
        assert!(
            tail.iter().any(|e| e.starts_with("unsubscribed 2")),
            "{tail:?}"
        );
        assert!(
            tail.last().is_some_and(|e| e.starts_with("disconnected 0")),
            "{tail:?}"
        );
        srv.wait_until(|s| (s.connection_count() == 0).then_some(()))
            .await;
    }
}

/// Calls made while connecting (or before connect) wait for the connection,
/// as in centrifuge-js.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_wait_for_connect() {
    for (kind, protocol) in MATRIX {
        let srv = server().await;
        // A slow connect reply.
        let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let g = gate.clone();
        srv.on_command(move |r| {
            (r.command.connect.is_some() && g.swap(false, std::sync::atomic::Ordering::SeqCst))
                .then_some(Action::NoReply)
        });
        let client = builder(&srv, kind, protocol)
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        // Before connect(): waits for the next connect.
        let c = client.clone();
        let early = tokio::spawn(async move { c.publish("chan", r#"{"early":1}"#).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.connect().unwrap();
        let c = client.clone();
        let during = tokio::spawn(async move { c.rpc("m", r#"{"during":1}"#).await });
        // The first connect gets no reply. Drop that connection: the client
        // reconnects, and both calls, still waiting, go out once connected.
        srv.wait_for(|r| r.command.connect.is_some()).await;
        srv.terminate();
        early.await.unwrap().unwrap();
        let rpc = during.await.unwrap().unwrap();
        assert_eq!(&rpc.data[..], br#"{"during":1}"#, "{kind:?} {protocol:?}");
        client.close().await;
    }
}

/// A waiting call fails with `ClientDisconnected` on `disconnect()`, and with
/// Timeout when nothing happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_calls_fail_on_disconnect_and_timeout() {
    let srv = server().await;
    let client = builder(
        &srv,
        centrifuge::TransportKind::WebSocket,
        centrifuge::Protocol::Json,
    )
    .timeout(Duration::from_millis(300))
    .build()
    .unwrap();
    // Nobody connects: timeout.
    assert!(matches!(
        client.publish("a", "{}").await,
        Err(Error::Timeout)
    ));
    // Hold the connect reply, then disconnect(): the waiter fails at once.
    srv.hold_replies(true);
    client.connect().unwrap();
    let c = client.clone();
    let waiting = tokio::spawn(async move { c.publish("a", "{}").await });
    srv.wait_for(|r| r.command.connect.is_some()).await;
    client.disconnect();
    assert!(matches!(
        waiting.await.unwrap(),
        Err(Error::ClientDisconnected)
    ));
    // ready() fails at once while disconnected.
    assert!(matches!(
        client.ready(None).await,
        Err(Error::ClientDisconnected)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_errors_and_invalid_json() {
    let srv = server().await;
    srv.on_command(|r| {
        r.command
            .publish
            .is_some()
            .then(|| Action::error(103, "permission denied", false))
    });
    let client = builder(
        &srv,
        centrifuge::TransportKind::WebSocket,
        centrifuge::Protocol::Json,
    )
    .build()
    .unwrap();
    client.connect().unwrap();
    match client.publish("x", "{}").await {
        Err(Error::Server(e)) => assert_eq!(e.code, 103),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        client.publish("x", "not json").await,
        Err(Error::InvalidData(_))
    ));
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry() {
    let srv = server().await;
    let client = builder(
        &srv,
        centrifuge::TransportKind::WebSocket,
        centrifuge::Protocol::Protobuf,
    )
    .build()
    .unwrap();
    let a = client.new_subscription("a").build().unwrap();
    assert!(matches!(
        client.new_subscription("a").build(),
        Err(Error::DuplicateSubscription)
    ));
    assert_eq!(client.get_subscription("a"), Some(a.clone()));
    assert_eq!(client.subscriptions().len(), 1);
    client.remove_subscription(&a).unwrap();
    assert!(client.get_subscription("a").is_none());
    assert_eq!(a.state(), SubState::Unsubscribed);
    assert!(matches!(
        a.publish("x").await,
        Err(Error::SubscriptionUnsubscribed)
    ));
    // The channel can be registered again.
    let a2 = client.new_subscription("a").build().unwrap();
    assert_ne!(a, a2);
    client.remove_subscription(&a).unwrap(); // a different object: no-op
    assert!(client.get_subscription("a").is_some());
}
