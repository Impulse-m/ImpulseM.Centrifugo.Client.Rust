//! Events as streams instead of handlers, combined with `tokio::select!`.
//!
//! Stream items are handed off: the client's next step waits until `next()`
//! takes the item, so a stream sees the same order and guarantees as handlers.

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::{Client, ClientEvent, SubscriptionEvent};
use futures_util::StreamExt as _;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let client = Client::builder(url).build()?;
    let sub = client.new_subscription("streams-demo").build()?;

    let mut client_events = client.events();
    let mut sub_events = sub.events();
    sub.subscribe();
    client.connect()?;

    let publisher = {
        let sub = sub.clone();
        tokio::spawn(async move {
            for i in 0..3 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = sub.publish(format!(r#"{{"i":{i}}}"#)).await;
            }
        })
    };

    let deadline = tokio::time::sleep(Duration::from_secs(3));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            Some(e) = client_events.next() => match e {
                ClientEvent::Connected(e) => println!("connected ({:?})", e.transport),
                ClientEvent::Disconnected(e) => println!("disconnected: {}", e.reason),
                other => println!("client event: {other:?}"),
            },
            Some(e) = sub_events.next() => match e {
                SubscriptionEvent::Publication(p) => println!("publication: {}", String::from_utf8_lossy(&p.data)),
                other => println!("subscription event: {other:?}"),
            },
            () = &mut deadline => break,
        }
    }
    publisher.await?;
    // Streams end on close().
    let close = client.close();
    while let Some(e) = client_events.next().await {
        println!("final: {e:?}");
    }
    close.await;
    Ok(())
}
