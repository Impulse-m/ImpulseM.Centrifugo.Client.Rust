//! Connect, subscribe, publish, read history.
//!
//! ```sh
//! docker compose up -d      # in the repo root: Centrifugo on localhost:8010
//! cargo run --example quickstart
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::{Client, HistoryOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());

    let client = Client::builder(url)
        .on_connecting(|_, e| println!("connecting: {} ({})", e.reason, e.code))
        .on_connected(|_, e| {
            println!(
                "connected over {:?}, client id {}",
                e.transport, e.client_id
            );
        })
        .on_disconnected(|_, e| println!("disconnected: {} ({})", e.reason, e.code))
        .on_error(|_, e| eprintln!("error: {}", e.error))
        .build()?;

    let sub = client
        .new_subscription("recovery:quickstart")
        .recoverable(true)
        .on_subscribed(|s, e| {
            println!("subscribed to {} (recovered: {})", s.channel(), e.recovered);
        })
        .on_publication(|_, e| {
            println!(
                "publication {}: {}",
                e.offset,
                String::from_utf8_lossy(&e.data)
            );
        })
        .build()?;

    // Neither call waits: the subscribe goes out right after the connect reply.
    sub.subscribe();
    client.connect()?;
    sub.ready(Some(Duration::from_secs(5))).await?;

    sub.publish(r#"{"text":"hello from Rust"}"#).await?;
    let recent = sub
        .history(HistoryOptions::new().limit(10).reverse(true))
        .await?;
    println!(
        "last {} publications, stream at offset {}",
        recent.publications.len(),
        recent.offset
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    client.close().await;
    Ok(())
}
