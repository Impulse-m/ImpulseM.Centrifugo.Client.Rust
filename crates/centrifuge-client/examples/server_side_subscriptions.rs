//! Server-side subscriptions: channels the server subscribes the connection to
//! (from the token's `channels` claim, the connect proxy, or the server API).
//! Their events come to the client-level handlers.

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let client = Client::builder(url)
        // A token with `"channels": ["personal:42"]`, from your backend.
        .token(std::env::var("CENTRIFUGO_TOKEN").unwrap_or_default())
        .on_subscribed(|_, e| {
            println!(
                "server-side subscription to {} (recovered: {})",
                e.channel, e.info.recovered
            );
        })
        .on_publication(|_, e| println!("{}: {}", e.channel, String::from_utf8_lossy(&e.data)))
        .on_unsubscribed(|_, e| println!("server-side subscription to {} ended", e.channel))
        .build()?;
    client.connect()?;
    client.ready(Some(Duration::from_secs(5))).await?;

    // Publishing into a server-side subscription needs no client-side one.
    client
        .publish("personal:42", r#"{"hello":"me"}"#)
        .await
        .ok();
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.close().await;
    Ok(())
}
