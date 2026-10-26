//! A terminal chat: lines typed are published, messages of others printed.
//! Typed JSON payloads with serde.
//!
//! ```sh
//! cargo run --example chat -- alice      # and in another terminal: -- bob
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use centrifuge::Client;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, BufReader};

#[derive(Serialize, Deserialize)]
struct Message {
    from: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let me = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "anonymous".into());
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let client = Client::builder(url).build()?;

    let who = me.clone();
    let room = client
        .new_subscription("recovery:chat")
        .recoverable(true)
        .on_publication(move |_, e| match e.json::<Message>() {
            Ok(m) if m.from != who => println!("<{}> {}", m.from, m.text),
            Ok(_) => {}
            Err(err) => eprintln!("unexpected payload: {err}"),
        })
        .build()?;
    room.subscribe();
    client.connect()?;
    room.ready(None).await?;
    println!("joined as {me}; type and press enter (Ctrl-D quits)");

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(text) = lines.next_line().await? {
        // Waits for the connection while reconnecting, up to the timeout.
        if let Err(e) = room
            .publish_json(&Message {
                from: me.clone(),
                text,
            })
            .await
        {
            eprintln!("not sent: {e}");
        }
    }
    client.close().await;
    Ok(())
}
