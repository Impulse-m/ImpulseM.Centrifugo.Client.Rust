//! Exactly-once delivery across restarts: the position of the last delivered
//! publication is saved (here to a file), and the next run subscribes `since`
//! it, so publications missed while the app wasn't running come first.
//!
//! ```sh
//! cargo run --example recovery     # run, stop, publish meanwhile, run again
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::sync::{Arc, Mutex};
use std::time::Duration;

use centrifuge::{Client, StreamPosition};

const STATE: &str = "recovery-example.position";

fn load() -> Option<StreamPosition> {
    let s = std::fs::read_to_string(STATE).ok()?;
    let (offset, epoch) = s.trim().split_once(' ')?;
    Some(StreamPosition::new(offset.parse().ok()?, epoch))
}

fn save(pos: &StreamPosition) {
    let _ = std::fs::write(STATE, format!("{} {}", pos.offset, pos.epoch));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let client = Client::builder(url).build()?;

    // Publications carry offsets; the epoch comes with the subscribe reply.
    let position = Arc::new(Mutex::new(load()));
    let (p1, p2) = (position.clone(), position.clone());
    let mut builder = client
        .new_subscription("recovery:position-demo")
        .recoverable(true)
        .on_subscribed(move |_, e| {
            if e.was_recovering && !e.recovered {
                println!(
                    "the missed publications are gone from history: reload the full state here"
                );
            }
            println!("subscribed (recovered: {})", e.recovered);
            if let Some(p) = &e.stream_position {
                let mut pos = p1.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                // After a recovery the reply carries the position recovered
                // from; the recovered publications move it on.
                *pos = Some(p.clone());
                save(p);
            }
        })
        .on_publication(move |_, e| {
            println!(
                "publication {}: {}",
                e.offset,
                String::from_utf8_lossy(&e.data)
            );
            let mut pos = p2.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(p) = pos.as_mut() {
                p.offset = e.offset;
                save(p);
            }
        });
    if let Some(pos) = load() {
        println!("resuming after offset {} (epoch {})", pos.offset, pos.epoch);
        builder = builder.since(pos);
    }
    let sub = builder.build()?;
    sub.subscribe();
    client.connect()?;
    sub.ready(Some(Duration::from_secs(5))).await?;
    sub.publish(format!(r#"{{"at":"{:?}"}}"#, std::time::SystemTime::now()))
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    client.close().await;
    Ok(())
}
