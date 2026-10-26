//! The patterns for a desktop app (Tauri, egui, iced, …), without a GUI
//! dependency:
//!
//! - the client lives as long as the app, also with no window open;
//! - `build()` is given the runtime explicitly (`tauri::async_runtime::handle()`
//!   in Tauri setup hooks, which may run outside a runtime context);
//! - events go to the UI through a channel; handlers never block the UI;
//! - proxy and TLS follow the OS settings, overridable from app settings;
//! - after the machine wakes from sleep, the client notices on its own.
//!
//! In Tauri, replace the `ui` channel with `app.emit("centrifugo", payload)`
//! and keep the `Client` in managed state (`app.manage(client)`).

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::{Client, EventDelivery, ProxyConfig, TlsConfig};
use tokio::sync::mpsc;

enum UiEvent {
    Status(&'static str),
    Message(String),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // An app-owned runtime, like Tauri's.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (ui, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();

    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let (u1, u2, u3) = (ui.clone(), ui.clone(), ui.clone());
    let client = Client::builder(url)
        .runtime(rt.handle().clone())
        // The OS proxy settings by default; an app setting may override them,
        // also later with client.set_proxy(…).
        .proxy(ProxyConfig::System)
        // The OS trust store; a corporate CA can be added with add_root_pem().
        .tls(TlsConfig::new())
        // Handlers run on the client's own thread: a slow one never freezes the UI.
        .event_delivery(EventDelivery::Thread)
        .on_connecting(move |_, _| drop(u1.send(UiEvent::Status("connecting"))))
        .on_connected(move |_, _| drop(u2.send(UiEvent::Status("online"))))
        .on_disconnected(move |_, _| drop(u3.send(UiEvent::Status("offline"))))
        .build()?;

    let notify = ui.clone();
    let sub = client
        .new_subscription("recovery:notifications")
        .recoverable(true)
        .on_publication(move |_, e| {
            drop(notify.send(UiEvent::Message(
                String::from_utf8_lossy(&e.data).into_owned(),
            )));
        })
        .build()?;
    sub.subscribe();
    client.connect()?;

    // The "UI loop": in a real app, the GUI framework's event loop.
    rt.block_on(async {
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        while let Ok(Some(ev)) = tokio::time::timeout_at(until, ui_rx.recv()).await {
            match ev {
                UiEvent::Status(s) => println!("UI status: {s}"),
                UiEvent::Message(m) => println!("UI notification: {m}"),
            }
        }
    });

    // On app exit (a Tauri `RunEvent::Exit` handler).
    rt.block_on(client.close());
    Ok(())
}
