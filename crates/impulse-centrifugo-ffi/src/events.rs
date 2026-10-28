//! Client events over the C ABI: connect, disconnect, background errors and
//! server-side publications, delivered to one callback per client.

use std::ffi::{c_char, c_void};

use centrifuge::{
    Client, ClientErrorKind, ConnectedEvent, DisconnectedEvent, ErrorEvent, ServerPublicationEvent,
};

use crate::{ImpulseCentrifugoClient, ImpulseCentrifugoStatus, UserData, guarded, status};

/// Kinds of [`ImpulseCentrifugoEvent`].
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpulseCentrifugoEventKind {
    /// The client connected. `text` is the client id the server assigned.
    Connected = 1,
    /// The client disconnected and will not reconnect by itself. `code` and
    /// `text` are the disconnect code and reason.
    Disconnected = 2,
    /// A publication in a server-side subscription. `channel` and `data` are set.
    Publication = 3,
    /// A background client error. `code` is the error kind (1 transport, 2 connect
    /// token, 3 connect, 4 refresh, 5 configuration, 6 protocol, 7 delta, 8 handler
    /// panic, 0 other) and `text` is the message.
    Error = 4,
}

/// One client event. Every pointer is borrowed for the duration of the callback
/// only; copy what is kept. Fields that a kind does not use are empty or null.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoEvent {
    /// What happened.
    pub kind: ImpulseCentrifugoEventKind,
    /// The disconnect code or the error kind, zero otherwise.
    pub code: u32,
    /// The client id, the disconnect reason or the error message.
    pub text: *const c_char,
    /// Length of `text` in bytes.
    pub text_len: usize,
    /// The channel of a publication, or null.
    pub channel: *const c_char,
    /// Length of `channel` in bytes.
    pub channel_len: usize,
    /// The payload of a publication, or null.
    pub data: *const u8,
    /// Length of `data` in bytes.
    pub data_len: usize,
}

/// Called for every client event. `user` is the pointer given with the callback.
pub type ImpulseCentrifugoEventCallback =
    unsafe extern "C" fn(user: *mut c_void, event: *const ImpulseCentrifugoEvent);

/// The callback of one client, with its user pointer.
#[derive(Debug, Clone, Copy)]
struct EventSink {
    callback: ImpulseCentrifugoEventCallback,
    user: UserData,
}

impl EventSink {
    /// Delivers `event` to the callback.
    fn emit(self, event: &ImpulseCentrifugoEvent) {
        // SAFETY: the caller of `impulse_centrifugo_client_set_event_callback` promised
        // that `user` stays valid for as long as the client lives, and the event is
        // valid for the call.
        unsafe { (self.callback)(self.user.0, event) };
    }
}

/// Builds an event from its borrowed fields.
fn event_of(
    kind: ImpulseCentrifugoEventKind,
    code: u32,
    text: &str,
    channel: &str,
    data: &[u8],
) -> ImpulseCentrifugoEvent {
    ImpulseCentrifugoEvent {
        kind,
        code,
        text: text.as_ptr().cast::<c_char>(),
        text_len: text.len(),
        channel: channel.as_ptr().cast::<c_char>(),
        channel_len: channel.len(),
        data: data.as_ptr(),
        data_len: data.len(),
    }
}

/// The number the event reports for a background error kind.
fn error_kind_code(kind: &ClientErrorKind) -> u32 {
    match kind {
        ClientErrorKind::Transport { .. } => 1,
        ClientErrorKind::ConnectToken => 2,
        ClientErrorKind::Connect => 3,
        ClientErrorKind::Refresh => 4,
        ClientErrorKind::Configuration => 5,
        ClientErrorKind::Protocol => 6,
        ClientErrorKind::Delta { .. } => 7,
        ClientErrorKind::HandlerPanicked => 8,
        _ => 0,
    }
}

/// Sets the callback that receives the events of the client. It replaces the one
/// set before. The callback runs on a runtime thread and must not block, nor call
/// into the same client.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`.
/// `callback` must be non-null, and `user` must stay valid until the client is
/// destroyed or another callback replaces this one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_client_set_event_callback(
    client: *const ImpulseCentrifugoClient,
    callback: Option<unsafe extern "C" fn(user: *mut c_void, event: *const ImpulseCentrifugoEvent)>,
    user: *mut c_void,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        let Some(callback): Option<ImpulseCentrifugoEventCallback> = callback else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        let sink: EventSink = EventSink {
            callback,
            user: UserData(user),
        };
        handle
            .client
            .on_connected(move |_: &Client, event: ConnectedEvent| {
                sink.emit(&event_of(
                    ImpulseCentrifugoEventKind::Connected,
                    0,
                    &event.client_id,
                    "",
                    &[],
                ));
            });
        handle
            .client
            .on_disconnected(move |_: &Client, event: DisconnectedEvent| {
                sink.emit(&event_of(
                    ImpulseCentrifugoEventKind::Disconnected,
                    event.code,
                    &event.reason,
                    "",
                    &[],
                ));
            });
        handle
            .client
            .on_error(move |_: &Client, event: ErrorEvent| {
                sink.emit(&event_of(
                    ImpulseCentrifugoEventKind::Error,
                    error_kind_code(&event.error.kind),
                    &event.error.message,
                    "",
                    &[],
                ));
            });
        handle
            .client
            .on_publication(move |_: &Client, event: ServerPublicationEvent| {
                sink.emit(&event_of(
                    ImpulseCentrifugoEventKind::Publication,
                    0,
                    "",
                    &event.channel,
                    &event.publication.data,
                ));
            });
        status(ImpulseCentrifugoStatus::Ok)
    })
}
