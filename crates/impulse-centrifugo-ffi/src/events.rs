//! Events over the C ABI: the client events (connect, disconnect, background
//! errors and server-side publications), delivered to one callback per client,
//! and the subscription events, delivered to the callback of their subscription.

use std::ffi::{c_char, c_void};

use centrifuge::{
    Client, ClientErrorKind, ConnectedEvent, DisconnectedEvent, ErrorEvent, ServerPublicationEvent,
    SubscribedEvent, SubscriptionErrorKind,
};

use crate::{ImpulseCentrifugoClient, ImpulseCentrifugoStatus, UserData, guarded, status};

/// Bit of `code` in a subscribed event: the subscription is positioned.
pub(crate) const SUBSCRIBED_POSITIONED: u32 = 1;

/// Bit of `code` in a subscribed event: the subscription is recoverable.
pub(crate) const SUBSCRIBED_RECOVERABLE: u32 = 2;

/// Bit of `code` in a subscribed event: a recovery was attempted.
pub(crate) const SUBSCRIBED_WAS_RECOVERING: u32 = 4;

/// Bit of `code` in a subscribed event: every missed publication was recovered.
pub(crate) const SUBSCRIBED_RECOVERED: u32 = 8;

/// Bit of `code` in a subscribed event: `offset` and `text` carry the stream
/// position the server replied with.
pub(crate) const SUBSCRIBED_HAS_POSITION: u32 = 16;

/// Kinds of [`ImpulseCentrifugoEvent`].
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpulseCentrifugoEventKind {
    /// The client connected. `text` is the client id the server assigned.
    Connected = 1,
    /// The client disconnected and will not reconnect by itself. `code` and
    /// `text` are the disconnect code and reason.
    Disconnected = 2,
    /// A publication. On the client callback it comes from a server-side
    /// subscription; on a subscription callback it comes from that subscription.
    /// `channel`, `data` and `offset` are set.
    Publication = 3,
    /// A background client error. `code` is the error kind (1 transport, 2 connect
    /// token, 3 connect, 4 refresh, 5 configuration, 6 protocol, 7 delta, 8 handler
    /// panic, 0 other) and `text` is the message.
    Error = 4,
    /// A subscription started subscribing or resubscribing. `code` and `text` are
    /// the code and reason; `channel` is set.
    Subscribing = 5,
    /// A subscription is subscribed. `code` holds the flags of the subscribed
    /// event: positioned 1, recoverable 2, was recovering 4, recovered 8, stream
    /// position 16. With the stream position flag, `offset` is its offset and
    /// `text` its epoch. `channel` is set.
    Subscribed = 6,
    /// A subscription is unsubscribed and will not resubscribe by itself. `code`
    /// and `text` are the code and reason; `channel` is set.
    Unsubscribed = 7,
    /// A background subscription error. `code` is the error kind (1 subscribe, 2
    /// token, 3 get state, 4 refresh, 5 configuration, 0 other) and `text` is the
    /// message; `channel` is set.
    SubscriptionError = 8,
}

/// One event. Every pointer is borrowed for the duration of the callback only;
/// copy what is kept. Fields that a kind does not use are empty, zero or null.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoEvent {
    /// What happened.
    pub kind: ImpulseCentrifugoEventKind,
    /// The disconnect code, the error kind or the subscribed flags, zero otherwise.
    pub code: u32,
    /// The client id, the disconnect reason, the error message or the epoch.
    pub text: *const c_char,
    /// Length of `text` in bytes.
    pub text_len: usize,
    /// The channel of a publication or a subscription event, or null.
    pub channel: *const c_char,
    /// Length of `channel` in bytes.
    pub channel_len: usize,
    /// The payload of a publication, or null.
    pub data: *const u8,
    /// Length of `data` in bytes.
    pub data_len: usize,
    /// The offset of a publication, or the stream offset of a subscribed event
    /// with the stream position flag. Zero otherwise.
    pub offset: u64,
}

/// Called for every event. `user` is the pointer given with the callback.
pub type ImpulseCentrifugoEventCallback =
    unsafe extern "C" fn(user: *mut c_void, event: *const ImpulseCentrifugoEvent);

/// The callback of one client or one subscription, with its user pointer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventSink {
    callback: ImpulseCentrifugoEventCallback,
    user: UserData,
}

impl EventSink {
    /// A sink for `callback` with its user pointer.
    pub(crate) fn new(callback: ImpulseCentrifugoEventCallback, user: *mut c_void) -> Self {
        Self {
            callback,
            user: UserData(user),
        }
    }

    /// Delivers `event` to the callback.
    pub(crate) fn emit(self, event: &ImpulseCentrifugoEvent) {
        // SAFETY: the caller of the function that set this sink promised that `user`
        // stays valid for as long as the client or subscription lives, and the event
        // is valid for the call.
        unsafe { (self.callback)(self.user.0, event) };
    }
}

/// The borrowed fields of an event, copied into the event by `event_of`.
pub(crate) struct EventFields<'text> {
    /// The event kind.
    pub(crate) kind: ImpulseCentrifugoEventKind,
    /// The code of the event.
    pub(crate) code: u32,
    /// The text of the event.
    pub(crate) text: &'text str,
    /// The channel of the event, empty when it has none.
    pub(crate) channel: &'text str,
    /// The payload of the event, empty when it has none.
    pub(crate) data: &'text [u8],
    /// The offset of the event, zero when it has none.
    pub(crate) offset: u64,
}

/// Builds an event from its borrowed fields.
pub(crate) fn event_of(fields: &EventFields<'_>) -> ImpulseCentrifugoEvent {
    ImpulseCentrifugoEvent {
        kind: fields.kind,
        code: fields.code,
        text: fields.text.as_ptr().cast::<c_char>(),
        text_len: fields.text.len(),
        channel: fields.channel.as_ptr().cast::<c_char>(),
        channel_len: fields.channel.len(),
        data: fields.data.as_ptr(),
        data_len: fields.data.len(),
        offset: fields.offset,
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

/// The number the event reports for a background subscription error kind.
pub(crate) fn subscription_error_code(kind: &SubscriptionErrorKind) -> u32 {
    match kind {
        SubscriptionErrorKind::Subscribe => 1,
        SubscriptionErrorKind::Token => 2,
        SubscriptionErrorKind::GetState => 3,
        SubscriptionErrorKind::Refresh => 4,
        SubscriptionErrorKind::Configuration => 5,
        _ => 0,
    }
}

/// The flags of a subscribed event, as the `code` of the event reports them.
pub(crate) fn subscribed_flags(event: &SubscribedEvent) -> u32 {
    let mut flags: u32 = 0;
    if event.positioned {
        flags |= SUBSCRIBED_POSITIONED;
    }
    if event.recoverable {
        flags |= SUBSCRIBED_RECOVERABLE;
    }
    if event.was_recovering {
        flags |= SUBSCRIBED_WAS_RECOVERING;
    }
    if event.recovered {
        flags |= SUBSCRIBED_RECOVERED;
    }
    if event.stream_position.is_some() {
        flags |= SUBSCRIBED_HAS_POSITION;
    }
    flags
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
        let sink: EventSink = EventSink::new(callback, user);
        handle
            .client
            .on_connected(move |_: &Client, event: ConnectedEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Connected,
                    code: 0,
                    text: &event.client_id,
                    channel: "",
                    data: &[],
                    offset: 0,
                }));
            });
        handle
            .client
            .on_disconnected(move |_: &Client, event: DisconnectedEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Disconnected,
                    code: event.code,
                    text: &event.reason,
                    channel: "",
                    data: &[],
                    offset: 0,
                }));
            });
        handle
            .client
            .on_error(move |_: &Client, event: ErrorEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Error,
                    code: error_kind_code(&event.error.kind),
                    text: &event.error.message,
                    channel: "",
                    data: &[],
                    offset: 0,
                }));
            });
        handle
            .client
            .on_publication(move |_: &Client, event: ServerPublicationEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Publication,
                    code: 0,
                    text: "",
                    channel: &event.channel,
                    data: &event.publication.data,
                    offset: event.publication.offset,
                }));
            });
        status(ImpulseCentrifugoStatus::Ok)
    })
}
