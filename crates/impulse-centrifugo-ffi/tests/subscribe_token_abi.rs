//! Tests of the subscribe, unsubscribe and token functions of the C ABI, with a
//! fake server for the paths that need a connection.

#![allow(unsafe_code)]

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::ptr;
use std::sync::mpsc::{Receiver, SendError, Sender, channel};
use std::time::Duration;

use centrifuge_fake_server::{FakeConfig, FakeServer, proto};
use impulse_centrifugo_ffi::{
    ImpulseCentrifugoClient, ImpulseCentrifugoConfig, ImpulseCentrifugoEvent,
    ImpulseCentrifugoEventKind, ImpulseCentrifugoStatus, ImpulseCentrifugoSubscribeRequest,
    ImpulseCentrifugoTokenKind, ImpulseCentrifugoTokenOutcome, ImpulseCentrifugoTokenRequest,
    impulse_centrifugo_client_connect, impulse_centrifugo_client_create,
    impulse_centrifugo_client_destroy, impulse_centrifugo_client_set_event_callback,
    impulse_centrifugo_client_set_token_callback, impulse_centrifugo_subscribe,
    impulse_centrifugo_token_resolve, impulse_centrifugo_unsubscribe,
};
use tokio::runtime::Runtime;

/// How long a test waits for an event or a token request that must arrive.
const WAIT: Duration = Duration::from_secs(10);

/// Subscription flag: positioned.
const FLAG_POSITIONED: u32 = 1;

/// Subscription flag: recoverable.
const FLAG_RECOVERABLE: u32 = 2;

/// Subscription flag: the stream position is set.
const FLAG_HAS_POSITION: u32 = 16;

/// One client or subscription event, with the borrowed fields copied.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    kind: ImpulseCentrifugoEventKind,
    code: u32,
    text: String,
    channel: String,
    data: Vec<u8>,
    offset: u64,
}

/// One token request as the test records it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedToken {
    request_id: u64,
    kind: ImpulseCentrifugoTokenKind,
    channel: String,
}

/// Copies `len` bytes at `data`. A zero length copies nothing.
///
/// # Safety
///
/// When `len` is non-zero, `data` must point to `len` readable bytes.
unsafe fn bytes_of(data: *const u8, len: usize) -> Vec<u8> {
    if len == 0 || data.is_null() {
        return Vec::new();
    }
    // SAFETY: the caller guarantees `len` readable bytes at `data`.
    unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
}

/// Records every field of every event on the channel behind `user`.
unsafe extern "C" fn record_event(user: *mut c_void, event: *const ImpulseCentrifugoEvent) {
    // SAFETY: the test passes a leaked `Sender<RecordedEvent>` that outlives every event.
    let sender: &Sender<RecordedEvent> = unsafe { &*user.cast::<Sender<RecordedEvent>>() };
    // SAFETY: the library passes an event that is valid for the call.
    let event: ImpulseCentrifugoEvent = unsafe { *event };
    // SAFETY: the library passes the text, channel and data valid for their lengths.
    let recorded: RecordedEvent = unsafe {
        RecordedEvent {
            kind: event.kind,
            code: event.code,
            text: String::from_utf8_lossy(&bytes_of(event.text.cast::<u8>(), event.text_len))
                .into_owned(),
            channel: String::from_utf8_lossy(&bytes_of(
                event.channel.cast::<u8>(),
                event.channel_len,
            ))
            .into_owned(),
            data: bytes_of(event.data, event.data_len),
            offset: event.offset,
        }
    };
    let _: Result<(), SendError<RecordedEvent>> = sender.send(recorded);
}

/// Records every token request on the channel behind `user`.
unsafe extern "C" fn record_token(
    user: *mut c_void,
    request: *const ImpulseCentrifugoTokenRequest,
) {
    // SAFETY: the test passes a leaked `Sender<RecordedToken>` that outlives every request.
    let sender: &Sender<RecordedToken> = unsafe { &*user.cast::<Sender<RecordedToken>>() };
    // SAFETY: the library passes a request that is valid for the call.
    let request: ImpulseCentrifugoTokenRequest = unsafe { *request };
    // SAFETY: the library passes the channel valid for its length, or null.
    let channel: String = unsafe {
        String::from_utf8_lossy(&bytes_of(request.channel.cast::<u8>(), request.channel_len))
            .into_owned()
    };
    let recorded: RecordedToken = RecordedToken {
        request_id: request.request_id,
        kind: request.kind,
        channel,
    };
    let _: Result<(), SendError<RecordedToken>> = sender.send(recorded);
}

/// A leaked channel for the events of one client or subscription. The sender is
/// leaked on purpose, because a callback may run after the test returns.
fn event_user() -> (*mut c_void, Receiver<RecordedEvent>) {
    let (sender, receiver): (Sender<RecordedEvent>, Receiver<RecordedEvent>) = channel();
    let user: *mut c_void = Box::into_raw(Box::new(sender)).cast::<c_void>();
    (user, receiver)
}

/// A leaked channel for the token requests of one client, as `event_user`.
fn token_user() -> (*mut c_void, Receiver<RecordedToken>) {
    let (sender, receiver): (Sender<RecordedToken>, Receiver<RecordedToken>) = channel();
    let user: *mut c_void = Box::into_raw(Box::new(sender)).cast::<c_void>();
    (user, receiver)
}

/// Receives events until one of `kind` arrives, and returns it.
fn wait_for_event(
    receiver: &Receiver<RecordedEvent>,
    kind: ImpulseCentrifugoEventKind,
) -> RecordedEvent {
    loop {
        let event: RecordedEvent = receiver.recv_timeout(WAIT).expect("the event arrives");
        if event.kind == kind {
            return event;
        }
    }
}

/// A client for `url`, with the given command timeout, not connected.
fn client_for_url(url: &CString, timeout_ms: u32) -> *mut ImpulseCentrifugoClient {
    let config: ImpulseCentrifugoConfig = ImpulseCentrifugoConfig {
        cfg_size: size_of::<ImpulseCentrifugoConfig>(),
        endpoint_url: url.as_ptr(),
        websocket_subprotocol: 1,
        headers: ptr::null(),
        header_count: 0,
        instance: ptr::null(),
        proxy_url: ptr::null(),
        timeout_ms,
        min_reconnect_ms: 0,
        max_reconnect_ms: 0,
        max_server_ping_delay_ms: 0,
        max_pending_rpc: 0,
    };
    let mut handle: *mut ImpulseCentrifugoClient = ptr::null_mut();
    // SAFETY: the config and the out pointer are valid for the call.
    let status: i32 =
        unsafe { impulse_centrifugo_client_create(&raw const config, &raw mut handle) };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
    assert!(!handle.is_null());
    handle
}

/// A client for a fake server that never answers, with the given command timeout.
fn client_without_server(timeout_ms: u32) -> *mut ImpulseCentrifugoClient {
    let url: CString =
        CString::new("ws://127.0.0.1:9/connection/websocket").expect("endpoint has no NUL");
    client_for_url(&url, timeout_ms)
}

/// Destroys a client made by `client_for_url`.
fn destroy_client(handle: *mut ImpulseCentrifugoClient) {
    // SAFETY: the handle is live and not used after this call.
    let status: i32 = unsafe { impulse_centrifugo_client_destroy(handle) };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
}

/// A subscribe request for `channel`: positioned and recoverable, no token, no
/// recovery position, events to `user`.
fn subscribe_request(
    channel_name: &CString,
    user: *mut c_void,
) -> ImpulseCentrifugoSubscribeRequest {
    let on_event: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    ImpulseCentrifugoSubscribeRequest {
        channel: channel_name.as_ptr(),
        data: ptr::null(),
        data_len: 0,
        positioned: 1,
        recoverable: 1,
        delta_fossil: 0,
        token_callback: 0,
        since_epoch: ptr::null(),
        since_offset: 0,
        on_event,
        user,
    }
}

#[test]
fn subscribe_rejects_a_null_handle_a_null_request_and_a_null_channel() {
    let handle: *mut ImpulseCentrifugoClient = client_without_server(50);
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let request: ImpulseCentrifugoSubscribeRequest =
        subscribe_request(&channel_name, event_user().0);

    // SAFETY: the null handle is the case under test; the function must not dereference it.
    let null_handle: i32 = unsafe { impulse_centrifugo_subscribe(ptr::null(), &raw const request) };
    assert_eq!(null_handle, ImpulseCentrifugoStatus::InvalidArgument as i32);

    // SAFETY: the null request is the case under test.
    let null_request: i32 = unsafe { impulse_centrifugo_subscribe(handle, ptr::null()) };
    assert_eq!(
        null_request,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );

    let nameless: ImpulseCentrifugoSubscribeRequest = ImpulseCentrifugoSubscribeRequest {
        channel: ptr::null(),
        ..request
    };
    // SAFETY: the request is readable and its channel is the null case under test.
    let null_channel: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const nameless) };
    assert_eq!(
        null_channel,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );
    destroy_client(handle);
}

#[test]
fn subscribing_the_same_channel_twice_is_a_configuration_error() {
    let handle: *mut ImpulseCentrifugoClient = client_without_server(50);
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let request: ImpulseCentrifugoSubscribeRequest =
        subscribe_request(&channel_name, event_user().0);

    // SAFETY: the handle and the request are valid for the call.
    let first: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(first, ImpulseCentrifugoStatus::Ok as i32);
    // SAFETY: as above.
    let second: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(second, ImpulseCentrifugoStatus::ConfigurationError as i32);
    destroy_client(handle);
}

#[test]
fn unsubscribe_removes_a_subscription_and_ignores_unknown_channels() {
    let handle: *mut ImpulseCentrifugoClient = client_without_server(50);
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let request: ImpulseCentrifugoSubscribeRequest =
        subscribe_request(&channel_name, event_user().0);
    // SAFETY: the handle and the request are valid for the call.
    let subscribed: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(subscribed, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live and the channel is NUL-terminated.
    let removed: i32 = unsafe { impulse_centrifugo_unsubscribe(handle, channel_name.as_ptr()) };
    assert_eq!(removed, ImpulseCentrifugoStatus::Ok as i32);

    let other: CString = CString::new("never-subscribed").expect("channel has no NUL");
    // SAFETY: as above.
    let unknown: i32 = unsafe { impulse_centrifugo_unsubscribe(handle, other.as_ptr()) };
    assert_eq!(unknown, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live; the null channel is the case under test.
    let null_channel: i32 = unsafe { impulse_centrifugo_unsubscribe(handle, ptr::null()) };
    assert_eq!(
        null_channel,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );

    // A removed channel can be subscribed again.
    // SAFETY: the handle and the request are valid for the call.
    let again: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(again, ImpulseCentrifugoStatus::Ok as i32);
    destroy_client(handle);
}

#[test]
fn token_resolve_rejects_bad_arguments_and_ignores_unknown_requests() {
    let handle: *mut ImpulseCentrifugoClient = client_without_server(50);
    let token: CString = CString::new("tok").expect("token has no NUL");

    // SAFETY: the handle is live; the arguments are the cases under test.
    let unknown_outcome: i32 =
        unsafe { impulse_centrifugo_token_resolve(handle, 1, 99, token.as_ptr()) };
    assert_eq!(
        unknown_outcome,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );

    // SAFETY: as above.
    let token_without_text: i32 = unsafe {
        impulse_centrifugo_token_resolve(
            handle,
            1,
            ImpulseCentrifugoTokenOutcome::Token as i32,
            ptr::null(),
        )
    };
    assert_eq!(
        token_without_text,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );

    // SAFETY: as above; an answer to a request that never existed does nothing.
    let unknown_request: i32 = unsafe {
        impulse_centrifugo_token_resolve(
            handle,
            987_654_321,
            ImpulseCentrifugoTokenOutcome::Token as i32,
            token.as_ptr(),
        )
    };
    assert_eq!(unknown_request, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the null handle is the case under test.
    let null_handle: i32 = unsafe {
        impulse_centrifugo_token_resolve(
            ptr::null(),
            1,
            ImpulseCentrifugoTokenOutcome::Anonymous as i32,
            ptr::null(),
        )
    };
    assert_eq!(null_handle, ImpulseCentrifugoStatus::InvalidArgument as i32);
    destroy_client(handle);
}

#[test]
fn connection_token_comes_from_the_token_callback() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = runtime.block_on(FakeServer::start(FakeConfig::default()));
    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let handle: *mut ImpulseCentrifugoClient = client_for_url(&url, 5_000);

    let (events_user, events): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let event_callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_events: i32 = unsafe {
        impulse_centrifugo_client_set_event_callback(handle, event_callback, events_user)
    };
    assert_eq!(set_events, ImpulseCentrifugoStatus::Ok as i32);

    let (tokens_user, tokens): (*mut c_void, Receiver<RecordedToken>) = token_user();
    let token_callback: Option<
        unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoTokenRequest),
    > = Some(record_token);
    // SAFETY: as above.
    let set_tokens: i32 = unsafe {
        impulse_centrifugo_client_set_token_callback(handle, token_callback, tokens_user)
    };
    assert_eq!(set_tokens, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    let requested: RecordedToken = tokens.recv_timeout(WAIT).expect("the token is requested");
    assert_eq!(requested.kind, ImpulseCentrifugoTokenKind::Connection);
    assert!(
        requested.channel.is_empty(),
        "the connection token names no channel"
    );
    // SAFETY: the handle is live; the request id names the request above.
    let resolved: i32 = unsafe {
        impulse_centrifugo_token_resolve(
            handle,
            requested.request_id,
            ImpulseCentrifugoTokenOutcome::Token as i32,
            CString::new("connection-token")
                .expect("token has no NUL")
                .as_ptr(),
        )
    };
    assert_eq!(resolved, ImpulseCentrifugoStatus::Ok as i32);

    let connected: RecordedEvent = wait_for_event(&events, ImpulseCentrifugoEventKind::Connected);
    assert!(
        !connected.text.is_empty(),
        "the connected event carries the client id"
    );
    let sent_token: Option<String> = server
        .commands()
        .into_iter()
        .find_map(|command: proto::Command| command.connect)
        .map(|connect: proto::ConnectRequest| connect.token);
    assert_eq!(sent_token.as_deref(), Some("connection-token"));

    destroy_client(handle);
    drop(server);
    drop(runtime);
}

#[test]
fn unanswered_connection_token_fails_after_the_command_timeout() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = runtime.block_on(FakeServer::start(FakeConfig::default()));
    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let handle: *mut ImpulseCentrifugoClient = client_for_url(&url, 300);

    let (events_user, events): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let event_callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_events: i32 = unsafe {
        impulse_centrifugo_client_set_event_callback(handle, event_callback, events_user)
    };
    assert_eq!(set_events, ImpulseCentrifugoStatus::Ok as i32);

    // The callback records the request and never answers it.
    let (tokens_user, tokens): (*mut c_void, Receiver<RecordedToken>) = token_user();
    let token_callback: Option<
        unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoTokenRequest),
    > = Some(record_token);
    // SAFETY: as above.
    let set_tokens: i32 = unsafe {
        impulse_centrifugo_client_set_token_callback(handle, token_callback, tokens_user)
    };
    assert_eq!(set_tokens, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    let requested: RecordedToken = tokens.recv_timeout(WAIT).expect("the token is requested");
    assert_eq!(requested.kind, ImpulseCentrifugoTokenKind::Connection);

    let failed: RecordedEvent = wait_for_event(&events, ImpulseCentrifugoEventKind::Error);
    assert_eq!(
        failed.code, 2,
        "a token getter failure is a connect token error"
    );
    assert!(
        failed.text.contains("timed out"),
        "the error names the timeout: {}",
        failed.text
    );

    destroy_client(handle);
    drop(server);
    drop(runtime);
}

#[test]
fn subscription_recovers_missed_publications_from_the_since_offset() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = runtime.block_on(FakeServer::start(FakeConfig::default()));
    assert_eq!(server.publish("chat", "a"), 1);
    assert_eq!(server.publish("chat", "b"), 2);
    assert_eq!(server.publish("chat", "c"), 3);
    let (_, epoch): (u64, String) = server.stream_position("chat");

    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let handle: *mut ImpulseCentrifugoClient = client_for_url(&url, 5_000);
    let channel_name: CString = CString::new("chat").expect("channel has no NUL");
    let epoch_text: CString = CString::new(epoch).expect("epoch has no NUL");
    let (user, events): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let mut request: ImpulseCentrifugoSubscribeRequest = subscribe_request(&channel_name, user);
    request.since_epoch = epoch_text.as_ptr();
    request.since_offset = 1;
    // SAFETY: the handle and the request, with its strings, are valid for the call.
    let subscribed: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(subscribed, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    let subscribed_event: RecordedEvent =
        wait_for_event(&events, ImpulseCentrifugoEventKind::Subscribed);
    assert_eq!(subscribed_event.channel, "chat");
    assert_ne!(
        subscribed_event.code & FLAG_POSITIONED,
        0,
        "positioned is reported"
    );
    assert_ne!(
        subscribed_event.code & FLAG_RECOVERABLE,
        0,
        "recoverable is reported"
    );
    assert_ne!(
        subscribed_event.code & FLAG_HAS_POSITION,
        0,
        "the stream position is reported"
    );

    let first_missed: RecordedEvent =
        wait_for_event(&events, ImpulseCentrifugoEventKind::Publication);
    assert_eq!(first_missed.channel, "chat");
    assert_eq!(first_missed.data, b"b");
    assert_eq!(first_missed.offset, 2);
    let second_missed: RecordedEvent =
        wait_for_event(&events, ImpulseCentrifugoEventKind::Publication);
    assert_eq!(second_missed.data, b"c");
    assert_eq!(second_missed.offset, 3);

    // SAFETY: the handle is live; the channel is NUL-terminated.
    let removed: i32 = unsafe { impulse_centrifugo_unsubscribe(handle, channel_name.as_ptr()) };
    assert_eq!(removed, ImpulseCentrifugoStatus::Ok as i32);
    destroy_client(handle);
    drop(server);
    drop(runtime);
}

#[test]
fn subscription_token_comes_from_the_token_callback() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = runtime.block_on(FakeServer::start(FakeConfig::default()));
    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let handle: *mut ImpulseCentrifugoClient = client_for_url(&url, 5_000);

    let (tokens_user, tokens): (*mut c_void, Receiver<RecordedToken>) = token_user();
    let token_callback: Option<
        unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoTokenRequest),
    > = Some(record_token);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_tokens: i32 = unsafe {
        impulse_centrifugo_client_set_token_callback(handle, token_callback, tokens_user)
    };
    assert_eq!(set_tokens, ImpulseCentrifugoStatus::Ok as i32);

    let channel_name: CString = CString::new("private").expect("channel has no NUL");
    let (events_user, events): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let mut request: ImpulseCentrifugoSubscribeRequest =
        subscribe_request(&channel_name, events_user);
    request.token_callback = 1;
    // SAFETY: the handle and the request are valid for the call.
    let subscribed: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(subscribed, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    // The connection token is requested first, and the subscription token after it.
    loop {
        let requested: RecordedToken = tokens.recv_timeout(WAIT).expect("a token is requested");
        let (outcome, token_text): (ImpulseCentrifugoTokenOutcome, &str) =
            if requested.kind == ImpulseCentrifugoTokenKind::Subscription {
                assert_eq!(requested.channel, "private");
                (ImpulseCentrifugoTokenOutcome::Token, "subscription-token")
            } else {
                (ImpulseCentrifugoTokenOutcome::Token, "connection-token")
            };
        let token: CString = CString::new(token_text).expect("token has no NUL");
        // SAFETY: the handle is live; the request id names the request above.
        let resolved: i32 = unsafe {
            impulse_centrifugo_token_resolve(
                handle,
                requested.request_id,
                outcome as i32,
                token.as_ptr(),
            )
        };
        assert_eq!(resolved, ImpulseCentrifugoStatus::Ok as i32);
        if requested.kind == ImpulseCentrifugoTokenKind::Subscription {
            break;
        }
    }

    let subscribed_event: RecordedEvent =
        wait_for_event(&events, ImpulseCentrifugoEventKind::Subscribed);
    assert_eq!(subscribed_event.channel, "private");
    let sent_token: Option<String> = server
        .last_subscribe()
        .map(|subscribe: proto::SubscribeRequest| subscribe.token);
    assert_eq!(sent_token.as_deref(), Some("subscription-token"));

    destroy_client(handle);
    drop(server);
    drop(runtime);
}
