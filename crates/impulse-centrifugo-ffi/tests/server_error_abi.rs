//! Tests of server errors over the C ABI: every reply the server sends with an
//! error has the `ServerError` status and carries its code in `server_code`, and
//! no call reports a server error with status zero.

#![allow(unsafe_code)]

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::ptr;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Duration;

use centrifuge_fake_server::{Action, FakeConfig, FakeServer, Received, proto};
use impulse_centrifugo_ffi::{
    ImpulseCentrifugoClient, ImpulseCentrifugoConfig, ImpulseCentrifugoEndpoint,
    ImpulseCentrifugoEvent, ImpulseCentrifugoEventKind, ImpulseCentrifugoPool,
    ImpulseCentrifugoPoolConfig, ImpulseCentrifugoResult, ImpulseCentrifugoRpcRequest,
    ImpulseCentrifugoStatus, ImpulseCentrifugoSubscribeRequest, impulse_centrifugo_client_connect,
    impulse_centrifugo_client_create, impulse_centrifugo_client_destroy,
    impulse_centrifugo_client_set_event_callback, impulse_centrifugo_pool_create,
    impulse_centrifugo_pool_destroy, impulse_centrifugo_pool_rpc, impulse_centrifugo_publish,
    impulse_centrifugo_rpc, impulse_centrifugo_subscribe,
};
use tokio::runtime::Runtime;

/// How long a test waits for something that must arrive.
const WAIT: Duration = Duration::from_secs(10);

/// The message the fake server sends with its error replies.
const SERVER_MESSAGE: &str = "refused by the fake server";

/// One completion of a call, with the fields a caller reads.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedResult {
    status: i32,
    server_code: u32,
    message: String,
}

/// One event, with the fields a caller reads.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    kind: ImpulseCentrifugoEventKind,
    code: u32,
    server_code: u32,
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

/// Sends every completion, copied, on the channel behind `user`.
unsafe extern "C" fn record_result(user: *mut c_void, result: *const ImpulseCentrifugoResult) {
    // SAFETY: the test passes a leaked `Sender<RecordedResult>` that outlives every completion.
    let sender: &Sender<RecordedResult> = unsafe { &*user.cast::<Sender<RecordedResult>>() };
    // SAFETY: the library passes a result that is valid for the call.
    let result: ImpulseCentrifugoResult = unsafe { *result };
    // SAFETY: the library passes the message valid for its length.
    let message: Vec<u8> = unsafe { bytes_of(result.message.cast::<u8>(), result.message_len) };
    let _ = sender.send(RecordedResult {
        status: result.status,
        server_code: result.server_code,
        message: String::from_utf8_lossy(&message).into_owned(),
    });
}

/// Sends every event, copied, on the channel behind `user`.
unsafe extern "C" fn record_event(user: *mut c_void, event: *const ImpulseCentrifugoEvent) {
    // SAFETY: the test passes a leaked `Sender<RecordedEvent>` that outlives every event.
    let sender: &Sender<RecordedEvent> = unsafe { &*user.cast::<Sender<RecordedEvent>>() };
    // SAFETY: the library passes an event that is valid for the call.
    let event: ImpulseCentrifugoEvent = unsafe { *event };
    let _ = sender.send(RecordedEvent {
        kind: event.kind,
        code: event.code,
        server_code: event.server_code,
    });
}

/// A leaked channel for completions. The sender is leaked on purpose, because a
/// callback may run after the test returns.
fn result_user() -> (*mut c_void, Receiver<RecordedResult>) {
    let (sender, receiver): (Sender<RecordedResult>, Receiver<RecordedResult>) = channel();
    let user: *mut c_void = Box::into_raw(Box::new(sender)).cast::<c_void>();
    (user, receiver)
}

/// A leaked channel for events, as `result_user`.
fn event_user() -> (*mut c_void, Receiver<RecordedEvent>) {
    let (sender, receiver): (Sender<RecordedEvent>, Receiver<RecordedEvent>) = channel();
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

/// A fake server, started on `runtime`.
fn start_server(runtime: &Runtime) -> FakeServer {
    runtime.block_on(FakeServer::start(FakeConfig::default()))
}

/// A client for the server's URL with a short command timeout, not connected.
fn client_for(server: &FakeServer) -> *mut ImpulseCentrifugoClient {
    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let config: ImpulseCentrifugoConfig = ImpulseCentrifugoConfig {
        cfg_size: size_of::<ImpulseCentrifugoConfig>(),
        endpoint_url: url.as_ptr(),
        websocket_subprotocol: 1,
        headers: ptr::null(),
        header_count: 0,
        instance: ptr::null(),
        proxy_url: ptr::null(),
        timeout_ms: 2_000,
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

/// Sets an event callback on `handle` and connects it, then waits for the
/// connected event. Returns the receiver of the client's events.
fn connect_and_wait(handle: *mut ImpulseCentrifugoClient) -> Receiver<RecordedEvent> {
    let (user, receiver): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_status: i32 =
        unsafe { impulse_centrifugo_client_set_event_callback(handle, callback, user) };
    assert_eq!(set_status, ImpulseCentrifugoStatus::Ok as i32);
    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);
    wait_for_event(&receiver, ImpulseCentrifugoEventKind::Connected);
    receiver
}

/// Destroys a client made by `client_for`.
fn destroy_client(handle: *mut ImpulseCentrifugoClient) {
    // SAFETY: the handle is live and not used after this call.
    let status: i32 = unsafe { impulse_centrifugo_client_destroy(handle) };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
}

/// A request for `method` with no payload, completing into `user`.
fn rpc_request(method: &CString, user: *mut c_void) -> ImpulseCentrifugoRpcRequest {
    let on_done: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoResult)> =
        Some(record_result);
    ImpulseCentrifugoRpcRequest {
        method: method.as_ptr(),
        data: ptr::null(),
        data_len: 0,
        headers: ptr::null(),
        header_count: 0,
        timeout_ms: 0,
        on_done,
        user,
    }
}

/// A fake server whose rpc commands fail with `code`.
fn failing_rpc_server(runtime: &Runtime, code: u32) -> FakeServer {
    let server: FakeServer = start_server(runtime);
    server.on_command(move |received: &Received| {
        received
            .command
            .rpc
            .is_some()
            .then(|| Action::error(code, SERVER_MESSAGE, false))
    });
    server
}

/// Runs one rpc against a server that fails it with `code`, and returns the
/// completion of the call.
fn rpc_outcome(code: u32) -> RecordedResult {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = failing_rpc_server(&runtime, code);
    let handle: *mut ImpulseCentrifugoClient = client_for(&server);
    let connect_events: Receiver<RecordedEvent> = connect_and_wait(handle);

    let (user, results): (*mut c_void, Receiver<RecordedResult>) = result_user();
    let method: CString = CString::new("ping").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = rpc_request(&method, user);
    // SAFETY: the handle is live; the request is valid for the call.
    let start: i32 = unsafe { impulse_centrifugo_rpc(handle, &raw const request, ptr::null_mut()) };
    assert_eq!(start, ImpulseCentrifugoStatus::Ok as i32);

    let outcome: RecordedResult = results.recv_timeout(WAIT).expect("the completion runs");
    assert!(
        results.recv_timeout(Duration::from_millis(200)).is_err(),
        "the completion runs once"
    );
    destroy_client(handle);
    drop(connect_events);
    drop(server);
    drop(runtime);
    outcome
}

#[test]
fn rpc_server_error_with_code_zero_is_a_server_error_not_success() {
    let outcome: RecordedResult = rpc_outcome(0);
    assert_eq!(outcome.status, ImpulseCentrifugoStatus::ServerError as i32);
    assert_ne!(outcome.status, ImpulseCentrifugoStatus::Ok as i32);
    assert_eq!(outcome.server_code, 0);
    assert_eq!(outcome.message, SERVER_MESSAGE);
}

#[test]
fn rpc_server_error_with_code_sixteen_is_a_server_error_not_a_limit() {
    let outcome: RecordedResult = rpc_outcome(16);
    assert_eq!(outcome.status, ImpulseCentrifugoStatus::ServerError as i32);
    assert_ne!(outcome.status, ImpulseCentrifugoStatus::Limit as i32);
    assert_eq!(outcome.server_code, 16);
    assert_eq!(outcome.message, SERVER_MESSAGE);
}

#[test]
fn publish_server_error_is_a_server_error_status_with_its_code() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = start_server(&runtime);
    server.on_command(|received: &Received| {
        received
            .command
            .publish
            .is_some()
            .then(|| Action::error(0, SERVER_MESSAGE, false))
    });
    let handle: *mut ImpulseCentrifugoClient = client_for(&server);
    let connect_events: Receiver<RecordedEvent> = connect_and_wait(handle);

    let (user, results): (*mut c_void, Receiver<RecordedResult>) = result_user();
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let on_done: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoResult)> =
        Some(record_result);
    // SAFETY: the handle, the channel and the payload are valid for the call.
    let start: i32 = unsafe {
        impulse_centrifugo_publish(handle, channel_name.as_ptr(), ptr::null(), 0, on_done, user)
    };
    assert_eq!(start, ImpulseCentrifugoStatus::Ok as i32);

    let outcome: RecordedResult = results.recv_timeout(WAIT).expect("the completion runs");
    assert_eq!(outcome.status, ImpulseCentrifugoStatus::ServerError as i32);
    assert_eq!(outcome.server_code, 0);
    assert_eq!(outcome.message, SERVER_MESSAGE);
    destroy_client(handle);
    drop(connect_events);
    drop(server);
    drop(runtime);
}

#[test]
fn pool_rpc_server_error_is_a_server_error_status_with_its_code() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = failing_rpc_server(&runtime, 16);
    let handle: *mut ImpulseCentrifugoClient = client_for(&server);
    let connect_events: Receiver<RecordedEvent> = connect_and_wait(handle);

    let endpoint_key: CString = CString::new("poolFleet").expect("key has no NUL");
    let endpoint_url: CString =
        CString::new(format!("{}?cf_protocol=protobuf", server.ws_url())).expect("url has no NUL");
    let endpoints: [ImpulseCentrifugoEndpoint; 1] = [ImpulseCentrifugoEndpoint {
        key: endpoint_key.as_ptr(),
        url: endpoint_url.as_ptr(),
    }];
    let pool_config: ImpulseCentrifugoPoolConfig = ImpulseCentrifugoPoolConfig {
        cfg_size: size_of::<ImpulseCentrifugoPoolConfig>(),
        endpoints: endpoints.as_ptr(),
        endpoint_count: endpoints.len(),
        headers: ptr::null(),
        header_count: 0,
        name: ptr::null(),
        version: ptr::null(),
        timeout_ms: 2_000,
        min_reconnect_ms: 0,
        max_reconnect_ms: 0,
        max_server_ping_delay_ms: 0,
        max_connections_per_endpoint: 0,
        max_inflight_per_connection: 0,
        max_queued: 0,
        idle_timeout_ms: 0,
        retries: 0,
        token_from: 0,
    };
    let mut pool: *mut ImpulseCentrifugoPool = ptr::null_mut();
    // SAFETY: the client, the config and the out pointer are valid for the call.
    let create: i32 =
        unsafe { impulse_centrifugo_pool_create(handle, &raw const pool_config, &raw mut pool) };
    assert_eq!(create, ImpulseCentrifugoStatus::Ok as i32);

    let (user, results): (*mut c_void, Receiver<RecordedResult>) = result_user();
    let method: CString = CString::new("connectionIdentity").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = rpc_request(&method, user);
    // SAFETY: the pool is live and the key, request and out pointer are valid for the call.
    let start: i32 = unsafe {
        impulse_centrifugo_pool_rpc(
            pool,
            endpoint_key.as_ptr(),
            &raw const request,
            ptr::null_mut(),
        )
    };
    assert_eq!(start, ImpulseCentrifugoStatus::Ok as i32);

    let outcome: RecordedResult = results.recv_timeout(WAIT).expect("the completion runs");
    assert_eq!(outcome.status, ImpulseCentrifugoStatus::ServerError as i32);
    assert_eq!(outcome.server_code, 16);
    assert_eq!(outcome.message, SERVER_MESSAGE);
    // SAFETY: the pool is not used after this call.
    let destroy_pool: i32 = unsafe { impulse_centrifugo_pool_destroy(pool) };
    assert_eq!(destroy_pool, ImpulseCentrifugoStatus::Ok as i32);
    destroy_client(handle);
    drop(connect_events);
    drop(server);
    drop(runtime);
}

#[test]
fn subscribe_server_error_event_carries_the_server_code() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = start_server(&runtime);
    server.on_subscribe(|_: &proto::SubscribeRequest| {
        Err(proto::Error {
            code: 16,
            message: SERVER_MESSAGE.to_owned(),
            temporary: true,
            ..Default::default()
        })
    });
    let handle: *mut ImpulseCentrifugoClient = client_for(&server);
    let client_events: Receiver<RecordedEvent> = connect_and_wait(handle);

    let (user, events): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let on_event: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    let request: ImpulseCentrifugoSubscribeRequest = ImpulseCentrifugoSubscribeRequest {
        channel: channel_name.as_ptr(),
        data: ptr::null(),
        data_len: 0,
        positioned: 0,
        recoverable: 0,
        delta_fossil: 0,
        token_callback: 0,
        since_epoch: ptr::null(),
        since_offset: 0,
        on_event,
        user,
    };
    // SAFETY: the handle is live; the request and its pointers are valid for the call.
    let start: i32 = unsafe { impulse_centrifugo_subscribe(handle, &raw const request) };
    assert_eq!(start, ImpulseCentrifugoStatus::Ok as i32);

    let failed: RecordedEvent =
        wait_for_event(&events, ImpulseCentrifugoEventKind::SubscriptionError);
    assert_eq!(failed.code, 1, "a subscribe error is the subscribe kind");
    assert_eq!(failed.server_code, 16);
    destroy_client(handle);
    drop(client_events);
    drop(server);
    drop(runtime);
}

#[test]
fn connect_server_error_event_carries_the_server_code() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = start_server(&runtime);
    server.on_connect(|_: &proto::ConnectRequest| {
        Err(proto::Error {
            code: 100,
            message: SERVER_MESSAGE.to_owned(),
            temporary: false,
            ..Default::default()
        })
    });
    let handle: *mut ImpulseCentrifugoClient = client_for(&server);
    let (user, events): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_status: i32 =
        unsafe { impulse_centrifugo_client_set_event_callback(handle, callback, user) };
    assert_eq!(set_status, ImpulseCentrifugoStatus::Ok as i32);
    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    let failed: RecordedEvent = wait_for_event(&events, ImpulseCentrifugoEventKind::Error);
    assert_eq!(failed.code, 3, "a connect error is the connect kind");
    assert_eq!(failed.server_code, 100);
    destroy_client(handle);
    drop(server);
    drop(runtime);
}
