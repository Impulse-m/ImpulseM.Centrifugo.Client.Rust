//! Tests of the rpc, pool, publish and event functions of the C ABI on a client
//! that never reaches a server, so every outcome is local and deterministic.

#![allow(unsafe_code)]

use std::ffi::{CString, c_void};
use std::mem::size_of;
use std::ptr;
use std::sync::mpsc::{Receiver, SendError, Sender, channel};
use std::time::Duration;

use centrifuge_fake_server::{FakeConfig, FakeServer, proto};
use impulse_centrifugo_ffi::{
    ImpulseCentrifugoClient, ImpulseCentrifugoConfig, ImpulseCentrifugoEndpoint,
    ImpulseCentrifugoEvent, ImpulseCentrifugoEventKind, ImpulseCentrifugoPool,
    ImpulseCentrifugoPoolConfig, ImpulseCentrifugoResult, ImpulseCentrifugoRpcRequest,
    ImpulseCentrifugoStatus, impulse_centrifugo_client_connect, impulse_centrifugo_client_create,
    impulse_centrifugo_client_destroy, impulse_centrifugo_client_set_event_callback,
    impulse_centrifugo_pool_create, impulse_centrifugo_pool_destroy, impulse_centrifugo_pool_rpc,
    impulse_centrifugo_pool_rpc_cancel, impulse_centrifugo_publish, impulse_centrifugo_rpc,
    impulse_centrifugo_rpc_cancel,
};
use tokio::runtime::Runtime;

/// How long a test waits for a completion that must arrive.
const WAIT: Duration = Duration::from_secs(10);

/// How long a test waits to confirm that no second completion arrives.
const QUIET: Duration = Duration::from_millis(200);

/// Records the status of every completion on the channel behind `user`.
unsafe extern "C" fn record_completion(user: *mut c_void, result: *const ImpulseCentrifugoResult) {
    // SAFETY: the test passes a leaked `Sender<i32>` that outlives every completion.
    let sender: &Sender<i32> = unsafe { &*user.cast::<Sender<i32>>() };
    // SAFETY: the library passes a result that is valid for the call.
    let status: i32 = unsafe { (*result).status };
    let _: Result<(), SendError<i32>> = sender.send(status);
}

/// Records the kind and code of every event on the channel behind `user`.
unsafe extern "C" fn record_event(user: *mut c_void, event: *const ImpulseCentrifugoEvent) {
    // SAFETY: the test passes a leaked `Sender<i32>` that outlives every event.
    let sender: &Sender<i32> = unsafe { &*user.cast::<Sender<i32>>() };
    // SAFETY: the library passes an event that is valid for the call.
    let kind: i32 = unsafe { (*event).kind as i32 };
    let _: Result<(), std::sync::mpsc::SendError<i32>> = sender.send(kind);
}

/// A leaked channel: the sender the callbacks write to, and the receiver the test
/// reads from. The sender is leaked on purpose, because a callback may run after
/// the test returns.
fn channel_user() -> (*mut c_void, Receiver<i32>) {
    let (sender, receiver): (Sender<i32>, Receiver<i32>) = channel();
    let user: *mut c_void = Box::into_raw(Box::new(sender)).cast::<c_void>();
    (user, receiver)
}

fn endpoint() -> CString {
    CString::new("ws://127.0.0.1:9/connection/websocket").expect("endpoint has no NUL")
}

/// A client with the given command timeout, not connected.
fn client_with_timeout(timeout_ms: u32) -> *mut ImpulseCentrifugoClient {
    client_for_url(&endpoint(), timeout_ms, 1)
}

/// A client for `url` with the given command timeout and subprotocol switch, not
/// connected.
fn client_for_url(
    url: &CString,
    timeout_ms: u32,
    websocket_subprotocol: u8,
) -> *mut ImpulseCentrifugoClient {
    let config: ImpulseCentrifugoConfig = ImpulseCentrifugoConfig {
        cfg_size: size_of::<ImpulseCentrifugoConfig>(),
        endpoint_url: url.as_ptr(),
        websocket_subprotocol,
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

/// Destroys a client made by `client_with_timeout`.
fn destroy_client(handle: *mut ImpulseCentrifugoClient) {
    // SAFETY: the handle is live and not used after this call.
    let status: i32 = unsafe { impulse_centrifugo_client_destroy(handle) };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
}

/// A request for `method` with no payload and no headers.
fn request_for(
    method: &CString,
    timeout_ms: u32,
    user: *mut c_void,
) -> ImpulseCentrifugoRpcRequest {
    let on_done: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoResult)> =
        Some(record_completion);
    ImpulseCentrifugoRpcRequest {
        method: method.as_ptr(),
        data: ptr::null(),
        data_len: 0,
        headers: ptr::null(),
        header_count: 0,
        timeout_ms,
        on_done,
        user,
    }
}

#[test]
fn rpc_completes_once_with_timeout_on_a_disconnected_client() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    let (user, receiver): (*mut c_void, Receiver<i32>) = channel_user();
    let method: CString = CString::new("ping").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = request_for(&method, 0, user);
    let mut call_id: u64 = 0;
    // SAFETY: the handle is live and the request and out pointer are valid for the call.
    let status: i32 =
        unsafe { impulse_centrifugo_rpc(handle, &raw const request, &raw mut call_id) };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
    assert_ne!(call_id, 0);

    let completed: i32 = receiver.recv_timeout(WAIT).expect("the completion runs");
    assert_eq!(completed, ImpulseCentrifugoStatus::Timeout as i32);
    assert!(
        receiver.recv_timeout(QUIET).is_err(),
        "the completion runs once"
    );
    destroy_client(handle);
}

#[test]
fn rpc_cancel_completes_once_with_cancelled() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(30_000);
    let (user, receiver): (*mut c_void, Receiver<i32>) = channel_user();
    let method: CString = CString::new("ping").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = request_for(&method, 0, user);
    let mut call_id: u64 = 0;
    // SAFETY: as in the test above.
    let start_status: i32 =
        unsafe { impulse_centrifugo_rpc(handle, &raw const request, &raw mut call_id) };
    assert_eq!(start_status, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is live; the call id names the call started above.
    let cancel_status: i32 = unsafe { impulse_centrifugo_rpc_cancel(handle, call_id) };
    assert_eq!(cancel_status, ImpulseCentrifugoStatus::Ok as i32);

    let completed: i32 = receiver.recv_timeout(WAIT).expect("the completion runs");
    assert_eq!(completed, ImpulseCentrifugoStatus::Cancelled as i32);
    assert!(
        receiver.recv_timeout(QUIET).is_err(),
        "the completion runs once"
    );
    destroy_client(handle);
}

#[test]
fn rpc_cancel_of_an_unknown_call_does_nothing() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    // SAFETY: the handle is live.
    let status: i32 = unsafe { impulse_centrifugo_rpc_cancel(handle, 987_654_321) };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
    destroy_client(handle);
}

#[test]
fn rpc_rejects_a_null_request_and_a_null_handle() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    // SAFETY: the null request is the case under test; the function must not dereference it.
    let null_request: i32 = unsafe { impulse_centrifugo_rpc(handle, ptr::null(), ptr::null_mut()) };
    assert_eq!(
        null_request,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );

    let method: CString = CString::new("ping").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = request_for(&method, 0, ptr::null_mut());
    // SAFETY: the null handle is the case under test.
    let null_handle: i32 =
        unsafe { impulse_centrifugo_rpc(ptr::null(), &raw const request, ptr::null_mut()) };
    assert_eq!(null_handle, ImpulseCentrifugoStatus::InvalidArgument as i32);
    destroy_client(handle);
}

#[test]
fn publish_completes_once_on_a_disconnected_client() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    let (user, receiver): (*mut c_void, Receiver<i32>) = channel_user();
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let payload: &[u8] = b"hello";
    let on_done: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoResult)> =
        Some(record_completion);
    // SAFETY: the handle, the channel and the payload are valid for the call.
    let status: i32 = unsafe {
        impulse_centrifugo_publish(
            handle,
            channel_name.as_ptr(),
            payload.as_ptr(),
            payload.len(),
            on_done,
            user,
        )
    };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);

    let completed: i32 = receiver.recv_timeout(WAIT).expect("the completion runs");
    assert_eq!(completed, ImpulseCentrifugoStatus::Timeout as i32);
    assert!(
        receiver.recv_timeout(QUIET).is_err(),
        "the completion runs once"
    );
    destroy_client(handle);
}

#[test]
fn pool_rpc_on_a_disconnected_client_completes_with_client_disconnected() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    let endpoint_key: CString = CString::new("poolFleet").expect("key has no NUL");
    let endpoint_url: CString = endpoint();
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
        timeout_ms: 50,
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
    let create_status: i32 =
        unsafe { impulse_centrifugo_pool_create(handle, &raw const pool_config, &raw mut pool) };
    assert_eq!(create_status, ImpulseCentrifugoStatus::Ok as i32);
    assert!(!pool.is_null());

    let (user, receiver): (*mut c_void, Receiver<i32>) = channel_user();
    let method: CString = CString::new("connectionIdentity").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = request_for(&method, 0, user);
    let mut call_id: u64 = 0;
    // SAFETY: the pool is live and the key, request and out pointer are valid for the call.
    let start_status: i32 = unsafe {
        impulse_centrifugo_pool_rpc(
            pool,
            endpoint_key.as_ptr(),
            &raw const request,
            &raw mut call_id,
        )
    };
    assert_eq!(start_status, ImpulseCentrifugoStatus::Ok as i32);

    let completed: i32 = receiver.recv_timeout(WAIT).expect("the completion runs");
    assert_eq!(
        completed,
        ImpulseCentrifugoStatus::ClientDisconnected as i32
    );
    assert!(
        receiver.recv_timeout(QUIET).is_err(),
        "the completion runs once"
    );

    // SAFETY: the pool is live; the unknown call id is the case under test.
    let cancel_status: i32 = unsafe { impulse_centrifugo_pool_rpc_cancel(pool, 123_456_789) };
    assert_eq!(cancel_status, ImpulseCentrifugoStatus::Ok as i32);
    // SAFETY: the pool is not used after this call.
    let destroy_status: i32 = unsafe { impulse_centrifugo_pool_destroy(pool) };
    assert_eq!(destroy_status, ImpulseCentrifugoStatus::Ok as i32);
    destroy_client(handle);
}

#[test]
fn pool_create_rejects_a_null_out_pointer_and_a_small_config() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    let pool_config: ImpulseCentrifugoPoolConfig = ImpulseCentrifugoPoolConfig {
        cfg_size: size_of::<usize>(),
        endpoints: ptr::null(),
        endpoint_count: 0,
        headers: ptr::null(),
        header_count: 0,
        name: ptr::null(),
        version: ptr::null(),
        timeout_ms: 0,
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
    // SAFETY: the null out pointer is the case under test.
    let null_out: i32 =
        unsafe { impulse_centrifugo_pool_create(handle, &raw const pool_config, ptr::null_mut()) };
    assert_eq!(null_out, ImpulseCentrifugoStatus::InvalidArgument as i32);

    let mut pool: *mut ImpulseCentrifugoPool = ptr::null_mut();
    // SAFETY: the config is readable, but its size is below this ABI's struct.
    let small: i32 =
        unsafe { impulse_centrifugo_pool_create(handle, &raw const pool_config, &raw mut pool) };
    assert_eq!(small, ImpulseCentrifugoStatus::InvalidArgument as i32);
    assert!(pool.is_null());
    destroy_client(handle);
}

#[test]
fn event_callback_accepts_a_callback_and_rejects_null() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(50);
    let (user, _): (*mut c_void, Receiver<i32>) = channel_user();
    let callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_status: i32 =
        unsafe { impulse_centrifugo_client_set_event_callback(handle, callback, user) };
    assert_eq!(set_status, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the null callback is the case under test.
    let null_status: i32 =
        unsafe { impulse_centrifugo_client_set_event_callback(handle, None, ptr::null_mut()) };
    assert_eq!(null_status, ImpulseCentrifugoStatus::InvalidArgument as i32);
    destroy_client(handle);
}

#[test]
fn publish_completes_before_a_non_runtime_destroy_returns() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(300);
    let (user, receiver): (*mut c_void, Receiver<i32>) = channel_user();
    let channel_name: CString = CString::new("news").expect("channel has no NUL");
    let payload: &[u8] = b"hello";
    let on_done: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoResult)> =
        Some(record_completion);
    // SAFETY: the handle, the channel and the payload are valid for the call.
    let status: i32 = unsafe {
        impulse_centrifugo_publish(
            handle,
            channel_name.as_ptr(),
            payload.as_ptr(),
            payload.len(),
            on_done,
            user,
        )
    };
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);

    // The publish is still pending: its deadline is 300 ms away. The destroy must not
    // return before its completion has run.
    destroy_client(handle);
    let completed: i32 = receiver
        .try_recv()
        .expect("the publish completion ran before destroy returned");
    assert!(
        completed == ImpulseCentrifugoStatus::Timeout as i32
            || completed == ImpulseCentrifugoStatus::ClientClosed as i32,
        "unexpected publish status {completed}"
    );
    assert!(
        receiver.recv_timeout(QUIET).is_err(),
        "the completion runs once"
    );
}

#[test]
fn pool_call_completes_before_a_non_runtime_client_destroy_returns() {
    let handle: *mut ImpulseCentrifugoClient = client_with_timeout(300);
    let endpoint_key: CString = CString::new("poolFleet").expect("key has no NUL");
    let endpoint_url: CString = endpoint();
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
        timeout_ms: 300,
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
    let create_status: i32 =
        unsafe { impulse_centrifugo_pool_create(handle, &raw const pool_config, &raw mut pool) };
    assert_eq!(create_status, ImpulseCentrifugoStatus::Ok as i32);

    let (user, receiver): (*mut c_void, Receiver<i32>) = channel_user();
    let method: CString = CString::new("connectionIdentity").expect("method has no NUL");
    let request: ImpulseCentrifugoRpcRequest = request_for(&method, 0, user);
    // SAFETY: the pool is live and the key, request and out pointer are valid for the call.
    let start_status: i32 = unsafe {
        impulse_centrifugo_pool_rpc(
            pool,
            endpoint_key.as_ptr(),
            &raw const request,
            ptr::null_mut(),
        )
    };
    assert_eq!(start_status, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the pool is not used after this call; its calls stay on the client.
    let destroy_pool_status: i32 = unsafe { impulse_centrifugo_pool_destroy(pool) };
    assert_eq!(destroy_pool_status, ImpulseCentrifugoStatus::Ok as i32);
    destroy_client(handle);
    receiver
        .try_recv()
        .expect("the pool call completion ran before the client destroy returned");
    assert!(
        receiver.recv_timeout(QUIET).is_err(),
        "the completion runs once"
    );
}

/// One client event as the test records it, with the borrowed fields copied.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    kind: ImpulseCentrifugoEventKind,
    code: u32,
    text: String,
    channel: String,
    data: Vec<u8>,
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
unsafe extern "C" fn record_full_event(user: *mut c_void, event: *const ImpulseCentrifugoEvent) {
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
        }
    };
    let _: Result<(), SendError<RecordedEvent>> = sender.send(recorded);
}

/// A leaked channel for the events of one client. The sender is leaked on purpose,
/// because a callback may run after the test returns.
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

/// A fake server whose connect reply lists the `news` channel as a server-side
/// subscription, so its publications reach the client as publication events.
fn news_server(runtime: &Runtime) -> FakeServer {
    let mut connect_result: proto::ConnectResult = FakeConfig::default().connect_result;
    connect_result
        .subs
        .insert("news".to_owned(), proto::SubscribeResult::default());
    let config: FakeConfig = FakeConfig {
        connect_result,
        ..FakeConfig::default()
    };
    runtime.block_on(FakeServer::start(config))
}

#[test]
fn events_carry_connect_publication_and_disconnect() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = news_server(&runtime);
    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let handle: *mut ImpulseCentrifugoClient = client_for_url(&url, 2_000, 1);
    let (user, receiver): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_full_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_status: i32 =
        unsafe { impulse_centrifugo_client_set_event_callback(handle, callback, user) };
    assert_eq!(set_status, ImpulseCentrifugoStatus::Ok as i32);
    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    let connected: RecordedEvent = wait_for_event(&receiver, ImpulseCentrifugoEventKind::Connected);
    assert!(
        !connected.text.is_empty(),
        "the connected event carries the client id"
    );

    server.publish("news", "hello");
    let published: RecordedEvent =
        wait_for_event(&receiver, ImpulseCentrifugoEventKind::Publication);
    assert_eq!(published.channel, "news");
    assert_eq!(published.data, b"hello");

    server.disconnect(3500, "bye");
    let disconnected: RecordedEvent =
        wait_for_event(&receiver, ImpulseCentrifugoEventKind::Disconnected);
    assert_eq!(disconnected.code, 3500);
    assert_eq!(disconnected.text, "bye");

    destroy_client(handle);
    drop(server);
    drop(runtime);
}

#[test]
fn error_event_carries_the_error_kind_and_message() {
    let runtime: Runtime = Runtime::new().expect("a test runtime");
    let server: FakeServer = news_server(&runtime);
    server.refuse_upgrade(Some(403));
    let url: CString = CString::new(server.ws_url()).expect("url has no NUL");
    let handle: *mut ImpulseCentrifugoClient = client_for_url(&url, 2_000, 0);
    let (user, receiver): (*mut c_void, Receiver<RecordedEvent>) = event_user();
    let callback: Option<unsafe extern "C" fn(*mut c_void, *const ImpulseCentrifugoEvent)> =
        Some(record_full_event);
    // SAFETY: the handle is live; the callback and its user pointer stay valid for the test.
    let set_status: i32 =
        unsafe { impulse_centrifugo_client_set_event_callback(handle, callback, user) };
    assert_eq!(set_status, ImpulseCentrifugoStatus::Ok as i32);
    // SAFETY: the handle is live.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    let failed: RecordedEvent = wait_for_event(&receiver, ImpulseCentrifugoEventKind::Error);
    assert_eq!(failed.code, 1, "a refused upgrade is a transport error");
    assert!(!failed.text.is_empty(), "the error event carries a message");

    destroy_client(handle);
    drop(server);
    drop(runtime);
}
