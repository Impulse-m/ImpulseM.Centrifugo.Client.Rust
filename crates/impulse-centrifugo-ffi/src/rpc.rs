//! Rpc calls, pool calls and publishes over the C ABI.
//!
//! Every call that starts from the ABI completes its callback exactly once: with
//! the reply, with the error the call failed with, or with status 17 after a
//! cancel. The callback runs on a runtime thread, never inside the ABI call that
//! started the request, and it must not block.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::future::Future;
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use centrifuge::{
    Bytes, Client, Error, RpcCall, RpcOptions, RpcPool, RpcPoolCallOptions, RpcPoolOptions,
};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::{
    DEFAULT_TIMEOUT_MS, ImpulseCentrifugoClient, ImpulseCentrifugoHeader, ImpulseCentrifugoStatus,
    UserData, copy_optional, guarded, millis_or, status,
};

/// Message of a call that was cancelled.
const MESSAGE_CANCELLED: &str = "cancelled";

/// Endpoint key, in a pool config, with the URL of one of its endpoints.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoEndpoint {
    /// The endpoint key that calls name, for example `poolFleet`.
    pub key: *const c_char,
    /// One URL of the key, `ws://` or `wss://`. Repeat the key for more URLs.
    pub url: *const c_char,
}

/// The outcome of a call, as its completion callback receives it. The pointers
/// are borrowed for the duration of the callback only; copy what is kept.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoResult {
    /// Zero on success. Otherwise the failure: a server error code (positive,
    /// 100 and up), or one of the client codes of `ImpulseCentrifugoStatus`.
    pub status: i32,
    /// The application error code the server attached, zero when none.
    pub app_code: i32,
    /// The application error type the server attached, or null when none.
    pub app_type: *const c_char,
    /// Length of `app_type` in bytes.
    pub app_type_len: usize,
    /// A description of the failure, or null on success.
    pub message: *const c_char,
    /// Length of `message` in bytes.
    pub message_len: usize,
    /// The reply payload, or null on failure.
    pub data: *const u8,
    /// Length of `data` in bytes.
    pub data_len: usize,
}

/// Called once with the outcome of a call. `user` is the pointer given with the
/// request.
pub type ImpulseCentrifugoCompletion =
    unsafe extern "C" fn(user: *mut c_void, result: *const ImpulseCentrifugoResult);

/// A request of the rpc functions. Everything is copied before the start call
/// returns, so the caller may free it then. `on_done` may be null, in which case
/// the call runs without a callback.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoRpcRequest {
    /// The method name, NUL-terminated. Required.
    pub method: *const c_char,
    /// The payload bytes; may be null when `data_len` is zero.
    pub data: *const u8,
    /// Length of `data` in bytes.
    pub data_len: usize,
    /// Headers of the call; may be null when `header_count` is zero.
    pub headers: *const ImpulseCentrifugoHeader,
    /// Number of entries in `headers`.
    pub header_count: usize,
    /// Deadline of the call in milliseconds. Zero selects the client's timeout.
    pub timeout_ms: u32,
    /// Completion callback, or null for none.
    pub on_done:
        Option<unsafe extern "C" fn(user: *mut c_void, result: *const ImpulseCentrifugoResult)>,
    /// Passed back to `on_done`.
    pub user: *mut c_void,
}

/// Config of an rpc pool for `impulse_centrifugo_pool_create`. Strings and arrays
/// are copied before the call returns. A zero in a numeric field selects the
/// pool's default, and `timeout_ms` defaults to 30000.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoPoolConfig {
    /// `sizeof` the struct as the caller compiled it. It must be at least the
    /// size of this ABI's struct, and it must be the first field.
    pub cfg_size: usize,
    /// The endpoints, one entry per URL; may be null when `endpoint_count` is zero.
    pub endpoints: *const ImpulseCentrifugoEndpoint,
    /// Number of entries in `endpoints`.
    pub endpoint_count: usize,
    /// Headers of each pool connection; may be null when `header_count` is zero.
    pub headers: *const ImpulseCentrifugoHeader,
    /// Number of entries in `headers`.
    pub header_count: usize,
    /// Client name of each connection, or null for the pool's default.
    pub name: *const c_char,
    /// Client version of each connection, or null for none.
    pub version: *const c_char,
    /// Deadline of a call and the reply timeout of each connection, in milliseconds.
    pub timeout_ms: u32,
    /// Minimum hold of an endpoint after a connection ended for good, in milliseconds.
    pub min_reconnect_ms: u32,
    /// Maximum of that hold, in milliseconds.
    pub max_reconnect_ms: u32,
    /// Slack on top of the server ping interval, in milliseconds.
    pub max_server_ping_delay_ms: u32,
    /// Connections opened per endpoint URL at most.
    pub max_connections_per_endpoint: u32,
    /// Calls in flight per connection at most.
    pub max_inflight_per_connection: u32,
    /// Calls waiting for a connection at most, over all keys.
    pub max_queued: u32,
    /// How long a connection without calls stays open, in milliseconds.
    pub idle_timeout_ms: u32,
    /// Resends of one call at most.
    pub retries: u32,
    /// Non-zero lends each connection the main client's confirmed token.
    pub token_from: u8,
}

/// An rpc pool over one client. Free it with `impulse_centrifugo_pool_destroy`.
#[derive(Debug)]
pub struct ImpulseCentrifugoPool {
    pool: RpcPool,
    runtime: Handle,
    calls: Arc<CallRegistry>,
}

/// Call ids start at 1, so 0 never names a call.
static NEXT_CALL_ID: AtomicU64 = AtomicU64::new(1);

/// Cancel actions of the calls in flight, by call id. An entry leaves the
/// registry after its completion has run.
#[derive(Default)]
pub(crate) struct CallRegistry {
    cancels: Mutex<HashMap<u64, Box<dyn FnOnce() + Send>>>,
}

impl std::fmt::Debug for CallRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallRegistry")
            .field("calls", &self.lock().len())
            .finish()
    }
}

impl CallRegistry {
    /// The map, read through a poisoned lock too: the map holds no invariant a
    /// panic could break.
    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Box<dyn FnOnce() + Send>>> {
        self.cancels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records how to cancel the call `id`.
    fn track(&self, id: u64, cancel: Box<dyn FnOnce() + Send>) {
        self.lock().insert(id, cancel);
    }

    /// Forgets the call `id` once its completion has run.
    fn untrack(&self, id: u64) {
        self.lock().remove(&id);
    }

    /// Cancels the call `id` if it is still in flight.
    pub(crate) fn cancel(&self, id: u64) {
        let cancel: Option<Box<dyn FnOnce() + Send>> = self.lock().remove(&id);
        if let Some(cancel) = cancel {
            cancel();
        }
    }

    /// Waits until every call in flight has run its completion.
    pub(crate) async fn drain(&self) {
        loop {
            let empty: bool = self.lock().is_empty();
            if empty {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// The completion of one call, moved to the runtime thread that finishes it.
#[derive(Debug, Clone, Copy)]
struct Completion {
    callback: Option<ImpulseCentrifugoCompletion>,
    user: UserData,
}

impl Completion {
    /// The completion of a request with its callback and user pointer.
    fn new(callback: Option<ImpulseCentrifugoCompletion>, user: *mut c_void) -> Self {
        Self {
            callback,
            user: UserData(user),
        }
    }

    /// Runs the callback with `result`, when the request has one.
    fn emit(self, result: &ImpulseCentrifugoResult) {
        if let Some(callback) = self.callback {
            // SAFETY: the caller of the start call promised that `user` stays valid
            // until the completion has run, and the result is valid for the call.
            unsafe { callback(self.user.0, result) };
        }
    }

    /// Completes the call with its reply, or with the error it failed with.
    fn complete(self, outcome: Result<Bytes, Error>) {
        match outcome {
            Ok(data) => {
                self.emit(&ImpulseCentrifugoResult {
                    status: ImpulseCentrifugoStatus::Ok as i32,
                    app_code: 0,
                    app_type: ptr::null(),
                    app_type_len: 0,
                    message: ptr::null(),
                    message_len: 0,
                    data: data.as_ptr(),
                    data_len: data.len(),
                });
            }
            Err(error) => {
                let (app_code, app_type, message): (i32, String, String) = match &error {
                    Error::Server(server) => (
                        server.app_code,
                        server.app_type.clone(),
                        server.message.clone(),
                    ),
                    other => (0, String::new(), other.to_string()),
                };
                self.emit(&ImpulseCentrifugoResult {
                    status: status_of(&error),
                    app_code,
                    app_type: app_type.as_ptr().cast::<c_char>(),
                    app_type_len: app_type.len(),
                    message: message.as_ptr().cast::<c_char>(),
                    message_len: message.len(),
                    data: ptr::null(),
                    data_len: 0,
                });
            }
        }
    }

    /// Completes a call that was cancelled before its reply.
    fn complete_cancelled(self) {
        self.emit(&ImpulseCentrifugoResult {
            status: ImpulseCentrifugoStatus::Cancelled as i32,
            app_code: 0,
            app_type: ptr::null(),
            app_type_len: 0,
            message: MESSAGE_CANCELLED.as_ptr().cast::<c_char>(),
            message_len: MESSAGE_CANCELLED.len(),
            data: ptr::null(),
            data_len: 0,
        });
    }
}

/// The status of a failed call: the server's code for a server error, the client
/// code of the failure otherwise. Subscription and deadlock errors cannot come
/// from a call and map to the configuration code.
pub(crate) fn status_of(error: &Error) -> i32 {
    match error {
        Error::Server(server) => i32::try_from(server.code).unwrap_or(i32::MAX),
        Error::ConnectionClosed => ImpulseCentrifugoStatus::ConnectionClosed as i32,
        Error::Timeout => ImpulseCentrifugoStatus::Timeout as i32,
        Error::ClientDisconnected => ImpulseCentrifugoStatus::ClientDisconnected as i32,
        Error::ClientClosed => ImpulseCentrifugoStatus::ClientClosed as i32,
        Error::Transport { .. } => ImpulseCentrifugoStatus::TransportWriteError as i32,
        Error::InvalidData(_) => ImpulseCentrifugoStatus::InvalidArgument as i32,
        _ => ImpulseCentrifugoStatus::ConfigurationError as i32,
    }
}

/// The next call id, unique across the process.
fn next_call_id() -> u64 {
    NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed)
}

/// Writes `id` to `out_call_id` when the caller asked for it.
///
/// # Safety
///
/// A non-null `out_call_id` must be writable for the call.
unsafe fn write_call_id(out_call_id: *mut u64, id: u64) {
    if !out_call_id.is_null() {
        // SAFETY: a non-null pointer is writable by the caller's contract.
        unsafe { *out_call_id = id };
    }
}

/// Runs `work` on the runtime, then completes the call and forgets its cancel
/// action. `None` from `work` means the call was cancelled. The completion runs
/// before the entry is forgotten, so a drain waits for it.
fn spawn_call<F>(
    handle: &Handle,
    calls: &Arc<CallRegistry>,
    id: u64,
    completion: Completion,
    work: F,
) where
    F: Future<Output = Option<Result<Bytes, Error>>> + Send + 'static,
{
    let registry: Arc<CallRegistry> = Arc::clone(calls);
    drop(handle.spawn(async move {
        match work.await {
            Some(outcome) => completion.complete(outcome),
            None => completion.complete_cancelled(),
        }
        registry.untrack(id);
    }));
}

/// The deadline of a call in milliseconds, or `None` for the default.
fn timeout_of(timeout_ms: u32) -> Option<Duration> {
    (timeout_ms != 0).then(|| Duration::from_millis(u64::from(timeout_ms)))
}

/// A `u32` config value as a `usize`, saturating on targets narrower than 32 bits.
fn to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

/// Copies `len` bytes at `data`. A zero length copies nothing and accepts null.
///
/// # Safety
///
/// When `len` is non-zero, `data` must point to `len` readable bytes.
pub(crate) unsafe fn copy_bytes(
    data: *const u8,
    len: usize,
) -> Result<Bytes, ImpulseCentrifugoStatus> {
    if len == 0 {
        return Ok(Bytes::new());
    }
    if data.is_null() {
        return Err(ImpulseCentrifugoStatus::InvalidArgument);
    }
    // SAFETY: the caller guarantees `len` readable bytes at `data`.
    let slice: &[u8] = unsafe { std::slice::from_raw_parts(data, len) };
    Ok(Bytes::copy_from_slice(slice))
}

/// Copies a header array into name and value pairs.
///
/// # Safety
///
/// When `count` is non-zero, `headers` must point to `count` readable entries,
/// each with valid strings for the call.
unsafe fn copy_pairs(
    headers: *const ImpulseCentrifugoHeader,
    count: usize,
) -> Result<HashMap<String, String>, ImpulseCentrifugoStatus> {
    let mut pairs: HashMap<String, String> = HashMap::new();
    if count == 0 {
        return Ok(pairs);
    }
    if headers.is_null() {
        return Err(ImpulseCentrifugoStatus::InvalidArgument);
    }
    for index in 0..count {
        // SAFETY: the caller guarantees `count` readable entries.
        let entry: ImpulseCentrifugoHeader = unsafe { *headers.add(index) };
        // SAFETY: the caller guarantees valid strings for the call.
        let name: String = unsafe { copy_optional(entry.name) }?
            .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
        // SAFETY: as above.
        let value: String = unsafe { copy_optional(entry.value) }?
            .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
        pairs.insert(name, value);
    }
    Ok(pairs)
}

/// A request copied out of the caller's memory.
struct CopiedRequest {
    method: String,
    data: Bytes,
    headers: HashMap<String, String>,
    timeout: Option<Duration>,
    completion: Completion,
}

/// Copies a request out of the caller's memory.
///
/// # Safety
///
/// Every pointer in `request` must be valid for the call, as the request's
/// documentation states.
unsafe fn copy_request(
    request: &ImpulseCentrifugoRpcRequest,
) -> Result<CopiedRequest, ImpulseCentrifugoStatus> {
    // SAFETY: the caller guarantees a NUL-terminated method.
    let method: String = unsafe { copy_optional(request.method) }?
        .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
    // SAFETY: the caller guarantees `data_len` readable bytes.
    let data: Bytes = unsafe { copy_bytes(request.data, request.data_len) }?;
    // SAFETY: the caller guarantees `header_count` readable entries.
    let headers: HashMap<String, String> =
        unsafe { copy_pairs(request.headers, request.header_count) }?;
    Ok(CopiedRequest {
        method,
        data,
        headers,
        timeout: timeout_of(request.timeout_ms),
        completion: Completion::new(request.on_done, request.user),
    })
}

/// Copies the endpoint table of a pool config into a map from key to URLs.
///
/// # Safety
///
/// When `count` is non-zero, `endpoints` must point to `count` readable entries,
/// each with valid strings for the call.
unsafe fn copy_endpoints(
    endpoints: *const ImpulseCentrifugoEndpoint,
    count: usize,
) -> Result<HashMap<String, Vec<String>>, ImpulseCentrifugoStatus> {
    let mut table: HashMap<String, Vec<String>> = HashMap::new();
    if count == 0 {
        return Ok(table);
    }
    if endpoints.is_null() {
        return Err(ImpulseCentrifugoStatus::InvalidArgument);
    }
    for index in 0..count {
        // SAFETY: the caller guarantees `count` readable entries.
        let entry: ImpulseCentrifugoEndpoint = unsafe { *endpoints.add(index) };
        // SAFETY: the caller guarantees valid strings for the call.
        let key: String =
            unsafe { copy_optional(entry.key) }?.ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
        // SAFETY: as above.
        let url: String =
            unsafe { copy_optional(entry.url) }?.ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
        table.entry(key).or_default().push(url);
    }
    Ok(table)
}

/// Starts a rpc call on the main client. The call is sent once the connection is
/// up, and its completion runs once with the reply or the failure. The call id is
/// written to `out_call_id` (when not null) for `impulse_centrifugo_rpc_cancel`.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`.
/// `request` must be readable, with the pointers its documentation names valid
/// for the call. `out_call_id` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_rpc(
    client: *const ImpulseCentrifugoClient,
    request: *const ImpulseCentrifugoRpcRequest,
    out_call_id: *mut u64,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: a non-null request is readable by the caller's contract.
        let Some(request): Option<&ImpulseCentrifugoRpcRequest> = (unsafe { request.as_ref() })
        else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: the pointers of the request are valid for the call.
        let copied: CopiedRequest = match unsafe { copy_request(request) } {
            Ok(copied) => copied,
            Err(error) => return status(error),
        };
        let call: RpcCall = handle.client.rpc_call(
            &copied.method,
            copied.data,
            RpcOptions {
                headers: copied.headers,
                timeout: copied.timeout,
            },
        );
        let id: u64 = next_call_id();
        // SAFETY: `out_call_id` is null or writable by the caller's contract.
        unsafe { write_call_id(out_call_id, id) };
        let canceller: RpcCall = call.clone();
        handle.calls.track(id, Box::new(move || canceller.cancel()));
        spawn_call(
            handle.runtime.handle(),
            &handle.calls,
            id,
            copied.completion,
            async move { Some(call.wait().await.map(|reply| reply.data)) },
        );
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Cancels a rpc call of the main client. A call not sent yet fails at once with
/// status 17, and a sent call also gets a cancel command for the server. Its
/// completion still runs once. An unknown or finished call id does nothing.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_rpc_cancel(
    client: *const ImpulseCentrifugoClient,
    call_id: u64,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        handle.calls.cancel(call_id);
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Builds a pool over the main client from `cfg`. The pool's connections run on
/// the client's runtime, so the pool must be destroyed before the client. On any
/// status other than success `*out` is null.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`. `cfg`
/// must be readable, with its first field its size and its pointers valid for the
/// call. `out` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_pool_create(
    client: *const ImpulseCentrifugoClient,
    cfg: *const ImpulseCentrifugoPoolConfig,
    out: *mut *mut ImpulseCentrifugoPool,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: `out` is non-null and writable by the caller's contract.
        unsafe { *out = ptr::null_mut() };
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: a non-null config is readable by the caller's contract.
        let Some(config): Option<&ImpulseCentrifugoPoolConfig> = (unsafe { cfg.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        if config.cfg_size < size_of::<ImpulseCentrifugoPoolConfig>() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: the pointers of the config are valid for the call by the caller's contract.
        match unsafe { build_pool(handle, config) } {
            Ok(pool) => {
                // SAFETY: `out` is non-null and writable, checked above.
                unsafe { *out = Box::into_raw(Box::new(pool)) };
                status(ImpulseCentrifugoStatus::Ok)
            }
            Err(error) => status(error),
        }
    })
}

/// Builds the pool from a validated config.
///
/// # Safety
///
/// The pointers of `config` must be valid for the call.
unsafe fn build_pool(
    handle: &ImpulseCentrifugoClient,
    config: &ImpulseCentrifugoPoolConfig,
) -> Result<ImpulseCentrifugoPool, ImpulseCentrifugoStatus> {
    // SAFETY: the caller guarantees the endpoint table is valid for the call.
    let table: HashMap<String, Vec<String>> =
        unsafe { copy_endpoints(config.endpoints, config.endpoint_count) }?;
    // SAFETY: the caller guarantees the headers are valid for the call.
    let headers: HashMap<String, String> =
        unsafe { copy_pairs(config.headers, config.header_count) }?;
    // SAFETY: the caller guarantees the name is valid for the call.
    let name: Option<String> = unsafe { copy_optional(config.name) }?;
    // SAFETY: the caller guarantees the version is valid for the call.
    let version: Option<String> = unsafe { copy_optional(config.version) }?;

    let mut options: RpcPoolOptions =
        RpcPoolOptions::new(move |key: &str| table.get(key).cloned().unwrap_or_default());
    options.headers = headers;
    if let Some(value) = name {
        options.name = value;
    }
    if let Some(value) = version {
        options.version = value;
    }
    options.timeout = millis_or(config.timeout_ms, DEFAULT_TIMEOUT_MS);
    if config.min_reconnect_ms != 0 {
        options.min_reconnect_delay = Duration::from_millis(u64::from(config.min_reconnect_ms));
    }
    if config.max_reconnect_ms != 0 {
        options.max_reconnect_delay = Duration::from_millis(u64::from(config.max_reconnect_ms));
    }
    if config.max_server_ping_delay_ms != 0 {
        options.max_server_ping_delay =
            Duration::from_millis(u64::from(config.max_server_ping_delay_ms));
    }
    if config.max_connections_per_endpoint != 0 {
        options.max_connections_per_endpoint = to_usize(config.max_connections_per_endpoint);
    }
    if config.max_inflight_per_connection != 0 {
        options.max_inflight_per_connection = to_usize(config.max_inflight_per_connection);
    }
    if config.max_queued != 0 {
        options.max_queued = to_usize(config.max_queued);
    }
    if config.idle_timeout_ms != 0 {
        options.idle_timeout = Duration::from_millis(u64::from(config.idle_timeout_ms));
    }
    if config.retries != 0 {
        options.retries = to_usize(config.retries);
    }
    options.token_from = config.token_from != 0;

    let runtime_guard: tokio::runtime::EnterGuard<'_> = handle.runtime.enter();
    let pool: RpcPool = RpcPool::new(&handle.client, options)
        .map_err(|_| ImpulseCentrifugoStatus::ConfigurationError)?;
    drop(runtime_guard);
    let calls: Arc<CallRegistry> = Arc::new(CallRegistry::default());
    register_pool(handle, &calls);
    Ok(ImpulseCentrifugoPool {
        pool,
        runtime: Handle::clone(handle.runtime.handle()),
        calls,
    })
}

/// Records the call registry of a new pool on its client, so that the client's
/// destroy waits for the pool's calls too. Registries whose pool and calls are
/// all gone are dropped from the record.
fn register_pool(handle: &ImpulseCentrifugoClient, calls: &Arc<CallRegistry>) {
    let mut registries: MutexGuard<'_, Vec<Weak<CallRegistry>>> = handle
        .pool_calls
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    registries.retain(|registry: &Weak<CallRegistry>| registry.strong_count() > 0);
    registries.push(Arc::downgrade(calls));
}

/// Starts a call on the pool, by endpoint key. The completion runs once with the
/// reply or the failure. The call id is written to `out_call_id` (when not null)
/// for `impulse_centrifugo_pool_rpc_cancel`.
///
/// # Safety
///
/// `pool` must be a live handle from `impulse_centrifugo_pool_create`. `key` must
/// be a NUL-terminated string. `request` must be readable, with the pointers its
/// documentation names valid for the call. `out_call_id` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_pool_rpc(
    pool: *const ImpulseCentrifugoPool,
    key: *const c_char,
    request: *const ImpulseCentrifugoRpcRequest,
    out_call_id: *mut u64,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoPool> = (unsafe { pool.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: the caller guarantees a NUL-terminated key.
        let endpoint_key: String = match unsafe { copy_optional(key) } {
            Ok(Some(text)) => text,
            _ => return status(ImpulseCentrifugoStatus::InvalidArgument),
        };
        // SAFETY: a non-null request is readable by the caller's contract.
        let Some(request): Option<&ImpulseCentrifugoRpcRequest> = (unsafe { request.as_ref() })
        else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: the pointers of the request are valid for the call.
        let copied: CopiedRequest = match unsafe { copy_request(request) } {
            Ok(copied) => copied,
            Err(error) => return status(error),
        };
        let id: u64 = next_call_id();
        // SAFETY: `out_call_id` is null or writable by the caller's contract.
        unsafe { write_call_id(out_call_id, id) };
        let notify: Arc<Notify> = Arc::new(Notify::new());
        let canceller: Arc<Notify> = Arc::clone(&notify);
        handle
            .calls
            .track(id, Box::new(move || canceller.notify_one()));
        let rpc_pool: RpcPool = handle.pool.clone();
        let options: RpcPoolCallOptions = RpcPoolCallOptions {
            headers: copied.headers,
            timeout: copied.timeout,
            ..Default::default()
        };
        let method: String = copied.method;
        let data: Bytes = copied.data;
        spawn_call(
            &handle.runtime,
            &handle.calls,
            id,
            copied.completion,
            async move {
                tokio::select! {
                    biased;
                    () = notify.notified() => None,
                    result = rpc_pool.rpc(&endpoint_key, &method, data, options) => {
                        Some(result.map(|reply| reply.data))
                    }
                }
            },
        );
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Cancels a pool call. A call waiting for a connection fails at once with status
/// 17. Its completion still runs once. An unknown or finished call id does nothing.
///
/// # Safety
///
/// `pool` must be a live handle from `impulse_centrifugo_pool_create`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_pool_rpc_cancel(
    pool: *const ImpulseCentrifugoPool,
    call_id: u64,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoPool> = (unsafe { pool.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        handle.calls.cancel(call_id);
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Closes the pool and frees its handle. Waiting calls fail with status 17 and
/// calls in flight with status 11, and their completions still run.
///
/// # Safety
///
/// `pool` must be a handle from `impulse_centrifugo_pool_create` that has not been
/// destroyed. No other call may use the handle during or after this one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_pool_destroy(pool: *mut ImpulseCentrifugoPool) -> i32 {
    guarded(|| {
        if pool.is_null() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: a non-null handle from create, not yet destroyed, by the caller's contract.
        let handle: Box<ImpulseCentrifugoPool> = unsafe { Box::from_raw(pool) };
        handle.pool.close();
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Publishes `data` into `channel` through the main client, with no subscription.
/// The completion runs once with the outcome; its `data` is empty.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`. `channel`
/// must be a NUL-terminated string. `data` must be valid for `data_len` bytes.
/// `user` must stay valid until the completion has run.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_publish(
    client: *const ImpulseCentrifugoClient,
    channel: *const c_char,
    data: *const u8,
    data_len: usize,
    on_done: Option<
        unsafe extern "C" fn(user: *mut c_void, result: *const ImpulseCentrifugoResult),
    >,
    user: *mut c_void,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: the caller guarantees a NUL-terminated channel.
        let channel_name: String = match unsafe { copy_optional(channel) } {
            Ok(Some(text)) => text,
            _ => return status(ImpulseCentrifugoStatus::InvalidArgument),
        };
        // SAFETY: the caller guarantees `data_len` readable bytes.
        let payload: Bytes = match unsafe { copy_bytes(data, data_len) } {
            Ok(payload) => payload,
            Err(error) => return status(error),
        };
        let id: u64 = next_call_id();
        // A publish has no cancel action. It is tracked anyway, so a drain waits for
        // its completion.
        handle.calls.track(id, Box::new(|| {}));
        let main: Client = handle.client.clone();
        spawn_call(
            handle.runtime.handle(),
            &handle.calls,
            id,
            Completion::new(on_done, user),
            async move {
                Some(
                    main.publish(&channel_name, payload)
                        .await
                        .map(|()| Bytes::new()),
                )
            },
        );
        status(ImpulseCentrifugoStatus::Ok)
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::mem::size_of;
    use std::ptr;

    use crate::{
        ImpulseCentrifugoClient, ImpulseCentrifugoConfig, ImpulseCentrifugoStatus,
        impulse_centrifugo_client_create, impulse_centrifugo_client_destroy,
    };

    use super::impulse_centrifugo_publish;

    #[test]
    fn publish_stays_tracked_until_its_completion_runs() {
        let endpoint: CString =
            CString::new("ws://127.0.0.1:9/connection/websocket").expect("endpoint has no NUL");
        let config: ImpulseCentrifugoConfig = ImpulseCentrifugoConfig {
            cfg_size: size_of::<ImpulseCentrifugoConfig>(),
            endpoint_url: endpoint.as_ptr(),
            websocket_subprotocol: 0,
            headers: ptr::null(),
            header_count: 0,
            instance: ptr::null(),
            proxy_url: ptr::null(),
            timeout_ms: 300,
            min_reconnect_ms: 0,
            max_reconnect_ms: 0,
            max_server_ping_delay_ms: 0,
            max_pending_rpc: 0,
        };
        let mut handle: *mut ImpulseCentrifugoClient = ptr::null_mut();
        // SAFETY: the config and the out pointer are valid for the call.
        let created: i32 =
            unsafe { impulse_centrifugo_client_create(&raw const config, &raw mut handle) };
        assert_eq!(created, ImpulseCentrifugoStatus::Ok as i32);

        let channel: CString = CString::new("news").expect("channel has no NUL");
        // SAFETY: the handle is live; the channel is valid; the payload is empty and
        // there is no callback.
        let started: i32 = unsafe {
            impulse_centrifugo_publish(
                handle,
                channel.as_ptr(),
                ptr::null(),
                0,
                None,
                ptr::null_mut(),
            )
        };
        assert_eq!(started, ImpulseCentrifugoStatus::Ok as i32);

        // SAFETY: the handle is live until it is destroyed below.
        let client: &ImpulseCentrifugoClient = unsafe { &*handle };
        assert_eq!(
            client.calls.lock().len(),
            1,
            "the publish is tracked until its completion has run"
        );
        // SAFETY: the handle is not used after this call.
        unsafe { impulse_centrifugo_client_destroy(handle) };
    }
}
