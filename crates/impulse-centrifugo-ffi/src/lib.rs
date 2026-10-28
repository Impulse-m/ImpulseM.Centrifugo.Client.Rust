//! C ABI of the `ImpulseM` Centrifugo client.
//!
//! Every exported function returns a status: [`ImpulseCentrifugoStatus::Ok`]
//! (0), a positive client error code, or a negative ABI error. A panic never
//! crosses the boundary: each function turns it into
//! [`ImpulseCentrifugoStatus::Panic`]. The C header is generated from this file
//! by cbindgen (`cbindgen.toml`) into `include/impulse_centrifugo.h`.

#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_void};
use std::future::Future;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use centrifuge::{Client, ClientBuilder, ProxyConfig};
use http::{HeaderMap, HeaderName, HeaderValue};
use tokio::runtime::{Builder, Handle, Runtime};
use url::Url;

mod events;
mod rpc;

pub use events::{
    ImpulseCentrifugoEvent, ImpulseCentrifugoEventCallback, ImpulseCentrifugoEventKind,
    impulse_centrifugo_client_set_event_callback,
};
use rpc::CallRegistry;
pub use rpc::{
    ImpulseCentrifugoCompletion, ImpulseCentrifugoEndpoint, ImpulseCentrifugoPool,
    ImpulseCentrifugoPoolConfig, ImpulseCentrifugoResult, ImpulseCentrifugoRpcRequest,
    impulse_centrifugo_pool_create, impulse_centrifugo_pool_destroy, impulse_centrifugo_pool_rpc,
    impulse_centrifugo_pool_rpc_cancel, impulse_centrifugo_publish, impulse_centrifugo_rpc,
    impulse_centrifugo_rpc_cancel,
};

/// Version of this C ABI. Bumped when a signature, a struct layout or a status
/// value changes. The C++ side compares it with the header macro at startup.
pub const IMPULSE_CENTRIFUGO_ABI_VERSION: u32 = 1;

/// Worker threads of the runtime each client handle owns.
const RUNTIME_WORKER_THREADS: usize = 2;

/// Command timeout when the config leaves `timeout_ms` at zero.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Reconnect backoff minimum when the config leaves `min_reconnect_ms` at zero.
const DEFAULT_MIN_RECONNECT_MS: u64 = 1_000;

/// Reconnect backoff maximum when the config leaves `max_reconnect_ms` at zero.
const DEFAULT_MAX_RECONNECT_MS: u64 = 30_000;

/// Slack on top of the server ping interval when the config leaves
/// `max_server_ping_delay_ms` at zero.
const DEFAULT_MAX_SERVER_PING_DELAY_MS: u64 = 10_000;

/// Pending tracked rpc calls when the config leaves `max_pending_rpc` at zero.
const DEFAULT_MAX_PENDING_RPC: u32 = 32;

/// Status of every exported function. Zero is success, positive values are the
/// client error codes shared with the Java and Swift SDKs, negative values are
/// ABI errors.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpulseCentrifugoStatus {
    /// The call succeeded.
    Ok = 0,
    /// A pointer is null, a string is not UTF-8, or `cfg_size` is smaller than
    /// the config struct of this ABI version.
    InvalidArgument = -1,
    /// The handle could not be initialised, for example its runtime did not start.
    NotInitialized = -2,
    /// Reserved for the C++ side, which reports it when
    /// `IMPULSE_CENTRIFUGO_ABI_VERSION` does not match the library.
    AbiMismatch = -3,
    /// A panic was caught at the boundary.
    Panic = -4,
    /// The client was closed; the call cannot run.
    ClientClosed = -5,
    /// Reserved: a request could not be encoded or written.
    TransportWriteError = 10,
    /// The connection was lost while the call was on it, and it was not resent.
    ConnectionClosed = 11,
    /// The configuration cannot work: an invalid instance, endpoint, proxy URL,
    /// header or `max_pending_rpc`.
    ConfigurationError = 12,
    /// No reply came within the call's deadline, or the connection did not come
    /// up in time for the call. The other SDKs carry no number for it.
    Timeout = 13,
    /// The main client is not connected, so a pool call cannot run. The other
    /// SDKs carry no number for it.
    ClientDisconnected = 14,
    /// The connection cannot carry what the call asks for.
    Unsupported = 15,
    /// The call hit a limit, such as `max_pending_rpc`.
    Limit = 16,
    /// The call was cancelled.
    Cancelled = 17,
    /// A redirect of a pool call did not complete.
    RedirectFailed = 18,
}

/// One HTTP header sent on the WebSocket upgrade. Both strings are NUL-terminated
/// and copied at `impulse_centrifugo_client_create`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoHeader {
    /// Header name, for example `Authorization`.
    pub name: *const c_char,
    /// Header value.
    pub value: *const c_char,
}

/// Client configuration for `impulse_centrifugo_client_create`. The strings and
/// the header array are copied before the call returns. Zero in a numeric field
/// selects its default.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoConfig {
    /// `sizeof` the struct as the caller compiled it. It must be at least the
    /// size of this ABI's struct, and it must be the first field.
    pub cfg_size: usize,
    /// The fleet endpoint, `ws://` or `wss://`, with its query. Required.
    pub endpoint_url: *const c_char,
    /// Non-zero requests the `centrifuge-protobuf` WebSocket subprotocol: on for
    /// the update fleet, zero for the media and pool fleets.
    pub websocket_subprotocol: u8,
    /// Extra HTTP headers on the WebSocket upgrade; may be null when
    /// `header_count` is zero.
    pub headers: *const ImpulseCentrifugoHeader,
    /// Number of entries in `headers`.
    pub header_count: usize,
    /// The client instance, or null for none. 22 to 64 characters of `A-Z`,
    /// `a-z`, `0-9`, `_` or `-`.
    pub instance: *const c_char,
    /// The proxy, `http://host:port` (HTTP CONNECT) or `socks5://host:port`; null
    /// or an empty string connects directly.
    pub proxy_url: *const c_char,
    /// Command timeout in milliseconds. Zero selects 30000.
    pub timeout_ms: u32,
    /// Reconnect backoff minimum in milliseconds. Zero selects 1000.
    pub min_reconnect_ms: u32,
    /// Reconnect backoff maximum in milliseconds. Zero selects 30000.
    pub max_reconnect_ms: u32,
    /// Slack on top of the server ping interval in milliseconds. Zero selects
    /// 10000.
    pub max_server_ping_delay_ms: u32,
    /// Tracked rpc calls of the client that may be unsettled at once. Zero
    /// selects 32.
    pub max_pending_rpc: u32,
}

/// An opaque client. It owns its runtime; free it with
/// `impulse_centrifugo_client_destroy`.
#[derive(Debug)]
pub struct ImpulseCentrifugoClient {
    runtime: Runtime,
    client: Client,
    calls: Arc<CallRegistry>,
    /// The call registries of the pools built on this client, so that destroy
    /// waits for the calls of pools that were destroyed before it.
    pool_calls: Mutex<Vec<Weak<CallRegistry>>>,
}

/// A caller's user pointer, moved to the runtime threads that run its callback.
/// The caller keeps the pointee valid until the callback it was given to has run.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UserData(pub(crate) *mut c_void);

// SAFETY: the caller's contract keeps the pointee valid and usable from any thread
// until the callback that receives it has run.
unsafe impl Send for UserData {}

// SAFETY: as for `Send`; the pointer is only handed back to the caller's callback.
unsafe impl Sync for UserData {}

/// Runs `body` and turns a panic into [`ImpulseCentrifugoStatus::Panic`].
fn guarded<F>(body: F) -> i32
where
    F: FnOnce() -> i32,
{
    catch_unwind(AssertUnwindSafe(body)).unwrap_or(ImpulseCentrifugoStatus::Panic as i32)
}

/// The status as the C integer the functions return.
const fn status(value: ImpulseCentrifugoStatus) -> i32 {
    value as i32
}

/// Copies an optional C string. Null is `None`.
///
/// # Safety
///
/// A non-null `text` must point to a NUL-terminated string that stays valid
/// for the call.
unsafe fn copy_optional(text: *const c_char) -> Result<Option<String>, ImpulseCentrifugoStatus> {
    if text.is_null() {
        return Ok(None);
    }
    // SAFETY: the caller guarantees a NUL-terminated string for the call.
    let borrowed: &CStr = unsafe { CStr::from_ptr(text) };
    match borrowed.to_str() {
        Ok(value) => Ok(Some(value.to_owned())),
        Err(_) => Err(ImpulseCentrifugoStatus::InvalidArgument),
    }
}

/// Copies the header array into a header map.
///
/// # Safety
///
/// When `config.header_count` is non-zero, `config.headers` must point to that
/// many readable entries, each with valid strings for the call.
unsafe fn copy_headers(
    config: &ImpulseCentrifugoConfig,
) -> Result<HeaderMap, ImpulseCentrifugoStatus> {
    let mut headers: HeaderMap = HeaderMap::new();
    if config.header_count == 0 {
        return Ok(headers);
    }
    if config.headers.is_null() {
        return Err(ImpulseCentrifugoStatus::InvalidArgument);
    }
    for index in 0..config.header_count {
        // SAFETY: the caller guarantees `header_count` readable entries.
        let entry: ImpulseCentrifugoHeader = unsafe { *config.headers.add(index) };
        // SAFETY: the caller guarantees valid strings for the call.
        let name: String = unsafe { copy_optional(entry.name) }?
            .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
        // SAFETY: as above.
        let value: String = unsafe { copy_optional(entry.value) }?
            .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
        let header_name: HeaderName = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| ImpulseCentrifugoStatus::ConfigurationError)?;
        let header_value: HeaderValue = HeaderValue::from_str(&value)
            .map_err(|_| ImpulseCentrifugoStatus::ConfigurationError)?;
        headers.append(header_name, header_value);
    }
    Ok(headers)
}

/// The proxy of the config: direct when absent, HTTP CONNECT for `http`,
/// SOCKS5 with remote name resolution for `socks5`.
fn proxy_from(proxy_url: Option<String>) -> Result<ProxyConfig, ImpulseCentrifugoStatus> {
    let text: String = match proxy_url {
        Some(value) if !value.is_empty() => value,
        _ => return Ok(ProxyConfig::None),
    };
    let url: Url = Url::parse(&text).map_err(|_| ImpulseCentrifugoStatus::ConfigurationError)?;
    match url.scheme() {
        "http" | "https" => Ok(ProxyConfig::http(url, None)),
        "socks5" => Ok(ProxyConfig::socks5(url, None, true)),
        _ => Err(ImpulseCentrifugoStatus::ConfigurationError),
    }
}

/// `millis` as a duration, or `default_millis` when `millis` is zero.
fn millis_or(millis: u32, default_millis: u64) -> Duration {
    if millis == 0 {
        Duration::from_millis(default_millis)
    } else {
        Duration::from_millis(u64::from(millis))
    }
}

/// Builds the client and its runtime from a validated config.
///
/// # Safety
///
/// The string and header pointers of `config` must be valid for the call, as
/// `impulse_centrifugo_client_create` requires.
unsafe fn build_client(
    config: &ImpulseCentrifugoConfig,
) -> Result<ImpulseCentrifugoClient, ImpulseCentrifugoStatus> {
    // SAFETY: the caller guarantees valid strings for the call.
    let endpoint: String = unsafe { copy_optional(config.endpoint_url) }?
        .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
    // SAFETY: as above.
    let instance: Option<String> = unsafe { copy_optional(config.instance) }?;
    // SAFETY: as above.
    let proxy_text: Option<String> = unsafe { copy_optional(config.proxy_url) }?;
    // SAFETY: as above.
    let headers: HeaderMap = unsafe { copy_headers(config) }?;
    let proxy: ProxyConfig = proxy_from(proxy_text)?;

    let runtime: Runtime = Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .thread_name("impulse-centrifugo")
        .enable_all()
        .build()
        .map_err(|_| ImpulseCentrifugoStatus::NotInitialized)?;

    let mut builder: ClientBuilder = Client::builder(endpoint)
        .runtime(Handle::clone(runtime.handle()))
        .websocket_subprotocol(config.websocket_subprotocol != 0)
        .http_headers(headers)
        .proxy(proxy)
        .timeout(millis_or(config.timeout_ms, DEFAULT_TIMEOUT_MS))
        .min_reconnect_delay(millis_or(config.min_reconnect_ms, DEFAULT_MIN_RECONNECT_MS))
        .max_reconnect_delay(millis_or(config.max_reconnect_ms, DEFAULT_MAX_RECONNECT_MS))
        .max_server_ping_delay(millis_or(
            config.max_server_ping_delay_ms,
            DEFAULT_MAX_SERVER_PING_DELAY_MS,
        ))
        .max_pending_rpc(if config.max_pending_rpc == 0 {
            DEFAULT_MAX_PENDING_RPC
        } else {
            config.max_pending_rpc
        });
    if let Some(value) = instance {
        builder = builder.instance(value);
    }

    let built: Result<Client, centrifuge::Error> = builder.build();
    let Ok(client): Result<Client, centrifuge::Error> = built else {
        // Dropping a runtime blocks, which is not allowed on a runtime thread.
        runtime.shutdown_background();
        return Err(ImpulseCentrifugoStatus::ConfigurationError);
    };
    Ok(ImpulseCentrifugoClient {
        runtime,
        client,
        calls: Arc::new(CallRegistry::default()),
        pool_calls: Mutex::new(Vec::new()),
    })
}

/// The ABI version of this library, the value of `IMPULSE_CENTRIFUGO_ABI_VERSION`.
#[unsafe(no_mangle)]
pub extern "C" fn impulse_centrifugo_abi_version() -> u32 {
    catch_unwind(|| IMPULSE_CENTRIFUGO_ABI_VERSION).unwrap_or(0)
}

/// Creates a client from `cfg` and stores it in `*out`. The client does not
/// connect until `impulse_centrifugo_client_connect`. On any status other than
/// success `*out` is null.
///
/// # Safety
///
/// `cfg` must point to a readable config whose first field is its size, with
/// NUL-terminated strings and, when `header_count` is non-zero, a readable header
/// array. `out` must be writable. Both are read or written only during the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_client_create(
    cfg: *const ImpulseCentrifugoConfig,
    out: *mut *mut ImpulseCentrifugoClient,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: `out` is non-null and writable by the caller's contract.
        unsafe { *out = ptr::null_mut() };
        if cfg.is_null() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: `cfg` is non-null and starts with `cfg_size`, readable by the caller's contract.
        let declared_size: usize = unsafe { (*cfg).cfg_size };
        if declared_size < size_of::<ImpulseCentrifugoConfig>() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: the caller declared at least `size_of` readable bytes.
        let config: &ImpulseCentrifugoConfig = unsafe { &*cfg };
        // SAFETY: the pointers in `config` are valid for the call by the caller's contract.
        match unsafe { build_client(config) } {
            Ok(client) => {
                // SAFETY: `out` is non-null and writable, checked above.
                unsafe { *out = Box::into_raw(Box::new(client)) };
                status(ImpulseCentrifugoStatus::Ok)
            }
            Err(error) => status(error),
        }
    })
}

/// Starts connecting. Never waits: the client connects in the background and
/// reconnects until disconnected or destroyed. Calling it while connecting or
/// connected does nothing.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create` that
/// has not been destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_client_connect(
    client: *const ImpulseCentrifugoClient,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        match handle.client.connect() {
            Ok(()) => status(ImpulseCentrifugoStatus::Ok),
            Err(_) => status(ImpulseCentrifugoStatus::ClientClosed),
        }
    })
}

/// Disconnects. Subscriptions keep their positions for the next connect.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create` that
/// has not been destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_client_disconnect(
    client: *const ImpulseCentrifugoClient,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        handle.client.disconnect();
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Closes the client and frees the handle. When called from a thread outside
/// the client's runtime, it waits until the close completes, until every rpc and
/// publish callback has run, and until the runtime has stopped, after which no
/// callbacks fire. Called from a runtime thread, it starts the close and stops
/// the runtime without waiting. Destroy the rpc pools of the client first.
///
/// # Safety
///
/// `client` must be a handle from `impulse_centrifugo_client_create` that has
/// not been destroyed. No other call may use the handle during or after this one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_client_destroy(
    client: *mut ImpulseCentrifugoClient,
) -> i32 {
    guarded(|| {
        if client.is_null() {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        }
        // SAFETY: a non-null handle from create, not yet destroyed, by the caller's contract.
        let handle: Box<ImpulseCentrifugoClient> = unsafe { Box::from_raw(client) };
        let ImpulseCentrifugoClient {
            runtime,
            client,
            calls,
            pool_calls,
        }: ImpulseCentrifugoClient = *handle;
        let closing: Pin<Box<dyn Future<Output = ()> + Send + 'static>> = Box::pin(client.close());
        if Handle::try_current().is_ok() {
            drop(closing);
            drop(client);
            runtime.shutdown_background();
        } else {
            let pool_registries: Vec<Weak<CallRegistry>> = pool_calls
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner);
            runtime.block_on(async move {
                closing.await;
                calls.drain().await;
                for registry in pool_registries
                    .iter()
                    .filter_map(|registry: &Weak<CallRegistry>| registry.upgrade())
                {
                    registry.drain().await;
                }
            });
            drop(client);
            drop(runtime);
        }
        status(ImpulseCentrifugoStatus::Ok)
    })
}

#[cfg(test)]
mod tests {
    use super::{ImpulseCentrifugoStatus, guarded};

    #[test]
    fn guarded_passes_the_status_through() {
        assert_eq!(
            guarded(|| ImpulseCentrifugoStatus::ConfigurationError as i32),
            12
        );
    }

    #[test]
    fn guarded_turns_a_panic_into_the_panic_status() {
        let status: i32 = guarded(|| panic!("boom"));
        assert_eq!(status, ImpulseCentrifugoStatus::Panic as i32);
    }
}
