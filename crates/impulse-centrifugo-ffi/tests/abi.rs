//! Tests of the C ABI: status codes, config validation and the handle lifecycle.

#![allow(unsafe_code)]

use std::ffi::CString;
use std::mem::size_of;
use std::ptr;

use impulse_centrifugo_ffi::{
    IMPULSE_CENTRIFUGO_ABI_VERSION, ImpulseCentrifugoClient, ImpulseCentrifugoConfig,
    ImpulseCentrifugoHeader, ImpulseCentrifugoStatus, impulse_centrifugo_abi_version,
    impulse_centrifugo_client_connect, impulse_centrifugo_client_create,
    impulse_centrifugo_client_destroy, impulse_centrifugo_client_disconnect,
};

const VALID_INSTANCE: &str = "abcdefghijklmnopqrstuv";

fn config_for(endpoint: &CString) -> ImpulseCentrifugoConfig {
    ImpulseCentrifugoConfig {
        cfg_size: size_of::<ImpulseCentrifugoConfig>(),
        endpoint_url: endpoint.as_ptr(),
        websocket_subprotocol: 1,
        headers: ptr::null(),
        header_count: 0,
        instance: ptr::null(),
        proxy_url: ptr::null(),
        timeout_ms: 0,
        min_reconnect_ms: 0,
        max_reconnect_ms: 0,
        max_server_ping_delay_ms: 0,
        max_pending_rpc: 0,
    }
}

fn endpoint() -> CString {
    CString::new("ws://127.0.0.1:9/connection/websocket").expect("endpoint has no NUL")
}

/// Calls create with `config` and returns the status and the handle it wrote.
fn create(config: &ImpulseCentrifugoConfig) -> (i32, *mut ImpulseCentrifugoClient) {
    let mut handle: *mut ImpulseCentrifugoClient = ptr::null_mut();
    // SAFETY: the config and the out pointer are valid for the call.
    let status: i32 = unsafe { impulse_centrifugo_client_create(config, &raw mut handle) };
    (status, handle)
}

#[test]
fn abi_version_matches_the_constant() {
    assert_eq!(
        impulse_centrifugo_abi_version(),
        IMPULSE_CENTRIFUGO_ABI_VERSION
    );
    assert_eq!(IMPULSE_CENTRIFUGO_ABI_VERSION, 1);
}

#[test]
fn create_rejects_a_null_out_pointer() {
    let endpoint: CString = endpoint();
    let config: ImpulseCentrifugoConfig = config_for(&endpoint);
    // SAFETY: a null out pointer is the case under test; create must not write through it.
    let status: i32 =
        unsafe { impulse_centrifugo_client_create(&raw const config, ptr::null_mut()) };
    assert_eq!(status, ImpulseCentrifugoStatus::InvalidArgument as i32);
}

#[test]
fn create_rejects_a_null_config_and_clears_out() {
    let mut handle: *mut ImpulseCentrifugoClient = ptr::dangling_mut();
    // SAFETY: the null config is the case under test; out is a valid local.
    let status: i32 = unsafe { impulse_centrifugo_client_create(ptr::null(), &raw mut handle) };
    assert_eq!(status, ImpulseCentrifugoStatus::InvalidArgument as i32);
    assert!(handle.is_null());
}

#[test]
fn create_rejects_a_config_smaller_than_this_abi() {
    let endpoint: CString = endpoint();
    let mut config: ImpulseCentrifugoConfig = config_for(&endpoint);
    config.cfg_size = size_of::<usize>();
    let (status, handle): (i32, *mut ImpulseCentrifugoClient) = create(&config);
    assert_eq!(status, ImpulseCentrifugoStatus::InvalidArgument as i32);
    assert!(handle.is_null());
}

#[test]
fn create_rejects_a_missing_endpoint() {
    let endpoint: CString = endpoint();
    let mut config: ImpulseCentrifugoConfig = config_for(&endpoint);
    config.endpoint_url = ptr::null();
    let (status, handle): (i32, *mut ImpulseCentrifugoClient) = create(&config);
    assert_eq!(status, ImpulseCentrifugoStatus::InvalidArgument as i32);
    assert!(handle.is_null());
}

#[test]
fn create_with_an_invalid_instance_returns_configuration_error() {
    let endpoint: CString = endpoint();
    let instance: CString = CString::new("too-short").unwrap();
    let mut config: ImpulseCentrifugoConfig = config_for(&endpoint);
    config.instance = instance.as_ptr();
    let (status, handle): (i32, *mut ImpulseCentrifugoClient) = create(&config);
    assert_eq!(status, 12);
    assert!(handle.is_null());
}

#[test]
fn create_with_an_unknown_proxy_scheme_returns_configuration_error() {
    let endpoint: CString = endpoint();
    let proxy: CString = CString::new("ftp://127.0.0.1:8080").unwrap();
    let mut config: ImpulseCentrifugoConfig = config_for(&endpoint);
    config.proxy_url = proxy.as_ptr();
    let (status, handle): (i32, *mut ImpulseCentrifugoClient) = create(&config);
    assert_eq!(status, 12);
    assert!(handle.is_null());
}

#[test]
fn create_with_an_invalid_header_name_returns_configuration_error() {
    let endpoint: CString = endpoint();
    let name: CString = CString::new("bad name").unwrap();
    let value: CString = CString::new("value").unwrap();
    let headers: [ImpulseCentrifugoHeader; 1] = [ImpulseCentrifugoHeader {
        name: name.as_ptr(),
        value: value.as_ptr(),
    }];
    let mut config: ImpulseCentrifugoConfig = config_for(&endpoint);
    config.headers = headers.as_ptr();
    config.header_count = headers.len();
    let (status, handle): (i32, *mut ImpulseCentrifugoClient) = create(&config);
    assert_eq!(status, 12);
    assert!(handle.is_null());
}

#[test]
fn create_connect_disconnect_and_destroy_succeed() {
    let endpoint: CString = endpoint();
    let instance: CString = CString::new(VALID_INSTANCE).unwrap();
    let proxy: CString = CString::new("http://127.0.0.1:8080").unwrap();
    let name: CString = CString::new("Authorization").unwrap();
    let value: CString = CString::new("Bearer test").unwrap();
    let headers: [ImpulseCentrifugoHeader; 1] = [ImpulseCentrifugoHeader {
        name: name.as_ptr(),
        value: value.as_ptr(),
    }];
    let mut config: ImpulseCentrifugoConfig = config_for(&endpoint);
    config.instance = instance.as_ptr();
    config.proxy_url = proxy.as_ptr();
    config.headers = headers.as_ptr();
    config.header_count = headers.len();
    config.websocket_subprotocol = 0;
    config.max_pending_rpc = 8;
    let (status, handle): (i32, *mut ImpulseCentrifugoClient) = create(&config);
    assert_eq!(status, ImpulseCentrifugoStatus::Ok as i32);
    assert!(!handle.is_null());

    // SAFETY: the handle is live until destroy below.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(handle) };
    assert_eq!(connect_status, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: as above.
    let disconnect_status: i32 = unsafe { impulse_centrifugo_client_disconnect(handle) };
    assert_eq!(disconnect_status, ImpulseCentrifugoStatus::Ok as i32);

    // SAFETY: the handle is not used after this call.
    let destroy_status: i32 = unsafe { impulse_centrifugo_client_destroy(handle) };
    assert_eq!(destroy_status, ImpulseCentrifugoStatus::Ok as i32);
}

#[test]
fn null_handles_are_rejected() {
    // SAFETY: null handles are the case under test; the functions must not dereference them.
    let connect_status: i32 = unsafe { impulse_centrifugo_client_connect(ptr::null()) };
    // SAFETY: as above.
    let disconnect_status: i32 = unsafe { impulse_centrifugo_client_disconnect(ptr::null()) };
    // SAFETY: as above.
    let destroy_status: i32 = unsafe { impulse_centrifugo_client_destroy(ptr::null_mut()) };
    assert_eq!(
        connect_status,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );
    assert_eq!(
        disconnect_status,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );
    assert_eq!(
        destroy_status,
        ImpulseCentrifugoStatus::InvalidArgument as i32
    );
}
