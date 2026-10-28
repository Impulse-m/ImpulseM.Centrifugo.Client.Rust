//! The redirects of an rpc pool call: the outcome the redirect hook sees, the
//! URL check, the bounded fetch of the bytes, and the cap on fetches in flight.
//!
//! A redirect fetch is a GET that sends only the redirect's headers: no
//! cookies and no credentials of the client. It never follows a redirect,
//! so a 3xx answer fails it. Its body is read under a byte limit, as the
//! bytes arrive. A `https` URL is always allowed; an `http` URL only when the
//! endpoint of the call is not secure (`wss` endpoints take `https` alone).
//! The fetches of one pool share `max_redirects_inflight` slots, and a fetch
//! that finds no free slot waits for one in arrival order.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use centrifuge_client_core::proto;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::RpcResult;
use crate::error::Error;
use crate::rpc_tracker::local_error;
use crate::transport::net::Net;

/// The code of a redirect that did not complete: the URL was refused, or the
/// fetch failed, or its status, its headers or its body was refused.
pub(crate) const CODE_REDIRECT_FAILED: u32 = 18;

/// What the redirect hook sees of a call that reached a connection: the reply,
/// or the error the call failed with on its connection. A timeout, a cancel
/// and a close are the pool's own outcomes, and the hook does not see them.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RpcPoolOutcome {
    /// The server answered the call.
    Reply(RpcResult),
    /// The call failed on its connection: a server error reply, or a
    /// connection that was lost with the call on it.
    Error(Error),
}

/// Bytes that the pool fetches from a URL instead of taking them from the
/// reply. The redirect hook returns one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RpcPoolRedirect {
    /// The absolute URL of the bytes. It must be `https`, or `http` when the
    /// endpoint of the call is not secure. Any other URL fails the call with
    /// code 18, with no request.
    pub url: String,
    /// Headers of the fetch. They are the only headers it sends.
    pub headers: HashMap<String, String>,
    /// Body bytes accepted at most. `None` means the pool's
    /// `max_redirect_bytes`.
    pub max_bytes: Option<usize>,
}

impl RpcPoolRedirect {
    /// A redirect to a URL, with no headers and the pool's byte limit.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: HashMap::new(),
            max_bytes: None,
        }
    }
}

/// Whether an endpoint is secure: a `wss:` or `https:` endpoint.
pub(crate) fn is_secure_endpoint(endpoint: &str) -> bool {
    let lowered: String = endpoint.to_ascii_lowercase();
    lowered.starts_with("wss:") || lowered.starts_with("https:")
}

/// The URL of a redirect when the endpoint of the call allows fetching it.
/// `https` is always allowed; `http` only for an endpoint that is not secure.
pub(crate) fn allowed_redirect_url(target: &str, endpoint: &str) -> Option<Url> {
    let url: Url = Url::parse(target).ok()?;
    url.host_str()?;
    match url.scheme() {
        "https" => Some(url),
        "http" if !is_secure_endpoint(endpoint) => Some(url),
        _ => None,
    }
}

/// The error of a redirect that did not complete, with its fixed message.
pub(crate) fn redirect_failed(message: &str) -> Error {
    local_error(CODE_REDIRECT_FAILED, message)
}

/// The bytes of a successful redirect fetch, as the reply of the call.
pub(crate) fn fetched_result(data: Bytes) -> RpcResult {
    proto::RpcResult {
        data,
        cached: false,
    }
    .into()
}

/// The fetch slots of one pool: at most `limit` fetches hold a slot at once,
/// and the others wait for one.
#[derive(Debug, Clone)]
pub(crate) struct RedirectGate {
    /// Holds one permit per free slot.
    slots: Arc<Semaphore>,
}

impl RedirectGate {
    /// A gate with `limit` slots.
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limit)),
        }
    }

    /// Waits for a free slot, in arrival order. The slot is held until the
    /// permit is dropped.
    pub(crate) async fn acquire(&self) -> Result<OwnedSemaphorePermit, Error> {
        Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|_: tokio::sync::AcquireError| Error::ClientClosed)
    }
}

/// Fetches the bytes of a redirect over the transport settings of the main
/// client. The body is read as it arrives, and refused past `max_bytes`.
///
/// # Errors
///
/// A message naming the step that refused the fetch: the connection, the
/// request, a status other than 2xx, a declared or received body over the
/// limit, or the body read itself.
#[cfg(feature = "http-stream")]
pub(crate) async fn fetch(
    net: &Net,
    url: &Url,
    headers: &HashMap<String, String>,
    max_bytes: usize,
) -> Result<Bytes, &'static str> {
    use http_body_util::{BodyExt as _, Empty};
    use hyper::body::Body as _;
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;

    let io = net
        .connect(url, &[b"http/1.1"])
        .await
        .map_err(|_: crate::transport::net::NetError| "redirect fetch failed")?;
    let (mut sender, connection) = http1::handshake(TokioIo::new(io))
        .await
        .map_err(|_: hyper::Error| "redirect fetch failed")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request: http::Request<Empty<Bytes>> = redirect_request(url, headers)?;
    let response: http::Response<hyper::body::Incoming> = sender
        .send_request(request)
        .await
        .map_err(|_: hyper::Error| "redirect fetch failed")?;
    if !response.status().is_success() {
        return Err("redirect fetch status");
    }
    let limit: u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if response.body().size_hint().lower() > limit {
        return Err("redirect body over the limit");
    }
    let mut body: hyper::body::Incoming = response.into_body();
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame: hyper::body::Frame<Bytes> =
            frame.map_err(|_: hyper::Error| "redirect fetch failed")?;
        if let Ok(data) = frame.into_data() {
            if buffer.len().saturating_add(data.len()) > max_bytes {
                return Err("redirect body over the limit");
            }
            buffer.extend_from_slice(&data);
        }
    }
    Ok(Bytes::from(buffer))
}

/// Without the `http-stream` feature there is no HTTP client, so a redirect
/// fails at once.
#[cfg(not(feature = "http-stream"))]
pub(crate) async fn fetch(
    net: &Net,
    url: &Url,
    headers: &HashMap<String, String>,
    max_bytes: usize,
) -> Result<Bytes, &'static str> {
    let _ = (net, url, headers, max_bytes);
    Err("redirect fetch needs the http-stream feature")
}

/// The request of a redirect fetch: a GET to the URL's path, with the
/// redirect's headers only.
#[cfg(feature = "http-stream")]
fn redirect_request(
    url: &Url,
    headers: &HashMap<String, String>,
) -> Result<http::Request<http_body_util::Empty<Bytes>>, &'static str> {
    let path: String = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    let mut builder: http::request::Builder = http::Request::get(path);
    let authority: String = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_owned(),
    };
    builder = builder.header(http::header::HOST, authority);
    for (name, value) in headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder
        .body(http_body_util::Empty::new())
        .map_err(|_: http::Error| "redirect headers are not valid HTTP")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use url::Url;

    use super::{RedirectGate, allowed_redirect_url, fetch, is_secure_endpoint};
    use crate::config::{ProxyConfig, TlsConfig};
    use crate::transport::net::Net;

    fn direct_net() -> Net {
        Net::new(
            ProxyConfig::None,
            &TlsConfig::default(),
            Duration::from_secs(5),
        )
        .unwrap_or_else(|_: crate::Error| panic!("the direct net must build"))
    }

    /// A local server that answers one request with `body` under the given
    /// status line, and returns its URL.
    async fn serve_once(status: &'static str, body: &'static [u8]) -> Url {
        let listener: TcpListener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|_: std::io::Error| panic!("bind a local port"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|_: std::io::Error| panic!("a local address"));
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request: [u8; 4096] = [0; 4096];
            let _ = socket.read(&mut request).await;
            let head: String = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body).await;
            let _ = socket.shutdown().await;
        });
        Url::parse(&format!("http://{address}/bytes"))
            .unwrap_or_else(|_: url::ParseError| panic!("a local URL"))
    }

    #[test]
    fn redirect_url_rules_follow_the_endpoint() {
        let secure: &str = "wss://pool.example.com/connection/websocket";
        let plain: &str = "ws://127.0.0.1:8000/connection/websocket";
        assert!(allowed_redirect_url("https://files.example.com/a", secure).is_some());
        assert!(allowed_redirect_url("http://files.example.com/a", secure).is_none());
        assert!(allowed_redirect_url("http://files.example.com/a", plain).is_some());
        assert!(allowed_redirect_url("ftp://files.example.com/a", plain).is_none());
        assert!(allowed_redirect_url("not a url", plain).is_none());
        assert!(is_secure_endpoint("WSS://pool.example.com"));
        assert!(!is_secure_endpoint("ws://pool.example.com"));
    }

    #[tokio::test]
    async fn redirect_body_over_the_limit_fails() {
        let url: Url = serve_once("200 OK", b"0123456789abcdefghij").await;
        let headers: HashMap<String, String> = HashMap::new();
        let refused = fetch(&direct_net(), &url, &headers, 10).await;
        assert_eq!(refused, Err("redirect body over the limit"));
    }

    #[tokio::test]
    async fn redirect_body_within_the_limit_is_returned() {
        let url: Url = serve_once("200 OK", b"0123456789abcdefghij").await;
        let headers: HashMap<String, String> = HashMap::new();
        let body: Result<Bytes, &'static str> = fetch(&direct_net(), &url, &headers, 20).await;
        assert_eq!(body, Ok(Bytes::from_static(b"0123456789abcdefghij")));
    }

    #[tokio::test]
    async fn redirect_status_other_than_2xx_fails() {
        let url: Url = serve_once("302 Found", b"moved").await;
        let headers: HashMap<String, String> = HashMap::new();
        let refused = fetch(&direct_net(), &url, &headers, 1024).await;
        assert_eq!(refused, Err("redirect fetch status"));
    }

    #[tokio::test]
    async fn redirect_gate_holds_at_most_its_limit() {
        let gate: RedirectGate = RedirectGate::new(2);
        let first = gate.acquire().await.ok();
        let second = gate.acquire().await.ok();
        assert!(first.is_some() && second.is_some());
        let third = tokio::time::timeout(Duration::from_millis(50), gate.acquire()).await;
        assert!(third.is_err(), "a third fetch must wait for a slot");
        drop(first);
        let fourth = tokio::time::timeout(Duration::from_secs(1), gate.acquire()).await;
        assert!(
            matches!(fourth, Ok(Ok(_))),
            "a freed slot admits the waiting fetch"
        );
    }
}
