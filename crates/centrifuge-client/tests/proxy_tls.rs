//! Proxies (HTTP CONNECT with Basic auth, SOCKS5 with username/password and
//! both address types, per-endpoint decisions) and TLS (OS trust store, extra
//! roots, accept-invalid) end to end, over both transports.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::many_single_char_names,
    clippy::print_stderr
)] // test code

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use centrifuge::{
    Client, Error, ProxyAuth, ProxyConfig, ProxyTarget, State, TlsConfig, TransportEndpoint,
};
use common::server;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

/// What a test proxy saw.
#[derive(Default)]
struct Seen {
    connects: AtomicUsize,
    requests: std::sync::Mutex<Vec<String>>,
}

async fn pipe(a: TcpStream, b: TcpStream) {
    let (mut ar, mut aw) = a.into_split();
    let (mut br, mut bw) = b.into_split();
    let _ = tokio::join!(
        tokio::io::copy(&mut ar, &mut bw),
        tokio::io::copy(&mut br, &mut aw)
    );
}

/// An HTTP CONNECT proxy; with `auth`, it answers 407 without the right
/// `Proxy-Authorization`.
async fn http_proxy(auth: Option<&'static str>) -> (u16, Arc<Seen>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let seen = Arc::new(Seen::default());
    let s = seen.clone();
    tokio::spawn(async move {
        loop {
            let (mut c, _) = l.accept().await.unwrap();
            let s = s.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut b = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") {
                    if c.read(&mut b).await.unwrap_or(0) == 0 {
                        return;
                    }
                    buf.push(b[0]);
                }
                let req = String::from_utf8_lossy(&buf).to_string();
                s.requests.lock().unwrap().push(req.clone());
                let target = req.split_whitespace().nth(1).unwrap_or_default().to_owned();
                if let Some(a) = auth {
                    if !req.contains(&format!("Proxy-Authorization: Basic {a}")) {
                        let _ = c.write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n").await;
                        return;
                    }
                }
                let Ok(up) = TcpStream::connect(&target).await else {
                    return;
                };
                s.connects.fetch_add(1, Ordering::SeqCst);
                c.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await
                    .unwrap();
                pipe(c, up).await;
            });
        }
    });
    (port, seen)
}

/// A SOCKS5 proxy (RFC 1928, RFC 1929 auth when `auth` is set). Records the
/// address type requested: "domain", "ipv4" or "ipv6".
async fn socks5_proxy(auth: Option<(&'static str, &'static str)>) -> (u16, Arc<Seen>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let seen = Arc::new(Seen::default());
    let s = seen.clone();
    tokio::spawn(async move {
        loop {
            let (mut c, _) = l.accept().await.unwrap();
            let s = s.clone();
            tokio::spawn(async move {
                let mut h = [0u8; 2];
                c.read_exact(&mut h).await.unwrap();
                let mut methods = vec![0u8; usize::from(h[1])];
                c.read_exact(&mut methods).await.unwrap();
                if let Some((user, pass)) = auth {
                    assert!(methods.contains(&2), "offers username/password");
                    c.write_all(&[5, 2]).await.unwrap();
                    let mut v = [0u8; 2];
                    c.read_exact(&mut v).await.unwrap();
                    let mut u = vec![0u8; usize::from(v[1])];
                    c.read_exact(&mut u).await.unwrap();
                    let mut pl = [0u8; 1];
                    c.read_exact(&mut pl).await.unwrap();
                    let mut p = vec![0u8; usize::from(pl[0])];
                    c.read_exact(&mut p).await.unwrap();
                    let ok = u == user.as_bytes() && p == pass.as_bytes();
                    c.write_all(&[1, u8::from(!ok)]).await.unwrap();
                    if !ok {
                        return;
                    }
                } else {
                    c.write_all(&[5, 0]).await.unwrap();
                }
                let mut r = [0u8; 4];
                c.read_exact(&mut r).await.unwrap();
                let host = match r[3] {
                    1 => {
                        let mut a = [0u8; 4];
                        c.read_exact(&mut a).await.unwrap();
                        s.requests.lock().unwrap().push("ipv4".into());
                        std::net::Ipv4Addr::from(a).to_string()
                    }
                    3 => {
                        let mut n = [0u8; 1];
                        c.read_exact(&mut n).await.unwrap();
                        let mut d = vec![0u8; usize::from(n[0])];
                        c.read_exact(&mut d).await.unwrap();
                        s.requests.lock().unwrap().push("domain".into());
                        String::from_utf8(d).unwrap()
                    }
                    4 => {
                        let mut a = [0u8; 16];
                        c.read_exact(&mut a).await.unwrap();
                        s.requests.lock().unwrap().push("ipv6".into());
                        std::net::Ipv6Addr::from(a).to_string()
                    }
                    t => panic!("address type {t}"),
                };
                let mut p = [0u8; 2];
                c.read_exact(&mut p).await.unwrap();
                let port = u16::from_be_bytes(p);
                let Ok(up) = TcpStream::connect((host.as_str(), port)).await else {
                    return;
                };
                s.connects.fetch_add(1, Ordering::SeqCst);
                c.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await.unwrap();
                pipe(c, up).await;
            });
        }
    });
    (port, seen)
}

fn endpoints(srv: &centrifuge_fake_server::FakeServer, host: &str) -> [TransportEndpoint; 2] {
    let port = srv.addr().port();
    [
        TransportEndpoint::websocket(format!("ws://{host}:{port}/connection/websocket")),
        TransportEndpoint::http_stream(format!("http://{host}:{port}/connection/http_stream")),
    ]
}

async fn connect_through(ep: TransportEndpoint, proxy: ProxyConfig) -> Client {
    let client = Client::builder_with_transports([ep])
        .proxy(proxy)
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    client.connect().unwrap();
    client.ready(Some(Duration::from_secs(5))).await.unwrap();
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_connect_proxy_with_basic_auth() {
    let srv = server().await;
    // "user:secret" in base64.
    let (port, seen) = http_proxy(Some("dXNlcjpzZWNyZXQ=")).await;
    let proxy = ProxyConfig::Http {
        url: format!("http://127.0.0.1:{port}").parse().unwrap(),
        auth: Some(ProxyAuth::new("user", "secret")),
    };
    for ep in endpoints(&srv, "127.0.0.1") {
        let client = connect_through(ep.clone(), proxy.clone()).await;
        // Calls go through too (emulation POSTs for HTTP-streaming).
        client.publish("p", r#"{"x":1}"#).await.unwrap();
        client.close().await;
    }
    assert!(
        seen.connects.load(Ordering::SeqCst) >= 3,
        "stream, emulation and websocket tunnels"
    );
    let reqs = seen.requests.lock().unwrap().clone();
    assert!(
        reqs.iter()
            .all(|r| r.starts_with(&format!("CONNECT 127.0.0.1:{} HTTP/1.1", srv.addr().port()))),
        "{reqs:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_connect_proxy_refusal_is_a_transport_error_with_status() {
    let srv = server().await;
    let (port, _) = http_proxy(Some("dXNlcjpzZWNyZXQ=")).await;
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let e = errors.clone();
    let client = Client::builder(srv.ws_url())
        .proxy(ProxyConfig::Http {
            url: format!("http://127.0.0.1:{port}").parse().unwrap(),
            auth: Some(ProxyAuth::new("user", "wrong")),
        })
        .min_reconnect_delay(Duration::from_millis(100))
        .on_error(move |_, ev| e.lock().unwrap().push(format!("{:?}", ev.error.kind)))
        .build()
        .unwrap();
    client.connect().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(client.state(), State::Connecting, "keeps retrying");
    let errors = errors.lock().unwrap().clone();
    assert!(
        errors.iter().any(|e| e.contains("status: Some(407)")),
        "{errors:?}"
    );
    assert_eq!(srv.connects(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socks5_proxy_with_auth_and_both_address_types() {
    let srv = server().await;
    let (port, seen) = socks5_proxy(Some(("user", "secret"))).await;
    for remote_dns in [true, false] {
        let proxy = ProxyConfig::Socks5 {
            url: format!("socks5://127.0.0.1:{port}").parse().unwrap(),
            auth: Some(ProxyAuth::new("user", "secret")),
            remote_dns,
        };
        for ep in endpoints(&srv, "localhost") {
            let client = connect_through(ep, proxy.clone()).await;
            client.rpc("m", r#"{"y":2}"#).await.unwrap();
            client.close().await;
        }
    }
    let reqs = seen.requests.lock().unwrap().clone();
    // Remote DNS sends the name; local resolution an address (localhost may
    // resolve to ::1 or 127.0.0.1).
    assert!(reqs.contains(&"domain".to_owned()), "{reqs:?}");
    assert!(reqs.iter().any(|r| r == "ipv4" || r == "ipv6"), "{reqs:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_proxy_decides_per_endpoint() {
    let srv = server().await;
    let (port, seen) = http_proxy(None).await;
    let proxy_url: url::Url = format!("http://127.0.0.1:{port}").parse().unwrap();
    let proxy = ProxyConfig::custom(move |u| {
        if u.scheme() == "ws" {
            ProxyTarget::Http {
                url: proxy_url.clone(),
                auth: None,
            }
        } else {
            ProxyTarget::Direct
        }
    });
    let [ws, http] = endpoints(&srv, "127.0.0.1");
    connect_through(http, proxy.clone()).await.close().await;
    assert_eq!(
        seen.connects.load(Ordering::SeqCst),
        0,
        "HTTP-streaming went direct"
    );
    connect_through(ws, proxy).await.close().await;
    assert_eq!(
        seen.connects.load(Ordering::SeqCst),
        1,
        "WebSocket through the proxy"
    );
}

/// A TLS front for the fake server, with a self-signed certificate for
/// `localhost`. Returns its port and the certificate PEM.
async fn tls_front(upstream: std::net::SocketAddr) -> (u16, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let pem = cert.cert.pem();
    let der = cert.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![der], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (c, _) = l.accept().await.unwrap();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(c).await else {
                    return;
                };
                let Ok(up) = TcpStream::connect(upstream).await else {
                    return;
                };
                let (mut tr, mut tw) = tokio::io::split(tls);
                let (mut ur, mut uw) = up.into_split();
                let _ = tokio::join!(
                    tokio::io::copy(&mut tr, &mut uw),
                    tokio::io::copy(&mut ur, &mut tw)
                );
            });
        }
    });
    (port, pem)
}

fn tls_endpoints(port: u16) -> [TransportEndpoint; 2] {
    [
        TransportEndpoint::websocket(format!("wss://localhost:{port}/connection/websocket")),
        TransportEndpoint::http_stream(format!("https://localhost:{port}/connection/http_stream")),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_with_extra_root_and_accept_invalid() {
    let srv = server().await;
    let (port, pem) = tls_front(srv.addr()).await;
    for ep in tls_endpoints(port) {
        // Trusted through an extra root (a corporate CA).
        let tls = TlsConfig::new().add_root_pem(pem.as_bytes()).unwrap();
        let client = Client::builder_with_transports([ep.clone()])
            .proxy(ProxyConfig::None)
            .tls(tls)
            .build()
            .unwrap();
        client.connect().unwrap();
        client
            .ready(Some(Duration::from_secs(5)))
            .await
            .unwrap_or_else(|e| panic!("{ep:?}: {e}"));
        client.publish("tls", "{}").await.unwrap();
        client.close().await;
        // Development: no verification.
        let client = Client::builder_with_transports([ep])
            .proxy(ProxyConfig::None)
            .tls(TlsConfig::new().danger_accept_invalid_certs())
            .build()
            .unwrap();
        client.connect().unwrap();
        client.ready(Some(Duration::from_secs(5))).await.unwrap();
        client.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_untrusted_certificate_is_refused() {
    let srv = server().await;
    let (port, _) = tls_front(srv.addr()).await;
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let e = errors.clone();
    let [ws, _] = tls_endpoints(port);
    let client = Client::builder_with_transports([ws])
        .proxy(ProxyConfig::None)
        .min_reconnect_delay(Duration::from_millis(100))
        .on_error(move |_, ev| e.lock().unwrap().push(ev.error.to_string()))
        .build()
        .unwrap();
    client.connect().unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(client.state(), State::Connecting);
    assert_eq!(srv.connects(), 0);
    let errors = errors.lock().unwrap().clone();
    assert!(!errors.is_empty(), "the verification failure is reported");
    assert!(matches!(
        TlsConfig::new().add_root_pem(b"not a pem"),
        Err(Error::Configuration(_))
    ));
}
