//! Local HTTP CONNECT and SOCKS5 proxies that relay to any address, for the
//! proxy tests against the fake server and against the live server.

#![allow(
    clippy::many_single_char_names,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)] // test code

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

/// What a test proxy saw.
#[derive(Default)]
pub struct Seen {
    pub connects: AtomicUsize,
    pub requests: std::sync::Mutex<Vec<String>>,
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
pub async fn http_proxy(auth: Option<&'static str>) -> (u16, Arc<Seen>) {
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
pub async fn socks5_proxy(auth: Option<(&'static str, &'static str)>) -> (u16, Arc<Seen>) {
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
