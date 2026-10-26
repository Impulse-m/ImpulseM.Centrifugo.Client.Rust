//! Configuration types (docs/spec.md §4.2, §5.2).

use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::error::Error;
use crate::maybe_send::{MaybeSend, MaybeSync};
pub use centrifuge_core::codec::Protocol;
pub use centrifuge_core::machine::TransportKind;

/// One transport endpoint of a fallback list (docs/spec.md §5.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportEndpoint {
    pub(crate) kind: TransportKind,
    pub(crate) url: String,
}

impl TransportEndpoint {
    /// A WebSocket endpoint (`ws://` or `wss://`).
    #[must_use]
    pub fn websocket(url: impl Into<String>) -> Self {
        Self {
            kind: TransportKind::WebSocket,
            url: url.into(),
        }
    }

    /// An HTTP-streaming endpoint (`http://` or `https://`), with bidirectional
    /// emulation through the `/emulation` endpoint.
    #[must_use]
    pub fn http_stream(url: impl Into<String>) -> Self {
        Self {
            kind: TransportKind::HttpStream,
            url: url.into(),
        }
    }

    /// The transport kind.
    #[must_use]
    pub fn kind(&self) -> TransportKind {
        self.kind
    }

    /// The endpoint URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// Where event handlers run (docs/spec.md §9.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum EventDelivery {
    /// A dedicated thread per client (the default). A handler that blocks stalls
    /// only this client's events, never the async runtime, and blocking on a
    /// call from a handler works.
    #[default]
    Thread,
    /// On the client's own task: no extra thread, for tools running thousands
    /// of clients. Handlers must not block.
    Inline,
}

/// Credentials for a proxy. `Debug` never prints the password.
#[derive(Clone, PartialEq, Eq)]
pub struct ProxyAuth {
    /// User name.
    pub username: String,
    /// Password.
    pub password: String,
}

impl ProxyAuth {
    /// Credentials.
    #[must_use]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl std::fmt::Debug for ProxyAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyAuth")
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

/// Where one connection goes (a resolved proxy decision).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProxyTarget {
    /// No proxy.
    Direct,
    /// Through an HTTP(S) proxy, with a CONNECT tunnel.
    Http {
        /// The proxy (`http://` or `https://`).
        url: Url,
        /// Basic credentials.
        auth: Option<ProxyAuth>,
    },
    /// Through a SOCKS5 proxy.
    Socks5 {
        /// The proxy (`socks5://` or `socks5h://`).
        url: Url,
        /// Username/password credentials.
        auth: Option<ProxyAuth>,
        /// Let the proxy resolve the host name (`socks5h`).
        remote_dns: bool,
    },
}

/// Proxy configuration (docs/spec.md §5.2).
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum ProxyConfig {
    /// Connect directly.
    None,
    /// `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY` and `NO_PROXY`, as curl reads them.
    FromEnv,
    /// The environment first, then the OS proxy settings (macOS System
    /// Configuration, Windows registry). The default.
    #[default]
    System,
    /// Always this HTTP(S) proxy.
    Http {
        /// The proxy URL.
        url: Url,
        /// Basic credentials.
        auth: Option<ProxyAuth>,
    },
    /// Always this SOCKS5 proxy.
    Socks5 {
        /// The proxy URL.
        url: Url,
        /// Credentials.
        auth: Option<ProxyAuth>,
        /// Let the proxy resolve host names.
        remote_dns: bool,
    },
    /// Decide per endpoint.
    Custom(Arc<dyn Fn(&Url) -> ProxyTarget + Send + Sync>),
}

impl ProxyConfig {
    /// A custom per-endpoint decision.
    pub fn custom(f: impl Fn(&Url) -> ProxyTarget + MaybeSend + MaybeSync + 'static) -> Self {
        Self::Custom(Arc::new(f))
    }
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::FromEnv => f.write_str("FromEnv"),
            Self::System => f.write_str("System"),
            Self::Http { url, auth } => f
                .debug_struct("Http")
                .field("url", &url.as_str())
                .field("auth", auth)
                .finish(),
            Self::Socks5 {
                url,
                auth,
                remote_dns,
            } => f
                .debug_struct("Socks5")
                .field("url", &url.as_str())
                .field("auth", auth)
                .field("remote_dns", remote_dns)
                .finish(),
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

/// TLS configuration (docs/spec.md §5.2). The default verifies with the OS
/// trust store and its settings (`rustls-platform-verifier`).
#[derive(Clone, Default)]
pub struct TlsConfig {
    pub(crate) extra_roots: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub(crate) accept_invalid: bool,
    #[cfg(feature = "rustls-config")]
    pub(crate) custom: Option<Arc<rustls::ClientConfig>>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("extra_roots", &self.extra_roots.len())
            .field("accept_invalid", &self.accept_invalid)
            .finish_non_exhaustive()
    }
}

impl TlsConfig {
    /// The OS trust store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Also trusts the certificates in a PEM bundle (e.g. a corporate CA).
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] if the PEM holds no certificate or can't be parsed.
    pub fn add_root_pem(mut self, pem: &[u8]) -> Result<Self, Error> {
        use rustls::pki_types::pem::PemObject as _;
        let before = self.extra_roots.len();
        for cert in rustls::pki_types::CertificateDer::pem_slice_iter(pem) {
            self.extra_roots.push(
                cert.map_err(|e| Error::Configuration(format!("invalid PEM certificate: {e}")))?,
            );
        }
        if self.extra_roots.len() == before {
            return Err(Error::Configuration(
                "PEM bundle holds no certificate".into(),
            ));
        }
        Ok(self)
    }

    /// Accepts any certificate. **For development only**: a warning is logged on
    /// every connect.
    #[must_use]
    pub fn danger_accept_invalid_certs(mut self) -> Self {
        self.accept_invalid = true;
        self
    }

    /// Uses a fully custom rustls configuration. Exempt from semver guarantees,
    /// because rustls is pre-1.0.
    #[cfg(feature = "rustls-config")]
    #[must_use]
    pub fn rustls_client_config(cfg: rustls::ClientConfig) -> Self {
        Self {
            custom: Some(Arc::new(cfg)),
            ..Self::default()
        }
    }
}

/// Client defaults (docs/spec.md §4.2).
pub(crate) mod defaults {
    use std::time::Duration;

    pub(crate) const TIMEOUT: Duration = Duration::from_secs(5);
    pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
    pub(crate) const MIN_RECONNECT_DELAY: Duration = Duration::from_millis(500);
    pub(crate) const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(20);
    pub(crate) const MAX_SERVER_PING_DELAY: Duration = Duration::from_secs(10);
    pub(crate) const MAX_MESSAGE_SIZE: usize = 64 << 20;
    pub(crate) const MIN_RESUBSCRIBE_DELAY: Duration = Duration::from_millis(500);
    pub(crate) const MAX_RESUBSCRIBE_DELAY: Duration = Duration::from_secs(20);
}

pub(crate) fn nonzero(name: &str, d: Duration) -> Result<Duration, Error> {
    if d.is_zero() {
        return Err(Error::Configuration(format!("{name} must not be zero")));
    }
    Ok(d)
}

/// Headers the transport manages and the app may not set (docs/spec.md §5.1).
pub(crate) fn check_http_headers(h: &http::HeaderMap) -> Result<(), Error> {
    for name in h.keys() {
        let n = name.as_str();
        if matches!(
            n,
            "host" | "connection" | "upgrade" | "content-length" | "transfer-encoding"
        ) || n.starts_with("sec-websocket-")
        {
            return Err(Error::Configuration(format!(
                "header {n} is managed by the transport"
            )));
        }
    }
    Ok(())
}
