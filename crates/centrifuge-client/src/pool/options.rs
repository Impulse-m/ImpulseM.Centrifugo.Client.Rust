//! Options of the rpc pool: the endpoints of each key, the settings of its
//! connections, and the options of a call and the statistics of the pool.
//!
//! [`RpcPoolOptions::new`] fills in the defaults of the fork. [`validate`]
//! refuses an option that cannot work, and [`validate_call`] checks the
//! options of one call against the pool's.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};

use super::redirect::{RpcPoolOutcome, RpcPoolRedirect};
use crate::ServerError;
use crate::client::ConnectionTokenContext;
use crate::config::defaults;
use crate::config::{MAX_DURATION, check_http_headers, duration};
use crate::driver::{ConnTokenGetter, TokenFuture};
use crate::error::{Error, TokenError};
use crate::maybe_send::{MaybeSend, MaybeSync};
use crate::rpc_call::validate_timeout;

/// The most redirect fetches a pool may run at once.
pub(crate) const MAX_REDIRECTS_INFLIGHT: usize = 16;

/// The pool's reconnect backoff minimum, the Swift fork's default (the main
/// client's backoff is 1 s to 30 s).
const POOL_MIN_RECONNECT_DELAY: Duration = Duration::from_millis(500);

/// The pool's reconnect backoff maximum, the Swift fork's default.
const POOL_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(20);

/// Maps an endpoint key to the URLs of its endpoint.
pub(crate) trait EndpointFn: Fn(&str) -> Vec<String> + MaybeSend + MaybeSync {}

impl<F: Fn(&str) -> Vec<String> + MaybeSend + MaybeSync> EndpointFn for F {}

/// Maps a server error reply to the endpoint key that the call moves to.
pub(crate) trait RerouteFn:
    Fn(&ServerError) -> Option<String> + MaybeSend + MaybeSync
{
}

impl<F: Fn(&ServerError) -> Option<String> + MaybeSend + MaybeSync> RerouteFn for F {}

/// Maps the outcome of a call that reached a connection to the bytes the pool
/// fetches, or to no redirect.
pub(crate) trait RedirectFn:
    Fn(&RpcPoolOutcome) -> Option<RpcPoolRedirect> + MaybeSend + MaybeSync
{
}

impl<F: Fn(&RpcPoolOutcome) -> Option<RpcPoolRedirect> + MaybeSend + MaybeSync> RedirectFn for F {}

/// Options of an [`RpcPool`](crate::RpcPool).
///
/// The connections of the pool are plain clients of the endpoint URLs, built
/// with the connection settings below. Calls are plain requests: no instance,
/// no message id.
///
/// ```
/// use centrifuge::RpcPoolOptions;
///
/// let options: RpcPoolOptions = RpcPoolOptions::new(|key: &str| {
///     vec![format!("wss://{key}.example.com/connection/websocket")]
/// });
/// assert_eq!(options.max_connections_per_endpoint, 2);
/// ```
pub struct RpcPoolOptions {
    /// The URLs of an endpoint key. An empty list means the key has no
    /// endpoint: its calls fail with a configuration error, and the next
    /// call asks again.
    pub(crate) endpoints: Arc<dyn EndpointFn>,
    /// Headers of each connection, sent on its WebSocket upgrade or stream
    /// request.
    pub headers: HashMap<String, String>,
    /// Client name of each connection (at most 16 characters).
    pub name: String,
    /// Client version of each connection (at most 64 characters).
    pub version: String,
    /// Deadline of a call, unless the call gives its own, and the reply
    /// timeout of each connection. Must be above zero.
    pub timeout: Duration,
    /// Minimum hold of an endpoint after a connection ended for good.
    pub min_reconnect_delay: Duration,
    /// Maximum of that hold.
    pub max_reconnect_delay: Duration,
    /// Maximum allowed time between pings from the server of each connection.
    pub max_server_ping_delay: Duration,
    /// Name of the header that carries a call's affinity. `None` means calls
    /// have no affinity.
    pub affinity_header: Option<String>,
    /// Connections opened per endpoint URL at most.
    pub max_connections_per_endpoint: usize,
    /// Calls in flight per connection at most.
    pub max_inflight_per_connection: usize,
    /// Calls waiting for a connection at most, over all keys. Beyond it a
    /// call fails with code 16, unless it can open a connection. Zero is
    /// valid.
    pub max_queued: usize,
    /// How long a connection without calls stays open. Zero keeps it open
    /// until the pool closes.
    pub idle_timeout: Duration,
    /// Resends of one call at most, counted over all causes together: a call
    /// lost with its connection, a server error that carries the later
    /// signal, and a move by the reroute hook. Zero sends no resend.
    pub retries: usize,
    /// Redirect fetches in flight at most, over the pool, from 1 to 16. A
    /// fetch that finds no free slot waits for one in arrival order.
    pub max_redirects_inflight: usize,
    /// Body bytes a redirect fetch accepts at most, unless its redirect gives
    /// its own limit.
    pub max_redirect_bytes: usize,
    /// The token of each connection, for a pool without `token_from`. Empty
    /// means no token. Exclusive with `token_from` and the token getter.
    pub token: String,
    /// Whether the connections take their token from the main client: each is
    /// lent the main client's confirmed token, the one the server accepted at
    /// its last connect or accepted refresh. A connection refused its token
    /// asks the main client for one renewal per confirmed token. Exclusive
    /// with `token` and the token getter.
    pub token_from: bool,
    /// Closes the idle connections once a call finds the main client closed,
    /// instead of at the idle timeout.
    pub close_ended_session: bool,
    /// Maps a server error reply to an endpoint key. A reply that maps to a
    /// key moves the call to that key, at the front of its line, while
    /// `retries` and the call's deadline allow. Runs on server error replies
    /// only.
    pub(crate) reroute: Option<Arc<dyn RerouteFn>>,
    /// Maps the outcome of a call that reached a connection to the bytes the
    /// pool fetches. Runs on a reply or on an error the call failed with on
    /// its connection, never on a timeout, a cancel or a close.
    pub(crate) redirect: Option<Arc<dyn RedirectFn>>,
    /// The token getter of each connection, for a pool without `token_from`.
    pub(crate) token_getter: Option<ConnTokenGetter>,
}

impl RpcPoolOptions {
    /// Options with the defaults of the fork, for the given endpoints.
    pub fn new(endpoints: impl Fn(&str) -> Vec<String> + MaybeSend + MaybeSync + 'static) -> Self {
        Self {
            endpoints: Arc::new(endpoints),
            headers: HashMap::new(),
            name: "rust".to_owned(),
            version: String::new(),
            timeout: defaults::TIMEOUT,
            min_reconnect_delay: POOL_MIN_RECONNECT_DELAY,
            max_reconnect_delay: POOL_MAX_RECONNECT_DELAY,
            max_server_ping_delay: defaults::MAX_SERVER_PING_DELAY,
            affinity_header: None,
            max_connections_per_endpoint: 2,
            max_inflight_per_connection: 1,
            max_queued: 64,
            idle_timeout: Duration::from_secs(60),
            retries: 3,
            max_redirects_inflight: 4,
            max_redirect_bytes: 16 * 1024 * 1024,
            token: String::new(),
            token_from: false,
            close_ended_session: true,
            reroute: None,
            redirect: None,
            token_getter: None,
        }
    }

    /// Sets the reroute hook: it maps a server error reply to the endpoint key
    /// that the call moves to, or to `None` for no move.
    #[must_use]
    pub fn reroute(
        mut self,
        hook: impl Fn(&ServerError) -> Option<String> + MaybeSend + MaybeSync + 'static,
    ) -> Self {
        self.reroute = Some(Arc::new(hook));
        self
    }

    /// Sets the redirect hook: it maps the outcome of a call that reached a
    /// connection to the bytes the pool fetches, or to `None` for no redirect.
    #[must_use]
    pub fn redirect(
        mut self,
        hook: impl Fn(&RpcPoolOutcome) -> Option<RpcPoolRedirect> + MaybeSend + MaybeSync + 'static,
    ) -> Self {
        self.redirect = Some(Arc::new(hook));
        self
    }

    /// Sets the token getter of each connection. It is exclusive with `token`
    /// and `token_from`.
    #[must_use]
    pub fn get_token<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(ConnectionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<Option<String>, TokenError>> + MaybeSend + 'static,
    {
        self.token_getter = Some(Arc::new(
            move |ctx: ConnectionTokenContext| -> TokenFuture { Box::pin(f(ctx)) },
        ));
        self
    }
}

impl std::fmt::Debug for RpcPoolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcPoolOptions")
            .field("endpoints", &"<fn>")
            .field("headers", &self.headers.len())
            .field("name", &self.name)
            .field("version", &self.version)
            .field("timeout", &self.timeout)
            .field("min_reconnect_delay", &self.min_reconnect_delay)
            .field("max_reconnect_delay", &self.max_reconnect_delay)
            .field("max_server_ping_delay", &self.max_server_ping_delay)
            .field("affinity_header", &self.affinity_header)
            .field(
                "max_connections_per_endpoint",
                &self.max_connections_per_endpoint,
            )
            .field(
                "max_inflight_per_connection",
                &self.max_inflight_per_connection,
            )
            .field("max_queued", &self.max_queued)
            .field("idle_timeout", &self.idle_timeout)
            .field("retries", &self.retries)
            .field("max_redirects_inflight", &self.max_redirects_inflight)
            .field("max_redirect_bytes", &self.max_redirect_bytes)
            .field("has_token", &!self.token.is_empty())
            .field("token_from", &self.token_from)
            .field("token_getter", &self.token_getter.is_some())
            .field("reroute", &self.reroute.is_some())
            .field("redirect", &self.redirect.is_some())
            .field("close_ended_session", &self.close_ended_session)
            .finish()
    }
}

/// Options of one pool call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RpcPoolCallOptions {
    /// Headers sent with the call. A header named as the pool's
    /// `affinity_header` is refused.
    pub headers: HashMap<String, String>,
    /// Deadline of the call. `None` means the pool's `timeout`. An explicit
    /// value must be above zero and no more than the pool's `timeout`.
    pub timeout: Option<Duration>,
    /// Affinity hint, sent in the pool's `affinity_header`. Needs that option.
    pub affinity: Option<String>,
}

/// The state of a pool, for the application's own pacing. A snapshot: it may
/// lag a call that is in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcPoolStats {
    /// Connections open or connecting.
    pub connections: usize,
    /// Calls holding a slot of a connection.
    pub inflight: usize,
    /// Calls waiting for a connection, and redirect fetches waiting for a
    /// slot.
    pub queued: usize,
    /// Redirect fetches holding a slot.
    pub redirects: usize,
}

/// Checks the pool options and returns the connection headers as a header
/// map.
///
/// # Errors
///
/// [`Error::Configuration`] for an option that cannot work, or for a header
/// the transport manages or that is not valid HTTP.
pub(crate) fn validate(options: &RpcPoolOptions) -> Result<HeaderMap, Error> {
    if options
        .affinity_header
        .as_ref()
        .is_some_and(String::is_empty)
    {
        return Err(Error::Configuration(
            "affinity_header must be a header name".into(),
        ));
    }
    if options.max_connections_per_endpoint < 1 {
        return Err(Error::Configuration(
            "max_connections_per_endpoint must be at least 1".into(),
        ));
    }
    if options.max_inflight_per_connection < 1 {
        return Err(Error::Configuration(
            "max_inflight_per_connection must be at least 1".into(),
        ));
    }
    if options.idle_timeout > MAX_DURATION {
        return Err(Error::Configuration(
            "idle_timeout must not be longer than 24 hours".into(),
        ));
    }
    if options.max_redirects_inflight < 1 || options.max_redirects_inflight > MAX_REDIRECTS_INFLIGHT
    {
        return Err(Error::Configuration(
            "max_redirects_inflight must be from 1 to 16".into(),
        ));
    }
    if options.token_from && (!options.token.is_empty() || options.token_getter.is_some()) {
        return Err(Error::Configuration(
            "token_from is exclusive with token and get_token".into(),
        ));
    }
    duration("timeout", options.timeout)?;
    duration("min_reconnect_delay", options.min_reconnect_delay)?;
    duration("max_reconnect_delay", options.max_reconnect_delay)?;
    duration("max_server_ping_delay", options.max_server_ping_delay)?;
    let headers: HeaderMap = header_map(&options.headers)?;
    check_http_headers(&headers)?;
    Ok(headers)
}

/// The deadline of one call, checked against the pool's options.
///
/// # Errors
///
/// [`Error::Configuration`] when the call names an affinity without the
/// pool's affinity header, sets that header itself, or has a timeout that is
/// zero or above the pool's.
pub(crate) fn validate_call(
    call: &RpcPoolCallOptions,
    pool: &RpcPoolOptions,
) -> Result<Duration, Error> {
    if call.affinity.is_some() && pool.affinity_header.is_none() {
        return Err(Error::Configuration(
            "affinity needs the affinity_header option".into(),
        ));
    }
    if let Some(name) = &pool.affinity_header {
        let repeated: bool = call
            .headers
            .keys()
            .any(|key: &String| key.eq_ignore_ascii_case(name));
        if repeated {
            return Err(Error::Configuration(
                "the affinity header is set by the affinity option, not by headers".into(),
            ));
        }
    }
    validate_timeout(call.timeout, pool.timeout)
}

/// Converts the connection headers to a header map.
fn header_map(headers: &HashMap<String, String>) -> Result<HeaderMap, Error> {
    let mut map: HeaderMap = HeaderMap::new();
    for (name, value) in headers {
        let name: HeaderName = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| Error::Configuration("pool header name is not valid HTTP".into()))?;
        let value: HeaderValue = HeaderValue::from_str(value)
            .map_err(|_| Error::Configuration("pool header value is not valid HTTP".into()))?;
        map.append(name, value);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{RpcPoolCallOptions, RpcPoolOptions, validate, validate_call};
    use crate::error::Error;

    fn options() -> RpcPoolOptions {
        RpcPoolOptions::new(|_: &str| vec!["ws://127.0.0.1:1/connection/websocket".to_owned()])
    }

    fn is_configuration(result: &Result<impl Sized, Error>) -> bool {
        matches!(result, Err(Error::Configuration(_)))
    }

    #[test]
    fn pool_defaults_equal_the_swift_fork() {
        let pool: RpcPoolOptions = options();
        assert_eq!(pool.timeout, Duration::from_secs(5));
        assert_eq!(pool.min_reconnect_delay, Duration::from_millis(500));
        assert_eq!(pool.max_reconnect_delay, Duration::from_secs(20));
        assert_eq!(pool.max_server_ping_delay, Duration::from_secs(10));
        assert_eq!(pool.affinity_header, None);
        assert_eq!(pool.max_connections_per_endpoint, 2);
        assert_eq!(pool.max_inflight_per_connection, 1);
        assert_eq!(pool.max_queued, 64);
        assert_eq!(pool.idle_timeout, Duration::from_secs(60));
        assert!(pool.close_ended_session);
        assert!(pool.headers.is_empty());
        assert_eq!(pool.retries, 3);
        assert_eq!(pool.max_redirects_inflight, 4);
        assert_eq!(pool.max_redirect_bytes, 16 * 1024 * 1024);
        assert!(pool.token.is_empty());
        assert!(!pool.token_from);
        assert!(pool.reroute.is_none());
        assert!(pool.redirect.is_none());
        assert!(pool.token_getter.is_none());
    }

    #[test]
    fn pool_redirects_inflight_range_is_one_to_sixteen() {
        let mut low: RpcPoolOptions = options();
        low.max_redirects_inflight = 0;
        assert!(is_configuration(&validate(&low)));
        let mut high: RpcPoolOptions = options();
        high.max_redirects_inflight = 17;
        assert!(is_configuration(&validate(&high)));
        for accepted in [1_usize, 16] {
            let mut pool: RpcPoolOptions = options();
            pool.max_redirects_inflight = accepted;
            assert!(validate(&pool).is_ok());
        }
    }

    #[test]
    fn pool_token_from_is_exclusive_with_token_and_getter() {
        let mut with_token: RpcPoolOptions = options();
        with_token.token_from = true;
        with_token.token = "jwt".to_owned();
        assert!(is_configuration(&validate(&with_token)));
        let getter: RpcPoolOptions =
            options().get_token(|_: crate::ConnectionTokenContext| async { Ok(None) });
        let mut with_getter: RpcPoolOptions = getter;
        with_getter.token_from = true;
        assert!(is_configuration(&validate(&with_getter)));
        let mut alone: RpcPoolOptions = options();
        alone.token_from = true;
        assert!(validate(&alone).is_ok());
    }

    #[test]
    fn pool_default_options_are_valid() {
        assert!(validate(&options()).is_ok());
    }

    #[test]
    fn pool_rejects_zero_connections_per_endpoint() {
        let mut pool: RpcPoolOptions = options();
        pool.max_connections_per_endpoint = 0;
        assert!(is_configuration(&validate(&pool)));
    }

    #[test]
    fn pool_rejects_zero_inflight_per_connection() {
        let mut pool: RpcPoolOptions = options();
        pool.max_inflight_per_connection = 0;
        assert!(is_configuration(&validate(&pool)));
    }

    #[test]
    fn pool_accepts_zero_queue_and_zero_idle_timeout() {
        let mut pool: RpcPoolOptions = options();
        pool.max_queued = 0;
        pool.idle_timeout = Duration::ZERO;
        assert!(validate(&pool).is_ok());
    }

    #[test]
    fn pool_rejects_zero_timeout_and_empty_affinity_header() {
        let mut zero_timeout: RpcPoolOptions = options();
        zero_timeout.timeout = Duration::ZERO;
        assert!(is_configuration(&validate(&zero_timeout)));
        let mut empty_affinity: RpcPoolOptions = options();
        empty_affinity.affinity_header = Some(String::new());
        assert!(is_configuration(&validate(&empty_affinity)));
    }

    #[test]
    fn pool_rejects_a_managed_or_invalid_connection_header() {
        let mut managed: RpcPoolOptions = options();
        managed.headers = HashMap::from([("host".to_owned(), "example.com".to_owned())]);
        assert!(is_configuration(&validate(&managed)));
        let mut invalid: RpcPoolOptions = options();
        invalid.headers = HashMap::from([("x bad name".to_owned(), "v".to_owned())]);
        assert!(is_configuration(&validate(&invalid)));
    }

    #[test]
    fn pool_call_affinity_needs_the_affinity_header() {
        let call: RpcPoolCallOptions = RpcPoolCallOptions {
            affinity: Some("shard-1".to_owned()),
            ..Default::default()
        };
        assert!(is_configuration(&validate_call(&call, &options())));
        let mut pool: RpcPoolOptions = options();
        pool.affinity_header = Some("x-affinity".to_owned());
        assert!(validate_call(&call, &pool).is_ok());
    }

    #[test]
    fn pool_call_header_may_not_repeat_the_affinity_header() {
        let mut pool: RpcPoolOptions = options();
        pool.affinity_header = Some("x-affinity".to_owned());
        let call: RpcPoolCallOptions = RpcPoolCallOptions {
            headers: HashMap::from([("X-Affinity".to_owned(), "a".to_owned())]),
            ..Default::default()
        };
        assert!(is_configuration(&validate_call(&call, &pool)));
    }

    #[test]
    fn pool_call_timeout_is_at_most_the_pool_timeout() {
        let above: RpcPoolCallOptions = RpcPoolCallOptions {
            timeout: Some(Duration::from_secs(6)),
            ..Default::default()
        };
        assert!(is_configuration(&validate_call(&above, &options())));
        let within: RpcPoolCallOptions = RpcPoolCallOptions {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        assert_eq!(
            validate_call(&within, &options()).ok(),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            validate_call(&RpcPoolCallOptions::default(), &options()).ok(),
            Some(Duration::from_secs(5))
        );
    }
}
