//! The rpc pool: calls by endpoint key, carried by connections to the
//! endpoints that the key resolves to.
//!
//! A call takes a free slot of a connected connection of its key, or waits in
//! the line of its key while a connection opens or a slot frees. A connection
//! carries up to `max_inflight_per_connection` calls at once, and an endpoint
//! has up to `max_connections_per_endpoint` connections. A connection that
//! ends for good holds its endpoint for a backoff, and a connection with no
//! calls closes after `idle_timeout`.
//!
//! The pool takes its settings from [`RpcPoolOptions`] and reports its state
//! with [`RpcPool::stats`]. Calls fail with a configuration error for the
//! options that cannot work, with [`Error::ClientDisconnected`] while the main
//! client is not connected, with code 16 when the line is full, and with
//! [`Error::Timeout`] at their deadline.

mod endpoint;
mod options;
mod queue;
mod redirect;
mod retry;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use http::HeaderMap;
use tokio::sync::oneshot;
use tokio::time::Instant;

pub use options::{RpcPoolCallOptions, RpcPoolOptions, RpcPoolStats};

use endpoint::{Connection, Endpoint};
use options::{validate, validate_call};
use queue::{Admission, Placement, SlotResult, Waiter};

use crate::client::Client;
use crate::error::Error;
use crate::rpc_tracker::{CODE_CANCELLED, CODE_LIMIT_EXCEEDED, local_error};
use crate::shared::lock;
use crate::{Bytes, ConnectedEvent, DisconnectedEvent, RpcOptions, RpcResult, State};

/// Calls by endpoint key over the connections of one main client's pool.
///
/// Cheap to clone; clones share the pool. Dropping the last clone drops the
/// connections, which stop their clients. Waiting calls hold a clone, so they
/// finish even when the handle is dropped.
#[derive(Clone)]
pub struct RpcPool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for RpcPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcPool")
            .field("stats", &self.stats())
            .finish()
    }
}

impl RpcPool {
    /// A pool over the main client. The connections are plain clients of the
    /// endpoint URLs; the pool reads the main client's state to refuse calls
    /// while it is not connected.
    ///
    /// Must be called inside a Tokio runtime.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] for an option that cannot work, or when there
    /// is no Tokio runtime.
    pub fn new(main: &Client, options: RpcPoolOptions) -> Result<Self, Error> {
        let headers: HeaderMap = validate(&options)?;
        let runtime: tokio::runtime::Handle = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Configuration("the rpc pool needs a Tokio runtime".into()))?;
        Ok(Self {
            inner: Arc::new(PoolInner {
                main: main.clone(),
                options,
                headers,
                runtime,
                state: Mutex::new(PoolState::default()),
            }),
        })
    }

    /// Runs a call on a connection of its endpoint key, and waits for its
    /// reply.
    ///
    /// # Errors
    ///
    /// [`Error::ClientClosed`] after [`close`](Self::close);
    /// [`Error::ClientDisconnected`] while the main client is not connected;
    /// [`Error::Configuration`] for call options that cannot work, or a key
    /// with no endpoint; [`Error::Server`] with code 16 when the line is full
    /// and no connection can open, with code 17 when the pool closes while
    /// the call waits; [`Error::Timeout`] at the call's deadline;
    /// [`Error::ConnectionClosed`] when the pool closes while the call is in
    /// flight.
    pub async fn rpc(
        &self,
        endpoint_key: &str,
        method: &str,
        data: impl Into<Bytes>,
        options: RpcPoolCallOptions,
    ) -> Result<RpcResult, Error> {
        let data: Bytes = data.into();
        self.inner.check_open()?;
        let timeout: Duration = validate_call(&options, &self.inner.options)?;
        let deadline: Instant = Instant::now() + timeout;
        let urls: Vec<String> = self.inner.resolve(endpoint_key)?;
        let mut admission: Admission = self.inner.enqueue(endpoint_key, urls)?;
        let received: Result<SlotResult, oneshot::error::RecvError> = tokio::select! {
            biased;
            result = &mut admission.receiver => result,
            () = tokio::time::sleep_until(deadline) => return Err(Error::Timeout),
        };
        let placement: Placement = match received {
            Ok(Ok(placement)) => placement,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(Error::ClientClosed),
        };
        admission.slot = Some(placement.clone());
        let remaining: Duration = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::Timeout);
        }
        let call_options: RpcOptions = RpcOptions {
            headers: self.inner.call_headers(&options),
            timeout: Some(remaining),
        };
        let outcome: Result<RpcResult, Error> = placement
            .client
            .rpc_call(method, data, call_options)
            .wait()
            .await;
        drop(admission);
        outcome.map_err(|error: Error| match error {
            Error::ClientClosed => Error::ConnectionClosed,
            other => other,
        })
    }

    /// The state of the pool: its connections, the calls in flight and the
    /// calls waiting. A snapshot, which may lag a call in progress.
    #[must_use]
    pub fn stats(&self) -> RpcPoolStats {
        lock(&self.inner.state).stats()
    }

    /// Closes the pool. Calls waiting for a connection fail with code 17,
    /// calls in flight with [`Error::ConnectionClosed`], and later calls with
    /// [`Error::ClientClosed`]. The connections are disconnected.
    pub fn close(&self) {
        self.inner.close();
    }
}

/// The state shared by the pool handles and the connections' events.
#[derive(Debug)]
struct PoolInner {
    /// The main client: its state refuses the calls while it is not connected.
    main: Client,
    /// The options the pool was made with.
    options: RpcPoolOptions,
    /// The connection headers, converted from the options.
    headers: HeaderMap,
    /// Runs the pool's timers and the connections' tasks.
    runtime: tokio::runtime::Handle,
    /// Everything that changes with the calls and the connections.
    state: Mutex<PoolState>,
}

/// The state under the pool's lock.
#[derive(Debug, Default)]
struct PoolState {
    /// Set by `close`: the pool refuses calls and holds no connection.
    closed: bool,
    /// The next id of a connection or a waiting call.
    next_id: u64,
    /// The endpoint URLs of each resolved key, kept until the pool closes.
    keys: HashMap<String, Vec<String>>,
    /// The endpoints, keyed by URL.
    endpoints: HashMap<String, Endpoint>,
    /// The waiting calls of each key, in admission order.
    lines: HashMap<String, VecDeque<Waiter>>,
}

impl PoolState {
    /// A fresh id for a connection or a waiting call.
    fn take_id(&mut self) -> u64 {
        let id: u64 = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// The calls waiting in all the lines.
    fn queued(&self) -> usize {
        self.lines.values().map(VecDeque::len).sum()
    }

    /// The stats of the pool.
    fn stats(&self) -> RpcPoolStats {
        let mut stats: RpcPoolStats = RpcPoolStats {
            queued: self.queued(),
            ..Default::default()
        };
        for endpoint in self.endpoints.values() {
            stats.connections += endpoint.connections.len();
            stats.inflight += endpoint
                .connections
                .iter()
                .map(|connection: &Connection| connection.inflight)
                .sum::<usize>();
        }
        stats
    }

    /// Removes a waiting call from its line. `false` when it is not there:
    /// it was placed or failed.
    fn remove_waiter(&mut self, key: &str, id: u64) -> bool {
        let Some(line) = self.lines.get_mut(key) else {
            return false;
        };
        let Some(position) = line.iter().position(|waiter: &Waiter| waiter.id == id) else {
            return false;
        };
        line.remove(position);
        if line.is_empty() {
            self.lines.remove(key);
        }
        true
    }

    /// The connection of the URLs with the fewest calls and a free slot, as
    /// its URL and id. Only connected connections take calls.
    fn live_connection(&self, urls: &[String], max_inflight: usize) -> Option<(String, u64)> {
        let mut best: Option<(&str, u64, usize)> = None;
        for url in urls {
            let Some(endpoint) = self.endpoints.get(url) else {
                continue;
            };
            for connection in &endpoint.connections {
                if !connection.connected || connection.inflight >= max_inflight {
                    continue;
                }
                let better: bool = match best {
                    None => true,
                    Some((_, _, inflight)) => connection.inflight < inflight,
                };
                if better {
                    best = Some((url.as_str(), connection.id, connection.inflight));
                }
            }
        }
        best.map(|(url, id, _): (&str, u64, usize)| (url.to_owned(), id))
    }

    /// The connection of an endpoint and id, when it is still open.
    fn connection_mut(&mut self, url: &str, id: u64) -> Option<&mut Connection> {
        self.endpoints
            .get_mut(url)?
            .connections
            .iter_mut()
            .find(|connection: &&mut Connection| connection.id == id)
    }

    /// Takes a slot of a connection for a call.
    fn take_slot(&mut self, url: &str, id: u64) -> Option<Placement> {
        let connection: &mut Connection = self.connection_mut(url, id)?;
        connection.inflight += 1;
        connection.last_use = Instant::now();
        Some(Placement {
            client: connection.client.clone(),
            url: url.to_owned(),
            connection_id: id,
        })
    }

    /// Gives a slot back. A connection that has ended gives nothing back.
    fn give_back(&mut self, placement: &Placement) {
        if let Some(connection) = self.connection_mut(&placement.url, placement.connection_id) {
            connection.inflight = connection.inflight.saturating_sub(1);
            connection.last_use = Instant::now();
        }
    }

    /// Takes a connection out of its endpoint, and returns its client.
    fn remove_connection(&mut self, url: &str, id: u64) -> Option<Client> {
        let endpoint: &mut Endpoint = self.endpoints.get_mut(url)?;
        let position: usize = endpoint
            .connections
            .iter()
            .position(|connection: &Connection| connection.id == id)?;
        Some(endpoint.connections.remove(position).client)
    }
}

impl PoolInner {
    /// Refuses a call while the pool is closed or the main client is not
    /// connected. A call that finds the main client closed also ends the idle
    /// connections, when `close_ended_session` is set.
    fn check_open(&self) -> Result<(), Error> {
        let closed: bool = lock(&self.state).closed;
        if closed {
            return Err(Error::ClientClosed);
        }
        match self.main.state() {
            State::Connected => Ok(()),
            State::Closed => {
                if self.options.close_ended_session {
                    self.close_idle();
                }
                Err(Error::ClientClosed)
            }
            _ => Err(Error::ClientDisconnected),
        }
    }

    /// Closes the connections with no call in flight.
    fn close_idle(&self) {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        let mut idle: Vec<Client> = Vec::new();
        for endpoint in state.endpoints.values_mut() {
            let (busy, free): (Vec<Connection>, Vec<Connection>) = endpoint
                .connections
                .drain(..)
                .partition(|connection: &Connection| connection.inflight > 0);
            endpoint.connections = busy;
            idle.extend(
                free.into_iter()
                    .map(|connection: Connection| connection.client),
            );
        }
        drop(state);
        for client in idle {
            client.disconnect();
        }
    }

    /// The URLs of an endpoint key. An empty resolution is an error, and is
    /// not kept: the next call resolves again.
    fn resolve(&self, key: &str) -> Result<Vec<String>, Error> {
        let cached: Option<Vec<String>> = lock(&self.state).keys.get(key).cloned();
        if let Some(urls) = cached {
            return Ok(urls);
        }
        let urls: Vec<String> = (self.options.endpoints.as_ref())(key);
        if urls.is_empty() {
            return Err(Error::Configuration(
                "the endpoint key has no endpoint".into(),
            ));
        }
        Ok(urls)
    }

    /// The headers of a call: its own, and the affinity header when set.
    fn call_headers(&self, options: &RpcPoolCallOptions) -> HashMap<String, String> {
        let mut headers: HashMap<String, String> = options.headers.clone();
        if let (Some(name), Some(value)) = (&self.options.affinity_header, &options.affinity) {
            headers.insert(name.clone(), value.clone());
        }
        headers
    }

    /// Puts a call in the line of its key, and places it when a slot is free.
    /// Fails with code 16 when the line is full and no connection can open.
    fn enqueue(self: &Arc<Self>, key: &str, urls: Vec<String>) -> Result<Admission, Error> {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        if state.closed {
            return Err(Error::ClientClosed);
        }
        if state.queued() >= self.options.max_queued && self.opener(&state, &urls).is_none() {
            return Err(local_error(CODE_LIMIT_EXCEEDED, "limit exceeded"));
        }
        let id: u64 = state.take_id();
        let (sender, receiver): (oneshot::Sender<SlotResult>, oneshot::Receiver<SlotResult>) =
            oneshot::channel();
        state.keys.entry(key.to_owned()).or_insert(urls);
        state
            .lines
            .entry(key.to_owned())
            .or_default()
            .push_back(Waiter { id, sender });
        self.drain_key(&mut state, key);
        Ok(Admission {
            pool: Arc::clone(self),
            key: key.to_owned(),
            waiter_id: id,
            receiver,
            slot: None,
        })
    }

    /// Places the calls of a key's line while a connection has a free slot.
    /// When the head cannot be placed, and an endpoint may open a connection,
    /// one opens and the line waits for it.
    fn drain_key(self: &Arc<Self>, state: &mut PoolState, key: &str) {
        if state.closed {
            return;
        }
        loop {
            let Some(urls) = state.keys.get(key).cloned() else {
                return;
            };
            if state.lines.get(key).is_none_or(VecDeque::is_empty) {
                state.lines.remove(key);
                return;
            }
            let max_inflight: usize = self.options.max_inflight_per_connection;
            if let Some((url, connection_id)) = state.live_connection(&urls, max_inflight) {
                let Some(placement) = state.take_slot(&url, connection_id) else {
                    return;
                };
                let Some(waiter) = state.lines.get_mut(key).and_then(VecDeque::pop_front) else {
                    return;
                };
                if let Err(Ok(returned)) = waiter.sender.send(Ok(placement)) {
                    state.give_back(&returned);
                }
                continue;
            }
            let Some(url) = self.opener(state, &urls) else {
                return;
            };
            if let Err(error) = self.open_connection(state, &url) {
                if let Some(waiter) = state.lines.get_mut(key).and_then(VecDeque::pop_front) {
                    let _ = waiter.sender.send(Err(error));
                }
            }
            return;
        }
    }

    /// Places the calls of every line.
    fn drain_all(self: &Arc<Self>, state: &mut PoolState) {
        let keys: Vec<String> = state.lines.keys().cloned().collect();
        for key in keys {
            self.drain_key(state, &key);
        }
    }

    /// The first endpoint of the URLs that may open a connection now: not
    /// held, below `max_connections_per_endpoint`, and with no connection
    /// still connecting.
    fn opener(&self, state: &PoolState, urls: &[String]) -> Option<String> {
        let now: Instant = Instant::now();
        urls.iter()
            .find(|url: &&String| {
                let Some(endpoint) = state.endpoints.get(*url) else {
                    return true;
                };
                if endpoint
                    .hold_until
                    .is_some_and(|until: Instant| now < until)
                {
                    return false;
                }
                endpoint.connections.len() < self.options.max_connections_per_endpoint
                    && endpoint
                        .connections
                        .iter()
                        .all(|connection: &Connection| connection.connected)
            })
            .cloned()
    }

    /// Opens a connection to an endpoint. Its events come back through the
    /// pool, and it takes calls once it has connected.
    fn open_connection(self: &Arc<Self>, state: &mut PoolState, url: &str) -> Result<(), Error> {
        let id: u64 = state.take_id();
        let client: Client = self.build_client(url, id)?;
        client.connect()?;
        let endpoint: &mut Endpoint = state
            .endpoints
            .entry(url.to_owned())
            .or_insert_with(Endpoint::new);
        endpoint.hold_until = None;
        endpoint.connections.push(Connection {
            id,
            client,
            connected: false,
            inflight: 0,
            last_use: Instant::now(),
        });
        if !self.options.idle_timeout.is_zero() {
            self.spawn_idle(url.to_owned(), id);
        }
        Ok(())
    }

    /// A client of the endpoint, with the connection settings of the pool.
    /// Its events call back into the pool while it is alive.
    fn build_client(self: &Arc<Self>, url: &str, id: u64) -> Result<Client, Error> {
        let connected_pool: Weak<Self> = Arc::downgrade(self);
        let ended_pool: Weak<Self> = Arc::downgrade(self);
        let connected_url: String = url.to_owned();
        let ended_url: String = url.to_owned();
        Client::builder(url)
            .runtime(self.runtime.clone())
            .name(self.options.name.clone())
            .version(self.options.version.clone())
            .http_headers(self.headers.clone())
            .timeout(self.options.timeout)
            .min_reconnect_delay(self.options.min_reconnect_delay)
            .max_reconnect_delay(self.options.max_reconnect_delay)
            .max_server_ping_delay(self.options.max_server_ping_delay)
            .on_connected(move |_: &Client, _: ConnectedEvent| {
                if let Some(pool) = connected_pool.upgrade() {
                    pool.connection_connected(&connected_url, id);
                }
            })
            .on_disconnected(move |_: &Client, _: DisconnectedEvent| {
                if let Some(pool) = ended_pool.upgrade() {
                    pool.connection_ended(&ended_url, id);
                }
            })
            .build()
    }

    /// A connection connected: it takes calls, and its endpoint's failures
    /// start again from zero.
    fn connection_connected(self: &Arc<Self>, url: &str, id: u64) {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        if state.closed {
            return;
        }
        let Some(endpoint) = state.endpoints.get_mut(url) else {
            return;
        };
        endpoint.failures = 0;
        let Some(connection) = endpoint
            .connections
            .iter_mut()
            .find(|connection: &&mut Connection| connection.id == id)
        else {
            return;
        };
        connection.connected = true;
        self.drain_all(&mut state);
    }

    /// A connection ended for good: it leaves its endpoint, which is held for
    /// a backoff. Its calls fail on their own, as their clients end.
    fn connection_ended(self: &Arc<Self>, url: &str, id: u64) {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        if state.closed {
            return;
        }
        let Some(endpoint) = state.endpoints.get_mut(url) else {
            return;
        };
        let Some(position) = endpoint
            .connections
            .iter()
            .position(|connection: &Connection| connection.id == id)
        else {
            return;
        };
        let ended: Connection = endpoint.connections.remove(position);
        endpoint.failures = endpoint.failures.saturating_add(1);
        let until: Instant = Instant::now()
            + hold_delay(
                self.options.min_reconnect_delay,
                self.options.max_reconnect_delay,
                endpoint.failures,
            );
        endpoint.hold_until = Some(until);
        drop(ended);
        self.spawn_hold(url.to_owned(), until);
        self.drain_all(&mut state);
    }

    /// Wakes the lines of an endpoint when its hold ends.
    fn spawn_hold(self: &Arc<Self>, url: String, until: Instant) {
        let weak: Weak<Self> = Arc::downgrade(self);
        self.runtime.spawn(async move {
            tokio::time::sleep_until(until).await;
            if let Some(pool) = weak.upgrade() {
                pool.hold_elapsed(&url, until);
            }
        });
    }

    /// A hold ended: the endpoint may open connections again.
    fn hold_elapsed(self: &Arc<Self>, url: &str, until: Instant) {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        if state.closed {
            return;
        }
        if let Some(endpoint) = state.endpoints.get_mut(url) {
            if endpoint.hold_until == Some(until) {
                endpoint.hold_until = None;
            }
        }
        self.drain_all(&mut state);
    }

    /// Starts the idle check of a connection. It checks again at the end of
    /// each idle span, and stops once the connection is closed.
    fn spawn_idle(self: &Arc<Self>, url: String, id: u64) {
        let weak: Weak<Self> = Arc::downgrade(self);
        self.runtime.spawn(async move {
            loop {
                let Some(pool) = weak.upgrade() else {
                    return;
                };
                let next: Option<Duration> = pool.idle_check(&url, id);
                drop(pool);
                let Some(wait) = next else {
                    return;
                };
                tokio::time::sleep(wait).await;
            }
        });
    }

    /// One idle check. Returns how long to wait before the next one, or `None`
    /// when the connection is closed (or gone).
    fn idle_check(&self, url: &str, id: u64) -> Option<Duration> {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        if state.closed {
            return None;
        }
        let (inflight, last_use): (usize, Instant) = {
            let connection: &mut Connection = state.connection_mut(url, id)?;
            (connection.inflight, connection.last_use)
        };
        let idle: Duration = self.options.idle_timeout;
        if inflight > 0 {
            return Some(idle);
        }
        let now: Instant = Instant::now();
        let due: Instant = last_use + idle;
        if now < due {
            return Some(due - now);
        }
        if let Some(client) = state.remove_connection(url, id) {
            client.disconnect();
        }
        None
    }

    /// A call finished with its slot: the slot is given back, and the line of
    /// its key is placed again.
    fn release(self: &Arc<Self>, state: &mut PoolState, key: &str, placement: &Placement) {
        state.give_back(placement);
        self.drain_key(state, key);
    }

    /// Closes the pool: waiting calls fail, and the connections disconnect.
    fn close(&self) {
        let mut state: MutexGuard<'_, PoolState> = lock(&self.state);
        if state.closed {
            return;
        }
        state.closed = true;
        let lines: Vec<VecDeque<Waiter>> = state
            .lines
            .drain()
            .map(|pair: (String, VecDeque<Waiter>)| pair.1)
            .collect();
        let mut clients: Vec<Client> = Vec::new();
        for endpoint in state.endpoints.values_mut() {
            clients.extend(
                endpoint
                    .connections
                    .drain(..)
                    .map(|connection: Connection| connection.client),
            );
        }
        drop(state);
        for line in lines {
            for waiter in line {
                let _ = waiter
                    .sender
                    .send(Err(local_error(CODE_CANCELLED, "cancelled")));
            }
        }
        for client in clients {
            client.disconnect();
        }
    }
}

/// The hold of an endpoint after `failures` connections ended for good: the
/// minimum reconnect delay, doubled per failure, at most the maximum.
fn hold_delay(minimum: Duration, maximum: Duration, failures: u32) -> Duration {
    let exponent: u32 = failures.saturating_sub(1).min(16);
    minimum
        .checked_mul(1_u32 << exponent)
        .unwrap_or(maximum)
        .min(maximum)
}
