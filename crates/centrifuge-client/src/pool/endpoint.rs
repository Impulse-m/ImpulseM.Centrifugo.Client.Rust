//! The endpoints of the rpc pool: one URL each, with the connections that
//! open it and the hold that follows a connection ending for good.
//!
//! Keys that resolve to the same URL share its endpoint. The pool's logic
//! lives in the parent module; this file holds the state only.

use tokio::sync::watch;
use tokio::time::Instant;

use crate::client::Client;

/// One connection of an endpoint.
#[derive(Debug)]
pub(crate) struct Connection {
    /// Identifies the connection within the pool.
    pub(crate) id: u64,
    /// The client that carries the calls.
    pub(crate) client: Client,
    /// Whether the connection has connected since it was opened. Only a
    /// connected connection takes calls.
    pub(crate) connected: bool,
    /// Calls holding a slot of the connection.
    pub(crate) inflight: usize,
    /// The last moment a call took or gave back a slot. The idle span counts
    /// from it.
    pub(crate) last_use: Instant,
    /// The disconnect code the connection ended with for good, once it has.
    pub(crate) ends: watch::Receiver<Option<u32>>,
}

/// One endpoint URL of the pool, with its connections and its hold.
#[derive(Debug)]
pub(crate) struct Endpoint {
    /// The connections of the endpoint, open or connecting.
    pub(crate) connections: Vec<Connection>,
    /// The moment the hold ends. Until then no connection opens. `None` when
    /// the endpoint is not held.
    pub(crate) hold_until: Option<Instant>,
    /// Connections that ended for good without a connect in between. Sets the
    /// length of the hold.
    pub(crate) failures: u32,
}

impl Endpoint {
    /// An endpoint with no connection and no hold.
    pub(crate) fn new() -> Self {
        Self {
            connections: Vec::new(),
            hold_until: None,
            failures: 0,
        }
    }
}
