//! The line of the rpc pool: a call waits in the line of its endpoint key
//! until a connection has a free slot, then holds that slot until the call
//! finishes.
//!
//! [`Admission`] is the handle of one call in the line. Dropping it, after a
//! timeout, a cancel or a finished call, removes the call from the line or
//! gives its slot back, whichever applies.

use std::sync::Arc;

use tokio::sync::oneshot;

use super::PoolInner;
use crate::client::Client;
use crate::error::Error;
use crate::shared::lock;

/// A slot of a connection, given to a call.
#[derive(Debug, Clone)]
pub(crate) struct Placement {
    /// The client of the connection.
    pub(crate) client: Client,
    /// The endpoint URL of the connection.
    pub(crate) url: String,
    /// The connection within its endpoint.
    pub(crate) connection_id: u64,
}

/// What a waiting call receives: its slot, or the error that fails it.
pub(crate) type SlotResult = Result<Placement, Error>;

/// A call waiting in a line.
#[derive(Debug)]
pub(crate) struct Waiter {
    /// Identifies the call within the pool.
    pub(crate) id: u64,
    /// Delivers the slot, or the error that fails the call.
    pub(crate) sender: oneshot::Sender<SlotResult>,
}

/// The handle of one pool call from its admission to its end.
#[derive(Debug)]
pub(crate) struct Admission {
    /// The pool the call belongs to.
    pub(crate) pool: Arc<PoolInner>,
    /// The endpoint key of the call.
    pub(crate) key: String,
    /// Identifies the call in the line.
    pub(crate) waiter_id: u64,
    /// Receives the slot once the call is placed.
    pub(crate) receiver: oneshot::Receiver<SlotResult>,
    /// The slot the call holds, once received.
    pub(crate) slot: Option<Placement>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut state = lock(&self.pool.state);
        if let Some(placement) = self.slot.take() {
            self.pool.release(&mut state, &self.key, &placement);
            return;
        }
        if state.remove_waiter(&self.key, self.waiter_id) {
            return;
        }
        // Not in the line: the slot was delivered (or the call failed) and
        // nobody took it. Give a delivered slot back.
        if let Ok(Ok(placement)) = self.receiver.try_recv() {
            self.pool.release(&mut state, &self.key, &placement);
        }
    }
}
