//! State shared by handles, the driver and the dispatcher (docs/spec.md §9.2).
//!
//! None of it is held across an `.await` or a handler call.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use centrifuge_client_core::codec::Protocol;
use centrifuge_client_core::machine::{Op, State, SubKey, SubState, TransportId, TransportKind};
use tokio::sync::{mpsc, watch};

use crate::calls::CallTable;
use crate::dispatch::{ClientHandlers, StreamSlot, SubHandlers};
use crate::events::{ClientEvent, PublicationEvent, SubscriptionEvent};
use crate::rpc_tracker::RpcTracker;
use crate::server_time::ServerTime;
use crate::transport::WriterTx;

/// Locks a mutex, recovering from poisoning (a panicking handler must never
/// make the client unusable for the rest of the process).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The connected session, for the call fast path.
#[derive(Debug, Clone)]
pub(crate) struct SessionHandle {
    pub(crate) transport: TransportId,
    pub(crate) kind: TransportKind,
    pub(crate) writer: WriterTx,
    pub(crate) session: String,
    pub(crate) node: String,
    /// The server confirmed the RPC options on this connection.
    pub(crate) rpc_options: bool,
}

/// What the driver publishes after each step.
#[derive(Debug, Clone)]
pub(crate) struct Snapshot {
    pub(crate) state: State,
    pub(crate) disconnects: u64,
    pub(crate) applied_seq: u64,
    /// `connected` and its handler has returned: `ready()` may resolve.
    pub(crate) ready: bool,
    pub(crate) session: Option<SessionHandle>,
    pub(crate) core_stats: centrifuge_client_core::machine::CoreStats,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            state: State::Disconnected,
            disconnects: 0,
            applied_seq: 0,
            ready: false,
            session: None,
            core_stats: centrifuge_client_core::machine::CoreStats::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubSnapshot {
    pub(crate) state: SubState,
    pub(crate) unsubscribes: u64,
    /// `subscribed` and its handler has returned.
    pub(crate) ready: bool,
}

impl Default for SubSnapshot {
    fn default() -> Self {
        Self {
            state: SubState::Unsubscribed,
            unsubscribes: 0,
            ready: false,
        }
    }
}

/// An op with the sequence number `state()` and waiters compare against.
#[derive(Debug)]
pub(crate) struct SeqOp {
    pub(crate) seq: u64,
    pub(crate) op: Op,
}

/// A message to the driver.
#[derive(Debug)]
pub(crate) enum Command {
    Op(SeqOp),
    /// The last handle was dropped.
    Dropped,
}

/// One subscription's shared state.
pub(crate) struct SubInner {
    pub(crate) key: SubKey,
    pub(crate) channel: String,
    pub(crate) snapshot: watch::Sender<SubSnapshot>,
    /// The state the not-yet-applied ops lead to: `(seq, state)`.
    pub(crate) projected: Mutex<Option<(u64, SubState)>>,
    pub(crate) handlers: Mutex<SubHandlers>,
    pub(crate) events_stream: Mutex<StreamSlot<SubscriptionEvent>>,
    pub(crate) publications_stream: Mutex<StreamSlot<PublicationEvent>>,
    pub(crate) removed: std::sync::atomic::AtomicBool,
    pub(crate) callbacks: crate::driver::SubCallbacks,
    /// Delta compression is on (the tags filter can't change then).
    pub(crate) delta: bool,
}

impl std::fmt::Debug for SubInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubInner")
            .field("key", &self.key)
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}

/// Configuration the handles and the call fast path need.
#[derive(Debug, Clone)]
pub(crate) struct CallConfig {
    pub(crate) protocol: Protocol,
    pub(crate) timeout: std::time::Duration,
    /// The client has an instance: its rpc calls count against `max_pending_rpc`
    /// and can be tracked.
    pub(crate) has_instance: bool,
    pub(crate) max_pending_rpc: u32,
}

pub(crate) struct Shared {
    pub(crate) id: u64,
    pub(crate) commands: mpsc::UnboundedSender<Command>,
    pub(crate) requested_seq: AtomicU64,
    pub(crate) snapshot: watch::Sender<Snapshot>,
    pub(crate) projected: Mutex<Option<(u64, State)>>,
    pub(crate) registry: Mutex<HashMap<String, Arc<SubInner>>>,
    pub(crate) by_key: Mutex<HashMap<SubKey, Arc<SubInner>>>,
    pub(crate) next_key: AtomicU64,
    pub(crate) calls: Mutex<CallTable>,
    pub(crate) rpc: RpcTracker,
    /// The offset of the server time, from the replies carrying a time.
    pub(crate) server_time: ServerTime,
    pub(crate) next_call_id: AtomicU32,
    pub(crate) handlers: Mutex<ClientHandlers>,
    pub(crate) events_stream: Mutex<StreamSlot<ClientEvent>>,
    pub(crate) cfg: CallConfig,
    pub(crate) bytes_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) connected_since: Mutex<Option<std::time::SystemTime>>,
    /// Real HTTP headers for the next connect attempt.
    pub(crate) http_headers: Mutex<http::HeaderMap>,
    /// DNS, proxy and TLS, shared by every attempt.
    pub(crate) net: Arc<crate::transport::net::Net>,
    /// `close()` futures waiting for the driver to stop; `None` once it has.
    pub(crate) close_waiters: Mutex<Option<Vec<tokio::sync::oneshot::Sender<()>>>>,
    /// Serializes op sending; `true` once `close()` was sent.
    op_lock: Mutex<bool>,
    /// Set by `close()` and when the last `Client` handle is dropped: the
    /// driver stops waiting for stream consumers (§9.4).
    pub(crate) stop: watch::Sender<bool>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

static CLIENT_IDS: AtomicU64 = AtomicU64::new(1);

/// Fast-path call ids live in the upper half of `u32`; the core uses the lower.
pub(crate) const CALL_ID_BASE: u32 = 1 << 31;

impl Shared {
    pub(crate) fn new(
        commands: mpsc::UnboundedSender<Command>,
        cfg: CallConfig,
        http_headers: http::HeaderMap,
        net: Arc<crate::transport::net::Net>,
    ) -> Self {
        let rpc: RpcTracker = RpcTracker::new(cfg.has_instance, cfg.max_pending_rpc);
        Self {
            id: CLIENT_IDS.fetch_add(1, Ordering::Relaxed),
            commands,
            requested_seq: AtomicU64::new(0),
            snapshot: watch::Sender::new(Snapshot::default()),
            projected: Mutex::new(None),
            registry: Mutex::new(HashMap::new()),
            by_key: Mutex::new(HashMap::new()),
            next_key: AtomicU64::new(1),
            calls: Mutex::new(CallTable::default()),
            rpc,
            server_time: ServerTime::default(),
            next_call_id: AtomicU32::new(CALL_ID_BASE),
            handlers: Mutex::new(ClientHandlers::default()),
            events_stream: Mutex::new(StreamSlot::default()),
            cfg,
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            connected_since: Mutex::new(None),
            http_headers: Mutex::new(http_headers),
            net,
            close_waiters: Mutex::new(Some(Vec::new())),
            op_lock: Mutex::new(false),
            stop: watch::Sender::new(false),
        }
    }

    /// Sends an op to the driver, recording its effect for `state()`.
    pub(crate) fn send_op(&self, op: Op) -> u64 {
        // Sequence numbers go out in order, and nothing after close: the
        // driver never applies it, so waiters would wait for it in vain.
        let mut closed = lock(&self.op_lock);
        if *closed {
            return self.requested_seq.load(Ordering::SeqCst);
        }
        *closed = matches!(op, Op::Close);
        let seq = self.requested_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.commands.send(Command::Op(SeqOp { seq, op }));
        seq
    }

    pub(crate) fn next_call_id(&self) -> u32 {
        loop {
            let id = self.next_call_id.fetch_add(1, Ordering::Relaxed);
            if id >= CALL_ID_BASE {
                return id;
            }
            // Wrapped: back to the base of the upper half.
            let _ = self.next_call_id.compare_exchange(
                id.wrapping_add(1),
                CALL_ID_BASE,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    /// The client state as the caller's ops leave it (docs/spec.md §9.2).
    pub(crate) fn state(&self) -> State {
        // Lock order: `projected` before the snapshot, never both held.
        let projected = *lock(&self.projected);
        self.state_with(projected)
    }

    fn state_with(&self, projected: Option<(u64, State)>) -> State {
        let snap = self.snapshot.borrow();
        match projected {
            Some((seq, st)) if seq > snap.applied_seq => st,
            _ => snap.state,
        }
    }

    /// Sends a client op, projecting the state it leads to with `next` (from
    /// the state as of the caller's previous ops). Atomic with other ops, so
    /// projections are recorded in sequence order.
    pub(crate) fn send_client_op(&self, op: Op, next: impl FnOnce(State) -> State) -> State {
        let mut projected = lock(&self.projected);
        let current = self.state_with(*projected);
        let st = next(current);
        let seq = self.send_op(op);
        *projected = Some((seq, st));
        st
    }

    /// Sends a subscription op, projecting its state the same way.
    pub(crate) fn send_sub_op(
        &self,
        sub: &SubInner,
        op: Op,
        next: impl FnOnce(SubState) -> SubState,
    ) {
        let mut projected = lock(&sub.projected);
        let current = sub.state_with(self, *projected);
        let st = next(current);
        let seq = self.send_op(op);
        *projected = Some((seq, st));
    }
}

impl SubInner {
    pub(crate) fn new(
        key: SubKey,
        channel: String,
        callbacks: crate::driver::SubCallbacks,
        delta: bool,
    ) -> Self {
        Self {
            key,
            channel,
            snapshot: watch::Sender::new(SubSnapshot::default()),
            projected: Mutex::new(None),
            handlers: Mutex::new(SubHandlers::default()),
            events_stream: Mutex::new(StreamSlot::default()),
            publications_stream: Mutex::new(StreamSlot::default()),
            removed: std::sync::atomic::AtomicBool::new(false),
            callbacks,
            delta,
        }
    }

    pub(crate) fn state(&self, shared: &Shared) -> SubState {
        let projected = *lock(&self.projected);
        self.state_with(shared, projected)
    }

    fn state_with(&self, shared: &Shared, projected: Option<(u64, SubState)>) -> SubState {
        let applied = shared.snapshot.borrow().applied_seq;
        match projected {
            Some((seq, st)) if seq > applied => st,
            _ => self.snapshot.borrow().state,
        }
    }
}
