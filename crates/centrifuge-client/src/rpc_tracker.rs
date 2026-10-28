//! Tracking of rpc calls: the admission bound, the `msg_ids`, the request shapes
//! and the cancel command of a sent call.
//!
//! A call of a client with an instance is tracked when the connection confirmed
//! the RPC options: it carries a `msg_id`, so the server runs it at most once,
//! and it counts against `max_pending_rpc`. Any other call is plain: no `msg_id`.
//! Stopping a sent tracked call (cancel, timeout, or its waiter going away)
//! sends a cancel command for its `msg_id`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use centrifuge_client_core::proto;

use crate::error::Error;
use crate::msg_id::MsgIdGenerator;
use crate::shared::{SessionHandle, Shared, lock};
use crate::transport::WriteReq;

/// Code of a call refused because `max_pending_rpc` calls are pending.
pub(crate) const CODE_LIMIT_EXCEEDED: u32 = 16;

/// Code of a call the connection cannot carry (its headers): not sent.
pub(crate) const CODE_UNSUPPORTED: u32 = 15;

/// Code of a call stopped with `cancel()`.
pub(crate) const CODE_CANCELLED: u32 = 17;

/// A client-local error: a code the server never sends.
pub(crate) fn local_error(code: u32, message: &str) -> Error {
    Error::Server(
        proto::Error {
            code,
            message: message.to_owned(),
            temporary: false,
            ..Default::default()
        }
        .into(),
    )
}

/// The `rpc` request of a call: its method and data, its headers, and the
/// `msg_id` of a tracked call (`None` for a plain one, which sends 0).
pub(crate) fn request(
    method: &str,
    data: Bytes,
    headers: HashMap<String, String>,
    msg_id: Option<u64>,
) -> proto::RpcRequest {
    proto::RpcRequest {
        method: method.to_owned(),
        data,
        headers,
        msg_id: msg_id.unwrap_or(0),
        ..Default::default()
    }
}

/// The `rpc` request that cancels the `msg_id` of a sent call: no method, no data.
pub(crate) fn cancel_request(msg_id: u64) -> proto::RpcRequest {
    proto::RpcRequest {
        cancel: msg_id,
        ..Default::default()
    }
}

/// The rpc bookkeeping of one client: the admission bound and the `msg_ids`.
#[derive(Debug)]
pub(crate) struct RpcTracker {
    has_instance: bool,
    max_pending: u32,
    pending: Arc<Mutex<u32>>,
    generator: Mutex<Option<MsgIdGenerator>>,
}

impl RpcTracker {
    /// A tracker for a client; calls count against `max_pending` only when the
    /// client has an instance.
    pub(crate) fn new(has_instance: bool, max_pending: u32) -> Self {
        Self {
            has_instance,
            max_pending,
            pending: Arc::new(Mutex::new(0)),
            generator: Mutex::new(None),
        }
    }

    /// Whether the client has an instance, so its calls can be tracked.
    pub(crate) fn has_instance(&self) -> bool {
        self.has_instance
    }

    /// Admits a call. With an instance, the call takes a pending slot, held
    /// until the returned value is dropped.
    ///
    /// # Errors
    ///
    /// Code 16 when `max_pending_rpc` calls are already pending.
    pub(crate) fn admit(&self) -> Result<Option<PendingSlot>, Error> {
        if !self.has_instance {
            return Ok(None);
        }
        let mut pending = lock(&self.pending);
        if *pending >= self.max_pending {
            return Err(local_error(CODE_LIMIT_EXCEEDED, "limit exceeded"));
        }
        *pending += 1;
        Ok(Some(PendingSlot {
            pending: Arc::clone(&self.pending),
        }))
    }

    /// The `msg_id` of the next tracked write.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] when the `msg_id` space is exhausted.
    pub(crate) fn next_msg_id(&self) -> Result<u64, Error> {
        let mut guard = lock(&self.generator);
        let generator: &mut MsgIdGenerator = guard.get_or_insert_with(|| {
            MsgIdGenerator::new(clock_millis(), MsgIdGenerator::random_lane())
        });
        generator.next(clock_millis())
    }
}

/// One call counted against `max_pending_rpc`. Dropping it frees the slot.
#[derive(Debug)]
pub(crate) struct PendingSlot {
    pending: Arc<Mutex<u32>>,
}

impl Drop for PendingSlot {
    fn drop(&mut self) {
        let mut pending = lock(&self.pending);
        *pending = pending.saturating_sub(1);
    }
}

/// Sends the cancel command of a sent tracked call when dropped, unless
/// disarmed. A call that times out, is cancelled, or whose waiter goes away
/// before its reply is stopped this way.
#[derive(Debug)]
pub(crate) struct CancelOnDrop {
    shared: Arc<Shared>,
    session: SessionHandle,
    msg_id: u64,
    armed: bool,
}

impl CancelOnDrop {
    /// Armed for the call `msg_id` written on `session`.
    pub(crate) fn new(shared: Arc<Shared>, session: SessionHandle, msg_id: u64) -> Self {
        Self {
            shared,
            session,
            msg_id,
            armed: true,
        }
    }

    /// The server answered the call: nothing is left to cancel.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            send_cancel(&self.shared, &self.session, self.msg_id);
        }
    }
}

/// Writes the cancel command of a `msg_id`. Its reply is not awaited: a reply
/// nobody waits for is dropped by the transport.
fn send_cancel(shared: &Shared, session: &SessionHandle, msg_id: u64) {
    let id: u32 = shared.next_call_id();
    let command: proto::Command = proto::Command {
        id,
        rpc: Some(cancel_request(msg_id)),
        ..Default::default()
    };
    let _ = session.writer.send(WriteReq::call(command, id, session));
}

/// The local clock in milliseconds since the unix epoch.
fn clock_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d: Duration| {
            i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::{CODE_LIMIT_EXCEEDED, RpcTracker, cancel_request, local_error, request};
    use crate::error::Error;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[test]
    fn rpc_admit_without_instance_never_counts_a_call() {
        let tracker: RpcTracker = RpcTracker::new(false, 1);
        let first = tracker.admit().expect("admitted");
        let second = tracker.admit().expect("admitted");
        assert!(first.is_none());
        assert!(second.is_none());
    }

    #[test]
    fn rpc_admit_refuses_beyond_max_pending_with_code_16_and_frees_on_drop() {
        let tracker: RpcTracker = RpcTracker::new(true, 2);
        let first = tracker.admit().expect("admitted");
        let second = tracker.admit().expect("admitted");
        let refused: Result<_, Error> = tracker.admit();
        match refused {
            Err(Error::Server(error)) => assert_eq!(error.code, CODE_LIMIT_EXCEEDED),
            other => panic!("expected code 16, got {other:?}"),
        }
        drop(first);
        assert!(tracker.admit().is_ok());
        drop(second);
    }

    #[test]
    fn rpc_next_msg_id_is_increasing_and_below_two_pow_53() {
        let tracker: RpcTracker = RpcTracker::new(true, 32);
        let first: u64 = tracker.next_msg_id().expect("an id");
        let second: u64 = tracker.next_msg_id().expect("an id");
        assert!(second > first);
        assert!(second < 1 << 53);
    }

    #[test]
    fn rpc_plain_request_sends_no_msg_id() {
        let plain = request("echo", Bytes::from_static(b"x"), HashMap::new(), None);
        let tracked = request("echo", Bytes::from_static(b"x"), HashMap::new(), Some(7));
        assert_eq!(plain.msg_id, 0);
        assert_eq!(tracked.msg_id, 7);
        assert_eq!(plain.method, "echo");
    }

    #[test]
    fn rpc_cancel_request_carries_only_the_msg_id() {
        let cancel = cancel_request(9);
        assert_eq!(cancel.cancel, 9);
        assert!(cancel.method.is_empty());
        assert!(cancel.data.is_empty());
    }

    #[test]
    fn rpc_local_error_is_a_server_error_with_the_local_code() {
        match local_error(17, "cancelled") {
            Error::Server(error) => {
                assert_eq!(error.code, 17);
                assert!(!error.temporary);
            }
            other => panic!("expected a server error, got {other:?}"),
        }
    }
}
