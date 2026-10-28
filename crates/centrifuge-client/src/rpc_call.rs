//! Rpc calls with options: the headers and the deadline of a call, and its
//! handle, which waits for the reply and cancels the call.
//!
//! [`Client::rpc_call`](crate::Client::rpc_call) admits a call and returns its
//! [`RpcCall`] at once. Awaiting [`RpcCall::wait`] sends the call when the
//! connection is up and returns its result. [`RpcCall::cancel`] stops it from
//! any clone of the handle: a call not sent yet fails at once, and a sent
//! tracked call also gets a cancel command for the server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use centrifuge_client_core::proto;
use centrifuge_client_core::types::RpcResult;
use tokio::sync::{oneshot, watch};

use crate::calls::{CallGuard, wait_ready, wait_reply};
use crate::error::Error;
use crate::rpc_tracker::{
    CODE_CANCELLED, CODE_UNSUPPORTED, CancelOnDrop, PendingSlot, local_error, request,
};
use crate::shared::{SessionHandle, Shared, lock};
use crate::transport::WriteReq;

/// Options of one rpc call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RpcOptions {
    /// Headers sent with the call. A connection that did not confirm the RPC
    /// options cannot carry them: the call then fails with code 15 and is not
    /// sent.
    pub headers: HashMap<String, String>,
    /// Deadline of the call across its attempts. `None` means the client's
    /// `timeout`. An explicit value must be above zero and no more than that
    /// timeout.
    pub timeout: Option<Duration>,
}

/// The deadline of a call: its explicit timeout, or the client's.
///
/// # Errors
///
/// [`Error::Configuration`] when the explicit timeout is zero or above `limit`.
pub(crate) fn validate_timeout(
    explicit: Option<Duration>,
    limit: Duration,
) -> Result<Duration, Error> {
    match explicit {
        None => Ok(limit),
        Some(timeout) if timeout.is_zero() || timeout > limit => Err(Error::Configuration(
            "rpc timeout must be above 0 and no more than the client timeout".into(),
        )),
        Some(timeout) => Ok(timeout),
    }
}

/// Handle of one rpc call, made with
/// [`Client::rpc_call`](crate::Client::rpc_call).
///
/// The call is sent by [`wait`](Self::wait); clones of the handle share its
/// cancellation.
#[derive(Clone, Debug)]
pub struct RpcCall {
    inner: Arc<RpcCallInner>,
}

#[derive(Debug)]
struct RpcCallInner {
    shared: Arc<Shared>,
    method: String,
    data: Bytes,
    headers: HashMap<String, String>,
    timeout: Duration,
    /// Set when the call was refused at once: invalid options, or
    /// `max_pending_rpc` reached.
    refused: Option<Error>,
    /// The pending slot of the call; `wait` takes it and frees it on return.
    pending: Mutex<Option<PendingSlot>>,
    /// `true` once the call is cancelled.
    cancelled: watch::Sender<bool>,
    /// Set by the first `wait`: a call is sent once.
    started: AtomicBool,
}

impl RpcCall {
    /// A call admitted with `admission`: its deadline and pending slot, or the
    /// error that refused it.
    pub(crate) fn new(
        shared: Arc<Shared>,
        method: &str,
        data: Bytes,
        headers: HashMap<String, String>,
        admission: Result<(Duration, Option<PendingSlot>), Error>,
    ) -> Self {
        let (timeout, refused, pending): (Duration, Option<Error>, Option<PendingSlot>) =
            match admission {
                Ok((timeout, pending)) => (timeout, None, pending),
                Err(error) => (shared.cfg.timeout, Some(error), None),
            };
        let (cancelled, _): (watch::Sender<bool>, watch::Receiver<bool>) = watch::channel(false);
        Self {
            inner: Arc::new(RpcCallInner {
                shared,
                method: method.to_owned(),
                data,
                headers,
                timeout,
                refused,
                pending: Mutex::new(pending),
                cancelled,
                started: AtomicBool::new(false),
            }),
        }
    }

    /// Sends the call once the connection is up and waits for its result.
    ///
    /// # Errors
    ///
    /// [`Error::Server`] with code 15, 16 or 17 for a call that was not sent or
    /// was stopped, or with the server's error; [`Error::Timeout`] past the
    /// deadline; [`Error::ConnectionClosed`] when the connection was lost while
    /// the call was on it. Awaiting a second time, on a clone, fails with
    /// [`Error::Configuration`].
    pub async fn wait(self) -> Result<RpcResult, Error> {
        if let Some(error) = self.inner.refused.clone() {
            return Err(error);
        }
        if self.inner.started.swap(true, Ordering::SeqCst) {
            return Err(Error::Configuration("rpc call already started".into()));
        }
        let pending_slot: Option<PendingSlot> = lock(&self.inner.pending).take();
        let deadline: tokio::time::Instant = tokio::time::Instant::now() + self.inner.timeout;
        let mut cancelled: watch::Receiver<bool> = self.inner.cancelled.subscribe();
        let outcome: Result<RpcResult, Error> = tokio::select! {
            biased;
            _ = cancelled.wait_for(|stop: &bool| *stop) => Err(local_error(CODE_CANCELLED, "cancelled")),
            result = self.inner.send_and_wait(deadline) => result,
        };
        drop(pending_slot);
        outcome
    }

    /// Stops the call. A call not sent yet fails at once with code 17; a sent
    /// tracked call also gets a cancel command for the server. Calling it after
    /// the call completed does nothing.
    pub fn cancel(&self) {
        self.inner.cancelled.send_replace(true);
    }
}

impl RpcCallInner {
    /// Writes the call once the connection is up, then waits for its reply.
    async fn send_and_wait(&self, deadline: tokio::time::Instant) -> Result<RpcResult, Error> {
        wait_ready(&self.shared, None, deadline).await?;
        let session: SessionHandle = self
            .shared
            .snapshot
            .borrow()
            .session
            .clone()
            .ok_or(Error::ClientDisconnected)?;
        let tracked: bool = session.rpc_options && self.shared.rpc.has_instance();
        if !tracked && !self.headers.is_empty() {
            return Err(local_error(
                CODE_UNSUPPORTED,
                "the connection cannot carry the headers of the call",
            ));
        }
        let msg_id: Option<u64> = if tracked {
            Some(self.shared.rpc.next_msg_id()?)
        } else {
            None
        };
        let id: u32 = self.shared.next_call_id();
        let command: proto::Command = proto::Command {
            id,
            rpc: Some(request(
                &self.method,
                self.data.clone(),
                self.headers.clone(),
                msg_id,
            )),
            ..Default::default()
        };
        let (tx, rx) = oneshot::channel();
        if lock(&self.shared.calls)
            .insert(id, session.transport, Error::ConnectionClosed, tx)
            .is_err()
        {
            return Err(Error::ClientDisconnected);
        }
        let guard: CallGuard<'_> = CallGuard {
            shared: &self.shared,
            id,
        };
        if session
            .writer
            .send(WriteReq::call(command, id, &session))
            .is_err()
        {
            return Err(Error::ClientDisconnected);
        }
        let mut cancel_on_drop: Option<CancelOnDrop> = msg_id
            .map(|msg: u64| CancelOnDrop::new(Arc::clone(&self.shared), session.clone(), msg));
        let outcome: Result<proto::Reply, Error> = wait_reply(rx, deadline).await;
        drop(guard);
        // A timeout or a lost connection returns here with the cancel still armed.
        let reply: proto::Reply = outcome?;
        if let Some(cancel) = cancel_on_drop.as_mut() {
            cancel.disarm();
        }
        if let Some(error) = reply.error {
            return Err(Error::Server(error.into()));
        }
        Ok(reply.rpc.map(Into::into).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::validate_timeout;
    use crate::error::Error;

    const LIMIT: Duration = Duration::from_secs(5);

    #[test]
    fn rpc_timeout_defaults_to_the_client_timeout() {
        let timeout: Duration = validate_timeout(None, LIMIT).expect("valid");
        assert_eq!(timeout, LIMIT);
    }

    #[test]
    fn rpc_timeout_accepts_values_above_zero_up_to_the_client_timeout() {
        assert_eq!(
            validate_timeout(Some(Duration::from_millis(1)), LIMIT).expect("valid"),
            Duration::from_millis(1)
        );
        assert_eq!(validate_timeout(Some(LIMIT), LIMIT).expect("valid"), LIMIT);
    }

    #[test]
    fn rpc_timeout_rejects_zero_and_values_above_the_client_timeout() {
        let zero: Result<Duration, Error> = validate_timeout(Some(Duration::ZERO), LIMIT);
        let above: Result<Duration, Error> =
            validate_timeout(Some(LIMIT + Duration::from_millis(1)), LIMIT);
        assert!(matches!(zero, Err(Error::Configuration(_))));
        assert!(matches!(above, Err(Error::Configuration(_))));
    }
}
