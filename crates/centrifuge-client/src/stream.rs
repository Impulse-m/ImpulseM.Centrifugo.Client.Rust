//! Hand-off event streams (docs/spec.md §9.4).

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_core::stream::FusedStream;
use tokio::sync::{mpsc, oneshot};

/// A stream of events with hand-off delivery: an item counts as delivered when
/// `next()` returns it, and the SDK's next step waits for that.
///
/// At most one stream of a kind is active per object: asking for a new one
/// ends this one. It also ends on `close()`, and a subscription's streams end
/// after `remove_subscription()`. `next()` is cancel-safe: an item not yet
/// returned stays offered.
#[must_use = "streams do nothing unless polled"]
pub struct EventStream<T> {
    rx: mpsc::Receiver<(T, oneshot::Sender<()>)>,
    tail: crate::dispatch::Tail<T>,
    done: bool,
}

impl<T> EventStream<T> {
    pub(crate) fn new(
        rx: mpsc::Receiver<(T, oneshot::Sender<()>)>,
        tail: crate::dispatch::Tail<T>,
    ) -> Self {
        Self {
            rx,
            tail,
            done: false,
        }
    }
}

impl<T> std::fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<T> Stream for EventStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some((item, ack))) => {
                let _ = ack.send(());
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                // The stream ended: what was offered while it was full last.
                let next = this
                    .tail
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front();
                if next.is_none() {
                    this.done = true;
                }
                Poll::Ready(next)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> FusedStream for EventStream<T> {
    fn is_terminated(&self) -> bool {
        self.done
    }
}

impl<T> Unpin for EventStream<T> {}
