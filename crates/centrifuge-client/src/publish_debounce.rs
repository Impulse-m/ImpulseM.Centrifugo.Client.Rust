//! Publish debounce of a subscription: publishes inside the window the server
//! sets are coalesced.
//!
//! The server gives the window in the subscribe reply (`publish_debounce`, in
//! milliseconds; zero turns debouncing off). The first publish of a window is
//! sent at once and opens the window. A publish inside an open window replaces
//! the pending value and completes at once with success, so nothing is sent for
//! it. When the window ends, the pending value is sent and a new window starts;
//! without a pending value the window goes idle. A window longer than 2^31 - 1
//! ms is capped to that length.
//!
//! The window stays open until its timer runs, even after its end time has
//! passed, so a publish that arrives before the timer is coalesced too.
//!
//! The module holds no timer and no clock. The caller passes the current time,
//! arms a timer at [`PublishDebounce::next_deadline`] and calls
//! [`PublishDebounce::on_timer`] when it fires. [`PublishDebounce::reset`] drops
//! the window and any pending value; the caller runs it when the subscription is
//! unsubscribed and when the connection is lost.

use std::time::{Duration, Instant};

use bytes::Bytes;

/// The largest window that is honoured, in milliseconds (2^31 - 1). A larger
/// window from the server is capped to this length.
pub(crate) const MAX_WINDOW_MILLIS: u32 = 2_147_483_647;

/// What a publish does with the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublishAction {
    /// The data is sent now. The caller completes the publish with the server's
    /// answer.
    Send(Bytes),
    /// The data replaced the pending value. The caller completes the publish at
    /// once with success.
    Coalesced,
}

/// The debounce window of one subscription.
#[derive(Debug, Default)]
pub(crate) struct PublishDebounce {
    /// The window length; `None` when debouncing is off.
    window: Option<Duration>,
    /// The end of the open window; `None` when no window is open.
    deadline: Option<Instant>,
    /// The latest value published while the window was open.
    pending: Option<Bytes>,
}

impl PublishDebounce {
    /// Sets the window from the subscribe reply. Zero turns debouncing off. Any
    /// open window and pending value are dropped first, and the new window
    /// starts closed.
    pub(crate) fn set_window(&mut self, millis: u32) {
        self.reset();
        if millis == 0 {
            return;
        }
        let capped: u32 = millis.min(MAX_WINDOW_MILLIS);
        self.window = Some(Duration::from_millis(u64::from(capped)));
    }

    /// Drops the window, the open window and the pending value. Publishes are
    /// sent as they come until the next [`set_window`](Self::set_window).
    pub(crate) fn reset(&mut self) {
        self.window = None;
        self.deadline = None;
        self.pending = None;
    }

    /// Takes a publish at `now`. The first publish of a window is sent and opens
    /// the window; a publish inside an open window replaces the pending value.
    pub(crate) fn publish(&mut self, data: Bytes, now: Instant) -> PublishAction {
        let Some(window) = self.window else {
            return PublishAction::Send(data);
        };
        if self.deadline.is_some() {
            self.pending = Some(data);
            return PublishAction::Coalesced;
        }
        self.deadline = Some(now + window);
        PublishAction::Send(data)
    }

    /// The end of the open window, where the caller must arm its timer. `None`
    /// when no window is open, so no timer is needed.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Runs when the timer fires at `now`. When the open window has ended, it is
    /// closed: a pending value is returned to be sent and a new window starts
    /// from `now`; without one the window goes idle. Returns `None` when there is
    /// nothing to send.
    pub(crate) fn on_timer(&mut self, now: Instant) -> Option<Bytes> {
        let deadline: Instant = self.deadline?;
        if deadline > now {
            return None;
        }
        let Some(data) = self.pending.take() else {
            self.deadline = None;
            return None;
        };
        self.deadline = self.window.map(|window: Duration| now + window);
        Some(data)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bytes::Bytes;

    use super::{MAX_WINDOW_MILLIS, PublishAction, PublishDebounce};

    /// The instant `millis` after `start`.
    fn at(start: Instant, millis: u64) -> Instant {
        start + Duration::from_millis(millis)
    }

    fn data(text: &'static str) -> Bytes {
        Bytes::from_static(text.as_bytes())
    }

    #[test]
    fn publish_debounce_first_is_sent_at_once_and_window_coalesces_the_rest() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        debounce.set_window(30);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        assert_eq!(debounce.publish(data("2"), start), PublishAction::Coalesced);
        assert_eq!(debounce.publish(data("3"), start), PublishAction::Coalesced);
        assert_eq!(debounce.on_timer(at(start, 29)), None);
        assert_eq!(debounce.on_timer(at(start, 30)), Some(data("3")));
        assert_eq!(debounce.next_deadline(), Some(at(start, 60)));
    }

    #[test]
    fn publish_debounce_idle_window_ends_and_next_publish_is_sent_at_once() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        debounce.set_window(30);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        assert_eq!(debounce.on_timer(at(start, 30)), None);
        assert_eq!(debounce.next_deadline(), None);

        assert_eq!(
            debounce.publish(data("2"), at(start, 60)),
            PublishAction::Send(data("2"))
        );
        assert_eq!(debounce.next_deadline(), Some(at(start, 90)));
    }

    #[test]
    fn publish_debounce_publish_at_window_end_is_held_until_timer_runs() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        debounce.set_window(30);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        // The end time has passed, but the timer has not run: the window is
        // still open and the publish is coalesced.
        assert_eq!(
            debounce.publish(data("2"), at(start, 30)),
            PublishAction::Coalesced
        );
        assert_eq!(debounce.on_timer(at(start, 30)), Some(data("2")));
        assert_eq!(debounce.next_deadline(), Some(at(start, 60)));
    }

    #[test]
    fn publish_debounce_reset_drops_pending_value_and_window() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        debounce.set_window(30);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        assert_eq!(
            debounce.publish(data("2"), at(start, 5)),
            PublishAction::Coalesced
        );
        debounce.reset();

        assert_eq!(debounce.on_timer(at(start, 100)), None);
        assert_eq!(debounce.next_deadline(), None);
        assert_eq!(
            debounce.publish(data("3"), at(start, 100)),
            PublishAction::Send(data("3"))
        );
        assert_eq!(debounce.next_deadline(), None);
    }

    #[test]
    fn publish_debounce_set_window_drops_open_window_and_pending_value() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        debounce.set_window(30);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        assert_eq!(debounce.publish(data("2"), start), PublishAction::Coalesced);
        debounce.set_window(30);

        assert_eq!(debounce.next_deadline(), None);
        assert_eq!(debounce.on_timer(at(start, 30)), None);
        assert_eq!(
            debounce.publish(data("3"), at(start, 31)),
            PublishAction::Send(data("3"))
        );
    }

    #[test]
    fn publish_debounce_zero_window_sends_every_publish() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        debounce.set_window(0);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        assert_eq!(
            debounce.publish(data("2"), start),
            PublishAction::Send(data("2"))
        );
        assert_eq!(
            debounce.publish(data("3"), start),
            PublishAction::Send(data("3"))
        );
        assert_eq!(debounce.next_deadline(), None);
        assert_eq!(debounce.on_timer(at(start, 1_000)), None);
    }

    #[test]
    fn publish_debounce_window_above_signed_int_range_is_capped() {
        let start: Instant = Instant::now();
        let mut debounce: PublishDebounce = PublishDebounce::default();
        // 2^31 + 1 ms: above the cap of 2^31 - 1 ms.
        debounce.set_window(2_147_483_649);

        assert_eq!(
            debounce.publish(data("1"), start),
            PublishAction::Send(data("1"))
        );
        let cap: u64 = u64::from(MAX_WINDOW_MILLIS);
        assert_eq!(debounce.next_deadline(), Some(at(start, cap)));

        assert_eq!(
            debounce.publish(data("2"), at(start, 1)),
            PublishAction::Coalesced
        );
        // One millisecond short of the cap: the window is still open.
        assert_eq!(debounce.on_timer(at(start, cap - 1)), None);
        assert_eq!(debounce.on_timer(at(start, cap)), Some(data("2")));
    }
}
