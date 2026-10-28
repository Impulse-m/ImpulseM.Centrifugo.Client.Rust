//! Server time of a client: the local wall clock plus the offset of the last
//! command reply that carried a time.
//!
//! The server sets `Reply.time`, its Unix time in milliseconds, on command
//! replies when it runs with `client.reply_include_server_time`. The client
//! keeps the offset of that time from its own wall clock, so the server time is
//! the local clock plus the offset. A reply without a time keeps the last
//! offset. Before any reply with a time the offset is zero, so the server time
//! is the local time.
//!
//! The server is untrusted input: a time more than 365 days from the local
//! clock is ignored, and so is a difference that overflows.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The largest offset a reply may set, in milliseconds (365 days). A larger one
/// is ignored.
pub(crate) const MAX_OFFSET_MILLIS: i64 = 365 * 86_400_000;

/// The local wall clock in milliseconds since the unix epoch.
pub(crate) fn wall_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d: Duration| {
            i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
        })
}

/// The offset a reply sets: its server time minus the local wall clock.
///
/// `None` for a reply without a time (0, the value of an unset field), for a
/// difference that overflows, and for one beyond [`MAX_OFFSET_MILLIS`]. The
/// caller then keeps the previous offset.
pub(crate) fn offset_from(time: i64, local_millis: i64) -> Option<i64> {
    if time == 0 {
        return None;
    }
    let difference: i64 = time.checked_sub(local_millis)?;
    if !(-MAX_OFFSET_MILLIS..=MAX_OFFSET_MILLIS).contains(&difference) {
        return None;
    }
    Some(difference)
}

/// The offset of the server time from the local clock, shared by the handles of
/// one client.
#[derive(Debug, Default)]
pub(crate) struct ServerTime {
    /// Written by the driver, read by any thread. A single value, so nothing
    /// else is ordered against it.
    offset_millis: AtomicI64,
}

impl ServerTime {
    /// Takes the time of a command reply: when it implies an offset, that
    /// offset replaces the last one.
    pub(crate) fn observe(&self, time: i64) {
        if let Some(offset) = offset_from(time, wall_millis()) {
            self.offset_millis.store(offset, Ordering::Relaxed);
        }
    }

    /// The server time in milliseconds since the unix epoch.
    pub(crate) fn now_millis(&self) -> i64 {
        let offset: i64 = self.offset_millis.load(Ordering::Relaxed);
        wall_millis().saturating_add(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_OFFSET_MILLIS, ServerTime, offset_from, wall_millis};

    #[test]
    fn server_time_zero_time_sets_no_offset() {
        assert_eq!(offset_from(0, 1_000), None);
    }

    #[test]
    fn server_time_offset_is_the_difference_to_the_local_clock() {
        assert_eq!(offset_from(5_000, 2_000), Some(3_000));
        assert_eq!(offset_from(2_000, 5_000), Some(-3_000));
    }

    #[test]
    fn server_time_offset_beyond_365_days_is_ignored() {
        assert_eq!(offset_from(MAX_OFFSET_MILLIS, 0), Some(MAX_OFFSET_MILLIS));
        assert_eq!(offset_from(MAX_OFFSET_MILLIS + 1, 0), None);
        assert_eq!(offset_from(-MAX_OFFSET_MILLIS - 1, 0), None);
    }

    #[test]
    fn server_time_overflowing_difference_is_ignored() {
        assert_eq!(offset_from(i64::MAX, -1), None);
        assert_eq!(offset_from(i64::MIN + 1, 1), None);
    }

    #[test]
    fn server_time_before_any_reply_is_the_local_time() {
        let clock: ServerTime = ServerTime::default();
        let server: i64 = clock.now_millis();
        let local: i64 = wall_millis();
        assert!((local - server).abs() < 1_000);
    }

    #[test]
    fn server_time_observed_offset_is_kept_until_the_next_time() {
        let clock: ServerTime = ServerTime::default();
        let ahead: i64 = wall_millis() + 3_600_000;
        clock.observe(ahead);
        clock.observe(0);
        let server: i64 = clock.now_millis();
        assert!((server - (wall_millis() + 3_600_000)).abs() < 1_000);
    }

    #[test]
    fn server_time_out_of_range_reply_keeps_the_previous_offset() {
        let clock: ServerTime = ServerTime::default();
        clock.observe(wall_millis() + 3_600_000);
        clock.observe(wall_millis() + 400 * 86_400_000);
        let server: i64 = clock.now_millis();
        assert!((server - (wall_millis() + 3_600_000)).abs() < 1_000);
    }
}
