//! The resends of an rpc pool call: the server's later signal, the delay of a
//! later resend, and the count of resends a call may still make.
//!
//! A call makes at most `retries` resends, counted over every cause together:
//! a call lost with its connection, a server error that carries the later
//! signal, and a move by the reroute hook. A later resend waits a full-jitter
//! delay, uniform over zero up to 250 ms doubled per later resend so far,
//! capped at the time left to the call's deadline, so that it is never due
//! after the deadline.

use std::hash::{BuildHasher as _, Hasher as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::ServerError;

/// The metadata key of the retry signal that the server sets on its errors.
pub(crate) const RETRY_METADATA_KEY: &str = "x-centrifugo-retry";

/// The signal value that asks for a resend later.
const RETRY_VALUE_LATER: &str = "later";

/// Base of the later-resend delay in nanoseconds: 250 ms, doubled per later
/// resend so far.
const LATER_BASE_NANOS: u64 = 250_000_000;

/// The lowest code of a server error reply. Codes below it are the client's
/// own (unsupported headers, limit, cancel, redirect), never a resend signal.
pub(crate) const SERVER_ERROR_FLOOR: u32 = 100;

/// Whether an error is a server reply, as opposed to a client-local error.
pub(crate) fn is_server_reply(error: &ServerError) -> bool {
    error.code >= SERVER_ERROR_FLOOR
}

/// Whether a server error asks for a resend later, with the same call.
pub(crate) fn wants_later(error: &ServerError) -> bool {
    error
        .metadata
        .get(RETRY_METADATA_KEY)
        .is_some_and(|value: &String| value == RETRY_VALUE_LATER)
}

/// Whether a call may make one more resend: it has resends left, and its
/// deadline is still ahead.
pub(crate) fn may_resend(resends: usize, retries: usize, deadline_ahead: bool) -> bool {
    deadline_ahead && resends < retries
}

/// The delay of a later resend. `attempt` is the number of later resends the
/// call has made so far, `remaining` the time to its deadline, and `draw` a
/// random value. The delay is uniform over zero up to the ceiling, which is
/// 250 ms doubled per attempt and capped at `remaining`.
pub(crate) fn later_delay(attempt: u32, remaining: Duration, draw: u64) -> Duration {
    let remaining_nanos: u64 = u64::try_from(remaining.as_nanos()).unwrap_or(u64::MAX);
    let shift: u32 = attempt.min(62);
    let scaled: u64 = LATER_BASE_NANOS
        .checked_mul(1_u64 << shift)
        .unwrap_or(remaining_nanos);
    let ceiling: u64 = scaled.min(remaining_nanos);
    let nanos: u64 = draw % ceiling.saturating_add(1);
    Duration::from_nanos(nanos)
}

/// A random value for a later resend's delay. Each call draws a new one, from
/// a hasher seeded per process and per draw.
pub(crate) fn draw() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher: std::collections::hash_map::DefaultHasher =
        std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use centrifuge_client_core::proto;

    use super::{RETRY_METADATA_KEY, later_delay, may_resend, wants_later};
    use crate::ServerError;

    fn error_with(metadata: HashMap<String, String>) -> ServerError {
        proto::Error {
            code: 100,
            message: "refused".to_owned(),
            temporary: true,
            metadata,
            ..Default::default()
        }
        .into()
    }

    #[test]
    fn later_signal_is_read_from_the_error_metadata() {
        let later: HashMap<String, String> =
            HashMap::from([(RETRY_METADATA_KEY.to_owned(), "later".to_owned())]);
        assert!(wants_later(&error_with(later)));
        let other: HashMap<String, String> =
            HashMap::from([(RETRY_METADATA_KEY.to_owned(), "new_msg_id".to_owned())]);
        assert!(!wants_later(&error_with(other)));
        assert!(!wants_later(&error_with(HashMap::new())));
    }

    #[test]
    fn resends_stop_at_retries_and_at_the_deadline() {
        assert!(may_resend(0, 3, true));
        assert!(may_resend(2, 3, true));
        assert!(!may_resend(3, 3, true));
        assert!(!may_resend(0, 0, true));
        assert!(!may_resend(0, 3, false));
    }

    #[test]
    fn later_delay_stays_within_the_ceiling_and_the_deadline() {
        let remaining: Duration = Duration::from_secs(1);
        assert!(later_delay(0, remaining, u64::MAX) <= Duration::from_millis(250));
        assert!(later_delay(2, remaining, u64::MAX) <= Duration::from_secs(1));
        assert_eq!(later_delay(0, remaining, 0), Duration::ZERO);
        let short: Duration = Duration::from_millis(100);
        assert!(later_delay(5, short, u64::MAX) <= short);
        assert_eq!(later_delay(5, Duration::ZERO, u64::MAX), Duration::ZERO);
    }

    #[test]
    fn later_delay_doubles_its_ceiling_per_attempt() {
        let remaining: Duration = Duration::from_secs(60);
        let first: Duration = later_delay(0, remaining, 250_000_000);
        let third: Duration = later_delay(2, remaining, 1_000_000_000);
        assert_eq!(first, Duration::from_millis(250));
        assert_eq!(third, Duration::from_secs(1));
    }
}
