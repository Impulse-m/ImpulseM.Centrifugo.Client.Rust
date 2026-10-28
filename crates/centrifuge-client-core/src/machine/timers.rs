//! Deadlines owned by the core (docs/spec.md §9.7). The driver sleeps until the
//! earliest one and reports it back with [`super::Input::TimerFired`].

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use super::SubKey;

/// What a deadline is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimerKey {
    /// Next connect attempt.
    Reconnect,
    /// Transport open (handshake) deadline.
    Open,
    /// Connect reply deadline.
    ConnectReply,
    /// No-ping deadline.
    NoPing,
    /// Connection token refresh.
    Refresh,
    /// A core-issued command's reply deadline, by command id.
    Command(u32),
    /// Resubscribe after a delay.
    Resubscribe(SubKey),
    /// Subscription token refresh.
    SubRefresh(SubKey),
    /// Backoff reset when no ping or reply arrives (docs/spec.md §6.3).
    BackoffReset,
}

/// A small ordered set of deadlines, with O(log n) arm, cancel and peek.
#[derive(Debug, Default)]
pub(crate) struct Timers {
    by_key: HashMap<TimerKey, (Instant, u64)>,
    order: BTreeSet<(Instant, u64, TimerKey)>,
    seq: u64,
}

impl Timers {
    pub(crate) fn arm(&mut self, key: TimerKey, at: Instant) {
        self.cancel(key);
        self.seq += 1;
        self.by_key.insert(key, (at, self.seq));
        self.order.insert((at, self.seq, key));
    }

    pub(crate) fn arm_after(&mut self, key: TimerKey, now: Instant, d: Duration) {
        // Saturating: a huge duration means "practically never", not a panic.
        let at = now
            .checked_add(d)
            .unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 24 * 3600));
        self.arm(key, at);
    }

    /// The keys armed, for re-arming after a system sleep.
    pub(crate) fn keys(&self) -> Vec<TimerKey> {
        self.by_key.keys().copied().collect()
    }

    pub(crate) fn cancel(&mut self, key: TimerKey) {
        if let Some((at, seq)) = self.by_key.remove(&key) {
            self.order.remove(&(at, seq, key));
        }
    }

    pub(crate) fn deadline(&self, key: TimerKey) -> Option<Instant> {
        self.by_key.get(&key).map(|(at, _)| *at)
    }

    pub(crate) fn is_armed(&self, key: TimerKey) -> bool {
        self.by_key.contains_key(&key)
    }

    /// The earliest deadline.
    pub(crate) fn next(&self) -> Option<(Instant, TimerKey)> {
        self.order.first().map(|(at, _, key)| (*at, *key))
    }

    /// Removes `key` if it's due at or before `now`, returning whether it was.
    pub(crate) fn take_due(&mut self, key: TimerKey, now: Instant) -> bool {
        match self.by_key.get(&key) {
            Some((at, _)) if *at <= now => {
                self.cancel(key);
                true
            }
            _ => false,
        }
    }
}

/// splitmix64: a tiny deterministic generator for backoff jitter, seeded by
/// the driver (from the OS) or by tests (fixed).
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform duration in `[lo, hi]`.
    pub(crate) fn between(&mut self, lo: Duration, hi: Duration) -> Duration {
        if hi <= lo {
            return lo;
        }
        let span = u64::try_from(hi.saturating_sub(lo).as_micros()).unwrap_or(u64::MAX);
        lo.saturating_add(Duration::from_micros(
            self.next_u64() % span.saturating_add(1),
        ))
    }
}

/// Jittered exponential backoff, exactly as centrifuge-js `backoff()` (full
/// jitter): `min + random(0, min(max, min * 2^attempt))`, capped at `max`.
pub(crate) fn backoff(rng: &mut Rng, attempt: u32, min: Duration, max: Duration) -> Duration {
    let factor = 1u32 << attempt.min(31);
    let interval = min.checked_mul(factor).unwrap_or(max).min(max);
    min.saturating_add(rng.between(Duration::ZERO, interval))
        .min(max)
}

/// The shortest delay of a retry of a failed token refresh.
pub(crate) const TOKEN_RETRY_MINIMUM: Duration = Duration::from_secs(10);

/// The ceiling of the first token refresh retry. It doubles with each failure
/// in a row, up to the token retry cap.
pub(crate) const TOKEN_RETRY_FIRST_CEILING: Duration = Duration::from_secs(20);

/// The delay of the retry after `step` consecutive failed token refreshes
/// (docs/spec.md §6.6), the shape of the Swift and Java SDKs.
///
/// The ceiling is `max(20 s, min(cap, 20 s * 2^step))`, and the delay is
/// `min(ceiling, 10 s + uniform(0, min(ceiling, 10 s * 2^step)))`. The first
/// retry is therefore 10 to 20 s, and a cap at or below 20 s keeps every
/// retry flat in that range. The delay never depends on the token's expiry.
pub(crate) fn token_retry_delay(rng: &mut Rng, step: u32, cap: Duration) -> Duration {
    let factor: u32 = 1 << step.min(16);
    let scaled_ceiling = TOKEN_RETRY_FIRST_CEILING.checked_mul(factor).unwrap_or(cap);
    let ceiling = TOKEN_RETRY_FIRST_CEILING.max(cap.min(scaled_ceiling));
    let scaled_jitter = TOKEN_RETRY_MINIMUM.checked_mul(factor).unwrap_or(ceiling);
    let jitter = rng.between(Duration::ZERO, ceiling.min(scaled_jitter));
    ceiling.min(TOKEN_RETRY_MINIMUM.saturating_add(jitter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timers_order_cancel_and_take_due() {
        let t0 = Instant::now();
        let mut t = Timers::default();
        t.arm_after(TimerKey::Refresh, t0, Duration::from_secs(5));
        t.arm_after(TimerKey::NoPing, t0, Duration::from_secs(1));
        t.arm_after(TimerKey::Command(7), t0, Duration::from_secs(3));
        assert_eq!(
            t.next(),
            Some((t0 + Duration::from_secs(1), TimerKey::NoPing))
        );
        t.arm_after(TimerKey::NoPing, t0, Duration::from_secs(10));
        assert_eq!(t.next().map(|x| x.1), Some(TimerKey::Command(7)));
        assert!(!t.take_due(TimerKey::Command(7), t0 + Duration::from_secs(2)));
        assert!(t.take_due(TimerKey::Command(7), t0 + Duration::from_secs(3)));
        t.cancel(TimerKey::Refresh);
        assert_eq!(t.next().map(|x| x.1), Some(TimerKey::NoPing));
        t.cancel(TimerKey::NoPing);
        assert_eq!(t.next(), None);
    }

    #[test]
    fn backoff_stays_in_bounds_and_grows() {
        let mut rng = Rng::new(1);
        let (min, max) = (Duration::from_millis(500), Duration::from_secs(20));
        for attempt in 0..40 {
            let d = backoff(&mut rng, attempt, min, max);
            assert!(d >= min && d <= max, "attempt {attempt}: {d:?}");
        }
        let first = backoff(&mut rng, 0, min, max);
        assert!(first >= min && first <= min * 2, "{first:?}");
        let late: Vec<Duration> = (0..50).map(|_| backoff(&mut rng, 10, min, max)).collect();
        assert!(late.iter().any(|d| *d > Duration::from_secs(5)));
    }

    #[test]
    fn token_retry_first_retry_is_ten_to_twenty_seconds() {
        let mut rng = Rng::new(7);
        for _ in 0..500 {
            let d = token_retry_delay(&mut rng, 0, Duration::from_secs(300));
            assert!(
                d >= Duration::from_secs(10) && d <= Duration::from_secs(20),
                "{d:?}"
            );
        }
    }

    #[test]
    fn token_retry_doubles_up_to_the_cap() {
        let mut rng = Rng::new(11);
        let cap = Duration::from_secs(300);
        let mut max_seen = [Duration::ZERO; 6];
        for step in 0_u32..6 {
            for _ in 0..500 {
                let d = token_retry_delay(&mut rng, step, cap);
                assert!(
                    d >= Duration::from_secs(10) && d <= cap,
                    "step {step}: {d:?}"
                );
                let seen = &mut max_seen[step as usize];
                *seen = (*seen).max(d);
            }
        }
        // The range of step n is 10 s to 10 s + 10 s * 2^n (bounded by the ceiling).
        assert!(max_seen[1] <= Duration::from_secs(30), "{:?}", max_seen[1]);
        assert!(max_seen[2] <= Duration::from_secs(50), "{:?}", max_seen[2]);
        assert!(max_seen[2] > Duration::from_secs(30), "{:?}", max_seen[2]);
        assert!(max_seen[5] > Duration::from_secs(120), "{:?}", max_seen[5]);
    }

    #[test]
    fn token_retry_cap_at_or_below_twenty_seconds_keeps_the_flat_retry() {
        let mut rng = Rng::new(3);
        for step in [0_u32, 1, 4, 30, u32::MAX] {
            for _ in 0..200 {
                let d = token_retry_delay(&mut rng, step, Duration::from_secs(5));
                assert!(
                    d >= Duration::from_secs(10) && d <= Duration::from_secs(20),
                    "step {step}: {d:?}"
                );
            }
        }
    }

    #[test]
    fn token_retry_huge_step_does_not_overflow() {
        let mut rng = Rng::new(5);
        let cap = Duration::from_secs(24 * 3600);
        for _ in 0..200 {
            let d = token_retry_delay(&mut rng, u32::MAX, cap);
            assert!(d >= Duration::from_secs(10) && d <= cap, "{d:?}");
        }
    }
}
