//! Message ids of tracked rpc calls.
//!
//! A `msg_id` packs the unix second of the call into its high 33 bits and a
//! sequence into its low 20 bits. The sequence of a second starts at a lane
//! drawn at random, so two generators in one second rarely issue the same id.
//! Every id is below 2^53, so it stays exact in a JavaScript number.
//!
//! The time part never moves back on its own: a clock that goes backwards
//! keeps the last second, and the sequence goes on. In the largest second the
//! generator refuses to issue more ids after 2^20 of them.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use crate::error::Error;

/// Bits of the sequence part of a `msg_id`.
const SEQUENCE_BITS: u32 = 20;

/// The sequence of a second moves on to the next second at this value.
const SEQUENCE_LIMIT: u64 = 1 << SEQUENCE_BITS;

/// Largest time part: 33 bits of unix seconds.
const MAX_SECONDS: u64 = (1 << 33) - 1;

/// Lanes are drawn from `[0, LANE_BOUND)`: a second holds at least 2^19 ids
/// from its lane on.
const LANE_BOUND: u64 = 1 << 19;

/// Issues the `msg_id` of each tracked rpc call of one client.
#[derive(Debug)]
pub(crate) struct MsgIdGenerator {
    lane: u64,
    /// The time part of the last `msg_id` issued.
    last_seconds: u64,
    /// The sequence the next `msg_id` of `last_seconds` takes.
    next_seq: u64,
}

impl MsgIdGenerator {
    /// A generator whose first id takes the second of `now_ms`.
    pub(crate) fn new(now_ms: i64, lane: u64) -> Self {
        Self {
            lane,
            last_seconds: clamp_seconds(now_ms),
            next_seq: lane,
        }
    }

    /// A lane for a new generator, drawn at random from `[0, LANE_BOUND)`.
    pub(crate) fn random_lane() -> u64 {
        // The std random state is seeded per process and per instance: enough
        // to keep two generators apart, not a cryptographic source.
        RandomState::new().hash_one(0_u8) % LANE_BOUND
    }

    /// The `msg_id` of a call written at `server_time_ms`.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] when the largest second has no ids left.
    pub(crate) fn next(&mut self, server_time_ms: i64) -> Result<u64, Error> {
        let mut seconds: u64 = clamp_seconds(server_time_ms).max(self.last_seconds);
        let mut seq: u64 = if seconds == self.last_seconds {
            self.next_seq
        } else {
            self.lane
        };
        if seq >= SEQUENCE_LIMIT {
            if seconds >= MAX_SECONDS {
                return Err(Error::Configuration("msg_id space exhausted".into()));
            }
            seconds += 1;
            seq = self.lane;
        }
        self.last_seconds = seconds;
        self.next_seq = seq + 1;
        Ok((seconds << SEQUENCE_BITS) + seq)
    }
}

/// The unix second of a time in milliseconds, clamped to the 33-bit time part.
fn clamp_seconds(time_ms: i64) -> u64 {
    let Ok(seconds) = u64::try_from(time_ms / 1000) else {
        return 0;
    };
    seconds.min(MAX_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::{LANE_BOUND, MAX_SECONDS, MsgIdGenerator, SEQUENCE_LIMIT};
    use crate::error::Error;

    const NOW_MS: i64 = 1_700_000_000_000;

    #[test]
    fn rpc_msg_id_packs_the_second_and_the_sequence() {
        let mut generator: MsgIdGenerator = MsgIdGenerator::new(NOW_MS, 5);
        let first: u64 = generator.next(NOW_MS).expect("an id");
        let second: u64 = generator.next(NOW_MS).expect("an id");
        assert_eq!(first, (1_700_000_000 << 20) + 5);
        assert_eq!(second, first + 1);
    }

    #[test]
    fn rpc_msg_id_takes_the_next_second_when_the_sequence_is_full() {
        let mut generator: MsgIdGenerator = MsgIdGenerator::new(NOW_MS, 5);
        generator.next_seq = SEQUENCE_LIMIT - 1;
        let last_of_second: u64 = generator.next(NOW_MS).expect("an id");
        let first_of_next: u64 = generator.next(NOW_MS).expect("an id");
        assert_eq!(last_of_second, (1_700_000_000 << 20) + SEQUENCE_LIMIT - 1);
        assert_eq!(first_of_next, (1_700_000_001 << 20) + 5);
    }

    #[test]
    fn rpc_msg_id_keeps_the_last_second_when_the_clock_goes_back() {
        let mut generator: MsgIdGenerator = MsgIdGenerator::new(NOW_MS, 0);
        let later: u64 = generator.next(NOW_MS + 5_000).expect("an id");
        let earlier: u64 = generator.next(NOW_MS).expect("an id");
        assert_eq!(later >> 20, 1_700_000_005);
        assert_eq!(earlier >> 20, 1_700_000_005);
        assert_eq!(earlier, later + 1);
    }

    #[test]
    fn rpc_msg_id_is_refused_once_the_largest_second_is_full() {
        let mut generator: MsgIdGenerator = MsgIdGenerator::new(NOW_MS, 0);
        generator.last_seconds = MAX_SECONDS;
        generator.next_seq = SEQUENCE_LIMIT;
        let refused: Result<u64, Error> = generator.next(NOW_MS);
        assert!(matches!(refused, Err(Error::Configuration(_))));
    }

    #[test]
    fn rpc_msg_id_stays_below_two_pow_53() {
        let mut generator: MsgIdGenerator = MsgIdGenerator::new(i64::MAX, LANE_BOUND - 1);
        let id: u64 = generator.next(i64::MAX).expect("an id");
        assert!(id < 1 << 53);
    }

    #[test]
    fn rpc_random_lane_stays_in_its_bound() {
        for _ in 0..200 {
            assert!(MsgIdGenerator::random_lane() < LANE_BOUND);
        }
    }
}
