//! The connection token policy (docs/spec.md §6.6).
//!
//! Tokens reach the client only through the token getters: the closure of
//! [`ClientBuilder::get_token`](crate::ClientBuilder::get_token) for the
//! connection, and the closure of the subscription builder for a channel.
//!
//! A failed timer refresh is retried with a delay that starts at 10 to 20 s
//! and doubles with each failure in a row, up to
//! [`ClientBuilder::token_retry_cap`](crate::ClientBuilder::token_retry_cap).
//! A token the server refuses as expired (109) is not sent again while
//! [`ClientBuilder::stop_on_refused_token`](crate::ClientBuilder::stop_on_refused_token)
//! is set: the next connect asks the getter for a new one. The retry timer
//! and the refusal memory live in the sans-IO core, which is why the rules run
//! there; this module validates the settings the builder takes for them.

use std::time::Duration;

use crate::config::duration;
use crate::error::Error;

/// Checks the token retry cap: longer than zero and at most 24 hours.
pub(crate) fn validate_token_retry_cap(cap: Duration) -> Result<Duration, Error> {
    duration("token_retry_cap", cap)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use centrifuge_client_core::machine::CoreConfig;

    use crate::client::Client;
    use crate::config::defaults;
    use crate::error::Error;

    use super::validate_token_retry_cap;

    #[test]
    fn token_retry_cap_defaults_to_five_minutes_and_stop_on_refused_token_to_true() {
        assert_eq!(defaults::TOKEN_RETRY_CAP, Duration::from_secs(300));
        let core = CoreConfig::default();
        assert_eq!(core.token_retry_cap, Duration::from_secs(300));
        assert!(core.stop_on_refused_token);
    }

    #[test]
    fn token_retry_cap_rejects_zero_and_more_than_a_day() {
        assert!(matches!(
            validate_token_retry_cap(Duration::ZERO),
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            validate_token_retry_cap(Duration::from_secs(24 * 3600 + 1)),
            Err(Error::Configuration(_))
        ));
        assert_eq!(
            validate_token_retry_cap(Duration::from_secs(24 * 3600)).unwrap(),
            Duration::from_secs(24 * 3600)
        );
    }

    #[tokio::test]
    async fn token_retry_cap_zero_fails_build() {
        let built = Client::builder("wss://example.com/connection/websocket")
            .token_retry_cap(Duration::ZERO)
            .build();
        assert!(matches!(built, Err(Error::Configuration(_))));
    }

    #[tokio::test]
    async fn token_options_valid_build() {
        let built = Client::builder("wss://example.com/connection/websocket")
            .token_retry_cap(Duration::from_secs(30))
            .stop_on_refused_token(false)
            .build();
        assert!(built.is_ok());
    }
}
