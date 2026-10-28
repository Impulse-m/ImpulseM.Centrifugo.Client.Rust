//! The connection tokens of a pool with `token_from`: each connection is lent
//! the main client's confirmed token, and a connection that is refused its
//! token asks the main client for one renewal per confirmed token.
//!
//! The confirmed token is the one the server accepted at the main client's
//! last connect or accepted refresh. A connection's request waits while the
//! main client connects, and fails with unauthorized while the main client is
//! disconnected or closed. A request for the token the connection presented
//! last is a refusal: the main client renews its token once, and when that
//! renewal ends refused (103, 109, or a non-temporary error) the confirmed
//! token is spent. A spent token is lent to no connection until the main
//! client confirms its next token, and the calls that no connection can take
//! fail with code 3, `token refused`.
//!
//! The source runs no timer of its own: it decides on each snapshot of the
//! main client, and the pool's connections wait on those snapshots.

use std::sync::{Arc, Mutex, MutexGuard};

use centrifuge_client_core::machine::{Op, State, TokenStatus};
use tokio::sync::watch;

use crate::error::TokenError;
use crate::shared::{Shared, Snapshot, lock};

/// What a connection's request is answered with, given the main client's
/// snapshot.
#[derive(Debug)]
enum Step {
    /// The request is answered now.
    Answer(Result<Option<String>, TokenError>),
    /// The request waits for the next snapshot of the main client.
    Wait,
    /// The main client is asked for a renewal, and the request waits.
    Renew,
}

/// A renewal of the main client that was asked for, for one confirmed token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Renewal {
    /// The confirmation count the renewal was asked for at.
    confirmations: u64,
    /// The refresh settle count when the renewal was asked for. A later count
    /// means the renewal ended.
    settled_at_start: u64,
}

/// The state of the source under its lock.
#[derive(Debug, Default)]
struct SourceState {
    /// The confirmation count of the spent token, if one is spent.
    spent: Option<u64>,
    /// The renewal in progress or last asked for.
    renewal: Option<Renewal>,
}

/// The token source of one pool with `token_from`.
#[derive(Debug, Default)]
pub(crate) struct TokenSource {
    /// The spent token and the renewal, shared by the pool's connections.
    state: Mutex<SourceState>,
}

impl TokenSource {
    /// A source with nothing spent and no renewal in progress.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Whether the confirmed token of the main client is spent: no connection
    /// takes it until the main client confirms its next token.
    pub(crate) fn is_spent(&self, main: &Shared) -> bool {
        let confirmations: u64 = main.snapshot.borrow().token.confirmations;
        lock(&self.state).spent == Some(confirmations)
    }

    /// The token of a connection's request. `presented` is the token the
    /// connection presented last, and is updated with the token lent.
    ///
    /// # Errors
    ///
    /// [`TokenError::Unauthorized`] while the main client is disconnected or
    /// closed, while the confirmed token is spent, or when the renewal ends
    /// refused.
    pub(crate) async fn lend(
        &self,
        main: Arc<Shared>,
        presented: Arc<Mutex<String>>,
    ) -> Result<Option<String>, TokenError> {
        let mut snapshots: watch::Receiver<Snapshot> = main.snapshot.subscribe();
        loop {
            let step: Step = {
                let snapshot: watch::Ref<'_, Snapshot> = snapshots.borrow_and_update();
                self.decide(&snapshot, &presented)
            };
            match step {
                Step::Answer(result) => return result,
                Step::Wait => {
                    if snapshots.changed().await.is_err() {
                        return Err(TokenError::Unauthorized);
                    }
                }
                Step::Renew => {
                    main.send_op(Op::RenewToken);
                }
            }
        }
    }

    /// Decides a request on a snapshot of the main client. A renewal is
    /// recorded here, and the caller sends it.
    fn decide(&self, snapshot: &Snapshot, presented: &Mutex<String>) -> Step {
        match snapshot.state {
            State::Connected => {}
            State::Disconnected | State::Closed => {
                return Step::Answer(Err(TokenError::Unauthorized));
            }
            _ => return Step::Wait,
        }
        let status: &TokenStatus = &snapshot.token;
        let Some(confirmed) = status.confirmed.clone() else {
            return Step::Wait;
        };
        let confirmations: u64 = status.confirmations;
        let mut state: MutexGuard<'_, SourceState> = lock(&self.state);
        if state.spent == Some(confirmations) {
            return Step::Answer(Err(TokenError::Unauthorized));
        }
        let mut lent: MutexGuard<'_, String> = lock(presented);
        let refused: bool = !confirmed.is_empty() && lent.as_str() == &*confirmed;
        if !refused {
            lent.clear();
            lent.push_str(&confirmed);
            let token: Option<String> = if confirmed.is_empty() {
                None
            } else {
                Some(confirmed.to_string())
            };
            return Step::Answer(Ok(token));
        }
        drop(lent);
        if !status.renewable {
            state.spent = Some(confirmations);
            return Step::Answer(Err(TokenError::Unauthorized));
        }
        match state.renewal {
            Some(renewal) if renewal.confirmations == confirmations => {
                if status.refresh_settled > renewal.settled_at_start {
                    state.spent = Some(confirmations);
                    return Step::Answer(Err(TokenError::Unauthorized));
                }
                Step::Wait
            }
            _ => {
                state.renewal = Some(Renewal {
                    confirmations,
                    settled_at_start: status.refresh_settled,
                });
                Step::Renew
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use centrifuge_client_core::machine::{State, TokenStatus};

    use super::{Step, TokenSource};
    use crate::error::TokenError;
    use crate::shared::{Snapshot, lock};

    fn snapshot(state: State, token: TokenStatus) -> Snapshot {
        Snapshot {
            state,
            token,
            ..Default::default()
        }
    }

    fn status(confirmed: &str, confirmations: u64, settled: u64, renewable: bool) -> TokenStatus {
        TokenStatus {
            confirmed: Some(confirmed.into()),
            confirmations,
            refresh_settled: settled,
            renewable,
        }
    }

    fn answer(step: Step) -> Result<Option<String>, TokenError> {
        match step {
            Step::Answer(result) => result,
            other => panic!("expected an answer, got {other:?}"),
        }
    }

    #[test]
    fn a_connecting_main_client_makes_the_request_wait() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new(String::new());
        let step: Step = source.decide(
            &snapshot(State::Connecting, status("a", 1, 0, true)),
            &presented,
        );
        assert!(matches!(step, Step::Wait));
    }

    #[test]
    fn a_disconnected_main_client_refuses_the_request() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new(String::new());
        let step: Step = source.decide(
            &snapshot(State::Disconnected, status("a", 1, 0, true)),
            &presented,
        );
        assert!(matches!(answer(step), Err(TokenError::Unauthorized)));
    }

    #[test]
    fn the_confirmed_token_is_lent_and_remembered() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new(String::new());
        let step: Step = source.decide(
            &snapshot(State::Connected, status("a", 1, 0, true)),
            &presented,
        );
        assert_eq!(answer(step).ok(), Some(Some("a".to_owned())));
        assert_eq!(lock(&presented).as_str(), "a");
    }

    #[test]
    fn an_empty_confirmed_token_is_no_token() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new(String::new());
        let step: Step = source.decide(
            &snapshot(State::Connected, status("", 1, 0, true)),
            &presented,
        );
        assert_eq!(answer(step).ok(), Some(None));
    }

    #[test]
    fn a_refused_token_renews_the_main_client_once_per_confirmation() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new("a".to_owned());
        let refused: Snapshot = snapshot(State::Connected, status("a", 1, 0, true));
        assert!(matches!(source.decide(&refused, &presented), Step::Renew));
        assert!(matches!(source.decide(&refused, &presented), Step::Wait));
    }

    #[test]
    fn a_renewal_that_settles_without_a_new_token_spends_it() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new("a".to_owned());
        let refused: Snapshot = snapshot(State::Connected, status("a", 1, 0, true));
        assert!(matches!(source.decide(&refused, &presented), Step::Renew));
        let settled: Snapshot = snapshot(State::Connected, status("a", 1, 1, true));
        assert!(matches!(
            answer(source.decide(&settled, &presented)),
            Err(TokenError::Unauthorized)
        ));
        assert!(spent_for(&source, 1));
        assert!(matches!(
            answer(source.decide(&refused, &presented)),
            Err(TokenError::Unauthorized)
        ));
    }

    #[test]
    fn a_new_confirmation_lifts_the_spent_token() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new("a".to_owned());
        let refused: Snapshot = snapshot(State::Connected, status("a", 1, 0, false));
        assert!(matches!(
            answer(source.decide(&refused, &presented)),
            Err(TokenError::Unauthorized)
        ));
        let confirmed_again: Snapshot = snapshot(State::Connected, status("a", 2, 0, false));
        let fresh: Mutex<String> = Mutex::new(String::new());
        assert_eq!(
            answer(source.decide(&confirmed_again, &fresh)).ok(),
            Some(Some("a".to_owned()))
        );
    }

    #[test]
    fn a_connection_that_presented_the_spent_token_is_refused_again() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new("a".to_owned());
        let refused: Snapshot = snapshot(State::Connected, status("a", 1, 0, false));
        assert!(matches!(
            answer(source.decide(&refused, &presented)),
            Err(TokenError::Unauthorized)
        ));
        let confirmed_again: Snapshot = snapshot(State::Connected, status("a", 2, 0, false));
        assert!(matches!(
            answer(source.decide(&confirmed_again, &presented)),
            Err(TokenError::Unauthorized)
        ));
    }

    #[test]
    fn a_token_that_is_not_renewable_is_spent_at_once() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new("a".to_owned());
        let refused: Snapshot = snapshot(State::Connected, status("a", 1, 0, false));
        assert!(matches!(
            answer(source.decide(&refused, &presented)),
            Err(TokenError::Unauthorized)
        ));
        assert!(spent_for(&source, 1));
    }

    #[test]
    fn a_new_token_is_lent_after_a_renewal_that_succeeds() {
        let source: TokenSource = TokenSource::new();
        let presented: Mutex<String> = Mutex::new("a".to_owned());
        let refused: Snapshot = snapshot(State::Connected, status("a", 1, 0, true));
        assert!(matches!(source.decide(&refused, &presented), Step::Renew));
        let renewed: Snapshot = snapshot(State::Connected, status("b", 2, 0, true));
        assert_eq!(
            answer(source.decide(&renewed, &presented)).ok(),
            Some(Some("b".to_owned()))
        );
    }

    fn spent_for(source: &TokenSource, confirmations: u64) -> bool {
        lock(&source.state).spent == Some(confirmations)
    }
}
