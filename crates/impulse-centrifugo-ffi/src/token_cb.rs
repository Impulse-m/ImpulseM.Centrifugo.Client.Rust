//! Connection and subscription tokens over the C ABI. The client asks for a token
//! through one callback, and the caller answers later with
//! `impulse_centrifugo_token_resolve`. The wait for the answer is bounded by the
//! client's command timeout and is awaited, so no runtime thread blocks on it.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use centrifuge::TokenError;
use tokio::sync::oneshot;

use crate::{
    ImpulseCentrifugoClient, ImpulseCentrifugoStatus, UserData, copy_optional, guarded, status,
};

/// Answer text used when the caller reports a failure without a message.
const MESSAGE_TOKEN_FAILED: &str = "the token callback failed";

/// Answer text used when the caller does not answer within the command timeout.
const MESSAGE_TOKEN_TIMED_OUT: &str = "the token request timed out";

/// Answer text used when no token callback is set for a subscription.
const MESSAGE_NO_CALLBACK: &str = "no token callback is set";

/// What a token request asks for.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpulseCentrifugoTokenKind {
    /// The connection token. `channel` is null.
    Connection = 1,
    /// The token of the subscription named by `channel`.
    Subscription = 2,
}

/// How a token request is answered, as the `outcome` of
/// `impulse_centrifugo_token_resolve`.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpulseCentrifugoTokenOutcome {
    /// Use the token the answer carries. The token must be non-null.
    Token = 1,
    /// No token: the connection goes on anonymously.
    Anonymous = 2,
    /// The user is not allowed: the client or the subscription stops.
    Unauthorized = 3,
    /// A failure to retry with backoff. The answer's text, when non-null, is the
    /// message of the error.
    Failed = 4,
}

impl ImpulseCentrifugoTokenOutcome {
    /// The outcome of a C value, or `None` when the value names none.
    fn from_c(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Token),
            2 => Some(Self::Anonymous),
            3 => Some(Self::Unauthorized),
            4 => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One token request, passed to the token callback. It is borrowed for the call
/// only: the callback copies what it keeps and answers with
/// `impulse_centrifugo_token_resolve` and `request_id`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoTokenRequest {
    /// Names the request in `impulse_centrifugo_token_resolve`.
    pub request_id: u64,
    /// The token the request is for.
    pub kind: ImpulseCentrifugoTokenKind,
    /// The channel of a subscription token, or null for the connection token.
    pub channel: *const c_char,
    /// Length of `channel` in bytes.
    pub channel_len: usize,
}

/// Called for every token request. It runs on a runtime thread and must not
/// block: answer later, from any thread, with `impulse_centrifugo_token_resolve`.
pub type ImpulseCentrifugoTokenCallback =
    unsafe extern "C" fn(user: *mut c_void, request: *const ImpulseCentrifugoTokenRequest);

/// The callback of one client, with its user pointer.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TokenSink {
    callback: ImpulseCentrifugoTokenCallback,
    user: UserData,
}

/// The answer to one token request, as the getter turns it into the client's
/// result.
#[derive(Debug)]
enum TokenAnswer {
    /// A token to use.
    Token(String),
    /// No token: the connection goes on anonymously.
    Anonymous,
    /// The user is not allowed: the client or the subscription stops.
    Unauthorized,
    /// A failure to retry with backoff.
    Failed(String),
}

/// The token state of one client: the callback, the requests waiting for an
/// answer and the wait limit.
#[derive(Debug)]
pub(crate) struct TokenBridge {
    sink: Mutex<Option<TokenSink>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<TokenAnswer>>>,
    next_request_id: AtomicU64,
    timeout: Duration,
}

impl TokenBridge {
    /// A bridge with no callback, waiting at most `timeout` for an answer.
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            sink: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            timeout,
        }
    }

    /// The callback, read through a poisoned lock too: the value holds no
    /// invariant a panic could break.
    fn sink(&self) -> MutexGuard<'_, Option<TokenSink>> {
        self.sink.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The waiting requests, read through a poisoned lock too.
    fn pending(&self) -> MutexGuard<'_, HashMap<u64, oneshot::Sender<TokenAnswer>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replaces the callback; `None` clears it.
    pub(crate) fn set_sink(&self, sink: Option<TokenSink>) {
        *self.sink() = sink;
    }

    /// Hands a token request to the callback and waits for its answer, at most
    /// the bridge's timeout. Without a callback a connection goes on
    /// anonymously, and a subscription fails with a retry.
    async fn fetch(&self, kind: ImpulseCentrifugoTokenKind, channel: Option<&str>) -> TokenAnswer {
        let sink: Option<TokenSink> = *self.sink();
        let Some(sink): Option<TokenSink> = sink else {
            return match kind {
                ImpulseCentrifugoTokenKind::Connection => TokenAnswer::Anonymous,
                ImpulseCentrifugoTokenKind::Subscription => {
                    TokenAnswer::Failed(MESSAGE_NO_CALLBACK.to_owned())
                }
            };
        };
        let request_id: u64 = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver): (oneshot::Sender<TokenAnswer>, oneshot::Receiver<TokenAnswer>) =
            oneshot::channel();
        self.pending().insert(request_id, sender);

        deliver_request(sink, request_id, kind, channel);

        match tokio::time::timeout(self.timeout, receiver).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(_)) => TokenAnswer::Failed(MESSAGE_TOKEN_FAILED.to_owned()),
            Err(_) => {
                self.pending().remove(&request_id);
                TokenAnswer::Failed(MESSAGE_TOKEN_TIMED_OUT.to_owned())
            }
        }
    }

    /// Delivers an answer to the request `request_id`. An unknown or finished
    /// request does nothing.
    fn answer(&self, request_id: u64, answer: TokenAnswer) {
        let sender: Option<oneshot::Sender<TokenAnswer>> = self.pending().remove(&request_id);
        if let Some(sender) = sender {
            let _: Result<(), TokenAnswer> = sender.send(answer);
        }
    }

    /// The connection token: a token, `None` for anonymous, or an error.
    pub(crate) async fn connection_token(&self) -> Result<Option<String>, TokenError> {
        match self
            .fetch(ImpulseCentrifugoTokenKind::Connection, None)
            .await
        {
            TokenAnswer::Token(token) => Ok(Some(token)),
            TokenAnswer::Anonymous => Ok(None),
            TokenAnswer::Unauthorized => Err(TokenError::Unauthorized),
            TokenAnswer::Failed(message) => Err(TokenError::from(std::io::Error::other(message))),
        }
    }

    /// The token of the subscription on `channel`. A subscription has no
    /// anonymous token, so anonymous is unauthorized.
    pub(crate) async fn subscription_token(&self, channel: &str) -> Result<String, TokenError> {
        match self
            .fetch(ImpulseCentrifugoTokenKind::Subscription, Some(channel))
            .await
        {
            TokenAnswer::Token(token) => Ok(token),
            TokenAnswer::Anonymous | TokenAnswer::Unauthorized => Err(TokenError::Unauthorized),
            TokenAnswer::Failed(message) => Err(TokenError::from(std::io::Error::other(message))),
        }
    }
}

/// Calls the token callback with one request. The raw pointers stay inside this
/// synchronous call, so no future holds them across an await.
fn deliver_request(
    sink: TokenSink,
    request_id: u64,
    kind: ImpulseCentrifugoTokenKind,
    channel: Option<&str>,
) {
    let channel_text: &str = channel.unwrap_or("");
    let channel_ptr: *const c_char = match channel {
        Some(text) => text.as_ptr().cast::<c_char>(),
        None => std::ptr::null(),
    };
    let request: ImpulseCentrifugoTokenRequest = ImpulseCentrifugoTokenRequest {
        request_id,
        kind,
        channel: channel_ptr,
        channel_len: channel_text.len(),
    };
    // SAFETY: the caller of `impulse_centrifugo_client_set_token_callback` keeps
    // `user` valid until the callback is replaced or the client is destroyed, and
    // the request is valid for the call.
    unsafe { (sink.callback)(sink.user.0, &raw const request) };
}

/// Sets the callback that answers token requests of the client: the connection
/// token and, for subscriptions created with `token_callback`, their tokens. It
/// replaces the callback set before; null clears it. Without a callback, the
/// connection goes on anonymously and a subscription retries with backoff. Set it
/// before the first connect, because the first token is requested then.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`.
/// `callback` must be null or a function that stays callable, and `user` must stay
/// valid, until the callback is replaced or the client is destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_client_set_token_callback(
    client: *const ImpulseCentrifugoClient,
    callback: Option<
        unsafe extern "C" fn(user: *mut c_void, request: *const ImpulseCentrifugoTokenRequest),
    >,
    user: *mut c_void,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        let sink: Option<TokenSink> =
            callback.map(|callback: ImpulseCentrifugoTokenCallback| TokenSink {
                callback,
                user: UserData(user),
            });
        handle.tokens.set_sink(sink);
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Answers the token request `request_id` of the client's token callback. `outcome`
/// is an `ImpulseCentrifugoTokenOutcome` value; with `Token` the token is in
/// `token`, and with `Failed` `token` is an optional message. Any thread may call
/// it, once per request; a late or unknown answer does nothing.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`. A
/// non-null `token` must be a NUL-terminated string valid for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_token_resolve(
    client: *const ImpulseCentrifugoClient,
    request_id: u64,
    outcome: i32,
    token: *const c_char,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        let Some(outcome): Option<ImpulseCentrifugoTokenOutcome> =
            ImpulseCentrifugoTokenOutcome::from_c(outcome)
        else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: a non-null `token` is NUL-terminated by the caller's contract.
        let text: Option<String> = match unsafe { copy_optional(token) } {
            Ok(text) => text,
            Err(error) => return status(error),
        };
        let answer: TokenAnswer = match outcome {
            ImpulseCentrifugoTokenOutcome::Token => match text {
                Some(token) => TokenAnswer::Token(token),
                None => return status(ImpulseCentrifugoStatus::InvalidArgument),
            },
            ImpulseCentrifugoTokenOutcome::Anonymous => TokenAnswer::Anonymous,
            ImpulseCentrifugoTokenOutcome::Unauthorized => TokenAnswer::Unauthorized,
            ImpulseCentrifugoTokenOutcome::Failed => {
                TokenAnswer::Failed(text.unwrap_or_else(|| MESSAGE_TOKEN_FAILED.to_owned()))
            }
        };
        handle.tokens.answer(request_id, answer);
        status(ImpulseCentrifugoStatus::Ok)
    })
}
