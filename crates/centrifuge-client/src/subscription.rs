//! Client-side subscriptions (docs/spec.md §4.5, §7).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use centrifuge_client_core::codec;
use centrifuge_client_core::filter::FilterNode;
use centrifuge_client_core::machine::{Op, State, SubConfig, SubState};
use centrifuge_client_core::types::{
    HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult, StreamPosition,
};

use crate::calls;
use crate::client::{
    Client, history_command, history_result, presence_command, presence_result,
    presence_stats_command, presence_stats_result, publish_command,
};
use crate::config::{defaults, duration};
use crate::dispatch::SubHandlers;
use crate::driver::{StateFuture, SubCallbacks, SubTokenFuture};
use crate::error::{BoxError, Error, TokenError};
use crate::events::{
    JoinEvent, LeaveEvent, PublicationEvent, SubscribedEvent, SubscribingEvent,
    SubscriptionErrorEvent, SubscriptionEvent, UnsubscribedEvent,
};
use crate::maybe_send::{MaybeSend, MaybeSync};
use crate::shared::{SubInner, lock};
use crate::stream::EventStream;

/// Passed to the subscription token getter.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubscriptionTokenContext {
    /// The channel the token is for.
    pub channel: String,
}

impl SubscriptionTokenContext {
    pub(crate) fn new(channel: String) -> Self {
        Self { channel }
    }
}

/// Passed to the `get_state` callback.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubscriptionGetStateContext {
    /// The channel whose state the app loads.
    pub channel: String,
}

impl SubscriptionGetStateContext {
    pub(crate) fn new(channel: String) -> Self {
        Self { channel }
    }
}

/// Delta compression for publications (docs/spec.md §7.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeltaType {
    /// Fossil delta.
    Fossil,
}

/// A client-side subscription to one channel.
///
/// Cheap to clone. Holding one keeps its client alive. Dropping the last
/// handle doesn't unsubscribe: the subscription stays registered and
/// [`Client::get_subscription`] finds it.
#[derive(Clone)]
pub struct Subscription {
    inner: Arc<SubInner>,
    client: Client,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("channel", &self.inner.channel)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl PartialEq for Subscription {
    /// Handles to the same registered subscription.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for Subscription {}

impl Subscription {
    pub(crate) fn from_parts(inner: Arc<SubInner>, client: Client) -> Self {
        Self { inner, client }
    }

    pub(crate) fn inner(&self) -> &Arc<SubInner> {
        &self.inner
    }

    fn removed(&self) -> bool {
        self.inner.removed.load(Ordering::SeqCst)
    }

    /// The channel.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.inner.channel
    }

    /// The client this subscription belongs to.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The subscription state, including the effect of operations called just
    /// before.
    #[must_use]
    pub fn state(&self) -> SubState {
        if self.removed() {
            return SubState::Unsubscribed;
        }
        self.inner.state(self.client.shared())
    }

    /// Starts subscribing. Never waits, and works before `connect()`: the
    /// subscribe goes out once connected, batched with the others. A no-op
    /// when subscribing or subscribed, after `close()` and once removed.
    pub fn subscribe(&self) {
        let shared = self.client.shared();
        if self.removed() || shared.state() == State::Closed {
            return;
        }
        shared.send_sub_op(&self.inner, Op::Subscribe(self.inner.key), |s| match s {
            SubState::Unsubscribed => SubState::Subscribing,
            s => s,
        });
    }

    /// Unsubscribes (code 0, "unsubscribe called"). The position is kept for a
    /// later `subscribe()`.
    pub fn unsubscribe(&self) {
        if self.removed() {
            return;
        }
        let shared = self.client.shared();
        shared.send_sub_op(&self.inner, Op::Unsubscribe(self.inner.key), |_| {
            SubState::Unsubscribed
        });
    }

    /// Waits until subscribed and the `subscribed` handler has returned.
    ///
    /// # Errors
    ///
    /// Fails at once with [`Error::SubscriptionUnsubscribed`] when
    /// unsubscribed, and when it becomes unsubscribed while waiting;
    /// [`Error::ClientClosed`] after `close()`; [`Error::Timeout`];
    /// [`Error::WouldDeadlock`] from an event handler while not subscribed.
    pub async fn ready(&self, timeout: Option<Duration>) -> Result<(), Error> {
        if self.removed() {
            return Err(Error::SubscriptionUnsubscribed);
        }
        calls::sub_ready(self.client.shared(), &self.inner, timeout).await
    }

    fn validate(&self, field: &'static str, data: &[u8]) -> Result<(), Error> {
        codec::validate_payload(self.client.shared().cfg.protocol, field, data)
            .map_err(|e| Error::InvalidData(e.to_string()))
    }

    /// Replaces the subscribe data sent with the next subscribe attempt,
    /// including automatic resubscribes.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidData`] when the JSON protocol is used and `data` isn't
    /// valid JSON.
    pub fn set_data(&self, data: impl Into<Bytes>) -> Result<(), Error> {
        let data = data.into();
        self.validate("data", &data)?;
        if !self.removed() {
            self.client
                .shared()
                .send_op(Op::SetSubscriptionData(self.inner.key, data));
        }
        Ok(())
    }

    /// Replaces the server-side tags filter for the next subscribe.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] unless unsubscribed, or with delta compression.
    pub fn set_tags_filter(&self, filter: Option<FilterNode>) -> Result<(), Error> {
        if self.inner.delta {
            return Err(Error::Configuration(
                "tags_filter can't be combined with delta".into(),
            ));
        }
        if self.state() != SubState::Unsubscribed {
            return Err(Error::Configuration(
                "tags_filter can only change while unsubscribed".into(),
            ));
        }
        self.client
            .shared()
            .send_op(Op::SetTagsFilter(self.inner.key, filter));
        Ok(())
    }

    fn check(&self) -> Result<(), Error> {
        if self.removed() {
            Err(Error::SubscriptionUnsubscribed)
        } else {
            Ok(())
        }
    }

    /// Publishes into the channel. Waits while subscribing, up to `timeout`.
    ///
    /// A publish whose future is dropped before it resolves may still have
    /// been delivered.
    ///
    /// # Errors
    ///
    /// [`Error::SubscriptionUnsubscribed`] when unsubscribed;
    /// [`Error::Server`] when the server rejects it; see [`Error`].
    pub async fn publish(&self, data: impl Into<Bytes>) -> Result<(), Error> {
        self.check()?;
        let data = data.into();
        self.validate("data", &data)?;
        calls::call(
            self.client.shared(),
            Some(&self.inner),
            publish_command(&self.inner.channel, data),
        )
        .await
        .map(drop)
    }

    /// [`publish()`](Self::publish) of a value serialized as JSON.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidData`] when serializing fails; else as `publish()`.
    #[cfg(feature = "serde")]
    pub async fn publish_json<T: serde::Serialize + ?Sized>(&self, value: &T) -> Result<(), Error> {
        let data = serde_json::to_vec(value).map_err(|e| Error::InvalidData(e.to_string()))?;
        self.publish(data).await
    }

    /// Reads the channel history. Waits while subscribing.
    ///
    /// **`HistoryOptions::new()` returns no publications**, only the stream
    /// position; use `.limit(n)` or `.all()` to get them.
    ///
    /// # Errors
    ///
    /// As [`publish()`](Self::publish).
    pub async fn history(&self, opts: HistoryOptions) -> Result<HistoryResult, Error> {
        self.check()?;
        calls::call(
            self.client.shared(),
            Some(&self.inner),
            history_command(&self.inner.channel, &opts),
        )
        .await
        .map(history_result)
    }

    /// Who is in the channel. Waits while subscribing.
    ///
    /// # Errors
    ///
    /// As [`publish()`](Self::publish).
    pub async fn presence(&self) -> Result<PresenceResult, Error> {
        self.check()?;
        calls::call(
            self.client.shared(),
            Some(&self.inner),
            presence_command(&self.inner.channel),
        )
        .await
        .map(presence_result)
    }

    /// How many clients and users are in the channel. Waits while subscribing.
    ///
    /// # Errors
    ///
    /// As [`publish()`](Self::publish).
    pub async fn presence_stats(&self) -> Result<PresenceStatsResult, Error> {
        self.check()?;
        calls::call(
            self.client.shared(),
            Some(&self.inner),
            presence_stats_command(&self.inner.channel),
        )
        .await
        .map(presence_stats_result)
    }

    /// Every subscription event as a stream, with hand-off delivery. One at a
    /// time: a new call ends the previous one. Ends on `close()` and after
    /// removal.
    pub fn events(&self) -> EventStream<SubscriptionEvent> {
        let mut slot = lock(&self.inner.events_stream);
        let rx = slot.open();
        if self.removed() || lock(&self.client.shared().close_waiters).is_none() {
            slot.close();
        }
        EventStream::new(rx)
    }

    /// The publications as a stream, with hand-off delivery. One at a time; it
    /// ends on `close()` and after removal.
    pub fn publications(&self) -> EventStream<PublicationEvent> {
        let mut slot = lock(&self.inner.publications_stream);
        let rx = slot.open();
        if self.removed() || lock(&self.client.shared().close_waiters).is_none() {
            slot.close();
        }
        EventStream::new(rx)
    }
}

macro_rules! sub_handlers {
    ($($(#[$doc:meta])* $name:ident => $field:ident: $ev:ty;)*) => {
        impl Subscription {
            $(
                $(#[$doc])*
                ///
                /// One handler per kind: registering again replaces it.
                pub fn $name(&self, f: impl FnMut(&Subscription, $ev) + MaybeSend + 'static) {
                    let mut h = lock(&self.inner.handlers);
                    h.$field = Some(Box::new(f));
                    h.generation += 1;
                }
            )*
        }

        impl SubscriptionBuilder {
            $(
                $(#[$doc])*
                pub fn $name(mut self, f: impl FnMut(&Subscription, $ev) + MaybeSend + 'static) -> Self {
                    self.handlers.$field = Some(Box::new(f));
                    self
                }
            )*
        }
    };
}

sub_handlers! {
    /// The subscription started subscribing or resubscribing.
    on_subscribing => subscribing: SubscribingEvent;
    /// The subscription is subscribed.
    on_subscribed => subscribed: SubscribedEvent;
    /// The subscription is unsubscribed (and won't resubscribe by itself).
    on_unsubscribed => unsubscribed: UnsubscribedEvent;
    /// A background error: subscribe or token failures.
    on_error => error: SubscriptionErrorEvent;
    /// A publication.
    on_publication => publication: PublicationEvent;
    /// Someone joined (with `join_leave(true)`).
    on_join => join: JoinEvent;
    /// Someone left (with `join_leave(true)`).
    on_leave => leave: LeaveEvent;
}

/// Builds a [`Subscription`]; see [`Client::new_subscription`].
#[must_use = "a builder does nothing until build() is called"]
pub struct SubscriptionBuilder {
    client: Client,
    cfg: SubConfig,
    delta: Option<DeltaType>,
    callbacks: SubCallbacks,
    handlers: SubHandlers,
}

impl std::fmt::Debug for SubscriptionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionBuilder")
            .field("channel", &self.cfg.channel)
            .finish_non_exhaustive()
    }
}

impl SubscriptionBuilder {
    pub(crate) fn new(client: Client, channel: String) -> Self {
        let mut cfg = SubConfig::new(channel);
        cfg.min_resubscribe_delay = defaults::MIN_RESUBSCRIBE_DELAY;
        cfg.max_resubscribe_delay = defaults::MAX_RESUBSCRIBE_DELAY;
        Self {
            client,
            cfg,
            delta: None,
            callbacks: SubCallbacks::default(),
            handlers: SubHandlers::default(),
        }
    }

    /// The subscription token (JWT).
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.cfg.token = token.into();
        self
    }

    /// Fetches the subscription token: before the first subscribe when no
    /// token is set, and when it expires. An empty string means unauthorized.
    pub fn get_token<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(SubscriptionTokenContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<String, TokenError>> + MaybeSend + 'static,
    {
        self.callbacks.get_token =
            Some(Arc::new(move |ctx| -> SubTokenFuture { Box::pin(f(ctx)) }));
        self
    }

    /// Subscribe data, sent with every subscribe.
    pub fn data(mut self, data: impl Into<Bytes>) -> Self {
        self.cfg.data = data.into();
        self
    }

    /// Ask for a positioned subscription (offsets checked, gaps detected).
    pub fn positioned(mut self, on: bool) -> Self {
        self.cfg.positioned = on;
        self
    }

    /// Ask for a recoverable subscription (missed publications recovered on
    /// resubscribe).
    pub fn recoverable(mut self, on: bool) -> Self {
        self.cfg.recoverable = on;
        self
    }

    /// Ask for join and leave events.
    pub fn join_leave(mut self, on: bool) -> Self {
        self.cfg.join_leave = on;
        self
    }

    /// Recover from this position on the first subscribe (e.g. one saved from
    /// a previous run).
    pub fn since(mut self, pos: StreamPosition) -> Self {
        self.cfg.since = Some(pos);
        self
    }

    /// Delta compression.
    pub fn delta(mut self, delta: DeltaType) -> Self {
        self.delta = Some(delta);
        self
    }

    /// A server-side tags filter. Can't be combined with `delta`.
    pub fn tags_filter(mut self, filter: FilterNode) -> Self {
        self.cfg.tags_filter = Some(filter);
        self
    }

    /// Loads the position to recover from, from the app's own state, before
    /// each subscribe that can't recover by itself (docs/spec.md §7.5).
    pub fn get_state<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(SubscriptionGetStateContext) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<StreamPosition, BoxError>> + MaybeSend + 'static,
    {
        self.callbacks.get_state = Some(Arc::new(move |ctx| -> StateFuture { Box::pin(f(ctx)) }));
        self
    }

    /// Resubscribe backoff minimum (default 500 ms).
    pub fn min_resubscribe_delay(mut self, d: Duration) -> Self {
        self.cfg.min_resubscribe_delay = d;
        self
    }

    /// Resubscribe backoff maximum (default 20 s).
    pub fn max_resubscribe_delay(mut self, d: Duration) -> Self {
        self.cfg.max_resubscribe_delay = d;
        self
    }

    /// Registers the subscription (unsubscribed until
    /// [`Subscription::subscribe`]).
    ///
    /// # Errors
    ///
    /// [`Error::DuplicateSubscription`] when the channel is registered,
    /// [`Error::Configuration`] for invalid options, [`Error::ClientClosed`]
    /// after `close()`.
    pub fn build(mut self) -> Result<Subscription, Error> {
        let shared = self.client.shared().clone();
        if shared.state() == State::Closed {
            return Err(Error::ClientClosed);
        }
        if self.cfg.channel.is_empty() {
            return Err(Error::Configuration("channel must not be empty".into()));
        }
        duration("min_resubscribe_delay", self.cfg.min_resubscribe_delay)?;
        duration("max_resubscribe_delay", self.cfg.max_resubscribe_delay)?;
        if self.cfg.min_resubscribe_delay > self.cfg.max_resubscribe_delay {
            return Err(Error::Configuration(
                "min_resubscribe_delay is above max_resubscribe_delay".into(),
            ));
        }
        if self.delta.is_some() {
            if self.cfg.tags_filter.is_some() {
                return Err(Error::Configuration(
                    "delta can't be combined with tags_filter".into(),
                ));
            }
            if !cfg!(feature = "delta") {
                return Err(Error::Configuration(
                    "delta compression needs the `delta` feature".into(),
                ));
            }
            self.cfg.delta = true;
        }
        codec::validate_payload(shared.cfg.protocol, "data", &self.cfg.data)
            .map_err(|e| Error::InvalidData(e.to_string()))?;
        self.cfg.has_get_token = self.callbacks.get_token.is_some();
        self.cfg.has_get_state = self.callbacks.get_state.is_some();

        let mut reg = lock(&shared.registry);
        if reg.contains_key(&self.cfg.channel) {
            return Err(Error::DuplicateSubscription);
        }
        let key = shared.next_key.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(SubInner::new(
            key,
            self.cfg.channel.clone(),
            self.callbacks,
            self.cfg.delta,
        ));
        *lock(&inner.handlers) = self.handlers;
        reg.insert(self.cfg.channel.clone(), inner.clone());
        lock(&shared.by_key).insert(key, inner.clone());
        shared.send_op(Op::NewSubscription {
            key,
            config: Box::new(self.cfg),
        });
        drop(reg);
        Ok(Subscription {
            inner,
            client: self.client,
        })
    }
}
