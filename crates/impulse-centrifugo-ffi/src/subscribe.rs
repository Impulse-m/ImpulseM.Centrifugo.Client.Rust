//! Client-side subscriptions over the C ABI: subscribe with the positioning and
//! recovery options, the events of each subscription and its token.

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use centrifuge::{
    Bytes, DeltaType, PublicationEvent, StreamPosition, SubscribedEvent, SubscribingEvent,
    Subscription, SubscriptionBuilder, SubscriptionErrorEvent, SubscriptionTokenContext,
    UnsubscribedEvent,
};

use crate::events::{
    EventFields, EventSink, ImpulseCentrifugoEvent, ImpulseCentrifugoEventKind, event_of,
    server_code_of_source, subscribed_flags, subscription_error_code,
};
use crate::rpc::status_of;
use crate::token_cb::TokenBridge;
use crate::{ImpulseCentrifugoClient, ImpulseCentrifugoStatus, copy_optional, guarded, status};

/// One subscription request of `impulse_centrifugo_subscribe`. Everything is copied
/// before the call returns. The subscription is registered under its channel and
/// starts subscribing at once; a second request for the same channel fails.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ImpulseCentrifugoSubscribeRequest {
    /// The channel, NUL-terminated. Required.
    pub channel: *const c_char,
    /// The subscribe data; may be null when `data_len` is zero.
    pub data: *const u8,
    /// Length of `data` in bytes.
    pub data_len: usize,
    /// Non-zero asks for a positioned subscription: offsets are checked and gaps
    /// are detected.
    pub positioned: u8,
    /// Non-zero asks for a recoverable subscription: missed publications are
    /// recovered on resubscribe.
    pub recoverable: u8,
    /// Non-zero asks for fossil delta compression of publications.
    pub delta_fossil: u8,
    /// Non-zero takes the subscription token from the client's token callback, with
    /// the channel as the request's channel. Zero subscribes without a token.
    pub token_callback: u8,
    /// The stream epoch to recover from on the first subscribe, with `since_offset`
    /// as its offset; null when there is none.
    pub since_epoch: *const c_char,
    /// Offset of `since_epoch`.
    pub since_offset: u64,
    /// Callback of the subscription events, or null for none.
    pub on_event:
        Option<unsafe extern "C" fn(user: *mut c_void, event: *const ImpulseCentrifugoEvent)>,
    /// Passed back to `on_event`.
    pub user: *mut c_void,
}

/// A subscribe request copied out of the caller's memory.
struct CopiedSubscribe {
    channel: String,
    data: Bytes,
    since: Option<StreamPosition>,
}

/// Copies the channel, data and recovery position of a subscribe request.
///
/// # Safety
///
/// Every pointer in `request` must be valid for the call, as the request's
/// documentation states.
unsafe fn copy_subscribe(
    request: &ImpulseCentrifugoSubscribeRequest,
) -> Result<CopiedSubscribe, ImpulseCentrifugoStatus> {
    // SAFETY: the caller guarantees a NUL-terminated channel.
    let channel: String = unsafe { copy_optional(request.channel) }?
        .ok_or(ImpulseCentrifugoStatus::InvalidArgument)?;
    // SAFETY: the caller guarantees `data_len` readable bytes.
    let data: Bytes = unsafe { crate::rpc::copy_bytes(request.data, request.data_len) }?;
    // SAFETY: the caller guarantees a NUL-terminated epoch when the pointer is set.
    let since: Option<StreamPosition> = unsafe { copy_optional(request.since_epoch) }?
        .map(|epoch: String| StreamPosition::new(request.since_offset, epoch));
    Ok(CopiedSubscribe {
        channel,
        data,
        since,
    })
}

/// Adds the event handlers of a subscription, each delivering to `sink`.
fn with_event_handlers(builder: SubscriptionBuilder, sink: EventSink) -> SubscriptionBuilder {
    builder
        .on_subscribing(
            move |subscription: &Subscription, event: SubscribingEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Subscribing,
                    code: event.code,
                    text: &event.reason,
                    channel: subscription.channel(),
                    data: &[],
                    offset: 0,
                    server_code: 0,
                }));
            },
        )
        .on_subscribed(move |subscription: &Subscription, event: SubscribedEvent| {
            let (offset, epoch): (u64, &str) = match &event.stream_position {
                Some(position) => (position.offset, position.epoch.as_str()),
                None => (0, ""),
            };
            sink.emit(&event_of(&EventFields {
                kind: ImpulseCentrifugoEventKind::Subscribed,
                code: subscribed_flags(&event),
                text: epoch,
                channel: subscription.channel(),
                data: &[],
                offset,
                server_code: 0,
            }));
        })
        .on_unsubscribed(
            move |subscription: &Subscription, event: UnsubscribedEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Unsubscribed,
                    code: event.code,
                    text: &event.reason,
                    channel: subscription.channel(),
                    data: &[],
                    offset: 0,
                    server_code: 0,
                }));
            },
        )
        .on_error(
            move |subscription: &Subscription, event: SubscriptionErrorEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::SubscriptionError,
                    code: subscription_error_code(&event.error.kind),
                    text: &event.error.message,
                    channel: subscription.channel(),
                    data: &[],
                    offset: 0,
                    server_code: server_code_of_source(event.error.source.as_ref()),
                }));
            },
        )
        .on_publication(
            move |subscription: &Subscription, event: PublicationEvent| {
                sink.emit(&event_of(&EventFields {
                    kind: ImpulseCentrifugoEventKind::Publication,
                    code: 0,
                    text: "",
                    channel: subscription.channel(),
                    data: &event.publication.data,
                    offset: event.publication.offset,
                    server_code: 0,
                }));
            },
        )
}

/// Registers a subscription on the client and starts it. Its events go to
/// `on_event`, and its token to the client's token callback when `token_callback`
/// is set.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`.
/// `request` must be readable, with the pointers its documentation names valid
/// for the call. `on_event`'s `user` must stay valid until the subscription is
/// removed with `impulse_centrifugo_unsubscribe` or the client is destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_subscribe(
    client: *const ImpulseCentrifugoClient,
    request: *const ImpulseCentrifugoSubscribeRequest,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: a non-null request is readable by the caller's contract.
        let Some(request): Option<&ImpulseCentrifugoSubscribeRequest> =
            (unsafe { request.as_ref() })
        else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: the pointers of the request are valid for the call.
        let copied: CopiedSubscribe = match unsafe { copy_subscribe(request) } {
            Ok(copied) => copied,
            Err(error) => return status(error),
        };

        let mut builder: SubscriptionBuilder = handle
            .client
            .new_subscription(copied.channel)
            .positioned(request.positioned != 0)
            .recoverable(request.recoverable != 0)
            .data(copied.data);
        if request.delta_fossil != 0 {
            builder = builder.delta(DeltaType::Fossil);
        }
        if let Some(position) = copied.since {
            builder = builder.since(position);
        }
        if request.token_callback != 0 {
            let bridge: Arc<TokenBridge> = Arc::clone(&handle.tokens);
            builder = builder.get_token(move |context: SubscriptionTokenContext| {
                let bridge: Arc<TokenBridge> = Arc::clone(&bridge);
                async move { bridge.subscription_token(&context.channel).await }
            });
        }
        if let Some(callback) = request.on_event {
            builder = with_event_handlers(builder, EventSink::new(callback, request.user));
        }

        let subscription: Subscription = match builder.build() {
            Ok(subscription) => subscription,
            Err(error) => return status_of(&error),
        };
        subscription.subscribe();
        status(ImpulseCentrifugoStatus::Ok)
    })
}

/// Unsubscribes the subscription on `channel` and removes it from the client. Its
/// event callback runs no more after the removal. An unknown channel does nothing.
///
/// # Safety
///
/// `client` must be a live handle from `impulse_centrifugo_client_create`, and
/// `channel` must be a NUL-terminated string valid for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn impulse_centrifugo_unsubscribe(
    client: *const ImpulseCentrifugoClient,
    channel: *const c_char,
) -> i32 {
    guarded(|| {
        // SAFETY: a non-null handle is live by the caller's contract.
        let Some(handle): Option<&ImpulseCentrifugoClient> = (unsafe { client.as_ref() }) else {
            return status(ImpulseCentrifugoStatus::InvalidArgument);
        };
        // SAFETY: the caller guarantees a NUL-terminated channel.
        let name: String = match unsafe { copy_optional(channel) } {
            Ok(Some(name)) => name,
            Ok(None) => return status(ImpulseCentrifugoStatus::InvalidArgument),
            Err(error) => return status(error),
        };
        if let Some(subscription) = handle.client.get_subscription(&name) {
            if let Err(error) = handle.client.remove_subscription(&subscription) {
                return status_of(&error);
            }
        }
        status(ImpulseCentrifugoStatus::Ok)
    })
}
