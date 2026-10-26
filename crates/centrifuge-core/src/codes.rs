//! Protocol codes (docs/spec.md §6.5, §10).

/// `connecting` codes set by the client.
pub mod connecting {
    /// `connect()` was called.
    pub const CONNECT_CALLED: u32 = 0;
    /// The transport closed.
    pub const TRANSPORT_CLOSED: u32 = 1;
    /// No ping from the server in time.
    pub const NO_PING: u32 = 2;
    /// A subscribe reply didn't arrive in time.
    pub const SUBSCRIBE_TIMEOUT: u32 = 3;
    /// An unsubscribe failed, so the server state is unknown.
    pub const UNSUBSCRIBE_ERROR: u32 = 4;
}

/// `disconnected` codes set by the client.
pub mod disconnected {
    /// `disconnect()` or `close()` was called.
    pub const DISCONNECT_CALLED: u32 = 0;
    /// The token getter refused, or no token could be obtained.
    pub const UNAUTHORIZED: u32 = 1;
    /// Undecodable data, a broken delta chain, a malformed reply, or a handler
    /// panic. The reason names which.
    pub const BAD_PROTOCOL: u32 = 2;
    /// An incoming message exceeded `max_message_size`.
    pub const MESSAGE_SIZE_LIMIT: u32 = 3;
}

/// `subscribing` codes set by the client.
pub mod subscribing {
    /// `subscribe()` was called.
    pub const SUBSCRIBE_CALLED: u32 = 0;
    /// The connection was lost.
    pub const TRANSPORT_CLOSED: u32 = 1;
}

/// `unsubscribed` codes set by the client.
pub mod unsubscribed {
    /// `unsubscribe()` was called.
    pub const UNSUBSCRIBE_CALLED: u32 = 0;
    /// The token getter refused.
    pub const UNAUTHORIZED: u32 = 1;
    /// The client was closed.
    pub const CLIENT_CLOSED: u32 = 2;
}

/// Server error: the token expired.
pub const TOKEN_EXPIRED: u32 = 109;
/// Server error: recovery from the given position is impossible.
pub const UNRECOVERABLE_POSITION: u32 = 112;
/// Server unsubscribe code: the subscription's cached state is invalid.
pub const UNSUBSCRIBE_STATE_INVALIDATED: u32 = 2502;
/// Server disconnect code: the connection's cached state is invalid.
pub const DISCONNECT_STATE_INVALIDATED: u32 = 3014;
/// Server disconnect code: the connection expired.
pub const DISCONNECT_EXPIRED: u32 = 3005;

/// Whether a server disconnect code asks the client to reconnect.
#[must_use]
pub fn is_reconnect_code(code: u32) -> bool {
    !(3500..5000).contains(&code) || (4000..4500).contains(&code)
}

#[cfg(test)]
mod tests {
    use super::is_reconnect_code;

    #[test]
    fn reconnect_rule() {
        for c in [3000, 3001, 3014, 3499, 4000, 4499, 5000] {
            assert!(is_reconnect_code(c), "{c}");
        }
        for c in [3500, 3501, 3999, 4500, 4999] {
            assert!(!is_reconnect_code(c), "{c}");
        }
    }
}
