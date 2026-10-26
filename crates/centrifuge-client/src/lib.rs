//! Real-time client SDK for [Centrifugo](https://centrifugal.dev) and the
//! [Centrifuge](https://github.com/centrifugal/centrifuge) library.
//!
//! **This crate is under development.** The API is specified in
//! [`docs/spec.md`](https://github.com/centrifugal/centrifuge-rust/blob/main/docs/spec.md)
//! and is not usable yet.
//!
//! ```
//! assert!(!centrifuge::VERSION.is_empty());
//! ```

mod maybe_send;

pub use maybe_send::{MaybeSend, MaybeSync};

/// Version of this crate, as published on crates.io.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn released_in_lockstep_with_core() {
        assert_eq!(VERSION, centrifuge_core::VERSION);
    }
}
