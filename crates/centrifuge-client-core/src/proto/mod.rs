//! Protocol types generated from `proto/client.proto` (`cargo xtask proto`).
//!
//! These are the wire model shared by both codecs. `bytes` fields are
//! [`bytes::Bytes`], so decoded payloads are slices of the received frame.

#[allow(
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    clippy::all,
    clippy::pedantic,
    rustdoc::broken_intra_doc_links,
    rustdoc::invalid_html_tags,
    rustdoc::bare_urls
)]
mod generated;

pub use generated::*;
