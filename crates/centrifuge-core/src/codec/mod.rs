//! Encoding of commands and decoding of replies, for both protocols
//! (docs/spec.md §5.3–§5.4, §5.7).
//!
//! - WebSocket frames are complete: [`decode_frame`] splits one frame into its
//!   replies.
//! - HTTP-streaming is a byte stream: [`StreamDecoder`] reassembles replies split
//!   across reads anywhere, including inside a varint length prefix.

#[cfg(feature = "json")]
#[allow(clippy::struct_excessive_bools, clippy::trivially_copy_pass_by_ref)]
// serde mirrors and skip_serializing_if
mod json;

use bytes::{Buf, Bytes, BytesMut};
use prost::Message as _;

use crate::proto;

/// Wire protocol of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Protocol {
    /// Protobuf, binary frames. The default (docs/spec.md §15, decision 4).
    #[default]
    Protobuf,
    /// JSON, newline-delimited text frames. Payloads must be valid JSON.
    #[cfg(feature = "json")]
    Json,
}

/// Data that couldn't be decoded as protocol messages.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// Invalid JSON or a JSON reply of the wrong shape.
    Json(String),
    /// Invalid Protobuf.
    Protobuf(String),
    /// A message, length prefix or JSON line larger than the limit.
    TooLarge {
        /// Size the data declared or reached.
        size: usize,
        /// The configured limit.
        max: usize,
    },
    /// The stream ended in the middle of a message.
    Truncated,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "invalid JSON reply: {e}"),
            Self::Protobuf(e) => write!(f, "invalid Protobuf reply: {e}"),
            Self::TooLarge { size, max } => {
                write!(f, "message of {size} bytes exceeds the limit of {max}")
            }
            Self::Truncated => f.write_str("stream ended inside a message"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// A command that can't be encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeError {
    /// A payload isn't valid JSON, which the JSON protocol requires.
    InvalidJson {
        /// Which payload.
        field: &'static str,
    },
    /// The JSON serializer failed.
    Json(String),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidJson { field } => write!(
                f,
                "{field} is not valid JSON, which the JSON protocol requires"
            ),
            Self::Json(e) => write!(f, "JSON encoding failed: {e}"),
        }
    }
}

impl std::error::Error for EncodeError {}

/// Checks a payload for the protocol: any bytes for Protobuf, valid JSON for
/// JSON (docs/spec.md §4.6). Empty means "no payload" and is always valid.
///
/// # Errors
///
/// [`EncodeError::InvalidJson`] for invalid JSON under the JSON protocol.
pub fn validate_payload(
    protocol: Protocol,
    field: &'static str,
    data: &[u8],
) -> Result<(), EncodeError> {
    match protocol {
        Protocol::Protobuf => Ok(()),
        #[cfg(feature = "json")]
        Protocol::Json => {
            if data.is_empty() {
                return Ok(());
            }
            serde_json::from_slice::<&serde_json::value::RawValue>(data)
                .map(|_| ())
                .map_err(|_| EncodeError::InvalidJson { field })
        }
    }
}

/// Appends `commands`, encoded as one frame, to `out`: newline-joined for JSON
/// (no trailing newline), varint-length-delimited for Protobuf.
///
/// # Errors
///
/// [`EncodeError`] when a payload is invalid for the protocol. Nothing is
/// appended then.
///
/// # Panics
///
/// Never: encoding into a `Vec` can't run out of space.
pub fn encode_commands(
    protocol: Protocol,
    commands: &[proto::Command],
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let start = out.len();
    let res = (|| {
        for (i, cmd) in commands.iter().enumerate() {
            match protocol {
                Protocol::Protobuf => {
                    cmd.encode_length_delimited(out)
                        .expect("a Vec<u8> grows as needed");
                }
                #[cfg(feature = "json")]
                Protocol::Json => {
                    if i > 0 {
                        out.push(b'\n');
                    }
                    json::encode_command(cmd, out)?;
                }
            }
            #[cfg(not(feature = "json"))]
            let _ = i;
        }
        Ok(())
    })();
    if res.is_err() {
        out.truncate(start);
    }
    res
}

/// Encodes an emulation request carrying an already encoded command frame
/// (docs/spec.md §5.7). JSON carries the frame as a JSON string; Protobuf as
/// raw bytes.
///
/// # Errors
///
/// [`EncodeError`] if a JSON frame isn't UTF-8 (it always is when produced by
/// [`encode_commands`]).
pub fn encode_emulation_request(
    protocol: Protocol,
    node: &str,
    session: &str,
    frame: &[u8],
) -> Result<Vec<u8>, EncodeError> {
    match protocol {
        Protocol::Protobuf => Ok(proto::EmulationRequest {
            node: node.to_owned(),
            session: session.to_owned(),
            data: Bytes::copy_from_slice(frame),
        }
        .encode_to_vec()),
        #[cfg(feature = "json")]
        Protocol::Json => json::encode_emulation_request(node, session, frame),
    }
}

/// Decodes a complete frame (a WebSocket message) into its replies.
///
/// # Errors
///
/// [`DecodeError`] on anything that isn't a sequence of valid replies. Replies
/// before the bad one are discarded too: a frame is accepted whole or not at
/// all (docs/spec.md §5.3).
pub fn decode_frame(
    protocol: Protocol,
    frame: Bytes,
    max_message_size: usize,
) -> Result<Vec<proto::Reply>, DecodeError> {
    if frame.len() > max_message_size {
        return Err(DecodeError::TooLarge {
            size: frame.len(),
            max: max_message_size,
        });
    }
    let mut out = Vec::new();
    match protocol {
        Protocol::Protobuf => {
            let mut buf = frame;
            while buf.has_remaining() {
                let len = prost::decode_length_delimiter(&mut buf)
                    .map_err(|e| DecodeError::Protobuf(e.to_string()))?;
                if len > max_message_size {
                    return Err(DecodeError::TooLarge {
                        size: len,
                        max: max_message_size,
                    });
                }
                if len > buf.remaining() {
                    return Err(DecodeError::Truncated);
                }
                let msg = buf.split_to(len);
                out.push(
                    proto::Reply::decode(msg).map_err(|e| DecodeError::Protobuf(e.to_string()))?,
                );
            }
        }
        #[cfg(feature = "json")]
        Protocol::Json => {
            for line in frame.split(|b| *b == b'\n') {
                let line = trim_line(line);
                if line.is_empty() {
                    continue;
                }
                out.push(json::decode_reply(&frame, line)?);
            }
        }
    }
    Ok(out)
}

#[cfg(feature = "json")]
fn trim_line(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Reassembles replies from a byte stream (HTTP-streaming, docs/spec.md §5.7).
///
/// It keeps one growable buffer, scans only newly arrived data for line ends
/// (pattern 71), caps every message at `max_message_size` (pattern 72), skips
/// blank keep-alive lines, and reports a message cut off by the end of the
/// stream through [`StreamDecoder::finish`] (pattern 24).
#[derive(Debug)]
pub struct StreamDecoder {
    protocol: Protocol,
    buf: BytesMut,
    /// JSON: how far `buf` has been scanned for `\n` without finding one.
    scanned: usize,
    max: usize,
}

/// Buffers that grew past this for one large message are released afterwards.
const RETAINED_CAPACITY: usize = 64 * 1024;

impl StreamDecoder {
    /// A decoder for `protocol` with a per-message limit.
    #[must_use]
    pub fn new(protocol: Protocol, max_message_size: usize) -> Self {
        Self {
            protocol,
            buf: BytesMut::new(),
            scanned: 0,
            max: max_message_size,
        }
    }

    /// Feeds a chunk of the stream and appends every complete reply to `out`.
    ///
    /// # Errors
    ///
    /// [`DecodeError`] on undecodable or oversized data. Replies decoded from
    /// this chunk before the error are still in `out`: they were received
    /// before it and are dispatched first (docs/spec.md §5.7).
    pub fn push(&mut self, chunk: &[u8], out: &mut Vec<proto::Reply>) -> Result<(), DecodeError> {
        self.buf.extend_from_slice(chunk);
        let res = match self.protocol {
            Protocol::Protobuf => self.drain_protobuf(out),
            #[cfg(feature = "json")]
            Protocol::Json => self.drain_json(out),
        };
        if self.buf.is_empty() && self.buf.capacity() > RETAINED_CAPACITY {
            self.buf = BytesMut::new();
        }
        res
    }

    /// Call at the end of the stream.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Truncated`] if a message was left incomplete.
    pub fn finish(&self) -> Result<(), DecodeError> {
        let rest: &[u8] = &self.buf;
        let only_blank = rest.iter().all(|b| matches!(b, b'\n' | b'\r'));
        if only_blank {
            Ok(())
        } else {
            Err(DecodeError::Truncated)
        }
    }

    fn drain_protobuf(&mut self, out: &mut Vec<proto::Reply>) -> Result<(), DecodeError> {
        loop {
            let mut peek: &[u8] = &self.buf;
            let before = peek.len();
            let len = match prost::decode_length_delimiter(&mut peek) {
                Ok(len) => len,
                // Not enough bytes for the varint yet. A varint is at most 10
                // bytes; anything longer is garbage.
                Err(_) if self.buf.len() < 10 => return Ok(()),
                Err(e) => return Err(DecodeError::Protobuf(e.to_string())),
            };
            if len > self.max {
                return Err(DecodeError::TooLarge {
                    size: len,
                    max: self.max,
                });
            }
            let prefix = before - peek.len();
            if self.buf.len() < prefix + len {
                return Ok(());
            }
            self.buf.advance(prefix);
            let msg = self.buf.split_to(len).freeze();
            out.push(proto::Reply::decode(msg).map_err(|e| DecodeError::Protobuf(e.to_string()))?);
        }
    }

    #[cfg(feature = "json")]
    fn drain_json(&mut self, out: &mut Vec<proto::Reply>) -> Result<(), DecodeError> {
        loop {
            let Some(pos) = self.buf[self.scanned..].iter().position(|b| *b == b'\n') else {
                self.scanned = self.buf.len();
                if self.buf.len() > self.max {
                    return Err(DecodeError::TooLarge {
                        size: self.buf.len(),
                        max: self.max,
                    });
                }
                return Ok(());
            };
            let end = self.scanned + pos;
            if end > self.max {
                return Err(DecodeError::TooLarge {
                    size: end,
                    max: self.max,
                });
            }
            let line = self.buf.split_to(end + 1).freeze();
            self.scanned = 0;
            let body = trim_line(&line[..end]);
            if body.is_empty() {
                continue;
            }
            let body = line.slice_ref(body);
            out.push(json::decode_reply(&body, &body)?);
        }
    }
}

#[cfg(test)]
mod tests;
