//! Fossil delta decoding (docs/spec.md §7.5), ported from `centrifugal/fdelta`.
//!
//! Like fdelta, applying is two passes: the whole command stream is validated
//! against the source and the delta first, so a malformed delta is rejected
//! without allocating, and the output is then allocated once at its exact size.
//! It never panics, whatever the delta contains.

/// A delta that can't be applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeltaError {
    /// The delta isn't well formed.
    Invalid(&'static str),
    /// The delta is well formed, but its output doesn't match the checksum it
    /// carries: it was almost certainly applied to the wrong base.
    ChecksumMismatch,
    /// The output it declares is larger than the allowed maximum.
    TooLarge {
        /// Declared output size.
        size: usize,
        /// Allowed maximum.
        max: usize,
    },
}

impl std::fmt::Display for DeltaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(why) => write!(f, "invalid delta: {why}"),
            Self::ChecksumMismatch => f.write_str("delta checksum mismatch"),
            Self::TooLarge { size, max } => {
                write!(f, "delta output of {size} bytes exceeds the limit of {max}")
            }
        }
    }
}

impl std::error::Error for DeltaError {}

const fn digit_table() -> [i8; 256] {
    const ALPHABET: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
    let mut t = [-1i8; 256];
    let mut i = 0;
    while i < 64 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        {
            t[ALPHABET[i] as usize] = i as i8;
        }
        i += 1;
    }
    t
}

static DIGITS: [i8; 256] = digit_table();

/// Reads the number at `b[i..]`, returning it and the index of the first
/// non-digit byte. An empty number is zero; overlong numbers wrap, as in
/// delta.c. Every value is bounds-checked by the caller.
fn decode_int(b: &[u8], mut i: usize) -> (u32, usize) {
    let mut v: u32 = 0;
    while i < b.len() {
        let d = DIGITS[b[i] as usize];
        if d < 0 {
            break;
        }
        #[allow(clippy::cast_sign_loss)]
        {
            v = (v << 6).wrapping_add(d as u32);
        }
        i += 1;
    }
    (v, i)
}

/// The size the delta declares for its output, from its header alone.
///
/// # Errors
///
/// [`DeltaError::Invalid`] when the header is malformed.
pub fn output_size(delta: &[u8]) -> Result<usize, DeltaError> {
    let (size, i) = decode_int(delta, 0);
    if i >= delta.len() || delta[i] != b'\n' {
        return Err(DeltaError::Invalid(
            "size integer not terminated by a newline",
        ));
    }
    Ok(size as usize)
}

struct Plan {
    size: usize,
    sum: u32,
    body: usize,
}

fn plan(src_len: usize, delta: &[u8]) -> Result<Plan, DeltaError> {
    let (limit, mut i) = decode_int(delta, 0);
    if i >= delta.len() || delta[i] != b'\n' {
        return Err(DeltaError::Invalid(
            "size integer not terminated by a newline",
        ));
    }
    i += 1;
    let body = i;
    let limit = u64::from(limit);
    let mut total: u64 = 0;
    while i < delta.len() {
        let (cnt, j) = decode_int(delta, i);
        if j >= delta.len() {
            break;
        }
        let op = delta[j];
        i = j + 1;
        let cnt64 = u64::from(cnt);
        match op {
            b'@' => {
                let (ofst, k) = decode_int(delta, i);
                if k < delta.len() && delta[k] != b',' {
                    return Err(DeltaError::Invalid(
                        "copy command not terminated by a comma",
                    ));
                }
                i = k + 1;
                total += cnt64;
                if total > limit {
                    return Err(DeltaError::Invalid("copy exceeds the declared output size"));
                }
                if u64::from(ofst) + cnt64 > src_len as u64 {
                    return Err(DeltaError::Invalid(
                        "copy extends past the end of the source",
                    ));
                }
            }
            b':' => {
                total += cnt64;
                if total > limit {
                    return Err(DeltaError::Invalid(
                        "insert exceeds the declared output size",
                    ));
                }
                if i as u64 + cnt64 > delta.len() as u64 {
                    return Err(DeltaError::Invalid(
                        "insert extends past the end of the delta",
                    ));
                }
                i += cnt as usize;
            }
            b';' => {
                if total != limit {
                    return Err(DeltaError::Invalid(
                        "output size does not match the declared size",
                    ));
                }
                #[allow(clippy::cast_possible_truncation)]
                return Ok(Plan {
                    size: limit as usize,
                    sum: cnt,
                    body,
                });
            }
            _ => return Err(DeltaError::Invalid("unknown delta operator")),
        }
    }
    Err(DeltaError::Invalid("unterminated delta"))
}

fn exec(out: &mut Vec<u8>, origin: &[u8], delta: &[u8], mut i: usize) {
    loop {
        let (cnt, j) = decode_int(delta, i);
        let op = delta[j];
        i = j + 1;
        match op {
            b'@' => {
                let (ofst, k) = decode_int(delta, i);
                i = k + 1;
                let at = ofst as usize;
                out.extend_from_slice(&origin[at..at + cnt as usize]);
            }
            b':' => {
                out.extend_from_slice(&delta[i..i + cnt as usize]);
                i += cnt as usize;
            }
            // ';', the only way `plan` succeeds.
            _ => return,
        }
    }
}

/// The input read as big-endian `u32` words, zero padded, summed modulo 2^32.
fn checksum(b: &[u8]) -> u32 {
    let mut sum: u32 = 0;
    let mut chunks = b.chunks_exact(4);
    for w in &mut chunks {
        sum = sum.wrapping_add(u32::from_be_bytes([w[0], w[1], w[2], w[3]]));
    }
    let rest = chunks.remainder();
    let mut last = [0u8; 4];
    last[..rest.len()].copy_from_slice(rest);
    sum.wrapping_add(u32::from_be_bytes(last))
}

/// Applies `delta` to `origin`, refusing outputs larger than `max_output`.
///
/// # Errors
///
/// [`DeltaError`] for a malformed delta, a checksum mismatch, or an output over
/// `max_output`. No partial output is ever returned.
pub fn apply(origin: &[u8], delta: &[u8], max_output: usize) -> Result<Vec<u8>, DeltaError> {
    let size = output_size(delta)?;
    if size > max_output {
        return Err(DeltaError::TooLarge {
            size,
            max: max_output,
        });
    }
    let p = plan(origin.len(), delta)?;
    let mut out = Vec::with_capacity(p.size);
    exec(&mut out, origin, delta, p.body);
    if checksum(&out) != p.sum {
        return Err(DeltaError::ChecksumMismatch);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vectors() -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/fossil-vectors");
        (1..=5)
            .map(|n| {
                let d = dir.join(n.to_string());
                let read = |f: &str| std::fs::read(d.join(f)).expect("vector file");
                (read("origin"), read("target"), read("delta"))
            })
            .collect()
    }

    #[test]
    fn fossil_reference_vectors_apply() {
        for (i, (origin, target, delta)) in vectors().into_iter().enumerate() {
            let out = apply(&origin, &delta, usize::MAX)
                .unwrap_or_else(|e| panic!("vector {}: {e}", i + 1));
            assert_eq!(out, target, "vector {}", i + 1);
        }
    }

    #[test]
    fn rejects_wrong_base_by_checksum() {
        let (origin, _, delta) = vectors().remove(0);
        let mut wrong = origin.clone();
        wrong[0] ^= 0x01;
        assert_eq!(
            apply(&wrong, &delta, usize::MAX),
            Err(DeltaError::ChecksumMismatch)
        );
    }

    #[test]
    fn enforces_output_limit() {
        let (origin, target, delta) = vectors().remove(0);
        assert!(matches!(
            apply(&origin, &delta, target.len() - 1),
            Err(DeltaError::TooLarge { .. })
        ));
    }

    #[test]
    fn malformed_deltas_are_errors() {
        for bad in [
            &b""[..],
            b"5",
            b"5\n",
            b"3\n3:abc",
            b"3\n4:abc;",
            b"3\n9@0,;",
            b"3\n3?abc;",
            b"2\n3:abc0;",
        ] {
            assert!(
                matches!(apply(b"xyz", bad, usize::MAX), Err(DeltaError::Invalid(_))),
                "{bad:?}"
            );
        }
    }

    proptest::proptest! {
        #[test]
        fn never_panics(origin in proptest::collection::vec(proptest::num::u8::ANY, 0..64),
                        delta in proptest::collection::vec(proptest::num::u8::ANY, 0..64)) {
            let _ = apply(&origin, &delta, 1 << 20);
        }
    }
}
