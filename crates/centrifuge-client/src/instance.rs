//! The instance of a client: an id the server keeps apart per client.
//!
//! It is checked when the client is built and is never printed: logs show
//! [`Redacted`] in its place, and error texts never contain it.

use std::fmt;

use crate::error::Error;

/// Shortest instance the server accepts, in bytes (every allowed character is ASCII).
pub(crate) const MIN_LENGTH: usize = 22;

/// Longest instance the server accepts, in bytes.
pub(crate) const MAX_LENGTH: usize = 64;

/// What logs show in place of the instance.
pub(crate) const REDACTED: &str = "<redacted>";

/// Stands in for an instance wherever it would be printed.
#[derive(Clone, Copy)]
pub(crate) struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl fmt::Display for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// Checks an instance: 22 to 64 characters of `A-Z`, `a-z`, `0-9`, `_` or `-`.
///
/// # Errors
///
/// [`Error::Configuration`] when the instance is not valid. The message does
/// not contain the instance.
pub(crate) fn validate(instance: &str) -> Result<(), Error> {
    let length_is_valid: bool = (MIN_LENGTH..=MAX_LENGTH).contains(&instance.len());
    let characters_are_valid: bool = instance.bytes().all(is_instance_byte);
    if length_is_valid && characters_are_valid {
        return Ok(());
    }
    Err(Error::Configuration(
        "instance must be 22 to 64 characters of A-Z, a-z, 0-9, _ or -".into(),
    ))
}

/// One byte of an instance: `A-Z`, `a-z`, `0-9`, `_` or `-`.
fn is_instance_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
}

#[cfg(test)]
mod tests {
    use super::{MAX_LENGTH, MIN_LENGTH, Redacted, validate};
    use crate::error::Error;

    #[test]
    fn instance_accepts_lengths_at_both_bounds() {
        let shortest: String = "a".repeat(MIN_LENGTH);
        let longest: String = "Z".repeat(MAX_LENGTH);
        assert!(validate(&shortest).is_ok());
        assert!(validate(&longest).is_ok());
    }

    #[test]
    fn instance_rejects_lengths_outside_the_bounds() {
        let too_short: String = "a".repeat(MIN_LENGTH - 1);
        let too_long: String = "a".repeat(MAX_LENGTH + 1);
        assert!(matches!(validate(""), Err(Error::Configuration(_))));
        assert!(matches!(validate(&too_short), Err(Error::Configuration(_))));
        assert!(matches!(validate(&too_long), Err(Error::Configuration(_))));
    }

    #[test]
    fn instance_accepts_the_allowed_alphabet() {
        let mixed: &str = "Az09_-Az09_-Az09_-Az09_-";
        assert_eq!(mixed.len(), 24);
        assert!(validate(mixed).is_ok());
    }

    #[test]
    fn instance_rejects_other_characters_and_non_ascii() {
        for value in [
            "abcdefghijklmnopqrstu.v",
            "abcdefghijklmnopqrstu v",
            "abcdefghijklmnopqrstu/v",
            "abcdefghijklmnopqrstuvé",
        ] {
            assert!(
                matches!(validate(value), Err(Error::Configuration(_))),
                "{value} must be rejected"
            );
        }
    }

    #[test]
    fn instance_error_does_not_echo_the_instance() {
        let value: &str = "secret-instance-with-a-bad.char";
        let error: Error = validate(value).expect_err("an invalid instance must fail");
        let text: String = error.to_string();
        assert!(!text.contains(value));
    }

    #[test]
    fn instance_redacted_prints_placeholder() {
        assert_eq!(format!("{Redacted}"), "<redacted>");
        assert_eq!(format!("{Redacted:?}"), "<redacted>");
    }
}
