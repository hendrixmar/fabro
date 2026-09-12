//! Fabro's secret scanner on the text seams pebble exposes.

use std::borrow::Cow;

use pebble_coding_agent::extensions::Redactor;

/// Fabro's secret scanner as pebble's [`Redactor`].
///
/// Pebble calls it where text a process or the operating system wrote leaves
/// a session: the output tail a shell tool puts on the event stream and the
/// model-facing message of a failed tool call. It runs the same
/// `fabro_redact::redact_string` pass the run's stored events go through, so
/// what the model reads back matches what the log keeps. The final pass over
/// every stored `RunEvent` stays in place: this one covers the text pebble
/// hands the model and does not replace redaction of the stored event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SecretRedactor;

impl Redactor for SecretRedactor {
    fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
        let redacted = fabro_redact::redact_string(text);
        if redacted == text {
            Cow::Borrowed(text)
        } else {
            Cow::Owned(redacted)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_secret_redactor_borrows_clean_text_and_masks_secrets() {
        let redactor = SecretRedactor;
        assert!(matches!(
            redactor.redact("plain stderr"),
            Cow::Borrowed("plain stderr")
        ));
        let redacted = redactor.redact("key=AKIAYRWQG5EJLPZLBYNP");
        assert!(matches!(redacted, Cow::Owned(_)));
        assert_eq!(redacted, "key=REDACTED");
    }
}
