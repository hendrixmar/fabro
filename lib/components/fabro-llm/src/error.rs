//! The one failure-classification rule that is Fabro's own.
//!
//! Retry, auth, cancellation, and failover questions are answered by the
//! lithos `Error` and `ErrorData` themselves. What stays here is the loop and
//! restart detector's signature format, which names Fabro's own categories.

use lithos_llm::catalog::ProviderId;
use lithos_llm::types::{ErrorData, ErrorKind};

/// A stable `category|provider|detail` string for loop and restart detection.
///
/// The category is `api_canceled` for a cancelled call, `api_transient` for a
/// failure the provider may be asked to repeat, and `api_deterministic` for
/// everything else; the detail is the error kind's stored spelling.
#[must_use]
pub fn failure_signature_hint(error: &ErrorData) -> String {
    let provider = error.provider().map_or("unknown", ProviderId::as_str);
    let category = if error.is_cancelled() {
        "api_canceled"
    } else if error.is_retryable() {
        "api_transient"
    } else {
        "api_deterministic"
    };
    let kind: ErrorKind = error.kind();
    format!("{category}|{provider}|{}", kind.as_str())
}

#[cfg(test)]
mod tests {
    use lithos_llm::types::{Error, RetryClassification};

    use super::*;

    fn error(kind: ErrorKind) -> ErrorData {
        Error::new(kind, "boom")
            .with_provider(ProviderId::new("openai"))
            .data()
    }

    #[test]
    fn signatures_name_category_provider_and_kind() {
        assert_eq!(
            failure_signature_hint(&error(ErrorKind::InvalidRequest)),
            "api_deterministic|openai|invalid_request"
        );
        assert_eq!(
            failure_signature_hint(
                &Error::new(ErrorKind::RateLimit, "boom")
                    .with_provider(ProviderId::new("openai"))
                    .with_retry(RetryClassification::Safe)
                    .data()
            ),
            "api_transient|openai|rate_limit"
        );
        assert_eq!(
            failure_signature_hint(&error(ErrorKind::Cancelled)),
            "api_canceled|openai|cancelled"
        );
    }
}
