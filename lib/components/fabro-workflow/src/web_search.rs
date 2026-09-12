//! The built-in `web_search` backend for workflow agents.
//!
//! Agents always call the same tool. Pebble ships the Brave and Venice
//! providers; fabro supplies the credential, read from the vault, and the
//! preference: Brave when its credential is present, else Venice when its is.
//! With neither, the agent gets no search tool.

use std::sync::Arc;

use pebble_coding_agent::extensions::SearchProvider;
use pebble_coding_agent::search::providers::{Brave, Venice};

/// Credentials for the built-in search backends, read from the vault.
#[derive(Clone, Default)]
pub struct SearchSecrets {
    pub brave_search_api_key: Option<String>,
    pub venice_api_key:       Option<String>,
}

impl std::fmt::Debug for SearchSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchSecrets")
            .field(
                "brave_search_configured",
                &self.brave_search_api_key.is_some(),
            )
            .field("venice_configured", &self.venice_api_key.is_some())
            .finish()
    }
}

/// The provider an agent's `web_search` calls, when a credential names one.
///
/// The HTTP client is fabro's, so its proxy and TLS policy apply.
#[must_use]
pub fn search_provider(secrets: &SearchSecrets) -> Option<Arc<dyn SearchProvider>> {
    let client = fabro_http::HttpClientBuilder::new().build().ok()?;
    match (
        secrets.brave_search_api_key.as_ref(),
        secrets.venice_api_key.as_ref(),
    ) {
        (Some(api_key), _) => Some(Arc::new(Brave::new(api_key.clone(), client))),
        (None, Some(api_key)) => Some(Arc::new(Venice::new(api_key.clone(), client))),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brave_is_preferred_and_neither_means_no_tool() {
        let both = SearchSecrets {
            brave_search_api_key: Some("b".into()),
            venice_api_key:       Some("v".into()),
        };
        assert!(search_provider(&both).is_some());
        let venice_only = SearchSecrets {
            brave_search_api_key: None,
            venice_api_key:       Some("v".into()),
        };
        assert!(search_provider(&venice_only).is_some());
        assert!(search_provider(&SearchSecrets::default()).is_none());
        assert_eq!(
            format!("{both:?}"),
            "SearchSecrets { brave_search_configured: true, venice_configured: true }",
            "a debug rendering never carries a key"
        );
    }
}
