//! Which secrets a provider reads.
//!
//! lithos-llm owns the convention: `openai` reads `OPENAI_API_KEY`, `gemini`
//! reads `GEMINI_API_KEY` then `GOOGLE_API_KEY`, and an operator-defined
//! provider reads a name derived from its id. Fabro stores secrets in its
//! vault under those same names, so the vault entry an operator creates and
//! the environment variable a shell exports are spelled alike.

use lithos_llm::catalog::{AuthScheme, CatalogProvider, ProviderId, builtin};
use lithos_llm::credentials::ConventionalCredentials;

use crate::OPENAI_CODEX_VAULT_SECRET_NAME;

/// The secret names `provider` reads, preferred name first.
#[must_use]
pub fn secret_names(provider: &CatalogProvider) -> Vec<String> {
    ConventionalCredentials::new().secret_names(provider)
}

/// The secret an operator creates to configure `provider`, when the provider
/// reads one.
#[must_use]
pub fn expected_secret_name(provider: &CatalogProvider) -> Option<String> {
    secret_names(provider).into_iter().next()
}

/// Whether the provider takes a single API key an operator can paste in.
///
/// Providers that read several secrets (Modal's two proxy-token headers) or
/// none at all (Ollama) do not.
#[must_use]
pub fn accepts_api_key(provider: &CatalogProvider) -> bool {
    matches!(
        provider.auth(),
        AuthScheme::Bearer { .. }
            | AuthScheme::Header { .. }
            | AuthScheme::BedrockBearer
            | AuthScheme::Aws { .. }
    ) && !secret_names(provider).is_empty()
}

/// The vault entry holding `provider`'s OAuth credential, for the providers
/// Fabro can log into with a browser flow.
pub(crate) fn oauth_secret_name(provider: &ProviderId) -> Option<&'static str> {
    (provider.as_str() == builtin::ids::OPENAI_CODEX).then_some(OPENAI_CODEX_VAULT_SECRET_NAME)
}
