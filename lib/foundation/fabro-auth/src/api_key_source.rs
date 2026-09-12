//! A credential source holding one operator-supplied API key.
//!
//! Used to validate a pasted key before it is stored: the key stands in for
//! the first secret the provider conventionally reads, so lithos shapes it
//! into the provider's auth scheme exactly as a stored secret would be.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fabro_vault::Vault;
use lithos_llm::catalog::{CatalogProvider, ProviderId};
use lithos_llm::credentials::{
    ConventionalCredentials, CredentialError, CredentialProvider, Credentials,
};
use tokio::sync::RwLock as AsyncRwLock;

use crate::secrets::expected_secret_name;
use crate::vault_source::interpolated_headers;

pub struct ApiKeyCredentialSource {
    provider: ProviderId,
    key:      String,
    vault:    Arc<AsyncRwLock<Vault>>,
}

impl ApiKeyCredentialSource {
    /// A source for `provider` with no vault behind it, so header secrets the
    /// provider interpolates from the vault fail to resolve.
    #[must_use]
    pub fn new(provider: ProviderId, key: String) -> Self {
        Self::with_vault(
            provider,
            key,
            Arc::new(AsyncRwLock::new(Vault::from_entries(HashMap::new()))),
        )
    }

    /// A source for `provider` whose interpolated headers resolve against
    /// `vault`.
    #[must_use]
    pub fn with_vault(provider: ProviderId, key: String, vault: Arc<AsyncRwLock<Vault>>) -> Self {
        Self {
            provider,
            key,
            vault,
        }
    }
}

impl std::fmt::Debug for ApiKeyCredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyCredentialSource")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

/// Shapes a caller-supplied API key into the provider's credentials.
pub(crate) async fn credentials_for_api_key(
    provider: &CatalogProvider,
    key: String,
    vault: &Vault,
) -> Result<Credentials, CredentialError> {
    let Some(name) = expected_secret_name(provider) else {
        return Err(CredentialError::SchemeMismatch {
            provider: provider.id().clone(),
        });
    };
    let interpolated = interpolated_headers(vault, provider)?;
    let mut credentials = ConventionalCredentials::new()
        .with_lookup(move |candidate| (candidate == name).then(|| key.clone()))
        .credentials(provider)
        .await?;
    if let Credentials::Http(http) = &mut credentials {
        http.extra_headers.extend(interpolated);
    }
    Ok(credentials)
}

#[async_trait]
impl CredentialProvider for ApiKeyCredentialSource {
    async fn credentials(
        &self,
        provider: &CatalogProvider,
    ) -> Result<Credentials, CredentialError> {
        if provider.id() != &self.provider {
            return Err(CredentialError::NotConfigured {
                provider: provider.id().clone(),
            });
        }
        let vault = self.vault.read().await.clone();
        credentials_for_api_key(provider, self.key.clone(), &vault).await
    }

    async fn is_configured(&self, provider: &CatalogProvider) -> bool {
        provider.id() == &self.provider
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lithos_llm::credentials::{HttpAuthentication, HttpCredentials};

    use super::*;
    use crate::secrets::accepts_api_key;
    use crate::test_support::test_catalog;

    #[tokio::test]
    async fn api_key_credentials_follow_the_provider_scheme() {
        let catalog = test_catalog();
        let vault = Vault::from_entries(HashMap::new());
        let openai = credentials_for_api_key(
            catalog.provider("openai").unwrap(),
            "sk-test".to_string(),
            &vault,
        )
        .await
        .unwrap();
        assert!(matches!(
            openai,
            Credentials::Http(HttpCredentials {
                auth: HttpAuthentication::Bearer(secret),
                ..
            }) if secret.expose_secret() == "sk-test"
        ));
        let bedrock = credentials_for_api_key(
            catalog.provider("bedrock").unwrap(),
            "sk-test".to_string(),
            &vault,
        )
        .await
        .unwrap();
        assert!(matches!(bedrock, Credentials::BedrockBearer(_)));
        let modal = credentials_for_api_key(
            catalog.provider("modal").unwrap(),
            "sk-test".to_string(),
            &vault,
        )
        .await;
        assert!(modal.is_err(), "modal has no single-key scheme");
        assert!(!accepts_api_key(catalog.provider("modal").unwrap()));
        assert!(accepts_api_key(catalog.provider("openai").unwrap()));
        assert!(accepts_api_key(catalog.provider("bedrock").unwrap()));
        assert!(!accepts_api_key(catalog.provider("ollama").unwrap()));
    }
}
