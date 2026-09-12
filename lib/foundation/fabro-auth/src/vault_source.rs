//! Credentials from Fabro's vault, with the process environment as a second
//! store when the caller allows it.
//!
//! lithos-llm knows which named secrets each provider reads and how they shape
//! into the provider's authentication scheme. This source supplies the store:
//! a name is looked up in the environment first, then in the vault, under the
//! same conventional spelling (`OPENAI_API_KEY`, `MODAL_TOKEN_ID`). On top of
//! that lithos table, Fabro adds what only it knows about:
//!
//! - an OAuth credential in the vault (the Codex login), refreshed when it
//!   expires and written back;
//! - `{{ secrets.NAME }}` tokens in a provider's `default_headers`, resolved
//!   against the vault and re-sent as credential headers so the literal token
//!   never reaches the wire;
//! - OpenAI organization and project headers from the environment.

use std::error::Error as StdError;
use std::sync::Arc;

use async_trait::async_trait;
use fabro_static::EnvVars;
use fabro_types::settings::{InterpString, ResolveCtx};
use fabro_vault::{SecretType, Vault};
use lithos_llm::catalog::{CatalogProvider, ProviderId, builtin};
use lithos_llm::credentials::{
    ConventionalCredentials, CredentialError, CredentialHeader, CredentialProvider, Credentials,
    HttpAuthentication, HttpCredentials, SecretValue,
};
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task::spawn_blocking;

use crate::credential::OAuthCredential;
use crate::refresh::refresh_oauth_credential;
use crate::secrets::oauth_secret_name;
use crate::vault_ext::{VaultLookupError, vault_get_oauth, vault_set_oauth, vault_token_lookup};

pub type EnvLookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

const CHATGPT_ACCOUNT_ID_HEADER: &str = "ChatGPT-Account-Id";
const OPENAI_ORGANIZATION_HEADER: &str = "OpenAI-Organization";
const OPENAI_PROJECT_HEADER: &str = "OpenAI-Project";

/// Credentials backed by an in-memory [`Vault`] plus an environment lookup.
#[derive(Clone)]
pub struct VaultCredentialSource {
    vault:      Arc<AsyncRwLock<Vault>>,
    env_lookup: EnvLookup,
}

impl VaultCredentialSource {
    /// A source over `vault` that falls back to the process environment.
    #[must_use]
    #[expect(
        clippy::disallowed_methods,
        reason = "VaultCredentialSource::new owns the process-env fallback used after vault lookup."
    )]
    pub fn new(vault: Arc<AsyncRwLock<Vault>>) -> Self {
        Self::with_env_lookup(vault, |name| std::env::var(name).ok())
    }

    #[must_use]
    pub fn with_env_lookup<F>(vault: Arc<AsyncRwLock<Vault>>, env_lookup: F) -> Self
    where
        F: Fn(&str) -> Option<String> + Send + Sync + 'static,
    {
        Self {
            vault,
            env_lookup: Arc::new(env_lookup),
        }
    }

    /// A source that reads the vault and nothing else.
    #[must_use]
    pub fn vault_only(vault: Arc<AsyncRwLock<Vault>>) -> Self {
        Self::with_env_lookup(vault, |_| None)
    }

    /// A source over an empty vault, so every secret comes from the process
    /// environment. For SDK callers and tools that have no Fabro vault.
    #[must_use]
    pub fn environment_only() -> Self {
        Self::new(Arc::new(AsyncRwLock::new(Vault::from_entries(
            std::collections::HashMap::new(),
        ))))
    }

    pub(crate) async fn snapshot(&self) -> Vault {
        self.vault.read().await.clone()
    }

    /// The lithos conventional table reading from the environment, then the
    /// vault.
    fn conventional(&self, vault: &Vault) -> ConventionalCredentials {
        let vault = vault.clone();
        let env_lookup = Arc::clone(&self.env_lookup);
        ConventionalCredentials::new()
            .with_lookup(move |name| env_lookup(name).or_else(|| vault_token_lookup(&vault, name)))
    }

    /// The vault's OAuth credential for `provider`, refreshed and persisted
    /// when it has expired. `None` when the provider has no OAuth path or the
    /// vault holds nothing under its name.
    async fn oauth_credentials(
        &self,
        provider: &CatalogProvider,
        vault: &Vault,
    ) -> Result<Option<Credentials>, CredentialError> {
        let Some(name) = oauth_secret_name(provider.id()) else {
            return Ok(None);
        };
        let Some(entry) = vault.get_entry(name) else {
            return Ok(None);
        };
        if entry.secret_type == SecretType::Token {
            // A pasted API key stored under the OAuth name still works.
            return Ok(Some(Credentials::bearer(SecretValue::new(
                entry.value.clone(),
            ))));
        }
        let credential = vault_get_oauth(vault, name)
            .map_err(|err| vault_lookup_error(provider.id(), name, err))?
            .expect("entry is present");
        let credential = if credential.needs_refresh() {
            if credential.tokens.refresh_token.is_none() {
                return Err(unusable(
                    provider.id(),
                    "requires re-authentication: refresh token missing",
                    None,
                ));
            }
            let refreshed = refresh_oauth_credential(&credential)
                .await
                .map_err(|source| {
                    unusable(
                        provider.id(),
                        format!("requires re-authentication: {source}"),
                        Some(source.into()),
                    )
                })?;
            self.persist_oauth(provider.id(), name, &refreshed).await?;
            refreshed
        } else {
            credential
        };
        Ok(Some(oauth_bearer(&credential)))
    }

    async fn persist_oauth(
        &self,
        provider: &ProviderId,
        name: &str,
        refreshed: &OAuthCredential,
    ) -> Result<(), CredentialError> {
        let refreshed = refreshed.clone();
        let name = name.to_string();
        let vault = Arc::clone(&self.vault);
        let failed = |source: anyhow::Error| {
            unusable(
                provider,
                format!("the refreshed token could not be stored: {source}"),
                Some(source.into()),
            )
        };
        spawn_blocking(move || {
            let mut vault = vault.blocking_write();
            vault_set_oauth(&mut vault, &name, &refreshed)
                .map(|_| ())
                .map_err(anyhow::Error::from)
        })
        .await
        .map_err(|join_err| failed(anyhow::Error::from(join_err)))?
        .map_err(failed)
    }

    /// Headers Fabro adds on top of what lithos shaped: interpolated
    /// `default_headers` and, for OpenAI, the organization and project ids
    /// from the environment.
    fn decorate(
        &self,
        provider: &CatalogProvider,
        mut credentials: Credentials,
        interpolated: Vec<CredentialHeader>,
    ) -> Credentials {
        if let Credentials::Http(http) = &mut credentials {
            http.extra_headers.extend(interpolated);
            if provider.id().as_str() == builtin::ids::OPENAI {
                for (variable, header) in [
                    (EnvVars::OPENAI_ORG_ID, OPENAI_ORGANIZATION_HEADER),
                    (EnvVars::OPENAI_PROJECT_ID, OPENAI_PROJECT_HEADER),
                ] {
                    if let Some(value) = (self.env_lookup)(variable) {
                        http.extra_headers
                            .push(CredentialHeader::new(header, SecretValue::new(value)));
                    }
                }
            }
        }
        credentials
    }

    async fn has_credential_material(&self, vault: &Vault, provider: &CatalogProvider) -> bool {
        oauth_secret_name(provider.id()).is_some_and(|name| vault.get_entry(name).is_some())
            || self.conventional(vault).credentials(provider).await.is_ok()
    }
}

impl std::fmt::Debug for VaultCredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultCredentialSource")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for VaultCredentialSource {
    async fn credentials(
        &self,
        provider: &CatalogProvider,
    ) -> Result<Credentials, CredentialError> {
        let vault = self.snapshot().await;
        let interpolated = interpolated_headers(&vault, provider)?;
        if let Some(oauth) = self.oauth_credentials(provider, &vault).await? {
            return Ok(self.decorate(provider, oauth, interpolated));
        }
        let credentials = self.conventional(&vault).credentials(provider).await?;
        Ok(self.decorate(provider, credentials, interpolated))
    }

    /// Presence without refresh: an OAuth entry under the provider's name, or
    /// a conventional secret that resolves.
    async fn is_configured(&self, provider: &CatalogProvider) -> bool {
        let vault = self.snapshot().await;
        self.has_credential_material(&vault, provider).await
    }
}

/// Shapes a Codex OAuth credential into the bearer the deployment expects.
fn oauth_bearer(credential: &OAuthCredential) -> Credentials {
    let mut http = HttpCredentials::new(HttpAuthentication::Bearer(SecretValue::new(
        credential.tokens.access_token.clone(),
    )));
    if let Some(account_id) = &credential.account_id {
        http.extra_headers.push(CredentialHeader::new(
            CHATGPT_ACCOUNT_ID_HEADER,
            SecretValue::new(account_id.clone()),
        ));
    }
    Credentials::Http(http)
}

/// The provider's `default_headers` whose values reference `{{ secrets.* }}`,
/// resolved against the vault.
///
/// lithos sends `default_headers` verbatim and lets credential headers of the
/// same name win, so only the interpolated ones are re-sent here. Resolved
/// values may contain secrets; keep this path free of value logging.
pub(crate) fn interpolated_headers(
    vault: &Vault,
    provider: &CatalogProvider,
) -> Result<Vec<CredentialHeader>, CredentialError> {
    let mut ctx =
        ResolveCtx::new().with_secrets(|secret_name| vault_token_lookup(vault, secret_name));
    provider
        .default_headers()
        .iter()
        .map(|(name, source)| (name, InterpString::parse(source)))
        .filter(|(_, template)| !template.is_literal())
        .map(|(name, template)| {
            let value = template.resolve_with(&mut ctx).map_err(|source| {
                unusable(
                    provider.id(),
                    format!("header interpolation failed: {source}"),
                    Some(Box::new(source)),
                )
            })?;
            Ok(CredentialHeader::new(name.clone(), SecretValue::new(value)))
        })
        .collect()
}

/// Material is present but cannot be used. `reason` is the operator-facing
/// line and must not carry secret content.
pub(crate) fn unusable(
    provider: &ProviderId,
    reason: impl Into<String>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
) -> CredentialError {
    CredentialError::Unusable {
        provider: provider.clone(),
        reason: reason.into(),
        source,
    }
}

fn vault_lookup_error(provider: &ProviderId, name: &str, err: VaultLookupError) -> CredentialError {
    let reason = match &err {
        VaultLookupError::SchemaMismatch { actual, .. } => {
            format!("vault credential '{name}' has schema {actual:?}, expected Token or Oauth")
        }
        VaultLookupError::DecodeFailed { .. } => {
            format!("vault credential '{name}' is not valid OAuth JSON")
        }
    };
    unusable(provider, reason, Some(Box::new(err)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, Utc};
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use lithos_llm::catalog::Catalog;
    use lithos_llm::credentials::readiness;

    use super::*;
    use crate::OPENAI_CODEX_VAULT_SECRET_NAME;
    use crate::credential::{OAuthConfig, OAuthTokens};
    use crate::test_support::{test_catalog, test_catalog_with_overlay};
    use crate::vault_ext::vault_set_token;

    fn oauth_credential(token_url: String, expires_at: chrono::DateTime<Utc>) -> OAuthCredential {
        OAuthCredential {
            tokens:     OAuthTokens {
                access_token: "expired-access".to_string(),
                refresh_token: Some("refresh-token".to_string()),
                expires_at,
            },
            config:     OAuthConfig {
                auth_url: "https://auth.openai.com".to_string(),
                token_url,
                client_id: "test-client".to_string(),
                scopes: vec!["openid".to_string()],
                redirect_uri: Some("https://auth.openai.com/deviceauth/callback".to_string()),
                use_pkce: true,
            },
            account_id: Some("acct_123".to_string()),
        }
    }

    fn empty_vault() -> Vault {
        Vault::from_entries(HashMap::new())
    }

    fn source_with(
        vault: Vault,
        env: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> VaultCredentialSource {
        VaultCredentialSource::with_env_lookup(Arc::new(AsyncRwLock::new(vault)), env)
    }

    fn bearer_secret(credentials: &Credentials) -> &str {
        match credentials {
            Credentials::Http(HttpCredentials {
                auth: HttpAuthentication::Bearer(secret),
                ..
            }) => secret.expose_secret(),
            _ => panic!("expected bearer credentials"),
        }
    }

    /// A header the credentials carry, whether as the primary `auth` header
    /// or as an extra header.
    fn header_value<'a>(credentials: &'a Credentials, name: &str) -> Option<&'a str> {
        let Credentials::Http(http) = credentials else {
            return None;
        };
        let primary = match &http.auth {
            HttpAuthentication::Header(header) => Some(header),
            _ => None,
        };
        primary
            .into_iter()
            .chain(http.extra_headers.iter())
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.expose_secret())
    }

    /// An operator-defined gateway whose header secret lives in the vault.
    fn gateway_catalog() -> Catalog {
        test_catalog_with_overlay(
            r#"
[providers.gateway]
display_name = "Gateway"
adapter = "openai-compatible"
codec = "openai-chat"
base_url = "https://gateway.test/v1"
auth = { type = "bearer" }
default_headers = { "x-portkey-api-key" = "{{ secrets.PORTKEY_API_KEY }}", "x-portkey-config" = "@prod" }

[providers.gateway.models.large]
display_name = "Large"
api_model = "large"
"#,
        )
    }

    #[tokio::test]
    async fn environment_wins_over_the_vault() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, "OPENAI_API_KEY", "vault-key").unwrap();
        let source = source_with(vault, |name| {
            (name == "OPENAI_API_KEY").then(|| "env-key".to_string())
        });
        let catalog = test_catalog();
        let credentials = source
            .credentials(catalog.provider("openai").unwrap())
            .await
            .unwrap();
        assert_eq!(bearer_secret(&credentials), "env-key");
    }

    #[tokio::test]
    async fn conventional_fallback_names_apply_to_the_vault() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, EnvVars::KIMI_API_KEY, "kimi-key").unwrap();
        let source = source_with(vault, |_| None);
        let catalog = test_catalog();
        let credentials = source
            .credentials(catalog.provider("moonshot").unwrap())
            .await
            .unwrap();
        assert_eq!(bearer_secret(&credentials), "kimi-key");
    }

    #[tokio::test]
    async fn anthropic_uses_its_header_scheme() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, "ANTHROPIC_API_KEY", "anthropic-key").unwrap();
        let source = source_with(vault, |_| None);
        let catalog = test_catalog();
        let credentials = source
            .credentials(catalog.provider("anthropic").unwrap())
            .await
            .unwrap();
        match credentials {
            Credentials::Http(HttpCredentials {
                auth: HttpAuthentication::Header(header),
                ..
            }) => {
                assert_eq!(header.name, "x-api-key");
                assert_eq!(header.value.expose_secret(), "anthropic-key");
            }
            _ => panic!("expected header credentials"),
        }
    }

    #[tokio::test]
    async fn an_unlisted_provider_reads_its_derived_secret_name() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, "GATEWAY_API_KEY", "gw-key").unwrap();
        vault_set_token(&mut vault, "PORTKEY_API_KEY", "pk-key").unwrap();
        let source = source_with(vault, |_| None);
        let catalog = gateway_catalog();
        let credentials = source
            .credentials(catalog.provider("gateway").unwrap())
            .await
            .unwrap();
        assert_eq!(bearer_secret(&credentials), "gw-key");
        assert_eq!(
            header_value(&credentials, "x-portkey-api-key"),
            Some("pk-key")
        );
        assert_eq!(
            header_value(&credentials, "x-portkey-config"),
            None,
            "literal default headers are lithos's to send"
        );
    }

    #[tokio::test]
    async fn a_missing_header_secret_is_an_interpolation_issue() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, "GATEWAY_API_KEY", "gw-key").unwrap();
        let source = source_with(vault, |_| None);
        let catalog = gateway_catalog();
        let err = source
            .credentials(catalog.provider("gateway").unwrap())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CredentialError::Unusable { reason, .. } if reason.contains("header interpolation failed")),
            "{err}"
        );
        assert!(!err.to_string().contains("gw-key"));
    }

    #[tokio::test]
    async fn codex_oauth_becomes_a_bearer_with_account_header() {
        let mut vault = empty_vault();
        vault_set_oauth(
            &mut vault,
            OPENAI_CODEX_VAULT_SECRET_NAME,
            &oauth_credential(
                "https://auth.openai.com/oauth/token".to_string(),
                Utc::now() + Duration::hours(1),
            ),
        )
        .unwrap();
        let source = source_with(vault, |_| None);
        let catalog = test_catalog();
        let credentials = source
            .credentials(catalog.provider("openai-codex").unwrap())
            .await
            .unwrap();
        assert_eq!(bearer_secret(&credentials), "expired-access");
        assert_eq!(
            header_value(&credentials, CHATGPT_ACCOUNT_ID_HEADER),
            Some("acct_123")
        );
    }

    #[tokio::test]
    async fn openai_api_key_attaches_org_and_project_from_env() {
        let source = source_with(empty_vault(), |name| match name {
            "OPENAI_API_KEY" => Some("key".to_string()),
            "OPENAI_ORG_ID" => Some("org".to_string()),
            "OPENAI_PROJECT_ID" => Some("proj".to_string()),
            _ => None,
        });
        let catalog = test_catalog();
        let credentials = source
            .credentials(catalog.provider("openai").unwrap())
            .await
            .unwrap();
        assert_eq!(
            header_value(&credentials, "OpenAI-Organization"),
            Some("org")
        );
        assert_eq!(header_value(&credentials, "OpenAI-Project"), Some("proj"));
    }

    #[tokio::test]
    async fn bedrock_takes_a_bearer_and_falls_back_to_the_aws_default_chain() {
        let catalog = test_catalog();
        let source = source_with(empty_vault(), |_| None);
        let credentials = source
            .credentials(catalog.provider("bedrock").unwrap())
            .await
            .unwrap();
        assert!(matches!(credentials, Credentials::AwsDefaultChain { .. }));

        let source = source_with(empty_vault(), |name| {
            (name == "BEDROCK_API_KEY").then(|| "bearer".to_string())
        });
        let credentials = source
            .credentials(catalog.provider("bedrock").unwrap())
            .await
            .unwrap();
        assert!(matches!(credentials, Credentials::BedrockBearer(_)));
    }

    #[tokio::test]
    async fn missing_provider_material_is_not_configured() {
        let source = source_with(empty_vault(), |_| None);
        let catalog = test_catalog();
        let err = source
            .credentials(catalog.provider("anthropic").unwrap())
            .await
            .unwrap_err();
        assert!(err.is_not_configured(), "{err}");
        assert_eq!(err.provider().as_str(), "anthropic");
    }

    #[tokio::test]
    async fn modal_needs_both_proxy_tokens() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, "MODAL_TOKEN_ID", "wk-test").unwrap();
        let source = source_with(vault, |_| None);
        let catalog = test_catalog();
        let modal = catalog.provider("modal").unwrap();
        assert!(
            source
                .credentials(modal)
                .await
                .unwrap_err()
                .is_not_configured()
        );

        let mut vault = empty_vault();
        vault_set_token(&mut vault, "MODAL_TOKEN_ID", "wk-test").unwrap();
        vault_set_token(&mut vault, "MODAL_TOKEN_SECRET", "ws-test").unwrap();
        let source = VaultCredentialSource::vault_only(Arc::new(AsyncRwLock::new(vault)));
        let credentials = source.credentials(modal).await.unwrap();
        assert_eq!(header_value(&credentials, "Modal-Key"), Some("wk-test"));
        assert_eq!(header_value(&credentials, "Modal-Secret"), Some("ws-test"));
    }

    async fn configured(source: &VaultCredentialSource, catalog: &Catalog) -> Vec<ProviderId> {
        let mut ids = Vec::new();
        for provider in catalog.enabled_providers() {
            if source.is_configured(provider).await {
                ids.push(provider.id().clone());
            }
        }
        ids
    }

    #[tokio::test]
    async fn configured_providers_reads_vault_and_env_without_refreshing() {
        let mut vault = empty_vault();
        vault_set_token(&mut vault, "OPENAI_API_KEY", "vault-key").unwrap();
        vault_set_oauth(
            &mut vault,
            OPENAI_CODEX_VAULT_SECRET_NAME,
            &oauth_credential(
                "http://127.0.0.1:9/oauth/token".to_string(),
                Utc::now() - Duration::hours(1),
            ),
        )
        .unwrap();
        let source = source_with(vault, |name| {
            (name == "ANTHROPIC_API_KEY").then(|| "env".to_string())
        });
        let catalog = test_catalog();
        let configured = configured(&source, &catalog).await;
        assert!(configured.contains(&ProviderId::new("openai")));
        assert!(configured.contains(&ProviderId::new("anthropic")));
        assert!(configured.contains(&ProviderId::new("openai-codex")));
        // Bedrock always resolves through the AWS chain but ships disabled.
        assert!(!configured.contains(&ProviderId::new("bedrock")));
        assert!(!configured.contains(&ProviderId::new("gemini")));
    }

    #[tokio::test]
    async fn resolve_all_separates_ready_providers_from_auth_issues() {
        let mut vault = empty_vault();
        vault_set_oauth(
            &mut vault,
            OPENAI_CODEX_VAULT_SECRET_NAME,
            &oauth_credential(
                "http://127.0.0.1:9/oauth/token".to_string(),
                Utc::now() - Duration::hours(1),
            ),
        )
        .unwrap();
        vault_set_token(&mut vault, "ANTHROPIC_API_KEY", "anthropic-key").unwrap();
        let source = source_with(vault, |_| None);
        let catalog = test_catalog();
        let resolved = readiness(catalog.enabled_providers(), &source).await;
        assert_eq!(resolved.ready, vec![ProviderId::new("anthropic")]);
        assert_eq!(resolved.issues.len(), 1);
        let (provider, issue) = &resolved.issues[0];
        assert_eq!(provider.as_str(), "openai-codex");
        assert!(
            issue.to_string().contains("requires re-authentication"),
            "{issue}"
        );
    }

    #[tokio::test]
    async fn vault_only_ignores_the_environment() {
        let catalog = test_catalog();
        let vault_only =
            VaultCredentialSource::vault_only(Arc::new(AsyncRwLock::new(empty_vault())));
        assert!(configured(&vault_only, &catalog).await.is_empty());
        let resolved = readiness(catalog.enabled_providers(), &vault_only).await;
        assert!(resolved.ready.is_empty());
        assert!(resolved.issues.is_empty());
    }

    #[tokio::test]
    async fn refreshes_expired_oauth_credentials_and_persists_them() {
        let server = MockServer::start_async().await;
        let refresh_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/oauth/token")
                    .form_urlencoded_tuple("grant_type", "refresh_token")
                    .form_urlencoded_tuple("client_id", "test-client")
                    .form_urlencoded_tuple("refresh_token", "refresh-token");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(
                        serde_json::json!({
                            "access_token": "new-access",
                            "refresh_token": "new-refresh",
                            "expires_in": 3600
                        })
                        .to_string(),
                    );
            })
            .await;

        let mut vault = empty_vault();
        vault_set_oauth(
            &mut vault,
            OPENAI_CODEX_VAULT_SECRET_NAME,
            &oauth_credential(
                server.url("/oauth/token"),
                Utc::now() - Duration::minutes(1),
            ),
        )
        .unwrap();
        let vault = Arc::new(AsyncRwLock::new(vault));
        let source = VaultCredentialSource::vault_only(Arc::clone(&vault));
        let catalog = test_catalog();

        let credentials = source
            .credentials(catalog.provider("openai-codex").unwrap())
            .await
            .unwrap();
        assert_eq!(bearer_secret(&credentials), "new-access");

        let stored = {
            let vault = vault.read().await;
            vault_get_oauth(&vault, OPENAI_CODEX_VAULT_SECRET_NAME)
                .unwrap()
                .unwrap()
        };
        assert_eq!(stored.tokens.access_token, "new-access");
        assert_eq!(stored.tokens.refresh_token.as_deref(), Some("new-refresh"));
        assert_eq!(stored.account_id.as_deref(), Some("acct_123"));
        refresh_mock.assert_async().await;
    }

    #[tokio::test]
    async fn expired_oauth_without_refresh_token_requires_reauthentication() {
        let mut vault = empty_vault();
        let mut credential = oauth_credential(
            "https://auth.openai.com/oauth/token".to_string(),
            Utc::now() - Duration::minutes(1),
        );
        credential.tokens.refresh_token = None;
        vault_set_oauth(&mut vault, OPENAI_CODEX_VAULT_SECRET_NAME, &credential).unwrap();
        let source = source_with(vault, |_| None);
        let catalog = test_catalog();
        let err = source
            .credentials(catalog.provider("openai-codex").unwrap())
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "credentials for provider openai-codex cannot be used: requires re-authentication: refresh token missing"
        );
    }
}
