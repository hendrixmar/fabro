use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lithos_llm::catalog::CatalogProvider;
use lithos_llm::credentials::{
    CredentialError, CredentialHeader, CredentialProvider, Credentials, SecretValue,
};

/// Decorates another [`CredentialProvider`] by appending fixed extra headers
/// to every HTTP credential it resolves.
///
/// Headers already present on a credential (for example from explicit
/// provider configuration) are left untouched. AWS-signed credentials carry
/// no header list and pass through unchanged.
pub struct ExtraHeadersCredentialSource {
    inner:   Arc<dyn CredentialProvider>,
    headers: HashMap<String, String>,
}

impl ExtraHeadersCredentialSource {
    #[must_use]
    pub fn new(inner: Arc<dyn CredentialProvider>, headers: HashMap<String, String>) -> Self {
        Self { inner, headers }
    }
}

#[async_trait]
impl CredentialProvider for ExtraHeadersCredentialSource {
    async fn credentials(
        &self,
        provider: &CatalogProvider,
    ) -> Result<Credentials, CredentialError> {
        let mut credentials = self.inner.credentials(provider).await?;
        if let Credentials::Http(http) = &mut credentials {
            for (name, value) in &self.headers {
                if http
                    .extra_headers
                    .iter()
                    .any(|existing| existing.name.eq_ignore_ascii_case(name))
                {
                    continue;
                }
                http.extra_headers.push(CredentialHeader::new(
                    name.clone(),
                    SecretValue::new(value.clone()),
                ));
            }
        }
        Ok(credentials)
    }

    async fn is_configured(&self, provider: &CatalogProvider) -> bool {
        self.inner.is_configured(provider).await
    }
}

#[cfg(test)]
mod tests {
    use lithos_llm::credentials::HttpAuthentication;

    use super::*;
    use crate::test_support::test_catalog;

    struct StubSource {
        configured:      bool,
        existing_header: Option<(String, String)>,
    }

    #[async_trait]
    impl CredentialProvider for StubSource {
        async fn credentials(
            &self,
            provider: &CatalogProvider,
        ) -> Result<Credentials, CredentialError> {
            if provider.id().as_str() == "bedrock" {
                return Ok(Credentials::AwsDefaultChain { region: None });
            }
            let mut credentials = Credentials::bearer(SecretValue::new("key"));
            if let (Credentials::Http(http), Some((name, value))) =
                (&mut credentials, &self.existing_header)
            {
                http.extra_headers.push(CredentialHeader::new(
                    name.clone(),
                    SecretValue::new(value.clone()),
                ));
            }
            Ok(credentials)
        }

        async fn is_configured(&self, _provider: &CatalogProvider) -> bool {
            self.configured
        }
    }

    fn header<'a>(credentials: &'a Credentials, name: &str) -> Option<&'a str> {
        match credentials {
            Credentials::Http(http) => http
                .extra_headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case(name))
                .map(|header| header.value.expose_secret()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn appends_headers_to_http_credentials_only() {
        let catalog = test_catalog();
        let source = ExtraHeadersCredentialSource::new(
            Arc::new(StubSource {
                configured:      false,
                existing_header: None,
            }),
            HashMap::from([("x-session-id".to_string(), "run-123".to_string())]),
        );
        let openai = source
            .credentials(catalog.provider("openai").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            &openai,
            Credentials::Http(http) if matches!(http.auth, HttpAuthentication::Bearer(_))
        ));
        assert_eq!(header(&openai, "x-session-id"), Some("run-123"));
        let bedrock = source
            .credentials(catalog.provider("bedrock").unwrap())
            .await
            .unwrap();
        assert!(matches!(bedrock, Credentials::AwsDefaultChain { .. }));
    }

    #[tokio::test]
    async fn preserves_case_insensitive_headers_already_set() {
        let catalog = test_catalog();
        let source = ExtraHeadersCredentialSource::new(
            Arc::new(StubSource {
                configured:      true,
                existing_header: Some(("X-Session-Id".to_string(), "configured".to_string())),
            }),
            HashMap::from([("x-session-id".to_string(), "run-123".to_string())]),
        );
        let credentials = source
            .credentials(catalog.provider("openai").unwrap())
            .await
            .unwrap();
        assert_eq!(header(&credentials, "x-session-id"), Some("configured"));
        assert!(
            source
                .is_configured(catalog.provider("openai").unwrap())
                .await
        );
    }
}
