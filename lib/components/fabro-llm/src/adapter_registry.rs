//! Adapter factory registry keyed by [`fabro_model::AdapterKind`].
//!
//! Every adapter kind ships with a matching factory in this module. Tests in
//! this file enforce that the registry covers every adapter kind.
//!
//! Factories take a pre-built [`AdapterConfig`] derived from resolved
//! credentials + provider settings, and produce a boxed
//! [`ProviderAdapter`] ready to register with the [`crate::Client`].
use std::collections::HashMap;
use std::sync::Arc;

use fabro_auth::ApiKeyHeader;
use fabro_model::{
    AdapterKind, AgentProfileKind, BillingPolicy, Catalog, CodecKind, Model, ProviderId,
};

use crate::error::Error;
use crate::provider::ProviderAdapter;
use crate::providers;

/// Configuration passed to an adapter factory. All values are pre-resolved
/// from settings + credentials; factories never touch the environment or the
/// vault directly.
#[derive(Debug, Clone)]
pub struct AdapterConfig {
    /// Provider ID this adapter will register under (used as the registry
    /// name on the resulting adapter).
    pub provider_id:   String,
    /// Authentication header constructed by `fabro-auth` from the provider's
    /// catalog auth policy and resolved credential.
    pub auth_header:   Option<ApiKeyHeader>,
    /// Provider base URL. Native adapters can use their direct-constructor
    /// defaults when this is `None`; OpenAI-compatible providers require it.
    pub base_url:      Option<String>,
    /// Extra HTTP headers attached to every outgoing request.
    pub extra_headers: HashMap<String, String>,
    /// Adapter-kind-specific options; factories for other kinds ignore
    /// options that are not theirs.
    pub kind_options:  AdapterKindOptions,
    pub catalog:       Option<Arc<Catalog>>,
}

/// Construction options that only apply to one adapter kind, kept out of the
/// shared [`AdapterConfig`] fields.
#[derive(Debug, Clone, Default)]
pub enum AdapterKindOptions {
    /// No kind-specific options.
    #[default]
    None,
    OpenAi(OpenAiAdapterOptions),
}

/// OpenAI-only construction options.
#[derive(Debug, Clone, Default)]
pub struct OpenAiAdapterOptions {
    /// Route through the ChatGPT Codex backend.
    pub codex_mode: bool,
    /// Organization ID.
    pub org_id:     Option<String>,
    /// Project ID.
    pub project_id: Option<String>,
}

impl AdapterConfig {
    /// Construct a minimal config with just provider ID and auth header.
    pub fn new(provider_id: impl Into<String>, auth_header: ApiKeyHeader) -> Self {
        Self {
            provider_id:   provider_id.into(),
            auth_header:   Some(auth_header),
            base_url:      None,
            extra_headers: HashMap::new(),
            kind_options:  AdapterKindOptions::None,
            catalog:       None,
        }
    }
}

/// Factory function signature. Takes a fully-resolved [`AdapterConfig`] and
/// returns a registered-ready [`ProviderAdapter`].
///
/// Adapter constructors validate provider-specific construction requirements
/// before a provider is registered with the client.
pub type AdapterFactory = fn(AdapterConfig) -> Result<Arc<dyn ProviderAdapter>, Error>;

fn apply_primary_auth_header(
    auth_header: Option<ApiKeyHeader>,
    extra_headers: &mut HashMap<String, String>,
) -> Option<String> {
    match auth_header {
        Some(ApiKeyHeader::Bearer(value)) => Some(value),
        Some(ApiKeyHeader::Custom { name, value }) => {
            extra_headers.insert(name, value);
            None
        }
        // SigV4 is not a static header; only the Bedrock adapter consumes
        // the marker (it signs at request time).
        Some(ApiKeyHeader::AwsSigv4) | None => None,
    }
}

fn build_anthropic_adapter(mut config: AdapterConfig) -> providers::AnthropicAdapter {
    let api_key = apply_primary_auth_header(config.auth_header.take(), &mut config.extra_headers);
    let mut adapter = providers::AnthropicAdapter::new_optional_auth(api_key)
        .with_name(config.provider_id.clone());
    if let Some(base_url) = config.base_url {
        adapter = adapter.with_base_url(base_url);
    }
    if !config.extra_headers.is_empty() {
        adapter = adapter.with_default_headers(config.extra_headers);
    }
    if let Some(catalog) = config.catalog {
        adapter = adapter.with_catalog(catalog);
    }
    adapter
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "Adapter factories share a fallible signature; openai_compatible validates base_url."
)]
fn build_anthropic(config: AdapterConfig) -> Result<Arc<dyn ProviderAdapter>, Error> {
    Ok(Arc::new(build_anthropic_adapter(config)))
}

fn build_openai_adapter(mut config: AdapterConfig) -> providers::OpenAiAdapter {
    let api_key = apply_primary_auth_header(config.auth_header.take(), &mut config.extra_headers);
    let options = match config.kind_options {
        AdapterKindOptions::OpenAi(options) => options,
        AdapterKindOptions::None => OpenAiAdapterOptions::default(),
    };
    let mut adapter =
        providers::OpenAiAdapter::new_optional_auth(api_key).with_name(config.provider_id.clone());
    if let Some(base_url) = config.base_url {
        adapter = adapter.with_base_url(base_url);
    }
    if !config.extra_headers.is_empty() {
        adapter = adapter.with_default_headers(config.extra_headers);
    }
    if options.codex_mode {
        adapter = adapter.with_codex_mode();
    }
    if let Some(org_id) = options.org_id {
        adapter = adapter.with_org_id(org_id);
    }
    if let Some(project_id) = options.project_id {
        adapter = adapter.with_project_id(project_id);
    }
    if let Some(catalog) = config.catalog {
        adapter = adapter.with_catalog(catalog);
    }
    adapter
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "Adapter factories share a fallible signature; openai_compatible validates base_url."
)]
fn build_openai(config: AdapterConfig) -> Result<Arc<dyn ProviderAdapter>, Error> {
    Ok(Arc::new(build_openai_adapter(config)))
}

fn build_gemini_adapter(mut config: AdapterConfig) -> providers::GeminiAdapter {
    let api_key = apply_primary_auth_header(config.auth_header.take(), &mut config.extra_headers);
    let mut adapter =
        providers::GeminiAdapter::new_optional_auth(api_key).with_name(config.provider_id.clone());
    if let Some(base_url) = config.base_url {
        adapter = adapter.with_base_url(base_url);
    }
    if !config.extra_headers.is_empty() {
        adapter = adapter.with_default_headers(config.extra_headers);
    }
    if let Some(catalog) = config.catalog {
        adapter = adapter.with_catalog(catalog);
    }
    adapter
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "Adapter factories share a fallible signature; openai_compatible validates base_url."
)]
fn build_gemini(config: AdapterConfig) -> Result<Arc<dyn ProviderAdapter>, Error> {
    Ok(Arc::new(build_gemini_adapter(config)))
}

fn build_openai_compatible_adapter(
    mut config: AdapterConfig,
) -> Result<providers::OpenAiCompatibleAdapter, Error> {
    let base_url = config.base_url.ok_or_else(|| Error::Configuration {
        message: format!(
            "provider '{}' uses openai_compatible adapter but does not configure base_url",
            config.provider_id
        ),
        source:  None,
    })?;
    let api_key = apply_primary_auth_header(config.auth_header.take(), &mut config.extra_headers);
    let mut adapter = providers::OpenAiCompatibleAdapter::new_optional_auth(api_key, base_url)
        .with_name(config.provider_id);
    if !config.extra_headers.is_empty() {
        adapter = adapter.with_default_headers(config.extra_headers);
    }
    if let Some(catalog) = config.catalog {
        adapter = adapter.with_catalog(catalog);
    }
    Ok(adapter)
}

fn build_openai_compatible(config: AdapterConfig) -> Result<Arc<dyn ProviderAdapter>, Error> {
    Ok(Arc::new(build_openai_compatible_adapter(config)?))
}

/// Return the factory for a known adapter kind.
#[must_use]
pub fn factory_for(adapter_kind: AdapterKind) -> AdapterFactory {
    match adapter_kind {
        AdapterKind::Anthropic => build_anthropic,
        AdapterKind::OpenAi => build_openai,
        AdapterKind::Gemini => build_gemini,
        AdapterKind::OpenAiCompatible => build_openai_compatible,
        AdapterKind::Bedrock => providers::bedrock::build,
    }
}

/// A resolved route for one catalog model: the transport+auth key, wire
/// dialect, and provider-facing identifiers a request for that model travels
/// with.
///
/// `(provider row, model row)` → route. Codec/transport pairings are
/// validated at catalog build, so any model in a successfully built catalog
/// resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// Canonical provider this route belongs to.
    pub provider:       ProviderId,
    /// Transport + auth scheme (the adapter registry key).
    pub transport:      AdapterKind,
    /// Wire dialect spoken on this route.
    pub codec:          CodecKind,
    /// Identifier sent to the provider API (the catalog `api_id`).
    pub deployment_id:  String,
    /// Billing family used to translate usage into billed tokens.
    pub billing_policy: BillingPolicy,
    /// Agent profile driving profile-specific behavior.
    pub agent_profile:  AgentProfileKind,
}

/// Resolve the route for one already-selected catalog offering.
#[must_use]
pub fn resolve_route(catalog: &Catalog, model: &Model) -> Option<Route> {
    let provider = catalog.provider(&model.provider)?;
    let settings = catalog.settings_for(model)?;
    Some(Route {
        provider:       provider.id.clone(),
        transport:      provider.adapter,
        codec:          settings.codec,
        deployment_id:  settings.api_id.clone(),
        billing_policy: settings.billing_policy,
        agent_profile:  settings.agent_profile,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select_from_all<'a>(catalog: &'a Catalog, selector: &str) -> &'a Model {
        catalog
            .select(selector, None, &catalog.all_provider_ids())
            .unwrap_or_else(|error| panic!("built-in model '{selector}' should resolve: {error}"))
    }

    #[test]
    fn every_builtin_catalog_offering_resolves() {
        let catalog = Catalog::builtin();

        for model in catalog.list(None) {
            let route = resolve_route(catalog, model).unwrap_or_else(|| {
                panic!(
                    "built-in offering '{}/{}' should resolve",
                    model.provider, model.id
                )
            });
            assert_eq!(route.provider, model.provider);
            assert!(!route.deployment_id.is_empty());
        }
    }

    #[test]
    fn resolve_route_follows_model_aliases() {
        let catalog = Catalog::builtin();

        let by_alias = resolve_route(catalog, select_from_all(catalog, "sonnet"))
            .expect("alias should resolve");
        let by_id = resolve_route(catalog, select_from_all(catalog, "claude-sonnet-5"))
            .expect("id should resolve");

        assert_eq!(by_alias, by_id);
        assert_eq!(by_alias.provider, ProviderId::anthropic());
    }

    #[test]
    fn resolve_route_resolves_by_id_for_model_from_another_catalog_instance() {
        let other = Catalog::from_builtin().unwrap();
        let model = select_from_all(&other, "gpt-5.4");
        assert!(resolve_route(Catalog::builtin(), model).is_some());
    }

    #[test]
    fn anthropic_factory_builds_anthropic_adapter() {
        let config = AdapterConfig::new("anthropic", ApiKeyHeader::Custom {
            name:  "x-api-key".to_string(),
            value: "test-key".to_string(),
        });
        let adapter = factory_for(AdapterKind::Anthropic)(config).unwrap();
        assert_eq!(adapter.name(), "anthropic");
    }

    #[test]
    fn custom_primary_auth_header_is_preserved() {
        let config = AdapterConfig::new("anthropic", ApiKeyHeader::Custom {
            name:  "x-api-key".to_string(),
            value: "test-key".to_string(),
        });

        let adapter = build_anthropic_adapter(config);

        assert!(adapter.http.api_key.is_none());
        assert_eq!(
            adapter.http.default_headers.get("x-api-key"),
            Some(&"test-key".to_string())
        );
    }

    #[test]
    fn custom_primary_auth_header_overrides_extra_header() {
        let config = AdapterConfig {
            base_url: Some("https://api.custom.test/v1".to_string()),
            extra_headers: HashMap::from([("x-api-key".to_string(), "secondary-key".to_string())]),
            ..AdapterConfig::new("custom", ApiKeyHeader::Custom {
                name:  "x-api-key".to_string(),
                value: "primary-key".to_string(),
            })
        };

        let adapter = build_openai_compatible_adapter(config).unwrap();

        assert!(adapter.http.api_key.is_none());
        assert_eq!(
            adapter.http.default_headers.get("x-api-key"),
            Some(&"primary-key".to_string())
        );
    }

    #[test]
    fn openai_compatible_factory_uses_provider_id_for_name() {
        let config = AdapterConfig {
            base_url: Some("https://api.moonshot.ai/v1".to_string()),
            ..AdapterConfig::new("moonshot", ApiKeyHeader::Bearer("k".to_string()))
        };
        let adapter = factory_for(AdapterKind::OpenAiCompatible)(config).unwrap();
        assert_eq!(adapter.name(), "moonshot");
    }

    #[test]
    fn openai_compatible_factory_preserves_extra_headers() {
        let config = AdapterConfig {
            base_url: Some("https://api.portkey.ai/v1".to_string()),
            extra_headers: HashMap::from([
                (
                    "x-portkey-api-key".to_string(),
                    "resolved-portkey-key".to_string(),
                ),
                (
                    "x-portkey-provider".to_string(),
                    "@bedrock-prod".to_string(),
                ),
            ]),
            ..AdapterConfig::new(
                "portkey",
                ApiKeyHeader::Bearer("unused-primary-key".to_string()),
            )
        };

        let adapter = build_openai_compatible_adapter(config).unwrap();

        assert_eq!(adapter.name(), "portkey");
        assert_eq!(
            adapter.http.default_headers.get("x-portkey-api-key"),
            Some(&"resolved-portkey-key".to_string()),
        );
        assert_eq!(
            adapter.http.default_headers.get("x-portkey-provider"),
            Some(&"@bedrock-prod".to_string()),
        );
    }

    #[test]
    fn anthropic_factory_preserves_extra_headers() {
        let config = AdapterConfig {
            base_url: Some("https://api.portkey.ai/v1".to_string()),
            extra_headers: HashMap::from([(
                "x-portkey-api-key".to_string(),
                "resolved-portkey-key".to_string(),
            )]),
            ..AdapterConfig::new("anthropic-through-portkey", ApiKeyHeader::Custom {
                name:  "x-api-key".to_string(),
                value: "unused-primary-key".to_string(),
            })
        };

        let adapter = build_anthropic_adapter(config);

        assert_eq!(adapter.name(), "anthropic-through-portkey");
        assert_eq!(
            adapter.http.default_headers.get("x-portkey-api-key"),
            Some(&"resolved-portkey-key".to_string()),
        );
    }

    #[test]
    fn openai_compatible_factory_errors_without_base_url() {
        let config = AdapterConfig::new("moonshot", ApiKeyHeader::Bearer("k".to_string()));
        let Err(err) = factory_for(AdapterKind::OpenAiCompatible)(config) else {
            panic!("expected missing base_url error");
        };
        assert!(
            err.to_string()
                .contains("uses openai_compatible adapter but does not configure base_url")
        );
    }
}
