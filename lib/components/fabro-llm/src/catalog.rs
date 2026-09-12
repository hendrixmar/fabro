//! Catalog construction and the agent-profile reading that is Fabro's own.
//!
//! Layer order is fixed: lithos built-ins, then the operator's `[llm]`
//! overlay. Which providers are on, which model a selector names, and which
//! model to pick for a job are lithos questions, answered by
//! [`Catalog`] and [`CatalogProvider`] (`enabled_providers`,
//! `offerings_matching`, `default_offering_for`, and the rest). What stays
//! here is the coding harness a model expects, read from the shared
//! `metadata.agent` namespace that Pebble reads too.

use fabro_config::LlmLayer;
use fabro_static::EnvVars;
use fabro_types::AgentProfileKind;
pub use lithos_llm::catalog::Offering;
use lithos_llm::catalog::{
    Catalog, CatalogBuilder, CatalogError, CatalogModel, CatalogProvider, Metadata,
};
use serde::Deserialize;

/// The metadata namespace agent harnesses read.
const AGENT_METADATA_NAMESPACE: &str = "agent";

/// Builds the effective catalog.
///
/// `env_lookup` supplies `OPENAI_BASE_URL`, the one environment override
/// Fabro honors: it repoints the `openai` provider so test doubles and
/// gateways can stand in for the real API without editing settings.
pub fn build_catalog(
    overlay: &LlmLayer,
    env_lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Catalog, CatalogError> {
    let mut builder = Catalog::builder().with_builtin();
    if !overlay.is_empty() {
        let mut document = overlay.to_overlay_toml();
        document.insert_str(0, "schema_version = 1\n");
        builder = builder.toml_layer("settings [llm]", &document)?;
    }
    if let Some(base_url) = env_lookup(EnvVars::OPENAI_BASE_URL) {
        let document = format!(
            "schema_version = 1\n[providers.openai]\nbase_url = {}\n",
            toml::Value::String(base_url.trim_end_matches("/v1").to_string())
        );
        builder = builder.toml_layer("OPENAI_BASE_URL", &document)?;
    }
    let catalog = builder.build()?;
    // The coding agent reads a provider's harness profile from
    // `metadata.agent.profile` and refuses a provider without one. The lithos
    // built-ins all declare theirs; an operator-defined provider that does
    // not gets the profile its wire protocol implies, layered on last.
    let implied = implied_agent_profiles(&catalog);
    if implied.is_empty() {
        return Ok(catalog);
    }
    let mut builder = Catalog::builder().with_builtin();
    if !overlay.is_empty() {
        let mut document = overlay.to_overlay_toml();
        document.insert_str(0, "schema_version = 1\n");
        builder = builder.toml_layer("settings [llm]", &document)?;
    }
    if let Some(base_url) = env_lookup(EnvVars::OPENAI_BASE_URL) {
        let document = format!(
            "schema_version = 1\n[providers.openai]\nbase_url = {}\n",
            toml::Value::String(base_url.trim_end_matches("/v1").to_string())
        );
        builder = builder.toml_layer("OPENAI_BASE_URL", &document)?;
    }
    builder
        .toml_layer("implied agent profiles", &implied)
        .and_then(CatalogBuilder::build)
}

/// A TOML layer declaring `metadata.agent.profile` for every provider that
/// has none, as the profile implied by the provider's adapter. Empty when
/// every provider already says.
fn implied_agent_profiles(catalog: &Catalog) -> String {
    use std::fmt::Write as _;

    let mut document = String::new();
    for provider in catalog.providers() {
        if agent_metadata(provider.metadata()).profile.is_some() {
            continue;
        }
        let profile = adapter_agent_profile(provider);
        let _ = writeln!(
            document,
            "[providers.{}.metadata.agent]\nprofile = {}\n",
            provider.id(),
            toml::Value::String(
                serde_json::to_value(profile)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "openai".to_string())
            ),
        );
    }
    if document.is_empty() {
        return document;
    }
    document.insert_str(0, "schema_version = 1\n");
    document
}

/// The profile a provider's wire protocol implies, for a provider whose
/// catalog entry does not name one.
fn adapter_agent_profile(provider: &CatalogProvider) -> AgentProfileKind {
    match provider.adapter().as_str() {
        "anthropic" | "bedrock" => AgentProfileKind::Anthropic,
        "gemini" => AgentProfileKind::Gemini,
        _ => AgentProfileKind::OpenAi,
    }
}

/// The catalog with no operator overlay: the lithos built-ins.
///
/// Used where no settings file is in play, such as the standalone hook
/// runner. Servers and the CLI build from the operator's `[llm]` overlay
/// with [`build_catalog`] instead.
#[must_use]
pub fn default_catalog() -> Catalog {
    build_catalog(&LlmLayer::default(), &|_| None).expect("the built-in catalog always builds")
}

/// The `metadata.agent` namespace on a catalog entry. Malformed metadata
/// falls back to the defaults; the lithos built-ins are validated in lithos.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct AgentMetadata {
    profile:              Option<AgentProfileKind>,
    reasoning_by_default: Option<bool>,
}

fn agent_metadata(metadata: &Metadata) -> AgentMetadata {
    metadata
        .namespace::<AgentMetadata>(AGENT_METADATA_NAMESPACE)
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// Whether requests to `offering` reason when no effort is requested.
///
/// The catalog can state it outright under `metadata.agent`. Otherwise a
/// model that supports reasoning and takes named effort levels reasons by
/// default, while one that needs an explicit thinking budget does not.
#[must_use]
pub fn reasons_by_default(offering: &Offering<'_>) -> bool {
    agent_metadata(offering.model.metadata())
        .reasoning_by_default
        .or(agent_metadata(offering.provider.metadata()).reasoning_by_default)
        .unwrap_or_else(|| {
            offering.model.capabilities().reasoning().is_supported()
                && offering.model.protocol_options().reasoning_effort_levels
        })
}

fn model_agent_profile(provider: &CatalogProvider, model: &CatalogModel) -> AgentProfileKind {
    agent_metadata(model.metadata())
        .profile
        .unwrap_or_else(|| provider_agent_profile(provider))
}

/// The agent profile a provider's models run under unless a model row says
/// otherwise: the provider's `metadata.agent.profile`, which
/// [`build_catalog`] fills in for a provider that declared none.
fn provider_agent_profile(provider: &CatalogProvider) -> AgentProfileKind {
    agent_metadata(provider.metadata())
        .profile
        .unwrap_or_else(|| adapter_agent_profile(provider))
}

/// The agent profile for a route on an enabled provider. Unknown
/// (passthrough) models take the provider default; a disabled or unknown
/// provider has none.
#[must_use]
pub fn agent_profile(
    catalog: &Catalog,
    provider_selector: &str,
    model_selector: Option<&str>,
) -> Option<AgentProfileKind> {
    let provider = catalog.enabled_provider(provider_selector)?;
    Some(
        match model_selector.and_then(|selector| provider.model(selector)) {
            Some(model) => model_agent_profile(provider, model),
            None => provider_agent_profile(provider),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_catalog;

    #[test]
    fn operator_overlay_applies_last() {
        let overlay = LlmLayer(
            toml::from_str(
                r"
[providers.openai]
priority = 500
enabled = false
",
            )
            .unwrap(),
        );
        let catalog = build_catalog(&overlay, &|_| None).unwrap();
        assert!(catalog.enabled_provider("openai").is_none());
        assert_eq!(
            catalog.provider("openai").unwrap().priority(),
            500,
            "overlay values win over the built-ins"
        );
    }

    #[test]
    fn openai_base_url_env_repoints_the_openai_provider() {
        let catalog = build_catalog(&LlmLayer::default(), &|name| {
            (name == EnvVars::OPENAI_BASE_URL).then(|| "http://127.0.0.1:1234/v1".to_string())
        })
        .unwrap();
        assert_eq!(
            catalog.provider("openai").unwrap().base_url(),
            "http://127.0.0.1:1234"
        );
    }

    #[test]
    fn agent_profiles_follow_the_model_then_the_provider() {
        let catalog = test_catalog();
        assert_eq!(
            agent_profile(&catalog, "openai", Some("gpt-5.6-sol")),
            Some(AgentProfileKind::Gpt56)
        );
        assert_eq!(
            agent_profile(&catalog, "moonshot", None),
            Some(AgentProfileKind::Kimi),
            "a passthrough model on Moonshot takes the provider's Kimi profile"
        );
        assert_eq!(
            agent_profile(&catalog, "deepseek", None),
            Some(AgentProfileKind::OpenAi)
        );
        assert_eq!(
            agent_profile(&catalog, "openrouter", None),
            None,
            "disabled providers have no profile to offer"
        );
        assert_eq!(
            agent_profile(&catalog, "moonshot", Some("kimi-k3")),
            Some(AgentProfileKind::Kimi)
        );
        assert_eq!(
            agent_profile(&catalog, "openai", Some("gpt-6-astra")),
            Some(AgentProfileKind::Gpt6)
        );
        assert_eq!(
            agent_profile(&catalog, "anthropic", Some("claude-sonnet-4.5")),
            Some(AgentProfileKind::Anthropic)
        );
    }

    #[test]
    fn reasoning_by_default_reads_agent_metadata_then_capabilities() {
        let catalog = test_catalog();
        let moonshot = catalog.enabled_provider("moonshot").unwrap();
        let kimi = moonshot.offering("kimi-k2.5").unwrap();
        assert!(reasons_by_default(&kimi), "the catalog row says so");
        let anthropic = catalog.enabled_provider("anthropic").unwrap();
        let sonnet = anthropic.offering("claude-sonnet-4.5").unwrap();
        assert!(
            !reasons_by_default(&sonnet),
            "a thinking-budget model reasons only when asked"
        );
    }

    /// An operator-defined provider with no `metadata.agent.profile` gets the
    /// one its adapter implies, so the coding agent can build on it.
    #[test]
    fn operator_providers_without_a_profile_get_the_adapter_implied_one() {
        let overlay = LlmLayer(
            toml::from_str(
                r#"
[providers.acme]
display_name = "Acme"
adapter = "openai-compatible"
codec = "openai-chat"
base_url = "https://api.acme.test/v1"
auth = { type = "bearer" }
default_model = "acme-llama"

[providers.acme.models.acme-llama]
display_name = "Acme Llama"
api_model = "acme-llama"
limits = { context_tokens = 131072, max_output_tokens = 8192 }
capabilities = { text = true, tools = true }
"#,
            )
            .unwrap(),
        );
        let catalog = build_catalog(&overlay, &|_| None).unwrap();
        let acme = catalog.provider("acme").unwrap();
        assert_eq!(
            agent_metadata(acme.metadata()).profile,
            Some(AgentProfileKind::OpenAi)
        );
        assert_eq!(
            agent_profile(&catalog, "acme", Some("acme-llama")),
            Some(AgentProfileKind::OpenAi)
        );
    }
}
