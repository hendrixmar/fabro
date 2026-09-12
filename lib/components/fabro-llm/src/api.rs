//! API projections of the catalog for `GET /models` and `GET /providers`.
//!
//! Every row is a lithos catalog entry stamped with whether the caller holds
//! credential material for the provider.

use std::collections::HashSet;

use fabro_types::{Model, ModelControls, ModelCosts, ModelFeatures, ModelLimits, Provider};
use lithos_llm::catalog::{Catalog, CatalogProvider, Offering, ProviderId};
use lithos_llm::types::ReasoningEffort;

const USD_MICROS_PER_USD: f64 = 1_000_000.0;

/// Every enabled model on every listed provider, provider priority order.
#[must_use]
pub fn models(catalog: &Catalog, configured: &HashSet<ProviderId>) -> Vec<Model> {
    catalog
        .listed_providers()
        .into_iter()
        .flat_map(CatalogProvider::offerings)
        .map(|offering| model_view(&offering, configured.contains(offering.provider.id())))
        .collect()
}

/// Every listed provider, priority order.
#[must_use]
pub fn providers(catalog: &Catalog, configured: &HashSet<ProviderId>) -> Vec<Provider> {
    catalog
        .listed_providers()
        .into_iter()
        .map(|provider| provider_view(provider, configured.contains(provider.id())))
        .collect()
}

fn model_view(entry: &Offering<'_>, configured: bool) -> Model {
    let model = entry.model;
    let capabilities = model.capabilities();
    let pricing = model.pricing();
    let limits = model.limits();
    Model {
        id: model.id().clone(),
        provider: entry.provider.id().clone(),
        family: model
            .family()
            .map_or_else(|| model.id().to_string(), str::to_string),
        display_name: model.display_name().to_string(),
        limits: ModelLimits {
            context_window: limits.map_or(0, |limits| saturating_i64(limits.context_tokens)),
            max_output:     limits
                .map(|limits| limits.max_output_tokens)
                .filter(|tokens| *tokens > 0)
                .map(saturating_i64),
        },
        training: model.training_cutoff().map(str::to_string),
        knowledge_cutoff: model.knowledge_cutoff().map(str::to_string),
        features: ModelFeatures {
            tools:        capabilities.tools().is_supported(),
            vision:       capabilities.images().is_supported(),
            reasoning:    capabilities.reasoning().is_supported(),
            prompt_cache: capabilities.caching().is_supported(),
            sampling:     capabilities.sampling().is_supported(),
        },
        controls: ModelControls {
            reasoning_effort: ReasoningEffort::ALL
                .into_iter()
                .filter(|effort| capabilities.reasoning_effort(*effort).is_supported())
                .collect(),
        },
        costs: ModelCosts {
            input_cost_per_mtok:       pricing
                .and_then(|pricing| pricing.input_usd_micros_per_million)
                .map(usd_per_million),
            output_cost_per_mtok:      pricing
                .and_then(|pricing| pricing.output_usd_micros_per_million)
                .map(usd_per_million),
            cache_input_cost_per_mtok: pricing
                .and_then(|pricing| pricing.cached_input_usd_micros_per_million)
                .map(usd_per_million),
        },
        estimated_output_tps: model.estimated_output_tps(),
        aliases: model.aliases().to_vec(),
        default: entry.provider.default_model() == Some(model.id().as_str()),
        small_default: model.is_small_default(),
        configured,
    }
}

fn provider_view(provider: &CatalogProvider, configured: bool) -> Provider {
    Provider {
        id: provider.id().clone(),
        display_name: provider.display_name().to_string(),
        adapter: provider.adapter().as_str().to_string(),
        base_url: provider.base_url().to_string(),
        api_key_url: provider.api_key_url().map(str::to_string),
        priority: provider.priority(),
        aliases: provider.aliases().to_vec(),
        model_count: u32::try_from(provider.offerings().len()).unwrap_or(u32::MAX),
        default_model: provider.default_model().map(str::to_string),
        configured,
        expected_secret_name: fabro_auth::expected_secret_name(provider),
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Catalog prices are display values; micros fit f64 exactly at these magnitudes."
)]
fn usd_per_million(micros: u64) -> f64 {
    micros as f64 / USD_MICROS_PER_USD
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use lithos_llm::catalog::builtin;

    use super::*;
    use crate::test_support::test_catalog;

    #[test]
    fn models_are_stamped_with_configured_providers() {
        let catalog = test_catalog();
        let configured = HashSet::from([builtin::openai()]);
        let models = models(&catalog, &configured);
        let openai = models
            .iter()
            .find(|model| model.provider == builtin::openai())
            .expect("openai models listed");
        assert!(openai.configured);
        assert!(openai.limits.context_window > 0);
        let anthropic = models
            .iter()
            .find(|model| model.provider == builtin::anthropic())
            .expect("anthropic models listed");
        assert!(!anthropic.configured);
        assert!(models.iter().any(|model| model.default));
    }

    #[test]
    fn providers_skip_stand_ins_and_disabled_entries() {
        let catalog = test_catalog();
        let providers = providers(&catalog, &HashSet::new());
        assert!(providers.iter().any(|p| p.id == builtin::openai()));
        assert!(providers.iter().all(|p| p.id.as_str() != "openai-codex"));
        assert!(providers.iter().all(|p| p.id.as_str() != "ollama"));
        let openai = providers
            .iter()
            .find(|p| p.id == builtin::openai())
            .unwrap();
        assert_eq!(
            openai.expected_secret_name.as_deref(),
            Some("OPENAI_API_KEY")
        );
        assert!(openai.model_count > 0);
    }
}
