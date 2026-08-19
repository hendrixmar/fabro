use std::collections::HashSet;
use std::sync::Arc;

use fabro_graphviz::graph::{AttrValue, Graph};
use fabro_model::{Catalog, ProviderId};

use super::Transform;
use crate::error::Error;

/// Resolves model aliases to canonical IDs and infers the provider from the
/// model catalog.
pub struct ModelResolutionTransform {
    catalog:            Arc<Catalog>,
    default_provider:   Option<ProviderId>,
    eligible_providers: HashSet<ProviderId>,
    catalog_fallback:   bool,
}

impl ModelResolutionTransform {
    #[must_use]
    pub fn new(catalog: Arc<Catalog>) -> Self {
        let eligible_providers = catalog.all_provider_ids();
        Self {
            catalog,
            default_provider: None,
            eligible_providers,
            catalog_fallback: false,
        }
    }

    #[must_use]
    pub fn for_eligible(catalog: Arc<Catalog>, eligible_providers: HashSet<ProviderId>) -> Self {
        Self {
            catalog,
            default_provider: None,
            eligible_providers,
            catalog_fallback: false,
        }
    }

    #[must_use]
    pub fn with_default_provider(mut self, provider: Option<ProviderId>) -> Self {
        self.default_provider = provider;
        self
    }

    /// When enabled, provider-readiness selection failures fall back to the
    /// full catalog instead of erroring.
    #[must_use]
    pub fn with_catalog_fallback(mut self, catalog_fallback: bool) -> Self {
        self.catalog_fallback = catalog_fallback;
        self
    }

    /// The catalog this transform resolves against, so callers can run the
    /// matching catalog-backed lint rules.
    #[must_use]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn resolve_model(
        &self,
        model: &str,
        explicit_provider: Option<&ProviderId>,
    ) -> Result<(String, ProviderId), Error> {
        let selected = if self.catalog_fallback {
            self.catalog.resolve_selection_with_catalog_fallback(
                Some(model),
                explicit_provider,
                &self.eligible_providers,
            )
        } else {
            self.catalog
                .resolve_selection(Some(model), explicit_provider, &self.eligible_providers)
        }?;
        Ok((selected.model, selected.provider))
    }
}

impl Transform for ModelResolutionTransform {
    fn apply(&self, graph: Graph) -> Result<Graph, Error> {
        let mut graph = graph;
        let graph_default_provider = graph
            .attrs
            .get("default_provider")
            .and_then(AttrValue::as_str)
            .filter(|provider| !provider.is_empty())
            .map(ProviderId::new);
        let requested_default_provider = self
            .default_provider
            .as_ref()
            .or(graph_default_provider.as_ref());
        if let Some(default_model) = graph
            .attrs
            .get("default_model")
            .and_then(AttrValue::as_str)
            .map(str::to_string)
        {
            let (model, provider) =
                self.resolve_model(&default_model, requested_default_provider)?;
            graph
                .attrs
                .insert("default_model".to_string(), AttrValue::String(model));
            graph.attrs.insert(
                "default_provider".to_string(),
                AttrValue::String(provider.to_string()),
            );
        }
        let default_provider = self.default_provider.clone().or_else(|| {
            graph
                .attrs
                .get("default_provider")
                .and_then(AttrValue::as_str)
                .filter(|provider| !provider.is_empty())
                .map(ProviderId::new)
        });
        for node in graph.nodes.values_mut() {
            // ACP-backed nodes carry `model` as a harness hint (translated to
            // harness env, e.g. CODEX_CONFIG/PI_MODEL), not an API route —
            // stamping `provider` here would make the node invalid.
            if node.backend() == Some("acp") {
                continue;
            }
            let model = node
                .attrs
                .get("model")
                .and_then(AttrValue::as_str)
                .map(String::from);
            if let Some(model) = model {
                let explicit_provider = node
                    .attrs
                    .get("provider")
                    .and_then(AttrValue::as_str)
                    .filter(|provider| !provider.is_empty())
                    .map(ProviderId::new)
                    .or_else(|| default_provider.clone());
                let (model, provider) = self.resolve_model(&model, explicit_provider.as_ref())?;
                node.attrs
                    .insert("model".to_string(), AttrValue::String(model));
                node.attrs.insert(
                    "provider".to_string(),
                    AttrValue::String(provider.to_string()),
                );
            }
        }

        Ok(graph)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fabro_graphviz::graph::{AttrValue, Graph, Node};
    use fabro_model::catalog::LlmCatalogSettings;

    use super::*;

    fn custom_catalog() -> Arc<Catalog> {
        let settings: LlmCatalogSettings = toml::from_str(
            r#"
[providers.venice]
display_name = "Venice"
adapter = "openai_compatible"
agent_profile = "openai"
base_url = "https://api.venice.ai/api/v1"

[providers.venice.auth]
credentials = ["env:VENICE_API_KEY"]

[models."venice-large"]
provider = "venice"
display_name = "Venice Large"
family = "venice"
default = true
aliases = ["vl"]

[models."venice-large".limits]
context_window = 128000

[models."venice-large".features]
tools = true
vision = false
reasoning = false
"#,
        )
        .unwrap();
        Arc::new(Catalog::from_settings(&settings).unwrap())
    }

    fn builtin_transform() -> ModelResolutionTransform {
        let catalog = Catalog::from_builtin().unwrap();
        ModelResolutionTransform::new(Arc::new(catalog))
    }

    #[test]
    fn provider_inference_sets_provider_from_catalog() {
        let mut graph = Graph::new("test");
        let mut node = Node::new("a");
        node.attrs.insert(
            "model".to_string(),
            AttrValue::String("claude-sonnet-4-5".to_string()),
        );
        graph.nodes.insert("a".to_string(), node);

        let graph = builtin_transform().apply(graph).unwrap();

        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("provider")
                .and_then(AttrValue::as_str),
            Some("anthropic")
        );
    }

    #[test]
    fn explicit_provider_allows_unknown_model_passthrough() {
        let mut graph = Graph::new("test");
        let mut node = Node::new("a");
        node.attrs.insert(
            "model".to_string(),
            AttrValue::String("claude-sonnet-4-5".to_string()),
        );
        node.attrs.insert(
            "provider".to_string(),
            AttrValue::String("openai".to_string()),
        );
        graph.nodes.insert("a".to_string(), node);

        let graph = builtin_transform().apply(graph).unwrap();

        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("provider")
                .and_then(AttrValue::as_str),
            Some("openai")
        );
        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("model")
                .and_then(AttrValue::as_str),
            Some("claude-sonnet-4-5")
        );
    }

    #[test]
    fn provider_inference_unknown_model_pins_default_eligible_provider() {
        let mut graph = Graph::new("test");
        let mut node = Node::new("a");
        node.attrs.insert(
            "model".to_string(),
            AttrValue::String("unknown-model-xyz".to_string()),
        );
        graph.nodes.insert("a".to_string(), node);

        let graph = builtin_transform().apply(graph).unwrap();

        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("provider")
                .and_then(AttrValue::as_str),
            Some("anthropic")
        );
    }

    #[test]
    fn provider_inference_no_model_no_change() {
        let mut graph = Graph::new("test");
        let node = Node::new("a");
        graph.nodes.insert("a".to_string(), node);

        let graph = builtin_transform().apply(graph).unwrap();

        assert_eq!(graph.nodes["a"].attrs.get("provider"), None);
    }

    #[test]
    fn model_resolution_resolves_alias_to_canonical_id() {
        let mut graph = Graph::new("test");
        let mut node = Node::new("a");
        node.attrs
            .insert("model".to_string(), AttrValue::String("gpt-54".to_string()));
        graph.nodes.insert("a".to_string(), node);

        let graph = builtin_transform().apply(graph).unwrap();

        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("model")
                .and_then(AttrValue::as_str),
            Some("gpt-5.4")
        );
        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("provider")
                .and_then(AttrValue::as_str),
            Some("openai")
        );
    }

    #[test]
    fn model_resolution_keeps_canonical_id_unchanged() {
        let mut graph = Graph::new("test");
        let mut node = Node::new("a");
        node.attrs.insert(
            "model".to_string(),
            AttrValue::String("gpt-5.4".to_string()),
        );
        graph.nodes.insert("a".to_string(), node);

        let graph = builtin_transform().apply(graph).unwrap();

        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("model")
                .and_then(AttrValue::as_str),
            Some("gpt-5.4")
        );
    }

    #[test]
    fn model_resolution_uses_injected_catalog_for_alias_and_provider() {
        let mut graph = Graph::new("test");
        let mut node = Node::new("a");
        node.attrs
            .insert("model".to_string(), AttrValue::String("vl".to_string()));
        graph.nodes.insert("a".to_string(), node);

        let graph = ModelResolutionTransform::new(custom_catalog())
            .apply(graph)
            .unwrap();

        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("model")
                .and_then(AttrValue::as_str),
            Some("venice-large")
        );
        assert_eq!(
            graph.nodes["a"]
                .attrs
                .get("provider")
                .and_then(AttrValue::as_str),
            Some("venice")
        );
    }

    #[test]
    fn fallback_resolution_keeps_ready_preference_for_unpinned_nodes() {
        let overrides: LlmCatalogSettings = toml::from_str(
            r"
[providers.openrouter]
enabled = true
",
        )
        .unwrap();
        let catalog = Arc::new(Catalog::from_builtin_with_overrides(&overrides).unwrap());
        let mut graph = Graph::new("test");
        let mut portable = Node::new("portable");
        portable.attrs.insert(
            "model".to_string(),
            AttrValue::String("claude-fable".to_string()),
        );
        graph.nodes.insert("portable".to_string(), portable);
        let mut pinned = Node::new("pinned");
        pinned.attrs.insert(
            "model".to_string(),
            AttrValue::String("claude-fable".to_string()),
        );
        pinned.attrs.insert(
            "provider".to_string(),
            AttrValue::String("anthropic".to_string()),
        );
        graph.nodes.insert("pinned".to_string(), pinned);

        let graph = ModelResolutionTransform::for_eligible(
            Arc::clone(&catalog),
            HashSet::from([ProviderId::new("openrouter")]),
        )
        .with_catalog_fallback(true)
        .apply(graph)
        .unwrap();

        assert_eq!(
            graph.nodes["portable"].provider(),
            Some("openrouter"),
            "the unrelated unavailable pin must not force catalog-wide routing"
        );
        assert_eq!(graph.nodes["pinned"].provider(), Some("anthropic"));
    }

    #[test]
    fn graph_default_alias_materializes_to_canonical_offering() {
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "default_model".to_string(),
            AttrValue::String("vl".to_string()),
        );

        let graph = ModelResolutionTransform::new(custom_catalog())
            .apply(graph)
            .unwrap();

        assert_eq!(
            graph.attrs.get("default_model").and_then(AttrValue::as_str),
            Some("venice-large")
        );
        assert_eq!(
            graph
                .attrs
                .get("default_provider")
                .and_then(AttrValue::as_str),
            Some("venice")
        );
    }
}
