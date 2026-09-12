use std::collections::{BTreeMap, HashMap, HashSet};

use fabro_llm::lithos_catalog::{Catalog, Offering};
use fabro_llm::{FallbackTarget, ModelSelectionError, selection};
use fabro_types::settings::{ModelRef, ResolvedModelRef};
use fabro_types::{RunNoticeCode, RunNoticeLevel};
use lithos_llm::catalog::ProviderId;
use lithos_llm::types::ReasoningEffort;

use crate::Error;

/// Catalog-resolved fallback chains keyed by canonical requested model ID.
///
/// A chain is selected from the original request only. Targets never cause
/// another chain lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelFallbackPolicy {
    chains: BTreeMap<String, Vec<FallbackTarget>>,
}

impl ModelFallbackPolicy {
    #[cfg(test)]
    #[must_use]
    pub fn new(chains: BTreeMap<String, Vec<FallbackTarget>>) -> Self {
        Self { chains }
    }

    #[must_use]
    pub fn chain_for<'a>(
        &'a self,
        catalog: &Catalog,
        provider: &ProviderId,
        model: &str,
    ) -> Option<&'a [FallbackTarget]> {
        self.chain_for_canonical(&canonical_model_id(catalog, provider, model))
    }

    /// Look up a chain by an already-canonicalized requested model ID.
    #[must_use]
    pub fn chain_for_canonical(&self, canonical_model: &str) -> Option<&[FallbackTarget]> {
        self.chains.get(canonical_model).map(Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[FallbackTarget])> {
        self.chains
            .iter()
            .map(|(model, chain)| (model.as_str(), chain.as_slice()))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.chains.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
    }
}

/// Server-side result of canonicalizing and filtering configured fallback
/// chains.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResolvedModelFallbacks {
    pub policy:  ModelFallbackPolicy,
    pub notices: Vec<ModelFallbackNotice>,
}

/// Why a configured fallback candidate was removed from one model's chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelFallbackNotice {
    ProviderUnconfigured {
        requested_model: String,
        reference:       ModelRef,
        provider:        ProviderId,
    },
    NoConfiguredOffering {
        requested_model: String,
        reference:       ModelRef,
        providers:       Vec<ProviderId>,
    },
    PrimaryNotInCatalog {
        requested_model: String,
        reference:       ModelRef,
        primary:         FallbackTarget,
    },
    NoCompatibleModel {
        requested_model: String,
        reference:       ModelRef,
        provider:        ProviderId,
    },
    Duplicate {
        requested_model: String,
        reference:       ModelRef,
        target:          FallbackTarget,
    },
    NoNearbyReasoningLevel {
        requested_model:  String,
        target:           FallbackTarget,
        requested_effort: ReasoningEffort,
    },
    ChainEmpty {
        requested_model: String,
    },
}

impl ModelFallbackNotice {
    #[must_use]
    pub fn code(&self) -> RunNoticeCode {
        match self {
            Self::ChainEmpty { .. } => RunNoticeCode::ModelFallbackChainEmpty,
            Self::ProviderUnconfigured { .. }
            | Self::NoConfiguredOffering { .. }
            | Self::PrimaryNotInCatalog { .. }
            | Self::NoCompatibleModel { .. }
            | Self::Duplicate { .. }
            | Self::NoNearbyReasoningLevel { .. } => RunNoticeCode::ModelFallbackSkipped,
        }
    }

    #[must_use]
    pub fn level(&self) -> RunNoticeLevel {
        match self {
            Self::Duplicate { .. } => RunNoticeLevel::Info,
            Self::ProviderUnconfigured { .. }
            | Self::NoConfiguredOffering { .. }
            | Self::PrimaryNotInCatalog { .. }
            | Self::NoCompatibleModel { .. }
            | Self::NoNearbyReasoningLevel { .. }
            | Self::ChainEmpty { .. } => RunNoticeLevel::Warn,
        }
    }

    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::ProviderUnconfigured {
                requested_model,
                reference,
                provider,
            } => format!(
                "Model fallback `{reference}` for requested model `{requested_model}` was skipped because provider `{provider}` is not configured."
            ),
            Self::NoConfiguredOffering {
                requested_model,
                reference,
                providers,
            } => {
                let providers = providers
                    .iter()
                    .map(ProviderId::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "Model fallback `{reference}` for requested model `{requested_model}` was skipped because none of its providers are configured. It is offered by: {providers}."
                )
            }
            Self::PrimaryNotInCatalog {
                requested_model,
                reference,
                primary,
            } => format!(
                "Model fallback `{reference}` for requested model `{requested_model}` was skipped because `{primary}` is not in the catalog, so there is no capability profile to match against."
            ),
            Self::NoCompatibleModel {
                requested_model,
                reference,
                provider,
            } => format!(
                "Model fallback `{reference}` for requested model `{requested_model}` was skipped because provider `{provider}` has no compatible model."
            ),
            Self::Duplicate {
                requested_model,
                reference,
                target,
            } => format!(
                "Model fallback `{reference}` for requested model `{requested_model}` was skipped because target `{target}` already appears in that chain."
            ),
            Self::NoNearbyReasoningLevel {
                requested_model,
                target,
                requested_effort,
            } => {
                format!(
                    "Model fallback `{target}` for requested model `{requested_model}` was skipped because it has no reasoning level near `{requested_effort}`."
                )
            }
            Self::ChainEmpty { requested_model } => format!(
                "No usable model fallbacks remain for requested model `{requested_model}` after filtering its configured candidates."
            ),
        }
    }
}

/// Resolve every model-keyed fallback chain against the server's catalog and
/// configured-provider snapshot.
///
/// This function must stay at server-side call sites. Offline validation only
/// parses the raw table and cannot canonicalize model aliases.
pub fn resolve_model_fallbacks(
    catalog: &Catalog,
    configured_providers: &[ProviderId],
    configured: &BTreeMap<String, Vec<ModelRef>>,
) -> Result<ResolvedModelFallbacks, Error> {
    let eligible = configured_providers.iter().cloned().collect::<HashSet<_>>();
    let mut resolved = ResolvedModelFallbacks::default();
    let mut raw_key_by_canonical = HashMap::<String, String>::new();

    for (raw_key, references) in configured {
        require_bare_model_key(catalog, raw_key)?;
        let selected = selection::resolve_selection_with_catalog_fallback(
            catalog,
            Some(raw_key),
            None,
            &eligible,
        )?;
        let requested_model = selected.model;

        if let Some(previous) =
            raw_key_by_canonical.insert(requested_model.clone(), raw_key.clone())
        {
            return Err(Error::Precondition(format!(
                "`run.model.fallbacks` keys `{previous}` and `{raw_key}` both resolve to requested model `{requested_model}`"
            )));
        }

        let primary = FallbackTarget::new(&selected.provider, &requested_model);
        let primary_model = catalog
            .enabled_provider(selected.provider.as_str())
            .and_then(|provider| provider.offering(&requested_model));
        let mut targets = Vec::new();

        for model_ref in references {
            let target = match resolve_fallback_candidate(
                catalog,
                &requested_model,
                &primary,
                primary_model.as_ref(),
                &eligible,
                model_ref,
            )? {
                FallbackCandidate::Skipped(notice) => {
                    resolved.notices.push(notice);
                    continue;
                }
                FallbackCandidate::Target(target) => target,
            };

            if targets.contains(&target) {
                resolved.notices.push(ModelFallbackNotice::Duplicate {
                    requested_model: requested_model.clone(),
                    reference: model_ref.clone(),
                    target,
                });
            } else {
                targets.push(target);
            }
        }

        if targets.is_empty() {
            resolved.notices.push(ModelFallbackNotice::ChainEmpty {
                requested_model: requested_model.clone(),
            });
        }
        resolved.policy.chains.insert(requested_model, targets);
    }

    Ok(resolved)
}

/// Reject chain keys that name a provider. Keys are requested-model selectors;
/// a provider-qualified key can never match a dispatch-time canonical model
/// ID, so it would be silently dead configuration.
fn require_bare_model_key(catalog: &Catalog, raw_key: &str) -> Result<(), Error> {
    let reference: ModelRef = raw_key
        .parse()
        .map_err(|error| Error::Precondition(format!("`run.model.fallbacks` key: {error}")))?;
    match reference.resolve(catalog) {
        Ok(ResolvedModelRef::Model { provider: None, .. }) => Ok(()),
        Ok(ResolvedModelRef::Model {
            provider: Some(_),
            selector,
        }) => Err(Error::Precondition(format!(
            "`run.model.fallbacks` keys name a requested model; use `{selector}` instead of `{raw_key}`"
        ))),
        Ok(ResolvedModelRef::Provider(provider)) => Err(Error::Precondition(format!(
            "`run.model.fallbacks` key `{raw_key}` names provider `{provider}`; keys must name a requested model"
        ))),
        Err(ambiguous) => Err(Error::Precondition(format!(
            "`run.model.fallbacks` key: {ambiguous}"
        ))),
    }
}

enum FallbackCandidate {
    Target(FallbackTarget),
    Skipped(ModelFallbackNotice),
}

/// The catalog id for `selector` on `provider`, else anywhere; the selector
/// itself for a passthrough model the catalog does not know.
pub(crate) fn canonical_model_id(
    catalog: &Catalog,
    provider: &ProviderId,
    selector: &str,
) -> String {
    catalog
        .canonical_model_id(Some(provider), selector)
        .map_or_else(|| selector.to_string(), ToString::to_string)
}

fn resolve_fallback_candidate(
    catalog: &Catalog,
    requested_model: &str,
    primary: &FallbackTarget,
    primary_model: Option<&Offering<'_>>,
    eligible: &HashSet<ProviderId>,
    model_ref: &ModelRef,
) -> Result<FallbackCandidate, Error> {
    let reference = model_ref.clone();

    Ok(match model_ref.resolve(catalog)? {
        ResolvedModelRef::Provider(provider_name) => {
            let provider = selection::require_provider(catalog, &provider_name)?;
            if !eligible.contains(&provider) {
                return Ok(FallbackCandidate::Skipped(
                    ModelFallbackNotice::ProviderUnconfigured {
                        requested_model: requested_model.to_string(),
                        reference,
                        provider,
                    },
                ));
            }
            let Some(primary_model) = primary_model else {
                return Ok(FallbackCandidate::Skipped(
                    ModelFallbackNotice::PrimaryNotInCatalog {
                        requested_model: requested_model.to_string(),
                        reference,
                        primary: primary.clone(),
                    },
                ));
            };
            match catalog
                .enabled_provider(provider.as_str())
                .and_then(|target| target.closest_offering(primary_model.model))
            {
                Some(entry) => {
                    FallbackCandidate::Target(FallbackTarget::new(provider, entry.model.id()))
                }
                None => FallbackCandidate::Skipped(ModelFallbackNotice::NoCompatibleModel {
                    requested_model: requested_model.to_string(),
                    reference,
                    provider,
                }),
            }
        }
        ResolvedModelRef::Model {
            provider: Some(provider_name),
            selector,
        } => {
            let provider = selection::require_provider(catalog, &provider_name)?;
            if !eligible.contains(&provider) {
                return Ok(FallbackCandidate::Skipped(
                    ModelFallbackNotice::ProviderUnconfigured {
                        requested_model: requested_model.to_string(),
                        reference,
                        provider,
                    },
                ));
            }
            match selection::resolve_on_provider(catalog, &provider, &selector) {
                Ok(entry) => FallbackCandidate::Target(FallbackTarget::new(
                    entry.provider.id(),
                    entry.model.id(),
                )),
                Err(ModelSelectionError::UnknownSelectorOnProvider { .. }) => {
                    FallbackCandidate::Target(FallbackTarget::new(provider, selector))
                }
                Err(error) => return Err(error.into()),
            }
        }
        ResolvedModelRef::Model {
            provider: None,
            selector,
        } => match selection::select(catalog, &selector, None, eligible) {
            Ok(entry) => FallbackCandidate::Target(FallbackTarget::new(
                entry.provider.id(),
                entry.model.id(),
            )),
            Err(ModelSelectionError::NoEligibleOffering { providers, .. }) => {
                FallbackCandidate::Skipped(ModelFallbackNotice::NoConfiguredOffering {
                    requested_model: requested_model.to_string(),
                    reference,
                    providers,
                })
            }
            Err(ModelSelectionError::UnknownSelector { .. }) => {
                FallbackCandidate::Target(FallbackTarget::new(&primary.provider, selector))
            }
            Err(error) => return Err(error.into()),
        },
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fabro_llm::FallbackTarget;
    use fabro_llm::lithos_catalog::Catalog;
    use fabro_llm::test_support::test_catalog_with_overlay;
    use lithos_llm::catalog::ProviderId;

    use super::{ModelFallbackNotice, resolve_model_fallbacks};

    fn references(values: &[&str]) -> Vec<fabro_types::settings::ModelRef> {
        values
            .iter()
            .map(|value| value.parse().expect("fixture reference should parse"))
            .collect()
    }

    fn openrouter_catalog() -> Catalog {
        test_catalog_with_overlay("[providers.openrouter]\nenabled = true\n")
    }

    #[test]
    fn canonicalizes_keys_and_keeps_each_chain_independent() {
        let catalog = openrouter_catalog();
        let eligible = [ProviderId::new("openrouter")];
        let configured = BTreeMap::from([
            ("gpt-sol".to_string(), references(&["claude-opus"])),
            (
                "claude-fable".to_string(),
                references(&["gpt-sol", "claude-opus"]),
            ),
        ]);

        let resolved = resolve_model_fallbacks(&catalog, &eligible, &configured).unwrap();

        assert_eq!(
            resolved
                .policy
                .chain_for(&catalog, &ProviderId::new("openrouter"), "gpt-sol"),
            Some([FallbackTarget::new("openrouter", "claude-opus-5")].as_slice())
        );
        assert_eq!(
            resolved
                .policy
                .chain_for(&catalog, &ProviderId::new("openrouter"), "claude-fable"),
            Some(
                [
                    FallbackTarget::new("openrouter", "gpt-5.6-sol"),
                    FallbackTarget::new("openrouter", "claude-opus-5"),
                ]
                .as_slice()
            )
        );
    }

    #[test]
    fn rejects_aliases_that_define_the_same_requested_model_twice() {
        let catalog = openrouter_catalog();
        let eligible = [ProviderId::new("openrouter")];
        let configured = BTreeMap::from([
            ("gpt-sol".to_string(), references(&["claude-opus"])),
            ("gpt-5.6-sol".to_string(), references(&["claude-fable"])),
        ]);

        let error = resolve_model_fallbacks(&catalog, &eligible, &configured).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("both resolve to requested model"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_provider_qualified_keys() {
        let catalog = openrouter_catalog();
        let eligible = [ProviderId::new("openrouter")];
        let configured = BTreeMap::from([(
            "openrouter:gpt-sol".to_string(),
            references(&["claude-opus"]),
        )]);

        let error = resolve_model_fallbacks(&catalog, &eligible, &configured).unwrap_err();

        assert!(
            error.to_string().contains("keys name a requested model"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn skips_unconfigured_candidates_per_requested_model() {
        let catalog = openrouter_catalog();
        let eligible = [ProviderId::new("openrouter")];
        let configured = BTreeMap::from([(
            "kimi-k3".to_string(),
            references(&["moonshot:kimi-k3", "openrouter:kimi-k3"]),
        )]);

        let resolved = resolve_model_fallbacks(&catalog, &eligible, &configured).unwrap();

        assert_eq!(
            resolved
                .policy
                .chain_for(&catalog, &ProviderId::new("openrouter"), "kimi-k3"),
            Some([FallbackTarget::new("openrouter", "kimi-k3")].as_slice())
        );
        assert!(matches!(
            resolved.notices.as_slice(),
            [ModelFallbackNotice::ProviderUnconfigured {
                requested_model,
                provider,
                ..
            }] if requested_model == "kimi-k3" && provider == &ProviderId::new("moonshot")
        ));
    }

    #[test]
    fn resolves_the_requested_production_policy_as_independent_chains() {
        let catalog = test_catalog_with_overlay(
            "[providers.modal]\nenabled = true\n\n[providers.openrouter]\nenabled = true\n",
        );
        let eligible = [
            ProviderId::new("modal"),
            ProviderId::new("moonshot"),
            ProviderId::new("openrouter"),
        ];
        let configured = BTreeMap::from([
            (
                "kimi-k3".to_string(),
                references(&["moonshot:kimi-k3", "openrouter:kimi-k3", "claude-opus"]),
            ),
            ("glm-5.2".to_string(), references(&["gpt-sol"])),
            ("gpt-sol".to_string(), references(&["claude-opus"])),
            ("claude-opus".to_string(), references(&["gpt-sol"])),
            ("gpt-terra".to_string(), references(&["claude-opus"])),
            ("gpt-luna".to_string(), references(&["claude-sonnet"])),
            (
                "claude-fable".to_string(),
                references(&["gpt-sol", "claude-opus"]),
            ),
        ]);

        let resolved = resolve_model_fallbacks(&catalog, &eligible, &configured).unwrap();

        assert!(resolved.notices.is_empty());
        let chain = |model: &str| {
            resolved
                .policy
                .chain_for(&catalog, &ProviderId::new("openrouter"), model)
                .expect("requested model should have a chain")
        };
        assert_eq!(chain("kimi-k3"), [
            FallbackTarget::new("moonshot", "kimi-k3"),
            FallbackTarget::new("openrouter", "kimi-k3"),
            FallbackTarget::new("openrouter", "claude-opus-5"),
        ]);
        assert_eq!(chain("glm-5.2"), [FallbackTarget::new(
            "openrouter",
            "gpt-5.6-sol"
        )]);
        assert_eq!(chain("gpt-sol"), [FallbackTarget::new(
            "openrouter",
            "claude-opus-5"
        )]);
        assert_eq!(chain("claude-opus"), [FallbackTarget::new(
            "openrouter",
            "gpt-5.6-sol"
        )]);
        assert_eq!(chain("gpt-terra"), [FallbackTarget::new(
            "openrouter",
            "claude-opus-5"
        )]);
        assert_eq!(chain("gpt-luna"), [FallbackTarget::new(
            "openrouter",
            "claude-sonnet-5"
        )]);
        assert_eq!(chain("claude-fable"), [
            FallbackTarget::new("openrouter", "gpt-5.6-sol"),
            FallbackTarget::new("openrouter", "claude-opus-5"),
        ]);
    }
}
