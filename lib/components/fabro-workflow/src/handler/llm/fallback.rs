//! The fixed fallback plan a stage follows when its model fails.
//!
//! The plan belongs to the originally requested model: advancing it never
//! activates a target model's own chain. `model_fallback.rs` decides the
//! policy; this module resolves it against the catalog and records each
//! failover as a run event. Agent stages hand the resolved routes to pebble
//! ([`FallbackPlan::pebble_routes`]), which executes them and reports each
//! move as `RouteFailover`; one-shot prompt stages walk the plan themselves.

use fabro_graphviz::graph::Node;
use fabro_llm::FallbackTarget;
use fabro_llm::lithos_catalog::Catalog;
use fabro_types::FailoverProps;
use lithos_llm::catalog::ProviderId;
use lithos_llm::types::ReasoningEffort;
use pebble_coding_agent::FallbackRoute;

use super::controls::EffectiveRequestControls;
use crate::event::{Emitter, Event, StageScope};
use crate::model_fallback::{ModelFallbackNotice, ModelFallbackPolicy, canonical_model_id};

#[derive(Clone, Debug)]
pub(crate) struct LlmRoute {
    pub(crate) target:   FallbackTarget,
    pub(crate) controls: EffectiveRequestControls,
}

impl LlmRoute {
    /// The `provider/model` selector the client resolves for this route.
    pub(crate) fn selector(&self) -> String {
        format!("{}/{}", self.target.provider, self.target.model)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FallbackPlan {
    pub(crate) original:  LlmRoute,
    pub(crate) remaining: Vec<LlmRoute>,
    /// 0 addresses the original route; N addresses `remaining[N - 1]`.
    pub(crate) position:  usize,
}

impl FallbackPlan {
    pub(crate) fn current(&self) -> &LlmRoute {
        self.route_at(self.position)
    }

    /// The route that was active before the most recent [`Self::advance`].
    pub(crate) fn previous(&self) -> &LlmRoute {
        self.route_at(self.position.saturating_sub(1))
    }

    fn route_at(&self, position: usize) -> &LlmRoute {
        position
            .checked_sub(1)
            .map_or(&self.original, |index| &self.remaining[index])
    }

    pub(crate) fn attempt(&self) -> u32 {
        u32::try_from(self.position).unwrap_or(u32::MAX)
    }

    #[must_use]
    pub(crate) fn has_next(&self) -> bool {
        self.position < self.remaining.len()
    }

    /// Move to the next fallback route. Returns false when the plan is
    /// exhausted.
    pub(crate) fn advance(&mut self) -> bool {
        if self.has_next() {
            self.position += 1;
            true
        } else {
            false
        }
    }

    /// Moves to the route whose `provider/model` selector is `selector`, the
    /// route pebble reports a prompt ended on. Returns whether the position
    /// changed; a selector the plan does not know leaves it where it was.
    pub(crate) fn advance_to(&mut self, selector: &str) -> bool {
        if self.current().selector() == selector {
            return false;
        }
        match self
            .remaining
            .iter()
            .position(|route| route.selector() == selector)
        {
            Some(index) => {
                self.position = index + 1;
                true
            }
            None => false,
        }
    }

    /// The routes after the current one, as pebble executes them: each with
    /// its own controls and the stage's output limit.
    pub(crate) fn pebble_routes(&self, max_tokens: Option<i64>) -> Vec<FallbackRoute> {
        self.remaining
            .iter()
            .skip(self.position)
            .map(|route| {
                FallbackRoute::new(route.selector())
                    .with_reasoning_effort(route.controls.reasoning_effort)
                    .with_speed(route.controls.speed)
                    .with_max_tokens(max_tokens)
            })
            .collect()
    }

    /// The `agent.failover` payload for a move from `from` to `to`, both
    /// `provider/model` selectors, on this plan.
    ///
    /// `from` may be a route that failed during activation without serving
    /// traffic; `error` says why it was abandoned. Consecutive payloads
    /// chain: one's `to` is the next one's `from`.
    pub(crate) fn failover_props(
        &self,
        from: &str,
        to: &str,
        attempt: u32,
        error: &str,
    ) -> FailoverProps {
        let (from_provider, from_model) = split_selector(from);
        let (to_provider, to_model) = split_selector(to);
        let effective_reasoning_effort = std::iter::once(&self.original)
            .chain(self.remaining.iter())
            .find(|route| route.selector() == to)
            .and_then(|route| route.controls.reasoning_effort);
        FailoverProps {
            original_provider: Some(self.original.target.provider.to_string()),
            original_model: Some(self.original.target.model.to_string()),
            attempt: Some(attempt),
            from_provider,
            from_model,
            to_provider,
            to_model,
            requested_reasoning_effort: self.original.controls.reasoning_effort,
            effective_reasoning_effort,
            error: error.to_string(),
        }
    }
}

/// A `provider/model` selector split at its first slash; a selector with no
/// slash is all model.
fn split_selector(selector: &str) -> (String, String) {
    match selector.split_once('/') {
        Some((provider, model)) => (provider.to_string(), model.to_string()),
        None => (String::new(), selector.to_string()),
    }
}

/// Request controls resolved for one fallback target.
enum FallbackControls {
    /// The target can serve the request with these controls.
    Usable(EffectiveRequestControls),
    /// The target advertises reasoning levels, but none is near the requested
    /// effort.
    NoNearbyReasoningLevel(ReasoningEffort),
}

fn fallback_controls_for_target(
    catalog: &Catalog,
    target: &FallbackTarget,
    requested: EffectiveRequestControls,
) -> FallbackControls {
    let Some(requested_effort) = requested.reasoning_effort else {
        return FallbackControls::Usable(requested);
    };
    let Some(offering) = catalog
        .enabled_provider(target.provider.as_str())
        .and_then(|provider| provider.offering(target.model.as_str()))
    else {
        // A catalog-unknown passthrough target has no advertised controls.
        // Preserve the request and let the provider validate it.
        return FallbackControls::Usable(requested);
    };
    let capabilities = offering.model.capabilities();
    let effective_effort = capabilities.closest_supported_effort(requested_effort);
    match effective_effort {
        Some(effort) => FallbackControls::Usable(EffectiveRequestControls {
            reasoning_effort: Some(effort),
            speed:            requested.speed,
        }),
        // No level is verified. Unless the requested one is verified
        // unsupported, preserve it and let the provider validate, as for
        // a passthrough target.
        None if !capabilities
            .reasoning_effort(requested_effort)
            .is_unsupported() =>
        {
            FallbackControls::Usable(requested)
        }
        None => FallbackControls::NoNearbyReasoningLevel(requested_effort),
    }
}

/// The plan for `model` on `provider`, and the configuration notices the
/// caller should surface once per run.
pub(crate) fn fallback_plan(
    catalog: &Catalog,
    fallbacks: &ModelFallbackPolicy,
    model: &str,
    provider: &ProviderId,
    requested_controls: EffectiveRequestControls,
) -> (FallbackPlan, Vec<ModelFallbackNotice>) {
    let primary_model = canonical_model_id(catalog, provider, model);
    let original = LlmRoute {
        target:   FallbackTarget::new(provider, &primary_model),
        controls: requested_controls,
    };
    let Some(configured) = fallbacks.chain_for_canonical(&primary_model) else {
        return (
            FallbackPlan {
                original,
                remaining: Vec::new(),
                position: 0,
            },
            Vec::new(),
        );
    };

    let mut remaining = Vec::new();
    let mut notices = Vec::new();
    for target in configured {
        // The resolver already de-duplicated the chain; only the primary
        // target, which the resolver cannot know, needs filtering here.
        if *target == original.target {
            continue;
        }

        let controls = match fallback_controls_for_target(catalog, target, requested_controls) {
            FallbackControls::Usable(controls) => controls,
            FallbackControls::NoNearbyReasoningLevel(requested_effort) => {
                notices.push(ModelFallbackNotice::NoNearbyReasoningLevel {
                    requested_model: original.target.model.to_string(),
                    target: target.clone(),
                    requested_effort,
                });
                continue;
            }
        };
        remaining.push(LlmRoute {
            target: target.clone(),
            controls,
        });
    }

    if !configured.is_empty() && remaining.is_empty() {
        notices.push(ModelFallbackNotice::ChainEmpty {
            requested_model: original.target.model.to_string(),
        });
    }

    (
        FallbackPlan {
            original,
            remaining,
            position: 0,
        },
        notices,
    )
}

/// Emit `agent.failover` for the plan's most recent
/// [`FallbackPlan::advance`], on a one-shot stage that walks the plan itself.
pub(crate) fn emit_failover(
    node: &Node,
    emitter: &Emitter,
    stage_scope: &StageScope,
    plan: &FallbackPlan,
    error: &str,
) {
    emitter.emit_scoped(
        &Event::Failover {
            stage: node.id.clone(),
            props: plan.failover_props(
                &plan.previous().selector(),
                &plan.current().selector(),
                plan.attempt(),
                error,
            ),
        },
        stage_scope,
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fabro_llm::test_support::test_catalog_with_overlay;
    use lithos_llm::catalog::builtin;

    use super::*;

    /// Modal and OpenRouter ship disabled; enable them the way an operator
    /// would so their models become fallback targets.
    fn enabled_fallback_catalog() -> Catalog {
        test_catalog_with_overlay(
            "[providers.modal]\nenabled = true\n\n[providers.openrouter]\nenabled = true\n",
        )
    }

    #[test]
    fn fallback_plan_maps_reasoning_to_each_target_and_rounds_ties_up() {
        let policy = ModelFallbackPolicy::new(BTreeMap::from([("kimi-k3".to_string(), vec![
            FallbackTarget::new("moonshot", "kimi-k3"),
            FallbackTarget::new("openrouter", "kimi-k3"),
            FallbackTarget::new("anthropic", "claude-opus-5"),
        ])]));

        let (plan, notices) = fallback_plan(
            &enabled_fallback_catalog(),
            &policy,
            "kimi-k3",
            &ProviderId::new("modal"),
            EffectiveRequestControls {
                reasoning_effort: Some(ReasoningEffort::Medium),
                speed:            None,
            },
        );

        assert!(notices.is_empty());
        assert_eq!(
            plan.remaining
                .iter()
                .map(|route| route.controls.reasoning_effort)
                .collect::<Vec<_>>(),
            vec![
                Some(ReasoningEffort::High),
                Some(ReasoningEffort::High),
                Some(ReasoningEffort::Medium),
            ]
        );
    }

    #[test]
    fn advancing_a_fallback_plan_never_activates_the_target_models_chain() {
        let policy = ModelFallbackPolicy::new(BTreeMap::from([
            ("claude-fable-5".to_string(), vec![
                FallbackTarget::new("openai", "gpt-5.6-sol"),
                FallbackTarget::new("anthropic", "claude-opus-5"),
            ]),
            ("gpt-5.6-sol".to_string(), vec![FallbackTarget::new(
                "anthropic",
                "claude-sonnet-5",
            )]),
        ]));
        let (mut plan, notices) = fallback_plan(
            &enabled_fallback_catalog(),
            &policy,
            "claude-fable-5",
            &builtin::anthropic(),
            EffectiveRequestControls::default(),
        );

        assert!(notices.is_empty());
        assert!(plan.advance(), "Sol should be first");
        assert_eq!(
            plan.current().target,
            FallbackTarget::new("openai", "gpt-5.6-sol")
        );
        assert_eq!(plan.current().selector(), "openai/gpt-5.6-sol");
        assert_eq!(plan.attempt(), 1);
        assert!(plan.advance(), "Opus should be second");
        assert_eq!(
            plan.current().target,
            FallbackTarget::new("anthropic", "claude-opus-5")
        );
        assert_eq!(plan.attempt(), 2);
        assert!(!plan.has_next());
        assert!(!plan.advance());
    }
}
