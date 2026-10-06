pub use fabro_core::outcome::{
    FailureCategory, FailureDetail, OutcomeMeta, StageOutcome, StageState,
};
use fabro_llm::lithos_catalog::Catalog;
pub use fabro_types::BilledModelUsage;
use fabro_types::{BilledTokenCounts, ModelRef, UsdMicros};
use lithos_llm::types::TokenCounts;

use crate::error::{Error, FailureSignature, classify_failure_reason};

pub type Outcome = fabro_core::Outcome<Option<BilledModelUsage>>;

/// Bills native token buckets from the selected catalog without changing usage.
pub fn billed_model_usage_from_llm(
    catalog: &Catalog,
    model: &ModelRef,
    usage: TokenCounts,
) -> Result<BilledModelUsage, Error> {
    if catalog.enabled_provider(model.provider.as_str()).is_none() {
        return Err(Error::Precondition(format!(
            "Provider \"{}\" is not configured",
            model.provider
        )));
    }
    let cost = catalog.estimate_cost(&model.handle(), usage, model.speed);
    Ok(BilledModelUsage::new(model.clone(), usage, cost))
}

#[must_use]
pub fn reported_model_usage(
    model: ModelRef,
    tokens: TokenCounts,
    reported_cost: Option<UsdMicros>,
) -> BilledModelUsage {
    BilledModelUsage {
        model,
        tokens,
        total_usd_micros: reported_cost.map(|cost| cost.0),
    }
}

#[must_use]
pub fn billed_token_counts_from_llm(usage: TokenCounts) -> BilledTokenCounts {
    BilledTokenCounts::from_token_counts(usage, None)
}

pub trait OutcomeExt: Sized {
    fn fail_deterministic(reason: impl Into<String>) -> Self;
    fn fail_classify(reason: impl Into<String>) -> Self;
    fn retry_classify(reason: impl Into<String>) -> Self;
    fn simulated(node_id: &str) -> Self;
    #[must_use]
    fn with_signature(self, sig: Option<impl Into<String>>) -> Self;
    fn failure_reason(&self) -> Option<&str>;
    fn failure_category(&self) -> Option<FailureCategory>;
    fn classified_failure_category(&self) -> Option<FailureCategory>;
}

impl OutcomeExt for Outcome {
    fn fail_deterministic(reason: impl Into<String>) -> Self {
        Self {
            status: StageOutcome::Failed {
                retry_requested: false,
            },
            failure: Some(FailureDetail::new(reason, FailureCategory::Deterministic)),
            ..Self::default()
        }
    }

    fn fail_classify(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let category = classify_failure_reason(&reason);
        Self {
            status: StageOutcome::Failed {
                retry_requested: false,
            },
            failure: Some(FailureDetail::new(reason, category)),
            ..Self::default()
        }
    }

    fn retry_classify(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let category = classify_failure_reason(&reason);
        Self {
            status: StageOutcome::Failed {
                retry_requested: true,
            },
            failure: Some(FailureDetail::new(reason, category)),
            ..Self::default()
        }
    }

    fn simulated(node_id: &str) -> Self {
        Self {
            notes: Some(format!("[Simulated] {node_id}")),
            ..Self::success()
        }
    }

    fn with_signature(mut self, sig: Option<impl Into<String>>) -> Self {
        if let Some(ref mut failure) = self.failure {
            failure.signature = sig.map(|sig| FailureSignature(sig.into()));
        }
        self
    }

    fn failure_reason(&self) -> Option<&str> {
        self.failure
            .as_ref()
            .map(|failure| failure.message.as_str())
    }

    fn failure_category(&self) -> Option<FailureCategory> {
        self.failure.as_ref().map(|failure| failure.category)
    }

    fn classified_failure_category(&self) -> Option<FailureCategory> {
        match self.status {
            StageOutcome::Succeeded | StageOutcome::PartiallySucceeded | StageOutcome::Skipped => {
                None
            }
            StageOutcome::Failed { .. } => self
                .failure_category()
                .or(Some(FailureCategory::Deterministic)),
        }
    }
}

#[must_use]
pub fn format_cost(cost: f64) -> String {
    format!("${cost:.2}")
}

#[cfg(test)]
mod tests {
    use fabro_llm::test_support::{test_catalog, test_catalog_with_overlay};
    use fabro_types::{ModelRef, UsdMicros};
    use lithos_llm::catalog::{ModelId, ProviderId, builtin};
    use lithos_llm::types::{Speed, TokenCounts};

    use super::{OutcomeExt, billed_model_usage_from_llm, reported_model_usage};

    fn model_ref(provider: ProviderId, model: &str, speed: Option<Speed>) -> ModelRef {
        ModelRef::new(provider, ModelId::new(model)).with_speed(speed)
    }

    #[test]
    fn reported_model_usage_keeps_exact_tokens_and_optional_cost() {
        let tokens = TokenCounts {
            input: 10,
            output: 3,
            cache_read: 5,
            ..TokenCounts::default()
        };
        let model = model_ref(ProviderId::new("omp"), "deepseek", None);
        let usage = reported_model_usage(model.clone(), tokens, Some(UsdMicros(42_000)));
        assert_eq!(usage.tokens(), tokens);
        assert_eq!(usage.total_usd_micros, Some(42_000));
        assert_eq!(
            reported_model_usage(model, tokens, None).total_usd_micros,
            None
        );
        let json = serde_json::to_value(&usage).unwrap();
        assert!(json.get("input").is_none());
        assert_eq!(
            serde_json::from_value::<super::BilledModelUsage>(json).unwrap(),
            usage
        );
    }

    #[test]
    fn billed_model_usage_from_llm_bills_openai_cached_input_and_reasoning_output() {
        let usage = TokenCounts {
            input: 100_000,
            output: 25_000,
            reasoning: 5_000,
            cache_read: 50_000,
            ..TokenCounts::default()
        };
        let billed = billed_model_usage_from_llm(
            &test_catalog(),
            &model_ref(builtin::openai(), "gpt-5.4", None),
            usage,
        )
        .unwrap();
        assert_eq!(billed.total_usd_micros, Some(712_500));
        assert_eq!(billed.tokens(), usage);
    }

    #[test]
    fn response_cost_overrides_catalog_estimate() {
        let billed = billed_model_usage_from_llm(
            &test_catalog(),
            &model_ref(builtin::openai(), "gpt-5.4", None),
            TokenCounts {
                input: 11,
                output: 7,
                ..TokenCounts::default()
            },
        )
        .unwrap()
        .with_reported_cost(Some(UsdMicros(125_000)));
        assert_eq!(billed.total_usd_micros, Some(125_000));
    }

    #[test]
    fn retry_classify_marks_failed_outcome_with_retry_request() {
        let outcome = super::Outcome::retry_classify("timeout");
        assert!(outcome.status.retry_requested());
    }

    #[test]
    fn billed_model_usage_from_llm_bills_anthropic_fast_mode_cache_write_pricing() {
        let usage = TokenCounts {
            input:       100_000,
            output:      10_000,
            reasoning:   5_000,
            cache_read:  20_000,
            cache_write: 30_000,
        };
        let billed = billed_model_usage_from_llm(
            &test_catalog(),
            &model_ref(builtin::anthropic(), "claude-opus-5", Some(Speed::Fast)),
            usage,
        )
        .unwrap();
        assert_eq!(billed.total_usd_micros, Some(2_145_000));
        assert_eq!(billed.tokens(), usage);
    }

    #[test]
    fn billed_model_usage_from_llm_uses_custom_catalog_pricing() {
        let catalog = test_catalog_with_overlay(
            r#"
[providers.proxy]
display_name = "Proxy"
adapter = "openai-compatible"
codec = "openai-chat"
base_url = "https://proxy.example/v1"
auth = { type = "bearer" }
default_model = "canonical-model"

[providers.proxy.models.canonical-model]
display_name = "Canonical Model"
api_model = "wire-model"
limits = { context_tokens = 1000, max_output_tokens = 500 }
capabilities = { text = true, tools = true }
pricing = { input_usd_micros_per_million = 1000000, output_usd_micros_per_million = 2000000 }
"#,
        );
        let usage = TokenCounts {
            input: 500_000,
            output: 250_000,
            ..TokenCounts::default()
        };
        let canonical = billed_model_usage_from_llm(
            &catalog,
            &model_ref(ProviderId::new("proxy"), "canonical-model", None),
            usage,
        )
        .unwrap();
        assert_eq!(canonical.total_usd_micros, Some(1_000_000));
    }

    #[test]
    fn unknown_provider_is_a_precondition_failure() {
        let error = billed_model_usage_from_llm(
            &test_catalog(),
            &model_ref(ProviderId::new("nowhere"), "model", None),
            TokenCounts::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not configured"));
    }
}
