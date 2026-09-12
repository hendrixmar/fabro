//! Billing rollup vocabulary.
//!
//! Per-response token usage and cost come from lithos: [`TokenCounts`] holds
//! the five disjoint buckets and [`CostSource`] says where a cost came from.
//! Fabro sums that usage across responses, stages, and runs. The types here
//! are those sums, plus [`ModelRef`], the identity a billed response is
//! grouped under.

use lithos_llm::catalog::{ModelHandle, ModelId, ProviderId};
use lithos_llm::types::{Cost, Speed, TokenCounts};
use serde::{Deserialize, Serialize};

const USD_MICROS_PER_USD_F64: f64 = 1_000_000.0;

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "Billing rounds bounded finite floats into i64 counters by design."
)]
fn saturating_rounded_f64_to_i64(value: f64) -> i64 {
    if !value.is_finite() {
        return if value.is_sign_negative() {
            i64::MIN
        } else {
            i64::MAX
        };
    }

    if value <= i64::MIN as f64 {
        i64::MIN
    } else if value >= i64::MAX as f64 {
        i64::MAX
    } else {
        value as i64
    }
}

fn saturating_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn saturating_i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

/// A USD amount in micros (one millionth of a dollar).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct UsdMicros(pub i64);

impl UsdMicros {
    #[must_use]
    pub fn from_usd(usd: f64) -> Self {
        Self(saturating_rounded_f64_to_i64(
            (usd * USD_MICROS_PER_USD_F64).round(),
        ))
    }

    /// Converts a lithos cost into Fabro's signed micros.
    #[must_use]
    pub fn from_cost(cost: &Cost) -> Self {
        Self(saturating_u64_to_i64(cost.usd_micros))
    }

    /// Folds a cost into a running total that stays `None` until a cost is
    /// observed (`None` means "no provider data", not $0).
    pub fn accumulate(total: &mut Option<Self>, cost: Option<Self>) {
        if let Some(cost) = cost {
            *total.get_or_insert_default() += cost;
        }
    }
}

impl std::ops::Add for UsdMicros {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::AddAssign for UsdMicros {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl std::iter::Sum for UsdMicros {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, value| acc + value)
    }
}

/// Adds `rhs` into `total` bucket by bucket with saturation.
pub fn add_usage(total: &mut TokenCounts, rhs: TokenCounts) {
    total.input = total.input.saturating_add(rhs.input);
    total.output = total.output.saturating_add(rhs.output);
    total.reasoning = total.reasoning.saturating_add(rhs.reasoning);
    total.cache_read = total.cache_read.saturating_add(rhs.cache_read);
    total.cache_write = total.cache_write.saturating_add(rhs.cache_write);
}

fn accumulate_optional_usd_micros(total: &mut Option<i64>, cost: Option<i64>) {
    let mut typed_total = (*total).map(UsdMicros);
    UsdMicros::accumulate(&mut typed_total, cost.map(UsdMicros));
    *total = typed_total.map(|value| value.0);
}

/// Provider-qualified model identity a billed response is grouped under.
///
/// Carries the requested speed tier because providers price tiers
/// differently, so two responses from the same model at different speeds are
/// separate billing rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRef {
    pub provider: ProviderId,
    pub model_id: ModelId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed:    Option<Speed>,
}

impl ModelRef {
    #[must_use]
    pub fn new(provider: ProviderId, model_id: ModelId) -> Self {
        Self {
            provider,
            model_id,
            speed: None,
        }
    }

    #[must_use]
    pub fn from_handle(handle: &ModelHandle, speed: Option<Speed>) -> Self {
        Self {
            provider: handle.provider().clone(),
            model_id: handle.model().clone(),
            speed,
        }
    }

    #[must_use]
    pub fn with_speed(mut self, speed: Option<Speed>) -> Self {
        self.speed = speed;
        self
    }

    #[must_use]
    pub fn handle(&self) -> ModelHandle {
        ModelHandle::new(self.provider.clone(), self.model_id.clone())
    }

    /// Stable ordering key: provider, then model, then speed label.
    #[must_use]
    pub fn sort_key(&self) -> (&str, &str, &'static str) {
        (
            self.provider.as_str(),
            self.model_id.as_str(),
            self.speed.map_or("", Speed::as_str),
        )
    }
}

impl std::hash::Hash for ModelRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.provider.hash(state);
        self.model_id.hash(state);
        self.speed.map(Speed::as_str).hash(state);
    }
}

impl std::fmt::Display for ModelRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.provider, self.model_id)?;
        if let Some(speed) = self.speed {
            write!(f, " ({speed})")?;
        }
        Ok(())
    }
}

/// Usage and cost of one billed model response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BilledModelUsage {
    pub model:            ModelRef,
    pub tokens:           TokenCounts,
    /// Cost for `tokens`, when the provider reported one or the catalog could
    /// price them. `None` means no cost data, not zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_usd_micros: Option<i64>,
}

impl BilledModelUsage {
    #[must_use]
    pub fn new(model: ModelRef, tokens: TokenCounts, cost: Option<Cost>) -> Self {
        Self {
            model,
            tokens,
            total_usd_micros: cost.map(|cost| UsdMicros::from_cost(&cost).0),
        }
    }

    #[must_use]
    pub fn model(&self) -> &ModelRef {
        &self.model
    }

    #[must_use]
    pub fn model_id(&self) -> &str {
        self.model.model_id.as_str()
    }

    #[must_use]
    pub fn tokens(&self) -> TokenCounts {
        self.tokens
    }

    /// Overrides the billed total with a reported cost; `None` leaves the
    /// existing value in place.
    #[must_use]
    pub fn with_reported_cost(mut self, cost: Option<UsdMicros>) -> Self {
        if let Some(cost) = cost {
            self.total_usd_micros = Some(cost.0);
        }
        self
    }
}

/// Token counts summed across one or more responses, with the summed cost.
///
/// `total_tokens` is the sum of the five buckets. `total_usd_micros` stays
/// `None` until at least one summed response carried a cost.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BilledTokenCounts {
    pub input_tokens:       i64,
    pub output_tokens:      i64,
    pub total_tokens:       i64,
    #[serde(default)]
    pub reasoning_tokens:   i64,
    #[serde(default)]
    pub cache_read_tokens:  i64,
    #[serde(default)]
    pub cache_write_tokens: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_usd_micros:   Option<i64>,
}

impl BilledTokenCounts {
    #[must_use]
    pub fn from_token_counts(tokens: TokenCounts, total_usd_micros: Option<i64>) -> Self {
        Self {
            input_tokens: saturating_u64_to_i64(tokens.input),
            output_tokens: saturating_u64_to_i64(tokens.output),
            total_tokens: saturating_u64_to_i64(tokens.total()),
            reasoning_tokens: saturating_u64_to_i64(tokens.reasoning),
            cache_read_tokens: saturating_u64_to_i64(tokens.cache_read),
            cache_write_tokens: saturating_u64_to_i64(tokens.cache_write),
            total_usd_micros,
        }
    }

    #[must_use]
    pub fn from_billed_usage(billed: &[BilledModelUsage]) -> Self {
        let mut counts = Self::default();
        for entry in billed {
            counts.add_billed_usage(entry);
        }
        counts
    }

    /// Returns the five disjoint per-call token buckets, dropping the derived
    /// `total_tokens` sum and the optional `total_usd_micros` cost.
    #[must_use]
    pub fn token_counts(&self) -> TokenCounts {
        TokenCounts {
            input:       saturating_i64_to_u64(self.input_tokens),
            output:      saturating_i64_to_u64(self.output_tokens),
            reasoning:   saturating_i64_to_u64(self.reasoning_tokens),
            cache_read:  saturating_i64_to_u64(self.cache_read_tokens),
            cache_write: saturating_i64_to_u64(self.cache_write_tokens),
        }
    }

    pub fn add_counts(&mut self, source: &Self) {
        self.input_tokens = self.input_tokens.saturating_add(source.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(source.output_tokens);
        self.total_tokens = self.total_tokens.saturating_add(source.total_tokens);
        self.reasoning_tokens = self
            .reasoning_tokens
            .saturating_add(source.reasoning_tokens);
        self.cache_read_tokens = self
            .cache_read_tokens
            .saturating_add(source.cache_read_tokens);
        self.cache_write_tokens = self
            .cache_write_tokens
            .saturating_add(source.cache_write_tokens);
        accumulate_optional_usd_micros(&mut self.total_usd_micros, source.total_usd_micros);
    }

    pub fn add_billed_usage(&mut self, usage: &BilledModelUsage) {
        self.add_counts(&Self::from_token_counts(
            usage.tokens,
            usage.total_usd_micros,
        ));
    }

    pub fn replace_with_billed_usage(&mut self, usage: &BilledModelUsage) {
        *self = Self::from_billed_usage(std::slice::from_ref(usage));
    }

    /// Overrides the billed total with a reported cost; `None` leaves any
    /// existing value in place.
    #[must_use]
    pub fn with_reported_cost(mut self, cost: Option<UsdMicros>) -> Self {
        if let Some(cost) = cost {
            self.total_usd_micros = Some(cost.0);
        }
        self
    }

    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.input_tokens == 0
            && self.output_tokens == 0
            && self.total_tokens == 0
            && self.reasoning_tokens == 0
            && self.cache_read_tokens == 0
            && self.cache_write_tokens == 0
            && self.total_usd_micros.unwrap_or(0) == 0
    }
}

#[cfg(test)]
mod tests {
    use lithos_llm::types::CostSource;
    use serde_json::json;

    use super::*;

    fn tokens() -> TokenCounts {
        TokenCounts {
            input:       100,
            output:      20,
            reasoning:   5,
            cache_read:  7,
            cache_write: 3,
        }
    }

    fn model() -> ModelRef {
        ModelRef::new(
            ProviderId::new("anthropic"),
            ModelId::new("claude-sonnet-5"),
        )
    }

    #[test]
    fn usd_micros_from_usd_rounds_to_nearest_micro() {
        assert_eq!(UsdMicros::from_usd(0.012_345), UsdMicros(12_345));
        assert_eq!(UsdMicros::from_usd(1.0), UsdMicros(1_000_000));
        assert_eq!(UsdMicros::from_usd(f64::INFINITY), UsdMicros(i64::MAX));
    }

    #[test]
    fn usd_micros_from_cost_saturates() {
        let cost = Cost {
            usd_micros: u64::MAX,
            source:     CostSource::Provider,
        };
        assert_eq!(UsdMicros::from_cost(&cost), UsdMicros(i64::MAX));
    }

    #[test]
    fn accumulate_stays_none_without_costs() {
        let mut total = None;
        UsdMicros::accumulate(&mut total, None);
        assert_eq!(total, None);
        UsdMicros::accumulate(&mut total, Some(UsdMicros(5)));
        UsdMicros::accumulate(&mut total, None);
        UsdMicros::accumulate(&mut total, Some(UsdMicros(7)));
        assert_eq!(total, Some(UsdMicros(12)));
    }

    #[test]
    fn billed_token_counts_from_token_counts_sums_total() {
        let counts = BilledTokenCounts::from_token_counts(tokens(), Some(42));
        assert_eq!(counts.input_tokens, 100);
        assert_eq!(counts.output_tokens, 20);
        assert_eq!(counts.reasoning_tokens, 5);
        assert_eq!(counts.cache_read_tokens, 7);
        assert_eq!(counts.cache_write_tokens, 3);
        assert_eq!(counts.total_tokens, 135);
        assert_eq!(counts.total_usd_micros, Some(42));
        assert_eq!(counts.token_counts(), tokens());
    }

    #[test]
    fn billed_token_counts_sum_billed_usage_and_costs() {
        let priced = BilledModelUsage::new(
            model(),
            tokens(),
            Some(Cost {
                usd_micros: 10,
                source:     CostSource::Catalog,
            }),
        );
        let unpriced = BilledModelUsage::new(model(), tokens(), None);
        let counts = BilledTokenCounts::from_billed_usage(&[priced, unpriced]);
        assert_eq!(counts.input_tokens, 200);
        assert_eq!(counts.total_tokens, 270);
        assert_eq!(counts.total_usd_micros, Some(10));
    }

    #[test]
    fn billed_token_counts_without_costs_report_none() {
        let counts =
            BilledTokenCounts::from_billed_usage(&[BilledModelUsage::new(model(), tokens(), None)]);
        assert_eq!(counts.total_usd_micros, None);
        assert!(!counts.is_zero());
        assert!(BilledTokenCounts::default().is_zero());
    }

    #[test]
    fn billed_model_usage_serializes_lithos_token_buckets() {
        let usage = BilledModelUsage::new(model().with_speed(Some(Speed::Fast)), tokens(), None);
        let value = serde_json::to_value(&usage).unwrap();
        assert_eq!(
            value,
            json!({
                "model": {
                    "provider": "anthropic",
                    "model_id": "claude-sonnet-5",
                    "speed": "fast",
                },
                "tokens": {
                    "input": 100,
                    "output": 20,
                    "reasoning": 5,
                    "cache_read": 7,
                    "cache_write": 3,
                },
            })
        );
        let back: BilledModelUsage = serde_json::from_value(value).unwrap();
        assert_eq!(back, usage);
    }

    #[test]
    fn model_ref_hash_distinguishes_speed_tiers() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(model());
        set.insert(model().with_speed(Some(Speed::Fast)));
        set.insert(model().with_speed(Some(Speed::Fast)));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn model_ref_display_names_the_route_and_speed() {
        assert_eq!(model().to_string(), "anthropic/claude-sonnet-5");
        assert_eq!(
            model().with_speed(Some(Speed::Fast)).to_string(),
            "anthropic/claude-sonnet-5 (fast)"
        );
    }
}
