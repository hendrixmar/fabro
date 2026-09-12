//! API projections of the model catalog.
//!
//! `GET /models` and `GET /providers` return these. They are views over the
//! lithos catalog plus Fabro policy, stamped per request with whether the
//! server holds credentials for each provider.

use lithos_llm::catalog::{ModelId, ProviderId};
use lithos_llm::types::ReasoningEffort;
use serde::{Deserialize, Serialize};

/// Token limits for a model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelLimits {
    pub context_window: i64,
    pub max_output:     Option<i64>,
}

/// Capability flags for a model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFeatures {
    pub tools:        bool,
    pub vision:       bool,
    pub reasoning:    bool,
    pub prompt_cache: bool,
    /// Whether the model accepts classic sampling parameters
    /// (`temperature`, `top_p`).
    pub sampling:     bool,
}

/// Request-control values a model accepts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelControls {
    /// Reasoning-effort values accepted by this offering. Empty means the
    /// control is unsupported.
    #[serde(default)]
    pub reasoning_effort: Vec<ReasoningEffort>,
}

/// Pricing per million tokens in USD.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ModelCosts {
    pub input_cost_per_mtok:       Option<f64>,
    pub output_cost_per_mtok:      Option<f64>,
    pub cache_input_cost_per_mtok: Option<f64>,
}

/// One provider's offering of a model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Model {
    pub id:                   ModelId,
    pub provider:             ProviderId,
    pub family:               String,
    pub display_name:         String,
    pub limits:               ModelLimits,
    pub training:             Option<String>,
    pub knowledge_cutoff:     Option<String>,
    pub features:             ModelFeatures,
    #[serde(default)]
    pub controls:             ModelControls,
    pub costs:                ModelCosts,
    pub estimated_output_tps: Option<f64>,
    pub aliases:              Vec<String>,
    #[serde(default)]
    pub default:              bool,
    #[serde(default)]
    pub small_default:        bool,
    /// Whether the server holds credential material for this model's
    /// provider. Stamped per request; never implies the credential works.
    #[serde(default)]
    pub configured:           bool,
}

/// An LLM provider with effective configuration and configured status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provider {
    pub id:                   ProviderId,
    pub display_name:         String,
    /// lithos adapter id, such as `openai` or `openai-compatible`.
    pub adapter:              String,
    pub base_url:             String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_url:          Option<String>,
    pub priority:             i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases:              Vec<String>,
    pub model_count:          u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model:        Option<String>,
    #[serde(default)]
    pub configured:           bool,
    /// Vault secret an operator creates to configure this provider, when the
    /// provider reads one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_secret_name: Option<String>,
}
