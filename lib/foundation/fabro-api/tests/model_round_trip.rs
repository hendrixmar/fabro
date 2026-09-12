use std::any::{TypeId, type_name};

use fabro_api::types::{Model as ApiModel, ModelControls as ApiModelControls};
use fabro_types::{Model, ModelControls, ModelCosts, ModelFeatures, ModelLimits};
use lithos_llm::catalog::builtin;
use lithos_llm::types::ReasoningEffort;

#[test]
fn model_reuses_canonical_type() {
    assert_same_type::<ApiModel, Model>();
    assert_same_type::<ApiModelControls, ModelControls>();
}

#[test]
fn model_json_matches_openapi_shape() {
    let model = Model {
        id:                   "claude-opus-4.7".into(),
        provider:             builtin::anthropic(),
        family:               "claude-4".to_string(),
        display_name:         "Claude Opus 4.7".to_string(),
        limits:               ModelLimits {
            context_window: 1_000_000,
            max_output:     Some(128_000),
        },
        training:             Some("2025-08-01".to_string()),
        knowledge_cutoff:     Some("May 2025".to_string()),
        features:             ModelFeatures {
            tools:        true,
            vision:       true,
            reasoning:    true,
            prompt_cache: true,
            sampling:     true,
        },
        controls:             ModelControls {
            reasoning_effort: vec![
                ReasoningEffort::Low,
                ReasoningEffort::High,
                ReasoningEffort::Max,
            ],
        },
        costs:                ModelCosts {
            input_cost_per_mtok:       Some(5.0),
            output_cost_per_mtok:      Some(25.0),
            cache_input_cost_per_mtok: Some(0.5),
        },
        estimated_output_tps: Some(25.0),
        aliases:              vec!["opus".to_string()],
        default:              false,
        small_default:        true,
        configured:           true,
    };

    let json = serde_json::to_value(&model).unwrap();
    assert_eq!(json["id"], "claude-opus-4.7");
    assert_eq!(json["provider"], "anthropic");
    assert_eq!(json["knowledge_cutoff"], "May 2025");
    assert_eq!(json["features"]["prompt_cache"], true);
    assert_eq!(json["features"]["sampling"], true);
    assert_eq!(
        json["controls"]["reasoning_effort"],
        serde_json::json!(["low", "high", "max"])
    );
    assert_eq!(json["estimated_output_tps"], 25.0);
    assert_eq!(json["small_default"], true);
    assert_eq!(json["configured"], true);

    let round_trip: ApiModel = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, model);
}

fn assert_same_type<T: 'static, U: 'static>() {
    assert_eq!(
        TypeId::of::<T>(),
        TypeId::of::<U>(),
        "{} should be the same type as {}",
        type_name::<T>(),
        type_name::<U>()
    );
}
