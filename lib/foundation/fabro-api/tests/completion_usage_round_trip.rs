use std::any::{TypeId, type_name};

use fabro_api::types::CompletionUsage as ApiCompletionUsage;
use lithos_llm::types::TokenCounts;
use serde_json::json;

#[test]
fn completion_usage_reuses_canonical_type() {
    assert_same_type::<ApiCompletionUsage, TokenCounts>();
}

#[test]
fn completion_usage_json_matches_openapi_shape() {
    let usage = TokenCounts {
        input:       10,
        output:      20,
        reasoning:   3,
        cache_read:  4,
        cache_write: 5,
    };

    let json = serde_json::to_value(usage).unwrap();
    assert_eq!(
        json,
        json!({
            "input": 10,
            "output": 20,
            "reasoning": 3,
            "cache_read": 4,
            "cache_write": 5
        })
    );

    let round_trip: ApiCompletionUsage = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, usage);
}

#[test]
fn completion_usage_missing_buckets_default_to_zero() {
    let round_trip: ApiCompletionUsage = serde_json::from_value(json!({"input": 7})).unwrap();
    assert_eq!(round_trip, TokenCounts {
        input: 7,
        ..TokenCounts::default()
    });
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
