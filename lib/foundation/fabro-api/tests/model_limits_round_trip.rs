use std::any::{TypeId, type_name};

use fabro_api::types::ModelLimits as ApiModelLimits;
use fabro_types::ModelLimits;

#[test]
fn model_limits_reuses_canonical_type() {
    assert_same_type::<ApiModelLimits, ModelLimits>();
}

#[test]
fn model_limits_json_matches_openapi_shape() {
    let limits = ModelLimits {
        context_window: 1_000_000,
        max_output:     Some(128_000),
    };

    let json = serde_json::to_value(limits).unwrap();
    assert_eq!(json["context_window"], 1_000_000);
    assert_eq!(json["max_output"], 128_000);

    let round_trip: ApiModelLimits = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, limits);
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
