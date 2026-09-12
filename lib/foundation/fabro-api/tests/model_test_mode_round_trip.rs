use std::any::{TypeId, type_name};

use fabro_api::types::ModelTestMode as ApiModelTestMode;
use fabro_types::ModelTestMode;
use serde_json::json;

#[test]
fn model_test_mode_reuses_canonical_type() {
    assert_same_type::<ApiModelTestMode, ModelTestMode>();
}

#[test]
fn model_test_mode_json_matches_openapi_shape() {
    assert_eq!(
        serde_json::to_value(ModelTestMode::Basic).unwrap(),
        json!("basic")
    );
    assert_eq!(
        serde_json::to_value(ModelTestMode::Deep).unwrap(),
        json!("deep")
    );

    assert_eq!(
        serde_json::from_value::<ApiModelTestMode>(json!("deep")).unwrap(),
        ModelTestMode::Deep
    );
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
