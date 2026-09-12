use std::any::{TypeId, type_name};

use fabro_api::types::{ModelHandle as ApiModelHandle, ProviderId as ApiProviderId};
use lithos_llm::catalog::{ModelHandle, ModelId, ProviderId, builtin};
use serde_json::json;

#[test]
fn provider_id_and_model_handle_reuse_lithos_types() {
    assert_same_type::<ApiProviderId, ProviderId>();
    assert_same_type::<ApiModelHandle, ModelHandle>();
}

#[test]
fn provider_id_json_is_a_bare_string() {
    assert_eq!(
        serde_json::to_value(builtin::anthropic()).unwrap(),
        json!("anthropic")
    );
    assert_eq!(
        serde_json::from_value::<ProviderId>(json!("venice")).unwrap(),
        ProviderId::new("venice")
    );
}

#[test]
fn model_handle_json_matches_openapi_shape() {
    let handle = ModelHandle::new(builtin::openai(), ModelId::new("gpt-5.4"));
    let json = serde_json::to_value(&handle).unwrap();
    assert_eq!(json, json!({"provider": "openai", "model": "gpt-5.4"}));
    let round_trip: ApiModelHandle = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, handle);
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
