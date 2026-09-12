use std::any::{TypeId, type_name};

use fabro_api::types::{CompletionCost as ApiCost, CostSource as ApiCostSource};
use lithos_llm::types::{Cost, CostSource};
use serde_json::json;

#[test]
fn cost_types_reuse_lithos_types() {
    assert_same_type::<ApiCostSource, CostSource>();
    assert_same_type::<ApiCost, Cost>();
}

#[test]
fn cost_source_json_matches_openapi_shape() {
    for (source, wire) in [
        (CostSource::Catalog, "catalog"),
        (CostSource::Provider, "provider"),
        (CostSource::Application, "application"),
    ] {
        assert_eq!(serde_json::to_value(source).unwrap(), json!(wire));
        assert_eq!(
            serde_json::from_value::<ApiCostSource>(json!(wire)).unwrap(),
            source
        );
    }
}

#[test]
fn cost_json_matches_openapi_shape() {
    let cost = Cost {
        usd_micros: 125_000,
        source:     CostSource::Provider,
    };
    let json = serde_json::to_value(cost).unwrap();
    assert_eq!(json, json!({"usd_micros": 125000, "source": "provider"}));
    assert_eq!(serde_json::from_value::<ApiCost>(json).unwrap(), cost);
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
