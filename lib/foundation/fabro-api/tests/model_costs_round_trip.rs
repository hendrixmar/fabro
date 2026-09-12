use std::any::{TypeId, type_name};

use fabro_api::types::ModelCosts as ApiModelCosts;
use fabro_types::ModelCosts;

#[test]
fn model_costs_reuses_canonical_type() {
    assert_same_type::<ApiModelCosts, ModelCosts>();
}

#[test]
fn model_costs_json_matches_openapi_shape() {
    let costs = ModelCosts {
        input_cost_per_mtok:       Some(5.0),
        output_cost_per_mtok:      Some(25.0),
        cache_input_cost_per_mtok: Some(0.5),
    };

    let json = serde_json::to_value(costs).unwrap();
    assert_eq!(json["input_cost_per_mtok"], 5.0);
    assert_eq!(json["output_cost_per_mtok"], 25.0);
    assert_eq!(json["cache_input_cost_per_mtok"], 0.5);

    let round_trip: ApiModelCosts = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, costs);
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
