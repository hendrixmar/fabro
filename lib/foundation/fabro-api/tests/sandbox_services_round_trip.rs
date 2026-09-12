use std::any::{TypeId, type_name};

use fabro_api::types::{
    SandboxService as ApiSandboxService,
    SandboxServiceListResponse as ApiSandboxServiceListResponse,
};
use fabro_types::{SandboxService, SandboxServiceListResponse};
use serde_json::json;

#[test]
fn sandbox_services_reuse_domain_types() {
    assert_same_type::<ApiSandboxService, SandboxService>();
    assert_same_type::<ApiSandboxServiceListResponse, SandboxServiceListResponse>();
}

#[test]
fn sandbox_services_json_matches_openapi_shape() {
    let response = SandboxServiceListResponse {
        data: vec![SandboxService {
            port:              3000,
            addresses:         vec!["127.0.0.1:3000".to_string(), "[::]:3000".to_string()],
            processes:         vec!["node".to_string()],
            preview_supported: true,
        }],
    };

    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        json!({
            "data": [{
                "port": 3000,
                "addresses": ["127.0.0.1:3000", "[::]:3000"],
                "processes": ["node"],
                "preview_supported": true
            }]
        })
    );
}

#[test]
fn sandbox_services_deserializes_empty_response() {
    let response: SandboxServiceListResponse = serde_json::from_value(json!({ "data": [] }))
        .expect("empty service response should deserialize");

    assert!(response.data.is_empty());
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
