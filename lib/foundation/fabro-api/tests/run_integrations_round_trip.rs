//! JSON parity and type-identity tests for `RunIntegrationsGithubSettings`.
//!
//! Asserts that the API-side `RunIntegrationsGithubSettings` is the
//! canonical Rust resolved type (via `with_replacement` in `build.rs`) and
//! that both names round-trip through the same JSON shape. Covers the
//! populated and empty permissions cases as well as populated and empty
//! `additional_repositories` sets.

use fabro_api::types::{
    RunIntegrationsGithubSettings as ApiRunIntegrationsGithubSettings,
    RunIntegrationsSettings as ApiRunIntegrationsSettings,
};
use fabro_types::settings::run::{RunIntegrationsGithubSettings, RunIntegrationsSettings};
use serde_json::json;

/// Type-identity witnesses: the generated API names are the canonical Rust
/// types, not parallel DTOs. Compiles only when they are the same type.
#[expect(dead_code, reason = "compile-time type-identity witness")]
fn github_settings_type_identity(
    value: ApiRunIntegrationsGithubSettings,
) -> RunIntegrationsGithubSettings {
    value
}

#[expect(dead_code, reason = "compile-time type-identity witness")]
fn integrations_settings_type_identity(
    value: ApiRunIntegrationsSettings,
) -> RunIntegrationsSettings {
    value
}

#[test]
fn run_integrations_github_settings_round_trips_with_permissions() {
    let json_value = json!({
        "permissions": {
            "issues": "read",
            "contents": "write",
        }
    });

    let api: ApiRunIntegrationsGithubSettings =
        serde_json::from_value(json_value.clone()).expect("api type should parse");
    let canonical: RunIntegrationsGithubSettings =
        serde_json::from_value(json_value.clone()).expect("canonical type should parse");

    assert_eq!(serde_json::to_value(&api).unwrap(), json_value);
    assert_eq!(serde_json::to_value(&canonical).unwrap(), json_value);
}

#[test]
fn run_integrations_github_settings_round_trips_empty_permissions() {
    // Empty map is the resolved form of "no token requested" — must
    // serialize as an object, not omitted.
    let json_value = json!({ "permissions": {} });

    let api: ApiRunIntegrationsGithubSettings =
        serde_json::from_value(json_value.clone()).expect("api type should parse empty");
    let canonical: RunIntegrationsGithubSettings =
        serde_json::from_value(json_value.clone()).expect("canonical type should parse empty");

    assert_eq!(serde_json::to_value(&api).unwrap(), json_value);
    assert_eq!(serde_json::to_value(&canonical).unwrap(), json_value);
}

#[test]
fn run_integrations_github_settings_round_trips_additional_repositories() {
    let json_value = json!({
        "permissions": { "contents": "read" },
        "additional_repositories": ["fabro-sh/arc", "fabro-sh/keystone"],
    });

    let api: ApiRunIntegrationsGithubSettings =
        serde_json::from_value(json_value.clone()).expect("api type should parse repositories");
    let canonical: RunIntegrationsGithubSettings =
        serde_json::from_value(json_value.clone()).expect("canonical type should parse");

    assert_eq!(serde_json::to_value(&api).unwrap(), json_value);
    assert_eq!(serde_json::to_value(&canonical).unwrap(), json_value);
}

#[test]
fn run_integrations_github_settings_omits_an_empty_repository_set() {
    // An absent field and an explicit empty array both deserialize to the
    // empty set, and the empty set serializes back with the field omitted —
    // keeping single-repository settings byte-identical to older releases.
    let empty_array = json!({
        "permissions": {},
        "additional_repositories": [],
    });
    let omitted = json!({ "permissions": {} });

    let api: ApiRunIntegrationsGithubSettings =
        serde_json::from_value(empty_array).expect("api type should parse an empty array");
    let canonical: RunIntegrationsGithubSettings =
        serde_json::from_value(omitted.clone()).expect("canonical type should parse");

    assert!(api.additional_repositories.is_empty());
    assert_eq!(serde_json::to_value(&api).unwrap(), omitted);
    assert_eq!(serde_json::to_value(&canonical).unwrap(), omitted);
}

#[test]
fn run_integrations_settings_round_trips() {
    let json_value = json!({
        "github": {
            "permissions": {
                "issues": "read",
            }
        }
    });

    let api: ApiRunIntegrationsSettings =
        serde_json::from_value(json_value.clone()).expect("api wrapper should parse");
    let canonical: RunIntegrationsSettings =
        serde_json::from_value(json_value.clone()).expect("canonical wrapper should parse");

    assert_eq!(serde_json::to_value(&api).unwrap(), json_value);
    assert_eq!(serde_json::to_value(&canonical).unwrap(), json_value);
}
