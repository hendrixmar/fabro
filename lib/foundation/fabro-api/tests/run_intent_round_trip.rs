use std::any::{TypeId, type_name};
use std::collections::HashMap;

use fabro_api::types::{RunIntent as ApiRunIntent, RunIntentArgs as ApiRunIntentArgs};
use fabro_types::{GitRunTarget, RunIntent, RunIntentArgs, RunTarget, test_support};
use serde_json::json;

#[test]
fn run_intent_schemas_reuse_canonical_types() {
    assert_same_type::<ApiRunIntent, RunIntent>();
    assert_same_type::<ApiRunIntentArgs, RunIntentArgs>();
    assert_same_type::<fabro_api::types::RunTarget, RunTarget>();
    assert_same_type::<fabro_api::types::GitRunTarget, GitRunTarget>();
}

#[test]
fn run_intent_round_trips_the_openapi_shape() {
    let intent = RunIntent {
        workflow_version_id: test_support::test_workflow_version_id(),
        target:              RunTarget::Git(GitRunTarget {
            repo:   "fabro-sh/fabro".to_string(),
            branch: "feature/run-intent".to_string(),
            tag:    Some("v1.2.3".to_string()),
            sha:    Some("abcdef0123456789abcdef0123456789abcdef01".to_string()),
        }),
        args:                RunIntentArgs {
            model:            Some("gpt-5.6-sol".to_string()),
            provider:         Some("openai".to_string()),
            inputs:           HashMap::from([
                ("attempts".to_string(), json!(3)),
                ("ship".to_string(), json!(true)),
            ]),
            labels:           HashMap::from([("team".to_string(), "platform".to_string())]),
            dry_run:          Some(false),
            auto_approve:     Some(true),
            preserve_sandbox: Some(false),
        },
        environment_id:      Some("default".to_string()),
        parent_id:           None,
        title:               Some("Ship RunIntent".to_string()),
        goal:                Some("Create the run without starting it".to_string()),
    };

    let value = serde_json::to_value(&intent).unwrap();
    let api: ApiRunIntent = serde_json::from_value(value.clone()).unwrap();

    assert_eq!(value["args"]["dry_run"], false);
    assert_eq!(value["args"]["auto_approve"], true);
    assert_eq!(value["args"]["preserve_sandbox"], false);
    assert_eq!(serde_json::to_value(api).unwrap(), value);
}

#[test]
fn run_intent_none_target_round_trips_the_openapi_shape() {
    let intent = RunIntent {
        workflow_version_id: test_support::test_workflow_version_id(),
        target:              RunTarget::None {},
        args:                RunIntentArgs::default(),
        environment_id:      Some("default".to_string()),
        parent_id:           None,
        title:               None,
        goal:                None,
    };

    let value = serde_json::to_value(&intent).unwrap();
    let api: ApiRunIntent = serde_json::from_value(value.clone()).unwrap();

    assert_eq!(value["target"], json!({ "kind": "none" }));
    assert_eq!(serde_json::to_value(api).unwrap(), value);
}

#[test]
fn run_intent_folder_target_round_trips_the_openapi_shape() {
    let intent = RunIntent {
        workflow_version_id: test_support::test_workflow_version_id(),
        target:              RunTarget::Folder {
            path: "/srv/fabro/workspaces/example".to_string(),
        },
        args:                RunIntentArgs::default(),
        environment_id:      Some("local".to_string()),
        parent_id:           None,
        title:               None,
        goal:                None,
    };

    let value = serde_json::to_value(&intent).unwrap();
    let api: ApiRunIntent = serde_json::from_value(value.clone()).unwrap();

    assert_eq!(
        value["target"],
        json!({
            "kind": "folder",
            "path": "/srv/fabro/workspaces/example",
        })
    );
    assert_eq!(serde_json::to_value(api).unwrap(), value);
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
