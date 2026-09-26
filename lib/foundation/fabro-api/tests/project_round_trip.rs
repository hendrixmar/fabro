use fabro_api::types::Project as ApiProject;
use fabro_automation::{GithubRepositoryId, Project, ProjectId, ProjectRevision};
use serde_json::json;

// Compile-time witness that the generated API type resolves to the domain
// type through `with_replacement(...)`. If progenitor stops reusing the
// domain type, this function stops type-checking and the build fails.
const _: fn(ApiProject) -> Project = |value| value;

#[test]
fn project_response_round_trips_public_json_shape() {
    let value = json!({
        "id": "tierrapay",
        "revision": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "name": "TierraPay",
        "github_repository_id": "812345678",
        "repository": "artesanos-digitales/tierrapay",
        "default_branch": "main",
        "intake_binding_id": "tierrapay"
    });

    let api: ApiProject = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(api).unwrap(), value);
}

#[test]
fn project_response_omits_absent_optional_fields() {
    let project = Project {
        id:                   ProjectId::new("mafeva").unwrap(),
        revision:             ProjectRevision::from_bytes(b"project"),
        name:                 "MAFEVA".to_string(),
        github_repository_id: GithubRepositoryId::new("42").unwrap(),
        repository:           "artesanos-digitales/mafeva".to_string(),
        default_branch:       "main".to_string(),
        intake_binding_id:    None,
    };

    let value = serde_json::to_value(project).unwrap();
    assert!(value.get("intake_binding_id").is_none());
}
