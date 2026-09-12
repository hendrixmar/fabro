// Integration regression for the native run tool using production Git setup.
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use chrono::{TimeZone, Utc};
use fabro_tool::fabro_client::ClientBackend;
use fabro_types::{
    GitRunTarget, Run, RunId, RunLifecycle, RunLinks, RunOrigin, RunProjection, RunStatus,
    RunTarget, RunTimestamps, WorkflowRef, WorkflowVersionId, test_support,
};
use httpmock::Method::{GET, POST};
use httpmock::{HttpMockRequest, HttpMockResponse, MockServer};
use serde_json::json;
use tokio::fs;
#[expect(
    clippy::disallowed_methods,
    reason = "test fixture setup uses the Git CLI against an isolated temporary repository"
)]
fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("git command should run");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn test_parent(target: Option<RunTarget>) -> RunProjection {
    let mut spec = fabro_types::test_support::test_run_spec();
    spec.target = target;
    RunProjection::new(String::new(), spec, chrono::Utc::now())
}

async fn mock_parent<'a>(server: &'a MockServer, parent: &RunProjection) -> httpmock::Mock<'a> {
    let path = format!("/api/v1/runs/{}/state", parent.spec.id());
    let body = serde_json::to_value(parent).unwrap();
    server
        .mock_async(move |when, then| {
            when.method(GET).path(path);
            then.status(200).json_body(body);
        })
        .await
}

#[tokio::test]
async fn run_create_child_checkout_contains_the_parents_pushed_work() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("parent");
    let origin = temp.path().join("origin.git");
    fs::create_dir(&workspace).await.unwrap();
    run_git(temp.path(), &[
        "init",
        "--bare",
        "--quiet",
        origin.to_str().unwrap(),
    ]);
    run_git(&workspace, &["init", "--quiet", "--initial-branch", "main"]);
    run_git(&workspace, &["config", "user.name", "Fabro Test"]);
    run_git(&workspace, &["config", "user.email", "fabro@example.com"]);
    fs::write(workspace.join("result.txt"), "original")
        .await
        .unwrap();
    run_git(&workspace, &["add", "."]);
    run_git(&workspace, &["commit", "--quiet", "-m", "initial"]);
    run_git(&workspace, &[
        "remote",
        "add",
        "origin",
        origin.to_str().unwrap(),
    ]);
    run_git(&workspace, &["push", "--quiet", "origin", "main"]);
    let base_sha = fabro_workflow::git::head_sha(&workspace).unwrap();
    let mut parent = test_parent(Some(RunTarget::Git(GitRunTarget {
        repo:   "acme/widgets".to_owned(),
        branch: "main".to_owned(),
        tag:    Some("v1.0.0".to_owned()),
        sha:    Some(base_sha),
    })));
    let sandbox = fabro_sandbox::local_sandbox(&workspace).await.unwrap();
    // Docker and Daytona use this same setup operation to create the run branch.
    let git = fabro_sandbox::setup_git(&sandbox, &fabro_sandbox::GitSetupIntent::NewRun {
        run_id: parent.spec.id().to_string(),
    })
    .await
    .unwrap();
    parent.start = Some(fabro_types::StartRecord {
        start_time: chrono::Utc::now(),
        run_branch: Some(git.run_branch.clone()),
        base_sha:   Some(git.base_sha),
    });
    fs::write(workspace.join("result.txt"), "parent implementation")
        .await
        .unwrap();
    run_git(&workspace, &["add", "."]);
    run_git(&workspace, &["commit", "--quiet", "-m", "implement"]);
    run_git(&workspace, &["push", "--quiet", "origin", &git.run_branch]);
    let server = MockServer::start_async().await;
    let state_request = mock_parent(&server, &parent).await;
    let client = fabro_client::Client::new_no_proxy(&server.url("")).unwrap();
    let workflow_version_id: WorkflowVersionId = fabro_types::BlobHash::new(b"stored").into();
    let child_id = RunId::new();
    let submitted = Arc::new(Mutex::new(Vec::<fabro_types::RunIntent>::new()));
    let captured = Arc::clone(&submitted);
    server
        .mock_async(move |when, then| {
            when.method(POST).path("/api/v1/runs");
            then.respond_with(move |request: &HttpMockRequest| {
                captured
                    .lock()
                    .unwrap()
                    .push(serde_json::from_str(&request.body_string()).unwrap());
                HttpMockResponse::builder()
                    .status(201)
                    .header("content-type", "application/json")
                    .body(serde_json::to_string(&run(child_id, None, 0)).unwrap())
                    .build()
            });
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(GET).path(format!("/api/v1/runs/{child_id}"));
            then.status(200)
                .json_body_obj(&run(child_id, Some(parent.spec.id()), 0));
        })
        .await;
    let params = serde_json::from_value(
        json!({"runs":[{"workflow_version_id":workflow_version_id,"start":false}]}),
    )
    .unwrap();
    fabro_tool::create_runs_with_options(
        Arc::new(ClientBackend::new(Arc::new(client))),
        params,
        fabro_tool::CreateRunOptions {
            forced_parent_id: Some(parent.spec.id()),
        },
    )
    .await
    .unwrap();
    state_request.assert_calls_async(1).await;
    let intents = submitted.lock().unwrap().clone();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].workflow_version_id, workflow_version_id);
    assert_eq!(intents[0].parent_id, Some(parent.spec.id()));
    let RunTarget::Git(target) = &intents[0].target else {
        panic!("child should have a Git target")
    };
    assert_eq!(target.repo, "acme/widgets");
    assert_eq!(target.sha, None);
    assert_eq!(target.tag, None);
    let child = temp.path().join("child");
    run_git(temp.path(), &[
        "clone",
        "--quiet",
        "--branch",
        &target.branch,
        origin.to_str().unwrap(),
        child.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(child.join("result.txt")).await.unwrap(),
        "parent implementation"
    );
}

fn run(run_id: RunId, parent_id: Option<RunId>, children_count: u64) -> Run {
    run_with_status(run_id, parent_id, children_count, RunStatus::Submitted)
}

fn run_with_status(
    run_id: RunId,
    parent_id: Option<RunId>,
    children_count: u64,
    status: RunStatus,
) -> Run {
    Run {
        id: run_id,
        parent_id,
        children_count,
        title: "Test run".to_string(),
        goal: "Test run".to_string(),
        workflow: WorkflowRef {
            slug:       Some("simple".to_string()),
            name:       Some("Simple".to_string()),
            graph_name: None,
            node_count: 0,
            edge_count: 0,
        },
        automation: None,
        repository: None,
        created_by: test_support::test_principal(),
        origin: RunOrigin::default(),
        labels: HashMap::new(),
        lifecycle: RunLifecycle {
            status,
            approval: None,
            pending_control: None,
            queue_position: None,
            error: None,
            archived: false,
            archived_at: None,
        },
        sandbox: None,
        models: Vec::new(),
        source_directory: Some("/srv/repo".to_string()),
        timestamps: RunTimestamps {
            created_at:    Utc.with_ymd_and_hms(2026, 4, 5, 12, 0, 0).unwrap(),
            started_at:    None,
            last_event_at: None,
            completed_at:  None,
        },
        timing: None,
        billing: None,
        size: fabro_types::RunSize::default(),
        ask_fabro: fabro_types::AskFabro::default(),
        diff: None,
        pull_request: None,
        current_question: None,
        superseded_by: None,
        retried_from: None,
        links: RunLinks { web: None },
    }
}
