use super::client::ScanProgress;
use super::*;

#[test]
fn repeated_events_are_not_new_work() {
    assert!(!discovery_eligible(
        true,
        Some((false, false)),
        (false, false),
        10,
        0
    ));
    assert!(discovery_eligible(
        true,
        Some((true, false)),
        (false, false),
        10,
        0
    ));
    assert!(!discovery_eligible(
        true,
        Some((true, false)),
        (true, false),
        10,
        0
    ));
    assert!(!discovery_eligible(false, None, (false, false), 10, 20));
    assert!(discovery_eligible(false, None, (false, false), 30, 20));
}

#[test]
fn cursor_cannot_change_coverage_or_repeat_after_restart() {
    let mut progress = ScanProgress::default();
    let next = "https://bugsink.example/api/canonical/0/issues/?project=25&sort=digest_order&order=asc&cursor=abc";
    progress
        .advance("https://bugsink.example", 25, Some(next))
        .unwrap();
    let mut resumed: ScanProgress =
        serde_json::from_str(&serde_json::to_string(&progress).unwrap()).unwrap();
    assert!(
        resumed
            .advance("https://bugsink.example", 25, Some(next))
            .is_err()
    );
    assert!(resumed.advance("https://evil.example/api/canonical/0/issues/?project=25&sort=digest_order&order=asc&cursor=def", 25, Some(next)).is_err());
    assert!(
        resumed
            .advance("https://bugsink.example", 26, Some(next))
            .is_err()
    );
}

#[test]
fn read_budget_parks_on_fifth_failure() {
    assert_eq!(retry_deadline(1, 100), Some(15100));
    assert_eq!(retry_deadline(2, 100), Some(30100));
    assert_eq!(retry_deadline(3, 100), Some(60100));
    assert_eq!(retry_deadline(4, 100), Some(120100));
    assert_eq!(retry_deadline(5, 100), None);
}

use std::sync::Arc;

use fabro_types::{AutomationRef, Principal, RunId, RunStatus, SystemActorKind};
use serde_json::json;

use super::super::AppState;
use crate::automation_materializer::TestAutomationRunMaterializer;
use crate::test_support::{TestAppStateBuilder, default_test_server_settings, test_store_bundle};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
const ORIGIN: &str = "https://bugsink.example";

fn manifest() -> fabro_api::types::RunManifest {
    serde_json::from_value(json!({
        "version":1,"cwd":"/tmp","target":{"path":"workflow.fabro"},
        "workflows":{"workflow.fabro":{"source":"digraph Incident { graph [goal=\"test\"]; start [shape=Mdiamond]; investigate [shape=parallelogram,script=\"true\",goal_gate=true,output_schema=\"routing\"]; exit [shape=Msquare]; start -> investigate -> exit; }","files":{}}},
        "git":{"origin_url":"https://github.com/example/repository.git","branch":REVISION,"sha":REVISION,"dirty":"clean"},
        "args":{"input":["incident=untrusted","episode=999","run_id=untrusted"]}
    })).unwrap()
}

fn build(
    dir: &std::path::Path,
    bundle: &(Arc<fabro_store::Database>, fabro_store::ArtifactStore),
    origin: &str,
    enabled: bool,
    dispatch: bool,
) -> Arc<AppState> {
    let mut settings = default_test_server_settings();
    settings.server.integrations.plane.api_base = Some(format!("{origin}/api/v1"));
    settings.server.integrations.plane.workspace = Some("workspace".into());
    settings.server.integrations.bugsink =
        fabro_types::settings::server::BugsinkIntegrationSettings {
            enabled,
            dispatch_enabled: dispatch,
            origin: Some(origin.into()),
            api_token_secret: Some("BUGSINK_API_TOKEN".into()),
            projects: vec![
                fabro_types::settings::server::BugsinkProjectSettings {
                    project_id:     25,
                    automation_id:  "incident-loop".into(),
                    signing_secret: "BUGSINK_SIGNING_25".into(),
                },
                fabro_types::settings::server::BugsinkProjectSettings {
                    project_id:     26,
                    automation_id:  "incident-loop".into(),
                    signing_secret: "BUGSINK_SIGNING_26".into(),
                },
            ],
        };
    TestAppStateBuilder::new()
        .runtime_settings(settings, fabro_config::RunLayer::default())
        .vault_path(dir.join("secrets.json"))
        .active_config_path(dir.join("settings.toml"))
        .vault_entries([
            ("BUGSINK_API_TOKEN", "fixture"),
            ("BUGSINK_SIGNING_25", "fixture-25"),
            ("BUGSINK_SIGNING_26", "fixture-26"),
            (fabro_static::EnvVars::OPENAI_API_KEY, "fixture-openai"),
            (fabro_static::EnvVars::PLANE_API_KEY, "fixture-plane"),
        ])
        .store_bundle(Arc::clone(&bundle.0), bundle.1.clone())
        .automation_materializer(TestAutomationRunMaterializer::succeed(
            manifest(),
            b"stale submitted bytes".to_vec(),
        ))
        .build()
}

async fn automation(state: &AppState) {
    state.automation_store().create(serde_json::from_value(json!({
        "id":"incident-loop","name":"Incident loop","target":{"repository":"example/repository","ref":REVISION,"workflow":"incident-loop"},"triggers":[]
    })).unwrap()).await.unwrap();
}

async fn ready_observation(state: &AppState) -> (String, uuid::Uuid) {
    let issue = uuid::Uuid::new_v4();
    let event = uuid::Uuid::new_v4();
    let store = state.incident_store();
    store.start_baseline(ORIGIN, &[25, 26], 0).await.unwrap();
    sqlx::query("UPDATE bugsink_scans SET baseline_complete=1")
        .execute(&store.pool)
        .await
        .unwrap();
    let alert = AcceptedAlert {
        project_id:  25,
        issue_id:    issue,
        reason:      AlertReason::New,
        body_digest: "a".repeat(64),
        received_ms: 10,
    };
    store.accept(ORIGIN, &alert).await.unwrap();
    let pending = store.pending(ORIGIN, 10).await.unwrap().remove(0);
    store
        .apply_observation(
            ORIGIN,
            &pending,
            &client::Issue {
                project:       25,
                id:            issue,
                resolved:      false,
                muted:         false,
                first_seen_ms: 10,
            },
            event,
        )
        .await
        .unwrap();
    (pending.key, event)
}

#[tokio::test]
async fn real_materialized_run_contains_only_trusted_five_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let initial = build(dir.path(), &bundle, ORIGIN, false, false);
    automation(&initial).await;
    drop(initial);
    let state = build(dir.path(), &bundle, ORIGIN, true, true);
    ready_observation(&state).await;
    let id = state
        .incident_store()
        .reserve(ORIGIN, 25, REVISION)
        .await
        .unwrap()
        .unwrap();
    reconcile_run(&state, id).await.unwrap();
    let intent = state.incident_store().intent(id).await.unwrap();
    let run = state.stores.runs.open_run(&id).await.unwrap();
    let projection = run.state().await.unwrap();
    assert_eq!(projection.status, RunStatus::Runnable);
    for (key, value) in intent.inputs() {
        assert_eq!(
            projection
                .spec
                .settings
                .run
                .inputs
                .get(&key)
                .and_then(toml::Value::as_str),
            Some(value.as_str())
        );
    }
    let blob = run
        .read_blob(projection.spec.manifest_blob.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let persisted: fabro_api::types::RunManifest = serde_json::from_slice(&blob).unwrap();
    assert_eq!(persisted.args.unwrap().input.len(), 5);
    for event in [
        fabro_workflow::event::Event::RunStarting,
        fabro_workflow::event::Event::RunRunning,
    ] {
        fabro_workflow::event::append_event(&run, &id, &event)
            .await
            .unwrap();
    }
    let mut report = json!({"schema_version":1,"run_id":id.to_string(),"incident":intent.inputs()[0].1,
        "event":intent.event.to_string(),"observation":intent.observation,"episode":intent.episode,"status":"completed",
        "publication":{"status":"completed","project_id":uuid::Uuid::new_v4(),"issue_id":uuid::Uuid::new_v4(),"operation_key":"fixture-operation"},
        "evidence_digest":"e".repeat(64),"missing":[]});
    let checkpoint = fabro_workflow::event::Event::CheckpointCompleted {
        node_id: "investigate".into(),
        current_node: "investigate".into(),
        status: "succeeded".into(),
        completed_nodes: vec!["investigate".into()],
        node_retries: Default::default(),
        context_values: std::collections::BTreeMap::from([(
            "incident_result".into(),
            report.clone(),
        )]),
        node_outcomes: std::collections::BTreeMap::from([(
            "investigate".into(),
            fabro_types::Outcome::success(),
        )]),
        next_node_id: Some("exit".into()),
        git_commit_sha: None,
        loop_failure_signatures: Default::default(),
        restart_failure_signatures: Default::default(),
        node_visits: Default::default(),
        diff: None,
        diff_summary: None,
        graph_visit: Some(1),
        resumed_from_stage_id: None,
    };
    fabro_workflow::event::append_event(&run, &id, &checkpoint)
        .await
        .unwrap();
    fabro_workflow::event::append_event(
        &run,
        &id,
        &fabro_workflow::event::Event::WorkflowRunCompleted {
            timing:               fabro_types::RunTiming::wall_only(1),
            artifact_count:       0,
            status:               "succeeded".into(),
            reason:               fabro_types::SuccessReason::Completed,
            total_usd_micros:     None,
            final_git_commit_sha: None,
            final_patch:          None,
            diff_summary:         None,
            billing:              None,
        },
    )
    .await
    .unwrap();
    reconcile_run(&state, id).await.unwrap();
    assert_eq!(
        state.incident_store().intent(id).await.unwrap().state,
        "succeeded"
    );
    let mut projection = run.state().await.unwrap();
    let original = projection.clone();
    projection
        .checkpoints
        .last_mut()
        .unwrap()
        .checkpoint
        .node_outcomes
        .remove("investigate");
    assert!(worker::terminal_result(&intent, &projection).is_err());
    projection = original.clone();
    report["publication"] = json!({"status":"completed"});
    projection
        .checkpoints
        .last_mut()
        .unwrap()
        .checkpoint
        .context_values
        .insert("incident_result".into(), report.clone());
    assert!(worker::terminal_result(&intent, &projection).is_err());
    projection = original;
    report["publication"] = json!({"status":"no_ticket"});
    report["run_id"] = json!(RunId::new().to_string());
    projection
        .checkpoints
        .last_mut()
        .unwrap()
        .checkpoint
        .context_values
        .insert("incident_result".into(), report);
    assert!(worker::terminal_result(&intent, &projection).is_err());
}

#[tokio::test]
async fn submitted_run_resumes_same_identity_after_reconstruction() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let initial = build(dir.path(), &bundle, ORIGIN, false, false);
    automation(&initial).await;
    ready_observation(&initial).await;
    let id = initial
        .incident_store()
        .reserve(ORIGIN, 25, REVISION)
        .await
        .unwrap()
        .unwrap();
    let intent = initial.incident_store().intent(id).await.unwrap();
    let mut manifest = manifest();
    manifest.args.as_mut().unwrap().input = intent
        .inputs()
        .into_iter()
        .map(|(key, value)| format!("{key}={}", serde_json::to_string(&value).unwrap()))
        .collect();
    let bytes = serde_json::to_vec(&manifest).unwrap();
    assert!(
        initial
            .incident_store()
            .begin_create(&intent, &bytes)
            .await
            .unwrap()
    );
    let response = Box::pin(super::super::handler::runs::create_run_from_manifest(
        Arc::clone(&initial),
        super::super::handler::runs::CreateRunFromManifestRequest {
            manifest,
            submitted_manifest_bytes: bytes,
            explicit_run_id: Some(id),
            explicit_title_supplied: true,
            actor: Principal::System {
                system_kind: SystemActorKind::Engine,
            },
            headers: axum::http::HeaderMap::new(),
            automation: Some(AutomationRef {
                id:         "incident-loop".into(),
                name:       None,
                trigger_id: None,
            }),
        },
    ))
    .await;
    assert_eq!(response.status(), axum::http::StatusCode::CREATED);
    assert_eq!(
        initial
            .stores
            .runs
            .open_run(&id)
            .await
            .unwrap()
            .state()
            .await
            .unwrap()
            .status,
        RunStatus::Submitted
    );
    drop(initial);
    let resumed = build(dir.path(), &bundle, ORIGIN, false, false);
    reconcile_run(&resumed, id).await.unwrap();
    assert_eq!(
        resumed
            .stores
            .runs
            .open_run(&id)
            .await
            .unwrap()
            .state()
            .await
            .unwrap()
            .status,
        RunStatus::Runnable
    );
    assert_eq!(resumed.incident_store().active_runs().await.unwrap(), vec![
        id
    ]);
    assert!(
        resumed
            .incident_store()
            .reserve(ORIGIN, 26, REVISION)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn created_only_run_and_missing_create_response_never_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let state = build(dir.path(), &bundle, ORIGIN, false, false);
    automation(&state).await;
    ready_observation(&state).await;
    let id = state
        .incident_store()
        .reserve(ORIGIN, 25, REVISION)
        .await
        .unwrap()
        .unwrap();
    let intent = state.incident_store().intent(id).await.unwrap();
    state
        .incident_store()
        .begin_create(&intent, b"attempted manifest")
        .await
        .unwrap();
    // Even an authoritative missing Run cannot authorize recreating an attempted
    // create.
    reconcile_run(&state, id).await.unwrap();
    assert_eq!(
        state.incident_store().intent(id).await.unwrap().state,
        "uncertain"
    );
    let run = state.stores.runs.create_run(&id).await.unwrap();
    fabro_workflow::event::append_event(&run, &id, &fabro_workflow::event::Event::RunCreated {
        run_id:           id,
        title:            None,
        settings:         serde_json::to_value(fabro_types::WorkflowSettings::default()).unwrap(),
        graph:            serde_json::to_value(fabro_types::Graph::new("partial")).unwrap(),
        workflow_source:  None,
        labels:           std::collections::BTreeMap::new(),
        source_directory: None,
        workflow_slug:    None,
        automation:       None,
        provenance:       fabro_types::test_support::test_run_provenance(),
        manifest_blob:    None,
        git:              None,
        fork_source_ref:  None,
        retried_from:     None,
        parent_id:        None,
        web_url:          None,
    })
    .await
    .unwrap();
    drop(state);
    let resumed = build(dir.path(), &bundle, ORIGIN, false, false);
    reconcile_run(&resumed, id).await.unwrap();
    let intent = resumed.incident_store().intent(id).await.unwrap();
    assert_eq!(intent.state, "uncertain");
    assert_eq!(run.list_events().await.unwrap().len(), 1);
    assert!(
        resumed
            .incident_store()
            .reserve(ORIGIN, 25, REVISION)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn captured_generation_survives_restart_and_poll_never_resets_parked_budget() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let state = build(dir.path(), &bundle, ORIGIN, false, false);
    let alert = AcceptedAlert {
        project_id:  25,
        issue_id:    uuid::Uuid::new_v4(),
        reason:      AlertReason::New,
        body_digest: "a".repeat(64),
        received_ms: 10,
    };
    state.incident_store().accept(ORIGIN, &alert).await.unwrap();
    let first = state
        .incident_store()
        .pending(ORIGIN, 10)
        .await
        .unwrap()
        .remove(0);
    drop(state);
    let resumed = build(dir.path(), &bundle, ORIGIN, false, false);
    let pending = resumed
        .incident_store()
        .pending(ORIGIN, 10)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(pending.generation, first.generation);
    let mut now = 10;
    for attempt in 1..=5 {
        let pending = resumed
            .incident_store()
            .pending(ORIGIN, now)
            .await
            .unwrap()
            .remove(0);
        resumed
            .incident_store()
            .read_failed(&pending, now, "authoritative_read_failed")
            .await
            .unwrap();
        if let Some(next) = retry_deadline(attempt, now) {
            now = next;
        }
    }
    let mut newer = alert.clone();
    newer.body_digest = "b".repeat(64);
    resumed
        .incident_store()
        .accept(ORIGIN, &newer)
        .await
        .unwrap();
    resumed
        .incident_store()
        .scan_issue(
            ORIGIN,
            &client::Issue {
                project:       25,
                id:            alert.issue_id,
                resolved:      false,
                muted:         false,
                first_seen_ms: 10,
            },
            true,
            0,
        )
        .await
        .unwrap();
    assert!(
        resumed
            .incident_store()
            .pending(ORIGIN, now + 1_000_000)
            .await
            .unwrap()
            .is_empty()
    );
    let snapshot = resumed.incident_store().snapshot().await.unwrap();
    assert_eq!(snapshot["incidents"][0]["read_attempts"], 5);
    assert_eq!(snapshot["incidents"][0]["requested_generation"], 2);
    assert_eq!(snapshot["incidents"][0]["applied_generation"], 0);
}

#[tokio::test]
async fn scans_more_than_ten_issues_across_restart_without_backlog_dispatch() {
    let remote = httpmock::MockServer::start();
    let origin = remote.base_url();
    let boundary = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let first:Vec<_>=(0..11).map(|_|json!({"id":uuid::Uuid::new_v4(),"project":25,"is_resolved":false,"is_muted":false,"first_seen":"2020-01-01T00:00:00Z"})).collect();
    let last_id = uuid::Uuid::new_v4();
    let first_page=remote.mock(|when,then| {
        when.method(httpmock::Method::GET).path(client::ISSUES_PATH).query_param("project","25").query_param_missing("cursor");
        then.json_body(json!({"results":first,"next":format!("{origin}/api/canonical/0/issues/?project=25&sort=digest_order&order=asc&cursor=page2")}));
    });
    let second_page=remote.mock(|when,then| {
        when.method(httpmock::Method::GET).path(client::ISSUES_PATH).query_param("cursor","page2");
        then.json_body(json!({"results":[{"id":last_id,"project":25,"is_resolved":false,"is_muted":false,"first_seen":"2026-01-01T00:00:00.001Z"}],"next":null}));
    });
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let state = build(dir.path(), &bundle, &origin, false, false);
    state
        .incident_store()
        .start_baseline(&origin, &[25], boundary)
        .await
        .unwrap();
    let progress = ScanProgress {
        import_complete: true,
        ..ScanProgress::default()
    };
    sqlx::query("UPDATE bugsink_scans SET cursor=?")
        .bind(serde_json::to_string(&progress).unwrap())
        .execute(&state.incident_store().pool)
        .await
        .unwrap();
    worker::scan_once(
        &state,
        &client::BugsinkClient::new(&state).await.unwrap(),
        boundary,
    )
    .await
    .unwrap();
    assert_eq!(first_page.calls(), 1);
    assert_eq!(second_page.calls(), 0);
    drop(state);
    let resumed = build(dir.path(), &bundle, &origin, false, false);
    worker::scan_once(
        &resumed,
        &client::BugsinkClient::new(&resumed).await.unwrap(),
        boundary + 1,
    )
    .await
    .unwrap();
    let snapshot = resumed.incident_store().snapshot().await.unwrap();
    assert_eq!(snapshot["incidents"].as_array().unwrap().len(), 12);
    let pending: Vec<_> = snapshot["incidents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["status"] == "pending")
        .collect();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["issue_id"], last_id.to_string());
    assert_eq!(snapshot["scans"][0]["baseline_complete"], true);
    worker::scan_once(
        &resumed,
        &client::BugsinkClient::new(&resumed).await.unwrap(),
        boundary + 2,
    )
    .await
    .unwrap();
    assert_eq!(first_page.calls(), 1);
    assert_eq!(second_page.calls(), 1);
}

#[test]
fn legacy_marker_is_whole_paragraph_not_a_substring_or_comment() {
    let key = "bugsink:25:11111111-1111-4111-8111-111111111111";
    assert_eq!(
        legacy::html_incidents(&format!("<p>incident: {key}</p>")).unwrap(),
        vec![key]
    );
    assert!(
        legacy::html_incidents(&format!(
            "<!--<p>incident: {key}</p>--><p>related {key}</p><p>incident: {key} extra</p>"
        ))
        .is_err()
    );
    assert!(
        legacy::html_incidents(&format!(
            "<!--<p>incident: {key}</p>--><p>related {key}</p>"
        ))
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn extra_run_is_explicit_audited_and_cannot_bypass_uncertainty() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let state = build(dir.path(), &bundle, ORIGIN, false, false);
    let (key, event) = ready_observation(&state).await;
    let store = state.incident_store();
    let first = store.reserve(ORIGIN, 25, REVISION).await.unwrap().unwrap();
    let first = store.intent(first).await.unwrap();
    store
        .accept(ORIGIN, &AcceptedAlert {
            project_id:  25,
            issue_id:    first.issue,
            reason:      AlertReason::Regressed,
            body_digest: "b".repeat(64),
            received_ms: 20,
        })
        .await
        .unwrap();
    store
        .transition_run(&first, "failed", Some("investigation_failed"))
        .await
        .unwrap();
    assert!(store.pending(ORIGIN, 20).await.unwrap().is_empty());
    let second = store.reserve(ORIGIN, 25, REVISION).await.unwrap().unwrap();
    let second = store.intent(second).await.unwrap();
    assert_eq!(first.observation, second.observation);
    store
        .transition_run(&second, "failed", Some("investigation_failed"))
        .await
        .unwrap();
    assert!(store.reserve(ORIGIN, 25, REVISION).await.unwrap().is_none());
    let issue = client::Issue {
        project:       25,
        id:            first.issue,
        resolved:      false,
        muted:         false,
        first_seen_ms: 10,
    };
    store
        .operator_retry(ORIGIN, &issue, event, "operator", false, REVISION, 1, 30)
        .await
        .unwrap();
    assert!(store.reserve(ORIGIN, 25, REVISION).await.unwrap().is_none());
    store
        .operator_retry(ORIGIN, &issue, event, "operator", true, REVISION, 1, 31)
        .await
        .unwrap();
    let third = store.active_runs().await.unwrap()[0];
    let row: (i64, Option<String>) =
        sqlx::query_as("SELECT attempt,authorized_by FROM bugsink_runs WHERE run_id=?")
            .bind(third.to_string())
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(row, (3, Some("operator".into())));
    assert_eq!(
        store.snapshot().await.unwrap()["incidents"][0]["requested_generation"],
        2
    );
    assert_eq!(
        store.snapshot().await.unwrap()["incidents"][0]["applied_generation"],
        1
    );
    let intent = store.intent(third).await.unwrap();
    store
        .transition_run(&intent, "uncertain", Some("create_response_uncertain"))
        .await
        .unwrap();
    assert!(
        store
            .operator_retry(ORIGIN, &issue, event, "operator", true, REVISION, 2, 32)
            .await
            .is_err()
    );
    let raw: String = sqlx::query_scalar("SELECT cursor FROM bugsink_scans WHERE project_id=25")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let audit: ScanProgress = serde_json::from_str(&raw).unwrap();
    assert_eq!(audit.operator_audit.last().unwrap()["operator"], "operator");
    assert_eq!(
        store.snapshot().await.unwrap()["incidents"][0]["incident_key"],
        key
    );
}

#[tokio::test]
async fn baseline_import_reads_actual_sources_and_exposes_only_verified_summary() {
    use std::os::unix::fs::PermissionsExt;
    let remote = httpmock::MockServer::start();
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let state = build(dir.path(), &bundle, &remote.base_url(), false, false);
    let issue = uuid::Uuid::new_v4();
    let incident = format!("bugsink:25:{issue}");
    let project = uuid::Uuid::new_v4();
    let ticket = uuid::Uuid::new_v4();
    let source = dir.path().join("legacy.json");
    std::fs::write(&source, b"{}").unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
    let config = dir.path().join("bugsink-legacy.json");
    let owner = json!({"state_file":source,"api_origin":remote.base_url(),"token_secret":"BUGSINK_API_TOKEN"});
    std::fs::write(&config,serde_json::to_vec(&json!({"schema_version":1,"owners":{"laptop":owner,"el-telar":owner},"plane_project_id":project})).unwrap()).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    remote.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/v1/automations");
        then.json_body(json!({"data":[]}));
    });
    remote.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/api/v1/runs");
        then.json_body(json!({"data":[],"meta":{"has_more":false,"total":0}}));
    });
    remote.mock(|when,then| {
        when.method(httpmock::Method::GET).path(format!("/api/v1/workspaces/workspace/projects/{project}/issues/"));
        then.json_body(json!({"results":[{"id":ticket,"project":project,"description_html":format!("<p>incident: {incident}</p>")}],"next_page_results":false}));
    });
    let summary = legacy::import(&state).await.unwrap();
    assert_eq!(summary["status"], "reconciled");
    assert!(
        summary["records"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["ticket_id"] == ticket.to_string() && r["run_id"].is_null())
    );
    std::fs::write(
        &source,
        serde_json::to_vec(&json!({(incident.clone()):{"status":"spawn-failed","run":null}}))
            .unwrap(),
    )
    .unwrap();
    let incomplete = legacy::import(&state).await.unwrap();
    assert_eq!(incomplete["status"], "incomplete");
    assert_eq!(
        incomplete["owners"]["laptop"]["in_flight_reconciled"],
        false
    );
    let snapshot = state.incident_store().snapshot().await.unwrap();
    assert_eq!(
        snapshot["incidents"][0]["parked_reason"],
        "legacy_spawn_uncertain"
    );
    assert!(snapshot["runs"].as_array().unwrap().is_empty());
    std::fs::write(&source, b"{}").unwrap();
    assert_eq!(
        legacy::import(&state).await.unwrap()["status"],
        "incomplete"
    );
    std::fs::remove_file(&source).unwrap();
    assert!(legacy::import(&state).await.is_err());
    assert_eq!(
        state.incident_store().snapshot().await.unwrap()["incidents"],
        snapshot["incidents"]
    );
}

#[tokio::test]
async fn owner_snapshots_can_exceed_provider_payloads_without_unbounded_reads() {
    let remote = httpmock::MockServer::start();
    remote.mock(|when, then| {
        when.path("/run");
        then.json_body(json!({"checkpoint":{"output":"x".repeat(300_000)}}));
    });
    let http = client::http_client().unwrap();
    let response = http.get(remote.url("/run")).send().await.unwrap();
    let snapshot = client::bounded_json(response, legacy::OWNER_BODY_LIMIT)
        .await
        .unwrap();
    assert_eq!(
        snapshot["checkpoint"]["output"].as_str().unwrap().len(),
        300_000
    );
    let response = http.get(remote.url("/run")).send().await.unwrap();
    assert!(
        client::bounded_json(response, client::BODY_LIMIT)
            .await
            .is_err()
    );
    remote.mock(|when, then| {
        when.path("/oversized");
        then.json_body(json!({"checkpoint":{"output":"x".repeat(2_097_152)}}));
    });
    let response = http.get(remote.url("/oversized")).send().await.unwrap();
    assert!(
        client::bounded_json(response, legacy::OWNER_BODY_LIMIT)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn newer_reason_survives_a_captured_read_and_waiting_work_is_coalesced() {
    let dir = tempfile::tempdir().unwrap();
    let bundle = test_store_bundle();
    let state = build(dir.path(), &bundle, ORIGIN, false, false);
    ready_observation(&state).await;
    let store = state.incident_store();
    let snapshot = store.snapshot().await.unwrap();
    let issue = snapshot["incidents"][0]["issue_id"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .unwrap();
    let mut alert = AcceptedAlert {
        project_id:  25,
        issue_id:    issue,
        reason:      AlertReason::New,
        body_digest: "b".repeat(64),
        received_ms: 20,
    };
    store.accept(ORIGIN, &alert).await.unwrap();
    assert!(
        store
            .pending("https://other.example", 20)
            .await
            .unwrap()
            .is_empty()
    );
    let captured = store.pending(ORIGIN, 20).await.unwrap().remove(0);
    alert.reason = AlertReason::Regressed;
    alert.body_digest = "c".repeat(64);
    store.accept(ORIGIN, &alert).await.unwrap();
    let current = client::Issue {
        project:       25,
        id:            issue,
        resolved:      false,
        muted:         false,
        first_seen_ms: 10,
    };
    store
        .apply_observation(ORIGIN, &captured, &current, uuid::Uuid::new_v4())
        .await
        .unwrap();
    let snapshot = store.snapshot().await.unwrap();
    assert_eq!(snapshot["incidents"][0]["applied_generation"], 2);
    assert_eq!(snapshot["incidents"][0]["requested_generation"], 3);
    assert_eq!(snapshot["incidents"][0]["alert_reason"], "REGRESSED");
    assert_eq!(snapshot["incidents"][0]["episode"], 1);
    let captured = store.pending(ORIGIN, 20).await.unwrap().remove(0);
    let event = uuid::Uuid::new_v4();
    store
        .apply_observation(ORIGIN, &captured, &current, event)
        .await
        .unwrap();
    assert!(
        store
            .reserve("https://other.example", 25, REVISION)
            .await
            .unwrap()
            .is_none()
    );
    let id = store.reserve(ORIGIN, 25, REVISION).await.unwrap().unwrap();
    let intent = store.intent(id).await.unwrap();
    assert_eq!(intent.event, event);
    assert_eq!(intent.episode, 2);
}
