use std::sync::Arc;

use anyhow::{Context as _, ensure};
use axum::http::HeaderMap;
use fabro_api::types::ManifestArgs;
use fabro_automation::{Automation, AutomationId};
use fabro_types::{AutomationRef, Principal, RunId, RunProjection, RunStatus, StageOutcome, SystemActorKind};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;

use super::{client::{BugsinkClient, Issue, ScanProgress}, worker_store::Intent};
use super::super::AppState;
use crate::automation_materializer::AutomationRunMaterializeInput;

pub(crate) fn discovery_eligible(baseline: bool, prior: Option<(bool,bool)>, current: (bool,bool), first_seen_ms: i64, baseline_started_ms: i64) -> bool {
    if current.0 || current.1 { return false; }
    match prior {
        Some((resolved, muted)) => baseline && (resolved || muted),
        None => first_seen_ms >= baseline_started_ms,
    }
}

pub(super) fn retry_deadline(attempt: u8, now: i64) -> Option<i64> {
    [15_000,30_000,60_000,120_000].get(usize::from(attempt.checked_sub(1)?)).map(|delay| now.saturating_add(*delay))
}

pub(crate) fn spawn_incident_intake(state: Arc<AppState>) {
    tokio::spawn(async move {
        let shutdown = state.shutdown_token();
        loop {
            if state.is_shutting_down() { break; }
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = super::reconcile_once(&state, chrono::Utc::now().timestamp_millis()) => {
                    if result.is_err() { tracing::warn!("Bugsink intake reconciliation incomplete"); }
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            }
        }
    });
}

pub(crate) async fn reconcile_once(state: &Arc<AppState>, now_ms: i64) -> anyhow::Result<()> {
    let store = state.incident_store();
    for id in store.active_runs().await? {
        super::reconcile_run(state, id).await?;
    }
    let config = state.server_settings().server.integrations.bugsink.clone();
    if !config.enabled { return Ok(()); }
    let client = BugsinkClient::new(state).await?;
    scan_once(state, &client, now_ms).await?;
    let ready: bool = sqlx::query_scalar("SELECT COUNT(*)=2 AND MIN(baseline_complete)=1 FROM bugsink_scans WHERE origin=? AND project_id IN (25,26)")
        .bind(&client.origin).fetch_one(&store.pool).await?;
    if !ready { return Ok(()); }
    for pending in store.pending(&client.origin, now_ms).await? {
        match client.issue(pending.project, pending.issue).await {
            Ok((issue,event)) => store.apply_observation(&client.origin, &pending, &issue,event).await?,
            Err(error) => {
                let reason = match error.to_string().as_str() {
                    "upstream_not_found_or_retained" => "issue_not_found_or_retained",
                    "event_not_found_or_retained" => "event_not_found_or_retained",
                    _ => "authoritative_read_failed",
                };
                store.read_failed(&pending, now_ms, reason).await?;
            }
        }
    }
    if !config.dispatch_enabled { return Ok(()); }
    for mapping in config.projects {
        let automation = mapped_automation(state, mapping.project_id).await?;
        if let Some(run_id) = store.reserve(&client.origin, mapping.project_id, &automation.target.ref_selector).await? {
            super::reconcile_run(state, run_id).await?;
            break;
        }
    }
    Ok(())
}

pub(crate) async fn begin_baseline(state: &Arc<AppState>, now: i64) -> anyhow::Result<()> {
    let config=state.server_settings().server.integrations.bugsink.clone();
    ensure!(config.enabled && !config.dispatch_enabled, "baseline_requires_disabled_dispatch");
    let mut projects: Vec<_>=config.projects.iter().map(|mapping|mapping.project_id).collect();
    projects.sort_unstable();
    ensure!(projects==[25,26],"project_mapping_incomplete");
    for project in &projects { mapped_automation(state,*project).await?; }
    state.incident_store().start_baseline(config.origin.as_deref().context("bugsink_not_configured")?,&projects,now).await
}

pub(crate) async fn operator_retry(state: &Arc<AppState>, project: u64, issue: uuid::Uuid, actor: &str,
    allow_additional_run: bool, now: i64) -> anyhow::Result<()> {
    let config=state.server_settings().server.integrations.bugsink.clone();
    ensure!(config.enabled && (!allow_additional_run || config.dispatch_enabled),"dispatch_disabled");
    let automation=mapped_automation(state,project).await?;
    let client=BugsinkClient::new(state).await?;
    let key=super::worker_store::incident_key(&client.origin,project,issue)?;
    let generation: i64=sqlx::query_scalar("SELECT requested_generation FROM bugsink_incidents WHERE incident_key=?")
        .bind(key).fetch_one(&state.incident_store().pool).await?;
    let (issue,event)=client.issue(project,issue).await?;
    state.incident_store().operator_retry(&client.origin,&issue,event,actor,allow_additional_run,
        &automation.target.ref_selector,generation,now).await
}

pub(super) async fn mapped_automation(state: &AppState, project: u64) -> anyhow::Result<Automation> {
    let config = state.server_settings().server.integrations.bugsink.clone();
    ensure!([25,26].contains(&project), "unmapped_project");
    let mapping = config.projects.iter().find(|mapping| mapping.project_id == project).context("unmapped_project")?;
    let id = AutomationId::new(mapping.automation_id.clone()).map_err(|_| anyhow::anyhow!("invalid_automation"))?;
    let automation = state.automation_store().get(&id).await?.context("automation_missing")?;
    ensure!(automation.target.workflow == "incident-loop" && pinned_revision(&automation.target.ref_selector), "workflow_source_not_pinned");
    Ok(automation)
}

pub(super) fn pinned_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

pub(crate) async fn reconcile_run(state: &Arc<AppState>, run_id: RunId) -> anyhow::Result<()> {
    let store = state.incident_store();
    let intent = store.intent(run_id).await?;
    if !matches!(intent.state.as_str(), "reserved" | "creating" | "submitted" | "active" | "uncertain") { return Ok(()); }
    let run = match state.stores.runs.open_run(&run_id).await {
        Ok(run) => run,
        Err(fabro_store::Error::RunNotFound(_)) if intent.state == "reserved" && intent.manifest_digest.is_none() => {
            // Disablement stops new creation too, including a reservation surviving a restart.
            if !state.server_settings().server.integrations.bugsink.dispatch_enabled { return Ok(()); }
            return materialize_create(state, &intent).await;
        }
        Err(_) => return store.transition_run(&intent, "uncertain", Some("run_storage_uncertain")).await,
    };
    let submitted = run.list_events_from_with_limit(1, 2).await
        .is_ok_and(|events| events.iter().any(|event|
            matches!(event.event.body, fabro_types::run_event::EventBody::RunSubmitted(_))));
    if !submitted { return store.transition_run(&intent, "uncertain", Some("partial_run_creation")).await; }
    let projection = match run.state().await {
        Ok(projection) => projection,
        Err(_) => return store.transition_run(&intent, "uncertain", Some("partial_run_creation")).await,
    };
    let automation = match mapped_automation(state, intent.project).await {
        Ok(automation) => automation,
        Err(_) => return store.transition_run(&intent, "uncertain", Some("workflow_mapping_changed")).await,
    };
    let blob = if let Some(hash) = &projection.spec.manifest_blob { run.read_blob(hash).await.ok().flatten() } else { None };
    let verified = verify_binding(&intent, &projection, &automation).is_ok()
        && blob.as_ref().is_some_and(|bytes| intent.manifest_digest.as_deref() == Some(hex::encode(Sha256::digest(bytes)).as_str()));
    if !verified { return store.transition_run(&intent, "uncertain", Some("run_binding_mismatch")).await; }
    match projection.status {
        RunStatus::Submitted => {
            store.transition_run(&intent, "submitted", None).await?;
            super::super::handler::lifecycle::queue_run_start(state.as_ref(), run_id, false, engine()).await
                .map_err(|_| anyhow::anyhow!("run_start_unavailable"))?;
            store.transition_run(&intent, "active", None).await
        }
        RunStatus::Succeeded { .. } => {
            if terminal_result(&intent, &projection).is_ok() {
                store.transition_run(&intent, "succeeded", None).await
            } else {
                store.transition_run(&intent, "failed", Some("incident_handoff_incomplete")).await
            }
        }
        status if status.is_terminal() => store.transition_run(&intent, "failed", Some("investigation_failed")).await,
        _ => store.transition_run(&intent, "active", None).await,
    }
}

fn engine() -> Principal { Principal::System { system_kind: SystemActorKind::Engine } }

async fn materialize_create(state: &Arc<AppState>, intent: &Intent) -> anyhow::Result<()> {
    let store = state.incident_store();
    let automation = mapped_automation(state, intent.project).await?;
    if automation.target.ref_selector != intent.revision {
        return store.transition_run(intent, "uncertain", Some("workflow_mapping_changed")).await;
    }
    let materialized = state.materialize_automation_run(AutomationRunMaterializeInput {
        automation_id: automation.id.clone(), target: automation.target.clone(), run_id: intent.run_id,
        user_settings_path: state.active_config_path().to_path_buf(), temp_root: state.automation_temp_root(),
    }).await;
    let mut materialized = match materialized {
        Ok(value) => value,
        Err(_) => return store.transition_run(intent, "failed", Some("materialization_failed")).await,
    };
    if !materialized.manifest.git.as_ref().is_some_and(|git| git.sha.as_deref() == Some(intent.revision.as_str())) {
        return store.transition_run(intent, "failed", Some("workflow_source_mismatch")).await;
    }
    let mut args = materialized.manifest.args.take().unwrap_or_else(|| ManifestArgs {
        auto_approve: None, dry_run: None, label: Vec::new(), model: None, preserve_sandbox: None,
        provider: None, environment: None, input: Vec::new(), verbose: None,
    });
    let inputs = intent.inputs();
    args.input.retain(|value| !inputs.iter().any(|(key,_)| value.split_once('=').is_some_and(|(existing,_)| existing.trim() == key)));
    // TOML quoting preserves all five declared string inputs, including decimal episodes.
    for (key,value) in inputs { args.input.push(format!("{key}={}", serde_json::to_string(&value)?)); }
    materialized.manifest.args = Some(args);
    materialized.submitted_manifest_bytes = serde_json::to_vec(&materialized.manifest)?;
    if !store.begin_create(intent, &materialized.submitted_manifest_bytes).await? { return Ok(()); }
    let response = Box::pin(super::super::handler::runs::create_run_from_manifest(Arc::clone(state),
        super::super::handler::runs::CreateRunFromManifestRequest {
            manifest: materialized.manifest, submitted_manifest_bytes: materialized.submitted_manifest_bytes,
            explicit_run_id: Some(intent.run_id), explicit_title_supplied: true, actor: engine(), headers: HeaderMap::new(),
            automation: Some(AutomationRef { id: automation.id.to_string(), name: Some(automation.name.clone()), trigger_id: None }),
        })).await;
    // A response failure can follow RunCreated or RunSubmitted. Never recreate this identity.
    if !response.status().is_success() {
        store.transition_run(intent, "uncertain", Some("create_response_uncertain")).await?;
    }
    Box::pin(reconcile_run(state, intent.run_id)).await
}

fn verify_binding(intent: &Intent, projection: &RunProjection, automation: &Automation) -> anyhow::Result<()> {
    ensure!(projection.spec.run_id == intent.run_id && projection.spec.automation.as_ref().is_some_and(|reference|
        reference.id == automation.id.to_string() && reference.trigger_id.is_none()), "run_identity_mismatch");
    ensure!(projection.spec.git.as_ref().is_some_and(|git| git.sha.as_deref() == Some(intent.revision.as_str())), "run_source_mismatch");
    for (key,value) in intent.inputs() {
        ensure!(projection.spec.settings.run.inputs.get(&key).and_then(toml::Value::as_str) == Some(value.as_str()), "run_input_mismatch");
    }
    Ok(())
}

pub(super) fn terminal_result(intent: &Intent, projection: &RunProjection) -> anyhow::Result<()> {
    ensure!(projection.conclusion.as_ref().is_some_and(|conclusion| conclusion.status == StageOutcome::Succeeded), "conclusion_missing");
    let checkpoint = projection.current_checkpoint().context("checkpoint_missing")?;
    let required = projection.spec.graph.nodes.get("investigate").context("required_stage_missing")?;
    ensure!(required.goal_gate() && required.output_schema() == Some("routing"), "required_stage_not_gated");
    for node in projection.spec.graph.nodes.values().filter(|node| node.goal_gate()) {
        ensure!(checkpoint.node_outcomes.get(&node.id).is_some_and(|outcome| outcome.status == StageOutcome::Succeeded), "required_stage_failed");
    }
    let value = checkpoint.context_values.get("incident_result").context("incident_result_missing")?;
    ensure!(serde_json::to_vec(value)?.len() <= 16_384, "incident_result_oversized");
    let object = value.as_object().context("incident_result_invalid")?;
    let allowed = ["schema_version","run_id","incident","event","observation","episode","status","publication","evidence_digest","missing","child_run_ids"];
    ensure!((10..=11).contains(&object.len()) && object.keys().all(|key| allowed.contains(&key.as_str()))
        && value.get("schema_version").and_then(Value::as_u64) == Some(1)
        && value.get("status").and_then(Value::as_str) == Some("completed"), "incident_result_invalid");
    for (key,expected) in intent.inputs() {
        if key == "episode" { ensure!(value.get(&key).and_then(Value::as_i64) == Some(intent.episode), "incident_result_binding_mismatch"); }
        else { ensure!(value.get(&key).and_then(Value::as_str) == Some(expected.as_str()), "incident_result_binding_mismatch"); }
    }
    let digest = value.get("evidence_digest").and_then(Value::as_str).context("evidence_digest_missing")?;
    ensure!(digest.len() == 64 && digest.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)), "evidence_digest_invalid");
    let missing = value.get("missing").and_then(Value::as_array).context("missing_evidence_invalid")?;
    ensure!(missing.len() <= 64 && missing.iter().all(|v| v.as_str().is_some_and(|v| !v.is_empty() && v.len() <= 200 && !v.chars().any(char::is_control))), "missing_evidence_invalid");
    let publication = value.get("publication").and_then(Value::as_object).context("publication_missing")?;
    match publication.get("status").and_then(Value::as_str) {
        Some("no_ticket") => ensure!(publication.len() == 1, "invalid_no_ticket"),
        Some("completed") => {
            ensure!(publication.len() == 4, "publication_identity_missing");
            for name in ["project_id","issue_id"] {
                let text = publication.get(name).and_then(Value::as_str).context("publication_identity_missing")?;
                ensure!(text.parse::<uuid::Uuid>()?.to_string() == text, "publication_identity_invalid");
            }
            let operation = publication.get("operation_key").and_then(Value::as_str).context("publication_identity_missing")?;
            ensure!(!operation.is_empty() && operation.len() <= 512 && !operation.chars().any(char::is_control), "publication_identity_invalid");
        }
        _ => anyhow::bail!("publication_incomplete"),
    }
    if let Some(children) = value.get("child_run_ids") {
        let children = children.as_array().context("child_id_invalid")?;
        ensure!(children.len() <= 16, "child_id_invalid");
        for child in children { let id = child.as_str().context("child_id_invalid")?.parse::<RunId>()?; ensure!(id != intent.run_id, "child_id_invalid"); }
    }
    Ok(())
}

pub(super) async fn scan_once(state: &Arc<AppState>, client: &BugsinkClient, now: i64) -> anyhow::Result<()> {
    let store = state.incident_store();
    let rows = sqlx::query("SELECT * FROM bugsink_scans WHERE origin=? AND baseline_started_ms IS NOT NULL
        AND (scan_started_ms IS NOT NULL OR next_scan_ms<=?)
        ORDER BY scan_started_ms IS NULL,project_id LIMIT 1")
        .bind(&client.origin).bind(now).fetch_all(&store.pool).await?;
    for row in rows {
        if row.try_get::<Option<i64>,_>("next_scan_ms")?.is_none_or(|due|due>now) { continue; }
        let project = u64::try_from(row.try_get::<i64,_>("project_id")?)?;
        let raw: String = row.try_get("cursor")?;
        let mut progress: ScanProgress = serde_json::from_str(&raw).context("corrupt_scan_progress")?;
        if progress.parked_reason.is_some() { continue; }
        let complete: bool = row.try_get("baseline_complete")?;
        let boundary: i64 = row.try_get("baseline_started_ms")?;
        let result: anyhow::Result<bool> = async {
            if !progress.import_complete {
                let completed: Option<String> = sqlx::query_scalar(
                    "SELECT json_extract(cursor,'$.legacy_import') FROM bugsink_scans WHERE origin=?
                     AND json_valid(cursor) AND json_extract(cursor,'$.import_complete')=1
                     AND json_extract(cursor,'$.legacy_import.status')='reconciled' LIMIT 1")
                    .bind(&client.origin).fetch_optional(&store.pool).await?;
                progress.legacy_import = Some(match completed {
                    Some(summary) => serde_json::from_str(&summary)?,
                    None => super::legacy::import(state).await?,
                });
                ensure!(progress.legacy_import.as_ref().and_then(|value|value.get("status")).and_then(Value::as_str)==Some("reconciled"), "legacy_import_incomplete");
                progress.import_complete = true;
            }
            if row.try_get::<Option<i64>,_>("scan_started_ms")?.is_none() {
                progress.cursor = None; progress.seen.clear(); progress.seen_issues.clear();
            }
            let page = client.page(project, progress.cursor.as_deref()).await?;
            let values = page.get("results").and_then(Value::as_array).context("invalid_scan_page")?;
            let next = page.get("next").context("invalid_scan_page")?;
            ensure!(next.is_null() || next.is_string(), "invalid_scan_cursor");
            let issues: Vec<_> = values.iter().map(|value| Issue::parse(value, project, None)).collect::<anyhow::Result<_>>()?;
            for issue in &issues {
                ensure!(progress.seen_issues.len()<100_000 && progress.seen_issues.insert(issue.id), "repeated_scan_issue");
            }
            progress.advance(&client.origin, project, next.as_str())?;
            for issue in issues { store.scan_issue(&client.origin, &issue, complete, boundary).await?; }
            Ok(next.is_null())
        }.await;
        let (finished, due) = match result {
            Ok(finished) => { progress.read_attempts=0; (finished, Some(if finished { now.saturating_add(300_000) } else { now })) },
            Err(_) => {
                // A page is retried from its original cursor; failed reads never consume coverage.
                let original: ScanProgress = serde_json::from_str(&raw)?;
                progress.cursor=original.cursor; progress.seen=original.seen; progress.seen_issues=original.seen_issues;
                progress.read_attempts=(progress.read_attempts+1).min(5);
                let due=retry_deadline(progress.read_attempts, now);
                if due.is_none() { progress.parked_reason=Some(if progress.import_complete { "scan_read_failed" } else { "legacy_import_incomplete" }.into()); }
                if !progress.import_complete && progress.legacy_import.is_none() {
                    progress.legacy_import=Some(serde_json::json!({"schema_version":1,"status":"incomplete","owners":{},"records":[]}));
                }
                (false,due)
            }
        };
        sqlx::query("UPDATE bugsink_scans SET cursor=?,baseline_complete=CASE WHEN ? THEN 1 ELSE baseline_complete END,
            scan_started_ms=?,next_scan_ms=? WHERE origin=? AND project_id=? AND cursor=?")
            .bind(serde_json::to_string(&progress)?).bind(finished)
            .bind(if finished { None } else { row.try_get::<Option<i64>,_>("scan_started_ms")?.or(Some(now)) })
            .bind(due).bind(&client.origin).bind(i64::try_from(project)?).bind(raw).execute(&store.pool).await?;
    }
    Ok(())
}
