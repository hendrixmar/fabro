use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use fabro_api::types::{ManifestGoal, ManifestGoalType};
use fabro_automation::{
    Automation, PlaneDispatchEffects, PlaneDispatchRecord, PlaneDispatchStore, PlaneTrigger,
};
use fabro_tracker::plane::{PlaneStateConflict, strong_etag};
use fabro_tracker::{Issue, PlaneClient, PlaneOptions};
use fabro_types::{
    AutomationRef, ExternalAgentHarness, FailureReason, PlaneDispatch, PlaneDispatchStatus,
    Principal, RunId, RunStatus, SystemActorKind,
};
use serde_json::Value;
use tokio::time::sleep;
use tracing::{error, info, warn};

use super::AppState;
use crate::automation_materializer::AutomationRunMaterializeInput;

const PLANE_DISPATCHER_IDLE: std::time::Duration = std::time::Duration::from_secs(15);
const PLANE_STATE_CONFLICT: &str = "Plane conditional state write rejected; authorization revoked";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedRun {
    pub status:           RunStatus,
    pub pull_request_url: Option<String>,
    pub pr_pending:       bool,
    pub pr_failed:        bool,
    pub clarification:    Option<String>,
}

#[async_trait]
pub(crate) trait PlanePort: Send + Sync {
    async fn fetch_candidate_issues(
        &self,
        project_id: &str,
        ready_state_id: &str,
    ) -> anyhow::Result<Vec<Issue>>;

    async fn fetch_issue(
        &self,
        project_id: &str,
        issue_id: &str,
    ) -> anyhow::Result<(Issue, Option<String>)>;

    async fn update_state(
        &self,
        project_id: &str,
        issue_id: &str,
        state_id: &str,
        etag: Option<&str>,
    ) -> anyhow::Result<bool>;

    async fn create_comment(
        &self,
        project_id: &str,
        issue_id: &str,
        comment_html: &str,
    ) -> anyhow::Result<()>;

    async fn add_label(
        &self,
        project_id: &str,
        issue_id: &str,
        label_id: &str,
    ) -> anyhow::Result<()>;
}

#[async_trait]
pub(crate) trait RunPort: Send + Sync {
    async fn preflight(&self, automation: &Automation) -> anyhow::Result<()>;

    async fn start_run(
        &self,
        run_id: RunId,
        automation: &Automation,
        trigger: &PlaneTrigger,
        issue: &Issue,
        harness: ExternalAgentHarness,
    ) -> anyhow::Result<RunId>;

    async fn observe_run(&self, run_id: &RunId) -> anyhow::Result<ObservedRun>;

    async fn cancel_run(&self, run_id: &RunId) -> anyhow::Result<()>;
}

pub(crate) struct PlaneTicketDispatcher<P, R> {
    store:               PlaneDispatchStore,
    plane:               P,
    runs:                R,
    public_url:          String,
    next_candidate_poll: HashMap<(String, String), DateTime<Utc>>,
}

impl<P, R> PlaneTicketDispatcher<P, R>
where
    P: PlanePort,
    R: RunPort,
{
    pub(crate) fn new(
        store: PlaneDispatchStore,
        plane: P,
        runs: R,
        public_url: impl Into<String>,
    ) -> Self {
        Self {
            store,
            plane,
            runs,
            public_url: public_url.into(),
            next_candidate_poll: HashMap::new(),
        }
    }

    pub(crate) async fn tick(
        &mut self,
        automations: &[Automation],
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        for automation in automations {
            for trigger in automation.enabled_plane_triggers() {
                if let Err(err) = self.tick_trigger(automation, trigger, now).await {
                    error!(
                        automation_id = %automation.id,
                        trigger_id = %trigger.id,
                        error = format!("{err:#}"),
                        "Plane dispatcher tick failed",
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn tick_trigger(
        &mut self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.reconcile_nonterminal(automation, trigger, now).await?;
        {
            let polls = &mut self.next_candidate_poll;
            let key = (automation.id.to_string(), trigger.id.to_string());
            if polls.get(&key).is_some_and(|due| now < *due) {
                return Ok(());
            }
            let interval =
                chrono::Duration::try_seconds(i64::try_from(trigger.poll_interval_seconds)?)
                    .context("Plane poll interval is out of range")?;
            polls.insert(
                key,
                now.checked_add_signed(interval)
                    .context("Plane poll deadline is out of range")?,
            );
        }
        let active = self
            .store
            .list_nonterminal(&automation.id, &trigger.id)
            .await
            .context("listing nonterminal plane dispatches")?;
        let remaining = trigger.max_concurrency.saturating_sub(active.len());
        if remaining == 0 {
            return Ok(());
        }

        let existing = self
            .store
            .list_existing_issue_ids(&automation.id, &trigger.id)
            .await
            .context("listing existing plane issue ids")?
            .into_iter()
            .collect::<HashSet<_>>();

        let mut candidates = match self
            .plane
            .fetch_candidate_issues(&trigger.project_id, &trigger.ready_state_id)
            .await
        {
            Ok(candidates) => candidates,
            Err(err) => {
                warn!(
                    automation_id = %automation.id,
                    trigger_id = %trigger.id,
                    error = %err,
                    "Plane candidate fetch failed",
                );
                return Ok(());
            }
        };
        candidates.sort_by_key(|issue| {
            (
                priority_rank(issue.priority),
                issue.identifier.clone(),
                issue.id.clone(),
            )
        });

        let mut claimed = 0;
        let mut preflight_ok = false;
        for issue in candidates {
            if claimed >= remaining {
                break;
            }
            if existing.contains(&issue.id) {
                continue;
            }
            let _harness = match resolve_harness(trigger, &issue) {
                Ok(harness) => harness,
                Err(err) => {
                    warn!(
                        automation_id = %automation.id,
                        trigger_id = %trigger.id,
                        issue_id = %issue.id,
                        error = %err,
                        "Skipping Plane ticket due to harness configuration error",
                    );
                    continue;
                }
            };
            if !preflight_ok {
                self.runs
                    .preflight(automation)
                    .await
                    .context("plane automation preflight failed")?;
                preflight_ok = true;
            }
            let (issue, _) = self
                .plane
                .fetch_issue(&trigger.project_id, &issue.id)
                .await?;
            if issue.state != trigger.ready_state_id {
                continue;
            }
            let harness = resolve_harness(trigger, &issue)?;
            if self
                .claim_issue(automation, trigger, &issue, harness, now)
                .await?
            {
                claimed += 1;
            }
        }
        Ok(())
    }

    async fn reconcile_nonterminal(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let records = self
            .store
            .list_nonterminal(&automation.id, &trigger.id)
            .await?;
        for record in records {
            if let Err(err) = self
                .reconcile_record(automation, trigger, record, now)
                .await
            {
                warn!(
                    automation_id = %automation.id,
                    trigger_id = %trigger.id,
                    error = %err,
                    "Failed to reconcile plane dispatch",
                );
            }
        }
        Ok(())
    }

    async fn reconcile_record(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        mut record: PlaneDispatchRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if record.dispatch.last_error.as_deref() == Some(PLANE_STATE_CONFLICT) {
            self.plane
                .fetch_issue(&trigger.project_id, &record.dispatch.issue_id)
                .await?;
            if let Some(id) = record.dispatch.current_run_id.as_deref() {
                self.runs.cancel_run(&id.parse()?).await?;
            }
            return self
                .mark_cancelled(trigger, &mut record, now, "ticket moved externally")
                .await;
        }
        match record.dispatch.status {
            PlaneDispatchStatus::Pending | PlaneDispatchStatus::Claimed => {
                self.advance_claim(automation, trigger, &mut record, now)
                    .await
            }
            PlaneDispatchStatus::RetryPending => {
                self.start_or_resume_run(automation, trigger, &mut record, now)
                    .await
            }
            PlaneDispatchStatus::Running => {
                self.reconcile_running(automation, trigger, &mut record, now)
                    .await
            }
            PlaneDispatchStatus::Succeeded
            | PlaneDispatchStatus::Failed
            | PlaneDispatchStatus::Cancelled => Ok(()),
        }
    }

    async fn claim_issue(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        issue: &Issue,
        harness: ExternalAgentHarness,
        now: DateTime<Utc>,
    ) -> anyhow::Result<bool> {
        let pending = PlaneDispatchRecord {
            dispatch: PlaneDispatch {
                automation_id: automation.id.to_string(),
                trigger_id: trigger.id.to_string(),
                issue_id: issue.id.clone(),
                issue_identifier: issue.identifier.clone(),
                issue_title: Some(issue.title.clone()),
                issue_url: Some(issue.url.clone()),
                status: PlaneDispatchStatus::Pending,
                harness,
                attempt: 1,
                run_ids: Vec::new(),
                current_run_id: None,
                pull_request_url: None,
                last_error: None,
                claimed_at: Some(now),
                completed_at: None,
                created_at: now,
                updated_at: now,
            },
            effects:  PlaneDispatchEffects::default(),
        };
        let (mut record, created) = self.store.create_pending(&pending).await?;
        if !created {
            return Ok(false);
        }
        info!(
            automation_id = %automation.id,
            trigger_id = %trigger.id,
            issue_id = %issue.id,
            harness = %harness.as_str(),
            "Created pending Plane dispatch",
        );
        self.advance_claim(automation, trigger, &mut record, now)
            .await?;
        Ok(true)
    }

    async fn advance_claim(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let (issue, etag) = self
            .plane
            .fetch_issue(&trigger.project_id, &record.dispatch.issue_id)
            .await?;
        let expected = if record.effects.claimed_state_applied {
            &trigger.in_progress_state_id
        } else {
            &trigger.ready_state_id
        };
        if issue.state != *expected && issue.state != trigger.in_progress_state_id {
            if let Some(id) = record.dispatch.current_run_id.as_deref() {
                self.runs.cancel_run(&id.parse()?).await?;
            }
            return self
                .mark_cancelled(trigger, record, now, "ticket moved externally")
                .await;
        }
        if !record.effects.claimed_state_applied {
            if issue.state != trigger.in_progress_state_id
                && !self
                    .plane
                    .update_state(
                        &trigger.project_id,
                        &record.dispatch.issue_id,
                        &trigger.in_progress_state_id,
                        etag.as_deref(),
                    )
                    .await
                    .context("moving Plane issue to configured in-progress state")?
            {
                return self.record_state_conflict(record, now).await;
            }
            record.effects.claimed_state_applied = true;
            record.dispatch.status = PlaneDispatchStatus::Claimed;
            record.dispatch.updated_at = now;
            self.store.save(record).await?;
        }

        self.start_or_resume_run(automation, trigger, record, now)
            .await
    }

    async fn start_or_resume_run(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let (issue, etag) = self
            .plane
            .fetch_issue(&trigger.project_id, &record.dispatch.issue_id)
            .await
            .context("fetching Plane issue before run start")?;
        if issue.state != trigger.in_progress_state_id {
            if let Some(id) = record.dispatch.current_run_id.as_deref() {
                self.runs.cancel_run(&id.parse()?).await?;
            }
            return self
                .mark_cancelled(trigger, record, now, "ticket moved externally")
                .await;
        }
        strong_etag(etag.as_deref())?;
        let run_id = if let Some(id) = record.dispatch.current_run_id.as_deref() {
            id.parse::<RunId>()?
        } else {
            let id = RunId::new();
            record.dispatch.run_ids.push(id.to_string());
            record.dispatch.current_run_id = Some(id.to_string());
            record.dispatch.updated_at = now;
            self.store.save(record).await?;
            id
        };
        self.runs
            .start_run(run_id, automation, trigger, &issue, record.dispatch.harness)
            .await
            .context("starting Plane automation run")?;
        record.dispatch.status = PlaneDispatchStatus::Running;
        record.dispatch.updated_at = now;
        record.dispatch.last_error = None;
        self.store.save(record).await?;

        if !record.effects.claim_comment_posted {
            let comment = format!(
                "<p>Fabro run started: {}/runs/{}</p>",
                self.public_url.trim_end_matches('/'),
                run_id
            );
            self.plane
                .create_comment(&trigger.project_id, &record.dispatch.issue_id, &comment)
                .await
                .context("posting Plane claim comment")?;
            record.effects.claim_comment_posted = true;
            record.dispatch.updated_at = now;
            self.store.save(record).await?;
        }
        Ok(())
    }

    async fn reconcile_running(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(run_id) = record
            .dispatch
            .current_run_id
            .as_deref()
            .and_then(|id| id.parse::<RunId>().ok())
        else {
            return self
                .start_or_resume_run(automation, trigger, record, now)
                .await;
        };

        let (issue, _) = self
            .plane
            .fetch_issue(&trigger.project_id, &record.dispatch.issue_id)
            .await
            .context("fetching Plane issue during running reconcile")?;
        let completing =
            record.effects.success_state_applied && issue.state == trigger.done_state_id;
        if issue.state != trigger.in_progress_state_id && !completing {
            self.runs.cancel_run(&run_id).await?;
            return self
                .mark_cancelled(trigger, record, now, "ticket moved externally")
                .await;
        }
        let observed = self.runs.observe_run(&run_id).await?;
        if !record.effects.claim_comment_posted {
            let comment = format!(
                "<p>Fabro run started: {}/runs/{run_id}</p>",
                self.public_url.trim_end_matches('/')
            );
            self.plane
                .create_comment(&trigger.project_id, &record.dispatch.issue_id, &comment)
                .await?;
            record.effects.claim_comment_posted = true;
            self.store.save(record).await?;
        }
        if matches!(observed.status, RunStatus::Blocked { .. }) {
            if let Some(comment) = observed.clarification.as_deref() {
                self.plane
                    .create_comment(&trigger.project_id, &record.dispatch.issue_id, comment)
                    .await?;
            }
        }
        match observed.status {
            RunStatus::Failed {
                reason: FailureReason::Cancelled,
            }
            | RunStatus::Dead => {
                self.mark_cancelled(trigger, record, now, "run cancelled")
                    .await
            }
            RunStatus::Failed { .. } => {
                self.handle_run_failure(automation, trigger, record, now, "run failed")
                    .await
            }
            RunStatus::Succeeded { .. } => {
                if observed.pr_pending {
                    return Ok(());
                }
                if observed.pr_failed {
                    return self
                        .handle_run_failure(
                            automation,
                            trigger,
                            record,
                            now,
                            "pull request creation failed",
                        )
                        .await;
                }
                let Some(pr_url) = observed.pull_request_url else {
                    return Ok(());
                };
                self.mark_succeeded(trigger, record, now, &run_id, &pr_url)
                    .await
            }
            _ => Ok(()),
        }
    }

    async fn handle_run_failure(
        &self,
        automation: &Automation,
        trigger: &PlaneTrigger,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
        reason: &str,
    ) -> anyhow::Result<()> {
        let run_url = record.dispatch.current_run_id.as_deref().map_or_else(
            || "unknown run".to_string(),
            |id| format!("{}/runs/{id}", self.public_url.trim_end_matches('/')),
        );
        let comment = format!("<p>Fabro run failed ({reason}): {run_url}</p>");
        if !record.effects.failure_comment_posted {
            self.plane
                .create_comment(&trigger.project_id, &record.dispatch.issue_id, &comment)
                .await?;
            record.effects.failure_comment_posted = true;
            self.store.save(record).await?;
        }

        if record.dispatch.attempt < 2 && (record.dispatch.attempt as usize) <= trigger.max_retries
        {
            record.dispatch.attempt += 1;
            record.dispatch.status = PlaneDispatchStatus::RetryPending;
            record.dispatch.current_run_id = None;
            record.dispatch.last_error = Some(reason.to_string());
            record.effects.failure_comment_posted = false;
            record.effects.claim_comment_posted = false;
            record.dispatch.updated_at = now;
            self.store.save(record).await?;
            return self
                .start_or_resume_run(automation, trigger, record, now)
                .await;
        }

        if let Some(label_id) = trigger.failure_label_id.as_deref() {
            if !record.effects.failure_label_applied {
                self.plane
                    .add_label(&trigger.project_id, &record.dispatch.issue_id, label_id)
                    .await?;
                record.effects.failure_label_applied = true;
                self.store.save(record).await?;
            }
        }
        record.dispatch.status = PlaneDispatchStatus::Failed;
        record.dispatch.last_error = Some(reason.to_string());
        record.dispatch.completed_at = Some(now);
        record.dispatch.updated_at = now;
        self.store.save(record).await?;
        Ok(())
    }

    async fn mark_succeeded(
        &self,
        trigger: &PlaneTrigger,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
        run_id: &RunId,
        pr_url: &str,
    ) -> anyhow::Result<()> {
        record.dispatch.pull_request_url = Some(pr_url.to_string());
        let (issue, etag) = self
            .plane
            .fetch_issue(&trigger.project_id, &record.dispatch.issue_id)
            .await?;
        if issue.state != trigger.in_progress_state_id && issue.state != trigger.done_state_id {
            return self
                .mark_cancelled(trigger, record, now, "ticket moved externally")
                .await;
        }
        if !record.effects.success_state_applied {
            if issue.state != trigger.done_state_id
                && !self
                    .plane
                    .update_state(
                        &trigger.project_id,
                        &record.dispatch.issue_id,
                        &trigger.done_state_id,
                        etag.as_deref(),
                    )
                    .await
                    .context("moving Plane issue to configured completion state")?
            {
                return self.record_state_conflict(record, now).await;
            }
            record.effects.success_state_applied = true;
            record.dispatch.updated_at = now;
            self.store.save(record).await?;
        }
        if !record.effects.success_comment_posted {
            let comment = format!(
                "<p>Fabro run succeeded: {}/runs/{}</p><p>Draft PR: {pr_url}</p>",
                self.public_url.trim_end_matches('/'),
                run_id
            );
            self.plane
                .create_comment(&trigger.project_id, &record.dispatch.issue_id, &comment)
                .await
                .context("posting Plane success comment")?;
            record.effects.success_comment_posted = true;
        }
        record.dispatch.status = PlaneDispatchStatus::Succeeded;
        record.dispatch.completed_at = Some(now);
        record.dispatch.updated_at = now;
        self.store.save(record).await?;
        Ok(())
    }

    async fn record_state_conflict(
        &self,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // Persist before any fallible reconciliation: a restart must never refresh
        // the tag and blindly re-authorize the rejected transition.
        record.dispatch.last_error = Some(PLANE_STATE_CONFLICT.into());
        record.dispatch.updated_at = now;
        self.store.save(record).await?;
        Err(PlaneStateConflict.into())
    }

    async fn mark_cancelled(
        &self,
        trigger: &PlaneTrigger,
        record: &mut PlaneDispatchRecord,
        now: DateTime<Utc>,
        reason: &str,
    ) -> anyhow::Result<()> {
        let mut reason = reason;
        if reason != "ticket moved externally" && !record.effects.cancelled_state_applied {
            let (issue, etag) = self
                .plane
                .fetch_issue(&trigger.project_id, &record.dispatch.issue_id)
                .await?;
            if issue.state != trigger.in_progress_state_id {
                reason = "ticket moved externally";
            } else if !self
                .plane
                .update_state(
                    &trigger.project_id,
                    &record.dispatch.issue_id,
                    &trigger.cancelled_state_id,
                    etag.as_deref(),
                )
                .await?
            {
                return self.record_state_conflict(record, now).await;
            } else {
                record.effects.cancelled_state_applied = true;
            }
        }
        if !record.effects.cancelled_comment_posted {
            let comment = format!("<p>Fabro run cancelled: {reason}</p>");
            self.plane
                .create_comment(&trigger.project_id, &record.dispatch.issue_id, &comment)
                .await?;
            record.effects.cancelled_comment_posted = true;
        }
        record.dispatch.status = PlaneDispatchStatus::Cancelled;
        record.dispatch.last_error = Some(reason.to_string());
        record.dispatch.completed_at = Some(now);
        record.dispatch.updated_at = now;
        self.store.save(record).await?;
        Ok(())
    }
}

fn resolve_harness(trigger: &PlaneTrigger, issue: &Issue) -> anyhow::Result<ExternalAgentHarness> {
    let has_codex = trigger
        .codex_label_id
        .as_deref()
        .is_some_and(|id| issue.labels.iter().any(|label| label == id));
    let has_omp = trigger
        .omp_label_id
        .as_deref()
        .is_some_and(|id| issue.labels.iter().any(|label| label == id));
    if has_codex && has_omp {
        anyhow::bail!("ticket has both Codex and OMP harness override labels");
    }
    if has_codex {
        return Ok(ExternalAgentHarness::Codex);
    }
    if has_omp {
        return Ok(ExternalAgentHarness::Omp);
    }
    Ok(trigger.default_harness)
}

fn priority_rank(priority: Option<i32>) -> i32 {
    match priority {
        Some(1) => 0,
        Some(2) => 1,
        Some(3) => 2,
        Some(4) => 3,
        Some(0) | None => 4,
        _ => 5,
    }
}

pub(crate) fn ticket_goal(issue: &Issue, project_id: &str) -> String {
    let labels = if issue.labels.is_empty() {
        "(none)".to_string()
    } else {
        issue.labels.join(", ")
    };
    format!(
        "{identifier} {title}\nURL: {url}\nProject: {project_id}\nPriority: {priority}\nLabels: {labels}\n\n{description}",
        identifier = issue.identifier,
        title = issue.title,
        url = issue.url,
        priority = issue
            .priority
            .map_or_else(|| "none".to_string(), |p| p.to_string()),
        description = issue.description.as_deref().unwrap_or(""),
    )
}

pub(crate) fn ticket_run_title(issue: &Issue) -> String {
    let title = format!("[{}] {}", issue.identifier, issue.title);
    if title.len() <= 100 {
        title
    } else {
        format!("{}...", &title[..97])
    }
}

pub(crate) fn spawn_plane_dispatcher(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut dispatcher = None;
        let shutdown = state.shutdown_token();
        loop {
            if state.is_shutting_down() {
                break;
            }
            if let Err(err) = tick_all(Arc::clone(&state), &mut dispatcher).await {
                error!(error = %err, "Plane dispatcher cycle failed");
            }
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = sleep(PLANE_DISPATCHER_IDLE) => {},
            }
        }
    });
}

async fn tick_all(
    state: Arc<AppState>,
    dispatcher: &mut Option<PlaneTicketDispatcher<LivePlanePort, LiveRunPort>>,
) -> anyhow::Result<()> {
    let Some(client) = plane_client_from_state(&state).await? else {
        return Ok(());
    };
    let automations = state.automation_store().list().await?;
    if automations
        .iter()
        .all(|automation| automation.enabled_plane_triggers().next().is_none())
    {
        return Ok(());
    }
    let dispatcher = dispatcher.get_or_insert_with(|| {
        PlaneTicketDispatcher::new(
            state.plane_dispatch_store().clone(),
            LivePlanePort {
                client: client.clone(),
            },
            LiveRunPort {
                state: Arc::clone(&state),
            },
            state.effective_web_url(),
        )
    });
    dispatcher.plane.client = client;
    dispatcher.tick(&automations, Utc::now()).await
}

async fn plane_client_from_state(state: &AppState) -> anyhow::Result<Option<PlaneClient>> {
    let settings = state.server_settings().server.integrations.plane.clone();
    if !settings.enabled {
        return Ok(None);
    }
    let Some(api_base) = settings.api_base.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let Some(workspace) = settings.workspace.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let Some(api_key) = state
        .vault_secret(fabro_static::EnvVars::PLANE_API_KEY)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(PlaneClient::new(PlaneOptions::new(
        api_base, workspace, api_key,
    ))))
}

struct LivePlanePort {
    client: PlaneClient,
}

#[async_trait]
impl PlanePort for LivePlanePort {
    async fn fetch_candidate_issues(
        &self,
        project_id: &str,
        ready_state_id: &str,
    ) -> anyhow::Result<Vec<Issue>> {
        self.client
            .fetch_candidate_issues(project_id, ready_state_id)
            .await
    }

    async fn fetch_issue(
        &self,
        project_id: &str,
        issue_id: &str,
    ) -> anyhow::Result<(Issue, Option<String>)> {
        let mut raw = self.client.fetch_issue_raw(project_id, issue_id).await?;
        let etag = raw.etag.take();
        Ok((self.client.normalize_issue(raw, project_id)?, etag))
    }

    async fn update_state(
        &self,
        project_id: &str,
        issue_id: &str,
        state_id: &str,
        etag: Option<&str>,
    ) -> anyhow::Result<bool> {
        match self
            .client
            .update_state(project_id, issue_id, state_id, etag)
            .await
        {
            Ok(()) => Ok(true),
            Err(err) if err.is::<PlaneStateConflict>() => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn create_comment(
        &self,
        project_id: &str,
        issue_id: &str,
        comment_html: &str,
    ) -> anyhow::Result<()> {
        // Reconcile an acknowledged-or-ambiguous POST before repeating it.
        let comments = self
            .client
            .paged_request(&format!(
                "projects/{project_id}/issues/{issue_id}/comments/"
            ))
            .await?;
        if comments.iter().any(|comment| {
            comment.get("comment_html").and_then(|v| v.as_str()) == Some(comment_html)
        }) {
            return Ok(());
        }
        self.client
            .create_comment(project_id, issue_id, comment_html)
            .await
            .map(|_| ())
    }

    async fn add_label(
        &self,
        project_id: &str,
        issue_id: &str,
        label_id: &str,
    ) -> anyhow::Result<()> {
        self.client.add_label(project_id, issue_id, label_id).await
    }
}

struct LiveRunPort {
    state: Arc<AppState>,
}

#[async_trait]
impl RunPort for LiveRunPort {
    async fn preflight(&self, automation: &Automation) -> anyhow::Result<()> {
        let run_id = RunId::new();
        self.state
            .materialize_automation_run(AutomationRunMaterializeInput {
                automation_id: automation.id.clone(),
                target: automation.target.clone(),
                run_id,
                user_settings_path: self.state.active_config_path().to_path_buf(),
                temp_root: self.state.automation_temp_root(),
            })
            .await
            .context("materializing automation target")?;
        Ok(())
    }

    async fn start_run(
        &self,
        run_id: RunId,
        automation: &Automation,
        trigger: &PlaneTrigger,
        issue: &Issue,
        harness: ExternalAgentHarness,
    ) -> anyhow::Result<RunId> {
        let existing = self
            .state
            .stores
            .runs
            .get_cached_projection(&run_id)
            .await?;
        if let Some(existing) = existing {
            anyhow::ensure!(
                existing
                    .spec
                    .automation
                    .as_ref()
                    .is_some_and(|reference| reference.id == automation.id.to_string()
                        && reference.trigger_id.as_deref() == Some(trigger.id.as_str())),
                "reserved run identity belongs to another automation"
            );
            if existing.status != RunStatus::Submitted {
                return Ok(run_id);
            }
            super::handler::lifecycle::queue_run_start(
                self.state.as_ref(),
                run_id,
                false,
                Principal::System {
                    system_kind: SystemActorKind::Engine,
                },
            )
            .await
            .map_err(|err| anyhow::anyhow!("{}", err.detail()))?;
            return Ok(run_id);
        }
        let mut materialized = self
            .state
            .materialize_automation_run(AutomationRunMaterializeInput {
                automation_id: automation.id.clone(),
                target: automation.target.clone(),
                run_id,
                user_settings_path: self.state.active_config_path().to_path_buf(),
                temp_root: self.state.automation_temp_root(),
            })
            .await?;
        materialized.manifest.title = ticket_run_title(issue).parse().ok();
        materialized.manifest.goal = Some(ManifestGoal {
            type_: ManifestGoalType::Value,
            text:  ticket_goal(issue, &trigger.project_id),
        });
        materialized.manifest.external_agent_harness = Some(harness);
        materialized.submitted_manifest_bytes = serde_json::to_vec(&materialized.manifest)?;
        let actor = Principal::System {
            system_kind: SystemActorKind::Engine,
        };
        let automation_ref = AutomationRef {
            id:         automation.id.to_string(),
            name:       Some(automation.name.clone()),
            trigger_id: Some(trigger.id.to_string()),
        };
        let response = Box::pin(super::handler::runs::create_run_from_manifest(
            Arc::clone(&self.state),
            super::handler::runs::CreateRunFromManifestRequest {
                manifest:                 materialized.manifest,
                submitted_manifest_bytes: materialized.submitted_manifest_bytes,
                explicit_run_id:          Some(run_id),
                explicit_title_supplied:  true,
                actor:                    actor.clone(),
                headers:                  HeaderMap::new(),
                automation:               Some(automation_ref),
            },
        ))
        .await;
        if !response.status().is_success() {
            anyhow::bail!("failed to create plane run: {}", response.status());
        }
        super::handler::lifecycle::queue_run_start(self.state.as_ref(), run_id, false, actor)
            .await
            .map_err(|err| anyhow::anyhow!("{}", err.detail()))?;
        Ok(run_id)
    }

    async fn observe_run(&self, run_id: &RunId) -> anyhow::Result<ObservedRun> {
        let projection = self
            .state
            .stores
            .runs
            .get_cached_projection(run_id)
            .await?
            .context("run projection not found")?;
        let creation = projection.pull_request_creation.as_ref();
        let pr_pending = creation.is_some_and(fabro_types::PullRequestCreation::is_pending);
        let pr_failed = creation.is_some_and(|creation| {
            creation.status == fabro_types::PullRequestCreationStatus::Failed
        });
        let pull_request_url = if matches!(projection.status, RunStatus::Succeeded { .. })
            && !pr_pending
            && !pr_failed
        {
            super::handler::pull_requests::verified_draft_for_run(&self.state, run_id)
                .await
                .map_err(|err| anyhow::anyhow!("{}", err.detail()))?
        } else {
            None
        };
        Ok(ObservedRun {
            status: projection.status,
            pull_request_url,
            pr_pending,
            pr_failed,
            clarification: clarification_comment(run_id, &projection)?,
        })
    }

    async fn cancel_run(&self, run_id: &RunId) -> anyhow::Result<()> {
        let Some(projection) = self.state.stores.runs.get_cached_projection(run_id).await? else {
            return Ok(()); // Reservation exists, but creation never committed.
        };
        if projection.status.is_terminal() {
            return Ok(());
        }
        super::append_control_request(
            self.state.as_ref(),
            *run_id,
            fabro_types::RunControlAction::Cancel,
            Some(Principal::System {
                system_kind: SystemActorKind::Engine,
            }),
        )
        .await?;
        if matches!(
            projection.status,
            RunStatus::Submitted | RunStatus::Pending { .. } | RunStatus::Runnable
        ) {
            return super::persist_cancelled_run_status(self.state.as_ref(), *run_id).await;
        }
        self.state
            .worker_control_bus
            .publish(
                *run_id,
                fabro_interview::WorkerControlEnvelope::cancel_run(),
            )
            .await
            .context("publishing plane run cancel")?;
        Ok(())
    }
}

/// Read the validated artifact mirrored into the durable checkpoint by
/// parse_clarity. The sandbox path is not a server-local path and is never read
/// as one.
fn clarification_comment(
    run_id: &RunId,
    projection: &fabro_types::RunProjection,
) -> anyhow::Result<Option<String>> {
    if !matches!(projection.status, RunStatus::Blocked { .. }) {
        return Ok(None);
    }
    let Some(checkpoint) = projection.current_checkpoint() else {
        return Ok(None);
    };
    if checkpoint.next_node_id.as_deref() != Some("clarification")
        || !checkpoint
            .node_outcomes
            .get("parse_clarity")
            .is_some_and(|outcome| outcome.status == fabro_types::StageOutcome::Succeeded)
    {
        return Ok(None);
    }
    let Some(value) = checkpoint.context_values.get("ticket_clarity") else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .context("clarification artifact is not an object")?;
    anyhow::ensure!(
        object.len() == 3 && object.get("schema_version").and_then(Value::as_u64) == Some(1),
        "invalid clarification artifact schema"
    );
    let status = object
        .get("status")
        .and_then(|v| v.as_str())
        .context("missing clarification status")?;
    let questions = object
        .get("questions")
        .and_then(|v| v.as_array())
        .context("missing clarification questions")?;
    if status == "ready" && questions.is_empty() {
        return Ok(None);
    }
    anyhow::ensure!(
        status == "needs_clarification" && (1..=5).contains(&questions.len()),
        "invalid clarification questions"
    );
    let mut seen = HashSet::new();
    let mut comment = format!("<p>Fabro run {run_id} is blocked and needs clarification:</p><ol>");
    for value in questions {
        let question = value
            .as_str()
            .context("clarification question is not text")?;
        anyhow::ensure!(
            (10..=500).contains(&question.chars().count())
                && question == question.trim()
                && question.ends_with('?')
                && !question.chars().any(|c| c < ' ')
                && seen.insert(question),
            "invalid clarification question"
        );
        comment.push_str("<li>");
        for c in question.chars() {
            match c {
                '&' => comment.push_str("&amp;"),
                '<' => comment.push_str("&lt;"),
                '>' => comment.push_str("&gt;"),
                '"' => comment.push_str("&quot;"),
                '\'' => comment.push_str("&#39;"),
                _ => comment.push(c),
            }
        }
        comment.push_str("</li>");
    }
    comment.push_str("</ol>");
    Ok(Some(comment))
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use fabro_automation::{
        AutomationDraft, AutomationId, AutomationStore, AutomationTarget, AutomationTrigger,
        AutomationTriggerId, PlaneTrigger,
    };
    use fabro_db::Database;
    use fabro_types::{ExternalAgentHarness, SuccessReason};

    use super::*;

    #[derive(Default)]
    struct FakePlane {
        issues:            Mutex<Vec<Issue>>,
        states:            Mutex<BTreeMap<String, String>>,
        comments:          Mutex<Vec<(String, String)>>,
        labels:            Mutex<Vec<(String, String)>>,
        candidate_fetches: Mutex<usize>,
        fail_fetch:        Mutex<bool>,
        fail_label_once:   Mutex<bool>,
        conflict_state:    Mutex<Option<String>>,
    }

    #[async_trait]
    impl PlanePort for &FakePlane {
        async fn fetch_candidate_issues(
            &self,
            _project_id: &str,
            ready_state_id: &str,
        ) -> anyhow::Result<Vec<Issue>> {
            *self.candidate_fetches.lock().unwrap() += 1;
            if *self.fail_fetch.lock().unwrap() {
                anyhow::bail!("plane unavailable");
            }
            Ok(self
                .issues
                .lock()
                .unwrap()
                .iter()
                .filter(|issue| issue.state == ready_state_id)
                .cloned()
                .collect())
        }

        async fn fetch_issue(
            &self,
            _project_id: &str,
            issue_id: &str,
        ) -> anyhow::Result<(Issue, Option<String>)> {
            self.issues
                .lock()
                .unwrap()
                .iter()
                .find(|issue| issue.id == issue_id)
                .cloned()
                .map(|issue| {
                    let etag = Some(format!("\"{}\"", issue.state));
                    (issue, etag)
                })
                .context("missing issue")
        }

        async fn update_state(
            &self,
            _project_id: &str,
            issue_id: &str,
            state_id: &str,
            etag: Option<&str>,
        ) -> anyhow::Result<bool> {
            let mut issues = self.issues.lock().unwrap();
            if let Some(state) = self.conflict_state.lock().unwrap().take() {
                issues
                    .iter_mut()
                    .find(|issue| issue.id == issue_id)
                    .unwrap()
                    .state = state;
                return Ok(false);
            }
            if let Some(issue) = issues.iter_mut().find(|issue| issue.id == issue_id) {
                anyhow::ensure!(
                    etag == Some(format!("\"{}\"", issue.state).as_str()),
                    "wrong observed version"
                );
                issue.state = state_id.to_string();
            }
            self.states
                .lock()
                .unwrap()
                .insert(issue_id.to_string(), state_id.to_string());
            Ok(true)
        }

        async fn create_comment(
            &self,
            _project_id: &str,
            issue_id: &str,
            comment_html: &str,
        ) -> anyhow::Result<()> {
            self.comments
                .lock()
                .unwrap()
                .push((issue_id.to_string(), comment_html.to_string()));
            Ok(())
        }

        async fn add_label(
            &self,
            _project_id: &str,
            issue_id: &str,
            label_id: &str,
        ) -> anyhow::Result<()> {
            if std::mem::take(&mut *self.fail_label_once.lock().unwrap()) {
                anyhow::bail!("label write unavailable");
            }
            self.labels
                .lock()
                .unwrap()
                .push((issue_id.to_string(), label_id.to_string()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeRuns {
        preflight_ok:    Mutex<bool>,
        started:         Mutex<Vec<(String, ExternalAgentHarness)>>,
        observed:        Mutex<BTreeMap<String, ObservedRun>>,
        observations:    Mutex<usize>,
        fail_start_once: Mutex<bool>,
        cancelled:       Mutex<Vec<String>>,
    }

    #[async_trait]
    impl RunPort for &FakeRuns {
        async fn preflight(&self, _automation: &Automation) -> anyhow::Result<()> {
            if *self.preflight_ok.lock().unwrap() {
                Ok(())
            } else {
                anyhow::bail!("workflow does not resolve")
            }
        }

        async fn start_run(
            &self,
            run_id: RunId,
            _automation: &Automation,
            _trigger: &PlaneTrigger,
            issue: &Issue,
            harness: ExternalAgentHarness,
        ) -> anyhow::Result<RunId> {
            if self
                .observed
                .lock()
                .unwrap()
                .contains_key(&run_id.to_string())
            {
                return Ok(run_id);
            }
            self.started
                .lock()
                .unwrap()
                .push((issue.id.clone(), harness));
            self.observed
                .lock()
                .unwrap()
                .insert(run_id.to_string(), ObservedRun {
                    status:           RunStatus::Running,
                    pull_request_url: None,
                    pr_pending:       false,
                    pr_failed:        false,
                    clarification:    None,
                });
            if std::mem::take(&mut *self.fail_start_once.lock().unwrap()) {
                anyhow::bail!("start committed but acknowledgment lost");
            }
            Ok(run_id)
        }

        async fn observe_run(&self, run_id: &RunId) -> anyhow::Result<ObservedRun> {
            *self.observations.lock().unwrap() += 1;
            self.observed
                .lock()
                .unwrap()
                .get(&run_id.to_string())
                .cloned()
                .context("missing observed run")
        }

        async fn cancel_run(&self, run_id: &RunId) -> anyhow::Result<()> {
            self.cancelled.lock().unwrap().push(run_id.to_string());
            Ok(())
        }
    }

    fn issue(id: &str, identifier: &str, priority: Option<i32>, labels: &[&str]) -> Issue {
        Issue {
            id: id.to_string(),
            project_item_id: None,
            identifier: identifier.to_string(),
            title: identifier.to_string(),
            description: Some("body".to_string()),
            priority,
            state: "ready".to_string(),
            branch_name: None,
            url: format!("https://plane.example/{id}"),
            assignee_id: None,
            labels: labels.iter().map(|label| (*label).to_string()).collect(),
            blocked_by: Vec::new(),
            created_at: None,
            updated_at: None,
        }
    }

    async fn setup() -> (tempfile::TempDir, Automation, PlaneDispatchStore) {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::connect(dir.path().join("db.sqlite"))
            .await
            .unwrap();
        database.migrate().await.unwrap();
        let automations = AutomationStore::new(database.clone_pool());
        let created = automations
            .create(AutomationDraft {
                id:          AutomationId::new("tierra").unwrap(),
                name:        "Tierra".to_string(),
                description: None,
                target:      AutomationTarget {
                    repository:   "owner/repo".to_string(),
                    ref_selector: "main".to_string(),
                    workflow:     "ticket".to_string(),
                },
                triggers:    vec![AutomationTrigger::Plane(sample_trigger())],
            })
            .await
            .unwrap();
        (dir, created, PlaneDispatchStore::new(database.clone_pool()))
    }

    fn sample_trigger() -> PlaneTrigger {
        PlaneTrigger {
            id:                    AutomationTriggerId::new("tickets").unwrap(),
            enabled:               true,
            project_id:            "proj".to_string(),
            ready_state_id:        "ready".to_string(),
            in_progress_state_id:  "progress".to_string(),
            done_state_id:         "in-review-uuid".to_string(),
            cancelled_state_id:    "cancelled".to_string(),
            failure_label_id:      Some("failed".to_string()),
            default_harness:       ExternalAgentHarness::Codex,
            codex_label_id:        Some("codex".to_string()),
            omp_label_id:          Some("omp".to_string()),
            poll_interval_seconds: 60,
            max_concurrency:       2,
            max_retries:           1,
        }
    }

    fn trigger(automation: &Automation) -> &PlaneTrigger {
        automation.enabled_plane_triggers().next().unwrap()
    }

    #[tokio::test]
    async fn claims_ready_tickets_in_priority_order_with_capacity() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![
            issue("low", "T-3", Some(4), &[]),
            issue("urgent", "T-1", Some(1), &[]),
            issue("high", "T-2", Some(2), &["omp"]),
        ];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();

        let started = runs.started.lock().unwrap().clone();
        assert_eq!(started.len(), 2);
        assert_eq!(started[0].0, "urgent");
        assert_eq!(started[0].1, ExternalAgentHarness::Codex);
        assert_eq!(started[1].0, "high");
        assert_eq!(started[1].1, ExternalAgentHarness::Omp);
        assert_eq!(
            plane.states.lock().unwrap().get("urgent").unwrap(),
            "progress"
        );
        assert!(
            plane
                .comments
                .lock()
                .unwrap()
                .iter()
                .any(|(id, body)| id == "urgent" && body.contains("/runs/"))
        );

        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        assert_eq!(runs.started.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn conflicting_labels_leave_ticket_untouched() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("both", "T-9", Some(1), &["codex", "omp"])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher = PlaneTicketDispatcher::new(store, &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        assert!(runs.started.lock().unwrap().is_empty());
        assert!(plane.states.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn retries_once_then_fails_in_progress() {
        let (_dir, mut automation, store) = setup().await;
        if let AutomationTrigger::Plane(trigger) = &mut automation.triggers[0] {
            trigger.max_retries = 10; // Even legacy over-permissive configuration cannot exceed two runs.
        }
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let first_run = runs.started.lock().unwrap().len();
        assert_eq!(first_run, 1);
        let run_id = store.list_for_automation(&automation.id).await.unwrap()[0]
            .dispatch
            .current_run_id
            .clone()
            .unwrap();
        runs.observed.lock().unwrap().insert(run_id, ObservedRun {
            status:           RunStatus::Failed {
                reason: FailureReason::WorkflowError,
            },
            pull_request_url: None,
            pr_pending:       false,
            pr_failed:        false,
            clarification:    None,
        });
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        assert_eq!(runs.started.lock().unwrap().len(), 2);
        let second = store.list_for_automation(&automation.id).await.unwrap()[0]
            .dispatch
            .current_run_id
            .clone()
            .unwrap();
        runs.observed.lock().unwrap().insert(second, ObservedRun {
            status:           RunStatus::Failed {
                reason: FailureReason::WorkflowError,
            },
            pull_request_url: None,
            pr_pending:       false,
            pr_failed:        false,
            clarification:    None,
        });
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(record.dispatch.status, PlaneDispatchStatus::Failed);
        assert_eq!(record.dispatch.attempt, 2);
        assert_eq!(plane.states.lock().unwrap().get("iss").unwrap(), "progress");
        assert_eq!(
            plane.labels.lock().unwrap()[0],
            ("iss".into(), "failed".into())
        );
    }

    #[tokio::test]
    async fn success_waits_for_pr_then_completes() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let run_id = store.list_for_automation(&automation.id).await.unwrap()[0]
            .dispatch
            .current_run_id
            .clone()
            .unwrap();
        runs.observed
            .lock()
            .unwrap()
            .insert(run_id.clone(), ObservedRun {
                status:           RunStatus::Succeeded {
                    reason: SuccessReason::Completed,
                },
                pull_request_url: None,
                pr_pending:       true,
                pr_failed:        false,
                clarification:    None,
            });
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        assert_eq!(
            store.list_for_automation(&automation.id).await.unwrap()[0]
                .dispatch
                .status,
            PlaneDispatchStatus::Running
        );
        runs.observed.lock().unwrap().insert(run_id, ObservedRun {
            status:           RunStatus::Succeeded {
                reason: SuccessReason::Completed,
            },
            pull_request_url: Some("https://github.com/o/r/pull/1".into()),
            pr_pending:       false,
            pr_failed:        false,
            clarification:    None,
        });
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(record.dispatch.status, PlaneDispatchStatus::Succeeded);
        assert_eq!(
            plane.states.lock().unwrap().get("iss").unwrap(),
            "in-review-uuid"
        );
    }

    #[tokio::test]
    async fn human_override_cancels_without_overwriting_state() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        plane.issues.lock().unwrap()[0].state = "review".to_string();
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(record.dispatch.status, PlaneDispatchStatus::Cancelled);
        assert_eq!(plane.issues.lock().unwrap()[0].state, "review");
        assert_eq!(runs.cancelled.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn plane_outage_does_not_claim_and_recovers() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        *plane.fail_fetch.lock().unwrap() = true;
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        assert!(runs.started.lock().unwrap().is_empty());
        *plane.fail_fetch.lock().unwrap() = false;
        dispatcher
            .tick_trigger(
                &automation,
                trigger(&automation),
                Utc::now() + chrono::Duration::seconds(60),
            )
            .await
            .unwrap();
        assert_eq!(runs.started.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn preflight_failure_does_not_move_ticket() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns::default();
        let mut dispatcher = PlaneTicketDispatcher::new(store, &plane, &runs, "http://fabro");
        let err = dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("preflight"));
        assert!(plane.states.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn poll_cadence_does_not_delay_reconciliation() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher = PlaneTicketDispatcher::new(store, &plane, &runs, "http://fabro");
        let now = Utc::now();
        for seconds in [0, 15, 59, 60] {
            dispatcher
                .tick_trigger(
                    &automation,
                    trigger(&automation),
                    now + chrono::Duration::seconds(seconds),
                )
                .await
                .unwrap();
        }
        assert_eq!(*plane.candidate_fetches.lock().unwrap(), 2);
        assert_eq!(*runs.observations.lock().unwrap(), 3);
        assert_eq!(runs.started.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reserved_identity_survives_ambiguous_start_and_restart() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            fail_start_once: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        assert!(
            dispatcher
                .tick_trigger(&automation, trigger(&automation), Utc::now())
                .await
                .is_err()
        );
        let reserved = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(reserved.dispatch.run_ids.len(), 1);
        assert_eq!(
            reserved.dispatch.current_run_id.as_ref(),
            reserved.dispatch.run_ids.first()
        );
        let mut restarted =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        restarted
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let recovered = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            recovered.dispatch.current_run_id,
            reserved.dispatch.current_run_id
        );
        assert_eq!(recovered.dispatch.status, PlaneDispatchStatus::Running);
        assert_eq!(runs.started.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn human_override_before_start_recovery_prevents_retry() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            fail_start_once: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        assert!(
            dispatcher
                .tick_trigger(&automation, trigger(&automation), Utc::now())
                .await
                .is_err()
        );
        plane.issues.lock().unwrap()[0].state = "backlog".into();
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(record.dispatch.status, PlaneDispatchStatus::Cancelled);
        assert_eq!(plane.issues.lock().unwrap()[0].state, "backlog");
        assert_eq!(runs.started.lock().unwrap().len(), 1);
        assert_eq!(runs.cancelled.lock().unwrap().len(), 1);
    }

    async fn completed_gated_run(state: &Arc<AppState>, run_id: RunId) {
        use fabro_types::{AttrValue, Graph, Node, Outcome};
        use fabro_workflow::event::Event;
        let mut graph = Graph::new("ticket");
        let mut node = Node::new("verify");
        node.attrs
            .insert("goal_gate".into(), AttrValue::Boolean(true));
        graph.nodes.insert("verify".into(), node);
        super::super::tests::create_durable_run_with_events(state, run_id, &[
            Event::RunCreated {
                run_id,
                title: None,
                settings: serde_json::to_value(fabro_types::WorkflowSettings::default()).unwrap(),
                graph: serde_json::to_value(graph).unwrap(),
                workflow_source: None,
                labels: BTreeMap::new(),
                source_directory: None,
                workflow_slug: None,
                automation: None,
                provenance: fabro_types::test_support::test_run_provenance(),
                manifest_blob: None,
                fork_source_ref: None,
                retried_from: None,
                parent_id: None,
                web_url: None,
                git: Some(fabro_types::GitContext {
                    origin_url: "https://github.com/acme/widgets".into(),
                    branch:     "main".into(),
                    sha:        None,
                    dirty:      fabro_types::DirtyStatus::Clean,
                }),
            },
            Event::CheckpointCompleted {
                node_id: "verify".into(),
                current_node: "verify".into(),
                status: "succeeded".into(),
                completed_nodes: vec!["verify".into()],
                node_retries: BTreeMap::new(),
                context_values: BTreeMap::new(),
                node_outcomes: BTreeMap::from([("verify".into(), Outcome::success())]),
                next_node_id: Some("exit".into()),
                git_commit_sha: Some("final-sha".into()),
                loop_failure_signatures: BTreeMap::new(),
                restart_failure_signatures: BTreeMap::new(),
                node_visits: BTreeMap::new(),
                diff: None,
                diff_summary: None,
                graph_visit: Some(1),
                resumed_from_stage_id: None,
            },
            Event::WorkflowRunCompleted {
                timing:               fabro_types::RunTiming::wall_only(1),
                artifact_count:       0,
                status:               "succeeded".into(),
                reason:               SuccessReason::Completed,
                total_usd_micros:     None,
                final_git_commit_sha: Some("final-sha".into()),
                final_patch:          Some("diff".into()),
                diff_summary:         None,
                billing:              None,
            },
        ])
        .await;
    }

    #[tokio::test]
    async fn production_observation_requires_creation_and_live_matching_draft() {
        use fabro_workflow::event::{Event, append_event};
        use serde_json::json;
        let github = httpmock::MockServer::start_async().await;
        let state = super::super::tests::create_github_token_app_state(
            Some("ghu_test"),
            Some(github.base_url()),
        );
        let run_id = RunId::new();
        completed_gated_run(&state, run_id).await;
        let runs = LiveRunPort {
            state: Arc::clone(&state),
        };
        assert!(
            runs.observe_run(&run_id)
                .await
                .unwrap()
                .pull_request_url
                .is_none()
        );
        let store = state.stores.runs.open_run(&run_id).await.unwrap();
        let creation_id = fabro_types::PullRequestCreationId::new();
        append_event(&store, &run_id, &Event::PullRequestCreationRequested {
            creation_id,
            model: "test".into(),
            force: false,
        })
        .await
        .unwrap();
        assert!(runs.observe_run(&run_id).await.unwrap().pr_pending);
        append_event(&store, &run_id, &Event::PullRequestFailed {
            creation_id: Some(creation_id),
            error:       "provider failed".into(),
        })
        .await
        .unwrap();
        assert!(runs.observe_run(&run_id).await.unwrap().pr_failed);
        append_event(&store, &run_id, &Event::PullRequestCreationRequested {
            creation_id: fabro_types::PullRequestCreationId::new(),
            model:       "test".into(),
            force:       false,
        })
        .await
        .unwrap();
        append_event(&store, &run_id, &Event::PullRequestCreated {
            pr_url:      "https://github.com/acme/widgets/pull/42".into(),
            pr_number:   42,
            owner:       "acme".into(),
            repo:        "widgets".into(),
            base_branch: "main".into(),
            head_branch: "feature".into(),
            head_sha:    Some("final-sha".into()),
            title:       "Fix".into(),
            draft:       true,
        })
        .await
        .unwrap();

        for (draft, sha, verified) in [
            (false, Some("final-sha"), false),
            (true, Some("stale"), false),
            (true, None, false),
            (true, Some("final-sha"), true),
        ] {
            let mut mock = github.mock(|when, then| {
                when.method("GET").path("/repos/acme/widgets/pulls/42");
                then.status(200).json_body(json!({
                    "number":42,"title":"Fix","body":"","state":"open","draft":draft,"merged":false,
                    "merged_at":null,"mergeable":true,"additions":1,"deletions":0,"changed_files":1,
                    "html_url":"https://github.com/acme/widgets/pull/42","user":{"login":"agent"},
                    "head":{"ref":"feature","sha":sha},"base":{"ref":"main"},
                    "created_at":"2026-09-08T00:00:00Z","updated_at":"2026-09-08T00:00:00Z"
                }));
            });
            let observed = runs.observe_run(&run_id).await.unwrap();
            assert!(!observed.pr_pending);
            assert!(!observed.pr_failed);
            assert_eq!(observed.pull_request_url.is_some(), verified);
            mock.assert();
            mock.delete();
        }
    }

    #[tokio::test]
    async fn clarification_uses_validated_checkpoint_only_while_blocked() {
        let state = crate::test_support::test_app_state();
        let run_id = RunId::new();
        completed_gated_run(&state, run_id).await;
        let mut projection = (*state
            .stores
            .runs
            .get_cached_projection(&run_id)
            .await
            .unwrap()
            .unwrap())
        .clone();
        let checkpoint = &mut projection.checkpoints.last_mut().unwrap().checkpoint;
        checkpoint.next_node_id = Some("clarification".into());
        checkpoint
            .node_outcomes
            .insert("parse_clarity".into(), fabro_types::Outcome::success());
        checkpoint.context_values.insert("ticket_clarity".into(), serde_json::json!({
            "schema_version":1,"status":"needs_clarification","questions":["Should <admin> users be included?"]
        }));
        assert!(
            clarification_comment(&run_id, &projection)
                .unwrap()
                .is_none()
        );
        projection.status = RunStatus::Blocked {
            blocked_reason: fabro_types::BlockedReason::HumanInputRequired,
        };
        let comment = clarification_comment(&run_id, &projection)
            .unwrap()
            .unwrap();
        assert!(comment.contains("Should &lt;admin&gt; users be included?"));
        projection.checkpoints.last_mut().unwrap().checkpoint.context_values.insert(
            "ticket_clarity".into(), serde_json::json!({"schema_version":1,"status":"needs_clarification","questions":[]})
        );
        assert!(clarification_comment(&run_id, &projection).is_err());
    }

    #[tokio::test]
    async fn failed_label_effect_is_not_terminal_or_acknowledged() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let mut record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        record.dispatch.attempt = 2;
        store.save(&record).await.unwrap();
        let id = record.dispatch.current_run_id.clone().unwrap();
        runs.observed.lock().unwrap().get_mut(&id).unwrap().status = RunStatus::Failed {
            reason: FailureReason::WorkflowError,
        };
        *plane.fail_label_once.lock().unwrap() = true;
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let pending = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(pending.dispatch.status, PlaneDispatchStatus::Running);
        assert!(!pending.effects.failure_label_applied);
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let done = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(done.dispatch.status, PlaneDispatchStatus::Failed);
        assert!(done.effects.failure_label_applied);
        assert_eq!(runs.started.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn stale_claim_is_not_reauthorized_after_restart() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        // A concurrent metadata edit may leave the state unchanged. A new tag
        // must still not silently authorize the rejected write.
        *plane.conflict_state.lock().unwrap() = Some("ready".into());
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        assert!(
            dispatcher
                .tick_trigger(&automation, trigger(&automation), Utc::now())
                .await
                .is_err()
        );
        let mut restarted =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        restarted
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        assert!(runs.started.lock().unwrap().is_empty());
        assert!(plane.states.lock().unwrap().is_empty());
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(record.dispatch.status, PlaneDispatchStatus::Cancelled);
        assert_eq!(plane.issues.lock().unwrap()[0].state, "ready");
    }

    #[tokio::test]
    async fn stale_completion_preserves_human_transition() {
        let (_dir, automation, store) = setup().await;
        let plane = FakePlane::default();
        *plane.issues.lock().unwrap() = vec![issue("iss", "T-1", Some(1), &[])];
        let runs = FakeRuns {
            preflight_ok: Mutex::new(true),
            ..FakeRuns::default()
        };
        let mut dispatcher =
            PlaneTicketDispatcher::new(store.clone(), &plane, &runs, "http://fabro");
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        let id = record.dispatch.current_run_id.unwrap();
        let mut observed = runs.observed.lock().unwrap();
        let run = observed.get_mut(&id).unwrap();
        run.status = RunStatus::Succeeded {
            reason: SuccessReason::Completed,
        };
        run.pull_request_url = Some("https://github.com/acme/widgets/pull/42".into());
        drop(observed);
        *plane.conflict_state.lock().unwrap() = Some("backlog".into());
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        dispatcher
            .tick_trigger(&automation, trigger(&automation), Utc::now())
            .await
            .unwrap();
        let record = store
            .list_for_automation(&automation.id)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(record.dispatch.status, PlaneDispatchStatus::Cancelled);
        assert!(!record.effects.success_state_applied);
        assert_eq!(plane.issues.lock().unwrap()[0].state, "backlog");
    }
}
