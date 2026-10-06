use anyhow::{Context as _, ensure};
use fabro_types::RunId;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use super::client::{Issue, ScanProgress};
use super::{IncidentStore, discovery_eligible, retry_deadline};

pub(super) fn digest(value: &impl serde::Serialize) -> anyhow::Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

pub(super) fn incident_key(origin: &str, project: u64, issue: Uuid) -> anyhow::Result<String> {
    digest(&(origin, project, issue.to_string()))
}

#[derive(Debug)]
pub(super) struct PendingIncident {
    pub key:        String,
    pub project:    u64,
    pub issue:      Uuid,
    pub generation: i64,
}

pub(super) struct Intent {
    pub run_id:          RunId,
    pub key:             String,
    pub project:         u64,
    pub issue:           Uuid,
    pub event:           Uuid,
    pub observation:     String,
    pub episode:         i64,
    pub state:           String,
    pub revision:        String,
    pub manifest_digest: Option<String>,
}

impl Intent {
    pub(super) fn inputs(&self) -> [(String, String); 5] {
        [
            (
                "incident".into(),
                format!("bugsink:{}:{}", self.project, self.issue),
            ),
            ("event".into(), self.event.to_string()),
            ("observation".into(), self.observation.clone()),
            ("episode".into(), self.episode.to_string()),
            ("run_id".into(), self.run_id.to_string()),
        ]
    }
}

impl IncidentStore {
    pub(super) async fn pending(
        &self,
        origin: &str,
        now: i64,
    ) -> anyhow::Result<Vec<PendingIncident>> {
        let rows = sqlx::query("SELECT incident_key, project_id, issue_id, refresh_generation FROM bugsink_incidents i
            WHERE origin=? AND project_id IN (25,26) AND requested_generation > applied_generation AND parked_reason IS NULL
            AND (next_read_ms IS NULL OR next_read_ms <= ?) AND NOT EXISTS
            (SELECT 1 FROM bugsink_runs r WHERE r.incident_key=i.incident_key AND r.state IN ('reserved','creating','submitted','active','uncertain'))
            AND NOT EXISTS (SELECT 1 FROM bugsink_runs r WHERE r.incident_key=i.incident_key AND r.observation_key=i.observation_key
                AND r.state='failed' AND r.attempt=1 AND NOT EXISTS (SELECT 1 FROM bugsink_runs next WHERE next.incident_key=r.incident_key AND next.observation_key=r.observation_key AND next.attempt>1))
            ORDER BY origin, project_id, issue_id LIMIT 32").bind(origin).bind(now).fetch_all(&self.pool).await?;
        let mut pending = Vec::new();
        for row in rows {
            let key: String = row.try_get("incident_key")?;
            let captured: Option<i64> = row.try_get("refresh_generation")?;
            let generation = match captured {
                Some(value) => Some(value),
                None => self.capture_refresh(&key).await?,
            };
            if let Some(generation) = generation {
                pending.push(PendingIncident {
                    key,
                    project: u64::try_from(row.try_get::<i64, _>("project_id")?)?,
                    issue: row.try_get::<String, _>("issue_id")?.parse()?,
                    generation,
                });
            }
        }
        Ok(pending)
    }

    pub(super) async fn read_failed(
        &self,
        pending: &PendingIncident,
        now: i64,
        reason: &str,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let attempts: i64 =
            sqlx::query_scalar("SELECT read_attempts FROM bugsink_incidents WHERE incident_key=?")
                .bind(&pending.key)
                .fetch_one(&mut *tx)
                .await?;
        let attempts = (attempts + 1).min(5);
        sqlx::query("UPDATE bugsink_incidents SET refresh_generation=NULL, read_attempts=?, next_read_ms=?, parked_reason=?,
            alert_reason=CASE WHEN requested_generation>refresh_generation THEN
                (SELECT reason FROM bugsink_deliveries d WHERE d.origin=bugsink_incidents.origin AND d.project_id=bugsink_incidents.project_id
                 AND d.issue_id=bugsink_incidents.issue_id AND d.reason!='TEST' ORDER BY d.rowid DESC LIMIT 1) ELSE alert_reason END
            WHERE incident_key=? AND refresh_generation=?")
            .bind(attempts).bind(retry_deadline(u8::try_from(attempts)?, now))
            .bind((attempts == 5).then_some(reason))
            .bind(&pending.key).bind(pending.generation).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn apply_observation(
        &self,
        origin: &str,
        pending: &PendingIncident,
        issue: &Issue,
        event: Uuid,
    ) -> anyhow::Result<()> {
        let observation = digest(&(
            origin,
            issue.project,
            issue.id.to_string(),
            event.to_string(),
            issue.resolved,
            issue.muted,
        ))?;
        let mut tx = self.pool.begin().await?;
        let old = sqlx::query("SELECT i.*, s.baseline_started_ms,
            EXISTS(SELECT 1 FROM bugsink_runs r WHERE r.incident_key=i.incident_key AND r.observation_key=i.observation_key) AS has_runs
            FROM bugsink_incidents i JOIN bugsink_scans s USING(origin,project_id)
            WHERE i.incident_key=? AND i.refresh_generation=? AND s.baseline_complete=1")
            .bind(&pending.key).bind(pending.generation).fetch_optional(&mut *tx).await?;
        let Some(old) = old else {
            return Ok(());
        };
        let previous: Option<String> = old.try_get("observation_key")?;
        let reason: Option<String> = old.try_get("alert_reason")?;
        let resolved: Option<bool> = old.try_get("is_resolved")?;
        let muted: Option<bool> = old.try_get("is_muted")?;
        let boundary: i64 = old.try_get("baseline_started_ms")?;
        let active = !issue.resolved && !issue.muted;
        let changed = previous.as_deref() != Some(observation.as_str());
        let waiting =
            previous.is_some() && reason.is_some() && !old.try_get::<bool, _>("has_runs")?;
        let eligible = active
            && (waiting
                || (changed
                    && (reason
                        .as_deref()
                        .is_some_and(|r| matches!(r, "REGRESSED" | "UNMUTED"))
                        || discovery_eligible(
                            true,
                            resolved.zip(muted),
                            (issue.resolved, issue.muted),
                            issue.first_seen_ms,
                            boundary,
                        )
                        || (previous.is_none()
                            && reason.as_deref() == Some("NEW")
                            && issue.first_seen_ms >= boundary))));
        let regression = eligible
            && changed
            && previous.is_some()
            && (resolved == Some(true) || reason.as_deref() == Some("REGRESSED"));
        // Keep the last eligible binding rather than turning ordinary repeated events
        // into work.
        let observed_event = if eligible {
            Some(event.to_string())
        } else {
            old.try_get::<Option<String>, _>("observed_event")?
        };
        let observation = if eligible {
            Some(observation)
        } else {
            previous
        };
        sqlx::query("UPDATE bugsink_incidents SET observation_key=?,observed_event=?,is_resolved=?,is_muted=?,
            episode=episode+?,applied_generation=?,refresh_generation=NULL,read_attempts=0,next_read_ms=NULL,
            alert_reason=CASE WHEN requested_generation>refresh_generation THEN
                (SELECT reason FROM bugsink_deliveries d WHERE d.origin=bugsink_incidents.origin AND d.project_id=bugsink_incidents.project_id
                 AND d.issue_id=bugsink_incidents.issue_id AND d.reason!='TEST' ORDER BY d.rowid DESC LIMIT 1) ELSE ? END
            WHERE incident_key=? AND refresh_generation=?")
            .bind(observation).bind(observed_event).bind(issue.resolved).bind(issue.muted).bind(i64::from(regression))
            .bind(pending.generation).bind(eligible.then_some(if regression { "REGRESSED" } else { "NEW" }))
            .bind(&pending.key).bind(pending.generation).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn scan_issue(
        &self,
        origin: &str,
        issue: &Issue,
        complete: bool,
        boundary: i64,
    ) -> anyhow::Result<()> {
        let key = incident_key(origin, issue.project, issue.id)?;
        let mut tx = self.pool.begin().await?;
        let old = sqlx::query("SELECT is_resolved,is_muted,requested_generation,applied_generation FROM bugsink_incidents WHERE incident_key=?")
            .bind(&key).fetch_optional(&mut *tx).await?;
        let prior = old
            .as_ref()
            .map(|row| {
                Ok::<_, sqlx::Error>((
                    row.try_get::<Option<bool>, _>("is_resolved")?,
                    row.try_get::<Option<bool>, _>("is_muted")?,
                ))
            })
            .transpose()?
            .and_then(|(a, b)| a.zip(b));
        let eligible = discovery_eligible(
            complete,
            prior,
            (issue.resolved, issue.muted),
            issue.first_seen_ms,
            boundary,
        );
        // Polling never clears read exhaustion or consumes an already-pending
        // notification.
        sqlx::query("INSERT INTO bugsink_incidents(incident_key,origin,project_id,issue_id,is_resolved,is_muted,requested_generation,alert_reason)
            VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(incident_key) DO UPDATE SET
            requested_generation=CASE WHEN ? AND requested_generation=applied_generation THEN requested_generation+1 ELSE requested_generation END,
            alert_reason=CASE WHEN ? AND requested_generation=applied_generation THEN excluded.alert_reason ELSE alert_reason END,
            is_resolved=CASE WHEN ? OR requested_generation>applied_generation OR EXISTS(SELECT 1 FROM bugsink_runs r WHERE r.incident_key=bugsink_incidents.incident_key AND r.state IN ('reserved','creating','submitted','active','uncertain')) THEN is_resolved ELSE excluded.is_resolved END,
            is_muted=CASE WHEN ? OR requested_generation>applied_generation OR EXISTS(SELECT 1 FROM bugsink_runs r WHERE r.incident_key=bugsink_incidents.incident_key AND r.state IN ('reserved','creating','submitted','active','uncertain')) THEN is_muted ELSE excluded.is_muted END")
            .bind(key).bind(origin).bind(i64::try_from(issue.project)?).bind(issue.id.to_string())
            .bind(issue.resolved).bind(issue.muted).bind(i64::from(eligible))
            .bind(eligible.then_some(if prior.is_some_and(|(resolved,_)| resolved) { "REGRESSED" } else if prior.is_some_and(|(_,muted)| muted) { "UNMUTED" } else { "NEW" }))
            .bind(eligible).bind(eligible).bind(eligible).bind(eligible).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn reserve(
        &self,
        origin: &str,
        project: u64,
        revision: &str,
    ) -> anyhow::Result<Option<RunId>> {
        let mut tx = self.pool.begin().await?;
        let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bugsink_runs WHERE state IN ('reserved','creating','submitted','active','uncertain'))")
            .fetch_one(&mut *tx).await?;
        if active {
            return Ok(None);
        }
        let selected = sqlx::query("SELECT i.incident_key,i.observation_key,i.episode,COALESCE(MAX(r.attempt),0) AS used,
            COALESCE(SUM(r.state='succeeded'),0) AS succeeded FROM bugsink_incidents i
            JOIN bugsink_scans s USING(origin,project_id) LEFT JOIN bugsink_runs r ON r.incident_key=i.incident_key AND r.observation_key=i.observation_key
            WHERE i.origin=? AND i.project_id=? AND s.baseline_complete=1 AND i.is_resolved=0 AND i.is_muted=0 AND i.observed_event IS NOT NULL
            AND i.parked_reason IS NULL AND i.alert_reason IS NOT NULL AND i.refresh_generation IS NULL
            GROUP BY i.incident_key HAVING succeeded=0 AND used<2 AND (i.requested_generation=i.applied_generation OR used=1)
            ORDER BY used DESC,i.incident_key LIMIT 1")
            .bind(origin).bind(i64::try_from(project)?)
            .fetch_optional(&mut *tx).await?;
        let Some(row) = selected else {
            return Ok(None);
        };
        let run_id = RunId::new();
        sqlx::query("INSERT INTO bugsink_runs(run_id,incident_key,observation_key,episode,attempt,authorized_by,state,source_revision)
            VALUES(?,?,?,?,?,NULL,'reserved',?)")
            .bind(run_id.to_string()).bind(row.try_get::<String,_>("incident_key")?).bind(row.try_get::<String,_>("observation_key")?)
            .bind(row.try_get::<i64,_>("episode")?).bind(row.try_get::<i64,_>("used")?+1).bind(revision)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(run_id))
    }

    pub(super) async fn intent(&self, run_id: RunId) -> anyhow::Result<Intent> {
        let row = sqlx::query("SELECT r.*,i.project_id,i.issue_id,i.observed_event FROM bugsink_runs r JOIN bugsink_incidents i USING(incident_key) WHERE run_id=?")
            .bind(run_id.to_string()).fetch_one(&self.pool).await?;
        Ok(Intent {
            run_id,
            key: row.try_get("incident_key")?,
            project: u64::try_from(row.try_get::<i64, _>("project_id")?)?,
            issue: row.try_get::<String, _>("issue_id")?.parse()?,
            event: row.try_get::<String, _>("observed_event")?.parse()?,
            observation: row.try_get("observation_key")?,
            episode: row.try_get("episode")?,
            state: row.try_get("state")?,
            revision: row.try_get("source_revision")?,
            manifest_digest: row.try_get("manifest_digest")?,
        })
    }

    pub(super) async fn active_runs(&self) -> anyhow::Result<Vec<RunId>> {
        let ids: Vec<String> = sqlx::query_scalar("SELECT run_id FROM bugsink_runs WHERE state IN ('reserved','creating','submitted','active','uncertain')")
            .fetch_all(&self.pool).await?;
        ids.into_iter()
            .map(|id| id.parse().map_err(Into::into))
            .collect()
    }

    pub(super) async fn transition_run(
        &self,
        intent: &Intent,
        state: &str,
        failure: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE bugsink_runs SET state=?,failure_class=? WHERE run_id=?")
            .bind(state)
            .bind(failure)
            .bind(intent.run_id.to_string())
            .execute(&mut *tx)
            .await?;
        if state == "failed" {
            sqlx::query("UPDATE bugsink_incidents SET parked_reason='investigation_exhausted' WHERE incident_key=? AND observation_key=?
                AND (SELECT COUNT(*) FROM bugsink_runs WHERE incident_key=? AND observation_key=?)>=2")
                .bind(&intent.key).bind(&intent.observation).bind(&intent.key).bind(&intent.observation).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn begin_create(&self, intent: &Intent, bytes: &[u8]) -> anyhow::Result<bool> {
        let digest = hex::encode(Sha256::digest(bytes));
        Ok(sqlx::query("UPDATE bugsink_runs SET state='creating',manifest_digest=? WHERE run_id=? AND state='reserved' AND manifest_digest IS NULL")
            .bind(digest).bind(intent.run_id.to_string()).execute(&self.pool).await?.rows_affected() == 1)
    }

    pub(super) async fn start_baseline(
        &self,
        origin: &str,
        projects: &[u64],
        now: i64,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bugsink_runs WHERE state IN ('reserved','creating','submitted','active','uncertain'))").fetch_one(&mut *tx).await?;
        ensure!(!active, "active_or_uncertain_run");
        for project in projects {
            sqlx::query("INSERT INTO bugsink_scans(origin,project_id,baseline_started_ms,cursor,scan_started_ms,next_scan_ms) VALUES(?,?,?,?,?,?)
                ON CONFLICT(origin,project_id) DO NOTHING")
                .bind(origin).bind(i64::try_from(*project)?).bind(now).bind(serde_json::to_string(&ScanProgress::default())?).bind(now).bind(now)
                .execute(&mut *tx).await?;
            let raw: String = sqlx::query_scalar(
                "SELECT cursor FROM bugsink_scans WHERE origin=? AND project_id=?",
            )
            .bind(origin)
            .bind(i64::try_from(*project)?)
            .fetch_one(&mut *tx)
            .await?;
            let mut progress: ScanProgress =
                serde_json::from_str(&raw).context("corrupt_scan_progress")?;
            // Explicit baseline resumes only scan/import errors; incident and Run budgets
            // are untouched.
            progress.read_attempts = 0;
            progress.parked_reason = None;
            sqlx::query(
                "UPDATE bugsink_scans SET cursor=?,next_scan_ms=? WHERE origin=? AND project_id=?
                AND (baseline_complete=0 OR json_extract(cursor,'$.parked_reason') IS NOT NULL)",
            )
            .bind(serde_json::to_string(&progress)?)
            .bind(now)
            .bind(origin)
            .bind(i64::try_from(*project)?)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn operator_retry(
        &self,
        origin: &str,
        issue: &Issue,
        event: Uuid,
        actor: &str,
        allow_additional_run: bool,
        revision: &str,
        generation: i64,
        now: i64,
    ) -> anyhow::Result<()> {
        ensure!(!actor.trim().is_empty(), "operator_identity_missing");
        let key = incident_key(origin, issue.project, issue.id)?;
        let observation = digest(&(
            origin,
            issue.project,
            issue.id.to_string(),
            event.to_string(),
            issue.resolved,
            issue.muted,
        ))?;
        let mut tx = self.pool.begin().await?;
        let blocked: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bugsink_runs WHERE state IN ('reserved','creating','submitted','active','uncertain') AND (? OR incident_key=?))")
            .bind(allow_additional_run).bind(&key).fetch_one(&mut *tx).await?;
        ensure!(!blocked, "active_or_uncertain_run");
        let old=sqlx::query("SELECT episode,observation_key,is_resolved,requested_generation,applied_generation,refresh_generation FROM bugsink_incidents WHERE incident_key=?")
            .bind(&key).fetch_optional(&mut *tx).await?.context("incident_not_found")?;
        ensure!(
            old.try_get::<Option<i64>, _>("refresh_generation")?
                .is_none()
                && old.try_get::<i64, _>("applied_generation")? <= generation
                && generation <= old.try_get::<i64, _>("requested_generation")?,
            "refresh_in_progress"
        );
        let prior: Option<String> = old.try_get("observation_key")?;
        let changed = prior.as_deref() != Some(observation.as_str());
        let episode = old.try_get::<i64, _>("episode")?
            + i64::from(
                changed
                    && prior.is_some()
                    && old.try_get::<Option<bool>, _>("is_resolved")? == Some(true),
            );
        let used: i64=sqlx::query_scalar("SELECT COALESCE(MAX(attempt),0) FROM bugsink_runs WHERE incident_key=? AND observation_key=?")
            .bind(&key).bind(&observation).fetch_one(&mut *tx).await?;
        let succeeded: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bugsink_runs WHERE incident_key=? AND observation_key=? AND state='succeeded')")
            .bind(&key).bind(&observation).fetch_one(&mut *tx).await?;
        ensure!(
            !allow_additional_run || (!succeeded && !issue.resolved && !issue.muted),
            "observation_not_dispatchable"
        );
        let row = sqlx::query(
            "SELECT cursor,baseline_complete FROM bugsink_scans WHERE origin=? AND project_id=?",
        )
        .bind(origin)
        .bind(i64::try_from(issue.project)?)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            row.try_get::<bool, _>("baseline_complete")?,
            "baseline_incomplete"
        );
        let mut progress: ScanProgress =
            serde_json::from_str(&row.try_get::<String, _>("cursor")?)?;
        // The operator identity is retained in the private durable cursor journal,
        // never in GET. ponytail: low-volume operator audit shares scan
        // progress; split into an audit table if this becomes high-volume.
        progress.operator_audit.push(json!({"operator":actor,"incident_key":key,"at_ms":now,"allow_additional_run":allow_additional_run}));
        let parked =
            (used >= 2 && !allow_additional_run && !succeeded).then_some("investigation_exhausted");
        sqlx::query("UPDATE bugsink_incidents SET observation_key=?,observed_event=?,is_resolved=?,is_muted=?,episode=?,
            applied_generation=?,refresh_generation=NULL,read_attempts=0,next_read_ms=NULL,parked_reason=?,alert_reason=? WHERE incident_key=?")
            .bind(&observation).bind(event.to_string()).bind(issue.resolved).bind(issue.muted).bind(episode).bind(generation).bind(parked)
            .bind((!issue.resolved && !issue.muted && !succeeded).then_some("NEW")).bind(&key).execute(&mut *tx).await?;
        if allow_additional_run {
            let id = RunId::new();
            sqlx::query("INSERT INTO bugsink_runs(run_id,incident_key,observation_key,episode,attempt,authorized_by,state,source_revision)
                VALUES(?,?,?,?,?,?,'reserved',?)")
                .bind(id.to_string()).bind(&key).bind(&observation).bind(episode).bind(used+1).bind(actor).bind(revision)
                .execute(&mut *tx).await?;
        }
        sqlx::query("UPDATE bugsink_scans SET cursor=? WHERE origin=? AND project_id=?")
            .bind(serde_json::to_string(&progress)?)
            .bind(origin)
            .bind(i64::try_from(issue.project)?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}
