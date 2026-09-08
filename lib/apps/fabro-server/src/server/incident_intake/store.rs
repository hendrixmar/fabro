use anyhow::{Context as _, ensure};
use fabro_db::DbPool;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{AcceptResult, AcceptedAlert, AlertReason};

pub(crate) struct IncidentStore {
    pub(super) pool: DbPool,
}

impl IncidentStore {
    pub(crate) fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// The caller supplies the exact configured origin, never a payload URL.
    pub(crate) async fn accept(
        &self,
        origin: &str,
        alert: &AcceptedAlert,
    ) -> anyhow::Result<AcceptResult> {
        ensure!(!origin.is_empty() && origin.trim() == origin, "invalid configured origin");
        let project_id = i64::try_from(alert.project_id).context("project ID exceeds SQLite bounds")?;
        let issue_id = alert.issue_id.to_string();
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO bugsink_deliveries (origin, project_id, body_digest, issue_id, reason, received_ms)
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(origin, project_id, body_digest) DO NOTHING",
        )
        .bind(origin)
        .bind(project_id)
        .bind(&alert.body_digest)
        .bind(&issue_id)
        .bind(alert.reason.as_str())
        .bind(alert.received_ms)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let result = if inserted == 0 {
            AcceptResult::Duplicate
        } else if alert.reason == AlertReason::Test {
            AcceptResult::Test
        } else {
            // JSON tuple encoding avoids delimiter collisions in the stable identity.
            let identity = serde_json::to_vec(&(origin, alert.project_id, &issue_id))?;
            let incident_key = hex::encode(Sha256::digest(identity));
            sqlx::query(
                "INSERT INTO bugsink_incidents
                 (incident_key, origin, project_id, issue_id, requested_generation, alert_reason)
                 VALUES (?, ?, ?, ?, 1, ?)
                 ON CONFLICT(origin, project_id, issue_id) DO UPDATE SET
                 requested_generation = requested_generation + 1, alert_reason = excluded.alert_reason",
            )
            .bind(incident_key)
            .bind(origin)
            .bind(project_id)
            .bind(issue_id)
            .bind(alert.reason.as_str())
            .execute(&mut *tx)
            .await?;
            AcceptResult::Queued
        };
        tx.commit().await?;
        Ok(result)
    }

    /// Persist the generation before doing authoritative HTTP work outside a transaction.
    pub(crate) async fn capture_refresh(&self, incident_key: &str) -> anyhow::Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "UPDATE bugsink_incidents SET refresh_generation = requested_generation
             WHERE incident_key = ? AND requested_generation > applied_generation
             AND refresh_generation IS NULL AND parked_reason IS NULL
             RETURNING refresh_generation",
        )
        .bind(incident_key)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Complete only the captured refresh; later notifications remain pending.
    /// Workers applying observation fields must do so in this same UPDATE/transaction.
    pub(crate) async fn complete_refresh(&self, incident_key: &str, generation: i64) -> anyhow::Result<()> {
        let changed = sqlx::query(
            "UPDATE bugsink_incidents SET applied_generation = ?, refresh_generation = NULL,
             read_attempts = 0, next_read_ms = NULL
             WHERE incident_key = ? AND refresh_generation = ?",
        )
        .bind(generation)
        .bind(incident_key)
        .bind(generation)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(changed == 1, "refresh generation is no longer current");
        Ok(())
    }

    /// Operator data only: no delivery bodies, signing material, or upstream cursors.
    /// The handler adds { data: snapshot, meta: { enabled, dispatch_enabled } }.
    pub(crate) async fn snapshot(&self) -> anyhow::Result<Value> {
        let mut tx = self.pool.begin().await?;
        let incidents: Vec<String> = sqlx::query_scalar(
            "SELECT json_object(
                'incident_key', incident_key, 'origin', origin, 'project_id', project_id,
                'issue_id', issue_id, 'observation_key', observation_key, 'observed_event', observed_event,
                'is_resolved', json(CASE is_resolved WHEN 1 THEN 'true' WHEN 0 THEN 'false' ELSE 'null' END),
                'is_muted', json(CASE is_muted WHEN 1 THEN 'true' WHEN 0 THEN 'false' ELSE 'null' END),
                'episode', episode, 'requested_generation', requested_generation,
                'applied_generation', applied_generation, 'refresh_generation', refresh_generation,
                'alert_reason', alert_reason, 'read_attempts', read_attempts,
                'next_read_ms', next_read_ms, 'parked_reason', parked_reason,
                'status', CASE WHEN parked_reason IS NOT NULL THEN 'parked'
                    WHEN refresh_generation IS NOT NULL THEN 'refreshing'
                    WHEN requested_generation > applied_generation THEN 'pending' ELSE 'current' END
             ) FROM bugsink_incidents ORDER BY origin, project_id, issue_id",
        ).fetch_all(&mut *tx).await?;
        let runs: Vec<String> = sqlx::query_scalar(
            "SELECT json_object('run_id', run_id, 'incident_key', incident_key,
                'observation_key', observation_key, 'episode', episode, 'attempt', attempt,
                'state', state, 'failure_class', failure_class)
             FROM bugsink_runs ORDER BY incident_key, observation_key, attempt",
        ).fetch_all(&mut *tx).await?;
        let scans: Vec<String> = sqlx::query_scalar(
            "SELECT json_object('origin', origin, 'project_id', project_id,
                'baseline_started_ms', baseline_started_ms,
                'baseline_complete', json(CASE baseline_complete WHEN 1 THEN 'true' ELSE 'false' END),
                'scan_started_ms', scan_started_ms, 'next_scan_ms', next_scan_ms)
             FROM bugsink_scans ORDER BY origin, project_id",
        ).fetch_all(&mut *tx).await?;
        let delivery_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bugsink_deliveries")
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        let parse_rows = |rows: Vec<String>| -> serde_json::Result<Vec<Value>> {
            rows.iter().map(|row| serde_json::from_str(row)).collect()
        };
        Ok(json!({
            "incidents": parse_rows(incidents)?,
            "runs": parse_rows(runs)?,
            "scans": parse_rows(scans)?,
            "delivery_count": delivery_count,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGIN: &str = "https://bugsink.example";

    fn alert(reason: AlertReason, digest: char) -> AcceptedAlert {
        AcceptedAlert {
            project_id: 7,
            issue_id: uuid::Uuid::from_u128(42),
            reason,
            body_digest: digest.to_string().repeat(64),
            received_ms: 1000,
        }
    }

    async fn database(path: &std::path::Path) -> fabro_db::Database {
        let db = fabro_db::Database::connect(path).await.unwrap();
        db.migrate().await.unwrap();
        db
    }

    #[tokio::test]
    async fn concurrent_duplicate_is_durable_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intake.sqlite");
        let db = database(&path).await;
        let other_db = database(&path).await;
        let first = IncidentStore::new(db.clone_pool());
        let second = IncidentStore::new(other_db.clone_pool());
        let alert = alert(AlertReason::New, 'a');
        let (one, two) = tokio::join!(first.accept(ORIGIN, &alert), second.accept(ORIGIN, &alert));
        assert!(matches!((one.unwrap(), two.unwrap()),
            (AcceptResult::Queued, AcceptResult::Duplicate) |
            (AcceptResult::Duplicate, AcceptResult::Queued)));
        db.pool().close().await;
        other_db.pool().close().await;
        let reopened = database(&path).await;
        let snapshot = IncidentStore::new(reopened.clone_pool()).snapshot().await.unwrap();
        assert_eq!(snapshot["delivery_count"], 1);
        assert_eq!(snapshot["incidents"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["incidents"][0]["requested_generation"], 1);
    }

    #[tokio::test]
    async fn test_audits_without_dirtying_incident_and_errors_do_not_accept() {
        let dir = tempfile::tempdir().unwrap();
        let db = database(&dir.path().join("intake.sqlite")).await;
        let store = IncidentStore::new(db.clone_pool());
        assert_eq!(store.accept(ORIGIN, &alert(AlertReason::Test, 'a')).await.unwrap(), AcceptResult::Test);
        assert_eq!(store.snapshot().await.unwrap()["incidents"], serde_json::json!([]));
        store.accept(ORIGIN, &alert(AlertReason::New, 'b')).await.unwrap();
        let before = store.snapshot().await.unwrap();
        store.accept(ORIGIN, &alert(AlertReason::Test, 'c')).await.unwrap();
        let after = store.snapshot().await.unwrap();
        assert_eq!(before["incidents"], after["incidents"]);
        assert_eq!(after["delivery_count"], 3);
        // Failure after the delivery insert must roll the entire acceptance back.
        sqlx::query("CREATE TRIGGER reject_incident BEFORE UPDATE ON bugsink_incidents BEGIN SELECT RAISE(ABORT, 'storage failure'); END")
            .execute(db.pool()).await.unwrap();
        assert!(store.accept(ORIGIN, &alert(AlertReason::Unmuted, 'd')).await.is_err());
        assert_eq!(store.snapshot().await.unwrap(), after);
        db.pool().close().await;
        assert!(store.accept(ORIGIN, &alert(AlertReason::Regressed, 'e')).await.is_err());
    }

    #[tokio::test]
    async fn notification_during_refresh_remains_pending() {
        let dir = tempfile::tempdir().unwrap();
        let db = database(&dir.path().join("intake.sqlite")).await;
        let store = IncidentStore::new(db.clone_pool());
        store.accept(ORIGIN, &alert(AlertReason::New, 'a')).await.unwrap();
        let key = store.snapshot().await.unwrap()["incidents"][0]["incident_key"].as_str().unwrap().to_owned();
        let generation = store.capture_refresh(&key).await.unwrap().unwrap();
        store.accept(ORIGIN, &alert(AlertReason::Regressed, 'b')).await.unwrap();
        store.complete_refresh(&key, generation).await.unwrap();
        let row = &store.snapshot().await.unwrap()["incidents"][0];
        assert_eq!(row["applied_generation"], 1);
        assert_eq!(row["requested_generation"], 2);
        assert_eq!(store.capture_refresh(&key).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn uncertain_run_holds_global_slot_and_attempts_need_authorization() {
        let dir = tempfile::tempdir().unwrap();
        let db = database(&dir.path().join("intake.sqlite")).await;
        let store = IncidentStore::new(db.clone_pool());
        store.accept(ORIGIN, &alert(AlertReason::New, 'a')).await.unwrap();
        let key = store.snapshot().await.unwrap()["incidents"][0]["incident_key"].as_str().unwrap().to_owned();
        let insert = "INSERT INTO bugsink_runs (run_id, incident_key, observation_key, episode, attempt, state, source_revision) VALUES (?, ?, ?, 1, ?, ?, 'revision')";
        sqlx::query(insert).bind("first").bind(&key).bind("observation").bind(1).bind("uncertain").execute(db.pool()).await.unwrap();
        assert!(sqlx::query(insert).bind("second").bind(&key).bind("different").bind(1).bind("reserved").execute(db.pool()).await.is_err());
        sqlx::query("UPDATE bugsink_runs SET state = 'failed' WHERE run_id = 'first'").execute(db.pool()).await.unwrap();
        assert!(sqlx::query(insert).bind("third").bind(&key).bind("observation").bind(3).bind("reserved").execute(db.pool()).await.is_err());
        sqlx::query(insert).bind("second").bind(&key).bind("observation").bind(2).bind("reserved").execute(db.pool()).await.unwrap();
    }
}
