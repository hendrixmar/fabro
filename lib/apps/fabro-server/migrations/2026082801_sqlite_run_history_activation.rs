//! Fail-closed activation of SQLite run history.
//!
//! This compatibility bridge remains for at least 30 days after the persisted
//! first-success timestamp, and until cold-start, warm-restart, production
//! observation, rollback-backup, deletion, and concurrent-reader evidence has
//! been accepted and Scott explicitly approves removal. The computed date is
//! an eligibility floor, never an automatic deletion trigger.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use tokio::fs;
use tracing::{info, warn};

use crate::migrations::sqlite_activation_backup::{self, BackupError};

pub(crate) const BACKUP_SUFFIX: &str = ".pre-run-history-activation.bak";
const REMOVAL_WINDOW: Duration = Duration::days(30);

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActivationRecord {
    source_fingerprint: Vec<u8>,
    source_runs:        u64,
    source_events:      u64,
    activated_at_ms:    i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RunHistoryActivationError {
    #[error("canonicalizing the SQLite database path {path}")]
    Canonicalize {
        path:   PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("reading the SQLite run-history activation state")]
    ActivationState(#[source] sqlx::Error),
    #[error("the persisted run-history activation marker does not match the legacy source")]
    MarkerMismatch,
    #[error("the persisted run-history activation marker contains an invalid count or timestamp")]
    InvalidMarker,
    #[error(
        "SQLite contains {target_runs} run rows and {target_events} run events, but the legacy run-history source is empty and no activation marker exists"
    )]
    EmptySourceWithTarget {
        target_runs:   u64,
        target_events: u64,
    },
    #[error(
        "run-history activation backup is missing at {path} after SQLite import progress was recorded"
    )]
    MissingBackupAfterProgress { path: PathBuf },
    #[error(transparent)]
    Backup(#[from] BackupError),
    #[error("importing legacy run history into SQLite")]
    Import(#[source] Box<fabro_store::LegacyRunHistoryImportError>),
    #[error("verifying legacy and SQLite run history")]
    Verification(#[source] Box<fabro_store::LegacyRunHistoryVerificationError>),
    #[error("running the live SQLite integrity check")]
    LiveIntegrity(#[source] sqlx::Error),
    #[error("the live SQLite integrity check failed")]
    LiveIntegrityFailed,
    #[error("persisting the SQLite run-history activation marker")]
    PersistMarker(#[source] sqlx::Error),
    #[error("the run-history activation timestamp is outside the supported range")]
    InvalidActivationTimestamp,
    #[error("running the final SQLite WAL truncate checkpoint")]
    FinalCheckpoint(#[source] sqlx::Error),
    #[error("a run-history activation count exceeds SQLite's integer range")]
    CountOverflow,
}

pub(crate) async fn activate_run_history(
    database: &fabro_db::Database,
    sqlite_path: &Path,
    store: &fabro_store::Database,
    identity: &fabro_store::LegacyRunHistorySourceIdentity,
) -> Result<(), RunHistoryActivationError> {
    let canonical_path = fs::canonicalize(sqlite_path).await.map_err(|source| {
        RunHistoryActivationError::Canonicalize {
            path: sqlite_path.to_path_buf(),
            source,
        }
    })?;
    let backup_path = fabro_db::append_to_path(&canonical_path, BACKUP_SUFFIX);
    info!(
        database_path = %canonical_path.display(),
        backup_path = %backup_path.display(),
        "Starting SQLite run-history activation"
    );

    let marker = read_activation_record(database.pool()).await?;
    let (target_runs, target_events) = target_counts(database.pool()).await?;

    if let Some(record) = &marker {
        verify_marker(record, identity)?;
    } else if identity.events == 0 && (target_runs != 0 || target_events != 0) {
        return Err(RunHistoryActivationError::EmptySourceWithTarget {
            target_runs,
            target_events,
        });
    }

    let backup_present = sqlite_activation_backup::backup_exists(&backup_path).await?;
    if backup_present {
        sqlite_activation_backup::validate_backup(&backup_path).await?;
    }
    let import_progress = target_events != 0 || marker.is_some();
    if identity.events != 0 && import_progress && !backup_present {
        return Err(RunHistoryActivationError::MissingBackupAfterProgress { path: backup_path });
    }
    let backup_required = identity.events != 0 && !backup_present;
    if backup_required {
        sqlite_activation_backup::create_backup(database.pool(), &backup_path).await?;
    }

    let import = store
        .import_legacy_run_history_into(database.pool())
        .await
        .map_err(|source| RunHistoryActivationError::Import(Box::new(source)))?;
    let verification = store
        .verify_legacy_run_history_in(database.pool())
        .await
        .map_err(|source| RunHistoryActivationError::Verification(Box::new(source)))?;
    validate_live_integrity(database.pool()).await?;

    let activated_at_ms = marker.as_ref().map_or_else(
        || Utc::now().timestamp_millis(),
        |record| record.activated_at_ms,
    );
    persist_activation_record(database.pool(), identity, activated_at_ms).await?;
    final_truncate_checkpoint(database.pool()).await?;

    let activated_at = DateTime::<Utc>::from_timestamp_millis(activated_at_ms)
        .ok_or(RunHistoryActivationError::InvalidActivationTimestamp)?;
    let removal_eligible_at = activated_at + REMOVAL_WINDOW;
    info!(
        source_runs = identity.runs,
        source_events = identity.events,
        imported_runs = import.imported_runs,
        imported_events = import.imported_events,
        existing_runs = import.verified_existing_runs,
        existing_events = import.verified_existing_events,
        tombstoned_source_runs = verification.tombstoned_source_runs,
        tombstoned_source_events = verification.tombstoned_source_events,
        target_runs = verification.target_runs,
        target_events = verification.target_events,
        sql_only_runs = verification.sql_only_runs,
        sql_only_events = verification.sql_only_events,
        backup_required,
        backup_path = %backup_path.display(),
        activated_at = %activated_at,
        removal_eligible_at = %removal_eligible_at,
        "Activated SQLite run history"
    );
    Ok(())
}

async fn read_activation_record(
    pool: &sqlx::SqlitePool,
) -> Result<Option<ActivationRecord>, RunHistoryActivationError> {
    let row = sqlx::query_as::<_, (Vec<u8>, i64, i64, i64)>(
        r"
SELECT source_fingerprint, source_runs, source_events, activated_at_ms
FROM legacy_run_history_activation
WHERE singleton = 1
",
    )
    .fetch_optional(pool)
    .await
    .map_err(RunHistoryActivationError::ActivationState)?;
    row.map(
        |(source_fingerprint, source_runs, source_events, activated_at_ms)| {
            Ok(ActivationRecord {
                source_fingerprint,
                source_runs: u64::try_from(source_runs)
                    .map_err(|_| RunHistoryActivationError::InvalidMarker)?,
                source_events: u64::try_from(source_events)
                    .map_err(|_| RunHistoryActivationError::InvalidMarker)?,
                activated_at_ms,
            })
        },
    )
    .transpose()
}

fn verify_marker(
    marker: &ActivationRecord,
    identity: &fabro_store::LegacyRunHistorySourceIdentity,
) -> Result<(), RunHistoryActivationError> {
    if marker.activated_at_ms < 0 {
        return Err(RunHistoryActivationError::InvalidMarker);
    }
    if marker.source_fingerprint.as_slice() != identity.fingerprint()
        || marker.source_runs != identity.runs
        || marker.source_events != identity.events
    {
        return Err(RunHistoryActivationError::MarkerMismatch);
    }
    Ok(())
}

async fn target_counts(pool: &sqlx::SqlitePool) -> Result<(u64, u64), RunHistoryActivationError> {
    let (runs, events): (i64, i64) =
        sqlx::query_as("SELECT (SELECT COUNT(*) FROM runs), (SELECT COUNT(*) FROM run_events)")
            .fetch_one(pool)
            .await
            .map_err(RunHistoryActivationError::ActivationState)?;
    Ok((
        u64::try_from(runs).map_err(|_| RunHistoryActivationError::CountOverflow)?,
        u64::try_from(events).map_err(|_| RunHistoryActivationError::CountOverflow)?,
    ))
}

async fn persist_activation_record(
    pool: &sqlx::SqlitePool,
    identity: &fabro_store::LegacyRunHistorySourceIdentity,
    activated_at_ms: i64,
) -> Result<(), RunHistoryActivationError> {
    let source_runs =
        i64::try_from(identity.runs).map_err(|_| RunHistoryActivationError::CountOverflow)?;
    let source_events =
        i64::try_from(identity.events).map_err(|_| RunHistoryActivationError::CountOverflow)?;
    // A pre-existing marker was already verified against `identity` above, so
    // leaving it untouched on conflict keeps the original activation time.
    sqlx::query(
        r"
INSERT INTO legacy_run_history_activation (
    singleton, source_fingerprint, source_runs, source_events, activated_at_ms
) VALUES (1, ?, ?, ?, ?)
ON CONFLICT(singleton) DO NOTHING
",
    )
    .bind(identity.fingerprint().as_slice())
    .bind(source_runs)
    .bind(source_events)
    .bind(activated_at_ms)
    .execute(pool)
    .await
    .map_err(RunHistoryActivationError::PersistMarker)?;
    Ok(())
}

async fn validate_live_integrity(pool: &sqlx::SqlitePool) -> Result<(), RunHistoryActivationError> {
    let ok = sqlite_activation_backup::integrity_check_is_ok(pool)
        .await
        .map_err(RunHistoryActivationError::LiveIntegrity)?;
    if !ok {
        return Err(RunHistoryActivationError::LiveIntegrityFailed);
    }
    Ok(())
}

async fn final_truncate_checkpoint(
    pool: &sqlx::SqlitePool,
) -> Result<(), RunHistoryActivationError> {
    let (busy, _, _): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_one(pool)
        .await
        .map_err(RunHistoryActivationError::FinalCheckpoint)?;
    if busy != 0 {
        // A concurrent reader can keep the WAL from truncating, but all
        // activation data is already committed and remains durable in that
        // WAL. A later checkpoint can truncate it after the reader exits.
        warn!(
            "The final SQLite run-history WAL truncate checkpoint could not complete; continuing startup"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration as StdDuration;

    use chrono::{TimeZone as _, Utc};
    use fabro_types::{Graph, RunId, WorkflowSettings, test_support};
    use object_store::memory::InMemory;
    use sqlx::Connection as _;
    use tokio::fs;
    use ulid::Ulid;

    use super::{
        BACKUP_SUFFIX, RunHistoryActivationError, activate_run_history, final_truncate_checkpoint,
        read_activation_record,
    };

    type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

    struct TestContext {
        _directory:  tempfile::TempDir,
        sqlite_path: PathBuf,
        database:    fabro_db::Database,
        store:       Arc<fabro_store::Database>,
    }

    impl TestContext {
        async fn new(prefix: &str) -> TestResult<Self> {
            let directory = tempfile::tempdir()?;
            let sqlite_path = directory.path().join("fabro.sqlite3");
            let database = fabro_db::Database::connect(&sqlite_path).await?;
            database.migrate().await?;
            let store = Arc::new(fabro_store::Database::new(
                Arc::new(InMemory::new()),
                prefix,
                StdDuration::from_millis(1),
                None,
                Arc::new(fabro_store::BlobStore::new(database.clone_pool())),
                Arc::new(fabro_store::RunSummaryStore::new(database.clone_pool())),
            ));
            Ok(Self {
                _directory: directory,
                sqlite_path,
                database,
                store,
            })
        }

        async fn put_event(
            &self,
            run_id: &RunId,
            seq: u32,
            event: &str,
            properties: serde_json::Value,
        ) -> TestResult<()> {
            let payload = serde_json::json!({
                "id": format!("evt-{seq}-{event}"),
                "ts": Utc
                    .timestamp_millis_opt(1_788_000_000_000 + i64::from(seq))
                    .single()
                    .unwrap()
                    .to_rfc3339(),
                "run_id": run_id.to_string(),
                "event": event,
                "properties": properties,
            });
            fabro_store::test_support::put_legacy_run_event(&self.store, run_id, seq, &payload)
                .await?;
            Ok(())
        }

        async fn put_created(&self, run_id: &RunId) -> TestResult<()> {
            self.put_event(
                run_id,
                1,
                "run.created",
                serde_json::json!({
                    "title": "Activation test",
                    "settings": WorkflowSettings::default(),
                    "graph": Graph::new("test"),
                    "workflow_slug": "test-workflow",
                    "labels": {},
                    "provenance": test_support::test_run_provenance(),
                }),
            )
            .await
        }

        async fn source_identity(&self) -> TestResult<fabro_store::LegacyRunHistorySourceIdentity> {
            Ok(self.store.legacy_run_history_source_identity().await?)
        }

        fn backup_path(&self) -> PathBuf {
            fabro_db::append_to_path(&self.sqlite_path, BACKUP_SUFFIX)
        }
    }

    fn run_id() -> RunId {
        RunId::from(Ulid::from_parts(1_788_000_000_000, 1))
    }

    #[tokio::test]
    async fn cold_activation_imports_and_warm_restart_preserves_marker() -> TestResult<()> {
        let context = TestContext::new("cold-and-warm-run-activation").await?;
        let run_id = run_id();
        context.put_created(&run_id).await?;
        context
            .put_event(&run_id, 2, "run.submitted", serde_json::json!({}))
            .await?;

        let identity = context.source_identity().await?;
        activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &identity,
        )
        .await?;
        let first_marker = read_activation_record(context.database.pool())
            .await?
            .unwrap();
        assert_eq!(first_marker.source_runs, 1);
        assert_eq!(first_marker.source_events, 2);
        assert!(context.backup_path().is_file());
        let backup_options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(context.backup_path())
            .read_only(true)
            .create_if_missing(false);
        let mut backup = sqlx::SqliteConnection::connect_with(&backup_options).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM run_events")
                .fetch_one(&mut backup)
                .await?,
            0,
            "the retained backup must capture the exact pre-import boundary"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM run_events")
                .fetch_one(context.database.pool())
                .await?,
            2
        );

        activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &identity,
        )
        .await?;
        assert_eq!(
            read_activation_record(context.database.pool()).await?,
            Some(first_marker)
        );
        Ok(())
    }

    #[tokio::test]
    async fn busy_final_checkpoint_warns_and_does_not_fail_activation() -> TestResult<()> {
        use sqlx::sqlite::{SqliteJournalMode, SqlitePoolOptions};

        let context = TestContext::new("busy-final-run-checkpoint").await?;
        sqlx::query("INSERT INTO blobs (hash, data) VALUES (?, ?)")
            .bind(fabro_types::BlobHash::new(b"wal-content").to_string())
            .bind(b"wal-content".as_slice())
            .execute(context.database.pool())
            .await?;

        let reader_options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&context.sqlite_path)
            .read_only(true)
            .create_if_missing(false);
        let mut reader = sqlx::SqliteConnection::connect_with(&reader_options).await?;
        sqlx::query("BEGIN").execute(&mut reader).await?;
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blobs")
            .fetch_one(&mut reader)
            .await?;

        let checkpoint_options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&context.sqlite_path)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(StdDuration::from_millis(50))
            .create_if_missing(false);
        let checkpoint_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(checkpoint_options)
            .await?;

        final_truncate_checkpoint(&checkpoint_pool).await?;

        let wal_bytes = fs::metadata(fabro_db::append_to_path(&context.sqlite_path, "-wal"))
            .await?
            .len();
        assert!(wal_bytes > 0, "the WAL should remain untruncated");
        drop(reader);
        Ok(())
    }

    #[tokio::test]
    async fn changed_legacy_source_fails_before_mutating_sqlite() -> TestResult<()> {
        let context = TestContext::new("changed-run-activation-source").await?;
        let run_id = run_id();
        context.put_created(&run_id).await?;
        let original_identity = context.source_identity().await?;
        activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &original_identity,
        )
        .await?;

        context
            .put_event(&run_id, 2, "run.submitted", serde_json::json!({}))
            .await?;
        let changed_identity = context.source_identity().await?;
        let error = activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &changed_identity,
        )
        .await
        .expect_err("the source identity must remain stable after activation");
        assert!(matches!(error, RunHistoryActivationError::MarkerMismatch));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM run_events")
                .fetch_one(context.database.pool())
                .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_source_with_unmarked_target_fails_closed() -> TestResult<()> {
        let context = TestContext::new("empty-source-with-target").await?;
        let run_id = run_id();
        context.put_created(&run_id).await?;
        let identity = context.source_identity().await?;
        activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &identity,
        )
        .await?;
        sqlx::query("DELETE FROM legacy_run_history_activation")
            .execute(context.database.pool())
            .await?;

        let empty_store = Arc::new(fabro_store::Database::new(
            Arc::new(InMemory::new()),
            "empty-source",
            StdDuration::from_millis(1),
            None,
            Arc::new(fabro_store::BlobStore::new(context.database.clone_pool())),
            Arc::new(fabro_store::RunSummaryStore::new(
                context.database.clone_pool(),
            )),
        ));
        let empty_identity = empty_store.legacy_run_history_source_identity().await?;
        let error = activate_run_history(
            &context.database,
            &context.sqlite_path,
            &empty_store,
            &empty_identity,
        )
        .await
        .expect_err("unmarked SQLite rows cannot be adopted from an empty source");
        assert!(matches!(
            error,
            RunHistoryActivationError::EmptySourceWithTarget { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_backup_after_import_progress_fails_closed() -> TestResult<()> {
        let context = TestContext::new("missing-run-activation-backup").await?;
        let run_id = run_id();
        context.put_created(&run_id).await?;
        context
            .store
            .import_legacy_run_history_into(context.database.pool())
            .await?;

        let identity = context.source_identity().await?;
        let error = activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &identity,
        )
        .await
        .expect_err("partial import progress requires the retained backup");
        assert!(matches!(
            error,
            RunHistoryActivationError::MissingBackupAfterProgress { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn empty_source_and_target_need_no_backup_on_cold_or_warm_start() -> TestResult<()> {
        let context = TestContext::new("empty-run-activation").await?;
        let identity = context.source_identity().await?;
        activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &identity,
        )
        .await?;
        let marker = read_activation_record(context.database.pool())
            .await?
            .unwrap();
        assert_eq!((marker.source_runs, marker.source_events), (0, 0));
        assert!(!context.backup_path().exists());

        activate_run_history(
            &context.database,
            &context.sqlite_path,
            &context.store,
            &identity,
        )
        .await?;
        assert!(!context.backup_path().exists());
        Ok(())
    }
}
