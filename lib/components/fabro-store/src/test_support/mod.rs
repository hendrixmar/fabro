use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use fabro_types::{BlobHash, RunId};
use object_store::ObjectStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use crate::keys::SlateKey;
#[cfg(test)]
use crate::{AuthCodeStore, AuthSessionStore};
use crate::{BlobStore, Database, Result, RunSummaryStore};

/// Returns an isolated SQLite blob authority backed by its own in-memory
/// database.
///
/// Every call creates a fresh blob table, so tests never observe rows written
/// by other tests in the same process. Reopen-style tests that model one
/// process-wide blob authority across several store handles should call this
/// once and share the result through [`test_database_with_blobs`].
#[must_use]
pub fn test_blob_store() -> Arc<BlobStore> {
    Arc::new(BlobStore::new(lazy_in_memory_pool(&[
        fabro_db::BLOBS_MIGRATION_SQL,
    ])))
}

/// Returns an isolated SQLite run-summary store backed by its own in-memory
/// database and the production `runs` and `run_events` schemas.
#[must_use]
pub fn test_run_summary_store() -> Arc<RunSummaryStore> {
    Arc::new(RunSummaryStore::new(lazy_in_memory_pool(&[
        fabro_db::RUNS_MIGRATION_SQL,
        fabro_db::RUN_EVENTS_MIGRATION_SQL,
        fabro_db::RUN_HISTORY_ACTIVATION_MIGRATION_SQL,
        fabro_db::RUN_EVENT_SESSION_OWNER_MIGRATION_SQL,
    ])))
}

/// Builds a single-connection in-memory SQLite pool that installs
/// `migrations` on first use.
///
/// The pool connects lazily so synchronous fixture builders can remain
/// synchronous.
fn lazy_in_memory_pool(migrations: &'static [&'static str]) -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(":memory:")
        .foreign_keys(true);
    SqlitePoolOptions::new()
        .max_connections(1)
        // A single in-memory test connection never needs reaping. Disabling
        // both timers also keeps this lazy fixture constructible from sync
        // tests, where SQLx has no Tokio runtime for maintenance tasks.
        .max_lifetime(None)
        .idle_timeout(None)
        .after_connect(move |connection, _metadata| {
            Box::pin(async move {
                for migration in migrations {
                    sqlx::raw_sql(*migration).execute(&mut *connection).await?;
                }
                Ok(())
            })
        })
        .connect_lazy_with(options)
}

/// Returns the SQLite file backing [`test_blob_store_at`] for `store_dir`.
#[must_use]
pub fn test_blob_store_path(store_dir: &Path) -> PathBuf {
    fabro_db::append_to_path(store_dir, "-blobs.sqlite3")
}

/// Returns the SQLite file backing [`test_run_summary_store_at`] for
/// `store_dir`.
#[must_use]
pub fn test_run_summary_store_path(store_dir: &Path) -> PathBuf {
    fabro_db::append_to_path(store_dir, "-runs.sqlite3")
}

/// Returns a durable SQLite blob authority stored beside `store_dir`.
///
/// Handles created for the same directory share one blob database file, so
/// reopen-style tests observe blobs across store handles the way production
/// handles share the process-wide blob authority. Tests that reuse a
/// directory must delete [`test_blob_store_path`] (and its `-wal`/`-shm`
/// siblings) when they reset the directory itself.
#[must_use]
pub fn test_blob_store_at(store_dir: &Path) -> Arc<BlobStore> {
    Arc::new(BlobStore::new(lazy_file_pool(
        test_blob_store_path(store_dir),
        "blobs",
        &[fabro_db::BLOBS_MIGRATION_SQL],
    )))
}

/// Returns a durable SQLite run-history authority stored beside `store_dir`.
///
/// Handles created for the same directory share one database file, which lets
/// reopen-style tests model the process-wide SQLite authority used in
/// production.
#[must_use]
pub fn test_run_summary_store_at(store_dir: &Path) -> Arc<RunSummaryStore> {
    Arc::new(RunSummaryStore::new(lazy_file_pool(
        test_run_summary_store_path(store_dir),
        "runs",
        &[
            fabro_db::RUNS_MIGRATION_SQL,
            fabro_db::RUN_EVENTS_MIGRATION_SQL,
            fabro_db::RUN_HISTORY_ACTIVATION_MIGRATION_SQL,
            fabro_db::RUN_EVENT_SESSION_OWNER_MIGRATION_SQL,
        ],
    )))
}

/// Builds a single-connection file-backed SQLite pool that installs
/// `migrations` the first time it opens a database without `probe_table`.
///
/// Like [`lazy_in_memory_pool`], the pool connects lazily so synchronous
/// fixture builders stay synchronous, and the file persists across handles so
/// reopen-style tests share one authority.
fn lazy_file_pool(
    path: PathBuf,
    probe_table: &'static str,
    migrations: &'static [&'static str],
) -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true);
    SqlitePoolOptions::new()
        .max_connections(1)
        .max_lifetime(None)
        .idle_timeout(None)
        .after_connect(move |connection, _metadata| {
            Box::pin(async move {
                let installed: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master \
                     WHERE type = 'table' AND name = ?)",
                )
                .bind(probe_table)
                .fetch_one(&mut *connection)
                .await?;
                if !installed {
                    for migration in migrations {
                        sqlx::raw_sql(*migration).execute(&mut *connection).await?;
                    }
                }
                Ok(())
            })
        })
        .connect_lazy_with(options)
}

/// Builds a test database whose SQLite blob and run-history authorities are
/// durable beside `store_dir` and shared by reopen-style handles.
#[must_use]
pub fn test_database_at(
    object_store: Arc<dyn ObjectStore>,
    base_prefix: impl Into<String>,
    flush_interval: Duration,
    cache_path: Option<PathBuf>,
    store_dir: &Path,
) -> Database {
    test_database_with_stores(
        object_store,
        base_prefix,
        flush_interval,
        cache_path,
        test_blob_store_at(store_dir),
        test_run_summary_store_at(store_dir),
    )
}

/// Builds a Slate-backed run database with its own isolated blob authority.
#[must_use]
pub fn test_database(
    object_store: Arc<dyn ObjectStore>,
    base_prefix: impl Into<String>,
    flush_interval: Duration,
    cache_path: Option<PathBuf>,
) -> Database {
    test_database_with_blobs(
        object_store,
        base_prefix,
        flush_interval,
        cache_path,
        test_blob_store(),
    )
}

/// Builds a Slate-backed run database sharing an explicit blob authority.
///
/// Use this for reopen-style tests where two store handles must observe the
/// same signed SQLite blob table, mirroring the one blob authority a
/// production process shares across every run handle.
#[must_use]
pub fn test_database_with_blobs(
    object_store: Arc<dyn ObjectStore>,
    base_prefix: impl Into<String>,
    flush_interval: Duration,
    cache_path: Option<PathBuf>,
    blobs: Arc<BlobStore>,
) -> Database {
    test_database_with_stores(
        object_store,
        base_prefix,
        flush_interval,
        cache_path,
        blobs,
        test_run_summary_store(),
    )
}

/// Builds a Slate-backed run database with explicit shared SQLite stores.
///
/// Use this only when a test needs a failing, persistent, or shared store;
/// ordinary fixtures should use [`test_database`].
#[must_use]
pub fn test_database_with_stores(
    object_store: Arc<dyn ObjectStore>,
    base_prefix: impl Into<String>,
    flush_interval: Duration,
    cache_path: Option<PathBuf>,
    blobs: Arc<BlobStore>,
    run_summaries: Arc<RunSummaryStore>,
) -> Database {
    Database::new(
        object_store,
        base_prefix,
        flush_interval,
        cache_path,
        blobs,
        run_summaries,
    )
}

/// Seeds one canonical row in the legacy SlateDB blob keyspace.
pub async fn put_legacy_blob(database: &Database, bytes: &[u8]) -> Result<BlobHash> {
    let hash = BlobHash::new(bytes);
    let source = database.open_db().await?;
    source
        .put(SlateKey::new("blobs").with("sha256").with(hash), bytes)
        .await?;
    source.flush().await?;
    Ok(hash)
}

/// Writes an event without append validation to model a log corrupted by an
/// older Fabro version.
pub async fn put_unvalidated_run_event(
    database: &Database,
    run_id: &RunId,
    seq: u32,
    payload: &serde_json::Value,
) -> Result<()> {
    database
        .put_unvalidated_run_event(run_id, seq, payload)
        .await
}

/// Seeds one event in the retired Slate run-history keyspace.
pub async fn put_legacy_run_event(
    database: &Database,
    run_id: &RunId,
    seq: u32,
    payload: &serde_json::Value,
) -> Result<()> {
    database
        .put_unvalidated_legacy_run_event(run_id, seq, payload)
        .await
}

/// Connects to a migrated `fabro.sqlite3` in `directory` and returns its pool.
#[cfg(test)]
async fn sqlite_test_pool(directory: &Path) -> sqlx::SqlitePool {
    let database = fabro_db::Database::connect(directory.join("fabro.sqlite3"))
        .await
        .unwrap();
    database.migrate().await.unwrap();
    database.clone_pool()
}

#[cfg(test)]
pub(crate) async fn sqlite_auth_session_store() -> (tempfile::TempDir, AuthSessionStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = AuthSessionStore::new(sqlite_test_pool(directory.path()).await);
    (directory, store)
}

#[cfg(test)]
pub(crate) async fn sqlite_auth_code_store() -> (tempfile::TempDir, AuthCodeStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = AuthCodeStore::new(sqlite_test_pool(directory.path()).await);
    (directory, store)
}

#[cfg(test)]
pub(crate) async fn sqlite_run_summary_store() -> (tempfile::TempDir, RunSummaryStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = sqlite_run_summary_store_at(directory.path()).await;
    (directory, store)
}

#[cfg(test)]
pub(crate) async fn sqlite_run_summary_store_at(directory: &Path) -> RunSummaryStore {
    RunSummaryStore::new(sqlite_test_pool(directory).await)
}
