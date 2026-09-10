mod run_store;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use fabro_types::{RunId, SessionId};
use object_store::ObjectStore;
pub use run_store::RunDatabase;
use run_store::RunDatabaseInner;
use slatedb::config::{CompressionCodec, Settings};
use tokio::sync::{Mutex, MutexGuard, OnceCell};

use crate::{BlobStore, Error, EventPayload, Result, RunProjection, RunSummaryStore, keys};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableRun {
    pub run_id:     RunId,
    pub created_at: DateTime<Utc>,
    pub error:      String,
}

#[derive(Clone)]
pub struct Database {
    object_store:      Arc<dyn ObjectStore>,
    base_prefix:       String,
    flush_interval:    Duration,
    cache_path:        Option<PathBuf>,
    db:                Arc<OnceCell<slatedb::Db>>,
    active_runs:       Arc<Mutex<HashMap<RunId, Arc<RunDatabaseInner>>>>,
    blobs:             Arc<BlobStore>,
    run_summary_store: Arc<RunSummaryStore>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("base_prefix", &self.base_prefix)
            .field("flush_interval", &self.flush_interval)
            .field("cache_path", &self.cache_path)
            .finish_non_exhaustive()
    }
}

impl Database {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        base_prefix: impl Into<String>,
        flush_interval: Duration,
        cache_path: Option<PathBuf>,
        blobs: Arc<BlobStore>,
        run_summary_store: Arc<RunSummaryStore>,
    ) -> Self {
        Self {
            object_store,
            base_prefix: normalize_base_prefix(base_prefix.into()),
            flush_interval,
            cache_path,
            db: Arc::new(OnceCell::new()),
            active_runs: Arc::new(Mutex::new(HashMap::new())),
            blobs,
            run_summary_store,
        }
    }

    #[must_use]
    pub fn run_summary_store(&self) -> Arc<RunSummaryStore> {
        Arc::clone(&self.run_summary_store)
    }

    fn shared_db_prefix(&self) -> String {
        self.base_prefix.clone()
    }

    pub(crate) async fn open_db(&self) -> Result<slatedb::Db> {
        let db = self
            .db
            .get_or_try_init(|| async {
                let mut settings = Settings {
                    flush_interval: Some(self.flush_interval),
                    compression_codec: Some(CompressionCodec::Zstd),
                    ..Settings::default()
                };
                if let Some(ref cache_path) = self.cache_path {
                    settings.object_store_cache_options.root_folder = Some(cache_path.clone());
                }
                slatedb::Db::builder(self.shared_db_prefix(), self.object_store.clone())
                    .with_settings(settings)
                    .build()
                    .await
            })
            .await?;
        Ok(db.clone())
    }

    async fn get_active_run(&self, run_id: &RunId) -> Option<RunDatabase> {
        let active_runs = self.active_runs.lock().await;
        active_run_from(&active_runs, run_id)
    }

    #[cfg(test)]
    async fn remove_active_run(&self, run_id: &RunId) -> Option<RunDatabase> {
        self.active_runs
            .lock()
            .await
            .remove(run_id)
            .map(RunDatabase::from_inner)
    }

    fn cache_active_run(
        active_runs: &mut HashMap<RunId, Arc<RunDatabaseInner>>,
        run_store: &RunDatabase,
    ) {
        active_runs.insert(run_store.run_id(), run_store.inner_arc());
    }

    /// Builds a run handle wired to the Database-owned shared stores.
    async fn open_run_database(&self, run_id: &RunId, read_only: bool) -> Result<RunDatabase> {
        RunDatabase::build(*run_id, read_only, self.blobs(), self.run_summary_store()).await
    }

    pub async fn create_run_with_first_event(
        &self,
        run_id: &RunId,
        payload: &EventPayload,
    ) -> Result<RunDatabase> {
        let (mut active_runs, run_store) = self.reserve_new_run(run_id).await?;
        let (envelope, projected) = run_store.commit_first_event(payload).await?;
        run_store.install_in_memory_state(projected);
        Self::cache_active_run(&mut active_runs, &run_store);
        run_store.publish(&envelope);
        Ok(run_store)
    }

    /// Creates an empty run handle for fixture setup. Production code must
    /// create sequence-1 `run.created` atomically with the canonical row.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn create_run(&self, run_id: &RunId) -> Result<RunDatabase> {
        let (mut active_runs, run_store) = self.reserve_new_run(run_id).await?;
        Self::cache_active_run(&mut active_runs, &run_store);
        Ok(run_store)
    }

    /// Builds an empty handle for a run that exists neither in memory nor in
    /// SQLite, returning the held `active_runs` guard so the caller can
    /// register the handle before any concurrent creator observes the gap.
    async fn reserve_new_run(
        &self,
        run_id: &RunId,
    ) -> Result<(
        MutexGuard<'_, HashMap<RunId, Arc<RunDatabaseInner>>>,
        RunDatabase,
    )> {
        let active_runs = self.active_runs.lock().await;
        if active_runs.contains_key(run_id) || self.run_summary_store.contains(run_id).await? {
            return Err(Error::RunAlreadyExists(run_id.to_string()));
        }
        let run_store = RunDatabase::build_empty(*run_id, self.blobs(), self.run_summary_store());
        Ok((active_runs, run_store))
    }

    pub async fn open_run(&self, run_id: &RunId) -> Result<RunDatabase> {
        // Keep the active-writer miss and insert atomic. Otherwise concurrent
        // callers can create independent writers with the same recovered seq.
        let mut active_runs = self.active_runs.lock().await;

        if let Some(active) = active_run_from(&active_runs, run_id) {
            return Ok(active);
        }
        let run_store = self.open_run_database(run_id, false).await?;
        Self::cache_active_run(&mut active_runs, &run_store);
        Ok(run_store)
    }

    pub async fn open_run_reader(&self, run_id: &RunId) -> Result<RunDatabase> {
        if let Some(active) = self.get_active_run(run_id).await {
            return Ok(active.read_only_clone());
        }
        self.open_run_database(run_id, true).await
    }

    pub async fn list_unreadable_runs(&self) -> Result<Vec<UnreadableRun>> {
        let run_ids = self.run_summary_store.list_run_ids().await?;
        let mut unreadable = Vec::new();
        for run_id in run_ids {
            if let Err(err) = self.run_summary_store.load_projection(&run_id).await {
                unreadable.push(UnreadableRun {
                    run_id,
                    created_at: run_id.created_at(),
                    error: err.to_string(),
                });
            }
        }
        unreadable.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.run_id.cmp(&left.run_id))
        });
        Ok(unreadable)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn put_unvalidated_run_event(
        &self,
        run_id: &RunId,
        seq: u32,
        payload: &serde_json::Value,
    ) -> Result<()> {
        self.run_summary_store
            .test_insert_unvalidated_event(run_id, seq, payload)
            .await?;
        self.active_runs.lock().await.remove(run_id);
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn put_unvalidated_legacy_run_event(
        &self,
        run_id: &RunId,
        seq: u32,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let db = self.open_db().await?;
        db.put(
            keys::run_event_key(run_id, seq, 0),
            serde_json::to_vec(payload)?,
        )
        .await?;
        Ok(())
    }

    pub async fn load_run_projection(&self, run_id: &RunId) -> Result<Option<Arc<RunProjection>>> {
        if let Some(active) = self.get_active_run(run_id).await {
            return active.projection_snapshot().await.map(Some);
        }
        match self.run_summary_store.load_projection(run_id).await {
            Ok(projected) => Ok(Some(projected.projection)),
            Err(Error::RunNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Resolves the run that owns `session_id` from the canonical typed
    /// creation event stored in SQLite.
    pub async fn find_session_owner(&self, session_id: &SessionId) -> Result<Option<RunId>> {
        self.run_summary_store.find_session_owner(session_id).await
    }

    pub async fn delete_run(&self, run_id: &RunId) -> Result<()> {
        let mut active_runs = self.active_runs.lock().await;
        let active = active_runs.get(run_id).cloned();
        let _state_guard = match &active {
            Some(active) => Some(active.state_lock.lock().await),
            None => None,
        };
        self.run_summary_store
            .delete_canonical(run_id, Utc::now().timestamp_millis())
            .await?;
        active_runs.remove(run_id);
        Ok(())
    }

    #[must_use]
    pub fn blobs(&self) -> Arc<BlobStore> {
        Arc::clone(&self.blobs)
    }

    /// Delete every record under the retired `auth/refresh` prefix.
    ///
    /// Refresh tokens moved to SQLite without an import, so these records are
    /// unreadable -- and the reaper that used to collect them is gone, so
    /// nothing else would ever remove them. Returns the number of records
    /// deleted; a later boot finds the prefix empty and does nothing.
    pub async fn retire_refresh_token_keyspace(&self) -> Result<u64> {
        let db = self.open_db().await?;
        let mut iter = db
            .scan_prefix(keys::SlateKey::new("auth").with("refresh").into_prefix())
            .await?;
        let mut batch = slatedb::WriteBatch::new();
        let mut deletes = 0_u64;
        while let Some(entry) = iter.next().await? {
            batch.delete(entry.key);
            deletes += 1;
        }
        if deletes > 0 {
            db.write(batch).await?;
        }
        Ok(deletes)
    }
}

pub(crate) fn normalize_base_prefix(prefix: String) -> String {
    if prefix.is_empty() {
        return String::new();
    }
    if prefix.ends_with('/') {
        prefix
    } else {
        format!("{prefix}/")
    }
}

fn active_run_from(
    active_runs: &HashMap<RunId, Arc<RunDatabaseInner>>,
    run_id: &RunId,
) -> Option<RunDatabase> {
    active_runs
        .get(run_id)
        .cloned()
        .map(RunDatabase::from_inner)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use fabro_types::{
        AttrValue, FailureReason, Graph, RunControlAction, RunSpec, RunStatus, StageId,
        SuccessReason, WorkflowSettings, test_support,
    };
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;
    use crate::run_state::ProjectedRun;
    use crate::{EventPayload, keys, test_support as store_test_support};

    fn dt(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    fn test_run_id(label: &str) -> RunId {
        let (timestamp_ms, random) = match label {
            "run-1" => (
                dt("2026-03-27T12:00:00Z")
                    .timestamp_millis()
                    .cast_unsigned(),
                1,
            ),
            "run-2" => (
                dt("2026-03-27T12:00:10Z")
                    .timestamp_millis()
                    .cast_unsigned(),
                2,
            ),
            "run-3" => (
                dt("2026-03-27T12:00:20Z")
                    .timestamp_millis()
                    .cast_unsigned(),
                3,
            ),
            "run-4" => (
                dt("2026-03-27T12:00:30Z")
                    .timestamp_millis()
                    .cast_unsigned(),
                4,
            ),
            _ => panic!("unknown test run id: {label}"),
        };
        RunId::from(ulid::Ulid::from_parts(timestamp_ms, random))
    }

    fn make_store() -> (Arc<dyn ObjectStore>, Database) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = store_test_support::test_database(
            object_store.clone(),
            "runs/",
            Duration::from_millis(1),
            None,
        );
        (object_store, store)
    }

    fn make_store_with_run_summaries(
        run_summaries: Arc<RunSummaryStore>,
    ) -> (Arc<dyn ObjectStore>, Database) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = store_test_support::test_database_with_stores(
            object_store.clone(),
            "runs/",
            Duration::from_millis(1),
            None,
            store_test_support::test_blob_store(),
            run_summaries,
        );
        (object_store, store)
    }

    /// Reopens a `Database` over an existing object store and SQLite summary
    /// store, simulating a process restart with no active run handles.
    fn reopen_store(
        object_store: Arc<dyn ObjectStore>,
        run_summaries: Arc<RunSummaryStore>,
    ) -> Database {
        store_test_support::test_database_with_stores(
            object_store,
            "runs",
            Duration::from_millis(1),
            None,
            store_test_support::test_blob_store(),
            run_summaries,
        )
    }

    #[tokio::test]
    async fn retire_refresh_token_keyspace_clears_the_prefix_and_is_idempotent() {
        let (_object_store, store) = make_store();
        let db = store.open_db().await.unwrap();

        let refresh_keys = ["aaa", "bbb"].map(|id| {
            keys::SlateKey::new("auth")
                .with("refresh")
                .with(id)
                .as_ref()
                .to_vec()
        });
        // "auth/code" sorts adjacent to "auth/refresh", so it is the
        // neighbour a too-wide prefix delete would take with it.
        let auth_code_key = keys::SlateKey::new("auth")
            .with("code")
            .with("keep")
            .as_ref()
            .to_vec();

        let mut batch = slatedb::WriteBatch::new();
        for key in &refresh_keys {
            batch.put(key.as_slice(), b"{}".as_slice());
        }
        batch.put(auth_code_key.as_slice(), b"{}".as_slice());
        db.write(batch).await.unwrap();

        assert_eq!(store.retire_refresh_token_keyspace().await.unwrap(), 2);
        assert_eq!(store.retire_refresh_token_keyspace().await.unwrap(), 0);
        for key in &refresh_keys {
            assert!(db.get(key.as_slice()).await.unwrap().is_none());
        }
        assert!(
            db.get(auth_code_key.as_slice()).await.unwrap().is_some(),
            "retiring refresh tokens must not touch the auth code prefix"
        );
    }

    async fn make_run_summary_store() -> (tempfile::TempDir, Arc<RunSummaryStore>) {
        let (directory, store) = store_test_support::sqlite_run_summary_store().await;
        (directory, Arc::new(store))
    }

    fn sample_run_spec(label: &str) -> RunSpec {
        let mut graph = Graph::new("night-sky");
        graph.attrs.insert(
            "goal".to_string(),
            AttrValue::String("map the constellations".to_string()),
        );
        RunSpec {
            run_id: test_run_id(label),
            settings: WorkflowSettings::default(),
            graph,
            graph_source: None,
            workflow_slug: Some("night-sky".to_string()),
            workflow_version_id: None,
            target: None,
            automation: None,
            source_directory: Some(format!("/tmp/{label}")),
            labels: std::collections::HashMap::from([("team".to_string(), "infra".to_string())]),
            provenance: test_support::test_run_provenance(),
            manifest_blob: None,
            definition_blob: None,
            spec_blob: None,
            git: Some(fabro_types::GitContext {
                origin_url: "https://github.com/fabro-sh/fabro".to_string(),
                branch:     "main".to_string(),
                sha:        None,
                dirty:      fabro_types::DirtyStatus::Clean,
            }),
            fork_source_ref: None,
        }
    }

    fn event_payload(
        run_id: &str,
        ts: &str,
        event: &str,
        properties: &serde_json::Value,
    ) -> EventPayload {
        event_payload_with_node(run_id, ts, event, properties, None)
    }

    fn event_payload_with_node(
        run_id: &str,
        ts: &str,
        event: &str,
        properties: &serde_json::Value,
        node_id: Option<&str>,
    ) -> EventPayload {
        EventPayload::new(
            serde_json::json!({
                "id": format!("evt-{run_id}-{event}"),
                "ts": ts,
                "run_id": test_run_id(run_id).to_string(),
                "event": event,
                "node_id": node_id,
                "stage_id": node_id.map(|node| format!("{node}@1")),
                "properties": properties,
            }),
            &test_run_id(run_id),
        )
        .unwrap()
    }

    fn session_created_payload(label: &str, session_id: &SessionId) -> EventPayload {
        EventPayload::new(
            serde_json::json!({
                "id": format!("evt-{label}-session-created"),
                "ts": "2026-03-27T12:00:05Z",
                "run_id": test_run_id(label).to_string(),
                "event": "run.session.created",
                "session_id": session_id,
                "properties": { "title": "Owned session" },
            }),
            &test_run_id(label),
        )
        .unwrap()
    }

    async fn append_created(run: &RunDatabase, label: &str, created_at: DateTime<Utc>) {
        let run_spec = sample_run_spec(label);
        run.append_event(&event_payload(
            label,
            &created_at.to_rfc3339(),
            "run.created",
            &serde_json::json!({
                "settings": run_spec.settings,
                "graph": run_spec.graph,
                "workflow_slug": run_spec.workflow_slug,
                "source_directory": run_spec.source_directory,
                "git": run_spec.git,
                "labels": run_spec.labels,
                "provenance": run_spec.provenance,
            }),
        ))
        .await
        .unwrap();
    }

    async fn append_created_with_parent(
        run: &RunDatabase,
        label: &str,
        created_at: DateTime<Utc>,
        parent_id: RunId,
    ) {
        let run_spec = sample_run_spec(label);
        run.append_event(&event_payload(
            label,
            &created_at.to_rfc3339(),
            "run.created",
            &serde_json::json!({
                "settings": run_spec.settings,
                "graph": run_spec.graph,
                "workflow_slug": run_spec.workflow_slug,
                "source_directory": run_spec.source_directory,
                "git": run_spec.git,
                "labels": run_spec.labels,
                "parent_id": parent_id,
                "provenance": run_spec.provenance,
            }),
        ))
        .await
        .unwrap();
    }

    async fn append_runnable(run: &RunDatabase, label: &str, created_at: DateTime<Utc>) {
        append_created(run, label, created_at).await;
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:01Z",
            "run.submitted",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:02Z",
            "run.start_requested",
            &serde_json::json!({ "resume": false }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:03Z",
            "run.runnable",
            &serde_json::json!({ "source": "start_requested" }),
        ))
        .await
        .unwrap();
    }

    fn failure_payload(label: &str, reason: FailureReason, message: &str) -> EventPayload {
        event_payload(
            label,
            "2026-03-27T12:00:04Z",
            "run.failed",
            &serde_json::json!({
                "failure": {
                    "reason": reason.to_string(),
                    "detail": {
                        "message": message,
                        "category": "deterministic"
                    }
                },
                "timing": {
                    "wall_time_ms": 1,
                    "inference_time_ms": 0,
                    "tool_time_ms": 0,
                    "active_time_ms": 0
                },
            }),
        )
    }

    fn workflow_failure_payload(label: &str) -> EventPayload {
        failure_payload(label, FailureReason::WorkflowError, "workflow failed")
    }

    /// A failure that can only occur after `Starting`, so a `Runnable` run must
    /// reject it.
    fn sandbox_init_failure_payload(label: &str) -> EventPayload {
        assert!(!FailureReason::SandboxInitFailed.can_occur_before_start());
        failure_payload(
            label,
            FailureReason::SandboxInitFailed,
            "sandbox initialization failed",
        )
    }

    async fn append_completed(run: &RunDatabase, label: &str, created_at: DateTime<Utc>) {
        append_running(run, label, created_at).await;
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:03Z",
            "run.completed",
            &serde_json::json!({
                "timing": {"wall_time_ms": 3210, "inference_time_ms": 0, "tool_time_ms": 0, "active_time_ms": 0},
                "artifact_count": 1,
                "status": "succeeded",
                "reason": "completed",
                "total_cost": 1.25,
            }),
        ))
        .await
        .unwrap();
    }

    async fn append_running(run: &RunDatabase, label: &str, created_at: DateTime<Utc>) {
        append_created(run, label, created_at).await;
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:01Z",
            "run.runnable",
            &serde_json::json!({ "source": "start_requested" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:02Z",
            "run.starting",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            label,
            "2026-03-27T12:00:03Z",
            "run.running",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
    }

    async fn list_paths(store: Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
        let mut items = store
            .list(Some(&Path::from(prefix.to_string())))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        items.sort();
        items
    }

    #[tokio::test]
    async fn create_open_list_and_delete_full_lifecycle_without_legacy_slate_writes() {
        let (object_store, store) = make_store();
        let run_1 = store.create_run(&test_run_id("run-1")).await.unwrap();
        let run_2 = store.create_run(&test_run_id("run-2")).await.unwrap();
        append_completed(&run_1, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_created(&run_2, "run-2", dt("2026-03-27T12:00:10Z")).await;

        let summary = store
            .run_summary_store()
            .list_all(Utc::now())
            .await
            .unwrap();
        assert_eq!(summary.len(), 2);
        assert_eq!(summary[0].id, test_run_id("run-2"));
        assert_eq!(summary[1].id, test_run_id("run-1"));
        assert_eq!(summary[1].workflow.name, None);
        assert_eq!(summary[1].workflow.graph_name.as_deref(), Some("night-sky"));
        assert_eq!(summary[1].goal, "map the constellations");
        assert_eq!(summary[1].lifecycle.status, RunStatus::Succeeded {
            reason: SuccessReason::Completed,
        });

        let reopened = store.open_run(&test_run_id("run-1")).await.unwrap();
        let stored = reopened.state().await.unwrap().spec;
        assert_eq!(stored.run_id, test_run_id("run-1"));

        store.delete_run(&test_run_id("run-1")).await.unwrap();
        assert!(store.open_run(&test_run_id("run-1")).await.is_err());
        let remaining = store
            .run_summary_store()
            .list_all(Utc::now())
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, test_run_id("run-2"));
        assert!(
            list_paths(object_store, "runs/").await.is_empty(),
            "canonical run lifecycle must not open SlateDB solely for retired session indexes"
        );
    }

    #[tokio::test]
    async fn session_owner_claims_are_atomic_durable_and_ignore_legacy_reverse_rows() {
        let directory = tempfile::tempdir().unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = store_test_support::test_database_at(
            Arc::clone(&object_store),
            "session-owner",
            Duration::from_millis(1),
            None,
            directory.path(),
        );
        let first_id = test_run_id("run-1");
        let second_id = test_run_id("run-2");
        let first = store.create_run(&first_id).await.unwrap();
        let second = store.create_run(&second_id).await.unwrap();
        append_created(&first, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_created(&second, "run-2", dt("2026-03-27T12:00:10Z")).await;

        let session_id = SessionId::new();
        assert_eq!(
            first
                .append_event(&session_created_payload("run-1", &session_id))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store.find_session_owner(&session_id).await.unwrap(),
            Some(first_id)
        );

        let legacy_key = keys::session_by_id_key(&session_id).as_ref().to_vec();
        let legacy = store.open_db().await.unwrap();
        assert!(legacy.get(&legacy_key).await.unwrap().is_none());
        legacy
            .put(
                &legacy_key,
                serde_json::to_vec(&serde_json::json!({ "run_id": second_id })).unwrap(),
            )
            .await
            .unwrap();
        legacy.flush().await.unwrap();
        assert_eq!(
            store.find_session_owner(&session_id).await.unwrap(),
            Some(first_id),
            "legacy reverse rows must not influence ownership"
        );

        assert!(
            first
                .append_event(&session_created_payload("run-1", &session_id))
                .await
                .is_err()
        );
        assert_eq!(first.last_event_seq().await.unwrap(), Some(2));
        assert!(
            second
                .append_event(&session_created_payload("run-2", &session_id))
                .await
                .is_err()
        );
        assert_eq!(second.last_event_seq().await.unwrap(), Some(1));
        assert_eq!(
            store.find_session_owner(&session_id).await.unwrap(),
            Some(first_id)
        );

        let reopened = store_test_support::test_database_at(
            object_store,
            "session-owner",
            Duration::from_millis(1),
            None,
            directory.path(),
        );
        assert_eq!(
            reopened.find_session_owner(&session_id).await.unwrap(),
            Some(first_id)
        );

        reopened.delete_run(&first_id).await.unwrap();
        assert_eq!(
            reopened.find_session_owner(&session_id).await.unwrap(),
            None
        );
        assert!(
            legacy.get(&legacy_key).await.unwrap().is_some(),
            "legacy reverse rows remain diagnostic-only during the support window"
        );
    }

    #[tokio::test]
    async fn delete_run_keeps_global_cas_blobs() {
        let (_object_store, store) = make_store();
        let run_1 = store.create_run(&test_run_id("run-1")).await.unwrap();
        let run_2 = store.create_run(&test_run_id("run-2")).await.unwrap();
        append_created(&run_1, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_created(&run_2, "run-2", dt("2026-03-27T12:00:10Z")).await;

        let shared_blob = br#"{"summary":"shared"}"#;
        let shared_blob_hash = run_1.write_blob(shared_blob).await.unwrap();

        store.delete_run(&test_run_id("run-1")).await.unwrap();

        let reopened = store.open_run(&test_run_id("run-2")).await.unwrap();
        let read = reopened.read_blob(&shared_blob_hash).await.unwrap();
        assert_eq!(read.as_deref(), Some(shared_blob.as_slice()));
    }

    #[tokio::test]
    async fn activated_delete_tombstone_prevents_legacy_history_resurrection() {
        let (directory, summaries) = make_run_summary_store().await;
        let (_object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_id = test_run_id("run-1");
        let created = event_payload(
            "run-1",
            "2026-03-27T12:00:00Z",
            "run.created",
            &serde_json::json!({
                "settings": sample_run_spec("run-1").settings,
                "graph": sample_run_spec("run-1").graph,
                "provenance": test_support::test_run_provenance(),
            }),
        );
        store
            .put_unvalidated_legacy_run_event(&run_id, 1, created.as_value())
            .await
            .unwrap();
        let run = store.create_run(&run_id).await.unwrap();
        run.append_event(&created).await.unwrap();
        summaries.test_mark_run_history_activated().await.unwrap();

        store.delete_run(&run_id).await.unwrap();

        assert!(
            summaries
                .test_is_run_history_tombstoned(&run_id)
                .await
                .unwrap()
        );
        assert!(store.open_run(&run_id).await.is_err());
        let database = fabro_db::Database::connect(directory.path().join("fabro.sqlite3"))
            .await
            .unwrap();
        let report = store
            .import_legacy_run_history_into(database.pool())
            .await
            .unwrap();
        assert_eq!(report.tombstoned_source_runs, 1);
        assert_eq!(report.tombstoned_source_events, 1);
        assert!(store.open_run(&run_id).await.is_err());

        let verification = store
            .verify_legacy_run_history_in(database.pool())
            .await
            .unwrap();
        assert_eq!(verification.tombstoned_source_runs, 1);
        assert_eq!(verification.tombstoned_source_events, 1);

        let recreated = store.create_run(&run_id).await.unwrap();
        recreated.append_event(&created).await.unwrap();
        assert!(
            store
                .verify_legacy_run_history_in(database.pool())
                .await
                .is_err(),
            "a tombstone and live canonical data must fail verification"
        );
    }

    #[tokio::test]
    async fn missing_run_open_paths_return_run_not_found() {
        let (_object_store, store) = make_store();
        let run_id = test_run_id("run-4");

        assert!(matches!(
            store.open_run(&run_id).await,
            Err(Error::RunNotFound(id)) if id == run_id.to_string()
        ));
        assert!(matches!(
            store.open_run_reader(&run_id).await,
            Err(Error::RunNotFound(id)) if id == run_id.to_string()
        ));
    }

    #[tokio::test]
    async fn open_run_reader_is_read_only() {
        let (_object_store, store) = make_store();
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;
        let blob = br#"{"summary":"readable"}"#;
        let blob_hash = run.write_blob(blob).await.unwrap();

        // Evict the cached writer so the reader is built through the real
        // `open_run_reader` construction path, not a clone of the writer.
        let _ = store.remove_active_run(&test_run_id("run-1")).await;

        let reader = store.open_run_reader(&test_run_id("run-1")).await.unwrap();
        assert_eq!(
            reader.read_blob(&blob_hash).await.unwrap().as_deref(),
            Some(blob.as_slice())
        );
        let err = reader.write_blob(b"blocked").await.unwrap_err();
        assert!(matches!(err, Error::ReadOnly));

        let err = reader
            .append_event(&event_payload(
                "run-1",
                "2026-03-27T12:00:01Z",
                "run.completed",
                &serde_json::json!({ "reason": "completed" }),
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ReadOnly));
    }

    #[tokio::test]
    async fn append_event_if_evaluates_latest_projection_before_appending() {
        let (_object_store, store) = make_store();
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;
        let initial_title = run.state().await.unwrap().title().into_owned();

        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.title.updated",
            &serde_json::json!({ "title": "User title" }),
        ))
        .await
        .unwrap();

        let generated_update = event_payload(
            "run-1",
            "2026-03-27T12:00:02Z",
            "run.title.updated",
            &serde_json::json!({ "title": "Generated title" }),
        );
        let appended = run
            .append_event_if(&generated_update, |projection| {
                projection.title() == initial_title
            })
            .await
            .unwrap();

        assert_eq!(appended, None);
        assert_eq!(run.state().await.unwrap().title(), "User title");
        assert_eq!(run.list_events().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn rejected_transition_writes_nothing_and_preserves_projection_state() {
        let (_object_store, store) = make_store();
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        append_runnable(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;
        let events_before = run.list_events().await.unwrap();

        let err = run
            .append_event(&sandbox_init_failure_payload("run-1"))
            .await
            .unwrap_err();

        let Error::EventRejected { source } = err else {
            panic!("expected event rejection");
        };
        assert!(matches!(
            *source,
            Error::InvalidTransition(fabro_types::InvalidTransition {
                from: RunStatus::Runnable,
                to:   RunStatus::Failed {
                    reason: FailureReason::SandboxInitFailed,
                },
            })
        ));
        assert_eq!(run.list_events().await.unwrap(), events_before);
        assert_eq!(run.state().await.unwrap().status, RunStatus::Runnable);
        let projection = store.load_run_projection(&run_id).await.unwrap().unwrap();
        assert_eq!(run.last_event_seq().await.unwrap(), Some(4));
        assert_eq!(projection.status, RunStatus::Runnable);
    }

    #[tokio::test]
    async fn rejected_transition_leaves_reconciled_summary_present() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (_object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        append_runnable(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        let err = run
            .append_event(&sandbox_init_failure_payload("run-1"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::EventRejected { .. }));

        let projection = store.load_run_projection(&run_id).await.unwrap().unwrap();
        let last_seq = run.last_event_seq().await.unwrap().unwrap();
        let entries = [ProjectedRun::new(run_id, projection, last_seq)];
        summaries.reconcile(&entries).await.unwrap();
        let summary = summaries.get(&run_id, Utc::now()).await.unwrap().unwrap();
        assert_eq!(summary.lifecycle.status, RunStatus::Runnable);
    }

    #[tokio::test]
    async fn sql_failure_leaves_event_and_projection_unpublished() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (_object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;
        let projection_before = store.load_run_projection(&run_id).await.unwrap().unwrap();
        summaries.close_pool().await;

        let result = run
            .append_event_envelope(&event_payload(
                "run-1",
                "2026-03-27T12:00:01Z",
                "run.title.updated",
                &serde_json::json!({ "title": "Uncommitted title" }),
            ))
            .await;

        assert!(matches!(
            result,
            Err(Error::Sqlite(sqlx::Error::PoolClosed))
        ));
        let projection_after = store.load_run_projection(&run_id).await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&projection_before, &projection_after));
    }

    #[tokio::test]
    async fn first_event_is_validated_before_write() {
        let (_object_store, store) = make_store();
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        let invalid_first = event_payload(
            "run-1",
            "2026-03-27T12:00:00Z",
            "run.title.updated",
            &serde_json::json!({ "title": "Too early" }),
        );

        let err = run.append_event(&invalid_first).await.unwrap_err();

        assert!(matches!(err, Error::EventRejected { .. }));
        assert_eq!(run.last_event_seq().await.unwrap(), None);

        append_created(&run, "run-1", dt("2026-03-27T12:00:01Z")).await;
        assert_eq!(run.list_events().await.unwrap().len(), 1);
        assert!(run.state().await.is_ok());
    }

    #[tokio::test]
    async fn malformed_optional_envelope_field_is_rejected_before_write() {
        let (_object_store, store) = make_store();
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        let malformed = EventPayload::new(
            serde_json::json!({
                "id": "evt-created",
                "ts": "2026-03-27T12:00:00Z",
                "run_id": run_id.to_string(),
                "event": "run.created",
                "node_id": 42,
                "properties": {
                    "settings": WorkflowSettings::default(),
                    "graph": Graph::new("test"),
                    "provenance": test_support::test_run_provenance(),
                },
            }),
            &run_id,
        )
        .unwrap();

        let err = run.append_event(&malformed).await.unwrap_err();

        assert!(matches!(err, Error::InvalidEvent(_)));
        assert_eq!(run.last_event_seq().await.unwrap(), None);
    }

    #[tokio::test]
    async fn control_request_events_set_pending_control_without_overwriting_status() {
        let (_object_store, store) = make_store();
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_running(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:02Z",
            "run.pause.requested",
            &serde_json::json!({ "action": "pause" }),
        ))
        .await
        .unwrap();

        let summary = store
            .run_summary_store()
            .list_all(Utc::now())
            .await
            .unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].lifecycle.status, RunStatus::Running);
        assert_eq!(
            summary[0].lifecycle.pending_control,
            Some(RunControlAction::Pause)
        );
    }

    #[tokio::test]
    async fn parent_id_is_projected_from_created_and_parent_events() {
        let (_object_store, store) = make_store();
        let parent_1 = store.create_run(&test_run_id("run-1")).await.unwrap();
        let parent_2 = store.create_run(&test_run_id("run-2")).await.unwrap();
        let child = store.create_run(&test_run_id("run-3")).await.unwrap();
        append_created(&parent_1, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_created(&parent_2, "run-2", dt("2026-03-27T12:00:10Z")).await;
        append_created_with_parent(
            &child,
            "run-3",
            dt("2026-03-27T12:00:20Z"),
            test_run_id("run-1"),
        )
        .await;

        let initial = store.open_run(&test_run_id("run-3")).await.unwrap();
        assert_eq!(
            initial.state().await.unwrap().parent_id,
            Some(test_run_id("run-1"))
        );
        assert_eq!(
            store
                .run_summary_store()
                .get(&test_run_id("run-3"), Utc::now())
                .await
                .unwrap()
                .unwrap()
                .parent_id,
            Some(test_run_id("run-1"))
        );

        child
            .append_event(&event_payload(
                "run-3",
                "2026-03-27T12:00:21Z",
                "run.parent.linked",
                &serde_json::json!({
                    "previous_parent_id": test_run_id("run-1"),
                    "parent_id": test_run_id("run-2"),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .run_summary_store()
                .get(&test_run_id("run-3"), Utc::now())
                .await
                .unwrap()
                .unwrap()
                .parent_id,
            Some(test_run_id("run-2"))
        );
        child
            .append_event(&event_payload(
                "run-3",
                "2026-03-27T12:00:22Z",
                "run.parent.unlinked",
                &serde_json::json!({
                    "previous_parent_id": test_run_id("run-2"),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .run_summary_store()
                .get(&test_run_id("run-3"), Utc::now())
                .await
                .unwrap()
                .unwrap()
                .parent_id,
            None
        );
    }

    #[tokio::test]
    async fn run_summary_includes_children_count() {
        let (_object_store, store) = make_store();
        let parent = store.create_run(&test_run_id("run-1")).await.unwrap();
        let child_a = store.create_run(&test_run_id("run-2")).await.unwrap();
        let child_b = store.create_run(&test_run_id("run-3")).await.unwrap();
        let unrelated = store.create_run(&test_run_id("run-4")).await.unwrap();
        append_created(&parent, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_created_with_parent(
            &child_a,
            "run-2",
            dt("2026-03-27T12:00:10Z"),
            test_run_id("run-1"),
        )
        .await;
        append_created_with_parent(
            &child_b,
            "run-3",
            dt("2026-03-27T12:00:20Z"),
            test_run_id("run-1"),
        )
        .await;
        append_created(&unrelated, "run-4", dt("2026-03-27T12:00:30Z")).await;

        let summaries = store
            .run_summary_store()
            .list_all(Utc::now())
            .await
            .unwrap();

        let parent_summary = summaries
            .iter()
            .find(|r| r.id == test_run_id("run-1"))
            .expect("parent summary should be present");
        assert_eq!(parent_summary.children_count, 2);

        let child_summary = summaries
            .iter()
            .find(|r| r.id == test_run_id("run-2"))
            .expect("child summary should be present");
        assert_eq!(child_summary.children_count, 0);

        let unrelated_summary = summaries
            .iter()
            .find(|r| r.id == test_run_id("run-4"))
            .expect("unrelated summary should be present");
        assert_eq!(unrelated_summary.children_count, 0);
    }

    #[tokio::test]
    async fn control_effect_events_clear_pending_control_and_update_status() {
        let (_object_store, store) = make_store();
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_running(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:02Z",
            "run.pause.requested",
            &serde_json::json!({ "action": "pause" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:03Z",
            "run.paused",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:04Z",
            "run.unpause.requested",
            &serde_json::json!({ "action": "unpause" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:05Z",
            "run.unpaused",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:06Z",
            "run.cancel.requested",
            &serde_json::json!({ "action": "cancel" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:07Z",
            "run.failed",
            &serde_json::json!({
                "failure": {
                    "reason": "cancelled",
                    "detail": {
                        "message": "cancelled",
                        "category": "canceled"
                    }
                },
                "timing": {"wall_time_ms": 1, "inference_time_ms": 0, "tool_time_ms": 0, "active_time_ms": 0},
            }),
        ))
        .await
        .unwrap();

        let summary = store
            .run_summary_store()
            .list_all(Utc::now())
            .await
            .unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].lifecycle.status, RunStatus::Failed {
            reason: FailureReason::Cancelled,
        });
        assert_eq!(summary[0].lifecycle.pending_control, None);
    }

    #[tokio::test]
    async fn reader_sees_cached_projection_and_recent_events_for_active_run() {
        let (_object_store, store) = make_store();
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        let reader = store.open_run_reader(&test_run_id("run-1")).await.unwrap();
        let state = reader.state().await.unwrap();
        assert_eq!(state.spec.run_id, test_run_id("run-1"));

        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.runnable",
            &serde_json::json!({ "source": "start_requested" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:02Z",
            "run.starting",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:03Z",
            "run.running",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:04Z",
            "run.completed",
            &serde_json::json!({
                "timing": {"wall_time_ms": 3210, "inference_time_ms": 0, "tool_time_ms": 0, "active_time_ms": 0},
                "artifact_count": 1,
                "status": "succeeded",
                "reason": "completed",
                "total_cost": 1.25,
            }),
        ))
        .await
        .unwrap();

        let recent = reader.list_events_from_with_limit(4, 10).await.unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].seq, 4);
    }

    #[tokio::test]
    async fn reopening_store_rebuilds_from_shared_sqlite() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_completed(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        let reopened = reopen_store(object_store, summaries);
        let summary = reopened
            .run_summary_store()
            .list_all(Utc::now())
            .await
            .unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].id, test_run_id("run-1"));
        assert_eq!(summary[0].lifecycle.status, RunStatus::Succeeded {
            reason: SuccessReason::Completed,
        });
    }

    #[tokio::test]
    async fn inactive_projection_loads_on_demand_without_registering_run() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_1 = store.create_run(&test_run_id("run-1")).await.unwrap();
        let run_2 = store.create_run(&test_run_id("run-2")).await.unwrap();
        let run_3 = store.create_run(&test_run_id("run-3")).await.unwrap();
        append_completed(&run_1, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_completed(&run_2, "run-2", dt("2026-03-27T12:00:10Z")).await;
        append_completed(&run_3, "run-3", dt("2026-03-27T12:00:20Z")).await;

        let reopened = reopen_store(object_store, summaries);
        assert!(reopened.active_runs.lock().await.is_empty());

        let projection = reopened
            .load_run_projection(&test_run_id("run-2"))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(projection.spec.run_id, test_run_id("run-2"));
        assert_eq!(projection.status, RunStatus::Succeeded {
            reason: SuccessReason::Completed,
        });
        assert!(
            reopened.active_runs.lock().await.is_empty(),
            "inactive detail reads must stay request-local"
        );
    }

    #[tokio::test]
    async fn active_projection_reads_are_coherent_immutable_snapshots() {
        let (_object_store, store) = make_store();
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        let first = store.load_run_projection(&run_id).await.unwrap().unwrap();
        let second = store.load_run_projection(&run_id).await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let original_title = first.title.clone();

        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.title.updated",
            &serde_json::json!({ "title": "New title" }),
        ))
        .await
        .unwrap();

        let updated = store.load_run_projection(&run_id).await.unwrap().unwrap();
        assert!(!Arc::ptr_eq(&first, &updated));
        assert_eq!(first.title, original_title);
        assert_eq!(updated.title, "New title");
    }

    #[tokio::test]
    async fn inactive_projection_replay_accepts_sequence_gaps_and_failures_do_not_poison_reads() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let healthy_id = test_run_id("run-1");
        let broken_id = test_run_id("run-2");
        let healthy = store.create_run(&healthy_id).await.unwrap();
        let broken = store.create_run(&broken_id).await.unwrap();
        append_created(&healthy, "run-1", dt("2026-03-27T12:00:00Z")).await;
        append_created(&broken, "run-2", dt("2026-03-27T12:00:10Z")).await;
        store.remove_active_run(&healthy_id).await;
        store.remove_active_run(&broken_id).await;
        store
            .put_unvalidated_run_event(
                &healthy_id,
                3,
                event_payload(
                    "run-1",
                    "2026-03-27T12:00:02Z",
                    "run.title.updated",
                    &serde_json::json!({ "title": "Imported gap" }),
                )
                .as_value(),
            )
            .await
            .unwrap();
        summaries.test_delete_run_events(&broken_id).await.unwrap();

        let reopened = reopen_store(object_store, summaries);
        assert!(matches!(
            reopened.load_run_projection(&broken_id).await,
            Err(Error::RunHeadMismatch { .. })
        ));

        let projection = reopened
            .load_run_projection(&healthy_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.title, "Imported gap");
        assert!(reopened.active_runs.lock().await.is_empty());
    }

    #[tokio::test]
    async fn inactive_projection_read_concurrent_with_append_is_one_coherent_snapshot() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (object_store, writer_store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_id = test_run_id("run-1");
        let writer = writer_store.create_run(&run_id).await.unwrap();
        append_created(&writer, "run-1", dt("2026-03-27T12:00:00Z")).await;
        let reader_store = reopen_store(object_store, summaries);
        let original_title = writer.state().await.unwrap().title;
        let update = event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.title.updated",
            &serde_json::json!({ "title": "Concurrent title" }),
        );

        let (loaded, appended) = tokio::join!(
            reader_store.load_run_projection(&run_id),
            writer.append_event(&update)
        );

        appended.unwrap();
        let loaded = loaded.unwrap().unwrap();
        assert!(loaded.title == original_title || loaded.title == "Concurrent title");
        assert!(reader_store.active_runs.lock().await.is_empty());
    }

    #[tokio::test]
    async fn required_run_summary_append_refreshes_active_projection_and_delete_removes_rows() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (_object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run = store.create_run(&test_run_id("run-1")).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.runnable",
            &serde_json::json!({ "source": "start_requested" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.starting",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:02Z",
            "run.running",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload_with_node(
            "run-1",
            "2026-03-27T12:00:03Z",
            "stage.started",
            &serde_json::json!({
                "index": 0,
                "handler_type": "prompt",
                "attempt": 1,
                "max_attempts": 1,
            }),
            Some("review"),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload_with_node(
            "run-1",
            "2026-03-27T12:00:04Z",
            "interview.started",
            &serde_json::json!({
                "question_id": "q-1",
                "question": "Approve deploy?",
                "stage": "review",
                "question_type": "yes_no",
                "options": [],
                "allow_freeform": false,
                "context_display": null,
                "timeout_seconds": null,
            }),
            Some("review"),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload_with_node(
            "run-1",
            "2026-03-27T12:00:05Z",
            "checkpoint.completed",
            &serde_json::json!({
                "status": "running",
                "current_node": "review",
                "completed_nodes": [],
                "node_retries": {},
                "context_values": {},
                "node_outcomes": {},
                "next_node_id": "review",
                "git_commit_sha": "abc123",
                "loop_failure_signatures": {},
                "restart_failure_signatures": {},
                "node_visits": { "review": 1 },
            }),
            Some("review"),
        ))
        .await
        .unwrap();

        let projection = store
            .load_run_projection(&test_run_id("run-1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.status, RunStatus::Running);
        assert_eq!(run.last_event_seq().await.unwrap(), Some(7));
        assert_eq!(
            projection
                .stage(&StageId::new("review", 1))
                .unwrap()
                .effective_state(),
            fabro_types::StageState::Running
        );
        assert_eq!(
            projection.pending_interviews["q-1"].question.text,
            "Approve deploy?"
        );
        assert_eq!(
            projection
                .current_checkpoint()
                .unwrap()
                .git_commit_sha
                .as_deref(),
            Some("abc123")
        );

        let comparison_time = dt("2026-03-27T12:00:10Z");
        let sql_summary = summaries
            .get(&test_run_id("run-1"), comparison_time)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sql_summary.lifecycle.status, RunStatus::Running);

        store.delete_run(&test_run_id("run-1")).await.unwrap();
        assert!(
            store
                .load_run_projection(&test_run_id("run-1"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            summaries
                .get(&test_run_id("run-1"), Utc::now())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn opening_sql_backed_run_does_not_read_legacy_event_history() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        append_completed(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;

        let reopened = reopen_store(object_store, summaries);
        // Opening and projecting from canonical SQLite must not inspect an
        // unreadable key in the retained legacy event history.
        let mut unreadable_old_key = keys::run_event_seq_prefix(&run_id, 2).as_ref().to_vec();
        unreadable_old_key.push(0xff);
        reopened
            .open_db()
            .await
            .unwrap()
            .put(unreadable_old_key, b"invalid json")
            .await
            .unwrap();

        let fresh_writer = reopened.open_run(&run_id).await.unwrap();
        assert_eq!(fresh_writer.last_event_seq().await.unwrap(), Some(5));
        let state = fresh_writer.state().await.unwrap();
        assert_eq!(state.status, RunStatus::Succeeded {
            reason: SuccessReason::Completed,
        });

        let seq = fresh_writer
            .append_event(&event_payload(
                "run-1",
                "2026-03-27T12:00:05Z",
                "run.title.updated",
                &serde_json::json!({ "title": "Renamed completed run" }),
            ))
            .await
            .unwrap();
        assert_eq!(seq, 6);
    }

    #[tokio::test]
    async fn append_event_hydrates_local_projection_cache_for_fresh_writer() {
        let (_directory, summaries) = make_run_summary_store().await;
        let (object_store, store) = make_store_with_run_summaries(Arc::clone(&summaries));
        let run_id = test_run_id("run-1");
        let run = store.create_run(&run_id).await.unwrap();
        append_created(&run, "run-1", dt("2026-03-27T12:00:00Z")).await;
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:01Z",
            "run.runnable",
            &serde_json::json!({ "source": "start_requested" }),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:02Z",
            "run.starting",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&event_payload(
            "run-1",
            "2026-03-27T12:00:03Z",
            "run.running",
            &serde_json::json!({}),
        ))
        .await
        .unwrap();
        run.append_event(&workflow_failure_payload("run-1"))
            .await
            .unwrap();

        let reopened = store_test_support::test_database_with_stores(
            object_store,
            "runs/",
            Duration::from_millis(1),
            None,
            store_test_support::test_blob_store(),
            summaries,
        );
        let fresh_writer = reopened.open_run(&run_id).await.unwrap();
        fresh_writer
            .append_event(&event_payload(
                "run-1",
                "2026-03-27T12:00:05Z",
                "run.title.updated",
                &serde_json::json!({ "title": "Renamed failed run" }),
            ))
            .await
            .unwrap();

        let state = fresh_writer.state().await.unwrap();
        assert_eq!(state.title, "Renamed failed run");
        assert_eq!(state.status, RunStatus::Failed {
            reason: FailureReason::WorkflowError,
        });

        let projection = reopened
            .load_run_projection(&run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.title, "Renamed failed run");
        assert_eq!(projection.status, RunStatus::Failed {
            reason: FailureReason::WorkflowError,
        });
    }
}
