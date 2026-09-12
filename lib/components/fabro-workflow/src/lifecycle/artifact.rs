use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use fabro_core::error::{Error as CoreError, Result as CoreResult};
use fabro_core::graph::NodeSpec;
use fabro_core::lifecycle::{AttemptResultContext, RunLifecycle};
use fabro_core::outcome::NodeResult;
use fabro_core::state::ExecutionState;
use fabro_store::{ArtifactKey, ArtifactStore};
use fabro_types::{ArtifactUpload, EventBody, RunId, StageId};
use fabro_util::error::collect_chain;
use fabro_util::workspace_glob::{WorkspaceGlobError, WorkspaceGlobSet};
use tokio::fs;
use tokio::sync::OnceCell;
use tokio::time::sleep;

use crate::artifact::{normalize_durable_updates, offload_large_values, sync_artifacts_to_env};
use crate::artifact_snapshot::{ArtifactCollectionSummary, collect_artifacts};
use crate::artifact_upload::ArtifactSink;
use crate::event::{Emitter, Event, RunNoticeCode, RunNoticeLevel};
use crate::graph::{WorkflowGraph, WorkflowNode};
use crate::lifecycle::event::stage_scope_for;
use crate::outcome::BilledModelUsage;
use crate::runtime_store::RunStoreHandle;
use crate::stage_execution::StageExecutionTracker;

type WfRunState = ExecutionState<Option<BilledModelUsage>>;
type WfNodeResult = NodeResult<Option<BilledModelUsage>>;
type ArtifactIdentity = (String, String);

const ARTIFACT_UPLOAD_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
];

/// Sub-lifecycle responsible for artifact collection, offloading, and syncing.
pub(crate) struct ArtifactLifecycle {
    pub sandbox:        Arc<fabro_sandbox::RunSandbox>,
    pub run_store:      RunStoreHandle,
    pub emitter:        Arc<Emitter>,
    pub run_id:         RunId,
    artifact_globs:     std::result::Result<WorkspaceGlobSet, WorkspaceGlobError>,
    pub artifact_sink:  Option<ArtifactSink>,
    captured_artifacts: std::sync::Mutex<HashSet<ArtifactIdentity>>,
    ledger_initialized: OnceCell<()>,
    /// Run-scoped stage execution allocator shared with `RunServices`.
    stage_executions:   StageExecutionTracker,
}

impl ArtifactLifecycle {
    pub(crate) fn new(
        sandbox: Arc<fabro_sandbox::RunSandbox>,
        run_store: RunStoreHandle,
        emitter: Arc<Emitter>,
        run_id: RunId,
        artifact_globs: &[String],
        artifact_sink: Option<ArtifactSink>,
        stage_executions: StageExecutionTracker,
    ) -> Self {
        Self {
            sandbox,
            run_store,
            emitter,
            run_id,
            artifact_globs: WorkspaceGlobSet::try_new(artifact_globs),
            artifact_sink,
            captured_artifacts: std::sync::Mutex::new(HashSet::new()),
            ledger_initialized: OnceCell::new(),
            stage_executions,
        }
    }

    fn artifact_globs(&self) -> CoreResult<&WorkspaceGlobSet> {
        self.artifact_globs.as_ref().map_err(|error| {
            CoreError::Other(format!("invalid run.artifacts.include pattern: {error}"))
        })
    }
}

#[async_trait]
impl RunLifecycle<WorkflowGraph> for ArtifactLifecycle {
    async fn on_run_start(&self, _graph: &WorkflowGraph, _state: &WfRunState) -> CoreResult<()> {
        let artifact_globs = self.artifact_globs()?;
        if artifact_globs.is_empty() {
            return Ok(());
        }
        self.ledger_initialized
            .get_or_try_init(|| async {
                let ledger = self
                    .rebuild_captured_artifact_ledger()
                    .await
                    .map_err(|err| {
                        let rendered = collect_chain(err.as_ref()).join(": ");
                        CoreError::Other(format!(
                            "failed to rebuild captured artifact ledger: {rendered}"
                        ))
                    })?;
                *self.captured_artifacts.lock().expect(
                    "artifact mutex should not be poisoned: no code panics while holding this lock",
                ) = ledger;
                Ok::<(), CoreError>(())
            })
            .await?;
        Ok(())
    }

    async fn after_attempt(
        &self,
        ctx: &AttemptResultContext<'_, WorkflowGraph>,
        state: &WfRunState,
    ) -> CoreResult<()> {
        let artifact_globs = self.artifact_globs()?;
        if artifact_globs.is_empty() {
            return Ok(());
        }
        let node_id = ctx.node.id();
        // Artifact identity follows the stage execution ordinal so a resumed
        // reexecution stores its captures under the new `StageId`.
        let scope = stage_scope_for(&self.stage_executions, state, node_id);
        let visit = scope.visit;
        let node_slug = if visit <= 1 {
            node_id.to_string()
        } else {
            format!("{node_id}-visit_{visit}")
        };
        let artifact_capture_dir =
            tempfile::tempdir().map_err(|err| CoreError::Other(err.to_string()))?;

        match collect_artifacts(&self.sandbox, artifact_capture_dir.path(), artifact_globs).await {
            Ok(summary) => {
                self.emit_collection_problem_notice(node_id, &summary);
                let new_assets = self.new_captured_assets(&summary.captured_assets);
                if new_assets.is_empty() {
                    return Ok(());
                }

                let stage_id = scope.stage_id();
                if let Err(err) = self
                    .persist_artifacts(
                        &stage_id,
                        ctx.attempt,
                        artifact_capture_dir.path(),
                        &new_assets,
                    )
                    .await
                {
                    self.emitter.notice(
                        RunNoticeLevel::Warn,
                        RunNoticeCode::ArtifactUploadFailed,
                        format!("[node: {node_id}] artifact upload failed: {err}"),
                    );
                    return Ok(());
                }
                self.record_captured_assets(&new_assets);
                for asset in &new_assets {
                    self.emitter.emit_scoped(
                        &Event::ArtifactCaptured {
                            node_id:        node_id.to_string(),
                            attempt:        ctx.attempt,
                            node_slug:      node_slug.clone(),
                            path:           asset.path.clone(),
                            mime:           asset.mime.clone(),
                            content_md5:    asset.content_md5.clone(),
                            content_sha256: asset.content_sha256.clone(),
                            bytes:          asset.bytes,
                        },
                        &scope,
                    );
                }
            }
            Err(e) => {
                self.emitter.notice(
                    RunNoticeLevel::Warn,
                    RunNoticeCode::ArtifactCollectionFailed,
                    format!("[node: {node_id}] artifact collection failed: {e}"),
                );
            }
        }

        Ok(())
    }

    async fn after_node(
        &self,
        node: &WorkflowNode,
        result: &mut WfNodeResult,
        _state: &WfRunState,
    ) -> CoreResult<()> {
        let node_id = node.id();

        // Offload large context_updates values to artifact store
        if let Err(e) =
            offload_large_values(&mut result.outcome.context_updates, &self.run_store).await
        {
            self.emitter.notice(
                RunNoticeLevel::Warn,
                RunNoticeCode::ArtifactOffloadFailed,
                format!("[node: {node_id}] artifact offload failed: {e}"),
            );
        }

        normalize_durable_updates(&mut result.outcome.context_updates);

        // Sync file-backed artifacts to sandbox environment
        if let Err(e) =
            sync_artifacts_to_env(&mut result.outcome.context_updates, &self.sandbox).await
        {
            self.emitter.notice(
                RunNoticeLevel::Warn,
                RunNoticeCode::ArtifactSyncFailed,
                format!("[node: {node_id}] artifact sync failed: {e}"),
            );
        }

        Ok(())
    }
}

impl ArtifactLifecycle {
    async fn rebuild_captured_artifact_ledger(&self) -> Result<HashSet<ArtifactIdentity>> {
        let events = self
            .run_store
            .list_events()
            .await
            .context("failed to list run events")?;
        Ok(events
            .into_iter()
            .filter_map(|envelope| match envelope.event.body {
                EventBody::ArtifactCaptured(props) => Some((props.path, props.content_sha256)),
                _ => None,
            })
            .collect())
    }

    fn emit_collection_problem_notice(&self, node_id: &str, summary: &ArtifactCollectionSummary) {
        if summary.download_errors == 0 && summary.hash_errors == 0 {
            return;
        }

        let mut parts = Vec::new();
        if summary.download_errors > 0 {
            parts.push(format!("{} download error(s)", summary.download_errors));
        }
        if summary.hash_errors > 0 {
            parts.push(format!("{} hash/read error(s)", summary.hash_errors));
        }
        self.emitter.notice(
            RunNoticeLevel::Warn,
            RunNoticeCode::ArtifactCollectionFailed,
            format!(
                "[node: {node_id}] artifact collection completed with {}",
                parts.join(", ")
            ),
        );
    }

    fn new_captured_assets(&self, artifacts: &[ArtifactUpload]) -> Vec<ArtifactUpload> {
        let ledger = self.captured_artifacts.lock().expect(
            "artifact mutex should not be poisoned: no code panics while holding this lock",
        );
        artifacts
            .iter()
            .filter(|artifact| !ledger.contains(&artifact_identity(artifact)))
            .cloned()
            .collect()
    }

    fn record_captured_assets(&self, artifacts: &[ArtifactUpload]) {
        let mut ledger = self.captured_artifacts.lock().expect(
            "artifact mutex should not be poisoned: no code panics while holding this lock",
        );
        for artifact in artifacts {
            ledger.insert(artifact_identity(artifact));
        }
    }

    async fn persist_artifacts(
        &self,
        stage_id: &StageId,
        retry: u32,
        artifact_capture_dir: &std::path::Path,
        artifacts: &[ArtifactUpload],
    ) -> Result<()> {
        let Some(sink) = self.artifact_sink.as_ref() else {
            return Err(anyhow!("artifact sink is not configured"));
        };

        let mut last_error = None;
        for attempt in 0..=ARTIFACT_UPLOAD_RETRY_DELAYS.len() {
            match self
                .persist_artifacts_once(sink, stage_id, retry, artifact_capture_dir, artifacts)
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => last_error = Some(err),
            }

            if let Some(delay) = ARTIFACT_UPLOAD_RETRY_DELAYS.get(attempt) {
                sleep(*delay).await;
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("artifact upload failed")))
    }

    async fn persist_artifacts_once(
        &self,
        sink: &ArtifactSink,
        stage_id: &StageId,
        retry: u32,
        artifact_capture_dir: &std::path::Path,
        artifacts: &[ArtifactUpload],
    ) -> Result<()> {
        match sink {
            ArtifactSink::Store(store) => {
                self.store_artifacts(store, stage_id, retry, artifact_capture_dir, artifacts)
                    .await
            }
            ArtifactSink::Uploader(uploader) => {
                uploader
                    .upload_stage_artifacts(stage_id, retry, artifact_capture_dir, artifacts)
                    .await
            }
        }
    }

    async fn store_artifacts(
        &self,
        store: &ArtifactStore,
        stage_id: &StageId,
        retry: u32,
        artifact_capture_dir: &std::path::Path,
        artifacts: &[ArtifactUpload],
    ) -> Result<()> {
        for artifact in artifacts {
            let local_path = artifact_capture_dir.join(&artifact.path);
            let bytes = fs::read(&local_path)
                .await
                .with_context(|| format!("failed to read artifact {}", local_path.display()))?;
            store
                .put(
                    &self.run_id,
                    &ArtifactKey::new(stage_id.clone(), retry, artifact.path.clone()),
                    &bytes,
                )
                .await
                .map_err(anyhow::Error::new)?;
        }
        Ok(())
    }
}

fn artifact_identity(artifact: &ArtifactUpload) -> ArtifactIdentity {
    (artifact.path.clone(), artifact.content_sha256.clone())
}
