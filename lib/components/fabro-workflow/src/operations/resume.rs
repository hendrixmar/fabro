use std::path::Path;

use super::start::{StartServices, Started, execute_persisted_run};
use crate::error::Error;
use crate::event::{Event, append_event_to_sink};
use crate::outcome::StageOutcome;
use crate::pipeline::ResumeState;
use crate::run_status::RunStatus;

/// Resume a workflow run from its checkpoint. Errors if no checkpoint is found.
pub async fn resume(run_dir: &Path, services: StartServices) -> Result<Started, Error> {
    let state = services
        .run_store
        .state()
        .await
        .map_err(|err| Error::engine(err.to_string()))?;

    let status = state.status;
    super::archive::ensure_not_archived(state.archived_at.is_some(), &services.run_id)?;
    if matches!(status, RunStatus::Succeeded { .. }) {
        return Err(Error::Precondition(
            "run already finished successfully — nothing to resume".to_string(),
        ));
    }
    if let Some(conclusion) = state.conclusion.as_ref() {
        if matches!(
            conclusion.status,
            StageOutcome::Succeeded | StageOutcome::PartiallySucceeded | StageOutcome::Skipped
        ) {
            return Err(Error::Precondition(
                "run already finished successfully — nothing to resume".to_string(),
            ));
        }
    }

    let resume_state = ResumeState::from_projection(&state)
        .ok_or_else(|| Error::Precondition("no checkpoint to resume from".to_string()))?;
    let definition_blob = state.spec.definition_blob;

    cleanup_resume_artifacts(run_dir);
    append_event_to_sink(
        &services.event_sink,
        &services.run_id,
        &Event::RunSubmitted { definition_blob },
    )
    .await?;

    Box::pin(execute_persisted_run(run_dir, Some(resume_state), services)).await
}

fn cleanup_resume_artifacts(run_dir: &Path) {
    let _ = run_dir;
}
