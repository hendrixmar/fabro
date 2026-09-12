use anyhow::{Context, Result};
use fabro_types::{RunId, RunSandboxInstance};
use sandbox_driver::{EventContext, PtySession, PtySize};

use crate::driver::ProviderAccess;
use crate::driver_sandbox::RunSandbox;
use crate::provider_sandbox;

/// Reconnect to a run's sandbox from its saved record.
///
/// `access` carries the provider settings and vault credentials the record's
/// provider needs; the process environment is never consulted. `run_id`
/// narrows the ownership scope to the run when known, and the driver reports
/// the sandbox's lifecycle from here on through `events`.
pub async fn reconnect_for_run(
    record: &RunSandboxInstance,
    access: &ProviderAccess,
    run_id: Option<RunId>,
    events: Option<EventContext>,
) -> Result<RunSandbox> {
    let runtime = &record.runtime;
    provider_sandbox::attach_provider_sandbox(
        record.provider.clone(),
        access,
        &runtime.id,
        // A record without the flag was written for a sandbox fabro never
        // cloned into.
        runtime.repo_cloned.unwrap_or(false),
        runtime.working_directory.clone(),
        runtime.clone_origin_url.clone(),
        run_id,
        events,
    )
    .await
    .with_context(|| format!("Failed to reconnect {} sandbox", record.provider))
}

/// Opens an interactive shell in a run's sandbox over the driver's Pty
/// facet, reconnecting from the run record first. The session is the
/// driver's own; it is closed by the caller.
pub async fn open_terminal_for_run(
    record: &RunSandboxInstance,
    access: &ProviderAccess,
    run_id: Option<RunId>,
    size: PtySize,
) -> crate::Result<Box<dyn PtySession>> {
    let sandbox = reconnect_for_run(record, access, run_id, None)
        .await
        .map_err(|err| crate::Error::context_anyhow("Failed to reconnect sandbox", err))?;
    sandbox.activate().await?;
    sandbox.open_terminal(size).await
}
