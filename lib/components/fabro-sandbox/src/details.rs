use anyhow::Result;
use fabro_types::{RunId, RunSandboxInstance, SandboxDetails};

use crate::driver::ProviderAccess;
use crate::reconnect;

/// The sandbox identified by `record`, as the run record fabro keeps and
/// the status the sandbox driver reports for it, on every provider.
pub async fn sandbox_details(
    record: &RunSandboxInstance,
    access: &ProviderAccess,
    run_id: Option<RunId>,
) -> Result<SandboxDetails> {
    let sandbox = reconnect::reconnect_for_run(record, access, run_id, None).await?;
    let status = sandbox.handle()?.describe().await.map_err(|err| {
        anyhow::anyhow!(
            "Failed to describe {} sandbox '{}': {err}",
            record.provider,
            record.runtime.id
        )
    })?;
    Ok(SandboxDetails {
        sandbox: record.clone(),
        status,
    })
}
