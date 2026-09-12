//! Project memory for prompt stages.
//!
//! Agent stages ask pebble to discover the profile's instruction files from
//! the repository root down (`MemoryDiscovery::from_git_root`). A prompt
//! stage reads the working directory alone, as it always has, through the
//! same discovery and the same loader, so the two agree on which files a
//! harness reads and how much of them fits.

use fabro_sandbox::RunSandbox;
use fabro_types::AgentProfileKind;
use pebble_coding_agent::environment::Environment;
use pebble_coding_agent::{InterruptReason, MemoryDiscovery, ProjectMemory};
use tokio_util::sync::CancellationToken;

use crate::error::Error;

/// The memory text a prompt stage inlines into its system prompt: the
/// profile's instruction files in the sandbox working directory, loaded by
/// pebble's [`ProjectMemory`] rules.
///
/// # Errors
///
/// Returns [`Error::Cancelled`] when `cancel` fires around a read.
pub async fn load_memory_text(
    sandbox: &RunSandbox,
    profile_kind: AgentProfileKind,
    cancel: &CancellationToken,
) -> Result<Option<String>, Error> {
    let environment: &dyn Environment = sandbox;
    let paths = MemoryDiscovery::working_directory()
        .resolve(environment, profile_kind, cancel)
        .await
        .map_err(cancelled_or_handler)?;
    let memory = ProjectMemory::load(environment, &paths, cancel)
        .await
        .map_err(cancelled_or_handler)?;
    Ok((!memory.is_empty()).then(|| memory.text()))
}

fn cancelled_or_handler(error: pebble_coding_agent::Error) -> Error {
    match error {
        pebble_coding_agent::Error::Interrupted(InterruptReason::Cancelled) => Error::Cancelled,
        other => Error::handler_with_source("Failed to load project memory", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_text_dedupes_and_skips_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("AGENTS.md"), "shared")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("CLAUDE.md"), "shared")
            .await
            .unwrap();
        let sandbox = fabro_sandbox::local_sandbox(dir.path().to_path_buf())
            .await
            .unwrap();

        let text = load_memory_text(
            &sandbox,
            AgentProfileKind::Anthropic,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(text.as_deref(), Some("shared"));
        let gemini = load_memory_text(
            &sandbox,
            AgentProfileKind::Gemini,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            gemini.as_deref(),
            Some("shared"),
            "AGENTS.md is every harness's"
        );
    }
}
