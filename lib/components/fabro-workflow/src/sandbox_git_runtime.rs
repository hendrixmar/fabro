use fabro_sandbox::{ExecResult, ExecResultExt, RunSandbox, Termination};
use fabro_util::error::SharedError;
use fabro_util::shell;
use tokio::sync::OnceCell;

use crate::sandbox_git::GitCommandError;

pub(crate) struct SandboxGitRuntime {
    probe:                   OnceCell<Result<(), SharedError>>,
    /// When the run last pushed its branch successfully (checkpoint or
    /// publish). Read by the publish failure report so "last success 67s
    /// before the failure" is visible from the run conclusion.
    last_successful_push_at: std::sync::Mutex<Option<chrono::DateTime<chrono::Utc>>>,
}

impl SandboxGitRuntime {
    pub(crate) fn new() -> Self {
        Self {
            probe:                   OnceCell::new(),
            last_successful_push_at: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn record_successful_push(&self) {
        *self
            .last_successful_push_at
            .lock()
            .expect("last push timestamp mutex poisoned") = Some(chrono::Utc::now());
    }

    pub(crate) fn last_successful_push_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        *self
            .last_successful_push_at
            .lock()
            .expect("last push timestamp mutex poisoned")
    }

    pub(crate) async fn ensure_git_available(
        &self,
        sandbox: &RunSandbox,
    ) -> Result<(), SharedError> {
        self.probe
            .get_or_init(|| async { probe_sandbox_git(sandbox).await })
            .await
            .clone()
    }
}

impl Default for SandboxGitRuntime {
    fn default() -> Self {
        Self::new()
    }
}

async fn probe_sandbox_git(sandbox: &RunSandbox) -> Result<(), SharedError> {
    let temp = sandbox_temp_dir(sandbox, "probe", "git");
    let index = format!("{temp}/index");
    let probe_file = format!("{temp}/probe.txt");
    let command = format!(
        "set -e\n\
         rm -rf {temp_q}\n\
         mkdir -p {temp_q}\n\
         printf probe > {probe_file_q}\n\
         GIT_INDEX_FILE={index_q} {git} read-tree --empty\n\
         blob=$({git} hash-object -w {probe_file_q})\n\
         GIT_INDEX_FILE={index_q} {git} update-index --add --cacheinfo 100644,$blob,probe.txt\n\
         GIT_INDEX_FILE={index_q} {git} write-tree >/dev/null\n\
         rm -rf {temp_q}",
        temp_q = shell::shell_quote(&temp),
        probe_file_q = shell::shell_quote(&probe_file),
        index_q = shell::shell_quote(&index),
        git = "git -c maintenance.auto=0 -c gc.auto=0",
    );
    exec_ok(sandbox, &command).await
}

fn sandbox_temp_dir(sandbox: &RunSandbox, run_id: &str, label: &str) -> String {
    let cwd = sandbox.working_directory().trim_end_matches('/');
    let id = uuid::Uuid::new_v4();
    format!("{cwd}/.fabro/tmp/{label}-{run_id}-{id}")
}

async fn exec_ok(sandbox: &RunSandbox, command: &str) -> Result<(), SharedError> {
    let result = sandbox
        .exec_command(command, 30_000, None, None, None)
        .await
        .map_err(|err| {
            SharedError::new(anyhow::Error::new(err).context("sandbox git probe command failed"))
        })?;
    if result.success() {
        Ok(())
    } else {
        Err(SharedError::new(anyhow::Error::new(exec_err(
            command, result,
        ))))
    }
}

/// The probe's failure, named by how the command ended; the output tail
/// travels in the source.
fn exec_err(label: &str, result: ExecResult) -> GitCommandError {
    let duration_ms = result.duration_ms();
    let message = match result.termination {
        Termination::TimedOut => format!("{label} timed out after {duration_ms}ms"),
        Termination::Cancelled | Termination::Killed => {
            format!("{label} cancelled after {duration_ms}ms")
        }
        _ => format!(
            "{label} failed (exit {})",
            result.program_exit_code().unwrap_or(-1)
        ),
    };
    GitCommandError {
        message,
        source: result.into_exec_error(label),
    }
}
