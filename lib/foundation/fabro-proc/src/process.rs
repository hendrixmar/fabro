//! Finite local commands: the leader and required output share one deadline.
//!
//! Capture and child waiting are scoped futures, never spawned. Cancellation
//! drops the readers and kills the owned group before reaping the direct child.
use std::future::{self, Future};
use std::io;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::time;
use tokio_util::sync::CancellationToken;

/// Infrastructure failure; command arguments, environment and output are
/// absent.
#[derive(thiserror::Error)]
pub enum ProcessError {
    #[error("process operation cancelled")]
    Cancelled,
    #[error("process operation timed out")]
    TimedOut,
    #[error("process I/O failed")]
    Io(#[from] io::Error),
}

impl std::fmt::Debug for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// Owns a child and its original process group through output completion.
///
/// On Unix, drop synchronously requests group KILL. Tokio's child reaper owns
/// the remaining direct-child wait (its orphan queue on Unix); no application
/// cleanup task or pipe task is detached. Orderly paths await reaping. Drop
/// reaping needs a live Tokio runtime and is best effort, not a bounded
/// promise. Non-Unix cleanup covers only the direct child. Escaped groups,
/// runtime/process destruction and uninterruptible OS failures are outside this
/// contract.
///
/// Normal completion, including nonzero exit, preserves helpers that closed
/// inherited pipes. Dropping an unfinished owner always requests cleanup.
#[must_use]
struct ProcessOwner {
    child: Child,
    group: Option<u32>,
    armed: bool,
}

// Bounds direct-child reaping after immediate termination. Output readers
// are dropped when capture fails, so there is no separate drain allowance.
const CLEANUP_ALLOWANCE: Duration = Duration::from_secs(2);

impl ProcessOwner {
    /// Spawn a prepared command in a fresh group. All command policy stays with
    /// the caller; only group setup and kill-on-drop are supplied here.
    fn spawn(command: &mut Command) -> Result<Self, ProcessError> {
        #[cfg(unix)]
        command.process_group(0);
        let child = command.kill_on_drop(true).spawn()?;
        let group = child.id();
        tracing::debug!(pid = group, "Spawned owned local process");
        Ok(Self {
            child,
            group,
            armed: true,
        })
    }

    /// Wait for the leader and both readers under one deadline. Dropping the
    /// completion future closes the pipes; the owner remains armed until the
    /// command completes normally or termination and reaping succeed.
    async fn complete(
        mut self,
        output: impl Future<Output = io::Result<()>>,
        timeout: Option<Duration>,
        cancel: &CancellationToken,
    ) -> Result<ExitStatus, ProcessError> {
        let deadline = async {
            match timeout {
                Some(duration) => time::sleep(duration).await,
                None => future::pending().await,
            }
        };
        let result = tokio::select! {
            result = async {
                let (status, ()) = tokio::try_join!(self.child.wait(), output)?;
                Ok(status)
            } => result,
            () = cancel.cancelled() => Err(ProcessError::Cancelled),
            () = deadline => Err(ProcessError::TimedOut),
        };
        match result {
            Ok(status) => {
                self.armed = false;
                tracing::debug!("Local process and output completed");
                Ok(status)
            }
            Err(failure) => {
                tracing::debug!(reason = %failure, "Stopping local process");
                if let Err(error) = self.terminate().await {
                    // Preserve an original I/O failure and leave drop's
                    // fallback armed. Cleanup failure is never called success.
                    tracing::warn!(error_kind = ?error.kind(), "Local process cleanup failed");
                    return Err(match failure {
                        ProcessError::Io(_) => failure,
                        _ => ProcessError::Io(error),
                    });
                }
                self.armed = false;
                Err(failure)
            }
        }
    }

    async fn terminate(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        if let Some(group) = self.group {
            kill_group(group)?;
        }
        self.child.start_kill()?;
        time::timeout(CLEANUP_ALLOWANCE, self.child.wait())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "process reaping timed out"))??;
        Ok(())
    }
}

impl Drop for ProcessOwner {
    fn drop(&mut self) {
        if self.armed {
            #[cfg(unix)]
            if let Some(group) = self.group {
                if let Err(error) = kill_group(group) {
                    tracing::warn!(error_kind = ?error.kind(), "Dropped local process group cleanup failed");
                }
            }
            // Also covers non-Unix and a failed group signal. Tokio retains
            // direct-child reaping responsibility when Child is dropped.
            let _ = self.child.start_kill();
        }
    }
}

#[cfg(unix)]
fn kill_group(group: u32) -> io::Result<()> {
    let group = i32::try_from(group)
        .ok()
        .filter(|group| *group > 0)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid owned process group")
        })?;
    // SAFETY: positive saved child id names the fresh group established at spawn;
    // negation targets that group, never the caller's group or all processes.
    if unsafe { libc::kill(-group, libc::SIGKILL) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

/// Raw capture with per-stream completeness, retaining std's output shape.
/// Deliberately does not implement Debug: output may contain credentials.
pub struct CapturedOutput {
    pub output:           Output,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

/// Capture raw bytes with optional per-stream prefix retention. Reaching the
/// cap continues draining and is not process failure. `None` retains unlimited
/// bytes. Like Tokio Command::output, stdin configuration is preserved (a piped
/// stdin is closed before waiting); stdout/stderr are captured concurrently.
///
/// Timeout/cancellation covers both leader exit and EOF. On failure, readers
/// close immediately, the owned group is killed, and direct-child reaping gets
/// up to two seconds. Dropping this future also requests group KILL; Tokio
/// owns eventual direct-child reaping while its runtime remains alive. On
/// non-Unix platforms only the direct child is killed. Normal completion,
/// including nonzero exit, preserves helpers that closed inherited pipes.
pub async fn capture(
    command: &mut Command,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
    prefix_cap: Option<usize>,
) -> Result<CapturedOutput, ProcessError> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut owner = ProcessOwner::spawn(command)?;
    let stdout = owner.child.stdout.take();
    let stderr = owner.child.stderr.take();
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let mut stdout_truncated = false;
    let mut stderr_truncated = false;
    let status = owner
        .complete(
            async {
                tokio::try_join!(
                    drain(stdout, &mut stdout_bytes, &mut stdout_truncated, prefix_cap),
                    drain(stderr, &mut stderr_bytes, &mut stderr_truncated, prefix_cap),
                )?;
                Ok(())
            },
            timeout,
            cancel,
        )
        .await?;
    Ok(CapturedOutput {
        output: Output {
            status,
            stdout: stdout_bytes,
            stderr: stderr_bytes,
        },
        stdout_truncated,
        stderr_truncated,
    })
}

async fn drain<R: AsyncRead + Unpin>(
    reader: Option<R>,
    bytes: &mut Vec<u8>,
    truncated: &mut bool,
    cap: Option<usize>,
) -> io::Result<()> {
    if let Some(mut reader) = reader {
        let mut chunk = vec![0; 8192];
        loop {
            let count = reader.read(&mut chunk).await?;
            if count == 0 {
                break;
            }
            let retained = count.min(cap.unwrap_or(usize::MAX).saturating_sub(bytes.len()));
            bytes.extend_from_slice(&chunk[..retained]);
            *truncated |= retained < count;
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::error::Error;

    use super::*;

    #[tokio::test]
    async fn read_failure_preserves_source_and_reaps_child() {
        let mut command = Command::new("sleep");
        command
            .arg("60")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let owner = ProcessOwner::spawn(&mut command).unwrap();
        let pid = owner.child.id().unwrap();
        let output = async {
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "reader failed",
            ))
        };
        let error = owner
            .complete(
                output,
                Some(Duration::from_secs(5)),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(error.source().unwrap().is::<io::Error>());
        assert_eq!(error.to_string(), "process I/O failed");
        assert_eq!(format!("{error:?}"), "process I/O failed");
        assert!(!crate::process_exists(pid));
    }
}
