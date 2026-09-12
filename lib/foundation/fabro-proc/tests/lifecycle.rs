#![cfg(unix)]
use std::process::Stdio;
use std::time::Duration;

use fabro_proc::{self as process, ProcessError};
use tokio::process::Command;
use tokio::{fs, task, time};
use tokio_util::sync::CancellationToken;

struct Fixture(tempfile::TempDir);
impl Fixture {
    fn new() -> Self {
        Self(tempfile::tempdir().expect("fixture directory"))
    }
    fn command(&self, script: &str) -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", script]).current_dir(self.0.path());
        command
    }
    async fn ready(&self) {
        time::timeout(Duration::from_secs(5), async {
            loop {
                if fs::read_to_string(self.0.path().join("helper.pid"))
                    .await
                    .ok()
                    .and_then(|s| s.trim().parse::<u32>().ok())
                    .is_some()
                {
                    break;
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("helper readiness");
    }
    async fn pid(&self, file: &str) -> u32 {
        fs::read_to_string(self.0.path().join(file))
            .await
            .expect("fixture process observation")
            .trim()
            .parse()
            .expect("fixture process observation")
    }
    async fn stopped(&self) {
        for name in ["leader.pid", "helper.pid"] {
            let pid = self.pid(name).await;
            time::timeout(Duration::from_secs(5), async {
                while task::spawn_blocking(move || process::process_running_strict(pid))
                    .await
                    .expect("fixture process observation")
                {
                    time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("owned process must stop");
        }
        // Direct child must also be reaped, not merely a zombie.
        let leader = self.pid("leader.pid").await;
        time::timeout(Duration::from_secs(5), async {
            while process::process_exists(leader) {
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("direct child must be reaped while runtime lives");
    }
}
impl Drop for Fixture {
    #[expect(
        clippy::disallowed_methods,
        reason = "Fail-safe test cleanup must run synchronously on panic"
    )]
    fn drop(&mut self) {
        for name in ["leader.pid", "helper.pid"] {
            if let Some(pid) = std::fs::read_to_string(self.0.path().join(name))
                .ok()
                .and_then(|s| s.trim().parse().ok())
            {
                if name == "leader.pid" {
                    process::sigkill_process_group(pid);
                } else {
                    process::sigkill(pid);
                }
            }
        }
    }
}
const HELD: &str = "echo $$ > leader.pid; sleep 60 & echo $! > helper.pid; printf partial; exit 0";
const WAIT: &str = "echo $$ > leader.pid; sleep 60 >/dev/null 2>&1 & echo $! > helper.pid; wait";
const IGNORE_TERM: &str = "echo $$ > leader.pid; trap 'exit 0' TERM; sh -c 'trap \"\" TERM; echo $$ > helper.pid; exec sleep 60' & wait";

#[tokio::test]
async fn abort_execution_and_held_pipe_drain_reaps_and_kills_group() {
    for script in [WAIT, HELD] {
        let fixture = Fixture::new();
        let mut command = fixture.command(script);
        let task = tokio::spawn(async move {
            process::capture(&mut command, None, &CancellationToken::new(), None).await
        });
        fixture.ready().await;
        if script == HELD {
            let leader = fixture.pid("leader.pid").await;
            time::timeout(Duration::from_secs(5), async {
                while process::process_exists(leader) {
                    time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        fixture.stopped().await;
    }
}

#[tokio::test]
async fn timeout_and_cancellation_stop_real_descendants() {
    for cancel in [false, true] {
        let fixture = Fixture::new();
        let mut command = fixture.command(IGNORE_TERM);
        let token = CancellationToken::new();
        let run = process::capture(&mut command, Some(Duration::from_millis(500)), &token, None);
        let trigger = async {
            fixture.ready().await;
            if cancel {
                token.cancel();
            }
        };
        let (result, ()) =
            time::timeout(Duration::from_secs(8), async { tokio::join!(run, trigger) })
                .await
                .unwrap();
        assert!(matches!(
            (cancel, result),
            (true, Err(ProcessError::Cancelled)) | (false, Err(ProcessError::TimedOut))
        ));
        fixture.stopped().await;
    }
}

#[tokio::test]
async fn normal_exit_preserves_helpers_with_closed_pipes() {
    for code in [0, 7] {
        let fixture = Fixture::new();
        let mut command = fixture.command(&format!("echo $$ > leader.pid; sleep 60 </dev/null >/dev/null 2>&1 & echo $! > helper.pid; exit {code}"));
        let output = process::capture(
            &mut command,
            Some(Duration::from_secs(5)),
            &CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(output.output.status.code(), Some(code));
        assert!(process::process_exists(fixture.pid("helper.pid").await));
    }
}

#[tokio::test]
async fn prefix_cap_drains_both_streams_and_preserves_raw_bytes() {
    for cap in [None, Some(0), Some(257)] {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "head -c 200000 /dev/zero & head -c 200000 /dev/zero >&2 & wait; printf '\\377'",
        ]);
        let output = time::timeout(
            Duration::from_secs(8),
            process::capture(&mut command, None, &CancellationToken::new(), cap),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.output.status.success());
        assert_eq!(output.output.stdout.len(), cap.unwrap_or(200_001));
        assert_eq!(output.output.stderr.len(), cap.unwrap_or(200_000));
        assert_eq!(output.stdout_truncated, cap.is_some());
        assert_eq!(output.stderr_truncated, cap.is_some());
        if cap.is_none() {
            assert_eq!(output.output.stdout.last(), Some(&255));
        }
    }
    let mut command = Command::new("sh");
    command.args(["-c", "printf abcd; printf x >&2"]);
    let output = process::capture(&mut command, None, &CancellationToken::new(), Some(2))
        .await
        .unwrap();
    assert!(output.stdout_truncated);
    assert!(!output.stderr_truncated);
}

#[tokio::test]
async fn spawn_failure_preserves_source_without_rendering_command_details() {
    use std::error::Error;
    let mut command = Command::new("/nonexistent/fabro-test-executable");
    let error = process::capture(&mut command, None, &CancellationToken::new(), None)
        .await
        .err()
        .unwrap();
    assert!(error.source().unwrap().is::<std::io::Error>());
    assert_eq!(error.to_string(), "process I/O failed");
    assert_eq!(format!("{error:?}"), "process I/O failed");
}

#[tokio::test]
async fn capture_preserves_prepared_stdin_and_literal_arguments() {
    let fixture = Fixture::new();
    let input_path = fixture.0.path().join("input");
    fs::write(&input_path, b"configured stdin\n").await.unwrap();
    let input = fs::File::open(&input_path).await.unwrap().into_std().await;
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            "cat; printf '%s' \"$1\"",
            "fixture",
            "$(must-not-run)",
        ])
        .stdin(Stdio::from(input));
    let result = process::capture(
        &mut command,
        Some(Duration::from_secs(5)),
        &CancellationToken::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.output.stdout, b"configured stdin\n$(must-not-run)");
    let mut command = Command::new("cat");
    command.stdin(Stdio::piped());
    let result = process::capture(
        &mut command,
        Some(Duration::from_secs(5)),
        &CancellationToken::new(),
        None,
    )
    .await
    .unwrap();
    assert!(result.output.stdout.is_empty());
    assert!(result.output.status.success());
}
