//! Fabro's command execution policy over the sandbox-driver [`Exec`] facet.
//!
//! The vocabulary is the driver's own: an [`ExecSpec`] and [`ExecControls`]
//! go in, an [`ExecResult`] or [`ExecStreamingResult`] comes out. This
//! module adds fabro's policy on the way in and fabro's reading of a result
//! on the way out.
//!
//! A command runs as Bash source under `bash -c` with `BASH_ENV` blanked by
//! the driver whatever the caller passed, and ends in one of three ways:
//!
//! - **timeout**: the spec's timeout fires and the provider runs the stop
//!   ladder fabro asks for — `TERM`, then `KILL` after
//!   [`SandboxExec::stop_grace`]. The result reports [`Termination::TimedOut`].
//! - **cancellation**: the caller's [`CancellationToken`] is the `term` stop;
//!   the provider escalates to `KILL` after the same grace. The result reports
//!   [`Termination::Cancelled`].
//! - **exit**: the process ended on its own.
//!
//! Output is drained regardless of the retention cap and delivered live
//! through the caller's [`sandbox_driver::OutputSink`]. Fabro reads command
//! output as text, so the policy asks the driver for
//! [`OutputSanitization::StripAll`]: terminal escape sequences and stray
//! control characters never reach a result, a sink chunk, or a tail. Secret
//! redaction stays fabro's job and happens only when a tail is rendered for
//! events or logs ([`ExecResultExt`]). The explicit environment reaches the
//! provider as the caller composed it: the driver filters credential-shaped
//! names out of the *inherited* host environment itself and treats the
//! spec's own variables as the deliberate channel for secrets, so fabro adds
//! no filter of its own.

use std::collections::HashMap;
use std::time::Duration;

use fabro_types::{CommandTermination, ExecOutputTail};
use sandbox_driver::{
    Exec, ExecControls, ExecFailure, ExecResult, ExecSpec, ExecStreamingResult, OutputSanitization,
    SpawnSpec, StdioProcess, Termination,
};
use tokio_util::sync::CancellationToken;

use crate::sandbox::{DEFAULT_EXEC_OUTPUT_TAIL_BYTES, redacted_output_tail};

/// Time between `TERM` and `KILL` when fabro stops a command.
pub const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(2);

/// Retention when a caller sets no cap: enough for any build log fabro
/// renders, bounded so a runaway command cannot exhaust memory.
pub const DEFAULT_RETAINED_OUTPUT_BYTES: usize = sandbox_driver::DEFAULT_BUFFER_BYTES;

/// Fabro's exec policy bound to one driver [`Exec`] facet.
pub struct SandboxExec<'a> {
    exec:        &'a dyn Exec,
    stop_grace:  Duration,
    /// Where a command runs when the caller names no directory. `None`
    /// leaves the choice to the provider's own working directory.
    working_dir: Option<String>,
}

impl<'a> SandboxExec<'a> {
    #[must_use]
    pub fn new(exec: &'a dyn Exec) -> Self {
        Self {
            exec,
            stop_grace: DEFAULT_STOP_GRACE,
            working_dir: None,
        }
    }

    /// The directory commands run in when the caller names none. Fabro's
    /// working directory can sit below the provider's (a cloned repository
    /// inside the container workspace), so it is passed explicitly.
    #[must_use]
    pub fn with_working_dir(mut self, working_dir: impl Into<String>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    /// Time between `TERM` and `KILL` when a command is stopped; the
    /// provider runs the ladder.
    #[must_use]
    pub fn with_stop_grace(mut self, stop_grace: Duration) -> Self {
        self.stop_grace = stop_grace;
        self
    }

    #[must_use]
    pub fn stop_grace(&self) -> Duration {
        self.stop_grace
    }

    /// Runs Bash source to completion and returns its captured output.
    ///
    /// Equivalent to `bash -c <command>` with a clean, non-login shell: no
    /// `errexit`, no `pipefail`, `BASH_ENV` blanked. A caller that wants
    /// different semantics writes them into the command. `None` for
    /// `timeout` runs without a deadline.
    pub async fn run(
        &self,
        command: &str,
        timeout: Option<Duration>,
        working_dir: Option<&str>,
        env_vars: Option<&HashMap<String, String>>,
        cancel_token: Option<CancellationToken>,
    ) -> crate::Result<ExecResult> {
        let mut spec = ExecSpec::bash(command).no_timeout();
        if let Some(timeout) = timeout {
            spec = spec.timeout(timeout);
        }
        if let Some(dir) = working_dir {
            spec = spec.working_dir(dir);
        }
        for (key, value) in env_vars.into_iter().flatten() {
            spec = spec.env_var(key, value);
        }
        let controls = ExecControls {
            term: cancel_token,
            ..ExecControls::default()
        };
        Ok(self.run_streaming(spec, controls).await?.result)
    }

    /// Runs `spec` under fabro's policy, delivering output through
    /// `controls.sink` as it arrives.
    ///
    /// The policy fills what the spec leaves open: the stop grace, the
    /// working directory, and the text output policy. The spec's environment
    /// goes to the provider as the caller composed it. The caller's
    /// `controls.term` is the `term` stop; the provider runs the grace and
    /// the `kill` itself. Output beyond `controls.retained_output_limit`
    /// (fabro's default when unset) is drained and counted, not kept.
    pub async fn run_streaming(
        &self,
        spec: ExecSpec,
        mut controls: ExecControls,
    ) -> crate::Result<ExecStreamingResult> {
        let spec = self.apply_policy(spec);
        if controls.retained_output_limit.is_none() {
            controls.retained_output_limit = Some(DEFAULT_RETAINED_OUTPUT_BYTES);
        }
        Ok(self.exec.run_streaming(&spec, controls).await?)
    }

    /// Launches a long-lived process with bidirectional stdio.
    ///
    /// `command` is evaluated under the same non-login Bash contract before
    /// the shell replaces itself with the requested process. The returned
    /// handle terminates the process; dropping it does not.
    pub async fn spawn_stdio(
        &self,
        command: &str,
        working_dir: Option<&str>,
        env_vars: Option<&HashMap<String, String>>,
    ) -> crate::Result<StdioProcess> {
        let mut spec = SpawnSpec::bash(format!("exec {command}"));
        if let Some(dir) = working_dir.or(self.working_dir.as_deref()) {
            spec = spec.working_dir(dir);
        }
        for (key, value) in env_vars.into_iter().flatten() {
            spec = spec.env_var(key, value);
        }
        Ok(self.exec.spawn_stdio(&spec).await?)
    }

    /// Fills what a spec leaves open. The output policy has no "unset"
    /// state: the driver's default is raw, and fabro reads command output
    /// as text, so a spec still at that default gets
    /// [`OutputSanitization::StripAll`]; a caller that chose another policy
    /// keeps it. Long-lived stdio processes ([`Self::spawn_stdio`]) and PTY
    /// sessions stay raw, as the driver requires.
    fn apply_policy(&self, mut spec: ExecSpec) -> ExecSpec {
        if spec.stop_grace.is_none() {
            spec.stop_grace = Some(self.stop_grace);
        }
        if spec.working_dir.is_none() {
            spec.working_dir.clone_from(&self.working_dir);
        }
        if spec.output_sanitization == OutputSanitization::default() {
            spec.output_sanitization = OutputSanitization::StripAll;
        }
        spec
    }
}

/// The driver says how the command ended; fabro's event vocabulary has two
/// stops. A timeout is the provider's deadline (the ladder ran for it); a
/// cancelled or killed command was stopped by the caller's token, by a
/// foreign `kill`, or by a provider-side abort — it did not finish and no
/// deadline passed. `Exited`, or a provider that could not tell, is a
/// completed process; nothing asserts success here.
#[must_use]
pub fn command_termination(termination: Termination) -> CommandTermination {
    match termination {
        Termination::TimedOut => CommandTermination::TimedOut,
        Termination::Cancelled | Termination::Killed => CommandTermination::Cancelled,
        _ => CommandTermination::Exited,
    }
}

/// An exit code is only the command's own when it exited on its own. A
/// stopped command may still report the shell's `128 + signal` (143 for a
/// trapped `TERM`), which events must not present as a program result.
#[must_use]
pub fn program_exit_code(termination: Termination, exit_code: Option<i32>) -> Option<i32> {
    // `CommandTermination` is pebble's and non-exhaustive: only a command
    // that exited on its own owns its exit code.
    match command_termination(termination) {
        CommandTermination::Exited => exit_code,
        _ => None,
    }
}

/// Fabro's reading of a driver [`ExecResult`]: the event-facing numbers,
/// the redacted output tail, and the failure a non-zero exit is.
pub trait ExecResultExt {
    /// The provider's measured run time in whole milliseconds.
    fn duration_ms(&self) -> u64;

    /// The exit code when the command ended on its own; see
    /// [`program_exit_code`].
    fn program_exit_code(&self) -> Option<i32>;

    /// Redacted tails of both streams, each bounded to
    /// `max_bytes_per_stream`. `None` when both streams are empty. Terminal
    /// control sequences were already stripped by the driver under
    /// [`SandboxExec`]'s output policy.
    fn redacted_output_tail(&self, max_bytes_per_stream: usize) -> Option<ExecOutputTail>;

    /// [`Self::redacted_output_tail`] at fabro's event budget.
    fn default_redacted_output_tail(&self) -> Option<ExecOutputTail>;

    /// The failure this result is, reported under `label`. The raw output
    /// stays behind the driver's [`ExecFailure`] accessors; `Display`
    /// carries only the label and the classified metadata.
    fn into_exec_error(self, label: impl Into<String>) -> crate::Error;

    /// `Ok(self)` for a clean exit, the failure under `label` otherwise.
    fn into_result(self, label: impl Into<String>) -> crate::Result<ExecResult>;
}

impl ExecResultExt for ExecResult {
    fn duration_ms(&self) -> u64 {
        u64::try_from(self.duration.as_millis()).unwrap_or(u64::MAX)
    }

    fn program_exit_code(&self) -> Option<i32> {
        program_exit_code(self.termination, self.exit_code)
    }

    fn redacted_output_tail(&self, max_bytes_per_stream: usize) -> Option<ExecOutputTail> {
        redacted_output_tail(
            &self.stdout_lossy(),
            &self.stderr_lossy(),
            max_bytes_per_stream,
        )
    }

    fn default_redacted_output_tail(&self) -> Option<ExecOutputTail> {
        self.redacted_output_tail(DEFAULT_EXEC_OUTPUT_TAIL_BYTES)
    }

    fn into_exec_error(self, label: impl Into<String>) -> crate::Error {
        let failure = ExecFailure::new(
            label,
            self.termination,
            self.exit_code,
            self.stdout,
            self.stderr,
        )
        .with_duration(self.duration);
        crate::Error::from(sandbox_driver::Error::from(failure))
    }

    fn into_result(self, label: impl Into<String>) -> crate::Result<ExecResult> {
        if self.success() {
            Ok(self)
        } else {
            Err(self.into_exec_error(label))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use sandbox_driver::{
        BASH_ENV_VAR, OutputSink, OutputStream, SandboxProvider as _, SandboxSource, SandboxSpec,
        TransportError,
    };
    use sandbox_driver_host::HostProvider;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::{fs, time};

    use super::*;

    struct HostFixture {
        workspace: tempfile::TempDir,
        provider:  HostProvider,
        sandbox:   Arc<dyn sandbox_driver::Sandbox>,
    }

    impl HostFixture {
        async fn new() -> Self {
            let workspace = tempfile::tempdir().unwrap();
            let provider = HostProvider::new();
            let sandbox = provider
                .create(
                    &SandboxSpec::new(SandboxSource::HostDirectory)
                        .working_directory(workspace.path().display().to_string()),
                    None,
                )
                .await
                .unwrap();
            Self {
                workspace,
                provider,
                sandbox,
            }
        }

        fn exec(&self) -> SandboxExec<'_> {
            let _ = &self.provider;
            SandboxExec::new(self.sandbox.exec())
        }
    }

    async fn run(fixture: &HostFixture, command: &str) -> ExecResult {
        fixture
            .exec()
            .run(command, Some(Duration::from_secs(10)), None, None, None)
            .await
            .unwrap()
    }

    fn exec_result(
        stdout: &str,
        stderr: &str,
        exit_code: Option<i32>,
        termination: Termination,
        duration_ms: u64,
    ) -> ExecResult {
        let mut result =
            ExecResult::new(termination, exit_code, Duration::from_millis(duration_ms));
        result.stdout = stdout.as_bytes().to_vec();
        result.stderr = stderr.as_bytes().to_vec();
        result
    }

    #[tokio::test]
    async fn runs_bash_source_and_reports_exit_code_and_streams() {
        let fixture = HostFixture::new().await;
        let result = run(&fixture, "echo out; echo err >&2; exit 3").await;
        assert_eq!(result.stdout_lossy(), "out\n");
        assert_eq!(result.stderr_lossy(), "err\n");
        assert_eq!(result.exit_code, Some(3));
        assert_eq!(result.termination, Termination::Exited);
        assert!(!result.success());
        assert!(run(&fixture, "true").await.success());
    }

    #[tokio::test]
    async fn runs_bash_only_syntax_in_a_clean_non_login_shell() {
        let fixture = HostFixture::new().await;
        let result = run(
            &fixture,
            "[[ -n ${BASH_VERSION:-} ]] && shopt -q login_shell && echo login || echo nonlogin; \
             set -o | grep -E '^(errexit|pipefail)' | awk '{print $2}' | sort -u",
        )
        .await;
        assert_eq!(result.stdout_lossy(), "nonlogin\noff\n", "{result:?}");
    }

    #[tokio::test]
    async fn a_caller_supplied_bash_env_never_runs() {
        let fixture = HostFixture::new().await;
        let startup = fixture.workspace.path().join("startup.sh");
        fs::write(&startup, "echo startup-source-loaded\n")
            .await
            .unwrap();
        let env = HashMap::from([(BASH_ENV_VAR.to_string(), startup.display().to_string())]);
        let result = fixture
            .exec()
            .run(
                "echo body",
                Some(Duration::from_secs(10)),
                None,
                Some(&env),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.stdout_lossy(), "body\n");
    }

    #[tokio::test]
    async fn explicit_variables_reach_the_command_as_composed() {
        let fixture = HostFixture::new().await;
        let env = HashMap::from([
            ("FABRO_WORKER_TOKEN".to_string(), "deliberate".to_string()),
            ("MY_VAR".to_string(), "ok".to_string()),
        ]);
        let stdout = fixture
            .exec()
            .run("env", Some(Duration::from_secs(10)), None, Some(&env), None)
            .await
            .unwrap()
            .stdout_lossy();
        assert!(stdout.contains("FABRO_WORKER_TOKEN=deliberate"), "{stdout}");
        assert!(stdout.contains("MY_VAR=ok"), "{stdout}");
    }

    #[tokio::test]
    async fn timeout_runs_the_ladder_and_reports_timed_out() {
        let fixture = HostFixture::new().await;
        let started = Instant::now();
        let result = fixture
            .exec()
            .run(
                "sleep 10",
                Some(Duration::from_millis(200)),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::TimedOut);
        assert_eq!(result.program_exit_code(), None);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "sleep honours TERM, so KILL should not have been needed"
        );
    }

    #[tokio::test]
    async fn a_command_that_ignores_term_is_killed_after_the_grace_period() {
        let fixture = HostFixture::new().await;
        let started = Instant::now();
        let result = fixture
            .exec()
            .with_stop_grace(Duration::from_millis(300))
            .run(
                "trap '' TERM; sleep 10",
                Some(Duration::from_millis(100)),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::TimedOut);
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    }

    #[tokio::test]
    async fn cancellation_reports_cancelled() {
        let fixture = HostFixture::new().await;
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis(100)).await;
            cancel.cancel();
        });
        let result = fixture
            .exec()
            .run(
                "sleep 10",
                Some(Duration::from_secs(30)),
                None,
                None,
                Some(token),
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::Cancelled);
        assert_eq!(result.program_exit_code(), None);
    }

    #[tokio::test]
    async fn streaming_delivers_live_chunks_and_drains_past_the_retention_cap() {
        let fixture = HostFixture::new().await;
        let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
        let sink_seen = Arc::clone(&seen);
        let sink: OutputSink = Arc::new(move |stream, chunk| {
            let seen = Arc::clone(&sink_seen);
            Box::pin(async move {
                assert_eq!(stream, OutputStream::Stdout);
                seen.lock().unwrap().extend_from_slice(&chunk);
                Ok(())
            })
        });
        let streaming = fixture
            .exec()
            .run_streaming(
                ExecSpec::bash("for i in $(seq 1 200); do echo line-$i; done")
                    .timeout(Duration::from_secs(10)),
                ExecControls {
                    sink: Some(sink),
                    retained_output_limit: Some(64),
                    ..ExecControls::default()
                },
            )
            .await
            .unwrap();
        assert!(streaming.result.success());
        assert!(streaming.live_streaming);
        assert!(streaming.streams_separated);
        let delivered = seen.lock().unwrap().len();
        assert_eq!(streaming.stdout_capture.observed_bytes, delivered);
        assert!(streaming.stdout_capture.omitted_bytes > 0);
        assert!(streaming.result.stdout.len() <= 64);
        assert!(streaming.result.stdout.starts_with(b"line-1\n"));
        assert!(streaming.result.stdout.ends_with(b"line-200\n"));
    }

    #[tokio::test]
    async fn stdin_bytes_are_written_exactly_then_closed() {
        let fixture = HostFixture::new().await;
        let stdin = b"first line\n$(touch must-not-run)\nlast line".to_vec();
        let streaming = fixture
            .exec()
            .run_streaming(
                ExecSpec::bash("cat; test -e must-not-run && echo RAN")
                    .timeout(Duration::from_secs(10))
                    .stdin(stdin.clone()),
                ExecControls::default(),
            )
            .await
            .unwrap();
        assert_eq!(streaming.result.stdout, stdin);
    }

    #[tokio::test]
    async fn a_failing_output_sink_stops_the_command_with_an_error() {
        let fixture = HostFixture::new().await;
        let sink: OutputSink = Arc::new(|_, _| {
            Box::pin(async {
                Err(sandbox_driver::Error::Transport(TransportError::new(
                    "consumer gave up",
                )))
            })
        });
        let error = fixture
            .exec()
            .run_streaming(
                ExecSpec::bash("echo hello; sleep 5").timeout(Duration::from_secs(10)),
                ExecControls {
                    sink: Some(sink),
                    ..ExecControls::default()
                },
            )
            .await
            .map(|streaming| streaming.result.termination);
        // The driver either surfaces the sink failure or reports the command
        // cancelled by it; both keep the consumer's error visible.
        match error {
            Ok(termination) => assert_eq!(termination, Termination::Cancelled),
            Err(error) => assert!(error.to_string().contains("consumer gave up"), "{error}"),
        }
    }

    #[tokio::test]
    async fn stdio_process_round_trips_lines_and_reports_exit() {
        let fixture = HostFixture::new().await;
        let process = fixture.exec().spawn_stdio("cat", None, None).await.unwrap();
        let mut stdin = process.stdin;
        let mut stdout = BufReader::new(process.stdout);
        stdin.write_all(b"ping\n").await.unwrap();
        let mut line = String::new();
        stdout.read_line(&mut line).await.unwrap();
        assert_eq!(line, "ping\n");
        drop(stdin);
        let (termination, exit_code) = process.handle.wait().await;
        assert_eq!(termination, Termination::Exited);
        assert_eq!(exit_code, Some(0));
    }

    #[tokio::test]
    async fn stdio_process_terminates_on_request_and_keeps_a_stderr_tail() {
        let fixture = HostFixture::new().await;
        let process = fixture
            .exec()
            .spawn_stdio("sh -c 'echo diag >&2; sleep 30'", None, None)
            .await
            .unwrap();
        time::sleep(Duration::from_millis(200)).await;
        process.handle.terminate().await;
        let (termination, _) = time::timeout(Duration::from_secs(5), process.handle.wait())
            .await
            .expect("terminate ends the process");
        assert_ne!(termination, Termination::Exited);
        assert_eq!(process.stderr_tail.to_string_lossy(), "diag\n");
    }

    #[test]
    fn termination_mapping_reads_the_drivers_verdict() {
        assert_eq!(
            command_termination(Termination::TimedOut),
            CommandTermination::TimedOut
        );
        assert_eq!(
            command_termination(Termination::Cancelled),
            CommandTermination::Cancelled
        );
        assert_eq!(
            command_termination(Termination::Killed),
            CommandTermination::Cancelled
        );
        assert_eq!(
            command_termination(Termination::Exited),
            CommandTermination::Exited
        );
    }

    #[test]
    fn program_exit_code_is_the_commands_own_only_when_it_exited() {
        assert_eq!(program_exit_code(Termination::Exited, Some(3)), Some(3));
        assert_eq!(program_exit_code(Termination::TimedOut, Some(143)), None);
        assert_eq!(program_exit_code(Termination::Cancelled, Some(143)), None);
        assert_eq!(program_exit_code(Termination::Killed, Some(137)), None);
    }

    #[test]
    fn into_result_reports_a_failure_under_its_label() {
        let result = exec_result(
            "out",
            "fatal: could not read Username",
            Some(128),
            Termination::Exited,
            42,
        );
        let error = result.into_result("git push").unwrap_err();
        let Some(sandbox_driver::Error::Exec(failure)) = error.driver() else {
            panic!("expected an exec failure, got {error:?}");
        };
        assert_eq!(failure.label(), "git push");
        assert_eq!(failure.exit_code(), Some(128));
        assert_eq!(failure.duration(), Some(Duration::from_millis(42)));
        assert!(
            !error.to_string().contains("could not read Username"),
            "raw output leaked into Display: {error}"
        );

        let ok = exec_result("out", "", Some(0), Termination::Exited, 1);
        assert!(ok.into_result("true").is_ok());
    }

    #[test]
    fn output_tail_redacts_before_truncating() {
        let secret = "sk-ant-api03-xK9mZ2vL8nQ5rT1wY4bC7dF0gH3jE6pA";
        let result = exec_result(
            &format!("{} {secret} done", "context ".repeat(20)),
            "",
            Some(1),
            Termination::Exited,
            1,
        );

        let tail = result
            .redacted_output_tail(32)
            .expect("redacted output tail");
        let stdout = tail.stdout.expect("stdout tail");
        assert!(stdout.contains("REDACTED"), "{stdout}");
        assert!(!stdout.contains("F0gH3jE6pA"), "{stdout}");
        assert!(tail.stdout_truncated);
    }

    #[tokio::test]
    async fn command_output_arrives_stripped_of_terminal_control_sequences() {
        let fixture = HostFixture::new().await;
        let result = run(
            &fixture,
            "printf '\\033[31mred\\033[0m \\033]0;window-title\\007shown \\033(Bset \\033Mtwo-byte \
             \\bbackspace'",
        )
        .await;
        assert!(result.success(), "{result:?}");
        assert_eq!(result.stdout_lossy(), "red shown set two-byte backspace");

        let tail = result
            .redacted_output_tail(1024)
            .expect("redacted output tail");
        assert_eq!(
            tail.stdout.as_deref(),
            Some("red shown set two-byte backspace")
        );
    }

    #[tokio::test]
    async fn policy_strips_output_unless_the_caller_chose_another_policy() {
        let fixture = HostFixture::new().await;
        let exec = fixture.exec();
        assert_eq!(
            exec.apply_policy(ExecSpec::bash("true"))
                .output_sanitization,
            OutputSanitization::StripAll
        );
        assert_eq!(
            exec.apply_policy(
                ExecSpec::bash("true").output_sanitization(OutputSanitization::StripAnsi)
            )
            .output_sanitization,
            OutputSanitization::StripAnsi
        );
    }

    #[test]
    fn default_output_tail_serialized_budget_stays_below_40_kib() {
        let result = exec_result(
            &"o".repeat(DEFAULT_EXEC_OUTPUT_TAIL_BYTES + 128),
            &"e".repeat(DEFAULT_EXEC_OUTPUT_TAIL_BYTES + 128),
            Some(1),
            Termination::Exited,
            1,
        );

        let tail = result.default_redacted_output_tail().expect("tail present");
        assert_eq!(
            tail.stdout.as_deref().map(str::len),
            Some(DEFAULT_EXEC_OUTPUT_TAIL_BYTES)
        );
        assert_eq!(
            tail.stderr.as_deref().map(str::len),
            Some(DEFAULT_EXEC_OUTPUT_TAIL_BYTES)
        );
        assert!(tail.stdout_truncated);
        assert!(tail.stderr_truncated);
        let serialized = serde_json::to_vec(&tail).expect("serialize tail");
        assert!(
            serialized.len() < 40 * 1024,
            "tail JSON was {} bytes",
            serialized.len()
        );
    }
}
