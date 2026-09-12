use std::time::Duration;

use chrono::{DateTime, Utc};
use fabro_github::token_source::TokenSnapshot;
use sandbox_driver::{
    Git as _, GitAttempt, GitCheckoutOptions, GitFetchOptions, GitPushOptions, GitRetryError,
    GitRetryPolicy, retry_git,
};
use serde::{Deserialize, Serialize};
use tokio::time;

use crate::credentials::{self, RepoCredentials};
use crate::driver_sandbox::RunSandbox;
use crate::git_policy::{self, GitRetryReason};

/// Git command prefix that disables background maintenance.
pub const DEFAULT_EXEC_OUTPUT_TAIL_BYTES: usize = 8 * 1024;

/// Where a clone-based sandbox put its files, as persisted on the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxWorkspaceLayout {
    pub workspace_root:    String,
    pub repos_root:        String,
    /// The repository checkout and its link in the workspace, when a
    /// repository was cloned.
    pub primary_repo_path: Option<String>,
    pub primary_repo_link: Option<String>,
}

/// Information returned when a sandbox sets up git for a workflow run.
#[derive(Debug, Clone)]
pub struct GitRunInfo {
    pub base_sha:    String,
    pub run_branch:  String,
    pub base_branch: Option<String>,
}

/// Git setup requested by the workflow layer.
#[derive(Debug, Clone)]
pub enum GitSetupIntent {
    NewRun {
        run_id: String,
    },
    ForkFromCheckpoint {
        new_run_id:     String,
        source_run_id:  String,
        checkpoint_sha: String,
    },
}

/// Build a redacted `ExecOutputTail` from stdout/stderr text without
/// fabricating a synthetic `ExecResult`. Each stream is redacted, then
/// capped to its newest `max_bytes_per_stream`. Terminal control sequences
/// are not stripped here: command output reaches fabro with them already
/// removed by the driver under [`crate::exec::SandboxExec`]'s output policy.
/// Pass `""` for either stream that isn't relevant. Returns `None` when both
/// streams are empty.
#[must_use]
pub fn redacted_output_tail(
    stdout: &str,
    stderr: &str,
    max_bytes_per_stream: usize,
) -> Option<fabro_types::ExecOutputTail> {
    let (stdout, stdout_truncated) = redacted_tail(stdout, max_bytes_per_stream);
    let (stderr, stderr_truncated) = redacted_tail(stderr, max_bytes_per_stream);
    let tail = fabro_types::ExecOutputTail {
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
    };
    (!tail.is_empty()).then_some(tail)
}

fn redacted_tail(text: &str, max_bytes: usize) -> (Option<String>, bool) {
    if text.is_empty() || max_bytes == 0 {
        return (None, !text.is_empty());
    }

    let redacted = fabro_redact::redact_string(text);
    let truncated = redacted.len() > max_bytes;
    let start = if truncated {
        redacted.floor_char_boundary(redacted.len() - max_bytes)
    } else {
        0
    };
    let tail = redacted[start..].to_string();
    ((!tail.is_empty()).then_some(tail), truncated)
}

/// A regular file discovered inside a sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxFile {
    /// Provider-resolved path accepted by sandbox filesystem operations.
    pub path:          String,
    /// `/`-separated path relative to the requested traversal base.
    pub relative_path: String,
    pub size:          u64,
}

pub(crate) fn resolve_path(path: &str, working_dir: &str) -> String {
    if std::path::Path::new(path).is_absolute() {
        path.to_string()
    } else {
        join_sandbox_path(working_dir, path)
    }
}

pub(crate) fn join_sandbox_path(base: &str, relative_path: &str) -> String {
    if relative_path.is_empty() {
        return base.to_string();
    }
    if base.is_empty() {
        return relative_path.to_string();
    }
    if base == "/" {
        return format!("/{relative_path}");
    }
    format!("{}/{relative_path}", base.trim_end_matches('/'))
}

/// Creates the run branch in the sandbox's checkout through the driver's
/// git facet: a new run branches from `HEAD`, a fork from the source run's
/// checkpoint. The branch is created at that base, or moved to it when an
/// earlier attempt already created it.
pub async fn setup_git(sandbox: &RunSandbox, intent: &GitSetupIntent) -> crate::Result<GitRunInfo> {
    let git = sandbox.git()?;
    let repo = sandbox.working_directory().to_owned();
    let status = git
        .status(&repo)
        .await
        .map_err(|error| crate::Error::context("git status", error))?;
    let base_branch = status
        .current_branch
        .filter(|name| !name.is_empty() && name != "HEAD");

    let (base_sha, branch_name) = match intent {
        GitSetupIntent::NewRun { run_id } => {
            let head = status.head.ok_or_else(|| {
                crate::Error::message("the repository has no commit to branch the run from")
            })?;
            (head, format!("fabro/run/{run_id}"))
        }
        GitSetupIntent::ForkFromCheckpoint {
            new_run_id,
            source_run_id,
            checkpoint_sha,
        } => {
            fetch_source_run_ref(sandbox, source_run_id, checkpoint_sha).await?;
            (checkpoint_sha.clone(), format!("fabro/run/{new_run_id}"))
        }
    };

    git.checkout(
        &repo,
        &GitCheckoutOptions::new(&branch_name)
            .create_or_reset()
            .start_point(&base_sha),
    )
    .await
    .map_err(|error| crate::Error::context("git checkout -B", error))?;

    Ok(GitRunInfo {
        base_sha,
        run_branch: branch_name,
        base_branch,
    })
}

#[tracing::instrument(name = "git_op", skip_all, fields(op = "fetch"))]
pub(crate) async fn fetch_source_run_ref(
    sandbox: &RunSandbox,
    source_run_id: &str,
    checkpoint_sha: &str,
) -> crate::Result<()> {
    let remote_ref = format!("refs/heads/fabro/run/{source_run_id}");
    let tracking_ref = format!("refs/remotes/origin/fabro/run/{source_run_id}");
    let git = sandbox.git()?;
    let repo = sandbox.working_directory();
    let mut fetch = GitFetchOptions::default();
    fetch.remote = Some("origin".to_owned());
    fetch.refspecs = vec![format!("{remote_ref}:{tracking_ref}")];
    fetch.timeout = Some(Duration::from_secs(30));

    // The source run's checkpoint may still be landing on the remote; a
    // few short retries cover the replication.
    let mut last_error = String::new();
    for _ in 0..5 {
        match git.fetch(repo, &fetch).await {
            Ok(()) => match git.is_ancestor(repo, checkpoint_sha, &tracking_ref).await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    last_error =
                        format!("checkpoint {checkpoint_sha} is not reachable from {remote_ref}");
                }
                Err(error) => last_error = format!("git merge-base --is-ancestor: {error}"),
            },
            Err(error) => last_error = format!("git fetch source run ref: {error}"),
        }
        time::sleep(Duration::from_millis(500)).await;
    }

    Err(crate::Error::message(last_error))
}

/// One push attempt inside a retried push operation. Runtime detail only —
/// the durable serialized shape lives in `fabro-types` and the workflow layer
/// owns the conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushAttempt {
    /// 1-based attempt number within this operation.
    pub attempt:          u32,
    pub started_at:       chrono::DateTime<chrono::Utc>,
    pub success:          bool,
    /// The classifier's verdict for a failed attempt — recorded on the
    /// terminal attempt too; whether a retry actually followed is positional
    /// (every entry except the last).
    pub retry_reason:     Option<GitRetryReason>,
    /// Redacted, bounded output tail; failed attempts only.
    pub exec_output_tail: Option<fabro_types::ExecOutputTail>,
    /// The token this attempt pushed with; `None` without managed
    /// credentials.
    pub token:            Option<TokenSnapshot>,
}

/// The attempt history of one push operation.
#[derive(Debug, Clone, Default)]
pub struct PushReport {
    pub attempts: Vec<PushAttempt>,
}

/// A failed push operation: the final typed error plus the attempt history.
/// The error type stays the safety boundary for output tails.
#[derive(Debug, thiserror::Error)]
#[error("git push failed")]
pub struct PushError {
    pub report: PushReport,
    #[source]
    pub error:  crate::Error,
}

/// Pushes a refspec to origin through the driver's git facet, retried by
/// the driver under `policy` with one token for the whole operation.
/// `credentials` is the checkout's managed credentials; `None` pushes with
/// whatever the checkout already has (a checkout fabro did not clone, or a
/// clone made without a GitHub App).
#[tracing::instrument(name = "git_op", skip_all, fields(op = "push"))]
pub(crate) async fn git_push(
    sandbox: &RunSandbox,
    credentials: Option<&RepoCredentials>,
    refspec: &str,
    policy: &GitRetryPolicy,
) -> Result<PushReport, PushError> {
    let start = time::Instant::now();
    let git = match sandbox.git() {
        Ok(git) => git,
        Err(error) => {
            return Err(PushError {
                report: PushReport::default(),
                error,
            });
        }
    };
    let repo = sandbox.working_directory().to_owned();

    // One token for the whole operation. A retry after replication lag must
    // present the same token, because replication of a given token only
    // makes progress, and a fresh mint would restart that clock.
    let token = match credentials {
        Some(credentials) => {
            let resolved = match policy.max_elapsed {
                Some(max_elapsed) => {
                    match time::timeout(max_elapsed, credentials.resolve()).await {
                        Ok(resolved) => resolved,
                        Err(_) => {
                            return Err(push_deadline_error(
                                Vec::new(),
                                "while acquiring credentials",
                            ));
                        }
                    }
                }
                None => credentials.resolve().await,
            };
            match resolved {
                Ok(token) => token,
                Err(error) => {
                    return Err(PushError {
                        report: PushReport::default(),
                        error,
                    });
                }
            }
        }
        None => None,
    };
    let snapshot = token.as_ref().map(|token| token.snapshot);
    let git_credentials = token.as_ref().map(credentials::git_credentials);
    // Resolving the token spent part of the operation's budget.
    let policy = match policy.max_elapsed {
        Some(max_elapsed) => policy.max_elapsed(max_elapsed.saturating_sub(start.elapsed())),
        None => *policy,
    };

    let label = format!("git push origin {refspec}");
    let result = retry_git(
        &policy,
        git_credentials.as_ref(),
        &label,
        |_attempt, timeout| {
            let mut options = GitPushOptions::default();
            options.remote = Some("origin".to_owned());
            options.refspec = Some(refspec.to_owned());
            options.timeout = Some(timeout.unwrap_or(Duration::from_mins(1)));
            options.credentials.clone_from(&git_credentials);
            let git = &git;
            let repo = &repo;
            async move { git.push(repo, &options).await }
        },
    )
    .await;
    match result {
        Ok(report) => {
            tracing::info!(
                refspec = %refspec,
                attempts = report.attempts.len(),
                token_generation = snapshot.map(|token| token.generation),
                token_age_ms = snapshot.and_then(|token| token.age_ms()),
                "Pushed git ref to origin"
            );
            Ok(PushReport {
                attempts: push_attempts(report.attempts, Ok(()), snapshot),
            })
        }
        Err(GitRetryError { attempts, error }) => {
            let error = crate::Error::context(label, error);
            Err(PushError {
                report: PushReport {
                    attempts: push_attempts(attempts, Err(&error), snapshot),
                },
                error,
            })
        }
    }
}

/// The driver's attempt history as fabro records it. In a completed
/// operation every attempt but the last failed; in a failed one every
/// attempt failed, and the last attempt's failure is `outcome`'s error.
fn push_attempts(
    attempts: Vec<GitAttempt>,
    outcome: Result<(), &crate::Error>,
    token: Option<TokenSnapshot>,
) -> Vec<PushAttempt> {
    let last = attempts.len();
    attempts
        .into_iter()
        .enumerate()
        .map(|(index, attempt)| {
            let is_last = index + 1 == last;
            let exec_output_tail = match (attempt.failure, &outcome) {
                (Some(failure), _) => crate::Error::from(failure).default_redacted_output_tail(),
                (None, Err(error)) if is_last => error.default_redacted_output_tail(),
                (None, _) => None,
            };
            PushAttempt {
                attempt: attempt.attempt,
                started_at: DateTime::<Utc>::from(attempt.started_at),
                success: is_last && outcome.is_ok(),
                retry_reason: attempt.retry_reason.map(git_policy::recorded_reason),
                exec_output_tail,
                token,
            }
        })
        .collect()
}

fn push_deadline_error(attempts: Vec<PushAttempt>, stage: &str) -> PushError {
    PushError {
        report: PushReport { attempts },
        error:  crate::Error::message(format!("Git push retry deadline expired {stage}")),
    }
}

#[cfg(test)]
mod push_tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::Utc;
    use fabro_github::InstallationToken;
    use fabro_github::test_support::{InstallationTokenMinter, installation_token_source};
    use fabro_github::token_source::{InstallationTokenSource, REFRESH_MARGIN};
    use fabro_types::SandboxProviderKind;
    use sandbox_driver::{ExecResult, Termination};
    use sandbox_driver_testing::ScriptedSandbox;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;
    use crate::credentials::RepoCredentials;
    use crate::git_policy::{GitRetryReason, checkpoint_push_policy, publish_push_policy};

    const ORIGIN: &str = "https://github.com/fabro-testing/repo";
    const REFSPEC: &str = "refs/heads/fabro/run/01M0DH033P2XSTHAGVBHG6922F";

    fn ok_exec() -> ExecResult {
        ExecResult::new(Termination::Exited, Some(0), Duration::from_millis(5))
    }

    fn failed_exec(stderr: &str) -> ExecResult {
        let mut result = ExecResult::new(Termination::Exited, Some(128), Duration::from_millis(5));
        result.stderr = stderr.as_bytes().to_vec();
        result
    }

    fn timed_out_exec() -> ExecResult {
        let mut result = ExecResult::new(Termination::TimedOut, None, Duration::from_mins(1));
        result.stderr = b"Command timed out".to_vec();
        result
    }

    /// A run sandbox over a scripted driver double. The driver's push reads
    /// `origin`'s URL when it carries credentials and then runs `git push`;
    /// push answers come from a script, and every command is recorded.
    struct ScriptedGitSandbox {
        run:    RunSandbox,
        driver: Arc<ScriptedSandbox>,
    }

    impl ScriptedGitSandbox {
        fn new(push_results: Vec<ExecResult>) -> Self {
            let driver = Arc::new(ScriptedSandbox::with_id_and_working_dir(
                "scripted-git",
                "/workspace",
            ));
            let pushes = Mutex::new(VecDeque::from(push_results));
            driver.scripted_exec().respond_with(move |spec| {
                let script = spec.args.last().map(String::as_str).unwrap_or_default();
                if script.contains("'remote' 'get-url' 'origin'") {
                    let mut url = ok_exec();
                    url.stdout = format!("{ORIGIN}\n").into_bytes();
                    return Some(url);
                }
                assert!(
                    script.contains("'push' 'origin'"),
                    "unexpected exec: {script}"
                );
                Some(
                    pushes
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("push script exhausted"),
                )
            });
            let run = RunSandbox::new(SandboxProviderKind::LOCAL, Arc::clone(&driver) as _);
            Self { run, driver }
        }

        fn commands(&self) -> Vec<String> {
            self.driver.scripted_exec().commands()
        }

        /// The `git push` commands that ran, in order.
        fn pushes(&self) -> Vec<String> {
            self.commands()
                .into_iter()
                .filter(|command| command.contains("'push' 'origin'"))
                .collect()
        }

        fn push_count(&self) -> usize {
            self.pushes().len()
        }

        /// The token each push carried in its per-call rewrite; `None` for
        /// a push without credentials.
        fn push_tokens(&self) -> Vec<Option<String>> {
            self.pushes()
                .iter()
                .map(|push| {
                    let start = push.find("x-access-token:")? + "x-access-token:".len();
                    let end = push[start..].find('@')? + start;
                    Some(push[start..end].to_owned())
                })
                .collect()
        }
    }

    enum MintAction {
        Token(&'static str, chrono::Duration),
        Error(&'static str),
    }

    struct ScriptedMinter {
        calls:  AtomicUsize,
        script: AsyncMutex<VecDeque<MintAction>>,
    }

    impl ScriptedMinter {
        fn new(script: Vec<MintAction>) -> Arc<Self> {
            Arc::new(Self {
                calls:  AtomicUsize::new(0),
                script: AsyncMutex::new(script.into()),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl InstallationTokenMinter for ScriptedMinter {
        async fn mint(&self) -> anyhow::Result<InstallationToken> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.script.lock().await.pop_front().expect("mint script") {
                MintAction::Token(token, ttl) => Ok(InstallationToken {
                    token:      token.to_string(),
                    expires_at: Utc::now() + ttl,
                }),
                MintAction::Error(message) => Err(anyhow::anyhow!(message)),
            }
        }
    }

    struct SlowMinter;

    #[async_trait]
    impl InstallationTokenMinter for SlowMinter {
        async fn mint(&self) -> anyhow::Result<InstallationToken> {
            time::sleep(Duration::from_secs(2)).await;
            Ok(InstallationToken {
                token:      "ghs_slow".to_string(),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            })
        }
    }

    fn minting_credentials(script: Vec<MintAction>) -> (RepoCredentials, Arc<ScriptedMinter>) {
        let minter = ScriptedMinter::new(script);
        let source = installation_token_source(
            "fabro-testing/repo",
            Arc::clone(&minter) as Arc<dyn InstallationTokenMinter>,
        );
        (RepoCredentials::new(Some(source)), minter)
    }

    /// Mint the clone token first, the way `initialize` does, so the push
    /// resolves the cached token instead of minting one.
    async fn seed_clone_token(credentials: &RepoCredentials) {
        credentials
            .mint_for_clone()
            .await
            .expect("clone mint succeeds")
            .expect("managed credentials mint");
    }

    /// Regression for run `01M0DH033P2XSTHAGVBHG6922F` (the push variant of
    /// `clone_not_found_after_a_successful_mint_is_retried`): GitHub rejected
    /// pushes with 404 "Repository not found" milliseconds after a token
    /// mint. The retry must reuse the same token — replication of a given
    /// token only makes progress — and recover inside the plan's budget.
    #[tokio::test(start_paused = true)]
    async fn push_not_found_after_a_successful_mint_is_retried_with_the_same_token() {
        let (credentials, minter) = minting_credentials(vec![MintAction::Token(
            "ghs_gen1",
            chrono::Duration::minutes(60),
        )]);
        let sandbox = ScriptedGitSandbox::new(vec![
            failed_exec("remote: Repository not found."),
            failed_exec("remote: Repository not found."),
            ok_exec(),
        ]);

        let report = git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &checkpoint_push_policy(),
        )
        .await
        .expect("push should recover within the checkpoint plan");

        assert_eq!(report.attempts.len(), 3);
        assert_eq!(minter.calls(), 1, "retries must not re-mint");
        for attempt in &report.attempts {
            assert_eq!(attempt.token.expect("token recorded").generation, 1);
        }
        assert_eq!(
            report.attempts[0].retry_reason,
            Some(GitRetryReason::TokenReplication)
        );
        assert!(report.attempts[0].exec_output_tail.is_some());
        assert!(report.attempts[2].success);
        assert!(report.attempts[2].exec_output_tail.is_none());
        assert_eq!(
            sandbox.push_tokens(),
            vec![Some("ghs_gen1".to_owned()); 3],
            "every attempt presents the same token"
        );
    }

    /// The publish plan gives the terminal push a real budget: four
    /// replication-lag failures still recover on the fifth attempt.
    #[tokio::test(start_paused = true)]
    async fn publish_plan_survives_four_not_found_failures() {
        let (credentials, minter) = minting_credentials(vec![MintAction::Token(
            "ghs_gen1",
            chrono::Duration::minutes(60),
        )]);
        let sandbox = ScriptedGitSandbox::new(vec![
            failed_exec("remote: Repository not found."),
            failed_exec("remote: Repository not found."),
            failed_exec("remote: Repository not found."),
            failed_exec("remote: Repository not found."),
            ok_exec(),
        ]);

        let report = git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &publish_push_policy(),
        )
        .await
        .expect("push should recover within the publish plan");

        assert_eq!(report.attempts.len(), 5);
        assert_eq!(minter.calls(), 1);
        assert!(report.attempts[4].success);
    }

    /// Margin-boundary pinning: a token resolved just above the refresh
    /// margin stays pinned through a full retry sequence — the operation
    /// never re-resolves mid-flight, so no fresh mint can restart the
    /// replication clock.
    #[tokio::test(start_paused = true)]
    async fn token_resolved_just_above_the_margin_stays_pinned_through_retries() {
        let ttl = REFRESH_MARGIN + Duration::from_secs(5);
        let (credentials, minter) = minting_credentials(vec![
            MintAction::Token("ghs_gen1", chrono::Duration::from_std(ttl).unwrap()),
            MintAction::Token("ghs_gen2", chrono::Duration::minutes(60)),
        ]);
        let sandbox = ScriptedGitSandbox::new(vec![
            failed_exec("remote: Repository not found."),
            failed_exec("remote: Repository not found."),
            ok_exec(),
        ]);

        let report = git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &checkpoint_push_policy(),
        )
        .await
        .expect("push recovers");

        assert_eq!(minter.calls(), 1, "the operation never re-resolves");
        assert_eq!(sandbox.push_tokens(), vec![Some("ghs_gen1".to_owned()); 3]);
        assert!(
            report
                .attempts
                .iter()
                .all(|attempt| attempt.token.map(|token| token.generation) == Some(1))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn static_credential_auth_failure_fails_fast() {
        let credentials =
            RepoCredentials::new(Some(InstallationTokenSource::pat("ghp_static".to_owned())));
        let sandbox = ScriptedGitSandbox::new(vec![failed_exec("remote: Repository not found.")]);

        let push_error = git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &publish_push_policy(),
        )
        .await
        .expect_err("static credentials cannot become valid by waiting");

        assert_eq!(push_error.report.attempts.len(), 1);
        assert_eq!(push_error.report.attempts[0].retry_reason, None);
        assert_eq!(
            push_error.report.attempts[0]
                .token
                .map(|token| token.generation),
            Some(0)
        );
        assert_eq!(sandbox.push_tokens(), vec![Some("ghp_static".to_owned())]);
    }

    /// A refresh that fails while the cached token is still valid pushes
    /// with the cached token.
    #[tokio::test(start_paused = true)]
    async fn mint_failure_falls_back_to_the_cached_token() {
        // The clone token is already inside the refresh margin, so the
        // push's resolve tries to re-mint and fails.
        let (credentials, minter) = minting_credentials(vec![
            MintAction::Token(
                "ghs_clone",
                chrono::Duration::from_std(
                    REFRESH_MARGIN
                        .checked_sub(Duration::from_mins(1))
                        .expect("the margin is longer than a minute"),
                )
                .unwrap(),
            ),
            MintAction::Error("github unavailable"),
        ]);
        seed_clone_token(&credentials).await;
        let sandbox = ScriptedGitSandbox::new(vec![ok_exec()]);

        let report = git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &checkpoint_push_policy(),
        )
        .await
        .expect("the cached token still pushes");

        assert_eq!(minter.calls(), 2, "the push tried to refresh once");
        assert_eq!(sandbox.push_tokens(), vec![Some("ghs_clone".to_owned())]);
        assert_eq!(
            report.attempts[0].token.map(|token| token.generation),
            Some(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn mint_failure_without_a_cached_token_fails_before_any_push() {
        let (credentials, minter) =
            minting_credentials(vec![MintAction::Error("github unavailable")]);
        let sandbox = ScriptedGitSandbox::new(vec![]);

        let push_error = git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &checkpoint_push_policy(),
        )
        .await
        .expect_err("no token to push with");

        assert!(push_error.report.attempts.is_empty());
        assert_eq!(sandbox.push_count(), 0);
        assert_eq!(minter.calls(), 1);
        assert!(
            push_error
                .error
                .to_string()
                .contains("Failed to refresh GitHub App credentials"),
            "{}",
            push_error.error
        );
    }

    /// The token reaches git through the driver's per-call rewrite and never
    /// through the remote URL.
    #[tokio::test(start_paused = true)]
    async fn credentials_travel_per_call_and_never_touch_the_remote() {
        let (credentials, _minter) = minting_credentials(vec![MintAction::Token(
            "ghs_gen1",
            chrono::Duration::minutes(60),
        )]);
        let sandbox = ScriptedGitSandbox::new(vec![ok_exec()]);

        git_push(
            &sandbox.run,
            Some(&credentials),
            REFSPEC,
            &checkpoint_push_policy(),
        )
        .await
        .expect("push succeeds");

        let commands = sandbox.commands();
        assert!(
            commands.iter().all(|command| !command.contains("set-url")),
            "{commands:#?}"
        );
        let push = &sandbox.pushes()[0];
        assert!(
            push.contains("insteadOf=https://github.com/fabro-testing/repo"),
            "{push}"
        );
        assert!(
            push.contains("'push' 'origin' 'refs/heads/fabro/run/"),
            "{push}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn push_without_managed_credentials_reports_no_token() {
        let sandbox = ScriptedGitSandbox::new(vec![ok_exec()]);

        let report = git_push(&sandbox.run, None, REFSPEC, &checkpoint_push_policy())
            .await
            .expect("push succeeds");

        assert_eq!(report.attempts.len(), 1);
        assert_eq!(report.attempts[0].token, None);
        assert_eq!(sandbox.push_tokens(), vec![None]);
    }

    #[tokio::test(start_paused = true)]
    async fn unauthenticated_auth_failure_is_permanent() {
        let sandbox = ScriptedGitSandbox::new(vec![failed_exec(
            "fatal: Authentication failed for 'https://github.com/fabro-testing/repo'",
        )]);

        let push_error = git_push(&sandbox.run, None, REFSPEC, &publish_push_policy())
            .await
            .expect_err("no credentials to wait on");

        assert_eq!(push_error.report.attempts.len(), 1);
        assert_eq!(push_error.report.attempts[0].retry_reason, None);
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_push_is_not_retried_while_the_remote_process_may_still_run() {
        let sandbox = ScriptedGitSandbox::new(vec![timed_out_exec()]);

        let push_error = git_push(&sandbox.run, None, REFSPEC, &publish_push_policy())
            .await
            .expect_err("an unconfirmed timeout must fail without another push");

        assert_eq!(sandbox.push_count(), 1);
        assert_eq!(push_error.report.attempts.len(), 1);
        assert_eq!(push_error.report.attempts[0].retry_reason, None);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_deadline_includes_credential_resolution() {
        let source = installation_token_source("fabro-testing/repo", Arc::new(SlowMinter));
        let credentials = RepoCredentials::new(Some(source));
        let sandbox = ScriptedGitSandbox::new(vec![]);
        let policy = checkpoint_push_policy().max_elapsed(Duration::from_secs(1));

        let push_error = git_push(&sandbox.run, Some(&credentials), REFSPEC, &policy)
            .await
            .expect_err("credential resolution must stop at the operation deadline");

        assert!(push_error.report.attempts.is_empty());
        assert_eq!(sandbox.push_count(), 0);
        assert!(push_error.error.to_string().contains("deadline expired"));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_retry_deadline_does_not_launch_a_zero_timeout_push() {
        let sandbox = ScriptedGitSandbox::new(vec![]);
        let policy = checkpoint_push_policy().max_elapsed(Duration::ZERO);

        let push_error = git_push(&sandbox.run, None, REFSPEC, &policy)
            .await
            .expect_err("an expired operation must stop before exec");

        assert!(push_error.report.attempts.is_empty());
        assert_eq!(sandbox.push_count(), 0);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sandbox_tracing_events_do_not_log_raw_command_or_stdin_fields() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut failures = Vec::new();
        scan_for_command_tracing(&root, &mut failures);
        assert!(
            failures.is_empty(),
            "raw command/cmd/stdin tracing fields found:\n{}",
            failures.join("\n")
        );
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "unit test performs a small synchronous source scan of local Rust files"
    )]
    fn scan_for_command_tracing(path: &std::path::Path, failures: &mut Vec<String>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                scan_for_command_tracing(&path, failures);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).unwrap();
            for macro_name in [
                "tracing::trace!",
                "tracing::debug!",
                "tracing::info!",
                "tracing::warn!",
                "tracing::error!",
                "trace!",
                "debug!",
                "info!",
                "warn!",
                "error!",
            ] {
                let mut rest = source.as_str();
                while let Some(idx) = rest.find(macro_name) {
                    let start = source.len() - rest.len() + idx;
                    if start > 0 && source.as_bytes()[start - 1] == b'"' {
                        rest = &source[start + macro_name.len()..];
                        continue;
                    }
                    let Some(call) = tracing_call(&source[start..]) else {
                        break;
                    };
                    if call.contains("command,")
                        || call.contains("command =")
                        || call.contains("cmd,")
                        || call.contains("cmd =")
                        || call.contains("stdin,")
                        || call.contains("stdin =")
                    {
                        failures.push(format!(
                            "{}: {}",
                            path.display(),
                            call.lines().next().unwrap_or(call)
                        ));
                    }
                    rest = &source[start + call.len()..];
                }
            }
        }
    }

    fn tracing_call(source: &str) -> Option<&str> {
        let open = source.find('(')?;
        let mut depth = 0usize;
        for (idx, ch) in source.char_indices().skip(open) {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(&source[..=idx]);
                    }
                }
                _ => {}
            }
        }
        None
    }
}
