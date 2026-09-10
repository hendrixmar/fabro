//! Retry for git operations against GitHub from clone-based sandboxes.
//!
//! Clone-based providers can mint a GitHub App installation token and use it
//! immediately. GitHub can reject that first operation before the token is
//! available to the git endpoint. On a private repository, the rejection can
//! arrive as `Repository not found.` or an authentication failure.
//!
//! Only a token minted recently makes those messages safe to retry. Static
//! PATs and pre-minted installation tokens fail fast; a mature App token can
//! still hit a service-side blip that presents the same surface, so it
//! retries as transient infrastructure.
//!
//! Retries reuse the same token on purpose. Replication of a given token only
//! makes progress, so each attempt strictly improves the odds, while
//! re-minting would restart the replication clock.

use std::future::Future;
use std::time::Duration;

use chrono::Utc;
use fabro_github::token_source::TokenSnapshot;
#[cfg(test)]
use fabro_github::token_source::{REFRESH_MARGIN, TokenProvenance};
use fabro_types::SandboxProviderKind;
pub use fabro_types::run_event::GitPushRetryReason as GitRetryReason;
use fabro_util::backoff::BackoffPolicy;
use tokio::time;

/// How long after its mint a token is presumed to still be replicating to
/// GitHub's git endpoints. Matches the observed scale of the lag (seconds,
/// occasionally tens of seconds).
pub(crate) const REPLICATION_HORIZON: Duration = Duration::from_mins(1);

/// What a git failure message tells us about retry safety.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GitMessageClass {
    Retry(GitRetryReason),
    Permanent,
    Unknown,
}

impl GitMessageClass {
    pub(crate) fn retry_reason(self) -> Option<GitRetryReason> {
        match self {
            Self::Retry(reason) => Some(reason),
            Self::Permanent | Self::Unknown => None,
        }
    }
}

/// What the operation's credentials say about retrying auth-shaped failures.
///
/// Derived from the [`TokenSnapshot`] of the token embedded for the attempt,
/// so classification reads provenance as data instead of threading booleans
/// through call stacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialContext {
    /// An installation token younger than [`REPLICATION_HORIZON`] — a 404 or
    /// auth failure is likely replication lag; retry with the same token.
    FreshApp,
    /// An installation token older than the horizon. A 404 with it is
    /// indistinguishable from a service-side blip at this layer, so it stays
    /// transient rather than proving access loss.
    MatureApp,
    /// A PAT or pre-minted token — it cannot become valid by waiting.
    Static,
    /// No credentials at all.
    None,
}

impl CredentialContext {
    #[must_use]
    pub fn from_snapshot(snapshot: Option<&TokenSnapshot>) -> Self {
        match snapshot {
            None => Self::None,
            Some(snapshot) => match snapshot.age_at(Utc::now()) {
                None => Self::Static,
                Some(age) if age < REPLICATION_HORIZON => Self::FreshApp,
                Some(_) => Self::MatureApp,
            },
        }
    }
}

/// Message fragments that mean the operation failed on infrastructure.
///
/// These are safe to retry whether or not the operation was authenticated.
const TRANSIENT_HINTS: &[&str] = &[
    "could not resolve host",
    "temporary failure in name resolution",
    "connection refused",
    "connection reset",
    "connection timed out",
    "timed out",
    "network is unreachable",
    "no route to host",
    "tls handshake",
    "early eof",
    "rpc failed",
    "unexpected disconnect",
    "the remote end hung up unexpectedly",
    "index-pack failed",
    "service unavailable",
    "gateway timeout",
    "too many requests",
    "rate limit",
];

/// Message fragments GitHub uses when a token is not yet visible.
///
/// Only meaningful when the operation carried credentials. The same lag
/// surfaces as 404 or as an auth failure depending on which endpoint answers
/// first.
const TOKEN_REPLICATION_HINTS: &[&str] = &[
    "repository not found",
    "authentication failed",
    "invalid username or password",
    "bad credentials",
    // git CLI over HTTP.
    "the requested url returned error: 401",
    "the requested url returned error: 403",
    "the requested url returned error: 404",
    // libgit2 (the run-metadata writer pushes through git2).
    "unexpected http status code: 401",
    "unexpected http status code: 403",
    "unexpected http status code: 404",
];

/// Whether a failure message has the 404/auth-failure shape GitHub produces
/// for both token-replication lag and a drifted or missing embedded token.
pub(crate) fn matches_auth_failure_hints(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    TOKEN_REPLICATION_HINTS
        .iter()
        .any(|hint| lower.contains(hint))
}

pub(crate) fn output_matches_auth_failure_hints(stderr: &str, stdout: &str) -> bool {
    matches_auth_failure_hints(stderr) || matches_auth_failure_hints(stdout)
}

/// Classify a failed git operation by its rendered message.
///
/// `cred` gates the reading of 404/auth-failure messages: a fresh App token
/// retries as replication lag, a mature one as transient infrastructure, and
/// a static credential (or none) fails fast because waiting cannot make it
/// valid.
pub(crate) fn classify_message(message: &str, cred: CredentialContext) -> GitMessageClass {
    let lower = message.to_ascii_lowercase();

    if TRANSIENT_HINTS.iter().any(|hint| lower.contains(hint)) {
        return GitMessageClass::Retry(GitRetryReason::TransientInfra);
    }
    if TOKEN_REPLICATION_HINTS
        .iter()
        .any(|hint| lower.contains(hint))
    {
        return match cred {
            CredentialContext::FreshApp => GitMessageClass::Retry(GitRetryReason::TokenReplication),
            CredentialContext::MatureApp => GitMessageClass::Retry(GitRetryReason::TransientInfra),
            CredentialContext::Static | CredentialContext::None => GitMessageClass::Permanent,
        };
    }
    let permanent = lower.contains("could not read username")
        || lower.contains("terminal prompts disabled")
        || lower.contains("permission denied")
        || (lower.contains("permission to") && lower.contains("denied"))
        || (lower.contains("destination path") && lower.contains("already exists"))
        || (lower.contains("remote branch") && lower.contains("not found"));
    if permanent {
        return GitMessageClass::Permanent;
    }
    GitMessageClass::Unknown
}

pub(crate) fn classify_output(
    stderr: &str,
    stdout: &str,
    cred: CredentialContext,
) -> GitMessageClass {
    let by_stderr = classify_message(stderr, cred);
    if by_stderr == GitMessageClass::Unknown {
        classify_message(stdout, cred)
    } else {
        by_stderr
    }
}

/// Classify a rendered git failure message, returning the retry reason when
/// the failure is transient for these credentials. `None` means the failure
/// is permanent or unrecognized.
#[must_use]
pub fn classify_failure(message: &str, cred: CredentialContext) -> Option<GitRetryReason> {
    classify_message(message, cred).retry_reason()
}

/// Backoff between attempts: 3s, then 9s.
///
/// GitHub's guidance for token replication is to wait a few seconds and retry
/// with the same token. Sub-second delays land inside the same replication
/// window and spend an attempt for nothing.
fn replication_backoff() -> BackoffPolicy {
    BackoffPolicy {
        initial_delay: Duration::from_secs(3),
        factor:        3.0,
        max_delay:     Duration::from_secs(10),
        jitter:        false,
    }
}

/// Attempt and time bounds for one retried git operation.
///
/// All bounds are optional so existing behaviors are expressible unchanged.
/// The effective deadline is the minimum of the bounds that are present
/// (`start + max_elapsed`, `outer_deadline`); each attempt runs with
/// `min(per_attempt_timeout, remaining)` over the caps that are present, and
/// no attempt or backoff starts past the effective deadline.
#[derive(Debug, Clone)]
pub struct RetryPlan {
    /// Total attempts, including the first.
    pub max_attempts:        u32,
    pub backoff:             BackoffPolicy,
    /// Wall clock for this whole operation.
    pub max_elapsed:         Option<Duration>,
    /// Cap for any single attempt.
    pub per_attempt_timeout: Option<Duration>,
    /// Caller-supplied absolute bound.
    pub outer_deadline:      Option<time::Instant>,
}

impl RetryPlan {
    /// Host-side repository probes use the same attempt count and pacing as
    /// clone operations against a freshly minted token.
    #[must_use]
    pub fn repository_probe() -> Self {
        Self::clone_default(None)
    }

    /// The clone policy both providers already trust: 3 attempts, 3s/9s
    /// backoff, no plan-level bounds. Docker supplies its existing absolute
    /// five-minute deadline through `outer_deadline`; Daytona supplies none.
    #[must_use]
    pub fn clone_default(outer_deadline: Option<time::Instant>) -> Self {
        Self {
            max_attempts: 3,
            backoff: replication_backoff(),
            max_elapsed: None,
            per_attempt_timeout: None,
            outer_deadline,
        }
    }

    /// Checkpoint pushes stay cheap: the next checkpoint re-pushes the same
    /// branch anyway. Worst case ~90 seconds of wall clock.
    #[must_use]
    pub fn checkpoint_push() -> Self {
        Self {
            max_attempts:        3,
            backoff:             replication_backoff(),
            max_elapsed:         Some(Duration::from_secs(90)),
            per_attempt_timeout: Some(Duration::from_mins(1)),
            outer_deadline:      None,
        }
    }

    /// The terminal publish push guards the whole run's value, so it gets a
    /// real budget: 5 attempts with growing backoff (~3s/10s/33s/60s),
    /// bounded at 4 minutes of wall clock. The 4-minute bound must stay
    /// under the token source's `REFRESH_MARGIN` (see the margin-invariant
    /// test) so a pinned token always outlives the operation.
    #[must_use]
    pub fn publish_push() -> Self {
        Self {
            max_attempts:        5,
            backoff:             BackoffPolicy {
                initial_delay: Duration::from_secs(3),
                factor:        10.0 / 3.0,
                max_delay:     Duration::from_mins(1),
                jitter:        false,
            },
            max_elapsed:         Some(Duration::from_mins(4)),
            per_attempt_timeout: Some(Duration::from_mins(1)),
            outer_deadline:      None,
        }
    }

    /// The absolute deadline this operation must finish by, if any bound is
    /// present.
    pub(crate) fn effective_deadline(&self, start: time::Instant) -> Option<time::Instant> {
        let elapsed_deadline = self.max_elapsed.map(|max| start + max);
        match (elapsed_deadline, self.outer_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// Time cap for an attempt starting now: the per-attempt cap bounded by
    /// the time remaining before the effective deadline.
    pub(crate) fn attempt_timeout(&self, deadline: Option<time::Instant>) -> Option<Duration> {
        let remaining = deadline.map(|d| d.saturating_duration_since(time::Instant::now()));
        match (self.per_attempt_timeout, remaining) {
            (Some(cap), Some(remaining)) => Some(cap.min(remaining)),
            (Some(cap), None) => Some(cap),
            (None, remaining) => remaining,
        }
    }

    pub(crate) fn retry_delay(
        &self,
        attempt_number: u32,
        deadline: Option<time::Instant>,
    ) -> Option<Duration> {
        let delay = self.backoff.delay_for_attempt(attempt_number);
        if deadline.is_some_and(|deadline| {
            delay >= deadline.saturating_duration_since(time::Instant::now())
        }) {
            None
        } else {
            Some(delay)
        }
    }
}

/// Run a git operation, repeating it while the failure looks transient.
///
/// `attempt` receives the 1-based attempt number. `classify` decides whether
/// an error is worth repeating; `None` returns it to the caller untouched.
/// A retry starts only when its backoff fits before the plan's effective
/// deadline. The final error is returned as-is.
pub async fn retry_git_operation<T, E, Attempt, Fut, Classify>(
    provider: SandboxProviderKind,
    op: &str,
    plan: &RetryPlan,
    mut attempt: Attempt,
    classify: Classify,
) -> Result<T, E>
where
    Attempt: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    Classify: Fn(&E) -> Option<GitRetryReason>,
{
    let deadline = plan.effective_deadline(time::Instant::now());

    for attempt_number in 1..plan.max_attempts.max(1) {
        match attempt(attempt_number).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let Some(reason) = classify(&err) else {
                    return Err(err);
                };
                let Some(delay) = plan.retry_delay(attempt_number, deadline) else {
                    return Err(err);
                };
                // The failure text can carry git stderr, so log the category
                // rather than the message. The caller still reports the full
                // error if the attempts run out.
                tracing::warn!(
                    provider = %provider,
                    op,
                    attempt = attempt_number,
                    max_attempts = plan.max_attempts,
                    reason = %reason,
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "Git operation failed, retrying"
                );
                time::sleep(delay).await;
            }
        }
    }

    attempt(plan.max_attempts.max(1)).await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Records the attempt numbers a closure was called with.
    #[derive(Default)]
    struct Attempts(Mutex<Vec<u32>>);

    impl Attempts {
        fn record(&self, attempt: u32) {
            self.0.lock().expect("attempt log mutex").push(attempt);
        }

        fn recorded(&self) -> Vec<u32> {
            self.0.lock().expect("attempt log mutex").clone()
        }
    }

    /// A classifier that treats every failure as worth repeating.
    const ALWAYS_RETRY: fn(&String) -> Option<GitRetryReason> =
        |_| Some(GitRetryReason::TokenReplication);

    fn fresh_snapshot(age: Duration, ttl: Duration) -> TokenSnapshot {
        let now = Utc::now();
        TokenSnapshot {
            generation: 1,
            provenance: TokenProvenance::Minted {
                minted_at:  now - chrono::Duration::from_std(age).unwrap(),
                expires_at: now + chrono::Duration::from_std(ttl).unwrap(),
            },
        }
    }

    #[test]
    fn credential_context_reads_token_age_from_provenance() {
        assert_eq!(
            CredentialContext::from_snapshot(None),
            CredentialContext::None
        );
        assert_eq!(
            CredentialContext::from_snapshot(Some(&TokenSnapshot {
                generation: 0,
                provenance: TokenProvenance::Static,
            })),
            CredentialContext::Static
        );
        assert_eq!(
            CredentialContext::from_snapshot(Some(&fresh_snapshot(
                Duration::from_secs(5),
                Duration::from_hours(1)
            ))),
            CredentialContext::FreshApp
        );
        assert_eq!(
            CredentialContext::from_snapshot(Some(&fresh_snapshot(
                Duration::from_mins(2),
                Duration::from_hours(1)
            ))),
            CredentialContext::MatureApp
        );
    }

    #[test]
    fn private_repo_not_found_with_a_fresh_token_is_a_replication_lag() {
        assert_eq!(
            classify_message(
                "repository not found: Repository not found.",
                CredentialContext::FreshApp
            ),
            GitMessageClass::Retry(GitRetryReason::TokenReplication)
        );
    }

    #[test]
    fn not_found_with_a_mature_token_is_transient_not_permanent() {
        // A service-side blip is indistinguishable from access loss at this
        // layer, so a mature-App 404 stays retryable.
        assert_eq!(
            classify_message(
                "repository not found: Repository not found.",
                CredentialContext::MatureApp
            ),
            GitMessageClass::Retry(GitRetryReason::TransientInfra)
        );
    }

    #[test]
    fn not_found_with_static_or_no_credentials_is_permanent() {
        for cred in [CredentialContext::Static, CredentialContext::None] {
            assert_eq!(
                classify_message("repository not found: Repository not found.", cred),
                GitMessageClass::Permanent,
                "{cred:?} cannot become valid by waiting"
            );
        }
    }

    #[test]
    fn auth_failure_classification_follows_the_credential_context() {
        let message = "fatal: Authentication failed for 'https://github.com/owner/repo'";
        assert_eq!(
            classify_message(message, CredentialContext::FreshApp),
            GitMessageClass::Retry(GitRetryReason::TokenReplication)
        );
        assert_eq!(
            classify_message(message, CredentialContext::MatureApp),
            GitMessageClass::Retry(GitRetryReason::TransientInfra)
        );
        assert_eq!(
            classify_message(message, CredentialContext::Static),
            GitMessageClass::Permanent
        );
    }

    #[test]
    fn infra_failures_retry_without_credentials() {
        for message in [
            "fatal: unable to access: Could not resolve host: github.com",
            "error: RPC failed; curl 56 recv failure",
            "fatal: early EOF",
            "Operation timed out",
        ] {
            assert_eq!(
                classify_message(message, CredentialContext::None),
                GitMessageClass::Retry(GitRetryReason::TransientInfra),
                "expected {message:?} to be transient"
            );
        }
    }

    #[test]
    fn genuine_failures_are_not_retried() {
        for message in [
            "fatal: could not read Username for 'https://github.com'",
            "remote: Permission to owner/repo.git denied",
            "fatal: destination path 'repo' already exists",
        ] {
            assert_eq!(
                classify_message(message, CredentialContext::FreshApp),
                GitMessageClass::Permanent,
                "expected {message:?} to fail fast"
            );
        }
    }

    #[test]
    fn unrecognized_failures_remain_unknown() {
        assert_eq!(
            classify_message(
                "git operation stopped for an unexpected reason",
                CredentialContext::FreshApp
            ),
            GitMessageClass::Unknown
        );
    }

    #[test]
    fn backoff_waits_seconds_not_milliseconds() {
        let plan = RetryPlan::clone_default(None);
        assert_eq!(plan.backoff.delay_for_attempt(1), Duration::from_secs(3));
        assert_eq!(plan.backoff.delay_for_attempt(2), Duration::from_secs(9));
    }

    #[test]
    fn publish_backoff_grows_toward_a_one_minute_cap() {
        let plan = RetryPlan::publish_push();
        assert_eq!(plan.backoff.delay_for_attempt(1), Duration::from_secs(3));
        assert_eq!(plan.backoff.delay_for_attempt(2), Duration::from_secs(10));
        assert!(plan.backoff.delay_for_attempt(3) < Duration::from_secs(35));
        assert_eq!(plan.backoff.delay_for_attempt(4), Duration::from_mins(1));
    }

    /// `REFRESH_MARGIN` must exceed every push plan's `max_elapsed`: a push
    /// pins the token of its single successful resolve, and any token the
    /// source returns has at least the margin of validity left, so the pinned
    /// token must outlive the whole operation.
    #[test]
    fn refresh_margin_exceeds_every_push_plan_elapsed_bound() {
        for plan in [RetryPlan::checkpoint_push(), RetryPlan::publish_push()] {
            let max_elapsed = plan.max_elapsed.expect("push plans bound elapsed time");
            assert!(
                REFRESH_MARGIN > max_elapsed,
                "margin invariant violated: {max_elapsed:?}"
            );
        }
    }

    #[test]
    fn effective_deadline_takes_the_minimum_of_present_bounds() {
        let start = time::Instant::now();
        let outer = start + Duration::from_secs(30);

        let unbounded = RetryPlan::clone_default(None);
        assert_eq!(unbounded.effective_deadline(start), None);

        let outer_only = RetryPlan::clone_default(Some(outer));
        assert_eq!(outer_only.effective_deadline(start), Some(outer));

        let mut both = RetryPlan::checkpoint_push();
        both.outer_deadline = Some(outer);
        assert_eq!(both.effective_deadline(start), Some(outer));

        both.outer_deadline = Some(start + Duration::from_mins(10));
        assert_eq!(
            both.effective_deadline(start),
            Some(start + Duration::from_secs(90))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_timeout_is_capped_by_the_remaining_deadline() {
        let plan = RetryPlan::checkpoint_push();
        let deadline = Some(time::Instant::now() + Duration::from_secs(20));
        assert_eq!(
            plan.attempt_timeout(deadline),
            Some(Duration::from_secs(20))
        );
        assert_eq!(plan.attempt_timeout(None), Some(Duration::from_mins(1)));

        let unbounded = RetryPlan::clone_default(None);
        assert_eq!(unbounded.attempt_timeout(None), None);
    }

    #[tokio::test(start_paused = true)]
    async fn first_success_runs_one_attempt() {
        let attempts = Attempts::default();

        let result = retry_git_operation(
            SandboxProviderKind::Docker,
            "clone",
            &RetryPlan::clone_default(None),
            |attempt| {
                attempts.record(attempt);
                async move { Ok::<_, String>(attempt) }
            },
            ALWAYS_RETRY,
        )
        .await;

        assert_eq!(result, Ok(1));
        assert_eq!(attempts.recorded(), vec![1]);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_a_later_attempt_succeeds() {
        let attempts = Attempts::default();

        let result = retry_git_operation(
            SandboxProviderKind::Docker,
            "clone",
            &RetryPlan::clone_default(None),
            |attempt| {
                attempts.record(attempt);
                async move {
                    if attempt < 3 {
                        Err("Repository not found.".to_string())
                    } else {
                        Ok(attempt)
                    }
                }
            },
            ALWAYS_RETRY,
        )
        .await;

        assert_eq!(result, Ok(3));
        assert_eq!(attempts.recorded(), vec![1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_attempts_return_the_final_error() {
        let attempts = Attempts::default();

        let result = retry_git_operation(
            SandboxProviderKind::Docker,
            "clone",
            &RetryPlan::clone_default(None),
            |attempt| {
                attempts.record(attempt);
                async move { Err::<(), _>(format!("Repository not found. (attempt {attempt})")) }
            },
            ALWAYS_RETRY,
        )
        .await;

        assert_eq!(
            result,
            Err("Repository not found. (attempt 3)".to_string()),
            "the caller should see the last failure, not the first"
        );
        assert_eq!(attempts.recorded(), vec![1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn unretryable_failure_stops_immediately() {
        let attempts = Attempts::default();

        let result = retry_git_operation(
            SandboxProviderKind::Docker,
            "clone",
            &RetryPlan::clone_default(None),
            |attempt| {
                attempts.record(attempt);
                async move { Err::<(), _>("permission denied".to_string()) }
            },
            |_: &String| None,
        )
        .await;

        assert_eq!(result, Err("permission denied".to_string()));
        assert_eq!(
            attempts.recorded(),
            vec![1],
            "a deterministic failure should not wait out the backoff"
        );
    }

    /// Docker clone parity: the caller's absolute deadline stops retries when
    /// the backoff no longer fits before it.
    #[tokio::test(start_paused = true)]
    async fn outer_deadline_stops_retry_when_backoff_does_not_fit() {
        let attempts = Attempts::default();
        let deadline = time::Instant::now() + Duration::from_secs(2);

        let result = retry_git_operation(
            SandboxProviderKind::Docker,
            "clone",
            &RetryPlan::clone_default(Some(deadline)),
            |attempt| {
                attempts.record(attempt);
                async move { Err::<(), _>("temporary failure".to_string()) }
            },
            ALWAYS_RETRY,
        )
        .await;

        assert_eq!(result, Err("temporary failure".to_string()));
        assert_eq!(attempts.recorded(), vec![1]);
        assert_eq!(time::Instant::now() + Duration::from_secs(2), deadline);
    }

    /// Daytona clone parity: with no bounds at all, attempts are limited only
    /// by `max_attempts` and backoff.
    #[tokio::test(start_paused = true)]
    async fn unbounded_plan_runs_all_attempts() {
        let attempts = Attempts::default();

        let result = retry_git_operation(
            SandboxProviderKind::Daytona,
            "clone",
            &RetryPlan::clone_default(None),
            |attempt| {
                attempts.record(attempt);
                async move { Err::<(), _>("temporary failure".to_string()) }
            },
            |_: &String| Some(GitRetryReason::TransientInfra),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.recorded(), vec![1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn max_elapsed_stops_retry_when_backoff_does_not_fit() {
        let attempts = Attempts::default();
        let plan = RetryPlan {
            max_attempts:        5,
            backoff:             replication_backoff(),
            max_elapsed:         Some(Duration::from_secs(4)),
            per_attempt_timeout: None,
            outer_deadline:      None,
        };

        let result = retry_git_operation(
            SandboxProviderKind::Docker,
            "push",
            &plan,
            |attempt| {
                attempts.record(attempt);
                async move { Err::<(), _>("temporary failure".to_string()) }
            },
            ALWAYS_RETRY,
        )
        .await;

        assert!(result.is_err());
        // Attempt 1 fails instantly, 3s backoff fits inside 4s, attempt 2
        // fails, and the 9s backoff no longer fits.
        assert_eq!(attempts.recorded(), vec![1, 2]);
    }
}
