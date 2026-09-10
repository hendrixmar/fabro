//! Shared push-credential state for clone-based sandbox providers.
//!
//! Docker and Daytona embed GitHub credentials into the cloned repository's
//! `origin` remote and refresh them before pushes. Both providers hold this
//! state so the compare → `set-url` → record sequence, the generation
//! tracking, and the refresh-error logging behave identically across
//! providers. The token cache itself sits below the providers, in
//! [`fabro_github::token_source::InstallationTokenSource`].

use std::future::Future;
use std::sync::Arc;

use fabro_github::GitHubCredentials;
use fabro_github::token_source::{InstallationTokenSource, ResolvedToken, TokenSnapshot};
use fabro_redact::DisplaySafeUrl;
pub use fabro_types::run_event::GitCredentialRefreshError as RefreshErrorKind;
use tokio::sync::{Mutex, MutexGuard};

use crate::redact;
use crate::sandbox::{RefreshOutcome, RemoteCredentialAction};

/// Build the shared installation-token source for a clone-based sandbox.
///
/// Returns `None` when there are no managed credentials or no GitHub origin
/// to scope them to. Minted tokens carry the same `contents: write`
/// permission the clone token uses.
pub(crate) fn build_token_source(
    github_app: Option<&GitHubCredentials>,
    clone_origin_url: Option<&str>,
) -> crate::Result<Option<Arc<InstallationTokenSource>>> {
    let Some(creds) = github_app else {
        return Ok(None);
    };
    let Some(origin_url) = clone_origin_url.filter(|url| !url.trim().is_empty()) else {
        return Ok(None);
    };
    let normalized = fabro_github::normalize_repo_origin_url(origin_url);
    let Ok((owner, repo)) = fabro_github::parse_github_owner_repo(&normalized) else {
        // Non-GitHub origins never clone in these providers, so there is no
        // remote to keep credentials fresh for.
        return Ok(None);
    };
    InstallationTokenSource::for_repository(
        creds,
        owner,
        repo,
        serde_json::json!({ "contents": "write" }),
    )
    .map(Some)
    .map_err(|err| crate::Error::context_anyhow("Failed to build GitHub token source", err))
}

/// Push-credential state one provider instance tracks for its `origin`
/// remote.
pub(crate) struct PushCredentialState {
    source:   Option<Arc<InstallationTokenSource>>,
    /// Serializes compare → `set-url` → record. The token source's
    /// single-flight ends before the sandbox exec, so without this lock a
    /// refresh-ahead tick and a push could both see the old embedded
    /// generation and race on `.git/config.lock`. Holds the last
    /// successfully embedded token: its secret is already in the remote URL
    /// inside the sandbox, so retaining it adds no exposure, and it is what
    /// a push falls back to when a refresh fails. The tracked value is local
    /// belief, not ground truth — agent code inside the sandbox can rewrite
    /// `origin`.
    embedded: Mutex<Option<ResolvedToken>>,
}

impl PushCredentialState {
    pub(crate) fn new(source: Option<Arc<InstallationTokenSource>>) -> Self {
        Self {
            source,
            embedded: Mutex::new(None),
        }
    }

    pub(crate) fn source(&self) -> Option<&Arc<InstallationTokenSource>> {
        self.source.as_ref()
    }

    /// Record the token embedded in `origin` outside the refresh path — the
    /// clone is the first operation to embed a token, and it seeds this
    /// state so the first refresh compares against the clone token instead
    /// of believing nothing was ever embedded.
    pub(crate) async fn record_embedded(&self, token: ResolvedToken) {
        *self.embedded.lock().await = Some(token);
    }

    /// Refresh the credentials embedded in `origin`.
    ///
    /// Resolves through the shared source, skips the `set-url` exec when the
    /// resolved generation is already embedded, and records the new
    /// generation only after `set_url` succeeds. `set_url` receives the
    /// authenticated URL to embed and runs under the embed lock.
    pub(crate) async fn refresh<F, Fut>(
        &self,
        origin_url: &str,
        set_url: F,
    ) -> crate::Result<RefreshOutcome>
    where
        F: FnOnce(DisplaySafeUrl) -> Fut,
        Fut: Future<Output = crate::Result<()>>,
    {
        let Some(source) = &self.source else {
            return Ok(RefreshOutcome::none());
        };
        let mut embedded = self.embedded.lock().await;
        let resolved = match source.resolve().await {
            Ok(resolved) => resolved,
            Err(err) => {
                // The refresh-error path is defined, not incidental: the push
                // proceeds with the last embedded token, so log which one
                // that is instead of losing the credential state.
                if let Some(prev) = embedded.as_ref() {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        generation = prev.snapshot.generation,
                        provenance = %prev.snapshot.provenance,
                        token_age_ms = prev.snapshot.age_ms(),
                        "GitHub token refresh failed; origin keeps the last embedded credentials"
                    );
                } else {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        "GitHub token refresh failed and no credentials were ever embedded"
                    );
                }
                return Err(crate::Error::context_anyhow(
                    "Failed to refresh push credentials",
                    err,
                ));
            }
        };
        if embedded
            .as_ref()
            .is_some_and(|prev| prev.snapshot.generation == resolved.snapshot.generation)
        {
            return Ok(RefreshOutcome::unchanged(resolved.snapshot));
        }
        let auth_url = fabro_github::embed_token_in_url(origin_url, resolved.token.expose())
            .map_err(|err| {
                crate::Error::context_anyhow("Failed to build authenticated origin URL", err)
            })?;
        set_url(auth_url).await?;
        let snapshot = resolved.snapshot;
        *embedded = Some(resolved);
        Ok(RefreshOutcome::embedded(snapshot))
    }
}

/// What [`CredentialLease::ensure_embedded`] did for one push attempt.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EnsureOutcome {
    pub action:        RemoteCredentialAction,
    /// The token embedded in the remote right now — never an unembedded mint.
    pub token:         Option<TokenSnapshot>,
    pub refresh_error: Option<RefreshErrorKind>,
}

/// Scoped pin of push credentials for one push operation.
///
/// Holds the provider's embed mutex until dropped, so no other refresh can
/// re-embed mid-operation — a refresh-ahead tick crossing the cache margin
/// during a retrying push waits here instead of swapping the remote out from
/// under the pin. Internally retains up to two secrets: the last successfully
/// embedded token (the fallback) and the operation's resolved target, so both
/// drift re-embedding and the refresh-error fallback work. Only non-secret
/// snapshots leave the lease.
///
/// A successful resolve happens at most once per operation and is never
/// replaced; the pin transitions to the target only through a successful
/// embed. The token source's refresh margin exceeds every push plan's elapsed
/// bound, so the pinned token always outlives the operation.
pub(crate) struct CredentialLease<'a> {
    source:             Option<&'a InstallationTokenSource>,
    /// Embed-mutex guard: the last successfully embedded token.
    embedded:           MutexGuard<'a, Option<ResolvedToken>>,
    /// The operation's resolved target, including a cached fallback when a
    /// refresh mint failed.
    target:             Option<ResolvedToken>,
    /// Skip an immediate duplicate resolve after lease acquisition already
    /// failed. A later push attempt can retry after backoff.
    defer_resolve_once: bool,
}

impl PushCredentialState {
    /// Acquire the push-credential lease for one push operation.
    ///
    /// Resolves the operation's target token up front. A failed refresh can
    /// return a valid cached token; the first attempt uses it, and
    /// [`CredentialLease::ensure_embedded`] retries the refresh after push
    /// backoff. A resolve with no cached or embedded token fails acquisition.
    pub(crate) async fn lease(&self) -> crate::Result<CredentialLease<'_>> {
        let embedded = self.embedded.lock().await;
        let Some(source) = self.source.as_deref() else {
            return Ok(CredentialLease {
                source: None,
                embedded,
                target: None,
                defer_resolve_once: false,
            });
        };
        match source.resolve().await {
            Ok(resolved) => {
                let defer_resolve_once = resolved.refresh_failed;
                Ok(CredentialLease {
                    source: Some(source),
                    embedded,
                    target: Some(resolved),
                    defer_resolve_once,
                })
            }
            Err(err) => {
                if let Some(prev) = embedded.as_ref() {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        generation = prev.snapshot.generation,
                        provenance = %prev.snapshot.provenance,
                        token_age_ms = prev.snapshot.age_ms(),
                        "token resolve failed; push pins the last embedded credentials"
                    );
                    Ok(CredentialLease {
                        source: Some(source),
                        embedded,
                        target: None,
                        defer_resolve_once: true,
                    })
                } else {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        "token resolve failed and no credentials were ever embedded"
                    );
                    Err(crate::Error::message(
                        "Failed to refresh push credentials: token_mint_failed",
                    ))
                }
            }
        }
    }
}

impl CredentialLease<'_> {
    /// Non-secret description of the token embedded in the remote right now.
    pub(crate) fn snapshot(&self) -> Option<TokenSnapshot> {
        self.embedded.as_ref().map(|token| token.snapshot)
    }

    /// Embed the pinned generation if the remote does not carry it.
    ///
    /// One call covers the initial embed, a deferred embed after an earlier
    /// failure, and drift repair (`force` re-embeds even when the tracked
    /// generation matches, for remotes rewritten inside the sandbox). While
    /// the lease has no target, this retries the failed `resolve()` first —
    /// retrying a failed resolve discards no fresh token, so it cannot
    /// restart any replication clock. Refresh failures are recorded, never
    /// propagated: the push proceeds with the last embedded token.
    pub(crate) async fn ensure_embedded(
        &mut self,
        sandbox: &dyn crate::Sandbox,
        origin_url: &str,
        force: bool,
    ) -> crate::Result<EnsureOutcome> {
        let Some(source) = self.source else {
            return Ok(EnsureOutcome {
                action:        RemoteCredentialAction::None,
                token:         None,
                refresh_error: None,
            });
        };
        let mut refresh_error = self.defer_resolve_once.then_some(RefreshErrorKind::Mint);
        if self.defer_resolve_once {
            self.defer_resolve_once = false;
        } else if self
            .target
            .as_ref()
            .is_none_or(|resolved| resolved.refresh_failed)
        {
            match source.resolve().await {
                Ok(resolved) => {
                    refresh_error = resolved.refresh_failed.then_some(RefreshErrorKind::Mint);
                    self.target = Some(resolved);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %format!("{err:#}"),
                        "token resolve retry failed; pushing with the last embedded token"
                    );
                    refresh_error = Some(RefreshErrorKind::Mint);
                }
            }
        }
        let Some(desired) = self.target.as_ref().or(self.embedded.as_ref()).cloned() else {
            // Managed credentials with nothing resolved or embedded:
            // acquisition fails before any attempt runs, so pushes never see
            // this state.
            return Ok(EnsureOutcome {
                action: RemoteCredentialAction::None,
                token: None,
                refresh_error,
            });
        };
        let embedded_generation = self
            .embedded
            .as_ref()
            .map(|token| token.snapshot.generation);
        if !force && embedded_generation == Some(desired.snapshot.generation) {
            return Ok(EnsureOutcome {
                action: RemoteCredentialAction::Unchanged,
                token: Some(desired.snapshot),
                refresh_error,
            });
        }
        match set_url_via_exec(sandbox, origin_url, &desired).await {
            Ok(()) => {
                let snapshot = desired.snapshot;
                *self.embedded = Some(desired);
                Ok(EnsureOutcome {
                    action: RemoteCredentialAction::Embedded,
                    token: Some(snapshot),
                    refresh_error,
                })
            }
            Err(err) => {
                if matches!(
                    &err,
                    crate::Error::Exec { result, .. }
                        if result.termination != fabro_types::CommandTermination::Exited
                ) {
                    return Err(err);
                }
                tracing::warn!(
                    error = %crate::display_for_log(&err),
                    "embedding push credentials in origin failed; pushing with the last embedded token"
                );
                Ok(EnsureOutcome {
                    action:        RemoteCredentialAction::Unchanged,
                    token:         self.snapshot(),
                    refresh_error: Some(RefreshErrorKind::SetUrl),
                })
            }
        }
    }
}

/// Rewrite `origin` with the token embedded, through the sandbox's uniform
/// exec surface.
async fn set_url_via_exec(
    sandbox: &dyn crate::Sandbox,
    origin_url: &str,
    token: &ResolvedToken,
) -> crate::Result<()> {
    let auth_url =
        fabro_github::embed_token_in_url(origin_url, token.token.expose()).map_err(|err| {
            crate::Error::context(
                "Failed to build authenticated origin URL",
                RedactedSetUrlError(fabro_redact::redact_string(&format!("{err:#}"))),
            )
        })?;
    set_auth_url_via_exec(sandbox, auth_url).await
}

pub(crate) async fn set_auth_url_via_exec(
    sandbox: &dyn crate::Sandbox,
    auth_url: DisplaySafeUrl,
) -> crate::Result<()> {
    let command = format!(
        "git -c maintenance.auto=0 remote set-url origin {}",
        crate::shell_quote(auth_url.as_raw_url().as_str())
    );
    let result = sandbox
        .exec_command(&command, 10_000, None, None, None)
        .await
        .map_err(|err| {
            let message = redact::redact_auth_url(&crate::display_for_log(&err), Some(&auth_url));
            crate::Error::context(
                "Failed to refresh push credentials: set_url_exec_failed",
                RedactedSetUrlError(message),
            )
        })?;
    if !result.is_success() {
        return Err(result.into_exec_error_with_redactor(
            "git remote set-url origin (refresh push credentials)",
            |s| redact::redact_auth_url(s, Some(&auth_url)),
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RedactedSetUrlError(String);

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use fabro_github::InstallationToken;
    use fabro_github::test_support::{InstallationTokenMinter, installation_token_source};
    use tokio::time::sleep;

    use super::*;
    use crate::sandbox::RemoteCredentialAction;

    struct FixedMinter {
        calls: AtomicUsize,
        ttl:   chrono::Duration,
    }

    #[async_trait::async_trait]
    impl InstallationTokenMinter for FixedMinter {
        async fn mint(&self) -> anyhow::Result<InstallationToken> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(InstallationToken {
                token:      format!("ghs_gen{call}"),
                expires_at: Utc::now() + self.ttl,
            })
        }
    }

    struct FailingMinter;

    #[async_trait::async_trait]
    impl InstallationTokenMinter for FailingMinter {
        async fn mint(&self) -> anyhow::Result<InstallationToken> {
            Err(anyhow::anyhow!("mint failed"))
        }
    }

    fn minting_state(ttl: chrono::Duration) -> PushCredentialState {
        PushCredentialState::new(Some(installation_token_source(
            "owner/repo",
            Arc::new(FixedMinter {
                calls: AtomicUsize::new(0),
                ttl,
            }),
        )))
    }

    const ORIGIN: &str = "https://github.com/owner/repo";
    /// Long enough for a blocked task to be observably pending on paused time.
    const SHORT_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

    /// A refresh-ahead tick crossing the cache margin during a push waits on
    /// the embed mutex until the operation releases the lease, so the remote
    /// can never be swapped out from under the pinned generation.
    #[tokio::test(start_paused = true)]
    async fn refresh_waits_for_the_lease_to_release() {
        let state = std::sync::Arc::new(minting_state(chrono::Duration::minutes(60)));

        let lease = state.lease().await.expect("lease acquires");

        let refresh_task = {
            let state = std::sync::Arc::clone(&state);
            tokio::spawn(async move {
                state
                    .refresh(ORIGIN, |_| async { Ok(()) })
                    .await
                    .expect("refresh succeeds after the lease releases")
            })
        };

        // The refresh must be blocked while the lease holds the embed mutex.
        sleep(SHORT_WAIT).await;
        assert!(
            !refresh_task.is_finished(),
            "refresh must wait on the embed mutex"
        );

        drop(lease);
        let outcome = refresh_task.await.expect("refresh task completes");
        // The lease's resolve minted generation 1; the deferred refresh
        // reuses it (the operation never embedded, so the refresh embeds).
        assert_eq!(outcome.token().unwrap().generation, 1);
    }

    #[tokio::test]
    async fn refresh_without_managed_credentials_reports_none() {
        let state = PushCredentialState::new(None);
        let outcome = state
            .refresh(ORIGIN, |_| async { panic!("set-url must not run") })
            .await
            .unwrap();
        assert_eq!(outcome, RefreshOutcome::none());
    }

    #[tokio::test]
    async fn refresh_embeds_a_new_generation_and_skips_matching_ones() {
        let state = minting_state(chrono::Duration::minutes(60));
        let set_url_calls = AtomicUsize::new(0);

        let first = state
            .refresh(ORIGIN, |auth_url| {
                set_url_calls.fetch_add(1, Ordering::SeqCst);
                assert!(auth_url.as_raw_url().as_str().contains("ghs_gen1"));
                async { Ok(()) }
            })
            .await
            .unwrap();
        assert_eq!(first.action(), RemoteCredentialAction::Embedded);
        assert_eq!(first.token().unwrap().generation, 1);

        // The cached token is fresh, so the second refresh must skip set-url.
        let second = state
            .refresh(ORIGIN, |_| {
                set_url_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .await
            .unwrap();
        assert_eq!(second.action(), RemoteCredentialAction::Unchanged);
        assert_eq!(second.token().unwrap().generation, 1);
        assert_eq!(set_url_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refresh_embeds_again_when_the_source_mints_a_new_generation() {
        // Tokens expire inside the margin, so every resolve re-mints.
        let state = minting_state(chrono::Duration::minutes(5));
        let set_url_calls = AtomicUsize::new(0);

        let first = state
            .refresh(ORIGIN, |_| {
                set_url_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .await
            .unwrap();
        let second = state
            .refresh(ORIGIN, |_| {
                set_url_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .await
            .unwrap();

        assert_eq!(first.token().unwrap().generation, 1);
        assert_eq!(second.action(), RemoteCredentialAction::Embedded);
        assert_eq!(second.token().unwrap().generation, 2);
        assert_eq!(set_url_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn clone_seed_makes_the_first_refresh_a_no_op() {
        let state = minting_state(chrono::Duration::minutes(60));
        let clone_token = state.source().unwrap().mint_for_clone().await.unwrap();
        state.record_embedded(clone_token).await;

        let outcome = state
            .refresh(ORIGIN, |_| async { panic!("set-url must not run") })
            .await
            .unwrap();
        assert_eq!(outcome.action(), RemoteCredentialAction::Unchanged);
        assert_eq!(outcome.token().unwrap().generation, 1);
    }

    #[tokio::test]
    async fn failed_set_url_does_not_record_the_new_generation() {
        let state = minting_state(chrono::Duration::minutes(60));

        let err = state
            .refresh(ORIGIN, |_| async {
                Err(crate::Error::message("set-url failed"))
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("set-url failed"));

        // The generation was not recorded, so the retry embeds again instead
        // of wrongly skipping.
        let retried = state.refresh(ORIGIN, |_| async { Ok(()) }).await.unwrap();
        assert_eq!(retried.action(), RemoteCredentialAction::Embedded);
        assert_eq!(retried.token().unwrap().generation, 1);
    }

    #[tokio::test]
    async fn static_credentials_seeded_at_clone_skip_set_url() {
        let source = InstallationTokenSource::for_origin(
            &GitHubCredentials::Pat("ghp_pat".to_string()),
            ORIGIN,
            serde_json::json!({ "contents": "write" }),
        )
        .unwrap();
        let state = PushCredentialState::new(Some(source));
        let clone_token = state.source().unwrap().mint_for_clone().await.unwrap();
        state.record_embedded(clone_token).await;

        let outcome = state
            .refresh(ORIGIN, |_| async { panic!("set-url must not run") })
            .await
            .unwrap();
        assert_eq!(outcome.action(), RemoteCredentialAction::Unchanged);
        assert!(outcome.token().unwrap().is_static());
    }

    #[tokio::test]
    async fn mint_failure_preserves_the_mint_error_chain() {
        let state = PushCredentialState::new(Some(installation_token_source(
            "owner/repo",
            Arc::new(FailingMinter),
        )));

        let err = state
            .refresh(ORIGIN, |_| async { panic!("set-url must not run") })
            .await
            .unwrap_err();
        assert_eq!(err.causes(), vec![
            "minting GitHub installation access token",
            "mint failed"
        ]);
    }

    #[test]
    fn token_source_requires_managed_credentials_and_a_github_origin() {
        assert!(build_token_source(None, Some(ORIGIN)).unwrap().is_none());
        let pat = GitHubCredentials::Pat("ghp_pat".to_string());
        assert!(build_token_source(Some(&pat), None).unwrap().is_none());
        assert!(
            build_token_source(Some(&pat), Some("https://gitlab.com/owner/repo"))
                .unwrap()
                .is_none()
        );
        assert!(
            build_token_source(Some(&pat), Some(ORIGIN))
                .unwrap()
                .is_some()
        );
    }
}
