//! GitHub credentials for a clone-based sandbox's repository.
//!
//! Fabro decides which credential a checkout works with and when it is
//! renewed; the sandbox driver applies it. The facet's own network
//! operations, fabro's clone and pushes, take the token per call and never
//! write it into the repository. The agent's own git commands read it from
//! the credential store the driver installs beside the checkout, which the
//! workflow's refresh tick rewrites as the token is renewed. The remote URL
//! is never touched, so no secret shows in `git remote -v` or in
//! `.git/config`. The token cache itself sits below, in
//! [`InstallationTokenSource`].

use std::sync::Arc;
use std::time::SystemTime;

use fabro_github::GitHubCredentials;
use fabro_github::token_source::{InstallationTokenSource, ResolvedToken};
use sandbox_driver::{Git as _, GitCredentials, GitFacet};

/// The username GitHub expects with an installation token or PAT.
pub(crate) const GITHUB_TOKEN_USERNAME: &str = "x-access-token";

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

/// The GitHub credentials a run's checkout works with: a token source when
/// fabro manages them, nothing when the repository was cloned without a
/// GitHub App or the sandbox was reattached by a later process.
pub(crate) struct RepoCredentials {
    source: Option<Arc<InstallationTokenSource>>,
}

impl RepoCredentials {
    pub(crate) fn new(source: Option<Arc<InstallationTokenSource>>) -> Self {
        Self { source }
    }

    /// No managed credentials: pushes and the agent's git commands use
    /// whatever the checkout already has.
    pub(crate) fn none() -> Self {
        Self::new(None)
    }

    pub(crate) fn managed(&self) -> bool {
        self.source.is_some()
    }

    /// Mint the clone token. Never a warm-cache reuse: a clone retried on
    /// replication lag must hold the token minted for it. The mint seeds
    /// the source, so later resolves reuse this token until it nears
    /// expiry.
    pub(crate) async fn mint_for_clone(&self) -> crate::Result<Option<ResolvedToken>> {
        let Some(source) = &self.source else {
            return Ok(None);
        };
        source.mint_for_clone().await.map(Some).map_err(|err| {
            crate::Error::context_anyhow("Failed to get GitHub App credentials for clone", err)
        })
    }

    /// The token one operation works with, reused from the cache until it
    /// nears expiry. A refresh that fails while the cached token is still
    /// valid returns that token.
    pub(crate) async fn resolve(&self) -> crate::Result<Option<ResolvedToken>> {
        let Some(source) = &self.source else {
            return Ok(None);
        };
        source.resolve().await.map(Some).map_err(|err| {
            crate::Error::context_anyhow("Failed to refresh GitHub App credentials", err)
        })
    }

    /// Install `token` as the credentials every git command run inside the
    /// sandbox picks up for the checkout at `repo_path`. The driver keeps
    /// them in a credential store beside the checkout and points the
    /// repository's helper configuration at it; calling again replaces
    /// them in place.
    pub(crate) async fn install(
        git: &GitFacet<'_>,
        repo_path: &str,
        token: &ResolvedToken,
    ) -> crate::Result<()> {
        git.set_ambient_credentials(repo_path, Some(&git_credentials(token)))
            .await
            .map_err(|error| {
                crate::Error::context("Failed to install the checkout's GitHub credentials", error)
            })
    }
}

/// The per-call form of `token` for the driver's network operations. The
/// mint time travels with a minted token so the driver's retry knows a
/// rejection may be replication lag; a static credential carries none.
pub(crate) fn git_credentials(token: &ResolvedToken) -> GitCredentials {
    let credentials = GitCredentials::new(GITHUB_TOKEN_USERNAME, token.token.expose());
    match token.snapshot.minted_at() {
        Some(minted_at) => credentials.minted_at(SystemTime::from(minted_at)),
        None => credentials,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unmanaged_credentials_resolve_to_nothing() {
        let credentials = RepoCredentials::none();
        assert!(!credentials.managed());
        assert!(credentials.mint_for_clone().await.unwrap().is_none());
        assert!(credentials.resolve().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_pat_becomes_per_call_credentials_under_the_github_username() {
        let source = InstallationTokenSource::pat("ghp_static".to_owned());
        let token = source.resolve().await.unwrap();
        let credentials = git_credentials(&token);
        assert_eq!(credentials.username, GITHUB_TOKEN_USERNAME);
        assert_eq!(credentials.password, "ghp_static");
        assert!(
            credentials.minted_at.is_none(),
            "a static credential has no mint time"
        );
    }
}
