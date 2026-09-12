use fabro_util::shell;

use crate::sandbox;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CloneDecision {
    EmptyWorkspace {
        reason: EmptyWorkspaceReason,
    },
    GitHub {
        origin_url: String,
        branch:     Option<String>,
        tag:        Option<String>,
        commit_sha: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GitHubRepoLayout {
    pub(crate) owner:               String,
    pub(crate) repo:                String,
    pub(crate) repos_owner_path:    String,
    pub(crate) primary_repo_path:   String,
    pub(crate) primary_repo_link:   String,
    pub(crate) execution_directory: String,
}

pub(crate) fn github_repo_layout(
    origin_url: &str,
    workspace_root: &str,
    repos_root: &str,
) -> crate::Result<GitHubRepoLayout> {
    let origin_url = fabro_github::normalize_repo_origin_url(origin_url);
    let (owner, repo) = fabro_github::parse_github_owner_repo(&origin_url).map_err(|err| {
        crate::Error::message(format!(
            "Clone-based sandboxes currently support GitHub repository origins only: {err}"
        ))
    })?;
    validate_path_component("owner", &owner)?;
    validate_path_component("repository", &repo)?;
    let workspace_root = trim_root(workspace_root);
    let repos_root = trim_root(repos_root);
    let repos_owner_path = sandbox::join_sandbox_path(repos_root, &owner);
    let primary_repo_path = sandbox::join_sandbox_path(&repos_owner_path, &repo);
    let primary_repo_link = sandbox::join_sandbox_path(workspace_root, &repo);

    Ok(GitHubRepoLayout {
        owner,
        repo,
        repos_owner_path,
        primary_repo_path,
        execution_directory: primary_repo_link.clone(),
        primary_repo_link,
    })
}

fn validate_path_component(label: &str, component: &str) -> crate::Result<()> {
    let is_safe = !matches!(component, "." | "..")
        && component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !is_safe {
        return Err(crate::Error::message(format!(
            "GitHub {label} is not a safe repository path component"
        )));
    }
    Ok(())
}

pub(crate) fn repo_symlink_command(layout: &GitHubRepoLayout) -> String {
    format!(
        "ln -s {} {}",
        shell::shell_quote(&layout.primary_repo_path),
        shell::shell_quote(&layout.primary_repo_link),
    )
}

/// The kind of revision a checkout is pinned to instead of the branch's
/// current HEAD.
///
/// The working branch names the checkout the run works on; it never constrains
/// which revision is fetched. No layer proves branch/revision ancestry. The
/// driver fetches the pin directly and attaches the branch to it, so an
/// unavailable revision fails the clone without falling back to branch HEAD,
/// and a successful clone has the pin checked out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PinnedRevision {
    /// An exact commit SHA.
    Commit,
    /// A bare tag name; the driver fetches it as `refs/tags/<tag>` so a
    /// same-named branch is never consulted.
    Tag,
}

impl PinnedRevision {
    /// An exact commit is authoritative over a tag; the tag stays on the run
    /// target as durable identity but does not drive the checkout.
    pub(crate) fn from_selectors(tag: Option<&str>, commit_sha: Option<&str>) -> Option<Self> {
        match (commit_sha, tag) {
            (Some(_), _) => Some(Self::Commit),
            (None, Some(_)) => Some(Self::Tag),
            (None, None) => None,
        }
    }

    /// Human-readable prefix for error messages.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Commit => "Exact commit checkout",
            Self::Tag => "Tag checkout",
        }
    }
}

fn trim_root(root: &str) -> &str {
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty() { "/" } else { trimmed }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EmptyWorkspaceReason {
    SkipClone,
    MissingOrigin,
}

impl EmptyWorkspaceReason {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::SkipClone => "clone disabled; creating an empty workspace",
            Self::MissingOrigin => {
                "no clone source was present; creating an empty workspace without repository files"
            }
        }
    }
}

pub(crate) fn decide_clone(
    skip_clone: bool,
    clone_origin_url: Option<&str>,
    clone_branch: Option<&str>,
    clone_tag: Option<&str>,
    clone_commit_sha: Option<&str>,
) -> crate::Result<CloneDecision> {
    if clone_tag.is_some_and(|tag| tag.trim().is_empty()) {
        return Err(crate::Error::message(
            "Tag checkout requires a non-empty tag",
        ));
    }
    let tag = clone_tag.map(str::to_string);
    let commit_sha = clone_commit_sha
        .map(normalize_exact_commit_sha)
        .transpose()?;

    if let Some(pin) = PinnedRevision::from_selectors(tag.as_deref(), commit_sha.as_deref()) {
        let selector = pin.label();
        if skip_clone {
            return Err(crate::Error::message(format!(
                "{selector} requires cloning to be enabled"
            )));
        }
        if clone_origin_url.is_none_or(|url| url.trim().is_empty()) {
            return Err(crate::Error::message(format!(
                "{selector} requires a repository origin"
            )));
        }
        // The branch names the checkout the run works on; it is not used to
        // constrain which commits may be fetched. No layer proves branch/SHA
        // ancestry, and an unavailable exact commit fails without falling back
        // to branch HEAD.
        if clone_branch.is_none_or(|branch| branch.trim().is_empty()) {
            return Err(crate::Error::message(format!(
                "{selector} requires a repository branch"
            )));
        }
    }

    if skip_clone {
        return Ok(CloneDecision::EmptyWorkspace {
            reason: EmptyWorkspaceReason::SkipClone,
        });
    }

    let Some(origin_url) = clone_origin_url.filter(|url| !url.trim().is_empty()) else {
        return Ok(CloneDecision::EmptyWorkspace {
            reason: EmptyWorkspaceReason::MissingOrigin,
        });
    };

    let origin_url = fabro_github::normalize_repo_origin_url(origin_url);
    if let Err(err) = fabro_github::parse_github_owner_repo(&origin_url) {
        return Err(crate::Error::message(format!(
            "Clone-based sandboxes currently support GitHub repository origins only: {err}"
        )));
    }

    Ok(CloneDecision::GitHub {
        origin_url,
        branch: clone_branch
            .filter(|branch| !branch.trim().is_empty())
            .map(str::to_string),
        tag,
        commit_sha,
    })
}

fn normalize_exact_commit_sha(commit_sha: &str) -> crate::Result<String> {
    fabro_types::normalize_git_commit_sha(commit_sha).ok_or_else(|| {
        crate::Error::message("Exact commit SHA must be exactly 40 ASCII hexadecimal characters")
    })
}

pub(crate) fn clean_clone_origin_for_record(clone_origin_url: Option<&str>) -> Option<String> {
    clone_origin_url
        .filter(|url| !url.trim().is_empty())
        .map(fabro_github::normalize_repo_origin_url)
}

pub(crate) fn repo_cloned_for_record(
    skip_clone: bool,
    clone_origin_url: Option<&str>,
) -> Option<bool> {
    Some(matches!(
        decide_clone(skip_clone, clone_origin_url, None, None, None).ok()?,
        CloneDecision::GitHub { .. }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_clone_overrides_present_origin() {
        assert_eq!(
            decide_clone(
                true,
                Some("https://gitlab.com/acme/widgets.git"),
                Some("main"),
                None,
                None,
            )
            .unwrap(),
            CloneDecision::EmptyWorkspace {
                reason: EmptyWorkspaceReason::SkipClone,
            }
        );
    }

    #[test]
    fn missing_origin_creates_empty_workspace() {
        assert_eq!(
            decide_clone(false, None, None, None, None).unwrap(),
            CloneDecision::EmptyWorkspace {
                reason: EmptyWorkspaceReason::MissingOrigin,
            }
        );
    }

    #[test]
    fn github_origin_is_normalized_with_branch() {
        assert_eq!(
            decide_clone(
                false,
                Some("git@github.com:acme/widgets.git"),
                Some("feature/work"),
                None,
                None,
            )
            .unwrap(),
            CloneDecision::GitHub {
                origin_url: "https://github.com/acme/widgets".to_string(),
                branch:     Some("feature/work".to_string()),
                tag:        None,
                commit_sha: None,
            }
        );
    }

    #[test]
    fn tag_clone_keeps_working_branch_and_bare_tag_distinct() {
        assert_eq!(
            decide_clone(
                false,
                Some("https://github.com/acme/widgets"),
                Some("release"),
                Some("v1.2.3"),
                None,
            )
            .unwrap(),
            CloneDecision::GitHub {
                origin_url: "https://github.com/acme/widgets".to_string(),
                branch:     Some("release".to_string()),
                tag:        Some("v1.2.3".to_string()),
                commit_sha: None,
            }
        );
    }

    #[test]
    fn pinned_revision_prefers_exact_commit_over_a_tag() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(PinnedRevision::from_selectors(None, None), None);
        assert_eq!(
            PinnedRevision::from_selectors(Some("release/v1"), None),
            Some(PinnedRevision::Tag)
        );
        assert_eq!(
            PinnedRevision::from_selectors(Some("release/v1"), Some(sha)),
            Some(PinnedRevision::Commit)
        );
    }

    #[test]
    fn non_github_origin_fails_without_skip_clone() {
        let error = decide_clone(
            false,
            Some("https://gitlab.com/acme/widgets.git"),
            None,
            None,
            None,
        )
        .expect_err("non-GitHub origins should fail");
        assert!(error.to_string().contains("GitHub repository origins only"));
    }

    #[test]
    fn exact_commit_sha_is_validated_and_normalized() {
        let lowercase = "0123456789abcdef0123456789abcdef01234567";
        let uppercase = "ABCDEF0123456789ABCDEF0123456789ABCDEF01";

        assert_eq!(
            decide_clone(
                false,
                Some("https://github.com/acme/widgets"),
                Some("moving-branch"),
                Some("release"),
                Some(lowercase),
            )
            .unwrap(),
            CloneDecision::GitHub {
                origin_url: "https://github.com/acme/widgets".to_string(),
                branch:     Some("moving-branch".to_string()),
                tag:        Some("release".to_string()),
                commit_sha: Some(lowercase.to_string()),
            }
        );
        assert_eq!(
            decide_clone(
                false,
                Some("https://github.com/acme/widgets"),
                Some("main"),
                None,
                Some(uppercase),
            )
            .unwrap(),
            CloneDecision::GitHub {
                origin_url: "https://github.com/acme/widgets".to_string(),
                branch:     Some("main".to_string()),
                tag:        None,
                commit_sha: Some(uppercase.to_ascii_lowercase()),
            }
        );
    }

    #[test]
    fn exact_commit_sha_rejects_noncanonical_inputs() {
        for sha in [
            "",
            "0123456789abcdef0123456789abcdef0123456",
            "0123456789abcdef0123456789abcdef012345678",
            "0123456789abcdef0123456789abcdef0123456g",
            " 0123456789abcdef0123456789abcdef01234567",
            "0123456789abcdef0123456789abcdef01234567 ",
            "0123456789abcdef0123456789abcdef012345é",
        ] {
            let error = decide_clone(
                false,
                Some("https://github.com/acme/widgets"),
                None,
                None,
                Some(sha),
            )
            .expect_err("invalid exact commit SHA should fail");
            assert!(
                error.to_string().contains("40 ASCII hexadecimal"),
                "unexpected error for {sha:?}: {error}"
            );
        }
    }

    #[test]
    fn pinned_checkout_requires_clone_origin_and_branch() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        for (tag, commit_sha) in [(None, Some(sha)), (Some("v1"), None)] {
            let skip_error = decide_clone(
                true,
                Some("https://github.com/acme/widgets"),
                Some("main"),
                tag,
                commit_sha,
            )
            .expect_err("pinned checkout with skip-clone should fail");
            assert!(skip_error.to_string().contains("requires cloning"));

            for origin in [None, Some(""), Some("   ")] {
                let error = decide_clone(false, origin, Some("main"), tag, commit_sha)
                    .expect_err("pinned checkout without an origin should fail");
                assert!(error.to_string().contains("requires a repository origin"));
            }

            for branch in [None, Some(""), Some("   ")] {
                let error = decide_clone(
                    false,
                    Some("https://github.com/acme/widgets"),
                    branch,
                    tag,
                    commit_sha,
                )
                .expect_err("pinned checkout without a branch should fail");
                assert!(error.to_string().contains("requires a repository branch"));
            }
        }
    }

    #[test]
    fn tag_checkout_rejects_empty_tag() {
        let empty_tag = decide_clone(
            false,
            Some("https://github.com/acme/widgets"),
            Some("main"),
            Some(""),
            None,
        )
        .expect_err("empty tags should fail");
        assert!(empty_tag.to_string().contains("non-empty tag"));
    }

    #[test]
    fn github_layout_maps_ssh_origin_to_repos_checkout_and_workspace_link() {
        let layout = github_repo_layout(
            "git@github.com:brynary/rack-test.git",
            "/workspace",
            "/repos",
        )
        .unwrap();

        assert_eq!(layout.owner, "brynary");
        assert_eq!(layout.repo, "rack-test");
        assert_eq!(layout.repos_owner_path, "/repos/brynary");
        assert_eq!(layout.primary_repo_path, "/repos/brynary/rack-test");
        assert_eq!(layout.primary_repo_link, "/workspace/rack-test");
        assert_eq!(layout.execution_directory, "/workspace/rack-test");
    }

    #[test]
    fn github_layout_normalizes_https_origin_and_trims_roots() {
        let layout = github_repo_layout(
            "https://github.com/fabro-sh/fabro.git/",
            "/workspace/",
            "/repos/",
        )
        .unwrap();

        assert_eq!(layout.owner, "fabro-sh");
        assert_eq!(layout.repo, "fabro");
        assert_eq!(layout.repos_owner_path, "/repos/fabro-sh");
        assert_eq!(layout.primary_repo_path, "/repos/fabro-sh/fabro");
        assert_eq!(layout.primary_repo_link, "/workspace/fabro");
        assert_eq!(layout.execution_directory, "/workspace/fabro");
    }

    #[test]
    fn github_layout_rejects_path_traversal_components() {
        for origin in [
            "https://github.com/../widgets",
            "https://github.com/acme/..",
            "https://github.com/%2e%2e/widgets",
        ] {
            let error = github_repo_layout(origin, "/workspace", "/repos")
                .expect_err("unsafe path component should fail");
            assert!(
                error.to_string().contains("safe repository path component"),
                "got {error} for {origin}"
            );
        }
    }

    #[test]
    fn repo_symlink_command_quotes_both_paths() {
        let layout = github_repo_layout(
            "https://github.com/fabro-sh/fabro",
            "/work space",
            "/repo root",
        )
        .unwrap();

        assert_eq!(
            repo_symlink_command(&layout),
            "ln -s '/repo root/fabro-sh/fabro' '/work space/fabro'"
        );
    }

    #[test]
    fn record_origin_strips_credentials() {
        assert_eq!(
            clean_clone_origin_for_record(Some(
                "https://x-access-token:secret@github.com/acme/widgets.git"
            )),
            Some("https://github.com/acme/widgets".to_string())
        );
    }
}
