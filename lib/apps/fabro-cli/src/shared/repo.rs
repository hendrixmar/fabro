use std::path::Path;

use anyhow::{Context as _, Result, bail};

/// Detect the git remote URL and current branch from a local repository.
///
/// Uses `git2` to discover the repo at `path`, reads the `origin` remote URL
/// and the HEAD branch name.
pub(crate) fn detect_repo_info(path: &Path) -> Result<(String, Option<String>)> {
    let repo = git2::Repository::discover(path)
        .with_context(|| format!("Failed to discover git repo at {}", path.display()))?;

    let url = repo
        .find_remote("origin")
        .context("Failed to find 'origin' remote")?
        .url()
        .context("origin remote URL is not valid UTF-8")?
        .to_string();

    let branch = repo
        .head()
        .ok()
        .and_then(|head| head.shorthand().map(String::from));

    Ok((url, branch))
}

pub(crate) fn ensure_matching_repo_origin(
    expected_origin_url: Option<&str>,
    action: &str,
) -> Result<()> {
    let Some(expected_origin_url) = expected_origin_url else {
        return Ok(());
    };

    let cwd = std::env::current_dir()?;
    let (origin_url, _) = detect_repo_info(&cwd).map_err(|_| {
        anyhow::anyhow!(
            "Current directory is not a git repository with an origin remote; refusing to {action} run from repository '{expected_origin_url}'"
        )
    })?;
    let current_origin_url = fabro_github::normalize_repo_origin_url(&origin_url);

    if current_origin_url != expected_origin_url {
        bail!(
            "Current repository origin '{current_origin_url}' does not match run repository '{expected_origin_url}'; refusing to {action} this run from the wrong checkout"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{detect_repo_info, ensure_matching_repo_origin};

    #[test]
    fn missing_expected_origin_skips_guard() {
        ensure_matching_repo_origin(None, "fork").unwrap();
    }

    #[test]
    fn detect_git_remote_from_repo() {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        repo.remote("origin", "https://github.com/org/repo.git")
            .unwrap();

        let (url, _branch) = detect_repo_info(dir.path()).unwrap();
        assert_eq!(url, "https://github.com/org/repo.git");
    }

    #[test]
    fn detect_repo_info_returns_worktree_branch() {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@test.com").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let commit = repo
            .commit(Some("HEAD"), &sig, &sig, "init", &tree, &[])
            .unwrap();
        repo.remote("origin", "https://github.com/org/repo.git")
            .unwrap();
        let commit_obj = repo.find_commit(commit).unwrap();
        repo.branch("fabro/run/ABC", &commit_obj, false).unwrap();
        repo.set_head("refs/heads/fabro/run/ABC").unwrap();

        let (_, branch) = detect_repo_info(dir.path()).unwrap();
        assert_eq!(branch, Some("fabro/run/ABC".into()));
    }
}
