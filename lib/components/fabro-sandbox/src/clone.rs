//! Fabro's clone orchestration over the sandbox-driver [`Git`] and [`Exec`]
//! facets.
//!
//! The driver clones; fabro decides what to clone, where it lands, which
//! credentials it carries, and how failures retry. The layout is fabro's:
//! the repository checks out under `<repos_root>/<owner>/<repo>` and the
//! run works in `<workspace_root>/<repo>`, a symlink to the checkout. An
//! exact commit or a tag is pinned by the driver's clone options, which
//! fetch the pin directly and attach the branch to it; an unavailable pin
//! fails the clone and never falls back to the branch head. The GitHub App
//! token travels with the clone per call and is then installed as the
//! checkout's ambient credentials, so the agent's own git commands can
//! push; the remote URL never carries it.

use std::time::Duration;

use fabro_types::SandboxProviderKind;
use sandbox_driver::{
    ExecResult, Git as _, GitCloneOptions, GitFailureKind, Sandbox as DriverHandle,
};
use tokio::time;

use crate::clone_source::{self, GitHubRepoLayout};
use crate::credentials::{self, RepoCredentials};
use crate::exec::{ExecResultExt, SandboxExec};
use crate::git_policy;

/// Whole-clone budget, shared by every network and local step.
pub(crate) const GIT_CLONE_TIMEOUT: Duration = Duration::from_mins(5);

/// What the operator hears when the image has no `git`: the driver classifies
/// the failing command, and fabro names the fix.
const GIT_UNAVAILABLE_MESSAGE: &str = "The sandbox image must include git for repository \
                                       clone and git lifecycle operations. Use an image with \
                                       bash and git, such as buildpack-deps:noble.";

/// A GitHub clone fabro decided to perform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GitHubClone {
    pub(crate) origin_url: String,
    pub(crate) branch:     Option<String>,
    pub(crate) tag:        Option<String>,
    pub(crate) commit_sha: Option<String>,
    pub(crate) depth:      Option<u32>,
}

/// What the clone left behind: the layout it checked out into.
pub(crate) struct CloneOutcome {
    pub(crate) layout: GitHubRepoLayout,
}

/// Whether a failing git step talked to the remote. Local steps cannot fail
/// on credentials, so they must not suggest reconfiguring the GitHub App.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloneStep {
    Network,
    Local,
}

/// Clone `plan` into `handle`, laid out under `workspace_root` and
/// `repos_root`, with a GitHub App token from `credentials` when one is
/// available: the clone carries it per call, and the checkout keeps it as
/// ambient credentials afterwards.
pub(crate) async fn clone_github_repo(
    kind: &SandboxProviderKind,
    handle: &dyn DriverHandle,
    exec: &SandboxExec<'_>,
    plan: &GitHubClone,
    workspace_root: &str,
    repos_root: &str,
    credentials: &RepoCredentials,
) -> crate::Result<CloneOutcome> {
    let layout = clone_source::github_repo_layout(&plan.origin_url, workspace_root, repos_root)?;
    let token = credentials.mint_for_clone().await?;

    let fs = handle.fs();
    for dir in [workspace_root, layout.repos_owner_path.as_str()] {
        fs.create_dir(dir)
            .await
            .map_err(|error| crate::Error::context(format!("Failed to create {dir}"), error))?;
    }

    let deadline = time::Instant::now() + GIT_CLONE_TIMEOUT;
    let has_app = credentials.managed();
    let git = handle.git().ok_or_else(|| {
        crate::Error::message(format!(
            "sandbox provider `{kind}` does not support git operations"
        ))
    })?;
    // `decide_clone` already requires a branch for a pin; the branch names
    // the checkout the run works on, and the driver attaches it to the
    // pinned commit or tag.
    let mut options = GitCloneOptions::default();
    options.branch = plan
        .branch
        .clone()
        .filter(|branch| !branch.trim().is_empty());
    options.commit = plan.commit_sha.clone();
    options.tag = plan.tag.clone().filter(|_| plan.commit_sha.is_none());
    options.depth = plan.depth;
    options.credentials = token.as_ref().map(credentials::git_credentials);
    // The driver retries a clone the remote refused while the token may
    // still be replicating, inside what is left of the clone budget.
    let policy = git_policy::clone_policy(deadline.saturating_duration_since(time::Instant::now()));
    let target = layout.primary_repo_path.clone();
    sandbox_driver::retry_git(
        &policy,
        options.credentials.as_ref(),
        "git clone",
        |_attempt, _timeout| {
            let git = &git;
            let options = &options;
            let target = &target;
            let origin_url = &plan.origin_url;
            async move { git.clone_repo(origin_url, target, options).await }
        },
    )
    .await
    .map_err(|failure| {
        clone_failure_error(
            crate::Error::from(failure.error),
            CloneStep::Network,
            has_app,
        )
    })?;

    run_local_step(
        exec,
        &clone_source::repo_symlink_command(&layout),
        "create workspace repo symlink",
        deadline,
        has_app,
    )
    .await?;

    if let Some(token) = &token {
        RepoCredentials::install(&git, &layout.primary_repo_path, token).await?;
    }
    Ok(CloneOutcome { layout })
}

/// Run a local (non-network) step under the shared clone deadline.
///
/// Materializing a large working tree takes far longer than the short fixed
/// timeout used for trivial commands, so these steps get the same budget the
/// network steps have.
async fn run_local_step(
    exec: &SandboxExec<'_>,
    command: &str,
    label: &'static str,
    deadline: time::Instant,
    has_app: bool,
) -> crate::Result<ExecResult> {
    let remaining = deadline.saturating_duration_since(time::Instant::now());
    if remaining.is_zero() {
        return Err(crate::Error::message(format!(
            "{label} deadline expired before the step could run"
        )));
    }
    let result = exec
        .run(command, Some(remaining), Some("/"), None, None)
        .await
        .map_err(|error| crate::Error::context(format!("{label} transport failed"), error))?;
    if result.success() {
        return Ok(result);
    }
    Err(clone_failure_error(
        result.into_exec_error(label),
        CloneStep::Local,
        has_app,
    ))
}

fn clone_failure_error(error: crate::Error, step: CloneStep, has_app: bool) -> crate::Error {
    if git_unavailable(&error) {
        return crate::Error::context(GIT_UNAVAILABLE_MESSAGE, error);
    }
    let message = match step {
        CloneStep::Network if !has_app => {
            "Git clone failed. If this is a private repository, configure a GitHub App with \
             `fabro install` and install it for your organization."
        }
        CloneStep::Network => "Failed to clone repository into the sandbox",
        CloneStep::Local => "Failed to prepare the cloned repository in the sandbox",
    };
    crate::Error::context(message, error)
}

/// Whether the driver found no usable `git` in the sandbox.
fn git_unavailable(error: &crate::Error) -> bool {
    matches!(
        error.driver(),
        Some(sandbox_driver::Error::Git(failure))
            if failure.kind() == GitFailureKind::GitUnavailable
    )
}

#[cfg(test)]
mod tests {
    use fabro_github::token_source::InstallationTokenSource;
    use sandbox_driver::{ExecFailure, GitFailure, Termination};
    use sandbox_driver_testing::ScriptedSandbox;

    use super::*;

    const ORIGIN: &str = "https://github.com/acme/widgets";

    fn ok() -> ExecResult {
        ExecResult::new(Termination::Exited, Some(0), Duration::from_millis(1))
    }

    /// A scripted sandbox whose `origin` answers with the fixture URL and
    /// whose every other command succeeds.
    fn scripted_handle() -> ScriptedSandbox {
        let handle = ScriptedSandbox::with_id_and_working_dir("scripted", "/workspace")
            .runtime_directory("/tmp/sandbox-driver/runtime");
        handle.scripted_exec().respond_with(|spec| {
            let script = spec.args.last().map(String::as_str).unwrap_or_default();
            script.contains("'remote' 'get-url' 'origin'").then(|| {
                let mut result = ok();
                result.stdout = format!("{ORIGIN}\n").into_bytes();
                result
            })
        });
        handle.scripted_exec().set_default(ok());
        handle
    }

    fn plan() -> GitHubClone {
        GitHubClone {
            origin_url: ORIGIN.to_owned(),
            branch:     Some("main".to_owned()),
            tag:        None,
            commit_sha: None,
            depth:      Some(1),
        }
    }

    async fn clone_with(handle: &ScriptedSandbox, credentials: &RepoCredentials) -> CloneOutcome {
        let exec = SandboxExec::new(handle.exec());
        clone_github_repo(
            &SandboxProviderKind::DOCKER,
            handle,
            &exec,
            &plan(),
            "/workspace",
            "/repos",
            credentials,
        )
        .await
        .expect("clone succeeds")
    }

    #[tokio::test]
    async fn a_clone_carries_the_token_per_call_and_installs_it_for_the_checkout() {
        let handle = scripted_handle();
        let credentials =
            RepoCredentials::new(Some(InstallationTokenSource::pat("ghp_test".to_owned())));

        let outcome = clone_with(&handle, &credentials).await;
        assert_eq!(outcome.layout.primary_repo_path, "/repos/acme/widgets");

        let commands = handle.scripted_exec().commands();
        assert!(
            commands
                .iter()
                .all(|command| !command.contains("git --version")),
            "no probe runs ahead of the clone: {commands:#?}"
        );
        assert!(
            commands.iter().all(|command| !command.contains("set-url")),
            "the remote URL is never rewritten: {commands:#?}"
        );
        let clone = commands
            .iter()
            .find(|command| command.contains("'clone'"))
            .expect("the clone ran");
        assert!(
            clone.contains(
                "x-access-token:ghp_test@github.com/acme/widgets.insteadOf=https://github.com/acme/widgets"
            ),
            "the clone carries the token per call: {clone}"
        );
        assert!(
            commands.iter().any(|command| command.starts_with("ln -s ")),
            "{commands:#?}"
        );
        let install = commands
            .iter()
            .find(|command| command.contains("--add credential.helper"))
            .expect("the checkout's credential store is installed");
        assert!(
            install.contains("/tmp/sandbox-driver/runtime/git-credentials/"),
            "{install}"
        );
        assert!(
            commands
                .iter()
                .all(|command| !command.contains("ghp_test") || command.contains("insteadOf")),
            "the secret enters no command but the clone's own rewrite: {commands:#?}"
        );
        assert!(
            handle.scripted_exec().recorded().iter().any(|spec| {
                spec.env
                    .get("SANDBOX_DRIVER_GIT_CREDENTIAL")
                    .map(String::as_str)
                    == Some("https://x-access-token:ghp_test@github.com")
            }),
            "the store line travels in the environment"
        );
    }

    #[tokio::test]
    async fn a_clone_without_managed_credentials_installs_nothing() {
        let handle = scripted_handle();

        clone_with(&handle, &RepoCredentials::none()).await;

        let commands = handle.scripted_exec().commands();
        assert!(
            commands.iter().any(|command| command.contains("'clone'")),
            "{commands:#?}"
        );
        assert!(
            commands
                .iter()
                .all(|command| !command.contains("insteadOf")
                    && !command.contains("credential.helper")),
            "{commands:#?}"
        );
    }

    fn git_failure(exit_code: i32, stderr: &str) -> crate::Error {
        crate::Error::from(sandbox_driver::Error::Git(GitFailure::from_command(
            "git clone",
            ExecFailure::new(
                "git clone",
                Termination::Exited,
                Some(exit_code),
                Vec::new(),
                stderr.as_bytes().to_vec(),
            ),
        )))
    }

    #[test]
    fn a_missing_git_executable_names_the_image_requirement() {
        let error = clone_failure_error(
            git_failure(127, "bash: line 1: git: command not found"),
            CloneStep::Network,
            true,
        );
        assert!(
            error.to_string().contains("image must include git"),
            "{error}"
        );
    }

    #[test]
    fn other_network_failures_keep_the_credential_guidance() {
        let without_app = clone_failure_error(
            git_failure(128, "remote: Repository not found."),
            CloneStep::Network,
            false,
        );
        assert!(without_app.to_string().contains("fabro install"));
        let with_app = clone_failure_error(
            git_failure(128, "remote: Repository not found."),
            CloneStep::Network,
            true,
        );
        assert!(
            with_app
                .to_string()
                .contains("Failed to clone repository into the sandbox")
        );
        let local = clone_failure_error(git_failure(1, "ln: failed"), CloneStep::Local, true);
        assert!(local.to_string().contains("prepare the cloned repository"));
    }
}
