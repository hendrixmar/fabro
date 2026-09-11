use std::path::Path;

use anyhow::{Context as _, anyhow, bail};
use fabro_manifest::{CollectedWorkflowClosure, ResolvedLocalWorkflowPackage};
use fabro_types::settings::run::EnvironmentProvider;
use fabro_types::{DirtyStatus, RunTarget};
use tokio::task;

use super::remote_workflow::{Interruption, NativeGit};
use super::selection::{TargetSelection, WorkflowSelection};

/// Owns the canonical collector result without copying its contents. Local
/// location metadata remains available solely for existing settings warnings.
pub(super) enum ResolvedWorkflow {
    Local(ResolvedLocalWorkflowPackage),
    Git(CollectedWorkflowClosure),
}

impl ResolvedWorkflow {
    pub(super) fn closure(&self) -> &CollectedWorkflowClosure {
        match self {
            Self::Local(package) => package.closure(),
            Self::Git(closure) => closure,
        }
    }
}

pub(super) async fn workflow(
    selection: &WorkflowSelection,
    cwd: &Path,
    user_workflows: Option<&Path>,
    interruption: &Interruption,
) -> anyhow::Result<ResolvedWorkflow> {
    match selection {
        WorkflowSelection::Local(path) => {
            let (path, cwd, user_workflows) = (
                path.clone(),
                cwd.to_path_buf(),
                user_workflows.map(Path::to_path_buf),
            );
            let package = task::spawn_blocking(move || {
                fabro_manifest::resolve_local_workflow_package(
                    &path,
                    &cwd,
                    user_workflows.as_deref(),
                )
                .map_err(anyhow::Error::new)
            })
            .await
            .context("local workflow collection task failed")??;
            Ok(ResolvedWorkflow::Local(package))
        }
        WorkflowSelection::Git {
            repository,
            selector,
            revision,
        } => {
            let git = NativeGit::new();
            let (repository, selector, revision) =
                (repository.clone(), selector.clone(), revision.clone());
            let closure = interruption
                .owned(move |cancel| async move {
                    git.collect(repository, selector, revision, cancel).await
                })
                .await?;
            Ok(ResolvedWorkflow::Git(closure))
        }
    }
}

pub(super) async fn target(
    selection: &TargetSelection,
    provider: EnvironmentProvider,
    cwd: &Path,
    interruption: &Interruption,
) -> anyhow::Result<(RunTarget, bool)> {
    let path = match selection {
        TargetSelection::Path(path) => cwd
            .join(path)
            .canonicalize()
            .context("failed to canonicalize target directory")?,
        TargetSelection::Git { repository, branch } => {
            if !provider.is_clone_based() {
                bail!("Git targets require a clone-enabled Docker or Daytona environment");
            }
            let git = NativeGit::new();
            let (repository, branch) = (repository.clone(), branch.clone());
            let target = interruption
                .owned(move |cancel| async move {
                    git.resolve_target(repository, branch, &cancel).await
                })
                .await?;
            // Canonical admission retains ownership of provider capabilities.
            return Ok((RunTarget::Git(target), false));
        }
    };
    if !path.is_dir() {
        bail!("target path must be a directory");
    }
    // The existing observer can push/query Git synchronously. Preserve its
    // behavior without blocking a Tokio worker or promising a new timeout.
    task::spawn_blocking(move || run_target_for_environment(provider, &path))
        .await
        .context("target observation task failed")?
}

/// Derives the run target from the selected directory for the environment's
/// provider. Returns the target plus whether a clone-based observation found a
/// dirty Git worktree, so the caller can warn about it.
fn run_target_for_environment(
    provider: EnvironmentProvider,
    canonical_cwd: &Path,
) -> anyhow::Result<(RunTarget, bool)> {
    if !provider.is_clone_based() {
        let path = canonical_cwd.to_str().ok_or_else(|| {
            anyhow!(
                "target directory is not valid UTF-8: {}",
                canonical_cwd.display()
            )
        })?;
        return Ok((
            RunTarget::Folder {
                path: path.to_string(),
            },
            false,
        ));
    }
    let Some(observation) = fabro_manifest::observe_git_run_target(canonical_cwd, None) else {
        return Ok((none_target_for_unversioned_directory(canonical_cwd)?, false));
    };
    let dirty = observation.legacy_git_context.dirty == DirtyStatus::Dirty;
    let target = observation.run_target.ok_or_else(|| {
        anyhow!("the target Git checkout cannot be represented as a canonical GitHub run target")
    })?;
    if target.sha.is_none() {
        bail!(
            "the exact local Git commit could not be made available from the canonical GitHub origin; push the commit and try again"
        );
    }
    Ok((RunTarget::Git(target), dirty))
}

fn none_target_for_unversioned_directory(canonical_cwd: &Path) -> anyhow::Result<RunTarget> {
    let repository = match git2::Repository::discover(canonical_cwd) {
        Ok(repository) => repository,
        Err(source) if source.code() == git2::ErrorCode::NotFound => return Ok(RunTarget::None {}),
        Err(source) => {
            return Err(anyhow::Error::new(source)).with_context(|| {
                format!(
                    "failed to inspect target directory {} for Git metadata",
                    canonical_cwd.display()
                )
            });
        }
    };

    if repository.is_bare() {
        bail!(
            "the target directory resolves to a bare Git repository; clone-based runs require a non-bare checkout with an attached branch"
        );
    }
    match repository.head() {
        Err(source)
            if matches!(
                source.code(),
                git2::ErrorCode::UnbornBranch | git2::ErrorCode::NotFound
            ) =>
        {
            bail!(
                "the target Git checkout has no commits; create a commit before using a clone-based environment"
            );
        }
        Err(source) => {
            return Err(anyhow::Error::new(source))
                .context("failed to inspect the target Git checkout HEAD");
        }
        Ok(head) if !head.is_branch() => {
            bail!(
                "the target Git checkout has a detached HEAD; check out a branch before using a clone-based environment"
            );
        }
        Ok(_) => {}
    }

    bail!(
        "the target Git checkout does not have a usable attached branch for a clone-based run target"
    )
}

#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "resolver tests construct small local workflow fixtures"
)]
mod tests {
    use super::super::test_support::write_workflow;
    use super::*;

    #[tokio::test]
    async fn run_selection_target_resolution_by_provider() {
        let caller = tempfile::tempdir().unwrap();
        let root = caller.path().canonicalize().unwrap();
        write_workflow(&root, ".fabro/workflows/review");
        std::fs::create_dir(root.join("target")).unwrap();
        let selected = TargetSelection::Path("target".into());
        let interruption = Interruption::new(false);
        assert_eq!(
            target(&selected, EnvironmentProvider::Local, &root, &interruption)
                .await
                .unwrap()
                .0,
            RunTarget::Folder {
                path: root.join("target").to_str().unwrap().into(),
            }
        );
        for provider in [EnvironmentProvider::Docker, EnvironmentProvider::Daytona] {
            assert_eq!(
                target(&selected, provider, &root, &interruption)
                    .await
                    .unwrap()
                    .0,
                RunTarget::None {}
            );
        }
        assert_eq!(
            target(
                &TargetSelection::Path(".".into()),
                EnvironmentProvider::Local,
                &root,
                &interruption
            )
            .await
            .unwrap()
            .0,
            RunTarget::Folder {
                path: root.to_str().unwrap().into(),
            }
        );
        assert!(
            target(
                &TargetSelection::Git {
                    repository: "acme/app".parse().unwrap(),
                    branch:     None,
                },
                EnvironmentProvider::Local,
                &root,
                &interruption
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("Docker or Daytona")
        );
        for path in ["missing", ".fabro/workflows/review/workflow.toml"] {
            assert!(
                target(
                    &TargetSelection::Path(path.into()),
                    EnvironmentProvider::Local,
                    &root,
                    &interruption
                )
                .await
                .is_err(),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn run_selection_local_lookup_preserves_precedence_and_explicit_failure() {
        let root = tempfile::tempdir().unwrap();
        let user = root.path().join("user");
        let project = root.path().join("project");
        let checkout = root.path().join("project/checkout");
        write_workflow(&user, "review");
        write_workflow(&project, ".fabro/workflows/review");
        write_workflow(&checkout, ".fabro/workflows/review");
        std::fs::write(project.join(".fabro/project.toml"), "_version = 1\n").unwrap();
        git2::Repository::init(&checkout).unwrap();
        let selected = WorkflowSelection::Local("review".into());
        let interruption = Interruption::new(false);
        for (cwd, expected_root) in [
            (checkout.as_path(), checkout.as_path()),
            (project.as_path(), project.as_path()),
            (root.path(), user.as_path()),
        ] {
            let ResolvedWorkflow::Local(package) =
                workflow(&selected, cwd, Some(&user), &interruption)
                    .await
                    .unwrap()
            else {
                panic!("local package");
            };
            assert_eq!(package.source_root(), expected_root.canonicalize().unwrap());
        }
        assert!(
            workflow(
                &WorkflowSelection::Local("missing.toml".into()),
                &checkout,
                Some(&user),
                &interruption
            )
            .await
            .is_err()
        );
    }
}
