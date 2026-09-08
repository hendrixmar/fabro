//! CLI syntax ends here. Resolvers receive selections and explicit caller
//! context.
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, bail};
use fabro_types::{GitHubRepositorySlug, repository};

use crate::args::RunArgs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WorkflowSelection {
    Local(PathBuf),
    Git {
        repository: GitHubRepositorySlug,
        selector:   PathBuf,
        revision:   RemoteWorkflowRevision,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TargetSelection {
    CurrentDirectory,
    Path(PathBuf),
    Git {
        repository: GitHubRepositorySlug,
        branch:     Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RemoteWorkflowRevision {
    DefaultBranch,
    Ref(String),
    Commit(String),
}

impl RemoteWorkflowRevision {
    pub(super) fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        match value {
            None | Some("HEAD") => Ok(Self::DefaultBranch),
            Some(value) => {
                if let Some(sha) = repository::normalize_git_commit_sha(value) {
                    return Ok(Self::Commit(sha));
                }
                if !repository::is_valid_github_ref_selector(value)
                    || (value.starts_with("refs/")
                        && !value.starts_with("refs/heads/")
                        && !value.starts_with("refs/tags/"))
                {
                    bail!("workflow ref must be a branch, tag, HEAD, or full 40-hex commit SHA");
                }
                Ok(Self::Ref(value.to_owned()))
            }
        }
    }
}

pub(super) fn validate_remote_selector(path: &Path) -> anyhow::Result<()> {
    let value = path
        .to_str()
        .context("remote workflow selector must be valid UTF-8")?;
    if value.is_empty()
        || value.contains('\\')
        || value.chars().any(char::is_control)
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        || value.split('/').any(|part| part == "..")
    {
        bail!(
            "remote workflow must be a name or repository-relative .fabro/.toml file without traversal"
        );
    }
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("toml" | "fabro") => {}
        None if path
            .file_name()
            .is_some_and(|name| path.as_os_str() == name)
            && value != "."
            && !value.starts_with('-') => {}
        _ => bail!(
            "remote workflow must be a name or explicit .fabro/.toml file; directories are ambiguous"
        ),
    }
    Ok(())
}

pub(super) fn parse(args: &RunArgs) -> anyhow::Result<(WorkflowSelection, TargetSelection)> {
    let workflow = args.workflow.as_ref().context("workflow is required")?;
    if args.workflow_ref.is_some() && args.workflow_git.is_none() {
        bail!("--workflow-ref requires --workflow-git");
    }
    if args.target_branch.is_some() && args.target_git.is_none() {
        bail!("--target-branch requires --target-git");
    }
    if args.target_path.is_some() && args.target_git.is_some() {
        bail!("--target-path conflicts with --target-git");
    }
    let workflow = match &args.workflow_git {
        None => WorkflowSelection::Local(workflow.clone()),
        Some(repository) => {
            validate_remote_selector(workflow)?;
            WorkflowSelection::Git {
                repository: repository.clone(),
                selector:   workflow.clone(),
                revision:   RemoteWorkflowRevision::parse(args.workflow_ref.as_deref())?,
            }
        }
    };
    let target = match (&args.target_path, &args.target_git) {
        (Some(path), _) => TargetSelection::Path(path.clone()),
        (_, Some(repository)) => {
            if args
                .target_branch
                .as_deref()
                .is_some_and(|branch| !repository::is_valid_git_branch_name(branch))
            {
                bail!(
                    "target branch must be a working branch name, not a tag, SHA, or qualified ref"
                );
            }
            TargetSelection::Git {
                repository: repository.clone(),
                branch:     args.target_branch.clone(),
            }
        }
        _ => TargetSelection::CurrentDirectory,
    };
    Ok((workflow, target))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_selection_remote_grammar_is_pure_and_rejects_unsafe_selectors() {
        for path in [
            "review",
            "./review.toml",
            ".fabro/workflows/review/workflow.toml",
            "dir/graph.fabro",
        ] {
            validate_remote_selector(Path::new(path)).unwrap();
        }
        for path in [
            "",
            ".",
            "..",
            "/tmp/workflow.toml",
            "../review.toml",
            "a/../review.toml",
            "dir/review",
            "dir/",
            "a\\review.toml",
        ] {
            assert!(validate_remote_selector(Path::new(path)).is_err(), "{path}");
        }
        for value in [
            "topic/slash",
            "refs/heads/release",
            "refs/tags/v1",
            "HEAD",
            "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
        ] {
            RemoteWorkflowRevision::parse(Some(value)).unwrap();
        }
        for value in [
            "--upload-pack=x",
            "topic*",
            "HEAD~1",
            "main..next",
            "refs/pull/1/head",
            "a.lock",
            "main@{1}",
        ] {
            assert!(
                RemoteWorkflowRevision::parse(Some(value)).is_err(),
                "{value}"
            );
        }
    }
}

#[cfg(test)]
mod adapter_tests {
    use clap::Parser as _;

    use super::*;
    use crate::args::{Cli, Commands, RunCommands};

    #[derive(clap::Parser)]
    struct Command {
        #[command(flatten)]
        args: RunArgs,
    }

    #[test]
    fn run_selection_both_commands_share_the_adapter() {
        for command in ["run", "create"] {
            let cli = Cli::try_parse_from([
                "fabro",
                command,
                "review",
                "--workflow-git",
                "acme/workflows",
                "--workflow-ref",
                "v1",
                "--target-git",
                "acme/app",
                "--target-branch",
                "release",
            ])
            .unwrap();
            let Commands::RunCmd(RunCommands::Run(args) | RunCommands::Create(args)) =
                *cli.command.unwrap()
            else {
                panic!("expected shared run arguments");
            };
            assert_eq!(
                parse(&args).unwrap(),
                (
                    WorkflowSelection::Git {
                        repository: "acme/workflows".parse().unwrap(),
                        selector:   "review".into(),
                        revision:   RemoteWorkflowRevision::Ref("v1".into()),
                    },
                    TargetSelection::Git {
                        repository: "acme/app".parse().unwrap(),
                        branch:     Some("release".into()),
                    },
                )
            );
        }
        let cli = Cli::try_parse_from(["fabro", "run", "create"]).unwrap();
        assert!(
            matches!(*cli.command.unwrap(), Commands::RunCmd(RunCommands::Run(args)) if args.workflow.as_deref() == Some(Path::new("create")))
        );
    }

    #[test]
    fn run_selection_adapter_rejects_invalid_inputs_without_acquisition() {
        for flags in [
            vec![
                "cmd",
                "review",
                "--workflow-git",
                "https://github.com/acme/workflows",
            ],
            vec!["cmd", "review", "--target-git", "acme/app/extra"],
            vec!["cmd", "../review.toml", "--workflow-git", "acme/workflows"],
            vec![
                "cmd",
                "/tmp/review.toml",
                "--workflow-git",
                "acme/workflows",
            ],
            vec![
                "cmd",
                "review",
                "--workflow-git",
                "acme/workflows",
                "--workflow-ref",
                "HEAD~1",
            ],
            vec![
                "cmd",
                "review",
                "--target-git",
                "acme/app",
                "--target-branch",
                "refs/tags/v1",
            ],
            vec![
                "cmd",
                "review",
                "--target-git",
                "acme/app",
                "--target-branch",
                "1234567890123456789012345678901234567890",
            ],
        ] {
            if let Ok(command) = Command::try_parse_from(flags) {
                assert!(parse(&command.args).is_err());
            }
        }
    }
}
