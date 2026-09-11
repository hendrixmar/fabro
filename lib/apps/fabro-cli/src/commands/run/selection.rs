//! CLI syntax ends here. Resolvers receive selections and explicit caller
//! context.
use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};
use fabro_types::{GitHubRepositorySlug, WorkflowPath, repository};

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
    /// Directory relative to the caller; the default is the caller directory
    /// itself.
    Path(PathBuf),
    Git {
        repository: GitHubRepositorySlug,
        branch:     Option<String>,
    },
}

/// A validated `--workflow-ref`, classified once so resolution never re-derives
/// which ref namespaces a value may name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RemoteWorkflowRevision {
    DefaultBranch,
    /// A fully qualified `refs/heads/...` branch.
    Branch(String),
    /// A fully qualified `refs/tags/...` tag.
    Tag(String),
    /// A bare name that may be a branch or a tag.
    Name(String),
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
                if !repository::is_valid_github_ref_selector(value) {
                    bail!("workflow ref must be a branch, tag, HEAD, or full 40-hex commit SHA");
                }
                let reference = value.to_owned();
                if value.starts_with("refs/heads/") {
                    Ok(Self::Branch(reference))
                } else if value.starts_with("refs/tags/") {
                    Ok(Self::Tag(reference))
                } else if value.starts_with("refs/") {
                    bail!("workflow ref must be a branch, tag, HEAD, or full 40-hex commit SHA");
                } else {
                    Ok(Self::Name(reference))
                }
            }
        }
    }
}

pub(super) fn validate_remote_selector(path: &Path) -> anyhow::Result<()> {
    let value = path
        .to_str()
        .context("remote workflow selector must be valid UTF-8")?;
    let value = value.strip_prefix("./").unwrap_or(value);
    if WorkflowPath::new(value).is_err() {
        bail!(
            "remote workflow must be a name or repository-relative .fabro/.toml file without traversal"
        );
    }
    let is_bare_name = !value.contains('/') && !value.starts_with('-');
    match Path::new(value).extension().and_then(|ext| ext.to_str()) {
        Some("toml" | "fabro") => Ok(()),
        None if is_bare_name => Ok(()),
        _ => bail!(
            "remote workflow must be a name or explicit .fabro/.toml file; directories are ambiguous"
        ),
    }
}

pub(super) fn parse(args: &RunArgs) -> anyhow::Result<(WorkflowSelection, TargetSelection)> {
    // Flag co-occurrence rules (`requires`/`conflicts_with`) are enforced by clap.
    let workflow = args.workflow.as_ref().context("workflow is required")?;
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
        _ => TargetSelection::Path(PathBuf::from(".")),
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
        for (value, expected) in [
            (
                "topic/slash",
                RemoteWorkflowRevision::Name("topic/slash".into()),
            ),
            (
                "refs/heads/release",
                RemoteWorkflowRevision::Branch("refs/heads/release".into()),
            ),
            (
                "refs/tags/v1",
                RemoteWorkflowRevision::Tag("refs/tags/v1".into()),
            ),
            ("HEAD", RemoteWorkflowRevision::DefaultBranch),
            (
                "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                RemoteWorkflowRevision::Commit("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd".into()),
            ),
        ] {
            assert_eq!(
                RemoteWorkflowRevision::parse(Some(value)).unwrap(),
                expected
            );
        }
        assert_eq!(
            RemoteWorkflowRevision::parse(None).unwrap(),
            RemoteWorkflowRevision::DefaultBranch
        );
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
    use super::super::test_support::parse_run_args;
    use super::*;
    use crate::args::{Cli, Commands, RunCommands};

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
                        revision:   RemoteWorkflowRevision::Name("v1".into()),
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
        // Malformed repository slugs never reach the adapter.
        for flags in [
            [
                "review",
                "--workflow-git",
                "https://github.com/acme/workflows",
            ],
            ["review", "--target-git", "acme/app/extra"],
        ] {
            assert!(parse_run_args(flags).is_err());
        }
        for flags in [
            vec!["../review.toml", "--workflow-git", "acme/workflows"],
            vec!["/tmp/review.toml", "--workflow-git", "acme/workflows"],
            vec![
                "review",
                "--workflow-git",
                "acme/workflows",
                "--workflow-ref",
                "HEAD~1",
            ],
            vec![
                "review",
                "--target-git",
                "acme/app",
                "--target-branch",
                "refs/tags/v1",
            ],
            vec![
                "review",
                "--target-git",
                "acme/app",
                "--target-branch",
                "1234567890123456789012345678901234567890",
            ],
        ] {
            let args = parse_run_args(flags.iter().copied()).unwrap();
            assert!(parse(&args).is_err(), "{flags:?}");
        }
    }
}
