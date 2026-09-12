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

/// Explicit local paths escape the shorthand grammar, including colons in
/// file names. A repository without `:WORKFLOW` retains local lookup behavior.
pub(super) fn workflow_shorthand(path: &Path) -> Option<(&str, &str)> {
    let value = path.to_str()?;
    if path.is_absolute() || value.starts_with("./") || value.starts_with("../") {
        return None;
    }
    value.split_once(':')
}

fn repository_revision(value: &str) -> anyhow::Result<(GitHubRepositorySlug, Option<&str>)> {
    let (repository, revision) = value
        .split_once('@')
        .map_or((value, None), |(repository, revision)| {
            (repository, Some(revision))
        });
    let repository = repository
        .parse()
        .context("repository must be a GitHub OWNER/REPO")?;
    if revision == Some("") {
        bail!("a revision or branch is required after '@'");
    }
    Ok((repository, revision))
}

pub(super) fn parse(args: &RunArgs) -> anyhow::Result<(WorkflowSelection, TargetSelection)> {
    // Flag co-occurrence rules (`requires`/`conflicts_with`) are enforced by clap.
    let workflow = args.workflow.as_ref().context("workflow is required")?;
    let workflow = match (&args.workflow_repo, workflow_shorthand(workflow)) {
        (_, Some(_)) if args.workflow_repo.is_some() || args.workflow_ref.is_some() => {
            bail!("workflow shorthand cannot be combined with --workflow-repo or --workflow-ref");
        }
        (None, Some((source, selector))) => {
            let (repository, revision) = repository_revision(source)?;
            let selector = PathBuf::from(selector);
            validate_remote_selector(&selector)?;
            WorkflowSelection::Git {
                repository,
                selector,
                revision: RemoteWorkflowRevision::parse(revision)?,
            }
        }
        (None, None) => WorkflowSelection::Local(workflow.clone()),
        (Some(repository), _) => {
            validate_remote_selector(workflow)?;
            WorkflowSelection::Git {
                repository: repository.clone(),
                selector:   workflow.clone(),
                revision:   RemoteWorkflowRevision::parse(args.workflow_ref.as_deref())?,
            }
        }
    };
    let target = if let Some(value) = &args.target_repo_selector {
        let (repository, branch) = repository_revision(value)?;
        git_target(repository, branch)?
    } else {
        match (&args.target_from, &args.target_repo) {
            (Some(path), _) => TargetSelection::Path(path.clone()),
            (_, Some(repository)) => git_target(repository.clone(), args.target_branch.as_deref())?,
            _ => TargetSelection::Path(PathBuf::from(".")),
        }
    };
    Ok((workflow, target))
}

fn git_target(
    repository: GitHubRepositorySlug,
    branch: Option<&str>,
) -> anyhow::Result<TargetSelection> {
    if branch.is_some_and(|branch| !repository::is_valid_git_branch_name(branch)) {
        bail!("target branch must be a working branch name, not a tag, SHA, or qualified ref");
    }
    Ok(TargetSelection::Git {
        repository,
        branch: branch.map(str::to_owned),
    })
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
    fn shorthand_matches_explicit_selections_for_both_commands() {
        for command in ["run", "create"] {
            for (suffix, reference) in [
                ("", None),
                ("@v1.2", Some("v1.2")),
                ("@release/v2", Some("release/v2")),
                ("@refs/tags/v1", Some("refs/tags/v1")),
                (
                    "@abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
                    Some("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"),
                ),
            ] {
                for selector in ["review", "./reviews/security.toml"] {
                    for branch in [None, Some("release/v2")] {
                        let workflow = format!("acme/workflows{suffix}:{selector}");
                        let target = branch.map_or_else(
                            || "acme/app".to_owned(),
                            |branch| format!("acme/app@{branch}"),
                        );
                        let short = ["fabro", command, &workflow, "--target", &target];
                        let mut explicit = vec![
                            "fabro",
                            command,
                            selector,
                            "--workflow-repo",
                            "acme/workflows",
                            "--target-repo",
                            "acme/app",
                        ];
                        if let Some(reference) = reference {
                            explicit.extend(["--workflow-ref", reference]);
                        }
                        if let Some(branch) = branch {
                            explicit.extend(["--target-branch", branch]);
                        }
                        let selections = |argv: &[&str]| {
                            let cli = Cli::try_parse_from(argv).unwrap();
                            let Commands::RunCmd(
                                RunCommands::Run(args) | RunCommands::Create(args),
                            ) = *cli.command.unwrap()
                            else {
                                panic!("expected run args")
                            };
                            parse(&args).unwrap()
                        };
                        assert_eq!(selections(&short), selections(&explicit));
                    }
                }
            }
        }
    }

    #[test]
    fn shorthand_preserves_local_paths_and_requires_remote_workflow_selector() {
        for value in [
            "review",
            "dir/review.toml",
            "acme/workflows",
            "acme/workflows@v1",
            "./acme/workflows:review",
            "../acme/workflows:review",
            "/tmp/workflows:review",
        ] {
            let args = parse_run_args([value]).unwrap();
            assert_eq!(
                parse(&args).unwrap(),
                (
                    WorkflowSelection::Local(value.into()),
                    TargetSelection::Path(".".into())
                )
            );
        }
    }

    #[test]
    fn shorthand_rejects_malformed_or_conflicting_selections_before_acquisition() {
        for flags in [
            vec!["acme/workflows:"],
            vec!["acme/workflows@:review"],
            vec!["acme/workflows@HEAD~1:review"],
            vec!["acme/workflows:../review.toml"],
            vec!["acme/workflows:/review.toml"],
            vec!["acme/workflows/extra:review"],
            vec!["https://github.com/acme/workflows:review"],
            vec!["acme/workflows:review", "--workflow-repo", "acme/other"],
            vec!["acme/workflows:review", "--workflow-ref", "v1"],
            vec!["review", "--target", "acme/app@"],
            vec!["review", "--target", "acme/app@refs/tags/v1"],
            vec![
                "review",
                "--target",
                "acme/app@abcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
            ],
            vec!["review", "--target", "acme/app@main..next"],
            vec!["review", "--target", "acme/app/extra"],
            vec!["review", "--target", "acme/app", "--target-from", "."],
            vec![
                "review",
                "--target",
                "acme/app",
                "--target-repo",
                "acme/app",
            ],
            vec!["review", "--target", "acme/app", "--target-branch", "main"],
        ] {
            if let Ok(args) = parse_run_args(flags.iter().copied()) {
                assert!(parse(&args).is_err(), "{flags:?}");
            }
        }
    }

    #[test]
    fn run_selection_both_commands_share_the_adapter() {
        for command in ["run", "create"] {
            let cli = Cli::try_parse_from([
                "fabro",
                command,
                "review",
                "--workflow-repo",
                "acme/workflows",
                "--workflow-ref",
                "v1",
                "--target-repo",
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
                "--workflow-repo",
                "https://github.com/acme/workflows",
            ],
            ["review", "--target-repo", "acme/app/extra"],
        ] {
            assert!(parse_run_args(flags).is_err());
        }
        for flags in [
            vec!["../review.toml", "--workflow-repo", "acme/workflows"],
            vec!["/tmp/review.toml", "--workflow-repo", "acme/workflows"],
            vec![
                "review",
                "--workflow-repo",
                "acme/workflows",
                "--workflow-ref",
                "HEAD~1",
            ],
            vec![
                "review",
                "--target-repo",
                "acme/app",
                "--target-branch",
                "refs/tags/v1",
            ],
            vec![
                "review",
                "--target-repo",
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
