//! Fixtures shared by the run selection, resolution, and remote workflow
//! unit tests.
#![expect(
    clippy::disallowed_methods,
    reason = "test fixtures write small files synchronously"
)]
use std::path::Path;

use clap::Parser as _;

use crate::args::RunArgs;

#[derive(clap::Parser)]
struct Command {
    #[command(flatten)]
    args: RunArgs,
}

/// Parse `fabro run`/`fabro create` arguments exactly as clap would.
pub(crate) fn parse_run_args<'a>(
    args: impl IntoIterator<Item = &'a str>,
) -> Result<RunArgs, clap::Error> {
    Command::try_parse_from(std::iter::once("cmd").chain(args)).map(|command| command.args)
}

/// Write a minimal two-file workflow package under `root/name`.
pub(super) fn write_workflow(root: &Path, name: &str) {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("workflow.toml"),
        "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("workflow.fabro"),
        "digraph Test { start [shape=Mdiamond] exit [shape=Msquare] start -> exit }",
    )
    .unwrap();
}

/// Stage every file in the worktree and commit it on HEAD, returning the SHA.
pub(super) fn commit_all(repo: &git2::Repository, message: &str) -> String {
    let mut index = repo.index().unwrap();
    index
        .add_all(["."], git2::IndexAddOption::DEFAULT, None)
        .unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let parent = repo.head().ok().and_then(|head| head.peel_to_commit().ok());
    let parents: Vec<_> = parent.iter().collect();
    let signature = git2::Signature::now("Fixture", "fixture@example.test").unwrap();
    repo.commit(
        Some("HEAD"),
        &signature,
        &signature,
        message,
        &tree,
        &parents,
    )
    .unwrap()
    .to_string()
}
