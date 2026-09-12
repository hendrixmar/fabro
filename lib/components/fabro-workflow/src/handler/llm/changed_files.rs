use std::collections::HashSet;
use std::sync::Arc;

use fabro_sandbox::RunSandbox;
use fabro_util::shell;
use sandbox_driver::{Git as _, GitDiffOptions, GitRevisionRange};

/// The paths the working tree changed against `HEAD`, plus the untracked
/// files git does not ignore, sorted and deduplicated. A sandbox without
/// git, or a working directory that is not a repository, has no changed
/// files.
pub async fn detect_changed_files(sandbox: &Arc<RunSandbox>) -> Vec<String> {
    let Ok(git) = sandbox.git() else {
        return Vec::new();
    };
    let repo = sandbox.working_directory();
    let mut files: Vec<String> = Vec::new();
    if let Ok(entries) = git
        .diff_entries(repo, &GitDiffOptions::new(GitRevisionRange::new("HEAD")))
        .await
    {
        files.extend(entries.into_iter().map(|entry| entry.path));
    }
    if let Ok(untracked) = git.untracked_files(repo).await {
        files.extend(untracked);
    }

    files.sort();
    files.dedup();
    files
}

pub async fn files_touched_since(
    sandbox: &Arc<RunSandbox>,
    files_before: &[String],
) -> (Vec<String>, Option<String>) {
    let files_after = detect_changed_files(sandbox).await;
    let files_before: HashSet<&str> = files_before.iter().map(String::as_str).collect();
    let files_touched: Vec<String> = files_after
        .into_iter()
        .filter(|file| !files_before.contains(file.as_str()))
        .collect();

    let last_file_touched = if files_touched.is_empty() {
        None
    } else {
        let quoted_files: Vec<String> = files_touched
            .iter()
            .map(|file| shell::shell_quote(file))
            .collect();
        let cmd = format!("ls -t {} | head -1", quoted_files.join(" "));
        sandbox
            .exec_command(&cmd, 5_000, None, None, None)
            .await
            .ok()
            .and_then(|result| {
                let trimmed = result.stdout_lossy().trim().to_string();
                (result.success() && !trimmed.is_empty()).then_some(trimmed)
            })
    };

    (files_touched, last_file_touched)
}
