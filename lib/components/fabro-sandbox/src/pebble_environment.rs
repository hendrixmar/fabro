//! [`RunSandbox`] as the [`Environment`] pebble's coding agent runs in.
//!
//! Pebble's tools speak the `Environment` contract; fabro's one sandbox type
//! speaks the sandbox driver's facets. This module is the mapping between the
//! two, and nothing else: every path resolves the way fabro resolves it, every
//! command runs through [`SandboxExec`](crate::SandboxExec) with fabro's
//! exec policy, and every failure keeps its driver cause. There is no adapter
//! struct; a run sandbox *is* an environment.
//!
//! Where the two contracts differ, pebble's wins here because the model reads
//! pebble's: a glob that pebble rejects is rejected before the driver sees it,
//! a directory listing is in tree order, and a command with no retention cap
//! still drains under the driver's default buffer rather than without bound.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pebble_coding_agent::environment::support::{capture_stats, tree_order, validate_glob};
use pebble_coding_agent::environment::{
    DirEntry, EnvResult, Environment, EnvironmentError, EnvironmentErrorKind, ExecOutcome,
    ExecOutputSink, ExecOutputStream, ExecRequest, ExecResult, GrepOptions,
};
use sandbox_driver::{ExecControls, ExecSpec, FileKind, OutputSink, OutputStream};

use crate::driver_sandbox::RunSandbox;
use crate::exec::{ExecResultExt as _, command_termination, program_exit_code};
use crate::sandbox;

#[async_trait]
impl Environment for RunSandbox {
    fn working_directory(&self) -> &str {
        Self::working_directory(self)
    }

    fn platform(&self) -> &str {
        Self::platform(self)
    }

    fn os_version(&self) -> String {
        Self::os_version(self)
    }

    async fn read_file_bytes(&self, path: &str) -> EnvResult<Vec<u8>> {
        Self::read_file_bytes(self, path)
            .await
            .map_err(|error| environment_error(&format!("Failed to read {path}"), error))
    }

    async fn write_file(&self, path: &str, content: &str) -> EnvResult<()> {
        Self::write_file(self, path, content)
            .await
            .map_err(|error| environment_error(&format!("Failed to write {path}"), error))
    }

    async fn rename_file(&self, source: &str, destination: &str) -> EnvResult<()> {
        let resolved_source = self.resolve_for_environment(source);
        let resolved_destination = self.resolve_for_environment(destination);
        if !Self::file_exists(self, source)
            .await
            .map_err(|error| environment_error(&format!("Failed to stat {source}"), error))?
        {
            return Err(EnvironmentError::new(
                EnvironmentErrorKind::NotFound,
                format!("Failed to move {source}: file does not exist"),
            ));
        }
        // The same path spelled twice is a move to itself, which must leave
        // the file where it is. Aliases the sandbox's own filesystem would
        // resolve (a symlinked parent, a hard link) are not checked: fabro has
        // no remote `realpath`, and a driver `mv a a` is a no-op anyway.
        if normalize(&resolved_source) == normalize(&resolved_destination) {
            return Ok(());
        }
        let handle = self
            .handle()
            .map_err(|error| environment_error("Sandbox is not initialized", error))?;
        // The destination's parent is created first, and a parent that is a
        // file fails here, before anything has moved, so the source stays
        // intact as the contract requires.
        if let Some(parent) = parent_directory(&resolved_destination) {
            handle.fs().create_dir(parent).await.map_err(|error| {
                environment_error(
                    &format!("Failed to create the parent directory of {destination}"),
                    crate::Error::from(error),
                )
            })?;
        }
        handle
            .fs()
            .rename(&resolved_source, &resolved_destination)
            .await
            .map_err(|error| {
                environment_error(
                    &format!("Failed to move {source} to {destination}"),
                    crate::Error::from(error),
                )
            })
    }

    async fn delete_file(&self, path: &str) -> EnvResult<()> {
        // The driver's delete is idempotent; pebble's is a `remove_file`, which
        // reports a path that is not there.
        if !Self::file_exists(self, path)
            .await
            .map_err(|error| environment_error(&format!("Failed to stat {path}"), error))?
        {
            return Err(EnvironmentError::new(
                EnvironmentErrorKind::NotFound,
                format!("Failed to delete {path}: file does not exist"),
            ));
        }
        Self::delete_file(self, path)
            .await
            .map_err(|error| environment_error(&format!("Failed to delete {path}"), error))
    }

    async fn file_exists(&self, path: &str) -> EnvResult<bool> {
        Self::file_exists(self, path)
            .await
            .map_err(|error| environment_error(&format!("Failed to stat {path}"), error))
    }

    async fn list_directory(&self, path: &str, depth: Option<usize>) -> EnvResult<Vec<DirEntry>> {
        let mut entries: Vec<DirEntry> = Self::list_directory(self, path, depth)
            .await
            .map_err(|error| environment_error(&format!("Failed to list {path}"), error))?
            .into_iter()
            .map(|entry| DirEntry {
                is_dir: entry.kind == FileKind::Directory,
                size:   (entry.kind == FileKind::File)
                    .then_some(entry.size)
                    .flatten(),
                name:   entry.path,
            })
            .collect();
        // The driver lists in flat lexicographic order of the whole relative
        // path, where `foo-bar` sorts between `foo` and `foo/x`. Pebble lists
        // in tree order, and says how.
        tree_order(&mut entries);
        Ok(entries)
    }

    async fn grep(
        &self,
        pattern: &str,
        path: &str,
        options: &GrepOptions,
    ) -> EnvResult<Vec<String>> {
        let mut driver_options = sandbox_driver::GrepOptions::default();
        driver_options.case_insensitive = options.case_insensitive;
        driver_options.max_matches = options.max_results;
        driver_options.include = options.glob_filter.clone();
        let matches = Self::grep(self, pattern, path, &driver_options)
            .await
            .map_err(|error| environment_error("Failed to search file contents", error))?;
        Ok(matches
            .into_iter()
            .map(|found| format!("{}:{}:{}", found.path, found.line_number, found.line))
            .collect())
    }

    async fn glob(&self, pattern: &str, path: Option<&str>) -> EnvResult<Vec<String>> {
        // Validated by pebble's own grammar before the driver sees the
        // pattern, so the reason reaches the model in pebble's words and the
        // patterns pebble rejects are rejected even where fabro's glob would
        // accept them.
        validate_glob(pattern)?;
        Self::glob(self, pattern, path)
            .await
            .map_err(|error| environment_error("Failed to match files", error))
    }

    async fn exec(&self, request: ExecRequest<'_>) -> EnvResult<ExecOutcome> {
        let ExecRequest {
            command,
            timeout_ms,
            working_dir,
            env_vars,
            cancel_token,
            output_bytes_cap,
            output_sink,
        } = request;
        let mut spec = ExecSpec::bash(command).no_timeout();
        if let Some(timeout_ms) = timeout_ms {
            spec = spec.timeout(Duration::from_millis(timeout_ms));
        }
        if let Some(dir) = working_dir {
            spec = spec.working_dir(dir);
        }
        for (key, value) in env_vars.into_iter().flatten() {
            spec = spec.env_var(key, value);
        }
        let controls = ExecControls {
            term: cancel_token,
            sink: output_sink.map(adapt_output_sink),
            // `None` asks pebble for no cap at all. Fabro's exec policy fills
            // its default buffer when the cap is unset, so a command with no
            // cap drains under that default rather than without bound; the
            // capture counts still say what was dropped.
            retained_output_limit: output_bytes_cap,
            ..ExecControls::default()
        };
        let streaming = self
            .exec_command_streaming(spec, controls)
            .await
            .map_err(|error| {
                let kind = match error.driver() {
                    Some(sandbox_driver::Error::Transport(_)) => EnvironmentErrorKind::Io,
                    Some(sandbox_driver::Error::Unsupported { .. }) => {
                        EnvironmentErrorKind::Unsupported
                    }
                    _ => EnvironmentErrorKind::Spawn,
                };
                EnvironmentError::with_source(kind, "Failed to run the command", error)
            })?;
        let result = streaming.result;
        Ok(ExecOutcome {
            result:            ExecResult {
                stdout:      result.stdout_lossy(),
                stderr:      result.stderr_lossy(),
                exit_code:   program_exit_code(result.termination, result.exit_code),
                termination: command_termination(result.termination),
                duration_ms: result.duration_ms(),
            },
            streams_separated: streaming.streams_separated,
            stdout_capture:    capture_stats(
                streaming.stdout_capture.observed_bytes,
                output_bytes_cap,
            ),
            stderr_capture:    capture_stats(
                streaming.stderr_capture.observed_bytes,
                output_bytes_cap,
            ),
        })
    }
}

impl RunSandbox {
    /// A caller path as the driver will see it: fabro's working directory
    /// applied where fabro applies it, and nothing more.
    fn resolve_for_environment(&self, path: &str) -> String {
        sandbox::resolve_path(path, Self::working_directory(self))
    }
}

/// Pebble's glob grammar, beyond what fabro's glob already rejects.
///
/// A path with its redundant separators and `.` segments removed, for
/// deciding whether two spellings name the same file.
fn normalize(path: &str) -> String {
    let absolute = path.starts_with('/');
    let joined = path
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>()
        .join("/");
    if absolute {
        format!("/{joined}")
    } else {
        joined
    }
}

/// The directory a path is in, when the path names one.
fn parent_directory(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    let (parent, _) = trimmed.rsplit_once('/')?;
    if parent.is_empty() {
        return Some("/");
    }
    Some(parent)
}

/// Feeds the driver's asynchronous chunk callback into pebble's synchronous
/// sink.
fn adapt_output_sink(sink: ExecOutputSink) -> OutputSink {
    Arc::new(move |stream, chunk: Vec<u8>| {
        let stream = match stream {
            OutputStream::Stdout => ExecOutputStream::Stdout,
            OutputStream::Stderr => ExecOutputStream::Stderr,
        };
        sink(stream, &chunk);
        Box::pin(async { Ok(()) })
    })
}

/// A sandbox failure as pebble classifies it, keeping the driver cause.
fn environment_error(message: &str, error: crate::Error) -> EnvironmentError {
    let kind = match error.driver() {
        Some(sandbox_driver::Error::NotFound { .. }) => EnvironmentErrorKind::NotFound,
        Some(sandbox_driver::Error::Unsupported { .. }) => EnvironmentErrorKind::Unsupported,
        _ => EnvironmentErrorKind::Io,
    };
    EnvironmentError::with_source(kind, message, error)
}

#[cfg(test)]
mod tests {
    use pebble_coding_agent::test_support::EnvironmentContract;

    use super::*;
    use crate::local_sandbox;

    /// The run sandbox over the driver's Host provider, in a directory that
    /// goes away with the test.
    async fn host_environment() -> (tempfile::TempDir, RunSandbox) {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let sandbox = local_sandbox(directory.path().to_path_buf())
            .await
            .expect("a local sandbox");
        (directory, sandbox)
    }

    #[tokio::test]
    async fn host_files_satisfy_pebbles_environment_contract() {
        let (_directory, sandbox) = host_environment().await;
        EnvironmentContract::new(&sandbox, "contract")
            .verify_files()
            .await
            .expect("file contract");
    }

    #[tokio::test]
    async fn host_search_satisfies_pebbles_environment_contract() {
        let (_directory, sandbox) = host_environment().await;
        EnvironmentContract::new(&sandbox, "contract")
            .verify_search()
            .await
            .expect("search contract");
    }

    #[tokio::test]
    async fn host_commands_satisfy_pebbles_environment_contract() {
        let (_directory, sandbox) = host_environment().await;
        EnvironmentContract::new(&sandbox, "contract")
            .verify_commands()
            .await
            .expect("command contract");
    }

    #[tokio::test]
    async fn a_directory_listing_is_in_tree_order() {
        let (directory, sandbox) = host_environment().await;
        for name in ["foo/x.txt", "foo-bar/y.txt", "foo.txt"] {
            Environment::write_file(&sandbox, name, "content")
                .await
                .expect("fixture");
        }
        let names: Vec<String> = Environment::list_directory(&sandbox, ".", Some(2))
            .await
            .expect("listing")
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        assert_eq!(names, [
            "foo",
            "foo/x.txt",
            "foo-bar",
            "foo-bar/y.txt",
            "foo.txt"
        ]);
        drop(directory);
    }

    #[test]
    fn a_path_spelled_two_ways_is_one_path() {
        assert_eq!(normalize("/work//a/./b.txt"), "/work/a/b.txt");
        assert_eq!(parent_directory("/work/a/b.txt"), Some("/work/a"));
        assert_eq!(parent_directory("/b.txt"), Some("/"));
        assert_eq!(parent_directory("b.txt"), None);
    }
}
