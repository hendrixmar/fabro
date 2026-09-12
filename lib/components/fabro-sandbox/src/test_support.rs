//! Test doubles for fabro's sandbox layer.
//!
//! [`MockSandbox`] is a configuration over the sandbox driver's scripted
//! double: a test writes down the files, the command answer, and the
//! failures it wants, and takes a [`RunSandbox`] from it. What the code
//! under test ran or wrote is read back from the driver double itself,
//! through [`MockSandbox::driver`]; the few accessors here convert what a
//! spec records into the shape fabro's tests assert on. Nothing here fakes
//! fabro's own logic; every call goes through the real `RunSandbox` and
//! fabro's exec policy, down to the scripted driver.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use fabro_types::SandboxProviderKind;
use sandbox_driver::{
    ExecResult, GrepMatch, PlatformInfo, SandboxState, StderrTail, Termination, WalkedFile,
};
use sandbox_driver_host::HostProvider;
pub use sandbox_driver_testing::{
    ScriptedExec, ScriptedProvider, ScriptedSandbox, ScriptedStdioProcess,
};
use tokio::io::DuplexStream;

use crate::driver::ConnectedProvider;
use crate::driver_sandbox::RunSandbox;
use crate::managed_labels::{MANAGED_LABEL, MANAGED_LABEL_VALUE};
use crate::sandbox::SandboxFile;

/// The id a run record carries for a local sandbox at `working_directory`,
/// as the Host provider derives it from the canonical path. A record a test
/// writes by hand reconnects the way one fabro wrote would. The directory
/// must exist.
pub async fn local_sandbox_id(working_directory: &Path) -> String {
    HostProvider::directory_id(working_directory)
        .await
        .unwrap_or_else(|| {
            panic!(
                "no local sandbox id for {}: the directory must exist",
                working_directory.display()
            )
        })
        .to_string()
}

/// A driver [`ExecResult`] with the given streams, for scripting a mock
/// sandbox's answers.
#[must_use]
pub fn exec_result(
    stdout: &str,
    stderr: &str,
    exit_code: Option<i32>,
    termination: Termination,
    duration_ms: u64,
) -> ExecResult {
    let mut result = ExecResult::new(termination, exit_code, Duration::from_millis(duration_ms));
    result.stdout = stdout.as_bytes().to_vec();
    result.stderr = stderr.as_bytes().to_vec();
    result
}

// --- MockSandbox ---

/// What a test wants its sandbox to be, and what the code under test did
/// with it.
///
/// Build it with a struct literal over [`MockSandbox::default`] (or
/// [`MockSandbox::linux`]), then take the run sandbox with
/// [`MockSandbox::sandbox`]. Every command answers with `exec_result`
/// unless `exec_error` is set, in which case every command fails as a
/// transport error. Files seed an in-memory filesystem under
/// `working_dir`; absolute paths are kept as given.
pub struct MockSandbox {
    pub files:               HashMap<String, String>,
    pub exec_result:         ExecResult,
    /// Fails every command before any process runs, so callers see a
    /// transport error rather than an `ExecResult`.
    pub exec_error:          Option<String>,
    pub working_dir:         &'static str,
    /// The run-scoped scratch directory the sandbox reports, outside any
    /// checkout; `None` models a provider without one.
    pub runtime_dir:         Option<&'static str>,
    pub platform_str:        &'static str,
    pub os_version_str:      String,
    /// Fails `activate` after the sandbox is built, as a sandbox whose
    /// Bash contract broke would.
    pub activate_error:      Option<String>,
    pub stdio_process:       Option<MockStdioProcess>,
    pub stdio_process_error: Option<String>,
    /// Lines every grep returns, as `path:line:content`.
    pub grep_results:        Vec<String>,
    /// Files returned by `walk_files` instead of the seeded files, before
    /// traversal-root and exclusion filtering.
    pub walk_files:          Vec<SandboxFile>,
    pub walk_files_error:    Option<String>,
    /// Reported by streaming execution. Set to `false` to model a provider
    /// that cannot separate stdout from stderr.
    pub streams_separated:   bool,
    /// The sandbox once built. Public only so `..Default::default()` works
    /// from other crates; leave it at its default.
    pub built:               OnceLock<Built>,
}

/// The lazily built sandbox and its scripted driver.
pub struct Built {
    run:    Arc<RunSandbox>,
    driver: Arc<ScriptedSandbox>,
}

impl Default for MockSandbox {
    fn default() -> Self {
        Self {
            files:               HashMap::new(),
            exec_result:         {
                let mut result =
                    ExecResult::new(Termination::Exited, Some(0), Duration::from_millis(10));
                result.stdout = b"mock output".to_vec();
                result
            },
            exec_error:          None,
            working_dir:         "/work",
            runtime_dir:         None,
            platform_str:        "darwin",
            os_version_str:      "Darwin 24.0.0".into(),
            activate_error:      None,
            stdio_process:       None,
            stdio_process_error: None,
            grep_results:        Vec::new(),
            walk_files:          Vec::new(),
            walk_files_error:    None,
            streams_separated:   true,
            built:               OnceLock::new(),
        }
    }
}

impl MockSandbox {
    pub fn linux() -> Self {
        Self {
            working_dir: "/home/test",
            platform_str: "linux",
            os_version_str: "Linux 6.1.0".into(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_walk_files(mut self, files: Vec<SandboxFile>) -> Self {
        self.walk_files = files;
        self
    }

    #[must_use]
    pub fn with_walk_files_error(mut self, error: impl Into<String>) -> Self {
        self.walk_files_error = Some(error.into());
        self
    }

    #[must_use]
    pub fn with_activate_error(mut self, error: impl Into<String>) -> Self {
        self.activate_error = Some(error.into());
        self
    }

    /// The run sandbox this configuration describes, built once: repeated
    /// calls return the same sandbox over the same recorder.
    pub fn sandbox(&self) -> Arc<RunSandbox> {
        Arc::clone(&self.built().run)
    }

    /// The scripted driver double behind [`MockSandbox::sandbox`], for
    /// scripting beyond what the fields express.
    pub fn driver(&self) -> Arc<ScriptedSandbox> {
        Arc::clone(&self.built().driver)
    }

    /// Answers commands by their Bash source, ahead of the queue and
    /// `exec_result`: a responder that returns `Some` decides the result,
    /// `None` falls through. For tests that interleave different commands
    /// and want each answered by what it is rather than by its position.
    pub fn respond_with(
        &self,
        responder: impl Fn(&str) -> Option<ExecResult> + Send + Sync + 'static,
    ) -> &Self {
        self.driver().scripted_exec().respond_with(move |spec| {
            let command = spec.args.last().map(String::as_str).unwrap_or_default();
            responder(command)
        });
        self
    }

    fn built(&self) -> &Built {
        self.built.get_or_init(|| {
            let driver = Arc::new(self.build_driver());
            // The kind is nominal for exec: the explicit environment reaches
            // the scripted driver as the caller composed it on every provider.
            let run = RunSandbox::new_with_platform(
                SandboxProviderKind::DOCKER,
                Arc::clone(&driver) as Arc<dyn sandbox_driver::Sandbox>,
                self.platform_str,
                self.os_version_str.clone(),
            );
            Built {
                run: Arc::new(run),
                driver,
            }
        })
    }

    fn build_driver(&self) -> ScriptedSandbox {
        let mut driver =
            ScriptedSandbox::with_id_and_working_dir("mock-sandbox", self.working_dir).platform(
                PlatformInfo::new(self.platform_str, "x86_64", self.os_version_str.clone()),
            );
        if let Some(directory) = self.runtime_dir {
            driver = driver.runtime_directory(directory);
        }
        if let Some(message) = &self.activate_error {
            // A stopped sandbox whose provider cannot start it.
            driver = driver
                .state(SandboxState::Stopped)
                .start_error(message.clone());
        }
        for (path, content) in &self.files {
            driver = driver.file(path, content);
        }
        let exec = driver.scripted_exec();
        match &self.exec_error {
            Some(message) => exec.fail_by_default(message.clone()),
            None => exec.set_default(self.exec_result.clone()),
        };
        exec.set_streams_separated(self.streams_separated);
        if let Some(message) = &self.stdio_process_error {
            exec.set_stdio_error(message.clone());
        }
        if let Some(process) = self.stdio_process.as_ref() {
            if let Some(scripted) = process.take() {
                exec.set_stdio_process(scripted);
            }
        }
        let search = driver.scripted_search();
        search.set_grep(
            self.grep_results
                .iter()
                .map(|line| {
                    let mut parts = line.splitn(3, ':');
                    let path = parts.next().unwrap_or_default();
                    let line_number = parts.next().and_then(|n| n.parse().ok()).unwrap_or(0);
                    GrepMatch::new(path, line_number, parts.next().unwrap_or_default())
                })
                .collect(),
        );
        if let Some(message) = &self.walk_files_error {
            search.set_walk_error(message.clone());
        } else if !self.walk_files.is_empty() {
            search.set_walk(
                self.walk_files
                    .iter()
                    .map(|file| WalkedFile::new(file.relative_path.clone(), Some(file.size)))
                    .collect(),
            );
        }
        driver
    }

    fn recorded(&self) -> Vec<sandbox_driver::ExecSpec> {
        self.built
            .get()
            .map(|built| built.driver.scripted_exec().recorded())
            .unwrap_or_default()
    }

    /// The last command's Bash source. Every command, in order, is
    /// `driver().scripted_exec().commands()`.
    pub fn captured_command(&self) -> Option<String> {
        self.recorded()
            .last()
            .and_then(|spec| spec.args.last().cloned())
    }

    /// The last command's timeout in milliseconds.
    pub fn captured_timeout(&self) -> Option<u64> {
        self.recorded()
            .last()
            .and_then(|spec| spec.timeout)
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX))
    }

    /// The timeout of every command in milliseconds, in order.
    pub fn captured_timeouts(&self) -> Vec<u64> {
        self.recorded()
            .iter()
            .filter_map(|spec| spec.timeout)
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX))
            .collect()
    }

    /// The explicit variables of the last command as the caller passed them.
    /// The driver's Bash helper records its own `BASH_ENV` blank on the
    /// spec; that is not the caller's.
    pub fn captured_env_vars(&self) -> Option<HashMap<String, String>> {
        self.recorded().last().map(|spec| {
            spec.env
                .iter()
                .filter(|(key, _)| key.as_str() != sandbox_driver::BASH_ENV_VAR)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
    }

    /// Every file written so far as `(path, content)`, in order.
    pub fn written_files(&self) -> Vec<(String, String)> {
        self.built
            .get()
            .map(|built| {
                built
                    .driver
                    .memory_fs()
                    .writes()
                    .into_iter()
                    .map(|(path, bytes)| (path, String::from_utf8_lossy(&bytes).into_owned()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

// --- MockStdioProcess ---

/// A stdio process a test drives, over the driver's scripted process.
///
/// The driver closure receives the process's end of standard input, its
/// end of standard output, and the rolling stderr tail the process reports.
pub struct MockStdioProcess {
    inner: std::sync::Mutex<Option<ScriptedStdioProcess>>,
}

impl MockStdioProcess {
    pub fn new(
        driver: impl FnOnce(DuplexStream, DuplexStream, StderrTail) + Send + 'static,
    ) -> Self {
        Self {
            inner: std::sync::Mutex::new(Some(ScriptedStdioProcess::new(driver))),
        }
    }

    #[must_use]
    pub fn with_exit_code(self, exit_code: Option<i32>) -> Self {
        let inner = self.inner.lock().expect("stdio process").take();
        Self {
            inner: std::sync::Mutex::new(inner.map(|process| process.exit_code(exit_code))),
        }
    }

    #[must_use]
    pub fn with_wait_delay(self, wait_delay: Duration) -> Self {
        let inner = self.inner.lock().expect("stdio process").take();
        Self {
            inner: std::sync::Mutex::new(inner.map(|process| process.wait_delay(wait_delay))),
        }
    }

    fn take(&self) -> Option<ScriptedStdioProcess> {
        self.inner.lock().expect("stdio process").take()
    }
}

// --- Inventory doubles ---

/// A running scripted sandbox carrying fabro's managed label, so an owned
/// inventory lists it and attaches to it.
#[must_use]
pub fn managed_scripted_sandbox(id: &str) -> Arc<ScriptedSandbox> {
    Arc::new(
        ScriptedSandbox::with_id_and_working_dir(id, "/work")
            .state(SandboxState::Running)
            .label(MANAGED_LABEL, MANAGED_LABEL_VALUE),
    )
}

/// A connected inventory provider of `kind` holding `sandboxes`, over the
/// driver's scripted provider.
#[must_use]
pub fn scripted_inventory_provider(
    kind: SandboxProviderKind,
    sandboxes: Vec<Arc<ScriptedSandbox>>,
) -> ConnectedProvider {
    let provider = ScriptedProvider::new(kind.as_str());
    for sandbox in sandboxes {
        provider.register(sandbox);
    }
    ConnectedProvider {
        kind,
        provider: Arc::new(provider),
    }
}
