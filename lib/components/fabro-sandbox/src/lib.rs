pub mod environment;
pub mod error;
pub mod provider;
pub mod sandbox;
pub mod sandbox_spec;

mod clone_source;

mod git_policy;

mod managed_labels;

mod credentials;

pub mod details;

pub mod driver;
pub mod driver_sandbox;

pub mod exec;
mod pebble_environment;

pub mod reconnect;
mod redact;

mod clone;
pub mod docker;
pub mod provider_sandbox;

pub mod daytona;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use details::sandbox_details;
pub use docker::check_docker_daemon;
pub use driver::{DaytonaCredentials, ProviderAccess};
pub use driver_sandbox::RunSandbox;
pub use environment::{CloneRequest, sandbox_spec_for_environment};
pub use error::{Error, Result, default_redacted_output_tail, display_for_log};
pub use exec::{
    DEFAULT_RETAINED_OUTPUT_BYTES, DEFAULT_STOP_GRACE, ExecResultExt, SandboxExec,
    command_termination, program_exit_code,
};
pub use fabro_github::token_source::{
    InstallationTokenSource, ResolvedToken, TokenProvenance, TokenSnapshot,
};
pub use fabro_types::{RunSandboxInstance, SandboxProviderKind};
pub use git_policy::{
    GitRetryReason, checkpoint_push_policy, publish_push_policy, repository_probe_policy,
    retry_git_messages, transient_git_failure,
};
pub use provider::{SandboxInventory, SandboxLookupError};
pub use provider_sandbox::{attach_provider_sandbox, local_sandbox, provider_sandbox};
pub use reconnect::{open_terminal_for_run, reconnect_for_run};
pub use redact::SecretRedactor;
pub use sandbox::{
    DEFAULT_EXEC_OUTPUT_TAIL_BYTES, GitRunInfo, GitSetupIntent, PushAttempt, PushError, PushReport,
    SandboxFile, SandboxWorkspaceLayout, redacted_output_tail, setup_git,
};
/// Driver types a run sandbox speaks: what a command is and how it ended,
/// what the file and search operations return, and what an environment
/// asks of a sandbox. Re-exported so consumers need no direct driver
/// dependency.
pub use sandbox_driver::{
    CaptureStats, DirEntry, ExecControls, ExecFailure, ExecResult, ExecSpec, ExecStreamingResult,
    FileKind, GitRetryPolicy, GrepMatch, GrepOptions, LifecycleTimers, NetworkPolicy, OutputSink,
    OutputStream, PtySession, PtySize, Resources, SandboxSource, SandboxSpec as DriverSpec,
    StderrTail, StdioProcess, StdioProcessHandle, Termination, TransportError, WalkOptions,
};
pub use sandbox_spec::SandboxSpec;
