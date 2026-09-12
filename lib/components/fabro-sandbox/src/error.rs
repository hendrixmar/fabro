use std::fmt::Write as _;

use fabro_util::error::{collect_causes, render_with_causes};

use crate::sandbox::{DEFAULT_EXEC_OUTPUT_TAIL_BYTES, redacted_output_tail};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),

    #[error("{message}")]
    Context {
        message: String,
        #[source]
        source:  Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("{message}")]
    AnyhowContext {
        message: String,
        #[source]
        source:  anyhow::Error,
    },

    /// A sandbox-driver failure: provider, transport, or an operation whose
    /// outcome is unknown. The driver's own variants stay reachable through
    /// [`Error::driver`] so callers can act on `Exec`, `Git`, and `NotFound`
    /// without string matching.
    #[error(transparent)]
    Driver(Box<sandbox_driver::Error>),
}

impl Error {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    pub fn context(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Context {
            message: message.into(),
            source:  Box::new(source),
        }
    }

    pub fn context_anyhow(message: impl Into<String>, source: anyhow::Error) -> Self {
        Self::AnyhowContext {
            message: message.into(),
            source,
        }
    }

    pub fn default_redacted_output_tail(&self) -> Option<fabro_types::ExecOutputTail> {
        default_redacted_output_tail(self)
    }

    pub fn causes(&self) -> Vec<String> {
        collect_causes(self)
    }

    /// The underlying sandbox-driver error, when this error carries one
    /// anywhere in its chain.
    pub fn driver(&self) -> Option<&sandbox_driver::Error> {
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(self);
        while let Some(err) = current {
            if let Some(Self::Driver(driver)) = err.downcast_ref::<Self>() {
                return Some(driver.as_ref());
            }
            if let Some(driver) = err.downcast_ref::<sandbox_driver::Error>() {
                return Some(driver);
            }
            current = err.source();
        }
        None
    }

    pub fn display_with_causes(&self) -> String {
        render_with_causes(&self.to_string(), &self.causes())
    }
}

impl From<sandbox_driver::Error> for Error {
    fn from(value: sandbox_driver::Error) -> Self {
        Self::Driver(Box::new(value))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn default_redacted_output_tail(
    err: &(dyn std::error::Error + 'static),
) -> Option<fabro_types::ExecOutputTail> {
    let mut current = Some(err);
    while let Some(err) = current {
        if let Some(Error::Driver(driver)) = err.downcast_ref::<Error>() {
            if let Some(tail) = driver_output_tail(driver) {
                return Some(tail);
            }
        }
        if let Some(driver) = err.downcast_ref::<sandbox_driver::Error>() {
            if let Some(tail) = driver_output_tail(driver) {
                return Some(tail);
            }
        }
        current = err.source();
    }
    None
}

/// The output a driver failure carries: a command that ran and failed, or
/// a git operation whose command output the driver kept as evidence.
fn driver_output_tail(error: &sandbox_driver::Error) -> Option<fabro_types::ExecOutputTail> {
    let failure = match error {
        sandbox_driver::Error::Exec(failure) => failure,
        sandbox_driver::Error::Git(git) => git.output()?,
        _ => return None,
    };
    redacted_output_tail(
        &String::from_utf8_lossy(failure.stdout()),
        &String::from_utf8_lossy(failure.stderr()),
        DEFAULT_EXEC_OUTPUT_TAIL_BYTES,
    )
}

pub fn display_for_log(err: &(dyn std::error::Error + 'static)) -> String {
    let mut rendered = render_with_causes(&err.to_string(), &collect_causes(err));
    if let Some(tail) = default_redacted_output_tail(err) {
        append_tail_for_log(
            &mut rendered,
            "stderr",
            tail.stderr.as_deref(),
            tail.stderr_truncated,
        );
        append_tail_for_log(
            &mut rendered,
            "stdout",
            tail.stdout.as_deref(),
            tail.stdout_truncated,
        );
    }
    rendered
}

fn append_tail_for_log(rendered: &mut String, stream: &str, tail: Option<&str>, truncated: bool) {
    let tail = tail.unwrap_or("");
    let _ = write!(
        rendered,
        "\n--- {stream} (truncated={truncated}, bytes={}) ---\n{tail}",
        tail.len()
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sandbox_driver::{ExecResult, Termination};

    use super::*;
    use crate::exec::ExecResultExt;

    const SECRET: &str = "ghs_xK9mZ2vL8nQ5rT1wY4bC7dF0gH3jE6pA";

    fn failed_push(stdout: &str, stderr: &str) -> Error {
        let mut result =
            ExecResult::new(Termination::Exited, Some(128), Duration::from_millis(210));
        result.stdout = stdout.as_bytes().to_vec();
        result.stderr = stderr.as_bytes().to_vec();
        result.into_exec_error("git push origin refs/heads/run")
    }

    fn leaky_stderr() -> String {
        format!(
            "fatal: unable to access 'https://x-access-token:{SECRET}@github.com/owner/repo/':\n\
             remote: Permission to owner/repo.git denied\n\
             identity ~/.ssh/id_rsa_work"
        )
    }

    #[test]
    fn exec_display_is_log_safe() {
        let error = failed_push("", &leaky_stderr());
        let rendered = error.to_string();

        assert_exec_rendering_is_safe(&rendered);
        assert!(rendered.contains("git push origin refs/heads/run"));
        assert!(rendered.contains("128"));
        assert!(rendered.contains("210 ms"));
    }

    #[test]
    fn display_with_causes_does_not_reintroduce_raw_exec_output() {
        let exec_error = failed_push(&format!("stdout secret {SECRET}"), &leaky_stderr());
        let error = Error::context("metadata push failed", exec_error);
        let rendered = error.display_with_causes();

        assert_exec_rendering_is_safe(&rendered);
        assert!(rendered.contains("metadata push failed"));
        assert!(rendered.contains("git push origin refs/heads/run"));
    }

    #[test]
    fn the_driver_error_is_reachable_through_the_context_chain() {
        let error = Error::context("metadata push failed", failed_push("", "boom"));

        let Some(sandbox_driver::Error::Exec(failure)) = error.driver() else {
            panic!("expected an exec failure, got {error:?}");
        };
        assert_eq!(failure.label(), "git push origin refs/heads/run");
        assert_eq!(failure.exit_code(), Some(128));
        assert_eq!(failure.termination(), Termination::Exited);
        assert!(Error::message("plain").driver().is_none());
    }

    #[test]
    fn display_for_log_walks_context_chain_and_emits_tail() {
        let exec_error = failed_push("last stdout line", "last stderr line");
        let error = Error::context("metadata push failed", exec_error);

        let rendered = display_for_log(&error);

        assert!(rendered.contains("metadata push failed"));
        assert!(rendered.contains("git push origin refs/heads/run"));
        assert!(rendered.contains("--- stderr (truncated=false, bytes=16) ---"));
        assert!(rendered.contains("last stderr line"));
        assert!(rendered.contains("--- stdout (truncated=false, bytes=16) ---"));
        assert!(rendered.contains("last stdout line"));
    }

    #[test]
    fn display_for_log_redacts_secrets() {
        let error = failed_push(
            &format!("stdout secret {SECRET}"),
            &format!("stderr secret {SECRET}"),
        );

        let rendered = display_for_log(&error);

        assert!(
            !rendered.contains(SECRET),
            "log rendering leaked raw secret: {rendered}"
        );
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn display_for_log_for_non_exec_error_returns_chain_only() {
        let error = Error::context("outer failure", std::io::Error::other("leaf failure"));

        let rendered = display_for_log(&error);

        assert_eq!(rendered, "outer failure\n  caused by: leaf failure");
        assert!(!rendered.contains("--- stderr"));
        assert!(!rendered.contains("--- stdout"));
    }

    fn assert_exec_rendering_is_safe(rendered: &str) {
        for forbidden in [
            "fatal:",
            "remote:",
            "x-access-token",
            SECRET,
            "~/.ssh",
            "id_rsa_work",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "Display leaked {forbidden:?}: {rendered}"
            );
        }
    }

    #[test]
    fn exec_error_exposes_default_redacted_output_tail() {
        let error = failed_push("last stdout line", &format!("stderr secret {SECRET}"));

        let tail = error.default_redacted_output_tail().expect("tail present");
        assert_eq!(tail.stdout.as_deref(), Some("last stdout line"));
        assert!(
            tail.stderr
                .as_deref()
                .expect("stderr tail")
                .contains("REDACTED")
        );
    }

    #[test]
    fn free_tail_helper_walks_context_chain() {
        let exec_error = failed_push("last stdout line", "last stderr line");
        let error = Error::context("metadata push failed", exec_error);

        let tail = default_redacted_output_tail(&error).expect("tail present");

        assert_eq!(tail.stdout.as_deref(), Some("last stdout line"));
        assert_eq!(tail.stderr.as_deref(), Some("last stderr line"));
    }
}
