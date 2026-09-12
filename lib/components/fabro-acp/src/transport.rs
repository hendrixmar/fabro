use std::collections::HashMap;
use std::io::Result as IoResult;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::util::internal_error;
use agent_client_protocol::{
    Agent, Client, ConnectTo, Error as ProtocolError, Lines, Result as AcpProtocolResult,
};
use fabro_sandbox::{
    DEFAULT_EXEC_OUTPUT_TAIL_BYTES, Error as SandboxError, Result as SandboxResult, RunSandbox,
    StderrTail, StdioProcessHandle, Termination, command_termination, program_exit_code,
};
use fabro_types::ExecOutputTail;
use futures::io::BufReader;
use futures::sink::unfold;
use futures::{AsyncBufReadExt, AsyncWriteExt, Stream};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::timeout;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::command::AcpProcessSpec;
use crate::error::AcpProcessExit;

const CLEAN_EXIT_PROTOCOL_GRACE: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub(crate) struct TransportState {
    handle:        Arc<TokioMutex<Option<Arc<dyn StdioProcessHandle>>>>,
    stderr:        Arc<TokioMutex<Option<StderrTail>>>,
    startup_error: Arc<TokioMutex<Option<SandboxError>>>,
    process_exit:  Arc<TokioMutex<Option<AcpProcessExit>>>,
}

impl TransportState {
    pub(crate) fn new() -> Self {
        Self {
            handle:        Arc::new(TokioMutex::new(None)),
            stderr:        Arc::new(TokioMutex::new(None)),
            startup_error: Arc::new(TokioMutex::new(None)),
            process_exit:  Arc::new(TokioMutex::new(None)),
        }
    }

    async fn set_process(&self, handle: Arc<dyn StdioProcessHandle>, stderr: StderrTail) {
        *self.handle.lock().await = Some(handle);
        *self.stderr.lock().await = Some(stderr);
    }

    async fn set_startup_error(&self, error: SandboxError) {
        *self.startup_error.lock().await = Some(error);
    }

    async fn set_process_exit(
        &self,
        termination: Termination,
        exit_code: Option<i32>,
        stderr: &str,
    ) {
        *self.process_exit.lock().await = Some(AcpProcessExit {
            termination:      command_termination(termination),
            exit_code:        program_exit_code(termination, exit_code),
            exec_output_tail: redacted_stderr_tail(stderr),
        });
    }

    pub(crate) async fn take_startup_error(&self) -> Option<SandboxError> {
        self.startup_error.lock().await.take()
    }

    pub(crate) async fn take_process_exit(&self) -> Option<AcpProcessExit> {
        self.process_exit.lock().await.take()
    }

    pub(crate) async fn terminate(&self) -> SandboxResult<()> {
        if let Some(handle) = self.handle.lock().await.as_ref().cloned() {
            handle.terminate().await;
        }
        Ok(())
    }

    pub(crate) async fn stderr_tail(&self) -> String {
        if let Some(stderr) = self.stderr.lock().await.as_ref().cloned() {
            return stderr.to_string_lossy();
        }
        String::new()
    }

    pub(crate) async fn exec_output_tail(&self) -> Option<ExecOutputTail> {
        let stderr = self.stderr_tail().await;
        redacted_stderr_tail(&stderr)
    }
}

pub(crate) struct SandboxAcpTransport {
    command: AcpProcessSpec,
    cwd:     String,
    env:     HashMap<String, String>,
    sandbox: Arc<RunSandbox>,
    state:   TransportState,
}

impl SandboxAcpTransport {
    pub(crate) fn new(
        command: AcpProcessSpec,
        cwd: String,
        env: HashMap<String, String>,
        sandbox: Arc<RunSandbox>,
        state: TransportState,
    ) -> Self {
        Self {
            command,
            cwd,
            env,
            sandbox,
            state,
        }
    }
}

impl ConnectTo<Client> for SandboxAcpTransport {
    async fn connect_to(self, client: impl ConnectTo<Agent>) -> AcpProtocolResult<()> {
        let mut env = self.command.env().clone();
        env.extend(self.env);

        let process = match self
            .sandbox
            .spawn_stdio_process(
                &self.command.to_shell_command(),
                Some(&self.cwd),
                Some(&env),
            )
            .await
        {
            Ok(process) => process,
            Err(error) => {
                self.state.set_startup_error(error).await;
                return Err(internal_error("ACP process failed to start"));
            }
        };

        let handle: Arc<dyn StdioProcessHandle> = Arc::from(process.handle);
        let stderr = process.stderr_tail.clone();
        self.state
            .set_process(Arc::clone(&handle), stderr.clone())
            .await;

        let incoming_lines = Box::pin(BufReader::new(process.stdout.compat()).lines())
            as Pin<Box<dyn Stream<Item = IoResult<String>> + Send>>;
        let outgoing_sink = Box::pin(unfold(
            process.stdin.compat_write(),
            async move |mut writer, line: String| {
                let mut bytes = line.into_bytes();
                bytes.push(b'\n');
                writer.write_all(&bytes).await?;
                Ok::<_, std::io::Error>(writer)
            },
        ));

        let mut protocol = Box::pin(agent_client_protocol::ConnectTo::<Client>::connect_to(
            Lines::new(outgoing_sink, incoming_lines),
            client,
        ));
        tokio::select! {
            result = &mut protocol => {
                handle.terminate().await;
                let _ = timeout(Duration::from_millis(500), handle.wait()).await;
                result
            }
            (termination, exit_code) = handle.wait() => {
                let stderr = stderr.to_string_lossy();
                if termination == Termination::Exited && exit_code == Some(0) {
                    // Stdio agents commonly exit immediately after writing their final response.
                    // Process wait can observe that exit before the line reader drains stdout.
                    if let Ok(result) = timeout(CLEAN_EXIT_PROTOCOL_GRACE, &mut protocol).await {
                        return result;
                    }
                }
                self.state.set_process_exit(termination, exit_code, &stderr).await;
                Err(process_exited_before_protocol_completed())
            }
        }
    }
}

fn redacted_stderr_tail(stderr: &str) -> Option<ExecOutputTail> {
    fabro_sandbox::redacted_output_tail("", stderr, DEFAULT_EXEC_OUTPUT_TAIL_BYTES)
}

fn process_exited_before_protocol_completed() -> ProtocolError {
    internal_error("ACP process exited before protocol completed")
}
