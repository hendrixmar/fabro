//! Workflow adapter for ACP-backed LLM stages.

use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use fabro_acp::{
    AcpCommandError, AcpControlHandle, AcpError, AcpLiveControl, AcpProcessSpec, AcpRunRequest,
    render_stop_reason,
};
use fabro_github::token_source::REFRESH_MARGIN;
use fabro_graphviz::graph::Node;
use fabro_sandbox::{RunSandbox, TokenSnapshot};
use fabro_static::EnvVars;
use fabro_types::{AgentBackend, SessionCapability, StageId, StageTiming};
use fabro_util::time::elapsed_ms;
use pebble_coding_agent::events::{Actor, CodingAgentEvent, CodingEvent};
use pebble_coding_agent::steering::SteerableSession;
use pebble_coding_agent::tools::{StaticEnvProvider, ToolEnvProvider};
use pebble_coding_agent::{SteeringMessage, SteeringOutcome};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::super::agent::{CodergenBackend, CodergenResult, CodergenRunRequest, OneShotRequest};
use super::activation_lease::{ActivationLease, ActivationLeaseOptions};
use super::changed_files;
use crate::error::Error;
use crate::event::{Emitter, Event, RunNoticeCode, RunNoticeLevel, StageScope};
use crate::handler::NodeTimeoutPolicy;
use crate::steering_hub::SteeringHub;

/// Default refresh-ahead interval — comfortably under the ~60-min GitHub App
/// installation-token TTL. Used as the loop cadence when a tick reports no
/// managed credentials; ticks that see a real token reschedule from its
/// expiry instead.
const REFRESH_INTERVAL_DEFAULT: Duration = Duration::from_mins(45);
/// Floor for expiry-driven rescheduling, so a token already inside the cache
/// margin cannot pin the loop in a hot cycle.
const REFRESH_RESCHEDULE_FLOOR: Duration = Duration::from_secs(30);
/// Upper bound on a single credential refresh (token mint + rewriting the
/// checkout's credential store). The turn-entry refresh runs before the ACP
/// process spawns
/// and the ACP node uses `NodeTimeoutPolicy::HandlerManaged`, so without this
/// bound a stalled GitHub API call would hang node entry indefinitely.
const REFRESH_MINT_TIMEOUT: Duration = Duration::from_secs(30);

/// Aborts the wrapped task when dropped, bounding the refresh-ahead loop to the
/// lifetime of a single ACP turn.
struct AbortOnDrop(JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Process-env lookup facade for the `FABRO_PUSH_CRED_REFRESH_*` tunables,
/// isolated so the single disallowed-methods exception is documented in one
/// place. Variable names come from [`EnvVars`].
#[expect(
    clippy::disallowed_methods,
    reason = "Documented process-env facade for the FABRO_PUSH_CRED_REFRESH_* tunables; names come from fabro_static::EnvVars."
)]
fn refresh_env(name: &str) -> Option<String> {
    env::var(name).ok()
}

/// Whether the push-credential refresh feature is enabled. Default ON; disabled
/// by a falsy value (empty / `0` / `false` / `off` / `no`, case-insensitive),
/// matching the repo's env-flag convention.
fn parse_refresh_enabled(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("" | "0" | "false" | "off" | "no")
    )
}

/// Parse the refresh-ahead loop interval. `None` disables the loop (an
/// explicit `0`). Unset/empty or an unparsable value falls back to the default.
fn parse_refresh_interval(raw: Option<&str>) -> Option<Duration> {
    match raw.map(str::trim) {
        None | Some("") => Some(REFRESH_INTERVAL_DEFAULT),
        Some(s) => match s.parse::<u64>() {
            Ok(0) => None,
            Ok(secs) => Some(Duration::from_secs(secs)),
            Err(_) => {
                tracing::warn!(
                    value = %s,
                    "invalid FABRO_PUSH_CRED_REFRESH_INTERVAL_SECONDS; using default"
                );
                Some(REFRESH_INTERVAL_DEFAULT)
            }
        },
    }
}

fn push_cred_refresh_enabled() -> bool {
    parse_refresh_enabled(refresh_env(EnvVars::FABRO_PUSH_CRED_REFRESH_AHEAD).as_deref())
}

fn push_cred_refresh_interval() -> Option<Duration> {
    parse_refresh_interval(
        refresh_env(EnvVars::FABRO_PUSH_CRED_REFRESH_INTERVAL_SECONDS).as_deref(),
    )
}

/// Delay until the next refresh-ahead tick after a successful refresh.
///
/// With a cached token source, a fixed interval is unsafe: a tick landing
/// just outside the cache margin returns a reused token, and a fixed
/// 45-minute sleep would leave the embedded token expired until the next
/// tick. Schedule from the token's own `expires_at` instead: wake when the
/// cache margin opens, so that tick re-mints. `None` disables the loop —
/// static credentials cannot be re-minted by waiting, and a sandbox without
/// managed credentials has nothing to renew.
fn next_refresh_delay(token: Option<&TokenSnapshot>) -> Option<Duration> {
    let expires_at = token?.expires_at()?;
    let margin = chrono::Duration::from_std(REFRESH_MARGIN).unwrap_or(chrono::Duration::MAX);
    let until_margin = ((expires_at - margin) - chrono::Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);
    Some(until_margin.max(REFRESH_RESCHEDULE_FLOOR))
}

/// Background loop that keeps the checkout's git credentials fresh for the
/// duration of one ACP turn, so a single turn that outlives the
/// installation-token TTL still pushes with a fresh token. Bounded by
/// `cancel` (the drop-guard cancels it at turn end). Each successful tick
/// reschedules from the installed token's expiry ([`next_refresh_delay`]); a
/// failed or timed-out tick retries after a shorter delay so a transient
/// error does not leave a longer-than-interval window with an expired token.
async fn refresh_ahead_loop<Fut>(
    refresh: impl Fn() -> Fut + Send,
    cancel: CancellationToken,
    interval: Duration,
    initial_delay: Duration,
) where
    Fut: Future<Output = fabro_sandbox::Result<Option<TokenSnapshot>>> + Send,
{
    let retry_delay = interval.min(Duration::from_mins(1));
    let mut delay = initial_delay;
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = sleep(delay) => {
                match timeout(REFRESH_MINT_TIMEOUT, refresh()).await {
                    Ok(Ok(token)) => {
                        match &token {
                            Some(token) => {
                                tracing::info!(
                                    generation = token.generation,
                                    "refresh-ahead renewed the checkout's git credentials mid-turn"
                                );
                            }
                            None => {
                                tracing::debug!(
                                    "refresh-ahead tick: no managed git credentials to renew"
                                );
                            }
                        }
                        if let Some(next) = next_refresh_delay(token.as_ref()) {
                            delay = next;
                        } else {
                            tracing::debug!(
                                "refresh-ahead loop stopped: static credentials cannot be re-minted"
                            );
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(
                            error = %fabro_sandbox::display_for_log(&e),
                            "refresh-ahead mid-turn refresh failed; retrying sooner"
                        );
                        delay = retry_delay;
                    }
                    Err(_elapsed) => {
                        tracing::warn!(
                            timeout_secs = REFRESH_MINT_TIMEOUT.as_secs(),
                            "refresh-ahead mid-turn refresh timed out; retrying sooner"
                        );
                        delay = retry_delay;
                    }
                }
            }
        }
    }
}

pub struct AgentAcpBackend {
    tool_env:                     Option<Arc<dyn ToolEnvProvider>>,
    github_token_refresh_managed: bool,
    steering_hub:                 Option<Arc<SteeringHub>>,
}

impl AgentAcpBackend {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tool_env:                     None,
            github_token_refresh_managed: false,
            steering_hub:                 None,
        }
    }

    #[must_use]
    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.tool_env = Some(Arc::new(StaticEnvProvider(env)));
        self
    }

    #[must_use]
    pub fn with_tool_env_provider(
        mut self,
        provider: Arc<dyn ToolEnvProvider>,
        github_token_refresh_managed: bool,
    ) -> Self {
        self.tool_env = Some(provider);
        self.github_token_refresh_managed = github_token_refresh_managed;
        self
    }

    #[must_use]
    pub fn with_steering_hub(mut self, steering_hub: Arc<SteeringHub>) -> Self {
        self.steering_hub = Some(steering_hub);
        self
    }

    async fn run_turn(
        &self,
        node: &Node,
        prompt: String,
        emitter: &Arc<Emitter>,
        stage_scope: &StageScope,
        sandbox: &Arc<RunSandbox>,
        cancel_token: CancellationToken,
    ) -> Result<CodergenResult, Error> {
        let process_spec = resolve_acp_process_spec(node)?;
        let config_name = process_spec.name().map(str::to_string);
        let launch_env = self.resolve_launch_env(emitter).await?;
        let on_activity = {
            let emitter = Arc::clone(emitter);
            Arc::new(move || emitter.touch()) as Arc<dyn Fn() + Send + Sync>
        };
        let command_display = process_spec.to_string();
        emitter.emit_scoped(
            &Event::AgentAcpStarted {
                node_id:     node.id.clone(),
                visit:       stage_scope.visit,
                command:     command_display,
                config_name: config_name.clone(),
            },
            stage_scope,
        );

        let control_handle = AcpControlHandle::new();
        let activation_session_id = format!("acp-{}", uuid::Uuid::new_v4());
        let activation_lease = self.activate_control_session(
            &control_handle,
            &activation_session_id,
            node,
            stage_scope,
            emitter,
            config_name.as_deref(),
        )?;
        let lease_for_completion = Arc::new(Mutex::new(activation_lease));
        let on_natural_completion = self.steering_hub.as_ref().map(|_| {
            let lease = Arc::clone(&lease_for_completion);
            Arc::new(move || {
                let mut lease = lease.lock().expect("ACP activation lease lock poisoned");
                let Some(active_lease) = lease.as_ref() else {
                    return true;
                };
                if active_lease.release_if_idle() {
                    lease.take();
                    true
                } else {
                    false
                }
            }) as Arc<dyn Fn() -> bool + Send + Sync>
        });
        let on_steer_prompt = self.steering_hub.as_ref().map(|_| {
            let emitter = Arc::clone(emitter);
            let stage_scope = stage_scope.clone();
            let node_id = node.id.clone();
            let session_id = activation_session_id.clone();
            Arc::new(move |text: String, actor: Option<Actor>| {
                emitter.emit_scoped(
                    &Event::Agent {
                        stage: node_id.clone(),
                        visit: stage_scope.visit,
                        event: CodingAgentEvent::new(
                            session_id.clone(),
                            CodingEvent::SteeringInjected {
                                text,
                                content: None,
                                actor,
                            },
                            std::time::SystemTime::now(),
                        ),
                    },
                    &stage_scope,
                );
            }) as Arc<dyn Fn(String, Option<Actor>) + Send + Sync>
        });

        // Refresh before launch for early pushes. Schedule later refreshes from
        // token expiry so the loop cannot sleep past the cache margin.
        let refresh_enabled = push_cred_refresh_enabled();
        let refresh_interval = refresh_enabled.then(push_cred_refresh_interval).flatten();
        let refresh_schedule = if refresh_enabled {
            match timeout(REFRESH_MINT_TIMEOUT, sandbox.refresh_ambient_credentials()).await {
                Ok(Ok(token)) => {
                    if let Some(token) = &token {
                        tracing::debug!(
                            generation = token.generation,
                            "refreshed the checkout's git credentials at ACP turn entry"
                        );
                    }
                    refresh_interval.zip(next_refresh_delay(token.as_ref()))
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        error = %fabro_sandbox::display_for_log(&e),
                        "node-entry push-credential refresh failed (non-fatal)"
                    );
                    refresh_interval
                        .map(|interval| (interval, interval.min(Duration::from_mins(1))))
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        timeout_secs = REFRESH_MINT_TIMEOUT.as_secs(),
                        "node-entry push-credential refresh timed out (non-fatal)"
                    );
                    refresh_interval
                        .map(|interval| (interval, interval.min(Duration::from_mins(1))))
                }
            }
        } else {
            None
        };
        let _refresh_ahead_guard: Option<AbortOnDrop> =
            refresh_schedule.map(|(interval, initial_delay)| {
                let sandbox = Arc::clone(sandbox);
                AbortOnDrop(tokio::spawn(refresh_ahead_loop(
                    move || {
                        let sandbox = Arc::clone(&sandbox);
                        async move { sandbox.refresh_ambient_credentials().await }
                    },
                    cancel_token.child_token(),
                    interval,
                    initial_delay,
                )))
            });

        let files_before = changed_files::detect_changed_files(sandbox).await;
        let launch_start = std::time::Instant::now();
        let result = match fabro_acp::run_acp_turn(AcpRunRequest {
            command: process_spec,
            prompt,
            cwd: sandbox.working_directory().to_string(),
            timeout_ms: node.timeout().map(crate::millis_u64),
            env: launch_env,
            sandbox: Arc::clone(sandbox),
            cancel_token: cancel_token.child_token(),
            on_activity: Some(on_activity),
            live_control: Some(AcpLiveControl {
                handle: control_handle.clone(),
                on_natural_completion,
                on_steer_prompt,
            }),
        })
        .await
        {
            Ok(result) => {
                emitter.emit_scoped(
                    &Event::AgentAcpCompleted {
                        node_id:     node.id.clone(),
                        stdout:      result.text.clone(),
                        stderr:      result.stderr.clone(),
                        stop_reason: render_stop_reason(&result.stop_reason),
                        duration_ms: result.duration_ms,
                    },
                    stage_scope,
                );
                result
            }
            Err(AcpError::Cancelled) => {
                emitter.emit_scoped(
                    &Event::AgentAcpCancelled {
                        node_id:     node.id.clone(),
                        stdout:      String::new(),
                        stderr:      String::new(),
                        duration_ms: elapsed_ms(launch_start),
                    },
                    stage_scope,
                );
                return Err(Error::Cancelled);
            }
            Err(AcpError::TimedOut { exec_output_tail }) => {
                let stderr = exec_output_tail
                    .as_ref()
                    .and_then(|tail| tail.stderr.clone())
                    .unwrap_or_default();
                emitter.emit_scoped(
                    &Event::AgentAcpTimedOut {
                        node_id:     node.id.clone(),
                        stdout:      String::new(),
                        stderr:      stderr.clone(),
                        duration_ms: elapsed_ms(launch_start),
                    },
                    stage_scope,
                );
                return Err(acp_error_to_workflow(AcpError::TimedOut {
                    exec_output_tail,
                }));
            }
            Err(AcpError::StopReason { stop_reason, text }) => {
                emitter.emit_scoped(
                    &Event::AgentAcpCompleted {
                        node_id:     node.id.clone(),
                        stdout:      text.clone(),
                        stderr:      String::new(),
                        stop_reason: stop_reason.clone(),
                        duration_ms: elapsed_ms(launch_start),
                    },
                    stage_scope,
                );
                return Err(acp_error_to_workflow(AcpError::StopReason {
                    stop_reason,
                    text,
                }));
            }
            Err(error) => return Err(acp_error_to_workflow(error)),
        };
        if let Some(lease) = lease_for_completion
            .lock()
            .expect("ACP activation lease lock poisoned")
            .take()
        {
            lease.release();
        }

        let (files_touched, last_file_touched) =
            changed_files::files_touched_since(sandbox, &files_before).await;

        Ok(CodergenResult::Text {
            text: result.text,
            usage: None,
            files_touched,
            last_file_touched,
            timing: StageTiming::active_only(result.duration_ms, 0),
        })
    }

    async fn resolve_launch_env(
        &self,
        emitter: &Arc<Emitter>,
    ) -> Result<HashMap<String, String>, Error> {
        let Some(provider) = &self.tool_env else {
            return Ok(HashMap::new());
        };
        if self.github_token_refresh_managed {
            emitter.notice(
                RunNoticeLevel::Info,
                RunNoticeCode::GithubTokenRefreshLimited,
                "ACP agent stages receive workflow env at process launch; GITHUB_TOKEN access to \
                 every declared repository expires together, so stages running beyond token \
                 expiry may need to be retried.",
            );
        }
        provider
            .resolve()
            .await
            .map_err(|err| Error::handler_with_source("Failed to resolve ACP agent env", err))
    }

    fn activate_control_session(
        &self,
        handle: &AcpControlHandle,
        session_id: &str,
        node: &Node,
        stage_scope: &StageScope,
        emitter: &Arc<Emitter>,
        config_name: Option<&str>,
    ) -> Result<Option<Arc<ActivationLease>>, Error> {
        let Some(steering_hub) = &self.steering_hub else {
            return Ok(None);
        };
        ActivationLease::activate(
            ActivationLeaseOptions {
                stage_id:         StageId::new(node.id.clone(), stage_scope.visit),
                session_id:       session_id.to_string(),
                thread_id:        None,
                provider:         Some(AgentBackend::Acp.to_string()),
                model:            config_name.map(str::to_string),
                reasoning_effort: None,
                speed:            None,
                permission_level: None,
                capabilities:     vec![SessionCapability::Steer],
                hub:              Arc::clone(steering_hub),
                emitter:          Arc::clone(emitter),
            },
            Arc::new(AcpSteerable(handle.clone())),
        )
        .map(Some)
    }
}

/// How many steers wait on an ACP session before the oldest is dropped.
/// Pebble's own sessions bound their queue themselves; the ACP session's
/// queue is fabro's, so the bound is stated here.
const ACP_STEERING_QUEUE_CAP: usize = 32;

/// The ACP session as a session on the steering bus. It cannot hold its
/// completion open, so a human cannot pair with it.
struct AcpSteerable(AcpControlHandle);

impl SteerableSession for AcpSteerable {
    fn steer(&self, message: SteeringMessage) -> SteeringOutcome {
        self.0
            .enqueue_bounded(message, ACP_STEERING_QUEUE_CAP)
            .map_or(SteeringOutcome::Accepted, SteeringOutcome::Evicted)
    }

    fn interrupt(&self) -> bool {
        self.0.interrupt();
        true
    }

    fn steer_now(&self, message: SteeringMessage) -> SteeringOutcome {
        self.0
            .interrupt_then_enqueue_bounded(message, ACP_STEERING_QUEUE_CAP)
            .map_or(SteeringOutcome::Accepted, SteeringOutcome::Evicted)
    }

    fn has_pending_steering(&self) -> bool {
        self.0.has_pending_control_work()
    }
}

impl Default for AgentAcpBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodergenBackend for AgentAcpBackend {
    async fn run(&self, request: CodergenRunRequest<'_>) -> Result<CodergenResult, Error> {
        if request.node.output_schema().is_some() {
            return Err(Error::Validation(
                "output_schema is not supported with backend=\"acp\" in this release".to_string(),
            ));
        }
        let stage_scope = StageScope::for_handler(request.context, &request.node.id);
        self.run_turn(
            request.node,
            request.prompt.to_string(),
            request.emitter,
            &stage_scope,
            request.sandbox,
            request.cancel_token,
        )
        .await
    }

    async fn one_shot(&self, _request: OneShotRequest<'_>) -> Result<CodergenResult, Error> {
        Err(Error::Validation(
            "backend=\"acp\" is only valid on agent nodes; prompt nodes are API-only".to_string(),
        ))
    }

    fn node_timeout_policy(&self, _node: &Node) -> NodeTimeoutPolicy {
        NodeTimeoutPolicy::HandlerManaged
    }
}

fn acp_process_error_to_workflow(error: AcpCommandError) -> Error {
    match error {
        AcpCommandError::LegacyCommandAttribute => {
            Error::handler("acp_command is no longer supported; use acp.command or acp.config")
        }
        AcpCommandError::EmptyOverride => Error::handler("ACP process attribute must not be empty"),
        AcpCommandError::MissingOverride => {
            Error::handler("backend=\"acp\" requires exactly one of acp.command or acp.config")
        }
        AcpCommandError::UnsupportedTransport => {
            Error::handler("only stdio ACP commands are supported")
        }
        AcpCommandError::InvalidCommandString => {
            Error::handler("Failed to parse acp.command as a shell command")
        }
        AcpCommandError::InvalidConfigJson(source) => {
            Error::handler_with_source("Failed to parse acp.config as JSON", source)
        }
        AcpCommandError::InvalidConfigShape(message) => {
            Error::handler(format!("Invalid acp.config shape: {message}"))
        }
    }
}

fn resolve_acp_process_spec(node: &Node) -> Result<AcpProcessSpec, Error> {
    AcpProcessSpec::from_attrs(
        node.legacy_acp_command_attr(),
        node.acp_command_attr(),
        node.acp_config_attr(),
    )
    .map_err(acp_process_error_to_workflow)
}

fn acp_error_to_workflow(error: AcpError) -> Error {
    match error {
        AcpError::Cancelled => Error::Cancelled,
        AcpError::TimedOut { exec_output_tail } => {
            Error::handler_with_exec_output_tail("ACP turn timed out", exec_output_tail)
        }
        AcpError::StopReason { stop_reason, text } => {
            Error::handler(format!("ACP prompt stopped with {stop_reason}: {text}"))
        }
        AcpError::Sandbox(source) => Error::handler_with_source("ACP turn failed", source),
        other => {
            let exec_output_tail = other.exec_output_tail();
            Error::handler_with_source_and_exec_output_tail(
                "ACP turn failed",
                other,
                exec_output_tail,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use fabro_acp::test_support::fake_acp_agent_script;
    use fabro_acp::{AcpError, AcpProcessExit};
    use fabro_graphviz::graph::{AttrValue, Node};
    use fabro_sandbox::test_support::MockSandbox;
    use fabro_sandbox::{RunSandbox, TokenProvenance, TokenSnapshot, local_sandbox};
    use fabro_types::{CommandTermination, EventBody, ExecOutputTail};
    use fabro_util::shell;
    use tokio_util::sync::CancellationToken;

    use super::{
        AgentAcpBackend, REFRESH_RESCHEDULE_FLOOR, acp_error_to_workflow, next_refresh_delay,
        parse_refresh_enabled, parse_refresh_interval, refresh_ahead_loop,
    };
    use crate::context::Context;
    use crate::event::Emitter;
    use crate::handler::agent::{CodergenBackend, CodergenResult, CodergenRunRequest};
    use crate::steering_hub::SteeringHub;

    #[test]
    fn refresh_enabled_defaults_on_and_honors_falsy_values() {
        // Default ON when unset.
        assert!(parse_refresh_enabled(None));
        // Truthy / non-falsy values stay enabled.
        for v in ["1", "true", "on", "yes", "anything"] {
            assert!(parse_refresh_enabled(Some(v)), "{v} should be enabled");
        }
        // Falsy values disable — case-insensitive, and empty/whitespace counts.
        for v in [
            "0", "false", "off", "no", "FALSE", "Off", "No", "OFF", "", "  ",
        ] {
            assert!(!parse_refresh_enabled(Some(v)), "{v} should be disabled");
        }
    }

    #[test]
    fn refresh_interval_parses_default_disable_and_override() {
        // Unset or empty → default.
        assert_eq!(parse_refresh_interval(None), Some(Duration::from_mins(45)));
        assert_eq!(
            parse_refresh_interval(Some("  ")),
            Some(Duration::from_mins(45))
        );
        // Explicit 0 disables the loop.
        assert_eq!(parse_refresh_interval(Some("0")), None);
        // A positive value overrides.
        assert_eq!(
            parse_refresh_interval(Some("1800")),
            Some(Duration::from_mins(30))
        );
        assert_eq!(
            parse_refresh_interval(Some(" 900 ")),
            Some(Duration::from_mins(15))
        );
        // Unparsable → default (never panics).
        for v in ["15m", "-1", "abc", "9999999999999999999999"] {
            assert_eq!(
                parse_refresh_interval(Some(v)),
                Some(Duration::from_mins(45)),
                "{v} should fall back to default"
            );
        }
    }

    #[tokio::test]
    async fn refresh_reports_no_token_without_managed_credentials() {
        // A mock sandbox has no cloned workspace and so no managed
        // credentials: refresh is a no-op that must report no token — the
        // signal the refresh-ahead loop relies on to stop rather than claim
        // a renewal.
        let sandbox = MockSandbox::linux().sandbox();
        assert_eq!(sandbox.refresh_ambient_credentials().await.unwrap(), None);
    }

    fn minted_token(
        generation: u64,
        minted_ago: chrono::Duration,
        expires_in: chrono::Duration,
        reused: bool,
    ) -> TokenSnapshot {
        let now = chrono::Utc::now();
        let minted_at = now - minted_ago;
        let expires_at = now + expires_in;
        let provenance = if reused {
            TokenProvenance::Reused {
                minted_at,
                expires_at,
            }
        } else {
            TokenProvenance::Minted {
                minted_at,
                expires_at,
            }
        };
        TokenSnapshot {
            generation,
            provenance,
        }
    }

    fn static_token() -> TokenSnapshot {
        TokenSnapshot {
            generation: 0,
            provenance: TokenProvenance::Static,
        }
    }

    #[test]
    fn next_refresh_delay_schedules_from_token_expiry_minus_margin() {
        let outcome = minted_token(
            1,
            chrono::Duration::zero(),
            chrono::Duration::minutes(60),
            false,
        );
        let delay = next_refresh_delay(Some(&outcome)).unwrap();
        // Expiry minus the 10-minute refresh margin: ~50 minutes out.
        assert!(delay > Duration::from_mins(49), "{delay:?}");
        assert!(delay <= Duration::from_mins(50), "{delay:?}");
    }

    #[test]
    fn next_refresh_delay_floors_when_the_margin_is_already_open() {
        let outcome = minted_token(
            1,
            chrono::Duration::minutes(55),
            chrono::Duration::minutes(5),
            true,
        );
        assert_eq!(
            next_refresh_delay(Some(&outcome)),
            Some(REFRESH_RESCHEDULE_FLOOR)
        );
    }

    #[test]
    fn next_refresh_delay_disables_the_loop_for_static_credentials() {
        assert_eq!(next_refresh_delay(Some(&static_token())), None);
    }

    #[test]
    fn next_refresh_delay_disables_the_loop_without_managed_credentials() {
        assert_eq!(next_refresh_delay(None), None);
    }

    /// Scripted refresh outcomes, recording when each refresh tick lands on
    /// the (paused) tokio clock.
    struct ScriptedRefresh {
        script: Mutex<std::collections::VecDeque<TokenSnapshot>>,
        ticks:  Mutex<Vec<tokio::time::Instant>>,
    }

    impl ScriptedRefresh {
        fn new(script: Vec<TokenSnapshot>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script.into()),
                ticks:  Mutex::new(Vec::new()),
            })
        }

        fn ticks(&self) -> Vec<tokio::time::Instant> {
            self.ticks.lock().expect("ticks lock").clone()
        }

        /// The refresh the loop calls: answers the next scripted outcome.
        fn refresher(
            self: &Arc<Self>,
        ) -> impl Fn() -> std::future::Ready<fabro_sandbox::Result<Option<TokenSnapshot>>> + Send
        {
            let this = Arc::clone(self);
            move || {
                this.ticks
                    .lock()
                    .expect("ticks lock")
                    .push(tokio::time::Instant::now());
                std::future::ready(Ok(Some(
                    this.script
                        .lock()
                        .expect("script lock")
                        .pop_front()
                        .expect("refresh script exhausted"),
                )))
            }
        }
    }

    /// Long-turn timeline: the clone/turn-entry mint happened at minute 0 with
    /// a 60-minute TTL. The loop's first tick at minute 45 sees the cached
    /// token reused with ~15 minutes left and must NOT sleep another fixed 45
    /// minutes (that would cross expiry at minute 60) — it reschedules for the
    /// margin opening (~5 minutes out). That margin-crossing tick re-mints and
    /// reschedules from the fresh token's expiry (~50 minutes out).
    #[tokio::test(start_paused = true)]
    async fn refresh_ahead_reschedules_from_token_expiry_across_a_long_turn() {
        let interval = Duration::from_mins(45);
        let sandbox = ScriptedRefresh::new(vec![
            // Minute 45: cache still fresh (expires minute 60, margin opens
            // minute 50).
            minted_token(
                1,
                chrono::Duration::minutes(45),
                chrono::Duration::minutes(15),
                true,
            ),
            // Minute ~50: margin open → the source minted generation 2.
            minted_token(
                2,
                chrono::Duration::zero(),
                chrono::Duration::minutes(60),
                false,
            ),
            // Minute ~100: generation 2 still fresh.
            minted_token(
                2,
                chrono::Duration::minutes(50),
                chrono::Duration::minutes(10),
                true,
            ),
        ]);
        let cancel = CancellationToken::new();
        let start = tokio::time::Instant::now();
        let loop_task = tokio::spawn(refresh_ahead_loop(
            sandbox.refresher(),
            cancel.clone(),
            interval,
            interval,
        ));

        while sandbox.ticks().len() < 3 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        cancel.cancel();
        loop_task.await.expect("refresh loop should exit cleanly");

        let ticks = sandbox.ticks();
        assert_eq!(ticks[0] - start, interval, "first tick uses the interval");
        // Reused token expiring in 15 minutes → next tick when the 10-minute
        // margin opens, ~5 minutes later (never another fixed 45 minutes).
        let second_gap = ticks[1] - ticks[0];
        assert!(second_gap <= Duration::from_mins(5), "{second_gap:?}");
        assert!(second_gap > Duration::from_mins(4), "{second_gap:?}");
        // Fresh 60-minute token → next tick ~50 minutes out.
        let third_gap = ticks[2] - ticks[1];
        assert!(third_gap <= Duration::from_mins(50), "{third_gap:?}");
        assert!(third_gap > Duration::from_mins(49), "{third_gap:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_ahead_honors_the_expiry_based_initial_delay() {
        let interval = Duration::from_mins(45);
        let entry_outcome = minted_token(
            1,
            chrono::Duration::minutes(45),
            chrono::Duration::minutes(15),
            true,
        );
        let initial_delay = next_refresh_delay(Some(&entry_outcome)).unwrap();
        let sandbox = ScriptedRefresh::new(vec![minted_token(
            2,
            chrono::Duration::zero(),
            chrono::Duration::minutes(60),
            false,
        )]);
        let cancel = CancellationToken::new();
        let start = tokio::time::Instant::now();
        let loop_task = tokio::spawn(refresh_ahead_loop(
            sandbox.refresher(),
            cancel.clone(),
            interval,
            initial_delay,
        ));

        while sandbox.ticks().is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        cancel.cancel();
        loop_task.await.expect("refresh loop should exit cleanly");

        let first_tick = sandbox.ticks()[0] - start;
        assert!(first_tick <= Duration::from_mins(5), "{first_tick:?}");
        assert!(first_tick > Duration::from_mins(4), "{first_tick:?}");
    }

    #[tokio::test]
    async fn acp_backend_run_sends_prompt_and_returns_text() {
        let tempdir = tempfile::tempdir().unwrap();
        init_git(tempdir.path());
        let script_path = tempdir.path().join("fake_acp_agent.py");
        tokio::fs::write(&script_path, fake_acp_agent_script())
            .await
            .unwrap();

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String(format!(
                "python3 {}",
                shell::shell_quote(&script_path.to_string_lossy())
            )),
        );

        let backend = AgentAcpBackend::new().with_env(HashMap::from([(
            "ACP_MODE".to_string(),
            "write_file".to_string(),
        )]));
        let sandbox: Arc<RunSandbox> =
            Arc::new(local_sandbox(tempdir.path().to_path_buf()).await.unwrap());
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await
            .unwrap();

        let CodergenResult::Text {
            text,
            files_touched,
            ..
        } = result
        else {
            panic!("expected text result");
        };
        assert_eq!(text, "hello from acp");
        assert_eq!(files_touched, vec!["hello.txt"]);
    }

    #[tokio::test]
    async fn acp_backend_rejects_output_schema_without_launching_process() {
        let tempdir = tempfile::tempdir().unwrap();
        let launched_path = tempdir.path().join("launched");

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String("sh -c 'touch launched'".to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String("routing".to_string()),
        );

        let backend = AgentAcpBackend::new();
        let sandbox: Arc<RunSandbox> =
            Arc::new(local_sandbox(tempdir.path().to_path_buf()).await.unwrap());
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await;

        let Err(error) = result else {
            panic!("expected output_schema guardrail error");
        };
        assert!(
            error
                .to_string()
                .contains("output_schema is not supported with backend=\"acp\" in this release"),
            "unexpected error: {error}",
        );
        assert!(
            !launched_path.exists(),
            "ACP process should not launch when output_schema is present",
        );
    }

    #[tokio::test]
    async fn acp_backend_accepts_steer_and_incorporates_followup_result() {
        let tempdir = tempfile::tempdir().unwrap();
        init_git(tempdir.path());
        let script_path = tempdir.path().join("fake_acp_agent.py");
        tokio::fs::write(&script_path, fake_acp_agent_script())
            .await
            .unwrap();

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String(format!(
                "python3 {}",
                shell::shell_quote(&script_path.to_string_lossy())
            )),
        );

        let emitter = Arc::new(Emitter::default());
        let steering_hub = Arc::new(SteeringHub::new(Arc::clone(&emitter)));
        let sent = Arc::new(AtomicBool::new(false));
        let sent_for_listener = Arc::clone(&sent);
        let hub_for_listener = Arc::clone(&steering_hub);
        emitter.on_event(move |event| {
            if event.event_name() == "agent.session.activated"
                && !sent_for_listener.swap(true, Ordering::AcqRel)
            {
                hub_for_listener.deliver_steer("please revise".to_string(), None);
            }
        });

        let backend = AgentAcpBackend::new()
            .with_env(HashMap::from([(
                "ACP_MODE".to_string(),
                "steer".to_string(),
            )]))
            .with_steering_hub(steering_hub);
        let sandbox: Arc<RunSandbox> =
            Arc::new(local_sandbox(tempdir.path().to_path_buf()).await.unwrap());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await
            .unwrap();

        let CodergenResult::Text { text, .. } = result else {
            panic!("expected text result");
        };
        assert_eq!(text, "initial steered:please revise");
    }

    #[tokio::test]
    async fn acp_backend_accepts_acp_command_attribute_without_model_or_provider() {
        let tempdir = tempfile::tempdir().unwrap();
        init_git(tempdir.path());
        let script_path = tempdir.path().join("fake_acp_agent.py");
        tokio::fs::write(&script_path, fake_acp_agent_script())
            .await
            .unwrap();

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String(format!(
                "python3 {}",
                shell::shell_quote(&script_path.to_string_lossy())
            )),
        );

        let backend = AgentAcpBackend::new().with_env(HashMap::from([(
            "ACP_MODE".to_string(),
            "write_file".to_string(),
        )]));
        let sandbox: Arc<RunSandbox> =
            Arc::new(local_sandbox(tempdir.path().to_path_buf()).await.unwrap());
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await
            .unwrap();

        let CodergenResult::Text { text, .. } = result else {
            panic!("expected text result");
        };
        assert_eq!(text, "hello from acp");
    }

    #[tokio::test]
    async fn acp_backend_does_not_forward_provider_credentials() {
        let mut sandbox = MockSandbox::linux();
        sandbox.stdio_process_error = Some("stop before ACP handshake".to_string());
        let sandbox_dyn = sandbox.sandbox();

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String("fake-acp-agent".to_string()),
        );

        let backend = AgentAcpBackend::new();
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox_dyn,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await;
        assert!(result.is_err());

        let captured = sandbox.captured_env_vars().unwrap_or_default();
        assert!(!captured.contains_key("OPENAI_API_KEY"));
        assert!(!captured.contains_key("ANTHROPIC_API_KEY"));
        assert!(!captured.contains_key("GEMINI_API_KEY"));
    }

    #[tokio::test]
    async fn acp_backend_cancelled_stop_reason_maps_to_cancelled_error() {
        let tempdir = tempfile::tempdir().unwrap();
        let script_path = tempdir.path().join("fake_acp_agent.py");
        tokio::fs::write(&script_path, fake_acp_agent_script())
            .await
            .unwrap();

        let mut node = Node::new("work");
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String(format!(
                "python3 {}",
                shell::shell_quote(&script_path.to_string_lossy())
            )),
        );

        let backend = AgentAcpBackend::new().with_env(HashMap::from([(
            "ACP_STOP_REASON".to_string(),
            "cancelled".to_string(),
        )]));
        let sandbox: Arc<RunSandbox> =
            Arc::new(local_sandbox(tempdir.path().to_path_buf()).await.unwrap());
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "cancel",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await;
        let Err(err) = result else {
            panic!("expected cancellation error");
        };

        assert!(matches!(err, crate::error::Error::Cancelled));
    }

    #[tokio::test]
    async fn acp_started_event_omits_json_command_env_values() {
        let tempdir = tempfile::tempdir().unwrap();
        let script_path = tempdir.path().join("fake_acp_agent.py");
        tokio::fs::write(&script_path, fake_acp_agent_script())
            .await
            .unwrap();

        let raw_command = serde_json::json!({
            "type": "stdio",
            "name": "fake",
            "command": "python3",
            "args": [script_path.to_string_lossy()],
            "env": [
                {"name": "OPENAI_API_KEY", "value": "secret-key"}
            ],
        })
        .to_string();
        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs
            .insert("acp.config".to_string(), AttrValue::String(raw_command));

        let backend = AgentAcpBackend::new();
        let sandbox: Arc<RunSandbox> =
            Arc::new(local_sandbox(tempdir.path().to_path_buf()).await.unwrap());
        let emitter = Arc::new(Emitter::default());
        let events = Arc::new(Mutex::new(Vec::new()));
        emitter.on_event({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event.clone())
        });

        let context = Context::new();
        backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await
            .unwrap();

        let events = events.lock().unwrap();
        let command = events
            .iter()
            .find_map(|event| match &event.body {
                EventBody::AgentAcpStarted(props) => Some(props.command.as_str()),
                _ => None,
            })
            .expect("ACP started event should be emitted");
        assert!(command.contains("python3"));
        assert!(command.contains("fake_acp_agent.py"));
        assert!(!command.contains("OPENAI_API_KEY"));
        assert!(!command.contains("secret-key"));
    }

    #[tokio::test]
    async fn acp_backend_requires_explicit_process_attr() {
        let sandbox = MockSandbox::linux();
        let sandbox_dyn = sandbox.sandbox();

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));

        let backend = AgentAcpBackend::new();
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox_dyn,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await;
        let Err(err) = result else {
            panic!("ACP without process attr should fail");
        };
        assert!(
            err.to_string()
                .contains("requires exactly one of acp.command or acp.config")
        );
        assert!(
            sandbox.captured_env_vars().is_none(),
            "ACP process should not launch when process attr is missing"
        );
    }

    #[tokio::test]
    async fn acp_backend_stdio_spawn_failure_preserves_sandbox_cause() {
        const DAYTONA_UNSUPPORTED_ACP: &str = "ACP backend requires bidirectional stdio; the Daytona sandbox provider does not support it yet";

        let mut sandbox = MockSandbox::linux();
        sandbox.stdio_process_error = Some(DAYTONA_UNSUPPORTED_ACP.to_string());
        let sandbox_dyn = sandbox.sandbox();

        let mut node = Node::new("work");
        node.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        node.attrs.insert(
            "acp.command".to_string(),
            AttrValue::String("fake-acp-agent".to_string()),
        );

        let backend = AgentAcpBackend::new().with_env(HashMap::from([(
            "WORKFLOW_ENV".to_string(),
            "test-value".to_string(),
        )]));
        let emitter = Arc::new(Emitter::default());
        let context = Context::new();
        let result = backend
            .run(CodergenRunRequest {
                node:            &node,
                prompt:          "write hello",
                context:         &context,
                thread_id:       None,
                emitter:         &emitter,
                sandbox:         &sandbox_dyn,
                tool_middleware: None,
                cancel_token:    CancellationToken::new(),
                human_input:     None,
            })
            .await;
        let Err(err) = result else {
            panic!("stdio spawn failure should fail the ACP turn");
        };

        let rendered = err.display_with_causes();
        assert!(
            rendered.contains("ACP turn failed"),
            "rendered error should keep ACP context: {rendered}"
        );
        assert!(
            err.causes()
                .iter()
                .any(|cause| cause == DAYTONA_UNSUPPORTED_ACP),
            "cause chain should include sandbox failure, got: {rendered}"
        );
        assert_eq!(
            err.failure_category(),
            crate::error::FailureCategory::Deterministic
        );
    }

    #[test]
    fn acp_timeout_maps_stderr_to_exec_tail_not_message() {
        let tail = ExecOutputTail {
            stdout:           None,
            stderr:           Some("redacted stderr tail".to_string()),
            stdout_truncated: false,
            stderr_truncated: true,
        };
        let err = acp_error_to_workflow(AcpError::TimedOut {
            exec_output_tail: Some(tail.clone()),
        });

        let detail = err.to_failure_detail();
        assert_eq!(detail.message, "ACP turn timed out");
        assert!(detail.causes.is_empty());
        assert_eq!(detail.exec_output_tail, Some(tail));
    }

    #[test]
    fn acp_process_exit_maps_stderr_to_exec_tail_not_cause_text() {
        let tail = ExecOutputTail {
            stdout:           None,
            stderr:           Some("early boom".to_string()),
            stdout_truncated: false,
            stderr_truncated: false,
        };
        let err = acp_error_to_workflow(AcpError::ProcessExited(AcpProcessExit {
            termination:      CommandTermination::Exited,
            exit_code:        Some(2),
            exec_output_tail: Some(tail.clone()),
        }));

        let detail = err.to_failure_detail();
        assert_eq!(detail.message, "ACP turn failed");
        assert_eq!(detail.exec_output_tail, Some(tail));
        assert!(
            detail
                .causes
                .iter()
                .any(|cause| cause.contains("exit_code=2")),
            "cause chain should retain process exit context: {:?}",
            detail.causes
        );
        assert!(
            !detail
                .causes
                .iter()
                .any(|cause| cause.contains("early boom")),
            "raw stderr belongs in exec_output_tail, not causes: {:?}",
            detail.causes
        );
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "unit test initializes an isolated git repository with the system git binary"
    )]
    fn init_git(path: &std::path::Path) {
        let output = std::process::Command::new("git")
            .arg("init")
            .current_dir(path)
            .output()
            .unwrap();
        assert!(output.status.success());
    }
}
