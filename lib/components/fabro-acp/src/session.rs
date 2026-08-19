use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::{
    CancelNotification, ContentBlock, ContentChunk, Cost, InitializeRequest, PermissionOptionKind,
    PromptRequest, PromptResponse, ProtocolVersion, RequestPermissionOutcome,
    RequestPermissionRequest, RequestPermissionResponse, SelectedPermissionOutcome,
    SessionNotification, SessionUpdate, StopReason, ToolCall, ToolCallId, ToolCallStatus,
    ToolCallUpdate, ToolKind, Usage,
};
use agent_client_protocol::util::MatchDispatch;
use agent_client_protocol::{ActiveSession, Agent, Client, Error as ProtocolError, SessionMessage};
use fabro_sandbox::Sandbox;
use fabro_types::{Principal, SteeringMessage};
use fabro_util::time::elapsed_ms;
use tokio::sync::{Notify, oneshot};
use tokio::sync::futures::Notified;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_util::sync::CancellationToken;

use crate::command::AcpProcessSpec;
use crate::error::AcpError;
use crate::transport::{SandboxAcpTransport, TransportState};

pub type AcpNaturalCompletionCallback = Arc<dyn Fn() -> bool + Send + Sync>;
pub type AcpSteerPromptCallback = Arc<dyn Fn(String, Option<Principal>) + Send + Sync>;

pub type AcpSessionActivityCallback = Arc<dyn Fn(AcpSessionActivity) + Send + Sync>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcpToolKind {
    Read,
    Edit,
    Delete,
    Move,
    Search,
    Execute,
    Think,
    Fetch,
    SwitchMode,
    Other,
}

impl From<ToolKind> for AcpToolKind {
    fn from(value: ToolKind) -> Self {
        match value {
            ToolKind::Read => Self::Read,
            ToolKind::Edit => Self::Edit,
            ToolKind::Delete => Self::Delete,
            ToolKind::Move => Self::Move,
            ToolKind::Search => Self::Search,
            ToolKind::Execute => Self::Execute,
            ToolKind::Think => Self::Think,
            ToolKind::Fetch => Self::Fetch,
            ToolKind::SwitchMode => Self::SwitchMode,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AcpSessionActivity {
    ToolStarted {
        tool_call_id: String,
        tool_name:    String,
        title:        String,
        raw_input:    serde_json::Value,
        kind:         AcpToolKind,
    },
    ToolCompleted {
        tool_call_id: String,
        tool_name:    String,
        output:       serde_json::Value,
        is_error:     bool,
    },
    UsageUpdated {
        used: u64,
        size: u64,
        cost: Option<AcpReportedCost>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcpReportedCost {
    pub amount:   f64,
    pub currency: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AcpRunUsage {
    pub input_tokens:       u64,
    pub output_tokens:      u64,
    pub reasoning_tokens:   u64,
    pub cache_read_tokens:  u64,
    pub cache_write_tokens: u64,
    pub total_tokens:       u64,
    pub reported_cost:      Option<AcpReportedCost>,
}

#[derive(Default)]
struct AcpUsageAccumulator {
    usage:            AcpRunUsage,
    saw_prompt_usage: bool,
}

impl AcpUsageAccumulator {
    fn add_prompt_usage(&mut self, usage: &Usage) {
        let reasoning_tokens = usage.thought_tokens.unwrap_or_default();
        let cache_read_tokens = usage.cached_read_tokens.unwrap_or_default();
        let cache_write_tokens = usage.cached_write_tokens.unwrap_or_default();
        let normalized_total = usage
            .input_tokens
            .saturating_add(usage.output_tokens)
            .saturating_add(reasoning_tokens)
            .saturating_add(cache_read_tokens)
            .saturating_add(cache_write_tokens);
        if normalized_total != usage.total_tokens {
            tracing::warn!(
                reported_total_tokens = usage.total_tokens,
                normalized_total_tokens = normalized_total,
                "ACP prompt usage total differs from disjoint bucket sum"
            );
        }

        self.usage.input_tokens = self
            .usage
            .input_tokens
            .saturating_add(usage.input_tokens);
        self.usage.output_tokens = self
            .usage
            .output_tokens
            .saturating_add(usage.output_tokens);
        self.usage.reasoning_tokens = self
            .usage
            .reasoning_tokens
            .saturating_add(reasoning_tokens);
        self.usage.cache_read_tokens = self
            .usage
            .cache_read_tokens
            .saturating_add(cache_read_tokens);
        self.usage.cache_write_tokens = self
            .usage
            .cache_write_tokens
            .saturating_add(cache_write_tokens);
        self.usage.total_tokens = self
            .usage
            .input_tokens
            .saturating_add(self.usage.output_tokens)
            .saturating_add(self.usage.reasoning_tokens)
            .saturating_add(self.usage.cache_read_tokens)
            .saturating_add(self.usage.cache_write_tokens);
        self.saw_prompt_usage = true;
    }

    fn record_reported_cost(&mut self, cost: Option<AcpReportedCost>) {
        let Some(cost) = cost else {
            return;
        };
        if cost.currency.eq_ignore_ascii_case("USD")
            && cost.amount.is_finite()
            && cost.amount >= 0.0
        {
            self.usage.reported_cost = Some(cost);
        } else {
            tracing::warn!(
                amount = cost.amount,
                currency = %cost.currency,
                "ignoring invalid or unsupported ACP reported cost update"
            );
        }
    }

    fn finish(self) -> Option<AcpRunUsage> {
        (self.saw_prompt_usage || self.usage.reported_cost.is_some()).then_some(self.usage)
    }
}

fn convert_reported_cost(cost: &Cost) -> AcpReportedCost {
    AcpReportedCost {
        amount:   cost.amount,
        currency: cost.currency.clone(),
    }
}

#[derive(Debug, Clone)]
struct TrackedToolCall {
    tool_name: String,
    kind:      AcpToolKind,
    started:   bool,
    completed: bool,
}

fn tool_call_id_string(id: &ToolCallId) -> String {
    id.0.to_string()
}

fn convert_session_update(
    update: &SessionUpdate,
    tracked: &mut HashMap<String, TrackedToolCall>,
) -> Vec<AcpSessionActivity> {
    match update {
        SessionUpdate::ToolCall(call) => convert_tool_call(call, tracked),
        SessionUpdate::ToolCallUpdate(update) => convert_tool_call_update(update, tracked),
        SessionUpdate::UsageUpdate(update) => vec![AcpSessionActivity::UsageUpdated {
            used: update.used,
            size: update.size,
            cost: update.cost.as_ref().map(convert_reported_cost),
        }],
        _ => Vec::new(),
    }
}

fn convert_tool_call(
    call: &ToolCall,
    tracked: &mut HashMap<String, TrackedToolCall>,
) -> Vec<AcpSessionActivity> {
    let tool_call_id = tool_call_id_string(&call.tool_call_id);
    let tool_name = call.title.clone();
    let entry = tracked
        .entry(tool_call_id.clone())
        .or_insert_with(|| TrackedToolCall {
            kind:      call.kind.into(),
            tool_name: tool_name.clone(),
            started:   false,
            completed: false,
        });
    let mut events = Vec::new();
    if !entry.started {
        entry.started = true;
        events.push(AcpSessionActivity::ToolStarted {
            kind:         entry.kind,
            tool_call_id: tool_call_id.clone(),
            tool_name:    entry.tool_name.clone(),
            title:        call.title.clone(),
            raw_input:    call.raw_input.clone().unwrap_or(serde_json::Value::Null),
        });
    }
    if matches!(
        call.status,
        ToolCallStatus::Completed | ToolCallStatus::Failed
    ) && !entry.completed
    {
        entry.completed = true;
        events.push(AcpSessionActivity::ToolCompleted {
            tool_call_id,
            tool_name: entry.tool_name.clone(),
            output: call.raw_output.clone().unwrap_or(serde_json::Value::Null),
            is_error: matches!(call.status, ToolCallStatus::Failed),
        });
    }
    events
}

fn convert_tool_call_update(
    update: &ToolCallUpdate,
    tracked: &mut HashMap<String, TrackedToolCall>,
) -> Vec<AcpSessionActivity> {
    let tool_call_id = tool_call_id_string(&update.tool_call_id);
    let entry = tracked
        .entry(tool_call_id.clone())
        .or_insert_with(|| TrackedToolCall {
            tool_name: update
                .fields
                .title
                .clone()
                .unwrap_or_else(|| "tool".to_string()),
            kind:      update.fields.kind.unwrap_or(ToolKind::Other).into(),
            started:   false,
            completed: false,
        });
    if let Some(title) = update.fields.title.clone() {
        entry.tool_name = title;
    }
    if let Some(kind) = update.fields.kind {
        entry.kind = kind.into();
    }
    let mut events = Vec::new();
    if !entry.started {
        entry.started = true;
        events.push(AcpSessionActivity::ToolStarted {
            kind:         entry.kind,
            tool_call_id: tool_call_id.clone(),
            tool_name:    entry.tool_name.clone(),
            title:        entry.tool_name.clone(),
            raw_input:    update
                .fields
                .raw_input
                .clone()
                .unwrap_or(serde_json::Value::Null),
        });
    }
    if let Some(status) = update.fields.status {
        if matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed) && !entry.completed
        {
            entry.completed = true;
            events.push(AcpSessionActivity::ToolCompleted {
                tool_call_id,
                tool_name: entry.tool_name.clone(),
                output: update
                    .fields
                    .raw_output
                    .clone()
                    .unwrap_or(serde_json::Value::Null),
                is_error: matches!(status, ToolCallStatus::Failed),
            });
        }
    }
    events
}

const CANCEL_GRACE_PERIOD: Duration = Duration::from_millis(500);
// Drain buffered notifications first, but recheck a ready prompt response after a bounded batch.
const MAX_SESSION_UPDATES_BEFORE_PROMPT_CHECK: usize = 64;
// Quiescence ends normal drains; this deadline bounds agents that stream after responding.
const POST_RESPONSE_DRAIN_LIMIT: Duration = Duration::from_millis(100);

#[derive(Default)]
struct AcpControlState {
    queue:               VecDeque<SteeringMessage>,
    waiting_for_steer:   bool,
    interrupt_requested: bool,
}

#[derive(Clone, Default)]
pub struct AcpControlHandle {
    state:  Arc<Mutex<AcpControlState>>,
    notify: Arc<Notify>,
}

impl AcpControlHandle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue_bounded(&self, item: SteeringMessage, cap: usize) -> Option<SteeringMessage> {
        self.push_bounded(item, cap, false)
    }

    pub fn interrupt(&self, _actor: Option<Principal>) {
        {
            let mut state = self.state.lock().expect("ACP control lock poisoned");
            if state.queue.is_empty() {
                state.waiting_for_steer = true;
            }
            state.interrupt_requested = true;
        }
        self.notify.notify_one();
    }

    pub fn interrupt_then_enqueue_bounded(
        &self,
        item: SteeringMessage,
        cap: usize,
    ) -> Option<SteeringMessage> {
        self.push_bounded(item, cap, true)
    }

    fn push_bounded(
        &self,
        item: SteeringMessage,
        cap: usize,
        request_interrupt: bool,
    ) -> Option<SteeringMessage> {
        let evicted = {
            let mut state = self.state.lock().expect("ACP control lock poisoned");
            let evicted = if state.queue.len() >= cap {
                state.queue.pop_front()
            } else {
                None
            };
            state.waiting_for_steer = false;
            if request_interrupt {
                state.interrupt_requested = true;
            }
            state.queue.push_back(item);
            evicted
        };
        self.notify.notify_one();
        evicted
    }

    #[must_use]
    pub fn has_pending_control_work(&self) -> bool {
        let state = self.state.lock().expect("ACP control lock poisoned");
        !state.queue.is_empty() || state.waiting_for_steer || state.interrupt_requested
    }

    #[cfg(test)]
    #[must_use]
    pub fn queue_len(&self) -> usize {
        self.state
            .lock()
            .expect("ACP control lock poisoned")
            .queue
            .len()
    }

    fn pop_steer(&self) -> Option<SteeringMessage> {
        let item = {
            let mut state = self.state.lock().expect("ACP control lock poisoned");
            let item = state.queue.pop_front();
            if item.is_some() {
                state.waiting_for_steer = false;
            }
            item
        };
        if item.is_some() {
            self.notify.notify_one();
        }
        item
    }

    fn take_interrupt_requested(&self) -> bool {
        let mut state = self.state.lock().expect("ACP control lock poisoned");
        let requested = state.interrupt_requested;
        state.interrupt_requested = false;
        requested
    }

    fn should_wait_for_steer(&self) -> bool {
        let state = self.state.lock().expect("ACP control lock poisoned");
        state.waiting_for_steer && state.queue.is_empty()
    }

    fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

#[derive(Default)]
pub struct AcpLiveControl {
    pub handle:                AcpControlHandle,
    pub on_natural_completion: Option<AcpNaturalCompletionCallback>,
    pub on_steer_prompt:       Option<AcpSteerPromptCallback>,
}

impl AcpLiveControl {
    #[must_use]
    pub fn new(handle: AcpControlHandle) -> Self {
        Self {
            handle,
            on_natural_completion: None,
            on_steer_prompt: None,
        }
    }
}

pub struct AcpRunRequest {
    pub command:             AcpProcessSpec,
    pub prompt:              String,
    pub cwd:                 String,
    pub timeout_ms:          Option<u64>,
    pub env:                 HashMap<String, String>,
    pub sandbox:             Arc<dyn Sandbox>,
    pub cancel_token:        CancellationToken,
    pub on_activity:         Option<Arc<dyn Fn() + Send + Sync>>,
    pub on_session_activity: Option<AcpSessionActivityCallback>,
    pub live_control:        Option<AcpLiveControl>,
}

#[derive(Debug)]
pub struct AcpRunResult {
    pub text:        String,
    pub stop_reason: StopReason,
    pub usage:       Option<AcpRunUsage>,
    pub stderr:      String,
    pub duration_ms: u64,
}

pub async fn run_acp_turn(request: AcpRunRequest) -> Result<AcpRunResult, AcpError> {
    let AcpRunRequest {
        command,
        prompt,
        cwd,
        timeout_ms,
        env,
        sandbox,
        cancel_token,
        on_activity,
        on_session_activity,
        live_control,
    } = request;
    let live_control = live_control.unwrap_or_default();
    let start = std::time::Instant::now();
    let state = TransportState::new();
    let read_cancel_token = cancel_token.clone();
    let run_cancel_token = cancel_token.clone();
    let permission_cancel_token = cancel_token.clone();
    let transport = SandboxAcpTransport::new(command, cwd.clone(), env, sandbox, state.clone());

    let run = Client
        .builder()
        .name("fabro")
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _connection| {
                let outcome = if permission_cancel_token.is_cancelled() {
                    RequestPermissionOutcome::Cancelled
                } else {
                    select_permission_outcome(&request)
                };
                responder.respond(RequestPermissionResponse::new(outcome))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(transport, async move |cx| {
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;

            cx.build_session(&cwd)
                .block_task()
                .run_until(async |mut session| {
                    let prompt_response = send_prompt_with_response(&session, prompt)?;
                    read_live_session(
                        &mut session,
                        prompt_response,
                        &read_cancel_token,
                        &live_control.handle,
                        live_control.on_natural_completion.as_ref(),
                        live_control.on_steer_prompt.as_ref(),
                        on_activity.as_ref(),
                        on_session_activity.as_ref(),
                    )
                    .await
                })
                .await
        });

    let cancel_deadline_token = cancel_token.clone();
    let run_outcome = async {
        match timeout_ms {
            Some(timeout_ms) => {
                if let Ok(result) = timeout(Duration::from_millis(timeout_ms), run).await {
                    Ok(result)
                } else {
                    state.terminate().await?;
                    if run_cancel_token.is_cancelled() {
                        return Err(AcpError::Cancelled);
                    }
                    Err(AcpError::TimedOut {
                        exec_output_tail: state.exec_output_tail().await,
                    })
                }
            }
            None => Ok(run.await),
        }
    };
    let outcome = tokio::select! {
        result = run_outcome => result?,
        () = async {
            cancel_deadline_token.cancelled().await;
            sleep(Duration::from_millis(500)).await;
        } => {
            state.terminate().await?;
            return Err(AcpError::Cancelled);
        }
    };
    let (text, stop_reason, usage) = match outcome {
        Ok(result) => result,
        Err(_) if run_cancel_token.is_cancelled() => {
            state.terminate().await?;
            return Err(AcpError::Cancelled);
        }
        Err(error) => {
            state.terminate().await?;
            if let Some(startup_error) = state.take_startup_error().await {
                return Err(AcpError::Sandbox(startup_error));
            }
            if let Some(process_exit) = state.take_process_exit().await {
                return Err(AcpError::ProcessExited(process_exit));
            }
            return Err(map_protocol_error(error));
        }
    };

    match stop_reason {
        StopReason::EndTurn | StopReason::Refusal => {}
        StopReason::Cancelled => {
            state.terminate().await?;
            return Err(AcpError::Cancelled);
        }
        _ => {
            state.terminate().await?;
            return Err(AcpError::StopReason {
                stop_reason: render_stop_reason(&stop_reason),
                text,
            });
        }
    }

    state.terminate().await?;
    let stderr = state.stderr_tail().await;
    Ok(AcpRunResult {
        text,
        stop_reason,
        usage,
        stderr,
        duration_ms: elapsed_ms(start),
    })
}

fn map_protocol_error(error: ProtocolError) -> AcpError {
    AcpError::Protocol(error)
}

fn select_permission_outcome(request: &RequestPermissionRequest) -> RequestPermissionOutcome {
    let selected = request
        .options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::AllowAlways)
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| option.kind == PermissionOptionKind::AllowOnce)
        })
        .or_else(|| {
            request.options.iter().find(|option| {
                !matches!(
                    option.kind,
                    PermissionOptionKind::RejectOnce | PermissionOptionKind::RejectAlways
                )
            })
        });

    selected.map_or(RequestPermissionOutcome::Cancelled, |option| {
        RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option.option_id.clone()))
    })
}

fn send_prompt_with_response(
    session: &ActiveSession<'_, Agent>,
    prompt: String,
) -> Result<oneshot::Receiver<Result<PromptResponse, ProtocolError>>, ProtocolError> {
    let (tx, rx) = oneshot::channel();
    session
        .connection()
        .send_request_to(
            Agent,
            PromptRequest::new(session.session_id().clone(), vec![prompt.into()]),
        )
        .on_receiving_result(async move |result| {
            let _ = tx.send(result);
            Ok(())
        })?;
    Ok(rx)
}

enum LiveSessionEvent {
    SessionMessage(Result<SessionMessage, ProtocolError>),
    PromptResponse(
        Result<Result<PromptResponse, ProtocolError>, oneshot::error::RecvError>,
    ),
    ControlNotified,
    CancelRequested,
    CancelGraceElapsed,
    PostResponseDrainComplete,
}

async fn read_live_session(
    session: &mut ActiveSession<'_, Agent>,
    initial_prompt_response: oneshot::Receiver<Result<PromptResponse, ProtocolError>>,
    cancel_token: &CancellationToken,
    control_handle: &AcpControlHandle,
    on_natural_completion: Option<&AcpNaturalCompletionCallback>,
    on_steer_prompt: Option<&AcpSteerPromptCallback>,
    on_activity: Option<&Arc<dyn Fn() + Send + Sync>>,
    on_session_activity: Option<&AcpSessionActivityCallback>,
) -> Result<(String, StopReason, Option<AcpRunUsage>), ProtocolError> {
    let mut text = String::new();
    let mut prompt_active = true;
    let mut prompt_response = Some(initial_prompt_response);
    let mut pending_prompt_response: Option<PromptResponse> = None;
    let mut post_response_drain_deadline: Option<Instant> = None;
    let mut cancel_sent = false;
    let mut cancel_deadline: Option<Instant> = None;
    let mut session_updates_since_prompt_check = 0;
    let mut last_stop_reason: Option<StopReason> = None;
    let mut tracked_tools = HashMap::new();
    let mut usage_accumulator = AcpUsageAccumulator::default();
    loop {
        if !prompt_active {
            if let Some(message) = control_handle.pop_steer() {
                if let Some(on_steer_prompt) = on_steer_prompt {
                    on_steer_prompt(message.text.clone(), message.actor.clone());
                }
                prompt_response = Some(send_prompt_with_response(session, message.text)?);
                pending_prompt_response = None;
                post_response_drain_deadline = None;
                prompt_active = true;
                cancel_sent = false;
                cancel_deadline = None;
                session_updates_since_prompt_check = 0;
                continue;
            }

            if control_handle.take_interrupt_requested() {
                continue;
            }

            if control_handle.should_wait_for_steer() {
                let notified = control_handle.notified();
                tokio::select! {
                    () = cancel_token.cancelled() => {
                        return Ok((text, StopReason::Cancelled, usage_accumulator.finish()));
                    }
                    () = notified => {}
                }
                continue;
            }

            let stop_reason = last_stop_reason.unwrap_or(StopReason::EndTurn);
            if matches!(stop_reason, StopReason::EndTurn | StopReason::Refusal)
                && on_natural_completion.is_some_and(|callback| !callback())
            {
                // The lease reports pending control work but our flags didn't
                // observe it yet. Wait on a notify so we don't spin.
                let notified = control_handle.notified();
                tokio::select! {
                    () = cancel_token.cancelled() => {
                        return Ok((text, StopReason::Cancelled, usage_accumulator.finish()));
                    }
                    () = notified => {}
                }
                continue;
            }
            return Ok((text, stop_reason, usage_accumulator.finish()));
        }

        if control_handle.take_interrupt_requested() && !cancel_sent {
            cancel_sent = true;
            cancel_deadline = Some(Instant::now() + CANCEL_GRACE_PERIOD);
            send_cancel_notification(session)?;
        }

        let control_notified = control_handle.notified();
        let prioritize_session_updates =
            session_updates_since_prompt_check < MAX_SESSION_UPDATES_BEFORE_PROMPT_CHECK;
        let event = if pending_prompt_response.is_some() {
            tokio::select! {
                biased;
                () = async {
                    sleep_until(cancel_deadline.expect("cancel deadline is guarded")).await;
                }, if cancel_deadline.is_some() => LiveSessionEvent::CancelGraceElapsed,
                () = cancel_token.cancelled(), if !cancel_sent => {
                    LiveSessionEvent::CancelRequested
                }
                () = control_notified => LiveSessionEvent::ControlNotified,
                () = async {
                    sleep_until(
                        post_response_drain_deadline
                            .expect("post-response drain deadline is guarded"),
                    )
                    .await;
                }, if post_response_drain_deadline.is_some() => {
                    LiveSessionEvent::PostResponseDrainComplete
                }
                update = session.read_update() => LiveSessionEvent::SessionMessage(update),
                () = tokio::task::yield_now() => LiveSessionEvent::PostResponseDrainComplete,
            }
        } else if prioritize_session_updates {
            tokio::select! {
                biased;
                () = async {
                    sleep_until(cancel_deadline.expect("cancel deadline is guarded")).await;
                }, if cancel_deadline.is_some() => LiveSessionEvent::CancelGraceElapsed,
                () = cancel_token.cancelled(), if !cancel_sent => {
                    LiveSessionEvent::CancelRequested
                }
                () = control_notified => LiveSessionEvent::ControlNotified,
                update = session.read_update() => LiveSessionEvent::SessionMessage(update),
                response = async {
                    prompt_response
                        .as_mut()
                        .expect("prompt response receiver is guarded")
                        .await
                }, if prompt_response.is_some() => LiveSessionEvent::PromptResponse(response),
            }
        } else {
            tokio::select! {
                biased;
                () = async {
                    sleep_until(cancel_deadline.expect("cancel deadline is guarded")).await;
                }, if cancel_deadline.is_some() => LiveSessionEvent::CancelGraceElapsed,
                () = cancel_token.cancelled(), if !cancel_sent => {
                    LiveSessionEvent::CancelRequested
                }
                () = control_notified => LiveSessionEvent::ControlNotified,
                response = async {
                    prompt_response
                        .as_mut()
                        .expect("prompt response receiver is guarded")
                        .await
                }, if prompt_response.is_some() => LiveSessionEvent::PromptResponse(response),
                update = session.read_update() => LiveSessionEvent::SessionMessage(update),
            }
        };
        let event = match event {
            LiveSessionEvent::SessionMessage(Err(_)) if pending_prompt_response.is_some() => {
                // A completed prompt is authoritative over connection closure while
                // draining its already-enqueued notifications.
                LiveSessionEvent::PostResponseDrainComplete
            }
            event => event,
        };

        match event {
            LiveSessionEvent::SessionMessage(update) => {
                session_updates_since_prompt_check =
                    session_updates_since_prompt_check.saturating_add(1);
                if let Some(on_activity) = on_activity {
                    on_activity();
                }
                match update? {
                    SessionMessage::SessionMessage(dispatch) => {
                        MatchDispatch::new(dispatch)
                            .if_notification(async |notification: SessionNotification| {
                                if let SessionUpdate::UsageUpdate(update) = &notification.update {
                                    usage_accumulator.record_reported_cost(
                                        update.cost.as_ref().map(convert_reported_cost),
                                    );
                                }
                                if let Some(on_session_activity) = on_session_activity {
                                    for activity in convert_session_update(
                                        &notification.update,
                                        &mut tracked_tools,
                                    ) {
                                        on_session_activity(activity);
                                    }
                                }
                                if let SessionUpdate::AgentMessageChunk(ContentChunk {
                                    content: ContentBlock::Text(text_chunk),
                                    ..
                                }) = notification.update
                                {
                                    text.push_str(&text_chunk.text);
                                }
                                Ok(())
                            })
                            .await
                            .otherwise_ignore()?;
                    }
                    _ => {}
                }
            }
            LiveSessionEvent::PromptResponse(response) => {
                let Ok(response) = response else {
                    // The ACP connection owns the protocol error. Keep reading it from
                    // the session channel rather than replacing it with a channel error.
                    prompt_response = None;
                    continue;
                };
                let response = response?;
                if let Some(on_activity) = on_activity {
                    on_activity();
                }
                prompt_response = None;
                pending_prompt_response = Some(response);
                post_response_drain_deadline =
                    Some(Instant::now() + POST_RESPONSE_DRAIN_LIMIT);
                session_updates_since_prompt_check = 0;
            }
            LiveSessionEvent::PostResponseDrainComplete => {
                let response = pending_prompt_response
                    .take()
                    .expect("completed prompt response is guarded");
                post_response_drain_deadline = None;
                if let Some(usage) = response.usage.as_ref() {
                    usage_accumulator.add_prompt_usage(usage);
                }
                prompt_active = false;
                cancel_sent = false;
                cancel_deadline = None;
                session_updates_since_prompt_check = 0;
                last_stop_reason = Some(response.stop_reason);
            }
            LiveSessionEvent::ControlNotified => {
                if control_handle.take_interrupt_requested() && !cancel_sent {
                    cancel_sent = true;
                    cancel_deadline = Some(Instant::now() + CANCEL_GRACE_PERIOD);
                    send_cancel_notification(session)?;
                }
            }
            LiveSessionEvent::CancelRequested => {
                cancel_sent = true;
                cancel_deadline = Some(Instant::now() + CANCEL_GRACE_PERIOD);
                send_cancel_notification(session)?;
            }
            LiveSessionEvent::CancelGraceElapsed => {
                return Ok((text, StopReason::Cancelled, usage_accumulator.finish()));
            }
        }
    }
}

fn send_cancel_notification(session: &ActiveSession<'_, Agent>) -> Result<(), ProtocolError> {
    session
        .connection()
        .send_notification_to(Agent, CancelNotification::new(session.session_id().clone()))
}

#[must_use]
pub fn render_stop_reason(stop_reason: &StopReason) -> String {
    serde_json::to_value(stop_reason)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{stop_reason:?}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use agent_client_protocol::schema::{
        SessionNotification, SessionUpdate, ToolCall, ToolCallStatus, ToolCallUpdate,
        ToolCallUpdateFields, ToolKind, Usage,
    };

    use super::{
        AcpReportedCost, AcpRunUsage, AcpSessionActivity, AcpToolKind, AcpUsageAccumulator,
        convert_session_update,
    };

    #[test]
    fn usage_update_preserves_context_telemetry_and_cost() {
        let notification = serde_json::json!({
            "sessionId": "session-1",
            "update": {
                "sessionUpdate": "usage_update",
                "used": 26128,
                "size": 258_400,
                "cost": {
                    "amount": 0.0123,
                    "currency": "USD"
                }
            }
        });
        let notification =
            serde_json::from_value::<SessionNotification>(notification).expect("valid usage update");

        assert_eq!(
            convert_session_update(&notification.update, &mut HashMap::new()),
            vec![AcpSessionActivity::UsageUpdated {
                used: 26128,
                size: 258_400,
                cost: Some(AcpReportedCost {
                    amount: 0.0123,
                    currency: "USD".to_string(),
                }),
            }]
        );
    }

    #[test]
    fn usage_accumulator_sums_disjoint_prompt_buckets_once_and_keeps_latest_cost() {
        let mut accumulator = AcpUsageAccumulator::default();
        accumulator.add_prompt_usage(
            &Usage::new(999, 10, 20)
                .thought_tokens(3)
                .cached_read_tokens(4)
                .cached_write_tokens(5),
        );
        accumulator.record_reported_cost(Some(AcpReportedCost {
            amount: 0.01,
            currency: "USD".to_string(),
        }));
        accumulator.add_prompt_usage(
            &Usage::new(1, 100, 200)
                .thought_tokens(30)
                .cached_read_tokens(40)
                .cached_write_tokens(50),
        );
        accumulator.record_reported_cost(Some(AcpReportedCost {
            amount: 0.02,
            currency: "USD".to_string(),
        }));
        accumulator.record_reported_cost(None);

        assert_eq!(
            accumulator.finish(),
            Some(AcpRunUsage {
                input_tokens: 110,
                output_tokens: 220,
                reasoning_tokens: 33,
                cache_read_tokens: 44,
                cache_write_tokens: 55,
                total_tokens: 462,
                reported_cost: Some(AcpReportedCost {
                    amount: 0.02,
                    currency: "USD".to_string(),
                }),
            })
        );
    }

    #[test]
    fn usage_accumulator_retains_latest_valid_cost_after_invalid_updates() {
        let mut accumulator = AcpUsageAccumulator::default();
        accumulator.record_reported_cost(Some(AcpReportedCost {
            amount: 0.02,
            currency: "Usd".to_string(),
        }));

        for cost in [
            AcpReportedCost {
                amount: -1.0,
                currency: "USD".to_string(),
            },
            AcpReportedCost {
                amount: f64::NAN,
                currency: "usd".to_string(),
            },
            AcpReportedCost {
                amount: 1.0,
                currency: "EUR".to_string(),
            },
        ] {
            accumulator.record_reported_cost(Some(cost));
        }
        accumulator.record_reported_cost(None);

        assert_eq!(
            accumulator.finish().unwrap().reported_cost,
            Some(AcpReportedCost {
                amount: 0.02,
                currency: "Usd".to_string(),
            })
        );
    }

    #[test]
    fn usage_accumulator_without_prompt_usage_or_cost_returns_none() {
        assert_eq!(AcpUsageAccumulator::default().finish(), None);
    }

    #[test]
    fn tool_call_preserves_structured_kind() {
        let mut tracked = HashMap::new();
        let started = SessionUpdate::ToolCall(
            ToolCall::new("call-read", "Read file /tmp/a.rs")
                .kind(ToolKind::Read)
                .raw_input(serde_json::json!({"path": "/tmp/a.rs"})),
        );

        let events = convert_session_update(&started, &mut tracked);
        assert!(matches!(
            &events[0],
            AcpSessionActivity::ToolStarted {
                kind: AcpToolKind::Read,
                ..
            }
        ));
    }

    #[test]
    fn tool_call_start_and_completion_emit_once() {
        let mut tracked = HashMap::new();
        let started = SessionUpdate::ToolCall(
            ToolCall::new("call-1", "Read file")
                .raw_input(serde_json::json!({"path": "src/main.rs"})),
        );
        let first = convert_session_update(&started, &mut tracked);
        assert_eq!(first.len(), 1);
        assert!(matches!(
            &first[0],
            AcpSessionActivity::ToolStarted { tool_call_id, title, .. }
                if tool_call_id == "call-1" && title == "Read file"
        ));

        let duplicate = convert_session_update(&started, &mut tracked);
        assert!(duplicate.is_empty());

        let completed = SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            "call-1",
            ToolCallUpdateFields::new()
                .status(ToolCallStatus::Completed)
                .raw_output(serde_json::json!({"ok": true})),
        ));
        let done = convert_session_update(&completed, &mut tracked);
        assert_eq!(done.len(), 1);
        assert!(matches!(
            &done[0],
            AcpSessionActivity::ToolCompleted { is_error, .. } if !is_error
        ));
        assert!(convert_session_update(&completed, &mut tracked).is_empty());
    }

    #[test]
    fn failed_tool_call_retains_error_output() {
        let mut tracked = HashMap::new();
        let failed = SessionUpdate::ToolCall(
            ToolCall::new("call-err", "Bash")
                .status(ToolCallStatus::Failed)
                .raw_output(serde_json::json!({"stderr": "boom"})),
        );
        let events = convert_session_update(&failed, &mut tracked);
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[1],
            AcpSessionActivity::ToolCompleted { is_error, output, .. }
                if *is_error && output["stderr"] == "boom"
        ));
    }
}
