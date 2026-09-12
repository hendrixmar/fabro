pub mod agent;
pub mod infra;
pub mod misc;
pub mod run;
pub mod session;
pub mod stage;

pub use agent::*;
use chrono::{DateTime, Utc};
pub use infra::*;
pub use misc::*;
pub use pebble_coding_agent::events::{ExecOutputTail, ExecOutputTailTrace};
pub use run::*;
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value, json};
pub use session::*;
pub use stage::*;

use crate::{BilledTokenCounts, ParallelBranchId, Principal, RunId, StageId};

/// Maximum accepted body size for `POST /runs/{id}/events`.
///
/// Producers that embed large payloads in an event (serialized tool output in
/// particular) must budget against this limit, leaving headroom for the rest
/// of the event envelope. The agent layer reserves half of it for serialized
/// tool output.
pub const MAX_RUN_EVENT_BODY_BYTES: usize = 3 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunNoticeLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunEvent {
    pub id:                 String,
    pub ts:                 DateTime<Utc>,
    pub run_id:             RunId,
    pub node_id:            Option<String>,
    pub node_label:         Option<String>,
    pub stage_id:           Option<StageId>,
    pub parallel_group_id:  Option<StageId>,
    pub parallel_branch_id: Option<ParallelBranchId>,
    pub session_id:         Option<String>,
    pub parent_session_id:  Option<String>,
    pub tool_call_id:       Option<String>,
    pub actor:              Option<Principal>,
    pub body:               EventBody,
}

#[allow(
    clippy::large_enum_variant,
    reason = "Run event bodies stay inline to match the tagged wire format."
)]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "event", content = "properties")]
pub enum EventBody {
    #[serde(rename = "run.created")]
    RunCreated(RunCreatedProps),
    #[serde(rename = "run.started")]
    RunStarted(RunStartedProps),
    #[serde(rename = "run.submitted")]
    RunSubmitted(RunSubmittedProps),
    #[serde(rename = "run.start_requested")]
    RunStartRequested(RunStartRequestedProps),
    #[serde(rename = "run.pending")]
    RunPending(RunPendingProps),
    #[serde(rename = "run.approved")]
    RunApproved(RunApprovedProps),
    #[serde(rename = "run.denied")]
    RunDenied(RunDeniedProps),
    #[serde(rename = "run.runnable")]
    RunRunnable(RunRunnableProps),
    #[serde(rename = "run.starting")]
    RunStarting(RunStatusTransitionProps),
    #[serde(rename = "run.running")]
    RunRunning(RunStatusTransitionProps),
    #[serde(rename = "run.interrupt")]
    RunInterrupt(RunInterruptProps),
    #[serde(rename = "run.steer")]
    RunSteer(RunSteerProps),
    #[serde(rename = "run.pair.started")]
    RunPairStarted(RunPairStartedProps),
    #[serde(rename = "run.pair.ended")]
    RunPairEnded(RunPairEndedProps),
    #[serde(rename = "run.pair.failed")]
    RunPairFailed(RunPairFailedProps),
    #[serde(rename = "run.blocked")]
    RunBlocked(RunBlockedProps),
    #[serde(rename = "run.unblocked")]
    RunUnblocked(RunStatusEffectProps),
    #[serde(rename = "run.removing")]
    RunRemoving(RunStatusTransitionProps),
    #[serde(rename = "run.cancel.requested")]
    RunCancelRequested(RunControlRequestedProps),
    #[serde(rename = "run.pause.requested")]
    RunPauseRequested(RunControlRequestedProps),
    #[serde(rename = "run.unpause.requested")]
    RunUnpauseRequested(RunControlRequestedProps),
    #[serde(rename = "run.paused")]
    RunPaused(RunControlEffectProps),
    #[serde(rename = "run.unpaused")]
    RunUnpaused(RunControlEffectProps),
    #[serde(rename = "run.superseded_by")]
    RunSupersededBy(RunSupersededByProps),
    #[serde(rename = "run.archived")]
    RunArchived(RunArchivedProps),
    #[serde(rename = "run.unarchived")]
    RunUnarchived(RunUnarchivedProps),
    #[serde(rename = "run.title.updated")]
    RunTitleUpdated(RunTitleUpdatedProps),
    #[serde(rename = "run.session.created")]
    RunSessionCreated(RunSessionCreatedProps),
    #[serde(rename = "run.session.turn.started")]
    RunSessionTurnStarted(RunSessionTurnStartedProps),
    #[serde(rename = "run.session.user_message")]
    RunSessionUserMessage(RunSessionUserMessageProps),
    #[serde(rename = "run.session.assistant_delta")]
    RunSessionAssistantDelta(RunSessionAssistantDeltaProps),
    #[serde(rename = "run.session.assistant_message")]
    RunSessionAssistantMessage(RunSessionAssistantMessageProps),
    #[serde(rename = "run.session.tool_call.started")]
    RunSessionToolCallStarted(RunSessionToolCallStartedProps),
    #[serde(rename = "run.session.tool_call.completed")]
    RunSessionToolCallCompleted(RunSessionToolCallCompletedProps),
    #[serde(rename = "run.session.turn.succeeded")]
    RunSessionTurnSucceeded(RunSessionTurnSucceededProps),
    #[serde(rename = "run.session.turn.failed")]
    RunSessionTurnFailed(RunSessionTurnFailedProps),
    #[serde(rename = "run.session.turn.interrupted")]
    RunSessionTurnInterrupted(RunSessionTurnInterruptedProps),
    #[serde(rename = "run.parent.linked")]
    RunParentLinked(RunParentLinkedProps),
    #[serde(rename = "run.parent.unlinked")]
    RunParentUnlinked(RunParentUnlinkedProps),
    #[serde(rename = "run.completed")]
    RunCompleted(RunCompletedProps),
    #[serde(rename = "run.failed")]
    RunFailed(RunFailedProps),
    #[serde(rename = "run.notice")]
    RunNotice(RunNoticeProps),
    #[serde(rename = "metadata.snapshot.started")]
    MetadataSnapshotStarted(MetadataSnapshotStartedProps),
    #[serde(rename = "metadata.snapshot.completed")]
    MetadataSnapshotCompleted(MetadataSnapshotCompletedProps),
    #[serde(rename = "metadata.snapshot.failed")]
    MetadataSnapshotFailed(MetadataSnapshotFailedProps),
    #[serde(rename = "stage.started")]
    StageStarted(StageStartedProps),
    #[serde(rename = "stage.completed")]
    StageCompleted(StageCompletedProps),
    #[serde(rename = "stage.failed")]
    StageFailed(StageFailedProps),
    #[serde(rename = "stage.retrying")]
    StageRetrying(StageRetryingProps),
    #[serde(rename = "parallel.started")]
    ParallelStarted(ParallelStartedProps),
    #[serde(rename = "parallel.branch.started")]
    ParallelBranchStarted(ParallelBranchStartedProps),
    #[serde(rename = "parallel.branch.completed")]
    ParallelBranchCompleted(ParallelBranchCompletedProps),
    #[serde(rename = "parallel.completed")]
    ParallelCompleted(ParallelCompletedProps),
    #[serde(rename = "interview.started")]
    InterviewStarted(InterviewStartedProps),
    #[serde(rename = "interview.completed")]
    InterviewCompleted(InterviewCompletedProps),
    #[serde(rename = "interview.timeout")]
    InterviewTimeout(InterviewTimeoutProps),
    #[serde(rename = "interview.interrupted")]
    InterviewInterrupted(InterviewInterruptedProps),
    #[serde(rename = "checkpoint.completed")]
    CheckpointCompleted(CheckpointCompletedProps),
    #[serde(rename = "checkpoint.failed")]
    CheckpointFailed(CheckpointFailedProps),
    #[serde(rename = "git.commit")]
    GitCommit(GitCommitProps),
    #[serde(rename = "git.push")]
    GitPush(GitPushProps),
    #[serde(rename = "git.fetch")]
    GitFetch(GitFetchProps),
    #[serde(rename = "git.reset")]
    GitReset(GitResetProps),
    #[serde(rename = "edge.selected")]
    EdgeSelected(EdgeSelectedProps),
    #[serde(rename = "loop.restart")]
    LoopRestart(LoopRestartProps),
    #[serde(rename = "stage.prompt")]
    StagePrompt(StagePromptProps),
    #[serde(rename = "prompt.completed")]
    PromptCompleted(PromptCompletedProps),
    /// One pebble coding-agent event. The wire name is derived from the
    /// inner `CodingEvent` variant (`agent.message`, `todo.created`, ...);
    /// see [`AgentEventProps::event_name`]. The derive's own tag is only a
    /// fallback for direct `EventBody` serialization; `RunEvent` writes and
    /// reads the derived name.
    #[serde(rename = "agent.event")]
    Agent(AgentEventProps),
    #[serde(rename = "agent.session.activated")]
    AgentSessionActivated(AgentSessionActivatedProps),
    #[serde(rename = "agent.tools.available")]
    AgentToolsAvailable(AgentToolsAvailableProps),
    #[serde(rename = "agent.session.deactivated")]
    AgentSessionDeactivated(AgentSessionDeactivatedProps),
    #[serde(rename = "agent.pair.user_message")]
    AgentPairUserMessage(AgentPairUserMessageProps),
    #[serde(rename = "agent.pair.system_message")]
    AgentPairSystemMessage(AgentPairSystemMessageProps),
    #[serde(rename = "agent.interrupt.injected")]
    AgentInterruptInjected(AgentInterruptInjectedProps),
    #[serde(rename = "agent.steer.buffered")]
    AgentSteerBuffered(AgentSteerBufferedProps),
    #[serde(rename = "agent.steer.dropped")]
    AgentSteerDropped(AgentSteerDroppedProps),
    #[serde(rename = "agent.mcp.ready")]
    AgentMcpReady(AgentMcpReadyProps),
    #[serde(rename = "agent.mcp.failed")]
    AgentMcpFailed(AgentMcpFailedProps),
    #[serde(rename = "agent.mcp.disconnected")]
    AgentMcpDisconnected(AgentMcpDisconnectedProps),
    #[serde(rename = "subgraph.started")]
    SubgraphStarted(SubgraphStartedProps),
    #[serde(rename = "subgraph.completed")]
    SubgraphCompleted(SubgraphCompletedProps),
    #[serde(rename = "sandbox.initializing")]
    SandboxInitializing(SandboxInitializingProps),
    #[serde(rename = "sandbox.ready")]
    SandboxReady(SandboxReadyProps),
    #[serde(rename = "sandbox.failed")]
    SandboxFailed(SandboxFailedProps),
    /// An event the sandbox driver reported about the run's sandbox, a
    /// snapshot, a volume, or the provider, stored as the driver's own event
    /// under a name derived from it (`sandbox.stop.completed`,
    /// `snapshot.create.started`, `sandbox.state`); see
    /// [`sandbox_driver_event_name`]. The derive never sees this variant:
    /// the run event writes the name and the driver's event itself.
    #[serde(skip)]
    SandboxDriver {
        name:  String,
        event: sandbox_driver::Event,
    },
    #[serde(rename = "sandbox.initialized")]
    SandboxInitialized(SandboxInitializedProps),
    #[serde(rename = "setup.started")]
    SetupStarted(SetupStartedProps),
    #[serde(rename = "setup.command.started")]
    SetupCommandStarted(SetupCommandStartedProps),
    #[serde(rename = "setup.command.completed")]
    SetupCommandCompleted(SetupCommandCompletedProps),
    #[serde(rename = "setup.completed")]
    SetupCompleted(SetupCompletedProps),
    #[serde(rename = "git.identity.resolved")]
    GitIdentityResolved(GitIdentityResolvedProps),
    #[serde(rename = "setup.failed")]
    SetupFailed(SetupFailedProps),
    #[serde(rename = "watchdog.timeout")]
    StallWatchdogTimeout(StallWatchdogTimeoutProps),
    #[serde(rename = "artifact.captured")]
    ArtifactCaptured(ArtifactCapturedProps),
    #[serde(rename = "ssh.ready")]
    SshAccessReady(SshAccessReadyProps),
    #[serde(rename = "agent.failover")]
    Failover(FailoverProps),
    #[serde(rename = "cli.ensure.started")]
    CliEnsureStarted(CliEnsureStartedProps),
    #[serde(rename = "cli.ensure.completed")]
    CliEnsureCompleted(CliEnsureCompletedProps),
    #[serde(rename = "cli.ensure.failed")]
    CliEnsureFailed(CliEnsureFailedProps),
    #[serde(rename = "command.started")]
    CommandStarted(CommandStartedProps),
    #[serde(rename = "command.completed")]
    CommandCompleted(CommandCompletedProps),
    #[serde(rename = "agent.acp.started")]
    AgentAcpStarted(AgentAcpStartedProps),
    #[serde(rename = "agent.acp.completed")]
    AgentAcpCompleted(AgentAcpCompletedProps),
    #[serde(rename = "agent.acp.cancelled")]
    AgentAcpCancelled(AgentAcpCancelledProps),
    #[serde(rename = "agent.acp.timed_out")]
    AgentAcpTimedOut(AgentAcpTimedOutProps),
    #[serde(rename = "pull_request.creation_requested")]
    PullRequestCreationRequested(PullRequestCreationRequestedProps),
    #[serde(rename = "pull_request.created")]
    PullRequestCreated(PullRequestCreatedProps),
    #[serde(rename = "pull_request.linked")]
    PullRequestLinked(PullRequestLinkedProps),
    #[serde(rename = "pull_request.unlinked")]
    PullRequestUnlinked(PullRequestUnlinkedProps),
    #[serde(rename = "pull_request.failed")]
    PullRequestFailed(PullRequestFailedProps),
    Unknown {
        name:       String,
        properties: Value,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct RunEventRaw {
    id:                 String,
    ts:                 DateTime<Utc>,
    run_id:             RunId,
    #[serde(default)]
    node_id:            Option<String>,
    #[serde(default)]
    node_label:         Option<String>,
    #[serde(default)]
    stage_id:           Option<StageId>,
    #[serde(default)]
    parallel_group_id:  Option<StageId>,
    #[serde(default)]
    parallel_branch_id: Option<ParallelBranchId>,
    #[serde(default)]
    session_id:         Option<String>,
    #[serde(default)]
    parent_session_id:  Option<String>,
    #[serde(default)]
    tool_call_id:       Option<String>,
    #[serde(default)]
    actor:              Option<Principal>,
    event:              String,
    #[serde(default = "default_properties")]
    properties:         Value,
}

fn default_properties() -> Value {
    Value::Object(Map::new())
}

struct RunEventParts<'a> {
    id:                 String,
    ts:                 DateTime<Utc>,
    run_id:             RunId,
    node_id:            Option<String>,
    node_label:         Option<String>,
    stage_id:           Option<StageId>,
    parallel_group_id:  Option<StageId>,
    parallel_branch_id: Option<ParallelBranchId>,
    session_id:         Option<String>,
    parent_session_id:  Option<String>,
    tool_call_id:       Option<String>,
    actor:              Option<Principal>,
    event:              &'a str,
    properties:         &'a Value,
}

impl EventBody {
    /// The sandbox driver's event as a run event body, named by
    /// [`sandbox_driver_event_name`].
    #[must_use]
    pub fn sandbox_driver(event: sandbox_driver::Event) -> Self {
        Self::SandboxDriver {
            name: sandbox_driver_event_name(&event),
            event,
        }
    }

    /// A stored driver event: `name` has the shape the driver's events are
    /// stored under and `properties` decode to a driver event that yields
    /// that name. Anything else, including an event stored under one of
    /// these names before the driver's events were kept whole, is left to
    /// the other variants.
    fn sandbox_driver_from_stored(name: &str, properties: &Value) -> Option<Self> {
        if !is_sandbox_driver_event_name(name) {
            return None;
        }
        let event: sandbox_driver::Event = serde_json::from_value(properties.clone()).ok()?;
        (sandbox_driver_event_name(&event) == name).then(|| Self::SandboxDriver {
            name: name.to_owned(),
            event,
        })
    }
}

/// Whether `name` has the shape the sandbox driver's events are stored
/// under: `<subject>.<action>.<phase>`, `<subject>.state`, `<subject>.notice`,
/// or `<subject>.event`, for the subjects the driver reports on.
fn is_sandbox_driver_event_name(name: &str) -> bool {
    let Some((subject, rest)) = name.split_once('.') else {
        return false;
    };
    matches!(subject, "sandbox" | "snapshot" | "volume" | "provider")
        && (matches!(rest, "state" | "notice" | "event")
            || rest.split_once('.').is_some_and(|(_, phase)| {
                matches!(phase, "started" | "progress" | "completed" | "failed")
            }))
}

/// The run event name for a sandbox driver event: the subject kind, the
/// action, and the phase, so a stop on the sandbox is `sandbox.stop.started`,
/// `sandbox.stop.completed`, or `sandbox.stop.failed`, an image pull inside
/// a create is `sandbox.create.progress`, and a snapshot build is
/// `snapshot.create.*`. A state observation is `<subject>.state`, a notice
/// `<subject>.notice`, and an event kind this build does not know
/// `<subject>.event`.
#[must_use]
pub fn sandbox_driver_event_name(event: &sandbox_driver::Event) -> String {
    use sandbox_driver::{EventBody as Body, EventSubject};

    let subject = match &event.subject {
        EventSubject::Snapshot { .. } => "snapshot",
        EventSubject::Volume { .. } => "volume",
        EventSubject::Provider => "provider",
        _ => "sandbox",
    };
    let (action, phase) = match &event.body {
        Body::OperationStarted { action } => (Some(*action), "started"),
        Body::OperationProgress { action, .. } => (Some(*action), "progress"),
        Body::OperationCompleted { action, .. } => (Some(*action), "completed"),
        Body::OperationFailed { action, .. } => (Some(*action), "failed"),
        Body::StateObserved { .. } => (None, "state"),
        Body::Notice { .. } => (None, "notice"),
        _ => (None, "event"),
    };
    match action {
        Some(action) => format!("{subject}.{}.{phase}", driver_action_name(action)),
        None => format!("{subject}.{phase}"),
    }
}

/// The driver action's wire name (`stop`, `refresh_activity`).
fn driver_action_name(action: sandbox_driver::Action) -> String {
    match serde_json::to_value(action) {
        Ok(Value::String(name)) => name,
        _ => "unknown".to_owned(),
    }
}

impl EventBody {
    pub fn event_name(&self) -> &str {
        match self {
            Self::RunCreated(_) => "run.created",
            Self::RunStarted(_) => "run.started",
            Self::RunSubmitted(_) => "run.submitted",
            Self::RunStartRequested(_) => "run.start_requested",
            Self::RunPending(_) => "run.pending",
            Self::RunApproved(_) => "run.approved",
            Self::RunDenied(_) => "run.denied",
            Self::RunRunnable(_) => "run.runnable",
            Self::RunStarting(_) => "run.starting",
            Self::RunRunning(_) => "run.running",
            Self::RunInterrupt(_) => "run.interrupt",
            Self::RunSteer(_) => "run.steer",
            Self::RunPairStarted(_) => "run.pair.started",
            Self::RunPairEnded(_) => "run.pair.ended",
            Self::RunPairFailed(_) => "run.pair.failed",
            Self::RunBlocked(_) => "run.blocked",
            Self::RunUnblocked(_) => "run.unblocked",
            Self::RunRemoving(_) => "run.removing",
            Self::RunCancelRequested(_) => "run.cancel.requested",
            Self::RunPauseRequested(_) => "run.pause.requested",
            Self::RunUnpauseRequested(_) => "run.unpause.requested",
            Self::RunPaused(_) => "run.paused",
            Self::RunUnpaused(_) => "run.unpaused",
            Self::RunSupersededBy(_) => "run.superseded_by",
            Self::RunArchived(_) => "run.archived",
            Self::RunUnarchived(_) => "run.unarchived",
            Self::RunTitleUpdated(_) => "run.title.updated",
            Self::RunSessionCreated(_) => "run.session.created",
            Self::RunSessionTurnStarted(_) => "run.session.turn.started",
            Self::RunSessionUserMessage(_) => "run.session.user_message",
            Self::RunSessionAssistantDelta(_) => "run.session.assistant_delta",
            Self::RunSessionAssistantMessage(_) => "run.session.assistant_message",
            Self::RunSessionToolCallStarted(_) => "run.session.tool_call.started",
            Self::RunSessionToolCallCompleted(_) => "run.session.tool_call.completed",
            Self::RunSessionTurnSucceeded(_) => "run.session.turn.succeeded",
            Self::RunSessionTurnFailed(_) => "run.session.turn.failed",
            Self::RunSessionTurnInterrupted(_) => "run.session.turn.interrupted",
            Self::RunParentLinked(_) => "run.parent.linked",
            Self::RunParentUnlinked(_) => "run.parent.unlinked",
            Self::RunCompleted(_) => "run.completed",
            Self::RunFailed(_) => "run.failed",
            Self::RunNotice(_) => "run.notice",
            Self::MetadataSnapshotStarted(_) => "metadata.snapshot.started",
            Self::MetadataSnapshotCompleted(_) => "metadata.snapshot.completed",
            Self::MetadataSnapshotFailed(_) => "metadata.snapshot.failed",
            Self::StageStarted(_) => "stage.started",
            Self::StageCompleted(_) => "stage.completed",
            Self::StageFailed(_) => "stage.failed",
            Self::StageRetrying(_) => "stage.retrying",
            Self::ParallelStarted(_) => "parallel.started",
            Self::ParallelBranchStarted(_) => "parallel.branch.started",
            Self::ParallelBranchCompleted(_) => "parallel.branch.completed",
            Self::ParallelCompleted(_) => "parallel.completed",
            Self::InterviewStarted(_) => "interview.started",
            Self::InterviewCompleted(_) => "interview.completed",
            Self::InterviewTimeout(_) => "interview.timeout",
            Self::InterviewInterrupted(_) => "interview.interrupted",
            Self::CheckpointCompleted(_) => "checkpoint.completed",
            Self::CheckpointFailed(_) => "checkpoint.failed",
            Self::GitCommit(_) => "git.commit",
            Self::GitPush(_) => "git.push",
            Self::GitFetch(_) => "git.fetch",
            Self::GitReset(_) => "git.reset",
            Self::EdgeSelected(_) => "edge.selected",
            Self::LoopRestart(_) => "loop.restart",
            Self::StagePrompt(_) => "stage.prompt",
            Self::PromptCompleted(_) => "prompt.completed",
            Self::Agent(props) => props.event_name(),
            Self::AgentSessionActivated(_) => "agent.session.activated",
            Self::AgentToolsAvailable(_) => "agent.tools.available",
            Self::AgentSessionDeactivated(_) => "agent.session.deactivated",
            Self::AgentPairUserMessage(_) => "agent.pair.user_message",
            Self::AgentPairSystemMessage(_) => "agent.pair.system_message",
            Self::AgentInterruptInjected(_) => "agent.interrupt.injected",
            Self::AgentSteerBuffered(_) => "agent.steer.buffered",
            Self::AgentSteerDropped(_) => "agent.steer.dropped",
            Self::AgentMcpReady(_) => "agent.mcp.ready",
            Self::AgentMcpFailed(_) => "agent.mcp.failed",
            Self::AgentMcpDisconnected(_) => "agent.mcp.disconnected",
            Self::SubgraphStarted(_) => "subgraph.started",
            Self::SubgraphCompleted(_) => "subgraph.completed",
            Self::SandboxInitializing(_) => "sandbox.initializing",
            Self::SandboxReady(_) => "sandbox.ready",
            Self::SandboxFailed(_) => "sandbox.failed",
            Self::SandboxInitialized(_) => "sandbox.initialized",
            Self::SetupStarted(_) => "setup.started",
            Self::SetupCommandStarted(_) => "setup.command.started",
            Self::SetupCommandCompleted(_) => "setup.command.completed",
            Self::SetupCompleted(_) => "setup.completed",
            Self::GitIdentityResolved(_) => "git.identity.resolved",
            Self::SetupFailed(_) => "setup.failed",
            Self::StallWatchdogTimeout(_) => "watchdog.timeout",
            Self::ArtifactCaptured(_) => "artifact.captured",
            Self::SshAccessReady(_) => "ssh.ready",
            Self::Failover(_) => "agent.failover",
            Self::CliEnsureStarted(_) => "cli.ensure.started",
            Self::CliEnsureCompleted(_) => "cli.ensure.completed",
            Self::CliEnsureFailed(_) => "cli.ensure.failed",
            Self::CommandStarted(_) => "command.started",
            Self::CommandCompleted(_) => "command.completed",
            Self::AgentAcpStarted(_) => "agent.acp.started",
            Self::AgentAcpCompleted(_) => "agent.acp.completed",
            Self::AgentAcpCancelled(_) => "agent.acp.cancelled",
            Self::AgentAcpTimedOut(_) => "agent.acp.timed_out",
            Self::PullRequestCreationRequested(_) => "pull_request.creation_requested",
            Self::PullRequestCreated(_) => "pull_request.created",
            Self::PullRequestLinked(_) => "pull_request.linked",
            Self::PullRequestUnlinked(_) => "pull_request.unlinked",
            Self::PullRequestFailed(_) => "pull_request.failed",
            Self::SandboxDriver { name, .. } | Self::Unknown { name, .. } => name.as_str(),
        }
    }

    pub fn is_run_session_event(&self) -> bool {
        self.event_name().starts_with("run.session.")
    }

    fn properties_value(&self) -> serde_json::Result<Value> {
        match self {
            Self::Unknown { properties, .. } => return Ok(properties.clone()),
            Self::Agent(props) => return serde_json::to_value(props),
            _ => {}
        }
        if let Self::SandboxDriver { event, .. } = self {
            return serde_json::to_value(event);
        }

        match serde_json::to_value(self)? {
            Value::Object(mut map) => {
                Ok(map.remove("properties").unwrap_or_else(default_properties))
            }
            _ => Ok(default_properties()),
        }
    }
}

fn is_known_event_name(event: &str) -> bool {
    is_coding_event_name(event)
        || matches!(
            event,
            "run.created"
                | "run.started"
                | "run.submitted"
                | "run.start_requested"
                | "run.pending"
                | "run.approved"
                | "run.denied"
                | "run.runnable"
                | "run.starting"
                | "run.running"
                | "run.interrupt"
                | "run.steer"
                | "run.pair.started"
                | "run.pair.ended"
                | "run.pair.failed"
                | "run.blocked"
                | "run.unblocked"
                | "run.removing"
                | "run.superseded_by"
                | "run.archived"
                | "run.unarchived"
                | "run.title.updated"
                | "run.session.created"
                | "run.session.turn.started"
                | "run.session.user_message"
                | "run.session.assistant_delta"
                | "run.session.assistant_message"
                | "run.session.tool_call.started"
                | "run.session.tool_call.completed"
                | "run.session.turn.succeeded"
                | "run.session.turn.failed"
                | "run.session.turn.interrupted"
                | "run.parent.linked"
                | "run.parent.unlinked"
                | "run.completed"
                | "run.failed"
                | "run.notice"
                | "metadata.snapshot.started"
                | "metadata.snapshot.completed"
                | "metadata.snapshot.failed"
                | "stage.started"
                | "stage.completed"
                | "stage.failed"
                | "stage.retrying"
                | "parallel.started"
                | "parallel.branch.started"
                | "parallel.branch.completed"
                | "parallel.completed"
                | "interview.started"
                | "interview.completed"
                | "interview.timeout"
                | "interview.interrupted"
                | "checkpoint.completed"
                | "checkpoint.failed"
                | "git.commit"
                | "git.push"
                | "git.fetch"
                | "git.reset"
                | "edge.selected"
                | "loop.restart"
                | "stage.prompt"
                | "prompt.completed"
                | "agent.session.activated"
                | "agent.tools.available"
                | "agent.session.deactivated"
                | "agent.pair.user_message"
                | "agent.pair.system_message"
                | "agent.interrupt.injected"
                | "agent.steer.buffered"
                | "agent.steer.dropped"
                | "agent.mcp.ready"
                | "agent.mcp.failed"
                | "agent.mcp.disconnected"
                | "subgraph.started"
                | "subgraph.completed"
                | "sandbox.initializing"
                | "sandbox.ready"
                | "sandbox.failed"
                | "sandbox.cleanup.started"
                | "sandbox.cleanup.completed"
                | "sandbox.cleanup.failed"
                | "sandbox.git.started"
                | "sandbox.git.completed"
                | "sandbox.git.failed"
                | "sandbox.initialized"
                | "setup.started"
                | "setup.command.started"
                | "setup.command.completed"
                | "setup.completed"
                | "setup.failed"
                | "git.identity.resolved"
                | "watchdog.timeout"
                | "artifact.captured"
                | "ssh.ready"
                | "agent.failover"
                | "cli.ensure.started"
                | "cli.ensure.completed"
                | "cli.ensure.failed"
                | "command.started"
                | "command.completed"
                | "agent.acp.started"
                | "agent.acp.completed"
                | "agent.acp.cancelled"
                | "agent.acp.timed_out"
                | "pull_request.created"
                | "pull_request.linked"
                | "pull_request.unlinked"
                | "pull_request.failed"
                | "agent.session.started"
                | "agent.session.ended"
                | "agent.processing.end"
                | "agent.input"
                | "agent.message"
                | "agent.tool.started"
                | "agent.tool.completed"
                | "agent.tool.process.completed"
                | "agent.error"
                | "agent.warning"
                | "agent.loop.detected"
                | "agent.steering.injected"
                | "agent.round.interrupted"
                | "agent.compaction.started"
                | "agent.compaction.completed"
                | "agent.llm.started"
                | "agent.llm.first_output"
                | "agent.llm.retry"
                | "agent.sub.spawned"
                | "agent.sub.turn.started"
                | "agent.sub.completed"
                | "agent.sub.failed"
                | "agent.sub.closed"
                | "agent.memory.loaded"
                | "agent.skills.discovered"
                | "agent.skill.activated"
                | "todo.created"
                | "todo.updated"
                | "todo.deleted"
        )
}

impl RunEvent {
    pub fn from_value(mut value: Value) -> serde_json::Result<Self> {
        normalize_legacy_event(&mut value);
        let raw: RunEventRaw = serde_json::from_value(value)?;
        Self::from_parts(RunEventParts {
            id:                 raw.id,
            ts:                 raw.ts,
            run_id:             raw.run_id,
            node_id:            raw.node_id,
            node_label:         raw.node_label,
            stage_id:           raw.stage_id,
            parallel_group_id:  raw.parallel_group_id,
            parallel_branch_id: raw.parallel_branch_id,
            session_id:         raw.session_id,
            parent_session_id:  raw.parent_session_id,
            tool_call_id:       raw.tool_call_id,
            actor:              raw.actor,
            event:              &raw.event,
            properties:         &raw.properties,
        })
    }

    pub fn from_ref(value: &Value) -> serde_json::Result<Self> {
        fn opt_field<T: for<'a> Deserialize<'a>>(
            obj: &Map<String, Value>,
            key: &str,
        ) -> serde_json::Result<Option<T>> {
            match obj.get(key) {
                Some(value) if !value.is_null() => Ok(Some(T::deserialize(value)?)),
                _ => Ok(None),
            }
        }

        let obj = value.as_object().ok_or_else(|| {
            <serde_json::Error as DeError>::custom("run event must be a JSON object")
        })?;
        let opt_str = |key: &str| obj.get(key).and_then(Value::as_str).map(str::to_string);
        let id = obj.get("id").and_then(Value::as_str).ok_or_else(|| {
            <serde_json::Error as DeError>::custom("missing or non-string field: id")
        })?;
        let ts = obj
            .get("ts")
            .ok_or_else(|| <serde_json::Error as DeError>::custom("missing field: ts"))
            .and_then(DateTime::<Utc>::deserialize)?;
        let run_id = obj
            .get("run_id")
            .ok_or_else(|| <serde_json::Error as DeError>::custom("missing field: run_id"))
            .and_then(RunId::deserialize)?;
        let event = obj.get("event").and_then(Value::as_str).ok_or_else(|| {
            <serde_json::Error as DeError>::custom("missing or non-string field: event")
        })?;
        let mut properties = obj
            .get("properties")
            .cloned()
            .unwrap_or_else(default_properties);
        normalize_legacy_event_properties(event, &mut properties);
        Self::from_parts(RunEventParts {
            id: id.to_string(),
            ts,
            run_id,
            node_id: opt_str("node_id"),
            node_label: opt_str("node_label"),
            stage_id: opt_field(obj, "stage_id")?,
            parallel_group_id: opt_field(obj, "parallel_group_id")?,
            parallel_branch_id: opt_field(obj, "parallel_branch_id")?,
            session_id: opt_str("session_id"),
            parent_session_id: opt_str("parent_session_id"),
            tool_call_id: opt_str("tool_call_id"),
            actor: opt_field(obj, "actor")?,
            event,
            properties: &properties,
        })
    }

    fn from_parts(parts: RunEventParts<'_>) -> serde_json::Result<Self> {
        let body: EventBody = if is_coding_event_name(parts.event) {
            EventBody::Agent(serde_json::from_value(parts.properties.clone())?)
        } else {
            let body_payload = json!({
                "event": parts.event,
                "properties": parts.properties,
            });
            match EventBody::sandbox_driver_from_stored(parts.event, parts.properties) {
                Some(body) => body,
                None => match serde_json::from_value(body_payload) {
                    Ok(body) => body,
                    Err(err) if is_known_event_name(parts.event) => return Err(err),
                    Err(_) => EventBody::Unknown {
                        name:       parts.event.to_string(),
                        properties: parts.properties.clone(),
                    },
                },
            }
        };
        Ok(Self {
            id: parts.id,
            ts: parts.ts,
            run_id: parts.run_id,
            node_id: parts.node_id,
            node_label: parts.node_label,
            stage_id: parts.stage_id,
            parallel_group_id: parts.parallel_group_id,
            parallel_branch_id: parts.parallel_branch_id,
            session_id: parts.session_id,
            parent_session_id: parts.parent_session_id,
            tool_call_id: parts.tool_call_id,
            actor: parts.actor,
            body,
        })
    }

    pub fn from_json_str(line: &str) -> serde_json::Result<Self> {
        Self::from_value(serde_json::from_str(line)?)
    }

    pub fn to_value(&self) -> serde_json::Result<Value> {
        fn insert_opt<T: Serialize>(
            map: &mut Map<String, Value>,
            key: &str,
            value: Option<&T>,
        ) -> serde_json::Result<()> {
            if let Some(v) = value {
                map.insert(key.to_string(), serde_json::to_value(v)?);
            }
            Ok(())
        }

        let mut map = Map::new();
        map.insert("id".to_string(), Value::String(self.id.clone()));
        map.insert("ts".to_string(), serde_json::to_value(self.ts)?);
        map.insert("run_id".to_string(), serde_json::to_value(self.run_id)?);
        map.insert(
            "event".to_string(),
            Value::String(self.body.event_name().to_string()),
        );
        insert_opt(&mut map, "session_id", self.session_id.as_ref())?;
        insert_opt(
            &mut map,
            "parent_session_id",
            self.parent_session_id.as_ref(),
        )?;
        insert_opt(&mut map, "node_id", self.node_id.as_ref())?;
        insert_opt(&mut map, "node_label", self.node_label.as_ref())?;
        insert_opt(&mut map, "stage_id", self.stage_id.as_ref())?;
        insert_opt(
            &mut map,
            "parallel_group_id",
            self.parallel_group_id.as_ref(),
        )?;
        insert_opt(
            &mut map,
            "parallel_branch_id",
            self.parallel_branch_id.as_ref(),
        )?;
        insert_opt(&mut map, "tool_call_id", self.tool_call_id.as_ref())?;
        insert_opt(&mut map, "actor", self.actor.as_ref())?;
        map.insert("properties".to_string(), self.body.properties_value()?);
        Ok(Value::Object(map))
    }

    pub fn event_name(&self) -> &str {
        self.body.event_name()
    }

    pub fn properties(&self) -> serde_json::Result<Value> {
        self.body.properties_value()
    }
}

/// Upgrades historical envelope shapes only in the value being decoded.
///
/// Event bodies carry no compatibility rewrites: Fabro is greenfield, so a
/// stored body either matches the current schema or fails to decode.
fn normalize_legacy_event(value: &mut Value) {
    let Some(event) = value
        .get("event")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return;
    };
    let Some(properties) = value.get_mut("properties") else {
        return;
    };
    normalize_legacy_event_properties(&event, properties);
}

fn normalize_legacy_event_properties(event: &str, properties: &mut Value) {
    let Some(object) = properties.as_object_mut() else {
        return;
    };
    match event {
        "run.completed" => normalize_legacy_timing(object, false),
        "run.failed" => {
            normalize_legacy_run_failure(object);
            normalize_legacy_timing(object, false);
        }
        "stage.completed" => normalize_legacy_timing(object, true),
        "sandbox.initialized" => normalize_legacy_sandbox_id(object),
        _ => {}
    }
}

fn normalize_legacy_timing(properties: &mut Map<String, Value>, stage: bool) {
    if properties.contains_key("timing") {
        return;
    }
    let Some(wall_time_ms) = properties.get("duration_ms").and_then(Value::as_u64) else {
        return;
    };
    let timing = json!({
        "wall_time_ms": wall_time_ms,
        "inference_time_ms": 0,
        "tool_time_ms": 0,
        "active_time_ms": 0,
    });
    properties.insert("timing".to_owned(), timing);
    if stage {
        properties.remove("duration_ms");
    }
}

fn normalize_legacy_run_failure(properties: &mut Map<String, Value>) {
    if !properties.contains_key("failure") {
        if let (Some(message), Some(reason)) = (
            properties.get("error").cloned(),
            properties.get("reason").cloned(),
        ) {
            let causes = properties
                .get("causes")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new()));
            properties.insert(
                "failure".to_owned(),
                json!({
                    "reason": reason,
                    "detail": {
                        "message": message,
                        "causes": causes,
                        "category": "deterministic",
                    }
                }),
            );
        }
    }
    if !properties.contains_key("final_git_commit_sha") {
        if let Some(commit) = properties.get("git_commit_sha").cloned() {
            properties.insert("final_git_commit_sha".to_owned(), commit);
        }
    }
}

fn normalize_legacy_sandbox_id(properties: &mut Map<String, Value>) {
    if properties.contains_key("id") {
        return;
    }
    let id = properties
        .get("identifier")
        .and_then(Value::as_str)
        .unwrap_or_default();
    properties.insert("id".to_owned(), Value::String(id.to_owned()));
}

impl Serialize for RunEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_value()
            .map_err(S::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RunEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Self::from_value(value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use pebble_coding_agent::events::{
        CodingAgentEvent, CodingEvent, TodoCreatedProps, TodoListKind, TodoStatus, TokenUsage,
        ToolCategory, ToolSource, ToolSummary,
    };
    use serde_json::json;

    use super::*;
    use crate::{
        AuthMethod, BlobHash, Edge, Graph, IdpIdentity, Node, PendingReason, WorkflowSettings,
        fixtures, test_support,
    };

    fn coding_event(stage: &str, visit: u32, event: CodingEvent) -> AgentEventProps {
        AgentEventProps::new(
            stage,
            visit,
            CodingAgentEvent::new(
                "ses_1",
                event,
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .with_seq(3),
        )
    }

    #[test]
    fn agent_events_store_under_their_derived_name_and_read_back() {
        let body = EventBody::Agent(coding_event("code", 2, CodingEvent::RoundInterrupted {
            generation: 3,
        }));
        let event = RunEvent {
            id: "evt_round_interrupted".to_string(),
            ts: DateTime::parse_from_rfc3339("2026-04-04T12:00:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
            run_id: fixtures::RUN_1,
            node_id: Some("code".to_string()),
            node_label: Some("code".to_string()),
            stage_id: Some(StageId::new("code", 2)),
            parallel_group_id: None,
            parallel_branch_id: None,
            session_id: Some("ses_1".to_string()),
            parent_session_id: None,
            tool_call_id: None,
            actor: None,
            body,
        };

        let value = event.to_value().unwrap();
        assert_eq!(value["event"], "agent.round.interrupted");
        assert_eq!(value["properties"]["stage"], "code");
        assert_eq!(value["properties"]["visit"], 2);
        assert_eq!(value["properties"]["seq"], 3);
        assert_eq!(value["properties"]["session_id"], "ses_1");
        assert_eq!(
            value["properties"]["event"],
            json!({"RoundInterrupted": {"generation": 3}})
        );

        let parsed = RunEvent::from_value(value).unwrap();
        assert_eq!(parsed, event);
        assert_eq!(parsed.event_name(), "agent.round.interrupted");
    }

    #[test]
    fn todo_events_store_under_todo_names() {
        let body = EventBody::Agent(coding_event(
            "code",
            1,
            CodingEvent::TodoCreated(TodoCreatedProps {
                list_id:     "openai_plan:ses_1".to_string(),
                list_kind:   TodoListKind::OpenAiPlan,
                todo_id:     "todo_1".to_string(),
                status:      TodoStatus::Pending,
                order:       0,
                subject:     "do the thing".to_string(),
                description: String::new(),
                active_form: None,
                owner:       None,
                blocks:      Vec::new(),
                blocked_by:  Vec::new(),
                metadata:    std::collections::BTreeMap::new(),
            }),
        ));
        assert_eq!(body.event_name(), "todo.created");
        assert!(is_known_event_name("todo.created"));
        assert!(is_known_event_name("agent.message"));
        assert!(is_known_event_name("agent.compaction.failed"));
    }

    #[test]
    fn bare_agent_events_serialize_as_their_variant_name() {
        let body = EventBody::Agent(coding_event("code", 1, CodingEvent::SessionEnded));
        let value = RunEvent {
            id: "evt_session_ended".to_string(),
            ts: Utc::now(),
            run_id: fixtures::RUN_1,
            node_id: None,
            node_label: None,
            stage_id: None,
            parallel_group_id: None,
            parallel_branch_id: None,
            session_id: Some("ses_1".to_string()),
            parent_session_id: None,
            tool_call_id: None,
            actor: None,
            body,
        }
        .to_value()
        .unwrap();
        assert_eq!(value["event"], "agent.session.ended");
        assert_eq!(value["properties"]["event"], "SessionEnded");
    }

    #[test]
    fn a_malformed_agent_event_is_rejected_not_demoted_to_unknown() {
        let value = json!({
            "id": "evt_bad",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "agent.message",
            "properties": {"stage": "code", "visit": 1}
        });
        assert!(RunEvent::from_value(value).is_err());
    }

    #[test]
    fn agent_message_carries_pebbles_usage_shape() {
        let body = EventBody::Agent(coding_event("code", 1, CodingEvent::AssistantMessage {
            text:            "ok".to_string(),
            model:           "gpt-5.4".to_string(),
            usage:           TokenUsage {
                input: 10,
                output: 5,
                ..TokenUsage::default()
            },
            cost_usd_micros: Some(42),
            cost_source:     None,
            tool_call_count: 0,
            context_window:  None,
            reasoning:       None,
        }));
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(
            value["properties"]["event"]["AssistantMessage"]["usage"],
            json!({"input": 10, "output": 5, "reasoning": 0, "cache_read": 0, "cache_write": 0})
        );
        assert_eq!(
            value["properties"]["event"]["AssistantMessage"]["cost_usd_micros"],
            42
        );
    }

    fn user_principal(login: &str) -> Principal {
        Principal::user(
            IdpIdentity::new("https://github.com", "12345").unwrap(),
            login.to_string(),
            AuthMethod::Github,
        )
    }

    fn stored_event(event: &str, properties: &Value) -> Value {
        json!({
            "id": format!("evt_{event}"),
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": event,
            "properties": properties,
        })
    }

    #[test]
    fn run_event_round_trips_json() {
        let event = RunEvent {
            id:                 "evt_1".to_string(),
            ts:                 DateTime::parse_from_rfc3339("2026-04-04T12:00:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
            run_id:             fixtures::RUN_1,
            node_id:            Some("build".to_string()),
            node_label:         Some("Build".to_string()),
            stage_id:           None,
            parallel_group_id:  None,
            parallel_branch_id: None,
            session_id:         None,
            parent_session_id:  None,
            tool_call_id:       None,
            actor:              None,
            body:               EventBody::StageCompleted(StageCompletedProps {
                index: 1,
                timing: crate::StageTiming::wall_only(1234),
                status: crate::StageOutcome::Succeeded,
                preferred_label: None,
                suggested_next_ids: vec!["next".to_string()],
                billing: None,
                failure: None,
                notes: Some("done".to_string()),
                files_touched: vec!["src/main.rs".to_string()],
                context_updates: None,
                jump_to_node: None,
                context_values: None,
                node_visits: None,
                loop_failure_signatures: None,
                restart_failure_signatures: None,
                response: None,
                attempt: 1,
                max_attempts: 1,
            }),
        };

        let value = event.to_value().unwrap();
        let parsed = RunEvent::from_value(value).unwrap();

        assert_eq!(parsed, event);
    }

    #[test]
    fn run_event_deserializes_adjacent_layout() {
        let settings = WorkflowSettings::default();
        let mut graph = Graph::new("test");
        graph.nodes.insert("start".to_string(), Node::new("start"));
        graph.edges.push(Edge::new("start", "done"));

        let line = json!({
            "id": "evt_2",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.created",
            "properties": {
                "settings": settings,
                "graph": graph,
                "labels": {},
                "source_directory": "/tmp/run",
                "provenance": test_support::test_run_provenance()
            }
        });

        let parsed = RunEvent::from_value(line).unwrap();
        assert!(matches!(parsed.body, EventBody::RunCreated(_)));
    }

    #[test]
    fn historical_failover_event_defaults_new_route_context() {
        let line = json!({
            "id": "evt_failover",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "agent.failover",
            "properties": {
                "from_provider": "anthropic",
                "from_model": "claude-fable-5",
                "to_provider": "openai",
                "to_model": "gpt-5.6-sol",
                "error": "provider unavailable"
            }
        });

        let parsed = RunEvent::from_value(line).unwrap();
        let EventBody::Failover(props) = parsed.body else {
            panic!("expected agent.failover");
        };
        assert_eq!(props.original_provider, None);
        assert_eq!(props.original_model, None);
        assert_eq!(props.attempt, None);
        assert_eq!(props.requested_reasoning_effort, None);
        assert_eq!(props.effective_reasoning_effort, None);
    }

    #[test]
    fn historical_run_created_defaults_new_run_settings() {
        let mut settings = serde_json::to_value(WorkflowSettings::default()).unwrap();
        let run = settings["run"].as_object_mut().unwrap();
        for field in ["clone", "run_branch", "integrations"] {
            run.remove(field);
        }
        let line = stored_event(
            "run.created",
            &json!({
                "settings": settings,
                "graph": Graph::new("test"),
                "labels": {},
                "source_directory": "/tmp/run",
                "provenance": test_support::test_run_provenance()
            }),
        );

        let parsed = RunEvent::from_value(line).unwrap();
        let EventBody::RunCreated(props) = parsed.body else {
            panic!("expected run.created");
        };

        assert_eq!(props.settings.run, WorkflowSettings::default().run);
    }

    #[test]
    fn historical_terminal_and_sandbox_events_are_upgraded() {
        let completed = stored_event(
            "run.completed",
            &json!({
                "duration_ms": 123,
                "artifact_count": 0,
                "status": "succeeded",
                "reason": "completed"
            }),
        );
        let failed = stored_event(
            "run.failed",
            &json!({
                "error": "cancelled by user",
                "causes": ["interrupt requested"],
                "duration_ms": 456,
                "reason": "cancelled",
                "git_commit_sha": "abc123"
            }),
        );
        let sandbox = stored_event(
            "sandbox.initialized",
            &json!({
                "provider": "local",
                "working_directory": "/tmp/run"
            }),
        );

        let completed = RunEvent::from_value(completed).unwrap().to_value().unwrap();
        let failed = RunEvent::from_value(failed).unwrap().to_value().unwrap();
        let sandbox = RunEvent::from_value(sandbox).unwrap().to_value().unwrap();

        assert_eq!(completed["properties"]["timing"]["wall_time_ms"], 123);
        assert_eq!(failed["properties"]["failure"]["reason"], "cancelled");
        assert_eq!(
            failed["properties"]["failure"]["detail"]["message"],
            "cancelled by user"
        );
        assert_eq!(
            failed["properties"]["failure"]["detail"]["causes"],
            json!(["interrupt requested"])
        );
        assert_eq!(failed["properties"]["timing"]["wall_time_ms"], 456);
        assert_eq!(failed["properties"]["final_git_commit_sha"], "abc123");
        assert_eq!(sandbox["properties"]["id"], "");
    }

    #[test]
    fn run_created_round_trip_preserves_manifest_blob() {
        let line = json!({
            "id": "evt_created_blob",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.created",
            "properties": {
                "settings": WorkflowSettings::default(),
                "graph": Graph::new("test"),
                "labels": {},
                "source_directory": "/tmp/run",
                "provenance": test_support::test_run_provenance(),
                "manifest_blob": BlobHash::new(br#"{"version":1}"#).to_string()
            }
        });

        let parsed = RunEvent::from_value(line.clone()).unwrap();
        let serialized = parsed.to_value().unwrap();

        assert_eq!(
            serialized["properties"]["manifest_blob"],
            line["properties"]["manifest_blob"]
        );
    }

    #[test]
    fn interview_interrupted_kind_matches_event_name() {
        let body = EventBody::InterviewInterrupted(InterviewInterruptedProps {
            question_id: "q-1".to_string(),
            question:    "approve?".to_string(),
            stage:       "gate".to_string(),
            reason:      "interrupted".to_string(),
            duration_ms: 12,
        });

        assert_eq!(body.event_name(), "interview.interrupted");
    }

    #[test]
    fn run_interrupt_round_trips_with_empty_properties_and_actor() {
        let line = json!({
            "id": "evt_interrupt",
            "ts": "2026-04-04T12:00:00Z",
            "run_id": fixtures::RUN_1,
            "event": "run.interrupt",
            "actor": { "kind": "system", "system_kind": "engine" },
            "properties": {}
        });

        let parsed = RunEvent::from_value(line.clone()).unwrap();
        assert!(matches!(parsed.body, EventBody::RunInterrupt(_)));
        assert_eq!(parsed.to_value().unwrap(), line);
    }

    #[test]
    fn run_steer_round_trips_with_text_and_actor() {
        let line = json!({
            "id": "evt_steer",
            "ts": "2026-04-04T12:00:00Z",
            "run_id": fixtures::RUN_1,
            "event": "run.steer",
            "actor": { "kind": "system", "system_kind": "engine" },
            "properties": { "text": "try another approach" }
        });

        let parsed = RunEvent::from_value(line.clone()).unwrap();
        assert!(matches!(
            &parsed.body,
            EventBody::RunSteer(props) if props.text == "try another approach"
        ));
        assert_eq!(parsed.to_value().unwrap(), line);
    }

    #[test]
    fn pre_execution_lifecycle_events_round_trip() {
        let cases = [
            (
                EventBody::RunStartRequested(RunStartRequestedProps { resume: false }),
                json!("run.start_requested"),
                json!({ "resume": false }),
            ),
            (
                EventBody::RunPending(RunPendingProps {
                    reason: PendingReason::ApprovalRequired,
                }),
                json!("run.pending"),
                json!({ "reason": "approval_required" }),
            ),
            (
                EventBody::RunApproved(RunApprovedProps::default()),
                json!("run.approved"),
                json!({}),
            ),
            (
                EventBody::RunDenied(RunDeniedProps {
                    reason: Some("Not approved for execution".to_string()),
                }),
                json!("run.denied"),
                json!({ "reason": "Not approved for execution" }),
            ),
            (
                EventBody::RunRunnable(RunRunnableProps {
                    source: RunRunnableSource::Approved,
                }),
                json!("run.runnable"),
                json!({ "source": "approved" }),
            ),
        ];

        for (body, event_name, properties) in cases {
            let event = RunEvent {
                id: format!("evt_{}", event_name.as_str().unwrap()),
                ts: DateTime::parse_from_rfc3339("2026-05-23T12:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                run_id: fixtures::RUN_1,
                node_id: None,
                node_label: None,
                stage_id: None,
                parallel_group_id: None,
                parallel_branch_id: None,
                session_id: None,
                parent_session_id: None,
                tool_call_id: None,
                actor: Some(Principal::System {
                    system_kind: crate::SystemActorKind::Engine,
                }),
                body,
            };
            let value = event.to_value().unwrap();
            assert_eq!(value["event"], event_name);
            assert_eq!(value["properties"], properties);
            assert_eq!(RunEvent::from_value(value).unwrap(), event);
        }
    }

    #[test]
    fn agent_interrupt_injected_round_trips_with_stage_session_and_actor() {
        let line = json!({
            "id": "evt_interrupt_injected",
            "ts": "2026-04-04T12:00:00Z",
            "run_id": fixtures::RUN_1,
            "event": "agent.interrupt.injected",
            "node_id": "code",
            "node_label": "code",
            "stage_id": "code@2",
            "session_id": "ses_1",
            "actor": { "kind": "system", "system_kind": "engine" },
            "properties": { "visit": 2 }
        });

        let parsed = RunEvent::from_value(line.clone()).unwrap();
        assert!(matches!(
            &parsed.body,
            EventBody::AgentInterruptInjected(props) if props.visit == 2
        ));
        assert_eq!(parsed.to_value().unwrap(), line);
    }

    #[test]
    fn run_interrupt_then_steer_is_not_a_known_persisted_event() {
        let line = json!({
            "id": "evt_combined",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.interrupt_then_steer",
            "properties": { "text": "try another approach" }
        });

        let parsed = RunEvent::from_value(line).unwrap();
        assert!(matches!(
            parsed.body,
            EventBody::Unknown { ref name, .. } if name == "run.interrupt_then_steer"
        ));
    }

    #[test]
    fn patch_bearing_events_round_trip_diff_summary() {
        for (event_name, properties) in [
            (
                "checkpoint.completed",
                json!({
                    "status": "running",
                    "current_node": "build",
                    "completed_nodes": ["build"],
                    "diff_summary": {
                        "files_changed": 2,
                        "additions": 10,
                        "deletions": 3
                    }
                }),
            ),
            (
                "run.completed",
                json!({
                    "timing": {
                        "wall_time_ms": 42,
                        "inference_time_ms": 0,
                        "tool_time_ms": 0,
                        "active_time_ms": 0
                    },
                    "artifact_count": 0,
                    "status": "succeeded",
                    "reason": "completed",
                    "diff_summary": {
                        "files_changed": 2,
                        "additions": 10,
                        "deletions": 3
                    }
                }),
            ),
            (
                "run.failed",
                json!({
                    "failure": {
                        "reason": "workflow_error",
                        "detail": {
                            "message": "boom",
                            "category": "deterministic"
                        }
                    },
                    "timing": {
                        "wall_time_ms": 42,
                        "inference_time_ms": 0,
                        "tool_time_ms": 0,
                        "active_time_ms": 0
                    },
                    "diff_summary": {
                        "files_changed": 2,
                        "additions": 10,
                        "deletions": 3
                    }
                }),
            ),
        ] {
            let line = json!({
                "id": format!("evt_{event_name}"),
                "ts": "2026-04-04T12:00:00Z",
                "run_id": fixtures::RUN_1,
                "event": event_name,
                "node_id": "build",
                "properties": properties
            });

            let parsed = RunEvent::from_value(line).unwrap();
            let serialized = parsed.to_value().unwrap();

            assert_eq!(
                serialized["properties"]["diff_summary"],
                json!({
                    "files_changed": 2,
                    "additions": 10,
                    "deletions": 3
                }),
                "{event_name} should preserve diff_summary"
            );
        }
    }

    #[test]
    fn run_submitted_round_trip_preserves_definition_blob() {
        let line = json!({
            "id": "evt_submitted_blob",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.submitted",
            "properties": {
                "definition_blob": BlobHash::new(br#"{"workflow_path":"workflow.fabro"}"#).to_string()
            }
        });

        let parsed = RunEvent::from_value(line.clone()).unwrap();
        let serialized = parsed.to_value().unwrap();

        assert_eq!(
            serialized["properties"]["definition_blob"],
            line["properties"]["definition_blob"]
        );
    }

    #[test]
    fn event_body_event_name_matches_wire_name() {
        let body = EventBody::StageCompleted(StageCompletedProps {
            index: 1,
            timing: crate::StageTiming::wall_only(1234),
            status: crate::StageOutcome::Succeeded,
            preferred_label: None,
            suggested_next_ids: vec!["next".to_string()],
            billing: None,
            failure: None,
            notes: Some("done".to_string()),
            files_touched: vec!["src/main.rs".to_string()],
            context_updates: None,
            jump_to_node: None,
            context_values: None,
            node_visits: None,
            loop_failure_signatures: None,
            restart_failure_signatures: None,
            response: None,
            attempt: 1,
            max_attempts: 1,
        });

        assert_eq!(body.event_name(), "stage.completed");
    }

    #[test]
    fn run_event_preserves_unknown_event_name_and_properties() {
        let value = json!({
            "id": "evt_unknown",
            "ts": "2026-04-04T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "vendor.custom.event",
            "properties": {
                "answer": 42,
                "nested": { "ok": true }
            }
        });

        let parsed = RunEvent::from_value(value.clone()).unwrap();
        let serialized = parsed.to_value().unwrap();

        assert_eq!(parsed.event_name(), "vendor.custom.event");
        assert_eq!(parsed.properties().unwrap(), value["properties"]);
        assert_eq!(serialized["event"], value["event"]);
        assert_eq!(serialized["properties"], value["properties"]);
    }

    #[test]
    fn run_event_round_trips_new_envelope_fields() {
        let value = json!({
            "id": "evt_envelope",
            "ts": "2026-04-08T16:21:11.106Z",
            "run_id": fixtures::RUN_1,
            "event": "agent.tool.completed",
            "stage_id": "code@1",
            "node_id": "code",
            "node_label": "Code",
            "parallel_group_id": "code@1",
            "parallel_branch_id": "code@1:0",
            "session_id": "ses_child",
            "parent_session_id": "ses_parent",
            "tool_call_id": "call_1",
            "actor": {
                "kind": "agent",
                "session_id": "ses_child",
                "parent_session_id": "ses_parent",
                "model": "claude-sonnet"
            },
            "properties": {
                "stage": "code",
                "visit": 1,
                "seq": 9,
                "stream_id": "ses_parent",
                "session_id": "ses_child",
                "parent_session_id": "ses_parent",
                "tool_call_id": "call_1",
                "timestamp": "2026-04-08T16:21:11.106Z",
                "event": {
                    "ToolCallCompleted": {
                        "tool_name": "read_file",
                        "tool_call_id": "call_1",
                        "output": {"summary": "read"},
                        "is_error": false
                    }
                }
            }
        });

        let parsed = RunEvent::from_value(value.clone()).unwrap();
        assert_eq!(parsed.stage_id, Some(StageId::new("code", 1)));
        assert_eq!(parsed.parallel_group_id, Some(StageId::new("code", 1)));
        assert_eq!(
            parsed.parallel_branch_id,
            Some(ParallelBranchId::new(StageId::new("code", 1), 0))
        );
        assert_eq!(parsed.tool_call_id.as_deref(), Some("call_1"));
        let actor = parsed.actor.as_ref().expect("actor present");
        assert_eq!(actor, &Principal::Agent {
            session_id:        Some("ses_child".to_string()),
            parent_session_id: Some("ses_parent".to_string()),
            model:             Some("claude-sonnet".to_string()),
        });

        let serialized = parsed.to_value().unwrap();
        assert_eq!(serialized["stage_id"], value["stage_id"]);
        assert_eq!(serialized["parallel_group_id"], value["parallel_group_id"]);
        assert_eq!(
            serialized["parallel_branch_id"],
            value["parallel_branch_id"]
        );
        assert_eq!(serialized["tool_call_id"], value["tool_call_id"]);
        assert_eq!(serialized["actor"], value["actor"]);
    }

    #[test]
    fn run_event_omits_absent_envelope_fields() {
        let event = RunEvent {
            id:                 "evt_bare".to_string(),
            ts:                 DateTime::parse_from_rfc3339("2026-04-04T12:00:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
            run_id:             fixtures::RUN_1,
            node_id:            None,
            node_label:         None,
            stage_id:           None,
            parallel_group_id:  None,
            parallel_branch_id: None,
            session_id:         None,
            parent_session_id:  None,
            tool_call_id:       None,
            actor:              None,
            body:               EventBody::RunStarted(RunStartedProps {
                name:         "demo".to_string(),
                base_branch:  None,
                base_sha:     None,
                run_branch:   None,
                worktree_dir: None,
                goal:         None,
            }),
        };

        let serialized = event.to_value().unwrap();
        let obj = serialized.as_object().unwrap();
        assert!(!obj.contains_key("stage_id"));
        assert!(!obj.contains_key("parallel_group_id"));
        assert!(!obj.contains_key("parallel_branch_id"));
        assert!(!obj.contains_key("tool_call_id"));
        assert!(!obj.contains_key("actor"));
    }

    #[test]
    fn canonical_run_lifecycle_events_are_known() {
        for event in [
            "run.start_requested",
            "run.pending",
            "run.approved",
            "run.denied",
            "run.runnable",
            "run.blocked",
            "run.unblocked",
        ] {
            assert!(
                is_known_event_name(event),
                "{event} should be a known event"
            );
        }
    }

    #[test]
    fn run_blocked_round_trips_as_typed_event() {
        let value = json!({
            "id": "evt_run_blocked",
            "ts": "2026-04-19T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.blocked",
            "properties": {
                "blocked_reason": "human_input_required"
            }
        });

        let parsed = RunEvent::from_value(value.clone()).unwrap();
        assert!(
            !matches!(parsed.body, EventBody::Unknown { .. }),
            "run.blocked should deserialize into a typed event body"
        );

        let serialized = parsed.to_value().unwrap();
        assert_eq!(serialized["event"], "run.blocked");
        assert_eq!(
            serialized["properties"]["blocked_reason"],
            value["properties"]["blocked_reason"]
        );
    }

    #[test]
    fn run_archived_serializes_with_dotted_event_name_without_actor_property() {
        let body = EventBody::RunArchived(RunArchivedProps::default());
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["event"], "run.archived");
        assert_eq!(value["properties"], json!({}));
    }

    #[test]
    fn run_unarchived_serializes_without_actor_property() {
        let body = EventBody::RunUnarchived(RunUnarchivedProps::default());
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["event"], "run.unarchived");
        assert_eq!(value["properties"], json!({}));
    }

    #[test]
    fn run_archived_round_trips_through_from_value() {
        let value = json!({
            "id": "evt_archived",
            "ts": "2026-04-19T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.archived",
            "actor": {
                "kind": "user",
                "identity": {
                    "issuer": "https://github.com",
                    "subject": "12345"
                },
                "login": "alice",
                "auth_method": "github"
            },
            "properties": {}
        });

        let parsed = RunEvent::from_value(value.clone()).unwrap();
        assert!(matches!(parsed.body, EventBody::RunArchived(_)));
        assert_eq!(parsed.actor, Some(user_principal("alice")));
        let serialized = parsed.to_value().unwrap();
        assert_eq!(serialized["event"], "run.archived");
        assert_eq!(serialized["actor"], value["actor"]);
        assert_eq!(serialized["properties"], json!({}));
    }

    #[test]
    fn run_unarchived_round_trips_through_from_value() {
        let value = json!({
            "id": "evt_unarchived",
            "ts": "2026-04-19T12:00:00.000Z",
            "run_id": fixtures::RUN_1,
            "event": "run.unarchived",
            "properties": {}
        });

        let parsed = RunEvent::from_value(value.clone()).unwrap();
        match &parsed.body {
            EventBody::RunUnarchived(_) => {}
            other => panic!("expected RunUnarchived body, got {other:?}"),
        }
    }

    #[test]
    fn run_runnable_and_unblocked_round_trip_as_typed_events() {
        for value in [
            json!({
                "id": "evt_run_runnable",
                "ts": "2026-04-19T12:00:00.000Z",
                "run_id": fixtures::RUN_1,
                "event": "run.runnable",
                "properties": { "source": "start_requested" }
            }),
            json!({
                "id": "evt_run_unblocked",
                "ts": "2026-04-19T12:00:00.000Z",
                "run_id": fixtures::RUN_1,
                "event": "run.unblocked",
                "properties": {}
            }),
        ] {
            let parsed = RunEvent::from_value(value.clone()).unwrap();
            assert!(
                !matches!(parsed.body, EventBody::Unknown { .. }),
                "{} should deserialize into a typed event body",
                value["event"].as_str().unwrap()
            );
            assert_eq!(parsed.to_value().unwrap()["event"], value["event"]);
        }
    }

    #[test]
    fn metadata_snapshot_events_are_known_and_round_trip_json() {
        let completed = RunEvent {
            id:                 "evt_metadata_completed".to_string(),
            ts:                 DateTime::parse_from_rfc3339("2026-04-29T12:00:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
            run_id:             fixtures::RUN_1,
            node_id:            None,
            node_label:         None,
            stage_id:           None,
            parallel_group_id:  None,
            parallel_branch_id: None,
            session_id:         None,
            parent_session_id:  None,
            tool_call_id:       None,
            actor:              None,
            body:               EventBody::MetadataSnapshotCompleted(
                MetadataSnapshotCompletedProps {
                    phase:       MetadataSnapshotPhase::Checkpoint,
                    branch:      "fabro/metadata/run".to_string(),
                    duration_ms: 2800,
                    entry_count: 3,
                    bytes:       42,
                    commit_sha:  "abc123".to_string(),
                },
            ),
        };

        let serialized = completed.to_value().unwrap();
        assert_eq!(serialized["event"], "metadata.snapshot.completed");
        assert_eq!(serialized["properties"]["phase"], "checkpoint");
        assert_eq!(serialized["properties"]["branch"], "fabro/metadata/run");
        assert_eq!(serialized["properties"]["duration_ms"], 2800);
        assert_eq!(serialized["properties"]["entry_count"], 3);
        assert_eq!(serialized["properties"]["bytes"], 42);
        assert_eq!(serialized["properties"]["commit_sha"], "abc123");

        let parsed = RunEvent::from_value(serialized).unwrap();
        assert_eq!(parsed.event_name(), "metadata.snapshot.completed");
        assert!(matches!(
            parsed.body,
            EventBody::MetadataSnapshotCompleted(MetadataSnapshotCompletedProps {
                phase: MetadataSnapshotPhase::Checkpoint,
                ..
            })
        ));
    }

    #[test]
    fn pull_request_linked_round_trips_json() {
        let event = RunEvent {
            id:                 "evt_pr_linked".to_string(),
            ts:                 DateTime::parse_from_rfc3339("2026-05-15T12:00:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
            run_id:             fixtures::RUN_1,
            node_id:            None,
            node_label:         None,
            stage_id:           None,
            parallel_group_id:  None,
            parallel_branch_id: None,
            session_id:         None,
            parent_session_id:  None,
            tool_call_id:       None,
            actor:              None,
            body:               EventBody::PullRequestLinked(PullRequestLinkedProps {
                pull_request: crate::PullRequestLink {
                    owner:  "acme".to_string(),
                    repo:   "widgets".to_string(),
                    number: 42,
                },
            }),
        };

        let value = event.to_value().unwrap();
        assert_eq!(value["event"], "pull_request.linked");
        assert_eq!(
            value["properties"]["pull_request"]["html_url"],
            "https://github.com/acme/widgets/pull/42"
        );

        let parsed = RunEvent::from_value(value).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn pull_request_unlinked_round_trips_json() {
        let event = RunEvent {
            id:                 "evt_pr_unlinked".to_string(),
            ts:                 DateTime::parse_from_rfc3339("2026-05-15T12:05:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
            run_id:             fixtures::RUN_1,
            node_id:            None,
            node_label:         None,
            stage_id:           None,
            parallel_group_id:  None,
            parallel_branch_id: None,
            session_id:         None,
            parent_session_id:  None,
            tool_call_id:       None,
            actor:              None,
            body:               EventBody::PullRequestUnlinked(PullRequestUnlinkedProps {
                pull_request: crate::PullRequestLink {
                    owner:  "acme".to_string(),
                    repo:   "widgets".to_string(),
                    number: 42,
                },
            }),
        };

        let value = event.to_value().unwrap();
        assert_eq!(value["event"], "pull_request.unlinked");
        assert_eq!(
            value["properties"]["pull_request"]["number"],
            serde_json::json!(42)
        );

        let parsed = RunEvent::from_value(value).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn retired_sandbox_snapshot_events_deserialize_as_unknown() {
        for (event_name, expected_properties) in [
            (
                "sandbox.snapshot.pulled",
                json!({"name": "buildpack-deps:noble", "duration_ms": 5000}),
            ),
            ("sandbox.snapshot.ensuring", json!({"name": "fabro-v8"})),
        ] {
            let value = json!({
                "id": "evt_retired_snapshot",
                "ts": "2026-04-29T12:00:00.000Z",
                "run_id": fixtures::RUN_1,
                "event": event_name,
                "properties": expected_properties
            });

            let parsed = RunEvent::from_value(value).unwrap();
            match parsed.body {
                EventBody::Unknown { name, properties } => {
                    assert_eq!(name, event_name);
                    assert_eq!(properties, expected_properties);
                }
                other => panic!("expected Unknown body, got {other:?}"),
            }
        }
    }

    #[test]
    fn retired_retro_events_deserialize_as_unknown() {
        for (event_name, expected_properties) in [
            (
                "retro.started",
                json!({"prompt": "Analyze the run", "provider": "openai", "model": "gpt-5"}),
            ),
            (
                "retro.completed",
                json!({"duration_ms": 1200, "response": "done", "retro": {"smoothness": "smooth"}}),
            ),
            (
                "retro.failed",
                json!({"duration_ms": 1200, "error": "state unavailable"}),
            ),
        ] {
            let value = json!({
                "id": "evt_retired_retro",
                "ts": "2026-05-08T12:00:00.000Z",
                "run_id": fixtures::RUN_1,
                "event": event_name,
                "properties": expected_properties
            });

            let parsed = RunEvent::from_value(value).unwrap();
            match parsed.body {
                EventBody::Unknown { name, properties } => {
                    assert_eq!(name, event_name);
                    assert_eq!(properties, expected_properties);
                }
                other => panic!("expected Unknown body, got {other:?}"),
            }
        }
    }

    #[test]
    fn metadata_snapshot_failed_omits_empty_optional_fields() {
        let body = EventBody::MetadataSnapshotFailed(MetadataSnapshotFailedProps {
            phase:            MetadataSnapshotPhase::Init,
            branch:           "fabro/metadata/run".to_string(),
            duration_ms:      15,
            failure_kind:     MetadataSnapshotFailureKind::LoadState,
            error:            "state unavailable".to_string(),
            causes:           Vec::new(),
            commit_sha:       None,
            entry_count:      None,
            bytes:            None,
            exec_output_tail: None,
        });

        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["event"], "metadata.snapshot.failed");
        assert_eq!(
            value["properties"],
            json!({
                "phase": "init",
                "branch": "fabro/metadata/run",
                "duration_ms": 15,
                "failure_kind": "load_state",
                "error": "state unavailable"
            })
        );
    }

    #[test]
    fn metadata_snapshot_failed_serializes_exec_output_tail_additively() {
        let body = EventBody::MetadataSnapshotFailed(MetadataSnapshotFailedProps {
            phase:            MetadataSnapshotPhase::Checkpoint,
            branch:           "fabro/metadata/run".to_string(),
            duration_ms:      20,
            failure_kind:     MetadataSnapshotFailureKind::Push,
            error:            "push failed".to_string(),
            causes:           Vec::new(),
            commit_sha:       None,
            entry_count:      None,
            bytes:            None,
            exec_output_tail: Some(ExecOutputTail {
                stdout:           Some("last stdout line".to_string()),
                stderr:           Some("last stderr line".to_string()),
                stdout_truncated: false,
                stderr_truncated: true,
            }),
        });

        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(
            value["properties"]["exec_output_tail"]["stdout"],
            "last stdout line"
        );
        assert_eq!(
            value["properties"]["exec_output_tail"]["stderr"],
            "last stderr line"
        );
        assert_eq!(
            value["properties"]["exec_output_tail"]["stderr_truncated"],
            true
        );
        assert!(
            value["properties"]["exec_output_tail"]
                .as_object()
                .expect("exec output tail object")
                .get("stdout_truncated")
                .is_none()
        );

        let body_without_tail = EventBody::MetadataSnapshotFailed(MetadataSnapshotFailedProps {
            phase:            MetadataSnapshotPhase::Checkpoint,
            branch:           "fabro/metadata/run".to_string(),
            duration_ms:      20,
            failure_kind:     MetadataSnapshotFailureKind::Push,
            error:            "push failed".to_string(),
            causes:           Vec::new(),
            commit_sha:       None,
            entry_count:      None,
            bytes:            None,
            exec_output_tail: None,
        });
        let value_without_tail = serde_json::to_value(&body_without_tail).unwrap();
        assert!(
            value_without_tail["properties"]
                .as_object()
                .expect("properties object")
                .get("exec_output_tail")
                .is_none()
        );
    }

    #[test]
    fn exec_output_tail_fields_are_additive_on_failure_props() {
        let tail = ExecOutputTail {
            stdout:           Some("last stdout line".to_string()),
            stderr:           Some("last stderr line".to_string()),
            stdout_truncated: false,
            stderr_truncated: true,
        };

        for body in [
            EventBody::RunNotice(RunNoticeProps {
                level:            RunNoticeLevel::Warn,
                code:             RunNoticeCode::GitDiffFailed.to_string(),
                message:          "git diff failed".to_string(),
                exec_output_tail: Some(tail.clone()),
            }),
            EventBody::CheckpointFailed(CheckpointFailedProps {
                error:            "git commit failed".to_string(),
                exec_output_tail: Some(tail.clone()),
            }),
            EventBody::GitPush(GitPushProps {
                branch:           "refs/heads/run:refs/heads/run".to_string(),
                success:          false,
                exec_output_tail: Some(tail.clone()),
                attempts:         Vec::new(),
            }),
        ] {
            let value = serde_json::to_value(&body).unwrap();
            assert_eq!(
                value["properties"]["exec_output_tail"]["stderr"],
                "last stderr line"
            );
            assert_eq!(
                value["properties"]["exec_output_tail"]["stderr_truncated"],
                true
            );
        }
    }

    #[test]
    fn absent_exec_output_tail_is_omitted_from_new_failure_props() {
        for body in [
            EventBody::RunNotice(RunNoticeProps {
                level:            RunNoticeLevel::Warn,
                code:             RunNoticeCode::GitDiffFailed.to_string(),
                message:          "git diff failed".to_string(),
                exec_output_tail: None,
            }),
            EventBody::CheckpointFailed(CheckpointFailedProps {
                error:            "git commit failed".to_string(),
                exec_output_tail: None,
            }),
            EventBody::GitPush(GitPushProps {
                branch:           "refs/heads/run:refs/heads/run".to_string(),
                success:          false,
                exec_output_tail: None,
                attempts:         Vec::new(),
            }),
        ] {
            let value = serde_json::to_value(&body).unwrap();
            assert!(
                value["properties"]
                    .as_object()
                    .expect("properties object")
                    .get("exec_output_tail")
                    .is_none()
            );
        }
    }

    #[test]
    fn agent_mcp_ready_serializes_with_tool_summaries() {
        let body = EventBody::AgentMcpReady(AgentMcpReadyProps {
            server_name: "github".to_string(),
            tool_count:  1,
            tools:       vec![AgentMcpToolSummary {
                name:          "mcp__github__create_issue".to_string(),
                original_name: "create_issue".to_string(),
            }],
            startup_ms:  0,
            visit:       1,
        });
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["event"], "agent.mcp.ready");
        assert_eq!(
            value["properties"]["tools"][0]["name"],
            "mcp__github__create_issue"
        );
        assert_eq!(
            value["properties"]["tools"][0]["original_name"],
            "create_issue"
        );
    }

    #[test]
    fn agent_mcp_ready_and_failed_carry_startup_ms_and_default_it_when_absent() {
        let ready = EventBody::AgentMcpReady(AgentMcpReadyProps {
            server_name: "github".to_string(),
            tool_count:  0,
            tools:       Vec::new(),
            startup_ms:  842,
            visit:       1,
        });
        let value = serde_json::to_value(&ready).unwrap();
        assert_eq!(value["properties"]["startup_ms"], 842);

        let failed = EventBody::AgentMcpFailed(AgentMcpFailedProps {
            server_name: "filesystem".to_string(),
            error:       "could not launch `npx`".to_string(),
            startup_ms:  4,
            visit:       1,
        });
        let value = serde_json::to_value(&failed).unwrap();
        assert_eq!(value["properties"]["startup_ms"], 4);

        // Events written before pebble reported startup time.
        let legacy: EventBody = serde_json::from_value(json!({
            "event": "agent.mcp.failed",
            "properties": {
                "server_name": "filesystem",
                "error": "Connection refused",
                "visit": 1
            }
        }))
        .unwrap();
        match legacy {
            EventBody::AgentMcpFailed(props) => assert_eq!(props.startup_ms, 0),
            other => panic!("unexpected body: {other:?}"),
        }
    }

    #[test]
    fn agent_mcp_disconnected_round_trips() {
        let body = EventBody::AgentMcpDisconnected(AgentMcpDisconnectedProps {
            server_name: "github".to_string(),
            error:       "transport closed".to_string(),
            visit:       1,
        });
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["event"], "agent.mcp.disconnected");
        assert_eq!(
            value["properties"],
            json!({
                "server_name": "github",
                "error": "transport closed",
                "visit": 1
            })
        );
        let parsed: EventBody = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, body);
    }

    #[test]
    fn agent_mcp_ready_omits_tools_when_empty() {
        let body = EventBody::AgentMcpReady(AgentMcpReadyProps {
            server_name: "github".to_string(),
            tool_count:  0,
            tools:       Vec::new(),
            startup_ms:  0,
            visit:       1,
        });
        let value = serde_json::to_value(&body).unwrap();
        assert!(
            value["properties"]
                .as_object()
                .unwrap()
                .get("tools")
                .is_none(),
            "empty tools should be omitted for legacy parity"
        );
    }

    #[test]
    fn agent_tools_available_round_trips_without_parameter_schemas() {
        let body = EventBody::AgentToolsAvailable(AgentToolsAvailableProps {
            tools: vec![
                ToolSummary {
                    name:        "apply_patch".to_string(),
                    description: "Apply a unified diff patch".to_string(),
                    source:      ToolSource::Native,
                    category:    ToolCategory::Write,
                    invoked:     false,
                },
                ToolSummary {
                    name:        "mcp__filesystem__read_file".to_string(),
                    description: "Read a file through the filesystem MCP server".to_string(),
                    source:      ToolSource::Mcp {
                        server_name:   "filesystem".to_string(),
                        original_name: "read_file".to_string(),
                    },
                    category:    ToolCategory::Other,
                    invoked:     false,
                },
            ],
            visit: 1,
        });

        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value["event"], "agent.tools.available");
        assert_eq!(value["properties"]["visit"], 1);
        assert_eq!(value["properties"]["tools"][0]["name"], "apply_patch");
        assert_eq!(value["properties"]["tools"][0]["source"]["kind"], "native");
        assert_eq!(value["properties"]["tools"][0]["category"], "write");
        assert!(
            value["properties"]["tools"][0]
                .as_object()
                .unwrap()
                .get("parameters")
                .is_none(),
            "StageProjection tool summaries must not expose full parameter schemas"
        );

        let parsed: EventBody = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, body);
    }
}
