use std::borrow::Cow;

use super::{Event, SandboxLifecycle};

#[must_use]
pub fn event_name(event: &Event) -> Cow<'static, str> {
    let name: &'static str = match event {
        Event::SandboxDriver { event } => {
            return Cow::Owned(fabro_types::sandbox_driver_event_name(event));
        }
        Event::RunCreated { .. } => "run.created",
        Event::WorkflowRunStarted { .. } => "run.started",
        Event::RunSubmitted { .. } => "run.submitted",
        Event::RunStartRequested { .. } => "run.start_requested",
        Event::RunPending { .. } => "run.pending",
        Event::RunApproved { .. } => "run.approved",
        Event::RunDenied { .. } => "run.denied",
        Event::RunRunnable { .. } => "run.runnable",
        Event::RunStarting => "run.starting",
        Event::RunRunning => "run.running",
        Event::RunInterrupt { .. } => "run.interrupt",
        Event::RunSteer { .. } => "run.steer",
        Event::RunPairStarted { .. } => "run.pair.started",
        Event::RunPairEnded { .. } => "run.pair.ended",
        Event::RunPairFailed { .. } => "run.pair.failed",
        Event::RunBlocked { .. } => "run.blocked",
        Event::RunUnblocked => "run.unblocked",
        Event::RunRemoving => "run.removing",
        Event::RunCancelRequested { .. } => "run.cancel.requested",
        Event::RunPauseRequested { .. } => "run.pause.requested",
        Event::RunUnpauseRequested { .. } => "run.unpause.requested",
        Event::RunPaused => "run.paused",
        Event::RunUnpaused => "run.unpaused",
        Event::RunSupersededBy { .. } => "run.superseded_by",
        Event::RunArchived { .. } => "run.archived",
        Event::RunUnarchived { .. } => "run.unarchived",
        Event::RunTitleUpdated { .. } => "run.title.updated",
        Event::RunParentLinked { .. } => "run.parent.linked",
        Event::RunParentUnlinked { .. } => "run.parent.unlinked",
        Event::WorkflowRunCompleted { .. } => "run.completed",
        Event::WorkflowRunFailed { .. } => "run.failed",
        Event::RunNotice { .. } => "run.notice",
        Event::MetadataSnapshotStarted { .. } => "metadata.snapshot.started",
        Event::MetadataSnapshotCompleted { .. } => "metadata.snapshot.completed",
        Event::MetadataSnapshotFailed { .. } => "metadata.snapshot.failed",
        Event::StageStarted { .. } => "stage.started",
        Event::StageCompleted { .. } => "stage.completed",
        Event::StageFailed { .. } => "stage.failed",
        Event::StageRetrying { .. } => "stage.retrying",
        Event::ParallelStarted { .. } => "parallel.started",
        Event::ParallelBranchStarted { .. } => "parallel.branch.started",
        Event::ParallelBranchCompleted { .. } => "parallel.branch.completed",
        Event::ParallelCompleted { .. } => "parallel.completed",
        Event::InterviewStarted { .. } => "interview.started",
        Event::InterviewCompleted { .. } => "interview.completed",
        Event::InterviewTimeout { .. } => "interview.timeout",
        Event::InterviewInterrupted { .. } => "interview.interrupted",
        Event::CheckpointCompleted { .. } => "checkpoint.completed",
        Event::CheckpointFailed { .. } => "checkpoint.failed",
        Event::GitCommit { .. } => "git.commit",
        Event::GitPush { .. } => "git.push",
        Event::GitFetch { .. } => "git.fetch",
        Event::GitReset { .. } => "git.reset",
        Event::EdgeSelected { .. } => "edge.selected",
        Event::LoopRestart { .. } => "loop.restart",
        Event::Prompt { .. } => "stage.prompt",
        Event::PromptCompleted { .. } => "prompt.completed",
        Event::Agent { event, .. } => fabro_types::coding_event_name(&event.event),
        Event::SubgraphStarted { .. } => "subgraph.started",
        Event::SubgraphCompleted { .. } => "subgraph.completed",
        Event::Sandbox { event } => match event {
            SandboxLifecycle::Initializing { .. } => "sandbox.initializing",
            SandboxLifecycle::Ready { .. } => "sandbox.ready",
            SandboxLifecycle::InitializeFailed { .. } => "sandbox.failed",
        },
        Event::SandboxInitialized { .. } => "sandbox.initialized",
        Event::SetupStarted { .. } => "setup.started",
        Event::SetupCommandStarted { .. } => "setup.command.started",
        Event::SetupCommandCompleted { .. } => "setup.command.completed",
        Event::SetupCompleted { .. } => "setup.completed",
        Event::GitIdentityResolved { .. } => "git.identity.resolved",
        Event::SetupFailed { .. } => "setup.failed",
        Event::StallWatchdogTimeout { .. } => "watchdog.timeout",
        Event::ArtifactCaptured { .. } => "artifact.captured",
        Event::SshAccessReady { .. } => "ssh.ready",
        Event::Failover { .. } => "agent.failover",
        Event::CommandStarted { .. } => "command.started",
        Event::CommandCompleted { .. } => "command.completed",
        Event::AgentSessionActivated { .. } => "agent.session.activated",
        Event::AgentToolsAvailable { .. } => "agent.tools.available",
        Event::AgentSessionDeactivated { .. } => "agent.session.deactivated",
        Event::AgentMcpReady { .. } => "agent.mcp.ready",
        Event::AgentMcpFailed { .. } => "agent.mcp.failed",
        Event::AgentMcpDisconnected { .. } => "agent.mcp.disconnected",
        Event::AgentInterruptInjected { .. } => "agent.interrupt.injected",
        Event::AgentPairUserMessage { .. } => "agent.pair.user_message",
        Event::AgentPairSystemMessage { .. } => "agent.pair.system_message",
        Event::AgentSteerBuffered { .. } => "agent.steer.buffered",
        Event::AgentSteerDropped { .. } => "agent.steer.dropped",
        Event::AgentAcpStarted { .. } => "agent.acp.started",
        Event::AgentAcpCompleted { .. } => "agent.acp.completed",
        Event::AgentAcpCancelled { .. } => "agent.acp.cancelled",
        Event::AgentAcpTimedOut { .. } => "agent.acp.timed_out",
        Event::PullRequestCreationRequested { .. } => "pull_request.creation_requested",
        Event::PullRequestCreated { .. } => "pull_request.created",
        Event::PullRequestLinked { .. } => "pull_request.linked",
        Event::PullRequestUnlinked { .. } => "pull_request.unlinked",
        Event::PullRequestFailed { .. } => "pull_request.failed",
    };
    Cow::Borrowed(name)
}

#[cfg(test)]
mod tests {
    use ::fabro_types::{ParallelBranchId, StageId};
    use pebble_coding_agent::events::{CodingAgentEvent, CodingEvent};

    use super::*;
    use crate::event::Event;

    #[test]
    fn event_name_matches_new_dot_notation() {
        assert_eq!(
            event_name(&Event::ParallelBranchStarted {
                graph_visit:           None,
                resumed_from_stage_id: None,
                parallel_group_id:     StageId::new("plan", 1),
                parallel_branch_id:    ParallelBranchId::new(StageId::new("plan", 1), 0),
                branch:                "fork".to_string(),
                index:                 0,
                item_label:            None,
            }),
            "parallel.branch.started"
        );
        assert_eq!(
            event_name(&Event::AgentMcpDisconnected {
                node_id:     "code".to_string(),
                visit:       1,
                server_name: "github".to_string(),
                error:       "transport closed".to_string(),
            }),
            "agent.mcp.disconnected"
        );
        assert_eq!(
            event_name(&Event::Agent {
                stage: "code".to_string(),
                visit: 1,
                event: CodingAgentEvent::new(
                    "ses_test".to_string(),
                    CodingEvent::SubAgentSpawned {
                        agent_id:   "a1".to_string(),
                        depth:      1,
                        task:       "do it".to_string(),
                        generation: 1,
                    },
                    std::time::SystemTime::UNIX_EPOCH,
                ),
            }),
            "agent.sub.spawned"
        );
        assert_eq!(
            event_name(&Event::Agent {
                stage: "code".to_string(),
                visit: 1,
                event: CodingAgentEvent::new(
                    "ses_test".to_string(),
                    CodingEvent::SubAgentTurnStarted {
                        agent_id:   "a1".to_string(),
                        depth:      1,
                        task:       "fix it".to_string(),
                        generation: 2,
                    },
                    std::time::SystemTime::UNIX_EPOCH,
                ),
            }),
            "agent.sub.turn.started"
        );
        assert_eq!(
            event_name(&Event::Agent {
                stage: "code".to_string(),
                visit: 1,
                event: CodingAgentEvent::new(
                    "session-1".to_string(),
                    CodingEvent::RoundInterrupted { generation: 1 },
                    std::time::SystemTime::UNIX_EPOCH,
                ),
            }),
            "agent.round.interrupted"
        );
        assert_eq!(
            event_name(&Event::AgentToolsAvailable {
                node_id:    "code".to_string(),
                visit:      1,
                session_id: "session-1".to_string(),
                tools:      Vec::new(),
            }),
            "agent.tools.available"
        );
    }

    #[test]
    fn run_archived_event_name_matches_dot_notation() {
        assert_eq!(
            event_name(&Event::RunArchived { actor: None }),
            "run.archived"
        );
        assert_eq!(
            event_name(&Event::RunUnarchived { actor: None }),
            "run.unarchived"
        );
    }
}
