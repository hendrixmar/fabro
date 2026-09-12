use chrono::{DateTime, Utc};
use fabro_types::{BilledModelUsage, EventBody, RunEvent};
use fabro_util::{error, text};
use fabro_workflow::event::RunNoticeLevel;
use pebble_coding_agent::events::{CodingEvent, ErrorKind as AgentErrorKind, LlmOutputKind};
use serde_json::Value;

#[derive(Debug, Clone)]
pub(super) struct ProgressUsage {
    pub(super) input_tokens:  u64,
    pub(super) output_tokens: u64,
    pub(super) cost:          Option<f64>,
}

impl ProgressUsage {
    pub(super) fn from_stage_usage(usage: &BilledModelUsage) -> Self {
        let tokens = usage.tokens();
        Self {
            input_tokens:  tokens.input,
            output_tokens: tokens.billable_output(),
            cost:          usage.total_usd_micros.map(|cost| cost as f64 / 1_000_000.0),
        }
    }

    pub(super) fn total_tokens(&self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }

    pub(super) fn display_cost(&self) -> Option<f64> {
        self.cost
    }
}

#[derive(Debug, Clone)]
pub(super) enum ProgressEvent {
    RunCreated {
        web_url: Option<String>,
    },
    WorkflowStarted {
        worktree_dir: Option<String>,
        base_branch:  Option<String>,
        base_sha:     Option<String>,
    },
    WorkingDirectorySet {
        working_directory: String,
    },
    SandboxInitializing {
        provider: String,
    },
    SandboxReady {
        provider:    String,
        duration_ms: u64,
        name:        Option<String>,
        url:         Option<String>,
    },
    SandboxFailed {
        provider: String,
        error:    String,
    },
    SnapshotPulling {
        name: String,
    },
    SnapshotCreating {
        name: String,
    },
    SnapshotReady {
        name:        String,
        duration_ms: u64,
    },
    SnapshotFailed {
        name:  String,
        error: String,
    },
    SshAccessReady {
        ssh_command: String,
    },
    SetupStarted {
        command_count: u64,
    },
    SetupCompleted {
        duration_ms: u64,
    },
    SetupCommandCompleted {
        command:       String,
        command_index: u64,
        exit_code:     i64,
        duration_ms:   u64,
    },
    CliEnsureStarted {
        cli_name: String,
    },
    CliEnsureCompleted {
        cli_name:          String,
        already_installed: bool,
        duration_ms:       u64,
    },
    CliEnsureFailed {
        cli_name: String,
    },
    StageStarted {
        node_id: String,
        name:    String,
        script:  Option<String>,
    },
    StageCompleted {
        node_id: String,
        name:    String,
        timing:  fabro_types::StageTiming,
        status:  String,
        usage:   Option<ProgressUsage>,
    },
    StageFailed {
        node_id: String,
        name:    String,
        error:   String,
    },
    StageRetrying {
        name:         String,
        attempt:      u64,
        max_attempts: u64,
        delay_ms:     u64,
    },
    ParallelStarted,
    ParallelBranchStarted {
        branch: String,
    },
    ParallelBranchCompleted {
        branch:      String,
        duration_ms: u64,
        status:      fabro_types::StageOutcome,
    },
    ParallelCompleted,
    AssistantMessage {
        stage_node_id: String,
        model:         String,
        root_session:  bool,
    },
    ToolCallStarted {
        stage_node_id: String,
        tool_name:     String,
        tool_call_id:  String,
        arguments:     Value,
        timestamp:     Option<DateTime<Utc>>,
    },
    ToolCallCompleted {
        stage_node_id: String,
        tool_call_id:  String,
        is_error:      bool,
        duration_ms:   Option<u64>,
        timestamp:     Option<DateTime<Utc>>,
    },
    ContextWindowWarning {
        stage_node_id: String,
        usage_percent: u64,
    },
    CompactionStarted {
        stage_node_id: String,
    },
    CompactionCompleted {
        stage_node_id:        String,
        original_turn_count:  u64,
        preserved_turn_count: u64,
        tracked_file_count:   u64,
    },
    CompactionFailed {
        stage_node_id: String,
        error:         String,
        root_session:  bool,
    },
    LlmRequestStarted {
        stage_node_id: String,
        model:         String,
    },
    LlmFirstOutput {
        stage_node_id: String,
        kind:          LlmOutputKind,
    },
    LlmRetry {
        stage_node_id: String,
        model:         String,
        attempt:       u64,
        delay_ms:      u64,
        error:         String,
    },
    LlmRequestFinished {
        stage_node_id: String,
    },
    /// A subagent started work. Generation 1 is the spawn; later generations
    /// are further turns in the same child session.
    SubagentStarted {
        stage_node_id: String,
        agent_id:      String,
        task:          String,
        generation:    u64,
    },
    SubagentCompleted {
        stage_node_id: String,
        agent_id:      String,
        success:       bool,
        turns_used:    u64,
    },
    EdgeSelected {
        from_node: String,
        to_node:   String,
        label:     Option<String>,
        condition: Option<String>,
    },
    LoopRestart {
        from_node: String,
        to_node:   String,
    },
    MetadataSnapshotFailed {
        phase:        String,
        failure_kind: String,
        error:        String,
    },
    RunNotice {
        level:   RunNoticeLevel,
        code:    String,
        message: String,
    },
    PullRequestCreated {
        pr_url: String,
        draft:  bool,
    },
    PullRequestFailed {
        error: String,
    },
}

pub(super) fn from_run_event(stored: &RunEvent) -> Option<ProgressEvent> {
    let node_id = stored.node_id.clone().unwrap_or_else(|| "?".to_string());
    let node_label = stored.node_label.clone().unwrap_or_else(|| node_id.clone());

    match &stored.body {
        EventBody::RunCreated(props) => Some(ProgressEvent::RunCreated {
            web_url: props.web_url.clone(),
        }),
        EventBody::RunStarted(props) => Some(ProgressEvent::WorkflowStarted {
            worktree_dir: props.worktree_dir.clone(),
            base_branch:  props.base_branch.clone(),
            base_sha:     props.base_sha.clone(),
        }),
        EventBody::SandboxInitialized(props) => Some(ProgressEvent::WorkingDirectorySet {
            working_directory: props.working_directory.clone(),
        }),
        EventBody::SandboxInitializing(props) => Some(ProgressEvent::SandboxInitializing {
            provider: props.provider.clone(),
        }),
        EventBody::SandboxReady(props) => Some(ProgressEvent::SandboxReady {
            provider:    props.provider.clone(),
            duration_ms: props.duration_ms,
            name:        props.name.clone(),
            url:         props.url.clone(),
        }),
        EventBody::SandboxFailed(props) => Some(ProgressEvent::SandboxFailed {
            provider: props.provider.clone(),
            error:    props.error.clone(),
        }),
        EventBody::SandboxDriver { event, .. } => driver_progress_event(event),
        EventBody::SshAccessReady(props) => Some(ProgressEvent::SshAccessReady {
            ssh_command: props.ssh_command.clone(),
        }),
        EventBody::SetupStarted(props) => Some(ProgressEvent::SetupStarted {
            command_count: props.command_count as u64,
        }),
        EventBody::SetupCompleted(props) => Some(ProgressEvent::SetupCompleted {
            duration_ms: props.duration_ms,
        }),
        EventBody::SetupCommandCompleted(props) => Some(ProgressEvent::SetupCommandCompleted {
            command:       props.command.clone(),
            command_index: props.index as u64,
            exit_code:     i64::from(props.exit_code),
            duration_ms:   props.duration_ms,
        }),
        EventBody::CliEnsureStarted(props) => Some(ProgressEvent::CliEnsureStarted {
            cli_name: props.cli_name.clone(),
        }),
        EventBody::CliEnsureCompleted(props) => Some(ProgressEvent::CliEnsureCompleted {
            cli_name:          props.cli_name.clone(),
            already_installed: props.already_installed,
            duration_ms:       props.duration_ms,
        }),
        EventBody::CliEnsureFailed(props) => Some(ProgressEvent::CliEnsureFailed {
            cli_name: props.cli_name.clone(),
        }),
        EventBody::StageStarted(_) => Some(ProgressEvent::StageStarted {
            node_id,
            name: node_label,
            script: None,
        }),
        EventBody::StageCompleted(props) => Some(ProgressEvent::StageCompleted {
            node_id,
            name: node_label,
            timing: props.timing,
            status: props.status.to_string(),
            usage: props.billing.as_ref().map(ProgressUsage::from_stage_usage),
        }),
        EventBody::StageFailed(props) => Some(ProgressEvent::StageFailed {
            node_id,
            name: node_label,
            error: props.failure.as_ref().map_or_else(
                || "unknown error".to_string(),
                |failure| error::render_compact_with_causes(&failure.message, &failure.causes),
            ),
        }),
        EventBody::StageRetrying(props) => Some(ProgressEvent::StageRetrying {
            name:         node_label,
            attempt:      props.attempt as u64,
            max_attempts: props.max_attempts as u64,
            delay_ms:     props.delay_ms,
        }),
        EventBody::ParallelStarted(_) => Some(ProgressEvent::ParallelStarted),
        EventBody::ParallelBranchStarted(props) => Some(ProgressEvent::ParallelBranchStarted {
            branch: parallel_branch_display(&node_id, props.index, props.item_label.as_deref()),
        }),
        EventBody::ParallelBranchCompleted(props) => Some(ProgressEvent::ParallelBranchCompleted {
            branch:      parallel_branch_display(
                &node_id,
                props.index,
                props.item_label.as_deref(),
            ),
            duration_ms: props.duration_ms,
            status:      props.status,
        }),
        EventBody::ParallelCompleted(_) => Some(ProgressEvent::ParallelCompleted),
        EventBody::Agent(props) => agent_progress_event(node_id, stored, props.coding_event()),
        EventBody::EdgeSelected(props) => Some(ProgressEvent::EdgeSelected {
            from_node: props.from_node.clone(),
            to_node:   props.to_node.clone(),
            label:     props.label.clone(),
            condition: props.condition.clone(),
        }),
        EventBody::LoopRestart(props) => Some(ProgressEvent::LoopRestart {
            from_node: props.from_node.clone(),
            to_node:   props.to_node.clone(),
        }),
        EventBody::MetadataSnapshotFailed(props) => Some(ProgressEvent::MetadataSnapshotFailed {
            phase:        props.phase.to_string(),
            failure_kind: props.failure_kind.to_string(),
            error:        props.error.clone(),
        }),
        EventBody::RunNotice(props) => Some(ProgressEvent::RunNotice {
            level:   props.level,
            code:    props.code.clone(),
            message: props.message.clone(),
        }),
        EventBody::PullRequestCreated(props) => Some(ProgressEvent::PullRequestCreated {
            pr_url: props.pr_url.clone(),
            draft:  props.draft,
        }),
        EventBody::PullRequestFailed(props) => Some(ProgressEvent::PullRequestFailed {
            error: props.error.clone(),
        }),
        _ => None,
    }
}

/// Name a parallel branch for the terminal.
///
/// `item_label` is sanitized where it is created, but events replayed from a
/// run recorded before that are not, and this string goes straight to the
/// terminal. Sanitizing again is cheap and keeps the guarantee local.
fn parallel_branch_display(node_id: &str, index: usize, item_label: Option<&str>) -> String {
    item_label
        .map(text::sanitize_display_label)
        .filter(|label| !label.is_empty())
        .map_or_else(
            || node_id.to_string(),
            |label| format!("{label} ({node_id} #{index})"),
        )
}

pub(super) fn from_json_line(line: &str) -> Option<ProgressEvent> {
    let stored = RunEvent::from_json_str(line).ok()?;
    from_run_event(&stored)
}

/// The progress line for one coding agent event, if the terminal shows it.
///
/// Inference brackets and interrupts are tracked for the root session only:
/// a subagent's rounds must not move the stage's live line.
fn agent_progress_event(
    node_id: String,
    stored: &RunEvent,
    event: &CodingEvent,
) -> Option<ProgressEvent> {
    let root_session = stored.parent_session_id.is_none();
    match event {
        CodingEvent::AssistantMessage { model, .. } => Some(ProgressEvent::AssistantMessage {
            stage_node_id: node_id,
            model: model.clone(),
            root_session,
        }),
        CodingEvent::ToolCallStarted {
            tool_name,
            tool_call_id,
            arguments,
        } => Some(ProgressEvent::ToolCallStarted {
            stage_node_id: node_id,
            tool_name:     tool_name.clone(),
            tool_call_id:  tool_call_id.clone(),
            arguments:     arguments.clone(),
            timestamp:     Some(stored.ts),
        }),
        CodingEvent::ToolCallCompleted {
            tool_call_id,
            is_error,
            ..
        } => Some(ProgressEvent::ToolCallCompleted {
            stage_node_id: node_id,
            tool_call_id:  tool_call_id.clone(),
            is_error:      *is_error,
            duration_ms:   None,
            timestamp:     Some(stored.ts),
        }),
        CodingEvent::Warning { kind, details, .. } if kind == "context_window" => {
            let usage_percent = details
                .as_object()
                .and_then(|details| details.get("usage_percent"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            Some(ProgressEvent::ContextWindowWarning {
                stage_node_id: node_id,
                usage_percent,
            })
        }
        CodingEvent::CompactionStarted { .. } => Some(ProgressEvent::CompactionStarted {
            stage_node_id: node_id,
        }),
        CodingEvent::CompactionCompleted {
            original_turn_count,
            preserved_turn_count,
            tracked_file_count,
            ..
        } => Some(ProgressEvent::CompactionCompleted {
            stage_node_id:        node_id,
            original_turn_count:  *original_turn_count as u64,
            preserved_turn_count: *preserved_turn_count as u64,
            tracked_file_count:   *tracked_file_count as u64,
        }),
        CodingEvent::CompactionFailed { error, .. } => Some(ProgressEvent::CompactionFailed {
            stage_node_id: node_id,
            error: error.message.clone(),
            root_session,
        }),
        CodingEvent::Error { error } if error.kind == AgentErrorKind::Compaction => {
            Some(ProgressEvent::CompactionFailed {
                stage_node_id: node_id,
                error: error.message.clone(),
                root_session,
            })
        }
        CodingEvent::Error { .. } if root_session => Some(ProgressEvent::LlmRequestFinished {
            stage_node_id: node_id,
        }),
        CodingEvent::LlmRequestStarted { requested_model } if root_session => {
            Some(ProgressEvent::LlmRequestStarted {
                stage_node_id: node_id,
                model:         requested_model.clone(),
            })
        }
        CodingEvent::LlmFirstOutput { kind } if root_session => {
            Some(ProgressEvent::LlmFirstOutput {
                stage_node_id: node_id,
                kind:          *kind,
            })
        }
        CodingEvent::LlmRetry {
            model,
            attempt,
            delay_secs,
            error,
            ..
        } if root_session => {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "Retry delays are represented as small non-negative millisecond values."
            )]
            let delay_ms = (delay_secs * 1000.0) as u64;
            Some(ProgressEvent::LlmRetry {
                stage_node_id: node_id,
                model: model.clone(),
                attempt: *attempt as u64,
                delay_ms,
                error: error.message.clone(),
            })
        }
        CodingEvent::RoundInterrupted { .. } if root_session => {
            Some(ProgressEvent::LlmRequestFinished {
                stage_node_id: node_id,
            })
        }
        CodingEvent::SubAgentSpawned {
            agent_id,
            task,
            generation,
            ..
        }
        | CodingEvent::SubAgentTurnStarted {
            agent_id,
            task,
            generation,
            ..
        } => Some(ProgressEvent::SubagentStarted {
            stage_node_id: node_id,
            agent_id:      agent_id.clone(),
            task:          task.clone(),
            generation:    *generation,
        }),
        CodingEvent::SubAgentCompleted {
            agent_id,
            success,
            turns_used,
            ..
        } => Some(ProgressEvent::SubagentCompleted {
            stage_node_id: node_id,
            agent_id:      agent_id.clone(),
            success:       *success,
            turns_used:    *turns_used as u64,
        }),
        _ => None,
    }
}

/// The setup progress a sandbox driver event stands for: the image pull
/// inside the sandbox's create, or a snapshot build. Every other driver
/// event is stored on the run but renders nothing here.
fn driver_progress_event(event: &sandbox_driver::Event) -> Option<ProgressEvent> {
    use sandbox_driver::{Action, EventBody as Body, EventSubject, ProgressCode};

    match (&event.subject, &event.body) {
        (
            EventSubject::Sandbox { .. },
            Body::OperationProgress {
                action: Action::Create,
                progress,
            },
        ) if progress.code.as_str() == ProgressCode::IMAGE_PULL => {
            Some(ProgressEvent::SnapshotPulling {
                name: pulled_image_name(progress.message.as_deref()),
            })
        }
        (EventSubject::Snapshot { id, name }, body) => {
            let name = name
                .clone()
                .or_else(|| id.as_ref().map(ToString::to_string))
                .unwrap_or_default();
            match body {
                Body::OperationStarted {
                    action: Action::Create,
                } => Some(ProgressEvent::SnapshotCreating { name }),
                Body::OperationCompleted {
                    action: Action::Create,
                    duration,
                } => Some(ProgressEvent::SnapshotReady {
                    name,
                    duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                }),
                Body::OperationFailed {
                    action: Action::Create,
                    error,
                    ..
                } => Some(ProgressEvent::SnapshotFailed {
                    name,
                    error: error.message.clone(),
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The image an image pull progress report names. The Docker provider
/// says `pulling image <reference>`; the reference alone reads better.
fn pulled_image_name(message: Option<&str>) -> String {
    let message = message.unwrap_or("image");
    message
        .strip_prefix("pulling image ")
        .unwrap_or(message)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use fabro_types::{MetadataSnapshotFailureKind, MetadataSnapshotPhase, fixtures};
    use fabro_workflow::event::{Event, RunNoticeCode, SandboxLifecycle, to_run_event};
    use pebble_coding_agent::events::CodingAgentEvent;

    use super::*;

    #[test]
    fn parallel_branch_display_neutralizes_runtime_labels() {
        assert_eq!(
            parallel_branch_display("reviewer", 0, Some("auth")),
            "auth (reviewer #0)"
        );
        assert_eq!(parallel_branch_display("reviewer", 0, None), "reviewer");
        // A label recorded before sanitizing moved to the source must not
        // reach the terminal with escapes or newlines intact.
        assert_eq!(
            parallel_branch_display("reviewer", 1, Some("\u{1b}[31mauth\u{1b}[0m")),
            "auth (reviewer #1)"
        );
        assert_eq!(
            parallel_branch_display("reviewer", 2, Some("auth\nSUCCESS")),
            "authSUCCESS (reviewer #2)"
        );
        // Nothing printable left, so fall back to the node id we control.
        assert_eq!(
            parallel_branch_display("reviewer", 3, Some("\u{1b}[0m  ")),
            "reviewer"
        );
    }

    #[test]
    fn parse_edge_selected() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::EdgeSelected {
            from_node:          "a".into(),
            to_node:            "b".into(),
            label:              Some("yes".into()),
            condition:          None,
            reason:             "condition".into(),
            preferred_label:    None,
            suggested_next_ids: Vec::new(),
            stage_status:       "succeeded".into(),
            is_jump:            false,
        });

        let event = from_run_event(&stored).unwrap();
        assert!(matches!(
            event,
            ProgressEvent::EdgeSelected {
                from_node,
                to_node,
                label,
                ..
            } if from_node == "a" && to_node == "b" && label.as_deref() == Some("yes")
        ));
    }

    #[test]
    fn round_trip_stage_completed() {
        let event = Event::StageCompleted {
            node_id: "plan".into(),
            name: "Plan".into(),
            index: 0,
            timing: fabro_types::StageTiming::wall_only(5000),
            status: "succeeded".into(),
            preferred_label: None,
            suggested_next_ids: Vec::new(),
            billing: None,
            failure: None,
            notes: None,
            files_touched: Vec::new(),
            context_updates: None,
            jump_to_node: None,
            context_values: None,
            node_visits: None,
            loop_failure_signatures: None,
            restart_failure_signatures: None,
            response: None,
            attempt: 1,
            max_attempts: 1,
        };

        let stored = to_run_event(&fixtures::RUN_1, &event);
        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::StageCompleted {
                node_id,
                name,
                timing,
                ..
            } if node_id == "plan" && name == "Plan" && timing.wall_time_ms == 5000
        ));
    }

    #[test]
    fn round_trip_agent_tool_call() {
        let event = Event::Agent {
            stage: "code".into(),
            visit: 1,
            event: CodingAgentEvent::new(
                "ses_root",
                CodingEvent::ToolCallStarted {
                    tool_name:    "read_file".into(),
                    tool_call_id: "tc1".into(),
                    arguments:    serde_json::json!({"path": "src/main.rs"}),
                },
                std::time::SystemTime::UNIX_EPOCH,
            ),
        };

        let stored = to_run_event(&fixtures::RUN_1, &event);
        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::ToolCallStarted {
                stage_node_id,
                tool_name,
                tool_call_id,
                ..
            } if stage_node_id == "code" && tool_name == "read_file" && tool_call_id == "tc1"
        ));
    }

    #[test]
    fn round_trip_pull_request_created_without_stage_scope() {
        let event = Event::PullRequestCreated {
            pr_url:      "https://github.com/acme/widgets/pull/42".into(),
            pr_number:   42,
            owner:       "acme".into(),
            repo:        "widgets".into(),
            base_branch: "main".into(),
            head_branch: "fabro/run/42".into(),
            head_sha:    Some("final-sha".to_string()),
            title:       "Ship the server-side PR".into(),
            draft:       true,
        };

        let stored = to_run_event(&fixtures::RUN_1, &event);
        assert!(stored.node_id.is_none());
        assert!(stored.stage_id.is_none());

        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::PullRequestCreated { pr_url, draft }
                if pr_url == "https://github.com/acme/widgets/pull/42" && draft
        ));
    }

    #[test]
    fn parse_tool_call_timestamps_from_jsonl() {
        let started = from_json_line(
            &serde_json::json!({
                "id": "evt_1",
                "ts": "2026-03-30T12:00:00.000Z",
                "run_id": fixtures::RUN_1.to_string(),
                "event": "agent.tool.started",
                "node_id": "code",
                "node_label": "code",
                "properties": {
                    "stage": "code",
                    "visit": 1,
                    "session_id": "ses_root",
                    "timestamp": "2026-03-30T12:00:00.000Z",
                    "event": {
                        "ToolCallStarted": {
                            "tool_name": "read_file",
                            "tool_call_id": "tc1",
                            "arguments": {"path": "src/main.rs"}
                        }
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        let completed = from_json_line(
            &serde_json::json!({
                "id": "evt_2",
                "ts": "2026-03-30T12:00:00.500Z",
                "run_id": fixtures::RUN_1.to_string(),
                "event": "agent.tool.completed",
                "node_id": "code",
                "node_label": "code",
                "properties": {
                    "stage": "code",
                    "visit": 1,
                    "session_id": "ses_root",
                    "timestamp": "2026-03-30T12:00:00.500Z",
                    "event": {
                        "ToolCallCompleted": {
                            "tool_name": "read_file",
                            "tool_call_id": "tc1",
                            "output": {"ok": true},
                            "is_error": false,
                            "output_bytes_observed": 11,
                            "output_bytes_retained": 11,
                            "output_bytes_omitted": 0
                        }
                    }
                }
            })
            .to_string(),
        )
        .unwrap();

        assert!(matches!(
            started,
            ProgressEvent::ToolCallStarted {
                timestamp: Some(timestamp),
                ..
            } if timestamp == DateTime::parse_from_rfc3339("2026-03-30T12:00:00.000Z")
                .unwrap()
                .with_timezone(&Utc)
        ));
        assert!(matches!(
            completed,
            ProgressEvent::ToolCallCompleted {
                duration_ms: None,
                timestamp: Some(timestamp),
                ..
            } if timestamp == DateTime::parse_from_rfc3339("2026-03-30T12:00:00.500Z")
                .unwrap()
                .with_timezone(&Utc)
        ));
    }

    #[test]
    fn round_trip_sandbox_ready() {
        let event = Event::Sandbox {
            event: SandboxLifecycle::Ready {
                provider:    "daytona".into(),
                duration_ms: 2500,
                name:        Some("sandbox-1".into()),
                url:         Some("https://example.test".into()),
            },
        };

        let stored = to_run_event(&fixtures::RUN_1, &event);
        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::SandboxReady {
                provider,
                duration_ms,
                name,
                ..
            } if provider == "daytona" && duration_ms == 2500 && name.as_deref() == Some("sandbox-1")
        ));
    }

    #[test]
    fn round_trip_sandbox_failed() {
        let event = Event::Sandbox {
            event: SandboxLifecycle::InitializeFailed {
                provider:    "docker".into(),
                error:       "pull failed".into(),
                causes:      Vec::new(),
                duration_ms: 900,
            },
        };

        let stored = to_run_event(&fixtures::RUN_1, &event);
        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::SandboxFailed { provider, error }
                if provider == "docker" && error == "pull failed"
        ));
    }

    fn driver_event(value: serde_json::Value) -> Event {
        Event::SandboxDriver {
            event: serde_json::from_value(value).expect("a driver event"),
        }
    }

    #[test]
    fn round_trip_driver_events_that_render_setup_progress() {
        let pulling = to_run_event(
            &fixtures::RUN_1,
            &driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 1},
                "occurred_at": "2026-01-01T00:00:00Z",
                "provider": "docker",
                "subject": {"type": "sandbox"},
                "type": "operation_progress",
                "action": "create",
                "progress": {"code": "image.pull", "message": "pulling image buildpack-deps:noble"}
            })),
        );
        let creating = to_run_event(
            &fixtures::RUN_1,
            &driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 2},
                "occurred_at": "2026-01-01T00:00:00Z",
                "provider": "daytona",
                "subject": {"type": "snapshot", "name": "fabro-v9"},
                "type": "operation_started",
                "action": "create"
            })),
        );
        let ready = to_run_event(
            &fixtures::RUN_1,
            &driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 3},
                "occurred_at": "2026-01-01T00:00:01Z",
                "provider": "daytona",
                "subject": {"type": "snapshot", "name": "fabro-v9"},
                "type": "operation_completed",
                "action": "create",
                "duration": {"secs": 1, "nanos": 200_000_000}
            })),
        );
        let failed = to_run_event(
            &fixtures::RUN_1,
            &driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 4},
                "occurred_at": "2026-01-01T00:00:02Z",
                "provider": "daytona",
                "subject": {"type": "snapshot", "name": "fabro-v9"},
                "type": "operation_failed",
                "action": "create",
                "duration": {"secs": 2, "nanos": 0},
                "error": {"kind": "provider", "message": "build failed", "retryable": false, "causes": []}
            })),
        );
        let stopped = to_run_event(
            &fixtures::RUN_1,
            &driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 5},
                "occurred_at": "2026-01-01T00:00:03Z",
                "provider": "docker",
                "subject": {"type": "sandbox", "id": "c1"},
                "type": "operation_completed",
                "action": "stop",
                "duration": {"secs": 0, "nanos": 0}
            })),
        );

        assert_eq!(pulling.event_name(), "sandbox.create.progress");
        assert!(matches!(
            from_run_event(&pulling).unwrap(),
            ProgressEvent::SnapshotPulling { name } if name == "buildpack-deps:noble"
        ));
        assert!(matches!(
            from_run_event(&creating).unwrap(),
            ProgressEvent::SnapshotCreating { name } if name == "fabro-v9"
        ));
        assert!(matches!(
            from_run_event(&ready).unwrap(),
            ProgressEvent::SnapshotReady { name, duration_ms }
                if name == "fabro-v9" && duration_ms == 1200
        ));
        assert!(matches!(
            from_run_event(&failed).unwrap(),
            ProgressEvent::SnapshotFailed { name, error }
                if name == "fabro-v9" && error == "build failed"
        ));
        assert_eq!(stopped.event_name(), "sandbox.stop.completed");
        assert!(
            from_run_event(&stopped).is_none(),
            "a stop is stored on the run but renders no setup progress"
        );
    }

    #[test]
    fn round_trip_run_notice() {
        let event = Event::RunNotice {
            level:            RunNoticeLevel::Warn,
            code:             RunNoticeCode::SandboxCleanupFailed.to_string(),
            message:          "sandbox cleanup failed".into(),
            exec_output_tail: None,
        };
        let expected_code = RunNoticeCode::SandboxCleanupFailed.to_string();

        let stored = to_run_event(&fixtures::RUN_1, &event);
        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::RunNotice {
                level: RunNoticeLevel::Warn,
                code,
                message,
            } if code == expected_code && message == "sandbox cleanup failed"
        ));
    }

    #[test]
    fn round_trip_metadata_snapshot_failed() {
        let event = Event::MetadataSnapshotFailed {
            phase:            MetadataSnapshotPhase::Finalize,
            branch:           "fabro/meta".into(),
            duration_ms:      900,
            failure_kind:     MetadataSnapshotFailureKind::Push,
            error:            "push rejected".into(),
            causes:           vec!["remote rejected".into()],
            commit_sha:       Some("abc123".into()),
            entry_count:      Some(2),
            bytes:            Some(42),
            exec_output_tail: None,
        };

        let stored = to_run_event(&fixtures::RUN_1, &event);
        let parsed = from_run_event(&stored).unwrap();
        assert!(matches!(
            parsed,
            ProgressEvent::MetadataSnapshotFailed {
                phase,
                failure_kind,
                error,
            } if phase == "finalize" && failure_kind == "push" && error == "push rejected"
        ));
    }
}
