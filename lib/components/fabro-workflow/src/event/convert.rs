use ::fabro_types::{
    EventBody, RunControlAction, RunEvent, RunId, StageOutcome, run_event as fabro_types,
};
use chrono::Utc;
use uuid::Uuid;

use super::stored_fields::stored_event_fields;
use super::{Event, SandboxLifecycle};
use crate::stage_scope::StageScope;

fn stage_status_from_string(status: &str) -> StageOutcome {
    status.parse().unwrap_or_else(|_| {
        tracing::warn!(
            status,
            "unknown stage status in StageCompleted event; using Fail"
        );
        StageOutcome::Failed {
            retry_requested: false,
        }
    })
}

/// Project the sandbox layer's runtime push attempts into the durable
/// `git.push` attempt shape.
///
/// This is the only place the runtime attempt record crosses into stored
/// events: the token snapshot flattens into the three flat `token_*` fields
/// (a nested provenance enum never appears in stored events), and the retry
/// classifier's verdict becomes `classified_reason`.
fn git_push_attempt_props(
    attempts: &[fabro_sandbox::PushAttempt],
) -> Vec<fabro_types::GitPushAttemptProps> {
    attempts
        .iter()
        .map(|attempt| fabro_types::GitPushAttemptProps {
            attempt:           attempt.attempt,
            started_at:        attempt.started_at,
            success:           attempt.success,
            classified_reason: attempt.retry_reason,
            exec_output_tail:  attempt.exec_output_tail.clone(),
            token_generation:  attempt.token.map(|token| token.generation),
            token_provenance:  attempt.token.map(|token| match token.provenance {
                fabro_sandbox::TokenProvenance::Minted { .. } => {
                    fabro_types::GitTokenProvenance::Minted
                }
                fabro_sandbox::TokenProvenance::Reused { .. } => {
                    fabro_types::GitTokenProvenance::Reused
                }
                fabro_sandbox::TokenProvenance::Static => fabro_types::GitTokenProvenance::Static,
            }),
            token_age_ms:      attempt
                .token
                .and_then(|token| token.age_at(attempt.started_at))
                .map(|age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX)),
        })
        .collect()
}

fn event_body_from_event(event: &Event) -> EventBody {
    match event {
        Event::RunCreated {
            title,
            settings,
            graph,
            workflow_source,
            labels,
            source_directory,
            workflow_slug,
            workflow_version_id,
            target,
            automation,
            provenance,
            manifest_blob,
            spec_blob,
            git,
            fork_source_ref,
            retried_from,
            parent_id,
            web_url,
            ..
        } => EventBody::RunCreated(fabro_types::RunCreatedProps {
            title:            title.clone(),
            settings:         serde_json::from_value(settings.clone())
                .expect("run.created settings should deserialize: value was serialized from a typed struct in this session"),
            graph:            serde_json::from_value(graph.clone()).expect("run.created graph should deserialize: value was serialized from a typed struct in this session"),
            workflow_source:  workflow_source.clone(),
            labels:           labels.clone(),
            source_directory: source_directory.clone(),
            workflow_slug:    workflow_slug.clone(),
            workflow_version_id: *workflow_version_id,
            target:           target.clone(),
            automation:       automation.clone(),
            provenance:       provenance.clone(),
            manifest_blob:    *manifest_blob,
            spec_blob:        *spec_blob,
            git:              git.clone(),
            fork_source_ref:  fork_source_ref.clone(),
            retried_from:     *retried_from,
            parent_id:        *parent_id,
            web_url:          web_url.clone(),
        }),
        Event::WorkflowRunStarted {
            name,
            base_branch,
            base_sha,
            run_branch,
            worktree_dir,
            goal,
            ..
        } => EventBody::RunStarted(fabro_types::RunStartedProps {
            name:         name.clone(),
            base_branch:  base_branch.clone(),
            base_sha:     base_sha.clone(),
            run_branch:   run_branch.clone(),
            worktree_dir: worktree_dir.clone(),
            goal:         goal.clone(),
        }),
        Event::RunSubmitted { definition_blob } => {
            EventBody::RunSubmitted(fabro_types::RunSubmittedProps {
                definition_blob: *definition_blob,
            })
        }
        Event::RunStartRequested { resume, .. } => {
            EventBody::RunStartRequested(fabro_types::RunStartRequestedProps { resume: *resume })
        }
        Event::RunPending { reason, .. } => {
            EventBody::RunPending(fabro_types::RunPendingProps { reason: *reason })
        }
        Event::RunApproved { .. } => {
            EventBody::RunApproved(fabro_types::RunApprovedProps::default())
        }
        Event::RunDenied { reason, .. } => EventBody::RunDenied(fabro_types::RunDeniedProps {
            reason: reason.clone(),
        }),
        Event::RunRunnable { source, .. } => {
            EventBody::RunRunnable(fabro_types::RunRunnableProps { source: *source })
        }
        Event::RunStarting => {
            EventBody::RunStarting(fabro_types::RunStatusTransitionProps::default())
        }
        Event::RunRunning => {
            EventBody::RunRunning(fabro_types::RunStatusTransitionProps::default())
        }
        Event::RunInterrupt { .. } => {
            EventBody::RunInterrupt(fabro_types::RunInterruptProps::default())
        }
        Event::RunSteer { text, .. } => {
            EventBody::RunSteer(fabro_types::RunSteerProps { text: text.clone() })
        }
        Event::RunPairStarted {
            pair_id, target, ..
        } => EventBody::RunPairStarted(fabro_types::RunPairStartedProps {
            pair_id: *pair_id,
            target:  target.clone(),
        }),
        Event::RunPairEnded {
            pair_id, reason, ..
        } => EventBody::RunPairEnded(fabro_types::RunPairEndedProps {
            pair_id: *pair_id,
            reason:  *reason,
        }),
        Event::RunPairFailed {
            pair_id,
            reason,
            message,
            ..
        } => EventBody::RunPairFailed(fabro_types::RunPairFailedProps {
            pair_id: *pair_id,
            reason:  *reason,
            message: message.clone(),
        }),
        Event::RunBlocked { blocked_reason } => {
            EventBody::RunBlocked(fabro_types::RunBlockedProps {
                blocked_reason: *blocked_reason,
            })
        }
        Event::RunUnblocked => {
            EventBody::RunUnblocked(fabro_types::RunStatusEffectProps::default())
        }
        Event::RunRemoving => {
            EventBody::RunRemoving(fabro_types::RunStatusTransitionProps::default())
        }
        Event::RunCancelRequested { .. } => {
            EventBody::RunCancelRequested(fabro_types::RunControlRequestedProps {
                action: RunControlAction::Cancel,
            })
        }
        Event::RunPauseRequested { .. } => {
            EventBody::RunPauseRequested(fabro_types::RunControlRequestedProps {
                action: RunControlAction::Pause,
            })
        }
        Event::RunUnpauseRequested { .. } => {
            EventBody::RunUnpauseRequested(fabro_types::RunControlRequestedProps {
                action: RunControlAction::Unpause,
            })
        }
        Event::RunPaused => EventBody::RunPaused(fabro_types::RunControlEffectProps::default()),
        Event::RunUnpaused => EventBody::RunUnpaused(fabro_types::RunControlEffectProps::default()),
        Event::RunSupersededBy {
            new_run_id,
            target_checkpoint_ordinal,
            target_node_id,
            target_visit,
        } => EventBody::RunSupersededBy(fabro_types::RunSupersededByProps {
            new_run_id:                *new_run_id,
            target_checkpoint_ordinal: *target_checkpoint_ordinal,
            target_node_id:            target_node_id.clone(),
            target_visit:              *target_visit,
        }),
        Event::RunArchived { .. } => {
            EventBody::RunArchived(fabro_types::RunArchivedProps::default())
        }
        Event::RunUnarchived { .. } => {
            EventBody::RunUnarchived(fabro_types::RunUnarchivedProps::default())
        }
        Event::RunTitleUpdated { title, .. } => {
            EventBody::RunTitleUpdated(fabro_types::RunTitleUpdatedProps {
                title: title.clone(),
            })
        }
        Event::RunParentLinked {
            previous_parent_id,
            parent_id,
            ..
        } => EventBody::RunParentLinked(fabro_types::RunParentLinkedProps {
            previous_parent_id: *previous_parent_id,
            parent_id:          *parent_id,
        }),
        Event::RunParentUnlinked {
            previous_parent_id, ..
        } => EventBody::RunParentUnlinked(fabro_types::RunParentUnlinkedProps {
            previous_parent_id: *previous_parent_id,
        }),
        Event::WorkflowRunCompleted {
            timing,
            artifact_count,
            status,
            reason,
            total_usd_micros,
            final_git_commit_sha,
            final_patch,
            diff_summary,
            billing,
        } => EventBody::RunCompleted(fabro_types::RunCompletedProps {
            timing:               *timing,
            artifact_count:       *artifact_count,
            status:               status.clone(),
            reason:               *reason,
            total_usd_micros:     *total_usd_micros,
            final_git_commit_sha: final_git_commit_sha.clone(),
            final_patch:          final_patch.clone(),
            diff_summary:         *diff_summary,
            billing:              billing.clone(),
        }),
        Event::WorkflowRunFailed {
            failure,
            timing,
            final_git_commit_sha,
            final_patch,
            diff_summary,
            billing,
        } => EventBody::RunFailed(fabro_types::RunFailedProps {
            failure:              failure.clone(),
            timing:               *timing,
            final_git_commit_sha: final_git_commit_sha.clone(),
            final_patch:          final_patch.clone(),
            diff_summary:         *diff_summary,
            billing:              billing.clone(),
        }),
        Event::RunNotice {
            level,
            code,
            message,
            exec_output_tail,
        } => EventBody::RunNotice(fabro_types::RunNoticeProps {
            level:            *level,
            code:             code.clone(),
            message:          message.clone(),
            exec_output_tail: exec_output_tail.clone(),
        }),
        Event::MetadataSnapshotStarted { phase, branch } => {
            EventBody::MetadataSnapshotStarted(fabro_types::MetadataSnapshotStartedProps {
                phase:  *phase,
                branch: branch.clone(),
            })
        }
        Event::MetadataSnapshotCompleted {
            phase,
            branch,
            duration_ms,
            entry_count,
            bytes,
            commit_sha,
        } => EventBody::MetadataSnapshotCompleted(fabro_types::MetadataSnapshotCompletedProps {
            phase:       *phase,
            branch:      branch.clone(),
            duration_ms: *duration_ms,
            entry_count: *entry_count,
            bytes:       *bytes,
            commit_sha:  commit_sha.clone(),
        }),
        Event::MetadataSnapshotFailed {
            phase,
            branch,
            duration_ms,
            failure_kind,
            error,
            causes,
            commit_sha,
            entry_count,
            bytes,
            exec_output_tail,
        } => EventBody::MetadataSnapshotFailed(fabro_types::MetadataSnapshotFailedProps {
            phase:            *phase,
            branch:           branch.clone(),
            duration_ms:      *duration_ms,
            failure_kind:     *failure_kind,
            error:            error.clone(),
            causes:           causes.clone(),
            commit_sha:       commit_sha.clone(),
            entry_count:      *entry_count,
            bytes:            *bytes,
            exec_output_tail: exec_output_tail.clone(),
        }),
        Event::StageStarted {
            index,
            handler_type,
            attempt,
            max_attempts,
            graph_visit,
            resumed_from_stage_id,
            ..
        } => EventBody::StageStarted(fabro_types::StageStartedProps {
            index: *index,
            handler_type: handler_type.clone(),
            attempt: *attempt,
            max_attempts: *max_attempts,
            graph_visit: *graph_visit,
            resumed_from_stage_id: resumed_from_stage_id.clone(),
        }),
        Event::StageCompleted {
            index,
            timing,
            status,
            preferred_label,
            suggested_next_ids,
            billing,
            failure,
            notes,
            files_touched,
            context_updates,
            jump_to_node,
            context_values,
            node_visits,
            loop_failure_signatures,
            restart_failure_signatures,
            response,
            attempt,
            max_attempts,
            ..
        } => EventBody::StageCompleted(fabro_types::StageCompletedProps {
            index: *index,
            timing: *timing,
            status: stage_status_from_string(status),
            preferred_label: preferred_label.clone(),
            suggested_next_ids: suggested_next_ids.clone(),
            billing: billing.clone(),
            failure: failure.clone(),
            notes: notes.clone(),
            files_touched: files_touched.clone(),
            context_updates: context_updates.clone(),
            jump_to_node: jump_to_node.clone(),
            context_values: context_values.clone(),
            node_visits: node_visits.clone(),
            loop_failure_signatures: loop_failure_signatures.clone(),
            restart_failure_signatures: restart_failure_signatures.clone(),
            response: response.clone(),
            attempt: *attempt,
            max_attempts: *max_attempts,
        }),
        Event::StageFailed {
            index,
            failure,
            will_retry,
            timing,
            billing,
            ..
        } => EventBody::StageFailed(fabro_types::StageFailedProps {
            index:      *index,
            failure:    Some(failure.clone()),
            will_retry: *will_retry,
            timing:     *timing,
            billing:    billing.clone(),
        }),
        Event::StageRetrying {
            index,
            attempt,
            max_attempts,
            delay_ms,
            ..
        } => EventBody::StageRetrying(fabro_types::StageRetryingProps {
            index:        *index,
            attempt:      *attempt,
            max_attempts: *max_attempts,
            delay_ms:     *delay_ms,
        }),
        Event::ParallelStarted {
            visit,
            branch_count,
            ..
        } => EventBody::ParallelStarted(fabro_types::ParallelStartedProps {
            visit:        *visit,
            branch_count: *branch_count,
        }),
        Event::ParallelBranchStarted {
            index,
            item_label,
            graph_visit,
            resumed_from_stage_id,
            ..
        } => EventBody::ParallelBranchStarted(fabro_types::ParallelBranchStartedProps {
            index: *index,
            item_label: item_label.clone(),
            graph_visit: *graph_visit,
            resumed_from_stage_id: resumed_from_stage_id.clone(),
        }),
        Event::ParallelBranchCompleted {
            index,
            item_label,
            duration_ms,
            status,
            ..
        } => EventBody::ParallelBranchCompleted(fabro_types::ParallelBranchCompletedProps {
            index:       *index,
            item_label:  item_label.clone(),
            duration_ms: *duration_ms,
            status:      *status,
        }),
        Event::ParallelCompleted {
            visit,
            duration_ms,
            success_count,
            failure_count,
            results,
            ..
        } => EventBody::ParallelCompleted(fabro_types::ParallelCompletedProps {
            visit:         *visit,
            duration_ms:   *duration_ms,
            success_count: *success_count,
            failure_count: *failure_count,
            results:       results.clone(),
        }),
        Event::InterviewStarted {
            question_id,
            question,
            stage,
            question_type,
            options,
            allow_freeform,
            timeout_seconds,
            context_display,
            review_target,
        } => EventBody::InterviewStarted(fabro_types::InterviewStartedProps {
            question_id:     question_id.clone(),
            question:        question.clone(),
            stage:           stage.clone(),
            question_type:   question_type.clone(),
            options:         options.clone(),
            allow_freeform:  *allow_freeform,
            timeout_seconds: *timeout_seconds,
            context_display: context_display.clone(),
            review_target:   review_target.clone(),
        }),
        Event::InterviewCompleted {
            actor: _,
            question_id,
            question,
            answer,
            duration_ms,
        } => EventBody::InterviewCompleted(fabro_types::InterviewCompletedProps {
            question_id: question_id.clone(),
            question:    question.clone(),
            answer:      answer.clone(),
            duration_ms: *duration_ms,
        }),
        Event::InterviewTimeout {
            actor: _,
            question_id,
            question,
            stage,
            duration_ms,
        } => EventBody::InterviewTimeout(fabro_types::InterviewTimeoutProps {
            question_id: question_id.clone(),
            question:    question.clone(),
            stage:       stage.clone(),
            duration_ms: *duration_ms,
        }),
        Event::InterviewInterrupted {
            actor: _,
            question_id,
            question,
            stage,
            reason,
            duration_ms,
        } => EventBody::InterviewInterrupted(fabro_types::InterviewInterruptedProps {
            question_id: question_id.clone(),
            question:    question.clone(),
            stage:       stage.clone(),
            reason:      reason.clone(),
            duration_ms: *duration_ms,
        }),
        Event::CheckpointCompleted {
            status,
            current_node,
            completed_nodes,
            node_retries,
            context_values,
            node_outcomes,
            next_node_id,
            git_commit_sha,
            loop_failure_signatures,
            restart_failure_signatures,
            node_visits,
            diff,
            diff_summary,
            graph_visit,
            resumed_from_stage_id,
            ..
        } => EventBody::CheckpointCompleted(fabro_types::CheckpointCompletedProps {
            status: status.clone(),
            current_node: current_node.clone(),
            completed_nodes: completed_nodes.clone(),
            node_retries: node_retries.clone(),
            context_values: context_values.clone(),
            node_outcomes: node_outcomes.clone(),
            next_node_id: next_node_id.clone(),
            git_commit_sha: git_commit_sha.clone(),
            loop_failure_signatures: loop_failure_signatures.clone(),
            restart_failure_signatures: restart_failure_signatures.clone(),
            node_visits: node_visits.clone(),
            diff: diff.clone(),
            diff_summary: *diff_summary,
            graph_visit: *graph_visit,
            resumed_from_stage_id: resumed_from_stage_id.clone(),
        }),
        Event::CheckpointFailed {
            error,
            exec_output_tail,
            ..
        } => EventBody::CheckpointFailed(fabro_types::CheckpointFailedProps {
            error:            error.clone(),
            exec_output_tail: exec_output_tail.clone(),
        }),
        Event::GitCommit { sha, .. } => {
            EventBody::GitCommit(fabro_types::GitCommitProps { sha: sha.clone() })
        }
        Event::GitPush {
            branch,
            success,
            exec_output_tail,
            attempts,
        } => EventBody::GitPush(fabro_types::GitPushProps {
            branch:           branch.clone(),
            success:          *success,
            exec_output_tail: exec_output_tail.clone(),
            attempts:         git_push_attempt_props(attempts),
        }),
        Event::GitFetch { branch, success } => EventBody::GitFetch(fabro_types::GitFetchProps {
            branch:  branch.clone(),
            success: *success,
        }),
        Event::GitReset { sha } => {
            EventBody::GitReset(fabro_types::GitResetProps { sha: sha.clone() })
        }
        Event::EdgeSelected {
            from_node,
            to_node,
            label,
            condition,
            reason,
            preferred_label,
            suggested_next_ids,
            stage_status,
            is_jump,
        } => EventBody::EdgeSelected(fabro_types::EdgeSelectedProps {
            from_node:          from_node.clone(),
            to_node:            to_node.clone(),
            label:              label.clone(),
            condition:          condition.clone(),
            reason:             reason.clone(),
            preferred_label:    preferred_label.clone(),
            suggested_next_ids: suggested_next_ids.clone(),
            stage_status:       stage_status.clone(),
            is_jump:            *is_jump,
        }),
        Event::LoopRestart { from_node, to_node } => {
            EventBody::LoopRestart(fabro_types::LoopRestartProps {
                from_node: from_node.clone(),
                to_node:   to_node.clone(),
            })
        }
        Event::Prompt {
            visit,
            text,
            mode,
            provider,
            model,
            reasoning_effort,
            speed,
            ..
        } => EventBody::StagePrompt(fabro_types::StagePromptProps {
            visit:            *visit,
            text:             text.clone(),
            mode:             mode.clone(),
            provider:         provider.clone(),
            model:            model.clone(),
            reasoning_effort: *reasoning_effort,
            speed:            *speed,
        }),
        Event::PromptCompleted {
            response,
            model,
            provider,
            billing,
            ..
        } => EventBody::PromptCompleted(fabro_types::PromptCompletedProps {
            response: response.clone(),
            model:    model.clone(),
            provider: provider.clone(),
            billing:  billing.clone(),
        }),
        Event::Agent {
            stage,
            visit,
            event,
        } => EventBody::Agent(fabro_types::AgentEventProps::new(
            stage.clone(),
            *visit,
            event.clone(),
        )),
        Event::SubgraphStarted { start_node, .. } => {
            EventBody::SubgraphStarted(fabro_types::SubgraphStartedProps {
                start_node: start_node.clone(),
            })
        }
        Event::SubgraphCompleted {
            steps_executed,
            status,
            duration_ms,
            ..
        } => EventBody::SubgraphCompleted(fabro_types::SubgraphCompletedProps {
            steps_executed: *steps_executed,
            status:         status.clone(),
            duration_ms:    *duration_ms,
        }),
        Event::Sandbox { event } => match event {
            SandboxLifecycle::Initializing { provider } => {
                EventBody::SandboxInitializing(fabro_types::SandboxInitializingProps {
                    provider: provider.clone(),
                })
            }
            SandboxLifecycle::Ready {
                provider,
                duration_ms,
                name,
                url,
            } => EventBody::SandboxReady(fabro_types::SandboxReadyProps {
                provider:    provider.clone(),
                duration_ms: *duration_ms,
                name:        name.clone(),
                url:         url.clone(),
            }),
            SandboxLifecycle::InitializeFailed {
                provider,
                error,
                causes,
                duration_ms,
            } => EventBody::SandboxFailed(fabro_types::SandboxFailedProps {
                provider:    provider.clone(),
                error:       error.clone(),
                causes:      causes.clone(),
                duration_ms: *duration_ms,
            }),
        },
        Event::SandboxDriver { event } => EventBody::sandbox_driver(event.clone()),
        Event::SandboxInitialized {
            working_directory,
            provider,
            id,
            image,
            snapshot,
            repo_cloned,
            clone_origin_url,
            clone_branch,
            workspace_root,
            repos_root,
            primary_repo_path,
            primary_repo_link,
        } => EventBody::SandboxInitialized(fabro_types::SandboxInitializedProps {
            working_directory: working_directory.clone(),
            provider:          provider.clone(),
            id:                id.clone(),
            image:             image.clone(),
            snapshot:          snapshot.clone(),
            repo_cloned:       *repo_cloned,
            clone_origin_url:  clone_origin_url.clone(),
            clone_branch:      clone_branch.clone(),
            workspace_root:    workspace_root.clone(),
            repos_root:        repos_root.clone(),
            primary_repo_path: primary_repo_path.clone(),
            primary_repo_link: primary_repo_link.clone(),
        }),
        Event::SetupStarted { command_count } => {
            EventBody::SetupStarted(fabro_types::SetupStartedProps {
                command_count: *command_count,
            })
        }
        Event::SetupCommandStarted { command, index } => {
            EventBody::SetupCommandStarted(fabro_types::SetupCommandStartedProps {
                command: command.clone(),
                index:   *index,
            })
        }
        Event::SetupCommandCompleted {
            command,
            index,
            exit_code,
            duration_ms,
        } => EventBody::SetupCommandCompleted(fabro_types::SetupCommandCompletedProps {
            command:     command.clone(),
            index:       *index,
            exit_code:   *exit_code,
            duration_ms: *duration_ms,
        }),
        Event::SetupCompleted { duration_ms } => {
            EventBody::SetupCompleted(fabro_types::SetupCompletedProps {
                duration_ms: *duration_ms,
            })
        }
        Event::GitIdentityResolved { identity } => {
            EventBody::GitIdentityResolved(fabro_types::GitIdentityResolvedProps {
                identity: identity.clone(),
            })
        }
        Event::SetupFailed {
            command,
            index,
            exit_code,
            stderr,
            exec_output_tail,
        } => EventBody::SetupFailed(fabro_types::SetupFailedProps {
            command:          command.clone(),
            index:            *index,
            exit_code:        *exit_code,
            stderr:           stderr.clone(),
            exec_output_tail: exec_output_tail.clone(),
        }),
        Event::StallWatchdogTimeout { idle_seconds, .. } => {
            EventBody::StallWatchdogTimeout(fabro_types::StallWatchdogTimeoutProps {
                idle_seconds: *idle_seconds,
            })
        }
        Event::ArtifactCaptured {
            attempt,
            node_slug,
            path,
            mime,
            content_md5,
            content_sha256,
            bytes,
            ..
        } => EventBody::ArtifactCaptured(fabro_types::ArtifactCapturedProps {
            attempt:        *attempt,
            node_slug:      node_slug.clone(),
            path:           path.clone(),
            mime:           mime.clone(),
            content_md5:    content_md5.clone(),
            content_sha256: content_sha256.clone(),
            bytes:          *bytes,
        }),
        Event::SshAccessReady { ssh_command } => {
            EventBody::SshAccessReady(fabro_types::SshAccessReadyProps {
                ssh_command: ssh_command.clone(),
            })
        }
        Event::Failover { props, .. } => EventBody::Failover(props.clone()),
        Event::CommandStarted {
            script,
            command,
            language,
            timeout_ms,
            ..
        } => EventBody::CommandStarted(fabro_types::CommandStartedProps {
            script:     script.clone(),
            command:    command.clone(),
            language:   language.clone(),
            timeout_ms: *timeout_ms,
        }),
        Event::CommandCompleted {
            output,
            exit_code,
            duration_ms,
            termination,
            output_bytes,
            live_streaming,
            ..
        } => EventBody::CommandCompleted(fabro_types::CommandCompletedProps {
            output:         output.clone(),
            exit_code:      *exit_code,
            duration_ms:    *duration_ms,
            termination:    *termination,
            output_bytes:   *output_bytes,
            live_streaming: *live_streaming,
        }),
        Event::AgentSessionActivated {
            thread_id,
            provider,
            model,
            reasoning_effort,
            speed,
            permission_level,
            capabilities,
            visit,
            ..
        } => EventBody::AgentSessionActivated(fabro_types::AgentSessionActivatedProps {
            thread_id:        thread_id.clone(),
            provider:         provider.clone(),
            model:            model.clone(),
            reasoning_effort: *reasoning_effort,
            speed:            *speed,
            permission_level: *permission_level,
            capabilities:     capabilities.clone(),
            visit:            *visit,
        }),
        Event::AgentToolsAvailable { tools, visit, .. } => {
            EventBody::AgentToolsAvailable(fabro_types::AgentToolsAvailableProps {
                tools: tools.clone(),
                visit: *visit,
            })
        }
        Event::AgentSessionDeactivated { visit, .. } => {
            EventBody::AgentSessionDeactivated(fabro_types::AgentSessionDeactivatedProps {
                visit: *visit,
            })
        }
        Event::AgentMcpReady {
            visit,
            server_name,
            tool_count,
            tools,
            ..
        } => EventBody::AgentMcpReady(fabro_types::AgentMcpReadyProps {
            server_name: server_name.clone(),
            tool_count:  *tool_count,
            tools:       tools.clone(),
            visit:       *visit,
        }),
        Event::AgentMcpFailed {
            visit,
            server_name,
            error,
            ..
        } => EventBody::AgentMcpFailed(fabro_types::AgentMcpFailedProps {
            server_name: server_name.clone(),
            error:       error.clone(),
            visit:       *visit,
        }),
        Event::AgentInterruptInjected { visit, .. } => {
            EventBody::AgentInterruptInjected(fabro_types::AgentInterruptInjectedProps {
                visit: *visit,
            })
        }
        Event::AgentPairUserMessage {
            visit,
            pair_id,
            message_id,
            client_message_id,
            text,
            ..
        } => EventBody::AgentPairUserMessage(fabro_types::AgentPairUserMessageProps {
            pair_id:           *pair_id,
            message_id:        *message_id,
            client_message_id: client_message_id.clone(),
            text:              text.clone(),
            visit:             *visit,
        }),
        Event::AgentPairSystemMessage {
            visit,
            pair_id,
            kind,
            text,
            ..
        } => EventBody::AgentPairSystemMessage(fabro_types::AgentPairSystemMessageProps {
            pair_id: *pair_id,
            kind:    *kind,
            text:    text.clone(),
            visit:   *visit,
        }),
        Event::AgentSteerBuffered { .. } => {
            EventBody::AgentSteerBuffered(fabro_types::AgentSteerBufferedProps::default())
        }
        Event::AgentSteerDropped { reason, count, .. } => {
            EventBody::AgentSteerDropped(fabro_types::AgentSteerDroppedProps {
                reason: *reason,
                count:  *count,
            })
        }
        Event::AgentAcpStarted {
            visit,
            command,
            config_name,
            ..
        } => EventBody::AgentAcpStarted(fabro_types::AgentAcpStartedProps {
            visit:       *visit,
            command:     command.clone(),
            config_name: config_name.clone(),
        }),
        Event::AgentAcpCompleted {
            stdout,
            stderr,
            stop_reason,
            duration_ms,
            ..
        } => EventBody::AgentAcpCompleted(fabro_types::AgentAcpCompletedProps {
            stdout:      stdout.clone(),
            stderr:      stderr.clone(),
            stop_reason: stop_reason.clone(),
            duration_ms: *duration_ms,
        }),
        Event::AgentAcpCancelled {
            stdout,
            stderr,
            duration_ms,
            ..
        } => EventBody::AgentAcpCancelled(fabro_types::AgentAcpCancelledProps {
            stdout:      stdout.clone(),
            stderr:      stderr.clone(),
            duration_ms: *duration_ms,
        }),
        Event::AgentAcpTimedOut {
            stdout,
            stderr,
            duration_ms,
            ..
        } => EventBody::AgentAcpTimedOut(fabro_types::AgentAcpTimedOutProps {
            stdout:      stdout.clone(),
            stderr:      stderr.clone(),
            duration_ms: *duration_ms,
        }),
        Event::PullRequestCreationRequested {
            creation_id,
            model,
            force,
        } => EventBody::PullRequestCreationRequested(
            fabro_types::PullRequestCreationRequestedProps {
                creation_id: *creation_id,
                model:       model.clone(),
                force:       *force,
            },
        ),
        Event::PullRequestCreated {
            pr_url,
            pr_number,
            owner,
            repo,
            base_branch,
            head_branch,
            head_sha,
            title,
            draft,
        } => EventBody::PullRequestCreated(fabro_types::PullRequestCreatedProps {
            pr_url:      pr_url.clone(),
            pr_number:   *pr_number,
            owner:       owner.clone(),
            repo:        repo.clone(),
            base_branch: base_branch.clone(),
            head_branch: head_branch.clone(),
            head_sha:    head_sha.clone(),
            title:       title.clone(),
            draft:       *draft,
        }),
        Event::PullRequestLinked { pull_request } => {
            EventBody::PullRequestLinked(fabro_types::PullRequestLinkedProps {
                pull_request: pull_request.clone(),
            })
        }
        Event::PullRequestUnlinked { pull_request } => {
            EventBody::PullRequestUnlinked(fabro_types::PullRequestUnlinkedProps {
                pull_request: pull_request.clone(),
            })
        }
        Event::PullRequestFailed { creation_id, error } => {
            EventBody::PullRequestFailed(fabro_types::PullRequestFailedProps {
                creation_id: *creation_id,
                error:       error.clone(),
            })
        }
    }
}

#[must_use]
pub fn to_run_event(run_id: &RunId, event: &Event) -> RunEvent {
    to_run_event_at(run_id, event, Utc::now(), None)
}

#[must_use]
pub fn to_run_event_at(
    run_id: &RunId,
    event: &Event,
    ts: chrono::DateTime<Utc>,
    scope: Option<&StageScope>,
) -> RunEvent {
    let fields = stored_event_fields(event, scope);
    let body = event_body_from_event(event);
    RunEvent {
        id: Uuid::now_v7().to_string(),
        ts,
        run_id: *run_id,
        node_id: fields.node_id,
        node_label: fields.node_label,
        stage_id: fields.stage_id,
        parallel_group_id: fields.parallel_group_id,
        parallel_branch_id: fields.parallel_branch_id,
        session_id: fields.session_id,
        parent_session_id: fields.parent_session_id,
        tool_call_id: fields.tool_call_id,
        actor: fields.actor,
        body,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ::fabro_types::{
        AutomationRef, EventBody, FailureReason, ParallelBranchId, Principal, RunNoticeCode,
        RunNoticeLevel, RunProvenance, StageId, SystemActorKind, fixtures,
        run_event as fabro_types, test_support,
    };
    use chrono::Utc;
    use lithos_llm::types::ReasoningOutput;
    use pebble_coding_agent::events::{CodingAgentEvent, CodingEvent, TokenUsage};

    use super::*;
    use crate::error::Error;
    use crate::event::test_support::user_principal;
    use crate::event::{Event, StageScope};
    use crate::outcome::FailureDetail;

    #[derive(Debug)]
    struct EventTestCause;

    impl std::fmt::Display for EventTestCause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("connection refused")
        }
    }

    impl std::error::Error for EventTestCause {}

    fn exec_tail() -> fabro_types::ExecOutputTail {
        fabro_types::ExecOutputTail {
            stdout:           Some("last stdout line".to_string()),
            stderr:           Some("last stderr line".to_string()),
            stdout_truncated: false,
            stderr_truncated: true,
        }
    }

    use crate::test_support::test_usage;

    #[test]
    fn run_event_stage_completed_places_node_fields_in_header() {
        let stored = to_run_event_at(
            &fixtures::RUN_2,
            &Event::StageCompleted {
                node_id: "plan".to_string(),
                name: "Plan".to_string(),
                index: 0,
                timing: ::fabro_types::StageTiming::wall_only(5000),
                status: "succeeded".to_string(),
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
            },
            Utc::now(),
            Some(&StageScope {
                node_id:            "plan".to_string(),
                visit:              1,
                parallel_group_id:  None,
                parallel_branch_id: None,
            }),
        );

        assert_eq!(stored.event_name(), "stage.completed");
        assert_eq!(stored.run_id, fixtures::RUN_2);
        assert_eq!(stored.node_id.as_deref(), Some("plan"));
        assert_eq!(stored.node_label.as_deref(), Some("Plan"));
        assert_eq!(stored.stage_id, Some(StageId::new("plan", 1)));
        let properties = stored.properties().unwrap();
        assert_eq!(properties["timing"]["wall_time_ms"], 5000);
        assert_eq!(properties["timing"]["active_time_ms"], 0);
        assert_eq!(properties["status"], "succeeded");
        assert!(stored.session_id.is_none());
    }

    #[test]
    fn run_event_stage_completed_keeps_response_and_signature_snapshots() {
        let stored = to_run_event(&fixtures::RUN_2, &Event::StageCompleted {
            node_id: "plan".to_string(),
            name: "Plan".to_string(),
            index: 0,
            timing: ::fabro_types::StageTiming::wall_only(5000),
            status: "succeeded".to_string(),
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
            loop_failure_signatures: Some(BTreeMap::from([("sig-a".to_string(), 2usize)])),
            restart_failure_signatures: Some(BTreeMap::from([("sig-b".to_string(), 1usize)])),
            response: Some("done".to_string()),
            attempt: 1,
            max_attempts: 1,
        });

        let properties = stored.properties().unwrap();
        assert_eq!(properties["response"], "done");
        assert_eq!(properties["loop_failure_signatures"]["sig-a"], 2);
        assert_eq!(properties["restart_failure_signatures"]["sig-b"], 1);
    }

    #[test]
    fn run_event_stage_failure_keeps_failure_detail() {
        let usage = test_usage("gpt-5.2", 321, 54);
        let stored = to_run_event(&fixtures::RUN_3, &Event::StageFailed {
            node_id:    "code".to_string(),
            name:       "Code".to_string(),
            index:      1,
            failure:    FailureDetail::new(
                "lint failed",
                crate::outcome::FailureCategory::Deterministic,
            ),
            will_retry: true,
            timing:     ::fabro_types::StageTiming::wall_only(5000),
            billing:    Some(usage.clone()),
            actor:      None,
        });

        assert_eq!(stored.event_name(), "stage.failed");
        let properties = stored.properties().unwrap();
        assert_eq!(properties["failure"]["message"], "lint failed");
        assert_eq!(properties["failure"]["category"], "deterministic");
        assert_eq!(properties["will_retry"], true);
        assert_eq!(properties["billing"], serde_json::to_value(&usage).unwrap());
    }

    #[test]
    fn run_event_agent_tools_available_moves_session_and_stage_metadata_to_header() {
        let stored = to_run_event(&fixtures::RUN_4, &Event::AgentToolsAvailable {
            node_id:    "code".to_string(),
            visit:      2,
            session_id: "ses_root".to_string(),
            tools:      vec![::fabro_types::ToolSummary {
                name:        "apply_patch".to_string(),
                description: "Apply a unified diff patch".to_string(),
                source:      ::fabro_types::ToolSource::Native,
                category:    ::fabro_types::ToolCategory::Write,
                invoked:     false,
            }],
        });

        assert_eq!(stored.event_name(), "agent.tools.available");
        assert_eq!(stored.node_id.as_deref(), Some("code"));
        assert_eq!(stored.stage_id, Some(StageId::new("code", 2)));
        assert_eq!(stored.session_id.as_deref(), Some("ses_root"));
        let properties = stored.properties().unwrap();
        assert_eq!(properties["visit"], 2);
        assert_eq!(properties["tools"][0]["name"], "apply_patch");
        assert_eq!(properties["tools"][0]["category"], "write");
    }

    #[test]
    fn run_event_sandbox_event_keeps_properties_nested() {
        let stored = to_run_event(&fixtures::RUN_5, &Event::Sandbox {
            event: SandboxLifecycle::Ready {
                provider:    "daytona".to_string(),
                duration_ms: 2500,
                name:        Some("sandbox-1".to_string()),
                url:         Some("https://example.test".to_string()),
            },
        });

        assert_eq!(stored.event_name(), "sandbox.ready");
        assert!(stored.node_id.is_none());
        let properties = stored.properties().unwrap();
        assert_eq!(properties["provider"], "daytona");
        assert_eq!(properties["duration_ms"], 2500);
    }

    #[test]
    fn run_event_driver_events_are_named_from_the_subject_action_and_phase() {
        let stopped = to_run_event(&fixtures::RUN_5, &Event::SandboxDriver {
            event: driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 1},
                "occurred_at": "2026-05-09T12:00:00Z",
                "provider": "docker",
                "subject": {"type": "sandbox", "id": "container-1"},
                "type": "operation_completed",
                "action": "stop",
                "duration": {"secs": 0, "nanos": 10_000_000}
            })),
        });
        let building = to_run_event(&fixtures::RUN_5, &Event::SandboxDriver {
            event: driver_event(serde_json::json!({
                "id": {"source_id": "test", "sequence": 2},
                "occurred_at": "2026-05-09T12:00:01Z",
                "provider": "daytona",
                "subject": {"type": "snapshot", "name": "sandbox-driver-abc"},
                "type": "operation_started",
                "action": "create"
            })),
        });

        assert_eq!(stopped.event_name(), "sandbox.stop.completed");
        assert_eq!(building.event_name(), "snapshot.create.started");
        let properties = stopped.properties().unwrap();
        assert_eq!(properties["action"], "stop");
        assert_eq!(properties["subject"]["id"], "container-1");
        assert_eq!(properties["duration"]["nanos"], 10_000_000);

        // The stored form reads back as the driver's event.
        let round_trip: RunEvent = serde_json::from_value(serde_json::to_value(&stopped).unwrap())
            .expect("a stored driver event decodes");
        assert!(matches!(
            &round_trip.body,
            EventBody::SandboxDriver { name, event }
                if name == "sandbox.stop.completed"
                    && matches!(event.body, sandbox_driver::EventBody::OperationCompleted { .. })
        ));
    }

    fn driver_event(value: serde_json::Value) -> sandbox_driver::Event {
        serde_json::from_value(value).expect("a driver event")
    }

    #[test]
    fn run_event_sandbox_failure_serializes_causes() {
        let stored = to_run_event(&fixtures::RUN_5, &Event::Sandbox {
            event: SandboxLifecycle::InitializeFailed {
                provider:    "docker".to_string(),
                error:       "Failed to pull Docker image buildpack-deps:noble".to_string(),
                causes:      vec!["connection refused".to_string()],
                duration_ms: 42,
            },
        });

        assert_eq!(stored.event_name(), "sandbox.failed");
        let properties = stored.properties().unwrap();
        assert_eq!(properties["provider"], "docker");
        assert_eq!(
            properties["error"],
            "Failed to pull Docker image buildpack-deps:noble"
        );
        assert_eq!(
            properties["causes"],
            serde_json::json!(["connection refused"])
        );
    }

    #[test]
    fn run_event_workflow_failure_uses_display_error() {
        let event = Event::workflow_run_failed_from_error(
            &Error::handler("boom"),
            ::fabro_types::RunTiming::wall_only(900),
            FailureReason::WorkflowError,
            Some("abc123".to_string()),
            None,
            None,
            None,
        );
        let stored = to_run_event(&fixtures::RUN_6, &event);

        assert_eq!(stored.event_name(), "run.failed");
        let properties = stored.properties().unwrap();
        assert_eq!(properties["failure"]["detail"]["message"], "boom");
        assert_eq!(properties["timing"]["wall_time_ms"], 900);
    }

    #[test]
    fn run_event_workflow_failure_serializes_causes() {
        let source = EventTestCause;
        let event = Event::workflow_run_failed_from_error(
            &Error::engine_with_source("Failed to initialize sandbox", source),
            ::fabro_types::RunTiming::wall_only(900),
            FailureReason::WorkflowError,
            None,
            None,
            None,
            None,
        );
        let stored = to_run_event(&fixtures::RUN_6, &event);

        let properties = stored.properties().unwrap();
        assert_eq!(
            properties["failure"]["detail"]["message"],
            "Failed to initialize sandbox"
        );
        assert_eq!(
            properties["failure"]["detail"]["causes"],
            serde_json::json!(["connection refused"])
        );
    }

    #[test]
    fn run_event_workflow_failure_projects_nested_failure_contract() {
        let source = EventTestCause;
        let event = Event::workflow_run_failed_from_error(
            &Error::engine_with_source("Failed to initialize sandbox", source),
            ::fabro_types::RunTiming::wall_only(900),
            FailureReason::SandboxInitFailed,
            Some("abc123".to_string()),
            None,
            None,
            None,
        );
        let stored = to_run_event(&fixtures::RUN_6, &event);

        assert_eq!(stored.event_name(), "run.failed");
        let properties = stored.properties().unwrap();
        assert_eq!(
            properties["failure"]["detail"]["message"],
            "Failed to initialize sandbox"
        );
        assert_eq!(
            properties["failure"]["detail"]["causes"],
            serde_json::json!(["connection refused"])
        );
        assert_eq!(properties["failure"]["reason"], "sandbox_init_failed");
        assert_eq!(
            properties["failure"]["detail"]["category"],
            "transient_infra"
        );
        assert_eq!(properties["timing"]["wall_time_ms"], 900);
        assert_eq!(properties["final_git_commit_sha"], "abc123");
        assert!(properties.get("error").is_none());
        assert!(properties.get("causes").is_none());
        assert!(properties.get("reason").is_none());
        assert!(properties.get("git_commit_sha").is_none());
    }

    #[test]
    fn stage_started_populates_parallel_ids_when_present() {
        let stored = to_run_event_at(
            &fixtures::RUN_1,
            &Event::StageStarted {
                graph_visit:           None,
                resumed_from_stage_id: None,
                node_id:               "review".to_string(),
                name:                  "review".to_string(),
                index:                 1,
                handler_type:          "agent".to_string(),
                attempt:               1,
                max_attempts:          1,
            },
            Utc::now(),
            Some(&StageScope {
                node_id:            "review".to_string(),
                visit:              1,
                parallel_group_id:  Some(StageId::new("fanout", 2)),
                parallel_branch_id: Some(ParallelBranchId::new(StageId::new("fanout", 2), 1)),
            }),
        );
        assert_eq!(stored.parallel_group_id, Some(StageId::new("fanout", 2)));
        assert_eq!(
            stored.parallel_branch_id,
            Some(ParallelBranchId::new(StageId::new("fanout", 2), 1))
        );
    }

    #[test]
    fn parallel_started_populates_group_id_and_public_properties() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::ParallelStarted {
            node_id:      "fanout".to_string(),
            visit:        2,
            branch_count: 3,
        });
        assert_eq!(stored.parallel_group_id, Some(StageId::new("fanout", 2)));
        assert!(stored.parallel_branch_id.is_none());
        assert_eq!(
            stored.properties().unwrap(),
            serde_json::json!({
                "visit": 2,
                "branch_count": 3,
            })
        );
    }

    #[test]
    fn parallel_branch_completed_public_properties() {
        let group_id = StageId::new("fanout", 2);
        let stored = to_run_event(&fixtures::RUN_1, &Event::ParallelBranchCompleted {
            parallel_group_id:  group_id.clone(),
            parallel_branch_id: ParallelBranchId::new(group_id, 1),
            branch:             "review".to_string(),
            index:              1,
            item_label:         Some("api".to_string()),
            duration_ms:        42,
            status:             StageOutcome::Succeeded,
        });

        assert_eq!(
            stored.properties().unwrap(),
            serde_json::json!({
                "index": 1,
                "item_label": "api",
                "duration_ms": 42,
                "status": "succeeded",
            })
        );
    }

    #[test]
    fn parallel_completed_exposes_typed_results_in_input_order() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::ParallelCompleted {
            node_id:       "fanout".to_string(),
            visit:         2,
            duration_ms:   84,
            success_count: 1,
            failure_count: 1,
            results:       vec![
                ::fabro_types::ParallelBranchResult {
                    id:              "review_api".to_string(),
                    index:           Some(0),
                    item_label:      Some("api".to_string()),
                    status:          StageOutcome::Succeeded,
                    context_updates: BTreeMap::from([(
                        "response.review_api".to_string(),
                        serde_json::json!("looks good"),
                    )]),
                },
                ::fabro_types::ParallelBranchResult {
                    id:              "review_ux".to_string(),
                    index:           Some(1),
                    item_label:      Some("ux".to_string()),
                    status:          StageOutcome::Failed {
                        retry_requested: false,
                    },
                    context_updates: BTreeMap::from([(
                        "response.review_ux".to_string(),
                        serde_json::json!("needs work"),
                    )]),
                },
            ],
        });

        assert_eq!(
            stored.properties().unwrap(),
            serde_json::json!({
                "visit": 2,
                "duration_ms": 84,
                "success_count": 1,
                "failure_count": 1,
                "results": [
                    {
                        "id": "review_api",
                        "index": 0,
                        "item_label": "api",
                        "status": "succeeded",
                        "context_updates": {"response.review_api": "looks good"},
                    },
                    {
                        "id": "review_ux",
                        "index": 1,
                        "item_label": "ux",
                        "status": "failed",
                        "context_updates": {"response.review_ux": "needs work"},
                    },
                ],
            })
        );
    }

    #[test]
    fn parallel_branch_started_populates_group_and_branch_ids() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::ParallelBranchStarted {
            graph_visit:           None,
            resumed_from_stage_id: None,
            parallel_group_id:     StageId::new("fanout", 2),
            parallel_branch_id:    ParallelBranchId::new(StageId::new("fanout", 2), 1),
            branch:                "review".to_string(),
            index:                 1,
            item_label:            Some("api".to_string()),
        });
        assert_eq!(stored.parallel_group_id, Some(StageId::new("fanout", 2)));
        assert_eq!(
            stored.parallel_branch_id,
            Some(ParallelBranchId::new(StageId::new("fanout", 2), 1))
        );
    }

    #[test]
    fn agent_interrupt_injected_populates_stage_session_and_actor() {
        let actor = Principal::System {
            system_kind: SystemActorKind::Engine,
        };
        let stored = to_run_event(&fixtures::RUN_1, &Event::AgentInterruptInjected {
            node_id:    "code".to_string(),
            visit:      3,
            session_id: "ses_1".to_string(),
            actor:      Some(actor.clone()),
        });

        assert_eq!(stored.event_name(), "agent.interrupt.injected");
        assert_eq!(stored.node_id.as_deref(), Some("code"));
        assert_eq!(stored.node_label.as_deref(), Some("code"));
        assert_eq!(stored.stage_id, Some(StageId::new("code", 3)));
        assert_eq!(stored.session_id.as_deref(), Some("ses_1"));
        assert_eq!(stored.actor, Some(actor));
        match stored.body {
            EventBody::AgentInterruptInjected(props) => assert_eq!(props.visit, 3),
            other => panic!("unexpected body: {other:?}"),
        }
    }

    #[test]
    fn stage_scope_populates_stage_id_on_non_stage_events() {
        // Events tied to a concrete stage execution but lacking scope in their
        // own variant fields (CheckpointCompleted, CommandStarted, PromptCompleted,
        // Prompt, InterviewStarted, Failover, GitCommit) should pick up stage_id
        // / parallel_group_id / parallel_branch_id from the scope argument.
        let scope = StageScope {
            node_id:            "build".to_string(),
            visit:              2,
            parallel_group_id:  Some(StageId::new("fanout", 1)),
            parallel_branch_id: Some(ParallelBranchId::new(StageId::new("fanout", 1), 0)),
        };

        let command_started = to_run_event_at(
            &fixtures::RUN_1,
            &Event::CommandStarted {
                node_id:    "build".to_string(),
                script:     "echo".to_string(),
                command:    "echo".to_string(),
                language:   "shell".to_string(),
                timeout_ms: None,
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(command_started.stage_id, Some(StageId::new("build", 2)));
        assert_eq!(command_started.parallel_group_id, scope.parallel_group_id);
        assert_eq!(command_started.parallel_branch_id, scope.parallel_branch_id);

        let prompt = to_run_event_at(
            &fixtures::RUN_1,
            &Event::Prompt {
                stage:            "build".to_string(),
                visit:            2,
                text:             "do it".to_string(),
                mode:             None,
                provider:         None,
                model:            None,
                reasoning_effort: None,
                speed:            None,
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(prompt.stage_id, Some(StageId::new("build", 2)));

        let git_commit = to_run_event_at(
            &fixtures::RUN_1,
            &Event::GitCommit {
                node_id: Some("build".to_string()),
                sha:     "deadbeef".to_string(),
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(git_commit.stage_id, Some(StageId::new("build", 2)));
    }

    #[test]
    fn run_level_events_without_scope_leave_stage_id_absent() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::RunRunning);
        assert!(stored.stage_id.is_none());
        assert!(stored.parallel_group_id.is_none());
        assert!(stored.parallel_branch_id.is_none());
    }

    #[test]
    fn control_action_events_carry_actor_in_envelope() {
        let actor = user_principal("alice");

        let cancel = to_run_event(&fixtures::RUN_1, &Event::RunCancelRequested {
            actor: Some(actor.clone()),
        });
        assert_eq!(cancel.event_name(), "run.cancel.requested");
        assert_eq!(cancel.actor.as_ref().expect("actor set"), &actor);

        let pause = to_run_event(&fixtures::RUN_1, &Event::RunPauseRequested {
            actor: Some(actor.clone()),
        });
        assert_eq!(pause.actor.as_ref().expect("actor set"), &actor);

        let unpause = to_run_event(&fixtures::RUN_1, &Event::RunUnpauseRequested {
            actor: None,
        });
        assert!(unpause.actor.is_none());
    }

    #[test]
    fn run_archived_round_trips_actor_in_envelope() {
        let actor = user_principal("alice");

        let archived = to_run_event(&fixtures::RUN_1, &Event::RunArchived {
            actor: Some(actor.clone()),
        });
        assert_eq!(archived.event_name(), "run.archived");
        assert_eq!(archived.actor.as_ref().expect("actor set"), &actor);
        assert!(matches!(archived.body, EventBody::RunArchived(_)));
    }

    #[test]
    fn run_unarchived_round_trips_actor_in_envelope() {
        let actor = user_principal("bob");

        let unarchived = to_run_event(&fixtures::RUN_1, &Event::RunUnarchived {
            actor: Some(actor.clone()),
        });
        assert_eq!(unarchived.event_name(), "run.unarchived");
        assert_eq!(unarchived.actor.as_ref().expect("actor set"), &actor);
        match &unarchived.body {
            EventBody::RunUnarchived(_) => {}
            other => panic!("expected RunUnarchived body, got {other:?}"),
        }
    }

    #[test]
    fn run_notice_maps_exec_output_tail_to_props() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::RunNotice {
            level:            RunNoticeLevel::Warn,
            code:             RunNoticeCode::GitDiffFailed.to_string(),
            message:          "git diff failed".to_string(),
            exec_output_tail: Some(exec_tail()),
        });

        match stored.body {
            EventBody::RunNotice(props) => {
                let tail = props.exec_output_tail.expect("exec output tail");
                assert_eq!(tail.stderr.as_deref(), Some("last stderr line"));
                assert!(tail.stderr_truncated);
            }
            other => panic!("expected RunNotice body, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_failed_maps_exec_output_tail_to_props() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::CheckpointFailed {
            node_id:          "build".to_string(),
            error:            "git commit failed".to_string(),
            exec_output_tail: Some(exec_tail()),
        });

        match stored.body {
            EventBody::CheckpointFailed(props) => {
                let tail = props.exec_output_tail.expect("exec output tail");
                assert_eq!(tail.stdout.as_deref(), Some("last stdout line"));
                assert!(!tail.stdout_truncated);
            }
            other => panic!("expected CheckpointFailed body, got {other:?}"),
        }
    }

    /// The `git.push` attempts contract: every runtime attempt fact
    /// round-trips through `GitPushAttemptProps`, the token snapshot is
    /// flattened to the three flat token fields (a nested provenance enum
    /// never appears in stored events), and optional failure fields are
    /// omitted when absent.
    #[test]
    fn git_push_attempts_round_trip_through_the_durable_shape() {
        let started_at = Utc::now();
        let minted_at = started_at - chrono::Duration::milliseconds(180);
        let expires_at = started_at + chrono::Duration::minutes(60);
        let runtime_attempts = vec![
            fabro_sandbox::PushAttempt {
                attempt: 1,
                started_at,
                success: false,
                retry_reason: Some(fabro_sandbox::GitRetryReason::TokenReplication),
                exec_output_tail: Some(exec_tail()),
                token: Some(fabro_sandbox::TokenSnapshot {
                    generation: 14,
                    provenance: fabro_sandbox::TokenProvenance::Minted {
                        minted_at,
                        expires_at,
                    },
                }),
            },
            // Terminal classified failure with a refresh error: the last
            // attempt carries its classification too.
            fabro_sandbox::PushAttempt {
                attempt:          2,
                started_at:       started_at + chrono::Duration::seconds(3),
                success:          false,
                retry_reason:     Some(fabro_sandbox::GitRetryReason::TransientInfra),
                exec_output_tail: Some(exec_tail()),
                token:            Some(fabro_sandbox::TokenSnapshot {
                    generation: 14,
                    provenance: fabro_sandbox::TokenProvenance::Reused {
                        minted_at,
                        expires_at,
                    },
                }),
            },
        ];
        let expected_attempts = git_push_attempt_props(&runtime_attempts);

        let stored = to_run_event(&fixtures::RUN_1, &Event::GitPush {
            branch:           "fabro/run/01M0DH033P2XSTHAGVBHG6922F".to_string(),
            success:          false,
            exec_output_tail: Some(exec_tail()),
            attempts:         runtime_attempts,
        });

        let json = serde_json::to_value(&stored).unwrap();
        let serialized = &json["properties"]["attempts"];
        assert_eq!(serialized[0]["attempt"], 1);
        assert_eq!(serialized[0]["classified_reason"], "token_replication");
        assert_eq!(serialized[0]["token_generation"], 14);
        assert_eq!(serialized[0]["token_provenance"], "minted");
        assert_eq!(serialized[0]["token_age_ms"], 180);
        assert_eq!(serialized[1]["classified_reason"], "transient_infra");
        assert_eq!(serialized[1]["token_provenance"], "reused");
        // The provenance enum never nests in stored events.
        assert!(serialized[0].get("token").is_none());

        let round_tripped: ::fabro_types::RunEvent = serde_json::from_value(json).unwrap();
        match round_tripped.body {
            EventBody::GitPush(props) => {
                assert!(!props.success);
                assert_eq!(props.attempts, expected_attempts);
            }
            other => panic!("expected GitPush body, got {other:?}"),
        }
    }

    /// Attempts stored by earlier releases carried `credential_action` and
    /// `refresh_error` from the origin-URL credential design. The fields are
    /// gone; the stored events still read.
    #[test]
    fn stored_attempts_with_retired_credential_fields_still_deserialize() {
        let json = serde_json::json!({
            "attempt": 1,
            "started_at": "2026-03-30T12:00:01.000Z",
            "success": true,
            "token_generation": 3,
            "token_provenance": "reused",
            "token_age_ms": 120,
            "credential_action": "embedded",
            "refresh_error": "set_url"
        });
        let props: ::fabro_types::run_event::GitPushAttemptProps =
            serde_json::from_value(json).unwrap();
        assert_eq!(props.attempt, 1);
        assert_eq!(props.token_generation, Some(3));
        assert_eq!(
            props.token_provenance,
            Some(::fabro_types::run_event::GitTokenProvenance::Reused)
        );
    }

    #[test]
    fn successful_single_attempt_push_omits_failure_fields() {
        let attempts = vec![fabro_sandbox::PushAttempt {
            attempt:          1,
            started_at:       Utc::now(),
            success:          true,
            retry_reason:     None,
            exec_output_tail: None,
            token:            Some(fabro_sandbox::TokenSnapshot {
                generation: 0,
                provenance: fabro_sandbox::TokenProvenance::Static,
            }),
        }];
        let stored = to_run_event(&fixtures::RUN_1, &Event::GitPush {
            branch: "fabro/run/run-1".to_string(),
            success: true,
            exec_output_tail: None,
            attempts,
        });

        let json = serde_json::to_value(&stored).unwrap();
        let attempt = &json["properties"]["attempts"][0];
        assert_eq!(attempt["success"], true);
        assert_eq!(attempt["token_provenance"], "static");
        for absent in ["classified_reason", "exec_output_tail", "token_age_ms"] {
            assert!(attempt.get(absent).is_none(), "{absent} should be omitted");
        }
    }

    /// Events stored before attempts were recorded deserialize with the field
    /// absent; the pre-existing three fields are untouched.
    #[test]
    fn stored_git_push_without_attempts_still_deserializes() {
        let json = serde_json::json!({
            "branch": "fabro/run/old",
            "success": true
        });
        let props: fabro_types::GitPushProps = serde_json::from_value(json).unwrap();
        assert!(props.attempts.is_empty());
        assert!(props.exec_output_tail.is_none());
    }

    #[test]
    fn git_push_maps_exec_output_tail_to_props() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::GitPush {
            branch:           "refs/heads/run:refs/heads/run".to_string(),
            success:          false,
            exec_output_tail: Some(exec_tail()),
            attempts:         Vec::new(),
        });

        match stored.body {
            EventBody::GitPush(props) => {
                assert!(!props.success);
                let tail = props.exec_output_tail.expect("exec output tail");
                assert_eq!(tail.stderr.as_deref(), Some("last stderr line"));
            }
            other => panic!("expected GitPush body, got {other:?}"),
        }
    }

    #[test]
    fn metadata_snapshot_events_map_to_typed_bodies() {
        let started = to_run_event(&fixtures::RUN_1, &Event::MetadataSnapshotStarted {
            phase:  fabro_types::MetadataSnapshotPhase::Init,
            branch: "fabro/metadata/run".to_string(),
        });

        assert_eq!(started.event_name(), "metadata.snapshot.started");
        assert!(started.node_id.is_none());
        assert!(started.stage_id.is_none());
        match started.body {
            EventBody::MetadataSnapshotStarted(props) => {
                assert_eq!(props.phase, fabro_types::MetadataSnapshotPhase::Init);
                assert_eq!(props.branch, "fabro/metadata/run");
            }
            other => panic!("expected MetadataSnapshotStarted body, got {other:?}"),
        }

        let completed = to_run_event(&fixtures::RUN_1, &Event::MetadataSnapshotCompleted {
            phase:       fabro_types::MetadataSnapshotPhase::Finalize,
            branch:      "fabro/metadata/run".to_string(),
            duration_ms: 2400,
            entry_count: 4,
            bytes:       512,
            commit_sha:  "abc123".to_string(),
        });

        assert_eq!(completed.event_name(), "metadata.snapshot.completed");
        match completed.body {
            EventBody::MetadataSnapshotCompleted(props) => {
                assert_eq!(props.phase, fabro_types::MetadataSnapshotPhase::Finalize);
                assert_eq!(props.duration_ms, 2400);
                assert_eq!(props.entry_count, 4);
                assert_eq!(props.bytes, 512);
                assert_eq!(props.commit_sha, "abc123");
            }
            other => panic!("expected MetadataSnapshotCompleted body, got {other:?}"),
        }

        let failed = to_run_event(&fixtures::RUN_1, &Event::MetadataSnapshotFailed {
            phase:            fabro_types::MetadataSnapshotPhase::Checkpoint,
            branch:           "fabro/metadata/run".to_string(),
            duration_ms:      120,
            failure_kind:     fabro_types::MetadataSnapshotFailureKind::Push,
            error:            "push rejected".to_string(),
            causes:           vec!["permission denied".to_string()],
            commit_sha:       Some("def456".to_string()),
            entry_count:      Some(4),
            bytes:            Some(512),
            exec_output_tail: Some(fabro_types::ExecOutputTail {
                stdout:           Some("last stdout line".to_string()),
                stderr:           Some("last stderr line".to_string()),
                stdout_truncated: false,
                stderr_truncated: true,
            }),
        });

        assert_eq!(failed.event_name(), "metadata.snapshot.failed");
        match failed.body {
            EventBody::MetadataSnapshotFailed(props) => {
                assert_eq!(
                    props.failure_kind,
                    fabro_types::MetadataSnapshotFailureKind::Push
                );
                assert_eq!(props.commit_sha.as_deref(), Some("def456"));
                assert_eq!(props.entry_count, Some(4));
                assert_eq!(props.bytes, Some(512));
                let tail = props.exec_output_tail.expect("exec output tail");
                assert_eq!(tail.stdout.as_deref(), Some("last stdout line"));
                assert_eq!(tail.stderr.as_deref(), Some("last stderr line"));
                assert!(tail.stderr_truncated);
                assert!(!tail.stdout_truncated);
            }
            other => panic!("expected MetadataSnapshotFailed body, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_metadata_snapshot_events_can_be_stage_scoped() {
        let scope = StageScope {
            node_id:            "build".to_string(),
            visit:              2,
            parallel_group_id:  Some(StageId::new("fanout", 1)),
            parallel_branch_id: Some(ParallelBranchId::new(StageId::new("fanout", 1), 0)),
        };
        let stored = to_run_event_at(
            &fixtures::RUN_1,
            &Event::MetadataSnapshotStarted {
                phase:  fabro_types::MetadataSnapshotPhase::Checkpoint,
                branch: "fabro/metadata/run".to_string(),
            },
            Utc::now(),
            Some(&scope),
        );

        assert_eq!(stored.node_id.as_deref(), Some("build"));
        assert_eq!(stored.node_label.as_deref(), Some("build"));
        assert_eq!(stored.stage_id, Some(StageId::new("build", 2)));
        assert_eq!(stored.parallel_group_id, scope.parallel_group_id);
        assert_eq!(stored.parallel_branch_id, scope.parallel_branch_id);
    }

    #[test]
    fn agent_acp_events_map_to_event_bodies_with_stage_scope() {
        let scope = StageScope {
            node_id:            "code".to_string(),
            visit:              2,
            parallel_group_id:  Some(StageId::new("fanout", 1)),
            parallel_branch_id: Some(ParallelBranchId::new(StageId::new("fanout", 1), 0)),
        };

        let started = to_run_event_at(
            &fixtures::RUN_1,
            &Event::AgentAcpStarted {
                node_id:     "code".to_string(),
                visit:       2,
                command:     "python fake_agent.py".to_string(),
                config_name: Some("fake".to_string()),
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(started.event_name(), "agent.acp.started");
        assert_eq!(started.node_id.as_deref(), Some("code"));
        assert_eq!(started.stage_id, Some(StageId::new("code", 2)));
        assert_eq!(started.parallel_group_id, scope.parallel_group_id);
        assert_eq!(started.parallel_branch_id, scope.parallel_branch_id);
        match &started.body {
            EventBody::AgentAcpStarted(props) => {
                assert_eq!(props.visit, 2);
                assert_eq!(props.command, "python fake_agent.py");
                assert_eq!(props.config_name.as_deref(), Some("fake"));
            }
            other => panic!("expected AgentAcpStarted, got {other:?}"),
        }

        let completed = to_run_event_at(
            &fixtures::RUN_1,
            &Event::AgentAcpCompleted {
                node_id:     "code".to_string(),
                stdout:      "done".to_string(),
                stderr:      "warn".to_string(),
                stop_reason: "end_turn".to_string(),
                duration_ms: 42,
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(completed.event_name(), "agent.acp.completed");
        match &completed.body {
            EventBody::AgentAcpCompleted(props) => {
                assert_eq!(props.stdout, "done");
                assert_eq!(props.stderr, "warn");
                assert_eq!(props.stop_reason, "end_turn");
                assert_eq!(props.duration_ms, 42);
            }
            other => panic!("expected AgentAcpCompleted, got {other:?}"),
        }

        let cancelled = to_run_event_at(
            &fixtures::RUN_1,
            &Event::AgentAcpCancelled {
                node_id:     "code".to_string(),
                stdout:      "partial".to_string(),
                stderr:      "cancelled".to_string(),
                duration_ms: 7,
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(cancelled.event_name(), "agent.acp.cancelled");
        assert_eq!(cancelled.stage_id, Some(StageId::new("code", 2)));
        assert!(matches!(
            cancelled.body,
            EventBody::AgentAcpCancelled(fabro_types::AgentAcpCancelledProps {
                duration_ms: 7,
                ..
            })
        ));

        let timed_out = to_run_event_at(
            &fixtures::RUN_1,
            &Event::AgentAcpTimedOut {
                node_id:     "code".to_string(),
                stdout:      "partial".to_string(),
                stderr:      "timeout".to_string(),
                duration_ms: 99,
            },
            Utc::now(),
            Some(&scope),
        );
        assert_eq!(timed_out.event_name(), "agent.acp.timed_out");
        assert_eq!(timed_out.stage_id, Some(StageId::new("code", 2)));
        assert!(matches!(
            timed_out.body,
            EventBody::AgentAcpTimedOut(fabro_types::AgentAcpTimedOutProps {
                duration_ms: 99,
                ..
            })
        ));
    }

    #[test]
    fn stall_watchdog_timeout_populates_watchdog_actor() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::StallWatchdogTimeout {
            node:         "code".to_string(),
            idle_seconds: 60,
        });

        assert_eq!(stored.event_name(), "watchdog.timeout");
        assert_eq!(stored.node_id.as_deref(), Some("code"));
        assert_eq!(
            stored.actor,
            Some(Principal::System {
                system_kind: SystemActorKind::Watchdog,
            })
        );
    }

    #[test]
    fn run_created_populates_user_actor_from_provenance() {
        use ::fabro_types::{Graph, WorkflowSettings, fixtures};

        let provenance = RunProvenance {
            server:  None,
            client:  None,
            subject: user_principal("alice"),
        };
        let automation = AutomationRef {
            id:              "nightly".to_string(),
            name:            Some("Nightly".to_string()),
            trigger_id:      Some("schedule_1".to_string()),
            workflow_source: None,
        };
        let workflow_version_id = test_support::test_workflow_version_id();

        let stored = to_run_event(&fixtures::RUN_1, &Event::RunCreated {
            run_id: fixtures::RUN_1,
            title: None,
            settings: serde_json::to_value(WorkflowSettings::default()).unwrap(),
            graph: serde_json::to_value(Graph::new("test")).unwrap(),
            workflow_source: None,
            labels: BTreeMap::default(),
            source_directory: Some("/tmp/run".to_string()),
            workflow_slug: None,
            workflow_version_id: Some(workflow_version_id),
            target: None,
            automation: Some(automation.clone()),
            provenance,
            manifest_blob: None,
            spec_blob: None,
            git: None,
            fork_source_ref: None,
            retried_from: None,
            parent_id: None,
            web_url: None,
        });
        let actor = stored.actor.as_ref().expect("actor set");
        assert_eq!(actor, &user_principal("alice"));
        let EventBody::RunCreated(props) = stored.body else {
            panic!("expected run.created body");
        };
        assert_eq!(props.automation, Some(automation));
        assert_eq!(props.workflow_version_id, Some(workflow_version_id));
    }

    fn agent_event(session_id: &str, event: CodingEvent) -> CodingAgentEvent {
        CodingAgentEvent::new(session_id, event, std::time::SystemTime::UNIX_EPOCH)
    }

    #[test]
    fn run_event_agent_tool_started_moves_session_metadata_to_header() {
        let stored = to_run_event(&fixtures::RUN_4, &Event::Agent {
            stage: "code".to_string(),
            visit: 2,
            event: agent_event("ses_child", CodingEvent::ToolCallStarted {
                tool_name:    "read_file".to_string(),
                tool_call_id: "call_1".to_string(),
                arguments:    serde_json::json!({"path": "src/main.rs"}),
            })
            .with_parent_session_id("ses_parent"),
        });

        assert_eq!(stored.event_name(), "agent.tool.started");
        assert_eq!(stored.node_id.as_deref(), Some("code"));
        assert_eq!(stored.node_label.as_deref(), Some("code"));
        assert_eq!(stored.stage_id, Some(StageId::new("code", 2)));
        assert_eq!(stored.session_id.as_deref(), Some("ses_child"));
        assert_eq!(stored.parent_session_id.as_deref(), Some("ses_parent"));
        assert_eq!(stored.tool_call_id.as_deref(), Some("call_1"));
        let properties = stored.properties().unwrap();
        assert_eq!(properties["visit"], 2);
        assert_eq!(properties["stage"], "code");
        assert_eq!(
            properties["event"]["ToolCallStarted"]["tool_name"],
            "read_file"
        );
        assert_eq!(
            properties["event"]["ToolCallStarted"]["tool_call_id"],
            "call_1"
        );
    }

    #[test]
    fn run_event_agent_tool_process_completed_carries_stage_session_and_actor() {
        let stored = to_run_event_at(
            &fixtures::RUN_4,
            &Event::Agent {
                stage: "code".to_string(),
                visit: 2,
                event: agent_event("ses_child", CodingEvent::ToolProcessCompleted {
                    exit_code:             Some(7),
                    termination:           ::fabro_types::CommandTermination::Exited,
                    duration_ms:           12,
                    streams_separated:     true,
                    output_bytes_observed: 120,
                    output_bytes_retained: 100,
                    output_bytes_omitted:  20,
                    exec_output_tail:      Some(exec_tail()),
                })
                .with_parent_session_id("ses_parent")
                .with_tool_call_id("call_1"),
            },
            Utc::now(),
            Some(&StageScope {
                node_id:            "code".to_string(),
                visit:              2,
                parallel_group_id:  Some(StageId::new("fanout", 2)),
                parallel_branch_id: Some(ParallelBranchId::new(StageId::new("fanout", 2), 1)),
            }),
        );

        assert_eq!(stored.event_name(), "agent.tool.process.completed");
        assert_eq!(stored.stage_id, Some(StageId::new("code", 2)));
        assert_eq!(stored.session_id.as_deref(), Some("ses_child"));
        assert_eq!(stored.parent_session_id.as_deref(), Some("ses_parent"));
        assert_eq!(stored.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(
            stored.actor,
            Some(::fabro_types::Principal::Agent {
                session_id:        Some("ses_child".to_string()),
                parent_session_id: Some("ses_parent".to_string()),
                model:             None,
            })
        );
        let properties = stored.properties().unwrap();
        let event = &properties["event"]["ToolProcessCompleted"];
        assert_eq!(event["exit_code"], 7);
        assert_eq!(event["termination"], "exited");
        assert_eq!(event["exec_output_tail"]["stdout"], "last stdout line");
        assert_eq!(
            stored.parallel_branch_id,
            Some(ParallelBranchId::new(StageId::new("fanout", 2), 1))
        );
    }

    #[test]
    fn agent_round_interrupted_populates_stage_and_session() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::Agent {
            stage: "code".to_string(),
            visit: 3,
            event: agent_event("ses_1", CodingEvent::RoundInterrupted { generation: 2 }),
        });

        assert_eq!(stored.event_name(), "agent.round.interrupted");
        assert_eq!(stored.stage_id, Some(StageId::new("code", 3)));
        assert_eq!(stored.session_id.as_deref(), Some("ses_1"));
        match stored.body {
            EventBody::Agent(props) => {
                assert_eq!(props.visit, 3);
                assert!(matches!(
                    props.coding_event(),
                    CodingEvent::RoundInterrupted { generation: 2 }
                ));
            }
            other => panic!("unexpected body: {other:?}"),
        }
    }

    #[test]
    fn agent_todo_event_uses_the_todo_event_name() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::Agent {
            stage: "code".to_string(),
            visit: 1,
            event: agent_event(
                "ses_1",
                CodingEvent::TodoCreated(::fabro_types::TodoCreatedProps {
                    list_id:     "openai_plan:ses_1".to_string(),
                    list_kind:   ::fabro_types::TodoListKind::OpenAiPlan,
                    todo_id:     "todo_1".to_string(),
                    status:      ::fabro_types::TodoStatus::Pending,
                    order:       0,
                    subject:     "step".to_string(),
                    description: String::new(),
                    active_form: None,
                    owner:       None,
                    blocks:      Vec::new(),
                    blocked_by:  Vec::new(),
                    metadata:    BTreeMap::new(),
                }),
            )
            .with_tool_call_id("call_todo"),
        });

        assert_eq!(stored.event_name(), "todo.created");
        assert_eq!(stored.session_id.as_deref(), Some("ses_1"));
        assert_eq!(stored.tool_call_id.as_deref(), Some("call_todo"));
        assert!(matches!(stored.body, EventBody::Agent(_)));
    }

    #[test]
    fn agent_assistant_message_populates_agent_actor_with_model() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::Agent {
            stage: "code".to_string(),
            visit: 1,
            event: agent_event("ses_agent", CodingEvent::AssistantMessage {
                text:            "ok".to_string(),
                model:           "claude-sonnet".to_string(),
                usage:           TokenUsage::default(),
                cost_usd_micros: None,
                cost_source:     None,
                tool_call_count: 0,
                context_window:  None,
                reasoning:       None,
            }),
        });

        assert_eq!(stored.event_name(), "agent.message");
        let actor = stored.actor.as_ref().expect("actor set");
        assert_eq!(actor, &Principal::Agent {
            session_id:        Some("ses_agent".to_string()),
            parent_session_id: None,
            model:             Some("claude-sonnet".to_string()),
        });
    }

    #[test]
    fn agent_assistant_message_round_trips_reasoning_through_the_stored_payload() {
        let stored = to_run_event(&fixtures::RUN_1, &Event::Agent {
            stage: "code".to_string(),
            visit: 1,
            event: agent_event("ses_agent", CodingEvent::AssistantMessage {
                text:            String::new(),
                model:           "gpt-5.4".to_string(),
                usage:           TokenUsage::default(),
                cost_usd_micros: Some(125_000),
                cost_source:     Some(pebble_coding_agent::events::CostSource::Provider),
                tool_call_count: 1,
                context_window:  None,
                reasoning:       Some(ReasoningOutput::new(
                    "inspect the conversion first",
                    "read convert.rs, then the sink",
                )),
            }),
        });

        let value = stored.to_value().unwrap();
        assert_eq!(value["event"], "agent.message");
        let message = &value["properties"]["event"]["AssistantMessage"];
        assert_eq!(message["cost_usd_micros"], 125_000);
        assert_eq!(
            message["reasoning"]["summary"],
            "inspect the conversion first"
        );
        assert_eq!(
            message["reasoning"]["trace"],
            "read convert.rs, then the sink"
        );

        let decoded = RunEvent::from_value(value).unwrap();
        let EventBody::Agent(props) = decoded.body else {
            panic!("expected agent body");
        };
        assert!(matches!(
            props.coding_event(),
            CodingEvent::AssistantMessage {
                tool_call_count: 1,
                ..
            }
        ));
    }
}
