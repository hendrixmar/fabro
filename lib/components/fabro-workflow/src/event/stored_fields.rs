use ::fabro_types::{ParallelBranchId, Principal, StageId, SystemActorKind};
use pebble_coding_agent::events::{Actor, CodingEvent};

use super::Event;
use crate::stage_scope::StageScope;

#[derive(Debug, Default)]
pub(super) struct StoredEventFields {
    pub(super) session_id:         Option<String>,
    pub(super) parent_session_id:  Option<String>,
    pub(super) node_id:            Option<String>,
    pub(super) node_label:         Option<String>,
    pub(super) stage_id:           Option<StageId>,
    pub(super) parallel_group_id:  Option<StageId>,
    pub(super) parallel_branch_id: Option<ParallelBranchId>,
    pub(super) tool_call_id:       Option<String>,
    pub(super) actor:              Option<Principal>,
}

fn default_node_label(node_id: Option<&String>, node_label: Option<String>) -> Option<String> {
    node_label.or_else(|| node_id.cloned())
}

fn node_stored_fields(node_id: Option<String>) -> StoredEventFields {
    let node_label = default_node_label(node_id.as_ref(), None);
    StoredEventFields {
        node_id,
        node_label,
        ..StoredEventFields::default()
    }
}

pub(super) fn stored_event_fields(event: &Event, scope: Option<&StageScope>) -> StoredEventFields {
    let mut fields = stored_event_fields_for_variant(event);
    if let Some(scope) = scope {
        if fields.node_id.is_none() {
            fields.node_id = Some(scope.node_id.clone());
            fields.node_label = default_node_label(Some(&scope.node_id), fields.node_label);
        }
        if fields.stage_id.is_none() {
            fields.stage_id = Some(StageId::new(scope.node_id.clone(), scope.visit));
        }
        if fields.parallel_group_id.is_none() {
            fields
                .parallel_group_id
                .clone_from(&scope.parallel_group_id);
        }
        if fields.parallel_branch_id.is_none() {
            fields
                .parallel_branch_id
                .clone_from(&scope.parallel_branch_id);
        }
    }
    fields
}

fn stored_event_fields_for_variant(event: &Event) -> StoredEventFields {
    match event {
        Event::RunCreated { provenance, .. } => StoredEventFields {
            actor: Some(provenance.subject.clone()),
            ..StoredEventFields::default()
        },
        Event::RunCancelRequested { actor }
        | Event::RunStartRequested { actor, .. }
        | Event::RunPending { actor, .. }
        | Event::RunApproved { actor }
        | Event::RunDenied { actor, .. }
        | Event::RunRunnable { actor, .. }
        | Event::RunPauseRequested { actor }
        | Event::RunUnpauseRequested { actor }
        | Event::RunInterrupt { actor }
        | Event::RunSteer { actor, .. }
        | Event::RunPairStarted { actor, .. }
        | Event::RunPairEnded { actor, .. }
        | Event::RunPairFailed { actor, .. }
        | Event::RunArchived { actor }
        | Event::RunUnarchived { actor, .. }
        | Event::RunTitleUpdated { actor, .. }
        | Event::RunParentLinked { actor, .. }
        | Event::RunParentUnlinked { actor, .. }
        | Event::InterviewCompleted { actor, .. }
        | Event::AgentSteerBuffered { actor, .. } => StoredEventFields {
            actor: actor.clone(),
            ..StoredEventFields::default()
        },
        Event::StageCompleted { node_id, name, .. }
        | Event::StageStarted { node_id, name, .. }
        | Event::StageRetrying { node_id, name, .. } => {
            let node_id_str = node_id.clone();
            let node_label = default_node_label(Some(&node_id_str), Some(name.clone()));
            StoredEventFields {
                node_id: Some(node_id_str),
                node_label,
                ..StoredEventFields::default()
            }
        }
        Event::StageFailed {
            node_id,
            name,
            actor,
            ..
        } => {
            let node_id_str = node_id.clone();
            let node_label = default_node_label(Some(&node_id_str), Some(name.clone()));
            StoredEventFields {
                node_id: Some(node_id_str),
                node_label,
                actor: actor.clone(),
                ..StoredEventFields::default()
            }
        }
        Event::ParallelStarted { node_id, visit, .. }
        | Event::ParallelCompleted { node_id, visit, .. } => {
            let node_id_str = node_id.clone();
            let node_label = default_node_label(Some(&node_id_str), None);
            let parallel_group_id = Some(StageId::new(node_id_str.clone(), *visit));
            StoredEventFields {
                node_id: Some(node_id_str),
                node_label,
                parallel_group_id,
                ..StoredEventFields::default()
            }
        }
        Event::CheckpointCompleted { node_id, .. }
        | Event::CheckpointFailed { node_id, .. }
        | Event::SubgraphStarted { node_id, .. }
        | Event::SubgraphCompleted { node_id, .. }
        | Event::ArtifactCaptured { node_id, .. }
        | Event::PromptCompleted { node_id, .. }
        | Event::CommandStarted { node_id, .. }
        | Event::CommandCompleted { node_id, .. }
        | Event::AgentAcpCompleted { node_id, .. }
        | Event::AgentAcpCancelled { node_id, .. }
        | Event::AgentAcpTimedOut { node_id, .. } => node_stored_fields(Some(node_id.clone())),
        Event::AgentAcpStarted { node_id, visit, .. }
        | Event::AgentMcpReady { node_id, visit, .. }
        | Event::AgentMcpFailed { node_id, visit, .. }
        | Event::AgentMcpDisconnected { node_id, visit, .. } => {
            let node_id_str = node_id.clone();
            let node_label = default_node_label(Some(&node_id_str), None);
            StoredEventFields {
                node_id: Some(node_id_str.clone()),
                node_label,
                stage_id: Some(StageId::new(node_id_str, *visit)),
                ..StoredEventFields::default()
            }
        }
        Event::AgentSessionActivated {
            node_id,
            visit,
            session_id,
            ..
        }
        | Event::AgentToolsAvailable {
            node_id,
            visit,
            session_id,
            ..
        }
        | Event::AgentSessionDeactivated {
            node_id,
            visit,
            session_id,
        }
        | Event::AgentPairSystemMessage {
            node_id,
            visit,
            session_id,
            ..
        } => {
            let node_id_str = node_id.clone();
            let node_label = default_node_label(Some(&node_id_str), None);
            StoredEventFields {
                session_id: Some(session_id.clone()),
                node_id: Some(node_id_str.clone()),
                node_label,
                stage_id: Some(StageId::new(node_id_str, *visit)),
                ..StoredEventFields::default()
            }
        }
        Event::AgentInterruptInjected {
            node_id,
            visit,
            session_id,
            actor,
        }
        | Event::AgentPairUserMessage {
            node_id,
            visit,
            session_id,
            actor,
            ..
        } => {
            let node_id_str = node_id.clone();
            let node_label = default_node_label(Some(&node_id_str), None);
            StoredEventFields {
                session_id: Some(session_id.clone()),
                node_id: Some(node_id_str.clone()),
                node_label,
                stage_id: Some(StageId::new(node_id_str, *visit)),
                actor: actor.clone(),
                ..StoredEventFields::default()
            }
        }
        Event::AgentSteerDropped {
            actor,
            node_id,
            visit,
            ..
        } => {
            let node_id_str = node_id.clone();
            let node_label = node_id_str
                .as_ref()
                .and_then(|n| default_node_label(Some(n), None));
            let stage_id = match (node_id.clone(), visit) {
                (Some(n), Some(v)) => Some(StageId::new(n, *v)),
                _ => None,
            };
            StoredEventFields {
                node_id: node_id_str,
                node_label,
                stage_id,
                actor: actor.clone(),
                ..StoredEventFields::default()
            }
        }
        Event::Agent {
            stage,
            visit,
            event: envelope,
        } => {
            let node_id = Some(stage.clone());
            let node_label = default_node_label(node_id.as_ref(), None);
            let stage_id = Some(StageId::new(stage.clone(), *visit));
            let tool_call_id = envelope
                .tool_call_id
                .clone()
                .or_else(|| agent_tool_call_id(&envelope.event).map(str::to_string));
            let actor = agent_actor_for_event(
                &envelope.event,
                Some(envelope.session_id.as_str()),
                envelope.parent_session_id.as_deref(),
            );
            StoredEventFields {
                session_id: Some(envelope.session_id.clone()),
                parent_session_id: envelope.parent_session_id.clone(),
                node_id,
                node_label,
                stage_id,
                tool_call_id,
                actor,
                ..StoredEventFields::default()
            }
        }
        Event::GitCommit { node_id, .. } => node_stored_fields(node_id.clone()),
        Event::ParallelBranchStarted {
            parallel_group_id,
            parallel_branch_id,
            branch,
            ..
        }
        | Event::ParallelBranchCompleted {
            parallel_group_id,
            parallel_branch_id,
            branch,
            ..
        } => {
            let node_id = Some(branch.clone());
            let node_label = default_node_label(node_id.as_ref(), None);
            StoredEventFields {
                node_id,
                node_label,
                parallel_group_id: Some(parallel_group_id.clone()),
                parallel_branch_id: Some(parallel_branch_id.clone()),
                ..StoredEventFields::default()
            }
        }
        Event::Prompt { stage, .. }
        | Event::InterviewStarted { stage, .. }
        | Event::Failover { stage, .. } => node_stored_fields(Some(stage.clone())),
        Event::InterviewTimeout { actor, stage, .. }
        | Event::InterviewInterrupted { actor, stage, .. } => {
            let mut fields = node_stored_fields(Some(stage.clone()));
            fields.actor.clone_from(actor);
            fields
        }
        Event::StallWatchdogTimeout { node, .. } => {
            let mut fields = node_stored_fields(Some(node.clone()));
            fields.actor = Some(Principal::System {
                system_kind: SystemActorKind::Watchdog,
            });
            fields
        }
        _ => StoredEventFields::default(),
    }
}

fn agent_tool_call_id(event: &CodingEvent) -> Option<&str> {
    match event {
        CodingEvent::ToolCallStarted { tool_call_id, .. }
        | CodingEvent::ToolCallCompleted { tool_call_id, .. } => Some(tool_call_id.as_str()),
        _ => None,
    }
}

fn agent_actor_for_event(
    event: &CodingEvent,
    session_id: Option<&str>,
    parent_session_id: Option<&str>,
) -> Option<Principal> {
    match event {
        CodingEvent::AssistantMessage { model, .. } => Some(Principal::Agent {
            session_id:        session_id.map(str::to_string),
            parent_session_id: parent_session_id.map(str::to_string),
            model:             Some(model.clone()),
        }),
        CodingEvent::ToolCallStarted { .. }
        | CodingEvent::ToolCallOutputDelta { .. }
        | CodingEvent::ToolCallCompleted { .. }
        | CodingEvent::ToolProcessCompleted { .. } => Some(Principal::Agent {
            session_id:        session_id.map(str::to_string),
            parent_session_id: parent_session_id.map(str::to_string),
            model:             None,
        }),
        CodingEvent::SteeringInjected { actor, .. } => {
            actor.as_ref().and_then(principal_from_actor)
        }
        _ => None,
    }
}

/// The principal pebble's steering author stands for, where the mapping is
/// lossless. A human author cannot be rebuilt from pebble's `Actor`; the
/// durable `run.steer` event that delivered the steer carries the principal.
pub fn principal_from_actor(actor: &Actor) -> Option<Principal> {
    match actor {
        Actor::Agent { id } => Some(Principal::Agent {
            session_id:        id.clone(),
            parent_session_id: None,
            model:             None,
        }),
        Actor::System => Some(Principal::System {
            system_kind: SystemActorKind::Engine,
        }),
        _ => None,
    }
}

/// The pebble author for a fabro principal steering a session.
#[must_use]
pub fn actor_from_principal(principal: &Principal) -> Actor {
    match principal {
        Principal::User(user) => Actor::User {
            id:           Some(format!(
                "{}|{}",
                user.identity.issuer(),
                user.identity.subject()
            )),
            display_name: Some(user.login.clone()),
        },
        Principal::Agent { session_id, .. } => Actor::Agent {
            id: session_id.clone(),
        },
        Principal::System { .. } | Principal::Worker { .. } => Actor::System,
        Principal::Webhook { delivery_id } => Actor::External {
            label: Some(format!("webhook:{delivery_id}")),
        },
        Principal::Slack {
            team_id,
            user_id,
            user_name,
        } => Actor::External {
            label: Some(
                user_name
                    .clone()
                    .unwrap_or_else(|| format!("slack:{team_id}:{user_id}")),
            ),
        },
    }
}
