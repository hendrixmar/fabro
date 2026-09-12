use std::collections::BTreeMap;

use fabro_types::{
    EventBody, EventEnvelope, RunId, RunSessionMetadata, SessionId, SessionStatus, SessionSummary,
    SessionTurn,
};

/// Ask Fabro session metadata at the event-log position it was read at.
///
/// The transcript is not projected from run events: pebble's session record
/// holds the durable history, and the `run.session.*` events stream it live.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedRunSession {
    pub record:   RunSessionMetadata,
    pub last_seq: u32,
}

pub fn project_run_sessions(run_id: RunId, events: &[EventEnvelope]) -> Vec<SessionSummary> {
    let mut projection = RunSessionProjection::default();
    projection.apply(run_id, events);
    projection
        .sessions
        .values()
        .map(|session| SessionSummary::from(&session.record))
        .collect()
}

pub fn project_run_session(
    run_id: RunId,
    session_id: SessionId,
    events: &[EventEnvelope],
) -> Option<ProjectedRunSession> {
    let mut projection = RunSessionProjection::default();
    projection.apply(run_id, events);
    projection.sessions.remove(&session_id)
}

#[derive(Default)]
struct RunSessionProjection {
    sessions: BTreeMap<SessionId, ProjectedRunSession>,
}

impl RunSessionProjection {
    fn apply(&mut self, run_id: RunId, events: &[EventEnvelope]) {
        for envelope in events {
            let Some(session_id) = event_session_id(envelope) else {
                continue;
            };
            match &envelope.event.body {
                EventBody::RunSessionCreated(props) => {
                    let mut record = RunSessionMetadata::new(session_id, run_id, envelope.event.ts);
                    record.title.clone_from(&props.title);
                    record.model.clone_from(&props.model);
                    record.provider.clone_from(&props.provider);
                    self.sessions.insert(session_id, ProjectedRunSession {
                        record,
                        last_seq: envelope.seq,
                    });
                }
                EventBody::RunSessionTurnStarted(props) => {
                    if let Some(session) = self.sessions.get_mut(&session_id) {
                        session.last_seq = envelope.seq;
                        session.record.status = SessionStatus::Running;
                        session.record.active_turn = Some(SessionTurn {
                            id:         props.turn_id,
                            started_at: envelope.event.ts,
                            input:      props.input.clone(),
                        });
                        session.record.updated_at = envelope.event.ts;
                    }
                }
                EventBody::RunSessionUserMessage(_)
                | EventBody::RunSessionAssistantMessage(_)
                | EventBody::RunSessionAssistantDelta(_)
                | EventBody::RunSessionToolCallStarted(_)
                | EventBody::RunSessionToolCallCompleted(_) => {
                    if let Some(session) = self.sessions.get_mut(&session_id) {
                        session.last_seq = envelope.seq;
                        session.record.updated_at = envelope.event.ts;
                    }
                }
                EventBody::RunSessionTurnFailed(_) => {
                    self.finish_turn(session_id, true, envelope.event.ts, envelope.seq);
                }
                EventBody::RunSessionTurnSucceeded(_) | EventBody::RunSessionTurnInterrupted(_) => {
                    self.finish_turn(session_id, false, envelope.event.ts, envelope.seq);
                }
                _ => {}
            }
        }
    }

    fn finish_turn(
        &mut self,
        session_id: SessionId,
        failed: bool,
        timestamp: chrono::DateTime<chrono::Utc>,
        seq: u32,
    ) {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            session.last_seq = seq;
            session.record.status = if failed {
                SessionStatus::Failed
            } else {
                SessionStatus::Idle
            };
            session.record.active_turn = None;
            session.record.updated_at = timestamp;
        }
    }
}

fn event_session_id(envelope: &EventEnvelope) -> Option<SessionId> {
    envelope
        .event
        .session_id
        .as_deref()
        .and_then(|id| id.parse().ok())
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use fabro_types::run_event::{
        RunSessionAssistantMessageProps, RunSessionCreatedProps, RunSessionTurnFailedCode,
        RunSessionTurnFailedProps, RunSessionTurnStartedProps, RunSessionTurnSucceededProps,
        RunSessionUserMessageProps,
    };
    use fabro_types::{EventBody, EventEnvelope, RunEvent, TurnId, fixtures};
    use serde_json::json;

    use super::{project_run_session, project_run_sessions};

    #[test]
    fn projection_tracks_turn_lifecycle_and_last_seq() {
        let session_id = fabro_types::SessionId::new();
        let turn_id = TurnId::new();
        let events = vec![
            event(
                1,
                session_id,
                EventBody::RunSessionCreated(RunSessionCreatedProps {
                    title:    Some("Ask".to_string()),
                    model:    Some("test-model".to_string()),
                    provider: None,
                }),
            ),
            event(
                2,
                session_id,
                EventBody::RunSessionTurnStarted(RunSessionTurnStartedProps {
                    turn_id,
                    input: "What happened?".to_string(),
                }),
            ),
            event(
                3,
                session_id,
                EventBody::RunSessionUserMessage(RunSessionUserMessageProps {
                    turn_id,
                    text: "What happened?".to_string(),
                }),
            ),
        ];

        let running = project_run_session(fixtures::RUN_1, session_id, &events)
            .expect("session should project from run events");
        assert_eq!(running.record.status, fabro_types::SessionStatus::Running);
        assert_eq!(
            running.record.active_turn.as_ref().map(|turn| turn.id),
            Some(turn_id)
        );
        assert_eq!(running.last_seq, 3);

        let mut events = events;
        events.push(event(
            4,
            session_id,
            EventBody::RunSessionAssistantMessage(RunSessionAssistantMessageProps {
                turn_id,
                text: "The run finished.".to_string(),
                model: Some("test-model".to_string()),
                usage: json!({ "output_tokens": 4 }),
            }),
        ));
        events.push(event(
            5,
            session_id,
            EventBody::RunSessionTurnSucceeded(RunSessionTurnSucceededProps {
                turn_id,
                output: Some("The run finished.".to_string()),
            }),
        ));

        let idle = project_run_session(fixtures::RUN_1, session_id, &events).unwrap();
        assert_eq!(idle.record.status, fabro_types::SessionStatus::Idle);
        assert!(idle.record.active_turn.is_none());
        assert_eq!(idle.record.model.as_deref(), Some("test-model"));
        assert_eq!(idle.last_seq, 5);
    }

    #[test]
    fn a_failed_turn_marks_the_session_failed() {
        let session_id = fabro_types::SessionId::new();
        let turn_id = TurnId::new();
        let events = vec![
            event(
                1,
                session_id,
                EventBody::RunSessionCreated(RunSessionCreatedProps {
                    title:    None,
                    model:    None,
                    provider: None,
                }),
            ),
            event(
                2,
                session_id,
                EventBody::RunSessionTurnStarted(RunSessionTurnStartedProps {
                    turn_id,
                    input: "hi".to_string(),
                }),
            ),
            event(
                3,
                session_id,
                EventBody::RunSessionTurnFailed(RunSessionTurnFailedProps {
                    turn_id,
                    error: "boom".to_string(),
                    output: None,
                    code: RunSessionTurnFailedCode::AgentError,
                    retryable: false,
                }),
            ),
        ];

        let summaries = project_run_sessions(fixtures::RUN_1, &events);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].status, fabro_types::SessionStatus::Failed);
        assert!(summaries[0].active_turn.is_none());
    }

    fn event(seq: u32, session_id: fabro_types::SessionId, body: EventBody) -> EventEnvelope {
        EventEnvelope {
            seq,
            event: RunEvent {
                id: format!("evt-{seq}"),
                ts: Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, seq).unwrap(),
                run_id: fixtures::RUN_1,
                node_id: None,
                node_label: None,
                stage_id: None,
                parallel_group_id: None,
                parallel_branch_id: None,
                session_id: Some(session_id.to_string()),
                parent_session_id: None,
                tool_call_id: None,
                actor: None,
                body,
            },
        }
    }
}
