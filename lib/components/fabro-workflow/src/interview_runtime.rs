use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use fabro_interview::{Answer, AnswerSubmission, AnswerValue, Interviewer, Question};
use fabro_types::{
    BlockedReason, InterviewOption, Principal, QuestionType, StageId, SystemActorKind,
};
use futures::future;
use pebble_coding_agent::extensions::{
    Answer as AgentAnswer, AnswerStatus, HumanInputError, HumanInputProvider,
    Question as AgentQuestion, QuestionKind,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::event::{Emitter, Event, StageScope};
use crate::millis_u64;

/// Unresolved interviews per stage. A stage is present only while it has at
/// least one, so the run is blocked exactly when the map is non-empty.
#[derive(Debug, Default)]
pub(crate) struct InterviewBlockState {
    blocked_stages: HashMap<StageId, usize>,
}

impl InterviewBlockState {
    pub(crate) fn is_run_blocked(&self) -> bool {
        !self.blocked_stages.is_empty()
    }

    pub(crate) fn is_stage_blocked(&self, stage_id: &StageId) -> bool {
        self.blocked_stages.contains_key(stage_id)
    }

    fn block(&mut self, stage_id: StageId) {
        *self.blocked_stages.entry(stage_id).or_default() += 1;
    }

    /// `RunInterviewGuard` resolves at most once, so an unknown stage here
    /// means the state is already clear. Runs from `Drop`, so it must not
    /// panic.
    fn resolve(&mut self, stage_id: &StageId) {
        let Some(count) = self.blocked_stages.get_mut(stage_id) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.blocked_stages.remove(stage_id);
        }
    }
}

/// Run-scoped state for unresolved human input. Emits `run.blocked` on the
/// first unresolved human/agent interview and `run.unblocked` after the last
/// one resolves. Subscribers use the same state to suspend run and stage
/// timeout budgets without deriving runtime control from persisted events.
///
/// Both transitions publish the new state before emitting the event, so a
/// listener that reads `subscribe()` from an event callback always sees state
/// that agrees with the event it just received.
pub(crate) struct RunInterviewBlocker {
    state:       watch::Sender<InterviewBlockState>,
    /// Serializes state change plus event emission so concurrent guards cannot
    /// interleave into an out-of-order `run.blocked` / `run.unblocked` pair.
    transitions: Mutex<()>,
}

impl RunInterviewBlocker {
    #[must_use]
    pub(crate) fn new() -> Self {
        let (state, _) = watch::channel(InterviewBlockState::default());
        Self {
            state,
            transitions: Mutex::new(()),
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<InterviewBlockState> {
        self.state.subscribe()
    }

    pub(crate) fn block(
        self: &Arc<Self>,
        emitter: Arc<Emitter>,
        stage_id: StageId,
    ) -> RunInterviewGuard {
        let _transition = self
            .transitions
            .lock()
            .expect("interview transition mutex should not be poisoned");
        let mut newly_blocked = false;
        self.state.send_modify(|state| {
            newly_blocked = !state.is_run_blocked();
            state.block(stage_id.clone());
        });
        if newly_blocked {
            emitter.emit(&Event::RunBlocked {
                blocked_reason: BlockedReason::HumanInputRequired,
            });
        }
        RunInterviewGuard {
            blocker: Arc::clone(self),
            emitter,
            stage_id,
            resolved: false,
        }
    }

    fn resolved(&self, emitter: &Emitter, stage_id: &StageId) {
        let _transition = self
            .transitions
            .lock()
            .expect("interview transition mutex should not be poisoned");
        let mut fully_unblocked = false;
        self.state.send_modify(|state| {
            state.resolve(stage_id);
            fully_unblocked = !state.is_run_blocked();
        });
        if fully_unblocked {
            emitter.emit(&Event::RunUnblocked);
        }
    }
}

pub(crate) struct RunInterviewGuard {
    blocker:  Arc<RunInterviewBlocker>,
    emitter:  Arc<Emitter>,
    stage_id: StageId,
    resolved: bool,
}

impl RunInterviewGuard {
    pub(crate) fn resolve(mut self) {
        self.resolve_in_place();
    }

    fn resolve_in_place(&mut self) {
        if !self.resolved {
            self.blocker.resolved(self.emitter.as_ref(), &self.stage_id);
            self.resolved = true;
        }
    }
}

impl Drop for RunInterviewGuard {
    fn drop(&mut self) {
        self.resolve_in_place();
    }
}

/// Pebble's human-input provider for a workflow stage: the `ask_user`
/// tool's questions go to the run's interviewer and are recorded as
/// interview events, blocking the run's timeout budgets while they wait.
pub(crate) struct WorkflowHumanInput {
    interviewer: Arc<dyn Interviewer>,
    emitter:     Arc<Emitter>,
    stage_scope: StageScope,
    /// Graph node id, reported as the `stage` on interview events. Distinct
    /// from `stage_scope.stage_id()`, which is the visit-qualified `StageId`
    /// used to key block state.
    node_id:     String,
    blocker:     Arc<RunInterviewBlocker>,
}

impl WorkflowHumanInput {
    #[must_use]
    pub(crate) fn new(
        interviewer: Arc<dyn Interviewer>,
        emitter: Arc<Emitter>,
        stage_scope: StageScope,
        node_id: impl Into<String>,
        blocker: Arc<RunInterviewBlocker>,
    ) -> Self {
        Self {
            interviewer,
            emitter,
            stage_scope,
            node_id: node_id.into(),
            blocker,
        }
    }
}

struct PreparedQuestion {
    agent_question: AgentQuestion,
    question:       Question,
}

struct PendingAgentQuestionBatch {
    emitter:     Arc<Emitter>,
    stage_scope: StageScope,
    node_id:     String,
    questions:   Vec<(String, String)>,
    started_at:  Instant,
    guard:       Option<RunInterviewGuard>,
}

impl PendingAgentQuestionBatch {
    fn new(
        emitter: Arc<Emitter>,
        stage_scope: StageScope,
        node_id: String,
        prepared: &[PreparedQuestion],
        guard: RunInterviewGuard,
        started_at: Instant,
    ) -> Self {
        Self {
            emitter,
            stage_scope,
            node_id,
            questions: prepared
                .iter()
                .map(|prepared_question| {
                    (
                        prepared_question.question.id.clone(),
                        prepared_question.question.text.clone(),
                    )
                })
                .collect(),
            started_at,
            guard: Some(guard),
        }
    }

    fn resolve(mut self) {
        if let Some(guard) = self.guard.take() {
            guard.resolve();
        }
    }
}

impl Drop for PendingAgentQuestionBatch {
    fn drop(&mut self) {
        if self.guard.is_none() {
            return;
        }
        let duration_ms = millis_u64(self.started_at.elapsed());
        for (question_id, question) in &self.questions {
            self.emitter.emit_scoped(
                &Event::InterviewInterrupted {
                    actor: Some(Principal::System {
                        system_kind: SystemActorKind::Engine,
                    }),
                    question_id: question_id.clone(),
                    question: question.clone(),
                    stage: self.node_id.clone(),
                    reason: "interrupted".to_string(),
                    duration_ms,
                },
                &self.stage_scope,
            );
        }
        if let Some(guard) = self.guard.take() {
            guard.resolve();
        }
    }
}

#[async_trait]
impl HumanInputProvider for WorkflowHumanInput {
    async fn ask_questions(
        &self,
        tool_call_id: &str,
        questions: Vec<AgentQuestion>,
        cancel_token: CancellationToken,
    ) -> Result<Vec<AgentAnswer>, HumanInputError> {
        if questions.is_empty() {
            return Ok(Vec::new());
        }

        let prepared = questions
            .into_iter()
            .enumerate()
            .map(|(index, question)| self.prepare_question(tool_call_id, index, question))
            .collect::<Vec<_>>();

        for prepared_question in &prepared {
            let question = &prepared_question.question;
            self.emitter.emit_scoped(
                &Event::InterviewStarted {
                    question_id:     question.id.clone(),
                    question:        question.text.clone(),
                    stage:           self.node_id.clone(),
                    question_type:   question.question_type.to_string(),
                    options:         question.options.clone(),
                    allow_freeform:  question.allow_freeform,
                    timeout_seconds: None,
                    context_display: question.context_display.clone(),
                    review_target:   question.review_target.clone(),
                },
                &self.stage_scope,
            );
        }

        let interview_start = Instant::now();
        let cleanup = PendingAgentQuestionBatch::new(
            Arc::clone(&self.emitter),
            self.stage_scope.clone(),
            self.node_id.clone(),
            &prepared,
            self.blocker
                .block(Arc::clone(&self.emitter), self.stage_scope.stage_id()),
            interview_start,
        );
        let ask_all = future::join_all(
            prepared
                .iter()
                .map(|prepared_question| self.interviewer.ask(prepared_question.question.clone())),
        );
        tokio::pin!(ask_all);

        let answers = tokio::select! {
            submissions = &mut ask_all => Some(submissions),
            () = cancel_token.cancelled() => None,
        };

        let results = match answers {
            Some(submissions) => prepared
                .iter()
                .zip(submissions)
                .map(|(prepared_question, submission)| {
                    self.emit_submission_event(
                        prepared_question,
                        &submission,
                        millis_u64(interview_start.elapsed()),
                    );
                    answer_from_submission(&prepared_question.agent_question, &submission)
                })
                .collect::<Vec<_>>(),
            None => prepared
                .iter()
                .map(|prepared_question| {
                    self.emit_interrupted(
                        prepared_question,
                        Some(Principal::System {
                            system_kind: SystemActorKind::Engine,
                        }),
                        "interrupted",
                        millis_u64(interview_start.elapsed()),
                    );
                    AgentAnswer::unanswered(
                        &prepared_question.agent_question,
                        AnswerStatus::Interrupted,
                    )
                })
                .collect::<Vec<_>>(),
        };

        cleanup.resolve();
        Ok(results)
    }
}

impl WorkflowHumanInput {
    fn prepare_question(
        &self,
        tool_call_id: &str,
        index: usize,
        agent_question: AgentQuestion,
    ) -> PreparedQuestion {
        let question_type = match agent_question.kind {
            QuestionKind::MultiSelect => QuestionType::MultiSelect,
            // Pebble may add kinds; anything else is one choice from a list.
            QuestionKind::MultipleChoice | _ => QuestionType::MultipleChoice,
        };
        let mut question = Question::new(agent_question.text.clone(), question_type);
        question.id = internal_question_id(&self.stage_scope, tool_call_id, index);
        question.options = agent_question
            .options
            .iter()
            .map(|option| InterviewOption {
                key:         option.key.clone(),
                label:       option.label.clone(),
                description: option.description.clone(),
                preview:     option.preview.clone(),
            })
            .collect();
        question.allow_freeform = agent_question.allow_freeform;
        question.stage.clone_from(&self.node_id);
        question.metadata.insert(
            "agent.tool_call_id".to_string(),
            serde_json::json!(tool_call_id),
        );
        question.metadata.insert(
            "agent.original_question".to_string(),
            serde_json::json!(agent_question.original_question),
        );
        if let Some(original_id) = &agent_question.original_id {
            question.metadata.insert(
                "agent.original_id".to_string(),
                serde_json::json!(original_id),
            );
        }
        if let Some(header) = &agent_question.header {
            question
                .metadata
                .insert("agent.header".to_string(), serde_json::json!(header));
        }
        PreparedQuestion {
            agent_question,
            question,
        }
    }

    fn emit_submission_event(
        &self,
        prepared: &PreparedQuestion,
        submission: &AnswerSubmission,
        duration_ms: u64,
    ) {
        match submission.answer.value {
            AnswerValue::Timeout => self.emitter.emit_scoped(
                &Event::InterviewTimeout {
                    actor: Some(Principal::System {
                        system_kind: SystemActorKind::Timeout,
                    }),
                    question_id: prepared.question.id.clone(),
                    question: prepared.question.text.clone(),
                    stage: self.node_id.clone(),
                    duration_ms,
                },
                &self.stage_scope,
            ),
            AnswerValue::Interrupted => self.emit_interrupted(
                prepared,
                Some(submission.actor.clone()),
                "interrupted",
                duration_ms,
            ),
            AnswerValue::Cancelled => self.emit_interrupted(
                prepared,
                Some(submission.actor.clone()),
                "cancelled",
                duration_ms,
            ),
            _ => self.emitter.emit_scoped(
                &Event::InterviewCompleted {
                    actor: Some(submission.actor.clone()),
                    question_id: prepared.question.id.clone(),
                    question: prepared.question.text.clone(),
                    answer: answer_labels(&prepared.question.options, &submission.answer)
                        .join(", "),
                    duration_ms,
                },
                &self.stage_scope,
            ),
        }
    }

    fn emit_interrupted(
        &self,
        prepared: &PreparedQuestion,
        actor: Option<Principal>,
        reason: &str,
        duration_ms: u64,
    ) {
        self.emitter.emit_scoped(
            &Event::InterviewInterrupted {
                actor,
                question_id: prepared.question.id.clone(),
                question: prepared.question.text.clone(),
                stage: self.node_id.clone(),
                reason: reason.to_string(),
                duration_ms,
            },
            &self.stage_scope,
        );
    }
}

fn answer_from_submission(
    agent_question: &AgentQuestion,
    submission: &AnswerSubmission,
) -> AgentAnswer {
    let status = match &submission.answer.value {
        AnswerValue::Cancelled => Some(AnswerStatus::Cancelled),
        AnswerValue::Interrupted => Some(AnswerStatus::Interrupted),
        AnswerValue::Skipped => Some(AnswerStatus::Skipped),
        AnswerValue::Timeout => Some(AnswerStatus::Timeout),
        _ => None,
    };
    match status {
        Some(status) => AgentAnswer::unanswered(agent_question, status),
        None => AgentAnswer::answered(
            agent_question,
            answer_labels(&interview_options(agent_question), &submission.answer),
        ),
    }
}

fn interview_options(agent_question: &AgentQuestion) -> Vec<InterviewOption> {
    agent_question
        .options
        .iter()
        .map(|option| InterviewOption {
            key:         option.key.clone(),
            label:       option.label.clone(),
            description: option.description.clone(),
            preview:     option.preview.clone(),
        })
        .collect()
}

fn answer_labels(options: &[InterviewOption], answer: &Answer) -> Vec<String> {
    match &answer.value {
        AnswerValue::Selected(key) => vec![label_for_key(options, key)],
        AnswerValue::MultiSelected(keys) => {
            keys.iter().map(|key| label_for_key(options, key)).collect()
        }
        AnswerValue::Text(text) => vec![text.clone()],
        AnswerValue::Yes => vec!["yes".to_string()],
        AnswerValue::No => vec!["no".to_string()],
        AnswerValue::Cancelled => vec!["cancelled".to_string()],
        AnswerValue::Interrupted => vec!["interrupted".to_string()],
        AnswerValue::Skipped => vec!["skipped".to_string()],
        AnswerValue::Timeout => vec!["timeout".to_string()],
    }
}

fn label_for_key(options: &[InterviewOption], key: &str) -> String {
    options
        .iter()
        .find(|option| option.key == key)
        .map_or_else(|| key.to_string(), |option| option.label.clone())
}

fn internal_question_id(scope: &StageScope, tool_call_id: &str, index: usize) -> String {
    format!(
        "agentq-{}-v{}-{}-{}-{}",
        slug(&scope.node_id),
        scope.visit,
        slug(tool_call_id),
        index + 1,
        Ulid::new(),
    )
}

fn slug(value: &str) -> String {
    let mut out = value
        .chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else if matches!(ch, '-' | '_') {
                Some(ch)
            } else {
                None
            }
        })
        .take(48)
        .collect::<String>();
    if out.is_empty() {
        out.push('x');
    }
    out
}

#[cfg(test)]
mod tests {
    use fabro_interview::ControlInterviewer;
    use fabro_types::{EventBody, RunId};
    use pebble_coding_agent::extensions::QuestionOption;

    use super::*;

    #[test]
    fn answer_labels_return_user_facing_labels_in_submission_order() {
        let options = vec![
            InterviewOption {
                key: "a".to_string(),
                label: "Alpha".to_string(),
                ..InterviewOption::default()
            },
            InterviewOption {
                key: "b".to_string(),
                label: "Beta".to_string(),
                ..InterviewOption::default()
            },
        ];
        let answer = Answer::multi_selected(vec!["b".to_string(), "a".to_string()]);

        assert_eq!(answer_labels(&options, &answer), vec!["Beta", "Alpha"]);
    }

    #[test]
    fn internal_question_id_includes_stage_visit_and_tool_call_context() {
        let scope = StageScope {
            node_id:            "Review Changes".to_string(),
            visit:              3,
            parallel_group_id:  None,
            parallel_branch_id: None,
        };

        let id = internal_question_id(&scope, "call_123", 1);

        assert!(id.starts_with("agentq-reviewchanges-v3-call_123-2-"));
        let ulid = id
            .rsplit('-')
            .next()
            .expect("question id should include a ULID suffix");
        assert_eq!(ulid.len(), 26);
    }

    #[tokio::test]
    async fn batch_questions_are_all_started_before_run_is_blocked_and_return_labels() {
        let interviewer = Arc::new(ControlInterviewer::new());
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        emitter.on_event({
            let events = Arc::clone(&events);
            move |event| events.lock().unwrap().push(event.clone())
        });
        let stage_scope = StageScope {
            node_id:            "ask".to_string(),
            visit:              1,
            parallel_group_id:  None,
            parallel_branch_id: None,
        };
        let stage_id = stage_scope.stage_id();
        let blocker = Arc::new(RunInterviewBlocker::new());
        let block_state = blocker.subscribe();
        let runtime = WorkflowHumanInput::new(
            interviewer.clone(),
            Arc::clone(&emitter),
            stage_scope,
            "ask",
            blocker,
        );
        let option = QuestionOption {
            key:         "ship".to_string(),
            label:       "Ship it".to_string(),
            description: Some("Deploy".to_string()),
            preview:     Some("preview".to_string()),
        };

        let ask = tokio::spawn(async move {
            runtime
                .ask_questions(
                    "call_1",
                    vec![
                        AgentQuestion {
                            original_id:       Some("q1".to_string()),
                            original_question: "First?".to_string(),
                            header:            None,
                            text:              "First?".to_string(),
                            kind:              QuestionKind::MultipleChoice,
                            options:           vec![option.clone()],
                            allow_freeform:    true,
                        },
                        AgentQuestion {
                            original_id:       Some("q2".to_string()),
                            original_question: "Second?".to_string(),
                            header:            None,
                            text:              "Second?".to_string(),
                            kind:              QuestionKind::MultipleChoice,
                            options:           vec![option.clone()],
                            allow_freeform:    true,
                        },
                    ],
                    CancellationToken::new(),
                )
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert!(block_state.borrow().is_run_blocked());
        assert!(block_state.borrow().is_stage_blocked(&stage_id));
        let question_ids = {
            let events = events.lock().unwrap();
            assert!(matches!(events[0].body, EventBody::InterviewStarted(_)));
            assert!(matches!(events[1].body, EventBody::InterviewStarted(_)));
            assert!(matches!(events[2].body, EventBody::RunBlocked(_)));
            events
                .iter()
                .filter_map(|event| match &event.body {
                    EventBody::InterviewStarted(props) => Some(props.question_id.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        for question_id in question_ids {
            let option = InterviewOption {
                key: "ship".to_string(),
                label: "Ship it".to_string(),
                ..InterviewOption::default()
            };
            interviewer
                .submit(
                    &question_id,
                    AnswerSubmission::system(
                        Answer::selected("ship", option),
                        SystemActorKind::Engine,
                    ),
                )
                .await
                .unwrap();
        }

        let answers = ask.await.unwrap();

        assert_eq!(answers.len(), 2);
        assert_eq!(answers[0].answers, vec!["Ship it"]);
        assert_eq!(answers[1].answers, vec!["Ship it"]);
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event.body, EventBody::RunUnblocked(_)))
        );
        assert!(!block_state.borrow().is_run_blocked());
        assert!(!block_state.borrow().is_stage_blocked(&stage_id));
    }

    #[tokio::test]
    async fn cancelling_agent_question_unblocks_its_stage() {
        let interviewer = Arc::new(ControlInterviewer::new());
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let stage_scope = StageScope {
            node_id:            "ask".to_string(),
            visit:              1,
            parallel_group_id:  None,
            parallel_branch_id: None,
        };
        let stage_id = stage_scope.stage_id();
        let blocker = Arc::new(RunInterviewBlocker::new());
        let block_state = blocker.subscribe();
        let runtime = WorkflowHumanInput::new(
            interviewer,
            emitter,
            stage_scope,
            "ask",
            Arc::clone(&blocker),
        );
        let cancel_token = CancellationToken::new();
        let ask_cancel_token = cancel_token.clone();
        let ask = tokio::spawn(async move {
            runtime
                .ask_questions(
                    "call_1",
                    vec![AgentQuestion {
                        original_id:       Some("q1".to_string()),
                        original_question: "Continue?".to_string(),
                        header:            None,
                        text:              "Continue?".to_string(),
                        kind:              QuestionKind::MultipleChoice,
                        options:           Vec::new(),
                        allow_freeform:    true,
                    }],
                    ask_cancel_token,
                )
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert!(block_state.borrow().is_run_blocked());
        assert!(block_state.borrow().is_stage_blocked(&stage_id));

        cancel_token.cancel();
        let answers = ask.await.unwrap();

        assert_eq!(answers[0].status, AnswerStatus::Interrupted);
        assert!(!block_state.borrow().is_run_blocked());
        assert!(!block_state.borrow().is_stage_blocked(&stage_id));
    }
}
