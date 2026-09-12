//! Fabro's control plane over pebble's steering bus.
//!
//! The bus carries steers and interrupts to every live agent session, buffers
//! steers that arrive between sessions, and holds a session open while a
//! human is paired with it. What fabro adds is attribution: which run and
//! stage a session belongs to, who asked (a [`Principal`]), the pair record
//! the API serves, and the run events (`run.steer`, `run.interrupt`,
//! `agent.steer.buffered`, `agent.steer.dropped`, `agent.interrupt.injected`,
//! the pair events) that put bus activity on the run's durable stream in the
//! order fabro's consumers expect.
//!
//! Every method is synchronous and never awaits under a lock, so the agent
//! loop's close-the-door check runs from its completion path.

use std::sync::{Arc, Mutex, PoisonError};

use chrono::Utc;
use fabro_types::run_event::AgentSteerDroppedReason;
use fabro_types::{
    PairId, PairMessageId, PairMessageRecord, PairRecord, PairStatus, PairSystemMessageKind,
    PairTarget, Principal, RunId, RunPairEndedReason, StageId,
};
use pebble_coding_agent::events::Actor;
use pebble_coding_agent::steering::{
    AttachError, Attachment, DropReason, DroppedSteer, SteerableSession, SteeringBus, TargetError,
};
use pebble_coding_agent::{SteeringMessage, SteeringOutcome};

use crate::event::{Emitter, Event, actor_from_principal, principal_from_actor};

#[derive(Debug, Clone)]
struct ActivePair {
    record:     PairRecord,
    /// The agent session active at `start_pair` time, so a later pair command
    /// or a session's deactivation can tell whether the session was replaced.
    session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairControlError {
    AlreadyPaired,
    PairNotCurrent,
    PairNotActive,
    TargetNotActive,
    MessageNotAccepted,
}

#[allow(
    clippy::module_name_repetitions,
    reason = "external callers refer to it as SteeringHub"
)]
pub struct SteeringHub {
    bus:         SteeringBus<StageId>,
    active_pair: Mutex<Option<ActivePair>>,
    emitter:     Arc<Emitter>,
}

impl SteeringHub {
    #[must_use]
    pub fn new(emitter: Arc<Emitter>) -> Self {
        Self {
            bus: SteeringBus::new(),
            active_pair: Mutex::new(None),
            emitter,
        }
    }

    /// Test-only constructor with an isolated emitter.
    #[cfg(test)]
    #[must_use]
    pub fn for_tests() -> Arc<Self> {
        Arc::new(Self::new(Arc::new(Emitter::new(RunId::new()))))
    }

    /// Test-only: how many steers wait for the next session.
    #[cfg(test)]
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.bus.pending_len()
    }

    /// Test-only: how many sessions are attached.
    #[cfg(test)]
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.bus.attached_count()
    }

    /// Attach a live session as steerable for this stage. Fails when a
    /// different session is already active for the stage.
    pub(crate) fn attach(
        &self,
        stage_id: &StageId,
        session_id: &str,
        session: Arc<dyn SteerableSession>,
    ) -> Result<(), AttachError> {
        self.bus.attach(stage_id.clone(), session_id, session)
    }

    /// Move buffered run-wide steers into the stage's session.
    pub(crate) fn drain_pending_into(&self, stage_id: &StageId) {
        let delivery = self.bus.drain_pending_into(stage_id);
        self.emit_dropped(&delivery.dropped);
    }

    /// Detach the session for this stage. Stale session ids are ignored.
    pub(crate) fn detach(&self, stage_id: &StageId, session_id: &str) -> bool {
        if !self.bus.detach(stage_id, session_id) {
            return false;
        }
        self.end_active_pair_for_target(stage_id, session_id, RunPairEndedReason::SessionEnded);
        true
    }

    /// The agent loop's close-the-door check: detach only when the session
    /// has no steering waiting, atomically against a steer arriving.
    pub(crate) fn detach_if_idle(&self, stage_id: &StageId, session_id: &str) -> bool {
        if !self.bus.detach_if_idle(stage_id, session_id) {
            return false;
        }
        self.end_active_pair_for_target(stage_id, session_id, RunPairEndedReason::SessionEnded);
        true
    }

    /// Deliver a steer from the control plane: to every active session, or
    /// into the run-wide buffer when none is active.
    pub fn deliver_steer(&self, text: String, actor: Option<Principal>) {
        self.emitter.emit(&Event::RunSteer {
            text:  text.clone(),
            actor: actor.clone(),
        });
        let delivery = self.bus.steer(steering_message(text, actor.as_ref()));
        self.emit_dropped(&delivery.dropped);
        if delivery.buffered {
            self.emitter.emit(&Event::AgentSteerBuffered { actor });
        }
    }

    /// Interrupt every active session. Not buffered: with no session active
    /// there is nothing to stop.
    pub fn interrupt(&self, actor: Option<&Principal>) {
        if self.bus.attached_count() == 0 {
            return;
        }
        self.emitter.emit(&Event::RunInterrupt {
            actor: actor.cloned(),
        });
        let interruption = self.bus.interrupt();
        self.emit_interrupted(&interruption.interrupted, actor);
    }

    /// Interrupt every active session and hand each the steering text as
    /// what replaces its round, emitting the run events in that order.
    pub fn interrupt_then_steer(&self, text: &str, actor: Option<&Principal>) {
        if self.bus.attached_count() == 0 {
            return;
        }
        self.emitter.emit(&Event::RunInterrupt {
            actor: actor.cloned(),
        });
        self.emitter.emit(&Event::RunSteer {
            text:  text.to_string(),
            actor: actor.cloned(),
        });
        let interruption = self
            .bus
            .interrupt_then_steer(&steering_message(text.to_string(), actor));
        self.emit_dropped(&interruption.dropped);
        self.emit_interrupted(&interruption.interrupted, actor);
    }

    /// Drop any steer nobody read and say so once, with `reason: run_ended`.
    /// Called from `operations::start` after the pipeline finishes but
    /// before the emitter is flushed.
    pub fn drain_pending_at_run_end(&self) {
        if let Some(dropped) = self.bus.drain_pending() {
            self.emitter.emit(&Event::AgentSteerDropped {
                reason:  AgentSteerDroppedReason::RunEnded,
                count:   u32::try_from(dropped.count).unwrap_or(u32::MAX),
                actor:   None,
                node_id: None,
                visit:   None,
            });
        }
        self.end_active_pair(RunPairEndedReason::RunEnded);
    }

    pub fn start_pair(
        &self,
        run_id: RunId,
        pair_id: PairId,
        target: PairTarget,
        actor: Option<Principal>,
    ) -> Result<PairRecord, PairControlError> {
        let session_id = self
            .bus
            .attachments()
            .into_iter()
            .find(|attachment| attachment.key == target.stage_id)
            .map(|attachment| attachment.session_id)
            .ok_or(PairControlError::TargetNotActive)?;

        let mut active_pair = self
            .active_pair
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if active_pair.is_some() {
            return Err(PairControlError::AlreadyPaired);
        }
        // The hold comes first: a session that cannot be held open cannot be
        // paired with, and nothing is queued on it.
        match self.bus.hold_open(&target.stage_id, &session_id) {
            Ok(()) => {}
            Err(TargetError::AlreadyHeld) => return Err(PairControlError::AlreadyPaired),
            Err(TargetError::NotAttached | TargetError::Unsupported) => {
                return Err(PairControlError::TargetNotActive);
            }
        }

        let text = human_joined_text();
        let notice = SteeringMessage::new(text).with_actor(Actor::System);
        if let Err(error) = self.send_to_paired(&target.stage_id, &session_id, notice) {
            self.bus.release_hold(&target.stage_id, &session_id);
            return Err(error);
        }

        let record = PairRecord {
            pair_id,
            run_id,
            status: PairStatus::Active,
            started_at: Utc::now(),
            ended_at: None,
            failure_reason: None,
            target,
        };
        self.emitter.emit(&Event::RunPairStarted {
            pair_id,
            target: record.target.clone(),
            actor,
        });
        // With the notice already queued the session does not park: the
        // notice opens its next round.
        let _ = self.bus.interrupt_at(&record.target.stage_id, &session_id);
        self.emitter.emit(&Event::AgentPairSystemMessage {
            node_id: record.target.stage_id.node_id().to_string(),
            visit: record.target.stage_id.visit(),
            session_id: session_id.clone(),
            pair_id,
            kind: PairSystemMessageKind::HumanJoined,
            text: text.to_string(),
        });
        *active_pair = Some(ActivePair {
            record: record.clone(),
            session_id,
        });
        Ok(record)
    }

    pub fn send_pair_message(
        &self,
        pair_id: PairId,
        message_id: PairMessageId,
        text: String,
        client_message_id: Option<String>,
        actor: Option<Principal>,
    ) -> Result<PairMessageRecord, PairControlError> {
        let active_pair = self
            .active_pair
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let pair = current_pair(active_pair.as_ref(), pair_id)?;
        let target = &pair.record.target;
        let session_id = pair.session_id.clone();

        let message = SteeringMessage::new(text.clone()).with_actor(Actor::User {
            id:           None,
            display_name: None,
        });
        self.send_to_paired(&target.stage_id, &session_id, message)?;
        self.emitter.emit(&Event::AgentPairUserMessage {
            node_id: target.stage_id.node_id().to_string(),
            visit: target.stage_id.visit(),
            session_id,
            pair_id,
            message_id,
            client_message_id: client_message_id.clone(),
            text: text.clone(),
            actor,
        });
        Ok(PairMessageRecord {
            message_id,
            client_message_id,
            pair_id,
            run_id: pair.record.run_id,
            stage_id: target.stage_id.clone(),
            text,
            accepted_at: Utc::now(),
        })
    }

    pub fn end_pair(
        &self,
        pair_id: PairId,
        actor: Option<Principal>,
    ) -> Result<PairRecord, PairControlError> {
        let mut active_pair = self
            .active_pair
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let pair = current_pair(active_pair.as_ref(), pair_id)?;
        let target = pair.record.target.clone();
        let session_id = pair.session_id.clone();

        if self.bus.is_attached(&target.stage_id, &session_id) {
            let text = human_left_text();
            let notice = SteeringMessage::new(text).with_actor(Actor::System);
            self.send_to_paired(&target.stage_id, &session_id, notice)?;
            self.emitter.emit(&Event::AgentPairSystemMessage {
                node_id: target.stage_id.node_id().to_string(),
                visit: target.stage_id.visit(),
                session_id: session_id.clone(),
                pair_id,
                kind: PairSystemMessageKind::HumanLeft,
                text: text.to_string(),
            });
            self.bus.release_hold(&target.stage_id, &session_id);
        }

        let mut record = pair.record.clone();
        record.status = PairStatus::Ended;
        record.ended_at = Some(Utc::now());
        self.emitter.emit(&Event::RunPairEnded {
            pair_id,
            reason: RunPairEndedReason::UserRequested,
            actor,
        });
        *active_pair = None;
        Ok(record)
    }

    #[must_use]
    pub fn pair_is_active_for(&self, stage_id: &StageId, session_id: &str) -> bool {
        self.active_pair
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|pair| {
                pair.record.status == PairStatus::Active
                    && pair.record.target.stage_id == *stage_id
                    && pair.session_id == session_id
            })
    }

    /// Queue a paired human's message or a pair notice on the target session.
    /// A message the session queued by evicting an older steer is accepted,
    /// and the eviction is recorded; a closed session accepts nothing.
    fn send_to_paired(
        &self,
        stage_id: &StageId,
        session_id: &str,
        message: SteeringMessage,
    ) -> Result<(), PairControlError> {
        match self.bus.send_to(stage_id, session_id, message) {
            Ok(SteeringOutcome::Accepted) => Ok(()),
            Ok(SteeringOutcome::Evicted(evicted)) => {
                self.emit_dropped(&[DroppedSteer {
                    reason:     DropReason::QueueFull,
                    count:      1,
                    actor:      evicted.actor().cloned(),
                    attachment: Some(Attachment {
                        key:        stage_id.clone(),
                        session_id: session_id.to_string(),
                    }),
                }]);
                Ok(())
            }
            Ok(_) => Err(PairControlError::MessageNotAccepted),
            Err(_) => Err(PairControlError::TargetNotActive),
        }
    }

    fn end_active_pair_for_target(
        &self,
        stage_id: &StageId,
        session_id: &str,
        reason: RunPairEndedReason,
    ) -> bool {
        let pair_id = {
            let mut active_pair = self
                .active_pair
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(pair) = active_pair.as_ref() else {
                return false;
            };
            if pair.record.status != PairStatus::Active
                || pair.record.target.stage_id != *stage_id
                || pair.session_id != session_id
            {
                return false;
            }
            let pair_id = pair.record.pair_id;
            *active_pair = None;
            pair_id
        };
        // The bus released the session's hold when it detached.
        self.emitter.emit(&Event::RunPairEnded {
            pair_id,
            reason,
            actor: None,
        });
        true
    }

    fn end_active_pair(&self, reason: RunPairEndedReason) -> bool {
        let pair_id = {
            let mut active_pair = self
                .active_pair
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(mut pair) = active_pair.take() else {
                return false;
            };
            if pair.record.status != PairStatus::Active {
                *active_pair = Some(pair);
                return false;
            }
            pair.record.status = PairStatus::Ended;
            pair.record.ended_at = Some(Utc::now());
            pair.record.pair_id
        };
        self.emitter.emit(&Event::RunPairEnded {
            pair_id,
            reason,
            actor: None,
        });
        true
    }

    /// One `agent.steer.dropped { queue_full }` per message a queue evicted,
    /// naming the stage whose session dropped it when one did.
    fn emit_dropped(&self, dropped: &[DroppedSteer<StageId>]) {
        for drop in dropped {
            let stage_id = drop.attachment.as_ref().map(|attachment| &attachment.key);
            self.emitter.emit(&Event::AgentSteerDropped {
                reason:  match drop.reason {
                    DropReason::Ended => AgentSteerDroppedReason::RunEnded,
                    DropReason::QueueFull | _ => AgentSteerDroppedReason::QueueFull,
                },
                count:   u32::try_from(drop.count).unwrap_or(u32::MAX),
                actor:   drop.actor.as_ref().and_then(principal_from_actor),
                node_id: stage_id.map(|stage| stage.node_id().to_string()),
                visit:   stage_id.map(StageId::visit),
            });
        }
    }

    fn emit_interrupted(&self, interrupted: &[Attachment<StageId>], actor: Option<&Principal>) {
        for attachment in interrupted {
            self.emitter.emit(&Event::AgentInterruptInjected {
                node_id:    attachment.key.node_id().to_string(),
                visit:      attachment.key.visit(),
                session_id: attachment.session_id.clone(),
                actor:      actor.cloned(),
            });
        }
    }
}

fn current_pair(
    pair: Option<&ActivePair>,
    pair_id: PairId,
) -> Result<&ActivePair, PairControlError> {
    let pair = pair.ok_or(PairControlError::PairNotActive)?;
    if pair.record.pair_id != pair_id {
        return Err(PairControlError::PairNotCurrent);
    }
    if pair.record.status != PairStatus::Active {
        return Err(PairControlError::PairNotActive);
    }
    Ok(pair)
}

/// A steer as the session reads it, with fabro's principal as pebble's actor.
fn steering_message(text: String, actor: Option<&Principal>) -> SteeringMessage {
    let message = SteeringMessage::new(text);
    match actor {
        Some(actor) => message.with_actor(actor_from_principal(actor)),
        None => message,
    }
}

pub fn human_joined_text() -> &'static str {
    "A human has joined this workflow run for live pairing. Wait for their next message before continuing."
}

pub fn human_left_text() -> &'static str {
    "The human has ended live pairing. Continue autonomously with the workflow."
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use fabro_types::{
        PairId, PairMessageId, PairTarget, Principal, RunEvent, RunId, StageId, SystemActorKind,
    };
    use pebble_coding_agent::steering::SessionHold;

    use super::*;
    use crate::event::Emitter;

    /// A steerable, pairable session with a bounded queue, standing in for
    /// pebble's control handle.
    struct SessionControlHandle {
        queue:       Mutex<VecDeque<SteeringMessage>>,
        capacity:    usize,
        interrupted: AtomicUsize,
        pairable:    bool,
    }

    impl SessionControlHandle {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                queue:       Mutex::new(VecDeque::new()),
                capacity:    32,
                interrupted: AtomicUsize::new(0),
                pairable:    true,
            })
        }

        /// A session on a backend that cannot hold its completion open, as
        /// the ACP adapter is.
        fn unpairable() -> Arc<Self> {
            Arc::new(Self {
                queue:       Mutex::new(VecDeque::new()),
                capacity:    32,
                interrupted: AtomicUsize::new(0),
                pairable:    false,
            })
        }

        fn queue_len(&self) -> usize {
            self.queue.lock().unwrap().len()
        }

        fn interrupt_count(&self) -> usize {
            self.interrupted.load(Ordering::SeqCst)
        }
    }

    impl SteerableSession for SessionControlHandle {
        fn steer(&self, message: SteeringMessage) -> SteeringOutcome {
            let mut queue = self.queue.lock().unwrap();
            let evicted = (queue.len() >= self.capacity)
                .then(|| queue.pop_front())
                .flatten();
            queue.push_back(message);
            evicted.map_or(SteeringOutcome::Accepted, SteeringOutcome::Evicted)
        }

        fn interrupt(&self) -> bool {
            self.interrupted.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn steer_now(&self, message: SteeringMessage) -> SteeringOutcome {
            self.interrupt();
            self.steer(message)
        }

        fn has_pending_steering(&self) -> bool {
            !self.queue.lock().unwrap().is_empty()
        }

        fn hold_open(&self) -> Option<SessionHold> {
            self.pairable.then(|| SessionHold::new(()))
        }
    }

    fn hub_with_event_names() -> (Arc<SteeringHub>, Arc<Mutex<Vec<String>>>) {
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let names = Arc::new(Mutex::new(Vec::new()));
        let names_for_listener = Arc::clone(&names);
        emitter.on_event(move |event| {
            names_for_listener
                .lock()
                .unwrap()
                .push(event.event_name().to_string());
        });
        (Arc::new(SteeringHub::new(emitter)), names)
    }

    fn hub_with_events() -> (Arc<SteeringHub>, Arc<Mutex<Vec<RunEvent>>>) {
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_for_listener = Arc::clone(&events);
        emitter.on_event(move |event| {
            events_for_listener.lock().unwrap().push(event.clone());
        });
        (Arc::new(SteeringHub::new(emitter)), events)
    }

    fn pair_target(stage_id: &StageId) -> PairTarget {
        PairTarget {
            stage_id:   stage_id.clone(),
            node_label: stage_id.node_id().to_string(),
        }
    }

    fn attach(
        hub: &SteeringHub,
        stage: &StageId,
        session_id: &str,
        handle: &Arc<SessionControlHandle>,
    ) {
        hub.attach(
            stage,
            session_id,
            Arc::clone(handle) as Arc<dyn SteerableSession>,
        )
        .expect("attaches");
    }

    #[test]
    fn deliver_with_no_active_buffers_message() {
        let (hub, names) = hub_with_event_names();
        hub.deliver_steer(
            "hi".into(),
            Some(Principal::System {
                system_kind: SystemActorKind::Engine,
            }),
        );
        assert_eq!(hub.pending_len(), 1);
        assert_eq!(names.lock().unwrap().as_slice(), [
            "run.steer",
            "agent.steer.buffered"
        ]);
    }

    #[test]
    fn drain_pending_at_run_end_reports_the_unread_steers_once() {
        let (hub, events) = hub_with_events();
        hub.deliver_steer("a".into(), None);
        hub.deliver_steer("b".into(), None);
        hub.drain_pending_at_run_end();
        assert_eq!(hub.pending_len(), 0);
        let events = events.lock().unwrap();
        let dropped = events
            .iter()
            .filter(|event| event.event_name() == "agent.steer.dropped")
            .collect::<Vec<_>>();
        assert_eq!(dropped.len(), 1);
    }

    #[test]
    fn attach_and_drain_pending_delivers_to_the_first_session() {
        let hub = SteeringHub::for_tests();
        hub.deliver_steer("queued1".into(), None);
        hub.deliver_steer("queued2".into(), None);

        let stage = StageId::new("agent-node", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage, "session-a", &handle);
        hub.drain_pending_into(&stage);

        assert_eq!(handle.queue_len(), 2);
        assert_eq!(hub.pending_len(), 0);
        assert_eq!(hub.active_count(), 1);
    }

    #[test]
    fn deliver_broadcasts_to_pebble_and_acp_sessions_alike() {
        let hub = SteeringHub::for_tests();
        let api_stage = StageId::new("api", 1);
        let acp_stage = StageId::new("acp", 1);
        let api_handle = SessionControlHandle::new();
        let acp_handle = SessionControlHandle::unpairable();
        attach(&hub, &api_stage, "session-api", &api_handle);
        attach(&hub, &acp_stage, "session-acp", &acp_handle);

        hub.deliver_steer("hello".into(), None);
        hub.interrupt(None);

        assert_eq!(api_handle.queue_len(), 1);
        assert_eq!(acp_handle.queue_len(), 1);
        assert_eq!(acp_handle.interrupt_count(), 1);
        assert_eq!(hub.pending_len(), 0);
    }

    #[test]
    fn a_steer_a_session_evicted_is_recorded_against_its_stage() {
        let (hub, events) = hub_with_events();
        let stage = StageId::new("a", 1);
        let handle = Arc::new(SessionControlHandle {
            queue:       Mutex::new(VecDeque::new()),
            capacity:    1,
            interrupted: AtomicUsize::new(0),
            pairable:    true,
        });
        attach(&hub, &stage, "session-a", &handle);

        hub.deliver_steer(
            "first".into(),
            Some(Principal::System {
                system_kind: SystemActorKind::Engine,
            }),
        );
        hub.deliver_steer("second".into(), None);

        assert_eq!(handle.queue_len(), 1);
        let events = events.lock().unwrap();
        let dropped = events
            .iter()
            .find(|event| event.event_name() == "agent.steer.dropped")
            .expect("the eviction is recorded");
        assert_eq!(dropped.node_id.as_deref(), Some("a"));
        assert_eq!(
            dropped.actor,
            Some(Principal::System {
                system_kind: SystemActorKind::Engine,
            }),
            "a system author survives the round trip through pebble's actor"
        );
    }

    #[test]
    fn detach_if_idle_respects_session_id_and_queue_state() {
        let hub = SteeringHub::for_tests();
        let stage = StageId::new("a", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage, "session-a", &handle);

        assert!(!hub.detach_if_idle(&stage, "session-b"));
        hub.deliver_steer("queued".into(), None);
        assert!(!hub.detach_if_idle(&stage, "session-a"));
        assert_eq!(hub.active_count(), 1);
        handle.queue.lock().unwrap().clear();
        assert!(hub.detach_if_idle(&stage, "session-a"));
        assert_eq!(hub.active_count(), 0);
    }

    #[test]
    fn pure_interrupt_marks_active_sessions_waiting_without_queueing_text() {
        let (hub, events) = hub_with_events();
        let stage = StageId::new("a", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage, "session-a", &handle);

        hub.interrupt(None);
        hub.interrupt(None);

        assert_eq!(handle.interrupt_count(), 2);
        assert_eq!(handle.queue_len(), 0);
        assert_eq!(hub.pending_len(), 0);
        let events = events.lock().unwrap();
        let names = events.iter().map(RunEvent::event_name).collect::<Vec<_>>();
        assert_eq!(names, [
            "run.interrupt",
            "agent.interrupt.injected",
            "run.interrupt",
            "agent.interrupt.injected",
        ]);
        assert_eq!(events[1].stage_id, Some(stage.clone()));
        assert_eq!(events[1].session_id.as_deref(), Some("session-a"));
        assert_eq!(events[3].stage_id, Some(stage));
        assert_eq!(events[3].session_id.as_deref(), Some("session-a"));
    }

    #[test]
    fn an_interrupt_with_no_session_emits_nothing() {
        let (hub, names) = hub_with_event_names();
        hub.interrupt(None);
        hub.interrupt_then_steer("stop", None);
        assert!(names.lock().unwrap().is_empty());
        assert_eq!(hub.pending_len(), 0, "an interrupt is not buffered");
    }

    #[test]
    fn interrupt_then_steer_cancels_and_queues_text() {
        let (hub, events) = hub_with_events();
        let stage = StageId::new("a", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage, "session-a", &handle);

        hub.interrupt_then_steer("stop", None);

        assert_eq!(handle.interrupt_count(), 1);
        assert_eq!(handle.queue_len(), 1);
        assert_eq!(hub.pending_len(), 0);
        let events = events.lock().unwrap();
        let names = events.iter().map(RunEvent::event_name).collect::<Vec<_>>();
        assert_eq!(names, [
            "run.interrupt",
            "run.steer",
            "agent.interrupt.injected",
        ]);
        assert_eq!(events[2].stage_id, Some(stage));
        assert_eq!(events[2].session_id.as_deref(), Some("session-a"));
    }

    #[test]
    fn pair_start_message_and_end_emit_typed_events_for_selected_target() {
        let (hub, events) = hub_with_events();
        let stage_id = StageId::new("code", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage_id, "ses_01", &handle);
        let pair_id = PairId::new();

        let started = hub
            .start_pair(RunId::new(), pair_id, pair_target(&stage_id), None)
            .unwrap();
        assert_eq!(started.status, fabro_types::PairStatus::Active);
        assert_eq!(handle.queue_len(), 1);
        assert_eq!(
            handle.interrupt_count(),
            1,
            "the paired session alone is told"
        );
        assert!(hub.pair_is_active_for(&stage_id, "ses_01"));

        let message = hub
            .send_pair_message(
                pair_id,
                PairMessageId::new(),
                "please inspect this".to_string(),
                Some("client-1".to_string()),
                None,
            )
            .unwrap();
        assert_eq!(message.text, "please inspect this");
        assert_eq!(handle.queue_len(), 2);

        let ended = hub.end_pair(pair_id, None).unwrap();
        assert_eq!(ended.status, fabro_types::PairStatus::Ended);
        assert!(!hub.pair_is_active_for(&stage_id, "ses_01"));
        assert_eq!(handle.queue_len(), 3);

        let names = events
            .lock()
            .unwrap()
            .iter()
            .map(|event| event.event_name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, [
            "run.pair.started",
            "agent.pair.system_message",
            "agent.pair.user_message",
            "agent.pair.system_message",
            "run.pair.ended"
        ]);
    }

    #[test]
    fn pair_start_rejects_missing_or_unpairable_targets_and_a_second_pair() {
        let hub = SteeringHub::for_tests();
        let stage_id = StageId::new("code", 1);
        let acp_stage = StageId::new("acp", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage_id, "ses_01", &handle);
        attach(
            &hub,
            &acp_stage,
            "ses_acp",
            &SessionControlHandle::unpairable(),
        );

        let missing_stage = StageId::new("other", 1);
        assert_eq!(
            hub.start_pair(
                RunId::new(),
                PairId::new(),
                pair_target(&missing_stage),
                None
            )
            .unwrap_err(),
            PairControlError::TargetNotActive
        );
        assert_eq!(
            hub.start_pair(RunId::new(), PairId::new(), pair_target(&acp_stage), None)
                .unwrap_err(),
            PairControlError::TargetNotActive,
            "a session that cannot be held open cannot be paired with"
        );
        hub.start_pair(RunId::new(), PairId::new(), pair_target(&stage_id), None)
            .unwrap();
        assert_eq!(
            hub.start_pair(RunId::new(), PairId::new(), pair_target(&stage_id), None)
                .unwrap_err(),
            PairControlError::AlreadyPaired
        );
    }

    #[test]
    fn detach_ends_active_pair_for_session() {
        let (hub, events) = hub_with_events();
        let stage_id = StageId::new("code", 1);
        let handle = SessionControlHandle::new();
        attach(&hub, &stage_id, "ses_01", &handle);
        let pair_id = PairId::new();
        hub.start_pair(RunId::new(), pair_id, pair_target(&stage_id), None)
            .unwrap();

        assert!(hub.detach(&stage_id, "ses_01"));

        assert!(!hub.pair_is_active_for(&stage_id, "ses_01"));
        let names = events
            .lock()
            .unwrap()
            .iter()
            .map(|event| event.event_name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, [
            "run.pair.started",
            "agent.pair.system_message",
            "run.pair.ended"
        ]);
    }
}
