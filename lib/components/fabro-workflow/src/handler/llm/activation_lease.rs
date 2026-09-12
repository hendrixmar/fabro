//! A stage's session on the steering bus, with fabro's lifecycle events.
//!
//! Activating attaches the session at its stage, records
//! `agent.session.activated` with the route and capabilities the run should
//! show, and then drains steers that waited for it. Releasing detaches and
//! records `agent.session.deactivated` once, however many times it is asked.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fabro_types::{PermissionLevel, SessionCapability, StageId};
use lithos_llm::types::{ReasoningEffort, Speed};
use pebble_coding_agent::steering::SteerableSession;

use crate::error::Error;
use crate::event::{Emitter, Event};
use crate::steering_hub::SteeringHub;

pub struct ActivationLease {
    stage_id:   StageId,
    session_id: String,
    hub:        Arc<SteeringHub>,
    emitter:    Arc<Emitter>,
    released:   AtomicBool,
}

pub struct ActivationLeaseOptions {
    pub stage_id:         StageId,
    pub session_id:       String,
    pub thread_id:        Option<String>,
    pub provider:         Option<String>,
    pub model:            Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub speed:            Option<Speed>,
    pub permission_level: Option<PermissionLevel>,
    pub capabilities:     Vec<SessionCapability>,
    pub hub:              Arc<SteeringHub>,
    pub emitter:          Arc<Emitter>,
}

impl ActivationLease {
    pub fn activate(
        options: ActivationLeaseOptions,
        session: Arc<dyn SteerableSession>,
    ) -> Result<Arc<Self>, Error> {
        options
            .hub
            .attach(&options.stage_id, &options.session_id, session)
            .map_err(|_| {
                Error::Precondition(format!(
                    "stage {} already has a different active agent session",
                    options.stage_id
                ))
            })?;

        options.emitter.emit(&Event::AgentSessionActivated {
            node_id:          options.stage_id.node_id().to_string(),
            visit:            options.stage_id.visit(),
            session_id:       options.session_id.clone(),
            thread_id:        options.thread_id,
            provider:         options.provider,
            model:            options.model,
            reasoning_effort: options.reasoning_effort,
            speed:            options.speed,
            permission_level: options.permission_level,
            capabilities:     options.capabilities,
        });
        options.hub.drain_pending_into(&options.stage_id);

        Ok(Arc::new(Self {
            stage_id:   options.stage_id,
            session_id: options.session_id,
            hub:        options.hub,
            emitter:    options.emitter,
            released:   AtomicBool::new(false),
        }))
    }

    pub fn release(&self) {
        if !self.mark_released() {
            return;
        }
        self.hub.detach(&self.stage_id, &self.session_id);
    }

    /// The close-the-door check: release only if the session has no steering
    /// waiting. Returns whether the lease is released.
    pub fn release_if_idle(&self) -> bool {
        if self.released.load(Ordering::Acquire) {
            return true;
        }
        if !self.hub.detach_if_idle(&self.stage_id, &self.session_id) {
            return false;
        }
        self.mark_released();
        true
    }

    pub fn is_pair_active(&self) -> bool {
        !self.released.load(Ordering::Acquire)
            && self
                .hub
                .pair_is_active_for(&self.stage_id, &self.session_id)
    }

    fn mark_released(&self) -> bool {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.emitter.emit(&Event::AgentSessionDeactivated {
            node_id:    self.stage_id.node_id().to_string(),
            visit:      self.stage_id.visit(),
            session_id: self.session_id.clone(),
        });
        true
    }
}

impl Drop for ActivationLease {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use fabro_types::RunId;
    use pebble_coding_agent::{SteeringMessage, SteeringOutcome};

    use super::*;

    #[derive(Default)]
    struct SessionControlHandle {
        queue: Mutex<Vec<SteeringMessage>>,
    }

    impl SessionControlHandle {
        fn queue_len(&self) -> usize {
            self.queue.lock().unwrap().len()
        }
    }

    impl SteerableSession for SessionControlHandle {
        fn steer(&self, message: SteeringMessage) -> SteeringOutcome {
            self.queue.lock().unwrap().push(message);
            SteeringOutcome::Accepted
        }

        fn interrupt(&self) -> bool {
            false
        }

        fn steer_now(&self, message: SteeringMessage) -> SteeringOutcome {
            self.steer(message)
        }

        fn has_pending_steering(&self) -> bool {
            !self.queue.lock().unwrap().is_empty()
        }
    }

    fn collect_event_names(emitter: &Arc<Emitter>) -> Arc<Mutex<Vec<String>>> {
        let names = Arc::new(Mutex::new(Vec::new()));
        let names_for_listener = Arc::clone(&names);
        emitter.on_event(move |event| {
            names_for_listener
                .lock()
                .unwrap()
                .push(event.event_name().to_string());
        });
        names
    }

    fn options(
        stage_id: StageId,
        session_id: &str,
        hub: Arc<SteeringHub>,
        emitter: Arc<Emitter>,
    ) -> ActivationLeaseOptions {
        ActivationLeaseOptions {
            stage_id,
            session_id: session_id.to_string(),
            thread_id: None,
            provider: Some("openai".to_string()),
            model: Some("gpt-5.4".to_string()),
            reasoning_effort: None,
            speed: None,
            permission_level: None,
            capabilities: vec![SessionCapability::Steer],
            hub,
            emitter,
        }
    }

    fn session(handle: &Arc<SessionControlHandle>) -> Arc<dyn SteerableSession> {
        Arc::clone(handle) as Arc<dyn SteerableSession>
    }

    #[test]
    fn activate_emits_activated_before_draining_pending() {
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let names = collect_event_names(&emitter);
        let hub = Arc::new(SteeringHub::new(Arc::clone(&emitter)));
        let stage_id = StageId::new("agent", 1);
        let handle = Arc::new(SessionControlHandle::default());

        hub.deliver_steer("queued".to_string(), None);
        let _lease = ActivationLease::activate(
            options(
                stage_id.clone(),
                "session-a",
                Arc::clone(&hub),
                Arc::clone(&emitter),
            ),
            session(&handle),
        )
        .unwrap();

        assert_eq!(handle.queue_len(), 1);
        assert_eq!(names.lock().unwrap().as_slice(), [
            "run.steer",
            "agent.steer.buffered",
            "agent.session.activated"
        ]);
    }

    #[test]
    fn activate_rejects_mismatched_existing_session() {
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let names = collect_event_names(&emitter);
        let hub = Arc::new(SteeringHub::new(Arc::clone(&emitter)));
        let stage_id = StageId::new("agent", 1);
        let handle_a = Arc::new(SessionControlHandle::default());
        let handle_b = Arc::new(SessionControlHandle::default());

        let _lease = ActivationLease::activate(
            options(
                stage_id.clone(),
                "session-a",
                Arc::clone(&hub),
                Arc::clone(&emitter),
            ),
            session(&handle_a),
        )
        .unwrap();
        let result = ActivationLease::activate(
            options(
                stage_id,
                "session-b",
                Arc::clone(&hub),
                Arc::clone(&emitter),
            ),
            session(&handle_b),
        );

        assert!(result.is_err());
        assert_eq!(handle_b.queue_len(), 0);
        assert_eq!(
            names
                .lock()
                .unwrap()
                .iter()
                .filter(|name| name.as_str() == "agent.session.activated")
                .count(),
            1
        );
    }

    #[test]
    fn release_is_idempotent_and_release_if_idle_waits_for_steering() {
        let emitter = Arc::new(Emitter::new(RunId::new()));
        let names = collect_event_names(&emitter);
        let hub = Arc::new(SteeringHub::new(Arc::clone(&emitter)));
        let stage_id = StageId::new("agent", 1);
        let handle = Arc::new(SessionControlHandle::default());

        let lease = ActivationLease::activate(
            options(
                stage_id,
                "session-a",
                Arc::clone(&hub),
                Arc::clone(&emitter),
            ),
            session(&handle),
        )
        .unwrap();
        hub.deliver_steer("late".to_string(), None);
        assert!(
            !lease.release_if_idle(),
            "a waiting steer keeps the door open"
        );
        handle.queue.lock().unwrap().clear();
        assert!(lease.release_if_idle());
        assert!(lease.release_if_idle(), "released stays released");
        lease.release();

        assert_eq!(
            names
                .lock()
                .unwrap()
                .iter()
                .filter(|name| name.as_str() == "agent.session.deactivated")
                .count(),
            1
        );
    }
}
