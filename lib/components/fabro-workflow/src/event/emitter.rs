use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ::fabro_types::{ExecOutputTail, RunEvent, RunId, RunNoticeCode, RunNoticeLevel};
use chrono::Utc;
use tokio::time::Instant;

use super::Event;
use super::convert::to_run_event_at;
use super::sink::{RunEventLogger, RunEventPersistenceError};
use crate::millis_u64;
use crate::stage_scope::StageScope;

/// Listener callback type for workflow run events.
type EventListener = Arc<dyn Fn(&RunEvent) + Send + Sync>;

/// Callback-based event emitter for workflow run events.
pub struct Emitter {
    run_id:           RunId,
    listeners:        std::sync::Mutex<Vec<EventListener>>,
    /// The persistence path. Events reach it before any listener, and
    /// [`Emitter::emit_durable`] waits for it.
    persisters:       std::sync::Mutex<Vec<RunEventLogger>>,
    /// Monotonic origin that `last_activity_ms` is measured from.
    activity_origin:  Instant,
    /// Milliseconds after `activity_origin` of the last `emit()` or `touch()`.
    /// 0 until the first event.
    last_activity_ms: AtomicU64,
}

impl std::fmt::Debug for Emitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.listeners.lock().map_or(0, |l| l.len());
        f.debug_struct("Emitter")
            .field("run_id", &self.run_id)
            .field("listener_count", &count)
            .field(
                "last_activity_ms",
                &self.last_activity_ms.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl Default for Emitter {
    fn default() -> Self {
        Self::new(RunId::new())
    }
}

impl Emitter {
    #[must_use]
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            listeners: std::sync::Mutex::new(Vec::new()),
            persisters: std::sync::Mutex::new(Vec::new()),
            activity_origin: Instant::now(),
            last_activity_ms: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn on_event(&self, listener: impl Fn(&RunEvent) + Send + Sync + 'static) {
        self.listeners
            .lock()
            .expect("listeners lock poisoned")
            .push(Arc::new(listener));
    }

    pub(super) fn attach_persistence(&self, logger: RunEventLogger) {
        self.persisters
            .lock()
            .expect("persisters lock poisoned")
            .push(logger);
    }

    pub fn emit(&self, event: &Event) {
        self.emit_with_scope(event, None);
    }

    /// Emits `event` and returns once every attached persistence path has
    /// accepted it.
    ///
    /// Listeners see the event only after it is durable. With no persistence
    /// attached the event is dispatched to listeners and the call succeeds.
    ///
    /// # Errors
    ///
    /// Returns the persistence failure that stopped the run event log.
    pub async fn emit_durable(
        &self,
        event: &Event,
        scope: Option<&StageScope>,
    ) -> Result<(), RunEventPersistenceError> {
        event.trace();
        let stored = to_run_event_at(&self.run_id, event, Utc::now(), scope);
        self.record_activity();
        let persisters: Vec<RunEventLogger> = self
            .persisters
            .lock()
            .expect("persisters lock poisoned")
            .clone();
        for persister in &persisters {
            persister.write_acknowledged(&stored).await?;
        }
        self.dispatch_to_listeners(&stored);
        Ok(())
    }

    pub fn emit_scoped(&self, event: &Event, scope: &StageScope) {
        self.emit_with_scope(event, Some(scope));
    }

    pub fn notice(&self, level: RunNoticeLevel, code: RunNoticeCode, message: impl Into<String>) {
        self.emit(&Event::RunNotice {
            level,
            code: code.to_string(),
            message: message.into(),
            exec_output_tail: None,
        });
    }

    pub fn notice_scoped(
        &self,
        level: RunNoticeLevel,
        code: RunNoticeCode,
        message: impl Into<String>,
        scope: &StageScope,
    ) {
        self.emit_scoped(
            &Event::RunNotice {
                level,
                code: code.to_string(),
                message: message.into(),
                exec_output_tail: None,
            },
            scope,
        );
    }

    pub fn notice_with_tail(
        &self,
        level: RunNoticeLevel,
        code: RunNoticeCode,
        message: impl Into<String>,
        exec_output_tail: Option<ExecOutputTail>,
    ) {
        self.emit(&Event::RunNotice {
            level,
            code: code.to_string(),
            message: message.into(),
            exec_output_tail,
        });
    }

    fn emit_with_scope(&self, event: &Event, scope: Option<&StageScope>) {
        event.trace();
        if let Event::WorkflowRunStarted { run_id, .. } = event {
            debug_assert_eq!(
                *run_id, self.run_id,
                "workflow run started event must match emitter run_id"
            );
        }
        let stored = to_run_event_at(&self.run_id, event, Utc::now(), scope);
        self.dispatch_run_event(&stored);
    }

    pub(crate) fn dispatch_run_event(&self, event: &RunEvent) {
        self.record_activity();
        let persisters: Vec<RunEventLogger> = self
            .persisters
            .lock()
            .expect("persisters lock poisoned")
            .clone();
        for persister in &persisters {
            persister.enqueue(event);
        }
        self.dispatch_to_listeners(event);
    }

    fn dispatch_to_listeners(&self, event: &RunEvent) {
        // Clone the listener list so we don't hold the lock during dispatch.
        // This prevents deadlocks if a listener calls emit() reentrantly.
        // Note: listeners added during this emit() won't receive the current event.
        let snapshot: Vec<EventListener> = self
            .listeners
            .lock()
            .expect("listeners lock poisoned")
            .clone();
        for listener in &snapshot {
            listener(event);
        }
    }

    /// Returns the monotonic instant of the last `emit()` or `touch()` call,
    /// or the emitter's creation instant if neither has been called.
    pub(crate) fn last_activity(&self) -> Instant {
        self.activity_origin + Duration::from_millis(self.last_activity_ms.load(Ordering::Relaxed))
    }

    /// Manually record activity (e.g. to seed the watchdog at workflow run
    /// start, or for agent stream deltas that are not emitted as run events).
    pub fn touch(&self) {
        self.record_activity();
    }

    /// Called for every event, including agent streaming deltas. Keep this to a
    /// single clock read and a relaxed store — the stall watchdog samples it at
    /// its own deadline rather than being woken here.
    fn record_activity(&self) {
        self.last_activity_ms.store(
            millis_u64(self.activity_origin.elapsed()),
            Ordering::Relaxed,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use ::fabro_types::fixtures;

    use super::*;
    use crate::event::Event;

    #[test]
    fn event_emitter_new_has_no_listeners() {
        let emitter = Emitter::new(fixtures::RUN_1);
        assert_eq!(emitter.listeners.lock().unwrap().len(), 0);
    }

    #[test]
    fn event_emitter_calls_listener_with_envelope() {
        let emitter = Emitter::new(fixtures::RUN_1);
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);
        emitter.on_event(move |event| {
            received_clone.lock().unwrap().push(event.clone());
        });
        emitter.emit(&Event::WorkflowRunStarted {
            name:         "test".to_string(),
            run_id:       fixtures::RUN_1,
            base_branch:  None,
            base_sha:     None,
            run_branch:   None,
            worktree_dir: None,
            goal:         None,
        });
        let events = received.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name(), "run.started");
        assert_eq!(events[0].run_id, fixtures::RUN_1);
        assert!(events[0].id.len() >= 32);
    }

    #[test]
    fn event_emitter_default() {
        let emitter = Emitter::default();
        assert_eq!(emitter.listeners.lock().unwrap().len(), 0);
    }
}
