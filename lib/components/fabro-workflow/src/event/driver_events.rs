//! The sandbox driver's events for a run's sandbox, kept as run events.
//!
//! A run's sandbox is created or attached with a driver [`EventContext`]
//! whose observer is a [`DriverEventRecorder`]. Everything the driver
//! reports about the sandbox — the operations it performs and their
//! outcome, progress inside a create such as an image pull, snapshot
//! builds, state observations, notices — is stored whole as an
//! [`Event::SandboxDriver`], named from the event (see
//! `fabro_types::sandbox_driver_event_name`).
//!
//! [`EventContext`]: sandbox_driver::EventContext

use std::sync::Arc;

use async_trait::async_trait;
use sandbox_driver::{Event as DriverEvent, EventObserver};

use super::{Emitter, Event};

/// Records every event the driver reports as a run event.
pub struct DriverEventRecorder {
    emitter: Arc<Emitter>,
}

impl DriverEventRecorder {
    pub fn new(emitter: Arc<Emitter>) -> Self {
        Self { emitter }
    }
}

#[async_trait]
impl EventObserver for DriverEventRecorder {
    async fn observe(&self, event: DriverEvent) {
        self.emitter.emit(&Event::SandboxDriver { event });
    }
}
