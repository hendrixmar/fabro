mod convert;
mod driver_events;
mod emitter;
mod events;
mod names;
mod redaction;
mod sink;
mod stored_fields;
#[cfg(test)]
mod test_support;

pub use fabro_types::{EventBody, RunNoticeCode, RunNoticeLevel};

pub use self::convert::{to_run_event, to_run_event_at};
pub use self::driver_events::DriverEventRecorder;
pub use self::emitter::Emitter;
pub use self::events::{Event, SandboxLifecycle};
pub use self::names::event_name;
pub use self::redaction::{
    build_redacted_event_payload, event_payload_from_redacted_json, redacted_event_json,
};
pub use self::sink::{
    RunEventLogger, RunEventPersistenceError, RunEventSink, StoreProgressLogger, append_event,
    append_event_if, append_event_to_sink, create_run,
};
pub use self::stored_fields::{actor_from_principal, principal_from_actor};
pub use crate::stage_scope::StageScope;
