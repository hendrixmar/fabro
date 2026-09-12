use chrono::{DateTime, Utc};
use lithos_llm::catalog::ProviderId;
pub use pebble_coding_agent::events::PermissionLevel;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::RunId;
use crate::id::ulid_id;

ulid_id!(SessionId);
ulid_id!(TurnId);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    Failed,
}

impl SessionStatus {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTurn {
    pub id:         TurnId,
    pub started_at: DateTime<Utc>,
    pub input:      String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Ask Fabro session metadata derived from the owning run event stream.
pub struct RunSessionMetadata {
    pub id:          SessionId,
    pub run_id:      RunId,
    pub title:       Option<String>,
    pub status:      SessionStatus,
    pub model:       Option<String>,
    #[serde(default)]
    pub provider:    Option<ProviderId>,
    #[serde(default)]
    pub active_turn: Option<SessionTurn>,
    pub created_at:  DateTime<Utc>,
    pub updated_at:  DateTime<Utc>,
}

impl RunSessionMetadata {
    pub fn new(id: SessionId, run_id: RunId, now: DateTime<Utc>) -> Self {
        Self {
            id,
            run_id,
            title: None,
            status: SessionStatus::Idle,
            model: None,
            provider: None,
            active_turn: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id:          SessionId,
    pub run_id:      RunId,
    pub title:       Option<String>,
    pub status:      SessionStatus,
    pub model:       Option<String>,
    #[serde(default)]
    pub provider:    Option<ProviderId>,
    #[serde(default)]
    pub active_turn: Option<SessionTurn>,
    pub created_at:  DateTime<Utc>,
    pub updated_at:  DateTime<Utc>,
}

impl From<&RunSessionMetadata> for SessionSummary {
    fn from(record: &RunSessionMetadata) -> Self {
        Self {
            id:          record.id,
            run_id:      record.run_id,
            title:       record.title.clone(),
            status:      record.status,
            model:       record.model.clone(),
            provider:    record.provider.clone(),
            active_turn: record.active_turn.clone(),
            created_at:  record.created_at,
            updated_at:  record.updated_at,
        }
    }
}

/// Session metadata plus the event-log position the projection was read at.
///
/// The transcript itself is not part of the API: pebble's session record
/// holds the durable history and the `run.session.*` events stream it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDetail {
    #[serde(flatten)]
    pub record:   RunSessionMetadata,
    pub last_seq: u32,
}

impl SessionDetail {
    pub fn new(record: RunSessionMetadata, last_seq: u32) -> Self {
        Self { record, last_seq }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::{RunSessionMetadata, SessionId, SessionStatus};
    use crate::fixtures;

    #[test]
    fn session_status_rejects_removed_terminal_states() {
        assert!(serde_json::from_value::<SessionStatus>(json!("closed")).is_err());
        assert!(serde_json::from_value::<SessionStatus>(json!("deleted")).is_err());
    }

    #[test]
    fn session_record_deserializes_legacy_json_without_provider() {
        let mut value = serde_json::to_value(RunSessionMetadata::new(
            SessionId::new(),
            fixtures::RUN_1,
            Utc::now(),
        ))
        .unwrap();
        value
            .as_object_mut()
            .expect("session record should serialize as an object")
            .remove("provider");

        let record: RunSessionMetadata = serde_json::from_value(value).unwrap();

        assert_eq!(record.provider, None);
    }
}
