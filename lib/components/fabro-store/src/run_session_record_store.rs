//! SQLite storage for Ask Fabro conversations.
//!
//! The run event log streams a session's turns live and projects its
//! metadata; the conversation the model sees is pebble's session record,
//! kept here as JSON and written after every turn. Resuming a session reads
//! the record back and continues it on the model it recorded.

use chrono::{DateTime, Utc};
use fabro_types::{RunId, SessionId};
use pebble_coding_agent::state::SessionRecord;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row as _, SqlitePool};

use crate::{Error, Result, sqlite_row};

const RECORD_NAME: &str = "run session record";

/// A stored conversation and when it was last written.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredSessionRecord {
    pub run_id:     RunId,
    pub record:     SessionRecord,
    pub updated_at: DateTime<Utc>,
}

/// Reads and writes pebble session records in SQLite.
pub struct RunSessionRecordStore {
    pool: SqlitePool,
}

impl std::fmt::Debug for RunSessionRecordStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunSessionRecordStore")
            .finish_non_exhaustive()
    }
}

impl RunSessionRecordStore {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Replace the stored record for `session_id`.
    pub async fn put(
        &self,
        session_id: SessionId,
        run_id: RunId,
        record: &SessionRecord,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let record_json = serde_json::to_string(record)?;
        sqlx::query(
            r"
INSERT INTO run_session_records (session_id, run_id, record_json, updated_at_ms)
VALUES (?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
    run_id = excluded.run_id,
    record_json = excluded.record_json,
    updated_at_ms = excluded.updated_at_ms
",
        )
        .bind(session_id.to_string())
        .bind(run_id.to_string())
        .bind(record_json)
        .bind(updated_at.timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The stored record for `session_id`, if a turn has been persisted.
    pub async fn get(&self, session_id: SessionId) -> Result<Option<StoredSessionRecord>> {
        let row = sqlx::query(
            "SELECT run_id, record_json, updated_at_ms FROM run_session_records WHERE session_id = ?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(record_from_row).transpose()
    }

    /// Forget the stored record for `session_id`.
    pub async fn delete(&self, session_id: SessionId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM run_session_records WHERE session_id = ?")
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

fn record_from_row(row: &SqliteRow) -> Result<StoredSessionRecord> {
    let run_id: String = row.try_get("run_id")?;
    let run_id = run_id
        .parse::<RunId>()
        .map_err(|error| Error::InvalidEvent(format!("stored {RECORD_NAME} run id: {error}")))?;
    let record_json: String = row.try_get("record_json")?;
    let record: SessionRecord = serde_json::from_str(&record_json)?;
    let updated_at = sqlite_row::timestamp_from_row(row, RECORD_NAME, "updated_at_ms")?;
    Ok(StoredSessionRecord {
        run_id,
        record,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use fabro_types::fixtures;
    use pebble_coding_agent::SessionScope;

    use super::*;
    use crate::test_support;

    fn record(session_id: &SessionId) -> SessionRecord {
        let mut record = SessionRecord::new(SessionScope::root(
            pebble_coding_agent::SessionId::new(session_id.to_string()),
        ));
        record.provider = Some("openai".to_string());
        record.model = Some("gpt-5.4".to_string());
        record.last_event_seq = 7;
        // The record stores timestamps at millisecond precision, so a fixture
        // that expects to read back what it wrote must not carry finer ones.
        let millis = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_789_156_874_678);
        record.created_at = millis;
        record.updated_at = millis;
        record
    }

    #[tokio::test]
    async fn put_then_get_round_trips_the_record() {
        let store = RunSessionRecordStore::new(test_support::in_memory_pool_with(&[
            fabro_db::RUN_SESSION_RECORDS_MIGRATION_SQL,
        ]));
        let session_id = SessionId::new();
        let record = record(&session_id);
        let updated_at = Utc.with_ymd_and_hms(2026, 9, 11, 12, 0, 0).unwrap();

        store
            .put(session_id, fixtures::RUN_1, &record, updated_at)
            .await
            .unwrap();

        let stored = store.get(session_id).await.unwrap().expect("stored record");
        assert_eq!(stored.run_id, fixtures::RUN_1);
        assert_eq!(stored.record, record);
        assert_eq!(stored.updated_at, updated_at);
    }

    #[tokio::test]
    async fn put_replaces_an_earlier_record() {
        let store = RunSessionRecordStore::new(test_support::in_memory_pool_with(&[
            fabro_db::RUN_SESSION_RECORDS_MIGRATION_SQL,
        ]));
        let session_id = SessionId::new();
        let first = record(&session_id);
        let mut second = first.clone();
        second.last_event_seq = 12;
        let now = Utc::now();

        store
            .put(session_id, fixtures::RUN_1, &first, now)
            .await
            .unwrap();
        store
            .put(session_id, fixtures::RUN_1, &second, now)
            .await
            .unwrap();

        let stored = store.get(session_id).await.unwrap().expect("stored record");
        assert_eq!(stored.record.last_event_seq, 12);
        assert!(store.delete(session_id).await.unwrap());
        assert!(store.get(session_id).await.unwrap().is_none());
    }
}
