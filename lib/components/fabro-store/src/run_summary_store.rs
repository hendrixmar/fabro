use std::fmt::Write as _;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use fabro_types::{
    BilledTokenCounts, EventEnvelope, Run, RunEvent, RunId, RunSize, RunStatusKind, RunTiming,
    SessionId, StageId, timing,
};
use sqlx::pool::PoolConnection;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteConnection, SqliteRow};
use sqlx::{Connection as _, QueryBuilder, Row as _, Sqlite, SqlitePool, Transaction};
use strum::VariantArray as _;

use crate::run_state::{ProjectedRun, build_summary, projected_billing};
use crate::{Error, EventPayload, Result, keys};

const INSERT_RUN_SQL: &str = r"
INSERT INTO runs (
    id, source_last_seq, created_at_ms, started_at_ms, last_event_at_ms, completed_at_ms,
    status, archived_at_ms, parent_id, title, workflow_slug, workflow_name,
    repository_name, automation_id, diff_files_changed, diff_additions, diff_deletions,
    input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens,
    total_usd_micros, summary_json
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
";

#[cfg(test)]
const UPSERT_RUN_SQL: &str = r"
INSERT INTO runs (
    id, source_last_seq, created_at_ms, started_at_ms, last_event_at_ms, completed_at_ms,
    status, archived_at_ms, parent_id, title, workflow_slug, workflow_name,
    repository_name, automation_id, diff_files_changed, diff_additions, diff_deletions,
    input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens,
    total_usd_micros, summary_json
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(id) DO UPDATE SET
    source_last_seq = excluded.source_last_seq,
    created_at_ms = excluded.created_at_ms,
    started_at_ms = excluded.started_at_ms,
    last_event_at_ms = excluded.last_event_at_ms,
    completed_at_ms = excluded.completed_at_ms,
    status = excluded.status,
    archived_at_ms = excluded.archived_at_ms,
    parent_id = excluded.parent_id,
    title = excluded.title,
    workflow_slug = excluded.workflow_slug,
    workflow_name = excluded.workflow_name,
    repository_name = excluded.repository_name,
    automation_id = excluded.automation_id,
    diff_files_changed = excluded.diff_files_changed,
    diff_additions = excluded.diff_additions,
    diff_deletions = excluded.diff_deletions,
    input_tokens = excluded.input_tokens,
    output_tokens = excluded.output_tokens,
    reasoning_tokens = excluded.reasoning_tokens,
    cache_read_tokens = excluded.cache_read_tokens,
    cache_write_tokens = excluded.cache_write_tokens,
    total_usd_micros = excluded.total_usd_micros,
    summary_json = excluded.summary_json
WHERE excluded.source_last_seq > runs.source_last_seq
";

const UPDATE_RUN_SQL: &str = r"
UPDATE runs SET
    source_last_seq = ?,
    created_at_ms = ?,
    started_at_ms = ?,
    last_event_at_ms = ?,
    completed_at_ms = ?,
    status = ?,
    archived_at_ms = ?,
    parent_id = ?,
    title = ?,
    workflow_slug = ?,
    workflow_name = ?,
    repository_name = ?,
    automation_id = ?,
    diff_files_changed = ?,
    diff_additions = ?,
    diff_deletions = ?,
    input_tokens = ?,
    output_tokens = ?,
    reasoning_tokens = ?,
    cache_read_tokens = ?,
    cache_write_tokens = ?,
    total_usd_micros = ?,
    summary_json = ?
WHERE id = ? AND source_last_seq = ?
";

const SELECT_EVENT_COLUMNS: &str =
    "SELECT run_id, seq, event_name, node_id, stage_id, session_id, event_json FROM run_events";

const INSERT_EVENT_SQL: &str = r"
INSERT INTO run_events (run_id, seq, event_name, node_id, stage_id, session_id, event_json)
VALUES (?, ?, ?, ?, ?, ?, ?)
";

const SELECT_RUN_SUMMARIES_SQL: &str = r"
SELECT runs.id, runs.summary_json,
       (SELECT COUNT(*) FROM runs AS child WHERE child.parent_id = runs.id) AS children_count
FROM runs";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunSummarySort {
    #[default]
    CreatedAt,
    UpdatedAt,
    Status,
    Elapsed,
    #[serde(rename = "repo")]
    Repository,
    Title,
    Workflow,
    Changes,
    Size,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunSummarySortDirection {
    Asc,
    #[default]
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunSummaryVisibility {
    All,
    Default {
        include_archived: bool,
    },
    Selected {
        statuses: Vec<RunStatusKind>,
        archived: bool,
    },
}

impl Default for RunSummaryVisibility {
    fn default() -> Self {
        Self::Default {
            include_archived: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummaryListQuery {
    pub parent_id:     Option<RunId>,
    pub automation_id: Option<String>,
    pub visibility:    RunSummaryVisibility,
    pub sort:          RunSummarySort,
    pub direction:     RunSummarySortDirection,
    pub limit:         u32,
    pub offset:        u32,
}

impl Default for RunSummaryListQuery {
    fn default() -> Self {
        Self {
            parent_id:     None,
            automation_id: None,
            visibility:    RunSummaryVisibility::default(),
            sort:          RunSummarySort::default(),
            direction:     RunSummarySortDirection::default(),
            limit:         100,
            offset:        0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunSummaryPage {
    pub data:     Vec<Run>,
    pub total:    u64,
    pub has_more: bool,
}

#[derive(Clone)]
pub struct RunSummaryStore {
    pool: SqlitePool,
}

impl std::fmt::Debug for RunSummaryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunSummaryStore").finish_non_exhaustive()
    }
}

impl RunSummaryStore {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    #[cfg(test)]
    pub(crate) async fn close_pool(&self) {
        self.pool.close().await;
    }

    /// Corrupts one fixture history while preserving its current row so
    /// cross-crate repair-endpoint tests can exercise unreadable SQL runs.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn test_delete_run_events(&self, run_id: &RunId) -> Result<()> {
        sqlx::query("DELETE FROM run_events WHERE run_id = ?")
            .bind(run_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Inserts a fixture event without reducing it into the current row.
    ///
    /// This deliberately creates an unreadable history for cross-crate repair
    /// endpoint tests. It is never linked into production builds.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn test_insert_unvalidated_event(
        &self,
        run_id: &RunId,
        seq: u32,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let payload = EventPayload::new(payload.clone(), run_id)?;
        let event = RunEvent::try_from(&payload)?;
        let envelope = EventEnvelope { seq, event };
        let event_json = serde_json::to_string(&payload)?;
        let mut transaction = self.pool.begin().await?;
        insert_event_json_on_connection(&mut transaction, run_id, &envelope, &event_json).await?;
        let updated = sqlx::query("UPDATE runs SET source_last_seq = ? WHERE id = ?")
            .bind(i64::from(seq))
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;
        if updated.rows_affected() != 1 {
            return Err(Error::RunNotFound(run_id.to_string()));
        }
        transaction.commit().await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn test_mark_run_history_activated(&self) -> Result<()> {
        sqlx::query(
            r"
INSERT INTO legacy_run_history_activation (
    singleton, source_fingerprint, source_runs, source_events, activated_at_ms
) VALUES (1, zeroblob(32), 0, 0, 1)
ON CONFLICT(singleton) DO NOTHING
",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn test_is_run_history_tombstoned(&self, run_id: &RunId) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM legacy_run_history_deletions WHERE run_id = ?)",
        )
        .bind(run_id.to_string())
        .fetch_one(&self.pool)
        .await?)
    }

    pub(crate) async fn acquire(&self) -> Result<PoolConnection<Sqlite>> {
        Ok(self.pool.acquire().await?)
    }

    pub(crate) async fn begin(&self) -> Result<Transaction<'static, Sqlite>> {
        Ok(self.pool.begin().await?)
    }

    pub(crate) async fn contains(&self, run_id: &RunId) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id = ?)")
                .bind(run_id.to_string())
                .fetch_one(&self.pool)
                .await?,
        )
    }

    #[cfg(test)]
    pub(crate) async fn upsert_projection(&self, entry: &ProjectedRun) -> Result<()> {
        let record = PreparedRunSummary::from_entry(entry);
        let mut connection = self.pool.acquire().await?;
        upsert_run_on_connection(&mut connection, &record).await
    }

    #[cfg(test)]
    pub(crate) async fn reconcile(&self, entries: &[ProjectedRun]) -> Result<()> {
        use std::collections::{HashMap, HashSet};

        let mut transaction = self.pool.begin().await?;
        let stored_seqs: HashMap<String, i64> =
            sqlx::query_as::<_, (String, i64)>("SELECT id, source_last_seq FROM runs")
                .fetch_all(&mut *transaction)
                .await?
                .into_iter()
                .collect();

        let mut authoritative_ids = HashSet::new();
        for entry in entries {
            let run_id = entry.run_id.to_string();
            let up_to_date = stored_seqs
                .get(&run_id)
                .is_some_and(|stored_seq| *stored_seq >= i64::from(entry.last_seq));
            authoritative_ids.insert(run_id);
            if up_to_date {
                continue;
            }
            upsert_run_on_connection(&mut transaction, &PreparedRunSummary::from_entry(entry))
                .await?;
        }

        let stale_ids = stored_seqs
            .keys()
            .filter(|stored_id| !authoritative_ids.contains(stored_id.as_str()))
            .collect::<Vec<_>>();
        for chunk in stale_ids.chunks(500) {
            let mut delete = QueryBuilder::<Sqlite>::new("DELETE FROM runs WHERE id IN (");
            let mut separated = delete.separated(", ");
            for stale_id in chunk {
                separated.push_bind(stale_id.as_str());
            }
            delete.push(")");
            delete.build().execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn list_run_ids(&self) -> Result<Vec<RunId>> {
        sqlx::query_scalar::<_, String>("SELECT id FROM runs ORDER BY id ASC")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(parse_stored_run_id)
            .collect()
    }

    pub(crate) async fn head(&self, run_id: &RunId) -> Result<Option<u32>> {
        let mut connection = self.acquire().await?;
        select_run_head(&mut connection, run_id).await
    }

    /// Replays one run's canonical history from one validated SQLite snapshot.
    /// Fails with `RunNotFound` when the run does not exist.
    pub(crate) async fn load_projection(&self, run_id: &RunId) -> Result<ProjectedRun> {
        let events = self.list_events_for_run(run_id).await?;
        ProjectedRun::replay(*run_id, &events)
    }

    pub(crate) async fn list_events_for_run(&self, run_id: &RunId) -> Result<Vec<EventEnvelope>> {
        let mut connection = self.acquire().await?;
        Self::list_events_on_connection(&mut connection, run_id).await
    }

    pub(crate) async fn list_events_from_with_limit(
        &self,
        run_id: &RunId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut connection = self.acquire().await?;
        Self::list_events_from_with_limit_on_connection(&mut connection, run_id, start_seq, limit)
            .await
    }

    pub(crate) async fn list_events_before_with_limit(
        &self,
        run_id: &RunId,
        before_seq: Option<u32>,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut connection = self.acquire().await?;
        Self::list_events_before_with_limit_on_connection(
            &mut connection,
            run_id,
            before_seq,
            limit,
        )
        .await
    }

    pub(crate) async fn get_event_for_run(
        &self,
        run_id: &RunId,
        seq: u32,
    ) -> Result<Option<EventEnvelope>> {
        let mut connection = self.acquire().await?;
        Self::get_event_on_connection(&mut connection, run_id, seq).await
    }

    pub(crate) async fn list_events_for_stage_from_with_limit(
        &self,
        run_id: &RunId,
        stage_id: &StageId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut connection = self.acquire().await?;
        Self::list_events_for_stage_from_with_limit_on_connection(
            &mut connection,
            run_id,
            stage_id,
            start_seq,
            limit,
        )
        .await
    }

    pub(crate) async fn list_events_for_session_from_with_limit(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut connection = self.acquire().await?;
        Self::list_events_for_session_from_with_limit_on_connection(
            &mut connection,
            run_id,
            session_id,
            start_seq,
            limit,
        )
        .await
    }

    pub(crate) async fn find_session_owner(&self, session_id: &SessionId) -> Result<Option<RunId>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query
            .push(" WHERE session_id = ")
            .push_bind(session_id.to_string())
            .push(" AND event_name = 'run.session.created'");
        let row = query.build().fetch_optional(&self.pool).await?;
        let Some(row) = row else {
            return Ok(None);
        };

        let stored_run_id: String = row.try_get("run_id")?;
        let run_id = stored_run_id
            .parse::<RunId>()
            .map_err(|_| Error::RunEventMismatch {
                run_id: stored_run_id.clone(),
                seq:    0,
                field:  "run_id",
            })?;
        // The WHERE clause pins the row's session_id and event_name columns
        // to the requested values, and decoding verifies the envelope against
        // every stored column, so a successful decode proves ownership.
        decode_event_row(&row, &run_id, &stored_run_id)?;
        Ok(Some(run_id))
    }

    pub(crate) async fn delete_canonical(&self, run_id: &RunId, deleted_at_ms: i64) -> Result<()> {
        // This transaction reads the activation marker before it writes the
        // tombstone and run deletion. A deferred SQLite transaction can fail
        // immediately when that read transaction is upgraded while another
        // writer is active, bypassing the configured busy timeout. Reserve
        // the write lock up front so concurrent deletes wait normally.
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let activated: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM legacy_run_history_activation WHERE singleton = 1)",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if activated {
            sqlx::query(
                r"
INSERT INTO legacy_run_history_deletions (run_id, deleted_at_ms)
VALUES (?, ?)
ON CONFLICT(run_id) DO UPDATE SET deleted_at_ms = excluded.deleted_at_ms
",
            )
            .bind(run_id.to_string())
            .bind(deleted_at_ms)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query("DELETE FROM runs WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn get(&self, run_id: &RunId, now: DateTime<Utc>) -> Result<Option<Run>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_RUN_SUMMARIES_SQL);
        query
            .push(" WHERE runs.id = ")
            .push_bind(run_id.to_string());
        let row = query.build().fetch_optional(&self.pool).await?;
        row.map(|row| decode_run_row(&row, now)).transpose()
    }

    /// Every current run row, without the bounded HTTP-list visibility or
    /// pagination semantics.
    pub async fn list_all(&self, now: DateTime<Utc>) -> Result<Vec<Run>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_RUN_SUMMARIES_SQL);
        push_order(
            &mut query,
            RunSummarySort::CreatedAt,
            RunSummarySortDirection::Desc,
            now,
        );
        let rows = query.build().fetch_all(&self.pool).await?;
        decode_run_rows(&rows, now)
    }

    /// Every current run row whose durable status matches one of `statuses`.
    pub async fn list_by_statuses(
        &self,
        statuses: &[RunStatusKind],
        now: DateTime<Utc>,
    ) -> Result<Vec<Run>> {
        if statuses.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_RUN_SUMMARIES_SQL);
        query.push(" WHERE status IN (");
        let mut separated = query.separated(", ");
        for status in statuses {
            separated.push_bind(status.to_string());
        }
        separated.push_unseparated(")");
        push_order(
            &mut query,
            RunSummarySort::CreatedAt,
            RunSummarySortDirection::Desc,
            now,
        );
        let rows = query.build().fetch_all(&self.pool).await?;
        decode_run_rows(&rows, now)
    }

    /// Run ids whose latest explicit pull request creation request has no
    /// later event that would resolve it. This mirrors the projection reducer:
    /// a newer request supersedes the old one; `created`, `linked`, and
    /// `unlinked` resolve any pending request; `failed` resolves only the
    /// request whose creation id it names. Callers still replay each candidate
    /// to confirm, so this must never omit a genuinely pending run, but it
    /// keeps the replayed set bounded by in-flight requests rather than by
    /// every run that ever asked for a pull request.
    pub async fn list_pull_request_creation_candidate_run_ids(&self) -> Result<Vec<RunId>> {
        sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT requested.run_id FROM run_events AS requested \
             WHERE requested.event_name = 'pull_request.creation_requested' \
               AND NOT EXISTS ( \
                 SELECT 1 FROM run_events AS later \
                 WHERE later.run_id = requested.run_id \
                   AND later.seq > requested.seq \
                   AND ( \
                     later.event_name IN ( \
                       'pull_request.creation_requested', \
                       'pull_request.created', \
                       'pull_request.linked', \
                       'pull_request.unlinked' \
                     ) \
                     OR ( \
                       later.event_name = 'pull_request.failed' \
                       AND json_extract(later.event_json, '$.properties.creation_id') \
                         = json_extract(requested.event_json, '$.properties.creation_id') \
                     ) \
                   ) \
               )",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(parse_stored_run_id)
        .collect()
    }

    /// Identity fields for every stored run, for selector resolution without
    /// decoding full summaries.
    pub async fn list_identities(&self) -> Result<Vec<RunSummaryIdentity>> {
        let rows = sqlx::query(
            r"
SELECT id, workflow_slug,
       json_extract(summary_json, '$.workflow.name') AS workflow_name,
       json_extract(summary_json, '$.repository.origin_url') AS repository_origin_url
FROM runs",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let stored_id: String = row.try_get("id")?;
                let id = parse_stored_run_id(stored_id)?;
                Ok(RunSummaryIdentity {
                    id,
                    workflow_slug: row.try_get("workflow_slug")?,
                    workflow_name: row.try_get("workflow_name")?,
                    repository_origin_url: row.try_get("repository_origin_url")?,
                })
            })
            .collect()
    }

    pub async fn list(
        &self,
        query: &RunSummaryListQuery,
        now: DateTime<Utc>,
    ) -> Result<RunSummaryPage> {
        let mut transaction = self.pool.begin().await?;

        let mut count_query = QueryBuilder::<Sqlite>::new("SELECT COUNT(*) FROM runs");
        push_filters(&mut count_query, query);
        let total: i64 = count_query
            .build_query_scalar()
            .fetch_one(&mut *transaction)
            .await?;

        let mut rows_query = QueryBuilder::<Sqlite>::new(SELECT_RUN_SUMMARIES_SQL);
        push_filters(&mut rows_query, query);
        push_order(&mut rows_query, query.sort, query.direction, now);
        rows_query.push(" LIMIT ").push_bind(i64::from(query.limit));
        rows_query
            .push(" OFFSET ")
            .push_bind(i64::from(query.offset));
        let rows = rows_query.build().fetch_all(&mut *transaction).await?;
        transaction.commit().await?;

        let data = decode_run_rows(&rows, now)?;
        let total = u64::try_from(total).expect("COUNT(*) is non-negative");
        let consumed = u64::from(query.offset).saturating_add(data.len() as u64);
        Ok(RunSummaryPage {
            data,
            total,
            has_more: consumed < total,
        })
    }
}

impl RunSummaryStore {
    pub(crate) async fn insert_first_event_on_connection(
        connection: &mut SqliteConnection,
        entry: &ProjectedRun,
        payload: &EventPayload,
    ) -> Result<EventEnvelope> {
        let record = PreparedRunSummary::from_entry(entry);
        ensure_entry_identity(entry, &record, 1)?;
        ensure_prepared_head(&record, 1)?;
        let envelope = validate_event_for_record(&record, payload, 1)?;
        if envelope.event.event_name() != "run.created" {
            return Err(run_event_mismatch(&record.run.id, 1, "event_name"));
        }

        insert_run_on_connection(connection, &record).await?;
        insert_event_on_connection(connection, &record, payload, &envelope).await?;
        Ok(envelope)
    }

    pub(crate) async fn append_event_on_connection(
        connection: &mut SqliteConnection,
        expected_last_seq: u32,
        entry: &ProjectedRun,
        payload: &EventPayload,
    ) -> Result<EventEnvelope> {
        let next_seq = next_event_seq_after(expected_last_seq)?;
        let record = PreparedRunSummary::from_entry(entry);
        ensure_entry_identity(entry, &record, next_seq)?;
        ensure_prepared_head(&record, next_seq)?;
        let envelope = validate_event_for_record(&record, payload, next_seq)?;

        update_run_on_connection(connection, &record, expected_last_seq).await?;
        insert_event_on_connection(connection, &record, payload, &envelope).await?;
        Ok(envelope)
    }

    pub(crate) async fn list_events_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
    ) -> Result<Vec<EventEnvelope>> {
        Ok(
            Self::list_events_with_json_on_connection(connection, run_id)
                .await?
                .into_iter()
                .map(|(envelope, _event_json)| envelope)
                .collect(),
        )
    }

    pub(crate) async fn list_events_with_json_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
    ) -> Result<Vec<(EventEnvelope, String)>> {
        // The current-row head and event rows must come from one snapshot.
        // Otherwise a concurrent append between the two SELECTs looks like
        // durable corruption even though both versions are individually valid.
        let mut transaction = connection.begin().await?;
        let events = Self::list_events_with_json_in_transaction(&mut transaction, run_id).await?;
        transaction.commit().await?;
        Ok(events)
    }

    pub(crate) async fn list_events_with_json_in_transaction(
        transaction: &mut Transaction<'_, Sqlite>,
        run_id: &RunId,
    ) -> Result<Vec<(EventEnvelope, String)>> {
        Self::list_events_with_json_in_snapshot(&mut *transaction, run_id).await
    }

    async fn list_events_with_json_in_snapshot(
        connection: &mut SqliteConnection,
        run_id: &RunId,
    ) -> Result<Vec<(EventEnvelope, String)>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query
            .push(" WHERE run_id = ")
            .push_bind(run_id.to_string())
            .push(" ORDER BY seq ASC");
        let expected_last_seq = select_run_head(&mut *connection, run_id)
            .await?
            .ok_or_else(|| Error::RunNotFound(run_id.to_string()))?;
        let rows = query.build().fetch_all(&mut *connection).await?;
        let actual_last_seq = rows
            .last()
            .map(|row| row.try_get::<i64, _>("seq"))
            .transpose()?
            .and_then(stored_seq);
        if actual_last_seq != Some(expected_last_seq) {
            return Err(Error::RunHeadMismatch {
                run_id: run_id.to_string(),
                expected_last_seq,
                actual_last_seq,
            });
        }
        decode_event_rows_with_json(&rows, run_id)
    }

    pub(crate) async fn insert_imported_run_on_connection(
        connection: &mut SqliteConnection,
        entry: &ProjectedRun,
    ) -> Result<()> {
        let record = PreparedRunSummary::from_entry(entry);
        ensure_entry_identity(entry, &record, entry.last_seq)?;
        if !(1..=keys::MAX_EVENT_SEQ).contains(&entry.last_seq) {
            return Err(Error::RunHeadMismatch {
                run_id:            entry.run_id.to_string(),
                expected_last_seq: entry.last_seq,
                actual_last_seq:   None,
            });
        }
        insert_run_on_connection(connection, &record).await
    }

    pub(crate) async fn insert_imported_event_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
        payload: &EventPayload,
        envelope: &EventEnvelope,
        event_json: &str,
    ) -> Result<()> {
        payload.validate(run_id)?;
        let decoded = RunEvent::try_from(payload)?;
        if envelope.event != decoded || envelope.event.run_id != *run_id {
            return Err(run_event_mismatch(run_id, envelope.seq, "event_json"));
        }
        if !(1..=keys::MAX_EVENT_SEQ).contains(&envelope.seq) {
            return Err(run_event_mismatch(run_id, envelope.seq, "seq"));
        }
        insert_event_json_on_connection(connection, run_id, envelope, event_json).await
    }

    pub(crate) async fn verify_current_run_on_connection(
        connection: &mut SqliteConnection,
        entry: &ProjectedRun,
    ) -> Result<()> {
        let record = PreparedRunSummary::from_entry(entry);
        ensure_entry_identity(entry, &record, entry.last_seq)?;
        let row = sqlx::query(
            r"
SELECT id, source_last_seq, created_at_ms, started_at_ms, last_event_at_ms, completed_at_ms,
       status, archived_at_ms, parent_id, title, workflow_slug, workflow_name,
       repository_name, automation_id, diff_files_changed, diff_additions, diff_deletions,
       input_tokens, output_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens,
       total_usd_micros, summary_json
FROM runs
WHERE id = ?
",
        )
        .bind(entry.run_id.to_string())
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| Error::RunNotFound(entry.run_id.to_string()))?;

        let run = &record.run;
        let diff = run.diff.unwrap_or_default();
        verify_run_field(&row, run, "id", &run.id.to_string())?;
        verify_run_field(&row, run, "source_last_seq", &i64::from(record.last_seq))?;
        verify_run_field(
            &row,
            run,
            "created_at_ms",
            &run.timestamps.created_at.timestamp_millis(),
        )?;
        verify_run_field(
            &row,
            run,
            "started_at_ms",
            &run.timestamps
                .started_at
                .map(|value| value.timestamp_millis()),
        )?;
        verify_run_field(
            &row,
            run,
            "last_event_at_ms",
            &run.timestamps
                .last_event_at
                .unwrap_or(run.timestamps.created_at)
                .timestamp_millis(),
        )?;
        verify_run_field(
            &row,
            run,
            "completed_at_ms",
            &run.timestamps
                .completed_at
                .map(|value| value.timestamp_millis()),
        )?;
        verify_run_field(
            &row,
            run,
            "status",
            &run.lifecycle.status.kind().to_string(),
        )?;
        verify_run_field(
            &row,
            run,
            "archived_at_ms",
            &run.lifecycle
                .archived_at
                .map(|value| value.timestamp_millis()),
        )?;
        verify_run_field(
            &row,
            run,
            "parent_id",
            &run.parent_id.map(|value| value.to_string()),
        )?;
        verify_run_field(&row, run, "title", &run.title)?;
        verify_run_field(&row, run, "workflow_slug", &run.workflow.slug)?;
        verify_run_field(&row, run, "workflow_name", &record.workflow_name)?;
        verify_run_field(&row, run, "repository_name", &record.repository_name)?;
        verify_run_field(
            &row,
            run,
            "automation_id",
            &run.automation
                .as_ref()
                .map(|automation| automation.id.clone()),
        )?;
        verify_run_field(&row, run, "diff_files_changed", &diff.files_changed)?;
        verify_run_field(&row, run, "diff_additions", &diff.additions)?;
        verify_run_field(&row, run, "diff_deletions", &diff.deletions)?;
        verify_run_field(&row, run, "input_tokens", &record.input_tokens)?;
        verify_run_field(&row, run, "output_tokens", &record.output_tokens)?;
        verify_run_field(&row, run, "reasoning_tokens", &record.reasoning_tokens)?;
        verify_run_field(&row, run, "cache_read_tokens", &record.cache_read_tokens)?;
        verify_run_field(&row, run, "cache_write_tokens", &record.cache_write_tokens)?;
        verify_run_field(&row, run, "total_usd_micros", &record.total_usd_micros)?;
        verify_run_json_field(&row, run)?;
        Ok(())
    }

    pub(crate) async fn list_events_from_with_limit_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query
            .push(" WHERE run_id = ")
            .push_bind(run_id.to_string())
            .push(" AND seq >= ")
            .push_bind(i64::from(start_seq))
            .push(" ORDER BY seq ASC LIMIT ")
            .push_bind(sql_limit(limit));
        let rows = query.build().fetch_all(&mut *connection).await?;
        decode_event_rows(&rows, run_id)
    }

    pub(crate) async fn list_events_before_with_limit_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
        before_seq: Option<u32>,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query.push(" WHERE run_id = ").push_bind(run_id.to_string());
        if let Some(before_seq) = before_seq {
            query.push(" AND seq < ").push_bind(i64::from(before_seq));
        }
        query
            .push(" ORDER BY seq DESC LIMIT ")
            .push_bind(sql_limit(limit));
        let rows = query.build().fetch_all(&mut *connection).await?;
        decode_event_rows(&rows, run_id)
    }

    pub(crate) async fn get_event_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
        seq: u32,
    ) -> Result<Option<EventEnvelope>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query
            .push(" WHERE run_id = ")
            .push_bind(run_id.to_string())
            .push(" AND seq = ")
            .push_bind(i64::from(seq));
        let row = query.build().fetch_optional(&mut *connection).await?;
        row.as_ref()
            .map(|row| decode_event_row(row, run_id, &run_id.to_string()))
            .transpose()
    }

    pub(crate) async fn list_events_for_stage_from_with_limit_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
        stage_id: &StageId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        // Legacy rows for a first visit carry only `node_id`. Query them as a
        // second `UNION ALL` arm instead of an `OR` so each arm can use its
        // own partial index rather than scanning the run's primary key range.
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query
            .push(" WHERE run_id = ")
            .push_bind(run_id.to_string())
            .push(" AND seq >= ")
            .push_bind(i64::from(start_seq))
            .push(" AND stage_id = ")
            .push_bind(stage_id.to_string());
        if stage_id.visit() == 1 {
            query
                .push(" UNION ALL ")
                .push(SELECT_EVENT_COLUMNS)
                .push(" WHERE run_id = ")
                .push_bind(run_id.to_string())
                .push(" AND seq >= ")
                .push_bind(i64::from(start_seq))
                .push(" AND stage_id IS NULL AND node_id = ")
                .push_bind(stage_id.node_id().to_string());
        }
        query
            .push(" ORDER BY seq ASC LIMIT ")
            .push_bind(sql_limit(limit));
        let rows = query.build().fetch_all(&mut *connection).await?;
        decode_event_rows(&rows, run_id)
    }

    pub(crate) async fn list_events_for_session_from_with_limit_on_connection(
        connection: &mut SqliteConnection,
        run_id: &RunId,
        session_id: &SessionId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_EVENT_COLUMNS);
        query
            .push(" WHERE run_id = ")
            .push_bind(run_id.to_string())
            .push(" AND seq >= ")
            .push_bind(i64::from(start_seq))
            .push(" AND session_id = ")
            .push_bind(session_id.to_string())
            .push(" AND event_name GLOB 'run.session.*' ORDER BY seq ASC LIMIT ")
            .push_bind(sql_limit(limit));
        let rows = query.build().fetch_all(&mut *connection).await?;
        decode_event_rows(&rows, run_id)
    }
}

/// Identity fields of a stored run summary, cheap to list for selector
/// resolution.
#[derive(Debug, Clone)]
pub struct RunSummaryIdentity {
    pub id:                    RunId,
    pub workflow_slug:         Option<String>,
    pub workflow_name:         Option<String>,
    pub repository_origin_url: Option<String>,
}

#[derive(Debug)]
struct PreparedRunSummary {
    run:                Run,
    last_seq:           u32,
    workflow_name:      Option<String>,
    repository_name:    Option<String>,
    input_tokens:       i64,
    output_tokens:      i64,
    reasoning_tokens:   i64,
    cache_read_tokens:  i64,
    cache_write_tokens: i64,
    total_usd_micros:   Option<i64>,
}

impl PreparedRunSummary {
    fn from_entry(entry: &ProjectedRun) -> Self {
        let mut run = build_summary(&entry.projection, &entry.run_id);
        if run.timing.is_none() {
            let at = run
                .timestamps
                .last_event_at
                .unwrap_or(run.timestamps.created_at);
            run.timing = entry.projection.live_run_timing(at);
        }
        let billing = normalize_billing_for_read_model(projected_billing(&entry.projection));
        let workflow_name = run.workflow.display_name().map(str::to_string);
        let repository_name = run
            .repository
            .as_ref()
            .map(|repository| repository.name.clone());

        Self {
            run,
            last_seq: entry.last_seq,
            workflow_name,
            repository_name,
            input_tokens: billing.input_tokens,
            output_tokens: billing.output_tokens,
            reasoning_tokens: billing.reasoning_tokens,
            cache_read_tokens: billing.cache_read_tokens,
            cache_write_tokens: billing.cache_write_tokens,
            total_usd_micros: billing.total_usd_micros,
        }
    }
}

fn ensure_entry_identity(
    entry: &ProjectedRun,
    record: &PreparedRunSummary,
    seq: u32,
) -> Result<()> {
    if entry.run_id != record.run.id || entry.projection.spec.run_id != entry.run_id {
        return Err(run_event_mismatch(&entry.run_id, seq, "run_id"));
    }
    Ok(())
}

fn ensure_prepared_head(record: &PreparedRunSummary, expected_last_seq: u32) -> Result<()> {
    if record.last_seq != expected_last_seq {
        return Err(Error::RunHeadMismatch {
            run_id: record.run.id.to_string(),
            expected_last_seq,
            actual_last_seq: Some(record.last_seq),
        });
    }
    Ok(())
}

fn validate_event_for_record(
    record: &PreparedRunSummary,
    payload: &EventPayload,
    seq: u32,
) -> Result<EventEnvelope> {
    payload.validate(&record.run.id)?;
    let event = RunEvent::try_from(payload)?;
    if event.run_id != record.run.id {
        return Err(run_event_mismatch(&record.run.id, seq, "run_id"));
    }
    Ok(EventEnvelope { seq, event })
}

fn run_event_mismatch(run_id: &RunId, seq: u32, field: &'static str) -> Error {
    Error::RunEventMismatch {
        run_id: run_id.to_string(),
        seq,
        field,
    }
}

async fn insert_event_on_connection(
    connection: &mut SqliteConnection,
    record: &PreparedRunSummary,
    payload: &EventPayload,
    envelope: &EventEnvelope,
) -> Result<()> {
    let event_json = serde_json::to_string(payload)?;
    insert_event_json_on_connection(connection, &record.run.id, envelope, &event_json).await
}

async fn insert_event_json_on_connection(
    connection: &mut SqliteConnection,
    run_id: &RunId,
    envelope: &EventEnvelope,
    event_json: &str,
) -> Result<()> {
    sqlx::query(INSERT_EVENT_SQL)
        .bind(run_id.to_string())
        .bind(i64::from(envelope.seq))
        .bind(envelope.event.event_name())
        .bind(envelope.event.node_id.as_deref())
        .bind(envelope.event.stage_id.as_ref().map(ToString::to_string))
        .bind(envelope.event.session_id.as_deref())
        .bind(event_json)
        .execute(connection)
        .await?;
    Ok(())
}

fn sql_limit(limit: usize) -> i64 {
    i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX)
}

pub(crate) fn next_event_seq_after(last_seq: u32) -> Result<u32> {
    last_seq
        .checked_add(1)
        .filter(|seq| *seq <= keys::MAX_EVENT_SEQ)
        .ok_or(Error::EventSequenceExhausted {
            max_seq: keys::MAX_EVENT_SEQ,
        })
}

/// Decodes a stored sequence column, rejecting anything outside the valid
/// `1..=MAX_EVENT_SEQ` range.
fn stored_seq(value: i64) -> Option<u32> {
    u32::try_from(value)
        .ok()
        .filter(|seq| (1..=keys::MAX_EVENT_SEQ).contains(seq))
}

fn decode_event_rows(rows: &[SqliteRow], run_id: &RunId) -> Result<Vec<EventEnvelope>> {
    let run_id_text = run_id.to_string();
    rows.iter()
        .map(|row| decode_event_row(row, run_id, &run_id_text))
        .collect()
}

fn decode_event_rows_with_json(
    rows: &[SqliteRow],
    run_id: &RunId,
) -> Result<Vec<(EventEnvelope, String)>> {
    let run_id_text = run_id.to_string();
    rows.iter()
        .map(|row| decode_event_row_with_json(row, run_id, &run_id_text))
        .collect()
}

fn verify_run_field<T>(row: &SqliteRow, run: &Run, field: &'static str, expected: &T) -> Result<()>
where
    T: for<'row> sqlx::Decode<'row, Sqlite> + sqlx::Type<Sqlite> + PartialEq,
{
    let stored: T = row.try_get(field)?;
    if &stored != expected {
        return Err(Error::RunSummaryMismatch {
            run_id: run.id.to_string(),
            field,
        });
    }
    Ok(())
}

fn verify_run_json_field(row: &SqliteRow, run: &Run) -> Result<()> {
    let stored_json: String = row.try_get("summary_json")?;
    let stored: serde_json::Value = serde_json::from_str(&stored_json)?;
    let expected = serde_json::to_value(run)?;
    if stored != expected {
        return Err(Error::RunSummaryMismatch {
            run_id: run.id.to_string(),
            field:  "summary_json",
        });
    }
    Ok(())
}

fn decode_event_row(
    row: &SqliteRow,
    expected_run_id: &RunId,
    expected_run_id_text: &str,
) -> Result<EventEnvelope> {
    decode_event_row_with_json(row, expected_run_id, expected_run_id_text)
        .map(|(envelope, _event_json)| envelope)
}

/// Decodes one event row and returns the raw `event_json` alongside it so
/// callers that need both do not fetch the column twice.
fn decode_event_row_with_json(
    row: &SqliteRow,
    expected_run_id: &RunId,
    expected_run_id_text: &str,
) -> Result<(EventEnvelope, String)> {
    let stored_run_id: String = row.try_get("run_id")?;
    let raw_seq: i64 = row.try_get("seq")?;
    let seq = stored_seq(raw_seq).ok_or_else(|| run_event_mismatch(expected_run_id, 0, "seq"))?;
    if stored_run_id != expected_run_id_text {
        return Err(run_event_mismatch(expected_run_id, seq, "run_id"));
    }

    let event_json: String = row.try_get("event_json")?;
    let payload: EventPayload = serde_json::from_str(&event_json)?;
    if payload
        .as_value()
        .get("run_id")
        .and_then(serde_json::Value::as_str)
        != Some(expected_run_id_text)
    {
        return Err(run_event_mismatch(expected_run_id, seq, "run_id"));
    }
    payload.validate(expected_run_id)?;
    let event = RunEvent::try_from(&payload)?;
    if event.run_id != *expected_run_id {
        return Err(run_event_mismatch(expected_run_id, seq, "run_id"));
    }

    let stored_event_name: String = row.try_get("event_name")?;
    if stored_event_name != event.event_name() {
        return Err(run_event_mismatch(expected_run_id, seq, "event_name"));
    }
    let stored_node_id: Option<String> = row.try_get("node_id")?;
    if stored_node_id.as_deref() != event.node_id.as_deref() {
        return Err(run_event_mismatch(expected_run_id, seq, "node_id"));
    }
    let stored_stage_id: Option<String> = row.try_get("stage_id")?;
    let decoded_stage_id = event.stage_id.as_ref().map(ToString::to_string);
    if stored_stage_id != decoded_stage_id {
        return Err(run_event_mismatch(expected_run_id, seq, "stage_id"));
    }
    let stored_session_id: Option<String> = row.try_get("session_id")?;
    if stored_session_id.as_deref() != event.session_id.as_deref() {
        return Err(run_event_mismatch(expected_run_id, seq, "session_id"));
    }

    Ok((EventEnvelope { seq, event }, event_json))
}

async fn select_run_head(connection: &mut SqliteConnection, run_id: &RunId) -> Result<Option<u32>> {
    let stored: Option<i64> = sqlx::query_scalar("SELECT source_last_seq FROM runs WHERE id = ?")
        .bind(run_id.to_string())
        .fetch_optional(connection)
        .await?;
    stored
        .map(|value| {
            stored_seq(value).ok_or_else(|| run_event_mismatch(run_id, 0, "source_last_seq"))
        })
        .transpose()
}

/// Older provider codecs could persist a negative disjoint bucket when a
/// detail count exceeded its inclusive parent total. The SQLite summary is a
/// rebuildable, nonnegative read model, so normalize those legacy values here
/// without rewriting the authoritative run events.
fn normalize_billing_for_read_model(mut billing: BilledTokenCounts) -> BilledTokenCounts {
    let input_total = billing
        .input_tokens
        .saturating_add(billing.cache_read_tokens)
        .saturating_add(billing.cache_write_tokens)
        .max(0);
    billing.cache_read_tokens = billing.cache_read_tokens.clamp(0, input_total);
    billing.cache_write_tokens = billing
        .cache_write_tokens
        .clamp(0, input_total - billing.cache_read_tokens);
    billing.input_tokens = input_total - billing.cache_read_tokens - billing.cache_write_tokens;

    let output_total = billing
        .output_tokens
        .saturating_add(billing.reasoning_tokens)
        .max(0);
    billing.reasoning_tokens = billing.reasoning_tokens.clamp(0, output_total);
    billing.output_tokens = output_total - billing.reasoning_tokens;
    billing.total_tokens = input_total.saturating_add(output_total);
    billing.total_usd_micros = billing.total_usd_micros.map(|value| value.max(0));
    billing
}

#[cfg(test)]
async fn upsert_run_on_connection(
    connection: &mut SqliteConnection,
    record: &PreparedRunSummary,
) -> Result<()> {
    write_insert_shaped_run(connection, record, UPSERT_RUN_SQL).await
}

async fn insert_run_on_connection(
    connection: &mut SqliteConnection,
    record: &PreparedRunSummary,
) -> Result<()> {
    write_insert_shaped_run(connection, record, INSERT_RUN_SQL).await
}

async fn write_insert_shaped_run(
    connection: &mut SqliteConnection,
    record: &PreparedRunSummary,
    sql: &'static str,
) -> Result<()> {
    bind_run_columns(sqlx::query(sql).bind(record.run.id.to_string()), record)?
        .execute(connection)
        .await?;
    Ok(())
}

/// Binds the `runs` columns shared by the insert, upsert, and update
/// statements, in the positional order those statements declare them
/// (`source_last_seq` through `summary_json`).
fn bind_run_columns<'q>(
    query: Query<'q, Sqlite, SqliteArguments>,
    record: &'q PreparedRunSummary,
) -> Result<Query<'q, Sqlite, SqliteArguments>> {
    let run = &record.run;
    let diff = run.diff.unwrap_or_default();
    let summary_json = serde_json::to_string(run)?;
    Ok(query
        .bind(i64::from(record.last_seq))
        .bind(run.timestamps.created_at.timestamp_millis())
        .bind(
            run.timestamps
                .started_at
                .map(|value| value.timestamp_millis()),
        )
        .bind(
            run.timestamps
                .last_event_at
                .unwrap_or(run.timestamps.created_at)
                .timestamp_millis(),
        )
        .bind(
            run.timestamps
                .completed_at
                .map(|value| value.timestamp_millis()),
        )
        .bind(run.lifecycle.status.kind().to_string())
        .bind(
            run.lifecycle
                .archived_at
                .map(|value| value.timestamp_millis()),
        )
        .bind(run.parent_id.map(|value| value.to_string()))
        .bind(&run.title)
        .bind(&run.workflow.slug)
        .bind(&record.workflow_name)
        .bind(&record.repository_name)
        .bind(run.automation.as_ref().map(|automation| &automation.id))
        .bind(diff.files_changed)
        .bind(diff.additions)
        .bind(diff.deletions)
        .bind(record.input_tokens)
        .bind(record.output_tokens)
        .bind(record.reasoning_tokens)
        .bind(record.cache_read_tokens)
        .bind(record.cache_write_tokens)
        .bind(record.total_usd_micros)
        .bind(summary_json))
}

async fn update_run_on_connection(
    connection: &mut SqliteConnection,
    record: &PreparedRunSummary,
    expected_last_seq: u32,
) -> Result<()> {
    let run = &record.run;
    let result = bind_run_columns(sqlx::query(UPDATE_RUN_SQL), record)?
        .bind(run.id.to_string())
        .bind(i64::from(expected_last_seq))
        .execute(&mut *connection)
        .await?;
    if result.rows_affected() == 0 {
        return Err(Error::RunHeadMismatch {
            run_id: run.id.to_string(),
            expected_last_seq,
            actual_last_seq: select_run_head(connection, &run.id).await?,
        });
    }
    Ok(())
}

fn push_filters(builder: &mut QueryBuilder<Sqlite>, query: &RunSummaryListQuery) {
    builder.push(" WHERE 1 = 1");
    if let Some(parent_id) = query.parent_id {
        builder
            .push(" AND parent_id = ")
            .push_bind(parent_id.to_string());
    }
    if let Some(automation_id) = &query.automation_id {
        builder
            .push(" AND automation_id = ")
            .push_bind(automation_id.clone());
    }

    match &query.visibility {
        RunSummaryVisibility::All => {}
        RunSummaryVisibility::Default { include_archived } => {
            let not_removing = format!("status <> '{}'", RunStatusKind::Removing);
            if *include_archived {
                builder.push(format!(
                    " AND (archived_at_ms IS NOT NULL OR {not_removing})"
                ));
            } else {
                builder.push(format!(" AND archived_at_ms IS NULL AND {not_removing}"));
            }
        }
        RunSummaryVisibility::Selected { statuses, archived } => {
            builder.push(" AND (");
            let mut has_condition = false;
            if *archived {
                builder.push("archived_at_ms IS NOT NULL");
                has_condition = true;
            }
            if !statuses.is_empty() {
                if has_condition {
                    builder.push(" OR ");
                }
                builder.push("(archived_at_ms IS NULL AND status IN (");
                let mut separated = builder.separated(", ");
                for status in statuses {
                    separated.push_bind(status.to_string());
                }
                separated.push_unseparated("))");
                has_condition = true;
            }
            if !has_condition {
                builder.push("0");
            }
            builder.push(")");
        }
    }
}

/// Status sort rank derived from [`RunStatusKind::board_rank`], so the SQL
/// order and the board column order share one source. Archived runs rank 7,
/// matching the `archived` board column.
static STATUS_RANK_CASE_SQL: LazyLock<String> = LazyLock::new(|| {
    let mut case = String::from("CASE WHEN archived_at_ms IS NOT NULL THEN 7");
    for kind in RunStatusKind::VARIANTS {
        let _ = write!(case, " WHEN status = '{kind}' THEN {}", kind.board_rank());
    }
    case.push_str(" ELSE 9 END");
    case
});

/// Size sort rank derived from [`RunSize::BUCKET_MAX_USD_MICROS`], so the SQL
/// order and the displayed size buckets share one source.
static SIZE_RANK_CASE_SQL: LazyLock<String> = LazyLock::new(|| {
    let mut case = String::from("CASE");
    for (rank, (_, max_usd_micros)) in RunSize::BUCKET_MAX_USD_MICROS.iter().enumerate() {
        let _ = write!(
            case,
            " WHEN COALESCE(total_usd_micros, 0) <= {max_usd_micros} THEN {rank}"
        );
    }
    let _ = write!(case, " ELSE {} END", RunSize::BUCKET_MAX_USD_MICROS.len());
    case
});

fn push_order(
    builder: &mut QueryBuilder<Sqlite>,
    sort: RunSummarySort,
    direction: RunSummarySortDirection,
    now: DateTime<Utc>,
) {
    builder.push(" ORDER BY ");
    match sort {
        RunSummarySort::CreatedAt => builder.push("created_at_ms"),
        RunSummarySort::UpdatedAt => builder.push("last_event_at_ms"),
        RunSummarySort::Status => builder.push(STATUS_RANK_CASE_SQL.as_str()),
        RunSummarySort::Elapsed => builder
            .push("(COALESCE(completed_at_ms, ")
            .push_bind(now.timestamp_millis())
            .push(") - COALESCE(started_at_ms, created_at_ms))"),
        RunSummarySort::Repository => builder.push("COALESCE(repository_name, '') COLLATE NOCASE"),
        RunSummarySort::Title => builder.push("TRIM(title) COLLATE NOCASE"),
        RunSummarySort::Workflow => builder.push("COALESCE(workflow_name, '') COLLATE NOCASE"),
        RunSummarySort::Changes => builder.push("(diff_additions + diff_deletions)"),
        RunSummarySort::Size => builder.push(SIZE_RANK_CASE_SQL.as_str()),
    };
    match direction {
        RunSummarySortDirection::Asc => builder.push(" ASC"),
        RunSummarySortDirection::Desc => builder.push(" DESC"),
    };
    builder.push(", id DESC");
}

fn parse_stored_run_id(stored_id: String) -> Result<RunId> {
    stored_id
        .parse::<RunId>()
        .map_err(|_| Error::RunSummaryMismatch {
            run_id: stored_id,
            field:  "id",
        })
}

fn decode_run_row(row: &SqliteRow, now: DateTime<Utc>) -> Result<Run> {
    let stored_id: String = row.try_get("id")?;
    let summary_json: String = row.try_get("summary_json")?;
    let children_count: i64 = row.try_get("children_count")?;
    let mut run: Run = serde_json::from_str(&summary_json)?;
    if stored_id != run.id.to_string() {
        return Err(Error::RunSummaryMismatch {
            run_id: stored_id,
            field:  "id",
        });
    }
    run.children_count = u64::try_from(children_count).map_err(|_| Error::RunSummaryMismatch {
        run_id: run.id.to_string(),
        field:  "children_count",
    })?;
    overlay_live_wall_time(&mut run, now);
    Ok(run)
}

fn decode_run_rows(rows: &[SqliteRow], now: DateTime<Utc>) -> Result<Vec<Run>> {
    rows.iter().map(|row| decode_run_row(row, now)).collect()
}

fn overlay_live_wall_time(run: &mut Run, now: DateTime<Utc>) {
    if run.timestamps.completed_at.is_some() {
        return;
    }
    let Some(started_at) = run.timestamps.started_at else {
        return;
    };
    let wall_time_ms = timing::elapsed_ms(started_at, now);
    run.timing = Some(
        run.timing
            .unwrap_or_else(|| RunTiming::wall_only(wall_time_ms))
            .with_wall_time(wall_time_ms),
    );
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{DateTime, Utc};
    use fabro_types::{
        AutomationRef, BilledTokenCounts, BlockedReason, Conclusion, DiffSummary, EventEnvelope,
        FailureReason, Graph, PendingReason, PullRequestCreationId, RunDiff, RunId, RunProjection,
        RunSize, RunSpec, RunStatus, RunStatusKind, RunTiming, SessionId, StageId, StageOutcome,
        SuccessReason, WorkflowSettings, test_support,
    };
    use strum::VariantArray as _;
    use tokio::time;
    use ulid::Ulid;

    use super::{
        INSERT_EVENT_SQL, RunSummaryListQuery, RunSummarySort, RunSummarySortDirection,
        RunSummaryStore, RunSummaryVisibility, decode_event_row,
    };
    use crate::run_state::ProjectedRun;
    use crate::{Error, EventPayload, RunProjectionReducer, test_support as store_test_support};

    fn dt(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    fn run_id(timestamp_ms: u64, random: u128) -> RunId {
        RunId::from(Ulid::from_parts(timestamp_ms, random))
    }

    fn projection(run_id: RunId, title: &str, created_at: DateTime<Utc>) -> RunProjection {
        RunProjection::new(
            title.to_string(),
            RunSpec {
                run_id,
                settings: WorkflowSettings::default(),
                graph: Graph::new("test"),
                graph_source: None,
                workflow_slug: Some("test-workflow".to_string()),
                workflow_version_id: None,
                target: None,
                automation: None,
                source_directory: None,
                labels: HashMap::new(),
                provenance: test_support::test_run_provenance(),
                manifest_blob: None,
                definition_blob: None,
                spec_blob: None,
                git: None,
                fork_source_ref: None,
            },
            created_at,
        )
    }

    fn entry(projection: RunProjection, last_seq: u32) -> ProjectedRun {
        ProjectedRun::new(projection.spec.run_id, Arc::new(projection), last_seq)
    }

    async fn store() -> (tempfile::TempDir, RunSummaryStore) {
        store_test_support::sqlite_run_summary_store().await
    }

    fn sql_event_payload(
        run_id: &RunId,
        event: &str,
        node_id: Option<&str>,
        stage_id: Option<&StageId>,
        session_id: Option<&SessionId>,
        properties: serde_json::Value,
    ) -> EventPayload {
        let mut value = serde_json::json!({
            "id": format!("evt-{event}"),
            "ts": "2026-08-27T12:00:00Z",
            "run_id": run_id.to_string(),
            "event": event,
        });
        let object = value.as_object_mut().unwrap();
        object.insert("properties".to_string(), properties);
        if let Some(node_id) = node_id {
            object.insert("node_id".to_string(), node_id.into());
        }
        if let Some(stage_id) = stage_id {
            object.insert("stage_id".to_string(), stage_id.to_string().into());
        }
        if let Some(session_id) = session_id {
            object.insert("session_id".to_string(), session_id.to_string().into());
        }
        EventPayload::new(value, run_id).unwrap()
    }

    fn seqs(events: &[EventEnvelope]) -> Vec<u32> {
        events.iter().map(|event| event.seq).collect()
    }

    fn created_payload(run_id: &RunId) -> EventPayload {
        sql_event_payload(
            run_id,
            "run.created",
            None,
            None,
            None,
            serde_json::json!({
                "title": "created",
                "settings": WorkflowSettings::default(),
                "graph": Graph::new("test"),
                "workflow_slug": "test-workflow",
                "labels": {},
                "provenance": test_support::test_run_provenance(),
            }),
        )
    }

    fn session_created_payload(run_id: &RunId, session_id: &SessionId) -> EventPayload {
        sql_event_payload(
            run_id,
            "run.session.created",
            None,
            None,
            Some(session_id),
            serde_json::json!({ "title": "Owned session" }),
        )
    }

    async fn seed_sql_event(
        store: &RunSummaryStore,
        run_id: &RunId,
        seq: u32,
        payload: &EventPayload,
    ) {
        let event = fabro_types::RunEvent::try_from(payload).unwrap();
        sqlx::query(INSERT_EVENT_SQL)
            .bind(run_id.to_string())
            .bind(i64::from(seq))
            .bind(event.event_name())
            .bind(event.node_id)
            .bind(event.stage_id.map(|stage_id| stage_id.to_string()))
            .bind(event.session_id)
            .bind(serde_json::to_string(payload).unwrap())
            .execute(&store.pool)
            .await
            .unwrap();
    }

    fn sample_status(kind: RunStatusKind) -> RunStatus {
        match kind {
            RunStatusKind::Submitted => RunStatus::Submitted,
            RunStatusKind::Pending => RunStatus::Pending {
                reason: PendingReason::ApprovalRequired,
            },
            RunStatusKind::Runnable => RunStatus::Runnable,
            RunStatusKind::Starting => RunStatus::Starting,
            RunStatusKind::Running => RunStatus::Running,
            RunStatusKind::Blocked => RunStatus::Blocked {
                blocked_reason: BlockedReason::HumanInputRequired,
            },
            RunStatusKind::Paused => RunStatus::Paused { prior_block: None },
            RunStatusKind::Removing => RunStatus::Removing,
            RunStatusKind::Succeeded => RunStatus::Succeeded {
                reason: SuccessReason::Completed,
            },
            RunStatusKind::Failed => RunStatus::Failed {
                reason: FailureReason::WorkflowError,
            },
            RunStatusKind::Dead => RunStatus::Dead,
        }
    }

    #[tokio::test]
    async fn sql_run_transitions_are_atomic_and_guard_the_current_head() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let first = entry(projection(id, "created", created_at), 1);
        let first_payload = created_payload(&id);

        let mut transaction = store.pool.begin().await.unwrap();
        let first_envelope = RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &first,
            &first_payload,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(first_envelope.seq, 1);

        let stored_first: (i64, String) =
            sqlx::query_as("SELECT source_last_seq, event_json FROM runs JOIN run_events ON run_events.run_id = runs.id WHERE runs.id = ? AND run_events.seq = 1")
                .bind(id.to_string())
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(stored_first.0, 1);
        assert_eq!(
            stored_first.1,
            serde_json::to_string(&first_payload).unwrap()
        );

        let mut duplicate_first = store.pool.begin().await.unwrap();
        assert!(
            RunSummaryStore::insert_first_event_on_connection(
                &mut duplicate_first,
                &first,
                &first_payload,
            )
            .await
            .is_err()
        );
        duplicate_first.rollback().await.unwrap();

        let rolled_back_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);
        let rolled_back = entry(projection(rolled_back_id, "rollback", created_at), 1);
        let mut transaction = store.pool.begin().await.unwrap();
        RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &rolled_back,
            &created_payload(&rolled_back_id),
        )
        .await
        .unwrap();
        transaction.rollback().await.unwrap();
        let rolled_back_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE id = ?")
            .bind(rolled_back_id.to_string())
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(rolled_back_rows, 0);

        let invalid_id = run_id(created_at.timestamp_millis().cast_unsigned() + 2, 3);
        let invalid = entry(projection(invalid_id, "invalid", created_at), 1);
        let invalid_payload = sql_event_payload(
            &invalid_id,
            "run.title.updated",
            None,
            None,
            None,
            serde_json::json!({ "title": "too early" }),
        );
        let mut transaction = store.pool.begin().await.unwrap();
        let error = RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &invalid,
            &invalid_payload,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::RunEventMismatch {
            field: "event_name",
            ..
        }));
        transaction.rollback().await.unwrap();

        let rejected_id = run_id(created_at.timestamp_millis().cast_unsigned() + 3, 4);
        sqlx::query(
            "CREATE TRIGGER reject_test_event BEFORE INSERT ON run_events BEGIN SELECT RAISE(ABORT, 'rejected'); END",
        )
        .execute(&store.pool)
        .await
        .unwrap();
        let rejected = entry(projection(rejected_id, "rejected", created_at), 1);
        let mut transaction = store.pool.begin().await.unwrap();
        assert!(
            RunSummaryStore::insert_first_event_on_connection(
                &mut transaction,
                &rejected,
                &created_payload(&rejected_id),
            )
            .await
            .is_err()
        );
        transaction.rollback().await.unwrap();
        let rejected_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE id = ?")
            .bind(rejected_id.to_string())
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(rejected_rows, 0);
        sqlx::query("DROP TRIGGER reject_test_event")
            .execute(&store.pool)
            .await
            .unwrap();

        let mut updated_projection = projection(id, "updated", created_at);
        updated_projection.last_event_at = created_at + chrono::Duration::seconds(1);
        updated_projection.status = RunStatus::Running;
        let second = entry(updated_projection, 2);
        let second_payload = sql_event_payload(
            &id,
            "run.title.updated",
            None,
            None,
            None,
            serde_json::json!({ "title": "updated" }),
        );
        let mut transaction = store.pool.begin().await.unwrap();
        RunSummaryStore::append_event_on_connection(&mut transaction, 1, &second, &second_payload)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let updated_row: (i64, String, String, i64) = sqlx::query_as(
            "SELECT source_last_seq, status, title, last_event_at_ms FROM runs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(
            updated_row,
            (
                2,
                "running".to_string(),
                "updated".to_string(),
                (created_at + chrono::Duration::seconds(1)).timestamp_millis(),
            )
        );

        let mut stale = store.pool.begin().await.unwrap();
        let stale_error =
            RunSummaryStore::append_event_on_connection(&mut stale, 1, &second, &second_payload)
                .await
                .unwrap_err();
        assert!(matches!(stale_error, Error::RunHeadMismatch {
            expected_last_seq: 1,
            actual_last_seq: Some(2),
            ..
        }));
        stale.rollback().await.unwrap();

        let third = entry(projection(id, "third", created_at), 3);
        let third_payload = sql_event_payload(
            &id,
            "future.event",
            None,
            None,
            None,
            serde_json::json!({ "preserved": true }),
        );
        let mut mismatched_value = third_payload.as_value().clone();
        mismatched_value["run_id"] = rolled_back_id.to_string().into();
        let mismatched_payload: EventPayload = serde_json::from_value(mismatched_value).unwrap();
        let mut invalid = store.pool.begin().await.unwrap();
        assert!(
            RunSummaryStore::append_event_on_connection(
                &mut invalid,
                2,
                &third,
                &mismatched_payload,
            )
            .await
            .is_err()
        );
        invalid.rollback().await.unwrap();
        let mut rollback = store.pool.begin().await.unwrap();
        RunSummaryStore::append_event_on_connection(&mut rollback, 2, &third, &third_payload)
            .await
            .unwrap();
        rollback.rollback().await.unwrap();

        seed_sql_event(&store, &id, 3, &third_payload).await;
        let mut duplicate = store.pool.begin().await.unwrap();
        assert!(
            RunSummaryStore::append_event_on_connection(&mut duplicate, 2, &third, &third_payload,)
                .await
                .is_err()
        );
        duplicate.rollback().await.unwrap();
        let head_after_failures: i64 =
            sqlx::query_scalar("SELECT source_last_seq FROM runs WHERE id = ?")
                .bind(id.to_string())
                .fetch_one(&store.pool)
                .await
                .unwrap();
        assert_eq!(head_after_failures, 2);

        sqlx::query("DELETE FROM run_events WHERE run_id = ? AND seq = 3")
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        let mut transaction = store.pool.begin().await.unwrap();
        RunSummaryStore::append_event_on_connection(&mut transaction, 2, &third, &third_payload)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let sequences = sqlx::query_scalar::<_, i64>(
            "SELECT seq FROM run_events WHERE run_id = ? ORDER BY seq",
        )
        .bind(id.to_string())
        .fetch_all(&store.pool)
        .await
        .unwrap();
        assert_eq!(sequences, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn load_projection_replays_committed_events_and_reports_missing_run() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 31);
        let first = entry(projection(id, "created", created_at), 1);
        let first_payload = created_payload(&id);

        let mut transaction = store.pool.begin().await.unwrap();
        let first_envelope = RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &first,
            &first_payload,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let second = entry(projection(id, "updated", created_at), 2);
        let second_payload = sql_event_payload(
            &id,
            "run.title.updated",
            None,
            None,
            None,
            serde_json::json!({ "title": "updated" }),
        );
        let mut transaction = store.pool.begin().await.unwrap();
        let second_envelope = RunSummaryStore::append_event_on_connection(
            &mut transaction,
            1,
            &second,
            &second_payload,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let expected = RunProjection::apply_events(&[first_envelope, second_envelope]).unwrap();

        let loaded = store.load_projection(&id).await.unwrap();
        assert_eq!(loaded.run_id, id);
        assert_eq!(loaded.last_seq, 2);
        assert_eq!(
            serde_json::to_value(loaded.projection.as_ref()).unwrap(),
            serde_json::to_value(expected).unwrap()
        );

        let missing = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 32);
        assert!(matches!(
            store.load_projection(&missing).await,
            Err(Error::RunNotFound(text)) if text == missing.to_string()
        ));
    }

    #[tokio::test]
    async fn load_projection_reports_removed_events() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 33);
        let current = entry(projection(id, "created", created_at), 1);
        let mut transaction = store.pool.begin().await.unwrap();
        RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &current,
            &created_payload(&id),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        store.test_delete_run_events(&id).await.unwrap();

        assert!(matches!(
            store.load_projection(&id).await,
            Err(Error::RunHeadMismatch {
                expected_last_seq: 1,
                actual_last_seq: None,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn session_owner_lookup_resolves_only_typed_creation_events() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 31);
        store
            .upsert_projection(&entry(projection(id, "owner", created_at), 3))
            .await
            .unwrap();

        let owner_session = SessionId::new();
        seed_sql_event(
            &store,
            &id,
            2,
            &session_created_payload(&id, &owner_session),
        )
        .await;
        assert_eq!(
            store.find_session_owner(&owner_session).await.unwrap(),
            Some(id)
        );

        let non_owner_session = SessionId::new();
        let non_creation = sql_event_payload(
            &id,
            "run.session.future",
            None,
            None,
            Some(&non_owner_session),
            serde_json::json!({ "kind": "future" }),
        );
        seed_sql_event(&store, &id, 3, &non_creation).await;
        assert_eq!(
            store.find_session_owner(&non_owner_session).await.unwrap(),
            None
        );
        assert_eq!(
            store.find_session_owner(&SessionId::new()).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn session_owner_lookup_rejects_corrupt_selected_events() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 32);
        let other_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 33);
        store
            .upsert_projection(&entry(projection(id, "owner", created_at), 2))
            .await
            .unwrap();
        let session_id = SessionId::new();
        let payload = session_created_payload(&id, &session_id);
        seed_sql_event(&store, &id, 2, &payload).await;

        let mut wrong_event_name = payload.as_value().clone();
        wrong_event_name["event"] = "run.session.future".into();
        sqlx::query("UPDATE run_events SET event_json = ? WHERE run_id = ? AND seq = 2")
            .bind(serde_json::to_string(&wrong_event_name).unwrap())
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(matches!(
            store.find_session_owner(&session_id).await.unwrap_err(),
            Error::RunEventMismatch {
                field: "event_name",
                ..
            }
        ));

        seed_sql_event_restore(&store, &id, 2, &payload).await;
        let mut wrong_run = payload.as_value().clone();
        wrong_run["run_id"] = other_id.to_string().into();
        sqlx::query("UPDATE run_events SET event_json = ? WHERE run_id = ? AND seq = 2")
            .bind(serde_json::to_string(&wrong_run).unwrap())
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(matches!(
            store.find_session_owner(&session_id).await.unwrap_err(),
            Error::RunEventMismatch {
                field: "run_id",
                ..
            }
        ));

        seed_sql_event_restore(&store, &id, 2, &payload).await;
        let mut wrong_session = payload.as_value().clone();
        wrong_session["session_id"] = SessionId::new().to_string().into();
        sqlx::query("UPDATE run_events SET event_json = ? WHERE run_id = ? AND seq = 2")
            .bind(serde_json::to_string(&wrong_session).unwrap())
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(matches!(
            store.find_session_owner(&session_id).await.unwrap_err(),
            Error::RunEventMismatch {
                field: "session_id",
                ..
            }
        ));

        seed_sql_event_restore(&store, &id, 2, &payload).await;
        sqlx::query("UPDATE run_events SET event_json = '{}' WHERE run_id = ? AND seq = 2")
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(store.find_session_owner(&session_id).await.is_err());
    }

    #[tokio::test]
    async fn canonical_delete_waits_for_a_concurrent_writer() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 5);
        let first = entry(projection(id, "created", created_at), 1);
        let mut transaction = store.pool.begin().await.unwrap();
        RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &first,
            &created_payload(&id),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        store.test_mark_run_history_activated().await.unwrap();

        let blocker = store.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let contender = store.clone();
        let delete = tokio::spawn(async move { contender.delete_canonical(&id, 2).await });
        time::sleep(Duration::from_millis(25)).await;
        assert!(
            !delete.is_finished(),
            "delete should wait for the existing writer"
        );

        blocker.commit().await.unwrap();
        delete.await.unwrap().unwrap();
        assert!(!store.contains(&id).await.unwrap());
        assert!(store.test_is_run_history_tombstoned(&id).await.unwrap());
    }

    #[tokio::test]
    async fn sql_run_reads_preserve_paging_filters_json_and_legacy_gaps() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 11);
        let first = entry(projection(id, "created", created_at), 1);
        let mut transaction = store.pool.begin().await.unwrap();
        RunSummaryStore::insert_first_event_on_connection(
            &mut transaction,
            &first,
            &created_payload(&id),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let visit_one = StageId::new("work", 1);
        let visit_two = StageId::new("work", 2);
        let session_id = SessionId::new();
        let payloads = [
            sql_event_payload(
                &id,
                "future.stage",
                Some("work"),
                Some(&visit_one),
                None,
                serde_json::json!({ "kind": "visit-one" }),
            ),
            sql_event_payload(
                &id,
                "future.stage",
                Some("work"),
                Some(&visit_two),
                None,
                serde_json::json!({ "kind": "visit-two" }),
            ),
            sql_event_payload(
                &id,
                "future.legacy",
                Some("work"),
                None,
                None,
                serde_json::json!({ "kind": "legacy" }),
            ),
            sql_event_payload(
                &id,
                "run.session.future",
                None,
                None,
                Some(&session_id),
                serde_json::json!({ "redacted": "[REDACTED]" }),
            ),
            sql_event_payload(
                &id,
                "future.non_session",
                None,
                None,
                Some(&session_id),
                serde_json::json!({ "same_session": true }),
            ),
        ];
        for (index, payload) in payloads.iter().enumerate() {
            seed_sql_event(&store, &id, u32::try_from(index).unwrap() + 2, payload).await;
        }
        sqlx::query("UPDATE runs SET source_last_seq = 6 WHERE id = ?")
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();

        let mut connection = store.pool.acquire().await.unwrap();
        let all = RunSummaryStore::list_events_on_connection(&mut connection, &id)
            .await
            .unwrap();
        assert_eq!(seqs(&all), vec![1, 2, 3, 4, 5, 6]);
        let forward =
            RunSummaryStore::list_events_from_with_limit_on_connection(&mut connection, &id, 2, 2)
                .await
                .unwrap();
        assert_eq!(seqs(&forward), vec![2, 3, 4]);
        let reverse = RunSummaryStore::list_events_before_with_limit_on_connection(
            &mut connection,
            &id,
            Some(5),
            2,
        )
        .await
        .unwrap();
        assert_eq!(seqs(&reverse), vec![4, 3, 2]);
        let exact = RunSummaryStore::get_event_on_connection(&mut connection, &id, 5)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact.event.event_name(), "run.session.future");
        assert_eq!(
            serde_json::to_value(&exact.event).unwrap(),
            payloads[3].as_value().clone()
        );

        let visit_one_events =
            RunSummaryStore::list_events_for_stage_from_with_limit_on_connection(
                &mut connection,
                &id,
                &visit_one,
                1,
                10,
            )
            .await
            .unwrap();
        assert_eq!(seqs(&visit_one_events), vec![2, 4]);
        let visit_two_events =
            RunSummaryStore::list_events_for_stage_from_with_limit_on_connection(
                &mut connection,
                &id,
                &visit_two,
                1,
                10,
            )
            .await
            .unwrap();
        assert_eq!(seqs(&visit_two_events), vec![3]);
        let session_events =
            RunSummaryStore::list_events_for_session_from_with_limit_on_connection(
                &mut connection,
                &id,
                &session_id,
                1,
                10,
            )
            .await
            .unwrap();
        assert_eq!(seqs(&session_events), vec![5]);
        drop(connection);

        sqlx::query("DELETE FROM run_events WHERE run_id = ? AND seq = 3")
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        let mut connection = store.pool.acquire().await.unwrap();
        let gapped = RunSummaryStore::list_events_on_connection(&mut connection, &id)
            .await
            .unwrap();
        assert_eq!(gapped.last().unwrap().seq, 6);
        assert_eq!(gapped.len(), 5);
    }

    #[tokio::test]
    async fn sql_run_reads_reject_extracted_column_and_identity_corruption() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let id = run_id(created_at.timestamp_millis().cast_unsigned(), 21);
        let other_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 22);
        store
            .upsert_projection(&entry(projection(id, "one", created_at), 2))
            .await
            .unwrap();
        store
            .upsert_projection(&entry(projection(other_id, "two", created_at), 1))
            .await
            .unwrap();
        let stage_id = StageId::new("work", 1);
        let session_id = SessionId::new();
        let payload = sql_event_payload(
            &id,
            "run.session.future",
            Some("work"),
            Some(&stage_id),
            Some(&session_id),
            serde_json::json!({ "safe": true }),
        );
        seed_sql_event(&store, &id, 2, &payload).await;

        for (sql, field) in [
            (
                "UPDATE run_events SET event_name = 'wrong' WHERE run_id = ? AND seq = 2",
                "event_name",
            ),
            (
                "UPDATE run_events SET node_id = 'wrong' WHERE run_id = ? AND seq = 2",
                "node_id",
            ),
            (
                "UPDATE run_events SET stage_id = 'wrong@1' WHERE run_id = ? AND seq = 2",
                "stage_id",
            ),
            (
                "UPDATE run_events SET session_id = 'wrong' WHERE run_id = ? AND seq = 2",
                "session_id",
            ),
        ] {
            sqlx::query(sql)
                .bind(id.to_string())
                .execute(&store.pool)
                .await
                .unwrap();
            let mut connection = store.pool.acquire().await.unwrap();
            let error = RunSummaryStore::get_event_on_connection(&mut connection, &id, 2)
                .await
                .unwrap_err();
            assert!(matches!(
                &error,
                Error::RunEventMismatch {
                    field: mismatch_field,
                    ..
                } if *mismatch_field == field
            ));
            assert!(!error.to_string().contains(&session_id.to_string()));
            drop(connection);
            seed_sql_event_restore(&store, &id, 2, &payload).await;
        }

        let mut wrong_json = payload.as_value().clone();
        wrong_json["run_id"] = other_id.to_string().into();
        sqlx::query("UPDATE run_events SET event_json = ? WHERE run_id = ? AND seq = 2")
            .bind(serde_json::to_string(&wrong_json).unwrap())
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        let mut connection = store.pool.acquire().await.unwrap();
        assert!(matches!(
            RunSummaryStore::get_event_on_connection(&mut connection, &id, 2)
                .await
                .unwrap_err(),
            Error::RunEventMismatch {
                field: "run_id",
                ..
            }
        ));
        drop(connection);

        seed_sql_event_restore(&store, &id, 2, &payload).await;
        sqlx::query("UPDATE run_events SET run_id = ? WHERE run_id = ? AND seq = 2")
            .bind(other_id.to_string())
            .bind(id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        let row = sqlx::query(super::SELECT_EVENT_COLUMNS)
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert!(matches!(
            decode_event_row(&row, &id, &id.to_string()).unwrap_err(),
            Error::RunEventMismatch {
                field: "run_id",
                ..
            }
        ));
    }

    async fn seed_sql_event_restore(
        store: &RunSummaryStore,
        run_id: &RunId,
        seq: u32,
        payload: &EventPayload,
    ) {
        sqlx::query("DELETE FROM run_events WHERE run_id = ? AND seq = ?")
            .bind(run_id.to_string())
            .bind(i64::from(seq))
            .execute(&store.pool)
            .await
            .unwrap();
        seed_sql_event(store, run_id, seq, payload).await;
    }

    /// The migration's `CHECK (status IN (...))` freezes the status strings;
    /// prove every `RunStatusKind` variant passes it so an enum change that
    /// forgets a follow-up migration fails in CI instead of at runtime.
    #[tokio::test]
    async fn every_status_kind_upserts_within_schema_check() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        for (index, kind) in RunStatusKind::VARIANTS.iter().enumerate() {
            let id = run_id(
                created_at.timestamp_millis().cast_unsigned(),
                u128::try_from(index).unwrap() + 1,
            );
            let mut projected = projection(id, "status", created_at);
            projected.status = sample_status(*kind);
            store.upsert_projection(&entry(projected, 1)).await.unwrap();
        }
    }

    #[tokio::test]
    async fn upsert_is_monotonic_and_get_applies_children_count() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let parent_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let child_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);

        let parent = entry(projection(parent_id, "parent", created_at), 1);
        store.upsert_projection(&parent).await.unwrap();

        let mut child_projection = projection(child_id, "new title", created_at);
        child_projection.parent_id = Some(parent_id);
        child_projection.last_event_at = created_at + chrono::Duration::seconds(2);
        store
            .upsert_projection(&entry(child_projection, 2))
            .await
            .unwrap();

        let mut stale = projection(child_id, "stale title", created_at);
        stale.parent_id = Some(parent_id);
        store.upsert_projection(&entry(stale, 1)).await.unwrap();

        let parent = store.get(&parent_id, created_at).await.unwrap().unwrap();
        let child = store.get(&child_id, created_at).await.unwrap().unwrap();
        assert_eq!(parent.children_count, 1);
        assert_eq!(child.title, "new title");
    }

    #[tokio::test]
    async fn list_filters_sorts_and_paginates_in_sqlite() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let first_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let second_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);
        let archived_id = run_id(created_at.timestamp_millis().cast_unsigned() + 2, 3);

        let mut first = projection(first_id, "bravo", created_at);
        first.spec.automation = Some(AutomationRef {
            id:              "nightly".to_string(),
            name:            None,
            trigger_id:      None,
            workflow_source: None,
        });
        let mut second = projection(second_id, "alpha", created_at);
        second.spec.automation = Some(AutomationRef {
            id:              "nightly".to_string(),
            name:            None,
            trigger_id:      None,
            workflow_source: None,
        });
        let mut archived = projection(archived_id, "charlie", created_at);
        archived.archived_at = Some(created_at);
        for projected in [first, second, archived] {
            store.upsert_projection(&entry(projected, 1)).await.unwrap();
        }

        let page = store
            .list(
                &RunSummaryListQuery {
                    automation_id: Some("nightly".to_string()),
                    sort: RunSummarySort::Title,
                    direction: RunSummarySortDirection::Asc,
                    limit: 1,
                    ..RunSummaryListQuery::default()
                },
                created_at,
            )
            .await
            .unwrap();
        assert_eq!(page.total, 2);
        assert!(page.has_more);
        assert_eq!(page.data[0].title, "alpha");

        let archived = store
            .list(
                &RunSummaryListQuery {
                    visibility: RunSummaryVisibility::Selected {
                        statuses: Vec::new(),
                        archived: true,
                    },
                    ..RunSummaryListQuery::default()
                },
                created_at,
            )
            .await
            .unwrap();
        assert_eq!(archived.data.len(), 1);
        assert_eq!(archived.data[0].id, archived_id);
    }

    #[tokio::test]
    async fn list_all_is_unbounded_complete_and_newest_first() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let parent_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let child_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);
        let archived_id = run_id(created_at.timestamp_millis().cast_unsigned() + 2, 3);
        let removing_id = run_id(created_at.timestamp_millis().cast_unsigned() + 3, 4);
        let tied_low_id = run_id(created_at.timestamp_millis().cast_unsigned() + 4, 5);
        let tied_high_id = run_id(created_at.timestamp_millis().cast_unsigned() + 4, 6);

        let mut projections = vec![projection(parent_id, "parent", created_at)];
        let mut child = projection(
            child_id,
            "child",
            created_at + chrono::Duration::milliseconds(1),
        );
        child.parent_id = Some(parent_id);
        projections.push(child);
        let mut archived = projection(
            archived_id,
            "archived",
            created_at + chrono::Duration::milliseconds(2),
        );
        archived.archived_at = Some(created_at + chrono::Duration::milliseconds(2));
        projections.push(archived);
        let mut removing = projection(
            removing_id,
            "removing",
            created_at + chrono::Duration::milliseconds(3),
        );
        removing.status = RunStatus::Removing;
        projections.push(removing);
        projections.push(projection(
            tied_low_id,
            "tied-low",
            created_at + chrono::Duration::milliseconds(4),
        ));
        projections.push(projection(
            tied_high_id,
            "tied-high",
            created_at + chrono::Duration::milliseconds(4),
        ));
        for index in 0_u64..101 {
            let timestamp_ms = created_at.timestamp_millis().cast_unsigned() + 10 + index;
            projections.push(projection(
                run_id(timestamp_ms, u128::from(index) + 10),
                "bulk",
                DateTime::from_timestamp_millis(timestamp_ms.cast_signed()).unwrap(),
            ));
        }

        let mut expected_ids = Vec::new();
        for projected in projections {
            expected_ids.push(projected.spec.run_id);
            store.upsert_projection(&entry(projected, 1)).await.unwrap();
        }
        expected_ids.sort_by(|left, right| {
            right
                .created_at()
                .cmp(&left.created_at())
                .then_with(|| right.cmp(left))
        });

        let listed = store.list_all(created_at).await.unwrap();
        assert_eq!(listed.len(), 107);
        assert_eq!(
            listed.iter().map(|run| run.id).collect::<Vec<_>>(),
            expected_ids
        );
        assert!(listed.iter().any(|run| run.id == archived_id));
        assert!(listed.iter().any(|run| run.id == removing_id));
        assert_eq!(
            listed
                .iter()
                .find(|run| run.id == parent_id)
                .unwrap()
                .children_count,
            1
        );
        let tied = listed
            .iter()
            .filter(|run| run.id == tied_low_id || run.id == tied_high_id)
            .map(|run| run.id)
            .collect::<Vec<_>>();
        assert_eq!(tied, vec![tied_high_id, tied_low_id]);
    }

    #[tokio::test]
    async fn list_by_statuses_is_exact_and_empty_is_empty() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        for (index, kind) in RunStatusKind::VARIANTS.iter().enumerate() {
            let id = run_id(
                created_at.timestamp_millis().cast_unsigned() + u64::try_from(index).unwrap(),
                u128::try_from(index).unwrap() + 1,
            );
            let mut projected = projection(id, &kind.to_string(), id.created_at());
            projected.status = sample_status(*kind);
            store.upsert_projection(&entry(projected, 1)).await.unwrap();
        }

        let startup_statuses = [
            RunStatusKind::Starting,
            RunStatusKind::Running,
            RunStatusKind::Blocked,
            RunStatusKind::Paused,
            RunStatusKind::Removing,
        ];
        let listed = store
            .list_by_statuses(&startup_statuses, created_at)
            .await
            .unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|run| run.lifecycle.status.kind())
                .collect::<std::collections::HashSet<_>>(),
            startup_statuses.into_iter().collect()
        );
        assert_eq!(listed.len(), startup_statuses.len());
        assert!(
            store
                .list_by_statuses(&[], created_at)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn pull_request_creation_candidates_are_distinct_and_indexed_by_event_name() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let first_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let second_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);
        let unrelated_id = run_id(created_at.timestamp_millis().cast_unsigned() + 2, 3);
        for id in [first_id, second_id, unrelated_id] {
            store
                .upsert_projection(&entry(projection(id, "candidate", id.created_at()), 3))
                .await
                .unwrap();
        }
        for (run_id, seq) in [(first_id, 2), (first_id, 3), (second_id, 2)] {
            let request = sql_event_payload(
                &run_id,
                "pull_request.creation_requested",
                None,
                None,
                None,
                serde_json::json!({
                    "creation_id": PullRequestCreationId::new(),
                    "model": "test-model",
                    "force": false,
                }),
            );
            seed_sql_event(&store, &run_id, seq, &request).await;
        }

        let mut candidates = store
            .list_pull_request_creation_candidate_run_ids()
            .await
            .unwrap();
        candidates.sort_unstable();
        let mut expected = vec![first_id, second_id];
        expected.sort_unstable();
        assert_eq!(candidates, expected);
    }

    /// The candidate query must agree with the projection reducer about which
    /// later events resolve a creation request, so recovery replays only runs
    /// that are still plausibly pending and never skips one that is.
    #[tokio::test]
    async fn pull_request_creation_candidates_exclude_requests_resolved_by_later_events() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-08-27T12:00:00Z");
        let base = created_at.timestamp_millis().cast_unsigned();
        let link = serde_json::json!({ "owner": "acme", "repo": "widgets", "number": 7 });
        let created = serde_json::json!({
            "pr_url": "https://github.com/acme/widgets/pull/7",
            "pr_number": 7,
            "owner": "acme",
            "repo": "widgets",
            "base_branch": "main",
            "head_branch": "fabro/run/7",
            "title": "Widgets",
            "draft": false,
        });
        let request = |creation_id: PullRequestCreationId| {
            serde_json::json!({
                "creation_id": creation_id,
                "model": "test-model",
                "force": false,
            })
        };

        let resolved_by_created = run_id(base, 1);
        let resolved_by_linked = run_id(base + 1, 2);
        let resolved_by_unlinked = run_id(base + 2, 3);
        let resolved_by_matching_failure = run_id(base + 3, 4);
        let failed_for_other_request = run_id(base + 4, 5);
        let failed_without_creation_id = run_id(base + 5, 6);
        let retried_after_failure = run_id(base + 6, 7);
        let superseded_then_resolved = run_id(base + 7, 8);
        let all_ids = [
            resolved_by_created,
            resolved_by_linked,
            resolved_by_unlinked,
            resolved_by_matching_failure,
            failed_for_other_request,
            failed_without_creation_id,
            retried_after_failure,
            superseded_then_resolved,
        ];
        for id in all_ids {
            store
                .upsert_projection(&entry(projection(id, "candidate", id.created_at()), 9))
                .await
                .unwrap();
        }

        let seed = async |id: RunId, seq: u32, event: &str, properties: serde_json::Value| {
            let payload = sql_event_payload(&id, event, None, None, None, properties);
            seed_sql_event(&store, &id, seq, &payload).await;
        };

        let id_a = PullRequestCreationId::new();
        seed(
            resolved_by_created,
            2,
            "pull_request.creation_requested",
            request(id_a),
        )
        .await;
        seed(
            resolved_by_created,
            3,
            "pull_request.created",
            created.clone(),
        )
        .await;

        let id_b = PullRequestCreationId::new();
        seed(
            resolved_by_linked,
            2,
            "pull_request.creation_requested",
            request(id_b),
        )
        .await;
        seed(
            resolved_by_linked,
            3,
            "pull_request.linked",
            serde_json::json!({ "pull_request": link }),
        )
        .await;

        let id_c = PullRequestCreationId::new();
        seed(
            resolved_by_unlinked,
            2,
            "pull_request.creation_requested",
            request(id_c),
        )
        .await;
        seed(
            resolved_by_unlinked,
            3,
            "pull_request.unlinked",
            serde_json::json!({ "pull_request": link }),
        )
        .await;

        let id_d = PullRequestCreationId::new();
        seed(
            resolved_by_matching_failure,
            2,
            "pull_request.creation_requested",
            request(id_d),
        )
        .await;
        seed(
            resolved_by_matching_failure,
            3,
            "pull_request.failed",
            serde_json::json!({ "creation_id": id_d, "error": "boom" }),
        )
        .await;

        let id_e = PullRequestCreationId::new();
        seed(
            failed_for_other_request,
            2,
            "pull_request.creation_requested",
            request(id_e),
        )
        .await;
        seed(
            failed_for_other_request,
            3,
            "pull_request.failed",
            serde_json::json!({ "creation_id": PullRequestCreationId::new(), "error": "other" }),
        )
        .await;

        let id_f = PullRequestCreationId::new();
        seed(
            failed_without_creation_id,
            2,
            "pull_request.creation_requested",
            request(id_f),
        )
        .await;
        seed(
            failed_without_creation_id,
            3,
            "pull_request.failed",
            serde_json::json!({ "error": "publish stage failure" }),
        )
        .await;

        let id_g = PullRequestCreationId::new();
        seed(
            retried_after_failure,
            2,
            "pull_request.creation_requested",
            request(id_g),
        )
        .await;
        seed(
            retried_after_failure,
            3,
            "pull_request.failed",
            serde_json::json!({ "creation_id": id_g, "error": "boom" }),
        )
        .await;
        seed(
            retried_after_failure,
            4,
            "pull_request.creation_requested",
            request(PullRequestCreationId::new()),
        )
        .await;

        let id_h = PullRequestCreationId::new();
        seed(
            superseded_then_resolved,
            2,
            "pull_request.creation_requested",
            request(id_h),
        )
        .await;
        seed(
            superseded_then_resolved,
            3,
            "pull_request.creation_requested",
            request(PullRequestCreationId::new()),
        )
        .await;
        seed(superseded_then_resolved, 4, "pull_request.created", created).await;

        let mut candidates = store
            .list_pull_request_creation_candidate_run_ids()
            .await
            .unwrap();
        candidates.sort_unstable();
        let mut expected = vec![
            failed_for_other_request,
            failed_without_creation_id,
            retried_after_failure,
        ];
        expected.sort_unstable();
        assert_eq!(candidates, expected);
    }

    #[tokio::test]
    async fn projection_persists_billing_diff_and_derived_size() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let run_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let mut projection = projection(run_id, "billed", created_at);
        projection.spec.automation = Some(AutomationRef {
            id:              "nightly".to_string(),
            name:            None,
            trigger_id:      None,
            workflow_source: None,
        });
        projection.status = RunStatus::Succeeded {
            reason: SuccessReason::Completed,
        };
        projection.last_event_at = created_at + chrono::Duration::minutes(1);
        projection.conclusion = Some(Conclusion {
            timestamp:            projection.last_event_at,
            status:               StageOutcome::Succeeded,
            timing:               RunTiming::wall_only(60_000),
            failure:              None,
            final_git_commit_sha: None,
            stages:               Vec::new(),
            billing:              Some(BilledTokenCounts {
                input_tokens:       100,
                output_tokens:      20,
                total_tokens:       135,
                reasoning_tokens:   5,
                cache_read_tokens:  10,
                cache_write_tokens: 0,
                total_usd_micros:   Some(21_000_000),
            }),
            total_retries:        0,
            diff:                 RunDiff {
                patch:   None,
                summary: Some(DiffSummary {
                    files_changed: 2,
                    additions:     10,
                    deletions:     3,
                }),
            },
        });
        store
            .upsert_projection(&entry(projection, 4))
            .await
            .unwrap();

        let row = sqlx::query(
            "SELECT source_last_seq, created_at_ms, last_event_at_ms, status, title, workflow_slug, \
             automation_id, input_tokens, reasoning_tokens, cache_read_tokens, total_usd_micros, \
             diff_files_changed, diff_additions, diff_deletions FROM runs WHERE id = ?",
        )
        .bind(run_id.to_string())
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "source_last_seq"), 4);
        assert_eq!(
            sqlx::Row::get::<i64, _>(&row, "created_at_ms"),
            created_at.timestamp_millis()
        );
        assert_eq!(
            sqlx::Row::get::<i64, _>(&row, "last_event_at_ms"),
            (created_at + chrono::Duration::minutes(1)).timestamp_millis()
        );
        assert_eq!(sqlx::Row::get::<String, _>(&row, "status"), "succeeded");
        assert_eq!(sqlx::Row::get::<String, _>(&row, "title"), "billed");
        assert_eq!(
            sqlx::Row::get::<String, _>(&row, "workflow_slug"),
            "test-workflow"
        );
        assert_eq!(
            sqlx::Row::get::<String, _>(&row, "automation_id"),
            "nightly"
        );
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "input_tokens"), 100);
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "reasoning_tokens"), 5);
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "cache_read_tokens"), 10);
        assert_eq!(
            sqlx::Row::get::<i64, _>(&row, "total_usd_micros"),
            21_000_000
        );
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "diff_files_changed"), 2);
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "diff_additions"), 10);
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "diff_deletions"), 3);

        let run = store.get(&run_id, created_at).await.unwrap().unwrap();
        assert_eq!(run.size, RunSize::S);
    }

    #[tokio::test]
    async fn projection_normalizes_legacy_overlapping_reasoning_tokens() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let run_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let mut projection = projection(run_id, "legacy billing", created_at);
        projection.conclusion = Some(Conclusion {
            timestamp:            created_at,
            status:               StageOutcome::Succeeded,
            timing:               RunTiming::default(),
            failure:              None,
            final_git_commit_sha: None,
            stages:               Vec::new(),
            billing:              Some(BilledTokenCounts {
                input_tokens: 53,
                output_tokens: -7,
                total_tokens: 112,
                reasoning_tokens: 66,
                ..BilledTokenCounts::default()
            }),
            total_retries:        0,
            diff:                 RunDiff::default(),
        });

        store
            .upsert_projection(&entry(projection, 1))
            .await
            .unwrap();

        let row = sqlx::query(
            "SELECT input_tokens, output_tokens, reasoning_tokens FROM runs WHERE id = ?",
        )
        .bind(run_id.to_string())
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "input_tokens"), 53);
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "output_tokens"), 0);
        assert_eq!(sqlx::Row::get::<i64, _>(&row, "reasoning_tokens"), 59);
    }

    #[tokio::test]
    async fn reconcile_removes_rows_absent_from_authoritative_entries() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let kept_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let removed_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);
        let kept = entry(projection(kept_id, "kept", created_at), 1);
        let removed = entry(projection(removed_id, "removed", created_at), 1);
        store.upsert_projection(&kept).await.unwrap();
        store.upsert_projection(&removed).await.unwrap();

        store.reconcile(std::slice::from_ref(&kept)).await.unwrap();

        assert!(store.get(&kept_id, created_at).await.unwrap().is_some());
        assert!(store.get(&removed_id, created_at).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn failed_reconcile_rolls_back_and_can_be_retried() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let stale_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let good_id = run_id(created_at.timestamp_millis().cast_unsigned() + 1, 2);
        let recovered_id = run_id(created_at.timestamp_millis().cast_unsigned() + 2, 3);
        store
            .upsert_projection(&entry(projection(stale_id, "stale", created_at), 1))
            .await
            .unwrap();

        let good = entry(projection(good_id, "good", created_at), 1);
        let recovered_projection = projection(recovered_id, "recovered", created_at);
        let invalid = entry(recovered_projection.clone(), 0);

        assert!(store.reconcile(&[good.clone(), invalid]).await.is_err());
        assert!(store.get(&stale_id, created_at).await.unwrap().is_some());
        assert!(store.get(&good_id, created_at).await.unwrap().is_none());

        let recovered = entry(recovered_projection, 1);
        store.reconcile(&[good, recovered]).await.unwrap();

        assert!(store.get(&stale_id, created_at).await.unwrap().is_none());
        assert!(store.get(&good_id, created_at).await.unwrap().is_some());
        assert!(
            store
                .get(&recovered_id, created_at)
                .await
                .unwrap()
                .is_some()
        );
    }
}
