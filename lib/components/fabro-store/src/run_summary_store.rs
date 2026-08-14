use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use fabro_types::{BilledTokenCounts, Run, RunId, RunSize, RunStatusKind, RunTiming, timing};
use sqlx::sqlite::{SqliteConnection, SqliteRow};
use sqlx::{QueryBuilder, Row as _, Sqlite, SqlitePool};
use strum::VariantArray as _;

use crate::run_state::projected_billing;
use crate::slate::CachedRunProjection;
use crate::{Error, Result};

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

    pub(crate) async fn upsert_projection(&self, entry: &CachedRunProjection) -> Result<()> {
        let record = ProjectedRunSummary::from_entry(entry);
        let mut connection = self.pool.acquire().await?;
        upsert_run(&mut connection, &record).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn close_pool(&self) {
        self.pool.close().await;
    }

    pub(crate) async fn reconcile(&self, entries: &[CachedRunProjection]) -> Result<()> {
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
            upsert_run(&mut transaction, &ProjectedRunSummary::from_entry(entry)).await?;
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

    pub async fn get(&self, run_id: &RunId, now: DateTime<Utc>) -> Result<Option<Run>> {
        let mut query = QueryBuilder::<Sqlite>::new(SELECT_RUN_SUMMARIES_SQL);
        query
            .push(" WHERE runs.id = ")
            .push_bind(run_id.to_string());
        let row = query.build().fetch_optional(&self.pool).await?;
        row.map(|row| decode_run_row(&row, now)).transpose()
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
                let id = stored_id
                    .parse::<RunId>()
                    .map_err(|_| Error::RunSummaryMismatch {
                        run_id: stored_id,
                        field:  "id",
                    })?;
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

        let data = rows
            .iter()
            .map(|row| decode_run_row(row, now))
            .collect::<Result<Vec<_>>>()?;
        let total = u64::try_from(total).expect("COUNT(*) is non-negative");
        let consumed = u64::from(query.offset).saturating_add(data.len() as u64);
        Ok(RunSummaryPage {
            data,
            total,
            has_more: consumed < total,
        })
    }

    pub async fn delete(&self, run_id: &RunId) -> Result<()> {
        sqlx::query("DELETE FROM runs WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
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
struct ProjectedRunSummary {
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

impl ProjectedRunSummary {
    fn from_entry(entry: &CachedRunProjection) -> Self {
        let mut run = entry.summary.clone();
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

async fn upsert_run(connection: &mut SqliteConnection, record: &ProjectedRunSummary) -> Result<()> {
    let run = &record.run;
    let diff = run.diff.unwrap_or_default();
    let summary_json = serde_json::to_string(run)?;
    sqlx::query(UPSERT_RUN_SQL)
        .bind(run.id.to_string())
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
        .bind(summary_json)
        .execute(connection)
        .await?;
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

    use chrono::{DateTime, Utc};
    use fabro_types::{
        AutomationRef, BilledTokenCounts, BlockedReason, Conclusion, DiffSummary, FailureReason,
        Graph, PendingReason, RunDiff, RunId, RunProjection, RunSize, RunSpec, RunStatus,
        RunStatusKind, RunTiming, StageOutcome, SuccessReason, WorkflowSettings, test_support,
    };
    use strum::VariantArray as _;
    use ulid::Ulid;

    use super::{
        RunSummaryListQuery, RunSummarySort, RunSummarySortDirection, RunSummaryStore,
        RunSummaryVisibility,
    };
    use crate::slate::CachedRunProjection;
    use crate::test_support as store_test_support;

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
                automation: None,
                source_directory: None,
                labels: HashMap::new(),
                provenance: test_support::test_run_provenance(),
                manifest_blob: None,
                definition_blob: None,
                git: None,
                fork_source_ref: None,
            },
            created_at,
        )
    }

    fn entry(projection: RunProjection, last_seq: u32) -> CachedRunProjection {
        CachedRunProjection::from_projection(projection.spec.run_id, projection, last_seq)
    }

    async fn store() -> (tempfile::TempDir, RunSummaryStore) {
        store_test_support::sqlite_summary_store().await
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
            id:         "nightly".to_string(),
            name:       None,
            trigger_id: None,
        });
        let mut second = projection(second_id, "alpha", created_at);
        second.spec.automation = Some(AutomationRef {
            id:         "nightly".to_string(),
            name:       None,
            trigger_id: None,
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
    async fn projection_persists_billing_diff_and_derived_size() {
        let (_directory, store) = store().await;
        let created_at = dt("2026-07-11T12:00:00Z");
        let run_id = run_id(created_at.timestamp_millis().cast_unsigned(), 1);
        let mut projection = projection(run_id, "billed", created_at);
        projection.spec.automation = Some(AutomationRef {
            id:         "nightly".to_string(),
            name:       None,
            trigger_id: None,
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
