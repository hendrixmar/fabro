//! Temporary compatibility importer for legacy SlateDB run history.
//!
//! Keep this source reader, its reports, and its verification path through the
//! 30-day run-history compatibility window. Remove them only after the
//! production evidence gate for that window has been accepted.

use std::collections::HashSet;
use std::error::Error as StdError;
use std::fmt;

use fabro_types::{EventEnvelope, RunEvent, RunId};
use sha2::{Digest as _, Sha256};
use sqlx::SqlitePool;
#[cfg(test)]
use tokio::sync::Barrier;
use tracing::debug;

use crate::keys::SlateKey;
use crate::run_state::ProjectedRun;
use crate::{Database, EventPayload, RunSummaryStore, keys};

/// Count-only observations about the legacy catalog and session indexes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyRunHistoryDiagnostics {
    pub catalog_markers:       u64,
    pub empty_catalog_markers: u64,
    pub session_reverse_rows:  u64,
}

/// Durable progress from one legacy run-history import attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyRunHistoryImportReport {
    pub scanned_source_runs:            u64,
    pub scanned_source_events:          u64,
    pub imported_runs:                  u64,
    pub imported_events:                u64,
    pub verified_existing_runs:         u64,
    pub verified_existing_events:       u64,
    pub discarded_projection_only_rows: u64,
    pub committed_run_transactions:     u64,
    pub tombstoned_source_runs:         u64,
    pub tombstoned_source_events:       u64,
    pub diagnostics:                    LegacyRunHistoryDiagnostics,
}

/// Aggregate proof from legacy-prefix and full-SQL-destination verification.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyRunHistoryVerificationReport {
    pub source_runs:              u64,
    pub source_events:            u64,
    pub matched_prefix_runs:      u64,
    pub matched_prefix_events:    u64,
    pub target_runs:              u64,
    pub target_events:            u64,
    pub sql_only_runs:            u64,
    pub sql_only_events:          u64,
    pub tombstoned_source_runs:   u64,
    pub tombstoned_source_events: u64,
    pub diagnostics:              LegacyRunHistoryDiagnostics,
}

/// Stable, aggregate-only identity of the exact legacy run-event source.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct LegacyRunHistorySourceIdentity {
    fingerprint: [u8; 32],
    pub runs:    u64,
    pub events:  u64,
}

impl LegacyRunHistorySourceIdentity {
    #[must_use]
    pub fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
}

impl fmt::Debug for LegacyRunHistorySourceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistorySourceIdentity")
            .field("runs", &self.runs)
            .field("events", &self.events)
            .finish_non_exhaustive()
    }
}

/// Failure while strictly identifying the legacy run-event source.
pub struct LegacyRunHistorySourceIdentityError {
    failure: LegacyRunHistorySourceFailure,
}

impl From<LegacyRunHistorySourceFailure> for LegacyRunHistorySourceIdentityError {
    fn from(failure: LegacyRunHistorySourceFailure) -> Self {
        Self { failure }
    }
}

impl fmt::Debug for LegacyRunHistorySourceIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistorySourceIdentityError")
            .field("failure", &self.failure.kind())
            .finish()
    }
}

impl fmt::Display for LegacyRunHistorySourceIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "identifying the legacy run-history source: {}",
            self.failure
        )
    }
}

impl StdError for LegacyRunHistorySourceIdentityError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.failure)
    }
}

/// An import failure plus the durable progress completed before it.
pub struct LegacyRunHistoryImportError {
    report:  LegacyRunHistoryImportReport,
    failure: LegacyRunHistoryImportFailure,
}

impl LegacyRunHistoryImportError {
    #[must_use]
    pub fn report(&self) -> &LegacyRunHistoryImportReport {
        &self.report
    }

    /// Returns secondary errors encountered while rolling back a failed run
    /// import transaction.
    ///
    /// The standard error source chain preserves the failure that interrupted
    /// the import. Because that chain is linear, rollback errors are exposed
    /// separately.
    pub fn cleanup_errors(&self) -> impl Iterator<Item = &(dyn StdError + 'static)> {
        let mut errors = Vec::new();
        self.failure.collect_cleanup_errors(&mut errors);
        errors.into_iter()
    }
}

impl fmt::Debug for LegacyRunHistoryImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistoryImportError")
            .field("report", &self.report)
            .field("failure", &self.failure.kind())
            .finish()
    }
}

impl fmt::Display for LegacyRunHistoryImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy run-history import failed after scanning {} runs and committing {} run transactions: {}",
            self.report.scanned_source_runs, self.report.committed_run_transactions, self.failure
        )
    }
}

impl StdError for LegacyRunHistoryImportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.failure.primary_failure())
    }
}

/// A verification failure plus the aggregate proof completed before it.
pub struct LegacyRunHistoryVerificationError {
    report:  LegacyRunHistoryVerificationReport,
    failure: LegacyRunHistoryVerificationFailure,
}

impl LegacyRunHistoryVerificationError {
    #[must_use]
    pub fn report(&self) -> &LegacyRunHistoryVerificationReport {
        &self.report
    }
}

impl fmt::Debug for LegacyRunHistoryVerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistoryVerificationError")
            .field("report", &self.report)
            .field("failure", &self.failure.kind())
            .finish()
    }
}

impl fmt::Display for LegacyRunHistoryVerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy run-history verification failed after checking {} source runs and {} target runs: {}",
            self.report.source_runs, self.report.target_runs, self.failure
        )
    }
}

impl StdError for LegacyRunHistoryVerificationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.failure)
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyRunHistoryImportFailure {
    #[error("reading and validating the legacy run-history source")]
    Source(#[source] LegacyRunHistorySourceFailure),
    #[error("reading legacy run-history activation state")]
    ActivationState(#[source] sqlx::Error),
    #[error("reading legacy run-history deletion tombstones")]
    DeletionState(#[source] sqlx::Error),
    #[error("a legacy run-history deletion tombstone still has canonical SQLite data")]
    TombstonedDestinationPresent,
    #[error("starting the projection-only row cleanup transaction")]
    BeginCleanup(#[source] sqlx::Error),
    #[error("deleting projection-only run rows")]
    DeleteProjectionOnlyRows(#[source] sqlx::Error),
    #[error("committing the projection-only row cleanup")]
    CommitCleanup(#[source] sqlx::Error),
    #[error("starting a per-run import transaction")]
    BeginRunTransaction(#[source] sqlx::Error),
    #[error("reading an existing destination run history")]
    ReadDestination(#[source] crate::Error),
    #[error("the destination history is partial or conflicts with the legacy prefix")]
    DestinationConflict,
    #[error("SQLite is missing an activated legacy run-history prefix")]
    MissingDestinationAfterActivation,
    #[error("replaying an existing destination run history")]
    ReplayDestination(#[source] crate::Error),
    #[error("verifying an existing destination run row")]
    VerifyDestination(#[source] crate::Error),
    #[error("inserting the imported final run row")]
    InsertRun(#[source] crate::Error),
    #[error("inserting an imported run event")]
    InsertEvent(#[source] crate::Error),
    #[error("committing a per-run import transaction")]
    CommitRunTransaction(#[source] sqlx::Error),
    #[error("rolling back a failed per-run import transaction")]
    RollbackRunTransaction {
        #[source]
        source: sqlx::Error,
        prior:  Box<Self>,
    },
    #[error("collecting count-only legacy index diagnostics")]
    Diagnostics(#[source] LegacyRunHistoryDiagnosticsFailure),
    #[error("a legacy run-history import counter overflowed")]
    CounterOverflow,
}

impl LegacyRunHistoryImportFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }

    fn primary_failure(&self) -> &Self {
        match self {
            Self::RollbackRunTransaction { prior, .. } => prior.primary_failure(),
            _ => self,
        }
    }

    fn collect_cleanup_errors<'a>(&'a self, errors: &mut Vec<&'a (dyn StdError + 'static)>) {
        if let Self::RollbackRunTransaction { source, prior } = self {
            errors.push(source);
            prior.collect_cleanup_errors(errors);
        }
    }
}

impl fmt::Debug for LegacyRunHistoryImportFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("LegacyRunHistoryImportFailure");
        debug.field("kind", &self.kind());
        if let Self::RollbackRunTransaction { prior, .. } = self {
            debug.field("prior_failure", &prior.kind());
        }
        debug.finish()
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyRunHistoryVerificationFailure {
    #[error("reading and validating the legacy run-history source")]
    Source(#[source] LegacyRunHistorySourceFailure),
    #[error("reading legacy run-history deletion tombstones")]
    DeletionState(#[source] sqlx::Error),
    #[error("a legacy run-history deletion tombstone still has canonical SQLite data")]
    TombstonedDestinationPresent,
    #[error("acquiring a SQLite verification connection")]
    AcquireConnection(#[source] sqlx::Error),
    #[error("reading a destination run history")]
    ReadDestination(#[source] crate::Error),
    #[error("SQLite is missing all or part of a legacy run-history prefix")]
    MissingDestinationPrefix,
    #[error("a destination run-history prefix conflicts with legacy JSON or sequence identity")]
    DestinationPrefixConflict,
    #[error("enumerating destination run rows")]
    ListDestinationRuns(#[source] sqlx::Error),
    #[error("a destination run row has an invalid identity")]
    InvalidDestinationRunId,
    #[error("a destination run has no event history")]
    EmptyDestinationHistory,
    #[error("replaying a destination run history")]
    ReplayDestination(#[source] crate::Error),
    #[error("verifying a destination run row")]
    VerifyDestination(#[source] crate::Error),
    #[error("collecting count-only legacy index diagnostics")]
    Diagnostics(#[source] LegacyRunHistoryDiagnosticsFailure),
    #[error("a legacy run-history verification counter overflowed")]
    CounterOverflow,
}

impl LegacyRunHistoryVerificationFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Debug for LegacyRunHistoryVerificationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistoryVerificationFailure")
            .field("kind", &self.kind())
            .finish()
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyRunHistorySourceFailure {
    #[error("opening the legacy run-history source")]
    OpenSource(#[source] crate::Error),
    #[error("opening the legacy run-history scan")]
    OpenScan(#[source] slatedb::Error),
    #[error("reading the legacy run-history scan")]
    ReadScan(#[source] slatedb::Error),
    #[error("a legacy run-event key is not UTF-8")]
    KeyUtf8(#[source] std::str::Utf8Error),
    #[error("a legacy run-event key is not canonical")]
    InvalidKey,
    #[error("a legacy run-event value is not UTF-8")]
    ValueUtf8(#[source] std::str::Utf8Error),
    #[error("a legacy run-event value is not valid JSON")]
    DecodeEvent(#[source] serde_json::Error),
    #[error("a legacy run-event value does not match its key or event contract")]
    ValidateEvent(#[source] crate::Error),
    #[error("a legacy run history has invalid sequence ordering")]
    InvalidSequence,
    #[error("a legacy run history does not begin with sequence 1 run.created")]
    InvalidFirstEvent,
    #[error("replaying a legacy run history")]
    Replay(#[source] crate::Error),
    #[error("a legacy run-history source counter overflowed")]
    CounterOverflow,
}

impl LegacyRunHistorySourceFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Debug for LegacyRunHistorySourceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistorySourceFailure")
            .field("kind", &self.kind())
            .finish()
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyRunHistoryDiagnosticsFailure {
    #[error("reading the legacy run catalog")]
    ReadCatalog(#[source] slatedb::Error),
    #[error("a legacy run-catalog key is not UTF-8")]
    CatalogKeyUtf8(#[source] std::str::Utf8Error),
    #[error("a legacy run-catalog key is not canonical")]
    InvalidCatalogKey,
    #[error("opening the legacy run source for diagnostics")]
    OpenSource(#[source] crate::Error),
    #[error("opening a legacy run-event probe")]
    OpenEventProbe(#[source] slatedb::Error),
    #[error("reading a legacy run-event probe")]
    ReadEventProbe(#[source] slatedb::Error),
    #[error("opening the legacy session reverse-row scan")]
    OpenSessionScan(#[source] slatedb::Error),
    #[error("reading the legacy session reverse-row scan")]
    ReadSessionScan(#[source] slatedb::Error),
    #[error("a legacy run-history diagnostics counter overflowed")]
    CounterOverflow,
}

impl LegacyRunHistoryDiagnosticsFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Debug for LegacyRunHistoryDiagnosticsFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyRunHistoryDiagnosticsFailure")
            .field("kind", &self.kind())
            .finish()
    }
}

#[derive(Default)]
struct ImportControls {
    #[cfg(test)]
    source_after_events: Option<u64>,
    #[cfg(test)]
    after_run_inserted:  Option<std::sync::Arc<Barrier>>,
}

impl ImportControls {
    fn source_scan_error(&self, observed_events: u64) -> Option<slatedb::Error> {
        #[cfg(test)]
        if self.source_after_events == Some(observed_events) {
            return Some(slatedb::Error::unavailable(
                "injected legacy run-history source failure".to_owned(),
            ));
        }
        let _ = (self, observed_events);
        None
    }

    #[cfg(test)]
    async fn after_run_inserted(&self) {
        if let Some(barrier) = &self.after_run_inserted {
            barrier.wait().await;
            barrier.wait().await;
        }
    }
}

struct ValidatedLegacyRunEvent {
    run_id:     RunId,
    payload:    EventPayload,
    envelope:   EventEnvelope,
    event_json: String,
    raw_key:    Vec<u8>,
}

struct ValidatedLegacyRunHistory {
    run_id:  RunId,
    events:  Vec<ValidatedLegacyRunEvent>,
    current: ProjectedRun,
}

struct LegacyRunHistorySource {
    entries:         slatedb::DbIterator,
    buffered:        Option<slatedb::KeyValue>,
    observed_events: u64,
}

impl LegacyRunHistorySource {
    async fn open(database: &Database) -> Result<Self, LegacyRunHistorySourceFailure> {
        let source = database
            .open_db()
            .await
            .map_err(LegacyRunHistorySourceFailure::OpenSource)?;
        let prefix = SlateKey::new("runs").into_prefix();
        let entries = source
            .scan_prefix(prefix)
            .await
            .map_err(LegacyRunHistorySourceFailure::OpenScan)?;
        Ok(Self {
            entries,
            buffered: None,
            observed_events: 0,
        })
    }

    async fn next_run(
        &mut self,
        controls: Option<&ImportControls>,
    ) -> Result<Option<ValidatedLegacyRunHistory>, LegacyRunHistorySourceFailure> {
        let first = if let Some(entry) = self.buffered.take() {
            parse_source_event(&entry.key, &entry.value)?
                .expect("the buffered source entry belongs to the event namespace")
        } else {
            let Some(event) = self.next_event(controls).await? else {
                return Ok(None);
            };
            event
        };
        let run_id = first.run_id;
        let run_id_text = run_id.to_string();
        let mut events = vec![first];
        loop {
            let Some(entry) = self.next_event_entry(controls).await? else {
                break;
            };
            if event_run_segment(&entry.key) != Some(run_id_text.as_bytes()) {
                self.buffered = Some(entry);
                break;
            }
            let event = parse_source_event(&entry.key, &entry.value)?
                .expect("an event-namespace entry parses as an event or fails");
            events.push(event);
        }

        if events[0].envelope.seq != 1 || events[0].envelope.event.event_name() != "run.created" {
            return Err(LegacyRunHistorySourceFailure::InvalidFirstEvent);
        }
        if events
            .windows(2)
            .any(|pair| pair[0].envelope.seq >= pair[1].envelope.seq)
        {
            return Err(LegacyRunHistorySourceFailure::InvalidSequence);
        }
        let envelopes = events
            .iter()
            .map(|event| event.envelope.clone())
            .collect::<Vec<_>>();
        let current = ProjectedRun::replay(run_id, &envelopes)
            .map_err(LegacyRunHistorySourceFailure::Replay)?;
        Ok(Some(ValidatedLegacyRunHistory {
            run_id,
            events,
            current,
        }))
    }

    async fn next_event(
        &mut self,
        controls: Option<&ImportControls>,
    ) -> Result<Option<ValidatedLegacyRunEvent>, LegacyRunHistorySourceFailure> {
        let Some(entry) = self.next_event_entry(controls).await? else {
            return Ok(None);
        };
        parse_source_event(&entry.key, &entry.value)
    }

    async fn next_event_entry(
        &mut self,
        controls: Option<&ImportControls>,
    ) -> Result<Option<slatedb::KeyValue>, LegacyRunHistorySourceFailure> {
        loop {
            if let Some(source) =
                controls.and_then(|controls| controls.source_scan_error(self.observed_events))
            {
                return Err(LegacyRunHistorySourceFailure::ReadScan(source));
            }
            let Some(entry) = self
                .entries
                .next()
                .await
                .map_err(LegacyRunHistorySourceFailure::ReadScan)?
            else {
                return Ok(None);
            };
            if event_run_segment(&entry.key).is_none() {
                continue;
            }
            self.observed_events = self
                .observed_events
                .checked_add(1)
                .ok_or(LegacyRunHistorySourceFailure::CounterOverflow)?;
            return Ok(Some(entry));
        }
    }
}

impl Database {
    /// Strictly identifies the exact legacy run-event key/value stream.
    pub async fn legacy_run_history_source_identity(
        &self,
    ) -> Result<LegacyRunHistorySourceIdentity, LegacyRunHistorySourceIdentityError> {
        const DOMAIN_SEPARATOR: &[u8] = b"fabro.legacy-run-history-source.v1\0";

        let mut source = LegacyRunHistorySource::open(self).await?;
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN_SEPARATOR);
        let mut runs = 0_u64;
        let mut events = 0_u64;
        while let Some(history) = source.next_run(None).await? {
            runs = runs
                .checked_add(1)
                .ok_or(LegacyRunHistorySourceFailure::CounterOverflow)?;
            for event in &history.events {
                hash_source_part(&mut hasher, &event.raw_key)?;
                hash_source_part(&mut hasher, event.event_json.as_bytes())?;
                events = events
                    .checked_add(1)
                    .ok_or(LegacyRunHistorySourceFailure::CounterOverflow)?;
            }
        }
        Ok(LegacyRunHistorySourceIdentity {
            fingerprint: hasher.finalize().into(),
            runs,
            events,
        })
    }

    /// Strictly imports legacy SlateDB run history into the inactive SQLite
    /// run store, committing one complete run at a time.
    ///
    /// The caller must prevent writes to both stores for the duration of the
    /// import. This operation does not establish a cross-store snapshot.
    pub async fn import_legacy_run_history_into(
        &self,
        pool: &SqlitePool,
    ) -> Result<LegacyRunHistoryImportReport, LegacyRunHistoryImportError> {
        self.import_legacy_run_history_with_controls(pool, &ImportControls::default())
            .await
    }

    async fn import_legacy_run_history_with_controls(
        &self,
        pool: &SqlitePool,
        controls: &ImportControls,
    ) -> Result<LegacyRunHistoryImportReport, LegacyRunHistoryImportError> {
        let mut report = LegacyRunHistoryImportReport::default();
        let result = self
            .run_legacy_run_history_import(pool, controls, &mut report)
            .await;
        debug_import_outcome(result.as_ref().map_or("failed", |()| "complete"), &report);
        match result {
            Ok(()) => Ok(report),
            Err(failure) => Err(LegacyRunHistoryImportError { report, failure }),
        }
    }

    async fn run_legacy_run_history_import(
        &self,
        pool: &SqlitePool,
        controls: &ImportControls,
        report: &mut LegacyRunHistoryImportReport,
    ) -> Result<(), LegacyRunHistoryImportFailure> {
        let activated = legacy_run_history_is_activated(pool)
            .await
            .map_err(LegacyRunHistoryImportFailure::ActivationState)?;
        let tombstones = legacy_run_history_tombstones(pool)
            .await
            .map_err(LegacyRunHistoryImportFailure::DeletionState)?;
        if tombstoned_destination_present(pool)
            .await
            .map_err(LegacyRunHistoryImportFailure::DeletionState)?
        {
            return Err(LegacyRunHistoryImportFailure::TombstonedDestinationPresent);
        }
        if !activated {
            discard_projection_only_rows(pool, report).await?;
        }
        let mut source = LegacyRunHistorySource::open(self)
            .await
            .map_err(LegacyRunHistoryImportFailure::Source)?;

        loop {
            let history = match source.next_run(Some(controls)).await {
                Ok(history) => history,
                Err(error) => {
                    report.scanned_source_events = source.observed_events;
                    return Err(LegacyRunHistoryImportFailure::Source(error));
                }
            };
            report.scanned_source_events = source.observed_events;
            let Some(history) = history else {
                break;
            };
            import_checked_add(&mut report.scanned_source_runs, 1)?;
            if tombstones.contains(&history.run_id.to_string()) {
                import_checked_add(&mut report.tombstoned_source_runs, 1)?;
                import_checked_add(
                    &mut report.tombstoned_source_events,
                    usize_to_import_count(history.events.len())?,
                )?;
                continue;
            }
            import_one_run(pool, controls, history, activated, report).await?;
        }

        report.diagnostics = self
            .legacy_run_history_diagnostics()
            .await
            .map_err(LegacyRunHistoryImportFailure::Diagnostics)?;
        Ok(())
    }

    /// Verifies every legacy history as an exact SQLite prefix and then
    /// independently replays and verifies every SQLite run.
    ///
    /// The caller must prevent writes to both stores for the duration of
    /// verification. This operation does not establish a cross-store snapshot.
    pub async fn verify_legacy_run_history_in(
        &self,
        pool: &SqlitePool,
    ) -> Result<LegacyRunHistoryVerificationReport, LegacyRunHistoryVerificationError> {
        let mut report = LegacyRunHistoryVerificationReport::default();
        let result = self
            .run_legacy_run_history_verification(pool, &mut report)
            .await;
        debug_verification_outcome(result.as_ref().map_or("failed", |()| "complete"), &report);
        match result {
            Ok(()) => Ok(report),
            Err(failure) => Err(LegacyRunHistoryVerificationError { report, failure }),
        }
    }

    async fn run_legacy_run_history_verification(
        &self,
        pool: &SqlitePool,
        report: &mut LegacyRunHistoryVerificationReport,
    ) -> Result<(), LegacyRunHistoryVerificationFailure> {
        let tombstones = legacy_run_history_tombstones(pool)
            .await
            .map_err(LegacyRunHistoryVerificationFailure::DeletionState)?;
        if tombstoned_destination_present(pool)
            .await
            .map_err(LegacyRunHistoryVerificationFailure::DeletionState)?
        {
            return Err(LegacyRunHistoryVerificationFailure::TombstonedDestinationPresent);
        }
        let mut source_ids = HashSet::new();
        let mut source = LegacyRunHistorySource::open(self)
            .await
            .map_err(LegacyRunHistoryVerificationFailure::Source)?;
        loop {
            let history = match source.next_run(None).await {
                Ok(history) => history,
                Err(error) => {
                    report.source_events = source.observed_events;
                    return Err(LegacyRunHistoryVerificationFailure::Source(error));
                }
            };
            report.source_events = source.observed_events;
            let Some(history) = history else {
                break;
            };
            verification_checked_add(&mut report.source_runs, 1)?;
            source_ids.insert(history.run_id);
            if tombstones.contains(&history.run_id.to_string()) {
                verification_checked_add(&mut report.tombstoned_source_runs, 1)?;
                verification_checked_add(
                    &mut report.tombstoned_source_events,
                    usize_to_verification_count(history.events.len())?,
                )?;
                continue;
            }
            verify_source_prefix(pool, &history).await?;
            verification_checked_add(&mut report.matched_prefix_runs, 1)?;
            verification_checked_add(
                &mut report.matched_prefix_events,
                usize_to_verification_count(history.events.len())?,
            )?;
        }

        let stored_ids: Vec<String> = sqlx::query_scalar("SELECT id FROM runs ORDER BY id ASC")
            .fetch_all(pool)
            .await
            .map_err(LegacyRunHistoryVerificationFailure::ListDestinationRuns)?;
        for stored_id in stored_ids {
            let run_id = stored_id
                .parse::<RunId>()
                .map_err(|_| LegacyRunHistoryVerificationFailure::InvalidDestinationRunId)?;
            let mut connection = pool
                .acquire()
                .await
                .map_err(LegacyRunHistoryVerificationFailure::AcquireConnection)?;
            let events =
                RunSummaryStore::list_events_with_json_on_connection(&mut connection, &run_id)
                    .await
                    .map_err(LegacyRunHistoryVerificationFailure::ReadDestination)?;
            if events.is_empty() {
                return Err(LegacyRunHistoryVerificationFailure::EmptyDestinationHistory);
            }
            let current = replay_destination(&run_id, &events)
                .map_err(LegacyRunHistoryVerificationFailure::ReplayDestination)?;
            RunSummaryStore::verify_current_run_on_connection(&mut connection, &current)
                .await
                .map_err(LegacyRunHistoryVerificationFailure::VerifyDestination)?;

            let event_count = usize_to_verification_count(events.len())?;
            verification_checked_add(&mut report.target_runs, 1)?;
            verification_checked_add(&mut report.target_events, event_count)?;
            if !source_ids.contains(&run_id) {
                verification_checked_add(&mut report.sql_only_runs, 1)?;
                verification_checked_add(&mut report.sql_only_events, event_count)?;
            }
        }

        report.diagnostics = self
            .legacy_run_history_diagnostics()
            .await
            .map_err(LegacyRunHistoryVerificationFailure::Diagnostics)?;
        Ok(())
    }

    async fn legacy_run_history_diagnostics(
        &self,
    ) -> Result<LegacyRunHistoryDiagnostics, LegacyRunHistoryDiagnosticsFailure> {
        let source = self
            .open_db()
            .await
            .map_err(LegacyRunHistoryDiagnosticsFailure::OpenSource)?;
        let mut catalog_ids = Vec::new();
        let mut catalog = source
            .scan_prefix(keys::run_catalog_prefix())
            .await
            .map_err(LegacyRunHistoryDiagnosticsFailure::ReadCatalog)?;
        while let Some(entry) = catalog
            .next()
            .await
            .map_err(LegacyRunHistoryDiagnosticsFailure::ReadCatalog)?
        {
            let key = std::str::from_utf8(&entry.key)
                .map_err(LegacyRunHistoryDiagnosticsFailure::CatalogKeyUtf8)?;
            catalog_ids.push(
                keys::parse_run_catalog_key(key)
                    .ok_or(LegacyRunHistoryDiagnosticsFailure::InvalidCatalogKey)?,
            );
        }
        let mut diagnostics = LegacyRunHistoryDiagnostics {
            catalog_markers: u64::try_from(catalog_ids.len())
                .map_err(|_| LegacyRunHistoryDiagnosticsFailure::CounterOverflow)?,
            ..LegacyRunHistoryDiagnostics::default()
        };
        for run_id in catalog_ids {
            let mut events = source
                .scan_prefix(keys::run_events_prefix(&run_id))
                .await
                .map_err(LegacyRunHistoryDiagnosticsFailure::OpenEventProbe)?;
            if events
                .next()
                .await
                .map_err(LegacyRunHistoryDiagnosticsFailure::ReadEventProbe)?
                .is_none()
            {
                diagnostics.empty_catalog_markers = diagnostics
                    .empty_catalog_markers
                    .checked_add(1)
                    .ok_or(LegacyRunHistoryDiagnosticsFailure::CounterOverflow)?;
            }
        }
        let mut sessions = source
            .scan_prefix(keys::sessions_by_id_prefix())
            .await
            .map_err(LegacyRunHistoryDiagnosticsFailure::OpenSessionScan)?;
        while sessions
            .next()
            .await
            .map_err(LegacyRunHistoryDiagnosticsFailure::ReadSessionScan)?
            .is_some()
        {
            diagnostics.session_reverse_rows = diagnostics
                .session_reverse_rows
                .checked_add(1)
                .ok_or(LegacyRunHistoryDiagnosticsFailure::CounterOverflow)?;
        }
        Ok(diagnostics)
    }
}

fn parse_source_event(
    key: &[u8],
    value: &[u8],
) -> Result<Option<ValidatedLegacyRunEvent>, LegacyRunHistorySourceFailure> {
    let key_text = std::str::from_utf8(key).map_err(LegacyRunHistorySourceFailure::KeyUtf8)?;
    let segments = SlateKey::segments(key_text).collect::<Vec<_>>();
    if segments.get(2).copied() != Some("events") {
        return Ok(None);
    }
    let ["runs", run_id_text, "events", leaf] = segments.as_slice() else {
        return Err(LegacyRunHistorySourceFailure::InvalidKey);
    };
    let run_id = run_id_text
        .parse::<RunId>()
        .map_err(|_| LegacyRunHistorySourceFailure::InvalidKey)?;
    let Some((sequence_text, epoch_ms_text)) = leaf.split_once('-') else {
        return Err(LegacyRunHistorySourceFailure::InvalidKey);
    };
    if sequence_text.len() != 6 || !sequence_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LegacyRunHistorySourceFailure::InvalidKey);
    }
    let sequence = sequence_text
        .parse::<u32>()
        .ok()
        .filter(|sequence| (1..=keys::MAX_EVENT_SEQ).contains(sequence))
        .ok_or(LegacyRunHistorySourceFailure::InvalidKey)?;
    let epoch_ms = epoch_ms_text
        .parse::<i64>()
        .map_err(|_| LegacyRunHistorySourceFailure::InvalidKey)?;
    if keys::run_event_key(&run_id, sequence, epoch_ms).as_ref() != key {
        return Err(LegacyRunHistorySourceFailure::InvalidKey);
    }

    let event_json = std::str::from_utf8(value)
        .map_err(LegacyRunHistorySourceFailure::ValueUtf8)?
        .to_owned();
    let payload: EventPayload =
        serde_json::from_str(&event_json).map_err(LegacyRunHistorySourceFailure::DecodeEvent)?;
    payload
        .validate(&run_id)
        .map_err(LegacyRunHistorySourceFailure::ValidateEvent)?;
    let event =
        RunEvent::try_from(&payload).map_err(LegacyRunHistorySourceFailure::ValidateEvent)?;
    if event.run_id != run_id {
        return Err(LegacyRunHistorySourceFailure::ValidateEvent(
            crate::Error::RunEventMismatch {
                run_id: run_id.to_string(),
                seq:    sequence,
                field:  "run_id",
            },
        ));
    }
    Ok(Some(ValidatedLegacyRunEvent {
        run_id,
        payload,
        envelope: EventEnvelope {
            seq: sequence,
            event,
        },
        event_json,
        raw_key: key.to_vec(),
    }))
}

fn hash_source_part(
    hasher: &mut Sha256,
    bytes: &[u8],
) -> Result<(), LegacyRunHistorySourceFailure> {
    let length =
        u64::try_from(bytes.len()).map_err(|_| LegacyRunHistorySourceFailure::CounterOverflow)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn event_run_segment(key: &[u8]) -> Option<&[u8]> {
    let mut segments = key.split(|byte| *byte == 0);
    (segments.next()? == b"runs").then_some(())?;
    let run_id = segments.next()?;
    (segments.next()? == b"events").then_some(run_id)
}

async fn legacy_run_history_is_activated(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM legacy_run_history_activation WHERE singleton = 1)",
    )
    .fetch_one(pool)
    .await
}

async fn legacy_run_history_tombstones(pool: &SqlitePool) -> Result<HashSet<String>, sqlx::Error> {
    Ok(
        sqlx::query_scalar("SELECT run_id FROM legacy_run_history_deletions")
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect(),
    )
}

/// Returns whether any tombstoned run still has canonical SQLite rows.
async fn tombstoned_destination_present(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r"
SELECT EXISTS(
    SELECT 1 FROM legacy_run_history_deletions AS deletion
    WHERE EXISTS(SELECT 1 FROM runs WHERE id = deletion.run_id)
       OR EXISTS(SELECT 1 FROM run_events WHERE run_id = deletion.run_id)
)
",
    )
    .fetch_one(pool)
    .await
}

async fn discard_projection_only_rows(
    pool: &SqlitePool,
    report: &mut LegacyRunHistoryImportReport,
) -> Result<(), LegacyRunHistoryImportFailure> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(LegacyRunHistoryImportFailure::BeginCleanup)?;
    let result = sqlx::query(
        r"
DELETE FROM runs
WHERE NOT EXISTS (
    SELECT 1 FROM run_events WHERE run_events.run_id = runs.id
)
",
    )
    .execute(&mut *transaction)
    .await
    .map_err(LegacyRunHistoryImportFailure::DeleteProjectionOnlyRows)?;
    let discarded = result.rows_affected();
    transaction
        .commit()
        .await
        .map_err(LegacyRunHistoryImportFailure::CommitCleanup)?;
    report.discarded_projection_only_rows = discarded;
    Ok(())
}

async fn import_one_run(
    pool: &SqlitePool,
    controls: &ImportControls,
    history: ValidatedLegacyRunHistory,
    activated: bool,
    report: &mut LegacyRunHistoryImportReport,
) -> Result<(), LegacyRunHistoryImportFailure> {
    #[cfg(not(test))]
    let _ = controls;
    let mut transaction = pool
        .begin()
        .await
        .map_err(LegacyRunHistoryImportFailure::BeginRunTransaction)?;
    let result = async {
        let has_destination: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM run_events WHERE run_id = ?)")
                .bind(history.run_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(|source| {
                    LegacyRunHistoryImportFailure::ReadDestination(crate::Error::Sqlite(source))
                })?;
        let mut updated = *report;
        if has_destination {
            let destination = RunSummaryStore::list_events_with_json_in_transaction(
                &mut transaction,
                &history.run_id,
            )
            .await
            .map_err(LegacyRunHistoryImportFailure::ReadDestination)?;
            require_exact_prefix(&history, &destination)
                .map_err(|()| LegacyRunHistoryImportFailure::DestinationConflict)?;
            let current = replay_destination(&history.run_id, &destination)
                .map_err(LegacyRunHistoryImportFailure::ReplayDestination)?;
            RunSummaryStore::verify_current_run_on_connection(&mut transaction, &current)
                .await
                .map_err(LegacyRunHistoryImportFailure::VerifyDestination)?;
            import_checked_add(&mut updated.verified_existing_runs, 1)?;
            import_checked_add(
                &mut updated.verified_existing_events,
                usize_to_import_count(destination.len())?,
            )?;
        } else if activated {
            return Err(LegacyRunHistoryImportFailure::MissingDestinationAfterActivation);
        } else {
            RunSummaryStore::insert_imported_run_on_connection(&mut transaction, &history.current)
                .await
                .map_err(LegacyRunHistoryImportFailure::InsertRun)?;
            #[cfg(test)]
            controls.after_run_inserted().await;
            for event in &history.events {
                RunSummaryStore::insert_imported_event_on_connection(
                    &mut transaction,
                    &history.run_id,
                    &event.payload,
                    &event.envelope,
                    &event.event_json,
                )
                .await
                .map_err(LegacyRunHistoryImportFailure::InsertEvent)?;
            }
            import_checked_add(&mut updated.imported_runs, 1)?;
            import_checked_add(
                &mut updated.imported_events,
                usize_to_import_count(history.events.len())?,
            )?;
        }
        import_checked_add(&mut updated.committed_run_transactions, 1)?;
        Ok(updated)
    }
    .await;

    match result {
        Ok(updated) => {
            transaction
                .commit()
                .await
                .map_err(LegacyRunHistoryImportFailure::CommitRunTransaction)?;
            *report = updated;
            Ok(())
        }
        Err(prior) => match transaction.rollback().await {
            Ok(()) => Err(prior),
            Err(source) => Err(LegacyRunHistoryImportFailure::RollbackRunTransaction {
                source,
                prior: Box::new(prior),
            }),
        },
    }
}

async fn verify_source_prefix(
    pool: &SqlitePool,
    history: &ValidatedLegacyRunHistory,
) -> Result<(), LegacyRunHistoryVerificationFailure> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(LegacyRunHistoryVerificationFailure::AcquireConnection)?;
    let has_destination: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM run_events WHERE run_id = ?)")
            .bind(history.run_id.to_string())
            .fetch_one(&mut *connection)
            .await
            .map_err(|source| {
                LegacyRunHistoryVerificationFailure::ReadDestination(crate::Error::Sqlite(source))
            })?;
    if !has_destination {
        return Err(LegacyRunHistoryVerificationFailure::MissingDestinationPrefix);
    }
    let destination =
        RunSummaryStore::list_events_with_json_on_connection(&mut connection, &history.run_id)
            .await
            .map_err(LegacyRunHistoryVerificationFailure::ReadDestination)?;
    if destination.len() < history.events.len() {
        return Err(LegacyRunHistoryVerificationFailure::MissingDestinationPrefix);
    }
    require_exact_prefix(history, &destination)
        .map_err(|()| LegacyRunHistoryVerificationFailure::DestinationPrefixConflict)
}

fn require_exact_prefix(
    history: &ValidatedLegacyRunHistory,
    destination: &[(EventEnvelope, String)],
) -> Result<(), ()> {
    if destination.len() < history.events.len() {
        return Err(());
    }
    if history
        .events
        .iter()
        .zip(destination)
        .any(|(source, (target, target_json))| {
            source.envelope.seq != target.seq || source.event_json != *target_json
        })
    {
        return Err(());
    }
    Ok(())
}

fn replay_destination(
    run_id: &RunId,
    events: &[(EventEnvelope, String)],
) -> crate::Result<ProjectedRun> {
    if let Some((first, _event_json)) = events.first() {
        if first.seq != 1 {
            return Err(crate::Error::RunEventMismatch {
                run_id: run_id.to_string(),
                seq:    first.seq,
                field:  "seq",
            });
        }
    }
    let envelopes = events
        .iter()
        .map(|(envelope, _event_json)| envelope.clone())
        .collect::<Vec<_>>();
    ProjectedRun::replay(*run_id, &envelopes)
}

fn usize_to_import_count(value: usize) -> Result<u64, LegacyRunHistoryImportFailure> {
    u64::try_from(value).map_err(|_| LegacyRunHistoryImportFailure::CounterOverflow)
}

fn import_checked_add(value: &mut u64, amount: u64) -> Result<(), LegacyRunHistoryImportFailure> {
    *value = value
        .checked_add(amount)
        .ok_or(LegacyRunHistoryImportFailure::CounterOverflow)?;
    Ok(())
}

fn usize_to_verification_count(value: usize) -> Result<u64, LegacyRunHistoryVerificationFailure> {
    u64::try_from(value).map_err(|_| LegacyRunHistoryVerificationFailure::CounterOverflow)
}

fn verification_checked_add(
    value: &mut u64,
    amount: u64,
) -> Result<(), LegacyRunHistoryVerificationFailure> {
    *value = value
        .checked_add(amount)
        .ok_or(LegacyRunHistoryVerificationFailure::CounterOverflow)?;
    Ok(())
}

fn debug_import_outcome(outcome: &'static str, report: &LegacyRunHistoryImportReport) {
    debug!(
        outcome,
        scanned_source_runs = report.scanned_source_runs,
        scanned_source_events = report.scanned_source_events,
        imported_runs = report.imported_runs,
        imported_events = report.imported_events,
        verified_existing_runs = report.verified_existing_runs,
        verified_existing_events = report.verified_existing_events,
        discarded_projection_only_rows = report.discarded_projection_only_rows,
        committed_run_transactions = report.committed_run_transactions,
        tombstoned_source_runs = report.tombstoned_source_runs,
        tombstoned_source_events = report.tombstoned_source_events,
        catalog_markers = report.diagnostics.catalog_markers,
        empty_catalog_markers = report.diagnostics.empty_catalog_markers,
        session_reverse_rows = report.diagnostics.session_reverse_rows,
        "Legacy run-history import finished"
    );
}

fn debug_verification_outcome(outcome: &'static str, report: &LegacyRunHistoryVerificationReport) {
    debug!(
        outcome,
        source_runs = report.source_runs,
        source_events = report.source_events,
        matched_prefix_runs = report.matched_prefix_runs,
        matched_prefix_events = report.matched_prefix_events,
        target_runs = report.target_runs,
        target_events = report.target_events,
        sql_only_runs = report.sql_only_runs,
        sql_only_events = report.sql_only_events,
        tombstoned_source_runs = report.tombstoned_source_runs,
        tombstoned_source_events = report.tombstoned_source_events,
        catalog_markers = report.diagnostics.catalog_markers,
        empty_catalog_markers = report.diagnostics.empty_catalog_markers,
        session_reverse_rows = report.diagnostics.session_reverse_rows,
        "Legacy run-history verification finished"
    );
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{TimeZone as _, Utc};
    use fabro_types::{Graph, RunEvent, RunId, SessionId, WorkflowSettings, test_support};
    use fabro_util::error;
    use object_store::memory::InMemory;
    use tokio::sync::Barrier;
    use ulid::Ulid;

    use super::{
        ImportControls, LegacyRunHistoryDiagnostics, LegacyRunHistoryImportError,
        LegacyRunHistoryImportFailure, LegacyRunHistoryImportReport,
        LegacyRunHistoryVerificationFailure, LegacyRunHistoryVerificationReport,
        parse_source_event,
    };
    use crate::keys::SlateKey;
    use crate::run_state::ProjectedRun;
    use crate::{
        Database, EventEnvelope, EventPayload, RunSummaryStore, keys,
        test_support as store_test_support,
    };

    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    struct TestContext {
        _directory: tempfile::TempDir,
        source:     Database,
        source_db:  slatedb::Db,
        sqlite:     sqlx::SqlitePool,
    }

    impl TestContext {
        async fn new() -> TestResult<Self> {
            let directory = tempfile::tempdir()?;
            let sqlite =
                fabro_db::Database::connect(directory.path().join("fabro.sqlite3")).await?;
            sqlite.migrate().await?;
            let sqlite = sqlite.clone_pool();
            let source = Database::new(
                Arc::new(InMemory::new()),
                "legacy-run-history-import-tests",
                Duration::from_millis(1),
                None,
                store_test_support::test_blob_store(),
                store_test_support::test_run_summary_store(),
            );
            let source_db = source.open_db().await?;
            Ok(Self {
                _directory: directory,
                source,
                source_db,
                sqlite,
            })
        }

        async fn put_event(
            &self,
            run_id: &RunId,
            seq: u32,
            epoch_ms: i64,
            event_json: &str,
        ) -> TestResult<()> {
            self.source_db
                .put(
                    keys::run_event_key(run_id, seq, epoch_ms),
                    event_json.as_bytes(),
                )
                .await?;
            Ok(())
        }

        async fn put_raw(&self, key: impl AsRef<[u8]>, value: &[u8]) -> TestResult<()> {
            self.source_db.put(key, value).await?;
            Ok(())
        }

        async fn source_entries(&self) -> TestResult<Vec<(Vec<u8>, Vec<u8>)>> {
            let mut entries = self.source_db.scan_prefix(Vec::<u8>::new()).await?;
            let mut snapshot = Vec::new();
            while let Some(entry) = entries.next().await? {
                snapshot.push((entry.key.to_vec(), entry.value.to_vec()));
            }
            Ok(snapshot)
        }

        async fn import(&self) -> TestResult<LegacyRunHistoryImportReport> {
            Ok(self
                .source
                .import_legacy_run_history_into(&self.sqlite)
                .await?)
        }
    }

    fn run_id(index: u128) -> RunId {
        RunId::from(Ulid::from_parts(
            1_788_000_000_000 + u64::try_from(index).unwrap(),
            index,
        ))
    }

    fn event_value(
        run_id: &RunId,
        seq: u32,
        event: &str,
        properties: &serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "id": format!("evt-{seq}-{event}"),
            "ts": Utc
                .timestamp_millis_opt(1_788_000_000_000 + i64::from(seq))
                .single()
                .unwrap()
                .to_rfc3339(),
            "run_id": run_id.to_string(),
            "event": event,
            "properties": properties,
        })
    }

    fn created_value(run_id: &RunId, title: &str) -> serde_json::Value {
        event_value(
            run_id,
            1,
            "run.created",
            &serde_json::json!({
                "title": title,
                "settings": WorkflowSettings::default(),
                "graph": Graph::new("test"),
                "workflow_slug": "test-workflow",
                "labels": {},
                "provenance": test_support::test_run_provenance(),
            }),
        )
    }

    fn submitted_value(run_id: &RunId, seq: u32) -> serde_json::Value {
        event_value(run_id, seq, "run.submitted", &serde_json::json!({}))
    }

    fn session_created_value(
        run_id: &RunId,
        seq: u32,
        session_id: &SessionId,
    ) -> serde_json::Value {
        let mut value = event_value(
            run_id,
            seq,
            "run.session.created",
            &serde_json::json!({ "title": "Imported session" }),
        );
        value
            .as_object_mut()
            .unwrap()
            .insert("session_id".to_string(), session_id.to_string().into());
        value
    }

    #[tokio::test]
    async fn legacy_source_identity_covers_exact_keys_and_json_bytes() -> TestResult<()> {
        let context = TestContext::new().await?;
        let run_id = run_id(0);
        let value = created_value(&run_id, "identity");
        let compact = serde_json::to_string(&value)?;
        context.put_event(&run_id, 1, 1, &compact).await?;
        let compact_identity = context.source.legacy_run_history_source_identity().await?;

        let pretty = serde_json::to_string_pretty(&value)?;
        context.put_event(&run_id, 1, 1, &pretty).await?;
        let pretty_identity = context.source.legacy_run_history_source_identity().await?;
        assert_eq!(
            (compact_identity.runs, compact_identity.events),
            (pretty_identity.runs, pretty_identity.events)
        );
        assert_ne!(
            compact_identity.fingerprint(),
            pretty_identity.fingerprint(),
            "semantically equivalent JSON bytes must still change the source identity"
        );

        context
            .source_db
            .delete(keys::run_event_key(&run_id, 1, 1))
            .await?;
        context.put_event(&run_id, 1, 2, &pretty).await?;
        let moved_key_identity = context.source.legacy_run_history_source_identity().await?;
        assert_eq!(
            (pretty_identity.runs, pretty_identity.events),
            (moved_key_identity.runs, moved_key_identity.events)
        );
        assert_ne!(
            pretty_identity.fingerprint(),
            moved_key_identity.fingerprint(),
            "changing only the raw legacy key must change the source identity"
        );
        Ok(())
    }

    fn decode_event(
        run_id: &RunId,
        seq: u32,
        event_json: &str,
    ) -> TestResult<(EventPayload, EventEnvelope)> {
        let payload: EventPayload = serde_json::from_str(event_json)?;
        payload.validate(run_id)?;
        let event = RunEvent::try_from(&payload)?;
        Ok((payload, EventEnvelope { seq, event }))
    }

    async fn seed_destination_history(
        pool: &sqlx::SqlitePool,
        run_id: &RunId,
        events: &[(u32, String)],
    ) -> TestResult<()> {
        let decoded = events
            .iter()
            .map(|(seq, event_json)| decode_event(run_id, *seq, event_json))
            .collect::<TestResult<Vec<_>>>()?;
        let envelopes = decoded
            .iter()
            .map(|(_payload, envelope)| envelope.clone())
            .collect::<Vec<_>>();
        let current = ProjectedRun::replay(*run_id, &envelopes)?;
        let mut transaction = pool.begin().await?;
        RunSummaryStore::insert_imported_run_on_connection(&mut transaction, &current).await?;
        for ((_, event_json), (payload, envelope)) in events.iter().zip(&decoded) {
            RunSummaryStore::insert_imported_event_on_connection(
                &mut transaction,
                run_id,
                payload,
                envelope,
                event_json,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn append_destination_event(
        pool: &sqlx::SqlitePool,
        run_id: &RunId,
        prior: &[(u32, String)],
        next: (u32, String),
    ) -> TestResult<()> {
        let mut all = prior.to_vec();
        all.push(next.clone());
        let decoded = all
            .iter()
            .map(|(seq, event_json)| decode_event(run_id, *seq, event_json))
            .collect::<TestResult<Vec<_>>>()?;
        let envelopes = decoded
            .iter()
            .map(|(_payload, envelope)| envelope.clone())
            .collect::<Vec<_>>();
        let current = ProjectedRun::replay(*run_id, &envelopes)?;
        let mut transaction = pool.begin().await?;
        RunSummaryStore::append_event_on_connection(
            &mut transaction,
            prior.last().unwrap().0,
            &current,
            &decoded.last().unwrap().0,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    #[test]
    fn legacy_run_history_import_exposes_rollback_cleanup_errors() {
        let error = LegacyRunHistoryImportError {
            report:  LegacyRunHistoryImportReport::default(),
            failure: LegacyRunHistoryImportFailure::RollbackRunTransaction {
                source: sqlx::Error::Protocol("injected rollback failure".to_owned()),
                prior:  Box::new(LegacyRunHistoryImportFailure::DestinationConflict),
            },
        };

        assert_eq!(
            error.source().map(ToString::to_string),
            Some(
                "the destination history is partial or conflicts with the legacy prefix".to_owned()
            )
        );
        assert!(
            error
                .cleanup_errors()
                .any(|source| source.downcast_ref::<sqlx::Error>().is_some()),
            "rollback source was absent from cleanup errors"
        );
    }

    #[tokio::test]
    async fn session_owner_imports_typed_event_and_counts_opaque_legacy_reverse_rows()
    -> TestResult<()> {
        let context = TestContext::new().await?;
        let first = run_id(1);
        let second = run_id(2);
        let empty_marker = run_id(3);
        let stale = run_id(4);
        let session_id = SessionId::new();
        let first_created = serde_json::to_string_pretty(&created_value(&first, "first"))?;
        let first_session = serde_json::to_string(&session_created_value(&first, 3, &session_id))?;
        let first_submitted = serde_json::to_string(&submitted_value(&first, 4))?;
        let second_created = serde_json::to_string(&created_value(&second, "second"))?;
        context.put_event(&first, 1, 10, &first_created).await?;
        context.put_event(&first, 3, 30, &first_session).await?;
        context.put_event(&first, 4, 40, &first_submitted).await?;
        context.put_event(&second, 1, 20, &second_created).await?;
        context.put_raw(keys::run_catalog_key(&first), b"").await?;
        context
            .put_raw(keys::run_catalog_key(&empty_marker), b"")
            .await?;
        context
            .put_raw(
                keys::session_by_id_key(&session_id),
                b"opaque legacy reverse row",
            )
            .await?;

        let stale_created = serde_json::to_string(&created_value(&stale, "stale"))?;
        seed_destination_history(&context.sqlite, &stale, &[(1, stale_created)]).await?;
        sqlx::query("DELETE FROM run_events WHERE run_id = ?")
            .bind(stale.to_string())
            .execute(&context.sqlite)
            .await?;
        sqlx::query("UPDATE runs SET summary_json = '{}' WHERE id = ?")
            .bind(stale.to_string())
            .execute(&context.sqlite)
            .await?;
        let source_before = context.source_entries().await?;

        let report = context.import().await?;

        assert_eq!(report, LegacyRunHistoryImportReport {
            scanned_source_runs: 2,
            scanned_source_events: 4,
            imported_runs: 2,
            imported_events: 4,
            discarded_projection_only_rows: 1,
            committed_run_transactions: 2,
            diagnostics: LegacyRunHistoryDiagnostics {
                catalog_markers:       2,
                empty_catalog_markers: 1,
                session_reverse_rows:  1,
            },
            ..LegacyRunHistoryImportReport::default()
        });
        let stored: Vec<(i64, String)> =
            sqlx::query_as("SELECT seq, event_json FROM run_events WHERE run_id = ? ORDER BY seq")
                .bind(first.to_string())
                .fetch_all(&context.sqlite)
                .await?;
        assert_eq!(stored, vec![
            (1, first_created),
            (3, first_session),
            (4, first_submitted)
        ]);
        let summaries = RunSummaryStore::new(context.sqlite.clone());
        assert_eq!(
            summaries.find_session_owner(&session_id).await?,
            Some(first)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT source_last_seq FROM runs WHERE id = ?")
                .bind(first.to_string())
                .fetch_one(&context.sqlite)
                .await?,
            4
        );
        assert_eq!(context.source_entries().await?, source_before);
        let verification = context
            .source
            .verify_legacy_run_history_in(&context.sqlite)
            .await?;
        assert_eq!(verification.target_runs, 2);
        assert_eq!(verification.target_events, 4);
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_retry_uses_complete_destination_histories_as_progress()
    -> TestResult<()> {
        let context = TestContext::new().await?;
        let run_id = run_id(10);
        context
            .put_event(
                &run_id,
                1,
                10,
                &serde_json::to_string(&created_value(&run_id, "retry"))?,
            )
            .await?;
        let first = context.import().await?;
        let second = context.import().await?;

        assert_eq!(first.imported_runs, 1);
        assert_eq!(second, LegacyRunHistoryImportReport {
            scanned_source_runs: 1,
            scanned_source_events: 1,
            verified_existing_runs: 1,
            verified_existing_events: 1,
            committed_run_transactions: 1,
            ..LegacyRunHistoryImportReport::default()
        });
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_retry_and_verification_compare_summary_json_semantically()
    -> TestResult<()> {
        let context = TestContext::new().await?;
        let run_id = run_id(11);
        let mut created = created_value(&run_id, "labeled");
        created["properties"]["labels"] = serde_json::json!({
            "alpha": "one",
            "beta": "two",
            "gamma": "three",
        });
        context
            .put_event(&run_id, 1, 10, &serde_json::to_string(&created)?)
            .await?;
        context.import().await?;

        let compact: String = sqlx::query_scalar("SELECT summary_json FROM runs WHERE id = ?")
            .bind(run_id.to_string())
            .fetch_one(&context.sqlite)
            .await?;
        let reformatted =
            serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&compact)?)?;
        assert_ne!(compact, reformatted);
        sqlx::query("UPDATE runs SET summary_json = ? WHERE id = ?")
            .bind(reformatted)
            .bind(run_id.to_string())
            .execute(&context.sqlite)
            .await?;

        let retry = context.import().await?;
        assert_eq!(retry.verified_existing_runs, 1);
        assert_eq!(retry.verified_existing_events, 1);
        let verification = context
            .source
            .verify_legacy_run_history_in(&context.sqlite)
            .await?;
        assert_eq!(verification.target_runs, 1);
        assert_eq!(verification.target_events, 1);
        Ok(())
    }

    #[test]
    fn legacy_run_history_rejects_noncanonical_event_key_shapes() -> TestResult<()> {
        let run_id = run_id(20);
        let event_json = serde_json::to_string(&created_value(&run_id, "key"))?;
        let invalid = [
            format!("runs\0{run_id}\0events\0000001-1\0extra").into_bytes(),
            b"runs\0not-a-run-id\0events\x00000001-1".to_vec(),
            format!("runs\0{run_id}\0events\000001-1").into_bytes(),
            format!("runs\0{run_id}\0events\0000000-1").into_bytes(),
            format!("runs\0{run_id}\0events\01000000-1").into_bytes(),
            format!("runs\0{run_id}\0events\0000001-nope").into_bytes(),
            format!("runs\0{run_id}\0events\0000001-01").into_bytes(),
        ];
        for key in invalid {
            assert!(parse_source_event(&key, event_json.as_bytes()).is_err());
        }
        let mut invalid_utf8 = b"runs\0".to_vec();
        invalid_utf8.extend_from_slice(&[0xff, 0xfe]);
        invalid_utf8.extend_from_slice(b"\0events\x00000001-1");
        assert!(parse_source_event(&invalid_utf8, event_json.as_bytes()).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_rejects_invalid_values_sequences_and_replay() -> TestResult<()> {
        let invalid_utf8 = TestContext::new().await?;
        let first = run_id(30);
        invalid_utf8
            .put_raw(keys::run_event_key(&first, 1, 1), &[0xff])
            .await?;
        assert!(
            invalid_utf8
                .source
                .import_legacy_run_history_into(&invalid_utf8.sqlite)
                .await
                .is_err()
        );

        let invalid_json = TestContext::new().await?;
        invalid_json.put_event(&first, 1, 1, "{not-json").await?;
        assert!(
            invalid_json
                .source
                .import_legacy_run_history_into(&invalid_json.sqlite)
                .await
                .is_err()
        );

        let missing_first = TestContext::new().await?;
        missing_first
            .put_event(
                &first,
                2,
                2,
                &serde_json::to_string(&submitted_value(&first, 2))?,
            )
            .await?;
        assert!(
            missing_first
                .source
                .import_legacy_run_history_into(&missing_first.sqlite)
                .await
                .is_err()
        );

        let wrong_first_event = TestContext::new().await?;
        wrong_first_event
            .put_event(
                &first,
                1,
                1,
                &serde_json::to_string(&submitted_value(&first, 1))?,
            )
            .await?;
        assert!(
            wrong_first_event
                .source
                .import_legacy_run_history_into(&wrong_first_event.sqlite)
                .await
                .is_err()
        );

        let mismatched_payload = TestContext::new().await?;
        let other_run = run_id(31);
        mismatched_payload
            .put_event(
                &first,
                1,
                1,
                &serde_json::to_string(&created_value(&other_run, "mismatch"))?,
            )
            .await?;
        assert!(
            mismatched_payload
                .source
                .import_legacy_run_history_into(&mismatched_payload.sqlite)
                .await
                .is_err()
        );

        let duplicate = TestContext::new().await?;
        let created = serde_json::to_string(&created_value(&first, "duplicate"))?;
        duplicate.put_event(&first, 1, 1, &created).await?;
        duplicate.put_event(&first, 1, 2, &created).await?;
        assert!(
            duplicate
                .source
                .import_legacy_run_history_into(&duplicate.sqlite)
                .await
                .is_err()
        );

        let unreplayable = TestContext::new().await?;
        unreplayable.put_event(&first, 1, 1, &created).await?;
        unreplayable.put_event(&first, 2, 2, &created).await?;
        assert!(
            unreplayable
                .source
                .import_legacy_run_history_into(&unreplayable.sqlite)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_rolls_back_a_run_when_event_insertion_fails() -> TestResult<()> {
        let context = TestContext::new().await?;
        let run_id = run_id(40);
        context
            .put_event(
                &run_id,
                1,
                1,
                &serde_json::to_string(&created_value(&run_id, "rollback"))?,
            )
            .await?;
        sqlx::query(
            "CREATE TRIGGER reject_imported_event BEFORE INSERT ON run_events BEGIN SELECT RAISE(FAIL, 'injected'); END",
        )
        .execute(&context.sqlite)
        .await?;

        let error = context
            .source
            .import_legacy_run_history_into(&context.sqlite)
            .await
            .unwrap_err();

        assert_eq!(error.report().imported_runs, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
                .fetch_one(&context.sqlite)
                .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM run_events")
                .fetch_one(&context.sqlite)
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_interruption_preserves_complete_runs_and_retry_converges()
    -> TestResult<()> {
        let context = TestContext::new().await?;
        let first = run_id(50);
        let second = run_id(51);
        for run_id in [first, second] {
            context
                .put_event(
                    &run_id,
                    1,
                    1,
                    &serde_json::to_string(&created_value(&run_id, "interrupted"))?,
                )
                .await?;
        }
        let controls = ImportControls {
            source_after_events: Some(2),
            ..ImportControls::default()
        };

        let error = context
            .source
            .import_legacy_run_history_with_controls(&context.sqlite, &controls)
            .await
            .unwrap_err();
        assert_eq!(error.report().imported_runs, 1);
        assert_eq!(error.report().committed_run_transactions, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
                .fetch_one(&context.sqlite)
                .await?,
            1
        );

        let retry = context.import().await?;
        assert_eq!(retry.imported_runs, 1);
        assert_eq!(retry.verified_existing_runs, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
                .fetch_one(&context.sqlite)
                .await?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_cancellation_leaves_no_half_imported_run() -> TestResult<()> {
        let context = TestContext::new().await?;
        let run_id = run_id(60);
        context
            .put_event(
                &run_id,
                1,
                1,
                &serde_json::to_string(&created_value(&run_id, "cancel"))?,
            )
            .await?;
        let barrier = Arc::new(Barrier::new(2));
        let source = context.source.clone();
        let sqlite = context.sqlite.clone();
        let task_barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            let controls = ImportControls {
                after_run_inserted: Some(task_barrier),
                ..ImportControls::default()
            };
            source
                .import_legacy_run_history_with_controls(&sqlite, &controls)
                .await
        });
        barrier.wait().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs")
                .fetch_one(&context.sqlite)
                .await?,
            0
        );
        assert_eq!(context.import().await?.imported_runs, 1);
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_rejects_partial_conflicting_and_corrupt_destinations()
    -> TestResult<()> {
        let run_id = run_id(70);
        let source_created = serde_json::to_string(&created_value(&run_id, "source"))?;
        let source_submitted = serde_json::to_string(&submitted_value(&run_id, 2))?;

        let partial = TestContext::new().await?;
        partial.put_event(&run_id, 1, 1, &source_created).await?;
        partial.put_event(&run_id, 2, 2, &source_submitted).await?;
        seed_destination_history(&partial.sqlite, &run_id, &[(1, source_created.clone())]).await?;
        assert!(
            partial
                .source
                .import_legacy_run_history_into(&partial.sqlite)
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM run_events")
                .fetch_one(&partial.sqlite)
                .await?,
            1
        );

        let conflicting = TestContext::new().await?;
        conflicting
            .put_event(&run_id, 1, 1, &source_created)
            .await?;
        let target_created = serde_json::to_string(&created_value(&run_id, "target"))?;
        seed_destination_history(&conflicting.sqlite, &run_id, &[(1, target_created)]).await?;
        assert!(
            conflicting
                .source
                .import_legacy_run_history_into(&conflicting.sqlite)
                .await
                .is_err()
        );

        let corrupt_event = TestContext::new().await?;
        corrupt_event
            .put_event(&run_id, 1, 1, &source_created)
            .await?;
        seed_destination_history(&corrupt_event.sqlite, &run_id, &[(
            1,
            source_created.clone(),
        )])
        .await?;
        sqlx::query("UPDATE run_events SET event_name = 'run.failed' WHERE run_id = ?")
            .bind(run_id.to_string())
            .execute(&corrupt_event.sqlite)
            .await?;
        assert!(
            corrupt_event
                .source
                .import_legacy_run_history_into(&corrupt_event.sqlite)
                .await
                .is_err()
        );

        let corrupt_summary = TestContext::new().await?;
        corrupt_summary
            .put_event(&run_id, 1, 1, &source_created)
            .await?;
        seed_destination_history(&corrupt_summary.sqlite, &run_id, &[(
            1,
            source_created.clone(),
        )])
        .await?;
        sqlx::query("UPDATE runs SET title = 'corrupt' WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&corrupt_summary.sqlite)
            .await?;
        assert!(
            corrupt_summary
                .source
                .import_legacy_run_history_into(&corrupt_summary.sqlite)
                .await
                .is_err()
        );

        let corrupt_head = TestContext::new().await?;
        corrupt_head
            .put_event(&run_id, 1, 1, &source_created)
            .await?;
        seed_destination_history(&corrupt_head.sqlite, &run_id, &[(1, source_created)]).await?;
        sqlx::query("UPDATE runs SET source_last_seq = 2 WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&corrupt_head.sqlite)
            .await?;
        assert!(
            corrupt_head
                .source
                .import_legacy_run_history_into(&corrupt_head.sqlite)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_verification_accepts_sql_suffixes_and_sql_only_runs()
    -> TestResult<()> {
        let context = TestContext::new().await?;
        let source_run = run_id(80);
        let sql_only_run = run_id(81);
        let source_created = serde_json::to_string(&created_value(&source_run, "source"))?;
        context
            .put_event(&source_run, 1, 1, &source_created)
            .await?;
        context.import().await?;
        let suffix = serde_json::to_string(&submitted_value(&source_run, 2))?;
        append_destination_event(
            &context.sqlite,
            &source_run,
            &[(1, source_created)],
            (2, suffix),
        )
        .await?;
        let sql_only_created = serde_json::to_string(&created_value(&sql_only_run, "sql-only"))?;
        seed_destination_history(&context.sqlite, &sql_only_run, &[(1, sql_only_created)]).await?;

        let report = context
            .source
            .verify_legacy_run_history_in(&context.sqlite)
            .await?;

        assert_eq!(report, LegacyRunHistoryVerificationReport {
            source_runs: 1,
            source_events: 1,
            matched_prefix_runs: 1,
            matched_prefix_events: 1,
            target_runs: 2,
            target_events: 3,
            sql_only_runs: 1,
            sql_only_events: 1,
            ..LegacyRunHistoryVerificationReport::default()
        });
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_verification_rejects_invalid_sql_only_histories() -> TestResult<()>
    {
        let missing_first = TestContext::new().await?;
        let missing_first_id = run_id(82);
        let created = serde_json::to_string(&created_value(&missing_first_id, "missing-first"))?;
        seed_destination_history(&missing_first.sqlite, &missing_first_id, &[(2, created)]).await?;
        let error = missing_first
            .source
            .verify_legacy_run_history_in(&missing_first.sqlite)
            .await
            .unwrap_err();
        assert!(matches!(
            error.failure,
            LegacyRunHistoryVerificationFailure::ReplayDestination(
                crate::Error::RunEventMismatch { field: "seq", .. }
            )
        ));

        let noncanonical = TestContext::new().await?;
        let noncanonical_id = run_id(83);
        let canonical_json =
            serde_json::to_string(&created_value(&noncanonical_id, "noncanonical"))?;
        seed_destination_history(&noncanonical.sqlite, &noncanonical_id, &[(
            1,
            canonical_json.clone(),
        )])
        .await?;
        let canonical_id = noncanonical_id.to_string();
        let lowercase_id = canonical_id.to_lowercase();
        assert_ne!(canonical_id, lowercase_id);
        sqlx::query("UPDATE run_events SET event_json = ? WHERE run_id = ?")
            .bind(canonical_json.replace(&canonical_id, &lowercase_id))
            .bind(canonical_id)
            .execute(&noncanonical.sqlite)
            .await?;
        let error = noncanonical
            .source
            .verify_legacy_run_history_in(&noncanonical.sqlite)
            .await
            .unwrap_err();
        assert!(matches!(
            error.failure,
            LegacyRunHistoryVerificationFailure::ReadDestination(crate::Error::RunEventMismatch {
                field: "run_id",
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_verification_fails_on_prefix_and_current_row_corruption()
    -> TestResult<()> {
        let run_id = run_id(90);
        let created = serde_json::to_string(&created_value(&run_id, "verify"))?;

        let changed_prefix = TestContext::new().await?;
        changed_prefix.put_event(&run_id, 1, 1, &created).await?;
        let changed = serde_json::to_string(&created_value(&run_id, "changed"))?;
        seed_destination_history(&changed_prefix.sqlite, &run_id, &[(1, changed)]).await?;
        assert!(
            changed_prefix
                .source
                .verify_legacy_run_history_in(&changed_prefix.sqlite)
                .await
                .is_err()
        );

        let corrupt_row = TestContext::new().await?;
        corrupt_row.put_event(&run_id, 1, 1, &created).await?;
        seed_destination_history(&corrupt_row.sqlite, &run_id, &[(1, created)]).await?;
        sqlx::query("UPDATE runs SET workflow_slug = 'corrupt' WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&corrupt_row.sqlite)
            .await?;
        assert!(
            corrupt_row
                .source
                .verify_legacy_run_history_in(&corrupt_row.sqlite)
                .await
                .is_err()
        );

        let corrupt_json = TestContext::new().await?;
        let created = serde_json::to_string(&created_value(&run_id, "verify"))?;
        corrupt_json.put_event(&run_id, 1, 1, &created).await?;
        seed_destination_history(&corrupt_json.sqlite, &run_id, &[(1, created)]).await?;
        sqlx::query("UPDATE runs SET summary_json = '{}' WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&corrupt_json.sqlite)
            .await?;
        assert!(
            corrupt_json
                .source
                .verify_legacy_run_history_in(&corrupt_json.sqlite)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_errors_expose_safe_counts_without_event_contents() -> TestResult<()>
    {
        let context = TestContext::new().await?;
        let run_id = run_id(100);
        let secret = "DO-NOT-RENDER-THIS-TITLE";
        let mut value = created_value(&run_id, secret);
        value
            .get_mut("properties")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("settings");
        context
            .put_event(&run_id, 1, 1, &serde_json::to_string(&value)?)
            .await?;

        let error = context
            .source
            .import_legacy_run_history_into(&context.sqlite)
            .await
            .unwrap_err();
        let rendered = format!("{error:?} {error}");
        let chain = error::collect_chain(&error).join(": ");
        assert!(!rendered.contains(secret));
        assert!(!chain.contains(secret));
        assert!(error.source().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn legacy_run_history_scan_ignores_non_event_run_namespaces() -> TestResult<()> {
        let context = TestContext::new().await?;
        let run_id = run_id(110);
        context
            .put_event(
                &run_id,
                1,
                1,
                &serde_json::to_string(&created_value(&run_id, "event"))?,
            )
            .await?;
        context
            .put_raw(
                SlateKey::new("runs").with(run_id).with("state"),
                b"not event JSON",
            )
            .await?;
        assert_eq!(context.import().await?.imported_events, 1);
        Ok(())
    }
}
