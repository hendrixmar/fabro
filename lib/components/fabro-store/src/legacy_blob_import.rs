//! Temporary compatibility importer for the legacy SlateDB blob keyspace.
//!
//! Remove this module with the Slate blob backend after the approved legacy
//! support window ends.

use std::error::Error as StdError;
use std::fmt;

use bytes::Bytes;
use fabro_types::BlobHash;
use fabro_util::error;
use futures::TryStreamExt as _;
use sqlx::pool::PoolConnection;
use sqlx::{Acquire as _, Sqlite, SqlitePool};
#[cfg(test)]
use tokio::sync::Barrier;
use tracing::{debug, error};

use crate::Database;
use crate::keys::SlateKey;

const MAX_BATCH_ROWS: usize = 100;
const MAX_BATCH_BYTES: u64 = 1024 * 1024;
const PASSIVE_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;

/// Aggregate size and row count of the exact legacy blob keyspace.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyBlobInventory {
    pub rows:          u64,
    pub bytes:         u64,
    /// Legacy rows whose hash is not yet present in the SQLite blobs table.
    pub pending_rows:  u64,
    /// Bytes belonging to [`Self::pending_rows`].
    pub pending_bytes: u64,
}

/// A failed strict inventory and the aggregate progress observed before it.
pub struct LegacyBlobInventoryError {
    report:  LegacyBlobInventory,
    failure: LegacyBlobInventoryFailure,
}

impl LegacyBlobInventoryError {
    #[must_use]
    pub fn report(&self) -> &LegacyBlobInventory {
        &self.report
    }
}

impl fmt::Debug for LegacyBlobInventoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyBlobInventoryError")
            .field("report", &self.report)
            .field("failure", &self.failure.kind())
            .finish()
    }
}

impl fmt::Display for LegacyBlobInventoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy blob inventory failed after scanning {} rows: {}",
            self.report.rows, self.failure
        )
    }
}

impl StdError for LegacyBlobInventoryError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.failure)
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyBlobInventoryFailure {
    #[error("opening the legacy blob source")]
    OpenSource(#[source] crate::Error),
    #[error("opening the legacy blob scan")]
    OpenSourceScan(#[source] slatedb::Error),
    #[error("reading the legacy blob scan")]
    ReadSourceScan(#[source] slatedb::Error),
    #[error("a legacy blob key is not canonical")]
    InvalidSourceKey,
    #[error("reading a SQLite blob row for the legacy inventory")]
    ReadDestination(#[source] sqlx::Error),
    #[error("a legacy blob inventory counter overflowed")]
    CounterOverflow,
}

impl LegacyBlobInventoryFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Debug for LegacyBlobInventoryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyBlobInventoryFailure")
            .field("kind", &self.kind())
            .finish()
    }
}

/// Aggregate proof produced by a complete legacy-source and SQLite-target
/// verification pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyBlobVerificationReport {
    pub source_rows:         u64,
    pub source_bytes:        u64,
    pub matched_rows:        u64,
    pub matched_bytes:       u64,
    pub target_rows:         u64,
    pub target_bytes:        u64,
    pub missing_rows:        u64,
    pub invalid_source_rows: u64,
    pub invalid_target_rows: u64,
    pub conflicting_rows:    u64,
}

/// A failed complete verification and its aggregate partial report.
pub struct LegacyBlobVerificationError {
    report:  LegacyBlobVerificationReport,
    failure: LegacyBlobVerificationFailure,
}

impl LegacyBlobVerificationError {
    #[must_use]
    pub fn report(&self) -> &LegacyBlobVerificationReport {
        &self.report
    }
}

impl fmt::Debug for LegacyBlobVerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyBlobVerificationError")
            .field("report", &self.report)
            .field("failure", &self.failure.kind())
            .finish()
    }
}

impl fmt::Display for LegacyBlobVerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy blob verification failed after checking {} source rows and {} target rows: {}",
            self.report.source_rows, self.report.target_rows, self.failure
        )
    }
}

impl StdError for LegacyBlobVerificationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.failure)
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyBlobVerificationFailure {
    #[error("opening the legacy blob source")]
    OpenSource(#[source] crate::Error),
    #[error("opening the legacy blob scan")]
    OpenSourceScan(#[source] slatedb::Error),
    #[error("reading the legacy blob scan")]
    ReadSourceScan(#[source] slatedb::Error),
    #[error("a legacy blob key is not canonical")]
    InvalidSourceKey,
    #[error("legacy blob bytes do not match their key digest")]
    SourceDigestMismatch,
    #[error("reading a SQLite blob row for legacy verification")]
    ReadDestination(#[source] sqlx::Error),
    #[error("SQLite is missing a legacy blob row")]
    MissingDestination,
    #[error("SQLite contains different bytes for a legacy blob hash")]
    DestinationConflict,
    #[error("scanning SQLite blob rows")]
    ScanTarget(#[source] sqlx::Error),
    #[error("a SQLite blob hash is not canonical")]
    InvalidTargetHash,
    #[error("SQLite blob bytes do not match their hash")]
    TargetDigestMismatch,
    #[error("a legacy blob verification counter overflowed")]
    CounterOverflow,
}

impl LegacyBlobVerificationFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Debug for LegacyBlobVerificationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyBlobVerificationFailure")
            .field("kind", &self.kind())
            .finish()
    }
}

/// Aggregate progress from one legacy blob import attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyBlobImportReport {
    /// Source rows observed under the exact legacy blob prefix.
    pub scanned_rows:        u64,
    /// Raw source value bytes observed under the exact legacy blob prefix.
    pub scanned_bytes:       u64,
    /// Destination rows newly inserted by committed transactions.
    pub imported_rows:       u64,
    /// Destination bytes newly inserted by committed transactions.
    pub imported_bytes:      u64,
    /// Byte-equal destination rows accepted by committed transactions.
    pub existing_rows:       u64,
    /// Bytes belonging to byte-equal destination rows.
    pub existing_bytes:      u64,
    /// Source rows rejected for malformed keys or digest mismatches.
    pub invalid_rows:        u64,
    /// Destination rows rejected because their bytes differed.
    pub conflicting_rows:    u64,
    /// Successfully committed import transactions.
    pub committed_batches:   u64,
    /// Successful passive WAL checkpoints during the import.
    pub passive_checkpoints: u64,
}

/// A failed legacy blob import and the durable progress completed before it.
pub struct LegacyBlobImportError {
    report:  LegacyBlobImportReport,
    failure: LegacyBlobImportFailure,
}

impl LegacyBlobImportError {
    /// Returns the aggregate durable progress completed before the failure.
    #[must_use]
    pub fn report(&self) -> &LegacyBlobImportReport {
        &self.report
    }

    /// Returns secondary errors encountered while cleaning up the failed
    /// import.
    ///
    /// The standard error source chain preserves the failure that interrupted
    /// the import. Because that chain is linear, rollback, connection-setting
    /// restoration, and connection-retirement errors are exposed separately.
    pub fn cleanup_errors(&self) -> impl Iterator<Item = &(dyn StdError + 'static)> {
        let mut errors = Vec::new();
        self.failure.collect_cleanup_errors(&mut errors);
        errors.into_iter()
    }
}

impl fmt::Debug for LegacyBlobImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyBlobImportError")
            .field("report", &self.report)
            .field("failure", &self.failure.kind())
            .finish()
    }
}

impl fmt::Display for LegacyBlobImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "legacy blob import failed after scanning {} rows and committing {} batches: {}",
            self.report.scanned_rows, self.report.committed_batches, self.failure
        )
    }
}

impl StdError for LegacyBlobImportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.failure.primary_failure())
    }
}

#[derive(strum::IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
enum LegacyBlobImportFailure {
    #[error("opening the legacy blob source")]
    OpenSource(#[source] crate::Error),

    #[error("opening the legacy blob scan")]
    OpenSourceScan(#[source] slatedb::Error),

    #[error("reading the legacy blob scan")]
    ReadSourceScan(#[source] slatedb::Error),

    #[error("a legacy blob key is not canonical")]
    InvalidSourceKey,

    #[error("legacy blob bytes do not match their key digest")]
    SourceDigestMismatch,

    #[error("a legacy blob import counter overflowed")]
    CounterOverflow,

    #[error("acquiring the SQLite import connection")]
    AcquireConnection(#[source] sqlx::Error),

    #[error("reading the SQLite automatic checkpoint setting")]
    ReadAutomaticCheckpoint(#[source] sqlx::Error),

    #[error("disabling SQLite automatic checkpointing")]
    DisableAutomaticCheckpoint(#[source] sqlx::Error),

    #[error("starting a SQLite blob import transaction")]
    BeginTransaction(#[source] sqlx::Error),

    #[error("inserting a SQLite blob row")]
    InsertDestination(#[source] sqlx::Error),

    #[error("reading an existing SQLite blob row")]
    ReadDestination(#[source] sqlx::Error),

    #[error("SQLite contains different bytes for a legacy blob hash")]
    DestinationConflict,

    #[error("committing a SQLite blob import transaction")]
    CommitTransaction(#[source] sqlx::Error),

    #[error("rolling back a failed SQLite blob import transaction")]
    RollbackTransaction {
        #[source]
        source: sqlx::Error,
        prior:  Box<Self>,
    },

    #[error("running a passive SQLite WAL checkpoint")]
    PassiveCheckpoint(#[source] sqlx::Error),

    #[error("the passive SQLite WAL checkpoint could not complete")]
    PassiveCheckpointBusy,

    #[error("restoring the SQLite automatic checkpoint setting")]
    RestoreAutomaticCheckpoint {
        #[source]
        source:           sqlx::Error,
        prior:            Option<Box<Self>>,
        retirement_error: Option<sqlx::Error>,
    },
}

impl LegacyBlobImportFailure {
    fn kind(&self) -> &'static str {
        self.into()
    }

    fn primary_failure(&self) -> &Self {
        match self {
            Self::RollbackTransaction { prior, .. }
            | Self::RestoreAutomaticCheckpoint {
                prior: Some(prior), ..
            } => prior.primary_failure(),
            _ => self,
        }
    }

    fn collect_cleanup_errors<'a>(&'a self, errors: &mut Vec<&'a (dyn StdError + 'static)>) {
        match self {
            Self::RollbackTransaction { source, prior } => {
                errors.push(source);
                prior.collect_cleanup_errors(errors);
            }
            Self::RestoreAutomaticCheckpoint {
                source,
                prior,
                retirement_error,
            } => {
                errors.push(source);
                if let Some(retirement_error) = retirement_error {
                    errors.push(retirement_error);
                }
                if let Some(prior) = prior {
                    prior.collect_cleanup_errors(errors);
                }
            }
            _ => {}
        }
    }
}

impl fmt::Debug for LegacyBlobImportFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("LegacyBlobImportFailure");
        debug.field("kind", &self.kind());
        match self {
            Self::RollbackTransaction { prior, .. } => {
                debug.field("prior_failure", &prior.kind());
            }
            Self::RestoreAutomaticCheckpoint {
                prior,
                retirement_error,
                ..
            } => {
                debug
                    .field("prior_failure", &prior.as_deref().map(Self::kind))
                    .field("retirement_failed", &retirement_error.is_some());
            }
            _ => {}
        }
        debug.finish()
    }
}

#[derive(Default)]
struct ImportControls {
    #[cfg(test)]
    after_automatic_checkpoint_disabled: Option<std::sync::Arc<Barrier>>,
    #[cfg(test)]
    source_after_rows:                   Option<u64>,
    #[cfg(test)]
    passive_checkpoint:                  bool,
    #[cfg(test)]
    restore_automatic_checkpoint:        bool,
}

impl ImportControls {
    #[cfg(test)]
    async fn after_automatic_checkpoint_disabled(&self) {
        if let Some(barrier) = &self.after_automatic_checkpoint_disabled {
            barrier.wait().await;
            barrier.wait().await;
        }
    }

    fn source_scan_error(&self, scanned_rows: u64) -> Option<slatedb::Error> {
        #[cfg(test)]
        if self.source_after_rows == Some(scanned_rows) {
            return Some(slatedb::Error::unavailable(
                "injected legacy source transport failure".to_owned(),
            ));
        }
        let _ = (self, scanned_rows);
        None
    }

    fn passive_checkpoint_error(&self) -> Option<sqlx::Error> {
        #[cfg(test)]
        if self.passive_checkpoint {
            return Some(sqlx::Error::Protocol(
                "injected passive checkpoint failure".to_owned(),
            ));
        }
        let _ = self;
        None
    }

    fn restore_automatic_checkpoint_error(&self) -> Option<sqlx::Error> {
        #[cfg(test)]
        if self.restore_automatic_checkpoint {
            return Some(sqlx::Error::Protocol(
                "injected automatic checkpoint restoration failure".to_owned(),
            ));
        }
        let _ = self;
        None
    }
}

struct ImportConnection {
    connection:     Option<PoolConnection<Sqlite>>,
    retire_on_drop: bool,
}

impl ImportConnection {
    fn new(connection: PoolConnection<Sqlite>) -> Self {
        Self {
            connection:     Some(connection),
            retire_on_drop: false,
        }
    }

    fn get_mut(&mut self) -> &mut PoolConnection<Sqlite> {
        self.connection
            .as_mut()
            .expect("import connection exists until explicit retirement")
    }

    fn retire_if_dropped(&mut self) {
        self.retire_on_drop = true;
    }

    fn checkpoint_setting_restored(&mut self) {
        self.retire_on_drop = false;
    }

    async fn retire(mut self) -> Result<(), sqlx::Error> {
        let Some(mut connection) = self.connection.take() else {
            return Ok(());
        };
        connection.close_on_drop();
        connection.close().await
    }
}

impl Drop for ImportConnection {
    fn drop(&mut self) {
        if self.retire_on_drop {
            if let Some(connection) = &mut self.connection {
                connection.close_on_drop();
            }
        }
    }
}

struct PendingBlob {
    hash:  BlobHash,
    bytes: Bytes,
}

#[derive(Clone, Copy, Default)]
struct BatchReport {
    imported_rows:  u64,
    imported_bytes: u64,
    existing_rows:  u64,
    existing_bytes: u64,
}

impl Database {
    /// Inventories the exact legacy SlateDB blob keyspace against the SQLite
    /// blobs table in `pool`.
    ///
    /// Keys must be canonical, but value digests are not rehashed here: the
    /// inventory only sizes the keyspace, and the import pass validates every
    /// digest before any row is persisted. Rows whose hash the blobs table
    /// does not contain yet are reported as pending so callers can size the
    /// remaining import work.
    pub async fn legacy_blob_inventory(
        &self,
        pool: &SqlitePool,
    ) -> std::result::Result<LegacyBlobInventory, LegacyBlobInventoryError> {
        let mut report = LegacyBlobInventory::default();
        let result = self.run_legacy_blob_inventory(pool, &mut report).await;
        match result {
            Ok(()) => Ok(report),
            Err(failure) => Err(LegacyBlobInventoryError { report, failure }),
        }
    }

    async fn run_legacy_blob_inventory(
        &self,
        pool: &SqlitePool,
        report: &mut LegacyBlobInventory,
    ) -> Result<(), LegacyBlobInventoryFailure> {
        let source = self
            .open_db()
            .await
            .map_err(LegacyBlobInventoryFailure::OpenSource)?;
        let prefix = legacy_blob_prefix();
        let mut entries = source
            .scan_prefix(&prefix)
            .await
            .map_err(LegacyBlobInventoryFailure::OpenSourceScan)?;
        while let Some(entry) = entries
            .next()
            .await
            .map_err(LegacyBlobInventoryFailure::ReadSourceScan)?
        {
            inventory_checked_add(&mut report.rows, 1)?;
            let value_bytes = inventory_usize_to_u64(entry.value.len())?;
            inventory_checked_add(&mut report.bytes, value_bytes)?;
            let hash = parse_source_key(&entry.key, &prefix)
                .ok_or(LegacyBlobInventoryFailure::InvalidSourceKey)?;
            let imported: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM blobs WHERE hash = ?)")
                    .bind(hash.to_string())
                    .fetch_one(pool)
                    .await
                    .map_err(LegacyBlobInventoryFailure::ReadDestination)?;
            if !imported {
                inventory_checked_add(&mut report.pending_rows, 1)?;
                inventory_checked_add(&mut report.pending_bytes, value_bytes)?;
            }
        }
        Ok(())
    }

    /// Verifies every legacy blob against SQLite and validates every SQLite
    /// blob row independently.
    pub async fn verify_legacy_blobs_in(
        &self,
        pool: &SqlitePool,
    ) -> std::result::Result<LegacyBlobVerificationReport, LegacyBlobVerificationError> {
        let mut report = LegacyBlobVerificationReport::default();
        let result = self.run_legacy_blob_verification(pool, &mut report).await;
        match result {
            Ok(()) => Ok(report),
            Err(failure) => Err(LegacyBlobVerificationError { report, failure }),
        }
    }

    async fn run_legacy_blob_verification(
        &self,
        pool: &SqlitePool,
        report: &mut LegacyBlobVerificationReport,
    ) -> Result<(), LegacyBlobVerificationFailure> {
        let source = self
            .open_db()
            .await
            .map_err(LegacyBlobVerificationFailure::OpenSource)?;
        let prefix = legacy_blob_prefix();
        let mut entries = source
            .scan_prefix(&prefix)
            .await
            .map_err(LegacyBlobVerificationFailure::OpenSourceScan)?;
        while let Some(entry) = entries
            .next()
            .await
            .map_err(LegacyBlobVerificationFailure::ReadSourceScan)?
        {
            verification_checked_add(&mut report.source_rows, 1)?;
            let value_bytes = verification_usize_to_u64(entry.value.len())?;
            verification_checked_add(&mut report.source_bytes, value_bytes)?;
            let hash = match validate_source_entry_common(&entry.key, &entry.value, &prefix) {
                Ok(hash) => hash,
                Err(SourceEntryFailure::InvalidKey) => {
                    verification_checked_add(&mut report.invalid_source_rows, 1)?;
                    return Err(LegacyBlobVerificationFailure::InvalidSourceKey);
                }
                Err(SourceEntryFailure::DigestMismatch) => {
                    verification_checked_add(&mut report.invalid_source_rows, 1)?;
                    return Err(LegacyBlobVerificationFailure::SourceDigestMismatch);
                }
            };
            let equal: Option<bool> =
                sqlx::query_scalar("SELECT data = ? FROM blobs WHERE hash = ?")
                    .bind(entry.value.as_ref())
                    .bind(hash.to_string())
                    .fetch_optional(pool)
                    .await
                    .map_err(LegacyBlobVerificationFailure::ReadDestination)?;
            match equal {
                Some(true) => {
                    verification_checked_add(&mut report.matched_rows, 1)?;
                    verification_checked_add(&mut report.matched_bytes, value_bytes)?;
                }
                Some(false) => {
                    verification_checked_add(&mut report.conflicting_rows, 1)?;
                    return Err(LegacyBlobVerificationFailure::DestinationConflict);
                }
                None => {
                    verification_checked_add(&mut report.missing_rows, 1)?;
                    return Err(LegacyBlobVerificationFailure::MissingDestination);
                }
            }
        }

        let mut rows =
            sqlx::query_as::<_, (String, Vec<u8>)>("SELECT hash, data FROM blobs").fetch(pool);
        while let Some((hash_text, bytes)) = rows
            .try_next()
            .await
            .map_err(LegacyBlobVerificationFailure::ScanTarget)?
        {
            verification_checked_add(&mut report.target_rows, 1)?;
            verification_checked_add(
                &mut report.target_bytes,
                verification_usize_to_u64(bytes.len())?,
            )?;
            let Some(hash) = parse_canonical_hash(&hash_text) else {
                verification_checked_add(&mut report.invalid_target_rows, 1)?;
                return Err(LegacyBlobVerificationFailure::InvalidTargetHash);
            };
            if BlobHash::new(&bytes) != hash {
                verification_checked_add(&mut report.invalid_target_rows, 1)?;
                return Err(LegacyBlobVerificationFailure::TargetDigestMismatch);
            }
        }
        Ok(())
    }

    /// Strictly imports the legacy SlateDB blob keyspace into a SQLite blob
    /// store.
    ///
    /// # Errors
    ///
    /// Returns a typed error with partial durable progress if source
    /// validation, destination persistence, checkpointing, or connection
    /// cleanup fails.
    pub async fn import_legacy_blobs_into(
        &self,
        pool: &SqlitePool,
    ) -> std::result::Result<LegacyBlobImportReport, LegacyBlobImportError> {
        self.import_legacy_blobs_with_controls(pool, &ImportControls::default())
            .await
    }

    async fn import_legacy_blobs_with_controls(
        &self,
        pool: &SqlitePool,
        controls: &ImportControls,
    ) -> std::result::Result<LegacyBlobImportReport, LegacyBlobImportError> {
        let mut report = LegacyBlobImportReport::default();
        let result = self
            .run_legacy_blob_import(pool, controls, &mut report)
            .await;

        match result {
            Ok(()) => {
                debug_import_outcome("complete", &report, None);
                Ok(report)
            }
            Err(failure) => {
                debug_import_outcome("failed", &report, Some(failure.kind()));
                let import_error = LegacyBlobImportError { report, failure };
                for cleanup_error in import_error.cleanup_errors() {
                    let rendered = error::collect_chain(cleanup_error).join(": ");
                    error!(
                        error = %rendered,
                        "Legacy blob import cleanup failed"
                    );
                }
                Err(import_error)
            }
        }
    }

    async fn run_legacy_blob_import(
        &self,
        pool: &SqlitePool,
        controls: &ImportControls,
        report: &mut LegacyBlobImportReport,
    ) -> Result<(), LegacyBlobImportFailure> {
        let mut connection = pool
            .acquire()
            .await
            .map_err(LegacyBlobImportFailure::AcquireConnection)?;
        let previous_automatic_checkpoint = sqlx::query_scalar("PRAGMA wal_autocheckpoint")
            .fetch_one(&mut *connection)
            .await
            .map_err(LegacyBlobImportFailure::ReadAutomaticCheckpoint)?;
        let mut connection = ImportConnection::new(connection);
        // A cancelled PRAGMA future may already have changed connection-local
        // state. Arm retirement before the first mutating await so an altered
        // connection can never return to the pool without restoration.
        connection.retire_if_dropped();

        let import_result = match set_automatic_checkpoint(connection.get_mut(), 0).await {
            Ok(()) => {
                #[cfg(test)]
                controls.after_automatic_checkpoint_disabled().await;
                self.copy_legacy_blobs(connection.get_mut(), controls, report)
                    .await
            }
            Err(source) => Err(LegacyBlobImportFailure::DisableAutomaticCheckpoint(source)),
        };

        let restore_result = if let Some(error) = controls.restore_automatic_checkpoint_error() {
            Err(error)
        } else {
            set_automatic_checkpoint(connection.get_mut(), previous_automatic_checkpoint).await
        };

        if let Err(source) = restore_result {
            let retirement_error = connection.retire().await.err();
            return Err(LegacyBlobImportFailure::RestoreAutomaticCheckpoint {
                source,
                prior: import_result.err().map(Box::new),
                retirement_error,
            });
        }

        connection.checkpoint_setting_restored();
        import_result
    }

    async fn copy_legacy_blobs(
        &self,
        connection: &mut PoolConnection<Sqlite>,
        controls: &ImportControls,
        report: &mut LegacyBlobImportReport,
    ) -> Result<(), LegacyBlobImportFailure> {
        let source = self
            .open_db()
            .await
            .map_err(LegacyBlobImportFailure::OpenSource)?;
        let prefix = SlateKey::new("blobs").with("sha256").into_prefix();
        let prefix_bytes = prefix.as_ref().to_vec();
        let mut entries = source
            .scan_prefix(&prefix_bytes)
            .await
            .map_err(LegacyBlobImportFailure::OpenSourceScan)?;
        let mut pending = Vec::with_capacity(MAX_BATCH_ROWS);
        let mut pending_bytes = 0_u64;
        let mut bytes_since_checkpoint = 0_u64;

        loop {
            if let Some(source) = controls.source_scan_error(report.scanned_rows) {
                return Err(LegacyBlobImportFailure::ReadSourceScan(source));
            }
            let Some(entry) = entries
                .next()
                .await
                .map_err(LegacyBlobImportFailure::ReadSourceScan)?
            else {
                break;
            };
            checked_add(&mut report.scanned_rows, 1)?;
            let value_bytes = usize_to_u64(entry.value.len())?;
            checked_add(&mut report.scanned_bytes, value_bytes)?;

            let hash = validate_source_entry(&entry.key, &entry.value, &prefix_bytes, report)?;

            let would_exceed_rows = pending.len() == MAX_BATCH_ROWS;
            let would_exceed_bytes = pending_bytes
                .checked_add(value_bytes)
                .ok_or(LegacyBlobImportFailure::CounterOverflow)?
                > MAX_BATCH_BYTES;
            if !pending.is_empty() && (would_exceed_rows || would_exceed_bytes) {
                commit_pending_batch(
                    connection,
                    &mut pending,
                    &mut pending_bytes,
                    &mut bytes_since_checkpoint,
                    controls,
                    report,
                )
                .await?;
            }

            pending.push(PendingBlob {
                hash,
                bytes: entry.value,
            });
            checked_add(&mut pending_bytes, value_bytes)?;

            if value_bytes > MAX_BATCH_BYTES {
                commit_pending_batch(
                    connection,
                    &mut pending,
                    &mut pending_bytes,
                    &mut bytes_since_checkpoint,
                    controls,
                    report,
                )
                .await?;
            }
        }

        commit_pending_batch(
            connection,
            &mut pending,
            &mut pending_bytes,
            &mut bytes_since_checkpoint,
            controls,
            report,
        )
        .await?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum SourceEntryFailure {
    InvalidKey,
    DigestMismatch,
}

fn legacy_blob_prefix() -> Vec<u8> {
    SlateKey::new("blobs")
        .with("sha256")
        .into_prefix()
        .as_ref()
        .to_vec()
}

fn parse_canonical_hash(value: &str) -> Option<BlobHash> {
    let canonical = value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    canonical.then(|| value.parse().ok()).flatten()
}

fn parse_source_key(key: &[u8], prefix: &[u8]) -> Option<BlobHash> {
    let suffix = key.strip_prefix(prefix)?;
    let hash_text = std::str::from_utf8(suffix).ok()?;
    parse_canonical_hash(hash_text)
}

fn validate_source_entry_common(
    key: &[u8],
    value: &[u8],
    prefix: &[u8],
) -> Result<BlobHash, SourceEntryFailure> {
    let hash = parse_source_key(key, prefix).ok_or(SourceEntryFailure::InvalidKey)?;
    if BlobHash::new(value) != hash {
        return Err(SourceEntryFailure::DigestMismatch);
    }
    Ok(hash)
}

fn validate_source_entry(
    key: &[u8],
    value: &[u8],
    prefix: &[u8],
    report: &mut LegacyBlobImportReport,
) -> Result<BlobHash, LegacyBlobImportFailure> {
    match validate_source_entry_common(key, value, prefix) {
        Ok(hash) => Ok(hash),
        Err(SourceEntryFailure::InvalidKey) => {
            invalid_source_row(report, LegacyBlobImportFailure::InvalidSourceKey)
        }
        Err(SourceEntryFailure::DigestMismatch) => {
            invalid_source_row(report, LegacyBlobImportFailure::SourceDigestMismatch)
        }
    }
}

fn invalid_source_row<T>(
    report: &mut LegacyBlobImportReport,
    failure: LegacyBlobImportFailure,
) -> Result<T, LegacyBlobImportFailure> {
    checked_add(&mut report.invalid_rows, 1)?;
    Err(failure)
}

async fn commit_pending_batch(
    connection: &mut PoolConnection<Sqlite>,
    pending: &mut Vec<PendingBlob>,
    pending_bytes: &mut u64,
    bytes_since_checkpoint: &mut u64,
    controls: &ImportControls,
    report: &mut LegacyBlobImportReport,
) -> Result<(), LegacyBlobImportFailure> {
    if pending.is_empty() {
        return Ok(());
    }

    let batch = std::mem::take(pending);
    *pending_bytes = 0;
    let batch_report = commit_batch(connection, batch, report).await?;

    let mut updated = *report;
    checked_add(&mut updated.imported_rows, batch_report.imported_rows)?;
    checked_add(&mut updated.imported_bytes, batch_report.imported_bytes)?;
    checked_add(&mut updated.existing_rows, batch_report.existing_rows)?;
    checked_add(&mut updated.existing_bytes, batch_report.existing_bytes)?;
    checked_add(&mut updated.committed_batches, 1)?;
    *report = updated;

    checked_add(bytes_since_checkpoint, batch_report.imported_bytes)?;
    if *bytes_since_checkpoint >= PASSIVE_CHECKPOINT_BYTES {
        run_checkpoint(connection, controls).await?;
        checked_add(&mut report.passive_checkpoints, 1)?;
        *bytes_since_checkpoint = 0;
    }

    Ok(())
}

async fn commit_batch(
    connection: &mut PoolConnection<Sqlite>,
    batch: Vec<PendingBlob>,
    report: &mut LegacyBlobImportReport,
) -> Result<BatchReport, LegacyBlobImportFailure> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(LegacyBlobImportFailure::BeginTransaction)?;
    let batch_result = async {
        let mut batch_report = BatchReport::default();
        for blob in batch {
            let value_bytes = usize_to_u64(blob.bytes.len())?;
            let result = sqlx::query(
                "INSERT INTO blobs (hash, data) VALUES (?, ?) ON CONFLICT(hash) DO NOTHING",
            )
            .bind(blob.hash.to_string())
            .bind(blob.bytes.as_ref())
            .execute(&mut *transaction)
            .await
            .map_err(LegacyBlobImportFailure::InsertDestination)?;

            if result.rows_affected() == 1 {
                checked_add(&mut batch_report.imported_rows, 1)?;
                checked_add(&mut batch_report.imported_bytes, value_bytes)?;
                continue;
            }

            let stored: Vec<u8> = sqlx::query_scalar("SELECT data FROM blobs WHERE hash = ?")
                .bind(blob.hash.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(LegacyBlobImportFailure::ReadDestination)?;
            if stored != blob.bytes {
                checked_add(&mut report.conflicting_rows, 1)?;
                return Err(LegacyBlobImportFailure::DestinationConflict);
            }
            checked_add(&mut batch_report.existing_rows, 1)?;
            checked_add(&mut batch_report.existing_bytes, value_bytes)?;
        }
        Ok(batch_report)
    }
    .await;

    match batch_result {
        Ok(batch_report) => {
            transaction
                .commit()
                .await
                .map_err(LegacyBlobImportFailure::CommitTransaction)?;
            Ok(batch_report)
        }
        Err(prior) => match transaction.rollback().await {
            Ok(()) => Err(prior),
            Err(source) => Err(LegacyBlobImportFailure::RollbackTransaction {
                source,
                prior: Box::new(prior),
            }),
        },
    }
}

async fn run_checkpoint(
    connection: &mut PoolConnection<Sqlite>,
    controls: &ImportControls,
) -> Result<(), LegacyBlobImportFailure> {
    if let Some(source) = controls.passive_checkpoint_error() {
        return Err(LegacyBlobImportFailure::PassiveCheckpoint(source));
    }

    let result = sqlx::query_as::<_, (i64, i64, i64)>("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_one(&mut **connection)
        .await;
    let (busy, _, _) = result.map_err(LegacyBlobImportFailure::PassiveCheckpoint)?;
    if busy != 0 {
        return Err(LegacyBlobImportFailure::PassiveCheckpointBusy);
    }
    Ok(())
}

fn inventory_usize_to_u64(value: usize) -> Result<u64, LegacyBlobInventoryFailure> {
    u64::try_from(value).map_err(|_| LegacyBlobInventoryFailure::CounterOverflow)
}

fn inventory_checked_add(value: &mut u64, amount: u64) -> Result<(), LegacyBlobInventoryFailure> {
    *value = value
        .checked_add(amount)
        .ok_or(LegacyBlobInventoryFailure::CounterOverflow)?;
    Ok(())
}

fn verification_usize_to_u64(value: usize) -> Result<u64, LegacyBlobVerificationFailure> {
    u64::try_from(value).map_err(|_| LegacyBlobVerificationFailure::CounterOverflow)
}

fn verification_checked_add(
    value: &mut u64,
    amount: u64,
) -> Result<(), LegacyBlobVerificationFailure> {
    *value = value
        .checked_add(amount)
        .ok_or(LegacyBlobVerificationFailure::CounterOverflow)?;
    Ok(())
}

async fn set_automatic_checkpoint(
    connection: &mut PoolConnection<Sqlite>,
    pages: i64,
) -> Result<(), sqlx::Error> {
    // `pages` comes directly from SQLite as an integer, so the dynamic PRAGMA
    // contains no caller-controlled text and cannot change the SQL shape.
    let statement = sqlx::AssertSqlSafe(format!("PRAGMA wal_autocheckpoint = {pages}"));
    sqlx::query(statement).execute(&mut **connection).await?;
    Ok(())
}

fn usize_to_u64(value: usize) -> Result<u64, LegacyBlobImportFailure> {
    u64::try_from(value).map_err(|_| LegacyBlobImportFailure::CounterOverflow)
}

fn checked_add(value: &mut u64, amount: u64) -> Result<(), LegacyBlobImportFailure> {
    *value = value
        .checked_add(amount)
        .ok_or(LegacyBlobImportFailure::CounterOverflow)?;
    Ok(())
}

fn debug_import_outcome(
    outcome: &'static str,
    report: &LegacyBlobImportReport,
    failure_kind: Option<&'static str>,
) {
    debug!(
        outcome,
        failure_kind,
        scanned_rows = report.scanned_rows,
        scanned_bytes = report.scanned_bytes,
        imported_rows = report.imported_rows,
        imported_bytes = report.imported_bytes,
        existing_rows = report.existing_rows,
        existing_bytes = report.existing_bytes,
        invalid_rows = report.invalid_rows,
        conflicting_rows = report.conflicting_rows,
        committed_batches = report.committed_batches,
        passive_checkpoints = report.passive_checkpoints,
        "Legacy blob import finished"
    );
}

#[cfg(test)]
mod tests {
    use std::fmt::{self, Write as _};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use fabro_types::BlobHash;
    use object_store::memory::InMemory;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use tokio::sync::Barrier;
    use tracing::field::{Field, Visit};
    use tracing::instrument::WithSubscriber as _;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    use super::{
        ImportControls, LegacyBlobImportFailure, LegacyBlobImportReport, MAX_BATCH_BYTES,
        PASSIVE_CHECKPOINT_BYTES, set_automatic_checkpoint,
    };
    use crate::keys::SlateKey;
    use crate::{BlobStore, Database, test_support as store_test_support};

    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    struct TestContext {
        _dir:      tempfile::TempDir,
        source:    Database,
        source_db: slatedb::Db,
        sqlite:    sqlx::SqlitePool,
        target:    Arc<BlobStore>,
    }

    impl TestContext {
        async fn new() -> TestResult<Self> {
            let dir = tempfile::tempdir()?;
            let sqlite = fabro_db::Database::connect(dir.path().join("fabro.sqlite3")).await?;
            sqlite.migrate().await?;
            let sqlite = sqlite.clone_pool();
            let target = Arc::new(BlobStore::new(sqlite.clone()));
            let source = Database::new(
                Arc::new(InMemory::new()),
                "legacy-blob-import-tests",
                Duration::from_millis(1),
                None,
                Arc::clone(&target),
                store_test_support::test_run_summary_store(),
            );
            let source_db = source.open_db().await?;
            Ok(Self {
                _dir: dir,
                source,
                source_db,
                sqlite,
                target,
            })
        }

        async fn new_with_single_sqlite_connection() -> TestResult<Self> {
            let mut context = Self::new().await?;
            let options = context.sqlite.connect_options().as_ref().clone();
            context.sqlite.close().await;
            let sqlite = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await?;
            context.target = Arc::new(BlobStore::new(sqlite.clone()));
            context.sqlite = sqlite;
            Ok(context)
        }

        async fn put_blob(&self, bytes: &[u8]) -> TestResult<BlobHash> {
            let hash = BlobHash::new(bytes);
            let key = SlateKey::new("blobs").with("sha256").with(hash);
            self.source_db.put(key, bytes).await?;
            Ok(hash)
        }

        async fn put_raw(&self, key: Vec<u8>, bytes: &[u8]) -> TestResult<()> {
            self.source_db.put(key, bytes).await?;
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

        async fn destination_rows(&self) -> TestResult<i64> {
            Ok(sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
                .fetch_one(&self.sqlite)
                .await?)
        }

        async fn insert_destination(&self, hash: BlobHash, bytes: &[u8]) -> TestResult<()> {
            sqlx::query("INSERT INTO blobs (hash, data) VALUES (?, ?)")
                .bind(hash.to_string())
                .bind(bytes)
                .execute(&self.sqlite)
                .await?;
            Ok(())
        }

        async fn delete_destination(&self, hash: BlobHash) -> TestResult<()> {
            sqlx::query("DELETE FROM blobs WHERE hash = ?")
                .bind(hash.to_string())
                .execute(&self.sqlite)
                .await?;
            Ok(())
        }

        async fn set_automatic_checkpoint(&self, pages: i64) -> TestResult<()> {
            let mut connection = self.sqlite.acquire().await?;
            let statement = sqlx::AssertSqlSafe(format!("PRAGMA wal_autocheckpoint = {pages}"));
            sqlx::query(statement).execute(&mut *connection).await?;
            Ok(())
        }

        async fn automatic_checkpoint(&self) -> TestResult<i64> {
            let mut connection = self.sqlite.acquire().await?;
            Ok(sqlx::query_scalar("PRAGMA wal_autocheckpoint")
                .fetch_one(&mut *connection)
                .await?)
        }

        async fn import(&self) -> TestResult<LegacyBlobImportReport> {
            Ok(self.source.import_legacy_blobs_into(&self.sqlite).await?)
        }
    }

    fn legacy_prefix() -> Vec<u8> {
        SlateKey::new("blobs")
            .with("sha256")
            .into_prefix()
            .as_ref()
            .to_vec()
    }

    async fn seed_blobs(
        context: &TestContext,
        count: usize,
    ) -> TestResult<Vec<(BlobHash, Vec<u8>)>> {
        let mut blobs = Vec::with_capacity(count);
        for index in 0..count {
            let bytes = format!("legacy-blob-{index:04}").into_bytes();
            let hash = context.put_blob(&bytes).await?;
            blobs.push((hash, bytes));
        }
        blobs.sort_by_key(|(hash, _)| *hash);
        Ok(blobs)
    }

    #[tokio::test]
    async fn imports_valid_raw_blobs_and_reports_aggregate_progress() -> TestResult<()> {
        let context = TestContext::new().await?;
        let binary = [0_u8, 0xff, 0x80, b'a'];
        let binary_hash = context.put_blob(&binary).await?;
        let empty_hash = context.put_blob(b"").await?;
        let source_before = context.source_entries().await?;

        let report = context.import().await?;

        assert_eq!(report, LegacyBlobImportReport {
            scanned_rows: 2,
            scanned_bytes: 4,
            imported_rows: 2,
            imported_bytes: 4,
            committed_batches: 1,
            ..LegacyBlobImportReport::default()
        });
        assert_eq!(
            context.target.read(&binary_hash).await?,
            Some(Bytes::copy_from_slice(&binary))
        );
        assert_eq!(context.target.read(&empty_hash).await?, Some(Bytes::new()));
        assert_eq!(context.source_entries().await?, source_before);
        Ok(())
    }

    #[tokio::test]
    async fn empty_source_succeeds_without_committing_a_batch() -> TestResult<()> {
        let context = TestContext::new().await?;

        let report = context.import().await?;

        assert_eq!(report, LegacyBlobImportReport::default());
        assert_eq!(context.destination_rows().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn scan_is_limited_to_the_exact_legacy_prefix() -> TestResult<()> {
        let context = TestContext::new().await?;
        context.put_blob(b"included").await?;
        context
            .source_db
            .put(
                SlateKey::new("blobs").with("other").with("ignored"),
                b"nearby",
            )
            .await?;

        let report = context.import().await?;

        assert_eq!(report.scanned_rows, 1);
        assert_eq!(report.imported_rows, 1);
        assert_eq!(context.destination_rows().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn inventory_streams_the_exact_prefix_and_reports_logical_bytes() -> TestResult<()> {
        let context = TestContext::new().await?;
        context.put_blob(b"").await?;
        context.put_blob(&[0, 0xff, 0x80, b'a']).await?;
        context
            .source_db
            .put(
                SlateKey::new("blobs").with("other").with("ignored"),
                b"nearby",
            )
            .await?;

        let inventory = context
            .source
            .legacy_blob_inventory(&context.sqlite)
            .await?;

        assert_eq!(inventory.rows, 2);
        assert_eq!(inventory.bytes, 4);
        assert_eq!(inventory.pending_rows, 2);
        assert_eq!(inventory.pending_bytes, 4);
        Ok(())
    }

    #[tokio::test]
    async fn inventory_reports_already_imported_rows_as_not_pending() -> TestResult<()> {
        let context = TestContext::new().await?;
        context.put_blob(b"imported-before-inventory").await?;
        context.import().await?;
        context.put_blob(b"still-pending").await?;

        let inventory = context
            .source
            .legacy_blob_inventory(&context.sqlite)
            .await?;

        assert_eq!(inventory.rows, 2);
        assert_eq!(inventory.pending_rows, 1);
        assert_eq!(
            inventory.pending_bytes,
            u64::try_from(b"still-pending".len())?
        );
        Ok(())
    }

    #[tokio::test]
    async fn verification_checks_every_source_and_allows_valid_sqlite_only_rows() -> TestResult<()>
    {
        let context = TestContext::new().await?;
        context.put_blob(b"legacy").await?;
        context.import().await?;
        let sqlite_only = b"written-after-activation";
        context
            .insert_destination(BlobHash::new(sqlite_only), sqlite_only)
            .await?;

        let report = context
            .source
            .verify_legacy_blobs_in(&context.sqlite)
            .await?;

        assert_eq!(report.source_rows, 1);
        assert_eq!(report.matched_rows, 1);
        assert_eq!(report.target_rows, 2);
        assert_eq!(report.missing_rows, 0);
        assert_eq!(report.invalid_source_rows, 0);
        assert_eq!(report.invalid_target_rows, 0);
        assert_eq!(report.conflicting_rows, 0);
        Ok(())
    }

    #[tokio::test]
    async fn verification_reports_a_missing_destination_without_exposing_its_hash() -> TestResult<()>
    {
        let context = TestContext::new().await?;
        let bytes = b"missing-sensitive-content";
        let hash = context.put_blob(bytes).await?;

        let error = context
            .source
            .verify_legacy_blobs_in(&context.sqlite)
            .await
            .expect_err("a missing destination row must fail verification");

        assert_eq!(error.report().source_rows, 1);
        assert_eq!(error.report().missing_rows, 1);
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains(&hash.to_string()));
        assert!(!rendered.contains(std::str::from_utf8(bytes)?));
        Ok(())
    }

    #[tokio::test]
    async fn verification_rejects_invalid_sqlite_hashes_and_bytes() -> TestResult<()> {
        let malformed_hash = TestContext::new().await?;
        let mut connection = malformed_hash.sqlite.acquire().await?;
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&mut *connection)
            .await?;
        sqlx::query("INSERT INTO blobs (hash, data) VALUES (?, ?)")
            .bind("not-a-canonical-hash")
            .bind(b"bytes".as_slice())
            .execute(&mut *connection)
            .await?;
        sqlx::query("PRAGMA ignore_check_constraints = OFF")
            .execute(&mut *connection)
            .await?;
        drop(connection);
        let error = malformed_hash
            .source
            .verify_legacy_blobs_in(&malformed_hash.sqlite)
            .await
            .expect_err("malformed SQLite hashes must fail verification");
        assert_eq!(error.report().invalid_target_rows, 1);

        let mismatched_bytes = TestContext::new().await?;
        mismatched_bytes
            .insert_destination(BlobHash::new(b"expected"), b"different")
            .await?;
        let error = mismatched_bytes
            .source
            .verify_legacy_blobs_in(&mismatched_bytes.sqlite)
            .await
            .expect_err("SQLite bytes must match their hash");
        assert_eq!(error.report().invalid_target_rows, 1);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_every_noncanonical_legacy_key_shape() -> TestResult<()> {
        let valid_hash = BlobHash::new(b"valid-shape").to_string();
        let cases = [
            ("empty", Vec::new()),
            ("short", vec![b'0'; 63]),
            ("long", vec![b'0'; 65]),
            ("uppercase", vec![b'A'; 64]),
            ("non_hex", vec![b'g'; 64]),
            ("non_utf8", vec![0xff; 64]),
            (
                "extra_segment",
                [valid_hash.as_bytes(), b"\0extra"].concat(),
            ),
        ];

        for (case, suffix) in cases {
            let context = TestContext::new().await?;
            let mut key = legacy_prefix();
            key.extend_from_slice(&suffix);
            context.put_raw(key, b"source-bytes").await?;
            let source_before = context.source_entries().await?;

            let error = context
                .source
                .import_legacy_blobs_into(&context.sqlite)
                .await
                .expect_err("noncanonical key should fail import");

            assert_eq!(error.report().scanned_rows, 1, "case {case}");
            assert_eq!(error.report().invalid_rows, 1, "case {case}");
            assert_eq!(error.report().committed_batches, 0, "case {case}");
            assert!(
                matches!(&error.failure, LegacyBlobImportFailure::InvalidSourceKey),
                "case {case}: {error:?}"
            );
            assert_eq!(context.destination_rows().await?, 0, "case {case}");
            assert_eq!(
                context.source_entries().await?,
                source_before,
                "case {case}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_source_bytes_that_do_not_match_the_key_digest() -> TestResult<()> {
        let context = TestContext::new().await?;
        let mut key = legacy_prefix();
        key.extend_from_slice(BlobHash::new(b"expected").to_string().as_bytes());
        context.put_raw(key, b"different").await?;

        let error = context
            .source
            .import_legacy_blobs_into(&context.sqlite)
            .await
            .expect_err("digest mismatch should fail import");

        assert_eq!(error.report().scanned_rows, 1);
        assert_eq!(error.report().scanned_bytes, 9);
        assert_eq!(error.report().invalid_rows, 1);
        assert!(matches!(
            &error.failure,
            LegacyBlobImportFailure::SourceDigestMismatch
        ));
        assert_eq!(context.destination_rows().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_source_row_discards_the_uncommitted_pending_batch() -> TestResult<()> {
        let context = TestContext::new().await?;
        context.put_blob(b"valid-but-not-yet-committed").await?;
        let mut invalid_key = legacy_prefix();
        invalid_key.extend_from_slice(&[b'z'; 64]);
        context.put_raw(invalid_key, b"invalid").await?;

        let error = context
            .source
            .import_legacy_blobs_into(&context.sqlite)
            .await
            .expect_err("invalid row should stop the import");

        assert_eq!(error.report().scanned_rows, 2);
        assert_eq!(error.report().invalid_rows, 1);
        assert_eq!(error.report().imported_rows, 0);
        assert_eq!(error.report().committed_batches, 0);
        assert_eq!(context.destination_rows().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn equal_destination_rows_are_retry_progress() -> TestResult<()> {
        let context = TestContext::new().await?;
        context.put_blob(b"first").await?;
        context.put_blob(b"second").await?;

        let first = context.import().await?;
        let second = context.import().await?;

        assert_eq!(first.imported_rows, 2);
        assert_eq!(second.scanned_rows, 2);
        assert_eq!(second.imported_rows, 0);
        assert_eq!(second.existing_rows, 2);
        assert_eq!(second.existing_bytes, 11);
        assert_eq!(second.committed_batches, 1);
        assert_eq!(context.destination_rows().await?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn destination_conflict_rolls_back_the_current_batch() -> TestResult<()> {
        let context = TestContext::new().await?;
        let blobs = seed_blobs(&context, 2).await?;
        context
            .insert_destination(blobs[1].0, b"conflicting-destination-bytes")
            .await?;

        let error = context
            .source
            .import_legacy_blobs_into(&context.sqlite)
            .await
            .expect_err("differing destination row should fail import");

        assert_eq!(error.report().scanned_rows, 2);
        assert_eq!(error.report().conflicting_rows, 1);
        assert_eq!(error.report().imported_rows, 0);
        assert_eq!(error.report().committed_batches, 0);
        assert!(matches!(
            &error.failure,
            LegacyBlobImportFailure::DestinationConflict
        ));
        assert_eq!(context.target.read(&blobs[0].0).await?, None);
        assert_eq!(context.destination_rows().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_import_preserves_commits_and_retry_converges() -> TestResult<()> {
        let context = TestContext::new().await?;
        let blobs = seed_blobs(&context, 101).await?;
        let conflict = blobs[100].0;
        context
            .insert_destination(conflict, b"conflicting-destination-bytes")
            .await?;

        let error = context
            .source
            .import_legacy_blobs_into(&context.sqlite)
            .await
            .expect_err("last-row conflict should interrupt import");

        assert_eq!(error.report().scanned_rows, 101);
        assert_eq!(error.report().imported_rows, 100);
        assert_eq!(error.report().conflicting_rows, 1);
        assert_eq!(error.report().committed_batches, 1);
        assert_eq!(context.destination_rows().await?, 101);

        context.delete_destination(conflict).await?;
        let retry = context.import().await?;
        assert_eq!(retry.scanned_rows, 101);
        assert_eq!(retry.existing_rows, 100);
        assert_eq!(retry.imported_rows, 1);
        assert_eq!(retry.committed_batches, 2);
        assert_eq!(context.destination_rows().await?, 101);
        Ok(())
    }

    #[tokio::test]
    async fn row_limit_splits_one_hundred_and_one_values() -> TestResult<()> {
        let context = TestContext::new().await?;
        seed_blobs(&context, 101).await?;

        let report = context.import().await?;

        assert_eq!(report.imported_rows, 101);
        assert_eq!(report.committed_batches, 2);
        Ok(())
    }

    #[tokio::test]
    async fn byte_limit_splits_batches_and_allows_exact_limit() -> TestResult<()> {
        let split_context = TestContext::new().await?;
        split_context.put_blob(&vec![b'a'; 600 * 1024]).await?;
        split_context.put_blob(&vec![b'b'; 600 * 1024]).await?;
        let split = split_context.import().await?;
        assert_eq!(split.committed_batches, 2);

        let exact_context = TestContext::new().await?;
        exact_context
            .put_blob(&vec![b'x'; usize::try_from(MAX_BATCH_BYTES)?])
            .await?;
        exact_context.put_blob(b"").await?;
        let exact = exact_context.import().await?;
        assert_eq!(exact.committed_batches, 1);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_value_is_committed_alone_between_surrounding_batches() -> TestResult<()> {
        let context = TestContext::new().await?;
        let oversized = vec![b'o'; usize::try_from(MAX_BATCH_BYTES + 1)?];
        let oversized_hash = BlobHash::new(&oversized);
        let mut lower = None;
        let mut upper = None;
        for index in 0_u32..10_000 {
            let bytes = format!("surrounding-{index}").into_bytes();
            let hash = BlobHash::new(&bytes);
            if hash < oversized_hash && lower.is_none() {
                lower = Some(bytes);
            } else if hash > oversized_hash && upper.is_none() {
                upper = Some(bytes);
            }
            if lower.is_some() && upper.is_some() {
                break;
            }
        }
        let lower = lower.expect("search should find a hash below the oversized value");
        let upper = upper.expect("search should find a hash above the oversized value");
        context.put_blob(&lower).await?;
        context.put_blob(&oversized).await?;
        context.put_blob(&upper).await?;

        let report = context.import().await?;

        assert_eq!(report.imported_rows, 3);
        assert_eq!(report.committed_batches, 3);
        Ok(())
    }

    #[tokio::test]
    async fn passive_checkpoint_runs_once_after_crossing_each_batch_threshold() -> TestResult<()> {
        let crossing_context = TestContext::new().await?;
        crossing_context
            .put_blob(&vec![b'c'; usize::try_from(PASSIVE_CHECKPOINT_BYTES + 1)?])
            .await?;
        let crossing = crossing_context.import().await?;
        assert_eq!(crossing.passive_checkpoints, 1);

        let multi_context = TestContext::new().await?;
        multi_context
            .put_blob(&vec![
                b'm';
                usize::try_from(PASSIVE_CHECKPOINT_BYTES * 2 + 1)?
            ])
            .await?;
        let multiple = multi_context.import().await?;
        assert_eq!(multiple.passive_checkpoints, 1);
        Ok(())
    }

    #[tokio::test]
    async fn passive_checkpoint_failure_returns_durable_partial_report() -> TestResult<()> {
        let context = TestContext::new().await?;
        context
            .put_blob(&vec![b'p'; usize::try_from(PASSIVE_CHECKPOINT_BYTES + 1)?])
            .await?;
        let controls = ImportControls {
            passive_checkpoint: true,
            ..ImportControls::default()
        };

        let error = context
            .source
            .import_legacy_blobs_with_controls(&context.sqlite, &controls)
            .await
            .expect_err("injected passive checkpoint should fail import");

        assert_eq!(error.report().imported_rows, 1);
        assert_eq!(error.report().committed_batches, 1);
        assert_eq!(error.report().passive_checkpoints, 0);
        assert!(matches!(
            &error.failure,
            LegacyBlobImportFailure::PassiveCheckpoint(sqlx::Error::Protocol(_))
        ));
        let failure_source = std::error::Error::source(&error)
            .and_then(std::error::Error::source)
            .expect("checkpoint failure should preserve its SQL source");
        assert!(failure_source.downcast_ref::<sqlx::Error>().is_some());
        assert_eq!(context.destination_rows().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_transport_failure_preserves_commits_and_typed_source() -> TestResult<()> {
        let context = TestContext::new().await?;
        seed_blobs(&context, 101).await?;
        let controls = ImportControls {
            source_after_rows: Some(101),
            ..ImportControls::default()
        };

        let error = context
            .source
            .import_legacy_blobs_with_controls(&context.sqlite, &controls)
            .await
            .expect_err("injected source transport failure should stop import");

        assert_eq!(error.report().scanned_rows, 101);
        assert_eq!(error.report().imported_rows, 100);
        assert_eq!(error.report().committed_batches, 1);
        assert!(matches!(
            &error.failure,
            LegacyBlobImportFailure::ReadSourceScan(_)
        ));
        let failure_source = std::error::Error::source(&error)
            .and_then(std::error::Error::source)
            .expect("transport failure should preserve its SlateDB source");
        assert!(failure_source.downcast_ref::<slatedb::Error>().is_some());

        let retry = context.import().await?;
        assert_eq!(retry.existing_rows, 100);
        assert_eq!(retry.imported_rows, 1);
        assert_eq!(context.destination_rows().await?, 101);
        Ok(())
    }

    #[tokio::test]
    async fn restoration_failure_preserves_the_prior_source_chain() -> TestResult<()> {
        let context = TestContext::new().await?;
        let controls = ImportControls {
            source_after_rows: Some(0),
            restore_automatic_checkpoint: true,
            ..ImportControls::default()
        };
        let capture = CapturedEvents::default();

        let error = context
            .source
            .import_legacy_blobs_with_controls(&context.sqlite, &controls)
            .with_subscriber(capture.clone())
            .await
            .expect_err("both injected failures should fail import");

        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut saw_slate_source = false;
        while let Some(source) = current {
            saw_slate_source |= source.downcast_ref::<slatedb::Error>().is_some();
            current = source.source();
        }
        assert!(saw_slate_source, "prior source was absent from the chain");
        assert!(
            error
                .cleanup_errors()
                .any(|source| source.downcast_ref::<sqlx::Error>().is_some()),
            "restoration source was absent from cleanup errors"
        );
        let events = capture.events().join("\n");
        assert!(
            events.contains("Legacy blob import cleanup failed"),
            "captured: {events}"
        );
        assert!(
            events.contains("injected automatic checkpoint restoration failure"),
            "captured: {events}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn automatic_checkpoint_setting_is_restored_after_success_and_failure() -> TestResult<()>
    {
        let success = TestContext::new_with_single_sqlite_connection().await?;
        success.set_automatic_checkpoint(37).await?;
        success.put_blob(b"success").await?;
        success.import().await?;
        assert_eq!(success.automatic_checkpoint().await?, 37);

        let failure = TestContext::new_with_single_sqlite_connection().await?;
        failure.set_automatic_checkpoint(41).await?;
        let mut invalid_key = legacy_prefix();
        invalid_key.extend_from_slice(&[b'z'; 64]);
        failure.put_raw(invalid_key, b"invalid").await?;
        failure
            .source
            .import_legacy_blobs_into(&failure.sqlite)
            .await
            .expect_err("invalid source should fail import");
        assert_eq!(failure.automatic_checkpoint().await?, 41);
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_retires_a_connection_with_disabled_checkpointing() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let options = SqliteConnectOptions::new()
            .filename(dir.path().join("fabro.sqlite3"))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE blobs (hash TEXT PRIMARY KEY NOT NULL, data BLOB NOT NULL)")
            .execute(&pool)
            .await?;
        let target = Arc::new(BlobStore::new(pool.clone()));
        let source = Database::new(
            Arc::new(InMemory::new()),
            "legacy-blob-import-cancellation-test",
            Duration::from_millis(1),
            None,
            Arc::clone(&target),
            store_test_support::test_run_summary_store(),
        );

        let mut connection = pool.acquire().await?;
        set_automatic_checkpoint(&mut connection, 73).await?;
        drop(connection);

        let barrier = Arc::new(Barrier::new(2));
        let controls = ImportControls {
            after_automatic_checkpoint_disabled: Some(Arc::clone(&barrier)),
            ..ImportControls::default()
        };
        let task = tokio::spawn({
            let source = source.clone();
            let pool = pool.clone();
            async move {
                let mut report = LegacyBlobImportReport::default();
                source
                    .run_legacy_blob_import(&pool, &controls, &mut report)
                    .await
            }
        });

        barrier.wait().await;
        task.abort();
        let join_error = task.await.expect_err("aborted import should be cancelled");
        assert!(join_error.is_cancelled());

        let mut connection = pool.acquire().await?;
        let observed: i64 = sqlx::query_scalar("PRAGMA wal_autocheckpoint")
            .fetch_one(&mut *connection)
            .await?;
        assert_ne!(observed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn failed_connection_restoration_retires_the_connection() -> TestResult<()> {
        let context = TestContext::new().await?;
        context.set_automatic_checkpoint(43).await?;
        context.put_blob(b"committed-before-restore").await?;
        let controls = ImportControls {
            restore_automatic_checkpoint: true,
            ..ImportControls::default()
        };

        let error = context
            .source
            .import_legacy_blobs_with_controls(&context.sqlite, &controls)
            .await
            .expect_err("injected restoration failure should fail import");

        assert_eq!(error.report().imported_rows, 1);
        assert!(matches!(
            &error.failure,
            LegacyBlobImportFailure::RestoreAutomaticCheckpoint {
                source: sqlx::Error::Protocol(_),
                ..
            }
        ));
        assert_ne!(context.automatic_checkpoint().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn logs_and_error_rendering_expose_counts_but_not_row_data() -> TestResult<()> {
        let context = TestContext::new().await?;
        let sensitive_key_fragment = "sensitive-invalid-legacy-key";
        let sensitive_content = b"sensitive-blob-content";
        let mut key = legacy_prefix();
        key.extend_from_slice(sensitive_key_fragment.as_bytes());
        context.put_raw(key, sensitive_content).await?;
        let capture = CapturedEvents::default();

        let error = context
            .source
            .import_legacy_blobs_into(&context.sqlite)
            .with_subscriber(capture.clone())
            .await
            .expect_err("invalid key should fail import");

        let rendered = format!("{error} {error:?}");
        let events = capture.events().join("\n");
        for output in [&rendered, &events] {
            assert!(!output.contains(sensitive_key_fragment));
            assert!(!output.contains(std::str::from_utf8(sensitive_content)?));
        }
        assert!(events.contains("scanned_rows=1"), "captured: {events}");
        assert!(events.contains("invalid_rows=1"), "captured: {events}");
        assert!(
            events.contains("failure_kind=\"invalid_source_key\""),
            "captured: {events}"
        );
        assert_eq!(capture.events().len(), 1);
        Ok(())
    }

    #[derive(Clone, Default)]
    struct CapturedEvents {
        events:       Arc<Mutex<Vec<String>>>,
        next_span_id: Arc<AtomicU64>,
    }

    impl CapturedEvents {
        fn events(&self) -> Vec<String> {
            self.events
                .lock()
                .expect("captured tracing events mutex should not be poisoned")
                .clone()
        }
    }

    impl Subscriber for CapturedEvents {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(self.next_span_id.fetch_add(1, Ordering::Relaxed) + 1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut visitor = CapturedFields::default();
            event.record(&mut visitor);
            self.events
                .lock()
                .expect("captured tracing events mutex should not be poisoned")
                .push(visitor.output);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[derive(Default)]
    struct CapturedFields {
        output: String,
    }

    impl Visit for CapturedFields {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            write!(&mut self.output, "{}={value:?};", field.name())
                .expect("writing tracing fields to String cannot fail");
        }
    }
}
