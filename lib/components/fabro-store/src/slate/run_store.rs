use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use chrono::Utc;
use fabro_types::{BlobHash, RunEvent, RunId, SessionId};
use futures::Stream;
use slatedb::{Db, DbIterator, DbRead};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

use super::projection_cache::{CachedRunProjection, RunProjectionCache};
use crate::run_state::{EventProjectionCache, RunProjectionReducer};
use crate::{
    BlobStore, Error, EventEnvelope, EventPayload, Result, RunProjection, RunSummaryStore, StageId,
    keys,
};

const DEFAULT_EVENT_TAIL_LIMIT: usize = 1024;
#[derive(Clone)]
pub struct RunDatabase {
    inner:     Arc<RunDatabaseInner>,
    read_only: bool,
}

impl std::fmt::Debug for RunDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunDatabase")
            .field("run_id", &self.inner.run_id)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

pub(crate) struct RunDatabaseInner {
    run_id: RunId,
    db: Db,
    blob_store: Arc<BlobStore>,
    // `None` for reader-built inners: readers never append, so they carry no
    // next-write sequence and any append through them fails as read-only.
    event_seq: Option<AtomicU32>,
    close_lock: Mutex<()>,
    state_lock: Mutex<()>,
    projection_cache: Mutex<EventProjectionCache>,
    shared_projection_cache: Arc<RunProjectionCache>,
    // Shared cell rather than a snapshot so a summary store attached after
    // this writer opened is still picked up by later appends.
    run_summary_store: Arc<OnceLock<Arc<RunSummaryStore>>>,
    recent_events: Mutex<VecDeque<EventEnvelope>>,
    recent_event_limit: usize,
    event_tx: broadcast::Sender<EventEnvelope>,
}

impl RunDatabase {
    pub(crate) async fn build(
        run_id: RunId,
        db: Db,
        read_only: bool,
        blob_store: Arc<BlobStore>,
        shared_projection_cache: Arc<RunProjectionCache>,
        run_summary_store: Arc<OnceLock<Arc<RunSummaryStore>>>,
    ) -> Result<Self> {
        let cached_projection = shared_projection_cache.projection_snapshot(&run_id).await;
        let projection_cache = cached_projection.as_ref().map_or_else(
            EventProjectionCache::default,
            |(projection, last_seq)| EventProjectionCache {
                last_seq: *last_seq,
                state:    Some(Arc::clone(projection)),
            },
        );
        let event_seq = if read_only {
            // Readers never append, so they do not need to scan the full event
            // history to recover the next write sequence.
            None
        } else {
            let next_seq = match &cached_projection {
                Some((_, last_seq)) => last_seq.saturating_add(1),
                None => recover_next_seq(&db, &run_id).await?,
            };
            Some(AtomicU32::new(next_seq))
        };
        let (event_tx, _) = broadcast::channel(DEFAULT_EVENT_TAIL_LIMIT.max(16));
        Ok(Self {
            inner: Arc::new(RunDatabaseInner {
                run_id,
                db,
                blob_store,
                event_seq,
                close_lock: Mutex::new(()),
                state_lock: Mutex::new(()),
                projection_cache: Mutex::new(projection_cache),
                shared_projection_cache,
                run_summary_store,
                recent_events: Mutex::new(VecDeque::with_capacity(DEFAULT_EVENT_TAIL_LIMIT)),
                recent_event_limit: DEFAULT_EVENT_TAIL_LIMIT,
                event_tx,
            }),
            read_only,
        })
    }

    pub(crate) fn from_inner(inner: Arc<RunDatabaseInner>) -> Self {
        Self {
            inner,
            read_only: false,
        }
    }

    pub(crate) fn read_only_clone(&self) -> Self {
        Self {
            inner:     Arc::clone(&self.inner),
            read_only: true,
        }
    }

    pub(crate) fn inner_arc(&self) -> Arc<RunDatabaseInner> {
        Arc::clone(&self.inner)
    }

    pub(crate) fn run_id(&self) -> RunId {
        self.inner.run_id
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.inner.event_tx.subscribe()
    }

    pub(crate) fn matches_run(&self, run_id: &RunId) -> bool {
        self.inner.run_id == *run_id
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let _guard = self.inner.close_lock.lock().await;
        Ok(())
    }

    pub(crate) async fn has_any_events<R>(db: &R, run_id: &RunId) -> Result<bool>
    where
        R: DbRead + Sync,
    {
        let mut iter = db.scan_prefix(keys::run_events_prefix(run_id)).await?;
        Ok(iter.next().await?.is_some())
    }

    pub(crate) async fn build_cached_projection<R>(
        db: &R,
        run_id: &RunId,
    ) -> Result<Option<CachedRunProjection>>
    where
        R: DbRead + Sync,
    {
        let events = list_events_from(db, run_id, 1).await?;
        let Some(last_seq) = events.last().map(|event| event.seq) else {
            return Ok(None);
        };
        let state = RunProjection::apply_events(&events)?;
        Ok(Some(CachedRunProjection::from_projection(
            *run_id, state, last_seq,
        )))
    }

    async fn projected_state(&self) -> Result<Arc<RunProjection>> {
        let _state_guard = self.inner.state_lock.lock().await;
        self.projected_state_locked().await
    }

    async fn projected_state_locked(&self) -> Result<Arc<RunProjection>> {
        self.projected_state_option_locked().await?.ok_or_else(|| {
            Error::InvalidEvent(format!(
                "run {} has no run.created event",
                self.inner.run_id
            ))
        })
    }

    async fn projected_state_option_locked(&self) -> Result<Option<Arc<RunProjection>>> {
        let next_seq = {
            let cache = self.inner.projection_cache.lock().await;
            cache.last_seq.saturating_add(1)
        };
        let events = list_events_from(&self.inner.db, &self.inner.run_id, next_seq).await?;
        let mut cache = self.inner.projection_cache.lock().await;
        for event in &events {
            apply_cached_projection_event(&mut cache.state, event)?;
            cache.last_seq = event.seq;
        }
        Ok(cache.state.clone())
    }

    /// Current projection for validating an append allocated at `seq`. In the
    /// steady state the local cache already sits at `seq - 1` because
    /// `state_lock` serializes appends, so this skips the storage scan that
    /// `projected_state_option_locked` issues.
    async fn projected_state_for_append_locked(
        &self,
        seq: u32,
    ) -> Result<Option<Arc<RunProjection>>> {
        {
            let cache = self.inner.projection_cache.lock().await;
            if cache.last_seq.saturating_add(1) == seq {
                return Ok(cache.state.clone());
            }
        }
        self.projected_state_option_locked().await
    }

    async fn install_in_memory_state_after_append(
        &self,
        event: &EventEnvelope,
        cached: &CachedRunProjection,
    ) {
        {
            let mut projection_cache = self.inner.projection_cache.lock().await;
            projection_cache.state = Some(Arc::clone(&cached.projection));
            projection_cache.last_seq = event.seq;
        }
        self.inner
            .shared_projection_cache
            .replace(cached.clone())
            .await;

        let mut recent_events = self.inner.recent_events.lock().await;
        recent_events.push_back(event.clone());
        while recent_events.len() > self.inner.recent_event_limit {
            recent_events.pop_front();
        }
        drop(recent_events);
        let _ = self.inner.event_tx.send(event.clone());
    }

    async fn update_summary_after_committed_append(&self, cached: &CachedRunProjection) {
        if let Some(store) = self.inner.run_summary_store.get() {
            if let Err(err) = store.upsert_projection(cached).await {
                warn!(
                    run_id = %self.inner.run_id,
                    source_last_seq = cached.last_seq,
                    error = ?err,
                    "failed to update SQLite run summary after committed append"
                );
            }
        }
    }

    async fn cached_events_from(&self, start_seq: u32, limit: usize) -> Option<Vec<EventEnvelope>> {
        let recent_events = self.inner.recent_events.lock().await;
        let oldest_seq = recent_events.front().map(|event| event.seq)?;
        if start_seq < oldest_seq {
            return None;
        }
        let events = recent_events
            .iter()
            .filter(|event| event.seq >= start_seq)
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        Some(events)
    }
}

impl RunDatabase {
    /// Appends an event after validating it against the current run projection.
    ///
    /// A rejected event writes nothing. Every returned error means the event
    /// was not committed and is safe to retry. Once the SlateDB write succeeds,
    /// the append returns success even if a derived cache or SQLite summary
    /// update fails; those failures are logged and repaired by later updates or
    /// startup reconciliation.
    pub async fn append_event(&self, payload: &EventPayload) -> Result<u32> {
        Ok(self.append_event_envelope(payload).await?.seq)
    }

    /// Atomically appends `payload` when `predicate` matches the latest run
    /// projection.
    ///
    /// `Ok(None)` means the predicate rejected the append and nothing was
    /// written. An invalid transition is also rejected before write, and every
    /// returned error means the event was not committed and is safe to retry.
    /// After the SlateDB write succeeds, derived cache and SQLite summary
    /// updates are best-effort and cannot turn the committed append into an
    /// error.
    pub async fn append_event_if(
        &self,
        payload: &EventPayload,
        predicate: impl FnOnce(&RunProjection) -> bool,
    ) -> Result<Option<u32>> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        payload.validate(&self.inner.run_id)?;
        let (envelope, cached) = {
            let _state_guard = self.inner.state_lock.lock().await;
            let projection = self.projected_state_locked().await?;
            if !predicate(&projection) {
                return Ok(None);
            }
            let event = RunEvent::try_from(payload)?;
            let event_bytes = serde_json::to_vec(payload)?;
            self.append_event_envelope_locked(event, event_bytes)
                .await?
        };
        self.update_summary_after_committed_append(&cached).await;
        Ok(Some(envelope.seq))
    }

    /// Appends and returns the stored event envelope after pre-write reduction.
    ///
    /// A rejected event writes nothing. Every returned error means the event
    /// was not committed and is safe to retry. Once the SlateDB write succeeds,
    /// derived cache and SQLite summary updates are best-effort: failures are
    /// logged, and this method still returns the committed envelope.
    pub async fn append_event_envelope(&self, payload: &EventPayload) -> Result<EventEnvelope> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        payload.validate(&self.inner.run_id)?;
        let event = RunEvent::try_from(payload)?;
        let event_bytes = serde_json::to_vec(payload)?;
        let (envelope, cached) = {
            let _state_guard = self.inner.state_lock.lock().await;
            self.append_event_envelope_locked(event, event_bytes)
                .await?
        };
        self.update_summary_after_committed_append(&cached).await;
        Ok(envelope)
    }

    async fn append_event_envelope_locked(
        &self,
        event: RunEvent,
        event_bytes: Vec<u8>,
    ) -> Result<(EventEnvelope, CachedRunProjection)> {
        let event_seq = self.inner.event_seq.as_ref().ok_or(Error::ReadOnly)?;
        let seq = next_event_seq(event_seq)?;
        let envelope = EventEnvelope { seq, event };
        // Validation reduces through the exact code replay uses, so an event
        // is written iff replay can reduce it. `Arc::make_mut` copy-on-writes,
        // leaving the local projection cache untouched on rejection.
        let mut next_state = self.projected_state_for_append_locked(seq).await?;
        apply_cached_projection_event(&mut next_state, &envelope).map_err(event_rejected)?;
        let next_projection =
            next_state.expect("apply_cached_projection_event sets the state on success");
        let cached = CachedRunProjection::from_projection(
            self.inner.run_id,
            Arc::unwrap_or_clone(next_projection),
            seq,
        );
        reserve_event_seq(event_seq, seq)?;
        self.inner
            .db
            .put(
                keys::run_event_key(&self.inner.run_id, seq, Utc::now().timestamp_millis()),
                event_bytes,
            )
            .await?;
        // Box::pin keeps this future small enough for the
        // clippy::large_futures budget of append_event_envelope's many
        // callers.
        Box::pin(self.install_in_memory_state_after_append(&envelope, &cached)).await;
        Ok((envelope, cached))
    }

    pub async fn list_events(&self) -> Result<Vec<EventEnvelope>> {
        self.list_events_from_with_limit(1, usize::MAX).await
    }

    /// Returns the newest stored event sequence without reading event bodies
    /// when a current projection is available.
    pub async fn last_event_seq(&self) -> Result<Option<u32>> {
        let local_last_seq = self.inner.projection_cache.lock().await.last_seq;
        if local_last_seq > 0 {
            return Ok(Some(local_last_seq));
        }

        let next_seq = recover_next_seq(&self.inner.db, &self.inner.run_id).await?;
        Ok(next_seq.checked_sub(1).filter(|seq| *seq > 0))
    }

    /// Returns up to `limit + 1` events starting at `start_seq`. The extra
    /// item lets callers compute `has_more` without a second read.
    pub async fn list_events_from_with_limit(
        &self,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        if let Some(events) = self.cached_events_from(start_seq, limit).await {
            return Ok(events);
        }
        list_events_from_with_limit(&self.inner.db, &self.inner.run_id, start_seq, limit).await
    }

    /// Returns up to `limit + 1` events immediately before `before_seq` in
    /// descending sequence order. Omitting `before_seq` starts at the newest
    /// event, and a cursor beyond the newest event pages from the newest
    /// event. The extra item lets callers compute `has_more`.
    pub async fn list_events_before_with_limit(
        &self,
        before_seq: Option<u32>,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        // Clamp the exclusive end to just past the newest stored event so an
        // oversized cursor pages from the newest event instead of probing
        // empty key space above it, and never past `MAX_EVENT_SEQ + 1`: event
        // keys zero-pad seq to six digits (see `keys::run_event_key`), so a
        // larger end bound would format as a seven-digit prefix that breaks
        // lexicographic key order.
        let newest = u64::from(self.latest_event_seq().await?);
        let end_seq = match before_seq {
            Some(seq) => u64::from(seq),
            None => u64::MAX,
        }
        .min(newest + 1)
        .min(u64::from(keys::MAX_EVENT_SEQ) + 1);
        if end_seq <= 1 {
            return Ok(Vec::new());
        }

        let window_size = u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX);
        let start_seq =
            u32::try_from(end_seq.saturating_sub(window_size).max(1)).unwrap_or(u32::MAX);
        let end_seq = u32::try_from(end_seq)
            .ok()
            .filter(|end| *end <= keys::MAX_EVENT_SEQ);
        let mut events = list_events_in_range_with_limit(
            &self.inner.db,
            &self.inner.run_id,
            start_seq,
            end_seq,
            limit,
        )
        .await?;
        events.reverse();
        Ok(events)
    }

    /// Latest appended event sequence, or 0 when the run has no events.
    /// Served from the projection cache when warm; otherwise recovered with
    /// bounded probes of the event key space rather than a full history scan.
    async fn latest_event_seq(&self) -> Result<u32> {
        match self
            .inner
            .shared_projection_cache
            .projection_snapshot(&self.inner.run_id)
            .await
        {
            Some((_, seq)) => Ok(seq),
            None => recover_latest_seq(&self.inner.db, &self.inner.run_id).await,
        }
    }

    pub async fn get_event(&self, seq: u32) -> Result<Option<EventEnvelope>> {
        get_event(&self.inner.db, &self.inner.run_id, seq).await
    }

    /// Returns up to `limit + 1` events for the given stage visit,
    /// starting at `start_seq`. The `+1` lets callers compute `has_more`.
    ///
    /// Implementation note: filters by stage identity *before* applying
    /// `limit`, so a stage with matches sparsely scattered late in the event
    /// log still returns its full slice (no premature truncation from a
    /// generic `limit`-bounded scan).
    pub async fn list_events_for_stage_from_with_limit(
        &self,
        stage_id: &StageId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        list_events_for_stage_from_with_limit(
            &self.inner.db,
            &self.inner.run_id,
            stage_id,
            start_seq,
            limit,
        )
        .await
    }

    /// Returns up to `limit + 1` durable Ask Fabro session events for the given
    /// session, starting at `start_seq`. The extra item lets callers compute
    /// `has_more` without a second read.
    pub async fn list_events_for_session_from_with_limit(
        &self,
        session_id: SessionId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        list_events_for_session_from_with_limit(
            &self.inner.db,
            &self.inner.run_id,
            session_id,
            start_seq,
            limit,
        )
        .await
    }

    pub fn watch_events_from(
        &self,
        seq: u32,
    ) -> Result<std::pin::Pin<Box<dyn Stream<Item = Result<EventEnvelope>> + Send>>> {
        let inner = Arc::clone(&self.inner);
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut rx = inner.event_tx.subscribe();
            let cached = {
                let recent_events = inner.recent_events.lock().await;
                recent_events
                    .iter()
                    .filter(|event| event.seq >= seq)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let mut next_seq = seq;
            for event in cached {
                next_seq = event.seq.saturating_add(1);
                if sender.send(Ok(event)).is_err() {
                    return;
                }
            }

            loop {
                loop {
                    match rx.try_recv() {
                        Ok(event) => {
                            if event.seq < next_seq {
                                continue;
                            }
                            next_seq = event.seq.saturating_add(1);
                            if sender.send(Ok(event)).is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                        Err(broadcast::error::TryRecvError::Closed) => return,
                    }
                }

                let event = match rx.recv().await {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                if event.seq < next_seq {
                    continue;
                }
                next_seq = event.seq.saturating_add(1);
                if sender.send(Ok(event)).is_err() {
                    return;
                }
            }
        });
        Ok(Box::pin(UnboundedReceiverStream::new(receiver)))
    }

    pub async fn write_blob(&self, data: &[u8]) -> Result<BlobHash> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.inner.blob_store.write(data).await
    }

    pub async fn read_blob(&self, id: &BlobHash) -> Result<Option<Bytes>> {
        self.inner.blob_store.read(id).await
    }

    pub async fn state(&self) -> Result<RunProjection> {
        Ok(Arc::unwrap_or_clone(self.projected_state().await?))
    }
}

fn event_rejected(error: Error) -> Error {
    Error::EventRejected {
        source: Box::new(error),
    }
}

fn next_event_seq(event_seq: &AtomicU32) -> Result<u32> {
    let seq = event_seq.load(Ordering::SeqCst);
    if seq > keys::MAX_EVENT_SEQ {
        return Err(Error::EventSequenceExhausted {
            max_seq: keys::MAX_EVENT_SEQ,
        });
    }
    Ok(seq)
}

fn reserve_event_seq(event_seq: &AtomicU32, seq: u32) -> Result<()> {
    event_seq
        .compare_exchange(seq, seq + 1, Ordering::SeqCst, Ordering::SeqCst)
        .map(|_| ())
        .map_err(|_| Error::Other("event sequence changed while append lock was held".to_string()))
}

fn apply_cached_projection_event(
    state: &mut Option<Arc<RunProjection>>,
    event: &EventEnvelope,
) -> Result<()> {
    if let Some(projection) = state {
        Arc::make_mut(projection).apply_event(event)?;
    } else {
        *state = Some(Arc::new(RunProjection::apply_events(
            std::slice::from_ref(event),
        )?));
    }
    Ok(())
}

async fn recover_next_seq<R>(db: &R, run_id: &RunId) -> Result<u32>
where
    R: DbRead + Sync,
{
    let mut iter = db.scan_prefix(keys::run_events_prefix(run_id)).await?;
    let mut max_seq = 0;
    while let Some(entry) = iter.next().await? {
        let key = key_to_str(&entry.key)?;
        if let Some(seq) = keys::parse_event_seq(key) {
            max_seq = max_seq.max(seq);
        }
    }
    Ok(max_seq.saturating_add(1).max(1))
}

/// Smallest stored event sequence at or above `seq`, if any.
async fn first_event_seq_at_or_after<R>(db: &R, run_id: &RunId, seq: u32) -> Result<Option<u32>>
where
    R: DbRead + Sync,
{
    let mut scan = EventScan::seek(db, run_id, seq).await?;
    Ok(scan.next().await?.map(|(seq, _)| seq))
}

/// Largest stored event sequence for the run, or 0 when the run has no
/// events. Binary-searches the sequence space with single-entry probes so
/// recovery reads O(log `MAX_EVENT_SEQ`) entries instead of the full event
/// history. Probing for the smallest sequence at or above a bound is
/// monotone even when failed appends leave gaps in the sequence.
async fn recover_latest_seq<R>(db: &R, run_id: &RunId) -> Result<u32>
where
    R: DbRead + Sync,
{
    let Some(mut lo) = first_event_seq_at_or_after(db, run_id, 1).await? else {
        return Ok(0);
    };
    // Invariant: `lo` is a stored sequence and no stored sequence is >= `hi`.
    let mut hi = keys::MAX_EVENT_SEQ + 1;
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        match first_event_seq_at_or_after(db, run_id, mid).await? {
            Some(seq) => lo = seq,
            None => hi = mid,
        }
    }
    Ok(lo)
}

/// Cursor over a run's stored events starting at `start_seq`, yielding raw
/// `(seq, payload)` entries in ascending sequence order (event keys embed a
/// zero-padded sequence, so key order matches sequence order).
struct EventScan {
    // `None` when the requested start is beyond the storable sequence range:
    // event keys zero-pad seq to six digits, so seeking past `MAX_EVENT_SEQ`
    // would format a seven-digit prefix that breaks lexicographic order and
    // returns an incorrect slice of history instead of an empty one.
    iter: Option<DbIterator>,
}

impl EventScan {
    async fn seek<R>(db: &R, run_id: &RunId, start_seq: u32) -> Result<Self>
    where
        R: DbRead + Sync,
    {
        if start_seq > keys::MAX_EVENT_SEQ {
            return Ok(Self { iter: None });
        }
        let iter = db.scan(keys::run_events_range(run_id, start_seq)).await?;
        Ok(Self { iter: Some(iter) })
    }

    /// Like `seek`, but stops before `end_seq` instead of scanning to the
    /// end of the run's event namespace.
    async fn seek_before<R>(db: &R, run_id: &RunId, start_seq: u32, end_seq: u32) -> Result<Self>
    where
        R: DbRead + Sync,
    {
        if end_seq > keys::MAX_EVENT_SEQ {
            // No stored sequence exceeds `MAX_EVENT_SEQ`, so a larger end
            // bound is equivalent to an unbounded scan.
            return Self::seek(db, run_id, start_seq).await;
        }
        if start_seq >= end_seq {
            return Ok(Self { iter: None });
        }
        let range = keys::run_event_seq_prefix(run_id, start_seq)
            ..keys::run_event_seq_prefix(run_id, end_seq);
        let iter = db.scan(range).await?;
        Ok(Self { iter: Some(iter) })
    }

    async fn next(&mut self) -> Result<Option<(u32, Bytes)>> {
        let Some(iter) = self.iter.as_mut() else {
            return Ok(None);
        };
        while let Some(entry) = iter.next().await? {
            let key = key_to_str(&entry.key)?;
            let Some(seq) = keys::parse_event_seq(key) else {
                continue;
            };
            return Ok(Some((seq, entry.value)));
        }
        Ok(None)
    }
}

async fn list_events_from<R>(db: &R, run_id: &RunId, start_seq: u32) -> Result<Vec<EventEnvelope>>
where
    R: DbRead + Sync,
{
    list_events_from_with_limit(db, run_id, start_seq, usize::MAX).await
}

/// Returns up to `limit + 1` events starting at `start_seq`; the extra item
/// lets callers compute `has_more` without a second read.
async fn list_events_from_with_limit<R>(
    db: &R,
    run_id: &RunId,
    start_seq: u32,
    limit: usize,
) -> Result<Vec<EventEnvelope>>
where
    R: DbRead + Sync,
{
    list_events_in_range_with_limit(db, run_id, start_seq, None, limit).await
}

async fn list_events_in_range_with_limit<R>(
    db: &R,
    run_id: &RunId,
    start_seq: u32,
    end_seq: Option<u32>,
    limit: usize,
) -> Result<Vec<EventEnvelope>>
where
    R: DbRead + Sync,
{
    if end_seq.is_some_and(|end_seq| end_seq <= start_seq) {
        return Ok(Vec::new());
    }

    let max_events = limit.saturating_add(1);
    // Seek to the page cursor and decode only the requested page plus the
    // sentinel used to compute `has_more`.
    let mut scan = match end_seq {
        Some(end_seq) => EventScan::seek_before(db, run_id, start_seq, end_seq).await?,
        None => EventScan::seek(db, run_id, start_seq).await?,
    };
    let mut events = Vec::new();
    while events.len() < max_events {
        let Some((seq, value)) = scan.next().await? else {
            break;
        };
        events.push(EventEnvelope {
            seq,
            event: serde_json::from_slice(&value)?,
        });
    }
    Ok(events)
}

async fn get_event<R>(db: &R, run_id: &RunId, seq: u32) -> Result<Option<EventEnvelope>>
where
    R: DbRead + Sync,
{
    let mut iter = db
        .scan_prefix(keys::run_event_seq_prefix(run_id, seq))
        .await?;
    let Some(entry) = iter.next().await? else {
        return Ok(None);
    };
    Ok(Some(EventEnvelope {
        seq,
        event: serde_json::from_slice(&entry.value)?,
    }))
}

async fn list_events_for_stage_from_with_limit<R>(
    db: &R,
    run_id: &RunId,
    stage_id: &StageId,
    start_seq: u32,
    limit: usize,
) -> Result<Vec<EventEnvelope>>
where
    R: DbRead + Sync,
{
    // Filter by stage identity *before* applying `limit`: a generic
    // limit-bounded scan would silently drop matches whenever the stage's
    // events are sparse late in the event log.
    //
    // We probe just the stage identity fields with a small partial deserialize and
    // only run the full `RunEvent` parse on matches. Most events in a run
    // belong to other nodes, so this avoids deserializing large payloads
    // (`agent.tool.completed.output`, `agent.message.text`, …) we'd discard.
    #[derive(serde::Deserialize)]
    struct StageIdProbe<'a> {
        #[serde(default, borrow)]
        stage_id: Option<&'a str>,
        #[serde(default, borrow)]
        node_id:  Option<&'a str>,
    }

    let stage_id_string = stage_id.to_string();
    let max_events = limit.saturating_add(1);
    let mut scan = EventScan::seek(db, run_id, start_seq).await?;
    let mut events = Vec::new();
    while events.len() < max_events {
        let Some((seq, value)) = scan.next().await? else {
            break;
        };
        let probe: StageIdProbe = serde_json::from_slice(&value)?;
        let matches_stage_id = probe.stage_id == Some(stage_id_string.as_str());
        let matches_legacy_node_id = probe.stage_id.is_none()
            && stage_id.visit() == 1
            && probe.node_id == Some(stage_id.node_id());
        if !matches_stage_id && !matches_legacy_node_id {
            continue;
        }
        let event: RunEvent = serde_json::from_slice(&value)?;
        events.push(EventEnvelope { seq, event });
    }
    Ok(events)
}

async fn list_events_for_session_from_with_limit<R>(
    db: &R,
    run_id: &RunId,
    session_id: SessionId,
    start_seq: u32,
    limit: usize,
) -> Result<Vec<EventEnvelope>>
where
    R: DbRead + Sync,
{
    #[derive(serde::Deserialize)]
    struct SessionEventProbe<'a> {
        #[serde(default, borrow)]
        session_id: Option<&'a str>,
        #[serde(rename = "event", default, borrow)]
        event_name: Option<&'a str>,
    }

    let session_id_string = session_id.to_string();
    let max_events = limit.saturating_add(1);
    let mut scan = EventScan::seek(db, run_id, start_seq).await?;
    let mut events = Vec::new();
    while events.len() < max_events {
        let Some((seq, value)) = scan.next().await? else {
            break;
        };
        let probe: SessionEventProbe = serde_json::from_slice(&value)?;
        if probe.session_id != Some(session_id_string.as_str())
            || !probe
                .event_name
                .is_some_and(|name| name.starts_with("run.session."))
        {
            continue;
        }

        let event: RunEvent = serde_json::from_slice(&value)?;
        if event.body.is_run_session_event() {
            events.push(EventEnvelope { seq, event });
        }
    }
    Ok(events)
}

fn key_to_str(key: &Bytes) -> Result<&str> {
    std::str::from_utf8(key)
        .map_err(|err| Error::Other(format!("stored key is not valid UTF-8: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use fabro_types::{Graph, RunId, SessionId, StageId, WorkflowSettings, test_support};
    use object_store::memory::InMemory;
    use serde_json::json;

    use crate::{Database, Error, EventPayload, keys};

    fn stage_prompt_payload(run_id: &RunId, idx: u32, node_id: Option<&str>) -> EventPayload {
        stage_prompt_payload_for_stage(run_id, idx, node_id, None)
    }

    fn session_message_payload(run_id: &RunId, idx: u32, session_id: SessionId) -> EventPayload {
        EventPayload::new(
            json!({
                "id": format!("evt-session-{idx}"),
                "ts": "2026-04-09T12:00:00Z",
                "run_id": run_id.to_string(),
                "session_id": session_id.to_string(),
                "event": "run.session.user_message",
                "properties": {
                    "turn_id": fabro_types::TurnId::new().to_string(),
                    "text": format!("message {idx}"),
                },
            }),
            run_id,
        )
        .unwrap()
    }

    fn run_created_payload(run_id: &RunId) -> EventPayload {
        EventPayload::new(
            json!({
                "id": "evt-created",
                "ts": "2026-04-09T11:59:00Z",
                "run_id": run_id.to_string(),
                "event": "run.created",
                "properties": {
                    "settings": WorkflowSettings::default(),
                    "graph": Graph::new("test"),
                    "provenance": test_support::test_run_provenance(),
                },
            }),
            run_id,
        )
        .unwrap()
    }

    fn stage_prompt_payload_for_stage(
        run_id: &RunId,
        idx: u32,
        node_id: Option<&str>,
        stage_id: Option<&StageId>,
    ) -> EventPayload {
        let mut value = json!({
            "id": format!("evt-{idx}"),
            "ts": "2026-04-09T12:00:00Z",
            "run_id": run_id.to_string(),
            "event": "stage.prompt",
            "properties": {
                "visit": 1,
                "text": format!("prompt {idx}"),
            },
        });
        if let Some(node_id) = node_id {
            value
                .as_object_mut()
                .unwrap()
                .insert("node_id".into(), json!(node_id));
        }
        if let Some(stage_id) = stage_id {
            value
                .as_object_mut()
                .unwrap()
                .insert("stage_id".into(), json!(stage_id.to_string()));
        }
        EventPayload::new(value, run_id).unwrap()
    }

    async fn fresh_run() -> super::RunDatabase {
        let object_store = Arc::new(InMemory::new());
        let store = Database::new(object_store, "", Duration::from_millis(1), None);
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let run = store.create_run(&run_id).await.unwrap();
        run.append_event(&run_created_payload(&run_id))
            .await
            .unwrap();
        run
    }

    #[tokio::test]
    async fn list_events_from_with_limit_does_not_read_past_limit_plus_one() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();
        run.inner
            .db
            .put(keys::run_event_key(&run_id, 4, 0), b"invalid json")
            .await
            .unwrap();

        let events = super::list_events_from_with_limit(&run.inner.db, &run_id, 1, 2)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn list_events_from_with_limit_seeks_to_start_sequence() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();
        let mut unreadable_earlier_key = keys::run_event_seq_prefix(&run_id, 2).as_ref().to_vec();
        unreadable_earlier_key.push(0xff);
        run.inner
            .db
            .put(unreadable_earlier_key, b"invalid json")
            .await
            .unwrap();

        let events = super::list_events_from_with_limit(&run.inner.db, &run_id, 3, 1)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![3]);
    }

    #[tokio::test]
    async fn list_events_before_with_limit_returns_newest_events_and_sentinel() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        for idx in 1..=5 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }

        let events = run.list_events_before_with_limit(None, 2).await.unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![6, 5, 4]);
    }

    #[tokio::test]
    async fn list_events_before_with_limit_does_not_read_older_history() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        for idx in 1..=5 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }
        run.inner
            .db
            .put(keys::run_event_key(&run_id, 2, 0), b"invalid json")
            .await
            .unwrap();

        let events = run.list_events_before_with_limit(None, 2).await.unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![6, 5, 4]);
    }

    #[tokio::test]
    async fn list_events_before_with_limit_uses_exclusive_cursor() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        for idx in 1..=5 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }

        let events = run.list_events_before_with_limit(Some(5), 2).await.unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![4, 3, 2]);
        assert!(
            run.list_events_before_with_limit(Some(1), 2)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn list_events_before_with_limit_reads_newest_page_at_max_event_seq() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.inner
            .event_seq
            .as_ref()
            .unwrap()
            .store(keys::MAX_EVENT_SEQ - 1, Ordering::SeqCst);
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();

        let events = run.list_events_before_with_limit(None, 2).await.unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![keys::MAX_EVENT_SEQ, keys::MAX_EVENT_SEQ - 1]);
    }

    #[tokio::test]
    async fn list_events_before_with_limit_clamps_cursor_beyond_max_event_seq() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.inner
            .event_seq
            .as_ref()
            .unwrap()
            .store(keys::MAX_EVENT_SEQ - 1, Ordering::SeqCst);
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();

        let events = run
            .list_events_before_with_limit(Some(u32::MAX), 2)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![keys::MAX_EVENT_SEQ, keys::MAX_EVENT_SEQ - 1]);
    }

    #[tokio::test]
    async fn list_events_from_with_limit_is_empty_beyond_key_order_limit() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.inner
            .event_seq
            .as_ref()
            .unwrap()
            .store(keys::MAX_EVENT_SEQ - 1, Ordering::SeqCst);
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();

        let events = super::list_events_from_with_limit(&run.inner.db, &run_id, 5_000_000, 10)
            .await
            .unwrap();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn list_events_before_with_limit_pages_from_newest_for_oversized_cursor() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        for idx in 1..=5 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }

        let events = run
            .list_events_before_with_limit(Some(500_000), 2)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![6, 5, 4]);
    }

    #[tokio::test]
    async fn recover_latest_seq_returns_zero_for_empty_history() {
        let object_store = Arc::new(InMemory::new());
        let store = Database::new(object_store, "", Duration::from_millis(1), None);
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let run = store.create_run(&run_id).await.unwrap();

        let latest = super::recover_latest_seq(&run.inner.db, &run_id)
            .await
            .unwrap();

        assert_eq!(latest, 0);
    }

    #[tokio::test]
    async fn recover_latest_seq_finds_latest_across_sparse_gaps() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.inner
            .db
            .put(keys::run_event_key(&run_id, 731_204, 0), b"{}")
            .await
            .unwrap();

        let latest = super::recover_latest_seq(&run.inner.db, &run_id)
            .await
            .unwrap();

        assert_eq!(latest, 731_204);
    }

    #[tokio::test]
    async fn recover_latest_seq_reads_max_event_seq() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.inner
            .db
            .put(keys::run_event_key(&run_id, keys::MAX_EVENT_SEQ, 0), b"{}")
            .await
            .unwrap();

        let latest = super::recover_latest_seq(&run.inner.db, &run_id)
            .await
            .unwrap();

        assert_eq!(latest, keys::MAX_EVENT_SEQ);
    }

    #[tokio::test]
    async fn list_events_before_with_limit_serves_newest_page_from_cold_cache() {
        let object_store = Arc::new(InMemory::new());
        let store = Database::new(object_store.clone(), "", Duration::from_millis(1), None);
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let run = store.create_run(&run_id).await.unwrap();
        run.append_event(&run_created_payload(&run_id))
            .await
            .unwrap();
        for idx in 1..=4 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }

        let reopened = Database::new(object_store, "", Duration::from_millis(1), None);
        let reader = reopened.open_run_reader(&run_id).await.unwrap();

        let events = reader.list_events_before_with_limit(None, 2).await.unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![5, 4, 3]);
    }

    #[tokio::test]
    async fn rejected_event_does_not_consume_last_available_sequence() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.inner
            .event_seq
            .as_ref()
            .unwrap()
            .store(keys::MAX_EVENT_SEQ, Ordering::SeqCst);

        let err = run
            .append_event(&run_created_payload(&run_id))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::EventRejected { .. }));

        let seq = run
            .append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        assert_eq!(seq, keys::MAX_EVENT_SEQ);
    }

    #[tokio::test]
    async fn append_event_rejects_sequences_beyond_key_order_limit() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.inner
            .event_seq
            .as_ref()
            .unwrap()
            .store(keys::MAX_EVENT_SEQ, Ordering::SeqCst);

        let seq = run
            .append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        assert_eq!(seq, keys::MAX_EVENT_SEQ);

        let events_before_error = run.list_events().await.unwrap();
        let err = run
            .append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::EventSequenceExhausted { max_seq }
                if max_seq == keys::MAX_EVENT_SEQ
        ));
        assert_eq!(run.list_events().await.unwrap(), events_before_error);
        assert!(
            run.get_event(keys::MAX_EVENT_SEQ + 1)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_events_for_stage_returns_only_matching_events_in_seq_order() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 3, Some("alpha")))
            .await
            .unwrap();

        let events = run
            .list_events_for_stage_from_with_limit(&StageId::new("alpha", 1), 1, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![2, 4]);
    }

    #[tokio::test]
    async fn list_events_for_stage_skips_events_with_no_stage_identity() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.append_event(&stage_prompt_payload(&run_id, 1, None))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("alpha")))
            .await
            .unwrap();

        let events = run
            .list_events_for_stage_from_with_limit(&StageId::new("alpha", 1), 1, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3]);
    }

    #[tokio::test]
    async fn list_events_for_stage_paginates_via_start_seq_on_filtered_slice() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        for idx in 1..=5 {
            let node = if idx % 2 == 0 { "beta" } else { "alpha" };
            run.append_event(&stage_prompt_payload(&run_id, idx, Some(node)))
                .await
                .unwrap();
        }

        // alpha events live at seqs 2, 4, 6. Start at seq=3 should skip seq=2.
        let events = run
            .list_events_for_stage_from_with_limit(&StageId::new("alpha", 1), 3, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![4, 6]);
    }

    #[tokio::test]
    async fn list_events_for_stage_seeks_to_start_sequence() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("alpha")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 2, Some("beta")))
            .await
            .unwrap();
        run.append_event(&stage_prompt_payload(&run_id, 3, Some("alpha")))
            .await
            .unwrap();
        let mut unreadable_earlier_key = keys::run_event_seq_prefix(&run_id, 2).as_ref().to_vec();
        unreadable_earlier_key.push(0xff);
        run.inner
            .db
            .put(unreadable_earlier_key, b"invalid json")
            .await
            .unwrap();

        let events = run
            .list_events_for_stage_from_with_limit(&StageId::new("alpha", 1), 3, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![4]);
    }

    #[tokio::test]
    async fn list_events_for_stage_walks_past_unrelated_events_for_sparse_matches() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        // 200 unrelated events first.
        for idx in 1..=200 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("noise")))
                .await
                .unwrap();
        }
        // Then 3 sparse "alpha" events at the tail.
        for idx in 201..=203 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }

        // limit smaller than the number of unrelated events would have
        // truncated the upstream scan if we had post-filtered.
        let events = run
            .list_events_for_stage_from_with_limit(&StageId::new("alpha", 1), 1, 5)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![202, 203, 204]);
    }

    #[tokio::test]
    async fn list_events_for_stage_returns_limit_plus_one_for_has_more_signal() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        for idx in 1..=5 {
            run.append_event(&stage_prompt_payload(&run_id, idx, Some("alpha")))
                .await
                .unwrap();
        }

        let events = run
            .list_events_for_stage_from_with_limit(&StageId::new("alpha", 1), 1, 2)
            .await
            .unwrap();

        // With limit=2, we expect up to limit+1 = 3 envelopes so the
        // caller can compute has_more.
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn list_events_for_stage_prefers_stage_id_over_node_id() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        let first_visit = StageId::new("verify", 1);
        let second_visit = StageId::new("verify", 2);
        run.append_event(&stage_prompt_payload_for_stage(
            &run_id,
            1,
            Some("verify"),
            Some(&first_visit),
        ))
        .await
        .unwrap();
        run.append_event(&stage_prompt_payload_for_stage(
            &run_id,
            2,
            Some("verify"),
            Some(&second_visit),
        ))
        .await
        .unwrap();

        let events = run
            .list_events_for_stage_from_with_limit(&second_visit, 1, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3]);
    }

    #[tokio::test]
    async fn list_events_for_session_returns_only_matching_run_session_events() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        let session_id = SessionId::new();
        let other_session_id = SessionId::new();
        run.append_event(&stage_prompt_payload(&run_id, 1, Some("noise")))
            .await
            .unwrap();
        run.append_event(&session_message_payload(&run_id, 2, session_id))
            .await
            .unwrap();
        run.append_event(&session_message_payload(&run_id, 3, other_session_id))
            .await
            .unwrap();
        run.append_event(&session_message_payload(&run_id, 4, session_id))
            .await
            .unwrap();

        let events = run
            .list_events_for_session_from_with_limit(session_id, 1, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3, 5]);
    }

    #[tokio::test]
    async fn list_events_for_session_seeks_to_start_sequence() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        let session_id = SessionId::new();
        let other_session_id = SessionId::new();
        run.append_event(&session_message_payload(&run_id, 1, session_id))
            .await
            .unwrap();
        run.append_event(&session_message_payload(&run_id, 2, other_session_id))
            .await
            .unwrap();
        run.append_event(&session_message_payload(&run_id, 3, session_id))
            .await
            .unwrap();
        let mut unreadable_earlier_key = keys::run_event_seq_prefix(&run_id, 2).as_ref().to_vec();
        unreadable_earlier_key.push(0xff);
        run.inner
            .db
            .put(unreadable_earlier_key, b"invalid json")
            .await
            .unwrap();

        let events = run
            .list_events_for_session_from_with_limit(session_id, 3, 100)
            .await
            .unwrap();

        let seqs: Vec<u32> = events.iter().map(|event| event.seq).collect();
        assert_eq!(seqs, vec![4]);
    }

    #[tokio::test]
    async fn list_events_for_session_returns_limit_plus_one_for_has_more_signal() {
        let run = fresh_run().await;
        let run_id = run.run_id();
        let session_id = SessionId::new();
        for idx in 1..=5 {
            run.append_event(&session_message_payload(&run_id, idx, session_id))
                .await
                .unwrap();
        }

        let events = run
            .list_events_for_session_from_with_limit(session_id, 1, 2)
            .await
            .unwrap();

        assert_eq!(events.len(), 3);
    }
}
