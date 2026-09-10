use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};

use bytes::Bytes;
use fabro_types::{BlobHash, RunEvent, RunId, SessionId};
use futures::Stream;
use tokio::sync::{Mutex as AsyncMutex, broadcast, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::run_state::{EventProjectionCache, ProjectedRun, RunProjectionReducer};
use crate::{
    BlobStore, Error, EventEnvelope, EventPayload, Result, RunProjection, RunSummaryStore, StageId,
    run_summary_store,
};

/// Broadcast capacity for live event subscribers; a lagging subscriber refills
/// from SQLite.
const EVENT_BROADCAST_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct RunDatabase {
    inner:     Arc<RunDatabaseInner>,
    read_only: bool,
}

impl std::fmt::Debug for RunDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunDatabase")
            .field("run_id", &self.inner.run_id)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

pub(crate) struct RunDatabaseInner {
    pub(crate) run_id:     RunId,
    blob_store:            Arc<BlobStore>,
    pub(crate) state_lock: AsyncMutex<()>,
    projection_cache:      StdMutex<EventProjectionCache>,
    run_summary_store:     Arc<RunSummaryStore>,
    event_tx:              broadcast::Sender<EventEnvelope>,
}

impl RunDatabaseInner {
    fn lock_projection_cache(&self) -> StdMutexGuard<'_, EventProjectionCache> {
        self.projection_cache.lock().expect(
            "event projection cache mutex is never poisoned: no code panics while holding this lock",
        )
    }
}

impl RunDatabase {
    pub(crate) async fn build(
        run_id: RunId,
        read_only: bool,
        blob_store: Arc<BlobStore>,
        run_summary_store: Arc<RunSummaryStore>,
    ) -> Result<Self> {
        let projected = run_summary_store.load_projection(&run_id).await?;
        Ok(Self::from_event_projection_cache(
            run_id,
            read_only,
            blob_store,
            run_summary_store,
            projected.into(),
        ))
    }

    pub(crate) fn build_empty(
        run_id: RunId,
        blob_store: Arc<BlobStore>,
        run_summary_store: Arc<RunSummaryStore>,
    ) -> Self {
        Self::from_event_projection_cache(
            run_id,
            false,
            blob_store,
            run_summary_store,
            EventProjectionCache::default(),
        )
    }

    fn from_event_projection_cache(
        run_id: RunId,
        read_only: bool,
        blob_store: Arc<BlobStore>,
        run_summary_store: Arc<RunSummaryStore>,
        projection_cache: EventProjectionCache,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(EVENT_BROADCAST_CAPACITY);
        Self {
            inner: Arc::new(RunDatabaseInner {
                run_id,
                blob_store,
                state_lock: AsyncMutex::new(()),
                projection_cache: StdMutex::new(projection_cache),
                run_summary_store,
                event_tx,
            }),
            read_only,
        }
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

    pub(super) async fn projection_snapshot(&self) -> Result<Arc<RunProjection>> {
        let _state_guard = self.inner.state_lock.lock().await;
        self.projection_snapshot_locked()
    }

    fn projection_snapshot_locked(&self) -> Result<Arc<RunProjection>> {
        self.inner
            .lock_projection_cache()
            .state
            .clone()
            .ok_or_else(|| {
                Error::InvalidEvent(format!(
                    "run {} has no run.created event",
                    self.inner.run_id
                ))
            })
    }

    pub(crate) fn install_in_memory_state(&self, projected: ProjectedRun) {
        *self.inner.lock_projection_cache() = projected.into();
    }

    pub(crate) fn publish(&self, event: &EventEnvelope) {
        let _ = self.inner.event_tx.send(event.clone());
    }

    pub(crate) async fn commit_first_event(
        &self,
        payload: &EventPayload,
    ) -> Result<(EventEnvelope, ProjectedRun)> {
        payload.validate(&self.inner.run_id)?;
        let event = RunEvent::try_from(payload)?;
        let _state_guard = self.inner.state_lock.lock().await;
        if self.inner.lock_projection_cache().last_seq != 0 {
            return Err(Error::RunAlreadyExists(self.inner.run_id.to_string()));
        }
        self.commit_event_locked(payload, event).await
    }
}

impl RunDatabase {
    /// Appends an event after validating it against the current run projection.
    ///
    /// Every returned error means the event/current-row transaction did not
    /// commit and is safe to retry. Memory and broadcasts advance only after
    /// the SQLite commit succeeds.
    pub async fn append_event(&self, payload: &EventPayload) -> Result<u32> {
        Ok(Box::pin(self.append_event_envelope(payload)).await?.seq)
    }

    /// Atomically appends `payload` when `predicate` matches the latest run
    /// projection.
    pub async fn append_event_if(
        &self,
        payload: &EventPayload,
        predicate: impl FnOnce(&RunProjection) -> bool,
    ) -> Result<Option<u32>> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        payload.validate(&self.inner.run_id)?;
        let event = RunEvent::try_from(payload)?;
        let _state_guard = self.inner.state_lock.lock().await;
        let projection = self.projection_snapshot_locked()?;
        if !predicate(&projection) {
            return Ok(None);
        }
        Ok(Some(
            Box::pin(self.append_event_envelope_locked(payload, event))
                .await?
                .seq,
        ))
    }

    /// Appends and returns the stored event envelope after pre-write reduction.
    pub async fn append_event_envelope(&self, payload: &EventPayload) -> Result<EventEnvelope> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        payload.validate(&self.inner.run_id)?;
        let event = RunEvent::try_from(payload)?;
        let _state_guard = self.inner.state_lock.lock().await;
        Box::pin(self.append_event_envelope_locked(payload, event)).await
    }

    async fn append_event_envelope_locked(
        &self,
        payload: &EventPayload,
        event: RunEvent,
    ) -> Result<EventEnvelope> {
        let (envelope, projected) = self.commit_event_locked(payload, event).await?;
        // Keep post-commit propagation await-free: cancellation after SQLite
        // commits must not leave in-memory state stale or omit the broadcast.
        self.install_in_memory_state(projected);
        self.publish(&envelope);
        Ok(envelope)
    }

    async fn commit_event_locked(
        &self,
        payload: &EventPayload,
        event: RunEvent,
    ) -> Result<(EventEnvelope, ProjectedRun)> {
        let (expected_last_seq, mut next_state) = {
            let cache = self.inner.lock_projection_cache();
            (cache.last_seq, cache.state.clone())
        };
        let seq = run_summary_store::next_event_seq_after(expected_last_seq)?;
        let prospective = EventEnvelope { seq, event };
        apply_cached_projection_event(&mut next_state, &prospective).map_err(event_rejected)?;
        let next_projection =
            next_state.expect("applying a valid event should always produce a projection");
        let projected = ProjectedRun::new(self.inner.run_id, next_projection, seq);

        let mut transaction = self.inner.run_summary_store.begin().await?;
        let envelope = if expected_last_seq == 0 {
            RunSummaryStore::insert_first_event_on_connection(&mut transaction, &projected, payload)
                .await?
        } else {
            RunSummaryStore::append_event_on_connection(
                &mut transaction,
                expected_last_seq,
                &projected,
                payload,
            )
            .await?
        };
        transaction.commit().await?;
        Ok((envelope, projected))
    }

    pub async fn list_events(&self) -> Result<Vec<EventEnvelope>> {
        self.inner
            .run_summary_store
            .list_events_for_run(&self.inner.run_id)
            .await
    }

    pub async fn last_event_seq(&self) -> Result<Option<u32>> {
        self.inner.run_summary_store.head(&self.inner.run_id).await
    }

    /// Returns up to `limit + 1` events starting at `start_seq`.
    pub async fn list_events_from_with_limit(
        &self,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        self.inner
            .run_summary_store
            .list_events_from_with_limit(&self.inner.run_id, start_seq, limit)
            .await
    }

    /// Returns up to `limit + 1` events before `before_seq`, newest first.
    pub async fn list_events_before_with_limit(
        &self,
        before_seq: Option<u32>,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        self.inner
            .run_summary_store
            .list_events_before_with_limit(&self.inner.run_id, before_seq, limit)
            .await
    }

    pub async fn get_event(&self, seq: u32) -> Result<Option<EventEnvelope>> {
        self.inner
            .run_summary_store
            .get_event_for_run(&self.inner.run_id, seq)
            .await
    }

    pub async fn list_events_for_stage_from_with_limit(
        &self,
        stage_id: &StageId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        self.inner
            .run_summary_store
            .list_events_for_stage_from_with_limit(&self.inner.run_id, stage_id, start_seq, limit)
            .await
    }

    pub async fn list_events_for_session_from_with_limit(
        &self,
        session_id: SessionId,
        start_seq: u32,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        self.inner
            .run_summary_store
            .list_events_for_session_from_with_limit(
                &self.inner.run_id,
                &session_id,
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
        // Subscribe before the durable catch-up query to close the read/subscribe race.
        let mut broadcasts = inner.event_tx.subscribe();
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut next_seq = seq;
            if !refill_from_sql(&inner, &sender, &mut next_seq).await {
                return;
            }
            loop {
                match broadcasts.recv().await {
                    Ok(event) if event.seq < next_seq => {}
                    Ok(event) if event.seq == next_seq => {
                        next_seq = event.seq.saturating_add(1);
                        if sender.send(Ok(event)).is_err() {
                            return;
                        }
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        if !refill_from_sql(&inner, &sender, &mut next_seq).await {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
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

    pub async fn read_blob(&self, blob_hash: &BlobHash) -> Result<Option<Bytes>> {
        self.inner.blob_store.read(blob_hash).await
    }

    pub async fn state(&self) -> Result<RunProjection> {
        Ok(Arc::unwrap_or_clone(self.projection_snapshot().await?))
    }
}

async fn refill_from_sql(
    inner: &RunDatabaseInner,
    sender: &mpsc::UnboundedSender<Result<EventEnvelope>>,
    next_seq: &mut u32,
) -> bool {
    let events = match inner
        .run_summary_store
        .list_events_from_with_limit(&inner.run_id, *next_seq, usize::MAX)
        .await
    {
        Ok(events) => events,
        Err(error) => {
            let _ = sender.send(Err(error));
            return false;
        }
    };
    for event in events {
        *next_seq = event.seq.saturating_add(1);
        if sender.send(Ok(event)).is_err() {
            return false;
        }
    }
    true
}

fn event_rejected(error: Error) -> Error {
    Error::EventRejected {
        source: Box::new(error),
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use fabro_types::{Graph, RunId, WorkflowSettings, test_support};
    use futures::StreamExt as _;
    use object_store::memory::InMemory;
    use serde_json::json;
    use tokio::task;

    use crate::{EventPayload, test_support as store_test_support};

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

    fn stage_payload(run_id: &RunId, index: u32) -> EventPayload {
        EventPayload::new(
            json!({
                "id": format!("evt-{index}"),
                "ts": "2026-04-09T12:00:00Z",
                "run_id": run_id.to_string(),
                "event": "stage.prompt",
                "node_id": "build",
                "stage_id": "build@1",
                "properties": { "visit": 1, "text": format!("prompt {index}") },
            }),
            run_id,
        )
        .unwrap()
    }

    fn store() -> crate::Database {
        store_test_support::test_database(
            Arc::new(InMemory::new()),
            "run-store-sql-tests",
            Duration::from_millis(1),
            None,
        )
    }

    #[tokio::test]
    async fn first_and_later_events_commit_to_sql_before_publication() {
        let store = store();
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();

        let run = store
            .create_run_with_first_event(&run_id, &run_created_payload(&run_id))
            .await
            .unwrap();
        assert_eq!(
            run.append_event(&stage_payload(&run_id, 2)).await.unwrap(),
            2
        );
        assert_eq!(
            run.list_events()
                .await
                .unwrap()
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn watcher_catches_up_from_sql_without_duplicates() {
        let store = store();
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65B".parse().unwrap();
        let run = store
            .create_run_with_first_event(&run_id, &run_created_payload(&run_id))
            .await
            .unwrap();
        run.append_event(&stage_payload(&run_id, 2)).await.unwrap();

        let mut stream = run.watch_events_from(1).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().seq, 1);
        assert_eq!(stream.next().await.unwrap().unwrap().seq, 2);
        run.append_event(&stage_payload(&run_id, 3)).await.unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().seq, 3);
    }

    #[tokio::test]
    async fn simultaneous_appends_allocate_one_contiguous_sql_sequence() {
        let store = store();
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65C".parse().unwrap();
        let run = store
            .create_run_with_first_event(&run_id, &run_created_payload(&run_id))
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for index in 2..=33 {
            let writer = run.clone();
            tasks.push(tokio::spawn(async move {
                writer.append_event(&stage_payload(&run_id, index)).await
            }));
        }
        let mut sequences = Vec::new();
        for task in tasks {
            sequences.push(task.await.unwrap().unwrap());
        }
        sequences.sort_unstable();
        assert_eq!(sequences, (2..=33).collect::<Vec<_>>());
        assert_eq!(run.last_event_seq().await.unwrap(), Some(33));
        assert_eq!(run.list_events().await.unwrap().len(), 33);
    }

    #[tokio::test]
    async fn simultaneous_creation_has_exactly_one_winner() {
        let store = store();
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65D".parse().unwrap();
        let left_payload = run_created_payload(&run_id);
        let right_payload = run_created_payload(&run_id);
        let left = store.create_run_with_first_event(&run_id, &left_payload);
        let right = store.create_run_with_first_event(&run_id, &right_payload);

        let (left, right) = tokio::join!(left, right);
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let error = left.err().or_else(|| right.err()).unwrap();
        assert!(matches!(error, crate::Error::RunAlreadyExists(_)));
        assert_eq!(
            store
                .open_run(&run_id)
                .await
                .unwrap()
                .list_events()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn readers_observe_only_complete_committed_prefixes_during_appends() {
        let store = store();
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65E".parse().unwrap();
        let run = store
            .create_run_with_first_event(&run_id, &run_created_payload(&run_id))
            .await
            .unwrap();
        let writer = run.clone();
        let write_task = tokio::spawn(async move {
            for index in 2..=25 {
                writer.append_event(&stage_payload(&run_id, index)).await?;
                task::yield_now().await;
            }
            crate::Result::Ok(())
        });

        while !write_task.is_finished() {
            let events = run.list_events().await.unwrap();
            assert!(
                events
                    .iter()
                    .enumerate()
                    .all(|(index, event)| event.seq as usize == index + 1)
            );
            task::yield_now().await;
        }
        write_task.await.unwrap().unwrap();
        assert_eq!(run.list_events().await.unwrap().len(), 25);
    }
}
