//! Run-scoped stage execution identity.
//!
//! A *stage execution* is one top-level handler invocation of a node that
//! became observable within a run. Its 1-based ordinal is the numeric
//! component of the external `StageId` (`node_id@N`). The ordinal is distinct
//! from the *graph visit* (how many times workflow control entered the node,
//! which drives `max_visits` and checkpoints) and from the *handler attempt*
//! (automatic retries inside one execution).
//!
//! The tracker is deliberately not checkpointed: its durable source of truth
//! is the append-only stage event history. On resume it is seeded from the
//! run projection's per-node maxima, so a reexecuted in-flight node allocates
//! the next unused ordinal instead of mutating the prior execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fabro_types::{RunProjection, StageId};

/// One reserved stage execution: the identity of a single resumable handler
/// invocation of a node.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StageExecution {
    /// Canonical external identity for this execution.
    pub stage_id:     StageId,
    /// Graph visit that produced this execution.
    pub graph_visit:  u32,
    /// Prior post-checkpoint execution superseded by this resumed execution.
    pub resumed_from: Option<StageId>,
}

#[derive(Debug, Default)]
struct NodeExecutionState {
    /// Highest execution ordinal observed or reserved for this node.
    high_water:   u32,
    /// Pending provenance link, consumed by the next reservation.
    resumed_from: Option<StageId>,
    /// Execution reserved since the latest node boundary.
    active:       Option<Arc<StageExecution>>,
}

/// Seed data for the [`StageExecutionTracker`], derived from the run
/// projection when a run is resumed. A fresh run uses the default (empty)
/// seed; new run IDs own a new ordinal sequence.
#[derive(Debug, Default)]
pub(crate) struct StageExecutionSeed {
    nodes: HashMap<String, NodeExecutionState>,
}

impl StageExecutionSeed {
    /// Build the seed from the run projection at resume time.
    ///
    /// `checkpoint_seq` is the event sequence number of the selected
    /// checkpoint. Only stages that first became observable *after* that
    /// checkpoint are eligible provenance targets: an older execution with the
    /// same node ID completed before the checkpoint and is not what the
    /// resumed replay supersedes.
    #[must_use]
    pub(crate) fn from_projection(projection: &RunProjection, checkpoint_seq: u32) -> Self {
        let mut nodes = HashMap::new();
        for (stage_id, stage) in projection.iter_stages_unordered() {
            let entry = nodes
                .entry(stage_id.node_id().to_owned())
                .or_insert_with(NodeExecutionState::default);
            entry.high_water = entry.high_water.max(stage_id.visit());
            if stage.first_event_seq.get() > checkpoint_seq {
                let is_latest = entry
                    .resumed_from
                    .as_ref()
                    .is_none_or(|current| current.visit() < stage_id.visit());
                if is_latest {
                    entry.resumed_from = Some(stage_id.clone());
                }
            }
        }
        Self { nodes }
    }

    #[cfg(test)]
    pub(crate) fn test_with_high_water(
        high_water: &StageId,
        resumed_from: Option<StageId>,
    ) -> Self {
        let node_id = high_water.node_id().to_owned();
        Self {
            nodes: HashMap::from([(node_id, NodeExecutionState {
                high_water: high_water.visit(),
                resumed_from,
                active: None,
            })]),
        }
    }
}

/// Cloneable, run-scoped allocator for stage execution ordinals. Clones share
/// one synchronized state so the core lifecycle and direct-dispatch handlers
/// (parallel branches) allocate from the same sequence.
#[derive(Clone, Debug, Default)]
pub(crate) struct StageExecutionTracker {
    state: Arc<Mutex<HashMap<String, NodeExecutionState>>>,
}

impl StageExecutionTracker {
    #[must_use]
    pub(crate) fn seeded(seed: StageExecutionSeed) -> Self {
        Self {
            state: Arc::new(Mutex::new(seed.nodes)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, NodeExecutionState>> {
        self.state
            .lock()
            .expect("stage execution tracker mutex is never poisoned: no code panics while holding this lock")
    }

    /// Clear the node's prior execution scope at the node boundary. The next
    /// `reserve`/`ensure` call allocates a fresh ordinal; a reservation is not
    /// made here so that a StageStart hook block or process exit before any
    /// stage-scoped event leaves no phantom execution.
    pub(crate) fn begin_node(&self, node_id: &str) {
        if let Some(node) = self.lock().get_mut(node_id) {
            node.active = None;
        }
    }

    /// The node's active execution scope, if one has been reserved since the
    /// last node boundary.
    pub(crate) fn active(&self, node_id: &str) -> Option<Arc<StageExecution>> {
        self.lock()
            .get(node_id)
            .and_then(|node| node.active.as_ref().map(Arc::clone))
    }

    fn reserve_locked(
        state: &mut HashMap<String, NodeExecutionState>,
        node_id: &str,
        graph_visit: u32,
    ) -> Arc<StageExecution> {
        let node = state.entry(node_id.to_owned()).or_default();
        node.high_water = node.high_water.saturating_add(1);
        let execution = Arc::new(StageExecution {
            stage_id: StageId::new(node_id, node.high_water),
            graph_visit,
            resumed_from: node.resumed_from.take(),
        });
        node.active = Some(Arc::clone(&execution));
        execution
    }

    /// Allocate the next execution ordinal for the node and make it the active
    /// scope. Consumes the node's pending provenance link, if any.
    pub(crate) fn reserve(&self, node_id: &str, graph_visit: u32) -> Arc<StageExecution> {
        let mut state = self.lock();
        Self::reserve_locked(&mut state, node_id, graph_visit)
    }

    /// Allocate an execution ordinal without changing the node's active
    /// lifecycle scope or consuming resume provenance.
    ///
    /// Parallel branch dispatches use detached reservations because several
    /// executions of one template node may run concurrently, while the parent
    /// parallel stage remains the owner of resume provenance.
    pub(crate) fn reserve_detached(&self, node_id: &str, graph_visit: u32) -> Arc<StageExecution> {
        let mut state = self.lock();
        let node = state.entry(node_id.to_owned()).or_default();
        node.high_water = node.high_water.saturating_add(1);
        Arc::new(StageExecution {
            stage_id: StageId::new(node_id, node.high_water),
            graph_visit,
            resumed_from: None,
        })
    }

    /// The active scope for the node, reserving one only when none exists.
    /// Later attempts within one execution and checkpoint pre-steps reuse the
    /// first attempt's reservation.
    pub(crate) fn ensure(&self, node_id: &str, graph_visit: u32) -> Arc<StageExecution> {
        let mut state = self.lock();
        if let Some(execution) = state
            .get(node_id)
            .and_then(|node| node.active.as_ref().map(Arc::clone))
        {
            return execution;
        }
        Self::reserve_locked(&mut state, node_id, graph_visit)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use chrono::Utc;
    use fabro_types::{Graph, RunId, RunSpec, StageId, WorkflowSettings, test_support};

    use super::*;

    fn projection_with_stages(stages: &[(&str, u32, u32)]) -> RunProjection {
        let spec = RunSpec {
            run_id:              RunId::new(),
            settings:            WorkflowSettings::default(),
            graph:               Graph::new("test"),
            graph_source:        None,
            workflow_slug:       None,
            workflow_version_id: None,
            automation:          None,
            source_directory:    None,
            labels:              std::collections::HashMap::new(),
            provenance:          test_support::test_run_provenance(),
            manifest_blob:       None,
            definition_blob:     None,
            git:                 None,
            fork_source_ref:     None,
        };
        let mut projection = RunProjection::new(String::new(), spec, Utc::now());
        for (node_id, visit, seq) in stages {
            projection.stage_entry(
                node_id,
                *visit,
                NonZeroU32::new(*seq).expect("test seq must be non-zero"),
            );
        }
        projection
    }

    #[test]
    fn reserve_starts_at_one_and_allocates_monotonically_per_node() {
        let tracker = StageExecutionTracker::default();

        assert_eq!(tracker.reserve("work", 1).stage_id.visit(), 1);
        tracker.begin_node("work");
        assert_eq!(tracker.reserve("work", 2).stage_id.visit(), 2);
        assert_eq!(tracker.reserve("other", 1).stage_id.visit(), 1);
    }

    #[test]
    fn seeds_from_projection_maxima() {
        let projection = projection_with_stages(&[("work", 1, 2), ("work", 2, 5), ("plan", 1, 3)]);
        let seed = StageExecutionSeed::from_projection(&projection, 0);
        let tracker = StageExecutionTracker::seeded(seed);

        assert_eq!(tracker.reserve("work", 1).stage_id.visit(), 3);
        assert_eq!(tracker.reserve("plan", 1).stage_id.visit(), 2);
        assert_eq!(tracker.reserve("new", 1).stage_id.visit(), 1);
    }

    #[test]
    fn graph_visit_and_ordinal_can_diverge() {
        let projection = projection_with_stages(&[("work", 1, 2), ("work", 2, 5)]);
        let seed = StageExecutionSeed::from_projection(&projection, 0);
        let tracker = StageExecutionTracker::seeded(seed);

        let execution = tracker.reserve("work", 2);
        assert_eq!(execution.stage_id.visit(), 3);
        assert_eq!(execution.graph_visit, 2);
    }

    #[test]
    fn ensure_reuses_active_reservation_across_attempts() {
        let tracker = StageExecutionTracker::default();

        let first = tracker.ensure("work", 1);
        let second = tracker.ensure("work", 1);
        assert_eq!(first, second);
        assert_eq!(second.stage_id.visit(), 1);

        tracker.begin_node("work");
        assert_eq!(tracker.ensure("work", 2).stage_id.visit(), 2);
    }

    #[test]
    fn begin_node_clears_only_that_node() {
        let tracker = StageExecutionTracker::default();
        tracker.reserve("work", 1);
        tracker.reserve("verify", 1);

        tracker.begin_node("work");

        assert_eq!(tracker.active("work"), None);
        assert_eq!(
            tracker
                .active("verify")
                .map(|execution| execution.stage_id.visit()),
            Some(1)
        );
    }

    #[test]
    fn provenance_only_selects_stages_after_the_checkpoint() {
        let projection = projection_with_stages(&[("work", 1, 2), ("work", 2, 8), ("plan", 1, 3)]);
        let seed = StageExecutionSeed::from_projection(&projection, 5);

        assert_eq!(
            seed.nodes
                .get("work")
                .and_then(|node| node.resumed_from.as_ref()),
            Some(&StageId::new("work", 2))
        );
        assert_eq!(
            seed.nodes
                .get("plan")
                .and_then(|node| node.resumed_from.as_ref()),
            None
        );
    }

    #[test]
    fn first_reservation_consumes_provenance() {
        let projection = projection_with_stages(&[("work", 1, 6)]);
        let seed = StageExecutionSeed::from_projection(&projection, 5);
        let tracker = StageExecutionTracker::seeded(seed);

        let first = tracker.reserve("work", 1);
        assert_eq!(first.stage_id.visit(), 2);
        assert_eq!(first.resumed_from, Some(StageId::new("work", 1)));

        tracker.begin_node("work");
        let second = tracker.reserve("work", 2);
        assert_eq!(second.stage_id.visit(), 3);
        assert_eq!(second.resumed_from, None);
    }

    #[test]
    fn detached_reservation_preserves_active_scope_and_resume_provenance() {
        let projection = projection_with_stages(&[("work", 1, 6)]);
        let seed = StageExecutionSeed::from_projection(&projection, 5);
        let tracker = StageExecutionTracker::seeded(seed);

        let detached = tracker.reserve_detached("work", 1);
        assert_eq!(detached.stage_id, StageId::new("work", 2));
        assert_eq!(detached.resumed_from, None);
        assert_eq!(tracker.active("work"), None);

        let normal = tracker.reserve("work", 1);
        assert_eq!(normal.stage_id, StageId::new("work", 3));
        assert_eq!(normal.resumed_from, Some(StageId::new("work", 1)));
        assert_eq!(tracker.active("work"), Some(normal));
    }

    #[test]
    fn detached_reservation_does_not_replace_existing_active_scope() {
        let tracker = StageExecutionTracker::default();
        let active = tracker.reserve("work", 1);

        let detached = tracker.reserve_detached("work", 1);

        assert_eq!(detached.stage_id, StageId::new("work", 2));
        assert_eq!(tracker.active("work"), Some(active));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_reservations_stay_unique_per_node() {
        let tracker = StageExecutionTracker::default();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let tracker = tracker.clone();
                tokio::spawn(async move { tracker.reserve("branch", 1).stage_id.visit() })
            })
            .collect();

        let mut ordinals = Vec::new();
        for handle in handles {
            ordinals.push(handle.await.expect("reservation task panicked"));
        }
        ordinals.sort_unstable();
        assert_eq!(ordinals, (1..=8).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_detached_reservations_stay_unique_without_becoming_active() {
        let tracker = StageExecutionTracker::default();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let tracker = tracker.clone();
                tokio::spawn(async move { tracker.reserve_detached("branch", 1).stage_id.visit() })
            })
            .collect();

        let mut ordinals = Vec::new();
        for handle in handles {
            ordinals.push(handle.await.expect("reservation task panicked"));
        }
        ordinals.sort_unstable();
        assert_eq!(ordinals, (1..=8).collect::<Vec<_>>());
        assert_eq!(tracker.active("branch"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_ensure_calls_reuse_one_reservation() {
        let tracker = StageExecutionTracker::default();
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let tracker = tracker.clone();
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    tracker.ensure("branch", 1).stage_id.visit()
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.await.expect("ensure task panicked"), 1);
        }
    }
}
