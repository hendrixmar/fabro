use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use fabro_agent::Sandbox;
use fabro_core::error::{Error as CoreError, Result as CoreResult};
use fabro_core::graph::NodeSpec;
use fabro_core::lifecycle::{EdgeContext, EdgeDecision, NodeDecision, RunLifecycle};
use fabro_core::state::ExecutionState;
use fabro_graphviz::graph::types::{Edge as GvEdge, Graph as GvGraph, Node as GvNode};

use crate::artifact;
use crate::context::{Context, ParallelBranchPreamble, keys};
use crate::graph::{WorkflowGraph, WorkflowNode};
use crate::handler::llm::preamble::build_preamble;
use crate::outcome::{BilledModelUsage, Outcome};
use crate::runtime_store::RunStoreHandle;

type WfRunState = ExecutionState<Option<BilledModelUsage>>;
type WfNodeDecision = NodeDecision<Option<BilledModelUsage>>;

/// Graphviz edge captured from edge selection, passed to the next node's
/// before_node for fidelity/thread resolution.
#[derive(Debug, Clone)]
struct IncomingEdgeData {
    edge: Arc<GvEdge>,
}

/// Sub-lifecycle responsible for fidelity/thread resolution and context key
/// setup.
pub(crate) struct FidelityLifecycle {
    pub graph:                  Arc<GvGraph>,
    pub sandbox:                Arc<dyn Sandbox>,
    pub run_store:              RunStoreHandle,
    pub run_dir:                PathBuf,
    incoming_edge_data:         Mutex<Option<IncomingEdgeData>>,
    /// True on the first node after checkpoint resume when prior fidelity was
    /// Full.
    degrade_fidelity_on_resume: Mutex<bool>,
}

impl FidelityLifecycle {
    pub(crate) fn new(
        graph: Arc<GvGraph>,
        sandbox: Arc<dyn Sandbox>,
        run_store: RunStoreHandle,
        run_dir: PathBuf,
    ) -> Self {
        Self {
            graph,
            sandbox,
            run_store,
            run_dir,
            incoming_edge_data: Mutex::new(None),
            degrade_fidelity_on_resume: Mutex::new(false),
        }
    }

    pub(crate) fn set_degrade_fidelity_on_resume(&self, flag: bool) {
        *self.degrade_fidelity_on_resume.lock().expect(
            "fidelity mutex should not be poisoned: no code panics while holding this lock",
        ) = flag;
    }

    /// Render the per-branch preamble stash for a parallel node, indexed by
    /// outgoing-edge order (the same order `ParallelHandler` fans out in).
    /// `Null` entries inherit the fork's preamble.
    fn build_parallel_branch_preambles(
        &self,
        node_id: &str,
        fork_fidelity: keys::Fidelity,
        resolved_context: &Context,
        resolved_outcomes: &HashMap<String, Outcome>,
        completed_nodes: &[String],
    ) -> Vec<serde_json::Value> {
        let edges = self.graph.outgoing_edges(node_id);
        let mut preambles: Vec<serde_json::Value> = Vec::with_capacity(edges.len());
        let mut rendered: HashMap<keys::Fidelity, usize> = HashMap::new();

        for (branch_index, edge) in edges.into_iter().enumerate() {
            let Some(target_node) = self.graph.nodes.get(&edge.to) else {
                preambles.push(serde_json::Value::Null);
                continue;
            };
            let resolution = resolve_parallel_branch_fidelity(edge, target_node, fork_fidelity);
            if resolution.requested == Some(keys::Fidelity::Full) {
                tracing::warn!(
                    parallel_node = %node_id,
                    branch = %edge.to,
                    branch_index,
                    effective_fidelity = %keys::Fidelity::Full.degraded(),
                    "Parallel branch fidelity degraded from full"
                );
            }
            let Some(branch_fidelity) = resolution.effective else {
                preambles.push(serde_json::Value::Null);
                continue;
            };
            if let Some(&rendered_index) = rendered.get(&branch_fidelity) {
                preambles.push(preambles[rendered_index].clone());
                continue;
            }

            let entry = ParallelBranchPreamble {
                fidelity: branch_fidelity,
                preamble: build_preamble(
                    branch_fidelity,
                    resolved_context,
                    &self.graph,
                    completed_nodes,
                    resolved_outcomes,
                ),
            };
            rendered.insert(branch_fidelity, preambles.len());
            preambles.push(
                serde_json::to_value(entry)
                    .expect("ParallelBranchPreamble serialization cannot fail"),
            );
        }

        preambles
    }
}

#[async_trait]
impl RunLifecycle<WorkflowGraph> for FidelityLifecycle {
    async fn on_run_start(&self, _graph: &WorkflowGraph, _state: &WfRunState) -> CoreResult<()> {
        // Clear incoming edge data (restart target must not inherit pre-restart edge)
        *self.incoming_edge_data.lock().expect(
            "fidelity mutex should not be poisoned: no code panics while holding this lock",
        ) = None;
        Ok(())
    }

    async fn before_node(
        &self,
        node: &WorkflowNode,
        state: &WfRunState,
    ) -> CoreResult<WfNodeDecision> {
        state.context.set(
            keys::INTERNAL_PARALLEL_BRANCH_PREAMBLES,
            serde_json::Value::Null,
        );

        let incoming = self
            .incoming_edge_data
            .lock()
            .expect("fidelity mutex should not be poisoned: no code panics while holding this lock")
            .take();
        let gv_node = node.inner();

        // 1. Fidelity resolution via resolve_fidelity: edge → node → graph default →
        //    Compact
        let incoming_edge_ref = incoming.as_ref().map(|d| d.edge.as_ref());
        let fidelity = resolve_fidelity(incoming_edge_ref, gv_node, &self.graph);

        // 2. Fidelity degradation on resume (full → summary:high)
        let fidelity = {
            let mut degrade = self.degrade_fidelity_on_resume.lock().expect(
                "fidelity mutex should not be poisoned: no code panics while holding this lock",
            );
            if *degrade {
                *degrade = false;
                fidelity.degraded()
            } else {
                fidelity
            }
        };

        // 3. Set INTERNAL_FIDELITY
        state.context.set(
            keys::INTERNAL_FIDELITY,
            serde_json::json!(fidelity.to_string()),
        );

        // 4. Preamble building: if Full, empty preamble; otherwise build from context
        let mut resolved_values = artifact::resolved_context_snapshot(
            &state.context,
            &self.run_store,
            &*self.sandbox,
            &self.run_dir,
        )
        .await
        .map_err(|err| CoreError::Other(err.to_string()))?;
        let mut resolved_outcomes = artifact::resolve_outcomes_for_execution(
            &state.node_outcomes,
            &self.run_store,
            &*self.sandbox,
            &self.run_dir,
        )
        .await
        .map_err(|err| CoreError::Other(err.to_string()))?;

        // The resolved copies exist only to render prompt preambles, so bound
        // what any one value may contribute before the builders see them.
        // Full renders no preamble and Truncate renders no context values, so
        // there is nothing to bound — except for a parallel node, whose branch
        // stash may render at a richer fidelity.
        let preamble_renders_values =
            !matches!(fidelity, keys::Fidelity::Full | keys::Fidelity::Truncate)
                || gv_node.handler_type() == Some("parallel");
        if preamble_renders_values {
            artifact::demote_large_values_for_prompt(
                &mut resolved_values,
                &mut resolved_outcomes,
                &self.run_store,
                &*self.sandbox,
                &self.run_dir,
            )
            .await;
        }
        let resolved_context = Context::from_values(resolved_values);

        let preamble = build_preamble(
            fidelity,
            &resolved_context,
            &self.graph,
            &state.completed_nodes,
            &resolved_outcomes,
        );
        state
            .context
            .set(keys::CURRENT_PREAMBLE, serde_json::json!(preamble));

        // 5. Parallel nodes: pre-render per-branch preambles into the stash that
        //    ParallelHandler consumes at fan-out.
        if gv_node.handler_type() == Some("parallel") {
            let branch_preambles = self.build_parallel_branch_preambles(
                node.id(),
                fidelity,
                &resolved_context,
                &resolved_outcomes,
                &state.completed_nodes,
            );
            state.context.set(
                keys::INTERNAL_PARALLEL_BRANCH_PREAMBLES,
                serde_json::Value::Array(branch_preambles),
            );
        }

        // 6. Thread ID resolution via resolve_thread_id: edge → node → graph default →
        //    class → previous
        let thread_id = resolve_thread_id(
            incoming_edge_ref,
            gv_node,
            &self.graph,
            state.previous_node_id.as_deref(),
        );

        // 7. Set thread.{tid}.current_node
        if let Some(ref tid) = thread_id {
            let key = keys::thread_current_node_key(tid);
            state.context.set(key, serde_json::json!(node.id()));
        }

        // 8. Set INTERNAL_THREAD_ID (or null)
        match thread_id {
            Some(tid) => {
                state
                    .context
                    .set(keys::INTERNAL_THREAD_ID, serde_json::json!(tid));
            }
            None => {
                state
                    .context
                    .set(keys::INTERNAL_THREAD_ID, serde_json::Value::Null);
            }
        }

        // 9. Set INTERNAL_NODE_VISIT_COUNT and CURRENT_NODE
        let visits = state.node_visits.get(node.id()).copied().unwrap_or(1);
        state
            .context
            .set(keys::CURRENT_NODE, serde_json::json!(node.id()));
        state
            .context
            .set(keys::INTERNAL_NODE_VISIT_COUNT, serde_json::json!(visits));

        Ok(NodeDecision::Continue)
    }

    async fn on_edge_selected(
        &self,
        ctx: &EdgeContext<'_, WorkflowGraph>,
        _state: &WfRunState,
    ) -> CoreResult<EdgeDecision> {
        // Capture fidelity/thread from edge for next node
        if let Some(ref edge) = ctx.edge {
            let gv_edge = edge.inner();
            let edge_data = IncomingEdgeData {
                edge: Arc::new(gv_edge.clone()),
            };
            *self.incoming_edge_data.lock().expect(
                "fidelity mutex should not be poisoned: no code panics while holding this lock",
            ) = Some(edge_data);
        }
        Ok(EdgeDecision::Continue)
    }
}

#[derive(Debug, Clone, Copy)]
struct ParallelBranchFidelityResolution {
    /// The explicit fidelity requested on the edge or node, pre-degradation.
    requested: Option<keys::Fidelity>,
    /// The fidelity to render an entry for; `None` inherits the fork preamble.
    effective: Option<keys::Fidelity>,
}

/// Resolve explicit branch fidelity with edge-over-node precedence.
///
/// Branches with no explicit fidelity inherit the parallel node's preamble.
/// Explicit full fidelity is degraded because concurrent branches cannot share
/// an LLM session. An effective fidelity equal to the parallel node also
/// inherits, avoiding a redundant preamble render.
fn resolve_parallel_branch_fidelity(
    edge: &GvEdge,
    target_node: &GvNode,
    parallel_fidelity: keys::Fidelity,
) -> ParallelBranchFidelityResolution {
    let requested = explicit_fidelity(Some(edge), target_node).map(|(fidelity, _)| fidelity);
    let effective = requested
        .map(keys::Fidelity::degraded)
        .filter(|fidelity| *fidelity != parallel_fidelity);

    ParallelBranchFidelityResolution {
        requested,
        effective,
    }
}

/// Explicit fidelity from the incoming edge attribute, else the node
/// attribute, with the winning source labeled for logging.
fn explicit_fidelity(
    incoming_edge: Option<&GvEdge>,
    node: &GvNode,
) -> Option<(keys::Fidelity, &'static str)> {
    incoming_edge
        .and_then(|e| e.fidelity())
        .and_then(|s| s.parse().ok())
        .map(|f| (f, "edge"))
        .or_else(|| {
            node.fidelity()
                .and_then(|s| s.parse().ok())
                .map(|f| (f, "node"))
        })
}

/// Resolve the context fidelity for a node, following the precedence:
/// 1. Incoming edge `fidelity` attribute
/// 2. Target node `fidelity` attribute
/// 3. Graph `default_fidelity` attribute
/// 4. Default: Compact
fn resolve_fidelity(
    incoming_edge: Option<&GvEdge>,
    node: &GvNode,
    graph: &GvGraph,
) -> keys::Fidelity {
    let (resolved, source) = if let Some((f, source)) = explicit_fidelity(incoming_edge, node) {
        (f, source)
    } else if let Some(f) = graph.default_fidelity().and_then(|s| s.parse().ok()) {
        (f, "graph")
    } else {
        (keys::Fidelity::default(), "default")
    };

    tracing::info!(
        node = %node.id,
        fidelity = %resolved,
        source = source,
        "Fidelity resolved"
    );

    resolved
}

/// Resolve the thread ID for a node, following the precedence:
/// 1. Incoming edge `thread_id` attribute
/// 2. Target node `thread_id` attribute
/// 3. Graph-level default thread
/// 4. Derived class from enclosing subgraph (first class from the node's
///    classes list)
/// 5. Fallback to previous node ID
fn resolve_thread_id(
    incoming_edge: Option<&GvEdge>,
    node: &GvNode,
    graph: &GvGraph,
    previous_node_id: Option<&str>,
) -> Option<String> {
    if let Some(edge) = incoming_edge {
        if let Some(tid) = edge.thread_id() {
            return Some(tid.to_string());
        }
    }
    if let Some(tid) = node.thread_id() {
        return Some(tid.to_string());
    }
    if let Some(tid) = graph.default_thread() {
        return Some(tid.to_string());
    }
    if let Some(first_class) = node.classes.first() {
        return Some(first_class.clone());
    }
    previous_node_id.map(String::from)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use fabro_core::graph::Graph as CoreGraph;
    use fabro_graphviz::graph::{AttrValue, Edge, Graph, Node};
    use fabro_types::fixtures;
    use object_store::memory::InMemory;

    use super::*;
    use crate::context::WorkflowContext;
    use crate::context::keys::Fidelity;

    fn str_attr(value: &str) -> AttrValue {
        AttrValue::String(value.to_string())
    }

    fn parallel_workflow_graph(
        fork_fidelity: Option<&str>,
        branch_a_fidelity: Option<&str>,
    ) -> WorkflowGraph {
        let mut graph = Graph::new("parallel-fidelity");
        let mut start = Node::new("start");
        start
            .attrs
            .insert("shape".to_string(), str_attr("Mdiamond"));
        let mut fork = Node::new("fork");
        fork.attrs
            .insert("shape".to_string(), str_attr("component"));
        if let Some(fidelity) = fork_fidelity {
            fork.attrs
                .insert("fidelity".to_string(), str_attr(fidelity));
        }
        let mut branch_a = Node::new("branch_a");
        if let Some(fidelity) = branch_a_fidelity {
            branch_a
                .attrs
                .insert("fidelity".to_string(), str_attr(fidelity));
        }
        let branch_b = Node::new("branch_b");
        let mut work = Node::new("work");
        work.attrs.insert("shape".to_string(), str_attr("box"));

        graph.nodes.insert(start.id.clone(), start);
        graph.nodes.insert(fork.id.clone(), fork);
        graph.nodes.insert(branch_a.id.clone(), branch_a);
        graph.nodes.insert(branch_b.id.clone(), branch_b);
        graph.nodes.insert(work.id.clone(), work);
        graph.edges.push(Edge::new("start", "fork"));
        graph.edges.push(Edge::new("fork", "branch_a"));
        graph.edges.push(Edge::new("fork", "branch_b"));

        WorkflowGraph(Arc::new(graph))
    }

    async fn test_lifecycle(graph: &WorkflowGraph, run_dir: &Path) -> FidelityLifecycle {
        let store = Arc::new(fabro_store::test_support::test_database(
            Arc::new(InMemory::new()),
            "",
            Duration::from_millis(1),
            None,
        ));
        let run_store = store.create_run(&fixtures::RUN_1).await.unwrap();
        let sandbox: Arc<dyn Sandbox> =
            Arc::new(fabro_agent::LocalSandbox::new(run_dir.to_path_buf()));
        FidelityLifecycle::new(
            graph.0.clone(),
            sandbox,
            RunStoreHandle::local(run_store),
            run_dir.to_path_buf(),
        )
    }

    #[test]
    fn parallel_branch_fidelity_edge_overrides_node() {
        let mut node = Node::new("branch");
        node.attrs
            .insert("fidelity".to_string(), str_attr("compact"));
        let mut edge = Edge::new("fork", "branch");
        edge.attrs
            .insert("fidelity".to_string(), str_attr("truncate"));

        let resolved = resolve_parallel_branch_fidelity(&edge, &node, Fidelity::SummaryHigh);

        assert_eq!(resolved.requested, Some(Fidelity::Truncate));
        assert_eq!(resolved.effective, Some(Fidelity::Truncate));
    }

    #[test]
    fn parallel_branch_fidelity_without_attribute_inherits() {
        let node = Node::new("branch");
        let edge = Edge::new("fork", "branch");

        let resolution = resolve_parallel_branch_fidelity(&edge, &node, Fidelity::Compact);

        assert_eq!(resolution.requested, None);
        assert_eq!(resolution.effective, None);
    }

    #[test]
    fn parallel_branch_full_fidelity_degrades_to_summary_high() {
        let mut node = Node::new("branch");
        node.attrs.insert("fidelity".to_string(), str_attr("full"));
        let edge = Edge::new("fork", "branch");

        let resolved = resolve_parallel_branch_fidelity(&edge, &node, Fidelity::Compact);

        assert_eq!(resolved.requested, Some(Fidelity::Full));
        assert_eq!(resolved.effective, Some(Fidelity::SummaryHigh));
    }

    #[test]
    fn parallel_branch_fidelity_equal_to_fork_inherits() {
        let mut node = Node::new("branch");
        node.attrs
            .insert("fidelity".to_string(), str_attr("summary:high"));
        let edge = Edge::new("fork", "branch");

        let resolution = resolve_parallel_branch_fidelity(&edge, &node, Fidelity::SummaryHigh);

        assert_eq!(resolution.requested, Some(Fidelity::SummaryHigh));
        assert_eq!(resolution.effective, None);
    }

    #[test]
    fn explicit_full_branch_equal_to_degraded_fork_inherits() {
        let mut node = Node::new("branch");
        node.attrs.insert("fidelity".to_string(), str_attr("full"));
        let edge = Edge::new("fork", "branch");

        let resolution = resolve_parallel_branch_fidelity(&edge, &node, Fidelity::SummaryHigh);

        assert_eq!(resolution.requested, Some(Fidelity::Full));
        assert_eq!(resolution.effective, None);
    }

    #[test]
    fn full_fork_without_branch_fidelity_does_not_create_entry() {
        let node = Node::new("branch");
        let edge = Edge::new("fork", "branch");

        let resolution = resolve_parallel_branch_fidelity(&edge, &node, Fidelity::Full);

        assert_eq!(resolution.requested, None);
        assert_eq!(resolution.effective, None);
    }

    #[tokio::test]
    async fn parallel_before_node_rebuilds_branch_preamble_stash() {
        let graph = parallel_workflow_graph(None, Some("truncate"));
        let run_dir = tempfile::tempdir().unwrap();
        let lifecycle = test_lifecycle(&graph, run_dir.path()).await;
        let state: WfRunState = ExecutionState::new(&graph).unwrap();
        let fork = graph.get_node("fork").unwrap();

        lifecycle.before_node(&fork, &state).await.unwrap();
        state.context.set(
            keys::INTERNAL_PARALLEL_BRANCH_PREAMBLES,
            serde_json::json!(["stale", "entries", "must disappear"]),
        );
        lifecycle.before_node(&fork, &state).await.unwrap();

        let stash = state
            .context
            .get(keys::INTERNAL_PARALLEL_BRANCH_PREAMBLES)
            .expect("parallel stash should be set");
        let entries = stash.as_array().expect("parallel stash should be an array");
        assert_eq!(entries.len(), 2);
        assert!(entries[0].is_object());
        assert!(entries[1].is_null());
    }

    #[tokio::test]
    async fn non_parallel_before_node_overwrites_branch_preamble_stash_with_null() {
        let graph = parallel_workflow_graph(None, Some("truncate"));
        let run_dir = tempfile::tempdir().unwrap();
        let lifecycle = test_lifecycle(&graph, run_dir.path()).await;
        let state: WfRunState = ExecutionState::new(&graph).unwrap();
        let fork = graph.get_node("fork").unwrap();
        let work = graph.get_node("work").unwrap();

        lifecycle.before_node(&fork, &state).await.unwrap();
        lifecycle.before_node(&work, &state).await.unwrap();

        assert_eq!(
            state.context.get(keys::INTERNAL_PARALLEL_BRANCH_PREAMBLES),
            Some(serde_json::Value::Null)
        );
    }

    #[tokio::test]
    async fn resumed_full_fork_degrades_without_rendering_fallback_branches() {
        let graph = parallel_workflow_graph(Some("full"), None);
        let run_dir = tempfile::tempdir().unwrap();
        let lifecycle = test_lifecycle(&graph, run_dir.path()).await;
        lifecycle.set_degrade_fidelity_on_resume(true);
        let state: WfRunState = ExecutionState::new(&graph).unwrap();
        let fork = graph.get_node("fork").unwrap();

        lifecycle.before_node(&fork, &state).await.unwrap();

        assert_eq!(state.context.fidelity(), Fidelity::SummaryHigh);
        assert_eq!(
            state.context.get(keys::INTERNAL_PARALLEL_BRANCH_PREAMBLES),
            Some(serde_json::json!([null, null]))
        );
    }

    #[test]
    fn fidelity_defaults_to_compact() {
        let node = Node::new("work");
        let graph = Graph::new("test");
        assert_eq!(resolve_fidelity(None, &node, &graph), Fidelity::Compact);
    }

    #[test]
    fn fidelity_from_graph_default() {
        let node = Node::new("work");
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "default_fidelity".to_string(),
            AttrValue::String("truncate".to_string()),
        );
        assert_eq!(resolve_fidelity(None, &node, &graph), Fidelity::Truncate);
    }

    #[test]
    fn fidelity_from_node_overrides_graph() {
        let mut node = Node::new("work");
        node.attrs.insert(
            "fidelity".to_string(),
            AttrValue::String("full".to_string()),
        );
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "default_fidelity".to_string(),
            AttrValue::String("truncate".to_string()),
        );
        assert_eq!(resolve_fidelity(None, &node, &graph), Fidelity::Full);
    }

    #[test]
    fn fidelity_from_edge_overrides_node() {
        let mut node = Node::new("work");
        node.attrs.insert(
            "fidelity".to_string(),
            AttrValue::String("full".to_string()),
        );
        let mut edge = Edge::new("a", "work");
        edge.attrs.insert(
            "fidelity".to_string(),
            AttrValue::String("summary:high".to_string()),
        );
        let graph = Graph::new("test");
        assert_eq!(
            resolve_fidelity(Some(&edge), &node, &graph),
            Fidelity::SummaryHigh
        );
    }

    #[test]
    fn thread_id_from_node_attribute() {
        let mut node = Node::new("work");
        node.attrs.insert(
            "thread_id".to_string(),
            AttrValue::String("main-thread".to_string()),
        );
        let graph = Graph::new("test");
        assert_eq!(
            resolve_thread_id(None, &node, &graph, Some("prev")),
            Some("main-thread".to_string())
        );
    }

    #[test]
    fn thread_id_from_edge_attribute() {
        let node = Node::new("work");
        let mut edge = Edge::new("prev", "work");
        edge.attrs.insert(
            "thread_id".to_string(),
            AttrValue::String("edge-thread".to_string()),
        );
        let graph = Graph::new("test");
        assert_eq!(
            resolve_thread_id(Some(&edge), &node, &graph, Some("prev")),
            Some("edge-thread".to_string())
        );
    }

    #[test]
    fn thread_id_node_used_when_no_edge_thread() {
        let mut node = Node::new("work");
        node.attrs.insert(
            "thread_id".to_string(),
            AttrValue::String("node-thread".to_string()),
        );
        let edge = Edge::new("prev", "work");
        let graph = Graph::new("test");
        assert_eq!(
            resolve_thread_id(Some(&edge), &node, &graph, Some("prev")),
            Some("node-thread".to_string())
        );
    }

    #[test]
    fn thread_id_edge_overrides_node() {
        let mut node = Node::new("work");
        node.attrs.insert(
            "thread_id".to_string(),
            AttrValue::String("node-thread".to_string()),
        );
        let mut edge = Edge::new("prev", "work");
        edge.attrs.insert(
            "thread_id".to_string(),
            AttrValue::String("edge-thread".to_string()),
        );
        let graph = Graph::new("test");
        assert_eq!(
            resolve_thread_id(Some(&edge), &node, &graph, Some("prev")),
            Some("edge-thread".to_string()),
            "edge thread_id should override node thread_id"
        );
    }

    #[test]
    fn thread_id_from_graph_default_thread() {
        let node = Node::new("work");
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "default_thread".to_string(),
            AttrValue::String("shared-thread".to_string()),
        );
        assert_eq!(
            resolve_thread_id(None, &node, &graph, Some("prev")),
            Some("shared-thread".to_string())
        );
    }

    #[test]
    fn thread_id_edge_overrides_graph_default() {
        let node = Node::new("work");
        let mut edge = Edge::new("prev", "work");
        edge.attrs.insert(
            "thread_id".to_string(),
            AttrValue::String("edge-thread".to_string()),
        );
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "default_thread".to_string(),
            AttrValue::String("shared-thread".to_string()),
        );
        assert_eq!(
            resolve_thread_id(Some(&edge), &node, &graph, Some("prev")),
            Some("edge-thread".to_string())
        );
    }

    #[test]
    fn thread_id_graph_default_overrides_class() {
        let mut node = Node::new("work");
        node.classes = vec!["planning".to_string()];
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "default_thread".to_string(),
            AttrValue::String("shared-thread".to_string()),
        );
        assert_eq!(
            resolve_thread_id(None, &node, &graph, Some("prev")),
            Some("shared-thread".to_string())
        );
    }

    #[test]
    fn thread_id_from_node_class() {
        let mut node = Node::new("work");
        node.classes = vec!["planning".to_string(), "review".to_string()];
        let graph = Graph::new("test");
        assert_eq!(
            resolve_thread_id(None, &node, &graph, Some("prev")),
            Some("planning".to_string())
        );
    }

    #[test]
    fn thread_id_fallback_to_previous_node() {
        let node = Node::new("work");
        let graph = Graph::new("test");
        assert_eq!(
            resolve_thread_id(None, &node, &graph, Some("prev_node")),
            Some("prev_node".to_string())
        );
    }

    #[test]
    fn thread_id_none_when_no_sources() {
        let node = Node::new("start");
        let graph = Graph::new("test");
        assert_eq!(resolve_thread_id(None, &node, &graph, None), None);
    }
}
