use std::collections::HashMap;

use fabro_types::ResolvedOnFailure;

use crate::context::Context;
use crate::error::Result;
use crate::outcome::{NodeResult, Outcome, OutcomeMeta};

/// How edge selection chose an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum EdgeSelectionReason {
    Condition,
    PreferredLabel,
    SuggestedNext,
    Unconditional,
}

impl EdgeSelectionReason {
    /// Returns whether this selection is an explicit route supplied by the
    /// node result or an edge condition.
    #[must_use]
    pub const fn is_explicit(self) -> bool {
        !matches!(self, Self::Unconditional)
    }
}

pub trait NodeSpec: Send + Sync + Clone {
    fn id(&self) -> &str;
    fn is_terminal(&self) -> bool;
    fn max_visits(&self) -> Option<usize>;
}

pub trait EdgeSpec: Send + Sync + Clone {
    fn target(&self) -> &str;
    fn label(&self) -> Option<&str>;
    fn is_loop_restart(&self) -> bool;
}

pub struct EdgeSelection<G: Graph + ?Sized> {
    pub edge:   G::Edge,
    pub reason: EdgeSelectionReason,
}

pub trait Graph: Send + Sync {
    type Node: NodeSpec + Clone;
    type Edge: EdgeSpec + Clone;
    type Meta: OutcomeMeta;

    fn get_node(&self, id: &str) -> Option<Self::Node>;
    fn find_start_node(&self) -> Result<Self::Node>;
    fn outgoing_edges(&self, node_id: &str) -> Vec<Self::Edge>;
    fn select_edge(
        &self,
        node: &Self::Node,
        outcome: &Outcome<Self::Meta>,
        context: &Context,
    ) -> Option<EdgeSelection<Self>>;
    /// Projects derived values from a pending node result into a context used
    /// to test edge conditions before the result is durably recorded.
    ///
    /// Implementations can add the same derived values that their lifecycle
    /// writes after recording. The executor applies `context_updates` before
    /// this method runs.
    fn project_result_context(
        &self,
        _node: &Self::Node,
        _result: &NodeResult<Self::Meta>,
        _context: &Context,
    ) {
    }
    fn check_goal_gates(
        &self,
        outcomes: &HashMap<String, Outcome<Self::Meta>>,
    ) -> std::result::Result<(), String>;
    fn get_retry_target(&self, failed_node_id: &str) -> Option<String>;
    /// Effective failure routing policy for a node: node-level `on_failure`
    /// overrides the graph level, and an absent node attribute inherits it.
    fn resolve_on_failure(&self, node: &Self::Node) -> ResolvedOnFailure;
}
