use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    DiffSummary, GitRunTarget, InterviewQuestionRecord, Principal, PullRequestLink, RepositoryRef,
    RunControlAction, RunId, RunSandbox, RunStatus, RunTiming,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskFabro {
    pub available:          bool,
    #[serde(default)]
    pub unavailable_reason: Option<AskFabroUnavailableReason>,
    #[serde(default)]
    pub default_model:      Option<String>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AskFabroUnavailableReason {
    NoSandbox,
    SandboxNotReady,
    LlmUnconfigured,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub id:               RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id:        Option<RunId>,
    #[serde(default)]
    pub children_count:   u64,
    pub title:            String,
    pub goal:             String,
    pub workflow:         WorkflowRef,
    #[serde(default)]
    pub automation:       Option<AutomationRef>,
    #[serde(default)]
    pub repository:       Option<RepositoryRef>,
    pub created_by:       Principal,
    pub origin:           RunOrigin,
    pub labels:           HashMap<String, String>,
    pub lifecycle:        RunLifecycle,
    #[serde(default)]
    pub sandbox:          Option<RunSandbox>,
    pub models:           Vec<RunModel>,
    #[serde(default)]
    pub source_directory: Option<String>,
    pub timestamps:       RunTimestamps,
    /// Run-level timing rollup. `None` until the run has measurable timing
    /// data; populated once a terminal event or partial rollup is available.
    #[serde(default)]
    pub timing:           Option<RunTiming>,
    #[serde(default)]
    pub billing:          Option<RunBillingSummary>,
    #[serde(default)]
    pub size:             RunSize,
    #[serde(default)]
    pub ask_fabro:        AskFabro,
    #[serde(default)]
    pub diff:             Option<DiffSummary>,
    #[serde(default)]
    pub pull_request:     Option<PullRequestLink>,
    #[serde(default)]
    pub current_question: Option<InterviewQuestionRecord>,
    #[serde(default)]
    pub superseded_by:    Option<RunId>,
    #[serde(default)]
    pub retried_from:     Option<RunId>,
    pub links:            RunLinks,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRef {
    #[serde(default)]
    pub slug:       Option<String>,
    #[serde(default)]
    pub name:       Option<String>,
    #[serde(default)]
    pub graph_name: Option<String>,
    /// Number of nodes in the workflow graph.
    #[serde(default)]
    pub node_count: i64,
    /// Number of edges in the workflow graph.
    #[serde(default)]
    pub edge_count: i64,
}

impl WorkflowRef {
    /// Best available human-facing workflow name: explicit name, then graph
    /// name, then slug.
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.name
            .as_deref()
            .or(self.graph_name.as_deref())
            .or(self.slug.as_deref())
    }
}

/// Requested workflow-source coordinate and the immutable commit selected for
/// one automation run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAutomationGitWorkflowSource {
    pub repo:         String,
    pub branch:       String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag:          Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha:          Option<String>,
    pub resolved_sha: String,
}

impl ResolvedAutomationGitWorkflowSource {
    #[must_use]
    pub fn from_requested(source: GitRunTarget, resolved_sha: String) -> Self {
        Self {
            repo: source.repo,
            branch: source.branch,
            tag: source.tag,
            sha: source.sha,
            resolved_sha,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomationRef {
    pub id:              String,
    #[serde(default)]
    pub name:            Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_id:      Option<String>,
    /// Boxed because this metadata is uncommon and run specs cross many async
    /// server boundaries where their inline size matters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_source: Option<Box<ResolvedAutomationGitWorkflowSource>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOrigin {
    pub kind: RunOriginKind,
}

impl Default for RunOrigin {
    fn default() -> Self {
        Self {
            kind: RunOriginKind::Api,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOriginKind {
    Api,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunModel {
    #[serde(default)]
    pub provider: Option<String>,
    pub name:     String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLifecycle {
    pub status:          RunStatus,
    #[serde(default)]
    pub approval:        Option<RunApproval>,
    #[serde(default)]
    pub pending_control: Option<RunControlAction>,
    #[serde(default)]
    pub queue_position:  Option<u32>,
    #[serde(default)]
    pub error:           Option<RunError>,
    pub archived:        bool,
    #[serde(default)]
    pub archived_at:     Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunApproval {
    pub state:         RunApprovalState,
    pub requested_at:  DateTime<Utc>,
    #[serde(default)]
    pub decided_at:    Option<DateTime<Utc>>,
    #[serde(default)]
    pub denial_reason: Option<String>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RunApprovalState {
    Pending,
    Approved,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunError {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunTimestamps {
    pub created_at:    DateTime<Utc>,
    #[serde(default)]
    pub started_at:    Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at:  Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunBillingSummary {
    #[serde(default)]
    pub total_usd_micros: Option<i64>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
pub enum RunSize {
    #[default]
    Xs,
    S,
    M,
    L,
    Xl,
}

impl RunSize {
    /// Inclusive upper bounds in USD micros for each bucket below [`Self::Xl`],
    /// ordered smallest to largest. Shared with the SQLite size sort so both
    /// stay in step.
    pub const BUCKET_MAX_USD_MICROS: [(Self, i64); 4] = [
        (Self::Xs, 20_000_000),
        (Self::S, 50_000_000),
        (Self::M, 100_000_000),
        (Self::L, 200_000_000),
    ];

    #[must_use]
    pub fn from_total_usd_micros(total_usd_micros: Option<i64>) -> Self {
        let total = total_usd_micros.unwrap_or(0);
        Self::BUCKET_MAX_USD_MICROS
            .iter()
            .find(|(_, max)| total <= *max)
            .map_or(Self::Xl, |(size, _)| *size)
    }
}

#[cfg(test)]
mod tests {
    use super::RunSize;

    #[test]
    fn run_size_uses_billed_usage_thresholds() {
        assert_eq!(RunSize::from_total_usd_micros(None), RunSize::Xs);
        assert_eq!(
            RunSize::from_total_usd_micros(Some(20_000_000)),
            RunSize::Xs
        );
        assert_eq!(RunSize::from_total_usd_micros(Some(20_000_001)), RunSize::S);
        assert_eq!(RunSize::from_total_usd_micros(Some(50_000_000)), RunSize::S);
        assert_eq!(RunSize::from_total_usd_micros(Some(50_000_001)), RunSize::M);
        assert_eq!(
            RunSize::from_total_usd_micros(Some(100_000_000)),
            RunSize::M
        );
        assert_eq!(
            RunSize::from_total_usd_micros(Some(100_000_001)),
            RunSize::L
        );
        assert_eq!(
            RunSize::from_total_usd_micros(Some(200_000_000)),
            RunSize::L
        );
        assert_eq!(
            RunSize::from_total_usd_micros(Some(200_000_001)),
            RunSize::Xl
        );
    }

    #[test]
    fn run_size_serializes_as_uppercase_string() {
        assert_eq!(serde_json::to_value(RunSize::Xs).unwrap(), "XS");
        assert_eq!(serde_json::to_value(RunSize::Xl).unwrap(), "XL");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLinks {
    #[serde(default)]
    pub web: Option<String>,
}
