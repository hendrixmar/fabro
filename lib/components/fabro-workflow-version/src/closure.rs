use fabro_types::WorkflowVersionId;

use crate::ValidatedWorkflowVersion;

/// Validated workflow versions in dependency-first order, with the root last.
/// Owns source contents; consumers borrow versions instead of cloning them.
#[derive(Debug)]
pub struct CollectedWorkflowClosure {
    root_id:  WorkflowVersionId,
    versions: Vec<(WorkflowVersionId, ValidatedWorkflowVersion)>,
}

impl CollectedWorkflowClosure {
    /// Assemble the result of a collector that has already ordered and
    /// validated the dependency graph. The caller supplies matching IDs,
    /// unique versions, and dependencies before parents, with `root_id`
    /// identifying the last entry. This preserves the collector's ordering
    /// without traversing or hashing again.
    #[must_use]
    pub fn from_dependency_order(
        root_id: WorkflowVersionId,
        versions: Vec<(WorkflowVersionId, ValidatedWorkflowVersion)>,
    ) -> Self {
        Self { root_id, versions }
    }

    #[must_use]
    pub fn root_id(&self) -> WorkflowVersionId {
        self.root_id
    }

    /// Iterate over every version with dependencies before parents.
    pub fn versions(
        &self,
    ) -> impl Iterator<Item = (WorkflowVersionId, &ValidatedWorkflowVersion)> + '_ {
        self.versions.iter().map(|(id, version)| (*id, version))
    }
}
