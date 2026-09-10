use std::collections::HashMap;

use crate::{
    AuthMethod, BlobHash, Graph, IdpIdentity, Principal, RunProvenance, RunSpec, WorkflowSettings,
    WorkflowVersionId, fixtures,
};

#[must_use]
pub fn test_principal() -> Principal {
    Principal::user(
        IdpIdentity::new("fabro:test", "test-user").expect("test identity should be valid"),
        "test".to_string(),
        AuthMethod::DevToken,
    )
}

#[must_use]
pub fn test_run_provenance() -> RunProvenance {
    RunProvenance {
        server:  None,
        client:  None,
        subject: test_principal(),
    }
}

/// Neutral [`RunSpec`] for tests: a fixed run id, default settings, a minimal
/// `test` graph, and every optional field unset.
///
/// Spread it so a test only spells out the fields it actually asserts on:
///
/// ```
/// # use fabro_types::{RunSpec, test_support};
/// let spec = RunSpec {
///     workflow_slug: Some("release-flow".to_string()),
///     ..test_support::test_run_spec()
/// };
/// # assert_eq!(spec.workflow_slug.as_deref(), Some("release-flow"));
/// ```
#[must_use]
pub fn test_run_spec() -> RunSpec {
    RunSpec {
        run_id:              fixtures::RUN_1,
        settings:            WorkflowSettings::default(),
        graph:               Graph::new("test"),
        graph_source:        None,
        workflow_slug:       None,
        workflow_version_id: None,
        target:              None,
        automation:          None,
        source_directory:    None,
        labels:              HashMap::new(),
        provenance:          test_run_provenance(),
        manifest_blob:       None,
        definition_blob:     None,
        spec_blob:           None,
        git:                 None,
        fork_source_ref:     None,
    }
}

#[must_use]
pub fn test_workflow_version_id() -> WorkflowVersionId {
    WorkflowVersionId::from(BlobHash::new(b"workflow"))
}
