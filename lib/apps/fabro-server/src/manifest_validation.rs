use anyhow::Result;
use fabro_api::types;
use fabro_config::RunLayer;
use fabro_workflow::pipeline::TEMPLATE_UNDEFINED_VARIABLE_RULE;

use crate::run_manifest;

/// Validate manifest structure without server-owned catalogs.
///
/// Every caller is a client — the CLI, an MCP server, or a run worker.
/// Environment, MCP, model and provider availability are validated by the
/// receiving server on create, not by the client's unrelated local catalogs.
pub fn validate_manifest(
    manifest_run_defaults: &RunLayer,
    manifest: &types::RunManifest,
) -> Result<types::ValidateResponse> {
    let prepared = run_manifest::prepare_manifest_for_client(manifest_run_defaults, manifest)?;
    let validated = run_manifest::validate_prepared_manifest_structural(&prepared)
        .map_err(anyhow::Error::new)?;
    Ok(run_manifest::validate_response(&prepared, &validated))
}

pub fn promote_template_undefined_variables_to_errors(response: &mut types::ValidateResponse) {
    let mut promoted = false;
    for diagnostic in &mut response.workflow.diagnostics {
        if diagnostic.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE {
            diagnostic.severity = types::WorkflowDiagnosticSeverity::Error;
            promoted = true;
        }
    }
    if promoted {
        response.ok = false;
    }
}
