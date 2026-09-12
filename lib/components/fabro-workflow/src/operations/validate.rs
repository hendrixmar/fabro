use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use fabro_llm::lithos_catalog::Catalog;
use fabro_types::WorkflowSettings;
use lithos_llm::catalog::ProviderId;

use super::create::{configured_default_provider, preprocess_and_validate, template_context};
use super::source::{ResolveWorkflowInput, WorkflowInput, resolve_workflow};
use crate::error::Error;
use crate::operations::RenderMode;
use crate::pipeline::{TransformOptions, Validated};
use crate::transforms::{ModelResolutionTransform, Transform};

pub struct ValidateInput {
    pub workflow:          WorkflowInput,
    pub settings:          WorkflowSettings,
    /// Run-scoped variables (`{{ vars.* }}`) available to prompts and goals.
    /// Empty for offline/CLI validation.
    pub vars:              HashMap<String, String>,
    pub cwd:               PathBuf,
    pub custom_transforms: Vec<Box<dyn Transform>>,
}

/// Parse, transform, and structurally validate a DOT source string without a
/// model catalog. Model and provider availability is left to the caller that
/// owns a catalog — typically the server.
///
/// Returns `Validated` even when validation produced errors. Call
/// `validated.raise_on_errors()` if the caller wants to fail fast.
pub fn validate(input: ValidateInput) -> Result<Validated, Error> {
    validate_resolving_models(input, None)
}

/// Parse, transform, and validate a DOT source string against `catalog`.
pub fn validate_with_catalog(
    input: ValidateInput,
    catalog: Arc<Catalog>,
) -> Result<Validated, Error> {
    validate_resolving_models(input, Some(ModelResolutionTransform::new(catalog)))
}

/// Parse, transform, and validate, resolving models against the ready
/// providers first and falling back to the full catalog only for
/// provider-readiness selection failures.
pub fn validate_with_ready_providers(
    input: ValidateInput,
    catalog: Arc<Catalog>,
    ready_providers: &[ProviderId],
) -> Result<Validated, Error> {
    validate_resolving_models(
        input,
        Some(
            ModelResolutionTransform::for_eligible(
                catalog,
                ready_providers.iter().cloned().collect(),
            )
            .with_catalog_fallback(true),
        ),
    )
}

/// The workflow's own default provider is only known once the workflow is
/// resolved, so callers hand in a partially built transform and it is
/// completed here.
fn validate_resolving_models(
    input: ValidateInput,
    model_resolution: Option<ModelResolutionTransform>,
) -> Result<Validated, Error> {
    let ValidateInput {
        workflow,
        settings,
        vars,
        cwd,
        custom_transforms,
    } = input;
    let resolved = resolve_workflow(ResolveWorkflowInput {
        workflow,
        settings,
        cwd,
    })
    .map_err(|err| Error::Parse(err.to_string()))?;

    let model_resolution = model_resolution.map(|resolution| {
        resolution.with_default_provider(configured_default_provider(&resolved.settings))
    });

    preprocess_and_validate(
        &resolved.raw_source,
        resolved.goal_override.as_deref(),
        &TransformOptions {
            current_dir: resolved.current_dir,
            file_resolver: resolved.file_resolver,
            template_context: template_context(Some(&resolved.settings), vars),
            source_name: resolved
                .dot_path
                .as_ref()
                .map(|path| path.display().to_string()),
            render_mode: RenderMode::Structural,
            custom_transforms,
            model_resolution,
        },
    )
}
