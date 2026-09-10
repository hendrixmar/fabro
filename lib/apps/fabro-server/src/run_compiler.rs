//! The create-time run compiler: the single pipeline that turns an acquired
//! workflow bundle into a complete, persistable run.
//!
//! The pipeline has four stages, each its own function with typed input and
//! output:
//!
//! 1. [`normalize_source`] — resolve the bundle entrypoint and parse the
//!    bundle-relative settings sources (workflow and project layers, with
//!    dockerfile references inlined from bundled files).
//! 2. [`layer_settings`] + [`apply_run_variables`] + graph compilation — layer
//!    settings from every configured source, substitute the run-scoped variable
//!    snapshot, then parse/transform/validate the graph through the
//!    fabro-workflow pipeline.
//! 3. Model pinning — materialize run-level model settings against the catalog
//!    and the configured provider set. Stages 2's graph compilation and stage 3
//!    share one blocking dispatch via [`compile_and_pin`].
//! 4. [`assemble_run`] — purely assemble the complete persistence input; no
//!    field is mutated after assembly.
//!
//! The input is deliberately source-neutral: it speaks in terms of an
//! acquired [`WorkflowBundle`], not any wire request type, so non-HTTP
//! callers and alternative workflow sources can drive the same pipeline.
//! Callers own source acquisition, run-id resolution, variable snapshotting,
//! and (for HTTP callers) all wire mapping — including turning
//! [`RunCompilerError`] into HTTP responses.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use fabro_config::parse::{self, ParseError, SettingsSource};
use fabro_config::{
    CliLayer, EnvironmentDockerfileLayer, EnvironmentImageLayer, EnvironmentLayer, MergeMap,
    RunLayer, SettingsLayer, WorkflowSettingsBuilder,
};
use fabro_model::{Catalog, ProviderId};
use fabro_types::settings::interp::{InterpString, ResolveError};
use fabro_types::settings::run::{McpServerSettings, RunGoal};
use fabro_types::{
    AutomationRef, GitContext, ManifestPath, RunId, RunProvenance, RunTarget, WorkflowSettings,
    WorkflowVersionId,
};
use fabro_util::workspace_glob::{WorkspaceGlob, WorkspaceGlobError};
use fabro_workflow::Error as WorkflowError;
use fabro_workflow::operations::{
    self, CompiledRun, CreateRunCompileInput, CreateRunPersistenceInput,
    CreateRunPersistenceMetadata, MaterializedRun, WorkflowInput,
};
use fabro_workflow::workflow_bundle::{BundledWorkflow, WorkflowBundle};
use tokio::task;

/// One project settings source in the acquired source's path namespace.
#[derive(Debug)]
pub(crate) struct ProjectSettingsSource {
    pub(crate) path: std::result::Result<ManifestPath, ProjectSettingsPathError>,
    pub(crate) toml: String,
}

/// A project settings path that the source adapter could not normalize.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProjectSettingsPathError {
    #[error("project settings path is missing")]
    Missing,

    #[error("invalid project settings path: {path}")]
    Invalid { path: String },
}

/// Transport-neutral inputs for compiling one submitted run.
///
/// Identity (`run_id`), lineage, title, git metadata, and provenance are
/// resolved by the caller. This boundary owns only source normalization,
/// settings resolution, workflow compilation, model pinning, and
/// persistence-input assembly.
#[derive(Debug)]
pub(crate) struct RawRunCompilerInput {
    pub(crate) workflow_bundle: WorkflowBundle,
    pub(crate) entrypoint: ManifestPath,
    pub(crate) cwd: PathBuf,
    pub(crate) server_run_defaults: RunLayer,
    pub(crate) server_environment_defaults: MergeMap<EnvironmentLayer>,
    pub(crate) server_mcp_catalog: HashMap<String, McpServerSettings>,
    pub(crate) settings_input: RunCompilerSettingsInput,
    pub(crate) user_toml: Vec<String>,
    pub(crate) run_overrides: Option<RunLayer>,
    pub(crate) cli_overrides: Option<CliLayer>,
    pub(crate) input_overrides: HashMap<String, toml::Value>,
    pub(crate) inline_goal_override: Option<String>,
    pub(crate) run_id: Option<RunId>,
    pub(crate) title: Option<String>,
    pub(crate) parent_id: Option<RunId>,
    pub(crate) git: Option<GitContext>,
    pub(crate) storage_root: PathBuf,
    pub(crate) workflow_slug: Option<String>,
    pub(crate) workflow_version_id: Option<WorkflowVersionId>,
    pub(crate) target: Option<RunTarget>,
    pub(crate) provenance: RunProvenance,
    pub(crate) web_url: Option<String>,
    pub(crate) submitted_manifest_bytes: Option<Vec<u8>>,
    pub(crate) automation: Option<AutomationRef>,
}

/// Settings already admitted by the caller, or the unchanged legacy manifest
/// inputs that still need their historical parsing and lookup behavior.
#[derive(Debug)]
pub(crate) enum RunCompilerSettingsInput {
    LegacyManifest {
        project_settings: Vec<ProjectSettingsSource>,
    },
    Admitted {
        workflow_layer: Option<Box<SettingsLayer>>,
    },
}

/// Stage-one output: the selected bundled workflow and all client settings
/// sources have been parsed and normalized, but no settings have been layered.
pub(crate) struct NormalizedRun {
    workflow_bundle: WorkflowBundle,
    entrypoint: ManifestPath,
    workflow: BundledWorkflow,
    workflow_layer: Option<SettingsLayer>,
    project_layers: Vec<SettingsLayer>,
    user_toml: Vec<String>,
    cwd: PathBuf,
    server_run_defaults: RunLayer,
    server_environment_defaults: MergeMap<EnvironmentLayer>,
    server_mcp_catalog: HashMap<String, McpServerSettings>,
    run_overrides: Option<RunLayer>,
    cli_overrides: Option<CliLayer>,
    input_overrides: HashMap<String, toml::Value>,
    inline_goal_override: Option<String>,
    metadata: RunMetadata,
}

struct RunMetadata {
    run_id: Option<RunId>,
    storage_root: PathBuf,
    workflow_slug: Option<String>,
    workflow_version_id: Option<WorkflowVersionId>,
    target: Option<RunTarget>,
    submitted_manifest_bytes: Option<Vec<u8>>,
    title: Option<String>,
    automation: Option<AutomationRef>,
    git: Option<GitContext>,
    parent_id: Option<RunId>,
    provenance: RunProvenance,
    web_url: Option<String>,
}

/// Settings-layered output. Variable substitution is a separate stage so
/// callers can snapshot run variables after settings resolution and apply
/// the snapshot through [`apply_run_variables`].
pub(crate) struct LayeredRun {
    workflow_bundle: WorkflowBundle,
    entrypoint:      ManifestPath,
    workflow:        BundledWorkflow,
    settings:        WorkflowSettings,
    cwd:             PathBuf,
    metadata:        RunMetadata,
}

/// Variable-substituted stage output. Callers may inspect the resolved
/// settings before policy checks, then move it into [`compile_and_pin`].
pub(crate) struct PreparedRun {
    layered: LayeredRun,
    vars:    HashMap<String, String>,
}

impl PreparedRun {
    pub(crate) fn settings(&self) -> &WorkflowSettings {
        &self.layered.settings
    }

    pub(crate) fn with_target_and_git(
        mut self,
        target: RunTarget,
        git: Option<GitContext>,
    ) -> Self {
        self.layered.metadata.target = Some(target);
        self.layered.metadata.git = git;
        self
    }

    pub(crate) fn with_identity(
        mut self,
        run_id: Option<RunId>,
        parent_id: Option<RunId>,
        title: Option<String>,
    ) -> Self {
        self.layered.metadata.run_id = run_id;
        self.layered.metadata.parent_id = parent_id;
        self.layered.metadata.title = title;
        self
    }

    pub(crate) fn parent_id(&self) -> Option<RunId> {
        self.layered.metadata.parent_id
    }

    pub(crate) fn resolve_run_id(mut self) -> (Self, RunId) {
        let run_id = self.layered.metadata.run_id.unwrap_or_default();
        self.layered.metadata.run_id = Some(run_id);
        (self, run_id)
    }

    pub(crate) fn with_web_url(mut self, web_url: Option<String>) -> Self {
        self.layered.metadata.web_url = web_url;
        self
    }
}

/// Graph-compiled stage output, retaining the metadata needed by later pure
/// assembly.
struct GraphCompiledRun {
    compiled: CompiledRun,
    metadata: RunMetadata,
}

/// Model-pinned stage output ready for pure persistence-input assembly.
pub(crate) struct PinnedRun {
    materialized: MaterializedRun,
    metadata:     RunMetadata,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RunCompilerError {
    /// The acquired source bundle is invalid: missing entrypoint or broken
    /// bundled-file references.
    #[error(transparent)]
    InvalidSource(#[from] InvalidSourceError),

    /// A settings source or path is invalid, or the layered settings failed to
    /// resolve.
    #[error(transparent)]
    InvalidSettings(Box<InvalidSettingsError>),

    /// The run-variable snapshot could not be substituted into the resolved
    /// run settings.
    #[error("Run config variable interpolation failed: {0}")]
    VariableInterpolation(#[from] VariableInterpolationError),

    /// Graph compilation or model pinning failed in the workflow engine. The
    /// full [`WorkflowError`] is preserved so callers can distinguish
    /// validation, parse, and model-selection failures.
    #[error(transparent)]
    Workflow(#[from] WorkflowError),
}

// The `Display` strings below are pinned to the pre-extraction wire
// contract: both the create handler and the manifest preparation path render
// them directly into HTTP 400 details.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidSourceError {
    #[error("manifest target path is missing from workflows map")]
    MissingEntrypoint { entrypoint: ManifestPath },

    #[error("unsupported dockerfile reference: {reference}")]
    UnsupportedDockerfileReference {
        config_path: ManifestPath,
        reference:   String,
    },

    #[error("missing bundled dockerfile: {dockerfile_path}")]
    MissingDockerfile {
        config_path:     ManifestPath,
        dockerfile_path: ManifestPath,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidSettingsError {
    #[error("Failed to parse run config TOML")]
    Parse {
        path:   ManifestPath,
        #[source]
        source: ParseError,
    },

    #[error(transparent)]
    User(fabro_config::Error),

    #[error("failed to resolve manifest settings")]
    Resolve {
        #[source]
        source: fabro_config::ResolveErrors,
    },

    #[error("{}", project_path_error(.source))]
    ProjectPath { source: ProjectSettingsPathError },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VariableInterpolationError {
    #[error(transparent)]
    Interpolation(#[from] ResolveError),

    #[error("run.artifacts.include[{index}]: {source}")]
    ArtifactGlob {
        index:  usize,
        #[source]
        source: WorkspaceGlobError,
    },
}

pub(crate) type Result<T> = std::result::Result<T, RunCompilerError>;

fn project_path_error(source: &ProjectSettingsPathError) -> String {
    match source {
        ProjectSettingsPathError::Missing => {
            "invalid manifest project config path: missing path".to_string()
        }
        ProjectSettingsPathError::Invalid { path } => {
            format!("invalid manifest project config path: {path}")
        }
    }
}

fn invalid_settings(source: InvalidSettingsError) -> RunCompilerError {
    RunCompilerError::InvalidSettings(Box::new(source))
}

/// Normalize the bundle entrypoint and parse workflow/project settings while
/// resolving dockerfile references against the selected workflow's files.
pub(crate) fn normalize_source(input: RawRunCompilerInput) -> Result<NormalizedRun> {
    let RawRunCompilerInput {
        workflow_bundle,
        entrypoint,
        cwd,
        server_run_defaults,
        server_environment_defaults,
        server_mcp_catalog,
        settings_input,
        user_toml,
        run_overrides,
        cli_overrides,
        input_overrides,
        inline_goal_override,
        run_id,
        title,
        parent_id,
        git,
        storage_root,
        workflow_slug,
        workflow_version_id,
        target,
        provenance,
        web_url,
        submitted_manifest_bytes,
        automation,
    } = input;
    let mut workflow = workflow_bundle
        .workflow(&entrypoint)
        .cloned()
        .ok_or_else(|| InvalidSourceError::MissingEntrypoint {
            entrypoint: entrypoint.clone(),
        })?;
    workflow.path = entrypoint.clone();

    let (workflow_layer, project_layers) = match settings_input {
        RunCompilerSettingsInput::LegacyManifest { project_settings } => {
            let workflow_layer = workflow
                .config
                .as_ref()
                .map(|config| {
                    settings_layer_with_resolved_dockerfiles(
                        &config.source,
                        &config.path,
                        &workflow.files,
                        SettingsSource::Workflow,
                    )
                })
                .transpose()?;
            let project_layers = project_settings
                .into_iter()
                .map(|project| {
                    let path = project.path.map_err(|source| {
                        invalid_settings(InvalidSettingsError::ProjectPath { source })
                    })?;
                    settings_layer_with_resolved_dockerfiles(
                        &project.toml,
                        &path,
                        &workflow.files,
                        SettingsSource::Project,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            (workflow_layer, project_layers)
        }
        RunCompilerSettingsInput::Admitted { workflow_layer } => {
            (workflow_layer.map(|layer| *layer), Vec::new())
        }
    };

    Ok(NormalizedRun {
        workflow_bundle,
        entrypoint,
        workflow,
        workflow_layer,
        project_layers,
        user_toml,
        cwd,
        server_run_defaults,
        server_environment_defaults,
        server_mcp_catalog,
        run_overrides,
        cli_overrides,
        input_overrides,
        inline_goal_override,
        metadata: RunMetadata {
            run_id,
            storage_root,
            workflow_slug,
            workflow_version_id,
            target,
            submitted_manifest_bytes,
            title,
            automation,
            git,
            parent_id,
            provenance,
            web_url,
        },
    })
}

/// Layer settings from every configured source and apply the submitted input
/// and goal overrides.
pub(crate) fn layer_settings(normalized: NormalizedRun) -> Result<LayeredRun> {
    let NormalizedRun {
        workflow_bundle,
        entrypoint,
        workflow,
        workflow_layer,
        project_layers,
        user_toml,
        cwd,
        server_run_defaults,
        server_environment_defaults,
        server_mcp_catalog,
        run_overrides,
        cli_overrides,
        input_overrides,
        inline_goal_override,
        metadata,
    } = normalized;
    let mut builder = WorkflowSettingsBuilder::new()
        .server_manifest_defaults(server_run_defaults, server_environment_defaults)
        .server_mcp_catalog(server_mcp_catalog);
    if let Some(run) = run_overrides {
        builder = builder.run_overrides(run);
    }
    if let Some(cli) = cli_overrides {
        builder = builder.cli_overrides(cli);
    }
    if let Some(layer) = workflow_layer {
        builder = builder.workflow_layer(layer);
    }
    for layer in project_layers {
        builder = builder.project_layer(layer);
    }
    for source in user_toml {
        builder = builder
            .user_toml(&source)
            .map_err(|source| invalid_settings(InvalidSettingsError::User(source)))?;
    }
    let mut settings = builder
        .build()
        .map_err(|source| invalid_settings(InvalidSettingsError::Resolve { source }))?;
    settings.run.inputs.extend(input_overrides);
    if let Some(goal) = inline_goal_override {
        settings.run.goal = Some(RunGoal::Inline(InterpString::parse(&goal)));
    }

    Ok(LayeredRun {
        workflow_bundle,
        entrypoint,
        workflow,
        settings,
        cwd,
        metadata,
    })
}

/// Apply a run-variable snapshot to the layered settings and validate the
/// resulting artifact globs. The snapshot is also retained for graph template
/// rendering during compilation.
pub(crate) fn apply_run_variables(
    mut layered: LayeredRun,
    vars: HashMap<String, String>,
) -> Result<PreparedRun> {
    substitute_run_variables(&vars, &mut layered.settings)?;
    Ok(PreparedRun { layered, vars })
}

/// Compile and validate the graph, then pin run-level model settings, in one
/// dispatch on Tokio's blocking pool: graph compilation is CPU-heavy and may
/// read a goal file, and pinning is pure CPU that belongs alongside it.
pub(crate) async fn compile_and_pin(
    prepared: PreparedRun,
    configured_providers: Vec<ProviderId>,
    catalog: Arc<Catalog>,
) -> Result<PinnedRun> {
    task::spawn_blocking(move || {
        let compiled = compile_graph(prepared, configured_providers, Arc::clone(&catalog))?;
        pin_models(compiled, &catalog)
    })
    .await
    .map_err(|source| {
        RunCompilerError::Workflow(WorkflowError::engine_with_source(
            "workflow create task failed",
            source,
        ))
    })?
}

/// Stage two's graph compilation: parse, transform, and validate through the
/// fabro-workflow pipeline, with undefined template variables promoted to
/// hard errors.
fn compile_graph(
    prepared: PreparedRun,
    configured_providers: Vec<ProviderId>,
    catalog: Arc<Catalog>,
) -> Result<GraphCompiledRun> {
    let PreparedRun {
        layered:
            LayeredRun {
                workflow_bundle,
                entrypoint,
                workflow,
                settings,
                cwd,
                metadata,
            },
        vars,
    } = prepared;
    let compiled = operations::compile_create_run(
        CreateRunCompileInput {
            workflow: WorkflowInput::Bundled(workflow),
            settings,
            vars,
            cwd,
            workflow_path: Some(entrypoint),
            workflow_bundle: Some(workflow_bundle),
            configured_providers,
        },
        catalog,
    )?;

    Ok(GraphCompiledRun { compiled, metadata })
}

/// Stage three: pin concrete model and provider selections against the
/// catalog and the configured provider set.
fn pin_models(compiled: GraphCompiledRun, catalog: &Catalog) -> Result<PinnedRun> {
    let GraphCompiledRun { compiled, metadata } = compiled;
    let materialized = operations::materialize_create_run(compiled, catalog)?;
    Ok(PinnedRun {
        materialized,
        metadata,
    })
}

/// Stage four: purely assemble the complete persistence input. Every durable
/// field — run id, submitted source bytes, automation reference — is set here
/// once; nothing mutates the result afterwards.
pub(crate) fn assemble_run(pinned: PinnedRun) -> CreateRunPersistenceInput {
    let PinnedRun {
        materialized,
        metadata,
    } = pinned;
    let RunMetadata {
        run_id,
        storage_root,
        workflow_slug,
        workflow_version_id,
        target,
        submitted_manifest_bytes,
        title,
        automation,
        git,
        parent_id,
        provenance,
        web_url,
    } = metadata;
    operations::assemble_create_run_persistence_input(materialized, CreateRunPersistenceMetadata {
        run_id: run_id.expect("run ID should be resolved before compilation"),
        storage_root,
        workflow_slug,
        workflow_version_id,
        target,
        submitted_manifest_bytes,
        title,
        automation,
        git,
        fork_source_ref: None,
        parent_id,
        provenance,
        web_url,
    })
}

/// Parse one bundle-relative settings source, rejecting keys that are not
/// allowed for `settings_source` and inlining dockerfile references from the
/// bundled files.
///
/// Parses via [`SettingsLayer`] so unknown nested keys (like a stale
/// `[server.integrations.github.permissions]` after the move to
/// `[run.integrations.github.permissions]`) trip `deny_unknown_fields`.
pub(crate) fn settings_layer_with_resolved_dockerfiles(
    source: &str,
    config_path: &ManifestPath,
    files: &HashMap<ManifestPath, String>,
    settings_source: SettingsSource,
) -> Result<SettingsLayer> {
    let parse_error = |source| {
        invalid_settings(InvalidSettingsError::Parse {
            path: config_path.clone(),
            source,
        })
    };
    let mut layer = source.parse::<SettingsLayer>().map_err(parse_error)?;
    parse::validate_settings_source(&layer, settings_source).map_err(parse_error)?;
    resolve_dockerfiles(&mut layer, config_path, files)?;
    Ok(layer)
}

fn resolve_dockerfiles(
    layer: &mut SettingsLayer,
    config_path: &ManifestPath,
    files: &HashMap<ManifestPath, String>,
) -> Result<()> {
    for image in layer.environment_images_mut() {
        resolve_dockerfile(image, config_path, files)?;
    }
    Ok(())
}

fn resolve_dockerfile(
    image: &mut EnvironmentImageLayer,
    config_path: &ManifestPath,
    files: &HashMap<ManifestPath, String>,
) -> Result<()> {
    let Some(EnvironmentDockerfileLayer::Path { path }) = image.dockerfile.as_ref() else {
        return Ok(());
    };
    let reference = path.clone();
    let dockerfile_path = ManifestPath::from_reference(config_path.parent_or_dot(), &reference)
        .ok_or_else(|| InvalidSourceError::UnsupportedDockerfileReference {
            config_path: config_path.clone(),
            reference:   reference.clone(),
        })?;
    let content = files.get(&dockerfile_path).cloned().ok_or_else(|| {
        InvalidSourceError::MissingDockerfile {
            config_path:     config_path.clone(),
            dockerfile_path: dockerfile_path.clone(),
        }
    })?;
    image.dockerfile = Some(EnvironmentDockerfileLayer::Inline(content));
    Ok(())
}

/// Substitute run-scoped variables into the resolved run settings, then
/// re-validate the artifact-include globs: a substituted variable can make a
/// previously-safe glob unsafe.
pub(crate) fn substitute_run_variables(
    variables: &HashMap<String, String>,
    settings: &mut WorkflowSettings,
) -> std::result::Result<(), VariableInterpolationError> {
    settings
        .run
        .substitute_variables(|name| variables.get(name).cloned())?;
    for (index, pattern) in settings.run.artifacts.include.iter().enumerate() {
        WorkspaceGlob::try_new(pattern)
            .map_err(|source| VariableInterpolationError::ArtifactGlob { index, source })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error as _;

    use fabro_config::EnvironmentDockerfileLayer;
    use fabro_graphviz::graph::AttrValue;
    use fabro_model::Catalog;
    use fabro_types::settings::interp::ResolveCtx;
    use fabro_types::settings::run::RunGoal;
    use fabro_types::{AutomationRef, Principal, RunProvenance, SystemActorKind};
    use fabro_workflow::workflow_bundle::ParsedWorkflowConfig;

    use super::*;

    const DOT: &str = r#"digraph Test {
        graph [goal="Graph goal"]
        start [shape=Mdiamond]
        work [prompt="Ship {{ inputs.target }} for {{ vars.owner }}", model="gpt-5.4"]
        exit [shape=Msquare]
        start -> work -> exit
    }"#;

    fn manifest_path(value: &str) -> ManifestPath {
        ManifestPath::from_wire(value).expect("fixture manifest path should be valid")
    }

    fn provenance() -> RunProvenance {
        RunProvenance {
            server:  None,
            client:  None,
            subject: Principal::System {
                system_kind: SystemActorKind::Engine,
            },
        }
    }

    fn workflow(
        entrypoint: &ManifestPath,
        workflow_toml: Option<&str>,
        files: HashMap<ManifestPath, String>,
    ) -> BundledWorkflow {
        BundledWorkflow {
            path: entrypoint.clone(),
            source: DOT.to_string(),
            config: workflow_toml.map(|source| ParsedWorkflowConfig {
                path:   manifest_path("flows/workflow.toml"),
                source: source.to_string(),
            }),
            files,
        }
    }

    fn raw_input(
        workflow_toml: Option<&str>,
        files: HashMap<ManifestPath, String>,
    ) -> RawRunCompilerInput {
        let entrypoint = manifest_path("flows/workflow.fabro");
        let workflow = workflow(&entrypoint, workflow_toml, files);
        RawRunCompilerInput {
            workflow_bundle: WorkflowBundle::new(HashMap::from([(entrypoint.clone(), workflow)])),
            entrypoint,
            cwd: PathBuf::from("/workspace"),
            server_run_defaults: RunLayer::default(),
            server_environment_defaults: fabro_environment::seeded_catalog_layer(),
            server_mcp_catalog: HashMap::new(),
            settings_input: RunCompilerSettingsInput::LegacyManifest {
                project_settings: Vec::new(),
            },
            user_toml: Vec::new(),
            run_overrides: None,
            cli_overrides: None,
            input_overrides: HashMap::new(),
            inline_goal_override: None,
            run_id: Some(RunId::new()),
            title: None,
            parent_id: None,
            git: None,
            storage_root: PathBuf::from("/tmp/fabro-storage"),
            workflow_slug: None,
            workflow_version_id: None,
            target: None,
            provenance: provenance(),
            web_url: None,
            submitted_manifest_bytes: None,
            automation: None,
        }
    }

    fn test_provider_ids() -> Vec<ProviderId> {
        Catalog::builtin().all_provider_ids().into_iter().collect()
    }

    fn prepare_run(
        input: RawRunCompilerInput,
        vars: HashMap<String, String>,
    ) -> Result<PreparedRun> {
        apply_run_variables(layer_settings(normalize_source(input)?)?, vars)
    }

    #[test]
    fn normalize_source_rejects_missing_entrypoint() {
        let mut input = raw_input(None, HashMap::new());
        input.entrypoint = manifest_path("flows/missing.fabro");

        let Err(error) = normalize_source(input) else {
            panic!("missing entrypoint should fail");
        };

        assert!(matches!(
            error,
            RunCompilerError::InvalidSource(InvalidSourceError::MissingEntrypoint { .. })
        ));
    }

    #[test]
    fn normalize_source_rejects_missing_dockerfile_with_pinned_message() {
        let workflow_toml = r#"
_version = 1

[run.environment.image]
dockerfile = { path = "Dockerfile" }
"#;

        let Err(error) = normalize_source(raw_input(Some(workflow_toml), HashMap::new())) else {
            panic!("missing dockerfile should fail");
        };

        assert!(matches!(
            error,
            RunCompilerError::InvalidSource(InvalidSourceError::MissingDockerfile { .. })
        ));
        assert_eq!(
            error.to_string(),
            "missing bundled dockerfile: flows/Dockerfile"
        );
    }

    #[test]
    fn normalize_source_preserves_settings_parse_source_chain() {
        let workflow_toml = r#"
_version = 1

[run.unknown-table]
key = "value"
"#;

        let Err(error) = normalize_source(raw_input(Some(workflow_toml), HashMap::new())) else {
            panic!("unknown settings key should fail");
        };

        assert_eq!(error.to_string(), "Failed to parse run config TOML");
        let source = error
            .source()
            .expect("parse error should retain the TOML source");
        assert!(source.to_string().contains("unknown"));
    }

    #[test]
    fn normalize_source_resolves_bundled_dockerfile() {
        let workflow_toml = r#"
_version = 1

[run.environment.image]
dockerfile = { path = "Dockerfile" }
"#;
        let normalized = normalize_source(raw_input(
            Some(workflow_toml),
            HashMap::from([(
                manifest_path("flows/Dockerfile"),
                "FROM ubuntu:24.04\n".to_string(),
            )]),
        ))
        .expect("bundled dockerfile should resolve");
        let dockerfile = normalized
            .workflow_layer
            .as_ref()
            .and_then(|layer| layer.run.as_ref())
            .and_then(|run| run.environment.as_ref())
            .and_then(|environment| environment.image.as_ref())
            .and_then(|image| image.dockerfile.as_ref());

        assert_eq!(
            dockerfile,
            Some(&EnvironmentDockerfileLayer::Inline(
                "FROM ubuntu:24.04\n".to_string()
            ))
        );
    }

    #[test]
    fn settings_apply_precedence_vars_inputs_and_safe_artifact_globs() {
        let workflow_toml = r#"
_version = 1

[run.metadata]
layer = "workflow"
owner = "{{ vars.owner }}"

[run.inputs]
target = "workflow"

[run.artifacts]
include = ["reports/{{ vars.owner }}/*.json"]
"#;
        let mut input = raw_input(Some(workflow_toml), HashMap::new());
        let RunCompilerSettingsInput::LegacyManifest { project_settings } =
            &mut input.settings_input
        else {
            panic!("test fixture should use legacy manifest settings");
        };
        project_settings.push(ProjectSettingsSource {
            path: Ok(manifest_path(".fabro/project.toml")),
            toml: r#"
_version = 1

[run.metadata]
layer = "project"
"#
            .to_string(),
        });
        input.user_toml = vec![
            r#"
_version = 1

[run.metadata]
layer = "user"
"#
            .to_string(),
        ];
        input.run_overrides = Some(
            toml::from_str::<SettingsLayer>(
                r#"
_version = 1

[run.metadata]
layer = "args"
owner = "{{ vars.owner }}"
"#,
            )
            .expect("args settings should parse")
            .run
            .expect("args run layer should exist"),
        );
        input.input_overrides.insert(
            "target".to_string(),
            toml::Value::String("override".to_string()),
        );
        input.inline_goal_override = Some("Ship {{ vars.owner }}".to_string());

        let prepared = prepare_run(
            input,
            HashMap::from([("owner".to_string(), "payments".to_string())]),
        )
        .expect("settings should prepare");
        let settings = prepared.settings();

        assert_eq!(
            settings.run.metadata.get("layer").map(String::as_str),
            Some("args")
        );
        assert_eq!(
            settings.run.metadata.get("owner").map(String::as_str),
            Some("payments")
        );
        assert_eq!(
            settings.run.inputs.get("target"),
            Some(&toml::Value::String("override".to_string()))
        );
        assert_eq!(settings.run.artifacts.include, vec![
            "reports/payments/*.json"
        ]);
        let Some(RunGoal::Inline(goal)) = settings.run.goal.as_ref() else {
            panic!("inline goal override should win");
        };
        assert_eq!(
            goal.resolve_with(&mut ResolveCtx::default()).unwrap(),
            "Ship payments"
        );
    }

    #[test]
    fn settings_reject_artifact_glob_made_unsafe_by_variable() {
        let workflow_toml = r#"
_version = 1

[run.artifacts]
include = ["reports/{{ vars.path }}/*.json"]
"#;
        let input = raw_input(Some(workflow_toml), HashMap::new());

        let Err(error) = prepare_run(
            input,
            HashMap::from([("path".to_string(), "../secrets".to_string())]),
        ) else {
            panic!("unsafe artifact glob should fail");
        };

        assert!(matches!(
            error,
            RunCompilerError::VariableInterpolation(VariableInterpolationError::ArtifactGlob {
                index:  0,
                source: WorkspaceGlobError::ParentTraversal { .. },
            })
        ));
    }

    #[test]
    fn graph_vars_are_hard_errors_and_successfully_render_when_present() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let missing = prepare_run(raw_input(None, HashMap::new()), HashMap::new())
            .expect("settings preparation should not compile graph vars");
        let Err(error) = compile_graph(missing, test_provider_ids(), Arc::clone(&catalog)) else {
            panic!("missing graph variable should be a hard error");
        };
        assert!(matches!(
            error,
            RunCompilerError::Workflow(WorkflowError::ValidationFailed { .. })
        ));

        let mut input = raw_input(None, HashMap::new());
        input.input_overrides.insert(
            "target".to_string(),
            toml::Value::String("checkout".to_string()),
        );
        let prepared = prepare_run(
            input,
            HashMap::from([("owner".to_string(), "payments".to_string())]),
        )
        .expect("settings should prepare");
        let compiled = compile_graph(prepared, test_provider_ids(), catalog)
            .expect("graph variables should render");
        let work = &compiled.compiled.validated().graph().nodes["work"];

        assert_eq!(
            work.attrs.get("prompt").and_then(AttrValue::as_str),
            Some("Ship checkout for payments")
        );
        assert_eq!(
            work.attrs.get("provider").and_then(AttrValue::as_str),
            Some("openai")
        );
    }

    #[test]
    fn assembly_retains_entrypoint_and_run_metadata() {
        let run_id = RunId::new();
        let parent_id = RunId::new();
        let automation = AutomationRef {
            id:              "nightly".to_string(),
            name:            Some("Nightly".to_string()),
            trigger_id:      Some("schedule".to_string()),
            workflow_source: None,
        };
        let submitted = b"submitted manifest".to_vec();
        let workflow_version_id = fabro_types::test_support::test_workflow_version_id();
        let mut input = raw_input(None, HashMap::new());
        input.run_id = Some(run_id);
        input.parent_id = Some(parent_id);
        input.title = Some("Compiler boundary".to_string());
        input.workflow_slug = Some("compiler-boundary".to_string());
        input.workflow_version_id = Some(workflow_version_id);
        input.web_url = Some(format!("https://fabro.test/runs/{run_id}"));
        input.submitted_manifest_bytes = Some(submitted.clone());
        input.automation = Some(automation.clone());
        input.input_overrides.insert(
            "target".to_string(),
            toml::Value::String("checkout".to_string()),
        );
        let expected_entrypoint = input.entrypoint.clone();
        let catalog = Arc::new(Catalog::from_builtin().unwrap());

        let prepared = prepare_run(
            input,
            HashMap::from([("owner".to_string(), "payments".to_string())]),
        )
        .expect("settings should prepare");
        let compiled = compile_graph(prepared, test_provider_ids(), Arc::clone(&catalog))
            .expect("graph should compile");
        let pinned = pin_models(compiled, &catalog).expect("models should pin");
        let persistence = assemble_run(pinned);

        assert_eq!(persistence.run_id(), run_id);
        assert_eq!(persistence.workflow_slug(), Some("compiler-boundary"));
        assert_eq!(persistence.workflow_version_id(), Some(workflow_version_id));
        assert_eq!(
            persistence.submitted_manifest_bytes(),
            Some(submitted.as_slice())
        );
        assert_eq!(persistence.automation(), Some(&automation));
        assert_eq!(
            persistence
                .definition()
                .map(|definition| &definition.workflow_path),
            Some(&expected_entrypoint)
        );
        assert_eq!(
            persistence.materialized().settings().run.goal.as_ref(),
            Some(&RunGoal::Inline(InterpString::parse("Graph goal")))
        );
    }
}
