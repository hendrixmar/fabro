#![cfg_attr(
    test,
    expect(
        clippy::disallowed_methods,
        reason = "tests write workflow fixture files synchronously before exercising async creation"
    )
)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fabro_config::Storage;
use fabro_graphviz::graph::{AttrValue, Graph};
use fabro_model::{Catalog, ProviderId};
use fabro_store::{BlobStore, Database};
use fabro_template::TemplateContext;
use fabro_types::{
    AutomationRef, BlobHash, ForkSourceRef, GitContext, ManifestPath, RunId, RunProvenance,
    RunTarget, WorkflowSettings, WorkflowVersionId,
};
use fabro_util::json::normalize_json_value;
use tokio::task::spawn_blocking;

use super::source::{ResolveWorkflowInput, WorkflowInput, resolve_workflow};
use crate::error::Error;
use crate::event::{self, Event, append_event};
use crate::pipeline::types::PersistOptions;
use crate::pipeline::{self, Persisted, TransformOptions, Validated};
use crate::records::RunSpec;
use crate::run_materialization;
use crate::transforms::{ModelResolutionTransform, RenderMode};
use crate::workflow_bundle::{RunDefinition, WorkflowBundle};

#[derive(Clone, Debug)]
pub struct CreateRunInput {
    pub workflow: WorkflowInput,
    pub settings: WorkflowSettings,
    /// Run-scoped variables (`{{ vars.* }}`) snapshotted from the server's
    /// variable store at create time, threaded into the template render
    /// context for prompts and goals. Empty for offline/CLI callers.
    pub vars: HashMap<String, String>,
    pub cwd: PathBuf,
    pub workflow_slug: Option<String>,
    pub workflow_path: Option<ManifestPath>,
    pub workflow_bundle: Option<WorkflowBundle>,
    pub target: Option<RunTarget>,
    pub submitted_manifest_bytes: Option<Vec<u8>>,
    pub run_id: Option<RunId>,
    pub title: Option<String>,
    pub automation: Option<AutomationRef>,
    pub git: Option<GitContext>,
    pub fork_source_ref: Option<ForkSourceRef>,
    pub parent_id: Option<RunId>,
    pub provenance: RunProvenance,
    pub configured_providers: Vec<ProviderId>,
    /// Public URL where this run can be viewed in the web UI, when the server
    /// has the web UI enabled. Recorded on the `run.created` event so attach
    /// replays can surface the link.
    pub web_url: Option<String>,
}

impl CreateRunInput {
    /// Split into the compile-stage input and the persistence metadata for
    /// `run_id`, the two halves of the create pipeline.
    fn into_stages(
        self,
        run_id: RunId,
        storage_root: PathBuf,
    ) -> (CreateRunCompileInput, CreateRunPersistenceMetadata) {
        let Self {
            workflow,
            settings,
            vars,
            cwd,
            workflow_slug,
            workflow_path,
            workflow_bundle,
            target,
            submitted_manifest_bytes,
            run_id: _,
            title,
            automation,
            git,
            fork_source_ref,
            parent_id,
            provenance,
            configured_providers,
            web_url,
        } = self;
        (
            CreateRunCompileInput {
                workflow,
                settings,
                vars,
                cwd,
                workflow_path,
                workflow_bundle,
                configured_providers,
            },
            CreateRunPersistenceMetadata {
                run_id,
                storage_root,
                workflow_slug,
                workflow_version_id: None,
                target,
                submitted_manifest_bytes,
                title,
                automation,
                git,
                fork_source_ref,
                parent_id,
                provenance,
                web_url,
            },
        )
    }
}

/// Inputs needed to resolve and compile a workflow for run creation.
#[derive(Debug)]
pub struct CreateRunCompileInput {
    pub workflow:             WorkflowInput,
    pub settings:             WorkflowSettings,
    pub vars:                 HashMap<String, String>,
    pub cwd:                  PathBuf,
    pub workflow_path:        Option<ManifestPath>,
    pub workflow_bundle:      Option<WorkflowBundle>,
    pub configured_providers: Vec<ProviderId>,
}

/// Durable metadata joined to a materialized workflow before persistence.
/// `run_id` is already resolved, and `storage_root` is used to derive the
/// run's scratch directory during pure input assembly.
#[derive(Debug)]
pub struct CreateRunPersistenceMetadata {
    pub run_id: RunId,
    pub storage_root: PathBuf,
    pub workflow_slug: Option<String>,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub target: Option<RunTarget>,
    pub submitted_manifest_bytes: Option<Vec<u8>>,
    pub title: Option<String>,
    pub automation: Option<AutomationRef>,
    pub git: Option<GitContext>,
    pub fork_source_ref: Option<ForkSourceRef>,
    pub parent_id: Option<RunId>,
    pub provenance: RunProvenance,
    pub web_url: Option<String>,
}

#[derive(Debug)]
pub struct CreatedRun {
    pub persisted: Persisted,
    pub run_id:    RunId,
    pub run_dir:   PathBuf,
    pub dot_path:  Option<PathBuf>,
}

/// Result of resolving, preprocessing, validating, and promoting a workflow
/// for run creation. Model selectors in the graph are resolved, while the run
/// settings still reflect the compiled source and have not been materialized.
pub struct CompiledRun {
    validated:            Validated,
    settings:             WorkflowSettings,
    raw_source:           String,
    workflow_slug:        Option<String>,
    dot_path:             Option<PathBuf>,
    definition:           Option<RunDefinition>,
    source_directory:     String,
    labels:               HashMap<String, String>,
    configured_providers: Vec<ProviderId>,
}

impl CompiledRun {
    pub fn validated(&self) -> &Validated {
        &self.validated
    }

    pub fn settings(&self) -> &WorkflowSettings {
        &self.settings
    }
}

/// Compiled workflow with its run-level model settings materialized against
/// the same provider snapshot used during compilation.
pub struct MaterializedRun {
    validated:        Validated,
    settings:         WorkflowSettings,
    raw_source:       String,
    workflow_slug:    Option<String>,
    dot_path:         Option<PathBuf>,
    definition:       Option<RunDefinition>,
    source_directory: String,
    labels:           HashMap<String, String>,
}

impl MaterializedRun {
    pub fn settings(&self) -> &WorkflowSettings {
        &self.settings
    }
}

/// Complete input for creating a durable run. The run ID and run directory
/// are resolved during assembly, before persistence begins.
pub struct CreateRunPersistenceInput {
    materialized: MaterializedRun,
    run_id: RunId,
    run_dir: PathBuf,
    workflow_slug: Option<String>,
    workflow_version_id: Option<WorkflowVersionId>,
    target: Option<RunTarget>,
    submitted_manifest_bytes: Option<Vec<u8>>,
    title: Option<String>,
    automation: Option<AutomationRef>,
    git: Option<GitContext>,
    fork_source_ref: Option<ForkSourceRef>,
    parent_id: Option<RunId>,
    provenance: RunProvenance,
    web_url: Option<String>,
}

impl CreateRunPersistenceInput {
    pub fn materialized(&self) -> &MaterializedRun {
        &self.materialized
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn workflow_slug(&self) -> Option<&str> {
        self.workflow_slug.as_deref()
    }

    pub fn workflow_version_id(&self) -> Option<WorkflowVersionId> {
        self.workflow_version_id
    }

    pub fn submitted_manifest_bytes(&self) -> Option<&[u8]> {
        self.submitted_manifest_bytes.as_deref()
    }

    pub fn automation(&self) -> Option<&AutomationRef> {
        self.automation.as_ref()
    }

    pub fn definition(&self) -> Option<&RunDefinition> {
        self.materialized.definition.as_ref()
    }
}

/// Resolve workflow inputs, normalize settings using the caller-provided
/// catalog, and persist a run directory.
pub async fn create(
    store: &Database,
    request: CreateRunInput,
    storage_root: PathBuf,
    catalog: Arc<Catalog>,
) -> Result<CreatedRun, Error> {
    let run_id = request.run_id.unwrap_or_default();
    let persistence_input = spawn_blocking(move || {
        let (compile_input, metadata) = request.into_stages(run_id, storage_root);
        let compiled = compile_create_run(compile_input, Arc::clone(&catalog))?;
        let materialized = materialize_create_run(compiled, catalog.as_ref())?;
        Ok::<_, Error>(assemble_create_run_persistence_input(
            materialized,
            metadata,
        ))
    })
    .await
    .map_err(|err| Error::engine_with_source("workflow create task failed", err))??;

    Box::pin(persist_create_run(store, persistence_input)).await
}

/// Resolve, preprocess, validate, and promote a workflow for run creation.
///
/// This stage is synchronous and may read workflow files. Async callers must
/// run it on a blocking thread.
pub fn compile_create_run(
    input: CreateRunCompileInput,
    catalog: Arc<Catalog>,
) -> Result<CompiledRun, Error> {
    let CreateRunCompileInput {
        workflow,
        settings,
        vars,
        cwd,
        workflow_path,
        workflow_bundle,
        configured_providers,
    } = input;
    let resolved = resolve_workflow(ResolveWorkflowInput {
        workflow,
        settings,
        cwd,
    })
    .map_err(|err| Error::Parse(err.to_string()))?;
    let settings = resolved.settings;
    let labels = settings.combined_labels();
    let source_name = resolved
        .dot_path
        .as_ref()
        .map(|path| path.display().to_string());
    let definition = match (workflow_path, workflow_bundle) {
        (Some(workflow_path), Some(workflow_bundle)) => {
            let bundled = workflow_bundle.workflow(&workflow_path).ok_or_else(|| {
                Error::Parse("workflow path is missing from workflow bundle".to_string())
            })?;
            if bundled.source != resolved.raw_source {
                return Err(Error::Parse(
                    "resolved workflow does not match workflow bundle entrypoint".to_string(),
                ));
            }
            Some(RunDefinition::new(workflow_path, workflow_bundle))
        }
        (None, None) => None,
        _ => {
            return Err(Error::Parse(
                "workflow path and workflow bundle must be provided together".to_string(),
            ));
        }
    };
    let mut validated = preprocess_and_validate(
        &resolved.raw_source,
        resolved.goal_override.as_deref(),
        &TransformOptions {
            current_dir: resolved.current_dir.clone(),
            file_resolver: resolved.file_resolver.clone(),
            template_context: template_context(Some(&settings), vars),
            source_name,
            render_mode: RenderMode::Structural,
            custom_transforms: Vec::new(),
            model_resolution: Some(
                ModelResolutionTransform::for_eligible(
                    catalog,
                    configured_providers.iter().cloned().collect(),
                )
                .with_default_provider(configured_default_provider(&settings)),
            ),
        },
    )?;

    validated.promote_template_undefined_variables_to_errors();
    if validated.has_errors() {
        return Err(Error::ValidationFailed {
            diagnostics: validated.diagnostics().to_vec(),
        });
    }

    Ok(CompiledRun {
        validated,
        settings,
        raw_source: resolved.raw_source,
        workflow_slug: resolved.workflow_slug,
        dot_path: resolved.dot_path,
        definition,
        source_directory: resolved.working_directory.to_string_lossy().to_string(),
        labels,
        configured_providers,
    })
}

/// Materialize run-level model settings from a compiled workflow.
pub fn materialize_create_run(
    compiled: CompiledRun,
    catalog: &Catalog,
) -> Result<MaterializedRun, Error> {
    let CompiledRun {
        validated,
        settings,
        raw_source,
        workflow_slug,
        dot_path,
        definition,
        source_directory,
        labels,
        configured_providers,
    } = compiled;
    let settings = run_materialization::materialize_run(
        settings,
        validated.graph(),
        catalog,
        &configured_providers,
    )?;
    Ok(MaterializedRun {
        validated,
        settings,
        raw_source,
        workflow_slug,
        dot_path,
        definition,
        source_directory,
        labels,
    })
}

/// Assemble all inputs needed for persistence without I/O or recompilation.
pub fn assemble_create_run_persistence_input(
    materialized: MaterializedRun,
    metadata: CreateRunPersistenceMetadata,
) -> CreateRunPersistenceInput {
    let CreateRunPersistenceMetadata {
        run_id,
        storage_root,
        workflow_slug,
        workflow_version_id,
        target,
        submitted_manifest_bytes,
        title,
        automation,
        git,
        fork_source_ref,
        parent_id,
        provenance,
        web_url,
    } = metadata;
    let run_dir = Storage::new(storage_root)
        .run_scratch(&run_id)
        .root()
        .to_path_buf();
    let workflow_slug = workflow_slug.or_else(|| materialized.workflow_slug.clone());

    CreateRunPersistenceInput {
        materialized,
        run_id,
        run_dir,
        workflow_slug,
        workflow_version_id,
        target,
        submitted_manifest_bytes,
        title,
        automation,
        git,
        fork_source_ref,
        parent_id,
        provenance,
        web_url,
    }
}

/// Persist one already-compiled and materialized run without recompiling it.
pub async fn persist_create_run(
    store: &Database,
    input: CreateRunPersistenceInput,
) -> Result<CreatedRun, Error> {
    let CreateRunPersistenceInput {
        materialized,
        run_id,
        run_dir,
        workflow_slug,
        workflow_version_id,
        target,
        submitted_manifest_bytes,
        title,
        automation,
        git,
        fork_source_ref,
        parent_id,
        provenance,
        web_url,
    } = input;
    let MaterializedRun {
        validated,
        settings,
        raw_source,
        workflow_slug: _,
        dot_path,
        definition,
        source_directory,
        labels,
    } = materialized;
    let (source_directory, git) = match target.as_ref() {
        Some(RunTarget::None {}) => (None, None),
        Some(RunTarget::Folder { path }) => (Some(path.clone()), git),
        Some(RunTarget::Git(_)) | None => (Some(source_directory), git),
    };
    let persisted_run_dir = run_dir.clone();
    let persisted = spawn_blocking(move || {
        let run_spec = RunSpec {
            run_id,
            settings,
            graph: validated.graph().clone(),
            graph_source: Some(validated.source().to_string()),
            workflow_slug,
            workflow_version_id,
            target,
            automation,
            source_directory,
            labels,
            provenance,
            manifest_blob: None,
            definition_blob: None,
            spec_blob: None,
            git,
            fork_source_ref,
        };
        pipeline::persist(validated, PersistOptions {
            run_dir: persisted_run_dir,
            run_spec,
        })
    })
    .await
    .map_err(|err| Error::engine_with_source("workflow create task failed", err))??;

    persist_created_run(
        store,
        &persisted,
        &raw_source,
        submitted_manifest_bytes.as_deref(),
        definition.as_ref(),
        title,
        parent_id,
        web_url,
    )
    .await?;

    Ok(CreatedRun {
        persisted,
        run_id,
        run_dir,
        dot_path,
    })
}

async fn persist_created_run(
    store: &Database,
    persisted: &Persisted,
    workflow_source: &str,
    submitted_manifest_bytes: Option<&[u8]>,
    accepted_definition: Option<&RunDefinition>,
    explicit_title: Option<String>,
    parent_id: Option<RunId>,
    web_url: Option<String>,
) -> Result<(), Error> {
    let record = persisted.run_spec();
    let definition_bytes = accepted_definition
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|err| Error::engine_with_source("failed to serialize run definition", err))?;
    let spec_bytes = serde_json::to_vec(record)
        .map_err(|err| Error::engine_with_source("failed to serialize run spec", err))?;
    let blob_store = store.blobs();
    let (manifest_blob, definition_blob, spec_blob) = tokio::try_join!(
        write_optional_blob(&blob_store, submitted_manifest_bytes),
        write_optional_blob(&blob_store, definition_bytes.as_deref()),
        async { blob_store.write(&spec_bytes).await.map_err(store_error) },
    )?;

    let title = explicit_title.unwrap_or_else(|| fabro_types::infer_run_title(record.graph.goal()));
    let first_event = Event::RunCreated {
        run_id: record.run_id,
        title: Some(title),
        settings: normalize_json_value(
            serde_json::to_value(&record.settings).map_err(|err| Error::engine(err.to_string()))?,
        ),
        graph: normalize_json_value(
            serde_json::to_value(&record.graph).map_err(|err| Error::engine(err.to_string()))?,
        ),
        workflow_source: (!workflow_source.is_empty()).then(|| workflow_source.to_string()),
        labels: record
            .labels
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        source_directory: record.source_directory.clone(),
        workflow_slug: record.workflow_slug.clone(),
        workflow_version_id: record.workflow_version_id,
        target: record.target.clone(),
        automation: record.automation.clone(),
        provenance: record.provenance.clone(),
        manifest_blob,
        spec_blob: Some(spec_blob),
        git: record.git.clone(),
        fork_source_ref: record.fork_source_ref.clone(),
        retried_from: None,
        parent_id,
        web_url,
    };
    let run_store = event::create_run(
        store,
        &record.run_id,
        &first_event,
        record.run_id.created_at(),
    )
    .await
    .map_err(|err| Error::engine_with_source("failed to create run store", err))?;
    append_event(&run_store, &record.run_id, &Event::RunSubmitted {
        definition_blob,
    })
    .await
    .map_err(store_error)
}

async fn write_optional_blob(
    blob_store: &BlobStore,
    bytes: Option<&[u8]>,
) -> Result<Option<BlobHash>, Error> {
    match bytes {
        Some(bytes) => blob_store.write(bytes).await.map(Some).map_err(store_error),
        None => Ok(None),
    }
}

fn store_error(err: impl Into<anyhow::Error>) -> Error {
    Error::engine_with_source("run store operation failed", err)
}

/// Parse, transform, and validate `dot_source`.
///
/// `options.model_resolution` drives both halves of catalog awareness: it
/// selects concrete models during TRANSFORM and enables the catalog-backed
/// lint rules during VALIDATE. `None` leaves authored model and provider
/// selectors untouched for offline structural validation.
pub(super) fn preprocess_and_validate(
    dot_source: &str,
    goal_override: Option<&str>,
    options: &TransformOptions,
) -> Result<Validated, Error> {
    let mut parsed = pipeline::parse(dot_source)?;
    apply_goal_override(&mut parsed.graph, goal_override);

    let transformed = pipeline::transform(parsed, options)?;
    let catalog = options
        .model_resolution
        .as_ref()
        .map(ModelResolutionTransform::catalog);
    Ok(pipeline::validate(transformed, catalog, &[]))
}

/// The workflow-level default provider, treating an empty setting as unset.
pub(super) fn configured_default_provider(settings: &WorkflowSettings) -> Option<ProviderId> {
    settings
        .run
        .model
        .provider
        .as_deref()
        .filter(|provider| !provider.is_empty())
        .map(ProviderId::new)
}

pub(super) fn template_context(
    settings: Option<&WorkflowSettings>,
    vars: HashMap<String, String>,
) -> TemplateContext {
    TemplateContext::new()
        .with_inputs(run_inputs(settings))
        .with_vars(vars)
}

fn run_inputs(settings: Option<&WorkflowSettings>) -> HashMap<String, toml::Value> {
    settings
        .map(|settings| settings.run.inputs.clone())
        .unwrap_or_default()
}

fn apply_goal_override(graph: &mut Graph, goal_override: Option<&str>) {
    if let Some(goal_override) = goal_override {
        graph.attrs.insert(
            "goal".to_string(),
            AttrValue::String(goal_override.to_string()),
        );
    }
}

pub fn make_run_dir(scratch_base: &Path, run_id: &RunId) -> PathBuf {
    fabro_config::RunScratch::for_run(scratch_base, run_id)
        .root()
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{Local, TimeZone, Utc};
    use fabro_config::{
        PrepareStep, ReplaceMap, RunExecutionLayer, RunGoalLayer, RunLayer, RunModelLayer,
        RunPrepareLayer, RunPullRequestLayer, WorkflowSettingsBuilder,
    };
    use fabro_graphviz::graph::AttrValue;
    use fabro_store::Database;
    use fabro_types::settings::InterpString;
    use fabro_types::settings::run::RunMode;
    use fabro_types::{EventBody, WorkflowSettings, fixtures, test_support};
    use fabro_util::error::collect_chain;
    use fabro_validate::Severity;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;

    use super::*;
    use crate::file_resolver::FileResolver;
    use crate::operations::{ValidateInput, validate, validate_with_catalog};
    use crate::pipeline::types::{GOAL_SELF_REFERENCE_RULE, TEMPLATE_UNDEFINED_VARIABLE_RULE};
    use crate::transforms::Transform;
    use crate::workflow_bundle::BundledWorkflow;
    fn memory_store() -> Arc<Database> {
        Arc::new(fabro_store::test_support::test_database(
            Arc::new(InMemory::new()),
            "",
            Duration::from_millis(1),
            None,
        ))
    }

    fn settings_from_run_layer(run: RunLayer) -> WorkflowSettings {
        WorkflowSettingsBuilder::new()
            .server_manifest_defaults(
                RunLayer::default(),
                fabro_environment::seeded_catalog_layer(),
            )
            .run_overrides(run)
            .build()
            .expect("settings should resolve")
    }

    fn test_default_settings() -> WorkflowSettings {
        WorkflowSettingsBuilder::new()
            .server_manifest_defaults(
                RunLayer::default(),
                fabro_environment::seeded_catalog_layer(),
            )
            .build()
            .expect("default settings should resolve")
    }

    fn test_catalog() -> Arc<Catalog> {
        Arc::new(Catalog::from_builtin().unwrap())
    }

    fn portable_model_catalog() -> Arc<Catalog> {
        let settings: fabro_model::catalog::LlmCatalogSettings = toml::from_str(
            r#"
[providers.openai]
display_name = "OpenAI"
adapter = "openai"
agent_profile = "openai"
priority = 90

[providers.openai.models."gpt-5.6-sol"]
display_name = "GPT-5.6 Sol"
family = "gpt-5"
aliases = ["gpt-56-sol"]
default = true

[providers.openai.models."gpt-5.6-sol".limits]
context_window = 1000

[providers.openai.models."gpt-5.6-sol".features]
tools = true
vision = false
reasoning = false

[providers.openrouter]
display_name = "OpenRouter"
adapter = "openai_compatible"
agent_profile = "openai"
priority = 25

[providers.openrouter.models."gpt-5.6-sol"]
api_id = "openai/gpt-5.6-sol"
display_name = "GPT-5.6 Sol (via OpenRouter)"
family = "gpt-5"
aliases = ["gpt-56-sol"]
default = true

[providers.openrouter.models."gpt-5.6-sol".limits]
context_window = 1000

[providers.openrouter.models."gpt-5.6-sol".features]
tools = true
vision = false
reasoning = false
"#,
        )
        .unwrap();
        Arc::new(Catalog::from_settings(&settings).unwrap())
    }

    fn test_provider_ids() -> Vec<ProviderId> {
        Catalog::builtin().all_provider_ids().into_iter().collect()
    }

    fn compile_input(request: &CreateRunInput) -> CreateRunCompileInput {
        let (compile_input, _) = request
            .clone()
            .into_stages(RunId::new(), PathBuf::from("/tmp/storage"));
        compile_input
    }

    fn persistence_metadata(
        request: &CreateRunInput,
        run_id: RunId,
        storage_root: &Path,
    ) -> CreateRunPersistenceMetadata {
        let (_, metadata) = request
            .clone()
            .into_stages(run_id, storage_root.to_path_buf());
        metadata
    }

    fn validate_dot(dot_source: &str, settings: WorkflowSettings) -> Validated {
        validate_with_catalog(
            ValidateInput {
                workflow: WorkflowInput::DotSource {
                    source:   dot_source.to_string(),
                    base_dir: None,
                },
                settings,
                vars: HashMap::new(),
                cwd: PathBuf::from("."),
                custom_transforms: Vec::new(),
            },
            test_catalog(),
        )
        .unwrap()
    }

    /// Drive the create-time pipeline with an explicit variable snapshot, the
    /// way the server does (`Structural` render mode, undefined vars promoted
    /// to errors at run-create).
    fn validate_dot_with_vars(dot_source: &str, vars: HashMap<String, String>) -> Validated {
        preprocess_and_validate(
            dot_source,
            None,
            &test_transform_options(
                PathBuf::from("."),
                None,
                RenderMode::Structural,
                template_context(Some(&WorkflowSettings::default()), vars),
            ),
        )
        .unwrap()
    }

    /// Catalog-backed TRANSFORM options for the built-in test catalog.
    fn test_transform_options(
        current_dir: PathBuf,
        file_resolver: Option<Arc<dyn FileResolver>>,
        render_mode: RenderMode,
        template_context: TemplateContext,
    ) -> TransformOptions {
        TransformOptions {
            current_dir: Some(current_dir),
            file_resolver,
            template_context,
            source_name: Some("workflow.fabro".to_string()),
            render_mode,
            custom_transforms: Vec::new(),
            model_resolution: Some(ModelResolutionTransform::new(test_catalog())),
        }
    }

    const MINIMAL_DOT: &str = r#"digraph Test {
        graph [goal="Build feature"]
        start [shape=Mdiamond]
        exit  [shape=Msquare]
        start -> exit
    }"#;

    #[test]
    fn validate_minimal() {
        let validated = validate_dot(MINIMAL_DOT, WorkflowSettings::default());
        validated.raise_on_errors().unwrap();

        assert_eq!(validated.graph().name, "Test");
        assert!(validated.graph().find_start_node().is_some());
        assert!(validated.graph().find_exit_node().is_some());
    }

    #[test]
    fn validate_rejects_goal_self_reference() {
        // A goal can't reference itself; a prompt can reference the goal.
        let dot = r#"digraph Test {
            graph [goal="Refine {{ goal }}"]
            start [shape=Mdiamond]
            work [prompt="Work on {{ goal }}"]
            exit [shape=Msquare]
            start -> work -> exit
        }"#;
        let validated = validate_dot(dot, WorkflowSettings::default());

        assert!(
            validated.has_errors(),
            "goal self-reference should fail validation"
        );
        let self_ref: Vec<_> = validated
            .diagnostics()
            .iter()
            .filter(|d| d.rule == GOAL_SELF_REFERENCE_RULE)
            .collect();
        assert_eq!(
            self_ref.len(),
            1,
            "expected one goal self-reference diagnostic, got: {:?}",
            validated.diagnostics()
        );
        assert_eq!(self_ref[0].severity, Severity::Error);
    }

    #[test]
    fn validate_with_unbound_inputs_warns_but_succeeds() {
        let dot = r#"digraph Test {
            graph [goal="Build feature"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare,  label="Exit"]
            work  [label="Work", prompt="Work on {{ inputs.app_dir }}"]
            start -> work -> exit
        }"#;
        let validated = validate_dot(dot, WorkflowSettings::default());
        validated.raise_on_errors().unwrap();

        let diagnostic = validated
            .diagnostics()
            .iter()
            .find(|d| d.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE)
            .expect("expected a template_undefined_variable diagnostic");
        assert_eq!(diagnostic.severity, Severity::Warning);
        assert!(
            diagnostic.message.contains("inputs.app_dir"),
            "missing variable in: {}",
            diagnostic.message
        );
    }

    #[test]
    fn vars_resolve_in_node_prompt_through_create_pipeline() {
        let dot = r#"digraph Test {
            graph [goal="Ship it"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare,  label="Exit"]
            work  [label="Work", prompt="Service: {{ vars.SERVICE }}"]
            start -> work -> exit
        }"#;
        let vars = HashMap::from([("SERVICE".to_string(), "billing".to_string())]);
        let validated = validate_dot_with_vars(dot, vars);
        validated.raise_on_errors().unwrap();
        assert!(
            !validated
                .diagnostics()
                .iter()
                .any(|d| d.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE),
            "vars.SERVICE should resolve through the create pipeline; got: {:?}",
            validated.diagnostics()
        );
    }

    #[test]
    fn unknown_var_in_prompt_warns_at_validate_then_errors_at_run_create() {
        let dot = r#"digraph Test {
            graph [goal="Ship it"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare,  label="Exit"]
            work  [label="Work", prompt="Service: {{ vars.MISSING }}"]
            start -> work -> exit
        }"#;
        let mut validated = validate_dot_with_vars(dot, HashMap::new());

        // `fabro validate` surfaces a warning, not a hard failure.
        let diagnostic = validated
            .diagnostics()
            .iter()
            .find(|d| d.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE)
            .expect("expected a template_undefined_variable diagnostic");
        assert_eq!(diagnostic.severity, Severity::Warning);
        assert!(
            diagnostic.message.contains("vars.MISSING"),
            "message: {}",
            diagnostic.message
        );

        // Run-create promotes the same diagnostic to a hard error.
        validated.promote_template_undefined_variables_to_errors();
        assert!(validated.has_errors());
    }

    #[test]
    fn unknown_input_in_model_stylesheet_warns_then_errors_at_run_create() {
        let dot = r#"digraph Test {
            graph [model_stylesheet="* { reasoning_effort: {{ inputs.effort }}; }"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare, label="Exit"]
            work  [label="Work", prompt="Do work"]
            start -> work -> exit
        }"#;
        let mut validated = validate_dot(dot, WorkflowSettings::default());

        let diagnostic = validated
            .diagnostics()
            .iter()
            .find(|diagnostic| diagnostic.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE)
            .expect("expected a template_undefined_variable diagnostic");
        assert_eq!(diagnostic.severity, Severity::Warning);
        assert!(
            diagnostic
                .message
                .contains("graph attribute `model_stylesheet`"),
            "message: {}",
            diagnostic.message
        );
        assert!(
            validated
                .diagnostics()
                .iter()
                .all(|diagnostic| diagnostic.rule != "stylesheet_syntax")
        );

        validated.promote_template_undefined_variables_to_errors();
        assert!(validated.has_errors());
        assert_eq!(
            validated
                .diagnostics()
                .iter()
                .find(|diagnostic| diagnostic.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE)
                .unwrap()
                .severity,
            Severity::Error
        );
    }

    #[test]
    fn vars_resolve_in_command_script_through_create_pipeline() {
        let dot = r#"digraph Test {
            graph [goal="Ship it"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare,  label="Exit"]
            work  [label="Work", shape=parallelogram, script="deploy --stage {{ vars.STAGE }}"]
            start -> work -> exit
        }"#;
        let vars = HashMap::from([("STAGE".to_string(), "staging".to_string())]);
        let validated = validate_dot_with_vars(dot, vars);
        validated.raise_on_errors().unwrap();

        assert_eq!(
            validated.graph().nodes["work"]
                .attrs
                .get("script")
                .and_then(fabro_graphviz::graph::AttrValue::as_str),
            Some("deploy --stage staging"),
        );
    }

    /// The script diagnostic must carry the same rule as the prompt one so the
    /// existing run-create promotion catches an unbound value before a run
    /// executes a half-interpolated command.
    #[test]
    fn unknown_input_in_script_warns_at_validate_then_errors_at_run_create() {
        let dot = r#"digraph Test {
            graph [goal="Ship it"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare,  label="Exit"]
            work  [label="Work", shape=parallelogram, script="deploy --stage {{ inputs.stage }}"]
            start -> work -> exit
        }"#;
        let mut validated = validate_dot_with_vars(dot, HashMap::new());

        let diagnostic = validated
            .diagnostics()
            .iter()
            .find(|d| d.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE)
            .expect("expected a template_undefined_variable diagnostic");
        assert_eq!(diagnostic.severity, Severity::Warning);
        assert!(
            diagnostic.message.contains("inputs.stage"),
            "message: {}",
            diagnostic.message
        );

        validated.promote_template_undefined_variables_to_errors();
        assert!(validated.has_errors());
    }

    #[test]
    fn promote_template_undefined_rule_turns_warning_into_error() {
        let dot = r#"digraph Test {
            graph [goal="Build {{ inputs.app_dir }}"]
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare,  label="Exit"]
            start -> exit
        }"#;
        let mut validated = validate_dot(dot, WorkflowSettings::default());
        assert!(!validated.has_errors());

        validated.promote_template_undefined_variables_to_errors();

        assert!(validated.has_errors());
        let diagnostic = validated
            .diagnostics()
            .iter()
            .find(|d| d.rule == TEMPLATE_UNDEFINED_VARIABLE_RULE)
            .expect("expected template diagnostic");
        assert_eq!(diagnostic.severity, Severity::Error);
    }

    #[test]
    fn strict_template_error_for_inline_prompt_names_workflow_file_and_node() {
        let dot = r#"digraph ValidatePlan {
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare, label="Exit"]
            test_inline_prompt [label="moo" prompt="{{ inputs.foo }}"]
            start -> test_inline_prompt -> exit
        }"#;

        let result = preprocess_and_validate(
            dot,
            None,
            &test_transform_options(
                PathBuf::from("."),
                None,
                RenderMode::Strict,
                template_context(Some(&WorkflowSettings::default()), HashMap::new()),
            ),
        );
        let Err(err) = result else {
            panic!("expected strict mode to hard-fail on unbound inline prompt");
        };

        let rendered = collect_chain(&err).join(": ");
        assert!(rendered.contains("workflow.fabro"), "{rendered}");
        assert!(rendered.contains("test_inline_prompt"), "{rendered}");
        assert!(rendered.contains("prompt"), "{rendered}");
        assert!(!rendered.contains("<string>"), "{rendered}");
    }

    #[test]
    fn imported_prompt_template_error_names_prompt_file_and_node() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_path = dir.path().join("test.md");
        std::fs::write(&prompt_path, "{{ inputs.foo }}").unwrap();
        let dot = r#"digraph ValidatePlan {
            start [shape=Mdiamond, label="Start"]
            exit  [shape=Msquare, label="Exit"]
            test_imported_prompt [label="moo" prompt="@test.md"]
            start -> test_imported_prompt -> exit
        }"#;

        let result = preprocess_and_validate(
            dot,
            None,
            &test_transform_options(
                dir.path().to_path_buf(),
                Some(Arc::new(crate::file_resolver::FilesystemFileResolver::new(
                    None,
                ))),
                RenderMode::Strict,
                template_context(Some(&WorkflowSettings::default()), HashMap::new()),
            ),
        );
        let Err(err) = result else {
            panic!("expected strict mode to hard-fail on unbound imported prompt");
        };

        let rendered = collect_chain(&err).join(": ");
        assert!(rendered.contains("test.md"), "{rendered}");
        assert!(rendered.contains("test_imported_prompt"), "{rendered}");
        assert!(rendered.contains("prompt"), "{rendered}");
        assert!(!rendered.contains("<string>"), "{rendered}");
    }

    #[test]
    fn validate_applies_variable_expansion() {
        let dot = r#"digraph Test {
            graph [goal="Fix bugs"]
            start [shape=Mdiamond]
            work  [prompt="Goal: {{ goal }}"]
            exit  [shape=Msquare]
            start -> work -> exit
        }"#;
        let validated = validate_dot(dot, WorkflowSettings::default());
        validated.raise_on_errors().unwrap();

        let prompt = validated.graph().nodes["work"]
            .attrs
            .get("prompt")
            .and_then(AttrValue::as_str)
            .unwrap();
        assert_eq!(prompt, "Goal: Fix bugs");
    }

    #[test]
    fn validate_does_not_render_source_level_templated_node_ids() {
        let dot = r#"digraph Test {
            graph [goal="Fix bugs"]
            start [shape=Mdiamond]
            {{ inputs.step }} [prompt="Do work"]
            exit [shape=Msquare]
            start -> exit
        }"#;

        let result = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   dot.to_string(),
                base_dir: None,
            },
            settings:          settings_from_run_layer({
                let mut inputs = std::collections::HashMap::new();
                inputs.insert("step".to_string(), toml::Value::String("work".to_string()));
                RunLayer {
                    inputs: Some(inputs),
                    ..RunLayer::default()
                }
            }),
            vars:              HashMap::new(),
            cwd:               PathBuf::from("."),
            custom_transforms: Vec::new(),
        });

        assert!(result.is_err());
    }

    #[test]
    fn inline_and_file_prompt_diagnostics_match() {
        fn normalized_diagnostics(
            validated: &Validated,
        ) -> Vec<(String, Severity, String, Option<String>)> {
            validated
                .diagnostics()
                .iter()
                .map(|diagnostic| {
                    (
                        diagnostic.rule.clone(),
                        diagnostic.severity.clone(),
                        diagnostic.message.clone(),
                        diagnostic.node_id.clone(),
                    )
                })
                .collect()
        }

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("missing.md"),
            "Work in {{ inputs.app_dir }}",
        )
        .unwrap();
        std::fs::write(dir.path().join("goal.md"), "Goal: {{ goal }}").unwrap();

        let inline_missing = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   r#"digraph Test {
                    graph [goal="Demo"]
                    start [shape=Mdiamond]
                    work [prompt="Work in {{ inputs.app_dir }}"]
                    exit [shape=Msquare]
                    start -> work -> exit
                }"#
                .to_string(),
                base_dir: Some(dir.path().to_path_buf()),
            },
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               dir.path().to_path_buf(),
            custom_transforms: Vec::new(),
        })
        .unwrap();
        let file_missing = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   r#"digraph Test {
                    graph [goal="Demo"]
                    start [shape=Mdiamond]
                    work [prompt="@missing.md"]
                    exit [shape=Msquare]
                    start -> work -> exit
                }"#
                .to_string(),
                base_dir: Some(dir.path().to_path_buf()),
            },
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               dir.path().to_path_buf(),
            custom_transforms: Vec::new(),
        })
        .unwrap();
        assert_eq!(
            normalized_diagnostics(&inline_missing),
            normalized_diagnostics(&file_missing)
        );

        let inline_goal = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   r#"digraph Test {
                    graph [goal="Ship"]
                    start [shape=Mdiamond]
                    work [prompt="Goal: {{ goal }}"]
                    exit [shape=Msquare]
                    start -> work -> exit
                }"#
                .to_string(),
                base_dir: Some(dir.path().to_path_buf()),
            },
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               dir.path().to_path_buf(),
            custom_transforms: Vec::new(),
        })
        .unwrap();
        let file_goal = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   r#"digraph Test {
                    graph [goal="Ship"]
                    start [shape=Mdiamond]
                    work [prompt="@goal.md"]
                    exit [shape=Msquare]
                    start -> work -> exit
                }"#
                .to_string(),
                base_dir: Some(dir.path().to_path_buf()),
            },
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               dir.path().to_path_buf(),
            custom_transforms: Vec::new(),
        })
        .unwrap();
        assert_eq!(
            inline_goal.graph().nodes["work"].attrs.get("prompt"),
            file_goal.graph().nodes["work"].attrs.get("prompt")
        );
        assert_eq!(
            normalized_diagnostics(&inline_goal),
            normalized_diagnostics(&file_goal)
        );
    }

    #[test]
    fn make_run_dir_uses_run_id_timestamp_in_local_time() {
        let scratch_base = Path::new("/tmp/scratch");
        let run_id = RunId::from(ulid::Ulid::from_datetime(
            Utc.with_ymd_and_hms(2026, 3, 27, 12, 0, 0).unwrap().into(),
        ));
        let expected_date = run_id
            .created_at()
            .with_timezone(&Local)
            .format("%Y%m%d")
            .to_string();

        assert_eq!(
            make_run_dir(scratch_base, &run_id),
            scratch_base.join(format!("{expected_date}-{run_id}"))
        );
    }

    #[test]
    fn validate_applies_stylesheet() {
        let dot = r#"digraph Test {
            graph [goal="Test", model_stylesheet="* { model: sonnet; }"]
            start [shape=Mdiamond]
            work  [label="Work"]
            exit  [shape=Msquare]
            start -> work -> exit
        }"#;
        let validated = validate_dot(dot, WorkflowSettings::default());
        validated.raise_on_errors().unwrap();

        assert_eq!(
            validated.graph().nodes["work"].attrs.get("model"),
            Some(&AttrValue::String("claude-sonnet-5".into()))
        );
    }

    #[test]
    fn validate_applies_config_vars_and_goal_override() {
        let dot = r#"digraph Test {
            graph [goal="original"]
            start [shape=Mdiamond]
            work [prompt="{{ inputs.who }}: {{ goal }}"]
            exit [shape=Msquare]
            start -> work -> exit
        }"#;
        let validated = validate_dot(
            dot,
            settings_from_run_layer({
                let mut inputs = std::collections::HashMap::new();
                inputs.insert("who".to_string(), toml::Value::String("agent".to_string()));
                RunLayer {
                    goal: Some(RunGoalLayer::Inline(InterpString::parse("override"))),
                    inputs: Some(inputs),
                    ..RunLayer::default()
                }
            }),
        );
        validated.raise_on_errors().unwrap();

        assert_eq!(validated.graph().goal(), "override");
        let prompt = validated.graph().nodes["work"]
            .attrs
            .get("prompt")
            .and_then(AttrValue::as_str)
            .unwrap();
        assert_eq!(prompt, "agent: override");
    }

    #[test]
    fn validate_returns_error_on_invalid_dot() {
        let result = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   "not a graph".to_string(),
                base_dir: None,
            },
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               PathBuf::from("."),
            custom_transforms: Vec::new(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn validate_returns_validation_diagnostics() {
        let dot = r#"digraph Test {
            graph [goal="Test"]
            work [label="Work"]
        }"#;
        let validated = validate_dot(dot, WorkflowSettings::default());

        assert!(validated.has_errors());
        assert!(validated.raise_on_errors().is_err());
    }

    #[test]
    fn validate_supports_custom_transforms() {
        struct TagTransform;

        impl Transform for TagTransform {
            fn apply(
                &self,
                graph: fabro_graphviz::graph::Graph,
            ) -> Result<fabro_graphviz::graph::Graph, Error> {
                let mut graph = graph;
                for node in graph.nodes.values_mut() {
                    node.attrs
                        .insert("tagged".to_string(), AttrValue::Boolean(true));
                }

                Ok(graph)
            }
        }

        let validated = validate(ValidateInput {
            workflow:          WorkflowInput::DotSource {
                source:   MINIMAL_DOT.to_string(),
                base_dir: None,
            },
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               PathBuf::from("."),
            custom_transforms: vec![Box::new(TagTransform)],
        })
        .unwrap();
        validated.raise_on_errors().unwrap();

        assert_eq!(
            validated.graph().nodes["start"].attrs.get("tagged"),
            Some(&AttrValue::Boolean(true))
        );
    }

    #[test]
    fn validate_from_file_uses_parent_directory_for_inlining() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("goal.txt");
        let dot_path = dir.path().join("workflow.fabro");

        std::fs::write(&data_path, "ship it").unwrap();
        std::fs::write(
            &dot_path,
            r#"digraph Test {
                graph [goal="@goal.txt"]
                start [shape=Mdiamond]
                exit [shape=Msquare]
                start -> exit
            }"#,
        )
        .unwrap();

        let validated = validate(ValidateInput {
            workflow:          WorkflowInput::Path(dot_path),
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               dir.path().to_path_buf(),
            custom_transforms: Vec::new(),
        })
        .unwrap();
        validated.raise_on_errors().unwrap();
        assert_eq!(validated.graph().goal(), "ship it");
    }

    #[test]
    fn validate_from_file_resolves_minijinja_includes_relative_to_prompt_and_goal_files() {
        let dir = tempfile::tempdir().unwrap();
        let prompt_dir = dir.path().join("prompts");
        let goal_dir = dir.path().join("goals");
        std::fs::create_dir_all(&prompt_dir).unwrap();
        std::fs::create_dir_all(&goal_dir).unwrap();
        std::fs::write(
            prompt_dir.join("prompt.md"),
            r#"{% include "prompt.tpl.md" %}"#,
        )
        .unwrap();
        std::fs::write(prompt_dir.join("prompt.tpl.md"), "included prompt").unwrap();
        std::fs::write(goal_dir.join("goal.md"), r#"{% include "goal.tpl.md" %}"#).unwrap();
        std::fs::write(goal_dir.join("goal.tpl.md"), "included goal").unwrap();

        let dot_path = dir.path().join("workflow.fabro");
        std::fs::write(
            &dot_path,
            r#"digraph Test {
                graph [goal="@goals/goal.md"]
                start [shape=Mdiamond]
                work [prompt="@prompts/prompt.md"]
                exit [shape=Msquare]
                start -> work -> exit
            }"#,
        )
        .unwrap();

        let validated = validate(ValidateInput {
            workflow:          WorkflowInput::Path(dot_path),
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               dir.path().to_path_buf(),
            custom_transforms: Vec::new(),
        })
        .unwrap();

        validated.raise_on_errors().unwrap();
        assert_eq!(validated.graph().goal(), "included goal");
        assert_eq!(
            validated.graph().nodes["work"]
                .attrs
                .get("prompt")
                .and_then(AttrValue::as_str),
            Some("included prompt")
        );
    }

    #[test]
    fn validate_from_bundle_resolves_nested_import_files_relative_to_imported_graph() {
        let validated = validate(ValidateInput {
            workflow:          WorkflowInput::Bundled(BundledWorkflow {
                path:   ManifestPath::from_wire("workflow.fabro").unwrap(),
                source: r#"digraph Test {
                    graph [goal="Ship"]
                    start [shape=Mdiamond]
                    validate [import="./child/validate.fabro"]
                    exit [shape=Msquare]
                    start -> validate -> exit
                }"#
                .to_string(),
                config: None,
                files:  HashMap::from([
                    (
                        ManifestPath::from_wire("child/validate.fabro").unwrap(),
                        r#"digraph Validate {
                            start [shape=Mdiamond]
                            lint [prompt="@../prompts/lint.md"]
                            exit [shape=Msquare]
                            start -> lint -> exit
                        }"#
                        .to_string(),
                    ),
                    (
                        ManifestPath::from_wire("prompts/lint.md").unwrap(),
                        "Lint {{ goal }}".to_string(),
                    ),
                ]),
            }),
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               PathBuf::from("."),
            custom_transforms: Vec::new(),
        })
        .unwrap();

        validated.raise_on_errors().unwrap();
        assert_eq!(
            validated.graph().nodes["validate.lint"]
                .attrs
                .get("prompt")
                .and_then(AttrValue::as_str),
            Some("Lint Ship")
        );
    }

    #[test]
    fn validate_from_bundle_resolves_minijinja_includes_in_prompt_and_goal_files() {
        let validated = validate(ValidateInput {
            workflow:          WorkflowInput::Bundled(BundledWorkflow {
                path:   ManifestPath::from_wire("workflow.fabro").unwrap(),
                source: r#"digraph Test {
                    graph [goal="@goals/goal.md"]
                    start [shape=Mdiamond]
                    work [prompt="@prompts/work.md"]
                    exit [shape=Msquare]
                    start -> work -> exit
                }"#
                .to_string(),
                config: None,
                files:  HashMap::from([
                    (
                        ManifestPath::from_wire("goals/goal.md").unwrap(),
                        r#"{% include "goal.tpl.md" %}"#.to_string(),
                    ),
                    (
                        ManifestPath::from_wire("goals/goal.tpl.md").unwrap(),
                        "Bundled goal".to_string(),
                    ),
                    (
                        ManifestPath::from_wire("prompts/work.md").unwrap(),
                        r#"{% include "work.tpl.md" %}"#.to_string(),
                    ),
                    (
                        ManifestPath::from_wire("prompts/work.tpl.md").unwrap(),
                        "Bundled prompt".to_string(),
                    ),
                ]),
            }),
            settings:          WorkflowSettings::default(),
            vars:              HashMap::new(),
            cwd:               PathBuf::from("."),
            custom_transforms: Vec::new(),
        })
        .unwrap();

        validated.raise_on_errors().unwrap();
        assert_eq!(validated.graph().goal(), "Bundled goal");
        assert_eq!(
            validated.graph().nodes["work"]
                .attrs
                .get("prompt")
                .and_then(AttrValue::as_str),
            Some("Bundled prompt")
        );
    }

    #[test]
    fn assemble_create_run_persistence_input_resolves_complete_durable_identity() {
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let automation = AutomationRef {
            id:              "nightly".to_string(),
            name:            Some("Nightly".to_string()),
            trigger_id:      Some("schedule_1".to_string()),
            workflow_source: None,
        };
        let request = CreateRunInput {
            workflow: WorkflowInput::DotSource {
                source:   MINIMAL_DOT.to_string(),
                base_dir: None,
            },
            settings: test_default_settings(),
            vars: HashMap::new(),
            cwd: dir.path().to_path_buf(),
            workflow_slug: Some("request-slug".to_string()),
            workflow_path: None,
            workflow_bundle: None,
            target: None,
            submitted_manifest_bytes: Some(b"submitted manifest".to_vec()),
            run_id: Some(fixtures::RUN_1),
            title: Some("Assembled run".to_string()),
            automation: Some(automation.clone()),
            git: None,
            fork_source_ref: None,
            parent_id: Some(fixtures::RUN_2),
            provenance: test_support::test_run_provenance(),
            configured_providers: test_provider_ids(),
            web_url: Some("https://fabro.test/runs/1".to_string()),
        };
        let catalog = test_catalog();
        let resolved_run_id = fixtures::RUN_64;

        let compiled = compile_create_run(compile_input(&request), Arc::clone(&catalog)).unwrap();
        let materialized = materialize_create_run(compiled, catalog.as_ref()).unwrap();
        let metadata = persistence_metadata(&request, resolved_run_id, &storage_root);
        let input = assemble_create_run_persistence_input(materialized, metadata);

        assert_eq!(input.run_id(), resolved_run_id);
        assert_eq!(
            input.run_dir(),
            Storage::new(&storage_root)
                .run_scratch(&resolved_run_id)
                .root()
        );
        assert_eq!(input.workflow_slug(), Some("request-slug"));
        assert_eq!(
            input.submitted_manifest_bytes(),
            Some(b"submitted manifest".as_slice())
        );
        assert_eq!(input.automation(), Some(&automation));
        assert_eq!(
            input.materialized().settings().run.model.name.as_deref(),
            Some("claude-sonnet-5")
        );
    }

    #[test]
    fn compile_create_run_rejects_mismatched_bundle_definition() {
        let workflow_path = ManifestPath::from_wire("workflows/main.fabro").unwrap();
        let compiled_workflow = BundledWorkflow {
            path:   workflow_path.clone(),
            source: MINIMAL_DOT.to_string(),
            config: None,
            files:  HashMap::new(),
        };
        let mismatched_bundle =
            WorkflowBundle::new(HashMap::from([(workflow_path.clone(), BundledWorkflow {
                source: MINIMAL_DOT.replace("Build feature", "Different goal"),
                ..compiled_workflow.clone()
            })]));

        let Err(error) = compile_create_run(
            CreateRunCompileInput {
                workflow:             WorkflowInput::Bundled(compiled_workflow),
                settings:             test_default_settings(),
                vars:                 HashMap::new(),
                cwd:                  PathBuf::from("/tmp/project"),
                workflow_path:        Some(workflow_path),
                workflow_bundle:      Some(mismatched_bundle),
                configured_providers: test_provider_ids(),
            },
            test_catalog(),
        ) else {
            panic!("mismatched accepted definition should fail");
        };

        assert!(matches!(error, Error::Parse(message) if message ==
            "resolved workflow does not match workflow bundle entrypoint"));
    }

    #[test]
    fn compile_create_run_exposes_resolved_metadata_and_definition() {
        let workflow_path = ManifestPath::from_wire("workflows/main.fabro").unwrap();
        let bundled = BundledWorkflow {
            path:   workflow_path.clone(),
            source: MINIMAL_DOT.to_string(),
            config: None,
            files:  HashMap::new(),
        };
        let bundle = WorkflowBundle::new(HashMap::from([(workflow_path.clone(), bundled.clone())]));
        let compiled = compile_create_run(
            CreateRunCompileInput {
                workflow:             WorkflowInput::Bundled(bundled),
                settings:             test_default_settings(),
                vars:                 HashMap::new(),
                cwd:                  PathBuf::from("/tmp/project"),
                workflow_path:        Some(workflow_path.clone()),
                workflow_bundle:      Some(bundle),
                configured_providers: test_provider_ids(),
            },
            test_catalog(),
        )
        .unwrap();

        assert_eq!(compiled.raw_source, MINIMAL_DOT);
        assert_eq!(compiled.dot_path.as_deref(), Some(workflow_path.as_path()));
        assert_eq!(compiled.labels, compiled.settings().combined_labels());
        let materialized = materialize_create_run(compiled, test_catalog().as_ref()).unwrap();
        let input =
            assemble_create_run_persistence_input(materialized, CreateRunPersistenceMetadata {
                run_id: fixtures::RUN_1,
                storage_root: PathBuf::from("/tmp/storage"),
                workflow_slug: None,
                workflow_version_id: None,
                target: None,
                submitted_manifest_bytes: None,
                title: None,
                automation: None,
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                web_url: None,
            });
        let definition = input
            .definition()
            .expect("bundled create input should retain a run definition");
        assert_eq!(definition.workflow_path, workflow_path);
    }

    #[tokio::test]
    async fn persist_create_run_uses_compiled_graph_without_recompiling_source() {
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let dot_path = dir.path().join("workflow.fabro");
        let compiled_source = MINIMAL_DOT.replace("Build feature", "Compiled goal");
        std::fs::write(&dot_path, &compiled_source).unwrap();
        let automation = AutomationRef {
            id:              "nightly".to_string(),
            name:            Some("Nightly".to_string()),
            trigger_id:      Some("schedule_1".to_string()),
            workflow_source: None,
        };
        let request = CreateRunInput {
            workflow: WorkflowInput::Path(dot_path.clone()),
            settings: test_default_settings(),
            vars: HashMap::new(),
            cwd: dir.path().to_path_buf(),
            workflow_slug: Some("compiled-slug".to_string()),
            workflow_path: None,
            workflow_bundle: None,
            target: None,
            submitted_manifest_bytes: Some(b"submitted manifest".to_vec()),
            run_id: Some(fixtures::RUN_2),
            title: Some("Compiled run".to_string()),
            automation: Some(automation.clone()),
            git: None,
            fork_source_ref: None,
            parent_id: None,
            provenance: test_support::test_run_provenance(),
            configured_providers: test_provider_ids(),
            web_url: None,
        };
        let catalog = test_catalog();
        let compiled = compile_create_run(compile_input(&request), Arc::clone(&catalog)).unwrap();

        std::fs::write(&dot_path, "this is no longer a graph").unwrap();

        let materialized = materialize_create_run(compiled, catalog.as_ref()).unwrap();
        let workflow_version_id = test_support::test_workflow_version_id();
        let mut metadata = persistence_metadata(&request, fixtures::RUN_2, &storage_root);
        metadata.workflow_version_id = Some(workflow_version_id);
        let input = assemble_create_run_persistence_input(materialized, metadata);
        let store = memory_store();
        let created = persist_create_run(store.as_ref(), input).await.unwrap();

        assert_eq!(created.run_id, fixtures::RUN_2);
        assert_eq!(created.dot_path.as_deref(), Some(dot_path.as_path()));
        assert_eq!(created.persisted.graph().goal(), "Compiled goal");
        assert_eq!(created.persisted.source(), compiled_source);

        let run_store = store.open_run_reader(&fixtures::RUN_2).await.unwrap();
        let state = run_store.state().await.unwrap();
        assert_eq!(state.spec.graph.goal(), "Compiled goal");
        assert_eq!(state.spec.automation, Some(automation));
        assert_eq!(state.spec.workflow_version_id, Some(workflow_version_id));
        let events = run_store.list_events().await.unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event.event_name())
                .collect::<Vec<_>>(),
            vec!["run.created", "run.submitted"]
        );
        let EventBody::RunCreated(created) = &events[0].event.body else {
            panic!("first durable event should be run.created");
        };
        assert_eq!(
            created.workflow_source.as_deref(),
            Some(compiled_source.as_str())
        );
        assert_eq!(created.workflow_version_id, Some(workflow_version_id));
        let manifest_blob = created
            .manifest_blob
            .as_ref()
            .expect("submitted manifest should be persisted");
        assert_eq!(
            run_store
                .read_blob(manifest_blob)
                .await
                .unwrap()
                .expect("submitted manifest blob should exist")
                .as_ref(),
            b"submitted manifest"
        );
    }

    #[tokio::test]
    async fn create_returns_validation_failed_with_diagnostics() {
        let dot = r#"digraph Test {
            graph [goal="Test"]
            work [label="Work"]
        }"#;
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let store = memory_store();
        let err = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   dot.to_string(),
                    base_dir: None,
                },
                settings: test_default_settings(),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: None,
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: None,
                title: None,
                automation: None,
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root,
            test_catalog(),
        )
        .await
        .unwrap_err();

        match err {
            Error::ValidationFailed { diagnostics } => {
                assert!(!diagnostics.is_empty());
            }
            other => panic!("expected ValidationFailed, got {other:?}"),
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "test asserts the raw template source"
    )]
    #[tokio::test]
    async fn create_persists_normalized_config_and_initial_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let store = memory_store();
        let created = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: settings_from_run_layer({
                    let mut metadata = HashMap::new();
                    metadata.insert("env".to_string(), "test".to_string());
                    RunLayer {
                        goal: Some(RunGoalLayer::Inline(InterpString::parse("override goal"))),
                        metadata: ReplaceMap::from(metadata),
                        model: Some(RunModelLayer {
                            name: Some("sonnet".to_string()),
                            ..RunModelLayer::default()
                        }),
                        pull_request: Some(RunPullRequestLayer {
                            enabled: Some(false),
                            ..RunPullRequestLayer::default()
                        }),
                        execution: Some(RunExecutionLayer {
                            mode: Some(RunMode::DryRun),
                            ..RunExecutionLayer::default()
                        }),
                        ..RunLayer::default()
                    }
                }),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: Some("slug".to_string()),
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_1),
                title: None,
                automation: None,
                git: Some(fabro_types::GitContext {
                    origin_url: String::new(),
                    branch:     "main".to_string(),
                    sha:        None,
                    dirty:      fabro_types::DirtyStatus::Clean,
                }),
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root.clone(),
            test_catalog(),
        )
        .await
        .unwrap();

        assert_eq!(created.run_id, fixtures::RUN_1);
        assert_eq!(created.persisted.run_spec().graph.goal(), "override goal");
        assert_eq!(
            created
                .persisted
                .run_spec()
                .settings
                .run
                .model
                .name
                .as_deref(),
            Some("claude-sonnet-5")
        );
        assert_eq!(
            created
                .persisted
                .run_spec()
                .settings
                .run
                .model
                .provider
                .as_deref(),
            Some("anthropic")
        );
        assert_eq!(
            match &created.persisted.run_spec().settings.run.goal {
                Some(fabro_types::settings::run::RunGoal::Inline(value)) => {
                    Some(value.as_source())
                }
                _ => None,
            }
            .as_deref(),
            Some("override goal")
        );
        assert!(
            created
                .persisted
                .run_spec()
                .settings
                .run
                .pull_request
                .is_none()
        );
        assert_eq!(
            created.persisted.run_spec().workflow_slug.as_deref(),
            Some("slug")
        );
        let run_store = store.open_run(&fixtures::RUN_1).await.unwrap();
        assert_eq!(
            run_store.state().await.unwrap().status,
            crate::run_status::RunStatus::Submitted
        );
        assert_eq!(
            created.run_dir,
            Storage::new(&storage_root)
                .run_scratch(&fixtures::RUN_1)
                .root()
                .to_path_buf()
        );
        assert!(created.run_dir.is_dir());
    }

    #[tokio::test]
    async fn create_materializes_portable_selectors_for_ready_provider_snapshot_and_pin() {
        const MODEL_DOT: &str = r#"digraph Test {
            graph [goal="Test"]
            start [shape=Mdiamond]
            work [prompt="Do work", model="MODEL_SELECTOR"]
            exit [shape=Msquare]
            start -> work -> exit
        }"#;
        let catalog = portable_model_catalog();
        let cases = [
            (vec![ProviderId::openai()], None, ProviderId::openai()),
            (
                vec![ProviderId::new("openrouter")],
                None,
                ProviderId::new("openrouter"),
            ),
            (
                vec![ProviderId::openai(), ProviderId::new("openrouter")],
                None,
                ProviderId::openai(),
            ),
            (
                vec![ProviderId::openai(), ProviderId::new("openrouter")],
                Some("openrouter"),
                ProviderId::new("openrouter"),
            ),
        ];

        for selector in ["gpt-56-sol", "openai/gpt-5.6-sol"] {
            for (ready, explicit_provider, expected_provider) in &cases {
                let dir = tempfile::tempdir().unwrap();
                let mut settings = test_default_settings();
                settings.run.model.name = Some(selector.to_string());
                settings.run.model.provider = explicit_provider.map(str::to_string);
                let store = memory_store();
                let created = create(
                    store.as_ref(),
                    CreateRunInput {
                        workflow: WorkflowInput::DotSource {
                            source:   MODEL_DOT.replace("MODEL_SELECTOR", selector),
                            base_dir: None,
                        },
                        settings,
                        vars: HashMap::new(),
                        cwd: dir.path().to_path_buf(),
                        workflow_slug: None,
                        workflow_path: None,
                        workflow_bundle: None,
                        target: None,
                        submitted_manifest_bytes: None,
                        run_id: None,
                        title: None,
                        automation: None,
                        git: None,
                        fork_source_ref: None,
                        parent_id: None,
                        provenance: test_support::test_run_provenance(),
                        configured_providers: ready.clone(),
                        web_url: None,
                    },
                    dir.path().join("storage"),
                    Arc::clone(&catalog),
                )
                .await
                .unwrap();
                let run_spec = created.persisted.run_spec();

                assert_eq!(
                    run_spec.settings.run.model.name.as_deref(),
                    Some("gpt-5.6-sol"),
                    "{selector}"
                );
                assert_eq!(
                    run_spec.settings.run.model.provider.as_deref(),
                    Some(expected_provider.as_str()),
                    "{selector}"
                );
                assert_eq!(
                    run_spec.graph.nodes["work"]
                        .attrs
                        .get("model")
                        .and_then(AttrValue::as_str),
                    Some("gpt-5.6-sol"),
                    "{selector}"
                );
                assert_eq!(
                    run_spec.graph.nodes["work"]
                        .attrs
                        .get("provider")
                        .and_then(AttrValue::as_str),
                    Some(expected_provider.as_str()),
                    "{selector}"
                );

                let run_store = store.open_run(&created.run_id).await.unwrap();
                let run_store = run_store.into();
                let reloaded = Persisted::load_from_store(&run_store, &created.run_dir)
                    .await
                    .unwrap();
                assert_eq!(
                    reloaded.run_spec().settings.run.model.provider.as_deref(),
                    Some(expected_provider.as_str()),
                    "{selector}"
                );
                assert_eq!(
                    reloaded.run_spec().graph.nodes["work"]
                        .attrs
                        .get("provider")
                        .and_then(AttrValue::as_str),
                    Some(expected_provider.as_str()),
                    "{selector}"
                );
                assert!(
                    reloaded.source().contains(selector),
                    "persisted source should preserve the user's selector '{selector}'"
                );
            }
        }
    }

    #[tokio::test]
    async fn create_persists_secret_tokens_in_run_created_settings_source_form() {
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let store = memory_store();
        let created = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: settings_from_run_layer(RunLayer {
                    prepare: Some(RunPrepareLayer {
                        steps:   vec![PrepareStep {
                            script:  None,
                            command: Some(vec![
                                InterpString::parse("deploy"),
                                InterpString::parse("{{ secrets.DEPLOY_TOKEN }}"),
                            ]),
                            env:     HashMap::from([(
                                "DEPLOY_TOKEN".to_string(),
                                InterpString::parse("{{ secrets.DEPLOY_TOKEN }}"),
                            )]),
                        }],
                        timeout: None,
                    }),
                    execution: Some(RunExecutionLayer {
                        mode: Some(RunMode::DryRun),
                        ..RunExecutionLayer::default()
                    }),
                    ..RunLayer::default()
                }),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: Some("secret-source".to_string()),
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_1),
                title: None,
                automation: None,
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root,
            test_catalog(),
        )
        .await
        .unwrap();

        let run_store = store.open_run(&created.run_id).await.unwrap();
        let events = run_store.list_events().await.unwrap();
        let run_created = events
            .iter()
            .find_map(|event| match &event.event.body {
                EventBody::RunCreated(props) => Some(props),
                _ => None,
            })
            .expect("run.created event should be persisted");
        let step = run_created
            .settings
            .run
            .prepare
            .steps
            .first()
            .expect("prepare step should be persisted");

        let fabro_types::settings::run::PreparedStepRun::Command { command } = &step.run else {
            panic!("expected command prepare step");
        };
        assert_eq!(command, &vec![
            "deploy".to_string(),
            "{{ secrets.DEPLOY_TOKEN }}".to_string()
        ]);
        assert_eq!(
            step.env.get("DEPLOY_TOKEN").map(String::as_str),
            Some("{{ secrets.DEPLOY_TOKEN }}")
        );
    }

    #[tokio::test]
    async fn create_persists_submitter_source_directory_from_request_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let storage_root = dir.path().join("storage");

        let store = memory_store();
        let created = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: settings_from_run_layer({
                    RunLayer {
                        working_dir: Some("workspace".to_string()),
                        execution: Some(RunExecutionLayer {
                            mode: Some(RunMode::DryRun),
                            ..RunExecutionLayer::default()
                        }),
                        ..RunLayer::default()
                    }
                }),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: None,
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_2),
                title: None,
                automation: None,
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root,
            test_catalog(),
        )
        .await
        .unwrap();

        assert_eq!(
            created.persisted.run_spec().source_directory.as_deref(),
            Some(workspace.to_string_lossy().as_ref())
        );
    }

    #[tokio::test]
    async fn create_none_target_omits_the_source_directory_projection() {
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let store = memory_store();
        let created = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: test_default_settings(),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: None,
                workflow_path: None,
                workflow_bundle: None,
                target: Some(RunTarget::None {}),
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_2),
                title: None,
                automation: None,
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root,
            test_catalog(),
        )
        .await
        .unwrap();

        assert_eq!(created.persisted.run_spec().source_directory, None);
        assert_eq!(
            created.persisted.run_spec().target,
            Some(RunTarget::None {})
        );
        assert_eq!(created.persisted.run_spec().git, None);
    }

    #[tokio::test]
    async fn create_folder_target_projects_its_path_over_the_compiler_directory() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let canonical = workspace
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let storage_root = dir.path().join("storage");
        let store = memory_store();
        let git = fabro_types::GitContext {
            origin_url: "https://github.com/fabro-sh/fabro".to_string(),
            branch:     "main".to_string(),
            sha:        None,
            dirty:      fabro_types::DirtyStatus::Clean,
        };
        let created = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: test_default_settings(),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: None,
                workflow_path: None,
                workflow_bundle: None,
                target: Some(RunTarget::Folder {
                    path: canonical.clone(),
                }),
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_2),
                title: None,
                automation: None,
                git: Some(git.clone()),
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root,
            test_catalog(),
        )
        .await
        .unwrap();

        assert_eq!(
            created.persisted.run_spec().target,
            Some(RunTarget::Folder {
                path: canonical.clone(),
            })
        );
        assert_eq!(
            created.persisted.run_spec().source_directory.as_deref(),
            Some(canonical.as_str())
        );
        assert_eq!(created.persisted.run_spec().git, Some(git));
    }

    #[tokio::test]
    async fn create_persists_repo_origin_url_from_request() {
        let dir = tempfile::tempdir().unwrap();
        let storage_root = dir.path().join("storage");
        let store = memory_store();
        let created = create(
            &store,
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: dry_run_only_settings(),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: None,
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_2),
                title: None,
                automation: None,
                git: Some(fabro_types::GitContext {
                    origin_url: "https://github.com/acme/widgets".to_string(),
                    branch:     String::new(),
                    sha:        None,
                    dirty:      fabro_types::DirtyStatus::Clean,
                }),
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_root,
            test_catalog(),
        )
        .await
        .unwrap();

        assert_eq!(
            created.persisted.run_spec().repo_origin_url(),
            Some("https://github.com/acme/widgets")
        );
    }

    fn dry_run_only_settings() -> WorkflowSettings {
        settings_from_run_layer(RunLayer {
            execution: Some(RunExecutionLayer {
                mode: Some(RunMode::DryRun),
                ..RunExecutionLayer::default()
            }),
            ..RunLayer::default()
        })
    }

    fn dry_run_with_storage(_storage_dir: &Path) -> WorkflowSettings {
        settings_from_run_layer(RunLayer {
            execution: Some(RunExecutionLayer {
                mode: Some(RunMode::DryRun),
                ..RunExecutionLayer::default()
            }),
            ..RunLayer::default()
        })
    }

    #[tokio::test]
    async fn create_hydrates_run_created_event_into_store() {
        let dir = tempfile::tempdir().unwrap();
        let storage_dir = dir.path().join("storage");
        std::fs::create_dir_all(storage_dir.join("store")).unwrap();
        let object_store =
            Arc::new(LocalFileSystem::new_with_prefix(storage_dir.join("store")).unwrap());
        let store = Arc::new(fabro_store::test_support::test_database(
            object_store,
            "",
            Duration::from_millis(1),
            None,
        ));
        let automation = fabro_types::AutomationRef {
            id:              "nightly".to_string(),
            name:            Some("Nightly".to_string()),
            trigger_id:      Some("schedule_1".to_string()),
            workflow_source: None,
        };
        let created = create(
            store.as_ref(),
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: dry_run_with_storage(&storage_dir),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: Some("slug".to_string()),
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_3),
                title: None,
                automation: Some(automation.clone()),
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: test_support::test_run_provenance(),
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_dir.clone(),
            test_catalog(),
        )
        .await
        .unwrap();
        let run_store = store.open_run_reader(&created.run_id).await.unwrap();
        let events = run_store.list_events().await.unwrap();
        let state = run_store.state().await.unwrap();

        assert_eq!(events.first().unwrap().event.event_name(), "run.created");
        assert_eq!(
            created.persisted.run_spec().automation,
            Some(automation.clone())
        );
        assert_eq!(state.spec.automation, Some(automation));
    }

    #[tokio::test]
    async fn create_hydrates_provenance_into_store_state() {
        let dir = tempfile::tempdir().unwrap();
        let storage_dir = dir.path().join("storage");
        std::fs::create_dir_all(storage_dir.join("store")).unwrap();
        let object_store =
            Arc::new(LocalFileSystem::new_with_prefix(storage_dir.join("store")).unwrap());
        let store = Arc::new(fabro_store::test_support::test_database(
            object_store,
            "",
            Duration::from_millis(1),
            None,
        ));
        let created = create(
            store.as_ref(),
            CreateRunInput {
                workflow: WorkflowInput::DotSource {
                    source:   MINIMAL_DOT.to_string(),
                    base_dir: None,
                },
                settings: dry_run_with_storage(&storage_dir),
                vars: HashMap::new(),
                cwd: dir.path().to_path_buf(),
                workflow_slug: Some("slug".to_string()),
                workflow_path: None,
                workflow_bundle: None,
                target: None,
                submitted_manifest_bytes: None,
                run_id: Some(fixtures::RUN_64),
                title: None,
                automation: None,
                git: None,
                fork_source_ref: None,
                parent_id: None,
                provenance: fabro_types::RunProvenance {
                    server:  Some(fabro_types::RunServerProvenance {
                        version: "0.9.0".to_string(),
                    }),
                    client:  Some(fabro_types::RunClientProvenance {
                        user_agent: Some("fabro-cli/0.9.0".to_string()),
                        name:       Some("fabro-cli".to_string()),
                        version:    Some("0.9.0".to_string()),
                    }),
                    subject: fabro_types::Principal::user(
                        fabro_types::IdpIdentity::new("https://github.com", "12345").unwrap(),
                        "octocat".to_string(),
                        fabro_types::AuthMethod::Github,
                    ),
                },
                configured_providers: test_provider_ids(),
                web_url: None,
            },
            storage_dir,
            test_catalog(),
        )
        .await
        .unwrap();

        let run_store = store.open_run_reader(&created.run_id).await.unwrap();
        let state = run_store.state().await.unwrap();
        let run = state.spec;
        let provenance = run.provenance;

        assert_eq!(provenance.server.unwrap().version, "0.9.0");
        assert_eq!(
            provenance.client.unwrap().name.as_deref(),
            Some("fabro-cli")
        );
        assert_eq!(
            provenance.subject,
            fabro_types::Principal::user(
                fabro_types::IdpIdentity::new("https://github.com", "12345").unwrap(),
                "octocat".to_string(),
                fabro_types::AuthMethod::Github,
            )
        );
    }
}
