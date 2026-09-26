//! Native project endpoints: connect an existing GitHub repository, rename it,
//! and enroll global automations as concrete project-owned instances.

use std::sync::Arc;

use axum::http::HeaderMap;
use fabro_api::types::{
    CreateProjectAutomationRequest, CreateProjectRequest, UpdateProjectRequest,
};
use fabro_automation::{
    Automation, AutomationDraft, AutomationId, GithubRepositoryId, Project, ProjectDraft,
    ProjectId, ProjectReplace, ProjectStoreError,
};

use super::super::{
    ApiError, AppState, IntoResponse, Json, Path, RequiredUser, Response, Router, State,
    StatusCode, get, post,
};
use super::automations::resolve_automation_environment;
use super::{json_with_etag_response, parse_required_if_match};

#[derive(serde::Serialize)]
struct ProjectListResponse {
    data: Vec<Project>,
    meta: ProjectListMeta,
}

#[derive(serde::Serialize)]
struct ProjectListMeta {
    total: usize,
}

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/projects", get(list_projects).post(create_project))
        .route("/projects/{id}", get(get_project).put(update_project))
        .route(
            "/projects/{id}/automations",
            post(create_project_automation),
        )
}

async fn list_projects(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    let data = state.project_store().list().await?;
    let total = data.len();
    Ok((
        StatusCode::OK,
        Json(ProjectListResponse {
            data,
            meta: ProjectListMeta { total },
        }),
    )
        .into_response())
}

async fn create_project(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateProjectRequest>,
) -> Result<Response, ApiError> {
    let id = ProjectId::new(String::from(request.id.clone()))
        .map_err(|err| ApiError::bad_request(format!("invalid project id: {err}")))?;
    let slug = request.repository.clone();
    let summary = super::github_repositories::read_repository(state.as_ref(), &slug).await?;
    let Some(default_branch) = summary.default_branch.clone() else {
        return Err(ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("{slug} has no default branch yet; push an initial commit, then connect it"),
            "project_repository_empty",
        ));
    };
    let draft = ProjectDraft {
        id,
        name: request.name.clone(),
        github_repository_id: GithubRepositoryId::new(summary.id.to_string())
            .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?,
        repository: summary.full_name.clone(),
        default_branch,
    };
    let project = state.project_store().create(draft).await?;
    let revision = project.revision.clone();
    Ok(json_with_etag_response(
        StatusCode::CREATED,
        "project",
        &revision,
        project,
    ))
}

async fn get_project(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse_project_id(id)?;
    match state.project_store().get(&id).await? {
        Some(project) => {
            let revision = project.revision.clone();
            Ok(json_with_etag_response(
                StatusCode::OK,
                "project",
                &revision,
                project,
            ))
        }
        None => Err(ApiError::not_found(format!("project not found: {id}"))),
    }
}

async fn update_project(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateProjectRequest>,
) -> Result<Response, ApiError> {
    let id = parse_project_id(id)?;
    let expected = parse_required_if_match(&headers, "project", &id)?;
    let project = state
        .project_store()
        .rename(&id, &expected, ProjectReplace {
            name: request.name.clone(),
        })
        .await?;
    let revision = project.revision.clone();
    Ok(json_with_etag_response(
        StatusCode::OK,
        "project",
        &revision,
        project,
    ))
}

/// Select an available global automation and create a validated link owned
/// by this project.
///
/// The link keeps the source definition's workflow selector and workflow
/// source, targets this project's repository and default branch, inherits no
/// trigger state, and carries no other project's Plane ids.
async fn create_project_automation(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<CreateProjectAutomationRequest>,
) -> Result<Response, ApiError> {
    let project_id = parse_project_id(id)?;
    let Some(project) = state.project_store().get(&project_id).await? else {
        return Err(ApiError::not_found(format!(
            "project not found: {project_id}"
        )));
    };

    let instance_id = AutomationId::new(String::from(request.id.clone()))
        .map_err(|err| ApiError::bad_request(format!("invalid automation id: {err}")))?;
    let source_id = AutomationId::new(String::from(request.source_automation_id.clone()))
        .map_err(|err| ApiError::bad_request(format!("invalid automation id: {err}")))?;
    let source_revision = String::from(request.source_revision.clone())
        .parse::<fabro_automation::AutomationRevision>()
        .map_err(|_| ApiError::bad_request("invalid source automation revision"))?;

    // An existing instance with this id is only reused when it is the exact
    // retry: same project, same source definition and revision, same
    // environment. Anything else is a conflict, never a silent overwrite.
    if let Some(existing) = state.automation_store().get(&instance_id).await? {
        return retry_or_conflict(
            &state,
            existing,
            &project,
            &source_id,
            &source_revision,
            &request,
        )
        .await;
    }

    let Some(source) = state.automation_store().get(&source_id).await? else {
        return Err(ApiError::not_found(format!(
            "automation not found: {source_id}"
        )));
    };
    validate_link_source(&source)?;
    if source.revision != source_revision {
        return Err(ApiError::with_code(
            StatusCode::CONFLICT,
            format!(
                "automation {source_id} changed since it was selected; reload the definition and retry"
            ),
            "automation_source_revision_stale",
        ));
    }

    let environment_id = resolve_automation_environment(
        state.as_ref(),
        Some(request.environment_id.as_str()),
        StatusCode::UNPROCESSABLE_ENTITY,
    )?;
    let draft =
        project_automation_draft(&instance_id, &request, &project, &source, environment_id)?;
    if !workflow_declares_project_input(state.as_ref(), &draft).await? {
        return Err(ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "workflow {} declares no `project` input; add `project = {{ type = \"string\", default = \"\" }}` to its [run.inputs] first",
                draft.workflow
            ),
            "automation_workflow_without_project_input",
        ));
    }
    let automation = state.automation_store().create(draft).await?;
    state.notify_automation_scheduler();
    let revision = automation.revision.clone();
    Ok(json_with_etag_response(
        StatusCode::CREATED,
        "automation",
        &revision,
        automation,
    ))
}

async fn retry_or_conflict(
    state: &Arc<AppState>,
    existing: Automation,
    project: &Project,
    source_id: &AutomationId,
    source_revision: &fabro_automation::AutomationRevision,
    request: &CreateProjectAutomationRequest,
) -> Result<Response, ApiError> {
    let environment_id = resolve_automation_environment(
        state.as_ref(),
        Some(request.environment_id.as_str()),
        StatusCode::UNPROCESSABLE_ENTITY,
    )?;
    let Some(source) = state.automation_store().get(source_id).await? else {
        return Err(ApiError::not_found(format!(
            "automation not found: {source_id}"
        )));
    };
    // A retry naming a different source revision is a stale selection, not an
    // exact retry: the caller decided on a definition that no longer exists.
    if &source.revision != source_revision {
        return Err(ApiError::with_code(
            StatusCode::CONFLICT,
            format!(
                "automation {source_id} changed since it was selected; reload the definition and retry"
            ),
            "automation_source_revision_stale",
        ));
    }
    let draft = project_automation_draft(&existing.id, request, project, &source, environment_id)?;
    // The draft is already canonical (project target, source workflow
    // selector, empty triggers), so an exact retry compares field by field.
    let identical = existing.project_id.as_ref() == Some(&project.id)
        && existing.name == draft.name
        && existing.description == draft.description
        && existing.environment_id == draft.environment_id
        && existing.target == draft.target
        && existing.workflow == draft.workflow
        && existing.workflow_source == draft.workflow_source
        && existing.triggers.is_empty()
        && existing.source_automation_id == draft.source_automation_id;
    if identical {
        let revision = existing.revision.clone();
        return Ok(json_with_etag_response(
            StatusCode::OK,
            "automation",
            &revision,
            existing,
        ));
    }
    Err(ApiError::with_code(
        StatusCode::CONFLICT,
        format!(
            "automation {} already exists with different configuration; choose another id",
            existing.id
        ),
        "automation_id_conflict",
    ))
}

/// Build the project-owned link from the selected global definition.
///
/// The application target always comes from the project (repository and
/// default branch), never from the source definition or its tag/SHA. The
/// workflow source is preserved: an explicit source stays authoritative, and
/// otherwise the source's original Git target keeps central workflows loading
/// from the central repository while executing against the project.
fn project_automation_draft(
    instance_id: &AutomationId,
    request: &CreateProjectAutomationRequest,
    project: &Project,
    source: &Automation,
    environment_id: String,
) -> Result<AutomationDraft, ApiError> {
    if project.intake_binding_id.is_none() {
        return Err(ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "project {} has no registry binding, so it cannot receive inputs.project",
                project.id
            ),
            "project_not_registered",
        ));
    }
    let source_target = source.git_target().ok_or_else(|| {
        ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("automation {} is not Git-backed", source.id),
            "automation_source_not_git",
        )
    })?;
    let workflow_source = source
        .workflow_source
        .clone()
        .unwrap_or_else(|| source_target.clone());
    Ok(AutomationDraft {
        id:                    instance_id.clone(),
        name:                  request.name.clone(),
        description:           request.description.clone(),
        environment_id:        Some(environment_id),
        target:                fabro_types::RunTarget::Git(fabro_types::GitRunTarget {
            repo:   project.repository.clone(),
            branch: project.default_branch.clone(),
            tag:    None,
            sha:    None,
        }),
        workflow:              source.workflow.clone(),
        workflow_source:       Some(workflow_source),
        project_id:            Some(project.id.clone()),
        available_to_projects: false,
        source_automation_id:  Some(source.id.clone()),
        // A newly enrolled instance never inherits trigger activation.
        triggers:              Vec::new(),
    })
}

/// A link source must be a global definition an operator made available.
pub(crate) fn validate_link_source(source: &Automation) -> Result<(), ApiError> {
    if source.project_id.is_some() {
        return Err(ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("automation {} is already owned by a project", source.id),
            "automation_source_not_global",
        ));
    }
    if !source.available_to_projects {
        return Err(ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "automation {} is not available to projects; enable it on the global definition first",
                source.id
            ),
            "automation_not_available_to_projects",
        ));
    }
    Ok(())
}

/// Materialize the source workflow for this project and read its declared
/// inputs. A link whose workflow cannot receive `inputs.project` would act
/// on every project, so it is refused.
pub(crate) async fn workflow_declares_project_input(
    state: &AppState,
    draft: &AutomationDraft,
) -> Result<bool, ApiError> {
    let fabro_types::RunTarget::Git(target) = &draft.target else {
        return Ok(false);
    };
    let materialized = state
        .materialize_automation_run(crate::automation_materializer::AutomationRunMaterializeInput {
            automation_id:   draft.id.clone(),
            target:          target.clone(),
            workflow_source: draft.workflow_source.clone(),
            workflow:        draft.workflow.clone(),
            run_id:          fabro_types::RunId::new(),
            temp_root:       state.automation_temp_root(),
            project_id:      draft.project_id.clone(),
        })
        .await
        .map_err(|err| {
            ApiError::with_code(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("workflow {} could not be loaded for this project: {err}", draft.workflow),
                "automation_workflow_unavailable",
            )
        })?;
    let versions = fabro_workflow_version::WorkflowVersionStore::new(state.store_ref().blobs());
    let internal = || ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "workflow version store failed");
    let closure = versions
        .get_closure(&materialized.workflow_version_id)
        .await
        .map_err(|_| internal())?
        .ok_or_else(internal)?;
    let lowered = crate::run_intent::lower_workflow_closure(&closure).map_err(|err| {
        ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("workflow {} is invalid: {err}", draft.workflow),
            "automation_workflow_unavailable",
        )
    })?;
    Ok(lowered
        .workflow_layer
        .as_ref()
        .and_then(|layer| layer.run.as_ref())
        .and_then(|run| run.inputs.as_ref())
        .is_some_and(|inputs| inputs.contains_key("project")))
}

fn parse_project_id(id: String) -> Result<ProjectId, ApiError> {
    ProjectId::new(id).map_err(|err| ApiError::bad_request(format!("invalid project id: {err}")))
}

impl From<ProjectStoreError> for ApiError {
    fn from(err: ProjectStoreError) -> Self {
        match err {
            ProjectStoreError::NotFound { id } => {
                Self::not_found(format!("project not found: {id}"))
            }
            ProjectStoreError::AlreadyExists { id } => Self::new(
                StatusCode::CONFLICT,
                format!("project already exists: {id}"),
            ),
            ProjectStoreError::RepositoryConflict {
                repository,
                existing_project_id,
            } => {
                let existing = existing_project_id
                    .map_or_else(|| "<unknown>".to_string(), |id| id.to_string());
                Self::with_code(
                    StatusCode::CONFLICT,
                    format!("repository {repository} is already connected as project {existing}"),
                    "project_repository_connected",
                )
            }
            ProjectStoreError::IntakeBindingConflict { .. } => Self::with_code(
                StatusCode::CONFLICT,
                "that feature-intake binding is already attached to another project",
                "project_intake_binding_conflict",
            ),
            ProjectStoreError::StaleRevision { id, .. } => Self::new(
                StatusCode::CONFLICT,
                format!("project revision is stale: {id}"),
            ),
            ProjectStoreError::Validation { source } => {
                Self::new(StatusCode::UNPROCESSABLE_ENTITY, source.to_string())
            }
            err => {
                tracing::error!(error = ?err, "Project store operation failed");
                Self::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "project store operation failed",
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project() -> Project {
        Project {
            id:                   ProjectId::new("tierrapay").unwrap(),
            revision:             fabro_automation::Project::revision_for(
                "TierraPay",
                "42",
                "artesanos-digitales/tierrapay",
                "main",
                Some("tierrapay"),
            ),
            name:                 "TierraPay".to_string(),
            github_repository_id: GithubRepositoryId::new("42").unwrap(),
            repository:           "artesanos-digitales/tierrapay".to_string(),
            default_branch:       "main".to_string(),
            intake_binding_id:    Some("tierrapay".to_string()),
        }
    }

    fn source(triggers: Vec<fabro_automation::AutomationTrigger>) -> Automation {
        Automation {
            id: AutomationId::new("ticket-loop").unwrap(),
            revision: fabro_automation::AutomationRevision::from_bytes(b"ticket-loop"),
            name: "Ticket loop".to_string(),
            description: None,
            environment_id: Some("intake-author".to_string()),
            last_error: None,
            target: fabro_types::RunTarget::Git(fabro_types::GitRunTarget {
                repo:   "fabro-sh/fabro".to_string(),
                branch: "main".to_string(),
                tag:    Some("v1".to_string()),
                sha:    None,
            }),
            workflow: "ticket".to_string(),
            workflow_source: None,
            project_id: None,
            available_to_projects: true,
            source_automation_id: None,
            triggers,
        }
    }

    fn request() -> CreateProjectAutomationRequest {
        serde_json::from_value(serde_json::json!({
            "id": "tierrapay-ticket-loop",
            "name": "TierraPay ticket loop",
            "source_automation_id": "ticket-loop",
            "source_revision": "0".repeat(64),
            "environment_id": "intake-author"
        }))
        .expect("request shape matches the API schema")
    }

    #[test]
    fn enrollment_keeps_the_central_workflow_source_and_drops_source_coordinates() {
        let project = project();
        let source = source(vec![fabro_automation::AutomationTrigger::Schedule(
            fabro_automation::ScheduleTrigger {
                id:         fabro_automation::AutomationTriggerId::new("nightly").unwrap(),
                enabled:    true,
                expression: "0 3 * * *".to_string(),
            },
        )]);
        let id = AutomationId::new("tierrapay-ticket-loop").unwrap();
        let draft = project_automation_draft(
            &id,
            &request(),
            &project,
            &source,
            "intake-author".to_string(),
        )
        .expect("the project instance drafts");

        assert_eq!(
            draft.project_id.as_ref().map(ProjectId::as_str),
            Some("tierrapay")
        );
        assert!(!draft.available_to_projects);
        assert!(draft.triggers.is_empty());
        let Some(fabro_types::RunTarget::Git(target)) = Some(&draft.target) else {
            panic!("project instances target Git");
        };
        assert_eq!(target.repo, "artesanos-digitales/tierrapay");
        assert_eq!(target.branch, "main");
        assert!(target.tag.is_none());
        assert!(target.sha.is_none());
        let workflow_source = draft.workflow_source.expect("workflow source is preserved");
        assert_eq!(workflow_source.repo, "fabro-sh/fabro");
        assert_eq!(workflow_source.tag.as_deref(), Some("v1"));
    }

    #[test]
    fn link_draft_names_its_source() {
        let id = AutomationId::new("tierrapay-ticket-loop").unwrap();
        let draft = project_automation_draft(
            &id,
            &request(),
            &project(),
            &source(Vec::new()),
            "intake-author".to_string(),
        )
        .expect("the link drafts");
        assert_eq!(
            draft.source_automation_id.as_ref().map(AutomationId::as_str),
            Some("ticket-loop")
        );
    }

    #[test]
    fn unregistered_projects_cannot_link() {
        let mut project = project();
        project.intake_binding_id = None;
        let id = AutomationId::new("tierrapay-ticket-loop").unwrap();
        let error = project_automation_draft(
            &id,
            &request(),
            &project,
            &source(Vec::new()),
            "intake-author".to_string(),
        )
        .expect_err("a project without a registry binding cannot receive inputs.project");
        assert_eq!(error.code(), Some("project_not_registered"));
    }

    #[test]
    fn only_available_global_sources_link() {
        let mut unavailable = source(Vec::new());
        unavailable.available_to_projects = false;
        assert_eq!(
            validate_link_source(&unavailable).unwrap_err().code(),
            Some("automation_not_available_to_projects")
        );
        let mut owned = source(Vec::new());
        owned.project_id = Some(ProjectId::new("mafeva").unwrap());
        assert_eq!(
            validate_link_source(&owned).unwrap_err().code(),
            Some("automation_source_not_global")
        );
        assert!(validate_link_source(&source(Vec::new())).is_ok());
    }

    async fn declares_project(workflow_toml: &str) -> bool {
        let target = fabro_types::GitRunTarget {
            repo:   "artesanos-digitales/tierrapay".to_string(),
            branch: "main".to_string(),
            tag:    None,
            sha:    Some("0123456789abcdef0123456789abcdef01234567".to_string()),
        };
        let state = crate::test_support::TestAppStateBuilder::new()
            .automation_materializer(
                crate::automation_materializer::TestAutomationRunMaterializer::succeed_with_workflow_toml(
                    target,
                    workflow_toml,
                ),
            )
            .build();
        let created = state
            .project_store()
            .create(fabro_automation::ProjectDraft {
                id:                   ProjectId::new("tierrapay").unwrap(),
                name:                 "TierraPay".to_string(),
                github_repository_id: GithubRepositoryId::new("42").unwrap(),
                repository:           "artesanos-digitales/tierrapay".to_string(),
                default_branch:       "main".to_string(),
            })
            .await
            .unwrap();
        let id = AutomationId::new("tierrapay-ticket-loop").unwrap();
        let draft = project_automation_draft(
            &id,
            &request(),
            &project(),
            &source(Vec::new()),
            "intake-author".to_string(),
        )
        .unwrap();
        assert_eq!(draft.project_id.as_ref(), Some(&created.id));
        workflow_declares_project_input(state.as_ref(), &draft)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn links_require_a_declared_project_input() {
        assert!(
            declares_project(
                "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n[run.inputs]\nproject = { type = \"string\", default = \"\" }\n"
            )
            .await
        );
        assert!(
            !declares_project("_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n").await
        );
    }
}
