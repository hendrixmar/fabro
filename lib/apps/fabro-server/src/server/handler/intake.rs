//! Native feature-intake endpoints.
//!
//! Every read and write requires an authenticated user. The project is always
//! selected from the database, its registered intake binding is resolved, and
//! the binding's repository identity is verified against the project before
//! the bridge is called. Actor and display identity are derived server-side:
//! the browser never supplies them, and no browser request reaches the bridge
//! socket directly.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderMap;
use fabro_api::types::{
    IntakeActionResponse, IntakeBindingSummary, IntakeChatRequest, IntakeCommentRequest,
    IntakeCreateRequest, IntakeCreateResponse, IntakeExecutionSetupRequest, IntakeImportResponse,
    IntakeImportRow, IntakeImportRowStatus, IntakeInitiativeDetail, IntakeInitiativeSummary,
    IntakeOkResponse, IntakePauseRequest, IntakePauseStatus, IntakeReadinessReport,
    IntakeReviseRequest, IntakeRunRecord, IntakeSetupRequest, IntakeSetupStatus,
    IntakeStageRequest, IntakeSupervisedRequest, IntakeTemplate,
};
use fabro_automation::{GithubRepositoryId, Project, ProjectDraft, ProjectId, ProjectStoreError};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::super::{
    ApiError, AppState, IntoResponse, Json, Path, RequiredUser, Response, Router, State,
    StatusCode, get, post, put,
};
use super::{github_repositories, json_with_etag_response, parse_required_if_match};
use crate::intake_bridge::{
    IntakeActor, IntakeBridge, IntakeBridgeError, MAX_CHAT_MESSAGE_BYTES, MAX_MUTATION_BODY_BYTES,
};

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/projects/import-intake", post(import_intake_bindings))
        .route(
            "/projects/{id}/intake",
            get(get_intake).delete(detach_intake),
        )
        .route("/projects/{id}/intake/setup", post(setup_intake))
        .route(
            "/projects/{id}/intake/execution",
            put(configure_intake_execution),
        )
        .route("/projects/{id}/intake/template", get(get_template))
        .route(
            "/projects/{id}/intake/initiatives",
            get(list_initiatives).post(create_initiative),
        )
        .route(
            "/projects/{id}/intake/readiness/recheck",
            post(recheck_readiness),
        )
        .route("/projects/{id}/intake/pause", post(set_pause))
        .route(
            "/projects/{id}/intake/initiatives/{issue}",
            get(get_initiative),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/history",
            get(get_history),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/comments",
            post(post_comment),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/approve",
            post(approve_initiative),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/revise",
            post(revise_initiative),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/cancel",
            post(cancel_initiative),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/run",
            post(run_stage),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/reconcile",
            post(reconcile_initiative),
        )
        .route(
            "/projects/{id}/intake/initiatives/{issue}/execute",
            post(execute_initiative),
        )
        .route(
            "/projects/{id}/intake/chat/{key}",
            post(stream_chat)
                .delete(close_chat)
                .layer(DefaultBodyLimit::max(MAX_CHAT_MESSAGE_BYTES)),
        )
        .layer(DefaultBodyLimit::max(MAX_MUTATION_BODY_BYTES))
}

impl From<IntakeBridgeError> for ApiError {
    fn from(error: IntakeBridgeError) -> Self {
        match error {
            IntakeBridgeError::Disabled => Self::with_code(
                StatusCode::SERVICE_UNAVAILABLE,
                "feature intake is not configured on this server",
                "intake_not_configured",
            ),
            IntakeBridgeError::Unreachable => Self::with_code(
                StatusCode::SERVICE_UNAVAILABLE,
                "the feature-intake bridge is unreachable",
                "intake_bridge_unreachable",
            ),
            IntakeBridgeError::Timeout => Self::with_code(
                StatusCode::SERVICE_UNAVAILABLE,
                "the feature-intake bridge did not answer in time; reconcile before retrying",
                "intake_bridge_timeout",
            ),
            IntakeBridgeError::Refused(detail) => {
                Self::with_code(StatusCode::CONFLICT, detail, "intake_refused")
            }
            IntakeBridgeError::Payload => Self::with_code(
                StatusCode::BAD_GATEWAY,
                "the feature-intake bridge returned a response this server could not read",
                "intake_bridge_payload",
            ),
            IntakeBridgeError::Failed => Self::with_code(
                StatusCode::BAD_GATEWAY,
                "the feature-intake bridge failed; no effect is assumed",
                "intake_bridge_failed",
            ),
        }
    }
}

/// A project plus the bridge binding that serves it.
struct BoundProject {
    project: Project,
    binding: String,
    bridge:  IntakeBridge,
    actor:   IntakeActor,
}

async fn load_bound_project(
    state: &AppState,
    principal: &fabro_types::UserPrincipal,
    id: String,
) -> Result<BoundProject, ApiError> {
    let id = parse_project_id(&id)?;
    let Some(project) = state.project_store().get(&id).await? else {
        return Err(ApiError::not_found(format!("project not found: {id}")));
    };
    let Some(binding) = project.intake_binding_id.clone() else {
        return Err(setup_required(&project.id));
    };
    let bridge = state.intake_bridge()?;
    let actor = IntakeActor::from_principal(principal);
    verify_binding_identity(&bridge, &project, &binding, &actor).await?;
    Ok(BoundProject {
        project,
        binding,
        bridge,
        actor,
    })
}

fn setup_required(project_id: &ProjectId) -> ApiError {
    ApiError::with_code(
        StatusCode::CONFLICT,
        format!(
            "feature intake is not set up for project {project_id}; set it up before using feature requests"
        ),
        "intake_setup_required",
    )
}

/// Confirm the binding the bridge will serve really belongs to this project's
/// repository before any read or write reaches it.
async fn verify_binding_identity(
    bridge: &IntakeBridge,
    project: &Project,
    binding: &str,
    actor: &IntakeActor,
) -> Result<(), ApiError> {
    let bindings = bridge.bindings(actor).await?;
    let Some(row) = bindings.into_iter().find(|row| row.id == binding) else {
        return Err(ApiError::with_code(
            StatusCode::CONFLICT,
            format!(
                "the registered intake binding {binding} no longer exists; set up feature intake again"
            ),
            "intake_binding_missing",
        ));
    };
    let same_repository = fabro_types::GitHubRepositorySlug::try_new(&row.github)
        .zip(fabro_types::GitHubRepositorySlug::try_new(
            &project.repository,
        ))
        .is_some_and(|(binding_repo, project_repo)| binding_repo == project_repo);
    if !same_repository {
        return Err(ApiError::with_code(
            StatusCode::CONFLICT,
            format!(
                "intake binding {binding} is registered for {} but project {} owns {}; resolve the registration conflict before continuing",
                row.github, project.id, project.repository
            ),
            "intake_binding_repository_mismatch",
        ));
    }
    Ok(())
}

fn parse_project_id(id: &str) -> Result<ProjectId, ApiError> {
    ProjectId::new(id).map_err(|err| ApiError::bad_request(format!("invalid project id: {err}")))
}

fn parse_issue(issue: &str) -> Result<String, ApiError> {
    let trimmed = issue.trim();
    if trimmed.is_empty() || trimmed.len() > 64 {
        return Err(ApiError::bad_request("invalid feature-request issue id"));
    }
    Ok(trimmed.to_string())
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|_| {
        ApiError::with_code(
            StatusCode::BAD_GATEWAY,
            "the feature-intake bridge returned a response this server could not read",
            "intake_bridge_payload",
        )
    })
}

async fn get_intake(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse_project_id(&id)?;
    let Some(project) = state.project_store().get(&id).await? else {
        return Err(ApiError::not_found(format!("project not found: {id}")));
    };
    let Some(binding) = project.intake_binding_id.clone() else {
        return Ok((
            StatusCode::OK,
            Json(IntakeSetupStatus {
                binding:        None,
                paused:         false,
                setup_required: true,
                readiness:      None,
            }),
        )
            .into_response());
    };
    let bridge = state.intake_bridge()?;
    let actor = IntakeActor::from_principal(&principal);
    verify_binding_identity(&bridge, &project, &binding, &actor).await?;
    let status = bridge.project_status(&binding, &actor).await?;
    Ok((StatusCode::OK, Json(status)).into_response())
}

async fn setup_intake(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<IntakeSetupRequest>,
) -> Result<Response, ApiError> {
    let id = parse_project_id(&id)?;
    let expected = parse_required_if_match(&headers, "project", &id)?;
    let Some(project) = state.project_store().get(&id).await? else {
        return Err(ApiError::not_found(format!("project not found: {id}")));
    };
    if project.revision != expected {
        return Err(ApiError::from(ProjectStoreError::StaleRevision {
            id: id.clone(),
            expected,
            actual: project.revision,
        }));
    }
    let settings = state.server_settings();
    let plane = &settings.server.integrations.plane;
    if !plane.enabled {
        return Err(ApiError::with_code(
            StatusCode::SERVICE_UNAVAILABLE,
            "the server's Plane integration is not enabled; feature intake needs it for authoring state",
            "intake_plane_not_configured",
        ));
    }
    let bridge = state.intake_bridge()?;
    let actor = IntakeActor::from_principal(&principal);
    // The registry binding id is the native project id, so an uncertain
    // response can always be reconciled by looking the exact binding up.
    let binding = project.id.as_str().to_string();
    bridge
        .setup(
            &binding,
            &actor,
            // The route owns the binding id; the bridge rejects an id in the body.
            serde_json::json!({
                "name": project.name,
                "github": project.repository,
                "default_branch": project.default_branch,
                "plane_project_id": request.plane_project_id.as_str(),
                "plane_workspace": plane.workspace,
                "plane_url": plane.api_base,
                "confirm": true,
            }),
        )
        .await?;
    // Read the binding back before linking it locally: a setup that cannot be
    // confirmed leaves the project visible and unbound.
    verify_binding_identity(&bridge, &project, &binding, &actor).await?;
    let updated = state
        .project_store()
        .set_intake_binding(&project.id, &project.revision, Some(&binding))
        .await?;
    let revision = updated.revision.clone();
    Ok(json_with_etag_response(
        StatusCode::OK,
        "project",
        &revision,
        updated,
    ))
}

async fn configure_intake_execution(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<IntakeExecutionSetupRequest>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let expected = parse_required_if_match(&headers, "project", &bound.project.id)?;
    if bound.project.revision != expected {
        return Err(ApiError::from(ProjectStoreError::StaleRevision {
            id: bound.project.id.clone(),
            expected,
            actual: bound.project.revision.clone(),
        }));
    }
    let body = serde_json::to_value(&request)
        .map_err(|_| ApiError::bad_request("invalid execution configuration body"))?;
    let mut body = body
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::bad_request("invalid execution configuration body"))?;
    body.insert("confirm".to_string(), Value::Bool(true));
    bound
        .bridge
        .configure_execution(&bound.binding, &bound.actor, Value::Object(body))
        .await?;
    let refreshed = state
        .project_store()
        .get(&bound.project.id)
        .await?
        .ok_or_else(|| ApiError::not_found("project not found"))?;
    let revision = refreshed.revision.clone();
    Ok(json_with_etag_response(
        StatusCode::OK,
        "project",
        &revision,
        refreshed,
    ))
}

async fn detach_intake(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    bound.bridge.detach(&bound.binding, &bound.actor).await?;
    let current = state
        .project_store()
        .get(&bound.project.id)
        .await?
        .ok_or_else(|| ApiError::not_found("project not found"))?;
    let updated = state
        .project_store()
        .set_intake_binding(&current.id, &current.revision, None)
        .await?;
    let revision = updated.revision.clone();
    Ok(json_with_etag_response(
        StatusCode::OK,
        "project",
        &revision,
        updated,
    ))
}

async fn get_template(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let template: IntakeTemplate = bound.bridge.template(&bound.binding, &bound.actor).await?;
    Ok((StatusCode::OK, Json(template)).into_response())
}

async fn list_initiatives(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let value = bound
        .bridge
        .list_initiatives(&bound.binding, &bound.actor)
        .await?;
    let rows: Vec<IntakeInitiativeSummary> = decode(value)?;
    Ok((StatusCode::OK, Json(rows)).into_response())
}

async fn get_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let value = bound
        .bridge
        .initiative(&bound.binding, &bound.actor, &issue)
        .await?;
    let detail: IntakeInitiativeDetail = decode(value)?;
    Ok((StatusCode::OK, Json(detail)).into_response())
}

async fn get_history(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let value = bound
        .bridge
        .history(&bound.binding, &bound.actor, &issue)
        .await?;
    let rows: Vec<IntakeRunRecord> = decode(value)?;
    Ok((StatusCode::OK, Json(rows)).into_response())
}

async fn create_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<IntakeCreateRequest>,
) -> Result<Response, ApiError> {
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("feature request needs a name"));
    }
    if request.fields.is_empty() {
        return Err(ApiError::bad_request(
            "feature request needs at least one template field",
        ));
    }
    let bound = load_bound_project(&state, &principal, id).await?;
    let value = bound
        .bridge
        .create_initiative(
            &bound.binding,
            &bound.actor,
            serde_json::json!({
                "name": request.name,
                "fields": request.fields,
                "supervised": request.supervised,
            }),
        )
        .await?;
    // The console projection returns the created issue id in `run`; native
    // callers get it as `issue` so the response says what it holds.
    let result: IntakeCreateResult = decode(value)?;
    let issue = result
        .run
        .as_ref()
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok((
        StatusCode::OK,
        Json(IntakeCreateResponse {
            ok: result.ok,
            issue,
        }),
    )
        .into_response())
}

/// Bridge projection of a create; `run` carries the created issue id.
#[derive(serde::Deserialize)]
struct IntakeCreateResult {
    ok:  bool,
    #[serde(default)]
    run: Option<Value>,
}

async fn post_comment(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
    Json(request): Json<IntakeCommentRequest>,
) -> Result<Response, ApiError> {
    if request.text.trim().is_empty() {
        return Err(ApiError::bad_request("comment text must not be empty"));
    }
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "comments",
            serde_json::json!({ "text": request.text }),
        )
        .await?;
    action_response(value)
}

async fn approve_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
    body: Option<Json<IntakeSupervisedRequest>>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let supervised = body.is_some_and(|Json(body)| body.supervised);
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "approve",
            serde_json::json!({ "supervised": supervised }),
        )
        .await?;
    action_response(value)
}

async fn revise_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
    Json(request): Json<IntakeReviseRequest>,
) -> Result<Response, ApiError> {
    if request.text.trim().is_empty() {
        return Err(ApiError::bad_request("revision text must not be empty"));
    }
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "revise",
            serde_json::json!({ "text": request.text, "supervised": request.supervised }),
        )
        .await?;
    action_response(value)
}

async fn cancel_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
    body: Option<Json<IntakeSupervisedRequest>>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let supervised = body.is_some_and(|Json(body)| body.supervised);
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "cancel",
            serde_json::json!({ "supervised": supervised }),
        )
        .await?;
    action_response(value)
}

async fn run_stage(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
    Json(request): Json<IntakeStageRequest>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "run",
            serde_json::json!({ "stage": request.stage, "supervised": request.supervised }),
        )
        .await?;
    action_response(value)
}

async fn reconcile_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "reconcile",
            Value::Null,
        )
        .await?;
    action_response(value)
}

async fn execute_initiative(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, issue)): Path<(String, String)>,
    body: Option<Json<IntakeSupervisedRequest>>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let issue = parse_issue(&issue)?;
    let supervised = body.is_some_and(|Json(body)| body.supervised);
    let value = bound
        .bridge
        .initiative_action(
            &bound.binding,
            &bound.actor,
            &issue,
            "execute",
            serde_json::json!({ "supervised": supervised }),
        )
        .await?;
    action_response(value)
}

async fn recheck_readiness(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    let report: IntakeReadinessReport = bound
        .bridge
        .recheck_readiness(&bound.binding, &bound.actor)
        .await?;
    Ok((StatusCode::OK, Json(report)).into_response())
}

async fn set_pause(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<IntakePauseRequest>,
) -> Result<Response, ApiError> {
    if request.reason.trim().is_empty() {
        return Err(ApiError::bad_request(
            "pausing or resuming intake needs a reason",
        ));
    }
    let bound = load_bound_project(&state, &principal, id).await?;
    let value = bound
        .bridge
        .pause(
            &bound.binding,
            &bound.actor,
            request.paused,
            request.reason.as_str(),
        )
        .await?;
    let status: IntakePauseStatus = decode(value)?;
    Ok((StatusCode::OK, Json(status)).into_response())
}

async fn stream_chat(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, key)): Path<(String, String)>,
    Json(request): Json<IntakeChatRequest>,
) -> Result<Response, ApiError> {
    let text = request.text;
    if text.trim().is_empty() {
        return Err(ApiError::bad_request("advisor message must not be empty"));
    }
    if text.len() > MAX_CHAT_MESSAGE_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "advisor message is too long",
        ));
    }
    let bound = load_bound_project(&state, &principal, id).await?;
    let response = bound
        .bridge
        .chat_stream(&bound.binding, &bound.actor, &key, &text)
        .await?;
    let stream = response.bytes_stream();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not open the advisor stream",
            )
            .into_response()
        }))
}

async fn close_chat(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((id, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let bound = load_bound_project(&state, &principal, id).await?;
    bound
        .bridge
        .chat_close(&bound.binding, &bound.actor, &key)
        .await?;
    Ok((StatusCode::OK, Json(IntakeOkResponse { ok: true })).into_response())
}

/// Import every registered binding as a native project.
///
/// Each binding is resolved against GitHub with the server's own credentials;
/// only exact repository matches are inserted or attached, and every conflict
/// is reported per row. Nothing is enabled anywhere.
async fn import_intake_bindings(
    RequiredUser(principal): RequiredUser,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    let bridge = state.intake_bridge()?;
    let actor = IntakeActor::from_principal(&principal);
    let bindings = bridge.bindings(&actor).await?;
    let mut rows = Vec::with_capacity(bindings.len());
    for binding in bindings {
        rows.push(import_one(&state, &binding).await);
    }
    Ok((StatusCode::OK, Json(IntakeImportResponse { data: rows })).into_response())
}

async fn import_one(state: &Arc<AppState>, binding: &IntakeBindingSummary) -> IntakeImportRow {
    let repository = binding.github.clone();
    let resolve = github_repositories::read_repository(state.as_ref(), &repository).await;
    let summary = match resolve {
        Ok(summary) => summary,
        Err(error) => {
            tracing::warn!(error = ?error, binding = %binding.id, "Intake binding repository is not readable");
            return import_row(
                binding,
                None,
                IntakeImportRowStatus::Error,
                Some(
                    "repository is not accessible with this server's GitHub credentials"
                        .to_string(),
                ),
            );
        }
    };
    let github_id =
        GithubRepositoryId::new(summary.id.to_string()).expect("GitHub repository ids are decimal");
    let existing = state.project_store().find_by_github_id(&github_id).await;
    let existing = match existing {
        Ok(project) => project,
        Err(error) => {
            tracing::warn!(error = ?error, binding = %binding.id, "Looking up the imported project failed");
            return import_row(
                binding,
                None,
                IntakeImportRowStatus::Error,
                Some("project lookup failed".to_string()),
            );
        }
    };
    if let Some(project) = existing {
        if project
            .intake_binding_id
            .as_deref()
            .is_some_and(|bound| bound != binding.id)
        {
            return import_row(
                binding,
                Some(project.id.to_string()),
                IntakeImportRowStatus::Conflict,
                Some(format!(
                    "project {} already owns a different intake binding",
                    project.id
                )),
            );
        }
        if project.intake_binding_id.is_none() {
            if let Err(error) = state
                .project_store()
                .set_intake_binding(&project.id, &project.revision, Some(&binding.id))
                .await
            {
                tracing::warn!(error = ?error, binding = %binding.id, "Attaching intake binding failed");
                return import_row(
                    binding,
                    Some(project.id.to_string()),
                    IntakeImportRowStatus::Error,
                    Some("could not attach the binding to the existing project".to_string()),
                );
            }
        }
        return import_row(
            binding,
            Some(project.id.to_string()),
            IntakeImportRowStatus::Attached,
            None,
        );
    }

    let Ok(id) = ProjectId::new(binding.id.clone()) else {
        return import_row(
            binding,
            None,
            IntakeImportRowStatus::Conflict,
            Some("binding id is not a valid native project id".to_string()),
        );
    };
    let Some(default_branch) = summary.default_branch.clone() else {
        return import_row(
            binding,
            None,
            IntakeImportRowStatus::Error,
            Some("repository has no default branch yet".to_string()),
        );
    };
    let draft = ProjectDraft {
        id: id.clone(),
        name: binding.name.clone(),
        github_repository_id: github_id,
        repository: summary.full_name.clone(),
        default_branch,
    };
    match state.project_store().create(draft).await {
        Ok(project) => {
            if let Err(error) = state
                .project_store()
                .set_intake_binding(&project.id, &project.revision, Some(&binding.id))
                .await
            {
                tracing::warn!(error = ?error, binding = %binding.id, "Linking imported project failed");
                return import_row(
                    binding,
                    Some(project.id.to_string()),
                    IntakeImportRowStatus::Error,
                    Some("imported the project but could not link the binding".to_string()),
                );
            }
            import_row(
                binding,
                Some(project.id.to_string()),
                IntakeImportRowStatus::Imported,
                None,
            )
        }
        Err(error) => {
            tracing::warn!(error = ?error, binding = %binding.id, "Creating imported project failed");
            import_row(
                binding,
                None,
                IntakeImportRowStatus::Conflict,
                Some("project could not be created from this binding".to_string()),
            )
        }
    }
}

fn import_row(
    binding: &IntakeBindingSummary,
    project_id: Option<String>,
    status: IntakeImportRowStatus,
    message: Option<String>,
) -> IntakeImportRow {
    IntakeImportRow {
        binding: binding.id.clone(),
        repository: Some(binding.github.clone()),
        project_id,
        status,
        message,
    }
}

fn action_response(value: Value) -> Result<Response, ApiError> {
    let response: IntakeActionResponse = decode(value)?;
    Ok((StatusCode::OK, Json(response)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_payload_from_the_bridge_deserializes() {
        let value = serde_json::json!({
            "project": "tierrapay",
            "checked_at": null,
            "checks": [{
                "key": "configuration",
                "outcome": "unknown",
                "message": "La configuración cambió desde la última comprobación.",
                "next_action": "Volver a comprobar integraciones"
            }],
            "authoring_ready": false,
            "execution_ready": false
        });
        let parsed: IntakeReadinessReport = serde_json::from_value(value.clone()).unwrap();
        assert!(!parsed.authoring_ready);
        assert!(parsed.checked_at.is_none());
        assert_eq!(parsed.checks.len(), 1);
        assert_eq!(
            serde_json::to_value(parsed).unwrap()["checks"][0]["outcome"],
            "unknown"
        );

        let setup = serde_json::json!({
            "binding": "tierrapay",
            "paused": false,
            "setup_required": false,
            "readiness": value
        });
        let status: IntakeSetupStatus = serde_json::from_value(setup).unwrap();
        assert!(status.readiness.is_some());
    }
}
