use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::super::{
    ApiError, AppState, IntoResponse, RequiredUser, Response, StatusCode, incident_intake,
};

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/incidents/bugsink", get(status))
        .route("/incidents/bugsink/baseline", post(baseline))
        .route("/incidents/bugsink/{project}/{issue}/retry", post(retry))
}

#[expect(
    clippy::empty_structs_with_brackets,
    reason = "Serde must accept exactly an empty JSON object; a unit struct would change the request contract to null"
)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineRequest {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryRequest {
    #[serde(default)]
    allow_additional_run: bool,
}

async fn status(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    let settings = state.server_settings();
    let config = &settings.server.integrations.bugsink;
    let data = state.incident_store().snapshot().await.map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Incident status unavailable.",
        )
    })?;
    Ok(Json(json!({"data":data,"meta":{"enabled":config.enabled,"dispatch_enabled":config.dispatch_enabled}})).into_response())
}

async fn baseline(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Json(_request): Json<BaselineRequest>,
) -> Result<Response, ApiError> {
    incident_intake::begin_baseline(&state,chrono::Utc::now().timestamp_millis()).await
        .map_err(|_| ApiError::new(StatusCode::CONFLICT,"Baseline cannot start while intake is disabled, dispatch is enabled, or a Run is active or uncertain."))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"status":"baseline_started"})),
    )
        .into_response())
}

async fn retry(
    RequiredUser(user): RequiredUser,
    State(state): State<Arc<AppState>>,
    Path((project, issue)): Path<(u64, String)>,
    Json(request): Json<RetryRequest>,
) -> Result<Response, ApiError> {
    let parsed = issue
        .parse::<uuid::Uuid>()
        .map_err(|_| ApiError::bad_request("Invalid issue UUID."))?;
    if parsed.to_string() != issue {
        return Err(ApiError::bad_request("Issue UUID must be canonical."));
    }
    let actor = serde_json::to_string(&user).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Operator identity unavailable.",
        )
    })?;
    incident_intake::operator_retry(&state,project,parsed,&actor,request.allow_additional_run,chrono::Utc::now().timestamp_millis()).await
        .map_err(|_|ApiError::new(StatusCode::CONFLICT,"Retry requires authoritative incident state, completed baseline, and no unresolved Run; additional Runs require explicit authorization and enabled dispatch."))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"status":"retry_requested"})),
    )
        .into_response())
}
