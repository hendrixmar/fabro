use std::sync::Arc;

use fabro_auth::ApiCredential;
use fabro_llm::client::Client as LlmClient;
use fabro_llm::model_test::{ModelTestStatus, run_basic_model_probe};
use fabro_model::{ModelSelectionError, ReasoningEffort};
use fabro_redact::redact_string;

use super::super::{
    ApiError, AppState, FromStr, HashSet, IntoResponse, Json, MAX_PAGE_OFFSET, ModelTestMode, Path,
    ProviderCredentialTestRequest, ProviderCredentialTestResponse, ProviderId, ProviderList, Query,
    RequiredUser, Response, Router, State, StatusCode, auth_issue_message, default_page_limit,
    error, get, post, run_model_test,
};
use crate::diagnostics;

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/models", get(list_models))
        .route("/models/{id}/test", post(test_model))
        .route("/providers", get(list_providers))
        .route(
            "/providers/{provider}/credentials/test",
            post(test_provider_credentials),
        )
        .route("/providers/test", post(test_providers))
}

#[derive(serde::Deserialize)]
struct ModelListParams {
    #[serde(rename = "page[limit]", default = "default_page_limit")]
    limit:    u32,
    #[serde(rename = "page[offset]", default)]
    offset:   u32,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    query:    Option<String>,
}

#[derive(serde::Deserialize)]
struct ModelTestParams {
    #[serde(default)]
    mode:             Option<String>,
    #[serde(default)]
    provider:         Option<String>,
    #[serde(default)]
    reasoning_effort: Option<String>,
}

async fn list_models(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Query(params): Query<ModelListParams>,
) -> Response {
    let provider_id = params.provider.as_deref().map(ProviderId::from);

    let query = params.query.as_ref().map(|value| value.to_lowercase());
    let limit = params.limit.clamp(1, 100) as usize;
    let offset = params.offset.min(MAX_PAGE_OFFSET) as usize;
    let catalog = state.catalog();
    let configured: HashSet<ProviderId> =
        state.ready_llm_provider_ids().await.into_iter().collect();

    let mut data = catalog
        .list(provider_id.as_ref())
        .into_iter()
        .filter(|model| match &query {
            Some(query) => {
                model.id.as_str().to_lowercase().contains(query)
                    || model.display_name.to_lowercase().contains(query)
                    || model
                        .aliases
                        .iter()
                        .any(|alias| alias.to_lowercase().contains(query))
            }
            None => true,
        })
        .skip(offset)
        .take(limit + 1)
        .cloned()
        .map(|mut model| {
            model.configured = configured.contains(&model.provider);
            model
        })
        .collect::<Vec<_>>();

    let has_more = data.len() > limit;
    data.truncate(limit);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": { "has_more": has_more }
        })),
    )
        .into_response()
}

async fn list_providers(_auth: RequiredUser, State(state): State<Arc<AppState>>) -> Response {
    let catalog = state.catalog();
    let configured: HashSet<ProviderId> = state
        .configured_llm_provider_ids()
        .await
        .into_iter()
        .collect();
    let data = catalog.provider_summaries(&configured);

    (StatusCode::OK, Json(ProviderList { data })).into_response()
}

async fn test_provider_credentials(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Json(body): Json<ProviderCredentialTestRequest>,
) -> Response {
    if body.api_key.trim().is_empty() {
        return ApiError::bad_request("api_key is required").into_response();
    }

    let requested_provider = ProviderId::new(provider);
    let catalog = state.catalog();
    let Some(catalog_provider) = catalog.provider(&requested_provider) else {
        return ApiError::not_found(format!("Provider not found: {requested_provider}"))
            .into_response();
    };
    if catalog_provider.auth.is_none() {
        return ApiError::bad_request(format!(
            "provider '{}' does not define an API-key credential path",
            catalog_provider.id,
        ))
        .into_response();
    }
    let provider_id = catalog_provider.id.clone();

    let credential =
        match ApiCredential::from_api_key(provider_id.clone(), body.api_key, catalog.as_ref()) {
            Ok(credential) => credential,
            Err(err) => {
                return ApiError::bad_request(err.to_string()).into_response();
            }
        };
    let client = match LlmClient::from_credentials(vec![credential], Arc::clone(&catalog)).await {
        Ok(client) => Arc::new(client),
        Err(err) => {
            error!(provider = %provider_id, error = ?err, "Failed to create LLM client for provider credential validation");
            return ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create LLM client: {err}"),
            )
            .into_response();
        }
    };
    let Some(model) = catalog.probe_for_provider(&provider_id) else {
        return ApiError::bad_request(format!(
            "provider '{provider_id}' does not define a probe model"
        ))
        .into_response();
    };

    let outcome = run_basic_model_probe(model.id.as_str(), &provider_id, client).await;
    match outcome.status {
        ModelTestStatus::Ok => (
            StatusCode::OK,
            Json(ProviderCredentialTestResponse { ok: true }),
        )
            .into_response(),
        ModelTestStatus::Error => {
            let message = outcome
                .error_message
                .unwrap_or_else(|| "provider credential validation failed".to_string());
            ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, redact_string(&message)).into_response()
        }
    }
}

async fn test_providers(_auth: RequiredUser, State(state): State<Arc<AppState>>) -> Response {
    match diagnostics::test_llm_providers(&state).await {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        Err(err) => {
            error!(error = ?err, "Failed to resolve LLM providers for provider test");
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to resolve LLM providers",
            )
            .into_response()
        }
    }
}

fn parse_query_enum<T: FromStr>(value: Option<&str>, label: &str) -> Result<Option<T>, ApiError> {
    value
        .map(|value| {
            T::from_str(value).map_err(|_| {
                ApiError::new(StatusCode::BAD_REQUEST, format!("invalid {label}: {value}"))
            })
        })
        .transpose()
}

async fn test_model(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<ModelTestParams>,
) -> Response {
    let mode = match parse_query_enum(params.mode.as_deref(), "model test mode") {
        Ok(mode) => mode.unwrap_or(ModelTestMode::Basic),
        Err(error) => return error.into_response(),
    };
    let reasoning_effort = match parse_query_enum::<ReasoningEffort>(
        params.reasoning_effort.as_deref(),
        "reasoning effort",
    ) {
        Ok(reasoning_effort) => reasoning_effort,
        Err(error) => return error.into_response(),
    };
    let llm_result = match state.resolve_llm_client().await {
        Ok(result) => result,
        Err(err) => {
            error!(error = ?err, "Failed to resolve LLM client");
            return ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to resolve LLM client: {err}"),
            )
            .into_response();
        }
    };
    let catalog = state.catalog();
    let eligible = llm_result
        .provider_ids()
        .into_iter()
        .collect::<HashSet<_>>();
    let explicit_provider = params.provider.map(ProviderId::new);
    let info = if let Some(provider) = explicit_provider.as_ref() {
        match catalog.resolve_on_provider(provider, &id) {
            Ok(info) => info,
            Err(error) => return model_selection_response(&error),
        }
    } else {
        match catalog.select(&id, None, &eligible) {
            Ok(info) => info,
            Err(error) => return model_selection_response(&error),
        }
    };
    if let Some((_, issue)) = llm_result
        .auth_issues
        .iter()
        .find(|(provider, _)| provider == &info.provider)
    {
        return ApiError::bad_request(auth_issue_message(&info.provider, issue)).into_response();
    }
    let provider_name = info.provider.as_str();
    if !llm_result.client.has_provider(provider_name) {
        return Json(serde_json::json!({
            "model_id": info.id,
            "provider": info.provider,
            "status": "skip",
        }))
        .into_response();
    }
    let client = Arc::new(llm_result.client);

    let outcome = run_model_test(info, mode, reasoning_effort, client).await;
    Json(serde_json::json!({
        "model_id": info.id,
        "provider": info.provider,
        "status": <&'static str>::from(outcome.status),
        "error_message": outcome.error_message,
    }))
    .into_response()
}

fn model_selection_response(error: &ModelSelectionError) -> Response {
    match error {
        ModelSelectionError::UnknownProvider { .. }
        | ModelSelectionError::UnknownSelector { .. }
        | ModelSelectionError::UnknownSelectorOnProvider { .. } => {
            ApiError::not_found(error.to_string()).into_response()
        }
        ModelSelectionError::ProviderUnavailable { .. }
        | ModelSelectionError::NoEligibleOffering { .. }
        | ModelSelectionError::NoDefaultModel { .. } => {
            ApiError::bad_request(error.to_string()).into_response()
        }
    }
}
