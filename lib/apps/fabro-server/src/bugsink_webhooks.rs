use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, FromRequest as _, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::post;
use fabro_types::Principal;
use hmac::{Hmac, Mac as _};
use mime_guess::mime::{APPLICATION, JSON};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio::sync::Semaphore;

use crate::principal_middleware::{RequestAuth, RequestAuthContext};
use crate::server::AppState;
use crate::server::incident_intake::{AcceptResult, AcceptedAlert, AlertReason};

const WEBHOOK_ROUTE: &str = "/api/v1/webhooks/bugsink";
const SIGNATURE_HEADER: &str = "sentry-hook-signature";
const BODY_LIMIT: usize = 256 * 1024;

struct ReceiverState {
    app:     Arc<AppState>,
    ingress: Semaphore,
}

pub(crate) fn routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route(WEBHOOK_ROUTE, post(receive))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(Arc::new(ReceiverState {
            app:     state,
            ingress: Semaphore::new(16),
        }))
}

/// Bugsink signs the exact transmitted bytes with an unprefixed SHA-256 HMAC.
pub(crate) fn verify_signature(secret: &[u8], body: &[u8], header: &str) -> bool {
    if secret.is_empty() || header.len() != 64 {
        return false;
    }
    let mut expected = [0u8; 32];
    if hex::decode_to_slice(header, &mut expected).is_err() {
        return false;
    }
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

// Serde's struct visitor rejects duplicate recognized fields, including escaped
// spellings of a key. Unknown canonical fields are skipped, never trusted.
#[derive(Deserialize)]
struct RoutingFields {
    project:      u64,
    id:           String,
    alert_reason: String,
}

pub(crate) fn normalize_alert(
    body: &[u8],
    signed_project: u64,
    received_ms: i64,
) -> Result<AcceptedAlert, StatusCode> {
    // Derived struct deserializers also accept sequences; the wire contract is
    // an object with named routing fields, never positional JSON.
    if body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        != Some(b'{')
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let fields: RoutingFields =
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let issue_id = uuid::Uuid::parse_str(&fields.id).map_err(|_| StatusCode::BAD_REQUEST)?;
    if fields.project == 0 || i64::try_from(fields.project).is_err() || issue_id.is_nil() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let reason = match fields.alert_reason.as_str() {
        "NEW" => AlertReason::New,
        "REGRESSED" => AlertReason::Regressed,
        "UNMUTED" => AlertReason::Unmuted,
        "TEST" => AlertReason::Test,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    if fields.project != signed_project {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(AcceptedAlert {
        project_id: fields.project,
        issue_id,
        reason,
        body_digest: hex::encode(Sha256::digest(body)),
        received_ms,
    })
}

async fn receive(
    State(state): State<Arc<ReceiverState>>,
    RequestAuth(auth_slot): RequestAuth,
    headers: HeaderMap,
    request: Request,
) -> StatusCode {
    // Admission precedes body collection; queued uploads cannot consume memory
    // beyond the sixteen bounded requests already admitted.
    let Ok(_permit) = state.ingress.try_acquire() else {
        return StatusCode::TOO_MANY_REQUESTS;
    };
    let settings = state.app.server_settings();
    let config = &settings.server.integrations.bugsink;
    if !config.enabled {
        return StatusCode::NOT_FOUND;
    }
    let content_type = single_header(&headers, header::CONTENT_TYPE.as_str())
        .and_then(|value| value.parse::<mime_guess::Mime>().ok());
    if !content_type.is_some_and(|value| {
        value.type_() == APPLICATION && value.subtype() == JSON && value.suffix().is_none()
    }) {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE;
    }
    // Move the request into the extractor before awaiting vault access. Holding
    // a reference to Request across an await would require its body to be Sync.
    let body = match Bytes::from_request(request, &state).await {
        Ok(body) => body,
        Err(rejection) => return rejection.status(),
    };
    let Some(signature) = single_header(&headers, SIGNATURE_HEADER) else {
        auth_slot.replace(RequestAuthContext::invalid());
        return StatusCode::UNAUTHORIZED;
    };
    let Some(origin) = config.origin.as_deref() else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    if config.projects.is_empty() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let mut signed_project = None;
    for project in &config.projects {
        let secret = match state.app.vault_secret(&project.signing_secret).await {
            Ok(Some(secret)) if !secret.trim().is_empty() => secret,
            _ => return StatusCode::SERVICE_UNAVAILABLE,
        };
        if verify_signature(secret.as_bytes(), &body, signature)
            && signed_project.replace(project.project_id).is_some()
        {
            // Shared key material cannot unambiguously identify a mapping.
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    let Some(signed_project) = signed_project else {
        auth_slot.replace(RequestAuthContext::invalid());
        return StatusCode::UNAUTHORIZED;
    };
    let alert = match normalize_alert(&body, signed_project, chrono::Utc::now().timestamp_millis())
    {
        Ok(alert) => alert,
        Err(status) => {
            auth_slot.replace(RequestAuthContext::invalid());
            return status;
        }
    };
    auth_slot.replace(RequestAuthContext::authenticated(
        Principal::Webhook {
            delivery_id: alert.body_digest.clone(),
        },
        None,
    ));
    match state.app.incident_store().accept(origin, &alert).await {
        Ok(_) if alert.reason == AlertReason::Test => StatusCode::OK,
        Ok(AcceptResult::Queued | AcceptResult::Duplicate) => StatusCode::ACCEPTED,
        Ok(AcceptResult::Test) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use fabro_automation::{AutomationDraft, AutomationId, AutomationStore};
    use fabro_types::{GitRunTarget, RunTarget};
    use fabro_types::settings::server::{BugsinkIntegrationSettings, BugsinkProjectSettings};
    use fabro_vault::{SecretStore, SecretType};
    use tower::ServiceExt as _;

    use super::*;
    use crate::principal_middleware::AuthContextSlot;
    use crate::server::{
        AppStateConfig, RouterOptions, build_app_state, build_router_with_options,
    };
    use crate::test_support::{
        TEST_DEV_TOKEN, default_test_server_settings, load_test_server_secrets,
        resolved_runtime_settings_for_tests, test_auth_mode, test_secret_snapshot,
        test_store_bundle,
    };

    const KEY: &str = "isolated-test-signing-key";
    const TEST_BODY: &[u8] = br#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"alert_reason":"TEST","title":"untrusted","is_resolved":false}"#;
    const NEW_BODY: &[u8] =
        br#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"alert_reason":"NEW"}"#;

    fn signature(body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn request(body: &[u8]) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri(WEBHOOK_ROUTE)
            .header(header::CONTENT_TYPE, "application/json")
            .header(SIGNATURE_HEADER, signature(body))
            .body(Body::from(body.to_vec()))
            .unwrap()
    }

    struct Fixture {
        app:   Router,
        state: Arc<AppState>,
        pool:  fabro_db::DbPool,
        _dir:  tempfile::TempDir,
    }

    async fn fixture(enabled: bool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let database = fabro_db::Database::connect(dir.path().join("fabro.db"))
            .await
            .unwrap();
        database.migrate().await.unwrap();
        let pool = database.clone_pool();
        let vault = SecretStore::new(pool.clone());
        for (name, value) in [
            ("BUGSINK_API_TOKEN", "isolated-api-material"),
            ("BUGSINK_SIGNING_25", KEY),
            ("BUGSINK_SIGNING_26", "other-project-key"),
        ] {
            vault
                .set(name, value, SecretType::Token, None)
                .await
                .unwrap();
        }
        AutomationStore::new(pool.clone())
            .create(AutomationDraft {
                id:              AutomationId::new("incident-loop").unwrap(),
                name:            "Incident intake".into(),
                description:     None,
                environment_id:  None,
                target:          RunTarget::Git(GitRunTarget {
                    repo:   "test/incident-workflows".into(),
                    branch: "main".into(),
                    tag:    None,
                    sha:    Some("a".repeat(40)),
                }),
                workflow:        "incident-loop".into(),
                workflow_source: None,
                triggers:        vec![],
            })
            .await
            .unwrap();
        let mut settings = default_test_server_settings().with_storage_override(dir.path());
        settings.server.integrations.bugsink = BugsinkIntegrationSettings {
            enabled,
            dispatch_enabled: false,
            origin: Some("https://bugsink.example".into()),
            api_token_secret: Some("BUGSINK_API_TOKEN".into()),
            projects: vec![
                BugsinkProjectSettings {
                    project_id:     25,
                    automation_id:  "incident-loop".into(),
                    signing_secret: "BUGSINK_SIGNING_25".into(),
                },
                BugsinkProjectSettings {
                    project_id:     26,
                    automation_id:  "incident-loop".into(),
                    signing_secret: "BUGSINK_SIGNING_26".into(),
                },
            ],
        };
        let (store, artifact_store) = test_store_bundle();
        let state = build_app_state(AppStateConfig {
            resolved_settings: resolved_runtime_settings_for_tests(
                settings,
                fabro_config::RunLayer::default(),
                Default::default(),
            ),
            registry_factory_override: None,
            max_concurrent_runs: 5,
            store,
            artifact_store,
            db_pool: pool.clone(),
            preloaded_vault: test_secret_snapshot(pool.clone()).unwrap(),
            server_secrets: load_test_server_secrets(
                dir.path().join("server.env"),
                Default::default(),
            ),
            env_lookup: Arc::new(|_| None),
            github_api_base_url: None,
            active_config_path: dir.path().join("settings.toml"),
            http_client: Some(fabro_http::test_http_client().unwrap()),
            sandbox_provider_registry: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
            worker_control_bus: None,
            worker_runtime: None,
            automation_materializer_override: None,
        })
        .unwrap();
        let app = build_router_with_options(state.clone(), &test_auth_mode(), RouterOptions {
            web_enabled: false,
            ..RouterOptions::default()
        });
        Fixture {
            app,
            state,
            pool,
            _dir: dir,
        }
    }

    async fn assert_no_work(state: &AppState, deliveries: u64) {
        let snapshot = state.incident_store().snapshot().await.unwrap();
        assert_eq!(snapshot["delivery_count"], deliveries);
        assert_eq!(snapshot["incidents"], serde_json::json!([]));
        assert_eq!(snapshot["runs"], serde_json::json!([]));
        assert!(
            state
                .stores
                .runs
                .list_runs(&fabro_store::ListRunsQuery::default(), chrono::Utc::now())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn signature_binds_exact_bytes() {
        let body = br#"{"project":25,"alert_reason":"TEST"}"#;
        let digest = signature(body);
        assert!(verify_signature(KEY.as_bytes(), body, &digest));
        assert!(!verify_signature(
            KEY.as_bytes(),
            br#"{"project":26,"alert_reason":"TEST"}"#,
            &digest
        ));
        assert!(!verify_signature(b"", body, &digest));
        for invalid in [
            format!("sha256={digest}"),
            digest[..62].to_owned(),
            "z".repeat(64),
        ] {
            assert!(!verify_signature(KEY.as_bytes(), body, &invalid));
        }
    }

    #[test]
    fn routing_parser_rejects_ambiguous_or_invalid_fields() {
        for body in [
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"project":26,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"pro\u006aect":25,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"alert_reason":"NEW","alert_reason":"TEST"}"#,
            r#"{"id":"bad","project":25,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":0,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":9223372036854775808,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":"25","alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25.0,"alert_reason":"NEW"}"#,
            r#"{"id":"497f6eca-6276-4993-bfeb-53cbbbba6f08","project":25,"alert_reason":"UNKNOWN"}"#,
            r#"{"project":25,"alert_reason":"TEST"}"#,
            r#"{}{}"#,
            r#"[25,"497f6eca-6276-4993-bfeb-53cbbbba6f08","TEST"]"#,
        ] {
            assert_eq!(
                normalize_alert(body.as_bytes(), 25, 1).unwrap_err(),
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            normalize_alert(NEW_BODY, 26, 1).unwrap_err(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn signed_test_uses_actual_project_without_management_auth_or_work() {
        let f = fixture(true).await;
        for _ in 0..2 {
            let response = f.app.clone().oneshot(request(TEST_BODY)).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.headers().contains_key("x-request-id"));
            assert!(response.headers().contains_key("x-content-type-options"));
        }
        assert_no_work(&f.state, 1).await;
        let management = HttpRequest::builder()
            .uri("/api/v1/runs")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            f.app.oneshot(management).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn signature_cannot_authorize_another_project_or_be_replaced_by_bearer() {
        let f = fixture(true).await;
        for project in [26, 99] {
            let body = String::from_utf8(TEST_BODY.to_vec())
                .unwrap()
                .replace("\"project\":25", &format!("\"project\":{project}"));
            assert_eq!(
                f.app
                    .clone()
                    .oneshot(request(body.as_bytes()))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::FORBIDDEN
            );
        }
        let mut tampered = request(TEST_BODY);
        *tampered.body_mut() = Body::from(NEW_BODY);
        assert_eq!(
            f.app.clone().oneshot(tampered).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let mut bearer = request(TEST_BODY);
        bearer.headers_mut().remove(SIGNATURE_HEADER);
        bearer.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {TEST_DEV_TOKEN}").parse().unwrap(),
        );
        assert_eq!(
            f.app.clone().oneshot(bearer).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let mut duplicate_header = request(TEST_BODY);
        duplicate_header
            .headers_mut()
            .append(SIGNATURE_HEADER, signature(TEST_BODY).parse().unwrap());
        assert_eq!(
            f.app
                .clone()
                .oneshot(duplicate_header)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_no_work(&f.state, 0).await;
    }

    #[tokio::test]
    async fn duplicate_notifications_acknowledge_one_durable_generation_without_runs() {
        let f = fixture(true).await;
        let (first, second) = tokio::join!(
            f.app.clone().oneshot(request(NEW_BODY)),
            f.app.clone().oneshot(request(NEW_BODY)),
        );
        assert_eq!(first.unwrap().status(), StatusCode::ACCEPTED);
        assert_eq!(second.unwrap().status(), StatusCode::ACCEPTED);
        let snapshot = f.state.incident_store().snapshot().await.unwrap();
        assert_eq!(snapshot["delivery_count"], 1);
        assert_eq!(snapshot["incidents"][0]["project_id"], 25);
        assert_eq!(snapshot["incidents"][0]["requested_generation"], 1);
        assert_eq!(snapshot["incidents"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["runs"], serde_json::json!([]));
        assert!(
            f.state
                .stores
                .runs
                .list_runs(&fabro_store::ListRunsQuery::default(), chrono::Utc::now())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn body_and_media_limits_fail_without_acceptance() {
        let f = fixture(true).await;
        for content_type in [
            None,
            Some("text/plain"),
            Some("application/problem+json"),
            Some("application/json; broken"),
        ] {
            let mut req = request(TEST_BODY);
            req.headers_mut().remove(header::CONTENT_TYPE);
            if let Some(value) = content_type {
                req.headers_mut()
                    .insert(header::CONTENT_TYPE, value.parse().unwrap());
            }
            assert_eq!(
                f.app.clone().oneshot(req).await.unwrap().status(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE
            );
        }
        let mut duplicate_type = request(TEST_BODY);
        duplicate_type
            .headers_mut()
            .append(header::CONTENT_TYPE, "application/json".parse().unwrap());
        assert_eq!(
            f.app
                .clone()
                .oneshot(duplicate_type)
                .await
                .unwrap()
                .status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(
            f.app
                .clone()
                .oneshot(request(&vec![b' '; BODY_LIMIT + 1]))
                .await
                .unwrap()
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            f.app
                .clone()
                .oneshot(request(b"not json"))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        let mut unauthenticated = request(b"not json");
        unauthenticated.headers_mut().remove(SIGNATURE_HEADER);
        assert_eq!(
            f.app
                .clone()
                .oneshot(unauthenticated)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_no_work(&f.state, 0).await;
        let mut maximum_body = TEST_BODY.to_vec();
        maximum_body.resize(BODY_LIMIT, b' ');
        assert_eq!(
            f.app
                .clone()
                .oneshot(request(&maximum_body))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let mut req = request(TEST_BODY);
        req.headers_mut().insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8".parse().unwrap(),
        );
        assert_eq!(f.app.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn disabled_missing_keys_and_closed_pool_fail_closed() {
        let disabled = fixture(false).await;
        assert_eq!(
            disabled
                .app
                .oneshot(request(NEW_BODY))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_no_work(&disabled.state, 0).await;
        let f = fixture(true).await;
        f.state
            .stores
            .vault
            .remove("BUGSINK_SIGNING_26")
            .await
            .unwrap();
        assert_eq!(
            f.app
                .clone()
                .oneshot(request(TEST_BODY))
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_no_work(&f.state, 0).await;
        f.pool.close().await;
        assert_eq!(
            f.app.oneshot(request(NEW_BODY)).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn persistence_failure_does_not_acknowledge_or_leave_delivery() {
        let f = fixture(true).await;
        sqlx::query("CREATE TRIGGER reject_incident BEFORE INSERT ON bugsink_incidents BEGIN SELECT RAISE(ABORT, 'isolated persistence failure'); END")
            .execute(&f.pool).await.unwrap();
        assert_eq!(
            f.app.oneshot(request(NEW_BODY)).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_no_work(&f.state, 0).await;
    }

    #[tokio::test]
    async fn saturated_ingress_rejects_before_reading_body() {
        let f = fixture(true).await;
        let state = Arc::new(ReceiverState {
            app:     f.state.clone(),
            ingress: Semaphore::new(16),
        });
        let _all_permits = state.ingress.try_acquire_many(16).unwrap();
        let mut req = request(TEST_BODY);
        *req.body_mut() =
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
        let headers = req.headers().clone();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            receive(
                State(state.clone()),
                RequestAuth(AuthContextSlot::initial()),
                headers,
                req,
            ),
        )
        .await
        .unwrap();
        assert_eq!(result, StatusCode::TOO_MANY_REQUESTS);
        assert_no_work(&f.state, 0).await;
    }
}
