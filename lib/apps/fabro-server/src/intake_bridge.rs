//! Private feature-intake bridge client.
//!
//! The bridge is a second consumer of the Python intake app that listens on a
//! Unix-domain socket owned by the same OS user as this server. It has no
//! static frontend and authenticates every request from the two headers
//! derived here; the browser never reaches it and never carries these values.
//!
//! This module is transport only: it builds fixed internal paths, maps bridge
//! failures onto API errors, and never exposes credentials, filesystem paths,
//! or raw command output to a caller.

use std::time::Duration;

use fabro_api::types::{
    IntakeBindingSummary, IntakeReadinessReport, IntakeSetupStatus, IntakeTemplate,
};
use fabro_http::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use fabro_types::UserPrincipal;
use fabro_types::settings::server::IntakeIntegrationSettings;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// Longest accepted mutation body handed to the bridge.
pub(crate) const MAX_MUTATION_BODY_BYTES: usize = 256 * 1024;
/// Longest accepted advisor message.
pub(crate) const MAX_CHAT_MESSAGE_BYTES: usize = 32 * 1024;
/// Longest accepted bridge response body.
pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Ordinary bridge calls must cover the existing 120-second submission wait.
const ORDINARY_TIMEOUT: Duration = Duration::from_secs(150);
/// Registration clones a repository and provisions Plane state.
const REGISTRATION_TIMEOUT: Duration = Duration::from_mins(5);
/// Idle timeout for an advisor stream: a stalled model call ends the stream.
const CHAT_IDLE_TIMEOUT: Duration = Duration::from_mins(2);

/// The prefix every bridge actor assertion carries.
const ACTOR_PREFIX: &str = "fabro-";
/// Length of the hex digest suffix in an actor assertion.
const ACTOR_DIGEST_HEX: usize = 32;

#[derive(Debug, thiserror::Error)]
pub(crate) enum IntakeBridgeError {
    #[error("feature intake is not configured on this server")]
    Disabled,
    #[error("the feature-intake bridge is unreachable")]
    Unreachable,
    #[error("the feature-intake bridge did not answer in time")]
    Timeout,
    #[error("{0}")]
    Refused(String),
    #[error("the feature-intake bridge returned an unusable response")]
    Payload,
    #[error("the feature-intake bridge failed")]
    Failed,
}

/// Authenticated identity asserted to the private bridge.
///
/// The actor is a stable, non-reversible handle derived from the IdP identity,
/// so the bridge can key per-user state without learning the operator's real
/// identity beyond the login it renders. The login is display attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IntakeActor {
    actor: String,
    login: String,
}

impl IntakeActor {
    #[must_use]
    pub(crate) fn from_principal(principal: &UserPrincipal) -> Self {
        let identity = &principal.identity;
        let canonical = serde_json::json!({
            "issuer": identity.issuer(),
            "subject": identity.subject(),
        });
        let digest = Sha256::digest(canonical.to_string().as_bytes());
        let hex = hex::encode(digest);
        Self {
            actor: format!("{ACTOR_PREFIX}{}", &hex[..ACTOR_DIGEST_HEX]),
            login: principal.login.clone(),
        }
    }

    #[must_use]
    pub(crate) fn actor(&self) -> &str {
        &self.actor
    }

    #[must_use]
    pub(crate) fn login(&self) -> &str {
        &self.login
    }
}

/// A response read fully from the bridge.
pub(crate) struct BridgeResponse {
    pub status: u16,
    pub body:   Value,
}

#[derive(Clone)]
pub(crate) struct IntakeBridge {
    client: fabro_http::HttpClient,
    /// Base URL with a placeholder authority: the transport is the socket.
    base:   String,
}

impl IntakeBridge {
    /// Build the socket client for the configured bridge, or `None` when the
    /// integration is disabled.
    pub(crate) fn from_settings(
        settings: &IntakeIntegrationSettings,
    ) -> Result<Option<Self>, IntakeBridgeError> {
        if !settings.enabled {
            return Ok(None);
        }
        let Some(socket) = settings
            .socket
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Err(IntakeBridgeError::Disabled);
        };
        let client = fabro_http::HttpClientBuilder::new()
            .unix_socket(socket)
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| IntakeBridgeError::Unreachable)?;
        Ok(Some(Self {
            client,
            // The Unix socket transport ignores the authority; a literal
            // localhost base keeps URL parsing uniform and leaks no path.
            base: "http://localhost".to_string(),
        }))
    }

    /// Public projection of every registered binding. Used to resolve a
    /// project's repository identity for import and for pre-call verification.
    pub(crate) async fn bindings(
        &self,
        actor: &IntakeActor,
    ) -> Result<Vec<IntakeBindingSummary>, IntakeBridgeError> {
        let response = self
            .request(
                fabro_http::Method::GET,
                "/api/bridge/bindings",
                Some(actor),
                None,
                ORDINARY_TIMEOUT,
            )
            .await?;
        let value = Self::expect_success(response)?;
        serde_json::from_value(value).map_err(|_| IntakeBridgeError::Payload)
    }

    /// Project pause state plus the stored readiness snapshot. A readiness
    /// probe failure is reported as a missing snapshot; it never hides the
    /// project's own state.
    pub(crate) async fn project_status(
        &self,
        binding: &str,
        actor: &IntakeActor,
    ) -> Result<IntakeSetupStatus, IntakeBridgeError> {
        let project = self
            .get_json(binding, "/project", actor, ORDINARY_TIMEOUT)
            .await?;
        // A refused readiness read means the bridge has no snapshot yet; a
        // payload we cannot read is an error, never a silently missing report.
        let readiness = match self
            .get_json(binding, "/readiness", actor, ORDINARY_TIMEOUT)
            .await
        {
            Ok(value) => Some(
                serde_json::from_value::<IntakeReadinessReport>(value)
                    .map_err(|_| IntakeBridgeError::Payload)?,
            ),
            Err(IntakeBridgeError::Refused(_)) => None,
            Err(error) => return Err(error),
        };
        Ok(IntakeSetupStatus {
            binding: Some(binding.to_string()),
            paused: project
                .get("paused")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            setup_required: false,
            readiness,
        })
    }

    pub(crate) async fn template(
        &self,
        binding: &str,
        actor: &IntakeActor,
    ) -> Result<IntakeTemplate, IntakeBridgeError> {
        let value = self
            .get_json(binding, "/template", actor, ORDINARY_TIMEOUT)
            .await?;
        serde_json::from_value(value).map_err(|_| IntakeBridgeError::Payload)
    }

    /// Register the authoring binding. Bounded by the registration timeout.
    ///
    /// Creation cannot live under `/api/p/{binding}`: those routes resolve the
    /// binding first, so they 404 until the binding this call creates exists.
    pub(crate) async fn setup(
        &self,
        binding: &str,
        actor: &IntakeActor,
        body: Value,
    ) -> Result<Value, IntakeBridgeError> {
        let response = self
            .request(
                fabro_http::Method::POST,
                &format!("/api/bridge/projects/{binding}/register-authoring"),
                Some(actor),
                Some(&body),
                REGISTRATION_TIMEOUT,
            )
            .await?;
        Self::expect_success(response)
    }

    /// Upgrade the binding to the factory profile; no provisioning happens
    /// here.
    pub(crate) async fn configure_execution(
        &self,
        binding: &str,
        actor: &IntakeActor,
        body: Value,
    ) -> Result<Value, IntakeBridgeError> {
        self.post_json(
            binding,
            "/configure-execution",
            actor,
            body,
            REGISTRATION_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn detach(
        &self,
        binding: &str,
        actor: &IntakeActor,
    ) -> Result<Value, IntakeBridgeError> {
        self.delete_json(binding, "/binding", actor, ORDINARY_TIMEOUT)
            .await
    }

    pub(crate) async fn pause(
        &self,
        binding: &str,
        actor: &IntakeActor,
        paused: bool,
        reason: &str,
    ) -> Result<Value, IntakeBridgeError> {
        self.post_json(
            binding,
            "/pause",
            actor,
            serde_json::json!({ "paused": paused, "reason": reason }),
            ORDINARY_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn recheck_readiness(
        &self,
        binding: &str,
        actor: &IntakeActor,
    ) -> Result<IntakeReadinessReport, IntakeBridgeError> {
        let value = self
            .post_json(
                binding,
                "/readiness/recheck",
                actor,
                Value::Null,
                ORDINARY_TIMEOUT,
            )
            .await?;
        serde_json::from_value(value).map_err(|_| IntakeBridgeError::Payload)
    }

    pub(crate) async fn list_initiatives(
        &self,
        binding: &str,
        actor: &IntakeActor,
    ) -> Result<Value, IntakeBridgeError> {
        self.get_json(binding, "/initiatives", actor, ORDINARY_TIMEOUT)
            .await
    }

    pub(crate) async fn create_initiative(
        &self,
        binding: &str,
        actor: &IntakeActor,
        body: Value,
    ) -> Result<Value, IntakeBridgeError> {
        self.post_json(binding, "/initiatives", actor, body, ORDINARY_TIMEOUT)
            .await
    }

    pub(crate) async fn initiative(
        &self,
        binding: &str,
        actor: &IntakeActor,
        issue: &str,
    ) -> Result<Value, IntakeBridgeError> {
        self.get_json(
            binding,
            &format!("/initiatives/{issue}"),
            actor,
            ORDINARY_TIMEOUT,
        )
        .await
    }

    pub(crate) async fn history(
        &self,
        binding: &str,
        actor: &IntakeActor,
        issue: &str,
    ) -> Result<Value, IntakeBridgeError> {
        self.get_json(
            binding,
            &format!("/initiatives/{issue}/history"),
            actor,
            ORDINARY_TIMEOUT,
        )
        .await
    }

    /// Every initiative-scoped action shares one shape: a fixed suffix, an
    /// optional JSON body, and the ordinary timeout.
    pub(crate) async fn initiative_action(
        &self,
        binding: &str,
        actor: &IntakeActor,
        issue: &str,
        action: &str,
        body: Value,
    ) -> Result<Value, IntakeBridgeError> {
        self.post_json(
            binding,
            &format!("/initiatives/{issue}/{action}"),
            actor,
            body,
            ORDINARY_TIMEOUT,
        )
        .await
    }

    /// Open an advisor stream. The caller streams the body through and never
    /// buffers it; closing the stream publishes nothing.
    pub(crate) async fn chat_stream(
        &self,
        binding: &str,
        actor: &IntakeActor,
        key: &str,
        text: &str,
    ) -> Result<fabro_http::Response, IntakeBridgeError> {
        let url = format!(
            "{}{}",
            self.base,
            Self::path(binding, &format!("/chat/{key}"))
        );
        let response = self
            .client
            .post(&url)
            .headers(Self::actor_headers(Some(actor)))
            .json(&serde_json::json!({ "text": text }))
            .timeout(CHAT_IDLE_TIMEOUT)
            .send()
            .await
            .map_err(|error| map_transport(&error))?;
        let status = response.status().as_u16();
        if status == 200 {
            return Ok(response);
        }
        let body = read_body(response).await?;
        Err(status_error(status, &body))
    }

    pub(crate) async fn chat_close(
        &self,
        binding: &str,
        actor: &IntakeActor,
        key: &str,
    ) -> Result<Value, IntakeBridgeError> {
        self.delete_json(binding, &format!("/chat/{key}"), actor, ORDINARY_TIMEOUT)
            .await
    }

    fn path(binding: &str, suffix: &str) -> String {
        format!("/api/p/{binding}{suffix}")
    }

    fn actor_headers(actor: Option<&IntakeActor>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(actor) = actor {
            if let (Ok(value), Ok(login)) = (
                HeaderValue::from_str(actor.actor()),
                HeaderValue::from_str(actor.login()),
            ) {
                headers.insert("x-fabro-actor", value);
                headers.insert("x-fabro-login", login);
            }
        }
        headers
    }

    async fn request(
        &self,
        method: fabro_http::Method,
        path: &str,
        actor: Option<&IntakeActor>,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Result<BridgeResponse, IntakeBridgeError> {
        let url = format!("{}{path}", self.base);
        let mut builder = self
            .client
            .request(method, &url)
            .headers(Self::actor_headers(actor))
            .timeout(timeout);
        if let Some(body) = body {
            if body.is_null() {
                // A null body means "no body": the bridge's request models are
                // optional where a caller has nothing to add.
            } else {
                let encoded = serde_json::to_vec(body).map_err(|_| IntakeBridgeError::Payload)?;
                if encoded.len() > MAX_MUTATION_BODY_BYTES {
                    return Err(IntakeBridgeError::Refused(
                        "intake request body is too large".to_string(),
                    ));
                }
                builder = builder.body(encoded);
            }
        }
        let response = builder
            .send()
            .await
            .map_err(|error| map_transport(&error))?;
        let status = response.status().as_u16();
        let body = read_body(response).await?;
        Ok(BridgeResponse { status, body })
    }

    fn expect_success(response: BridgeResponse) -> Result<Value, IntakeBridgeError> {
        if (200..300).contains(&response.status) {
            return Ok(response.body);
        }
        Err(status_error(response.status, &response.body))
    }

    async fn get_json(
        &self,
        binding: &str,
        suffix: &str,
        actor: &IntakeActor,
        timeout: Duration,
    ) -> Result<Value, IntakeBridgeError> {
        let response = self
            .request(
                fabro_http::Method::GET,
                &Self::path(binding, suffix),
                Some(actor),
                None,
                timeout,
            )
            .await?;
        Self::expect_success(response)
    }

    async fn post_json(
        &self,
        binding: &str,
        suffix: &str,
        actor: &IntakeActor,
        body: Value,
        timeout: Duration,
    ) -> Result<Value, IntakeBridgeError> {
        let response = self
            .request(
                fabro_http::Method::POST,
                &Self::path(binding, suffix),
                Some(actor),
                Some(&body),
                timeout,
            )
            .await?;
        Self::expect_success(response)
    }

    async fn delete_json(
        &self,
        binding: &str,
        suffix: &str,
        actor: &IntakeActor,
        timeout: Duration,
    ) -> Result<Value, IntakeBridgeError> {
        let response = self
            .request(
                fabro_http::Method::DELETE,
                &Self::path(binding, suffix),
                Some(actor),
                None,
                timeout,
            )
            .await?;
        Self::expect_success(response)
    }
}

fn map_transport(error: &reqwest::Error) -> IntakeBridgeError {
    if error.is_timeout() {
        IntakeBridgeError::Timeout
    } else {
        IntakeBridgeError::Unreachable
    }
}

/// Read a bounded response body. An oversized body is a payload error, never a
/// truncated success.
async fn read_body(response: fabro_http::Response) -> Result<Value, IntakeBridgeError> {
    let bytes = response
        .bytes()
        .await
        .map_err(|error| map_transport(&error))?;
    if bytes.len() > MAX_RESPONSE_BODY_BYTES {
        return Err(IntakeBridgeError::Payload);
    }
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).map_err(|_| IntakeBridgeError::Payload)
}

fn status_error(status: u16, body: &Value) -> IntakeBridgeError {
    let detail = body
        .get("detail")
        .and_then(Value::as_str)
        .filter(|detail| !detail.trim().is_empty())
        .map(str::to_string);
    match status {
        400..=499 => detail.map_or(
            IntakeBridgeError::Refused(format!("feature intake refused the request ({status})")),
            IntakeBridgeError::Refused,
        ),
        _ => IntakeBridgeError::Failed,
    }
}

#[cfg(test)]
mod tests {
    use fabro_types::settings::server::IntakeIntegrationSettings;
    use fabro_types::{AuthMethod, IdpIdentity, UserPrincipal};

    use super::{ACTOR_PREFIX, IntakeActor, IntakeBridge, IntakeBridgeError, status_error};

    fn principal(issuer: &str, subject: &str, login: &str) -> UserPrincipal {
        UserPrincipal {
            identity:    IdpIdentity::new(issuer, subject).unwrap(),
            login:       login.to_string(),
            auth_method: AuthMethod::DevToken,
            avatar_url:  None,
        }
    }

    #[test]
    fn actor_is_stable_per_identity_and_never_carries_the_login() {
        let actor = IntakeActor::from_principal(&principal("issuer", "subject", "el-telar"));
        assert!(actor.actor().starts_with(ACTOR_PREFIX));
        assert_eq!(actor.actor().len(), ACTOR_PREFIX.len() + 32);
        assert!(
            actor
                .actor()
                .chars()
                .skip(ACTOR_PREFIX.len())
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert!(!actor.actor().contains("el-telar"));
        assert_eq!(actor.login(), "el-telar");

        let same = IntakeActor::from_principal(&principal("issuer", "subject", "other-login"));
        assert_eq!(same.actor(), actor.actor());
        let different = IntakeActor::from_principal(&principal("issuer", "other", "el-telar"));
        assert_ne!(different.actor(), actor.actor());
    }

    fn settings(enabled: bool, socket: Option<&str>) -> IntakeIntegrationSettings {
        IntakeIntegrationSettings {
            enabled,
            socket: socket.map(str::to_string),
        }
    }

    #[test]
    fn disabled_or_socketless_configuration_never_builds_a_client() {
        assert!(matches!(
            IntakeBridge::from_settings(&settings(false, None)),
            Ok(None)
        ));
        assert!(matches!(
            IntakeBridge::from_settings(&settings(true, None)),
            Err(IntakeBridgeError::Disabled)
        ));
        assert!(matches!(
            IntakeBridge::from_settings(&settings(true, Some("   "))),
            Err(IntakeBridgeError::Disabled)
        ));
    }

    #[test]
    fn a_complete_configuration_builds_a_client() {
        let bridge = IntakeBridge::from_settings(&settings(true, Some("/run/fabro/intake.sock")))
            .expect("socket client should build");
        assert!(bridge.is_some());
    }

    #[test]
    fn upstream_domain_messages_pass_through_but_server_failures_do_not() {
        let refusal = serde_json::json!({ "detail": "Proyecto pausado" });
        assert!(matches!(
            status_error(409, &refusal),
            IntakeBridgeError::Refused(message) if message == "Proyecto pausado"
        ));
        let failure = serde_json::json!({ "detail": "Traceback: /home/operator/secret" });
        assert!(matches!(
            status_error(500, &failure),
            IntakeBridgeError::Failed
        ));
    }
}
