//! GitHub repository reads shared by the native project picker and project
//! creation.
//!
//! Both paths read canonical repository metadata with the server's own GitHub
//! credentials. The browser never sees a token and never supplies a URL: the
//! picker cursor carries only validated page/installation positions, and the
//! direct `owner/repo` fallback is re-read here before anything is persisted.

use std::sync::Arc;

use axum_extra::extract::Query as ExtraQuery;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fabro_api::types::{GithubRepository, GithubRepositoryId, GithubRepositoryListResponse};
use fabro_github::repositories::{
    self, REPOSITORIES_PER_PAGE, RepositoryEnumerationError, RepositorySummary,
};
use fabro_types::settings::server::{GithubIntegrationSettings, GithubIntegrationStrategy};
use serde::{Deserialize, Serialize};

use super::super::{
    ApiError, AppState, IntoResponse, Json, RequiredUser, Response, Router, State, StatusCode, get,
};

const CURSOR_VERSION: u32 = 1;

/// Which credential shape produced a cursor. A cursor minted under one shape
/// is rejected under another so a page position can never be replayed against
/// a different credential's repository set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReaderMode {
    Pat,
    Installation,
    App,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RepositoryCursor {
    v:  u32,
    m:  ReaderMode,
    /// 1-based page of `/user/repos` or `/installation/repositories`.
    #[serde(default = "first_page", skip_serializing_if = "is_first_page")]
    p:  u32,
    /// 1-based page of `/app/installations`.
    #[serde(default = "first_page", skip_serializing_if = "is_first_page")]
    ip: u32,
    /// Index of the installation within that page.
    #[serde(default, skip_serializing_if = "is_zero")]
    i:  u32,
}

fn first_page() -> u32 {
    1
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if helpers receive borrowed field values"
)]
fn is_first_page(value: &u32) -> bool {
    *value <= 1
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if helpers receive borrowed field values"
)]
fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl RepositoryCursor {
    fn start(mode: ReaderMode) -> Self {
        Self {
            v:  CURSOR_VERSION,
            m:  mode,
            p:  1,
            ip: 1,
            i:  0,
        }
    }

    fn encode(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("repository cursor is JSON-serializable");
        URL_SAFE_NO_PAD.encode(bytes)
    }

    fn decode(value: &str) -> Result<Self, ApiError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| invalid_cursor())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())?;
        if cursor.v != CURSOR_VERSION
            || cursor.p == 0
            || cursor.ip == 0
            || cursor.i >= REPOSITORIES_PER_PAGE
        {
            return Err(invalid_cursor());
        }
        Ok(cursor)
    }
}

fn invalid_cursor() -> ApiError {
    ApiError::with_code(
        StatusCode::BAD_REQUEST,
        "repository cursor is invalid or expired",
        "github_repository_cursor_invalid",
    )
}

#[derive(Debug, Deserialize)]
struct RepositoryListQuery {
    cursor: Option<String>,
}

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/repos/github", get(list_github_repositories))
}

async fn list_github_repositories(
    _auth: RequiredUser,
    State(state): State<Arc<AppState>>,
    ExtraQuery(query): ExtraQuery<RepositoryListQuery>,
) -> Result<Response, ApiError> {
    let reader = GithubReader::load(state.as_ref()).await?;
    let cursor = match query
        .cursor
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        Some(value) => {
            let cursor = RepositoryCursor::decode(value)?;
            if cursor.m == reader.mode() {
                cursor
            } else {
                return Err(invalid_cursor());
            }
        }
        None => RepositoryCursor::start(reader.mode()),
    };

    let page = reader.page(&cursor).await?;
    let data = page
        .repositories
        .into_iter()
        .map(repository_response)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        StatusCode::OK,
        Json(GithubRepositoryListResponse {
            data,
            next_cursor: page.next_cursor.map(|cursor| cursor.encode()),
        }),
    )
        .into_response())
}

/// Read canonical metadata for one repository with the server credentials.
///
/// Reuses the same credential resolution as the picker so a repository that
/// the picker listed is always re-verified before it is persisted, and a
/// repository omitted from enumeration can still be connected explicitly.
pub(super) async fn read_repository(
    state: &AppState,
    slug: &str,
) -> Result<RepositorySummary, ApiError> {
    let reader = GithubReader::load(state).await?;
    reader.fetch(slug).await
}

fn repository_response(summary: RepositorySummary) -> Result<GithubRepository, ApiError> {
    let id = GithubRepositoryId::try_from(summary.id.to_string()).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "GitHub returned a repository id this server cannot represent",
        )
    })?;
    Ok(GithubRepository {
        archived: summary.archived,
        default_branch: summary.default_branch,
        disabled: summary.disabled,
        full_name: summary.full_name,
        id,
        private: summary.private,
    })
}

struct GithubPage {
    repositories: Vec<RepositorySummary>,
    next_cursor:  Option<RepositoryCursor>,
}

/// Resolved GitHub credentials plus the transport used to read repositories.
pub(super) struct GithubReader {
    credentials: ReaderCredentials,
    client:      fabro_http::HttpClient,
    base_url:    String,
}

enum ReaderCredentials {
    Pat(String),
    Installation(String),
    App(String),
}

impl GithubReader {
    fn mode(&self) -> ReaderMode {
        match self.credentials {
            ReaderCredentials::Pat(_) => ReaderMode::Pat,
            ReaderCredentials::Installation(_) => ReaderMode::Installation,
            ReaderCredentials::App(_) => ReaderMode::App,
        }
    }

    pub(super) async fn load(state: &AppState) -> Result<Self, ApiError> {
        let settings = state.server_settings();
        let github_settings = &settings.server.integrations.github;
        let base_url = fabro_github::github_api_base_url();
        let client = state
            .http_client()
            .map_err(|err| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, err.to_string()))?;

        match github_settings.strategy {
            GithubIntegrationStrategy::App => {
                let credentials = load_credentials(state, github_settings).await?;
                let Some(fabro_github::GitHubCredentials::App(creds)) = credentials else {
                    return Err(missing_credentials(github_settings));
                };
                let jwt = fabro_github::sign_app_jwt(&creds.app_id, &creds.private_key_pem)
                    .map_err(|_| github_unavailable())?;
                Ok(Self {
                    credentials: ReaderCredentials::App(jwt),
                    client,
                    base_url,
                })
            }
            GithubIntegrationStrategy::Token => {
                let credentials = load_credentials(state, github_settings).await?;
                let token = match credentials {
                    Some(fabro_github::GitHubCredentials::Pat(token)) => {
                        ReaderCredentials::Pat(token)
                    }
                    Some(fabro_github::GitHubCredentials::Installation(token)) => {
                        let token = token.valid_token().map_err(|_| github_unavailable())?;
                        ReaderCredentials::Installation(token.to_string())
                    }
                    Some(fabro_github::GitHubCredentials::App(_)) | None => {
                        return Err(missing_credentials(github_settings));
                    }
                };
                Ok(Self {
                    credentials: token,
                    client,
                    base_url,
                })
            }
        }
    }

    async fn fetch(&self, slug: &str) -> Result<RepositorySummary, ApiError> {
        let token = match &self.credentials {
            ReaderCredentials::Pat(token) | ReaderCredentials::Installation(token) => token,
            // App credentials can only read a repository through an
            // installation token; resolve the installation for that slug.
            ReaderCredentials::App(jwt) => {
                return self.fetch_with_app(&self.client, jwt, slug).await;
            }
        };
        repositories::fetch_repository(&self.client, token, slug, &self.base_url)
            .await
            .map_err(repository_error)
    }

    async fn fetch_with_app(
        &self,
        client: &fabro_http::HttpClient,
        jwt: &str,
        slug: &str,
    ) -> Result<RepositorySummary, ApiError> {
        let (owner, repo) = slug.split_once('/').ok_or_else(|| {
            ApiError::with_code(
                StatusCode::UNPROCESSABLE_ENTITY,
                "repository must be an owner/repo slug",
                "project_repository_invalid",
            )
        })?;
        let token = fabro_github::create_installation_access_token_with_permissions(
            client,
            jwt,
            owner,
            repo,
            &self.base_url,
            serde_json::json!({ "contents": "read" }),
        )
        .await
        .map_err(|_| {
            ApiError::with_code(
                StatusCode::UNPROCESSABLE_ENTITY,
                "GitHub App installation for this repository is not available; install the App for the owner and retry",
                "project_repository_not_installed",
            )
        })?;
        repositories::fetch_repository(client, &token, slug, &self.base_url)
            .await
            .map_err(repository_error)
    }

    async fn page(&self, cursor: &RepositoryCursor) -> Result<GithubPage, ApiError> {
        match &self.credentials {
            ReaderCredentials::Pat(token) => {
                let page = repositories::list_user_repositories(
                    &self.client,
                    token,
                    cursor.p,
                    &self.base_url,
                )
                .await
                .map_err(repository_error)?;
                Ok(GithubPage {
                    repositories: page.repositories,
                    next_cursor:  page.has_more.then(|| RepositoryCursor {
                        p: cursor.p + 1,
                        ..cursor.clone()
                    }),
                })
            }
            ReaderCredentials::Installation(token) => {
                let page = repositories::list_installation_repositories(
                    &self.client,
                    token,
                    cursor.p,
                    &self.base_url,
                )
                .await
                .map_err(repository_error)?;
                Ok(GithubPage {
                    repositories: page.repositories,
                    next_cursor:  page.has_more.then(|| RepositoryCursor {
                        p: cursor.p + 1,
                        ..cursor.clone()
                    }),
                })
            }
            ReaderCredentials::App(jwt) => self.app_page(jwt, cursor).await,
        }
    }

    /// App credentials enumerate installations first, then each installation's
    /// repositories with a read-scoped token minted for that installation.
    async fn app_page(&self, jwt: &str, cursor: &RepositoryCursor) -> Result<GithubPage, ApiError> {
        let installations =
            repositories::list_app_installations(&self.client, jwt, cursor.ip, &self.base_url)
                .await
                .map_err(repository_error)?;
        let Some(installation_id) = installations
            .installation_ids
            .get(cursor.i as usize)
            .copied()
        else {
            return Err(invalid_cursor());
        };
        let token = repositories::mint_read_installation_token(
            &self.client,
            jwt,
            installation_id,
            &self.base_url,
        )
        .await
        .map_err(repository_error)?;
        let page = repositories::list_installation_repositories(
            &self.client,
            &token,
            cursor.p,
            &self.base_url,
        )
        .await
        .map_err(repository_error)?;

        let installation_count =
            u32::try_from(installations.installation_ids.len()).unwrap_or(u32::MAX);
        let next_cursor = if page.has_more {
            Some(RepositoryCursor {
                p: cursor.p + 1,
                ..cursor.clone()
            })
        } else if cursor.i + 1 < installation_count {
            Some(RepositoryCursor {
                p: 1,
                i: cursor.i + 1,
                ..cursor.clone()
            })
        } else if installations.has_more {
            Some(RepositoryCursor {
                p: 1,
                i: 0,
                ip: cursor.ip + 1,
                ..cursor.clone()
            })
        } else {
            None
        };

        Ok(GithubPage {
            repositories: page.repositories,
            next_cursor,
        })
    }
}

async fn load_credentials(
    state: &AppState,
    settings: &GithubIntegrationSettings,
) -> Result<Option<fabro_github::GitHubCredentials>, ApiError> {
    state.github_credentials(settings).await.map_err(|err| {
        tracing::error!(error = ?err, "Loading GitHub credentials failed");
        github_unavailable()
    })
}

fn missing_credentials(settings: &GithubIntegrationSettings) -> ApiError {
    let message = match settings.strategy {
        GithubIntegrationStrategy::App => {
            "GitHub App credentials are not configured on this server"
        }
        GithubIntegrationStrategy::Token => {
            "GITHUB_TOKEN is not configured -- run fabro install or fabro secret set GITHUB_TOKEN"
        }
    };
    ApiError::with_code(
        StatusCode::SERVICE_UNAVAILABLE,
        message,
        "github_credentials_missing",
    )
}

fn github_unavailable() -> ApiError {
    ApiError::with_code(
        StatusCode::SERVICE_UNAVAILABLE,
        "GitHub credentials are unavailable",
        "github_credentials_unavailable",
    )
}

fn repository_error(error: RepositoryEnumerationError) -> ApiError {
    match error {
        RepositoryEnumerationError::RepositoryNotFound { slug } => ApiError::with_code(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "{slug} is not accessible with this Fabro server's GitHub credentials; grant access or install the GitHub App for its owner"
            ),
            "project_repository_not_accessible",
        ),
        RepositoryEnumerationError::Unauthorized => ApiError::with_code(
            StatusCode::SERVICE_UNAVAILABLE,
            "GitHub rejected the server's credentials; refresh the token or App key",
            "github_credentials_rejected",
        ),
        RepositoryEnumerationError::RateLimited => ApiError::with_code(
            StatusCode::SERVICE_UNAVAILABLE,
            "GitHub rate limit reached; wait for the reset window and retry",
            "github_rate_limited",
        ),
        RepositoryEnumerationError::MissingCredentials => missing_credentials_default(),
        RepositoryEnumerationError::Status { status } => ApiError::with_code(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("GitHub API request failed with status {status}"),
            "github_request_failed",
        ),
        RepositoryEnumerationError::Response | RepositoryEnumerationError::Transport { .. } => {
            ApiError::with_code(
                StatusCode::SERVICE_UNAVAILABLE,
                "GitHub is unreachable or returned an unreadable response",
                "github_unavailable",
            )
        }
    }
}

fn missing_credentials_default() -> ApiError {
    ApiError::with_code(
        StatusCode::SERVICE_UNAVAILABLE,
        "GitHub credentials are not configured on this server",
        "github_credentials_missing",
    )
}

#[cfg(test)]
mod tests {
    use super::{ReaderMode, RepositoryCursor, RepositoryListQuery};

    #[test]
    fn cursor_round_trips_without_carrying_a_url() {
        let cursor = RepositoryCursor {
            v:  1,
            m:  ReaderMode::App,
            p:  3,
            ip: 2,
            i:  4,
        };
        let encoded = cursor.encode();
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains("github.com"));
        assert_eq!(RepositoryCursor::decode(&encoded).unwrap(), cursor);
    }

    #[test]
    fn cursor_rejects_tampered_or_out_of_range_values() {
        use base64::Engine as _;
        let encode =
            |json: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.as_bytes());
        assert!(RepositoryCursor::decode("not-base64!!").is_err());
        // Zero pages, oversized installation indexes, and unknown versions are
        // rejected even when the payload is well-formed base64 JSON.
        assert!(RepositoryCursor::decode(&encode(r#"{"v":1,"m":"pat","p":0}"#)).is_err());
        assert!(RepositoryCursor::decode(&encode(r#"{"v":1,"m":"app","ip":0}"#)).is_err());
        assert!(RepositoryCursor::decode(&encode(r#"{"v":1,"m":"app","i":100}"#)).is_err());
        assert!(RepositoryCursor::decode(&encode(r#"{"v":9,"m":"pat"}"#)).is_err());
        assert!(RepositoryCursor::decode(&encode(r#"{"v":1,"m":"stolen"}"#)).is_err());
    }

    #[test]
    fn compact_cursor_omits_default_positions() {
        let encoded = RepositoryCursor::start(ReaderMode::Pat).encode();
        let json = String::from_utf8(
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, encoded)
                .unwrap(),
        )
        .unwrap();
        assert!(!json.contains("\"p\""), "unexpected page in {json}");
        assert!(
            !json.contains("\"ip\""),
            "unexpected installation page in {json}"
        );
    }

    #[test]
    fn query_accepts_absent_cursor() {
        let query: RepositoryListQuery = serde_json::from_str("{}").unwrap();
        assert!(query.cursor.is_none());
    }
}
