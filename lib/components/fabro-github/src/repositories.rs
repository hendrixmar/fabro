//! Repository enumeration for the server's configured GitHub credentials.
//!
//! The native project picker lists what the Fabro *server* can see, which is
//! not necessarily what the logged-in operator's browser can see. Three
//! credential shapes are supported, matching [`GitHubCredentials`]:
//!
//! * a personal access token enumerates `/user/repos` across every accessible
//!   affiliation;
//! * a static installation token enumerates its own
//!   `/installation/repositories`;
//! * GitHub App credentials enumerate `/app/installations`, then mint a
//!   read-scoped installation token per installation to enumerate its
//!   repositories.
//!
//! Every page request is bounded to [`REPOSITORIES_PER_PAGE`] repositories and
//! follows only the configured GitHub API origin supplied by the caller.

use serde::{Deserialize, Serialize};

use crate::{HttpClient, HttpMethod};

/// Maximum repositories requested per upstream page.
pub const REPOSITORIES_PER_PAGE: u32 = 100;

const USER_AGENT: &str = "fabro-server";

/// Public metadata for one repository visible to the server credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositorySummary {
    /// Immutable numeric GitHub repository id.
    pub id:             u64,
    pub full_name:      String,
    pub default_branch: Option<String>,
    pub private:        bool,
    pub archived:       bool,
    pub disabled:       bool,
}

/// One page of repository enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPage {
    pub repositories: Vec<RepositorySummary>,
    /// Whether the caller should request the next page.
    pub has_more:     bool,
}

/// One page of `/app/installations` results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallationPage {
    pub installation_ids: Vec<u64>,
    pub has_more:         bool,
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryEnumerationError {
    #[error("GitHub credentials are not configured on this server")]
    MissingCredentials,
    #[error("GitHub rejected the server's credentials; refresh GITHUB_TOKEN or the GitHub App key")]
    Unauthorized,
    #[error("GitHub rate limit reached; wait for the reset window and retry")]
    RateLimited,
    #[error("GitHub API request failed with status {status}")]
    Status { status: u16 },
    #[error("GitHub returned a response this server could not read")]
    Response,
    #[error("repository {slug} is not accessible to this Fabro server")]
    RepositoryNotFound { slug: String },
    #[error("GitHub request failed")]
    Transport {
        #[source]
        source: anyhow::Error,
    },
}

#[derive(Debug, Deserialize)]
struct ApiRepository {
    id:             u64,
    full_name:      String,
    default_branch: Option<String>,
    #[serde(default)]
    private:        bool,
    #[serde(default)]
    archived:       bool,
    #[serde(default)]
    disabled:       bool,
}

impl From<ApiRepository> for RepositorySummary {
    fn from(value: ApiRepository) -> Self {
        Self {
            id:             value.id,
            full_name:      value.full_name,
            default_branch: value.default_branch,
            private:        value.private,
            archived:       value.archived,
            disabled:       value.disabled,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApiInstallation {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct ApiInstallationRepositories {
    #[serde(default)]
    repositories: Vec<ApiRepository>,
}

/// Fetch the canonical metadata for one repository with an authenticated read.
pub async fn fetch_repository(
    client: &impl HttpClient,
    token: &str,
    slug: &str,
    base_url: &str,
) -> Result<RepositorySummary, RepositoryEnumerationError> {
    let response = get(client, &format!("{base_url}/repos/{slug}"), token, 1).await?;
    match response.status {
        200 => response
            .json::<ApiRepository>()
            .map(RepositorySummary::from)
            .map_err(|_| RepositoryEnumerationError::Response),
        401 => Err(RepositoryEnumerationError::Unauthorized),
        403 => Err(rate_limited_or_forbidden(&response)),
        404 => Err(RepositoryEnumerationError::RepositoryNotFound {
            slug: slug.to_string(),
        }),
        status => Err(RepositoryEnumerationError::Status { status }),
    }
}

/// Enumerate one page of `/user/repos` for a personal access token.
pub async fn list_user_repositories(
    client: &impl HttpClient,
    token: &str,
    page: u32,
    base_url: &str,
) -> Result<RepositoryPage, RepositoryEnumerationError> {
    let url = format!(
        "{base_url}/user/repos?per_page={REPOSITORIES_PER_PAGE}&page={page}&sort=full_name&direction=asc&affiliation=owner,collaborator,organization_member"
    );
    let response = get(client, &url, token, page).await?;
    match response.status {
        200 => {
            let repositories = response
                .json::<Vec<ApiRepository>>()
                .map_err(|_| RepositoryEnumerationError::Response)?;
            Ok(page_from(repositories))
        }
        401 => Err(RepositoryEnumerationError::Unauthorized),
        403 => Err(rate_limited_or_forbidden(&response)),
        status => Err(RepositoryEnumerationError::Status { status }),
    }
}

/// Enumerate one page of `/installation/repositories` for an installation
/// token (either a static `ghs_` token or one minted from App credentials).
pub async fn list_installation_repositories(
    client: &impl HttpClient,
    token: &str,
    page: u32,
    base_url: &str,
) -> Result<RepositoryPage, RepositoryEnumerationError> {
    let url = format!(
        "{base_url}/installation/repositories?per_page={REPOSITORIES_PER_PAGE}&page={page}"
    );
    let response = get(client, &url, token, page).await?;
    match response.status {
        200 => {
            let body = response
                .json::<ApiInstallationRepositories>()
                .map_err(|_| RepositoryEnumerationError::Response)?;
            Ok(page_from(body.repositories))
        }
        401 => Err(RepositoryEnumerationError::Unauthorized),
        403 => Err(rate_limited_or_forbidden(&response)),
        status => Err(RepositoryEnumerationError::Status { status }),
    }
}

/// Enumerate one page of `/app/installations` with a GitHub App JWT.
pub async fn list_app_installations(
    client: &impl HttpClient,
    jwt: &str,
    page: u32,
    base_url: &str,
) -> Result<InstallationPage, RepositoryEnumerationError> {
    let url = format!("{base_url}/app/installations?per_page={REPOSITORIES_PER_PAGE}&page={page}");
    let response = get(client, &url, jwt, page).await?;
    match response.status {
        200 => {
            let installations = response
                .json::<Vec<ApiInstallation>>()
                .map_err(|_| RepositoryEnumerationError::Response)?;
            let has_more =
                u32::try_from(installations.len()).unwrap_or(u32::MAX) >= REPOSITORIES_PER_PAGE;
            Ok(InstallationPage {
                installation_ids: installations.into_iter().map(|entry| entry.id).collect(),
                has_more,
            })
        }
        401 => Err(RepositoryEnumerationError::Unauthorized),
        403 => Err(rate_limited_or_forbidden(&response)),
        status => Err(RepositoryEnumerationError::Status { status }),
    }
}

/// Mint a read-scoped installation token for one App installation.
///
/// The permission set is deliberately read-only: repository enumeration must
/// never need write access.
pub async fn mint_read_installation_token(
    client: &impl HttpClient,
    jwt: &str,
    installation_id: u64,
    base_url: &str,
) -> Result<String, RepositoryEnumerationError> {
    let url = format!("{base_url}/app/installations/{installation_id}/access_tokens");
    let response = client
        .request(
            HttpMethod::Post,
            &url,
            &[
                ("Authorization", &format!("Bearer {jwt}")),
                ("Accept", "application/vnd.github+json"),
                ("User-Agent", USER_AGENT),
            ],
            Some(&serde_json::json!({ "permissions": { "contents": "read" } })),
        )
        .await
        .map_err(|source| RepositoryEnumerationError::Transport { source })?;
    match response.status {
        200 | 201 => response
            .json::<InstallationTokenBody>()
            .map(|body| body.token)
            .map_err(|_| RepositoryEnumerationError::Response),
        401 => Err(RepositoryEnumerationError::Unauthorized),
        403 => Err(rate_limited_or_forbidden(&response)),
        status => Err(RepositoryEnumerationError::Status { status }),
    }
}

#[derive(Debug, Deserialize)]
struct InstallationTokenBody {
    token: String,
}

fn page_from(repositories: Vec<ApiRepository>) -> RepositoryPage {
    let has_more = u32::try_from(repositories.len()).unwrap_or(u32::MAX) >= REPOSITORIES_PER_PAGE;
    RepositoryPage {
        repositories: repositories
            .into_iter()
            .map(RepositorySummary::from)
            .collect(),
        has_more,
    }
}

async fn get(
    client: &impl HttpClient,
    url: &str,
    token: &str,
    _page: u32,
) -> Result<crate::HttpResponse, RepositoryEnumerationError> {
    client
        .request(
            HttpMethod::Get,
            url,
            &[
                ("Authorization", &format!("Bearer {token}")),
                ("Accept", "application/vnd.github+json"),
                ("User-Agent", USER_AGENT),
            ],
            None,
        )
        .await
        .map_err(|source| RepositoryEnumerationError::Transport { source })
}

/// Distinguish an exhausted rate limit from a genuine authorization failure so
/// the UI can tell the operator whether to wait or to fix credentials.
fn rate_limited_or_forbidden(response: &crate::HttpResponse) -> RepositoryEnumerationError {
    let body = response.text().to_ascii_lowercase();
    if body.contains("rate limit") || body.contains("secondary rate") {
        RepositoryEnumerationError::RateLimited
    } else {
        RepositoryEnumerationError::Status {
            status: response.status,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::executor::block_on;
    use parking_lot::Mutex;

    use super::{
        RepositoryEnumerationError, list_app_installations, list_installation_repositories,
        list_user_repositories,
    };
    use crate::{HttpClient, HttpMethod, HttpResponse};

    /// Scripted transport that returns canned bodies per URL substring.
    #[derive(Default)]
    struct FakeHttp {
        routes: Mutex<HashMap<String, (u16, String)>>,
        urls:   Mutex<Vec<String>>,
    }

    impl FakeHttp {
        fn with(self, pattern: &str, status: u16, body: &str) -> Self {
            self.routes
                .lock()
                .insert(pattern.to_string(), (status, body.to_string()));
            self
        }

        fn requested(&self) -> Vec<String> {
            self.urls.lock().clone()
        }
    }

    impl HttpClient for FakeHttp {
        async fn request(
            &self,
            _method: HttpMethod,
            url: &str,
            _headers: &[(&str, &str)],
            _body: Option<&serde_json::Value>,
        ) -> anyhow::Result<HttpResponse> {
            self.urls.lock().push(url.to_string());
            for (pattern, (status, body)) in self.routes.lock().iter() {
                if url.contains(pattern.as_str()) {
                    return Ok(HttpResponse::new(*status, body.clone()));
                }
            }
            Ok(HttpResponse::new(
                404,
                "{\"message\":\"Not Found\"}".to_string(),
            ))
        }
    }

    fn repo(id: u64, name: &str) -> String {
        format!(
            "{{\"id\":{id},\"full_name\":\"{name}\",\"default_branch\":\"main\",\"private\":true,\"archived\":false,\"disabled\":false}}"
        )
    }

    #[test]
    fn pat_repositories_parse_and_assert_no_secret_in_url() {
        let http = FakeHttp::default().with("/user/repos", 200, &format!("[{}]", repo(7, "o/r")));
        let page = block_on(list_user_repositories(
            &http,
            "secret-token",
            1,
            "https://api.github.com",
        ))
        .unwrap();
        assert_eq!(page.repositories.len(), 1);
        assert_eq!(page.repositories[0].id, 7);
        assert!(!page.has_more);
        let url = &http.requested()[0];
        assert!(!url.contains("secret-token"));
        assert!(url.contains("affiliation=owner,collaborator,organization_member"));
        assert!(url.contains("page=1"));
    }

    #[test]
    fn full_page_reports_another_page_is_available() {
        let bodies = (0..super::REPOSITORIES_PER_PAGE)
            .map(|index| repo(u64::from(index) + 1, "o/r"))
            .collect::<Vec<_>>()
            .join(",");
        let http = FakeHttp::default().with(
            "/installation/repositories",
            200,
            &format!("{{\"total_count\":200,\"repositories\":[{bodies}]}}"),
        );
        let page = block_on(list_installation_repositories(
            &http,
            "ghs_token",
            2,
            "https://api.github.com",
        ))
        .unwrap();
        assert_eq!(page.repositories.len(), 100);
        assert!(page.has_more);
        assert!(http.requested()[0].contains("page=2"));
    }

    #[test]
    fn rate_limited_page_is_an_error_not_an_empty_list() {
        let http = FakeHttp::default().with(
            "/user/repos",
            403,
            "{\"message\":\"API rate limit exceeded\"}",
        );
        let error = block_on(list_user_repositories(
            &http,
            "token",
            1,
            "https://api.github.com",
        ))
        .unwrap_err();
        assert!(matches!(error, RepositoryEnumerationError::RateLimited));
    }

    #[test]
    fn app_installations_are_enumerated_with_the_jwt() {
        let http = FakeHttp::default().with("/app/installations", 200, "[{\"id\":42},{\"id\":43}]");
        let page = block_on(list_app_installations(
            &http,
            "jwt",
            1,
            "https://api.github.com",
        ))
        .unwrap();
        assert_eq!(page.installation_ids, vec![42, 43]);
        assert!(!page.has_more);
    }
}
