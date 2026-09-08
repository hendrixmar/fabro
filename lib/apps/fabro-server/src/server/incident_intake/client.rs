use anyhow::{Context as _, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::super::AppState;

pub(super) const BODY_LIMIT: usize = 262_144;
pub(super) const ISSUES_PATH: &str = "/api/canonical/0/issues/";

pub(super) struct BugsinkClient {
    pub origin: String,
    http: reqwest::Client,
    token: String,
}

pub(super) fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).no_proxy()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(15)).build()?)
}

pub(super) async fn bounded_json(mut response: reqwest::Response) -> anyhow::Result<Value> {
    ensure!(response.status().is_success(), "upstream_unavailable");
    ensure!(response.content_length().is_none_or(|n| n <= BODY_LIMIT as u64), "upstream_oversized");
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(bytes.len() + chunk.len() <= BODY_LIMIT, "upstream_oversized");
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes).context("upstream_malformed")?)
}

#[derive(Clone, Debug)]
pub(super) struct Issue {
    pub project: u64,
    pub id: Uuid,
    pub resolved: bool,
    pub muted: bool,
    pub first_seen_ms: i64,
}

impl Issue {
    pub fn parse(value: &Value, project: u64, expected: Option<Uuid>) -> anyhow::Result<Self> {
        let id = value.get("id").and_then(Value::as_str).context("issue_identity_missing")?.parse::<Uuid>()?;
        ensure!(value.get("project").and_then(Value::as_u64) == Some(project)
            && expected.is_none_or(|expected| expected == id), "issue_identity_mismatch");
        let resolved = value.get("is_resolved").and_then(Value::as_bool).context("issue_state_missing")?;
        let muted = value.get("is_muted").and_then(Value::as_bool).context("issue_state_missing")?;
        let first_seen_ms = chrono::DateTime::parse_from_rfc3339(value.get("first_seen")
            .and_then(Value::as_str).context("issue_timestamp_missing")?)?.timestamp_millis();
        Ok(Self { project, id, resolved, muted, first_seen_ms })
    }
}

impl BugsinkClient {
    pub async fn new(state: &AppState) -> anyhow::Result<Self> {
        let settings = state.server_settings();
        let config = &settings.server.integrations.bugsink;
        let origin = config.origin.clone().context("bugsink_not_configured")?;
        let name = config.api_token_secret.as_deref().context("bugsink_not_configured")?;
        let token = state.vault_secret(name).await?.filter(|s| !s.trim().is_empty()).context("bugsink_credential_unavailable")?;
        Ok(Self { origin, http: http_client()?, token })
    }

    async fn get(&self, url: url::Url) -> anyhow::Result<Value> {
        let response = self.http.get(url).bearer_auth(&self.token).send().await?;
        ensure!(response.status() != reqwest::StatusCode::NOT_FOUND, "upstream_not_found_or_retained");
        bounded_json(response).await
    }

    pub async fn issue(&self, project: u64, id: Uuid) -> anyhow::Result<(Issue, Uuid)> {
        let issue = Issue::parse(&self.get(url::Url::parse(&format!("{}{ISSUES_PATH}{id}/", self.origin))?).await?, project, Some(id))?;
        let mut url = url::Url::parse(&format!("{}/api/canonical/0/events/", self.origin))?;
        url.query_pairs_mut().append_pair("issue", &id.to_string()).append_pair("order", "desc").append_pair("limit", "1");
        let page = self.get(url).await?;
        let event = page.get("results").and_then(Value::as_array).and_then(|rows| rows.first()).context("event_not_found_or_retained")?;
        let event_id = event.get("id").and_then(Value::as_str).context("event_identity_missing")?.parse::<Uuid>()?;
        ensure!(event.get("project").and_then(Value::as_u64) == Some(project)
            && event.get("issue").and_then(Value::as_str) == Some(id.to_string().as_str()), "event_identity_mismatch");
        Ok((issue, event_id))
    }

    pub async fn page(&self, project: u64, cursor: Option<&str>) -> anyhow::Result<Value> {
        let mut url = url::Url::parse(&format!("{}{ISSUES_PATH}", self.origin))?;
        url.query_pairs_mut().append_pair("project", &project.to_string()).append_pair("sort", "digest_order").append_pair("order", "asc");
        if let Some(cursor) = cursor { url.query_pairs_mut().append_pair("cursor", cursor); }
        self.get(url).await
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScanProgress {
    pub cursor: Option<String>,
    pub seen: Vec<String>,
    #[serde(default)]
    pub seen_issues: std::collections::BTreeSet<Uuid>,
    pub read_attempts: u8,
    pub parked_reason: Option<String>,
    pub import_complete: bool,
    #[serde(default)]
    pub legacy_import: Option<Value>,
    #[serde(default)]
    pub operator_audit: Vec<Value>,
}

impl ScanProgress {
    pub fn advance(&mut self, origin: &str, project: u64, next: Option<&str>) -> anyhow::Result<()> {
        let Some(next) = next else { self.cursor = None; return Ok(()); };
        let next = url::Url::parse(next)?;
        ensure!(next.origin().ascii_serialization() == origin && next.path() == ISSUES_PATH
            && next.username().is_empty() && next.password().is_none() && next.fragment().is_none(), "invalid_scan_destination");
        let pairs: Vec<_> = next.query_pairs().collect();
        for (key, expected) in [("project", project.to_string()), ("sort", "digest_order".to_owned()), ("order", "asc".to_owned())] {
            let values: Vec<_> = pairs.iter().filter(|(k, _)| k == key).map(|(_, v)| v.as_ref()).collect();
            ensure!(values == [expected.as_str()], "scan_coverage_changed");
        }
        ensure!(pairs.iter().all(|(key, _)| matches!(key.as_ref(), "project" | "sort" | "order" | "cursor" | "limit")), "invalid_scan_query");
        let cursors: Vec<_> = pairs.iter().filter(|(key, _)| key == "cursor").map(|(_, value)| value.as_ref()).collect();
        ensure!(cursors.len() == 1, "invalid_scan_cursor");
        let cursor = cursors[0];
        ensure!(!cursor.is_empty() && cursor.len() <= 1024 && cursor.bytes().all(|c| c.is_ascii_alphanumeric() || b"+/=_-".contains(&c))
            && !self.seen.iter().any(|old| old == cursor) && self.seen.len() < 100_000, "invalid_or_repeated_scan_cursor");
        self.seen.push(cursor.to_owned());
        self.cursor = Some(cursor.to_owned());
        Ok(())
    }
}
