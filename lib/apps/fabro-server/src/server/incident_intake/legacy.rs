use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, ensure};
use fabro_types::{RunId, RunSpec, RunStatus};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use super::super::AppState;
use super::client::{BODY_LIMIT, bounded_json, http_client};
use super::worker_store::{digest, incident_key};

// Persisted Run projections contain checkpoints and stage history, unlike
// provider payloads.
pub(super) const OWNER_BODY_LIMIT: usize = 2 * 1024 * 1024;

// Import identity and terminal state, not version-specific stage/usage
// metadata.
#[derive(Deserialize)]
struct LegacyRun {
    spec:   RunSpec,
    status: RunStatus,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyConfig {
    schema_version:   u8,
    owners:           BTreeMap<String, Owner>,
    plane_project_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    state_file:   PathBuf,
    api_origin:   String,
    token_secret: String,
}

pub(super) async fn private_json(path: &Path) -> anyhow::Result<Value> {
    use std::os::unix::fs::PermissionsExt;
    // Group and other permissions occupy the six low POSIX mode bits.
    let metadata = fs::symlink_metadata(path).await?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode().trailing_zeros() >= 6
            && metadata.len() <= 1_048_576,
        "legacy_source_not_private"
    );
    let mut bytes = Vec::new();
    File::open(path)
        .await?
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 1_048_576, "legacy_source_oversized");
    Ok(serde_json::from_slice(&bytes)?)
}

fn canonical_incident(value: &str) -> anyhow::Result<(u64, Uuid)> {
    let parts: Vec<_> = value.split(':').collect();
    ensure!(
        parts.len() == 3 && parts[0] == "bugsink",
        "legacy_incident_invalid"
    );
    let project = parts[1].parse::<u64>()?;
    let issue = parts[2].parse::<Uuid>()?;
    ensure!(
        value == format!("bugsink:{project}:{issue}"),
        "legacy_incident_invalid"
    );
    Ok((project, issue))
}

/// The old publisher emitted a plain paragraph, not an arbitrary occurrence of
/// an ID. Parse tag boundaries and accept only the complete marked text of that
/// paragraph.
pub(super) fn html_incidents(html: &str) -> anyhow::Result<Vec<String>> {
    ensure!(html.len() <= 262_144, "legacy_description_oversized");
    let mut result = Vec::new();
    let mut rest = html;
    while let Some(start) = rest.find('<') {
        rest = &rest[start..];
        if rest.starts_with("<!--") {
            let end = rest.find("-->").context("legacy_html_invalid")?;
            rest = &rest[end + 3..];
            continue;
        }
        let end = rest.find('>').context("legacy_html_invalid")?;
        let tag = &rest[1..end];
        rest = &rest[end + 1..];
        if tag == "script" || tag == "style" {
            let closer = format!("</{tag}>");
            let end = rest.find(&closer).context("legacy_html_invalid")?;
            rest = &rest[end + closer.len()..];
        } else if tag == "p" {
            let end = rest.find('<').context("legacy_html_invalid")?;
            if rest[end..].starts_with("</p>") {
                if let Some(incident) = rest[..end].strip_prefix("incident: ") {
                    let (project, _) = canonical_incident(incident)?;
                    if [25, 26].contains(&project) {
                        result.push(incident.to_owned());
                    }
                }
            }
        }
    }
    Ok(result)
}

fn origin(value: &str) -> anyhow::Result<()> {
    #[expect(
        clippy::disallowed_types,
        reason = "Validates configured origin components without formatting the raw URL in diagnostics"
    )]
    let url = url::Url::parse(value)?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.origin().ascii_serialization() == value
            && url.username().is_empty()
            && url.password().is_none(),
        "legacy_origin_invalid"
    );
    Ok(())
}

pub(super) async fn import(state: &AppState) -> anyhow::Result<Value> {
    let path = state
        .active_config_path()
        .with_file_name("bugsink-legacy.json");
    let config: LegacyConfig = serde_json::from_value(private_json(&path).await?)?;
    ensure!(
        config.schema_version == 1
            && config
                .owners
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from(["el-telar", "laptop"]),
        "legacy_owners_missing"
    );
    let http = http_client()?;
    let settings = state.server_settings();
    let bugsink_origin = settings
        .server
        .integrations
        .bugsink
        .origin
        .as_deref()
        .context("bugsink_not_configured")?;
    let mut owners = serde_json::Map::new();
    let mut records = Vec::new();
    let mut imported = BTreeMap::<String, (String, RunId, LegacyRun)>::new();
    let mut abandoned = BTreeMap::<String, String>::new();
    let mut resolved_unknown = BTreeSet::new();
    let mut inventories = BTreeMap::<String, String>::new();
    for (name, owner) in &config.owners {
        ensure!(
            fabro_types::is_env_style_name(&owner.token_secret),
            "legacy_token_reference_invalid"
        );
        origin(&owner.api_origin)?;
        ensure!(
            owner.state_file.is_absolute(),
            "legacy_source_must_be_absolute"
        );
        let local = private_json(&owner.state_file).await?;
        let local = local.as_object().context("legacy_state_invalid")?;
        let token = state
            .vault_secret(&owner.token_secret)
            .await?
            .filter(|s| !s.trim().is_empty())
            .context("legacy_owner_credential_missing")?;
        let get = |path: String| {
            let http = &http;
            let token = &token;
            let api = &owner.api_origin;
            async move {
                bounded_json(
                    http.get(format!("{api}/api/v1{path}"))
                        .bearer_auth(token)
                        .send()
                        .await?,
                    OWNER_BODY_LIMIT,
                )
                .await
            }
        };
        let inventory = get("/automations".into()).await?;
        let automation_rows = inventory
            .get("data")
            .and_then(Value::as_array)
            .context("legacy_inventory_invalid")?;
        let mut owned = Vec::new();
        for automation in automation_rows {
            let id = automation
                .get("id")
                .and_then(Value::as_str)
                .context("legacy_inventory_invalid")?;
            let workflow = automation
                .pointer("/target/workflow")
                .and_then(Value::as_str)
                .context("legacy_inventory_invalid")?;
            if ["bugsink-loop", "incident-loop", "ticket-loop"].contains(&id)
                || ["bugsink-loop", "incident-loop", "ticket-loop"].contains(&workflow)
            {
                ensure!(
                    automation
                        .get("triggers")
                        .and_then(Value::as_array)
                        .context("legacy_inventory_invalid")?
                        .iter()
                        .all(|trigger| trigger.get("enabled").and_then(Value::as_bool)
                            == Some(false)),
                    "legacy_owner_still_enabled"
                );
                owned.push(automation.clone());
            }
        }
        owned.sort_by_key(|value| value["id"].as_str().unwrap_or_default().to_owned());
        let inventory_digest = digest(&owned)?;
        inventories.insert(name.clone(), inventory_digest.clone());
        let mut runs = BTreeMap::<String, Vec<(RunId, LegacyRun)>>::new();
        let mut seen = BTreeSet::new();
        let mut offset = 0;
        loop {
            ensure!(offset < 100_000, "legacy_inventory_limit");
            let page = get(format!(
                "/runs?page[limit]=100&page[offset]={offset}&include_archived=true&direction=asc"
            ))
            .await?;
            let rows = page
                .get("data")
                .and_then(Value::as_array)
                .context("legacy_runs_invalid")?;
            let more = page
                .pointer("/meta/has_more")
                .and_then(Value::as_bool)
                .context("legacy_runs_invalid")?;
            ensure!(!more || !rows.is_empty(), "legacy_runs_no_progress");
            for row in rows {
                let run_id = row
                    .get("id")
                    .and_then(Value::as_str)
                    .context("legacy_run_id_invalid")?
                    .parse::<RunId>()?;
                ensure!(seen.insert(run_id), "legacy_runs_repeated");
                // Never classify an absent/stale list projection as authoritative RunNotFound.
                let projection: LegacyRun =
                    serde_json::from_value(get(format!("/runs/{run_id}/state")).await?)?;
                ensure!(
                    projection.spec.run_id == run_id,
                    "legacy_run_identity_mismatch"
                );
                let incident = projection
                    .spec
                    .settings
                    .run
                    .inputs
                    .get("incident")
                    .and_then(toml::Value::as_str)
                    .filter(|incident| incident.starts_with("bugsink:"));
                let owned_run = incident.is_some()
                    || projection
                        .spec
                        .workflow_slug
                        .as_deref()
                        .is_some_and(|slug| {
                            ["bugsink-loop", "incident-loop", "ticket-loop"].contains(&slug)
                        })
                    || projection.spec.automation.as_ref().is_some_and(|a| {
                        ["bugsink-loop", "incident-loop", "ticket-loop"].contains(&a.id.as_str())
                    });
                ensure!(
                    !owned_run || projection.status.is_terminal(),
                    "legacy_run_in_flight"
                );
                if let Some(incident) = incident {
                    let (project, _) = canonical_incident(incident)?;
                    if [25, 26].contains(&project) {
                        runs.entry(incident.to_owned())
                            .or_default()
                            .push((run_id, projection));
                    }
                }
            }
            offset += rows.len();
            if !more {
                break;
            }
        }
        for (incident, record) in local {
            let (project, _) = canonical_incident(incident)?;
            if ![25, 26].contains(&project) {
                continue;
            }
            let status = record
                .get("status")
                .and_then(Value::as_str)
                .context("legacy_record_invalid")?;
            ensure!(
                !status.is_empty() && status.len() <= 100,
                "legacy_record_status_invalid"
            );
            if let Some(run_id) = record.get("run").and_then(Value::as_str) {
                let run_id = run_id.parse::<RunId>()?;
                if !runs
                    .get(incident)
                    .is_some_and(|found| found.iter().any(|(id, _)| *id == run_id))
                {
                    let projection: LegacyRun =
                        serde_json::from_value(get(format!("/runs/{run_id}/state")).await?)?;
                    ensure!(
                        projection.spec.run_id == run_id
                            && projection.status.is_terminal()
                            && projection
                                .spec
                                .settings
                                .run
                                .inputs
                                .get("incident")
                                .and_then(toml::Value::as_str)
                                == Some(incident.as_str()),
                        "legacy_record_run_unverified"
                    );
                    runs.entry(incident.clone())
                        .or_default()
                        .push((run_id, projection));
                }
                resolved_unknown.insert(incident.clone());
            } else {
                // A missing cached summary is not authoritative proof that a lost create never
                // happened.
                abandoned.insert(incident.clone(), name.clone());
            }
        }
        // Every discovered identity is checked through its exact persisted owner
        // projection.
        for (incident, found) in runs {
            for (id, projection) in found {
                if let Some((old_incident, _, _)) = imported.get(&id.to_string()) {
                    ensure!(old_incident == &incident, "legacy_run_owner_mismatch");
                }
                imported.insert(id.to_string(), (incident.clone(), id, projection));
                records.push(json!({"owner":name,"incident_key":incident_key(bugsink_origin, canonical_incident(&incident)?.0, canonical_incident(&incident)?.1)?,"run_id":id.to_string()}));
            }
        }
        owners.insert(name.clone(), json!({"disabled":true,"in_flight_reconciled":true,"inventory_digest":inventory_digest}));
    }
    let plane = &settings.server.integrations.plane;
    let api_base = plane
        .api_base
        .as_deref()
        .context("legacy_plane_not_configured")?;
    #[expect(
        clippy::disallowed_types,
        reason = "Mutates a validated Plane URL solely for bounded HTTP transit; failures are normalized before exposure"
    )]
    let mut base = url::Url::parse(api_base)?;
    ensure!(
        matches!(base.scheme(), "http" | "https")
            && base.username().is_empty()
            && base.password().is_none()
            && base.query().is_none()
            && base.fragment().is_none(),
        "legacy_plane_origin_invalid"
    );
    let workspace = plane
        .workspace
        .as_deref()
        .context("legacy_plane_not_configured")?;
    ensure!(
        !workspace.is_empty()
            && workspace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c)),
        "legacy_plane_workspace_invalid"
    );
    let path = format!(
        "{}/workspaces/{workspace}/projects/{}/issues/",
        base.path().trim_end_matches('/'),
        config.plane_project_id
    );
    base.set_path(&path);
    let token = state
        .vault_secret(fabro_static::EnvVars::PLANE_API_KEY)
        .await?
        .context("legacy_plane_credential_missing")?;
    let mut tickets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut seen = BTreeSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = base.clone();
        url.query_pairs_mut().append_pair("per_page", "100");
        if let Some(cursor) = &cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        let page = bounded_json(
            http.get(url).header("X-Api-Key", &token).send().await?,
            BODY_LIMIT,
        )
        .await?;
        for ticket in page
            .get("results")
            .and_then(Value::as_array)
            .context("legacy_plane_page_invalid")?
        {
            let ticket_id = ticket
                .get("id")
                .and_then(Value::as_str)
                .context("legacy_ticket_invalid")?
                .parse::<Uuid>()?;
            ensure!(
                ticket.get("project").and_then(Value::as_str)
                    == Some(config.plane_project_id.to_string().as_str()),
                "legacy_ticket_project_mismatch"
            );
            for incident in html_incidents(
                ticket
                    .get("description_html")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )? {
                tickets
                    .entry(incident)
                    .or_default()
                    .insert(ticket_id.to_string());
            }
        }
        let more = page
            .get("next_page_results")
            .and_then(Value::as_bool)
            .context("legacy_plane_page_invalid")?;
        if !more {
            break;
        }
        let next = page
            .get("next_cursor")
            .and_then(Value::as_str)
            .context("legacy_plane_cursor_invalid")?;
        ensure!(
            !next.is_empty()
                && next.len() <= 1024
                && seen.len() < 100_000
                && seen.insert(next.to_owned()),
            "legacy_plane_cursor_repeated"
        );
        cursor = Some(next.to_owned());
    }
    // Re-read the source inventories after the scan: a re-enabled owner invalidates
    // ownership transfer.
    for (name, owner) in &config.owners {
        let token = state
            .vault_secret(&owner.token_secret)
            .await?
            .context("legacy_owner_credential_missing")?;
        let inventory = bounded_json(
            http.get(format!("{}/api/v1/automations", owner.api_origin))
                .bearer_auth(token)
                .send()
                .await?,
            OWNER_BODY_LIMIT,
        )
        .await?;
        let mut owned: Vec<_> = inventory
            .get("data")
            .and_then(Value::as_array)
            .context("legacy_inventory_invalid")?
            .iter()
            .filter(|a| {
                a.get("id").and_then(Value::as_str).is_some_and(|id| {
                    ["bugsink-loop", "incident-loop", "ticket-loop"].contains(&id)
                }) || a
                    .pointer("/target/workflow")
                    .and_then(Value::as_str)
                    .is_some_and(|id| {
                        ["bugsink-loop", "incident-loop", "ticket-loop"].contains(&id)
                    })
            })
            .cloned()
            .collect();
        owned.sort_by_key(|v| v["id"].as_str().unwrap_or_default().to_owned());
        ensure!(
            inventories.get(name) == Some(&digest(&owned)?),
            "legacy_inventory_changed"
        );
    }
    let store = state.incident_store();
    let mut tx = store.pool.begin().await?;
    for (incident, _, _) in imported.values() {
        let (project, issue) = canonical_incident(incident)?;
        sqlx::query("INSERT INTO bugsink_incidents(incident_key,origin,project_id,issue_id) VALUES(?,?,?,?) ON CONFLICT DO NOTHING")
            .bind(incident_key(bugsink_origin,project,issue)?).bind(bugsink_origin).bind(i64::try_from(project)?).bind(issue.to_string()).execute(&mut *tx).await?;
    }
    for (incident, id, projection) in imported.values() {
        let (project, issue) = canonical_incident(incident)?;
        let key = incident_key(bugsink_origin, project, issue)?;
        let observation = digest(&("legacy", incident, id.to_string()))?;
        let revision = match projection
            .spec
            .git
            .as_ref()
            .and_then(|git| git.sha.as_deref())
            .filter(|sha| super::worker::pinned_revision(sha))
        {
            Some(sha) => sha.to_owned(),
            None => format!("legacy-graph:{}", digest(&projection.spec.graph)?),
        };
        sqlx::query("INSERT INTO bugsink_runs(run_id,incident_key,observation_key,episode,attempt,state,source_revision,failure_class)
            VALUES(?,?,?,1,1,'failed',?,'legacy_handoff_unverified') ON CONFLICT(run_id) DO NOTHING")
            .bind(id.to_string()).bind(&key).bind(observation).bind(revision).execute(&mut *tx).await?;
        let actual: String =
            sqlx::query_scalar("SELECT incident_key FROM bugsink_runs WHERE run_id=?")
                .bind(id.to_string())
                .fetch_one(&mut *tx)
                .await?;
        ensure!(actual == key, "legacy_import_collision");
    }
    for incident in resolved_unknown
        .iter()
        .filter(|incident| !abandoned.contains_key(*incident))
    {
        let (project, issue) = canonical_incident(incident)?;
        sqlx::query("UPDATE bugsink_incidents SET parked_reason=NULL WHERE incident_key=? AND parked_reason='legacy_spawn_uncertain'")
            .bind(incident_key(bugsink_origin,project,issue)?).execute(&mut *tx).await?;
    }
    for (incident, owner) in abandoned {
        let (project, issue) = canonical_incident(&incident)?;
        let key = incident_key(bugsink_origin, project, issue)?;
        sqlx::query("INSERT INTO bugsink_incidents(incident_key,origin,project_id,issue_id,parked_reason) VALUES(?,?,?,?,'legacy_spawn_uncertain')
            ON CONFLICT(incident_key) DO UPDATE SET parked_reason='legacy_spawn_uncertain'")
            .bind(&key).bind(bugsink_origin).bind(i64::try_from(project)?).bind(issue.to_string()).execute(&mut *tx).await?;
        records.push(
            json!({"owner":owner,"incident_key":key,"run_id":null,"status":"unresolved_spawn"}),
        );
    }
    for (incident, ticket) in &tickets {
        let (project, issue) = canonical_incident(incident)?;
        let key = incident_key(bugsink_origin, project, issue)?;
        sqlx::query("INSERT INTO bugsink_incidents(incident_key,origin,project_id,issue_id) VALUES(?,?,?,?) ON CONFLICT DO NOTHING")
            .bind(&key).bind(bugsink_origin).bind(i64::try_from(project)?).bind(issue.to_string()).execute(&mut *tx).await?;
        for ticket in ticket {
            records.push(json!({"owner":"plane","incident_key":key,"run_id":null,"ticket_id":ticket,"status":"verified_ticket"}));
        }
    }
    let incomplete: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM bugsink_incidents WHERE origin=? AND parked_reason='legacy_spawn_uncertain')")
        .bind(bugsink_origin).fetch_one(&mut *tx).await?;
    if incomplete {
        for owner in owners.values_mut() {
            owner["in_flight_reconciled"] = json!(false);
        }
    }
    tx.commit().await?;
    Ok(
        json!({"schema_version":1,"status":if incomplete {"incomplete"} else {"reconciled"},"owners":owners,"records":records}),
    )
}
