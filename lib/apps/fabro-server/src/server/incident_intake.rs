mod store;

pub(crate) use store::IncidentStore;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AlertReason {
    New,
    Regressed,
    Unmuted,
    Test,
}

impl AlertReason {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::Regressed => "REGRESSED",
            Self::Unmuted => "UNMUTED",
            Self::Test => "TEST",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AcceptedAlert {
    pub project_id: u64,
    pub issue_id: uuid::Uuid,
    pub reason: AlertReason,
    pub body_digest: String,
    pub received_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcceptResult {
    Queued,
    Duplicate,
    Test,
}

/// Fail startup closed before the authenticated receiver can be installed.
/// Secrets are read through the existing vault, never retained in settings or errors.
pub(super) async fn validate_enablement(state: &super::AppState) -> anyhow::Result<()> {
    use anyhow::{Context as _, ensure};

    let settings = state.server_settings();
    let bugsink = &settings.server.integrations.bugsink;
    ensure!(!bugsink.dispatch_enabled || bugsink.enabled, "Bugsink dispatch requires enabled intake");
    if !bugsink.enabled {
        return Ok(());
    }
    let origin = bugsink.origin.as_deref().context("Bugsink requires an origin")?;
    let url = url::Url::parse(origin).context("Bugsink origin is invalid")?;
    ensure!(matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty() && url.password().is_none()
        && url.query().is_none() && url.fragment().is_none()
        && origin == url.origin().ascii_serialization(), "Bugsink requires an exact HTTP(S) origin");
    ensure!(!bugsink.projects.is_empty(), "Bugsink requires project mappings");
    let token_name = bugsink.api_token_secret.as_deref().context("Bugsink requires an API token vault name")?;
    let mut names = std::collections::HashSet::new();
    let mut projects = std::collections::HashSet::new();
    for name in std::iter::once(token_name).chain(bugsink.projects.iter().map(|project| project.signing_secret.as_str())) {
        ensure!(!name.is_empty() && name.trim() == name && !name.chars().any(char::is_control)
            && names.insert(name), "Bugsink requires unique nonempty vault secret names");
        let secret = state.vault_secret(name).await.map_err(|_| anyhow::anyhow!("Bugsink vault lookup failed"))?;
        ensure!(secret.as_deref().is_some_and(|value| !value.trim().is_empty()), "Bugsink required vault secret is missing or empty");
    }
    for project in &bugsink.projects {
        ensure!(i64::try_from(project.project_id).is_ok() && projects.insert(project.project_id),
            "Bugsink project IDs must be unique SQLite integers");
        let id = fabro_automation::AutomationId::new(project.automation_id.clone())
            .map_err(|_| anyhow::anyhow!("Bugsink automation ID is invalid"))?;
        let automation = state.automation_store().get(&id).await
            .map_err(|_| anyhow::anyhow!("Bugsink automation lookup failed"))?
            .context("Bugsink configured automation does not exist")?;
        ensure!(automation.target.workflow == "incident-loop", "Bugsink automation must target incident-loop");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::test_support::{TestAppStateBuilder, default_test_server_settings};
    use fabro_types::settings::server::{BugsinkIntegrationSettings, BugsinkProjectSettings};

    #[test]
    fn enabled_intake_cannot_start_without_vault_keys_or_real_automation() {
        for with_keys in [false, true] {
            let mut settings = default_test_server_settings();
            settings.server.integrations.bugsink = BugsinkIntegrationSettings {
                enabled: true,
                dispatch_enabled: false,
                origin: Some("https://bugsink.example".into()),
                api_token_secret: Some("bugsink-api".into()),
                projects: vec![BugsinkProjectSettings {
                    project_id: 7,
                    automation_id: "incident-loop".into(),
                    signing_secret: "bugsink-signing".into(),
                }],
            };
            let mut builder = TestAppStateBuilder::new()
                .runtime_settings(settings, fabro_config::RunLayer::default());
            if with_keys {
                builder = builder.vault_entries([
                    ("bugsink-api", "test-api-material"),
                    ("bugsink-signing", "test-signing-material"),
                ]);
            }
            let error = match builder.try_build() {
                Ok(_) => panic!("enabled intake started with a missing prerequisite"),
                Err(error) => error.to_string(),
            };
            assert!(error.contains(if with_keys { "automation does not exist" } else { "vault secret is missing or empty" }));
            assert!(!error.contains("test-api-material"));
            assert!(!error.contains("test-signing-material"));
        }
    }
}
