use std::path::Path;

use fabro_types::settings::server::{
    BugsinkIntegrationSettings, BugsinkProjectSettings, GithubIntegrationSettings,
    GithubIntegrationStrategy, IntegrationWebhooksSettings, ObjectStoreProvider,
    ObjectStoreSettings, PlaneIntegrationSettings, ServerApiSettings, ServerArtifactsSettings,
    ServerAuthGithubSettings, ServerAuthMethod, ServerAuthSettings, ServerIntegrationsSettings,
    ServerListenSettings, ServerLoggingSettings, ServerNamespace, ServerSandboxProviderSettings,
    ServerSandboxProvidersSettings, ServerSandboxSettings, ServerSchedulerSettings,
    ServerSlateDbSettings, ServerStorageSettings, ServerWebSettings, SlackIntegrationSettings,
    WebhookStrategy,
};
use fabro_types::{ExternalAgentProfile, ExternalAgentsSettings};
use fabro_util::Home;

use super::{
    ResolveError, default_string, parse_socket_addr, require_interp, require_string,
    warn_if_demoted_template,
};
use crate::user::default_storage_dir;
use crate::{
    BugsinkIntegrationLayer, ExternalAgentProfileLayer, ExternalAgentsLayer,
    IntegrationWebhooksLayer, ObjectStoreLocalLayer, ObjectStoreS3Layer, PlaneIntegrationLayer,
    ServerApiLayer, ServerArtifactsLayer, ServerAuthLayer, ServerIntegrationsLayer, ServerLayer,
    ServerListenLayer, ServerSandboxLayer, ServerSandboxProviderLayer, ServerSlateDbLayer,
    ServerStorageLayer, ServerWebLayer,
};

pub fn resolve_server(layer: &ServerLayer, errors: &mut Vec<ResolveError>) -> ServerNamespace {
    let storage = resolve_storage(layer.storage.as_ref());
    let listen = resolve_listen(layer.listen.as_ref(), errors);
    let web = resolve_web(layer.web.as_ref());
    let auth = resolve_auth(layer.auth.as_ref(), errors);
    let integrations = resolve_integrations(layer.integrations.as_ref(), errors);
    validate_github_webhook_strategy(&integrations, layer.api.as_ref(), errors);

    let api_url = layer.api.as_ref().and_then(|api| api.url.clone());
    warn_if_demoted_template("server.api.url", api_url.as_deref());

    ServerNamespace {
        listen,
        api: ServerApiSettings { url: api_url },
        web,
        auth,
        sandbox: resolve_sandbox(layer.sandbox.as_ref()),
        storage: storage.clone(),
        artifacts: resolve_artifacts(layer.artifacts.as_ref(), &storage.root, errors),
        slatedb: resolve_slatedb(layer.slatedb.as_ref(), &storage.root, errors),
        scheduler: ServerSchedulerSettings {
            max_concurrent_runs: layer
                .scheduler
                .as_ref()
                .and_then(|scheduler| scheduler.max_concurrent_runs)
                .expect("defaults.toml should provide server.scheduler.max_concurrent_runs"),
        },
        logging: ServerLoggingSettings {
            level:       layer
                .logging
                .as_ref()
                .and_then(|logging| logging.level.as_ref())
                .map(|level| level.as_str().to_owned()),
            destination: layer
                .logging
                .as_ref()
                .and_then(|logging| logging.destination)
                .unwrap_or_default(),
        },
        integrations,
        external_agents: resolve_external_agents(layer.external_agents.as_ref()),
    }
}

fn resolve_sandbox(layer: Option<&ServerSandboxLayer>) -> ServerSandboxSettings {
    let providers = layer.and_then(|sandbox| sandbox.providers.as_ref());
    ServerSandboxSettings {
        providers: ServerSandboxProvidersSettings {
            local:   resolve_sandbox_provider(
                providers.and_then(|providers| providers.local.as_ref()),
            ),
            docker:  resolve_sandbox_provider(
                providers.and_then(|providers| providers.docker.as_ref()),
            ),
            daytona: resolve_sandbox_provider(
                providers.and_then(|providers| providers.daytona.as_ref()),
            ),
        },
    }
}

fn resolve_sandbox_provider(
    layer: Option<&ServerSandboxProviderLayer>,
) -> ServerSandboxProviderSettings {
    ServerSandboxProviderSettings {
        enabled: layer.and_then(|provider| provider.enabled).unwrap_or(true),
    }
}

fn resolve_storage(layer: Option<&ServerStorageLayer>) -> ServerStorageSettings {
    let root = layer.and_then(|storage| storage.root.as_deref());
    warn_if_demoted_template("server.storage.root", root);
    ServerStorageSettings {
        root: root.map_or_else(|| default_string(default_storage_dir()), str::to_owned),
    }
}

fn resolve_listen(
    layer: Option<&ServerListenLayer>,
    errors: &mut Vec<ResolveError>,
) -> ServerListenSettings {
    match layer {
        None => ServerListenSettings::Unix {
            path: default_string(Home::from_env().socket_path()),
        },
        Some(ServerListenLayer::Unix { path }) => {
            warn_if_demoted_template("server.listen.path", path.as_deref());
            ServerListenSettings::Unix {
                path: path
                    .clone()
                    .unwrap_or_else(|| default_string(Home::from_env().socket_path())),
            }
        }
        Some(ServerListenLayer::Tcp { address }) => {
            let address = parse_socket_addr(
                &require_interp(address.as_ref(), "server.listen.address", errors),
                "server.listen.address",
                errors,
            );
            ServerListenSettings::Tcp { address }
        }
    }
}

fn resolve_web(layer: Option<&ServerWebLayer>) -> ServerWebSettings {
    let layer = layer.expect("defaults.toml should provide server.web defaults");

    let url = layer
        .url
        .clone()
        .expect("defaults.toml should provide server.web.url");
    warn_if_demoted_template("server.web.url", Some(url.as_str()));

    ServerWebSettings {
        enabled: layer
            .enabled
            .expect("defaults.toml should provide server.web.enabled"),
        url,
    }
}

fn resolve_auth(
    layer: Option<&ServerAuthLayer>,
    errors: &mut Vec<ResolveError>,
) -> ServerAuthSettings {
    let methods = if let Some(mut methods) = layer.and_then(|auth| auth.methods.clone()) {
        if methods.is_empty() {
            errors.push(ResolveError::Invalid {
                path:   "server.auth.methods".to_string(),
                reason: "must not be empty".to_string(),
            });
        }
        methods.dedup();
        methods
    } else {
        errors.push(ResolveError::Missing {
            path: "server.auth.methods".to_string(),
        });
        Vec::new()
    };

    let github = layer
        .and_then(|auth| auth.github.as_ref())
        .cloned()
        .unwrap_or_default();
    if methods.contains(&ServerAuthMethod::Github) && github.allowed_usernames.is_empty() {
        errors.push(ResolveError::Invalid {
            path:   "server.auth.github.allowed_usernames".to_string(),
            reason: "must not be empty when github auth is enabled".to_string(),
        });
    }

    ServerAuthSettings {
        methods,
        github: ServerAuthGithubSettings {
            allowed_usernames: github.allowed_usernames,
        },
    }
}

fn validate_github_webhook_strategy(
    integrations: &ServerIntegrationsSettings,
    api_layer: Option<&ServerApiLayer>,
    errors: &mut Vec<ResolveError>,
) {
    let github = &integrations.github;
    let strategy = github
        .webhooks
        .as_ref()
        .and_then(|webhooks| webhooks.strategy);

    if strategy.is_some()
        && github.strategy == GithubIntegrationStrategy::App
        && github.app_id.is_none()
    {
        errors.push(ResolveError::Invalid {
            path:   "server.integrations.github.app_id".to_string(),
            reason: "must be set when server.integrations.github.webhooks.strategy is configured"
                .to_string(),
        });
    }

    if matches!(strategy, Some(WebhookStrategy::ServerUrl))
        && api_layer.and_then(|api| api.url.as_ref()).is_none()
    {
        errors.push(ResolveError::Invalid {
            path:   "server.api.url".to_string(),
            reason:
                "must be set when server.integrations.github.webhooks.strategy = \"server_url\""
                    .to_string(),
        });
    }
}

fn resolve_artifacts(
    layer: Option<&ServerArtifactsLayer>,
    storage_root: &str,
    errors: &mut Vec<ResolveError>,
) -> ServerArtifactsSettings {
    let provider = layer
        .and_then(|artifacts| artifacts.provider)
        .expect("defaults.toml should provide server.artifacts.provider");

    let prefix = layer
        .and_then(|artifacts| artifacts.prefix.clone())
        .expect("defaults.toml should provide server.artifacts.prefix");
    warn_if_demoted_template("server.artifacts.prefix", Some(prefix.as_str()));

    ServerArtifactsSettings {
        prefix,
        store: resolve_object_store(
            provider,
            layer.and_then(|artifacts| artifacts.local.as_ref()),
            layer.and_then(|artifacts| artifacts.s3.as_ref()),
            &object_store_default_root(storage_root, "artifacts"),
            "server.artifacts",
            errors,
        ),
    }
}

fn resolve_slatedb(
    layer: Option<&ServerSlateDbLayer>,
    storage_root: &str,
    errors: &mut Vec<ResolveError>,
) -> ServerSlateDbSettings {
    let provider = layer
        .and_then(|slatedb| slatedb.provider)
        .expect("defaults.toml should provide server.slatedb.provider");

    let disk_cache = layer
        .and_then(|slatedb| slatedb.disk_cache)
        .expect("defaults.toml should provide server.slatedb.disk_cache");

    if disk_cache && provider == ObjectStoreProvider::Local {
        tracing::warn!(
            "disk_cache enabled with local provider; \
             disk cache is designed for S3-backed deployments \
             and adds overhead on local filesystems"
        );
    }

    let prefix = layer
        .and_then(|slatedb| slatedb.prefix.clone())
        .expect("defaults.toml should provide server.slatedb.prefix");
    warn_if_demoted_template("server.slatedb.prefix", Some(prefix.as_str()));

    ServerSlateDbSettings {
        prefix,
        store: resolve_object_store(
            provider,
            layer.and_then(|slatedb| slatedb.local.as_ref()),
            layer.and_then(|slatedb| slatedb.s3.as_ref()),
            &object_store_default_root(storage_root, "slatedb"),
            "server.slatedb",
            errors,
        ),
        flush_interval: layer
            .and_then(|slatedb| slatedb.flush_interval)
            .map(|duration| duration.as_std())
            .expect("defaults.toml should provide server.slatedb.flush_interval"),
        disk_cache,
    }
}

fn resolve_object_store(
    provider: ObjectStoreProvider,
    local: Option<&ObjectStoreLocalLayer>,
    s3: Option<&ObjectStoreS3Layer>,
    storage_root: &str,
    path_prefix: &str,
    errors: &mut Vec<ResolveError>,
) -> ObjectStoreSettings {
    match provider {
        ObjectStoreProvider::Local => {
            let root = local.and_then(|local| local.root.as_deref());
            warn_if_demoted_template(&format!("{path_prefix}.local.root"), root);
            ObjectStoreSettings::Local {
                root: root.map_or_else(|| storage_root.to_owned(), str::to_owned),
            }
        }
        ObjectStoreProvider::S3 => {
            let bucket_field = format!("{path_prefix}.s3.bucket");
            let region_field = format!("{path_prefix}.s3.region");
            let endpoint_field = format!("{path_prefix}.s3.endpoint");
            let bucket =
                require_string(s3.and_then(|s3| s3.bucket.as_ref()), &bucket_field, errors);
            let region =
                require_string(s3.and_then(|s3| s3.region.as_ref()), &region_field, errors);
            let endpoint = s3.and_then(|s3| s3.endpoint.clone());
            warn_if_demoted_template(&bucket_field, Some(bucket.as_str()));
            warn_if_demoted_template(&region_field, Some(region.as_str()));
            warn_if_demoted_template(&endpoint_field, endpoint.as_deref());
            ObjectStoreSettings::S3 {
                bucket,
                region,
                endpoint,
                path_style: s3.and_then(|s3| s3.path_style).unwrap_or(false),
            }
        }
    }
}

fn object_store_default_root(storage_root: &str, domain: &str) -> String {
    Path::new(storage_root)
        .join("objects")
        .join(domain)
        .to_string_lossy()
        .into_owned()
}

fn resolve_integrations(
    layer: Option<&ServerIntegrationsLayer>,
    errors: &mut Vec<ResolveError>,
) -> ServerIntegrationsSettings {
    ServerIntegrationsSettings {
        github:  layer
            .and_then(|integrations| integrations.github.as_ref())
            .map(|github| {
                warn_if_demoted_template(
                    "server.integrations.github.app_id",
                    github.app_id.as_deref(),
                );
                warn_if_demoted_template(
                    "server.integrations.github.client_id",
                    github.client_id.as_deref(),
                );
                warn_if_demoted_template("server.integrations.github.slug", github.slug.as_deref());
                GithubIntegrationSettings {
                    enabled:   github.enabled.unwrap_or(true),
                    strategy:  github.strategy.unwrap_or_default(),
                    app_id:    github.app_id.clone(),
                    client_id: github.client_id.clone(),
                    slug:      github.slug.clone(),
                    webhooks:  github.webhooks.as_ref().map(resolve_github_webhooks),
                }
            })
            .unwrap_or_default(),
        slack:   layer
            .and_then(|integrations| integrations.slack.as_ref())
            .map_or(
                SlackIntegrationSettings {
                    enabled:         false,
                    default_channel: None,
                },
                |slack| {
                    warn_if_demoted_template(
                        "server.integrations.slack.default_channel",
                        slack.default_channel.as_deref(),
                    );
                    SlackIntegrationSettings {
                        enabled:         slack.enabled.unwrap_or(true),
                        default_channel: slack.default_channel.clone(),
                    }
                },
            ),
        plane:   layer
            .and_then(|integrations| integrations.plane.as_ref())
            .map(resolve_plane)
            .unwrap_or_default(),
        bugsink: layer
            .and_then(|integrations| integrations.bugsink.as_ref())
            .map(|bugsink| resolve_bugsink(bugsink, errors))
            .unwrap_or_default(),
    }
}

fn resolve_plane(layer: &PlaneIntegrationLayer) -> PlaneIntegrationSettings {
    warn_if_demoted_template(
        "server.integrations.plane.api_base",
        layer.api_base.as_deref(),
    );
    warn_if_demoted_template(
        "server.integrations.plane.workspace",
        layer.workspace.as_deref(),
    );
    PlaneIntegrationSettings {
        enabled:   layer.enabled.unwrap_or(false),
        api_base:  layer.api_base.clone(),
        workspace: layer.workspace.clone(),
    }
}

fn resolve_bugsink(
    layer: &BugsinkIntegrationLayer,
    errors: &mut Vec<ResolveError>,
) -> BugsinkIntegrationSettings {
    let enabled = layer.enabled.unwrap_or(false);
    let dispatch_enabled = layer.dispatch_enabled.unwrap_or(false);
    let path = "server.integrations.bugsink";
    let mut invalid = |field: &str, reason: &str| {
        errors.push(ResolveError::Invalid {
            path:   format!("{path}.{field}"),
            reason: reason.to_owned(),
        });
    };
    if dispatch_enabled && !enabled {
        invalid("dispatch_enabled", "requires enabled intake");
    }
    if enabled || layer.origin.is_some() {
        let valid_origin = layer
            .origin
            .as_deref()
            .and_then(|origin| {
                #[expect(
                    clippy::disallowed_types,
                    reason = "parse trusted config only to reject credentials and non-origin components; neither the raw URL nor parse error is logged"
                )]
                let url = url::Url::parse(origin).ok()?;
                Some(
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.query().is_none()
                        && url.fragment().is_none()
                        && origin == url.origin().ascii_serialization(),
                )
            })
            .unwrap_or(false);
        if !valid_origin {
            invalid(
                "origin",
                "requires an exact HTTP(S) origin without credentials, path, query or fragment",
            );
        }
    }
    if (enabled || layer.api_token_secret.is_some())
        && !layer
            .api_token_secret
            .as_deref()
            .is_some_and(fabro_types::is_env_style_name)
    {
        invalid(
            "api_token_secret",
            "requires an environment-style vault secret name",
        );
    }
    if enabled && layer.projects.as_ref().is_none_or(Vec::is_empty) {
        invalid(
            "projects",
            "enabled intake requires at least one project mapping",
        );
    }
    let mut project_ids = std::collections::HashSet::new();
    let mut secret_names = std::collections::HashSet::new();
    if let Some(name) = &layer.api_token_secret {
        secret_names.insert(name.as_str());
    }
    let projects = layer
        .projects
        .iter()
        .flatten()
        .enumerate()
        .map(|(index, project)| {
            let prefix = format!("{path}.projects[{index}]");
            let project_id = project.project_id.unwrap_or_else(|| {
                errors.push(ResolveError::Missing {
                    path: format!("{prefix}.project_id"),
                });
                0
            });
            let automation_id = require_string(
                project.automation_id.as_ref(),
                &format!("{prefix}.automation_id"),
                errors,
            );
            let signing_secret = require_string(
                project.signing_secret.as_ref(),
                &format!("{prefix}.signing_secret"),
                errors,
            );
            let mut invalid = |field: &str, reason: &str| {
                errors.push(ResolveError::Invalid {
                    path:   format!("{prefix}.{field}"),
                    reason: reason.to_owned(),
                });
            };
            if i64::try_from(project_id).is_err() || !project_ids.insert(project_id) {
                invalid("project_id", "must be a unique nonnegative SQLite integer");
            }
            if fabro_automation::AutomationId::new(automation_id.clone()).is_err() {
                invalid("automation_id", "must be a valid automation ID");
            }
            if !fabro_types::is_env_style_name(&signing_secret) {
                invalid(
                    "signing_secret",
                    "requires an environment-style vault secret name",
                );
            }
            if enabled && !secret_names.insert(project.signing_secret.as_deref().unwrap_or("")) {
                invalid(
                    "signing_secret",
                    "enabled mappings must use distinct signing and API secret names",
                );
            }
            BugsinkProjectSettings {
                project_id,
                automation_id,
                signing_secret,
            }
        })
        .collect();
    BugsinkIntegrationSettings {
        enabled,
        dispatch_enabled,
        origin: layer.origin.clone(),
        api_token_secret: layer.api_token_secret.clone(),
        projects,
    }
}

#[cfg(test)]
mod bugsink_tests {
    use super::*;

    fn valid_layer() -> BugsinkIntegrationLayer {
        toml::from_str(
            r#"
enabled = true
origin = "https://bugsink.example"
api_token_secret = "BUGSINK_API_TOKEN"
[[projects]]
project_id = 7
automation_id = "incident-loop"
signing_secret = "BUGSINK_SIGNING_7"
"#,
        )
        .unwrap()
    }

    #[test]
    fn bugsink_rejects_unknown_fields_and_missing_enabled_mapping_fields() {
        assert!(toml::from_str::<BugsinkIntegrationLayer>("enable = true").is_err());
        let mut errors = Vec::new();
        let settings = resolve_bugsink(&valid_layer(), &mut errors);
        assert!(errors.is_empty());
        assert!(settings.enabled);
        assert!(!settings.dispatch_enabled);
        let mut layer = valid_layer();
        layer.projects.as_mut().unwrap()[0].project_id = None;
        resolve_bugsink(&layer, &mut errors);
        assert!(
            errors
                .iter()
                .any(|error| matches!(error, ResolveError::Missing { .. }))
        );
    }

    #[test]
    fn bugsink_rejects_ambiguous_identity_credentials_and_dispatch_without_intake() {
        for edit in 0..9 {
            let mut layer = valid_layer();
            match edit {
                0 => {
                    let duplicate = layer.projects.as_ref().unwrap()[0].clone();
                    layer.projects.as_mut().unwrap().push(duplicate);
                }
                1 => layer.api_token_secret = Some("BUGSINK_SIGNING_7".into()),
                2 => layer.projects.as_mut().unwrap()[0].project_id = Some(u64::MAX),
                3 => layer.projects.as_mut().unwrap()[0].automation_id = Some("../other".into()),
                4 => {
                    layer.origin = Some("https://user:password@bugsink.example/path?token=x".into())
                }
                5 => layer.api_token_secret = Some(" ".into()),
                6 => layer.api_token_secret = Some("bugsink-api".into()),
                7 => {
                    layer.projects.as_mut().unwrap()[0].signing_secret =
                        Some("bugsink-signing".into())
                }
                _ => {
                    layer.enabled = Some(false);
                    layer.dispatch_enabled = Some(true);
                }
            }
            let mut errors = Vec::new();
            resolve_bugsink(&layer, &mut errors);
            assert!(!errors.is_empty(), "invalid mapping {edit} was accepted");
        }
    }

    #[test]
    fn project_mapping_override_replaces_instead_of_merging_credentials() {
        use crate::Combine as _;
        let fallback = valid_layer();
        let override_layer = BugsinkIntegrationLayer {
            projects: Some(Vec::new()),
            ..Default::default()
        };
        let combined = override_layer.combine(fallback);
        let mut errors = Vec::new();
        resolve_bugsink(&combined, &mut errors);
        assert!(errors.iter().any(|error| matches!(error,
            ResolveError::Invalid { path, .. } if path.ends_with(".projects"))));
    }
}

fn resolve_external_agents(layer: Option<&ExternalAgentsLayer>) -> ExternalAgentsSettings {
    ExternalAgentsSettings {
        codex: layer
            .and_then(|agents| agents.codex.as_ref())
            .map(resolve_external_agent_profile),
        omp:   layer
            .and_then(|agents| agents.omp.as_ref())
            .map(resolve_external_agent_profile),
    }
}

fn resolve_external_agent_profile(layer: &ExternalAgentProfileLayer) -> ExternalAgentProfile {
    ExternalAgentProfile {
        command: layer.command.clone().unwrap_or_default(),
        args:    layer.args.clone().unwrap_or_default(),
        env:     layer.env.0.clone().into_iter().collect(),
    }
}

fn resolve_github_webhooks(layer: &IntegrationWebhooksLayer) -> IntegrationWebhooksSettings {
    IntegrationWebhooksSettings {
        strategy: layer.strategy,
    }
}
