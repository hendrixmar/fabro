//! The `docker` provider kind: what fabro adds to a run's spec for the
//! sandbox-driver Docker provider.
//!
//! The overlay fixes the workspace at `/workspace`, checks repositories out
//! under `/repos`, supplies the default image, pulls missing images, and drops
//! unsupported lifecycle timers. Typed Docker settings, including the two
//! managed Codex OAuth file mounts, survive the overlay. The private profile
//! is revalidated immediately before provider creation without reading it.

use std::path::Path;

use fabro_types::SandboxProviderKind;
use sandbox_driver::{HealthStatus, LifecycleTimers, SandboxSource, SandboxSpec as DriverSpec};
use sandbox_driver_docker_config::{BindMount, DockerProviderConfig};
use serde::Deserialize as _;

use crate::driver::ProviderAccess;
use crate::driver_sandbox::WorkspaceLayout;
use crate::provider_sandbox;

pub(crate) const CODEX_OAUTH_PROFILE_MARKER: &str = "FABRO_CODEX_OAUTH_PROFILE";
const CODEX_AUTH_JSON_CONTAINER_FILE: &str = "/root/.codex/auth.json";
const CODEX_AUTH_LOCK_CONTAINER_FILE: &str = "/root/.codex/auth.lock";
pub const WORKING_DIRECTORY: &str = "/workspace";
pub const REPOS_ROOT: &str = "/repos";
/// The image a Docker environment gets when it names none.
pub const DEFAULT_IMAGE: &str = "buildpack-deps:noble";

/// The workspace layout every Docker sandbox uses.
pub(crate) fn layout() -> WorkspaceLayout {
    WorkspaceLayout {
        workspace_root: WORKING_DIRECTORY.to_string(),
        repos_root:     REPOS_ROOT.to_string(),
    }
}

/// The image a Docker sandbox runs: the environment's, or the default.
pub(crate) fn effective_image(spec: &DriverSpec) -> String {
    match &spec.source {
        SandboxSource::Image { reference } => reference.clone(),
        _ => DEFAULT_IMAGE.to_string(),
    }
}

/// Docker's additions to the environment's spec: its image, fixed workspace,
/// and a pull for a missing image. Docker has no lifecycle timers.
pub(crate) fn overlay(mut spec: DriverSpec) -> crate::Result<DriverSpec> {
    let mut config = provider_config(&mut spec)?;
    config.auto_pull = true;
    if !matches!(&spec.source, SandboxSource::Image { .. }) {
        spec.source = SandboxSource::Image {
            reference: DEFAULT_IMAGE.to_string(),
        };
    }
    spec.timers = LifecycleTimers::default();
    Ok(spec
        .working_directory(WORKING_DIRECTORY)
        .provider_config(config.into_value()))
}

fn provider_config(spec: &mut DriverSpec) -> crate::Result<DockerProviderConfig> {
    let value = std::mem::take(&mut spec.provider_config);
    if value.is_null() {
        return Ok(DockerProviderConfig::default());
    }
    serde_json::from_value(value)
        .map_err(|error| crate::Error::context("invalid Docker provider config", error))
}

/// Whether the Docker daemon answers. Used by `fabro doctor`.
pub async fn check_docker_daemon() -> crate::Result<()> {
    let provider = provider_sandbox::connect_bundled_docker(&ProviderAccess::default()).await?;
    let health = provider
        .health()
        .await
        .map_err(|error| crate::Error::context("Docker health check failed", error))?;
    match health.status {
        HealthStatus::Ok | HealthStatus::Unknown => Ok(()),
        HealthStatus::Unreachable | HealthStatus::Unauthorized => {
            Err(crate::Error::message(health.message.unwrap_or_else(|| {
                "Failed to reach Docker daemon".to_string()
            })))
        }
        _ => Err(crate::Error::message(
            "Docker daemon reported an unknown health state",
        )),
    }
}

/// Add the two server-managed Codex credential file binds to the driver's
/// typed Docker configuration. The selected directory and files must already
/// exist; this never creates or reads them.
pub(crate) fn with_codex_oauth_profile(
    mut spec: DriverSpec,
    profile: &Path,
) -> crate::Result<DriverSpec> {
    if spec.env.contains_key(CODEX_OAUTH_PROFILE_MARKER) {
        return Err(crate::Error::message(
            "FABRO_CODEX_OAUTH_PROFILE is reserved for managed Docker mounts",
        ));
    }
    validate_profile_env(&spec)?;
    let mut config = provider_config(&mut spec)?;
    if !config.binds.is_empty() {
        return Err(crate::Error::message(
            "managed Codex OAuth requires exactly auth.json and auth.lock file binds",
        ));
    }
    validate_private_profile(profile)?;

    let bind = |name: &str, container: &str| -> crate::Result<BindMount> {
        let host = profile.join(name);
        let host = host.to_str().ok_or_else(|| {
            crate::Error::message("Codex OAuth profile files must have UTF-8 paths")
        })?;
        Ok(BindMount {
            host:      host.to_owned(),
            container: container.to_owned(),
            mode:      Some("rw".to_owned()),
        })
    };
    config.binds = vec![
        bind("auth.json", CODEX_AUTH_JSON_CONTAINER_FILE)?,
        bind("auth.lock", CODEX_AUTH_LOCK_CONTAINER_FILE)?,
    ];
    spec.provider_config = config.into_value();
    spec.env
        .insert(CODEX_OAUTH_PROFILE_MARKER.to_owned(), "1".to_owned());
    Ok(spec)
}

fn validate_profile_env(spec: &DriverSpec) -> crate::Result<()> {
    if spec.env.contains_key("CODEX_AUTH_B64") {
        return Err(crate::Error::message(
            "CODEX_AUTH_B64 is no longer supported; configure codex_oauth_profile instead",
        ));
    }
    if spec.env.contains_key("OPENAI_API_KEY") || spec.env.contains_key("CODEX_API_KEY") {
        return Err(crate::Error::message(
            "codex_oauth_profile cannot be combined with API key credentials",
        ));
    }
    Ok(())
}

/// Revalidate the private profile immediately before a new provider create.
pub(crate) fn validate_codex_oauth_profile_spec(
    provider: &SandboxProviderKind,
    spec: &DriverSpec,
) -> crate::Result<()> {
    let Some(marker) = spec.env.get(CODEX_OAUTH_PROFILE_MARKER) else {
        return Ok(());
    };
    if provider != &SandboxProviderKind::DOCKER {
        return Err(crate::Error::message(
            "codex_oauth_profile is supported only by Docker environments",
        ));
    }
    if marker != "1" {
        return Err(crate::Error::message(
            "FABRO_CODEX_OAUTH_PROFILE is reserved for managed Docker mounts",
        ));
    }

    validate_profile_env(spec)?;
    let config = DockerProviderConfig::deserialize(&spec.provider_config).map_err(|error| {
        crate::Error::context(
            "invalid Docker config for managed Codex OAuth mounts",
            error,
        )
    })?;
    if config.binds.len() != 2 {
        return Err(crate::Error::message(
            "managed Codex OAuth requires exactly auth.json and auth.lock file binds",
        ));
    }
    let auth_json = &config.binds[0];
    let auth_lock = &config.binds[1];
    let profile = Path::new(&auth_json.host).parent().ok_or_else(|| {
        crate::Error::message("managed Codex OAuth auth.json bind has no profile directory")
    })?;
    let expected_auth_json = profile.join("auth.json");
    let expected_auth_lock = profile.join("auth.lock");
    if !profile.is_absolute()
        || Path::new(&auth_json.host) != expected_auth_json
        || auth_json.container != CODEX_AUTH_JSON_CONTAINER_FILE
        || auth_json.mode.as_deref() != Some("rw")
        || Path::new(&auth_lock.host) != expected_auth_lock
        || auth_lock.container != CODEX_AUTH_LOCK_CONTAINER_FILE
        || auth_lock.mode.as_deref() != Some("rw")
    {
        return Err(crate::Error::message(
            "managed Codex OAuth requires exactly two writable auth.json and auth.lock file binds",
        ));
    }
    validate_private_profile(profile)
}

fn validate_private_profile(profile: &Path) -> crate::Result<()> {
    let raw_path = profile.to_str().ok_or_else(|| {
        crate::Error::message("Codex OAuth profile directory must have a UTF-8 path")
    })?;
    if !profile.is_absolute() || raw_path.contains(':') {
        return Err(crate::Error::message(
            "Codex OAuth profile directory must have an absolute UTF-8 path without ':'",
        ));
    }
    validate_private_profile_entry(profile, true)?;
    for name in ["auth.json", "auth.lock"] {
        validate_private_profile_entry(&profile.join(name), false)?;
    }
    Ok(())
}

fn validate_private_profile_entry(path: &Path, directory: bool) -> crate::Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        crate::Error::context(
            format!(
                "Codex OAuth profile requires pre-existing private {} at {}; provision it on the host before preflight",
                if directory { "directory" } else { "file" },
                path.display(),
            ),
            error,
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = if directory { 0o700 } else { 0o600 };
        let valid_type = if directory {
            metadata.is_dir()
        } else {
            metadata.is_file()
        };
        if !valid_type || metadata.permissions().mode() & 0o7777 != mode {
            return Err(crate::Error::message(format!(
                "Codex OAuth profile entry {} must be a nonsymlink {} with permissions {mode:04o}",
                path.display(),
                if directory {
                    "directory"
                } else {
                    "regular file"
                },
            )));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err(crate::Error::message(
            "Managed Codex OAuth profiles require Unix private file permissions",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    use std::time::Duration;

    use sandbox_driver::{NetworkPolicy, SandboxSource, SandboxSpec as DriverSpec};
    use sandbox_driver_docker_config::Sidecar;
    use serde::Deserialize as _;

    use super::{
        CODEX_OAUTH_PROFILE_MARKER, DEFAULT_IMAGE, DockerProviderConfig, LifecycleTimers,
        WORKING_DIRECTORY, effective_image, overlay, validate_codex_oauth_profile_spec,
        with_codex_oauth_profile,
    };
    use crate::SandboxProviderKind;

    #[test]
    fn overlay_fixes_the_workspace_and_pulls_the_named_image() {
        let mut requested = LifecycleTimers::default();
        requested.auto_stop_after_idle = Some(Duration::from_mins(45));
        requested.auto_pause_after_idle = Some(Duration::from_mins(15));
        requested.auto_archive_after_stop = Some(Duration::from_hours(1));
        requested.auto_delete_after_stop = Some(Duration::from_mins(90));
        requested.ttl = Some(Duration::from_hours(2));
        let spec = overlay(
            DriverSpec::new(SandboxSource::Image {
                reference: "ubuntu:24.04".to_string(),
            })
            .network(NetworkPolicy::Block)
            .timers(requested)
            .provider_config(
                DockerProviderConfig {
                    auto_pull: false,
                    init: true,
                    platform: Some("linux/amd64".to_string()),
                    ..DockerProviderConfig::default()
                }
                .into_value(),
            ),
        )
        .expect("docker overlay");
        assert!(matches!(
            &spec.source,
            SandboxSource::Image { reference } if reference == "ubuntu:24.04"
        ));
        assert_eq!(spec.working_directory.as_deref(), Some(WORKING_DIRECTORY));
        assert!(matches!(spec.network, NetworkPolicy::Block));
        assert_eq!(
            spec.timers,
            LifecycleTimers::default(),
            "docker has no timers to honor the environment's auto-stop with"
        );
        let config = DockerProviderConfig::deserialize(&spec.provider_config)
            .expect("docker provider config");
        assert!(config.auto_pull);
        assert!(config.init);
        assert_eq!(config.platform.as_deref(), Some("linux/amd64"));
    }

    #[test]
    fn overlay_supplies_the_default_image_when_the_environment_names_none() {
        let spec = overlay(DriverSpec::new(SandboxSource::HostDirectory)).expect("docker overlay");
        assert!(matches!(
            &spec.source,
            SandboxSource::Image { reference } if reference == DEFAULT_IMAGE
        ));
        let config = DockerProviderConfig::deserialize(&spec.provider_config)
            .expect("docker provider config");
        assert!(config.auto_pull);
        assert_eq!(
            effective_image(&DriverSpec::new(SandboxSource::HostDirectory)),
            DEFAULT_IMAGE
        );
    }

    #[test]
    fn overlay_rejects_invalid_typed_settings_without_losing_the_source() {
        let spec = DriverSpec::new(SandboxSource::HostDirectory)
            .provider_config(serde_json::json!({"unsupported": true}));
        let error = overlay(spec).expect_err("invalid Docker settings");
        let source = std::error::Error::source(&error).expect("serde cause");
        assert!(source.downcast_ref::<serde_json::Error>().is_some());
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "synthetic empty files exercise profile validation and bind construction"
    )]
    fn oauth_profile_requires_private_existing_files_and_binds_only_them() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let profile = root.path().join("profile");
        let initial_config = DockerProviderConfig {
            auto_pull: false,
            init: true,
            platform: Some("linux/amd64".to_string()),
            extra_hosts: vec!["gateway:host-gateway".to_string()],
            dns: vec!["127.0.0.1".to_string()],
            cap_add: vec!["NET_ADMIN".to_string()],
            sidecars: vec![Sidecar::new("db", "postgres:16")],
            ..DockerProviderConfig::default()
        };
        let empty = DriverSpec::new(SandboxSource::HostDirectory)
            .provider_config(initial_config.into_value());

        assert!(with_codex_oauth_profile(empty.clone(), &profile).is_err());
        assert!(!profile.exists());

        std::fs::create_dir(&profile)?;
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700))?;
        for name in ["auth.json", "auth.lock"] {
            let path = profile.join(name);
            std::fs::write(&path, "")?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }

        let spec = overlay(with_codex_oauth_profile(empty, &profile)?)?;
        assert_eq!(
            spec.env.get(CODEX_OAUTH_PROFILE_MARKER).map(String::as_str),
            Some("1")
        );
        validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec)?;
        let config = DockerProviderConfig::deserialize(&spec.provider_config)?;
        assert!(config.auto_pull);
        assert!(config.init);
        assert_eq!(config.platform.as_deref(), Some("linux/amd64"));
        assert_eq!(config.extra_hosts, ["gateway:host-gateway"]);
        assert_eq!(config.dns, ["127.0.0.1"]);
        assert_eq!(config.cap_add, ["NET_ADMIN"]);
        assert_eq!(config.sidecars.len(), 1);
        assert_eq!(config.sidecars[0].name, "db");
        assert_eq!(config.sidecars[0].image, "postgres:16");
        assert_eq!(config.binds.len(), 2);
        assert_eq!(
            config.binds[0].host,
            profile.join("auth.json").display().to_string()
        );
        assert_eq!(config.binds[0].container, "/root/.codex/auth.json");
        assert_eq!(config.binds[0].mode.as_deref(), Some("rw"));
        assert_eq!(
            config.binds[1].host,
            profile.join("auth.lock").display().to_string()
        );
        assert_eq!(config.binds[1].container, "/root/.codex/auth.lock");
        assert_eq!(config.binds[1].mode.as_deref(), Some("rw"));

        let auth_json = profile.join("auth.json");
        std::fs::set_permissions(&auth_json, std::fs::Permissions::from_mode(0o644))?;
        assert!(validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec).is_err());
        std::fs::set_permissions(&auth_json, std::fs::Permissions::from_mode(0o600))?;
        let original = profile.join("auth.json.original");
        std::fs::rename(&auth_json, &original)?;
        symlink(&original, &auth_json)?;
        assert!(validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec).is_err());
        std::fs::remove_file(&auth_json)?;
        std::fs::rename(original, auth_json)?;
        validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec)?;
        let auth_lock = profile.join("auth.lock");
        std::fs::remove_file(&auth_lock)?;
        assert!(validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec).is_err());
        std::fs::create_dir(&auth_lock)?;
        std::fs::set_permissions(&auth_lock, std::fs::Permissions::from_mode(0o600))?;
        assert!(validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec).is_err());
        std::fs::remove_dir(&auth_lock)?;
        std::fs::write(&auth_lock, "")?;
        std::fs::set_permissions(&auth_lock, std::fs::Permissions::from_mode(0o600))?;
        validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &spec)?;

        let mut spoofed = DriverSpec::new(SandboxSource::HostDirectory);
        spoofed
            .env
            .insert(CODEX_OAUTH_PROFILE_MARKER.to_owned(), "1".to_owned());
        assert!(with_codex_oauth_profile(spoofed, &profile).is_err());
        let mut invalid_marker = spec.clone();
        invalid_marker
            .env
            .insert(CODEX_OAUTH_PROFILE_MARKER.to_owned(), "0".to_owned());
        assert!(
            validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &invalid_marker)
                .is_err()
        );
        for key in ["OPENAI_API_KEY", "CODEX_API_KEY", "CODEX_AUTH_B64"] {
            let mut invalid = spec.clone();
            invalid.env.insert(key.to_string(), String::new());
            assert!(
                validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &invalid).is_err()
            );
            invalid.env.remove(CODEX_OAUTH_PROFILE_MARKER);
            assert!(with_codex_oauth_profile(invalid, &profile).is_err());
        }
        let mut directory_bind = spec.clone();
        let mut invalid_config =
            DockerProviderConfig::deserialize(&directory_bind.provider_config)?;
        invalid_config.binds[0].host = profile.display().to_string();
        invalid_config.binds[0].container = "/root/.codex".to_string();
        directory_bind.provider_config = invalid_config.into_value();
        assert!(
            validate_codex_oauth_profile_spec(&SandboxProviderKind::DOCKER, &directory_bind)
                .is_err()
        );
        assert!(validate_codex_oauth_profile_spec(&SandboxProviderKind::LOCAL, &spec).is_err());
        Ok(())
    }
}
