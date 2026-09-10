//! Convert resolved [`RunEnvironmentSettings`] into runtime sandbox configs.
//!
//! These mappings are consumed by both the workflow run-start path and the
//! server preflight path, so they live here next to their destination types.

use std::path::{Path, PathBuf};

#[cfg(feature = "docker")]
use fabro_types::settings::ResolveError;
#[cfg(feature = "daytona")]
use fabro_types::settings::run::DockerfileSource as ResolvedDockerfileSource;
use fabro_types::settings::run::{
    EnvironmentNetworkMode, RunCloneSettings, RunEnvironmentSettings,
};

#[cfg(feature = "daytona")]
use crate::config::{
    DaytonaNetwork, DaytonaSnapshotSettings, DaytonaSnapshotSource,
    DockerfileSource as SandboxDockerfileSource,
};
#[cfg(feature = "daytona")]
use crate::daytona::DaytonaConfig;
#[cfg(feature = "docker")]
use crate::docker::DockerSandboxOptions;

#[cfg(feature = "daytona")]
#[must_use]
pub fn daytona_config_from_environment(
    settings: &RunEnvironmentSettings,
    clone: &RunCloneSettings,
) -> DaytonaConfig {
    // fabro-config rejects Daytona environments that set both image.docker
    // and image.dockerfile. If both still arrive here, the image wins, which
    // matches how the Docker provider treats the pair.
    let source = match (&settings.image.docker, &settings.image.dockerfile) {
        (Some(image), _) => Some(DaytonaSnapshotSource::Image(image.clone())),
        (None, Some(ResolvedDockerfileSource::Inline(text))) => Some(
            DaytonaSnapshotSource::Dockerfile(SandboxDockerfileSource::Inline(text.clone())),
        ),
        (None, Some(ResolvedDockerfileSource::Path { path })) => Some(
            DaytonaSnapshotSource::Dockerfile(SandboxDockerfileSource::Path { path: path.clone() }),
        ),
        (None, None) => None,
    };
    let snapshot = source.map(|source| DaytonaSnapshotSettings {
        cpu: settings.resources.cpu,
        memory: settings
            .resources
            .memory
            .map(|size| size_to_gb_i32(size.as_bytes())),
        disk: settings
            .resources
            .disk
            .map(|size| size_to_gb_i32(size.as_bytes())),
        source,
    });

    DaytonaConfig {
        auto_stop_interval: settings
            .lifecycle
            .auto_stop
            .map(|duration| duration_to_minutes_i32(duration.as_std())),
        labels: (!settings.labels.is_empty()).then(|| settings.labels.clone()),
        snapshot,
        network: Some(match settings.network.mode {
            EnvironmentNetworkMode::Block => DaytonaNetwork::Block,
            EnvironmentNetworkMode::AllowAll => DaytonaNetwork::AllowAll,
            EnvironmentNetworkMode::CidrAllowList => {
                DaytonaNetwork::AllowList(settings.network.allow.clone())
            }
        }),
        clone_depth: clone.depth_limit(),
        skip_clone: !clone.enabled,
    }
}

#[cfg(feature = "docker")]
#[must_use]
pub fn docker_config_from_environment(
    settings: &RunEnvironmentSettings,
    clone: &RunCloneSettings,
) -> DockerSandboxOptions {
    // No vault is available on this path (server preflight / manifest), so a
    // `{{ secrets.* }}` value keeps its source form. Nothing else is left to
    // resolve: `{{ vars.* }}` is substituted at run creation.
    #[expect(
        clippy::disallowed_methods,
        reason = "preflight has no vault, so an unresolved secret token is carried in source \
                  form; the real value is resolved by docker_config_from_environment_with_secrets"
    )]
    let env = settings
        .env
        .iter()
        .map(|(key, value)| (key.clone(), value.as_source()))
        .collect();
    docker_config_from_environment_env(settings, clone, env)
}

#[cfg(feature = "docker")]
pub fn docker_config_from_environment_with_secrets(
    settings: &RunEnvironmentSettings,
    clone: &RunCloneSettings,
    secrets_lookup: impl FnMut(&str) -> Option<String>,
) -> Result<DockerSandboxOptions, ResolveError> {
    let env = settings.resolve_env(secrets_lookup)?;
    Ok(docker_config_from_environment_env(settings, clone, env))
}

#[cfg(feature = "docker")]
fn docker_config_from_environment_env(
    settings: &RunEnvironmentSettings,
    clone: &RunCloneSettings,
    env: std::collections::HashMap<String, String>,
) -> DockerSandboxOptions {
    let mut env_vars = env
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    env_vars.sort();
    let default_options = DockerSandboxOptions::default();

    DockerSandboxOptions {
        image: settings
            .image
            .docker
            .clone()
            .unwrap_or(default_options.image),
        network_mode: match settings.network.mode {
            EnvironmentNetworkMode::Block => Some("none".to_string()),
            EnvironmentNetworkMode::AllowAll | EnvironmentNetworkMode::CidrAllowList => {
                default_options.network_mode
            }
        },
        memory_limit: settings
            .resources
            .memory
            .and_then(|size| i64::try_from(size.as_bytes()).ok()),
        cpu_quota: settings
            .resources
            .cpu
            .map(|cpu| i64::from(cpu).saturating_mul(100_000)),
        env_vars,
        clone_depth: clone
            .depth_limit()
            .and_then(|depth| usize::try_from(depth).ok()),
        skip_clone: !clone.enabled,
        ..DockerSandboxOptions::default()
    }
}

pub fn local_working_directory_from_environment(
    settings: &RunEnvironmentSettings,
    source_directory: Option<&Path>,
) -> crate::Result<PathBuf> {
    if let Some(cwd) = settings.cwd.as_deref() {
        return Ok(PathBuf::from(cwd));
    }

    let Some(source_directory) = source_directory else {
        return Err(crate::Error::message(
            "local environment requires a server-side working directory; configure `environment.cwd = \"/absolute/path\"` on the selected local environment",
        ));
    };

    if source_directory.is_dir() {
        return Ok(source_directory.to_path_buf());
    }

    Err(crate::Error::message(format!(
        "local environment source_directory does not exist or is not a directory on this server: {}. Configure `environment.cwd = \"/absolute/path\"` on the selected local environment for remote client/server deployments.",
        source_directory.display()
    )))
}

#[cfg(feature = "daytona")]
fn duration_to_minutes_i32(duration: std::time::Duration) -> i32 {
    let minutes = duration.as_secs() / 60;
    i32::try_from(minutes).unwrap_or(i32::MAX)
}

#[cfg(feature = "daytona")]
fn size_to_gb_i32(bytes: u64) -> i32 {
    let gb = bytes / 1_000_000_000;
    i32::try_from(gb).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    use fabro_types::settings::run::{
        EnvironmentImageSettings, EnvironmentLifecycleSettings, EnvironmentNetworkSettings,
        EnvironmentProvider, EnvironmentResourcesSettings,
    };

    use super::*;

    fn run_environment(provider: EnvironmentProvider) -> RunEnvironmentSettings {
        RunEnvironmentSettings {
            id: "host".to_string(),
            provider,
            cwd: None,
            image: EnvironmentImageSettings::default(),
            resources: EnvironmentResourcesSettings::default(),
            network: EnvironmentNetworkSettings::default(),
            lifecycle: EnvironmentLifecycleSettings::default(),
            labels: HashMap::new(),
            env: HashMap::new(),
        }
    }

    #[test]
    fn local_working_directory_prefers_environment_cwd() {
        let mut settings = run_environment(EnvironmentProvider::Local);
        settings.cwd = Some("/srv/fabro/workspaces/team-a".to_string());
        let missing_source = Path::new("/path/that/should/not/exist");

        let resolved = local_working_directory_from_environment(&settings, Some(missing_source))
            .expect("configured cwd should be accepted");

        assert_eq!(resolved, PathBuf::from("/srv/fabro/workspaces/team-a"));
        assert!(!missing_source.exists());
    }

    #[test]
    fn local_working_directory_uses_existing_source_directory_without_cwd() {
        let settings = run_environment(EnvironmentProvider::Local);
        let dir = tempfile::tempdir().unwrap();

        let resolved = local_working_directory_from_environment(&settings, Some(dir.path()))
            .expect("existing source directory should be accepted");

        assert_eq!(resolved, dir.path());
    }

    #[test]
    fn local_working_directory_rejects_missing_source_directory_without_cwd() {
        let settings = run_environment(EnvironmentProvider::Local);
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("client-only");

        let err = local_working_directory_from_environment(&settings, Some(&missing))
            .expect_err("missing source directory without cwd should fail");

        let message = err.to_string();
        assert!(
            message.contains("environment.cwd") && message.contains("does not exist"),
            "unexpected error: {message}"
        );
        assert!(!missing.exists());
    }

    #[cfg(feature = "daytona")]
    #[test]
    fn daytona_config_maps_docker_image_to_snapshot() {
        let mut settings = run_environment(EnvironmentProvider::Daytona);
        settings.image.docker = Some("ubuntu:24.04".to_string());
        settings.resources.cpu = Some(2);

        let config = daytona_config_from_environment(&settings, &RunCloneSettings::default());
        let snapshot = config.snapshot.expect("image should configure a snapshot");

        assert_eq!(
            snapshot.source,
            DaytonaSnapshotSource::Image("ubuntu:24.04".to_string())
        );
        assert_eq!(snapshot.cpu, Some(2));
    }
}
