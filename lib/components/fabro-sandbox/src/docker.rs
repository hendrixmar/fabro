//! The `docker` provider kind: what fabro adds to a run's spec for the
//! sandbox-driver Docker provider.
//!
//! The environment's options build the spec once; Docker's overlay fixes the
//! container's working directory at [`WORKING_DIRECTORY`], supplies the
//! default image when the environment names none, and asks the provider to
//! pull a missing image. A cloned repository checks out under
//! [`REPOS_ROOT`] and is linked into the workspace, so the run works in
//! `/workspace/<repo>`.

use sandbox_driver::{HealthStatus, LifecycleTimers, SandboxSource, SandboxSpec as DriverSpec};
use sandbox_driver_docker_config::DockerProviderConfig;

use crate::driver::ProviderAccess;
use crate::driver_sandbox::WorkspaceLayout;
use crate::provider_sandbox;

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

/// Docker's additions to the environment's spec: the image it will run,
/// the fixed working directory, and a pull for a missing image. Docker has
/// no lifecycle timers, so the environment's auto-stop does not apply.
pub(crate) fn overlay(spec: DriverSpec) -> DriverSpec {
    let image = effective_image(&spec);
    let mut spec = spec;
    spec.source = SandboxSource::Image { reference: image };
    spec.timers = LifecycleTimers::default();
    spec.working_directory(WORKING_DIRECTORY).provider_config(
        DockerProviderConfig {
            auto_pull: true,
            ..DockerProviderConfig::default()
        }
        .into_value(),
    )
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sandbox_driver::NetworkPolicy;

    use super::*;

    #[test]
    fn overlay_fixes_the_workspace_and_pulls_the_named_image() {
        let mut requested = LifecycleTimers::default();
        requested.auto_stop_after_idle = Some(Duration::from_mins(45));
        let spec = overlay(
            DriverSpec::new(SandboxSource::Image {
                reference: "ubuntu:24.04".to_string(),
            })
            .network(NetworkPolicy::Block)
            .timers(requested),
        );
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
        let config: DockerProviderConfig =
            serde_json::from_value(spec.provider_config).expect("docker provider config");
        assert!(config.auto_pull);
    }

    #[test]
    fn overlay_supplies_the_default_image_when_the_environment_names_none() {
        let spec = overlay(DriverSpec::new(SandboxSource::HostDirectory));
        assert!(matches!(
            &spec.source,
            SandboxSource::Image { reference } if reference == DEFAULT_IMAGE
        ));
        assert_eq!(
            effective_image(&DriverSpec::new(SandboxSource::HostDirectory)),
            DEFAULT_IMAGE
        );
    }
}
