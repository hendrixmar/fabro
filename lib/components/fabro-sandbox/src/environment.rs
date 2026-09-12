//! What an environment asks of a sandbox, mapped once onto the driver's spec.
//!
//! The environment names an image or Dockerfile, resources, a network
//! policy, labels, variables, and a lifecycle. Every provider starts from
//! the same driver [`SandboxSpec`] built here; a bundled provider adds only
//! what its backend needs on top (the Docker working directory and default
//! image, the Daytona snapshot and timers) in its own overlay, and the
//! ownership scope adds fabro's labels. The clone policy travels beside the
//! spec as a [`CloneRequest`]: cloning is fabro's work once the sandbox
//! exists, not the provider's.

use std::collections::BTreeMap;

use fabro_types::RunId;
use fabro_types::settings::run::{
    DockerfileSource, EnvironmentNetworkMode, RunCloneSettings, RunEnvironmentSettings,
};
use sandbox_driver::{
    Capabilities, LifecycleTimers, NetworkPolicy, Resources, SandboxSource, SandboxSpec,
};

/// What to clone into a provider sandbox, if anything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CloneRequest {
    pub origin_url: Option<String>,
    /// The branch the checkout works on.
    pub branch:     Option<String>,
    /// A tag to pin the checkout to; the branch still names the checkout.
    pub tag:        Option<String>,
    /// An exact commit to pin the checkout to, authoritative over `tag`.
    pub commit_sha: Option<String>,
    /// Maximum Git history depth fetched; `None` fetches full history.
    pub depth:      Option<u32>,
    /// Create an empty workspace instead of cloning, even when an origin
    /// is present.
    pub skip:       bool,
}

impl CloneRequest {
    /// No clone: the run starts in an empty workspace.
    #[must_use]
    pub fn none() -> Self {
        Self {
            skip: true,
            ..Self::default()
        }
    }

    /// The environment's clone policy: whether to clone and how deep. The
    /// origin and the selectors come from the run's target.
    #[must_use]
    pub fn from_settings(clone: &RunCloneSettings) -> Self {
        Self {
            depth: clone
                .depth_limit()
                .and_then(|depth| u32::try_from(depth).ok()),
            skip: !clone.enabled,
            ..Self::default()
        }
    }
}

/// The driver spec every provider starts from: the environment's source
/// (an image, a Dockerfile, or a managed directory when it names neither),
/// its labels, variables, resources, network policy, and auto-stop. `env`
/// is the environment's variables, resolved by the caller: the worker
/// resolves secrets through the vault, while preflight carries them in
/// source form.
///
/// A Dockerfile given as a path must have been resolved to inline content
/// earlier; none of the providers can read a path.
pub fn sandbox_spec_for_environment(
    settings: &RunEnvironmentSettings,
    env: BTreeMap<String, String>,
) -> crate::Result<SandboxSpec> {
    // fabro-config rejects environments that set both image.docker and
    // image.dockerfile. If both still arrive here, the image wins.
    let source = match (&settings.image.docker, &settings.image.dockerfile) {
        (Some(reference), _) => SandboxSource::Image {
            reference: reference.clone(),
        },
        (None, Some(DockerfileSource::Inline(content))) => SandboxSource::Dockerfile {
            content: content.clone(),
        },
        (None, Some(DockerfileSource::Path { path })) => {
            return Err(crate::Error::message(format!(
                "environment `{}` names a Dockerfile path ({path}) that should have been \
                 resolved to inline content before sandbox creation",
                settings.id
            )));
        }
        // A provider without images (a host-style plugin) manages a
        // workspace directory of its own.
        (None, None) => SandboxSource::HostDirectory,
    };
    let network = match settings.network.mode {
        EnvironmentNetworkMode::Block => NetworkPolicy::Block,
        EnvironmentNetworkMode::AllowAll => NetworkPolicy::AllowAll,
        EnvironmentNetworkMode::CidrAllowList => NetworkPolicy::CidrAllowList {
            cidrs: settings.network.allow.clone(),
        },
    };
    let mut spec = SandboxSpec::new(source).network(network);
    // The environment's labels; fabro's ownership labels are stamped by the
    // ownership scope the provider is connected through.
    for (key, value) in &settings.labels {
        spec = spec.label(key, value);
    }
    for (key, value) in env {
        spec = spec.env_var(key, value);
    }
    let mut resources = Resources::default();
    resources.cpu_cores = settings
        .resources
        .cpu
        .and_then(|cpu| u32::try_from(cpu).ok());
    resources.memory_mb = settings
        .resources
        .memory
        .map(|size| mebibytes(size.as_bytes()));
    resources.disk_mb = settings
        .resources
        .disk
        .map(|size| mebibytes(size.as_bytes()));
    let mut timers = LifecycleTimers::default();
    timers.auto_stop_after_idle = settings
        .lifecycle
        .auto_stop
        .map(|duration| duration.as_std());
    Ok(spec.resources(resources).timers(timers))
}

/// Whole mebibytes, rounded up: the unit the driver sizes resources in.
fn mebibytes(bytes: u64) -> u64 {
    bytes.div_ceil(1024 * 1024)
}

/// The provider-side name of a run's sandbox.
pub(crate) fn run_name(run_id: &RunId) -> String {
    format!("fabro-run-{run_id}")
}

/// The environment's default `allow_all` means "unrestricted", which a
/// provider without network controls already is; asking such a provider
/// for it explicitly would be rejected. An explicit restriction is still
/// requested, and refused by the provider when it cannot honor it.
pub(crate) fn supported_network(
    requested: NetworkPolicy,
    capabilities: &Capabilities,
) -> NetworkPolicy {
    match requested {
        NetworkPolicy::AllowAll if !capabilities.network.allow_all => {
            NetworkPolicy::ProviderDefault
        }
        other => other,
    }
}

/// The environment's auto-stop is a request a backend without timers
/// cannot take; such a provider gets no timers rather than a rejected spec.
pub(crate) fn supported_timers(
    requested: LifecycleTimers,
    capabilities: &Capabilities,
) -> LifecycleTimers {
    if capabilities.lifecycle.timers {
        requested
    } else {
        LifecycleTimers::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use fabro_types::SandboxProviderKind;
    use fabro_types::settings::run::{
        EnvironmentImageSettings, EnvironmentLifecycleSettings, EnvironmentNetworkSettings,
        EnvironmentResourcesSettings,
    };
    use fabro_types::settings::{Duration as SettingsDuration, Size};

    use super::*;

    fn environment(kind: &str) -> RunEnvironmentSettings {
        RunEnvironmentSettings {
            id:        kind.to_string(),
            provider:  SandboxProviderKind::try_new(kind).unwrap(),
            cwd:       None,
            image:     EnvironmentImageSettings::default(),
            resources: EnvironmentResourcesSettings::default(),
            network:   EnvironmentNetworkSettings::default(),
            lifecycle: EnvironmentLifecycleSettings::default(),
            labels:    HashMap::from([("team".to_string(), "platform".to_string())]),
            env:       HashMap::new(),
        }
    }

    #[test]
    fn an_environment_without_an_image_asks_for_a_managed_directory() {
        let spec = sandbox_spec_for_environment(
            &environment("host"),
            BTreeMap::from([("FOO".to_string(), "bar".to_string())]),
        )
        .unwrap();
        assert!(matches!(spec.source, SandboxSource::HostDirectory));
        assert!(spec.working_directory.is_none());
        assert!(
            spec.name.is_none(),
            "the run names the sandbox, not the environment"
        );
        assert_eq!(spec.env.get("FOO").map(String::as_str), Some("bar"));
        assert_eq!(
            spec.labels.get("team").map(String::as_str),
            Some("platform")
        );
        assert!(
            !spec.labels.contains_key("sh.fabro.managed"),
            "ownership labels come from the scope, not the environment"
        );
        assert!(matches!(spec.network, NetworkPolicy::AllowAll));
        assert_eq!(spec.resources, Resources::default());
        assert_eq!(spec.timers, LifecycleTimers::default());
    }

    #[test]
    fn an_environment_with_an_image_maps_resources_network_and_lifecycle() {
        let mut settings = environment("e2b");
        settings.image.docker = Some("ubuntu:24.04".to_string());
        settings.resources.cpu = Some(2);
        settings.resources.memory = Some(Size::from_bytes(4_000_000_000));
        settings.network.mode = EnvironmentNetworkMode::Block;
        settings.lifecycle.auto_stop = Some(SettingsDuration::from_std(Duration::from_mins(45)));

        let spec = sandbox_spec_for_environment(&settings, BTreeMap::new()).unwrap();
        assert!(matches!(
            &spec.source,
            SandboxSource::Image { reference } if reference == "ubuntu:24.04"
        ));
        assert_eq!(spec.resources.cpu_cores, Some(2));
        assert_eq!(spec.resources.memory_mb, Some(3815));
        assert!(matches!(spec.network, NetworkPolicy::Block));
        assert_eq!(
            spec.timers.auto_stop_after_idle,
            Some(Duration::from_mins(45))
        );
    }

    #[test]
    fn the_clone_request_carries_the_environments_policy() {
        let clone = CloneRequest::from_settings(&RunCloneSettings::default());
        assert_eq!(clone.depth, Some(100));
        assert!(!clone.skip);

        let clone = CloneRequest::from_settings(&RunCloneSettings {
            enabled: false,
            depth:   0,
        });
        assert_eq!(clone.depth, None);
        assert!(clone.skip);
        assert!(CloneRequest::none().skip);
    }

    #[test]
    fn an_inline_dockerfile_becomes_the_source_and_a_path_is_rejected() {
        let mut settings = environment("daytona");
        settings.image.dockerfile = Some(DockerfileSource::Inline("FROM ubuntu".to_string()));
        let spec = sandbox_spec_for_environment(&settings, BTreeMap::new()).unwrap();
        assert!(matches!(
            spec.source,
            SandboxSource::Dockerfile { content } if content == "FROM ubuntu"
        ));

        settings.image.dockerfile = Some(DockerfileSource::Path {
            path: "Dockerfile".to_string(),
        });
        let error = sandbox_spec_for_environment(&settings, BTreeMap::new()).unwrap_err();
        assert!(error.to_string().contains("Dockerfile path"), "{error}");
    }

    #[test]
    fn allow_all_falls_back_to_the_provider_default_without_network_control() {
        let none = Capabilities::minimal(sandbox_driver::Isolation::None);
        assert!(matches!(
            supported_network(NetworkPolicy::AllowAll, &none),
            NetworkPolicy::ProviderDefault
        ));
        assert!(matches!(
            supported_network(NetworkPolicy::Block, &none),
            NetworkPolicy::Block
        ));
        let mut full = Capabilities::minimal(sandbox_driver::Isolation::Container);
        full.network.allow_all = true;
        assert!(matches!(
            supported_network(NetworkPolicy::AllowAll, &full),
            NetworkPolicy::AllowAll
        ));
    }

    #[test]
    fn timers_are_dropped_for_a_provider_without_them() {
        let mut requested = LifecycleTimers::default();
        requested.auto_stop_after_idle = Some(Duration::from_mins(45));
        let none = Capabilities::minimal(sandbox_driver::Isolation::None);
        assert_eq!(
            supported_timers(requested, &none),
            LifecycleTimers::default()
        );
        let mut with_timers = Capabilities::minimal(sandbox_driver::Isolation::Container);
        with_timers.lifecycle.timers = true;
        assert_eq!(supported_timers(requested, &with_timers), requested);
    }
}
