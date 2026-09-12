//! The `daytona` provider kind: what fabro adds to a run's spec for the
//! sandbox-driver Daytona provider.
//!
//! The environment's options build the spec once; Daytona's overlay fixes
//! the working directory, names the run, sets the lifecycle timers, and
//! falls back to Daytona's default snapshot when the environment names no
//! image or Dockerfile. An image or Dockerfile goes to the driver as is:
//! the Daytona provider builds it into a snapshot named by its inputs under
//! the API key and reuses that snapshot for the same inputs. The run works
//! in `/home/daytona/workspace`, with a cloned repository checked out under
//! `/home/daytona/repos` and linked into the workspace.

use std::sync::Arc;
use std::time::Duration;

use fabro_types::settings::server::ServerSandboxProviderSettings;
use fabro_types::{RunId, SandboxProviderKind};
use sandbox_driver::{
    HealthStatus, Resources, SandboxProvider, SandboxSource, SandboxSpec as DriverSpec, SnapshotId,
};
use tokio::time;

pub use crate::driver::DaytonaCredentials;
use crate::driver::{ProviderConnectOptions, connect_provider};
use crate::driver_sandbox::WorkspaceLayout;

pub(crate) const WORKING_DIRECTORY: &str = "/home/daytona/workspace";
pub(crate) const REPOS_ROOT: &str = "/home/daytona/repos";
const DEFAULT_SNAPSHOT: &str = "daytona-medium";
pub const DEFAULT_DAYTONA_API_URL: &str = "https://app.daytona.io/api";
/// Budget for the credential probe `fabro doctor` and the install flow run.
pub const DAYTONA_CREDENTIAL_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
/// Auto-stop applied when `lifecycle.auto_stop` is unset. Omitting the timer
/// would inherit Daytona's server-side default of 15 idle minutes, which is
/// shorter than a single long inference call and stops the sandbox mid-run;
/// 120 minutes clears any realistic call while still reclaiming sandboxes
/// leaked by a dead worker. An explicit zero disables auto-stop entirely.
const DEFAULT_AUTO_STOP: Duration = Duration::from_hours(2);

/// Outcome of probing a Daytona credential through the provider's health
/// check. The provider owns the list of scopes it needs and the order it
/// reports them in; fabro only renders them.
#[derive(Debug)]
pub struct DaytonaKeyCheck {
    /// Scopes the key lacks, in Daytona's wire names.
    pub missing:  Vec<String>,
    /// Every scope the provider requires, for the remediation text.
    pub required: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("Daytona credential probe timed out after {timeout:?}")]
pub struct DaytonaCredentialProbeTimeout {
    timeout: Duration,
}

impl DaytonaCredentialProbeTimeout {
    #[must_use]
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl DaytonaKeyCheck {
    #[must_use]
    pub fn ok(&self) -> bool {
        self.missing.is_empty()
    }

    #[must_use]
    pub fn missing_display(&self) -> String {
        self.missing.join(", ")
    }

    #[must_use]
    pub fn missing_message(&self) -> String {
        format!(
            "Daytona API key is missing required scopes: {}. Regenerate the key with all \
             snapshot and sandbox scopes.",
            self.missing_display()
        )
    }

    /// Every scope the provider requires, comma separated, for remediation.
    #[must_use]
    pub fn required_display(&self) -> String {
        self.required.join(", ")
    }
}

/// Whether `credentials` reach Daytona, are accepted, and carry the scopes
/// fabro needs. Reachability and authentication failures are errors; a key
/// that authenticates but lacks scopes is an `Ok` check that is not `ok()`.
pub async fn check_daytona_api_key(
    credentials: &DaytonaCredentials,
    probe_timeout: Duration,
) -> anyhow::Result<DaytonaKeyCheck> {
    let probe = async {
        let provider = connect(credentials).await?;
        let health = provider
            .health()
            .await
            .map_err(|error| anyhow::Error::new(error).context("Daytona health check failed"))?;
        match health.status {
            HealthStatus::Ok | HealthStatus::Unknown => Ok(DaytonaKeyCheck {
                missing:  Vec::new(),
                required: health.required_permissions,
            }),
            HealthStatus::Unauthorized if !health.missing_permissions.is_empty() => {
                Ok(DaytonaKeyCheck {
                    missing:  health.missing_permissions,
                    required: health.required_permissions,
                })
            }
            HealthStatus::Unauthorized => Err(anyhow::anyhow!(
                "failed to authenticate with Daytona: {}",
                health
                    .message
                    .unwrap_or_else(|| "the credential was rejected".to_string())
            )),
            _ => Err(anyhow::anyhow!(
                "failed to reach Daytona: {}",
                health
                    .message
                    .unwrap_or_else(|| "the control plane did not answer".to_string())
            )),
        }
    };
    match time::timeout(probe_timeout, probe).await {
        Ok(result) => result,
        Err(_) => Err(anyhow::Error::new(DaytonaCredentialProbeTimeout::new(
            probe_timeout,
        ))),
    }
}

async fn connect(credentials: &DaytonaCredentials) -> anyhow::Result<Arc<dyn SandboxProvider>> {
    connect_provider(
        &SandboxProviderKind::DAYTONA,
        &ServerSandboxProviderSettings::default(),
        &ProviderConnectOptions {
            host_registry_root: None,
            daytona:            Some(credentials.clone()),
        },
    )
    .await
    .map(|connected| connected.provider)
    .map_err(|error| anyhow::Error::new(error).context("Failed to connect to Daytona"))
}

/// The workspace layout every Daytona sandbox uses.
pub(crate) fn layout() -> WorkspaceLayout {
    WorkspaceLayout {
        workspace_root: WORKING_DIRECTORY.to_string(),
        repos_root:     REPOS_ROOT.to_string(),
    }
}

/// Daytona's additions to the environment's spec: the fixed working
/// directory, the run's Daytona name, the lifecycle timers, and Daytona's
/// default snapshot when the environment names no image or Dockerfile. An
/// image or Dockerfile stays as it is: the driver builds it into a cached
/// snapshot sized by the spec's resources. A create from the default
/// snapshot carries no resources, which Daytona refuses on a sandbox
/// created from a snapshot.
pub(crate) fn overlay(spec: DriverSpec, run_id: Option<&RunId>) -> DriverSpec {
    let mut spec = spec.working_directory(WORKING_DIRECTORY);
    if !matches!(
        spec.source,
        SandboxSource::Image { .. } | SandboxSource::Dockerfile { .. }
    ) {
        spec.source = SandboxSource::Snapshot {
            id: SnapshotId::try_new(DEFAULT_SNAPSHOT).expect("the default snapshot name is valid"),
        };
        spec.resources = Resources::default();
    }
    spec.name = run_id.map(|run_id| format!("fabro-{run_id}"));
    let mut timers = spec.timers;
    // An explicit zero disables auto-stop; the driver encodes
    // `Duration::ZERO` as that wire value.
    timers.auto_stop_after_idle = Some(timers.auto_stop_after_idle.unwrap_or(DEFAULT_AUTO_STOP));
    // Run sandboxes are never deleted on stop: the run record may need
    // them again on resume, and `fabro system prune` reclaims them.
    timers.auto_delete_after_stop = Some(Duration::ZERO);
    spec.timers(timers)
}

#[cfg(test)]
mod tests {
    use sandbox_driver::{LifecycleTimers, NetworkPolicy};

    use super::*;

    fn run_id() -> RunId {
        "01HY0000000000000000000000".parse().unwrap()
    }

    #[test]
    fn overlay_names_the_run_and_carries_fabro_labels_and_timers() {
        let mut resources = Resources::default();
        resources.cpu_cores = Some(2);
        let base = DriverSpec::new(SandboxSource::HostDirectory)
            .label("team", "platform")
            .network(NetworkPolicy::CidrAllowList {
                cidrs: vec!["10.0.0.0/8".to_string()],
            })
            .resources(resources);
        let spec = overlay(base, Some(&run_id()));

        assert!(
            matches!(&spec.source, SandboxSource::Snapshot { id } if id.as_str() == DEFAULT_SNAPSHOT),
            "a spec without an image comes from Daytona's default snapshot"
        );
        assert_eq!(
            spec.name.as_deref(),
            Some("fabro-01HY0000000000000000000000")
        );
        assert_eq!(spec.working_directory.as_deref(), Some(WORKING_DIRECTORY));
        // Fabro's ownership labels are stamped by the scope the provider is
        // connected through, not by the spec.
        assert!(!spec.labels.contains_key("sh.fabro.managed"));
        assert_eq!(
            spec.labels.get("team").map(String::as_str),
            Some("platform")
        );
        assert!(matches!(
            &spec.network,
            NetworkPolicy::CidrAllowList { cidrs } if cidrs == &["10.0.0.0/8".to_string()]
        ));
        assert_eq!(
            spec.timers.auto_stop_after_idle,
            Some(Duration::from_hours(2)),
            "an unset auto-stop gets fabro's explicit default, never Daytona's 15 minutes"
        );
        assert_eq!(spec.timers.auto_delete_after_stop, Some(Duration::ZERO));
        assert_eq!(
            spec.resources,
            Resources::default(),
            "the default snapshot carries the resources; Daytona refuses them on the sandbox"
        );
        assert!(!spec.ephemeral);
    }

    #[test]
    fn overlay_leaves_an_image_and_its_resources_for_the_driver_to_cache() {
        let mut resources = Resources::default();
        resources.cpu_cores = Some(2);
        resources.memory_mb = Some(4096);
        let base = DriverSpec::new(SandboxSource::Image {
            reference: "ubuntu:24.04".to_string(),
        })
        .resources(resources);
        let spec = overlay(base, None);
        assert!(
            matches!(&spec.source, SandboxSource::Image { reference } if reference == "ubuntu:24.04")
        );
        assert_eq!(
            spec.resources, resources,
            "the resources size the cached snapshot"
        );
        assert_eq!(spec.working_directory.as_deref(), Some(WORKING_DIRECTORY));

        let dockerfile = overlay(
            DriverSpec::new(SandboxSource::Dockerfile {
                content: "FROM ubuntu".to_string(),
            }),
            None,
        );
        assert!(matches!(
            dockerfile.source,
            SandboxSource::Dockerfile { .. }
        ));
    }

    #[test]
    fn overlay_passes_explicit_auto_stop_through_and_zero_disables() {
        let mut timers = LifecycleTimers::default();
        timers.auto_stop_after_idle = Some(Duration::from_mins(45));
        let base = DriverSpec::new(SandboxSource::HostDirectory)
            .network(NetworkPolicy::Block)
            .timers(timers);
        let explicit = overlay(base, None);
        assert_eq!(
            explicit.timers.auto_stop_after_idle,
            Some(Duration::from_mins(45))
        );
        assert!(matches!(explicit.network, NetworkPolicy::Block));
        assert!(explicit.name.is_none());

        let mut timers = LifecycleTimers::default();
        timers.auto_stop_after_idle = Some(Duration::ZERO);
        let disabled = overlay(
            DriverSpec::new(SandboxSource::HostDirectory).timers(timers),
            None,
        );
        assert_eq!(disabled.timers.auto_stop_after_idle, Some(Duration::ZERO));
    }

    #[test]
    fn missing_scopes_render_as_the_provider_reports_them() {
        let check = DaytonaKeyCheck {
            missing:  vec!["write:snapshots".to_string(), "write:sandboxes".to_string()],
            required: vec![
                "write:snapshots".to_string(),
                "delete:snapshots".to_string(),
                "write:sandboxes".to_string(),
                "delete:sandboxes".to_string(),
            ],
        };
        assert!(!check.ok());
        assert_eq!(check.missing_display(), "write:snapshots, write:sandboxes");
        assert_eq!(
            check.missing_message(),
            "Daytona API key is missing required scopes: write:snapshots, write:sandboxes. \
             Regenerate the key with all snapshot and sandbox scopes."
        );
        assert_eq!(
            check.required_display(),
            "write:snapshots, delete:snapshots, write:sandboxes, delete:sandboxes"
        );
    }

    #[tokio::test]
    async fn credential_probe_reports_configured_timeout() {
        // A non-routable address: the probe cannot finish within the budget.
        let credentials = DaytonaCredentials::new("dtn_test".to_string())
            .with_api_url(Some("http://10.255.255.1:1/api".to_string()));
        let err = check_daytona_api_key(&credentials, Duration::from_millis(1))
            .await
            .expect_err("probe should time out");
        let timeout = err
            .downcast_ref::<DaytonaCredentialProbeTimeout>()
            .expect("timeout should preserve its type");
        assert_eq!(timeout.timeout(), Duration::from_millis(1));
        assert_eq!(
            err.to_string(),
            "Daytona credential probe timed out after 1ms"
        );
    }
}

/// The git clone contract over the plugin wire against live Daytona.
///
/// Host and Docker derive their git facet from `Exec`, so only Daytona
/// exercises the driver's native clone through the JSON-RPC protocol. The
/// provider is served over an in-process duplex pipe exactly as a plugin
/// executable would serve it on stdio.
#[cfg(test)]
mod wire_gate {
    use std::sync::Arc;

    use fabro_static::EnvVars;
    use fabro_types::SandboxProviderKind;
    use sandbox_driver::SandboxProvider;
    use sandbox_driver_protocol::{PluginProvider, serve};
    use tokio::io::{duplex, split};

    use super::*;
    use crate::driver_sandbox::{LayoutSource, RepoWorkspace, RunSandbox};
    use crate::environment::CloneRequest;

    #[expect(
        clippy::disallowed_methods,
        reason = "the live gate takes Daytona credentials from the developer's environment"
    )]
    fn live_credentials() -> Option<DaytonaCredentials> {
        let api_key = std::env::var(EnvVars::DAYTONA_API_KEY).ok()?;
        Some(DaytonaCredentials::from_api_key(api_key, |name| {
            std::env::var(name).ok()
        }))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires live Daytona credentials and provisions a sandbox"]
    async fn native_clone_over_the_wire_lays_out_the_repository() {
        let credentials = live_credentials().expect("DAYTONA_API_KEY must be set");
        let in_process = connect(&credentials).await.expect("connect to Daytona");

        let (host_side, plugin_side) = duplex(1024 * 1024);
        let (host_read, host_write) = split(host_side);
        let (plugin_read, plugin_write) = split(plugin_side);
        tokio::spawn(serve(Arc::clone(&in_process), plugin_read, plugin_write));
        let remote = PluginProvider::connect(host_read, host_write)
            .await
            .expect("protocol handshake");
        assert_eq!(remote.kind().as_str(), "daytona");
        let remote: Arc<dyn SandboxProvider> = Arc::new(remote);

        let workspace = RepoWorkspace::plan(
            LayoutSource::Fixed(layout()),
            &CloneRequest {
                origin_url: Some("https://github.com/brynary/rack-test".to_string()),
                depth: Some(100),
                ..CloneRequest::default()
            },
            None,
        )
        .expect("clone plan");
        // No image: the overlay creates from Daytona's default snapshot.
        let spec = overlay(DriverSpec::new(SandboxSource::HostDirectory), None);
        let sandbox = RunSandbox::pending(SandboxProviderKind::DAYTONA, remote, spec, workspace);
        sandbox
            .initialize()
            .await
            .expect("initialize over the wire");

        let checks = async {
            assert_eq!(
                sandbox.working_directory(),
                "/home/daytona/workspace/rack-test"
            );
            let result = sandbox
                .exec_command(
                    "test -d /home/daytona/repos/brynary/rack-test/.git && \
                     test -L /home/daytona/workspace/rack-test && \
                     git rev-parse --is-inside-work-tree",
                    30_000,
                    None,
                    None,
                    None,
                )
                .await
                .expect("layout check");
            assert!(result.success(), "{result:?}");
            assert!(result.stdout_lossy().contains("true"));
            let layout = sandbox.workspace_layout().expect("layout record");
            assert_eq!(
                layout.primary_repo_path.as_deref(),
                Some("/home/daytona/repos/brynary/rack-test")
            );
        };
        checks.await;
        sandbox.delete().await.expect("cleanup");
    }
}
