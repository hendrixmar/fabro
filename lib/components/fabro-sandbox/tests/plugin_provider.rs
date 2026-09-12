//! The construction function serves a non-bundled kind through a plugin
//! executable, and a sandbox created through one plugin generation is
//! reachable by persisted id from a fresh connection.
//!
//! The executable is the driver's own `sandbox-driver-host`, found on `PATH`
//! (CI installs it at the rev the workspace pins). Without it the tests skip,
//! unless `FABRO_REQUIRE_SANDBOX_PLUGINS` is set.

#![expect(
    clippy::disallowed_methods,
    reason = "the test locates the plugin executable through the process PATH"
)]
#![expect(clippy::print_stderr, reason = "a skipped test says why on its stderr")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use fabro_sandbox::driver::{ProviderConnectOptions, connect_provider};
use fabro_types::SandboxProviderKind;
use fabro_types::settings::server::{SandboxPluginSettings, ServerSandboxProviderSettings};
use sandbox_driver::{ExecSpec, SandboxId, SandboxSource, SandboxSpec};

const HOST_PLUGIN: &str = "sandbox-driver-host";
const REQUIRE_ENV: &str = "FABRO_REQUIRE_SANDBOX_PLUGINS";

/// The driver's Host executable on `PATH`, or `None` (after saying so) when
/// the test should skip.
fn host_plugin() -> Option<PathBuf> {
    let found = std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join(HOST_PLUGIN))
            .find(|candidate| candidate.is_file())
    });
    if found.is_none() {
        assert!(
            std::env::var_os(REQUIRE_ENV).is_none(),
            "{REQUIRE_ENV} is set but {HOST_PLUGIN} is not on PATH"
        );
        eprintln!("skipping: {HOST_PLUGIN} is not on PATH");
    }
    found
}

fn host_plugin_settings(executable: &Path, registry: &Path) -> ServerSandboxProviderSettings {
    ServerSandboxProviderSettings {
        enabled: true,
        plugin:  Some(SandboxPluginSettings {
            path:        Some(executable.display().to_string()),
            sha256:      None,
            dev:         true,
            args:        Vec::new(),
            env:         BTreeMap::from([(
                "SANDBOX_DRIVER_HOST_REGISTRY".to_string(),
                registry.display().to_string(),
            )]),
            inherit_env: Vec::new(),
        }),
    }
}

#[tokio::test]
async fn host_plugin_under_a_non_bundled_kind_creates_and_reattaches_by_persisted_id() {
    let Some(executable) = host_plugin() else {
        return;
    };
    let registry = tempfile::tempdir().expect("registry tempdir");
    let workspace = tempfile::tempdir().expect("workspace tempdir");
    let kind = SandboxProviderKind::try_new("host").expect("host is a valid kind");
    assert_eq!(
        kind.bundled(),
        None,
        "host is not one of fabro's bundled kinds"
    );
    let settings = host_plugin_settings(&executable, registry.path());

    let persisted_id: SandboxId = {
        let connected = connect_provider(&kind, &settings, &ProviderConnectOptions::default())
            .await
            .expect("plugin launches");
        assert_eq!(connected.kind, kind);
        assert_eq!(connected.provider.kind().as_str(), "host");
        let spec = SandboxSpec::new(SandboxSource::HostDirectory)
            .working_directory(workspace.path().display().to_string())
            .label("sh.fabro.managed", "true");
        let sandbox = connected
            .provider
            .create(&spec, None)
            .await
            .expect("create over the wire");
        let result = sandbox
            .exec()
            .run(&ExecSpec::bash(
                "printf hello > marker.txt && cat marker.txt",
            ))
            .await
            .expect("exec over the wire");
        assert!(result.success(), "{result:?}");
        assert_eq!(result.stdout_lossy(), "hello");
        sandbox.id().clone()
    };

    // A fresh connection is a new plugin process; the id alone must be
    // enough to find the sandbox again, exactly as run reconnect will do.
    let connected = connect_provider(&kind, &settings, &ProviderConnectOptions::default())
        .await
        .expect("plugin relaunches");
    let sandbox = connected
        .provider
        .attach(&persisted_id, None)
        .await
        .expect("attach by persisted id");
    let content = sandbox
        .fs()
        .read("marker.txt")
        .await
        .expect("file survives across plugin generations");
    assert_eq!(content, b"hello");
    assert!(workspace.path().join("marker.txt").is_file());
    sandbox.delete().await.expect("delete releases the handle");
    assert!(
        workspace.path().is_dir(),
        "designated directories are never removed by delete"
    );
}

/// The configured kind is fabro's name for the executable it points at; the
/// plugin's own declared kind is information, not a gate.
#[tokio::test]
async fn the_configured_kind_names_the_plugin_whatever_it_declares() {
    let Some(executable) = host_plugin() else {
        return;
    };
    let registry = tempfile::tempdir().expect("registry tempdir");
    let kind = SandboxProviderKind::try_new("host-alias").expect("valid kind");
    let connected = connect_provider(
        &kind,
        &host_plugin_settings(&executable, registry.path()),
        &ProviderConnectOptions::default(),
    )
    .await
    .expect("an aliased plugin launches");
    assert_eq!(connected.kind, kind);
    // Fabro's handle on the plugin carries the configured name, so records,
    // events, and errors all speak of the kind the operator wrote down.
    assert_eq!(connected.provider.kind().as_str(), "host-alias");
}
