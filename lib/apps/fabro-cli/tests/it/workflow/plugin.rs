//! Sandbox providers served by sandbox-driver plugin executables, for the
//! workflow scenarios.
//!
//! The executables are the driver's own `sandbox-driver-host` and
//! `sandbox-driver-docker`, found on `PATH`; CI installs them at the rev the
//! workspace pins, and a developer installs them with
//! `cargo install --locked --git https://github.com/lithoscomputer/sandbox-driver --rev <rev> sandbox-driver-host sandbox-driver-docker`.
//! Each runs under a kind of the scenario's choosing (`host`,
//! `docker-plugin`): the configured kind names the plugin, whatever the
//! executable declares. A scenario configured here runs against its own
//! server so the plugin settings and the environment it creates never leak
//! into the shared session server.

#![expect(
    clippy::disallowed_methods,
    reason = "test setup reads the process environment for its opt-in gate and probes Docker synchronously"
)]
#![expect(
    clippy::print_stderr,
    reason = "a skipped scenario says why on the test's stderr"
)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use fabro_test::{TestContext, expect_reqwest_status};
use serde_json::json;

use crate::cmd::support::server_endpoint;

/// Set in CI so a missing executable or daemon fails the test instead of
/// skipping it.
const REQUIRE_ENV: &str = "FABRO_REQUIRE_SANDBOX_PLUGINS";
const DOCKER_IMAGE: &str = "buildpack-deps:noble";

#[derive(Clone, Copy, Debug)]
pub(crate) enum Plugin {
    /// The driver's Host executable under the non-bundled `host` kind.
    Host,
    /// The driver's Docker executable under the non-bundled `docker-plugin`
    /// kind: the same containers, reached over stdio.
    Docker,
}

impl Plugin {
    fn kind(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Docker => "docker-plugin",
        }
    }

    fn executable(self) -> &'static str {
        match self {
            Self::Host => "sandbox-driver-host",
            Self::Docker => "sandbox-driver-docker",
        }
    }

    /// The environment id the scenario selects with `--environment`.
    fn environment(self) -> &'static str {
        match self {
            Self::Host => "host-plugin",
            Self::Docker => "docker-plugin",
        }
    }
}

/// Point `context` at an isolated server that serves `plugin` and has an
/// environment for it. Returns the environment id, or `None` when the
/// prerequisites are missing and the test should skip.
pub(crate) fn configure(context: &mut TestContext, plugin: Plugin) -> Option<&'static str> {
    let required = std::env::var_os(REQUIRE_ENV).is_some();
    let Some(executable) = plugin_executable(plugin) else {
        assert!(
            !required,
            "{REQUIRE_ENV} is set but the {} executable is not built",
            plugin.executable()
        );
        eprintln!(
            "skipping: {} is not on PATH; install the sandbox-driver executables at the rev \
             Cargo.toml pins",
            plugin.executable()
        );
        return None;
    };
    if matches!(plugin, Plugin::Docker) && !docker_image_available() {
        assert!(
            !required,
            "{REQUIRE_ENV} is set but no Docker daemon with {DOCKER_IMAGE} is available"
        );
        eprintln!("skipping: no Docker daemon with {DOCKER_IMAGE}");
        return None;
    }

    let storage_dir = context.temp_dir.join("plugin-server-storage");
    let registry = context.temp_dir.join("host-registry");
    std::fs::create_dir_all(&registry).expect("registry dir should be created");
    let settings = match plugin {
        Plugin::Host => format!(
            r#"[server.storage]
root = "{storage}"

[server.auth]
methods = ["dev-token"]

[server.sandbox.providers.host]
path = "{path}"
dev = true
inherit_env = ["PATH", "HOME"]

[server.sandbox.providers.host.env]
SANDBOX_DRIVER_HOST_REGISTRY = "{registry}"
"#,
            storage = toml_path(&storage_dir),
            path = toml_path(&executable),
            registry = toml_path(&registry),
        ),
        Plugin::Docker => format!(
            r#"[server.storage]
root = "{storage}"

[server.auth]
methods = ["dev-token"]

[server.sandbox.providers.docker-plugin]
path = "{path}"
dev = true
inherit_env = ["PATH", "HOME", "DOCKER_HOST", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY"]
"#,
            storage = toml_path(&storage_dir),
            path = toml_path(&executable),
        ),
    };
    context.write_home(".fabro/settings.toml", settings);
    context.isolated_server();
    create_environment(&context.storage_dir, plugin);
    Some(plugin.environment())
}

/// The driver executable on `PATH`, when installed.
fn plugin_executable(plugin: Plugin) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(plugin.executable()))
        .find(|candidate| candidate.is_file())
}

fn docker_image_available() -> bool {
    Command::new("docker")
        .args(["image", "inspect", DOCKER_IMAGE])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn toml_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn create_environment(storage_dir: &Path, plugin: Plugin) {
    let body = json!({
        "id": plugin.environment(),
        "provider": plugin.kind(),
        "image": {
            "docker": match plugin {
                Plugin::Host => serde_json::Value::Null,
                Plugin::Docker => json!(DOCKER_IMAGE),
            },
            "dockerfile": null
        },
        "resources": { "cpu": null, "memory": null, "disk": null },
        "network": { "mode": "allow_all", "allow": [] },
        "lifecycle": { "preserve": false, "stop_on_terminal": true, "auto_stop": null },
        "labels": {},
        "env": {}
    });
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime should build")
        .block_on(async {
            let (client, base_url) =
                server_endpoint(storage_dir).expect("isolated server endpoint should exist");
            let response = client
                .post(format!("{base_url}/api/v1/environments"))
                .json(&body)
                .send()
                .await
                .expect("environment create request should send");
            if response.status() != fabro_http::StatusCode::CREATED {
                eprintln!("server log tail:\n{}", server_log_tail(storage_dir));
            }
            expect_reqwest_status(
                response,
                fabro_http::StatusCode::CREATED,
                "POST /api/v1/environments",
            )
            .await;
        });
}

/// Run a scenario; when it fails, print the isolated server's log first, since
/// the worker's stderr (and so a plugin's launch failure) lands only there
/// and the server root is removed when the context drops.
pub(crate) fn run_with_server_log(context: &TestContext, scenario: impl FnOnce()) {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(scenario));
    if let Err(panic) = outcome {
        eprintln!(
            "server log tail:\n{}",
            server_log_tail(&context.storage_dir)
        );
        std::panic::resume_unwind(panic);
    }
}

/// The last lines of the isolated server's log, for a failure message.
pub(crate) fn server_log_tail(storage_dir: &Path) -> String {
    let path = fabro_config::Storage::new(storage_dir)
        .runtime_directory()
        .log_path();
    let Ok(contents) = std::fs::read_to_string(&path) else {
        return format!("(no server log at {})", path.display());
    };
    let lines: Vec<&str> = contents.lines().collect();
    let start = lines.len().saturating_sub(60);
    lines[start..].join("\n")
}
