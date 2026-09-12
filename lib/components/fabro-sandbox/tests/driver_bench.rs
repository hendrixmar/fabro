//! Phase 2 of the sandbox-driver adoption: measure agent tool-call latency
//! through the driver against fabro's current providers before any cutover.
//!
//! Three comparisons, each over the same medium repository (fabro's own
//! `lib/` tree, about 1,100 Rust files):
//!
//! - Docker file reads and content search: fabro's driver-backed Docker sandbox
//!   (its path resolution and result shaping) against the bare driver
//!   `DockerProvider` in-process.
//! - Host tool calls: fabro's local sandbox against the driver `HostProvider`
//!   in-process, to confirm no regression on the path every local run takes.
//! - The wire: the driver Host and Docker providers served over the JSON-RPC
//!   protocol on an in-process duplex pipe, to size the budget for running a
//!   provider out of process later (the plan allows 100 ms per tool call).
//!
//! Ignored: it needs a Docker daemon with `buildpack-deps:noble` present and
//! takes a minute. Run with
//! `cargo nextest run -p fabro-sandbox --test driver_bench --run-ignored only
//! --no-capture`.

#![allow(
    clippy::print_stderr,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a benchmark reports through stderr and rounds durations for display"
)]
#![expect(
    clippy::disallowed_methods,
    reason = "the fixture is packed and enumerated synchronously before the timed section starts"
)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fabro_sandbox::{
    CloneRequest, ProviderAccess, RunSandbox, SandboxProviderKind, local_sandbox, provider_sandbox,
};
use sandbox_driver::{
    ExecSpec, GrepOptions, Sandbox as DriverHandle, SandboxProvider, SandboxSource, SandboxSpec,
    Search,
};
use sandbox_driver_docker::DockerProvider;
use sandbox_driver_host::HostProvider;
use sandbox_driver_protocol::{PluginProvider, serve};
use tokio::io::{duplex, split};

const IMAGE: &str = "buildpack-deps:noble";
const READS: usize = 200;
const GREPS: usize = 20;
const GREP_PATTERN: &str = "async fn ";

/// The medium repository: fabro's `lib/` tree, packed once per run.
struct Repository {
    tarball: PathBuf,
    /// Repository-relative paths of the files the read benchmark samples.
    files:   Vec<String>,
    _dir:    tempfile::TempDir,
}

impl Repository {
    fn pack() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace lib dir");
        let dir = tempfile::tempdir().expect("tempdir");
        let tarball = dir.path().join("repo.tar");
        let status = Command::new("tar")
            .args(["-cf"])
            .arg(&tarball)
            .args(["--exclude", "target", "--exclude", "node_modules", "-C"])
            .arg(&root)
            .arg(".")
            .status()
            .expect("tar available");
        assert!(status.success(), "packing the repository failed");
        let mut files: Vec<String> = walkdir(&root)
            .into_iter()
            .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
            .filter_map(|path| {
                path.strip_prefix(&root)
                    .ok()
                    .map(|rel| rel.to_string_lossy().into_owned())
            })
            .collect();
        files.sort();
        // A fixed stride samples the tree evenly and identically for every
        // provider under test.
        let stride = (files.len() / READS).max(1);
        let files = files.into_iter().step_by(stride).take(READS).collect();
        Self {
            tarball,
            files,
            _dir: dir,
        }
    }
}

fn walkdir(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "target") {
                    continue;
                }
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

#[derive(Default)]
struct Samples(Vec<Duration>);

impl Samples {
    fn record(&mut self, duration: Duration) {
        self.0.push(duration);
    }

    fn percentile(&self, pct: f64) -> Duration {
        let mut sorted = self.0.clone();
        sorted.sort();
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        let index = ((sorted.len() - 1) as f64 * pct).round() as usize;
        sorted[index]
    }

    fn mean(&self) -> Duration {
        if self.0.is_empty() {
            return Duration::ZERO;
        }
        self.0.iter().sum::<Duration>() / self.0.len() as u32
    }
}

struct Row {
    label: &'static str,
    op:    &'static str,
    n:     usize,
    stats: Samples,
}

fn report(rows: &[Row]) {
    eprintln!();
    eprintln!(
        "{:<34} {:<8} {:>5} {:>9} {:>9} {:>9}",
        "provider", "op", "n", "p50 ms", "p95 ms", "mean ms"
    );
    for row in rows {
        eprintln!(
            "{:<34} {:<8} {:>5} {:>9.2} {:>9.2} {:>9.2}",
            row.label,
            row.op,
            row.n,
            row.stats.percentile(0.5).as_secs_f64() * 1000.0,
            row.stats.percentile(0.95).as_secs_f64() * 1000.0,
            row.stats.mean().as_secs_f64() * 1000.0,
        );
    }
    eprintln!();
}

/// The two operations an agent issues most: a file read and a content
/// search, expressed against fabro's current trait.
async fn bench_fabro(label: &'static str, sandbox: &RunSandbox, repo: &Repository) -> Vec<Row> {
    let mut reads = Samples::default();
    for file in &repo.files {
        let started = Instant::now();
        let bytes = sandbox
            .read_file_bytes(&format!("repo/{file}"))
            .await
            .expect("read");
        assert!(!bytes.is_empty());
        reads.record(started.elapsed());
    }
    let mut greps = Samples::default();
    let mut options = GrepOptions::default();
    options.include = Some("*.rs".to_owned());
    options.max_matches = Some(50);
    for _ in 0..GREPS {
        let started = Instant::now();
        let matches = sandbox
            .grep(GREP_PATTERN, "repo", &options)
            .await
            .expect("grep");
        assert!(!matches.is_empty());
        greps.record(started.elapsed());
    }
    vec![
        Row {
            label,
            op: "read",
            n: repo.files.len(),
            stats: reads,
        },
        Row {
            label,
            op: "grep",
            n: GREPS,
            stats: greps,
        },
    ]
}

/// The same two operations against the driver's facets.
async fn bench_driver(
    label: &'static str,
    sandbox: &dyn DriverHandle,
    repo: &Repository,
) -> Vec<Row> {
    let mut reads = Samples::default();
    for file in &repo.files {
        let started = Instant::now();
        let bytes = sandbox
            .fs()
            .read(&format!("repo/{file}"))
            .await
            .expect("read");
        assert!(!bytes.is_empty());
        reads.record(started.elapsed());
    }
    let search = sandbox.search().expect("search facet");
    let mut options = GrepOptions::default();
    options.include = Some("*.rs".to_owned());
    options.max_matches = Some(50);
    let mut greps = Samples::default();
    for _ in 0..GREPS {
        let started = Instant::now();
        let matches = search
            .grep(GREP_PATTERN, "repo", &options)
            .await
            .expect("grep");
        assert!(!matches.is_empty());
        greps.record(started.elapsed());
    }
    vec![
        Row {
            label,
            op: "read",
            n: repo.files.len(),
            stats: reads,
        },
        Row {
            label,
            op: "grep",
            n: GREPS,
            stats: greps,
        },
    ]
}

async fn unpack_fabro(sandbox: &RunSandbox, repo: &Repository) {
    sandbox
        .upload_file_from_local(&repo.tarball, "/tmp/repo.tar")
        .await
        .expect("upload");
    let result = sandbox
        .exec_command(
            "mkdir -p repo && tar -xf /tmp/repo.tar -C repo",
            120_000,
            None,
            None,
            None,
        )
        .await
        .expect("unpack exec");
    assert!(result.success(), "unpack failed: {}", result.stderr_lossy());
}

async fn unpack_driver(sandbox: &dyn DriverHandle, repo: &Repository) {
    sandbox
        .fs()
        .upload(&repo.tarball, "/tmp/repo.tar")
        .await
        .expect("upload");
    let result = sandbox
        .exec()
        .run(
            &ExecSpec::bash("mkdir -p repo && tar -xf /tmp/repo.tar -C repo")
                .timeout(Duration::from_mins(2)),
        )
        .await
        .expect("unpack exec");
    assert!(result.success(), "unpack failed: {}", result.stderr_lossy());
}

fn docker_spec() -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Image {
        reference: IMAGE.to_owned(),
    })
    .working_directory("/workspace")
}

async fn serve_over_duplex(provider: Arc<dyn SandboxProvider>) -> PluginProvider {
    let (host_side, plugin_side) = duplex(1024 * 1024);
    let (host_read, host_write) = split(host_side);
    let (plugin_read, plugin_write) = split(plugin_side);
    tokio::spawn(serve(provider, plugin_read, plugin_write));
    PluginProvider::connect(host_read, host_write)
        .await
        .expect("handshake")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: needs a Docker daemon with buildpack-deps:noble and takes about a minute"]
async fn agent_tool_call_latency_through_the_driver() {
    let image_check = Command::new("docker")
        .args(["image", "inspect", IMAGE])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    if !image_check.is_ok_and(|status| status.success()) {
        eprintln!("no Docker daemon or {IMAGE} is not present locally; skipping");
        return;
    }
    let repo = Repository::pack();
    let mut rows = Vec::new();

    // -- Host, in-process: fabro local sandbox vs driver HostProvider.
    let host_dir = tempfile::tempdir().expect("tempdir");
    let local = local_sandbox(host_dir.path().to_path_buf())
        .await
        .expect("local sandbox should be created");
    local.initialize().await.expect("local init");
    unpack_fabro(&local, &repo).await;
    rows.extend(bench_fabro("fabro local sandbox", &local, &repo).await);

    let host_provider = Arc::new(HostProvider::new());
    let host = host_provider
        .create(
            &SandboxSpec::new(SandboxSource::HostDirectory)
                .working_directory(host_dir.path().to_string_lossy().into_owned()),
            None,
        )
        .await
        .expect("host create");
    rows.extend(bench_driver("driver Host (in-process)", host.as_ref(), &repo).await);

    // -- Host over the wire (duplex pipe, no process boundary).
    let remote_host = serve_over_duplex(host_provider.clone()).await;
    let wire_host = remote_host.attach(host.id(), None).await.expect("attach");
    rows.extend(bench_driver("driver Host (JSON-RPC, duplex)", wire_host.as_ref(), &repo).await);
    drop(wire_host);
    remote_host.shutdown().await.expect("shutdown");
    host.delete().await.expect("host delete");

    // -- Docker, in-process: fabro's driver-backed sandbox vs the bare driver.
    let fabro_docker = provider_sandbox(
        SandboxProviderKind::DOCKER,
        &ProviderAccess::default(),
        SandboxSpec::new(SandboxSource::Image {
            reference: IMAGE.to_owned(),
        }),
        &CloneRequest::none(),
        None,
        None,
    )
    .await
    .expect("fabro docker sandbox");
    fabro_docker.initialize().await.expect("fabro docker init");
    unpack_fabro(&fabro_docker, &repo).await;
    rows.extend(bench_fabro("fabro Docker (driver-backed)", &fabro_docker, &repo).await);
    fabro_docker.delete().await.expect("fabro docker cleanup");

    let docker_provider = Arc::new(DockerProvider::connect().await.expect("docker connect"));
    let container = docker_provider
        .create(&docker_spec(), None)
        .await
        .expect("driver docker create");
    unpack_driver(container.as_ref(), &repo).await;
    rows.extend(bench_driver("driver Docker (in-process)", container.as_ref(), &repo).await);

    // -- Docker over the wire (duplex pipe, no process boundary).
    let remote_docker = serve_over_duplex(docker_provider.clone()).await;
    let wire_docker = remote_docker
        .attach(container.id(), None)
        .await
        .expect("attach");
    rows.extend(
        bench_driver(
            "driver Docker (JSON-RPC, duplex)",
            wire_docker.as_ref(),
            &repo,
        )
        .await,
    );
    drop(wire_docker);
    remote_docker.shutdown().await.expect("shutdown");
    container.delete().await.expect("driver docker delete");

    report(&rows);
}
