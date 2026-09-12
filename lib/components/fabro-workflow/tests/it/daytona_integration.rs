//! Integration tests for the driver-backed Daytona sandbox.
//!
//! These tests require a `DAYTONA_API_KEY` environment variable and network
//! access. Run with: `cargo test --package arc-workflows -- --ignored daytona`

#![allow(
    clippy::absolute_paths,
    clippy::format_push_string,
    clippy::ignore_without_reason,
    clippy::items_after_statements,
    clippy::print_stderr,
    reason = "These Daytona integration tests value explicit scenarios over pedantic style lints."
)]
#![expect(
    clippy::disallowed_methods,
    reason = "These Daytona integration tests use real process env and git CLI fixtures for workflow runs."
)]

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

use fabro_graphviz::graph::{AttrValue, Edge, Graph, Node};
use fabro_sandbox::{
    CloneRequest, DaytonaCredentials, ProviderAccess, RunSandbox, SandboxProviderKind,
    provider_sandbox,
};
use fabro_static::EnvVars;
use fabro_store::{ArtifactKey, ArtifactStore};
use fabro_types::{RunId, StageId, WorkflowSettings, parse_blob_ref};
use fabro_workflow::artifact;
use fabro_workflow::context::Context;
use fabro_workflow::error::Error;
use fabro_workflow::event::Emitter;
use fabro_workflow::handler::exit::ExitHandler;
use fabro_workflow::handler::start::StartHandler;
use fabro_workflow::handler::{Handler, HandlerRegistry};
use fabro_workflow::outcome::{Outcome, StageOutcome};
use fabro_workflow::records::Checkpoint;
use fabro_workflow::run_options::{GitCheckpointOptions, RunOptions};
use fabro_workflow::runtime_store::RunStoreHandle;
use fabro_workflow::test_support::{WorkflowRunner, test_store_dir};
use object_store::local::LocalFileSystem;
use sandbox_driver::{LifecycleTimers, Resources, SandboxSource, SandboxSpec};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

fn test_run_id(label: &str) -> RunId {
    let mut hasher = DefaultHasher::new();
    label.hash(&mut hasher);
    RunId::from(Ulid(u128::from(hasher.finish())))
}

#[expect(
    clippy::disallowed_methods,
    reason = "This helper spins up a dedicated current-thread runtime when called from inside an existing Tokio runtime."
)]
fn load_run_checkpoint(run_dir: &Path) -> Result<Checkpoint, Box<dyn std::error::Error>> {
    let run_dir = run_dir.to_path_buf();
    let uses_shared_store = run_dir
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "scratch");
    let store_dir = if uses_shared_store {
        let runs_dir = run_dir.parent().ok_or("run dir should have parent")?;
        let storage_dir = runs_dir.parent().ok_or("runs dir should have parent")?;
        storage_dir.join("store")
    } else {
        test_store_dir(&run_dir)
    };
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(&store_dir)?);
    let store = Arc::new(fabro_store::test_support::test_database_at(
        object_store,
        "",
        std::time::Duration::from_millis(1),
        None,
        &store_dir,
    ));
    let state = if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(
            move || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let run_id = if uses_shared_store {
                    run_dir
                        .file_name()
                        .ok_or("run dir should have file name")?
                        .to_string_lossy()
                        .rsplit('-')
                        .next()
                        .ok_or("run dir should contain run id suffix")?
                        .parse()?
                } else {
                    runtime
                        .block_on(store.run_summary_store().list_all(chrono::Utc::now()))?
                        .into_iter()
                        .next()
                        .ok_or("test store should contain one run")?
                        .id
                };
                let run = runtime.block_on(store.open_run_reader(&run_id))?;
                let state = runtime.block_on(async {
                    for attempt in 0..20 {
                        let state = run.state().await?;
                        if state.current_checkpoint().is_some() || attempt == 19 {
                            return Ok::<_, fabro_store::Error>(state);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    unreachable!()
                })?;
                Ok(state)
            },
        )
        .join()
        .map_err(|_| "checkpoint loader thread panicked")?
        .map_err(|err| err.to_string())?
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let run_id = if uses_shared_store {
            run_dir
                .file_name()
                .ok_or("run dir should have file name")?
                .to_string_lossy()
                .rsplit('-')
                .next()
                .ok_or("run dir should contain run id suffix")?
                .parse()?
        } else {
            runtime
                .block_on(store.run_summary_store().list_all(chrono::Utc::now()))?
                .into_iter()
                .next()
                .ok_or("test store should contain one run")?
                .id
        };
        let run = runtime.block_on(store.open_run_reader(&run_id))?;
        runtime.block_on(async {
            for attempt in 0..20 {
                let state = run.state().await?;
                if state.current_checkpoint().is_some() || attempt == 19 {
                    return Ok::<_, fabro_store::Error>(state);
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            unreachable!()
        })?
    };
    state
        .current_checkpoint()
        .cloned()
        .ok_or_else(|| "checkpoint should exist in run store".into())
}

async fn resolve_checkpoint_text(
    run_dir: &Path,
    run_id: &RunId,
    value: &serde_json::Value,
) -> Result<String, Box<dyn std::error::Error>> {
    let Some(current) = value.as_str() else {
        return Ok(value.to_string());
    };
    if parse_blob_ref(current).is_none() {
        return Ok(current.to_string());
    }

    let store_dir = test_store_dir(run_dir);
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(&store_dir)?);
    let store = fabro_store::test_support::test_database_at(
        object_store,
        "",
        std::time::Duration::from_millis(1),
        None,
        &store_dir,
    );
    let run = store.open_run_reader(run_id).await?;
    let run_store = RunStoreHandle::from(run);
    Ok(artifact::resolve_text_or_blob_ref_str(current, &run_store).await?)
}

/// Live credentials from the process environment, the way the vault would
/// supply them in production.
fn daytona_access(credentials: DaytonaCredentials) -> ProviderAccess {
    ProviderAccess {
        daytona: Some(credentials),
        ..ProviderAccess::default()
    }
}

fn live_daytona_credentials() -> DaytonaCredentials {
    let api_key = std::env::var(EnvVars::DAYTONA_API_KEY).expect("DAYTONA_API_KEY must be set");
    DaytonaCredentials::from_api_key(api_key, |name| std::env::var(name).ok())
}

async fn create_env() -> RunSandbox {
    let creds = load_github_app_credentials();
    create_env_with_github_app(Some(creds)).await
}

fn test_artifact_store(run_dir: &Path) -> ArtifactStore {
    let object_store = Arc::new(
        LocalFileSystem::new_with_prefix(test_store_dir(run_dir))
            .expect("failed to create local artifact store"),
    );
    ArtifactStore::new(object_store, "artifacts")
}

async fn create_env_with_github_app(
    github_app: Option<fabro_github::GitHubCredentials>,
) -> RunSandbox {
    provider_sandbox(
        SandboxProviderKind::DAYTONA,
        &daytona_access(live_daytona_credentials()),
        SandboxSpec::new(SandboxSource::HostDirectory),
        &CloneRequest::default(),
        github_app.as_ref(),
        None,
    )
    .await
    .expect("Failed to create Daytona client — is DAYTONA_API_KEY set?")
}

fn load_github_app_credentials() -> fabro_github::GitHubCredentials {
    // Read app_id from ~/.fabro/settings.toml
    let home = dirs::home_dir().expect("No home directory");
    let config_path = home.join(".fabro/settings.toml");
    let config_str = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", config_path.display()));

    #[derive(serde::Deserialize)]
    struct Config {
        #[serde(default)]
        git: GitSection,
    }
    #[derive(serde::Deserialize, Default)]
    struct GitSection {
        app_id: Option<String>,
    }

    let config: Config = toml::from_str(&config_str).expect("Failed to parse settings.toml");
    let app_id = config
        .git
        .app_id
        .expect("app_id not set in settings.toml [git] section");

    let raw =
        std::env::var(EnvVars::GITHUB_APP_PRIVATE_KEY).expect("GITHUB_APP_PRIVATE_KEY not set");
    let private_key_pem = if raw.starts_with("-----") {
        raw
    } else {
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &raw)
            .expect("GITHUB_APP_PRIVATE_KEY is not valid base64");
        String::from_utf8(bytes).expect("GITHUB_APP_PRIVATE_KEY decoded to invalid UTF-8")
    };
    fabro_github::GitHubCredentials::App(fabro_github::GitHubAppCredentials {
        app_id,
        private_key_pem,
        slug: None,
    })
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_exec_command() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;
    env.initialize().await.unwrap();

    let result = env
        .exec_command("echo hello", 30_000, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(result.stdout_lossy().contains("hello"));

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_exec_command_with_pipe() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;
    env.initialize().await.unwrap();

    let result = env
        .exec_command("echo hello world | wc -w", 30_000, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(result.stdout_lossy().trim().contains('2'));

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_exec_command_cancelled() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;
    env.initialize().await.unwrap();

    let token = CancellationToken::new();
    let token_clone = token.clone();

    // Cancel the token shortly after starting
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        token_clone.cancel();
    });

    // Execute a command that would normally take a while
    let result = env
        .exec_command("sleep 10", 30_000, None, None, Some(token))
        .await
        .unwrap();

    assert_eq!(result.exit_code, None);
    assert!(matches!(
        result.termination,
        fabro_sandbox::Termination::Cancelled | fabro_sandbox::Termination::Killed
    ));
    assert_eq!(result.stderr_lossy(), "Command cancelled");

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_exec_command_local_timeout() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;
    env.initialize().await.unwrap();

    // Use a tiny timeout_ms of 100ms, our local timeout is 100 + 2000 = 2100ms.
    // If the server doesn't enforce the timeout properly or drops the connection,
    // our local timeout should catch it. To simulate this without making a bad
    // server, we can't easily force the local timeout to hit before the server
    // timeout without mocking. But if we run `sleep 10` and Daytona does NOT
    // respect the short timeout parameter, the local 2.1s timeout will
    // definitely fire. Let's at least test that a 100ms timeout works and
    // doesn't run for 10s.
    let start = std::time::Instant::now();
    let result = env
        .exec_command("sleep 10", 100, None, None, None)
        .await
        .unwrap();

    let duration = start.elapsed();

    assert!(
        duration < std::time::Duration::from_secs(3),
        "Command stalled for longer than the local timeout mechanism"
    );
    assert_eq!(result.exit_code, None);
    assert_eq!(result.termination, fabro_sandbox::Termination::TimedOut);
    assert_eq!(result.stderr_lossy(), "Command timed out locally");

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_file_round_trip() {
    let env = create_env().await;
    env.initialize().await.unwrap();

    let test_path = "test_round_trip.txt";
    let content = "Hello from Daytona integration test!";

    // Write
    env.write_file(test_path, content).await.unwrap();

    // Exists
    assert!(env.file_exists(test_path).await.unwrap());

    // Read
    let read_back = env.read_file_text(test_path).await.unwrap();
    assert!(read_back.contains(content));

    // Delete
    env.delete_file(test_path).await.unwrap();
    assert!(!env.file_exists(test_path).await.unwrap());

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_full_lifecycle() {
    let env = create_env().await;

    // Initialize (creates sandbox + clones repo)
    env.initialize().await.unwrap();

    // Verify platform
    assert_eq!(env.platform(), "linux");

    // Verify working directory is accessible
    let result = env
        .exec_command("pwd", 10_000, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));

    // List directory
    let entries = env.list_directory(".", None).await.unwrap();
    assert!(!entries.is_empty());

    // Cleanup (deletes sandbox)
    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_snapshot_sandbox() {
    let mut resources = Resources::default();
    resources.cpu_cores = Some(2);
    resources.memory_mb = Some(4096);
    resources.disk_mb = Some(10_240);
    let mut timers = LifecycleTimers::default();
    timers.auto_stop_after_idle = Some(std::time::Duration::from_hours(1));
    let spec = SandboxSpec::new(SandboxSource::Dockerfile {
        content: "FROM ubuntu:22.04\nRUN apt-get update && apt-get install -y ripgrep".to_string(),
    })
    .resources(resources)
    .timers(timers);

    let creds = load_github_app_credentials();
    let env = provider_sandbox(
        SandboxProviderKind::DAYTONA,
        &daytona_access(live_daytona_credentials()),
        spec,
        &CloneRequest::default(),
        Some(&creds),
        None,
    )
    .await
    .expect("Failed to create Daytona client — is DAYTONA_API_KEY set?");
    env.initialize().await.unwrap();

    // Verify rg is available (installed by snapshot)
    let result = env
        .exec_command("rg --version", 10_000, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(result.stdout_lossy().contains("ripgrep"));

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_artifact_sync_uploads_and_rewrites_pointer() {
    let env = create_env().await;
    env.initialize().await.unwrap();

    // Create a local artifact file (simulating what offload_large_values produces)
    let dir = tempfile::tempdir().unwrap();
    let artifact_content = "x".repeat(150 * 1024); // 150KB
    let artifact_json = serde_json::json!(artifact_content);
    let artifact_file = dir.path().join("response.plan.json");
    std::fs::write(
        &artifact_file,
        serde_json::to_string(&artifact_json).unwrap(),
    )
    .unwrap();

    // Build updates with a file:// pointer (as offload_large_values would)
    let pointer = format!("file://{}", artifact_file.display());
    let mut updates = HashMap::new();
    updates.insert("response.plan".to_string(), serde_json::json!(pointer));

    // Sync — the local file doesn't exist in the Daytona sandbox, so it should
    // upload
    artifact::sync_artifacts_to_env(&mut updates, &env)
        .await
        .unwrap();

    // Pointer should be rewritten to the Daytona working directory
    let new_pointer = updates["response.plan"].as_str().unwrap();
    let expected_prefix = format!("file://{}/.fabro/artifacts/", env.working_directory());
    assert!(
        new_pointer.starts_with(&expected_prefix),
        "pointer should reference Daytona path, got: {new_pointer}"
    );

    // Verify the file actually exists in the sandbox by reading it back
    let remote_path = new_pointer.strip_prefix("file://").unwrap();
    assert!(
        env.file_exists(remote_path).await.unwrap(),
        "artifact file should exist in Daytona sandbox at {remote_path}"
    );

    let remote_content = env.read_file_text(remote_path).await.unwrap();
    assert!(
        remote_content.len() > 100 * 1024,
        "remote artifact should be >100KB, got {} bytes",
        remote_content.len()
    );

    env.delete().await.unwrap();
}

// ---------------------------------------------------------------------------
// Full pipeline E2E on Daytona
// ---------------------------------------------------------------------------

/// Handler that produces a >100KB context_update to trigger artifact
/// offloading.
struct LargeOutputHandler;

#[async_trait::async_trait]
impl Handler for LargeOutputHandler {
    async fn execute(
        &self,
        node: &Node,
        _context: &Context,
        _graph: &Graph,
        _run_dir: &Path,
        _services: &fabro_workflow::handler::EngineServices,
    ) -> Result<Outcome, Error> {
        let mut outcome = Outcome::success();
        let large_value = "x".repeat(150 * 1024);
        outcome.context_updates.insert(
            format!("response.{}", node.id),
            serde_json::json!(large_value),
        );
        Ok(outcome)
    }
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_pipeline_artifact_offload_and_sync() {
    let env = create_env().await;
    env.initialize().await.unwrap();
    let env: Arc<RunSandbox> = Arc::new(env);

    // Pipeline: start -> big_output -> exit
    let mut graph = Graph::new("DaytonaArtifactPipeline");
    graph.attrs.insert(
        "goal".to_string(),
        AttrValue::String("Test artifact offload+sync on Daytona".to_string()),
    );

    let mut start = Node::new("start");
    start.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Mdiamond".to_string()),
    );
    graph.nodes.insert("start".to_string(), start);

    let mut exit = Node::new("exit");
    exit.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Msquare".to_string()),
    );
    graph.nodes.insert("exit".to_string(), exit);

    let mut big_output = Node::new("big_output");
    big_output.attrs.insert(
        "label".to_string(),
        AttrValue::String("Big Output".to_string()),
    );
    graph.nodes.insert("big_output".to_string(), big_output);

    graph.edges.push(Edge::new("start", "big_output"));
    graph.edges.push(Edge::new("big_output", "exit"));

    let dir = tempfile::tempdir().unwrap();
    let mut registry = HandlerRegistry::new(Box::new(LargeOutputHandler));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));

    let engine = WorkflowRunner::new(registry, Arc::new(Emitter::default()), env.clone());
    let run_options = RunOptions {
        settings:         WorkflowSettings::default(),
        run_dir:          dir.path().to_path_buf(),
        cancel_token:     CancellationToken::new(),
        run_id:           test_run_id("test-run"),
        labels:           std::collections::HashMap::new(),
        workflow_slug:    None,
        github_app:       None,
        base_branch:      None,
        display_base_sha: None,
        git_identity:     None,
        pre_run_git:      None,
        fork_source_ref:  None,
        git:              None,
    };
    let outcome = engine
        .run(&graph, &run_options)
        .await
        .expect("pipeline should succeed");
    assert_eq!(outcome.status, StageOutcome::Succeeded);

    // Checkpoint should persist a durable blob ref.
    let checkpoint = load_run_checkpoint(dir.path()).expect("checkpoint should load");
    let pointer_value = checkpoint
        .context_values
        .get("response.big_output")
        .expect("context should have response.big_output");
    let pointer_str = pointer_value.as_str().expect("pointer should be a string");
    assert!(
        parse_blob_ref(pointer_str).is_some(),
        "checkpoint should persist a blob ref"
    );
    let resolved = resolve_checkpoint_text(dir.path(), &run_options.run_id, pointer_value)
        .await
        .expect("offloaded value should resolve through the run store");
    assert_eq!(
        resolved,
        "x".repeat(150 * 1024),
        "offloaded value should round-trip through the run store"
    );

    env.delete().await.unwrap();
}

// ---------------------------------------------------------------------------
// CLI Backend on Daytona — real CLI tools via exec_command
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Git checkpoint E2E on Daytona
// ---------------------------------------------------------------------------

/// Handler that writes a file via exec_command so git has something to commit.
struct FileWriterHandler;

#[async_trait::async_trait]
impl Handler for FileWriterHandler {
    async fn execute(
        &self,
        node: &Node,
        _context: &Context,
        _graph: &Graph,
        _run_dir: &Path,
        services: &fabro_workflow::handler::EngineServices,
    ) -> Result<Outcome, Error> {
        let content = format!("output from {}", node.id);
        let cmd = format!("echo '{content}' > {}.txt", node.id);
        let _ = services
            .run
            .sandbox
            .exec_command(&cmd, 10_000, None, None, None)
            .await;
        Ok(Outcome::success())
    }
}

/// Set up git inside a Daytona sandbox for checkpoint commits.
/// Returns (run_id, base_sha, branch_name) on success.
async fn setup_daytona_git(sandbox: &RunSandbox) -> (RunId, String, String) {
    // Get current HEAD as base SHA
    let sha_result = sandbox
        .exec_command("git rev-parse HEAD", 10_000, None, None, None)
        .await
        .expect("git rev-parse HEAD should succeed");
    assert_eq!(
        sha_result.exit_code,
        Some(0),
        "git rev-parse HEAD failed: {}",
        sha_result.stderr_lossy()
    );
    let base_sha = sha_result.stdout_lossy().trim().to_string();

    let run_id = RunId::from(Ulid::new());
    let branch_name = format!("fabro/run/{run_id}");

    let checkout_cmd = format!("git checkout -b {branch_name}");
    let checkout_result = sandbox
        .exec_command(&checkout_cmd, 10_000, None, None, None)
        .await
        .expect("git checkout should succeed");
    assert_eq!(
        checkout_result.exit_code,
        Some(0),
        "git checkout -b failed (exit {:?}): stdout={} stderr={}",
        checkout_result.exit_code,
        checkout_result.stdout_lossy(),
        checkout_result.stderr_lossy()
    );

    (run_id, base_sha, branch_name)
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_git_checkpoint_remote_emits_events() {
    let env = create_env().await;
    env.initialize().await.unwrap();
    let env: Arc<RunSandbox> = Arc::new(env);

    // Install git if not available (the default ubuntu:22.04 image may not have it)
    let git_check = env
        .exec_command("git --version", 10_000, None, None, None)
        .await;
    if git_check.as_ref().map_or(true, |r| !r.success()) {
        let install = env
            .exec_command(
                "apt-get update -qq && apt-get install -y -qq git >/dev/null 2>&1",
                120_000,
                None,
                None,
                None,
            )
            .await
            .expect("apt-get install git should not error");
        assert_eq!(
            install.exit_code,
            Some(0),
            "git install failed: {}",
            install.stderr_lossy()
        );
    }

    // Set up git in the sandbox
    let (_run_id, base_sha, branch_name) = setup_daytona_git(&env).await;

    // Pipeline: start -> work -> exit
    let mut graph = Graph::new("DaytonaGitCheckpoint");
    graph.attrs.insert(
        "goal".to_string(),
        AttrValue::String("Test Remote git checkpoint".to_string()),
    );

    let mut start = Node::new("start");
    start.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Mdiamond".to_string()),
    );
    graph.nodes.insert("start".to_string(), start);

    let mut exit = Node::new("exit");
    exit.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Msquare".to_string()),
    );
    graph.nodes.insert("exit".to_string(), exit);

    let mut work = Node::new("work");
    work.attrs
        .insert("label".to_string(), AttrValue::String("Work".to_string()));
    graph.nodes.insert("work".to_string(), work);

    graph.edges.push(Edge::new("start", "work"));
    graph.edges.push(Edge::new("work", "exit"));

    // Set up event collection
    let dir = tempfile::tempdir().unwrap();
    let emitter = Emitter::default();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    {
        let events_clone = Arc::clone(&events);
        emitter.on_event(move |event| {
            events_clone.lock().unwrap().push(event.clone());
        });
    }

    let mut registry = HandlerRegistry::new(Box::new(FileWriterHandler));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));

    let engine = WorkflowRunner::new(registry, Arc::new(emitter), env.clone());
    let run_options = RunOptions {
        settings:         WorkflowSettings::default(),
        run_dir:          dir.path().to_path_buf(),
        cancel_token:     CancellationToken::new(),
        run_id:           test_run_id("git-cp-test"),
        labels:           std::collections::HashMap::new(),
        workflow_slug:    None,
        github_app:       None,
        base_branch:      None,
        display_base_sha: None,
        git_identity:     None,
        pre_run_git:      None,
        fork_source_ref:  None,
        git:              Some(GitCheckpointOptions {
            base_sha:    Some(base_sha),
            run_branch:  Some(branch_name),
            meta_branch: None,
        }),
    };
    let outcome = engine
        .run(&graph, &run_options)
        .await
        .expect("pipeline should succeed");
    assert_eq!(outcome.status, StageOutcome::Succeeded);

    // Assert CheckpointCompleted events with git SHAs were emitted
    {
        let events = events.lock().unwrap();
        let git_events: Vec<_> = events
            .iter()
            .filter_map(|e| {
                if e.event_name() != "checkpoint.completed" {
                    return None;
                }
                let properties = e.properties().ok()?;
                Some((
                    e.node_id.clone()?,
                    properties.get("git_commit_sha")?.as_str()?.to_string(),
                ))
            })
            .collect();
        // Only the "work" node gets a checkpoint — start is skipped and exit breaks
        // before the checkpoint code runs.
        assert_eq!(
            git_events.len(),
            1,
            "expected 1 CheckpointCompleted event with SHA (work node only), got {}",
            git_events.len()
        );
        assert!(
            git_events
                .iter()
                .all(|(_, sha)| sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit())),
            "all SHAs should be 40-char hex, got: {git_events:?}"
        );
    }

    // Verify the persisted checkpoint snapshot has git_commit_sha
    let checkpoint = load_run_checkpoint(dir.path()).expect("checkpoint should load");
    assert!(
        checkpoint.git_commit_sha.is_some(),
        "checkpoint should have git_commit_sha"
    );

    env.delete().await.unwrap();
}

// ---------------------------------------------------------------------------
// Daytona shadow commit E2E with sandbox-native metadata
// ---------------------------------------------------------------------------

/// End-to-end test: pipeline with git checkpointing enabled + `meta_branch`
/// writes shadow branch in the sandbox repo and includes `Fabro-Checkpoint`
/// trailer in sandbox commits.
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_git_checkpoint_with_shadow_branch() {
    let env = create_env().await;
    env.initialize().await.unwrap();
    let env: Arc<RunSandbox> = Arc::new(env);

    // Install git if not available
    let git_check = env
        .exec_command("git --version", 10_000, None, None, None)
        .await;
    if git_check.as_ref().map_or(true, |r| !r.success()) {
        let install = env
            .exec_command(
                "apt-get update -qq && apt-get install -y -qq git >/dev/null 2>&1",
                120_000,
                None,
                None,
                None,
            )
            .await
            .expect("apt-get install git should not error");
        assert_eq!(
            install.exit_code,
            Some(0),
            "git install failed: {}",
            install.stderr_lossy()
        );
    }

    // Set up git in the sandbox
    let (run_id, base_sha, branch_name) = setup_daytona_git(&env).await;

    // Pipeline: start -> work -> exit
    let mut graph = Graph::new("DaytonaShadowBranch");
    graph.attrs.insert(
        "goal".to_string(),
        AttrValue::String("Test Daytona shadow branch".to_string()),
    );

    let mut start = Node::new("start");
    start.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Mdiamond".to_string()),
    );
    graph.nodes.insert("start".to_string(), start);

    let mut exit = Node::new("exit");
    exit.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Msquare".to_string()),
    );
    graph.nodes.insert("exit".to_string(), exit);

    let mut work = Node::new("work");
    work.attrs
        .insert("label".to_string(), AttrValue::String("Work".to_string()));
    graph.nodes.insert("work".to_string(), work);

    graph.edges.push(Edge::new("start", "work"));
    graph.edges.push(Edge::new("work", "exit"));

    let dir = tempfile::tempdir().unwrap();
    // Write graph.fabro so init_run can read it
    std::fs::write(dir.path().join("graph.fabro"), "digraph {}").unwrap();

    let mut registry = HandlerRegistry::new(Box::new(FileWriterHandler));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));

    let meta_branch = format!("fabro/meta/{run_id}");
    let engine = WorkflowRunner::new(registry, Arc::new(Emitter::default()), env.clone());
    let run_options = RunOptions {
        settings: WorkflowSettings::default(),
        run_dir: dir.path().to_path_buf(),
        cancel_token: CancellationToken::new(),
        run_id,
        labels: std::collections::HashMap::new(),
        workflow_slug: None,
        github_app: None,
        base_branch: None,
        display_base_sha: None,
        git_identity: None,
        pre_run_git: None,
        fork_source_ref: None,
        git: Some(GitCheckpointOptions {
            base_sha:    Some(base_sha),
            run_branch:  Some(branch_name),
            meta_branch: Some(meta_branch.clone()),
        }),
    };
    let outcome = engine
        .run(&graph, &run_options)
        .await
        .expect("pipeline should succeed");
    assert_eq!(outcome.status, StageOutcome::Succeeded);

    // Assert shadow branch in the sandbox has checkpoint data
    let run_json = env
        .exec_command(
            &format!("git show refs/heads/{meta_branch}:run.json"),
            10_000,
            None,
            None,
            None,
        )
        .await
        .expect("git show should succeed");
    assert_eq!(run_json.exit_code, Some(0), "{}", run_json.stderr_lossy());
    let projection: fabro_store::RunProjection =
        serde_json::from_slice(run_json.stdout_lossy().as_bytes()).expect("run.json should parse");
    let checkpoint = projection
        .current_checkpoint()
        .cloned()
        .expect("shadow branch should contain checkpoint data");
    assert!(
        !checkpoint.completed_nodes.is_empty(),
        "checkpoint should have completed nodes"
    );
    assert!(
        checkpoint.completed_nodes.contains(&"work".to_string()),
        "checkpoint should contain the 'work' node"
    );

    // Assert sandbox commit has Fabro-Checkpoint trailer
    let log_result = env
        .exec_command("git log --format=%B -1", 10_000, None, None, None)
        .await
        .expect("git log should succeed");
    assert_eq!(log_result.exit_code, Some(0));
    let commit_msg = log_result.stdout_lossy().trim().to_string();
    assert!(
        commit_msg.contains("Fabro-Checkpoint:"),
        "sandbox commit should have Fabro-Checkpoint trailer, got:\n{commit_msg}"
    );
    assert!(
        commit_msg.contains("Fabro-Run:"),
        "sandbox commit should have Fabro-Run trailer, got:\n{commit_msg}"
    );

    env.delete().await.unwrap();
}

// ---------------------------------------------------------------------------
// Artifact collection e2e — Daytona sandbox
// ---------------------------------------------------------------------------

/// Handler that creates artifact files via exec_command on the sandbox.
struct AssetCreatorHandler;

#[async_trait::async_trait]
impl Handler for AssetCreatorHandler {
    async fn execute(
        &self,
        _node: &Node,
        _context: &Context,
        _graph: &Graph,
        _run_dir: &Path,
        services: &fabro_workflow::handler::EngineServices,
    ) -> Result<Outcome, Error> {
        let script = concat!(
            "mkdir -p test-results && ",
            "echo '<testsuites><testsuite name=\"example\"/></testsuites>' > test-results/report.xml && ",
            "echo 'test output' > test-results/output.txt"
        );
        services
            .run
            .sandbox
            .exec_command(script, 30_000, None, None, None)
            .await
            .map_err(|e| Error::handler(format!("exec failed: {e}")))?;
        Ok(Outcome::success())
    }
}

/// Daytona sandbox: artifact collection discovers files on the remote sandbox
/// and downloads them to the local logs directory.
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_asset_collection() {
    let env = create_env().await;
    env.initialize().await.unwrap();
    let env: Arc<RunSandbox> = Arc::new(env);

    let dir = tempfile::tempdir().unwrap();

    let mut registry = HandlerRegistry::new(Box::new(AssetCreatorHandler));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));

    let engine = WorkflowRunner::new(registry, Arc::new(Emitter::default()), env.clone());

    let mut graph = Graph::new("DaytonaAssetTest");
    graph.attrs.insert(
        "goal".to_string(),
        AttrValue::String("Test artifact collection on Daytona".to_string()),
    );

    let mut start = Node::new("start");
    start.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Mdiamond".to_string()),
    );
    graph.nodes.insert("start".to_string(), start);

    let mut create_assets = Node::new("create_assets");
    create_assets.attrs.insert(
        "label".to_string(),
        AttrValue::String("Create Assets".to_string()),
    );
    graph
        .nodes
        .insert("create_assets".to_string(), create_assets);

    let mut exit = Node::new("exit");
    exit.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Msquare".to_string()),
    );
    graph.nodes.insert("exit".to_string(), exit);

    graph.edges.push(Edge::new("start", "create_assets"));
    graph.edges.push(Edge::new("create_assets", "exit"));

    let run_options = RunOptions {
        settings:         WorkflowSettings {
            run: fabro_types::settings::RunNamespace {
                artifacts: fabro_types::settings::run::ArtifactsSettings {
                    include: vec!["test-results/**".to_string()],
                },
                ..fabro_types::settings::RunNamespace::default()
            },
            ..WorkflowSettings::default()
        },
        run_dir:          dir.path().to_path_buf(),
        cancel_token:     CancellationToken::new(),
        run_id:           test_run_id("artifact-test-daytona"),
        labels:           std::collections::HashMap::new(),
        workflow_slug:    None,
        github_app:       None,
        base_branch:      None,
        display_base_sha: None,
        git_identity:     None,
        pre_run_git:      None,
        fork_source_ref:  None,
        git:              None,
    };
    let outcome = engine
        .run(&graph, &run_options)
        .await
        .expect("pipeline should succeed");
    assert_eq!(outcome.status, StageOutcome::Succeeded);

    let content = String::from_utf8(
        test_artifact_store(dir.path())
            .get(
                &run_options.run_id,
                &ArtifactKey::new(
                    StageId::new("create_assets", 1),
                    1,
                    "test-results/report.xml",
                ),
            )
            .await
            .unwrap()
            .expect("artifact should be stored from Daytona sandbox")
            .to_vec(),
    )
    .unwrap();
    assert!(content.contains("testsuites"));
    assert!(
        !dir.path().join("cache").join("artifacts").exists(),
        "artifact scratch cache should not be created"
    );

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_ssh_access() {
    let env = create_env().await;
    env.initialize().await.unwrap();

    let ssh_command = env
        .ssh_access_command()
        .await
        .unwrap()
        .expect("Daytona should offer an SSH command");
    assert!(!ssh_command.is_empty(), "ssh_command should not be empty");
    assert!(
        ssh_command.contains("ssh"),
        "ssh_command should contain 'ssh': {ssh_command}",
    );

    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_ssh_access_before_init_fails() {
    let env = create_env().await;

    let result = env.ssh_access_command().await;
    assert!(result.is_err(), "should fail before initialize()");
    assert!(
        result.unwrap_err().to_string().contains("not initialized"),
        "error should mention not initialized"
    );
}

// ---------------------------------------------------------------------------
// GitHub App Installation Access Token (IAT) clone tests
// ---------------------------------------------------------------------------

/// E2E: Clone the current (private) repo using GitHub App IAT credentials.
/// Verifies the full flow: JWT signing, installation lookup, token creation,
/// clone.
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_clone_private_repo_with_github_app_iat() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;

    // initialize() clones the current repo — with IAT credentials this should
    // succeed
    env.initialize().await.unwrap();

    // Verify the clone worked: CLAUDE.md should exist in the workspace
    let result = env
        .exec_command("test -f CLAUDE.md && echo EXISTS", 10_000, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        result.exit_code,
        Some(0),
        "CLAUDE.md should exist after clone"
    );
    assert!(
        result.stdout_lossy().contains("EXISTS"),
        "clone should have populated the workspace"
    );

    // Install git if not available (the default ubuntu:22.04 image may not have it)
    let git_check = env
        .exec_command("git --version", 10_000, None, None, None)
        .await;
    if git_check.as_ref().map_or(true, |r| !r.success()) {
        let install = env
            .exec_command(
                "apt-get update -qq && apt-get install -y -qq git >/dev/null 2>&1",
                120_000,
                None,
                None,
                None,
            )
            .await
            .expect("apt-get install git should not error");
        assert_eq!(
            install.exit_code,
            Some(0),
            "git install failed: {}",
            install.stderr_lossy()
        );
    }

    // Verify this is actually the fabro repo
    let result = env
        .exec_command("git remote get-url origin", 10_000, None, None, None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(
        result.stdout_lossy().contains("fabro-sh/fabro"),
        "origin should point to fabro-sh/fabro, got: {}",
        result.stdout_lossy().trim()
    );

    env.delete().await.unwrap();
}

/// E2E: Verify that repos in an installed org get credentials (needed for
/// pushing).
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_clone_public_repo_gets_credentials() {
    let creds = load_github_app_credentials();

    // Directly test resolve_clone_credentials against a repo in an org where the
    // app is installed
    let credentials = fabro_github::resolve_clone_credentials(
        &fabro_github::GitHubContext::new(&creds, &fabro_github::github_api_base_url()),
        "fabro-sh",
        "fabro",
    )
    .await
    .unwrap();

    assert_eq!(
        credentials.username(),
        "x-access-token",
        "installed org repo should get credentials for pushing"
    );
    assert!(
        !credentials.password().is_empty(),
        "installed org repo should get a token for pushing"
    );
}

/// E2E: Verify that requesting an IAT for a repo the app isn't installed on
/// gives a clear error message.
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_iat_not_installed_gives_clear_error() {
    let creds = load_github_app_credentials();

    let result = fabro_github::resolve_clone_credentials(
        &fabro_github::GitHubContext::new(&creds, &fabro_github::github_api_base_url()),
        "torvalds",
        "linux",
    )
    .await;

    assert!(
        result.is_err(),
        "should fail for repo the app isn't installed on"
    );
    let err = result.unwrap_err();
    let err = format!("{err:#}");
    assert!(
        err.contains("not installed"),
        "error should mention 'not installed', got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Push run branch to origin after each checkpoint (GitHub App)
// ---------------------------------------------------------------------------

/// E2E: After each remote checkpoint, the run branch is pushed to origin.
/// Verifies the branch appears on the remote via `git ls-remote`.
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_git_push_run_branch_to_origin() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;
    env.initialize().await.unwrap();
    let env: Arc<RunSandbox> = Arc::new(env);

    // Install git if not available
    let git_check = env
        .exec_command("git --version", 10_000, None, None, None)
        .await;
    if git_check.as_ref().map_or(true, |r| !r.success()) {
        let install = env
            .exec_command(
                "apt-get update -qq && apt-get install -y -qq git >/dev/null 2>&1",
                120_000,
                None,
                None,
                None,
            )
            .await
            .expect("apt-get install git should not error");
        assert_eq!(
            install.exit_code,
            Some(0),
            "git install failed: {}",
            install.stderr_lossy()
        );
    }

    // Set up git in the sandbox
    let (run_id, base_sha, branch_name) = setup_daytona_git(&env).await;

    // Pipeline: start -> work -> exit
    let mut graph = Graph::new("DaytonaGitPush");
    graph.attrs.insert(
        "goal".to_string(),
        AttrValue::String("Test push run branch to origin".to_string()),
    );

    let mut start = Node::new("start");
    start.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Mdiamond".to_string()),
    );
    graph.nodes.insert("start".to_string(), start);

    let mut exit = Node::new("exit");
    exit.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Msquare".to_string()),
    );
    graph.nodes.insert("exit".to_string(), exit);

    let mut work = Node::new("work");
    work.attrs
        .insert("label".to_string(), AttrValue::String("Work".to_string()));
    graph.nodes.insert("work".to_string(), work);

    graph.edges.push(Edge::new("start", "work"));
    graph.edges.push(Edge::new("work", "exit"));

    let dir = tempfile::tempdir().unwrap();

    let mut registry = HandlerRegistry::new(Box::new(FileWriterHandler));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));

    let engine = WorkflowRunner::new(registry, Arc::new(Emitter::default()), env.clone());
    let run_options = RunOptions {
        settings: WorkflowSettings::default(),
        run_dir: dir.path().to_path_buf(),
        cancel_token: CancellationToken::new(),
        run_id,
        labels: std::collections::HashMap::new(),
        workflow_slug: None,
        github_app: None,
        base_branch: None,
        display_base_sha: None,
        git_identity: None,
        pre_run_git: None,
        fork_source_ref: None,
        git: Some(GitCheckpointOptions {
            base_sha:    Some(base_sha),
            run_branch:  Some(branch_name.clone()),
            meta_branch: None,
        }),
    };
    let outcome = engine
        .run(&graph, &run_options)
        .await
        .expect("pipeline should succeed");
    assert_eq!(outcome.status, StageOutcome::Succeeded);

    // Verify the run branch was pushed to origin
    let ls_remote_cmd = format!("git ls-remote --heads origin {branch_name}");
    let ls_result = env
        .exec_command(&ls_remote_cmd, 30_000, None, None, None)
        .await
        .expect("git ls-remote should succeed");
    assert_eq!(
        ls_result.exit_code,
        Some(0),
        "git ls-remote failed: {}",
        ls_result.stdout_lossy()
    );
    assert!(
        ls_result.stdout_lossy().contains(&branch_name),
        "run branch should exist on origin after push, got: {}",
        ls_result.stdout_lossy().trim()
    );

    // Clean up the remote branch
    let delete_cmd = format!("git push origin --delete {branch_name}");
    let delete_result = env
        .exec_command(&delete_cmd, 30_000, None, None, None)
        .await;
    if let Ok(r) = &delete_result {
        if !r.success() {
            eprintln!(
                "Warning: failed to delete remote branch {branch_name}: {}",
                r.stdout_lossy()
            );
        }
    }

    env.delete().await.unwrap();
}

/// Diagnose toolbox proxy staleness after idle time.
///
/// Creates a sandbox, runs a command, sleeps for increasing durations, then
/// retries. If a call fails, makes raw HTTP requests to capture the actual
/// underlying error that the SDK normally swallows.
///
/// Run: cargo test -p arc-workflows -- --ignored
/// daytona_toolbox_idle_diagnostic --nocapture
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_toolbox_idle_diagnostic() {
    let creds = load_github_app_credentials();
    let env = create_env_with_github_app(Some(creds)).await;
    env.initialize().await.unwrap();

    // 1. Verify toolbox works immediately after init
    let result = env
        .exec_command("echo alive", 30_000, None, None, None)
        .await;
    eprintln!(
        "[t=0s] exec_command after init: {:?}",
        result.as_ref().map(|r| r.exit_code)
    );
    assert!(
        result.is_ok(),
        "exec_command should work immediately after init"
    );

    let sandbox_name = env.sandbox_info();
    eprintln!("[t=0s] sandbox: {sandbox_name}");

    // 2. Sleep for increasing durations and test
    for sleep_secs in [1, 2, 3] {
        eprintln!("\n--- sleeping {sleep_secs}s ---");
        tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;

        let result = env
            .exec_command("echo alive", 30_000, None, None, None)
            .await;

        match &result {
            Ok(r) => {
                eprintln!(
                    "[t=+{sleep_secs}s] OK exit_code={:?} stdout={}",
                    r.exit_code,
                    r.stdout_lossy().trim()
                );
            }
            Err(e) => {
                eprintln!("[t=+{sleep_secs}s] FAILED: {e}");

                // Diagnose with raw HTTP calls
                let api_key = std::env::var(EnvVars::DAYTONA_API_KEY).unwrap_or_default();
                let client = fabro_http::HttpClientBuilder::new()
                    .timeout(std::time::Duration::from_secs(15))
                    .build()
                    .unwrap();
                let api_url = std::env::var(EnvVars::DAYTONA_API_URL)
                    .or_else(|_| std::env::var(EnvVars::DAYTONA_SERVER_URL))
                    .unwrap_or_else(|_| "https://app.daytona.io/api".to_string());

                // Check sandbox state
                let state_resp = client
                    .get(format!("{api_url}/sandbox/{sandbox_name}"))
                    .bearer_auth(&api_key)
                    .send()
                    .await;
                match state_resp {
                    Ok(resp) => {
                        let body = resp.text().await.unwrap_or_default();
                        let state = serde_json::from_str::<serde_json::Value>(&body)
                            .ok()
                            .and_then(|v| v.get("state").cloned());
                        eprintln!("[diag] sandbox state: {state:?}");
                    }
                    Err(e) => {
                        eprintln!("[diag] sandbox API failed: {e}");
                    }
                }

                // Get toolbox proxy URL and try a direct call
                let proxy_resp = client
                    .get(format!(
                        "{api_url}/sandbox/{sandbox_name}/toolbox-proxy-url"
                    ))
                    .bearer_auth(&api_key)
                    .send()
                    .await;
                if let Ok(resp) = proxy_resp {
                    let body = resp.text().await.unwrap_or_default();
                    eprintln!(
                        "[diag] proxy URL response: {}",
                        &body[..body.len().min(200)]
                    );
                    if let Some(url) = serde_json::from_str::<serde_json::Value>(&body)
                        .ok()
                        .and_then(|v| v.get("url").and_then(|u| u.as_str()).map(String::from))
                    {
                        let toolbox_url = format!("{url}/{sandbox_name}/process/execute");
                        eprintln!("[diag] trying direct POST to {toolbox_url}");
                        let direct = client
                            .post(&toolbox_url)
                            .bearer_auth(&api_key)
                            .json(&serde_json::json!({"command": "echo diag", "timeout": 10}))
                            .send()
                            .await;
                        match direct {
                            Ok(resp) => {
                                let status = resp.status();
                                let body = resp.text().await.unwrap_or_default();
                                eprintln!(
                                    "[diag] direct call: {status} body={}",
                                    &body[..body.len().min(300)]
                                );
                            }
                            Err(e) => {
                                // Walk the FULL error source chain
                                let mut msg = format!("[diag] direct call FAILED: {e}");
                                let mut source: Option<&dyn std::error::Error> =
                                    std::error::Error::source(&e);
                                while let Some(cause) = source {
                                    msg.push_str(&format!("\n  caused by: {cause}"));
                                    source = cause.source();
                                }
                                eprintln!("{msg}");
                            }
                        }
                    }
                }

                panic!("exec_command failed after {sleep_secs}s idle: {e}");
            }
        }
    }

    eprintln!("\n=== PASS: all idle durations survived ===");
    env.delete().await.unwrap();
}

/// E2E test for `fabro cp` against a live Daytona sandbox.
///
/// Creates a sandbox, reconnects via `cp::reconnect`,
/// uploads a file, downloads it back, and verifies the round-trip.
#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"), live("GITHUB_APP_PRIVATE_KEY"))]
async fn daytona_cp_upload_download_round_trip() {
    use fabro_sandbox::reconnect::reconnect_for_run;
    use fabro_types::RunSandboxInstance;

    // 1. Create and initialize a real Daytona sandbox
    let env = create_env().await;
    env.initialize().await.unwrap();

    let sandbox_name = env.sandbox_info();
    assert!(
        !sandbox_name.is_empty(),
        "sandbox_info() should return the Daytona sandbox name"
    );

    // 2. Build initialized sandbox metadata (same as `fabro run` would persist)
    let record = RunSandboxInstance {
        provider: SandboxProviderKind::DAYTONA,
        image:    None,
        snapshot: None,
        runtime:  fabro_types::RunSandboxRuntime {
            id:                sandbox_name.clone(),
            working_directory: env.working_directory().to_string(),
            repo_cloned:       Some(false),
            clone_origin_url:  None,
            clone_branch:      None,
            workspace_root:    Some("/home/daytona/workspace".to_string()),
            repos_root:        Some("/home/daytona/repos".to_string()),
            primary_repo_path: None,
            primary_repo_link: None,
        },
    };

    // 3. Reconnect via the real cp::reconnect path
    let tmp = tempfile::tempdir().unwrap();
    let access = ProviderAccess {
        daytona: Some(live_daytona_credentials()),
        ..ProviderAccess::default()
    };
    let reconnected = reconnect_for_run(&record, &access, None, None)
        .await
        .expect("reconnect should succeed");

    // 4. Upload: write a local file, then upload it to the sandbox
    let upload_content = b"hello from fabro cp e2e test\n";
    let local_upload = tmp.path().join("upload.txt");
    std::fs::write(&local_upload, upload_content).unwrap();

    reconnected
        .upload_file_from_local(&local_upload, "cp_test_upload.txt")
        .await
        .expect("upload_file_from_local should succeed");

    // 5. Verify the file exists in the sandbox via the original connection
    assert!(
        env.file_exists("cp_test_upload.txt").await.unwrap(),
        "uploaded file should exist in the sandbox"
    );
    let remote_content = env.read_file_text("cp_test_upload.txt").await.unwrap();
    assert!(
        remote_content.contains("hello from fabro cp e2e test"),
        "expected uploaded content in sandbox, got: {remote_content}"
    );

    // 6. Download: retrieve the file back to local via the reconnected sandbox
    let local_download = tmp.path().join("download.txt");
    reconnected
        .download_file_to_local("cp_test_upload.txt", &local_download)
        .await
        .expect("download_file_to_local should succeed");

    let downloaded = std::fs::read(&local_download).unwrap();
    assert_eq!(downloaded, upload_content);

    // 7. Upload a binary file to test non-UTF-8 content
    let binary_content: Vec<u8> = (0..=255).collect();
    let local_binary = tmp.path().join("binary.bin");
    std::fs::write(&local_binary, &binary_content).unwrap();

    reconnected
        .upload_file_from_local(&local_binary, "cp_test_binary.bin")
        .await
        .expect("binary upload should succeed");

    let local_binary_dl = tmp.path().join("binary_dl.bin");
    reconnected
        .download_file_to_local("cp_test_binary.bin", &local_binary_dl)
        .await
        .expect("binary download should succeed");

    let downloaded_binary = std::fs::read(&local_binary_dl).unwrap();
    assert_eq!(
        downloaded_binary, binary_content,
        "binary round-trip should be exact"
    );

    // 9. Cleanup
    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"))]
async fn daytona_computer_use_browser_screenshot() {
    let env = provider_sandbox(
        SandboxProviderKind::DAYTONA,
        &daytona_access(live_daytona_credentials()),
        SandboxSpec::new(SandboxSource::HostDirectory),
        &CloneRequest::none(),
        None,
        None,
    )
    .await
    .expect("DAYTONA_API_KEY must be set");
    env.initialize().await.unwrap();

    // 1. Start the computer use desktop environment (Xvfb, xfce4, etc.) through the
    //    driver's VNC facet, which also signs a viewer URL.
    let vnc = env
        .handle()
        .expect("initialized sandbox has a handle")
        .vnc()
        .expect("Daytona exposes VNC");
    let connection = vnc.vnc_connection().await.expect("VNC connection failed");
    eprintln!("VNC viewer: {}", connection.url);
    assert!(connection.url.contains("vnc.html"));

    // 2. Find or install a browser
    let check = env
        .exec_command(
            "which chromium || which chromium-browser || which google-chrome || echo NONE",
            30_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    eprintln!("Browser check: {}", check.stdout_lossy().trim());

    if check.stdout_lossy().trim() == "NONE" {
        let install_result = env
            .exec_command(
                "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chromium 2>&1",
                180_000, None, None, None,
            )
            .await
            .unwrap();
        eprintln!(
            "Browser install exit_code={:?}, last_line={}",
            install_result.exit_code,
            install_result.stdout_lossy().lines().last().unwrap_or("")
        );
        assert_eq!(install_result.exit_code, Some(0), "Chromium install failed");
    }

    let browser_bin = env
        .exec_command(
            "which chromium || which chromium-browser || which google-chrome",
            10_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let browser = browser_bin.stdout_lossy().trim().to_string();
    eprintln!("Using browser: {browser}");

    // 3. Detect the DISPLAY that computer use started
    let display_check = env
        .exec_command(
            "ps aux | grep Xvfb | grep -v grep | head -1",
            10_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    eprintln!("Xvfb process: {}", display_check.stdout_lossy().trim());

    // 4. Launch browser with setsid to fully detach, and log stderr
    let launch_cmd = format!(
        "DISPLAY=:0 setsid {browser} --no-sandbox --disable-gpu \
         --window-size=1024,768 --window-position=0,0 \
         https://example.com > /tmp/chrome_stdout.log 2>/tmp/chrome_stderr.log &\n\
         sleep 2 && echo launched"
    );
    let launch_result = env
        .exec_command(&launch_cmd, 30_000, None, None, None)
        .await
        .unwrap();
    eprintln!("Browser launch exit_code={:?}", launch_result.exit_code);

    // 5. Wait for the page to load, then check if browser is running
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;

    let ps_check = env
        .exec_command(
            "ps aux | grep -i chrom | grep -v grep",
            10_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    eprintln!("Chrome processes:\n{}", ps_check.stdout_lossy());

    let stderr_check = env
        .exec_command(
            "cat /tmp/chrome_stderr.log 2>/dev/null | tail -20",
            10_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    eprintln!("Chrome stderr:\n{}", stderr_check.stdout_lossy());

    // 5. The desktop is serving: noVNC listens on its port.
    let listening = env
        .exec_command(
            "ss -ltn 2>/dev/null | grep -q ':6080 ' || (command -v curl >/dev/null && curl -sf -o /dev/null http://127.0.0.1:6080/)",
            10_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        listening.success(),
        "noVNC should be reachable inside the sandbox"
    );

    // 7. Cleanup
    env.delete().await.unwrap();
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"))]
async fn daytona_playwright_mcp_sandbox_transport() {
    // Create sandbox from daytona-medium (has Node.js + Chromium)
    let sandbox = provider_sandbox(
        SandboxProviderKind::DAYTONA,
        &daytona_access(live_daytona_credentials()),
        SandboxSpec::new(SandboxSource::HostDirectory),
        &CloneRequest::none(),
        None,
        None,
    )
    .await
    .expect("DAYTONA_API_KEY must be set");
    sandbox.initialize().await.unwrap();

    // 1. Install Playwright MCP server and its browser
    eprintln!("Installing @playwright/mcp and Chromium browser...");
    let install = sandbox
        .exec_command(
            "npm install -g @playwright/mcp@latest 2>&1 && npx playwright install --with-deps chromium 2>&1",
            300_000,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    eprintln!(
        "Install exit_code={:?}, last_lines:\n{}",
        install.exit_code,
        install
            .stdout_lossy()
            .lines()
            .rev()
            .take(5)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(install.exit_code, Some(0), "Playwright install failed");

    // 2. The Playwright MCP server as an agent stage gets it: launched in the
    //    sandbox by pebble and reached over SSE through Daytona's preview link,
    //    token header included. A scripted model drives the tools, so the test is
    //    about the sandbox transport and nothing else.
    let mcp_port = 3100u16;
    let server = fabro_mcp::pebble::pebble_server(&fabro_mcp::config::McpServerSettings {
        name:                 "playwright".into(),
        transport:            fabro_mcp::config::McpTransport::Sandbox {
            protocol: fabro_mcp::config::McpHttpProtocol::Sse,
            command:  vec![
                "npx".into(),
                "@playwright/mcp@latest".into(),
                "--port".into(),
                mcp_port.to_string(),
                "--headless".into(),
                "--browser".into(),
                "chromium".into(),
            ],
            port:     mcp_port,
            env:      std::collections::HashMap::new(),
        },
        current_dir:          None,
        clear_env:            false,
        startup_timeout_secs: 60,
        tool_timeout_secs:    120,
    });
    let (client, _provider) = pebble_coding_agent::test_support::client_from(
        pebble_coding_agent::test_support::ScriptedProvider::new(vec![
            pebble_coding_agent::test_support::ScriptedCall::response(
                pebble_coding_agent::test_support::tool_call_response(
                    "mcp__playwright__browser_install",
                    "install",
                    serde_json::json!({}),
                ),
            ),
            pebble_coding_agent::test_support::ScriptedCall::response(
                pebble_coding_agent::test_support::tool_call_response(
                    "mcp__playwright__browser_navigate",
                    "navigate",
                    serde_json::json!({"url": "https://example.com"}),
                ),
            ),
            pebble_coding_agent::test_support::ScriptedCall::response(
                pebble_coding_agent::test_support::tool_call_response(
                    "mcp__playwright__browser_snapshot",
                    "snapshot",
                    serde_json::json!({}),
                ),
            ),
            pebble_coding_agent::test_support::ScriptedCall::response(
                pebble_coding_agent::test_support::text_response("browsed"),
            ),
        ]),
    );
    let sandbox = Arc::new(sandbox);
    let routes = sandbox
        .port_routes()
        .expect("Daytona forwards ports through preview URLs");
    let mut agent = pebble_coding_agent::CodingAgent::builder(
        client,
        Arc::clone(&sandbox) as Arc<dyn pebble_coding_agent::environment::Environment>,
    )
    .model("test/model")
    .permission_level(pebble_coding_agent::events::PermissionLevel::Full)
    .mcp_servers([server])
    .port_routes(routes)
    .build()
    .await
    .expect("the agent builds with the sandbox-hosted server");

    // 3. The server started and its tools are registered.
    let statuses = agent.snapshot().mcp_servers().to_vec();
    assert_eq!(statuses.len(), 1, "{statuses:?}");
    assert_eq!(
        statuses[0].error, None,
        "the Playwright server should start: {statuses:?}"
    );
    eprintln!("Discovered {} MCP tools:", statuses[0].tools.len());
    for tool in &statuses[0].tools {
        eprintln!("  - {}", tool.name);
    }
    assert!(
        statuses[0]
            .tools
            .iter()
            .any(|tool| tool.name == "mcp__playwright__browser_navigate"),
        "Should have discovered Playwright tools"
    );

    // 4. Install the browser, navigate, and snapshot through the agent.
    let mut events = agent.subscribe();
    let report = agent.prompt("browse example.com").await;
    assert!(report.result.is_ok(), "{report:?}");
    let mut completions = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let pebble_coding_agent::events::CodingEvent::ToolCallCompleted {
            tool_name,
            output,
            is_error,
            ..
        } = event.event
        {
            completions.push((tool_name, output, is_error));
        }
    }
    let navigate = completions
        .iter()
        .find(|(name, _, _)| name == "mcp__playwright__browser_navigate")
        .expect("navigate ran");
    assert!(!navigate.2, "Navigate should succeed: {navigate:?}");
    let snapshot = completions
        .iter()
        .find(|(name, _, _)| name == "mcp__playwright__browser_snapshot")
        .expect("snapshot ran");
    assert!(!snapshot.2, "Snapshot should succeed: {snapshot:?}");
    assert!(
        snapshot.1.to_string().contains("Example Domain"),
        "Snapshot should contain 'Example Domain'"
    );
    agent
        .shutdown(pebble_coding_agent::ShutdownReason::Completed)
        .await
        .expect("the agent shuts down");

    // 8. Cleanup
    sandbox.delete().await.unwrap();
}
