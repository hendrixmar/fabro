#![expect(
    clippy::disallowed_methods,
    reason = "These git integration tests intentionally exercise the real git CLI to validate repository helper behavior."
)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use fabro_graphviz::graph::{AttrValue, Edge, Graph, Node};
use fabro_sandbox::RunSandbox;
use fabro_types::{RunEvent, WorkflowSettings, fixtures};
use fabro_workflow::event::Emitter;
use fabro_workflow::git;
use fabro_workflow::handler::HandlerRegistry;
use fabro_workflow::handler::command::CommandHandler;
use fabro_workflow::handler::exit::ExitHandler;
use fabro_workflow::handler::start::StartHandler;
use fabro_workflow::outcome::StageOutcome;
use fabro_workflow::run_options::{GitCheckpointOptions, RunOptions};
use fabro_workflow::test_support::{run_graph, run_graph_with_env};
use sandbox_driver::{
    Capabilities, DirEntry, Exec, FileMetadata, Filesystem, PlatformInfo, SandboxId, SandboxStatus,
};
use tokio_util::sync::CancellationToken;

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).expect("failed to create repo dir");
    let init = Command::new("git")
        .args(["init"])
        .current_dir(dir)
        .output()
        .expect("git init should run");
    assert_success(&init, "git init");
    let commit = Command::new("git")
        .args([
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@test",
            "commit",
            "--allow-empty",
            "-m",
            "init",
        ])
        .current_dir(dir)
        .output()
        .expect("git commit --allow-empty should run");
    assert_success(&commit, "git commit --allow-empty");
}

fn init_bare_remote(dir: &Path) {
    std::fs::create_dir_all(
        dir.parent()
            .expect("bare remote path should have a parent directory"),
    )
    .expect("failed to create bare remote parent dir");
    let init = Command::new("git")
        .args(["init", "--bare"])
        .arg(dir)
        .output()
        .expect("git init --bare should run");
    assert_success(&init, "git init --bare");
}

fn add_origin(repo_dir: &Path, remote_dir: &Path) {
    let output = Command::new("git")
        .args(["remote", "add", "origin"])
        .arg(remote_dir)
        .current_dir(repo_dir)
        .output()
        .expect("git remote add origin should run");
    assert_success(&output, "git remote add origin");
}

fn rename_branch(repo_dir: &Path, branch: &str) {
    let output = Command::new("git")
        .args(["branch", "-M", branch])
        .current_dir(repo_dir)
        .output()
        .expect("git branch -M should run");
    assert_success(&output, "git branch -M");
}

fn empty_commit(repo_dir: &Path, message: &str) {
    let output = Command::new("git")
        .args([
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@test",
            "commit",
            "--allow-empty",
            "-m",
            message,
        ])
        .current_dir(repo_dir)
        .output()
        .expect("git commit --allow-empty should run");
    assert_success(&output, "git commit --allow-empty");
}

fn list_branch(repo_dir: &Path, branch: &str) -> String {
    let output = Command::new("git")
        .args(["branch", "--list", branch])
        .current_dir(repo_dir)
        .output()
        .expect("git branch --list should run");
    assert_success(&output, "git branch --list");
    String::from_utf8(output.stdout).expect("git branch --list output should be UTF-8")
}

async fn local_env(repo: &Path) -> Arc<RunSandbox> {
    Arc::new(
        fabro_sandbox::local_sandbox(repo.to_path_buf())
            .await
            .expect("local sandbox should be created"),
    )
}

fn simple_graph() -> Graph {
    let mut g = Graph::new("git_checkpoint");
    g.attrs.insert(
        "goal".to_string(),
        AttrValue::String("Create git checkpoints".to_string()),
    );

    let mut start = Node::new("start");
    start.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Mdiamond".to_string()),
    );
    g.nodes.insert("start".to_string(), start);

    let mut exit = Node::new("exit");
    exit.attrs.insert(
        "shape".to_string(),
        AttrValue::String("Msquare".to_string()),
    );
    g.nodes.insert("exit".to_string(), exit);

    g
}

fn make_registry() -> HandlerRegistry {
    let mut registry = HandlerRegistry::new(Box::new(StartHandler));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));
    registry
}

fn test_run_options(run_dir: &Path) -> RunOptions {
    RunOptions {
        run_dir:          run_dir.to_path_buf(),
        cancel_token:     CancellationToken::new(),
        run_id:           fixtures::RUN_2,
        settings:         WorkflowSettings::default(),
        git:              None,
        pre_run_git:      None,
        fork_source_ref:  None,
        labels:           HashMap::new(),
        github_app:       None,
        base_branch:      None,
        display_base_sha: None,
        git_identity:     None,
        workflow_slug:    None,
    }
}

#[test]
fn push_ref_to_bare_remote() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let remote_dir = dir.path().join("remote.git");

    init_bare_remote(&remote_dir);
    init_repo(&repo_dir);
    add_origin(&repo_dir, &remote_dir);

    rename_branch(&repo_dir, "test-push");
    let url = format!("file://{}", remote_dir.display());
    git::push_ref(&repo_dir, &url, "refs/heads/test-push").unwrap();

    assert!(list_branch(&remote_dir, "test-push").contains("test-push"));
}

#[test]
fn push_branch_to_remote() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let remote_dir = dir.path().join("remote.git");

    init_bare_remote(&remote_dir);
    init_repo(&repo_dir);
    add_origin(&repo_dir, &remote_dir);
    rename_branch(&repo_dir, "main");

    git::push_branch(&repo_dir, "origin", "main").unwrap();

    assert!(list_branch(&remote_dir, "main").contains("main"));
}

#[test]
fn branch_needs_push_when_ahead() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let remote_dir = dir.path().join("remote.git");

    init_bare_remote(&remote_dir);
    init_repo(&repo_dir);
    add_origin(&repo_dir, &remote_dir);
    rename_branch(&repo_dir, "main");

    git::push_branch(&repo_dir, "origin", "main").unwrap();
    empty_commit(&repo_dir, "second");

    assert!(git::branch_needs_push(&repo_dir, "origin", "main"));
}

#[test]
fn branch_needs_push_when_in_sync() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let remote_dir = dir.path().join("remote.git");

    init_bare_remote(&remote_dir);
    init_repo(&repo_dir);
    add_origin(&repo_dir, &remote_dir);
    rename_branch(&repo_dir, "main");

    git::push_branch(&repo_dir, "origin", "main").unwrap();

    assert!(!git::branch_needs_push(&repo_dir, "origin", "main"));
}

#[test]
fn remote_branch_sha_ignores_a_locally_rewritten_tracking_ref() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    let remote_dir = dir.path().join("remote.git");

    init_bare_remote(&remote_dir);
    init_repo(&repo_dir);
    add_origin(&repo_dir, &remote_dir);
    rename_branch(&repo_dir, "main");
    git::push_branch(&repo_dir, "origin", "main").unwrap();
    let remote_sha = git::head_sha(&repo_dir).unwrap();

    empty_commit(&repo_dir, "local-only");
    let local_sha = git::head_sha(&repo_dir).unwrap();
    let update_tracking = Command::new("git")
        .args(["update-ref", "refs/remotes/origin/main", "HEAD"])
        .current_dir(&repo_dir)
        .output()
        .expect("git update-ref should run");
    assert_success(&update_tracking, "git update-ref");
    assert!(!git::branch_needs_push(&repo_dir, "origin", "main"));

    assert_eq!(
        git::remote_branch_sha_noninteractive(&repo_dir, "origin", "main").unwrap(),
        Some(remote_sha.clone()),
    );
    assert_ne!(local_sha, remote_sha);
}

#[tokio::test]
async fn git_checkpoint_skips_start_node() {
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    init_repo(repo);

    let base_sha = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();

    let run_tmp = tempfile::tempdir().unwrap();
    let mut g = simple_graph();
    g.nodes.insert("work".to_string(), Node::new("work"));
    g.edges.clear();
    g.edges.push(Edge::new("start", "work"));
    g.edges.push(Edge::new("work", "exit"));

    let events = Arc::new(std::sync::Mutex::new(Vec::<RunEvent>::new()));
    let events_clone = Arc::clone(&events);
    let emitter = Emitter::new(fixtures::RUN_2);
    emitter.on_event(move |event| {
        events_clone.lock().unwrap().push(event.clone());
    });

    let mut run_options = test_run_options(run_tmp.path());
    run_options.git = Some(GitCheckpointOptions {
        base_sha:    Some(base_sha),
        run_branch:  None,
        meta_branch: Some(format!("fabro/meta/{}", fixtures::RUN_2)),
    });

    Box::pin(run_graph(
        make_registry(),
        Arc::new(emitter),
        local_env(repo).await,
        &g,
        &run_options,
    ))
    .await
    .unwrap();

    let collected = events.lock().unwrap();
    let checkpoint_node_ids: Vec<&str> = collected
        .iter()
        .filter(|event| {
            event.event_name() == "checkpoint.completed"
                && event.properties().is_ok_and(|properties| {
                    properties
                        .get("git_commit_sha")
                        .and_then(|value| value.as_str())
                        .is_some()
                })
        })
        .filter_map(|event| event.node_id.as_deref())
        .collect();
    assert!(!checkpoint_node_ids.contains(&"start"));
    assert!(checkpoint_node_ids.contains(&"work"));
}

/// Sandbox double for remote-style runs: commands and files operate on a real
/// local checkout, but the workflow engine's run directory is reported as
/// inaccessible (as it is for Docker/Daytona) and the sandbox exposes a
/// runtime directory outside the checkout.
struct RemoteStyleSandbox {
    inner:             Arc<dyn sandbox_driver::Sandbox>,
    fs:                HidingFs,
    runtime_directory: String,
}

impl RemoteStyleSandbox {
    fn over(
        inner: Arc<dyn sandbox_driver::Sandbox>,
        hidden_path: String,
        runtime_directory: String,
    ) -> Self {
        Self {
            fs: HidingFs {
                inner: Arc::clone(&inner),
                hidden_path,
            },
            inner,
            runtime_directory,
        }
    }
}

#[async_trait::async_trait]
impl sandbox_driver::Sandbox for RemoteStyleSandbox {
    fn id(&self) -> &SandboxId {
        self.inner.id()
    }

    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    async fn describe(&self) -> sandbox_driver::Result<SandboxStatus> {
        self.inner.describe().await
    }

    fn working_directory(&self) -> &str {
        self.inner.working_directory()
    }

    async fn environment(&self) -> sandbox_driver::Result<BTreeMap<String, String>> {
        self.inner.environment().await
    }

    fn runtime_directory(&self) -> Option<&str> {
        Some(&self.runtime_directory)
    }

    async fn platform_info(&self) -> sandbox_driver::Result<PlatformInfo> {
        self.inner.platform_info().await
    }

    async fn start(&self) -> sandbox_driver::Result<()> {
        self.inner.start().await
    }

    async fn stop(&self) -> sandbox_driver::Result<()> {
        self.inner.stop().await
    }

    async fn delete(&self) -> sandbox_driver::Result<()> {
        self.inner.delete().await
    }

    fn exec(&self) -> &dyn Exec {
        self.inner.exec()
    }

    fn fs(&self) -> &dyn Filesystem {
        &self.fs
    }
}

/// The real filesystem with one path reported absent.
struct HidingFs {
    inner:       Arc<dyn sandbox_driver::Sandbox>,
    hidden_path: String,
}

#[async_trait::async_trait]
impl Filesystem for HidingFs {
    async fn read(&self, path: &str) -> sandbox_driver::Result<Vec<u8>> {
        self.inner.fs().read(path).await
    }

    async fn write(&self, path: &str, content: &[u8]) -> sandbox_driver::Result<()> {
        self.inner.fs().write(path, content).await
    }

    async fn delete(&self, path: &str, recursive: bool) -> sandbox_driver::Result<()> {
        self.inner.fs().delete(path, recursive).await
    }

    async fn exists(&self, path: &str) -> sandbox_driver::Result<bool> {
        if path == self.hidden_path {
            return Ok(false);
        }
        self.inner.fs().exists(path).await
    }

    async fn metadata(&self, path: &str) -> sandbox_driver::Result<FileMetadata> {
        self.inner.fs().metadata(path).await
    }

    async fn list_dir(&self, path: &str, depth: usize) -> sandbox_driver::Result<Vec<DirEntry>> {
        self.inner.fs().list_dir(path, depth).await
    }

    async fn create_dir(&self, path: &str) -> sandbox_driver::Result<()> {
        self.inner.fs().create_dir(path).await
    }

    async fn rename(&self, from: &str, to: &str) -> sandbox_driver::Result<()> {
        self.inner.fs().rename(from, to).await
    }
}

fn git_status_porcelain(repo_dir: &Path) -> String {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(repo_dir)
        .output()
        .expect("git status --porcelain should run");
    assert_success(&output, "git status --porcelain");
    String::from_utf8(output.stdout).expect("git status output should be UTF-8")
}

fn git_committed_files(repo_dir: &Path, sha: &str) -> String {
    let output = Command::new("git")
        .args(["show", "--name-only", "--format=", sha])
        .current_dir(repo_dir)
        .output()
        .expect("git show --name-only should run");
    assert_success(&output, "git show --name-only");
    String::from_utf8(output.stdout).expect("git show output should be UTF-8")
}

/// Remote-style prompt demotion must materialize blobs in the sandbox runtime
/// directory, outside the checkout, so a real checkpoint commit can never pick
/// them up, and re-resolution must recreate a deleted materialized file from
/// the durable blob store. Regression test for issue #798.
#[tokio::test]
async fn remote_prompt_demotion_stays_outside_checkout_and_survives_checkpoint() {
    use std::time::Duration;

    use fabro_store::test_support as store_test_support;
    use fabro_types::settings::run::RunCheckpointSettings;
    use fabro_workflow::context::Context;
    use fabro_workflow::git::GitAuthor;
    use fabro_workflow::runtime_store::RunStoreHandle;
    use fabro_workflow::{artifact, sandbox_git};
    use object_store::memory::InMemory;

    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    init_repo(&repo_dir);
    let runtime_dir = dir.path().join("fabro").join("runtime");
    let run_dir = dir.path().join("run");
    std::fs::create_dir_all(&run_dir).unwrap();

    let local = fabro_sandbox::local_sandbox(repo_dir.clone())
        .await
        .expect("local sandbox should be created");
    let sandbox = RunSandbox::new(
        fabro_sandbox::SandboxProviderKind::LOCAL,
        Arc::new(RemoteStyleSandbox::over(
            Arc::clone(local.handle().expect("local sandbox is initialized")),
            run_dir.to_string_lossy().to_string(),
            runtime_dir.to_string_lossy().to_string(),
        )),
    );

    let store = store_test_support::test_database(
        Arc::new(InMemory::new()),
        "runs/",
        Duration::from_millis(1),
        None,
    );
    let run_store: RunStoreHandle = store.create_run(&fixtures::RUN_2).await.unwrap().into();

    let oversized = serde_json::json!("x".repeat(64 * 1024));
    let oversized_bytes = serde_json::to_vec(&oversized).unwrap();
    let mut values = HashMap::from([("dataset".to_string(), oversized.clone())]);
    artifact::demote_large_values_for_prompt(
        &mut values,
        &mut HashMap::new(),
        &run_store,
        &sandbox,
        &run_dir,
    )
    .await;

    let marker = values["dataset"]
        .get("fabroLargeValue")
        .expect("oversized value should demote to a marker");
    let blob_path = marker["path"].as_str().unwrap().to_string();
    assert!(
        blob_path.starts_with(&runtime_dir.to_string_lossy().to_string()),
        "materialized blob {blob_path} should live under the sandbox runtime directory"
    );
    assert!(
        !blob_path.starts_with(&repo_dir.to_string_lossy().to_string()),
        "materialized blob {blob_path} must not live inside the checkout"
    );

    // The agent-facing path is readable through the sandbox.
    let contents = sandbox.read_file_bytes(&blob_path).await.unwrap();
    assert_eq!(contents, oversized_bytes);

    // Materialization leaves the checkout clean, and a real checkpoint commit
    // stages no runtime blob file.
    assert_eq!(git_status_porcelain(&repo_dir), "");
    let sha = sandbox_git::git_checkpoint(
        &sandbox,
        &fixtures::RUN_2.to_string(),
        "work",
        "succeeded",
        1,
        None,
        &RunCheckpointSettings::default(),
        &GitAuthor::default(),
    )
    .await
    .expect("checkpoint commit should succeed");
    assert_eq!(git_committed_files(&repo_dir, &sha).trim(), "");
    assert_eq!(git_status_porcelain(&repo_dir), "");

    // Removing the materialized file and resolving the value again recreates
    // it from the durable blob store.
    std::fs::remove_file(&blob_path).unwrap();
    let blob_hash = fabro_types::BlobHash::new(&oversized_bytes);
    let context = Context::new();
    context.set(
        "report",
        serde_json::json!(fabro_types::format_blob_ref(&blob_hash)),
    );
    let resolved = artifact::resolved_context_snapshot(&context, &run_store, &sandbox, &run_dir)
        .await
        .unwrap();
    assert_eq!(
        resolved["report"],
        serde_json::json!(format!("file://{blob_path}"))
    );
    assert_eq!(
        sandbox.read_file_bytes(&blob_path).await.unwrap(),
        oversized_bytes
    );
}

// ---------------------------------------------------------------------------
// One Git identity per run: engine checkpoints and workflow commands agree.
// ---------------------------------------------------------------------------

fn git_stdout(repo_dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .unwrap_or_else(|err| panic!("git {args:?} should run: {err}"));
    assert_success(&output, &format!("git {args:?}"));
    String::from_utf8(output.stdout)
        .expect("git output should be UTF-8")
        .trim()
        .to_string()
}

/// `author name`, `author email`, `committer name`, `committer email`.
fn commit_identity(repo_dir: &Path, rev: &str) -> Vec<String> {
    git_stdout(repo_dir, &[
        "show",
        "-s",
        "--format=%an%n%ae%n%cn%n%ce",
        rev,
    ])
    .lines()
    .map(str::to_string)
    .collect()
}

fn set_local_identity(repo_dir: &Path, name: &str, email: &str) {
    for (key, value) in [("user.name", name), ("user.email", email)] {
        let output = Command::new("git")
            .args(["config", key, value])
            .current_dir(repo_dir)
            .output()
            .expect("git config should run");
        assert_success(&output, "git config");
    }
}

fn command_node(id: &str, script: &str) -> Node {
    let mut node = Node::new(id);
    node.attrs.insert(
        "shape".to_string(),
        AttrValue::String("parallelogram".to_string()),
    );
    node.attrs
        .insert("script".to_string(), AttrValue::String(script.to_string()));
    node
}

fn identity_registry() -> HandlerRegistry {
    let mut registry = make_registry();
    registry.register("command", Box::new(CommandHandler));
    registry
}

/// The identity a workflow command sees is the run's, not the checkout's
/// local config, not an inherited `GIT_*` variable, and not a
/// `[run.environment]` entry. It reaches the primary checkout, a clone the
/// workflow creates, and a repository the workflow initializes, and the
/// engine's own checkpoint commit carries the same identity.
#[tokio::test]
async fn run_identity_governs_engine_and_workflow_commits_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let repo_dir = dir.path().join("repo");
    init_repo(&repo_dir);
    set_local_identity(&repo_dir, "Local Config", "local@example.com");
    let base_sha = git_stdout(&repo_dir, &["rev-parse", "HEAD"]);

    let identity = fabro_types::GitIdentity {
        name:   "fabro-sh[bot]".to_string(),
        email:  "281434857+fabro-sh[bot]@users.noreply.github.com".to_string(),
        source: fabro_types::GitIdentitySource::GithubApp,
    };
    let expected = vec![
        identity.name.clone(),
        identity.email.clone(),
        identity.name.clone(),
        identity.email.clone(),
    ];

    let clone_dir = dir.path().join("clone");
    let fresh_dir = dir.path().join("fresh");
    let script = format!(
        "set -e
        printf work > work.txt && git add work.txt && git commit -q -m 'workflow commit'
        git clone -q . {clone} && (cd {clone} && printf x > x.txt && git add x.txt && git commit -q -m 'clone commit')
        git init -q {fresh} && (cd {fresh} && printf y > y.txt && git add y.txt && git commit -q -m 'fresh commit')",
        clone = clone_dir.display(),
        fresh = fresh_dir.display(),
    );

    let mut graph = simple_graph();
    graph
        .nodes
        .insert("work".to_string(), command_node("work", &script));
    graph.edges.clear();
    graph.edges.push(Edge::new("start", "work"));
    graph.edges.push(Edge::new("work", "exit"));

    let run_tmp = tempfile::tempdir().unwrap();
    let mut run_options = test_run_options(run_tmp.path());
    run_options.git_identity = Some(identity.clone());
    run_options.git = Some(GitCheckpointOptions {
        base_sha:    Some(base_sha),
        run_branch:  None,
        meta_branch: None,
    });

    // A `[run.environment]` entry and an inherited host variable both name a
    // different author; the run identity must win over both.
    let env = HashMap::from([
        ("GIT_AUTHOR_NAME".to_string(), "Run Env".to_string()),
        (
            "GIT_COMMITTER_EMAIL".to_string(),
            "run-env@example.com".to_string(),
        ),
    ]);
    let outcome = run_graph_with_env(
        identity_registry(),
        Arc::new(Emitter::new(fixtures::RUN_2)),
        local_env(&repo_dir).await,
        &graph,
        &run_options,
        env,
    )
    .await
    .expect("workflow should complete");
    assert_eq!(outcome.status, StageOutcome::Succeeded, "{outcome:?}");

    // The workflow's own commit in the primary checkout.
    assert_eq!(
        commit_identity(&repo_dir, "HEAD~1"),
        expected,
        "workflow commit in the primary checkout"
    );
    assert_eq!(
        git_stdout(&repo_dir, &["log", "-1", "--format=%s", "HEAD~1"]),
        "workflow commit"
    );
    // The engine's checkpoint commit on top of it.
    assert_eq!(
        commit_identity(&repo_dir, "HEAD"),
        expected,
        "engine checkpoint commit"
    );
    assert!(
        git_stdout(&repo_dir, &["log", "-1", "--format=%s", "HEAD"]).starts_with("fabro("),
        "HEAD should be the checkpoint commit"
    );
    // A clone the workflow created and a repository it initialized.
    assert_eq!(
        commit_identity(&clone_dir, "HEAD"),
        expected,
        "clone commit"
    );
    assert_eq!(
        commit_identity(&fresh_dir, "HEAD"),
        expected,
        "fresh repo commit"
    );

    // The checkout's own configuration is left alone.
    assert_eq!(
        git_stdout(&repo_dir, &["config", "user.name"]),
        "Local Config"
    );
    assert_eq!(
        git_stdout(&repo_dir, &["config", "user.email"]),
        "local@example.com"
    );
}

/// Two runs with different identities in the same process do not leak into
/// each other: each run's commits carry only its own identity.
#[tokio::test]
async fn concurrent_runs_keep_their_own_identities() {
    async fn run_with(name: &str, email: &str) -> (tempfile::TempDir, Vec<String>) {
        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        init_repo(&repo_dir);
        let mut graph = simple_graph();
        graph.nodes.insert(
            "work".to_string(),
            command_node(
                "work",
                "for i in 1 2 3; do printf $i > f$i.txt; git add f$i.txt; git commit -q -m c$i; \
                 sleep 0.05; done",
            ),
        );
        graph.edges.clear();
        graph.edges.push(Edge::new("start", "work"));
        graph.edges.push(Edge::new("work", "exit"));
        let run_tmp = tempfile::tempdir().unwrap();
        let mut run_options = test_run_options(run_tmp.path());
        run_options.run_id = fabro_types::RunId::new();
        run_options.git_identity = Some(fabro_types::GitIdentity {
            name:   name.to_string(),
            email:  email.to_string(),
            source: fabro_types::GitIdentitySource::Explicit,
        });
        run_graph(
            identity_registry(),
            Arc::new(Emitter::new(run_options.run_id)),
            local_env(&repo_dir).await,
            &graph,
            &run_options,
        )
        .await
        .expect("workflow should complete");
        let identities = git_stdout(&repo_dir, &["log", "--format=%an <%ae> %cn <%ce>", "-3"])
            .lines()
            .map(str::to_string)
            .collect();
        (dir, identities)
    }

    let (first, second) = tokio::join!(
        run_with("Run One", "one@example.com"),
        run_with("Run Two", "two@example.com"),
    );
    assert_eq!(first.1, vec![
        "Run One <one@example.com> Run One <one@example.com>";
        3
    ]);
    assert_eq!(second.1, vec![
        "Run Two <two@example.com> Run Two <two@example.com>";
        3
    ]);
}
