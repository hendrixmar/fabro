#![expect(
    clippy::disallowed_methods,
    reason = "integration test initializes an isolated git repository with the system git binary"
)]

use std::path::Path;
use std::process::Command;

use fabro_acp::test_support::fake_acp_agent_script;
use fabro_test::{TestContext, test_context};
use fabro_types::{EventBody, GitIdentitySource};

use super::{find_run_dir, read_conclusion, run_events, run_state};

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|err| panic!("git {args:?} should run: {err}"));
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn init_repo_with_local_identity(dir: &Path) {
    git(dir, &["init", "-q"]);
    git(dir, &["config", "user.name", "Local Config"]);
    git(dir, &["config", "user.email", "local@example.com"]);
    std::fs::write(dir.join("README.md"), "seed\n").expect("seed file should write");
    git(dir, &["add", "README.md"]);
    git(dir, &["commit", "-q", "-m", "seed"]);
}

fn identity_of(dir: &Path, rev: &str) -> Vec<String> {
    git(dir, &["show", "-s", "--format=%an%n%ae%n%cn%n%ce", rev])
        .lines()
        .map(str::to_string)
        .collect()
}

fn setup(context: &mut TestContext) {
    context.write_home(
        ".fabro/settings.toml",
        "[server.auth]\nmethods = [\"dev-token\"]\n",
    );
    context.isolated_server();
}

/// A host run with no GitHub credential and no explicit author commits as
/// the generic Fabro identity: a commit a script stage creates in the
/// checkout, and a commit it creates in a repository it initializes itself. The
/// checkout's own Git configuration and the host's inherited `GIT_*` variables
/// do not leak in, and the run does not touch the isolated HOME's Git
/// configuration.
#[test]
fn script_stage_commits_carry_the_run_identity() {
    let mut context = test_context!();
    setup(&mut context);
    init_repo_with_local_identity(&context.temp_dir);
    let other_repo = context.temp_dir.join("other-repo");
    let home_gitconfig = context.home_dir.join(".gitconfig");
    let script = format!(
        "set -e; printf work > work.txt && git add work.txt && git commit -q -m 'workflow commit' && \
         git init -q {other} && cd {other} && printf x > x.txt && git add x.txt && git commit -q -m 'other commit'",
        other = other_repo.display()
    );
    context.write_temp(
        "identity.fabro",
        format!(
            r#"digraph Identity {{
  graph [goal="Commit as the run identity"]
  start [shape=Mdiamond]
  work [shape=parallelogram, script="{script}"]
  exit [shape=Msquare]
  start -> work -> exit
}}"#
        ),
    );

    context
        .run_cmd()
        .env("GIT_AUTHOR_NAME", "Inherited Host")
        .env("GIT_AUTHOR_EMAIL", "host@example.com")
        .env("GIT_COMMITTER_NAME", "Inherited Host")
        .env("GIT_COMMITTER_EMAIL", "host@example.com")
        .args(["--auto-approve", "--environment", "local"])
        .arg(context.temp_dir.join("identity.fabro"))
        .assert()
        .success();

    let run_dir = find_run_dir(&context);
    assert_eq!(read_conclusion(&run_dir)["status"], "succeeded");

    let expected = vec![
        "Fabro".to_string(),
        "noreply@fabro.sh".to_string(),
        "Fabro".to_string(),
        "noreply@fabro.sh".to_string(),
    ];
    // A host run without a clone has no managed run branch, so the checkout
    // HEAD is the workflow's own commit. The engine checkpoint path is
    // covered by the fabro-workflow git integration tests.
    assert_eq!(
        git(&context.temp_dir, &["log", "-1", "--format=%s"]),
        "workflow commit"
    );
    assert_eq!(
        identity_of(&context.temp_dir, "HEAD"),
        expected,
        "workflow commit in the checkout"
    );
    assert_eq!(
        identity_of(&other_repo, "HEAD"),
        expected,
        "commit in a workflow-created repository"
    );

    // No configuration was written: the checkout keeps its local identity and
    // the isolated HOME gained no Git configuration.
    assert_eq!(
        git(&context.temp_dir, &["config", "user.name"]),
        "Local Config"
    );
    assert_eq!(
        git(&context.temp_dir, &["config", "user.email"]),
        "local@example.com"
    );
    assert!(
        !home_gitconfig.exists(),
        "the run must not write {}",
        home_gitconfig.display()
    );

    // The resolved identity is recorded durably and in the event stream.
    let state = run_state(&run_dir);
    let identity = state
        .git_identity
        .expect("run state should record the identity");
    assert_eq!(identity.name, "Fabro");
    assert_eq!(identity.email, "noreply@fabro.sh");
    assert_eq!(identity.source, GitIdentitySource::Default);
    assert!(
        run_events(&run_dir)
            .iter()
            .any(|event| matches!(&event.event.body, EventBody::GitIdentityResolved(props) if props.identity == identity)),
        "git.identity.resolved should be emitted"
    );
}

/// `run.git.author` overrides the identity for every path, and a run whose
/// working directory has no Git origin still receives it.
#[test]
fn explicit_author_reaches_script_stages_without_a_git_origin() {
    let mut context = test_context!();
    setup(&mut context);
    let repo = context.temp_dir.join("fresh");
    let script = format!(
        "set -e; git init -q {repo} && cd {repo} && printf x > x.txt && git add x.txt && git commit -q -m 'fresh commit'",
        repo = repo.display()
    );
    context.write_temp(
        "explicit.fabro",
        format!(
            r#"digraph Explicit {{
  graph [goal="Commit as the explicit author"]
  start [shape=Mdiamond]
  work [shape=parallelogram, script="{script}"]
  exit [shape=Msquare]
  start -> work -> exit
}}"#
        ),
    );
    context.write_temp(
        "workflow.toml",
        r#"_version = 1

[workflow]
graph = "explicit.fabro"

[run]
goal = "Commit as the explicit author"

[run.git.author]
name = "Release Bot"
email = "release@example.com"

[run.environment.env]
GIT_AUTHOR_NAME = "Run Env"
"#,
    );

    context
        .run_cmd()
        .args(["--auto-approve", "--environment", "local"])
        .arg(context.temp_dir.join("workflow.toml"))
        .assert()
        .success();

    let run_dir = find_run_dir(&context);
    assert_eq!(read_conclusion(&run_dir)["status"], "succeeded");
    assert_eq!(identity_of(&repo, "HEAD"), vec![
        "Release Bot".to_string(),
        "release@example.com".to_string(),
        "Release Bot".to_string(),
        "release@example.com".to_string(),
    ]);
    let identity = run_state(&run_dir)
        .git_identity
        .expect("run state should record the identity");
    assert_eq!(identity.source, GitIdentitySource::Explicit);
}

/// An ACP agent process is launched with the run identity in its environment.
#[test]
fn acp_agent_launch_env_carries_the_run_identity() {
    let mut context = test_context!();
    setup(&mut context);
    context.write_temp("fake_acp_agent.py", fake_acp_agent_script());
    let fake_agent = context.temp_dir.join("fake_acp_agent.py");
    let env_record = context.temp_dir.join("acp-env.json");
    let config = serde_json::json!({
        "type": "stdio",
        "name": "fake",
        "command": "python3",
        "args": [fake_agent.to_string_lossy()],
        "env": [
            {"name": "ACP_MODE", "value": "write_file"},
            {"name": "ACP_ENV_RECORD", "value": env_record.to_string_lossy()},
            {
                "name": "ACP_ENV_RECORD_KEYS",
                "value": "GIT_AUTHOR_NAME,GIT_AUTHOR_EMAIL,GIT_COMMITTER_NAME,GIT_COMMITTER_EMAIL",
            },
        ],
    })
    .to_string();
    let acp_config = format!("{config:?}");
    context.write_temp(
        "acp_identity.fabro",
        format!(
            r#"digraph ACP {{
  graph [goal="Exercise ACP launch env"]
  start [shape=Mdiamond]
  work [type="agent", backend="acp", prompt="write hello.txt", acp.config={acp_config}]
  exit [shape=Msquare]
  start -> work -> exit
}}"#
        ),
    );
    git(&context.temp_dir, &["init", "-q"]);

    context
        .run_cmd()
        .env("GIT_AUTHOR_NAME", "Inherited Host")
        .args(["--auto-approve", "--environment", "local"])
        .arg(context.temp_dir.join("acp_identity.fabro"))
        .assert()
        .success();

    let run_dir = find_run_dir(&context);
    assert_eq!(read_conclusion(&run_dir)["status"], "succeeded");
    let recorded: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&env_record).expect("fake ACP agent should record its env"),
    )
    .expect("record should be JSON");
    assert_eq!(
        recorded,
        serde_json::json!({
            "GIT_AUTHOR_NAME": "Fabro",
            "GIT_AUTHOR_EMAIL": "noreply@fabro.sh",
            "GIT_COMMITTER_NAME": "Fabro",
            "GIT_COMMITTER_EMAIL": "noreply@fabro.sh",
        })
    );
}
