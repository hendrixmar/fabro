#![cfg(feature = "docker")]

use std::sync::Arc;

use bollard::Docker;
use fabro_sandbox::{
    CommandOutputCallback, DockerSandbox, DockerSandboxOptions, ExecStreamingRequest, Sandbox,
};
use tokio::sync::Mutex;

fn capture_bytes(chunks: Arc<Mutex<Vec<u8>>>) -> CommandOutputCallback {
    Arc::new(move |_stream, bytes| {
        let chunks = Arc::clone(&chunks);
        Box::pin(async move {
            chunks.lock().await.extend(bytes);
            Ok(())
        })
    })
}

#[tokio::test]
#[ignore = "requires real Docker container lifecycle; run explicitly when changing Docker exec integration"]
async fn streaming_timeout_terminates_docker_exec_before_returning() {
    let image = "buildpack-deps:noble";
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return;
    };
    if docker.inspect_image(image).await.is_err() {
        return;
    }

    let sandbox = DockerSandbox::new(
        DockerSandboxOptions {
            image: image.to_string(),
            auto_pull: false,
            skip_clone: true,
            ..DockerSandboxOptions::default()
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("docker sandbox should construct");
    sandbox
        .initialize()
        .await
        .expect("docker sandbox should initialize");

    let chunks = Arc::new(Mutex::new(Vec::new()));

    let marker = "fabro_streaming_timeout_sentinel";
    let command = format!("trap '' HUP TERM; echo start; sleep 5 # {marker}");
    let result = sandbox
        .exec_command_streaming(ExecStreamingRequest {
            timeout_ms: Some(200),
            output_callback: Some(capture_bytes(Arc::clone(&chunks))),
            ..ExecStreamingRequest::new(&command)
        })
        .await
        .expect("streaming command should return a timeout result");

    assert!(result.result.is_timed_out());
    assert!(
        String::from_utf8_lossy(&chunks.lock().await).contains("start"),
        "stream should include output emitted before timeout"
    );

    let probe = sandbox
        .exec_command(
            "marker='fabro_streaming_timeout_''sentinel'; \
             ps -eo pid,args | awk -v marker=\"$marker\" \
             'index($0, marker) && $0 !~ /awk/ && $0 !~ /ps -eo/ { print }'",
            1_000,
            None,
            None,
            None,
        )
        .await
        .expect("process probe should run");
    sandbox
        .cleanup()
        .await
        .expect("docker cleanup should succeed");

    assert!(
        !probe.stdout.contains(marker),
        "timed-out docker exec should be terminated before returning, found: {}",
        probe.stdout
    );
}

#[tokio::test]
#[ignore = "requires real Docker container lifecycle; run explicitly when changing Docker exec integration"]
async fn streaming_command_receives_exact_stdin_and_eof() {
    let image = "buildpack-deps:noble";
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return;
    };
    if docker.inspect_image(image).await.is_err() {
        return;
    }

    let sandbox = DockerSandbox::new(
        DockerSandboxOptions {
            image: image.to_string(),
            auto_pull: false,
            skip_clone: true,
            ..DockerSandboxOptions::default()
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("docker sandbox should construct");
    sandbox
        .initialize()
        .await
        .expect("docker sandbox should initialize");

    let stdin = b"first line\n$(touch /tmp/must-not-run)\nlast line".to_vec();
    let result = sandbox
        .exec_command_streaming(ExecStreamingRequest {
            timeout_ms: Some(10_000),
            stdin: Some(stdin.clone()),
            ..ExecStreamingRequest::new("cat")
        })
        .await
        .expect("streaming command should read stdin and finish at EOF");
    let injection_probe = sandbox
        .exec_command("test ! -e /tmp/must-not-run", 10_000, None, None, None)
        .await
        .expect("injection probe should run");

    sandbox
        .cleanup()
        .await
        .expect("docker cleanup should succeed");

    assert!(
        result.result.is_success(),
        "stdin command failed: stdout={} stderr={}",
        result.result.stdout,
        result.result.stderr
    );
    assert_eq!(result.result.stdout.as_bytes(), stdin);
    assert!(
        injection_probe.is_success(),
        "stdin bytes must not be evaluated as shell source"
    );
}

#[tokio::test]
#[ignore = "requires real Docker container lifecycle, image, network, and a public GitHub clone"]
async fn cloned_docker_sandbox_uses_repos_checkout_and_workspace_symlink() {
    let image = "buildpack-deps:noble";
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return;
    };
    if docker.inspect_image(image).await.is_err() {
        return;
    }

    let sandbox = DockerSandbox::new(
        DockerSandboxOptions {
            image: image.to_string(),
            auto_pull: false,
            skip_clone: false,
            ..DockerSandboxOptions::default()
        },
        None,
        None,
        Some("https://github.com/brynary/rack-test".to_string()),
        None,
        None,
        None,
    )
    .expect("docker sandbox should construct");
    sandbox
        .initialize()
        .await
        .expect("docker sandbox should initialize");

    assert_eq!(sandbox.working_directory(), "/workspace/rack-test");

    let result = sandbox
        .exec_command(
            "test -d /repos/brynary/rack-test/.git && \
             test -L /workspace/rack-test && \
             test \"$(readlink /workspace/rack-test)\" = /repos/brynary/rack-test && \
             test \"$(git -C /repos/brynary/rack-test rev-parse HEAD)\" = \
                  \"$(git -C /workspace/rack-test rev-parse HEAD)\" && \
             git rev-parse --is-inside-work-tree",
            10_000,
            None,
            None,
            None,
        )
        .await
        .expect("layout verification command should run");
    sandbox
        .cleanup()
        .await
        .expect("docker cleanup should succeed");

    assert!(
        result.is_success(),
        "layout verification failed: stdout={} stderr={}",
        result.stdout,
        result.stderr
    );
    assert!(result.stdout.contains("true"));
}

// Both command paths must evaluate the same interpreter, so Bash-only syntax
// that `sh` rejects has to behave identically through `exec_command` and
// `exec_command_streaming`. Neither path is evidence for the other: they build
// separate exec invocations, and the streaming one wraps the user command in a
// controlled child.
#[tokio::test]
#[ignore = "requires real Docker container lifecycle; run explicitly when changing Docker exec integration"]
async fn docker_runs_clean_bash_through_both_command_paths() {
    let image = "buildpack-deps:noble";
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return;
    };
    if docker.inspect_image(image).await.is_err() {
        return;
    }

    let sandbox = DockerSandbox::new(
        DockerSandboxOptions {
            image: image.to_string(),
            auto_pull: false,
            env_vars: vec!["BASH_ENV=/tmp/fabro-bash-env".to_string()],
            skip_clone: true,
            ..DockerSandboxOptions::default()
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("docker sandbox should construct");
    sandbox
        .initialize()
        .await
        .expect("docker sandbox should initialize");

    // If the image-level BASH_ENV survives either exec boundary, every
    // subsequent Bash process prints this line before the requested command.
    let setup = sandbox
        .exec_command(
            "printf \"printf 'startup-source-loaded\\\\n'\\n\" > /tmp/fabro-bash-env",
            10_000,
            None,
            None,
            None,
        )
        .await
        .expect("startup-file fixture should be created");
    assert!(setup.is_success());

    // Arrays, `[[ ]]`, and `${arr[@]}` are Bash-only; `shopt -q login_shell`
    // proves the command did not run under a login shell. Exact output also
    // proves the image's BASH_ENV startup file was not sourced.
    let command = "arr=(one two three); [[ ${#arr[@]} -eq 3 ]] || exit 1; \
                   shopt -q login_shell && exit 2; echo ${arr[1]}";

    let non_streaming = sandbox
        .exec_command(command, 10_000, None, None, None)
        .await
        .expect("non-streaming command should run");

    let chunks = Arc::new(Mutex::new(Vec::new()));
    let streaming = sandbox
        .exec_command_streaming(ExecStreamingRequest {
            timeout_ms: Some(10_000),
            output_callback: Some(capture_bytes(Arc::clone(&chunks))),
            ..ExecStreamingRequest::new(command)
        })
        .await
        .expect("streaming command should run");

    sandbox
        .cleanup()
        .await
        .expect("docker cleanup should succeed");

    assert!(
        non_streaming.is_success(),
        "non-streaming Bash-only command failed: stdout={} stderr={}",
        non_streaming.stdout,
        non_streaming.stderr
    );
    assert_eq!(non_streaming.stdout.trim(), "two");
    assert!(
        streaming.result.is_success(),
        "streaming Bash-only command failed: stdout={} stderr={}",
        streaming.result.stdout,
        streaming.result.stderr
    );
    assert_eq!(streaming.result.stdout.trim(), "two");
    assert_eq!(String::from_utf8_lossy(&chunks.lock().await).trim(), "two");
}

// Regression test for glob patterns that contain a path separator. Before the
// glob fix, the remote providers ran `find <base> -name <pattern>`, and
// `find -name` matches only the basename and rejects patterns containing `/`.
// So `*/SKILL.md` and `**/SKILL.md` silently returned an empty list inside a
// real container even though the files existed. Both `glob` calls below fail
// against that old implementation and pass once traversal and matching are
// split (find files, then match host-side).
#[tokio::test]
#[ignore = "requires real Docker container lifecycle; run explicitly when changing Sandbox::glob"]
async fn docker_glob_matches_patterns_containing_a_path_separator() {
    let image = "buildpack-deps:noble";
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return;
    };
    if docker.inspect_image(image).await.is_err() {
        return;
    }

    let sandbox = DockerSandbox::new(
        DockerSandboxOptions {
            image: image.to_string(),
            auto_pull: false,
            skip_clone: true,
            ..DockerSandboxOptions::default()
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("docker sandbox should construct");
    sandbox
        .initialize()
        .await
        .expect("docker sandbox should initialize");

    // Build a skills tree with a SKILL.md at the search root, one level below
    // it, and two levels below it.
    let seed = sandbox
        .exec_command(
            "mkdir -p skills/patch skills/nested/deeper && \
             touch skills/SKILL.md skills/patch/SKILL.md skills/nested/deeper/SKILL.md",
            10_000,
            None,
            None,
            None,
        )
        .await
        .expect("seed command should run");

    // `*/SKILL.md` matches exactly one path segment: only the file one level
    // below the search directory, not the root file or the deeper one.
    let one_level = sandbox.glob("*/SKILL.md", Some("skills")).await;
    // `**/SKILL.md` matches at any depth, including several levels down.
    let recursive = sandbox.glob("**/SKILL.md", Some("skills")).await;

    sandbox
        .cleanup()
        .await
        .expect("docker cleanup should succeed");

    assert!(
        seed.is_success(),
        "seeding the skills tree failed: stdout={} stderr={}",
        seed.stdout,
        seed.stderr
    );

    let one_level = one_level.expect("glob should run");
    assert_eq!(
        one_level.len(),
        1,
        "`*/SKILL.md` should match exactly one level below the search dir, got: {one_level:?}"
    );
    assert!(
        one_level[0].ends_with("skills/patch/SKILL.md"),
        "`*/SKILL.md` should match the one-level-deep file, got: {one_level:?}"
    );

    let recursive = recursive.expect("recursive glob should run");
    assert!(
        recursive
            .iter()
            .any(|path| path.ends_with("skills/nested/deeper/SKILL.md")),
        "`**/SKILL.md` should match files nested several levels deep, got: {recursive:?}"
    );
}

// The Fabro runtime directory is where prompt blobs materialize, so it must
// exist after initialization, sit outside the repository checkout, and stay
// owner-private along with the files written beneath it (issue #798).
#[tokio::test]
#[ignore = "requires real Docker container lifecycle; run explicitly when changing Docker runtime directory setup"]
async fn docker_runtime_directory_is_private_and_outside_workspace() {
    let image = "buildpack-deps:noble";
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        return;
    };
    if docker.inspect_image(image).await.is_err() {
        return;
    }

    let sandbox = DockerSandbox::new(
        DockerSandboxOptions {
            image: image.to_string(),
            auto_pull: false,
            skip_clone: true,
            ..DockerSandboxOptions::default()
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("docker sandbox should construct");
    sandbox
        .initialize()
        .await
        .expect("docker sandbox should initialize");

    let runtime_directory = sandbox
        .runtime_directory()
        .expect("docker sandbox should expose a runtime directory")
        .to_string();
    assert!(
        !runtime_directory.starts_with(sandbox.working_directory()),
        "runtime directory {runtime_directory} must sit outside the workspace"
    );

    let blob_path = format!("{runtime_directory}/blobs/test-blob.json");
    sandbox
        .write_file(&blob_path, "{}")
        .await
        .expect("runtime blob write should succeed");

    let modes = sandbox
        .exec_command(
            &format!("stat -c '%a' {runtime_directory} {blob_path}"),
            10_000,
            None,
            None,
            None,
        )
        .await
        .expect("stat should run");
    let readback = sandbox.read_file_text(&blob_path).await;

    sandbox
        .cleanup()
        .await
        .expect("docker cleanup should succeed");

    assert!(modes.is_success(), "stat failed: {}", modes.stderr);
    let modes: Vec<&str> = modes.stdout.split_whitespace().collect();
    assert_eq!(
        modes,
        ["700", "600"],
        "runtime directory and blob file should be owner-private"
    );
    assert_eq!(readback.expect("runtime blob should be readable"), "{}");
}
