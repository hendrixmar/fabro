//! Sandbox-owned MCP processes. Dropping a guard signals its workers; explicit
//! shutdown also waits for process-group cleanup before the sandbox is
//! released.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail, ensure};
use fabro_sandbox::Sandbox;
use fabro_util::shell;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use crate::config::{McpServerSettings, McpTransport};
use crate::http_transport::sandbox_mcp_http_url;

pub struct ManagedMcpServers {
    servers: Vec<McpServerSettings>,
    stop:    CancellationToken,
    workers: Vec<JoinHandle<()>>,
}

impl ManagedMcpServers {
    /// Resolve sandbox transports without moving host stdio processes or their
    /// credentials across the sandbox boundary. Startup failure is fatal.
    pub async fn start(
        servers: &[McpServerSettings],
        sandbox: Arc<dyn Sandbox>,
        cancel_token: &CancellationToken,
        client_in_sandbox: bool,
    ) -> Result<Self> {
        let mut managed = Self {
            servers: Vec::with_capacity(servers.len()),
            stop:    cancel_token.child_token(),
            workers: Vec::new(),
        };
        for config in servers {
            if managed.stop.is_cancelled() {
                managed.shutdown().await;
                bail!("MCP startup cancelled");
            }
            if !matches!(&config.transport, McpTransport::Sandbox { .. }) {
                managed.servers.push(config.clone());
                continue;
            }
            let (send, receive) = oneshot::channel();
            let sandbox = Arc::clone(&sandbox);
            let stop = managed.stop.clone();
            let config = config.clone();
            // The worker owns the launch future, including when start() is
            // dropped. Never abandon a remote detached launch on cancellation.
            managed.workers.push(tokio::spawn(async move {
                let directory = format!("/tmp/fabro-mcp-{}", uuid::Uuid::new_v4());
                let result = start_one(
                    &config,
                    sandbox.as_ref(),
                    &directory,
                    &stop,
                    client_in_sandbox,
                )
                .await;
                let started = result.is_ok();
                if send.send(result).is_ok() && started {
                    stop.cancelled().await;
                }
                cleanup(sandbox.as_ref(), &directory).await;
            }));
            match receive.await {
                Ok(Ok(resolved)) => managed.servers.push(resolved),
                result => {
                    managed.shutdown().await;
                    return Err(match result {
                        Ok(Err(error)) => error,
                        _ => anyhow!("MCP startup worker stopped unexpectedly"),
                    });
                }
            }
        }
        if managed.stop.is_cancelled() && !servers.is_empty() {
            managed.shutdown().await;
            bail!("MCP startup cancelled");
        }
        Ok(managed)
    }

    #[must_use]
    pub fn servers(&self) -> &[McpServerSettings] {
        &self.servers
    }

    pub async fn shutdown(&mut self) {
        self.stop.cancel();
        for worker in self.workers.drain(..) {
            let _ = worker.await;
        }
    }
}

impl Drop for ManagedMcpServers {
    fn drop(&mut self) {
        // Workers keep their sandbox Arc until cleanup completes. Unlike a
        // spawned task in Drop this works even outside a runtime context.
        self.stop.cancel();
    }
}

async fn start_one(
    config: &McpServerSettings,
    sandbox: &dyn Sandbox,
    directory: &str,
    stop: &CancellationToken,
    client_in_sandbox: bool,
) -> Result<McpServerSettings> {
    let McpTransport::Sandbox {
        protocol,
        command,
        port,
        env,
    } = &config.transport
    else {
        unreachable!("only sandbox transports have lifecycle workers");
    };
    ensure!(!command.is_empty(), "invalid sandbox MCP command");
    ensure!(!stop.is_cancelled(), "MCP startup cancelled");
    let deadline = Instant::now() + config.startup_timeout();
    let timeout_ms = u64::try_from(config.startup_timeout().as_millis()).unwrap_or(u64::MAX);
    ensure!(timeout_ms > 0, "MCP startup timed out");
    let token = uuid::Uuid::new_v4().to_string();
    let mut env = env.clone();
    env.insert("FABRO_MCP_BEARER_TOKEN".into(), token.clone());
    env.insert("FABRO_MCP_PORT".into(), port.to_string());
    let launch = launch_script(command, directory);
    let working_dir = config
        .current_dir
        .as_deref()
        .map(|path| {
            path.to_str()
                .ok_or_else(|| anyhow!("MCP working directory is not UTF-8"))
        })
        .transpose()?;
    // Explicit MCP credentials belong to the trusted process-launch seam.
    // Tool exec intentionally filters secret-shaped environment keys.
    let launch_cancel = stop.child_token();
    let cancel_launch_on_drop = launch_cancel.clone().drop_guard();
    let command = format!("\"$BASH\" -c {}", shell::shell_quote(&launch));
    let launched = timeout_at(
        deadline,
        sandbox.spawn_stdio_process(
            &command,
            working_dir,
            Some(&env),
            Some(launch_cancel.clone()),
        ),
    )
    .await
    .map_err(|_| anyhow!("MCP server '{}' startup timed out", config.name))?
    .map_err(|_| anyhow!("MCP server '{}' trusted launch failed", config.name))?;
    let completion = tokio::select! {
        () = stop.cancelled() => Err(anyhow!("MCP startup cancelled")),
        result = timeout_at(deadline, launched.handle.wait()) => {
            result.map_err(|_| anyhow!("MCP server '{}' startup timed out", config.name))
                .and_then(|result| result.map_err(|_| anyhow!("MCP server '{}' launch failed", config.name)))
        }
    };
    if completion.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(5), launched.handle.terminate()).await;
    }
    drop(cancel_launch_on_drop);
    let completion = completion?;
    ensure!(!stop.is_cancelled(), "MCP startup cancelled");
    ensure!(
        completion.exit_code == Some(0),
        "MCP server '{}' launch failed",
        config.name
    );

    let ready_script = readiness_script(directory, *port);
    let port = loop {
        ensure!(
            Instant::now() < deadline,
            "MCP server '{}' startup timed out",
            config.name
        );
        let ready = tokio::select! {
            () = stop.cancelled() => bail!("MCP startup cancelled"),
            result = timeout_at(deadline, sandbox.exec_command(&ready_script, timeout_ms, None, None, Some(stop.child_token()))) => {
                result.map_err(|_| anyhow!("MCP server '{}' startup timed out", config.name))?
                    .map_err(|_| anyhow!("MCP server '{}' readiness check failed", config.name))?
            }
        };
        ensure!(
            !ready.is_timed_out(),
            "MCP server '{}' startup timed out",
            config.name
        );
        match ready.exit_code {
            Some(0) => {
                let actual_port = ready.stdout.trim().parse::<u16>().map_err(|_| {
                    anyhow!("MCP server '{}' reported an invalid port", config.name)
                })?;
                ensure!(
                    actual_port != 0 && (*port == 0 || actual_port == *port),
                    "MCP server '{}' reported an unexpected port",
                    config.name
                );
                break actual_port;
            }
            Some(1) => {}
            _ => bail!(
                "MCP server '{}' exited or process ownership could not be verified",
                config.name
            ),
        }
        tokio::select! {
            () = stop.cancelled() => bail!("MCP startup cancelled"),
            _ = tokio::time::sleep_until((Instant::now() + Duration::from_millis(100)).min(deadline)) => {}
        }
    };
    let (base_url, mut headers) = if client_in_sandbox {
        (format!("http://127.0.0.1:{port}"), HashMap::new())
    } else {
        tokio::select! {
            () = stop.cancelled() => bail!("MCP startup cancelled"),
            result = timeout_at(deadline, sandbox.get_preview_url(port)) => {
                result.map_err(|_| anyhow!("MCP server '{}' startup timed out", config.name))?
                    .map_err(|_| anyhow!("MCP server '{}' preview lookup failed", config.name))?
                    .unwrap_or_else(|| (format!("http://127.0.0.1:{port}"), HashMap::new()))
            }
        }
    };
    // Reserved authentication cannot be overridden by configured environment
    // or preview headers, and is never included in diagnostic output.
    headers.retain(|name, _| !name.eq_ignore_ascii_case("authorization"));
    headers.insert("Authorization".into(), format!("Bearer {token}"));
    let mut resolved = config.clone();
    resolved.transport = McpTransport::Http {
        protocol: *protocol,
        url: sandbox_mcp_http_url(*protocol, &base_url)?,
        headers,
    };
    Ok(resolved)
}

fn launch_script(command: &[String], directory: &str) -> String {
    let source = match command {
        [interpreter, flag, source] if interpreter == "bash" && flag == "-c" => {
            format!("{{\n{source}\n}}")
        }
        _ => shell::shell_join(command),
    };
    let directory = shell::shell_quote(directory);
    let inner = format!(
        r#"
dir={directory}
stat=$(< /proc/$$/stat) || exit 1
stat=${{stat##*) }}
read -r -a fields <<< "$stat"
[[ ${{fields[2]}} == $$ && ${{fields[3]}} == $$ ]] || exit 1
printf '%s %s\n' "$$" "${{fields[19]}}" > "$dir/identity"
if [[ -e "$dir/stop" ]]; then rm -rf -- "$dir"; exit 0; fi
# Keep the session leader alive through TERM, so cleanup can revalidate its
# birth time before KILL even when the actual server has already exited.
trap ':' TERM
{source} > "$dir/stdout.log" 2> "$dir/stderr.log" &
child=$!
wait "$child"
printf '%s\n' "$?" > "$dir/exited"
while :; do sleep 1; done
"#
    );
    format!(
        "umask 077\n\
         [[ -r /proc/self/stat && -r /proc/net/tcp ]] || exit 1\n\
         command -v setsid >/dev/null && command -v readlink >/dev/null || exit 1\n\
         mkdir -m 700 -- {directory} || exit 1\n\
         setsid \"$BASH\" -c {} </dev/null >/dev/null 2>&1 &",
        shell::shell_quote(&inner),
    )
}

// Bash builtins parse procfs; readlink is in coreutils. No ss/netstat/lsof,
// Python, Node or shell tools from the server's configured PATH are required.
const IDENTITY_CHECK: &str = r#"
read -r pid born < "$dir/identity" || exit 1
[[ $pid =~ ^[0-9]+$ && $born =~ ^[0-9]+$ && $pid -gt 1 ]] || exit 2
same_process() {
    local stat
    stat=$(< "/proc/$pid/stat") || return 1
    stat=${stat##*) }
    read -r -a fields <<< "$stat"
    [[ ${fields[2]} == "$pid" && ${fields[3]} == "$pid" && ${fields[19]} == "$born" ]]
}
same_process || exit 2
"#;

fn readiness_script(directory: &str, port: u16) -> String {
    format!(
        r#"
dir={}
{IDENTITY_CHECK}
[[ ! -e "$dir/exited" ]] || exit 2
declare -A sockets
for table in /proc/net/tcp /proc/net/tcp6; do
    [[ -r $table ]] || continue
    while read -r slot local remote state queues timer retransmit uid timeout inode rest; do
        if [[ ( {port} == 0 || ${{local##*:}} == {port:04X} ) && $state == 0A ]]; then
            sockets[$inode]=${{local##*:}}
        fi
    done < "$table"
done
[[ ${{#sockets[@]}} -gt 0 ]] || exit 1
for proc in /proc/[0-9]*; do
    stat=$(< "$proc/stat") 2>/dev/null || continue
    stat=${{stat##*) }}
    read -r -a fields <<< "$stat"
    [[ ${{fields[2]}} == "$pid" && ${{fields[3]}} == "$pid" ]] || continue
    for fd in "$proc"/fd/*; do
        target=$(readlink "$fd" 2>/dev/null) || continue
        if [[ $target =~ ^socket:\[([0-9]+)\]$ && -n ${{sockets[${{BASH_REMATCH[1]}}]:-}} ]]; then
            socket_port=${{sockets[${{BASH_REMATCH[1]}}]}}
            same_process && [[ ! -e "$dir/exited" ]] || exit 2
            printf '%d\n' "$((16#$socket_port))"
            exit 0
        fi
    done
done
exit 1
"#,
        shell::shell_quote(directory)
    )
}

async fn cleanup(sandbox: &dyn Sandbox, directory: &str) {
    let script = format!(
        r#"
dir={}
# Retain a stop tombstone even when a timed-out remote launch has not reached
# mkdir yet. Its exclusive mkdir will fail, so it cannot spawn after cleanup.
umask 077
mkdir -m 700 -- "$dir" 2>/dev/null || [[ -d "$dir" ]] || exit 1
: > "$dir/stop"
{IDENTITY_CHECK}
kill -TERM -- "-$pid" 2>/dev/null
sleep 1
if same_process; then kill -KILL -- "-$pid" 2>/dev/null; fi
rm -rf -- "$dir"
"#,
        shell::shell_quote(directory)
    );
    if sandbox
        .exec_command(&script, 5_000, None, None, None)
        .await
        .is_err()
    {
        tracing::warn!("Sandbox MCP cleanup could not reach the sandbox");
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use fabro_sandbox::LocalSandbox;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    #[tokio::test]
    async fn late_launch_cannot_start_after_cleanup() {
        let sandbox = fabro_sandbox::LocalSandbox::new(std::env::temp_dir());
        let directory = format!("/tmp/fabro-mcp-{}", uuid::Uuid::new_v4());
        let launch = launch_script(&["sleep".into(), "30".into()], &directory);
        // The remote launch request is delayed until after its owner times out
        // and completes cleanup. Executing the real launcher now must not spawn.
        cleanup(&sandbox, &directory).await;
        let result = sandbox
            .exec_command(&launch, 5_000, None, None, None)
            .await
            .unwrap();
        let identity_exists = sandbox
            .file_exists(&format!("{directory}/identity"))
            .await
            .unwrap();
        sandbox
            .exec_command(
                &format!("rm -rf -- {}", shell::shell_quote(&directory)),
                5_000,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_ne!(result.exit_code, Some(0));
        assert!(
            !identity_exists,
            "late launch must not create a process group"
        );
    }

    fn sandbox() -> Arc<dyn Sandbox> {
        Arc::new(LocalSandbox::new(std::env::temp_dir()))
    }

    async fn available_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn server(port: u16) -> McpServerSettings {
        McpServerSettings {
            name:                 "debugger".into(),
            transport:            McpTransport::Sandbox {
                protocol: crate::config::McpHttpProtocol::StreamableHttp,
                command: vec![
                    "python3".into(),
                    "-m".into(),
                    "http.server".into(),
                    port.to_string(),
                    "--bind".into(),
                    "127.0.0.1".into(),
                ],
                port,
                env: HashMap::new(),
            },
            current_dir:          None,
            clear_env:            false,
            startup_timeout_secs: 5,
            tool_timeout_secs:    30,
        }
    }

    async fn wait_closed(port: u16) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn simultaneous_ephemeral_servers_in_one_sandbox_are_isolated() {
        let sandbox = sandbox();
        let mut config = server(0);
        if let McpTransport::Sandbox { command, env, .. } = &mut config.transport {
            env.insert("FABRO_MCP_PORT".into(), "1".into());
            *command = vec!["python3".into(), "-c".into(), "import http.server, os; http.server.HTTPServer(('127.0.0.1', int(os.environ['FABRO_MCP_PORT'])), http.server.BaseHTTPRequestHandler).serve_forever()".into()];
        }
        let configs = [config];
        let first_token = CancellationToken::new();
        let second_token = CancellationToken::new();
        let (first, second) = tokio::join!(
            ManagedMcpServers::start(&configs, Arc::clone(&sandbox), &first_token, true),
            ManagedMcpServers::start(&configs, Arc::clone(&sandbox), &second_token, true),
        );
        let mut first = first.unwrap();
        let mut second = second.unwrap();
        let resolved_port = |managed: &ManagedMcpServers| -> u16 {
            let McpTransport::Http { url, .. } = &managed.servers()[0].transport else {
                panic!("expected HTTP");
            };
            url.strip_prefix("http://127.0.0.1:")
                .unwrap()
                .strip_suffix("/mcp")
                .unwrap()
                .parse()
                .unwrap()
        };
        let first_port = resolved_port(&first);
        let second_port = resolved_port(&second);
        assert_ne!(first_port, 0);
        assert_ne!(second_port, 0);
        assert_ne!(first_port, second_port);
        assert!(TcpStream::connect(("127.0.0.1", first_port)).await.is_ok());
        assert!(TcpStream::connect(("127.0.0.1", second_port)).await.is_ok());
        first.shutdown().await;
        wait_closed(first_port).await;
        assert!(TcpStream::connect(("127.0.0.1", second_port)).await.is_ok());
        second.shutdown().await;
        wait_closed(second_port).await;
    }

    #[tokio::test]
    async fn occupied_port_is_not_readiness_and_is_not_killed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let result =
            ManagedMcpServers::start(&[server(port)], sandbox(), &CancellationToken::new(), true)
                .await;
        assert!(result.is_err());
        assert!(TcpStream::connect(("127.0.0.1", port)).await.is_ok());
    }

    #[tokio::test]
    async fn cancellation_and_drop_only_stop_owned_servers() {
        let first_port = available_port().await;
        let token = CancellationToken::new();
        let mut first = ManagedMcpServers::start(&[server(first_port)], sandbox(), &token, true)
            .await
            .unwrap();
        let second_port = available_port().await;
        let second = ManagedMcpServers::start(
            &[server(second_port)],
            sandbox(),
            &CancellationToken::new(),
            true,
        )
        .await
        .unwrap();
        assert!(
            matches!(&first.servers()[0].transport, McpTransport::Http { url, headers, .. } if url == &format!("http://127.0.0.1:{first_port}/mcp") && headers.get("Authorization").is_some_and(|value| value.starts_with("Bearer ")))
        );
        token.cancel();
        wait_closed(first_port).await;
        assert!(TcpStream::connect(("127.0.0.1", second_port)).await.is_ok());
        first.shutdown().await;
        drop(second);
        wait_closed(second_port).await;
    }

    #[tokio::test]
    async fn bearer_is_ephemeral_and_overrides_configured_credentials() {
        let port = available_port().await;
        let mut config = server(port);
        if let McpTransport::Sandbox { command, env, .. } = &mut config.transport {
            env.insert("FABRO_MCP_BEARER_TOKEN".into(), "configured-token".into());
            *command = vec![
                "python3".into(),
                "-c".into(),
                format!(
                    r#"
import http.server, os
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        valid = self.headers.get('Authorization') == 'Bearer ' + os.environ['FABRO_MCP_BEARER_TOKEN']
        self.send_response(204 if valid else 403)
        self.end_headers()
http.server.HTTPServer(('127.0.0.1', {port}), Handler).serve_forever()
"#
                ),
            ];
        }
        let mut managed =
            ManagedMcpServers::start(&[config], sandbox(), &CancellationToken::new(), true)
                .await
                .unwrap();
        let McpTransport::Http { headers, .. } = &managed.servers()[0].transport else {
            panic!("expected HTTP");
        };
        let bearer = headers.get("Authorization").unwrap();
        assert!(bearer != "Bearer configured-token");
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream
            .write_all(format!("GET /mcp HTTP/1.0\r\nAuthorization: {bearer}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.0 204"));
        managed.shutdown().await;
        wait_closed(port).await;
    }

    #[tokio::test]
    async fn stale_process_identity_does_not_kill_reused_pid() {
        let sandbox = sandbox();
        let directory = format!("/tmp/fabro-mcp-{}", uuid::Uuid::new_v4());
        let launch = launch_script(&["sleep".into(), "30".into()], &directory);
        sandbox
            .exec_command(&launch, 5_000, None, None, None)
            .await
            .unwrap();
        let identity_path = format!("{directory}/identity");
        let identity = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(identity) = sandbox.read_file_text(&identity_path).await {
                    if identity.split_whitespace().count() == 2 {
                        break identity;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        let pid: u32 = identity.split_whitespace().next().unwrap().parse().unwrap();
        sandbox
            .write_file(&identity_path, &format!("{pid} 0\n"))
            .await
            .unwrap();
        cleanup(sandbox.as_ref(), &directory).await;
        let alive = sandbox
            .exec_command(&format!("kill -0 {pid}"), 5_000, None, None, None)
            .await
            .unwrap();
        // Restore the real identity before asserting, so even a failing
        // regression assertion cannot strand the fixture's process group.
        sandbox.write_file(&identity_path, &identity).await.unwrap();
        cleanup(sandbox.as_ref(), &directory).await;
        assert_eq!(alive.exit_code, Some(0));
    }

    #[tokio::test]
    async fn startup_timeout_is_explicit_and_does_not_leak_stderr() {
        let mut config = server(available_port().await);
        config.startup_timeout_secs = 1;
        if let McpTransport::Sandbox { command, .. } = &mut config.transport {
            *command = vec![
                "bash".into(),
                "-c".into(),
                "echo secret-value >&2; sleep 30".into(),
            ];
        }
        let result =
            ManagedMcpServers::start(&[config], sandbox(), &CancellationToken::new(), true).await;
        let error = result.err().expect("startup must fail").to_string();
        assert!(error.contains("timed out"), "{error}");
        assert!(!error.contains("secret-value"));
    }

    #[tokio::test]
    async fn cancelled_startup_releases_earlier_servers() {
        let port = available_port().await;
        let mut pending = server(available_port().await);
        if let McpTransport::Sandbox { command, .. } = &mut pending.transport {
            *command = vec!["sleep".into(), "30".into()];
        }
        let token = CancellationToken::new();
        let cancellation = token.clone();
        let task = tokio::spawn(async move {
            ManagedMcpServers::start(&[server(port), pending], sandbox(), &token, true).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        assert!(task.await.unwrap().is_err());
        wait_closed(port).await;
    }
}
