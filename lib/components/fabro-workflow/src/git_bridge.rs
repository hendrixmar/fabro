//! Secret-free Git bridging environment for additional-repository access.
//!
//! When a run declares additional GitHub repositories, every resolved
//! command/tool/ACP environment receives `GIT_CONFIG_COUNT` /
//! `GIT_CONFIG_KEY_n` / `GIT_CONFIG_VALUE_n` entries that make plain Git
//! commands work against the declared set through the managed
//! `GITHUB_TOKEN`:
//!
//! - a credential helper for `https://github.com` that reads `$GITHUB_TOKEN`
//!   from the invoking Git process's environment at invocation time, so token
//!   refresh flows through per-stage environment resolution with no bridging
//!   update;
//! - per-repository `url.<https>.insteadOf` rewrites for the
//!   `git@github.com:owner/repo[.git]` and
//!   `ssh://git@github.com/owner/repo[.git]` SSH spellings of each effective
//!   repository.
//!
//! None of the values contain a secret; the token lives only in
//! `GITHUB_TOKEN`.
//!
//! The credential helper is host-scoped to `https://github.com`, not
//! path-scoped. This is safe because the token is scoped server-side to the
//! declared repository set and is only ever offered to github.com. It does
//! change one failure mode for *undeclared* repositories: public HTTPS
//! clones are unaffected (Git tries unauthenticated first), while private
//! undeclared HTTPS repositories fail with a GitHub authorization error
//! instead of a missing-credential error. Both fail; only the diagnostic
//! differs.
//!
//! `insteadOf` matches by string prefix, not exactly: a rule for
//! `owner/repo` also matches `owner/repo-other`. An undeclared repository
//! that shares a declared prefix is therefore rewritten to HTTPS; the scoped
//! token is invalid for it at GitHub, so authority is unchanged, but its Git
//! transport changes from SSH to HTTPS.

use std::collections::HashMap;

use fabro_github::{GITHUB_CREDENTIAL_HELPER, GITHUB_CREDENTIAL_HELPER_KEY};
use fabro_types::GitHubRepositorySlug;

use crate::error::Error;

/// Section base for the effective repositories' HTTPS routes.
const GITHUB_HTTPS_BASE: &str = "https://github.com/";

/// Merge the bridging entries into `env` for the effective repository set
/// (primary first). Appends after any valid user-provided `GIT_CONFIG_COUNT`
/// overlay without overwriting it, and fails with a configuration error when
/// the user overlay is malformed rather than silently replacing it.
pub(crate) fn merge_git_bridge_env(
    env: &mut HashMap<String, String>,
    targets: &[&GitHubRepositorySlug],
) -> Result<(), Error> {
    let start = user_git_config_count(env)?;
    let entries = bridge_entries(targets, GITHUB_HTTPS_BASE);
    let total = start + entries.len();
    for (offset, (key, value)) in entries.into_iter().enumerate() {
        let index = start + offset;
        env.insert(format!("GIT_CONFIG_KEY_{index}"), key);
        env.insert(format!("GIT_CONFIG_VALUE_{index}"), value);
    }
    env.insert("GIT_CONFIG_COUNT".to_string(), total.to_string());
    // Fail instead of hanging when access is missing or invalid; a user who
    // explicitly configured prompting keeps their value.
    env.entry("GIT_TERMINAL_PROMPT".to_string())
        .or_insert_with(|| "0".to_string());
    Ok(())
}

/// The bridge's Git config entries in order: the credential helper, then two
/// SSH-to-HTTPS rewrites per repository. `https_base` is
/// [`GITHUB_HTTPS_BASE`] in production; contract tests substitute a local
/// `file://` root to prove real Git applies the generated entries without
/// touching the network.
fn bridge_entries(targets: &[&GitHubRepositorySlug], https_base: &str) -> Vec<(String, String)> {
    let mut entries = Vec::with_capacity(1 + targets.len() * 2);
    entries.push((
        GITHUB_CREDENTIAL_HELPER_KEY.to_string(),
        GITHUB_CREDENTIAL_HELPER.to_string(),
    ));
    for slug in targets {
        let owner = slug.owner();
        let repo = slug.repo();
        let https = format!("{https_base}{owner}/{repo}");
        // One prefix rule per SSH spelling covers both the bare and `.git`
        // suffixed forms.
        entries.push((
            format!("url.{https}.insteadOf"),
            format!("git@github.com:{owner}/{repo}"),
        ));
        entries.push((
            format!("url.{https}.insteadOf"),
            format!("ssh://git@github.com/{owner}/{repo}"),
        ));
    }
    entries
}

/// Validate and measure a user-provided `GIT_CONFIG_COUNT` overlay so the
/// bridge appends after it. Orphaned `GIT_CONFIG_KEY_n` entries without a
/// count are inert to Git and are treated as absent.
fn user_git_config_count(env: &HashMap<String, String>) -> Result<usize, Error> {
    let Some(raw) = env.get("GIT_CONFIG_COUNT") else {
        return Ok(0);
    };
    let count: usize = raw.trim().parse().map_err(|_| {
        Error::Precondition(format!(
            "environment variable GIT_CONFIG_COUNT must be a non-negative integer to combine \
             with Fabro's Git bridging entries, got `{raw}`"
        ))
    })?;
    for index in 0..count {
        let key = format!("GIT_CONFIG_KEY_{index}");
        let value = format!("GIT_CONFIG_VALUE_{index}");
        if !env.contains_key(&key) || !env.contains_key(&value) {
            return Err(Error::Precondition(format!(
                "GIT_CONFIG_COUNT is {count} but {key} or {value} is missing; fix the indexed \
                 Git config overlay so Fabro can append its bridging entries after it"
            )));
        }
    }
    Ok(count)
}

#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "contract tests drive the installed git binary synchronously in non-async tests"
)]
mod tests {
    use std::path::Path;
    use std::process::Command;

    use super::*;

    fn slug(value: &str) -> GitHubRepositorySlug {
        value.parse().expect("test slug should parse")
    }

    fn bridged_env(
        base_env: HashMap<String, String>,
        targets: &[&GitHubRepositorySlug],
    ) -> HashMap<String, String> {
        let mut env = base_env;
        merge_git_bridge_env(&mut env, targets).expect("bridge entries should merge");
        env
    }

    /// Run `git` with ONLY the bridge-relevant environment: the inherited
    /// user/system/global Git config is disabled so assertions observe just
    /// the generated entries.
    fn git(args: &[&str], env: &HashMap<String, String>, cwd: &Path) -> std::process::Output {
        let mut command = Command::new("git");
        command
            .args(args)
            .current_dir(cwd)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "true");
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().expect("git should run")
    }

    /// Create a bare fixture answering both the bare and `.git`-suffixed
    /// routes, the way GitHub serves both HTTPS spellings.
    fn init_bare_fixture(root: &Path, owner_repo: &str) -> String {
        let fixture = root.join(format!("{owner_repo}.git"));
        std::fs::create_dir_all(&fixture).unwrap();
        let init = Command::new("git")
            .args(["init", "--bare", "--initial-branch=main"])
            .arg(&fixture)
            .output()
            .expect("git init should run");
        assert!(init.status.success(), "{init:?}");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&fixture, root.join(owner_repo)).unwrap();
        format!("file://{}/", root.display())
    }

    #[test]
    fn no_targets_means_no_bridge_call_and_empty_env_stays_empty() {
        // The caller only bridges when the additional set is non-empty; the
        // pure entry builder is still total for the primary-only case.
        assert_eq!(bridge_entries(&[], GITHUB_HTTPS_BASE).len(), 1);
        let env: HashMap<String, String> = HashMap::new();
        assert!(!env.contains_key("GIT_CONFIG_COUNT"));
    }

    #[test]
    fn merges_helper_rewrites_count_and_terminal_prompt() {
        let keystone = slug("fabro-sh/keystone");
        let fabro = slug("fabro-sh/fabro");
        let env = bridged_env(HashMap::new(), &[&fabro, &keystone]);

        assert_eq!(env.get("GIT_CONFIG_COUNT").map(String::as_str), Some("5"));
        assert_eq!(
            env.get("GIT_CONFIG_KEY_0").map(String::as_str),
            Some("credential.https://github.com.helper")
        );
        assert_eq!(
            env.get("GIT_CONFIG_KEY_1").map(String::as_str),
            Some("url.https://github.com/fabro-sh/fabro.insteadOf")
        );
        assert_eq!(
            env.get("GIT_CONFIG_VALUE_1").map(String::as_str),
            Some("git@github.com:fabro-sh/fabro")
        );
        assert_eq!(
            env.get("GIT_CONFIG_VALUE_2").map(String::as_str),
            Some("ssh://git@github.com/fabro-sh/fabro")
        );
        assert_eq!(
            env.get("GIT_TERMINAL_PROMPT").map(String::as_str),
            Some("0")
        );

        // No secrets anywhere in the generated values.
        for (key, value) in &env {
            assert!(!value.contains("ghs_"), "{key}={value}");
        }
    }

    #[test]
    fn respects_an_explicit_user_terminal_prompt() {
        let keystone = slug("fabro-sh/keystone");
        let env = bridged_env(
            HashMap::from([("GIT_TERMINAL_PROMPT".to_string(), "1".to_string())]),
            &[&keystone],
        );
        assert_eq!(
            env.get("GIT_TERMINAL_PROMPT").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn appends_after_a_valid_user_git_config_overlay() {
        let keystone = slug("fabro-sh/keystone");
        let env = bridged_env(
            HashMap::from([
                ("GIT_CONFIG_COUNT".to_string(), "1".to_string()),
                ("GIT_CONFIG_KEY_0".to_string(), "user.name".to_string()),
                ("GIT_CONFIG_VALUE_0".to_string(), "Overlay User".to_string()),
            ]),
            &[&keystone],
        );

        assert_eq!(env.get("GIT_CONFIG_COUNT").map(String::as_str), Some("4"));
        assert_eq!(
            env.get("GIT_CONFIG_KEY_0").map(String::as_str),
            Some("user.name"),
            "user entry must survive at its original index"
        );
        assert_eq!(
            env.get("GIT_CONFIG_KEY_1").map(String::as_str),
            Some("credential.https://github.com.helper")
        );

        // Real Git sees both the user's entry and the appended bridge entry.
        let dir = tempfile::tempdir().unwrap();
        let output = git(&["config", "--list"], &env, dir.path());
        assert!(output.status.success(), "{output:?}");
        let listed = String::from_utf8_lossy(&output.stdout);
        assert!(listed.contains("user.name=Overlay User"), "{listed}");
        assert!(
            listed.contains("credential.https://github.com.helper"),
            "{listed}"
        );
    }

    #[test]
    fn rejects_a_malformed_user_git_config_overlay() {
        let keystone = slug("fabro-sh/keystone");

        let mut non_numeric = HashMap::from([("GIT_CONFIG_COUNT".to_string(), "two".to_string())]);
        let err = merge_git_bridge_env(&mut non_numeric, &[&keystone]).unwrap_err();
        assert!(err.to_string().contains("GIT_CONFIG_COUNT"), "{err}");

        let mut missing_index = HashMap::from([
            ("GIT_CONFIG_COUNT".to_string(), "2".to_string()),
            ("GIT_CONFIG_KEY_0".to_string(), "user.name".to_string()),
            ("GIT_CONFIG_VALUE_0".to_string(), "Overlay".to_string()),
        ]);
        let err = merge_git_bridge_env(&mut missing_index, &[&keystone]).unwrap_err();
        assert!(err.to_string().contains("GIT_CONFIG_KEY_1"), "{err}");
    }

    /// With the bridge active, `git credential fill` for github.com resolves
    /// through the generated helper and reads `$GITHUB_TOKEN` from the
    /// invoking process environment at invocation time.
    #[test]
    fn credential_helper_reads_github_token_at_invocation_time() {
        use std::io::Write as _;

        let keystone = slug("fabro-sh/keystone");
        let mut env = bridged_env(HashMap::new(), &[&keystone]);
        env.insert("GITHUB_TOKEN".to_string(), "test-token-value".to_string());

        let dir = tempfile::tempdir().unwrap();
        let mut command = Command::new("git");
        command
            .args(["credential", "fill"])
            .current_dir(dir.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (key, value) in &env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("git credential fill should spawn");
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"protocol=https\nhost=github.com\npath=fabro-sh/keystone\n\n")
            .unwrap();
        let output = child.wait_with_output().unwrap();

        assert!(output.status.success(), "{output:?}");
        let filled = String::from_utf8_lossy(&output.stdout);
        assert!(filled.contains("username=x-access-token"), "{filled}");
        assert!(filled.contains("password=test-token-value"), "{filled}");
    }

    /// Real Git applies the generated `insteadOf` rewrites: the exact SSH
    /// spellings of a declared repository resolve to their HTTPS-analog
    /// route (a local `file://` fixture here, so no network is involved),
    /// while `GIT_SSH_COMMAND=false` proves SSH is never attempted.
    #[test]
    fn declared_ssh_urls_rewrite_to_the_https_route() {
        let root = tempfile::tempdir().unwrap();
        let base = init_bare_fixture(root.path(), "fabro-sh/keystone");
        let keystone = slug("fabro-sh/keystone");

        let mut env: HashMap<String, String> = HashMap::new();
        for (offset, (key, value)) in bridge_entries(&[&keystone], &base).into_iter().enumerate() {
            env.insert(format!("GIT_CONFIG_KEY_{offset}"), key);
            env.insert(format!("GIT_CONFIG_VALUE_{offset}"), value);
        }
        env.insert("GIT_CONFIG_COUNT".to_string(), "3".to_string());
        env.insert("GIT_SSH_COMMAND".to_string(), "false".to_string());

        for url in [
            "ssh://git@github.com/fabro-sh/keystone.git",
            "ssh://git@github.com/fabro-sh/keystone",
            "git@github.com:fabro-sh/keystone.git",
            "git@github.com:fabro-sh/keystone",
        ] {
            let output = git(&["ls-remote", url], &env, root.path());
            assert!(
                output.status.success(),
                "{url} should rewrite to the fixture route: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    /// An undeclared SSH URL that shares no declared prefix is not
    /// rewritten: Git still routes it to SSH, where the scripted
    /// `GIT_SSH_COMMAND=false` fails immediately without network access.
    #[test]
    fn undeclared_ssh_urls_are_not_rewritten() {
        let root = tempfile::tempdir().unwrap();
        let base = init_bare_fixture(root.path(), "fabro-sh/keystone");
        let keystone = slug("fabro-sh/keystone");

        let mut env: HashMap<String, String> = HashMap::new();
        for (offset, (key, value)) in bridge_entries(&[&keystone], &base).into_iter().enumerate() {
            env.insert(format!("GIT_CONFIG_KEY_{offset}"), key);
            env.insert(format!("GIT_CONFIG_VALUE_{offset}"), value);
        }
        env.insert("GIT_CONFIG_COUNT".to_string(), "3".to_string());
        env.insert("GIT_SSH_COMMAND".to_string(), "false".to_string());
        env.insert("GITHUB_TOKEN".to_string(), "test-token-value".to_string());

        let output = git(
            &["ls-remote", "git@github.com:fabro-sh/undeclared"],
            &env,
            root.path(),
        );
        assert!(!output.status.success(), "{output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Not rewritten: the failure never mentions the local HTTPS-analog
        // fixture route, so Git still chose the SSH transport.
        assert!(
            !stderr.contains(&root.path().display().to_string()),
            "undeclared URL must not be rewritten to the fixture route: {stderr}"
        );
        assert!(!stderr.contains("test-token-value"), "{stderr}");
    }

    /// Prefix collision: with `fabro-sh/keystone` declared, both SSH
    /// spellings of `fabro-sh/keystone-other` are rewritten to the HTTPS
    /// route (prefix match), where access fails — at GitHub this is an
    /// authorization error for the scoped token — and no token leaks into
    /// the output.
    #[test]
    fn prefix_colliding_undeclared_repositories_rewrite_and_fail_without_token_leak() {
        let root = tempfile::tempdir().unwrap();
        let base = init_bare_fixture(root.path(), "fabro-sh/keystone");
        let keystone = slug("fabro-sh/keystone");

        let mut env: HashMap<String, String> = HashMap::new();
        for (offset, (key, value)) in bridge_entries(&[&keystone], &base).into_iter().enumerate() {
            env.insert(format!("GIT_CONFIG_KEY_{offset}"), key);
            env.insert(format!("GIT_CONFIG_VALUE_{offset}"), value);
        }
        env.insert("GIT_CONFIG_COUNT".to_string(), "3".to_string());
        env.insert("GIT_SSH_COMMAND".to_string(), "false".to_string());
        env.insert("GITHUB_TOKEN".to_string(), "test-token-value".to_string());

        for url in [
            "git@github.com:fabro-sh/keystone-other",
            "ssh://git@github.com/fabro-sh/keystone-other.git",
        ] {
            let output = git(&["ls-remote", url], &env, root.path());
            assert!(!output.status.success(), "{url}: {output:?}");
            let stderr = String::from_utf8_lossy(&output.stderr);
            // The failure names the (missing) HTTPS-analog fixture route,
            // proving the prefix rule rewrote the URL away from SSH.
            assert!(
                stderr.contains("keystone-other"),
                "{url} must be rewritten away from SSH, got: {stderr}"
            );
            assert!(
                stderr.contains(&root.path().display().to_string()),
                "{url} must land on the rewritten route, got: {stderr}"
            );
            assert!(!stderr.contains("test-token-value"), "{stderr}");
        }
    }
}
