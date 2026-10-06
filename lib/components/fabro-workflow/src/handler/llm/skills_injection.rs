//! Materialize engine skill-library entries into sandbox-local skill
//! directories that harnesses natively load (codex `~/.codex/skills`, omp
//! `~/.omp/agent/skills`, engine `~/.fabro/skills`).
//!
//! Sources per skill name, in order:
//! 1. engine home `~/.fabro/skills/<name>/` (host side, uploaded file by file),
//! 2. sandbox-side `{cwd}/.fabro/skills/<name>/` or `{cwd}/skills/<name>/`
//!    (copied in place).
//!
//! A name found nowhere is skipped with a warning — skills injection is
//! non-fatal and must never block a turn (same contract as the pre-spawn push
//! credential refresh).

use std::path::{Component, Path};

use fabro_util::Home;
use fabro_util::shell::shell_quote;
use pebble_coding_agent::environment::{Environment, ExecRequest};
use tokio::task::spawn_blocking;

/// Which consumer the materialized skills target; picks the directory layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillTarget {
    AcpCodex,
    AcpOmp,
    AcpGeneric,
    ApiDiscovery,
}

impl SkillTarget {
    /// Directory suffix under the sandbox home for this target.
    #[must_use]
    pub fn dir_suffix(&self) -> &'static str {
        match self {
            Self::AcpCodex => ".codex/skills",
            Self::AcpOmp => ".omp/agent/skills",
            Self::AcpGeneric | Self::ApiDiscovery => ".fabro/skills",
        }
    }

    /// Harness label for events/logs.
    #[must_use]
    pub fn harness_label(&self) -> &'static str {
        match self {
            Self::AcpCodex => "codex",
            Self::AcpOmp => "omp",
            Self::AcpGeneric => "generic",
            Self::ApiDiscovery => "api",
        }
    }

    /// Resolve a harness name to its ACP skill target.
    #[must_use]
    pub fn for_harness(harness: &str) -> Self {
        match harness {
            "codex" => Self::AcpCodex,
            "omp" => Self::AcpOmp,
            _ => Self::AcpGeneric,
        }
    }
}

const HOME_PROBE_TIMEOUT_MS: u64 = 5_000;
const COPY_TIMEOUT_MS: u64 = 30_000;

/// Resolve the sandbox's physical home directory. Falls back to the default
/// container home, `/root`, when the probe fails or returns empty.
pub async fn resolve_sandbox_home(sandbox: &dyn Environment) -> String {
    match sandbox
        .exec(ExecRequest {
            timeout_ms: Some(HOME_PROBE_TIMEOUT_MS),
            ..ExecRequest::new(
                r#"if [ -d "$HOME" ]; then (cd -- "$HOME" && pwd -P); else printf %s "$HOME"; fi"#,
            )
        })
        .await
    {
        Ok(outcome)
            if outcome.result.is_success() && outcome.result.stdout.trim().starts_with('/') =>
        {
            outcome.result.stdout.trim().to_string()
        }
        _ => {
            tracing::warn!("sandbox HOME probe failed; falling back to /root");
            "/root".to_string()
        }
    }
}

/// Base directory for a skill target under a resolved sandbox home.
#[must_use]
pub fn skill_target_base(home: &str, target: SkillTarget) -> String {
    format!("{home}/{}", target.dir_suffix())
}

/// Materialize `names` into the target directory; returns the names actually
/// materialized (skips entries found nowhere, never fails the turn).
/// Resolves the engine skill library and sandbox home from the process
/// environment.
pub async fn materialize_skills(
    sandbox: &dyn Environment,
    names: &[String],
    target: SkillTarget,
) -> Vec<String> {
    let home = resolve_sandbox_home(sandbox).await;
    let host_root = Home::from_env().skills_dir();
    materialize_skills_at(sandbox, names, target, &host_root, &home).await
}

/// Testable core of [`materialize_skills`] with explicit library root and
/// sandbox home.
pub async fn materialize_skills_at(
    sandbox: &dyn Environment,
    names: &[String],
    target: SkillTarget,
    host_root: &Path,
    home: &str,
) -> Vec<String> {
    materialize_skills_into(sandbox, names, host_root, &skill_target_base(home, target)).await
}

/// Materialize only selected skills into a caller-owned discovery directory.
pub async fn materialize_skills_into(
    sandbox: &dyn Environment,
    names: &[String],
    host_root: &Path,
    base: &str,
) -> Vec<String> {
    if names.is_empty() {
        return Vec::new();
    }
    // RunSandbox's checkout working directory is an engine-owned link. Resolve
    // that trusted root once; links inside a skill are still always refused.
    let cwd = match sandbox
        .exec(ExecRequest {
            timeout_ms: Some(HOME_PROBE_TIMEOUT_MS),
            ..ExecRequest::new("pwd -P")
        })
        .await
    {
        Ok(outcome)
            if outcome.result.is_success() && outcome.result.stdout.trim().starts_with('/') =>
        {
            outcome.result.stdout.trim().to_string()
        }
        _ => {
            tracing::warn!("sandbox skill root probe failed; skipping materialization");
            return Vec::new();
        }
    };

    let mut materialized = Vec::new();
    for name in names {
        match materialize_one(sandbox, name, host_root, base, &cwd).await {
            Ok(()) => materialized.push(name.clone()),
            Err(reason) => tracing::warn!(
                skill = %name,
                target_dir = %base,
                error = %reason,
                "skill materialization skipped (non-fatal)"
            ),
        }
    }
    materialized
}

async fn materialize_one(
    sandbox: &dyn Environment,
    name: &str,
    host_root: &Path,
    base: &str,
    cwd: &str,
) -> Result<(), String> {
    validate_skill_name(name)?;
    let host_dir = host_root.join(name);
    // Validate and read the complete source before clearing any destination.
    // symlink_metadata sees dangling links too; is_dir would hide them.
    match std::fs::symlink_metadata(&host_dir) {
        Ok(_) => {
            // The configured library is the trust root; reject links beneath
            // its physical location, not harmless aliases of the root itself.
            let root = host_root
                .canonicalize()
                .map_err(|error| error.to_string())?;
            let host_dir = root.join(name);
            reject_host_symlinks(&host_dir)?;
            let source = host_dir.canonicalize().map_err(|error| error.to_string())?;
            if !source.starts_with(&root) || source == root {
                return Err("host skill source escapes library root".to_string());
            }
            let files = spawn_blocking(move || {
                let mut files = Vec::new();
                collect_files(&source, &source, &mut files)?;
                Ok::<_, std::io::Error>(files)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            clear_destination(sandbox, base, name).await?;
            for (relative, content) in files {
                let dest = format!("{base}/{name}/{relative}");
                run_in_sandbox(
                    sandbox,
                    &format!("{PATH_CHECK}\ncheck_path {}", shell_quote(&dest)),
                )
                .await?;
                sandbox
                    .write_file(&dest, &content)
                    .await
                    .map_err(|error| format!("write_file {dest}: {error}"))?;
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }

    for relative in [".fabro/skills", "skills"] {
        let root = format!("{cwd}/{relative}");
        let src = format!("{root}/{name}");
        if sandbox
            .file_exists(&src)
            .await
            .map_err(|error| error.to_string())?
        {
            let dest = format!("{base}/{name}");
            let script = format!(
                "{PATH_CHECK}\n\
                 check_path {root} && check_tree {src} && check_tree {dest} && \
                 mkdir -p -- {base} && rm -rf -- {dest} && cp -R -- {src} {dest}",
                root = shell_quote(&root),
                src = shell_quote(&src),
                dest = shell_quote(&dest),
                base = shell_quote(base),
            );
            run_in_sandbox(sandbox, &script).await?;
            return Ok(());
        }
    }
    Err(format!(
        "skill '{name}' not found in engine library ({}) or repo-local skills",
        host_root.display()
    ))
}

fn validate_skill_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.contains(['\\', '\n', '\r', '\0'])
        || !Path::new(name)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err("skill name must be a relative path without parent components".to_string());
    }
    Ok(())
}

fn reject_host_symlinks(path: &Path) -> Result<(), String> {
    for ancestor in path.ancestors() {
        if std::fs::symlink_metadata(ancestor)
            .map_err(|error| error.to_string())?
            .file_type()
            .is_symlink()
        {
            return Err(format!(
                "symlink in host skill source: {}",
                ancestor.display()
            ));
        }
    }
    Ok(())
}

// Check every existing component, including dangling links, before mkdir,
// remove, copy or write. No realpath fallback may silently follow a link.
const PATH_CHECK: &str = r#"
check_path() {
    local path=$1 part current=''
    local -a parts
    [[ $path == /* && $path != / ]] || return 1
    [[ $path != *$'\n'* && $path != *$'\r'* ]] || return 1
    IFS=/ read -r -a parts <<< "$path"
    for part in "${parts[@]}"; do
        [[ -n $part ]] || continue
        [[ $part != . && $part != .. ]] || return 1
        current="$current/$part"
        [[ ! -L $current ]] || return 1
    done
}
check_tree() {
    local links
    check_path "$1" || return 1
    if [[ -e $1 ]]; then
        [[ -d $1 ]] || return 1
        links=$(find "$1" -type l -print -quit) || return 1
        [[ -z $links ]] || return 1
    fi
}
"#;

async fn clear_destination(
    sandbox: &dyn Environment,
    base: &str,
    name: &str,
) -> Result<(), String> {
    let dest = format!("{base}/{name}");
    run_in_sandbox(
        sandbox,
        &format!(
            "{PATH_CHECK}\ncheck_tree {dest} && mkdir -p -- {base} && rm -rf -- {dest}",
            dest = shell_quote(&dest),
            base = shell_quote(base),
        ),
    )
    .await
}

/// Remove a stage-owned skill selection, refusing links or traversal.
pub async fn remove_skill_selection(sandbox: &dyn Environment, base: &str) -> Result<(), String> {
    run_in_sandbox(
        sandbox,
        &format!(
            "{PATH_CHECK}\ncheck_tree {base} && rm -rf -- {base}",
            base = shell_quote(base),
        ),
    )
    .await
}

async fn run_in_sandbox(sandbox: &dyn Environment, script: &str) -> Result<(), String> {
    match sandbox
        .exec(ExecRequest {
            timeout_ms: Some(COPY_TIMEOUT_MS),
            ..ExecRequest::new(script)
        })
        .await
    {
        Ok(outcome) if outcome.result.is_success() => Ok(()),
        Ok(outcome) => Err(format!(
            "exit {:?}: {}",
            outcome.result.exit_code,
            outcome.result.stderr.trim()
        )),
        Err(error) => Err(error.to_string()),
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "Host skill trees are read inside spawn_blocking before sandbox writes"
)]
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "symlink in host skill source",
            ));
        }
        if file_type.is_dir() {
            collect_files(root, &path, out)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(std::io::Error::other)?
                .to_str()
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "non-UTF-8 skill path")
                })?
                .to_string();
            match std::fs::read_to_string(&path) {
                Ok(content) => out.push((relative, content)),
                Err(error) => {
                    tracing::warn!(file = %path.display(), error = %error, "skipping non-UTF-8 skill file");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pebble_coding_agent::environment::LocalEnvironment;
    use tokio::fs::{read_to_string, write};

    use super::*;

    #[test]
    fn skill_target_dir_suffixes_match_harness_layouts() {
        assert_eq!(SkillTarget::AcpCodex.dir_suffix(), ".codex/skills");
        assert_eq!(SkillTarget::AcpOmp.dir_suffix(), ".omp/agent/skills");
        assert_eq!(SkillTarget::AcpGeneric.dir_suffix(), ".fabro/skills");
        assert_eq!(SkillTarget::ApiDiscovery.dir_suffix(), ".fabro/skills");
        assert_eq!(SkillTarget::for_harness("codex"), SkillTarget::AcpCodex);
        assert_eq!(SkillTarget::for_harness("omp"), SkillTarget::AcpOmp);
        assert_eq!(SkillTarget::for_harness("other"), SkillTarget::AcpGeneric);
        assert_eq!(SkillTarget::AcpCodex.harness_label(), "codex");
    }

    #[tokio::test]
    async fn materializes_host_library_and_repo_local_skills() {
        let sandbox_root = tempfile::tempdir().unwrap();
        let fake_home = tempfile::tempdir().unwrap();
        let engine_home = tempfile::tempdir().unwrap();

        // Engine library carries `tdd` but not `code-review`.
        let tdd_dir = engine_home.path().join("tdd");
        std::fs::create_dir_all(tdd_dir.join("references")).unwrap();
        write(tdd_dir.join("SKILL.md"), "# tdd skill\n")
            .await
            .unwrap();
        write(tdd_dir.join("references").join("guide.md"), "# guide\n")
            .await
            .unwrap();

        // Repo-local skill inside the sandbox working directory.
        let repo_skill = sandbox_root.path().join("skills").join("code-review");
        std::fs::create_dir_all(&repo_skill).unwrap();
        write(repo_skill.join("SKILL.md"), "# code review\n")
            .await
            .unwrap();

        let sandbox = LocalEnvironment::new(sandbox_root.path().to_path_buf());
        let home = fake_home
            .path()
            .canonicalize()
            .unwrap()
            .display()
            .to_string();

        // Codex target: host-library upload, repo-local copy, and a missing
        // name that must be skipped non-fatally.
        let materialized = materialize_skills_at(
            &sandbox,
            &[
                "tdd".to_string(),
                "no-such-skill".to_string(),
                "code-review".to_string(),
            ],
            SkillTarget::AcpCodex,
            engine_home.path(),
            &home,
        )
        .await;
        assert_eq!(materialized, vec![
            "tdd".to_string(),
            "code-review".to_string()
        ]);

        let tdd_skill = fake_home.path().join(".codex/skills/tdd/SKILL.md");
        assert_eq!(read_to_string(&tdd_skill).await.unwrap(), "# tdd skill\n");
        let guide = fake_home
            .path()
            .join(".codex/skills/tdd/references/guide.md");
        assert_eq!(read_to_string(&guide).await.unwrap(), "# guide\n");
        let review = fake_home.path().join(".codex/skills/code-review/SKILL.md");
        assert_eq!(read_to_string(&review).await.unwrap(), "# code review\n");

        // Omp target: same sources land under ~/.omp/agent/skills.
        let materialized = materialize_skills_at(
            &sandbox,
            &["code-review".to_string()],
            SkillTarget::AcpOmp,
            engine_home.path(),
            &home,
        )
        .await;
        assert_eq!(materialized, vec!["code-review".to_string()]);
        let omp_review = fake_home
            .path()
            .join(".omp/agent/skills/code-review/SKILL.md");
        assert_eq!(
            read_to_string(&omp_review).await.unwrap(),
            "# code review\n"
        );

        // Re-materializing into a populated destination stays idempotent
        // (rm -rf before upload/copy, no nested tdd/tdd).
        let materialized = materialize_skills_at(
            &sandbox,
            &["tdd".to_string()],
            SkillTarget::AcpCodex,
            engine_home.path(),
            &home,
        )
        .await;
        assert_eq!(materialized, vec!["tdd".to_string()]);
        let nested = fake_home.path().join(".codex/skills/tdd/tdd");
        assert!(!nested.exists(), "destination must not nest on re-run");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn traversal_and_symlinks_never_mutate_outside_skill_roots() {
        use std::os::unix::fs::symlink;

        let repo = tempfile::tempdir().unwrap();
        let library = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("SKILL.md");
        write(&sentinel, "untouched").await.unwrap();
        let sandbox = LocalEnvironment::new(repo.path());
        let base = home.path().canonicalize().unwrap().join(".codex/skills");
        let destination = base.join("good");
        std::fs::create_dir_all(&destination).unwrap();
        write(destination.join("SKILL.md"), "old").await.unwrap();
        for name in [
            "../good",
            "good/../../good",
            outside.path().to_str().unwrap(),
            "",
            "good\\..\\bad",
        ] {
            assert!(validate_skill_name(name).is_err());
            assert!(
                materialize_one(
                    &sandbox,
                    name,
                    library.path(),
                    base.to_str().unwrap(),
                    repo.path().to_str().unwrap()
                )
                .await
                .is_err()
            );
        }
        assert_eq!(
            read_to_string(destination.join("SKILL.md")).await.unwrap(),
            "old"
        );

        let source = library.path().join("good");
        symlink(outside.path(), &source).unwrap();
        assert!(
            materialize_one(
                &sandbox,
                "good",
                library.path(),
                base.to_str().unwrap(),
                repo.path().to_str().unwrap()
            )
            .await
            .is_err()
        );
        std::fs::remove_file(&source).unwrap();
        std::fs::create_dir(&source).unwrap();
        write(source.join("SKILL.md"), "new").await.unwrap();
        symlink(&sentinel, source.join("escape")).unwrap();
        assert!(
            materialize_one(
                &sandbox,
                "good",
                library.path(),
                base.to_str().unwrap(),
                repo.path().to_str().unwrap()
            )
            .await
            .is_err()
        );
        assert_eq!(
            read_to_string(destination.join("SKILL.md")).await.unwrap(),
            "old"
        );
        std::fs::remove_file(source.join("escape")).unwrap();

        std::fs::remove_dir_all(&destination).unwrap();
        symlink(outside.path(), &destination).unwrap();
        assert!(
            materialize_one(
                &sandbox,
                "good",
                library.path(),
                base.to_str().unwrap(),
                repo.path().to_str().unwrap()
            )
            .await
            .is_err()
        );
        std::fs::remove_file(&destination).unwrap();
        std::fs::remove_dir_all(&base).unwrap();
        symlink(outside.path(), &base).unwrap();
        assert!(
            materialize_one(
                &sandbox,
                "good",
                library.path(),
                base.to_str().unwrap(),
                repo.path().to_str().unwrap()
            )
            .await
            .is_err()
        );
        std::fs::remove_file(&base).unwrap();

        std::fs::remove_dir_all(&source).unwrap();
        std::fs::create_dir_all(repo.path().join("skills")).unwrap();
        symlink(outside.path(), repo.path().join("skills/good")).unwrap();
        assert!(
            materialize_one(
                &sandbox,
                "good",
                library.path(),
                base.to_str().unwrap(),
                repo.path().to_str().unwrap()
            )
            .await
            .is_err()
        );
        assert!(!base.exists(), "source rejection precedes mkdir");
        assert_eq!(read_to_string(&sentinel).await.unwrap(), "untouched");

        // A configured library alias is trusted; links inside it are not.
        std::fs::create_dir(&source).unwrap();
        write(source.join("SKILL.md"), "safe").await.unwrap();
        let alias = repo.path().join("library-alias");
        symlink(library.path(), &alias).unwrap();
        materialize_one(
            &sandbox,
            "good",
            &alias,
            base.to_str().unwrap(),
            repo.path().to_str().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            read_to_string(destination.join("SKILL.md")).await.unwrap(),
            "safe"
        );
        assert_eq!(read_to_string(&sentinel).await.unwrap(), "untouched");
    }

    #[tokio::test]
    async fn resolves_local_sandbox_home_from_env() {
        let sandbox_root = tempfile::tempdir().unwrap();
        let sandbox = LocalEnvironment::new(sandbox_root.path().to_path_buf());
        let home = resolve_sandbox_home(&sandbox).await;
        assert!(!home.is_empty());
        assert!(home.starts_with('/'), "absolute home expected, got {home}");
    }
}
