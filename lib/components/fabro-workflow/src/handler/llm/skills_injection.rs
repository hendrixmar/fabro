//! Materialize engine skill-library entries into sandbox-local skill
//! directories that harnesses natively load (codex `~/.codex/skills`, omp
//! `~/.omp/agent/skills`, engine `~/.fabro/skills`).
//!
//! Sources per skill name, in order:
//! 1. engine home `~/.fabro/skills/<name>/` (host side, uploaded file by file),
//! 2. sandbox-side `{cwd}/.fabro/skills/<name>/` or `{cwd}/skills/<name>/`
//!    (copied in place).
//! A name found nowhere is skipped with a warning — skills injection is
//! non-fatal and must never block a turn (same contract as the pre-spawn push
//! credential refresh).

use std::path::Path;

use fabro_agent::{Sandbox, shell_quote};
use fabro_util::Home;

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

/// Resolve the sandbox home directory. Falls back to `/root` (the docker
/// image bakes `/root/.codex/auth.json`, so `/root` is known-good) when the
/// probe fails or returns empty.
pub async fn resolve_sandbox_home(sandbox: &dyn Sandbox) -> String {
    match sandbox
        .exec_command(
            r#"sh -lc 'printf %s "$HOME"'"#,
            HOME_PROBE_TIMEOUT_MS,
            None,
            None,
            None,
        )
        .await
    {
        Ok(result)
            if result.exit_code == Some(0) && !result.stdout.trim().is_empty() =>
        {
            result.stdout.trim().to_string()
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
    sandbox: &dyn Sandbox,
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
    sandbox: &dyn Sandbox,
    names: &[String],
    target: SkillTarget,
    host_root: &Path,
    home: &str,
) -> Vec<String> {
    if names.is_empty() {
        return Vec::new();
    }
    let base = skill_target_base(home, target);
    let cwd = sandbox.working_directory().to_string();

    let mut materialized = Vec::new();
    for name in names {
        match materialize_one(sandbox, name, host_root, &base, &cwd).await {
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
    sandbox: &dyn Sandbox,
    name: &str,
    host_root: &Path,
    base: &str,
    cwd: &str,
) -> Result<(), String> {
    let host_dir = host_root.join(name);
    if host_dir.is_dir() {
        clear_destination(sandbox, base, name).await?;
        upload_dir(sandbox, &host_dir, name, base)
            .await
            .map_err(|error| format!("host upload failed: {error}"))?;
        return Ok(());
    }

    for relative in [".fabro/skills", "skills"] {
        let src = format!("{cwd}/{relative}/{name}");
        if sandbox
            .file_exists(&src)
            .await
            .map_err(|error| format!("file_exists probe failed: {error}"))?
        {
            copy_in_sandbox(sandbox, &src, base, name).await?;
            return Ok(());
        }
    }

    Err(format!(
        "skill '{name}' not found in engine library ({}) or repo-local \
         {{cwd}}/.fabro/skills, {{cwd}}/skills",
        host_root.display()
    ))
}

async fn clear_destination(sandbox: &dyn Sandbox, base: &str, name: &str) -> Result<(), String> {
    let dest = format!("{base}/{name}");
    let script = format!("rm -rf {}", shell_quote(&dest));
    run_in_sandbox(sandbox, &script).await
}

async fn copy_in_sandbox(
    sandbox: &dyn Sandbox,
    src: &str,
    base: &str,
    name: &str,
) -> Result<(), String> {
    let dest = format!("{base}/{name}");
    let script = format!(
        "mkdir -p {} && rm -rf {} && cp -r {} {}",
        shell_quote(base),
        shell_quote(&dest),
        shell_quote(src),
        shell_quote(&dest),
    );
    run_in_sandbox(sandbox, &script).await
}

async fn run_in_sandbox(sandbox: &dyn Sandbox, script: &str) -> Result<(), String> {
    match sandbox
        .exec_command(script, COPY_TIMEOUT_MS, None, None, None)
        .await
    {
        Ok(result) if result.exit_code == Some(0) => Ok(()),
        Ok(result) => Err(format!(
            "exit {:?}: {}",
            result.exit_code,
            result.stderr.trim()
        )),
        Err(error) => Err(error.to_string()),
    }
}

/// Walk a host skill directory and upload every regular UTF-8 file, creating
/// the same relative layout under `{base}/{name}/`.
async fn upload_dir(
    sandbox: &dyn Sandbox,
    dir: &Path,
    name: &str,
    base: &str,
) -> Result<(), String> {
    let mut files = Vec::new();
    collect_files(dir, dir, &mut files).map_err(|error| error.to_string())?;
    for (relative, content) in files {
        let dest = format!("{base}/{name}/{relative}");
        sandbox
            .write_file(&dest, &content)
            .await
            .map_err(|error| format!("write_file {dest}: {error}"))?;
    }
    Ok(())
}

fn collect_files(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(String, String)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_files(root, &path, out)?;
            continue;
        }
        if !file_type.is_file() && !std::fs::metadata(&path).is_ok_and(|meta| meta.is_file()) {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        match std::fs::read_to_string(&path) {
            Ok(content) => out.push((relative, content)),
            Err(error) => tracing::warn!(
                file = %path.display(),
                error = %error,
                "skipping non-UTF-8 skill file"
            ),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use fabro_agent::LocalSandbox;

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
        std::fs::write(tdd_dir.join("SKILL.md"), "# tdd skill\n").unwrap();
        std::fs::write(tdd_dir.join("references").join("guide.md"), "# guide\n").unwrap();

        // Repo-local skill inside the sandbox working directory.
        let repo_skill = sandbox_root.path().join("skills").join("code-review");
        std::fs::create_dir_all(&repo_skill).unwrap();
        std::fs::write(repo_skill.join("SKILL.md"), "# code review\n").unwrap();

        let sandbox = LocalSandbox::new(sandbox_root.path().to_path_buf());
        let home = fake_home.path().to_str().unwrap().to_string();

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
        assert_eq!(
            materialized,
            vec!["tdd".to_string(), "code-review".to_string()]
        );

        let tdd_skill = fake_home.path().join(".codex/skills/tdd/SKILL.md");
        assert_eq!(
            std::fs::read_to_string(&tdd_skill).unwrap(),
            "# tdd skill\n"
        );
        let guide = fake_home
            .path()
            .join(".codex/skills/tdd/references/guide.md");
        assert_eq!(std::fs::read_to_string(&guide).unwrap(), "# guide\n");
        let review = fake_home.path().join(".codex/skills/code-review/SKILL.md");
        assert_eq!(
            std::fs::read_to_string(&review).unwrap(),
            "# code review\n"
        );

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
            std::fs::read_to_string(&omp_review).unwrap(),
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

    #[tokio::test]
    async fn resolves_local_sandbox_home_from_env() {
        let sandbox_root = tempfile::tempdir().unwrap();
        let sandbox = LocalSandbox::new(sandbox_root.path().to_path_buf());
        let home = resolve_sandbox_home(&sandbox).await;
        assert!(!home.is_empty());
        assert!(home.starts_with('/'), "absolute home expected, got {home}");
    }
}
