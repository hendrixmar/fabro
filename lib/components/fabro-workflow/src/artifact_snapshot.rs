use std::path::Path;

use fabro_sandbox::{RunSandbox, SandboxFile, WalkOptions};
use fabro_types::ArtifactUpload;
use fabro_util::workspace_glob::WorkspaceGlobSet;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncReadExt as _;
use tracing::warn;

/// Summary of an artifact collection run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactCollectionSummary {
    pub files_copied:    usize,
    pub total_bytes:     u64,
    pub files_skipped:   usize,
    pub download_errors: usize,
    pub hash_errors:     usize,
    pub captured_assets: Vec<ArtifactUpload>,
}

/// Directories to exclude from artifact traversal and checkpoint commits.
pub const EXCLUDE_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    ".pnpm-store",
    ".npm",
    "target",
    ".next",
    "__pycache__",
    ".venv",
    "venv",
    ".cache",
    ".tox",
    ".pytest_cache",
    ".mypy_cache",
    "dist",
];

/// Maximum number of files to collect.
const MAX_FILE_COUNT: usize = 100;

/// Maximum size for a single file (10 MB).
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// Maximum total size for all collected files (50 MB).
const MAX_TOTAL_SIZE: u64 = 50 * 1024 * 1024;

/// Independent traversal roots may run concurrently, but remote providers
/// should not receive an unbounded burst of file-walk operations.
const MAX_CONCURRENT_ARTIFACT_WALKS: usize = 4;

/// Select which files should be collected based on size budgets.
pub fn select_files_to_collect(discovered: Vec<SandboxFile>) -> Vec<SandboxFile> {
    let mut candidates: Vec<SandboxFile> = discovered
        .into_iter()
        .filter(|file| file.size <= MAX_FILE_SIZE)
        .collect();

    candidates.sort_by(|left, right| {
        left.size
            .cmp(&right.size)
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });

    let mut total = 0;
    let mut selected = Vec::new();
    for file in candidates {
        if selected.len() >= MAX_FILE_COUNT || total + file.size > MAX_TOTAL_SIZE {
            break;
        }
        total += file.size;
        selected.push(file);
    }

    selected
}

async fn compute_artifact_info(
    relative_path: &str,
    local_path: &Path,
) -> std::result::Result<Option<ArtifactUpload>, String> {
    let mime = mime_guess::from_path(relative_path)
        .first_or_octet_stream()
        .to_string();
    let file = fs::File::open(local_path)
        .await
        .map_err(|error| format!("failed to open {}: {error}", local_path.display()))?;
    let mut data = Vec::new();
    file.take(MAX_FILE_SIZE + 1)
        .read_to_end(&mut data)
        .await
        .map_err(|error| format!("failed to read {}: {error}", local_path.display()))?;
    let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
    if bytes > MAX_FILE_SIZE {
        return Ok(None);
    }
    let content_md5 = format!("{:x}", md5::compute(&data));
    let content_sha256 = hex::encode(Sha256::digest(&data));
    Ok(Some(ArtifactUpload {
        path: relative_path.to_string(),
        mime,
        content_md5,
        content_sha256,
        bytes,
    }))
}

/// Collect artifact files matching the configured workspace globs.
pub async fn collect_artifacts(
    sandbox: &RunSandbox,
    artifact_capture_dir: &Path,
    globs: &WorkspaceGlobSet,
) -> Result<ArtifactCollectionSummary, String> {
    let mut walk_options = WalkOptions::default();
    walk_options.exclude_dirs = EXCLUDE_DIRS
        .iter()
        .map(|directory| (*directory).to_string())
        .collect();
    let walk_options = &walk_options;
    let traversal_roots = globs
        .traversal_roots()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let walks = stream::iter(traversal_roots)
        .map(|traversal_root| async move {
            sandbox
                .walk_files(sandbox.working_directory(), &traversal_root, walk_options)
                .await
                .map_err(|error| {
                    format!(
                        "artifact file traversal failed below {traversal_root:?}: {}",
                        error.display_with_causes()
                    )
                })
        })
        .buffer_unordered(MAX_CONCURRENT_ARTIFACT_WALKS)
        .try_collect::<Vec<_>>()
        .await?;
    let discovered = walks
        .into_iter()
        .flatten()
        .filter(|file| globs.is_match(&file.relative_path))
        .collect::<Vec<_>>();

    let total_discovered = discovered.len();
    let to_collect = select_files_to_collect(discovered);
    let mut files_skipped = total_discovered - to_collect.len();

    let mut files_copied = 0;
    let mut total_bytes: u64 = 0;
    let mut download_errors = 0;
    let mut hash_errors = 0;
    let mut captured_assets = Vec::new();

    for file in &to_collect {
        let dest = artifact_capture_dir.join(&file.relative_path);
        match sandbox.download_file_to_local(&file.path, &dest).await {
            Ok(()) => match compute_artifact_info(&file.relative_path, &dest).await {
                Ok(Some(info)) if total_bytes.saturating_add(info.bytes) <= MAX_TOTAL_SIZE => {
                    files_copied += 1;
                    total_bytes += info.bytes;
                    captured_assets.push(info);
                }
                Ok(Some(_) | None) => {
                    let _ = fs::remove_file(&dest).await;
                    files_skipped += 1;
                }
                Err(error) => {
                    warn!(
                        path = file.relative_path.as_str(),
                        error = error.as_str(),
                        "Asset hash failed"
                    );
                    let _ = fs::remove_file(&dest).await;
                    hash_errors += 1;
                }
            },
            Err(error) => {
                let rendered = error.display_with_causes();
                warn!(
                    path = file.relative_path.as_str(),
                    error = rendered.as_str(),
                    "Asset download failed"
                );
                download_errors += 1;
            }
        }
    }

    Ok(ArtifactCollectionSummary {
        files_copied,
        total_bytes,
        files_skipped,
        download_errors,
        hash_errors,
        captured_assets,
    })
}

#[cfg(test)]
#[expect(clippy::disallowed_methods, reason = "tests write fixtures to disk")]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use fabro_sandbox::test_support::MockSandbox;

    use super::*;

    fn sandbox_file(relative_path: &str, size: u64) -> SandboxFile {
        SandboxFile {
            path: format!("/home/test/{relative_path}"),
            relative_path: relative_path.to_string(),
            size,
        }
    }

    fn asset_sandbox(contents: HashMap<String, String>) -> MockSandbox {
        let mut files = HashMap::new();
        let mut discovered = Vec::new();
        for (relative_path, content) in contents {
            let file = sandbox_file(&relative_path, content.len() as u64);
            files.insert(file.path.clone(), content);
            discovered.push(file);
        }

        MockSandbox {
            files,
            ..MockSandbox::linux()
        }
        .with_walk_files(discovered)
    }

    fn workspace_globs(patterns: &[&str]) -> WorkspaceGlobSet {
        WorkspaceGlobSet::try_new(patterns).unwrap()
    }

    #[test]
    fn select_files_skips_oversized_files() {
        let selected = select_files_to_collect(vec![sandbox_file("huge.xml", MAX_FILE_SIZE + 1)]);

        assert!(selected.is_empty());
    }

    #[test]
    fn select_files_sorts_smallest_first() {
        let discovered = vec![
            sandbox_file("a.xml", 3000),
            sandbox_file("b.xml", 1000),
            sandbox_file("c.xml", 2000),
        ];

        let selected = select_files_to_collect(discovered);

        assert_eq!(
            selected
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["b.xml", "c.xml", "a.xml"]
        );
    }

    #[test]
    fn select_files_enforces_total_budget() {
        let discovered = (0..6)
            .map(|index| sandbox_file(&format!("file{index}.xml"), 9 * 1024 * 1024))
            .collect::<Vec<_>>();

        let selected = select_files_to_collect(discovered);

        assert_eq!(selected.len(), 5);
    }

    #[test]
    fn select_files_enforces_count_limit() {
        let discovered = (0..150)
            .map(|index| sandbox_file(&format!("file{index}.txt"), 100))
            .collect::<Vec<_>>();

        let selected = select_files_to_collect(discovered);

        assert_eq!(selected.len(), MAX_FILE_COUNT);
    }

    #[tokio::test]
    async fn collect_artifacts_matches_workspace_relative_paths() {
        let stage_dir = tempfile::tempdir().unwrap();
        let contents = HashMap::from([
            (".ai/reports/summary.md".to_string(), "summary".to_string()),
            (
                ".ai/reports/nested/ignored.md".to_string(),
                "nested".to_string(),
            ),
            (
                ".ai/plans/2026-07-25-globbing.md".to_string(),
                "plan".to_string(),
            ),
            (".ai/plans/DRAFTING.md".to_string(), "drafting".to_string()),
            ("README.md".to_string(), "readme".to_string()),
        ]);
        let sandbox = asset_sandbox(contents);
        let globs = workspace_globs(&[".ai/reports/*.md", ".ai/plans/????-??-??-*.md"]);

        let summary = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .unwrap();

        assert_eq!(summary.files_copied, 2);
        assert_eq!(
            summary
                .captured_assets
                .iter()
                .map(|asset| asset.path.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([".ai/plans/2026-07-25-globbing.md", ".ai/reports/summary.md",])
        );
        assert!(!stage_dir.path().join("manifest.json").exists());
    }

    #[tokio::test]
    async fn collect_artifacts_preserves_content_metadata() {
        let stage_dir = tempfile::tempdir().unwrap();
        let sandbox = asset_sandbox(HashMap::from([(
            "test-results/r.xml".to_string(),
            "<test/>".to_string(),
        )]));
        let globs = workspace_globs(&["test-results/**"]);

        let summary = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .unwrap();

        assert_eq!(summary.files_copied, 1);
        assert_eq!(summary.total_bytes, 7);
        assert_eq!(summary.download_errors, 0);
        assert_eq!(summary.hash_errors, 0);
        assert_eq!(summary.captured_assets.len(), 1);
        let asset = &summary.captured_assets[0];
        assert_eq!(asset.path, "test-results/r.xml");
        assert_eq!(asset.mime, "text/xml");
        assert_eq!(asset.bytes, 7);
        assert_eq!(asset.content_md5, "f1430934c390c118ed2f148e1d44d36c");
        assert_eq!(
            asset.content_sha256,
            "28e51ddac37391b99c2b9053f1122d0bf84b02365e6fd8c6e8667378bd00f436"
        );
        assert_eq!(
            std::fs::read_to_string(stage_dir.path().join("test-results/r.xml")).unwrap(),
            "<test/>"
        );
    }

    #[tokio::test]
    async fn collect_artifacts_rechecks_downloaded_file_size() {
        let stage_dir = tempfile::tempdir().unwrap();
        let content = "x".repeat(usize::try_from(MAX_FILE_SIZE + 1).unwrap());
        let file = sandbox_file("test-results/grew.bin", 1);
        let sandbox = MockSandbox {
            files: HashMap::from([(file.path.clone(), content)]),
            ..MockSandbox::linux()
        }
        .with_walk_files(vec![file]);
        let globs = workspace_globs(&["test-results/**"]);

        let summary = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .unwrap();

        assert_eq!(summary.files_copied, 0);
        assert_eq!(summary.files_skipped, 1);
        assert!(summary.captured_assets.is_empty());
        assert!(!stage_dir.path().join("test-results/grew.bin").exists());
    }

    #[tokio::test]
    async fn collect_artifacts_prunes_dependency_and_build_directories() {
        let stage_dir = tempfile::tempdir().unwrap();
        let sandbox = asset_sandbox(HashMap::from([
            (".ai/reports/keep.md".to_string(), "keep".to_string()),
            ("target/report.md".to_string(), "target".to_string()),
            (
                "nested/node_modules/report.md".to_string(),
                "dependency".to_string(),
            ),
        ]));
        let globs = workspace_globs(&["**/*.md"]);

        let summary = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .unwrap();

        assert_eq!(summary.files_copied, 1);
        assert_eq!(summary.captured_assets[0].path, ".ai/reports/keep.md");
    }

    #[tokio::test]
    async fn collect_artifacts_deduplicates_overlapping_patterns() {
        let stage_dir = tempfile::tempdir().unwrap();
        let sandbox = asset_sandbox(HashMap::from([(
            ".ai/reports/summary.md".to_string(),
            "summary".to_string(),
        )]));
        let globs = workspace_globs(&[".ai/**/*.md", ".ai/reports/*.md"]);

        let summary = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .unwrap();

        assert_eq!(summary.files_copied, 1);
        assert_eq!(summary.captured_assets.len(), 1);
    }

    #[tokio::test]
    async fn collect_artifacts_reports_traversal_errors() {
        let stage_dir = tempfile::tempdir().unwrap();
        let sandbox = asset_sandbox(HashMap::new()).with_walk_files_error("permission denied");
        let globs = workspace_globs(&["test-results/**"]);

        let error = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .expect_err("failed traversal should fail artifact collection");

        assert!(error.contains("artifact file traversal failed"), "{error}");
        assert!(error.contains("permission denied"), "{error}");
    }

    #[tokio::test]
    async fn collect_artifacts_keeps_download_errors_non_fatal() {
        let stage_dir = tempfile::tempdir().unwrap();
        let sandbox = asset_sandbox(HashMap::new()).with_walk_files(vec![
            sandbox_file("test-results/missing.xml", 100),
            sandbox_file("test-results/also-missing.xml", 200),
        ]);
        let globs = workspace_globs(&["test-results/**"]);

        let summary = collect_artifacts(&sandbox.sandbox(), stage_dir.path(), &globs)
            .await
            .unwrap();

        assert_eq!(summary.files_copied, 0);
        assert_eq!(summary.download_errors, 2);
        assert_eq!(summary.hash_errors, 0);
    }
}
