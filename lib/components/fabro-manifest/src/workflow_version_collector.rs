use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use fabro_api::types;
use fabro_config::project::WorkflowLocation;
use fabro_types::{
    WorkflowPath, WorkflowPathParseError, WorkflowVersion, WorkflowVersionId,
    WorkflowVersionShapeError,
};
use fabro_workflow_version::{
    CollectedWorkflowClosure, ValidatedWorkflowVersion, WorkflowVersionError,
};
use thiserror::Error;

use crate::workflow_bundler::{CollectedWorkflowSource, CollectedWorkflowSources, WorkflowBundler};

/// Maximum active graph nesting while collecting or assembling a version.
/// Bounds native stack use independently of file-count and byte budgets.
pub const MAX_WORKFLOW_VERSION_DEPTH: usize = 64;

pub(super) fn check_workflow_depth(
    depth: usize,
    path: &str,
) -> Result<(), WorkflowVersionCollectError> {
    if depth > MAX_WORKFLOW_VERSION_DEPTH {
        return Err(WorkflowVersionCollectError::DepthExceeded {
            path:    path.to_owned(),
            maximum: MAX_WORKFLOW_VERSION_DEPTH,
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum WorkflowVersionCollectError {
    #[error("workflow dependency nesting at `{path}` exceeds {maximum} levels")]
    DepthExceeded { path: String, maximum: usize },
    #[error("workflow `{path}` was not found")]
    WorkflowNotFound { path: PathBuf },
    #[error("failed to collect workflow `{path}`")]
    Collect {
        path:   PathBuf,
        #[source]
        source: anyhow::Error,
    },
    #[error("collected workflow path `{path}` is invalid")]
    InvalidPath {
        path:   String,
        #[source]
        source: WorkflowPathParseError,
    },
    #[error("collected workflow `{entrypoint}` has conflicting content at `{path}`")]
    PathCollision {
        entrypoint: WorkflowPath,
        path:       WorkflowPath,
    },
    #[error("collected workflow `{entrypoint}` has an invalid shape")]
    InvalidShape {
        entrypoint: WorkflowPath,
        #[source]
        source:     WorkflowVersionShapeError,
    },
    #[error("collected workflow `{entrypoint}` is invalid")]
    InvalidVersion {
        entrypoint: WorkflowPath,
        #[source]
        source:     WorkflowVersionError,
    },
    #[error("workflow dependency cycle reaches `{path}`")]
    DependencyCycle { path: WorkflowPath },
    #[error("collected workflow dependency `{path}` is missing")]
    MissingWorkflow { path: String },
    /// A referenced file is absent from the package root. Surfaced separately
    /// from [`Self::Collect`] because the path is the whole message.
    #[error("referenced file `{path}` is missing from the workflow source")]
    MissingPackageFile { path: String },
    /// Supplied-content packaging: the collector read a file whose exact key
    /// the caller did not supply (a case-insensitive host satisfied a
    /// reference that differs from the supplied key).
    #[error("collected file `{path}` was not supplied")]
    NotSupplied { path: WorkflowPath },
    /// Supplied-content packaging: a supplied key differs from the implicit
    /// sibling config name `{config_path}` only by case or normalization, so
    /// hosts with different filesystem rules would package different trees.
    #[error("supplied file `{alias}` aliases the workflow config name `{config_path}`")]
    ConfigAlias {
        config_path: WorkflowPath,
        alias:       WorkflowPath,
    },
    #[error("supplied workflow configuration `{path}` is invalid")]
    InvalidSuppliedConfig {
        path:   WorkflowPath,
        #[source]
        source: Box<fabro_config::Error>,
    },
    #[error("failed to stage supplied workflow files")]
    Stage {
        #[source]
        source: std::io::Error,
    },
}

/// Package one workflow and every separately runnable dependency from a local
/// checkout. An extensionless selector names
/// `.fabro/workflows/<selector>/workflow.toml` directly; repository project
/// settings and user workflows do not participate in selection. All paths are
/// rooted at `checkout_root`, so moving the physical checkout does not change
/// canonical version bytes or IDs.
pub fn collect_workflow_versions(
    workflow: &Path,
    checkout_root: &Path,
) -> Result<CollectedWorkflowClosure, WorkflowVersionCollectError> {
    let repository_workflow = repository_workflow_path(workflow);
    let package_root =
        checkout_root
            .canonicalize()
            .map_err(|source| WorkflowVersionCollectError::Collect {
                path:   workflow.to_path_buf(),
                source: anyhow::Error::new(source).context(format!(
                    "failed to canonicalize workflow package root {}",
                    checkout_root.display()
                )),
            })?;
    ensure_selection_contained(
        &checkout_root.join(&repository_workflow),
        &package_root,
        workflow,
    )?;
    let location = crate::resolve_existing_workflow_location(&repository_workflow, checkout_root)
        .map_err(|source| match source {
        fabro_config::Error::WorkflowNotFound(_) => WorkflowVersionCollectError::WorkflowNotFound {
            path: workflow.to_path_buf(),
        },
        source => WorkflowVersionCollectError::Collect {
            path:   workflow.to_path_buf(),
            source: source.into(),
        },
    })?;
    let location = canonicalize_location(location, |path, source| {
        WorkflowVersionCollectError::Collect {
            path:   workflow.to_path_buf(),
            source: anyhow::Error::new(source).context(format!(
                "failed to canonicalize workflow path {}",
                path.display()
            )),
        }
    })?;
    collect_workflow_versions_at_location(&location, &package_root, workflow)
}

/// Canonicalize a resolved workflow location so its paths compare against a
/// canonical package root. `map_err` receives the path that failed.
pub(super) fn canonicalize_location<E>(
    location: WorkflowLocation,
    map_err: impl Fn(&Path, std::io::Error) -> E,
) -> Result<WorkflowLocation, E> {
    let canonicalize = |path: &Path| path.canonicalize().map_err(|source| map_err(path, source));
    let graph = canonicalize(&location.graph)?;
    let toml = location.toml.as_deref().map(canonicalize).transpose()?;
    let dir = graph
        .parent()
        .expect("a canonical workflow graph has a parent")
        .to_path_buf();
    Ok(WorkflowLocation {
        dir,
        graph,
        toml,
        slug: location.slug,
    })
}

/// Collect an already resolved exact location inside a canonical package root.
/// Every dependency is validated before this returns; no registration occurs.
pub fn collect_workflow_versions_at_location(
    location: &WorkflowLocation,
    package_root: &Path,
    workflow: &Path,
) -> Result<CollectedWorkflowClosure, WorkflowVersionCollectError> {
    let inputs = HashMap::new();
    let collected = WorkflowBundler::new(package_root, &inputs)
        .collect_versions(location)
        .map_err(|source| {
            source
                .downcast::<WorkflowVersionCollectError>()
                .unwrap_or_else(|source| WorkflowVersionCollectError::Collect {
                    path: workflow.to_path_buf(),
                    source,
                })
        })?;
    VersionAssembler::new(collected).assemble()
}

/// Location resolution reads the selected TOML, or a selected graph's sibling
/// `workflow.toml`, before the bundler's root-checked reads begin. Refuse a
/// selection whose file resolves outside the package first, so a symlink in an
/// untrusted checkout never reads host content. Missing files are left for
/// resolution to report; symlinks elsewhere in the checkout are irrelevant
/// because every file the bundler reads is checked when it is opened.
fn ensure_selection_contained(
    selected: &Path,
    package_root: &Path,
    workflow: &Path,
) -> Result<(), WorkflowVersionCollectError> {
    let mut candidates = vec![selected.to_path_buf()];
    if selected
        .extension()
        .is_none_or(|extension| extension != "toml")
    {
        candidates.push(selected.with_file_name("workflow.toml"));
    }
    for path in candidates {
        if path.symlink_metadata().is_err() {
            continue;
        }
        let canonical =
            path.canonicalize()
                .map_err(|source| WorkflowVersionCollectError::Collect {
                    path:   workflow.to_path_buf(),
                    source: anyhow::Error::new(source).context(format!(
                        "failed to canonicalize workflow file {}",
                        path.display()
                    )),
                })?;
        if !canonical.starts_with(package_root) {
            return Err(WorkflowVersionCollectError::Collect {
                path:   workflow.to_path_buf(),
                source: anyhow::anyhow!(
                    "workflow file `{}` resolves outside its source root `{}`",
                    path.display(),
                    package_root.display()
                ),
            });
        }
    }
    Ok(())
}

fn repository_workflow_path(workflow: &Path) -> PathBuf {
    if workflow.is_relative() && workflow.extension().is_none() {
        Path::new(".fabro/workflows")
            .join(workflow)
            .join("workflow.toml")
    } else {
        workflow.to_path_buf()
    }
}

struct VersionAssembler {
    root_key: String,
    /// Sources still waiting to be assembled; each is removed once visited.
    pending:  HashMap<String, CollectedWorkflowSource>,
    visiting: HashSet<String>,
    ids:      HashMap<String, WorkflowVersionId>,
    versions: Vec<(WorkflowVersionId, ValidatedWorkflowVersion)>,
}

impl VersionAssembler {
    fn new(collected: CollectedWorkflowSources) -> Self {
        Self {
            root_key: collected.root_key,
            pending:  collected.workflows,
            visiting: HashSet::new(),
            ids:      HashMap::new(),
            versions: Vec::new(),
        }
    }

    fn assemble(mut self) -> Result<CollectedWorkflowClosure, WorkflowVersionCollectError> {
        let root_key = std::mem::take(&mut self.root_key);
        let root_id = self.assemble_one(&root_key)?;
        Ok(CollectedWorkflowClosure::from_dependency_order(
            root_id,
            self.versions,
        ))
    }

    fn assemble_one(
        &mut self,
        key: &str,
    ) -> Result<WorkflowVersionId, WorkflowVersionCollectError> {
        if let Some(id) = self.ids.get(key) {
            return Ok(*id);
        }
        check_workflow_depth(self.visiting.len() + 1, key)?;
        if !self.visiting.insert(key.to_owned()) {
            return Err(WorkflowVersionCollectError::DependencyCycle {
                path: workflow_path(key)?,
            });
        }
        let source = self.pending.remove(key).ok_or_else(|| {
            WorkflowVersionCollectError::MissingWorkflow {
                path: key.to_owned(),
            }
        })?;

        let mut dependencies = BTreeMap::new();
        for dependency_key in &source.dependency_keys {
            let dependency_id = self.assemble_one(dependency_key)?;
            dependencies.insert(workflow_path(dependency_key)?, dependency_id);
        }

        let entrypoint = workflow_path(key)?;
        let files = workflow_files(&entrypoint, source.workflow)?;
        let version =
            WorkflowVersion::new(entrypoint.clone(), files, dependencies).map_err(|source| {
                WorkflowVersionCollectError::InvalidShape {
                    entrypoint: entrypoint.clone(),
                    source,
                }
            })?;
        let validated = ValidatedWorkflowVersion::new(version).map_err(|source| {
            WorkflowVersionCollectError::InvalidVersion {
                entrypoint: entrypoint.clone(),
                source,
            }
        })?;
        let id = validated.version().id().map_err(|source| {
            WorkflowVersionCollectError::InvalidShape {
                entrypoint: entrypoint.clone(),
                source,
            }
        })?;

        self.visiting.remove(key);
        // Keys are distinct entrypoints and the entrypoint is part of the
        // canonical bytes, so each key yields a distinct ID.
        self.ids.insert(key.to_owned(), id);
        self.versions.push((id, validated));
        Ok(id)
    }
}

fn workflow_files(
    entrypoint: &WorkflowPath,
    workflow: types::ManifestWorkflow,
) -> Result<BTreeMap<WorkflowPath, String>, WorkflowVersionCollectError> {
    let mut files = BTreeMap::new();
    insert_file(&mut files, entrypoint, entrypoint.clone(), workflow.source)?;
    if let Some(config) = workflow.config {
        insert_file(
            &mut files,
            entrypoint,
            workflow_path(&config.path)?,
            config.source,
        )?;
    }
    for (path, file) in workflow.files {
        insert_file(&mut files, entrypoint, workflow_path(&path)?, file.content)?;
    }
    Ok(files)
}

fn insert_file(
    files: &mut BTreeMap<WorkflowPath, String>,
    entrypoint: &WorkflowPath,
    path: WorkflowPath,
    content: String,
) -> Result<(), WorkflowVersionCollectError> {
    if let Some(existing) = files.get(&path) {
        if existing == &content {
            return Ok(());
        }
        return Err(WorkflowVersionCollectError::PathCollision {
            entrypoint: entrypoint.clone(),
            path,
        });
    }
    files.insert(path, content);
    Ok(())
}

fn workflow_path(value: &str) -> Result<WorkflowPath, WorkflowVersionCollectError> {
    WorkflowPath::new(value).map_err(|source| WorkflowVersionCollectError::InvalidPath {
        path: value.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::disallowed_methods,
        reason = "collector tests write small temporary workflow fixtures synchronously"
    )]

    use std::fs;

    use super::*;

    #[test]
    fn version_assembly_bounds_its_own_dependency_traversal() {
        // Collection and assembly visit shared dependencies in different orders.
        // Assembly must bound its stack even if collection already cached nodes.
        for count in [MAX_WORKFLOW_VERSION_DEPTH, MAX_WORKFLOW_VERSION_DEPTH + 1] {
            let workflows = (0..count)
                .map(|index| {
                    let child = (index + 1 < count).then(|| format!("f{}.fabro", index + 1));
                    let source = child.as_ref().map_or_else(
                        || "digraph W {}".to_owned(),
                        |child| format!("digraph W {{ child [stack.child_workflow=\"{child}\"] }}"),
                    );
                    (format!("f{index}.fabro"), CollectedWorkflowSource {
                        workflow:        types::ManifestWorkflow {
                            config: None,
                            files: HashMap::new(),
                            source,
                        },
                        dependency_keys: child.into_iter().collect(),
                    })
                })
                .collect();
            let result = VersionAssembler::new(CollectedWorkflowSources {
                root_key: "f0.fabro".to_owned(),
                workflows,
            })
            .assemble();
            if count == MAX_WORKFLOW_VERSION_DEPTH {
                assert_eq!(result.unwrap().versions().count(), count);
            } else {
                assert!(matches!(
                    result,
                    Err(WorkflowVersionCollectError::DepthExceeded { .. })
                ));
            }
        }
    }

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        fs::create_dir_all(path.parent().expect("fixture path should have a parent")).unwrap();
        fs::write(path, content).unwrap();
    }

    fn write_complete_fixture(root: &Path) {
        write(
            root,
            ".fabro/workflows/root/workflow.toml",
            r#"_version = 1
[workflow]
graph = "workflow.fabro"
[run.goal]
file = "goal.md"
[run.environment.image]
dockerfile = { path = "Dockerfile" }
"#,
        );
        write(
            root,
            ".fabro/workflows/root/workflow.fabro",
            r#"digraph Root {
                graph [goal="@graph-goal.md"]
                prompt [prompt="@prompts/task.md"]
                child [stack.child_workflow="../child/workflow.fabro"]
            }"#,
        );
        write(
            root,
            ".fabro/workflows/root/goal.md",
            r#"Ship it. {% include "shared.md" %}"#,
        );
        write(root, ".fabro/workflows/root/shared.md", "shared");
        write(root, ".fabro/workflows/root/Dockerfile", "FROM alpine\n");
        write(root, ".fabro/workflows/root/graph-goal.md", "graph goal");
        write(
            root,
            ".fabro/workflows/root/prompts/task.md",
            r#"Task {% include "detail.md" %}"#,
        );
        write(root, ".fabro/workflows/root/prompts/detail.md", "detail");
        write(
            root,
            ".fabro/workflows/child/workflow.fabro",
            "digraph Child {}",
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_selected_files_that_resolve_outside_the_package_root() {
        let host = tempfile::tempdir().unwrap();
        write(host.path(), "workflow.toml", "_version = 1\n");
        for selector in ["root", ".fabro/workflows/root/workflow.fabro"] {
            let temp = tempfile::tempdir().unwrap();
            write_complete_fixture(temp.path());
            let toml = temp.path().join(".fabro/workflows/root/workflow.toml");
            fs::remove_file(&toml).unwrap();
            std::os::unix::fs::symlink(host.path().join("workflow.toml"), &toml).unwrap();
            let error = collect_workflow_versions(Path::new(selector), temp.path()).unwrap_err();
            assert!(
                format!("{error:?}").contains("outside its source root"),
                "{selector}: {error:?}"
            );
            // A dangling selection is refused rather than reported as missing.
            fs::remove_file(&toml).unwrap();
            std::os::unix::fs::symlink(host.path().join("missing.toml"), &toml).unwrap();
            let error = collect_workflow_versions(Path::new(selector), temp.path()).unwrap_err();
            assert!(
                format!("{error:?}").contains("failed to canonicalize workflow file"),
                "{selector}: {error:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn ignores_symlinks_the_selected_workflow_never_reads() {
        let host = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        write_complete_fixture(temp.path());
        std::os::unix::fs::symlink(host.path(), temp.path().join("tools")).unwrap();
        std::os::unix::fs::symlink("../missing", temp.path().join("vendor")).unwrap();
        std::os::unix::fs::symlink(
            "/nonexistent/module",
            temp.path().join(".fabro/workflows/root/unrelated"),
        )
        .unwrap();
        let closure = collect_workflow_versions(Path::new("root"), temp.path()).unwrap();
        assert_eq!(closure.versions().count(), 2);
    }

    #[test]
    fn packages_named_workflow_without_project_config() {
        let temp = tempfile::tempdir().unwrap();
        write_complete_fixture(temp.path());

        let closure = collect_workflow_versions(Path::new("root"), temp.path()).unwrap();
        let versions = closure.versions().collect::<Vec<_>>();

        assert_eq!(versions.len(), 2);
        let (child_id, child) = versions[0];
        assert_eq!(
            child.version().entrypoint().as_str(),
            ".fabro/workflows/child/workflow.fabro"
        );
        let (root_id, root) = versions[1];
        assert_eq!(root_id, closure.root_id());
        assert_eq!(
            root.version().workflow_dependencies(),
            &BTreeMap::from([(
                WorkflowPath::new(".fabro/workflows/child/workflow.fabro").unwrap(),
                child_id,
            )])
        );
        for path in [
            ".fabro/workflows/root/workflow.fabro",
            ".fabro/workflows/root/workflow.toml",
            ".fabro/workflows/root/goal.md",
            ".fabro/workflows/root/shared.md",
            ".fabro/workflows/root/Dockerfile",
            ".fabro/workflows/root/graph-goal.md",
            ".fabro/workflows/root/prompts/task.md",
            ".fabro/workflows/root/prompts/detail.md",
        ] {
            assert!(
                root.version()
                    .files()
                    .contains_key(&WorkflowPath::new(path).unwrap()),
                "missing {path}"
            );
        }
    }

    #[test]
    fn package_identity_does_not_depend_on_checkout_location() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        write_complete_fixture(first.path());
        write_complete_fixture(second.path());

        let first = collect_workflow_versions(Path::new("root"), first.path()).unwrap();
        let second = collect_workflow_versions(Path::new("root"), second.path()).unwrap();

        assert_eq!(first.root_id(), second.root_id());
        assert_eq!(
            first
                .versions()
                .map(|(id, version)| (id, version.version().canonical_bytes().unwrap()))
                .collect::<Vec<_>>(),
            second
                .versions()
                .map(|(id, version)| (id, version.version().canonical_bytes().unwrap()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn named_and_explicit_selectors_ignore_project_config() {
        let temp = tempfile::tempdir().unwrap();
        write_complete_fixture(temp.path());

        let named = collect_workflow_versions(Path::new("root"), temp.path()).unwrap();
        write(
            temp.path(),
            ".fabro/project.toml",
            "this project config must not be loaded",
        );
        let explicit = collect_workflow_versions(
            Path::new(".fabro/workflows/root/workflow.toml"),
            temp.path(),
        )
        .unwrap();

        assert_eq!(named.root_id(), explicit.root_id());
        assert_eq!(
            named
                .versions()
                .map(|(id, version)| (id, version.version().canonical_bytes().unwrap()))
                .collect::<Vec<_>>(),
            explicit
                .versions()
                .map(|(id, version)| (id, version.version().canonical_bytes().unwrap()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn packages_nested_imported_and_diamond_dependencies_deterministically() {
        let temp = tempfile::tempdir().unwrap();
        write(
            temp.path(),
            ".fabro/workflows/root/workflow.toml",
            "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n",
        );
        write(
            temp.path(),
            ".fabro/workflows/root/workflow.fabro",
            r#"digraph Root {
                imported [import="imports/extension.fabro"]
                left [stack.child_workflow="../left/workflow.fabro"]
                right [stack.child_workflow="../right/workflow.fabro"]
            }"#,
        );
        write(
            temp.path(),
            ".fabro/workflows/root/imports/extension.fabro",
            r#"digraph Extension {
                child [stack.child_workflow="../../imported-child/workflow.fabro"]
            }"#,
        );
        write(
            temp.path(),
            ".fabro/workflows/imported-child/workflow.fabro",
            "digraph ImportedChild {}",
        );
        write(
            temp.path(),
            ".fabro/workflows/left/workflow.fabro",
            r#"digraph Left { nested [stack.child_workflow="../nested/workflow.fabro"] }"#,
        );
        write(
            temp.path(),
            ".fabro/workflows/nested/workflow.fabro",
            r#"digraph Nested { shared [stack.child_workflow="../shared/workflow.fabro"] }"#,
        );
        write(
            temp.path(),
            ".fabro/workflows/right/workflow.fabro",
            r#"digraph Right { shared [stack.child_workflow="../shared/workflow.fabro"] }"#,
        );
        write(
            temp.path(),
            ".fabro/workflows/shared/workflow.fabro",
            "digraph Shared {}",
        );

        let first = collect_workflow_versions(Path::new("root"), temp.path()).unwrap();
        let second = collect_workflow_versions(Path::new("root"), temp.path()).unwrap();
        let entrypoint_order = |closure: &CollectedWorkflowClosure| {
            closure
                .versions()
                .map(|(_, version)| version.version().entrypoint().as_str().to_owned())
                .collect::<Vec<_>>()
        };

        let expected = vec![
            ".fabro/workflows/imported-child/workflow.fabro",
            ".fabro/workflows/shared/workflow.fabro",
            ".fabro/workflows/nested/workflow.fabro",
            ".fabro/workflows/left/workflow.fabro",
            ".fabro/workflows/right/workflow.fabro",
            ".fabro/workflows/root/workflow.fabro",
        ];
        assert_eq!(entrypoint_order(&first), expected);
        assert_eq!(entrypoint_order(&second), expected);
        assert_eq!(first.root_id(), second.root_id());
        assert_eq!(
            entrypoint_order(&first)
                .iter()
                .filter(|path| path.as_str() == ".fabro/workflows/shared/workflow.fabro")
                .count(),
            1
        );
        let root = first.versions().last().unwrap().1;
        assert!(root.version().files().contains_key(
            &WorkflowPath::new(".fabro/workflows/root/imports/extension.fabro").unwrap()
        ));
        assert_eq!(root.version().workflow_dependencies().len(), 3);
    }

    #[test]
    fn rejects_dependency_cycles() {
        let temp = tempfile::tempdir().unwrap();
        write(
            temp.path(),
            ".fabro/workflows/root/workflow.toml",
            "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n",
        );
        write(
            temp.path(),
            ".fabro/workflows/child/workflow.toml",
            "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n",
        );
        write(
            temp.path(),
            ".fabro/workflows/root/workflow.fabro",
            r#"digraph Root { child [stack.child_workflow="../child/workflow.fabro"] }"#,
        );
        write(
            temp.path(),
            ".fabro/workflows/child/workflow.fabro",
            r#"digraph Child { root [stack.child_workflow="../root/workflow.fabro"] }"#,
        );

        let error = collect_workflow_versions(Path::new("root"), temp.path()).unwrap_err();

        assert!(
            matches!(&error, WorkflowVersionCollectError::DependencyCycle { .. }),
            "unexpected error: {error:?}"
        );
    }
}
