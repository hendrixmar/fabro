//! Package workflow versions from caller-supplied file contents instead of a
//! checkout on disk.

use std::collections::BTreeMap;
use std::path::Path;

use fabro_config::project::WorkflowLocation;
use fabro_types::WorkflowPath;
use tempfile::TempDir;

use crate::{CollectedWorkflowClosure, WorkflowVersionCollectError};

type Result<T> = std::result::Result<T, WorkflowVersionCollectError>;

/// Stage `files` in a private temporary directory and collect the workflow
/// closure rooted at `entrypoint` with the same collector used for checkouts.
/// Only supplied files can satisfy references; the staging directory is
/// removed on every return path. Every dependency is validated before this
/// returns and nothing is registered.
pub fn collect_supplied_workflow_versions(
    entrypoint: &WorkflowPath,
    files: &BTreeMap<WorkflowPath, String>,
) -> Result<CollectedWorkflowClosure> {
    let staging = tempfile::Builder::new()
        .prefix("fabro-workflow-version-")
        .tempdir()
        .map_err(stage_error)?;
    collect_in_staging(entrypoint, files, &staging)
}

fn stage_error(source: std::io::Error) -> WorkflowVersionCollectError {
    WorkflowVersionCollectError::Stage { source }
}

fn collect_in_staging(
    entrypoint: &WorkflowPath,
    files: &BTreeMap<WorkflowPath, String>,
    staging: &TempDir,
) -> Result<CollectedWorkflowClosure> {
    let root = staging.path().canonicalize().map_err(stage_error)?;
    for (path, contents) in files {
        let destination = root.join(path.as_str());
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(stage_error)?;
        }
        std::fs::write(destination, contents).map_err(stage_error)?;
    }
    let entrypoint = Path::new(entrypoint.as_str());
    let location =
        WorkflowLocation::from_exact_path(entrypoint, &root).map_err(|source| match source {
            fabro_config::Error::WorkflowNotFound(_) => {
                WorkflowVersionCollectError::WorkflowNotFound {
                    path: entrypoint.to_path_buf(),
                }
            }
            source => WorkflowVersionCollectError::Collect {
                path:   entrypoint.to_path_buf(),
                source: source.into(),
            },
        })?;
    let closure = crate::collect_workflow_versions_at_location(&location, &root, entrypoint)?;
    for (_, version) in closure.versions() {
        confine_to_supplied(version.version(), files, &root)?;
    }
    Ok(closure)
}

/// The collector resolves references against the staged tree, so its result
/// can depend on the host filesystem's case and normalization rules. Reject
/// every version whose collected files are not exactly the supplied keys, and
/// pin the one implicit probe (the sibling `workflow.toml`) to the supplied
/// map so the same request packages identically on every host.
fn confine_to_supplied(
    version: &fabro_types::WorkflowVersion,
    files: &BTreeMap<WorkflowPath, String>,
    root: &Path,
) -> Result<()> {
    // A supplied sibling config attaches only when its `[workflow].graph`
    // selects this entrypoint, exactly as for a checkout; several graphs may
    // share one directory. When no exact sibling was supplied, the probe must
    // not find one either.
    let config_path = version.config_path();
    if !files.contains_key(&config_path) {
        let alias = files.keys().find(|path| {
            fabro_types::validate_workflow_source_paths([*path, &config_path]).is_err()
        });
        if let Some(alias) = alias {
            // A case-insensitive host would probe this key as the config and
            // a case-sensitive one would not; neither outcome is what was
            // asked for.
            return Err(WorkflowVersionCollectError::ConfigAlias {
                config_path,
                alias: alias.clone(),
            });
        }
    } else if !version.files().contains_key(&config_path) {
        // Graph discovery deliberately ignores sibling configs that it cannot
        // load. A supplied config may be omitted only if it is valid and selects
        // a different graph; malformed settings must not silently disappear.
        WorkflowLocation::from_exact_path(Path::new(config_path.as_str()), root).map_err(
            |source| WorkflowVersionCollectError::InvalidSuppliedConfig {
                path:   config_path,
                source: Box::new(source),
            },
        )?;
    }
    // A case-insensitive host must not satisfy a reference that is missing
    // from the supplied tree under its exact key.
    for path in version.files().keys() {
        if !files.contains_key(path) {
            return Err(WorkflowVersionCollectError::NotSupplied { path: path.clone() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use fabro_tool::ValidatedWorkflowVersionCreate as Supplied;
    use fabro_util::error::collect_chain;

    use super::*;
    use crate::test_support::{fixture, source as supplied};

    fn collect(input: &Supplied) -> CollectedWorkflowClosure {
        collect_supplied_workflow_versions(&input.entrypoint, &input.files).unwrap()
    }

    /// Consumes `staging` so the tests can assert cleanup after return.
    fn collect_with_staging(
        input: &Supplied,
        staging: TempDir,
    ) -> Result<CollectedWorkflowClosure> {
        let result = collect_in_staging(&input.entrypoint, &input.files, &staging);
        drop(staging);
        result
    }

    #[test]
    fn supplied_content_matches_checkout_collector_and_cleans_staging() {
        for input in [
            supplied("workflow.fabro", &[("workflow.fabro", "digraph W {}")]),
            fixture(),
        ] {
            let source = tempfile::tempdir().unwrap();
            for (path, content) in &input.files {
                std::fs::write(source.path().join(path.as_str()), content).unwrap();
            }
            let expected = crate::collect_workflow_versions(
                Path::new(input.entrypoint.as_str()),
                source.path(),
            )
            .unwrap();
            let staging = tempfile::tempdir().unwrap();
            let path = staging.path().to_owned();
            let actual = collect_with_staging(&input, staging).unwrap();
            assert!(!path.exists());
            assert_eq!(actual.root_id(), expected.root_id());
            assert_eq!(
                actual
                    .versions()
                    .map(|(_, v)| v.version())
                    .collect::<Vec<_>>(),
                expected
                    .versions()
                    .map(|(_, v)| v.version())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn exact_extensionless_entrypoint_and_child_ignore_selectors() {
        let input = supplied("workflow", &[
            (
                "workflow",
                r#"digraph W { child [stack.child_workflow="child"] }"#,
            ),
            ("child", "digraph Child {}"),
            (
                ".fabro/project.toml",
                "malformed project config must not be read",
            ),
            (
                ".fabro/workflows/workflow/workflow.toml",
                "misleading named workflow",
            ),
        ]);
        let closure = collect(&input);
        let versions = closure.versions().collect::<Vec<_>>();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[1].1.version().entrypoint().as_str(), "workflow");
        assert_eq!(versions[0].1.version().entrypoint().as_str(), "child");
    }

    #[test]
    fn rejects_missing_and_escaping_references_and_cleans_failure() {
        let parent = tempfile::tempdir().unwrap();
        std::fs::write(
            parent.path().join("outside.md"),
            "host content must never satisfy a reference",
        )
        .unwrap();
        std::fs::write(parent.path().join("child.fabro"), "digraph Host {}").unwrap();
        // Malformed on purpose: a parser that reaches this file would quote it.
        std::fs::write(
            parent.path().join("secret.toml"),
            "HOST_SECRET = [unterminated",
        )
        .unwrap();
        std::fs::write(
            parent.path().join("workflow.toml"),
            "HOST_SECRET = [unterminated",
        )
        .unwrap();
        for (index, input) in [
            supplied("workflow.fabro", &[
                ("workflow.fabro", r#"digraph W { p [prompt="@prompt.md"] }"#),
                ("Prompt.md", "wrong case"),
            ]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [prompt="@../outside.md"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [prompt="@sub/../../outside.md"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [prompt="@outside.md"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [output_schema="@../outside.md"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [stack.child_workflow="../child.fabro"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [stack.child_workflow="sub/../../child.fabro"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [stack.child_workflow="../secret.toml"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [stack.child_workflow="../graph.fabro"] }"#,
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                r#"digraph W { p [stack.child_workflow="missing"] }"#,
            )]),
            supplied("workflow.toml", &[(
                "workflow.toml",
                "_version = 1\n[workflow]\ngraph = \"../child.fabro\"\n",
            )]),
            supplied("workflow.fabro", &[(
                "workflow.fabro",
                "invalid source PRIVATE_CONTENT",
            )]),
        ]
        .into_iter()
        .enumerate()
        {
            let staging = tempfile::tempdir_in(parent.path()).unwrap();
            let path = staging.path().to_owned();
            let error = collect_with_staging(&input, staging)
                .err()
                .unwrap_or_else(|| panic!("accepted invalid fixture {index}"));
            let rendered = collect_chain(&error).join(": ");
            // Escaping references must fail before any host file is opened,
            // so no host diagnostic (parse error, exists-vs-missing) leaks.
            assert!(
                !rendered.contains("HOST_SECRET") && !rendered.contains("secret.toml:"),
                "fixture {index} read a host file: {rendered}"
            );
            assert!(!path.exists());
        }
    }

    #[test]
    fn config_entrypoint_must_be_workflow_toml_beside_its_graph() {
        let config = "_version = 1\n[workflow]\ngraph = \"g.fabro\"\n[run]\ngoal = \"hello\"\n";
        let accepted = supplied("sub/workflow.toml", &[
            ("sub/workflow.toml", config),
            ("sub/g.fabro", "digraph W {}"),
        ]);
        let root = collect(&accepted)
            .versions()
            .last()
            .unwrap()
            .1
            .version()
            .clone();
        assert!(root.files().contains_key(&root.config_path()));

        // Same tree under another config name: runtime would never read it.
        let renamed = supplied("sub/run.toml", &[
            ("sub/run.toml", config),
            ("sub/g.fabro", "digraph W {}"),
        ]);
        let error =
            collect_supplied_workflow_versions(&renamed.entrypoint, &renamed.files).unwrap_err();
        let rendered = collect_chain(&error).join(": ");
        assert!(
            rendered.contains("must be `sub/workflow.toml`"),
            "{rendered}"
        );
        // A config that selects a graph in another directory is not its sibling.
        let elsewhere = supplied("sub/workflow.toml", &[
            (
                "sub/workflow.toml",
                "_version = 1\n[workflow]\ngraph = \"../g.fabro\"\n",
            ),
            ("g.fabro", "digraph W {}"),
        ]);
        assert!(
            collect_supplied_workflow_versions(&elsewhere.entrypoint, &elsewhere.files).is_err()
        );
    }

    #[test]
    fn sibling_config_resolution_does_not_depend_on_the_host_filesystem() {
        let graph = r#"digraph W { child [stack.child_workflow="sub/child.fabro"] }"#;
        let selects = |target: &str| format!("_version = 1\n[workflow]\ngraph = \"{target}\"\n");

        // Exact sibling configs attach to the root and to the child.
        let attached = supplied("workflow.fabro", &[
            ("workflow.fabro", graph),
            ("workflow.toml", &selects("workflow.fabro")),
            ("sub/child.fabro", "digraph Child {}"),
            ("sub/workflow.toml", &selects("child.fabro")),
        ]);
        for (_, version) in collect(&attached).versions() {
            let version = version.version();
            assert!(
                version.files().contains_key(&version.config_path()),
                "{} lost its config",
                version.entrypoint()
            );
        }

        // A case variant of the implicit probe name is rejected everywhere,
        // instead of attaching on APFS and vanishing on ext4.
        for (entrypoint, files) in [
            ("workflow.fabro", vec![
                ("workflow.fabro", graph.to_owned()),
                ("Workflow.toml", selects("workflow.fabro")),
                ("sub/child.fabro", "digraph Child {}".to_owned()),
            ]),
            ("workflow.fabro", vec![
                ("workflow.fabro", graph.to_owned()),
                ("sub/child.fabro", "digraph Child {}".to_owned()),
                ("sub/WORKFLOW.toml", selects("child.fabro")),
            ]),
        ] {
            let files = files
                .iter()
                .map(|(path, content)| (*path, content.as_str()))
                .collect::<Vec<_>>();
            let input = supplied(entrypoint, &files);
            let error =
                collect_supplied_workflow_versions(&input.entrypoint, &input.files).unwrap_err();
            assert!(
                matches!(error, WorkflowVersionCollectError::ConfigAlias { .. }),
                "{error}"
            );
        }

        // Several graphs may share a directory: a sibling config attaches only
        // to the graph it selects, as for a checkout.
        let shared_dir = supplied("workflow.fabro", &[
            ("workflow.fabro", graph),
            ("sub/child.fabro", "digraph Child {}"),
            ("sub/other.fabro", "digraph Other {}"),
            ("sub/workflow.toml", &selects("other.fabro")),
        ]);
        let closure = collect(&shared_dir);
        let (_, child) = closure.versions().next().unwrap();
        assert_eq!(child.version().entrypoint().as_str(), "sub/child.fabro");
        assert!(
            !child
                .version()
                .files()
                .contains_key(&child.version().config_path())
        );
    }

    #[test]
    fn preserves_literal_scripts_without_executing() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("must-not-exist");
        // Script is literal command text in Fabro, not an @file import.
        let graph = format!(
            "digraph W {{ command [script=\"touch {}\"] }}",
            marker.display()
        );
        let input = supplied("workflow", &[("workflow", &graph)]);
        let closure = collect(&input);
        let root = closure.versions().last().unwrap().1.version();
        assert_eq!(root.files()[&"workflow".parse().unwrap()], graph);
        assert!(!marker.exists());
    }

    #[test]
    fn map_order_is_irrelevant_and_reachable_changes_change_ids() {
        let input = fixture();
        let first = collect(&input);
        let mut reordered = Supplied {
            entrypoint: input.entrypoint.clone(),
            files:      input.files.into_iter().rev().collect(),
        };
        assert_eq!(first.root_id(), collect(&reordered).root_id());
        reordered
            .files
            .insert("prompt.md".parse().unwrap(), "changed".into());
        let changed = collect(&reordered);
        assert_ne!(first.root_id(), changed.root_id());
        assert_eq!(
            first.versions().next().unwrap().0,
            changed.versions().next().unwrap().0
        );
        reordered
            .files
            .insert("child.fabro".parse().unwrap(), "digraph Changed {}".into());
        let changed_child = collect(&reordered);
        assert_ne!(changed.root_id(), changed_child.root_id());
        assert_ne!(
            changed.versions().next().unwrap().0,
            changed_child.versions().next().unwrap().0
        );
    }
}
