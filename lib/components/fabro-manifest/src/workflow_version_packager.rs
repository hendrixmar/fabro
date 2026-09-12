//! Application adapter that packages caller-supplied workflow contents for
//! the `fabro_workflow_version_create` tool.

use anyhow::Context as _;
use async_trait::async_trait;
use fabro_tool::{ToolError, ValidatedWorkflowVersionCreate, WorkflowVersionPackager};
use fabro_util::error::collect_chain;
use fabro_workflow_version::{CollectedWorkflowClosure, WorkflowVersionError};
use tokio::task;
use tracing::debug;

use crate::WorkflowVersionCollectError;

/// Packages supplied workflow contents for standalone MCP and capable run
/// workers; the backend that owns the API client performs registration.
pub struct SuppliedWorkflowVersionPackager;

const PACKAGING_HINT: &str = "check configuration, syntax, local references, and package limits";

#[async_trait]
impl WorkflowVersionPackager for SuppliedWorkflowVersionPackager {
    async fn package(
        &self,
        source: ValidatedWorkflowVersionCreate,
    ) -> anyhow::Result<CollectedWorkflowClosure> {
        let packaged = task::spawn_blocking(move || package_blocking(&source))
            .await
            .context("workflow packaging task failed")??;
        Ok(packaged)
    }
}

/// Stage, collect, and validate on the calling thread.
///
/// Packaging failures are expected input errors, so they log at DEBUG. The
/// event carries the collector error's own message only: parser and template
/// diagnostics further down the chain quote supplied file contents, which
/// must not reach the log at any level.
fn package_blocking(
    source: &ValidatedWorkflowVersionCreate,
) -> Result<CollectedWorkflowClosure, ToolError> {
    let closure = crate::collect_supplied_workflow_versions(&source.entrypoint, &source.files)
        .map_err(|err| {
            debug!(
                entrypoint = %source.entrypoint,
                file_count = source.files.len(),
                error = %err,
                "workflow version packaging failed"
            );
            ToolError::message(render_packaging_error(&err))
        })?;
    Ok(closure)
}

/// Render a packaging failure for the tool caller. Every collector variant's
/// own message names paths and counts only, so most render their full cause
/// chain and the caller can fix the input. The graph parser, TOML parser, and
/// template engine quote the offending source in their diagnostics, so
/// failures that reach them stop at the last path-only level and add a hint.
fn render_packaging_error(err: &WorkflowVersionCollectError) -> String {
    let quotes_source = match err {
        WorkflowVersionCollectError::Collect { .. }
        | WorkflowVersionCollectError::InvalidSuppliedConfig { .. } => true,
        WorkflowVersionCollectError::InvalidVersion { source, .. } => matches!(
            source,
            WorkflowVersionError::GraphParse { .. }
                | WorkflowVersionError::Template { .. }
                | WorkflowVersionError::Config { .. }
        ),
        _ => false,
    };
    if !quotes_source {
        return collect_chain(err).join(": ");
    }
    let summary = match err {
        // `WorkflowVersionError` names the offending path; only its source
        // quotes content.
        WorkflowVersionCollectError::InvalidVersion { source, .. } => format!("{err}: {source}"),
        _ => err.to_string(),
    };
    format!("{summary}; {PACKAGING_HINT}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[expect(
        clippy::disallowed_types,
        reason = "test log capture writes synchronously into memory"
    )]
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tracing::{Level, subscriber};

    use super::*;

    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl CapturedLog {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    use crate::test_support::{fixture, source};

    async fn package_error(input: ValidatedWorkflowVersionCreate) -> String {
        let error = SuppliedWorkflowVersionPackager
            .package(input)
            .await
            .unwrap_err();
        format!("{error:#}")
    }

    #[tokio::test]
    async fn deeply_nested_graphs_return_errors_on_the_packaging_thread() {
        for kind in ["child", "import", "mixed"] {
            for count in [
                crate::MAX_WORKFLOW_VERSION_DEPTH,
                crate::MAX_WORKFLOW_VERSION_DEPTH + 1,
                384,
                512,
            ] {
                let files: BTreeMap<_, _> = (0..count)
                    .map(|index| {
                        let attribute = if kind == "import" || (kind == "mixed" && index % 2 == 0) {
                            "import"
                        } else {
                            "stack.child_workflow"
                        };
                        let graph = if index + 1 < count {
                            format!(
                                "digraph W {{ node{index} [{attribute}=\"f{}.fabro\"] }}",
                                index + 1
                            )
                        } else {
                            "digraph W {}".to_owned()
                        };
                        (format!("f{index}.fabro").parse().unwrap(), graph)
                    })
                    .collect();
                let input = ValidatedWorkflowVersionCreate::try_from(
                    fabro_tool::FabroWorkflowVersionCreateParams {
                        entrypoint: "f0.fabro".parse().unwrap(),
                        files,
                    },
                )
                .unwrap();
                let result = SuppliedWorkflowVersionPackager.package(input).await;
                if count == crate::MAX_WORKFLOW_VERSION_DEPTH {
                    assert!(result.is_ok(), "{kind} at limit: {result:?}");
                } else {
                    let error = result.unwrap_err();
                    assert!(
                        error.to_string().contains("exceeds 64 levels"),
                        "{kind}/{count}: {error:#}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn invalid_supplied_sibling_configs_fail_without_quoting_source() {
        for config in [
            "_version = 1\nPRIVATE_CONTENT = [unterminated",
            "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n[run]\ngoal = \"PRIVATE_CONTENT\"\nunknown_setting = true\n",
        ] {
            for child in [false, true] {
                let input = if child {
                    source("root.fabro", &[
                        (
                            "root.fabro",
                            r#"digraph W { child [stack.child_workflow="sub/workflow.fabro"] }"#,
                        ),
                        ("sub/workflow.fabro", "digraph Child {}"),
                        ("sub/workflow.toml", config),
                    ])
                } else {
                    source("workflow.fabro", &[
                        ("workflow.fabro", "digraph W {}"),
                        ("workflow.toml", config),
                    ])
                };
                let error = package_error(input).await;
                let path = if child {
                    "sub/workflow.toml"
                } else {
                    "workflow.toml"
                };
                assert!(
                    error.contains(&format!(
                        "supplied workflow configuration `{path}` is invalid"
                    )),
                    "{error}"
                );
                assert!(!error.contains("PRIVATE_CONTENT"), "{error}");
            }
        }
    }

    #[tokio::test]
    async fn packager_returns_dependencies_before_root() {
        let packaged = SuppliedWorkflowVersionPackager
            .package(fixture())
            .await
            .unwrap();
        let versions = packaged
            .versions()
            .map(|(_, v)| v.version())
            .collect::<Vec<_>>();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].entrypoint().as_str(), "child.fabro");
        assert_eq!(
            versions[1].id().unwrap(),
            packaged.root_id(),
            "root version must be last"
        );
        let child_id = versions[0].id().unwrap();
        assert!(
            versions[1]
                .workflow_dependencies()
                .values()
                .any(|id| *id == child_id)
        );
    }

    #[tokio::test]
    async fn packaging_errors_never_quote_supplied_source() {
        let mut invalid_root = fixture();
        // Child is valid, but the root fails after its dependency is assembled.
        invalid_root.files.insert(
            "workflow.toml".parse().unwrap(),
            "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n[run.goal]\nfile = \"missing.md\""
                .into(),
        );
        let mut invalid_config = fixture();
        invalid_config.files.insert(
            "workflow.toml".parse().unwrap(),
            "_version = 1\nPRIVATE_CONTENT = [unterminated".into(),
        );
        for input in [
            invalid_root,
            invalid_config,
            source("workflow", &[(
                "workflow",
                "PRIVATE_CONTENT invalid source",
            )]),
        ] {
            let rendered = package_error(input).await;
            assert!(!rendered.contains("PRIVATE_CONTENT"), "{rendered}");
        }
    }

    #[test]
    fn packaging_failure_log_never_carries_supplied_source() {
        let log = CapturedLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let inputs = [
            source("workflow", &[(
                "workflow",
                "PRIVATE_CONTENT invalid source",
            )]),
            source("workflow.toml", &[(
                "workflow.toml",
                "_version = 1\nPRIVATE_CONTENT = [unterminated",
            )]),
            source("workflow.fabro", &[
                ("workflow.fabro", "digraph W {}"),
                (
                    "workflow.toml",
                    "_version = 1\nPRIVATE_CONTENT = [unterminated",
                ),
            ]),
        ];
        // The guard is load-bearing: the full chain does quote the source.
        let leaky =
            crate::collect_supplied_workflow_versions(&inputs[0].entrypoint, &inputs[0].files)
                .unwrap_err();
        assert!(collect_chain(&leaky).join(": ").contains("PRIVATE_CONTENT"));
        subscriber::with_default(subscriber, || {
            for input in &inputs {
                package_blocking(input).unwrap_err();
            }
        });
        let text = log.text();
        assert!(text.contains("workflow version packaging failed"), "{text}");
        assert!(!text.contains("PRIVATE_CONTENT"), "{text}");
        assert!(text.contains("DEBUG"), "{text}");
    }

    #[tokio::test]
    async fn path_only_failures_tell_the_caller_what_to_fix() {
        let mut missing_child = fixture();
        missing_child.files.remove(&"child.fabro".parse().unwrap());
        let rendered = package_error(missing_child).await;
        assert!(
            rendered.contains("`child.fabro`") && rendered.contains("missing"),
            "{rendered}"
        );

        let mut wrong_case = fixture();
        let prompt = wrong_case
            .files
            .remove(&"prompt.md".parse().unwrap())
            .unwrap();
        wrong_case
            .files
            .insert("Prompt.md".parse().unwrap(), prompt);
        // A case-insensitive host reads the file and reports the unsupplied
        // key; a case-sensitive host reports it missing. Either names the
        // path the graph asked for.
        let rendered = package_error(wrong_case).await;
        assert!(rendered.contains("`prompt.md`"), "{rendered}");

        let mut oversized = fixture();
        oversized.files.insert(
            "prompt.md".parse().unwrap(),
            "\u{1}".repeat(fabro_types::MAX_WORKFLOW_VERSION_FILE_BYTES - 1),
        );
        let rendered = package_error(oversized).await;
        assert!(rendered.contains("canonical bytes"), "{rendered}");

        let rendered = package_error(source("workflow", &[(
            "workflow",
            "PRIVATE_CONTENT invalid source",
        )]))
        .await;
        assert!(rendered.ends_with(PACKAGING_HINT), "{rendered}");
    }
}
