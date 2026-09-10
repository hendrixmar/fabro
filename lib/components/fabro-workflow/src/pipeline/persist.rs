use std::path::Path;

use super::types::{PersistOptions, Persisted, Validated};
use crate::error::Error;
use crate::records::RunSpec;
use crate::runtime_store::RunStoreHandle;

/// PERSIST phase: create the run directory and return durable metadata for
/// store persistence.
pub(crate) fn persist(
    validated: Validated,
    mut options: PersistOptions,
) -> Result<Persisted, Error> {
    let (graph, source, diagnostics) = validated.into_parts();
    options.run_spec.graph = graph.clone();

    std::fs::create_dir_all(&options.run_dir).map_err(|err| {
        Error::Io(format!(
            "creating run directory {}: {err}",
            options.run_dir.display()
        ))
    })?;

    Ok(Persisted::new(
        graph,
        source,
        diagnostics,
        options.run_dir,
        options.run_spec,
    ))
}

pub(crate) async fn load_from_store(
    run_store: &RunStoreHandle,
    run_dir: &Path,
) -> Result<Persisted, Error> {
    let state = run_store
        .state()
        .await
        .map_err(|err| Error::engine(err.to_string()))?;
    let run_spec = executable_run_spec(run_store, state.spec).await?;
    let graph = run_spec.graph.clone();
    let source = run_spec.graph_source.clone().unwrap_or_default();

    Ok(Persisted::new(
        graph,
        source,
        Vec::new(),
        run_dir.to_path_buf(),
        run_spec,
    ))
}

/// Replace the event-folded spec content with the exact bytes from the spec
/// blob. Stored events pass through secret redaction, so the folded spec is
/// display data; the blob written at creation is what execution must see.
/// Runs created before the blob existed fall back to the folded spec.
async fn executable_run_spec(
    run_store: &RunStoreHandle,
    folded: RunSpec,
) -> Result<RunSpec, Error> {
    let Some(blob_id) = folded.spec_blob else {
        return Ok(folded);
    };
    let bytes = run_store
        .read_blob(&blob_id)
        .await
        .map_err(|err| Error::engine_with_anyhow("failed to read run spec blob", err))?
        .ok_or_else(|| {
            Error::engine(format!(
                "run spec blob is missing from the run store: {blob_id}"
            ))
        })?;
    let mut spec: RunSpec = serde_json::from_slice(&bytes)
        .map_err(|err| Error::engine_with_source("run spec blob was not valid JSON", err))?;
    // The event stream stays authoritative for run identity, provenance, and
    // blob ids. Prefer the unredacted graph source from the blob, with the
    // folded source as a compatibility fallback.
    spec.run_id = folded.run_id;
    spec.provenance = folded.provenance;
    spec.manifest_blob = folded.manifest_blob;
    spec.definition_blob = folded.definition_blob;
    spec.spec_blob = folded.spec_blob;
    spec.fork_source_ref = folded.fork_source_ref;
    spec.graph_source = spec.graph_source.or(folded.graph_source);
    Ok(spec)
}

#[cfg(test)]
#[expect(clippy::disallowed_methods, reason = "tests stage pipeline fixtures")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use fabro_graphviz::graph::{AttrValue, Edge, Graph, Node};
    use fabro_store::{Database, RunDatabase};
    use fabro_types::{fixtures, test_support};
    use object_store::memory::InMemory;

    use super::*;
    use crate::event::{Event, append_event};
    use crate::records::RunSpec;

    fn memory_store() -> Arc<Database> {
        Arc::new(fabro_store::test_support::test_database(
            Arc::new(InMemory::new()),
            "",
            Duration::from_millis(1),
            None,
        ))
    }

    fn graph_and_source() -> (Graph, String) {
        let source = r#"digraph test {
  graph [goal="Ship feature"];
  start [shape=Mdiamond];
  exit [shape=Msquare];
  start -> exit;
}"#
        .to_string();

        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "goal".to_string(),
            AttrValue::String("Ship feature".to_string()),
        );

        let mut start = Node::new("start");
        start.attrs.insert(
            "shape".to_string(),
            AttrValue::String("Mdiamond".to_string()),
        );
        graph.nodes.insert("start".to_string(), start);

        let mut exit = Node::new("exit");
        exit.attrs.insert(
            "shape".to_string(),
            AttrValue::String("Msquare".to_string()),
        );
        graph.nodes.insert("exit".to_string(), exit);

        graph.edges.push(Edge::new("start", "exit"));
        (graph, source)
    }

    fn different_graph() -> Graph {
        let mut graph = Graph::new("different");
        let mut start = Node::new("start");
        start.attrs.insert(
            "shape".to_string(),
            AttrValue::String("Mdiamond".to_string()),
        );
        graph.nodes.insert("start".to_string(), start);
        graph
    }

    fn sample_record(graph: Graph) -> RunSpec {
        RunSpec {
            run_id: fixtures::RUN_1,
            settings: fabro_types::WorkflowSettings {
                run: fabro_types::settings::RunNamespace {
                    execution: fabro_types::settings::run::RunExecutionSettings {
                        mode: fabro_types::settings::run::RunMode::DryRun,
                        ..fabro_types::settings::run::RunExecutionSettings::default()
                    },
                    ..fabro_types::settings::RunNamespace::default()
                },
                ..fabro_types::WorkflowSettings::default()
            },
            graph,
            graph_source: None,
            workflow_slug: Some("ship".to_string()),
            workflow_version_id: None,
            target: None,
            automation: None,
            source_directory: Some("/tmp/project".to_string()),
            git: Some(fabro_types::GitContext {
                origin_url: String::new(),
                branch:     "main".to_string(),
                sha:        None,
                dirty:      fabro_types::DirtyStatus::Clean,
            }),
            labels: HashMap::from([
                ("env".to_string(), "test".to_string()),
                ("team".to_string(), "workflow".to_string()),
            ]),
            provenance: test_support::test_run_provenance(),
            manifest_blob: None,
            definition_blob: None,
            spec_blob: None,
            fork_source_ref: None,
        }
    }

    async fn seeded_store(record: &RunSpec, source: Option<&str>) -> RunDatabase {
        seeded_store_with(record, source, Some(record)).await
    }

    async fn seeded_store_with(
        record: &RunSpec,
        source: Option<&str>,
        blob_record: Option<&RunSpec>,
    ) -> RunDatabase {
        let store = memory_store();
        let run_store = store.create_run(&record.run_id).await.unwrap();
        let spec_blob = match blob_record {
            Some(blob_record) => Some(
                run_store
                    .write_blob(&serde_json::to_vec(blob_record).unwrap())
                    .await
                    .unwrap(),
            ),
            None => None,
        };
        append_event(&run_store, &record.run_id, &Event::RunCreated {
            run_id: record.run_id,
            title: None,
            settings: serde_json::to_value(&record.settings).unwrap(),
            graph: serde_json::to_value(&record.graph).unwrap(),
            workflow_source: source.map(ToOwned::to_owned),
            labels: record.labels.clone().into_iter().collect(),
            source_directory: record.source_directory.clone(),
            workflow_slug: record.workflow_slug.clone(),
            workflow_version_id: None,
            target: record.target.clone(),
            automation: record.automation.clone(),
            provenance: record.provenance.clone(),
            manifest_blob: None,
            spec_blob,
            git: record.git.clone(),
            fork_source_ref: record.fork_source_ref.clone(),
            retried_from: None,
            parent_id: None,
            web_url: None,
        })
        .await
        .unwrap();
        run_store
    }

    #[test]
    fn persist_creates_run_dir_without_writing_legacy_files() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        let (graph, source) = graph_and_source();
        let persisted = persist(
            Validated::new(graph.clone(), source, vec![]),
            PersistOptions {
                run_dir:  run_dir.clone(),
                run_spec: sample_record(different_graph()),
            },
        )
        .unwrap();

        assert!(run_dir.is_dir());
        assert!(
            std::fs::read_dir(&run_dir).unwrap().next().is_none(),
            "persist should not project files into the scratch dir"
        );
        assert_eq!(persisted.run_dir(), run_dir.as_path());
        assert_eq!(
            serde_json::to_value(persisted.run_spec().graph.clone()).unwrap(),
            serde_json::to_value(graph).unwrap()
        );
    }

    #[test]
    fn persist_overwrites_run_spec_graph_with_validated_graph() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        let (graph, source) = graph_and_source();

        let persisted = persist(
            Validated::new(graph.clone(), source, vec![]),
            PersistOptions {
                run_dir:  run_dir.clone(),
                run_spec: sample_record(different_graph()),
            },
        )
        .unwrap();

        assert_eq!(persisted.run_spec().graph.name, graph.name);
        assert!(persisted.run_spec().graph.nodes.contains_key("exit"));
        assert_eq!(
            serde_json::to_value(persisted.run_spec().graph.clone()).unwrap(),
            serde_json::to_value(graph).unwrap()
        );
    }

    #[tokio::test]
    async fn load_from_store_roundtrips_full_run_spec_fields() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        let (graph, source) = graph_and_source();
        let mut expected = sample_record(different_graph());
        expected.graph = graph.clone();

        persist(
            Validated::new(graph, source.clone(), vec![]),
            PersistOptions {
                run_dir:  run_dir.clone(),
                run_spec: expected.clone(),
            },
        )
        .unwrap();

        let run_store = seeded_store(&expected, Some(&source)).await;
        let loaded = load_from_store(&run_store.clone().into(), &run_dir)
            .await
            .unwrap();

        let loaded_record = loaded.run_spec();
        assert_eq!(loaded_record.run_id, expected.run_id);
        assert!(
            (loaded_record.run_id.created_at().timestamp_millis()
                - expected.run_id.created_at().timestamp_millis())
            .abs()
                <= 1
        );
        assert_eq!(loaded_record.settings, expected.settings);
        assert_eq!(
            serde_json::to_value(&loaded_record.graph).unwrap(),
            serde_json::to_value(&expected.graph).unwrap()
        );
        assert_eq!(loaded_record.workflow_slug, expected.workflow_slug);
        assert_eq!(loaded_record.source_directory, expected.source_directory);
        assert_eq!(loaded_record.base_branch(), expected.base_branch());
        assert_eq!(loaded_record.labels, expected.labels);
        assert_eq!(loaded.source(), source);
        assert!(loaded.diagnostics().is_empty());
    }

    #[tokio::test]
    async fn load_from_store_preserves_high_entropy_dockerfile_content() {
        // The spec the worker executes must survive the store byte-identical.
        // Event redaction is a storage/display concern; when it reaches the
        // spec that `load_from_store` rehydrates, the sandbox builds a
        // corrupted Dockerfile: `ARG NAME=<hex>` pairs come back as
        // `ARG REDACTED`, the build's `set -eu` step fails on the unset
        // variable, and the environment's snapshot identity silently changes.
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        let (graph, source) = graph_and_source();

        // Two shapes that must both survive: the hex pins that triggered the
        // production failure, and a token high-entropy enough that any
        // detector will keep flagging it in stored events. The second keeps
        // this test red until execution stops reading redacted content,
        // independent of how the entropy heuristic evolves.
        let dockerfile = "FROM buildpack-deps:noble\n\
             ARG DOCKER_INSTALL_COMMIT=5ce20f2eef3615d08fea941eda5a109e949e8ebf\n\
             ARG DOCKER_INSTALL_SHA256=b991f2806186f7287bb9e53362060c382e906d154599b2fb0982f34246bacfd4\n\
             ENV CACHE_SALT=xK9mZ2vL8nQ5rT1wY4bC7dF0gH3jE6p\n\
             RUN install-docker \"${DOCKER_INSTALL_COMMIT}\" \"${DOCKER_INSTALL_SHA256}\"\n";

        let mut record = sample_record(different_graph());
        record.graph = graph;
        record.settings.run.environment.image.dockerfile = Some(
            fabro_types::settings::run::DockerfileSource::Inline(dockerfile.to_string()),
        );

        let run_store = seeded_store(&record, Some(&source)).await;
        let loaded = load_from_store(&run_store.clone().into(), &run_dir)
            .await
            .unwrap();

        assert_eq!(
            loaded.run_spec().settings.run.environment.image.dockerfile,
            Some(fabro_types::settings::run::DockerfileSource::Inline(
                dockerfile.to_string()
            )),
            "the executable run spec must round-trip through the store unredacted"
        );
    }

    #[tokio::test]
    async fn load_from_store_falls_back_to_folded_spec_without_spec_blob() {
        // Runs created before the spec blob existed carry no spec_blob on
        // run.created; the folded spec is their only copy.
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        let (graph, source) = graph_and_source();
        let mut record = sample_record(different_graph());
        record.graph = graph;

        let run_store = seeded_store_with(&record, Some(&source), None).await;
        let loaded = load_from_store(&run_store.clone().into(), &run_dir)
            .await
            .unwrap();

        assert_eq!(loaded.run_spec().settings, record.settings);
        assert_eq!(loaded.run_spec().spec_blob, None);
    }

    #[tokio::test]
    async fn load_from_store_uses_fork_reference_from_event_fold() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        let (graph, source) = graph_and_source();
        let source_record = sample_record(graph.clone());
        let mut fork_record = source_record.clone();
        fork_record.run_id = fixtures::RUN_7;
        fork_record.fork_source_ref = Some(fabro_types::ForkSourceRef {
            source_run_id:  source_record.run_id,
            checkpoint_sha: "checkpoint-sha".to_string(),
        });

        let run_store = seeded_store_with(&fork_record, Some(&source), Some(&source_record)).await;
        let loaded = load_from_store(&run_store.clone().into(), &run_dir)
            .await
            .unwrap();

        assert_eq!(loaded.run_spec().run_id, fork_record.run_id);
        assert_eq!(
            loaded.run_spec().fork_source_ref,
            fork_record.fork_source_ref
        );
    }

    #[test]
    fn persist_returns_error_on_io_failure() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        std::fs::write(&run_dir, "not a directory").unwrap();
        let (graph, source) = graph_and_source();

        let err = persist(Validated::new(graph, source, vec![]), PersistOptions {
            run_dir,
            run_spec: sample_record(different_graph()),
        })
        .unwrap_err();

        assert!(matches!(err, Error::Io(_)));
    }

    #[tokio::test]
    async fn load_from_store_uses_empty_source_when_graph_missing() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        let (graph, _source) = graph_and_source();
        let mut record = sample_record(different_graph());
        record.graph = graph;

        let run_store = seeded_store(&record, None).await;
        let loaded = load_from_store(&run_store.clone().into(), &run_dir)
            .await
            .unwrap();

        assert!(loaded.source().is_empty());
    }

    #[tokio::test]
    async fn load_from_store_reads_graph_from_run_spec_and_source_from_store() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join("run");
        std::fs::create_dir_all(&run_dir).unwrap();

        let (graph, source) = graph_and_source();
        let mut record = sample_record(different_graph());
        record.graph = graph.clone();

        let run_store = seeded_store(&record, Some(&source)).await;
        let loaded = load_from_store(&run_store.clone().into(), &run_dir)
            .await
            .unwrap();

        assert_eq!(
            serde_json::to_value(loaded.graph()).unwrap(),
            serde_json::to_value(graph).unwrap()
        );
        assert_eq!(loaded.source(), source);
    }
}
