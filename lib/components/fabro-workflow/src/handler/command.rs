use std::path::Path;

use async_trait::async_trait;
use fabro_agent::{CommandOutputCallback, ExecStreamingRequest};
use fabro_graphviz::graph::{ContextKeyAttr, Graph, Node};
use fabro_types::{CommandTermination, StageTiming};
use fabro_util::shell::shell_quote;

use super::structured_output::{self, StructuredOutputError};
use super::{EngineServices, Handler, NodeTimeoutPolicy};
use crate::artifact;
use crate::command_log::CommandLogRecorder;
use crate::context::{Context, keys};
use crate::error::Error;
use crate::event::{Event, StageScope};
use crate::outcome::{Outcome, OutcomeExt};

fn timeout_ms(node: &Node) -> Option<u64> {
    node.timeout().map(crate::millis_u64)
}

fn non_blank_script(node: &Node) -> Option<&str> {
    node.script().filter(|script| !script.trim().is_empty())
}

/// Executes an external script configured via node attributes.
pub struct CommandHandler;

#[async_trait]
impl Handler for CommandHandler {
    async fn simulate(
        &self,
        node: &Node,
        _context: &Context,
        _graph: &Graph,
        _run_dir: &Path,
        _services: &EngineServices,
    ) -> Result<Outcome, Error> {
        if let Err(reason) = validated_stdin_source(node) {
            return Ok(Outcome::fail_deterministic(reason));
        }
        let Some(script) = non_blank_script(node) else {
            return Ok(Outcome::fail_classify("No script specified"));
        };

        let mut outcome = Outcome::simulated(&node.id);
        outcome.notes = Some(format!("[Simulated] Command skipped: {script}"));
        outcome
            .context_updates
            .insert(keys::COMMAND_OUTPUT.to_string(), serde_json::json!(""));
        Ok(outcome)
    }

    async fn execute(
        &self,
        node: &Node,
        context: &Context,
        _graph: &Graph,
        run_dir: &Path,
        services: &EngineServices,
    ) -> Result<Outcome, Error> {
        let Some(script) = non_blank_script(node) else {
            return Ok(Outcome::fail_classify("No script specified"));
        };

        let language = node
            .attrs
            .get("language")
            .and_then(|v| v.as_str())
            .unwrap_or("shell");

        if language != "shell" && language != "python" {
            return Ok(Outcome::fail_classify(format!(
                "Invalid language: {language:?} (expected \"shell\" or \"python\")"
            )));
        }

        let stdin = match resolve_stdin(node, context, services).await {
            Ok(stdin) => stdin,
            Err(outcome) => return Ok(outcome),
        };
        let output_schema = structured_output::parse_node_output_schema(node)?;

        let command = if language == "python" {
            format!("python3 -c {}", shell_quote(script))
        } else {
            script.to_string()
        };
        let command = format!("exec 2>&1\n{command}");
        let stage_scope = StageScope::for_handler(context, &node.id);
        services.run.emitter.emit_scoped(
            &Event::CommandStarted {
                node_id:    node.id.clone(),
                script:     script.to_string(),
                command:    command.clone(),
                language:   language.to_string(),
                timeout_ms: timeout_ms(node),
            },
            &stage_scope,
        );

        let timeout_ms = node.timeout().map_or(600_000, crate::millis_u64);
        let env = services
            .env_for_stage()
            .await
            .map_err(|err| Error::handler_with_anyhow("Failed to resolve stage env", err))?;
        let env_vars = if env.is_empty() { None } else { Some(&env) };
        let cancel_token = services.run.cancel_token().child_token();
        let stage_id = stage_scope.stage_id();
        let recorder = CommandLogRecorder::create(run_dir, &stage_id).await?;
        let output_callback: CommandOutputCallback = {
            let recorder = recorder.clone();
            std::sync::Arc::new(move |_stream, bytes| {
                let recorder = recorder.clone();
                Box::pin(async move {
                    recorder
                        .append(&bytes)
                        .await
                        .map_err(|err| fabro_sandbox::Error::message(err.to_string()))
                })
            })
        };

        let result = services
            .run
            .sandbox
            .exec_command_streaming(ExecStreamingRequest {
                timeout_ms: Some(timeout_ms),
                env_vars,
                cancel_token: Some(cancel_token.clone()),
                stdin,
                output_callback: Some(output_callback),
                ..ExecStreamingRequest::new(&command)
            })
            .await;
        cancel_token.cancel();
        let streaming = match result {
            Ok(streaming) => streaming,
            Err(err) => {
                recorder.discard().await?;
                return Err(Error::handler_with_source("Failed to spawn script", err));
            }
        };
        let result = streaming.result;
        let finalized = recorder.finalize(&services.run.run_store).await?;

        services.run.emitter.emit_scoped(
            &Event::CommandCompleted {
                node_id:        node.id.clone(),
                output:         finalized.output_ref.clone(),
                exit_code:      result.exit_code,
                duration_ms:    result.duration_ms,
                termination:    result.termination,
                output_bytes:   finalized.output_bytes,
                live_streaming: streaming.live_streaming,
            },
            &stage_scope,
        );

        if result.termination == CommandTermination::TimedOut {
            let mut reason = format!("Script timed out after {timeout_ms}ms: {script}");
            append_output_tail(&mut reason, &finalized.output_text);
            return Err(Error::handler(reason));
        }

        if result.termination == CommandTermination::Cancelled {
            let mut reason = format!("Script cancelled: {script}");
            append_output_tail(&mut reason, &finalized.output_text);
            return Err(Error::handler(reason));
        }

        if result.exit_code == Some(0) {
            let validation = output_schema.as_ref().map(|schema| {
                (
                    schema,
                    structured_output::validate_response_text(schema, &finalized.output_text),
                )
            });
            let mut outcome = if let Some((_, Err(error))) = &validation {
                Outcome::fail_deterministic(schema_validation_failure_reason(
                    script,
                    error,
                    &finalized.output_text,
                ))
            } else {
                let mut outcome = Outcome::success();
                outcome.notes = Some(format!("Script completed: {script}"));
                outcome
            };
            outcome.context_updates.insert(
                keys::COMMAND_OUTPUT.to_string(),
                serde_json::json!(finalized.output_ref),
            );
            outcome.timing = Some(StageTiming::active_only(0, result.duration_ms));
            if let Some((schema, Ok(validated))) = validation {
                structured_output::apply_validated_output(node, schema, &validated, &mut outcome);
            }
            Ok(outcome)
        } else {
            let mut reason = format!(
                "Script failed with exit code: {}",
                result.exit_code.unwrap_or(-1)
            );
            append_output_tail(&mut reason, &finalized.output_text);
            let mut outcome = Outcome::fail_classify(reason);
            outcome.context_updates.insert(
                keys::COMMAND_OUTPUT.to_string(),
                serde_json::json!(finalized.output_ref),
            );
            outcome.timing = Some(StageTiming::active_only(0, result.duration_ms));
            Ok(outcome)
        }
    }

    fn node_timeout_policy(&self, _node: &Node) -> NodeTimeoutPolicy {
        NodeTimeoutPolicy::HandlerManaged
    }
}

/// Ceiling on encoded stdin bytes. `stdin_source` values are runtime data —
/// often model-produced — so their size is not something a workflow author
/// reviewed; this bounds peak memory and remote uploads the same way
/// `MAX_FOR_EACH_ITEMS` bounds `for_each` fan-out. Sized for wide fan-in:
/// a `context.parallel.results` batch from a large `for_each` round easily
/// carries tens of structured agent outputs.
const MAX_STDIN_BYTES: usize = 30 * 1024 * 1024;

fn validated_stdin_source(node: &Node) -> Result<Option<&str>, String> {
    match node.context_key_attr("stdin_source") {
        ContextKeyAttr::Absent => Ok(None),
        ContextKeyAttr::Invalid => Err(format!(
            "Node '{}' requires 'stdin_source' to be a non-empty string",
            node.id
        )),
        ContextKeyAttr::Present(source) => Ok(Some(source)),
    }
}

async fn resolve_stdin(
    node: &Node,
    context: &Context,
    services: &EngineServices,
) -> Result<Option<Vec<u8>>, Outcome> {
    let Some(source) = validated_stdin_source(node).map_err(Outcome::fail_deterministic)? else {
        return Ok(None);
    };
    let value = match artifact::resolve_flat_context_value(context, source, &services.run.run_store)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Err(Outcome::fail_deterministic(format!(
                "stdin_source '{source}' was not found in workflow context"
            )));
        }
        Err(err) => {
            return Err(Outcome::fail_deterministic(format!(
                "stdin_source '{source}' could not be resolved: {err}"
            )));
        }
    };
    let stdin = encode_stdin_value(value).map_err(|err| {
        Outcome::fail_deterministic(format!(
            "stdin_source '{source}' could not be serialized: {err}"
        ))
    })?;
    if stdin.len() > MAX_STDIN_BYTES {
        return Err(Outcome::fail_deterministic(format!(
            "stdin_source '{source}' resolved to {} bytes, above the limit of {MAX_STDIN_BYTES}. \
             Reduce the value in the node that produces it, or pass it through a file instead.",
            stdin.len()
        )));
    }
    Ok(Some(stdin))
}

fn encode_stdin_value(value: serde_json::Value) -> serde_json::Result<Vec<u8>> {
    match value {
        serde_json::Value::String(text) => Ok(text.into_bytes()),
        value => serde_json::to_vec(&value),
    }
}

fn schema_validation_failure_reason(
    script: &str,
    error: &StructuredOutputError,
    output_text: &str,
) -> String {
    let mut reason = format!("Script output failed output_schema validation: {script}");
    for message in error.messages() {
        reason.push_str("\n- ");
        reason.push_str(&message);
    }
    append_output_tail(&mut reason, output_text);
    reason
}

fn append_output_tail(reason: &mut String, output: &str) {
    let output_tail = tail_bytes(output, 4096);
    if !output_tail.trim().is_empty() {
        reason.push_str("\n\n## output\n");
        reason.push_str(&output_tail);
    }
}

fn tail_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut start = text.len() - max_bytes;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use fabro_graphviz::graph::AttrValue;
    use fabro_sandbox::test_support::MockSandbox;
    use fabro_store::{Database, RunDatabase, StageId};
    use fabro_types::{Graph, RunProjection, RunSpec, WorkflowSettings, fixtures, test_support};
    use object_store::memory::InMemory;
    use tokio::sync::Mutex;

    use super::*;
    use crate::command_log::command_log_path;
    use crate::outcome::{FailureCategory, StageOutcome};
    use crate::runtime_store::{RunStoreBackend, RunStoreHandle};

    const PASSED_OUTPUT_SCHEMA: &str =
        r#"{"type":"object","required":["passed"],"properties":{"passed":{"type":"boolean"}}}"#;

    #[test]
    fn stdin_json_encoding_is_compact_and_strings_are_raw() {
        for (value, expected) in [
            (serde_json::json!("text"), b"text".as_slice()),
            (serde_json::json!([1, 2]), br"[1,2]".as_slice()),
            (
                serde_json::json!({"ok": true}),
                br#"{"ok":true}"#.as_slice(),
            ),
            (serde_json::json!(42), b"42".as_slice()),
            (serde_json::json!(false), b"false".as_slice()),
            (serde_json::Value::Null, b"null".as_slice()),
        ] {
            assert_eq!(encode_stdin_value(value).unwrap(), expected);
        }
    }

    #[derive(Default)]
    struct MemoryRunStoreBackend {
        blobs: Mutex<std::collections::HashMap<fabro_types::BlobHash, Bytes>>,
    }

    #[async_trait::async_trait]
    impl RunStoreBackend for MemoryRunStoreBackend {
        async fn load_state(&self) -> anyhow::Result<fabro_store::RunProjection> {
            Ok(RunProjection::new(
                "Test run".to_string(),
                RunSpec {
                    run_id:              fixtures::RUN_1,
                    settings:            WorkflowSettings::default(),
                    graph:               Graph::new("test"),
                    graph_source:        None,
                    workflow_slug:       None,
                    workflow_version_id: None,
                    target:              None,
                    automation:          None,
                    source_directory:    None,
                    labels:              std::collections::HashMap::default(),
                    provenance:          test_support::test_run_provenance(),
                    manifest_blob:       None,
                    definition_blob:     None,
                    spec_blob:           None,
                    git:                 None,
                    fork_source_ref:     None,
                },
                chrono::Utc::now(),
            ))
        }

        async fn list_events(&self) -> anyhow::Result<Vec<fabro_store::EventEnvelope>> {
            Ok(Vec::new())
        }

        async fn append_run_event(&self, _event: &fabro_types::RunEvent) -> anyhow::Result<()> {
            Ok(())
        }

        async fn write_blob(&self, data: &[u8]) -> anyhow::Result<fabro_types::BlobHash> {
            let blob_hash = fabro_types::BlobHash::new(data);
            self.blobs
                .lock()
                .await
                .insert(blob_hash, Bytes::copy_from_slice(data));
            Ok(blob_hash)
        }

        async fn read_blob(
            &self,
            blob_hash: &fabro_types::BlobHash,
        ) -> anyhow::Result<Option<Bytes>> {
            Ok(self.blobs.lock().await.get(blob_hash).cloned())
        }

        async fn read_run_log(&self) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    fn make_services() -> EngineServices {
        let mut services = EngineServices::test_default();
        services.run = services.run.with_run_store(RunStoreHandle::new(Arc::new(
            MemoryRunStoreBackend::default(),
        )));
        services
    }

    async fn command_text(services: &EngineServices, value: &serde_json::Value) -> String {
        crate::artifact::resolve_text_or_blob_ref(value, &services.run.run_store)
            .await
            .unwrap()
    }

    async fn command_log_text(services: &EngineServices, value: &str) -> String {
        crate::command_log::read_json_string_blob(&services.run.run_store, value)
            .await
            .unwrap()
            .unwrap_or_else(|| value.to_string())
    }

    fn test_store() -> Arc<Database> {
        Arc::new(fabro_store::test_support::test_database(
            Arc::new(InMemory::new()),
            "",
            Duration::from_millis(1),
            None,
        ))
    }

    async fn make_services_with_run_store() -> (
        EngineServices,
        RunDatabase,
        crate::event::StoreProgressLogger,
    ) {
        let store = test_store();
        let run_store = store.create_run(&fixtures::RUN_1).await.unwrap();
        seed_created(&run_store).await;
        let mut services = EngineServices::test_default();
        services.run = services
            .run
            .with_emitter(Arc::new(crate::event::Emitter::new(fixtures::RUN_1)))
            .with_run_store(run_store.clone().into());
        let logger = crate::event::StoreProgressLogger::new(run_store.clone());
        logger.register(services.run.emitter.as_ref());
        (services, run_store, logger)
    }

    async fn seed_created(run_store: &RunDatabase) {
        crate::event::append_event(
            run_store,
            &fixtures::RUN_1,
            &crate::event::Event::RunCreated {
                run_id:              fixtures::RUN_1,
                title:               None,
                settings:            serde_json::to_value(WorkflowSettings::default()).unwrap(),
                graph:               serde_json::to_value(Graph::new("test")).unwrap(),
                workflow_source:     None,
                labels:              std::collections::BTreeMap::default(),
                source_directory:    None,
                workflow_slug:       None,
                workflow_version_id: None,
                target:              None,
                automation:          None,
                provenance:          test_support::test_run_provenance(),
                manifest_blob:       None,
                spec_blob:           None,
                git:                 None,
                fork_source_ref:     None,
                retried_from:        None,
                parent_id:           None,
                web_url:             None,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn missing_and_blank_scripts_fail_execution_and_simulation() {
        let handler = CommandHandler;
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_services();

        for script in [None, Some("  \t\n")] {
            let mut node = Node::new("script_node");
            if let Some(script) = script {
                node.attrs
                    .insert("script".to_string(), AttrValue::String(script.to_string()));
            }

            let outcomes = [
                handler
                    .execute(&node, &context, &graph, run_dir.path(), &services)
                    .await
                    .unwrap(),
                handler
                    .simulate(&node, &context, &graph, run_dir.path(), &services)
                    .await
                    .unwrap(),
            ];

            for outcome in outcomes {
                assert_eq!(outcome.status, StageOutcome::Failed {
                    retry_requested: false,
                });
                assert_eq!(outcome.failure_reason(), Some("No script specified"));
            }
        }
    }

    #[tokio::test]
    async fn simulate_skips_execution() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .simulate(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert!(outcome.notes.as_deref().unwrap().contains("[Simulated]"));
        assert!(outcome.notes.as_deref().unwrap().contains("echo hello"));
        assert_eq!(
            outcome.context_updates.get(keys::COMMAND_OUTPUT),
            Some(&serde_json::json!(""))
        );
        assert!(!outcome.context_updates.contains_key("command.stderr"));
    }

    #[tokio::test]
    async fn dispatch_routes_to_simulate_in_dry_run() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let mut services = make_services();
        services.dry_run = true;

        let outcome = crate::handler::dispatch_handler(
            &handler,
            &node,
            &context,
            &graph,
            run_dir.path(),
            &services,
        )
        .await
        .unwrap();
        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert!(outcome.notes.as_deref().unwrap().contains("[Simulated]"));
    }

    #[tokio::test]
    async fn script_handler_echo_command() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert!(outcome.notes.as_deref().unwrap().contains("echo hello"));
        let command_output = outcome.context_updates.get(keys::COMMAND_OUTPUT).unwrap();
        assert!(
            command_text(&services, command_output)
                .await
                .contains("hello")
        );
        assert!(!outcome.context_updates.contains_key("command.stderr"));
    }

    #[tokio::test]
    async fn command_custom_output_schema_stores_output_context_key() {
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(r#"echo '{"passed": true}'"#.to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String(PASSED_OUTPUT_SCHEMA.to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_services();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert_eq!(
            outcome.context_updates.get("output.audit"),
            Some(&serde_json::json!({"passed": true})),
        );
        let command_output = outcome
            .context_updates
            .get(keys::COMMAND_OUTPUT)
            .expect("command.output should still be set");
        assert!(
            command_text(&services, command_output)
                .await
                .contains(r#"{"passed": true}"#)
        );
    }

    #[tokio::test]
    async fn command_custom_output_schema_validates_last_json_object() {
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(
                r#"printf '%s\n' 'starting audit' '{"passed": false}' 'final result:' '{"passed": true}'"#
                    .to_string(),
            ),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String(PASSED_OUTPUT_SCHEMA.to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert_eq!(
            outcome.context_updates.get("output.audit"),
            Some(&serde_json::json!({"passed": true})),
        );
    }

    #[tokio::test]
    async fn command_custom_output_schema_failure_is_deterministic() {
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(r#"echo '{"passed":"yes"}'"#.to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String(PASSED_OUTPUT_SCHEMA.to_string()),
        );
        node.attrs
            .insert("output_retries".to_string(), AttrValue::Integer(7));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_services();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        assert_eq!(
            outcome.failure_category(),
            Some(FailureCategory::Deterministic)
        );
        let reason = outcome
            .failure_reason()
            .expect("schema validation failure should have a reason");
        assert!(
            reason.contains("Script output failed output_schema validation: echo"),
            "unexpected failure reason: {reason}"
        );
        assert!(
            reason.contains("boolean"),
            "validator message should be included: {reason}"
        );
        assert!(
            reason.contains("## output"),
            "output heading missing: {reason}"
        );
        assert!(
            reason.contains(r#"{"passed":"yes"}"#),
            "output tail missing: {reason}"
        );
        assert!(
            !reason.contains("repair attempt"),
            "commands must not claim repair attempts: {reason}"
        );
        assert!(
            outcome.context_updates.contains_key(keys::COMMAND_OUTPUT),
            "command.output should be set on validation failure"
        );
        assert!(outcome.timing.is_some());
        assert_eq!(outcome.notes, None);
    }

    #[tokio::test]
    async fn command_routing_output_schema_no_json_object_fails() {
        let handler = CommandHandler;
        let mut node = Node::new("route");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo not-json".to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String("routing".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        assert_eq!(
            outcome.failure_category(),
            Some(FailureCategory::Deterministic)
        );
        let reason = outcome.failure_reason().unwrap();
        assert!(reason.contains("no JSON object found"), "got: {reason}");
        assert!(reason.contains("## output\nnot-json"), "got: {reason}");
    }

    #[tokio::test]
    async fn command_routing_output_schema_applies_routing_fields() {
        let handler = CommandHandler;
        let mut node = Node::new("route");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(
                r#"echo '{"preferred_next_label":"fix","context_updates":{"kept_count":2}}'"#
                    .to_string(),
            ),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String("routing".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert_eq!(outcome.preferred_label.as_deref(), Some("fix"));
        assert_eq!(
            outcome.context_updates.get("kept_count"),
            Some(&serde_json::json!(2))
        );
        assert!(outcome.context_updates.contains_key(keys::COMMAND_OUTPUT));
    }

    #[tokio::test]
    async fn command_routing_output_schema_outcome_failed_override() {
        let handler = CommandHandler;
        let mut node = Node::new("route");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(
                r#"echo '{"outcome":"failed","failure_reason":"tests failed"}'"#.to_string(),
            ),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String("routing".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        assert_eq!(outcome.failure_reason(), Some("tests failed"));
        assert_eq!(
            outcome.failure_category(),
            Some(FailureCategory::Deterministic)
        );
    }

    #[tokio::test]
    async fn command_invalid_output_schema_fails_before_execution() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      String::new(),
            stderr:      String::new(),
            exit_code:   Some(0),
            termination: CommandTermination::Exited,
            duration_ms: 1,
        }));
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo should-not-run".to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String("{".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let mut services = make_sandbox_services(spy.clone());
        let event_names = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_event_names = Arc::clone(&event_names);
        let emitter = Arc::new(crate::event::Emitter::new(fixtures::RUN_1));
        emitter.on_event(move |event| {
            captured_event_names
                .lock()
                .unwrap()
                .push(event.event_name().to_string());
        });
        services.run = services.run.with_emitter(emitter);

        let error = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("Invalid output_schema"),
            "unexpected error: {error}"
        );
        assert_eq!(
            spy.captured_command(),
            None,
            "invalid schema must fail before sandbox execution"
        );
        assert!(
            event_names.lock().unwrap().is_empty(),
            "invalid schema must fail before event emission"
        );
    }

    #[tokio::test]
    async fn command_nonzero_exit_skips_schema_validation() {
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(r#"echo '{"passed":"bad"}'; exit 1"#.to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String(PASSED_OUTPUT_SCHEMA.to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        let reason = outcome.failure_reason().unwrap();
        assert!(reason.contains("exit code: 1"), "got: {reason}");
        assert!(
            !reason.contains("output_schema validation"),
            "nonzero exits must skip schema validation: {reason}"
        );
    }

    #[tokio::test]
    async fn command_simulate_ignores_output_schema() {
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo should-not-run".to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String("{not a valid schema".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .simulate(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert!(outcome.notes.as_deref().unwrap().contains("[Simulated]"));
        assert_eq!(
            outcome.context_updates.get(keys::COMMAND_OUTPUT),
            Some(&serde_json::json!(""))
        );
        assert!(!outcome.context_updates.contains_key("output.audit"));
    }

    #[tokio::test]
    async fn command_python_custom_output_schema() {
        let handler = CommandHandler;
        let mut node = Node::new("audit");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(r#"import json; print(json.dumps({"passed": True}))"#.to_string()),
        );
        node.attrs.insert(
            "language".to_string(),
            AttrValue::String("python".to_string()),
        );
        node.attrs.insert(
            "output_schema".to_string(),
            AttrValue::String(PASSED_OUTPUT_SCHEMA.to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert_eq!(
            outcome.context_updates.get("output.audit"),
            Some(&serde_json::json!({"passed": true})),
        );
        assert!(outcome.context_updates.contains_key(keys::COMMAND_OUTPUT));
    }

    #[tokio::test]
    async fn script_handler_reports_command_duration_as_tool_timing() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("sleep 0.05; echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        let timing = outcome.timing.expect("command outcome should carry timing");
        assert_eq!(timing.inference_time_ms, 0);
        assert!(
            timing.tool_time_ms >= 25,
            "expected command duration to be reported as tool time, got {timing:?}"
        );
        assert_eq!(timing.active_time_ms, timing.tool_time_ms);
    }

    #[tokio::test]
    async fn script_handler_failing_command() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs
            .insert("script".to_string(), AttrValue::String("false".to_string()));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
    }

    #[tokio::test]
    async fn script_handler_timeout() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("sleep 60".to_string()),
        );
        node.attrs.insert(
            "timeout".to_string(),
            AttrValue::Duration(Duration::from_millis(50)),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let err = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("timed out"),
            "expected timeout message, got: {msg}"
        );
    }

    #[tokio::test]
    async fn writes_script_invocation_json() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let json = node_state.script_invocation.as_ref().unwrap();
        assert_eq!(json["command"], "exec 2>&1\necho hello");
        assert_eq!(json["language"], "shell");
        assert_eq!(json["timeout_ms"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn writes_script_invocation_json_with_timeout() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        node.attrs.insert(
            "timeout".to_string(),
            AttrValue::Duration(Duration::from_secs(5)),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let json = node_state.script_invocation.as_ref().unwrap();
        assert_eq!(json["command"], "exec 2>&1\necho hello");
        assert_eq!(json["language"], "shell");
        assert_eq!(json["timeout_ms"], 5000);
    }

    #[tokio::test]
    async fn writes_output_log() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let output = node_state.output.as_deref().unwrap();
        assert_eq!(command_log_text(&services, output).await.trim(), "hello");
        assert_eq!(node_state.output_bytes, Some(6));
        assert_eq!(node_state.live_streaming, Some(true));
    }

    #[tokio::test]
    async fn writes_stderr_to_output_log_on_failure() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo oops >&2 && false".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let output = node_state.output.as_deref().unwrap();
        assert_eq!(command_log_text(&services, output).await.trim(), "oops");
    }

    #[tokio::test]
    async fn writes_script_timing_json_on_success() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let json = node_state.script_timing.as_ref().unwrap();
        assert!(json["duration_ms"].is_u64());
        assert_eq!(json["exit_code"], 0);
        assert_eq!(json["termination"], "exited");
    }

    #[tokio::test]
    async fn writes_script_timing_json_on_failure() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs
            .insert("script".to_string(), AttrValue::String("false".to_string()));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let json = node_state.script_timing.as_ref().unwrap();
        assert_eq!(json["exit_code"], 1);
        assert_eq!(json["termination"], "exited");
    }

    #[tokio::test]
    async fn writes_script_timing_json_on_timeout() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("sleep 60".to_string()),
        );
        node.attrs.insert(
            "timeout".to_string(),
            AttrValue::Duration(Duration::from_millis(50)),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        let _err = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap_err();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node_state = snapshot.stage(&StageId::new("script_node", 1)).unwrap();
        let json = node_state.script_timing.as_ref().unwrap();
        assert!(json["duration_ms"].is_u64());
        assert_eq!(json["exit_code"], serde_json::Value::Null);
        assert_eq!(json["termination"], "timed_out");
    }

    #[tokio::test]
    async fn stores_script_invocation_and_timing_in_run_store() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let (services, run_store, logger) = make_services_with_run_store().await;

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        logger.flush().await.unwrap();

        let snapshot = run_store.state().await.unwrap();
        let node = snapshot
            .stage(&StageId::new("script_node", 1))
            .cloned()
            .unwrap();

        assert_eq!(node.script_invocation.unwrap()["script"], "echo hello");
        assert_eq!(node.script_timing.unwrap()["exit_code"], 0);
    }

    #[tokio::test]
    async fn script_handler_python_echo() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("print('hello from python')".to_string()),
        );
        node.attrs.insert(
            "language".to_string(),
            AttrValue::String("python".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Succeeded);
        let command_output = outcome.context_updates.get(keys::COMMAND_OUTPUT).unwrap();
        assert!(
            command_text(&services, command_output)
                .await
                .contains("hello from python")
        );
    }

    #[tokio::test]
    async fn script_handler_python_failure() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("raise Exception('boom')".to_string()),
        );
        node.attrs.insert(
            "language".to_string(),
            AttrValue::String("python".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
    }

    #[tokio::test]
    async fn script_handler_invalid_language() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        node.attrs.insert(
            "language".to_string(),
            AttrValue::String("ruby".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        assert!(
            outcome
                .failure_reason()
                .unwrap()
                .contains("Invalid language")
        );
    }

    #[tokio::test]
    async fn tool_command_attribute_is_not_read() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "tool_command".to_string(),
            AttrValue::String("echo legacy".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        assert!(outcome.failure_reason().unwrap().contains("No script"));
    }

    #[tokio::test]
    async fn script_handler_merges_stderr_into_output() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo out && echo err >&2".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Succeeded);
        let command_output = outcome.context_updates.get(keys::COMMAND_OUTPUT).unwrap();
        assert!(
            command_text(&services, command_output)
                .await
                .contains("err"),
            "command.output should contain 'err', got: {:?}",
            command_output
        );
    }

    /// A sandbox that returns a canned `ExecResult` and captures the command,
    /// proving that `CommandHandler` delegates to the sandbox rather than
    /// spawning a host process.
    struct SpySandbox {
        exec_result:           fabro_agent::sandbox::ExecResult,
        exec_error:            Option<String>,
        captured_command:      std::sync::Mutex<Option<String>>,
        captured_env_vars:     std::sync::Mutex<Option<std::collections::HashMap<String, String>>>,
        captured_cancel_token: std::sync::Mutex<Option<bool>>,
    }

    impl SpySandbox {
        fn new(exec_result: fabro_agent::sandbox::ExecResult) -> Self {
            Self {
                exec_result,
                exec_error: None,
                captured_command: std::sync::Mutex::new(None),
                captured_env_vars: std::sync::Mutex::new(None),
                captured_cancel_token: std::sync::Mutex::new(None),
            }
        }

        fn fail(message: impl Into<String>) -> Self {
            Self {
                exec_result:           fabro_agent::sandbox::ExecResult {
                    stdout:      String::new(),
                    stderr:      String::new(),
                    exit_code:   Some(1),
                    termination: CommandTermination::Exited,
                    duration_ms: 0,
                },
                exec_error:            Some(message.into()),
                captured_command:      std::sync::Mutex::new(None),
                captured_env_vars:     std::sync::Mutex::new(None),
                captured_cancel_token: std::sync::Mutex::new(None),
            }
        }

        fn captured_command(&self) -> Option<String> {
            self.captured_command.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl fabro_agent::sandbox::Sandbox for SpySandbox {
        async fn read_file_bytes(&self, _: &str) -> fabro_sandbox::Result<Vec<u8>> {
            unimplemented!()
        }
        async fn write_file(&self, _: &str, _: &str) -> fabro_sandbox::Result<()> {
            unimplemented!()
        }
        async fn delete_file(&self, _: &str) -> fabro_sandbox::Result<()> {
            unimplemented!()
        }
        async fn file_exists(&self, _: &str) -> fabro_sandbox::Result<bool> {
            unimplemented!()
        }
        async fn list_directory(
            &self,
            _: &str,
            _: Option<usize>,
        ) -> fabro_sandbox::Result<Vec<fabro_agent::sandbox::DirEntry>> {
            unimplemented!()
        }
        async fn exec_command(
            &self,
            command: &str,
            _timeout_ms: u64,
            _working_dir: Option<&str>,
            env_vars: Option<&std::collections::HashMap<String, String>>,
            cancel_token: Option<tokio_util::sync::CancellationToken>,
        ) -> fabro_sandbox::Result<fabro_agent::sandbox::ExecResult> {
            *self.captured_command.lock().unwrap() = Some(command.to_string());
            *self.captured_env_vars.lock().unwrap() = env_vars.cloned();
            *self.captured_cancel_token.lock().unwrap() = Some(cancel_token.is_some());
            if let Some(message) = self.exec_error.as_ref() {
                return Err(fabro_sandbox::Error::message(message.clone()));
            }
            Ok(self.exec_result.clone())
        }
        async fn grep(
            &self,
            _: &str,
            _: &str,
            _: &fabro_agent::sandbox::GrepOptions,
        ) -> fabro_sandbox::Result<Vec<String>> {
            unimplemented!()
        }
        async fn glob(&self, _: &str, _: Option<&str>) -> fabro_sandbox::Result<Vec<String>> {
            unimplemented!()
        }
        async fn download_file_to_local(
            &self,
            _: &str,
            _: &std::path::Path,
        ) -> fabro_sandbox::Result<()> {
            unimplemented!()
        }
        async fn upload_file_from_local(
            &self,
            _: &std::path::Path,
            _: &str,
        ) -> fabro_sandbox::Result<()> {
            unimplemented!()
        }
        async fn initialize(&self) -> fabro_sandbox::Result<()> {
            Ok(())
        }
        async fn cleanup(&self) -> fabro_sandbox::Result<()> {
            Ok(())
        }
        fn working_directory(&self) -> &str {
            "/mock"
        }
        fn platform(&self) -> &str {
            "linux"
        }
        fn os_version(&self) -> String {
            "Mock".into()
        }
    }

    fn make_sandbox_services(sandbox: std::sync::Arc<dyn fabro_agent::Sandbox>) -> EngineServices {
        let mut services = make_services();
        services.run = services.run.with_sandbox(sandbox);
        services
    }

    #[tokio::test]
    async fn stdin_source_serializes_parallel_results_as_compact_json() {
        let mock = std::sync::Arc::new(MockSandbox::default());
        let handler = CommandHandler;
        let mut node = Node::new("merge");
        node.attrs
            .insert("script".to_string(), AttrValue::String("cat".to_string()));
        node.attrs.insert(
            "stdin_source".to_string(),
            AttrValue::String("context.parallel.results".to_string()),
        );
        let parallel_results = serde_json::json!([
            {
                "branch": "one",
                "response": "$(touch /tmp/must-not-run)\nsecond line"
            },
            {"branch": "two", "passed": true}
        ]);
        let context = Context::new();
        context.set(keys::PARALLEL_RESULTS, parallel_results.clone());
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_sandbox_services(mock.clone());

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert_eq!(
            *mock.captured_stdin.lock().unwrap(),
            Some(serde_json::to_vec(&parallel_results).unwrap())
        );
        assert!(
            !mock
                .captured_command
                .lock()
                .unwrap()
                .clone()
                .expect("command should run")
                .contains("must-not-run"),
            "stdin content must not be inserted into shell source"
        );
    }

    #[tokio::test]
    async fn stdin_source_passes_strings_without_adding_a_newline() {
        let mock = std::sync::Arc::new(MockSandbox::default());
        let handler = CommandHandler;
        let mut node = Node::new("consume");
        node.attrs
            .insert("script".to_string(), AttrValue::String("cat".to_string()));
        node.attrs.insert(
            "stdin_source".to_string(),
            AttrValue::String("context.input".to_string()),
        );
        let context = Context::new();
        context.set("input", serde_json::json!("first\nlast"));
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_sandbox_services(mock.clone());

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert_eq!(
            mock.captured_stdin.lock().unwrap().as_deref(),
            Some(b"first\nlast".as_slice())
        );
    }

    #[tokio::test]
    async fn missing_stdin_source_fails_before_starting_the_command() {
        let mock = std::sync::Arc::new(MockSandbox::default());
        let handler = CommandHandler;
        let mut node = Node::new("consume");
        node.attrs
            .insert("script".to_string(), AttrValue::String("cat".to_string()));
        node.attrs.insert(
            "stdin_source".to_string(),
            AttrValue::String("context.missing".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_sandbox_services(mock.clone());

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(
            outcome.failure_category(),
            Some(FailureCategory::Deterministic)
        );
        assert!(
            outcome
                .failure_reason()
                .unwrap()
                .contains("was not found in workflow context")
        );
        assert_eq!(*mock.captured_command.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn simulation_validates_stdin_source_without_resolving_context() {
        let handler = CommandHandler;
        let mut valid = Node::new("valid");
        valid
            .attrs
            .insert("script".to_string(), AttrValue::String("cat".to_string()));
        valid.attrs.insert(
            "stdin_source".to_string(),
            AttrValue::String("context.not_available_in_dry_run".to_string()),
        );
        let mut invalid = valid.clone();
        invalid.id = "invalid".to_string();
        invalid
            .attrs
            .insert("stdin_source".to_string(), AttrValue::Integer(7));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_services();

        let valid_outcome = handler
            .simulate(&valid, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        let invalid_outcome = handler
            .simulate(&invalid, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(valid_outcome.status, StageOutcome::Succeeded);
        assert_eq!(
            invalid_outcome.failure_category(),
            Some(FailureCategory::Deterministic)
        );
    }

    struct RefreshingMinter {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl fabro_github::test_support::InstallationTokenMinter for RefreshingMinter {
        async fn mint(&self) -> anyhow::Result<fabro_github::InstallationToken> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Ok(fabro_github::InstallationToken {
                token:      format!("ghs_{call}"),
                expires_at: chrono::Utc::now() + chrono::Duration::minutes(10),
            })
        }
    }

    #[tokio::test]
    async fn executes_script_via_sandbox() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      "SANDBOX_MARKER\n".into(),
            stderr:      String::new(),
            exit_code:   Some(0),
            termination: CommandTermination::Exited,
            duration_ms: 5,
        }));

        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_sandbox_services(spy.clone());
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        let command_output = outcome.context_updates.get(keys::COMMAND_OUTPUT).unwrap();
        assert_eq!(
            command_text(&services, command_output).await,
            "SANDBOX_MARKER\n",
            "CommandHandler must delegate to the sandbox, not spawn a host process"
        );
        assert_eq!(
            spy.captured_command().as_deref(),
            Some("exec 2>&1\necho hello"),
            "sandbox should receive the wrapped script as the command"
        );
    }

    #[tokio::test]
    async fn executes_python_script_via_sandbox() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      "PYTHON_SANDBOX\n".into(),
            stderr:      String::new(),
            exit_code:   Some(0),
            termination: CommandTermination::Exited,
            duration_ms: 5,
        }));

        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("print('hi')".to_string()),
        );
        node.attrs.insert(
            "language".to_string(),
            AttrValue::String("python".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(
                &node,
                &context,
                &graph,
                run_dir.path(),
                &make_sandbox_services(spy.clone()),
            )
            .await
            .unwrap();

        assert_eq!(outcome.status, StageOutcome::Succeeded);
        let captured = spy.captured_command().unwrap();
        assert!(
            captured.starts_with("exec 2>&1\npython3 -c ") && captured.contains("print"),
            "sandbox command should invoke python3 with the script, got: {captured}"
        );
    }

    #[tokio::test]
    async fn passes_env_vars_to_sandbox() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      String::new(),
            stderr:      String::new(),
            exit_code:   Some(0),
            termination: CommandTermination::Exited,
            duration_ms: 5,
        }));

        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs
            .insert("script".to_string(), AttrValue::String("true".to_string()));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let mut services = make_sandbox_services(spy.clone());
        services
            .base_env
            .insert("MY_VAR".to_string(), "my_value".to_string());

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        let captured_env = spy.captured_env_vars.lock().unwrap().clone().unwrap();
        assert_eq!(
            captured_env.get("MY_VAR").map(String::as_str),
            Some("my_value")
        );
    }

    #[tokio::test]
    async fn refreshes_github_token_for_each_command_stage_when_near_expiry() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      String::new(),
            stderr:      String::new(),
            exit_code:   Some(0),
            termination: CommandTermination::Exited,
            duration_ms: 5,
        }));
        let minter = std::sync::Arc::new(RefreshingMinter {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut services = make_sandbox_services(spy.clone());
        services.github_token = Some(fabro_github::test_support::installation_token_source(
            "owner/repo",
            minter.clone(),
        ));

        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs
            .insert("script".to_string(), AttrValue::String("true".to_string()));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(
            spy.captured_env_vars
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|env| env.get("GITHUB_TOKEN"))
                .map(String::as_str),
            Some("ghs_1")
        );

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(
            spy.captured_env_vars
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|env| env.get("GITHUB_TOKEN"))
                .map(String::as_str),
            Some("ghs_2")
        );
        assert_eq!(minter.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn passes_run_cancellation_to_sandbox() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      String::new(),
            stderr:      String::new(),
            exit_code:   Some(0),
            termination: CommandTermination::Exited,
            duration_ms: 5,
        }));

        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs
            .insert("script".to_string(), AttrValue::String("true".to_string()));
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let mut services = make_sandbox_services(spy.clone());
        services.run = services
            .run
            .with_cancel_token(tokio_util::sync::CancellationToken::new());

        handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();

        assert_eq!(*spy.captured_cancel_token.lock().unwrap(), Some(true));
    }

    #[tokio::test]
    async fn script_handler_timeout_error_includes_output_tails() {
        let spy = std::sync::Arc::new(SpySandbox::new(fabro_agent::sandbox::ExecResult {
            stdout:      "partial stdout\n".into(),
            stderr:      "partial stderr\n".into(),
            exit_code:   None,
            termination: CommandTermination::TimedOut,
            duration_ms: 50,
        }));

        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("sleep 10".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let err = handler
            .execute(
                &node,
                &context,
                &graph,
                run_dir.path(),
                &make_sandbox_services(spy),
            )
            .await
            .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("timed out"), "got: {message}");
        assert!(
            message.contains("partial stdout"),
            "timeout error should include output tail, got: {message}"
        );
        assert!(
            message.contains("partial stderr"),
            "timeout error should include merged output tail, got: {message}"
        );
    }

    #[tokio::test]
    async fn tool_output_context_key_not_emitted() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo dual".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &make_services())
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Succeeded);
        assert!(outcome.context_updates.contains_key(keys::COMMAND_OUTPUT));
        assert!(
            !outcome.context_updates.contains_key("tool.output"),
            "tool.output should not be emitted"
        );
    }

    #[tokio::test]
    async fn script_handler_failure_includes_output() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(r#"echo "build output" && echo "oops" >&2 && exit 1"#.to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();

        let services = make_services();
        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        let reason = outcome.failure_reason().unwrap();
        assert!(
            reason.contains("build output"),
            "failure_reason should contain output, got: {reason}"
        );
        assert!(
            reason.contains("oops"),
            "failure_reason should contain merged stderr, got: {reason}"
        );
        assert!(
            reason.contains("exit code: 1"),
            "failure_reason should contain exit code, got: {reason}"
        );
    }

    #[tokio::test]
    async fn script_handler_spawn_failure() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String("echo hello".to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_sandbox_services(std::sync::Arc::new(SpySandbox::fail("No such file")));

        let err = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("Failed to spawn script"));
        let stage_id = StageId::new("script_node", 1);
        assert!(
            !command_log_path(run_dir.path(), &stage_id).exists(),
            "spawn failure should remove pre-created output scratch log"
        );
    }

    #[tokio::test]
    async fn script_handler_failure_sets_command_output() {
        let handler = CommandHandler;
        let mut node = Node::new("script_node");
        node.attrs.insert(
            "script".to_string(),
            AttrValue::String(r#"echo "build output" && exit 1"#.to_string()),
        );
        let context = Context::new();
        let graph = Graph::new("test");
        let run_dir = tempfile::tempdir().unwrap();
        let services = make_services();

        let outcome = handler
            .execute(&node, &context, &graph, run_dir.path(), &services)
            .await
            .unwrap();
        assert_eq!(outcome.status, StageOutcome::Failed {
            retry_requested: false,
        });
        let command_output = outcome
            .context_updates
            .get(keys::COMMAND_OUTPUT)
            .expect("command.output should be set on failure");
        assert!(
            command_text(&services, command_output)
                .await
                .contains("build output"),
            "command.output should contain output, got: {command_output:?}"
        );
    }
}
