//! Agent stages on pebble's `CodingAgent`, driven end to end through the
//! workflow engine against a scripted OpenAI-compatible model.
//!
//! Each test covers one behaviour the pebble backend owes the run: the tool
//! vocabulary of every harness profile, steering, interrupts, cancellation,
//! the stage timeout, questions, subagents, MCP tools, model failover, and a
//! failing event sink.

#![allow(
    clippy::absolute_paths,
    clippy::items_after_statements,
    clippy::large_futures,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "These integration tests value explicit scenarios over pedantic style lints."
)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fabro_auth::test_support as auth_test_support;
use fabro_graphviz::graph::{AttrValue, Edge, Graph, Node};
use fabro_sandbox::RunSandbox;
use fabro_types::settings::{McpServerSettings, McpTransport, ModelRef};
use fabro_types::{
    EventBody, Principal, RunEvent, RunId, StageId, SystemActorKind, WorkflowSettings,
};
use fabro_workflow::context::Context;
use fabro_workflow::error::Error;
use fabro_workflow::event::{Emitter, RunEventLogger, RunEventSink};
use fabro_workflow::handler::HandlerRegistry;
use fabro_workflow::handler::agent::{AgentHandler, CodergenBackend, CodergenRunRequest};
use fabro_workflow::handler::exit::ExitHandler;
use fabro_workflow::handler::llm::PebbleBackend;
use fabro_workflow::handler::start::StartHandler;
use fabro_workflow::model_fallback::{self, ModelFallbackPolicy};
use fabro_workflow::outcome::{Outcome, StageOutcome};
use fabro_workflow::run_options::RunOptions;
use fabro_workflow::steering_hub::SteeringHub;
use fabro_workflow::test_support::WorkflowRunner;
use httpmock::Method::POST;
use httpmock::MockServer;
use lithos_llm::catalog::ProviderId;
use pebble_coding_agent::events::{CodingEvent, FailoverStop};
use tokio_util::sync::CancellationToken;

const MODEL: &str = "mock-model";
const PROVIDER: &str = "mock";
const CHAT_PATH: &str = "/v1/chat/completions";
const TOOL_RESULT_MARKER: &str = r#""role":"tool""#;
const INPUT_TOKENS_PER_CALL: i64 = 11;
const OUTPUT_TOKENS_PER_CALL: i64 = 7;

// --- Scripted model ---------------------------------------------------------

fn chat_chunk(delta: &serde_json::Value, finish_reason: Option<&str>) -> String {
    let chunk = serde_json::json!({
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "model": MODEL,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish_reason,
        }]
    });
    format!("data: {chunk}\n\n")
}

fn usage_chunk() -> String {
    let chunk = serde_json::json!({
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "model": MODEL,
        "choices": [],
        "usage": {
            "prompt_tokens": INPUT_TOKENS_PER_CALL,
            "completion_tokens": OUTPUT_TOKENS_PER_CALL,
            "total_tokens": INPUT_TOKENS_PER_CALL + OUTPUT_TOKENS_PER_CALL,
        }
    });
    format!("data: {chunk}\n\n")
}

/// A streamed assistant answer of `text`.
fn sse_text(text: &str) -> String {
    let mut body = chat_chunk(&serde_json::json!({ "role": "assistant" }), None);
    body.push_str(&chat_chunk(&serde_json::json!({ "content": text }), None));
    body.push_str(&chat_chunk(&serde_json::json!({}), Some("stop")));
    body.push_str(&usage_chunk());
    body.push_str("data: [DONE]\n\n");
    body
}

/// A streamed assistant turn calling `tool` with `arguments`.
fn sse_tool_call(tool_call_id: &str, tool: &str, arguments: &serde_json::Value) -> String {
    let mut body = chat_chunk(&serde_json::json!({ "role": "assistant" }), None);
    body.push_str(&chat_chunk(
        &serde_json::json!({
            "tool_calls": [{
                "index": 0,
                "id": tool_call_id,
                "type": "function",
                "function": {
                    "name": tool,
                    "arguments": arguments.to_string(),
                }
            }]
        }),
        None,
    ));
    body.push_str(&chat_chunk(&serde_json::json!({}), Some("tool_calls")));
    body.push_str(&usage_chunk());
    body.push_str("data: [DONE]\n\n");
    body
}

fn sse_headers(then: httpmock::Then, body: String) -> httpmock::Then {
    then.status(200)
        .header("content-type", "text/event-stream")
        .body(body)
}

/// One OpenAI-compatible provider on `server`, reached at `base_path`, whose
/// models run under `profile`. Priced so a call's cost is checkable: one
/// microdollar per input token, two per output token.
fn provider_toml(name: &str, model: &str, base_url: &str, profile: &str) -> String {
    format!(
        r#"
[providers.{name}]
display_name = "{name}"
adapter = "openai-compatible"
codec = "openai-chat"
base_url = {base_url}
auth = {{ type = "bearer" }}
default_model = "{model}"

[providers.{name}.metadata.agent]
profile = "{profile}"

[providers.{name}.models.{model}]
display_name = "{model}"
api_model = "{model}"
limits = {{ context_tokens = 100000, max_output_tokens = 1024 }}
capabilities = {{ text = true, tools = true }}
pricing = {{ input_usd_micros_per_million = 1000000, output_usd_micros_per_million = 2000000 }}
"#,
        base_url = toml::Value::String(base_url.to_string()),
    )
}

fn mock_catalog(server: &MockServer, profile: &str) -> Arc<fabro_llm::lithos_catalog::Catalog> {
    Arc::new(fabro_llm::test_support::test_catalog_with_overlay(
        &provider_toml(PROVIDER, MODEL, &server.url("/v1"), profile),
    ))
}

fn mock_credentials() -> Arc<dyn fabro_llm::credentials::CredentialProvider> {
    auth_test_support::env_credential_source(|name| {
        name.ends_with("_API_KEY").then(|| "sk-test".to_string())
    })
}

fn mock_backend(server: &MockServer, profile: &str, hub: Arc<SteeringHub>) -> PebbleBackend {
    PebbleBackend::new_with_catalog(
        MODEL.to_string(),
        ProviderId::new(PROVIDER),
        ModelFallbackPolicy::default(),
        mock_credentials(),
        hub,
        mock_catalog(server, profile),
    )
}

// --- Workflow harness -------------------------------------------------------

/// `start -> work -> exit`, where `work` is an agent stage prompted with
/// `prompt`.
fn agent_graph(name: &str, prompt: &str) -> Graph {
    let mut graph = Graph::new(name);
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
    let mut work = Node::new("work");
    work.attrs
        .insert("prompt".to_string(), AttrValue::String(prompt.to_string()));
    graph.nodes.insert("work".to_string(), work);
    graph.edges.push(Edge::new("start", "work"));
    graph.edges.push(Edge::new("work", "exit"));
    graph
}

fn run_options(run_dir: &Path, cancel_token: CancellationToken) -> RunOptions {
    RunOptions {
        settings: WorkflowSettings::default(),
        run_dir: run_dir.to_path_buf(),
        cancel_token,
        run_id: RunId::new(),
        labels: std::collections::HashMap::new(),
        workflow_slug: None,
        github_app: None,
        base_branch: None,
        display_base_sha: None,
        git_identity: None,
        pre_run_git: None,
        fork_source_ref: None,
        git: None,
    }
}

async fn local_sandbox(dir: &Path) -> Arc<RunSandbox> {
    Arc::new(
        fabro_sandbox::local_sandbox(dir.to_path_buf())
            .await
            .expect("local sandbox should be created"),
    )
}

fn agent_registry(backend: PebbleBackend) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new(Box::new(AgentHandler::new(Some(Box::new(backend)))));
    registry.register("start", Box::new(StartHandler));
    registry.register("exit", Box::new(ExitHandler));
    registry
}

/// Every run event the run emitted, in order.
type Events = Arc<Mutex<Vec<RunEvent>>>;

fn observe(emitter: &Emitter) -> Events {
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    emitter.on_event(move |event| sink.lock().unwrap().push(event.clone()));
    events
}

fn names(events: &Events) -> Vec<String> {
    events
        .lock()
        .unwrap()
        .iter()
        .map(|event| event.event_name().to_string())
        .collect()
}

fn position(events: &Events, name: &str) -> Option<usize> {
    names(events).iter().position(|actual| actual == name)
}

fn count(events: &Events, name: &str) -> usize {
    names(events)
        .iter()
        .filter(|actual| *actual == name)
        .count()
}

/// Whether the event at `index` was emitted for the `work` stage.
fn work_stage_event(events: &Events, index: usize) -> bool {
    events.lock().unwrap()[index].node_id.as_deref() == Some("work")
}

fn coding_events(events: &Events) -> Vec<(RunEvent, CodingEvent)> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match &event.body {
            EventBody::Agent(props) => Some((event.clone(), props.event.event.clone())),
            _ => None,
        })
        .collect()
}

/// The everything-in-one-place fixture: a scripted model, a temp working
/// directory, an observed emitter, and a steering hub.
struct Stage {
    server:  MockServer,
    dir:     tempfile::TempDir,
    emitter: Arc<Emitter>,
    events:  Events,
    hub:     Arc<SteeringHub>,
}

impl Stage {
    async fn new() -> Self {
        let server = MockServer::start_async().await;
        let dir = tempfile::tempdir().unwrap();
        let emitter = Arc::new(Emitter::default());
        let events = observe(&emitter);
        let hub = Arc::new(SteeringHub::new(Arc::clone(&emitter)));
        Self {
            server,
            dir,
            emitter,
            events,
            hub,
        }
    }

    fn backend(&self, profile: &str) -> PebbleBackend {
        mock_backend(&self.server, profile, Arc::clone(&self.hub))
    }

    fn file(&self, name: &str) -> String {
        self.dir.path().join(name).display().to_string()
    }

    async fn run(
        &self,
        backend: PebbleBackend,
        graph: &Graph,
        cancel_token: CancellationToken,
    ) -> Result<(Outcome, fabro_types::RunProjection), Error> {
        let sandbox = local_sandbox(self.dir.path()).await;
        let runner =
            WorkflowRunner::new(agent_registry(backend), Arc::clone(&self.emitter), sandbox);
        let options = run_options(self.dir.path(), cancel_token);
        runner.run_with_state(graph, &options).await
    }

    /// Runs `graph` and returns the `work` stage's response.
    async fn run_ok(&self, backend: PebbleBackend, graph: &Graph) -> fabro_types::RunProjection {
        let (outcome, state) = self
            .run(backend, graph, CancellationToken::new())
            .await
            .expect("workflow execution should complete");
        assert_eq!(outcome.status, StageOutcome::Succeeded, "{outcome:?}");
        state
    }

    /// Fires `action` once, when the stage's first model call starts.
    fn on_first_llm_call(&self, action: impl Fn() + Send + Sync + 'static) {
        let fired = AtomicBool::new(false);
        self.emitter.on_event(move |event| {
            if event.event_name() == "agent.llm.started" && !fired.swap(true, Ordering::SeqCst) {
                action();
            }
        });
    }
}

fn work_stage(state: &fabro_types::RunProjection) -> &fabro_types::StageProjection {
    state
        .stage(&StageId::new("work", 1))
        .expect("the work stage should be projected")
}

// --- Profiles ---------------------------------------------------------------

/// One agent stage under `profile`: the model writes a file with the profile's
/// own spelling of the write tool and answers "Done". Checks the event
/// sequence, the files the stage touched, the response, usage, and cost.
async fn write_file_under_profile(profile: &str, tool: &str, path_key: &str) {
    let stage = Stage::new().await;
    let path = stage.file("hello.txt");
    let arguments = serde_json::json!({ path_key: path, "content": "hello from the model" });
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_excludes(TOOL_RESULT_MARKER);
            sse_headers(then, sse_tool_call("call-1", tool, &arguments));
        })
        .await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes(TOOL_RESULT_MARKER);
            sse_headers(then, sse_text("Done"));
        })
        .await;

    let backend = stage.backend(profile);
    let graph = agent_graph("Profile", "Create hello.txt");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "hello from the model",
        "{profile}: the write tool should reach the sandbox"
    );
    let work = work_stage(&state);
    assert_eq!(work.response.as_deref(), Some("Done"), "{profile}");
    assert_eq!(
        work.usage.input_tokens,
        2 * INPUT_TOKENS_PER_CALL,
        "{profile}: two model calls of input"
    );
    assert_eq!(
        work.usage.output_tokens,
        2 * OUTPUT_TOKENS_PER_CALL,
        "{profile}"
    );
    assert_eq!(
        work.usage.total_usd_micros,
        Some(2 * (INPUT_TOKENS_PER_CALL + 2 * OUTPUT_TOKENS_PER_CALL)),
        "{profile}: cost from the catalog's pricing"
    );
    let checkpoint = state.current_checkpoint().expect("a checkpoint");
    let outcome = checkpoint
        .node_outcomes
        .get("work")
        .expect("the work outcome");
    assert_eq!(outcome.files_touched, vec![path.clone()], "{profile}");

    // The assistant message that carries the tool call comes before the
    // tool runs; the answer comes after; the stage closes after the session.
    let sequence = [
        "agent.session.started",
        "agent.message",
        "agent.tool.started",
        "agent.tool.completed",
        "agent.llm.started",
        "agent.message",
        "agent.session.ended",
        "stage.completed",
    ];
    let mut cursor = 0;
    let all_names = names(&stage.events);
    for name in sequence {
        let found = all_names
            .iter()
            .enumerate()
            .skip(cursor)
            .find(|(index, actual)| {
                *actual == name
                    && (name != "stage.completed" || work_stage_event(&stage.events, *index))
            })
            .map(|(index, _)| index);
        let Some(index) = found else {
            panic!("{profile}: {name} should follow position {cursor}, got {all_names:?}");
        };
        cursor = index + 1;
    }
    assert_eq!(count(&stage.events, "agent.message"), 2, "{profile}");
    let tool_started = coding_events(&stage.events)
        .into_iter()
        .find_map(|(_, event)| match event {
            CodingEvent::ToolCallStarted { tool_name, .. } => Some(tool_name),
            _ => None,
        })
        .expect("the tool call should be reported");
    assert_eq!(
        tool_started, tool,
        "{profile}: the tool keeps the profile's name"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openai_profile_writes_a_file() {
    write_file_under_profile("openai", "write_file", "file_path").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anthropic_profile_writes_a_file() {
    write_file_under_profile("anthropic", "write_file", "file_path").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_5_profile_writes_a_file() {
    write_file_under_profile("claude-5", "Write", "file_path").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gemini_profile_writes_a_file() {
    write_file_under_profile("gemini", "write_file", "file_path").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kimi_profile_writes_a_file() {
    write_file_under_profile("kimi", "Write", "path").await;
}

/// The codex vocabulary edits through `apply_patch`, a custom tool the chat
/// codec cannot carry, so this one runs on the OpenAI twin's responses API.
#[fabro_macros::e2e_test(twin)]
async fn codex_vocabulary_applies_a_patch() {
    use fabro_test::{TwinScenario, TwinScenarios, TwinToolCall};

    let twin = fabro_test::twin_openai().await;
    let namespace = format!("{}::{}", module_path!(), line!());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("codex.txt").display().to_string();
    TwinScenarios::new(namespace.clone())
        .scenario(
            TwinScenario::responses("gpt-5.6-sol")
                .tool_call(TwinToolCall::custom(
                    "apply_patch",
                    format!("*** Begin Patch\n*** Add File: {path}\n+hello codex\n*** End Patch"),
                ))
                .text("Done"),
        )
        .load(twin)
        .await;

    let base_url = twin.base_url.clone();
    let catalog = fabro_llm::build_catalog(&fabro_config::LlmLayer::default(), &move |name| {
        (name == fabro_static::EnvVars::OPENAI_BASE_URL).then(|| base_url.clone())
    })
    .expect("twin catalog should build");
    let api_key = namespace.clone();
    let source = auth_test_support::env_credential_source(move |name| {
        (name == fabro_static::EnvVars::OPENAI_API_KEY).then(|| api_key.clone())
    });
    let emitter = Arc::new(Emitter::default());
    let events = observe(&emitter);
    let backend = PebbleBackend::new_with_catalog(
        "gpt-5.6-sol".to_string(),
        lithos_llm::catalog::builtin::openai(),
        ModelFallbackPolicy::default(),
        source,
        Arc::new(SteeringHub::new(Arc::clone(&emitter))),
        Arc::new(catalog),
    );

    let sandbox = local_sandbox(dir.path()).await;
    let runner = WorkflowRunner::new(agent_registry(backend), emitter, sandbox);
    let graph = agent_graph("Codex", "Create codex.txt");
    let (outcome, state) = runner
        .run_with_state(&graph, &run_options(dir.path(), CancellationToken::new()))
        .await
        .expect("workflow execution should complete");
    assert_eq!(outcome.status, StageOutcome::Succeeded, "{outcome:?}");

    let written = tokio::fs::read_to_string(&path)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "codex.txt should be written ({error}); events {:?}; tool calls {:?}",
                names(&events),
                coding_events(&events)
                    .into_iter()
                    .filter(|(_, event)| matches!(
                        event,
                        CodingEvent::ToolCallStarted { .. } | CodingEvent::ToolCallCompleted { .. }
                    ))
                    .map(|(_, event)| event)
                    .collect::<Vec<_>>(),
            )
        });
    assert_eq!(written.trim_end(), "hello codex");
    let checkpoint = state.current_checkpoint().expect("a checkpoint");
    assert_eq!(
        checkpoint.node_outcomes["work"].files_touched,
        vec![path],
        "apply_patch adds count as touched files"
    );
    assert!(position(&events, "agent.tool.completed").is_some());
}

// --- Steering, interrupts, cancellation, timeout -----------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_steer_delivered_mid_stage_reaches_the_model() {
    let stage = Stage::new().await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_excludes("mention the steer");
            sse_headers(then, sse_text("First answer")).delay(Duration::from_millis(300));
        })
        .await;
    let steered = stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes("mention the steer");
            sse_headers(then, sse_text("Steered answer"));
        })
        .await;

    let hub = Arc::clone(&stage.hub);
    stage.on_first_llm_call(move || {
        hub.deliver_steer("Please also mention the steer".to_string(), None);
    });

    let backend = stage.backend("openai");
    let graph = agent_graph("Steer", "Say hello");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(steered.calls_async().await, 1, "{:?}", names(&stage.events));
    assert_eq!(
        work_stage(&state).response.as_deref(),
        Some("Steered answer")
    );
    assert_eq!(count(&stage.events, "run.steer"), 1);
    assert_eq!(
        count(&stage.events, "agent.steering.injected"),
        1,
        "the steer is recorded as steering, got {:?}",
        names(&stage.events)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupt_with_a_steer_abandons_the_round() {
    let stage = Stage::new().await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path(CHAT_PATH).body_excludes("STOPPED");
            sse_headers(then, sse_text("Original answer")).delay(Duration::from_millis(800));
        })
        .await;
    let steered = stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path(CHAT_PATH).body_includes("STOPPED");
            sse_headers(then, sse_text("Stopped as asked"));
        })
        .await;

    let hub = Arc::clone(&stage.hub);
    stage.on_first_llm_call(move || {
        hub.interrupt_then_steer("Stop and reply STOPPED", None);
    });

    let backend = stage.backend("openai");
    let graph = agent_graph("Interrupt", "Write an essay");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(steered.calls_async().await, 1, "{:?}", names(&stage.events));
    assert_eq!(
        work_stage(&state).response.as_deref(),
        Some("Stopped as asked")
    );
    assert_eq!(count(&stage.events, "run.interrupt"), 1);
    assert_eq!(count(&stage.events, "agent.interrupt.injected"), 1);
    assert_eq!(
        count(&stage.events, "agent.round.interrupted"),
        1,
        "pebble announces the abandoned round once, got {:?}",
        names(&stage.events)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_the_run_ends_the_stage_as_cancelled() {
    let stage = Stage::new().await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path(CHAT_PATH);
            sse_headers(then, sse_text("Too late")).delay(Duration::from_secs(2));
        })
        .await;

    let cancel_token = CancellationToken::new();
    let trigger = cancel_token.clone();
    stage.on_first_llm_call(move || trigger.cancel());

    let backend = stage.backend("openai");
    let graph = agent_graph("Cancel", "Take your time");
    let started = std::time::Instant::now();
    let result = stage.run(backend, &graph, cancel_token).await;

    let error = result.expect_err("a cancelled run fails");
    assert!(matches!(error, Error::Cancelled), "got {error:#}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "cancellation should not wait for the model"
    );
    let work_completed = names(&stage.events)
        .iter()
        .enumerate()
        .any(|(index, name)| name == "stage.completed" && work_stage_event(&stage.events, index));
    assert!(!work_completed, "got {:?}", names(&stage.events));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stage_timeout_fails_a_slow_agent() {
    let stage = Stage::new().await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path(CHAT_PATH);
            sse_headers(then, sse_text("Too late")).delay(Duration::from_secs(2));
        })
        .await;

    let mut graph = agent_graph("Timeout", "Take your time");
    let work = graph.nodes.get_mut("work").unwrap();
    work.attrs.insert(
        "timeout".to_string(),
        AttrValue::Duration(Duration::from_millis(300)),
    );
    work.attrs
        .insert("max_retries".to_string(), AttrValue::Integer(0));
    graph.edges.retain(|edge| edge.from != "work");
    let mut fail_edge = Edge::new("work", "exit");
    fail_edge.attrs.insert(
        "condition".to_string(),
        AttrValue::String("outcome=failed".to_string()),
    );
    graph.edges.push(fail_edge);

    let backend = stage.backend("openai");
    let (_, state) = stage
        .run(backend, &graph, CancellationToken::new())
        .await
        .expect("the fail edge carries the run to exit");

    let completion = work_stage(&state)
        .completion
        .as_ref()
        .expect("the work stage completes");
    assert_eq!(completion.outcome, StageOutcome::Failed {
        retry_requested: false,
    });
    let failed = stage
        .events
        .lock()
        .unwrap()
        .iter()
        .find(|event| {
            event.event_name() == "stage.failed" && event.node_id.as_deref() == Some("work")
        })
        .cloned()
        .expect("the stage failure is emitted");
    assert_eq!(
        failed.actor,
        Some(Principal::System {
            system_kind: SystemActorKind::Timeout,
        })
    );
}

// --- Questions, subagents, MCP
// --------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_question_is_answered_through_the_interviewer() {
    let stage = Stage::new().await;
    let question = serde_json::json!({
        "questions": [{
            "id": "ship",
            "header": "Ship",
            "question": "Ship it?",
            "options": [
                { "label": "Yes", "description": "Ship now" },
                { "label": "No", "description": "Hold" }
            ]
        }]
    });
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_excludes(TOOL_RESULT_MARKER);
            sse_headers(
                then,
                sse_tool_call("call-1", "request_user_input", &question),
            );
        })
        .await;
    let answered = stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes(TOOL_RESULT_MARKER)
                .body_includes("Yes");
            sse_headers(then, sse_text("Shipping"));
        })
        .await;

    let backend = stage.backend("openai");
    let graph = agent_graph("Question", "Decide whether to ship");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(
        answered.calls_async().await,
        1,
        "{:?}",
        names(&stage.events)
    );
    assert_eq!(work_stage(&state).response.as_deref(), Some("Shipping"));
    assert_eq!(count(&stage.events, "interview.started"), 1);
    let completed = stage
        .events
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| match &event.body {
            EventBody::InterviewCompleted(props) => Some(props.clone()),
            _ => None,
        })
        .expect("the interview completes");
    assert!(completed.question.contains("Ship it?"), "got {completed:?}");
    assert!(completed.answer.contains("Yes"), "got {completed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subagent_runs_under_its_parent_session() {
    let stage = Stage::new().await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes("Delegate the review")
                .body_excludes(TOOL_RESULT_MARKER);
            sse_headers(
                then,
                sse_tool_call(
                    "call-1",
                    "spawn_agent",
                    &serde_json::json!({ "task": "Inspect the module" }),
                ),
            );
        })
        .await;
    let child = stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes("Inspect the module")
                .body_excludes("Delegate the review");
            sse_headers(then, sse_text("Child done: 42"));
        })
        .await;
    // Spawning answers at once with the child's id; the parent then waits for
    // every child, and the wait result carries the child's answer.
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes("Delegate the review")
                .body_includes(TOOL_RESULT_MARKER)
                .body_excludes("Child done: 42");
            sse_headers(
                then,
                sse_tool_call("call-2", "wait", &serde_json::json!({})),
            );
        })
        .await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes("Delegate the review")
                .body_includes("Child done: 42");
            sse_headers(then, sse_text("Parent done"));
        })
        .await;

    let backend = stage.backend("openai");
    let graph = agent_graph("Subagent", "Delegate the review");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(child.calls_async().await, 1, "{:?}", names(&stage.events));
    assert_eq!(work_stage(&state).response.as_deref(), Some("Parent done"));
    assert_eq!(count(&stage.events, "agent.sub.spawned"), 1);

    let agent_events = coding_events(&stage.events);
    let root_session = agent_events
        .iter()
        .find_map(|(event, coding)| {
            matches!(coding, CodingEvent::SessionStarted { .. })
                .then(|| event.session_id.clone())
                .flatten()
        })
        .expect("the root session starts");
    let child_events: Vec<&RunEvent> = agent_events
        .iter()
        .map(|(event, _)| event)
        .filter(|event| event.parent_session_id.is_some())
        .collect();
    assert!(
        !child_events.is_empty(),
        "child events carry a parent session id, got {:?}",
        names(&stage.events)
    );
    for event in child_events {
        assert_eq!(
            event.parent_session_id.as_deref(),
            Some(root_session.as_str())
        );
        assert_ne!(event.session_id.as_deref(), Some(root_session.as_str()));
    }
    let root_events = agent_events
        .iter()
        .filter(|(event, _)| event.session_id.as_deref() == Some(root_session.as_str()));
    assert!(
        root_events.clone().count() > 0
            && root_events
                .into_iter()
                .all(|(event, _)| event.parent_session_id.is_none()),
        "root events carry no parent session id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_mcp_tool_is_available_to_the_stage() {
    let stage = Stage::new().await;
    stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_excludes(TOOL_RESULT_MARKER);
            sse_headers(
                then,
                sse_tool_call(
                    "call-1",
                    "mcp__echo__echo",
                    &serde_json::json!({ "message": "hello mcp" }),
                ),
            );
        })
        .await;
    let echoed = stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes(TOOL_RESULT_MARKER)
                .body_includes("hello mcp");
            sse_headers(then, sse_text("Echoed"));
        })
        .await;

    let server_script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fabro-mcp/tests/test_mcp_server.py")
        .canonicalize()
        .expect("the MCP test server ships with fabro-mcp");
    let backend = stage
        .backend("openai")
        .with_mcp_servers(vec![McpServerSettings {
            name: "echo".to_string(),
            transport: McpTransport::Stdio {
                command: vec!["python3".to_string(), server_script.display().to_string()],
                env:     std::collections::HashMap::new(),
            },
            ..McpServerSettings::default()
        }]);
    let graph = agent_graph("Mcp", "Echo hello mcp");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(echoed.calls_async().await, 1, "{:?}", names(&stage.events));
    assert_eq!(work_stage(&state).response.as_deref(), Some("Echoed"));
    let ready = stage
        .events
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| match &event.body {
            EventBody::AgentMcpReady(props) => Some(props.clone()),
            _ => None,
        })
        .expect("the MCP server reports ready");
    assert_eq!(ready.server_name, "echo");
    assert_eq!(ready.tool_count, 1);
    let completed = coding_events(&stage.events)
        .into_iter()
        .find_map(|(_, event)| match event {
            CodingEvent::ToolCallCompleted {
                tool_name, output, ..
            } => Some((tool_name, output)),
            _ => None,
        })
        .expect("the MCP tool call completes");
    assert_eq!(completed.0, "mcp__echo__echo");
    assert!(
        completed.1.to_string().contains("hello mcp"),
        "got {}",
        completed.1
    );
}

// --- Failover ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_continues_the_conversation_without_rerunning_tools() {
    let stage = Stage::new().await;
    let path = stage.file("failover.txt");
    let arguments = serde_json::json!({ "file_path": path, "content": "written once" });
    let primary_tool_call = stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/primary/v1/chat/completions")
                .body_excludes(TOOL_RESULT_MARKER);
            sse_headers(then, sse_tool_call("call-1", "write_file", &arguments));
        })
        .await;
    let primary_failure = stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path("/primary/v1/chat/completions");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "error": { "message": "primary key revoked", "type": "invalid_request_error" }
                }));
        })
        .await;
    let backup = stage
        .server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/backup/v1/chat/completions")
                .body_includes(TOOL_RESULT_MARKER)
                .body_includes("write_file");
            sse_headers(then, sse_text("Recovered on backup"));
        })
        .await;

    let overlay = format!(
        "{}\n{}",
        provider_toml(
            "primary",
            "primary-model",
            &stage.server.url("/primary/v1"),
            "openai"
        ),
        provider_toml(
            "backup",
            "backup-model",
            &stage.server.url("/backup/v1"),
            "openai"
        ),
    );
    let catalog = Arc::new(fabro_llm::test_support::test_catalog_with_overlay(&overlay));
    let primary = ProviderId::new("primary");
    let fallbacks = model_fallback::resolve_model_fallbacks(
        &catalog,
        &[primary.clone(), ProviderId::new("backup")],
        &BTreeMap::from([("primary-model".to_string(), vec![
            "backup/backup-model".parse::<ModelRef>().unwrap(),
        ])]),
    )
    .expect("the fallback chain resolves");
    assert!(fallbacks.notices.is_empty(), "{:?}", fallbacks.notices);
    let backend = PebbleBackend::new_with_catalog(
        "primary-model".to_string(),
        primary,
        fallbacks.policy,
        mock_credentials(),
        Arc::clone(&stage.hub),
        catalog,
    );

    let graph = agent_graph("Failover", "Create failover.txt");
    let state = stage.run_ok(backend, &graph).await;

    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "written once"
    );
    assert_eq!(primary_tool_call.calls_async().await, 1);
    assert_eq!(
        primary_failure.calls_async().await,
        1,
        "an auth failure is not retried on the same route"
    );
    assert_eq!(
        backup.calls_async().await,
        1,
        "the backup sees the tool result, got {:?}",
        names(&stage.events)
    );
    assert_eq!(
        work_stage(&state).response.as_deref(),
        Some("Recovered on backup")
    );
    let failover = stage
        .events
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| match &event.body {
            EventBody::Failover(props) => Some(props.clone()),
            _ => None,
        })
        .expect("the failover is emitted");
    assert_eq!(failover.from_provider, "primary");
    assert_eq!(failover.to_provider, "backup");
    assert_eq!(failover.to_model, "backup-model");
    assert!(
        failover.error.contains("primary key revoked"),
        "got {}",
        failover.error
    );
    assert_eq!(
        failover.continuation.as_deref(),
        Some("continue_turn"),
        "the primary committed a tool result, so the backup continued the turn"
    );
    let tool_completions = coding_events(&stage.events)
        .into_iter()
        .filter(|(_, event)| matches!(event, CodingEvent::ToolCallCompleted { .. }))
        .count();
    assert_eq!(tool_completions, 1, "the tool ran once across both routes");
    assert_eq!(
        work_stage(&state)
            .provider_used
            .as_ref()
            .and_then(|used| used.provider.clone()),
        Some("backup".to_string())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exhausted_fallback_chain_stores_the_stopped_failover() {
    let stage = Stage::new().await;
    let revoked = |then: httpmock::Then, key: &str| {
        then.status(401)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "error": { "message": format!("{key} key revoked"), "type": "invalid_request_error" }
            }));
    };
    let primary = stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path("/primary/v1/chat/completions");
            revoked(then, "primary");
        })
        .await;
    let backup = stage
        .server
        .mock_async(|when, then| {
            when.method(POST).path("/backup/v1/chat/completions");
            revoked(then, "backup");
        })
        .await;

    let overlay = format!(
        "{}\n{}",
        provider_toml(
            "primary",
            "primary-model",
            &stage.server.url("/primary/v1"),
            "openai"
        ),
        provider_toml(
            "backup",
            "backup-model",
            &stage.server.url("/backup/v1"),
            "openai"
        ),
    );
    let catalog = Arc::new(fabro_llm::test_support::test_catalog_with_overlay(&overlay));
    let primary_provider = ProviderId::new("primary");
    let fallbacks = model_fallback::resolve_model_fallbacks(
        &catalog,
        &[primary_provider.clone(), ProviderId::new("backup")],
        &BTreeMap::from([("primary-model".to_string(), vec![
            "backup/backup-model".parse::<ModelRef>().unwrap(),
        ])]),
    )
    .expect("the fallback chain resolves");
    let backend = PebbleBackend::new_with_catalog(
        "primary-model".to_string(),
        primary_provider,
        fallbacks.policy,
        mock_credentials(),
        Arc::clone(&stage.hub),
        catalog,
    );

    let mut graph = agent_graph("Exhausted", "Say hello");
    let work = graph.nodes.get_mut("work").unwrap();
    work.attrs
        .insert("max_retries".to_string(), AttrValue::Integer(0));
    graph.edges.retain(|edge| edge.from != "work");
    let mut fail_edge = Edge::new("work", "exit");
    fail_edge.attrs.insert(
        "condition".to_string(),
        AttrValue::String("outcome=failed".to_string()),
    );
    graph.edges.push(fail_edge);

    let (_, state) = stage
        .run(backend, &graph, CancellationToken::new())
        .await
        .expect("the fail edge carries the run to exit");

    assert_eq!(primary.calls_async().await, 1);
    assert_eq!(backup.calls_async().await, 1);
    assert_eq!(
        work_stage(&state)
            .completion
            .as_ref()
            .expect("the work stage completes")
            .outcome,
        StageOutcome::Failed {
            retry_requested: false,
        }
    );

    // The move to the backup is fabro's own event; the stop on the backup
    // is pebble's, stored under its derived name after the error it reports.
    assert_eq!(count(&stage.events, "agent.failover"), 1);
    assert_eq!(count(&stage.events, "agent.route.failover.stopped"), 1);
    let stopped_at = position(&stage.events, "agent.route.failover.stopped").unwrap();
    assert!(work_stage_event(&stage.events, stopped_at));
    let error_at = position(&stage.events, "agent.error").expect("the model error is stored");
    assert!(
        error_at < stopped_at,
        "the stop follows the error, got {:?}",
        names(&stage.events)
    );
    let (route, attempt, reason, error) = coding_events(&stage.events)
        .into_iter()
        .find_map(|(_, event)| match event {
            CodingEvent::RouteFailoverStopped {
                route,
                attempt,
                reason,
                error,
            } => Some((route, attempt, reason, error)),
            _ => None,
        })
        .expect("the stopped failover is stored as pebble's event");
    assert_eq!(route, "backup/backup-model");
    assert_eq!(attempt, 1);
    assert_eq!(reason, FailoverStop::Exhausted);
    assert!(
        error.message.contains("backup key revoked"),
        "got {}",
        error.message
    );
}

// --- Durability
// ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_event_sink_ends_the_stage() {
    let server = MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.method(POST).path(CHAT_PATH);
            sse_headers(then, sse_text("Never persisted"));
        })
        .await;
    let dir = tempfile::tempdir().unwrap();
    let emitter = Arc::new(Emitter::default());
    RunEventLogger::new(RunEventSink::callback(|_event| async {
        Err(anyhow::anyhow!("disk full"))
    }))
    .register(&emitter);
    let backend = mock_backend(
        &server,
        "openai",
        Arc::new(SteeringHub::new(Arc::clone(&emitter))),
    );
    let sandbox = local_sandbox(dir.path()).await;
    let node = agent_graph("Sink", "Say hello")
        .nodes
        .remove("work")
        .unwrap();
    let context = Context::new();

    let result = backend
        .run(CodergenRunRequest {
            node:            &node,
            prompt:          "Say hello",
            context:         &context,
            thread_id:       None,
            emitter:         &emitter,
            sandbox:         &sandbox,
            tool_middleware: None,
            cancel_token:    CancellationToken::new(),
            human_input:     None,
        })
        .await;

    let error = result
        .err()
        .expect("a stage whose events cannot persist fails");
    let rendered = format!("{:#}", anyhow::Error::new(error));
    assert!(
        rendered.contains("disk full"),
        "the sink failure is the cause, got {rendered}"
    );
}

// --- Provider smokes
// ----------------------------------------------------------

/// One agent stage whose tools run in `sandbox`: the model writes a file
/// there and reads it back through the shell, so both the filesystem and the
/// exec facets are exercised through pebble's `Environment`.
async fn agent_stage_smoke(sandbox: Arc<RunSandbox>, label: &str) {
    let server = MockServer::start_async().await;
    let path = format!("{}/smoke.txt", sandbox.working_directory());
    let arguments = serde_json::json!({ "file_path": path, "content": "hello from the model" });
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_excludes(TOOL_RESULT_MARKER);
            sse_headers(then, sse_tool_call("call-1", "write_file", &arguments));
        })
        .await;
    server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes(TOOL_RESULT_MARKER)
                .body_excludes("hello from the model\\n");
            sse_headers(
                then,
                sse_tool_call(
                    "call-2",
                    "shell",
                    &serde_json::json!({ "command": format!("cat {path}") }),
                ),
            );
        })
        .await;
    let finished = server
        .mock_async(|when, then| {
            when.method(POST)
                .path(CHAT_PATH)
                .body_includes("hello from the model\\n");
            sse_headers(then, sse_text("Done"));
        })
        .await;

    let emitter = Arc::new(Emitter::default());
    let events = observe(&emitter);
    let backend = mock_backend(
        &server,
        "openai",
        Arc::new(SteeringHub::new(Arc::clone(&emitter))),
    );
    let run_dir = tempfile::tempdir().unwrap();
    let runner = WorkflowRunner::new(agent_registry(backend), emitter, Arc::clone(&sandbox));
    let graph = agent_graph("Smoke", "Create and read smoke.txt");
    let (outcome, state) = runner
        .run_with_state(
            &graph,
            &run_options(run_dir.path(), CancellationToken::new()),
        )
        .await
        .expect("workflow execution should complete");
    assert_eq!(
        outcome.status,
        StageOutcome::Succeeded,
        "{label}: {outcome:?}"
    );

    assert_eq!(
        finished.calls_async().await,
        1,
        "{label}: {:?}",
        names(&events)
    );
    assert_eq!(
        sandbox.read_file_text(&path).await.unwrap(),
        "hello from the model",
        "{label}: the file lives in the sandbox"
    );
    assert_eq!(
        work_stage(&state).response.as_deref(),
        Some("Done"),
        "{label}"
    );
    let checkpoint = state.current_checkpoint().expect("a checkpoint");
    assert_eq!(
        checkpoint.node_outcomes["work"].files_touched,
        vec![path],
        "{label}"
    );
    assert_eq!(count(&events, "agent.tool.completed"), 2, "{label}");
}

/// Requires Docker with the default sandbox image available locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a Docker daemon"]
async fn docker_sandbox_runs_an_agent_stage() {
    let sandbox: Arc<RunSandbox> = Arc::new(
        fabro_sandbox::provider_sandbox(
            fabro_sandbox::SandboxProviderKind::DOCKER,
            &fabro_sandbox::ProviderAccess::default(),
            sandbox_driver::SandboxSpec::new(sandbox_driver::SandboxSource::HostDirectory),
            &fabro_sandbox::CloneRequest::none(),
            None,
            None,
        )
        .await
        .expect("Docker not available"),
    );
    sandbox.initialize().await.expect("Docker init failed");

    agent_stage_smoke(Arc::clone(&sandbox), "docker").await;

    sandbox.delete().await.expect("Docker cleanup failed");
}

#[fabro_macros::e2e_test(live("DAYTONA_API_KEY"))]
#[expect(
    clippy::disallowed_methods,
    reason = "The live Daytona smoke reads its credentials from the process environment."
)]
async fn daytona_sandbox_runs_an_agent_stage() {
    use fabro_static::EnvVars;

    let api_key = std::env::var(EnvVars::DAYTONA_API_KEY).expect("DAYTONA_API_KEY must be set");
    let access = fabro_sandbox::ProviderAccess {
        daytona: Some(fabro_sandbox::DaytonaCredentials::from_api_key(
            api_key,
            |name| std::env::var(name).ok(),
        )),
        ..fabro_sandbox::ProviderAccess::default()
    };
    let sandbox: Arc<RunSandbox> = Arc::new(
        fabro_sandbox::provider_sandbox(
            fabro_sandbox::SandboxProviderKind::DAYTONA,
            &access,
            sandbox_driver::SandboxSpec::new(sandbox_driver::SandboxSource::HostDirectory),
            &fabro_sandbox::CloneRequest::none(),
            None,
            None,
        )
        .await
        .expect("Failed to create Daytona client"),
    );
    sandbox.initialize().await.expect("Daytona init failed");

    agent_stage_smoke(Arc::clone(&sandbox), "daytona").await;

    sandbox.delete().await.expect("Daytona cleanup failed");
}
