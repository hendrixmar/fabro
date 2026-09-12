//! The API backend for LLM stages: pebble's `CodingAgent` bound to the
//! workflow's sandbox, events, steering, hooks, and human input.
//!
//! One agent serves one stage invocation. At `full` fidelity, stages sharing a
//! `thread_id` continue one conversation: the agent is exported when a stage
//! ends and resumed by the next, which binds its own event scope, hooks, and
//! interviewer. Model failover is pebble's: the stage hands it the resolved
//! fallback routes, pebble keeps the conversation as it stands and asks the
//! next route to continue it, and this module mirrors each move as the run's
//! `agent.failover` event.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fabro_graphviz::graph::Node;
use fabro_llm::credentials::CredentialProvider;
use fabro_llm::lithos_catalog::Catalog;
use fabro_llm::types::ResponseFormat;
use fabro_llm::{Client, ClientOptions, Request, Response};
use fabro_mcp::config::McpServerSettings;
use fabro_mcp::pebble::pebble_servers;
use fabro_sandbox::{RunSandbox, SecretRedactor};
use fabro_types::settings::run::RunModelControls;
use fabro_types::{
    AgentMcpToolSummary, AgentProfileKind, ModelRef, PermissionLevel, SessionCapability, StageId,
    StageTiming, UsdMicros, billing,
};
use fabro_util::home::Home;
use lithos_llm::catalog::{ModelId, ProviderId};
use lithos_llm::types::{Message as LlmMessage, Role, TokenCounts};
use pebble_agent::ToolMiddleware;
use pebble_coding_agent::environment::Environment;
use pebble_coding_agent::events::{CodingAgentEvent, CodingEvent, EventSink, EventSinkError};
use pebble_coding_agent::extensions::HumanInputProvider;
use pebble_coding_agent::state::Message;
use pebble_coding_agent::steering::SteerableSession;
use pebble_coding_agent::subagents::SubagentOptions;
use pebble_coding_agent::tools::{RegisteredTool, ToolEnvProvider};
use pebble_coding_agent::{
    CodingAgent, CodingAgentBuilder, CodingAgentControlHandle, CodingAgentExport,
    CodingAgentOptions, CodingInput, InterruptReason, MemoryDiscovery, ShutdownReason,
    SkillDiscovery,
};
use tokio_util::sync::CancellationToken;

use super::super::agent::{
    CodergenBackend, CodergenResult, CodergenRunRequest, OneShotRequest,
    validate_agent_output_sources,
};
use super::super::structured_output;
use super::activation_lease::{ActivationLease, ActivationLeaseOptions};
use super::controls::{
    EffectiveRequestControls, effective_request_controls, node_max_output_tokens,
};
use super::fabro_tools::register_fabro_run_tools;
use super::fallback::{self, FallbackPlan, LlmRoute};
use super::routing::{self, ProviderContext};
use crate::context::WorkflowContext;
use crate::context::keys::Fidelity;
use crate::error::Error;
use crate::event::{Emitter, Event, StageScope};
use crate::model_fallback::{ModelFallbackNotice, ModelFallbackPolicy};
use crate::outcome::billed_model_usage_from_llm;
use crate::services::FabroRunToolServices;
use crate::steering_hub::SteeringHub;
use crate::web_search::{self, SearchSecrets};

/// The share of the model's context window at which an agent stage compacts
/// its conversation. Fabro's own agent loop used this value; pebble's default
/// is the same, and it is set here so the stage's policy is fabro's to state.
pub const COMPACTION_THRESHOLD_PERCENT: usize = 80;

/// How many recent turns compaction leaves verbatim, as fabro's agent loop
/// did.
pub const COMPACTION_PRESERVE_TURNS: usize = 6;

/// The API backend: pebble coding agents over the workflow's LLM client.
pub struct PebbleBackend {
    model:                String,
    provider_id:          ProviderId,
    fallbacks:            ModelFallbackPolicy,
    /// Exported conversations keyed by thread, waiting for the next stage.
    threads:              Mutex<HashMap<String, CachedThread>>,
    /// Messages of fallback-plan notices already emitted for this run, so the
    /// same configuration warning is not repeated on every LLM call.
    emitted_plan_notices: Mutex<HashSet<String>>,
    tool_env:             Option<Arc<dyn ToolEnvProvider>>,
    mcp_servers:          Vec<McpServerSettings>,
    search_secrets:       SearchSecrets,
    skill_dirs:           Option<Vec<String>>,
    run_model_controls:   RunModelControls,
    source:               Arc<dyn CredentialProvider>,
    steering_hub:         Arc<SteeringHub>,
    catalog:              Arc<Catalog>,
    fabro_run_tools:      Option<FabroRunToolServices>,
}

/// A conversation between stages: what the next stage resumes from. The
/// successor starts the stage's MCP servers again; the same settings give
/// the same tool names, so the conversation's earlier calls stay valid.
struct CachedThread {
    export:        CodingAgentExport,
    fallback_plan: FallbackPlan,
}

/// How the backend reports a failed prompt.
///
/// A model error reaches this after pebble has followed every fallback route
/// the stage gave it, so it is terminal here whatever its kind.
enum AgentErrorDisposition {
    /// The run's token cancelled the prompt; surface as `Error::Cancelled`.
    Cancelled,
    /// Terminal error; abort the invocation with this workflow `Error`.
    Terminal(Error),
}

fn classify_agent_error(error: pebble_coding_agent::Error) -> AgentErrorDisposition {
    if let Some(llm) = error.llm_source() {
        return AgentErrorDisposition::Terminal(Error::from(llm.data()));
    }
    match error {
        pebble_coding_agent::Error::Interrupted(InterruptReason::Cancelled) => {
            AgentErrorDisposition::Cancelled
        }
        pebble_coding_agent::Error::Interrupted(InterruptReason::WallClockTimeout) => {
            AgentErrorDisposition::Terminal(Error::Precondition(
                "Agent session exceeded its wall-clock timeout".to_string(),
            ))
        }
        pebble_coding_agent::Error::Interrupted(InterruptReason::TurnLimit) => {
            AgentErrorDisposition::Terminal(Error::Precondition(
                "Agent session used every model turn it was allowed".to_string(),
            ))
        }
        // Stages set no round budget; the arm names the outcome should one
        // ever be configured.
        pebble_coding_agent::Error::ToolRoundsExhausted { limit } => {
            AgentErrorDisposition::Terminal(Error::Precondition(format!(
                "Agent session reached its limit of {limit} tool rounds"
            )))
        }
        pebble_coding_agent::Error::EventSink(sink) => AgentErrorDisposition::Terminal(Error::Io(
            format!("Failed to persist agent events: {sink:#}"),
        )),
        pebble_coding_agent::Error::FallbackRoute { route, source } => {
            AgentErrorDisposition::Terminal(Error::Precondition(format!(
                "Fallback route {route} could not be started: {source:#}"
            )))
        }
        // `InterruptReason` may grow; a reason this build does not know still
        // ended the prompt.
        pebble_coding_agent::Error::Interrupted(_) => AgentErrorDisposition::Terminal(
            Error::Precondition(format!("Agent session was interrupted: {error}")),
        ),
        other => AgentErrorDisposition::Terminal(Error::Precondition(format!(
            "Agent session failed: {other:#}"
        ))),
    }
}

// --- Event sink -----------------------------------------------------------

/// Pebble's durable event sink for one stage: every agent event becomes a
/// run event in the run's log before the agent goes on. A route failover and
/// an MCP server's outcome are facts the run already has events for, so
/// those are mirrored onto the run's own `agent.failover`, `agent.mcp.ready`,
/// and `agent.mcp.failed` events instead of being stored twice.
struct WorkflowEventSink {
    emitter: Arc<Emitter>,
    node_id: String,
    scope:   StageScope,
    /// The stage's resolved plan, for the controls and origin the mirrored
    /// failover event names.
    plan:    FallbackPlan,
}

#[async_trait]
impl EventSink for WorkflowEventSink {
    async fn record(&self, event: &CodingAgentEvent) -> Result<(), EventSinkError> {
        // Every event, including streaming deltas, resets the run's activity
        // watchdog.
        self.emitter.touch();
        match &event.event {
            CodingEvent::RouteFailover {
                from,
                to,
                attempt,
                error,
            } => {
                self.emitter.emit_scoped(
                    &Event::Failover {
                        stage: self.node_id.clone(),
                        props: self.plan.failover_props(from, to, *attempt, &error.message),
                    },
                    &self.scope,
                );
                return Ok(());
            }
            CodingEvent::McpServerReady { server, tools, .. } => {
                self.emitter.emit_scoped(
                    &Event::AgentMcpReady {
                        node_id:     self.node_id.clone(),
                        visit:       self.scope.visit,
                        server_name: server.clone(),
                        tool_count:  tools.len(),
                        tools:       tools
                            .iter()
                            .map(|tool| AgentMcpToolSummary {
                                name:          tool.name.clone(),
                                original_name: tool.original_name.clone(),
                            })
                            .collect(),
                    },
                    &self.scope,
                );
                return Ok(());
            }
            CodingEvent::McpServerFailed { server, error, .. } => {
                self.emitter.emit_scoped(
                    &Event::AgentMcpFailed {
                        node_id:     self.node_id.clone(),
                        visit:       self.scope.visit,
                        server_name: server.clone(),
                        error:       error.clone(),
                    },
                    &self.scope,
                );
                return Ok(());
            }
            _ => {}
        }
        // Deltas and the prompt's own durability barrier are not run history.
        if event.event.is_streaming_noise() || matches!(event.event, CodingEvent::ProcessingEnd) {
            return Ok(());
        }
        self.emitter
            .emit_durable(
                &Event::Agent {
                    stage: self.node_id.clone(),
                    visit: self.scope.visit,
                    event: event.clone(),
                },
                Some(&self.scope),
            )
            .await
            .map_err(|error| {
                EventSinkError::new(format!("failed to persist agent event: {error}"))
                    .with_source(error)
            })
    }
}

// --- Live invocation ------------------------------------------------------

/// One stage invocation's live agent and its accounting.
///
/// A stage may run several prompts on one agent (the prompt, output repairs,
/// late steering); the usage, cost, timing, and files of every one of them
/// are summed here, across whatever routes pebble moved through.
struct LiveAgent {
    agent:              CodingAgent,
    handle:             CodingAgentControlHandle,
    lease:              Option<Arc<ActivationLease>>,
    total_usage:        TokenCounts,
    total_cost:         Option<UsdMicros>,
    inference_duration: Duration,
    tool_duration:      Duration,
    /// Every file the stage's prompts wrote or edited, subagents included.
    files_touched:      BTreeSet<String>,
    /// The most recently written path.
    last_file_touched:  Option<String>,
}

impl LiveAgent {
    fn new(agent: CodingAgent, handle: CodingAgentControlHandle) -> Self {
        Self {
            agent,
            handle,
            lease: None,
            total_usage: TokenCounts::default(),
            total_cost: None,
            inference_duration: Duration::ZERO,
            tool_duration: Duration::ZERO,
            files_touched: BTreeSet::new(),
            last_file_touched: None,
        }
    }

    fn record_report(&mut self, report: &pebble_coding_agent::PromptReport) {
        billing::add_usage(&mut self.total_usage, TokenCounts::from(report.usage));
        UsdMicros::accumulate(
            &mut self.total_cost,
            report
                .cost_usd_micros
                .map(|micros| UsdMicros(i64::try_from(micros).unwrap_or(i64::MAX))),
        );
        self.inference_duration = self
            .inference_duration
            .saturating_add(report.timing.inference);
        self.tool_duration = self.tool_duration.saturating_add(report.timing.tool);
        self.files_touched
            .extend(report.files_touched.iter().cloned());
        for compaction in &report.compactions {
            // The summary call's usage is already in `report.usage`; this is
            // the breakdown, for anyone asking why a stage cost what it did.
            tracing::debug!(
                reason = ?compaction.reason,
                original_turns = compaction.original_turn_count,
                preserved_turns = compaction.preserved_turn_count,
                usage = ?compaction.usage,
                cost_usd_micros = ?compaction.cost_usd_micros,
                "agent stage compacted its conversation"
            );
        }
        if report.last_file_touched.is_some() {
            self.last_file_touched.clone_from(&report.last_file_touched);
        }
    }

    fn release_lease(&mut self) {
        if let Some(lease) = self.lease.take() {
            lease.release();
        }
    }

    /// End the agent for a prompt that will not continue on it.
    async fn discard(&mut self, reason: ShutdownReason) {
        self.release_lease();
        if let Err(error) = self.agent.shutdown(reason).await {
            tracing::debug!(error = %error, "agent session did not shut down cleanly");
        }
    }

    /// The text of the agent's last answer, when the report carried none.
    fn last_assistant_text(&self) -> String {
        self.agent
            .history()
            .turns()
            .iter()
            .rev()
            .find_map(|turn| match turn {
                Message::Assistant { content, .. } if !content.is_empty() => Some(content.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }
}

/// Everything one stage binds to an agent it builds or resumes.
struct StageBindings<'a> {
    node_id:         &'a str,
    stage_scope:     &'a StageScope,
    emitter:         &'a Arc<Emitter>,
    sandbox:         &'a Arc<RunSandbox>,
    tool_middleware: Option<&'a Arc<dyn ToolMiddleware>>,
    human_input:     Option<&'a Arc<dyn HumanInputProvider>>,
}

impl PebbleBackend {
    #[must_use]
    pub fn new(
        model: String,
        provider_id: impl Into<ProviderId>,
        fallbacks: ModelFallbackPolicy,
        source: Arc<dyn CredentialProvider>,
        steering_hub: Arc<SteeringHub>,
    ) -> Self {
        let catalog = Arc::new(fabro_llm::default_catalog());
        Self::new_with_catalog(
            model,
            provider_id.into(),
            fallbacks,
            source,
            steering_hub,
            catalog,
        )
    }

    #[must_use]
    pub fn new_with_catalog(
        model: String,
        provider_id: ProviderId,
        fallbacks: ModelFallbackPolicy,
        source: Arc<dyn CredentialProvider>,
        steering_hub: Arc<SteeringHub>,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            model,
            provider_id,
            fallbacks,
            threads: Mutex::new(HashMap::new()),
            emitted_plan_notices: Mutex::new(HashSet::new()),
            tool_env: None,
            mcp_servers: Vec::new(),
            search_secrets: SearchSecrets::default(),
            skill_dirs: None,
            run_model_controls: RunModelControls::default(),
            source,
            steering_hub,
            catalog,
            fabro_run_tools: None,
        }
    }

    #[must_use]
    pub fn with_tool_env_provider(mut self, provider: Arc<dyn ToolEnvProvider>) -> Self {
        self.tool_env = Some(provider);
        self
    }

    #[must_use]
    pub fn with_mcp_servers(mut self, servers: Vec<McpServerSettings>) -> Self {
        self.mcp_servers = servers;
        self
    }

    #[must_use]
    pub fn with_search_secrets(mut self, secrets: SearchSecrets) -> Self {
        self.search_secrets = secrets;
        self
    }

    /// Directories searched for skills, replacing the defaults (the user's
    /// Fabro skills directory plus `.fabro/skills` and `skills` under the
    /// sandbox working directory).
    #[must_use]
    pub fn with_skill_dirs(mut self, dirs: Vec<String>) -> Self {
        self.skill_dirs = Some(dirs);
        self
    }

    #[must_use]
    pub fn with_run_model_controls(mut self, controls: RunModelControls) -> Self {
        self.run_model_controls = controls;
        self
    }

    #[must_use]
    pub fn with_fabro_run_tools(mut self, services: FabroRunToolServices) -> Self {
        self.fabro_run_tools = Some(services);
        self
    }

    fn resolve_effective_request_controls(
        &self,
        node: &Node,
    ) -> Result<EffectiveRequestControls, Error> {
        effective_request_controls(&self.run_model_controls, node)
    }

    fn resolve_provider_context(
        &self,
        model: &str,
        provider_attr: Option<&str>,
    ) -> Result<ProviderContext, Error> {
        routing::resolve_provider_context(
            self.catalog.as_ref(),
            &self.provider_id,
            model,
            provider_attr,
        )
    }

    fn fallback_plan(
        &self,
        model: &str,
        provider: &ProviderId,
        requested_controls: EffectiveRequestControls,
    ) -> (FallbackPlan, Vec<ModelFallbackNotice>) {
        fallback::fallback_plan(
            self.catalog.as_ref(),
            &self.fallbacks,
            model,
            provider,
            requested_controls,
        )
    }

    fn emit_fallback_plan_notices(
        &self,
        notices: &[ModelFallbackNotice],
        emitter: &Emitter,
        stage_scope: &StageScope,
    ) {
        let mut emitted = self
            .emitted_plan_notices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for notice in notices {
            let message = notice.message();
            if emitted.insert(message.clone()) {
                emitter.notice_scoped(notice.level(), notice.code(), message, stage_scope);
            }
        }
    }

    async fn build_llm_client(&self) -> Result<Client, Error> {
        build_llm_client(&self.catalog, Arc::clone(&self.source)).await
    }

    /// Where a stage's skills come from: the directories the backend was
    /// given, else fabro's convention — the user's skills directory, then
    /// `.fabro/skills` and `skills` under the repository root — which pebble
    /// resolves and searches.
    fn skill_options(&self, options: CodingAgentOptions) -> CodingAgentOptions {
        match &self.skill_dirs {
            Some(dirs) => options.with_skill_dirs(dirs.clone()),
            None => options.with_skill_discovery(
                SkillDiscovery::new()
                    .search(Home::from_env().skills_dir().to_string_lossy().into_owned())
                    .search_under_git_root(".fabro/skills")
                    .search_under_git_root("skills"),
            ),
        }
    }

    fn agent_options(&self, node: &Node, controls: EffectiveRequestControls) -> CodingAgentOptions {
        // The profile's own instruction files, from the repository root down
        // to the working directory: pebble knows the files and does the walk.
        let options = CodingAgentOptions::default()
            .with_reasoning_effort(controls.reasoning_effort)
            .with_speed(controls.speed)
            .with_max_tokens(node_max_output_tokens(node).map(i64::from))
            .with_memory_discovery(MemoryDiscovery::from_git_root());
        self.skill_options(options)
            .with_recorded_permission_level(PermissionLevel::Full)
            .with_context_compaction(true)
            .with_compaction_threshold_percent(COMPACTION_THRESHOLD_PERCENT)
            .with_compaction_preserve_turns(COMPACTION_PRESERVE_TURNS)
    }

    /// The application tools a stage agent gets beyond pebble's own and the
    /// MCP servers'.
    fn stage_tools(&self) -> Vec<RegisteredTool> {
        match &self.fabro_run_tools {
            Some(services) => register_fabro_run_tools(services),
            None => Vec::new(),
        }
    }

    /// Bind the stage's services, the plan's current route, and the routes
    /// left to fail over to, to `builder`.
    fn bind_builder(
        &self,
        mut builder: CodingAgentBuilder,
        node: &Node,
        plan: &FallbackPlan,
        provider: &ProviderContext,
        bindings: &StageBindings<'_>,
    ) -> CodingAgentBuilder {
        let route = plan.current();
        let max_tokens = node_max_output_tokens(node).map(i64::from);
        builder = builder
            .tools(self.stage_tools())
            .mcp_servers(pebble_servers(&self.mcp_servers))
            .permission_level(PermissionLevel::Full)
            .options(self.agent_options(node, route.controls))
            .fallback_routes(plan.pebble_routes(max_tokens))
            .event_sink(Arc::new(WorkflowEventSink {
                emitter: Arc::clone(bindings.emitter),
                node_id: bindings.node_id.to_string(),
                scope:   bindings.stage_scope.clone(),
                plan:    plan.clone(),
            }))
            .redactor(Arc::new(SecretRedactor))
            .subagents(SubagentOptions::enabled());
        if let Some(routes) = bindings.sandbox.port_routes() {
            builder = builder.port_routes(routes);
        }
        if let Some(provider) = &self.tool_env {
            builder = builder.tool_env_provider(Arc::clone(provider));
        }
        if let Some(middleware) = bindings.tool_middleware {
            builder = builder.tool_middleware(Arc::clone(middleware));
        }
        if let Some(human_input) = bindings.human_input {
            builder = builder.human_input(Arc::clone(human_input));
        }
        if let Some(search) = web_search::search_provider(&self.search_secrets) {
            builder = builder.search_provider(search);
        }
        if provider.profile_kind == AgentProfileKind::Claude5 {
            builder = builder.web_fetch_summarizer(route.selector());
        }
        builder
    }

    /// A new agent on the plan's current route.
    async fn build_agent(
        &self,
        node: &Node,
        plan: &FallbackPlan,
        provider: &ProviderContext,
        bindings: &StageBindings<'_>,
    ) -> Result<CodingAgent, Error> {
        let client = self.build_llm_client().await?;
        let environment: Arc<dyn Environment> =
            Arc::clone(bindings.sandbox) as Arc<dyn Environment>;
        let builder = CodingAgent::builder(client, environment).model(plan.current().selector());
        self.bind_builder(builder, node, plan, provider, bindings)
            .build()
            .await
            .map_err(|error| Error::handler_with_source("Failed to start agent session", error))
    }

    /// The exported conversation of an earlier stage, continued on the
    /// route it was on, with the routes it had left.
    async fn resume_exported_agent(
        &self,
        export: CodingAgentExport,
        node: &Node,
        plan: &FallbackPlan,
        provider: &ProviderContext,
        bindings: &StageBindings<'_>,
    ) -> Result<CodingAgent, Error> {
        let client = self.build_llm_client().await?;
        let environment: Arc<dyn Environment> =
            Arc::clone(bindings.sandbox) as Arc<dyn Environment>;
        let builder = CodingAgent::resume_from_export(client, environment, export);
        self.bind_builder(builder, node, plan, provider, bindings)
            .build()
            .await
            .map_err(|error| Error::handler_with_source("Failed to resume agent session", error))
    }

    /// Register `live` with the steering hub so steers reach it, and tell
    /// the run which tools it has.
    fn activate(
        &self,
        live: &mut LiveAgent,
        route: &LlmRoute,
        stage_id: &StageId,
        thread_id: Option<&str>,
        bindings: &StageBindings<'_>,
    ) -> Result<(), Error> {
        let session: Arc<dyn SteerableSession> = Arc::new(live.handle.clone());
        let lease = ActivationLease::activate(
            ActivationLeaseOptions {
                stage_id:         stage_id.clone(),
                session_id:       live.agent.id().to_string(),
                thread_id:        thread_id.map(str::to_string),
                provider:         Some(route.target.provider.to_string()),
                model:            Some(route.target.model.to_string()),
                reasoning_effort: route.controls.reasoning_effort,
                speed:            route.controls.speed,
                permission_level: Some(PermissionLevel::Full),
                capabilities:     vec![SessionCapability::Steer],
                hub:              Arc::clone(&self.steering_hub),
                emitter:          Arc::clone(bindings.emitter),
            },
            session,
        )?;
        live.lease = Some(lease);
        bindings.emitter.emit(&Event::AgentToolsAvailable {
            node_id:    bindings.node_id.to_string(),
            visit:      stage_id.visit(),
            session_id: live.agent.id().to_string(),
            tools:      live.agent.snapshot().tools().to_vec(),
        });
        Ok(())
    }

    /// Run `input` on `live`. Pebble follows the stage's fallback routes
    /// itself; the plan here follows the route the prompt ended on, so a
    /// later prompt of this stage and a successor on the thread start there,
    /// and the run hears which route the session is on now.
    async fn prompt_live(
        &self,
        live: &mut LiveAgent,
        input: CodingInput,
        fallback_plan: &mut FallbackPlan,
        stage_id: &StageId,
        thread_id: Option<&str>,
        bindings: &StageBindings<'_>,
        cancel_token: &CancellationToken,
    ) -> Result<String, Error> {
        let report = live
            .agent
            .prompt_with_cancellation(input, cancel_token)
            .await;
        live.record_report(&report);
        if fallback_plan.advance_to(&report.route) {
            live.release_lease();
            self.activate(live, fallback_plan.current(), stage_id, thread_id, bindings)?;
        }
        match report.result {
            Ok(output) => Ok(output.text.unwrap_or_else(|| live.last_assistant_text())),
            Err(error) => match classify_agent_error(error) {
                AgentErrorDisposition::Cancelled => Err(Error::Cancelled),
                AgentErrorDisposition::Terminal(error) => Err(error),
            },
        }
    }

    /// Steers that landed between the answer and the hub's close-the-door
    /// check run as further prompts, so the stage never ends with a steer
    /// nobody saw.
    async fn drain_late_steering(
        &self,
        live: &mut LiveAgent,
        fallback_plan: &mut FallbackPlan,
        stage_id: &StageId,
        thread_id: Option<&str>,
        bindings: &StageBindings<'_>,
        cancel_token: &CancellationToken,
        mut response: String,
    ) -> Result<String, Error> {
        loop {
            let released = live
                .lease
                .as_ref()
                .is_none_or(|lease| lease.release_if_idle());
            if released {
                live.lease.take();
                return Ok(response);
            }
            let (steering, follow_ups) = live.handle.take_pending_input().into_parts();
            for message in steering.into_iter().chain(follow_ups) {
                response = self
                    .prompt_live(
                        live,
                        CodingInput::from(message.content().clone()),
                        fallback_plan,
                        stage_id,
                        thread_id,
                        bindings,
                        cancel_token,
                    )
                    .await?;
            }
        }
    }

    fn take_thread(&self, key: &str) -> Option<CachedThread> {
        self.threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key)
    }

    fn store_thread(&self, key: String, thread: CachedThread) {
        self.threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, thread);
    }

    // --- One-shot completions -------------------------------------------

    fn route_max_tokens(&self, node: &Node, route: &LlmRoute) -> Option<u32> {
        node_max_output_tokens(node).or_else(|| {
            self.catalog
                .enabled_provider(route.target.provider.as_str())
                .and_then(|provider| provider.offering(route.target.model.as_str()))
                .and_then(|entry| entry.model.limits())
                .map(|limits| u32::try_from(limits.max_output_tokens).unwrap_or(u32::MAX))
        })
    }

    /// Build a one-shot completion request addressed to `route`.
    fn route_request(
        &self,
        node: &Node,
        route: &LlmRoute,
        messages: Vec<LlmMessage>,
        response_format: Option<ResponseFormat>,
    ) -> Result<Request, Error> {
        let mut builder = Request::builder().model(route.selector());
        for message in messages {
            builder = builder.message(message);
        }
        if let Some(format) = response_format {
            builder = builder.response_format(format);
        }
        if let Some(max_tokens) = self.route_max_tokens(node, route) {
            builder = builder.max_output_tokens(max_tokens);
        }
        if let Some(effort) = route.controls.reasoning_effort {
            builder = builder.reasoning_effort(effort);
        }
        if let Some(speed) = route.controls.speed {
            builder = builder.speed(speed);
        }
        builder
            .build()
            .map_err(|err| Error::handler(format!("invalid LLM request: {err}")))
    }

    async fn complete_one_shot_request(
        &self,
        client: &Client,
        node: &Node,
        emitter: &Arc<Emitter>,
        stage_scope: &StageScope,
        mut request: Request,
        plan: &mut FallbackPlan,
    ) -> Result<OneShotCompletion, Error> {
        loop {
            match client.complete(request.clone()).await {
                Ok(response) => {
                    let route = plan.current();
                    return Ok(OneShotCompletion {
                        response,
                        model: ModelRef::new(
                            route.target.provider.clone(),
                            route.target.model.clone(),
                        )
                        .with_speed(route.controls.speed),
                    });
                }
                Err(error) if error.failover_eligible() && plan.has_next() => {
                    let error_message = error.to_string();
                    plan.advance();
                    fallback::emit_failover(node, emitter, stage_scope, plan, &error_message);
                    request = self.route_request(
                        node,
                        plan.current(),
                        request.messages().to_vec(),
                        request.response_format().cloned(),
                    )?;
                }
                Err(error) => return Err(Error::from(error)),
            }
        }
    }
}

struct OneShotCompletion {
    response: Response,
    model:    ModelRef,
}

/// Build the LLM client a stage session dispatches through.
async fn build_llm_client(
    catalog: &Arc<Catalog>,
    source: Arc<dyn CredentialProvider>,
) -> Result<Client, Error> {
    fabro_llm::build_client(Catalog::clone(catalog), source, ClientOptions::standard())
        .await
        .map(|built| built.client)
        .map_err(|e| Error::handler_with_source("Failed to create LLM client", e))
}

#[async_trait]
impl CodergenBackend for PebbleBackend {
    async fn shutdown(&self, _emitter: &Arc<Emitter>) {
        // Exported conversations were shut down when their stages ended, and
        // their MCP servers with them.
        self.threads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn effective_request_controls(&self, node: &Node) -> Result<EffectiveRequestControls, Error> {
        self.resolve_effective_request_controls(node)
    }

    async fn one_shot(&self, request: OneShotRequest<'_>) -> Result<CodergenResult, Error> {
        let node = request.node;
        let prompt = request.prompt;
        let system_prompt = request.system_prompt;
        let emitter = request.emitter;
        let stage_scope = request.stage_scope;

        let client = self.build_llm_client().await?;

        let model = node.model().unwrap_or(&self.model);
        let provider = self.resolve_provider_context(model, node.provider())?;
        let controls = self.resolve_effective_request_controls(node)?;
        let (mut fallback_plan, notices) =
            self.fallback_plan(model, &provider.provider_id, controls);
        self.emit_fallback_plan_notices(&notices, emitter, stage_scope);

        let mut messages = Vec::new();
        if let Some(sys) = system_prompt {
            messages.push(LlmMessage::text(Role::System, sys));
        }
        messages.push(LlmMessage::text(Role::User, prompt));

        let output_schema = structured_output::parse_node_output_schema(node)?;
        let response_format = output_schema
            .as_ref()
            .map(structured_output::prompt_response_format);
        let mut repair_attempts = 0_i64;
        let mut previous_validation_error = None;
        let mut total_usage = TokenCounts::default();
        let mut total_cost = None;
        let mut inference_duration = Duration::ZERO;

        loop {
            let request = self.route_request(
                node,
                fallback_plan.current(),
                messages.clone(),
                response_format.clone(),
            )?;

            let inference_start = Instant::now();
            let completion_result = self
                .complete_one_shot_request(
                    &client,
                    node,
                    emitter,
                    stage_scope,
                    request,
                    &mut fallback_plan,
                )
                .await;
            inference_duration = inference_duration.saturating_add(inference_start.elapsed());
            let completion = completion_result?;
            billing::add_usage(&mut total_usage, completion.response.usage);
            UsdMicros::accumulate(
                &mut total_cost,
                completion.response.cost.as_ref().map(UsdMicros::from_cost),
            );
            let response_text = completion.response.text();

            let validation_error = if let Some(schema) = &output_schema {
                match structured_output::validate_response_text(schema, &response_text) {
                    Ok(_) => None,
                    Err(error) => Some((schema, error)),
                }
            } else {
                None
            };

            if let Some((schema, error)) = validation_error {
                if repair_attempts >= node.output_retries() {
                    return Err(Error::OutputSchemaValidation(
                        structured_output::exhausted_failure_reason(node.output_retries()),
                    ));
                }
                let repair_message =
                    error.repair_message(schema, previous_validation_error.as_ref());
                previous_validation_error = Some(error);
                messages.push(LlmMessage::text(Role::Assistant, response_text));
                messages.push(LlmMessage::text(Role::User, repair_message));
                repair_attempts += 1;
                continue;
            }

            let stage_usage =
                billed_model_usage_from_llm(self.catalog.as_ref(), &completion.model, total_usage)?
                    .with_reported_cost(total_cost);

            return Ok(CodergenResult::Text {
                text:              response_text,
                usage:             Some(stage_usage),
                files_touched:     Vec::new(),
                last_file_touched: None,
                timing:            StageTiming::active_only(
                    crate::millis_u64(inference_duration),
                    0,
                ),
            });
        }
    }

    async fn run(&self, request: CodergenRunRequest<'_>) -> Result<CodergenResult, Error> {
        let node = request.node;
        let emitter = request.emitter;
        let cancel_token = &request.cancel_token;
        let output_schema = structured_output::parse_node_output_schema(node)?;

        let fidelity = request.context.fidelity();
        let reuse_key = if fidelity == Fidelity::Full {
            request.thread_id.map(String::from)
        } else {
            None
        };

        if cancel_token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let stage_scope = StageScope::for_handler(request.context, &node.id);
        let stage_id = stage_scope.stage_id();
        let bindings = StageBindings {
            node_id: &node.id,
            stage_scope: &stage_scope,
            emitter,
            sandbox: request.sandbox,
            tool_middleware: request.tool_middleware.as_ref(),
            human_input: request.human_input.as_ref(),
        };

        let cached = reuse_key.as_ref().and_then(|key| self.take_thread(key));
        let is_reused = cached.is_some();
        let (agent, mut fallback_plan) = if let Some(thread) = cached {
            let route = thread.fallback_plan.current().clone();
            let provider = self.resolve_provider_context(
                route.target.model.as_str(),
                Some(route.target.provider.as_str()),
            )?;
            let agent = self
                .resume_exported_agent(
                    thread.export,
                    node,
                    &thread.fallback_plan,
                    &provider,
                    &bindings,
                )
                .await?;
            (agent, thread.fallback_plan)
        } else {
            let model = node.model().unwrap_or(&self.model);
            let provider = routing::resolve_node_provider_context(
                self.catalog.as_ref(),
                &self.provider_id,
                &self.model,
                node,
            )?;
            let controls = self.resolve_effective_request_controls(node)?;
            let (fallback_plan, notices) =
                self.fallback_plan(model, &provider.provider_id, controls);
            self.emit_fallback_plan_notices(&notices, emitter, &stage_scope);
            let route = fallback_plan.current().clone();
            let route_provider = self.resolve_provider_context(
                route.target.model.as_str(),
                Some(route.target.provider.as_str()),
            )?;
            let agent = self
                .build_agent(node, &fallback_plan, &route_provider, &bindings)
                .await?;
            (agent, fallback_plan)
        };
        if cancel_token.is_cancelled() {
            let mut agent = agent;
            let _ = agent.shutdown(ShutdownReason::Cancelled).await;
            return Err(Error::Cancelled);
        }

        tracing::info!(
            node = %node.id,
            fidelity = %fidelity,
            reused = is_reused,
            "Agent session ready"
        );

        let handle = agent.control_handle();
        let mut live = LiveAgent::new(agent, handle);
        let route = fallback_plan.current().clone();
        if let Err(error) =
            self.activate(&mut live, &route, &stage_id, request.thread_id, &bindings)
        {
            live.discard(ShutdownReason::Error).await;
            return Err(error);
        }

        let result = async {
            let mut response = self
                .prompt_live(
                    &mut live,
                    CodingInput::text(request.prompt),
                    &mut fallback_plan,
                    &stage_id,
                    request.thread_id,
                    &bindings,
                    cancel_token,
                )
                .await?;

            if let Some(schema) = &output_schema {
                let mut repair_attempts = 0_i64;
                let mut previous_validation_error = None;
                loop {
                    let last_file_touched = live.last_file_touched.clone();
                    match validate_agent_output_sources(
                        schema,
                        &response,
                        request.sandbox,
                        last_file_touched.as_deref(),
                    )
                    .await
                    {
                        Ok(_) => break,
                        Err(error) => {
                            if repair_attempts >= node.output_retries() {
                                return Err(Error::OutputSchemaValidation(
                                    structured_output::exhausted_failure_reason(
                                        node.output_retries(),
                                    ),
                                ));
                            }
                            let repair_message =
                                error.repair_message(schema, previous_validation_error.as_ref());
                            // Only once the model has seen the repair can a later
                            // identical failure mean it ignored the correction.
                            previous_validation_error = Some(error);
                            response = self
                                .prompt_live(
                                    &mut live,
                                    CodingInput::text(repair_message),
                                    &mut fallback_plan,
                                    &stage_id,
                                    request.thread_id,
                                    &bindings,
                                    cancel_token,
                                )
                                .await?;
                            repair_attempts += 1;
                        }
                    }
                }
            }

            self.drain_late_steering(
                &mut live,
                &mut fallback_plan,
                &stage_id,
                request.thread_id,
                &bindings,
                cancel_token,
                response,
            )
            .await
        }
        .await;

        let response = match result {
            Ok(response) => response,
            Err(error) => {
                let reason = if matches!(error, Error::Cancelled) {
                    ShutdownReason::Cancelled
                } else {
                    ShutdownReason::Error
                };
                live.discard(reason).await;
                return Err(error);
            }
        };

        let route = fallback_plan.current().clone();
        let stage_usage = billed_model_usage_from_llm(
            self.catalog.as_ref(),
            &ModelRef::new(
                route.target.provider.clone(),
                ModelId::new(route.target.model.as_str()),
            )
            .with_speed(route.controls.speed),
            live.total_usage,
        )?
        .with_reported_cost(live.total_cost);

        live.release_lease();
        match reuse_key {
            // The thread's successor continues from an export whose cursor is
            // already past this session's close.
            Some(key) => match live.agent.export_for_reuse(ShutdownReason::Completed).await {
                Ok(export) => self.store_thread(key, CachedThread {
                    export,
                    fallback_plan: fallback_plan.clone(),
                }),
                Err(error) => {
                    tracing::debug!(error = %error, "agent session did not shut down cleanly");
                }
            },
            None => {
                if let Err(error) = live.agent.shutdown(ShutdownReason::Completed).await {
                    tracing::debug!(error = %error, "agent session did not shut down cleanly");
                }
            }
        }

        Ok(CodergenResult::Text {
            text:              response,
            usage:             Some(stage_usage),
            files_touched:     live.files_touched.into_iter().collect(),
            last_file_touched: live.last_file_touched,
            timing:            StageTiming::active_only(
                crate::millis_u64(live.inference_duration),
                crate::millis_u64(live.tool_duration),
            ),
        })
    }
}
