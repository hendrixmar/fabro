use std::sync::Arc;

use async_trait::async_trait;
use fabro_sandbox::RunSandbox;
use fabro_types::{RunId, tool_call_arguments};
use pebble_agent::{
    ToolCallNext, ToolCallRequest, ToolErrorKind, ToolMiddleware, ToolOutcome, ToolSystemError,
};

use crate::runner::HookRunner;
use crate::types::{HookContext, HookDecision, HookEvent, HookExecutionContext};

/// Bridge between the workflow hook system and pebble's tool pipeline.
///
/// Created per-node in the workflow engine, capturing the `HookRunner` and
/// context needed to build `HookContext` for tool-level events. A blocking
/// `pre_tool_use` decision denies the call before it runs; `post_tool_use`
/// and `post_tool_use_failure` fire after the tool finishes, on success and
/// on failure respectively.
pub struct WorkflowToolHookCallback {
    pub hook_runner:            Arc<HookRunner>,
    pub sandbox:                Arc<RunSandbox>,
    pub run_id:                 RunId,
    pub workflow_name:          String,
    pub hook_execution_context: HookExecutionContext,
    pub node_id:                String,
}

impl WorkflowToolHookCallback {
    fn base_context(&self, event: HookEvent, tool_name: &str) -> HookContext {
        let mut ctx = HookContext::new(event, self.run_id, self.workflow_name.clone());
        ctx.node_id = Some(self.node_id.clone());
        ctx.tool_name = Some(tool_name.to_string());
        ctx
    }

    async fn run_hook(&self, ctx: &HookContext) -> HookDecision {
        self.hook_runner
            .run(
                ctx,
                self.sandbox.clone(),
                self.hook_execution_context.clone(),
            )
            .await
    }

    /// Whether a `pre_tool_use` hook blocks the call, and why.
    pub async fn pre_tool_use(
        &self,
        tool_name: &str,
        tool_input: &serde_json::Value,
    ) -> Option<String> {
        let mut ctx = self.base_context(HookEvent::PreToolUse, tool_name);
        ctx.tool_input = Some(tool_input.clone());

        match self.run_hook(&ctx).await {
            HookDecision::Block { reason } => {
                Some(reason.unwrap_or_else(|| "Blocked by hook".to_string()))
            }
            _ => None,
        }
    }

    pub async fn post_tool_use(&self, tool_name: &str, tool_call_id: &str, tool_output: &str) {
        let mut ctx = self.base_context(HookEvent::PostToolUse, tool_name);
        ctx.tool_call_id = Some(tool_call_id.to_string());
        ctx.tool_output = Some(tool_output.to_string());

        self.run_hook(&ctx).await;
    }

    pub async fn post_tool_use_failure(&self, tool_name: &str, tool_call_id: &str, error: &str) {
        let mut ctx = self.base_context(HookEvent::PostToolUseFailure, tool_name);
        ctx.tool_call_id = Some(tool_call_id.to_string());
        ctx.error_message = Some(error.to_string());

        self.run_hook(&ctx).await;
    }
}

#[async_trait]
impl ToolMiddleware for WorkflowToolHookCallback {
    async fn call(
        &self,
        request: ToolCallRequest,
        next: ToolCallNext<'_>,
    ) -> Result<ToolOutcome, ToolSystemError> {
        let tool_name = request.call().name.clone();
        let tool_call_id = request.call().id.clone();
        let tool_input = tool_call_arguments(request.call());

        if let Some(reason) = self.pre_tool_use(&tool_name, &tool_input).await {
            return Ok(ToolOutcome::failure(ToolErrorKind::Denied, reason));
        }

        let outcome = next.run(request).await?;
        match &outcome {
            ToolOutcome::Success { output, .. } => {
                self.post_tool_use(&tool_name, &tool_call_id, &output.text())
                    .await;
            }
            ToolOutcome::Failure { message, .. } => {
                self.post_tool_use_failure(&tool_name, &tool_call_id, message)
                    .await;
            }
            // `ToolOutcome` is non-exhaustive; an outcome this build does not
            // know is neither a success nor a failure the hooks describe.
            _ => {}
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use fabro_llm::credentials::CredentialProvider;
    use fabro_llm::lithos_catalog::Catalog;
    use fabro_types::fixtures;

    use super::*;
    use crate::config::{HookDefinition, HookSettings};
    use crate::executor::HookExecutor;
    use crate::types::{HookContext, HookResult};

    struct CapturingExecutor {
        captured_contexts:           Arc<Mutex<Vec<HookContext>>>,
        captured_execution_contexts: Arc<Mutex<Vec<HookExecutionContext>>>,
        decision:                    HookDecision,
    }

    #[async_trait::async_trait]
    impl HookExecutor for CapturingExecutor {
        async fn execute(
            &self,
            _definition: &HookDefinition,
            context: &HookContext,
            _sandbox: Arc<RunSandbox>,
            execution_context: &HookExecutionContext,
            _llm_source: Arc<dyn CredentialProvider>,
            _catalog: Arc<Catalog>,
        ) -> HookResult {
            self.captured_contexts.lock().unwrap().push(context.clone());
            self.captured_execution_contexts
                .lock()
                .unwrap()
                .push(execution_context.clone());
            HookResult {
                hook_name:   None,
                decision:    self.decision.clone(),
                duration_ms: 1,
            }
        }
    }

    fn make_hook(event: HookEvent) -> HookDefinition {
        HookDefinition {
            name: Some("test-hook".into()),
            event,
            command: Some("echo test".into()),
            hook_type: None,
            matcher: None,
            blocking: None,
            timeout_ms: None,
            sandbox: Some(false),
        }
    }

    async fn make_sandbox() -> Arc<RunSandbox> {
        Arc::new(
            fabro_sandbox::local_sandbox(std::env::current_dir().unwrap())
                .await
                .unwrap(),
        )
    }

    fn make_bridge(
        hook_runner: Arc<HookRunner>,
        sandbox: Arc<RunSandbox>,
        hook_execution_context: HookExecutionContext,
    ) -> WorkflowToolHookCallback {
        WorkflowToolHookCallback {
            hook_runner,
            sandbox,
            run_id: fixtures::RUN_1,
            workflow_name: "test-wf".into(),
            hook_execution_context,
            node_id: "plan".into(),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_builds_correct_context() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(CapturingExecutor {
            captured_contexts:           captured.clone(),
            captured_execution_contexts: Arc::new(Mutex::new(Vec::new())),
            decision:                    HookDecision::Proceed,
        });
        let config = HookSettings {
            hooks: vec![make_hook(HookEvent::PreToolUse)],
        };
        let runner = Arc::new(HookRunner::with_executor(config, executor));
        let sandbox = make_sandbox().await;
        let bridge = make_bridge(runner, sandbox, HookExecutionContext::default());

        bridge
            .pre_tool_use("shell", &serde_json::json!({"command": "ls"}))
            .await;

        let contexts = captured.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].event, HookEvent::PreToolUse);
        assert_eq!(contexts[0].tool_name.as_deref(), Some("shell"));
        assert_eq!(
            contexts[0].tool_input,
            Some(serde_json::json!({"command": "ls"}))
        );
        assert_eq!(contexts[0].run_id, fixtures::RUN_1);
        assert_eq!(contexts[0].node_id.as_deref(), Some("plan"));
    }

    #[tokio::test]
    async fn pre_tool_use_maps_block_decision() {
        let executor = Arc::new(CapturingExecutor {
            captured_contexts:           Arc::new(Mutex::new(Vec::new())),
            captured_execution_contexts: Arc::new(Mutex::new(Vec::new())),
            decision:                    HookDecision::Block {
                reason: Some("forbidden".into()),
            },
        });
        let config = HookSettings {
            hooks: vec![make_hook(HookEvent::PreToolUse)],
        };
        let runner = Arc::new(HookRunner::with_executor(config, executor));
        let sandbox = make_sandbox().await;
        let bridge = make_bridge(runner, sandbox, HookExecutionContext::default());

        let decision = bridge.pre_tool_use("shell", &serde_json::json!({})).await;
        assert_eq!(decision.as_deref(), Some("forbidden"));
    }

    #[tokio::test]
    async fn pre_tool_use_maps_proceed() {
        let executor = Arc::new(CapturingExecutor {
            captured_contexts:           Arc::new(Mutex::new(Vec::new())),
            captured_execution_contexts: Arc::new(Mutex::new(Vec::new())),
            decision:                    HookDecision::Proceed,
        });
        let config = HookSettings {
            hooks: vec![make_hook(HookEvent::PreToolUse)],
        };
        let runner = Arc::new(HookRunner::with_executor(config, executor));
        let sandbox = make_sandbox().await;
        let bridge = make_bridge(runner, sandbox, HookExecutionContext::default());

        let decision = bridge.pre_tool_use("shell", &serde_json::json!({})).await;
        assert_eq!(decision, None);
    }

    #[tokio::test]
    async fn post_tool_use_builds_context_with_output() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(CapturingExecutor {
            captured_contexts:           captured.clone(),
            captured_execution_contexts: Arc::new(Mutex::new(Vec::new())),
            decision:                    HookDecision::Proceed,
        });
        let config = HookSettings {
            hooks: vec![make_hook(HookEvent::PostToolUse)],
        };
        let runner = Arc::new(HookRunner::with_executor(config, executor));
        let sandbox = make_sandbox().await;
        let bridge = make_bridge(runner, sandbox, HookExecutionContext::default());

        bridge
            .post_tool_use("shell", "call_1", "file1.txt\nfile2.txt")
            .await;

        let contexts = captured.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].event, HookEvent::PostToolUse);
        assert_eq!(contexts[0].tool_name.as_deref(), Some("shell"));
        assert_eq!(contexts[0].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(
            contexts[0].tool_output.as_deref(),
            Some("file1.txt\nfile2.txt")
        );
    }

    #[tokio::test]
    async fn post_tool_use_failure_builds_context_with_error() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(CapturingExecutor {
            captured_contexts:           captured.clone(),
            captured_execution_contexts: Arc::new(Mutex::new(Vec::new())),
            decision:                    HookDecision::Proceed,
        });
        let config = HookSettings {
            hooks: vec![make_hook(HookEvent::PostToolUseFailure)],
        };
        let runner = Arc::new(HookRunner::with_executor(config, executor));
        let sandbox = make_sandbox().await;
        let bridge = make_bridge(runner, sandbox, HookExecutionContext::default());

        bridge
            .post_tool_use_failure("shell", "call_1", "command not found")
            .await;

        let contexts = captured.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].event, HookEvent::PostToolUseFailure);
        assert_eq!(contexts[0].tool_name.as_deref(), Some("shell"));
        assert_eq!(contexts[0].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(
            contexts[0].error_message.as_deref(),
            Some("command not found")
        );
    }

    #[tokio::test]
    async fn pre_tool_use_passes_supplied_hook_execution_context() {
        let captured_contexts = Arc::new(Mutex::new(Vec::new()));
        let captured_execution_contexts = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(CapturingExecutor {
            captured_contexts,
            captured_execution_contexts: Arc::clone(&captured_execution_contexts),
            decision: HookDecision::Proceed,
        });
        let config = HookSettings {
            hooks: vec![make_hook(HookEvent::PreToolUse)],
        };
        let runner = Arc::new(HookRunner::with_executor(config, executor));
        let sandbox = make_sandbox().await;
        let hook_execution_context = HookExecutionContext {
            host_source_dir:  Some(PathBuf::from("/host/source")),
            sandbox_work_dir: Some(PathBuf::from("/supplied/sandbox")),
        };
        let bridge = make_bridge(runner, sandbox, hook_execution_context.clone());

        bridge.pre_tool_use("shell", &serde_json::json!({})).await;

        assert_eq!(captured_execution_contexts.lock().unwrap().as_slice(), &[
            hook_execution_context
        ]);
    }
}
