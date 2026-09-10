use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use fabro_model::Model;
pub use fabro_model::ModelTestMode;
use strum::IntoStaticStr;
use tokio::time;

use crate::client::Client;
use crate::generate::{self, GenerateParams};
use crate::tools::Tool;
use crate::types::{GenerateResult, ReasoningEffort};

#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum ModelTestStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelTestOutcome {
    pub status:        ModelTestStatus,
    pub error_message: Option<String>,
}

impl ModelTestOutcome {
    #[must_use]
    pub fn ok() -> Self {
        Self {
            status:        ModelTestStatus::Ok,
            error_message: None,
        }
    }

    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status:        ModelTestStatus::Error,
            error_message: Some(message.into()),
        }
    }
}

pub async fn run_model_test(
    info: &Model,
    mode: ModelTestMode,
    reasoning_effort: Option<ReasoningEffort>,
    client: Arc<Client>,
) -> ModelTestOutcome {
    match mode {
        ModelTestMode::Basic => run_basic_test(info, reasoning_effort, client).await,
        ModelTestMode::Deep => run_tools_test(info, reasoning_effort, client).await,
    }
}

/// Output budget for tests where reasoning or tool rounds consume completion
/// tokens before the final answer.
const EXPANDED_MAX_TOKENS: i64 = 1024;

async fn run_basic_test(
    info: &Model,
    reasoning_effort: Option<ReasoningEffort>,
    client: Arc<Client>,
) -> ModelTestOutcome {
    basic_probe(
        info.id.as_str(),
        info.provider.to_string(),
        reasoning_effort,
        client,
        Duration::from_secs(ModelTestMode::Basic.timeout_secs()),
    )
    .await
}

/// Run the cheap single-prompt model availability probe without requiring a
/// catalog-backed [`Model`].
pub async fn run_basic_model_probe(
    model_id: &str,
    provider: impl ToString,
    client: Arc<Client>,
) -> ModelTestOutcome {
    run_basic_model_probe_with_timeout(
        model_id,
        provider,
        client,
        Duration::from_secs(ModelTestMode::Basic.timeout_secs()),
    )
    .await
}

pub async fn run_basic_model_probe_with_timeout(
    model_id: &str,
    provider: impl ToString,
    client: Arc<Client>,
    probe_timeout: Duration,
) -> ModelTestOutcome {
    basic_probe(model_id, provider.to_string(), None, client, probe_timeout).await
}

async fn basic_probe(
    model_id: &str,
    provider: String,
    reasoning_effort: Option<ReasoningEffort>,
    client: Arc<Client>,
    probe_timeout: Duration,
) -> ModelTestOutcome {
    let params = build_basic_test_params(model_id, provider, reasoning_effort, client);
    basic_model_probe_outcome(generate::generate(params), probe_timeout).await
}

fn build_basic_test_params(
    model_id: &str,
    provider: String,
    reasoning_effort: Option<ReasoningEffort>,
    client: Arc<Client>,
) -> GenerateParams {
    let max_tokens = if reasoning_effort.is_some() {
        EXPANDED_MAX_TOKENS
    } else {
        16
    };
    let mut params = GenerateParams::new(model_id, client)
        .provider(provider)
        .prompt("Say OK")
        .max_tokens(max_tokens);

    if let Some(reasoning_effort) = reasoning_effort {
        params = params.reasoning_effort(reasoning_effort);
    }

    params
}

async fn basic_model_probe_outcome<F>(probe: F, probe_timeout: Duration) -> ModelTestOutcome
where
    F: Future<Output = Result<GenerateResult, crate::Error>>,
{
    match time::timeout(probe_timeout, probe).await {
        Ok(Ok(_)) => ModelTestOutcome::ok(),
        Ok(Err(err)) => ModelTestOutcome::error(err.to_string()),
        Err(_) => ModelTestOutcome::error(format!("timeout ({probe_timeout:?})")),
    }
}

async fn run_tools_test(
    info: &Model,
    reasoning_effort: Option<ReasoningEffort>,
    client: Arc<Client>,
) -> ModelTestOutcome {
    let Some(params) = build_tools_test_params(info, reasoning_effort, client) else {
        return ModelTestOutcome::error("model does not support tools");
    };

    let result = time::timeout(
        Duration::from_secs(ModelTestMode::Deep.timeout_secs()),
        generate::generate(params),
    )
    .await;

    match result {
        Ok(Ok(gen_result)) => match validate_tools_result(&gen_result) {
            Ok(()) => ModelTestOutcome::ok(),
            Err(message) => ModelTestOutcome::error(message),
        },
        Ok(Err(err)) => ModelTestOutcome::error(err.to_string()),
        Err(_) => ModelTestOutcome::error("timeout (90s)"),
    }
}

fn build_tools_test_params(
    info: &Model,
    reasoning_effort: Option<ReasoningEffort>,
    client: Arc<Client>,
) -> Option<GenerateParams> {
    if !info.features.tools {
        return None;
    }

    let add_tool = Tool::active(
        "add",
        "Add two integers and return the sum",
        serde_json::json!({
            "type": "object",
            "properties": {
                "a": { "type": "integer", "description": "First number" },
                "b": { "type": "integer", "description": "Second number" }
            },
            "required": ["a", "b"]
        }),
        |args, _ctx| async move {
            let a = args
                .get("a")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            let b = args
                .get("b")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            Ok(serde_json::json!(a + b))
        },
    );

    let mut params = GenerateParams::new(info.id.to_string(), client)
        .provider(info.provider.to_string())
        .prompt(
            "Use the add tool twice: first add 15 and 27, then add that result to 42. \
             Finally, tell me whether the grand total is even or odd and why.",
        )
        .tools(vec![add_tool])
        .max_tool_rounds(5)
        .max_tokens(EXPANDED_MAX_TOKENS);

    if let Some(reasoning_effort) = reasoning_effort {
        params = params.reasoning_effort(reasoning_effort);
    }

    Some(params)
}

fn validate_tools_result(result: &GenerateResult) -> Result<(), String> {
    if result.steps.len() < 2 {
        return Err("model did not call tool".to_string());
    }

    if result.steps[0].tool_results.is_empty() {
        return Err("tool was not executed".to_string());
    }

    if !result.response.text().contains("84") {
        return Err("wrong answer".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use fabro_model::{
        ModelControls, ModelCosts, ModelFeatures, ModelLimits, ProviderId, ReasoningEffortFeature,
    };

    use super::*;
    use crate::types::{FinishReason, Message, Response, StepResult, TokenCounts, ToolResult};

    fn test_model_with(features: ModelFeatures) -> Model {
        Model {
            id: "test-model".into(),
            provider: ProviderId::anthropic(),
            family: "test".to_string(),
            display_name: "Test Model".to_string(),
            limits: ModelLimits {
                context_window: 200_000,
                max_output:     Some(8_000),
            },
            training: None,
            knowledge_cutoff: None,
            features,
            controls: ModelControls::default(),
            costs: ModelCosts {
                input_cost_per_mtok:       None,
                output_cost_per_mtok:      None,
                cache_input_cost_per_mtok: None,
            },
            estimated_output_tps: None,
            aliases: vec![],
            default: false,
            small_default: false,
            configured: false,
        }
    }

    fn response_with_text(text: &str) -> Response {
        Response {
            id:            "resp_1".to_string(),
            model:         "test-model".to_string(),
            provider:      "anthropic".to_string(),
            message:       Message::assistant(text),
            finish_reason: FinishReason::Stop,
            usage:         TokenCounts::default(),
            raw:           None,
            warnings:      vec![],
            rate_limit:    None,
            cost_usd:      None,
            cost_source:   None,
        }
    }

    fn empty_test_client() -> Arc<Client> {
        Arc::new(Client::new(HashMap::new(), None, vec![]))
    }

    #[tokio::test]
    async fn run_model_test_tools_errors_when_model_lacks_tools() {
        let info = test_model_with(ModelFeatures {
            tools:                     false,
            vision:                    false,
            reasoning:                 true,
            reasoning_effort:          ReasoningEffortFeature::Levels,
            prompt_cache:              false,
            cache_control_breakpoints: false,
            sampling_params:           true,
        });

        let outcome = run_model_test(&info, ModelTestMode::Deep, None, empty_test_client()).await;

        assert_eq!(outcome.status, ModelTestStatus::Error);
        assert_eq!(
            outcome.error_message.as_deref(),
            Some("model does not support tools")
        );
    }

    #[tokio::test]
    async fn basic_model_probe_reports_configured_timeout() {
        let outcome = basic_model_probe_outcome(
            std::future::pending::<Result<GenerateResult, crate::Error>>(),
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(outcome.status, ModelTestStatus::Error);
        assert_eq!(outcome.error_message.as_deref(), Some("timeout (1ms)"));
    }

    #[test]
    fn basic_test_expands_output_budget_for_reasoning() {
        let params = build_basic_test_params(
            "test-model",
            "anthropic".to_string(),
            Some(ReasoningEffort::Max),
            empty_test_client(),
        );

        assert_eq!(params.reasoning_effort, Some(ReasoningEffort::Max));
        assert_eq!(params.max_tokens, Some(1024));
    }

    #[test]
    fn tools_test_omits_effort_when_not_requested() {
        let info = test_model_with(ModelFeatures {
            tools:                     true,
            vision:                    false,
            reasoning:                 true,
            reasoning_effort:          ReasoningEffortFeature::Levels,
            prompt_cache:              true,
            cache_control_breakpoints: false,
            sampling_params:           true,
        });

        let params = build_tools_test_params(&info, None, empty_test_client())
            .expect("tool-capable model should produce tools-test params");

        assert_eq!(params.reasoning_effort, None);
    }

    #[test]
    fn tools_test_uses_requested_effort() {
        let info = test_model_with(ModelFeatures {
            tools:                     true,
            vision:                    false,
            reasoning:                 true,
            reasoning_effort:          ReasoningEffortFeature::Levels,
            prompt_cache:              true,
            cache_control_breakpoints: false,
            sampling_params:           true,
        });

        let params =
            build_tools_test_params(&info, Some(ReasoningEffort::Low), empty_test_client())
                .expect("tool-capable model should produce tools-test params");

        assert_eq!(params.reasoning_effort, Some(ReasoningEffort::Low));
    }

    #[test]
    fn validate_tools_result_does_not_fail_only_for_missing_reasoning() {
        let tool_results = vec![ToolResult::success("call_1", serde_json::json!(42))];
        let first_step = StepResult {
            response:     response_with_text("tool step"),
            tool_results: tool_results.clone(),
        };
        let second_step = StepResult {
            response:     response_with_text("84 is even"),
            tool_results: vec![],
        };
        let result = GenerateResult {
            response: response_with_text("84 is even"),
            tool_results,
            total_usage: TokenCounts::default(),
            steps: vec![first_step, second_step],
            output: None,
        };

        assert_eq!(validate_tools_result(&result), Ok(()));
    }
}
