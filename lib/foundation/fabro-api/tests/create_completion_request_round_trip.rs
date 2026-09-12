use fabro_api::types::CreateCompletionRequest;
use lithos_llm::types::{ReasoningEffort, ResponseFormat, Speed, ToolChoice, ToolDefinitionKind};
use serde_json::json;

#[test]
fn create_completion_request_reuses_lithos_vocabulary() {
    let request: CreateCompletionRequest = serde_json::from_value(json!({
        "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        "model": "openai/gpt-5.4",
        "reasoning_effort": "high",
        "speed": "fast",
        "tools": [{
            "name": "lookup",
            "description": "Look something up",
            "kind": {"type": "function", "input_schema": {"type": "object"}}
        }],
        "tool_choice": {"type": "tool", "name": "lookup"},
        "response_format": {"type": "json_object"}
    }))
    .unwrap();

    assert_eq!(request.reasoning_effort, Some(ReasoningEffort::High));
    assert_eq!(request.speed, Some(Speed::Fast));
    assert_eq!(request.tools.len(), 1);
    assert!(matches!(
        request.tools[0].kind,
        ToolDefinitionKind::Function { .. }
    ));
    assert_eq!(
        request.tool_choice,
        Some(ToolChoice::Tool {
            name: "lookup".to_string(),
        })
    );
    assert_eq!(request.response_format, Some(ResponseFormat::JsonObject));
    assert_eq!(request.messages[0].content().len(), 1);
}
