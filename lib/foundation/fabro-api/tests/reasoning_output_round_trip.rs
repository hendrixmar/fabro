use std::any::{TypeId, type_name};

use fabro_api::types::{
    AgentEventProps as ApiAgentEventProps, ReasoningOutput as ApiReasoningOutput,
};
use fabro_types::AgentEventProps;
use lithos_llm::types::ReasoningOutput;
use serde_json::json;

#[test]
fn reasoning_output_reuses_canonical_type() {
    assert_same_type::<ApiReasoningOutput, ReasoningOutput>();
}

#[test]
fn reasoning_output_matches_openapi_json_shape() {
    let value = json!({
        "summary": "inspect the conversion first",
        "trace": "read convert.rs, then the sink",
    });

    let output: ReasoningOutput = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(output.summary(), Some("inspect the conversion first"));
    assert_eq!(output.trace(), Some("read convert.rs, then the sink"));
    assert_eq!(serde_json::to_value(&output).unwrap(), value);

    let api_output: ApiReasoningOutput = serde_json::from_value(value).unwrap();
    assert_eq!(api_output, output);
}

#[test]
fn reasoning_output_members_are_individually_optional() {
    let summary_only: ReasoningOutput =
        serde_json::from_value(json!({"summary": "only a summary"})).unwrap();
    assert!(summary_only.trace().is_none());
    assert_eq!(
        serde_json::to_value(&summary_only).unwrap(),
        json!({"summary": "only a summary"})
    );

    let trace_only: ReasoningOutput =
        serde_json::from_value(json!({"trace": "only a trace"})).unwrap();
    assert!(trace_only.summary().is_none());
    assert_eq!(
        serde_json::to_value(&trace_only).unwrap(),
        json!({"trace": "only a trace"})
    );
}

#[test]
fn reasoning_output_rejects_an_empty_object() {
    let error = serde_json::from_value::<ApiReasoningOutput>(json!({})).unwrap_err();
    assert!(error.to_string().contains("requires a summary or trace"));
}

#[test]
fn agent_event_props_reuse_the_canonical_type() {
    assert_same_type::<ApiAgentEventProps, AgentEventProps>();
}

/// An `agent.message` event carries the coding agent's own envelope; the
/// assistant message inside it keeps reasoning optional on the wire.
#[test]
fn agent_event_props_round_trip_an_assistant_message_with_reasoning() {
    let without = json!({
        "stage": "code",
        "visit": 1,
        "seq": 7,
        "stream_id": "ses_root",
        "session_id": "ses_root",
        "timestamp": "2026-05-23T12:34:56.000Z",
        "event": {
            "AssistantMessage": {
                "text": "ok",
                "model": "gpt-5.4",
                "usage": {"input": 1, "output": 1},
                "tool_call_count": 0
            }
        }
    });
    let props: ApiAgentEventProps = serde_json::from_value(without.clone()).unwrap();
    assert_eq!(props.stage, "code");
    assert_eq!(props.event.session_id, "ses_root");
    let value = serde_json::to_value(&props).unwrap();
    assert!(
        value["event"]["AssistantMessage"]
            .get("reasoning")
            .is_none()
    );

    let mut with = without;
    with["event"]["AssistantMessage"]["reasoning"] =
        json!({"summary": "checked the parser", "trace": "step one"});
    let props: ApiAgentEventProps = serde_json::from_value(with).unwrap();
    let value = serde_json::to_value(&props).unwrap();
    assert_eq!(
        value["event"]["AssistantMessage"]["reasoning"],
        json!({"summary": "checked the parser", "trace": "step one"})
    );
}

fn assert_same_type<T: 'static, U: 'static>() {
    assert_eq!(
        TypeId::of::<T>(),
        TypeId::of::<U>(),
        "{} should be the same type as {}",
        type_name::<T>(),
        type_name::<U>()
    );
}
