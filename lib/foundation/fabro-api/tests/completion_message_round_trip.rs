//! Proves the `CompletionMessage` / `CompletionMessageRole` /
//! `CompletionContentPart` OpenAPI schemas are served by the lithos
//! `Message`, `Role`, and `ContentPart` types re-exported from `fabro_types`
//! via build.rs `with_replacement`, and that the lithos serde output matches
//! the wire shape the spec describes.

use std::any::{TypeId, type_name};

use fabro_api::types::{ContentPart as ApiContentPart, Message as ApiMessage, Role as ApiRole};
use lithos_llm::types::{ContentPart, Message, Role, ToolCall, ToolResult};
use serde_json::json;

#[test]
fn completion_message_reuses_domain_types() {
    assert_same_type::<ApiMessage, Message>();
    assert_same_type::<ApiRole, Role>();
    assert_same_type::<ApiContentPart, ContentPart>();
}

#[test]
fn role_json_matches_openapi_enum() {
    for (role, wire) in [
        (Role::System, "system"),
        (Role::Developer, "developer"),
        (Role::User, "user"),
        (Role::Assistant, "assistant"),
        (Role::Tool, "tool"),
    ] {
        assert_eq!(serde_json::to_value(role).unwrap(), json!(wire));
        assert_eq!(
            serde_json::from_value::<Role>(json!(wire)).unwrap(),
            role,
            "round trip for {wire}"
        );
    }
}

#[test]
fn message_json_matches_openapi_shape() {
    // Optional fields are omitted, not serialized as null.
    assert_eq!(
        serde_json::to_value(Message::text(Role::User, "hello")).unwrap(),
        json!({
            "role": "user",
            "content": [{"type": "text", "text": "hello"}]
        })
    );

    let message = Message::new(Role::Tool, vec![ContentPart::ToolResult(ToolResult {
        tool_call_id: "call_1".to_string(),
        name:         None,
        content:      vec![ContentPart::Text {
            text: "ok".to_string(),
        }],
        is_error:     false,
    })])
    .with_name("checker")
    .with_tool_call_id("call_1");
    assert_eq!(
        serde_json::to_value(message).unwrap(),
        json!({
            "role": "tool",
            "content": [{
                "type": "tool_result",
                "tool_call_id": "call_1",
                "content": [{"type": "text", "text": "ok"}],
                "is_error": false
            }],
            "name": "checker",
            "tool_call_id": "call_1"
        })
    );
}

#[test]
fn tool_call_part_json_matches_lithos_shape() {
    let part = ContentPart::ToolCall(ToolCall::function(
        "call_1",
        "write_workflow_file",
        json!({"file_name": "workflow.fabro"}),
    ));
    let json = serde_json::to_value(&part).unwrap();
    assert_eq!(json["type"], "tool_call");
    assert_eq!(json["id"], "call_1");
    assert_eq!(json["name"], "write_workflow_file");
    let round_trip: ContentPart = serde_json::from_value(json).unwrap();
    assert_eq!(round_trip, part);
}

#[test]
fn content_part_preserves_unknown_types() {
    // The spec leaves `type` open-ended; unknown types must round-trip so a
    // newer writer's parts survive an older reader.
    let wire = json!({"type": "mystery", "x": 1});
    let part: ContentPart = serde_json::from_value(wire.clone()).unwrap();
    assert!(matches!(part, ContentPart::Unknown(_)));
    assert_eq!(serde_json::to_value(part).unwrap(), wire);
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
